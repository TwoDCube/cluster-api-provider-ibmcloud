/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package managed

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/clustercache"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"

	. "github.com/onsi/gomega"
)

// The delete tests drive ReconcileDelete reconcile by reconcile against a fake management cluster (which keeps the
// disruption token), a fake workload cluster behind a fake ClusterCache and a gomock IBM Cloud Kubernetes Service.

// failIfCalledClusterCache is a ClusterCache for paths that must make no workload-cluster call.
type failIfCalledClusterCache struct {
	clustercache.ClusterCache
	t *testing.T
}

// GetClient implements clustercache.ClusterCache; it fails the test.
func (c failIfCalledClusterCache) GetClient(context.Context, client.ObjectKey) (client.Client, error) {
	c.t.Errorf("ClusterCache.GetClient called, but this path must make no workload-cluster call")
	return nil, clustercache.ErrClusterNotConnected
}

// deleteFixture is a pool fixture whose pool is being deleted, with a connected workload cluster.
type deleteFixture struct {
	*poolFixture
	calls    *workloadCalls
	workload client.Client
	// workers are this pool's four ready workers and two ready workers of another pool, which keep IBM's minimum.
	workers []kubernetesservice.Worker
	// clusterState is the IBM cluster state the gate tests answer.
	clusterState string
}

// newDeleteFixture returns the fixture of a deleting pool "workers" with four ready workers w0..w3, one Node and one pod
// each, and a pool "other" with two ready workers, whose Nodes the drain never touches.
//
// Parameters:
//   - t is the test.
//   - refusals are the evictions a PodDisruptionBudget refuses, by pod name.
func newDeleteFixture(t *testing.T, refusals map[string]error) *deleteFixture {
	t.Helper()
	f := &deleteFixture{poolFixture: newPoolFixture(t), calls: &workloadCalls{}}
	f.pool.Status.PoolID = poolTestPoolID
	f.pool.Finalizers = []string{infrav1.IBMCloudManagedMachinePoolFinalizer}
	f.pool.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow.Add(-time.Minute)))
	f.workers = append(readyWorkers(4),
		kubernetesservice.Worker{ID: "o0", PoolID: "other-pool", Zone: "us-south-1", ActualState: WorkerStateDeployed, HealthState: "normal"},
		kubernetesservice.Worker{ID: "o1", PoolID: "other-pool", Zone: "us-south-2", ActualState: WorkerStateDeployed, HealthState: "normal"})
	objs := make([]client.Object, 0, 10)
	objs = append(objs, victimNode("10.0.1.1", "o0"), victimNode("10.0.1.2", "o1"))
	for i, id := range []string{"w0", "w1", "w2", "w3"} {
		name := "10.0.0." + string(rune('1'+i))
		objs = append(objs, victimNode(name, id), workloadPod("web-"+id, name))
	}
	f.workload = newWorkloadClient(t, f.calls, refusals, objs...)
	f.clusterCache = clustercache.NewFakeClusterCache(f.workload, client.ObjectKey{Namespace: poolTestNamespace, Name: poolTestCluster})
	return f
}

// expectIBM expects one GetCluster answering a normal cluster, one ListWorkerPools answering pools and one ListWorkers
// answering the fixture's workers.
func (f *deleteFixture) expectIBM(pools ...kubernetesservice.WorkerPool) {
	f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{ID: poolTestClusterID, State: "normal"}, nil)
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(pools, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(f.workers, nil)
}

// ourPool is the IBM pool of the fixture with state ("" steady, "deleting").
func ourPool(state string) kubernetesservice.WorkerPool {
	pool := ibmPool("workers", 2, "us-south-1", "us-south-2")
	pool.ActualState = state
	return pool
}

// heldLease is the disruption token Lease held by holder.
func heldLease(holder string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: poolTestNamespace, Name: DisruptionTokenName(poolTestCluster)},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: ptr.To(holder)},
	}
}

// lease returns the disruption token Lease, or nil.
func (f *deleteFixture) lease() *coordinationv1.Lease {
	f.t.Helper()
	lease := &coordinationv1.Lease{}
	err := f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: poolTestNamespace, Name: DisruptionTokenName(poolTestCluster)}, lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return lease
}

// deleting returns the Deleting condition of the pool.
func (f *deleteFixture) deleting() *metav1.Condition {
	return condition(f.pool, infrav1.IBMCloudManagedMachinePoolDeletingCondition)
}

// cordoned returns the names of the cordoned Nodes, sorted.
func (f *deleteFixture) cordoned() []string {
	var names []string
	for _, node := range readNodes(f.t, f.workload) {
		if node.Spec.Unschedulable {
			names = append(names, node.Name)
		}
	}
	return names
}

func TestMachinePoolScopeDeleteSkips(t *testing.T) {
	testCases := []struct {
		name   string
		modify func(f *deleteFixture)
	}{
		{name: "the_Cluster_is_being_deleted", modify: func(f *deleteFixture) {
			f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
			f.cluster.Finalizers = []string{"cluster.cluster.x-k8s.io"}
		}},
		{name: "delete_for_move", modify: func(f *deleteFixture) {
			f.pool.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
		}},
		{name: "the control plane is gone", modify: func(f *deleteFixture) { f.cp = nil }},
		{name: "the control plane is being deleted", modify: func(f *deleteFixture) {
			f.cp.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
			f.cp.Finalizers = []string{"test"}
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newDeleteFixture(t, nil)
			f.clusterCache = failIfCalledClusterCache{t: t}
			tc.modify(f)
			// gomock fails on any IBM call.
			outcome, remove := f.scope().ReconcileDelete(context.Background())
			g.Expect(outcome).To(Equal(Done()))
			g.Expect(remove).To(BeTrue())
			g.Expect(f.resolver.holders).To(BeEmpty(), "the skip paths never resolve credentials")
			g.Expect(f.calls.all()).To(BeEmpty())
		})
	}
}

func TestMachinePoolScopeDeleteOwners(t *testing.T) {
	t.Run("another_live_claimant_keeps_the_IBM_pool", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.others = []client.Object{poolObject("workers-copy", "workers")}
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(Done()))
		g.Expect(remove).To(BeTrue())
		g.Expect(f.events()).To(ConsistOf(ContainSubstring("Warning WorkerPoolNotRemoved IBM worker pool workers stays: IBMCloudManagedMachinePool workers-copy still claims it")))
	})
	t.Run("a claimant that is being deleted does not keep it", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		other := poolObject("workers-copy", "workers")
		other.Finalizers, other.DeletionTimestamp = []string{"test"}, ptr.To(metav1.NewTime(poolTestNow))
		f.others = []client.Object{other}
		f.scope()
		g.Expect(f.loadedScope(t).liveClaimants()).To(BeEmpty())
	})
	t.Run("the IBMCloudManagedCluster is gone while the IBM cluster exists", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.infra = nil
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeFalse())
		g.Expect(outcome).To(Equal(NotDone(IntervalSteadyState, infrav1.IBMCloudManagedMachinePoolDeletingWaitingForInfrastructureReason)))
		g.Expect(f.deleting().Status).To(Equal(metav1.ConditionTrue))
		g.Expect(f.resolver.holders).To(BeEmpty())
	})
	t.Run("no cluster ID with an endpoint waits for the control plane", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.cp.Status.ClusterID = ""
		f.cp.Spec.ControlPlaneEndpoint.Host = "c100.us-south.containers.cloud.ibm.com"
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeFalse())
		g.Expect(outcome).To(Equal(NotDone(IntervalClusterProvisioning, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason)))
	})
	t.Run("no IBM cluster was ever created", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.cp.Status.ClusterID = ""
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(Done()))
		g.Expect(remove).To(BeTrue())
	})
}

func TestMachinePoolScopeDeleteEnds(t *testing.T) {
	t.Run("the IBM cluster is gone", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).
			Return(nil, apiError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(Done()))
		g.Expect(remove).To(BeTrue())
	})
	t.Run("an empty answer is transient", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(nil, nil)
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(remove).To(BeFalse())
	})
	t.Run("IBM deletes the cluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "deleting"}, nil)
		_, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeTrue())
	})
	t.Run("the pool is gone and the token is released", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.others = []client.Object{heldLease(DisruptionTokenHolder(f.pool))}
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
			Phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval, WorkerIDs: []string{"w0"}}
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(Done()))
		g.Expect(remove).To(BeTrue())
		g.Expect(f.lease()).To(BeNil())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	})
	t.Run("a deleted pool is gone", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{ourPool(WorkerStateDeleted)}, nil)
		_, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeTrue())
	})
	t.Run("a_pool_of_another_flavor_is_left_alone", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		foreign := ourPool("")
		foreign.Flavor = "cx2.2x4"
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{foreign}, nil)
		_, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeTrue())
		g.Expect(f.events()).To(ConsistOf(ContainSubstring("Warning WorkerPoolNotRemoved IBM worker pool workers stays: it has flavor cx2.2x4")))
		g.Expect(f.calls.all()).To(BeEmpty())
	})
	t.Run("a failed listing is classified", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, apiError(kubernetesservice.OpListWorkerPools, http.StatusForbidden, ""))
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeFalse())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.deleting().Reason).To(Equal(MachinePoolIBMAPIErrorReason))
		g.Expect(f.deleting().Status).To(Equal(metav1.ConditionTrue))
	})
}

func TestMachinePoolScopeDeleteDrainsThenRemoves(t *testing.T) {
	t.Run("the_pool_is_drained_then_removed", testScopeDeleteDrainsThenRemoves)
}

// testScopeDeleteDrainsThenRemoves follows one stand-alone pool deletion reconcile by reconcile.
func testScopeDeleteDrainsThenRemoves(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	reconcile := func() (Outcome, bool) { return f.scope().ReconcileDelete(context.Background()) }

	// 1. Gates pass, the token is taken, every Node of the pool is cordoned before the first eviction.
	f.expectIBM(ourPool(""))
	outcome, remove := reconcile()
	g.Expect(remove).To(BeFalse())
	g.Expect(outcome).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
	g.Expect(f.lease().Spec.HolderIdentity).To(Equal(ptr.To(DisruptionTokenHolder(f.pool))))
	g.Expect(f.calls.all()).To(Equal([]string{"list nodes",
		"cordon 10.0.0.1", "cordon 10.0.0.2", "cordon 10.0.0.3", "cordon 10.0.0.4",
		"list pods 10.0.0.1", "evict apps/web-w0", "list pods 10.0.0.2", "evict apps/web-w1",
		"list pods 10.0.0.3", "evict apps/web-w2", "list pods 10.0.0.4", "evict apps/web-w3"}),
		"every Node cordoned before the first eviction; the other pool's Nodes untouched")
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{
		Operation: infrav1.WorkerDisruptionOperationDeletePool, Phase: infrav1.WorkerDisruptionPhaseDraining,
		WorkerIDs: []string{"w0", "w1", "w2", "w3"}, DrainStartTime: metav1.NewTime(poolTestNow),
	}))
	g.Expect(f.deleting().Reason).To(Equal(string(ReasonDrainingNodes)))
	draining := condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition)
	g.Expect(draining.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(draining.Reason).To(Equal(string(ReasonDrainingNode)))
	g.Expect(draining.Message).To(ContainSubstring("Node 10.0.0.1: Drain not completed yet"))
	g.Expect(f.events()).To(ConsistOf(ContainSubstring("Normal DrainStarted cordoning 4 Nodes of pool workers")))

	// 2. The pods are gone: on to the volume detach wait.
	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	g.Expect(reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	g.Expect(f.events()).To(ConsistOf(ContainSubstring("Normal DrainCompleted the 4 Nodes of pool workers are drained")))

	// 3. Volumes detached and the pre-call gate passes: status is persisted, then RemoveWorkerPool is sent once.
	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).DoAndReturn(func(context.Context, string, string) error {
		g.Expect(f.persisted).To(HaveLen(1), "status is persisted before the call")
		g.Expect(f.persisted[0].Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(f.persisted[0].IBMRequestTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
		return nil
	})
	outcome, remove = reconcile()
	g.Expect(remove).To(BeFalse())
	g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
	g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Status).To(Equal(metav1.ConditionFalse))
	g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}), "the Nodes stay cordoned")

	// 4. IBM removes the pool: wait, no second call.
	f.clock.Step(30 * time.Second)
	f.expectIBM(ourPool("deleting"))
	g.Expect(reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))

	// 5. The pool is gone: the token is released and the finalizer may go.
	f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
	outcome, remove = reconcile()
	g.Expect(outcome).To(Equal(Done()))
	g.Expect(remove).To(BeTrue())
	g.Expect(f.lease()).To(BeNil())
	g.Expect(f.resolver.holders).To(HaveEach(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{})), "every holder is the InfraCluster")
}

func TestMachinePoolScopeDeleteGates(t *testing.T) {
	testCases := []struct {
		name       string
		modify     func(f *deleteFixture)
		wantReason DisruptionReason
	}{
		{name: "the workload cluster is not connected", wantReason: ReasonWaitingForWorkloadCluster,
			modify: func(f *deleteFixture) { f.clusterCache = clustercache.NewFakeEmptyClusterCache() }},
		{name: "IBM's minimum would be broken", wantReason: ReasonWaitingForClusterMinimum,
			modify: func(f *deleteFixture) { f.workers = f.workers[:4] }},
		{name: "the cluster state forbids the removal", wantReason: ReasonWaitingForClusterState,
			modify: func(f *deleteFixture) { f.clusterState = "unavailable" }},
		{name: "a worker of the cluster is changing", wantReason: ReasonWaitingForSettledCluster,
			modify: func(f *deleteFixture) { f.workers[5].ActualState = "provisioning" }},
		{name: "a_provision_pending_worker_of_another_pool_first_seen_less_than_WorkerStuckAfter_ago_blocks",
			wantReason: ReasonWaitingForSettledCluster, modify: func(f *deleteFixture) {
				f.workers = append(f.workers, otherPoolWorker("provision_pending"))
				f.transitionalSince = map[string]time.Time{"o2": poolTestNow.Add(-WorkerStuckAfter + time.Minute)}
			}},
		{name: "a_provision_pending_worker_the_controller_has_not_timed_blocks", wantReason: ReasonWaitingForSettledCluster,
			modify: func(f *deleteFixture) { f.workers = append(f.workers, otherPoolWorker("provision_pending")) }},
		{name: "another pool holds the token", wantReason: ReasonWaitingForDisruptionToken,
			modify: func(f *deleteFixture) {
				other := poolObject("other", "other")
				f.others = []client.Object{other, heldLease(DisruptionTokenHolder(other))}
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newDeleteFixture(t, nil)
			f.clusterState = "normal"
			tc.modify(f)
			f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: f.clusterState}, nil)
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{ourPool("")}, nil)
			f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(f.workers, nil)
			outcome, remove := f.scope().ReconcileDelete(context.Background())
			g.Expect(remove).To(BeFalse())
			g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(tc.wantReason))))
			g.Expect(f.deleting().Reason).To(Equal(string(tc.wantReason)))
			g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}), "nothing is recorded before the first cordon")
			g.Expect(f.cordoned()).To(BeEmpty())
		})
	}
}

// otherPoolWorker is the worker o2 of another pool in state; ready capacity elsewhere keeps IBM's minimum, so only the
// settled-cluster rule can hold the deletion for it.
func otherPoolWorker(state string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: "o2", PoolID: "other-pool", Zone: "us-south-1", ActualState: state}
}

// TestMachinePoolScopeDeleteStuckWorker checks that both disruption gates of a pool deletion read the controller's
// first-seen times (MachinePoolScopeInput.TransitionalSince): a worker IBM has left transitional for longer than
// WorkerStuckAfter no longer holds the deletion, and the condition names it.
func TestMachinePoolScopeDeleteStuckWorker(t *testing.T) {
	stuckSince := map[string]time.Time{"o2": poolTestNow.Add(-WorkerStuckAfter - time.Minute)}
	t.Run("a_provision_pending_worker_of_another_pool_first_seen_more_than_WorkerStuckAfter_ago_no_longer_blocks", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.workers = append(f.workers, otherPoolWorker("provision_pending"))
		f.transitionalSince = stuckSince
		f.expectIBM(ourPool(""))
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeFalse())
		g.Expect(outcome).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
		g.Expect(f.deleting().Reason).To(Equal(string(ReasonDrainingNodes)))
		g.Expect(f.deleting().Message).To(Equal("cordoning every victim Node, then evicting their pods; WorkerOperationStuck: " +
			"IBM has been changing worker o2 for more than 60 minutes; treated like a failed worker, it no longer blocks worker removals"))
		g.Expect(f.cordoned()).To(HaveLen(4))
	})
	for _, tc := range []struct {
		name      string
		since     map[string]time.Time
		wantCall  bool
		wantPhase infrav1.WorkerDisruptionPhase
	}{
		{name: "the_pre_call_gate_lets_a_stuck_worker_pass", since: stuckSince, wantCall: true,
			wantPhase: infrav1.WorkerDisruptionPhaseWaitingForRemoval},
		{name: "the_pre_call_gate_holds_for_a_worker_changing_for_less_than_WorkerStuckAfter",
			since: map[string]time.Time{"o2": poolTestNow.Add(-time.Minute)}, wantPhase: infrav1.WorkerDisruptionPhaseWaitingForIBM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newDeleteFixture(t, nil)
			f.others = append(f.others, heldLease(DisruptionTokenHolder(f.pool)))
			f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
				Phase: infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, WorkerIDs: []string{"w0", "w1", "w2", "w3"},
				DrainStartTime: metav1.NewTime(poolTestNow.Add(-10 * time.Minute))}
			drainedAll(t, f)
			f.workers = append(f.workers, otherPoolWorker("provisioning"))
			f.transitionalSince = tc.since
			f.expectIBM(ourPool(""))
			if tc.wantCall {
				f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(nil)
			}
			_, remove := f.scope().ReconcileDelete(context.Background())
			g.Expect(remove).To(BeFalse())
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(tc.wantPhase))
			if !tc.wantCall {
				g.Expect(f.deleting().Reason).To(Equal(string(ReasonWaitingForSettledCluster)))
				g.Expect(f.deleting().Message).To(ContainSubstring("worker o2 is provisioning"))
			}
		})
	}
	t.Run("the_deletion_hands_its_listing_back_for_the_first_seen_map", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.workers = append(f.workers, otherPoolWorker("provision_pending"))
		f.expectIBM(ourPool(""))
		scope := f.scope()
		scope.ReconcileDelete(context.Background())
		listed, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeTrue())
		g.Expect(listed).To(Equal(f.workers))
	})
	t.Run("the skip path of a Cluster deletion lists nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
		f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
		scope := f.scope()
		_, remove := scope.ReconcileDelete(context.Background())
		g.Expect(remove).To(BeTrue())
		_, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeFalse())
	})
}

func TestMachinePoolScopeDeleteInFlight(t *testing.T) {
	inPhase := func(f *deleteFixture, op infrav1.WorkerDisruptionOperation, phase infrav1.WorkerDisruptionPhase, workerIDs ...string) {
		f.others = append(f.others, heldLease(DisruptionTokenHolder(f.pool)))
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: op, Phase: phase, WorkerIDs: workerIDs,
			DrainStartTime: metav1.NewTime(poolTestNow.Add(-10 * time.Minute))}
	}
	t.Run("E1437_holds_with_the_Nodes_cordoned", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, "w0", "w1", "w2", "w3")
		drainedAll(t, f)
		e1437 := apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode)
		refusal := "IBM refused the removal because the cluster would fall below its minimum workers (E1437); the drained Nodes stay cordoned"
		holds := func(when string) {
			t.Helper()
			outcome, remove := f.scope().ReconcileDelete(context.Background())
			g.Expect(remove).To(BeFalse())
			g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))), when)
			g.Expect(f.deleting().Reason).To(Equal(string(ReasonWaitingForClusterMinimum)), when+": never RemovingWorkerPool in between")
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
			g.Expect(f.cordoned()).To(HaveLen(4))
		}

		// 1. IBM refuses: hold in WaitingForIBM with the Nodes cordoned.
		f.expectIBM(ourPool(""))
		f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(e1437)
		holds("the refusal")
		g.Expect(f.deleting().Message).To(Equal(refusal))
		g.Expect(f.persisted).To(HaveLen(1))
		g.Expect(f.persisted[0].IBMRequestTime.Time).To(Equal(poolTestNow), "the call time is persisted before the call")
		g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow), "and kept: it rate-limits the retry")

		// 2. The reconcile that the pool's own status patch triggers at once sends no second call, and neither does one
		//    within DisruptionSettleDelay; the hold stays instead of RemovingWorkerPool ("the IBM call was sent at ...").
		f.expectIBM(ourPool(""))
		holds("the reconcile right after the refusal")
		g.Expect(f.deleting().Message).To(Equal(refusal))
		f.clock.Step(2 * time.Minute)
		f.expectIBM(ourPool(""))
		holds("within the settle delay")

		// 3. After the settle delay the call is retried while CAPIBM's own gate passes ...
		f.clock.Step(2 * time.Minute)
		f.expectIBM(ourPool(""))
		f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(e1437)
		holds("the retry")
		g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow.Add(4 * time.Minute)))

		// 4. ... and held without a call once that gate fails.
		f.clock.Step(4 * time.Minute)
		f.workers[4].HealthState, f.workers[5].HealthState = "critical", "critical"
		f.expectIBM(ourPool(""))
		holds("CAPIBM's own gate")
		g.Expect(f.deleting().Message).To(ContainSubstring("below IBM's minimum"))
	})
	t.Run("a refused removal of an autoscaled pool holds as ExternallyAutoscaled", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, "w0", "w1", "w2", "w3")
		drainedAll(t, f)
		autoscaled := ourPool("")
		autoscaled.AutoscaleEnabled = true
		f.expectIBM(autoscaled)
		f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(&kubernetesservice.APIError{
			Operation: kubernetesservice.OpRemoveWorkerPool, StatusCode: http.StatusForbidden, Description: "Forbidden. The request will not be permitted."})
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonExternallyAutoscaled))))

		f.expectIBM(autoscaled)
		outcome, _ = f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonExternallyAutoscaled))), "no second call, the hold stays")
		g.Expect(f.cordoned()).To(HaveLen(4))
	})
	t.Run("the autoscaled-pool 403 on a pool that is not autoscaled needs a role", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, "w0", "w1", "w2", "w3")
		drainedAll(t, f)
		f.expectIBM(ourPool(""))
		f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(&kubernetesservice.APIError{
			Operation: kubernetesservice.OpRemoveWorkerPool, StatusCode: http.StatusForbidden, Description: "Forbidden. The request will not be permitted."})
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal), "a missing role is PermissionDenied, not a transient failure")
		g.Expect(f.deleting().Reason).To(Equal(MachinePoolIBMAPIErrorReason))
		g.Expect(f.deleting().Message).To(ContainSubstring("grant it " + poolKubernetesServiceRoles))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
	})
	t.Run("a refused removal is classified and keeps the phase", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, "w0", "w1", "w2", "w3")
		// An earlier call was refused with E1437.
		f.pool.Status.Conditions = append(f.pool.Status.Conditions, metav1.Condition{Type: infrav1.IBMCloudManagedMachinePoolDeletingCondition,
			Status: metav1.ConditionTrue, Reason: string(ReasonWaitingForClusterMinimum), Message: "IBM refused the removal"})
		drainedAll(t, f)
		f.expectIBM(ourPool(""))
		f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).
			Return(apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusInternalServerError, ""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))

		// The outcome of a 5xx is unknown, so the next reconcile waits for IBM's listing as after any call; the earlier
		// refusal's hold is not brought back.
		f.expectIBM(ourPool(""))
		outcome, _ = f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
		g.Expect(f.deleting().Message).To(HavePrefix("the IBM call was sent at 2026-11-16T12:00:00Z"))
	})
	t.Run("no call when status cannot be persisted", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, "w0", "w1", "w2", "w3")
		drainedAll(t, f)
		f.persistErr = errors.New("conflict")
		f.expectIBM(ourPool(""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
	})
	t.Run("the workload cluster down holds before the call", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, "w0", "w1", "w2", "w3")
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expectIBM(ourPool(""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))), "no RemoveWorkerPool")
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Reason).To(Equal(string(ReasonWaitingForWorkloadCluster)))
	})
	t.Run("a replace of the pool is handed over to the deletion", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		inPhase(f, infrav1.WorkerDisruptionOperationReplace, infrav1.WorkerDisruptionPhaseWaitingForRemoval, "w0")
		f.pool.Status.Disruption.IBMRequestTime = metav1.NewTime(poolTestNow.Add(-5 * time.Minute))
		f.expectIBM(ourPool(""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{
			Operation: infrav1.WorkerDisruptionOperationDeletePool, Phase: infrav1.WorkerDisruptionPhaseDraining,
			WorkerIDs:      []string{"w0", "w1", "w2", "w3"},
			DrainStartTime: metav1.NewTime(poolTestNow.Add(-10 * time.Minute)), IBMRequestTime: metav1.NewTime(poolTestNow.Add(-5 * time.Minute)),
		}), "every cordon, the drain start and the earlier call are kept")
		g.Expect(f.cordoned()).To(HaveLen(4))
	})
	t.Run("a drain the Node markers show resumes after a lost status", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.others = append(f.others, heldLease(DisruptionTokenHolder(f.pool)))
		f.clock.Step(-5 * time.Minute)
		cordonAll(t, f)
		f.clock.Step(5 * time.Minute)
		f.expectIBM(ourPool(""))
		f.scope().ReconcileDelete(context.Background())
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
		g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow.Add(-5*time.Minute)), "the start comes from the Node markers")
		g.Expect(f.events()).To(BeEmpty(), "a resumed drain is not a new one")
	})
	t.Run("the_time_limit_stops_evicting_and_the_removal_proceeds", func(t *testing.T) {
		g := NewWithT(t)
		refusals := map[string]error{"web-w0": pdbRefusal(), "web-w1": pdbRefusal(), "web-w2": pdbRefusal(), "web-w3": pdbRefusal()}
		f := newDeleteFixture(t, refusals)
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		f.expectIBM(ourPool(""))
		f.scope().ReconcileDelete(context.Background())
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Reason).To(Equal(string(ReasonDrainBlocked)))
		g.Expect(f.events()).To(ContainElement(HavePrefix("Warning DrainBlocked")))
		f.calls.all()

		f.clock.Step(61 * time.Second)
		f.expectIBM(ourPool(""))
		f.scope().ReconcileDelete(context.Background())
		g.Expect(f.calls.count("evict")).To(BeZero(), "no eviction after the limit")
		g.Expect(f.calls.count("list pods")).To(Equal(4), "one RemainingPods listing per Node")
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
		events := f.events()
		g.Expect(events).To(ContainElement(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 60s")))
		g.Expect(events).To(ContainElement(HavePrefix("Normal DrainCompleted")))

		f.clock.Step(20 * time.Second)
		f.calls.all()
		f.expectIBM(ourPool(""))
		f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(nil)
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
		g.Expect(f.calls.count("list pods")).To(BeZero(), "timed-out Nodes need no listing")
	})
}

// drainedAll cordons the Nodes of the pool's workers and removes their pods, as earlier reconciles of this pool's
// deletion would have.
func drainedAll(t *testing.T, f *deleteFixture) {
	t.Helper()
	cordonAll(t, f)
	for _, id := range []string{"w0", "w1", "w2", "w3"} {
		NewWithT(t).Expect(f.workload.Delete(context.Background(), workloadPod("web-"+id, ""))).To(Succeed())
	}
	f.calls.all()
}

// cordonAll cordons the Nodes of the pool's workers as an earlier reconcile of this pool would have.
func cordonAll(t *testing.T, f *deleteFixture) {
	t.Helper()
	helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	nodes := VictimNodes(readNodes(t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0", "w1", "w2", "w3"})
	for i := range nodes {
		NewWithT(t).Expect(helper.Cordon(context.Background(), &nodes[i], drain.CordonOptions{Owner: DrainOwner(f.pool)})).To(Succeed())
	}
	f.calls.all()
}

// TestMachinePoolScopeDeleteWithoutTheWorkloadCluster deletes a pool whose workload cluster is not connected (a
// private-only cluster, an expired admin certificate, a broken API server): no RemoveWorkerPool while the drain or the
// detach wait is unknown, until a set time limit runs out and then allows the call (as for a Cluster API Machine), or
// both waits are excluded (the escape hatch for an unreachable workload cluster).
func TestMachinePoolScopeDeleteWithoutTheWorkloadCluster(t *testing.T) {
	// midDrain is a deletion in phase Draining whose drain started two hours ago, while the workload cluster is gone.
	midDrain := func(t *testing.T) *deleteFixture {
		t.Helper()
		f := newDeleteFixture(t, nil)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.others = append(f.others, heldLease(DisruptionTokenHolder(f.pool)))
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
			Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0", "w1", "w2", "w3"},
			DrainStartTime: metav1.NewTime(poolTestNow.Add(-2 * time.Hour))}
		return f
	}
	detachLimits := []struct {
		name   string
		modify func(f *deleteFixture)
	}{
		{name: "excluded", modify: func(f *deleteFixture) {
			f.pool.Annotations = map[string]string{clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: ""}
		}},
		{name: "timed out", modify: func(f *deleteFixture) {
			f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](60)
		}},
	}
	for _, tc := range detachLimits {
		t.Run("the drain time limit runs out and allows the call, detach wait "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := midDrain(t)
			f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
			tc.modify(f)
			f.expectIBM(ourPool(""))
			outcome, _ := f.scope().ReconcileDelete(context.Background())
			g.Expect(outcome).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
			g.Expect(f.events()).To(ConsistOf(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 60s "+
				"(MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed since 2026-11-16T10:00:00Z while "+
				"the workload cluster is not connected")), "no DrainCompleted: no Node was seen drained")

			f.clock.Step(20 * time.Second)
			f.expectIBM(ourPool(""))
			f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(nil)
			outcome, _ = f.scope().ReconcileDelete(context.Background())
			g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
			g.Expect(f.calls.all()).To(BeEmpty(), "no workload-cluster call")
		})
	}
	t.Run("without a drain time limit the drain waits for the workload cluster", func(t *testing.T) {
		g := NewWithT(t)
		f := midDrain(t)
		f.expectIBM(ourPool(""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))), "no RemoveWorkerPool")
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	})
	t.Run("without a detach limit the detach wait holds after the drain timed out (CAPI parity)", func(t *testing.T) {
		g := NewWithT(t)
		f := midDrain(t)
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach
		f.expectIBM(ourPool(""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))), "no RemoveWorkerPool")
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Reason).To(Equal(string(ReasonWaitingForWorkloadCluster)))
	})
	t.Run("both exclusions need no workload cluster from the first step", testDeleteBothExclusionsWithoutTheWorkloadCluster)
	t.Run("the drain exclusion alone still waits for the workload cluster before the first cordon", func(t *testing.T) {
		g := NewWithT(t)
		f := newDeleteFixture(t, nil)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: ""}
		f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](60)
		f.expectIBM(ourPool(""))
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))))
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.lease()).To(BeNil())
	})
}

// testDeleteBothExclusionsWithoutTheWorkloadCluster deletes a pool with both exclusion annotations while its workload
// cluster was never reachable: nothing is cordoned, evicted or waited for, so the removal goes from the gates to
// RemoveWorkerPool without it.
func testDeleteBothExclusionsWithoutTheWorkloadCluster(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.clusterCache = clustercache.NewFakeEmptyClusterCache()
	f.pool.Annotations = map[string]string{
		clusterv1.ExcludeNodeDrainingAnnotation:            "",
		clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: "",
	}
	reconcile := func() Outcome {
		outcome, remove := f.scope().ReconcileDelete(context.Background())
		g.Expect(remove).To(BeFalse())
		return outcome
	}

	f.expectIBM(ourPool(""))
	g.Expect(reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
	g.Expect(f.lease().Spec.HolderIdentity).To(Equal(ptr.To(DisruptionTokenHolder(f.pool))), "the token still applies")
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))

	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	g.Expect(reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))

	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(nil)
	g.Expect(reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
	g.Expect(f.events()).To(BeEmpty(), "no drain ran, so no drain event")
	g.Expect(f.calls.all()).To(BeEmpty(), "no workload-cluster call")
	g.Expect(f.cordoned()).To(BeEmpty())
}

func TestMachinePoolScopeDeleteExcludedDrainDetachLimit(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: ""}
	f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](60)
	g.Expect(f.workload.Create(context.Background(), attachedVolume("10.0.0.1"))).To(Succeed())
	reconcile := func() Outcome {
		outcome, _ := f.scope().ReconcileDelete(context.Background())
		return outcome
	}

	// Nothing is cordoned, so the detach limit counts from status.disruption.drainStartTime. Neither the conditions
	// nor the events say "cordon"; they say that the drain is excluded.
	f.expectIBM(ourPool(""))
	reconcile()
	g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow))
	g.Expect(f.cordoned()).To(BeEmpty())
	excluded := "the drain is excluded (machine.cluster.x-k8s.io/exclude-node-draining): CAPIBM leaves the victim Nodes " +
		"schedulable and evicts no pod"
	g.Expect(f.deleting().Message).To(Equal(excluded))
	g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Message).To(Equal(excluded))
	g.Expect(f.events()).To(Equal([]string{"Normal DrainStarted the drain of the 4 Nodes of pool workers is excluded " +
		"(machine.cluster.x-k8s.io/exclude-node-draining): CAPIBM leaves them schedulable and evicts no pod"}))
	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	reconcile()
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	g.Expect(f.events()).To(Equal([]string{"Normal DrainCompleted the 4 Nodes of pool workers are not drained: " +
		"machine.cluster.x-k8s.io/exclude-node-draining is set, so CAPIBM evicts no more pods"}))

	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	g.Expect(reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))), "40 s: the volume is waited for")
	draining := condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition)
	g.Expect(draining.Reason).To(Equal(string(ReasonWaitingForVolumeDetach)))
	g.Expect(draining.Message).To(ContainSubstring("pv-data"))

	f.clock.Step(20 * time.Second)
	f.expectIBM(ourPool(""))
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(nil)
	g.Expect(reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))), "60 s: the limit ran out")
	g.Expect(f.cordoned()).To(BeEmpty())
}

func TestMachinePoolScopeDeleteEmptyPool(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.workers = f.workers[4:]
	f.expectIBM(ourPool(""))
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).Return(nil)
	outcome, remove := f.scope().ReconcileDelete(context.Background())
	g.Expect(remove).To(BeFalse())
	g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
	g.Expect(f.persisted).To(HaveLen(1))
	g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow))
	g.Expect(f.lease()).To(BeNil(), "a pool without workers takes no token")
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))

	f.clock.Step(time.Minute)
	f.expectIBM(ourPool(""))
	outcome, _ = f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonRemovingWorkerPool))), "no second call within the settle delay")

	f.expectIBM(ourPool("deleting"))
	outcome, _ = f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonRemovingWorkerPool))))
}

func TestMachinePoolScopeDeleteEmptyPoolGates(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.workers = nil
	f.expectIBM(ourPool(""))
	outcome, _ := f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))), "IBM's minimum is still checked")

	f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "scaled_down"}, nil)
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{ourPool("")}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(nil, nil)
	outcome, _ = f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterState))))
}

func TestMachinePoolScopeDeleteReleasesAnIdleToken(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.others = []client.Object{heldLease(DisruptionTokenHolder(f.pool))}
	f.ibm.EXPECT().GetCluster(gomock.Any(), poolTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "unavailable"}, nil)
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{ourPool("")}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(f.workers, nil)
	outcome, _ := f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterState))))
	g.Expect(f.lease()).To(BeNil(), "a removal that has not cordoned anything does not keep the Cluster's token")
}

func TestMachinePoolScopeDeleteEmptyPoolAtTheMinimum(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.workers = f.workers[4:]
	f.expectIBM(ourPool(""))
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).
		Return(apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode))
	outcome, remove := f.scope().ReconcileDelete(context.Background())
	g.Expect(remove).To(BeFalse())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
	g.Expect(f.deleting().Reason).To(Equal(string(ReasonWaitingForClusterMinimum)))
	g.Expect(f.persisted).To(HaveLen(1), "lastDisruptionTime is persisted before the call")
	g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow), "and kept: it rate-limits the retry")

	// The reconcile right after the refusal sends no second call and keeps WaitingForClusterMinimum instead of
	// RemovingWorkerPool for the settle delay; after it the call is retried.
	f.expectIBM(ourPool(""))
	outcome, _ = f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
	g.Expect(f.deleting().Message).To(ContainSubstring("(E1437)"))
	f.clock.Step(4 * time.Minute)
	f.expectIBM(ourPool(""))
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID).
		Return(apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode))
	outcome, _ = f.scope().ReconcileDelete(context.Background())
	g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
	g.Expect(f.deleting().Reason).To(Equal(string(ReasonWaitingForClusterMinimum)))
}

func TestPoolRemovalHandOverWithNothingCordoned(t *testing.T) {
	g := NewWithT(t)
	f := newDeleteFixture(t, nil)
	f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace,
		Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0"}}
	removal := &poolRemoval{s: f.scope(), victims: []string{"w0", "w1"}}
	removal.handOver("")
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	removal.handOver(infrav1.WorkerDisruptionPhaseDraining)
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
		Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0", "w1"}}))
}
