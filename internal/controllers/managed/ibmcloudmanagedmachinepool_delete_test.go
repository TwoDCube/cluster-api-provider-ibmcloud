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
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"

	. "github.com/onsi/gomega"
)

// The pool deletion tests call Reconcile on a deleting pool: the skip paths (a deleting Cluster, a clusterctl move)
// remove the finalizer with zero IBM and zero workload-cluster calls; a stand-alone deletion drains every Node before
// RemoveWorkerPool, reports Deleting=True with its reason and removes the finalizer once IBM no longer lists the pool.
// The envtest cases use a real API server for the finalizer and a real ClusterCache for the connect event.

// noWorkloadCallsClusterCache is a ClusterCache for paths that must make no workload-cluster call.
type noWorkloadCallsClusterCache struct {
	clustercache.ClusterCache
	t *testing.T
}

// GetClient implements clustercache.ClusterCache; it fails the test.
func (c noWorkloadCallsClusterCache) GetClient(context.Context, client.ObjectKey) (client.Client, error) {
	c.t.Errorf("ClusterCache.GetClient called, but this path must make no workload-cluster call")
	return nil, clustercache.ErrClusterNotConnected
}

// orderedCalls records the cordons and evictions of the workload cluster, in order.
type orderedCalls struct {
	mu    sync.Mutex
	calls []string
}

// add records one call.
func (o *orderedCalls) add(call string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, call)
}

// all returns the calls so far.
func (o *orderedCalls) all() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.calls...)
}

// deletingMachinePool marks the fixture's pool as deleting and gives it a connected workload cluster with one Node and
// one pod per worker w0..w3; it returns the recorder of the workload calls.
func (f *machinePoolFixture) deletingMachinePool(t *testing.T) *orderedCalls {
	t.Helper()
	f.pool.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow.Add(-time.Minute)))
	f.pool.Status.PoolID = mpTestPoolID
	calls := &orderedCalls{}
	objs := make([]client.Object, 0, 8)
	for i, id := range []string{"w0", "w1", "w2", "w3"} {
		name := "10.0.0." + string(rune('1'+i))
		objs = append(objs,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{ProviderID: "ibm://" + mpTestAccount + "///" + mpTestClusterID + "/" + id}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web-" + id}, Spec: corev1.PodSpec{NodeName: name}})
	}
	workload := fake.NewClientBuilder().WithScheme(machinePoolScheme(t)).WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				calls.add("cordon " + obj.GetName())
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourceCreate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
				calls.add("evict " + obj.GetName())
				return c.SubResource(subResource).Create(ctx, obj, sub, opts...)
			},
		}).Build()
	f.clusterCache = clustercache.NewFakeClusterCache(workload, client.ObjectKey{Namespace: mpTestNamespace, Name: mpTestCluster})
	return calls
}

// expectDeleteObservation expects the IBM reads of one delete reconcile: the cluster, the pools (ours in state
// poolState, or none when poolState is "gone") and four ready workers of ours plus two of another pool.
func (f *machinePoolFixture) expectDeleteObservation(poolState string) {
	f.ibm.EXPECT().GetCluster(gomock.Any(), mpTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
	if poolState == "gone" {
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return(nil, nil)
		return
	}
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{{
		ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2, ActualState: poolState,
	}}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(mpTestWorkers(), nil)
}

// mpTestWorkers are four ready workers w0..w3 of the test pool and two of another pool.
func mpTestWorkers() []kubernetesservice.Worker {
	workers := make([]kubernetesservice.Worker, 0, 6)
	for i, id := range []string{"w0", "w1", "w2", "w3", "o0", "o1"} {
		poolID := mpTestPoolID
		if i > 3 {
			poolID = "other-pool"
		}
		workers = append(workers, kubernetesservice.Worker{ID: id, PoolID: poolID, ActualState: "deployed", HealthState: "normal",
			KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"})
	}
	return workers
}

func TestIBMCloudManagedMachinePoolReconcileDeleteSkips(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(f *machinePoolFixture)
	}{
		{name: "the_Cluster_is_being_deleted", modify: func(f *machinePoolFixture) {
			f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
			f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
		}},
		{name: "delete_for_move", modify: func(f *machinePoolFixture) {
			f.pool.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
		}},
		{name: "the_Cluster_is_being_deleted_makes_no_workload_call", modify: func(f *machinePoolFixture) {
			f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
			f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
			f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
				Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newMachinePoolFixture(t)
			f.pool.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
			tc.modify(f)
			r := f.reconciler()
			r.Resolver, r.ClientBuilder, r.ClusterCache = failIfCalledResolver{t: t}, failIfCalledBuilder{t: t}, noWorkloadCallsClusterCache{t: t}
			result, err := f.reconcile(r)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result).To(Equal(ctrl.Result{}))
			err = f.mgmt.Get(context.Background(), client.ObjectKeyFromObject(f.pool), &infrav1.IBMCloudManagedMachinePool{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the finalizer is removed, so the object is gone")
		})
	}
}

func TestIBMCloudManagedMachinePoolReconcileDeletePaused(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	f.pool.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
	f.cluster.Spec.Paused = ptr.To(true)
	r := f.reconciler()
	r.Resolver, r.ClientBuilder, r.ClusterCache = failIfCalledResolver{t: t}, failIfCalledBuilder{t: t}, noWorkloadCallsClusterCache{t: t}
	_, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	stored := f.stored()
	g.Expect(stored.Finalizers).To(ConsistOf(infrav1.IBMCloudManagedMachinePoolFinalizer), "a paused pool keeps its finalizer")
	g.Expect(conditions.IsTrue(stored, clusterv1.PausedCondition)).To(BeTrue())
}

func TestIBMCloudManagedMachinePoolReconcileDeleteForeignFinalizer(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	f.pool.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
	f.pool.Finalizers = []string{"example.com/other"}
	r := f.reconciler()
	r.Resolver, r.ClientBuilder = failIfCalledResolver{t: t}, failIfCalledBuilder{t: t}
	_, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.stored().Finalizers).To(ConsistOf("example.com/other"), "an object this controller never finalized is left alone")
}

func TestIBMCloudManagedMachinePoolReconcileDelete(t *testing.T) {
	t.Run("every_Node_is_cordoned_before_the_first_eviction_and_the_pool_removed_after_the_drain", testDeleteDrainsThenRemoves)
	t.Run("the_delete_path_resolves_from_the_IBMCloudManagedCluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.deletingMachinePool(t)
		f.expectDeleteObservation("")
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		holders := f.resolver.Holders()
		g.Expect(holders).To(HaveLen(1))
		g.Expect(holders[0]).To(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{}))
		g.Expect(holders[0].GetName()).To(Equal(mpTestCluster))
	})
	t.Run("a_deleting_pool_reports_Deleting_True_with_its_reason", testDeleteWaitsForTheWorkloadCluster)
}

// testDeleteDrainsThenRemoves follows one stand-alone pool deletion from the first cordon to the finalizer removal.
func testDeleteDrainsThenRemoves(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	calls := f.deletingMachinePool(t)
	r := f.reconciler()

	// 1. Every Node is cordoned before the first eviction.
	f.expectDeleteObservation("")
	result, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(BeNumerically("~", 20*time.Second, 3*time.Second), "drainRetryInterval")
	g.Expect(calls.all()).To(Equal([]string{"cordon 10.0.0.1", "cordon 10.0.0.2", "cordon 10.0.0.3", "cordon 10.0.0.4",
		"evict web-w0", "evict web-w1", "evict web-w2", "evict web-w3"}))
	stored := f.stored()
	g.Expect(stored.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	deleting := conditions.Get(stored, infrav1.IBMCloudManagedMachinePoolDeletingCondition)
	g.Expect(deleting.Status).To(Equal(metav1.ConditionTrue), "Deleting is True with its reason while deleting")
	g.Expect(deleting.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolDeletingDrainingNodesReason))
	g.Expect(conditions.IsTrue(stored, infrav1.IBMCloudManagedMachinePoolDrainingCondition)).To(BeTrue())

	// 2. Drained: wait for volume detach.
	f.clock.Step(20 * time.Second)
	f.expectDeleteObservation("")
	_, err = f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))

	// 3. RemoveWorkerPool only after the drain, with phase WaitingForIBM already persisted.
	f.clock.Step(20 * time.Second)
	f.expectDeleteObservation("")
	f.ibm.EXPECT().RemoveWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID).DoAndReturn(func(context.Context, string, string) error {
		persisted := f.stored().Status.Disruption
		g.Expect(persisted.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(persisted.IBMRequestTime.IsZero()).To(BeFalse())
		return nil
	})
	_, err = f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	g.Expect(conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolDeletingCondition).Reason).
		To(Equal(infrav1.IBMCloudManagedMachinePoolDeletingRemovingWorkerPoolReason))

	// 4. IBM no longer lists the pool: the token goes, then the finalizer, then the object.
	f.clock.Step(30 * time.Second)
	f.expectDeleteObservation("gone")
	result, err = f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result).To(Equal(ctrl.Result{}))
	err = f.mgmt.Get(context.Background(), client.ObjectKeyFromObject(f.pool), &infrav1.IBMCloudManagedMachinePool{})
	g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
	lease := &coordinationv1.Lease{}
	err = f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: mpTestCluster + "-ibm-worker-disruption"}, lease)
	g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the disruption token is released")
}

// testDeleteWaitsForTheWorkloadCluster deletes a pool whose workload cluster is not connected: Deleting is True with the
// gate's reason, and nothing is removed.
func testDeleteWaitsForTheWorkloadCluster(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	f.deletingMachinePool(t)
	f.clusterCache = clustercache.NewFakeEmptyClusterCache()
	f.expectDeleteObservation("")
	result, err := f.reconcile(f.reconciler())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(BeNumerically("~", 2*time.Minute, 15*time.Second), "guardedPoll is the backstop of the connect event")
	deleting := conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolDeletingCondition)
	g.Expect(deleting.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(deleting.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason), "no RemoveWorkerPool")
}

// expectDeleteObservationWith expects the IBM reads of one delete reconcile of the steady pool, whose IBM cluster also
// lists extra.
func (f *machinePoolFixture) expectDeleteObservationWith(extra ...kubernetesservice.Worker) {
	f.ibm.EXPECT().GetCluster(gomock.Any(), mpTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil)
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{{
		ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2,
	}}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(append(mpTestWorkers(), extra...), nil)
}

// deletingReason returns the reason and message of the stored pool's Deleting condition.
func (f *machinePoolFixture) deletingReason() (string, string) {
	f.t.Helper()
	deleting := conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolDeletingCondition)
	if deleting == nil {
		return "", ""
	}
	return deleting.Reason, deleting.Message
}

// restarted returns a reconciler with r's dependencies and an empty first-seen map, as after a controller restart.
func restarted(r *IBMCloudManagedMachinePoolReconciler) *IBMCloudManagedMachinePoolReconciler {
	return &IBMCloudManagedMachinePoolReconciler{Client: r.Client, APIReader: r.APIReader, Recorder: r.Recorder,
		Resolver: r.Resolver, ClientBuilder: r.ClientBuilder, ClusterCache: r.ClusterCache, Clock: r.Clock,
		WatchFilterValue: r.WatchFilterValue}
}

// holdUntil reconciles the deleting pool every 10 minutes while o2 stays provision_pending, as the deletion's own
// requeue (guardedPoll, 2 minutes) would, and expects every reconcile to hold at the settled-cluster gate; then it
// sets the clock to at. The listings keep the reconciler's first-seen map current (transitionalObservationMaxGap).
//
// Parameters:
//   - r is the reconciler.
//   - at is the clock reading to stop at.
func (f *machinePoolFixture) holdUntil(r *IBMCloudManagedMachinePoolReconciler, at time.Time) {
	f.t.Helper()
	g := NewWithT(f.t)
	const every = 10 * time.Minute
	for f.clock.Now().Add(every).Before(at) {
		f.clock.Step(every)
		f.expectDeleteObservationWith(mpTestOtherWorker("provision_pending"))
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		reason, _ := f.deletingReason()
		g.Expect(reason).To(Equal(infrav1.IBMCloudManagedMachinePoolDeletingWaitingForSettledClusterReason), "held at %s", f.clock.Now())
	}
	f.clock.SetTime(at)
}

// TestIBMCloudManagedMachinePoolReconcileDeleteStuckWorker checks CheckDisruption rule 1 in the controller: the
// reconciler's first-seen map, fed by its own listings and read through the fake clock, decides whether another
// pool's provision_pending worker still holds a pool deletion's gates. The map lives in memory only, so after a
// restart such a worker blocks again until WorkerStuckAfter has passed (fail closed).
func TestIBMCloudManagedMachinePoolReconcileDeleteStuckWorker(t *testing.T) {
	settled := infrav1.IBMCloudManagedMachinePoolDeletingWaitingForSettledClusterReason
	draining := infrav1.IBMCloudManagedMachinePoolDeletingDrainingNodesReason
	// firstReconcile reconciles the deleting pool once at mpTestNow, while o2 is provision_pending: the gate holds.
	firstReconcile := func(t *testing.T, f *machinePoolFixture, r *IBMCloudManagedMachinePoolReconciler) {
		t.Helper()
		g := NewWithT(t)
		f.expectDeleteObservationWith(mpTestOtherWorker("provision_pending"))
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		reason, message := f.deletingReason()
		g.Expect(reason).To(Equal(settled))
		g.Expect(message).To(ContainSubstring("worker o2 is provision_pending"))
	}
	// lastReconcile reconciles once more with o2 still provision_pending and returns the Deleting reason and message.
	lastReconcile := func(t *testing.T, f *machinePoolFixture, r *IBMCloudManagedMachinePoolReconciler) (string, string) {
		t.Helper()
		f.expectDeleteObservationWith(mpTestOtherWorker("provision_pending"))
		_, err := f.reconcile(r)
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		return f.deletingReason()
	}
	t.Run("first_seen_more_than_60m_ago_it_no_longer_blocks_and_is_named", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		calls := f.deletingMachinePool(t)
		r := f.reconciler()
		firstReconcile(t, f, r)
		f.holdUntil(r, mpTestNow.Add(managedscope.WorkerStuckAfter+time.Minute))
		g.Expect(calls.all()).To(BeEmpty(), "nothing is cordoned while the gate holds")

		reason, message := lastReconcile(t, f, r)
		g.Expect(reason).To(Equal(draining), "the gates pass")
		g.Expect(message).To(ContainSubstring("WorkerOperationStuck: IBM has been changing worker o2 for more than 60 minutes"))
		g.Expect(calls.all()).To(ContainElement("cordon 10.0.0.1"))
	})
	t.Run("first_seen_less_than_60m_ago_it_still_blocks", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		calls := f.deletingMachinePool(t)
		r := f.reconciler()
		firstReconcile(t, f, r)
		f.holdUntil(r, mpTestNow.Add(managedscope.WorkerStuckAfter-time.Minute))
		reason, _ := lastReconcile(t, f, r)
		g.Expect(reason).To(Equal(settled))
		g.Expect(calls.all()).To(BeEmpty())
	})
	t.Run("a_restart_blocks_again_until_WorkerStuckAfter_has_passed_since_the_restart", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		calls := f.deletingMachinePool(t)
		r := f.reconciler()
		firstReconcile(t, f, r)
		f.holdUntil(r, mpTestNow.Add(managedscope.WorkerStuckAfter+time.Minute))

		// The controller restarts after the worker has been provision_pending for more than WorkerStuckAfter.
		r = restarted(r)
		restart := f.clock.Now()
		reason, _ := lastReconcile(t, f, r)
		g.Expect(reason).To(Equal(settled), "the empty map fails closed")
		f.holdUntil(r, restart.Add(managedscope.WorkerStuckAfter-time.Minute))
		reason, _ = lastReconcile(t, f, r)
		g.Expect(reason).To(Equal(settled), "still within WorkerStuckAfter of the restart")
		g.Expect(calls.all()).To(BeEmpty())

		f.holdUntil(r, restart.Add(managedscope.WorkerStuckAfter+time.Minute))
		reason, _ = lastReconcile(t, f, r)
		g.Expect(reason).To(Equal(draining))
	})
	t.Run("a_gap_in_the_listings_longer_than_WorkerStuckAfter_counts_like_a_restart", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.deletingMachinePool(t)
		r := f.reconciler()
		firstReconcile(t, f, r)
		// No pool of the Cluster listed the workers in between (a paused Cluster, a stalled controller).
		f.clock.Step(managedscope.WorkerStuckAfter + time.Minute)
		reason, _ := lastReconcile(t, f, r)
		g.Expect(reason).To(Equal(settled), "nothing shows that o2 stayed transitional in between")
	})
	t.Run("the entry is dropped once the worker is deployed", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.deletingMachinePool(t)
		r := f.reconciler()
		f.expectDeleteObservationWith(mpTestOtherWorker("provision_pending"))
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(r.transitional.snapshot("cluster-uid", f.clock.Now())).To(HaveKey("o2"), "the deletion's listing feeds the map")

		f.clock.Step(10 * time.Minute)
		deployed := mpTestOtherWorker("deployed")
		deployed.HealthState = "normal"
		f.expectDeleteObservationWith(deployed)
		_, err = f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(r.transitional.snapshot("cluster-uid", f.clock.Now())).To(BeNil())
		reason, _ := f.deletingReason()
		g.Expect(reason).To(Equal(draining))
	})
}

// envtestDeletablePool creates, in namespace, a managed Cluster named name with its IBMCloudManagedCluster, an
// IBMCloudManagedControlPlane whose IBM cluster exists, a MachinePool and the IBMCloudManagedMachinePool "workers" it
// owns.
func envtestDeletablePool(t *testing.T, namespace, name string) (*clusterv1.Cluster, *infrav1.IBMCloudManagedMachinePool) {
	t.Helper()
	g := NewWithT(t)
	cluster := createTestCluster(t, namespace, name, managedControlPlaneRef(name))
	infra := createTestInfra(t, namespace, name, cluster)
	infra.Status.VPC = infrav1.ResolvedResource{ID: testVPCID}
	infra.Status.Zones = []infrav1.ResolvedZone{
		{Name: "us-south-1", Subnet: infrav1.ResolvedResource{ID: "sn-1"}},
		{Name: "us-south-2", Subnet: infrav1.ResolvedResource{ID: "sn-2"}},
	}
	g.Expect(testEnv.Status().Update(ctx, infra)).To(Succeed())
	labels := map[string]string{clusterv1.ClusterNameLabel: name}
	cp := &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels},
		Spec:       controlplanev1.IBMCloudManagedControlPlaneSpec{Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.36"},
	}
	g.Expect(testEnv.Create(ctx, cp)).To(Succeed())
	cp.Status = controlplanev1.IBMCloudManagedControlPlaneStatus{
		ClusterID:  mpTestClusterID,
		ClusterCRN: "crn:v1:bluemix:public:containers-kubernetes:us-south:a/" + mpTestAccount + ":" + mpTestClusterID + "::",
		State:      "normal",
	}
	g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())
	machinePool := &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "workers", Labels: labels},
		Spec: clusterv1.MachinePoolSpec{ClusterName: name, Replicas: ptr.To[int32](4), Template: clusterv1.MachineTemplateSpec{
			Spec: clusterv1.MachineSpec{ClusterName: name, InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedMachinePoolKind, Name: "workers",
			}},
		}},
	}
	g.Expect(testEnv.Create(ctx, machinePool)).To(Succeed())
	pool := &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "workers", Labels: labels, OwnerReferences: []metav1.OwnerReference{{
			APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool", Name: "workers", UID: machinePool.UID,
		}}},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{PoolName: "workers", Flavor: mpTestFlavor,
			Zones: []infrav1.WorkerPoolZone{{Name: "us-south-1"}, {Name: "us-south-2"}}},
	}
	g.Expect(testEnv.Create(ctx, pool)).To(Succeed())
	return cluster, pool
}

func TestIBMCloudManagedMachinePoolFinalizerEnvtest(t *testing.T) {
	t.Run("a_pool_no_MachinePool_owns_gets_no_finalizer_and_is_deleted", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "pool-unowned")
		pool := &infrav1.IBMCloudManagedMachinePool{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "typo"},
			Spec: infrav1.IBMCloudManagedMachinePoolSpec{PoolName: "typo", Flavor: mpTestFlavor,
				Zones: []infrav1.WorkerPoolZone{{Name: "us-south-1"}}},
		}
		g.Expect(testEnv.Create(ctx, pool)).To(Succeed())
		r := &IBMCloudManagedMachinePoolReconciler{Client: uncachedClient(t), APIReader: testEnv.GetAPIReader(),
			Recorder: newMachinePoolFixture(t).recorder, Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t},
			ClusterCache: noWorkloadCallsClusterCache{t: t}, Clock: clocktesting.NewFakeClock(time.Now())}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
		g.Expect(err).NotTo(HaveOccurred())
		stored := &infrav1.IBMCloudManagedMachinePool{}
		g.Expect(testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(pool), stored)).To(Succeed())
		g.Expect(stored.Finalizers).To(BeEmpty())
		g.Expect(testEnv.Delete(ctx, stored)).To(Succeed())
		g.Eventually(func() bool {
			return apierrors.IsNotFound(testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(pool), &infrav1.IBMCloudManagedMachinePool{}))
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
	})
	t.Run("an_owned_pool_gets_the_finalizer_and_a_Cluster_deletion_removes_it", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "pool-owned")
		cluster, pool := envtestDeletablePool(t, namespace, "dev")
		r := &IBMCloudManagedMachinePoolReconciler{Client: uncachedClient(t), APIReader: testEnv.GetAPIReader(),
			Recorder: newMachinePoolFixture(t).recorder, Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t},
			ClusterCache: noWorkloadCallsClusterCache{t: t}, Clock: clocktesting.NewFakeClock(time.Now())}
		key := client.ObjectKeyFromObject(pool)
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
		stored := &infrav1.IBMCloudManagedMachinePool{}
		g.Expect(testEnv.GetAPIReader().Get(ctx, key, stored)).To(Succeed())
		g.Expect(stored.Finalizers).To(ConsistOf(infrav1.IBMCloudManagedMachinePoolFinalizer), "the finalizer comes after the owner")

		// The Cluster is being deleted (a test finalizer holds it): the pool goes with no IBM and no workload call.
		cluster.Finalizers = []string{"test.cluster.x-k8s.io/hold"}
		g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
		g.Expect(testEnv.Delete(ctx, cluster)).To(Succeed())
		g.Expect(testEnv.Delete(ctx, stored)).To(Succeed())
		for i := 0; i < 3; i++ {
			// First the paused condition is recorded, then the deletion runs.
			_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(HaveOccurred())
		}
		g.Eventually(func() bool {
			return apierrors.IsNotFound(testEnv.GetAPIReader().Get(ctx, key, &infrav1.IBMCloudManagedMachinePool{}))
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		latest := &clusterv1.Cluster{}
		g.Expect(testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(cluster), latest)).To(Succeed())
		latest.Finalizers = nil
		g.Expect(client.IgnoreNotFound(testEnv.Update(ctx, latest))).To(Succeed())
	})
}

// TestIBMCloudManagedMachinePoolConnectEventEnvtest runs the pool controller behind a real manager and a real
// ClusterCache: a deletion that waits for the workload cluster is reconciled on the connect event, long before
// guardedPoll. Test isolation: the manager's cache, and so its watches and the ClusterCache, see only this test's
// namespace, so that objects other tests leave in the shared API server never reach this reconciler and its IBM mock;
// and the Cluster is paused when the test ends, after the manager stopped, so that a manager of a later test leaves
// the objects left behind alone.
func TestIBMCloudManagedMachinePoolConnectEventEnvtest(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "pool-connect")
	cluster, pool := envtestDeletablePool(t, namespace, "dev")
	// Registered before the manager starts, so it runs after the manager has stopped (cleanups run last in, first out).
	pauseAtCleanup(t, cluster, client.ObjectKeyFromObject(pool))
	mgr := newNamespacedTestManager(t, namespace)
	clusterCache, err := SetupClusterCache(ctx, mgr, ClusterCacheOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	shutdownClusterCacheOnCleanup(t, clusterCache)

	ibm := ksmock.NewMockKubernetesService(gomock.NewController(t))
	ibm.EXPECT().GetCluster(gomock.Any(), mpTestClusterID, testResourceGroupID).Return(&kubernetesservice.Cluster{State: "normal"}, nil).AnyTimes()
	ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{{
		ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2,
	}}, nil).AnyTimes()
	ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(mpTestWorkers(), nil).AnyTimes()
	ibm.EXPECT().RemoveWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID).Return(nil).AnyTimes()
	clock := clocktesting.NewFakeClock(time.Now())
	r := &IBMCloudManagedMachinePoolReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(),
		Recorder: mgr.GetEventRecorderFor("ibmcloudmanagedmachinepool-controller"), Resolver: &recordingResolver{},
		ClientBuilder: &stubClientBuilder{kubernetes: ibm}, ClusterCache: clusterCache, Clock: clock}
	g.Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
	startTestManager(t, mgr)

	key := client.ObjectKeyFromObject(pool)
	read := func() *infrav1.IBMCloudManagedMachinePool {
		stored := &infrav1.IBMCloudManagedMachinePool{}
		g.Expect(testEnv.GetAPIReader().Get(ctx, key, stored)).To(Succeed())
		return stored
	}
	g.Eventually(func() bool { return !read().Status.LastDisruptionTime.IsZero() }, 30*time.Second, 200*time.Millisecond).
		Should(BeTrue(), "the pool is observed")
	// The first observation started the settle delay; move the controller's clock past it.
	clock.Step(10 * time.Minute)

	g.Expect(testEnv.Delete(ctx, read())).To(Succeed())
	deletingReason := func() string {
		if c := conditions.Get(read(), infrav1.IBMCloudManagedMachinePoolDeletingCondition); c != nil && c.Status == metav1.ConditionTrue {
			return c.Reason
		}
		return ""
	}
	g.Eventually(deletingReason, 30*time.Second, 200*time.Millisecond).Should(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason))
	g.Consistently(deletingReason, 2*time.Second, 200*time.Millisecond).Should(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason),
		"nothing else re-triggers the pool")

	// The workload cluster connects: its kubeconfig Secret appears and the infrastructure is provisioned.
	g.Expect(testEnv.Create(ctx, kubeconfig.GenerateSecret(cluster, kubeconfig.FromEnvTestConfig(testEnv.Config, cluster)))).To(Succeed())
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
	// guardedPoll is 2 minutes; a reconcile within 30 seconds can only come from the connect event.
	g.Eventually(deletingReason, 30*time.Second, 200*time.Millisecond).ShouldNot(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason))
	g.Expect(read().Status.Disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationDeletePool))
}
