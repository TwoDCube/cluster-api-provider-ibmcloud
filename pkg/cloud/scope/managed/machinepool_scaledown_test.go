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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"

	. "github.com/onsi/gomega"
)

// The scale-down tests drive ReconcileNormal reconcile by reconcile against a fake management cluster (which keeps the
// disruption token), a fake workload cluster behind a fake ClusterCache and a gomock IBM Cloud Kubernetes Service. The
// pure planning and verification are tested on their own first.

// sdWorker is a ready worker of the test pool, or of another pool when poolID is "other-pool".
//
// Parameters:
//   - id is the worker ID.
//   - poolID is the IBM pool ID.
//   - zone is its zone.
func sdWorker(id, poolID, zone string) kubernetesservice.Worker {
	w := poolWorker(id, zone, WorkerStateDeployed, "normal")
	w.PoolID = poolID
	return w
}

// sdWorkers are the four ready workers w0..w3 of the test pool (w0, w2 in us-south-1; w1, w3 in us-south-2) and the two
// ready workers o0, o1 of another pool, which keep IBM's minimum. IBM's order picks w0 and w1 when the pool shrinks by
// one worker per zone.
func sdWorkers() []kubernetesservice.Worker {
	return append(readyWorkers(4), sdWorker("o0", "other-pool", "us-south-1"), sdWorker("o1", "other-pool", "us-south-2"))
}

// withState returns workers with the worker id in state.
//
// Parameters:
//   - workers are the workers; they are copied.
//   - state is the new lifecycle.actualState.
//   - ids are the workers to change.
func withState(workers []kubernetesservice.Worker, state string, ids ...string) []kubernetesservice.Worker {
	changed := append([]kubernetesservice.Worker(nil), workers...)
	for i := range changed {
		for _, id := range ids {
			if changed[i].ID == id {
				changed[i].ActualState = state
			}
		}
	}
	return changed
}

func TestPlanScaleDown(t *testing.T) {
	zones := []string{"us-south-1", "us-south-2"}
	provisioning := withState(readyWorkers(4), "provisioning", "w0", "w1", "w2", "w3")
	// An out-of-band `worker rm` of w3 left us-south-2 with w1 only.
	unbalanced := withState(sdWorkers(), WorkerStateDeleted, "w3")
	testCases := []struct {
		name string
		in   ScaleDownPlanInput
		want ScaleDownPlan
	}{
		{name: "a new plan predicts one victim per zone",
			in:   ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, TriggerActive: true, RemovedReady: 2}},
		{name: "a new plan to zero takes every worker",
			in:   ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 0},
			want: ScaleDownPlan{Victims: []string{"w0", "w1", "w2", "w3"}, Target: 0, TriggerActive: true, RemovedReady: 4}},
		{name: "no plan when the MachinePool does not ask for fewer workers",
			in:   ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 2},
			want: ScaleDownPlan{Target: 2}},
		{name: "no victims when IBM lists no worker of the pool",
			in:   ScaleDownPlanInput{Workers: sdWorkers()[4:], PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{}, Target: 1}},
		{name: "zero-ready exemption: a pool without ready workers removes no ready capacity",
			in:   ScaleDownPlanInput{Workers: provisioning, PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, TriggerActive: true, RemovedReady: 0}},
		{name: "a plan in flight keeps its victims",
			in: ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1,
				Victims: []string{"w1", "w0"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, TriggerActive: true, RemovedReady: 2}},
		{name: "replicas raised back: the trigger is gone",
			in: ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 2,
				Victims: []string{"w0", "w1"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, RemovedReady: 2}},
		{name: "replicas raised part of the way: more victims than wanted, so the plan is redone",
			in: ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1,
				Victims: []string{"w0", "w1", "w2", "w3"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1", "w2", "w3"}, Target: 0, RemovedReady: 4}},
		{name: "replicas lowered further: the drained victims go first, at their own target",
			in: ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 0,
				Victims: []string{"w0", "w1"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, TriggerActive: true, RemovedReady: 2}},
		{name: "victims in one zone only: the only safe target removes nothing, so the plan is redone",
			in: ScaleDownPlanInput{Workers: sdWorkers(), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1,
				Victims: []string{"w0"}},
			want: ScaleDownPlan{Victims: []string{"w0"}, Target: 2, RemovedReady: 0}},
		{name: "a smaller zone gives all its workers",
			in: ScaleDownPlanInput{Workers: readyWorkers(3), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 0,
				Victims: []string{"w0", "w1", "w2"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1", "w2"}, Target: 0, TriggerActive: true, RemovedReady: 3}},
		{name: "a deleted victim no longer counts",
			in: ScaleDownPlanInput{Workers: withState(sdWorkers(), WorkerStateDeleted, "w0", "w1"), PoolID: poolTestPoolID,
				Zones: zones, SizePerZone: 2, Desired: 1, Victims: []string{"w0", "w1"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, RemovedReady: 0}},

		// IBM's resize leaves Target workers in every zone, so a short zone loses fewer.
		{name: "an unbalanced pool: the short zone gives no victim",
			in:   ScaleDownPlanInput{Workers: unbalanced, PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{"w0"}, Target: 1, TriggerActive: true, RemovedReady: 1}},
		{name: "an unbalanced pool in flight keeps its target",
			in: ScaleDownPlanInput{Workers: unbalanced, PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1,
				Victims: []string{"w0"}},
			want: ScaleDownPlan{Victims: []string{"w0"}, Target: 1, TriggerActive: true, RemovedReady: 1}},
		{name: "an unbalanced pool in flight whose victims cover the short zone too would leave one cordoned",
			in: ScaleDownPlanInput{Workers: unbalanced, PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1,
				Victims: []string{"w0", "w1"}},
			want: ScaleDownPlan{Victims: []string{"w0", "w1"}, Target: 1, RemovedReady: 1}},
		{name: "a zone above the pool size gives its extra workers",
			in: ScaleDownPlanInput{Workers: append(sdWorkers(), sdWorker("w4", poolTestPoolID, "us-south-1")), PoolID: poolTestPoolID,
				Zones: zones, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{"w0", "w1", "w2"}, Target: 1, TriggerActive: true, RemovedReady: 3}},
		{name: "no zone above Desired: no victims, the resize removes nothing",
			in:   ScaleDownPlanInput{Workers: withState(sdWorkers(), WorkerStateDeleted, "w2", "w3"), PoolID: poolTestPoolID, Zones: zones, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{}, Target: 1}},
		{name: "every zone of IBM's pool, also one spec.zones leaves out",
			in: ScaleDownPlanInput{Workers: append(sdWorkers(), sdWorker("w4", poolTestPoolID, "us-south-3"), sdWorker("w5", poolTestPoolID, "us-south-3")),
				PoolID: poolTestPoolID, Zones: []string{"us-south-1", "us-south-2", "us-south-3"}, SizePerZone: 2, Desired: 1},
			want: ScaleDownPlan{Victims: []string{"w0", "w1", "w4"}, Target: 1, TriggerActive: true, RemovedReady: 3}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			plan, err := PlanScaleDown(tc.in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(plan).To(Equal(tc.want))
		})
	}
	invalid := []struct {
		name string
		in   ScaleDownPlanInput
		want string
	}{
		{name: "no pool ID", in: ScaleDownPlanInput{Zones: zones}, want: "PoolID is empty"},
		{name: "no zones", in: ScaleDownPlanInput{PoolID: poolTestPoolID}, want: "Zones is empty"},
		{name: "negative size", in: ScaleDownPlanInput{PoolID: poolTestPoolID, Zones: zones, SizePerZone: -1}, want: "neither negative"},
		{name: "negative desired", in: ScaleDownPlanInput{PoolID: poolTestPoolID, Zones: zones, Desired: -1}, want: "neither negative"},
		{name: "a zone twice", in: ScaleDownPlanInput{PoolID: poolTestPoolID, Zones: []string{"us-south-1", "us-south-1"}, SizePerZone: 2},
			want: "zone us-south-1 twice"},
	}
	for _, tc := range invalid {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := PlanScaleDown(tc.in)
			NewWithT(t).Expect(err).To(MatchError(ContainSubstring(tc.want)))
		})
	}
}

func TestScaleDownZones(t *testing.T) {
	g := NewWithT(t)
	object := poolObject("workers", "workers")
	g.Expect(scaleDownZones(ptr.To(ibmPool("workers", 2, "us-south-2", "us-south-1")), object)).
		To(Equal([]string{"us-south-1", "us-south-2"}), "the same zones once each")
	g.Expect(scaleDownZones(ptr.To(ibmPool("workers", 2, "us-south-1", "us-south-2", "us-south-3")), object)).
		To(Equal([]string{"us-south-1", "us-south-2", "us-south-3"}), "an IBM zone spec.zones leaves out")
	g.Expect(scaleDownZones(ptr.To(ibmPool("workers", 2, "us-south-1")), object)).
		To(Equal([]string{"us-south-1", "us-south-2"}), "a spec zone IBM's listing leaves out")
	g.Expect(scaleDownZones(&kubernetesservice.WorkerPool{Zones: []kubernetesservice.WorkerPoolZone{{}}}, object)).
		To(Equal([]string{"us-south-1", "us-south-2"}), "a zone without an ID is no zone")
}

func TestVerifyScaleDownVictims(t *testing.T) {
	victims := []string{"w0", "w1"}
	called := poolTestNow.Add(-5 * time.Minute)
	staleCall := poolTestNow.Add(-WorkerStuckAfter - time.Minute)
	beforeCall := map[string]time.Time{"w2": called.Add(-WorkerStuckAfter)}
	afterCall := map[string]time.Time{"w2": called.Add(time.Minute)}
	testCases := []struct {
		name      string
		workers   []kubernetesservice.Worker
		victims   []string
		published []string
		called    time.Time
		since     map[string]time.Time
		want      ScaleDownVictimsResult
	}{
		{name: "before IBM acts", workers: sdWorkers(), published: victims, called: called,
			want: ScaleDownVictimsResult{}},
		{name: "IBM removes every victim", workers: withState(sdWorkers(), WorkerStateDeleting, "w0", "w1"), published: victims,
			called: called, want: ScaleDownVictimsResult{Progress: RemovalProgress{Removing: true}, IBMVictims: victims}},
		{name: "IBM removed every victim", workers: withState(sdWorkers(), WorkerStateDeleted, "w0", "w1"), called: called,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{Removing: true, Removed: true}, IBMVictims: victims}},
		{name: "a victim no longer listed counts as removed by IBM", workers: withState(sdWorkers()[1:], WorkerStateDeleting, "w1"),
			published: []string{"w0"}, called: called,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{Removing: true}, IBMVictims: victims}},
		{name: "IBM removes another worker than a predicted victim",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w1", "w2"), published: victims, called: called,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{UnexpectedVictims: true}, Spared: []string{"w0"},
				Unexpected: []string{"w2"}, IBMVictims: []string{"w1", "w2"}}},
		{name: "IBM has not picked its full set in any zone yet: no victim is spared",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w0"), victims: []string{"w0", "w1", "w2", "w3"},
			published: []string{"w0", "w1", "w2", "w3"}, called: called, want: ScaleDownVictimsResult{}},
		{name: "IBM picks another worker of one zone first: that zone is decided, the other one is not",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w2"), published: victims, called: called,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{UnexpectedVictims: true}, Spared: []string{"w0"},
				Unexpected: []string{"w2"}, IBMVictims: []string{"w1", "w2"}}},
		{name: "each zone is decided by its own picks",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w0", "w3"), published: victims, called: called,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{UnexpectedVictims: true}, Spared: []string{"w1"},
				Unexpected: []string{"w3"}, IBMVictims: []string{"w0", "w3"}}},
		{name: "a pick in one zone never spares a victim of another zone",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w0", "w2"), published: victims, called: called, since: afterCall,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{UnexpectedVictims: true}, Unexpected: []string{"w2"},
				IBMVictims: []string{"w0", "w1", "w2"}}},
		{name: "a worker IBM was already removing before the resize is no pick",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w0", "w2"), published: victims, called: called, since: beforeCall,
			want: ScaleDownVictimsResult{IBMVictims: victims}},
		{name: "a worker IBM was already removing before the resize does not decide its zone",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w2"), published: victims, called: called, since: beforeCall,
			want: ScaleDownVictimsResult{}},
		{name: "IBM removes more workers than predicted",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w0", "w1", "w2"), published: victims, called: called,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{Removing: true, UnexpectedVictims: true},
				Unexpected: []string{"w2"}, IBMVictims: []string{"w0", "w1", "w2"}}},
		{name: "after WorkerStuckAfter a victim IBM still keeps is spared",
			workers: withState(sdWorkers(), WorkerStateDeleting, "w1"), published: victims, called: staleCall,
			want: ScaleDownVictimsResult{Progress: RemovalProgress{UnexpectedVictims: true}, Spared: []string{"w0"},
				IBMVictims: []string{"w1"}}},
		{name: "after WorkerStuckAfter with nothing removed every victim is spared", workers: sdWorkers(), published: victims,
			called: staleCall, want: ScaleDownVictimsResult{Progress: RemovalProgress{UnexpectedVictims: true}, Spared: victims}},
		{name: "an old deleted worker and another pool's deleting worker are ignored",
			workers: append(withState(sdWorkers(), WorkerStateDeleting, "o0"),
				poolWorker("w9", "us-south-1", WorkerStateDeleted, "normal")),
			published: victims, called: called, want: ScaleDownVictimsResult{}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := ScaleDownVictimsInput{Workers: tc.workers, PoolID: poolTestPoolID, Victims: victims,
				StillPublished: tc.published, Now: poolTestNow, IBMRequestTime: tc.called, TransitionalSince: tc.since}
			if tc.victims != nil {
				in.Victims = tc.victims
			}
			result, err := VerifyScaleDownVictims(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result).To(Equal(tc.want))
		})
	}
	for _, tc := range []struct {
		name string
		in   ScaleDownVictimsInput
		want string
	}{
		{name: "no pool ID", in: ScaleDownVictimsInput{Victims: victims, Now: poolTestNow}, want: "PoolID is empty"},
		{name: "no victims", in: ScaleDownVictimsInput{PoolID: poolTestPoolID, Now: poolTestNow}, want: "Victims is empty"},
		{name: "no clock reading", in: ScaleDownVictimsInput{PoolID: poolTestPoolID, Victims: victims}, want: "Now is not set"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			_, err := VerifyScaleDownVictims(tc.in)
			NewWithT(t).Expect(err).To(MatchError(ContainSubstring(tc.want)))
		})
	}
}

func TestRefusalMessageAndWorkerLists(t *testing.T) {
	g := NewWithT(t)
	g.Expect(refusalMessage(ReasonWaitingForClusterMinimum)).To(Equal("IBM refused the resize because the cluster would " +
		"fall below its minimum workers (E1437); the victim Nodes stay as they are, and the resize is sent again at most once " +
		"every 3 minutes while CAPIBM's own guards allow it"))
	g.Expect(refusalMessage(ReasonExternallyAutoscaled)).To(HavePrefix("IBM refused the resize because its cluster autoscaler owns this pool"))
	g.Expect(noneIfEmptyWorkers(nil)).To(Equal("no worker"))
	g.Expect(noneIfEmptyWorkers([]string{"w0", "w1"})).To(Equal("workers w0, w1"))
}

// scaleDownFixture is a pool fixture whose MachinePool asks for one worker per zone (replicas 2) while IBM's pool has
// two, with a connected workload cluster that holds a Node and a pod for every worker of sdWorkers.
type scaleDownFixture struct {
	*poolFixture
	calls    *workloadCalls
	workload client.Client
	// workers are what ListWorkers answers; ibmPool is what ListWorkerPools answers.
	workers []kubernetesservice.Worker
	ibmPool kubernetesservice.WorkerPool
}

// sdNodeName is the Node name of a test worker: 10.0.0.1-4 for w0-w3 and 10.0.1.1-2 for o0, o1.
func sdNodeName(workerID string) string {
	names := map[string]string{"w0": "10.0.0.1", "w1": "10.0.0.2", "w2": "10.0.0.3", "w3": "10.0.0.4", "o0": "10.0.1.1", "o1": "10.0.1.2"}
	return names[workerID]
}

// newScaleDownFixture returns the scale-down fixture; the settle delay of the last removal has passed.
//
// Parameters:
//   - t is the test.
//   - refusals are the evictions a PodDisruptionBudget refuses, by pod name.
func newScaleDownFixture(t *testing.T, refusals map[string]error) *scaleDownFixture {
	t.Helper()
	f := &scaleDownFixture{poolFixture: newPoolFixture(t), calls: &workloadCalls{}, workers: sdWorkers(),
		ibmPool: ibmPool("workers", 2, "us-south-1", "us-south-2")}
	f.machinePool.Spec.Replicas = ptr.To[int32](2)
	f.pool.Status.PoolID = poolTestPoolID
	f.pool.Status.LastDisruptionTime = metav1.NewTime(poolTestNow.Add(-time.Hour))
	objs := make([]client.Object, 0, 12)
	for _, w := range f.workers {
		objs = append(objs, victimNode(sdNodeName(w.ID), w.ID), workloadPod("web-"+w.ID, sdNodeName(w.ID)))
	}
	f.workload = newWorkloadClient(t, f.calls, refusals, objs...)
	f.clusterCache = clustercache.NewFakeClusterCache(f.workload, client.ObjectKey{Namespace: poolTestNamespace, Name: poolTestCluster})
	return f
}

// expect expects one ListWorkerPools and one ListWorkers answering the fixture's pool and workers, and returns the
// calls for gomock.InOrder.
func (f *scaleDownFixture) expect() []any {
	return []any{
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{f.ibmPool}, nil),
		f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(f.workers, nil),
	}
}

// reconcile runs ReconcileNormal once with a new scope, as the controller does.
func (f *scaleDownFixture) reconcile() Outcome {
	return f.scope().ReconcileNormal(context.Background())
}

// ready returns the WorkerPoolReady condition of the pool.
func (f *scaleDownFixture) ready() *metav1.Condition {
	return condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
}

// draining returns the Draining condition of the pool.
func (f *scaleDownFixture) draining() *metav1.Condition {
	return condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition)
}

// cordoned returns the names of the cordoned Nodes, sorted.
func (f *scaleDownFixture) cordoned() []string {
	var names []string
	for _, node := range readNodes(f.t, f.workload) {
		if node.Spec.Unschedulable {
			names = append(names, node.Name)
		}
	}
	return names
}

// lease returns the disruption token Lease, or nil.
func (f *scaleDownFixture) lease() *coordinationv1.Lease {
	f.t.Helper()
	lease := &coordinationv1.Lease{}
	err := f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: poolTestNamespace, Name: DisruptionTokenName(poolTestCluster)}, lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return lease
}

// inPhase records a scale-down of w0 and w1 in phase whose drain started ten minutes ago, with the token held.
func (f *scaleDownFixture) inPhase(phase infrav1.WorkerDisruptionPhase) {
	f.others = append(f.others, heldLease(DisruptionTokenHolder(f.pool)))
	f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationScaleDown, Phase: phase,
		WorkerIDs: []string{"w0", "w1"}, DrainStartTime: metav1.NewTime(poolTestNow.Add(-10 * time.Minute))}
}

// cordonVictims cordons the Nodes of w0 and w1 for the pool, as an earlier reconcile would have; with drained the
// pods are gone too.
func (f *scaleDownFixture) cordonVictims(drained bool) {
	f.t.Helper()
	g := NewWithT(f.t)
	helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	nodes := VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0", "w1"})
	for i := range nodes {
		g.Expect(helper.Cordon(context.Background(), &nodes[i], drain.CordonOptions{Owner: DrainOwner(f.pool)})).To(Succeed())
	}
	if drained {
		for _, id := range []string{"w0", "w1"} {
			g.Expect(f.workload.Delete(context.Background(), workloadPod("web-"+id, ""))).To(Succeed())
		}
	}
	f.calls.all()
}

// afterResize sets the IBM pool to one worker per zone and the scale-down to phase WaitingForRemoval with the call sent
// five minutes ago, with the victims drained.
func (f *scaleDownFixture) afterResize() {
	f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForRemoval)
	f.pool.Status.Disruption.IBMRequestTime = metav1.NewTime(poolTestNow.Add(-5 * time.Minute))
	f.pool.Status.LastDisruptionTime = f.pool.Status.Disruption.IBMRequestTime
	f.ibmPool = ibmPool("workers", 1, "us-south-1", "us-south-2")
	f.cordonVictims(true)
}

func TestMachinePoolScopeScaleDown(t *testing.T) {
	t.Run("shrink drains the predicted victims then resizes in order", testScaleDownDrainsThenResizes)
}

// testScaleDownDrainsThenResizes follows one scale-down reconcile by reconcile. gomock.InOrder proves that
// ResizeWorkerPool comes only in the reconcile after the drain and the volume detach wait, and its DoAndReturn that the
// victims' pods are gone and phase WaitingForIBM is persisted when it is sent; the phases run Draining →
// WaitingForVolumeDetach → WaitingForIBM → WaitingForRemoval.
func testScaleDownDrainsThenResizes(t *testing.T) {
	g := NewWithT(t)
	f := newScaleDownFixture(t, nil)
	removing := withState(sdWorkers(), WorkerStateDeleting, "w0", "w1")
	removed := withState(sdWorkers(), WorkerStateDeleted, "w0", "w1")
	smaller := []kubernetesservice.WorkerPool{ibmPool("workers", 1, "us-south-1", "us-south-2")}
	listings := append(append(f.expect(), f.expect()...), f.expect()...)
	resize := f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).DoAndReturn(
		func(context.Context, string, string, int64) error {
			g.Expect(f.persisted).To(HaveLen(1), "status is persisted before the call")
			g.Expect(f.persisted[0].Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
			g.Expect(f.persisted[0].IBMRequestTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
			pods := &corev1.PodList{}
			g.Expect(f.workload.List(context.Background(), pods, client.MatchingFields{"spec.nodeName": "10.0.0.1"})).To(Succeed())
			g.Expect(pods.Items).To(BeEmpty(), "no resize while a victim still runs evictable pods")
			return nil
		})
	after := []any{
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(smaller, nil),
		f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(removing, nil),
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(smaller, nil),
		f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(removed, nil),
	}
	gomock.InOrder(append(append(listings, resize), after...)...)

	// 1. Every gate passes, the token is taken, both victims are cordoned before the first eviction; the other workers
	//    of the pool and the other pool's Nodes are untouched.
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
	g.Expect(f.calls.all()).To(Equal([]string{"list nodes", "cordon 10.0.0.1", "cordon 10.0.0.2",
		"list pods 10.0.0.1", "evict apps/web-w0", "list pods 10.0.0.2", "evict apps/web-w1"}))
	g.Expect(f.lease().Spec.HolderIdentity).To(Equal(ptr.To(DisruptionTokenHolder(f.pool))))
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{
		Operation: infrav1.WorkerDisruptionOperationScaleDown, Phase: infrav1.WorkerDisruptionPhaseDraining,
		WorkerIDs: []string{"w0", "w1"}, DrainStartTime: metav1.NewTime(poolTestNow),
	}))
	g.Expect(f.ready().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyDrainingNodeReason))
	g.Expect(f.ready().Message).To(Equal("scale-down of pool workers removing workers w0, w1: cordoning every victim Node, then evicting their pods"))
	g.Expect(f.draining().Status).To(Equal(metav1.ConditionTrue))
	g.Expect(f.draining().Message).To(ContainSubstring("Node 10.0.0.1: Drain not completed yet"))
	g.Expect(f.events()).To(ConsistOf("Normal DrainStarted cordoning 2 Nodes of pool workers, then evicting their pods"))

	// 2. The pods are gone: on to the volume detach wait.
	f.clock.Step(20 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	g.Expect(f.events()).To(ConsistOf("Normal DrainCompleted the 2 Nodes of pool workers are drained"))

	// 3. Volumes detached, the pre-call gate passes: WaitingForIBM is persisted, then ResizeWorkerPool(1) is sent once.
	f.clock.Step(20 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
	g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
	g.Expect(f.draining().Status).To(Equal(metav1.ConditionFalse))
	g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}), "the victims stay cordoned")

	// 4. IBM removes the victims: wait, no second call.
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
	g.Expect(f.pool.Spec.ProviderIDList).To(ContainElement(providerIDOf("w0")), "a deleting worker stays published")

	// 5. IBM lists them deleted: they leave spec.providerIDList, the token is released and status.disruption cleared.
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(Done()))
	g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w2"), providerIDOf("w3")}))
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(f.lease()).To(BeNil())
	g.Expect(f.ready().Status).To(Equal(metav1.ConditionTrue))
	g.Expect(f.draining().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolNotDrainingReason))
	g.Expect(f.resolver.holders).To(HaveEach(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{})), "every holder is the InfraCluster")
}

func TestMachinePoolScopeScaleDownGates(t *testing.T) {
	testCases := []struct {
		name       string
		modify     func(f *scaleDownFixture)
		wantReason DisruptionReason
	}{
		{name: "the settle delay of another removal", wantReason: ReasonWaitingForSettledCluster, modify: func(f *scaleDownFixture) {
			other := poolObject("other", "other")
			other.Status.LastDisruptionTime = metav1.NewTime(poolTestNow.Add(-time.Minute))
			f.others = append(f.others, other)
		}},
		{name: "a changing worker of the cluster", wantReason: ReasonWaitingForSettledCluster, modify: func(f *scaleDownFixture) {
			f.workers = withState(f.workers, "provisioning", "o1")
		}},
		{name: "IBM's minimum", wantReason: ReasonWaitingForClusterMinimum, modify: func(f *scaleDownFixture) {
			f.cp.Spec.Distribution = "OpenShift"
			f.workers = withState(f.workers, "provision_failed", "o0", "o1", "w2", "w3")
		}},
		{name: "the cluster is unavailable", wantReason: ReasonWaitingForClusterState, modify: func(f *scaleDownFixture) {
			f.cp.Status.State = "unavailable"
		}},
		{name: "the cluster is scaled_down", wantReason: ReasonWaitingForClusterState, modify: func(f *scaleDownFixture) {
			f.cp.Status.State = "scaled_down"
		}},
		{name: "the workload cluster is not connected", wantReason: ReasonWaitingForWorkloadCluster, modify: func(f *scaleDownFixture) {
			f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		}},
		{name: "another pool holds the token", wantReason: ReasonWaitingForDisruptionToken, modify: func(f *scaleDownFixture) {
			other := poolObject("other", "other")
			f.others = append(f.others, other, heldLease(DisruptionTokenHolder(other)))
		}},
		{name: "a worker of another pool changing for less than WorkerStuckAfter", wantReason: ReasonWaitingForSettledCluster,
			modify: func(f *scaleDownFixture) {
				f.workers = append(f.workers, otherPoolWorker("provision_pending"))
				f.transitionalSince = map[string]time.Time{"o2": poolTestNow.Add(-WorkerStuckAfter + time.Minute)}
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			tc.modify(f)
			f.expect()
			// gomock fails on a ResizeWorkerPool.
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(tc.wantReason))))
			g.Expect(f.ready().Reason).To(Equal(string(tc.wantReason)))
			g.Expect(f.ready().Message).To(HavePrefix("scale-down of pool workers removing workers "))
			g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}), "nothing is recorded before the first cordon")
			g.Expect(f.cordoned()).To(BeEmpty())
		})
	}
	t.Run("another pool's drain on a Node holds the scale-down", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.1.1"}, node)).To(Succeed())
		node.Annotations = map[string]string{infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner: poolTestNamespace + "/other"}
		g.Expect(f.workload.Update(context.Background(), node)).To(Succeed())
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForDisruptionToken))))
		g.Expect(f.ready().Message).To(ContainSubstring("Nodes are still drained by IBMCloudManagedMachinePool " + poolTestNamespace + "/other"))
	})
	t.Run("an autoscaled pool is not shrunk", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.ibmPool.AutoscaleEnabled = true
		f.expect()
		g.Expect(f.reconcile()).To(Equal(Done()))
		g.Expect(f.ready().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyExternallyAutoscaledReason))
		g.Expect(f.cordoned()).To(BeEmpty())
	})
}

// TestMachinePoolScopeScaleDownAllowed covers the gates that let a scale-down through: a shrink on an unsupported
// cluster (that state only stops adding or reloading workers), a worker IBM has left transitional for longer than
// WorkerStuckAfter, and the zero-ready exemption (a pool without ready workers takes no ready capacity away).
func TestMachinePoolScopeScaleDownAllowed(t *testing.T) {
	testCases := []struct {
		name        string
		modify      func(f *scaleDownFixture)
		wantMessage string
	}{
		{name: "a shrink is allowed on an unsupported cluster", modify: func(f *scaleDownFixture) { f.cp.Status.State = "unsupported" },
			wantMessage: "scale-down of pool workers removing workers w0, w1: cordoning every victim Node, then evicting their pods"},
		{name: "a worker of another pool changing for more than WorkerStuckAfter no longer blocks",
			modify: func(f *scaleDownFixture) {
				f.workers = append(f.workers, otherPoolWorker("provision_pending"))
				f.transitionalSince = map[string]time.Time{"o2": poolTestNow.Add(-WorkerStuckAfter - time.Minute)}
			},
			wantMessage: "scale-down of pool workers removing workers w0, w1: cordoning every victim Node, then evicting their pods; " +
				"WorkerOperationStuck: IBM has been changing worker o2 for more than 60 minutes; treated like a failed worker, it no " +
				"longer blocks worker removals"},
		{name: "zero-ready exemption while the pool's own workers provision", modify: func(f *scaleDownFixture) {
			f.workers = withState(f.workers, "provisioning", "w0", "w1", "w2", "w3")
			other := poolObject("other", "other")
			other.Status.LastDisruptionTime = metav1.NewTime(poolTestNow.Add(-time.Minute))
			f.others = append(f.others, other)
		}, wantMessage: "scale-down of pool workers removing workers w0, w1: cordoning every victim Node, then evicting their pods"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			tc.modify(f)
			f.expect()
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
			g.Expect(f.ready().Message).To(Equal(tc.wantMessage))
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
			g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
		})
	}
}

func TestMachinePoolScopeScaleDownClusterMinimum(t *testing.T) {
	t.Run("a racing E1437 holds with the Nodes cordoned and sends no second resize within DisruptionSettleDelay", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		e1437 := apiError(kubernetesservice.OpResizeWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode)
		refusal := "scale-down of pool workers removing workers w0, w1: " + refusalMessage(ReasonWaitingForClusterMinimum)
		holds := func(when string) {
			t.Helper()
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))), when)
			g.Expect(f.ready().Reason).To(Equal(string(ReasonWaitingForClusterMinimum)), when+": never Resizing in between")
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
			g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}), when+": the Nodes stay cordoned")
		}

		// 1. IBM refuses: hold in WaitingForIBM, never an error.
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).Return(e1437)
		holds("the refusal")
		g.Expect(f.ready().Message).To(Equal(refusal))
		g.Expect(f.persisted).To(HaveLen(1))
		g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow))

		// 2. The reconcile the own status patch triggers, and one within the settle delay, send no second call.
		f.expect()
		holds("the reconcile right after the refusal")
		g.Expect(f.ready().Message).To(Equal(refusal), "the same message: nothing new is written")
		f.clock.Step(2 * time.Minute)
		f.expect()
		holds("within the settle delay")

		// 3. After DisruptionSettleDelay the resize is sent again while CAPIBM's own gate passes ...
		f.clock.Step(2 * time.Minute)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).Return(e1437)
		holds("the retry")
		g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow.Add(4 * time.Minute)))

		// 4. ... and held without a call once that gate fails.
		f.clock.Step(4 * time.Minute)
		f.workers = withState(f.workers, "provision_failed", "o0", "o1", "w2", "w3")
		f.expect()
		holds("CAPIBM's own gate")
		g.Expect(f.ready().Message).To(ContainSubstring("below IBM's minimum"))
	})
	t.Run("a refused resize of an autoscaled pool holds as ExternallyAutoscaled", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		f.ibmPool.AutoscaleEnabled = true
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).Return(&kubernetesservice.APIError{
			Operation: kubernetesservice.OpResizeWorkerPool, StatusCode: http.StatusForbidden, Description: "Forbidden. The request will not be permitted."})
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonExternallyAutoscaled))))
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonExternallyAutoscaled))), "no second call; the hold stays")
		g.Expect(f.ready().Status).To(Equal(metav1.ConditionFalse))
	})
}

func TestMachinePoolScopeScaleDownCallFailures(t *testing.T) {
	t.Run("no resize when status cannot be persisted", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		f.persistErr = errors.New("conflict")
		f.expect()
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTransient))
	})
	t.Run("an unknown outcome is never repeated within DisruptionSettleDelay", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).
			Return(apiError(kubernetesservice.OpResizeWorkerPool, http.StatusInternalServerError, ""))
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTransient))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))

		f.clock.Step(time.Minute)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.ready().Message).To(ContainSubstring("the IBM call was sent at 2026-11-16T12:00:00Z"))
	})
	t.Run("a refused resize needs a role", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).
			Return(apiError(kubernetesservice.OpResizeWorkerPool, http.StatusForbidden, ""))
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.ready().Reason).To(Equal(MachinePoolIBMAPIErrorReason))
		g.Expect(f.ready().Message).To(ContainSubstring("while shrinking worker pool workers of IBM cluster " + poolTestClusterID +
			" to 1 workers per zone; grant it " + poolKubernetesServiceRoles))
	})
	t.Run("the workload cluster down before the call holds", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))), "no ResizeWorkerPool")
		g.Expect(f.draining().Reason).To(Equal(string(ReasonWaitingForWorkloadCluster)))
	})
}

func TestMachinePoolScopeScaleDownVerifies(t *testing.T) {
	t.Run("IBM deletes another worker: the spared victim is uncordoned", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.afterResize()
		f.workers = withState(f.workers, WorkerStateDeleting, "w1", "w2")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.events()).To(ConsistOf("Warning UnexpectedScaleDownVictim IBM's resize of pool workers removes workers w1, w2, " +
			"not the predicted workers w0, w1; CAPIBM waits for the workers IBM removes and uncordoned the Nodes of the spared worker w0"))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.2"}), "the spared victim is uncordoned, the removed one stays cordoned")
		spared := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.1"}, spared)).To(Succeed())
		g.Expect(spared.Annotations).NotTo(HaveKey(infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner), "no marker is left behind")
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w1", "w2"}))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))

		// The removal now waits for IBM's workers and ends when they are deleted, with no second warning.
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.events()).To(BeEmpty())
		f.workers = withState(f.workers, WorkerStateDeleted, "w1", "w2")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(Done()))
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.lease()).To(BeNil())
	})
	t.Run("IBM lists its picks one by one: a zone without its pick yet is not corrected", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.afterResize()
		f.workers = withState(f.workers, WorkerStateDeleting, "w0")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.events()).To(BeEmpty())
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}), "w1 is not spared before IBM picks in us-south-2")
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0", "w1"}))
	})
	t.Run("a spared victim waits for the workload cluster to be uncordoned", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.afterResize()
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.workers = withState(f.workers, WorkerStateDeleting, "w1", "w2")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0", "w1"}), "nothing is given up yet")
		g.Expect(f.events()).To(BeEmpty())
	})
	t.Run("IBM removed nothing for WorkerStuckAfter: every victim is uncordoned and the scale-down ends", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.afterResize()
		f.pool.Status.Disruption.IBMRequestTime = metav1.NewTime(poolTestNow.Add(-WorkerStuckAfter - time.Minute))
		f.expect()
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeNotDone))
		g.Expect(f.events()).To(ConsistOf(ContainSubstring("removes no worker, not the predicted workers w0, w1")))
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.lease()).To(BeNil())
	})
	t.Run("raised replicas after the resize do not stop the removal", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.afterResize()
		f.machinePool.Spec.Replicas = ptr.To[int32](4)
		f.workers = withState(f.workers, WorkerStateDeleting, "w0", "w1")
		f.expect()
		// gomock fails on a ResizeWorkerPool: the growth waits until the removal in flight is done.
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
	})
}

func TestMachinePoolScopeScaleDownAborts(t *testing.T) {
	t.Run("replicas raised before the resize: uncordon and release", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		// A human cordoned w1's Node before the scale-down: CAPIBM's cordon did not flip it, so it stays cordoned.
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.2"}, node)).To(Succeed())
		node.Spec.Unschedulable = true
		g.Expect(f.workload.Update(context.Background(), node)).To(Succeed())
		f.cordonVictims(false)
		f.machinePool.Spec.Replicas = ptr.To[int32](4)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(Done()))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.2"}), "only CAPIBM's cordon is reverted")
		g.Expect(f.events()).To(ConsistOf("Normal NodeUncordoned reverted CAPIBM's cordon of 2 Nodes of pool workers: the reason " +
			"for the removal went away before the IBM call; uncordoning what CAPIBM cordoned"))
		g.Expect(f.lease()).To(BeNil())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.draining().Status).To(Equal(metav1.ConditionFalse))
		g.Expect(f.ready().Status).To(Equal(metav1.ConditionTrue))
	})
	t.Run("an abort waits for the workload cluster to uncordon", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.cordonVictims(false)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.machinePool.Spec.Replicas = ptr.To[int32](4)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))))
		g.Expect(f.lease()).NotTo(BeNil(), "the token is kept until the Nodes are uncordoned")
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	})
	t.Run("IBM removes the victims without a CAPIBM call: hold, then end", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		f.workers = withState(f.workers, WorkerStateDeleting, "w0", "w1")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonResizing))), "no ResizeWorkerPool")
		f.workers = withState(f.workers, WorkerStateDeleted, "w0", "w1")
		f.expect()
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeNotDone))
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.lease()).To(BeNil())
	})
}

func TestMachinePoolScopeScaleDownResumes(t *testing.T) {
	t.Run("a drain the Node markers show resumes after a lost status", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.clock.Step(-5 * time.Minute)
		f.cordonVictims(false)
		f.clock.Step(5 * time.Minute)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0", "w1"}))
		g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow.Add(-5*time.Minute)), "the start comes from the Node markers")
		g.Expect(f.lease()).NotTo(BeNil(), "the token is taken again")
		g.Expect(f.events()).To(BeEmpty(), "a resumed drain is not a new one")
	})
	t.Run("Node markers without a wanted scale-down are reverted", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.machinePool.Spec.Replicas = ptr.To[int32](4)
		f.cordonVictims(false)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(Done()))
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.events()).To(ConsistOf(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 2 Nodes of pool workers")))
	})
	t.Run("Node markers that a resize would not cover are reverted and the scale-down is planned anew", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
		nodes := VictimNodes(readNodes(t, f.workload), poolTestAccount, poolTestClusterID, []string{"w3"})
		g.Expect(helper.Cordon(context.Background(), &nodes[0], drain.CordonOptions{Owner: DrainOwner(f.pool)})).To(Succeed())
		f.expect()
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeNotDone))
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))

		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0", "w1"}))
	})
	t.Run("a replace in flight comes first", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace,
			Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w3"}}
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason)))
		g.Expect(f.calls.all()).To(BeEmpty(), "no scale-down step runs")
		g.Expect(f.pool.Status.Disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationReplace))
	})
}

func TestMachinePoolScopeScaleDownResizeOnly(t *testing.T) {
	t.Run("the fallback resizes without a drain and says so", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		reconcile := func() Outcome {
			scope := f.scope()
			scope.scaleDownMode = ScaleDownResizeOnly
			return scope.ReconcileNormal(context.Background())
		}
		warning := "scale-down of pool workers (ScaleDownMode ResizeOnly; IBM is expected to remove workers w0, " +
			"w1): IBM picks the workers and CAPIBM does not drain them first, so their PodDisruptionBudgets may not be honoured; "

		f.expect()
		g.Expect(reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.ready().Reason).To(Equal(string(ReasonResizing)))
		g.Expect(f.ready().Message).To(Equal(warning + "moving on to the resize"))
		g.Expect(f.draining().Status).To(Equal(metav1.ConditionFalse))
		g.Expect(f.lease()).NotTo(BeNil(), "one removal per Cluster still holds")

		f.clock.Step(20 * time.Second)
		f.expect()
		reconcile()
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))

		f.clock.Step(20 * time.Second)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).Return(nil)
		g.Expect(reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.ready().Message).To(HavePrefix(warning))
		g.Expect(f.calls.all()).To(BeEmpty(), "no Node is touched")
		g.Expect(f.events()).To(BeEmpty(), "no drain events")

		// IBM removes other workers: they are recorded without a warning, since nothing was drained.
		f.ibmPool = ibmPool("workers", 1, "us-south-1", "us-south-2")
		f.workers = withState(f.workers, WorkerStateDeleting, "w2", "w3")
		f.expect()
		reconcile()
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w2", "w3"}))
		g.Expect(f.events()).To(BeEmpty())
	})
}

func TestMachinePoolScopeScaleDownWithoutVictims(t *testing.T) {
	t.Run("a pool IBM lists no worker of is resized without a drain", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.workers = f.workers[4:]
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).DoAndReturn(
			func(context.Context, string, string, int64) error {
				g.Expect(f.persisted).To(HaveLen(1), "lastDisruptionTime is persisted before the call")
				return nil
			})
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason)))
		g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow))
		g.Expect(f.lease()).To(BeNil(), "no token for a resize that removes no worker")

		f.clock.Step(time.Minute)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonResizing))), "no second call within the settle delay")
	})
	t.Run("a refusal holds", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.workers = f.workers[4:]
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).
			Return(apiError(kubernetesservice.OpResizeWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode))
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
		message := f.ready().Message
		f.clock.Step(time.Minute)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
		g.Expect(f.ready().Message).To(Equal(message))
	})
	for _, tc := range []struct {
		name       string
		modify     func(f *scaleDownFixture)
		wantReason string
	}{
		{name: "the cluster state forbids it", wantReason: string(ReasonWaitingForClusterState),
			modify: func(f *scaleDownFixture) { f.cp.Status.State = "unavailable" }},
		{name: "IBM's minimum would be broken", wantReason: string(ReasonWaitingForClusterMinimum),
			modify: func(f *scaleDownFixture) { f.workers = nil }},
		{name: "a failed persist", modify: func(f *scaleDownFixture) { f.persistErr = errors.New("conflict") }},
	} {
		t.Run("no resize when "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			f.workers = f.workers[4:]
			tc.modify(f)
			f.expect()
			outcome := f.reconcile()
			if tc.wantReason == "" {
				g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
				return
			}
			g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, tc.wantReason)))
		})
	}
}

// TestMachinePoolScopeScaleDownDrainLimits covers the MachinePool's drain knobs in a scale-down: a blocking
// PodDisruptionBudget holds the resize until the drain time limit, which then names the pods left; a time limit also
// runs out while the workload cluster is gone; and both exclusions need no workload cluster.
func TestMachinePoolScopeScaleDownDrainLimits(t *testing.T) {
	t.Run("a blocking budget holds the resize until the time limit", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, map[string]error{"web-w0": pdbRefusal()})
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.draining().Reason).To(Equal(string(ReasonDrainBlocked)))
		g.Expect(f.events()).To(ContainElement(HavePrefix("Warning DrainBlocked")))

		f.clock.Step(20 * time.Second)
		f.calls.all()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))), "no resize while a budget blocks")
		g.Expect(f.calls.count("evict apps/web-w0")).To(Equal(1), "the eviction is retried, never forced")
		g.Expect(f.draining().Reason).To(Equal(string(ReasonDrainBlocked)))
		g.Expect(f.events()).To(BeEmpty(), "DrainBlocked is not announced again")

		f.clock.Step(45 * time.Second)
		f.calls.all()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
		g.Expect(f.calls.count("evict")).To(BeZero(), "no eviction after the limit")
		events := f.events()
		g.Expect(events).To(ContainElement(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 60s")))
		g.Expect(events).To(ContainElement(HavePrefix("Normal DrainCompleted")))

		f.clock.Step(20 * time.Second)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).Return(nil)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.events()).To(BeEmpty(), "the timeout is announced once")
	})
	t.Run("a time limit runs out while the workload cluster is gone", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](60)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
		g.Expect(f.events()).To(ConsistOf(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 60s (MachinePool " +
			"spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed since 2026-11-16T11:50:00Z while the workload cluster is not connected")))
	})
	t.Run("both exclusions need no workload cluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: "",
			clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: ""}
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.ready().Message).To(ContainSubstring("the drain is excluded"))
		g.Expect(f.ready().Message).NotTo(ContainSubstring("cordon"), "the drain is excluded: nothing is cordoned")
		g.Expect(f.events()).To(BeEmpty(), "nothing is announced without the workload cluster")
	})
}

func TestMachinePoolScopeMarkedWorkers(t *testing.T) {
	g := NewWithT(t)
	f := newScaleDownFixture(t, nil)
	f.cordonVictims(false)
	scope := f.loadedScope(t)
	view, err := scope.readWorkloadNodes(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	marked, err := scope.markedWorkers(view, poolTestPoolID, f.workers)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(marked).To(Equal([]string{"w0", "w1"}))
	marked, err = scope.markedWorkers(view, poolTestPoolID, withState(f.workers, WorkerStateDeleted, "w0"))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(marked).To(Equal([]string{"w1"}), "a deleted worker is no victim")
	marked, err = scope.markedWorkers(workloadNodes{}, poolTestPoolID, f.workers)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(marked).To(BeEmpty(), "nothing is known without the workload cluster")
	scope.ControlPlane.Status.ClusterCRN = "not-a-crn"
	_, err = scope.markedWorkers(view, poolTestPoolID, f.workers)
	g.Expect(err).To(HaveOccurred())
}

// addWorker adds a ready worker of the test pool to IBM's listing, with its Node and a pod on that Node.
//
// Parameters:
//   - id is the worker ID.
//   - zone is its zone.
//   - nodeName is the name of its Node.
func (f *scaleDownFixture) addWorker(id, zone, nodeName string) {
	f.t.Helper()
	g := NewWithT(f.t)
	f.workers = append(f.workers, sdWorker(id, poolTestPoolID, zone))
	g.Expect(f.workload.Create(context.Background(), victimNode(nodeName, id))).To(Succeed())
	g.Expect(f.workload.Create(context.Background(), workloadPod("web-"+id, nodeName))).To(Succeed())
}

// TestMachinePoolScopeScaleDownZones checks that the scale-down plans over every zone IBM's resize changes: the zones
// of IBM's pool, which can be more than spec.zones for a hand-made pool the object took over by name, and an unbalanced
// zone, which IBM's resize shrinks by fewer workers. No worker the resize removes may go undrained.
func TestMachinePoolScopeScaleDownZones(t *testing.T) {
	t.Run("an adopted pool with a zone more than spec.zones drains the victims of every IBM zone", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.ibmPool = ibmPool("workers", 2, "us-south-1", "us-south-2", "us-south-3")
		f.addWorker("w4", "us-south-3", "10.0.0.5")
		f.addWorker("w5", "us-south-3", "10.0.0.6")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0", "w1", "w4"}))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2", "10.0.0.5"}))
	})
	t.Run("workers listed only in a zone outside spec.zones are drained, never resized away undrained", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.ibmPool = ibmPool("workers", 2, "us-south-1", "us-south-2", "us-south-3")
		f.workers = f.workers[4:]
		f.addWorker("w4", "us-south-3", "10.0.0.5")
		f.addWorker("w5", "us-south-3", "10.0.0.6")
		f.expect()
		// gomock fails on a ResizeWorkerPool: the worker IBM removes is drained first.
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w4"}))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.5"}))
		g.Expect(f.lease()).NotTo(BeNil())
	})
	t.Run("an unbalanced pool drains only the zones IBM shrinks", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		// An out-of-band `worker rm` left us-south-2 with w1 only.
		f.workers = withState(f.workers, WorkerStateDeleted, "w3")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0"}))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}), "us-south-2's only Node stays schedulable")
	})
}

// TestMachinePoolScopeScaleDownPreCallClusterState checks that the pre-call gate reads the IBM cluster state again: a
// cluster that turns unavailable during a long drain gets no resize (PoolMutationAllowed).
func TestMachinePoolScopeScaleDownPreCallClusterState(t *testing.T) {
	for _, state := range []string{"unavailable", "scaled_down"} {
		t.Run("no_resize_when_the_cluster_turned_"+state+"_during_the_drain", func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
			f.cordonVictims(true)
			f.cp.Status.State = state
			f.expect()
			// gomock fails on a ResizeWorkerPool.
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterState))))
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
			g.Expect(f.ready().Message).To(ContainSubstring("IBM cluster " + poolTestClusterID + " is " + state +
				"; IBM accepts no worker pool change in that state; the drained Nodes stay cordoned"))
			g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
			g.Expect(f.persisted).To(BeEmpty(), "nothing is persisted for a call that is not sent")
		})
	}
}

// TestMachinePoolScopeScaleDownRefusedThenRaised checks that raising spec.replicas after IBM refused the resize gives
// the scale-down up at once (the refusal left IBM's pool as it was, so the Nodes are uncordoned and the token released,
// as before the call), while a call whose outcome is unknown keeps the Nodes cordoned until WorkerStuckAfter.
func TestMachinePoolScopeScaleDownRefusedThenRaised(t *testing.T) {
	refusals := []struct {
		name string
		err  error
		auto bool
	}{
		{name: "E1437", err: apiError(kubernetesservice.OpResizeWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode)},
		{name: "autoscaled_403", auto: true, err: &kubernetesservice.APIError{Operation: kubernetesservice.OpResizeWorkerPool,
			StatusCode: http.StatusForbidden, Description: "Forbidden. The request will not be permitted."}},
	}
	for _, tc := range refusals {
		t.Run(tc.name+"_then_replicas_raised_uncordons_and_releases", func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
			f.cordonVictims(true)
			f.ibmPool.AutoscaleEnabled = tc.auto
			f.expect()
			f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).Return(tc.err)
			g.Expect(f.reconcile().Kind).To(Equal(OutcomeNotDone))
			f.events()

			f.clock.Step(time.Minute)
			f.machinePool.Spec.Replicas = ptr.To[int32](4)
			f.expect()
			g.Expect(f.reconcile()).To(Equal(Done()))
			g.Expect(f.cordoned()).To(BeEmpty())
			g.Expect(f.lease()).To(BeNil())
			g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
			g.Expect(f.events()).To(ConsistOf(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 2 Nodes of pool " +
				"workers: the reason for the removal went away after IBM refused the call of 2026-11-16T12:00:00Z")))
			g.Expect(f.ready().Status).To(Equal(metav1.ConditionTrue))
		})
	}
	t.Run("replicas raised after an unknown answer keep the Nodes cordoned until WorkerStuckAfter", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
		f.cordonVictims(true)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).
			Return(apiError(kubernetesservice.OpResizeWorkerPool, http.StatusInternalServerError, ""))
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTransient))

		f.clock.Step(10 * time.Minute)
		f.machinePool.Spec.Replicas = ptr.To[int32](4)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.ready().Message).To(ContainSubstring("no second call is made, and the Nodes stay cordoned until IBM acts"))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}), "IBM may still act on the call")
		g.Expect(f.lease()).NotTo(BeNil())
	})
	// The record of a refusal is the WorkerPoolReady condition; whenever it no longer proves that IBM took nothing,
	// the removal waits as for a call IBM may still carry out (fails closed).
	notProven := []struct {
		name   string
		modify func(f *scaleDownFixture)
	}{
		{name: "the capacity rule of the pre-call gate reported over the refusal", modify: func(f *scaleDownFixture) {
			f.clock.Step(DisruptionSettleDelay)
			f.workers = withState(f.workers, "provision_failed", "o0", "o1", "w2", "w3")
			f.expect()
			g := NewWithT(f.t)
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
			g.Expect(f.ready().Message).NotTo(ContainSubstring(refusalMessage(ReasonWaitingForClusterMinimum)))
		}},
		{name: "IBM's pool already has the size the resize asked for", modify: func(f *scaleDownFixture) {
			f.ibmPool = ibmPool("workers", 1, "us-south-1", "us-south-2")
		}},
	}
	for _, tc := range notProven {
		t.Run("E1437_then_replicas_raised_waits_when_"+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
			f.cordonVictims(true)
			f.expect()
			f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(1)).
				Return(apiError(kubernetesservice.OpResizeWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode))
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
			tc.modify(f)

			f.clock.Step(time.Minute)
			f.machinePool.Spec.Replicas = ptr.To[int32](4)
			f.expect()
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
			g.Expect(f.ready().Message).To(ContainSubstring("no second call is made"))
			g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
			g.Expect(f.lease()).NotTo(BeNil())
		})
	}
}

// TestMachinePoolScopeScaleDownPreexistingRemoval checks that a worker IBM was already removing before the resize is
// no pick of the resize (VerifyScaleDownVictims with the controller's first-seen times): a victim of another zone is
// not spared, and the scale-down does not wait for that worker.
func TestMachinePoolScopeScaleDownPreexistingRemoval(t *testing.T) {
	g := NewWithT(t)
	f := newScaleDownFixture(t, nil)
	f.afterResize()
	f.workers = withState(f.workers, WorkerStateDeleting, "w0", "w2")
	f.transitionalSince = map[string]time.Time{"w2": poolTestNow.Add(-WorkerStuckAfter - time.Hour)}
	f.expect()
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
	g.Expect(f.events()).To(BeEmpty(), "no UnexpectedScaleDownVictim")
	g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}), "w1 is not spared before IBM picked in its zone")
	g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0", "w1"}))
}

// TestMachinePoolScopeScaleDownGuardBlocked checks that a guard failing while a scale-down is in flight does not
// freeze it: before the IBM call it is given up (uncordon, release), after the call it follows IBM, and the guard is
// reported once nothing is in flight.
func TestMachinePoolScopeScaleDownGuardBlocked(t *testing.T) {
	guards := []struct {
		name       string
		modify     func(f *scaleDownFixture)
		wantReason string
	}{
		{name: "replicas_not_divisible_by_the_zones", wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason,
			modify: func(f *scaleDownFixture) { f.machinePool.Spec.Replicas = ptr.To[int32](3) }},
		{name: "zero_replicas_without_AllowZeroReplicaPools", wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyZeroReplicasNotSupportedReason,
			modify: func(f *scaleDownFixture) { f.machinePool.Spec.Replicas = ptr.To[int32](0) }},
	}
	for _, tc := range guards {
		t.Run(tc.name+"_before_the_call_uncordons_and_releases", func(t *testing.T) {
			g := NewWithT(t)
			f := newScaleDownFixture(t, nil)
			f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
			f.cordonVictims(false)
			tc.modify(f)
			f.expect()
			outcome := f.reconcile()
			g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
			g.Expect(outcome.Reason).To(Equal(tc.wantReason))
			g.Expect(f.cordoned()).To(BeEmpty())
			g.Expect(f.lease()).To(BeNil())
			g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
			g.Expect(f.draining().Status).To(Equal(metav1.ConditionFalse))
			g.Expect(f.ready().Reason).To(Equal(tc.wantReason))
			g.Expect(f.events()).To(ConsistOf(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 2 Nodes")))
		})
	}
	t.Run("after the call the removal follows IBM, then the guard is reported", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.afterResize()
		f.machinePool.Spec.Replicas = ptr.To[int32](3)
		f.workers = withState(f.workers, WorkerStateDeleting, "w0", "w1")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonResizing))))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
		g.Expect(f.ready().Message).To(ContainSubstring("replicas=3 is not a multiple of 2 zones"), "the guard is named")

		f.workers = withState(f.workers, WorkerStateDeleted, "w0", "w1")
		f.expect()
		outcome := f.reconcile()
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.lease()).To(BeNil())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.ready().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason))
		g.Expect(f.ready().Message).NotTo(ContainSubstring("; replicas=3"), "the guard is not named twice")
	})
	t.Run("the Node markers of a lost status are reverted too", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.cordonVictims(false)
		f.machinePool.Spec.Replicas = ptr.To[int32](3)
		f.expect()
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.events()).To(ConsistOf(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 2 Nodes")))
	})
	t.Run("without a scale-down in flight the guard is reported at once", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.machinePool.Spec.Replicas = ptr.To[int32](3)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(Terminal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason,
			"replicas=3 is not a multiple of 2 zones; use 2 or 4 (IBM sizes worker pools per zone)")))
		g.Expect(f.calls.all()).To(Equal([]string{"list nodes"}), "only the Nodes are read, for this pool's markers")
	})
	t.Run("a pool name conflict makes no IBM call and says that the removal waits", func(t *testing.T) {
		g := NewWithT(t)
		f := newScaleDownFixture(t, nil)
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.cordonVictims(false)
		f.others = append(f.others, poolObject("twin", "workers"))
		// gomock fails on any IBM call.
		outcome := f.reconcile()
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.ready().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason))
		g.Expect(f.ready().Message).To(ContainSubstring("the ScaleDown removal of this pool is in flight; while the conflict lasts"))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
	})
}
