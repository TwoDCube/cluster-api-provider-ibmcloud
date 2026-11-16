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
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"

	. "github.com/onsi/gomega"
)

// The rollout tests check the pure parts of the worker rollout directly (SummarizeRollout, ReplaceProgress,
// MasterUpdateHoldsWorkers) and drive the replace flow reconcile by reconcile against a fake management cluster (which
// keeps the disruption token), a fake workload cluster with a Node and a pod per worker behind a fake ClusterCache, and
// a gomock IBM Cloud Kubernetes Service. gomock fails a test on any call it does not expect, which is how "no
// ReplaceWorker" is proven. The cases that need the controller (persisted phases, events, two pools) are in
// internal/controllers/managed.

// rolloutMasterMinor is the fixture master's minor, 1.36 (control plane status.distributionVersion 1.36.5_1537).
var rolloutMasterMinor = ibmversion.Minor{Major: 1, Minor: 36}

// behind returns w with a kubeVersion of the minor before the master's.
func behind(w kubernetesservice.Worker) kubernetesservice.Worker {
	w.KubeVersionActual, w.KubeVersionTarget, w.KubeVersionDesired = "1.35.9_1530", "1.35.9_1530", "1.35.9_1530"
	return w
}

// withFixPack returns w with IBM offering it fix pack 1.36.5_1545 of the master minor.
func withFixPack(w kubernetesservice.Worker) kubernetesservice.Worker {
	w.KubeVersionTarget = "1.36.5_1545"
	return w
}

func TestSummarizeRollout(t *testing.T) {
	after := poolTestNow.Add(-time.Hour)
	older, newer := map[string]time.Time{}, map[string]time.Time{}
	for _, id := range []string{"w0", "w1", "w2", "w3"} {
		older[id], newer[id] = after.Add(-time.Hour), after.Add(time.Minute)
	}
	upToDate := readyWorkers(4)
	testCases := []struct {
		name    string
		in      RolloutInput
		want    RolloutSummary
		wantErr string
	}{
		{name: "nothing due, nothing available", in: RolloutInput{Workers: upToDate, SizePerZone: 2, Zones: 2},
			want: RolloutSummary{Workers: 4}},
		{name: "manual_patch_available_is_counted_not_due",
			in:   RolloutInput{Workers: []kubernetesservice.Worker{withFixPack(upToDate[0]), withFixPack(upToDate[1]), upToDate[2], upToDate[3]}, SizePerZone: 2, Zones: 2},
			want: RolloutSummary{Workers: 4, PatchAvailable: 2, FixPack: "1.36.5_1545"}},
		{name: "automatic patch available is due",
			in: RolloutInput{Workers: []kubernetesservice.Worker{withFixPack(upToDate[0]), upToDate[1], upToDate[2], upToDate[3]}, SizePerZone: 2, Zones: 2,
				Policy: infrav1.WorkerUpdatePolicyAutomatic},
			want: RolloutSummary{Workers: 4, Due: 1, PatchAvailable: 1, FixPack: "1.36.5_1545", HasNext: true,
				Next: ReplacementPick{Worker: withFixPack(upToDate[0]), Decision: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable,
					Due: true, DueBecause: WorkerUpdatePatchAvailable}}}},
		{name: "minor_follow_is_due_under_Manual",
			in: RolloutInput{Workers: []kubernetesservice.Worker{behind(upToDate[0]), behind(upToDate[1]), upToDate[2], upToDate[3]}, SizePerZone: 2, Zones: 2},
			want: RolloutSummary{Workers: 4, Due: 2, HasNext: true, Next: ReplacementPick{Worker: behind(upToDate[0]),
				Decision: WorkerUpdateDecision{Class: WorkerUpdateMinorBehind, Due: true, DueBecause: WorkerUpdateMinorBehind}}}},
		{name: "rollout.after passed recycles workers whose Node is older, under both policies",
			in: RolloutInput{Workers: upToDate, SizePerZone: 2, Zones: 2, RolloutAfter: after, NodeCreated: older},
			want: RolloutSummary{Workers: 4, Due: 4, HasNext: true, Next: ReplacementPick{Worker: upToDate[0],
				Decision: WorkerUpdateDecision{Class: WorkerUpdateRolloutRequested, Due: true, DueBecause: WorkerUpdateRolloutRequested}}}},
		{name: "rollout.after passed leaves newer Nodes alone", in: RolloutInput{Workers: upToDate, SizePerZone: 2, Zones: 2,
			RolloutAfter: after, NodeCreated: newer}, want: RolloutSummary{Workers: 4}},
		{name: "a worker IBM removes is no candidate and keeps its place",
			in: RolloutInput{Workers: withState([]kubernetesservice.Worker{behind(upToDate[0]), behind(upToDate[1]), upToDate[2], upToDate[3]}, WorkerStateDeleting, "w0"), SizePerZone: 2, Zones: 2},
			want: RolloutSummary{Workers: 4, Due: 1, HasNext: true, Next: ReplacementPick{Worker: behind(upToDate[1]),
				Decision: WorkerUpdateDecision{Class: WorkerUpdateMinorBehind, Due: true, DueBecause: WorkerUpdateMinorBehind}}}},
		{name: "a replacement next to its deleting worker counts once", in: RolloutInput{SizePerZone: 2, Zones: 2,
			Workers: append(withState(upToDate, WorkerStateDeleting, "w0"), poolWorker("w4", "us-south-1", "provisioning", "normal"))},
			want: RolloutSummary{Workers: 4}},
		{name: "workers of other pools do not count", in: RolloutInput{Workers: append(upToDate, behind(sdWorker("o0", "other-pool", "us-south-1"))),
			SizePerZone: 2, Zones: 2}, want: RolloutSummary{Workers: 4}},
		{name: "more workers than IBM's size count all", in: RolloutInput{Workers: readyWorkers(6), SizePerZone: 2, Zones: 2},
			want: RolloutSummary{Workers: 6}},
		{name: "a negative size is rejected", in: RolloutInput{SizePerZone: -1}, wantErr: "neither negative"},
		{name: "an unknown policy is rejected", in: RolloutInput{Policy: "Sometimes"}, wantErr: "spec.workerUpdates.policy is \"Sometimes\""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := tc.in
			in.PoolID, in.MasterMinor, in.Now = poolTestPoolID, rolloutMasterMinor, poolTestNow
			got, err := SummarizeRollout(in)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
	t.Run("Replaced is n minus the due workers, never negative", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect(RolloutSummary{Workers: 6, Due: 4}.Replaced()).To(Equal(2))
		g.Expect(RolloutSummary{Workers: 1, Due: 2}.Replaced()).To(BeZero())
	})
}

func TestReplaceProgress(t *testing.T) {
	ready := map[string]time.Time{"w1": poolTestNow.Add(-time.Hour), "w2": poolTestNow.Add(-time.Hour), "w3": poolTestNow.Add(-time.Hour),
		"w4": poolTestNow.Add(-time.Minute)}
	replaced := append(withState(readyWorkers(4), WorkerStateDeleted, "w0"), poolWorker("w4", "us-south-1", WorkerStateDeployed, "normal"))
	testCases := []struct {
		name    string
		in      ReplaceProgressInput
		want    ReplaceProgressResult
		wantErr string
	}{
		{name: "before the call nothing is removed and the zone is one short", in: ReplaceProgressInput{Workers: readyWorkers(4),
			StillPublished: true}, want: ReplaceProgressResult{Waiting: []string{"w1", "w2", "w3"}}},
		{name: "IBM removes the victim", in: ReplaceProgressInput{Workers: withState(readyWorkers(4), WorkerStateDeleting, "w0"), StillPublished: true,
			NodeReadySince: ready}, want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true}}},
		{name: "the victim is no longer published: removed, no replacement yet",
			in:   ReplaceProgressInput{Workers: withState(readyWorkers(4), WorkerStateDeleted, "w0"), NodeReadySince: ready},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true}}},
		{name: "the replacement is created but provisioning",
			in: ReplaceProgressInput{Workers: append(withState(readyWorkers(4), WorkerStateDeleted, "w0"),
				poolWorker("w4", "us-south-1", "provisioning", "normal")), NodeReadySince: ready},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true, ReplacementCreated: true}, Waiting: []string{"w4"}}},
		{name: "the_replacement_Node_soaks_for_minReadySeconds",
			in: ReplaceProgressInput{Workers: replaced, NodeReadySince: ready, MinReadySeconds: 300},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true, ReplacementCreated: true},
				ReadyAt: poolTestNow.Add(4 * time.Minute), Waiting: []string{"w4"}}},
		{name: "the replacement is ready", in: ReplaceProgressInput{Workers: replaced, NodeReadySince: ready, MinReadySeconds: 60},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true, ReplacementCreated: true, ReplacementReady: true}}},
		{name: "a Node of another zone that is not Ready holds the next worker",
			in:   ReplaceProgressInput{Workers: replaced, NodeReadySince: map[string]time.Time{"w1": poolTestNow, "w2": poolTestNow, "w4": poolTestNow}},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true, ReplacementCreated: true}, Waiting: []string{"w3"}}},
		{name: "a victim IBM no longer lists needs the whole pool back",
			in: ReplaceProgressInput{Workers: append(readyWorkers(4)[1:], poolWorker("w4", "us-south-1", WorkerStateDeployed, "normal")),
				NodeReadySince: ready, Zones: 2},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true, ReplacementCreated: true, ReplacementReady: true}}},
		{name: "a victim IBM no longer lists, the pool still short", in: ReplaceProgressInput{Workers: readyWorkers(4)[1:], NodeReadySince: ready, Zones: 2},
			want: ReplaceProgressResult{Progress: RemovalProgress{Removing: true, Removed: true}}},
		{name: "the victim is required", in: ReplaceProgressInput{Victim: "-"}, wantErr: "needs PoolID and Victim"},
		{name: "the clock is required", in: ReplaceProgressInput{Now: time.Time{}}, wantErr: "Now is not set"},
		{name: "a negative soak is rejected", in: ReplaceProgressInput{MinReadySeconds: -1}, wantErr: "none may be negative"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := tc.in
			in.PoolID, in.SizePerZone = poolTestPoolID, 2
			switch in.Victim {
			case "":
				in.Victim = "w0"
			case "-":
				in.Victim = ""
			}
			if tc.name != "the clock is required" {
				in.Now = poolTestNow
			}
			got, err := ReplaceProgress(in)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestMasterUpdateHoldsWorkers(t *testing.T) {
	cp := func(state, version, target string, rolling bool) *controlplanev1.IBMCloudManagedControlPlane {
		c := &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev"},
			Status: controlplanev1.IBMCloudManagedControlPlaneStatus{ClusterID: "c1", State: state, DistributionVersion: version,
				TargetDistributionVersion: target}}
		if rolling {
			c.Status.Conditions = []metav1.Condition{{Type: controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition,
				Status: metav1.ConditionTrue, Reason: controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason}}
		}
		return c
	}
	testCases := []struct {
		name        string
		cp          *controlplanev1.IBMCloudManagedControlPlane
		wantHold    bool
		wantMessage string
	}{
		{name: "a steady master holds nothing", cp: cp("normal", "1.36.5_1537", "1.36.5_1537", false)},
		{name: "a steady master without a target holds nothing", cp: cp("normal", "1.36.5_1537", "", false)},
		{name: "IBM updating the master holds the rollout", cp: cp("updating", "1.36.5_1537", "1.36.5_1537", false), wantHold: true,
			wantMessage: "IBM cluster c1 is updating its master (state updating); workers are replaced once the master update has " +
				"finished, so that they come back on the master's version"},
		{name: "an IBM master patch holds the rollout", cp: cp("normal", "1.36.5_1537", "1.36.6_1540", false), wantHold: true,
			wantMessage: "IBM cluster c1 is updating its master from 1.36.5_1537 to 1.36.6_1540; workers are replaced once the master " +
				"update has finished, so that they come back on the master's version"},
		{name: "a requested minor holds the rollout", cp: cp("normal", "1.36.5_1537", "1.37.1_1550", false), wantHold: true,
			wantMessage: "IBM cluster c1 is updating its master from 1.36.5_1537 to 1.37.1_1550; workers are replaced once the master " +
				"update has finished, so that they come back on the master's version"},
		{name: "CAPIBM's own request before IBM shows it holds the rollout (RollingOut)", cp: cp("normal", "1.36.5_1537", "", true),
			wantHold: true, wantMessage: "IBMCloudManagedControlPlane dev reports a master update in progress (RollingOut); workers are " +
				"replaced once it has finished"},
		{name: "an unknown master version holds the rollout", cp: cp("normal", "", "", false), wantHold: true,
			wantMessage: "the master version of IBM cluster c1 is not known yet; workers are replaced once IBMCloudManagedControlPlane dev " +
				"has observed it"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			hold, message := MasterUpdateHoldsWorkers(tc.cp)
			g.Expect(hold).To(Equal(tc.wantHold))
			g.Expect(message).To(Equal(tc.wantMessage))
		})
	}
}

func TestReplaceRefusalMessage(t *testing.T) {
	g := NewWithT(t)
	g.Expect(replaceRefusalMessage(ReasonWaitingForClusterMinimum)).To(Equal("IBM refused the replace because the cluster would " +
		"fall below its minimum workers (E1437); the drained Node stays cordoned, and the replace is requested again at most once " +
		"every 3 minutes while CAPIBM's own guards allow it"))
	g.Expect(replaceRefusalMessage(ReasonExternallyAutoscaled)).To(HavePrefix("IBM refused the replace because its cluster autoscaler owns this pool"))
}

func TestNodeReadySince(t *testing.T) {
	created := metav1.NewTime(poolTestNow.Add(-time.Hour))
	testCases := []struct {
		name      string
		node      corev1.Node
		wantSince time.Time
		wantReady bool
	}{
		{name: "no Ready condition", node: corev1.Node{}},
		{name: "not Ready", node: corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}}}},
		{name: "Ready since its transition", wantReady: true, wantSince: poolTestNow.Add(-time.Minute),
			node: corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(poolTestNow.Add(-time.Minute))}}}}},
		{name: "Ready without a transition time counts from its creation", wantReady: true, wantSince: created.Time,
			node: corev1.Node{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: created},
				Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			since, ready := nodeReadySince(&tc.node)
			g.Expect(ready).To(Equal(tc.wantReady))
			g.Expect(since).To(Equal(tc.wantSince))
		})
	}
}

func TestWorkerNodes(t *testing.T) {
	t.Run("finds each worker's Node by provider ID through the index", func(t *testing.T) {
		g := NewWithT(t)
		calls := &workloadCalls{}
		workload := newWorkloadClient(t, calls, nil, victimNode("10.0.0.1", "w0"), victimNode("10.0.0.2", "w1"))
		nodes, err := workerNodes(context.Background(), workload, poolTestAccount, poolTestClusterID, []string{"w0", "w1", "w9"})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(nodes).To(HaveLen(2), "a worker without a Node has no entry")
		g.Expect(nodes["w0"].Name).To(Equal("10.0.0.1"))
		g.Expect(nodes["w1"].Name).To(Equal("10.0.0.2"))
		g.Expect(calls.all()).To(Equal([]string{"find node " + providerIDOf("w0"), "find node " + providerIDOf("w1"),
			"find node " + providerIDOf("w9")}))
	})
	t.Run("two Nodes with one provider ID: the first by name", func(t *testing.T) {
		g := NewWithT(t)
		workload := newWorkloadClient(t, &workloadCalls{}, nil, victimNode("10.0.0.9", "w0"), victimNode("10.0.0.1", "w0"))
		nodes, err := workerNodes(context.Background(), workload, poolTestAccount, poolTestClusterID, []string{"w0"})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(nodes["w0"].Name).To(Equal("10.0.0.1"))
	})
	t.Run("a cache without the index fails with the worker named", func(t *testing.T) {
		g := NewWithT(t)
		workload := fake.NewClientBuilder().WithScheme(poolScheme(t)).Build()
		_, err := workerNodes(context.Background(), workload, poolTestAccount, poolTestClusterID, []string{"w0"})
		g.Expect(err).To(MatchError(ContainSubstring("finding the Node of worker w0 by its provider ID")))
	})
}

func TestRolloutOutcome(t *testing.T) {
	gated := NotDone(IntervalGuarded, string(ReasonWaitingForSettledCluster))
	provisioning := NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason)
	notReady := NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWorkersNotReadyReason)
	draining := NotDone(IntervalDrainRetry, string(ReasonDrainingNode))
	failed := Transient(errors.New("boom"))
	throttled := RateLimited(time.Minute)
	refused := Terminal(MachinePoolIBMAPIErrorReason, "grant it the roles")
	for _, tc := range []struct {
		name               string
		rollout, readiness Outcome
		want               Outcome
	}{
		{name: "a finished step leaves readiness", rollout: Done(), readiness: provisioning, want: provisioning},
		{name: "a wait gives way to a sooner readiness poll", rollout: gated, readiness: provisioning, want: provisioning},
		{name: "a wait stays when readiness polls no sooner", rollout: gated, readiness: notReady, want: gated},
		{name: "a wait stays when the pool is ready", rollout: gated, readiness: Done(), want: gated},
		{name: "an active step keeps its outcome", rollout: draining, readiness: provisioning, want: draining},
		{name: "a failure keeps its backoff", rollout: failed, readiness: provisioning, want: failed},
		{name: "a 429 keeps its Retry-After", rollout: throttled, readiness: provisioning, want: throttled},
		{name: "a condition for a human stays", rollout: refused, readiness: provisioning, want: refused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(rolloutOutcome(tc.rollout, tc.readiness)).To(Equal(tc.want))
		})
	}
}

func TestZoneAutobalance(t *testing.T) {
	g := NewWithT(t)
	pool := ibmPool("workers", 2, "us-south-1")
	pool.Zones = append(pool.Zones, kubernetesservice.WorkerPoolZone{ID: "us-south-2"})
	g.Expect(zoneAutobalance(&pool, "us-south-1")).To(BeTrue())
	g.Expect(zoneAutobalance(&pool, "us-south-2")).To(BeFalse())
	g.Expect(zoneAutobalance(&pool, "us-south-3")).To(BeFalse(), "a zone IBM does not list fails safe")
}

// rolloutNodeCreated is the creation time of every Node of the rollout fixture: a day before the clock starts.
var rolloutNodeCreated = poolTestNow.Add(-24 * time.Hour)

// rolloutFixture is a pool fixture with four ready workers w0..w3 of the pool (w0, w2 in us-south-1; w1, w3 in
// us-south-2) on fix pack 1.36.5_1540 of the master minor 1.36, two ready workers o0, o1 of another pool, which keep
// IBM's minimum, and a connected workload cluster with a Node (created a day ago) and a pod for every worker. IBM's pool
// and the MachinePool ask for two workers per zone, and the last removal of the Cluster was an hour ago.
type rolloutFixture struct {
	*poolFixture
	calls    *workloadCalls
	workload client.Client
	// workers are what ListWorkers answers; ibmPool is what ListWorkerPools answers.
	workers []kubernetesservice.Worker
	ibmPool kubernetesservice.WorkerPool
}

// newRolloutFixture returns the rollout fixture.
//
// Parameters:
//   - t is the test.
//   - refusals are the evictions a PodDisruptionBudget refuses, by pod name.
func newRolloutFixture(t *testing.T, refusals map[string]error) *rolloutFixture {
	t.Helper()
	f := &rolloutFixture{poolFixture: newPoolFixture(t), calls: &workloadCalls{}, workers: sdWorkers(),
		ibmPool: ibmPool("workers", 2, "us-south-1", "us-south-2")}
	f.pool.Status.PoolID = poolTestPoolID
	f.pool.Status.LastDisruptionTime = metav1.NewTime(poolTestNow.Add(-time.Hour))
	objs := make([]client.Object, 0, 12)
	for _, w := range f.workers {
		node := victimNode(sdNodeName(w.ID), w.ID)
		node.CreationTimestamp = metav1.NewTime(rolloutNodeCreated)
		objs = append(objs, node, workloadPod("web-"+w.ID, sdNodeName(w.ID)))
	}
	f.workload = newWorkloadClient(t, f.calls, refusals, objs...)
	f.clusterCache = clustercache.NewFakeClusterCache(f.workload, client.ObjectKey{Namespace: poolTestNamespace, Name: poolTestCluster})
	return f
}

// minorBehind puts the workers a minor behind the master, which makes them due under both policies: every pool
// follows the master minor.
func (f *rolloutFixture) minorBehind(ids ...string) {
	for i := range f.workers {
		for _, id := range ids {
			if f.workers[i].ID == id {
				f.workers[i] = behind(f.workers[i])
			}
		}
	}
}

// expect expects one ListWorkerPools and one ListWorkers answering the fixture's pool and workers, and returns the
// calls for gomock.InOrder.
func (f *rolloutFixture) expect() []any {
	return []any{
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{f.ibmPool}, nil),
		f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(f.workers, nil),
	}
}

// reconcile runs ReconcileNormal once with a new scope, as the controller does, and returns the scope too.
func (f *rolloutFixture) reconcile() Outcome {
	outcome, _ := f.reconcileScope()
	return outcome
}

// reconcileScope runs ReconcileNormal once with a new scope and returns its outcome and the scope.
func (f *rolloutFixture) reconcileScope() (Outcome, *MachinePoolScope) {
	scope := f.scope()
	return scope.ReconcileNormal(context.Background()), scope
}

// upToDate returns the WorkersUpToDate condition of the pool.
func (f *rolloutFixture) upToDate() *metav1.Condition {
	return condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition)
}

// cordoned returns the names of the cordoned Nodes, sorted.
func (f *rolloutFixture) cordoned() []string {
	var names []string
	for _, node := range readNodes(f.t, f.workload) {
		if node.Spec.Unschedulable {
			names = append(names, node.Name)
		}
	}
	return names
}

// lease returns the disruption token Lease, or nil.
func (f *rolloutFixture) lease() *coordinationv1.Lease {
	f.t.Helper()
	lease := &coordinationv1.Lease{}
	err := f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: poolTestNamespace, Name: DisruptionTokenName(poolTestCluster)}, lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return lease
}

// inPhase records a replace of w0 in phase whose drain started ten minutes ago, with the token held.
func (f *rolloutFixture) inPhase(phase infrav1.WorkerDisruptionPhase) {
	f.others = append(f.others, heldLease(DisruptionTokenHolder(f.pool)))
	f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace, Phase: phase,
		WorkerIDs: []string{"w0"}, DrainStartTime: metav1.NewTime(poolTestNow.Add(-10 * time.Minute))}
}

// drainedVictim cordons w0's Node for the pool as an earlier reconcile would have and removes its pod.
func (f *rolloutFixture) drainedVictim() {
	f.t.Helper()
	g := NewWithT(f.t)
	helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	nodes := VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0"})
	g.Expect(helper.Cordon(context.Background(), &nodes[0], drain.CordonOptions{Owner: DrainOwner(f.pool)})).To(Succeed())
	g.Expect(f.workload.Delete(context.Background(), workloadPod("web-w0", ""))).To(Succeed())
	f.calls.all()
}

// afterCall records the replace of w0 in phase WaitingForIBM with the call sent at sentAt, the victim drained.
func (f *rolloutFixture) afterCall(sentAt time.Time) {
	f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForIBM)
	f.pool.Status.Disruption.IBMRequestTime = metav1.NewTime(sentAt)
	f.pool.Status.LastDisruptionTime = metav1.NewTime(sentAt)
	f.drainedVictim()
}

// addReplacement lists w0 as deleted and its replacement w4 in us-south-1 in state, deletes w0's Node as IBM does while it
// deletes a replaced worker (observed on IBM Cloud), and creates w4's Node, Ready since readySince.
func (f *rolloutFixture) addReplacement(state string, readySince time.Time) {
	f.t.Helper()
	NewWithT(f.t).Expect(f.workload.Delete(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: sdNodeName("w0")}})).To(Succeed())
	w4 := poolWorker("w4", "us-south-1", state, "normal")
	f.workers = append(withState(f.workers, WorkerStateDeleted, "w0"), w4)
	node := victimNode("10.0.0.5", "w4")
	node.CreationTimestamp = metav1.NewTime(readySince)
	node.Status.Conditions[0].LastTransitionTime = metav1.NewTime(readySince)
	NewWithT(f.t).Expect(f.workload.Create(context.Background(), node)).To(Succeed())
}

func TestMachinePoolScopeRollout(t *testing.T) {
	t.Run("one_replace_drains_then_calls_ReplaceWorker_update_true_then_waits_for_the_replacement", testRolloutReplacesOneWorker)
}

// testRolloutReplacesOneWorker follows one replace reconcile by reconcile: every worker is a minor behind after a master
// upgrade, IBM's order picks w0, and the phases run Draining → WaitingForVolumeDetach → WaitingForIBM →
// WaitingForRemoval → WaitingForReplacement → idle. gomock.InOrder proves that ReplaceWorker comes once, with
// update=true, after the drain, and its DoAndReturn that phase WaitingForIBM is persisted and the pod gone when it is
// sent.
func testRolloutReplacesOneWorker(t *testing.T) {
	g := NewWithT(t)
	f := newRolloutFixture(t, nil)
	f.minorBehind("w0", "w1", "w2", "w3")
	replace := f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).DoAndReturn(
		func(context.Context, string, string, bool) error {
			g.Expect(f.persisted).To(HaveLen(1), "status is persisted before the call")
			g.Expect(f.persisted[0].Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
			g.Expect(f.persisted[0].IBMRequestTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
			pods := &corev1.PodList{}
			g.Expect(f.workload.List(context.Background(), pods, client.MatchingFields{"spec.nodeName": sdNodeName("w0")})).To(Succeed())
			g.Expect(pods.Items).To(BeEmpty(), "no replace while the victim still runs evictable pods")
			return nil
		})
	gomock.InOrder(append(append(append(f.expect(), f.expect()...), f.expect()...), replace)...)

	// 1. Every gate passes, the token is taken, w0's Node is cordoned and its pod evicted; no other Node is touched.
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
	g.Expect(f.calls.all()).To(Equal([]string{"list nodes", "cordon 10.0.0.1", "list pods 10.0.0.1", "evict apps/web-w0"}))
	g.Expect(f.lease().Spec.HolderIdentity).To(Equal(ptr.To(DisruptionTokenHolder(f.pool))))
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace,
		Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0"}, DrainStartTime: metav1.NewTime(poolTestNow)}))
	g.Expect(f.upToDate().Status).To(Equal(metav1.ConditionFalse))
	g.Expect(f.upToDate().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateDrainingNodeReason))
	g.Expect(f.upToDate().Message).To(Equal("replaced 0 of 4; worker w0 (MinorBehind): cordoning every victim Node, then evicting their pods"))
	g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Status).To(Equal(metav1.ConditionTrue),
		"WorkersUpToDate is not part of the pool's readiness")
	g.Expect(f.events()).To(ConsistOf("Normal DrainStarted cordoning 1 Nodes of pool workers, then evicting their pods"))

	// 2. The pod is gone: on to the volume detach wait.
	f.clock.Step(20 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	g.Expect(f.events()).To(ConsistOf("Normal DrainCompleted the 1 Nodes of pool workers are drained"))

	// 3. Volumes detached, the pre-call gate passes: WaitingForIBM is persisted, then ReplaceWorker(update=true) once.
	f.clock.Step(20 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
	g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow.Add(40 * time.Second)))
	g.Expect(f.events()).To(ConsistOf("Normal WorkerReplaceRequested asked IBM to replace worker w0 of pool workers with update=true (MinorBehind)"))
	g.Expect(f.upToDate().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReplacingWorkerReason))
	g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}), "the victim stays cordoned")

	// 4. IBM deletes w0: wait, no second call.
	f.workers = withState(f.workers, WorkerStateDeleting, "w0")
	f.expect()
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
	g.Expect(f.upToDate().Message).To(HavePrefix("replaced 1 of 4; worker w0 (MinorBehind): "),
		"the worker IBM removes has handed its place over")

	// 5. w0 is deleted and its replacement w4 provisions: WaitingForReplacement.
	f.addReplacement("provisioning", poolTestNow.Add(2*time.Minute))
	f.expect()
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonWaitingForReplacement))))
	g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForReplacement))
	g.Expect(f.pool.Spec.ProviderIDList).NotTo(ContainElement(providerIDOf("w0")))

	// 6. w4 is provisioning: the replace waits for it.
	f.expect()
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonWaitingForReplacement))))
	g.Expect(f.upToDate().Message).To(ContainSubstring("worker w4 not yet deployed, normal and Ready"))

	// 7. w4 is deployed and its Node Ready: the token is released, status.disruption cleared, and the rollout moves on.
	f.workers = withState(f.workers, WorkerStateDeployed, "w4")
	f.workers[len(f.workers)-1].KubeVersionActual = "1.36.5_1545"
	f.expect()
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason)))
	g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(f.lease()).To(BeNil())
	g.Expect(f.upToDate().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason))
	g.Expect(f.upToDate().Message).To(Equal("replaced 1 of 4; the next worker starts once the cluster has settled"))
	g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Status).To(Equal(metav1.ConditionFalse))
	g.Expect(f.resolver.holders).To(HaveEach(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{})), "every holder is the InfraCluster")

	// 8. Between workers only the settle delay holds the next one: RollingOut, w1 not cordoned yet.
	f.expect()
	f.clock.Step(30 * time.Second)
	g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason)))
	g.Expect(f.upToDate().Message).To(HavePrefix("replaced 1 of 4; worker w2 (MinorBehind): IBMCloudManagedMachinePool workers removed workers at "),
		"IBM's order picks the next worker by zone, then ID")
	g.Expect(f.cordoned()).To(BeEmpty())
}

// TestMachinePoolScopeRolloutGates checks every gate of the Gated step for a due worker: each one reports its reason
// on WorkersUpToDate, cordons nothing, records no status.disruption and sends no ReplaceWorker (gomock).
func TestMachinePoolScopeRolloutGates(t *testing.T) {
	testCases := []struct {
		name        string
		modify      func(f *rolloutFixture)
		wantReason  string
		wantMessage string
		wantOutcome Outcome
	}{
		{name: "unsupported_cluster_state", wantReason: string(ReasonWaitingForClusterState),
			modify: func(f *rolloutFixture) { f.cp.Status.State = "unsupported" },
			wantMessage: "replaced 0 of 4; worker w0 (MinorBehind): IBM cluster " + poolTestClusterID + " runs an unsupported version: " +
				"IBM does not add or reload workers on an unsupported version; upgrade the master first"},
		{name: "unavailable_cluster_state", wantReason: string(ReasonWaitingForClusterState),
			modify: func(f *rolloutFixture) { f.cp.Status.State = "unavailable" },
			wantMessage: "replaced 0 of 4; worker w0 (MinorBehind): IBM cluster " + poolTestClusterID + " is unavailable; IBM accepts " +
				"no worker pool change in that state"},
		{name: "master_pending_IBM_patch", wantReason: string(ReasonWaitingForControlPlaneUpgrade),
			modify: func(f *rolloutFixture) { f.cp.Status.TargetDistributionVersion = "1.36.6_1540" },
			wantMessage: "replaced 0 of 4; worker w0 (MinorBehind): IBM cluster " + poolTestClusterID + " is updating its master from " +
				"1.36.5_1537 to 1.36.6_1540; workers are replaced once the master update has finished, so that they come back on the master's version"},
		{name: "master_update_requested_by_CAPIBM", wantReason: string(ReasonWaitingForControlPlaneUpgrade),
			modify: func(f *rolloutFixture) {
				f.cp.Status.Conditions = []metav1.Condition{{Type: controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition,
					Status: metav1.ConditionTrue, Reason: controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason}}
			}},
		{name: "another_worker_is_changing", wantReason: string(ReasonWaitingForSettledCluster),
			modify: func(f *rolloutFixture) { f.workers = withState(f.workers, "provisioning", "o1") },
			wantMessage: "replaced 0 of 4; worker w0 (MinorBehind): worker o1 is provisioning; workers are removed only while no " +
				"worker of the cluster is changing"},
		{name: "the_settle_delay_of_another_pool_names_it", wantReason: string(ReasonWaitingForSettledCluster),
			modify: func(f *rolloutFixture) {
				other := poolObject("other", "other")
				other.Status.LastDisruptionTime = metav1.NewTime(poolTestNow.Add(-time.Minute))
				f.others = append(f.others, other)
			},
			wantMessage: "replaced 0 of 4; worker w0 (MinorBehind): IBMCloudManagedMachinePool other removed workers at " +
				"2026-11-16T11:59:00Z; the next removal waits until 2026-11-16T12:02:00Z, so that IBM's worker list shows the last one"},
		{name: "a_1_worker_IKS_cluster_needs_a_second_worker", wantReason: string(ReasonWaitingForCapacity),
			modify: func(f *rolloutFixture) {
				f.pool.Spec.Zones = []infrav1.WorkerPoolZone{{Name: "us-south-1"}}
				f.machinePool.Spec.Replicas = ptr.To[int32](1)
				f.ibmPool = ibmPool("workers", 1, "us-south-1")
				f.workers = []kubernetesservice.Worker{behind(poolWorker("w0", "us-south-1", WorkerStateDeployed, "normal"))}
			},
			wantMessage: "replaced 0 of 1; worker w0 (MinorBehind): the cluster has 1 ready workers; removing 1 would leave 0, below " +
				"IBM's minimum of 1 workers for a Kubernetes cluster (E1437); scale MachinePool workers to 2 to allow a rolling replacement"},
		{name: "a 2-worker ROKS cluster needs a third worker", wantReason: string(ReasonWaitingForCapacity),
			modify: func(f *rolloutFixture) {
				f.cp.Spec.Distribution = controlplanev1.DistributionOpenShift
				f.pool.Spec.Zones = []infrav1.WorkerPoolZone{{Name: "us-south-1"}}
				f.machinePool.Spec.Replicas = ptr.To[int32](2)
				f.ibmPool = ibmPool("workers", 2, "us-south-1")
				f.workers = []kubernetesservice.Worker{behind(poolWorker("w0", "us-south-1", WorkerStateDeployed, "normal")),
					behind(poolWorker("w2", "us-south-1", WorkerStateDeployed, "normal"))}
			},
			wantMessage: "replaced 0 of 2; worker w0 (MinorBehind): the cluster has 2 ready workers; removing 1 would leave 1, below " +
				"IBM's minimum of 2 workers for a OpenShift cluster (E1437); scale MachinePool workers to 3 to allow a rolling replacement"},
		{name: "an autoscaled pool", wantReason: string(ReasonExternallyAutoscaled), wantOutcome: Done(),
			modify: func(f *rolloutFixture) { f.ibmPool.AutoscaleEnabled = true }},
		{name: "auto-rebalance disabled in the worker's zone", wantReason: string(ReasonAutoRebalanceDisabled),
			modify: func(f *rolloutFixture) { f.ibmPool.Zones[0].AutobalanceEnabled = false }},
		{name: "the workload cluster is not connected", wantReason: string(ReasonWaitingForWorkloadCluster),
			modify: func(f *rolloutFixture) { f.clusterCache = clustercache.NewFakeEmptyClusterCache() }},
		{name: "both_exclusions_still_need_the_workload_cluster_for_a_replace", wantReason: string(ReasonWaitingForWorkloadCluster),
			modify: func(f *rolloutFixture) {
				f.clusterCache = clustercache.NewFakeEmptyClusterCache()
				f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: "",
					clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: ""}
			}},
		{name: "another pool holds the token", wantReason: string(ReasonWaitingForDisruptionToken),
			modify: func(f *rolloutFixture) {
				f.others = append(f.others, heldLease("other/uid-other"), poolObject("other", "other"))
			},
			wantMessage: "replaced 0 of 4; worker w0 (MinorBehind): another pool of the Cluster holds the disruption token; one worker " +
				"removal runs at a time"},
		{name: "another pool's drain-owner marker on a Node", wantReason: string(ReasonWaitingForDisruptionToken),
			modify: func(f *rolloutFixture) {
				helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
				nodes := VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, []string{"o0"})
				NewWithT(f.t).Expect(helper.Cordon(context.Background(), &nodes[0], drain.CordonOptions{Owner: poolTestNamespace + "/other"})).To(Succeed())
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRolloutFixture(t, nil)
			f.minorBehind("w0", "w1", "w2", "w3")
			tc.modify(f)
			cordonedBefore := f.cordoned()
			f.expect()
			want := tc.wantOutcome
			if want.Kind == "" {
				want = NotDone(IntervalGuarded, tc.wantReason)
			}
			g.Expect(f.reconcile()).To(Equal(want))
			g.Expect(f.upToDate().Status).To(Equal(metav1.ConditionFalse))
			g.Expect(f.upToDate().Reason).To(Equal(tc.wantReason))
			if tc.wantMessage != "" {
				g.Expect(f.upToDate().Message).To(Equal(tc.wantMessage))
			}
			g.Expect(f.cordoned()).To(Equal(cordonedBefore), "nothing is cordoned before every gate passed")
			g.Expect(f.calls.count("evict")).To(BeZero())
			g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
			if lease := f.lease(); lease != nil {
				g.Expect(lease.Spec.HolderIdentity).NotTo(Equal(ptr.To(DisruptionTokenHolder(f.pool))), "the token is not taken")
			}
		})
	}
}

// TestMachinePoolScopeRolloutPreCallGate checks the pre-call cluster-state gate of the replace: the cluster turns
// unavailable during the drain, so the pre-call gate sends no ReplaceWorker and holds with the Node cordoned; back to
// normal, exactly one call goes out.
func TestMachinePoolScopeRolloutPreCallGate(t *testing.T) {
	for _, state := range []string{"unavailable", "scaled_down", "unsupported"} {
		t.Run("no_ReplaceWorker_while_"+state, func(t *testing.T) {
			g := NewWithT(t)
			f := newRolloutFixture(t, nil)
			f.minorBehind("w0")
			f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach)
			f.drainedVictim()
			f.cp.Status.State = state
			f.expect()
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterState))))
			g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
			g.Expect(f.pool.Status.Disruption.IBMRequestTime.IsZero()).To(BeTrue())
			g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}), "the drained Node stays cordoned")
			g.Expect(f.upToDate().Message).To(ContainSubstring("IBM cluster " + poolTestClusterID))

			// The cluster is normal again: exactly one ReplaceWorker.
			f.cp.Status.State = "normal"
			f.mgmt = nil
			f.params = nil
			f.expect()
			f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(nil)
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		})
	}
}

// TestMachinePoolScopeRolloutCallOutcomes checks IBM's answers to ReplaceWorker: 404 and 204 go on to the removal,
// 409 waits, a 5xx is retried only after DisruptionSettleDelay, E1437 holds with the Node cordoned and is repeated at
// most once per DisruptionSettleDelay.
func TestMachinePoolScopeRolloutCallOutcomes(t *testing.T) {
	ready := func(t *testing.T) *rolloutFixture {
		t.Helper()
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForIBM)
		f.drainedVictim()
		return f
	}
	t.Run("404_the_worker_is_gone_and_the_replace_goes_on", func(t *testing.T) {
		g := NewWithT(t)
		f := ready(t)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(apiError(kubernetesservice.OpReplaceWorker, http.StatusNotFound, ""))
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
		g.Expect(f.events()).To(BeEmpty(), "WorkerReplaceRequested only for an accepted call")
	})
	t.Run("409_waits_without_a_second_call", func(t *testing.T) {
		g := NewWithT(t)
		f := ready(t)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(apiError(kubernetesservice.OpReplaceWorker, http.StatusConflict, ""))
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(f.upToDate().Message).To(ContainSubstring("IBM reports an operation in progress on the workers"))
		f.expect()
		f.clock.Step(DisruptionSettleDelay - time.Second)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))), "no second call within the delay")
	})
	t.Run("5xx_is_transient_and_retried_only_after_DisruptionSettleDelay", func(t *testing.T) {
		g := NewWithT(t)
		f := ready(t)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(apiError(kubernetesservice.OpReplaceWorker, http.StatusInternalServerError, "E0004"))
		outcome := f.reconcile()
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(f.pool.Status.Disruption.IBMRequestTime.Time).To(Equal(poolTestNow), "the attempt is recorded")
		f.expect()
		f.clock.Step(time.Minute)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))), "the workers are re-listed, no call")
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(nil)
		f.clock.Step(DisruptionSettleDelay)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	})
	t.Run("E1437_holds_with_the_Node_cordoned_and_is_repeated_at_most_once_per_DisruptionSettleDelay", func(t *testing.T) {
		g := NewWithT(t)
		f := ready(t)
		e1437 := apiError(kubernetesservice.OpReplaceWorker, http.StatusBadRequest, ClusterMinimumErrorCode)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(e1437)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}))
		refused := f.upToDate().DeepCopy()
		g.Expect(refused.Reason).To(Equal(string(ReasonWaitingForClusterMinimum)))
		g.Expect(refused.Message).To(Equal("replaced 3 of 4; worker w0 (MinorBehind): " + replaceRefusalMessage(ReasonWaitingForClusterMinimum)))

		// Within the delay: the hold is kept, the same condition, no call.
		f.expect()
		f.clock.Step(time.Minute)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
		g.Expect(f.upToDate().Message).To(Equal(refused.Message), "no churn: the hold writes nothing new")

		// After the delay the call is repeated once.
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(e1437)
		f.clock.Step(DisruptionSettleDelay)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
	})
	// A 403 for a missing Kubernetes Service role is reported on WorkersUpToDate, where the replace reports its steps, and
	// kept there while the replace waits out DisruptionSettleDelay: WorkerPoolReady is reported afresh from IBM's pool
	// on every reconcile, and the reconcile that the status patch triggers at once would otherwise hide the cause behind
	// "the IBM call was sent at ...", with the Node cordoned and the Cluster's token held.
	t.Run("an answer DisruptionCallOutcome leaves alone goes to the common error handling", func(t *testing.T) {
		g := NewWithT(t)
		f := ready(t)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(apiError(kubernetesservice.OpReplaceWorker, http.StatusForbidden, "E3e8a"))
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM), "the phase stays")
		refused := f.upToDate().DeepCopy()
		g.Expect(refused.Reason).To(Equal(MachinePoolIBMAPIErrorReason))
		g.Expect(refused.Message).To(Equal("replaced 3 of 4; worker w0 (MinorBehind): IBM Cloud refused the CAPIBM controller " +
			"identity (HTTP 403, code E3e8a) while replacing worker w0 of IBM cluster " + poolTestClusterID + "; grant it " +
			poolKubernetesServiceRoles + " (the IAM roles of the managed Kubernetes book)"))
		poolReady := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).DeepCopy()
		g.Expect(poolReady.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReason), "the pool itself is ready")

		f.clock.Step(time.Second)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, MachinePoolIBMAPIErrorReason)), "no second call")
		g.Expect(f.upToDate()).To(Equal(refused), "the permission message stays")
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)).To(Equal(poolReady), "no flip")
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}))

		// After the delay the replace is requested again, and the same answer writes the same condition.
		f.clock.Step(DisruptionSettleDelay)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(apiError(kubernetesservice.OpReplaceWorker, http.StatusForbidden, "E3e8a"))
		g.Expect(f.reconcile().Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.upToDate()).To(Equal(refused))
	})
}

// TestMachinePoolScopeRolloutAborts checks how a replace is given up: a trigger gone before the call or after a call
// IBM refused synchronously (LastCallRefused) aborts at once; after any other call attempt the replace waits
// WorkerStuckAfter; a worker IBM removes out of band is waited out, then the replace aborts without a call.
func TestMachinePoolScopeRolloutAborts(t *testing.T) {
	automatic := func(t *testing.T) *rolloutFixture {
		t.Helper()
		f := newRolloutFixture(t, nil)
		f.pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyAutomatic
		f.workers[0] = withFixPack(f.workers[0])
		return f
	}
	t.Run("the_policy_set_back_to_Manual_before_the_call_reverts_only_CAPIBM's_cordon", func(t *testing.T) {
		g := NewWithT(t)
		f := automatic(t)
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.drainedVictim()
		f.pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyManual
		f.expect()
		g.Expect(f.reconcile()).To(Equal(Done()), "nothing is left to roll")
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.events()).To(ConsistOf("Normal NodeUncordoned reverted CAPIBM's cordon of 1 Nodes of pool workers: the reason " +
			"for the removal went away before the IBM call; uncordoning what CAPIBM cordoned"))
		g.Expect(f.lease()).To(BeNil())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.upToDate().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason))
	})
	t.Run("E1437_then_the_trigger_goes_away_aborts_at_once", func(t *testing.T) {
		g := NewWithT(t)
		f := automatic(t)
		f.afterCall(poolTestNow.Add(-time.Minute))
		f.pool.Status.Conditions = append(f.pool.Status.Conditions, metav1.Condition{
			Type: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition, Status: metav1.ConditionFalse,
			Reason: string(ReasonWaitingForClusterMinimum), LastTransitionTime: metav1.NewTime(poolTestNow),
			Message: "replaced 0 of 4; worker w0 (PatchAvailable): " + replaceRefusalMessage(ReasonWaitingForClusterMinimum)})
		f.pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyManual
		f.expect()
		f.reconcile()
		g.Expect(f.cordoned()).To(BeEmpty(), "IBM never acts on a refused call")
		g.Expect(f.events()).To(ConsistOf(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 1 Nodes of pool workers: " +
			"the reason for the removal went away after IBM refused the call of")))
		g.Expect(f.lease()).To(BeNil())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	})
	t.Run("without the refusal record the replace waits WorkerStuckAfter after its call", func(t *testing.T) {
		g := NewWithT(t)
		f := automatic(t)
		f.afterCall(poolTestNow.Add(-time.Minute))
		f.pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyManual
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}), "IBM may still act on the call")
		g.Expect(f.upToDate().Message).To(ContainSubstring("no second call is made, and the Nodes stay cordoned until IBM acts or"))
		f.expect()
		f.clock.Step(WorkerStuckAfter)
		f.reconcile()
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	})
	t.Run("restart_idempotency_a_worker_already_deleting_gets_no_call", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.afterCall(poolTestNow.Add(-10 * time.Minute))
		f.workers = withState(f.workers, WorkerStateDeleting, "w0")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	})
	t.Run("a worker IBM removes out of band before the call is waited out, then the replace aborts", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForIBM)
		f.drainedVictim()
		f.workers = withState(f.workers, WorkerStateDeleting, "w0")
		f.expect()
		// The replace holds (guardedPoll); the pool, whose worker IBM is deleting, polls sooner (rolloutOutcome).
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason)))
		g.Expect(f.upToDate().Reason).To(Equal(string(ReasonReplacingWorker)))
		g.Expect(f.cordoned()).To(Equal([]string{"10.0.0.1"}))
		g.Expect(f.upToDate().Message).To(ContainSubstring("IBM removes the workers without a CAPIBM call"))

		f.workers = withState(f.workers, WorkerStateDeleted, "w0")
		f.expect()
		f.clock.Step(30 * time.Second)
		f.reconcile()
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.lease()).To(BeNil())
	})
	t.Run("an abort waits for the workload cluster to uncordon", func(t *testing.T) {
		g := NewWithT(t)
		f := automatic(t)
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.drainedVictim()
		f.pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyManual
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining), "nothing is given up")
		g.Expect(f.lease()).NotTo(BeNil())
	})
}

// TestMachinePoolScopeRolloutReplacement checks the end of a replace (the phases WaitingForRemoval and
// WaitingForReplacement): WorkerOperationStuck, ReplacementNotCreated, and the minReadySeconds soak with its recheck
// time.
func TestMachinePoolScopeRolloutReplacement(t *testing.T) {
	t.Run("WorkerOperationStuck_after_WorkerStuckAfter", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.afterCall(poolTestNow.Add(-WorkerStuckAfter - time.Minute))
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForRemoval
		f.workers = withState(f.workers, WorkerStateDeleting, "w0")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonWorkerOperationStuck))))
		g.Expect(f.upToDate().Reason).To(Equal(string(ReasonWorkerOperationStuck)))
	})
	t.Run("ReplacementNotCreated_after_ReplacementNotCreatedAfter", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.afterCall(poolTestNow.Add(-ReplacementNotCreatedAfter))
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForReplacement
		f.workers = withState(f.workers, WorkerStateDeleted, "w0")
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacementNotCreated))))
		g.Expect(f.upToDate().Message).To(ContainSubstring("run `ibmcloud ks worker-pool rebalance` for this pool"))
	})
	// Without the Nodes, ReplaceProgress finds no Ready time for any worker; the message must blame the connection, not
	// IBM's worker health: every worker here is deployed, normal and Ready.
	t.Run("WaitingForReplacement_without_the_workload_cluster_says_the_Nodes_cannot_be_read", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.afterCall(poolTestNow.Add(-10 * time.Minute))
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForReplacement
		f.addReplacement(WorkerStateDeployed, poolTestNow.Add(-5*time.Minute))
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonWaitingForReplacement))))
		// IBM lists the victim as deleted, so it has no class any more.
		g.Expect(f.upToDate().Message).To(Equal("replaced 4 of 4; worker w0: waiting for the replacement worker to be deployed " +
			"and its Node Ready (the workload cluster is not connected, so CAPIBM cannot read whether the Nodes are Ready; the " +
			"replace goes on once it is connected)"))
		g.Expect(f.lease()).NotTo(BeNil())
	})
	t.Run("the reconcile that enters WaitingForReplacement names no worker before it has read the Nodes", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.afterCall(poolTestNow.Add(-10 * time.Minute))
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForRemoval
		f.addReplacement(WorkerStateDeployed, poolTestNow.Add(-5*time.Minute))
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonWaitingForReplacement))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForReplacement))
		g.Expect(f.upToDate().Message).To(Equal("replaced 4 of 4; worker w0: IBM removed the worker; waiting for its replacement"))
	})
	t.Run("the_next_worker_waits_for_minReadySeconds_and_the_scope_names_the_recheck_time", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0", "w1")
		f.machinePool.Spec.Template.Spec.MinReadySeconds = ptr.To[int32](300)
		f.afterCall(poolTestNow.Add(-10 * time.Minute))
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForReplacement
		f.addReplacement(WorkerStateDeployed, poolTestNow.Add(-time.Minute))
		f.expect()
		outcome, scope := f.reconcileScope()
		g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, string(ReasonWaitingForReplacement))))
		g.Expect(scope.RolloutRecheckAt()).To(BeTemporally("==", poolTestNow.Add(4*time.Minute)))
		g.Expect(f.cordoned()).To(BeEmpty(), "w1 is not touched; w0's Node is gone")
		g.Expect(f.calls.count("find node")).To(Equal(4), "the pool's Nodes are found by provider ID")

		f.expect()
		f.clock.Step(4*time.Minute + time.Second)
		outcome, scope = f.reconcileScope()
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason)))
		g.Expect(scope.RolloutRecheckAt().IsZero()).To(BeTrue())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	})
}

// TestMachinePoolScopeRolloutReports checks WorkersUpToDate while no worker is replaced, and the recheck time of a
// scheduled spec.rollout.after.
func TestMachinePoolScopeRolloutReports(t *testing.T) {
	future := poolTestNow.Add(2 * time.Hour)
	testCases := []struct {
		name        string
		modify      func(f *rolloutFixture)
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
		wantRecheck time.Time
	}{
		{name: "every worker up to date", wantStatus: metav1.ConditionTrue, wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReason,
			wantMessage: "no worker of pool workers is due for replacement, and IBM offers none of them a newer fix pack or minor"},
		{name: "Manual_with_a_fix_pack_reports_PatchUpdateAvailable", wantStatus: metav1.ConditionFalse,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason,
			modify: func(f *rolloutFixture) {
				for i := range 3 {
					f.workers[i] = withFixPack(f.workers[i])
				}
			},
			wantMessage: "3 of 4 workers can move to IBM fix pack 1.36.5_1545; set spec.rollout.after to roll them, or set " +
				"spec.workerUpdates.policy: Automatic"},
		{name: "a scheduled rollout.after is named and rechecked", wantStatus: metav1.ConditionFalse,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason, wantRecheck: future,
			modify: func(f *rolloutFixture) {
				f.workers[0] = withFixPack(f.workers[0])
				f.pool.Spec.Rollout.After = metav1.NewTime(future)
			},
			wantMessage: "1 of 4 workers can move to IBM fix pack 1.36.5_1545; spec.rollout.after rolls them at 2026-11-16T14:00:00Z, " +
				"or set spec.workerUpdates.policy: Automatic"},
		{name: "up to date with a scheduled rollout.after", wantStatus: metav1.ConditionTrue, wantRecheck: future,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReason,
			modify:     func(f *rolloutFixture) { f.pool.Spec.Rollout.After = metav1.NewTime(future) },
			wantMessage: "no worker of pool workers is due for replacement, and IBM offers none of them a newer fix pack or minor; " +
				"spec.rollout.after replaces every worker whose Node is older at 2026-11-16T14:00:00Z"},
		{name: "MachinePool_version_is_ignored_and_named", wantStatus: metav1.ConditionTrue,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReason,
			modify:     func(f *rolloutFixture) { f.machinePool.Spec.Template.Spec.Version = "v1.30.0" },
			wantMessage: "no worker of pool workers is due for replacement, and IBM offers none of them a newer fix pack or minor; " +
				VersionIgnoredMessage},
		{name: "a passed rollout.after with Nodes newer than it replaces nothing", wantStatus: metav1.ConditionTrue,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReason,
			modify: func(f *rolloutFixture) {
				f.pool.Spec.Rollout.After = metav1.NewTime(rolloutNodeCreated.Add(-time.Minute))
			}},
		{name: "a passed rollout.after without the workload cluster says what it cannot read", wantStatus: metav1.ConditionFalse,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForWorkloadClusterReason,
			modify: func(f *rolloutFixture) {
				f.pool.Spec.Rollout.After = metav1.NewTime(poolTestNow.Add(-time.Minute))
				f.clusterCache = clustercache.NewFakeEmptyClusterCache()
			},
			wantMessage: "spec.rollout.after 2026-11-16T11:59:00Z has passed; CAPIBM replaces every worker of pool workers whose " +
				"Node is older, and it reads the Nodes through the workload cluster, which is not connected"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRolloutFixture(t, nil)
			if tc.modify != nil {
				tc.modify(f)
			}
			f.expect()
			outcome, scope := f.reconcileScope()
			g.Expect(outcome).To(Equal(Done()), "a pool that rolls nothing is done")
			g.Expect(f.upToDate().Status).To(Equal(tc.wantStatus))
			g.Expect(f.upToDate().Reason).To(Equal(tc.wantReason))
			if tc.wantMessage != "" {
				g.Expect(f.upToDate().Message).To(Equal(tc.wantMessage))
			}
			g.Expect(scope.RolloutRecheckAt()).To(Equal(tc.wantRecheck))
			g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Status).To(Equal(metav1.ConditionTrue),
				"the pool stays Ready")
			g.Expect(f.lease()).To(BeNil(), "no token for a pool that rolls nothing")
		})
	}
	t.Run("workersWithUpdateAvailable_counts_the_fix_packs_whatever_the_policy", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.workers[0], f.workers[1] = withFixPack(f.workers[0]), withFixPack(f.workers[1])
		f.expect()
		f.reconcile()
		g.Expect(f.pool.Status.WorkersWithUpdateAvailable).To(Equal(ptr.To[int32](2)))
	})
}

// TestMachinePoolScopeRolloutTriggers checks which workers a rollout replaces: a passed rollout.after recycles
// workers with an older Node under both policies, Manual plus a fix pack plus a passed rollout.after replaces with the
// reason RolloutRequested, and the minor follow needs no Node.
func TestMachinePoolScopeRolloutTriggers(t *testing.T) {
	passed := metav1.NewTime(poolTestNow.Add(-time.Minute))
	testCases := []struct {
		name    string
		modify  func(f *rolloutFixture)
		wantMsg string
	}{
		{name: "rollout_after_recycles_up_to_date_workers_under_Manual", modify: func(f *rolloutFixture) { f.pool.Spec.Rollout.After = passed },
			wantMsg: "replaced 0 of 4; worker w0 (RolloutRequested): "},
		{name: "rollout.after under Automatic", modify: func(f *rolloutFixture) {
			f.pool.Spec.Rollout.After, f.pool.Spec.WorkerUpdates.Policy = passed, infrav1.WorkerUpdatePolicyAutomatic
		}, wantMsg: "replaced 0 of 4; worker w0 (RolloutRequested): "},
		{name: "Manual patch with a passed rollout.after is due", modify: func(f *rolloutFixture) {
			f.pool.Spec.Rollout.After = passed
			f.workers[0] = withFixPack(f.workers[0])
		}, wantMsg: "replaced 0 of 4; worker w0 (RolloutRequested): "},
		{name: "minor_follow_under_Manual", modify: func(f *rolloutFixture) { f.minorBehind("w2") },
			wantMsg: "replaced 3 of 4; worker w2 (MinorBehind): "},
		{name: "an unhealthy due worker goes first", modify: func(f *rolloutFixture) {
			f.pool.Spec.Rollout.After = passed
			f.workers = withState(f.workers, "provision_failed", "w3")
			f.workers[3].HealthState = "critical"
		}, wantMsg: "replaced 0 of 4; worker w3 (RolloutRequested): "},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRolloutFixture(t, nil)
			tc.modify(f)
			f.expect()
			g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
			g.Expect(f.upToDate().Message).To(HavePrefix(tc.wantMsg))
			g.Expect(f.pool.Status.Disruption.WorkerIDs).To(HaveLen(1))
		})
	}
}

// TestMachinePoolScopeRolloutResumes checks a replace after a controller restart: a replace whose status was lost
// resumes from its victim's drain-owner marker with the original drain start, re-acquiring the token, and is not
// announced again; a marker of a worker that is no longer due is reverted by the scale-down flow instead.
func TestMachinePoolScopeRolloutResumes(t *testing.T) {
	markW0 := func(f *rolloutFixture, start time.Time) {
		helper := &drain.Helper{Client: f.workload, Clock: clocktesting.NewFakeClock(start), EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
		nodes := VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0"})
		NewWithT(f.t).Expect(helper.Cordon(context.Background(), &nodes[0], drain.CordonOptions{Owner: DrainOwner(f.pool)})).To(Succeed())
		f.calls.all()
	}
	t.Run("status_lost_mid_drain_resumes_with_the_original_drain_start", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0", "w1")
		markW0(f, poolTestNow.Add(-7*time.Minute))
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationReplace))
		g.Expect(f.pool.Status.Disruption.WorkerIDs).To(Equal([]string{"w0"}))
		g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow.Add(-7 * time.Minute)))
		g.Expect(f.lease().Spec.HolderIdentity).To(Equal(ptr.To(DisruptionTokenHolder(f.pool))), "the token is re-acquired")
		g.Expect(f.events()).To(BeEmpty(), "the drain is not announced again")
	})
	t.Run("a marker on a worker that is no longer due is reverted", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		markW0(f, poolTestNow.Add(-7*time.Minute))
		f.expect()
		f.reconcile()
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	})
}

// TestMachinePoolScopeRolloutRequeue checks which outcome a reconcile with a due worker returns: a rollout that only
// waits (a gate, a hold, guardedPoll) never slows the pool's readiness poll, which follows IBM's provisioning at
// poolTransitionPoll; a ready pool keeps the rollout's wait, and an active step of the rollout keeps its own outcome.
func TestMachinePoolScopeRolloutRequeue(t *testing.T) {
	provisioning := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason
	for _, tc := range []struct {
		name         string
		modify       func(f *rolloutFixture)
		want         Outcome
		wantUpToDate string
	}{
		{name: "a gated rollout returns the readiness poll of a provisioning pool",
			modify:       func(f *rolloutFixture) { f.workers = withState(f.workers, "provisioning", "w3") },
			want:         NotDone(IntervalPoolTransition, provisioning),
			wantUpToDate: string(ReasonWaitingForSettledCluster)},
		{name: "a gated rollout of a ready pool keeps its own wait",
			modify: func(f *rolloutFixture) {
				f.others = append(f.others, heldLease("other/uid-other"), poolObject("other", "other"))
			},
			want:         NotDone(IntervalGuarded, string(ReasonWaitingForDisruptionToken)),
			wantUpToDate: string(ReasonWaitingForDisruptionToken)},
		{name: "a hold of the rollout returns the readiness poll of a provisioning pool",
			modify: func(f *rolloutFixture) {
				f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
				f.clusterCache = clustercache.NewFakeEmptyClusterCache()
				f.workers = withState(f.workers, "provisioning", "w3")
			},
			want:         NotDone(IntervalPoolTransition, provisioning),
			wantUpToDate: string(ReasonWaitingForWorkloadCluster)},
		{name: "an active step keeps the rollout's outcome",
			modify:       func(f *rolloutFixture) { f.inPhase(infrav1.WorkerDisruptionPhaseDraining) },
			want:         NotDone(IntervalDrainRetry, string(ReasonDrainingNode)),
			wantUpToDate: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateDrainingNodeReason},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRolloutFixture(t, nil)
			f.minorBehind("w0", "w1", "w2", "w3")
			tc.modify(f)
			f.expect()
			g.Expect(f.reconcile()).To(Equal(tc.want))
			g.Expect(f.upToDate().Reason).To(Equal(tc.wantUpToDate), "WorkersUpToDate still reports the rollout")
		})
	}
}

// TestMachinePoolScopeRolloutDispatch checks the order of a pool's flows around a replace: growth runs first (it
// removes nothing), a replace in flight goes before a scale-down, a failing pool guard settles a replace in flight,
// and a status that cannot be persisted sends no ReplaceWorker.
func TestMachinePoolScopeRolloutDispatch(t *testing.T) {
	t.Run("growth runs while a replace is in flight", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.machinePool.Spec.Replicas = ptr.To[int32](6)
		f.expect()
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(3)).Return(nil)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason)))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining), "the replace waits one reconcile")
	})
	t.Run("a failing pool guard settles a replace in flight before its call", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.drainedVictim()
		f.machinePool.Spec.Replicas = ptr.To[int32](3)
		f.expect()
		outcome := f.reconcile()
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(outcome.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason))
		g.Expect(f.cordoned()).To(BeEmpty())
		g.Expect(f.lease()).To(BeNil())
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	})
	t.Run("a failing pool guard follows a replace after its call to its end, without a call", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.afterCall(poolTestNow.Add(-time.Minute))
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForRemoval
		f.workers = withState(f.workers, WorkerStateDeleting, "w0")
		f.machinePool.Spec.Replicas = ptr.To[int32](3)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Reason).
			To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason))
	})
	t.Run("no ReplaceWorker when the status cannot be persisted", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForIBM)
		f.drainedVictim()
		f.persistErr = errors.New("conflict")
		f.expect()
		outcome := f.reconcile()
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(outcome.Err).To(MatchError(ContainSubstring("persisting the replace of pool workers before the IBM call")))
	})
	t.Run("a scale-down waits while workers are due and says so on WorkersUpToDate", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0", "w1", "w2", "w3")
		f.machinePool.Spec.Replicas = ptr.To[int32](2)
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.pool.Status.Disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationScaleDown))
		g.Expect(f.upToDate().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason))
		g.Expect(f.upToDate().Message).To(Equal("replaced 0 of 4; the next replacement waits until the scale-down of pool workers has finished"))
	})
	t.Run("a workload cluster lost mid-drain holds the replace with its Node cordoned", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.pool.Spec.Rollout.After = metav1.NewTime(poolTestNow.Add(-time.Hour))
		f.inPhase(infrav1.WorkerDisruptionPhaseDraining)
		f.drainedVictim()
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
		g.Expect(f.upToDate().Message).To(Equal("rolling out pool workers; worker w0 (RolloutRequested): the workload cluster is "+
			"not connected; the Nodes stay cordoned and no IBM call is made"), "the victim keeps its classification")
	})
}

// TestMachinePoolScopeRolloutWorkloadClusterLost checks that a replace needs the workload cluster past the Gated
// step: a replace whose workload cluster goes away before its IBM call waits in WaitingForWorkloadCluster with both
// exclusion annotations set, in every phase before the call, because it could neither revert its cordon on an abort
// nor see its replacement's Node become Ready. Only a drain or detach time limit that runs out still allows the call
// (CAPI parity).
func TestMachinePoolScopeRolloutWorkloadClusterLost(t *testing.T) {
	excluded := map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: "", clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: ""}
	for _, phase := range []infrav1.WorkerDisruptionPhase{infrav1.WorkerDisruptionPhaseDraining,
		infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, infrav1.WorkerDisruptionPhaseWaitingForIBM} {
		t.Run("both_exclusions_in_"+string(phase)+"_wait_for_the_workload_cluster_without_a_call", func(t *testing.T) {
			g := NewWithT(t)
			f := newRolloutFixture(t, nil)
			f.minorBehind("w0")
			f.pool.Annotations = excluded
			f.inPhase(phase)
			f.clusterCache = clustercache.NewFakeEmptyClusterCache()
			for _, step := range []time.Duration{0, time.Second} {
				f.clock.Step(step)
				f.expect()
				g.Expect(f.reconcile()).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))), "no ReplaceWorker")
				g.Expect(f.pool.Status.Disruption.Phase).To(Equal(phase))
				g.Expect(f.pool.Status.Disruption.IBMRequestTime.IsZero()).To(BeTrue())
				g.Expect(f.upToDate().Reason).To(Equal(string(ReasonWaitingForWorkloadCluster)))
				g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Reason).
					To(Equal(string(ReasonWaitingForWorkloadCluster)))
			}
			g.Expect(f.lease()).NotTo(BeNil(), "the token stays with the replace")
			g.Expect(f.events()).To(BeEmpty())
		})
	}
	t.Run("the_workload_cluster_back_lets_the_excluded_replace_call_IBM", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.pool.Annotations = excluded
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForIBM)
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(nil)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	})
	t.Run("a drain and detach time limit that ran out still allow the call without the workload cluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newRolloutFixture(t, nil)
		f.minorBehind("w0")
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](300)
		f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](300)
		f.inPhase(infrav1.WorkerDisruptionPhaseWaitingForIBM)
		f.clusterCache = clustercache.NewFakeEmptyClusterCache()
		f.expect()
		f.ibm.EXPECT().ReplaceWorker(gomock.Any(), poolTestClusterID, "w0", true).Return(nil)
		g.Expect(f.reconcile()).To(Equal(NotDone(IntervalPoolTransition, string(ReasonReplacingWorker))))
	})
}
