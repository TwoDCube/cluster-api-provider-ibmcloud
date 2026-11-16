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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

// The removal runner tests drive the shared removal steps directly, outside any flow: the pool deletion and
// scale-down tests cover them again through their flows. The runner works on the pool fixture's object, a fake
// workload cluster with the Nodes of the victims w0 and w1, and a send function that records the IBM call.

// runnerFixture is a removal runner over the pool fixture.
type runnerFixture struct {
	*poolFixture
	calls    *workloadCalls
	workload client.Client
	// sent counts the IBM calls the runner sent; sendErr is IBM's answer to them.
	sent    int
	sendErr error
	// persistedAtSend is how many times the pool was persisted when the call was sent.
	persistedAtSend int
	// reported are the decisions the runner reported on the flow's condition.
	reported []DisruptionDecision
}

// newRunnerFixture returns the fixture with a connected workload cluster: the Nodes of w0 and w1 with one pod each.
func newRunnerFixture(t *testing.T) *runnerFixture {
	t.Helper()
	f := &runnerFixture{poolFixture: newPoolFixture(t), calls: &workloadCalls{}}
	f.workload = newWorkloadClient(t, f.calls, nil, victimNode("10.0.0.1", "w0"), workloadPod("web-w0", "10.0.0.1"),
		victimNode("10.0.0.2", "w1"), workloadPod("web-w1", "10.0.0.2"))
	return f
}

// input returns the runner input of a pool deletion of w0 and w1 in a normal cluster, with the fixture's workload
// cluster and a send function that records the call.
func (f *runnerFixture) input() removalRunnerInput {
	scope := f.scope()
	scope.ControlPlane = f.cp
	pool := ibmPool("workers", 2, "us-south-1", "us-south-2")
	return removalRunnerInput{
		scope: scope, clients: &poolClients{identity: credentials.Identity{Description: "the test identity"}, kubernetes: f.ibm},
		pool: &pool, operation: infrav1.WorkerDisruptionOperationDeletePool, victims: []string{"w0", "w1"}, clusterState: "normal",
		call: removalCall{op: kubernetesservice.OpRemoveWorkerPool, action: "removing worker pool workers of IBM cluster " + poolTestClusterID,
			send: func(context.Context) error {
				f.sent++
				f.persistedAtSend = len(f.persisted)
				return f.sendErr
			}},
		refusal:     refusalRecord{condition: infrav1.IBMCloudManagedMachinePoolDeletingCondition, status: metav1.ConditionTrue},
		report:      func(d DisruptionDecision) { f.reported = append(f.reported, d) },
		workload:    f.workload,
		victimNodes: VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0", "w1"}),
	}
}

// runner returns the runner of input after modify changed it.
func (f *runnerFixture) runner(modify func(in *removalRunnerInput)) *removalRunner {
	f.t.Helper()
	in := f.input()
	if modify != nil {
		modify(&in)
	}
	r, err := newRemovalRunner(in)
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	f.calls.all()
	return r
}

// disconnected drops the workload cluster from the runner input, as while ClusterCache is not connected.
func disconnected(in *removalRunnerInput) {
	in.workload, in.victimNodes = nil, nil
}

func TestNewRemovalRunner(t *testing.T) {
	t.Run("every missing input is named", func(t *testing.T) {
		g := NewWithT(t)
		_, err := newRemovalRunner(removalRunnerInput{})
		g.Expect(err).To(HaveOccurred())
		for _, want := range []string{"scope is nil", "clients is nil", "pool is nil", `operation is ""`,
			"call needs its operation and its send function", "refusal needs the condition type and status", "report is nil"} {
			g.Expect(err.Error()).To(ContainSubstring(want))
		}
	})
	// DisruptionCallOutcome rejects such an op only after the call went out, so the runner is never built with one.
	for _, op := range []kubernetesservice.Operation{kubernetesservice.OpListWorkers, kubernetesservice.OpCreateWorkerPool} {
		t.Run("a call DisruptionCallOutcome does not map gives no runner: "+string(op), func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			in := f.input()
			in.call.op = op
			r, err := newRemovalRunner(in)
			g.Expect(r).To(BeNil())
			g.Expect(err).To(MatchError(ContainSubstring(`managed: removal runner: call.op: DisruptionCallOutcome call is "` +
				string(op) + `"`)))
			g.Expect(f.sent).To(BeZero(), "no IBM call is sent")
			g.Expect(f.persisted).To(BeEmpty(), "nothing is persisted")
		})
	}
	t.Run("a drain step that cannot be built gives no runner", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		in := f.input()
		params := *in.scope.params
		params.Recorder = nil
		in.scope.params = &params
		r, err := newRemovalRunner(in)
		g.Expect(r).To(BeNil())
		g.Expect(err).To(MatchError(ContainSubstring("managed: drain step: Recorder is nil")))
	})
	for _, tc := range []struct {
		name      string
		modify    func(in *removalRunnerInput)
		wantDrain bool
	}{
		{name: "a connected workload cluster gets a drain step", wantDrain: true},
		{name: "no drain step while the workload cluster is not connected", modify: disconnected},
		{name: "no drain step for a removal that drains nothing", modify: func(in *removalRunnerInput) { in.drainsNothing = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			r := f.runner(tc.modify)
			g.Expect(r.drain != nil).To(Equal(tc.wantDrain))
			g.Expect(f.calls.all()).To(BeEmpty(), "building the runner calls nothing")
			g.Expect(f.sent).To(BeZero())
		})
	}
	t.Run("the drain settings are read from the pool", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: ""}
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		r := f.runner(nil)
		g.Expect(r.knobs.ExcludeDraining).To(BeTrue())
		g.Expect(r.knobs.NodeDrainTimeoutSeconds).To(Equal(ptr.To[int32](60)))
	})
}

func TestRemovalPoolOperation(t *testing.T) {
	g := NewWithT(t)
	g.Expect(removalPoolOperation(infrav1.WorkerDisruptionOperationDeletePool)).To(Equal(PoolOperationRemove))
	g.Expect(removalPoolOperation(infrav1.WorkerDisruptionOperationScaleDown)).To(Equal(PoolOperationShrink))
	g.Expect(removalPoolOperation(infrav1.WorkerDisruptionOperationReplace)).To(Equal(PoolOperationReplace))
	g.Expect(removalNoun(infrav1.WorkerDisruptionOperationDeletePool)).To(Equal("removal"))
	g.Expect(removalNoun(infrav1.WorkerDisruptionOperationScaleDown)).To(Equal("scale-down"))
	g.Expect(removalNoun(infrav1.WorkerDisruptionOperationReplace)).To(Equal("replace"))
}

// TestRemovalRunnerClusterStateCheck checks the cluster-state gate of every removal: unavailable and scaled_down
// block every removal; unsupported still allows a shrink and a pool removal, never a replace.
func TestRemovalRunnerClusterStateCheck(t *testing.T) {
	deletePool, scaleDown, replace := infrav1.WorkerDisruptionOperationDeletePool, infrav1.WorkerDisruptionOperationScaleDown,
		infrav1.WorkerDisruptionOperationReplace
	for _, tc := range []struct {
		name        string
		state       string
		op          infrav1.WorkerDisruptionOperation
		wantAllowed bool
	}{
		{name: "normal allows a pool removal", state: "normal", op: deletePool, wantAllowed: true},
		{name: "a state IBM documents no limit for allows a pool removal", state: "deploying", op: deletePool, wantAllowed: true},
		{name: "unavailable_blocks_a_pool_removal", state: "unavailable", op: deletePool},
		{name: "scaled_down_blocks_a_pool_removal", state: "scaled_down", op: deletePool},
		{name: "unsupported_allows_a_pool_removal", state: "unsupported", op: deletePool, wantAllowed: true},
		{name: "unavailable_blocks_a_shrink", state: "unavailable", op: scaleDown},
		{name: "unsupported_allows_a_shrink", state: "unsupported", op: scaleDown, wantAllowed: true},
		{name: "unsupported_blocks_a_replace", state: "unsupported", op: replace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			r := f.runner(func(in *removalRunnerInput) { in.operation, in.clusterState = tc.op, tc.state })
			got := r.clusterStateCheck()
			if tc.wantAllowed {
				g.Expect(got).To(Equal(DisruptionCheckResult{Allowed: true}))
				return
			}
			g.Expect(got).To(Equal(DisruptionCheckResult{Reason: ReasonWaitingForClusterState,
				Message: "IBM cluster " + poolTestClusterID + " is " + tc.state + "; IBM accepts no worker pool change in that state"}))
		})
	}
}

func TestRemovalRunnerPreCallGate(t *testing.T) {
	allowed := DisruptionCheckResult{Allowed: true}
	t.Run("a_state_that_forbids_the_call_answers_before_the_disruption_check", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(func(in *removalRunnerInput) { in.clusterState = "scaled_down" })
		got, err := r.preCallGate(func(bool) (DisruptionCheckResult, error) {
			t.Error("the disruption check runs only when the cluster state allows the call")
			return allowed, nil
		})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.Allowed).To(BeFalse())
		g.Expect(got.Reason).To(Equal(ReasonWaitingForClusterState))
		g.Expect(got.Message).To(ContainSubstring("is scaled_down"))
	})
	t.Run("an allowed state hands over to the check without the settle delay", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		var skipped []bool
		blocked := DisruptionCheckResult{Reason: ReasonWaitingForSettledCluster, Message: "worker o0 is provisioning"}
		got, err := r.preCallGate(func(skipSettleDelay bool) (DisruptionCheckResult, error) {
			skipped = append(skipped, skipSettleDelay)
			return blocked, nil
		})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).To(Equal(blocked))
		g.Expect(skipped).To(Equal([]bool{true}))
	})
	t.Run("the check's error is returned", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		_, err := r.preCallGate(func(bool) (DisruptionCheckResult, error) { return DisruptionCheckResult{}, errors.New("invalid") })
		g.Expect(err).To(MatchError("invalid"))
	})
}

// TestRefusalRecordRead checks both kinds of refusal record: the reason alone (pool deletion) and the reason with a
// fixed message (scale-down), which tells a refusal apart from another step that reports the same reason.
func TestRefusalRecordRead(t *testing.T) {
	deleting := infrav1.IBMCloudManagedMachinePoolDeletingCondition
	ready := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition
	reasonOnly := refusalRecord{condition: deleting, status: metav1.ConditionTrue}
	fixed := refusalRecord{condition: ready, status: metav1.ConditionFalse,
		message: func(reason DisruptionReason) string { return "IBM refused it: " + string(reason) }}
	for _, tc := range []struct {
		name        string
		record      refusalRecord
		recorded    *metav1.Condition
		wantReason  DisruptionReason
		wantMessage string
	}{
		{name: "no condition", record: reasonOnly},
		{name: "the condition has another status", record: reasonOnly,
			recorded: &metav1.Condition{Type: deleting, Status: metav1.ConditionFalse, Reason: string(ReasonWaitingForClusterMinimum)}},
		{name: "a reason that is no refusal", record: reasonOnly,
			recorded: &metav1.Condition{Type: deleting, Status: metav1.ConditionTrue, Reason: string(ReasonRemovingWorkerPool)}},
		{name: "the reason alone is the record", record: reasonOnly,
			recorded:   &metav1.Condition{Type: deleting, Status: metav1.ConditionTrue, Reason: string(ReasonWaitingForClusterMinimum), Message: "as written"},
			wantReason: ReasonWaitingForClusterMinimum, wantMessage: "as written"},
		{name: "an autoscaled-pool refusal", record: reasonOnly,
			recorded:   &metav1.Condition{Type: deleting, Status: metav1.ConditionTrue, Reason: string(ReasonExternallyAutoscaled), Message: "m"},
			wantReason: ReasonExternallyAutoscaled, wantMessage: "m"},
		{name: "a fixed-message record needs its message", record: fixed,
			recorded: &metav1.Condition{Type: ready, Status: metav1.ConditionFalse, Reason: string(ReasonWaitingForClusterMinimum),
				Message: "the cluster has 2 ready workers; removing 2 would leave 0"}},
		{name: "a fixed-message record holds with its own message", record: fixed,
			recorded: &metav1.Condition{Type: ready, Status: metav1.ConditionFalse, Reason: string(ReasonWaitingForClusterMinimum),
				Message: "scale-down of pool workers: IBM refused it: WaitingForClusterMinimum; a note"},
			wantReason: ReasonWaitingForClusterMinimum, wantMessage: "IBM refused it: WaitingForClusterMinimum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			pool := poolObject("workers", "workers")
			if tc.recorded != nil {
				pool.Status.Conditions = []metav1.Condition{*tc.recorded}
			}
			reason, message := tc.record.read(pool)
			g.Expect(reason).To(Equal(tc.wantReason))
			g.Expect(message).To(Equal(tc.wantMessage))
		})
	}
}

func TestRemovalRunnerRefusalHold(t *testing.T) {
	waitForIBM := DisruptionDecision{Step: StepWaitForIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
		Reason: ReasonRemovingWorkerPool, DrainingReason: ReasonDrainingNode, Message: "the IBM call was sent at ..."}
	t.Run("a recorded refusal keeps the hold", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Status.Conditions = []metav1.Condition{{Type: infrav1.IBMCloudManagedMachinePoolDeletingCondition,
			Status: metav1.ConditionTrue, Reason: string(ReasonWaitingForClusterMinimum), Message: "IBM refused the removal"}}
		r := f.runner(nil)
		hold, held := r.refusalHold(waitForIBM)
		g.Expect(held).To(BeTrue())
		g.Expect(hold).To(Equal(DisruptionDecision{Step: StepWaitForIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
			Reason: ReasonWaitingForClusterMinimum, Message: "IBM refused the removal"}))
		g.Expect(r.recordedRefusal()).To(Equal(ReasonWaitingForClusterMinimum))
	})
	t.Run("without a recorded refusal the decision stays", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		hold, held := r.refusalHold(waitForIBM)
		g.Expect(held).To(BeFalse())
		g.Expect(hold).To(Equal(waitForIBM))
		g.Expect(r.recordedRefusal()).To(BeEmpty())
	})
}

func TestRemovalRunnerRecord(t *testing.T) {
	draining, detach := infrav1.WorkerDisruptionPhaseDraining, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach
	t.Run("the first record writes the removal and announces the drain", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Status.Disruption.IBMRequestTime = metav1.NewTime(poolTestNow.Add(-time.Hour))
		r := f.runner(nil)
		r.record(draining)
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{
			Operation: infrav1.WorkerDisruptionOperationDeletePool, Phase: draining, WorkerIDs: []string{"w0", "w1"},
			DrainStartTime: metav1.NewTime(poolTestNow), IBMRequestTime: metav1.NewTime(poolTestNow.Add(-time.Hour)),
		}), "ibmRequestTime is kept")
		g.Expect(f.events()).To(Equal([]string{"Normal DrainStarted cordoning 2 Nodes of pool workers, then evicting their pods"}))
	})
	t.Run("the drain start comes from this reconcile's drain", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		r.drainResult = &DrainStepResult{StartTime: poolTestNow.Add(-5 * time.Minute)}
		r.record(draining)
		g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow.Add(-5 * time.Minute)))
	})
	t.Run("a recorded drain start is kept", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
			Phase: draining, WorkerIDs: []string{"w0", "w1"}, DrainStartTime: metav1.NewTime(poolTestNow.Add(-time.Hour))}
		r := f.runner(nil)
		r.record(detach)
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(detach))
		g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow.Add(-time.Hour)))
		g.Expect(f.events()).To(Equal([]string{"Normal DrainCompleted the 2 Nodes of pool workers are drained"}))
	})
	t.Run("a resumed drain is not announced again", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		r.resumed = true
		r.record(draining)
		g.Expect(f.events()).To(BeEmpty())
	})
	t.Run("a removal that drains nothing announces nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(func(in *removalRunnerInput) {
			in.operation, in.drainsNothing = infrav1.WorkerDisruptionOperationScaleDown, true
		})
		r.record(draining)
		r.record(detach)
		g.Expect(f.pool.Status.Disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationScaleDown))
		g.Expect(f.events()).To(BeEmpty())
	})
	t.Run("a drain that times out without the workload cluster is announced as a Warning", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
			Phase: draining, WorkerIDs: []string{"w0", "w1"}, DrainStartTime: metav1.NewTime(poolTestNow.Add(-time.Hour))}
		r := f.runner(disconnected)
		r.record(detach)
		g.Expect(f.events()).To(Equal([]string{"Warning DrainTimeoutExceeded the drain time limit of 60s " +
			"(MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed since 2026-11-16T11:00:00Z while the " +
			"workload cluster is not connected; CAPIBM proceeds without knowing which pods are left on the Nodes of pool workers"}))
	})
	t.Run("an excluded drain without the workload cluster announces nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: ""}
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
			Phase: draining, WorkerIDs: []string{"w0", "w1"}, DrainStartTime: metav1.NewTime(poolTestNow.Add(-time.Hour))}
		r := f.runner(disconnected)
		r.record(detach)
		g.Expect(f.events()).To(BeEmpty())
	})
}

func TestRemovalRunnerDisconnectedProgress(t *testing.T) {
	for _, tc := range []struct {
		name   string
		start  time.Time
		modify func(f *runnerFixture)
		want   DrainProgress
	}{
		{name: "without a recorded start no limit runs out", modify: func(f *runnerFixture) {
			f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		}},
		{name: "without limits nothing runs out", start: poolTestNow.Add(-time.Hour)},
		{name: "both limits count from the drain start", start: poolTestNow.Add(-2 * time.Minute), modify: func(f *runnerFixture) {
			f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
			f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](180)
		}, want: DrainProgress{TimedOut: true}},
		{name: "the exclusions hold", modify: func(f *runnerFixture) {
			f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: "",
				clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: ""}
		}, want: DrainProgress{Excluded: true, DetachExcluded: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			f.pool.Status.Disruption.DrainStartTime = metav1.NewTime(tc.start)
			if tc.modify != nil {
				tc.modify(f)
			}
			g.Expect(f.runner(disconnected).disconnectedProgress()).To(Equal(tc.want))
		})
	}
}

func TestRemovalRunnerRunDrain(t *testing.T) {
	t.Run("Check observes without evicting", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		g.Expect(r.runDrain(context.Background(), false)).To(Succeed())
		g.Expect(r.evicted).To(BeFalse())
		g.Expect(r.drainResult.Progress.Done).To(BeFalse(), "the pods are still there")
		g.Expect(f.calls.count("evict")).To(BeZero())
		g.Expect(f.calls.count("cordon")).To(BeZero())
	})
	t.Run("Evict cordons every victim Node, then evicts", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		g.Expect(r.runDrain(context.Background(), true)).To(Succeed())
		g.Expect(r.evicted).To(BeTrue())
		g.Expect(f.calls.all()).To(Equal([]string{"cordon 10.0.0.1", "cordon 10.0.0.2",
			"list pods 10.0.0.1", "evict apps/web-w0", "list pods 10.0.0.2", "evict apps/web-w1"}))
	})
	t.Run("without the workload cluster it touches nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(disconnected)
		g.Expect(r.runDrain(context.Background(), true)).To(Succeed())
		g.Expect(r.evicted).To(BeTrue())
		g.Expect(r.drainResult).To(Equal(&DrainStepResult{Progress: r.disconnectedProgress()}))
		g.Expect(f.calls.all()).To(BeEmpty())
	})
}

func TestRemovalRunnerReport(t *testing.T) {
	drainingCondition := func(f *runnerFixture) *metav1.Condition {
		return condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition)
	}
	decision := DisruptionDecision{Step: StepDrain, Reason: ReasonDrainingNodes, DrainingReason: ReasonDrainingNode, Message: "evicting"}
	t.Run("the flow reports its condition, the runner Draining", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		g.Expect(r.wait(decision, IntervalDrainRetry)).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNodes))))
		g.Expect(f.reported).To(Equal([]DisruptionDecision{decision}))
		draining := drainingCondition(f)
		g.Expect(draining.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(draining.Reason).To(Equal(string(ReasonDrainingNode)))
		g.Expect(draining.Message).To(Equal("evicting"))
	})
	t.Run("the drain's own message wins", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		r.drainResult = &DrainStepResult{Message: "Node 10.0.0.1: Drain not completed yet"}
		r.report(decision)
		g.Expect(drainingCondition(f).Message).To(Equal("Node 10.0.0.1: Drain not completed yet"))
	})
	t.Run("no draining reason sets Draining False", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		r.report(DisruptionDecision{Step: StepWaitForRemoval, Reason: ReasonRemovingWorkerPool})
		g.Expect(drainingCondition(f).Status).To(Equal(metav1.ConditionFalse))
		g.Expect(drainingCondition(f).Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolNotDrainingReason))
	})
	t.Run("a removal that drains nothing keeps Draining False", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(func(in *removalRunnerInput) { in.drainsNothing = true })
		r.report(decision)
		g.Expect(drainingCondition(f).Status).To(Equal(metav1.ConditionFalse))
	})
}

// TestRemovalRunnerCallIBM checks the call-and-hold step: status first, then the call, once.
func TestRemovalRunnerCallIBM(t *testing.T) {
	callDecision := DisruptionDecision{Step: StepCallIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
		Reason: ReasonRemovingWorkerPool, Message: "the Nodes are drained; asking IBM to remove the workers"}
	e1437 := apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusBadRequest, ClusterMinimumErrorCode)
	t.Run("the status is persisted before the call, and IBM's acceptance waits for the removal", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		g.Expect(r.callIBM(context.Background(), callDecision)).To(Equal(NotDone(IntervalPoolTransition, string(ReasonRemovingWorkerPool))))
		g.Expect(f.sent).To(Equal(1))
		g.Expect(f.persistedAtSend).To(Equal(1), "persisted before the call")
		g.Expect(f.persisted[0].Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(f.persisted[0].IBMRequestTime.Time).To(Equal(poolTestNow))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
		g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow))
		g.Expect(f.reported).To(HaveLen(2), "the call, then IBM's answer")
		g.Expect(f.reported[0]).To(Equal(callDecision))
	})
	t.Run("no call when the status cannot be persisted", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.persistErr = errors.New("conflict")
		r := f.runner(nil)
		outcome := r.callIBM(context.Background(), callDecision)
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(outcome.Err).To(MatchError(ContainSubstring("persisting the removal of pool workers before the IBM call")))
		g.Expect(f.sent).To(BeZero())
	})
	t.Run("E1437 holds with DisruptionCallOutcome's message", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.sendErr = e1437
		r := f.runner(nil)
		g.Expect(r.callIBM(context.Background(), callDecision)).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
		g.Expect(f.reported[1].Message).To(Equal("IBM refused the removal because the cluster would fall below its minimum " +
			"workers (E1437); the drained Nodes stay cordoned"))
	})
	t.Run("E1437 holds with the record's fixed message", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.sendErr = e1437
		r := f.runner(func(in *removalRunnerInput) {
			in.refusal.message = func(reason DisruptionReason) string { return "refused: " + string(reason) }
		})
		g.Expect(r.callIBM(context.Background(), callDecision)).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForClusterMinimum))))
		g.Expect(f.reported[1].Message).To(Equal("refused: WaitingForClusterMinimum"))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
	})
	t.Run("an answer DisruptionCallOutcome leaves alone goes to the common error handling", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.sendErr = apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusBadRequest, "E9999")
		r := f.runner(nil)
		outcome := r.callIBM(context.Background(), callDecision)
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM), "the phase stays")
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Message).
			To(ContainSubstring("while removing worker pool workers of IBM cluster " + poolTestClusterID))
	})
}

// TestRemovalRunnerSendCall checks the call of a removal without victims, which persists and sends without recording a
// phase, and that a call DisruptionCallOutcome does not map is neither persisted nor sent.
func TestRemovalRunnerSendCall(t *testing.T) {
	t.Run("the call is persisted and sent without recording a phase", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		result, _, done := r.sendCall(context.Background(), r.call)
		g.Expect(done).To(BeFalse())
		g.Expect(result.Decision.Step).To(Equal(StepWaitForRemoval))
		g.Expect(f.sent).To(Equal(1))
		g.Expect(f.persistedAtSend).To(Equal(1))
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}), "no phase is recorded")
		g.Expect(f.reported).To(BeEmpty())
	})
	t.Run("a call that is no removal call is neither persisted nor sent", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		// A flow may pass a call other than the runner's own one, which newRemovalRunner checked.
		call := r.call
		call.op = kubernetesservice.OpListWorkers
		_, outcome, done := r.sendCall(context.Background(), call)
		g.Expect(done).To(BeTrue())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(outcome.Err).To(MatchError(ContainSubstring(
			`not sending the IBM call of the removal of pool workers: DisruptionCallOutcome call is "ListWorkers"`)))
		g.Expect(f.sent).To(BeZero(), "no IBM call is sent")
		g.Expect(f.persisted).To(BeEmpty(), "nothing is persisted")
	})
}
