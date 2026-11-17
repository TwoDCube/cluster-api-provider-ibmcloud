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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"

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
	t.Run("a call without its action gives no runner", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		in := f.input()
		in.call.action = ""
		r, err := newRemovalRunner(in)
		g.Expect(r).To(BeNil())
		g.Expect(err).To(MatchError(ContainSubstring(`managed: removal runner: call.action is ""; say what the call does`)))
	})
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
			// The message is every cluster-state gate's: on unsupported it names the master upgrade a replace needs.
			g.Expect(got).To(Equal(DisruptionCheckResult{Reason: ReasonWaitingForClusterState,
				Message: clusterStateMessage(poolTestClusterID, tc.state)}))
		})
	}
	t.Run("the_unsupported_message_of_a_replace_asks_for_a_master_upgrade", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(func(in *removalRunnerInput) { in.operation, in.clusterState = replace, "unsupported" })
		g.Expect(r.clusterStateCheck().Message).To(Equal("IBM cluster " + poolTestClusterID + " runs an unsupported version: " +
			"IBM does not add or reload workers on an unsupported version; upgrade the master first"))
	})
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
	// IBM's answer that the common error handling reported (a 403 for a missing role) is held like a refusal, so that
	// the wait after the call does not hide it; it is no refusal for LastCallRefused, which stays fail-closed: only
	// IBM's synchronous refusals count there.
	callError := "IBM Cloud refused the test identity (HTTP 403, code E3e8a) while removing worker pool workers of IBM cluster " +
		poolTestClusterID + "; grant it the roles"
	for _, tc := range []struct {
		name     string
		recorded metav1.Condition
		wantHeld bool
	}{
		{name: "a recorded call error keeps the hold as IBMAPIError", wantHeld: true,
			recorded: metav1.Condition{Status: metav1.ConditionTrue, Reason: MachinePoolIBMAPIErrorReason, Message: callError}},
		{name: "an IBMAPIError of another call is no record",
			recorded: metav1.Condition{Status: metav1.ConditionTrue, Reason: MachinePoolIBMAPIErrorReason,
				Message: "IBM Cloud refused the test identity (HTTP 403, code E3e8a) while listing the workers of IBM cluster " + poolTestClusterID}},
		{name: "a call error under another status is no record",
			recorded: metav1.Condition{Status: metav1.ConditionFalse, Reason: MachinePoolIBMAPIErrorReason, Message: callError}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			tc.recorded.Type = infrav1.IBMCloudManagedMachinePoolDeletingCondition
			f.pool.Status.Conditions = []metav1.Condition{tc.recorded}
			r := f.runner(nil)
			hold, held := r.refusalHold(waitForIBM)
			g.Expect(held).To(Equal(tc.wantHeld))
			g.Expect(r.recordedRefusal()).To(BeEmpty(), "a call error is no synchronous refusal")
			if !tc.wantHeld {
				g.Expect(hold).To(Equal(waitForIBM))
				return
			}
			g.Expect(hold).To(Equal(DisruptionDecision{Step: StepWaitForIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
				Reason: DisruptionReason(MachinePoolIBMAPIErrorReason), Message: callError}))
			g.Expect(keepsCallError(hold)).To(BeTrue())
		})
	}
}

func TestKeepsCallError(t *testing.T) {
	g := NewWithT(t)
	g.Expect(keepsCallError(DisruptionDecision{Reason: DisruptionReason(MachinePoolIBMAPIErrorReason)})).To(BeTrue())
	for _, reason := range []DisruptionReason{"", ReasonWaitingForClusterMinimum, ReasonExternallyAutoscaled, ReasonRemovingWorkerPool} {
		g.Expect(keepsCallError(DisruptionDecision{Reason: reason})).To(BeFalse(), string(reason))
	}
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
	t.Run("the first WaitingForVolumeDetach starts the detach wait, and a return to Draining keeps it", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		r.record(draining)
		g.Expect(f.pool.Status.Disruption.VolumeDetachStartTime.IsZero()).To(BeTrue(), "the drain is no detach wait")
		f.clock.Step(7 * time.Minute)
		r.record(detach)
		g.Expect(f.pool.Status.Disruption.VolumeDetachStartTime.Time).To(Equal(poolTestNow.Add(7*time.Minute)),
			"the detach wait begins when the drain ends, not when it started")
		g.Expect(f.pool.Status.Disruption.DrainStartTime.Time).To(Equal(poolTestNow))

		// A pod came back: Draining again, then the volume detach wait again. Cluster API keeps its
		// waitForNodeVolumeDetachStartTime once set, and so does the removal.
		f.clock.Step(time.Minute)
		r.record(draining)
		f.clock.Step(time.Minute)
		r.record(detach)
		g.Expect(f.pool.Status.Disruption.VolumeDetachStartTime.Time).To(Equal(poolTestNow.Add(7 * time.Minute)))
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

// detachWaitBegan is status.disruption.volumeDetachStartTime for a removal fixture recorded in phase: five minutes ago
// for a phase after Draining, which a removal reaches only through the step into WaitingForVolumeDetach that records
// it, and unset for Draining and no phase, whose volume detach wait has not begun.
//
// Parameters:
//   - phase is the fixture's status.disruption.phase.
func detachWaitBegan(phase infrav1.WorkerDisruptionPhase) metav1.Time {
	if phase == "" || phase == infrav1.WorkerDisruptionPhaseDraining {
		return metav1.Time{}
	}
	return metav1.NewTime(poolTestNow.Add(-5 * time.Minute))
}

func TestRemovalRunnerDisconnectedProgress(t *testing.T) {
	// limits sets nodeDrainTimeoutSeconds 600 and nodeVolumeDetachTimeoutSeconds 300.
	limits := func(f *runnerFixture) {
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](600)
		f.machinePool.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](300)
	}
	for _, tc := range []struct {
		name        string
		drainStart  time.Time
		detachStart time.Time
		modify      func(f *runnerFixture)
		want        DrainProgress
	}{
		{name: "without a recorded start no limit runs out", modify: limits},
		{name: "without limits nothing runs out", drainStart: poolTestNow.Add(-time.Hour), detachStart: poolTestNow.Add(-time.Hour)},
		{
			// The drain's own time never counts against the detach wait: the detach limit runs only once the step into
			// WaitingForVolumeDetach recorded its start.
			name: "a drain that ran out of time does not use up the detach wait", drainStart: poolTestNow.Add(-10 * time.Minute),
			modify: limits, want: DrainProgress{TimedOut: true},
		},
		{
			name: "the detach limit counts from the detach start", drainStart: poolTestNow.Add(-time.Hour),
			detachStart: poolTestNow.Add(-299 * time.Second), modify: limits, want: DrainProgress{TimedOut: true},
		},
		{
			name: "the detach limit runs out at the detach start plus the limit", drainStart: poolTestNow.Add(-time.Hour),
			detachStart: poolTestNow.Add(-300 * time.Second), modify: limits, want: DrainProgress{TimedOut: true, DetachTimedOut: true},
		},
		{name: "the exclusions hold", modify: func(f *runnerFixture) {
			f.pool.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: "",
				clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: ""}
		}, want: DrainProgress{Excluded: true, DetachExcluded: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			f.pool.Status.Disruption.DrainStartTime = metav1.NewTime(tc.drainStart)
			f.pool.Status.Disruption.VolumeDetachStartTime = metav1.NewTime(tc.detachStart)
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
	t.Run("the hold of a recorded call error leaves the flow's condition as written", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		hold := DisruptionDecision{Step: StepWaitForIBM, Reason: DisruptionReason(MachinePoolIBMAPIErrorReason), Message: "as written"}
		g.Expect(r.wait(hold, IntervalGuarded)).To(Equal(NotDone(IntervalGuarded, MachinePoolIBMAPIErrorReason)))
		g.Expect(f.reported).To(BeEmpty(), "the flow does not report it again")
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
	t.Run("a flow's reportCallError gets the answer instead of the pool's own report", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.sendErr = apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusForbidden, "E3e8a")
		var got []string
		r := f.runner(func(in *removalRunnerInput) {
			in.reportCallError = func(reason, message string) { got = append(got, reason+": "+message) }
		})
		g.Expect(r.callIBM(context.Background(), callDecision).Kind).To(Equal(OutcomeTerminal))
		g.Expect(got).To(ConsistOf(MachinePoolIBMAPIErrorReason + ": IBM Cloud refused the test identity (HTTP 403, code E3e8a) " +
			"while removing worker pool workers of IBM cluster " + poolTestClusterID + "; grant it " + poolKubernetesServiceRoles +
			" (the IAM roles of the managed Kubernetes book)"))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)).To(BeNil())
	})
	t.Run("a 403 stays on the flow's condition while the call waits out DisruptionSettleDelay", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.sendErr = apiError(kubernetesservice.OpRemoveWorkerPool, http.StatusForbidden, "E3e8a")
		r := f.runner(nil)
		// A deleted pool: MachinePoolScope.report, the default reportCallError, writes Deleting, the condition the pool
		// deletion reports its steps and records refusals on.
		f.pool.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
		g.Expect(r.callIBM(context.Background(), callDecision).Kind).To(Equal(OutcomeTerminal))
		refused := condition(f.pool, infrav1.IBMCloudManagedMachinePoolDeletingCondition).DeepCopy()
		g.Expect(refused.Reason).To(Equal(MachinePoolIBMAPIErrorReason))
		g.Expect(refused.Message).To(ContainSubstring("(HTTP 403, code E3e8a) while removing worker pool workers of IBM cluster"))

		// The reconcile that the status patch triggers a second later waits for IBM (StepWaitForIBM) and keeps the report.
		f.clock.Step(time.Second)
		next := f.runner(nil)
		wait := DisruptionDecision{Step: StepWaitForIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
			Reason: ReasonRemovingWorkerPool, Message: "the IBM call was sent at ..."}
		hold, held := next.refusalHold(wait)
		g.Expect(held).To(BeTrue())
		g.Expect(next.wait(hold, IntervalGuarded)).To(Equal(NotDone(IntervalGuarded, MachinePoolIBMAPIErrorReason)))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDeletingCondition)).To(Equal(refused), "a repeated refusal writes nothing new")
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

// cordonAt cordons the Nodes of the workers for the fixture's pool as a drain that started at start would have, with the
// drain-start-time, drain-owner and cordoned markers.
func (f *runnerFixture) cordonAt(start time.Time, workerIDs ...string) {
	f.t.Helper()
	helper := &drain.Helper{Client: f.workload, Clock: clocktesting.NewFakeClock(start), EvictionBudget: time.Second,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	nodes := VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, workerIDs)
	for i := range nodes {
		NewWithT(f.t).Expect(helper.Cordon(context.Background(), &nodes[i], drain.CordonOptions{Owner: DrainOwner(f.pool)})).To(Succeed())
	}
	f.calls.all()
}

// gonePoolDrainStart is when the drain of a pool that no longer exists started (cordonForGonePool), two hours before
// poolTestNow, so that a drain time limit counted from it would have run out.
var gonePoolDrainStart = poolTestNow.Add(-2 * time.Hour)

// cordonForGonePool cordons Node name of workload and excludes it from load balancers as the drain of the pool named by
// owner did at gonePoolDrainStart, with every marker of that drain: what a pool leaves on a Node when it goes mid-drain.
//
// Parameters:
//   - t is the test.
//   - workload is the workload cluster.
//   - name is the Node.
//   - owner is the drain-owner marker, "<namespace>/<pool>".
func cordonForGonePool(t *testing.T, workload client.Client, name, owner string) {
	t.Helper()
	g := NewWithT(t)
	node := &corev1.Node{}
	g.Expect(workload.Get(context.Background(), client.ObjectKey{Name: name}, node)).To(Succeed())
	helper := &drain.Helper{Client: workload, Clock: clocktesting.NewFakeClock(gonePoolDrainStart), EvictionBudget: time.Second,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	g.Expect(helper.Cordon(context.Background(), node, drain.CordonOptions{Owner: owner, ExcludeFromLoadBalancers: true})).To(Succeed())
}

// expectTakenOver checks that Node name of workload is cordoned and excluded from load balancers for pool's drain, which
// started at start: every marker names pool's drain, none the drain of the pool that was there before.
//
// Parameters:
//   - t is the test.
//   - workload is the workload cluster.
//   - name is the Node.
//   - pool is the pool that drains the Node now.
//   - start is when its drain of the Node started.
func expectTakenOver(t *testing.T, workload client.Client, name string, pool *infrav1.IBMCloudManagedMachinePool, start time.Time) {
	t.Helper()
	g := NewWithT(t)
	node := &corev1.Node{}
	g.Expect(workload.Get(context.Background(), client.ObjectKey{Name: name}, node)).To(Succeed())
	g.Expect(node.Spec.Unschedulable).To(BeTrue(), "Node %s stays cordoned", name)
	g.Expect(node.Labels).To(HaveKeyWithValue(corev1.LabelNodeExcludeBalancers, "true"))
	g.Expect(node.Annotations).To(Equal(map[string]string{
		infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner:     DrainOwner(pool),
		infrav1.NodeAnnotationPrefix + drain.MarkerDrainStartTime: start.UTC().Format(time.RFC3339),
		infrav1.NodeAnnotationPrefix + drain.MarkerCordoned:       "true",
		infrav1.NodeAnnotationPrefix + drain.MarkerExcludedFromLB: "true",
	}), "the markers of Node %s are this pool's", name)
}

// TestRemovalRunnerAdoptsGoneMarkers checks how the drain step treats the drain-owner markers of other pools on its
// victim Nodes: the marker of a pool that no longer exists is taken over, with that pool's cordon reverted first so that
// the drain time limit counts from this pool's cordon; the marker of a pool that still exists is never touched, and
// Cordon refuses that Node.
func TestRemovalRunnerAdoptsGoneMarkers(t *testing.T) {
	gone, live := poolTestNamespace+"/gone", poolTestNamespace+"/other"
	// newRunner returns the fixture with pool "other" and the runner of the pool deletion of victims, whose Nodes it
	// reads after prepare marked them.
	newRunner := func(t *testing.T, victims []string, prepare func(f *runnerFixture)) (*runnerFixture, *removalRunner) {
		t.Helper()
		f := newRunnerFixture(t)
		f.others = append(f.others, poolObject("other", "other"))
		prepare(f)
		r := f.runner(func(in *removalRunnerInput) {
			in.victims = victims
			in.victimNodes = VictimNodes(readNodes(t, f.workload), poolTestAccount, poolTestClusterID, victims)
		})
		return f, r
	}
	// observe runs the flow's part, the gate's read of the other pools' markers on every Node, and returns the pools that
	// still drain.
	observe := func(t *testing.T, f *runnerFixture, r *removalRunner) []string {
		t.Helper()
		token, err := f.scope().disruptionToken()
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		draining, err := r.drainingPools(context.Background(), token, readNodes(t, f.workload))
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		return draining
	}
	t.Run("the_marker_of_a_pool_that_no_longer_exists_is_taken_over_before_the_cordon", func(t *testing.T) {
		g := NewWithT(t)
		f, r := newRunner(t, []string{"w0", "w1"}, func(f *runnerFixture) { cordonForGonePool(t, f.workload, "10.0.0.1", gone) })
		g.Expect(observe(t, f, r)).To(BeEmpty(), "the gate passes over it")
		g.Expect(r.goneOwners).To(Equal([]string{gone}))
		g.Expect(r.runDrain(context.Background(), true)).To(Succeed())
		expectTakenOver(t, f.workload, "10.0.0.1", f.pool, poolTestNow)
		expectTakenOver(t, f.workload, "10.0.0.2", f.pool, poolTestNow)
		g.Expect(r.drainResult.StartTime).To(BeTemporally("==", poolTestNow), "the drain starts with this pool's cordon")
		g.Expect(f.calls.count("evict")).To(Equal(2))
	})
	t.Run("a_Check_takes_it_over_too_so_that_the_old_drain_start_decides_nothing", func(t *testing.T) {
		g := NewWithT(t)
		f, r := newRunner(t, []string{"w0", "w1"}, func(f *runnerFixture) {
			f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](3600)
			cordonForGonePool(t, f.workload, "10.0.0.1", gone)
		})
		observe(t, f, r)
		g.Expect(r.runDrain(context.Background(), false)).To(Succeed())
		g.Expect(r.drainResult.Progress.TimedOut).To(BeFalse(), "the gone pool's drain started two hours ago")
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.1"}, node)).To(Succeed())
		g.Expect(node.Spec.Unschedulable).To(BeFalse(), "the gone pool's cordon is reverted")
		g.Expect(node.Annotations).To(BeEmpty())
		g.Expect(node.Labels).NotTo(HaveKey(corev1.LabelNodeExcludeBalancers))
		g.Expect(f.calls.count("evict")).To(BeZero())
	})
	t.Run("the_marker_of_a_pool_that_exists_is_left_and_Cordon_refuses_the_Node", func(t *testing.T) {
		g := NewWithT(t)
		f, r := newRunner(t, []string{"w0", "w1"}, func(f *runnerFixture) { cordonForGonePool(t, f.workload, "10.0.0.1", live) })
		g.Expect(observe(t, f, r)).To(Equal([]string{live}))
		g.Expect(r.goneOwners).To(BeEmpty())
		g.Expect(r.runDrain(context.Background(), true)).To(MatchError(drain.ErrDrainOwnerConflict))
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.1"}, node)).To(Succeed())
		g.Expect(node.Annotations).To(HaveKeyWithValue(infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner, live))
	})
	t.Run("a_Node_that_is_no_victim_keeps_the_marker", func(t *testing.T) {
		g := NewWithT(t)
		f, r := newRunner(t, []string{"w0"}, func(f *runnerFixture) { cordonForGonePool(t, f.workload, "10.0.0.2", gone) })
		observe(t, f, r)
		g.Expect(r.runDrain(context.Background(), true)).To(Succeed())
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.2"}, node)).To(Succeed())
		g.Expect(node.Annotations).To(HaveKeyWithValue(infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner, gone))
	})
	t.Run("an_excluded_drain_cordons_nothing_and_takes_nothing_over", func(t *testing.T) {
		g := NewWithT(t)
		f, r := newRunner(t, []string{"w0", "w1"}, func(f *runnerFixture) {
			f.machinePool.Spec.Template.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: ""}
			cordonForGonePool(t, f.workload, "10.0.0.1", gone)
		})
		observe(t, f, r)
		f.calls.all()
		g.Expect(r.runDrain(context.Background(), true)).To(Succeed())
		g.Expect(f.calls.count("cordon")).To(BeZero())
	})
	t.Run("a_failed_revert_is_returned_and_names_the_Node_and_the_gone_pool", func(t *testing.T) {
		g := NewWithT(t)
		f, r := newRunner(t, []string{"w0", "w1"}, func(f *runnerFixture) { cordonForGonePool(t, f.workload, "10.0.0.1", gone) })
		observe(t, f, r)
		// The Node changed after this reconcile read it, so the revert's optimistic lock fails.
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.1"}, node)).To(Succeed())
		node.Labels["topology.kubernetes.io/zone"] = "us-south-1"
		g.Expect(f.workload.Update(context.Background(), node)).To(Succeed())
		g.Expect(r.runDrain(context.Background(), true)).To(MatchError(ContainSubstring("taking over Node 10.0.0.1, whose " +
			"drain-owner marker names IBMCloudManagedMachinePool " + gone + ", which no longer exists")))
	})
}

// TestRemovalRunnerCheckDrain checks the drain observation of a removal that can still be given up (the scale-down and
// the replace): it never evicts, reads the Nodes only while this pool holds the token in a phase before the IBM call,
// and falls back to the exclusions and the recorded drain start without the workload cluster.
func TestRemovalRunnerCheckDrain(t *testing.T) {
	draining := infrav1.WorkerDisruptionPhaseDraining
	knobs := DrainProgress{}
	for _, tc := range []struct {
		name       string
		o          DisruptionObservation
		modify     func(in *removalRunnerInput)
		want       *DrainProgress
		wantChecks int
	}{
		{name: "without the token only the exclusions are read", o: DisruptionObservation{Phase: draining, Token: TokenNotHeld}, want: &knobs},
		{name: "before the first cordon only the exclusions are read", o: DisruptionObservation{Token: TokenHeld}, want: &knobs},
		{name: "after the IBM call only the exclusions are read",
			o: DisruptionObservation{Phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval, Token: TokenHeld}, want: &knobs},
		{name: "a removal that drains nothing counts as excluded", o: DisruptionObservation{Phase: draining, Token: TokenHeld},
			modify: func(in *removalRunnerInput) { in.drainsNothing = true }, want: &DrainProgress{Excluded: true, DetachExcluded: true}},
		{name: "without the workload cluster the recorded start decides", o: DisruptionObservation{Phase: draining, Token: TokenHeld},
			modify: disconnected, want: &DrainProgress{}},
		{name: "a drain in flight is checked once, without evicting", o: DisruptionObservation{Phase: draining, Token: TokenHeld},
			wantChecks: 1},
		{name: "a drain resumed from the Node markers is checked", o: DisruptionObservation{ResumeFromNodeMarkers: true, Token: TokenHeld},
			wantChecks: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newRunnerFixture(t)
			r := f.runner(tc.modify)
			got, err := r.checkDrain(context.Background(), tc.o)
			g.Expect(err).NotTo(HaveOccurred())
			if tc.want != nil {
				g.Expect(got).To(Equal(*tc.want))
			}
			again, err := r.checkDrain(context.Background(), tc.o)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(again).To(Equal(got), "the observation of this reconcile is kept")
			g.Expect(f.calls.count("list pods")).To(Equal(2*tc.wantChecks), "one pod listing per victim Node per check")
			g.Expect(f.calls.count("evict")).To(BeZero())
			g.Expect(f.calls.count("cordon")).To(BeZero())
			g.Expect(r.evicted).To(BeFalse())
		})
	}
}

// TestRemovalRunnerAnnounceTimeout checks that a drain that leaves Draining on its time limit names the pods left on
// the Nodes in DrainTimeoutExceeded, once, and sends no eviction there.
func TestRemovalRunnerAnnounceTimeout(t *testing.T) {
	timedOut := func(t *testing.T) (*runnerFixture, *removalRunner) {
		t.Helper()
		f := newRunnerFixture(t)
		f.machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](60)
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationScaleDown,
			Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0", "w1"}}
		f.cordonAt(poolTestNow.Add(-2*time.Minute), "w0", "w1")
		r := f.runner(func(in *removalRunnerInput) {
			in.operation = infrav1.WorkerDisruptionOperationScaleDown
			in.victimNodes = VictimNodes(readNodes(f.t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0", "w1"})
		})
		_, err := r.checkDrain(context.Background(), DisruptionObservation{Phase: infrav1.WorkerDisruptionPhaseDraining, Token: TokenHeld})
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		NewWithT(t).Expect(r.drainResult.Progress.TimedOut).To(BeTrue())
		f.calls.all()
		return f, r
	}
	t.Run("the_pods_left_are_named_once_without_an_eviction", func(t *testing.T) {
		g := NewWithT(t)
		f, r := timedOut(t)
		g.Expect(r.announceTimeout(context.Background())).To(Succeed())
		g.Expect(f.calls.count("evict")).To(BeZero(), "no eviction after the limit")
		events := f.events()
		g.Expect(events).To(HaveLen(2), "one Warning per Node")
		g.Expect(events).To(HaveEach(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 60s")))
		g.Expect(r.announceTimeout(context.Background())).To(Succeed())
		g.Expect(f.events()).To(BeEmpty(), "announced once per reconcile")
	})
	t.Run("not when the drain stays in Draining", func(t *testing.T) {
		g := NewWithT(t)
		f, r := timedOut(t)
		f.pool.Status.Disruption.Phase = infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach
		g.Expect(r.announceTimeout(context.Background())).To(Succeed())
		g.Expect(f.events()).To(BeEmpty())
	})
	t.Run("not for a drain that has not timed out", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationDeletePool,
			Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0", "w1"}}
		r := f.runner(nil)
		g.Expect(r.runDrain(context.Background(), false)).To(Succeed())
		g.Expect(r.announceTimeout(context.Background())).To(Succeed())
		g.Expect(f.events()).To(BeEmpty())
		g.Expect(f.calls.count("evict")).To(BeZero())
	})
}

func TestRemovalRunnerDrainVictims(t *testing.T) {
	drainDecision := DisruptionDecision{Step: StepDrain, Phase: infrav1.WorkerDisruptionPhaseDraining, Reason: ReasonDrainingNode,
		DrainingReason: ReasonDrainingNode, Message: "cordoning every victim Node, then evicting their pods"}
	scaleDown := func(in *removalRunnerInput) { in.operation = infrav1.WorkerDisruptionOperationScaleDown }
	t.Run("cordons every victim, evicts, records Draining and waits drainRetryInterval", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(scaleDown)
		g.Expect(r.drainVictims(context.Background(), drainDecision)).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(f.calls.all()).To(Equal([]string{"cordon 10.0.0.1", "cordon 10.0.0.2",
			"list pods 10.0.0.1", "evict apps/web-w0", "list pods 10.0.0.2", "evict apps/web-w1"}))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Reason).To(Equal(string(ReasonDrainingNode)))
	})
	t.Run("a budget's refusal is reported at once", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.workload = newWorkloadClient(t, f.calls, map[string]error{"web-w0": pdbRefusal()}, victimNode("10.0.0.1", "w0"),
			workloadPod("web-w0", "10.0.0.1"), victimNode("10.0.0.2", "w1"), workloadPod("web-w1", "10.0.0.2"))
		r := f.runner(scaleDown)
		g.Expect(r.drainVictims(context.Background(), drainDecision)).To(Equal(NotDone(IntervalDrainRetry, string(ReasonDrainingNode))))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Reason).To(Equal(string(ReasonDrainBlocked)))
	})
	t.Run("an observation that already evicted is not repeated", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(scaleDown)
		g.Expect(r.runDrain(context.Background(), true)).To(Succeed())
		f.calls.all()
		r.drainVictims(context.Background(), drainDecision)
		g.Expect(f.calls.all()).To(BeEmpty())
	})
	t.Run("a removal that drains nothing only moves on", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(func(in *removalRunnerInput) { scaleDown(in); in.drainsNothing = true })
		r.drainVictims(context.Background(), drainDecision)
		g.Expect(f.calls.all()).To(BeEmpty())
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition).Status).To(Equal(metav1.ConditionFalse))
	})
}

// TestRemovalRunnerUncordon checks the revert of an abort: only CAPIBM's cordon is reverted, a human's stays, and
// without the workload cluster nothing is given up unless the flow allows it.
func TestRemovalRunnerUncordon(t *testing.T) {
	draining := infrav1.WorkerDisruptionPhaseDraining
	t.Run("only_CAPIBM's_cordon_and_load_balancer_label_are_reverted", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		// A human cordoned w1's Node before the removal, so CAPIBM's cordon flipped nothing there.
		node := &corev1.Node{}
		g.Expect(f.workload.Get(context.Background(), client.ObjectKey{Name: "10.0.0.2"}, node)).To(Succeed())
		node.Spec.Unschedulable = true
		g.Expect(f.workload.Update(context.Background(), node)).To(Succeed())
		helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
		for _, n := range VictimNodes(readNodes(t, f.workload), poolTestAccount, poolTestClusterID, []string{"w0", "w1"}) {
			g.Expect(helper.Cordon(context.Background(), &n, drain.CordonOptions{Owner: DrainOwner(f.pool),
				ExcludeFromLoadBalancers: true})).To(Succeed())
		}
		r := f.runner(nil)
		reverted, _, held := r.uncordon(context.Background(), poolTestAccount, []string{"w0", "w1"}, draining, false)
		g.Expect(held).To(BeFalse())
		g.Expect(reverted).To(Equal(2))
		for _, n := range readNodes(t, f.workload) {
			g.Expect(n.Spec.Unschedulable).To(Equal(n.Name == "10.0.0.2"), "the human's cordon stays: "+n.Name)
			g.Expect(n.Labels).NotTo(HaveKey(corev1.LabelNodeExcludeBalancers), n.Name)
			g.Expect(hasDrainOwner(&n, f.pool)).To(BeFalse(), n.Name)
		}
	})
	t.Run("without the workload cluster the removal holds with its Nodes cordoned", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace, Phase: draining,
			WorkerIDs: []string{"w0"}, DrainStartTime: metav1.NewTime(poolTestNow.Add(-time.Minute))}
		r := f.runner(func(in *removalRunnerInput) {
			disconnected(in)
			in.operation, in.victims = infrav1.WorkerDisruptionOperationReplace, []string{"w0"}
		})
		_, outcome, held := r.uncordon(context.Background(), poolTestAccount, []string{"w0"}, draining, false)
		g.Expect(held).To(BeTrue())
		g.Expect(outcome).To(Equal(NotDone(IntervalGuarded, string(ReasonWaitingForWorkloadCluster))))
		g.Expect(f.pool.Status.Disruption.Phase).To(Equal(draining), "nothing is given up")
		g.Expect(f.reported[0].Message).To(Equal("the workload cluster is not connected; CAPIBM uncordons the Nodes it cordoned once it is"))
	})
	t.Run("a flow that touched nothing gives up without the workload cluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(disconnected)
		reverted, _, held := r.uncordon(context.Background(), poolTestAccount, []string{"w0"}, draining, true)
		g.Expect(held).To(BeFalse())
		g.Expect(reverted).To(BeZero())
	})
	t.Run("no workers and a removal that drains nothing uncordon nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newRunnerFixture(t)
		r := f.runner(nil)
		_, _, held := r.uncordon(context.Background(), poolTestAccount, nil, draining, false)
		g.Expect(held).To(BeFalse())
		r = f.runner(func(in *removalRunnerInput) { disconnected(in); in.drainsNothing = true })
		_, _, held = r.uncordon(context.Background(), poolTestAccount, []string{"w0"}, draining, false)
		g.Expect(held).To(BeFalse())
		g.Expect(f.calls.all()).To(BeEmpty())
	})
}
