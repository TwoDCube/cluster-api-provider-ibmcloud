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
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
)

// This file is the removal machinery that every worker removal of an IBMCloudManagedMachinePool shares: the pool
// deletion (machinepool_delete.go) and the scale-down (machinepool_scaledown.go), and the worker replacement next.
// Each flow observes its removal in its own way and asks DecideDisruptionStep for the step. What it then does is the
// same for every flow and lives here once: it writes status.disruption and announces the drain, observes the drain
// (also while the workload cluster is not connected), checks the IBM cluster state and the disruption guards right
// before the IBM call, sends that call after persisting the status, and holds after IBM refused it. One copy matters
// for safety: every removal must check the cluster state again right before its call, or it could send its call to a
// cluster that turned unavailable during a long drain. The runner is parameterised by what differs between the flows:
// the operation and its victims, the one IBM call, how the flow reports a step on its condition, and where it records
// IBM's refusal of the call.
//
// The two removals that can be given up before their IBM call, the scale-down and the worker replacement, also share
// how they observe the drain without evicting (checkDrain), announce a drain that leaves Draining on its time limit
// (announceTimeout), run the drain step (drainVictims) and revert their cordons when they give up (uncordon). The pool
// deletion never gives up, so it evicts while it observes and keeps its own drain step.

// removalCall is the one IBM call of a removal (StepCallIBM): RemoveWorkerPool for a pool deletion, ResizeWorkerPool for
// a scale-down, ReplaceWorker for a replace.
type removalCall struct {
	// op is the call's operation. DisruptionCallOutcome maps IBM's answer per operation, and ibmProblem classifies the
	// answers that DisruptionCallOutcome leaves to the common error handling. It must be an operation that
	// DisruptionCallOutcome maps (removalCallReason); newRemovalRunner and sendCall refuse any other before anything is
	// sent.
	op kubernetesservice.Operation

	// action says what the call does, for the condition and the error of a failed call, for example "removing worker
	// pool default of IBM cluster c1". Required: the report of a failed call names it, and that is how the runner finds
	// the report again (removalRunner.recordedCallError).
	action string

	// send makes the call and returns IBM's error; nil for success.
	send func(ctx context.Context) error
}

// refusalRecord says where a flow records IBM's synchronous refusal of its call: E1437 (WaitingForClusterMinimum) or the
// 403 of an autoscaled pool (ExternallyAutoscaled), which DisruptionCallOutcome holds as StepHold; IBM refuses both
// synchronously and without side effects. Status has no field for the answer to a call (no API change), so the flow's
// own condition is the record: the call reports the refusal there, and every later step that reports something else
// replaces it. The hold after a refusal reads it back (removalRunner.refusalHold), and so does a flow that needs
// DisruptionObservation.LastCallRefused.
type refusalRecord struct {
	// condition is the condition type the flow reports its steps on: Deleting for a pool deletion, WorkerPoolReady for a
	// scale-down.
	condition string

	// status is the status of that condition while the flow reports a step on it: True for Deleting, False for
	// WorkerPoolReady.
	status metav1.ConditionStatus

	// message returns the fixed message that the refusal of reason writes. When set, callIBM reports it instead of
	// DisruptionCallOutcome's message, and the record counts only while the condition message carries it: another step
	// can report the same reason with another message (the capacity rule of the pre-call gate reports
	// WaitingForClusterMinimum too, after a call that IBM may have accepted), and only the refusal's own message tells
	// them apart. nil keeps DisruptionCallOutcome's message and reads the reason alone. That is enough for a flow that
	// reads the record only within DisruptionSettleDelay of its latest call (the pool deletion), because every call
	// reports its own reason before it is sent, so the condition names the answer to that call.
	message func(reason DisruptionReason) string
}

// read returns the reason of the refusal that pool's condition records, with the message to hold with: the fixed
// message when the record has one, else the condition message as read. It returns "" when the condition records no
// refusal.
//
// Parameters:
//   - pool is the IBMCloudManagedMachinePool.
func (rec refusalRecord) read(pool *infrav1.IBMCloudManagedMachinePool) (DisruptionReason, string) {
	recorded := conditions.Get(pool, rec.condition)
	if recorded == nil || recorded.Status != rec.status {
		return "", ""
	}
	reason := DisruptionReason(recorded.Reason)
	if reason != ReasonWaitingForClusterMinimum && reason != ReasonExternallyAutoscaled {
		return "", ""
	}
	if rec.message == nil {
		return reason, recorded.Message
	}
	if !strings.Contains(recorded.Message, rec.message(reason)) {
		return "", ""
	}
	return reason, rec.message(reason)
}

// removalRunner carries out, for one reconcile, the steps of a worker removal that every flow shares (see the top of
// this file). Create it with newRemovalRunner. The flow keeps its observation, its decision loop and the steps only it
// has (the hand-over of a pool deletion; the victim correction and the abort of a scale-down). For the shared steps it
// calls record, runDrain, disconnectedProgress, callIBM, sendCall, refusalHold and wait, and it builds the gates of its
// DisruptionObservation with clusterStateCheck, preCallGate and drainingPools.
type removalRunner struct {
	// s is the pool's scope.
	s *MachinePoolScope
	// clients are this reconcile's IBM clients.
	clients *poolClients
	// pool is the IBM pool as this reconcile listed it.
	pool *kubernetesservice.WorkerPool

	// operation is status.disruption.operation of the removal.
	operation infrav1.WorkerDisruptionOperation
	// victims are the workers the removal drains and waits for, sorted: status.disruption.workerIDs. A scale-down
	// replaces them when IBM's resize removes other workers (StepCorrectVictims).
	victims []string
	// clusterState is the IBM cluster state of this reconcile, for the cluster-state checks.
	clusterState string
	// call is the removal's one IBM call.
	call removalCall
	// refusal is where the flow records IBM's refusal of the call.
	refusal refusalRecord
	// reportFlow reports a decision on the flow's condition, in the flow's words.
	reportFlow func(d DisruptionDecision)
	// reportCallError reports IBM's answer to a call that DisruptionCallOutcome leaves to the common error handling on
	// the flow's condition (removalRunnerInput.reportCallError).
	reportCallError func(reason, message string)
	// drainsNothing is true for a removal that drains no Node at all (a scale-down in ScaleDownResizeOnly mode): it
	// builds no drain step, announces no drain and keeps Draining False.
	drainsNothing bool

	// knobs are the pool's drain settings, read in this reconcile.
	knobs DrainKnobs
	// workload is the workload cluster's client from ClusterCache; nil while it is not connected. uncordon reverts the
	// cordons through it.
	workload client.Client
	// drain is the drain step; nil while the workload cluster is not connected and for a removal that drains nothing.
	drain *DrainStep
	// victimNodes are the Nodes of the victims, sorted by name; none while the workload cluster is not connected.
	victimNodes []corev1.Node
	// resumed is true when the removal resumes from this pool's drain-owner markers because status.disruption was lost
	// (a clusterctl move or a lost status patch): the drain started before and is not announced again. The flow sets it
	// once it knows.
	resumed bool
	// goneOwners are the drain-owner markers, found on Nodes of the workload cluster, that name a pool that no longer
	// exists; drainingPools sets them in the flow's observation, and runDrain takes such a marker over on a victim Node
	// (adoptGoneMarkers).
	goneOwners []string
	// drainResult is the drain observed in this reconcile; evicted tells whether it came from Evict.
	drainResult *DrainStepResult
	evicted     bool
}

// removalRunnerInput are the inputs of newRemovalRunner: what the flow observed, and what differs between the flows.
type removalRunnerInput struct {
	// scope is the pool's scope. Required.
	scope *MachinePoolScope
	// clients are this reconcile's IBM clients; their identity sends the call. Required.
	clients *poolClients
	// pool is the IBM pool as this reconcile listed it; its autoscaleEnabled maps IBM's answer to the call. Required.
	pool *kubernetesservice.WorkerPool
	// operation is the removal: DeletePool, ScaleDown or Replace. It selects the cluster-state check
	// (removalPoolOperation). Required.
	operation infrav1.WorkerDisruptionOperation
	// victims are the workers the removal drains and waits for, sorted; empty for a removal that takes no listed worker
	// away (a pool without workers, a resize that removes none).
	victims []string
	// clusterState is the IBM cluster state of this reconcile: the pool deletion reads it from IBM (GetCluster), the
	// scale-down from the control plane's status.state.
	clusterState string
	// call is the removal's one IBM call; its send and an op that DisruptionCallOutcome maps are required.
	call removalCall
	// refusal is where the flow records IBM's refusal of the call; its condition and status are required.
	refusal refusalRecord
	// report reports a decision on the flow's condition. Required.
	report func(d DisruptionDecision)
	// reportCallError reports IBM's answer to the call that DisruptionCallOutcome leaves to the common error handling
	// (MachinePoolScope.ibmProblemOn: a 401, a 403 that is no refusal of an autoscaled pool, a 400 other than E1437) on
	// the condition named by refusal, from the reason (IBMAPIError) and ibmProblemOn's message, which carries IBM's code
	// only, never the incident ID that changes with every answer. The flow may add its own words, but the message it
	// writes must keep ibmProblemOn's, which names call.action: refusalHold finds the report that way and keeps it for
	// the rest of DisruptionSettleDelay. Optional: nil reports with MachinePoolScope.report (WorkerPoolReady, or
	// Deleting while the pool is deleted), which is the condition of the pool deletion and the scale-down. A replace
	// reports on WorkersUpToDate and must pass its own: WorkerPoolReady is reported afresh from IBM's pool on every
	// reconcile, which would hide the answer at once.
	reportCallError func(reason, message string)
	// drainsNothing is true for a removal that drains no Node at all (ScaleDownResizeOnly).
	drainsNothing bool
	// workload is the workload cluster's client from ClusterCache; nil while it is not connected.
	workload client.Client
	// victimNodes are the Nodes of the victims (VictimNodes); none while the workload cluster is not connected.
	victimNodes []corev1.Node
}

// newRemovalRunner validates in and returns the runner of one reconcile. It reads the pool's drain settings and, while
// the workload cluster is connected and the removal drains, builds the drain step; nothing else is read or called.
//
// Parameters:
//   - in are the inputs; see removalRunnerInput.
//
// It returns no runner and an error naming every missing or wrong input, each one a programming error of the flow. A
// call whose op DisruptionCallOutcome does not map is one of them. It returns no runner and NewDrainStep's error when
// the drain step cannot be built.
func newRemovalRunner(in removalRunnerInput) (*removalRunner, error) {
	var problems []string
	if in.scope == nil {
		problems = append(problems, "scope is nil; pass the pool's MachinePoolScope")
	}
	if in.clients == nil {
		problems = append(problems, "clients is nil; pass this reconcile's IBM clients")
	}
	if in.pool == nil {
		problems = append(problems, "pool is nil; pass the IBM pool of this reconcile's listing")
	}
	if err := validateOperation("operation", in.operation); err != nil {
		problems = append(problems, err.Error())
	}
	if in.call.op == "" || in.call.send == nil {
		problems = append(problems, "call needs its operation and its send function; pass the removal's IBM call")
	} else if _, err := removalCallReason(in.call.op); err != nil {
		// DisruptionCallOutcome rejects an op it does not map only after the call went out (sendCall), and the
		// persisted ibmRequestTime only delays the next attempt: a runner built with such an op would send a real IBM
		// mutation and fail afterwards, once every DisruptionSettleDelay.
		problems = append(problems, "call.op: "+err.Error())
	}
	if in.call.action == "" {
		problems = append(problems, `call.action is ""; say what the call does, for example "removing worker pool p of IBM cluster c"`)
	}
	if in.refusal.condition == "" || in.refusal.status == "" {
		problems = append(problems, "refusal needs the condition type and status the flow reports on")
	}
	if in.report == nil {
		problems = append(problems, "report is nil; pass the function that reports a decision on the flow's condition")
	}
	if len(problems) > 0 {
		return nil, errors.New("managed: removal runner: " + strings.Join(problems, "; "))
	}
	r := &removalRunner{s: in.scope, clients: in.clients, pool: in.pool, operation: in.operation, victims: in.victims,
		clusterState: in.clusterState, call: in.call, refusal: in.refusal, reportFlow: in.report, reportCallError: in.reportCallError,
		drainsNothing: in.drainsNothing, knobs: DrainKnobsFor(in.scope.MachinePool, in.scope.Pool), workload: in.workload,
		victimNodes: in.victimNodes}
	if r.reportCallError == nil {
		r.reportCallError = in.scope.report
	}
	if in.workload == nil || in.drainsNothing {
		return r, nil
	}
	drainStep, err := NewDrainStep(DrainStepInput{WorkloadClient: in.workload, Clock: r.s.params.Clock, Recorder: r.s.params.Recorder,
		Pool: r.s.Pool, Knobs: r.knobs})
	if err != nil {
		return nil, err
	}
	r.drain = drainStep
	return r, nil
}

// removalPoolOperation returns the pool operation that the cluster-state gate checks for a removal (PoolMutationAllowed):
// Remove for a pool deletion, Shrink for a scale-down, Replace for a replace. IBM's documented states block them
// differently: unsupported still allows a shrink and a removal, never a replace.
//
// Parameters:
//   - op is the removal.
func removalPoolOperation(op infrav1.WorkerDisruptionOperation) PoolOperation {
	switch op {
	case infrav1.WorkerDisruptionOperationDeletePool:
		return PoolOperationRemove
	case infrav1.WorkerDisruptionOperationScaleDown:
		return PoolOperationShrink
	default:
		return PoolOperationReplace
	}
}

// removalNoun names a removal in an error, for example "persisting the scale-down of pool p".
//
// Parameters:
//   - op is the removal.
func removalNoun(op infrav1.WorkerDisruptionOperation) string {
	switch op {
	case infrav1.WorkerDisruptionOperationDeletePool:
		return "removal"
	case infrav1.WorkerDisruptionOperationScaleDown:
		return "scale-down"
	default:
		return "replace"
	}
}

// clusterStateCheck is the pool's cluster-state gate for this removal, as a DisruptionCheckResult so that it can head
// the pre-call gate: allowed, or WaitingForClusterState with the IBM cluster and its state in the message
// (PoolMutationAllowed). Without it IBM's documented refusal would look like an invalid configuration. The flows read
// it at the Gated step (DisruptionGates.ClusterStateAllows), before a call without victims, and in preCallGate. The
// message is the one every cluster-state gate of the pool writes (clusterStateMessage): only a replace is ever refused
// on an unsupported cluster, and that message names the master upgrade it needs.
func (r *removalRunner) clusterStateCheck() DisruptionCheckResult {
	if PoolMutationAllowed(r.clusterState, removalPoolOperation(r.operation)) {
		return DisruptionCheckResult{Allowed: true}
	}
	return DisruptionCheckResult{Reason: ReasonWaitingForClusterState,
		Message: clusterStateMessage(r.s.ControlPlane.Status.ClusterID, r.clusterState)}
}

// preCallGate is the pre-call gate for this removal (DisruptionObservation.PreCallGate): the IBM cluster state of this
// reconcile first, then check without the settle delay. The Gated step reads the cluster state only before the first
// cordon, and a long drain can outlive it: a cluster that turned unavailable or scaled_down meanwhile gets no call,
// and DecideDisruptionStep holds the removal in phase WaitingForIBM with its Nodes cordoned (WaitingForClusterState),
// rechecked at guardedPoll until the state allows the call again. check runs only when the state allows the call.
//
// Parameters:
//   - check is the flow's CheckDisruption for its removal; it is called with skipSettleDelay true.
func (r *removalRunner) preCallGate(check func(skipSettleDelay bool) (DisruptionCheckResult, error)) (DisruptionCheckResult, error) {
	if state := r.clusterStateCheck(); !state.Allowed {
		return state, nil
	}
	return check(true)
}

// disconnectedProgress is the drain progress while the workload cluster is not connected. Nothing can be observed, so
// Done and VolumesDetached stay false, but the exclusions hold and the time limits still run out: the drain limit
// counts from status.disruption.drainStartTime, which the first drain step recorded, and the detach limit from
// status.disruption.volumeDetachStartTime, which the step into WaitingForVolumeDetach recorded, as in DrainStep.Check.
// Cluster API records both starts in the Machine's status for the same reason: a set time limit still runs out and
// then allows the call. A drain that runs out of time while the workload cluster is away therefore waits the whole
// detach limit after it, as in Cluster API, instead of having both limits end together. Without a recorded start that
// limit does not run out.
func (r *removalRunner) disconnectedProgress() DrainProgress {
	now, disruption := r.s.params.Clock.Now(), r.s.Pool.Status.Disruption
	return DrainProgress{
		Excluded:       r.knobs.ExcludeDraining,
		TimedOut:       drain.TimeoutExceeded(now, disruption.DrainStartTime.Time, r.knobs.NodeDrainTimeoutSeconds),
		DetachExcluded: r.knobs.ExcludeVolumeDetachWait,
		DetachTimedOut: drain.TimeoutExceeded(now, disruption.VolumeDetachStartTime.Time, r.knobs.NodeVolumeDetachTimeoutSeconds),
	}
}

// drainingPools returns the other pools that still drain Nodes of the workload cluster (DisruptionToken.DrainingPools),
// for the gate of the flow's observation, and keeps the drain-owner markers of the pools that no longer exist for the
// drain step (goneOwners, adoptGoneMarkers). The gate passes over such a marker; the drain step must then not fail on
// it either.
//
// Parameters:
//   - ctx is the reconcile context; one read per other pool's marker.
//   - token is the flow's handle of the Cluster's disruption token; it knows this pool and reads the other pools.
//   - nodes are every Node of the workload cluster.
//
// It returns DrainingPools' error; the caller requeues.
func (r *removalRunner) drainingPools(ctx context.Context, token *DisruptionToken, nodes []corev1.Node) ([]string, error) {
	draining, err := token.DrainingPools(ctx, nodes)
	if err != nil {
		return nil, err
	}
	r.goneOwners = slices.DeleteFunc(OtherDrainOwners(nodes, DrainOwner(r.s.Pool)), func(owner string) bool {
		return slices.Contains(draining, owner)
	})
	return draining, nil
}

// adoptGoneMarkers reverts, on every victim Node whose drain-owner marker names a pool that no longer exists
// (goneOwners), what that pool recorded there: drain.Helper.Uncordon reverts only CAPIBM's own cordon and load-balancer
// exclusion and removes the markers, so a cordon a human set stays. The drain step can then cordon the Node for this
// pool; drain.Helper.Cordon refuses a Node another pool's marker is on (drain.ErrDrainOwnerConflict), which protects a
// drain in flight, but a marker that outlived its pool protects nothing (a finalizer removed by hand, a status lost while
// the workload cluster was not connected). Without this the removal would hold the Cluster's disruption token and fail
// at its first cordon on every reconcile, without recording anything, and every other pool of the Cluster would wait
// for the token. The Node's drain time limit then counts from this pool's cordon, since the old drain was never this
// pool's. A drain that is excluded cordons nothing, so nothing is taken over.
//
// Parameters:
//   - ctx is the reconcile context.
//
// It returns the error of an Uncordon; the caller requeues.
func (r *removalRunner) adoptGoneMarkers(ctx context.Context) error {
	if len(r.goneOwners) == 0 || r.knobs.ExcludeDraining {
		return nil
	}
	helper := &drain.Helper{Client: r.workload, Clock: r.s.params.Clock, EvictionBudget: EvictionBudgetPerReconcile,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	key := infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner
	for i := range r.victimNodes {
		node := &r.victimNodes[i]
		gone := node.Annotations[key]
		if !slices.Contains(r.goneOwners, gone) {
			continue
		}
		// Uncordon updates node on success, so the drain step patches the Node with its new resourceVersion.
		if err := helper.Uncordon(ctx, node); err != nil {
			return fmt.Errorf("taking over Node %s, whose drain-owner marker names IBMCloudManagedMachinePool %s, which no "+
				"longer exists: %w", node.Name, gone, err)
		}
		ctrl.LoggerFrom(ctx).Info("Took over a Node whose drain-owner marker names a pool that no longer exists",
			"Node", node.Name, "previousOwner", gone, "owner", DrainOwner(r.s.Pool))
	}
	return nil
}

// runDrain runs Evict (evict true) or Check on the victim Nodes and keeps the result. Without the drain step (a
// workload cluster that is not connected, which only a removal that needs nothing from it gets this far without), it
// touches nothing and keeps disconnectedProgress. Every caller holds the Cluster's disruption token, so a victim Node
// that carries the marker of a pool that no longer exists is taken over first (adoptGoneMarkers), also before a Check:
// that pool's drain start must not decide this pool's drain time limit.
//
// Parameters:
//   - ctx is the reconcile context.
//   - evict selects Evict.
func (r *removalRunner) runDrain(ctx context.Context, evict bool) error {
	if r.drain == nil {
		r.drainResult, r.evicted = &DrainStepResult{Progress: r.disconnectedProgress()}, r.evicted || evict
		return nil
	}
	if err := r.adoptGoneMarkers(ctx); err != nil {
		return err
	}
	var result DrainStepResult
	var err error
	if evict {
		result, err = r.drain.Evict(ctx, r.victimNodes)
	} else {
		result, err = r.drain.Check(ctx, r.victimNodes)
	}
	if err != nil {
		return err
	}
	r.drainResult, r.evicted = &result, r.evicted || evict
	return nil
}

// checkDrain observes the drain for the effective phase of o, for a removal that can still be given up before its IBM
// call (a scale-down whose MachinePool went back up, a replace whose worker is no longer due): in every phase before the
// call it checks the victims without evicting (DrainStep.Check), and the gates read only the exclusions. Evicting while
// observing would only move pods for nothing when the decision then aborts the removal; the eviction runs once the
// decision is StepDrain (drainVictims). Nothing is cordoned or evicted before this pool holds the disruption token.
// A removal that drains nothing counts its drain and detach wait as excluded. The pool deletion, which never gives up,
// evicts while it observes instead (poolRemoval.drainProgress).
//
// Parameters:
//   - ctx is the reconcile context.
//   - o is the observation so far.
func (r *removalRunner) checkDrain(ctx context.Context, o DisruptionObservation) (DrainProgress, error) {
	knobsOnly := DrainProgress{Excluded: r.knobs.ExcludeDraining || r.drainsNothing,
		DetachExcluded: r.knobs.ExcludeVolumeDetachWait || r.drainsNothing}
	phase := o.Phase
	if phase == "" && o.ResumeFromNodeMarkers {
		phase = infrav1.WorkerDisruptionPhaseDraining
	}
	if r.drainsNothing || o.Token != TokenHeld || !isPreCallPhase(phase) {
		return knobsOnly, nil
	}
	if r.drain == nil {
		return r.disconnectedProgress(), nil
	}
	if r.drainResult == nil {
		if err := r.runDrain(ctx, false); err != nil {
			return DrainProgress{}, err
		}
	}
	return r.drainResult.Progress, nil
}

// announceTimeout emits DrainTimeoutExceeded when the drain leaves phase Draining because its time limit ran out: the
// observation only checked the Nodes (checkDrain), and Evict is what names the pods left on a timed-out Node (it sends
// no eviction there). It runs once, on the step out of Draining. ctx is the reconcile context.
func (r *removalRunner) announceTimeout(ctx context.Context) error {
	from := r.s.Pool.Status.Disruption.Phase
	leavesDraining := from == infrav1.WorkerDisruptionPhaseDraining || (from == "" && r.resumed)
	if !leavesDraining || r.drain == nil || r.evicted || r.drainResult == nil || !r.drainResult.Progress.TimedOut {
		return nil
	}
	return r.runDrain(ctx, true)
}

// drainVictims carries out StepDrain for a removal that observes with checkDrain: cordon every victim Node, then evict
// (every victim is cordoned before the first eviction, so that evicted pods never land on another victim), record the
// decision's phase and wait drainRetryInterval. A removal that drains nothing only moves on. The first eviction of a
// removal runs after the decision, so its refusals are reported at once (DrainBlocked): the next reconcile then sees
// DrainBlocked as the previous reason and does not announce it again.
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (r *removalRunner) drainVictims(ctx context.Context, d DisruptionDecision) Outcome {
	if !r.evicted && !r.drainsNothing {
		if err := r.runDrain(ctx, true); err != nil {
			return Transient(err)
		}
	}
	if r.drainResult != nil && r.drainResult.Progress.Blocked {
		d.DrainingReason = ReasonDrainBlocked
	}
	r.record(d.Phase)
	return r.wait(d, IntervalDrainRetry)
}

// uncordon reverts CAPIBM's cordon of the Nodes of the workers when a removal is given up: drain.Helper.Uncordon
// reverts only CAPIBM's own changes, so a Node a human cordoned before stays cordoned. It returns how many of those
// Nodes carried this pool's drain-owner marker, for the NodeUncordoned event. While the workload cluster is not
// connected it returns a hold, and true: the Nodes cannot be uncordoned yet, so nothing is given up, unless the flow
// can give up without it (withoutWorkload: nothing of the removal touched the workload cluster). A removal that
// drains nothing, and an empty set of workers, uncordon nothing.
//
// Parameters:
//   - ctx is the reconcile context.
//   - account is the IBM account of the cluster (AccountIDFromCRN), which the Nodes' provider IDs carry.
//   - workerIDs are the workers whose Nodes to uncordon.
//   - phase is the phase to keep while holding; "" records none.
//   - withoutWorkload is true when the flow may give up without the workload cluster.
func (r *removalRunner) uncordon(ctx context.Context, account string, workerIDs []string, phase infrav1.WorkerDisruptionPhase,
	withoutWorkload bool) (int, Outcome, bool) {
	if r.drainsNothing || len(workerIDs) == 0 {
		return 0, Outcome{}, false
	}
	if r.workload == nil {
		if withoutWorkload {
			return 0, Outcome{}, false
		}
		hold := DisruptionDecision{Step: StepHold, Phase: phase, Reason: ReasonWaitingForWorkloadCluster,
			Message: "the workload cluster is not connected; CAPIBM uncordons the Nodes it cordoned once it is"}
		if phase != "" {
			r.record(phase)
		}
		return 0, r.wait(hold, IntervalGuarded), true
	}
	helper := &drain.Helper{Client: r.workload, Clock: r.s.params.Clock, EvictionBudget: EvictionBudgetPerReconcile,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	// The victims' Nodes as this reconcile last wrote them: Uncordon patches with the Node's resourceVersion.
	nodes := VictimNodes(r.victimNodes, account, r.s.ControlPlane.Status.ClusterID, workerIDs)
	reverted := 0
	for i := range nodes {
		if hasDrainOwner(&nodes[i], r.s.Pool) {
			reverted++
		}
		if err := helper.Uncordon(ctx, &nodes[i]); err != nil {
			return 0, Transient(err), true
		}
	}
	return reverted, Outcome{}, false
}

// hasDrainOwner reports whether node carries pool's drain-owner marker, which CAPIBM writes with every cordon.
//
// Parameters:
//   - node is the Node.
//   - pool is the IBMCloudManagedMachinePool.
func hasDrainOwner(node *corev1.Node, pool *infrav1.IBMCloudManagedMachinePool) bool {
	return node.Annotations[infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner] == DrainOwner(pool)
}

// record writes status.disruption for phase: the runner's operation and victims, and the drain start, kept, or taken
// from this reconcile's drain (whose start comes from the Node markers of a drain that started before) or now. The
// first WaitingForVolumeDetach of the removal sets the volume detach start to now, the moment the detach wait begins;
// it is kept afterwards, also through a return to Draining, as Cluster API keeps a Machine's
// status.deletion.waitForNodeVolumeDetachStartTime. ibmRequestTime is kept. The drain events of the phase change are
// emitted here.
//
// Parameters:
//   - phase is the phase the step records.
func (r *removalRunner) record(phase infrav1.WorkerDisruptionPhase) {
	disruption := &r.s.Pool.Status.Disruption
	from := disruption.Phase
	if from == "" && r.resumed {
		// The drain started before status.disruption was lost; it is not announced again.
		from = infrav1.WorkerDisruptionPhaseDraining
	}
	disruption.Operation = r.operation
	disruption.Phase = phase
	disruption.WorkerIDs = slices.Clone(r.victims)
	if disruption.DrainStartTime.IsZero() {
		start := r.s.params.Clock.Now()
		if r.drainResult != nil && !r.drainResult.StartTime.IsZero() {
			start = r.drainResult.StartTime
		}
		disruption.DrainStartTime = metav1.NewTime(start)
	}
	if phase == infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach && disruption.VolumeDetachStartTime.IsZero() {
		disruption.VolumeDetachStartTime = metav1.NewTime(r.s.params.Clock.Now())
	}
	r.drainEvents(from, phase)
}

// drainEvents emits the drain events of a phase change. DrainStarted and DrainCompleted count the victim Nodes, so they
// need the workload cluster. Without it nothing was cordoned or evicted in this reconcile: a removal that needs nothing
// from it announces nothing, and a drain that moves on because its time limit ran out (disconnectedProgress) is
// announced with the Warning DrainTimeoutExceeded, which says that the pods left are not known. A removal that drains
// nothing announces nothing.
//
// Parameters:
//   - from is the phase before the step, as record derived it.
//   - to is the phase the step records.
func (r *removalRunner) drainEvents(from, to infrav1.WorkerDisruptionPhase) {
	switch {
	case r.drainsNothing:
	case r.drain != nil:
		RecordDrainPhaseChange(r.s.params.Recorder, r.s.Pool, from, to, len(r.victimNodes), r.knobs.ExcludeDraining)
	case from == infrav1.WorkerDisruptionPhaseDraining && to == infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach && !r.knobs.ExcludeDraining:
		// Without the workload cluster the drain leaves Draining only when it is excluded or timed out (drainFinished).
		r.s.params.Recorder.Eventf(r.s.Pool, corev1.EventTypeWarning, EventDrainTimeoutExceeded,
			"the drain time limit of %ds (MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed since %s "+
				"while the workload cluster is not connected; CAPIBM proceeds without knowing which pods are left on the Nodes of pool %s",
			ptrValue(r.knobs.NodeDrainTimeoutSeconds), r.s.Pool.Status.Disruption.DrainStartTime.UTC().Format(time.RFC3339),
			r.s.Pool.Spec.PoolName)
	}
}

// report reports the decision: on the flow's condition in the flow's words (reportFlow), then on Draining from its
// DrainingReason, with the drain message of this reconcile (deterministic, at most 3 pods per category, so that a
// repeated reconcile writes nothing). A removal that drains nothing keeps Draining False. The hold of a recorded call
// error (keepsCallError) leaves the flow's condition as reportCallError wrote it: reporting it again in the flow's
// words would wrap the message once more on every reconcile.
//
// Parameters:
//   - d is the decision.
func (r *removalRunner) report(d DisruptionDecision) {
	if !keepsCallError(d) {
		r.reportFlow(d)
	}
	draining := metav1.Condition{
		Type:   infrav1.IBMCloudManagedMachinePoolDrainingCondition,
		Status: metav1.ConditionFalse,
		Reason: infrav1.IBMCloudManagedMachinePoolNotDrainingReason,
	}
	if d.DrainingReason != "" && !r.drainsNothing {
		draining.Status, draining.Reason, draining.Message = metav1.ConditionTrue, string(d.DrainingReason), d.Message
		if r.drainResult != nil && r.drainResult.Message != "" {
			draining.Message = r.drainResult.Message
		}
	}
	conditions.Set(r.s.Pool, draining)
}

// wait reports the decision (report) and returns NotDone after interval with the decision's reason.
//
// Parameters:
//   - d is the decision.
//   - interval is the wait.
func (r *removalRunner) wait(d DisruptionDecision, interval Interval) Outcome {
	r.report(d)
	return NotDone(interval, string(d.Reason))
}

// callIBM carries out StepCallIBM, the removal's one IBM call: it records phase WaitingForIBM with ibmRequestTime and
// lastDisruptionTime set to now and reports the decision, then sends the call (sendCall, which persists all of it
// first), records the phase of IBM's answer and waits. A refusal (E1437, the 403 of an autoscaled pool) holds in
// WaitingForIBM with the Nodes cordoned, reported with the record's fixed message when it has one, and is retried at
// most once per DisruptionSettleDelay. The decision is reported before the persist, so that the flow's condition
// always names the answer to the latest call, also when an unknown outcome (5xx, timeout) leaves the conditions as
// they are: refusalHold reads it.
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (r *removalRunner) callIBM(ctx context.Context, d DisruptionDecision) Outcome {
	now := metav1.NewTime(r.s.params.Clock.Now())
	r.record(infrav1.WorkerDisruptionPhaseWaitingForIBM)
	r.s.Pool.Status.Disruption.IBMRequestTime = now
	r.s.Pool.Status.LastDisruptionTime = now
	r.report(d)
	result, outcome, done := r.sendCall(ctx, r.call)
	if done {
		return outcome
	}
	r.record(result.Decision.Phase)
	if result.Decision.Step != StepHold {
		return r.wait(result.Decision, IntervalPoolTransition)
	}
	if r.refusal.message != nil {
		result.Decision.Message = r.refusal.message(result.Decision.Reason)
	}
	return r.wait(result.Decision, IntervalGuarded)
}

// sendCall persists the pool, then sends call and maps IBM's answer with DisruptionCallOutcome. The persist comes first
// so that a restart never sends a call twice within DisruptionSettleDelay: the caller has set ibmRequestTime or
// lastDisruptionTime before; when the persist fails, no call is sent. It returns the handled answer, or the outcome to
// return and true when call is not a removal call (then nothing is persisted or sent), the persist failed, or IBM's
// answer is left to the common error handling (ibmProblemOn), which reports it on the flow's condition through
// reportCallError. A call without victims (a pool without workers, a resize that removes none) uses it directly.
//
// Parameters:
//   - ctx is the reconcile context.
//   - call is the IBM call to send.
func (r *removalRunner) sendCall(ctx context.Context, call removalCall) (DisruptionCallResult, Outcome, bool) {
	// newRemovalRunner checks the runner's own call, but a flow may pass another one here (the scale-down's resize
	// without victims). DisruptionCallOutcome would reject an op it does not map only after IBM carried the call out.
	if _, err := removalCallReason(call.op); err != nil {
		return DisruptionCallResult{}, Transient(fmt.Errorf("not sending the IBM call of the %s of pool %s: %w",
			removalNoun(r.operation), r.s.Pool.Spec.PoolName, err)), true
	}
	if err := r.s.persist(ctx); err != nil {
		return DisruptionCallResult{}, Transient(fmt.Errorf("persisting the %s of pool %s before the IBM call: %w",
			removalNoun(r.operation), r.s.Pool.Spec.PoolName, err)), true
	}
	err := call.send(ctx)
	result, verr := DisruptionCallOutcome(call.op, err, r.pool.AutoscaleEnabled)
	switch {
	case verr != nil:
		return DisruptionCallResult{}, Transient(verr), true
	case !result.Handled:
		return DisruptionCallResult{}, r.s.ibmProblemOn(r.reportCallError, r.clients, call.op, call.action, err), true
	}
	return result, Outcome{}, false
}

// recordedCallError returns the message of the report of IBM's answer to the runner's call that the flow's condition
// records, and true: the condition named by refusal, with its status, the reason IBMAPIError, and a message that names
// call.action, which only ibmProblemOn's report of this call writes (reportCallError). A failed listing or credential
// resolution reports IBMAPIError too, but names another action, and a step that reports anything else replaces the
// record. It returns "" and false when the condition records no such answer.
func (r *removalRunner) recordedCallError() (string, bool) {
	recorded := conditions.Get(r.s.Pool, r.refusal.condition)
	if recorded == nil || recorded.Status != r.refusal.status || recorded.Reason != MachinePoolIBMAPIErrorReason ||
		!strings.Contains(recorded.Message, r.call.action) {
		return "", false
	}
	return recorded.Message, true
}

// keepsCallError reports whether d is the hold of a recorded call error that refusalHold returned: its reason is
// IBMAPIError, which no step of DecideDisruptionStep reports, and the flow's condition already carries the report as
// reportCallError wrote it. A flow that reports a decision without the runner (report) must leave the condition as it
// is for such a decision too.
//
// Parameters:
//   - d is the decision.
func keepsCallError(d DisruptionDecision) bool {
	return d.Reason == DisruptionReason(MachinePoolIBMAPIErrorReason)
}

// recordedRefusal returns the reason of IBM's synchronous refusal of the latest call as the flow's condition records it
// (refusalRecord.read), or "". A flow's DisruptionObservation.LastCallRefused starts from it.
func (r *removalRunner) recordedRefusal() DisruptionReason {
	reason, _ := r.refusal.read(r.s.Pool)
	return reason
}

// refusalHold keeps the hold of a refused call while the call waits out DisruptionSettleDelay. It returns d with the
// reason and message of the refusal the flow's condition records (WaitingForClusterMinimum for E1437, which IBM
// refuses synchronously and without side effects; ExternallyAutoscaled for the 403 of an autoscaled pool), and true.
// It does the same for IBM's answer that the common error handling reported (recordedCallError: a 403 for a missing
// Kubernetes Service role, a 401, a 400), with the reason IBMAPIError, which report leaves on the condition as written
// (keepsCallError). Otherwise it returns d and false. The flow asks only while its latest call is less than
// DisruptionSettleDelay old (StepWaitForIBM, or the gate of a call without victims), and every call reports its own
// answer first (callIBM), so the condition names the answer to that call.
//
// The wait itself stays: ibmRequestTime and lastDisruptionTime keep the retry out of the delay, also in the reconcile
// that this pool's own status patch triggers at once (the For watch has no generation predicate); taking them back would
// retry the refused call in a loop. Only the report changes: DecideDisruptionStep's wait after a call ("the IBM call was
// sent at ...") would otherwise replace the hold for the delay after each refusal, so that the reason flips back and
// forth and status is rewritten on every reconcile, and the operator would never see the cause of a refusal they must
// fix, such as a missing role, while the Nodes stay cordoned and the Cluster's disruption token stays held.
//
// Parameters:
//   - d is the decision of a wait after a call.
func (r *removalRunner) refusalHold(d DisruptionDecision) (DisruptionDecision, bool) {
	if reason, message := r.refusal.read(r.s.Pool); reason != "" {
		d.Reason, d.Message, d.DrainingReason = reason, message, ""
		return d, true
	}
	if message, recorded := r.recordedCallError(); recorded {
		d.Reason, d.Message, d.DrainingReason = DisruptionReason(MachinePoolIBMAPIErrorReason), message, ""
		return d, true
	}
	return d, false
}
