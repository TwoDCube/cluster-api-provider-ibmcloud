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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file decides when and how CAPIBM removes workers. Every worker removal (a replace for a rollout, a scale-down, a
// pool deletion) runs as a small state machine whose state is status.disruption.phase. Both functions here are pure:
// the pool controller observes IBM, the workload cluster and the disruption token, passes what it saw, and carries out
// the one step the decision names. Two safety properties hold for these functions, and their tests check them over
// generated inputs: every cordon and the IBM call need the Cluster's disruption token, so two pools never remove
// workers at the same time; and the IBM call is never decided while evictable pods may remain on a victim Node, unless
// the drain timed out or is excluded.

// DisruptionReason is a condition reason of the disruption gates and steps. The values are the reason constants of
// api/managed/v1alpha1, which use the same token on every condition that has it (WorkersUpToDate for a replace,
// WorkerPoolReady for a scale-down, Deleting for a pool deletion, and Draining); the caller sets the reason on the
// condition of its flow.
type DisruptionReason string

// Reasons of the gates before the first cordon (the Gated phase of every worker removal).
const (
	// ReasonWaitingForClusterState surfaces when the IBM cluster state forbids the pool operation
	// (PoolMutationAllowed).
	ReasonWaitingForClusterState DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForClusterStateReason
	// ReasonWaitingForControlPlaneUpgrade surfaces while the master is pending or updating (replace only).
	ReasonWaitingForControlPlaneUpgrade DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForControlPlaneUpgradeReason
	// ReasonWaitingForSettledCluster surfaces while a worker of the cluster is changing, or another removal happened
	// less than DisruptionSettleDelay ago (CheckDisruption rules 1 and 2).
	ReasonWaitingForSettledCluster DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForSettledClusterReason
	// ReasonWaitingForClusterMinimum surfaces when removing the workers would leave fewer ready workers than IBM's
	// minimum (rule 3, scale-down and pool deletion), or when IBM answered E1437.
	ReasonWaitingForClusterMinimum DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForClusterMinimumReason
	// ReasonWaitingForCapacity surfaces when a replace would leave fewer ready workers than IBM's minimum (rule 3,
	// replace); the message names the MachinePool size that allows a rolling replacement.
	ReasonWaitingForCapacity DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForCapacityReason
	// ReasonExternallyAutoscaled surfaces when IBM's cluster autoscaler owns the pool size, so CAPIBM neither resizes
	// nor replaces.
	ReasonExternallyAutoscaled DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyExternallyAutoscaledReason
	// ReasonAutoRebalanceDisabled surfaces when the victim's zone has IBM auto-rebalance off, so a replace would create
	// no replacement (replace only).
	ReasonAutoRebalanceDisabled DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateAutoRebalanceDisabledReason
	// ReasonWaitingForWorkloadCluster surfaces while ClusterCache has no connection to the workload cluster, so
	// nothing can be cordoned, evicted or checked.
	ReasonWaitingForWorkloadCluster DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForWorkloadClusterReason
	// ReasonWorkersNotReady surfaces while another worker of the pool is not deployed, healthy and Ready for
	// minReadySeconds, so a replace does not start yet (replace only; DisruptionGates.OthersAvailable).
	ReasonWorkersNotReady DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWorkersNotReadyReason
	// ReasonWaitingForDisruptionToken surfaces while another pool of the Cluster holds the disruption token, or still
	// drains Nodes (its drain-owner marker).
	ReasonWaitingForDisruptionToken DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForDisruptionTokenReason
)

// Reasons of the steps after the first cordon: the drain, the IBM call and the wait for IBM.
const (
	// ReasonDrainingNode surfaces while the victim Nodes are cordoned and their pods evicted (replace and scale-down);
	// it is also the Draining condition's reason while evictions progress.
	ReasonDrainingNode DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateDrainingNodeReason
	// ReasonDrainingNodes surfaces while every Node of the pool is drained before the pool is removed (pool deletion).
	ReasonDrainingNodes DisruptionReason = infrav1.IBMCloudManagedMachinePoolDeletingDrainingNodesReason
	// ReasonDrainBlocked surfaces when a PodDisruptionBudget refused an eviction (Draining condition).
	ReasonDrainBlocked DisruptionReason = infrav1.IBMCloudManagedMachinePoolDrainingDrainBlockedReason
	// ReasonWaitingForVolumeDetach surfaces while the drained Nodes still have attached volumes (Draining condition).
	ReasonWaitingForVolumeDetach DisruptionReason = infrav1.IBMCloudManagedMachinePoolDrainingWaitingForVolumeDetachReason
	// ReasonReplacingWorker surfaces while IBM replaces the worker (replace).
	ReasonReplacingWorker DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReplacingWorkerReason
	// ReasonResizing surfaces while IBM resizes the pool (scale-down).
	ReasonResizing DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason
	// ReasonRemovingWorkerPool surfaces while IBM removes the pool (pool deletion).
	ReasonRemovingWorkerPool DisruptionReason = infrav1.IBMCloudManagedMachinePoolDeletingRemovingWorkerPoolReason
	// ReasonWaitingForReplacement surfaces while the replacement worker is not yet deployed, normal and Ready for
	// minReadySeconds.
	ReasonWaitingForReplacement DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForReplacementReason
	// ReasonReplacementNotCreated surfaces when IBM created no replacement within ReplacementNotCreatedAfter of the
	// call.
	ReasonReplacementNotCreated DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReplacementNotCreatedReason
	// ReasonWorkerOperationStuck surfaces when IBM's removal has taken longer than WorkerStuckAfter.
	ReasonWorkerOperationStuck DisruptionReason = infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWorkerOperationStuckReason
)

// DisruptionCheck is the input of CheckDisruption: what the pool controller observed for one planned worker removal.
type DisruptionCheck struct {
	// Operation is the planned removal: Replace, ScaleDown or DeletePool. It selects the capacity reason:
	// WaitingForCapacity for a replace, WaitingForClusterMinimum otherwise. Required.
	Operation infrav1.WorkerDisruptionOperation

	// Distribution is the cluster's distribution; it selects ClusterMinimumWorkers. Required.
	Distribution controlplanev1.Distribution

	// Now is the caller's clock reading. Required.
	Now time.Time

	// Workers are every worker of the IBM cluster from one ListWorkers call (all pools; deleted records are ignored).
	Workers []kubernetesservice.Worker

	// TransitionalSince maps a worker ID to the time the caller first saw that worker transitional. A worker without
	// an entry is never treated as stuck. IBM's worker listing carries no state timestamp, so only the caller can
	// know. Optional.
	TransitionalSince map[string]time.Time

	// Pools are every IBMCloudManagedMachinePool of the Cluster, the caller's included; their status.lastDisruptionTime
	// feeds the settle delay.
	Pools []infrav1.IBMCloudManagedMachinePool

	// RemovedReady is the worst-case number of ready workers the removal takes away: 1 for a replace;
	// min((sizePerZone - desired) × zones, ready workers of the pool) for a scale-down; the ready workers of the pool
	// for a pool deletion. 0 skips rules 1 and 2: such a removal takes no ready capacity away, and it is how a user
	// stops paying for a stalled scale-up. Must not be negative.
	RemovedReady int

	// SkipSettleDelay leaves out rule 2. The pre-call gate (DisruptionObservation.PreCallGate) sets it: it re-checks
	// the settled cluster and the capacity right before the IBM call, while the settle delay was checked before the
	// drain started.
	SkipSettleDelay bool

	// MachinePoolName is the caller's MachinePool, named in the WaitingForCapacity message of a replace ("scale
	// MachinePool <name> to <n>"). Optional; without it the message gives no size.
	MachinePoolName string

	// Replicas is that MachinePool's spec.replicas, the base of the size the message suggests.
	Replicas int32

	// ZoneCount is the number of zones of the caller's pool; the suggested size adds whole workers per zone because
	// IBM sizes pools per zone. Must be at least 1 when MachinePoolName is set.
	ZoneCount int
}

// DisruptionCheckResult is CheckDisruption's answer.
type DisruptionCheckResult struct {
	// Allowed is true when every rule passed.
	Allowed bool

	// Reason is the reason of the first rule that failed; "" when Allowed.
	Reason DisruptionReason

	// Message explains the failure, or names the stuck workers when Allowed. It is deterministic (IDs are sorted) and
	// carries no IBM free text, so a repeated reconcile writes the same condition.
	Message string

	// StuckWorkerIDs lists, sorted, the transitional workers that no longer block because they have been transitional
	// for longer than WorkerStuckAfter (reason WorkerOperationStuck in the caller's message).
	StuckWorkerIDs []string
}

// CheckDisruption decides whether a worker removal may start now. It is pure and applies three rules in order:
//
//  1. Settled cluster: no worker of the cluster is transitional (IsWorkerTransitional); a worker transitional for
//     longer than WorkerStuckAfter is treated like a failed worker and no longer blocks. Else WaitingForSettledCluster.
//  2. Settle delay: Now is at least DisruptionSettleDelay after the newest lastDisruptionTime of the Cluster's pools,
//     which covers IBM's lag before a removal shows up in the worker list. Else WaitingForSettledCluster. Skipped with
//     SkipSettleDelay.
//  3. Capacity: the ready workers of the cluster minus RemovedReady stay at or above ClusterMinimumWorkers (IBM's own
//     E1437 thresholds, observed on IBM Cloud: 1 worker for IKS, 2 for ROKS). Else WaitingForCapacity for a replace and
//     WaitingForClusterMinimum otherwise.
//
// With RemovedReady 0, rules 1 and 2 are skipped and rule 3 still runs. The caller still needs the disruption token:
// these rules read cached state, and only the token keeps two pools from passing them at the same moment.
//
// Parameters:
//   - in: the observation; see DisruptionCheck.
//
// It returns an error naming the field to fix when in is invalid.
func CheckDisruption(in DisruptionCheck) (DisruptionCheckResult, error) {
	if err := in.validate(); err != nil {
		return DisruptionCheckResult{}, err
	}
	var stuck []string
	if in.RemovedReady > 0 {
		var blocked *DisruptionCheckResult
		if stuck, blocked = in.checkSettled(); blocked != nil {
			return *blocked, nil
		}
		if !in.SkipSettleDelay {
			if blocked := in.checkSettleDelay(); blocked != nil {
				blocked.StuckWorkerIDs = stuck
				return *blocked, nil
			}
		}
	}
	if blocked := in.checkCapacity(); blocked != nil {
		blocked.StuckWorkerIDs = stuck
		return *blocked, nil
	}
	result := DisruptionCheckResult{Allowed: true, StuckWorkerIDs: stuck}
	if len(stuck) > 0 {
		result.Message = fmt.Sprintf("%s: IBM has been changing %s for more than %d minutes; treated like a failed "+
			"worker, it no longer blocks worker removals", ReasonWorkerOperationStuck, listWorkers(stuck),
			int(WorkerStuckAfter.Minutes()))
	}
	return result, nil
}

// validate returns an error naming the first invalid field of in.
func (in DisruptionCheck) validate() error {
	if err := validateOperation("DisruptionCheck.Operation", in.Operation); err != nil {
		return err
	}
	switch {
	case in.Distribution != controlplanev1.DistributionKubernetes && in.Distribution != controlplanev1.DistributionOpenShift:
		return fmt.Errorf("DisruptionCheck.Distribution is %q; want %q or %q", in.Distribution,
			controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift)
	case in.Now.IsZero():
		return errors.New("DisruptionCheck.Now is not set; pass the controller clock's current time")
	case in.RemovedReady < 0:
		return fmt.Errorf("DisruptionCheck.RemovedReady is %d; want 0 or more ready workers", in.RemovedReady)
	case in.MachinePoolName != "" && in.ZoneCount < 1:
		return fmt.Errorf("DisruptionCheck.ZoneCount is %d for MachinePool %s; want the number of zones of the pool, at least 1",
			in.ZoneCount, in.MachinePoolName)
	case in.Replicas < 0:
		return fmt.Errorf("DisruptionCheck.Replicas is %d; want the MachinePool's spec.replicas", in.Replicas)
	}
	return nil
}

// checkSettled applies rule 1. It returns the sorted IDs of stuck workers and, when a worker blocks, the result to
// return.
func (in DisruptionCheck) checkSettled() ([]string, *DisruptionCheckResult) {
	var blocking, stuck []string
	activity := map[string]string{}
	for _, w := range in.Workers {
		if !IsWorkerTransitional(w) {
			continue
		}
		if since, found := in.TransitionalSince[w.ID]; found && in.Now.Sub(since) > WorkerStuckAfter {
			stuck = append(stuck, w.ID)
			continue
		}
		blocking = append(blocking, w.ID)
		// The value of a pending operation is IBM free text, which no status message carries; only the state token goes
		// into the message.
		activity[w.ID] = w.ActualState
		if !slices.Contains(TransitionalWorkerStates(), w.ActualState) {
			activity[w.ID] = "running an IBM operation"
		}
	}
	sort.Strings(stuck)
	if len(blocking) == 0 {
		return stuck, nil
	}
	sort.Strings(blocking)
	message := fmt.Sprintf("worker %s is %s", blocking[0], activity[blocking[0]])
	if len(blocking) > 1 {
		message += fmt.Sprintf(" (and %d more changing workers)", len(blocking)-1)
	}
	message += "; workers are removed only while no worker of the cluster is changing"
	return stuck, &DisruptionCheckResult{Reason: ReasonWaitingForSettledCluster, Message: message}
}

// checkSettleDelay applies rule 2. It returns the result to return when the delay has not passed, else nil.
func (in DisruptionCheck) checkSettleDelay() *DisruptionCheckResult {
	var newest time.Time
	var newestPool string
	for i := range in.Pools {
		last := in.Pools[i].Status.LastDisruptionTime.Time
		name := in.Pools[i].Name
		// Ties go to the smaller name, so the message does not depend on the order of the list.
		if last.After(newest) || (last.Equal(newest) && !last.IsZero() && name < newestPool) {
			newest, newestPool = last, name
		}
	}
	if newest.IsZero() {
		return nil
	}
	until := newest.Add(DisruptionSettleDelay)
	if !in.Now.Before(until) {
		return nil
	}
	return &DisruptionCheckResult{
		Reason: ReasonWaitingForSettledCluster,
		Message: fmt.Sprintf("IBMCloudManagedMachinePool %s removed workers at %s; the next removal waits until %s, so that "+
			"IBM's worker list shows the last one", newestPool, newest.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339)),
	}
}

// checkCapacity applies rule 3. It returns the result to return when the capacity is too small, else nil.
func (in DisruptionCheck) checkCapacity() *DisruptionCheckResult {
	ready := 0
	for _, w := range in.Workers {
		if IsWorkerReady(w) {
			ready++
		}
	}
	minimum := ClusterMinimumWorkers(in.Distribution)
	left := ready - in.RemovedReady
	if left >= minimum {
		return nil
	}
	message := fmt.Sprintf("the cluster has %d ready workers; removing %d would leave %d, below IBM's minimum of %d "+
		"workers for a %s cluster (%s)", ready, in.RemovedReady, max(left, 0), minimum, in.Distribution, ClusterMinimumErrorCode)
	if in.Operation != infrav1.WorkerDisruptionOperationReplace {
		return &DisruptionCheckResult{
			Reason:  ReasonWaitingForClusterMinimum,
			Message: message + "; keep more workers, or add ready workers in another pool first",
		}
	}
	if in.MachinePoolName == "" {
		message += "; add ready workers to allow a rolling replacement"
	} else {
		// IBM sizes pools per zone, so the suggestion adds whole workers per zone until the shortfall is covered.
		shortfall := minimum - left
		perZone := (shortfall + in.ZoneCount - 1) / in.ZoneCount
		suggested := int(in.Replicas) + perZone*in.ZoneCount
		message += fmt.Sprintf("; scale MachinePool %s to %d to allow a rolling replacement", in.MachinePoolName, suggested)
	}
	return &DisruptionCheckResult{Reason: ReasonWaitingForCapacity, Message: message}
}

// WorkerBeingRemoved reports whether IBM is already removing a worker: it is deleting or deleted, or it has a pending
// operation while PendingOperationBlocks is true. The pool controller skips the IBM call for such a victim (so the call
// is idempotent after a restart, and after an out-of-band replace) and sets RemovalProgress.Removing when this holds
// for every victim, or the victim is no longer listed. Before CAPIBM's own call such a removal is an out-of-band one,
// which DecideDisruptionStep waits out and then aborts (RemovalProgress).
//
// Parameters:
//   - w: the victim as ListWorkers returned it.
func WorkerBeingRemoved(w kubernetesservice.Worker) bool {
	return w.ActualState == WorkerStateDeleting || w.ActualState == WorkerStateDeleted ||
		(PendingOperationBlocks && w.PendingOperation != "")
}

// listWorkers names sorted worker IDs in a message: "worker a", "workers a, b, c" or "workers a, b, c and 2 more", so
// that a long list cannot grow a condition message without bound.
//
// Parameters:
//   - ids: the sorted worker IDs; at least one.
func listWorkers(ids []string) string {
	const maxNamed = 3
	if len(ids) == 1 {
		return "worker " + ids[0]
	}
	if len(ids) <= maxNamed {
		return "workers " + strings.Join(ids, ", ")
	}
	return fmt.Sprintf("workers %s and %d more", strings.Join(ids[:maxNamed], ", "), len(ids)-maxNamed)
}

// validateOperation returns an error when op is not one of the three removal operations.
//
// Parameters:
//   - field: the name of the field in the error, for example "DisruptionCheck.Operation".
//   - op: the value to check.
func validateOperation(field string, op infrav1.WorkerDisruptionOperation) error {
	switch op {
	case infrav1.WorkerDisruptionOperationReplace, infrav1.WorkerDisruptionOperationScaleDown,
		infrav1.WorkerDisruptionOperationDeletePool:
		return nil
	default:
		return fmt.Errorf("%s is %q; want %q, %q or %q", field, op, infrav1.WorkerDisruptionOperationReplace,
			infrav1.WorkerDisruptionOperationScaleDown, infrav1.WorkerDisruptionOperationDeletePool)
	}
}

// DisruptionStep is the one thing the pool controller does for a worker removal in this reconcile.
type DisruptionStep string

const (
	// StepNone means that no removal is wanted and none is in flight: nothing to do, except releasing the token when
	// ReleaseToken says so.
	StepNone DisruptionStep = "None"
	// StepWait means that a gate before the first cordon fails. Nothing is cordoned, status.disruption stays absent,
	// and the failing gate is the reason; release the token when ReleaseToken says so.
	StepWait DisruptionStep = "Wait"
	// StepAcquireToken asks the caller to acquire the Cluster's disruption token (DisruptionToken.Acquire) and to
	// decide again with the new token state.
	StepAcquireToken DisruptionStep = "AcquireToken"
	// StepDrain asks the caller to cordon every victim Node (all of them before the first eviction, so evicted pods
	// never land on another victim; Cordon is idempotent) and to evict their pods, then to requeue after
	// DrainRetryInterval. Under exclude-node-draining the caller does neither (DrainProgress.Excluded), and the message
	// says so.
	StepDrain DisruptionStep = "Drain"
	// StepWaitForVolumeDetach waits until the drained Nodes have no attached volumes.
	StepWaitForVolumeDetach DisruptionStep = "WaitForVolumeDetach"
	// StepHold waits without changing anything: every cordon, the token and status.disruption stay. Uncordoning here
	// would let pods return, and the drain would have to start over before IBM may remove the worker.
	StepHold DisruptionStep = "Hold"
	// StepCallIBM asks the caller to patch status.disruption to phase WaitingForIBM with ibmRequestTime = now, together
	// with status.lastDisruptionTime = now, and only after that patch succeeded to make the operation's one IBM call
	// (ReplaceWorker with update=true, ResizeWorkerPool or RemoveWorkerPool) and map its answer with
	// DisruptionCallOutcome. The patch first means a restart never sends the call twice within DisruptionSettleDelay.
	StepCallIBM DisruptionStep = "CallIBM"
	// StepWaitForIBM waits because the IBM call was sent less than DisruptionSettleDelay ago and IBM does not show the
	// removal yet; the call is never sent again in that time.
	StepWaitForIBM DisruptionStep = "WaitForIBM"
	// StepWaitForRemoval waits while IBM removes the victims, until they are deleted or gone from two listings
	// poolTransitionPoll apart. When status.disruption.ibmRequestTime is still zero (a pool deletion
	// whose pool IBM removes without a CAPIBM call, out of band or after a clusterctl move), the caller records it as
	// now, so that the stuck timer has a start. A replace or scale-down normally has it set by its own call:
	// DecideDisruptionStep waits out a removal IBM makes without one in the pre-call phase and then aborts
	// (RemovalProgress).
	StepWaitForRemoval DisruptionStep = "WaitForRemoval"
	// StepWaitForReplacement waits (replace only) until the replacement is deployed and normal and its Node has been
	// Ready for minReadySeconds.
	StepWaitForReplacement DisruptionStep = "WaitForReplacement"
	// StepComplete means that the removal is finished: release the token and clear status.disruption.
	StepComplete DisruptionStep = "Complete"
	// StepAbort means that the reason for the removal went away before the IBM call, or that IBM ignored the call for
	// WorkerStuckAfter after it went away: uncordon what CAPIBM changed (drain.Helper.Uncordon reverts only CAPIBM's
	// own changes), release the token, emit NodeUncordoned, then clear status.disruption.
	StepAbort DisruptionStep = "Abort"
	// StepHandOver means that the pool is being deleted while a replace or scale-down is wanted or in flight: the pool
	// deletion takes it over. Keep every cordon and the token. With Phase Draining (a removal in flight, in any phase),
	// record status.disruption as operation DeletePool, phase Draining and every worker of the pool as workerIDs,
	// keeping drainStartTime and ibmRequestTime: the pool deletion then cordons and drains every Node of the pool, its
	// pre-call gate (rule 1 of CheckDisruption) waits until IBM has settled a replace or resize it is still carrying
	// out, and its own IBM call keeps DisruptionSettleDelay from the earlier one. Restarting at Draining from every
	// phase means that the pool deletion never waits for a pool removal nobody requested, or for a replacement worker
	// of a pool that is going away. With Phase "" (nothing cordoned yet), decide again with operation DeletePool.
	StepHandOver DisruptionStep = "HandOver"
	// StepCorrectVictims (scale-down only) means that IBM's resize removes other workers than the predicted victims
	// (resizeWorkerPool cannot name its victims, and whether it follows IBM's documented order is not yet verified
	// against IBM Cloud): emit the Warning UnexpectedScaleDownVictim naming both sets, uncordon the predicted victims
	// IBM spares (drain.Helper.Uncordon reverts only CAPIBM's changes), and record the workers IBM removes as
	// status.disruption.workerIDs, keeping phase WaitingForRemoval and the token; the removal then waits for those
	// workers. Without this step a spared victim, never deleted, would hold the removal and the token for ever.
	StepCorrectVictims DisruptionStep = "CorrectVictims"
	// StepStop means that the Cluster is being deleted: make no IBM and no workload-cluster call and leave the Nodes as
	// they are. Every worker is going away, so a drain would only push pods onto Nodes that are deleted as well; CAPI
	// does not drain on Cluster deletion either.
	StepStop DisruptionStep = "Stop"
)

// DisruptionObservation is what the pool controller observed in this reconcile for one worker removal;
// DecideDisruptionStep reads nothing else. A false gate or a zero DisruptionCheckResult blocks, so a field the caller
// forgot to fill fails closed.
type DisruptionObservation struct {
	// Operation is the removal: Replace, ScaleDown or DeletePool. Required.
	Operation infrav1.WorkerDisruptionOperation

	// Phase is status.disruption.phase as read; "" when no removal is recorded.
	Phase infrav1.WorkerDisruptionPhase

	// ResumeFromNodeMarkers is true when Phase is "" but a victim Node carries this pool's drain-owner marker: a
	// clusterctl move cleared status mid-drain. The drain resumes instead of starting over at the gates, so its
	// drain-start-time, and with it the drain time limit, carries over; the caller rebuilds status.disruption from the
	// Node markers.
	ResumeFromNodeMarkers bool

	// Now is the caller's clock reading. Required.
	Now time.Time

	// IBMRequestTime is status.disruption.ibmRequestTime: when the IBM call was last attempted, or when the caller
	// first saw IBM removing the victims without a call (see StepWaitForRemoval); zero when neither happened. The
	// removal is aborted at once only while it is zero or LastCallRefused holds, and the stuck and replacement timers
	// count from it.
	IBMRequestTime time.Time

	// LastCallRefused is true when IBM refused the call of IBMRequestTime synchronously and shows no effect of it
	// (E1437, which was observed on IBM Cloud to be synchronous and side-effect free, or the 403 of an autoscaled
	// pool): IBM never acts on such a call, so the removal is as far as before the call. A replace or scale-down whose
	// trigger went away is then aborted at once, uncordoning and releasing the token, instead of waiting
	// WorkerStuckAfter for an IBM action that never comes, exactly as when the replicas go back up before the call.
	// False keeps that wait, so a caller that cannot tell fails closed.
	LastCallRefused bool

	// ClusterDeleting is true when the Cluster has a deletionTimestamp: no drain and no IBM pool call.
	ClusterDeleting bool

	// PoolDeleting is true when the IBMCloudManagedMachinePool has a deletionTimestamp.
	PoolDeleting bool

	// TriggerActive is true while the reason for the removal holds: for a replace, the victim is still listed and
	// still due; for a scale-down, the MachinePool still asks for fewer workers. A pool deletion never aborts, so it is
	// ignored for DeletePool.
	TriggerActive bool

	// Token is the state of the Cluster's disruption token for this pool (DisruptionToken.Status). The zero value
	// counts as TokenNotHeld.
	Token TokenState

	// WorkloadClusterConnected is true while ClusterCache has a connection to the workload cluster.
	WorkloadClusterConnected bool

	// Gates are the gates before the first cordon; read only while no removal is in flight.
	Gates DisruptionGates

	// Drain is the drain progress of the victims, observed in this reconcile in every phase before the IBM call.
	Drain DrainProgress

	// PreCallGate is CheckDisruption with SkipSettleDelay, evaluated right before the IBM call: a long drain can
	// outlive the first check.
	PreCallGate DisruptionCheckResult

	// IBM is what the latest IBM listing says about the victims.
	IBM RemovalProgress
}

// DisruptionGates are the gates a removal passes before its first cordon (the Gated phase, checked in this order).
type DisruptionGates struct {
	// ClusterStateAllows is PoolMutationAllowed for the IBM cluster state and the operation.
	ClusterStateAllows bool

	// MasterUpdating is true while IsMasterUpdatePending holds or IBM reports the state updating; it blocks a replace,
	// which would bring the worker back on a version that is about to change.
	MasterUpdating bool

	// Disruption is CheckDisruption for the operation's RemovedReady.
	Disruption DisruptionCheckResult

	// Autoscaled is true when IBM reports the pool autoscaled (replace and scale-down only).
	Autoscaled bool

	// AutobalanceEnabled is the victim zone's autobalanceEnabled (replace only): without auto-rebalance a replace
	// creates no replacement.
	AutobalanceEnabled bool

	// OthersAvailable is true when every worker of the pool other than the victim already is what a replace waits for
	// before it completes: deployed and healthy in the victim's zone, and with a Node Ready for minReadySeconds (replace
	// only). A replace waits for every worker of its pool, not only for its replacement, and IBM's listing does not tell
	// the replacement apart from a worker that was broken before; a replace started next to a broken worker (for example
	// one IBM failed to provision, which has no Node) would therefore wait for as long as that worker stays broken, holding
	// the Cluster's disruption token all along and with it every scale-down, replace and pool deletion of the Cluster.
	// Checked before the first cordon, the broken worker holds only the rollout of its own pool, without the token.
	OthersAvailable bool

	// UnavailableWorkers are the workers of the pool, sorted, that keep OthersAvailable false; the message names them.
	UnavailableWorkers []string

	// OtherDrainOwners are the drain-owner markers of other pools found on any Node of the workload cluster whose pool
	// still exists (DisruptionToken.DrainingPools). A drain half done before a clusterctl move, which does not move the
	// token, still holds the Cluster; the marker of a pool that is gone holds nothing.
	OtherDrainOwners []string
}

// DrainProgress is the state of the drain of every victim Node, observed in this reconcile.
type DrainProgress struct {
	// Excluded is true when the exclude-node-draining annotation is set: no cordon, no eviction.
	Excluded bool

	// TimedOut is true when nodeDrainTimeoutSeconds has passed since the drain started (drain.TimeoutExceeded).
	TimedOut bool

	// Done is true when the latest Drain of every victim Node left no pod that needs eviction or completion; a victim
	// without a Node counts as done. Only meaningful while the workload cluster is connected.
	Done bool

	// Blocked is true when a PodDisruptionBudget refused an eviction in this reconcile.
	Blocked bool

	// DetachExcluded is true when the exclude-wait-for-node-volume-detach annotation is set.
	DetachExcluded bool

	// DetachTimedOut is true when nodeVolumeDetachTimeoutSeconds has passed.
	DetachTimedOut bool

	// VolumesDetached is true when drain.Helper.VolumesDetached holds for every victim Node.
	VolumesDetached bool
}

// RemovalProgress is what the latest IBM listing says about the victims, the workers of status.disruption.workerIDs.
//
// When IBM removes the victims of a replace or scale-down before CAPIBM's own IBM call (IBMRequestTime zero: IBM
// autorecovery, a manual replace or removal, or a call whose status a clusterctl move lost), DecideDisruptionStep makes
// no call, holds the cordons while Removing and not Removed, and aborts once Removed, releasing the token. It does not
// wait for a replacement that may never come; rule 1 of CheckDisruption still holds back every pool while one
// provisions.
type RemovalProgress struct {
	// Removing is true when IBM is already removing every victim (WorkerBeingRemoved) or no longer lists it; the IBM
	// call is then skipped. For a pool deletion: the pool is deleting, deleted or gone.
	Removing bool

	// Removed is true when every victim is deleted, or absent from two listings poolTransitionPoll apart; for a pool
	// deletion, when the pool is gone or deleted. For a scale-down the victims are IBM's actual ones once
	// StepCorrectVictims replaced the predicted set.
	Removed bool

	// UnexpectedVictims is true for a scale-down when IBM's listing shows that the resize removes workers of the pool
	// other than the victims, so that a victim is spared: the caller compares the pool's workers IBM removes
	// (WorkerBeingRemoved) with status.disruption.workerIDs. It is read in phase WaitingForRemoval and leads to
	// StepCorrectVictims. Ignored for a replace and a pool deletion, whose victims IBM does not pick.
	UnexpectedVictims bool

	// ReplacementCreated is true when the victim's zone holds sizePerZone workers again that are not deleted (replace).
	ReplacementCreated bool

	// ReplacementReady is true when, in addition, every worker of that zone is deployed and normal and every pool
	// worker's Node has been Ready for minReadySeconds (replace).
	ReplacementReady bool
}

// DisruptionDecision is the step DecideDisruptionStep chose, with what the caller records.
type DisruptionDecision struct {
	// Step is what to do in this reconcile.
	Step DisruptionStep

	// Phase is the status.disruption.phase to record; "" means no status.disruption (before the first cordon, or
	// cleared). The caller keeps the other fields of status.disruption (drainStartTime, ibmRequestTime) when it records
	// a phase.
	Phase infrav1.WorkerDisruptionPhase

	// Reason is the reason for the flow's condition (WorkersUpToDate, WorkerPoolReady or Deleting); "" when the step
	// needs none.
	Reason DisruptionReason

	// DrainingReason is the reason for the Draining condition; "" means Draining=False/NotDraining.
	DrainingReason DisruptionReason

	// Message explains Reason; it is deterministic and free of IBM text, so a repeated reconcile writes nothing new.
	// While draining, the caller adds the drain library's DrainResult.ConditionMessage to the Draining condition.
	Message string

	// ReleaseToken is true when the caller releases the disruption token (DisruptionToken.Release) in this step.
	ReleaseToken bool
}

// CallsIBM reports whether the decision is the step that makes the IBM call. The rule that IBM is never asked to
// remove a worker whose Node may still run evictable pods is stated over it: the call happens only after the drain is
// done, timed out or excluded.
func (d DisruptionDecision) CallsIBM() bool {
	return d.Step == StepCallIBM
}

// DecideDisruptionStep decides the next step of a worker removal. It is pure: it reads only o, so every IBM call can be
// explained by what the caller observed.
//
// The phases and their exits:
//
//   - no phase (Gated): the gates of DisruptionGates in order, then the token. A failing gate is StepWait with its
//     reason and writes nothing; all pass and the token held is StepDrain with phase Draining.
//   - Draining: StepDrain until the drain is done, timed out or excluded, then phase WaitingForVolumeDetach.
//   - WaitingForVolumeDetach: until the volumes are detached or the wait timed out or is excluded, then the pre-call
//     gate.
//   - WaitingForIBM (pre-call gate and IBM call): the victims already being removed skip the call; a call sent less
//     than DisruptionSettleDelay ago is never repeated; a failing pre-call gate holds; else StepCallIBM.
//   - WaitingForRemoval: until IBM removed the victims, then WaitingForReplacement (replace) or StepComplete.
//   - WaitingForReplacement: until the replacement is ready, then StepComplete.
//
// Before the phase rules: a Cluster deletion stops everything; a pool deletion hands a replace or scale-down over to
// DeletePool, restarting at Draining; victims IBM removes before CAPIBM's call are waited out, then the removal aborts
// (RemovalProgress); a trigger that went away aborts before the IBM call or after a call IBM refused (LastCallRefused),
// and after any other call attempt only once IBM ignored it for WorkerStuckAfter (the call is never repeated for a
// removal nobody wants); and every phase needs the token. In WaitingForRemoval a mispredicted scale-down corrects its
// victims (StepCorrectVictims). Before the IBM call the drain and the volume detach are re-checked in every phase, so a
// pod that arrived after the drain sends the removal back to Draining: the IBM call is never decided while evictable
// pods remain, unless the drain timed out or is excluded.
//
// Parameters:
//   - o: the observation; see DisruptionObservation.
//
// It returns an error naming the field to fix when o is invalid.
func DecideDisruptionStep(o DisruptionObservation) (DisruptionDecision, error) {
	if err := o.validate(); err != nil {
		return DisruptionDecision{}, err
	}
	if o.ClusterDeleting {
		return DisruptionDecision{Step: StepStop, Phase: o.Phase,
			Message: "the Cluster is being deleted; IBM removes every worker with it, so nothing is drained"}, nil
	}
	phase := o.Phase
	if phase == "" && o.ResumeFromNodeMarkers {
		phase = infrav1.WorkerDisruptionPhaseDraining
	}
	if phase == "" {
		return o.decideGated(), nil
	}
	if d, decided := o.decideInterruption(phase); decided {
		return d, nil
	}
	switch phase {
	case infrav1.WorkerDisruptionPhaseDraining:
		return o.decideDraining(), nil
	case infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, infrav1.WorkerDisruptionPhaseWaitingForIBM:
		return o.decideBeforeCall(phase), nil
	case infrav1.WorkerDisruptionPhaseWaitingForRemoval:
		return o.decideRemoval(), nil
	default: // WaitingForReplacement; validate rejects every other value.
		return o.decideReplacement(), nil
	}
}

// validate returns an error naming the first invalid field of o.
func (o DisruptionObservation) validate() error {
	if err := validateOperation("DisruptionObservation.Operation", o.Operation); err != nil {
		return err
	}
	switch o.Phase {
	case "", infrav1.WorkerDisruptionPhaseDraining, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach,
		infrav1.WorkerDisruptionPhaseWaitingForIBM, infrav1.WorkerDisruptionPhaseWaitingForRemoval:
	case infrav1.WorkerDisruptionPhaseWaitingForReplacement:
		if o.Operation != infrav1.WorkerDisruptionOperationReplace {
			return fmt.Errorf("DisruptionObservation.Phase is %q for operation %q; only a Replace waits for a replacement",
				o.Phase, o.Operation)
		}
	default:
		return fmt.Errorf("DisruptionObservation.Phase is %q; want \"\" or a WorkerDisruptionPhase", o.Phase)
	}
	switch o.Token {
	case "", TokenNotHeld, TokenHeld, TokenHeldByOther:
	default:
		return fmt.Errorf("DisruptionObservation.Token is %q; want %q, %q or %q", o.Token, TokenNotHeld, TokenHeld, TokenHeldByOther)
	}
	if o.Now.IsZero() {
		return errors.New("DisruptionObservation.Now is not set; pass the controller clock's current time")
	}
	return nil
}

// decideGated applies the gates before the first cordon. Nothing is written before the token is held, so a failing gate
// costs nothing.
func (o DisruptionObservation) decideGated() DisruptionDecision {
	holdsToken := o.Token == TokenHeld
	switch {
	case o.PoolDeleting && o.Operation != infrav1.WorkerDisruptionOperationDeletePool:
		return DisruptionDecision{Step: StepHandOver, Message: "the pool is being deleted; its Nodes are drained before IBM removes the pool"}
	case o.Operation != infrav1.WorkerDisruptionOperationDeletePool && !o.TriggerActive:
		return DisruptionDecision{Step: StepNone, ReleaseToken: holdsToken}
	}
	if reason, message := o.failingGate(); reason != "" {
		return DisruptionDecision{Step: StepWait, Reason: reason, Message: message, ReleaseToken: holdsToken}
	}
	switch o.Token {
	case TokenHeld:
		return DisruptionDecision{Step: StepDrain, Phase: infrav1.WorkerDisruptionPhaseDraining,
			Reason: drainingReason(o.Operation), DrainingReason: ReasonDrainingNode, Message: o.firstDrainMessage()}
	case TokenHeldByOther:
		return DisruptionDecision{Step: StepWait, Reason: ReasonWaitingForDisruptionToken,
			Message: "another pool of the Cluster holds the disruption token; one worker removal runs at a time"}
	default:
		return DisruptionDecision{Step: StepAcquireToken, Message: "every gate passed; acquiring the disruption token"}
	}
}

// firstDrainMessage is the message of the step that starts the drain once every gate passed: what CAPIBM does to the
// victim Nodes, followed by the stuck workers the disruption guard no longer counts.
//
// Under machine.cluster.x-k8s.io/exclude-node-draining nothing is cordoned and nothing is evicted, as for a CAPI
// Machine, so the message says that and never says "cordoning". A worker that CheckDisruption let pass because it has
// been transitional for longer than WorkerStuckAfter is named here: the gates are read only before the first cordon, so
// this message is where the operator learns why a removal started while IBM was still changing a worker. Both parts are
// deterministic, so a repeated reconcile writes the same message and no status.
func (o DisruptionObservation) firstDrainMessage() string {
	message := "cordoning every victim Node, then evicting their pods"
	if o.Drain.Excluded {
		message = fmt.Sprintf("the drain is excluded (%s): CAPIBM leaves the victim Nodes schedulable and evicts no pod",
			clusterv1.ExcludeNodeDrainingAnnotation)
	}
	if gate := o.Gates.Disruption; gate.Allowed && len(gate.StuckWorkerIDs) > 0 && gate.Message != "" {
		message += "; " + gate.Message
	}
	return message
}

// failingGate returns the reason and message of the first gate that fails, or "" when all pass.
func (o DisruptionObservation) failingGate() (DisruptionReason, string) {
	g := o.Gates
	replace := o.Operation == infrav1.WorkerDisruptionOperationReplace
	switch {
	case !g.ClusterStateAllows:
		return ReasonWaitingForClusterState, "the IBM cluster state does not allow this pool operation"
	case replace && g.MasterUpdating:
		return ReasonWaitingForControlPlaneUpgrade, "IBM is updating the master; workers are replaced after it finished"
	case !g.Disruption.Allowed:
		if g.Disruption.Reason == "" {
			return ReasonWaitingForSettledCluster, "the disruption guards were not evaluated"
		}
		return g.Disruption.Reason, g.Disruption.Message
	case o.Operation != infrav1.WorkerDisruptionOperationDeletePool && g.Autoscaled:
		return ReasonExternallyAutoscaled, "IBM's cluster autoscaler owns this pool; CAPIBM neither resizes nor replaces its workers"
	case replace && !g.AutobalanceEnabled:
		return ReasonAutoRebalanceDisabled, "IBM auto-rebalance is disabled in the worker's zone, so a replace would " +
			"create no replacement; run `ibmcloud ks worker-pool rebalance` for this pool to enable it"
	case !o.WorkloadClusterConnected:
		return ReasonWaitingForWorkloadCluster, "the workload cluster is not connected; nothing is cordoned until it is"
	case replace && !g.OthersAvailable:
		return ReasonWorkersNotReady, othersUnavailableMessage(g.UnavailableWorkers)
	case len(g.OtherDrainOwners) > 0:
		return ReasonWaitingForDisruptionToken, fmt.Sprintf("Nodes are still drained by IBMCloudManagedMachinePool %s; "+
			"one worker removal runs at a time", strings.Join(g.OtherDrainOwners, ", "))
	}
	return "", ""
}

// othersUnavailableMessage is the message of the gate that holds a replace while another worker of its pool is not
// deployed, healthy and Ready for minReadySeconds (DisruptionGates.OthersAvailable). It names the workers, bounded by
// listWorkers, and says why the replace waits and what a human does about a worker that stays broken; a worker whose Node
// only soaks for minReadySeconds needs nothing and passes on its own. It is deterministic, so a repeated reconcile writes
// nothing new.
//
// Parameters:
//   - workers: DisruptionGates.UnavailableWorkers, sorted; empty when the caller could not tell which workers they are.
func othersUnavailableMessage(workers []string) string {
	why := "a replace starts only once every other worker of the pool is deployed, healthy and Ready for " +
		"spec.template.spec.minReadySeconds, because it waits for all of them before it completes"
	if len(workers) == 0 {
		return why + "; the other workers of the pool were not evaluated"
	}
	return fmt.Sprintf("%s; waiting for %s of the pool; repair or replace a worker that stays broken "+
		"(for example `ibmcloud ks worker replace`)", why, listWorkers(workers))
}

// decideInterruption applies the rules that come before the phase rules of an in-flight removal: hand-over to a pool
// deletion, a removal IBM carries out without CAPIBM's call, abort before the IBM call, and the token. It returns
// decided false when the phase rules apply.
//
// Parameters:
//   - phase: the effective phase (Draining when resuming from the Node markers).
func (o DisruptionObservation) decideInterruption(phase infrav1.WorkerDisruptionPhase) (DisruptionDecision, bool) {
	if o.PoolDeleting && o.Operation != infrav1.WorkerDisruptionOperationDeletePool {
		// Phase Draining in every phase, not the current one: recorded with operation DeletePool, a later phase would
		// wait for a pool removal nobody requested (WaitingForRemoval) or be invalid (WaitingForReplacement). The pool
		// deletion's own drain and pre-call gate then cover every Node and IBM's unfinished work (StepHandOver).
		return DisruptionDecision{Step: StepHandOver, Phase: infrav1.WorkerDisruptionPhaseDraining,
			Message: "the pool is being deleted; the removal restarts as a pool deletion at Draining, keeping every cordon"}, true
	}
	if d, decided := o.decideRemovedOutOfBand(phase); decided {
		return d, true
	}
	if d, decided := o.decideTriggerGone(phase); decided {
		return d, true
	}
	switch o.Token {
	case TokenHeld:
		return DisruptionDecision{}, false
	case TokenHeldByOther:
		return DisruptionDecision{Step: StepHold, Phase: phase, Reason: ReasonWaitingForDisruptionToken,
			Message: "another pool holds the disruption token; the removal in flight waits with its Nodes cordoned"}, true
	default:
		return DisruptionDecision{Step: StepAcquireToken, Phase: phase,
			Message: "re-acquiring the disruption token of the removal in flight"}, true
	}
}

// decideRemovedOutOfBand handles the victims of a replace or scale-down that IBM removes before CAPIBM's own IBM call
// (IBMRequestTime zero, so CAPIBM never sent it: the status patch comes first). CAPIBM makes no call, keeps the cordons
// while IBM removes them, and aborts once they are removed, which releases the token. Following such a removal to its
// end instead would wait for a replacement that a manual removal never creates, holding the token of the Cluster for
// ever. A pool deletion follows IBM instead: it has no replacement to wait for. It returns decided false when the rule
// does not apply.
//
// Parameters:
//   - phase: the effective phase.
func (o DisruptionObservation) decideRemovedOutOfBand(phase infrav1.WorkerDisruptionPhase) (DisruptionDecision, bool) {
	if o.Operation == infrav1.WorkerDisruptionOperationDeletePool || !isPreCallPhase(phase) || !o.IBMRequestTime.IsZero() {
		return DisruptionDecision{}, false
	}
	switch {
	case o.IBM.Removed:
		return DisruptionDecision{Step: StepAbort, Phase: phase, ReleaseToken: true,
			Message: "IBM removed the workers without a CAPIBM call (IBM autorecovery, or a manual replace or removal); " +
				"releasing the disruption token"}, true
	case o.IBM.Removing:
		return DisruptionDecision{Step: StepHold, Phase: phase, Reason: removalReason(o.Operation),
			Message: "IBM removes the workers without a CAPIBM call; no call is made, and the Nodes stay cordoned until " +
				"IBM has removed them"}, true
	}
	return DisruptionDecision{}, false
}

// decideTriggerGone handles a replace or scale-down whose reason went away: before the IBM call it aborts, and so it
// does after a call IBM refused synchronously (LastCallRefused), which IBM never acts on. After a call attempt that IBM
// shows no sign of, it neither repeats the call (nobody wants the removal any more) nor uncordons at once (IBM may
// still act on it, and the Nodes must then be drained): it waits until WorkerStuckAfter after the attempt and aborts
// only then. Once IBM removes the victims, the removal is followed to its end. It returns decided false when the
// trigger is active or the phase rules apply.
//
// Parameters:
//   - phase: the effective phase.
func (o DisruptionObservation) decideTriggerGone(phase infrav1.WorkerDisruptionPhase) (DisruptionDecision, bool) {
	if o.TriggerActive || o.Operation == infrav1.WorkerDisruptionOperationDeletePool || !isPreCallPhase(phase) || o.IBM.Removing {
		return DisruptionDecision{}, false
	}
	if o.IBMRequestTime.IsZero() {
		return DisruptionDecision{Step: StepAbort, Phase: phase, ReleaseToken: true,
			Message: "the reason for the removal went away before the IBM call; uncordoning what CAPIBM cordoned"}, true
	}
	if o.LastCallRefused {
		return DisruptionDecision{Step: StepAbort, Phase: phase, ReleaseToken: true,
			Message: fmt.Sprintf("the reason for the removal went away after IBM refused the call of %s; uncordoning "+
				"what CAPIBM cordoned", o.IBMRequestTime.UTC().Format(time.RFC3339))}, true
	}
	if abortAt := o.IBMRequestTime.Add(WorkerStuckAfter); o.Now.Before(abortAt) {
		return DisruptionDecision{Step: StepWaitForIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
			Reason: removalReason(o.Operation), Message: fmt.Sprintf("the reason for the removal went away after the IBM "+
				"call of %s; no second call is made, and the Nodes stay cordoned until IBM acts or %s passes",
				o.IBMRequestTime.UTC().Format(time.RFC3339), abortAt.UTC().Format(time.RFC3339))}, true
	}
	return DisruptionDecision{Step: StepAbort, Phase: phase, ReleaseToken: true,
		Message: fmt.Sprintf("IBM did not act on the call of %s within %d minutes and the reason for the removal went "+
			"away; uncordoning what CAPIBM cordoned", o.IBMRequestTime.UTC().Format(time.RFC3339),
			int(WorkerStuckAfter.Minutes()))}, true
}

// isPreCallPhase reports whether phase comes before IBM's removal is under way (Draining, WaitingForVolumeDetach,
// WaitingForIBM), the only phases in which a removal may be aborted.
//
// Parameters:
//   - phase: the effective phase.
func isPreCallPhase(phase infrav1.WorkerDisruptionPhase) bool {
	return phase == infrav1.WorkerDisruptionPhaseDraining || phase == infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach ||
		phase == infrav1.WorkerDisruptionPhaseWaitingForIBM
}

// decideDraining keeps draining until the drain is finished, then moves to the volume detach wait.
func (o DisruptionObservation) decideDraining() DisruptionDecision {
	phase := infrav1.WorkerDisruptionPhaseDraining
	if o.drainFinished() {
		return DisruptionDecision{Step: StepWaitForVolumeDetach, Phase: infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach,
			Reason: drainingReason(o.Operation), DrainingReason: ReasonWaitingForVolumeDetach,
			Message: "the Nodes are drained; waiting for their volumes to detach"}
	}
	if !o.WorkloadClusterConnected {
		return o.waitForWorkloadCluster(phase)
	}
	d := DisruptionDecision{Step: StepDrain, Phase: phase, Reason: drainingReason(o.Operation),
		DrainingReason: ReasonDrainingNode, Message: "evicting the pods of the victim Nodes"}
	if o.Drain.Blocked {
		d.DrainingReason = ReasonDrainBlocked
		d.Message = "a PodDisruptionBudget refuses an eviction; the Nodes stay cordoned and evictions are retried, never forced"
	}
	return d
}

// decideBeforeCall decides the phases before the IBM call (WaitingForVolumeDetach, then the pre-call gate and the call
// of WaitingForIBM): it skips the call when IBM already removes the victims, never repeats a call within
// DisruptionSettleDelay of the last attempt, re-checks that the drain is still finished and the volumes are detached,
// and then applies the pre-call gate.
//
// Parameters:
//   - phase: WaitingForVolumeDetach or WaitingForIBM.
func (o DisruptionObservation) decideBeforeCall(phase infrav1.WorkerDisruptionPhase) DisruptionDecision {
	if o.IBM.Removing {
		return o.waitForRemoval("IBM removes the workers; no further IBM call is needed")
	}
	if !o.IBMRequestTime.IsZero() {
		if repeatAt := o.IBMRequestTime.Add(DisruptionSettleDelay); o.Now.Before(repeatAt) {
			return DisruptionDecision{Step: StepWaitForIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
				Reason: removalReason(o.Operation), Message: fmt.Sprintf("the IBM call was sent at %s; waiting for "+
					"IBM's worker list to show it, without a second call before %s",
					o.IBMRequestTime.UTC().Format(time.RFC3339), repeatAt.UTC().Format(time.RFC3339))}
		}
	}
	if !o.drainFinished() {
		if !o.WorkloadClusterConnected {
			return o.waitForWorkloadCluster(phase)
		}
		return DisruptionDecision{Step: StepDrain, Phase: infrav1.WorkerDisruptionPhaseDraining,
			Reason: drainingReason(o.Operation), DrainingReason: ReasonDrainingNode,
			Message: "pods arrived on a drained Node; draining again before the IBM call"}
	}
	if !o.detachFinished() {
		if !o.WorkloadClusterConnected {
			return o.waitForWorkloadCluster(phase)
		}
		return DisruptionDecision{Step: StepWaitForVolumeDetach, Phase: infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach,
			Reason: drainingReason(o.Operation), DrainingReason: ReasonWaitingForVolumeDetach,
			Message: "waiting for the volumes of the drained Nodes to detach"}
	}
	if !o.PreCallGate.Allowed {
		reason, message := o.PreCallGate.Reason, o.PreCallGate.Message
		if reason == "" {
			reason, message = ReasonWaitingForSettledCluster, "the pre-call disruption guards were not evaluated"
		}
		return DisruptionDecision{Step: StepHold, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM, Reason: reason,
			Message: message + "; the drained Nodes stay cordoned"}
	}
	return DisruptionDecision{Step: StepCallIBM, Phase: infrav1.WorkerDisruptionPhaseWaitingForIBM,
		Reason: removalReason(o.Operation), Message: "the Nodes are drained; asking IBM to remove the workers"}
}

// decideRemoval waits until IBM removed the victims. A scale-down whose victims IBM picked differently first corrects
// its victim set (StepCorrectVictims): the spared predicted victims would never be deleted.
func (o DisruptionObservation) decideRemoval() DisruptionDecision {
	if o.Operation == infrav1.WorkerDisruptionOperationScaleDown && o.IBM.UnexpectedVictims {
		return DisruptionDecision{Step: StepCorrectVictims, Phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval,
			Reason: ReasonResizing, Message: "IBM's resize removes other workers than the predicted victims; the spared " +
				"ones are uncordoned and the removal waits for the workers IBM removes"}
	}
	if o.IBM.Removed {
		if o.Operation == infrav1.WorkerDisruptionOperationReplace {
			return DisruptionDecision{Step: StepWaitForReplacement, Phase: infrav1.WorkerDisruptionPhaseWaitingForReplacement,
				Reason: ReasonWaitingForReplacement, Message: "IBM removed the worker; waiting for its replacement"}
		}
		return DisruptionDecision{Step: StepComplete, ReleaseToken: true, Message: "IBM removed the workers"}
	}
	if !o.IBMRequestTime.IsZero() && o.Now.Sub(o.IBMRequestTime) > WorkerStuckAfter {
		return DisruptionDecision{Step: StepWaitForRemoval, Phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval,
			Reason: ReasonWorkerOperationStuck, Message: fmt.Sprintf("IBM has not removed the workers %d minutes after "+
				"the call of %s; still waiting", int(WorkerStuckAfter.Minutes()), o.IBMRequestTime.UTC().Format(time.RFC3339))}
	}
	return o.waitForRemoval("IBM removes the workers")
}

// decideReplacement waits until the replacement worker is ready (replace only).
func (o DisruptionObservation) decideReplacement() DisruptionDecision {
	phase := infrav1.WorkerDisruptionPhaseWaitingForReplacement
	if o.IBM.ReplacementReady {
		return DisruptionDecision{Step: StepComplete, ReleaseToken: true, Message: "the replacement worker is ready"}
	}
	if !o.IBM.ReplacementCreated && !o.IBMRequestTime.IsZero() && !o.Now.Before(o.IBMRequestTime.Add(ReplacementNotCreatedAfter)) {
		return DisruptionDecision{Step: StepWaitForReplacement, Phase: phase, Reason: ReasonReplacementNotCreated,
			Message: fmt.Sprintf("IBM created no replacement worker within %d minutes of the replace; check the zone's "+
				"auto-rebalance and capacity, and run `ibmcloud ks worker-pool rebalance` for this pool",
				int(ReplacementNotCreatedAfter.Minutes()))}
	}
	return DisruptionDecision{Step: StepWaitForReplacement, Phase: phase, Reason: ReasonWaitingForReplacement,
		Message: "waiting for the replacement worker to be deployed and its Node Ready"}
}

// waitForRemoval returns the decision to wait in phase WaitingForRemoval.
//
// Parameters:
//   - message: the condition message.
func (o DisruptionObservation) waitForRemoval(message string) DisruptionDecision {
	return DisruptionDecision{Step: StepWaitForRemoval, Phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval,
		Reason: removalReason(o.Operation), Message: message}
}

// waitForWorkloadCluster returns the decision to hold in phase while the workload cluster is not connected: nothing can
// be drained or checked, so no IBM call is made while evictable pods may remain, until a set time limit runs out.
//
// Parameters:
//   - phase: the phase to keep.
func (o DisruptionObservation) waitForWorkloadCluster(phase infrav1.WorkerDisruptionPhase) DisruptionDecision {
	return DisruptionDecision{Step: StepHold, Phase: phase, Reason: ReasonWaitingForWorkloadCluster,
		DrainingReason: ReasonWaitingForWorkloadCluster,
		Message:        "the workload cluster is not connected; the Nodes stay cordoned and no IBM call is made"}
}

// drainFinished reports whether no evictable pod can remain on a victim: the drain is excluded, timed out, or done
// as observed over a connected workload cluster. Only then may IBM be asked to remove the victim.
func (o DisruptionObservation) drainFinished() bool {
	return o.Drain.Excluded || o.Drain.TimedOut || (o.WorkloadClusterConnected && o.Drain.Done)
}

// detachFinished reports whether the volume detach wait is over: excluded, timed out, or every volume detached as
// observed over a connected workload cluster.
func (o DisruptionObservation) detachFinished() bool {
	return o.Drain.DetachExcluded || o.Drain.DetachTimedOut || (o.WorkloadClusterConnected && o.Drain.VolumesDetached)
}

// DisruptionCallResult is DisruptionCallOutcome's answer.
type DisruptionCallResult struct {
	// Handled is true when Decision is the next step of the removal. When false, the caller applies its common error
	// handling for Class (Transient, RateLimited, Unauthorized, PermissionDenied, BadRequest) and keeps phase
	// WaitingForIBM; the next reconcile re-lists the workers before any retry, which DecideDisruptionStep never makes
	// within DisruptionSettleDelay of the attempt.
	Handled bool

	// Decision is the next step when Handled; otherwise a hold in phase WaitingForIBM with the flow's reason.
	Decision DisruptionDecision

	// Class is what IBM's answer means for this removal call: kubernetesservice.Classify, except that E1437 is
	// ClassClusterMinimum and a 409 is ClassOperationInProgress on every removal call (Classify knows these rows only
	// for some operations), and that an autoscaled-pool refusal of a pool IBM does not report autoscaled is
	// ClassPermissionDenied.
	Class kubernetesservice.ErrorClass
}

// DisruptionCallOutcome maps IBM's answer to the call of StepCallIBM to the next decision: success and 404 (the worker
// or pool is already gone) go on to WaitingForRemoval; E1437 holds with the Nodes cordoned (WaitingForClusterMinimum);
// a 409 (an operation in progress) waits for IBM without a second call within DisruptionSettleDelay; an autoscaled-pool
// refusal holds as ExternallyAutoscaled when the pool shows autoscaleEnabled. Every other answer is left to the
// caller's common error handling (DisruptionCallResult.Handled).
//
// Parameters:
//   - call: the IBM call the caller made for the removal: kubernetesservice.OpReplaceWorker (Replace),
//     OpRemoveWorkerPool (DeletePool) or OpResizeWorkerPool (ScaleDown).
//   - err: the error the call returned; nil for success.
//   - autoscaleEnabled: the pool's autoscaleEnabled in IBM's latest pool listing (kubernetesservice.WorkerPool).
//
// It returns an error naming the call when call is not one of the removal calls.
func DisruptionCallOutcome(call kubernetesservice.Operation, err error, autoscaleEnabled bool) (DisruptionCallResult, error) {
	reason, verr := removalCallReason(call)
	if verr != nil {
		return DisruptionCallResult{}, verr
	}
	class := removalCallClass(call, err, autoscaleEnabled)
	waitingForIBM := infrav1.WorkerDisruptionPhaseWaitingForIBM
	result := DisruptionCallResult{Handled: true, Class: class}
	switch class {
	case kubernetesservice.ClassNone, kubernetesservice.ClassNotFound:
		result.Decision = DisruptionDecision{Step: StepWaitForRemoval, Phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval,
			Reason: reason, Message: "IBM accepted the request; waiting for it to remove the workers"}
	case kubernetesservice.ClassClusterMinimum:
		result.Decision = DisruptionDecision{Step: StepHold, Phase: waitingForIBM, Reason: ReasonWaitingForClusterMinimum,
			Message: fmt.Sprintf("IBM refused the removal because the cluster would fall below its minimum workers (%s); "+
				"the drained Nodes stay cordoned", ClusterMinimumErrorCode)}
	case kubernetesservice.ClassOperationInProgress:
		result.Decision = DisruptionDecision{Step: StepWaitForIBM, Phase: waitingForIBM, Reason: reason,
			Message: "IBM reports an operation in progress on the workers; waiting before the next attempt"}
	case kubernetesservice.ClassAutoscaled:
		result.Decision = DisruptionDecision{Step: StepHold, Phase: waitingForIBM, Reason: ReasonExternallyAutoscaled,
			Message: "IBM's cluster autoscaler owns this pool; the drained Nodes stay cordoned"}
	default:
		result.Handled = false
		result.Decision = DisruptionDecision{Step: StepHold, Phase: waitingForIBM, Reason: reason}
	}
	return result, nil
}

// removalCallReason returns the flow reason while IBM carries out call, or an error when call is not a removal call.
//
// Parameters:
//   - call: the IBM call of StepCallIBM.
func removalCallReason(call kubernetesservice.Operation) (DisruptionReason, error) {
	switch call {
	case kubernetesservice.OpReplaceWorker:
		return ReasonReplacingWorker, nil
	case kubernetesservice.OpRemoveWorkerPool:
		return ReasonRemovingWorkerPool, nil
	case kubernetesservice.OpResizeWorkerPool:
		return ReasonResizing, nil
	default:
		return "", fmt.Errorf("DisruptionCallOutcome call is %q; want the IBM call of StepCallIBM: %q, %q or %q", call,
			kubernetesservice.OpReplaceWorker, kubernetesservice.OpRemoveWorkerPool, kubernetesservice.OpResizeWorkerPool)
	}
}

// removalCallClass classifies IBM's answer to a removal call. It adds the two answers that kubernetesservice.Classify
// maps for some operations only (E1437 and a 409), so that the caller never treats them as a terminal BadRequest or an
// Unknown error, and reports an autoscaled-pool refusal as ExternallyAutoscaled only for a pool IBM reports autoscaled.
//
// Parameters:
//   - call: the IBM call.
//   - err: its error; nil for success.
//   - autoscaleEnabled: the pool's autoscaleEnabled.
func removalCallClass(call kubernetesservice.Operation, err error, autoscaleEnabled bool) kubernetesservice.ErrorClass {
	var apiErr *kubernetesservice.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.Code == ClusterMinimumErrorCode:
			// IBM's minimum applies to every removal, a replace included, while Classify maps E1437 only for
			// RemoveWorkerPool and ResizeWorkerPool; ReplaceWorker would be a terminal BadRequest.
			return kubernetesservice.ClassClusterMinimum
		case apiErr.StatusCode == http.StatusConflict:
			// A 409 on a removal call means that an operation is in progress: wait and look again. Classify knows a 409
			// only for CreateWorkerPool and UpdateMaster, and IBM's error code of a 409 on a removal is not yet
			// verified against IBM Cloud, so the status decides.
			return kubernetesservice.ClassOperationInProgress
		}
	}
	class := kubernetesservice.Classify(call, err)
	if class == kubernetesservice.ClassAutoscaled && !autoscaleEnabled {
		// ExternallyAutoscaled only when IBM reports autoscaleEnabled. The Gated phase already refuses an autoscaled
		// replace or scale-down, so this 403 most likely means that the credentials lack the Kubernetes Service role
		// (Administrator platform role and Manager service role).
		return kubernetesservice.ClassPermissionDenied
	}
	return class
}

// drainingReason returns the flow reason while the victims drain: DrainingNodes for a pool deletion (Deleting
// condition), DrainingNode otherwise.
//
// Parameters:
//   - op: the removal.
func drainingReason(op infrav1.WorkerDisruptionOperation) DisruptionReason {
	if op == infrav1.WorkerDisruptionOperationDeletePool {
		return ReasonDrainingNodes
	}
	return ReasonDrainingNode
}

// removalReason returns the flow reason while IBM removes the victims: ReplacingWorker, Resizing or
// RemovingWorkerPool.
//
// Parameters:
//   - op: the removal.
func removalReason(op infrav1.WorkerDisruptionOperation) DisruptionReason {
	switch op {
	case infrav1.WorkerDisruptionOperationReplace:
		return ReasonReplacingWorker
	case infrav1.WorkerDisruptionOperationScaleDown:
		return ReasonResizing
	default:
		return ReasonRemovingWorkerPool
	}
}
