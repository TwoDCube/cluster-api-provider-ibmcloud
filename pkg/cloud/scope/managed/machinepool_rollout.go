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
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/index"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file is the worker rollout of an IBMCloudManagedMachinePool: workers follow the control plane minor, take IBM's
// newer fix packs under the Automatic policy, and are recycled once spec.rollout.after has passed (CAPI
// MachineDeployment semantics). IBM's only way to update a VPC worker is to replace it (IBM deletes the old worker
// before it creates the new one, as observed on IBM Cloud), so each due worker is one Replace removal of the state
// machine that DecideDisruptionStep drives: the gates, the drain of its Node, the pre-call gate, ReplaceWorker with
// update=true, the wait for IBM's removal and for the replacement, one worker per Cluster at a time.
// ClassifyWorkerUpdate and PickReplacement decide which worker is due and which goes next; the shared removal runner
// (removal_runner.go) carries out the steps every removal shares. This file only gathers the inputs those pure
// functions read and reports the rollout on WorkersUpToDate. A worker's Node is found by its provider ID through
// ClusterCache's NodeProviderIDIndex, and only when the decision needs one: the Node's creation time for
// spec.rollout.after, its Ready time for minReadySeconds.

const (
	// EventWorkerReplaceRequested is the Normal event emitted when IBM accepted the replace of a worker.
	EventWorkerReplaceRequested = "WorkerReplaceRequested"

	// maxRolloutDecisions bounds the decisions of one rollout reconcile: acquiring the token asks for one more
	// decision, and it cannot repeat.
	maxRolloutDecisions = 2
)

// RolloutInput is the input of SummarizeRollout: one worker listing and the pool's rollout settings.
type RolloutInput struct {
	// Workers are every worker of the cluster from one ListWorkers call (all pools, showDeleted=true).
	Workers []kubernetesservice.Worker

	// PoolID is the IBM pool ID, status.poolID; only its workers count. Required.
	PoolID string

	// SizePerZone is IBM's workerCount of the pool, and Zones the number of zones a resize of the pool changes; their
	// product is the pool's size, the "of n" of the progress (RolloutSummary.Workers). Neither may be negative.
	SizePerZone int64
	Zones       int

	// MasterMinor is the master's minor, as for UpdateAvailability.
	MasterMinor ibmversion.Minor

	// Policy is the pool's spec.workerUpdates.policy; "" means Manual.
	Policy infrav1.WorkerUpdatePolicy

	// RolloutAfter is the pool's spec.rollout.after; the zero time means unset.
	RolloutAfter time.Time

	// NodeCreated maps a worker ID to the creationTimestamp of its Node. A worker without an entry has no Node.
	NodeCreated map[string]time.Time

	// Now is the controller's clock reading. Required.
	Now time.Time
}

// RolloutSummary is SummarizeRollout's answer: what the WorkersUpToDate condition reports and which worker goes next.
type RolloutSummary struct {
	// Workers is the pool's size for the progress: the larger of IBM's size (SizePerZone × Zones) and the workers of the
	// pool that IBM is not removing. A replaced worker that IBM is removing has handed its place to the replacement, so
	// the count stays the same while IBM swaps them.
	Workers int

	// Due counts the workers of the pool that are due (ClassifyWorkerUpdate) and that IBM is not removing.
	Due int

	// PatchAvailable counts the workers of the pool IBM offers a newer fix pack of their minor (UpdateAvailability
	// PatchAvailable), due or not.
	PatchAvailable int

	// FixPack is the newest kubeVersion.target of those workers; "" when there are none.
	FixPack string

	// Next is the worker PickReplacement replaces next; HasNext is false when no worker is due.
	Next    ReplacementPick
	HasNext bool
}

// Replaced is the "replaced k of n" count of the progress: the pool's workers that are no longer due.
func (r RolloutSummary) Replaced() int {
	return max(r.Workers-r.Due, 0)
}

// SummarizeRollout summarizes the rollout of one pool for WorkersUpToDate. It is pure: it classifies every worker of
// the pool with ClassifyWorkerUpdate, whose class precedence it keeps (the reported class is the first match, but a
// worker is due when any class makes it due), and asks PickReplacement for the next worker, so the counts and the
// pick never disagree on what is due. A worker that IBM is removing (WorkerBeingRemoved) is no candidate, as for
// PickReplacement.
//
// Parameters:
//   - in: the workers and the pool's rollout settings; see RolloutInput.
//
// It returns an error naming the field to fix when in is invalid.
func SummarizeRollout(in RolloutInput) (RolloutSummary, error) {
	if in.SizePerZone < 0 || in.Zones < 0 {
		return RolloutSummary{}, fmt.Errorf("RolloutInput has SizePerZone %d and Zones %d; want IBM's workerCount and the "+
			"pool's zones, neither negative", in.SizePerZone, in.Zones)
	}
	pick, found, err := PickReplacement(ReplacementCandidates{Workers: in.Workers, PoolID: in.PoolID, MasterMinor: in.MasterMinor,
		Policy: in.Policy, RolloutAfter: in.RolloutAfter, NodeCreated: in.NodeCreated, Now: in.Now})
	if err != nil {
		return RolloutSummary{}, err
	}
	summary := RolloutSummary{Next: pick, HasNext: found}
	listed := 0
	var fixPack ibmversion.Version
	for _, w := range in.Workers {
		if w.PoolID != in.PoolID || WorkerBeingRemoved(w) {
			continue
		}
		listed++
		decision, err := ClassifyWorkerUpdate(w, in.MasterMinor, in.Policy, in.RolloutAfter, in.NodeCreated[w.ID], in.Now)
		if err != nil {
			return RolloutSummary{}, err
		}
		if decision.Due {
			summary.Due++
		}
		if UpdateAvailability(w, in.MasterMinor) != WorkerUpdatePatchAvailable {
			continue
		}
		summary.PatchAvailable++
		// WorkerHasNewerFixPack parsed the target already.
		if target, err := ibmversion.Parse(w.KubeVersionTarget); err == nil && (summary.FixPack == "" || compareIBMVersions(target, fixPack) > 0) {
			fixPack, summary.FixPack = target, target.String()
		}
	}
	summary.Workers = max(listed, int(in.SizePerZone)*in.Zones)
	return summary, nil
}

// ReplaceProgressInput is the input of ReplaceProgress: the latest IBM listing, the Nodes, and the worker being
// replaced.
type ReplaceProgressInput struct {
	// Workers are every worker of the cluster from one ListWorkers call (all pools, showDeleted=true).
	Workers []kubernetesservice.Worker

	// PoolID is the IBM pool ID, status.poolID. Required.
	PoolID string

	// Victim is the worker being replaced, status.disruption.workerIDs[0]. Required.
	Victim string

	// SizePerZone is IBM's workerCount of the pool: the victim's zone is back at full size once it holds that many
	// workers other than the victim. Zones is the number of zones of the pool; the whole pool must be back at
	// SizePerZone × Zones when IBM no longer lists the victim, so that its zone is not known.
	SizePerZone int64
	Zones       int

	// StillPublished is true while the victim's provider ID is in spec.providerIDList after this reconcile's
	// BuildProviderIDList: it leaves the list once IBM lists the worker deleted or two listings poolTransitionPoll apart
	// omit it, which is when its removal counts as complete.
	StillPublished bool

	// NodeReadySince maps a worker ID to the time its Node became Ready (nodeReadySince); a worker without an entry has
	// no Node that is Ready.
	NodeReadySince map[string]time.Time

	// MinReadySeconds is the MachinePool's spec.template.spec.minReadySeconds: how long every Node of the pool must have
	// been Ready before the next worker is touched. Must not be negative.
	MinReadySeconds int32

	// Now is the controller's clock reading. Required.
	Now time.Time
}

// ReplaceProgressResult is ReplaceProgress's answer.
type ReplaceProgressResult struct {
	// Progress is DisruptionObservation.IBM for the replace.
	Progress RemovalProgress

	// ReadyAt is when the last Node of the pool has been Ready for minReadySeconds, while that is still ahead; zero
	// otherwise. The caller rechecks then instead of only at its poll interval.
	ReadyAt time.Time

	// Waiting are the workers of the pool, sorted, that keep ReplacementReady false: not deployed and normal (in the
	// victim's zone), or without a Node that has been Ready for minReadySeconds. The condition message names them.
	Waiting []string
}

// ReplaceProgress says how far IBM has got with the replace of one worker (the phases WaitingForRemoval and
// WaitingForReplacement). It is pure.
//
// Progress.Removing holds once IBM removes the victim (WorkerBeingRemoved) or no longer lists it; Progress.Removed once
// the victim is no longer published (StillPublished). The replacement is created once the victim's zone holds
// SizePerZone workers of the pool other than the victim that IBM does not list as deleted, and ready once, in
// addition, each of them is deployed and normal and every worker of the pool has a Node that has been Ready for
// MinReadySeconds: the next worker starts only after that. When IBM no longer lists the victim, its zone is not known
// and the whole pool must be back at its size.
//
// Parameters:
//   - in: the observation; see ReplaceProgressInput.
//
// It returns an error naming the field to fix when in is invalid.
func ReplaceProgress(in ReplaceProgressInput) (ReplaceProgressResult, error) {
	switch {
	case in.PoolID == "" || in.Victim == "":
		return ReplaceProgressResult{}, errors.New("ReplaceProgressInput needs PoolID and Victim; pass status.poolID and " +
			"status.disruption.workerIDs[0]")
	case in.Now.IsZero():
		return ReplaceProgressResult{}, errors.New("ReplaceProgressInput.Now is not set; pass the controller clock's current time")
	case in.MinReadySeconds < 0 || in.SizePerZone < 0 || in.Zones < 0:
		return ReplaceProgressResult{}, fmt.Errorf("ReplaceProgressInput has MinReadySeconds %d, SizePerZone %d and Zones %d; "+
			"none may be negative", in.MinReadySeconds, in.SizePerZone, in.Zones)
	}
	victim, listed := workerByID(in.Workers, in.Victim)
	result := ReplaceProgressResult{Progress: RemovalProgress{
		Removing: !listed || WorkerBeingRemoved(victim),
		Removed:  !in.StillPublished,
	}}
	inZone, inPool := 0, 0
	for _, w := range in.Workers {
		if w.PoolID != in.PoolID || w.ID == in.Victim || w.ActualState == WorkerStateDeleted {
			continue
		}
		inPool++
		if listed && w.Zone == victim.Zone {
			inZone++
			if !IsWorkerReady(w) {
				result.Waiting = append(result.Waiting, w.ID)
			}
		}
		result.addNodeWait(w.ID, in)
	}
	if listed {
		result.Progress.ReplacementCreated = int64(inZone) >= in.SizePerZone
	} else {
		result.Progress.ReplacementCreated = int64(inPool) >= in.SizePerZone*int64(in.Zones)
	}
	result.Waiting = slices.Compact(slices.Sorted(slices.Values(result.Waiting)))
	result.Progress.ReplacementReady = result.Progress.ReplacementCreated && len(result.Waiting) == 0
	return result, nil
}

// addNodeWait records the worker as waiting while its Node is not Ready or has been Ready for less than
// MinReadySeconds, and moves ReadyAt to the end of that soak.
//
// Parameters:
//   - id is the worker ID.
//   - in is the validated input.
func (r *ReplaceProgressResult) addNodeWait(id string, in ReplaceProgressInput) {
	since, ready := in.NodeReadySince[id]
	if !ready {
		r.Waiting = append(r.Waiting, id)
		return
	}
	readyAt := since.Add(time.Duration(in.MinReadySeconds) * time.Second)
	if in.Now.Before(readyAt) {
		r.Waiting = append(r.Waiting, id)
		if readyAt.After(r.ReadyAt) {
			r.ReadyAt = readyAt
		}
	}
}

// workerByID returns the worker with the ID, and whether the listing has it.
//
// Parameters:
//   - workers are the listed workers.
//   - id is the worker ID.
func workerByID(workers []kubernetesservice.Worker, id string) (kubernetesservice.Worker, bool) {
	for _, w := range workers {
		if w.ID == id {
			return w, true
		}
	}
	return kubernetesservice.Worker{}, false
}

// MasterUpdateHoldsWorkers reports whether a master update holds the worker rollout (the master is pending or
// updating, an IBM-driven master patch included: WaitingForControlPlaneUpgrade), with the message that says why. A
// worker replaced now would come back on a version that is about to change. It is pure and reads only the control
// plane's status, so the pool makes no GetCluster call for it: IBM's state and versions as the control plane last
// observed them (IsMasterUpdatePending on those values; status.distributionVersion carries no " (pending)" marker, so
// a pending update shows through the state or the target version), and the control plane's RollingOut condition,
// which is True from CAPIBM's own UpdateMaster request until IBM shows the update or MasterUpdatePendingGrace has
// passed. An unknown master version holds the rollout too: the master minor the workers follow is not known yet (fail
// safe).
//
// Parameters:
//   - cp is the Cluster's IBMCloudManagedControlPlane.
func MasterUpdateHoldsWorkers(cp *controlplanev1.IBMCloudManagedControlPlane) (bool, string) {
	status := cp.Status
	if status.DistributionVersion == "" {
		return true, fmt.Sprintf("the master version of IBM cluster %s is not known yet; workers are replaced once "+
			"IBMCloudManagedControlPlane %s has observed it", status.ClusterID, cp.Name)
	}
	observed := &kubernetesservice.Cluster{State: status.State, MasterKubeVersion: status.DistributionVersion,
		TargetVersion: status.TargetDistributionVersion}
	if IsMasterUpdatePending(observed) {
		update := "updating its master (state " + status.State + ")"
		if status.TargetDistributionVersion != "" && status.TargetDistributionVersion != status.DistributionVersion {
			update = fmt.Sprintf("updating its master from %s to %s", status.DistributionVersion, status.TargetDistributionVersion)
		}
		return true, fmt.Sprintf("IBM cluster %s is %s; workers are replaced once the master update has finished, so that "+
			"they come back on the master's version", status.ClusterID, update)
	}
	if conditions.IsTrue(cp, controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition) {
		return true, fmt.Sprintf("IBMCloudManagedControlPlane %s reports a master update in progress (RollingOut); workers are "+
			"replaced once it has finished", cp.Name)
	}
	return false, ""
}

// replaceRefusalMessage is the WorkersUpToDate message while IBM's synchronous refusal of ReplaceWorker holds, and the
// replace's refusal record (refusalRecord.message): no other step of the rollout writes it. The reason alone is no
// record, because the Gated step reports ExternallyAutoscaled for an autoscaled pool too; only this message tells
// IBM's refusal apart. It is fixed per reason, so that the hold writes the same condition on every reconcile and status
// does not churn, and it carries no IBM text.
//
// Parameters:
//   - reason is the refusal's reason: WaitingForClusterMinimum (E1437) or ExternallyAutoscaled.
func replaceRefusalMessage(reason DisruptionReason) string {
	if upToDateReason(reason) == infrav1.IBMCloudManagedMachinePoolWorkersUpToDateExternallyAutoscaledReason {
		return "IBM refused the replace because its cluster autoscaler owns this pool; the drained Node stays cordoned; " +
			"disable the autoscaler of the pool to let CAPIBM replace its workers"
	}
	return fmt.Sprintf("IBM refused the replace because the cluster would fall below its minimum workers (%s); the drained "+
		"Node stays cordoned, and the replace is requested again at most once every %d minutes while CAPIBM's own guards allow it",
		ClusterMinimumErrorCode, int(DisruptionSettleDelay.Minutes()))
}

// workerRollout is one reconcile of the worker rollout of a pool: what was observed, the worker being replaced (in
// flight, resumed from this pool's drain-owner marker, or the next due one), and the handles that carry out the step
// DecideDisruptionStep chooses. The steps every removal shares are its runner's (removal_runner.go).
type workerRollout struct {
	s       *MachinePoolScope
	clients *poolClients
	pool    *kubernetesservice.WorkerPool
	workers []kubernetesservice.Worker
	// workload is the workload cluster's Nodes; no client while not connected.
	workload workloadNodes
	// account is the IBM account of the cluster, for provider IDs.
	account string
	// summary is SummarizeRollout's answer of this reconcile.
	summary RolloutSummary
	// countsKnown is false while spec.rollout.after has passed and the workload cluster is not connected: the Nodes'
	// creation times cannot be read, so the summary does not know which workers are due.
	countsKnown bool
	// nodesRead is true when this reconcile read the Nodes of the pool's workers (readNodes): only then does
	// ReplaceProgress know which Nodes are Ready, and only then may the report name the workers it waits for.
	nodesRead bool

	// victim is the worker being replaced or replaced next; "" when no worker is due and none is in flight.
	victim string
	// victimClass is why the victim is replaced (WorkerUpdateDecision.DueBecause), or its class when it is no longer due.
	victimClass WorkerUpdateClass
	// triggerActive is DisruptionObservation.TriggerActive: the victim is listed and still due.
	triggerActive bool
	// resumed is true when the replace resumes from the victim's drain-owner marker because status.disruption was lost.
	resumed bool
	// replacement is IBM's progress with the victim's replace.
	replacement ReplaceProgressResult
	// token is the Cluster's disruption token; runner carries out the shared steps. Both nil without a victim.
	token  *DisruptionToken
	runner *removalRunner
	// preCall is the pre-call gate of this reconcile, which the report reads to tell the settle delay between workers.
	preCall DisruptionCheckResult
}

// newWorkerRollout observes the worker rollout of the pool: the Nodes of the pool's workers when a decision needs
// them, the summary, the victim, and, with a victim, IBM's progress with it, the token and the runner. It also
// records the time the rollout decision changes without a watched change (MachinePoolScope.RolloutRecheckAt).
//
// Parameters:
//   - ctx is the reconcile context.
//   - in are the observations of reconcileSize (newScaleDownInput).
//   - triggerGone treats the replace in flight as no longer wanted: a failing pool guard leaves the pool without a valid
//     spec, and every failing guard forbids an IBM mutation.
func (s *MachinePoolScope) newWorkerRollout(ctx context.Context, in scaleDownInput, triggerGone bool) (*workerRollout, error) {
	ro := &workerRollout{s: s, clients: in.clients, pool: in.pool, workers: in.workers, workload: in.workload}
	var err error
	if ro.account, err = AccountIDFromCRN(s.ControlPlane.Status.ClusterCRN); err != nil {
		return nil, fmt.Errorf("finding the Nodes of pool %s: %w", s.Pool.Spec.PoolName, err)
	}
	nodes, err := ro.readNodes(ctx)
	if err != nil {
		return nil, err
	}
	created := nodeCreationTimes(nodes)
	ro.countsKnown = ro.workload.client != nil || !ro.rolloutAfterPassed()
	if ro.summary, err = ro.summarize(created); err != nil {
		return nil, err
	}
	ro.chooseVictim(in, created)
	if ro.victim == "" {
		ro.recordRecheck(time.Time{})
		return ro, nil
	}
	ro.triggerActive = ro.triggerActive && !triggerGone
	if err := ro.observeReplace(nodes); err != nil {
		return nil, err
	}
	if ro.token, err = s.disruptionToken(); err != nil {
		return nil, err
	}
	if ro.runner, err = ro.newRunner(); err != nil {
		return nil, err
	}
	// The victim comes from its Node's drain-owner marker because status.disruption was lost: the drain is not
	// announced again (removalRunner.record).
	ro.runner.resumed = ro.resumed
	return ro, nil
}

// readNodes returns the Node of every worker of the pool that IBM does not list as deleted, by worker ID, when this
// reconcile's decision needs them: spec.rollout.after has passed (the Nodes' creation times), or a replace waits for
// its replacement (the Nodes' Ready times). Otherwise, and while the workload cluster is not connected, it returns none
// and makes no workload-cluster call. It records whether it read them (nodesRead). ctx is the reconcile context.
func (ro *workerRollout) readNodes(ctx context.Context) (map[string]*corev1.Node, error) {
	waitsForReplacement := ro.s.Pool.Status.Disruption.Phase == infrav1.WorkerDisruptionPhaseWaitingForReplacement
	if ro.workload.client == nil || (!ro.rolloutAfterPassed() && !waitsForReplacement) {
		return nil, nil
	}
	var ids []string
	for _, w := range ro.workers {
		if w.PoolID == ro.pool.ID && w.ActualState != WorkerStateDeleted {
			ids = append(ids, w.ID)
		}
	}
	nodes, err := workerNodes(ctx, ro.workload.client, ro.account, ro.s.ControlPlane.Status.ClusterID, ids)
	if err != nil {
		return nil, err
	}
	ro.nodesRead = true
	return nodes, nil
}

// workerNodes finds the Node of each worker by its provider ID through ClusterCache's NodeProviderIDIndex (the index
// is registered in SetupClusterCache), as Cluster API's Machine controller finds a Machine's Node. A worker without a
// Node has no entry; should two Nodes carry one provider ID, the first by name is taken, so that the answer is
// deterministic.
//
// Parameters:
//   - ctx is the reconcile context.
//   - workload is the workload cluster's cached client.
//   - account, clusterID name the IBM cluster (AccountIDFromCRN, status.clusterID).
//   - workerIDs are the workers.
func workerNodes(ctx context.Context, workload client.Client, account, clusterID string, workerIDs []string) (map[string]*corev1.Node, error) {
	nodes := make(map[string]*corev1.Node, len(workerIDs))
	for _, id := range workerIDs {
		list := &corev1.NodeList{}
		if err := workload.List(ctx, list, client.MatchingFields{index.NodeProviderIDField: ProviderID(account, clusterID, id)}); err != nil {
			return nil, fmt.Errorf("finding the Node of worker %s by its provider ID: %w", id, err)
		}
		if len(list.Items) == 0 {
			continue
		}
		slices.SortFunc(list.Items, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
		nodes[id] = &list.Items[0]
	}
	return nodes, nil
}

// nodeCreationTimes returns the creationTimestamp of each worker's Node, for ClassifyWorkerUpdate. nodes are the Nodes
// by worker ID.
func nodeCreationTimes(nodes map[string]*corev1.Node) map[string]time.Time {
	created := make(map[string]time.Time, len(nodes))
	for id, node := range nodes {
		created[id] = node.CreationTimestamp.Time
	}
	return created
}

// nodeReadySince returns when node became Ready, and false when it is not Ready. The Ready condition's
// lastTransitionTime is the time; a Node that reports none counts from its creation.
//
// Parameters:
//   - node is the Node.
func nodeReadySince(node *corev1.Node) (time.Time, bool) {
	for _, c := range node.Status.Conditions {
		if c.Type != corev1.NodeReady {
			continue
		}
		if c.Status != corev1.ConditionTrue {
			return time.Time{}, false
		}
		if c.LastTransitionTime.IsZero() {
			return node.CreationTimestamp.Time, true
		}
		return c.LastTransitionTime.Time, true
	}
	return time.Time{}, false
}

// rolloutAfterPassed reports whether spec.rollout.after is set and has passed.
func (ro *workerRollout) rolloutAfterPassed() bool {
	after := ro.s.Pool.Spec.Rollout.After
	return !after.IsZero() && !after.After(ro.s.params.Clock.Now())
}

// summarize is SummarizeRollout for this reconcile.
//
// Parameters:
//   - created are the creation times of the workers' Nodes (nodeCreationTimes).
func (ro *workerRollout) summarize(created map[string]time.Time) (RolloutSummary, error) {
	summary, err := SummarizeRollout(ro.rolloutInput(created))
	if err != nil {
		return RolloutSummary{}, fmt.Errorf("summarizing the worker rollout of pool %s: %w", ro.s.Pool.Spec.PoolName, err)
	}
	return summary, nil
}

// rolloutInput is the RolloutInput of this reconcile.
//
// Parameters:
//   - created are the creation times of the workers' Nodes.
func (ro *workerRollout) rolloutInput(created map[string]time.Time) RolloutInput {
	return RolloutInput{Workers: ro.workers, PoolID: ro.pool.ID, SizePerZone: ro.pool.SizePerZone,
		Zones: len(scaleDownZones(ro.pool, ro.s.Pool)), MasterMinor: ro.s.masterMinor(), Policy: ro.s.Pool.Spec.WorkerUpdates.Policy,
		RolloutAfter: ro.s.Pool.Spec.Rollout.After.Time, NodeCreated: created, Now: ro.s.params.Clock.Now()}
}

// chooseVictim picks the worker the rollout works on: the replace in flight (status.disruption), a replace whose status
// was lost and whose victim's Node still carries this pool's drain-owner marker (as after a controller restart or a
// clusterctl move), or the next due worker (PickReplacement). A lost status resumes as a replace only when exactly one
// Node of the pool is marked, the MachinePool asks for no fewer workers than IBM's pool has, and that worker is still
// due; any other marked set is a scale-down's, which the scale-down resumes or gives up. It also decides whether the
// trigger of the replace is active: the victim is listed, not deleted, and still due.
//
// Parameters:
//   - in are the observations of reconcileSize.
//   - created are the creation times of the workers' Nodes.
func (ro *workerRollout) chooseVictim(in scaleDownInput, created map[string]time.Time) {
	disruption := ro.s.Pool.Status.Disruption
	switch {
	case disruption.Operation == infrav1.WorkerDisruptionOperationReplace && len(disruption.WorkerIDs) > 0:
		ro.victim = disruption.WorkerIDs[0]
	case disruption.Operation == "" && len(in.marked) == 1 && in.desired >= ro.pool.SizePerZone:
		ro.victim, ro.resumed = in.marked[0], true
	case ro.summary.HasNext:
		ro.victim, ro.victimClass, ro.triggerActive = ro.summary.Next.Worker.ID, ro.summary.Next.Decision.DueBecause, true
		return
	default:
		return
	}
	ro.classifyVictim(created)
	if ro.resumed && !ro.triggerActive {
		ro.victim, ro.resumed = "", false
	}
}

// classifyVictim decides whether the victim of a replace in flight, or of one resumed from its marker, is still due
// (TriggerActive), and why. While the workload cluster is not connected the victim's Node cannot be read; its creation
// time never changes, and it was before spec.rollout.after when the victim was picked, so the victim keeps that
// classification for as long as spec.rollout.after stays set and passed. A rollout.after removed meanwhile, or a policy
// set back to Manual, still ends the trigger.
//
// Parameters:
//   - created are the creation times of the workers' Nodes.
func (ro *workerRollout) classifyVictim(created map[string]time.Time) {
	w, listed := workerByID(ro.workers, ro.victim)
	if !listed || w.ActualState == WorkerStateDeleted {
		return
	}
	in := ro.rolloutInput(created)
	nodeCreated := created[ro.victim]
	if ro.workload.client == nil && ro.rolloutAfterPassed() {
		nodeCreated = in.RolloutAfter.Add(-time.Nanosecond)
	}
	decision, err := ClassifyWorkerUpdate(w, in.MasterMinor, in.Policy, in.RolloutAfter, nodeCreated, in.Now)
	if err != nil {
		// The policy and the clock were validated by SummarizeRollout; an error here leaves the trigger inactive.
		return
	}
	ro.victimClass, ro.triggerActive = decision.DueBecause, decision.Due
	if !decision.Due {
		ro.victimClass = decision.Class
	}
}

// observeReplace computes IBM's progress with the victim's replace (ReplaceProgress) from this reconcile's listing, the
// published provider IDs and the Nodes, and records when the replacement's soak ends.
//
// Parameters:
//   - nodes are the Nodes of the pool's workers, by worker ID (readNodes).
func (ro *workerRollout) observeReplace(nodes map[string]*corev1.Node) error {
	readySince := make(map[string]time.Time, len(nodes))
	for id, node := range nodes {
		if since, ready := nodeReadySince(node); ready {
			readySince[id] = since
		}
	}
	clusterID := ro.s.ControlPlane.Status.ClusterID
	progress, err := ReplaceProgress(ReplaceProgressInput{Workers: ro.workers, PoolID: ro.pool.ID, Victim: ro.victim,
		SizePerZone: ro.pool.SizePerZone, Zones: len(scaleDownZones(ro.pool, ro.s.Pool)),
		StillPublished:  slices.Contains(ro.s.Pool.Spec.ProviderIDList, ProviderID(ro.account, clusterID, ro.victim)),
		NodeReadySince:  readySince,
		MinReadySeconds: ptr.Deref(ro.s.MachinePool.Spec.Template.Spec.MinReadySeconds, 0),
		Now:             ro.s.params.Clock.Now()})
	if err != nil {
		return fmt.Errorf("observing the replace of worker %s of pool %s: %w", ro.victim, ro.s.Pool.Spec.PoolName, err)
	}
	ro.replacement = progress
	ro.recordRecheck(progress.ReadyAt)
	return nil
}

// recordRecheck records on the scope the earliest time at which the rollout decision changes without a watched
// change: spec.rollout.after while it is ahead (the rollout starts then), or the end of a replacement's soak.
//
// Parameters:
//   - soakEnds is ReplaceProgressResult.ReadyAt; zero for none.
func (ro *workerRollout) recordRecheck(soakEnds time.Time) {
	at := soakEnds
	if after := ro.s.Pool.Spec.Rollout.After.Time; after.After(ro.s.params.Clock.Now()) && (at.IsZero() || after.Before(at)) {
		at = after
	}
	ro.s.rolloutRecheckAt = at
}

// newRunner builds the replace's runner (removal_runner.go): the victim and its Node, ReplaceWorker with update=true,
// reports on WorkersUpToDate in the rollout's words (reportUpToDate), IBM's refusal recorded there with
// replaceRefusalMessage, which only a refusal writes (lastCallRefused tells it apart from the pre-call gate's holds
// that way), and the cluster state the control plane observed. IBM's answers that the common error handling
// classifies (a 403 for a missing Kubernetes Service role, a 401, a 400) are reported on WorkersUpToDate too, with the
// reason IBMAPIError and IBM's code only (an incident ID changes with every call): WorkerPoolReady is reported afresh
// from IBM's pool on every reconcile (reportReadiness), so the answer would be gone from status by the reconcile that
// the status patch triggers, and the runner's refusalHold keeps it on WorkersUpToDate for the rest of
// DisruptionSettleDelay.
func (ro *workerRollout) newRunner() (*removalRunner, error) {
	clusterID := ro.s.ControlPlane.Status.ClusterID
	victim, kubernetes, s := ro.victim, ro.clients.kubernetes, ro.s
	return newRemovalRunner(removalRunnerInput{
		scope: s, clients: ro.clients, pool: ro.pool,
		operation: infrav1.WorkerDisruptionOperationReplace, victims: []string{victim}, clusterState: s.ControlPlane.Status.State,
		call: removalCall{op: kubernetesservice.OpReplaceWorker,
			action: fmt.Sprintf("replacing worker %s of IBM cluster %s", victim, clusterID),
			send: func(ctx context.Context) error {
				// update=true always: the worker comes back on IBM's newest fix pack of the master minor.
				err := kubernetes.ReplaceWorker(ctx, clusterID, victim, true)
				if err == nil {
					s.params.Recorder.Eventf(s.Pool, corev1.EventTypeNormal, EventWorkerReplaceRequested,
						"asked IBM to replace worker %s of pool %s with update=true (%s)", victim, s.Pool.Spec.PoolName, ro.victimClass)
				}
				return err
			}},
		refusal: refusalRecord{condition: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition, status: metav1.ConditionFalse,
			message: replaceRefusalMessage},
		report: ro.reportUpToDate, reportCallError: ro.reportCallError, workload: ro.workload.client,
		victimNodes: VictimNodes(ro.workload.nodes, ro.account, clusterID, []string{victim}),
	})
}

// reportCallError reports IBM's answer to ReplaceWorker that the common error handling classified (the runner's
// reportCallError) on WorkersUpToDate=False, in the rollout's words: the progress and the worker, then ibmProblemOn's
// message, which names the call and carries IBM's code only, never the incident ID.
//
// Parameters:
//   - reason is the condition reason, IBMAPIError, which reportUpToDate writes as WorkersUpToDate's own constant.
//   - message is ibmProblemOn's message.
func (ro *workerRollout) reportCallError(reason, message string) {
	ro.reportUpToDate(DisruptionDecision{Reason: DisruptionReason(reason), Message: message})
}

// reconcile runs the rollout once: one step of the replace in flight or of the next due worker, or, when no worker is
// due, the WorkersUpToDate report. ctx is the reconcile context; readiness is the WorkerPoolReady outcome of this
// reconcile, which is returned when the rollout has nothing to wait for, and when it requeues sooner than a rollout
// that only waits (rolloutOutcome).
func (ro *workerRollout) reconcile(ctx context.Context, readiness Outcome) Outcome {
	if ro.victim == "" {
		ro.reportIdle()
		return readiness
	}
	return rolloutOutcome(ro.run(ctx), readiness)
}

// rolloutOutcome returns the outcome of a pool reconcile that ran a step of the rollout. A step that is done leaves the
// pool's readiness outcome. A rollout that only waits (a gate, a hold, the settle delay between workers) polls at
// guardedPoll, and the pool's readiness then goes first when it polls IBM's provisioning at poolTransitionPoll: after a
// master minor upgrade every worker is due and gated, and a guardedPoll requeue would show the pool's growth and
// provisioning up to four times later than without a rollout. An active step (the drain, the call, the waits for
// IBM, at drainRetryInterval or poolTransitionPoll), a failure, a 429 and a condition that needs a human keep the
// rollout's outcome: none of them requeues later than the readiness poll, and a 429's Retry-After and an error's
// backoff must not be shortened. It is pure.
//
// Parameters:
//   - rollout is the outcome of the rollout's step.
//   - readiness is the WorkerPoolReady outcome of the reconcile (reportReadiness), or Done.
func rolloutOutcome(rollout, readiness Outcome) Outcome {
	switch {
	case rollout.Kind == OutcomeDone:
		return readiness
	case rollout.Kind == OutcomeNotDone && rollout.Interval == IntervalGuarded &&
		readiness.Kind == OutcomeNotDone && readiness.Interval == IntervalPoolTransition:
		return readiness
	}
	return rollout
}

// run observes, decides and carries out one step of the replace. ctx is the reconcile context.
func (ro *workerRollout) run(ctx context.Context) Outcome {
	o, err := ro.observation(ctx)
	if err != nil {
		return Transient(err)
	}
	for range maxRolloutDecisions {
		decision, err := DecideDisruptionStep(o)
		if err != nil {
			return Transient(err)
		}
		if decision.Step != StepAcquireToken {
			return ro.apply(ctx, decision)
		}
		status, err := ro.token.Acquire(ctx)
		if err != nil {
			return Transient(err)
		}
		o.Token = status.State
		if o.Drain, err = ro.drainProgress(ctx, o); err != nil {
			return Transient(err)
		}
	}
	return Transient(errors.New("managed: the worker replace did not settle on a step (programming error)"))
}

// observation is what DecideDisruptionStep reads for the replace. The gates follow the order of the Gated step; the
// workload cluster counts as connected only with a ClusterCache connection, also under both exclusion annotations: a
// replace waits in WaitingForWorkloadCluster where a pool removal would not, because it must see its replacement's
// Node become Ready, at the Gated step and, through drainProgress, in every phase before its IBM call. ctx is the
// reconcile context.
func (ro *workerRollout) observation(ctx context.Context) (DisruptionObservation, error) {
	tokenStatus, err := ro.token.Status(ctx)
	if err != nil {
		return DisruptionObservation{}, err
	}
	gate, err := ro.check(false)
	if err != nil {
		return DisruptionObservation{}, err
	}
	// The pre-call gate re-reads the cluster state of this reconcile before the settled and capacity rules: a long
	// drain can outlive the Gated check, and ReplaceWorker is never sent to a cluster that turned unavailable,
	// scaled_down or unsupported meanwhile.
	if ro.preCall, err = ro.runner.preCallGate(ro.check); err != nil {
		return DisruptionObservation{}, err
	}
	victim, _ := workerByID(ro.workers, ro.victim)
	masterHolds, _ := MasterUpdateHoldsWorkers(ro.s.ControlPlane)
	o := DisruptionObservation{
		Operation:                infrav1.WorkerDisruptionOperationReplace,
		ResumeFromNodeMarkers:    ro.resumed,
		Now:                      ro.s.params.Clock.Now(),
		IBMRequestTime:           ro.s.Pool.Status.Disruption.IBMRequestTime.Time,
		LastCallRefused:          ro.lastCallRefused(),
		TriggerActive:            ro.triggerActive,
		Token:                    tokenStatus.State,
		WorkloadClusterConnected: ro.runner.drain != nil,
		Gates: DisruptionGates{
			ClusterStateAllows: ro.runner.clusterStateCheck().Allowed,
			MasterUpdating:     masterHolds,
			Disruption:         gate,
			Autoscaled:         ro.pool.AutoscaleEnabled,
			AutobalanceEnabled: zoneAutobalance(ro.pool, victim.Zone),
			OtherDrainOwners:   OtherDrainOwners(ro.workload.nodes, DrainOwner(ro.s.Pool)),
		},
		PreCallGate: ro.preCall,
		IBM:         ro.replacement.Progress,
	}
	if ro.s.Pool.Status.Disruption.Operation == infrav1.WorkerDisruptionOperationReplace {
		o.Phase = ro.s.Pool.Status.Disruption.Phase
	}
	o.Drain, err = ro.drainProgress(ctx, o)
	return o, err
}

// drainProgress is DisruptionObservation.Drain for the replace: the runner's checkDrain, except that while the workload
// cluster is not connected the exclusion annotations do not count as a finished drain or volume detach. The runner's
// disconnectedProgress counts them, which lets a pool removal go on without the workload cluster, but a replace that
// did so would send ReplaceWorker without workload access, then wait in WaitingForReplacement with the Cluster's token
// held until the cluster came back, since its replacement's Node can only be seen there; and a replace given up
// meanwhile could not revert its cordon. So it holds in WaitingForWorkloadCluster before the call, with both
// annotations set, as at the Gated step. A drain or detach time limit that ran out still counts: Cluster API does the
// same, and a human set that limit deliberately.
//
// Parameters:
//   - ctx is the reconcile context.
//   - o is the observation so far.
func (ro *workerRollout) drainProgress(ctx context.Context, o DisruptionObservation) (DrainProgress, error) {
	progress, err := ro.runner.checkDrain(ctx, o)
	if err != nil {
		return DrainProgress{}, err
	}
	if ro.runner.drain == nil {
		progress.Excluded, progress.DetachExcluded = false, false
	}
	return progress, nil
}

// check is CheckDisruption for one replace (removedReady = 1: a replace takes one ready worker away). The capacity
// message names the MachinePool size that allows a rolling replacement ("scale MachinePool <name> to <n+1>", reason
// WaitingForCapacity). skipSettleDelay selects the pre-call gate. Both get the controller's first-seen times, so that
// a worker IBM has left transitional for longer than WorkerStuckAfter no longer holds the rollout (CheckDisruption
// rule 1), and the settle delay reads this object's lastDisruptionTime as this reconcile set it.
func (ro *workerRollout) check(skipSettleDelay bool) (DisruptionCheckResult, error) {
	pools := slices.Clone(ro.s.clusterPools)
	for i := range pools {
		if pools[i].Name == ro.s.Pool.Name {
			pools[i].Status.LastDisruptionTime = ro.s.Pool.Status.LastDisruptionTime
		}
	}
	return CheckDisruption(DisruptionCheck{
		Operation:         infrav1.WorkerDisruptionOperationReplace,
		Distribution:      ro.s.ControlPlane.Spec.Distribution,
		Now:               ro.s.params.Clock.Now(),
		Workers:           ro.workers,
		TransitionalSince: ro.s.firstSeen,
		Pools:             pools,
		RemovedReady:      1,
		SkipSettleDelay:   skipSettleDelay,
		MachinePoolName:   ro.s.MachinePool.Name,
		Replicas:          ptr.Deref(ro.s.MachinePool.Spec.Replicas, 1),
		ZoneCount:         max(len(ro.s.Pool.Spec.Zones), 1),
	})
}

// lastCallRefused is DisruptionObservation.LastCallRefused for the replace: a ReplaceWorker was sent, WorkersUpToDate
// still records IBM's synchronous refusal of it (the runner's recordedRefusal, whose record needs
// replaceRefusalMessage, which no other step writes), and IBM shows no effect of it: the victim is listed and IBM is
// not removing it. A replace whose trigger went away after such a refusal is then given up at once. Without the record
// (a later step reported over it, or its status patch was lost) this is false, and the replace waits WorkerStuckAfter
// as after any other call attempt, which fails closed.
func (ro *workerRollout) lastCallRefused() bool {
	victim, listed := workerByID(ro.workers, ro.victim)
	return !ro.s.Pool.Status.Disruption.IBMRequestTime.IsZero() && listed && !WorkerBeingRemoved(victim) &&
		ro.runner.recordedRefusal() != ""
}

// zoneAutobalance returns autobalanceEnabled of the zone of IBM's pool; false when IBM does not list the zone, so that
// a replace that might create no replacement never starts (fail safe).
//
// Parameters:
//   - pool is the IBM pool.
//   - zone is the victim's zone.
func zoneAutobalance(pool *kubernetesservice.WorkerPool, zone string) bool {
	for _, z := range pool.Zones {
		if z.ID == zone {
			return z.AutobalanceEnabled
		}
	}
	return false
}

// apply carries out the decision of this reconcile, one step of the replace.
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (ro *workerRollout) apply(ctx context.Context, d DisruptionDecision) Outcome {
	runner := ro.runner
	switch d.Step {
	case StepNone, StepWait:
		if d.ReleaseToken {
			if err := ro.token.Release(ctx); err != nil {
				return Transient(err)
			}
		}
		return runner.wait(ro.betweenWorkers(d), IntervalGuarded)
	case StepDrain:
		return runner.drainVictims(ctx, d)
	case StepWaitForVolumeDetach:
		if err := runner.announceTimeout(ctx); err != nil {
			return Transient(err)
		}
		runner.record(d.Phase)
		return runner.wait(d, IntervalDrainRetry)
	case StepHold:
		runner.record(d.Phase)
		return runner.wait(d, IntervalGuarded)
	case StepCallIBM:
		// ReplaceWorker(update=true); IBM's E1437 holds as WaitingForClusterMinimum with the Node cordoned, and the call
		// is repeated at most once per DisruptionSettleDelay (DisruptionCallOutcome).
		return runner.callIBM(ctx, d)
	case StepWaitForIBM:
		runner.record(d.Phase)
		// With the trigger gone, decideTriggerGone's message says what happens; the refusal's would promise a call.
		if hold, held := runner.refusalHold(d); held && ro.triggerActive {
			return runner.wait(hold, IntervalGuarded)
		}
		return runner.wait(d, IntervalPoolTransition)
	case StepWaitForRemoval, StepWaitForReplacement:
		runner.record(d.Phase)
		return runner.wait(d, IntervalPoolTransition)
	case StepComplete:
		return ro.finish(ctx, "")
	case StepAbort:
		return ro.abort(ctx, d)
	default:
		// StepHandOver and StepStop belong to a pool or Cluster deletion, which never reach the normal flow;
		// StepCorrectVictims belongs to a scale-down.
		return Transient(fmt.Errorf("managed: step %s is not a step of a worker replace (programming error)", d.Step))
	}
}

// abort carries out StepAbort: the worker is no longer due before the IBM call, or after a call IBM refused, or IBM
// removed it out of band. It uncordons what CAPIBM cordoned (only CAPIBM's changes: a cordon a human set stays),
// emits NodeUncordoned and releases the token. The replace needs the workload cluster for that: while it is not
// connected, nothing is given up. ctx is the reconcile context; d is the decision.
func (ro *workerRollout) abort(ctx context.Context, d DisruptionDecision) Outcome {
	reverted, outcome, held := ro.runner.uncordon(ctx, ro.account, []string{ro.victim}, ro.s.Pool.Status.Disruption.Phase, false)
	if held {
		return outcome
	}
	if reverted > 0 {
		ro.s.params.Recorder.Eventf(ro.s.Pool, corev1.EventTypeNormal, EventNodeUncordoned,
			"reverted CAPIBM's cordon of %d Nodes of pool %s: %s", reverted, ro.s.Pool.Spec.PoolName, d.Message)
	}
	return ro.finish(ctx, d.Message)
}

// finish ends the replace: it releases the token, clears status.disruption, sets Draining False and reports
// WorkersUpToDate for the workers left. With more workers due it waits guardedPoll, so that the next worker starts
// from idle after the settle delay.
//
// Parameters:
//   - ctx is the reconcile context.
//   - givenUp says why the replace was given up, for the log; "" when it completed.
func (ro *workerRollout) finish(ctx context.Context, givenUp string) Outcome {
	if err := ro.token.Release(ctx); err != nil {
		return Transient(err)
	}
	ro.s.Pool.Status.Disruption = infrav1.WorkerDisruptionStatus{}
	conditions.Set(ro.s.Pool, metav1.Condition{Type: infrav1.IBMCloudManagedMachinePoolDrainingCondition,
		Status: metav1.ConditionFalse, Reason: infrav1.IBMCloudManagedMachinePoolNotDrainingReason})
	if givenUp != "" {
		ctrl.LoggerFrom(ctx).Info("The worker replace ends without replacing the worker", "worker", ro.victim, "reason", givenUp)
	}
	// The summary of this reconcile counts the workers still due: a completed victim is deleted or gone, and one that is
	// given up counts only while it is still due, as it will be picked again.
	if ro.summary.Due == 0 {
		ro.reportIdle()
		return Done()
	}
	reason := infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason
	ro.setUpToDate(metav1.ConditionFalse, reason, fmt.Sprintf("%s; the next worker starts once the cluster has settled",
		ro.progress(ro.summary.Replaced())))
	return NotDone(IntervalGuarded, reason)
}

// reportUpToDate reports a decision on WorkersUpToDate=False (the runner's report of the replace, which adds the
// Draining condition): the step's reason as WorkersUpToDate names it (upToDateReason), with the progress and the worker
// in the message. A decision without a reason leaves WorkersUpToDate as it is. d is the decision.
func (ro *workerRollout) reportUpToDate(d DisruptionDecision) {
	if d.Reason == "" {
		return
	}
	reason, detail := upToDateReason(d.Reason), d.Message
	switch {
	case d.Step != StepWait:
	case d.Reason == ReasonWaitingForClusterState:
		// The gate's generic text, replaced by the state and, on an unsupported cluster, the fix.
		detail = ro.runner.clusterStateCheck().Message
	case d.Reason == ReasonWaitingForControlPlaneUpgrade:
		_, detail = MasterUpdateHoldsWorkers(ro.s.ControlPlane)
	}
	if d.Reason == ReasonWaitingForReplacement {
		detail += ro.replacementDetail()
	}
	worker := "worker " + ro.victim
	if ro.victimClass != "" {
		worker += " (" + string(ro.victimClass) + ")"
	}
	ro.setUpToDate(metav1.ConditionFalse, reason, fmt.Sprintf("%s; %s: %s", ro.progress(ro.summary.Replaced()), worker, detail))
}

// replacementDetail is the part of the WaitingForReplacement message that says what the replace waits for: the workers
// that ReplaceProgress found not deployed, normal and Ready for minReadySeconds, when this reconcile read their Nodes
// (nodesRead). While the workload cluster is not connected no Node can be read, and ReplaceProgress then finds every
// worker waiting; naming them would send the operator after IBM's worker health when the connection is what holds the
// replace (and the Cluster's disruption token), so it says that instead. The reconcile that enters
// WaitingForReplacement has read no Node yet and adds nothing; the next one names the workers.
func (ro *workerRollout) replacementDetail() string {
	switch {
	case ro.workload.client == nil:
		return " (the workload cluster is not connected, so CAPIBM cannot read whether the Nodes are Ready; the replace " +
			"goes on once it is connected)"
	case !ro.nodesRead || len(ro.replacement.Waiting) == 0:
		return ""
	}
	return fmt.Sprintf(" (%s not yet deployed, normal and Ready for spec.template.spec.minReadySeconds)", listWorkers(ro.replacement.Waiting))
}

// progress is the progress part of a rollout message: "replaced <k> of <n>" (reason RollingOut), or, while the Nodes'
// creation times cannot be read, only that the pool is rolling out.
//
// Parameters:
//   - replaced is k.
func (ro *workerRollout) progress(replaced int) string {
	if !ro.countsKnown {
		return "rolling out pool " + ro.s.Pool.Spec.PoolName
	}
	return fmt.Sprintf("replaced %d of %d", max(replaced, 0), ro.summary.Workers)
}

// reportIdle reports WorkersUpToDate while no worker is due and none is replaced: PatchUpdateAvailable under the
// Manual policy while IBM offers workers a newer fix pack, else UpToDate. While spec.rollout.after has passed and the
// workload cluster is not connected, the Nodes' creation times cannot be read, so it reports
// WaitingForWorkloadCluster instead of a state it does not know.
func (ro *workerRollout) reportIdle() {
	poolName := ro.s.Pool.Spec.PoolName
	after := ro.s.Pool.Spec.Rollout.After
	switch {
	case !ro.countsKnown:
		ro.setUpToDate(metav1.ConditionFalse, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForWorkloadClusterReason,
			fmt.Sprintf("spec.rollout.after %s has passed; CAPIBM replaces every worker of pool %s whose Node is older, and "+
				"it reads the Nodes through the workload cluster, which is not connected", rfc3339(after.Time), poolName))
	case ro.summary.PatchAvailable > 0:
		message := fmt.Sprintf("%d of %d workers can move to IBM fix pack %s; set spec.rollout.after to roll them, or set "+
			"spec.workerUpdates.policy: Automatic", ro.summary.PatchAvailable, ro.summary.Workers, ro.summary.FixPack)
		if after.After(ro.s.params.Clock.Now()) {
			message = fmt.Sprintf("%d of %d workers can move to IBM fix pack %s; spec.rollout.after rolls them at %s, or set "+
				"spec.workerUpdates.policy: Automatic", ro.summary.PatchAvailable, ro.summary.Workers, ro.summary.FixPack, rfc3339(after.Time))
		}
		ro.setUpToDate(metav1.ConditionFalse, infrav1.IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason, message)
	default:
		message := fmt.Sprintf("no worker of pool %s is due for replacement, and IBM offers none of them a newer fix pack or minor", poolName)
		if after.After(ro.s.params.Clock.Now()) {
			message += fmt.Sprintf("; spec.rollout.after replaces every worker whose Node is older at %s", rfc3339(after.Time))
		}
		ro.setUpToDate(metav1.ConditionTrue, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReason, message)
	}
}

// reportHeldByScaleDown reports WorkersUpToDate while a scale-down of this pool runs or is wanted, which goes before a
// rollout (a removal in flight goes first, then a scale-down, then a rollout): RollingOut with the progress while
// workers are due, the idle report otherwise.
func (ro *workerRollout) reportHeldByScaleDown() {
	if ro.summary.Due == 0 {
		ro.reportIdle()
		return
	}
	ro.setUpToDate(metav1.ConditionFalse, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason,
		fmt.Sprintf("%s; the next replacement waits until the scale-down of pool %s has finished",
			ro.progress(ro.summary.Replaced()), ro.s.Pool.Spec.PoolName))
}

// setUpToDate sets WorkersUpToDate, with the VersionIgnored note when the MachinePool sets spec.template.spec.version
// (whatever the status).
//
// Parameters:
//   - status is the condition status.
//   - reason is the condition reason.
//   - message explains it; deterministic and free of IBM text, so that status does not churn.
func (ro *workerRollout) setUpToDate(status metav1.ConditionStatus, reason, message string) {
	if note := versionMessage(ro.s.MachinePool); note != "" {
		message += "; " + note
	}
	conditions.Set(ro.s.Pool, metav1.Condition{Type: infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition,
		Status: status, Reason: reason, Message: message})
}

// upToDateReason returns the WorkersUpToDate reason of a decision of the replace. The decisions of
// DecideDisruptionStep and of the removal runner carry DisruptionReason values, which are the reason constants of the
// condition the flow reports on. Two of a replace's come from other conditions: ExternallyAutoscaled
// (WorkerPoolReady's: the autoscaler gate, and DisruptionCallOutcome's hold for an autoscaled pool) and IBMAPIError
// (the runner's report of a failed call, MachinePoolIBMAPIErrorReason); they become WorkersUpToDate's own constants.
// Every other reason of a replace already is a WorkersUpToDate constant and is returned as it is. The tokens are the
// same on every condition, which the runner relies on: it reads its records back from WorkersUpToDate by those tokens
// (refusalRecord.read, recordedCallError). It is pure.
//
// Parameters:
//   - reason is the decision's reason.
func upToDateReason(reason DisruptionReason) string {
	switch reason {
	case ReasonExternallyAutoscaled:
		return infrav1.IBMCloudManagedMachinePoolWorkersUpToDateExternallyAutoscaledReason
	case DisruptionReason(MachinePoolIBMAPIErrorReason):
		return infrav1.IBMCloudManagedMachinePoolWorkersUpToDateIBMAPIErrorReason
	default:
		return string(reason)
	}
}

// rfc3339 formats t for a condition message.
//
// Parameters:
//   - t is the time.
func rfc3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// settleReplace runs a replace in flight on while a pool guard fails (reconcileGuardBlocked): the pool has no valid
// spec, so the replace is no longer wanted (TriggerActive false). Before its IBM call it is given up (uncordon,
// NodeUncordoned, token released); after the call it follows IBM to its end; it never makes an IBM mutation. Freezing
// it instead would keep its Node cordoned and the Cluster's disruption token held until a human fixed the spec. The
// replace reports on WorkersUpToDate, so WorkerPoolReady reports the guard all along; the replace's outcome is
// returned while it runs, the guard's once it ended.
//
// Parameters:
//   - ctx is the reconcile context.
//   - in are the observations of the guard path (newScaleDownInput).
//   - guards is PoolGuards' answer; it is blocked.
func (s *MachinePoolScope) settleReplace(ctx context.Context, in scaleDownInput, guards PoolGuardResult) Outcome {
	rollout, err := s.newWorkerRollout(ctx, in, true)
	if err != nil {
		return Transient(err)
	}
	outcome := Done()
	if rollout.victim != "" {
		outcome = rollout.run(ctx)
	}
	guarded := s.guardOutcome(guards)
	if outcome.Kind == OutcomeTransient || s.Pool.Status.Disruption.Operation != "" {
		return outcome
	}
	return guarded
}

// RolloutRecheckAt returns the time at which the worker rollout of this reconcile may decide differently without any
// watched change: spec.rollout.after while it lies ahead (the replacements start then), or the end of a replacement
// Node's minReadySeconds; zero when neither applies. The controller requeues no later than that. Call it after
// ReconcileNormal.
func (s *MachinePoolScope) RolloutRecheckAt() time.Time {
	return s.rolloutRecheckAt
}

// betweenWorkers returns the wait of a gate as the rollout reports it: when only the settle delay holds the next
// worker (CheckDisruption rule 2; the pre-call gate, which leaves it out, passes) and this pool removed a worker less
// than DisruptionSettleDelay ago, the rollout is between its own workers and the reason is RollingOut (a rollout is in
// progress between workers), with the settle delay's message. The first observation of a pool, which sets its
// lastDisruptionTime to now, counts as its own removal too. Another pool's recent removal keeps
// WaitingForSettledCluster, which names that pool, and every other gate keeps its reason.
//
// Parameters:
//   - d is the decision of a gate (StepWait).
func (ro *workerRollout) betweenWorkers(d DisruptionDecision) DisruptionDecision {
	ownDelay := ro.s.params.Clock.Now().Before(ro.s.Pool.Status.LastDisruptionTime.Add(DisruptionSettleDelay))
	if d.Step == StepWait && d.Reason == ReasonWaitingForSettledCluster && ro.preCall.Allowed && ownDelay {
		d.Reason = DisruptionReason(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason)
	}
	return d
}
