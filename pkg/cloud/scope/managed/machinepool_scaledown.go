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

	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
)

// This file is the scale-down of an IBMCloudManagedMachinePool. IBM's resizeWorkerPool cannot name the workers it
// removes, and removeWorker would switch IBM's auto-rebalance off for the pool (ts-worker-autorebalance.md:37-50), so
// CAPIBM predicts IBM's victims (PredictScaleDownVictims), drains them with the shared drain step behind the disruption
// guards and the Cluster's disruption token, asks IBM for the smaller size, and then verifies what IBM really removes:
// a spared victim is uncordoned and named in the Warning UnexpectedScaleDownVictim. The removal is the ScaleDown
// operation of the state machine that DecideDisruptionStep drives one step per reconcile; PlanScaleDown and
// VerifyScaleDownVictims compute its inputs, purely. CAPIBM never calls removeWorker and never sends
// allowSingleOpenShiftWorker (kubernetesservice.ResizeWorkerPool leaves it unset).

const (
	// EventUnexpectedScaleDownVictim is the Warning event emitted when IBM's resize removes other workers than the
	// predicted victims. It names both sets. That IBM removes the predicted victims is not yet verified against IBM Cloud.
	EventUnexpectedScaleDownVictim = "UnexpectedScaleDownVictim"

	// EventNodeUncordoned is the Normal event emitted when a removal is given up before its IBM call and CAPIBM reverts
	// its cordons.
	EventNodeUncordoned = "NodeUncordoned"
)

// maxScaleDownDecisions bounds the decisions of one scale-down reconcile: acquiring the token asks for one more
// decision, and it cannot repeat.
const maxScaleDownDecisions = 2

// ScaleDownPlanInput is the input of PlanScaleDown: what the pool controller observed for the scale-down of one pool.
type ScaleDownPlanInput struct {
	// Workers are every worker of the cluster from one ListWorkers call (all pools, showDeleted=true).
	Workers []kubernetesservice.Worker

	// PoolID is the IBM pool ID, status.poolID. Required.
	PoolID string

	// Zones are the zones IBM's resize changes (scaleDownZones): every zone of IBM's pool, plus spec.zones. Required; each
	// zone once. A resize sets the size of every zone of IBM's pool, which for a hand-made pool the object took over by
	// name can be more zones than spec.zones lists (zonesNote); a plan over spec.zones alone would let IBM remove the
	// workers of the other zones undrained, and CAPIBM never asks IBM to remove a worker whose Node still runs evictable
	// pods.
	Zones []string

	// SizePerZone is IBM's workerCount of the pool in this reconcile's pool listing. Must not be negative.
	SizePerZone int64

	// Desired is the size per zone the MachinePool asks for (spec.replicas / zones). Must not be negative.
	Desired int64

	// Victims are the victims of a scale-down in flight: status.disruption.workerIDs, or, when a lost status is rebuilt
	// (after a clusterctl move or a status patch that never landed), the pool's workers whose Nodes carry this pool's
	// drain-owner marker. Empty plans a new scale-down.
	Victims []string
}

// ScaleDownPlan is PlanScaleDown's answer.
type ScaleDownPlan struct {
	// Victims are the workers the scale-down drains and waits for, sorted: Victims of the input, or the prediction.
	Victims []string

	// Target is the size per zone the resize asks IBM for. A resize to Target removes max(0, workers - Target) workers
	// in each zone, so Target is the smallest size at which that is no more than the victims of the zone, in every
	// zone (the largest workers - victims of a zone): IBM is never asked to remove more workers of a zone than were
	// drained there. It equals Desired for a new plan; it is larger when the MachinePool asked for even
	// fewer workers after the drain started, which a later scale-down removes.
	Target int64

	// TriggerActive is DisruptionObservation.TriggerActive: the MachinePool still asks for fewer workers than IBM's pool
	// has, at most Target per zone, and the victims are complete (in every zone, as many victims as a resize to Target
	// removes there). It is false once spec.replicas went back up (before the IBM call the removal is then aborted: its
	// Nodes are uncordoned and the token released), and for a victim set that a resize to Target would not match, such as
	// one rebuilt from Node markers that another operation left; that removal is aborted and planned anew.
	TriggerActive bool

	// RemovedReady is DisruptionCheck.RemovedReady: the ready capacity a resize to Target takes away in the worst case,
	// because IBM picks the workers it removes, but only from this pool: summed over the zones, the workers the resize
	// removes there, at most the ready workers there. For a balanced pool that is min((sizePerZone - desired) × zones,
	// ready workers of the pool); counted per zone, a zone that is already short adds only what IBM really removes. Zero
	// skips rules 1 and 2 of CheckDisruption: a pool without ready workers can always be scaled back down, which is how a
	// user stops paying for a stalled scale-up.
	RemovedReady int
}

// PlanScaleDown plans the scale-down of one pool. It is pure.
//
// IBM's resize sets every zone of the pool to the target size, so it removes max(0, workers - target) workers of each
// zone, counting the workers IBM does not list as deleted: a zone that is already short (for example after an
// out-of-band `ibmcloud ks worker rm`) loses fewer workers than sizePerZone - desired, or none. A new plan (no Victims)
// targets Desired and takes that many victims per zone from PredictScaleDownVictims; it plans nothing (no victims,
// TriggerActive false) when Desired is not below SizePerZone, and finds no victims when no zone holds more than Desired
// workers. A plan in flight keeps its victims and derives the target from them, so that a restart, a clusterctl move or
// a later replicas change never makes CAPIBM ask IBM for more removals in a zone than it drained there
// (ScaleDownPlan.Target, TriggerActive).
//
// Parameters:
//   - in: the observation; see ScaleDownPlanInput.
//
// It returns an error naming the field to fix when in is invalid.
func PlanScaleDown(in ScaleDownPlanInput) (ScaleDownPlan, error) {
	if err := in.validate(); err != nil {
		return ScaleDownPlan{}, err
	}
	perZone := scaleDownZoneCounts(in)
	if len(in.Victims) == 0 {
		return planNewScaleDown(in, perZone)
	}
	target, anyVictim := int64(0), false
	for _, counts := range perZone {
		target = max(target, int64(counts.workers-counts.victims))
		anyVictim = anyVictim || counts.victims > 0
	}
	return ScaleDownPlan{
		Victims:       slices.Sorted(slices.Values(in.Victims)),
		Target:        target,
		TriggerActive: anyVictim && in.Desired < in.SizePerZone && in.Desired <= target && victimsComplete(perZone, target),
		RemovedReady:  removedReady(perZone, target),
	}, nil
}

// validate returns an error naming the first invalid field of in.
func (in ScaleDownPlanInput) validate() error {
	switch {
	case in.PoolID == "":
		return errors.New("ScaleDownPlanInput.PoolID is empty; pass the IBMCloudManagedMachinePool's status.poolID")
	case len(in.Zones) == 0:
		return errors.New("ScaleDownPlanInput.Zones is empty; pass the zones of IBM's pool and of spec.zones")
	case in.SizePerZone < 0 || in.Desired < 0:
		return fmt.Errorf("ScaleDownPlanInput has SizePerZone %d and Desired %d; want IBM's workerCount and replicas per "+
			"zone, neither negative", in.SizePerZone, in.Desired)
	}
	for i, zone := range in.Zones {
		if slices.Contains(in.Zones[:i], zone) {
			return fmt.Errorf("ScaleDownPlanInput.Zones names zone %s twice; pass each zone once", zone)
		}
	}
	return nil
}

// planNewScaleDown predicts the victims of a new scale-down to Desired. It is PlanScaleDown's case without victims.
//
// Parameters:
//   - in: the validated input.
//   - perZone: the counts of scaleDownZoneCounts.
func planNewScaleDown(in ScaleDownPlanInput, perZone map[string]scaleDownZone) (ScaleDownPlan, error) {
	if in.Desired >= in.SizePerZone {
		return ScaleDownPlan{Target: in.SizePerZone}, nil
	}
	victims := []string{}
	for _, zone := range in.Zones {
		// IBM's order applies within a zone, so a prediction per zone gives each zone its own number of victims.
		zoneVictims, err := PredictScaleDownVictims(in.Workers, in.PoolID, []string{zone}, perZone[zone].removals(in.Desired))
		if err != nil {
			return ScaleDownPlan{}, err
		}
		victims = append(victims, zoneVictims...)
	}
	slices.Sort(victims)
	return ScaleDownPlan{
		Victims:       victims,
		Target:        in.Desired,
		TriggerActive: len(victims) > 0,
		RemovedReady:  removedReady(perZone, in.Desired),
	}, nil
}

// scaleDownZone counts one zone of the pool: the workers IBM does not list as deleted, the ready ones among them, and
// the victims among them.
type scaleDownZone struct {
	workers, ready, victims int
}

// removals returns how many workers a resize to target removes in the zone: every worker above target. target is the
// size per zone the resize asks for.
func (z scaleDownZone) removals(target int64) int {
	return int(max(int64(z.workers)-target, 0))
}

// scaleDownZoneCounts counts the pool's workers, ready workers and victims in every zone of in.Zones. A worker of
// another pool or zone, or one IBM lists as deleted, is not counted. in is the validated input.
func scaleDownZoneCounts(in ScaleDownPlanInput) map[string]scaleDownZone {
	perZone := make(map[string]scaleDownZone, len(in.Zones))
	for _, zone := range in.Zones {
		perZone[zone] = scaleDownZone{}
	}
	for _, w := range in.Workers {
		counts, inPool := perZone[w.Zone]
		if !inPool || w.PoolID != in.PoolID || w.ActualState == WorkerStateDeleted {
			continue
		}
		counts.workers++
		if IsWorkerReady(w) {
			counts.ready++
		}
		if slices.Contains(in.Victims, w.ID) {
			counts.victims++
		}
		perZone[w.Zone] = counts
	}
	return perZone
}

// victimsComplete reports whether every zone has exactly as many victims as a resize to target removes there: fewer
// would let IBM remove an undrained worker, more would leave a drained one cordoned that IBM keeps.
//
// Parameters:
//   - perZone: the counts of scaleDownZoneCounts.
//   - target: the size per zone the resize asks for.
func victimsComplete(perZone map[string]scaleDownZone, target int64) bool {
	for _, counts := range perZone {
		if counts.victims != counts.removals(target) {
			return false
		}
	}
	return true
}

// removedReady is the worst-case ready capacity a resize to target takes away: IBM removes removals(target) workers of
// each zone, at worst every one of them ready.
//
// Parameters:
//   - perZone: the counts of scaleDownZoneCounts.
//   - target: the size per zone the resize asks for.
func removedReady(perZone map[string]scaleDownZone, target int64) int {
	total := 0
	for _, counts := range perZone {
		total += min(counts.removals(target), counts.ready)
	}
	return total
}

// scaleDownZones returns the zones a resize of the IBM pool changes, sorted, each once: every zone IBM lists for the
// pool, plus spec.zones. IBM's resize sets the size of every zone of its pool, which for a hand-made pool the object
// took over by name can be more zones than spec.zones lists (zonesNote only reports the difference); spec.zones is
// added so that a pool listing that leaves out a zone never hides that zone's workers from the drain. A zone too many
// costs nothing: a zone without workers of the pool gets no victim.
//
// Parameters:
//   - pool: the IBM pool.
//   - object: the IBMCloudManagedMachinePool.
func scaleDownZones(pool *kubernetesservice.WorkerPool, object *infrav1.IBMCloudManagedMachinePool) []string {
	zones := poolZoneNames(object)
	for _, z := range pool.Zones {
		if z.ID != "" {
			zones = append(zones, z.ID)
		}
	}
	slices.Sort(zones)
	return slices.Compact(zones)
}

// ScaleDownVictimsInput is the input of VerifyScaleDownVictims: the latest IBM listing and the victims of a scale-down.
type ScaleDownVictimsInput struct {
	// Workers are every worker of the cluster from one ListWorkers call (all pools, showDeleted=true).
	Workers []kubernetesservice.Worker

	// PoolID is the IBM pool ID, status.poolID. Required.
	PoolID string

	// Victims are status.disruption.workerIDs. Required.
	Victims []string

	// StillPublished are the victims whose provider ID is still in spec.providerIDList after this reconcile's
	// BuildProviderIDList: a victim leaves the list only once IBM lists it deleted, or it was absent from two listings
	// poolTransitionPoll apart, which is exactly when its removal counts as complete.
	StillPublished []string

	// Now is the caller's clock reading. Required.
	Now time.Time

	// IBMRequestTime is status.disruption.ibmRequestTime; zero before the resize was sent.
	IBMRequestTime time.Time

	// TransitionalSince maps a worker ID to the time the pool controller first saw the worker transitional
	// (MachinePoolScopeInput.TransitionalSince; IBM reports no time for a transition). A worker of the pool that is not a
	// victim and was already transitional before IBMRequestTime is no pick of the resize: IBM was changing it before
	// CAPIBM asked. Optional: without an entry a worker counts as IBM's pick.
	TransitionalSince map[string]time.Time
}

// ScaleDownVictimsResult is VerifyScaleDownVictims' answer.
type ScaleDownVictimsResult struct {
	// Progress is DisruptionObservation.IBM for the scale-down.
	Progress RemovalProgress

	// Spared are the victims IBM keeps, sorted: IBM's resize removes other workers of their zone instead. They are
	// uncordoned by StepCorrectVictims.
	Spared []string

	// Unexpected are the workers of the pool that IBM removes although they are not victims, sorted.
	Unexpected []string

	// IBMVictims are the workers IBM removes, sorted: the victims without the spared ones, plus the unexpected ones.
	// StepCorrectVictims records them as status.disruption.workerIDs, and the scale-down then waits for them. Empty while
	// no zone is decided, and when IBM removed nothing at all for WorkerStuckAfter after the resize.
	IBMVictims []string
}

// VerifyScaleDownVictims compares what IBM's latest listing shows with the victims of a scale-down. It is pure.
//
// Progress.Removing holds when IBM removes every victim (WorkerBeingRemoved) or no longer lists it; Progress.Removed
// when no victim is published any more (StillPublished is empty). IBM's resize removes a set number of workers of every
// zone and may list its picks one by one, so the victims are verified zone by zone: a zone is decided once IBM removes
// at least as many of the pool's workers there as the zone has victims (victims being removed, plus workers of the pool
// in state deleting that are not victims), or once WorkerStuckAfter has passed since the resize (a zone where IBM
// removes fewer workers than predicted). Only then are the victims of that zone that IBM keeps spared, and its other
// deleting workers taken as IBM's; Progress.UnexpectedVictims is set when a decided zone has either. A pick in one zone
// never spares a victim of another zone. A victim no longer listed counts as removed, in no zone.
//
// Two kinds of workers of the pool are no pick of the resize and are ignored: one in state deleted that is not a victim
// (the listing keeps deleted workers of earlier removals), and one that was already transitional before the resize
// (TransitionalSince), such as a worker IBM has been deleting for longer than WorkerStuckAfter, which the disruption
// guards let pass. The first-seen times are kept in memory only: after a controller restart they start again, and such
// a worker counts as IBM's pick, which fails closed and is bounded to the zone of that worker.
//
// Parameters:
//   - in: the observation; see ScaleDownVictimsInput.
//
// It returns an error naming the field to fix when in is invalid.
func VerifyScaleDownVictims(in ScaleDownVictimsInput) (ScaleDownVictimsResult, error) {
	if err := in.validate(); err != nil {
		return ScaleDownVictimsResult{}, err
	}
	stuck := !in.IBMRequestTime.IsZero() && in.Now.Sub(in.IBMRequestTime) > WorkerStuckAfter
	result := ScaleDownVictimsResult{Progress: RemovalProgress{Removing: true, Removed: len(in.StillPublished) == 0}}
	decided := false
	for _, zone := range in.victimZones() {
		result.Progress.Removing = result.Progress.Removing && len(zone.kept) == 0
		if stuck || zone.picked >= zone.victims {
			decided = true
			result.Spared = append(result.Spared, zone.kept...)
			result.Unexpected = append(result.Unexpected, zone.unexpected...)
		}
	}
	if !decided {
		return ScaleDownVictimsResult{Progress: result.Progress}, nil
	}
	slices.Sort(result.Spared)
	slices.Sort(result.Unexpected)
	result.Progress.UnexpectedVictims = len(result.Spared) > 0 || len(result.Unexpected) > 0
	for _, id := range in.Victims {
		if !slices.Contains(result.Spared, id) {
			result.IBMVictims = append(result.IBMVictims, id)
		}
	}
	result.IBMVictims = slices.Sorted(slices.Values(append(result.IBMVictims, result.Unexpected...)))
	return result, nil
}

// validate returns an error naming the first invalid field of in.
func (in ScaleDownVictimsInput) validate() error {
	switch {
	case in.PoolID == "":
		return errors.New("ScaleDownVictimsInput.PoolID is empty; pass the IBMCloudManagedMachinePool's status.poolID")
	case len(in.Victims) == 0:
		return errors.New("ScaleDownVictimsInput.Victims is empty; pass status.disruption.workerIDs")
	case in.Now.IsZero():
		return errors.New("ScaleDownVictimsInput.Now is not set; pass the controller clock's current time")
	}
	return nil
}

// victimZone is one zone of the pool as VerifyScaleDownVictims sees it.
type victimZone struct {
	// victims are the listed victims of the zone; picked are the workers of the zone IBM removes (victims and others).
	victims, picked int
	// kept are the victims of the zone IBM does not remove; unexpected are the other workers of the zone it removes.
	kept, unexpected []string
}

// victimZones groups the listed victims, and the other workers of the pool IBM removes for the resize, by zone. Only
// zones that have one of them are returned.
func (in ScaleDownVictimsInput) victimZones() map[string]*victimZone {
	zones := map[string]*victimZone{}
	for _, w := range in.Workers {
		if w.PoolID != in.PoolID {
			continue
		}
		victim := slices.Contains(in.Victims, w.ID)
		if !victim && (w.ActualState != WorkerStateDeleting || in.transitionalBeforeCall(w.ID)) {
			continue
		}
		zone, found := zones[w.Zone]
		if !found {
			zone = &victimZone{}
			zones[w.Zone] = zone
		}
		switch {
		case !victim:
			zone.picked++
			zone.unexpected = append(zone.unexpected, w.ID)
		case WorkerBeingRemoved(w):
			zone.victims++
			zone.picked++
		default:
			zone.victims++
			zone.kept = append(zone.kept, w.ID)
		}
	}
	return zones
}

// transitionalBeforeCall reports whether the controller first saw the worker transitional before the resize was sent
// (TransitionalSince older than IBMRequestTime). id is the worker ID.
func (in ScaleDownVictimsInput) transitionalBeforeCall(id string) bool {
	since, seen := in.TransitionalSince[id]
	return seen && !in.IBMRequestTime.IsZero() && since.Before(in.IBMRequestTime)
}

// workloadNodes is the workload cluster as one pool reconcile sees it.
type workloadNodes struct {
	// client is the workload cluster's client from ClusterCache; nil while ClusterCache is not connected.
	client client.Client
	// nodes are every Node of the workload cluster; nil while not connected.
	nodes []corev1.Node
}

// readWorkloadNodes reads every Node of the workload cluster through ClusterCache (Nodes are cached there). A workload
// cluster that is not connected is no error: the result then has no client, and every step that needs one waits
// (WaitingForWorkloadCluster). ctx is the reconcile context.
func (s *MachinePoolScope) readWorkloadNodes(ctx context.Context) (workloadNodes, error) {
	workload, err := s.params.ClusterCache.GetClient(ctx, client.ObjectKeyFromObject(s.Cluster))
	if errors.Is(err, clustercache.ErrClusterNotConnected) {
		return workloadNodes{}, nil
	}
	if err != nil {
		return workloadNodes{}, fmt.Errorf("getting the workload cluster client of Cluster %s: %w", s.Cluster.Name, err)
	}
	nodes := &corev1.NodeList{}
	if err := workload.List(ctx, nodes); err != nil {
		return workloadNodes{}, fmt.Errorf("listing the Nodes of Cluster %s: %w", s.Cluster.Name, err)
	}
	return workloadNodes{client: workload, nodes: nodes.Items}, nil
}

// markedWorkers returns the sorted IDs of the pool's workers whose Node carries this pool's drain-owner marker. With
// status.disruption empty they are the victims of a removal whose status was lost (a clusterctl move, a status patch
// that never landed), which DecideDisruptionStep resumes from the Node markers instead of starting over or leaving the
// Nodes cordoned (ResumeFromNodeMarkers).
//
// Parameters:
//   - view: the workload cluster's Nodes; none while not connected.
//   - poolID: the IBM pool ID.
//   - workers: every worker of the cluster from this reconcile's listing.
func (s *MachinePoolScope) markedWorkers(view workloadNodes, poolID string, workers []kubernetesservice.Worker) ([]string, error) {
	if len(view.nodes) == 0 {
		return nil, nil
	}
	account, err := AccountIDFromCRN(s.ControlPlane.Status.ClusterCRN)
	if err != nil {
		return nil, fmt.Errorf("finding the Nodes of pool %s: %w", s.Pool.Spec.PoolName, err)
	}
	byProviderID := map[string]string{}
	for _, w := range workers {
		if w.PoolID == poolID && w.ActualState != WorkerStateDeleted {
			byProviderID[ProviderID(account, s.ControlPlane.Status.ClusterID, w.ID)] = w.ID
		}
	}
	var marked []string
	for i := range view.nodes {
		node := &view.nodes[i]
		if id, found := byProviderID[node.Spec.ProviderID]; found && ownsDrainMarker(node, s.Pool) {
			marked = append(marked, id)
		}
	}
	slices.Sort(marked)
	return slices.Compact(marked), nil
}

// ownsDrainMarker reports whether node carries pool's drain-owner marker, which CAPIBM writes with every cordon.
//
// Parameters:
//   - node is the Node.
//   - pool is the IBMCloudManagedMachinePool.
func ownsDrainMarker(node *corev1.Node, pool *infrav1.IBMCloudManagedMachinePool) bool {
	return node.Annotations[infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner] == DrainOwner(pool)
}

// scaleDown is one reconcile of the ScaleDown removal: what was observed, the plan, and the handles that carry out the
// step DecideDisruptionStep chooses.
type scaleDown struct {
	s       *MachinePoolScope
	clients *poolClients
	pool    *kubernetesservice.WorkerPool
	workers []kubernetesservice.Worker
	desired int64

	// resizeOnly is ScaleDownMode == ScaleDownResizeOnly: no prediction-based drain, the fallback for an IBM resize that
	// does not remove the predicted victims.
	resizeOnly bool
	// resumed is true when the victims come from this pool's Node markers because status.disruption was lost.
	resumed bool
	// account is the IBM account of the cluster, for provider IDs.
	account string
	// workload is the workload cluster's Nodes; no client while not connected.
	workload workloadNodes
	// plan and verified are the pure answers of this reconcile.
	plan     ScaleDownPlan
	verified ScaleDownVictimsResult
	// token is the Cluster's disruption token.
	token *DisruptionToken
	// knobs are the pool's drain settings.
	knobs DrainKnobs
	// drain is the drain step; nil while the workload cluster is not connected, and in ResizeOnly mode.
	drain *DrainStep
	// victimNodes are the Nodes of the victims, sorted by name.
	victimNodes []corev1.Node
	// drainResult is the drain observed in this reconcile; evicted tells whether it came from Evict.
	drainResult *DrainStepResult
	evicted     bool
}

// scaleDownInput are the observations reconcileSize hands to the scale-down.
type scaleDownInput struct {
	// clients are this reconcile's IBM clients.
	clients *poolClients
	// pool is the IBM pool.
	pool *kubernetesservice.WorkerPool
	// workers are every worker of the cluster from this reconcile's listing.
	workers []kubernetesservice.Worker
	// desired is the size per zone the MachinePool asks for.
	desired int64
	// workload is the workload cluster's Nodes.
	workload workloadNodes
	// marked are the pool's workers that carry this pool's drain-owner marker (markedWorkers).
	marked []string
	// triggerGone treats the scale-down as no longer wanted (TriggerActive false), whatever desired says: a failing
	// pool guard leaves the MachinePool without a valid size (reconcileGuardBlocked).
	triggerGone bool
}

// reconcileScaleDown runs one step of the scale-down: a new one when the MachinePool asks for fewer workers per zone
// than IBM's pool has, or the one in flight (status.disruption, or this pool's Node markers when the status was lost).
// Every IBM call goes through the clients of the scope's ibm, which resolves the identity lazily from the Cluster's
// IBMCloudManagedCluster. ctx is the reconcile context; in are the observations of reconcileSize.
func (s *MachinePoolScope) reconcileScaleDown(ctx context.Context, in scaleDownInput) Outcome {
	sd, err := s.newScaleDown(in)
	if err != nil {
		return Transient(err)
	}
	switch {
	case len(sd.plan.Victims) > 0:
		return sd.run(ctx)
	case in.triggerGone:
		// A failing guard blocks every IBM mutation (PoolGuardResult), and the resize without victims is one.
		return s.reportReadiness(in.pool, in.workers, in.desired)
	}
	return sd.resizeWithoutVictims(ctx)
}

// newScaleDownInput gathers what the scale-down reads besides IBM's listings: the workload cluster's Nodes and the
// pool's workers whose Nodes carry this pool's drain-owner marker, which reveal a scale-down whose status was lost.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - pool is the IBM pool.
//   - workers are every worker of the cluster from this reconcile's listing.
//   - desired is the size per zone the MachinePool asks for.
func (s *MachinePoolScope) newScaleDownInput(ctx context.Context, clients *poolClients, pool *kubernetesservice.WorkerPool,
	workers []kubernetesservice.Worker, desired int64) (scaleDownInput, error) {
	view, err := s.readWorkloadNodes(ctx)
	if err != nil {
		return scaleDownInput{}, err
	}
	marked, err := s.markedWorkers(view, pool.ID, workers)
	if err != nil {
		return scaleDownInput{}, err
	}
	return scaleDownInput{clients: clients, pool: pool, workers: workers, desired: desired, workload: view, marked: marked}, nil
}

// reconcileGuardBlocked reports a failing pool guard (PoolGuards) once no scale-down of this pool is in flight. A
// scale-down in flight (status.disruption, or this pool's drain-owner markers when the status was lost) first runs on
// with its trigger treated as gone: the MachinePool asks for no valid size, so the scale-down is no longer wanted.
// Before the IBM call it is aborted (uncordon, NodeUncordoned, token released); after the call it follows IBM to its
// end. That makes no IBM mutation, which every failing guard forbids: with TriggerActive false DecideDisruptionStep
// never decides StepCallIBM. Returning the guard's Terminal outcome right away instead would freeze the scale-down with
// its Nodes cordoned and excluded from load balancers and the Cluster's disruption token held, blocking every other
// pool, until a human fixes the spec. While the scale-down runs, the guard's message is added to its WorkerPoolReady
// messages; once it ended, the guard is reported alone.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - pool is the IBM pool.
//   - workers are every worker of the cluster from this reconcile's listing.
//   - guards is PoolGuards' answer; it is blocked.
func (s *MachinePoolScope) reconcileGuardBlocked(ctx context.Context, clients *poolClients, pool *kubernetesservice.WorkerPool,
	workers []kubernetesservice.Worker, guards PoolGuardResult) Outcome {
	inFlight := s.Pool.Status.Disruption.Operation
	if inFlight != "" && inFlight != infrav1.WorkerDisruptionOperationScaleDown {
		// A removal of another operation (a worker replace) settles in its own flow.
		return s.guardOutcome(guards)
	}
	input, err := s.newScaleDownInput(ctx, clients, pool, workers, guards.DesiredSizePerZone)
	if err != nil {
		return Transient(err)
	}
	if inFlight == "" && len(input.marked) == 0 {
		return s.guardOutcome(guards)
	}
	input.triggerGone = true
	notes := s.notes
	s.addNote(guards.Message)
	outcome := s.reconcileScaleDown(ctx, input)
	if outcome.Kind == OutcomeTransient || s.Pool.Status.Disruption.Operation != "" {
		return outcome
	}
	// The scale-down ended: the guard is reported once, not also as a note of its own message.
	s.notes = notes
	return s.guardOutcome(guards)
}

// removalHeldNote returns a WorkerPoolReady note when a guard that stops every IBM call (PoolNameConflict) meets a
// worker removal of this pool in flight, else "". Without IBM's listing the removal can neither be followed nor safely
// given up (IBM may be removing its victims), so it waits as it is; the note tells the operator that resolving the
// conflict also releases the Nodes and the Cluster's disruption token.
//
// Parameters:
//   - guards is PoolGuards' answer.
func (s *MachinePoolScope) removalHeldNote(guards PoolGuardResult) string {
	operation := s.Pool.Status.Disruption.Operation
	if !guards.StopsPublication || operation == "" {
		return ""
	}
	return fmt.Sprintf("the %s removal of this pool is in flight; while the conflict lasts CAPIBM makes no IBM call, so its "+
		"Nodes stay cordoned and the Cluster's disruption token stays held", operation)
}

// newScaleDown observes the scale-down: the victims (recorded, rebuilt from the Node markers, or predicted), the plan,
// IBM's progress, the token and, while the workload cluster is connected, the drain step. in are the observations of
// reconcileSize.
func (s *MachinePoolScope) newScaleDown(in scaleDownInput) (*scaleDown, error) {
	sd := &scaleDown{s: s, clients: in.clients, pool: in.pool, workers: in.workers, desired: in.desired, workload: in.workload,
		resizeOnly: s.scaleDownMode == ScaleDownResizeOnly, knobs: DrainKnobsFor(s.MachinePool, s.Pool)}
	victims := s.Pool.Status.Disruption.WorkerIDs
	if s.Pool.Status.Disruption.Operation == "" {
		victims, sd.resumed = in.marked, len(in.marked) > 0
	}
	plan, err := PlanScaleDown(ScaleDownPlanInput{Workers: in.workers, PoolID: in.pool.ID, Zones: scaleDownZones(in.pool, s.Pool),
		SizePerZone: in.pool.SizePerZone, Desired: in.desired, Victims: victims})
	if err != nil {
		return nil, fmt.Errorf("planning the scale-down of pool %s: %w", s.Pool.Spec.PoolName, err)
	}
	sd.plan = plan
	if in.triggerGone {
		sd.plan.TriggerActive = false
	}
	if sd.account, err = AccountIDFromCRN(s.ControlPlane.Status.ClusterCRN); err != nil {
		return nil, fmt.Errorf("finding the Nodes of pool %s: %w", s.Pool.Spec.PoolName, err)
	}
	if len(plan.Victims) == 0 {
		return sd, nil
	}
	if err := sd.verify(); err != nil {
		return nil, err
	}
	if sd.token, err = s.disruptionToken(); err != nil {
		return nil, err
	}
	sd.victimNodes = VictimNodes(in.workload.nodes, sd.account, s.ControlPlane.Status.ClusterID, plan.Victims)
	if in.workload.client == nil || sd.resizeOnly {
		return sd, nil
	}
	sd.drain, err = NewDrainStep(DrainStepInput{WorkloadClient: in.workload.client, Clock: s.params.Clock, Recorder: s.params.Recorder,
		Pool: s.Pool, Knobs: sd.knobs, RecordedStartTime: s.Pool.Status.Disruption.DrainStartTime.Time})
	return sd, err
}

// verify compares IBM's listing with the victims (VerifyScaleDownVictims).
func (sd *scaleDown) verify() error {
	var published []string
	for _, id := range sd.plan.Victims {
		if slices.Contains(sd.s.Pool.Spec.ProviderIDList, ProviderID(sd.account, sd.s.ControlPlane.Status.ClusterID, id)) {
			published = append(published, id)
		}
	}
	verified, err := VerifyScaleDownVictims(ScaleDownVictimsInput{Workers: sd.workers, PoolID: sd.pool.ID, Victims: sd.plan.Victims,
		StillPublished: published, Now: sd.s.params.Clock.Now(), IBMRequestTime: sd.s.Pool.Status.Disruption.IBMRequestTime.Time,
		TransitionalSince: sd.s.firstSeen})
	if err != nil {
		return fmt.Errorf("verifying the scale-down of pool %s: %w", sd.s.Pool.Spec.PoolName, err)
	}
	sd.verified = verified
	return nil
}

// run observes, decides and carries out one step of the scale-down. ctx is the reconcile context.
func (sd *scaleDown) run(ctx context.Context) Outcome {
	o, err := sd.observation(ctx)
	if err != nil {
		return Transient(err)
	}
	for range maxScaleDownDecisions {
		decision, err := DecideDisruptionStep(o)
		if err != nil {
			return Transient(err)
		}
		if decision.Step != StepAcquireToken {
			return sd.apply(ctx, decision)
		}
		status, err := sd.token.Acquire(ctx)
		if err != nil {
			return Transient(err)
		}
		o.Token = status.State
		if o.Drain, err = sd.drainProgress(ctx, o); err != nil {
			return Transient(err)
		}
	}
	return Transient(errors.New("managed: the scale-down did not settle on a step (programming error)"))
}

// observation is what DecideDisruptionStep reads for the scale-down. ctx is the reconcile context.
func (sd *scaleDown) observation(ctx context.Context) (DisruptionObservation, error) {
	tokenStatus, err := sd.token.Status(ctx)
	if err != nil {
		return DisruptionObservation{}, err
	}
	gate, err := sd.check(false)
	if err != nil {
		return DisruptionObservation{}, err
	}
	preCall, err := sd.preCallCheck()
	if err != nil {
		return DisruptionObservation{}, err
	}
	disruption := sd.s.Pool.Status.Disruption
	o := DisruptionObservation{
		Operation:             infrav1.WorkerDisruptionOperationScaleDown,
		Phase:                 disruption.Phase,
		ResumeFromNodeMarkers: sd.resumed,
		Now:                   sd.s.params.Clock.Now(),
		IBMRequestTime:        disruption.IBMRequestTime.Time,
		LastCallRefused:       sd.lastCallRefused(),
		TriggerActive:         sd.plan.TriggerActive,
		Token:                 tokenStatus.State,
		// As for the pool deletion, a scale-down that needs nothing from the workload cluster (ResizeOnly, or both
		// exclusion annotations) counts it as reachable, so that an unreachable workload cluster does not hold it.
		WorkloadClusterConnected: sd.drain != nil || sd.needsNoWorkloadCluster(),
		Gates: DisruptionGates{
			ClusterStateAllows: sd.clusterStateCheck().Allowed,
			Disruption:         gate,
			Autoscaled:         sd.pool.AutoscaleEnabled,
			OtherDrainOwners:   OtherDrainOwners(sd.workload.nodes, DrainOwner(sd.s.Pool)),
		},
		PreCallGate: preCall,
		IBM:         sd.verified.Progress,
	}
	o.Drain, err = sd.drainProgress(ctx, o)
	return o, err
}

// needsNoWorkloadCluster reports whether the scale-down can run without the workload cluster: ResizeOnly drains
// nothing, and with both exclusion annotations nothing is cordoned, evicted or waited for, which makes the two
// annotations the way to scale down a pool whose workload cluster cannot be reached.
func (sd *scaleDown) needsNoWorkloadCluster() bool {
	return sd.resizeOnly || (sd.knobs.ExcludeDraining && sd.knobs.ExcludeVolumeDetachWait)
}

// check is CheckDisruption for the scale-down's RemovedReady. skipSettleDelay selects the pre-call gate. Both get the
// controller's first-seen times (MachinePoolScopeInput.TransitionalSince), so that a worker IBM has left transitional
// for longer than WorkerStuckAfter, in any pool, no longer holds the scale-down (rule 1 of CheckDisruption). The settle
// delay reads this object's lastDisruptionTime as this reconcile set it: the first observation of a pool (after a
// clusterctl move or a restore) sets it to now, and the listed copy of the pool does not show that yet.
func (sd *scaleDown) check(skipSettleDelay bool) (DisruptionCheckResult, error) {
	pools := slices.Clone(sd.s.clusterPools)
	for i := range pools {
		if pools[i].Name == sd.s.Pool.Name {
			pools[i].Status.LastDisruptionTime = sd.s.Pool.Status.LastDisruptionTime
		}
	}
	return CheckDisruption(DisruptionCheck{
		Operation:         infrav1.WorkerDisruptionOperationScaleDown,
		Distribution:      sd.s.ControlPlane.Spec.Distribution,
		Now:               sd.s.params.Clock.Now(),
		Workers:           sd.workers,
		TransitionalSince: sd.s.firstSeen,
		Pools:             pools,
		RemovedReady:      sd.plan.RemovedReady,
		SkipSettleDelay:   skipSettleDelay,
	})
}

// preCallCheck is the pre-call gate of the resize: the IBM cluster state again, then CheckDisruption without the settle
// delay. The Gated step reads the cluster state only before the first cordon, and a long drain can outlive it: a
// cluster that turned unavailable or scaled_down meanwhile gets no resize, because IBM's refusal would otherwise be
// classified as an invalid configuration (PoolMutationAllowed).
func (sd *scaleDown) preCallCheck() (DisruptionCheckResult, error) {
	if state := sd.clusterStateCheck(); !state.Allowed {
		return state, nil
	}
	return sd.check(true)
}

// clusterStateCheck is the cluster-state gate of a shrink (PoolMutationAllowed with PoolOperationShrink) as a
// DisruptionCheckResult: allowed, or WaitingForClusterState with the state in the message.
func (sd *scaleDown) clusterStateCheck() DisruptionCheckResult {
	cp := sd.s.ControlPlane.Status
	if PoolMutationAllowed(cp.State, PoolOperationShrink) {
		return DisruptionCheckResult{Allowed: true}
	}
	return DisruptionCheckResult{Reason: ReasonWaitingForClusterState, Message: fmt.Sprintf("IBM cluster %s is %s; IBM "+
		"accepts no worker pool change in that state", cp.ClusterID, cp.State)}
}

// lastCallRefused is DisruptionObservation.LastCallRefused for the scale-down: a resize was sent, WorkerPoolReady still
// records IBM's synchronous refusal of it (refusalRecorded), and IBM's pool still has more workers per zone than the
// resize asked for, so nothing carried it out. The condition is the record because status has no field for the answer
// (no API change): callResize reports the answer to every call, and every later step that reports something else
// replaces it. Without the record (a later hold or drain reported over it, or the status patch of the refusal was
// lost) this is false, and a removal whose trigger went away waits WorkerStuckAfter as for a call IBM may still carry
// out, which fails closed.
func (sd *scaleDown) lastCallRefused() bool {
	return !sd.s.Pool.Status.Disruption.IBMRequestTime.IsZero() && sd.pool.SizePerZone > sd.plan.Target &&
		sd.refusalRecorded() != ""
}

// refusalRecorded returns the reason of IBM's synchronous refusal of the latest resize as WorkerPoolReady records it
// (WaitingForClusterMinimum for E1437, ExternallyAutoscaled for the 403 of an autoscaled pool), or "" when it records
// none. The message must carry refusalMessage, which only a refusal writes: the capacity rule of the pre-call gate
// reports WaitingForClusterMinimum too, with CheckDisruption's message, after a call IBM may have accepted.
func (sd *scaleDown) refusalRecorded() DisruptionReason {
	ready := conditions.Get(sd.s.Pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
	if ready == nil || ready.Status != metav1.ConditionFalse {
		return ""
	}
	switch reason := DisruptionReason(ready.Reason); reason {
	case ReasonWaitingForClusterMinimum, ReasonExternallyAutoscaled:
		if strings.Contains(ready.Message, refusalMessage(reason)) {
			return reason
		}
	}
	return ""
}

// drainProgress observes the drain for the effective phase of o: in every phase before the IBM call it checks the
// victims without evicting (DrainStep.Check), and the gates read only the exclusions. Unlike the pool deletion, which
// never aborts, it does not evict while it observes: a scale-down whose MachinePool went back up is aborted, and
// evicting first would only move pods for nothing. The eviction runs once the decision is StepDrain (drainVictims).
// Nothing is cordoned or evicted before this pool holds the disruption token. ResizeOnly drains nothing, so its
// drain counts as excluded.
//
// Parameters:
//   - ctx is the reconcile context.
//   - o is the observation so far.
func (sd *scaleDown) drainProgress(ctx context.Context, o DisruptionObservation) (DrainProgress, error) {
	knobsOnly := DrainProgress{Excluded: sd.knobs.ExcludeDraining || sd.resizeOnly,
		DetachExcluded: sd.knobs.ExcludeVolumeDetachWait || sd.resizeOnly}
	phase := o.Phase
	if phase == "" && o.ResumeFromNodeMarkers {
		phase = infrav1.WorkerDisruptionPhaseDraining
	}
	if sd.resizeOnly || o.Token != TokenHeld || !isPreCallPhase(phase) {
		return knobsOnly, nil
	}
	if sd.drain == nil {
		return sd.disconnectedProgress(), nil
	}
	if sd.drainResult == nil {
		if err := sd.runDrain(ctx, false); err != nil {
			return DrainProgress{}, err
		}
	}
	return sd.drainResult.Progress, nil
}

// announceTimeout emits DrainTimeoutExceeded when the drain leaves phase Draining because its time limit ran out: the
// observation only checked the Nodes, and Evict is what names the pods left on a timed-out Node (it sends no eviction
// there). It runs once, on the step out of Draining. ctx is the reconcile context.
func (sd *scaleDown) announceTimeout(ctx context.Context) error {
	from := sd.s.Pool.Status.Disruption.Phase
	leavesDraining := from == infrav1.WorkerDisruptionPhaseDraining || (from == "" && sd.resumed)
	if !leavesDraining || sd.drain == nil || sd.evicted || sd.drainResult == nil || !sd.drainResult.Progress.TimedOut {
		return nil
	}
	return sd.runDrain(ctx, true)
}

// disconnectedProgress is the drain progress while the workload cluster is not connected: nothing is observed, but the
// exclusions hold and a time limit counted from status.disruption.drainStartTime still runs out, as in Cluster API.
func (sd *scaleDown) disconnectedProgress() DrainProgress {
	now, start := sd.s.params.Clock.Now(), sd.s.Pool.Status.Disruption.DrainStartTime.Time
	return DrainProgress{
		Excluded:       sd.knobs.ExcludeDraining,
		TimedOut:       drain.TimeoutExceeded(now, start, sd.knobs.NodeDrainTimeoutSeconds),
		DetachExcluded: sd.knobs.ExcludeVolumeDetachWait,
		DetachTimedOut: drain.TimeoutExceeded(now, start, sd.knobs.NodeVolumeDetachTimeoutSeconds),
	}
}

// runDrain runs Evict (evict true) or Check on the victim Nodes and keeps the result; without the drain step it keeps
// disconnectedProgress.
//
// Parameters:
//   - ctx is the reconcile context.
//   - evict selects Evict.
func (sd *scaleDown) runDrain(ctx context.Context, evict bool) error {
	if sd.drain == nil {
		sd.drainResult, sd.evicted = &DrainStepResult{Progress: sd.disconnectedProgress()}, sd.evicted || evict
		return nil
	}
	var result DrainStepResult
	var err error
	if evict {
		result, err = sd.drain.Evict(ctx, sd.victimNodes)
	} else {
		result, err = sd.drain.Check(ctx, sd.victimNodes)
	}
	if err != nil {
		return err
	}
	sd.drainResult, sd.evicted = &result, sd.evicted || evict
	return nil
}

// apply carries out the decision of this reconcile: one step of the removal state machine DecideDisruptionStep drives.
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (sd *scaleDown) apply(ctx context.Context, d DisruptionDecision) Outcome {
	switch d.Step {
	case StepNone, StepWait:
		if d.ReleaseToken {
			if err := sd.token.Release(ctx); err != nil {
				return Transient(err)
			}
		}
		return sd.wait(d, IntervalGuarded)
	case StepDrain:
		return sd.drainVictims(ctx, d)
	case StepWaitForVolumeDetach:
		if err := sd.announceTimeout(ctx); err != nil {
			return Transient(err)
		}
		sd.record(d.Phase)
		return sd.wait(d, IntervalDrainRetry)
	case StepHold:
		sd.record(d.Phase)
		return sd.wait(d, IntervalGuarded)
	case StepCallIBM:
		return sd.callResize(ctx, d)
	case StepWaitForIBM:
		sd.record(d.Phase)
		// With the trigger gone, decideTriggerGone's message says what happens (no second call); the refusal's message
		// would promise a resize that is never sent.
		if hold, held := sd.refusalHold(d); held && sd.plan.TriggerActive {
			return sd.wait(hold, IntervalGuarded)
		}
		return sd.wait(d, IntervalPoolTransition)
	case StepWaitForRemoval:
		sd.record(d.Phase)
		return sd.wait(d, IntervalPoolTransition)
	case StepCorrectVictims:
		return sd.correctVictims(ctx, d)
	case StepComplete:
		return sd.finish(ctx, "")
	case StepAbort:
		return sd.abort(ctx, d)
	default:
		// StepHandOver and StepStop belong to a pool or Cluster deletion, which never reach the normal flow;
		// StepWaitForReplacement belongs to a replace.
		return Transient(fmt.Errorf("managed: step %s is not a step of a scale-down (programming error)", d.Step))
	}
}

// drainVictims carries out StepDrain: cordon every victim Node first, so that evicted pods never land on another
// victim, then evict, and record phase Draining. ResizeOnly drains nothing and only moves the removal on. ctx is the
// reconcile context; d is the decision.
func (sd *scaleDown) drainVictims(ctx context.Context, d DisruptionDecision) Outcome {
	if !sd.evicted && !sd.resizeOnly {
		if err := sd.runDrain(ctx, true); err != nil {
			return Transient(err)
		}
	}
	if sd.drainResult != nil && sd.drainResult.Progress.Blocked {
		// The first drain runs after the decision; its refusals are reported at once, so that the next reconcile sees
		// DrainBlocked as the previous reason and does not announce it again (as in the pool deletion).
		d.DrainingReason = ReasonDrainBlocked
	}
	sd.record(d.Phase)
	return sd.wait(d, IntervalDrainRetry)
}

// record writes status.disruption for phase: operation ScaleDown, the victims, the drain start kept or taken from the
// Node markers or now, ibmRequestTime kept. The drain events of the phase change are emitted here.
//
// Parameters:
//   - phase is the phase the step records.
func (sd *scaleDown) record(phase infrav1.WorkerDisruptionPhase) {
	disruption := &sd.s.Pool.Status.Disruption
	from := disruption.Phase
	if from == "" && sd.resumed {
		// The drain started before status.disruption was lost; it is not announced again.
		from = infrav1.WorkerDisruptionPhaseDraining
	}
	disruption.Operation = infrav1.WorkerDisruptionOperationScaleDown
	disruption.Phase = phase
	disruption.WorkerIDs = slices.Clone(sd.plan.Victims)
	if disruption.DrainStartTime.IsZero() {
		start := sd.s.params.Clock.Now()
		if sd.drainResult != nil && !sd.drainResult.StartTime.IsZero() {
			start = sd.drainResult.StartTime
		}
		disruption.DrainStartTime = metav1.NewTime(start)
	}
	sd.drainEvents(from, phase)
}

// drainEvents emits the drain events of a phase change: DrainStarted and DrainCompleted over a connected workload
// cluster, and DrainTimeoutExceeded when a drain moves on without it because its time limit ran out (as in the pool
// deletion). ResizeOnly drains nothing and announces nothing.
//
// Parameters:
//   - from is the phase before the step, as record derived it.
//   - to is the phase the step records.
func (sd *scaleDown) drainEvents(from, to infrav1.WorkerDisruptionPhase) {
	switch {
	case sd.resizeOnly:
	case sd.drain != nil:
		RecordDrainPhaseChange(sd.s.params.Recorder, sd.s.Pool, from, to, len(sd.victimNodes), sd.knobs.ExcludeDraining)
	case from == infrav1.WorkerDisruptionPhaseDraining && to == infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach && !sd.knobs.ExcludeDraining:
		sd.s.params.Recorder.Eventf(sd.s.Pool, corev1.EventTypeWarning, EventDrainTimeoutExceeded,
			"the drain time limit of %ds (MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed since %s "+
				"while the workload cluster is not connected; CAPIBM proceeds without knowing which pods are left on the Nodes of pool %s",
			ptrValue(sd.knobs.NodeDrainTimeoutSeconds), sd.s.Pool.Status.Disruption.DrainStartTime.UTC().Format(time.RFC3339),
			sd.s.Pool.Spec.PoolName)
	}
}

// callResize makes the scale-down's one IBM call: it persists phase WaitingForIBM with ibmRequestTime and
// lastDisruptionTime first, so that a restart never sends the resize twice within DisruptionSettleDelay, then asks IBM
// for ScaleDownPlan.Target workers per zone and maps the answer with DisruptionCallOutcome. A racing E1437 holds as
// WaitingForClusterMinimum with the Nodes cordoned, never an error storm; the resize is repeated at most once per
// DisruptionSettleDelay. The decision is reported before the persist, so that WorkerPoolReady names the answer to the
// latest call (refusalHold reads it). ctx is the reconcile context; d is the decision.
func (sd *scaleDown) callResize(ctx context.Context, d DisruptionDecision) Outcome {
	now := metav1.NewTime(sd.s.params.Clock.Now())
	sd.record(infrav1.WorkerDisruptionPhaseWaitingForIBM)
	sd.s.Pool.Status.Disruption.IBMRequestTime = now
	sd.s.Pool.Status.LastDisruptionTime = now
	sd.report(d)
	if err := sd.s.persist(ctx); err != nil {
		return Transient(fmt.Errorf("persisting the scale-down of pool %s before the IBM call: %w", sd.s.Pool.Spec.PoolName, err))
	}
	clusterID := sd.s.ControlPlane.Status.ClusterID
	err := sd.clients.kubernetes.ResizeWorkerPool(ctx, clusterID, sd.pool.ID, sd.plan.Target)
	result, verr := DisruptionCallOutcome(kubernetesservice.OpResizeWorkerPool, err, sd.pool.AutoscaleEnabled)
	if verr != nil {
		return Transient(verr)
	}
	if !result.Handled {
		return sd.s.ibmProblem(sd.clients, kubernetesservice.OpResizeWorkerPool,
			fmt.Sprintf("shrinking worker pool %s of IBM cluster %s to %d workers per zone", sd.pool.Name, clusterID, sd.plan.Target), err)
	}
	sd.record(result.Decision.Phase)
	if result.Decision.Step == StepHold {
		result.Decision.Message = refusalMessage(result.Decision.Reason)
		return sd.wait(result.Decision, IntervalGuarded)
	}
	return sd.wait(result.Decision, IntervalPoolTransition)
}

// refusalHold keeps the hold of a refused resize while the call waits out DisruptionSettleDelay, as the pool deletion
// does for RemoveWorkerPool. It returns d with the refusal's reason and message, and true, when WorkerPoolReady holds
// IBM's synchronous refusal of the latest call (refusalRecorded): WaitingForClusterMinimum for E1437 (IBM's refusal to
// go below the cluster's minimum workers, which changes nothing) or ExternallyAutoscaled for the 403 of an autoscaled
// pool. callResize reports every call's answer first, so the condition names the answer to that call. Otherwise it
// returns d and false. ibmRequestTime and lastDisruptionTime keep the retry out of the delay; only the report changes,
// so that the reason does not flip to Resizing in between and back, which would write status on every reconcile.
//
// Parameters:
//   - d is the decision of a wait after a call.
func (sd *scaleDown) refusalHold(d DisruptionDecision) (DisruptionDecision, bool) {
	reason := sd.refusalRecorded()
	if reason == "" {
		return d, false
	}
	d.Reason, d.Message, d.DrainingReason = reason, refusalMessage(reason), ""
	return d, true
}

// refusalMessage is the WorkerPoolReady message while IBM's refusal of the resize holds. It is fixed per reason, so
// that the hold writes the same condition on every reconcile, and it carries no IBM text.
//
// Parameters:
//   - reason is the refusal's reason: WaitingForClusterMinimum or ExternallyAutoscaled.
func refusalMessage(reason DisruptionReason) string {
	if reason == ReasonExternallyAutoscaled {
		return "IBM refused the resize because its cluster autoscaler owns this pool; the victim Nodes stay as they are; " +
			"disable the autoscaler of the pool, or set the MachinePool's spec.replicas back"
	}
	return fmt.Sprintf("IBM refused the resize because the cluster would fall below its minimum workers (%s); the victim "+
		"Nodes stay as they are, and the resize is sent again at most once every %d minutes while CAPIBM's own guards allow it",
		ClusterMinimumErrorCode, int(DisruptionSettleDelay.Minutes()))
}

// correctVictims carries out StepCorrectVictims: IBM's resize removes other workers than predicted. It emits the
// Warning UnexpectedScaleDownVictim with both sets, uncordons the spared victims (Uncordon reverts only CAPIBM's
// changes), and records IBM's workers as the victims, so that the scale-down waits for them and not for a spared worker
// that IBM never deletes. When IBM removed nothing at all, nothing is left to wait for and the scale-down ends.
// ResizeOnly drained nothing, so it only records IBM's workers. ctx is the reconcile context; d is the decision.
func (sd *scaleDown) correctVictims(ctx context.Context, d DisruptionDecision) Outcome {
	spared := sd.verified.Spared
	if !sd.resizeOnly {
		if _, outcome, held := sd.uncordon(ctx, spared, d.Phase); held {
			return outcome
		}
		message := fmt.Sprintf("IBM's resize of pool %s removes %s, not the predicted %s; CAPIBM waits for the workers IBM removes",
			sd.s.Pool.Spec.PoolName, noneIfEmptyWorkers(sd.verified.IBMVictims), listWorkers(sd.plan.Victims))
		if len(spared) > 0 {
			message += fmt.Sprintf(" and uncordoned the Nodes of the spared %s", listWorkers(spared))
		}
		sd.s.params.Recorder.Event(sd.s.Pool, corev1.EventTypeWarning, EventUnexpectedScaleDownVictim, message)
	}
	ctrl.LoggerFrom(ctx).Info("IBM removes other workers than the predicted victims", "predicted", sd.plan.Victims,
		"removed", sd.verified.IBMVictims, "spared", spared)
	if len(sd.verified.IBMVictims) == 0 {
		return sd.finish(ctx, "IBM removed none of the pool's workers within the stuck time after the resize")
	}
	sd.plan.Victims = sd.verified.IBMVictims
	sd.record(d.Phase)
	return sd.wait(d, IntervalPoolTransition)
}

// abort carries out StepAbort: the reason for the scale-down went away before its IBM call (spec.replicas went back
// up), or the victims came from Node markers that a resize would not cover. It uncordons what CAPIBM cordoned, emits
// NodeUncordoned and releases the token. ctx is the reconcile context; d is the decision.
func (sd *scaleDown) abort(ctx context.Context, d DisruptionDecision) Outcome {
	reverted, outcome, held := sd.uncordon(ctx, sd.plan.Victims, sd.s.Pool.Status.Disruption.Phase)
	if held {
		return outcome
	}
	if reverted > 0 {
		sd.s.params.Recorder.Eventf(sd.s.Pool, corev1.EventTypeNormal, EventNodeUncordoned,
			"reverted CAPIBM's cordon of %d Nodes of pool %s: %s", reverted, sd.s.Pool.Spec.PoolName, d.Message)
	}
	return sd.finish(ctx, d.Message)
}

// uncordon reverts CAPIBM's cordon of the Nodes of the workers (drain.Helper.Uncordon reverts only CAPIBM's own
// changes, so a Node a human cordoned stays cordoned). It returns how many of the Nodes carried this pool's drain
// markers. It returns a hold, and true, while the workload cluster is not connected and the scale-down needs it: the
// Nodes cannot be uncordoned yet, so nothing is given up.
//
// Parameters:
//   - ctx is the reconcile context.
//   - workerIDs are the workers whose Nodes to uncordon.
//   - phase is the phase to keep while holding.
func (sd *scaleDown) uncordon(ctx context.Context, workerIDs []string, phase infrav1.WorkerDisruptionPhase) (int, Outcome, bool) {
	if sd.resizeOnly || len(workerIDs) == 0 {
		return 0, Outcome{}, false
	}
	if sd.workload.client == nil {
		if sd.needsNoWorkloadCluster() {
			return 0, Outcome{}, false
		}
		hold := DisruptionDecision{Step: StepHold, Phase: phase, Reason: ReasonWaitingForWorkloadCluster,
			Message: "the workload cluster is not connected; CAPIBM uncordons the Nodes it cordoned once it is"}
		if phase != "" {
			sd.record(phase)
		}
		return 0, sd.wait(hold, IntervalGuarded), true
	}
	helper := &drain.Helper{Client: sd.workload.client, Clock: sd.s.params.Clock, EvictionBudget: EvictionBudgetPerReconcile,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	// The victims' Nodes as this reconcile last wrote them: Uncordon patches with the Node's resourceVersion.
	nodes := VictimNodes(sd.victimNodes, sd.account, sd.s.ControlPlane.Status.ClusterID, workerIDs)
	reverted := 0
	for i := range nodes {
		if ownsDrainMarker(&nodes[i], sd.s.Pool) {
			reverted++
		}
		if err := helper.Uncordon(ctx, &nodes[i]); err != nil {
			return 0, Transient(err), true
		}
	}
	return reverted, Outcome{}, false
}

// finish ends the scale-down: it releases the token, clears status.disruption, sets Draining False and reports the
// pool's readiness.
//
// Parameters:
//   - ctx is the reconcile context.
//   - givenUp says why the scale-down was given up, for the log; "" when it completed.
func (sd *scaleDown) finish(ctx context.Context, givenUp string) Outcome {
	if err := sd.token.Release(ctx); err != nil {
		return Transient(err)
	}
	sd.s.Pool.Status.Disruption = infrav1.WorkerDisruptionStatus{}
	conditions.Set(sd.s.Pool, metav1.Condition{Type: infrav1.IBMCloudManagedMachinePoolDrainingCondition,
		Status: metav1.ConditionFalse, Reason: infrav1.IBMCloudManagedMachinePoolNotDrainingReason})
	if givenUp != "" {
		ctrl.LoggerFrom(ctx).Info("The scale-down ends without removing its victims", "victims", sd.plan.Victims, "reason", givenUp)
	}
	return sd.s.reportReadiness(sd.pool, sd.workers, sd.desired)
}

// wait reports the decision on WorkerPoolReady and Draining and returns NotDone after interval.
//
// Parameters:
//   - d is the decision.
//   - interval is the wait.
func (sd *scaleDown) wait(d DisruptionDecision, interval Interval) Outcome {
	sd.report(d)
	return NotDone(interval, string(d.Reason))
}

// report sets WorkerPoolReady=False with the decision's reason and the scale-down's message, and Draining from the
// decision's DrainingReason with the drain message of this reconcile (deterministic, at most 3 pods per category, so a
// repeated reconcile writes nothing new). ResizeOnly drains nothing, so Draining stays False. d is the decision.
func (sd *scaleDown) report(d DisruptionDecision) {
	reason := d.Reason
	if sd.resizeOnly && reason == ReasonDrainingNode {
		// ResizeOnly passes through the drain phases without draining; the pool is being resized.
		reason = ReasonResizing
	}
	if reason != "" {
		sd.s.report(string(reason), sd.message(d))
	}
	draining := metav1.Condition{Type: infrav1.IBMCloudManagedMachinePoolDrainingCondition, Status: metav1.ConditionFalse,
		Reason: infrav1.IBMCloudManagedMachinePoolNotDrainingReason}
	if d.DrainingReason != "" && !sd.resizeOnly {
		draining.Status, draining.Reason, draining.Message = metav1.ConditionTrue, string(d.DrainingReason), d.Message
		if sd.drainResult != nil && sd.drainResult.Message != "" {
			draining.Message = sd.drainResult.Message
		}
	}
	conditions.Set(sd.s.Pool, draining)
}

// message is the WorkerPoolReady message of a scale-down step: it names the victims, then the step. In ResizeOnly mode
// it says that the workers are not drained, so their PodDisruptionBudgets may not be honoured, the documented warning
// of that fallback: the drain messages of the state machine do not apply there. d is the decision.
func (sd *scaleDown) message(d DisruptionDecision) string {
	if sd.resizeOnly {
		step := d.Message
		if d.Step == StepDrain || d.Step == StepWaitForVolumeDetach {
			step = "moving on to the resize"
		}
		return fmt.Sprintf("scale-down of pool %s (ScaleDownMode %s; IBM is expected to remove %s): IBM "+
			"picks the workers and CAPIBM does not drain them first, so their PodDisruptionBudgets may not be honoured; %s",
			sd.s.Pool.Spec.PoolName, ScaleDownResizeOnly, listWorkers(sd.plan.Victims), step)
	}
	return fmt.Sprintf("scale-down of pool %s removing %s: %s", sd.s.Pool.Spec.PoolName, listWorkers(sd.plan.Victims), d.Message)
}

// resizeWithoutVictims shrinks a pool in which no zone holds more workers that IBM lists than the MachinePool asks for
// (IBM has not listed the workers of a growth yet, every worker is gone, or the zones are already short): the resize
// removes no listed worker, so there is nothing to drain and no disruption token is taken. The cluster state, IBM's
// minimum (CheckDisruption with RemovedReady 0) and DisruptionSettleDelay after this pool's last resize still gate the
// call, and lastDisruptionTime is persisted before it, as for every removal call. ctx is the reconcile context.
func (sd *scaleDown) resizeWithoutVictims(ctx context.Context) Outcome {
	if sd.desired >= sd.pool.SizePerZone {
		// Not reached from reconcileSize, which enters a scale-down without victims only for a smaller MachinePool.
		return sd.s.reportReadiness(sd.pool, sd.workers, sd.desired)
	}
	if d, blocked, err := sd.emptyGate(); err != nil || blocked {
		if err != nil {
			return Transient(err)
		}
		sd.s.report(string(d.Reason), fmt.Sprintf("pool %s: %s", sd.pool.Name, d.Message))
		return NotDone(IntervalGuarded, string(d.Reason))
	}
	sd.s.Pool.Status.LastDisruptionTime = metav1.NewTime(sd.s.params.Clock.Now())
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason
	sd.s.report(reason, fmt.Sprintf("no zone of pool %s holds more than %d workers that IBM lists, so the resize removes "+
		"none of them; asking IBM for %d workers per zone", sd.pool.Name, sd.desired, sd.desired))
	if err := sd.s.persist(ctx); err != nil {
		return Transient(fmt.Errorf("persisting the scale-down of pool %s before the IBM call: %w", sd.s.Pool.Spec.PoolName, err))
	}
	clusterID := sd.s.ControlPlane.Status.ClusterID
	err := sd.clients.kubernetes.ResizeWorkerPool(ctx, clusterID, sd.pool.ID, sd.desired)
	result, verr := DisruptionCallOutcome(kubernetesservice.OpResizeWorkerPool, err, sd.pool.AutoscaleEnabled)
	switch {
	case verr != nil:
		return Transient(verr)
	case !result.Handled:
		return sd.s.ibmProblem(sd.clients, kubernetesservice.OpResizeWorkerPool,
			fmt.Sprintf("shrinking worker pool %s of IBM cluster %s to %d workers per zone", sd.pool.Name, clusterID, sd.desired), err)
	case result.Decision.Step == StepHold:
		sd.s.report(string(result.Decision.Reason), fmt.Sprintf("pool %s: %s", sd.pool.Name, refusalMessage(result.Decision.Reason)))
		return NotDone(IntervalGuarded, string(result.Decision.Reason))
	}
	return NotDone(IntervalPoolTransition, reason)
}

// emptyGate returns the wait of a resize without victims, and true, while the last resize of this pool is less than
// DisruptionSettleDelay old, while the cluster state forbids a shrink, or while IBM's minimum would be broken
// (CheckDisruption rule 3; rules 1 and 2 are skipped for a resize that takes no ready worker away).
func (sd *scaleDown) emptyGate() (DisruptionDecision, bool, error) {
	now, last := sd.s.params.Clock.Now(), sd.s.Pool.Status.LastDisruptionTime
	if !last.IsZero() && now.Before(last.Add(DisruptionSettleDelay)) {
		wait, _ := sd.refusalHold(DisruptionDecision{Reason: ReasonResizing, Message: fmt.Sprintf("the resize removes none "+
			"of the workers IBM lists; the pool is resized from %s, %d minutes after its last resize",
			last.Add(DisruptionSettleDelay).UTC().Format(time.RFC3339), int(DisruptionSettleDelay.Minutes()))})
		return wait, true, nil
	}
	if state := sd.clusterStateCheck(); !state.Allowed {
		return DisruptionDecision{Reason: state.Reason, Message: state.Message}, true, nil
	}
	gate, err := sd.check(false)
	if err != nil || gate.Allowed {
		return DisruptionDecision{}, false, err
	}
	return DisruptionDecision{Reason: gate.Reason, Message: gate.Message}, true, nil
}

// noneIfEmptyWorkers names worker IDs in a message like listWorkers, or "no worker" for none.
//
// Parameters:
//   - ids: the sorted worker IDs.
func noneIfEmptyWorkers(ids []string) string {
	if len(ids) == 0 {
		return "no worker"
	}
	return listWorkers(ids)
}
