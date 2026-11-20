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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
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
	// predicted victims. It names both sets. IBM removed exactly the predicted victims when observed on IBM Cloud (IKS,
	// healthy workers on one version); other cases are not yet verified.
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
		if id, found := byProviderID[node.Spec.ProviderID]; found && hasDrainOwner(node, s.Pool) {
			marked = append(marked, id)
		}
	}
	slices.Sort(marked)
	return slices.Compact(marked), nil
}

// scaleDown is one reconcile of the ScaleDown removal: what was observed, the plan, and the handles that carry out the
// step DecideDisruptionStep chooses. The steps every removal shares (recording status.disruption, the drain, the resize
// call and the hold after a refusal) are its runner's (removal_runner.go), which the pool deletion uses too.
type scaleDown struct {
	s       *MachinePoolScope
	clients *poolClients
	pool    *kubernetesservice.WorkerPool
	workers []kubernetesservice.Worker
	desired int64

	// resizeOnly is ScaleDownMode == ScaleDownResizeOnly: no prediction-based drain, the fallback for an IBM resize that
	// does not remove the predicted victims.
	resizeOnly bool
	// account is the IBM account of the cluster, for provider IDs.
	account string
	// workload is the workload cluster's Nodes; no client while not connected.
	workload workloadNodes
	// plan and verified are the pure answers of this reconcile.
	plan     ScaleDownPlan
	verified ScaleDownVictimsResult
	// token is the Cluster's disruption token; nil while the plan has no victims.
	token *DisruptionToken
	// runner carries out the shared removal steps; newScaleDown builds it. Its victims are the plan's until IBM's resize
	// removes other workers (correctVictims).
	runner *removalRunner
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
	if len(sd.plan.Victims) > 0 {
		return sd.run(ctx)
	}
	// No victim: no removal of this pool is in flight, and the resize without victims takes no token.
	if err := s.releaseIdleToken(ctx, in.workload, in.marked); err != nil {
		return Transient(err)
	}
	if in.triggerGone {
		// A failing guard blocks every IBM mutation (PoolGuardResult), and the resize without victims is one.
		return s.reportReadiness(in.pool, in.workers, in.desired)
	}
	return sd.resizeWithoutVictims(ctx)
}

// releaseIdleToken gives the Cluster's disruption token back when this pool holds it while no worker removal of the pool
// is in flight: status.disruption is empty and no Node carries this pool's drain-owner marker. A removal takes the token
// right before its first cordon and records itself only with that cordon; when the cordon fails, or the controller
// stops, or the status patch is lost in between, nothing records the removal, and once its reason goes away (the
// MachinePool's replicas restored, the worker no longer due) no removal step runs again to release the token. Every other
// pool of the Cluster would then wait for it in WaitingForDisruptionToken until a human deleted the Lease. Release is a
// no-op when another pool or no pool holds the token, so the idle paths call this on every reconcile, at the cost of one
// read of the Lease. While the workload cluster is not connected the markers cannot be read, so a removal whose status
// was lost cannot be ruled out and the token is kept, which fails closed.
//
// Parameters:
//   - ctx is the reconcile context.
//   - workload is the workload cluster as this reconcile read it (readWorkloadNodes); no client while not connected.
//   - marked are the pool's workers whose Nodes carry this pool's drain-owner marker (markedWorkers).
func (s *MachinePoolScope) releaseIdleToken(ctx context.Context, workload workloadNodes, marked []string) error {
	if workload.client == nil || s.Pool.Status.Disruption.Operation != "" || len(marked) > 0 {
		return nil
	}
	return s.releaseToken(ctx)
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
// messages; once it ended, the guard is reported alone. A worker replace in flight is settled the same way by its own
// flow (settleReplace).
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
	if inFlight != "" && inFlight != infrav1.WorkerDisruptionOperationScaleDown && inFlight != infrav1.WorkerDisruptionOperationReplace {
		return s.guardOutcome(guards)
	}
	input, err := s.newScaleDownInput(ctx, clients, pool, workers, guards.DesiredSizePerZone)
	if err != nil {
		return Transient(err)
	}
	if inFlight == infrav1.WorkerDisruptionOperationReplace {
		return s.settleReplace(ctx, input, guards)
	}
	if inFlight == "" && len(input.marked) == 0 {
		if err := s.releaseIdleToken(ctx, input.workload, input.marked); err != nil {
			return Transient(err)
		}
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
// the runner with, while the workload cluster is connected, its drain step, and, when the plan has victims, IBM's
// progress and the token. in are the observations of reconcileSize.
func (s *MachinePoolScope) newScaleDown(in scaleDownInput) (*scaleDown, error) {
	sd := &scaleDown{s: s, clients: in.clients, pool: in.pool, workers: in.workers, desired: in.desired, workload: in.workload,
		resizeOnly: s.scaleDownMode == ScaleDownResizeOnly}
	victims, resumed := s.Pool.Status.Disruption.WorkerIDs, false
	if s.Pool.Status.Disruption.Operation == "" {
		victims, resumed = in.marked, len(in.marked) > 0
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
	if sd.runner, err = sd.newRunner(); err != nil {
		return nil, err
	}
	// The victims come from this pool's Node markers because status.disruption was lost.
	sd.runner.resumed = resumed
	if len(plan.Victims) == 0 {
		return sd, nil
	}
	if err := sd.verify(); err != nil {
		return nil, err
	}
	if sd.token, err = s.disruptionToken(); err != nil {
		return nil, err
	}
	return sd, nil
}

// newRunner builds the scale-down's runner (removal_runner.go): the plan's victims and their Nodes, the resize to the
// plan's target, reports on WorkerPoolReady in the scale-down's words (reportReady), IBM's refusal recorded there with
// refusalMessage, which only a refusal writes (lastCallRefused tells it apart from the pre-call gate's capacity hold
// that way), and the cluster state the control plane observed, for the cluster-state gate. ResizeOnly drains nothing.
func (sd *scaleDown) newRunner() (*removalRunner, error) {
	clusterID := sd.s.ControlPlane.Status.ClusterID
	return newRemovalRunner(removalRunnerInput{
		scope: sd.s, clients: sd.clients, pool: sd.pool,
		operation: infrav1.WorkerDisruptionOperationScaleDown, victims: sd.plan.Victims, clusterState: sd.s.ControlPlane.Status.State,
		call: sd.resizeCall(sd.plan.Target),
		refusal: refusalRecord{condition: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition, status: metav1.ConditionFalse,
			message: refusalMessage},
		report: sd.reportReady, drainsNothing: sd.resizeOnly, workload: sd.workload.client,
		victimNodes: VictimNodes(sd.workload.nodes, sd.account, clusterID, sd.plan.Victims),
	})
}

// resizeCall is the scale-down's one IBM call: ResizeWorkerPool to target workers per zone. CAPIBM never calls
// removeWorker, which would switch IBM's auto-rebalance off for the pool, and never sends allowSingleOpenShiftWorker
// (kubernetesservice.ResizeWorkerPool leaves it unset).
//
// Parameters:
//   - target is the size per zone the resize asks IBM for.
func (sd *scaleDown) resizeCall(target int64) removalCall {
	clusterID, poolID, kubernetes := sd.s.ControlPlane.Status.ClusterID, sd.pool.ID, sd.clients.kubernetes
	return removalCall{
		op:     kubernetesservice.OpResizeWorkerPool,
		action: fmt.Sprintf("shrinking worker pool %s of IBM cluster %s to %d workers per zone", sd.pool.Name, clusterID, target),
		send: func(ctx context.Context) error {
			return kubernetes.ResizeWorkerPool(ctx, clusterID, poolID, target)
		},
	}
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
		if o.Drain, err = sd.runner.checkDrain(ctx, o); err != nil {
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
	// The drain-owner markers of a pool that no longer exists hold no drain: they are left out of the gate, and the drain
	// step takes them over on a victim Node.
	owners, err := sd.runner.drainingPools(ctx, sd.token, sd.workload.nodes)
	if err != nil {
		return DisruptionObservation{}, err
	}
	gate, err := sd.check(false)
	if err != nil {
		return DisruptionObservation{}, err
	}
	// The pre-call gate reads the IBM cluster state again before CheckDisruption without the settle delay: the Gated step
	// reads it only before the first cordon, and a long drain can outlive it. A cluster that turned unavailable or
	// scaled_down meanwhile gets no resize, because IBM's refusal would otherwise be classified as an invalid
	// configuration (PoolMutationAllowed).
	preCall, err := sd.runner.preCallGate(sd.check)
	if err != nil {
		return DisruptionObservation{}, err
	}
	disruption := sd.s.Pool.Status.Disruption
	o := DisruptionObservation{
		Operation:             infrav1.WorkerDisruptionOperationScaleDown,
		Phase:                 disruption.Phase,
		ResumeFromNodeMarkers: sd.runner.resumed,
		Now:                   sd.s.params.Clock.Now(),
		IBMRequestTime:        disruption.IBMRequestTime.Time,
		LastCallRefused:       sd.lastCallRefused(),
		TriggerActive:         sd.plan.TriggerActive,
		Token:                 tokenStatus.State,
		// As for the pool deletion, a scale-down that needs nothing from the workload cluster (ResizeOnly, or both
		// exclusion annotations) counts it as reachable, so that an unreachable workload cluster does not hold it.
		WorkloadClusterConnected: sd.runner.drain != nil || sd.needsNoWorkloadCluster(),
		Gates: DisruptionGates{
			ClusterStateAllows: sd.runner.clusterStateCheck().Allowed,
			Disruption:         gate,
			Autoscaled:         sd.pool.AutoscaleEnabled,
			OtherDrainOwners:   owners,
		},
		PreCallGate: preCall,
		IBM:         sd.verified.Progress,
	}
	o.Drain, err = sd.runner.checkDrain(ctx, o)
	return o, err
}

// needsNoWorkloadCluster reports whether the scale-down can run without the workload cluster: ResizeOnly drains
// nothing, and with both exclusion annotations nothing is cordoned, evicted or waited for, which makes the two
// annotations the way to scale down a pool whose workload cluster cannot be reached.
func (sd *scaleDown) needsNoWorkloadCluster() bool {
	return sd.resizeOnly || (sd.runner.knobs.ExcludeDraining && sd.runner.knobs.ExcludeVolumeDetachWait)
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

// lastCallRefused is DisruptionObservation.LastCallRefused for the scale-down: a resize was sent, WorkerPoolReady still
// records IBM's synchronous refusal of it (the runner's recordedRefusal, whose record needs refusalMessage), and IBM's
// pool still has more workers per zone than the resize asked for, so nothing carried it out. The condition is the
// record because status has no field for the answer (no API change): the runner's callIBM reports the answer to every
// call, and every later step that reports something else replaces it. The message must carry refusalMessage, which only
// a refusal writes: the capacity rule of the pre-call gate reports WaitingForClusterMinimum too, with CheckDisruption's
// message, after a call IBM may have accepted. Without the record (a later hold or drain reported over it, or the
// status patch of the refusal was lost) this is false, and a removal whose trigger went away waits WorkerStuckAfter as
// for a call IBM may still carry out, which fails closed.
func (sd *scaleDown) lastCallRefused() bool {
	return !sd.s.Pool.Status.Disruption.IBMRequestTime.IsZero() && sd.pool.SizePerZone > sd.plan.Target &&
		sd.runner.recordedRefusal() != ""
}

// apply carries out the decision of this reconcile: one step of the removal state machine DecideDisruptionStep drives.
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (sd *scaleDown) apply(ctx context.Context, d DisruptionDecision) Outcome {
	runner := sd.runner
	switch d.Step {
	case StepNone, StepWait:
		if d.ReleaseToken {
			if err := sd.token.Release(ctx); err != nil {
				return Transient(err)
			}
		}
		return runner.wait(d, IntervalGuarded)
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
		// The resize to ScaleDownPlan.Target. A racing E1437 holds as WaitingForClusterMinimum with the Nodes cordoned and is
		// sent again at most once per DisruptionSettleDelay, never an error storm.
		return runner.callIBM(ctx, d)
	case StepWaitForIBM:
		runner.record(d.Phase)
		// With the trigger gone, decideTriggerGone's message says what happens (no second call); the refusal's message
		// would promise a resize that is never sent.
		if hold, held := runner.refusalHold(d); held && sd.plan.TriggerActive {
			return runner.wait(hold, IntervalGuarded)
		}
		return runner.wait(d, IntervalPoolTransition)
	case StepWaitForRemoval:
		runner.record(d.Phase)
		return runner.wait(d, IntervalPoolTransition)
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

// refusalMessage is the WorkerPoolReady message while IBM's refusal of the resize holds, and the scale-down's refusal
// record (refusalRecord.message). It is fixed per reason, so that the hold writes the same condition on every
// reconcile, and it carries no IBM text.
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
	predicted := sd.runner.victims
	if !sd.resizeOnly {
		if _, outcome, held := sd.runner.uncordon(ctx, sd.account, spared, d.Phase, sd.needsNoWorkloadCluster()); held {
			return outcome
		}
		message := fmt.Sprintf("IBM's resize of pool %s removes %s, not the predicted %s; CAPIBM waits for the workers IBM removes",
			sd.s.Pool.Spec.PoolName, noneIfEmptyWorkers(sd.verified.IBMVictims), listWorkers(predicted))
		if len(spared) > 0 {
			message += fmt.Sprintf(" and uncordoned the Nodes of the spared %s", listWorkers(spared))
		}
		sd.s.params.Recorder.Event(sd.s.Pool, corev1.EventTypeWarning, EventUnexpectedScaleDownVictim, message)
	}
	ctrl.LoggerFrom(ctx).Info("IBM removes other workers than the predicted victims", "predicted", predicted,
		"removed", sd.verified.IBMVictims, "spared", spared)
	if len(sd.verified.IBMVictims) == 0 {
		return sd.finish(ctx, "IBM removed none of the pool's workers within the stuck time after the resize")
	}
	sd.runner.victims = sd.verified.IBMVictims
	sd.runner.record(d.Phase)
	return sd.runner.wait(d, IntervalPoolTransition)
}

// abort carries out StepAbort: the reason for the scale-down went away before its IBM call (spec.replicas went back
// up), or the victims came from Node markers that a resize would not cover. It uncordons what CAPIBM cordoned, emits
// NodeUncordoned and releases the token. ctx is the reconcile context; d is the decision.
func (sd *scaleDown) abort(ctx context.Context, d DisruptionDecision) Outcome {
	reverted, outcome, held := sd.runner.uncordon(ctx, sd.account, sd.runner.victims, sd.s.Pool.Status.Disruption.Phase,
		sd.needsNoWorkloadCluster())
	if held {
		return outcome
	}
	if reverted > 0 {
		sd.s.params.Recorder.Eventf(sd.s.Pool, corev1.EventTypeNormal, EventNodeUncordoned,
			"reverted CAPIBM's cordon of %d Nodes of pool %s: %s", reverted, sd.s.Pool.Spec.PoolName, d.Message)
	}
	return sd.finish(ctx, d.Message)
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
		ctrl.LoggerFrom(ctx).Info("The scale-down ends without removing its victims", "victims", sd.runner.victims, "reason", givenUp)
	}
	return sd.s.reportReadiness(sd.pool, sd.workers, sd.desired)
}

// reportReady reports a decision on WorkerPoolReady=False with the decision's reason and the scale-down's message
// (message); a decision without a reason leaves WorkerPoolReady as it is. It is the runner's report of the scale-down,
// which adds the Draining condition. d is the decision.
func (sd *scaleDown) reportReady(d DisruptionDecision) {
	reason := d.Reason
	if sd.resizeOnly && reason == ReasonDrainingNode {
		// ResizeOnly passes through the drain phases without draining; the pool is being resized.
		reason = ReasonResizing
	}
	if reason != "" {
		sd.s.report(string(reason), sd.message(d))
	}
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
			sd.s.Pool.Spec.PoolName, ScaleDownResizeOnly, listWorkers(sd.runner.victims), step)
	}
	return fmt.Sprintf("scale-down of pool %s removing %s: %s", sd.s.Pool.Spec.PoolName, listWorkers(sd.runner.victims), d.Message)
}

// resizeWithoutVictims shrinks a pool in which no zone holds more workers that IBM lists than the MachinePool asks for
// (IBM has not listed the workers of a growth yet, every worker is gone, or the zones are already short): the resize
// removes no listed worker, so there is nothing to drain and no disruption token is taken. The cluster state, IBM's
// minimum (CheckDisruption with RemovedReady 0) and DisruptionSettleDelay after this pool's last resize still gate the
// call, and lastDisruptionTime is persisted before it, as for every removal call (the runner's sendCall). ctx is the
// reconcile context.
func (sd *scaleDown) resizeWithoutVictims(ctx context.Context) Outcome {
	if sd.desired >= sd.pool.SizePerZone {
		// Not reached from reconcileSize, which enters a scale-down without victims only for a smaller MachinePool.
		return sd.s.reportReadiness(sd.pool, sd.workers, sd.desired)
	}
	if d, blocked, err := sd.emptyGate(); err != nil || blocked {
		if err != nil {
			return Transient(err)
		}
		// WorkerPoolReady already names IBM's answer to the last resize while the gate keeps it (keepsCallError); the
		// prefix would wrap it once more on every reconcile.
		if !keepsCallError(d) {
			sd.s.report(string(d.Reason), fmt.Sprintf("pool %s: %s", sd.pool.Name, d.Message))
		}
		return NotDone(IntervalGuarded, string(d.Reason))
	}
	sd.s.Pool.Status.LastDisruptionTime = metav1.NewTime(sd.s.params.Clock.Now())
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason
	sd.s.report(reason, fmt.Sprintf("no zone of pool %s holds more than %d workers that IBM lists, so the resize removes "+
		"none of them; asking IBM for %d workers per zone", sd.pool.Name, sd.desired, sd.desired))
	result, outcome, done := sd.runner.sendCall(ctx, sd.resizeCall(sd.desired))
	switch {
	case done:
		return outcome
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
		wait, _ := sd.runner.refusalHold(DisruptionDecision{Reason: ReasonResizing, Message: fmt.Sprintf("the resize removes none "+
			"of the workers IBM lists; the pool is resized from %s, %d minutes after its last resize",
			last.Add(DisruptionSettleDelay).UTC().Format(time.RFC3339), int(DisruptionSettleDelay.Minutes()))})
		return wait, true, nil
	}
	if state := sd.runner.clusterStateCheck(); !state.Allowed {
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
