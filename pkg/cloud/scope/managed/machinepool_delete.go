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

	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
)

// This file is the deletion of an IBMCloudManagedMachinePool. When its Cluster is not being deleted, the IBM pool is
// removed only after every Node of the pool was drained (IBM is never asked to remove a worker whose Node still runs
// evictable pods, unless a drain time limit ran out or drain is excluded), behind the cluster-wide disruption guards
// and the Cluster's disruption token (one disruption per Cluster at a time), as one DeletePool removal that
// DecideDisruptionStep advances one step per reconcile. When the Cluster, the control plane or a clusterctl move
// deletes the pool, nothing is drained and no IBM call is made: the IBM cluster deletion takes every pool with it (the
// CAPZ pattern; CAPI does not drain during a Cluster deletion either).

// EventPoolNotRemoved is the Warning event of a deletion that leaves the IBM pool alone because it is not this
// object's to remove: another IBMCloudManagedMachinePool still claims the name, or the IBM pool has another flavor.
const EventPoolNotRemoved = "WorkerPoolNotRemoved"

// ReconcileDelete runs the pool deletion once. It returns the outcome and whether the controller may remove the
// finalizer. Every skip path decides from management-cluster objects only: no IBM and no workload-cluster call, no
// credential resolution. ctx is the reconcile context.
func (s *MachinePoolScope) ReconcileDelete(ctx context.Context) (Outcome, bool) {
	if err := s.loadOwners(ctx); err != nil {
		return Transient(err), false
	}
	if why := s.deleteSkipReason(); why != "" {
		ctrl.LoggerFrom(ctx).Info("Removing the finalizer without touching the IBM pool", "reason", why)
		return Done(), true
	}
	if others := s.liveClaimants(); len(others) > 0 {
		s.params.Recorder.Eventf(s.Pool, corev1.EventTypeWarning, EventPoolNotRemoved, "IBM worker pool %s stays: "+
			"IBMCloudManagedMachinePool %s still claims it", s.Pool.Spec.PoolName, boundedList(others))
		return Done(), true
	}
	if s.ControlPlane.Status.ClusterID == "" {
		return s.deleteWithoutClusterID()
	}
	if s.InfraCluster == nil {
		reason := infrav1.IBMCloudManagedMachinePoolDeletingWaitingForInfrastructureReason
		s.report(reason, fmt.Sprintf("IBM cluster %s exists, but IBMCloudManagedCluster %s, which holds the credentials "+
			"of its IBM calls, is gone; restore it to remove pool %s", s.ControlPlane.Status.ClusterID,
			s.Cluster.Spec.InfrastructureRef.Name, s.Pool.Spec.PoolName))
		return NotDone(IntervalSteadyState, reason), false
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.credentialsProblem(err), false
	}
	state, outcome, done := s.observeClusterForDelete(ctx, clients)
	if done {
		return outcome, outcome.Kind == OutcomeDone
	}
	pool, outcome, done := s.poolToRemove(ctx, clients)
	if done {
		return outcome, outcome.Kind == OutcomeDone
	}
	workers, err := clients.kubernetes.ListWorkers(ctx, s.ControlPlane.Status.ClusterID)
	if err != nil {
		return s.ibmProblem(clients, kubernetesservice.OpListWorkers,
			fmt.Sprintf("listing the workers of IBM cluster %s", s.ControlPlane.Status.ClusterID), err), false
	}
	if err := s.publishProviderIDs(pool.ID, workers); err != nil {
		// The list only shrinks as IBM reports workers deleted; an unreadable list stays as it is.
		ctrl.LoggerFrom(ctx).Info("spec.providerIDList is left unchanged while the pool is deleted", "cause", err.Error())
	}
	removal := &poolRemoval{s: s, clients: clients, pool: pool, workers: workers, clusterState: state}
	return removal.run(ctx)
}

// deleteSkipReason returns why the deletion leaves the IBM pool alone and removes the finalizer at once, or "": a
// clusterctl move deletes the source object (delete-for-move), the Cluster is being deleted, or the control plane is
// gone or being deleted. In each case the IBM cluster deletion, or the moved object, takes care of the pool, and a
// drain would only push pods onto Nodes that are going away too.
func (s *MachinePoolScope) deleteSkipReason() string {
	if hasAnnotation(s.Pool, clusterctlv1.DeleteForMoveAnnotation) {
		return "clusterctl move deletes the source object"
	}
	if why := s.clusterDeletingReason(); why != "" {
		return why
	}
	if s.ControlPlane == nil {
		return "the control plane is gone"
	}
	return ""
}

// liveClaimants returns the sorted names of the other IBMCloudManagedMachinePools of the Cluster that claim the same
// IBM pool and are not being deleted: the IBM pool goes only with its last claimant.
func (s *MachinePoolScope) liveClaimants() []string {
	var others []string
	for i := range s.clusterPools {
		other := &s.clusterPools[i]
		if other.Name == s.Pool.Name || other.Spec.PoolName != s.Pool.Spec.PoolName || !other.DeletionTimestamp.IsZero() {
			continue
		}
		others = append(others, other.Name)
	}
	slices.Sort(others)
	return others
}

// deleteWithoutClusterID decides a deletion while the control plane knows no IBM cluster ID: with an endpoint the
// cluster exists and the control plane has not observed it again yet (after a clusterctl move), so the deletion waits;
// without one no IBM cluster was ever created and nothing is left to remove.
func (s *MachinePoolScope) deleteWithoutClusterID() (Outcome, bool) {
	if s.ControlPlane.Spec.ControlPlaneEndpoint.Host == "" {
		return Done(), true
	}
	// The Deleting condition has no WaitingForControlPlane reason of its own; the token is WorkerPoolReady's.
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason
	s.report(reason, fmt.Sprintf("IBMCloudManagedControlPlane %s has an endpoint but no IBM cluster ID yet; pool %s is "+
		"removed once the control plane has observed the IBM cluster again", s.ControlPlane.Name, s.Pool.Spec.PoolName))
	return NotDone(IntervalClusterProvisioning, reason), false
}

// observeClusterForDelete reads the IBM cluster and returns its state. A cluster that is gone (404) or that IBM is
// deleting takes the pool with it, so the deletion ends.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
func (s *MachinePoolScope) observeClusterForDelete(ctx context.Context, clients *poolClients) (string, Outcome, bool) {
	clusterID := s.ControlPlane.Status.ClusterID
	cluster, err := clients.kubernetes.GetCluster(ctx, clusterID, s.InfraCluster.Spec.ResourceGroup.ID)
	switch {
	case kubernetesservice.Classify(kubernetesservice.OpGetCluster, err) == kubernetesservice.ClassNotFound:
		return "", Done(), true
	case err != nil:
		return "", s.ibmProblem(clients, kubernetesservice.OpGetCluster, fmt.Sprintf("reading IBM cluster %s", clusterID), err), true
	case cluster == nil:
		return "", Transient(fmt.Errorf("reading IBM cluster %s: IBM Cloud answered without a cluster", clusterID)), true
	case ClassifyClusterState(cluster.State) == ClusterStateGoneOutOfBand:
		return "", Done(), true
	}
	return cluster.State, Outcome{}, false
}

// poolToRemove finds the IBM pool to remove. When IBM no longer lists it, or lists it as deleted, the deletion is
// complete: the token is released (it is held only by a removal of this pool) and the finalizer may go. A pool of
// another flavor is not this object's: it stays, with a Warning event.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
func (s *MachinePoolScope) poolToRemove(ctx context.Context, clients *poolClients) (*kubernetesservice.WorkerPool, Outcome, bool) {
	clusterID := s.ControlPlane.Status.ClusterID
	pools, err := clients.kubernetes.ListWorkerPools(ctx, clusterID)
	if err != nil {
		return nil, s.ibmProblem(clients, kubernetesservice.OpListWorkerPools,
			fmt.Sprintf("listing the worker pools of IBM cluster %s", clusterID), err), true
	}
	pool := poolNamed(pools, s.Pool.Spec.PoolName)
	if pool == nil || pool.ActualState == WorkerStateDeleted {
		if err := s.releaseToken(ctx); err != nil {
			return nil, Transient(err), true
		}
		s.Pool.Status.Disruption = infrav1.WorkerDisruptionStatus{}
		return nil, Done(), true
	}
	if pool.Flavor != s.Pool.Spec.Flavor {
		s.params.Recorder.Eventf(s.Pool, corev1.EventTypeWarning, EventPoolNotRemoved, "IBM worker pool %s stays: it has "+
			"flavor %s, not this object's %s, so it is not this object's pool", pool.Name, pool.Flavor, s.Pool.Spec.Flavor)
		return nil, Done(), true
	}
	return pool, Outcome{}, false
}

// disruptionToken returns the handle of the Cluster's disruption token for this pool.
func (s *MachinePoolScope) disruptionToken() (*DisruptionToken, error) {
	return NewDisruptionToken(DisruptionTokenParams{Client: s.params.Client, APIReader: s.params.APIReader,
		Clock: s.params.Clock, Cluster: s.Cluster, Pool: s.Pool})
}

// releaseToken releases the disruption token when this pool holds it; another pool's token is never touched. ctx is
// the reconcile context.
func (s *MachinePoolScope) releaseToken(ctx context.Context) error {
	token, err := s.disruptionToken()
	if err != nil {
		return err
	}
	return token.Release(ctx)
}

// poolRemoval is one reconcile of the DeletePool removal: what was observed, and the handles that carry out the step
// DecideDisruptionStep chooses.
type poolRemoval struct {
	s            *MachinePoolScope
	clients      *poolClients
	pool         *kubernetesservice.WorkerPool
	workers      []kubernetesservice.Worker
	clusterState string

	// victims are the IDs of the pool's workers that IBM does not list as deleted, sorted.
	victims []string
	// token is the Cluster's disruption token.
	token *DisruptionToken
	// knobs are the pool's drain settings.
	knobs DrainKnobs
	// drain is the drain step; nil while the workload cluster is not connected.
	drain *DrainStep
	// allNodes are every Node of the workload cluster, victimNodes those of the pool's workers.
	allNodes, victimNodes []corev1.Node
	// drainResult is the drain observed in this reconcile; evicted tells whether it came from Evict.
	drainResult *DrainStepResult
	evicted     bool
	// resumed is true when the drain resumes from this pool's Node markers after status.disruption was lost.
	resumed bool
}

// maxDeleteDecisions bounds the decisions of one reconcile: acquiring the token and handing a replace or scale-down
// over each ask for a new decision, and neither can repeat.
const maxDeleteDecisions = 3

// run observes, decides and carries out one step of the removal. ctx is the reconcile context.
func (r *poolRemoval) run(ctx context.Context) (Outcome, bool) {
	if err := r.observe(ctx); err != nil {
		return Transient(err), false
	}
	if len(r.victims) == 0 {
		return r.removeEmptyPool(ctx)
	}
	o, err := r.observation(ctx)
	if err != nil {
		return Transient(err), false
	}
	for range maxDeleteDecisions {
		decision, err := DecideDisruptionStep(o)
		if err != nil {
			return Transient(err), false
		}
		switch decision.Step {
		case StepAcquireToken:
			status, err := r.token.Acquire(ctx)
			if err != nil {
				return Transient(err), false
			}
			o.Token = status.State
		case StepHandOver:
			// A replace or scale-down of this pool becomes the pool deletion, in any phase, keeping every cordon and
			// the token.
			r.handOver(decision.Phase)
			o.Operation, o.Phase = infrav1.WorkerDisruptionOperationDeletePool, decision.Phase
		default:
			return r.apply(ctx, decision)
		}
		if o.Drain, err = r.drainProgress(ctx, o); err != nil {
			return Transient(err), false
		}
	}
	return Transient(errors.New("managed: the pool deletion did not settle on a step (programming error)")), false
}

// observe lists the victims, builds the token handle and, when the workload cluster is connected, the drain step and
// the Nodes. ctx is the reconcile context.
func (r *poolRemoval) observe(ctx context.Context) error {
	for _, w := range r.workers {
		if w.PoolID == r.pool.ID && w.ActualState != WorkerStateDeleted && !slices.Contains(r.victims, w.ID) {
			r.victims = append(r.victims, w.ID)
		}
	}
	slices.Sort(r.victims)
	token, err := r.s.disruptionToken()
	if err != nil {
		return err
	}
	r.token = token
	r.knobs = DrainKnobsFor(r.s.MachinePool, r.s.Pool)
	workload, err := r.s.params.ClusterCache.GetClient(ctx, client.ObjectKeyFromObject(r.s.Cluster))
	if errors.Is(err, clustercache.ErrClusterNotConnected) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("getting the workload cluster client of Cluster %s: %w", r.s.Cluster.Name, err)
	}
	nodes := &corev1.NodeList{}
	if err := workload.List(ctx, nodes); err != nil {
		return fmt.Errorf("listing the Nodes of Cluster %s: %w", r.s.Cluster.Name, err)
	}
	account, err := AccountIDFromCRN(r.s.ControlPlane.Status.ClusterCRN)
	if err != nil {
		return fmt.Errorf("finding the Nodes of pool %s: %w", r.s.Pool.Spec.PoolName, err)
	}
	r.allNodes = nodes.Items
	r.victimNodes = VictimNodes(nodes.Items, account, r.s.ControlPlane.Status.ClusterID, r.victims)
	r.drain, err = NewDrainStep(DrainStepInput{WorkloadClient: workload, Clock: r.s.params.Clock, Recorder: r.s.params.Recorder,
		Pool: r.s.Pool, Knobs: r.knobs, RecordedStartTime: r.s.Pool.Status.Disruption.DrainStartTime.Time})
	return err
}

// needsNoWorkloadCluster reports whether the removal can run without the workload cluster: with both exclusion
// annotations set nothing is cordoned, evicted or waited for, and a Cluster API Machine with both is deleted without
// its workload cluster too. It keeps the documented escape hatch open for a pool whose workload cluster is unreachable
// (a private-only cluster, an expired admin certificate after a long pause, a broken API server). Another pool's
// drain-owner marker cannot be read then; the token and CheckDisruption still apply.
func (r *poolRemoval) needsNoWorkloadCluster() bool {
	return r.knobs.ExcludeDraining && r.knobs.ExcludeVolumeDetachWait
}

// observation is what DecideDisruptionStep reads for the removal. ctx is the reconcile context.
func (r *poolRemoval) observation(ctx context.Context) (DisruptionObservation, error) {
	disruption := r.s.Pool.Status.Disruption
	operation := disruption.Operation
	if operation == "" {
		operation = infrav1.WorkerDisruptionOperationDeletePool
	}
	tokenStatus, err := r.token.Status(ctx)
	if err != nil {
		return DisruptionObservation{}, err
	}
	gate, err := r.check(false)
	if err != nil {
		return DisruptionObservation{}, err
	}
	preCall, err := r.check(true)
	if err != nil {
		return DisruptionObservation{}, err
	}
	o := DisruptionObservation{
		Operation:             operation,
		Phase:                 disruption.Phase,
		ResumeFromNodeMarkers: disruption.Phase == "" && r.ownsADrain(),
		Now:                   r.s.params.Clock.Now(),
		IBMRequestTime:        disruption.IBMRequestTime.Time,
		PoolDeleting:          true,
		TriggerActive:         true,
		Token:                 tokenStatus.State,
		// The Gated step of DecideDisruptionStep holds while this is false. A removal that needs nothing from the
		// workload cluster reports it as reachable for what it does, so that an unreachable workload cluster does not
		// hold it for ever; with both exclusions every later step counts the drain and the detach wait as finished
		// before it looks at the connection.
		WorkloadClusterConnected: r.drain != nil || r.needsNoWorkloadCluster(),
		Gates: DisruptionGates{
			ClusterStateAllows: PoolMutationAllowed(r.clusterState, PoolOperationRemove),
			Disruption:         gate,
			OtherDrainOwners:   OtherDrainOwners(r.allNodes, DrainOwner(r.s.Pool)),
		},
		PreCallGate: preCall,
		IBM:         RemovalProgress{Removing: slices.Contains(PoolNameReleasedStates(), r.pool.ActualState)},
	}
	r.resumed = o.ResumeFromNodeMarkers
	o.Drain, err = r.drainProgress(ctx, o)
	return o, err
}

// check is CheckDisruption for removing every ready worker of the pool. skipSettleDelay selects the pre-call gate,
// which leaves out the settle delay.
func (r *poolRemoval) check(skipSettleDelay bool) (DisruptionCheckResult, error) {
	ready := 0
	for _, w := range r.workers {
		if w.PoolID == r.pool.ID && IsWorkerReady(w) {
			ready++
		}
	}
	return CheckDisruption(DisruptionCheck{
		Operation:       infrav1.WorkerDisruptionOperationDeletePool,
		Distribution:    r.s.ControlPlane.Spec.Distribution,
		Now:             r.s.params.Clock.Now(),
		Workers:         r.workers,
		Pools:           r.s.clusterPools,
		RemovedReady:    ready,
		SkipSettleDelay: skipSettleDelay,
	})
}

// ownsADrain reports whether a victim Node carries this pool's drain-owner marker: a clusterctl move or a lost status
// patch cleared status.disruption mid-drain, and the drain resumes instead of starting over.
func (r *poolRemoval) ownsADrain() bool {
	key := infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner
	owner := DrainOwner(r.s.Pool)
	for i := range r.victimNodes {
		if r.victimNodes[i].Annotations[key] == owner {
			return true
		}
	}
	return false
}

// drainProgress observes the drain for the effective phase of o: Draining evicts, the phases after it check without
// evicting, and earlier or later phases need no drain observation. Nothing is cordoned or evicted before this pool
// holds the disruption token, so that a Cluster has one disruption at a time; DecideDisruptionStep asks for the token
// before it reads the drain. The exclusions are known without the workload cluster, and so are the time limits once a
// drain start is recorded (disconnectedProgress). Each kind of observation runs at most once per reconcile.
//
// Parameters:
//   - ctx is the reconcile context.
//   - o is the observation so far.
func (r *poolRemoval) drainProgress(ctx context.Context, o DisruptionObservation) (DrainProgress, error) {
	phase := o.Phase
	if phase == "" && o.ResumeFromNodeMarkers {
		phase = infrav1.WorkerDisruptionPhaseDraining
	}
	if o.Token != TokenHeld || !isPreCallPhase(phase) || o.Operation != infrav1.WorkerDisruptionOperationDeletePool {
		return DrainProgress{Excluded: r.knobs.ExcludeDraining, DetachExcluded: r.knobs.ExcludeVolumeDetachWait}, nil
	}
	if r.drain == nil {
		return r.disconnectedProgress(), nil
	}
	if r.drainResult == nil || (phase == infrav1.WorkerDisruptionPhaseDraining && !r.evicted) {
		if err := r.runDrain(ctx, phase == infrav1.WorkerDisruptionPhaseDraining); err != nil {
			return DrainProgress{}, err
		}
	}
	return r.drainResult.Progress, nil
}

// disconnectedProgress is the drain progress while the workload cluster is not connected. Nothing can be observed, so
// Done and VolumesDetached stay false, but the exclusions hold and the time limits still run out: they count from
// status.disruption.drainStartTime, which the first drain step recorded. Cluster API records the drain start in the
// Machine's status for the same reason: a set time limit still runs out and then allows the call. The detach limit
// counts from the drain start too, as in DrainStep.Check. Without a recorded start no limit runs out.
func (r *poolRemoval) disconnectedProgress() DrainProgress {
	now := r.s.params.Clock.Now()
	start := r.s.Pool.Status.Disruption.DrainStartTime.Time
	return DrainProgress{
		Excluded:       r.knobs.ExcludeDraining,
		TimedOut:       drain.TimeoutExceeded(now, start, r.knobs.NodeDrainTimeoutSeconds),
		DetachExcluded: r.knobs.ExcludeVolumeDetachWait,
		DetachTimedOut: drain.TimeoutExceeded(now, start, r.knobs.NodeVolumeDetachTimeoutSeconds),
	}
}

// runDrain runs Evict (evict true) or Check on the victim Nodes and keeps the result. Without the workload cluster,
// which only a removal that needs nothing from it gets this far without (needsNoWorkloadCluster), it touches nothing and
// keeps disconnectedProgress.
//
// Parameters:
//   - ctx is the reconcile context.
//   - evict selects Evict.
func (r *poolRemoval) runDrain(ctx context.Context, evict bool) error {
	if r.drain == nil {
		r.drainResult, r.evicted = &DrainStepResult{Progress: r.disconnectedProgress()}, r.evicted || evict
		return nil
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

// handOver rewrites status.disruption as the pool deletion of every worker of the pool, keeping drainStartTime and
// ibmRequestTime, so that the drain time limits and DisruptionSettleDelay still count from the earlier drain and IBM
// call. phase is the decision's phase: Draining for a removal in flight, "" when nothing was cordoned yet.
func (r *poolRemoval) handOver(phase infrav1.WorkerDisruptionPhase) {
	disruption := &r.s.Pool.Status.Disruption
	if phase == "" {
		*disruption = infrav1.WorkerDisruptionStatus{}
		return
	}
	disruption.Operation = infrav1.WorkerDisruptionOperationDeletePool
	disruption.Phase = phase
	disruption.WorkerIDs = slices.Clone(r.victims)
}

// apply carries out the decision of this reconcile (the DecideDisruptionStep steps, as the pool deletion uses them).
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (r *poolRemoval) apply(ctx context.Context, d DisruptionDecision) (Outcome, bool) {
	switch d.Step {
	case StepWait:
		if d.ReleaseToken {
			if err := r.token.Release(ctx); err != nil {
				return Transient(err), false
			}
		}
		return r.wait(d, IntervalGuarded), false
	case StepDrain:
		if !r.evicted {
			if err := r.runDrain(ctx, true); err != nil {
				return Transient(err), false
			}
		}
		if r.drainResult.Progress.Blocked {
			// The first drain of a removal runs after the decision; its refusals are reported right away, so that the
			// next reconcile sees DrainBlocked as the previous reason and does not announce it again.
			d.DrainingReason = ReasonDrainBlocked
		}
		r.record(d.Phase)
		return r.wait(d, IntervalDrainRetry), false
	case StepWaitForVolumeDetach:
		r.record(d.Phase)
		return r.wait(d, IntervalDrainRetry), false
	case StepHold:
		r.record(d.Phase)
		return r.wait(d, IntervalGuarded), false
	case StepCallIBM:
		return r.callIBM(ctx, d), false
	case StepWaitForIBM:
		r.record(d.Phase)
		if hold, held := r.refusalHold(d); held {
			return r.wait(hold, IntervalGuarded), false
		}
		return r.wait(d, IntervalPoolTransition), false
	case StepWaitForRemoval:
		r.record(d.Phase)
		return r.wait(d, IntervalPoolTransition), false
	default:
		// StepComplete cannot come: the deletion completes in poolToRemove once IBM no longer lists the pool, before any
		// decision. Abort, CorrectVictims, Stop and None belong to other operations or to a Cluster deletion.
		return Transient(fmt.Errorf("managed: step %s is not a step of a pool deletion (programming error)", d.Step)), false
	}
}

// record writes status.disruption for phase: operation DeletePool, every worker of the pool, the drain start kept or
// taken from the Node markers or now, ibmRequestTime kept (or now when IBM already removes the pool without a CAPIBM
// call, so the stuck timer has a start). The drain events of the phase change are emitted here.
func (r *poolRemoval) record(phase infrav1.WorkerDisruptionPhase) {
	disruption := &r.s.Pool.Status.Disruption
	from := disruption.Phase
	if from == "" && r.resumed {
		// The drain started before status.disruption was lost; it is not announced again.
		from = infrav1.WorkerDisruptionPhaseDraining
	}
	now := r.s.params.Clock.Now()
	disruption.Operation = infrav1.WorkerDisruptionOperationDeletePool
	disruption.Phase = phase
	disruption.WorkerIDs = slices.Clone(r.victims)
	if disruption.DrainStartTime.IsZero() {
		start := now
		if r.drainResult != nil && !r.drainResult.StartTime.IsZero() {
			start = r.drainResult.StartTime
		}
		disruption.DrainStartTime = metav1.NewTime(start)
	}
	if phase == infrav1.WorkerDisruptionPhaseWaitingForRemoval && disruption.IBMRequestTime.IsZero() {
		disruption.IBMRequestTime = metav1.NewTime(now)
	}
	r.drainEvents(from, phase)
}

// drainEvents emits the drain events of a phase change. DrainStarted and DrainCompleted count the victim Nodes, so they
// need the workload cluster. Without it nothing was cordoned or evicted in this reconcile: a removal that needs nothing
// from it announces nothing, and a drain that moves on because its time limit ran out (disconnectedProgress) is
// announced with the Warning DrainTimeoutExceeded, which says that the pods left are not known.
//
// Parameters:
//   - from is the phase before the step, as record derived it.
//   - to is the phase the step records.
func (r *poolRemoval) drainEvents(from, to infrav1.WorkerDisruptionPhase) {
	if r.drain != nil {
		RecordDrainPhaseChange(r.s.params.Recorder, r.s.Pool, from, to, len(r.victimNodes))
		return
	}
	// Without the workload cluster the drain leaves Draining only when it is excluded or timed out (drainFinished).
	if from == infrav1.WorkerDisruptionPhaseDraining && to == infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach && !r.knobs.ExcludeDraining {
		r.s.params.Recorder.Eventf(r.s.Pool, corev1.EventTypeWarning, EventDrainTimeoutExceeded,
			"the drain time limit of %ds (MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed since %s "+
				"while the workload cluster is not connected; CAPIBM proceeds without knowing which pods are left on the Nodes of pool %s",
			ptrValue(r.knobs.NodeDrainTimeoutSeconds), r.s.Pool.Status.Disruption.DrainStartTime.UTC().Format(time.RFC3339),
			r.s.Pool.Spec.PoolName)
	}
}

// wait reports the decision on Deleting and Draining and returns NotDone after interval.
//
// Parameters:
//   - d is the decision.
//   - interval is the wait.
func (r *poolRemoval) wait(d DisruptionDecision, interval Interval) Outcome {
	r.reportDecision(d)
	return NotDone(interval, string(d.Reason))
}

// reportDecision sets Deleting=True with the decision's reason and message, and Draining from its DrainingReason with
// the drain message of this reconcile (deterministic, at most 3 pods per category, so that a repeated reconcile writes
// nothing). d is the decision.
func (r *poolRemoval) reportDecision(d DisruptionDecision) {
	if d.Reason != "" {
		r.s.report(string(d.Reason), d.Message)
	}
	draining := metav1.Condition{
		Type:   infrav1.IBMCloudManagedMachinePoolDrainingCondition,
		Status: metav1.ConditionFalse,
		Reason: infrav1.IBMCloudManagedMachinePoolNotDrainingReason,
	}
	if d.DrainingReason != "" {
		draining.Status, draining.Reason, draining.Message = metav1.ConditionTrue, string(d.DrainingReason), d.Message
		if r.drainResult != nil && r.drainResult.Message != "" {
			draining.Message = r.drainResult.Message
		}
	}
	conditions.Set(r.s.Pool, draining)
}

// callIBM makes the removal's one IBM call: it persists phase WaitingForIBM with ibmRequestTime and
// lastDisruptionTime first, so that a restart never sends the call twice within DisruptionSettleDelay, then sends
// RemoveWorkerPool and maps the answer with DisruptionCallOutcome. E1437 holds with the Nodes cordoned
// (WaitingForClusterMinimum). The decision is reported before the persist, so that Deleting always names the answer
// to the latest call, also when an unknown outcome (5xx, timeout) leaves the conditions as they are: refusalHold reads
// it. ctx is the reconcile context; d is the decision.
func (r *poolRemoval) callIBM(ctx context.Context, d DisruptionDecision) Outcome {
	now := metav1.NewTime(r.s.params.Clock.Now())
	r.record(infrav1.WorkerDisruptionPhaseWaitingForIBM)
	r.s.Pool.Status.Disruption.IBMRequestTime = now
	r.s.Pool.Status.LastDisruptionTime = now
	r.reportDecision(d)
	if err := r.s.persist(ctx); err != nil {
		return Transient(fmt.Errorf("persisting the removal of pool %s before the IBM call: %w", r.s.Pool.Spec.PoolName, err))
	}
	clusterID := r.s.ControlPlane.Status.ClusterID
	err := r.clients.kubernetes.RemoveWorkerPool(ctx, clusterID, r.pool.ID)
	result, verr := DisruptionCallOutcome(kubernetesservice.OpRemoveWorkerPool, err, r.pool.AutoscaleEnabled)
	if verr != nil {
		return Transient(verr)
	}
	if !result.Handled {
		return r.s.ibmProblem(r.clients, kubernetesservice.OpRemoveWorkerPool,
			fmt.Sprintf("removing worker pool %s of IBM cluster %s", r.pool.Name, clusterID), err)
	}
	r.record(result.Decision.Phase)
	interval := IntervalPoolTransition
	if result.Decision.Step == StepHold {
		interval = IntervalGuarded
	}
	return r.wait(result.Decision, interval)
}

// refusalHold keeps the hold of a refused call while the call waits out DisruptionSettleDelay. It returns d with the
// reason and message of the Deleting condition as read, and true, when that condition holds IBM's synchronous refusal
// of the latest call: WaitingForClusterMinimum for E1437 (IBM refuses synchronously and without side effects) or
// ExternallyAutoscaled for the 403 of an autoscaled pool (DisruptionCallOutcome). The caller only asks while a call is
// less than DisruptionSettleDelay old (StepWaitForIBM, emptyPoolGate), and every call reports its own reason first
// (callIBM, removeEmptyPool), so the condition names the answer to that call. Otherwise it returns d and false.
//
// The wait itself stays: ibmRequestTime and lastDisruptionTime are what keep the retry out of the delay, also in the
// reconcile that this pool's own status patch triggers at once (the For watch has no generation predicate); taking them
// back would retry the refused call in a loop. Only the report changes: DecideDisruptionStep's RemovingWorkerPool ("the
// IBM call was sent at ...") would otherwise replace the hold for the delay after each refusal, so that the reason
// flips back and forth and every reconcile rewrites status.
//
// Parameters:
//   - d is the decision of a wait after a call.
func (r *poolRemoval) refusalHold(d DisruptionDecision) (DisruptionDecision, bool) {
	deleting := conditions.Get(r.s.Pool, infrav1.IBMCloudManagedMachinePoolDeletingCondition)
	if deleting == nil || deleting.Status != metav1.ConditionTrue {
		return d, false
	}
	switch reason := DisruptionReason(deleting.Reason); reason {
	case ReasonWaitingForClusterMinimum, ReasonExternallyAutoscaled:
		d.Reason, d.Message, d.DrainingReason = reason, deleting.Message, ""
		return d, true
	}
	return d, false
}

// removeEmptyPool removes a pool without workers: no Node to drain and no ready capacity to take away, so neither the
// token nor status.disruption (whose workerIDs need a worker) is used. The cluster state and IBM's minimum still gate
// the call, and lastDisruptionTime keeps a second call out of DisruptionSettleDelay. As in callIBM, Deleting names the
// call before the persist, for refusalHold. ctx is the reconcile context.
func (r *poolRemoval) removeEmptyPool(ctx context.Context) (Outcome, bool) {
	if err := r.token.Release(ctx); err != nil {
		return Transient(err), false
	}
	r.s.Pool.Status.Disruption = infrav1.WorkerDisruptionStatus{}
	if d, blocked, err := r.emptyPoolGate(); err != nil || blocked {
		if err != nil {
			return Transient(err), false
		}
		return r.wait(d, IntervalGuarded), false
	}
	r.s.Pool.Status.LastDisruptionTime = metav1.NewTime(r.s.params.Clock.Now())
	r.reportDecision(DisruptionDecision{Reason: ReasonRemovingWorkerPool, Message: "the pool has no workers; asking IBM to remove it"})
	if err := r.s.persist(ctx); err != nil {
		return Transient(fmt.Errorf("persisting the removal of pool %s before the IBM call: %w", r.s.Pool.Spec.PoolName, err)), false
	}
	clusterID := r.s.ControlPlane.Status.ClusterID
	err := r.clients.kubernetes.RemoveWorkerPool(ctx, clusterID, r.pool.ID)
	result, verr := DisruptionCallOutcome(kubernetesservice.OpRemoveWorkerPool, err, r.pool.AutoscaleEnabled)
	switch {
	case verr != nil:
		return Transient(verr), false
	case !result.Handled:
		return r.s.ibmProblem(r.clients, kubernetesservice.OpRemoveWorkerPool,
			fmt.Sprintf("removing worker pool %s of IBM cluster %s", r.pool.Name, clusterID), err), false
	}
	result.Decision.DrainingReason = ""
	interval := IntervalPoolTransition
	if result.Decision.Step == StepHold {
		interval = IntervalGuarded
	}
	return r.wait(result.Decision, interval), false
}

// emptyPoolGate returns the wait of a pool without workers, and true, while IBM removes the pool, while the last
// removal call of this pool is less than DisruptionSettleDelay old (keeping the hold of a refused call, refusalHold),
// while the cluster state forbids the removal, or while IBM's minimum would be broken (CheckDisruption rule 3; rules 1
// and 2 are skipped for a removal that takes no ready worker away).
func (r *poolRemoval) emptyPoolGate() (DisruptionDecision, bool, error) {
	now := r.s.params.Clock.Now()
	last := r.s.Pool.Status.LastDisruptionTime
	switch {
	case slices.Contains(PoolNameReleasedStates(), r.pool.ActualState):
		return DisruptionDecision{Reason: ReasonRemovingWorkerPool, Message: "IBM removes the pool, which has no workers"}, true, nil
	case !last.IsZero() && now.Before(last.Add(DisruptionSettleDelay)):
		wait, _ := r.refusalHold(DisruptionDecision{Reason: ReasonRemovingWorkerPool, Message: fmt.Sprintf("the pool has "+
			"no workers; it is removed from %s, %d minutes after the last removal request of this pool",
			last.Add(DisruptionSettleDelay).UTC().Format(time.RFC3339), int(DisruptionSettleDelay.Minutes()))})
		return wait, true, nil
	case !PoolMutationAllowed(r.clusterState, PoolOperationRemove):
		return DisruptionDecision{Reason: ReasonWaitingForClusterState, Message: fmt.Sprintf("IBM cluster %s is %s; IBM "+
			"accepts no worker pool change in that state", r.s.ControlPlane.Status.ClusterID, r.clusterState)}, true, nil
	}
	gate, err := r.check(false)
	if err != nil || gate.Allowed {
		return DisruptionDecision{}, false, err
	}
	return DisruptionDecision{Reason: gate.Reason, Message: gate.Message}, true, nil
}

// hasAnnotation reports whether obj carries the annotation, whatever its value.
//
// Parameters:
//   - obj is the object.
//   - annotation is the key.
func hasAnnotation(obj metav1.Object, annotation string) bool {
	_, found := obj.GetAnnotations()[annotation]
	return found
}
