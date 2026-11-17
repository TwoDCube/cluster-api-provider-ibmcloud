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
// credential resolution. A deletion that leaves the IBM pool to another object that still claims it makes no IBM call
// and no credential resolution either, but reads the workload cluster's Nodes to revert this pool's own unfinished
// drain (releaseOwnDrain). ctx is the reconcile context.
func (s *MachinePoolScope) ReconcileDelete(ctx context.Context) (Outcome, bool) {
	if err := s.loadOwners(ctx); err != nil {
		return Transient(err), false
	}
	if why := s.deleteSkipReason(); why != "" {
		ctrl.LoggerFrom(ctx).Info("Removing the finalizer without touching the IBM pool", "reason", why)
		return Done(), true
	}
	if others := s.liveClaimants(); len(others) > 0 {
		reverted, outcome, released := s.releaseOwnDrain(ctx)
		if !released {
			return outcome, false
		}
		s.params.Recorder.Eventf(s.Pool, corev1.EventTypeWarning, EventPoolNotRemoved, "IBM worker pool %s stays: "+
			"IBMCloudManagedMachinePool %s still claims it%s", s.Pool.Spec.PoolName, boundedList(others), revertedNote(reverted))
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
	s.recordListing(workers)
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
// another flavor is not this object's: it stays, with a Warning event, once this pool's own unfinished drain is
// reverted (releaseOwnDrain).
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
		reverted, outcome, released := s.releaseOwnDrain(ctx)
		if !released {
			return nil, outcome, true
		}
		s.params.Recorder.Eventf(s.Pool, corev1.EventTypeWarning, EventPoolNotRemoved, "IBM worker pool %s stays: it has "+
			"flavor %s, not this object's %s, so it is not this object's pool%s", pool.Name, pool.Flavor, s.Pool.Spec.Flavor,
			revertedNote(reverted))
		return nil, Done(), true
	}
	return pool, Outcome{}, false
}

// releaseOwnDrain ends this pool's worker removal when its deletion leaves the IBM pool alone (another object still
// claims it, or it has another flavor): once the finalizer is gone, nothing carries that removal on. It reverts
// CAPIBM's cordon of every Node that carries this pool's drain-owner marker (drain.Helper.Uncordon reverts only CAPIBM's
// own changes, so a cordon a human set stays), clears status.disruption and releases the disruption token. Left behind,
// the markers would keep those Nodes cordoned and out of the load balancers for good, and the object that keeps the IBM
// pool could never drain them (drain.ErrDrainOwnerConflict). It returns how many Nodes it reverted and true when the
// finalizer may go; otherwise the outcome to return.
//
// A removal whose IBM call went out less than WorkerStuckAfter ago is waited out first, as DecideDisruptionStep waits out
// a removal whose reason went away after its call: IBM may still remove those workers, and their Nodes must stay
// drained until it has. The wait ends early once no Node carries the marker (IBM removed the workers, and their Nodes
// with them). While the workload cluster is not connected the markers cannot be read: a removal that status.disruption
// records waits for the connection, unless both exclusion annotations say that the pool needs nothing from the workload
// cluster; without a recorded removal nothing is known to revert, and the finalizer goes.
//
// Parameters:
//   - ctx is the reconcile context.
func (s *MachinePoolScope) releaseOwnDrain(ctx context.Context) (int, Outcome, bool) {
	disruption := s.Pool.Status.Disruption
	workload, err := s.params.ClusterCache.GetClient(ctx, client.ObjectKeyFromObject(s.Cluster))
	switch {
	case errors.Is(err, clustercache.ErrClusterNotConnected):
		knobs := DrainKnobsFor(s.MachinePool, s.Pool)
		if disruption.Operation == "" || (knobs.ExcludeDraining && knobs.ExcludeVolumeDetachWait) {
			return s.dropOwnDrain(ctx, 0)
		}
		reason := string(ReasonWaitingForWorkloadCluster)
		s.report(reason, fmt.Sprintf("IBM worker pool %s stays, and the %s removal of this pool is unfinished; CAPIBM "+
			"reverts its cordons once the workload cluster is connected, and then removes the finalizer", s.Pool.Spec.PoolName,
			disruption.Operation))
		return 0, NotDone(IntervalGuarded, reason), false
	case err != nil:
		return 0, Transient(fmt.Errorf("getting the workload cluster client of Cluster %s: %w", s.Cluster.Name, err)), false
	}
	nodes := &corev1.NodeList{}
	if err := workload.List(ctx, nodes); err != nil {
		return 0, Transient(fmt.Errorf("listing the Nodes of Cluster %s: %w", s.Cluster.Name, err)), false
	}
	var marked []corev1.Node
	for i := range nodes.Items {
		if hasDrainOwner(&nodes.Items[i], s.Pool) {
			marked = append(marked, nodes.Items[i])
		}
	}
	if len(marked) == 0 {
		return s.dropOwnDrain(ctx, 0)
	}
	if sent := disruption.IBMRequestTime.Time; !sent.IsZero() && s.params.Clock.Now().Before(sent.Add(WorkerStuckAfter)) {
		// Deleting's own reason: the removal's reason (Resizing, ReplacingWorker) belongs to WorkerPoolReady and
		// WorkersUpToDate, and would say that the pool is resized or a worker replaced while the deletion only waits.
		reason := infrav1.IBMCloudManagedMachinePoolDeletingWaitingForWorkerRemovalReason
		s.report(reason, fmt.Sprintf("IBM worker pool %s stays, and the %s removal of this pool asked IBM at %s to remove "+
			"workers; their %d Nodes stay cordoned until IBM has removed them or %s passes, then CAPIBM reverts the cordons "+
			"and removes the finalizer", s.Pool.Spec.PoolName, disruption.Operation, sent.UTC().Format(time.RFC3339), len(marked),
			sent.Add(WorkerStuckAfter).UTC().Format(time.RFC3339)))
		return 0, NotDone(IntervalPoolTransition, reason), false
	}
	helper := &drain.Helper{Client: workload, Clock: s.params.Clock, EvictionBudget: EvictionBudgetPerReconcile,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	for i := range marked {
		if err := helper.Uncordon(ctx, &marked[i]); err != nil {
			return 0, Transient(err), false
		}
	}
	return s.dropOwnDrain(ctx, len(marked))
}

// dropOwnDrain releases the disruption token when this pool holds it and clears status.disruption: the last step of
// releaseOwnDrain, which then lets the finalizer go.
//
// Parameters:
//   - ctx is the reconcile context.
//   - reverted is how many Nodes releaseOwnDrain reverted, passed through.
func (s *MachinePoolScope) dropOwnDrain(ctx context.Context, reverted int) (int, Outcome, bool) {
	if err := s.releaseToken(ctx); err != nil {
		return 0, Transient(err), false
	}
	s.Pool.Status.Disruption = infrav1.WorkerDisruptionStatus{}
	return reverted, Done(), true
}

// revertedNote is the part of the WorkerPoolNotRemoved event that says how many Nodes releaseOwnDrain reverted; "" for
// none.
//
// Parameters:
//   - reverted is that number.
func revertedNote(reverted int) string {
	if reverted == 0 {
		return ""
	}
	return fmt.Sprintf("; CAPIBM reverted its cordon of %d Nodes of this pool's unfinished removal", reverted)
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
// DecideDisruptionStep chooses. The steps every removal shares (recording status.disruption, the drain, the IBM call
// and the hold after a refusal) are its runner's (removal_runner.go).
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
	// allNodes are every Node of the workload cluster; none while it is not connected.
	allNodes []corev1.Node
	// runner carries out the shared removal steps for the pool's workers; observe builds it.
	runner *removalRunner
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

// observe lists the victims, builds the token handle, reads the workload cluster's Nodes when it is connected, and
// builds the runner of the removal: the pool's RemoveWorkerPool call, reported on Deleting, with the cluster state that
// observeClusterForDelete read from IBM in this reconcile. ctx is the reconcile context.
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
	workload, victimNodes, err := r.observeWorkload(ctx)
	if err != nil {
		return err
	}
	clusterID := r.s.ControlPlane.Status.ClusterID
	poolID := r.pool.ID
	r.runner, err = newRemovalRunner(removalRunnerInput{
		scope: r.s, clients: r.clients, pool: r.pool,
		operation: infrav1.WorkerDisruptionOperationDeletePool, victims: r.victims, clusterState: r.clusterState,
		call: removalCall{op: kubernetesservice.OpRemoveWorkerPool,
			action: fmt.Sprintf("removing worker pool %s of IBM cluster %s", r.pool.Name, clusterID),
			send: func(ctx context.Context) error {
				return r.clients.kubernetes.RemoveWorkerPool(ctx, clusterID, poolID)
			}},
		// Deleting is True while the pool is deleted (MachinePoolScope.report). The deletion reads the record only
		// within DisruptionSettleDelay of its latest call, so the reason alone is the record (refusalRecord.message).
		refusal: refusalRecord{condition: infrav1.IBMCloudManagedMachinePoolDeletingCondition, status: metav1.ConditionTrue},
		report:  r.reportDeleting, workload: workload, victimNodes: victimNodes,
	})
	return err
}

// observeWorkload reads every Node of the workload cluster through ClusterCache and returns the client and the Nodes of
// the victims; it keeps every Node for the drain-owner markers of other pools (allNodes). A workload cluster that is not
// connected is no error: it returns no client and no Node, and every step that needs them waits
// (WaitingForWorkloadCluster). ctx is the reconcile context.
func (r *poolRemoval) observeWorkload(ctx context.Context) (client.Client, []corev1.Node, error) {
	workload, err := r.s.params.ClusterCache.GetClient(ctx, client.ObjectKeyFromObject(r.s.Cluster))
	if errors.Is(err, clustercache.ErrClusterNotConnected) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("getting the workload cluster client of Cluster %s: %w", r.s.Cluster.Name, err)
	}
	nodes := &corev1.NodeList{}
	if err := workload.List(ctx, nodes); err != nil {
		return nil, nil, fmt.Errorf("listing the Nodes of Cluster %s: %w", r.s.Cluster.Name, err)
	}
	account, err := AccountIDFromCRN(r.s.ControlPlane.Status.ClusterCRN)
	if err != nil {
		return nil, nil, fmt.Errorf("finding the Nodes of pool %s: %w", r.s.Pool.Spec.PoolName, err)
	}
	r.allNodes = nodes.Items
	return workload, VictimNodes(nodes.Items, account, r.s.ControlPlane.Status.ClusterID, r.victims), nil
}

// needsNoWorkloadCluster reports whether the removal can run without the workload cluster: with both exclusion
// annotations set nothing is cordoned, evicted or waited for, and a Cluster API Machine with both is deleted without
// its workload cluster too. It keeps the documented escape hatch open for a pool whose workload cluster is unreachable
// (a private-only cluster, an expired admin certificate after a long pause, a broken API server). Another pool's
// drain-owner marker cannot be read then; the token and CheckDisruption still apply.
func (r *poolRemoval) needsNoWorkloadCluster() bool {
	return r.runner.knobs.ExcludeDraining && r.runner.knobs.ExcludeVolumeDetachWait
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
	// The drain-owner markers of a pool that no longer exists hold no drain: they are left out of the gate, and the drain
	// step takes them over on a Node of this pool.
	owners, err := r.runner.drainingPools(ctx, r.token, r.allNodes)
	if err != nil {
		return DisruptionObservation{}, err
	}
	gate, err := r.check(false)
	if err != nil {
		return DisruptionObservation{}, err
	}
	// The pre-call gate checks the cluster state of this reconcile again before CheckDisruption: RemoveWorkerPool is
	// never sent to a cluster that turned unavailable or scaled_down during a long drain.
	preCall, err := r.runner.preCallGate(r.check)
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
		WorkloadClusterConnected: r.runner.drain != nil || r.needsNoWorkloadCluster(),
		Gates: DisruptionGates{
			ClusterStateAllows: r.runner.clusterStateCheck().Allowed,
			Disruption:         gate,
			OtherDrainOwners:   owners,
		},
		PreCallGate: preCall,
		IBM:         RemovalProgress{Removing: slices.Contains(PoolNameReleasedStates(), r.pool.ActualState)},
	}
	r.runner.resumed = o.ResumeFromNodeMarkers
	o.Drain, err = r.drainProgress(ctx, o)
	return o, err
}

// check is CheckDisruption for removing every ready worker of the pool. skipSettleDelay selects the pre-call gate,
// which leaves out the settle delay. Both the gate and the pre-call gate get the controller's first-seen times
// (MachinePoolScopeInput.TransitionalSince), so that a worker IBM has left transitional for longer than
// WorkerStuckAfter, in any pool, no longer holds the deletion.
func (r *poolRemoval) check(skipSettleDelay bool) (DisruptionCheckResult, error) {
	ready := 0
	for _, w := range r.workers {
		if w.PoolID == r.pool.ID && IsWorkerReady(w) {
			ready++
		}
	}
	return CheckDisruption(DisruptionCheck{
		Operation:         infrav1.WorkerDisruptionOperationDeletePool,
		Distribution:      r.s.ControlPlane.Spec.Distribution,
		Now:               r.s.params.Clock.Now(),
		Workers:           r.workers,
		TransitionalSince: r.s.firstSeen,
		Pools:             r.s.clusterPools,
		RemovedReady:      ready,
		SkipSettleDelay:   skipSettleDelay,
	})
}

// ownsADrain reports whether a victim Node carries this pool's drain-owner marker: a clusterctl move or a lost status
// patch cleared status.disruption mid-drain, and the drain resumes instead of starting over.
func (r *poolRemoval) ownsADrain() bool {
	key := infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner
	owner := DrainOwner(r.s.Pool)
	for i := range r.runner.victimNodes {
		if r.runner.victimNodes[i].Annotations[key] == owner {
			return true
		}
	}
	return false
}

// drainProgress observes the drain for the effective phase of o: Draining evicts, the phases after it check without
// evicting, and earlier or later phases need no drain observation. Nothing is cordoned or evicted before this pool
// holds the disruption token, so that a Cluster has one disruption at a time; DecideDisruptionStep asks for the token
// before it reads the drain. The exclusions are known without the workload cluster, and so are the time limits
// (disconnectedProgress): the drain limit once a drain start is recorded, the detach limit once the step into
// WaitingForVolumeDetach recorded its start. Each kind of observation runs at most once per reconcile.
//
// Parameters:
//   - ctx is the reconcile context.
//   - o is the observation so far.
func (r *poolRemoval) drainProgress(ctx context.Context, o DisruptionObservation) (DrainProgress, error) {
	runner := r.runner
	phase := o.Phase
	if phase == "" && o.ResumeFromNodeMarkers {
		phase = infrav1.WorkerDisruptionPhaseDraining
	}
	if o.Token != TokenHeld || !isPreCallPhase(phase) || o.Operation != infrav1.WorkerDisruptionOperationDeletePool {
		return DrainProgress{Excluded: runner.knobs.ExcludeDraining, DetachExcluded: runner.knobs.ExcludeVolumeDetachWait}, nil
	}
	if runner.drain == nil {
		return runner.disconnectedProgress(), nil
	}
	if runner.drainResult == nil || (phase == infrav1.WorkerDisruptionPhaseDraining && !runner.evicted) {
		if err := runner.runDrain(ctx, phase == infrav1.WorkerDisruptionPhaseDraining); err != nil {
			return DrainProgress{}, err
		}
	}
	return runner.drainResult.Progress, nil
}

// handOver rewrites status.disruption as the pool deletion of every worker of the pool, keeping drainStartTime and
// ibmRequestTime, so that the drain time limits and DisruptionSettleDelay still count from the earlier drain and IBM
// call. It clears volumeDetachStartTime: the pool deletion adds Nodes that are not drained yet, and their volumes only
// start to detach once they are, so a detach wait counted from the earlier, smaller removal could end before theirs
// began. The detach wait of the whole pool begins anew when it reaches WaitingForVolumeDetach, which can only make the
// wait of the earlier victims longer. phase is the decision's phase: Draining for a removal in flight, "" when nothing
// was cordoned yet.
func (r *poolRemoval) handOver(phase infrav1.WorkerDisruptionPhase) {
	disruption := &r.s.Pool.Status.Disruption
	if phase == "" {
		*disruption = infrav1.WorkerDisruptionStatus{}
		return
	}
	disruption.Operation = infrav1.WorkerDisruptionOperationDeletePool
	disruption.Phase = phase
	disruption.WorkerIDs = slices.Clone(r.victims)
	disruption.VolumeDetachStartTime = metav1.Time{}
}

// apply carries out the decision of this reconcile (the DecideDisruptionStep steps, as the pool deletion uses them).
//
// Parameters:
//   - ctx is the reconcile context.
//   - d is the decision.
func (r *poolRemoval) apply(ctx context.Context, d DisruptionDecision) (Outcome, bool) {
	runner := r.runner
	switch d.Step {
	case StepWait:
		if d.ReleaseToken {
			if err := r.token.Release(ctx); err != nil {
				return Transient(err), false
			}
		}
		return runner.wait(d, IntervalGuarded), false
	case StepDrain:
		if !runner.evicted {
			if err := runner.runDrain(ctx, true); err != nil {
				return Transient(err), false
			}
		}
		if runner.drainResult.Progress.Blocked {
			// The first drain of a removal runs after the decision; its refusals are reported right away, so that the
			// next reconcile sees DrainBlocked as the previous reason and does not announce it again.
			d.DrainingReason = ReasonDrainBlocked
		}
		runner.record(d.Phase)
		return runner.wait(d, IntervalDrainRetry), false
	case StepWaitForVolumeDetach:
		runner.record(d.Phase)
		return runner.wait(d, IntervalDrainRetry), false
	case StepHold:
		runner.record(d.Phase)
		return runner.wait(d, IntervalGuarded), false
	case StepCallIBM:
		return runner.callIBM(ctx, d), false
	case StepWaitForIBM:
		runner.record(d.Phase)
		if hold, held := runner.refusalHold(d); held {
			return runner.wait(hold, IntervalGuarded), false
		}
		return runner.wait(d, IntervalPoolTransition), false
	case StepWaitForRemoval:
		r.recordRemoval(d.Phase)
		return runner.wait(d, IntervalPoolTransition), false
	default:
		// StepComplete cannot come: the deletion completes in poolToRemove once IBM no longer lists the pool, before any
		// decision. Abort, CorrectVictims, Stop and None belong to other operations or to a Cluster deletion.
		return Transient(fmt.Errorf("managed: step %s is not a step of a pool deletion (programming error)", d.Step)), false
	}
}

// recordRemoval records phase (WaitingForRemoval) like every step, and sets ibmRequestTime to now when it is still
// zero: IBM removes the pool without a CAPIBM call (out of band, or after a clusterctl move lost the status of a call),
// and the stuck timer of the removal counts from it (StepWaitForRemoval).
//
// Parameters:
//   - phase is the phase the step records.
func (r *poolRemoval) recordRemoval(phase infrav1.WorkerDisruptionPhase) {
	disruption := &r.s.Pool.Status.Disruption
	if disruption.IBMRequestTime.IsZero() {
		disruption.IBMRequestTime = metav1.NewTime(r.s.params.Clock.Now())
	}
	r.runner.record(phase)
}

// reportDeleting reports a decision on Deleting=True with the decision's reason and message (MachinePoolScope.report);
// a decision without a reason leaves Deleting as it is. It is the runner's report of the pool deletion. d is the
// decision.
func (r *poolRemoval) reportDeleting(d DisruptionDecision) {
	if d.Reason != "" {
		r.s.report(string(d.Reason), d.Message)
	}
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
		return r.runner.wait(d, IntervalGuarded), false
	}
	r.s.Pool.Status.LastDisruptionTime = metav1.NewTime(r.s.params.Clock.Now())
	r.runner.report(DisruptionDecision{Reason: ReasonRemovingWorkerPool, Message: "the pool has no workers; asking IBM to remove it"})
	result, outcome, done := r.runner.sendCall(ctx, r.runner.call)
	if done {
		return outcome, false
	}
	result.Decision.DrainingReason = ""
	interval := IntervalPoolTransition
	if result.Decision.Step == StepHold {
		interval = IntervalGuarded
	}
	return r.runner.wait(result.Decision, interval), false
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
		wait, _ := r.runner.refusalHold(DisruptionDecision{Reason: ReasonRemovingWorkerPool, Message: fmt.Sprintf("the pool has "+
			"no workers; it is removed from %s, %d minutes after the last removal request of this pool",
			last.Add(DisruptionSettleDelay).UTC().Format(time.RFC3339), int(DisruptionSettleDelay.Minutes()))})
		return wait, true, nil
	}
	if state := r.runner.clusterStateCheck(); !state.Allowed {
		return DisruptionDecision{Reason: state.Reason, Message: state.Message}, true, nil
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
