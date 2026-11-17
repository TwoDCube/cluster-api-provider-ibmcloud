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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file is the IBMCloudManagedMachinePool scope: it adopts the IBM worker pool of the same name or creates it,
// observes the pool and its workers, publishes the InfraMachinePool contract (spec.providerIDList, status.replicas,
// status.ready, status.initialization.provisioned), grows the pool and dispatches its scale-down
// (machinepool_scaledown.go) and its worker rollout (machinepool_rollout.go). Every decision is one of the pure
// functions of this package (PoolGuards, PoolMutationAllowed, BuildProviderIDList, SummarizeWorkerVersions,
// PlanScaleDown, SummarizeRollout, PickReplacement, DecideDisruptionStep); the scope only gathers their inputs, makes
// the IBM calls they allow and writes their results. A pool reconcile costs at most ListWorkerPools, ListWorkers and
// one mutation.

const (
	// EventWorkerPoolAdopted is the Normal event emitted when CAPIBM takes over an IBM pool that it did not create. It
	// repeats once after a clusterctl move, which empties status.poolID.
	EventWorkerPoolAdopted = "WorkerPoolAdopted"

	// MachinePoolIBMAPIErrorReason is the WorkerPoolReady (while deleting: Deleting) reason of an IBM call or a
	// credential resolution that failed in a way a human must look at. The pool's WorkerPoolReady condition defines no
	// such reason of its own; the scope uses IBMAPIError, the token of the control plane and the IBMCloudManagedCluster,
	// so that one token matches this failure on every managed kind. The message names the call and the HTTP status,
	// never IBM's description, so that a repeated failure does not rewrite status.
	MachinePoolIBMAPIErrorReason = infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason

	// poolKubernetesServiceRoles are the IAM roles a 403 message asks for: the pool and worker operations need them on
	// Kubernetes Service.
	poolKubernetesServiceRoles = "the Administrator platform role and the Manager service role on Kubernetes Service"

	// poolControlPlaneKind and poolInfraClusterKind are the kinds the spec.controlPlaneRef and spec.infrastructureRef
	// of a managed Cluster name.
	poolControlPlaneKind = "IBMCloudManagedControlPlane"
	poolInfraClusterKind = "IBMCloudManagedCluster"
)

// MachinePoolScopeInput are the objects and settings of one IBMCloudManagedMachinePool reconcile.
type MachinePoolScopeInput struct {
	// Cluster is the owning CAPI Cluster. Required.
	Cluster *clusterv1.Cluster

	// MachinePool is the CAPI MachinePool that owns Pool; its spec.replicas sizes the IBM pool. Required.
	MachinePool *clusterv1.MachinePool

	// Pool is the IBMCloudManagedMachinePool being reconciled; the scope writes its spec.providerIDList, status and
	// conditions, and the controller's deferred patch persists them. Required.
	Pool *infrav1.IBMCloudManagedMachinePool

	// TransitionalSince maps the ID of a worker of the Cluster's IBM cluster to the time the pool controller first saw
	// it transitional; the disruption gates pass it to CheckDisruption (DisruptionCheck.TransitionalSince), so that a
	// worker IBM leaves transitional for longer than WorkerStuckAfter stops blocking worker removals. IBM reports no
	// time for a worker's state, so the controller keeps the times in memory, from every listing of the Cluster's pools
	// (ListedWorkers). After a controller restart they are gone, and a stuck worker blocks again for up to
	// WorkerStuckAfter: the rule fails closed. The scope only reads the map; the controller passes a copy. Optional: nil
	// treats no worker as stuck.
	TransitionalSince map[string]time.Time

	// ProviderIDDropAfter is poolTransitionPoll: a worker that IBM's listing omits leaves spec.providerIDList only
	// when a listing at least this long after the first omission still omits it (BuildProviderIDList), because CAPI
	// deletes the Node of every provider ID that leaves the list. It is an input because the requeue intervals belong
	// to the controllers. Required, > 0.
	ProviderIDDropAfter time.Duration

	// Persist patches Pool now. A removal patches its status (phase WaitingForIBM, ibmRequestTime,
	// lastDisruptionTime) before it sends the IBM call, so that a restart never sends the call twice within
	// DisruptionSettleDelay. The controller passes its patch helper. Required.
	Persist func(ctx context.Context) error
}

// MachinePoolScope is the scope of one IBMCloudManagedMachinePool reconcile. Create it with NewMachinePoolScope; it
// lives for one reconcile only.
type MachinePoolScope struct {
	params *ScopeParams

	// Cluster is the owning CAPI Cluster.
	Cluster *clusterv1.Cluster
	// MachinePool is the CAPI MachinePool that owns Pool.
	MachinePool *clusterv1.MachinePool
	// Pool is the object being reconciled.
	Pool *infrav1.IBMCloudManagedMachinePool

	// ControlPlane is the Cluster's IBMCloudManagedControlPlane; nil until loadOwners found it.
	ControlPlane *controlplanev1.IBMCloudManagedControlPlane
	// InfraCluster is the Cluster's IBMCloudManagedCluster, the identity holder of every IBM call on every path,
	// deletion included; nil until loadOwners found it.
	InfraCluster *infrav1.IBMCloudManagedCluster

	// clusterPools are every IBMCloudManagedMachinePool of the Cluster (by the cluster-name label), Pool included.
	clusterPools []infrav1.IBMCloudManagedMachinePool
	// dropAfter is MachinePoolScopeInput.ProviderIDDropAfter.
	dropAfter time.Duration
	// persist is MachinePoolScopeInput.Persist.
	persist func(ctx context.Context) error
	// notes are non-blocking WorkerPoolReady messages (an ignored spec.failureDomains, IBM pool zones that differ
	// from spec.zones), appended to every WorkerPoolReady message of this reconcile.
	notes []string
	// clients are the IBM clients of this reconcile, built by ibm on first use.
	clients *poolClients
	// firstSeen is MachinePoolScopeInput.TransitionalSince.
	firstSeen map[string]time.Time
	// listedWorkers are every worker of the cluster from this reconcile's successful ListWorkers call, and
	// workersListed tells whether one succeeded (an empty list is an answer too); see ListedWorkers.
	listedWorkers []kubernetesservice.Worker
	workersListed bool
	// scaleDownMode is ScaleDownMode. It is a field so that the tests of this package can run the ScaleDownResizeOnly
	// fallback, which the constant leaves unreachable unless IBM Cloud turns out not to remove the predicted victims.
	scaleDownMode ScaleDownStrategy
	// rolloutRecheckAt is RolloutRecheckAt: when the worker rollout of this reconcile decides differently without a
	// watched change; zero for never.
	rolloutRecheckAt time.Time
}

// poolClients are one reconcile's IBM clients for the Cluster's identity.
type poolClients struct {
	identity   credentials.Identity
	kubernetes kubernetesservice.KubernetesService
}

// NewMachinePoolScope returns the scope of one IBMCloudManagedMachinePool reconcile. It only stores its inputs: it
// reads no object, resolves no identity, builds no client and makes no call, so that a failure in any of those
// happens in a reconcile step, after the controller's patch helper exists, and is persisted as a condition.
//
// Parameters:
//   - params are the shared dependencies; the scope needs Client, APIReader, Recorder, Resolver, ClientBuilder, Clock
//     and ClusterCache.
//   - in are the objects and settings of the reconcile; see MachinePoolScopeInput.
//
// It returns an error naming every missing dependency or input.
func NewMachinePoolScope(params *ScopeParams, in MachinePoolScopeInput) (*MachinePoolScope, error) {
	if err := params.validate("MachinePoolScope", depClient, depAPIReader, depRecorder, depResolver, depClientBuilder, depClock,
		depClusterCache); err != nil {
		return nil, err
	}
	var problems []string
	if in.Cluster == nil {
		problems = append(problems, "Cluster is nil; pass the owning Cluster")
	}
	if in.MachinePool == nil {
		problems = append(problems, "MachinePool is nil; pass the MachinePool that owns the IBMCloudManagedMachinePool")
	}
	if in.Pool == nil {
		problems = append(problems, "Pool is nil; pass the IBMCloudManagedMachinePool being reconciled")
	}
	if in.ProviderIDDropAfter <= 0 {
		problems = append(problems, fmt.Sprintf("ProviderIDDropAfter is %s; pass poolTransitionPoll, which is greater than zero",
			in.ProviderIDDropAfter))
	}
	if in.Persist == nil {
		problems = append(problems, "Persist is nil; pass the function that patches the IBMCloudManagedMachinePool")
	}
	if len(problems) > 0 {
		return nil, errors.New("managed: MachinePoolScope: " + strings.Join(problems, "; "))
	}
	return &MachinePoolScope{
		params:        params,
		Cluster:       in.Cluster,
		MachinePool:   in.MachinePool,
		Pool:          in.Pool,
		firstSeen:     in.TransitionalSince,
		dropAfter:     in.ProviderIDDropAfter,
		persist:       in.Persist,
		scaleDownMode: ScaleDownMode,
	}, nil
}

// ReconcileNormal runs the pool flow once: the owners, the guards, the cluster ID, adoption or creation of the IBM
// pool, the observation of its workers, growth and the WorkerPoolReady condition. At most ListWorkerPools,
// ListWorkers and one mutation are sent; none while the Cluster or its control plane is being deleted. ctx is the
// reconcile context.
func (s *MachinePoolScope) ReconcileNormal(ctx context.Context) Outcome {
	conditions.Set(s.Pool, metav1.Condition{
		Type:   infrav1.IBMCloudManagedMachinePoolDeletingCondition,
		Status: metav1.ConditionFalse,
		Reason: infrav1.IBMCloudManagedMachinePoolNotDeletingReason,
	})
	if err := s.loadOwners(ctx); err != nil {
		return Transient(err)
	}
	if why := s.clusterDeletingReason(); why != "" {
		// No IBM call for a pool while its cluster goes away: IBM removes every pool with the cluster. CAPI deletes the
		// MachinePool first, and that update event reaches this object before the MachinePool controller deletes it; the
		// normal flow would then list the pool and create it again when IBM no longer lists it, or grow it, on a cluster
		// that is going away. The pool's own deletion follows and takes the skip path of ReconcileDelete.
		ctrl.LoggerFrom(ctx).Info("Making no IBM call while the IBM cluster goes away", "reason", why)
		return Done()
	}
	if outcome, missing := s.ownersMissing(); missing {
		return outcome
	}
	guards, err := PoolGuards(PoolGuardInput{Pool: s.Pool, MachinePool: s.MachinePool, InfraCluster: s.InfraCluster,
		Distribution: s.ControlPlane.Spec.Distribution, ClusterPools: s.clusterPools})
	if err != nil {
		return Transient(fmt.Errorf("checking %s: %w", s.objectName(), err))
	}
	s.addNote(guards.FailureDomainsMessage)
	clusterID := s.ControlPlane.Status.ClusterID
	// PoolNameConflict stops every IBM call; any other failing guard still lets an existing pool be observed, but a
	// pool CAPIBM never saw is not even looked up, so a misconfigured new pool (for example one whose size does not
	// divide by its zones) costs no IBM call.
	if guards.StopsPublication || (guards.Blocked() && (clusterID == "" || s.Pool.Status.PoolID == "")) {
		s.addNote(s.removalHeldNote(guards))
		return s.guardOutcome(guards)
	}
	if clusterID == "" {
		return s.waitForControlPlane(fmt.Sprintf("IBMCloudManagedControlPlane %s has no IBM cluster ID yet; the pool is "+
			"adopted or created once IBM has created the cluster", s.ControlPlane.Name))
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.credentialsProblem(err)
	}
	pool, outcome, done := s.findPool(ctx, clients)
	if done {
		return outcome
	}
	if pool == nil {
		return s.createPool(ctx, clients, guards)
	}
	workers, outcome, done := s.observe(ctx, clients, pool)
	if done {
		return outcome
	}
	if guards.Blocked() {
		// No mutation, but a scale-down in flight is settled first instead of frozen (reconcileGuardBlocked).
		return s.reconcileGuardBlocked(ctx, clients, pool, workers, guards)
	}
	return s.reconcileSize(ctx, clients, pool, workers, guards.DesiredSizePerZone)
}

// loadOwners reads the Cluster's IBMCloudManagedControlPlane and IBMCloudManagedCluster (each nil when the Cluster
// does not name one of that kind or it does not exist) and every IBMCloudManagedMachinePool of the Cluster. ctx is the
// reconcile context. It returns an error only when the API server fails.
func (s *MachinePoolScope) loadOwners(ctx context.Context) error {
	cpRef := s.Cluster.Spec.ControlPlaneRef
	if cpRef.APIGroup == controlplanev1.GroupVersion.Group && cpRef.Kind == poolControlPlaneKind {
		cp := &controlplanev1.IBMCloudManagedControlPlane{}
		found, err := s.getOwner(ctx, cpRef.Name, cp)
		if err != nil {
			return err
		}
		if found {
			s.ControlPlane = cp
		}
	}
	infraRef := s.Cluster.Spec.InfrastructureRef
	if infraRef.APIGroup == infrav1.GroupVersion.Group && infraRef.Kind == poolInfraClusterKind {
		infra := &infrav1.IBMCloudManagedCluster{}
		found, err := s.getOwner(ctx, infraRef.Name, infra)
		if err != nil {
			return err
		}
		if found {
			s.InfraCluster = infra
		}
	}
	pools := &infrav1.IBMCloudManagedMachinePoolList{}
	if err := s.params.Client.List(ctx, pools, client.InNamespace(s.Cluster.Namespace),
		client.MatchingLabels{clusterv1.ClusterNameLabel: s.Cluster.Name}); err != nil {
		return fmt.Errorf("listing the IBMCloudManagedMachinePools of Cluster %s: %w", s.Cluster.Name, err)
	}
	s.clusterPools = pools.Items
	return nil
}

// getOwner reads one object of the Cluster's namespace and reports whether it exists.
//
// Parameters:
//   - ctx is the reconcile context.
//   - name is the object's name.
//   - obj receives the object.
func (s *MachinePoolScope) getOwner(ctx context.Context, name string, obj client.Object) (bool, error) {
	err := s.params.Client.Get(ctx, types.NamespacedName{Namespace: s.Cluster.Namespace, Name: name}, obj)
	switch {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading %T %s/%s of Cluster %s: %w", obj, s.Cluster.Namespace, name, s.Cluster.Name, err)
	}
	return true, nil
}

// clusterDeletingReason returns why the IBM cluster is going away, or "" when it is not: the Cluster or its control
// plane is being deleted. IBM then removes every pool with the cluster, so a pool makes no IBM and no workload-cluster
// call: no pool call and no drain, which would only push pods onto Nodes that go away too. It reads the control plane
// loadOwners found; a missing control plane is not a deletion here (ownersMissing and ReconcileDelete decide that
// case).
func (s *MachinePoolScope) clusterDeletingReason() string {
	switch {
	case !s.Cluster.DeletionTimestamp.IsZero():
		return "the Cluster is being deleted; IBM removes every pool with the cluster"
	case s.ControlPlane != nil && !s.ControlPlane.DeletionTimestamp.IsZero():
		return "the control plane is being deleted; IBM removes every pool with the cluster"
	}
	return ""
}

// ownersMissing reports the outcome when the control plane or the IBMCloudManagedCluster is missing. A Cluster that
// names another kind needs a human; a missing object usually only needs time, and its watch or the requeue brings
// the pool back.
func (s *MachinePoolScope) ownersMissing() (Outcome, bool) {
	cpRef, infraRef := s.Cluster.Spec.ControlPlaneRef, s.Cluster.Spec.InfrastructureRef
	switch {
	case cpRef.APIGroup != controlplanev1.GroupVersion.Group || cpRef.Kind != poolControlPlaneKind:
		message := fmt.Sprintf("Cluster %s has no IBMCloudManagedControlPlane as spec.controlPlaneRef; an "+
			"IBMCloudManagedMachinePool belongs to a managed (IKS or ROKS) Cluster only", s.Cluster.Name)
		s.report(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason, message)
		return Terminal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason, message), true
	case s.ControlPlane == nil:
		return s.waitForControlPlane(fmt.Sprintf("IBMCloudManagedControlPlane %s of Cluster %s does not exist yet",
			cpRef.Name, s.Cluster.Name)), true
	case infraRef.APIGroup != infrav1.GroupVersion.Group || infraRef.Kind != poolInfraClusterKind:
		message := fmt.Sprintf("Cluster %s has no IBMCloudManagedCluster as spec.infrastructureRef; an "+
			"IBMCloudManagedMachinePool belongs to a managed (IKS or ROKS) Cluster only", s.Cluster.Name)
		s.report(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason, message)
		return Terminal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason, message), true
	case s.InfraCluster == nil:
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason
		s.report(reason, fmt.Sprintf("IBMCloudManagedCluster %s of Cluster %s does not exist yet", infraRef.Name, s.Cluster.Name))
		return NotDone(IntervalPoolTransition, reason), true
	}
	return Outcome{}, false
}

// waitForControlPlane reports WaitingForControlPlane with message and waits clusterProvisioningPoll; the control plane
// watch usually brings the pool back earlier.
func (s *MachinePoolScope) waitForControlPlane(message string) Outcome {
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason
	s.report(reason, message)
	return NotDone(IntervalClusterProvisioning, reason)
}

// guardOutcome reports the failing guard and returns its outcome: WaitingForInfrastructure only needs time, every other
// guard needs a human (an edit of the MachinePool or of the pool re-triggers the reconcile). guards is PoolGuards'
// answer; it is blocked.
func (s *MachinePoolScope) guardOutcome(guards PoolGuardResult) Outcome {
	s.report(guards.Reason, guards.Message)
	if guards.Reason == infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason {
		return NotDone(IntervalPoolTransition, guards.Reason)
	}
	return Terminal(guards.Reason, guards.Message)
}

// ibm resolves the Cluster's identity with its IBMCloudManagedCluster as the holder and builds the IBM Cloud
// Kubernetes Service client for the IBMCloudManagedCluster's region, on first use, memoized for this reconcile only,
// so that the scope constructor makes no call and a failure is persisted as a condition. The pool and the control
// plane never act as holders. ctx is the reconcile context.
func (s *MachinePoolScope) ibm(ctx context.Context) (*poolClients, error) {
	if s.clients != nil {
		return s.clients, nil
	}
	if s.InfraCluster == nil {
		return nil, errors.New("managed: MachinePoolScope.ibm called before the IBMCloudManagedCluster was loaded (programming error)")
	}
	identity, err := s.params.Resolver.Resolve(ctx, s.InfraCluster)
	if err != nil {
		return nil, fmt.Errorf("resolving the IBM Cloud identity of IBMCloudManagedCluster %s: %w", s.InfraCluster.Name, err)
	}
	k, err := s.params.ClientBuilder.KubernetesService(ctx, identity, s.InfraCluster.Spec.Region)
	if err != nil {
		return nil, &clientBuildError{identity: identity, err: fmt.Errorf("building the IBM Cloud Kubernetes Service client of %s for region %s: %w",
			identity.Description, s.InfraCluster.Spec.Region, err)}
	}
	s.clients = &poolClients{identity: identity, kubernetes: k}
	return s.clients, nil
}

// credentialsProblem reports a failure of ibm, before any IBM call, and returns it as Transient. The message names
// the identity by its Description when it resolved but never carries the error text, which the outcome hands to the
// controller log, so the condition stays the same while the same failure repeats. err is the error of ibm.
func (s *MachinePoolScope) credentialsProblem(err error) Outcome {
	message := fmt.Sprintf("CAPIBM cannot resolve the IBM Cloud credentials of IBMCloudManagedCluster %s; the controller "+
		"log names the cause", s.InfraCluster.Name)
	var buildErr *clientBuildError
	if errors.As(err, &buildErr) {
		message = fmt.Sprintf("CAPIBM cannot build the IBM Cloud Kubernetes Service client of %s; the controller log names "+
			"the cause", buildErr.identity.Description)
	}
	s.report(MachinePoolIBMAPIErrorReason, message)
	return Transient(err)
}

// findPool lists the IBM pools of the cluster and returns the one named spec.poolName, or nil when IBM has none. It
// decides the cases that end the reconcile: a failed listing, a pool IBM still removes (PoolNameBeingReleased) and a
// pool of another flavor (PoolConflict, which also stops publication). For an IBM pool CAPIBM did not create it
// records status.poolID and emits WorkerPoolAdopted, except for the pool IBM creates together with the cluster
// (comesWithCluster).
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
func (s *MachinePoolScope) findPool(ctx context.Context, clients *poolClients) (*kubernetesservice.WorkerPool, Outcome, bool) {
	clusterID := s.ControlPlane.Status.ClusterID
	pools, err := clients.kubernetes.ListWorkerPools(ctx, clusterID)
	if err != nil {
		return nil, s.ibmProblem(clients, kubernetesservice.OpListWorkerPools,
			fmt.Sprintf("listing the worker pools of IBM cluster %s", clusterID), err), true
	}
	pool := poolNamed(pools, s.Pool.Spec.PoolName)
	if pool == nil {
		return nil, Outcome{}, false
	}
	if slices.Contains(PoolNameReleasedStates(), pool.ActualState) {
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameBeingReleasedReason
		s.report(reason, fmt.Sprintf("IBM still removes a worker pool named %s from cluster %s (state %s); the pool is "+
			"created again once IBM has released the name", pool.Name, clusterID, pool.ActualState))
		return nil, NotDone(IntervalPoolTransition, reason), true
	}
	if pool.Flavor != s.Pool.Spec.Flavor {
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolConflictReason
		message := fmt.Sprintf("IBM worker pool %s of cluster %s has flavor %s, but spec.flavor is %s; it is not this "+
			"object's pool, so CAPIBM neither publishes its workers, sizes it nor removes it; choose another spec.poolName",
			pool.Name, clusterID, pool.Flavor, s.Pool.Spec.Flavor)
		s.report(reason, message)
		return nil, Terminal(reason, message), true
	}
	if s.Pool.Status.PoolID == "" && !s.comesWithCluster() {
		// Every pool found without a recorded ID, other than the one IBM creates with the cluster, was made outside
		// this object (or before a clusterctl move), and from now on CAPIBM sizes and removes it. The event is the only
		// effect of the marker here; every pool is found by name and owned alike.
		s.params.Recorder.Eventf(s.Pool, corev1.EventTypeNormal, EventWorkerPoolAdopted,
			"CAPIBM now sizes IBM worker pool %s (%s) and removes it when this object is deleted", pool.Name, pool.ID)
	}
	s.Pool.Status.PoolID = pool.ID
	s.addNote(zonesNote(pool, s.Pool))
	return pool, Outcome{}, false
}

// comesWithCluster reports whether this pool is the one IBM creates together with the cluster, so that finding it is
// expected and no adoption: it is marked spec.creation WithCluster and the control plane is not initialized yet. The
// marker, not the pool name, tells it apart, because any name is valid for that pool (IBM honours a custom pool name
// at create). The control plane reads the marker only until the IBM cluster exists, so once the control plane is
// initialized a WithCluster pool is handled like any other: one that names a pool made by hand takes it over with
// WorkerPoolAdopted. The price is one extra WorkerPoolAdopted event for the initial pool when this controller first
// lists it only after the control plane was initialized, for example after a clusterctl move; that is accepted.
func (s *MachinePoolScope) comesWithCluster() bool {
	return s.Pool.Spec.Creation == infrav1.PoolCreationWithCluster &&
		!ptr.Deref(s.ControlPlane.Status.Initialization.ControlPlaneInitialized, false)
}

// createPool creates the IBM pool when every guard passes, the control plane is initialized and the cluster state
// allows it. The zones carry the subnet of each zone from the IBMCloudManagedCluster (zones[].subnetID of IBM's
// createWorkerPool). A 409 answer means the pool exists or is being created; it is adopted by name on the next
// reconcile.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - guards is PoolGuards' answer.
func (s *MachinePoolScope) createPool(ctx context.Context, clients *poolClients, guards PoolGuardResult) Outcome {
	if guards.Blocked() {
		return s.guardOutcome(guards)
	}
	clusterID := s.ControlPlane.Status.ClusterID
	// Only the wait message depends on spec.creation. Once the control plane is initialized, a WithCluster pool that
	// IBM does not list (an adopted cluster, a marker set after the create, a pool removed out of band) is created like
	// any other pool: the control plane reads the marker only before it creates the IBM cluster.
	if !ptr.Deref(s.ControlPlane.Status.Initialization.ControlPlaneInitialized, false) {
		message := fmt.Sprintf("the master of IBM cluster %s is not ready yet; pool %s is created once "+
			"IBMCloudManagedControlPlane %s is initialized", clusterID, s.Pool.Spec.PoolName, s.ControlPlane.Name)
		if s.Pool.Spec.Creation == infrav1.PoolCreationWithCluster {
			message = fmt.Sprintf("IBM creates pool %s together with cluster %s (spec.creation: %s); waiting for it to "+
				"be listed", s.Pool.Spec.PoolName, clusterID, infrav1.PoolCreationWithCluster)
		}
		return s.waitForControlPlane(message)
	}
	if outcome, blocked := s.clusterStateGate(PoolOperationCreate); blocked {
		return outcome
	}
	req, problem := s.createRequest(guards.DesiredSizePerZone)
	if problem != "" {
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason
		s.report(reason, problem)
		return NotDone(IntervalPoolTransition, reason)
	}
	poolID, err := clients.kubernetes.CreateWorkerPool(ctx, req)
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason
	switch kubernetesservice.Classify(kubernetesservice.OpCreateWorkerPool, err) {
	case kubernetesservice.ClassNone:
		s.Pool.Status.PoolID = poolID
		s.report(reason, fmt.Sprintf("IBM is creating pool %s with %d workers in each of %d zones",
			s.Pool.Spec.PoolName, req.WorkerCountPerZone, len(req.Zones)))
		return NotDone(IntervalPoolTransition, reason)
	case kubernetesservice.ClassPoolNameExists, kubernetesservice.ClassOperationInProgress:
		s.report(reason, fmt.Sprintf("IBM reports that a pool named %s exists or is being created in cluster %s; it is "+
			"adopted by name on the next reconcile", s.Pool.Spec.PoolName, clusterID))
		return NotDone(IntervalClusterProvisioning, reason)
	default:
		return s.ibmProblem(clients, kubernetesservice.OpCreateWorkerPool,
			fmt.Sprintf("creating worker pool %s in IBM cluster %s", s.Pool.Spec.PoolName, clusterID), err)
	}
}

// createRequest builds the CreateWorkerPool request from the pool, the control plane and the subnets the
// IBMCloudManagedCluster resolved. It returns a message instead when the IBMCloudManagedCluster has not published the
// VPC or a zone's subnet yet. desired is the workers per zone (PoolGuardResult.DesiredSizePerZone).
func (s *MachinePoolScope) createRequest(desired int64) (kubernetesservice.CreateWorkerPoolRequest, string) {
	infra := s.InfraCluster
	req := kubernetesservice.CreateWorkerPoolRequest{
		ClusterID:          s.ControlPlane.Status.ClusterID,
		Name:               s.Pool.Spec.PoolName,
		Flavor:             s.Pool.Spec.Flavor,
		VPCID:              infra.Status.VPC.ID,
		WorkerCountPerZone: desired,
	}
	if req.VPCID == "" {
		return req, fmt.Sprintf("IBMCloudManagedCluster %s has not resolved its VPC yet", infra.Name)
	}
	for _, zone := range s.Pool.Spec.Zones {
		subnetID := ""
		for _, resolved := range infra.Status.Zones {
			if resolved.Name == zone.Name {
				subnetID = resolved.Subnet.ID
			}
		}
		if subnetID == "" {
			return req, fmt.Sprintf("IBMCloudManagedCluster %s has not resolved the subnet of zone %s yet", infra.Name, zone.Name)
		}
		req.Zones = append(req.Zones, kubernetesservice.ZoneSubnet{Zone: zone.Name, SubnetID: subnetID})
	}
	if s.Pool.Spec.Entitlement == infrav1.EntitlementOCPEntitled {
		req.Entitlement = createRequestOCPEntitled
	}
	return req, ""
}

// clusterStateGate applies PoolMutationAllowed to the IBM cluster state the control plane observed: without it IBM's
// documented refusal would look like an invalid configuration. op is the pool operation. It returns the outcome and
// true when the state forbids op.
func (s *MachinePoolScope) clusterStateGate(op PoolOperation) (Outcome, bool) {
	state := s.ControlPlane.Status.State
	if PoolMutationAllowed(state, op) {
		return Outcome{}, false
	}
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason
	s.report(reason, clusterStateMessage(s.ControlPlane.Status.ClusterID, state))
	return NotDone(IntervalGuarded, reason), true
}

// clusterStateMessage is the WaitingForClusterState message of a pool operation that PoolMutationAllowed denies in
// state: unavailable and scaled_down accept no pool change at all, while unsupported refuses only the operations
// that add or reload workers (create, grow, replace), so its message says that and names the fix. Every
// cluster-state gate of the pool, before a mutation and in the pre-call gate of a removal, uses it, so that one state
// always reads the same.
//
// Parameters:
//   - clusterID is the IBM cluster ID.
//   - state is the IBM cluster state that denied the operation.
func clusterStateMessage(clusterID, state string) string {
	if state == clusterStateUnsupported {
		return fmt.Sprintf("IBM cluster %s runs an unsupported version: IBM does not add or reload workers on an "+
			"unsupported version; upgrade the master first", clusterID)
	}
	return fmt.Sprintf("IBM cluster %s is %s; IBM accepts no worker pool change in that state", clusterID, state)
}

// observe lists the workers of the cluster and publishes what the pool listing and the worker listing say:
// status.poolID, sizePerZone, zones, the worker versions, the sticky ready and provisioned flags, the first
// lastDisruptionTime, spec.providerIDList with status.missingWorkers, and status.replicas. A failed listing leaves
// spec.providerIDList and status.replicas as they are, because CAPI deletes the Node of every provider ID that leaves
// the list.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - pool is the IBM pool of this object.
func (s *MachinePoolScope) observe(ctx context.Context, clients *poolClients, pool *kubernetesservice.WorkerPool) ([]kubernetesservice.Worker, Outcome, bool) {
	clusterID := s.ControlPlane.Status.ClusterID
	workers, err := clients.kubernetes.ListWorkers(ctx, clusterID)
	if err != nil {
		return nil, s.ibmProblem(clients, kubernetesservice.OpListWorkers, fmt.Sprintf("listing the workers of IBM cluster %s", clusterID), err), true
	}
	s.recordListing(workers)
	if err := s.publishPool(pool, workers); err != nil {
		return nil, Transient(err), true
	}
	if err := s.publishProviderIDs(pool.ID, workers); err != nil {
		if s.ControlPlane.Status.ClusterCRN == "" {
			return nil, s.waitForControlPlane(fmt.Sprintf("IBMCloudManagedControlPlane %s has not published the cluster "+
				"CRN yet; spec.providerIDList is built from its account", s.ControlPlane.Name)), true
		}
		return nil, Transient(fmt.Errorf("building spec.providerIDList of %s: %w", s.objectName(), err)), true
	}
	return workers, Outcome{}, false
}

// ListedWorkers returns every worker of the cluster from this reconcile's successful ListWorkers call, and true; nil
// and false when no listing succeeded. The pool controller refreshes its first-seen map (TransitionalSince) from it
// after each reconcile, so that every listing a pool of the Cluster makes keeps the map current without another IBM
// call. Call it after ReconcileNormal or ReconcileDelete.
func (s *MachinePoolScope) ListedWorkers() ([]kubernetesservice.Worker, bool) {
	return s.listedWorkers, s.workersListed
}

// recordListing keeps the answer of a successful ListWorkers call for ListedWorkers. workers are every worker of the
// cluster, as IBM listed them.
func (s *MachinePoolScope) recordListing(workers []kubernetesservice.Worker) {
	s.listedWorkers, s.workersListed = workers, true
}

// publishPool writes the pool facts and the worker versions into status and sets the sticky ready and provisioned
// flags: the IBM pool exists (CAPI v1.14 reads status.ready, later releases initialization.provisioned; neither is
// ever set back to false, not even when a worker fails). An empty lastDisruptionTime becomes now, so that no
// disruptive call starts inside IBM's reflection lag of a disruption that a lost status recorded (a new object,
// clusterctl move, a restore).
//
// Parameters:
//   - pool is the IBM pool of this object.
//   - workers are every worker of the cluster from one successful listing.
func (s *MachinePoolScope) publishPool(pool *kubernetesservice.WorkerPool, workers []kubernetesservice.Worker) error {
	status := &s.Pool.Status
	status.PoolID = pool.ID
	status.SizePerZone = ptr.To(clampInt32(pool.SizePerZone))
	status.Zones = observedZones(pool.Zones)
	summary, err := SummarizeWorkerVersions(workers, pool.ID, s.masterMinor())
	if err != nil {
		return err
	}
	status.WorkerVersions = summary.WorkerVersions
	// Status string fields have MinLength=1: an unknown target is "", which the omitempty JSON tag leaves out of the
	// patch; it is never written as a value.
	status.TargetWorkerVersion = summary.TargetWorkerVersion
	status.WorkersWithUpdateAvailable = ptr.To(summary.WorkersWithUpdateAvailable)
	status.Ready = ptr.To(true)
	status.Initialization.Provisioned = ptr.To(true)
	if status.LastDisruptionTime.IsZero() {
		status.LastDisruptionTime = metav1.NewTime(s.params.Clock.Now())
	}
	return nil
}

// publishProviderIDs rebuilds spec.providerIDList and status.missingWorkers with BuildProviderIDList and publishes
// status.replicas = len(spec.providerIDList) in the same patch. The list is written only when it changed. On an error
// both are left as they are: CAPI would delete the Node of every provider ID that leaves the list. status.replicas is
// published in every case, 0 included.
//
// Parameters:
//   - poolID is the IBM pool ID.
//   - workers are every worker of the cluster from one successful listing.
func (s *MachinePoolScope) publishProviderIDs(poolID string, workers []kubernetesservice.Worker) error {
	defer func() {
		s.Pool.Status.Replicas = ptr.To(clampInt32(int64(len(s.Pool.Spec.ProviderIDList))))
	}()
	account, err := AccountIDFromCRN(s.ControlPlane.Status.ClusterCRN)
	if err != nil {
		return err
	}
	result, err := BuildProviderIDList(ProviderIDListInput{
		AccountID: account,
		ClusterID: s.ControlPlane.Status.ClusterID,
		PoolID:    poolID,
		Workers:   workers,
		Current:   s.Pool.Spec.ProviderIDList,
		Missing:   s.Pool.Status.MissingWorkers,
		Now:       s.params.Clock.Now(),
		DropAfter: s.dropAfter,
	})
	if err != nil {
		return err
	}
	if !slices.Equal(result.ProviderIDList, s.Pool.Spec.ProviderIDList) {
		s.Pool.Spec.ProviderIDList = result.ProviderIDList
	}
	s.Pool.Status.MissingWorkers = result.MissingWorkers
	return nil
}

// masterMinor returns the minor of the master version the control plane observed, or the zero Minor when it is not
// known yet; then no worker counts as a minor behind (UpdateAvailability).
func (s *MachinePoolScope) masterMinor() ibmversion.Minor {
	version, err := ibmversion.Parse(s.ControlPlane.Status.DistributionVersion)
	if err != nil {
		return ibmversion.Minor{}
	}
	return version.MinorVersion()
}

// reconcileSize sizes the pool and makes at most one disruptive step per reconcile, in this priority: a removal in
// flight, then the scale-down, then a worker rollout (machinepool_rollout.go). A scale-down in flight runs before
// anything else, also when it is to be given up because spec.replicas went back up, so that its cordons are reverted
// before the pool grows; so does a removal whose status was lost, which this pool's drain-owner markers on its Nodes
// reveal, unless the marked Node is the one worker a lost replace was draining (workerRollout.chooseVictim). An
// autoscaled pool is never resized, while a replace in flight on it runs on and a new one waits in its
// ExternallyAutoscaled gate. Growth is requested at once, also while a replace is in flight (it removes nothing, so it
// is not disruptive); a smaller MachinePool starts the scale-down unless a replace is in flight. Otherwise the rollout
// runs, after WorkerPoolReady reports the pool's readiness. WorkersUpToDate is reported in every case: the rollout's
// step, or why it waits.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - pool is the IBM pool.
//   - workers are every worker of the cluster from this reconcile's listing.
//   - desired is the size per zone the MachinePool asks for.
func (s *MachinePoolScope) reconcileSize(ctx context.Context, clients *poolClients, pool *kubernetesservice.WorkerPool,
	workers []kubernetesservice.Worker, desired int64) Outcome {
	input, err := s.newScaleDownInput(ctx, clients, pool, workers, desired)
	if err != nil {
		return Transient(err)
	}
	rollout, err := s.newWorkerRollout(ctx, input, false)
	if err != nil {
		return Transient(err)
	}
	inFlight := s.Pool.Status.Disruption.Operation
	replacing := inFlight == infrav1.WorkerDisruptionOperationReplace || rollout.resumed
	switch {
	case inFlight == infrav1.WorkerDisruptionOperationScaleDown || (inFlight == "" && len(input.marked) > 0 && !rollout.resumed):
		rollout.reportHeldByScaleDown()
		return s.reconcileScaleDown(ctx, input)
	case pool.AutoscaleEnabled:
		s.setWorkerPoolReady(metav1.ConditionTrue, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyExternallyAutoscaledReason,
			fmt.Sprintf("IBM's cluster autoscaler owns the size of pool %s; CAPIBM does not apply MachinePool %s spec.replicas",
				pool.Name, s.MachinePool.Name))
		outcome := rollout.reconcile(ctx, Done())
		if !replacing && outcome.Kind == OutcomeNotDone {
			// A new replace waits in its gates (ExternallyAutoscaled) for a human; the pool itself is fine.
			return Done()
		}
		return outcome
	case desired > pool.SizePerZone:
		if err := s.releaseIdleToken(ctx, input.workload, input.marked); err != nil {
			return Transient(err)
		}
		return s.growPool(ctx, clients, pool, desired)
	case desired < pool.SizePerZone && !replacing:
		rollout.reportHeldByScaleDown()
		return s.reconcileScaleDown(ctx, input)
	}
	return rollout.reconcile(ctx, s.reportReadiness(pool, workers, desired))
}

// growPool asks IBM for more workers per zone (growth is not disruptive and runs at once) when the cluster state
// allows it.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - pool is the IBM pool.
//   - desired is the size per zone the MachinePool asks for; larger than the pool's.
func (s *MachinePoolScope) growPool(ctx context.Context, clients *poolClients, pool *kubernetesservice.WorkerPool, desired int64) Outcome {
	if outcome, blocked := s.clusterStateGate(PoolOperationGrow); blocked {
		return outcome
	}
	clusterID := s.ControlPlane.Status.ClusterID
	if err := clients.kubernetes.ResizeWorkerPool(ctx, clusterID, pool.ID, desired); err != nil {
		return s.ibmProblem(clients, kubernetesservice.OpResizeWorkerPool,
			fmt.Sprintf("growing worker pool %s of IBM cluster %s to %d workers per zone", pool.Name, clusterID, desired), err)
	}
	reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason
	s.report(reason, fmt.Sprintf("IBM grows pool %s from %d to %d workers per zone", pool.Name, pool.SizePerZone, desired))
	return NotDone(IntervalPoolTransition, reason)
}

// reportReadiness sets WorkerPoolReady from the observed pool: True when IBM's size per zone is the desired one, the
// pool has the desired number of workers and every one of them is ready.
//
// Parameters:
//   - pool is the IBM pool.
//   - workers are every worker of the cluster from this reconcile's listing.
//   - desired is the size per zone the MachinePool asks for.
func (s *MachinePoolScope) reportReadiness(pool *kubernetesservice.WorkerPool, workers []kubernetesservice.Worker, desired int64) Outcome {
	total, ready, changing := 0, 0, 0
	for _, w := range workers {
		if w.PoolID != pool.ID || w.ActualState == WorkerStateDeleted {
			continue
		}
		total++
		if IsWorkerReady(w) {
			ready++
		}
		if IsWorkerTransitional(w) {
			changing++
		}
	}
	desiredTotal := desired * int64(len(s.Pool.Spec.Zones))
	counts := fmt.Sprintf("%d of %d workers of pool %s are ready; MachinePool %s asks for %d", ready, total, pool.Name,
		s.MachinePool.Name, desiredTotal)
	switch {
	case pool.SizePerZone != desired:
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason
		s.report(reason, fmt.Sprintf("IBM pool %s has %d workers per zone and MachinePool %s asks for %d; %s", pool.Name,
			pool.SizePerZone, s.MachinePool.Name, desired, counts))
		return NotDone(IntervalGuarded, reason)
	case ready == total && int64(total) == desiredTotal:
		s.setWorkerPoolReady(metav1.ConditionTrue, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReason, counts)
		return Done()
	case changing > 0 || int64(total) < desiredTotal:
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason
		s.report(reason, counts+"; IBM is still changing workers")
		return NotDone(IntervalPoolTransition, reason)
	default:
		reason := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWorkersNotReadyReason
		s.report(reason, counts)
		return NotDone(IntervalGuarded, reason)
	}
}

// ibmProblem turns a failed IBM Cloud Kubernetes Service call into an outcome by its kubernetesservice.Classify
// class. A 401 invalidates only this identity's authenticator; 401 and 403 messages name the identity by its
// Description; a 400 and a 403 need a human. Transient failures and 429 leave the conditions as they are, so a blip
// does not flip a ready pool. Messages carry the HTTP status and IBM's code, never IBM's description or incident ID,
// so a repeated failure writes nothing new; the outcome hands the full error to the controller log.
//
// Parameters:
//   - clients are this reconcile's IBM clients; their identity made the call.
//   - op is the failed operation; Classify reads the error per operation.
//   - action says what the call did, for example "listing the workers of IBM cluster c1".
//   - err is the call's error.
func (s *MachinePoolScope) ibmProblem(clients *poolClients, op kubernetesservice.Operation, action string, err error) Outcome {
	return s.ibmProblemOn(s.report, clients, op, action, err)
}

// ibmProblemOn is ibmProblem with the report of the problem left to report instead of MachinePoolScope.report: a
// worker removal reports IBM's answer to its call on the condition it reports its steps on, which for a worker replace
// is WorkersUpToDate (removalRunnerInput.reportCallError). Classification, outcome and the 401 handling are the same.
//
// Parameters:
//   - report sets the condition the problem is reported on, from the reason and the message; it is called at most
//     once, and not for a failure that leaves the conditions as they are (429, transient).
//   - clients are this reconcile's IBM clients; their identity made the call.
//   - op is the failed operation; Classify reads the error per operation.
//   - action says what the call did, for example "listing the workers of IBM cluster c1".
//   - err is the call's error.
func (s *MachinePoolScope) ibmProblemOn(report func(reason, message string), clients *poolClients, op kubernetesservice.Operation,
	action string, err error) Outcome {
	reason := MachinePoolIBMAPIErrorReason
	statusCode, code := 0, ""
	var apiErr *kubernetesservice.APIError
	if errors.As(err, &apiErr) {
		statusCode, code = apiErr.StatusCode, apiErr.Code
	}
	answer := fmt.Sprintf("HTTP %d", statusCode)
	if code != "" {
		answer += ", code " + code
	}
	switch kubernetesservice.Classify(op, err) {
	case kubernetesservice.ClassUnauthorized:
		// Only this identity's authenticator is rebuilt, on its next use; other identities keep theirs.
		s.params.ClientBuilder.InvalidateAuthenticator(clients.identity.Key)
		report(reason, fmt.Sprintf("IBM Cloud rejected the credentials of %s while %s; CAPIBM rebuilds them and retries",
			clients.identity.Description, action))
		return NotDone(IntervalSteadyState, reason)
	case kubernetesservice.ClassPermissionDenied, kubernetesservice.ClassAutoscaled:
		// IBM's autoscaled-pool 403 means ExternallyAutoscaled only for a pool IBM reports autoscaleEnabled, and every
		// caller that can get it decides that case itself: reconcileSize never resizes an autoscaled pool, and
		// DisruptionCallOutcome holds a removal of an autoscaled pool as ExternallyAutoscaled. Here the pool is not
		// autoscaled, so the 403 is a missing role like any other; left to the default case it would be retried as a
		// transient failure with the fast backoff.
		message := fmt.Sprintf("IBM Cloud refused %s (%s) while %s; grant it %s (the IAM roles of the managed Kubernetes book)",
			clients.identity.Description, answer, action, poolKubernetesServiceRoles)
		report(reason, message)
		return Terminal(reason, message)
	case kubernetesservice.ClassBadRequest:
		message := fmt.Sprintf("IBM Cloud refused the request (%s) while %s; check spec.flavor and spec.zones of %s; the "+
			"controller log has IBM's description", answer, action, s.objectName())
		report(reason, message)
		return Terminal(reason, message)
	case kubernetesservice.ClassNotFound:
		report(reason, fmt.Sprintf("IBM Cloud answered %s while %s: the IBM cluster or pool is gone; CAPIBM looks again", answer, action))
		return NotDone(IntervalSteadyState, reason)
	case kubernetesservice.ClassRateLimited:
		return RateLimited(kubernetesservice.RateLimitBackoff(err))
	default:
		return Transient(fmt.Errorf("%s: %w", action, err))
	}
}

// report sets the condition a pool step reports its waits and problems on: WorkerPoolReady=False while the pool lives,
// Deleting=True while it is being deleted (the delete flow sets Deleting=True with its specific reason).
//
// Parameters:
//   - reason is the condition reason.
//   - message explains it; deterministic and free of IBM text.
func (s *MachinePoolScope) report(reason, message string) {
	if !s.Pool.DeletionTimestamp.IsZero() {
		conditions.Set(s.Pool, metav1.Condition{
			Type:    infrav1.IBMCloudManagedMachinePoolDeletingCondition,
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		})
		return
	}
	s.setWorkerPoolReady(metav1.ConditionFalse, reason, message)
}

// setWorkerPoolReady sets WorkerPoolReady with the non-blocking notes of this reconcile appended to message.
//
// Parameters:
//   - status is the condition status.
//   - reason is the condition reason.
//   - message explains it.
func (s *MachinePoolScope) setWorkerPoolReady(status metav1.ConditionStatus, reason, message string) {
	conditions.Set(s.Pool, metav1.Condition{
		Type:    infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition,
		Status:  status,
		Reason:  reason,
		Message: strings.Join(append([]string{message}, s.notes...), "; "),
	})
}

// addNote records a non-blocking WorkerPoolReady message; "" is ignored. note is the message.
func (s *MachinePoolScope) addNote(note string) {
	if note != "" && !slices.Contains(s.notes, note) {
		s.notes = append(s.notes, note)
	}
}

// objectName names the reconciled object in messages, for example IBMCloudManagedMachinePool/workers.
func (s *MachinePoolScope) objectName() string {
	return "IBMCloudManagedMachinePool/" + s.Pool.Name
}

// poolNamed returns the pool named name, or nil. IBM keeps pool names unique within a cluster; should a listing ever
// carry two, the first one that IBM is not removing wins, so a pool that is going away never hides the live one.
//
// Parameters:
//   - pools are IBM's pools of the cluster.
//   - name is spec.poolName.
func poolNamed(pools []kubernetesservice.WorkerPool, name string) *kubernetesservice.WorkerPool {
	var found *kubernetesservice.WorkerPool
	for i := range pools {
		if pools[i].Name != name {
			continue
		}
		if found == nil || (slices.Contains(PoolNameReleasedStates(), found.ActualState) &&
			!slices.Contains(PoolNameReleasedStates(), pools[i].ActualState)) {
			found = &pools[i]
		}
	}
	return found
}

// observedZones returns status.zones from IBM's pool zones, sorted by name so that the same pool always writes the
// same list and a repeated reconcile writes nothing new. zones are IBM's pool zones.
func observedZones(zones []kubernetesservice.WorkerPoolZone) []infrav1.ObservedPoolZone {
	observed := make([]infrav1.ObservedPoolZone, 0, len(zones))
	for _, z := range zones {
		if z.ID == "" {
			continue
		}
		observed = append(observed, infrav1.ObservedPoolZone{Name: z.ID, WorkerCount: ptr.To(clampInt32(z.WorkerCount))})
	}
	slices.SortFunc(observed, func(a, b infrav1.ObservedPoolZone) int { return strings.Compare(a.Name, b.Name) })
	return observed
}

// zonesNote returns a non-blocking message when the IBM pool's zones differ from spec.zones, else "". Pool zones are
// immutable in v1alpha1, so CAPIBM changes neither side.
//
// Parameters:
//   - pool is the IBM pool.
//   - object is the IBMCloudManagedMachinePool.
func zonesNote(pool *kubernetesservice.WorkerPool, object *infrav1.IBMCloudManagedMachinePool) string {
	ibmZones := make([]string, 0, len(pool.Zones))
	for _, z := range pool.Zones {
		ibmZones = append(ibmZones, z.ID)
	}
	specZones := poolZoneNames(object)
	slices.Sort(ibmZones)
	slices.Sort(specZones)
	if slices.Equal(ibmZones, specZones) {
		return ""
	}
	return fmt.Sprintf("IBM pool %s has the zones %s while spec.zones lists %s; CAPIBM changes the zones of neither",
		pool.Name, noneIfEmpty(ibmZones), noneIfEmpty(specZones))
}
