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

	"github.com/IBM/go-sdk-core/v5/core"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/paused"
	"sigs.k8s.io/cluster-api/util/predicates"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

const (
	// ibmCloudManagedMachinePoolKind is the kind name a MachinePool's spec.template.spec.infrastructureRef uses for this
	// controller's objects.
	ibmCloudManagedMachinePoolKind = "IBMCloudManagedMachinePool"

	// machinePoolControllerName names the controller, also for the ClusterCache source of connection events.
	machinePoolControllerName = "ibmcloudmanagedmachinepool"
)

// machinePoolOwnedConditions are the conditions this controller writes; the patch helper resolves conflicts on them
// in its favour (patch.WithOwnedConditions).
var machinePoolOwnedConditions = []string{
	infrav1.IBMCloudManagedMachinePoolReadyCondition,
	infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition,
	infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition,
	infrav1.IBMCloudManagedMachinePoolDrainingCondition,
	infrav1.IBMCloudManagedMachinePoolDeletingCondition,
	clusterv1.PausedCondition,
}

// IBMCloudManagedMachinePoolReconciler reconciles IBMCloudManagedMachinePool objects, the InfraMachinePool of a managed
// (IKS or ROKS) cluster: one IBM worker pool. It adopts or creates the pool, publishes its workers as
// spec.providerIDList and grows it; every IBM call goes through the scope, with the credentials resolved from the
// Cluster's IBMCloudManagedCluster.
type IBMCloudManagedMachinePoolReconciler struct {
	// Client is the manager's client.
	Client client.Client
	// APIReader reads uncached from the API server (the disruption token and its holder).
	APIReader client.Reader
	// Recorder emits the pool events.
	Recorder record.EventRecorder
	// Resolver resolves the IBM Cloud identity of a Cluster, always with its IBMCloudManagedCluster as the holder;
	// credentials.ControllerOnly in this feature.
	Resolver credentials.Resolver
	// ClientBuilder builds the IBM Cloud Kubernetes Service client for the resolved identity.
	ClientBuilder managedscope.ClientBuilder
	// ClusterCache gives the workload-cluster client for the drain and its connection events.
	ClusterCache clustercache.ClusterCache
	// Clock is the time source of every time-based decision.
	Clock clock.PassiveClock
	// WatchFilterValue is the --watch-filter label value; objects without it are ignored.
	WatchFilterValue string
}

// The controller writes the pool's spec.providerIDList and status, reads its owners and lists the pools of a Cluster.
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedmachinepools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedmachinepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=ibmcloudmanagedcontrolplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinepools;clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile reconciles one IBMCloudManagedMachinePool in Cluster API's KubeadmControlPlane order: object, owning
// MachinePool, Cluster, pause, deletion, patch helper, scope. No IBM call happens before the owner is known or while
// paused.
func (r *IBMCloudManagedMachinePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	log := ctrl.LoggerFrom(ctx)

	pool := &infrav1.IBMCloudManagedMachinePool{}
	if err := r.Client.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting IBMCloudManagedMachinePool %s: %w", req.NamespacedName, err)
	}

	machinePool, cluster, err := r.owners(ctx, pool)
	if err != nil || cluster == nil {
		// Without an owner the object never gets a finalizer and causes no IBM call, so a pool no MachinePool references can
		// always be deleted; the MachinePool watch brings it back once a MachinePool references it.
		return ctrl.Result{}, err
	}
	log = log.WithValues("MachinePool", klog.KObj(machinePool), "Cluster", klog.KObj(cluster))
	ctx = ctrl.LoggerInto(ctx, log)

	// Pause is checked before deletion handling, so that a paused object causes no IBM and no workload-cluster call
	// on any path: clusterctl move pauses the Cluster, and a paused object must not act while it moves.
	if isPaused, requeue, err := paused.EnsurePausedCondition(ctx, r.Client, cluster, pool); err != nil || isPaused || requeue {
		return ctrl.Result{}, err
	}

	if !pool.DeletionTimestamp.IsZero() {
		// This controller sets no finalizer yet, so nothing holds the object and nothing in IBM Cloud is removed.
		return ctrl.Result{}, nil
	}

	patcher, err := newMachinePoolPatcher(r.Client, pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	// The deferred patch persists every condition a step sets, including a credential resolution failure, which
	// happens only inside a step: the scope resolves the credentials lazily, after the patch helper exists.
	defer func() {
		if err := patcher.Patch(ctx); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	dropAfter, err := requeueInterval(managedscope.IntervalPoolTransition)
	if err != nil {
		return ctrl.Result{}, err
	}
	scope, err := managedscope.NewMachinePoolScope(r.scopeParams(), managedscope.MachinePoolScopeInput{
		Cluster:             cluster,
		MachinePool:         machinePool,
		Pool:                pool,
		ProviderIDDropAfter: dropAfter,
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating the scope of IBMCloudManagedMachinePool %s: %w", req.NamespacedName, err)
	}
	return r.reconcileNormal(ctx, scope)
}

// owners returns the MachinePool that owns pool and the MachinePool's Cluster (util.GetOwnerMachinePool, then
// util.GetClusterFromMetadata). It returns nil owners, and no error, while either is missing: the owner reference is
// set by CAPI's MachinePool controller, whose update event brings the pool back.
//
// Parameters:
//   - ctx is the reconcile context.
//   - pool is the IBMCloudManagedMachinePool.
func (r *IBMCloudManagedMachinePoolReconciler) owners(ctx context.Context, pool *infrav1.IBMCloudManagedMachinePool) (*clusterv1.MachinePool, *clusterv1.Cluster, error) {
	log := ctrl.LoggerFrom(ctx)
	machinePool, err := util.GetOwnerMachinePool(ctx, r.Client, pool.ObjectMeta)
	switch {
	case apierrors.IsNotFound(err):
		log.V(4).Info("Waiting for the owning MachinePool to exist")
		return nil, nil, nil
	case err != nil:
		return nil, nil, fmt.Errorf("getting the owner MachinePool of IBMCloudManagedMachinePool %s: %w", klog.KObj(pool), err)
	case machinePool == nil:
		log.V(4).Info("Waiting for the MachinePool controller to set the owner reference")
		return nil, nil, nil
	}
	cluster, err := util.GetClusterFromMetadata(ctx, r.Client, machinePool.ObjectMeta)
	switch {
	case apierrors.IsNotFound(err), errors.Is(err, util.ErrNoCluster):
		log.V(4).Info("Waiting for the Cluster of the MachinePool", "MachinePool", klog.KObj(machinePool))
		return nil, nil, nil
	case err != nil:
		return nil, nil, fmt.Errorf("getting the Cluster of MachinePool %s: %w", klog.KObj(machinePool), err)
	}
	return machinePool, cluster, nil
}

// scopeParams are the dependencies the pool scope receives.
func (r *IBMCloudManagedMachinePoolReconciler) scopeParams() *managedscope.ScopeParams {
	return &managedscope.ScopeParams{
		Client:        r.Client,
		APIReader:     r.APIReader,
		Recorder:      r.Recorder,
		Resolver:      r.Resolver,
		ClientBuilder: r.ClientBuilder,
		Clock:         r.Clock,
		ClusterCache:  r.ClusterCache,
	}
}

// reconcileNormal runs the pool flow (adopt or create the IBM pool, observe it, publish its workers, grow it) and
// requeues at steadyStatePoll once the pool is ready, so that IBM's own changes (autorecovery, fix packs, health) show
// up in status. scope is this reconcile's scope.
func (r *IBMCloudManagedMachinePoolReconciler) reconcileNormal(ctx context.Context, scope *managedscope.MachinePoolScope) (ctrl.Result, error) {
	outcome := scope.ReconcileNormal(ctx)
	logMachinePoolTerminal(ctx, "The IBMCloudManagedMachinePool needs attention", outcome)
	return resultFor(outcome, managedscope.IntervalSteadyState)
}

// logMachinePoolTerminal logs a Terminal outcome next to the reconcile: a human must act, and the condition says what.
//
// Parameters:
//   - ctx carries the logger.
//   - msg is the log message.
//   - outcome is the step's outcome.
func logMachinePoolTerminal(ctx context.Context, msg string, outcome managedscope.Outcome) {
	if outcome.Kind == managedscope.OutcomeTerminal {
		ctrl.LoggerFrom(ctx).Info(msg, "reason", outcome.Reason, "message", outcome.Message)
	}
}

// machinePoolPatcher patches an IBMCloudManagedMachinePool with the Ready summary and the owned conditions. A step
// that must persist status before an IBM call patches through it, and the deferred patch then sends only what
// changed after that.
type machinePoolPatcher struct {
	client client.Client
	pool   *infrav1.IBMCloudManagedMachinePool
	helper *patch.Helper
}

// newMachinePoolPatcher returns a patcher whose patch helper starts from pool as read.
//
// Parameters:
//   - c is the manager's client.
//   - pool is the object as read.
func newMachinePoolPatcher(c client.Client, pool *infrav1.IBMCloudManagedMachinePool) (*machinePoolPatcher, error) {
	helper, err := patch.NewHelper(pool, c)
	if err != nil {
		return nil, fmt.Errorf("creating the patch helper of IBMCloudManagedMachinePool %s: %w", klog.KObj(pool), err)
	}
	return &machinePoolPatcher{client: c, pool: pool, helper: helper}, nil
}

// Patch computes the Ready summary from WorkerPoolReady only (a fix pack waiting under the Manual policy, reported in
// WorkersUpToDate, must not make a healthy pool look broken), patches the object and restarts the patch helper from
// the patched object. ctx is the reconcile context.
func (p *machinePoolPatcher) Patch(ctx context.Context) error {
	if err := conditions.SetSummaryCondition(p.pool, p.pool, infrav1.IBMCloudManagedMachinePoolReadyCondition,
		conditions.ForConditionTypes{infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition},
	); err != nil {
		return fmt.Errorf("computing the %s condition: %w", infrav1.IBMCloudManagedMachinePoolReadyCondition, err)
	}
	if err := p.helper.Patch(ctx, p.pool, patch.WithOwnedConditions{Conditions: machinePoolOwnedConditions}); err != nil {
		return fmt.Errorf("patching IBMCloudManagedMachinePool %s: %w", klog.KObj(p.pool), err)
	}
	helper, err := patch.NewHelper(p.pool, p.client)
	if err != nil {
		return fmt.Errorf("restarting the patch helper of IBMCloudManagedMachinePool %s: %w", klog.KObj(p.pool), err)
	}
	p.helper = helper
	return nil
}

// SetupWithManager registers the controller with mgr and its watches: MachinePool (replicas, drain limits, exclusion
// annotations), Cluster (pause transitions), IBMCloudManagedControlPlane (cluster ID, master minor) and the
// ClusterCache's connection events, so a pool waiting for the workload cluster resumes as soon as it connects. It
// fails when a dependency or the MachinePool CRD is missing, so that cmd/main.go stops at start-up.
//
// Parameters:
//   - ctx is the setup context; its logger is used by the predicates and the map functions.
//   - mgr is the controller manager.
func (r *IBMCloudManagedMachinePoolReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := r.validate(); err != nil {
		return err
	}
	if err := requireMachinePoolCRD(mgr.GetRESTMapper()); err != nil {
		return err
	}
	predicateLog := ctrl.LoggerFrom(ctx).WithValues("controller", machinePoolControllerName)
	err := ctrl.NewControllerManagedBy(mgr).
		Named(machinePoolControllerName).
		// No generation predicate on For: status-only changes made by others must still be reconciled; this
		// controller writes status only when it changes, so its own patches do not keep re-triggering it.
		For(&infrav1.IBMCloudManagedMachinePool{}).
		WithEventFilter(predicates.ResourceHasFilterLabel(mgr.GetScheme(), predicateLog, r.WatchFilterValue)).
		WithEventFilter(predicates.ResourceIsNotExternallyManaged(mgr.GetScheme(), predicateLog)).
		Watches(
			&clusterv1.MachinePool{},
			handler.EnqueueRequestsFromMapFunc(util.MachinePoolToInfrastructureMapFunc(ctx,
				infrav1.GroupVersion.WithKind(ibmCloudManagedMachinePoolKind))),
		).
		Watches(
			&clusterv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.clusterToIBMCloudManagedMachinePools),
			builder.WithPredicates(predicates.ClusterPausedTransitions(mgr.GetScheme(), predicateLog)),
		).
		Watches(
			&controlplanev1.IBMCloudManagedControlPlane{},
			handler.EnqueueRequestsFromMapFunc(r.controlPlaneToIBMCloudManagedMachinePools),
		).
		// Workload-cluster objects are not watched; only the connection events are, as the clustercache package asks
		// controllers to do instead of requeueing on ErrClusterNotConnected. The drain polls at drainRetryInterval, like
		// Cluster API's Machine drain.
		WatchesRawSource(r.ClusterCache.GetClusterSource(machinePoolControllerName, r.clusterToIBMCloudManagedMachinePools)).
		Complete(r)
	if err != nil {
		return fmt.Errorf("setting up the IBMCloudManagedMachinePool controller: %w", err)
	}
	return nil
}

// requireMachinePoolCRD returns an error when the API server does not serve Cluster API's MachinePool, which owns every
// IBMCloudManagedMachinePool; without it the controller would wait for owners that can never exist. mapper is the
// manager's REST mapper.
func requireMachinePoolCRD(mapper meta.RESTMapper) error {
	kind := schema.GroupKind{Group: clusterv1.GroupVersion.Group, Kind: "MachinePool"}
	if _, err := mapper.RESTMapping(kind, clusterv1.GroupVersion.Version); err != nil {
		return fmt.Errorf("the IBMCloudManagedMachinePool controller needs the Cluster API %s CRD (version %s), which the "+
			"API server does not serve; install the Cluster API core provider with its MachinePool CRD: %w",
			kind, clusterv1.GroupVersion.Version, err)
	}
	return nil
}

// clusterToIBMCloudManagedMachinePools maps a Cluster to the IBMCloudManagedMachinePools of that Cluster. It serves the
// Cluster watch and the ClusterCache's connection events.
//
// Parameters:
//   - ctx is the watch context.
//   - o is the Cluster.
func (r *IBMCloudManagedMachinePoolReconciler) clusterToIBMCloudManagedMachinePools(ctx context.Context, o client.Object) []ctrl.Request {
	return r.machinePoolsOfCluster(ctx, o.GetNamespace(), o.GetName())
}

// controlPlaneToIBMCloudManagedMachinePools maps an IBMCloudManagedControlPlane to the IBMCloudManagedMachinePools with
// the same cluster-name label: a new cluster ID or master minor changes what the pools do.
//
// Parameters:
//   - ctx is the watch context.
//   - o is the control plane.
func (r *IBMCloudManagedMachinePoolReconciler) controlPlaneToIBMCloudManagedMachinePools(ctx context.Context, o client.Object) []ctrl.Request {
	clusterName := o.GetLabels()[clusterv1.ClusterNameLabel]
	if clusterName == "" {
		return nil
	}
	return r.machinePoolsOfCluster(ctx, o.GetNamespace(), clusterName)
}

// machinePoolsOfCluster returns a request for every IBMCloudManagedMachinePool labelled with the Cluster's name, in
// name order.
//
// Parameters:
//   - ctx is the watch context.
//   - namespace is the Cluster's namespace.
//   - clusterName is the Cluster's name.
func (r *IBMCloudManagedMachinePoolReconciler) machinePoolsOfCluster(ctx context.Context, namespace, clusterName string) []ctrl.Request {
	pools := &infrav1.IBMCloudManagedMachinePoolList{}
	if err := r.Client.List(ctx, pools, client.InNamespace(namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: clusterName}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "Listing the IBMCloudManagedMachinePools of a Cluster failed", "Cluster", klog.KRef(namespace, clusterName))
		return nil
	}
	requests := make([]ctrl.Request, 0, len(pools.Items))
	for i := range pools.Items {
		requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&pools.Items[i])})
	}
	slices.SortFunc(requests, func(a, b ctrl.Request) int { return strings.Compare(a.Name, b.Name) })
	return requests
}

// validate names every missing dependency of the reconciler.
func (r *IBMCloudManagedMachinePoolReconciler) validate() error {
	var errs []error
	missing := func(unset bool, field, fix string) {
		if unset {
			errs = append(errs, fmt.Errorf("IBMCloudManagedMachinePoolReconciler.%s is nil; pass %s", field, fix))
		}
	}
	missing(core.IsNil(r.Client), "Client", "mgr.GetClient()")
	missing(core.IsNil(r.APIReader), "APIReader", "mgr.GetAPIReader()")
	missing(core.IsNil(r.Recorder), "Recorder", `mgr.GetEventRecorderFor("ibmcloudmanagedmachinepool-controller")`)
	missing(core.IsNil(r.Resolver), "Resolver", "credentials.ControllerOnly{}")
	missing(core.IsNil(r.ClientBuilder), "ClientBuilder", "the client builder from managedscope.NewProdClientBuilder")
	missing(core.IsNil(r.ClusterCache), "ClusterCache", "the ClusterCache from SetupClusterCache")
	missing(core.IsNil(r.Clock), "Clock", "clock.RealClock{}")
	return errors.Join(errs...)
}
