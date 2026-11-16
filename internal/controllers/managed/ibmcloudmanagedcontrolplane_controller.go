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

	"github.com/IBM/go-sdk-core/v5/core"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/finalizers"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/paused"
	"sigs.k8s.io/cluster-api/util/predicates"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"
)

// IBMCloudManagedControlPlaneReconciler reconciles IBMCloudManagedControlPlane objects: one IBM Cloud Kubernetes Service
// or Red Hat OpenShift on IBM Cloud cluster on VPC Gen2 each. The control plane is externally managed: IBM runs the
// master, and CAPIBM binds the object to one IBM cluster, creates it when none exists and deletes it with the Cluster.
type IBMCloudManagedControlPlaneReconciler struct {
	// Client is the manager's client.
	Client client.Client
	// APIReader reads uncached: the reconciled control plane itself (Reconcile says why) and the kubeconfig Secret
	// (CAPIBM runs no cluster-wide Secret informer).
	APIReader client.Reader
	// Recorder emits the control plane's events.
	Recorder record.EventRecorder
	// Resolver resolves the IBM Cloud identity of a Cluster, always with its IBMCloudManagedCluster as the holder;
	// credentials.ControllerOnly in this feature.
	Resolver credentials.Resolver
	// ClientBuilder builds the Kubernetes Service client for the resolved identity.
	ClientBuilder managedscope.ClientBuilder
	// Prober verifies kubeconfigs against the workload API server before they are written and on every steady reconcile.
	Prober workloadprobe.Prober
	// VersionsCache caches IBM's offered versions per identity and region, so that a listing made with one identity's
	// credentials is never served to another.
	VersionsCache *managedscope.VersionsCache
	// Clock is the time source of every time-based decision; clock.RealClock{} in production.
	Clock clock.PassiveClock
	// WatchFilterValue is the --watch-filter label value; objects without it are ignored.
	WatchFilterValue string
}

// The controller manages its objects (finalizer, status), reads their Clusters, InfraClusters, MachinePools and
// IBMCloudManagedMachinePools, writes the kubeconfig Secret and emits events.
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=ibmcloudmanagedcontrolplanes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=ibmcloudmanagedcontrolplanes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=ibmcloudmanagedcontrolplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinepools,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedmachinepools,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile reconciles one IBMCloudManagedControlPlane in Cluster API's KubeadmControlPlane order: object, owner,
// finalizer, pause, patch helper, scope, then deletion or the normal flow. A control plane that no Cluster owns never
// gets a finalizer and never causes an IBM call; a paused one causes none on any path, deletion included.
func (r *IBMCloudManagedControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	log := ctrl.LoggerFrom(ctx)

	// The control plane is read uncached. Its own writes (the create intent before a POST, then the answer) re-trigger
	// this reconcile, and the cache can still hold the version before the last write: a reconcile on that version would
	// act on an attempt that has already been answered and overwrite the answer's condition, which would void the hold
	// after a create refusal (cpCreateHold) and send the next create at once. One GET per reconcile is the price; the
	// other objects stay cached.
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting IBMCloudManagedControlPlane %s: %w", req.NamespacedName, err)
	}

	cluster, err := util.GetOwnerCluster(ctx, r.Client, cp.ObjectMeta)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting the owner Cluster of IBMCloudManagedControlPlane %s: %w", req.NamespacedName, err)
	}
	if cluster == nil {
		// The Cluster controller sets the owner reference for the Cluster whose spec.controlPlaneRef names this object;
		// its update event brings the object back. Until then: no finalizer, no IBM call, so an unowned object can always be
		// deleted.
		log.V(4).Info("Waiting for the Cluster controller to set the owner reference")
		return ctrl.Result{}, nil
	}
	log = log.WithValues("Cluster", klog.KObj(cluster))
	ctx = ctrl.LoggerInto(ctx, log)

	// The finalizer comes only after the owner, so that an object no Cluster owns can always be deleted; the update event
	// of the finalizer patch re-triggers.
	if added, err := finalizers.EnsureFinalizer(ctx, r.Client, cp, controlplanev1.IBMCloudManagedControlPlaneFinalizer); err != nil || added {
		return ctrl.Result{}, err
	}

	// Pause is checked before deletion handling, so that a paused object causes no IBM call on any path.
	if isPaused, requeue, err := paused.EnsurePausedCondition(ctx, r.Client, cluster, cp); err != nil || isPaused || requeue {
		return ctrl.Result{}, err
	}

	patcher, err := newControlPlanePatcher(r.Client, cp)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating the patch helper of IBMCloudManagedControlPlane %s: %w", req.NamespacedName, err)
	}
	// The deferred patch persists every condition a step sets, including a credential resolution failure, which
	// happens only inside a step: the scope resolves the credentials lazily, after the patch helper exists.
	defer func() {
		if err := patcher.persist(ctx); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	scope, err := managedscope.NewControlPlaneScope(r.scopeParams(), cluster, cp, patcher.persist)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating the scope of IBMCloudManagedControlPlane %s: %w", req.NamespacedName, err)
	}
	if !cp.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, scope)
	}
	return r.reconcileNormal(ctx, scope)
}

// reconcileNormal binds the control plane to its IBM cluster and reports the cluster, then requeues at the interval the
// scope chose, or at steadyStatePoll when nothing is in progress. scope is this reconcile's scope.
func (r *IBMCloudManagedControlPlaneReconciler) reconcileNormal(ctx context.Context, scope *managedscope.ControlPlaneScope) (ctrl.Result, error) {
	outcome := scope.ReconcileNormal(ctx)
	logControlPlaneTerminal(ctx, outcome)
	return resultFor(outcome, managedscope.IntervalSteadyState)
}

// reconcileDelete removes the finalizer when the deletion cannot leave an IBM cluster behind: clusterctl move deletes
// the source copy (the target takes over the IBM cluster), or no IBM cluster can exist for this control plane (no
// status.clusterID, no spec.controlPlaneEndpoint and no create intent). Every other deletion keeps the finalizer,
// because this build does not delete IBM clusters. Neither path makes an IBM call. scope is this reconcile's scope.
func (r *IBMCloudManagedControlPlaneReconciler) reconcileDelete(_ context.Context, scope *managedscope.ControlPlaneScope) (ctrl.Result, error) {
	cp := scope.ControlPlane
	if controlPlaneBeingMoved(cp) || managedscope.NoIBMClusterCanExist(cp) {
		controllerutil.RemoveFinalizer(cp, controlplanev1.IBMCloudManagedControlPlaneFinalizer)
		return ctrl.Result{}, nil
	}
	conditions.Set(cp, metav1.Condition{
		Type:   controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
		Status: metav1.ConditionTrue,
		Reason: controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason,
		Message: fmt.Sprintf("this CAPIBM build does not delete IBM clusters; delete IBM cluster %s in IBM Cloud, then remove "+
			"the finalizer %s by hand", cp.Status.ClusterID, controlplanev1.IBMCloudManagedControlPlaneFinalizer),
	})
	return resultFor(managedscope.NotDone(managedscope.IntervalTerminalRecheck,
		controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason), managedscope.IntervalSteadyState)
}

// scopeParams returns the reconciler's dependencies as the scope's parameters.
func (r *IBMCloudManagedControlPlaneReconciler) scopeParams() *managedscope.ScopeParams {
	return &managedscope.ScopeParams{
		Client:        r.Client,
		APIReader:     r.APIReader,
		Recorder:      r.Recorder,
		Resolver:      r.Resolver,
		ClientBuilder: r.ClientBuilder,
		Clock:         r.Clock,
		Prober:        r.Prober,
		VersionsCache: r.VersionsCache,
	}
}

// logControlPlaneTerminal logs an outcome that needs a human next to its reconcile; the condition says what to fix.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - outcome is the step's outcome; only a Terminal one is logged.
func logControlPlaneTerminal(ctx context.Context, outcome managedscope.Outcome) {
	if outcome.Kind == managedscope.OutcomeTerminal {
		ctrl.LoggerFrom(ctx).Info("The control plane needs a human", "reason", outcome.Reason, "message", outcome.Message)
	}
}

// controlPlaneBeingMoved reports whether clusterctl move is deleting cp from the source management cluster:
// clusterctl sets the delete-for-move annotation on every object it moves before it deletes it there, and the copy on
// the target takes over the IBM cluster, so the source must make no IBM call. cp is the control plane.
func controlPlaneBeingMoved(cp *controlplanev1.IBMCloudManagedControlPlane) bool {
	_, ok := cp.Annotations[clusterctlv1.DeleteForMoveAnnotation]
	return ok
}

// controlPlaneOwnedConditions are the conditions the control plane controller writes; the patch helper resolves
// conflicts on them in this controller's favour.
var controlPlaneOwnedConditions = []string{
	controlplanev1.IBMCloudManagedControlPlaneReadyCondition,
	controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
	controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
	controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
	controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
	controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition,
	controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
	controlplanev1.IBMCloudManagedControlPlanePausedCondition,
}

// controlPlanePatcher persists the changes one reconcile makes to a control plane. After every successful write it
// takes the written object as its new baseline, so that a later write in the same reconcile, including a field cleared
// again after an earlier write set it, is computed against what the API server holds.
type controlPlanePatcher struct {
	client client.Client
	helper *patch.Helper
	cp     *controlplanev1.IBMCloudManagedControlPlane
}

// newControlPlanePatcher returns a patcher whose baseline is cp as read.
//
// Parameters:
//   - c is the manager's client.
//   - cp is the control plane as read from the API server; the patcher writes this object.
func newControlPlanePatcher(c client.Client, cp *controlplanev1.IBMCloudManagedControlPlane) (*controlPlanePatcher, error) {
	helper, err := patch.NewHelper(cp, c)
	if err != nil {
		return nil, err
	}
	return &controlPlanePatcher{client: c, helper: helper, cp: cp}, nil
}

// persist computes the Ready summary and patches the control plane, owning the conditions of
// controlPlaneOwnedConditions, then rebases the patcher on the written object. ctx is the reconcile context.
func (p *controlPlanePatcher) persist(ctx context.Context) error {
	if err := conditions.SetSummaryCondition(p.cp, p.cp, controlplanev1.IBMCloudManagedControlPlaneReadyCondition,
		conditions.ForConditionTypes{
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
			controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
		},
	); err != nil {
		return fmt.Errorf("computing the %s condition: %w", controlplanev1.IBMCloudManagedControlPlaneReadyCondition, err)
	}
	if err := p.helper.Patch(ctx, p.cp, patch.WithOwnedConditions{Conditions: controlPlaneOwnedConditions}); err != nil {
		return err
	}
	helper, err := patch.NewHelper(p.cp, p.client)
	if err != nil {
		return fmt.Errorf("rebasing the patch helper of IBMCloudManagedControlPlane %s: %w", client.ObjectKeyFromObject(p.cp), err)
	}
	p.helper = helper
	return nil
}

// SetupWithManager registers the controller with mgr. It fails when a dependency is missing, so that cmd/main.go stops
// at start-up instead of panicking in the first reconcile.
//
// Parameters:
//   - ctx is the setup context; its logger is used by the predicates and the map functions.
//   - mgr is the controller manager.
func (r *IBMCloudManagedControlPlaneReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := r.validate(); err != nil {
		return err
	}
	predicateLog := ctrl.LoggerFrom(ctx).WithValues("controller", "ibmcloudmanagedcontrolplane")
	err := ctrl.NewControllerManagedBy(mgr).
		// No generation predicate on For: status-only changes made by others must still be reconciled, and a
		// generation filter would drop the update event of the finalizer patch that the reconcile relies on. This
		// controller's own status patches change nothing when nothing changed.
		For(&controlplanev1.IBMCloudManagedControlPlane{}).
		WithEventFilter(predicates.ResourceHasFilterLabel(mgr.GetScheme(), predicateLog, r.WatchFilterValue)).
		WithEventFilter(predicates.ResourceIsNotExternallyManaged(mgr.GetScheme(), predicateLog)).
		Watches(
			&clusterv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.clusterToControlPlane),
			// Pause transitions and the infrastructure becoming provisioned, which the create waits for.
			builder.WithPredicates(predicates.ClusterPausedTransitionsOrInfrastructureProvisioned(mgr.GetScheme(), predicateLog)),
		).
		// The initial worker pool is created with the cluster, and a deleting control plane waits for the pools of its
		// Cluster; both react to pool events, and only those (machinePoolToControlPlane).
		Watches(
			&infrav1.IBMCloudManagedMachinePool{},
			handler.EnqueueRequestsFromMapFunc(r.machinePoolToControlPlane),
		).
		Complete(r)
	if err != nil {
		return fmt.Errorf("setting up the IBMCloudManagedControlPlane controller: %w", err)
	}
	return nil
}

// clusterToControlPlane maps a Cluster to its IBMCloudManagedControlPlane, through spec.controlPlaneRef.
//
// Parameters:
//   - ctx is unused.
//   - o is the Cluster.
func (r *IBMCloudManagedControlPlaneReconciler) clusterToControlPlane(_ context.Context, o client.Object) []ctrl.Request {
	cluster, ok := o.(*clusterv1.Cluster)
	if !ok {
		return nil
	}
	return controlPlaneRequestOf(cluster)
}

// machinePoolToControlPlane maps an IBMCloudManagedMachinePool to the IBMCloudManagedControlPlane of the same Cluster,
// found through the pool's cluster-name label and the Cluster's spec.controlPlaneRef, while that control plane waits
// for pools: before its IBM cluster is created or bound (no status.clusterID; the initial pool sizes the create) and
// while it is being deleted (it waits until no pool is left). Every reconcile of a bound control plane costs a
// GetCluster, and the pool controller writes status through drains, rollouts and resizes, so a bound, live control
// plane is not woken by pool events and keeps its IBM request budget of one call per steadyStatePoll.
//
// Parameters:
//   - ctx is the watch context.
//   - o is the IBMCloudManagedMachinePool.
func (r *IBMCloudManagedControlPlaneReconciler) machinePoolToControlPlane(ctx context.Context, o client.Object) []ctrl.Request {
	clusterName := o.GetLabels()[clusterv1.ClusterNameLabel]
	if clusterName == "" {
		return nil
	}
	cluster := &clusterv1.Cluster{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: o.GetNamespace(), Name: clusterName}, cluster); err != nil {
		// A missing Cluster has no control plane to wake; another read error is retried by the next pool event or
		// the control plane's own requeue.
		return nil
	}
	requests := controlPlaneRequestOf(cluster)
	if len(requests) == 0 {
		return nil
	}
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	if err := r.Client.Get(ctx, requests[0].NamespacedName, cp); err != nil {
		// A control plane that is not there yet is reconciled by its own create event; a failed read is covered by its
		// own requeue (clusterProvisioningPoll before create, deletePoll while deleting).
		return nil
	}
	if cp.Status.ClusterID != "" && cp.DeletionTimestamp.IsZero() {
		return nil
	}
	return requests
}

// controlPlaneRequestOf returns the request of cluster's IBMCloudManagedControlPlane, or none when the Cluster's control
// plane is of another kind. cluster is the Cluster.
func controlPlaneRequestOf(cluster *clusterv1.Cluster) []ctrl.Request {
	if !IsIBMCloudManagedCluster(cluster) || cluster.Spec.ControlPlaneRef.Name == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.ControlPlaneRef.Name}}}
}

// validate names every missing dependency of the reconciler.
func (r *IBMCloudManagedControlPlaneReconciler) validate() error {
	var errs []error
	missing := func(unset bool, field, hint string) {
		if unset {
			errs = append(errs, fmt.Errorf("IBMCloudManagedControlPlaneReconciler.%s is nil; %s", field, hint))
		}
	}
	missing(core.IsNil(r.Client), "Client", "pass mgr.GetClient()")
	missing(core.IsNil(r.APIReader), "APIReader", "pass mgr.GetAPIReader()")
	missing(core.IsNil(r.Recorder), "Recorder", `pass mgr.GetEventRecorderFor("ibmcloudmanagedcontrolplane-controller")`)
	missing(core.IsNil(r.Resolver), "Resolver", "pass credentials.ControllerOnly{}")
	missing(core.IsNil(r.ClientBuilder), "ClientBuilder", "pass the client builder from managedscope.NewProdClientBuilder")
	missing(core.IsNil(r.Prober), "Prober", "pass the prober from workloadprobe.NewRESTProber")
	missing(r.VersionsCache == nil, "VersionsCache", "pass the cache from managedscope.NewVersionsCache")
	missing(core.IsNil(r.Clock), "Clock", "pass clock.RealClock{}")
	return errors.Join(errs...)
}
