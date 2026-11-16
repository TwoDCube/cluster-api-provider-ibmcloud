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
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/paused"
	"sigs.k8s.io/cluster-api/util/predicates"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

// ibmCloudManagedClusterKind is the kind name a Cluster's spec.infrastructureRef uses for this controller's objects.
const ibmCloudManagedClusterKind = "IBMCloudManagedCluster"

// IBMCloudManagedClusterReconciler reconciles IBMCloudManagedCluster objects, the InfraCluster of a managed (IKS or
// ROKS) cluster. It validates the referenced VPC network and publishes it; it creates and deletes nothing in IBM Cloud,
// so it needs no finalizer.
type IBMCloudManagedClusterReconciler struct {
	// Client is the manager's client.
	Client client.Client
	// Resolver resolves the IBM Cloud identity of a Cluster, always with the IBMCloudManagedCluster as the holder;
	// credentials.ControllerOnly in this feature.
	Resolver credentials.Resolver
	// ClientBuilder builds the VPC and Resource Manager clients for the resolved identity.
	ClientBuilder managedscope.ClientBuilder
	// WatchFilterValue is the --watch-filter label value; objects without it are ignored.
	WatchFilterValue string
}

// The controller patches the status of its objects (no finalizer, no spec writes) and reads the owning Clusters.
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedclusters,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch

// Reconcile validates the network of one IBMCloudManagedCluster, in Cluster API's KubeadmControlPlane order: object,
// owner, pause, deletion, patch helper, scope. No IBM call happens before the owner is known, while paused, or while
// the object is being deleted.
func (r *IBMCloudManagedClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	log := ctrl.LoggerFrom(ctx)

	infra := &infrav1.IBMCloudManagedCluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, infra); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting IBMCloudManagedCluster %s: %w", req.NamespacedName, err)
	}

	cluster, err := util.GetOwnerCluster(ctx, r.Client, infra.ObjectMeta)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("getting the owner Cluster of IBMCloudManagedCluster %s: %w", req.NamespacedName, err)
	}
	if cluster == nil {
		// The Cluster controller sets the owner reference; its update event brings the object back.
		log.V(4).Info("Waiting for the Cluster controller to set the owner reference")
		return ctrl.Result{}, nil
	}
	log = log.WithValues("Cluster", klog.KObj(cluster))
	ctx = ctrl.LoggerInto(ctx, log)

	// No finalizer: CAPIBM creates nothing in IBM Cloud for this object, and the IBMCloudManagedCluster webhook refuses a
	// stand-alone delete while the control plane may still need it.

	// Pause is checked before deletion handling, so that a paused object causes no IBM call on any path.
	if isPaused, requeue, err := paused.EnsurePausedCondition(ctx, r.Client, cluster, infra); err != nil || isPaused || requeue {
		return ctrl.Result{}, err
	}

	if !infra.DeletionTimestamp.IsZero() {
		// Nothing to clean up in IBM Cloud; the object goes as soon as other finalizers, if any, are gone.
		return ctrl.Result{}, nil
	}

	patchHelper, err := patch.NewHelper(infra, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating the patch helper of IBMCloudManagedCluster %s: %w", req.NamespacedName, err)
	}
	// The deferred patch persists every condition a step sets, including a credential resolution failure, which
	// happens only inside the step: the scope resolves the credentials lazily, after the patch helper exists.
	defer func() {
		if err := patchIBMCloudManagedCluster(ctx, patchHelper, infra); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	scope, err := managedscope.NewClusterScope(&managedscope.ScopeParams{
		Client:        r.Client,
		Resolver:      r.Resolver,
		ClientBuilder: r.ClientBuilder,
	}, cluster, infra)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("creating the scope of IBMCloudManagedCluster %s: %w", req.NamespacedName, err)
	}
	return r.reconcileNormal(ctx, scope)
}

// reconcileNormal validates and publishes the network and requeues: every infraRevalidatePoll when it is confirmed, so
// that a deleted subnet or VPC shows up in NetworkReady. scope is this reconcile's scope.
func (r *IBMCloudManagedClusterReconciler) reconcileNormal(ctx context.Context, scope *managedscope.ClusterScope) (ctrl.Result, error) {
	outcome := scope.ReconcileNetwork(ctx)
	if outcome.Kind == managedscope.OutcomeTerminal {
		// A human must fix the spec or the VPC; the condition says what, and the log keeps it next to the reconcile.
		ctrl.LoggerFrom(ctx).Info("The network of the IBMCloudManagedCluster is not usable", "reason", outcome.Reason, "message", outcome.Message)
	}
	return resultFor(outcome, managedscope.IntervalInfraRevalidate)
}

// patchIBMCloudManagedCluster computes the Ready summary from NetworkReady and patches the object, owning the
// conditions this controller writes.
//
// Parameters:
//   - ctx is the reconcile context.
//   - patchHelper was created from the object as it was read.
//   - infra is the object with this reconcile's changes.
func patchIBMCloudManagedCluster(ctx context.Context, patchHelper *patch.Helper, infra *infrav1.IBMCloudManagedCluster) error {
	if err := conditions.SetSummaryCondition(infra, infra, infrav1.IBMCloudManagedClusterReadyCondition,
		conditions.ForConditionTypes{infrav1.IBMCloudManagedClusterNetworkReadyCondition},
	); err != nil {
		return fmt.Errorf("computing the %s condition: %w", infrav1.IBMCloudManagedClusterReadyCondition, err)
	}
	return patchHelper.Patch(ctx, infra, patch.WithOwnedConditions{Conditions: []string{
		infrav1.IBMCloudManagedClusterReadyCondition,
		infrav1.IBMCloudManagedClusterNetworkReadyCondition,
		clusterv1.PausedCondition,
	}})
}

// SetupWithManager registers the controller with mgr. It fails when a dependency is missing, so that cmd/main.go
// stops at start-up instead of panicking in the first reconcile.
//
// Parameters:
//   - ctx is the setup context; its logger is used by the predicates.
//   - mgr is the controller manager.
func (r *IBMCloudManagedClusterReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := r.validate(); err != nil {
		return err
	}
	predicateLog := ctrl.LoggerFrom(ctx).WithValues("controller", "ibmcloudmanagedcluster")
	err := ctrl.NewControllerManagedBy(mgr).
		// No generation predicate on For: status-only changes made by others must still be reconciled; this
		// controller writes status only when it changes, so its own patches do not keep re-triggering it.
		For(&infrav1.IBMCloudManagedCluster{}).
		WithEventFilter(predicates.ResourceHasFilterLabel(mgr.GetScheme(), predicateLog, r.WatchFilterValue)).
		WithEventFilter(predicates.ResourceIsNotExternallyManaged(mgr.GetScheme(), predicateLog)).
		Watches(
			&clusterv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(util.ClusterToInfrastructureMapFunc(ctx,
				infrav1.GroupVersion.WithKind(ibmCloudManagedClusterKind), mgr.GetClient(), &infrav1.IBMCloudManagedCluster{})),
			// Only pause transitions of the Cluster matter: the network does not depend on anything else in it.
			builder.WithPredicates(predicates.ClusterPausedTransitions(mgr.GetScheme(), predicateLog)),
		).
		Complete(r)
	if err != nil {
		return fmt.Errorf("setting up the IBMCloudManagedCluster controller: %w", err)
	}
	return nil
}

// validate names every missing dependency of the reconciler.
func (r *IBMCloudManagedClusterReconciler) validate() error {
	var errs []error
	if core.IsNil(r.Client) {
		errs = append(errs, errors.New("IBMCloudManagedClusterReconciler.Client is nil; pass mgr.GetClient()"))
	}
	if core.IsNil(r.Resolver) {
		errs = append(errs, errors.New("IBMCloudManagedClusterReconciler.Resolver is nil; pass credentials.ControllerOnly{}"))
	}
	if core.IsNil(r.ClientBuilder) {
		errs = append(errs, errors.New("IBMCloudManagedClusterReconciler.ClientBuilder is nil; pass the client builder from managedscope.NewProdClientBuilder"))
	}
	return errors.Join(errs...)
}
