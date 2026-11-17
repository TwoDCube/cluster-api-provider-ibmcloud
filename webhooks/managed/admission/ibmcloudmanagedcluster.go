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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// Ensure IBMCloudManagedCluster implements the typed validator interface. There is no defaulter (package doc).
var _ admission.Validator[*infrav1.IBMCloudManagedCluster] = &IBMCloudManagedCluster{}

//+kubebuilder:webhook:verbs=create;update;delete,path=/validate-infrastructure-cluster-x-k8s-io-v1alpha1-ibmcloudmanagedcluster,mutating=false,failurePolicy=fail,groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedclusters,versions=v1alpha1,name=vibmcloudmanagedcluster.kb.io,sideEffects=None,admissionReviewVersions=v1;v1beta1

// IBMCloudManagedCluster validates IBMCloudManagedCluster objects: the feature gate on create and the delete guard,
// which keeps the object while the Cluster's control plane may still need it, whatever the feature gate value. Every
// field rule is a CEL rule on the type. It never calls IBM Cloud.
type IBMCloudManagedCluster struct {
	// ManagedKubernetesEnabled is the value of the ManagedKubernetes feature gate. When false, CREATE is rejected and
	// updates are allowed unchecked, but DELETE still passes the delete guard. The object has no finalizer, so a
	// deletion admitted while the gate is off would remove it at once, and the control plane, which is reconciled again
	// once the gate is on, would then be unable to prove and delete its IBM cluster.
	ManagedKubernetesEnabled bool

	// Reader reads the owning Cluster and its IBMCloudManagedControlPlane for the delete guard. It must be the manager's
	// API reader (mgr.GetAPIReader()), see the package documentation; nil is a construction error.
	Reader client.Reader
}

// SetupWebhookWithManager registers the validator with mgr's webhook server. It fails when Reader is nil, so that a
// wiring mistake stops the manager at startup instead of failing admission requests.
//
// mgr is the manager whose webhook server serves the validator.
func (r *IBMCloudManagedCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	if err := requireReader("IBMCloudManagedCluster", r.Reader); err != nil {
		return err
	}
	return ctrl.NewWebhookManagedBy(mgr, &infrav1.IBMCloudManagedCluster{}).
		WithValidator(r).
		Complete()
}

// ValidateCreate rejects the create while the feature gate is off and otherwise allows it.
//
// ctx is unused; infra is the IBMCloudManagedCluster being created.
func (r *IBMCloudManagedCluster) ValidateCreate(_ context.Context, infra *infrav1.IBMCloudManagedCluster) (admission.Warnings, error) {
	if !r.ManagedKubernetesEnabled {
		return nil, gateOffCreateError(clusterResource, "IBMCloudManagedCluster", infra.Name)
	}
	return nil, nil
}

// ValidateUpdate allows every update: the field rules (immutability, add-only zones) are CEL rules on the type.
//
// ctx, oldInfra and newInfra are unused.
func (r *IBMCloudManagedCluster) ValidateUpdate(_ context.Context, _, _ *infrav1.IBMCloudManagedCluster) (admission.Warnings, error) {
	return nil, nil
}

// ValidateDelete is the delete guard of IBMCloudManagedCluster. The IBMCloudManagedCluster has no finalizer, so a
// deletion removes it at once, while the Cluster's IBMCloudManagedControlPlane still needs its spec.resourceGroup.id to
// prove which IBM cluster it owns (on every path, its delete flow included) and resolves its credential identity
// through it: without it the control plane's delete flow keeps its finalizer with WaitingForInfrastructure and never
// deletes the IBM cluster, which keeps running and billing. The deletion is allowed when clusterctl move deletes the
// object (delete-for-move), when the owning Cluster is gone, when the Cluster's spec.infrastructureRef does not name
// this IBMCloudManagedCluster, when the Cluster's spec.controlPlaneRef names no IBMCloudManagedControlPlane or that
// control plane is gone, and when that control plane cannot own an IBM cluster yet (noIBMClusterCanExist, the test the
// control plane's delete guard uses). Otherwise it is rejected: with "delete Cluster <name> instead" while the Cluster
// lives, and with "leave this deletion to Cluster API" while the Cluster is being deleted. The guard runs whatever the
// feature gate value (see ManagedKubernetesEnabled).
//
// A Cluster that is being deleted does not exempt the object, unlike in the control plane's guard. Cluster API deletes
// the InfraCluster only after the control plane is gone (reconcileDelete in
// core/reconcilers/cluster/cluster_controller.go, CAPI v1.14.4), so on Cluster API's own path the "control plane is
// gone" exemption already applies. An earlier deletion comes from a tool that deletes every object of the cluster at
// once (kubectl delete -f, a GitOps prune), and admitting it would strand the control plane's delete flow; rejecting it
// cannot block Cluster API, and a retry succeeds once the control plane is gone. A Cluster that is gone still exempts
// the object: the control plane's controller reads its owner Cluster before anything else (util.GetOwnerCluster), so
// it cannot run its delete flow, nor use this object, until the Cluster exists again.
//
// ctx is the admission request's context, used for the reads; infra is the IBMCloudManagedCluster being deleted.
func (r *IBMCloudManagedCluster) ValidateDelete(ctx context.Context, infra *infrav1.IBMCloudManagedCluster) (admission.Warnings, error) {
	if isBeingMoved(infra) {
		return nil, nil
	}
	if err := requireReader("IBMCloudManagedCluster", r.Reader); err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	cluster, err := ownerCluster(ctx, r.Reader, infra, "IBMCloudManagedCluster")
	if err != nil || cluster == nil {
		return nil, err
	}
	// The control plane and the pools find their InfraCluster only through Cluster.spec.infrastructureRef, and resolve
	// their credentials only from it. CAPI v1.14.0 lets a user point that reference at another InfraCluster (its
	// Cluster webhook forbids only removing it), while the old object keeps the cluster-name label and the owner
	// reference that led here. Nothing reads the old object any more, so guarding it would block every prune of it
	// for as long as the Cluster lives, with a rejection message that is not true. The check needs no extra read.
	if !refersTo(cluster.Spec.InfrastructureRef, infraClusterGroupKind, infra.Name) {
		return nil, nil
	}
	ref := cluster.Spec.ControlPlaneRef
	if !ref.IsDefined() || ref.GroupKind() != controlPlaneGroupKind {
		return nil, nil
	}
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: ref.Name}
	if err := r.Reader.Get(ctx, key, cp); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, readError("IBMCloudManagedControlPlane", key, "IBMCloudManagedCluster", infra.Name, err)
	}
	if noIBMClusterCanExist(cp) {
		return nil, nil
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return nil, apierrors.NewForbidden(clusterResource, infra.Name, deletingClusterError(cluster.Name, cp))
	}
	return nil, apierrors.NewForbidden(clusterResource, infra.Name, fmt.Errorf(
		"delete Cluster %s instead; IBMCloudManagedControlPlane/%s still needs this IBMCloudManagedCluster to manage and "+
			"delete its IBM cluster", cluster.Name, cp.Name))
}

// deletingClusterError is the rejection of an IBMCloudManagedCluster deletion while its Cluster is being deleted and
// the Cluster's control plane may still own an IBM cluster. It tells the user that nothing needs to be done, because
// Cluster API deletes the object itself once the control plane is gone, and names the IBM cluster when the control
// plane has recorded its ID, so that the user can follow its deletion in IBM Cloud.
//
// clusterName is the name of the Cluster being deleted; cp is the Cluster's control plane, which must not be nil.
func deletingClusterError(clusterName string, cp *controlplanev1.IBMCloudManagedControlPlane) error {
	target := "its IBM cluster"
	if cp.Status.ClusterID != "" {
		target = "IBM cluster " + cp.Status.ClusterID
	}
	return fmt.Errorf("leave this deletion to Cluster API: Cluster %s is being deleted, and "+
		"IBMCloudManagedControlPlane/%s still needs this IBMCloudManagedCluster to delete %s; Cluster API deletes this "+
		"object once that control plane is gone, and a retried deletion succeeds from then on", clusterName, cp.Name,
		target)
}
