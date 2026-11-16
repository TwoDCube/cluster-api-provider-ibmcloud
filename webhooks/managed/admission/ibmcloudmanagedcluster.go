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
// which keeps the object while the Cluster's control plane may still need it. Every field rule is a CEL rule on the
// type. It never calls IBM Cloud.
type IBMCloudManagedCluster struct {
	// ManagedKubernetesEnabled is the value of the ManagedKubernetes feature gate. When false, CREATE is rejected and
	// every other operation is allowed unchecked, the delete guard included: no controller reconciles the kind then.
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
// stand-alone delete would remove it at once, while the Cluster's IBMCloudManagedControlPlane still needs its
// spec.resourceGroup.id to prove which IBM cluster it owns (on every path, its delete flow included) and resolves its
// credential identity through it. The deletion is allowed when the gate is off, when clusterctl move deletes the
// object (delete-for-move), when the owning Cluster is gone or being deleted, when the Cluster's
// spec.infrastructureRef does not name this IBMCloudManagedCluster, when the Cluster's spec.controlPlaneRef names no
// IBMCloudManagedControlPlane or that control plane is gone, and when that control plane cannot own an IBM cluster yet
// (noIBMClusterCanExist, the test the control plane's delete guard uses). Otherwise it is rejected with "delete
// Cluster <name> instead".
//
// ctx is the admission request's context, used for the reads; infra is the IBMCloudManagedCluster being deleted.
func (r *IBMCloudManagedCluster) ValidateDelete(ctx context.Context, infra *infrav1.IBMCloudManagedCluster) (admission.Warnings, error) {
	if !r.ManagedKubernetesEnabled || isBeingMoved(infra) {
		return nil, nil
	}
	if err := requireReader("IBMCloudManagedCluster", r.Reader); err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	cluster, err := liveOwnerCluster(ctx, r.Reader, infra, "IBMCloudManagedCluster")
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
	return nil, apierrors.NewForbidden(clusterResource, infra.Name, fmt.Errorf(
		"delete Cluster %s instead; IBMCloudManagedControlPlane/%s still needs this IBMCloudManagedCluster to manage and "+
			"delete its IBM cluster", cluster.Name, cp.Name))
}
