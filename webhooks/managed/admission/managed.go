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
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// This file holds what the three validators share: the gate-off rejection, the lookup of the owning Cluster, the
// test for a Cluster's spec references and the "no IBM cluster can exist" test of the control plane's delete guard,
// which the IBMCloudManagedCluster delete guard reuses so that both guards exempt exactly the same control planes.

// The webhooks get Clusters and IBMCloudManagedControlPlanes for the delete guards, and list the Clusters of a
// namespace to name the Cluster in the ibmCloudClusterName warning of a control plane created from a template, which
// carries neither the cluster-name label nor an owner reference yet. Getting and listing Clusters is already granted
// by the VPC controllers' markers; it is declared here as well so that the webhooks' needs do not depend on another
// package's markers.
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=ibmcloudmanagedcontrolplanes,verbs=get

var (
	// controlPlaneGroupKind identifies the IBMCloudManagedControlPlane: in a Cluster's spec.controlPlaneRef (group and
	// kind, the shape of CAPI v1beta2's ContractVersionedObjectReference) and in the control plane's Invalid errors.
	controlPlaneGroupKind = controlplanev1.GroupVersion.WithKind("IBMCloudManagedControlPlane").GroupKind()

	// infraClusterGroupKind identifies the IBMCloudManagedCluster in a Cluster's spec.infrastructureRef, which the
	// IBMCloudManagedCluster delete guard checks.
	infraClusterGroupKind = infrav1.GroupVersion.WithKind("IBMCloudManagedCluster").GroupKind()

	// The group/resource pairs that name each managed kind in Forbidden errors, for example
	// ibmcloudmanagedcontrolplanes.controlplane.cluster.x-k8s.io "dev" is forbidden: ...
	controlPlaneResource = controlplanev1.GroupVersion.WithResource("ibmcloudmanagedcontrolplanes").GroupResource()
	clusterResource      = infrav1.GroupVersion.WithResource("ibmcloudmanagedclusters").GroupResource()
	machinePoolResource  = infrav1.GroupVersion.WithResource("ibmcloudmanagedmachinepools").GroupResource()
)

// requireReader rejects a validator whose Reader is nil. Both delete guards need it, and a nil reader would otherwise
// panic inside an admission request; SetupWebhookWithManager calls it so that cmd/main.go fails at startup instead.
//
// kind names the validator in the error; reader is the validator's Reader field.
func requireReader(kind string, reader client.Reader) error {
	if reader == nil {
		return fmt.Errorf("%s webhook: the Reader field is nil; set it to the manager's API reader "+
			"(mgr.GetAPIReader()), which the delete guard uses to read the owning Cluster", kind)
	}
	return nil
}

// gateOffCreateError is the rejection of a CREATE while the ManagedKubernetes feature gate is off. Only creates are
// rejected: objects that already exist must stay updatable and deletable, and a create is the one request that cannot
// be useful while no controller reconciles the kind.
//
// resource names the kind in the error, for example ibmcloudmanagedcontrolplanes.controlplane.cluster.x-k8s.io; kind is
// the kind's name for the message text; name is the name of the object being created (it may be empty when the
// request uses generateName).
func gateOffCreateError(resource schema.GroupResource, kind, name string) error {
	return apierrors.NewForbidden(resource, name, fmt.Errorf(
		"%s objects cannot be created while the ManagedKubernetes feature gate is off, because no controller would "+
			"reconcile them; set EXP_MANAGED_KUBERNETES=true when installing or upgrading the IBM Cloud provider with "+
			"clusterctl (manager flag --feature-gates=ManagedKubernetes=true)", kind))
}

// clusterNameOf returns the name of the CAPI Cluster that obj belongs to: the cluster.x-k8s.io/cluster-name label
// when it is set, else the name in an owner reference to a cluster.x-k8s.io Cluster (any API version), else "". The
// label comes first because templates and CAPI set it, while the owner reference appears only after the Cluster
// controller has reconciled the object.
//
// obj is the managed object being admitted; only its labels and owner references are read.
func clusterNameOf(obj client.Object) string {
	if name := obj.GetLabels()[clusterv1.ClusterNameLabel]; name != "" {
		return name
	}
	for _, ref := range obj.GetOwnerReferences() {
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			// A malformed apiVersion cannot name a Cluster; the API server rejects such references anyway.
			continue
		}
		if gv.Group == clusterv1.GroupVersion.Group && ref.Kind == clusterv1.ClusterKind {
			return ref.Name
		}
	}
	return ""
}

// refersTo reports whether ref, a reference in a Cluster's spec, names the object of group kind gk called name. The
// reference carries no namespace and no version: the object lives in the Cluster's namespace, and CAPI v1beta2 resolves
// the version from the contract label of the CRD, so group, kind and name identify it.
//
// ref is the Cluster's spec.controlPlaneRef or spec.infrastructureRef; gk and name identify the object being tested.
func refersTo(ref clusterv1.ContractVersionedObjectReference, gk schema.GroupKind, name string) bool {
	return ref.GroupKind() == gk && ref.Name == name
}

// isBeingMoved reports whether clusterctl move is deleting obj from the source management cluster. clusterctl sets
// the delete-for-move annotation on every object it moves before it deletes it there; the copies on the target
// management cluster take over the IBM cluster, so the deletion must pass both delete guards.
//
// obj is the managed object being deleted.
func isBeingMoved(obj client.Object) bool {
	_, ok := obj.GetAnnotations()[clusterctlv1.DeleteForMoveAnnotation]
	return ok
}

// noIBMClusterCanExist reports whether cp cannot be bound to an IBM cluster yet: it has no status.clusterID, no
// spec.controlPlaneEndpoint and no status.createIntent. The controller writes createIntent immediately BEFORE it
// sends createCluster and controlPlaneEndpoint is the never-recreate marker that clusterctl move and restores carry,
// so a control plane without all three never sent a create and never was bound to an IBM cluster; deleting it cannot
// delete one. The IBMCloudManagedCluster delete guard uses the same test, so both guards exempt exactly the same
// control planes.
//
// cp is the control plane to test; it must not be nil.
func noIBMClusterCanExist(cp *controlplanev1.IBMCloudManagedControlPlane) bool {
	return cp.Status.ClusterID == "" &&
		cp.Spec.ControlPlaneEndpoint.Host == "" && cp.Spec.ControlPlaneEndpoint.Port == 0 &&
		!hasCreateIntent(cp.Status.CreateIntent)
}

// hasCreateIntent reports whether any field of status.createIntent is set. The struct is omitted from the API object
// when it is empty (omitzero), and its CEL MinProperties=1 means a written intent always has at least one field.
//
// intent is the control plane's status.createIntent.
func hasCreateIntent(intent controlplanev1.CreateIntent) bool {
	return intent.ClusterName != "" ||
		!intent.InFlightAttemptTime.IsZero() ||
		!intent.FirstUnknownOutcomeTime.IsZero() ||
		!intent.LastUnknownOutcomeTime.IsZero()
}

// liveOwnerCluster returns the Cluster that obj belongs to when that Cluster exists and is not being deleted, and nil
// when obj names no Cluster, the Cluster is gone, or it has a deletionTimestamp. In the nil cases a deletion is part of
// a Cluster deletion (CAPI deletes the control plane and then the InfraCluster while the Cluster is held by its
// finalizer: reconcileDelete in core/reconcilers/cluster/cluster_controller.go, lines 407-465 in CAPI v1.14.0) or
// concerns an object no Cluster uses, so both delete guards allow it.
//
// ctx is the admission request's context. reader must be the manager's API reader (see the package documentation).
// obj is the managed object being deleted; the Cluster is looked up in its namespace, by clusterNameOf. kind is obj's
// kind, used only in the error message. A read error other than NotFound is returned as an InternalError that names
// the Cluster, so the deletion is rejected and can be retried (fail closed).
func liveOwnerCluster(ctx context.Context, reader client.Reader, obj client.Object, kind string) (*clusterv1.Cluster, error) {
	name := clusterNameOf(obj)
	if name == "" {
		return nil, nil
	}
	cluster := &clusterv1.Cluster{}
	key := client.ObjectKey{Namespace: obj.GetNamespace(), Name: name}
	if err := reader.Get(ctx, key, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, readError("Cluster", key, kind, obj.GetName(), err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	return cluster, nil
}

// readError is the rejection of a deletion whose guard could not read an object it needs. The deletion is refused
// (fail closed), because allowing it unchecked could delete an IBM cluster that the CAPI objects still describe.
//
// readKind and key name the object that could not be read; kind and name name the object being deleted; err is the
// reader's error.
func readError(readKind string, key client.ObjectKey, kind, name string, err error) error {
	return apierrors.NewInternalError(fmt.Errorf(
		"cannot read %s %s to decide whether %s %s may be deleted: %w; retry the deletion", readKind, key, kind, name,
		err))
}
