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
	"testing"

	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// infraClusterDeleteDeniedMessage is the rejection of the IBMCloudManagedCluster delete guard for an
// IBMCloudManagedCluster whose Cluster is clusterName.
func infraClusterDeleteDeniedMessage(clusterName string) string {
	return "delete Cluster " + clusterName + " instead; IBMCloudManagedControlPlane/" + controlPlaneNameFor(clusterName) +
		" still needs this IBMCloudManagedCluster to manage and delete its IBM cluster"
}

// infraClusterDeletingClusterMessage is the rejection of the IBMCloudManagedCluster delete guard while the Cluster
// clusterName is being deleted and its control plane may still own an IBM cluster. clusterID is the control plane's
// status.clusterID, "" when it has recorded none.
func infraClusterDeletingClusterMessage(clusterName, clusterID string) string {
	target := "its IBM cluster"
	if clusterID != "" {
		target = "IBM cluster " + clusterID
	}
	return "leave this deletion to Cluster API: Cluster " + clusterName + " is being deleted, and " +
		"IBMCloudManagedControlPlane/" + controlPlaneNameFor(clusterName) + " still needs this IBMCloudManagedCluster " +
		"to delete " + target + "; Cluster API deletes this object once that control plane is gone, and a retried " +
		"deletion succeeds from then on"
}

// controlPlaneWith returns the unit-test control plane of Cluster "dev" after mutate.
func controlPlaneWith(mutate func(cp *controlplanev1.IBMCloudManagedControlPlane)) *controlplanev1.IBMCloudManagedControlPlane {
	cp := newControlPlane(unitNamespace, "dev")
	mutate(cp)
	return cp
}

// clusterWithControlPlaneRef returns the unit-test Cluster "dev" with its controlPlaneRef replaced by ref.
func clusterWithControlPlaneRef(ref clusterv1.ContractVersionedObjectReference) *clusterv1.Cluster {
	cluster := newCluster(unitNamespace, "dev")
	cluster.Spec.ControlPlaneRef = ref
	return cluster
}

// clusterWithInfrastructureRef returns the unit-test Cluster "dev" with its infrastructureRef replaced by ref.
func clusterWithInfrastructureRef(ref clusterv1.ContractVersionedObjectReference) *clusterv1.Cluster {
	cluster := newCluster(unitNamespace, "dev")
	cluster.Spec.InfrastructureRef = ref
	return cluster
}

func TestIBMCloudManagedClusterValidateCreateAndUpdate(t *testing.T) {
	g := NewWithT(t)
	reader := newStrictReader(t, nil, nil)
	validator := &IBMCloudManagedCluster{ManagedKubernetesEnabled: true, Reader: reader}
	infra := newInfraCluster(unitNamespace, "dev")

	warnings, err := validator.ValidateCreate(context.Background(), infra)
	expectAllowed(g, warnings, err)
	warnings, err = validator.ValidateUpdate(context.Background(), infra, infra.DeepCopy())
	expectAllowed(g, warnings, err)
	g.Expect(reader.calls).To(BeEmpty())
}

// TestIBMCloudManagedClusterValidateDelete covers every case of the delete guard on the typed validator with a strict
// reader, which also proves which reads each case makes and that nothing else is read. Every case runs with the
// feature gate on and off, because the guard does not depend on it.
func TestIBMCloudManagedClusterValidateDelete(t *testing.T) {
	readFailure := errors.New("connection refused")
	dev := clusterGet("dev")
	devCP := controlPlaneGet(controlPlaneNameFor("dev"))
	testCases := []struct {
		name    string
		mutate  func(infra *infrav1.IBMCloudManagedCluster)
		objs    []client.Object
		allowed []string
		errs    map[string]error
		wantErr string
	}{
		{
			name:    "cluster live and control plane has clusterID rejected",
			objs:    []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(withClusterID)},
			allowed: []string{dev, devCP},
			wantErr: infraClusterDeleteDeniedMessage("dev"),
		},
		{
			name:    "cluster live and control plane has only controlPlaneEndpoint rejected",
			objs:    []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(withEndpoint)},
			allowed: []string{dev, devCP},
			wantErr: infraClusterDeleteDeniedMessage("dev"),
		},
		{
			name:    "cluster live and control plane has only createIntent rejected",
			objs:    []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(withCreateIntent)},
			allowed: []string{dev, devCP},
			wantErr: infraClusterDeleteDeniedMessage("dev"),
		},
		{
			name: "cluster found by owner reference rejected",
			mutate: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Labels = nil
				infra.OwnerReferences = []metav1.OwnerReference{
					{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: "dev", UID: "uid"},
				}
			},
			objs:    []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(withClusterID)},
			allowed: []string{dev, devCP},
			wantErr: infraClusterDeleteDeniedMessage("dev"),
		},
		{
			// kubectl delete -f or a GitOps prune deletes the Cluster and this object at once. The control plane still
			// needs this object to delete its IBM cluster, and Cluster API deletes it itself once the control plane is
			// gone, so the deletion waits for that.
			name:    "cluster deleting and control plane has clusterID rejected",
			objs:    []client.Object{deleting(newCluster(unitNamespace, "dev")), controlPlaneWith(withClusterID)},
			allowed: []string{dev, devCP},
			wantErr: infraClusterDeletingClusterMessage("dev", testIBMClusterID),
		},
		{
			name:    "cluster deleting and control plane has only createIntent rejected",
			objs:    []client.Object{deleting(newCluster(unitNamespace, "dev")), controlPlaneWith(withCreateIntent)},
			allowed: []string{dev, devCP},
			wantErr: infraClusterDeletingClusterMessage("dev", ""),
		},
		{
			// Cluster API's own order: the control plane is gone before Cluster API deletes this object.
			name:    "cluster deleting and control plane gone allowed",
			objs:    []client.Object{deleting(newCluster(unitNamespace, "dev"))},
			allowed: []string{dev, devCP},
		},
		{
			name: "cluster deleting and control plane without IBM cluster allowed",
			objs: []client.Object{
				deleting(newCluster(unitNamespace, "dev")),
				controlPlaneWith(func(*controlplanev1.IBMCloudManagedControlPlane) {}),
			},
			allowed: []string{dev, devCP},
		},
		{
			name: "cluster deleting and infrastructureRef names another IBMCloudManagedCluster allowed",
			objs: []client.Object{
				deleting(clusterWithInfrastructureRef(clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group, Kind: "IBMCloudManagedCluster", Name: "dev-v2",
				})),
				controlPlaneWith(withClusterID),
			},
			allowed: []string{dev},
		},
		{
			// The control plane's controller cannot reconcile without its owner Cluster, so it could not use this
			// object anyway.
			name:    "cluster gone allowed",
			objs:    []client.Object{controlPlaneWith(withClusterID)},
			allowed: []string{dev},
		},
		{
			name:   "no cluster named allowed without reads",
			mutate: func(infra *infrav1.IBMCloudManagedCluster) { infra.Labels = nil },
			objs:   []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(withClusterID)},
		},
		{
			name: "delete-for-move allowed without reads",
			mutate: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
			},
			objs: []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(withClusterID)},
		},
		{
			// The Cluster was pointed at a new InfraCluster; the old one keeps its label (and owner reference), but
			// the control plane no longer reads it, so it is not guarded. The control plane is not even read.
			name: "cluster infrastructureRef names another IBMCloudManagedCluster allowed",
			objs: []client.Object{
				clusterWithInfrastructureRef(clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group, Kind: "IBMCloudManagedCluster", Name: "dev-v2",
				}),
				controlPlaneWith(withClusterID),
			},
			allowed: []string{dev},
		},
		{
			name: "cluster infrastructureRef names another kind allowed",
			objs: []client.Object{
				clusterWithInfrastructureRef(clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group, Kind: "IBMVPCCluster", Name: "dev",
				}),
				controlPlaneWith(withClusterID),
			},
			allowed: []string{dev},
		},
		{
			name: "cluster without infrastructureRef allowed",
			objs: []client.Object{
				clusterWithInfrastructureRef(clusterv1.ContractVersionedObjectReference{}),
				controlPlaneWith(withClusterID),
			},
			allowed: []string{dev},
		},
		{
			name: "cluster without IBMCloudManagedControlPlane ref allowed",
			objs: []client.Object{clusterWithControlPlaneRef(clusterv1.ContractVersionedObjectReference{
				APIGroup: "controlplane.cluster.x-k8s.io", Kind: "KubeadmControlPlane", Name: "dev-control-plane",
			})},
			allowed: []string{dev},
		},
		{
			name: "cluster ref with same kind in another group allowed",
			objs: []client.Object{clusterWithControlPlaneRef(clusterv1.ContractVersionedObjectReference{
				APIGroup: "example.com", Kind: "IBMCloudManagedControlPlane", Name: "dev-control-plane",
			})},
			allowed: []string{dev},
		},
		{
			name:    "cluster without controlPlaneRef allowed",
			objs:    []client.Object{clusterWithControlPlaneRef(clusterv1.ContractVersionedObjectReference{})},
			allowed: []string{dev},
		},
		{
			name:    "control plane absent allowed",
			objs:    []client.Object{newCluster(unitNamespace, "dev")},
			allowed: []string{dev, devCP},
		},
		{
			name: "control plane without IBM cluster allowed",
			objs: []client.Object{newCluster(unitNamespace, "dev"), controlPlaneWith(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				// Reconciled, but no create was sent: status fields other than the three marks do not count.
				cp.Status.ExternalManagedControlPlane = ptr.To(true)
			})},
			allowed: []string{dev, devCP},
		},
		{
			name:    "cluster read error rejected (fail closed)",
			allowed: []string{dev},
			errs:    map[string]error{dev: readFailure},
			wantErr: "cannot read Cluster unit/dev to decide whether IBMCloudManagedCluster dev may be deleted",
		},
		{
			name:    "control plane read error rejected (fail closed)",
			objs:    []client.Object{newCluster(unitNamespace, "dev")},
			allowed: []string{dev, devCP},
			errs:    map[string]error{devCP: readFailure},
			wantErr: "cannot read IBMCloudManagedControlPlane unit/dev-control-plane to decide whether IBMCloudManagedCluster " +
				"dev may be deleted: connection refused; retry the deletion",
		},
	}
	for _, tc := range testCases {
		// The guard must answer the same whatever the feature gate value.
		for _, gate := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s (gate on: %t)", tc.name, gate), func(t *testing.T) {
				g := NewWithT(t)
				infra := newInfraCluster(unitNamespace, "dev")
				if tc.mutate != nil {
					tc.mutate(infra)
				}
				reader := newStrictReader(t, tc.allowed, tc.errs, tc.objs...)

				validator := &IBMCloudManagedCluster{ManagedKubernetesEnabled: gate, Reader: reader}
				warnings, err := validator.ValidateDelete(context.Background(), infra)

				g.Expect(warnings).To(BeEmpty())
				g.Expect(reader.calls).To(Equal(tc.allowed))
				switch {
				case tc.wantErr == "":
					g.Expect(err).NotTo(HaveOccurred())
				case tc.errs != nil:
					g.Expect(apierrors.IsInternalError(err)).To(BeTrue(), "want InternalError, got %v", err)
					g.Expect(err.Error()).To(ContainSubstring(tc.wantErr))
				default:
					expectForbidden(g, err, tc.wantErr)
				}
			})
		}
	}
}

// TestIBMCloudManagedClusterWebhookDelete covers every case of the delete guard through the API server (the gate-off
// rows are unit tests, TestGateOff and TestIBMCloudManagedClusterValidateDelete, because one API server registers only
// one validator per webhook path). Each case creates its own Cluster, control plane and IBMCloudManagedCluster in one
// namespace.
func TestIBMCloudManagedClusterWebhookDelete(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmc-webhook-delete")
	clusterIDStatus := &controlplanev1.IBMCloudManagedControlPlaneStatus{ClusterID: testIBMClusterID}

	testCases := []struct {
		name string
		// prepare creates the objects of the row, apart from the IBMCloudManagedCluster, which the loop creates.
		prepare    func(t *testing.T, g *WithT, clusterName string)
		annotation bool
		// wantErr returns the rejection the row expects for the Cluster clusterName; nil means the deletion is allowed.
		wantErr func(clusterName string) string
	}{
		{
			name: "rejected while the cluster lives and the control plane has a clusterID",
			prepare: func(t *testing.T, g *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				createControlPlane(g, newControlPlane(namespace, clusterName), clusterIDStatus)
			},
			wantErr: infraClusterDeleteDeniedMessage,
		},
		{
			name: "rejected while the cluster lives and the control plane has only a controlPlaneEndpoint",
			prepare: func(t *testing.T, g *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				cp := newControlPlane(namespace, clusterName)
				withEndpoint(cp)
				createControlPlane(g, cp, nil)
			},
			wantErr: infraClusterDeleteDeniedMessage,
		},
		{
			name: "rejected while the cluster lives and the control plane has only a createIntent",
			prepare: func(t *testing.T, g *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				cp := newControlPlane(namespace, clusterName)
				withCreateIntent(cp)
				// Create drops status, so the intent is copied out first and written through the status subresource.
				status := &controlplanev1.IBMCloudManagedControlPlaneStatus{CreateIntent: cp.Status.CreateIntent}
				createControlPlane(g, cp, status)
			},
			wantErr: infraClusterDeleteDeniedMessage,
		},
		{
			// kubectl delete -f or a GitOps prune: the Cluster's deletion has started (held by a test finalizer), but
			// its control plane still exists and needs this object to delete the IBM cluster.
			name: "rejected while the cluster is deleting and the control plane has a clusterID",
			prepare: func(t *testing.T, g *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, true)
				createControlPlane(g, newControlPlane(namespace, clusterName), clusterIDStatus)
			},
			wantErr: func(clusterName string) string {
				return infraClusterDeletingClusterMessage(clusterName, testIBMClusterID)
			},
		},
		{
			// Cluster API's own order: it deletes this object only after the control plane is gone.
			name: "allowed while the cluster is deleting and the control plane is gone",
			prepare: func(t *testing.T, _ *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, true)
			},
		},
		{
			name: "allowed when the cluster is gone",
			prepare: func(_ *testing.T, g *WithT, clusterName string) {
				createControlPlane(g, newControlPlane(namespace, clusterName), clusterIDStatus)
			},
		},
		{
			name: "allowed with delete-for-move",
			prepare: func(t *testing.T, g *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				createControlPlane(g, newControlPlane(namespace, clusterName), clusterIDStatus)
			},
			annotation: true,
		},
		{
			name: "allowed when the cluster has no IBMCloudManagedControlPlane ref",
			prepare: func(_ *testing.T, g *WithT, clusterName string) {
				cluster := newCluster(namespace, clusterName)
				cluster.Spec.ControlPlaneRef = clusterv1.ContractVersionedObjectReference{
					APIGroup: "controlplane.cluster.x-k8s.io", Kind: "KubeadmControlPlane", Name: controlPlaneNameFor(clusterName),
				}
				g.Expect(testEnv.Create(ctx, cluster)).To(Succeed())
				// A control plane of this name with an IBM cluster exists, but the Cluster does not use it.
				createControlPlane(g, newControlPlane(namespace, clusterName), clusterIDStatus)
			},
		},
		{
			name: "allowed when the cluster's infrastructureRef names another IBMCloudManagedCluster",
			prepare: func(_ *testing.T, g *WithT, clusterName string) {
				// The live Cluster was pointed at a new InfraCluster, and its control plane has an IBM cluster; the
				// old IBMCloudManagedCluster keeps the cluster-name label but is no longer read by anyone.
				cluster := newCluster(namespace, clusterName)
				cluster.Spec.InfrastructureRef.Name = clusterName + "-v2"
				g.Expect(testEnv.Create(ctx, cluster)).To(Succeed())
				createControlPlane(g, newControlPlane(namespace, clusterName), clusterIDStatus)
			},
		},
		{
			name: "allowed when the control plane is absent",
			prepare: func(t *testing.T, _ *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
			},
		},
		{
			name: "allowed when the control plane has no IBM cluster",
			prepare: func(t *testing.T, g *WithT, clusterName string) {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				createControlPlane(g, newControlPlane(namespace, clusterName), nil)
			},
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			clusterName := fmt.Sprintf("d51-%d", i)
			tc.prepare(t, g, clusterName)
			infra := newInfraCluster(namespace, clusterName)
			if tc.annotation {
				infra.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
			}
			g.Expect(testEnv.Create(ctx, infra)).To(Succeed())

			err := testEnv.Delete(ctx, infra)

			if tc.wantErr == nil {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(apierrors.IsForbidden(err)).To(BeTrue(), "want Forbidden, got %v", err)
			g.Expect(err.Error()).To(ContainSubstring(tc.wantErr(clusterName)))
		})
	}
}

// TestIBMCloudManagedClusterWebhookDeleteWithCluster deletes a whole cluster through the API server the way
// `kubectl delete -f` of a template does (the Cluster first, then the IBMCloudManagedCluster, then the control plane)
// and then lets the deletion finish the way Cluster API does. Envtest runs no controller, so test finalizers stand in
// for Cluster API's and the control plane's: the IBMCloudManagedCluster must stay while the control plane still
// exists, even while that control plane is itself being deleted, and must go once the control plane is gone, so that
// the guard never blocks Cluster API's own ordered deletion.
func TestIBMCloudManagedClusterWebhookDeleteWithCluster(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "ibmcmc-webhook-delete-all")
	const clusterName = "dev"
	// The test finalizers stand in for Cluster API's on the Cluster and for the control plane's own, which holds the
	// control plane while it deletes the IBM cluster.
	cluster := newCluster(namespace, clusterName)
	cluster.Finalizers = []string{holdFinalizer}
	g.Expect(testEnv.Create(ctx, cluster)).To(Succeed())
	t.Cleanup(func() { g.Expect(client.IgnoreNotFound(removeFinalizers(cluster))).To(Succeed()) })
	cp := newControlPlane(namespace, clusterName)
	cp.Finalizers = []string{holdFinalizer}
	createControlPlane(g, cp, &controlplanev1.IBMCloudManagedControlPlaneStatus{ClusterID: testIBMClusterID})
	t.Cleanup(func() { g.Expect(client.IgnoreNotFound(removeFinalizers(cp))).To(Succeed()) })
	infra := newInfraCluster(namespace, clusterName)
	g.Expect(testEnv.Create(ctx, infra)).To(Succeed())
	wantRejection := infraClusterDeletingClusterMessage(clusterName, testIBMClusterID)

	// The Cluster is the first document of the template, so its deletion starts first.
	g.Expect(testEnv.Delete(ctx, cluster)).To(Succeed())

	// The IBMCloudManagedCluster comes next: rejected, because the control plane still needs it.
	err := testEnv.Delete(ctx, infra)
	g.Expect(apierrors.IsForbidden(err)).To(BeTrue(), "want Forbidden, got %v", err)
	g.Expect(err.Error()).To(ContainSubstring(wantRejection))

	// The control plane's deletion is admitted, because its Cluster is being deleted, and its finalizer holds it while
	// it deletes the IBM cluster; the IBMCloudManagedCluster stays guarded meanwhile.
	g.Expect(testEnv.Delete(ctx, cp)).To(Succeed())
	err = testEnv.Delete(ctx, infra)
	g.Expect(apierrors.IsForbidden(err)).To(BeTrue(), "want Forbidden, got %v", err)
	g.Expect(err.Error()).To(ContainSubstring(wantRejection))

	// The IBM cluster is gone and the control plane removes its finalizer, which removes the object at once; Cluster API
	// then deletes the IBMCloudManagedCluster, which the guard admits. The check reads through the API reader, like the
	// webhook does: the API server removes the object within the finalizer patch, while the manager's cache (testEnv.Get)
	// may still hold it for a moment.
	g.Expect(removeFinalizers(cp)).To(Succeed())
	err = testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(cp), &controlplanev1.IBMCloudManagedControlPlane{})
	g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
		"the control plane must be gone once its last finalizer is removed, got %v", err)
	g.Expect(testEnv.Delete(ctx, infra)).To(Succeed())
}

// removeFinalizers removes every finalizer of obj with a merge patch, after re-reading obj from the API server so that
// the patch applies to the stored object. The read goes through the API reader, not the manager's cache, because the
// cache may not have seen an object created or deleted a moment ago: a stale read could miss an object that exists or
// return one that is gone. It returns the error of the read or of the patch, NotFound when obj is already gone.
//
// obj is the envtest object whose finalizers are removed; it is updated to the stored state.
func removeFinalizers(obj client.Object) error {
	if err := testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		return err
	}
	patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
	obj.SetFinalizers(nil)
	return testEnv.Patch(ctx, obj, patch)
}

// TestIBMCloudManagedClusterWebhookUpdate re-tests the field rules of IBMCloudManagedCluster through the API server with the
// webhook registered (the CEL tests of api/managed/v1alpha1 test CEL without webhooks). ValidateUpdate allows every
// update, so each rejection must come from CEL, and the allowed row proves the webhook does not block what CEL accepts.
func TestIBMCloudManagedClusterWebhookUpdate(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmc-webhook-update")

	testCases := []struct {
		name    string
		mutate  func(infra *infrav1.IBMCloudManagedCluster)
		wantErr string
	}{
		{
			name: "zone_added_allowed",
			mutate: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Spec.Zones = append(infra.Spec.Zones,
					infrav1.ZoneSubnet{Name: "us-south-3", Subnet: infrav1.ResourceIdentifier{Name: "test-subnet-3"}})
			},
		},
		{
			name:    "CEL rejects a removed zone",
			mutate:  func(infra *infrav1.IBMCloudManagedCluster) { infra.Spec.Zones = infra.Spec.Zones[:1] },
			wantErr: "zones can be added; existing zones cannot be removed or changed in v1alpha1",
		},
		{
			name: "CEL rejects a changed zone subnet",
			mutate: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Spec.Zones[1].Subnet = infrav1.ResourceIdentifier{Name: "other-subnet"}
			},
			wantErr: "zones can be added; existing zones cannot be removed or changed in v1alpha1",
		},
		{
			name: "CEL rejects a resource group change",
			mutate: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Spec.ResourceGroup.ID = "fedcba9876543210fedcba9876543210"
			},
			wantErr: "resourceGroup is immutable",
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			infra := newInfraCluster(namespace, fmt.Sprintf("update-%d", i))
			infra.Spec.Zones = append(infra.Spec.Zones,
				infrav1.ZoneSubnet{Name: "us-south-2", Subnet: infrav1.ResourceIdentifier{Name: "test-subnet-2"}})
			g.Expect(testEnv.Create(ctx, infra)).To(Succeed())

			tc.mutate(infra)
			err := testEnv.Update(ctx, infra)

			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			expectRejectedByCEL(g, err, tc.wantErr)
		})
	}
}
