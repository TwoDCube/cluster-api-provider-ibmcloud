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
	"strings"
	"testing"

	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmclustername"
)

// Messages of the control plane validator, kept in one place so a reworded message fails loudly in every case.
const (
	tooHighMessage   = "the master moves up one minor at a time"
	downgradeMessage = "a master cannot be downgraded"

	// unknownClusterTarget stands in for the Cluster name in the warning text when the Cluster cannot be told.
	unknownClusterTarget = "after its Cluster (the Cluster's metadata.name)"
)

// clusterNameWarningText is the middle part of the ibmCloudClusterName warning with <Cluster name> filled in; the
// warning must carry it verbatim.
func clusterNameWarningText(clusterName string) string {
	return "the IBM cluster will be named " + clusterName + "; IBM names are unique per account, so other namespaces " +
		"or management clusters using the same name will conflict"
}

// clusterNameWarning is the complete ibmCloudClusterName admission warning: clusterNameWarningText between the prefix
// that names the omitted field and the suffix that says how to choose a name.
func clusterNameWarning(clusterName string) string {
	return "spec.ibmCloudClusterName is omitted: " + clusterNameWarningText(clusterName) +
		"; set spec.ibmCloudClusterName (immutable) to choose a name"
}

// controlPlaneDeleteDeniedMessage is the rejection of the control plane's delete guard for a control plane whose
// Cluster is clusterName.
func controlPlaneDeleteDeniedMessage(clusterName string) string {
	return "delete Cluster " + clusterName + " instead; deleting the control plane alone would delete the IBM cluster"
}

func TestValidateMinorStep(t *testing.T) {
	testCases := []struct {
		name     string
		observed string
		desired  string
		wantErr  string
	}{
		{name: "no observed master: any value", observed: "", desired: "1.40"},
		{name: "same minor", observed: "1.36.5_1537", desired: "1.36"},
		{name: "one minor up", observed: "1.36.5_1537", desired: "1.37"},
		{
			name: "two minors up", observed: "1.36.5_1537", desired: "1.38",
			wantErr: `spec.distributionVersion: Invalid value: "1.38": the master moves up one minor at a time: it runs ` +
				`1.36 (status.distributionVersion 1.36.5_1537), so the highest allowed value is 1.37; raise it again ` +
				`once status.distributionVersion reports 1.37`,
		},
		{
			name: "one minor down", observed: "1.36.5_1537", desired: "1.35",
			wantErr: `spec.distributionVersion: Invalid value: "1.35": a master cannot be downgraded: it runs 1.36 ` +
				`(status.distributionVersion 1.36.5_1537), so the lowest allowed value is 1.36`,
		},
		{name: "new major is more than one minor up", observed: "1.36.5_1537", desired: "2.0", wantErr: tooHighMessage},
		{name: "older major is a downgrade", observed: "4.21.35_1537_openshift", desired: "3.99", wantErr: downgradeMessage},
		{name: "OpenShift one minor up", observed: "4.21.35_1537_openshift", desired: "4.22"},
		{name: "OpenShift two minors up", observed: "4.21.35_1537_openshift", desired: "4.23", wantErr: tooHighMessage},
		{name: "OpenShift one minor down", observed: "4.21.35_1537_openshift", desired: "4.20", wantErr: downgradeMessage},
		{name: "observed version with the pending marker", observed: "1.36.5_1537 (pending)", desired: "1.37"},
		{name: "observed minor-only version", observed: "1.36", desired: "1.37"},
		{
			name: "unparsable observed version is rejected, not skipped", observed: "v1.36.5", desired: "1.37",
			wantErr: `cannot check the minor step against status.distributionVersion "v1.36.5"`,
		},
		{
			name: "desired value with a patch", observed: "1.36.5_1537", desired: "1.36.6",
			wantErr: `invalid minor version "1.36.6"`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := validateMinorStep(tc.observed, tc.desired)
			if tc.wantErr == "" {
				g.Expect(err).To(BeNil())
				return
			}
			g.Expect(err).NotTo(BeNil())
			g.Expect(err.Field).To(Equal("spec.distributionVersion"))
			g.Expect(err.Error()).To(ContainSubstring(tc.wantErr))
		})
	}
}

// TestIBMCloudManagedControlPlaneValidateCreate covers the IBM name check and the ibmCloudClusterName warning on the
// typed validator with a strict reader, which also proves when the Clusters are listed: only when the control plane
// omits ibmCloudClusterName and names no Cluster itself, as one from a clusterctl template does.
func TestIBMCloudManagedControlPlaneValidateCreate(t *testing.T) {
	// omitName is the template shape: no ibmCloudClusterName, no cluster-name label and no owner reference.
	omitName := func(cp *controlplanev1.IBMCloudManagedControlPlane) {
		cp.Spec.IBMCloudClusterName = ""
		cp.Labels = nil
	}
	// clusterReferencing returns a Cluster called name in namespace whose controlPlaneRef is ref.
	clusterReferencing := func(namespace, name string, ref clusterv1.ContractVersionedObjectReference) *clusterv1.Cluster {
		cluster := newCluster(namespace, name)
		cluster.Spec.ControlPlaneRef = ref
		return cluster
	}
	devCPRef := newCluster(unitNamespace, "dev").Spec.ControlPlaneRef
	// labelledWith is the shape of a control plane that names its Cluster by the cluster-name label and omits
	// ibmCloudClusterName, so that the Cluster's name becomes the IBM name.
	labelledWith := func(clusterName string) func(cp *controlplanev1.IBMCloudManagedControlPlane) {
		return func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Spec.IBMCloudClusterName = ""
			cp.Labels[clusterv1.ClusterNameLabel] = clusterName
		}
	}
	// named sets an explicit ibmCloudClusterName.
	named := func(name string) func(cp *controlplanev1.IBMCloudManagedControlPlane) {
		return func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.IBMCloudClusterName = name }
	}
	// name33 is one character longer than IBM allows.
	name33 := "team-a-dev-" + strings.Repeat("x", 22)
	testCases := []struct {
		name   string
		mutate func(cp *controlplanev1.IBMCloudManagedControlPlane)
		objs   []client.Object
		// allowed are the reads the case allows (see strictReader); nilReader runs the validator without a Reader.
		allowed     []string
		errs        map[string]error
		nilReader   bool
		wantWarning string
		// wantErr are parts of the expected Invalid error; the case then expects no warning.
		wantErr []string
	}{
		{
			name:   "explicit ibmCloudClusterName of 32 characters with underscore and dot: no warning and no read",
			mutate: named("team_a.dev-" + strings.Repeat("x", 21)),
		},
		{
			name:   "explicit ibmCloudClusterName of 33 characters rejected without a read",
			mutate: named(name33),
			wantErr: []string{fmt.Sprintf("spec.ibmCloudClusterName: Invalid value: %q", name33),
				"IBM would refuse to create a cluster with this name: it has 33 characters, more than 32", ibmclustername.Rule},
		},
		{
			name:    "explicit ibmCloudClusterName starting with a digit rejected",
			mutate:  named("1dev"),
			wantErr: []string{"it starts with '1' instead of a letter", ibmclustername.Rule},
		},
		{
			name:    "explicit ibmCloudClusterName with a space rejected",
			mutate:  named("dev cluster"),
			wantErr: []string{"it contains ' ', which is not a letter, digit, '-', '_' or '.'"},
		},
		{
			name:    "explicit ibmCloudClusterName with a unicode letter rejected",
			mutate:  named("clüster"),
			wantErr: []string{"it contains 'ü'"},
		},
		{
			// The name of the live refusal (IBM HTTP 400, code E0125), found from the label without a read.
			name:   "defaulted Cluster name of 33 characters from the label rejected without a read",
			mutate: labelledWith("example-test-cluster-e01-mgmt-iks"),
			wantErr: []string{"spec.ibmCloudClusterName: Required value",
				`without it the IBM cluster would be named after Cluster "example-test-cluster-e01-mgmt-iks", and IBM ` +
					"would refuse that name: it has 33 characters, more than 32", ibmclustername.Rule,
				"set spec.ibmCloudClusterName to a name that follows this rule (it cannot be added after the control plane is created)"},
		},
		{
			name:        "defaulted Cluster name of 32 characters from the label: warning only",
			mutate:      labelledWith("example-test-cluster-e01-mgmt-ik"),
			wantWarning: clusterNameWarning("example-test-cluster-e01-mgmt-ik"),
		},
		{
			// A Cluster name is a DNS-1123 name, which may start with a digit; IBM's may not.
			name:    "defaulted Cluster name starting with a digit from the label rejected",
			mutate:  labelledWith("1dev"),
			wantErr: []string{`would be named after Cluster "1dev"`, "it starts with '1' instead of a letter"},
		},
		{
			name: "defaulted Cluster name from the owner reference rejected without a read",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				omitName(cp)
				cp.OwnerReferences = []metav1.OwnerReference{
					{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: "9-lives", UID: "uid"},
				}
			},
			wantErr: []string{`would be named after Cluster "9-lives"`},
		},
		{
			name:    "defaulted Cluster name found through the controlPlaneRef of a template rejected",
			mutate:  omitName,
			objs:    []client.Object{clusterReferencing(unitNamespace, "a"+strings.Repeat("b", 32), devCPRef)},
			allowed: []string{clusterList},
			wantErr: []string{`would be named after Cluster "a` + strings.Repeat("b", 32) + `"`, "it has 33 characters"},
		},
		{
			// The Cluster with the long name references another control plane, so the owner is not known: admission
			// allows the object with the generic warning and leaves the name check to the controller.
			name:   "unknown Cluster name is not checked: generic warning",
			mutate: omitName,
			objs: []client.Object{clusterReferencing(unitNamespace, "a"+strings.Repeat("b", 32),
				newCluster(unitNamespace, "other").Spec.ControlPlaneRef)},
			allowed:     []string{clusterList},
			wantWarning: clusterNameWarning(unknownClusterTarget),
		},
		{
			name:   "ibmCloudClusterName set: no warning and no read",
			mutate: func(*controlplanev1.IBMCloudManagedControlPlane) {},
			objs:   []client.Object{newCluster(unitNamespace, "dev")},
		},
		{
			name:        "warning names the Cluster from the label without a read",
			mutate:      func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.IBMCloudClusterName = "" },
			wantWarning: clusterNameWarning("dev"),
		},
		{
			name: "warning names the Cluster from the owner reference without a read",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				omitName(cp)
				cp.OwnerReferences = []metav1.OwnerReference{
					{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: "owner", UID: "uid"},
				}
			},
			wantWarning: clusterNameWarning("owner"),
		},
		{
			name:   "warning names the Cluster whose controlPlaneRef names the template control plane",
			mutate: omitName,
			objs: []client.Object{
				newCluster(unitNamespace, "other"),
				newCluster(unitNamespace, "dev"),
			},
			allowed:     []string{clusterList},
			wantWarning: clusterNameWarning("dev"),
		},
		{
			name:   "generic warning when no Cluster of the namespace references the control plane",
			mutate: omitName,
			objs: []client.Object{
				newCluster(unitNamespace, "other"),
				// The same reference from another namespace names another control plane.
				newCluster("elsewhere", "dev"),
			},
			allowed:     []string{clusterList},
			wantWarning: clusterNameWarning(unknownClusterTarget),
		},
		{
			name:   "generic warning when the matching reference names another kind",
			mutate: omitName,
			objs: []client.Object{clusterReferencing(unitNamespace, "dev", clusterv1.ContractVersionedObjectReference{
				APIGroup: "controlplane.cluster.x-k8s.io", Kind: "KubeadmControlPlane", Name: controlPlaneNameFor("dev"),
			})},
			allowed:     []string{clusterList},
			wantWarning: clusterNameWarning(unknownClusterTarget),
		},
		{
			name:   "generic warning when two Clusters reference the control plane",
			mutate: omitName,
			objs: []client.Object{
				newCluster(unitNamespace, "dev"),
				clusterReferencing(unitNamespace, "dev-copy", devCPRef),
			},
			allowed:     []string{clusterList},
			wantWarning: clusterNameWarning(unknownClusterTarget),
		},
		{
			name:        "generic warning and no error when the list fails",
			mutate:      omitName,
			objs:        []client.Object{newCluster(unitNamespace, "dev")},
			allowed:     []string{clusterList},
			errs:        map[string]error{clusterList: errors.New("connection refused")},
			wantWarning: clusterNameWarning(unknownClusterTarget),
		},
		{
			name:        "generic warning without a Reader",
			mutate:      omitName,
			nilReader:   true,
			wantWarning: clusterNameWarning(unknownClusterTarget),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := newControlPlane(unitNamespace, "dev")
			tc.mutate(cp)
			reader := newStrictReader(t, tc.allowed, tc.errs, tc.objs...)
			validator := &IBMCloudManagedControlPlane{ManagedKubernetesEnabled: true, Reader: reader}
			if tc.nilReader {
				validator.Reader = nil
			}

			warnings, err := validator.ValidateCreate(context.Background(), cp)

			g.Expect(reader.calls).To(Equal(tc.allowed))
			if len(tc.wantErr) > 0 {
				g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)
				for _, part := range tc.wantErr {
					g.Expect(err.Error()).To(ContainSubstring(part))
				}
				g.Expect(warnings).To(BeEmpty(), "a rejected create gets no warning")
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			if tc.wantWarning == "" {
				g.Expect(warnings).To(BeEmpty())
				return
			}
			g.Expect(warnings).To(HaveExactElements(tc.wantWarning))
		})
	}
}

func TestIBMCloudClusterNameWarning(t *testing.T) {
	testCases := []struct {
		name        string
		clusterName string
		want        string
	}{
		{
			name:        "the warning text names the Cluster verbatim",
			clusterName: "dev",
			want: "spec.ibmCloudClusterName is omitted: the IBM cluster will be named dev; IBM names are unique per " +
				"account, so other namespaces or management clusters using the same name will conflict; set " +
				"spec.ibmCloudClusterName (immutable) to choose a name",
		},
		{
			name: "unknown Cluster",
			want: "spec.ibmCloudClusterName is omitted: the IBM cluster will be named after its Cluster (the " +
				"Cluster's metadata.name); IBM names are unique per account, so other namespaces or management " +
				"clusters using the same name will conflict; set spec.ibmCloudClusterName (immutable) to choose a name",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := ibmCloudClusterNameWarning(tc.clusterName)
			g.Expect(got).To(Equal(tc.want))
			if tc.clusterName != "" {
				g.Expect(got).To(ContainSubstring(clusterNameWarningText(tc.clusterName)))
			}
		})
	}
}

func TestIBMCloudManagedControlPlaneValidateUpdate(t *testing.T) {
	testCases := []struct {
		name     string
		observed string
		oldSpec  string
		newSpec  string
		mutate   func(cp *controlplanev1.IBMCloudManagedControlPlane)
		wantErr  string
	}{
		{name: "one minor up", observed: "1.36.5_1537", oldSpec: "1.36", newSpec: "1.37"},
		{name: "two minors up", observed: "1.36.5_1537", oldSpec: "1.36", newSpec: "1.38", wantErr: tooHighMessage},
		{name: "downgrade", observed: "1.36.5_1537", oldSpec: "1.36", newSpec: "1.35", wantErr: downgradeMessage},
		{
			name: "back to the observed minor cancels a pending upgrade", observed: "1.36.5_1537", oldSpec: "1.37",
			newSpec: "1.36",
		},
		{
			// The stored spec is already two minors above status (status moved after admission, or was written by
			// hand); an update that does not touch distributionVersion must still pass.
			name: "unchanged distributionVersion is not checked", observed: "1.34.9_1530", oldSpec: "1.36",
			newSpec: "1.36", mutate: withEndpoint,
		},
		{name: "no observed master", observed: "", oldSpec: "1.36", newSpec: "1.38"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			oldCP := newControlPlane(unitNamespace, "dev")
			oldCP.Spec.DistributionVersion = tc.oldSpec
			oldCP.Status.DistributionVersion = tc.observed
			newCP := oldCP.DeepCopy()
			newCP.Spec.DistributionVersion = tc.newSpec
			if tc.mutate != nil {
				tc.mutate(newCP)
			}
			reader := newStrictReader(t, nil, nil)

			warnings, err := (&IBMCloudManagedControlPlane{ManagedKubernetesEnabled: true, Reader: reader}).ValidateUpdate(context.Background(), oldCP, newCP)

			g.Expect(warnings).To(BeEmpty())
			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)
			g.Expect(err.Error()).To(ContainSubstring(tc.wantErr))
		})
	}
}

// TestIBMCloudManagedControlPlaneValidateDelete covers every case of the delete guard on the typed validator with a
// strict reader, which also proves which reads each case makes (none for the exemptions that need no read). Every case
// runs with the feature gate on and off, because the guard does not depend on it.
func TestIBMCloudManagedControlPlaneValidateDelete(t *testing.T) {
	readFailure := errors.New("connection refused")
	testCases := []struct {
		name    string
		mutate  func(cp *controlplanev1.IBMCloudManagedControlPlane)
		objs    []client.Object
		allowed []string
		errs    map[string]error
		wantErr string
	}{
		{
			name:    "cluster live, rejected",
			mutate:  withClusterID,
			objs:    []client.Object{newCluster(unitNamespace, "dev")},
			allowed: []string{clusterGet("dev")},
			wantErr: controlPlaneDeleteDeniedMessage("dev"),
		},
		{
			name: "cluster live, found by the owner reference, rejected",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				withClusterID(cp)
				cp.Labels = nil
				cp.OwnerReferences = []metav1.OwnerReference{
					{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: "dev", UID: "uid"},
				}
			},
			objs:    []client.Object{newCluster(unitNamespace, "dev")},
			allowed: []string{clusterGet("dev")},
			wantErr: controlPlaneDeleteDeniedMessage("dev"),
		},
		{
			name:    "cluster live and only the endpoint set, rejected",
			mutate:  withEndpoint,
			objs:    []client.Object{newCluster(unitNamespace, "dev")},
			allowed: []string{clusterGet("dev")},
			wantErr: controlPlaneDeleteDeniedMessage("dev"),
		},
		{
			name:    "cluster live and only the create intent set, rejected",
			mutate:  withCreateIntent,
			objs:    []client.Object{newCluster(unitNamespace, "dev")},
			allowed: []string{clusterGet("dev")},
			wantErr: controlPlaneDeleteDeniedMessage("dev"),
		},
		{
			name:    "cluster deleting, allowed",
			mutate:  withClusterID,
			objs:    []client.Object{deleting(newCluster(unitNamespace, "dev"))},
			allowed: []string{clusterGet("dev")},
		},
		{
			name:    "cluster gone, allowed",
			mutate:  withClusterID,
			allowed: []string{clusterGet("dev")},
		},
		{
			name: "no Cluster named: allowed without reads",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				withClusterID(cp)
				cp.Labels = nil
			},
		},
		{
			name: "delete-for-move: allowed without reads",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				withClusterID(cp)
				cp.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
			},
			objs: []client.Object{newCluster(unitNamespace, "dev")},
		},
		{
			name:   "no IBM cluster yet: allowed without reads",
			mutate: func(*controlplanev1.IBMCloudManagedControlPlane) {},
			objs:   []client.Object{newCluster(unitNamespace, "dev")},
		},
		{
			name:    "cluster read error rejected (fail closed)",
			mutate:  withClusterID,
			allowed: []string{clusterGet("dev")},
			errs:    map[string]error{clusterGet("dev"): readFailure},
			wantErr: "cannot read Cluster unit/dev to decide whether IBMCloudManagedControlPlane dev-control-plane may be deleted",
		},
	}
	for _, tc := range testCases {
		// The guard must answer the same whatever the feature gate value.
		for _, gate := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s (gate on: %t)", tc.name, gate), func(t *testing.T) {
				g := NewWithT(t)
				cp := newControlPlane(unitNamespace, "dev")
				tc.mutate(cp)
				reader := newStrictReader(t, tc.allowed, tc.errs, tc.objs...)

				validator := &IBMCloudManagedControlPlane{ManagedKubernetesEnabled: gate, Reader: reader}
				warnings, err := validator.ValidateDelete(context.Background(), cp)

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

// warningRecorder collects the admission warnings the API server returns, so the envtest tests can assert the
// ibmCloudClusterName warning. The client calls it synchronously during the request.
type warningRecorder struct {
	warnings []string
}

func (w *warningRecorder) HandleWarningHeaderWithContext(_ context.Context, _ int, _ string, text string) {
	w.warnings = append(w.warnings, text)
}

// newWarningClient returns a client of the envtest API server that records warnings in the returned recorder.
func newWarningClient(g *WithT) (client.Client, *warningRecorder) {
	recorder := &warningRecorder{}
	config := rest.CopyConfig(testEnv.Config)
	config.WarningHandlerWithContext = recorder
	c, err := client.New(config, client.Options{Scheme: scheme.Scheme})
	g.Expect(err).NotTo(HaveOccurred())
	return c, recorder
}

// createCluster creates the CAPI Cluster name in namespace (see newCluster). With beingDeleted it is held by a test
// finalizer and deleted, so it keeps a deletionTimestamp; the finalizer is removed when the test ends.
func createCluster(t *testing.T, namespace, name string, beingDeleted bool) *clusterv1.Cluster {
	t.Helper()
	g := NewWithT(t)
	cluster := newCluster(namespace, name)
	if beingDeleted {
		cluster.Finalizers = []string{holdFinalizer}
	}
	g.Expect(testEnv.Create(ctx, cluster)).To(Succeed())
	if beingDeleted {
		g.Expect(testEnv.Delete(ctx, cluster)).To(Succeed())
		t.Cleanup(func() {
			patch := client.MergeFrom(cluster.DeepCopy())
			cluster.Finalizers = nil
			g.Expect(client.IgnoreNotFound(testEnv.Patch(ctx, cluster, patch))).To(Succeed())
		})
	}
	return cluster
}

// createControlPlane creates cp and then, when status is not nil, writes it through the status subresource as the
// controller does (the webhook never sees status writes: its rules do not match the status subresource).
func createControlPlane(g *WithT, cp *controlplanev1.IBMCloudManagedControlPlane, status *controlplanev1.IBMCloudManagedControlPlaneStatus) {
	g.Expect(testEnv.Create(ctx, cp)).To(Succeed())
	if status != nil {
		cp.Status = *status
		g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())
	}
}

// TestIBMCloudManagedControlPlaneWebhookCreate covers the ibmCloudClusterName warning through the API server,
// including the shape of the managed templates (a control plane with only a name, created after its Cluster): no
// template sets ibmCloudClusterName, so template users get the IBM name from the Cluster and its warning.
func TestIBMCloudManagedControlPlaneWebhookCreate(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-webhook-create")

	t.Run("warning names the Cluster of a template control plane", func(t *testing.T) {
		g := NewWithT(t)
		c, recorder := newWarningClient(g)
		// kubectl apply of a clusterctl template creates the Cluster first; the control plane has no label and no
		// owner reference, so only the Cluster's controlPlaneRef tells which Cluster names the IBM cluster.
		createCluster(t, namespace, "templated", false)
		cp := newControlPlane(namespace, "templated")
		cp.Spec.IBMCloudClusterName = ""
		cp.Labels = nil

		g.Expect(c.Create(ctx, cp)).To(Succeed())
		g.Expect(recorder.warnings).To(Equal([]string{clusterNameWarning("templated")}))
	})
	t.Run("warning names the Cluster from the label", func(t *testing.T) {
		g := NewWithT(t)
		c, recorder := newWarningClient(g)
		cp := newControlPlane(namespace, "warned")
		cp.Spec.IBMCloudClusterName = ""

		g.Expect(c.Create(ctx, cp)).To(Succeed())
		g.Expect(recorder.warnings).To(Equal([]string{clusterNameWarning("warned")}))
	})
	t.Run("generic warning when no Cluster references the control plane", func(t *testing.T) {
		g := NewWithT(t)
		c, recorder := newWarningClient(g)
		cp := newControlPlane(namespace, "orphan")
		cp.Spec.IBMCloudClusterName = ""
		cp.Labels = nil

		g.Expect(c.Create(ctx, cp)).To(Succeed())
		g.Expect(recorder.warnings).To(Equal([]string{clusterNameWarning(unknownClusterTarget)}))
	})
	t.Run("no warning when ibmCloudClusterName is set", func(t *testing.T) {
		g := NewWithT(t)
		c, recorder := newWarningClient(g)

		g.Expect(c.Create(ctx, newControlPlane(namespace, "named"))).To(Succeed())
		g.Expect(recorder.warnings).To(BeEmpty())
	})

	// IBM's cluster name rule: 1 to 32 characters, a letter first, then letters, digits, '-', '_' and '.'.
	const denial = webhookDenial + ` "vibmcloudmanagedcontrolplane.kb.io" denied the request`
	t.Run("defaulted_Cluster_name_of_33_characters_of_a_template_control_plane_rejected", func(t *testing.T) {
		g := NewWithT(t)
		// The 33-character name IBM refused with HTTP 400, code E0125: a valid Cluster name, but not a valid IBM name.
		const clusterName = "example-test-cluster-e01-mgmt-iks"
		createCluster(t, namespace, clusterName, false)
		cp := newControlPlane(namespace, clusterName)
		cp.Spec.IBMCloudClusterName = ""
		cp.Labels = nil

		err := testEnv.Create(ctx, cp)

		g.Expect(err).To(MatchError(And(
			ContainSubstring(denial),
			ContainSubstring("spec.ibmCloudClusterName: Required value"),
			ContainSubstring(`would be named after Cluster "`+clusterName+`"`),
			ContainSubstring("it has 33 characters, more than 32"),
			ContainSubstring("set spec.ibmCloudClusterName to a name that follows this rule"))))
		g.Expect(apierrors.IsNotFound(testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(cp), cp))).To(BeTrue(),
			"the rejected control plane was not stored")
	})
	t.Run("defaulted_Cluster_name_starting_with_a_digit_from_the_label_rejected", func(t *testing.T) {
		g := NewWithT(t)
		cp := newControlPlane(namespace, "1st-cluster")
		cp.Spec.IBMCloudClusterName = ""

		g.Expect(testEnv.Create(ctx, cp)).To(MatchError(And(
			ContainSubstring(denial),
			ContainSubstring(`would be named after Cluster "1st-cluster"`),
			ContainSubstring("it starts with '1' instead of a letter"))))
	})
	t.Run("long_Cluster_name_with_a_valid_ibmCloudClusterName_allowed", func(t *testing.T) {
		g := NewWithT(t)
		c, recorder := newWarningClient(g)
		const clusterName = "example-test-cluster-e02-mgmt-iks"
		createCluster(t, namespace, clusterName, false)
		cp := newControlPlane(namespace, clusterName)
		cp.Spec.IBMCloudClusterName = "etc-e02-mgmt-iks"
		cp.Labels = nil

		g.Expect(c.Create(ctx, cp)).To(Succeed(), "setting ibmCloudClusterName is the fix the rejection names")
		g.Expect(recorder.warnings).To(BeEmpty())
	})
	t.Run("explicit_ibmCloudClusterName_of_33_characters_rejected_by_the_schema", func(t *testing.T) {
		g := NewWithT(t)
		cp := newControlPlane(namespace, "too-long")
		cp.Spec.IBMCloudClusterName = "a" + strings.Repeat("b", 32)

		expectRejectedByCEL(g, testEnv.Create(ctx, cp), "spec.ibmCloudClusterName: Too long")
	})
	t.Run("explicit_ibmCloudClusterName_with_underscore_and_dot_allowed", func(t *testing.T) {
		g := NewWithT(t)
		c, recorder := newWarningClient(g)
		cp := newControlPlane(namespace, "underscore")
		cp.Spec.IBMCloudClusterName = "team_a.underscore"

		g.Expect(c.Create(ctx, cp)).To(Succeed())
		g.Expect(recorder.warnings).To(BeEmpty())
	})
}

// TestIBMCloudManagedControlPlaneWebhookUpdate covers the minor step through the API server: the status is written
// through the status subresource, and only a change of spec.distributionVersion is checked. The byCEL rows re-test
// CEL rules with the webhook registered (the CEL tests of api/managed/controlplane/v1alpha1 test CEL without
// webhooks): the API server runs CEL first, so a value CEL rejects never reaches the validator, and every other
// rejection must carry the webhook's denial prefix.
func TestIBMCloudManagedControlPlaneWebhookUpdate(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-webhook-update")
	const denial = webhookDenial + ` "vibmcloudmanagedcontrolplane.kb.io" denied the request`

	testCases := []struct {
		name     string
		roks     bool
		oldSpec  string
		observed string
		mutate   func(cp *controlplanev1.IBMCloudManagedControlPlane)
		wantErr  string
		// byCEL marks a row whose rejection must come from a CEL rule, not from the validator.
		byCEL bool
	}{
		{
			// The validator would reject "1.37.1" too (ibmversion.ParseMinor), so the missing denial prefix proves
			// that CEL ran first.
			name: "CEL rejects a patch version before the validator", oldSpec: "1.36", observed: "1.36.5_1537",
			mutate:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.37.1" },
			wantErr: "distributionVersion must be MAJOR.MINOR", byCEL: true,
		},
		{
			name: "CEL rejects a distribution change", oldSpec: "1.36", observed: "1.36.5_1537",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Distribution = controlplanev1.DistributionOpenShift
			},
			wantErr: "distribution is immutable", byCEL: true,
		},
		{
			name: "CEL rejects an ibmCloudClusterName change", oldSpec: "1.36", observed: "1.36.5_1537",
			mutate:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.IBMCloudClusterName = "renamed" },
			wantErr: "ibmCloudClusterName is immutable", byCEL: true,
		},
		{
			name: "plus one minor allowed", oldSpec: "1.36", observed: "1.36.5_1537",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.37" },
		},
		{
			name: "plus two minors rejected", oldSpec: "1.36", observed: "1.36.5_1537",
			mutate:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.38" },
			wantErr: "so the highest allowed value is 1.37",
		},
		{
			name: "downgrade rejected", oldSpec: "1.36", observed: "1.36.5_1537",
			mutate:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.35" },
			wantErr: "so the lowest allowed value is 1.36",
		},
		{
			name: "unchanged version with a two-minor gap: label update allowed", oldSpec: "1.36", observed: "1.34.9_1530",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Labels["example.com/touched"] = "true" },
		},
		{
			name: "unchanged version with a downgrade gap: spec update allowed", oldSpec: "1.36", observed: "1.37.1_1517",
			mutate: withEndpoint,
		},
		{
			name: "no observed version: any step allowed", oldSpec: "1.36",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.38" },
		},
		{
			name: "OpenShift plus one minor allowed", roks: true, oldSpec: "4.21", observed: "4.21.35_1537_openshift",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "4.22" },
		},
		{
			name: "OpenShift plus two minors rejected", roks: true, oldSpec: "4.21", observed: "4.21.35_1537_openshift",
			mutate:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "4.23" },
			wantErr: "so the highest allowed value is 4.22",
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := newControlPlane(namespace, fmt.Sprintf("update-%d", i))
			cp.Spec.DistributionVersion = tc.oldSpec
			if tc.roks {
				cp.Spec.Distribution = controlplanev1.DistributionOpenShift
				cp.Spec.OpenShift.ImageRegistry.COS.InstanceCRN = "crn:v1:bluemix:public:cloud-object-storage:global:" +
					"a/00000000000000000000000000000000:00000000-0000-0000-0000-000000000000::"
			}
			var status *controlplanev1.IBMCloudManagedControlPlaneStatus
			if tc.observed != "" {
				status = &controlplanev1.IBMCloudManagedControlPlaneStatus{DistributionVersion: tc.observed}
			}
			createControlPlane(g, cp, status)

			tc.mutate(cp)
			err := testEnv.Update(ctx, cp)

			switch {
			case tc.wantErr == "":
				g.Expect(err).NotTo(HaveOccurred())
			case tc.byCEL:
				expectRejectedByCEL(g, err, tc.wantErr)
			default:
				g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)
				g.Expect(err.Error()).To(ContainSubstring(denial))
				g.Expect(err.Error()).To(ContainSubstring("spec.distributionVersion"))
				g.Expect(err.Error()).To(ContainSubstring(tc.wantErr))
			}
		})
	}
}

// TestIBMCloudManagedControlPlaneWebhookDelete covers every case of the delete guard through the API server.
func TestIBMCloudManagedControlPlaneWebhookDelete(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-webhook-delete")
	ibmCluster := &controlplanev1.IBMCloudManagedControlPlaneStatus{ClusterID: testIBMClusterID}

	testCases := []struct {
		name string
		// prepare creates the objects of the row and returns the control plane to delete.
		prepare func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane
		wantErr bool
	}{
		{
			name: "delete rejected while the Cluster lives",
			prepare: func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				cp := newControlPlane(namespace, clusterName)
				createControlPlane(g, cp, ibmCluster)
				return cp
			},
			wantErr: true,
		},
		{
			name: "delete rejected while the owning Cluster lives, found by the owner reference",
			prepare: func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
				t.Helper()
				cluster := createCluster(t, namespace, clusterName, false)
				cp := newControlPlane(namespace, clusterName)
				cp.Labels = nil
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: cluster.Name, UID: cluster.UID,
				}}
				createControlPlane(g, cp, ibmCluster)
				return cp
			},
			wantErr: true,
		},
		{
			name: "delete allowed while the Cluster is deleting",
			prepare: func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
				t.Helper()
				createCluster(t, namespace, clusterName, true)
				cp := newControlPlane(namespace, clusterName)
				createControlPlane(g, cp, ibmCluster)
				return cp
			},
		},
		{
			name: "delete allowed when the Cluster is gone",
			prepare: func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
				t.Helper()
				cp := newControlPlane(namespace, clusterName)
				createControlPlane(g, cp, ibmCluster)
				return cp
			},
		},
		{
			name: "delete allowed with delete-for-move",
			prepare: func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				cp := newControlPlane(namespace, clusterName)
				cp.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
				createControlPlane(g, cp, ibmCluster)
				return cp
			},
		},
		{
			name: "delete allowed with no IBM cluster yet",
			prepare: func(t *testing.T, g *WithT, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
				t.Helper()
				createCluster(t, namespace, clusterName, false)
				cp := newControlPlane(namespace, clusterName)
				createControlPlane(g, cp, nil)
				return cp
			},
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			clusterName := fmt.Sprintf("d41-%d", i)
			cp := tc.prepare(t, g, clusterName)

			err := testEnv.Delete(ctx, cp)

			if !tc.wantErr {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(apierrors.IsForbidden(err)).To(BeTrue(), "want Forbidden, got %v", err)
			g.Expect(err.Error()).To(ContainSubstring(controlPlaneDeleteDeniedMessage(clusterName)))
		})
	}
}
