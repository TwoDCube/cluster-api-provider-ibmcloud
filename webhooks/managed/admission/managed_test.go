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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	utilyaml "sigs.k8s.io/cluster-api/util/yaml"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// Fixtures shared by the unit tests (typed validators with a strict fake reader) and the envtest tests (validators
// behind the API server). Every value is synthetic; tests never carry real account data.
const (
	// testResourceGroupID is a synthetic resource group ID (32 hex characters).
	testResourceGroupID = "0123456789abcdef0123456789abcdef"

	// testIBMClusterID is a synthetic IBM cluster ID in IBM's 20-character form.
	testIBMClusterID = "c0ffee0000000000test"

	// holdFinalizer keeps a test Cluster in the "being deleted" state: envtest runs no CAPI controller, so a Cluster
	// with a finalizer keeps its deletionTimestamp until the test removes the finalizer.
	holdFinalizer = "test.cluster.x-k8s.io/hold"

	// unitNamespace is the namespace of the objects in the unit tests, which never reach an API server.
	unitNamespace = "unit"
)

// controlPlaneNameFor returns the name of the IBMCloudManagedControlPlane that newCluster's controlPlaneRef names.
func controlPlaneNameFor(clusterName string) string {
	return clusterName + "-control-plane"
}

// newCluster returns a CAPI Cluster whose controlPlaneRef names the IBMCloudManagedControlPlane controlPlaneNameFor(name)
// and whose infrastructureRef names the IBMCloudManagedCluster name, the shape of the managed templates.
func newCluster(namespace, name string) *clusterv1.Cluster {
	return &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: clusterv1.ClusterSpec{
			ControlPlaneRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: controlplanev1.GroupVersion.Group,
				Kind:     "IBMCloudManagedControlPlane",
				Name:     controlPlaneNameFor(name),
			},
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group,
				Kind:     "IBMCloudManagedCluster",
				Name:     name,
			},
		},
	}
}

// newControlPlane returns a valid IKS IBMCloudManagedControlPlane named controlPlaneNameFor(clusterName) and labelled
// with clusterName. ibmCloudClusterName is set so that creating it raises no ibmCloudClusterName warning.
func newControlPlane(namespace, clusterName string) *controlplanev1.IBMCloudManagedControlPlane {
	return &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controlPlaneNameFor(clusterName),
			Namespace: namespace,
			Labels:    map[string]string{clusterv1.ClusterNameLabel: clusterName},
		},
		Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution:        controlplanev1.DistributionKubernetes,
			DistributionVersion: "1.36",
			IBMCloudClusterName: clusterName,
		},
	}
}

// newInfraCluster returns a valid IBMCloudManagedCluster named clusterName and labelled with it.
func newInfraCluster(namespace, clusterName string) *infrav1.IBMCloudManagedCluster {
	return &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
			Labels:    map[string]string{clusterv1.ClusterNameLabel: clusterName},
		},
		Spec: infrav1.IBMCloudManagedClusterSpec{
			Region:        "us-south",
			ResourceGroup: infrav1.ResourceGroupReference{ID: testResourceGroupID},
			VPC: infrav1.VPCSource{
				Type:      infrav1.SourceTypeReference,
				Reference: infrav1.ResourceIdentifier{Name: "test-vpc"},
			},
			Zones: []infrav1.ZoneSubnet{
				{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{Name: "test-subnet-1"}},
			},
		},
	}
}

// newMachinePool returns a valid IBMCloudManagedMachinePool named name and labelled with clusterName.
func newMachinePool(namespace, name, clusterName string) *infrav1.IBMCloudManagedMachinePool {
	return &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{clusterv1.ClusterNameLabel: clusterName},
		},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName: "default",
			Flavor:   "bx2.4x16",
			Zones:    []infrav1.WorkerPoolZone{{Name: "us-south-1"}},
		},
	}
}

// withClusterID, withEndpoint and withCreateIntent give a control plane exactly one of the three marks of an IBM
// cluster that may exist (noIBMClusterCanExist), so each mark is tested alone.
func withClusterID(cp *controlplanev1.IBMCloudManagedControlPlane) {
	cp.Status.ClusterID = testIBMClusterID
}

func withEndpoint(cp *controlplanev1.IBMCloudManagedControlPlane) {
	cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c100.us-south.containers.cloud.ibm.com", Port: 30000}
}

func withCreateIntent(cp *controlplanev1.IBMCloudManagedControlPlane) {
	cp.Status.CreateIntent = controlplanev1.CreateIntent{
		ClusterName:         cp.Spec.IBMCloudClusterName,
		InFlightAttemptTime: metav1.NewTime(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)),
	}
}

// deleting marks a unit-test Cluster as being deleted. The fake client accepts a deletionTimestamp only together with
// a finalizer, as the API server would hold the object.
func deleting(cluster *clusterv1.Cluster) *clusterv1.Cluster {
	cluster.Finalizers = []string{holdFinalizer}
	cluster.DeletionTimestamp = ptr.To(metav1.NewTime(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)))
	return cluster
}

// clusterGet and controlPlaneGet are the read ids strictReader records for a Get: "<kind> <namespace>/<name>".
func clusterGet(name string) string {
	return "Cluster " + unitNamespace + "/" + name
}

func controlPlaneGet(name string) string {
	return "IBMCloudManagedControlPlane " + unitNamespace + "/" + name
}

// clusterList is the read id strictReader records for a List of the Clusters in the unit-test namespace.
const clusterList = "ClusterList " + unitNamespace

// strictReader is the client.Reader of the validator unit tests. It serves Gets and Lists from a fake client, returns
// an injected error for the reads named in errs, and fails the test on every read that the case does not list, which
// proves that a delete guard reads nothing but the owning Cluster and its control plane (never a List), that the
// ibmCloudClusterName warning lists Clusters only when the object names none, and that nothing is read when a cheaper
// path applies. calls records every read in order.
type strictReader struct {
	t       *testing.T
	store   client.Reader
	allowed []string
	errs    map[string]error
	calls   []string
}

// newStrictReader returns a strictReader that allows the reads in allowed, injects the errors in errs and serves objs.
func newStrictReader(t *testing.T, allowed []string, errs map[string]error, objs ...client.Object) *strictReader {
	t.Helper()
	scheme := runtime.NewScheme()
	g := NewWithT(t)
	g.Expect(clusterv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(controlplanev1.AddToScheme(scheme)).To(Succeed())
	g.Expect(infrav1.AddToScheme(scheme)).To(Succeed())
	return &strictReader{
		t:       t,
		store:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		allowed: allowed,
		errs:    errs,
	}
}

func (r *strictReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	id := fmt.Sprintf("%T %s", obj, key)
	switch obj.(type) {
	case *clusterv1.Cluster:
		id = "Cluster " + key.String()
	case *controlplanev1.IBMCloudManagedControlPlane:
		id = "IBMCloudManagedControlPlane " + key.String()
	}
	if err := r.record(id); err != nil {
		return err
	}
	return r.store.Get(ctx, key, obj, opts...)
}

func (r *strictReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := &client.ListOptions{}
	listOpts.ApplyOptions(opts)
	id := fmt.Sprintf("%T %s", list, listOpts.Namespace)
	if _, ok := list.(*clusterv1.ClusterList); ok {
		id = "ClusterList " + listOpts.Namespace
	}
	if err := r.record(id); err != nil {
		return err
	}
	return r.store.List(ctx, list, opts...)
}

// record appends the read id to calls, fails the test when the case does not allow the read, and returns the error
// that the case injects for it (nil when none).
//
// id is the read id, see clusterGet, controlPlaneGet and clusterList.
func (r *strictReader) record(id string) error {
	r.calls = append(r.calls, id)
	if !slices.Contains(r.allowed, id) {
		r.t.Errorf("unexpected read %s; this case allows only %v", id, r.allowed)
		return fmt.Errorf("unexpected read %s", id)
	}
	return r.errs[id]
}

// expectAllowed checks a validator result that must allow the request without warnings.
func expectAllowed(g *WithT, warnings []string, err error) {
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(warnings).To(BeEmpty())
}

// expectForbidden checks a validator result that must reject the request as Forbidden with a message containing
// wantMessage.
func expectForbidden(g *WithT, err error, wantMessage string) {
	g.Expect(err).To(HaveOccurred())
	g.Expect(apierrors.IsForbidden(err)).To(BeTrue(), "want Forbidden, got %v", err)
	g.Expect(err.Error()).To(ContainSubstring(wantMessage))
}

// webhookDenial is the prefix the API server puts before every message of a validating webhook that rejects a request
// (`admission webhook "<name>" denied the request: <message>`); a CRD schema or CEL rejection never carries it.
const webhookDenial = "admission webhook"

// expectRejectedByCEL checks an envtest error that a CEL rule of the CRD must have caused, with the webhooks
// registered: Invalid, with wantMessage (the rule's message), and without webhookDenial. The API server runs the schema
// and its CEL rules before the validating webhooks, so a request CEL rejects never reaches a validator; the missing
// prefix proves that CEL, not the validator, rejected it.
func expectRejectedByCEL(g *WithT, err error, wantMessage string) {
	g.Expect(err).To(HaveOccurred())
	g.Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)
	g.Expect(err.Error()).To(ContainSubstring(wantMessage))
	g.Expect(err.Error()).NotTo(ContainSubstring(webhookDenial))
}

func TestClusterNameOf(t *testing.T) {
	clusterOwner := func(apiVersion, kind, name string) metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name, UID: "uid"}
	}
	testCases := []struct {
		name   string
		labels map[string]string
		owners []metav1.OwnerReference
		want   string
	}{
		{
			name: "no label and no owner names no Cluster",
		},
		{
			name:   "label",
			labels: map[string]string{clusterv1.ClusterNameLabel: "dev"},
			want:   "dev",
		},
		{
			name:   "label wins over the owner reference",
			labels: map[string]string{clusterv1.ClusterNameLabel: "dev"},
			owners: []metav1.OwnerReference{clusterOwner("cluster.x-k8s.io/v1beta2", "Cluster", "other")},
			want:   "dev",
		},
		{
			name:   "empty label falls back to the owner reference",
			labels: map[string]string{clusterv1.ClusterNameLabel: ""},
			owners: []metav1.OwnerReference{clusterOwner("cluster.x-k8s.io/v1beta2", "Cluster", "dev")},
			want:   "dev",
		},
		{
			name:   "owner reference of any Cluster API version",
			owners: []metav1.OwnerReference{clusterOwner("cluster.x-k8s.io/v1beta1", "Cluster", "dev")},
			want:   "dev",
		},
		{
			name: "owner of another kind or group is skipped",
			owners: []metav1.OwnerReference{
				clusterOwner("cluster.x-k8s.io/v1beta2", "MachinePool", "pool"),
				clusterOwner("example.com/v1", "Cluster", "foreign"),
				clusterOwner("cluster.x-k8s.io/v1beta2", "Cluster", "dev"),
			},
			want: "dev",
		},
		{
			name:   "malformed apiVersion is skipped",
			owners: []metav1.OwnerReference{clusterOwner("a/b/c", "Cluster", "broken")},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := &infrav1.IBMCloudManagedCluster{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels, OwnerReferences: tc.owners}}
			g.Expect(clusterNameOf(obj)).To(Equal(tc.want))
		})
	}
}

func TestRefersTo(t *testing.T) {
	ref := func(group, kind, name string) clusterv1.ContractVersionedObjectReference {
		return clusterv1.ContractVersionedObjectReference{APIGroup: group, Kind: kind, Name: name}
	}
	testCases := []struct {
		name string
		ref  clusterv1.ContractVersionedObjectReference
		gk   schema.GroupKind
		obj  string
		want bool
	}{
		{
			name: "same group, kind and name",
			ref:  ref("infrastructure.cluster.x-k8s.io", "IBMCloudManagedCluster", "dev"),
			gk:   infraClusterGroupKind, obj: "dev", want: true,
		},
		{
			name: "control plane reference",
			ref:  ref("controlplane.cluster.x-k8s.io", "IBMCloudManagedControlPlane", "dev-control-plane"),
			gk:   controlPlaneGroupKind, obj: "dev-control-plane", want: true,
		},
		{
			name: "another name",
			ref:  ref("infrastructure.cluster.x-k8s.io", "IBMCloudManagedCluster", "dev-v2"),
			gk:   infraClusterGroupKind, obj: "dev",
		},
		{
			name: "another kind",
			ref:  ref("infrastructure.cluster.x-k8s.io", "IBMVPCCluster", "dev"),
			gk:   infraClusterGroupKind, obj: "dev",
		},
		{
			name: "same kind in another group",
			ref:  ref("example.com", "IBMCloudManagedCluster", "dev"),
			gk:   infraClusterGroupKind, obj: "dev",
		},
		{
			name: "undefined reference",
			gk:   infraClusterGroupKind, obj: "dev",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(refersTo(tc.ref, tc.gk, tc.obj)).To(Equal(tc.want))
		})
	}
}

func TestIsBeingMoved(t *testing.T) {
	testCases := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{name: "no annotations"},
		{name: "other annotation", annotations: map[string]string{"example.com/x": ""}},
		{name: "delete-for-move with an empty value", annotations: map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}, want: true},
		{name: "delete-for-move with a value", annotations: map[string]string{clusterctlv1.DeleteForMoveAnnotation: "true"}, want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
			g.Expect(isBeingMoved(obj)).To(Equal(tc.want))
		})
	}
}

func TestNoIBMClusterCanExist(t *testing.T) {
	someTime := metav1.NewTime(time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC))
	testCases := []struct {
		name   string
		mutate func(cp *controlplanev1.IBMCloudManagedControlPlane)
		want   bool
	}{
		{name: "new control plane", mutate: func(*controlplanev1.IBMCloudManagedControlPlane) {}, want: true},
		{
			name: "unrelated status fields do not count",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Status.ExternalManagedControlPlane = ptr.To(true)
				cp.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}}
			},
			want: true,
		},
		{name: "status.clusterID", mutate: withClusterID},
		{name: "spec.controlPlaneEndpoint", mutate: withEndpoint},
		{
			name:   "spec.controlPlaneEndpoint with a host only",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.ControlPlaneEndpoint.Host = "h" },
		},
		{
			name:   "spec.controlPlaneEndpoint with a port only",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.ControlPlaneEndpoint.Port = 1 },
		},
		{name: "status.createIntent", mutate: withCreateIntent},
		{
			name:   "createIntent.clusterName alone",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Status.CreateIntent.ClusterName = "dev" },
		},
		{
			name: "createIntent.inFlightAttemptTime alone",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Status.CreateIntent.InFlightAttemptTime = someTime
			},
		},
		{
			name: "createIntent.firstUnknownOutcomeTime alone",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Status.CreateIntent.FirstUnknownOutcomeTime = someTime
			},
		},
		{
			name: "createIntent.lastUnknownOutcomeTime alone",
			mutate: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Status.CreateIntent.LastUnknownOutcomeTime = someTime
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := newControlPlane(unitNamespace, "dev")
			tc.mutate(cp)
			g.Expect(noIBMClusterCanExist(cp)).To(Equal(tc.want))
		})
	}
}

func TestLiveOwnerCluster(t *testing.T) {
	readFailure := errors.New("connection refused")
	testCases := []struct {
		name        string
		objs        []client.Object
		labels      map[string]string
		allowed     []string
		errs        map[string]error
		wantCluster string
		wantErr     string
	}{
		{
			name: "object names no Cluster: nothing is read",
		},
		{
			name:    "Cluster gone",
			labels:  map[string]string{clusterv1.ClusterNameLabel: "dev"},
			allowed: []string{clusterGet("dev")},
		},
		{
			name:    "Cluster being deleted",
			objs:    []client.Object{deleting(newCluster(unitNamespace, "dev"))},
			labels:  map[string]string{clusterv1.ClusterNameLabel: "dev"},
			allowed: []string{clusterGet("dev")},
		},
		{
			name:        "Cluster live",
			objs:        []client.Object{newCluster(unitNamespace, "dev")},
			labels:      map[string]string{clusterv1.ClusterNameLabel: "dev"},
			allowed:     []string{clusterGet("dev")},
			wantCluster: "dev",
		},
		{
			name:    "read error fails closed",
			labels:  map[string]string{clusterv1.ClusterNameLabel: "dev"},
			allowed: []string{clusterGet("dev")},
			errs:    map[string]error{clusterGet("dev"): readFailure},
			wantErr: "cannot read Cluster unit/dev to decide whether IBMCloudManagedCluster dev may be deleted: " +
				"connection refused; retry the deletion",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			reader := newStrictReader(t, tc.allowed, tc.errs, tc.objs...)
			obj := &infrav1.IBMCloudManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: unitNamespace, Labels: tc.labels}}

			cluster, err := liveOwnerCluster(context.Background(), reader, obj, "IBMCloudManagedCluster")

			g.Expect(reader.calls).To(Equal(tc.allowed))
			if tc.wantErr != "" {
				g.Expect(apierrors.IsInternalError(err)).To(BeTrue(), "want InternalError, got %v", err)
				g.Expect(err.Error()).To(ContainSubstring(tc.wantErr))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			if tc.wantCluster == "" {
				g.Expect(cluster).To(BeNil())
				return
			}
			g.Expect(cluster).NotTo(BeNil())
			g.Expect(cluster.Name).To(Equal(tc.wantCluster))
		})
	}
}

// TestSetupWebhookWithManagerRejectsNilReader checks that a validator with a delete guard refuses to register without a
// reader, so that cmd/main.go fails at startup. The manager is nil on purpose: the check runs before it is used.
func TestSetupWebhookWithManagerRejectsNilReader(t *testing.T) {
	testCases := []struct {
		name  string
		setup func() error
	}{
		{name: "IBMCloudManagedControlPlane", setup: func() error { return (&IBMCloudManagedControlPlane{}).SetupWebhookWithManager(nil) }},
		{name: "IBMCloudManagedCluster", setup: func() error { return (&IBMCloudManagedCluster{}).SetupWebhookWithManager(nil) }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := tc.setup()
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(Equal(tc.name + " webhook: the Reader field is nil; set it to the manager's API " +
				"reader (mgr.GetAPIReader()), which the delete guard uses to read the owning Cluster"))
		})
	}
}

// TestValidateDeleteWithNilReader checks that a guard whose reader is nil rejects the deletion with an InternalError
// instead of panicking inside the admission request.
func TestValidateDeleteWithNilReader(t *testing.T) {
	g := NewWithT(t)
	cp := newControlPlane(unitNamespace, "dev")
	withClusterID(cp)

	_, err := (&IBMCloudManagedControlPlane{ManagedKubernetesEnabled: true}).ValidateDelete(context.Background(), cp)
	g.Expect(apierrors.IsInternalError(err)).To(BeTrue(), "want InternalError, got %v", err)
	g.Expect(err.Error()).To(ContainSubstring("the Reader field is nil"))

	_, err = (&IBMCloudManagedCluster{ManagedKubernetesEnabled: true}).ValidateDelete(context.Background(), newInfraCluster(unitNamespace, "dev"))
	g.Expect(apierrors.IsInternalError(err)).To(BeTrue(), "want InternalError, got %v", err)
	g.Expect(err.Error()).To(ContainSubstring("the Reader field is nil"))
}

// TestGateOff covers the feature gate for all three kinds with the typed validators: with the ManagedKubernetes gate
// off, create is rejected with a message naming EXP_MANAGED_KUBERNETES, and update and delete are allowed without
// any read, even for objects the delete guards would otherwise protect (neither delete guard applies while the gate
// is off).
func TestGateOff(t *testing.T) {
	ctx := context.Background()
	// The objects carry every mark of an IBM cluster and a live Cluster exists, so only the gate can allow the
	// deletions; the strict reader allows no read at all.
	cp := newControlPlane(unitNamespace, "dev")
	withClusterID(cp)
	withEndpoint(cp)
	infra := newInfraCluster(unitNamespace, "dev")
	pool := newMachinePool(unitNamespace, "dev-pool", "dev")

	testCases := []struct {
		name string
		// deleteCase prefixes the name of the delete subtest: "webhook" for the delete rules of the control plane and pool
		// webhooks, "infracluster_guard" for the delete guard of IBMCloudManagedCluster.
		deleteCase     string
		validateCreate func(reader client.Reader) ([]string, error)
		validateUpdate func(reader client.Reader) ([]string, error)
		validateDelete func(reader client.Reader) ([]string, error)
		resource       string
	}{
		{
			name:       "IBMCloudManagedControlPlane",
			deleteCase: "webhook",
			resource:   "ibmcloudmanagedcontrolplanes.controlplane.cluster.x-k8s.io",
			validateCreate: func(reader client.Reader) ([]string, error) {
				return (&IBMCloudManagedControlPlane{Reader: reader}).ValidateCreate(ctx, cp)
			},
			validateUpdate: func(reader client.Reader) ([]string, error) {
				// Two minors above the observed master: rejected with the gate on, allowed with it off.
				newCP := cp.DeepCopy()
				newCP.Spec.DistributionVersion = "1.38"
				oldCP := cp.DeepCopy()
				oldCP.Status.DistributionVersion = "1.36.5_1537"
				return (&IBMCloudManagedControlPlane{Reader: reader}).ValidateUpdate(ctx, oldCP, newCP)
			},
			validateDelete: func(reader client.Reader) ([]string, error) {
				return (&IBMCloudManagedControlPlane{Reader: reader}).ValidateDelete(ctx, cp)
			},
		},
		{
			name:       "IBMCloudManagedCluster",
			deleteCase: "infracluster_guard",
			resource:   "ibmcloudmanagedclusters.infrastructure.cluster.x-k8s.io",
			validateCreate: func(reader client.Reader) ([]string, error) {
				return (&IBMCloudManagedCluster{Reader: reader}).ValidateCreate(ctx, infra)
			},
			validateUpdate: func(reader client.Reader) ([]string, error) {
				return (&IBMCloudManagedCluster{Reader: reader}).ValidateUpdate(ctx, infra, infra)
			},
			validateDelete: func(reader client.Reader) ([]string, error) {
				return (&IBMCloudManagedCluster{Reader: reader}).ValidateDelete(ctx, infra)
			},
		},
		{
			name:       "IBMCloudManagedMachinePool",
			deleteCase: "webhook",
			resource:   "ibmcloudmanagedmachinepools.infrastructure.cluster.x-k8s.io",
			validateCreate: func(client.Reader) ([]string, error) {
				return (&IBMCloudManagedMachinePool{}).ValidateCreate(ctx, pool)
			},
			validateUpdate: func(client.Reader) ([]string, error) {
				return (&IBMCloudManagedMachinePool{}).ValidateUpdate(ctx, pool, pool)
			},
			validateDelete: func(client.Reader) ([]string, error) {
				return (&IBMCloudManagedMachinePool{}).ValidateDelete(ctx, pool)
			},
		},
	}
	for _, tc := range testCases {
		t.Run("gate_off_"+tc.name+"_create_rejected", func(t *testing.T) {
			g := NewWithT(t)
			reader := newStrictReader(t, nil, nil, newCluster(unitNamespace, "dev"), cp)
			warnings, err := tc.validateCreate(reader)
			g.Expect(warnings).To(BeEmpty())
			expectForbidden(g, err, tc.resource)
			g.Expect(err.Error()).To(ContainSubstring(tc.name + " objects cannot be created while the " +
				"ManagedKubernetes feature gate is off"))
			g.Expect(err.Error()).To(ContainSubstring("set EXP_MANAGED_KUBERNETES=true"))
		})
		t.Run("gate_off_"+tc.name+"_update_allowed", func(t *testing.T) {
			g := NewWithT(t)
			reader := newStrictReader(t, nil, nil, newCluster(unitNamespace, "dev"), cp)
			warnings, err := tc.validateUpdate(reader)
			expectAllowed(g, warnings, err)
		})
		t.Run(tc.deleteCase+"_gate_off_"+tc.name+"_delete_allowed_without_reads", func(t *testing.T) {
			g := NewWithT(t)
			reader := newStrictReader(t, nil, nil, newCluster(unitNamespace, "dev"), cp)
			warnings, err := tc.validateDelete(reader)
			expectAllowed(g, warnings, err)
			g.Expect(reader.calls).To(BeEmpty())
		})
	}
}

// TestManagedWebhookManifests checks the generated webhook configuration (config/webhook/manifests.yaml, written by
// make manifests from the markers): exactly three validating webhooks for the managed kinds, the control plane and
// IBMCloudManagedCluster with DELETE for their delete guards, the pool without it (pool deletions are never
// rejected), and no mutating webhook (no defaulter).
func TestManagedWebhookManifests(t *testing.T) {
	g := NewWithT(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "webhook", "manifests.yaml"))
	g.Expect(err).NotTo(HaveOccurred())
	objs, err := utilyaml.ToUnstructured(data)
	g.Expect(err).NotTo(HaveOccurred())

	type webhook struct {
		path       string
		operations []string
	}
	validating := map[string]webhook{}
	for i := range objs {
		switch objs[i].GetKind() {
		case "MutatingWebhookConfiguration":
			var config admissionregistrationv1.MutatingWebhookConfiguration
			g.Expect(runtime.DefaultUnstructuredConverter.FromUnstructured(objs[i].Object, &config)).To(Succeed())
			for _, w := range config.Webhooks {
				for _, rule := range w.Rules {
					for _, resource := range rule.Resources {
						g.Expect(resource).NotTo(HavePrefix("ibmcloudmanaged"), "no defaulting webhook for %s", resource)
					}
				}
			}
		case "ValidatingWebhookConfiguration":
			var config admissionregistrationv1.ValidatingWebhookConfiguration
			g.Expect(runtime.DefaultUnstructuredConverter.FromUnstructured(objs[i].Object, &config)).To(Succeed())
			for _, w := range config.Webhooks {
				for _, rule := range w.Rules {
					for _, resource := range rule.Resources {
						if !strings.HasPrefix(resource, "ibmcloudmanaged") {
							continue
						}
						g.Expect(w.FailurePolicy).To(HaveValue(Equal(admissionregistrationv1.Fail)), w.Name)
						g.Expect(w.SideEffects).To(HaveValue(Equal(admissionregistrationv1.SideEffectClassNone)), w.Name)
						operations := make([]string, 0, len(rule.Operations))
						for _, op := range rule.Operations {
							operations = append(operations, string(op))
						}
						validating[resource] = webhook{path: *w.ClientConfig.Service.Path, operations: operations}
					}
				}
			}
		}
	}

	g.Expect(validating).To(Equal(map[string]webhook{
		"ibmcloudmanagedclusters": {
			path:       "/validate-infrastructure-cluster-x-k8s-io-v1alpha1-ibmcloudmanagedcluster",
			operations: []string{"CREATE", "UPDATE", "DELETE"},
		},
		"ibmcloudmanagedcontrolplanes": {
			path:       "/validate-controlplane-cluster-x-k8s-io-v1alpha1-ibmcloudmanagedcontrolplane",
			operations: []string{"CREATE", "UPDATE", "DELETE"},
		},
		"ibmcloudmanagedmachinepools": {
			path:       "/validate-infrastructure-cluster-x-k8s-io-v1alpha1-ibmcloudmanagedmachinepool",
			operations: []string{"CREATE", "UPDATE"},
		},
	}))
}
