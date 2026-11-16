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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// The control plane controller tests run Reconcile against the controller-runtime fake client, with a gomock
// Kubernetes Service client behind the stub ClientBuilder and the recording Resolver of fakes_test.go; gomock fails a
// test on every IBM call it does not expect, so a case without an expectation proves that no IBM call happens. An
// interceptor records every status patch, so that tests can check what reached the API server (for example that no
// status string is written as "", which the CRD's MinLength=1 rejects) and inject patch failures. The envtest tests at
// the end of this file cover the finalizer, pause and owner semantics against a real API server with the CRDs and
// webhooks; the manager tests among them count the IBM calls of the real watches, which is where a hot loop shows: a
// definitive create refusal must hold the next create, and an IBM error's changing incident ID must never reach status.

const (
	// cpTestNamespace and cpTestName name the Cluster, the IBMCloudManagedCluster and the control plane of a test.
	cpTestNamespace = "default"
	cpTestName      = "dev"
	// cpIKSClusterID is the synthetic ID of the IBM cluster of a test (the shape of the scrubbed live fixtures).
	cpIKSClusterID = "iksclusterid00000001"
	// cpTestCRN is a synthetic cluster CRN; its account segment is the scrubbed placeholder of the live fixtures.
	cpTestCRN = "crn:v1:bluemix:public:containers-kubernetes:us-south:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:iksclusterid00000001::"
)

// cpTestStart is the fake clock's start: three seconds before the createdDate of the live IKS fixture.
var cpTestStart = time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)

// cpObjects are the objects of one fake-client test; a test changes them before newCPFixture stores them.
type cpObjects struct {
	cluster *clusterv1.Cluster
	infra   *infrav1.IBMCloudManagedCluster
	cp      *controlplanev1.IBMCloudManagedControlPlane
	// extra are further objects, for example the initial pool, its MachinePool or the kubeconfig Secret.
	extra []client.Object
}

// newCPObjects returns a provisioned Cluster with its IBMCloudManagedCluster and an IKS control plane that already
// carries the finalizer and the Paused=False condition, so that one Reconcile runs the flow under test.
func newCPObjects() *cpObjects {
	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: cpTestName, UID: "cluster-uid"},
		Spec: clusterv1.ClusterSpec{
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedClusterKind, Name: cpTestName,
			},
			ControlPlaneRef: managedControlPlaneRef(cpTestName),
		},
		Status: clusterv1.ClusterStatus{Initialization: clusterv1.ClusterInitializationStatus{InfrastructureProvisioned: ptr.To(true)}},
	}
	infra := &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: cpTestName},
		Spec:       testInfraSpec(),
		Status: infrav1.IBMCloudManagedClusterStatus{
			VPC: infrav1.ResolvedResource{ID: testVPCID, Name: testVPCName},
			Zones: []infrav1.ResolvedZone{
				{Name: "us-south-1", Subnet: infrav1.ResolvedResource{ID: "sn-1"}, IPv4CIDRBlock: "10.240.0.0/24"},
				{Name: "us-south-2", Subnet: infrav1.ResolvedResource{ID: "sn-2"}, IPv4CIDRBlock: "10.240.64.0/24"},
			},
			Initialization: infrav1.IBMCloudManagedClusterInitializationStatus{Provisioned: ptr.To(true)},
		},
	}
	cp := &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  cpTestNamespace,
			Name:       cpTestName,
			Labels:     map[string]string{clusterv1.ClusterNameLabel: cpTestName},
			Finalizers: []string{controlplanev1.IBMCloudManagedControlPlaneFinalizer},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: cpTestName, UID: cluster.UID,
			}},
		},
		Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution:        controlplanev1.DistributionKubernetes,
			DistributionVersion: "1.36",
		},
		Status: controlplanev1.IBMCloudManagedControlPlaneStatus{Conditions: []metav1.Condition{{
			Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse, Reason: clusterv1.NotPausedReason,
			LastTransitionTime: metav1.NewTime(cpTestStart),
		}}},
	}
	return &cpObjects{cluster: cluster, infra: infra, cp: cp}
}

// cpFixture is one fake-client control plane test: the API objects, the gomock IBM client, the seam fakes and the
// reconciler wired to them.
type cpFixture struct {
	t        *testing.T
	client   client.Client
	kube     *ksmock.MockKubernetesService
	builder  *stubClientBuilder
	resolver *recordingResolver
	recorder *record.FakeRecorder
	clock    *clocktesting.FakePassiveClock
	r        *IBMCloudManagedControlPlaneReconciler

	mu sync.Mutex
	// statusPatches are the bodies of every status patch, in order.
	statusPatches []string
	// failStatusPatch, when set, fails every status patch whose body it matches, before the API server sees it.
	failStatusPatch func(body string) bool
}

// newCPFixture stores objs in a fake client and returns a fixture whose reconciler uses gomock IBM clients.
func newCPFixture(t *testing.T, objs *cpObjects) *cpFixture {
	t.Helper()
	g := NewWithT(t)
	f := &cpFixture{
		t:        t,
		kube:     ksmock.NewMockKubernetesService(gomock.NewController(t)),
		resolver: &recordingResolver{},
		recorder: record.NewFakeRecorder(100),
		clock:    clocktesting.NewFakePassiveClock(cpTestStart),
	}
	f.builder = &stubClientBuilder{kubernetes: f.kube}
	s := runtime.NewScheme()
	g.Expect(clientgoscheme.AddToScheme(s)).To(Succeed())
	g.Expect(clusterv1.AddToScheme(s)).To(Succeed())
	g.Expect(infrav1.AddToScheme(s)).To(Succeed())
	g.Expect(controlplanev1.AddToScheme(s)).To(Succeed())
	initial := append([]client.Object{objs.cluster, objs.infra, objs.cp}, objs.extra...)
	f.client = fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(initial...).
		WithStatusSubresource(&clusterv1.Cluster{}, &infrav1.IBMCloudManagedCluster{}, &controlplanev1.IBMCloudManagedControlPlane{},
			&infrav1.IBMCloudManagedMachinePool{}, &clusterv1.MachinePool{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: f.interceptStatusPatch}).
		Build()
	versions, err := managedscope.NewVersionsCache(f.clock, time.Hour)
	g.Expect(err).NotTo(HaveOccurred())
	f.r = &IBMCloudManagedControlPlaneReconciler{
		Client:        f.client,
		APIReader:     f.client,
		Recorder:      f.recorder,
		Resolver:      f.resolver,
		ClientBuilder: f.builder,
		Prober:        failIfCalledProber{t: t},
		VersionsCache: versions,
		Clock:         f.clock,
	}
	return f
}

// interceptStatusPatch records the body of a status patch and fails it when failStatusPatch matches.
func (f *cpFixture) interceptStatusPatch(ctx context.Context, c client.Client, subResource string, obj client.Object,
	patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if _, ok := obj.(*controlplanev1.IBMCloudManagedControlPlane); ok && subResource == "status" {
		data, err := patch.Data(obj)
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.statusPatches = append(f.statusPatches, string(data))
		fail := f.failStatusPatch != nil && f.failStatusPatch(string(data))
		f.mu.Unlock()
		if fail {
			return apierrors.NewServiceUnavailable("injected status patch failure")
		}
	}
	return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
}

// reconcile runs one Reconcile of the test control plane.
func (f *cpFixture) reconcile() (ctrl.Result, error) {
	return f.r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cpTestNamespace, Name: cpTestName}})
}

// controlPlane returns the persisted control plane, or nil when it is gone.
func (f *cpFixture) controlPlane() *controlplanev1.IBMCloudManagedControlPlane {
	f.t.Helper()
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	err := f.client.Get(ctx, types.NamespacedName{Namespace: cpTestNamespace, Name: cpTestName}, cp)
	if apierrors.IsNotFound(err) {
		return nil
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return cp
}

// events drains the recorded events ("<type> <reason> <message>").
func (f *cpFixture) events() []string {
	var events []string
	for {
		select {
		case event := <-f.recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// patches returns the recorded status patch bodies.
func (f *cpFixture) patches() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.statusPatches...)
}

// failIfCalledProber is a workloadprobe.Prober for the control plane steps of this release, which never probe.
type failIfCalledProber struct {
	t *testing.T
}

// Probe implements workloadprobe.Prober; it fails the test.
func (p failIfCalledProber) Probe(context.Context, []byte) (workloadprobe.Result, error) {
	p.t.Errorf("Prober.Probe called, but no control plane step of this release probes a kubeconfig")
	return workloadprobe.Result{}, errors.New("failIfCalledProber: unexpected Probe")
}

// cpCluster returns the IBM cluster of the test control plane in state, shaped like the scrubbed IKS getCluster
// answers recorded on IBM Cloud during a create: masterState, masterStatus and masterURL follow the state.
func cpCluster(state string) *kubernetesservice.Cluster {
	c := &kubernetesservice.Cluster{
		ID:                cpIKSClusterID,
		Name:              cpTestName,
		CRN:               cpTestCRN,
		Type:              "kubernetes",
		Provider:          "vpc-gen2",
		ResourceGroupID:   testResourceGroupID,
		CreatedDate:       "2026-11-16T03:05:33+0000",
		State:             state,
		MasterState:       "deployed",
		MasterStatus:      "Ready",
		MasterKubeVersion: "1.36.5_1537",
		TargetVersion:     "1.36.5_1537",
		MasterURL:         "https://c111.us-south.containers.cloud.ibm.com:30101",
	}
	if state == "deploying" || state == "requested" {
		c.MasterState, c.MasterStatus, c.MasterURL = "deploying", "Deploy in progress.", ""
	}
	return c
}

// ibmError returns IBM's answer to op with status and code.
func ibmError(op kubernetesservice.Operation, status int, code string) error {
	return &kubernetesservice.APIError{Operation: op, StatusCode: status, Code: code, IncidentID: "incident-1"}
}

// expectCondition checks one condition of cp.
func expectCondition(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane, conditionType string, status metav1.ConditionStatus, reason string) *metav1.Condition {
	condition := conditions.Get(cp, conditionType)
	g.ExpectWithOffset(1, condition).NotTo(BeNil(), "condition %s", conditionType)
	g.ExpectWithOffset(1, condition.Status).To(Equal(status), "condition %s: %s", conditionType, condition.Message)
	g.ExpectWithOffset(1, condition.Reason).To(Equal(reason), "condition %s: %s", conditionType, condition.Message)
	return condition
}

// expectRequeue checks that result requeues after about want (±10 % jitter).
func expectRequeue(g Gomega, result ctrl.Result, want time.Duration) {
	g.ExpectWithOffset(1, result.RequeueAfter).To(BeNumerically(">=", want*9/10), "requeue after %s", result.RequeueAfter)
	g.ExpectWithOffset(1, result.RequeueAfter).To(BeNumerically("<", want*11/10+time.Second), "requeue after %s", result.RequeueAfter)
}

// emptyStatusStrings returns the paths of status fields outside the conditions whose value is "" in a status patch
// body. CAPIBM omits an unset status string instead of writing "", which the CRD's MinLength=1 rejects.
// Condition messages are not checked: metav1.Condition always serializes its message.
func emptyStatusStrings(t *testing.T, body string) []string {
	t.Helper()
	var patch map[string]any
	NewWithT(t).Expect(json.Unmarshal([]byte(body), &patch)).To(Succeed())
	status, _ := patch["status"].(map[string]any)
	delete(status, "conditions")
	var empty []string
	var walk func(path string, value any)
	walk = func(path string, value any) {
		switch v := value.(type) {
		case string:
			if v == "" {
				empty = append(empty, path)
			}
		case map[string]any:
			for key, child := range v {
				walk(path+"."+key, child)
			}
		case []any:
			for _, child := range v {
				walk(path+"[]", child)
			}
		}
	}
	walk("status", status)
	return empty
}

func TestControlPlaneReconcileBinding(t *testing.T) {
	testCases := []struct {
		name string
		// setup changes the objects before the fixture stores them.
		setup func(objs *cpObjects)
		// expect sets the IBM calls the case allows.
		expect func(f *cpFixture)
		// want checks the persisted control plane.
		want        func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane)
		wantRequeue time.Duration
		wantErr     string
	}{
		{
			name: "a deploying cluster is creating, without an endpoint",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
				objs.cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: cpTestName, InFlightAttemptTime: metav1.NewTime(cpTestStart)}
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("deploying"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
				g.Expect(cp.Spec.ControlPlaneEndpoint).To(BeZero(), "no endpoint while the master deploys: IBM's masterURL is empty until then")
				g.Expect(cp.Status.CreateIntent).To(BeZero(), "row 1 observed the cluster by ID, which clears the intent")
				g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID))
			},
			wantRequeue: time.Minute,
		},
		{
			name: "row 1 normal cluster is provisioned",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("normal"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
				g.Expect(condition.Message).To(BeEmpty())
				g.Expect(cp.Status.ExternalManagedControlPlane).To(Equal(ptr.To(true)))
			},
			wantRequeue: 5 * time.Minute,
		},
		{
			name: "an unknown IBM state is named and treated as existing",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("hibernating"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
				g.Expect(condition.Message).To(Equal("unknown IBM state hibernating"))
			},
			wantRequeue: 5 * time.Minute,
		},
		{
			name: "create_failed is terminal and never deleted",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("create_failed"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreateFailedReason)
				g.Expect(condition.Message).To(ContainSubstring("delete this Cluster and create it again"))
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "a cluster IBM deletes while CAPI does not is DeletedOutOfBand",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("deleting"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedDeletedOutOfBandReason)
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "a known ID that IBM answers with 404 is never recreated",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason)
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "an endpoint set and a lookup answered with 404 is never recreated",
			setup: func(objs *cpObjects) {
				objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpTestName, testResourceGroupID).Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason)
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "row 1b binding mismatch changes nothing",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				other := cpCluster("normal")
				other.ResourceGroupID = "fedcba9876543210fedcba9876543210"
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(other, nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason)
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "a foreign same-named cluster without a create intent is a name conflict",
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpTestName, testResourceGroupID).Return(cpCluster("normal"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason)
				g.Expect(condition.Message).NotTo(ContainSubstring("delete it"), "never suggests deleting the other cluster")
				g.Expect(cp.Status.ClusterID).To(BeEmpty())
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "a moved Secret's cluster-id without the proof is OwnershipNotProven",
			setup: func(objs *cpObjects) {
				objs.extra = append(objs.extra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
					Namespace: cpTestNamespace, Name: cpTestName + "-kubeconfig",
					Annotations: map[string]string{controlplanev1.KubeconfigClusterIDAnnotation: cpIKSClusterID},
				}})
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("normal"), nil)
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason)
				g.Expect(cp.Status.ClusterID).To(BeEmpty(), "row 2b binds nothing")
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "row 3 waits for IBM to list a created cluster",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
				objs.cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: cpTestName, InFlightAttemptTime: metav1.NewTime(cpTestStart.Add(-time.Minute))}
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
				g.Expect(cp.Status.CreateIntent.ClusterName).To(Equal(cpTestName), "the intent stays during the wait")
			},
			wantRequeue: time.Minute,
		},
		{
			name: "infrastructure not provisioned waits without an IBM call",
			setup: func(objs *cpObjects) {
				objs.cluster.Status.Initialization.InfrastructureProvisioned = nil
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason)
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason)
			},
			wantRequeue: time.Minute,
		},
		{
			name: "a missing IBMCloudManagedCluster waits without an IBM call",
			setup: func(objs *cpObjects) {
				objs.cluster.Spec.InfrastructureRef.Name = "missing"
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason)
				g.Expect(condition.Message).To(ContainSubstring("IBMCloudManagedCluster/missing"))
			},
			wantRequeue: time.Minute,
		},
		{
			name: "an infrastructureRef of another kind is InvalidConfiguration",
			setup: func(objs *cpObjects) {
				objs.cluster.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group, Kind: "IBMVPCCluster", Name: cpTestName,
				}
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason)
				g.Expect(condition.Message).To(ContainSubstring("IBMVPCCluster"))
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "a Cluster name IBM refuses is InvalidConfiguration without an IBM call",
			setup: func(objs *cpObjects) {
				objs.cluster.Name = "1dev"
				objs.cp.OwnerReferences[0].Name = "1dev"
				objs.cp.Labels[clusterv1.ClusterNameLabel] = "1dev"
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason)
				g.Expect(condition.Message).To(ContainSubstring("spec.ibmCloudClusterName"))
			},
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "IBM 500 on GetCluster is retried with backoff",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusInternalServerError, "E0004"))
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason)
				g.Expect(condition.Message).To(Equal("CAPIBM could not complete getting IBM cluster iksclusterid00000001 (IBM code E0004); "+
					"it retries, and the controller log names the cause"), "no incident ID: it changes on every retry")
			},
			wantErr: "HTTP 500",
		},
		{
			name: "IBM 429 on GetCluster backs off without an error",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
			},
			expect: func(f *cpFixture) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusTooManyRequests, ""))
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason)
			},
			wantRequeue: 31 * time.Second,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newCPObjects()
			if tc.setup != nil {
				tc.setup(objs)
			}
			f := newCPFixture(t, objs)
			if tc.expect != nil {
				tc.expect(f)
			}

			result, err := f.reconcile()

			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				expectRequeue(g, result, tc.wantRequeue)
			}
			cp := f.controlPlane()
			expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneNotDeletingReason)
			tc.want(g, cp)
			g.Expect(f.events()).To(BeEmpty(), "binding and observing emit no event")
			for _, body := range f.patches() {
				g.Expect(emptyStatusStrings(t, body)).To(BeEmpty(), "no status patch writes an empty string: %s", body)
			}
		})
	}
}

func TestControlPlaneReconcileCredentials(t *testing.T) {
	t.Run("the identity holder is the IBMCloudManagedCluster when the cluster is observed", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		f := newCPFixture(t, objs)
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("normal"), nil)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectHolderIsInfraCluster(g, f)
	})

	t.Run("an identity resolution error is persisted on the object", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newCPObjects())
		f.resolver.err = errors.New("the holder lost its identity")

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("the holder lost its identity")))
		g.Expect(f.builder.Identities()).To(BeEmpty(), "no client is built without an identity")
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason)
		g.Expect(condition.Message).To(Equal("CAPIBM cannot resolve the IBM Cloud credentials of this control plane through " +
			"IBMCloudManagedCluster/dev; the controller log names the cause"))
	})

	t.Run("a client build error names the identity", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newCPObjects())
		f.builder.kubernetes = nil // the stub builder then fails, as a broken credential would

		_, err := f.reconcile()

		g.Expect(err).To(HaveOccurred())
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason)
		g.Expect(condition.Message).To(Equal("CAPIBM cannot build the Kubernetes Service client of the CAPIBM controller identity " +
			"for region us-south; the controller log names the cause"))
	})

	t.Run("the scope constructor resolves no identity and builds no client", func(t *testing.T) {
		g := NewWithT(t)
		params := &managedscope.ScopeParams{
			Client: fake.NewClientBuilder().Build(), APIReader: fake.NewClientBuilder().Build(), Recorder: record.NewFakeRecorder(1),
			Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t}, Clock: clocktesting.NewFakePassiveClock(cpTestStart),
		}
		scope, err := managedscope.NewControlPlaneScope(params, &clusterv1.Cluster{}, &controlplanev1.IBMCloudManagedControlPlane{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(scope).NotTo(BeNil())
	})

	t.Run("a 401 invalidates only that identity's authenticator and names the identity", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		f := newCPFixture(t, objs)
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusUnauthorized, ""))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		g.Expect(f.builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason)
		g.Expect(condition.Message).To(Equal("IBM Cloud rejected the credentials of the CAPIBM controller identity while getting " +
			"IBM cluster iksclusterid00000001; CAPIBM rebuilds its authenticator and retries in about 5 minutes; check that the " +
			"API key of the CAPIBM controller identity is valid and not deleted"))
		expectNoIdentityDetails(g, condition.Message)
	})

	t.Run("a 403 names the roles and the identity", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		f := newCPFixture(t, objs)
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusForbidden, ""))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason)
		g.Expect(condition.Message).To(Equal("IBM Cloud refused getting IBM cluster iksclusterid00000001 with HTTP 403: " +
			"the credentials of the CAPIBM controller identity need the Administrator platform role and the Manager " +
			"service role of Kubernetes Service on resource group 0123456789abcdef0123456789abcdef; the IAM page of the managed " +
			"Kubernetes book lists every role"))
		expectNoIdentityDetails(g, condition.Message)
	})
}

// expectHolderIsInfraCluster checks that every identity was resolved with the Cluster's IBMCloudManagedCluster as the
// holder and that every client was built for the controller identity.
func expectHolderIsInfraCluster(g Gomega, f *cpFixture) {
	holders := f.resolver.Holders()
	g.ExpectWithOffset(1, holders).NotTo(BeEmpty())
	for _, holder := range holders {
		infra, ok := holder.(*infrav1.IBMCloudManagedCluster)
		g.ExpectWithOffset(1, ok).To(BeTrue(), "the holder is an IBMCloudManagedCluster, got %T", holder)
		g.ExpectWithOffset(1, client.ObjectKeyFromObject(infra)).To(Equal(client.ObjectKey{Namespace: cpTestNamespace, Name: cpTestName}))
	}
	g.ExpectWithOffset(1, f.builder.Identities()).To(HaveEach(HaveField("Key", credentials.ControllerKey())))
}

// expectNoIdentityDetails checks that a credential message carries no IAM ID, e-mail address or key fingerprint: it
// names the identity only by its description.
func expectNoIdentityDetails(g Gomega, message string) {
	for _, pattern := range []string{
		`(?i)ibmid-|iam-serviceid-|serviceid-[0-9a-f]`,   // IAM IDs
		`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`, // e-mail addresses
		`([0-9A-Fa-f]{2}:){15,}[0-9A-Fa-f]{2}`,           // colon-separated key fingerprints
		`KubeCert[#]|(?i)sha256:`,                        // certificate subjects (IBM's admin CN prefix) and digests
	} {
		g.ExpectWithOffset(1, message).NotTo(MatchRegexp(pattern))
	}
}

func TestControlPlaneReconcileMakesNoIBMCall(t *testing.T) {
	newNoCallFixture := func(t *testing.T, objs *cpObjects) *cpFixture {
		t.Helper()
		f := newCPFixture(t, objs)
		f.r.Resolver = failIfCalledResolver{t: t}
		f.r.ClientBuilder = failIfCalledBuilder{t: t}
		return f
	}

	t.Run("a paused Cluster makes no IBM call", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		objs.cluster.Spec.Paused = ptr.To(true)
		f := newNoCallFixture(t, objs)

		for range 2 {
			_, err := f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
		}
		g.Expect(conditions.IsTrue(f.controlPlane(), clusterv1.PausedCondition)).To(BeTrue())
	})

	t.Run("a paused Cluster makes no IBM call while the control plane is deleted", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		objs.cluster.Spec.Paused = ptr.To(true)
		f := newNoCallFixture(t, objs)

		for range 2 {
			_, err := f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
		}
		g.Expect(f.controlPlane().Finalizers).To(ContainElement(controlplanev1.IBMCloudManagedControlPlaneFinalizer))
	})

	t.Run("a control plane deleted by clusterctl move makes no IBM call", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		objs.cp.Annotations = map[string]string{clusterctlv1.DeleteForMoveAnnotation: ""}
		objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		f := newNoCallFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed and the object is gone")
	})

	t.Run("a deleting control plane whose IBMCloudManagedCluster is gone keeps its finalizer and reports Deleting", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}
		objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		f := newNoCallFixture(t, objs)
		f.r.Resolver, f.r.ClientBuilder = f.resolver, f.builder
		f.kube.EXPECT().GetCluster(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
			Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		if cp := f.controlPlane(); cp != nil {
			condition := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), "Deleting is True with a specific reason while deleting")
			g.Expect(condition.Reason).NotTo(Equal(controlplanev1.IBMCloudManagedControlPlaneNotDeletingReason))
		}
	})

	t.Run("a deleting control plane that cannot have an IBM cluster loses its finalizer", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		f := newNoCallFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
	})

	t.Run("an unowned control plane is left alone", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.OwnerReferences = nil
		objs.cp.Finalizers = nil
		f := newNoCallFixture(t, objs)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		g.Expect(f.controlPlane().Finalizers).To(BeEmpty())
	})

	t.Run("a missing control plane is ignored", func(t *testing.T) {
		g := NewWithT(t)
		f := newNoCallFixture(t, newCPObjects())
		result, err := f.r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cpTestNamespace, Name: "missing"}})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
	})
}

func TestControlPlaneMapFunctions(t *testing.T) {
	objs := newCPObjects()
	other := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: "kcp"},
		Spec: clusterv1.ClusterSpec{ControlPlaneRef: clusterv1.ContractVersionedObjectReference{
			APIGroup: controlplanev1.GroupVersion.Group, Kind: "KubeadmControlPlane", Name: "kcp",
		}},
	}
	objs.extra = append(objs.extra, other)
	f := newCPFixture(t, objs)
	want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: cpTestNamespace, Name: cpTestName}}}

	t.Run("a Cluster maps to its managed control plane", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect(f.r.clusterToControlPlane(ctx, objs.cluster)).To(Equal(want))
		g.Expect(f.r.clusterToControlPlane(ctx, other)).To(BeEmpty(), "another control plane kind is not ours")
		g.Expect(f.r.clusterToControlPlane(ctx, objs.cp)).To(BeEmpty(), "not a Cluster")
	})

	t.Run("a pool maps to the control plane of its Cluster", func(t *testing.T) {
		g := NewWithT(t)
		pool := &infrav1.IBMCloudManagedMachinePool{ObjectMeta: metav1.ObjectMeta{
			Namespace: cpTestNamespace, Name: "pool", Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName},
		}}
		g.Expect(f.r.machinePoolToControlPlane(ctx, pool)).To(Equal(want), "before create the initial pool matters")
		pool.Labels[clusterv1.ClusterNameLabel] = "kcp"
		g.Expect(f.r.machinePoolToControlPlane(ctx, pool)).To(BeEmpty())
		pool.Labels[clusterv1.ClusterNameLabel] = "gone"
		g.Expect(f.r.machinePoolToControlPlane(ctx, pool)).To(BeEmpty())
		pool.Labels = nil
		g.Expect(f.r.machinePoolToControlPlane(ctx, pool)).To(BeEmpty())
	})

	// Pool events reach the control plane only before create and while it is deleted, because every reconcile of a
	// bound control plane costs a GetCluster.
	poolOf := func(clusterName string) *infrav1.IBMCloudManagedMachinePool {
		return &infrav1.IBMCloudManagedMachinePool{ObjectMeta: metav1.ObjectMeta{
			Namespace: cpTestNamespace, Name: "pool", Labels: map[string]string{clusterv1.ClusterNameLabel: clusterName},
		}}
	}
	t.Run("a pool does not wake a bound control plane", func(t *testing.T) {
		g := NewWithT(t)
		bound := newCPObjects()
		bound.cp.Status.ClusterID = cpIKSClusterID
		f := newCPFixture(t, bound)
		g.Expect(f.r.machinePoolToControlPlane(ctx, poolOf(cpTestName))).To(BeEmpty())
	})

	t.Run("a pool wakes a bound control plane that is being deleted", func(t *testing.T) {
		g := NewWithT(t)
		deleting := newCPObjects()
		deleting.cp.Status.ClusterID = cpIKSClusterID
		deleting.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		f := newCPFixture(t, deleting)
		g.Expect(f.r.machinePoolToControlPlane(ctx, poolOf(cpTestName))).To(Equal(want))
	})

	t.Run("a pool whose control plane does not exist yet wakes nothing", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cluster.Spec.ControlPlaneRef.Name = "not-created-yet"
		f := newCPFixture(t, objs)
		g.Expect(f.r.machinePoolToControlPlane(ctx, poolOf(cpTestName))).To(BeEmpty())
	})
}

func TestControlPlaneReconcilerValidate(t *testing.T) {
	g := NewWithT(t)
	err := (&IBMCloudManagedControlPlaneReconciler{}).validate()
	for _, field := range []string{"Client", "APIReader", "Recorder", "Resolver", "ClientBuilder", "Prober", "VersionsCache", "Clock"} {
		g.Expect(err).To(MatchError(ContainSubstring("IBMCloudManagedControlPlaneReconciler." + field + " is nil")))
	}
	g.Expect(strings.Count(err.Error(), "is nil")).To(Equal(8))
}

// ibmErrorWithIncident returns IBM's answer to op with status, code and incident: IBM sends a new incident ID with
// every answer, also for the same error.
func ibmErrorWithIncident(op kubernetesservice.Operation, status int, code, incident string) error {
	return &kubernetesservice.APIError{Operation: op, StatusCode: status, Code: code, IncidentID: incident}
}

func TestControlPlaneConditionsIgnoreTheIncident(t *testing.T) {
	testCases := []struct {
		name  string
		setup func(objs *cpObjects)
		// expect sets the IBM answers of one reconcile; incident is that answer's incident ID.
		expect        func(f *cpFixture, incident string)
		wantCondition string
		wantReason    string
	}{
		{
			name:  "403_on_GetCluster",
			setup: func(objs *cpObjects) { objs.cp.Status.ClusterID = cpIKSClusterID },
			expect: func(f *cpFixture, incident string) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").
					Return(nil, ibmErrorWithIncident(kubernetesservice.OpGetCluster, http.StatusForbidden, "", incident))
			},
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
		},
		{
			name:  "400_on_GetCluster",
			setup: func(objs *cpObjects) { objs.cp.Status.ClusterID = cpIKSClusterID },
			expect: func(f *cpFixture, incident string) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").
					Return(nil, ibmErrorWithIncident(kubernetesservice.OpGetCluster, http.StatusBadRequest, "E1234", incident))
			},
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newCPObjects()
			tc.setup(objs)
			f := newCPFixture(t, objs)
			tc.expect(f, "incident-a")
			_, err := f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			first := conditions.Get(f.controlPlane(), tc.wantCondition)
			g.Expect(first).NotTo(BeNil())
			g.Expect(first.Reason).To(Equal(tc.wantReason))
			g.Expect(first.Message).NotTo(ContainSubstring("incident-a"), "the condition never carries IBM's incident ID")
			patches := len(f.patches())

			// IBM answers the same error with a new incident ID: status does not change, so the controller's own watch
			// does not re-trigger the reconcile (condition messages carry IBM's error code, never its incident ID).
			f.clock.SetTime(cpTestStart.Add(time.Minute))
			tc.expect(f, "incident-b")
			_, err = f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.patches()).To(HaveLen(patches), "no status patch for the same answer with another incident ID")
			g.Expect(conditions.Get(f.controlPlane(), tc.wantCondition).Message).To(Equal(first.Message))
		})
	}
}

// The envtest tests below run Reconcile against the envtest API server with the CRDs and the managed webhooks, for the
// semantics a fake client does not have: the finalizer only after the owner, deletion of an unowned object, pause,
// and the CRD validation of every status write.

// newEnvtestControlPlaneReconciler returns a reconciler on the envtest API server whose IBM seams fail the test when
// called.
func newEnvtestControlPlaneReconciler(t *testing.T) *IBMCloudManagedControlPlaneReconciler {
	t.Helper()
	c := uncachedClient(t)
	versions, err := managedscope.NewVersionsCache(clocktesting.NewFakePassiveClock(cpTestStart), time.Hour)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return &IBMCloudManagedControlPlaneReconciler{
		Client: c, APIReader: c, Recorder: record.NewFakeRecorder(100),
		Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t}, Prober: failIfCalledProber{t: t},
		VersionsCache: versions, Clock: clocktesting.NewFakePassiveClock(cpTestStart),
	}
}

// createEnvtestControlPlane creates an IKS control plane named name; when cluster is not nil it is owned by it, as the
// Cluster controller would set it.
func createEnvtestControlPlane(t *testing.T, namespace, name string, cluster *clusterv1.Cluster) *controlplanev1.IBMCloudManagedControlPlane {
	t.Helper()
	cp := &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution:        controlplanev1.DistributionKubernetes,
			DistributionVersion: "1.36",
			IBMCloudClusterName: name,
		},
	}
	if cluster != nil {
		cp.Labels = map[string]string{clusterv1.ClusterNameLabel: cluster.Name}
		cp.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: cluster.Name, UID: cluster.UID,
		}}
	}
	NewWithT(t).Expect(testEnv.Create(ctx, cp)).To(Succeed())
	return cp
}

// getEnvtestControlPlane reads the control plane from the API server, uncached; nil when it is gone.
func getEnvtestControlPlane(t *testing.T, key client.ObjectKey) *controlplanev1.IBMCloudManagedControlPlane {
	t.Helper()
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	err := testEnv.GetAPIReader().Get(ctx, key, cp)
	if apierrors.IsNotFound(err) {
		return nil
	}
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return cp
}

func TestControlPlaneEnvtestOwnerFinalizerPause(t *testing.T) {
	t.Run("an unowned control plane gets no finalizer and can be deleted", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "cp-unowned")
		cp := createEnvtestControlPlane(t, namespace, "dev", nil)
		key := client.ObjectKeyFromObject(cp)
		r := newEnvtestControlPlaneReconciler(t)

		result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		persisted := getEnvtestControlPlane(t, key)
		g.Expect(persisted.Finalizers).To(BeEmpty())
		g.Expect(persisted.ResourceVersion).To(Equal(cp.ResourceVersion), "nothing is written before the owner is known")
		g.Expect(testEnv.Delete(ctx, persisted)).To(Succeed())
		g.Eventually(func() *controlplanev1.IBMCloudManagedControlPlane { return getEnvtestControlPlane(t, key) },
			10*time.Second, 100*time.Millisecond).Should(BeNil(), "kubectl delete completes")
	})

	t.Run("the finalizer comes after the owner, then pause, then the flow", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "cp-owned")
		cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
		cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
		key := client.ObjectKeyFromObject(cp)
		r := newEnvtestControlPlaneReconciler(t)

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(getEnvtestControlPlane(t, key).Finalizers).To(Equal([]string{controlplanev1.IBMCloudManagedControlPlaneFinalizer}))
		g.Expect(conditions.Get(getEnvtestControlPlane(t, key), clusterv1.PausedCondition)).To(BeNil(), "nothing else in the finalizer reconcile")

		// The Cluster's infrastructure is not provisioned: the flow waits, and every status write passes the CRD's
		// validation (no status string is written as "").
		g.Eventually(func(g Gomega) {
			result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(BeNumerically(">", 0))
			persisted := getEnvtestControlPlane(t, key)
			g.Expect(conditions.IsFalse(persisted, clusterv1.PausedCondition)).To(BeTrue())
			expectCondition(g, persisted, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason)
			expectCondition(g, persisted, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneNotDeletingReason)
			g.Expect(persisted.Status.ExternalManagedControlPlane).To(Equal(ptr.To(true)))
			g.Expect(conditions.Get(persisted, controlplanev1.IBMCloudManagedControlPlaneReadyCondition)).NotTo(BeNil())
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		// No IBM cluster can exist yet, so the webhook allows the deletion and the finalizer goes without an IBM call.
		g.Expect(testEnv.Delete(ctx, getEnvtestControlPlane(t, key))).To(Succeed())
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
		g.Eventually(func() *controlplanev1.IBMCloudManagedControlPlane { return getEnvtestControlPlane(t, key) },
			10*time.Second, 100*time.Millisecond).Should(BeNil())
	})

	t.Run("a paused Cluster makes no IBM call against the API server", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "cp-paused")
		cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
		cluster.Spec.Paused = ptr.To(true)
		g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
		cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
		key := client.ObjectKeyFromObject(cp)
		r := newEnvtestControlPlaneReconciler(t)

		g.Eventually(func(g Gomega) {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(conditions.IsTrue(getEnvtestControlPlane(t, key), clusterv1.PausedCondition)).To(BeTrue())
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
		persisted := getEnvtestControlPlane(t, key)
		g.Expect(conditions.Get(persisted, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)).To(BeNil())
		g.Expect(persisted.Status.ExternalManagedControlPlane).To(BeNil(), "a paused object is not reconciled")
	})
}

// The manager tests below run the control plane controller behind a real manager, with its watches and a real clock,
// so that the controller's own status patches re-trigger it as in production. They count the IBM calls of a few
// seconds: a hot loop shows up as hundreds. A refusal must hold the next create request until its recheck, and a
// condition message must not change with IBM's incident ID, which IBM changes on every answer.

// noHotLoopWindow is how long the manager tests watch the IBM call counts once the answer is on record.
const noHotLoopWindow = 3 * time.Second

// newNamespacedTestManager returns a test manager whose cache, and so whose watches, see only namespace: the objects
// other tests leave in the shared API server never reach this test's reconciler and its counting IBM mock, and tests
// in their own namespaces can run in parallel.
func newNamespacedTestManager(t *testing.T, namespace string) manager.Manager {
	t.Helper()
	mgr, err := ctrl.NewManager(testEnv.Config, ctrl.Options{
		Scheme:     clientgoscheme.Scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
		Cache:      cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
	})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return mgr
}

// startControlPlaneManager starts the control plane controller for namespace with its real watches (SetupWithManager)
// and a real clock; kube answers its IBM calls through the returned stub ClientBuilder. The manager stops when the
// test ends, before the cleanups registered earlier run.
func startControlPlaneManager(t *testing.T, namespace string, kube kubernetesservice.KubernetesService) *stubClientBuilder {
	t.Helper()
	mgr := newNamespacedTestManager(t, namespace)
	builder := &stubClientBuilder{kubernetes: kube}
	r := newEnvtestControlPlaneReconciler(t)
	r.Client, r.APIReader = mgr.GetClient(), mgr.GetAPIReader()
	r.Resolver, r.ClientBuilder = &recordingResolver{}, builder
	r.Clock = clock.RealClock{}
	NewWithT(t).Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
	startTestManager(t, mgr)
	return builder
}

// pauseTestCluster pauses cluster, so that a manager of a later test that watches every namespace leaves this test's
// objects alone: pause stops the managed controllers before any IBM call.
func pauseTestCluster(t *testing.T, cluster *clusterv1.Cluster) {
	t.Helper()
	NewWithT(t).Eventually(func() error {
		current := &clusterv1.Cluster{}
		if err := testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		current.Spec.Paused = ptr.To(true)
		return testEnv.Update(ctx, current)
	}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
}

// pauseAtCleanup pauses cluster when the test ends (pauseTestCluster); registered before the manager starts, it runs
// after the manager has stopped. key is unused; it gives every preparation of the incident test the same signature.
func pauseAtCleanup(t *testing.T, cluster *clusterv1.Cluster, _ client.ObjectKey) {
	t.Helper()
	t.Cleanup(func() { pauseTestCluster(t, cluster) })
}

func TestControlPlaneEnvtestIncidentSettles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
		// prepare readies the bound control plane key of cluster before the manager starts.
		prepare       func(t *testing.T, cluster *clusterv1.Cluster, key client.ObjectKey)
		wantCondition string
		wantReason    string
	}{
		{name: "403_on_GetCluster", status: http.StatusForbidden, prepare: pauseAtCleanup,
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason},
		{name: "400_on_GetCluster", status: http.StatusBadRequest, code: "E1234", prepare: pauseAtCleanup,
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason},
	} {
		t.Run(tc.name+"_manager_settles", func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			namespace := newTestNamespace(t, "cp-incident")
			cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
			cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
			g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
			createTestInfra(t, namespace, "dev", cluster)
			cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
			key := client.ObjectKeyFromObject(cp)
			cp.Finalizers = []string{controlplanev1.IBMCloudManagedControlPlaneFinalizer}
			g.Expect(testEnv.Update(ctx, cp)).To(Succeed())
			cp.Status.ClusterID = cpIKSClusterID
			g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())
			tc.prepare(t, cluster, key)

			var gets atomic.Int64
			kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
			kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").DoAndReturn(
				func(context.Context, string, string) (*kubernetesservice.Cluster, error) {
					n := gets.Add(1)
					return nil, ibmErrorWithIncident(kubernetesservice.OpGetCluster, tc.status, tc.code, fmt.Sprintf("incident-%d", n))
				}).AnyTimes()
			startControlPlaneManager(t, namespace, kube)

			g.Eventually(func(g Gomega) {
				condition := conditions.Get(getEnvtestControlPlane(t, key), tc.wantCondition)
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Reason).To(Equal(tc.wantReason))
			}, 30*time.Second, 100*time.Millisecond).Should(Succeed())

			// The answer's reconcile writes the condition, and the reconcile that write triggers finds nothing to change,
			// however many incident IDs IBM hands out. The control plane exists before the manager starts, so the
			// start-up events of the Cluster and the control plane may add a reconcile or two; a hot loop makes hundreds
			// of calls a second. The count must settle low and then stay put.
			settled := int64(-1)
			g.Eventually(func() bool {
				n := gets.Load()
				stable := n == settled
				settled = n
				return stable
			}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "GetCluster calls keep growing")
			g.Expect(settled).To(BeNumerically("<=", 4), "GetCluster calls before the status settled")
			g.Consistently(gets.Load, noHotLoopWindow, 100*time.Millisecond).Should(Equal(settled))
		})
	}
}

func TestControlPlaneReconcilerSetupWithManager(t *testing.T) {
	t.Run("missing dependencies are named", func(t *testing.T) {
		g := NewWithT(t)
		err := (&IBMCloudManagedControlPlaneReconciler{}).SetupWithManager(ctx, newTestManager(t))
		g.Expect(err).To(MatchError(ContainSubstring("Client is nil")))
		g.Expect(err).To(MatchError(ContainSubstring("Resolver is nil; pass credentials.ControllerOnly{}")))
	})

	t.Run("the watches reconcile a new control plane", func(t *testing.T) {
		g := NewWithT(t)
		mgr := newTestManager(t)
		r := newEnvtestControlPlaneReconciler(t)
		r.Client, r.APIReader = mgr.GetClient(), mgr.GetAPIReader()
		g.Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
		startTestManager(t, mgr)

		namespace := newTestNamespace(t, "cp-watch")
		cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
		cp := createEnvtestControlPlane(t, namespace, "dev", cluster)

		g.Eventually(func(g Gomega) {
			persisted := getEnvtestControlPlane(t, client.ObjectKeyFromObject(cp))
			g.Expect(persisted.Finalizers).To(ContainElement(controlplanev1.IBMCloudManagedControlPlaneFinalizer))
			expectCondition(g, persisted, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason)
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		// A Cluster that pauses reaches the control plane through the Cluster watch.
		cluster.Spec.Paused = ptr.To(true)
		g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
		g.Eventually(func(g Gomega) {
			g.Expect(conditions.IsTrue(getEnvtestControlPlane(t, client.ObjectKeyFromObject(cp)), clusterv1.PausedCondition)).To(BeTrue())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		// Unpause and delete: no IBM cluster can exist, so the finalizer goes.
		cluster.Spec.Paused = nil
		g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
		g.Eventually(func(g Gomega) {
			g.Expect(conditions.IsFalse(getEnvtestControlPlane(t, client.ObjectKeyFromObject(cp)), clusterv1.PausedCondition)).To(BeTrue())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
		g.Expect(testEnv.Delete(ctx, getEnvtestControlPlane(t, client.ObjectKeyFromObject(cp)))).To(Succeed())
		g.Eventually(func() *controlplanev1.IBMCloudManagedControlPlane {
			return getEnvtestControlPlane(t, client.ObjectKeyFromObject(cp))
		}, 30*time.Second, 200*time.Millisecond).Should(BeNil())
	})
}
