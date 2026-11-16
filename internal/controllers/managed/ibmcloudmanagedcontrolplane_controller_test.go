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
	"os"
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
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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

// failIfCalledProber is a workloadprobe.Prober for control plane tests whose steps must not probe a kubeconfig: before the
// master is ready, before the endpoint is recorded, and on every error path.
type failIfCalledProber struct {
	t *testing.T
}

// Probe implements workloadprobe.Prober; it fails the test.
func (p failIfCalledProber) Probe(context.Context, []byte) (workloadprobe.Result, error) {
	p.t.Errorf("Prober.Probe called, but this control plane step must not probe a kubeconfig")
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
		versions, err := managedscope.NewVersionsCache(clocktesting.NewFakePassiveClock(cpTestStart), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		params := &managedscope.ScopeParams{
			Client: fake.NewClientBuilder().Build(), APIReader: fake.NewClientBuilder().Build(), Recorder: record.NewFakeRecorder(1),
			Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t}, Clock: clocktesting.NewFakePassiveClock(cpTestStart),
			Prober: failIfCalledProber{t: t}, VersionsCache: versions,
		}
		persist := func(context.Context) error { t.Error("the constructor must not persist"); return nil }
		scope, err := managedscope.NewControlPlaneScope(params, &clusterv1.Cluster{}, &controlplanev1.IBMCloudManagedControlPlane{}, persist)
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
		objs.cp.Status.ClusterID = cpIKSClusterID
		objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		objs.cluster.Spec.InfrastructureRef.Name = "gone"
		f := newNoCallFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp).NotTo(BeNil(), "the finalizer stays while an IBM cluster may exist")
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason)
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneAvailableDeletingReason)
	})

	t.Run("a deleting control plane without an IBM cluster or IBMCloudManagedCluster loses its finalizer", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		objs.cluster.Spec.InfrastructureRef.Name = "gone"
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

	// The pool created with the cluster is chosen by spec.creation, so re-marking a pool must reach a control plane that
	// waits for its initial pool (WaitingForInitialWorkerPool, or InvalidConfiguration with its 15-minute
	// terminalRecheck) at once. The watch has no predicate and the map reads only the cluster-name label, so the handler
	// the watch uses enqueues every spec.creation change and the deletion of a marked pool, with the same filter as any
	// pool event: only a control plane without status.clusterID or one being deleted is woken.
	creationEdits := []struct {
		name     string
		from, to infrav1.PoolCreation
	}{
		{name: "is marked WithCluster", from: "", to: infrav1.PoolCreationWithCluster},
		{name: "moves from WithCluster to AfterCluster", from: infrav1.PoolCreationWithCluster, to: infrav1.PoolCreationAfterCluster},
		{name: "loses its WithCluster marker", from: infrav1.PoolCreationWithCluster, to: ""},
	}
	for _, state := range []struct {
		name  string
		setup func(objs *cpObjects)
		wake  bool
	}{
		{name: "waits for its initial pool", setup: func(*cpObjects) {}, wake: true},
		{name: "is bound", setup: func(objs *cpObjects) { objs.cp.Status.ClusterID = cpIKSClusterID }},
		{name: "is bound and being deleted", wake: true, setup: func(objs *cpObjects) {
			objs.cp.Status.ClusterID = cpIKSClusterID
			objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
		}},
	} {
		objs := newCPObjects()
		state.setup(objs)
		f := newCPFixture(t, objs)
		enqueue := handler.EnqueueRequestsFromMapFunc(f.r.machinePoolToControlPlane)
		verb := "wakes"
		if !state.wake {
			verb = "does not wake"
		}
		expectWoken := func(g Gomega, queue *controllertest.Queue) {
			if state.wake {
				g.Expect(queuedRequests(queue)).To(Equal(want))
				return
			}
			g.Expect(queuedRequests(queue)).To(BeEmpty())
		}
		for _, edit := range creationEdits {
			t.Run(fmt.Sprintf("a pool that %s %s a control plane that %s", edit.name, verb, state.name), func(t *testing.T) {
				g := NewWithT(t)
				before := poolOf(cpTestName)
				before.Spec.Creation, before.ResourceVersion = edit.from, "1"
				after := before.DeepCopy()
				after.Spec.Creation, after.ResourceVersion = edit.to, "2"
				queue := newTestQueue(t)
				enqueue.Update(ctx, event.UpdateEvent{ObjectOld: before, ObjectNew: after}, queue)
				expectWoken(g, queue)
			})
		}
		t.Run(fmt.Sprintf("a deleted WithCluster pool %s a control plane that %s", verb, state.name), func(t *testing.T) {
			g := NewWithT(t)
			pool := poolOf(cpTestName)
			pool.Spec.Creation = infrav1.PoolCreationWithCluster
			queue := newTestQueue(t)
			enqueue.Delete(ctx, event.DeleteEvent{Object: pool}, queue)
			expectWoken(g, queue)
		})
	}
}

// newTestQueue returns a synchronous work queue for calling an event handler directly; it is shut down when the test
// ends. t is the test.
func newTestQueue(t *testing.T) *controllertest.Queue {
	t.Helper()
	queue := &controllertest.Queue{TypedInterface: workqueue.NewTyped[reconcile.Request]()}
	t.Cleanup(queue.ShutDown)
	return queue
}

// queuedRequests drains queue and returns the requests in the order they were added. queue is the test queue.
func queuedRequests(queue *controllertest.Queue) []reconcile.Request {
	var requests []reconcile.Request
	for queue.Len() > 0 {
		request, _ := queue.Get()
		queue.Done(request)
		requests = append(requests, request)
	}
	return requests
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
		{
			name:  "403_on_ListVersions",
			setup: withInitialPool,
			expect: func(f *cpFixture, incident string) {
				gomock.InOrder(f.expectLookupByName(nil), f.kube.EXPECT().ListVersions(gomock.Any()).
					Return(nil, ibmErrorWithIncident(kubernetesservice.OpListVersions, http.StatusForbidden, "", incident)))
			},
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
		},
		{
			name: "403_on_GetCluster_while_deleting",
			setup: func(objs *cpObjects) {
				objs.cp.Status.ClusterID = cpIKSClusterID
				objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart.Add(-time.Minute)))
			},
			expect: func(f *cpFixture, incident string) {
				f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").
					Return(nil, ibmErrorWithIncident(kubernetesservice.OpGetCluster, http.StatusForbidden, "", incident))
			},
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason,
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
		{name: "403_on_GetCluster_while_deleting", status: http.StatusForbidden, prepare: deleteEnvtestControlPlaneWithItsCluster,
			wantCondition: controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
			wantReason:    controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason},
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

// deleteEnvtestControlPlaneWithItsCluster deletes the control plane key as a Cluster deletion would: the Cluster is
// deleted first, held by a test finalizer, which the webhook requires for a bound control plane (it rejects deleting
// one on its own while its Cluster lives). Cleanups, registered before the manager starts, release both objects once
// the manager has stopped.
func deleteEnvtestControlPlaneWithItsCluster(t *testing.T, cluster *clusterv1.Cluster, key client.ObjectKey) {
	t.Helper()
	g := NewWithT(t)
	const holdCluster = "test.cluster.x-k8s.io/hold"
	cluster.Finalizers = []string{holdCluster}
	g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
	g.Expect(testEnv.Delete(ctx, cluster)).To(Succeed())
	g.Eventually(func() error { return testEnv.Delete(ctx, getEnvtestControlPlane(t, key)) }, 10*time.Second, 100*time.Millisecond).Should(Succeed())
	t.Cleanup(func() {
		release := func(obj client.Object) {
			g.Eventually(func() error {
				if err := testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
					return client.IgnoreNotFound(err)
				}
				obj.SetFinalizers(nil)
				return testEnv.Update(ctx, obj)
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		}
		release(&controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}})
		release(&clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: cluster.Name}})
	})
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

// Create and observe: the create intent before the POST, a lost response, create refusals and warnings, and the
// readiness of the master.

// cpTestPoolName names the initial pool and its MachinePool in the create tests.
const cpTestPoolName = "dev-default"

// withInitialPool adds the initial IBMCloudManagedMachinePool of the templates (poolName default, spec.creation
// WithCluster, two zones) and its MachinePool (replicas 2) to objs, linked by the owner reference CAPI sets. The
// MachinePool is objs.extra[0] and the pool objs.extra[1] when objs had no extra objects before.
func withInitialPool(objs *cpObjects) {
	withPool(objs, cpTestPoolName, "default", infrav1.PoolCreationWithCluster)
}

// withPool adds an IBMCloudManagedMachinePool of the test Cluster (two zones) and its MachinePool (replicas 2) to objs,
// linked by the owner reference CAPI sets; the MachinePool comes first.
//
// Parameters:
//   - objs are the objects of the test.
//   - name names both objects.
//   - poolName is the pool's spec.poolName.
//   - creation is the pool's spec.creation ("" leaves it omitted).
func withPool(objs *cpObjects, name, poolName string, creation infrav1.PoolCreation) {
	mp := &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: name, UID: types.UID("machinepool-uid-" + name),
			Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
		Spec: clusterv1.MachinePoolSpec{ClusterName: cpTestName, Replicas: ptr.To[int32](2)},
	}
	pool := &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cpTestNamespace, Name: name,
			Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool", Name: mp.Name, UID: mp.UID,
			}},
		},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName: poolName,
			Creation: creation,
			Flavor:   "bx2.4x16",
			Zones:    []infrav1.WorkerPoolZone{{Name: "us-south-1"}, {Name: "us-south-2"}},
		},
	}
	objs.extra = append(objs.extra, mp, pool)
}

// setPoolCreation changes spec.creation of the stored IBMCloudManagedMachinePool name, as a user's edit between two
// reconciles would.
//
// Parameters:
//   - name is the pool's object name.
//   - creation is the new spec.creation ("" removes it).
func (f *cpFixture) setPoolCreation(name string, creation infrav1.PoolCreation) {
	f.t.Helper()
	g := NewWithT(f.t)
	pool := &infrav1.IBMCloudManagedMachinePool{}
	g.Expect(f.client.Get(ctx, types.NamespacedName{Namespace: cpTestNamespace, Name: name}, pool)).To(Succeed())
	pool.Spec.Creation = creation
	g.Expect(f.client.Update(ctx, pool)).To(Succeed())
}

// cpOfferedVersions are the versions IBM offers in the create tests.
func cpOfferedVersions() *kubernetesservice.Versions {
	return &kubernetesservice.Versions{
		Kubernetes: []kubernetesservice.ReleaseVersion{{Major: 1, Minor: 35, Patch: 9}, {Major: 1, Minor: 36, Patch: 5, Default: true}},
		OpenShift:  []kubernetesservice.ReleaseVersion{{Major: 4, Minor: 21, Patch: 35}},
	}
}

// expectLookupByName expects one GetCluster by the effective name in the resource group, answering c (nil: 404).
func (f *cpFixture) expectLookupByName(c *kubernetesservice.Cluster) *gomock.Call {
	call := f.kube.EXPECT().GetCluster(gomock.Any(), cpTestName, testResourceGroupID)
	if c == nil {
		return call.Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
	}
	return call.Return(c, nil)
}

// expectCreate expects one CreateCluster; before it answers, it checks that the create intent is already on record in
// the API server: the intent is persisted before the POST, so that a lost response can be told from a foreign cluster.
func (f *cpFixture) expectCreate(result *kubernetesservice.CreateClusterResult, err error) *gomock.Call {
	return f.kube.EXPECT().CreateCluster(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req kubernetesservice.CreateClusterRequest) (*kubernetesservice.CreateClusterResult, error) {
			persisted := f.controlPlane()
			if persisted.Status.CreateIntent.ClusterName != req.Name || persisted.Status.CreateIntent.InFlightAttemptTime.IsZero() {
				f.t.Errorf("CreateCluster was called before the create intent was persisted: %+v", persisted.Status.CreateIntent)
			}
			return result, err
		})
}

// createTimeoutErr is a create request whose answer never arrived: its outcome is unknown.
var createTimeoutErr = context.DeadlineExceeded

// liveNonCriticalResult returns the result of the createCluster fixture with non-critical errors, recorded on IBM Cloud
// (IKS and ROKS creates can succeed with them), read from the scrubbed copy of kubernetesservice/testdata.
func liveNonCriticalResult(t *testing.T) *kubernetesservice.CreateClusterResult {
	t.Helper()
	g := NewWithT(t)
	data, err := os.ReadFile("../../../pkg/cloud/services/kubernetesservice/testdata/createCluster-201-non-critical-errors.json")
	g.Expect(err).NotTo(HaveOccurred())
	var fixture struct {
		ClusterID         string `json:"clusterID"`
		NonCriticalErrors struct {
			IncidentID string `json:"incidentID"`
			Items      []struct {
				Code        string `json:"code"`
				Type        string `json:"type"`
				Description string `json:"description"`
				RecoveryCLI string `json:"recoveryCLI"`
			} `json:"items"`
		} `json:"non_critical_errors"`
	}
	g.Expect(json.Unmarshal(data, &fixture)).To(Succeed())
	result := &kubernetesservice.CreateClusterResult{ClusterID: fixture.ClusterID, IncidentID: fixture.NonCriticalErrors.IncidentID}
	for _, item := range fixture.NonCriticalErrors.Items {
		result.NonCriticalErrors = append(result.NonCriticalErrors, kubernetesservice.IBMMessage{
			Code: item.Code, Type: item.Type, Description: item.Description, RecoveryCLI: item.RecoveryCLI,
		})
	}
	return result
}

func TestControlPlaneCreate(t *testing.T) {
	t.Run("the identity holder is the IBMCloudManagedCluster on create and the intent is persisted before the POST", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		var sent kubernetesservice.CreateClusterRequest
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(&kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil).
				Do(func(_ context.Context, req kubernetesservice.CreateClusterRequest) { sent = req }),
		)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		g.Expect(sent.Name).To(Equal(cpTestName))
		g.Expect(sent.ResourceGroupID).To(Equal(testResourceGroupID))
		g.Expect(sent.KubeVersion).To(Equal("1.36"))
		g.Expect(sent.WorkerPool.WorkerCountPerZone).To(Equal(int64(1)), "2 replicas over 2 zones")
		g.Expect(sent.WorkerPool.Zones).To(Equal([]kubernetesservice.ZoneSubnet{{Zone: "us-south-1", SubnetID: "sn-1"}, {Zone: "us-south-2", SubnetID: "sn-2"}}))
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID))
		g.Expect(cp.Status.CreateIntent.InFlightAttemptTime.Time).To(BeTemporally("==", cpTestStart), "the intent stays until row 1 observes the cluster")
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
		g.Expect(f.events()).To(Equal([]string{"Normal SuccessfulCreateIBMCluster Created IBM cluster dev (ID iksclusterid00000001)"}))
		expectHolderIsInfraCluster(g, f)
		for _, body := range f.patches() {
			g.Expect(emptyStatusStrings(t, body)).To(BeEmpty(), "no status patch writes an empty string: %s", body)
		}

		// The next reconcile observes the cluster by ID (row 1), which clears the intent.
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("deploying"), nil)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane().Status.CreateIntent).To(BeZero())
	})

	t.Run("non-critical create errors are warnings, never failures", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		live := liveNonCriticalResult(t)
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(live, nil),
		)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(Equal(live.ClusterID))
		g.Expect(cp.Status.NonCriticalCreateErrorCodes).To(Equal([]string{"E49df"}))
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
		g.Expect(condition.Message).To(HaveSuffix("IBM created the cluster with non-critical errors E49df (see the IBMCreateWarnings events)"))
		g.Expect(condition.Message).NotTo(ContainSubstring("icr.io"), "IBM's text stays in the event")
		events := f.events()
		g.Expect(events).To(HaveLen(1 + len(live.NonCriticalErrors)))
		g.Expect(events[1]).To(HavePrefix("Warning IBMCreateWarnings IBM created cluster dev (ID " + live.ClusterID +
			") with the non-critical error E49df (Provisioning): Your cluster cannot pull images"))

		// Once the cluster exists, Provisioned is True and still names the codes.
		observed := cpCluster("normal")
		observed.ID = live.ClusterID
		f.kube.EXPECT().GetCluster(gomock.Any(), live.ClusterID, "").Return(observed, nil)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		condition = expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
		g.Expect(condition.Message).To(Equal("IBM created the cluster with non-critical errors E49df (see the IBMCreateWarnings events)"))
	})

	t.Run("a lost status patch after a successful POST is adopted by name on the next reconcile", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		f.failStatusPatch = func(body string) bool { return strings.Contains(body, `"clusterID"`) }
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(&kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil).Times(1),
		)

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("injected status patch failure")))
		persisted := f.controlPlane()
		g.Expect(persisted.Status.ClusterID).To(BeEmpty(), "the cluster ID was lost with the patch")
		g.Expect(persisted.Status.CreateIntent.InFlightAttemptTime.IsZero()).To(BeFalse(), "the intent was on record before the POST")

		// Next reconcile: the lookup by name finds the cluster our lost response created, inside the window (row 6).
		f.failStatusPatch = nil
		f.clock.SetTime(cpTestStart.Add(time.Minute))
		f.expectLookupByName(cpCluster("deploying"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane().Status.ClusterID).To(Equal(cpIKSClusterID))
	})

	t.Run("a failed intent patch sends no POST", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		f.failStatusPatch = func(body string) bool { return strings.Contains(body, `"createIntent"`) }
		f.expectLookupByName(nil)
		f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil)
		// No CreateCluster expectation: gomock fails the test if it is called.

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("recording the create intent")))
		g.Expect(f.controlPlane().Status.CreateIntent).To(BeZero(), "no attempt is recorded that was never sent")
	})

	createFailureCases := []struct {
		name string
		// err is CreateCluster's answer.
		err error
		// relookup is the answer of the lookup after an E0007 (nil: 404); unused otherwise.
		relookup   func() *kubernetesservice.Cluster
		wantReason string
		// wantIntent is the persisted intent after the reconcile.
		wantIntent  controlplanev1.CreateIntent
		wantErr     bool
		wantRequeue time.Duration
		wantMessage string
	}{
		{
			name: "a 400 P4102 clears the intent", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusBadRequest, "P4102"),
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
			wantRequeue: 15 * time.Minute,
			wantMessage: "IBM Cloud refused to create cluster dev because a subnet of the request does not exist (IBM code P4102); " +
				"check the subnets of IBMCloudManagedCluster dev (status.zones)",
		},
		{
			name: "a 400 E7278 clears the intent", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusBadRequest, "E7278"),
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
			wantRequeue: 15 * time.Minute,
			wantMessage: "IBM Cloud refused to create cluster dev because an OpenShift cluster on VPC needs a standard Cloud Object " +
				"Storage instance for its registry (IBM code E7278); set spec.openShift.imageRegistry.cos.instanceCRN",
		},
		{
			name: "a 500 moves the attempt into the unknown-outcome bounds", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusInternalServerError, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			wantIntent: controlplanev1.CreateIntent{ClusterName: cpTestName, FirstUnknownOutcomeTime: metav1.NewTime(cpTestStart),
				LastUnknownOutcomeTime: metav1.NewTime(cpTestStart)},
			wantErr: true,
		},
		{
			name: "a 429 makes the outcome unknown and backs off", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusTooManyRequests, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			wantIntent: controlplanev1.CreateIntent{ClusterName: cpTestName, FirstUnknownOutcomeTime: metav1.NewTime(cpTestStart),
				LastUnknownOutcomeTime: metav1.NewTime(cpTestStart)},
			wantRequeue: 31 * time.Second,
		},
		{
			name: "E0007 with a lookup answered with 404 is a name conflict", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusConflict, "E0007"),
			relookup:    func() *kubernetesservice.Cluster { return nil },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "E0007 with a cluster created 30s earlier is a name conflict", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusConflict, "E0007"),
			relookup: func() *kubernetesservice.Cluster {
				c := cpCluster("deploying")
				c.CreatedDate = cpTestStart.Add(-30 * time.Second).Format("2006-01-02T15:04:05-0700")
				return c
			},
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "A0656 is permission denied with the remedy", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusUnauthorized, "A0656"),
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
			wantRequeue: 15 * time.Minute,
			wantMessage: "IBM cannot create the infrastructure API key for region us-south and resource group " +
				"0123456789abcdef0123456789abcdef with the credentials of the CAPIBM controller identity (IBM code A0656); " +
				"a service ID needs the IAM Identity Service roles (Service ID creator, User API key creator, Viewer " +
				"and Operator), or an account administrator pre-sets the key with `ibmcloud ks api-key reset --region us-south`; " +
				"the IAM page of the managed Kubernetes book lists every role",
		},
		{
			name: "a create 403 names the stored infrastructure key and the identity", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusForbidden, ""),
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
			wantRequeue: 15 * time.Minute,
			wantMessage: "IBM Cloud refused to create cluster dev with HTTP 403: the infrastructure API key IBM " +
				"stores for region us-south and resource group 0123456789abcdef0123456789abcdef, or the credentials of the CAPIBM " +
				"controller identity, lack permissions; the credentials need the Administrator platform role and the Manager service " +
				"role of Kubernetes Service, and `ibmcloud ks api-key info --cluster <a cluster of that region and resource group>` " +
				"shows whose key IBM stores, which `ibmcloud ks api-key reset --region us-south` replaces; the IAM page of the " +
				"managed Kubernetes book lists every role",
		},
		{
			name: "a create 401 clears the intent and rebuilds the credentials", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusUnauthorized, ""),
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
			wantRequeue: 5 * time.Minute,
		},
	}
	for _, tc := range createFailureCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newCPObjects()
			withInitialPool(objs)
			f := newCPFixture(t, objs)
			calls := []any{
				f.expectLookupByName(nil),
				f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
				f.expectCreate(nil, tc.err).Times(1),
			}
			if tc.relookup != nil {
				calls = append(calls, f.expectLookupByName(tc.relookup()))
			}
			gomock.InOrder(calls...)

			result, err := f.reconcile()

			if tc.wantErr {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				expectRequeue(g, result, tc.wantRequeue)
			}
			cp := f.controlPlane()
			g.Expect(cpUTCIntent(cp.Status.CreateIntent)).To(Equal(tc.wantIntent))
			g.Expect(cp.Status.ClusterID).To(BeEmpty())
			condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
				metav1.ConditionFalse, tc.wantReason)
			if tc.wantMessage != "" {
				g.Expect(condition.Message).To(Equal(tc.wantMessage))
			}
			expectNoIdentityDetails(g, condition.Message)
			for _, body := range f.patches() {
				g.Expect(emptyStatusStrings(t, body)).To(BeEmpty(), "no status patch writes an empty string: %s", body)
			}
		})
	}

	t.Run("a timed-out attempt 1 is bound after attempt 2 gets E0007", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(nil, createTimeoutErr),
		)
		_, err := f.reconcile()
		g.Expect(err).To(HaveOccurred(), "attempt 1 timed out")

		// Attempt 2, after the read-after-create window of attempt 1: IBM still lists nothing (lag), the create answers
		// E0007, and the lookup then finds the cluster attempt 1 created. Row 6 binds it over attempt 1 only.
		f.clock.SetTime(cpTestStart.Add(6 * time.Minute))
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.expectCreate(nil, ibmError(kubernetesservice.OpCreateCluster, http.StatusConflict, "E0007")),
			f.expectLookupByName(cpCluster("deploying")),
		)
		_, err = f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID), "row 6: our own attempt 1 created it")
		g.Expect(cp.Status.CreateIntent.InFlightAttemptTime.IsZero()).To(BeTrue(), "attempt 2 created nothing and left the intent")
		g.Expect(cp.Status.CreateIntent.FirstUnknownOutcomeTime.Time).To(BeTemporally("==", cpTestStart))
	})

	t.Run("the WithCluster pool is sent under its own name whatever the other pools are called", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		// The marker selects the pool, not the name; a pool named default without it is created later.
		withPool(objs, "dev-default", "default", "")
		withPool(objs, "dev-gpu", "gpu", infrav1.PoolCreationAfterCluster)
		withPool(objs, "dev-workers-a", "workers-a", infrav1.PoolCreationWithCluster)
		objs.extra[4].(*clusterv1.MachinePool).Spec.Replicas = ptr.To[int32](4)
		f := newCPFixture(t, objs)
		var sent kubernetesservice.CreateClusterRequest
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(&kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil).
				Do(func(_ context.Context, req kubernetesservice.CreateClusterRequest) { sent = req }),
		)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(sent.WorkerPool.Name).To(Equal("workers-a"), "IBM honours the custom pool name")
		g.Expect(sent.WorkerPool.WorkerCountPerZone).To(Equal(int64(2)), "4 replicas of the workers-a MachinePool over 2 zones")
		g.Expect(f.controlPlane().Status.ClusterID).To(Equal(cpIKSClusterID))
	})

	// Two pools, neither marked (one omits spec.creation, one says AfterCluster), make the control plane wait without
	// any IBM call besides the binding lookup; once one of them is marked, the next reconcile creates the cluster with
	// that pool. That the pool watch brings this reconcile at once, not at the next clusterProvisioningPoll, is proven by
	// TestControlPlaneEnvtestCreationMarkerWakesTheControlPlane.
	t.Run("marking one of two waiting pools creates the cluster with it", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withPool(objs, cpTestPoolName, "default", "")
		withPool(objs, "dev-gpu", "gpu", infrav1.PoolCreationAfterCluster)
		f := newCPFixture(t, objs)
		f.expectLookupByName(nil) // no ListVersions and no CreateCluster expectation: gomock fails the test on either

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Status.CreateIntent).To(BeZero(), "nothing is recorded while no pool is marked")
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason)
		g.Expect(condition.Message).To(Equal(cpNoWithClusterPoolMessage))

		f.setPoolCreation("dev-gpu", infrav1.PoolCreationWithCluster)
		var sent kubernetesservice.CreateClusterRequest
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(&kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil).
				Do(func(_ context.Context, req kubernetesservice.CreateClusterRequest) { sent = req }),
		)
		_, err = f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(sent.WorkerPool.Name).To(Equal("gpu"), "the marked pool, not the one named default")
		g.Expect(f.controlPlane().Status.ClusterID).To(Equal(cpIKSClusterID))
	})

	// Pools b and a both marked stop the create with InvalidConfiguration naming them, sorted, and an unmarked pool is
	// not named; no IBM call besides the binding lookup. Once the marker is removed from b, the next reconcile creates the
	// cluster with a.
	t.Run("two marked pools stop the create until the marker is removed from one", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withPool(objs, "dev-b", "workers-b", infrav1.PoolCreationWithCluster)
		withPool(objs, "dev-other", "other", "")
		withPool(objs, "dev-a", "workers-a", infrav1.PoolCreationWithCluster)
		f := newCPFixture(t, objs)
		f.expectLookupByName(nil) // no ListVersions and no CreateCluster expectation: gomock fails the test on either

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Status.CreateIntent).To(BeZero(), "nothing is recorded while several pools are marked")
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason)
		g.Expect(condition.Message).To(Equal("IBMCloudManagedMachinePools dev-a, dev-b of Cluster dev all have spec.creation: " +
			"WithCluster; IBM creates the cluster with one initial pool, so keep exactly one of them WithCluster and set " +
			"spec.creation: AfterCluster on the others (or omit it)"))

		f.setPoolCreation("dev-b", "")
		var sent kubernetesservice.CreateClusterRequest
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(&kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil).
				Do(func(_ context.Context, req kubernetesservice.CreateClusterRequest) { sent = req }),
		)
		_, err = f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(sent.WorkerPool.Name).To(Equal("workers-a"))
		g.Expect(f.controlPlane().Status.ClusterID).To(Equal(cpIKSClusterID))
	})

	// The control plane reads spec.creation only until the IBM cluster exists. A second WithCluster pool added to a
	// bound control plane, and the marker removed from the initial pool, change no condition and send no create (no
	// InvalidConfiguration, no WaitingForInitialWorkerPool); the pool controller creates the new pool like an
	// AfterCluster pool (TestMachinePoolScopeCreatesThePool).
	t.Run("markers changed after the IBM cluster exists change nothing", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		objs.cp.Status.ClusterID = cpIKSClusterID
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		// Only the observation by ID, once per reconcile: gomock fails the test on a lookup, ListVersions or CreateCluster.
		// The master is still deploying, so the kubeconfig step does not run; the IBM cluster exists, which is all the
		// marker rule depends on.
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("deploying"), nil).Times(2)
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		before := f.controlPlane()
		f.events()

		added := &cpObjects{}
		withPool(added, "dev-gpu", "gpu", infrav1.PoolCreationWithCluster)
		for _, obj := range added.extra {
			g.Expect(f.client.Create(ctx, obj)).To(Succeed())
		}
		f.setPoolCreation(cpTestPoolName, "")
		_, err = f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		after := f.controlPlane()
		g.Expect(after.Status.Conditions).To(Equal(before.Status.Conditions))
		g.Expect(after.Status.ClusterID).To(Equal(cpIKSClusterID))
		g.Expect(after.Status.CreateIntent).To(BeZero())
		g.Expect(f.events()).To(BeEmpty())
	})

	// spec.creation only shapes the create request. The binding of a create whose response was lost rests on the
	// cluster name and the intent's time window (DecideBinding row 6, untouched), and pools are found by name, so
	// moving, removing or doubling the marker before the retry neither changes the binding nor sends a second create.
	// The pool side (each object touches only the IBM pool its poolName names) is
	// TestMachinePoolScopeMarkerMovedBetweenCreateAttempts.
	for _, tc := range []struct {
		name string
		edit func(f *cpFixture)
	}{
		{name: "marker moved to another pool", edit: func(f *cpFixture) {
			f.setPoolCreation(cpTestPoolName, infrav1.PoolCreationAfterCluster)
			other := &cpObjects{}
			withPool(other, "dev-workers", "workers", infrav1.PoolCreationWithCluster)
			for _, obj := range other.extra {
				NewWithT(f.t).Expect(f.client.Create(ctx, obj)).To(Succeed())
			}
		}},
		{name: "marker removed", edit: func(f *cpFixture) { f.setPoolCreation(cpTestPoolName, "") }},
		{name: "marker set on a second pool", edit: func(f *cpFixture) {
			other := &cpObjects{}
			withPool(other, "dev-workers", "workers", infrav1.PoolCreationWithCluster)
			for _, obj := range other.extra {
				NewWithT(f.t).Expect(f.client.Create(ctx, obj)).To(Succeed())
			}
		}},
	} {
		t.Run("a lost create is still bound after the "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newCPObjects()
			withInitialPool(objs)
			f := newCPFixture(t, objs)
			gomock.InOrder(
				f.expectLookupByName(nil),
				f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
				f.expectCreate(nil, createTimeoutErr),
			)
			_, err := f.reconcile()
			g.Expect(err).To(HaveOccurred(), "the create's response was lost")

			tc.edit(f)
			// The retry, inside the read-after-create window: the lookup finds the cluster the lost create made.
			f.clock.SetTime(cpTestStart.Add(time.Minute))
			f.expectLookupByName(cpCluster("deploying")) // no CreateCluster expectation: gomock fails the test if it is called
			_, err = f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			cp := f.controlPlane()
			g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID), "row 6 binds the cluster of the lost create")
			provisioned := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)
			g.Expect(provisioned).NotTo(BeNil())
			g.Expect(provisioned.Reason).NotTo(BeElementOf(
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason),
				"the marker is not read once a cluster is bound: %s", provisioned.Message)
		})
	}

	// The E0007 path: after attempt 1 (pool default) timed out, the marker moved to pool b (poolName workers), and
	// attempt 2, sent after attempt 1's read-after-create window while IBM still listed nothing, carries b. IBM refuses
	// it with E0007, the lookup then finds attempt 1's cluster, and row 6 binds it over attempt 1. IBM pool default
	// stays object a's, found by name (TestMachinePoolScopeMarkerMovedBetweenCreateAttempts).
	t.Run("marker moved, then attempt 2 gets E0007 and attempt 1 is bound", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(nil, createTimeoutErr),
		)
		_, err := f.reconcile()
		g.Expect(err).To(HaveOccurred(), "attempt 1 timed out")

		f.setPoolCreation(cpTestPoolName, infrav1.PoolCreationAfterCluster)
		moved := &cpObjects{}
		withPool(moved, "dev-workers", "workers", infrav1.PoolCreationWithCluster)
		for _, obj := range moved.extra {
			g.Expect(f.client.Create(ctx, obj)).To(Succeed())
		}
		f.clock.SetTime(cpTestStart.Add(6 * time.Minute))
		var sent kubernetesservice.CreateClusterRequest
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.expectCreate(nil, ibmError(kubernetesservice.OpCreateCluster, http.StatusConflict, "E0007")).
				Do(func(_ context.Context, req kubernetesservice.CreateClusterRequest) { sent = req }),
			f.expectLookupByName(cpCluster("deploying")),
		)
		_, err = f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(sent.WorkerPool.Name).To(Equal("workers"), "attempt 2 carried the newly marked pool")
		g.Expect(f.controlPlane().Status.ClusterID).To(Equal(cpIKSClusterID), "row 6: attempt 1 created it")
	})

	t.Run("the next create goes out only after the terminal recheck", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		gomock.InOrder(
			f.expectLookupByName(nil),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
			f.expectCreate(nil, ibmError(kubernetesservice.OpCreateCluster, http.StatusUnauthorized, "A0656")).Times(1),
		)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		g.Expect(f.builder.invalidated).To(BeEmpty(), "A0656 is a missing role, not rejected credentials")
		cp := f.controlPlane()
		g.Expect(cp.Status.CreateIntent).To(BeZero())
		g.Expect(cp.Annotations).To(HaveKeyWithValue(controlplanev1.CreateHoldAnnotation,
			cpTestStart.Add(managedscope.CreateRefusalHold).Format(time.RFC3339)))

		// The refusal's own status patch brings the control plane straight back (no predicate on For): that reconcile makes
		// no IBM call (gomock has no expectation left) and writes nothing, so nothing re-triggers it.
		patches := len(f.patches())
		result, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		g.Expect(f.patches()).To(HaveLen(patches), "a held reconcile writes no status")
	})
}

// cpRefusalCase is one definitive create refusal of the hold tests.
type cpRefusalCase struct {
	name string
	// err is CreateCluster's answer.
	err error
	// relookup is true for E0007: decideBinding looks the name up again, and IBM still finds nothing (the name is taken
	// by a cluster that the lookup in the resource group does not see).
	relookup   bool
	wantReason string
	wantHold   time.Duration
}

// cpRefusalCases are the definitive create refusals that hold the next create request, so that the reconcile the
// refusal's own status patch triggers sends none.
func cpRefusalCases() []cpRefusalCase {
	return []cpRefusalCase{
		{name: "A0656", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusUnauthorized, "A0656"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantHold: managedscope.CreateRefusalHold},
		{name: "400_P4102", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusBadRequest, "P4102"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason, wantHold: managedscope.CreateRefusalHold},
		{name: "403", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusForbidden, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantHold: managedscope.CreateRefusalHold},
		{name: "401", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusUnauthorized, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantHold: managedscope.CreateUnauthorizedHold},
		{name: "E0007_then_404", err: ibmError(kubernetesservice.OpCreateCluster, http.StatusConflict, "E0007"), relookup: true,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason, wantHold: managedscope.CreateRefusalHold},
	}
}

// expectRefusedCreate expects the IBM calls of one create that tc refuses: the lookup by name (404), the version list
// when withVersions (the VersionsCache keeps it afterwards), the create and, for E0007, the second lookup. creates
// counts the create requests.
func (f *cpFixture) expectRefusedCreate(tc cpRefusalCase, withVersions bool, creates *int) {
	calls := []any{f.expectLookupByName(nil)}
	if withVersions {
		calls = append(calls, f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil))
	}
	calls = append(calls, f.expectCreate(nil, tc.err).Do(func(context.Context, kubernetesservice.CreateClusterRequest) { *creates++ }))
	if tc.relookup {
		calls = append(calls, f.expectLookupByName(nil))
	}
	gomock.InOrder(calls...)
}

func TestControlPlaneCreateHold(t *testing.T) {
	for _, tc := range cpRefusalCases() {
		t.Run(tc.name+"_holds_the_next_create_until_its_recheck", func(t *testing.T) {
			g := NewWithT(t)
			objs := newCPObjects()
			withInitialPool(objs)
			f := newCPFixture(t, objs)
			creates := 0
			f.expectRefusedCreate(tc, true, &creates)

			_, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(creates).To(Equal(1))
			g.Expect(f.controlPlane().Annotations).To(HaveKeyWithValue(controlplanev1.CreateHoldAnnotation,
				cpTestStart.Add(tc.wantHold).Format(time.RFC3339)))
			patches, invalidated := len(f.patches()), len(f.builder.invalidated)

			// The own-patch reconcile, one a minute later and one just before the hold ends: no IBM call (gomock has
			// no expectation), no write, and for a 401 no second authenticator rebuild.
			for _, at := range []time.Duration{0, time.Minute, tc.wantHold - time.Second} {
				f.clock.SetTime(cpTestStart.Add(at))
				result, err := f.reconcile()
				g.Expect(err).NotTo(HaveOccurred())
				expectRequeue(g, result, 5*time.Minute)
			}
			g.Expect(f.patches()).To(HaveLen(patches))
			g.Expect(f.builder.invalidated).To(HaveLen(invalidated))
			expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
				metav1.ConditionFalse, tc.wantReason)

			// The hold is over: the next reconcile sends the create again, and IBM's second refusal holds it again.
			f.clock.SetTime(cpTestStart.Add(tc.wantHold))
			f.expectRefusedCreate(tc, false, &creates)
			_, err = f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(creates).To(Equal(2))
			g.Expect(f.controlPlane().Annotations).To(HaveKeyWithValue(controlplanev1.CreateHoldAnnotation,
				cpTestStart.Add(2*tc.wantHold).Format(time.RFC3339)))
		})
	}

	t.Run("a spec change sends the next create at once and a created cluster ends the hold", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs)
		creates := 0
		f.expectRefusedCreate(cpRefusalCases()[0], true, &creates)
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())

		// A spec change raises metadata.generation (the fake client does not, so the test does): the refusal was
		// observed for the old generation and holds nothing any more (Terminal: "spec edits re-trigger").
		cp := f.controlPlane()
		cp.Generation++
		g.Expect(f.client.Update(ctx, cp)).To(Succeed())
		f.clock.SetTime(cpTestStart.Add(time.Minute))
		gomock.InOrder(f.expectLookupByName(nil), f.expectCreate(&kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil))
		_, err = f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp = f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID))
		g.Expect(cp.Annotations).NotTo(HaveKey(controlplanev1.CreateHoldAnnotation), "the hold went with the intent's write")
	})

	t.Run("a hold annotation far in the future is not CAPIBM's and holds nothing", func(t *testing.T) {
		g := NewWithT(t)
		objs := newCPObjects()
		withInitialPool(objs)
		objs.cp.Annotations = map[string]string{controlplanev1.CreateHoldAnnotation: "2099-01-01T00:00:00Z"}
		objs.cp.Status.Conditions = append(objs.cp.Status.Conditions, metav1.Condition{
			Type: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, Status: metav1.ConditionFalse,
			Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, LastTransitionTime: metav1.NewTime(cpTestStart),
		})
		f := newCPFixture(t, objs)
		creates := 0
		f.expectRefusedCreate(cpRefusalCases()[0], true, &creates)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(creates).To(Equal(1))
	})
}

// TestCreateHoldsEndBeforeTheirRecheck pins the hold durations of the scope to the controller's cadence: each ends
// before the smallest jittered requeue of the outcome its refusal returns, so that recheck sends the next create
// (about 15 minutes after a Terminal refusal, about 5 after a 401).
func TestCreateHoldsEndBeforeTheirRecheck(t *testing.T) {
	g := NewWithT(t)
	terminal, err := requeueInterval(managedscope.IntervalTerminalRecheck)
	g.Expect(err).NotTo(HaveOccurred())
	steady, err := requeueInterval(managedscope.IntervalSteadyState)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(managedscope.CreateRefusalHold).To(BeNumerically("<", terminal*9/10))
	g.Expect(managedscope.CreateUnauthorizedHold).To(BeNumerically("<", steady*9/10))
	g.Expect(managedscope.CreateRefusalHold).To(BeNumerically(">", managedscope.CreateUnauthorizedHold))
}

// cpNoWithClusterPoolMessage is the IBMClusterProvisioned message of a Cluster without a WithCluster pool.
const cpNoWithClusterPoolMessage = "No IBMCloudManagedMachinePool of Cluster dev has spec.creation: WithCluster; mark exactly one " +
	"pool, it is created together with the IBM cluster"

// TestControlPlaneCreateWaitsAndGuards covers the checks before a create. No case reaches CreateCluster or records an
// intent: after the binding lookup (row 10) the initial pool selection and the request guards make no IBM call, and
// only the version check lists the offered versions; gomock fails a case on any IBM call it does not expect.
func TestControlPlaneCreateWaitsAndGuards(t *testing.T) {
	testCases := []struct {
		name        string
		setup       func(objs *cpObjects)
		expect      func(f *cpFixture)
		wantReason  string
		wantRequeue time.Duration
		wantMessage string
	}{
		{
			name:        "no pool waits for the initial pool",
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
			wantRequeue: time.Minute,
			wantMessage: cpNoWithClusterPoolMessage,
		},
		{
			// The name is no longer a convention; a pool named default needs the marker as well.
			name: "a pool named default without spec.creation waits",
			setup: func(objs *cpObjects) {
				withPool(objs, cpTestPoolName, "default", "")
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
			wantRequeue: time.Minute,
			wantMessage: cpNoWithClusterPoolMessage,
		},
		{
			// One pool omits spec.creation, the other says AfterCluster.
			name: "two pools, neither marked, wait for the initial pool",
			setup: func(objs *cpObjects) {
				withPool(objs, cpTestPoolName, "default", "")
				withPool(objs, "dev-gpu", "gpu", infrav1.PoolCreationAfterCluster)
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
			wantRequeue: time.Minute,
			wantMessage: cpNoWithClusterPoolMessage,
		},
		{
			name: "a deleting WithCluster pool is not the initial pool",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
				pool := objs.extra[1].(*infrav1.IBMCloudManagedMachinePool)
				pool.Finalizers = []string{infrav1.IBMCloudManagedMachinePoolFinalizer}
				pool.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart))
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
			wantRequeue: time.Minute,
			wantMessage: cpNoWithClusterPoolMessage,
		},
		{
			name: "a WithCluster pool of another Cluster does not count",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
				objs.extra[1].SetLabels(map[string]string{clusterv1.ClusterNameLabel: "other"})
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
			wantRequeue: time.Minute,
			wantMessage: cpNoWithClusterPoolMessage,
		},
		{
			name: "a pool CAPI has not linked yet waits",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
				objs.extra[1].SetOwnerReferences(nil)
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
			wantRequeue: time.Minute,
		},
		{
			name: "a configuration IBM would refuse is caught without an IBM mutation",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
				objs.extra[0].(*clusterv1.MachinePool).Spec.Replicas = ptr.To[int32](3)
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
			wantRequeue: 15 * time.Minute,
		},
		{
			name: "an unresolved zone of the IBMCloudManagedCluster waits",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
				objs.infra.Status.Zones = objs.infra.Status.Zones[:1]
			},
			expect:      func(f *cpFixture) { f.expectLookupByName(nil) },
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason,
			wantRequeue: time.Minute,
		},
		{
			name: "a version IBM does not offer is VersionNotAvailable",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
				objs.cp.Spec.DistributionVersion = "1.33"
			},
			expect: func(f *cpFixture) {
				f.expectLookupByName(nil)
				f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil)
			},
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedVersionNotAvailableReason,
			wantRequeue: 15 * time.Minute,
			wantMessage: "IBM Cloud does not offer Kubernetes 1.33 (offered: 1.35, 1.36)",
		},
		{
			name: "a failed version listing is retried",
			setup: func(objs *cpObjects) {
				withInitialPool(objs)
			},
			expect: func(f *cpFixture) {
				f.expectLookupByName(nil)
				f.kube.EXPECT().ListVersions(gomock.Any()).Return(nil, ibmError(kubernetesservice.OpListVersions, http.StatusTooManyRequests, ""))
			},
			wantReason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
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
			tc.expect(f) // no CreateCluster expectation: gomock fails the test if it is called

			result, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			expectRequeue(g, result, tc.wantRequeue)
			cp := f.controlPlane()
			g.Expect(cp.Status.CreateIntent).To(BeZero(), "nothing is recorded before the configuration is complete")
			condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
				metav1.ConditionFalse, tc.wantReason)
			if tc.wantMessage != "" {
				g.Expect(condition.Message).To(Equal(tc.wantMessage))
			}
		})
	}
}

func TestControlPlaneObserve(t *testing.T) {
	testCases := []struct {
		name string
		// cluster returns IBM's answer.
		cluster     func() *kubernetesservice.Cluster
		want        func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane)
		wantRequeue time.Duration
	}{
		{
			name: "pending_marker_never_reaches_status",
			cluster: func() *kubernetesservice.Cluster {
				c := cpCluster("updating")
				c.MasterKubeVersion = "1.36.5_1537 (pending)"
				c.TargetVersion = "1.37.1_1550"
				c.MasterHealth = "normal"
				return c
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				g.Expect(cp.Status.DistributionVersion).To(Equal("1.36.5_1537"))
				g.Expect(cp.Status.TargetDistributionVersion).To(Equal("1.37.1_1550"))
				g.Expect(cp.Status.State).To(Equal("updating"))
				g.Expect(cp.Status.Master).To(Equal(controlplanev1.MasterStatus{State: "deployed", Health: "normal"}))
				g.Expect(cp.Status.ClusterCRN).To(Equal(cpTestCRN))
			},
			wantRequeue: 5 * time.Minute,
		},
		{
			name:    "the deployed fixture's master is ready with an empty health",
			cluster: func() *kubernetesservice.Cluster { return cpCluster("deployed") },
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionTrue,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason)
				g.Expect(condition.Message).To(HavePrefix("The master of IBM cluster iksclusterid00000001 is ready"))
				g.Expect(cp.Status.Master).To(Equal(controlplanev1.MasterStatus{State: "deployed"}), "an empty health is omitted, never written as an empty string")
			},
			wantRequeue: 5 * time.Minute,
		},
		{
			name: "a master that is not ready is polled at the provisioning cadence",
			cluster: func() *kubernetesservice.Cluster {
				c := cpCluster("pending")
				c.MasterState, c.MasterStatus = "deploying", "Deploy in progress."
				return c
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneAvailableMasterNotReadyReason)
				g.Expect(condition.Message).To(Equal("The master of IBM cluster iksclusterid00000001 is not ready yet (masterState deploying)"))
				g.Expect(condition.Message).NotTo(ContainSubstring("progress"), "masterStatus is IBM free text")
			},
			wantRequeue: time.Minute,
		},
		{
			name: "an unhealthy master is not available; ingress is never consulted",
			cluster: func() *kubernetesservice.Cluster {
				c := cpCluster("critical")
				c.MasterHealth = "error"
				return c
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
					controlplanev1.IBMCloudManagedControlPlaneAvailableMasterNotReadyReason)
				g.Expect(condition.Message).To(Equal("IBM reports the master health of cluster iksclusterid00000001 as error"))
				expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionTrue,
					controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
			},
			wantRequeue: 5 * time.Minute,
		},
		{
			name: "an unparseable IBM version is left out, the rest is recorded",
			cluster: func() *kubernetesservice.Cluster {
				c := cpCluster("normal")
				c.MasterKubeVersion = "not-a-version"
				return c
			},
			want: func(g Gomega, cp *controlplanev1.IBMCloudManagedControlPlane) {
				g.Expect(cp.Status.DistributionVersion).To(BeEmpty())
				g.Expect(cp.Status.State).To(Equal("normal"))
			},
			wantRequeue: 5 * time.Minute,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newCPObjects()
			objs.cp.Status.ClusterID = cpIKSClusterID
			f := newCPFixture(t, objs)
			f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(tc.cluster(), nil)

			result, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			expectRequeue(g, result, tc.wantRequeue)
			cp := f.controlPlane()
			tc.want(g, cp)
			for _, body := range f.patches() {
				g.Expect(emptyStatusStrings(t, body)).To(BeEmpty(), "no status patch writes an empty string: %s", body)
			}
		})
	}
}

// cpUTCIntent returns intent with its times in UTC: the API returns them in the local zone, the tests write UTC.
func cpUTCIntent(intent controlplanev1.CreateIntent) controlplanev1.CreateIntent {
	utc := func(t metav1.Time) metav1.Time {
		if t.IsZero() {
			return t
		}
		return metav1.NewTime(t.UTC())
	}
	intent.InFlightAttemptTime = utc(intent.InFlightAttemptTime)
	intent.FirstUnknownOutcomeTime = utc(intent.FirstUnknownOutcomeTime)
	intent.LastUnknownOutcomeTime = utc(intent.LastUnknownOutcomeTime)
	return intent
}

// createEnvtestCreateInputs creates in namespace what a create needs: the Cluster "dev" with its infrastructure
// provisioned, its IBMCloudManagedCluster with the resolved zones, and the initial pool with its MachinePool, linked by
// the owner reference CAPI sets.
func createEnvtestCreateInputs(t *testing.T, namespace string) (*clusterv1.Cluster, *infrav1.IBMCloudManagedCluster) {
	t.Helper()
	g := NewWithT(t)
	cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
	infra := createTestInfra(t, namespace, "dev", cluster)
	infra.Status = newCPObjects().infra.Status
	g.Expect(testEnv.Status().Update(ctx, infra)).To(Succeed())
	pools := &cpObjects{}
	withInitialPool(pools)
	for _, obj := range pools.extra {
		obj.SetNamespace(namespace)
		obj.SetUID("")
		if obj.GetOwnerReferences() != nil {
			obj.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool",
				Name: pools.extra[0].GetName(), UID: pools.extra[0].GetUID()}})
		}
		g.Expect(testEnv.Create(ctx, obj)).To(Succeed())
	}
	return cluster, infra
}

func TestControlPlaneEnvtestCreate(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "cp-create")
	cluster, infra := createEnvtestCreateInputs(t, namespace)
	// The test ends with a bound control plane and an owned initial pool: paused, a later manager that watches every
	// namespace (the pool controller's envtests) leaves them alone instead of calling IBM for this cluster ID.
	t.Cleanup(func() { pauseTestCluster(t, cluster) })
	cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
	key := client.ObjectKeyFromObject(cp)

	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	resolver := &recordingResolver{}
	r := newEnvtestControlPlaneReconciler(t)
	r.Resolver, r.ClientBuilder = resolver, &stubClientBuilder{kubernetes: kube}
	live := liveNonCriticalResult(t)
	gomock.InOrder(
		kube.EXPECT().GetCluster(gomock.Any(), "dev", testResourceGroupID).Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, "")),
		kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil),
		kube.EXPECT().CreateCluster(gomock.Any(), gomock.Any()).Return(live, nil),
	)

	// Finalizer, then the Paused condition, then the create; every status write passes the CRD validation.
	g.Eventually(func(g Gomega) {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(getEnvtestControlPlane(t, key).Status.ClusterID).To(Equal(live.ClusterID))
	}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

	persisted := getEnvtestControlPlane(t, key)
	g.Expect(persisted.Status.NonCriticalCreateErrorCodes).To(Equal([]string{"E49df"}))
	g.Expect(persisted.Status.CreateIntent.ClusterName).To(Equal("dev"))
	g.Expect(persisted.Status.CreateIntent.InFlightAttemptTime.IsZero()).To(BeFalse())
	expectCondition(g, persisted, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
		controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
	holders := resolver.Holders()
	g.Expect(holders).NotTo(BeEmpty())
	g.Expect(holders[0]).To(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{}))
	g.Expect(holders[0].GetName()).To(Equal(infra.Name), "the identity holder is the Cluster's IBMCloudManagedCluster")
}

func TestControlPlaneEnvtestCreateRefusalSendsOneCreate(t *testing.T) {
	for _, tc := range cpRefusalCases() {
		t.Run(tc.name+"_manager_sends_one_create", func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			namespace := newTestNamespace(t, "cp-hold")
			cluster, _ := createEnvtestCreateInputs(t, namespace)
			// Registered before the manager starts, so it runs after the manager has stopped.
			t.Cleanup(func() { pauseTestCluster(t, cluster) })

			var gets, creates atomic.Int64
			kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
			kube.EXPECT().GetCluster(gomock.Any(), "dev", testResourceGroupID).DoAndReturn(
				func(context.Context, string, string) (*kubernetesservice.Cluster, error) {
					gets.Add(1)
					return nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, "")
				}).AnyTimes()
			kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil).AnyTimes()
			kube.EXPECT().CreateCluster(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, kubernetesservice.CreateClusterRequest) (*kubernetesservice.CreateClusterResult, error) {
					creates.Add(1)
					return nil, tc.err
				}).AnyTimes()
			builder := startControlPlaneManager(t, namespace, kube)
			key := client.ObjectKeyFromObject(createEnvtestControlPlane(t, namespace, "dev", cluster))

			g.Eventually(func(g Gomega) {
				g.Expect(creates.Load()).To(BeNumerically(">=", 1))
				persisted := getEnvtestControlPlane(t, key)
				expectCondition(g, persisted, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
					metav1.ConditionFalse, tc.wantReason)
				g.Expect(persisted.Annotations).To(HaveKey(controlplanev1.CreateHoldAnnotation))
			}, 30*time.Second, 100*time.Millisecond).Should(Succeed())

			// One lookup and one create (E0007: a second lookup). The control plane is created after the manager
			// started, so every reconcile before the create makes no IBM call, and every one after it is held.
			wantGets := int64(1)
			if tc.relookup {
				wantGets = 2
			}
			g.Consistently(func(g Gomega) {
				g.Expect(creates.Load()).To(Equal(int64(1)), "CreateCluster calls")
				g.Expect(gets.Load()).To(Equal(wantGets), "GetCluster calls")
			}, noHotLoopWindow, 100*time.Millisecond).Should(Succeed())
			builder.mu.Lock()
			invalidated := len(builder.invalidated)
			builder.mu.Unlock()
			g.Expect(invalidated).To(BeNumerically("<=", 1), "a 401 rebuilds the authenticator once, not on every pass")
		})
	}
}

// TestControlPlaneEnvtestCreationMarkerWakesTheControlPlane proves the pool watch end to end: a control plane that
// waits for its initial pool (WaitingForInitialWorkerPool, requeued only after clusterProvisioningPoll, one minute)
// creates the IBM cluster within seconds of a pool being marked spec.creation: WithCluster, because the pool's update
// reaches it through the IBMCloudManagedMachinePool watch. The manager watches only the test's namespace and the
// Cluster is paused at cleanup, so that neither this manager nor one of a later test acts on another test's objects.
func TestControlPlaneEnvtestCreationMarkerWakesTheControlPlane(t *testing.T) {
	t.Run("marking a pool WithCluster wakes the waiting control plane through the pool watch", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "cp-marker")
		cluster, _ := createEnvtestCreateInputs(t, namespace)
		// Registered before the manager starts, so it runs after the manager has stopped.
		t.Cleanup(func() { pauseTestCluster(t, cluster) })
		poolKey := client.ObjectKey{Namespace: namespace, Name: cpTestPoolName}
		setCreation := func(creation infrav1.PoolCreation) {
			g.Eventually(func() error {
				pool := &infrav1.IBMCloudManagedMachinePool{}
				if err := testEnv.GetAPIReader().Get(ctx, poolKey, pool); err != nil {
					return err
				}
				pool.Spec.Creation = creation
				return testEnv.Update(ctx, pool)
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		}
		setCreation("") // the template's pool without its marker

		var lookups, creates atomic.Int64
		kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
		kube.EXPECT().GetCluster(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, idOrName, _ string) (*kubernetesservice.Cluster, error) {
				if idOrName == cpIKSClusterID {
					return cpCluster("deploying"), nil // the bound cluster, observed by ID after the create
				}
				lookups.Add(1)
				return nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, "")
			}).AnyTimes()
		kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil).AnyTimes()
		kube.EXPECT().CreateCluster(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, req kubernetesservice.CreateClusterRequest) (*kubernetesservice.CreateClusterResult, error) {
				creates.Add(1)
				if req.WorkerPool.Name != "default" {
					t.Errorf("CreateCluster sent worker pool %q, want the marked pool's spec.poolName default", req.WorkerPool.Name)
				}
				return &kubernetesservice.CreateClusterResult{ClusterID: cpIKSClusterID}, nil
			}).AnyTimes()
		startControlPlaneManager(t, namespace, kube)
		key := client.ObjectKeyFromObject(createEnvtestControlPlane(t, namespace, "dev", cluster))

		g.Eventually(func(g Gomega) {
			expectCondition(g, getEnvtestControlPlane(t, key), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
				metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason)
		}, 30*time.Second, 100*time.Millisecond).Should(Succeed())
		// Settled: the wait made no create, and the control plane's own status write does not keep waking it.
		settled := lookups.Load()
		g.Consistently(func(g Gomega) {
			g.Expect(creates.Load()).To(BeZero(), "no create without a WithCluster pool")
			g.Expect(lookups.Load()).To(BeNumerically("<=", settled+1), "binding lookups while waiting")
		}, noHotLoopWindow, 100*time.Millisecond).Should(Succeed())

		setCreation(infrav1.PoolCreationWithCluster)

		// Well inside clusterProvisioningPoll minus its jitter: only the pool watch can have brought the control plane back.
		g.Eventually(func(g Gomega) {
			g.Expect(creates.Load()).To(Equal(int64(1)))
			g.Expect(getEnvtestControlPlane(t, key).Status.ClusterID).To(Equal(cpIKSClusterID))
		}, 20*time.Second, 100*time.Millisecond).Should(Succeed())
	})
}
