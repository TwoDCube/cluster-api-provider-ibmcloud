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
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"

	. "github.com/onsi/gomega"
)

// The control plane scope tests cover what the controller tests do not reach directly: constructor validation,
// NoIBMClusterCanExist, the order guard of ibm, and the mapping of every IBM error class to a condition and an
// outcome. The flows themselves are tested through the controller (internal/controllers/managed).

// cpScopeBuilder is a ClientBuilder that returns one IKS client and records invalidations.
type cpScopeBuilder struct {
	kubernetes  kubernetesservice.KubernetesService
	invalidated []credentials.IdentityKey
}

// KubernetesService implements ClientBuilder.
func (b *cpScopeBuilder) KubernetesService(context.Context, credentials.Identity, string) (kubernetesservice.KubernetesService, error) {
	return b.kubernetes, nil
}

// VPC implements ClientBuilder; the control plane never asks for it.
func (b *cpScopeBuilder) VPC(context.Context, credentials.Identity, string) (vpc.Vpc, error) {
	return nil, errors.New("cpScopeBuilder: the control plane needs no VPC client")
}

// ResourceManager implements ClientBuilder; the control plane never asks for it.
func (b *cpScopeBuilder) ResourceManager(context.Context, credentials.Identity) (resourcemanager.ResourceManager, error) {
	return nil, errors.New("cpScopeBuilder: the control plane needs no Resource Manager client")
}

// InvalidateAuthenticator implements ClientBuilder.
func (b *cpScopeBuilder) InvalidateAuthenticator(key credentials.IdentityKey) {
	b.invalidated = append(b.invalidated, key)
}

// newCPScopeParams returns complete ScopeParams for a control plane scope test.
func newCPScopeParams(builder ClientBuilder) *ScopeParams {
	c := fake.NewClientBuilder().Build()
	return &ScopeParams{
		Client:        c,
		APIReader:     c,
		Recorder:      record.NewFakeRecorder(10),
		Resolver:      credentials.ControllerOnly{},
		ClientBuilder: builder,
		Clock:         clocktesting.NewFakePassiveClock(time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)),
	}
}

func TestNewControlPlaneScopeValidation(t *testing.T) {
	cluster := &clusterv1.Cluster{}
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	testCases := []struct {
		name    string
		params  func() *ScopeParams
		cluster *clusterv1.Cluster
		cp      *controlplanev1.IBMCloudManagedControlPlane
		wantErr string
	}{
		{name: "nil params", params: func() *ScopeParams { return nil }, cluster: cluster, cp: cp,
			wantErr: "ControlPlaneScope needs ScopeParams, got nil"},
		{name: "every missing dependency is named", params: func() *ScopeParams { return &ScopeParams{} }, cluster: cluster, cp: cp,
			wantErr: "ControlPlaneScope needs ScopeParams.Client, ScopeParams.APIReader, ScopeParams.Recorder, ScopeParams.Resolver, " +
				"ScopeParams.ClientBuilder, ScopeParams.Clock"},
		{name: "nil Cluster", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cp: cp,
			wantErr: "ControlPlaneScope needs the owning Cluster"},
		{name: "nil control plane", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cluster: cluster,
			wantErr: "ControlPlaneScope needs the IBMCloudManagedControlPlane"},
		{name: "complete", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cluster: cluster, cp: cp},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			scope, err := NewControlPlaneScope(tc.params(), tc.cluster, tc.cp)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(scope).To(BeNil())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(scope.InfraCluster).To(BeNil(), "the constructor reads nothing")
		})
	}
}

func TestNoIBMClusterCanExist(t *testing.T) {
	testCases := []struct {
		name string
		cp   *controlplanev1.IBMCloudManagedControlPlane
		want bool
	}{
		{name: "nil", want: true},
		{name: "fresh", cp: &controlplanev1.IBMCloudManagedControlPlane{}, want: true},
		{name: "cluster ID", cp: &controlplanev1.IBMCloudManagedControlPlane{
			Status: controlplanev1.IBMCloudManagedControlPlaneStatus{ClusterID: "iksclusterid00000001"}}},
		{name: "endpoint only", cp: &controlplanev1.IBMCloudManagedControlPlane{
			Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{ControlPlaneEndpoint: controlplanev1.APIEndpoint{Host: "c1.example.com", Port: 443}}}},
		{name: "intent name only", cp: &controlplanev1.IBMCloudManagedControlPlane{
			Status: controlplanev1.IBMCloudManagedControlPlaneStatus{CreateIntent: controlplanev1.CreateIntent{ClusterName: "dev"}}}},
		{name: "intent time only", cp: &controlplanev1.IBMCloudManagedControlPlane{
			Status: controlplanev1.IBMCloudManagedControlPlaneStatus{CreateIntent: controlplanev1.CreateIntent{
				LastUnknownOutcomeTime: metav1.NewTime(time.Date(2026, 11, 16, 3, 0, 0, 0, time.UTC))}}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(NoIBMClusterCanExist(tc.cp)).To(Equal(tc.want))
		})
	}
}

func TestControlPlaneScopeIBMNeedsTheInfraCluster(t *testing.T) {
	g := NewWithT(t)
	scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{}), &clusterv1.Cluster{}, &controlplanev1.IBMCloudManagedControlPlane{})
	g.Expect(err).NotTo(HaveOccurred())
	_, err = scope.ibm(context.Background())
	g.Expect(err).To(MatchError(ContainSubstring("before the IBMCloudManagedCluster was loaded")))
}

func TestControlPlaneClassifyCall(t *testing.T) {
	const rg = "0123456789abcdef0123456789abcdef"
	identity := credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}
	apiError := func(status int, code string) error {
		return &kubernetesservice.APIError{Operation: kubernetesservice.OpGetCluster, StatusCode: status, Code: code, IncidentID: "inc-7"}
	}
	testCases := []struct {
		name           string
		err            error
		wantReason     string
		wantKind       OutcomeKind
		wantInterval   Interval
		wantTransient  bool
		wantMessage    string
		wantInvalidate bool
	}{
		{name: "401", err: apiError(http.StatusUnauthorized, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState, wantInvalidate: true,
			wantMessage: "IBM Cloud rejected the credentials of the CAPIBM controller identity while getting IBM cluster dev; CAPIBM " +
				"rebuilds its authenticator and retries in about 5 minutes; check that the API key of the CAPIBM controller " +
				"identity is valid and not deleted"},
		{name: "IAM refused the credentials before the request", err: kubernetesservice.ErrCredentialsRejected,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState, wantInvalidate: true},
		{name: "403", err: apiError(http.StatusForbidden, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantKind: OutcomeTerminal,
			wantMessage: "IBM Cloud refused getting IBM cluster dev with HTTP 403: the credentials of the CAPIBM " +
				"controller identity need the Administrator platform role and the Manager service role of Kubernetes Service on " +
				"resource group 0123456789abcdef0123456789abcdef; the IAM page of the managed Kubernetes book lists every role"},
		{name: "429", err: apiError(http.StatusTooManyRequests, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason, wantKind: OutcomeRateLimited,
			wantTransient: true, wantMessage: "IBM Cloud rate limited CAPIBM (HTTP 429) while getting IBM cluster dev; CAPIBM retries"},
		{name: "400", err: apiError(http.StatusBadRequest, "E1234"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason, wantKind: OutcomeTerminal,
			wantMessage: "IBM Cloud refused getting IBM cluster dev with HTTP 400 (IBM code E1234); the controller log " +
				"has IBM's description"},
		{name: "500", err: apiError(http.StatusInternalServerError, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason, wantKind: OutcomeTransient,
			wantTransient: true},
		{name: "network", err: errors.New("dial tcp: connection refused"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason, wantKind: OutcomeTransient,
			wantTransient: true,
			wantMessage:   "CAPIBM could not complete getting IBM cluster dev; it retries, and the controller log names the cause"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			builder := &cpScopeBuilder{}
			scope := &ControlPlaneScope{params: newCPScopeParams(builder)}
			problem := scope.classifyCall(&controlPlaneCallError{identity: identity, op: kubernetesservice.OpGetCluster,
				action: "getting IBM cluster dev", err: tc.err}, rg)
			g.Expect(problem.reason).To(Equal(tc.wantReason))
			g.Expect(problem.outcome.Kind).To(Equal(tc.wantKind))
			g.Expect(problem.transient).To(Equal(tc.wantTransient))
			if tc.wantInterval != "" {
				g.Expect(problem.outcome.Interval).To(Equal(tc.wantInterval))
			}
			if tc.wantMessage != "" {
				g.Expect(problem.message).To(Equal(tc.wantMessage))
			}
			g.Expect(problem.message).NotTo(ContainSubstring("inc-7"), "IBM's per-answer incident ID never reaches status")
			if tc.wantInvalidate {
				g.Expect(builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
			} else {
				g.Expect(builder.invalidated).To(BeEmpty())
			}
			if tc.wantKind == OutcomeTransient {
				g.Expect(problem.outcome.Err).To(MatchError(ContainSubstring("getting IBM cluster dev")))
			}
		})
	}
}

func TestControlPlaneScopeLoadsTheInfraClusterOnce(t *testing.T) {
	g := NewWithT(t)
	infra := &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "dev"},
		Spec:       infrav1.IBMCloudManagedClusterSpec{Region: "us-south"},
	}
	params := newCPScopeParams(&cpScopeBuilder{kubernetes: ksmock.NewMockKubernetesService(gomock.NewController(t))})
	params.Client = fake.NewClientBuilder().WithScheme(cpScopeScheme(t)).WithObjects(infra).Build()
	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "dev"},
		Spec: clusterv1.ClusterSpec{InfrastructureRef: clusterv1.ContractVersionedObjectReference{
			APIGroup: infrav1.GroupVersion.Group, Kind: "IBMCloudManagedCluster", Name: "dev",
		}},
	}
	scope, err := NewControlPlaneScope(params, cluster, &controlplanev1.IBMCloudManagedControlPlane{})
	g.Expect(err).NotTo(HaveOccurred())

	loaded, err := scope.loadInfraCluster(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(client.ObjectKeyFromObject(loaded)).To(Equal(client.ObjectKeyFromObject(infra)))
	first, err := scope.ibm(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	second, err := scope.ibm(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(second).To(BeIdenticalTo(first), "ibm is memoized for the reconcile")
	g.Expect(first.identity.Key).To(Equal(credentials.ControllerKey()))
}

// cpScopeScheme returns a scheme with the core, Cluster API and managed kinds, for fake clients.
func cpScopeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(clientgoscheme.AddToScheme(s)).To(Succeed())
	g.Expect(clusterv1.AddToScheme(s)).To(Succeed())
	g.Expect(infrav1.AddToScheme(s)).To(Succeed())
	g.Expect(controlplanev1.AddToScheme(s)).To(Succeed())
	return s
}

func TestControlPlaneErrorTypes(t *testing.T) {
	g := NewWithT(t)
	answer := &kubernetesservice.APIError{Operation: kubernetesservice.OpGetCluster, StatusCode: http.StatusBadGateway, Code: "E0004", IncidentID: "inc-1"}
	callErr := &controlPlaneCallError{op: kubernetesservice.OpGetCluster, action: "getting IBM cluster dev", err: answer}
	g.Expect(callErr.Error()).To(Equal("getting IBM cluster dev: IBM Cloud Kubernetes Service GetCluster failed with HTTP 502 (code E0004) [incident inc-1]"))
	var unwrapped *kubernetesservice.APIError
	g.Expect(errors.As(callErr, &unwrapped)).To(BeTrue(), "Classify sees IBM's answer through the wrapper")
	g.Expect(kubernetesservice.Classify(kubernetesservice.OpGetCluster, callErr)).To(Equal(kubernetesservice.ClassTransient))
	g.Expect(cpIBMCode(callErr)).To(Equal(" (IBM code E0004)"))
	g.Expect(cpIBMCode(errors.New("no answer"))).To(BeEmpty())
	g.Expect(cpIBMStatus(callErr)).To(Equal(http.StatusBadGateway))
	g.Expect(cpIBMStatus(errors.New("no answer"))).To(BeZero())
	g.Expect((&controlPlaneConfigError{message: "fix spec.x"}).Error()).To(Equal("fix spec.x"))
}
