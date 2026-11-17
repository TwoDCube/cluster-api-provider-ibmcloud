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
	"sigs.k8s.io/cluster-api/util/conditions"

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
	clk := clocktesting.NewFakePassiveClock(time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC))
	versions, err := NewVersionsCache(clk, time.Hour)
	if err != nil {
		panic(err)
	}
	return &ScopeParams{
		Client:        c,
		APIReader:     c,
		Recorder:      record.NewFakeRecorder(10),
		Resolver:      credentials.ControllerOnly{},
		ClientBuilder: builder,
		Clock:         clk,
		Prober:        &cpTestProber{},
		VersionsCache: versions,
	}
}

// cpNoPersist is a PersistFunc for scope tests that never write.
func cpNoPersist(context.Context) error { return nil }

func TestNewControlPlaneScopeValidation(t *testing.T) {
	cluster := &clusterv1.Cluster{}
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	testCases := []struct {
		name    string
		params  func() *ScopeParams
		cluster *clusterv1.Cluster
		cp      *controlplanev1.IBMCloudManagedControlPlane
		persist PersistFunc
		wantErr string
	}{
		{name: "nil params", params: func() *ScopeParams { return nil }, cluster: cluster, cp: cp,
			wantErr: "ControlPlaneScope needs ScopeParams, got nil"},
		{name: "every missing dependency is named", params: func() *ScopeParams { return &ScopeParams{} }, cluster: cluster, cp: cp,
			wantErr: "ControlPlaneScope needs ScopeParams.Client, ScopeParams.APIReader, ScopeParams.Recorder, ScopeParams.Resolver, " +
				"ScopeParams.ClientBuilder, ScopeParams.Clock, ScopeParams.Prober, ScopeParams.VersionsCache"},
		{name: "nil Cluster", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cp: cp,
			wantErr: "ControlPlaneScope needs the owning Cluster"},
		{name: "nil control plane", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cluster: cluster,
			wantErr: "ControlPlaneScope needs the IBMCloudManagedControlPlane"},
		{name: "nil persist", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cluster: cluster, cp: cp,
			wantErr: "ControlPlaneScope needs a PersistFunc"},
		{name: "complete", params: func() *ScopeParams { return newCPScopeParams(&cpScopeBuilder{}) }, cluster: cluster, cp: cp,
			persist: cpNoPersist},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			scope, err := NewControlPlaneScope(tc.params(), tc.cluster, tc.cp, tc.persist)
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
	scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{}), &clusterv1.Cluster{}, &controlplanev1.IBMCloudManagedControlPlane{}, cpNoPersist)
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
			wantMessage: "IBM Cloud rejected the credentials of the CAPIBM controller identity with HTTP 401 while getting IBM " +
				"cluster dev; CAPIBM rebuilds its authenticator and retries in about 5 minutes; check that the API key of the " +
				"CAPIBM controller identity is valid and not deleted"},
		{name: "IAM refused the credentials before the request", err: iamRejected(kubernetesservice.OpGetCluster),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState, wantInvalidate: true,
			wantMessage: "The IAM token service of IBM Cloud rejected the credentials of the CAPIBM controller identity with HTTP " +
				"400 while getting IBM cluster dev, so the request was not sent; CAPIBM rebuilds its authenticator and retries " +
				"in about 5 minutes; check that the API key of the CAPIBM controller identity is valid and not deleted"},
		{name: "IAM refused the credentials, status not known", err: fmt.Errorf("%w: GetCluster", kubernetesservice.ErrCredentialsRejected),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState, wantInvalidate: true,
			wantMessage: "The IAM token service of IBM Cloud rejected the credentials of the CAPIBM controller identity while " +
				"getting IBM cluster dev, so the request was not sent; CAPIBM rebuilds its authenticator and retries in about " +
				"5 minutes; check that the API key of the CAPIBM controller identity is valid and not deleted"},
		{name: "IAM refused the credentials, status zero", err: &kubernetesservice.CredentialsRejectedError{Operation: kubernetesservice.OpGetCluster},
			wantReason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason, wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState, wantInvalidate: true,
			wantMessage: "The IAM token service of IBM Cloud rejected the credentials of the CAPIBM controller identity while " +
				"getting IBM cluster dev, so the request was not sent; CAPIBM rebuilds its authenticator and retries in about " +
				"5 minutes; check that the API key of the CAPIBM controller identity is valid and not deleted"},
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

// iamRejected returns the error of a call of op that the IAM token service refused before it was sent, as
// kubernetesservice returns it: IAM answers 400 for a deleted API key. op is the operation.
func iamRejected(op kubernetesservice.Operation) error {
	return &kubernetesservice.CredentialsRejectedError{Operation: op, IAMStatus: http.StatusBadRequest,
		Err: errors.New("Provided API key could not be found")}
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
	scope, err := NewControlPlaneScope(params, cluster, &controlplanev1.IBMCloudManagedControlPlane{}, cpNoPersist)
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

func TestControlPlaneCreateIntentModel(t *testing.T) {
	at := func(minute int) metav1.Time {
		return metav1.NewTime(time.Date(2026, 11, 16, 3, minute, 0, 0, time.UTC))
	}
	testCases := []struct {
		name      string
		previous  controlplanev1.CreateIntent
		wantNext  controlplanev1.CreateIntent
		wantFold  controlplanev1.CreateIntent
		wantClear controlplanev1.CreateIntent
	}{
		{
			name:      "first attempt",
			wantNext:  controlplanev1.CreateIntent{ClusterName: "dev", InFlightAttemptTime: at(10)},
			wantFold:  controlplanev1.CreateIntent{},
			wantClear: controlplanev1.CreateIntent{},
		},
		{
			name:      "a leftover in-flight attempt is folded into the unknown bounds first",
			previous:  controlplanev1.CreateIntent{ClusterName: "dev", InFlightAttemptTime: at(2)},
			wantNext:  controlplanev1.CreateIntent{ClusterName: "dev", InFlightAttemptTime: at(10), FirstUnknownOutcomeTime: at(2), LastUnknownOutcomeTime: at(2)},
			wantFold:  controlplanev1.CreateIntent{ClusterName: "dev", FirstUnknownOutcomeTime: at(2), LastUnknownOutcomeTime: at(2)},
			wantClear: controlplanev1.CreateIntent{},
		},
		{
			name: "earlier unknown attempts widen the bounds",
			previous: controlplanev1.CreateIntent{ClusterName: "dev", InFlightAttemptTime: at(5), FirstUnknownOutcomeTime: at(1),
				LastUnknownOutcomeTime: at(3)},
			wantNext: controlplanev1.CreateIntent{ClusterName: "dev", InFlightAttemptTime: at(10), FirstUnknownOutcomeTime: at(1),
				LastUnknownOutcomeTime: at(5)},
			wantFold: controlplanev1.CreateIntent{ClusterName: "dev", FirstUnknownOutcomeTime: at(1), LastUnknownOutcomeTime: at(5)},
			wantClear: controlplanev1.CreateIntent{ClusterName: "dev", FirstUnknownOutcomeTime: at(1),
				LastUnknownOutcomeTime: at(3)},
		},
		{
			name:      "an intent of another name is replaced",
			previous:  controlplanev1.CreateIntent{ClusterName: "old", FirstUnknownOutcomeTime: at(1), LastUnknownOutcomeTime: at(1)},
			wantNext:  controlplanev1.CreateIntent{ClusterName: "dev", InFlightAttemptTime: at(10)},
			wantFold:  controlplanev1.CreateIntent{ClusterName: "old", FirstUnknownOutcomeTime: at(1), LastUnknownOutcomeTime: at(1)},
			wantClear: controlplanev1.CreateIntent{ClusterName: "old", FirstUnknownOutcomeTime: at(1), LastUnknownOutcomeTime: at(1)},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(cpIntentForAttempt(tc.previous, "dev", at(10))).To(Equal(tc.wantNext))
			g.Expect(cpFoldInFlight(tc.previous)).To(Equal(tc.wantFold))
			g.Expect(cpClearAttempt(tc.previous)).To(Equal(tc.wantClear))
		})
	}
}

func TestCPCreatedNothing(t *testing.T) {
	apiError := func(status int, code string) error {
		return &kubernetesservice.APIError{Operation: kubernetesservice.OpCreateCluster, StatusCode: status, Code: code}
	}
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "E0007 any status", err: apiError(http.StatusConflict, "E0007"), want: true},
		{name: "A0656 any status", err: apiError(http.StatusInternalServerError, "A0656"), want: true},
		{name: "400 P4102 subnet not found", err: apiError(http.StatusBadRequest, "P4102"), want: true},
		{name: "400 E7278 ROKS without a COS instance", err: apiError(http.StatusBadRequest, "E7278"), want: true},
		{name: "401", err: apiError(http.StatusUnauthorized, ""), want: true},
		{name: "403", err: apiError(http.StatusForbidden, ""), want: true},
		{name: "422", err: apiError(http.StatusUnprocessableEntity, ""), want: true},
		{name: "never sent: invalid request", err: kubernetesservice.ErrInvalidRequest, want: true},
		{name: "never sent: IAM refused", err: kubernetesservice.ErrCredentialsRejected, want: true},
		{name: "409 without a code", err: apiError(http.StatusConflict, "")},
		{name: "429", err: apiError(http.StatusTooManyRequests, "")},
		{name: "500", err: apiError(http.StatusInternalServerError, "E0004")},
		{name: "timeout", err: context.DeadlineExceeded},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(cpCreatedNothing(tc.err)).To(Equal(tc.want))
		})
	}
}

func TestCPNonCriticalCodes(t *testing.T) {
	g := NewWithT(t)
	g.Expect(cpNonCriticalCodes(nil)).To(BeNil(), "no codes omit the field")
	items := []kubernetesservice.IBMMessage{{Code: "E49df"}, {Code: "A1234"}, {Code: "E49df"}, {Code: ""}, {Code: "TOO-LONG-FOR-STATUS"}}
	g.Expect(cpNonCriticalCodes(items)).To(Equal([]string{"A1234", "E49df"}))
	many := make([]kubernetesservice.IBMMessage, 0, 20)
	for i := range 20 {
		many = append(many, kubernetesservice.IBMMessage{Code: "E" + string(rune('a'+i))})
	}
	g.Expect(cpNonCriticalCodes(many)).To(HaveLen(cpNonCriticalCodesMaxItems))
	g.Expect(cpNonCriticalNote(nil)).To(BeEmpty())
	g.Expect(cpNonCriticalNote([]string{"E49df"})).To(Equal("; IBM created the cluster with non-critical errors E49df (see the IBMCreateWarnings events)"))
	g.Expect(cpNonCriticalEventMessage("dev", "iksclusterid00000001", "inc-1", kubernetesservice.IBMMessage{
		Code: "E49df", Type: "Provisioning", Description: "Your cluster cannot pull images.", RecoveryCLI: "Ask for the role.",
	})).To(Equal("IBM created cluster dev (ID iksclusterid00000001) with the non-critical error E49df (Provisioning): Your " +
		"cluster cannot pull images. Recovery: Ask for the role. [incident inc-1]"))
	g.Expect(cpNonCriticalEventMessage("dev", "id", "", kubernetesservice.IBMMessage{Code: "E1"})).
		To(Equal("IBM created cluster dev (ID id) with the non-critical error E1"))
}

func TestInfrastructureKeyNotCreatableMessage(t *testing.T) {
	g := NewWithT(t)
	message := InfrastructureKeyNotCreatableMessage("us-south", "0123456789abcdef0123456789abcdef", credentials.ControllerDescription,
		" (IBM code A0656)")
	g.Expect(message).To(Equal("IBM cannot create the infrastructure API key for region us-south and resource group " +
		"0123456789abcdef0123456789abcdef with the credentials of the CAPIBM controller identity (IBM code A0656); a service " +
		"ID needs the IAM Identity Service roles (Service ID creator, User API key creator, Viewer and Operator), or an " +
		"account administrator pre-sets the key with `ibmcloud ks api-key reset --region us-south`; the IAM page of the " +
		"managed Kubernetes book lists every role"))
}

func TestControlPlaneCreateRefusedMessage(t *testing.T) {
	scope := &ControlPlaneScope{InfraCluster: &infrav1.IBMCloudManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "dev"}}}
	testCases := []struct {
		name string
		err  error
		want string
	}{
		{name: "CAPIBM refused to send", err: fmt.Errorf("%w: CreateCluster: Name is empty", kubernetesservice.ErrInvalidRequest),
			want: "CAPIBM did not send the create request of IBM cluster dev: kubernetesservice: invalid request, nothing was sent " +
				"to IBM Cloud: CreateCluster: Name is empty"},
		{name: "another 400", err: &kubernetesservice.APIError{Operation: kubernetesservice.OpCreateCluster, StatusCode: http.StatusBadRequest,
			Code: "E9999", Description: "IBM free text", IncidentID: "inc-2"},
			want: "IBM Cloud refused to create cluster dev with HTTP 400 (IBM code E9999); the controller log has " +
				"IBM's description of the configuration to fix"},
		{name: "not an IBM answer", err: errors.New("unexpected"),
			want: "IBM Cloud refused to create cluster dev; the controller log has the cause"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			message := scope.createRefusedMessage("dev", tc.err)
			g.Expect(message).To(Equal(tc.want))
			g.Expect(message).NotTo(ContainSubstring("IBM free text"), "IBM's description stays in the controller log")
			g.Expect(message).NotTo(ContainSubstring("inc-2"), "IBM's per-answer incident ID never reaches status")
		})
	}
}

func TestCPCreateHold(t *testing.T) {
	now := time.Date(2026, 11, 16, 3, 10, 0, 0, time.UTC)
	refused := func(mutate func(cp *controlplanev1.IBMCloudManagedControlPlane)) *controlplanev1.IBMCloudManagedControlPlane {
		cp := &controlplanev1.IBMCloudManagedControlPlane{
			ObjectMeta: metav1.ObjectMeta{Generation: 3, Annotations: map[string]string{
				controlplanev1.CreateHoldAnnotation: now.Add(10 * time.Minute).Format(time.RFC3339),
			}},
			Status: controlplanev1.IBMCloudManagedControlPlaneStatus{Conditions: []metav1.Condition{{
				Type:               controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
				Status:             metav1.ConditionFalse,
				Reason:             controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
				ObservedGeneration: 3,
			}}},
		}
		if mutate != nil {
			mutate(cp)
		}
		return cp
	}
	setReason := func(reason string) func(cp *controlplanev1.IBMCloudManagedControlPlane) {
		return func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Status.Conditions[0].Reason = reason }
	}
	testCases := []struct {
		name string
		cp   *controlplanev1.IBMCloudManagedControlPlane
		want bool
	}{
		{name: "a fresh refusal holds", cp: refused(nil), want: true},
		{name: "an InvalidConfiguration refusal holds",
			cp: refused(setReason(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason)), want: true},
		{name: "a ClusterNameConflict refusal holds",
			cp: refused(setReason(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason)), want: true},
		{name: "a hold up to CreateRefusalHold ahead holds", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Annotations[controlplanev1.CreateHoldAnnotation] = now.Add(CreateRefusalHold).Format(time.RFC3339)
		}), want: true},
		{name: "nil", cp: nil},
		{name: "no annotation", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Annotations = nil })},
		{name: "an annotation that is not a time", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Annotations[controlplanev1.CreateHoldAnnotation] = "soon"
		})},
		{name: "a hold that has ended", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Annotations[controlplanev1.CreateHoldAnnotation] = now.Format(time.RFC3339)
		})},
		{name: "a time beyond CreateRefusalHold was not written by CAPIBM", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Annotations[controlplanev1.CreateHoldAnnotation] = now.Add(CreateRefusalHold + time.Second).Format(time.RFC3339)
		})},
		{name: "a spec change ends the hold", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Generation = 4 })},
		{name: "another report replaced the refusal",
			cp: refused(setReason(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason))},
		{name: "a condition that is not False", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Status.Conditions[0].Status = metav1.ConditionUnknown
		})},
		{name: "no condition", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Status.Conditions = nil })},
		{name: "a bound control plane is never held", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Status.ClusterID = "iksclusterid00000001"
		})},
		{name: "a control plane with an endpoint is never held", cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
			cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}
		})},
		// The refusal took its own attempt out of the intent; an earlier attempt of unknown outcome stays, and the
		// lookup by name decides a new create once that attempt is older than CreateGracePeriod, so the hold must hold.
		{name: "an earlier attempt of unknown outcome still holds", want: true,
			cp: refused(func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				earlier := metav1.NewTime(now.Add(-10 * time.Minute))
				cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: "dev", FirstUnknownOutcomeTime: earlier,
					LastUnknownOutcomeTime: earlier}
			})},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			until, held := cpCreateHold(tc.cp, now)
			g.Expect(held).To(Equal(tc.want))
			if tc.want {
				g.Expect(until).To(BeTemporally("==", mustParseRFC3339(t, tc.cp.Annotations[controlplanev1.CreateHoldAnnotation])))
			} else {
				g.Expect(until.IsZero()).To(BeTrue())
			}
		})
	}
}

// mustParseRFC3339 parses an RFC 3339 time of a test.
func mustParseRFC3339(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return parsed
}

func TestCPCreateHoldFor(t *testing.T) {
	testCases := []struct {
		class    kubernetesservice.ErrorClass
		wantHold time.Duration
		wantOK   bool
	}{
		{class: kubernetesservice.ClassUnauthorized, wantHold: CreateUnauthorizedHold, wantOK: true},
		{class: kubernetesservice.ClassInfrastructureKeyNotCreatable, wantHold: CreateRefusalHold, wantOK: true},
		{class: kubernetesservice.ClassInfrastructureKeyDenied, wantHold: CreateRefusalHold, wantOK: true},
		{class: kubernetesservice.ClassPermissionDenied, wantHold: CreateRefusalHold, wantOK: true},
		{class: kubernetesservice.ClassBadRequest, wantHold: CreateRefusalHold, wantOK: true},
		// E0007 is held only after a lookup that finds nothing (afterNameExists); unknown outcomes are never held.
		{class: kubernetesservice.ClassClusterNameExists},
		{class: kubernetesservice.ClassRateLimited},
		{class: kubernetesservice.ClassTransient},
		{class: kubernetesservice.ClassUnknown},
	}
	for _, tc := range testCases {
		t.Run(string(tc.class), func(t *testing.T) {
			g := NewWithT(t)
			hold, ok := cpCreateHoldFor(tc.class)
			g.Expect(ok).To(Equal(tc.wantOK))
			g.Expect(hold).To(Equal(tc.wantHold))
		})
	}
}

func TestControlPlaneHoldCreates(t *testing.T) {
	g := NewWithT(t)
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{}), &clusterv1.Cluster{}, cp, cpNoPersist)
	g.Expect(err).NotTo(HaveOccurred())

	scope.holdCreates(CreateRefusalHold)

	// The fake clock of newCPScopeParams starts at 03:05:30 UTC; the annotation is UTC at second precision.
	g.Expect(cp.Annotations).To(HaveKeyWithValue(controlplanev1.CreateHoldAnnotation, "2026-11-16T03:17:30Z"))
}

// TestControlPlaneReportKeepsAHeldRefusal covers the two conditions report keeps: a True condition against a transient
// problem, and the condition of a definitive create refusal against any problem while that refusal holds the next
// create (cpCreateHold). A failure written over the refusal would end the hold, and the write itself would trigger the
// reconcile that sends the refused create again.
func TestControlPlaneReportKeepsAHeldRefusal(t *testing.T) {
	// The fake clock of newCPScopeParams starts at 03:05:30 UTC.
	now := time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)
	provisioned := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition
	refusal := metav1.Condition{Type: provisioned, Status: metav1.ConditionFalse, ObservedGeneration: 2,
		Reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
		Message: "IBM Cloud refused to create cluster dev (IBM code P4102)"}
	ready := metav1.Condition{Type: provisioned, Status: metav1.ConditionTrue, ObservedGeneration: 2,
		Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason, Message: "IBM cluster dev exists"}
	controlPlane := func(condition metav1.Condition, holdUntil time.Time) *controlplanev1.IBMCloudManagedControlPlane {
		cp := &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Generation: 2}}
		cp.Status.Conditions = []metav1.Condition{condition}
		if !holdUntil.IsZero() {
			cp.Annotations = map[string]string{controlplanev1.CreateHoldAnnotation: holdUntil.Format(time.RFC3339)}
		}
		return cp
	}
	lookupFailed := cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message: "CAPIBM could not complete getting IBM cluster dev; it retries", outcome: Transient(errors.New("HTTP 503")),
		transient: true}
	lookupRateLimited := cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message: "IBM Cloud rate limited CAPIBM (HTTP 429) while getting IBM cluster dev; CAPIBM retries",
		outcome: RateLimited(time.Minute), transient: true}
	credentialsFailed := cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message: "CAPIBM cannot resolve the IBM Cloud credentials of this control plane", outcome: Transient(errors.New("no Secret"))}
	lookupDenied := cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
		message: "IBM Cloud refused getting IBM cluster dev with HTTP 403", outcome: Terminal("PermissionDenied", "denied")}

	testCases := []struct {
		name    string
		cp      *controlplanev1.IBMCloudManagedControlPlane
		problem cpProblem
		// want is the condition after report.
		want metav1.Condition
	}{
		{name: "a held refusal is kept against a failed lookup", cp: controlPlane(refusal, now.Add(time.Minute)),
			problem: lookupFailed, want: refusal},
		{name: "a held refusal is kept against a rate limited lookup", cp: controlPlane(refusal, now.Add(time.Minute)),
			problem: lookupRateLimited, want: refusal},
		{name: "a held refusal is kept against credentials that cannot be resolved", cp: controlPlane(refusal, now.Add(time.Minute)),
			problem: credentialsFailed, want: refusal},
		{name: "a held refusal is kept against a refused lookup", cp: controlPlane(refusal, now.Add(time.Minute)),
			problem: lookupDenied, want: refusal},
		{name: "a refusal whose hold has ended is replaced", cp: controlPlane(refusal, now),
			problem: lookupFailed, want: metav1.Condition{Reason: lookupFailed.reason, Message: lookupFailed.message}},
		{name: "a refusal without a hold is replaced", cp: controlPlane(refusal, time.Time{}),
			problem: credentialsFailed, want: metav1.Condition{Reason: credentialsFailed.reason, Message: credentialsFailed.message}},
		{name: "a True condition is kept against a transient problem", cp: controlPlane(ready, time.Time{}),
			problem: lookupFailed, want: ready},
		{name: "a True condition is replaced by a problem that is not transient", cp: controlPlane(ready, time.Time{}),
			problem: credentialsFailed, want: metav1.Condition{Reason: credentialsFailed.reason, Message: credentialsFailed.message}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{}), &clusterv1.Cluster{}, tc.cp, cpNoPersist)
			g.Expect(err).NotTo(HaveOccurred())

			outcome := scope.report(context.Background(), tc.problem)

			g.Expect(outcome).To(Equal(tc.problem.outcome), "the problem's outcome sets the retry pace either way")
			condition := conditions.Get(tc.cp, provisioned)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Reason).To(Equal(tc.want.Reason))
			g.Expect(condition.Message).To(Equal(tc.want.Message))
		})
	}
}
