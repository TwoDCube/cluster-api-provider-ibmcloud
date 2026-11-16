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
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// The ownership tests cover the proof of possession of DecideBinding rows 2 and 5 (proveOwnership) and the delete
// flow's IBMClusterNotVisible message directly; the bindings and the delete flow that use them run through the
// controller tests (ibmcloudmanagedcontrolplane_move_test.go, ibmcloudmanagedcontrolplane_delete_test.go). The gomock
// client has no GetAdminKubeconfig expectation in any case: the proof never fetches a credential.

// ownershipCandidateURL is the candidate's master URL; the Secret's kubeconfig names another server on purpose.
const ownershipCandidateURL = "https://c111.us-south.containers.cloud.ibm.com:30101"

// ownershipFixture is a control plane scope whose kubeconfig Secret holds value, with a gomock IKS client and a
// scripted prober.
type ownershipFixture struct {
	scope   *ControlPlaneScope
	clients *controlPlaneClients
	kube    *ksmock.MockKubernetesService
	prober  *cpTestProber
}

// newOwnershipFixture returns the fixture of a control plane of distribution whose Secret has type secretType and
// value.
//
// Parameters:
//   - t is the test.
//   - distribution is spec.distribution.
//   - value is the Secret's value key; nil stores no Secret.
func newOwnershipFixture(t *testing.T, distribution controlplanev1.Distribution, value []byte) *ownershipFixture {
	t.Helper()
	g := NewWithT(t)
	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	prober := &cpTestProber{}
	params := newCPScopeParams(&cpScopeBuilder{kubernetes: kube})
	builder := fake.NewClientBuilder().WithScheme(cpScopeScheme(t))
	if value != nil {
		builder = builder.WithObjects(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "dev-kubeconfig",
				Annotations: map[string]string{controlplanev1.KubeconfigClusterIDAnnotation: "iksclusterid00000001"}},
			Type: clusterv1.ClusterSecretType,
			Data: map[string][]byte{"value": value},
		})
	}
	params.Client = builder.Build()
	params.APIReader = params.Client
	params.Prober = prober
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "dev"}}
	cp := &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "dev"},
		Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{Distribution: distribution}}
	scope, err := NewControlPlaneScope(params, cluster, cp, cpNoPersist)
	g.Expect(err).NotTo(HaveOccurred())
	identity := credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}
	return &ownershipFixture{scope: scope, clients: &controlPlaneClients{identity: identity, kubernetes: kube}, kube: kube, prober: prober}
}

// ownershipCandidate is the candidate cluster of the proof.
func ownershipCandidate() *kubernetesservice.Cluster {
	return &kubernetesservice.Cluster{ID: "iksclusterid00000001", MasterURL: ownershipCandidateURL}
}

// movedKubeconfig is a kubeconfig of cert whose server is not the candidate's master URL: the proof must ignore it.
// t is the test; cert is the credential and its CA.
func movedKubeconfig(t *testing.T, cert cpTestCertificate) []byte {
	t.Helper()
	value, err := managedkubeconfig.Build(managedkubeconfig.Input{ClusterName: "dev", Server: "https://forged.example.com:6443",
		Trust: managedkubeconfig.ClusterCA, CertificateAuthorityPEM: cert.caPEM, Credential: cert.credential})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return value
}

// tokenKubeconfig is a kubeconfig whose user holds a bearer token, as a user-provided Secret may.
func tokenKubeconfig(t *testing.T) []byte {
	t.Helper()
	cfg := clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"dev": {Server: ownershipCandidateURL}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"dev-admin": {Token: "synthetic-token"}},
		Contexts:       map[string]*clientcmdapi.Context{"dev": {Cluster: "dev", AuthInfo: "dev-admin"}},
		CurrentContext: "dev",
	}
	value, err := clientcmd.Write(cfg)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return value
}

func TestProveOwnership(t *testing.T) {
	notBefore := time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)
	cert := newCPTestCertificate(t, "KubeCert#<synthetic-iam-id-of-another-caller>", notBefore, notBefore.AddDate(2, 0, 0))
	caAnswer := &kubernetesservice.CACertificate{PEM: cert.caPEM}
	admin := workloadprobe.Result{Username: "KubeCert#<synthetic-iam-id-of-another-caller>", Groups: []string{"ibm-admins"},
		GitVersion: "v1.36.5+IKS", ClusterAdmin: true}
	tlsFailure := &workloadprobe.ProbeError{Reason: workloadprobe.FailureTLS, Step: "SelfSubjectReview", Err: errors.New("x509")}

	testCases := []struct {
		name         string
		distribution controlplanev1.Distribution
		value        []byte
		expect       func(f *ownershipFixture)
		answer       func(kubeconfig []byte) (workloadprobe.Result, error)
		wantProof    OwnershipProof
		wantFailure  string
		wantProbes   int
		wantErr      string
	}{
		{
			name: "an admin certificate of another identity proves ownership by the access review", distribution: controlplanev1.DistributionKubernetes,
			value: movedKubeconfig(t, cert),
			expect: func(f *ownershipFixture) {
				f.kube.EXPECT().GetCACertificate(gomock.Any(), "iksclusterid00000001").Return(caAnswer, nil)
			},
			answer: func(kubeconfig []byte) (workloadprobe.Result, error) {
				// The kubeconfig is rebuilt with the candidate's master URL and the getCACert CA, never the Secret's server.
				info, err := managedkubeconfig.Inspect(kubeconfig)
				if err != nil || info.Server != ownershipCandidateURL || info.Trust != managedkubeconfig.ClusterCA {
					return workloadprobe.Result{}, errors.New("the proof must use the candidate's master URL with the cluster CA")
				}
				return admin, nil
			},
			wantProof: OwnershipProofProven, wantProbes: 1,
		},
		{
			name: "access review denied is not proof", distribution: controlplanev1.DistributionKubernetes, value: movedKubeconfig(t, cert),
			expect: func(f *ownershipFixture) {
				f.kube.EXPECT().GetCACertificate(gomock.Any(), "iksclusterid00000001").Return(caAnswer, nil)
			},
			answer: func([]byte) (workloadprobe.Result, error) {
				denied := admin
				denied.ClusterAdmin = false
				return denied, nil
			},
			wantProof: OwnershipProofFailed, wantFailure: "the SelfSubjectAccessReview for verb * on resource * in group * was not allowed",
			wantProbes: 1,
		},
		{
			name: "an expired credential is not proof", distribution: controlplanev1.DistributionKubernetes, value: movedKubeconfig(t, cert),
			expect: func(f *ownershipFixture) {
				f.kube.EXPECT().GetCACertificate(gomock.Any(), "iksclusterid00000001").Return(caAnswer, nil)
			},
			answer: func([]byte) (workloadprobe.Result, error) {
				return workloadprobe.Result{}, &workloadprobe.ProbeError{Reason: workloadprobe.FailureUnauthorized, Step: "SelfSubjectReview",
					Err: errors.New("401")}
			},
			wantProof: OwnershipProofFailed, wantFailure: "SelfSubjectReview failed (Unauthorized)", wantProbes: 1,
		},
		{
			name: "IKS cluster CA fails TLS, the system roots pass", distribution: controlplanev1.DistributionKubernetes, value: movedKubeconfig(t, cert),
			expect: func(f *ownershipFixture) {
				f.kube.EXPECT().GetCACertificate(gomock.Any(), "iksclusterid00000001").Return(caAnswer, nil)
			},
			answer: func(kubeconfig []byte) (workloadprobe.Result, error) {
				if info, err := managedkubeconfig.Inspect(kubeconfig); err == nil && info.Trust == managedkubeconfig.ClusterCA {
					return workloadprobe.Result{}, tlsFailure
				}
				return admin, nil
			},
			wantProof: OwnershipProofProven, wantProbes: 2,
		},
		{
			name: "ROKS proves with the system roots and fetches no CA", distribution: controlplanev1.DistributionOpenShift,
			value: movedKubeconfig(t, cert), answer: func([]byte) (workloadprobe.Result, error) { return admin, nil },
			wantProof: OwnershipProofProven, wantProbes: 1,
		},
		{
			name: "no trust mode verifies", distribution: controlplanev1.DistributionOpenShift, value: movedKubeconfig(t, cert),
			expect: func(f *ownershipFixture) {
				f.kube.EXPECT().GetCACertificate(gomock.Any(), "iksclusterid00000001").Return(caAnswer, nil)
			},
			answer:    func([]byte) (workloadprobe.Result, error) { return workloadprobe.Result{}, tlsFailure },
			wantProof: OwnershipProofFailed, wantProbes: 2,
			wantFailure: "the server certificate of " + ownershipCandidateURL + " did not verify with the cluster CA or the system roots",
		},
		{
			name: "a bearer token cannot be proven and nothing is called", distribution: controlplanev1.DistributionKubernetes,
			value: tokenKubeconfig(t), wantProof: OwnershipProofFailed,
			wantFailure: "the Secret's kubeconfig user holds no inline client certificate and key; a bearer token, an exec plugin or an " +
				"auth-provider cannot be rebuilt for the cluster's master URL, so the proof needs a client-certificate kubeconfig " +
				"(ibmcloud ks cluster config --admin)",
		},
		{
			name: "a value that is not a kubeconfig", distribution: controlplanev1.DistributionKubernetes, value: []byte("{"),
			wantProof: OwnershipProofFailed, wantFailure: "the Secret's value is not a kubeconfig whose current context names a user",
		},
		{
			name: "no Secret", distribution: controlplanev1.DistributionKubernetes, wantProof: OwnershipProofFailed,
			wantFailure: "there is no Secret dev-kubeconfig",
		},
		{
			name: "getCACert fails", distribution: controlplanev1.DistributionKubernetes, value: movedKubeconfig(t, cert),
			expect: func(f *ownershipFixture) {
				f.kube.EXPECT().GetCACertificate(gomock.Any(), "iksclusterid00000001").
					Return(nil, &kubernetesservice.APIError{Operation: kubernetesservice.OpGetCACertificate, StatusCode: http.StatusBadGateway})
			},
			wantErr: "fetching the CA of IBM cluster iksclusterid00000001 for the ownership proof",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newOwnershipFixture(t, tc.distribution, tc.value)
			if tc.expect != nil {
				tc.expect(f)
			}
			f.prober.answer = tc.answer

			proof, failure, err := f.scope.proveOwnership(context.Background(), f.clients, ownershipCandidate())

			if tc.wantErr != "" {
				var callErr *controlPlaneCallError
				g.Expect(errors.As(err, &callErr)).To(BeTrue())
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(proof).To(Equal(tc.wantProof))
			g.Expect(failure).To(Equal(tc.wantFailure))
			g.Expect(f.prober.probed).To(HaveLen(tc.wantProbes))
			g.Expect(failure).NotTo(ContainSubstring("KubeCert"), "the user name never reaches a message")
		})
	}
}

func TestIBMClusterNotVisibleMessage(t *testing.T) {
	testCases := []struct {
		name    string
		account string
		want    string
	}{
		{name: "names the identity by its description and the recorded account", account: "a1b2c3d4e5f60718293a4b5c6d7e8f90",
			want: "The credentials of the CAPIBM controller identity cannot see IBM cluster iksclusterid00000001 in account " +
				"a1b2c3d4e5f60718293a4b5c6d7e8f90, but its kubeconfig Secret still authenticates against it; check the account and " +
				"IAM scope of those credentials. CAPIBM keeps the finalizer ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io " +
				"and never deletes a cluster it cannot see; remove the finalizer by hand to orphan the IBM cluster"},
		{name: "without a recorded account", want: "The credentials of the CAPIBM controller identity cannot see IBM cluster " +
			"iksclusterid00000001 in its IBM Cloud account, but its kubeconfig Secret still authenticates against it; check the " +
			"account and IAM scope of those credentials. CAPIBM keeps the finalizer " +
			"ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io and never deletes a cluster it cannot see; remove the " +
			"finalizer by hand to orphan the IBM cluster"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := IBMClusterNotVisibleMessage(credentials.ControllerDescription, "iksclusterid00000001", tc.account)
			g.Expect(got).To(Equal(tc.want))
			g.Expect(got).NotTo(MatchRegexp(`(?i)ibmid-|iam-serviceid-|@|KubeCert`), "no IAM ID, e-mail or certificate subject")
		})
	}
}

func TestIBMClusterNotCheckedMessage(t *testing.T) {
	g := NewWithT(t)
	got := IBMClusterNotCheckedMessage(credentials.ControllerDescription, "iksclusterid00000001", "a1b2c3d4e5f60718293a4b5c6d7e8f90",
		"SelfSubjectReview failed (Unreachable)")
	g.Expect(got).To(Equal("The credentials of the CAPIBM controller identity cannot see IBM cluster iksclusterid00000001 in account " +
		"a1b2c3d4e5f60718293a4b5c6d7e8f90; its kubeconfig Secret authenticated against it before but cannot be checked now " +
		"(SelfSubjectReview failed (Unreachable)); check the account and IAM scope of those credentials. CAPIBM keeps the " +
		"finalizer ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io and never deletes a cluster it cannot see; remove " +
		"the finalizer by hand to orphan the IBM cluster"))
	g.Expect(got).NotTo(MatchRegexp(`(?i)ibmid-|iam-serviceid-|@|KubeCert`), "a credential message names no IAM ID, e-mail address or certificate subject")
	g.Expect(IBMClusterNotCheckedMessage(credentials.ControllerDescription, "c1", "", "x")).To(ContainSubstring(" in its IBM Cloud account;"))
}

func TestDecideNotFoundProbe(t *testing.T) {
	testCases := []struct {
		name            string
		failure         workloadprobe.FailureReason
		deleteRequested bool
		seen            bool
		withinGrace     bool
		want            NotFoundProbeVerdict
	}{
		{name: "a 401 means the kubeconfig no longer authenticates", failure: workloadprobe.FailureUnauthorized, seen: true,
			withinGrace: true, want: NotFoundProbeGone},
		{name: "a server certificate that no longer verifies", failure: workloadprobe.FailureTLS, withinGrace: true, want: NotFoundProbeGone},
		{name: "a value that is not a kubeconfig", failure: workloadprobe.FailureInvalidKubeconfig, withinGrace: true, want: NotFoundProbeGone},
		{name: "unreachable within the grace period is checked again", failure: workloadprobe.FailureUnreachable, withinGrace: true,
			want: NotFoundProbeRecheck},
		{name: "a 5xx or 429 of the API server within the grace period is checked again", failure: workloadprobe.FailureOther,
			withinGrace: true, want: NotFoundProbeRecheck},
		{name: "unreachable after the grace period without an earlier authentication", failure: workloadprobe.FailureUnreachable,
			want: NotFoundProbeGone},
		{name: "unreachable after IBMClusterNotVisible keeps the finalizer past the grace period", failure: workloadprobe.FailureUnreachable,
			seen: true, want: NotFoundProbeStillNotVisible},
		{name: "unreachable after CAPIBM's own DELETE is the expected sign of a removed master", failure: workloadprobe.FailureUnreachable,
			deleteRequested: true, seen: true, withinGrace: true, want: NotFoundProbeGone},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(DecideNotFoundProbe(tc.failure, tc.deleteRequested, tc.seen, tc.withinGrace)).To(Equal(tc.want))
		})
	}
}

func TestRecordedAccount(t *testing.T) {
	for _, tc := range []struct {
		name, value, want string
	}{
		{name: "an account ID", value: "a1b2c3d4e5f60718293a4b5c6d7e8f90", want: "a1b2c3d4e5f60718293a4b5c6d7e8f90"},
		{name: "absent"},
		{name: "a forged value with separators is not quoted", value: "x; delete everything"},
		{name: "the longest value quoted", value: strings.Repeat("a", maxRecordedAccountLength), want: strings.Repeat("a", maxRecordedAccountLength)},
		// A long alphanumeric value would push the Deleting message past the CRD's 32768-character limit on a condition
		// message, and every status patch of the deleting control plane would fail.
		{name: "a forged value longer than any account ID is not quoted", value: strings.Repeat("a", 40000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{KubeconfigAccountIDAnnotation: tc.value}}}
			NewWithT(t).Expect(recordedAccount(s)).To(Equal(tc.want))
		})
	}
}

func TestStoredKubeconfigFor(t *testing.T) {
	cp := &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev"}}
	owned := func(clusterID string, value []byte, secretType corev1.SecretType) *corev1.Secret {
		return &corev1.Secret{Type: secretType, Data: map[string][]byte{"value": value},
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{controlplanev1.KubeconfigClusterIDAnnotation: clusterID}}}
	}
	for _, tc := range []struct {
		name      string
		secret    *corev1.Secret
		clusterID string
		want      bool
	}{
		{name: "CAPIBM owned for the known ID", secret: owned("c1", []byte("x"), clusterv1.ClusterSecretType), clusterID: "c1", want: true},
		{name: "recorded for another cluster", secret: owned("c2", []byte("x"), clusterv1.ClusterSecretType), clusterID: "c1"},
		{name: "user-provided", secret: owned("c1", []byte("x"), corev1.SecretTypeOpaque), clusterID: "c1"},
		{name: "no value", secret: owned("c1", nil, clusterv1.ClusterSecretType), clusterID: "c1"},
		{name: "no known ID", secret: owned("", []byte("x"), clusterv1.ClusterSecretType)},
		{name: "no Secret", clusterID: "c1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(storedKubeconfigFor(tc.secret, cp, tc.clusterID)).To(Equal(tc.want))
		})
	}
}

func TestProveOwnershipNeedsAUsableMasterURL(t *testing.T) {
	g := NewWithT(t)
	notBefore := time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)
	f := newOwnershipFixture(t, controlplanev1.DistributionKubernetes,
		movedKubeconfig(t, newCPTestCertificate(t, "KubeCert#<synthetic-iam-id>", notBefore, notBefore.AddDate(2, 0, 0))))
	candidate := ownershipCandidate()
	candidate.MasterURL = "http://c111.us-south.containers.cloud.ibm.com:30101"

	proof, failure, err := f.scope.proveOwnership(context.Background(), f.clients, candidate)

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(proof).To(Equal(OwnershipProofFailed))
	g.Expect(failure).To(Equal("IBM reports no master URL of the cluster that CAPIBM can use"))
	g.Expect(f.prober.probed).To(BeEmpty(), "the credential is never sent to a URL that is not IBM's https master")
	g.Expect(masterURLProblemText(nil)).To(BeEmpty())
}
