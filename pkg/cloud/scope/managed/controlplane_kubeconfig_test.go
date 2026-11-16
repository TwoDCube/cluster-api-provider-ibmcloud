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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// The kubeconfig scope tests cover the pure parts of the control plane's access steps directly: the endpoint, the
// annotation set, the start of a 401 run, the user-provided Secret checks, the ownership rule and the probe texts. The
// flows (refresh, verify before write, initialization, versions) run through the controller tests
// (internal/controllers/managed/ibmcloudmanagedcontrolplane_kubeconfig_test.go). Test certificates are generated
// in-test; subjects use the shape of IBM's admin certificate (O=ibm-admins, CN=KubeCert#<IAM ID>) with synthetic
// placeholders.

// cpTestProber is a workloadprobe.Prober whose answer a test scripts; it records every probed kubeconfig. Without an
// answer every probe fails, so a test that does not expect a probe notices one.
type cpTestProber struct {
	mu     sync.Mutex
	answer func(kubeconfig []byte) (workloadprobe.Result, error)
	probed [][]byte
}

// Probe implements workloadprobe.Prober.
func (p *cpTestProber) Probe(_ context.Context, kubeconfig []byte) (workloadprobe.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probed = append(p.probed, append([]byte(nil), kubeconfig...))
	if p.answer == nil {
		return workloadprobe.Result{}, errors.New("cpTestProber: no probe expected")
	}
	return p.answer(kubeconfig)
}

// cpTestCertificate is an in-test CA and an admin client credential it issued.
type cpTestCertificate struct {
	caPEM      []byte
	credential managedkubeconfig.ClientCredential
}

// newCPTestCertificate returns a CA and a client credential with subject O=ibm-admins, CN=cn, valid from notBefore to
// notAfter. cn must be a synthetic placeholder, never a real IAM ID.
func newCPTestCertificate(t *testing.T, cn string, notBefore, notAfter time.Time) cpTestCertificate {
	t.Helper()
	g := NewWithT(t)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	g.Expect(err).NotTo(HaveOccurred())
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "iksclusterid00000001-kubernetes-ca"},
		NotBefore: notBefore.Add(-time.Hour), NotAfter: notAfter.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	g.Expect(err).NotTo(HaveOccurred())
	caCert, err := x509.ParseCertificate(caDER)
	g.Expect(err).NotTo(HaveOccurred())
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	g.Expect(err).NotTo(HaveOccurred())
	template := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: cn, Organization: []string{"ibm-admins"}},
		NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	g.Expect(err).NotTo(HaveOccurred())
	keyDER, err := x509.MarshalECPrivateKey(key)
	g.Expect(err).NotTo(HaveOccurred())
	return cpTestCertificate{
		caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		credential: managedkubeconfig.ClientCredential{
			CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			KeyPEM:         pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		},
	}
}

func TestEndpointFromServerURL(t *testing.T) {
	testCases := []struct {
		name    string
		server  string
		want    controlplanev1.APIEndpoint
		wantErr string
	}{
		{name: "IKS master URL with its port", server: "https://c111.us-south.containers.cloud.ibm.com:30101",
			want: controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}},
		{name: "port 443 that ManagementServerURL added", server: "https://c111-e.us-south.containers.cloud.ibm.com:443",
			want: controlplanev1.APIEndpoint{Host: "c111-e.us-south.containers.cloud.ibm.com", Port: 443}},
		{name: "no port", server: "https://c111.us-south.containers.cloud.ibm.com", wantErr: "needs a host and a port"},
		{name: "port 0", server: "https://c111.us-south.containers.cloud.ibm.com:0", wantErr: "needs a host and a port"},
		{name: "no host", server: "https://:443", wantErr: "needs a host and a port"},
		{name: "does not parse", server: "https://c111 .example.com:443", wantErr: "does not parse"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := EndpointFromServerURL(tc.server)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestUserProvidedSecretProblem(t *testing.T) {
	labelled := map[string]string{clusterv1.ClusterNameLabel: "dev"}
	testCases := []struct {
		name   string
		secret *corev1.Secret
		want   []string
	}{
		{name: "label and value", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-kubeconfig", Labels: labelled},
			Data: map[string][]byte{"value": []byte("apiVersion: v1")}}},
		{name: "no cluster-name label", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-kubeconfig"},
			Data: map[string][]byte{"value": []byte("apiVersion: v1")}}, want: []string{"lacks the label cluster.x-k8s.io/cluster-name",
			"never modifies"}},
		{name: "empty value", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-kubeconfig", Labels: labelled},
			Data: map[string][]byte{"value": {}}}, want: []string{`a non-empty "value" key`}},
		{name: "neither label nor value", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-kubeconfig"}},
			want: []string{"cluster.x-k8s.io/cluster-name", `and a non-empty "value" key`}},
		{name: "no Secret", want: []string{"does not exist"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := UserProvidedSecretProblem(tc.secret)
			if len(tc.want) == 0 {
				g.Expect(got).To(BeEmpty())
				return
			}
			for _, want := range tc.want {
				g.Expect(got).To(ContainSubstring(want))
			}
		})
	}
}

func TestUnauthorizedSinceValue(t *testing.T) {
	now := time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)
	earlier := "2026-11-16T02:00:00Z"
	testCases := []struct {
		name         string
		stored       string
		passed       bool
		unauthorized bool
		want         string
	}{
		{name: "first_401_of_a_run_sets_now", unauthorized: true, want: "2026-11-16T03:05:30Z"},
		{name: "second_401_keeps_the_start", stored: earlier, unauthorized: true, want: earlier},
		{name: "successful_probe_removes_it", stored: earlier, passed: true, want: ""},
		{name: "other_failure_keeps_the_run", stored: earlier, want: earlier},
		{name: "no_run_and_no_probe_stays_absent", want: ""},
		{name: "unparseable_value_restarts_the_run", stored: "yesterday", unauthorized: true, want: "2026-11-16T03:05:30Z"},
		{name: "forged_future_value_is_kept_and_bounded_by_Decide", stored: "2030-01-01T00:00:00Z", unauthorized: true,
			want: "2030-01-01T00:00:00Z"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(UnauthorizedSinceValue(tc.stored, tc.passed, tc.unauthorized, now)).To(Equal(tc.want))
		})
	}
}

func TestStoredTLSRecheck(t *testing.T) {
	now := time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)
	backoff := KubeconfigPolicy().AuthFailureBackoffMax
	testCases := []struct {
		name      string
		checkedAt time.Time
		wantDue   bool
		wantWait  time.Duration
	}{
		{name: "no attempt recorded: due at once", wantDue: true},
		{name: "the last attempt was four minutes ago: six minutes to wait", checkedAt: now.Add(-4 * time.Minute), wantWait: 6 * time.Minute},
		{name: "the last attempt was exactly the backoff ago: due", checkedAt: now.Add(-backoff), wantDue: true},
		{name: "the last attempt was a day ago: due", checkedAt: now.Add(-24 * time.Hour), wantDue: true},
		{name: "a forged future time counts as absent: due", checkedAt: now.Add(time.Hour), wantDue: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			due, wait := StoredTLSRecheck(tc.checkedAt, now, backoff)
			g.Expect(due).To(Equal(tc.wantDue))
			g.Expect(wait).To(Equal(tc.wantWait))
		})
	}
}

func TestPlanTLSRecheck(t *testing.T) {
	now := time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)
	policy := KubeconfigPolicy()
	testCases := []struct {
		name        string
		plan        kubeconfigPlan
		checkedAt   time.Time
		wantHeld    bool
		wantCA      bool
		wantRequeue time.Duration
	}{
		{name: "due_adds_a_CA_re_check_and_looks_again_after_MinRequeue", plan: kubeconfigPlan{Decision: managedkubeconfig.Decision{RequeueAfter: 12 * time.Hour}},
			wantCA: true, wantRequeue: time.Minute},
		{name: "held_by_a_recent_attempt_requeues_when_it_is_due", plan: kubeconfigPlan{Decision: managedkubeconfig.Decision{RequeueAfter: 12 * time.Hour}},
			checkedAt: now.Add(-3 * time.Minute), wantHeld: true, wantRequeue: 7 * time.Minute},
		{name: "held_keeps_an_earlier_deadline_of_Decide", plan: kubeconfigPlan{Decision: managedkubeconfig.Decision{RequeueAfter: 2 * time.Minute}},
			checkedAt: now.Add(-3 * time.Minute), wantHeld: true, wantRequeue: 2 * time.Minute},
		{name: "a_refresh_of_everything_already_fetches_the_CA", plan: kubeconfigPlan{Decision: managedkubeconfig.Decision{Refresh: true,
			RefreshCA: true, RequeueAfter: time.Minute}}, wantCA: true, wantRequeue: time.Minute},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			plan := tc.plan
			g.Expect(planTLSRecheck(&plan, tc.checkedAt, now, policy)).To(Equal(tc.wantHeld))
			g.Expect(plan.RefreshCA).To(Equal(tc.wantCA))
			g.Expect(plan.RequeueAfter).To(Equal(tc.wantRequeue))
		})
	}
}

func TestRefreshHeld(t *testing.T) {
	now := time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)
	policy := KubeconfigPolicy()
	desired := managedkubeconfig.Desired{Server: "https://c111.us-south.containers.cloud.ibm.com:30101"}
	// kubeconfigOfAge describes a stored certificate issued age ago and valid for two years, like IBM's admin certificate,
	// with SystemRoots trust (no CA rule).
	kubeconfigOfAge := func(age time.Duration) *managedkubeconfig.Info {
		notBefore := now.Add(-age)
		return &managedkubeconfig.Info{Server: desired.Server, Trust: managedkubeconfig.SystemRoots,
			Certificate: managedkubeconfig.CertificateInfo{Fingerprint: "sha256:00", NotBefore: notBefore, NotAfter: notBefore.AddDate(2, 0, 0)}}
	}
	aging := kubeconfigOfAge(366 * 24 * time.Hour)
	testCases := []struct {
		name    string
		current managedkubeconfig.Current
		want    bool
	}{
		{name: "aging certificate within SameCertificateRetry of the last attempt", current: managedkubeconfig.Current{
			Kubeconfig: aging, RefreshCheckedAt: now.Add(-time.Hour)}, want: true},
		{name: "401 run within its backoff", current: managedkubeconfig.Current{Kubeconfig: kubeconfigOfAge(time.Hour),
			RefreshCheckedAt: now.Add(-30 * time.Second), LastProbe: managedkubeconfig.ProbeStatus{Unauthorized: true,
				UnauthorizedSince: now.Add(-time.Minute)}}, want: true},
		{name: "no attempt recorded", current: managedkubeconfig.Current{Kubeconfig: aging}},
		{name: "a fresh certificate has no rule to hold", current: managedkubeconfig.Current{Kubeconfig: kubeconfigOfAge(time.Hour),
			RefreshCheckedAt: now.Add(-time.Hour)}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			held, err := refreshHeld(now, desired, tc.current, policy)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(held).To(Equal(tc.want))
		})
	}
}

func TestKubeconfigSecretAnnotations(t *testing.T) {
	notBefore := time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)
	fetched := time.Date(2026, 11, 16, 3, 41, 0, 0, time.UTC)
	cert := managedkubeconfig.CertificateInfo{Fingerprint: "sha256:aa", NotBefore: notBefore, NotAfter: notBefore.AddDate(2, 0, 0)}
	ca := &managedkubeconfig.CertificateInfo{Fingerprint: "sha256:bb", NotBefore: notBefore, NotAfter: notBefore.AddDate(10, 0, 0)}
	iks := KubeconfigAnnotationInput{ClusterID: "iksclusterid00000001", AccountID: "a1b2c3d4e5f60718293a4b5c6d7e8f90",
		Info: managedkubeconfig.Info{Server: "https://c111.us-south.containers.cloud.ibm.com:30101", Trust: managedkubeconfig.ClusterCA,
			CA: ca, Certificate: cert}, FetchedAt: fetched, CAFetchedAt: fetched}

	t.Run("ClusterCA_records_every_annotation_and_no_secret_material", func(t *testing.T) {
		g := NewWithT(t)
		got, err := KubeconfigSecretAnnotations(iks)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).To(Equal(map[string]string{ //nolint:gosec // G101: annotation keys and synthetic values, no credential
			controlplanev1.KubeconfigClusterIDAnnotation: "iksclusterid00000001",
			KubeconfigAccountIDAnnotation:                "a1b2c3d4e5f60718293a4b5c6d7e8f90",
			KubeconfigServerAnnotation:                   "https://c111.us-south.containers.cloud.ibm.com:30101",
			KubeconfigCredentialTypeAnnotation:           "ClientCertificate",
			KubeconfigCredentialNotBeforeAnnotation:      "2026-11-16T03:40:08Z",
			KubeconfigCredentialNotAfterAnnotation:       "2028-11-16T03:40:08Z",
			KubeconfigCredentialFingerprintAnnotation:    "sha256:aa",
			KubeconfigFetchedAtAnnotation:                "2026-11-16T03:41:00Z",
			KubeconfigCAModeAnnotation:                   "ClusterCA",
			KubeconfigCAFingerprintAnnotation:            "sha256:bb",
			KubeconfigCANotAfterAnnotation:               "2036-11-16T03:40:08Z",
			KubeconfigCAFetchedAtAnnotation:              "2026-11-16T03:41:00Z",
		}))
		for key := range got {
			g.Expect(kubeconfigOwnedAnnotations()).To(ContainElement(key), "only annotations CAPIBM owns, none that names the identity behind the credential")
		}
	})

	t.Run("SystemRoots_has_no_CA_annotations_and_carries_the_marker_and_the_check", func(t *testing.T) {
		g := NewWithT(t)
		roks := iks
		roks.Info.Trust, roks.Info.CA, roks.CAFetchedAt = managedkubeconfig.SystemRoots, nil, time.Time{}
		roks.AccountID = ""
		roks.RefreshCheckedAt = fetched
		roks.CARotationMarker = "CA certificate rotation complete|2026-11-01"
		got, err := KubeconfigSecretAnnotations(roks)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).NotTo(HaveKey(KubeconfigCAFingerprintAnnotation))
		g.Expect(got).NotTo(HaveKey(KubeconfigCAFetchedAtAnnotation))
		g.Expect(got).NotTo(HaveKey(KubeconfigAccountIDAnnotation), "an account the CRN does not name is left out")
		g.Expect(got).To(HaveKeyWithValue(KubeconfigCAModeAnnotation, "SystemRoots"))
		g.Expect(got).To(HaveKeyWithValue(KubeconfigRefreshCheckedAtAnnotation, "2026-11-16T03:41:00Z"))
		g.Expect(got).To(HaveKeyWithValue(KubeconfigCARotationMarkerAnnotation, "CA certificate rotation complete|2026-11-01"))
		g.Expect(got).NotTo(HaveKey(KubeconfigUnauthorizedSinceAnnotation), "a new kubeconfig passed a probe, which ends a 401 run")
	})

	for _, tc := range []struct {
		name   string
		modify func(in *KubeconfigAnnotationInput)
		want   string
	}{
		{name: "no cluster ID", modify: func(in *KubeconfigAnnotationInput) { in.ClusterID = "" }, want: "ClusterID"},
		{name: "no server", modify: func(in *KubeconfigAnnotationInput) { in.Info.Server = "" }, want: "no server or trust mode"},
		{name: "unknown trust", modify: func(in *KubeconfigAnnotationInput) { in.Info.Trust = "Insecure" }, want: "no server or trust mode"},
		{name: "no fetch time", modify: func(in *KubeconfigAnnotationInput) { in.FetchedAt = time.Time{} }, want: "FetchedAt"},
		{name: "ClusterCA without the CA", modify: func(in *KubeconfigAnnotationInput) { in.Info.CA = nil }, want: "Info.CA and CAFetchedAt"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			in := iks
			tc.modify(&in)
			_, err := KubeconfigSecretAnnotations(in)
			NewWithT(t).Expect(err).To(MatchError(ContainSubstring(tc.want)))
		})
	}
}

func TestKubeconfigSecretOwned(t *testing.T) {
	cp := &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", UID: "cp-uid"}}
	controller := func(kind, name, uid string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{APIVersion: controlplanev1.GroupVersion.String(), Kind: kind, Name: name, UID: k8stypes.UID(uid),
			Controller: ptr.To(true)}}
	}
	testCases := []struct {
		name   string
		secret *corev1.Secret
		want   bool
	}{
		{name: "cluster secret type without an owner (KCP's rule)", secret: &corev1.Secret{Type: clusterv1.ClusterSecretType}, want: true},
		{name: "controlled by this control plane", secret: &corev1.Secret{Type: clusterv1.ClusterSecretType,
			ObjectMeta: metav1.ObjectMeta{OwnerReferences: controller(controlPlaneKind, "dev", "cp-uid")}}, want: true},
		{name: "controlled by another object", secret: &corev1.Secret{Type: clusterv1.ClusterSecretType,
			ObjectMeta: metav1.ObjectMeta{OwnerReferences: controller("KubeadmControlPlane", "dev", "other-uid")}}},
		{name: "another type is user provided", secret: &corev1.Secret{Type: corev1.SecretTypeOpaque}},
		{name: "no type is user provided", secret: &corev1.Secret{}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(kubeconfigSecretOwned(tc.secret, cp)).To(Equal(tc.want))
		})
	}
}

func TestReadStoredKubeconfig(t *testing.T) {
	notBefore := time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)
	cert := newCPTestCertificate(t, "KubeCert#<synthetic-iam-id>", notBefore, notBefore.AddDate(2, 0, 0))
	value, err := managedkubeconfig.Build(managedkubeconfig.Input{ClusterName: "dev", Server: "https://c111.us-south.containers.cloud.ibm.com:30101",
		Trust: managedkubeconfig.ClusterCA, CertificateAuthorityPEM: cert.caPEM, Credential: cert.credential})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	secretOf := func(clusterID string, data []byte) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			controlplanev1.KubeconfigClusterIDAnnotation: clusterID,
			KubeconfigRefreshCheckedAtAnnotation:         "2026-11-17T00:00:00Z",
			KubeconfigCAFetchedAtAnnotation:              "not-a-time",
			KubeconfigUnauthorizedSinceAnnotation:        "2026-11-16T05:00:00Z",
			KubeconfigCARotationMarkerAnnotation:         "m|d",
		}}, Data: map[string][]byte{"value": data}}
	}

	t.Run("the bound cluster's kubeconfig is described", func(t *testing.T) {
		g := NewWithT(t)
		stored := readStoredKubeconfig(secretOf("iksclusterid00000001", value), "iksclusterid00000001")
		g.Expect(stored.info).NotTo(BeNil())
		g.Expect(stored.info.Trust).To(Equal(managedkubeconfig.ClusterCA))
		g.Expect(stored.refreshCheckedAt).To(Equal(time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)))
		g.Expect(stored.caFetchedAt.IsZero()).To(BeTrue(), "an unparseable time counts as absent")
		g.Expect(stored.unauthorizedSince).To(Equal("2026-11-16T05:00:00Z"))
		g.Expect(stored.marker).To(Equal("m|d"))
	})
	t.Run("a Secret recorded for another cluster is treated as missing", func(t *testing.T) {
		NewWithT(t).Expect(readStoredKubeconfig(secretOf("iksclusterid00000002", value), "iksclusterid00000001").info).To(BeNil())
	})
	t.Run("a value Inspect rejects is treated as missing", func(t *testing.T) {
		NewWithT(t).Expect(readStoredKubeconfig(secretOf("iksclusterid00000001", []byte("not a kubeconfig")), "iksclusterid00000001").info).To(BeNil())
	})
	t.Run("no Secret", func(t *testing.T) {
		NewWithT(t).Expect(readStoredKubeconfig(nil, "iksclusterid00000001")).To(BeZero())
	})
}

func TestProbeFailureTextHasNoServerText(t *testing.T) {
	// The API server's 403 text names the authenticated user (KubeCert#<caller-iam-id> for IBM's admin certificate); the
	// prober withholds it, and the condition text names only the step and the class.
	const marker = "server-body-marker-7f3a"
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "selfsubjectaccessreviews"}, "", fmt.Errorf("user %s", marker))
	testCases := []struct {
		name       string
		err        error
		wantText   string
		wantReason string
	}{
		{name: "unauthorized", err: &workloadprobe.ProbeError{Reason: workloadprobe.FailureUnauthorized, Step: "SelfSubjectReview",
			Err: apierrors.NewUnauthorized(marker)}, wantText: "SelfSubjectReview failed (Unauthorized)",
			wantReason: controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason},
		{name: "forbidden", err: &workloadprobe.ProbeError{Reason: workloadprobe.FailureOther, Step: "SelfSubjectAccessReview", Err: forbidden},
			wantText: "SelfSubjectAccessReview failed (Other)", wantReason: controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason},
		{name: "unreachable", err: &workloadprobe.ProbeError{Reason: workloadprobe.FailureUnreachable, Step: "SelfSubjectReview",
			Err: fmt.Errorf("dial tcp 10.0.0.1:%d: %s", http.StatusOK, marker)}, wantText: "SelfSubjectReview failed (Unreachable)",
			wantReason: controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason},
		{name: "not a probe error", err: errors.New(marker), wantText: "the probe failed",
			wantReason: controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			text := probeFailureText(tc.err)
			g.Expect(text).To(Equal(tc.wantText))
			g.Expect(text).NotTo(ContainSubstring(marker))
			g.Expect(kubeconfigProbeReason(tc.err)).To(Equal(tc.wantReason))
		})
	}
}

func TestKubeconfigOwnedAnnotationsShareThePrefix(t *testing.T) {
	g := NewWithT(t)
	for _, key := range kubeconfigOwnedAnnotations() {
		g.Expect(strings.HasPrefix(key, controlplanev1.KubeconfigAnnotationPrefix)).To(BeTrue(), key)
	}
	g.Expect(kubeconfigOwnedAnnotations()).To(HaveLen(15), "CAPIBM owns fifteen kubeconfig annotations")
}
