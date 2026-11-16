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
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// The kubeconfig tests run the control plane's endpoint, kubeconfig, initialization and version steps through
// Reconcile with the fake-client fixture of ibmcloudmanagedcontrolplane_controller_test.go: a gomock Kubernetes Service
// client (GetAdminKubeconfig and GetCACertificate), a scripted prober that answers by what a kubeconfig holds (server,
// trust mode, client certificate), and in-test certificates. gomock fails a test on every IBM call it does not expect,
// so "no credential fetched" is the absence of an expectation. An envtest test at the end writes the Secret to the
// real API server.

// cpTestServer is the master URL of cpCluster and cpTestEndpoint its endpoint.
const cpTestServer = "https://c111.us-south.containers.cloud.ibm.com:30101"

// cpTestEndpoint is the endpoint a bound test control plane recorded.
var cpTestEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}

// cpTestAccount is the account of cpTestCRN.
const cpTestAccount = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

// cpTestAdminCN is the subject common name of the in-test admin certificates: the shape of IBM's (KubeCert#<IAM ID>)
// with a synthetic placeholder, never an IAM ID, so that no real identity reaches a test or a message.
const cpTestAdminCN = "KubeCert#<synthetic-iam-id>"

// testClusterCA is an in-test cluster CA that issues admin client certificates.
type testClusterCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

// newTestClusterCA returns a CA valid for ten years up to notAfter, like the cluster CA IBM issues. t is the test.
func newTestClusterCA(t *testing.T, notAfter time.Time) *testClusterCA {
	t.Helper()
	g := NewWithT(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	g.Expect(err).NotTo(HaveOccurred())
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cpIKSClusterID + "-kubernetes-ca"},
		NotBefore: notAfter.AddDate(-10, 0, 0), NotAfter: notAfter, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	g.Expect(err).NotTo(HaveOccurred())
	cert, err := x509.ParseCertificate(der)
	g.Expect(err).NotTo(HaveOccurred())
	return &testClusterCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issueAdmin returns an admin client credential (O=ibm-admins, CN=cn) valid from notBefore to notAfter.
//
// Parameters:
//   - t is the test.
//   - cn is the subject common name, a synthetic placeholder.
//   - notBefore and notAfter bound its validity.
func (ca *testClusterCA) issueAdmin(t *testing.T, cn string, notBefore, notAfter time.Time) managedkubeconfig.ClientCredential {
	t.Helper()
	g := NewWithT(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	g.Expect(err).NotTo(HaveOccurred())
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn, Organization: []string{"ibm-admins"}},
		NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	g.Expect(err).NotTo(HaveOccurred())
	keyDER, err := x509.MarshalECPrivateKey(key)
	g.Expect(err).NotTo(HaveOccurred())
	return managedkubeconfig.ClientCredential{
		CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:         pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// kubeconfigCredentials holds the in-test CA and the admin credential IBM serves in one test.
type kubeconfigCredentials struct {
	ca    *testClusterCA
	admin managedkubeconfig.ClientCredential
}

// newKubeconfigCredentials returns a CA valid for ten years and a two-year admin certificate issued issuedAgo before
// cpTestStart (IBM's admin certificate is valid two years from its first fetch). t is the test; issuedAgo is the
// certificate's age.
func newKubeconfigCredentials(t *testing.T, issuedAgo time.Duration) kubeconfigCredentials {
	t.Helper()
	ca := newTestClusterCA(t, cpTestStart.AddDate(9, 0, 0))
	notBefore := cpTestStart.Add(-issuedAgo)
	return kubeconfigCredentials{ca: ca, admin: ca.issueAdmin(t, cpTestAdminCN, notBefore, notBefore.AddDate(2, 0, 0))}
}

// adminAnswer is IBM's admin kubeconfig answer (GetAdminKubeconfig) for creds: the IKS shape carries the CA, the ROKS
// one none, as observed on IBM Cloud.
//
// Parameters:
//   - creds are the credentials.
//   - withCA selects the IKS shape.
func adminAnswer(creds kubeconfigCredentials, withCA bool) *kubernetesservice.AdminKubeconfig {
	answer := &kubernetesservice.AdminKubeconfig{Server: cpTestServer, CertificatePEM: creds.admin.CertificatePEM, KeyPEM: creds.admin.KeyPEM}
	if withCA {
		answer.CertificateAuthorityPEM = creds.ca.pem
	}
	return answer
}

// buildTestKubeconfig returns the kubeconfig CAPIBM builds for creds with trust at server.
//
// Parameters:
//   - t is the test.
//   - server is the API server URL.
//   - trust is the trust mode.
//   - creds are the credentials.
func buildTestKubeconfig(t *testing.T, server string, trust managedkubeconfig.CATrust, creds kubeconfigCredentials) []byte {
	t.Helper()
	in := managedkubeconfig.Input{ClusterName: cpTestName, Server: server, Trust: trust, Credential: creds.admin}
	if trust == managedkubeconfig.ClusterCA {
		in.CertificateAuthorityPEM = creds.ca.pem
	}
	value, err := managedkubeconfig.Build(in)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return value
}

// ownedKubeconfigSecret returns the CAPIBM-owned Secret of value with the annotations CAPIBM writes, fetched at
// fetchedAt, plus extra annotations.
//
// Parameters:
//   - t is the test.
//   - value is the kubeconfig.
//   - fetchedAt is when it was fetched.
//   - extra are further annotations, written over the computed ones.
func ownedKubeconfigSecret(t *testing.T, value []byte, fetchedAt time.Time, extra map[string]string) *corev1.Secret {
	t.Helper()
	g := NewWithT(t)
	info, err := managedkubeconfig.Inspect(value)
	g.Expect(err).NotTo(HaveOccurred())
	annotations, err := managedscope.KubeconfigSecretAnnotations(managedscope.KubeconfigAnnotationInput{ClusterID: cpIKSClusterID,
		AccountID: cpTestAccount, Info: info, FetchedAt: fetchedAt, CAFetchedAt: fetchedAt})
	g.Expect(err).NotTo(HaveOccurred())
	for key, v := range extra {
		annotations[key] = v
	}
	owned := kubeconfig.GenerateSecretWithOwner(client.ObjectKey{Namespace: cpTestNamespace, Name: cpTestName}, value, metav1.OwnerReference{
		APIVersion: controlplanev1.GroupVersion.String(), Kind: "IBMCloudManagedControlPlane", Name: cpTestName,
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)})
	owned.Annotations = annotations
	return owned
}

// probedKubeconfig is what the scripted prober saw in one kubeconfig.
type probedKubeconfig struct {
	// server is the cluster entry's server.
	server string
	// trust is ClusterCA when CA data is embedded.
	trust managedkubeconfig.CATrust
	// certificate is the client certificate's fingerprint.
	certificate string
}

// kubeconfigProber is a workloadprobe.Prober whose answer a test scripts by what a kubeconfig holds. It records every
// probe; with no answer every probe fails the test.
type kubeconfigProber struct {
	t      *testing.T
	mu     sync.Mutex
	answer func(p probedKubeconfig) (workloadprobe.Result, error)
	probes []probedKubeconfig
}

// Probe implements workloadprobe.Prober.
func (p *kubeconfigProber) Probe(_ context.Context, value []byte) (workloadprobe.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	probed := probedKubeconfig{}
	if info, err := managedkubeconfig.Inspect(value); err == nil {
		probed = probedKubeconfig{server: info.Server, trust: info.Trust, certificate: info.Certificate.Fingerprint}
	}
	p.probes = append(p.probes, probed)
	if p.answer == nil {
		p.t.Errorf("Prober.Probe called for %+v, but the test expects no probe", probed)
		return workloadprobe.Result{}, errors.New("kubeconfigProber: unexpected probe")
	}
	return p.answer(probed)
}

// calls returns the probes so far.
func (p *kubeconfigProber) calls() []probedKubeconfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]probedKubeconfig(nil), p.probes...)
}

// setAnswer replaces the scripted answer. answer decides each probe.
func (p *kubeconfigProber) setAnswer(answer func(p probedKubeconfig) (workloadprobe.Result, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.answer = answer
}

// passing is a probe answer of a cluster-admin credential on an API server that reports gitVersion. The user name has
// the shape of IBM's admin certificate and must never surface in status, events, annotations or logs above debug.
// gitVersion is the version.
func passing(gitVersion string) workloadprobe.Result {
	return workloadprobe.Result{Username: cpTestAdminCN, Groups: []string{"ibm-admins", "system:authenticated"},
		GitVersion: gitVersion, ClusterAdmin: true}
}

// probeFailure is a failed probe of class reason in step.
//
// Parameters:
//   - reason is the failure class.
//   - step is the probe step.
func probeFailure(reason workloadprobe.FailureReason, step string) error {
	return &workloadprobe.ProbeError{Reason: reason, Step: step, Err: errors.New("injected " + string(reason))}
}

// alwaysPassing answers every probe with passing(gitVersion). gitVersion is the version.
func alwaysPassing(gitVersion string) func(probedKubeconfig) (workloadprobe.Result, error) {
	return func(probedKubeconfig) (workloadprobe.Result, error) { return passing(gitVersion), nil }
}

// newKubeconfigFixture returns the fixture of a bound IKS control plane whose endpoint is recorded, with the scripted
// prober. setup changes the objects first; t is the test.
func newKubeconfigFixture(t *testing.T, setup func(objs *cpObjects)) (*cpFixture, *kubeconfigProber) {
	t.Helper()
	objs := newCPObjects()
	objs.cp.Status.ClusterID = cpIKSClusterID
	objs.cp.Spec.ControlPlaneEndpoint = cpTestEndpoint
	if setup != nil {
		setup(objs)
	}
	f := newCPFixture(t, objs)
	prober := &kubeconfigProber{t: t}
	f.r.Prober = prober
	return f, prober
}

// kubeconfigSecret returns the persisted kubeconfig Secret, or nil.
func (f *cpFixture) kubeconfigSecret() *corev1.Secret {
	f.t.Helper()
	stored := &corev1.Secret{}
	err := f.client.Get(ctx, types.NamespacedName{Namespace: cpTestNamespace, Name: cpTestName + "-kubeconfig"}, stored)
	if apierrors.IsNotFound(err) {
		return nil
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return stored
}

// expectObserve expects one GetCluster of the bound cluster, answered with c.
func (f *cpFixture) expectObserve(c *kubernetesservice.Cluster) *gomock.Call {
	return f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(c, nil)
}

func TestControlPlaneKubeconfigEndpoint(t *testing.T) {
	t.Run("endpoint_written_on_the_deployed_fixture", func(t *testing.T) {
		g := NewWithT(t)
		deployed := liveCluster(t, "iks-getCluster-deployed.json")
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{}
			objs.cp.Spec.IBMCloudClusterName = deployed.Name
		})
		f.expectObserve(deployed)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Spec.ControlPlaneEndpoint).To(Equal(controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 30101}))
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil(), "never in the reconcile that writes the endpoint")
		g.Expect(f.kubeconfigSecret()).To(BeNil(), "the kubeconfig follows in the next reconcile")
		g.Expect(prober.calls()).To(BeEmpty())
	})

	t.Run("deploying_fixture_writes_no_endpoint", func(t *testing.T) {
		g := NewWithT(t)
		deploying := liveCluster(t, "iks-getCluster-deploying.json")
		f, _ := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{}
			objs.cp.Spec.IBMCloudClusterName = deploying.Name
		})
		f.expectObserve(deploying)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Spec.ControlPlaneEndpoint).To(BeZero())
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyWaitingForMasterReason)
	})

	for _, tc := range []struct {
		name      string
		masterURL string
		want      controlplanev1.APIEndpoint
	}{
		{name: "master_url_without_a_port_gets_443", masterURL: "https://c111-e.us-south.containers.cloud.ibm.com",
			want: controlplanev1.APIEndpoint{Host: "c111-e.us-south.containers.cloud.ibm.com", Port: 443}},
		{name: "master_url_with_a_port_keeps_it", masterURL: "https://c111.us-south.containers.cloud.ibm.com:30123",
			want: controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30123}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f, _ := newKubeconfigFixture(t, func(objs *cpObjects) { objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{} })
			c := cpCluster("normal")
			c.MasterURL = tc.masterURL
			f.expectObserve(c)

			_, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.controlPlane().Spec.ControlPlaneEndpoint).To(Equal(tc.want))
		})
	}

	t.Run("http_master_url_is_terminal_and_writes_nothing", func(t *testing.T) {
		g := NewWithT(t)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{} })
		c := cpCluster("normal")
		c.MasterURL = "http://c111.us-south.containers.cloud.ibm.com:30101"
		f.expectObserve(c)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Spec.ControlPlaneEndpoint).To(BeZero())
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason)
		g.Expect(condition.Message).To(ContainSubstring(`"http://c111.us-south.containers.cloud.ibm.com:30101"`))
		g.Expect(condition.Message).To(ContainSubstring("only over https"))
		g.Expect(f.kubeconfigSecret()).To(BeNil())
		g.Expect(prober.calls()).To(BeEmpty())
	})
}

// liveCluster decodes a scrubbed getCluster capture of pkg/cloud/services/kubernetesservice/testdata into the domain
// type, with the fields the control plane reads. name is the file name.
func liveCluster(t *testing.T, name string) *kubernetesservice.Cluster {
	t.Helper()
	g := NewWithT(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "cloud", "services", "kubernetesservice", "testdata", name)) //nolint:gosec // G304: a fixture of the repository's testdata
	g.Expect(err).NotTo(HaveOccurred())
	var capture struct {
		ID, Name, CRN, State, CreatedDate, MasterKubeVersion, TargetVersion, MasterURL, Type, Provider string
		ResourceGroup                                                                                  string `json:"resourceGroup"`
		Lifecycle                                                                                      struct {
			MasterState, MasterHealth, MasterStatus string
		}
	}
	g.Expect(json.Unmarshal(data, &capture)).To(Succeed())
	return &kubernetesservice.Cluster{ID: capture.ID, Name: capture.Name, CRN: capture.CRN, Type: capture.Type, Provider: capture.Provider,
		ResourceGroupID: capture.ResourceGroup, CreatedDate: capture.CreatedDate, State: capture.State,
		MasterKubeVersion: capture.MasterKubeVersion, TargetVersion: capture.TargetVersion, MasterURL: capture.MasterURL,
		MasterState: capture.Lifecycle.MasterState, MasterHealth: capture.Lifecycle.MasterHealth,
		MasterStatus: strings.TrimSpace(capture.Lifecycle.MasterStatus)}
}

func TestControlPlaneKubeconfigInitialization(t *testing.T) {
	t.Run("initialized_only_after_the_persisted_endpoint_and_a_verified_secret_with_cluster_id", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{} })
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))

		// The reconcile that first sees the master ready records the endpoint and returns.
		f.expectObserve(cpCluster("normal"))
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane().Status.Initialization.ControlPlaneInitialized).To(BeNil())
		g.Expect(f.kubeconfigSecret()).To(BeNil())

		// The next one reads the endpoint from the API server, writes a verified Secret with cluster-id and initializes.
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))
		result, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		// Decide asked for the write, so its outcome is looked at again after MinRequeue.
		expectRequeue(g, result, time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)))
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(controlplanev1.KubeconfigClusterIDAnnotation, cpIKSClusterID))
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneAvailableReason)
		g.Expect(conditions.IsTrue(cp, controlplanev1.IBMCloudManagedControlPlaneReadyCondition)).To(BeTrue(), "the summary of the three")
		g.Expect(prober.calls()).To(HaveLen(1), "only the new kubeconfig is probed; there was none stored")
		for _, body := range f.patches() {
			g.Expect(emptyStatusStrings(t, body)).To(BeEmpty(), "no status patch writes an empty string: %s", body)
		}

		// Sticky: an API server that cannot be reached later leaves the control plane initialized.
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnreachable, "SelfSubjectReview")
		})
		f.clock.SetTime(cpTestStart.Add(5 * time.Minute))
		f.expectObserve(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		cp = f.controlPlane()
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)))
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason)
		g.Expect(condition.Message).To(Equal("The management cluster cannot reach " + cpTestServer + " (SelfSubjectReview failed " +
			"(Unreachable)); private-only clusters need private connectivity"))
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneAvailableWorkloadAPIUnreachableReason)
	})

	t.Run("no_initialization_without_a_verified_secret", func(t *testing.T) {
		g := NewWithT(t)
		f, prober := newKubeconfigFixture(t, nil)
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).
			Return(nil, ibmError(kubernetesservice.OpGetAdminKubeconfig, http.StatusInternalServerError, "E0004")))

		_, err := f.reconcile()

		// kubernetesservice.Classify makes a 5xx Transient, so controller-runtime retries with its backoff instead of
		// Decide's MinRequeue.
		g.Expect(err).To(MatchError(ContainSubstring("fetching the admin kubeconfig of IBM cluster iksclusterid00000001")))
		cp := f.controlPlane()
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil())
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason)
		g.Expect(condition.Message).To(Equal("CAPIBM could not complete fetching the admin kubeconfig of IBM cluster iksclusterid00000001 " +
			"(IBM code E0004), and the controller log names the cause; CAPIBM writes Secret dev-kubeconfig only once a new kubeconfig " +
			"passes the probe, and tries again after a backoff"))
		g.Expect(condition.Message).NotTo(ContainSubstring("incident"), "the IBM code only, never the incident ID")
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason)
		g.Expect(f.kubeconfigSecret()).To(BeNil())
		g.Expect(prober.calls()).To(BeEmpty())
	})
}

func TestControlPlaneKubeconfigSecretContract(t *testing.T) {
	t.Run("owned_secret_name_type_label_key_owner_and_annotations", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, nil)
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		written := f.kubeconfigSecret()
		g.Expect(written).NotTo(BeNil())
		g.Expect(written.Name).To(Equal("dev-kubeconfig"))
		g.Expect(written.Type).To(Equal(clusterv1.ClusterSecretType))
		g.Expect(written.Labels).To(HaveKeyWithValue(clusterv1.ClusterNameLabel, cpTestName))
		g.Expect(written.Data).To(HaveKey("value"))
		owner := metav1.GetControllerOf(written)
		g.Expect(owner).NotTo(BeNil())
		g.Expect(owner.Kind).To(Equal("IBMCloudManagedControlPlane"))
		g.Expect(owner.Name).To(Equal(cpTestName))
		// Inspect accepts only the shape Build writes: inline server, CA and client certificate; no exec, auth-provider,
		// token, file path or skipped TLS verification.
		info, err := managedkubeconfig.Inspect(written.Data["value"])
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(info.Trust).To(Equal(managedkubeconfig.ClusterCA))
		g.Expect(info.Server).To(Equal(cpTestServer))
		fetched := "2026-11-16T03:05:30Z"
		g.Expect(written.Annotations).To(Equal(map[string]string{
			controlplanev1.KubeconfigClusterIDAnnotation:           cpIKSClusterID,
			managedscope.KubeconfigAccountIDAnnotation:             cpTestAccount,
			managedscope.KubeconfigServerAnnotation:                cpTestServer,
			managedscope.KubeconfigCredentialTypeAnnotation:        "ClientCertificate",
			managedscope.KubeconfigCredentialNotBeforeAnnotation:   info.Certificate.NotBefore.UTC().Format(time.RFC3339),
			managedscope.KubeconfigCredentialNotAfterAnnotation:    info.Certificate.NotAfter.UTC().Format(time.RFC3339),
			managedscope.KubeconfigCredentialFingerprintAnnotation: info.Certificate.Fingerprint,
			managedscope.KubeconfigFetchedAtAnnotation:             fetched,
			managedscope.KubeconfigCAModeAnnotation:                "ClusterCA",
			managedscope.KubeconfigCAFingerprintAnnotation:         info.CA.Fingerprint,
			managedscope.KubeconfigCANotAfterAnnotation:            info.CA.NotAfter.UTC().Format(time.RFC3339),
			managedscope.KubeconfigCAFetchedAtAnnotation:           fetched,
		}), "the account comes from the cluster CRN, and no annotation names the identity")
		expectSecretHasNoSecretMaterialInAnnotations(g, written)
	})

	t.Run("user_provided_secret_is_respected_and_never_modified", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		userProvided := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: "dev-kubeconfig",
				Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"value": buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)},
		}
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { objs.extra = append(objs.extra, userProvided) })
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.expectObserve(cpCluster("normal")) // no GetAdminKubeconfig: gomock fails the test on one
		before := f.kubeconfigSecret()

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret()).To(Equal(before), "the Secret is untouched")
		cp := f.controlPlane()
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretReason)
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)), "a passing probe of the user's Secret")
	})

	for _, tc := range []struct {
		name   string
		modify func(s *corev1.Secret)
		want   string
	}{
		{name: "user_provided_secret_without_the_cluster_name_label", modify: func(s *corev1.Secret) { s.Labels = nil },
			want: "lacks the label cluster.x-k8s.io/cluster-name"},
		{name: "user_provided_secret_with_an_empty_value", modify: func(s *corev1.Secret) { s.Data["value"] = nil },
			want: `lacks a non-empty "value" key`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			userProvided := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: "dev-kubeconfig",
					Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
				Data: map[string][]byte{"value": []byte("apiVersion: v1")},
			}
			tc.modify(userProvided)
			f, _ := newKubeconfigFixture(t, func(objs *cpObjects) { objs.extra = append(objs.extra, userProvided) })
			f.expectObserve(cpCluster("normal")) // no probe, no fetch
			before := f.kubeconfigSecret()

			_, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.kubeconfigSecret()).To(Equal(before))
			cp := f.controlPlane()
			condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretInvalidReason)
			g.Expect(condition.Message).To(ContainSubstring(tc.want))
			g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil())
		})
	}
}

// expectSecretHasNoSecretMaterialInAnnotations checks that no annotation of s carries PEM material.
func expectSecretHasNoSecretMaterialInAnnotations(g Gomega, s *corev1.Secret) {
	for key, value := range s.Annotations {
		g.ExpectWithOffset(1, value).NotTo(ContainSubstring("BEGIN"), key)
		g.ExpectWithOffset(1, value).NotTo(ContainSubstring("KubeCert"), key)
	}
}

func TestControlPlaneKubeconfigVerifyBeforeWrite(t *testing.T) {
	t.Run("new_credential_fails_the_review_and_the_stored_secret_stays", func(t *testing.T) {
		g := NewWithT(t)
		// A two-year certificate 366 days old is past half its lifetime (KubeconfigPolicy's RefreshFraction).
		stored := newKubeconfigCredentials(t, 366*24*time.Hour)
		storedValue := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, stored)
		fresh := kubeconfigCredentials{ca: stored.ca, admin: stored.ca.issueAdmin(t, cpTestAdminCN, cpTestStart, cpTestStart.AddDate(2, 0, 0))}
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, storedValue, cpTestStart.Add(-366*24*time.Hour), caFetchedRecently()))
		})
		storedFingerprint := mustInspect(t, storedValue).Certificate.Fingerprint
		prober.setAnswer(func(p probedKubeconfig) (workloadprobe.Result, error) {
			if p.certificate == storedFingerprint {
				return passing("v1.36.5+IKS"), nil
			}
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnauthorized, "SelfSubjectReview")
		})
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(fresh, true), nil).Times(1))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		// The attempt is recorded and SameCertificateRetry holds the next one: no requeue at Decide's MinRequeue.
		expectRequeue(g, result, 5*time.Minute)
		written := f.kubeconfigSecret()
		g.Expect(written.Data["value"]).To(Equal(storedValue), "a failing kubeconfig never replaces a working one")
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigRefreshCheckedAtAnnotation, "2026-11-16T03:05:30Z"))
		cp := f.controlPlane()
		wantMessage := "A new kubeconfig of IBM cluster iksclusterid00000001 did not pass the probe against " + cpTestServer +
			": SelfSubjectReview failed (Unauthorized); CAPIBM keeps Secret dev-kubeconfig as it is and tries the refresh again after a backoff"
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason)
		g.Expect(condition.Message).To(Equal(wantMessage))
		g.Expect(prober.calls()).To(HaveLen(2), "the stored kubeconfig, then the new one; a 401 is not a TLS failure, so no other trust mode")
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneAvailableReason)
		patched := len(f.patches())

		// The reconcile that the attempt's own status patch starts at once, and every later one until the next attempt,
		// keeps the VerificationFailed report until its backoff ends: KubeconfigReady and Ready do not flap to True, and
		// status is not patched again. (The 12-hourly CA re-check, due 11 hours from now, is the next attempt of another
		// kind; the steps stop before it.)
		for _, after := range []time.Duration{time.Second, 5 * time.Minute, 10 * time.Hour} {
			f.clock.SetTime(cpTestStart.Add(after))
			f.expectObserve(cpCluster("normal")) // SameCertificateRetry: no GetAdminKubeconfig
			_, err = f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			cp = f.controlPlane()
			condition = expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason)
			g.Expect(condition.Message).To(Equal(wantMessage), "after %s", after)
			g.Expect(conditions.IsFalse(cp, controlplanev1.IBMCloudManagedControlPlaneReadyCondition)).To(BeTrue(), "after %s", after)
			g.Expect(f.patches()).To(HaveLen(patched), "no status patch after %s", after)
			g.Expect(f.kubeconfigSecret().Data["value"]).To(Equal(storedValue))
		}

		// SameCertificateRetry after the attempt, IBM serves a certificate that passes: it is written and the report ends.
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.clock.SetTime(cpTestStart.Add(24 * time.Hour))
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(fresh, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: stored.ca.pem}, nil))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret().Data["value"]).NotTo(Equal(storedValue))
		expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
	})

	t.Run("IKS_cluster_CA_fails_and_system_roots_pass", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, nil)
		prober.setAnswer(func(p probedKubeconfig) (workloadprobe.Result, error) {
			if p.trust == managedkubeconfig.ClusterCA {
				return workloadprobe.Result{}, probeFailure(workloadprobe.FailureTLS, "SelfSubjectReview")
			}
			return passing("v1.36.5+IKS"), nil
		})
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		written := f.kubeconfigSecret()
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCAModeAnnotation, "SystemRoots"))
		g.Expect(written.Annotations).NotTo(HaveKey(managedscope.KubeconfigCAFingerprintAnnotation))
		g.Expect(mustInspect(t, written.Data["value"]).CA).To(BeNil(), "SystemRoots embeds no CA data")
		g.Expect(prober.calls()).To(HaveLen(2))
		g.Expect(prober.calls()[0].trust).To(Equal(managedkubeconfig.ClusterCA), "the flavor's mode first (CATrustFor)")
	})

	t.Run("ROKS_system_roots_fail_and_the_cluster_CA_passes", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, withOpenShift)
		prober.setAnswer(func(p probedKubeconfig) (workloadprobe.Result, error) {
			if p.trust == managedkubeconfig.SystemRoots {
				return workloadprobe.Result{}, probeFailure(workloadprobe.FailureTLS, "SelfSubjectReview")
			}
			return passing("v1.34.9"), nil
		})
		gomock.InOrder(f.expectObserve(roksCluster()),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, false), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		written := f.kubeconfigSecret()
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCAModeAnnotation, "ClusterCA"))
		g.Expect(written.Annotations).To(HaveKey(managedscope.KubeconfigCAFingerprintAnnotation))
		g.Expect(f.controlPlane().Status.Version).To(Equal("v1.34.9"), "ROKS reports the Kubernetes version, never OpenShift's")
	})

	t.Run("ROKS_system_roots_pass_without_a_CA_fetch", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, withOpenShift)
		prober.setAnswer(alwaysPassing("v1.34.9"))
		gomock.InOrder(f.expectObserve(roksCluster()),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, false), nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCAModeAnnotation, "SystemRoots"))
		g.Expect(prober.calls()).To(HaveLen(1))
	})
}

// withOpenShift makes the test control plane a ROKS 4.21 one. objs are the test objects.
func withOpenShift(objs *cpObjects) {
	objs.cp.Spec.Distribution = controlplanev1.DistributionOpenShift
	objs.cp.Spec.DistributionVersion = "4.21"
}

// roksCluster returns the bound IBM cluster as a ROKS 4.21 cluster with an "-e" master URL, as IBM serves it.
func roksCluster() *kubernetesservice.Cluster {
	c := cpCluster("normal")
	c.Type = "openshift"
	c.MasterKubeVersion, c.TargetVersion = "4.21.35_1537_openshift", "4.21.35_1537_openshift"
	return c
}

// mustInspect describes a kubeconfig CAPIBM built. value is the kubeconfig; t is the test.
func mustInspect(t *testing.T, value []byte) managedkubeconfig.Info {
	t.Helper()
	info, err := managedkubeconfig.Inspect(value)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return info
}

func TestControlPlaneKubeconfigRefresh(t *testing.T) {
	t.Run("fresh_certificate_is_not_refreshed_and_probed_every_steady_poll", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), nil))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.expectObserve(cpCluster("normal")) // no GetAdminKubeconfig, no GetCACertificate
		before := f.kubeconfigSecret()

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		g.Expect(f.kubeconfigSecret()).To(Equal(before), "a valid Secret is not rewritten")
		g.Expect(prober.calls()).To(HaveLen(1))
		g.Expect(f.controlPlane().Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)))
	})

	t.Run("same_certificate_at_366_days_writes_only_refresh_checked_at", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, 366*24*time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-366*24*time.Hour), caFetchedRecently()))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil).Times(1))
		before := f.kubeconfigSecret()

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		after := f.kubeconfigSecret()
		g.Expect(after.Data).To(Equal(before.Data), "IBM returned the stored certificate: no value write")
		g.Expect(after.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigRefreshCheckedAtAnnotation, "2026-11-16T03:05:30Z"))
		g.Expect(after.Annotations[managedscope.KubeconfigFetchedAtAnnotation]).To(Equal(before.Annotations[managedscope.KubeconfigFetchedAtAnnotation]))
		g.Expect(prober.calls()).To(HaveLen(1), "the same kubeconfig is not probed twice")

		// The next attempt is SameCertificateRetry (24 h) later: none at the next steady poll.
		f.clock.SetTime(cpTestStart.Add(5 * time.Minute))
		f.expectObserve(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
	})

	t.Run("server_change_refreshes_and_reports_EndpointChanged", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		oldValue := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, oldValue, cpTestStart.Add(-time.Hour), nil))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		moved := cpCluster("normal")
		moved.MasterURL = "https://c111-e.us-south.containers.cloud.ibm.com"
		gomock.InOrder(f.expectObserve(moved),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		written := f.kubeconfigSecret()
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigServerAnnotation, "https://c111-e.us-south.containers.cloud.ibm.com:443"))
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigRefreshCheckedAtAnnotation, "2026-11-16T03:05:30Z"),
			"the same certificate came back")
		cp := f.controlPlane()
		g.Expect(cp.Spec.ControlPlaneEndpoint).To(Equal(cpTestEndpoint), "the endpoint is written once and never changed")
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
		g.Expect(condition.Message).To(HavePrefix("EndpointChanged: IBM serves the API server at https://c111-e.us-south.containers.cloud.ibm.com:443, " +
			"not at spec.controlPlaneEndpoint c111.us-south.containers.cloud.ibm.com:30101"))
	})

	t.Run("CA_marker_change_refreshes_the_CA_and_the_credential_and_polls_every_minute", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), nil))
		})
		rotatedCA := newTestClusterCA(t, cpTestStart.AddDate(10, 0, 0))
		rotated := kubeconfigCredentials{ca: rotatedCA, admin: rotatedCA.issueAdmin(t, cpTestAdminCN, cpTestStart, cpTestStart.AddDate(2, 0, 0))}
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		c := cpCluster("normal")
		c.CARotationStatus, c.CARotationCompletedDate = "CA certificate rotation complete", "2026-11-16T03:00:00+0000"
		gomock.InOrder(f.expectObserve(c),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(rotated, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: rotatedCA.pem}, nil))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		written := f.kubeconfigSecret()
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCARotationMarkerAnnotation,
			"CA certificate rotation complete|2026-11-16T03:00:00+0000"))
		g.Expect(written.Annotations[managedscope.KubeconfigCAFingerprintAnnotation]).To(Equal(mustInspect(t, written.Data["value"]).CA.Fingerprint))
		g.Expect(written.Data["value"]).NotTo(Equal(value))
	})

	t.Run("CA_rotation_in_progress_is_unknown_and_polls_every_minute", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), nil))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		c := cpCluster("normal")
		c.CARotationStatus = "CA certificate rotation in progress"
		gomock.InOrder(f.expectObserve(c),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionUnknown,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCARotationInProgressReason)
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCARotationMarkerAnnotation,
			"CA certificate rotation in progress|"), "the marker is recorded, so the next poll does not refresh again")
		g.Expect(f.kubeconfigSecret().Data["value"]).To(Equal(value), "IBM returned the stored certificate and CA")
	})

	t.Run("CA_near_expiry_is_fetched_again_without_a_credential", func(t *testing.T) {
		g := NewWithT(t)
		ca := newTestClusterCA(t, cpTestStart.Add(6*time.Hour))
		creds := kubeconfigCredentials{ca: ca, admin: ca.issueAdmin(t, cpTestAdminCN, cpTestStart.Add(-time.Hour), cpTestStart.Add(5*time.Hour))}
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), map[string]string{
				managedscope.KubeconfigRefreshCheckedAtAnnotation: "2026-11-16T03:00:00Z"}))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: ca.pem}, nil))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		after := f.kubeconfigSecret()
		g.Expect(after.Data["value"]).To(Equal(value), "the same CA: nothing but ca-fetched-at changes")
		g.Expect(after.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCAFetchedAtAnnotation, "2026-11-16T03:05:30Z"))
	})

	t.Run("expiry_warning_is_a_message_and_one_event", func(t *testing.T) {
		g := NewWithT(t)
		// Two years minus 20 days old: within ExpiryWarning (30 days) of notAfter; the last same-certificate attempt was an
		// hour ago, so no refresh is due before SameCertificateRetry.
		creds := newKubeconfigCredentials(t, (2*365-20)*24*time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), map[string]string{
				managedscope.KubeconfigRefreshCheckedAtAnnotation: "2026-11-16T02:05:30Z"}))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.expectObserve(cpCluster("normal"))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
		g.Expect(condition.Message).To(HavePrefix("AdminCertificateExpiring: IBM's admin certificate in Secret dev-kubeconfig expires at "))
		events := f.events()
		g.Expect(events).To(HaveLen(1))
		g.Expect(events[0]).To(HavePrefix("Warning AdminCertificateExpiring "))

		f.clock.SetTime(cpTestStart.Add(5 * time.Minute))
		f.expectObserve(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.events()).To(BeEmpty(), "the event marks the transition, not every reconcile")
	})
}

func TestControlPlaneKubeconfigUnauthorizedRun(t *testing.T) {
	// newUnauthorizedFixture stores a valid Secret (fetched an hour ago) with extra annotations; the stored credential is
	// answered 401, and IBM returns the same certificate on a refresh.
	newUnauthorizedFixture := func(t *testing.T, extra map[string]string) (*cpFixture, *kubeconfigProber, kubeconfigCredentials) {
		t.Helper()
		creds := newKubeconfigCredentials(t, time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), extra))
		})
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnauthorized, "SelfSubjectReview")
		})
		return f, prober, creds
	}

	t.Run("first_401_sets_the_annotation_and_a_second_keeps_it_and_success_removes_it", func(t *testing.T) {
		g := NewWithT(t)
		f, prober, creds := newUnauthorizedFixture(t, nil)
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(managedscope.KubeconfigUnauthorizedSinceAnnotation, "2026-11-16T03:05:30Z"))
		expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason)

		// A second 401 thirty seconds later keeps the start and waits for the backoff: no fetch.
		f.clock.SetTime(cpTestStart.Add(30 * time.Second))
		f.expectObserve(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(managedscope.KubeconfigUnauthorizedSinceAnnotation, "2026-11-16T03:05:30Z"))

		// The API server accepts the credential again: the run ends.
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.clock.SetTime(cpTestStart.Add(time.Minute))
		f.expectObserve(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret().Annotations).NotTo(HaveKey(managedscope.KubeconfigUnauthorizedSinceAnnotation))
		expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
	})

	t.Run("a_controller_restart_mid_run_keeps_the_backoff", func(t *testing.T) {
		g := NewWithT(t)
		// A 401 run started four minutes ago; the last attempt (the same certificate) was one minute ago, three minutes into
		// the run, so the next is due three minutes after it (backoff = the run's length at that attempt), two minutes from now.
		f, _, _ := newUnauthorizedFixture(t, map[string]string{
			managedscope.KubeconfigUnauthorizedSinceAnnotation: "2026-11-16T03:01:30Z",
			managedscope.KubeconfigRefreshCheckedAtAnnotation:  "2026-11-16T03:04:30Z",
		})
		f.expectObserve(cpCluster("normal"))
		result, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		first := result.RequeueAfter

		// A new reconciler (a restart: no memory) over the same objects decides the same.
		restarted := *f.r
		f.r = &restarted
		f.expectObserve(cpCluster("normal"))
		result, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically("~", first, first/10+time.Second))
		g.Expect(result.RequeueAfter).To(BeNumerically("<", 2*time.Minute+12*time.Second), "due at 03:05:30 + jitter")
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(managedscope.KubeconfigUnauthorizedSinceAnnotation, "2026-11-16T03:01:30Z"))
	})

	for _, tc := range []struct {
		name         string
		since        string
		checked      string
		wantFetch    bool
		wantMaxAfter time.Duration
	}{
		{name: "forged_future_value_refreshes_at_once_within_the_bounds", since: "2030-01-01T00:00:00Z", checked: "2026-11-16T03:05:00Z",
			wantFetch: true, wantMaxAfter: time.Minute},
		{name: "forged_past_value_waits_at_most_AuthFailureBackoffMax", since: "2020-01-01T00:00:00Z", checked: "2026-11-16T03:05:00Z",
			wantMaxAfter: 10 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f, _, creds := newUnauthorizedFixture(t, map[string]string{
				managedscope.KubeconfigUnauthorizedSinceAnnotation: tc.since,
				managedscope.KubeconfigRefreshCheckedAtAnnotation:  tc.checked,
			})
			f.expectObserve(cpCluster("normal"))
			if tc.wantFetch {
				f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil)
			}

			result, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(BeNumerically(">=", time.Minute*9/10), "never below MinRequeue")
			g.Expect(result.RequeueAfter).To(BeNumerically("<=", tc.wantMaxAfter*11/10+time.Second), "never beyond AuthFailureBackoffMax")
		})
	}

	t.Run("never_written_on_a_user_provided_secret", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		userProvided := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: "dev-kubeconfig",
				Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"value": buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)},
		}
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { objs.extra = append(objs.extra, userProvided) })
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnauthorized, "SelfSubjectReview")
		})
		f.expectObserve(cpCluster("normal"))
		before := f.kubeconfigSecret()

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret()).To(Equal(before))
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason)
		g.Expect(condition.Message).To(Equal("The user-provided Secret dev-kubeconfig did not pass the probe: SelfSubjectReview failed " +
			"(Unauthorized); CAPIBM never modifies it, so put a working kubeconfig of the cluster into it"))
	})
}

func TestControlPlaneKubeconfigProbeHygiene(t *testing.T) {
	const marker = "server-body-marker-5c1e"
	// markedFailure is a failed probe of class reason whose error carries the API server's text with the marker (the
	// prober's ProbeError withholds that text; this fake does not, so that a leak would show).
	markedFailure := func(reason workloadprobe.FailureReason) error {
		return &workloadprobe.ProbeError{Reason: reason, Step: "SelfSubjectReview", Err: errors.New("server said: " + marker)}
	}
	// markedUser is a passing probe as a user whose name carries the marker (Result.Username, which must never surface).
	markedUser := func() workloadprobe.Result {
		result := passing("v1.36.5+IKS")
		result.Username = cpTestAdminCN + marker
		return result
	}

	for _, tc := range []struct {
		name string
		// setup stores the scenario's objects, scripts the prober and the IBM calls.
		setup func(t *testing.T, f *cpFixture, prober *kubeconfigProber)
		// stored is the Secret the fixture starts with; nil for none.
		stored func(t *testing.T) *corev1.Secret
		// wantInCondition is the probe text (step and class only) a condition must carry, which proves that the failure
		// reached a condition; "" when the scenario's probe passed.
		wantInCondition string
	}{
		{
			name: "a_stored_kubeconfig_answered_401_and_a_new_one_that_fails_too",
			stored: func(t *testing.T) *corev1.Secret {
				t.Helper()
				creds := newKubeconfigCredentials(t, time.Hour)
				return ownedKubeconfigSecret(t, buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds), cpTestStart.Add(-time.Hour), nil)
			},
			setup: func(t *testing.T, f *cpFixture, prober *kubeconfigProber) {
				t.Helper()
				prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
					return workloadprobe.Result{}, markedFailure(workloadprobe.FailureUnauthorized)
				})
				fresh := newKubeconfigCredentials(t, 0)
				gomock.InOrder(f.expectObserve(cpCluster("normal")),
					f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(fresh, true), nil))
			},
			wantInCondition: "SelfSubjectReview failed (Unauthorized)",
		},
		{
			name: "a_stored_kubeconfig_whose_API_server_cannot_be_reached",
			stored: func(t *testing.T) *corev1.Secret {
				t.Helper()
				creds := newKubeconfigCredentials(t, time.Hour)
				return ownedKubeconfigSecret(t, buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds), cpTestStart.Add(-time.Hour), nil)
			},
			setup: func(_ *testing.T, f *cpFixture, prober *kubeconfigProber) {
				prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
					return workloadprobe.Result{}, markedFailure(workloadprobe.FailureUnreachable)
				})
				f.expectObserve(cpCluster("normal"))
			},
			wantInCondition: "SelfSubjectReview failed (Unreachable)",
		},
		{
			name: "a_user_provided_Secret_that_the_API_server_refuses",
			stored: func(t *testing.T) *corev1.Secret {
				t.Helper()
				creds := newKubeconfigCredentials(t, time.Hour)
				return &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: "dev-kubeconfig",
						Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
					Type: corev1.SecretTypeOpaque,
					Data: map[string][]byte{"value": buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)},
				}
			},
			setup: func(_ *testing.T, f *cpFixture, prober *kubeconfigProber) {
				prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
					return workloadprobe.Result{}, markedFailure(workloadprobe.FailureOther)
				})
				f.expectObserve(cpCluster("normal"))
			},
			wantInCondition: "SelfSubjectReview failed (Other)",
		},
		{
			name: "a_first_write_after_a_TLS_failure_as_a_user_whose_name_carries_the_marker",
			setup: func(t *testing.T, f *cpFixture, prober *kubeconfigProber) {
				t.Helper()
				creds := newKubeconfigCredentials(t, time.Hour)
				prober.setAnswer(func(p probedKubeconfig) (workloadprobe.Result, error) {
					if p.trust == managedkubeconfig.ClusterCA {
						return workloadprobe.Result{}, markedFailure(workloadprobe.FailureTLS)
					}
					return markedUser(), nil
				})
				gomock.InOrder(f.expectObserve(cpCluster("normal")),
					f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
					f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
				if tc.stored != nil {
					objs.extra = append(objs.extra, tc.stored(t))
				}
			})
			tc.setup(t, f, prober)

			_, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			cp := f.controlPlane()
			if tc.wantInCondition != "" {
				kubeconfigReady := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition)
				g.Expect(kubeconfigReady.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(kubeconfigReady.Message).To(ContainSubstring(tc.wantInCondition), "the failure reaches KubeconfigReady")
			}
			for _, condition := range cp.Status.Conditions {
				g.Expect(condition.Message).NotTo(ContainSubstring(marker), condition.Type)
				g.Expect(condition.Message).NotTo(ContainSubstring("KubeCert"), condition.Type)
			}
			for _, event := range f.events() {
				g.Expect(event).NotTo(ContainSubstring(marker))
			}
			if written := f.kubeconfigSecret(); written != nil {
				for key, value := range written.Annotations {
					g.Expect(value).NotTo(ContainSubstring(marker), key)
				}
			}
			g.Expect(f.patches()).NotTo(BeEmpty())
			for _, body := range f.patches() {
				g.Expect(body).NotTo(ContainSubstring(marker))
				g.Expect(body).NotTo(ContainSubstring("KubeCert"))
			}
		})
	}
}

func TestControlPlaneKubeconfigVersions(t *testing.T) {
	t.Run("versions_are_the_probe_gitVersion", func(t *testing.T) {
		for _, tc := range []struct {
			name, gitVersion string
			setup            func(objs *cpObjects)
			cluster          func() *kubernetesservice.Cluster
		}{
			{name: "IKS", gitVersion: "v1.36.5+IKS", cluster: func() *kubernetesservice.Cluster { return cpCluster("normal") }},
			{name: "ROKS", gitVersion: "v1.34.9", setup: withOpenShift, cluster: roksCluster},
		} {
			t.Run(tc.name, func(t *testing.T) {
				g := NewWithT(t)
				creds := newKubeconfigCredentials(t, time.Hour)
				trust := managedscope.CATrustFor(controlplanev1.DistributionKubernetes)
				if tc.setup != nil {
					trust = managedkubeconfig.SystemRoots
				}
				value := buildTestKubeconfig(t, cpTestServer, trust, creds)
				f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
					if tc.setup != nil {
						tc.setup(objs)
					}
					objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), nil))
				})
				prober.setAnswer(alwaysPassing(tc.gitVersion))
				f.expectObserve(tc.cluster())

				_, err := f.reconcile()

				g.Expect(err).NotTo(HaveOccurred())
				cp := f.controlPlane()
				g.Expect(cp.Status.Versions).To(Equal([]controlplanev1.StatusVersion{{Version: tc.gitVersion}}))
				g.Expect(cp.Status.Version).To(Equal(tc.gitVersion), "never an IBM or OpenShift version")
			})
		}
	})

	t.Run("alternating_probes_while_rolling_make_no_status_patch", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), nil))
		})
		rolling := func() *kubernetesservice.Cluster {
			c := cpCluster("normal")
			c.TargetVersion = "1.36.6_1541" // IBM applies a master patch: the three API server replicas are updated one by one
			return c
		}
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.expectObserve(rolling())
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane().Status.Version).To(Equal("v1.36.5+IKS"))
		settled := len(f.patches())

		for i, gitVersion := range []string{"v1.36.6+IKS", "v1.36.5+IKS", "v1.36.6+IKS"} {
			prober.setAnswer(alwaysPassing(gitVersion))
			f.clock.SetTime(cpTestStart.Add(time.Duration(i+1) * 5 * time.Minute))
			f.expectObserve(rolling())
			_, err := f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.patches()).To(HaveLen(settled), "no status patch while the probes alternate (%s)", gitVersion)
		}
		g.Expect(f.controlPlane().Status.Version).To(Equal("v1.36.5+IKS"), "the minimum while rolling")

		// IBM reports the update done: the new version is published.
		done := cpCluster("normal")
		done.MasterKubeVersion, done.TargetVersion = "1.36.6_1541", "1.36.6_1541"
		prober.setAnswer(alwaysPassing("v1.36.6+IKS"))
		f.clock.SetTime(cpTestStart.Add(30 * time.Minute))
		f.expectObserve(done)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.Versions).To(Equal([]controlplanev1.StatusVersion{{Version: "v1.36.6+IKS"}}))
		g.Expect(cp.Status.Version).To(Equal("v1.36.6+IKS"))
	})
}

func TestRequeueAtKubeconfigDeadline(t *testing.T) {
	testCases := []struct {
		name     string
		result   ctrl.Result
		err      error
		deadline time.Duration
		wantMin  time.Duration
		wantMax  time.Duration
		wantErr  bool
	}{
		{name: "an earlier deadline wins", result: ctrl.Result{RequeueAfter: 5 * time.Minute}, deadline: time.Minute,
			wantMin: time.Minute, wantMax: time.Minute * 11 / 10},
		{name: "a later deadline changes nothing", result: ctrl.Result{RequeueAfter: 5 * time.Minute}, deadline: 12 * time.Hour,
			wantMin: 5 * time.Minute, wantMax: 5 * time.Minute},
		{name: "no deadline changes nothing", result: ctrl.Result{RequeueAfter: 5 * time.Minute},
			wantMin: 5 * time.Minute, wantMax: 5 * time.Minute},
		{name: "an error is returned unchanged", err: errors.New("boom"), deadline: time.Minute, wantErr: true},
		{name: "no requeue gets the deadline", deadline: 2 * time.Minute, wantMin: 2 * time.Minute, wantMax: 2 * time.Minute * 11 / 10},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			result, err := requeueAtKubeconfigDeadline(tc.result, tc.err, tc.deadline)
			if tc.wantErr {
				g.Expect(err).To(HaveOccurred())
				g.Expect(result).To(Equal(tc.result))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(BeNumerically(">=", tc.wantMin))
			g.Expect(result.RequeueAfter).To(BeNumerically("<=", tc.wantMax))
		})
	}
}

// TestControlPlaneEnvtestKubeconfigSecret writes the kubeconfig Secret to the real API server: the owner reference
// carries the control plane's UID (clusterctl move and garbage collection follow it), the Secret type and label pass
// the API server, and a second reconcile at the same state writes nothing. No manager runs.
func TestControlPlaneEnvtestKubeconfigSecret(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "cp-kubeconfig")
	cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
	infra := createTestInfra(t, namespace, "dev", cluster)
	infra.Status = newCPObjects().infra.Status
	g.Expect(testEnv.Status().Update(ctx, infra)).To(Succeed())
	// The test ends with a bound control plane: paused, a later manager watching every namespace leaves it alone.
	t.Cleanup(func() { pauseTestCluster(t, cluster) })
	cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
	key := client.ObjectKeyFromObject(cp)
	cp.Finalizers = []string{controlplanev1.IBMCloudManagedControlPlaneFinalizer}
	cp.Spec.ControlPlaneEndpoint = cpTestEndpoint
	g.Expect(testEnv.Update(ctx, cp)).To(Succeed())
	cp.Status.ClusterID = cpIKSClusterID
	cp.Status.Conditions = []metav1.Condition{{Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse,
		Reason: clusterv1.NotPausedReason, LastTransitionTime: metav1.NewTime(cpTestStart)}}
	g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())

	creds := newKubeconfigCredentials(t, time.Hour)
	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	r := newEnvtestControlPlaneReconciler(t)
	prober := &kubeconfigProber{t: t, answer: alwaysPassing("v1.36.5+IKS")}
	r.Resolver, r.ClientBuilder, r.Prober = &recordingResolver{}, &stubClientBuilder{kubernetes: kube}, prober
	c := cpCluster("normal")
	c.Name = "dev"
	gomock.InOrder(
		kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(c, nil),
		kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
		kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil),
		kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(c, nil),
	)

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(HaveOccurred())

	written := &corev1.Secret{}
	g.Expect(testEnv.GetAPIReader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: "dev-kubeconfig"}, written)).To(Succeed())
	persisted := getEnvtestControlPlane(t, key)
	g.Expect(written.Type).To(Equal(clusterv1.ClusterSecretType))
	g.Expect(written.Labels).To(HaveKeyWithValue(clusterv1.ClusterNameLabel, "dev"))
	g.Expect(metav1.IsControlledBy(written, persisted)).To(BeTrue(), "controller reference with the control plane's UID")
	g.Expect(persisted.Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)))
	g.Expect(persisted.Status.Version).To(Equal("v1.36.5+IKS"))
	g.Expect(conditions.IsTrue(persisted, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition)).To(BeTrue())

	// The same state again: neither the Secret nor the control plane is written.
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(HaveOccurred())
	again := &corev1.Secret{}
	g.Expect(testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(written), again)).To(Succeed())
	g.Expect(again.ResourceVersion).To(Equal(written.ResourceVersion))
	g.Expect(getEnvtestControlPlane(t, key).ResourceVersion).To(Equal(persisted.ResourceVersion))
}

// caFetchedRecently is the annotation of a CA fetched an hour before cpTestStart, inside CARecheckInterval (12 h), so
// that Decide asks for no CA re-check.
func caFetchedRecently() map[string]string {
	return map[string]string{managedscope.KubeconfigCAFetchedAtAnnotation: "2026-11-16T02:05:30Z"}
}

func TestControlPlaneKubeconfigFailures(t *testing.T) {
	t.Run("an admin credential whose key does not match its certificate is never written", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		other := newKubeconfigCredentials(t, time.Hour)
		answer := adminAnswer(creds, true)
		answer.KeyPEM = other.admin.KeyPEM
		f, prober := newKubeconfigFixture(t, nil)
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(answer, nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute) // IBM's answer is unusable: terminalRecheck, not Decide's MinRequeue
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason)
		g.Expect(condition.Message).To(Equal("IBM's admin kubeconfig of cluster iksclusterid00000001 holds no usable client certificate, " +
			"key or CA (the controller log names the cause); CAPIBM writes Secret dev-kubeconfig only once a new kubeconfig passes " +
			"the probe, and tries again after a backoff"))
		g.Expect(f.kubeconfigSecret()).To(BeNil())
		g.Expect(prober.calls()).To(BeEmpty(), "nothing to verify")
	})

	t.Run("a failed Secret write is retried with backoff and reported", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, nil)
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		f.r.Client = interceptor.NewClient(f.client.(client.WithWatch), interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return apierrors.NewServiceUnavailable("injected Secret create failure")
				}
				return c.Create(ctx, obj, opts...)
			},
		})
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
			f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("injected Secret create failure")))
		cp := f.controlPlane()
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason)
		g.Expect(condition.Message).To(Equal("CAPIBM could not read or write Secret dev-kubeconfig; it retries, and the controller log names the cause"))
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil())
	})
}

// roksStoredFixture returns the fixture of a bound ROKS control plane whose CAPIBM-owned Secret holds a SystemRoots
// kubeconfig (the ROKS trust) of a certificate issued age ago, with extra annotations. ROKS embeds no CA, so no CA
// re-check of Decide's rule 5 interferes with the timing the tests check.
//
// Parameters:
//   - t is the test.
//   - age is the stored certificate's age.
//   - extra are further annotations of the Secret.
func roksStoredFixture(t *testing.T, age time.Duration, extra map[string]string) (*cpFixture, *kubeconfigProber, []byte) {
	t.Helper()
	creds := newKubeconfigCredentials(t, age)
	value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.SystemRoots, creds)
	f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
		withOpenShift(objs)
		objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-age), extra))
	})
	return f, prober, value
}

// reconcileAt reconciles at cpTestStart + after with the IBM calls expect sets up, and returns the result.
//
// Parameters:
//   - after is the time since cpTestStart.
//   - expect sets up the gomock expectations of this reconcile.
func (f *cpFixture) reconcileAt(after time.Duration, expect func()) (ctrl.Result, error) {
	f.clock.SetTime(cpTestStart.Add(after))
	expect()
	return f.reconcile()
}

// TestControlPlaneKubeconfigRefreshBackoff checks the timing of the second and third refresh attempts after a failure:
// a failed IBM call is asked again after the interval of its error class, a recorded attempt holds Decide's own
// backoff, and nothing repeats every MinRequeue.
func TestControlPlaneKubeconfigRefreshBackoff(t *testing.T) {
	t.Run("a_403_of_a_refresh_due_by_age_is_retried_at_terminalRecheck_and_then_only_after_SameCertificateRetry", func(t *testing.T) {
		g := NewWithT(t)
		f, prober, value := roksStoredFixture(t, 366*24*time.Hour, nil)
		prober.setAnswer(alwaysPassing("v1.34.9"))
		forbidden := ibmError(kubernetesservice.OpGetAdminKubeconfig, http.StatusForbidden, "")
		attempt := func() {
			gomock.InOrder(f.expectObserve(roksCluster()),
				f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(nil, forbidden))
		}
		observeOnly := func() { f.expectObserve(roksCluster()) }

		for _, step := range []struct {
			after       time.Duration
			expect      func()
			wantRequeue time.Duration
			wantChecked string
		}{
			{after: 0, expect: attempt, wantRequeue: 15 * time.Minute, wantChecked: "2026-11-16T03:05:30Z"},
			{after: time.Second, expect: observeOnly, wantRequeue: 5 * time.Minute, wantChecked: "2026-11-16T03:05:30Z"},
			{after: 15 * time.Minute, expect: observeOnly, wantRequeue: 5 * time.Minute, wantChecked: "2026-11-16T03:05:30Z"},
			{after: 24 * time.Hour, expect: attempt, wantRequeue: 15 * time.Minute, wantChecked: "2026-11-17T03:05:30Z"},
			{after: 24*time.Hour + 15*time.Minute, expect: observeOnly, wantRequeue: 5 * time.Minute, wantChecked: "2026-11-17T03:05:30Z"},
			{after: 48 * time.Hour, expect: attempt, wantRequeue: 15 * time.Minute, wantChecked: "2026-11-18T03:05:30Z"},
		} {
			result, err := f.reconcileAt(step.after, step.expect)
			g.Expect(err).NotTo(HaveOccurred(), "after %s", step.after)
			expectRequeue(g, result, step.wantRequeue)
			stored := f.kubeconfigSecret()
			g.Expect(stored.Data["value"]).To(Equal(value))
			g.Expect(stored.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigRefreshCheckedAtAnnotation, step.wantChecked), "after %s", step.after)
			cp := f.controlPlane()
			condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason)
			g.Expect(condition.Message).To(HavePrefix("IBM Cloud refused fetching the admin kubeconfig of IBM cluster iksclusterid00000001 " +
				"with HTTP 403: the credentials of the CAPIBM controller identity need the Administrator platform role"))
			g.Expect(condition.Message).To(HaveSuffix("; CAPIBM keeps Secret dev-kubeconfig as it is and tries the refresh again after a backoff"))
			expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionTrue,
				controlplanev1.IBMCloudManagedControlPlaneAvailableReason)
		}
	})

	t.Run("a_429_without_a_Secret_is_retried_after_Retry_After", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, nil)
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		rateLimited := &kubernetesservice.APIError{Operation: kubernetesservice.OpGetAdminKubeconfig, StatusCode: http.StatusTooManyRequests,
			RetryAfter: 10 * time.Minute}
		limited := func() {
			gomock.InOrder(f.expectObserve(cpCluster("normal")),
				f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(nil, rateLimited))
		}

		for _, after := range []time.Duration{0, 10 * time.Minute} {
			result, err := f.reconcileAt(after, limited)
			g.Expect(err).NotTo(HaveOccurred(), "a 429 is a wait, not an error")
			g.Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Minute), "never before Retry-After (after %s)", after)
			g.Expect(result.RequeueAfter).To(BeNumerically("<", 11*time.Minute))
			condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
				metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason)
			g.Expect(condition.Message).To(Equal("IBM Cloud rate limited CAPIBM (HTTP 429) while fetching the admin kubeconfig of IBM " +
				"cluster iksclusterid00000001; CAPIBM writes Secret dev-kubeconfig only once a new kubeconfig passes the probe, and " +
				"tries again after a backoff"))
			g.Expect(f.kubeconfigSecret()).To(BeNil())
		}

		// The third attempt, Retry-After later again, succeeds.
		_, err := f.reconcileAt(20*time.Minute, func() {
			gomock.InOrder(f.expectObserve(cpCluster("normal")),
				f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
				f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))
		})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.kubeconfigSecret()).NotTo(BeNil())
	})

	t.Run("a_429_of_a_refresh_due_by_age_keeps_the_verified_Secret_Ready_and_waits_for_Retry_After", func(t *testing.T) {
		g := NewWithT(t)
		f, prober, _ := roksStoredFixture(t, 366*24*time.Hour, nil)
		prober.setAnswer(alwaysPassing("v1.34.9"))
		rateLimited := &kubernetesservice.APIError{Operation: kubernetesservice.OpGetAdminKubeconfig, StatusCode: http.StatusTooManyRequests,
			RetryAfter: 10 * time.Minute}

		result, err := f.reconcileAt(0, func() {
			gomock.InOrder(f.expectObserve(roksCluster()),
				f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(nil, rateLimited))
		})

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Minute))
		g.Expect(result.RequeueAfter).To(BeNumerically("<", 11*time.Minute))
		// A 429 says nothing about the stored Secret, which passed the probe: KubeconfigReady stays True.
		expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
		g.Expect(f.kubeconfigSecret().Annotations).To(HaveKeyWithValue(managedscope.KubeconfigRefreshCheckedAtAnnotation, "2026-11-16T03:05:30Z"))

		// Retry-After later the attempt is recorded, so SameCertificateRetry holds the next one.
		_, err = f.reconcileAt(10*time.Minute, func() { f.expectObserve(roksCluster()) })
		g.Expect(err).NotTo(HaveOccurred())
	})

	t.Run("an_unreachable_first_write_is_retried_at_the_steady_poll", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, nil)
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnreachable, "SelfSubjectReview")
		})
		attempt := func() {
			gomock.InOrder(f.expectObserve(cpCluster("normal")),
				f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(adminAnswer(creds, true), nil),
				f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil))
		}

		for i, after := range []time.Duration{0, 5 * time.Minute, 10 * time.Minute} {
			result, err := f.reconcileAt(after, attempt)
			g.Expect(err).NotTo(HaveOccurred())
			expectRequeue(g, result, 5*time.Minute) // never Decide's MinRequeue of a missing kubeconfig
			cp := f.controlPlane()
			condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionFalse,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason)
			g.Expect(condition.Message).To(Equal("A new kubeconfig of IBM cluster iksclusterid00000001 did not pass the probe against " +
				cpTestServer + ": SelfSubjectReview failed (Unreachable); CAPIBM writes Secret dev-kubeconfig only once a new kubeconfig " +
				"passes the probe, and tries again after a backoff"))
			g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil())
			g.Expect(f.kubeconfigSecret()).To(BeNil(), "verify before write")
			g.Expect(prober.calls()).To(HaveLen(i+1), "one probe per attempt: an unreachable server is not a TLS failure")
		}
	})
}

// TestControlPlaneKubeconfigTLSRecheck checks the trust re-check of a stored kubeconfig whose probe fails TLS (the
// trust of a private-only master is not yet verified against IBM Cloud): both trust modes are tried with the stored
// credential, the CA comes from getCACert and no credential is fetched, and a re-check that fails waits ten minutes
// before the next one.
func TestControlPlaneKubeconfigTLSRecheck(t *testing.T) {
	failTLS := func(probedKubeconfig) (workloadprobe.Result, error) {
		return workloadprobe.Result{}, probeFailure(workloadprobe.FailureTLS, "SelfSubjectReview")
	}

	t.Run("ROKS_stored_system_roots_that_fail_TLS_are_rewritten_with_the_cluster_CA", func(t *testing.T) {
		g := NewWithT(t)
		f, prober, value := roksStoredFixture(t, time.Hour, nil)
		prober.setAnswer(func(p probedKubeconfig) (workloadprobe.Result, error) {
			if p.trust == managedkubeconfig.SystemRoots {
				return failTLS(p)
			}
			return passing("v1.34.9"), nil
		})
		storedFingerprint := mustInspect(t, value).Certificate.Fingerprint
		var ca []byte
		gomock.InOrder(f.expectObserve(roksCluster()), f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).DoAndReturn(
			func(context.Context, string) (*kubernetesservice.CACertificate, error) {
				ca = newTestClusterCA(t, cpTestStart.AddDate(9, 0, 0)).pem
				return &kubernetesservice.CACertificate{PEM: ca}, nil
			})) // no GetAdminKubeconfig: the stored credential is kept

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		written := f.kubeconfigSecret()
		g.Expect(written.Annotations).To(HaveKeyWithValue(managedscope.KubeconfigCAModeAnnotation, "ClusterCA"))
		info := mustInspect(t, written.Data["value"])
		g.Expect(info.Certificate.Fingerprint).To(Equal(storedFingerprint), "the stored credential with a new trust")
		g.Expect(info.CA).NotTo(BeNil())
		expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason)
	})

	t.Run("a_stored_kubeconfig_that_keeps_failing_TLS_is_rechecked_at_most_every_ten_minutes", func(t *testing.T) {
		g := NewWithT(t)
		f, prober, value := roksStoredFixture(t, time.Hour, nil)
		ca := newTestClusterCA(t, cpTestStart.AddDate(9, 0, 0))
		prober.setAnswer(failTLS)
		recheck := func() {
			gomock.InOrder(f.expectObserve(roksCluster()),
				f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: ca.pem}, nil))
		}
		observeOnly := func() { f.expectObserve(roksCluster()) }
		wantMessage := "A new kubeconfig of IBM cluster iksclusterid00000001 did not pass the probe against " + cpTestServer +
			": SelfSubjectReview failed (TLS); CAPIBM keeps Secret dev-kubeconfig as it is and tries the refresh again after a backoff"

		for _, step := range []struct {
			after       time.Duration
			expect      func()
			wantRequeue time.Duration
		}{
			{after: 0, expect: recheck, wantRequeue: 5 * time.Minute},
			{after: time.Second, expect: observeOnly, wantRequeue: 5 * time.Minute},
			{after: 6 * time.Minute, expect: observeOnly, wantRequeue: 4 * time.Minute},
			{after: 10 * time.Minute, expect: recheck, wantRequeue: 5 * time.Minute},
		} {
			result, err := f.reconcileAt(step.after, step.expect)
			g.Expect(err).NotTo(HaveOccurred(), "after %s", step.after)
			expectRequeue(g, result, step.wantRequeue)
			g.Expect(f.kubeconfigSecret().Data["value"]).To(Equal(value))
			condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
				metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason)
			g.Expect(condition.Message).To(Equal(wantMessage), "after %s", step.after)
		}
	})

	t.Run("a_TLS_failure_soon_after_another_attempt_waits_and_says_what_comes", func(t *testing.T) {
		g := NewWithT(t)
		f, prober, _ := roksStoredFixture(t, time.Hour, map[string]string{
			managedscope.KubeconfigRefreshCheckedAtAnnotation: "2026-11-16T03:03:30Z"})
		prober.setAnswer(failTLS)
		f.expectObserve(roksCluster()) // the re-check is due at 03:13:30: no IBM call yet

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason)
		g.Expect(condition.Message).To(Equal("The server certificate of " + cpTestServer + " does not verify with the trust of the " +
			"kubeconfig stored in Secret dev-kubeconfig (SelfSubjectReview failed (TLS)); CAPIBM keeps it and tries IBM's cluster CA " +
			"and the system roots again, at most once every 10 minutes"))
	})
}
