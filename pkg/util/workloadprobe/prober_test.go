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

package workloadprobe

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
)

const (
	adminCN = "KubeCert#<synthetic-iam-id>"
	// localFile is a path on the controller's file system, which the prober must never read.
	localFile = "/etc/probe/local-file"
)

// testEnv holds the CAs of the trust tests: clusterCA plays an IKS cluster CA (it signs IKS serving certificates and
// the admin client certificates), publicCA plays a public root in the system pool (it signs the ROKS "-e" serving
// certificate, which a public CA issues).
type testEnv struct {
	clusterCA, publicCA *testCA
	admin               managedkubeconfig.ClientCredential
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	clusterCA := newTestCA(t, "<cluster-id>-kubernetes-ca-1")
	return testEnv{
		clusterCA: clusterCA,
		publicCA:  newTestCA(t, "In-test public root CA"),
		admin:     clusterCA.issueClient(t, adminCN, adminGroup),
	}
}

// kubeconfigFor builds the kubeconfig CAPIBM would write for srv with the given trust mode and credential.
func (e testEnv) kubeconfigFor(t *testing.T, srv *fakeAPIServer, trust managedkubeconfig.CATrust, cred managedkubeconfig.ClientCredential) []byte {
	t.Helper()
	in := managedkubeconfig.Input{ClusterName: "probe", Server: srv.URL, Trust: trust, Credential: cred}
	if trust == managedkubeconfig.ClusterCA {
		in.CertificateAuthorityPEM = e.clusterCA.pem
	}
	out, err := managedkubeconfig.Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return out
}

// newProber returns a RESTProber whose "system roots" are the in-test public CA.
func (e testEnv) newProber(t *testing.T, timeout time.Duration) *RESTProber {
	t.Helper()
	p, err := NewRESTProber(RESTProberOptions{Timeout: timeout, SystemRootsPEM: e.publicCA.pem})
	if err != nil {
		t.Fatalf("NewRESTProber: %v", err)
	}
	return p
}

// TestProbeTrust covers which CA trust mode verifies which master: the cluster CA for IKS, the system roots for ROKS.
func TestProbeTrust(t *testing.T) {
	env := newTestEnv(t)
	iks := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
	roks := newFakeAPIServer(t, env.publicCA, env.clusterCA, func(s *fakeAPIServer) { s.gitVersion = "v1.34.9" })
	prober := env.newProber(t, 5*time.Second)

	testCases := []struct {
		name       string
		server     *fakeAPIServer
		trust      managedkubeconfig.CATrust
		wantReason FailureReason
		wantGit    string
	}{
		{name: "IKS cluster CA trust passes", server: iks, trust: managedkubeconfig.ClusterCA, wantGit: "v1.36.5+IKS"},
		{name: "IKS system roots fail TLS", server: iks, trust: managedkubeconfig.SystemRoots, wantReason: FailureTLS},
		{name: "ROKS system roots pass", server: roks, trust: managedkubeconfig.SystemRoots, wantGit: "v1.34.9"},
		{name: "ROKS cluster CA trust fails TLS", server: roks, trust: managedkubeconfig.ClusterCA, wantReason: FailureTLS},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			result, err := prober.Probe(context.Background(), env.kubeconfigFor(t, tc.server, tc.trust, env.admin))
			if tc.wantReason != "" {
				g.Expect(ReasonOf(err)).To(Equal(tc.wantReason), "error: %v", err)
				g.Expect(result).To(Equal(Result{}))
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(result).To(Equal(Result{Username: adminCN, Groups: []string{adminGroup, authenticated}, GitVersion: tc.wantGit, ClusterAdmin: true}))
		})
	}
}

func TestProbe(t *testing.T) {
	env := newTestEnv(t)
	prober := env.newProber(t, 5*time.Second)

	t.Run("cluster admin does not depend on the user name", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
		other := env.clusterCA.issueClient(t, "KubeCert#<synthetic-iam-id-b>", adminGroup)
		result, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, other))
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(result.Username).To(Equal("KubeCert#<synthetic-iam-id-b>"))
		g.Expect(result.ClusterAdmin).To(BeTrue())
	})

	t.Run("credential without cluster-admin", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
		viewer := env.clusterCA.issueClient(t, "viewer", "viewers")
		result, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, viewer))
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(result).To(Equal(Result{Username: "viewer", Groups: []string{"viewers", authenticated}, GitVersion: "v1.36.5+IKS"}))
	})

	t.Run("401 for a credential the server does not accept", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
		foreign := newTestCA(t, "another-cluster-ca").issueClient(t, adminCN, adminGroup)
		_, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, foreign))
		g.Expect(ReasonOf(err)).To(Equal(FailureUnauthorized))
		g.Expect(err.Error()).To(ContainSubstring("SelfSubjectReview failed (Unauthorized)"))
		g.Expect(srv.requests.Load()).To(BeEquivalentTo(1))
	})

	t.Run("403 naming the user keeps the user out of the error", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, func(s *fakeAPIServer) { s.accessStatus = http.StatusForbidden })
		_, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin))
		g.Expect(ReasonOf(err)).To(Equal(FailureOther))
		g.Expect(apierrors.IsForbidden(err)).To(BeTrue())
		// kube-apiserver's 403 text names the user (KubeCert#<caller-iam-id> for IBM's admin certificate); callers
		// put ProbeError texts into conditions and logs, which must never reveal an IAM identity, so it must not be there.
		g.Expect(err.Error()).ToNot(ContainSubstring(adminCN))
		g.Expect(err.Error()).ToNot(ContainSubstring("synthetic-iam-id"))
		g.Expect(err.Error()).To(ContainSubstring("SelfSubjectAccessReview failed (Other): the API server answered HTTP 403 (Forbidden)"))
	})

	t.Run("TLS alert of a server that rejects the client certificate is not Unreachable", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, func(s *fakeAPIServer) { s.requireClientCert = true })
		foreign := newTestCA(t, "another-cluster-ca").issueClient(t, adminCN, adminGroup)
		_, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, foreign))
		g.Expect(ReasonOf(err)).To(Equal(FailureOther), "error: %v", err)
		g.Expect(err.Error()).To(ContainSubstring("remote error: tls:"))
		// The handshake failed, so no request reached the handler.
		g.Expect(srv.requests.Load()).To(BeZero())
	})

	t.Run("bearer token of a user-provided kubeconfig", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
		kubeconfig := writeKubeconfig(t, &clientcmdapi.Cluster{Server: srv.URL, CertificateAuthorityData: env.clusterCA.pem},
			&clientcmdapi.AuthInfo{Token: testToken})
		result, err := prober.Probe(context.Background(), kubeconfig)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(result.Username).To(Equal("system:serviceaccount:kube-system:robot"))
		g.Expect(result.ClusterAdmin).To(BeFalse())
	})

	t.Run("redirect within the same server is followed", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, func(s *fakeAPIServer) { s.redirectVersion = true })
		result, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin))
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(result.GitVersion).To(Equal("v1.36.5+IKS"))
	})

	t.Run("redirect to another host is refused and that host never sees the credential", func(t *testing.T) {
		g := NewWithT(t)
		elsewhere := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, func(s *fakeAPIServer) { s.redirectReviewTo = elsewhere.URL })
		_, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin))
		g.Expect(ReasonOf(err)).To(Equal(FailureOther))
		g.Expect(err.Error()).To(ContainSubstring("refusing a redirect"))
		g.Expect(elsewhere.requests.Load()).To(BeZero())
	})

	statusCases := []struct {
		name       string
		configure  func(*fakeAPIServer)
		wantStep   string
		wantReason FailureReason
	}{
		{name: "SelfSubjectReview 500", configure: func(s *fakeAPIServer) { s.reviewStatus = http.StatusInternalServerError },
			wantStep: stepSelfSubjectReview, wantReason: FailureOther},
		{name: "SelfSubjectAccessReview 403", configure: func(s *fakeAPIServer) { s.accessStatus = http.StatusForbidden },
			wantStep: stepAccessReview, wantReason: FailureOther},
		{name: "version that is not JSON", configure: func(s *fakeAPIServer) { s.versionBody = "<html>" },
			wantStep: stepVersion, wantReason: FailureOther},
	}
	for _, tc := range statusCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, tc.configure)
			result, err := prober.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin))
			g.Expect(result).To(Equal(Result{}))
			var probeErr *ProbeError
			g.Expect(err).To(BeAssignableToTypeOf(probeErr))
			g.Expect(err.(*ProbeError).Step).To(Equal(tc.wantStep))
			g.Expect(ReasonOf(err)).To(Equal(tc.wantReason))
		})
	}

	t.Run("timeout", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, func(s *fakeAPIServer) { s.stall = true })
		quick := env.newProber(t, 300*time.Millisecond)
		start := time.Now()
		_, err := quick.Probe(context.Background(), env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin))
		g.Expect(ReasonOf(err)).To(Equal(FailureUnreachable), "error: %v", err)
		g.Expect(time.Since(start)).To(BeNumerically("<", 5*time.Second))
	})

	t.Run("connection refused", func(t *testing.T) {
		g := NewWithT(t)
		srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
		kubeconfig := env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin)
		srv.Close()
		_, err := prober.Probe(context.Background(), kubeconfig)
		g.Expect(ReasonOf(err)).To(Equal(FailureUnreachable), "error: %v", err)
	})
}

func TestProbeRejectsKubeconfigs(t *testing.T) {
	env := newTestEnv(t)
	prober := env.newProber(t, 5*time.Second)
	srv := newFakeAPIServer(t, env.clusterCA, env.clusterCA, nil)
	base := env.kubeconfigFor(t, srv, managedkubeconfig.ClusterCA, env.admin)

	testCases := []struct {
		name    string
		change  func(*clientcmdapi.Config)
		raw     []byte
		wantErr string
	}{
		{name: "not a kubeconfig", raw: []byte("{not: [yaml"), wantErr: "does not parse"},
		{name: "no current context", change: func(c *clientcmdapi.Config) { c.CurrentContext = "" }, wantErr: "current-context"},
		{name: "context names a missing cluster", change: func(c *clientcmdapi.Config) { c.Contexts[c.CurrentContext].Cluster = "missing" },
			wantErr: `names cluster "missing"`},
		{name: "context names a missing user", change: func(c *clientcmdapi.Config) { c.Contexts[c.CurrentContext].AuthInfo = "missing" },
			wantErr: `names user "missing"`},
		{name: "http server", change: func(c *clientcmdapi.Config) {
			u, _ := url.Parse(c.Clusters["probe"].Server)
			u.Scheme = "http"
			c.Clusters["probe"].Server = u.String()
		}, wantErr: "is not an https URL"},
		{name: "insecure skip TLS verify", change: func(c *clientcmdapi.Config) {
			c.Clusters["probe"].CertificateAuthorityData = nil
			c.Clusters["probe"].InsecureSkipTLSVerify = true
		}, wantErr: "disables TLS verification"},
		{name: "CA file", change: func(c *clientcmdapi.Config) {
			c.Clusters["probe"].CertificateAuthorityData = nil
			c.Clusters["probe"].CertificateAuthority = localFile
		}, wantErr: "references a CA file"},
		{name: "exec plugin", change: func(c *clientcmdapi.Config) {
			c.AuthInfos["probe-admin"].Exec = &clientcmdapi.ExecConfig{Command: "/bin/true", APIVersion: "client.authentication.k8s.io/v1"}
		}, wantErr: "exec plugin"},
		{name: "auth provider", change: func(c *clientcmdapi.Config) {
			c.AuthInfos["probe-admin"].AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "oidc"}
		}, wantErr: "auth-provider"},
		{name: "client certificate file", change: func(c *clientcmdapi.Config) {
			u := c.AuthInfos["probe-admin"]
			u.ClientCertificateData, u.ClientCertificate = nil, localFile
		}, wantErr: "references certificate, key or token files"},
		{name: "token file", change: func(c *clientcmdapi.Config) {
			c.AuthInfos["probe-admin"] = &clientcmdapi.AuthInfo{TokenFile: localFile}
		}, wantErr: "references certificate, key or token files"},
		{name: "unusable key", change: func(c *clientcmdapi.Config) {
			c.AuthInfos["probe-admin"].ClientKeyData = []byte("not a key")
		}, wantErr: "building the HTTP transport"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			data := tc.raw
			if tc.change != nil {
				data = mutate(t, base, tc.change)
			}
			before := srv.requests.Load()
			result, err := prober.Probe(context.Background(), data)
			g.Expect(ReasonOf(err)).To(Equal(FailureInvalidKubeconfig))
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(result).To(Equal(Result{}))
			// Nothing was sent.
			g.Expect(srv.requests.Load()).To(Equal(before))
		})
	}
}

func TestNewRESTProber(t *testing.T) {
	ca := newTestCA(t, "roots")
	testCases := []struct {
		name    string
		opts    RESTProberOptions
		wantErr string
	}{
		{name: "default timeout, system roots", opts: RESTProberOptions{Timeout: DefaultTimeout}},
		{name: "injected roots", opts: RESTProberOptions{Timeout: time.Second, SystemRootsPEM: ca.pem}},
		{name: "zero timeout", opts: RESTProberOptions{}, wantErr: "Timeout is 0s"},
		{name: "negative timeout", opts: RESTProberOptions{Timeout: -time.Second}, wantErr: "want a positive duration"},
		{name: "roots that are not PEM", opts: RESTProberOptions{Timeout: time.Second, SystemRootsPEM: []byte("garbage")}, wantErr: "holds no PEM certificate"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			p, err := NewRESTProber(tc.opts)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(p).To(BeNil())
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(p.timeout).To(Equal(tc.opts.Timeout))
			g.Expect(p.systemRootsPEM).To(Equal(append([]byte(nil), tc.opts.SystemRootsPEM...)))
		})
	}
}

func TestSameHostRedirectsOnly(t *testing.T) {
	first := &http.Request{URL: &url.URL{Scheme: "https", Host: "c100.example:31000", Path: "/version"}}
	testCases := []struct {
		name    string
		target  url.URL
		via     int
		wantErr string
	}{
		{name: "same host", target: url.URL{Scheme: "https", Host: "c100.example:31000", Path: "/v"}, via: 1},
		{name: "other port", target: url.URL{Scheme: "https", Host: "c100.example:443", Path: "/v"}, via: 1, wantErr: "refusing a redirect"},
		{name: "other host", target: url.URL{Scheme: "https", Host: "evil.example:31000", Path: "/v"}, via: 1, wantErr: "refusing a redirect"},
		{name: "downgrade to http", target: url.URL{Scheme: "http", Host: "c100.example:31000", Path: "/v"}, via: 1, wantErr: "refusing a redirect"},
		{name: "too many", target: url.URL{Scheme: "https", Host: "c100.example:31000", Path: "/v"}, via: maxRedirects, wantErr: "stopped after 10 redirects"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			via := make([]*http.Request, tc.via)
			for i := range via {
				via[i] = first
			}
			err := sameHostRedirectsOnly(&http.Request{URL: &tc.target}, via)
			if tc.wantErr == "" {
				g.Expect(err).ToNot(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

// writeKubeconfig serialises a one-context kubeconfig.
func writeKubeconfig(t *testing.T, cluster *clientcmdapi.Cluster, user *clientcmdapi.AuthInfo) []byte {
	t.Helper()
	out, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"c": cluster},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"u": user},
		Contexts:       map[string]*clientcmdapi.Context{"u@c": {Cluster: "c", AuthInfo: "u"}},
		CurrentContext: "u@c",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return out
}

// mutate loads kubeconfig, applies change and serialises it again.
func mutate(t *testing.T, kubeconfig []byte, change func(*clientcmdapi.Config)) []byte {
	t.Helper()
	cfg, err := clientcmd.Load(kubeconfig)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	change(cfg)
	out, err := clientcmd.Write(*cfg)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return out
}
