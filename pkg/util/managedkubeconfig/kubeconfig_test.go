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

package managedkubeconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	logsjson "k8s.io/component-base/logs/json"
)

const (
	testCluster = "capi-managed"
	testServer  = "https://c100.us-south.containers.cloud.ibm.com:31000"
)

// fixture is a CA and an admin-shaped client credential valid around a fixed instant.
type fixture struct {
	ca   *testCA
	cred ClientCredential
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	start := time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)
	ca := newTestCA(t, "<cluster-id>-kubernetes-ca-1", start.Add(-time.Hour), start.AddDate(10, 0, 0))
	return fixture{ca: ca, cred: ca.issueClient(t, "KubeCert#<synthetic-iam-id>", start, start.AddDate(2, 0, 0))}
}

// iksInput returns a valid IKS-shaped Input (cluster CA trust).
func (f fixture) iksInput() Input {
	return Input{ClusterName: testCluster, Server: testServer, Trust: ClusterCA, CertificateAuthorityPEM: f.ca.pem, Credential: f.cred}
}

// roksInput returns a valid ROKS-shaped Input (system roots, no CA data).
func (f fixture) roksInput() Input {
	return Input{ClusterName: testCluster, Server: "https://c100-e.us-south.containers.cloud.ibm.com:32000", Trust: SystemRoots, Credential: f.cred}
}

// mustBuild builds in or fails the test.
func mustBuild(t *testing.T, in Input) []byte {
	t.Helper()
	out, err := Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
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

// onlyUser returns the single user entry of cfg.
func onlyUser(cfg *clientcmdapi.Config) *clientcmdapi.AuthInfo {
	for _, u := range cfg.AuthInfos {
		return u
	}
	return nil
}

// onlyCluster returns the single cluster entry of cfg.
func onlyCluster(cfg *clientcmdapi.Config) *clientcmdapi.Cluster {
	for _, c := range cfg.Clusters {
		return c
	}
	return nil
}

func fingerprintOf(der []byte) string {
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestBuild(t *testing.T) {
	f := newFixture(t)

	testCases := []struct {
		name     string
		in       Input
		wantCA   bool
		wantMode CATrust
	}{
		{name: "IKS: cluster CA embedded", in: f.iksInput(), wantCA: true, wantMode: ClusterCA},
		{name: "ROKS: no CA data, system roots", in: f.roksInput(), wantCA: false, wantMode: SystemRoots},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			out, err := Build(tc.in)
			g.Expect(err).ToNot(HaveOccurred())

			cfg, err := clientcmd.Load(out)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cfg.Clusters).To(HaveLen(1))
			g.Expect(cfg.AuthInfos).To(HaveLen(1))
			g.Expect(cfg.Contexts).To(HaveLen(1))
			g.Expect(cfg.CurrentContext).To(Equal(testCluster + "-admin@" + testCluster))
			kctx := cfg.Contexts[cfg.CurrentContext]
			g.Expect(kctx.Cluster).To(Equal(testCluster))
			g.Expect(kctx.AuthInfo).To(Equal(testCluster + "-admin"))

			cluster := cfg.Clusters[testCluster]
			g.Expect(cluster.Server).To(Equal(tc.in.Server))
			g.Expect(cluster.InsecureSkipTLSVerify).To(BeFalse())
			g.Expect(cluster.CertificateAuthority).To(BeEmpty())
			if tc.wantCA {
				g.Expect(cluster.CertificateAuthorityData).To(Equal(f.ca.pem))
			} else {
				g.Expect(cluster.CertificateAuthorityData).To(BeEmpty())
			}

			// Inline certificate and key only; never exec, auth-provider, token or file paths.
			user := cfg.AuthInfos[testCluster+"-admin"]
			g.Expect(user.ClientCertificateData).To(Equal(f.cred.CertificatePEM))
			g.Expect(user.ClientKeyData).To(Equal(f.cred.KeyPEM))
			g.Expect(user.Exec).To(BeNil())
			g.Expect(user.AuthProvider).To(BeNil())
			g.Expect(user.Token).To(BeEmpty())
			g.Expect(user.TokenFile).To(BeEmpty())
			g.Expect(user.ClientCertificate).To(BeEmpty())
			g.Expect(user.ClientKey).To(BeEmpty())
			g.Expect(string(out)).ToNot(ContainSubstring("insecure"))

			// Build and Inspect agree on the shape.
			info, err := Inspect(out)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(info.Trust).To(Equal(tc.wantMode))
			g.Expect(info.Server).To(Equal(tc.in.Server))

			// Deterministic output lets the caller skip unchanged Secret writes.
			again, err := Build(tc.in)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(again).To(Equal(out))
		})
	}
}

func TestBuildAcceptsRSAKeys(t *testing.T) {
	g := NewWithT(t)
	f := newFixture(t)
	in := f.iksInput()
	in.Credential = f.ca.issueRSAClient(t, "KubeCert#<synthetic-iam-id>", time.Now(), time.Now().AddDate(2, 0, 0))

	out, err := Build(in)
	g.Expect(err).ToNot(HaveOccurred())
	_, err = Inspect(out)
	g.Expect(err).ToNot(HaveOccurred())
}

func TestBuildRejectsInvalidInput(t *testing.T) {
	f := newFixture(t)
	other := newFixture(t)
	keyAsCA := Input{ClusterName: testCluster, Server: testServer, Trust: ClusterCA, CertificateAuthorityPEM: f.cred.KeyPEM, Credential: f.cred}

	testCases := []struct {
		name    string
		change  func(*Input)
		in      *Input
		wantErr string
	}{
		{name: "empty cluster name", change: func(in *Input) { in.ClusterName = "" }, wantErr: "ClusterName is empty"},
		{name: "empty server", change: func(in *Input) { in.Server = "" }, wantErr: "server URL is empty"},
		{name: "http server", change: func(in *Input) { in.Server = "http://c100.us-south.containers.cloud.ibm.com:31000" }, wantErr: "is not an https URL"},
		{name: "server without host", change: func(in *Input) { in.Server = "https:///path" }, wantErr: "is not an https URL"},
		{name: "server with user info", change: func(in *Input) { in.Server = "https://user:pass@c100.example:443" }, wantErr: "without user info"},
		{name: "server does not parse", change: func(in *Input) { in.Server = "https://[::1" }, wantErr: "does not parse"},
		{name: "unknown trust mode", change: func(in *Input) { in.Trust = "Insecure" }, wantErr: `CA trust mode "Insecure" is not supported`},
		{name: "empty trust mode", change: func(in *Input) { in.Trust = "" }, wantErr: "is not supported"},
		{name: "cluster CA trust without CA", change: func(in *Input) { in.CertificateAuthorityPEM = nil }, wantErr: "needs Input.CertificateAuthorityPEM"},
		{name: "cluster CA that is not PEM", change: func(in *Input) { in.CertificateAuthorityPEM = []byte("not pem") }, wantErr: "holds no PEM certificate"},
		{name: "cluster CA that is a key", in: &keyAsCA, wantErr: "want CERTIFICATE"},
		{name: "system roots with CA data", change: func(in *Input) { in.Trust = SystemRoots }, wantErr: "embeds no CA data"},
		{name: "empty certificate", change: func(in *Input) { in.Credential.CertificatePEM = nil }, wantErr: "client certificate or key is empty"},
		{name: "empty key", change: func(in *Input) { in.Credential.KeyPEM = nil }, wantErr: "client certificate or key is empty"},
		{name: "key of another certificate", change: func(in *Input) { in.Credential.KeyPEM = other.cred.KeyPEM }, wantErr: "do not form a valid pair"},
		{name: "trailing data after the certificate", change: func(in *Input) {
			in.Credential.CertificatePEM = append(append([]byte{}, f.cred.CertificatePEM...), []byte("garbage")...)
		}, wantErr: "trailing data"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := f.iksInput()
			if tc.in != nil {
				in = *tc.in
			}
			if tc.change != nil {
				tc.change(&in)
			}
			out, err := Build(in)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(out).To(BeNil())
		})
	}
}

func TestInspect(t *testing.T) {
	g := NewWithT(t)
	f := newFixture(t)

	info, err := Inspect(mustBuild(t, f.iksInput()))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(info.Server).To(Equal(testServer))
	g.Expect(info.Trust).To(Equal(ClusterCA))
	g.Expect(info.CA).ToNot(BeNil())
	g.Expect(info.CA.Fingerprint).To(Equal(fingerprintOf(f.ca.cert.Raw)))
	g.Expect(info.CA.NotAfter).To(Equal(f.ca.cert.NotAfter))
	g.Expect(info.CA.NotBefore).To(Equal(f.ca.cert.NotBefore))

	// The credential fingerprint is the SHA-256 of the leaf DER, the same hex as Kubernetes' audit credential ID.
	leafDER := derOf(t, f.cred.CertificatePEM)
	g.Expect(info.Certificate.Fingerprint).To(Equal(fingerprintOf(leafDER)))
	g.Expect(info.Certificate.NotBefore).To(Equal(time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)))
	g.Expect(info.Certificate.NotAfter).To(Equal(time.Date(2028, 11, 16, 3, 40, 8, 0, time.UTC)))

	roks, err := Inspect(mustBuild(t, f.roksInput()))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(roks.Trust).To(Equal(SystemRoots))
	g.Expect(roks.CA).To(BeNil())
	g.Expect(roks.Certificate).To(Equal(info.Certificate))
}

func TestInspectRejects(t *testing.T) {
	f := newFixture(t)
	base := mustBuild(t, f.iksInput())

	testCases := []struct {
		name    string
		change  func(*clientcmdapi.Config)
		raw     []byte
		wantErr string
	}{
		{name: "not a kubeconfig", raw: []byte("{not: [yaml"), wantErr: "does not parse"},
		{name: "two contexts", change: func(c *clientcmdapi.Config) {
			c.Contexts["second"] = &clientcmdapi.Context{Cluster: testCluster, AuthInfo: testCluster + "-admin"}
		}, wantErr: "exactly one cluster, one user and one context"},
		{name: "no current context", change: func(c *clientcmdapi.Config) { c.CurrentContext = "" }, wantErr: "no current-context"},
		{name: "undefined current context", change: func(c *clientcmdapi.Config) { c.CurrentContext = "missing" }, wantErr: `current-context "missing" is not defined`},
		{name: "context names a missing cluster", change: func(c *clientcmdapi.Config) {
			c.Contexts[c.CurrentContext].Cluster = "missing"
		}, wantErr: `names cluster "missing"`},
		{name: "context names a missing user", change: func(c *clientcmdapi.Config) {
			c.Contexts[c.CurrentContext].AuthInfo = "missing"
		}, wantErr: `names user "missing"`},
		{name: "http server", change: func(c *clientcmdapi.Config) { onlyCluster(c).Server = "http://c100.example:80" }, wantErr: "is not an https URL"},
		{name: "insecure skip TLS verify", change: func(c *clientcmdapi.Config) {
			onlyCluster(c).CertificateAuthorityData = nil
			onlyCluster(c).InsecureSkipTLSVerify = true
		}, wantErr: "disables TLS verification"},
		{name: "CA file", change: func(c *clientcmdapi.Config) {
			onlyCluster(c).CertificateAuthorityData = nil
			onlyCluster(c).CertificateAuthority = "/etc/ca.crt"
		}, wantErr: "references a CA file"},
		{name: "proxy URL", change: func(c *clientcmdapi.Config) { onlyCluster(c).ProxyURL = "https://proxy.example" }, wantErr: "proxy-url or tls-server-name"},
		{name: "TLS server name", change: func(c *clientcmdapi.Config) { onlyCluster(c).TLSServerName = "other.example" }, wantErr: "proxy-url or tls-server-name"},
		{name: "CA data that is not a certificate", change: func(c *clientcmdapi.Config) {
			onlyCluster(c).CertificateAuthorityData = []byte("garbage")
		}, wantErr: "certificate authority holds no PEM certificate"},
		{name: "token", change: func(c *clientcmdapi.Config) { onlyUser(c).Token = "secret-token" }, wantErr: "uses a bearer token"},
		{name: "token file", change: func(c *clientcmdapi.Config) { onlyUser(c).TokenFile = "/var/run/token" }, wantErr: "uses a bearer token"},
		{name: "exec plugin", change: func(c *clientcmdapi.Config) {
			onlyUser(c).Exec = &clientcmdapi.ExecConfig{Command: "ibmcloud", APIVersion: "client.authentication.k8s.io/v1"}
		}, wantErr: "uses an exec plugin"},
		{name: "auth provider", change: func(c *clientcmdapi.Config) {
			onlyUser(c).AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "oidc"}
		}, wantErr: "uses an auth-provider"},
		{name: "basic auth", change: func(c *clientcmdapi.Config) { onlyUser(c).Username = "admin" }, wantErr: "uses basic authentication"},
		{name: "certificate file", change: func(c *clientcmdapi.Config) { onlyUser(c).ClientCertificate = "/etc/client.crt" }, wantErr: "references certificate or key files"},
		{name: "impersonation", change: func(c *clientcmdapi.Config) { onlyUser(c).Impersonate = "system:admin" }, wantErr: "impersonates another user"},
		{name: "impersonated groups", change: func(c *clientcmdapi.Config) {
			onlyUser(c).ImpersonateGroups = []string{"system:masters"}
		}, wantErr: "impersonates another user"},
		{name: "missing key data", change: func(c *clientcmdapi.Config) { onlyUser(c).ClientKeyData = nil }, wantErr: "lacks client-certificate-data or client-key-data"},
		{name: "mismatched key", change: func(c *clientcmdapi.Config) {
			onlyUser(c).ClientKeyData = newFixture(t).cred.KeyPEM
		}, wantErr: "do not form a valid pair"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			data := tc.raw
			if tc.change != nil {
				data = mutate(t, base, tc.change)
			}
			_, err := Inspect(data)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			// No secret material in any error.
			g.Expect(err.Error()).ToNot(ContainSubstring("PRIVATE KEY"))
			g.Expect(err.Error()).ToNot(ContainSubstring("secret-token"))
		})
	}
}

func TestExtractClientCredential(t *testing.T) {
	f := newFixture(t)
	base := mustBuild(t, f.iksInput())

	t.Run("CAPIBM kubeconfig", func(t *testing.T) {
		g := NewWithT(t)
		cred, err := ExtractClientCredential(base)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(cred.CertificatePEM).To(Equal(f.cred.CertificatePEM))
		g.Expect(cred.KeyPEM).To(Equal(f.cred.KeyPEM))
	})

	t.Run("user-provided kubeconfig with further entries", func(t *testing.T) {
		g := NewWithT(t)
		data := mutate(t, base, func(c *clientcmdapi.Config) {
			c.Clusters["elsewhere"] = &clientcmdapi.Cluster{Server: "https://elsewhere.example"}
			c.Clusters["elsewhere"].InsecureSkipTLSVerify = true
			c.AuthInfos["robot"] = &clientcmdapi.AuthInfo{Token: "secret-token"}
			c.Contexts["robot@elsewhere"] = &clientcmdapi.Context{Cluster: "elsewhere", AuthInfo: "robot"}
		})
		cred, err := ExtractClientCredential(data)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(cred.KeyPEM).To(Equal(f.cred.KeyPEM))
	})

	testCases := []struct {
		name    string
		data    []byte
		wantErr string
		// unsupported is true when the error must wrap ErrUnsupportedCredential: the Secret is a valid kubeconfig, but
		// its user is not an inline client certificate and key, so the ownership proof cannot rebuild it.
		unsupported bool
	}{
		{name: "not a kubeconfig", data: []byte("{not: [yaml"), wantErr: "does not parse"},
		{name: "no current context", data: mutate(t, base, func(c *clientcmdapi.Config) { c.CurrentContext = "" }), wantErr: "no current-context"},
		{name: "token user of a user-provided Secret", data: mutate(t, base, func(c *clientcmdapi.Config) {
			u := onlyUser(c)
			u.ClientCertificateData, u.ClientKeyData, u.Token = nil, nil, "secret-token"
		}), wantErr: "uses a bearer token", unsupported: true},
		{name: "exec user of a user-provided Secret", data: mutate(t, base, func(c *clientcmdapi.Config) {
			u := onlyUser(c)
			u.ClientCertificateData, u.ClientKeyData = nil, nil
			u.Exec = &clientcmdapi.ExecConfig{Command: "ibmcloud", APIVersion: "client.authentication.k8s.io/v1"}
		}), wantErr: "uses an exec plugin", unsupported: true},
		{name: "mismatched key", data: mutate(t, base, func(c *clientcmdapi.Config) {
			onlyUser(c).ClientKeyData = newFixture(t).cred.KeyPEM
		}), wantErr: "do not form a valid pair"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cred, err := ExtractClientCredential(tc.data)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(errors.Is(err, ErrUnsupportedCredential)).To(Equal(tc.unsupported))
			g.Expect(err.Error()).ToNot(ContainSubstring("secret-token"))
			g.Expect(cred).To(Equal(ClientCredential{}))
		})
	}
}

func TestClientCredentialIsRedacted(t *testing.T) {
	f := newFixture(t)
	in := f.iksInput()
	keyBlock, _ := pem.Decode(f.cred.KeyPEM)
	if keyBlock == nil {
		t.Fatal("the in-test key is not PEM")
	}
	// keyMarkers are the forms in which the key could leak: its PEM text, a slice of it, and the base64 of its PEM
	// (encoding/json writes []byte as base64) and of its DER bytes.
	keyMarkers := []string{
		"PRIVATE KEY",
		string(f.cred.KeyPEM[30:60]),
		base64.StdEncoding.EncodeToString(f.cred.KeyPEM)[:40],
		base64.StdEncoding.EncodeToString(keyBlock.Bytes)[:40],
	}

	// jsonLog logs the way the component-base JSON log format that CAPIBM registers (cmd/main.go) does: zap encodes a
	// nested struct such as Input with encoding/json, so only MarshalJSON keeps the key out of it.
	jsonLog := func(keysAndValues ...any) string {
		var buf bytes.Buffer
		logger, _ := logsjson.NewJSONLogger(0, logsjson.AddNopSync(&buf), nil, nil)
		logger.Info("probe input", keysAndValues...)
		return buf.String()
	}
	mustJSON := func(v any) string {
		out, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		return string(out)
	}

	testCases := []struct {
		name string
		out  string
	}{
		{name: "%v of a slice", out: fmt.Sprintf("%v", []ClientCredential{f.cred})},
		{name: "%v of Input", out: fmt.Sprintf("%v", in)},
		{name: "%+v of Input", out: fmt.Sprintf("%+v", in)},
		{name: "%#v of Input", out: fmt.Sprintf("%#v", in)},
		{name: "%#v of the credential", out: fmt.Sprintf("%#v", f.cred)},
		{name: "%d of the credential", out: fmt.Sprintf("%d", f.cred)},
		{name: "%x of a pointer to Input", out: fmt.Sprintf("%x", &in)},
		{name: "MarshalLog", out: fmt.Sprint(f.cred.MarshalLog())},
		{name: "json.Marshal of Input", out: mustJSON(in)},
		{name: "json.Marshal of a pointer to the credential", out: mustJSON(&f.cred)},
		{name: "JSON log of the credential", out: jsonLog("credential", f.cred)},
		{name: "JSON log of Input", out: jsonLog("input", in)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			// "redacted" without the angle brackets: encoding/json escapes them as < and >.
			g.Expect(tc.out).To(ContainSubstring("redacted"))
			for _, marker := range keyMarkers {
				g.Expect(tc.out).ToNot(ContainSubstring(marker))
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect(f.cred.String()).To(Equal("ClientCredential{<redacted>}"))
	})
}

func TestInspectPEM(t *testing.T) {
	start := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	older := newTestCA(t, "<cluster-id>-kubernetes-ca-1", start.AddDate(-1, 0, 0), start.AddDate(9, 0, 0))
	newer := newTestCA(t, "<cluster-id>-kubernetes-ca-2", start, start.AddDate(10, 0, 0))
	bundle := append(append([]byte{}, older.pem...), newer.pem...)

	t.Run("single certificate", func(t *testing.T) {
		g := NewWithT(t)
		info, err := InspectPEM(newer.pem)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(info).To(Equal(CertificateInfo{Fingerprint: fingerprintOf(newer.cert.Raw), NotBefore: newer.cert.NotBefore, NotAfter: newer.cert.NotAfter}))
	})

	t.Run("bundle: common validity window and one fingerprint for the whole bundle", func(t *testing.T) {
		g := NewWithT(t)
		info, err := InspectPEM(bundle)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(info.NotBefore).To(Equal(newer.cert.NotBefore))
		g.Expect(info.NotAfter).To(Equal(older.cert.NotAfter))
		g.Expect(info.Fingerprint).To(Equal(fingerprintOf(append(append([]byte{}, older.cert.Raw...), newer.cert.Raw...))))
	})

	f := newFixture(t)
	errorCases := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{name: "empty", data: nil, wantErr: "holds no PEM certificate"},
		{name: "a key", data: f.cred.KeyPEM, wantErr: "want CERTIFICATE"},
		{name: "garbage DER", data: []byte("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"), wantErr: "does not parse"},
		{name: "trailing data", data: append(append([]byte{}, newer.pem...), []byte("tail")...), wantErr: "trailing data"},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			_, err := InspectPEM(tc.data)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

func TestCATrust(t *testing.T) {
	testCases := []struct {
		trust     CATrust
		wantValid bool
		wantOther CATrust
	}{
		{trust: ClusterCA, wantValid: true, wantOther: SystemRoots},
		{trust: SystemRoots, wantValid: true, wantOther: ClusterCA},
		{trust: "", wantValid: false, wantOther: ""},
		{trust: "clusterca", wantValid: false, wantOther: ""},
	}
	for _, tc := range testCases {
		t.Run(string(tc.trust), func(t *testing.T) {
			g := NewWithT(t)
			err := tc.trust.Validate()
			if tc.wantValid {
				g.Expect(err).ToNot(HaveOccurred())
			} else {
				g.Expect(err).To(MatchError(ContainSubstring(`use "ClusterCA" or "SystemRoots"`)))
			}
			g.Expect(tc.trust.Other()).To(Equal(tc.wantOther))
		})
	}
}
