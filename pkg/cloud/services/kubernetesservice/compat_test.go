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

package kubernetesservice

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"

	. "github.com/onsi/gomega"
)

// testCredentials are a CA and an admin client certificate generated for one test, shaped like IBM's.
type testCredentials struct {
	caPEM   []byte
	certPEM []byte
	keyPEM  []byte
}

// newTestCredentials generates a CA and a client certificate with IBM's admin subject shape (O=ibm-admins). The CN
// uses the documents' placeholder, never a real IAM ID.
func newTestCredentials(t *testing.T) testCredentials {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "synthetic-kubernetes-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"ibm-admins"}, CommonName: "KubeCert#<caller-iam-id>"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return testCredentials{
		caPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// kubeconfigShape is the part of a *-kubeconfig-shape.json fixture that describes IBM's answer.
type kubeconfigShape struct {
	Server     string   `json:"server"`
	HasCAData  bool     `json:"hasCAData"`
	UserKeys   []string `json:"userKeys"`
	CACertKeys []string `json:"caCertKeys"`
}

// loadShape reads a *-kubeconfig-shape.json fixture.
func loadShape(t *testing.T, name string) kubeconfigShape {
	t.Helper()
	var shape kubeconfigShape
	if err := json.Unmarshal(fixture(t, name), &shape); err != nil {
		t.Fatalf("decoding %s: %v", name, err)
	}
	return shape
}

// kubeconfigOptions tweak a synthesized kubeconfig.
type kubeconfigOptions struct {
	// user changes the user entry after it is built from the shape.
	user func(user map[string]any)
	// cluster changes the cluster entry after it is built from the shape.
	cluster func(cluster map[string]any)
	// config changes the whole document last.
	config func(config map[string]any)
}

// synthesizeKubeconfig builds IBM's JSON admin kubeconfig as the shape fixture describes it (server, CA data or not,
// the user keys), with the in-test credentials, and applies opts.
func synthesizeKubeconfig(t *testing.T, shapeName string, credentials testCredentials, opts kubeconfigOptions) []byte {
	t.Helper()
	shape := loadShape(t, shapeName)
	user := map[string]any{}
	for _, key := range shape.UserKeys {
		switch key {
		case "client-certificate-data":
			user[key] = base64.StdEncoding.EncodeToString(credentials.certPEM)
		case "client-key-data":
			user[key] = base64.StdEncoding.EncodeToString(credentials.keyPEM)
		default:
			t.Fatalf("shape %s lists an unexpected user key %q", shapeName, key)
		}
	}
	cluster := map[string]any{"server": shape.Server}
	if shape.HasCAData {
		cluster["certificate-authority-data"] = base64.StdEncoding.EncodeToString(credentials.caPEM)
	}
	if opts.user != nil {
		opts.user(user)
	}
	if opts.cluster != nil {
		opts.cluster(cluster)
	}
	config := map[string]any{
		"apiVersion":      "v1",
		"kind":            "Config",
		"clusters":        []any{map[string]any{"name": "synthetic-cluster", "cluster": cluster}},
		"users":           []any{map[string]any{"name": "admin", "user": user}},
		"contexts":        []any{map[string]any{"name": "synthetic-cluster/admin", "context": map[string]any{"cluster": "synthetic-cluster", "user": "admin"}}},
		"current-context": "synthetic-cluster/admin",
	}
	if opts.config != nil {
		opts.config(config)
	}
	body, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestGetAdminKubeconfig(t *testing.T) {
	credentials := newTestCredentials(t)

	t.Run("IKS: server, cluster CA, certificate and key; request shape", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, synthesizeKubeconfig(t, "iks-kubeconfig-shape.json", credentials, kubeconfigOptions{})))
		service, limiter := newTestService(t, fake)
		admin, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(admin.Server).To(Equal("https://c111.eu-de.containers.cloud.ibm.com:30101"))
		g.Expect(admin.CertificateAuthorityPEM).To(Equal(credentials.caPEM))
		g.Expect(admin.CertificatePEM).To(Equal(credentials.certPEM))
		g.Expect(admin.KeyPEM).To(Equal(credentials.keyPEM))
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodGet))
		g.Expect(request.Path).To(Equal("/global/v2/getKubeconfig"))
		g.Expect(request.RawQuery).To(Equal("admin=true&cluster=" + testIKSClusterID + "&format=json"))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Get("Accept")).To(Equal("application/json"))
		g.Expect(request.Header.Values("X-Auth-Refresh-Token")).To(BeEmpty())
		g.Expect(limiter.count()).To(Equal(1))
	})

	t.Run("ROKS: no CA data", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, synthesizeKubeconfig(t, "roks-kubeconfig-shape.json", credentials, kubeconfigOptions{})))
		service, _ := newTestService(t, fake)
		admin, err := service.GetAdminKubeconfig(context.Background(), testROKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(admin.Server).To(Equal("https://c113-e.eu-de.containers.cloud.ibm.com:30102"))
		g.Expect(admin.CertificateAuthorityPEM).To(BeNil())
		g.Expect(admin.CertificatePEM).To(Equal(credentials.certPEM))
		g.Expect(admin.KeyPEM).To(Equal(credentials.keyPEM))
	})

	t.Run("a single context is used when current-context is empty", func(t *testing.T) {
		g := NewWithT(t)
		body := synthesizeKubeconfig(t, "iks-kubeconfig-shape.json", credentials, kubeconfigOptions{
			config: func(config map[string]any) { delete(config, "current-context") },
		})
		fake := newFakeIKS(t, reply(http.StatusOK, body))
		service, _ := newTestService(t, fake)
		admin, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(admin.KeyPEM).To(Equal(credentials.keyPEM))
	})

	rejected := []struct {
		name    string
		opts    kubeconfigOptions
		message string
	}{
		{
			name:    "a token user",
			opts:    kubeconfigOptions{user: func(user map[string]any) { user["token"] = "synthetic-token" }},
			message: "authenticates with a token, an exec plugin, an auth provider or a password",
		},
		{
			name: "an exec user",
			opts: kubeconfigOptions{user: func(user map[string]any) {
				user["exec"] = map[string]any{"apiVersion": "client.authentication.k8s.io/v1", "command": "synthetic-plugin"}
			}},
			message: "authenticates with a token, an exec plugin, an auth provider or a password",
		},
		{
			name:    "an auth-provider user",
			opts:    kubeconfigOptions{user: func(user map[string]any) { user["auth-provider"] = map[string]any{"name": "oidc"} }},
			message: "authenticates with a token, an exec plugin, an auth provider or a password",
		},
		{
			name: "a basic-auth user",
			opts: kubeconfigOptions{user: func(user map[string]any) {
				user["username"] = "admin"
				user["password"] = "synthetic"
			}},
			message: "authenticates with a token, an exec plugin, an auth provider or a password",
		},
		{
			name:    "no client key",
			opts:    kubeconfigOptions{user: func(user map[string]any) { delete(user, "client-key-data") }},
			message: "has no client-certificate-data and client-key-data",
		},
		{
			name:    "no server",
			opts:    kubeconfigOptions{cluster: func(cluster map[string]any) { delete(cluster, "server") }},
			message: "has no server",
		},
		{
			name:    "insecure TLS",
			opts:    kubeconfigOptions{cluster: func(cluster map[string]any) { cluster["insecure-skip-tls-verify"] = true }},
			message: "disables TLS verification",
		},
		{
			name:    "current-context names a missing context",
			opts:    kubeconfigOptions{config: func(config map[string]any) { config["current-context"] = "other" }},
			message: `has no usable current context (current-context "other", 1 contexts)`,
		},
		{
			name: "two contexts and no current-context",
			opts: kubeconfigOptions{config: func(config map[string]any) {
				delete(config, "current-context")
				config["contexts"] = append(config["contexts"].([]any), map[string]any{"name": "second", "context": map[string]any{"cluster": "synthetic-cluster", "user": "admin"}})
			}},
			message: "has no usable current context",
		},
		{
			name: "the context names a missing cluster",
			opts: kubeconfigOptions{config: func(config map[string]any) {
				config["contexts"] = []any{map[string]any{"name": "synthetic-cluster/admin", "context": map[string]any{"cluster": "gone", "user": "admin"}}}
			}},
			message: `names cluster "gone", which it does not contain`,
		},
		{
			name: "the context names a missing user",
			opts: kubeconfigOptions{config: func(config map[string]any) {
				config["contexts"] = []any{map[string]any{"name": "synthetic-cluster/admin", "context": map[string]any{"cluster": "synthetic-cluster", "user": "gone"}}}
			}},
			message: `names user "gone", which it does not contain`,
		},
	}
	for _, tc := range rejected {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusOK, synthesizeKubeconfig(t, "iks-kubeconfig-shape.json", credentials, tc.opts)))
			service, _ := newTestService(t, fake)
			admin, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
			g.Expect(admin).To(BeNil())
			g.Expect(err).To(MatchError(ContainSubstring(tc.message)))
			g.Expect(err.Error()).ToNot(ContainSubstring(base64.StdEncoding.EncodeToString(credentials.keyPEM)))
		})
	}

	t.Run("a body that is not a kubeconfig is not quoted", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, []byte(`{"clusters": "synthetic-secret-material"}`)))
		service, _ := newTestService(t, fake)
		_, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		g.Expect(err).To(MatchError(ContainSubstring("is not a kubeconfig")))
		g.Expect(err.Error()).ToNot(ContainSubstring("synthetic-secret-material"))
	})

	t.Run("a body above 1 MiB is refused", func(t *testing.T) {
		g := NewWithT(t)
		large := synthesizeKubeconfig(t, "iks-kubeconfig-shape.json", credentials, kubeconfigOptions{
			config: func(config map[string]any) {
				config["preferences"] = map[string]any{"padding": strings.Repeat("x", maxKubeconfigBytes)}
			},
		})
		fake := newFakeIKS(t, reply(http.StatusOK, large))
		service, _ := newTestService(t, fake)
		_, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		g.Expect(err).To(MatchError(ContainSubstring("exceeds 1048576 bytes")))
	})

	errorCases := []struct {
		name     string
		status   int
		body     []byte
		expected ErrorClass
	}{
		{name: "404 is NotFound", status: http.StatusNotFound, body: []byte(`{"code":"G0004","description":"The specified cluster could not be found.","incidentID":"inc-404"}`), expected: ClassNotFound},
		{name: "403 is PermissionDenied", status: http.StatusForbidden, body: []byte(`{"code":"","description":"synthesized: admin=true needs the Administrator role"}`), expected: ClassPermissionDenied},
		{name: "500 without a JSON body is Transient", status: http.StatusInternalServerError, body: []byte(`internal error`), expected: ClassTransient},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(tc.status, tc.body))
			service, _ := newTestService(t, fake)
			_, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
			g.Expect(Classify(OpGetAdminKubeconfig, err)).To(Equal(tc.expected))
			var apiErr *APIError
			g.Expect(errors.As(err, &apiErr)).To(BeTrue())
			g.Expect(apiErr.Operation).To(Equal(OpGetAdminKubeconfig))
			g.Expect(apiErr.StatusCode).To(Equal(tc.status))
		})
	}

	t.Run("404 keeps IBM's code and incident", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusNotFound, []byte(`{"code":"G0004","description":"The specified cluster could not be found.","incidentID":"inc-404"}`)))
		service, _ := newTestService(t, fake)
		_, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		var apiErr *APIError
		g.Expect(errors.As(err, &apiErr)).To(BeTrue())
		g.Expect(apiErr.Code).To(Equal("G0004"))
		g.Expect(apiErr.IncidentID).To(Equal("inc-404"))
	})

	t.Run("a connection error is Transient", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, nil))
		service, _ := newTestService(t, fake)
		fake.server.Close()
		_, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		g.Expect(err).To(HaveOccurred())
		g.Expect(Classify(OpGetAdminKubeconfig, err)).To(Equal(ClassTransient))
	})

	t.Run("an empty cluster ID is refused before sending", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, nil))
		service, limiter := newTestService(t, fake)
		_, err := service.GetAdminKubeconfig(context.Background(), "")
		g.Expect(err).To(MatchError(ErrInvalidRequest))
		g.Expect(fake.received()).To(BeEmpty())
		g.Expect(limiter.count()).To(BeZero())
	})
}

// TestGetAdminKubeconfigAuthentication covers the authenticator failing on the compat path, which calls it itself.
func TestGetAdminKubeconfigAuthentication(t *testing.T) {
	testCases := []struct {
		name      string
		iamStatus int
		expected  ErrorClass
		rejected  bool
	}{
		{name: "IAM 400 is Unauthorized", iamStatus: http.StatusBadRequest, expected: ClassUnauthorized, rejected: true},
		{name: "IAM 503 is Transient", iamStatus: http.StatusServiceUnavailable, expected: ClassTransient, rejected: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			iam := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.iamStatus)
				_, _ = w.Write([]byte(`{"errorCode":"BXNIM0415E","errorMessage":"Provided API key could not be found."}`))
			}))
			t.Cleanup(iam.Close)
			authenticator, err := core.NewIamAuthenticatorBuilder().SetApiKey("synthetic-api-key").SetURL(iam.URL).SetClient(iam.Client()).Build()
			g.Expect(err).ToNot(HaveOccurred())
			fake := newFakeIKS(t, reply(http.StatusOK, nil))
			limiter := &countingLimiter{}
			opts := testOptions(t, fake, limiter)
			opts.Authenticator = authenticator
			service, err := newService(opts, fake.server.Client().Transport)
			g.Expect(err).ToNot(HaveOccurred())

			_, err = service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
			g.Expect(Classify(OpGetAdminKubeconfig, err)).To(Equal(tc.expected))
			g.Expect(errors.Is(err, ErrCredentialsRejected)).To(Equal(tc.rejected))
			g.Expect(fake.received()).To(BeEmpty())
			g.Expect(limiter.count()).To(BeZero())
		})
	}
}

func TestAdminKubeconfigRedactsTheKey(t *testing.T) {
	g := NewWithT(t)
	admin := &AdminKubeconfig{Server: "https://example.invalid:443", CertificatePEM: []byte("cert"), KeyPEM: []byte("synthetic-private-key")}
	for _, format := range []string{"%v", "%+v", "%s", "%#v"} {
		text := fmt.Sprintf(format, admin)
		g.Expect(text).ToNot(ContainSubstring("synthetic-private-key"), format)
		g.Expect(text).To(ContainSubstring("<redacted>"), format)
		g.Expect(fmt.Sprintf(format, *admin)).ToNot(ContainSubstring("synthetic-private-key"), format)
	}
}

func TestGetCACertificate(t *testing.T) {
	credentials := newTestCredentials(t)
	encodedCA := base64.StdEncoding.EncodeToString(credentials.caPEM)

	t.Run("decodes the live answer shape", func(t *testing.T) {
		g := NewWithT(t)
		// Build the body from the keys observed on IBM Cloud: caCert and rotatedAt.
		answer := map[string]any{}
		for _, key := range loadShape(t, "iks-kubeconfig-shape.json").CACertKeys {
			switch key {
			case "caCert":
				answer[key] = encodedCA
			case "rotatedAt":
				answer[key] = "2026-11-16T03:40:08+0000"
			default:
				t.Fatalf("unexpected getCACert key %q", key)
			}
		}
		body, err := json.Marshal(answer)
		g.Expect(err).ToNot(HaveOccurred())
		fake := newFakeIKS(t, reply(http.StatusOK, body))
		service, limiter := newTestService(t, fake)
		ca, err := service.GetCACertificate(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(*ca).To(Equal(CACertificate{PEM: credentials.caPEM, RotatedAt: "2026-11-16T03:40:08+0000"}))
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodGet))
		g.Expect(request.Path).To(Equal("/global/v2/getCACert"))
		g.Expect(request.RawQuery).To(Equal("cluster=" + testIKSClusterID))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Get("User-Agent")).To(Equal(testUserAgent))
		g.Expect(limiter.count()).To(Equal(1))
	})

	rotatedAtCases := []struct {
		name     string
		body     string
		expected string
	}{
		{name: "rotatedAt null", body: `{"caCert":"` + encodedCA + `","rotatedAt":null}`, expected: ""},
		{name: "rotatedAt absent", body: `{"caCert":"` + encodedCA + `"}`, expected: ""},
		{name: "rotatedAt empty", body: `{"caCert":"` + encodedCA + `","rotatedAt":""}`, expected: ""},
		{name: "rotatedAt as a number is kept as text", body: `{"caCert":"` + encodedCA + `","rotatedAt":1763264408}`, expected: "1763264408"},
	}
	for _, tc := range rotatedAtCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusOK, []byte(tc.body)))
			service, _ := newTestService(t, fake)
			ca, err := service.GetCACertificate(context.Background(), testIKSClusterID)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(ca.RotatedAt).To(Equal(tc.expected))
			g.Expect(ca.PEM).To(Equal(credentials.caPEM))
		})
	}

	invalidCases := []struct {
		name    string
		body    string
		message string
	}{
		{name: "no caCert", body: `{"rotatedAt":""}`, message: "IBM's answer has no caCert"},
		{name: "caCert not base64", body: `{"caCert":"%%%"}`, message: "IBM's caCert is not base64"},
		{name: "caCert not a PEM certificate", body: `{"caCert":"` + base64.StdEncoding.EncodeToString([]byte("not a certificate")) + `"}`, message: "does not hold a PEM certificate"},
		{name: "caCert holds a private key", body: `{"caCert":"` + base64.StdEncoding.EncodeToString(credentials.keyPEM) + `"}`, message: "does not hold a PEM certificate"},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusOK, []byte(tc.body)))
			service, _ := newTestService(t, fake)
			ca, err := service.GetCACertificate(context.Background(), testIKSClusterID)
			g.Expect(ca).To(BeNil())
			g.Expect(err).To(MatchError(ContainSubstring(tc.message)))
		})
	}

	t.Run("404 is NotFound", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusNotFound, []byte(`{"code":"G0004","description":"The specified cluster could not be found."}`)))
		service, _ := newTestService(t, fake)
		_, err := service.GetCACertificate(context.Background(), testIKSClusterID)
		g.Expect(Classify(OpGetCACertificate, err)).To(Equal(ClassNotFound))
	})

	t.Run("an empty cluster ID is refused before sending", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, nil))
		service, limiter := newTestService(t, fake)
		_, err := service.GetCACertificate(context.Background(), "")
		g.Expect(err).To(MatchError(ErrInvalidRequest))
		g.Expect(fake.received()).To(BeEmpty())
		g.Expect(limiter.count()).To(BeZero())
	})
}

// syncBuffer is a bytes.Buffer safe for the logger's writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns everything written so far.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestAdminKubeconfigNeverLogged switches go-sdk-core to debug logging, as CAPIBM's VPC and PowerVS scopes do at -v=5,
// and proves that a GetAdminKubeconfig call logs neither the private key nor its base64 form. It changes the
// process-wide go-sdk-core logger, restores it in t.Cleanup, and therefore never runs in parallel.
func TestAdminKubeconfigNeverLogged(t *testing.T) {
	t.Run("admin kubeconfig key never in debug log", func(t *testing.T) {
		g := NewWithT(t)
		captured := &syncBuffer{}
		previous := core.GetLogger()
		t.Cleanup(func() { core.SetLogger(previous) })
		core.SetLogger(core.NewLogger(core.LevelError, log.New(captured, "", 0), log.New(captured, "", 0)))
		core.SetLoggingLevel(core.LevelDebug)

		credentials := newTestCredentials(t)
		kubeconfig := synthesizeKubeconfig(t, "iks-kubeconfig-shape.json", credentials, kubeconfigOptions{})
		cluster := fixture(t, "iks-getCluster-deployed.json")
		fake := newFakeIKS(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/v2/getKubeconfig") {
				reply(http.StatusOK, kubeconfig)(w, r)
				return
			}
			reply(http.StatusOK, cluster)(w, r)
		})
		service, _ := newTestService(t, fake)

		// Control: an SDK-path call is dumped at debug level, so the capture really sees go-sdk-core's debug output.
		_, err := service.GetCluster(context.Background(), testIKSClusterID, "")
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(captured.String()).To(ContainSubstring("Response:"))
		g.Expect(captured.String()).To(ContainSubstring(testIKSClusterID))

		admin, err := service.GetAdminKubeconfig(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(admin.KeyPEM).To(Equal(credentials.keyPEM))

		logged := captured.String()
		block, _ := pem.Decode(credentials.keyPEM)
		g.Expect(block).ToNot(BeNil())
		g.Expect(logged).ToNot(ContainSubstring(string(credentials.keyPEM)), "the PEM key")
		g.Expect(logged).ToNot(ContainSubstring(base64.StdEncoding.EncodeToString(credentials.keyPEM)), "client-key-data as IBM sends it")
		g.Expect(logged).ToNot(ContainSubstring(base64.StdEncoding.EncodeToString(block.Bytes)[:32]), "the key's DER body")
		g.Expect(logged).ToNot(ContainSubstring("client-key-data"))
	})
}

// TestUserAgentOnTheCompatPath proves that GetAdminKubeconfig sends CAPIBM's User-Agent, which it sets itself because
// it bypasses Request().
func TestUserAgentOnTheCompatPath(t *testing.T) {
	t.Run("user agent on the admin kubeconfig path", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, synthesizeKubeconfig(t, "roks-kubeconfig-shape.json", newTestCredentials(t), kubeconfigOptions{})))
		service, _ := newTestService(t, fake)
		_, err := service.GetAdminKubeconfig(context.Background(), testROKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(fake.last(t).Header.Values("User-Agent")).To(Equal([]string{testUserAgent}))
	})
}

// iksCreateWorkerPoolRequest is the request whose wire body is testdata/golden/createWorkerPool.json.
func iksCreateWorkerPoolRequest() CreateWorkerPoolRequest {
	return CreateWorkerPoolRequest{
		ClusterID:          testIKSClusterID,
		Name:               "example-test-cluster-pool",
		Flavor:             "bx2.4x16",
		VPCID:              testVPCID,
		WorkerCountPerZone: 2,
		Zones: []ZoneSubnet{
			{Zone: "eu-de-1", SubnetID: testSubnetID},
			{Zone: "eu-de-2", SubnetID: "02b7-00000000-0000-4000-8000-000000000003"},
		},
	}
}

func TestCreateWorkerPool(t *testing.T) {
	created := []byte(`{"workerPoolID":"` + testIKSClusterID + `-0000001"}`)

	t.Run("golden body with zones and subnets, no operatingSystem", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, servePath("/global/v2/vpc/createWorkerPool", http.StatusCreated, created))
		service, limiter := newTestService(t, fake)
		poolID, err := service.CreateWorkerPool(context.Background(), iksCreateWorkerPoolRequest())
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(poolID).To(Equal(testIKSClusterID + "-0000001"))
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodPost))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Get("Content-Type")).To(Equal("application/json"))
		g.Expect(request.Header.Get("User-Agent")).To(Equal(testUserAgent))
		g.Expect(string(request.Body)).To(MatchJSON(fixture(t, "golden/createWorkerPool.json")))
		g.Expect(limiter.count()).To(Equal(1))
	})

	t.Run("ROKS entitlement is sent", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusCreated, created))
		service, _ := newTestService(t, fake)
		req := iksCreateWorkerPoolRequest()
		req.Entitlement = "ocp_entitled"
		_, err := service.CreateWorkerPool(context.Background(), req)
		g.Expect(err).ToNot(HaveOccurred())
		var body map[string]any
		g.Expect(json.Unmarshal(fake.last(t).Body, &body)).To(Succeed())
		g.Expect(body).To(HaveKeyWithValue("entitlement", "ocp_entitled"))
		g.Expect(body).ToNot(HaveKey("operatingSystem"))
	})

	errorCases := []struct {
		name     string
		status   int
		body     []byte
		expected ErrorClass
	}{
		{name: "409 E0d39 is PoolNameExists", status: http.StatusConflict, body: []byte(`{"code":"E0d39","description":"synthesized: the worker pool name is already in use"}`), expected: ClassPoolNameExists},
		{name: "another 409 is OperationInProgress", status: http.StatusConflict, body: []byte(`{"description":"synthesized: worker pool create may already be in progress"}`), expected: ClassOperationInProgress},
		{name: "400 is BadRequest", status: http.StatusBadRequest, body: []byte(`{"code":"E0001","description":"synthesized"}`), expected: ClassBadRequest},
		{name: "500 is Transient", status: http.StatusInternalServerError, body: nil, expected: ClassTransient},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(tc.status, tc.body))
			service, _ := newTestService(t, fake)
			poolID, err := service.CreateWorkerPool(context.Background(), iksCreateWorkerPoolRequest())
			g.Expect(poolID).To(BeEmpty())
			g.Expect(Classify(OpCreateWorkerPool, err)).To(Equal(tc.expected))
			g.Expect(fake.received()).To(HaveLen(1), "CreateWorkerPool is sent exactly once")
		})
	}

	t.Run("a 201 without workerPoolID leaves the outcome unknown", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusCreated, []byte(`{}`)))
		service, _ := newTestService(t, fake)
		_, err := service.CreateWorkerPool(context.Background(), iksCreateWorkerPoolRequest())
		g.Expect(err).To(MatchError(ContainSubstring("without a workerPoolID; the outcome of this create attempt is unknown")))
		g.Expect(Classify(OpCreateWorkerPool, err)).To(Equal(ClassTransient))
	})

	validationCases := []struct {
		name     string
		mutate   func(r *CreateWorkerPoolRequest)
		messages []string
	}{
		{name: "empty cluster ID", mutate: func(r *CreateWorkerPoolRequest) { r.ClusterID = "" }, messages: []string{"ClusterID is empty"}},
		{name: "empty name", mutate: func(r *CreateWorkerPoolRequest) { r.Name = "" }, messages: []string{"Name is empty"}},
		{name: "empty flavor", mutate: func(r *CreateWorkerPoolRequest) { r.Flavor = "" }, messages: []string{"Flavor is empty"}},
		{name: "empty VPC", mutate: func(r *CreateWorkerPoolRequest) { r.VPCID = "" }, messages: []string{"VPCID is empty"}},
		{name: "zero workers", mutate: func(r *CreateWorkerPoolRequest) { r.WorkerCountPerZone = 0 }, messages: []string{"WorkerCountPerZone is 0; pass at least 1"}},
		{name: "unknown entitlement", mutate: func(r *CreateWorkerPoolRequest) { r.Entitlement = "x" }, messages: []string{`Entitlement "x" is not supported`}},
		{name: "no zones", mutate: func(r *CreateWorkerPoolRequest) { r.Zones = nil }, messages: []string{"Zones is empty"}},
		{name: "zone without subnet", mutate: func(r *CreateWorkerPoolRequest) { r.Zones[1].SubnetID = "" }, messages: []string{`Zones[1].SubnetID is empty; pass the ID of a VPC subnet in zone "eu-de-2"`}},
	}
	for _, tc := range validationCases {
		t.Run("refused before sending: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusCreated, created))
			service, limiter := newTestService(t, fake)
			req := iksCreateWorkerPoolRequest()
			tc.mutate(&req)
			_, err := service.CreateWorkerPool(context.Background(), req)
			g.Expect(err).To(MatchError(ErrInvalidRequest))
			for _, message := range tc.messages {
				g.Expect(err.Error()).To(ContainSubstring(message))
			}
			g.Expect(fake.received()).To(BeEmpty())
			g.Expect(limiter.count()).To(BeZero())
		})
	}
}

// serveZoneByZone returns a handler for the zone-by-zone fallback of CreateWorkerPool: it answers POST
// /v2/vpc/createWorkerPool with createStatus and createBody, the n-th POST /v2/vpc/createWorkerPoolZone with
// zoneStatuses[n] (201 once they run out), and 404 on any other path, so that a test also proves which paths the
// fallback calls.
func serveZoneByZone(createStatus int, createBody []byte, zoneStatuses ...int) http.HandlerFunc {
	var mu sync.Mutex
	zoneAdds := 0
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/global/v2/vpc/createWorkerPool":
			reply(createStatus, createBody)(w, r)
		case "/global/v2/vpc/createWorkerPoolZone":
			mu.Lock()
			status := http.StatusCreated
			if zoneAdds < len(zoneStatuses) {
				status = zoneStatuses[zoneAdds]
			}
			zoneAdds++
			mu.Unlock()
			var body []byte
			if status >= http.StatusBadRequest {
				body = []byte(`{"code":"E0001","description":"synthesized: the zone cannot be added"}`)
			}
			reply(status, body)(w, r)
		default:
			reply(http.StatusNotFound, []byte(`{"code":"test","description":"unexpected path"}`))(w, r)
		}
	}
}

// The zone-by-zone fallback is inactive while CreatePoolWithZones is true, so it is tested directly, like the
// /v2/vpc/getWorkerPools fallback of ListWorkerPools, so that switching is the one-line change it is meant to be.
func TestCreateWorkerPoolZoneByZone(t *testing.T) {
	poolID := testIKSClusterID + "-0000001"
	created := []byte(`{"workerPoolID":"` + poolID + `"}`)
	zoneAddBody := func(zone, subnetID string) string {
		return fmt.Sprintf(`{"cluster":%q,"id":%q,"subnetID":%q,"workerpool":%q}`, testIKSClusterID, zone, subnetID, poolID)
	}

	t.Run("zone-less SDK create, then one SDK zone add per zone in order", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, serveZoneByZone(http.StatusCreated, created))
		service, limiter := newTestService(t, fake)
		req := iksCreateWorkerPoolRequest()
		got, err := service.createWorkerPoolZoneByZone(context.Background(), req)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(got).To(Equal(poolID))
		requests := fake.received()
		g.Expect(requests).To(HaveLen(3))
		g.Expect(requests[0].Path).To(Equal("/global/v2/vpc/createWorkerPool"))
		g.Expect(string(requests[0].Body)).To(MatchJSON(fmt.Sprintf(
			`{"cluster":%q,"name":"example-test-cluster-pool","flavor":"bx2.4x16","vpcID":%q,"workerCount":2}`,
			testIKSClusterID, testVPCID)), "no zones, no operatingSystem, no entitlement")
		g.Expect(requests[1].Path).To(Equal("/global/v2/vpc/createWorkerPoolZone"))
		g.Expect(string(requests[1].Body)).To(MatchJSON(zoneAddBody(req.Zones[0].Zone, req.Zones[0].SubnetID)))
		g.Expect(requests[2].Path).To(Equal("/global/v2/vpc/createWorkerPoolZone"))
		g.Expect(string(requests[2].Body)).To(MatchJSON(zoneAddBody(req.Zones[1].Zone, req.Zones[1].SubnetID)))
		for _, request := range requests {
			g.Expect(request.Method).To(Equal(http.MethodPost))
			g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
			g.Expect(request.Header.Get("User-Agent")).To(Equal(testUserAgent))
			g.Expect(request.Header.Get("X-Auth-Resource-Group")).To(BeEmpty())
		}
		g.Expect(limiter.count()).To(Equal(3), "every request waits on the limiter once")
	})

	t.Run("ROKS entitlement is sent with the create", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, serveZoneByZone(http.StatusCreated, created))
		service, _ := newTestService(t, fake)
		req := iksCreateWorkerPoolRequest()
		req.Entitlement = "ocp_entitled"
		_, err := service.createWorkerPoolZoneByZone(context.Background(), req)
		g.Expect(err).ToNot(HaveOccurred())
		var body map[string]any
		g.Expect(json.Unmarshal(fake.received()[0].Body, &body)).To(Succeed())
		g.Expect(body).To(HaveKeyWithValue("entitlement", "ocp_entitled"))
		g.Expect(body).ToNot(HaveKey("zones"))
		g.Expect(body).ToNot(HaveKey("operatingSystem"))
	})

	createFailures := []struct {
		name     string
		status   int
		body     []byte
		expected ErrorClass
	}{
		{name: "409 E0d39 is PoolNameExists", status: http.StatusConflict, body: []byte(`{"code":"E0d39","description":"synthesized: the worker pool name is already in use"}`), expected: ClassPoolNameExists},
		{name: "500 is Transient", status: http.StatusInternalServerError, body: nil, expected: ClassTransient},
	}
	for _, tc := range createFailures {
		t.Run("a failed create adds no zone: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, serveZoneByZone(tc.status, tc.body))
			service, _ := newTestService(t, fake)
			got, err := service.createWorkerPoolZoneByZone(context.Background(), iksCreateWorkerPoolRequest())
			g.Expect(got).To(BeEmpty())
			g.Expect(Classify(OpCreateWorkerPool, err)).To(Equal(tc.expected))
			g.Expect(fake.received()).To(HaveLen(1), "the create is sent exactly once and no zone add follows")
		})
	}

	t.Run("a create without workerPoolID leaves the outcome unknown and adds no zone", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, serveZoneByZone(http.StatusCreated, []byte(`{}`)))
		service, _ := newTestService(t, fake)
		_, err := service.createWorkerPoolZoneByZone(context.Background(), iksCreateWorkerPoolRequest())
		g.Expect(err).To(MatchError(ContainSubstring("without a workerPoolID; the outcome of this create attempt is unknown")))
		g.Expect(Classify(OpCreateWorkerPool, err)).To(Equal(ClassTransient))
		g.Expect(fake.received()).To(HaveLen(1))
	})

	zoneFailures := []struct {
		name         string
		zoneStatuses []int
		requests     int
		message      string
		expected     ErrorClass
	}{
		{
			name:         "the second zone add answers 400",
			zoneStatuses: []int{http.StatusCreated, http.StatusBadRequest},
			requests:     3,
			message:      `IBM created pool "example-test-cluster-pool" with ID ` + poolID + ", but adding zone eu-de-2 with subnet 02b7-00000000-0000-4000-8000-000000000003 failed, so the pool lacks the zones eu-de-2; add them",
			expected:     ClassBadRequest,
		},
		{
			name:         "the first zone add answers 500",
			zoneStatuses: []int{http.StatusInternalServerError},
			requests:     2,
			message:      "adding zone eu-de-1 with subnet " + testSubnetID + " failed, so the pool lacks the zones eu-de-1, eu-de-2; add them",
			expected:     ClassTransient,
		},
	}
	for _, tc := range zoneFailures {
		t.Run("a failed zone add names the pool and the zones it lacks: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, serveZoneByZone(http.StatusCreated, created, tc.zoneStatuses...))
			service, _ := newTestService(t, fake)
			got, err := service.createWorkerPoolZoneByZone(context.Background(), iksCreateWorkerPoolRequest())
			g.Expect(got).To(BeEmpty())
			g.Expect(err).To(MatchError(ContainSubstring(tc.message)))
			var apiErr *APIError
			g.Expect(errors.As(err, &apiErr)).To(BeTrue(), "the zone add's APIError stays reachable")
			g.Expect(Classify(OpCreateWorkerPool, err)).To(Equal(tc.expected))
			g.Expect(fake.received()).To(HaveLen(tc.requests), "no request after the failed zone add, and none is resent")
		})
	}
}

func TestZoneNames(t *testing.T) {
	testCases := []struct {
		name     string
		zones    []ZoneSubnet
		expected string
	}{
		{name: "none", zones: nil, expected: ""},
		{name: "one", zones: []ZoneSubnet{{Zone: "eu-de-1", SubnetID: testSubnetID}}, expected: "eu-de-1"},
		{name: "several keep their order", zones: []ZoneSubnet{{Zone: "eu-de-3"}, {Zone: "eu-de-1"}}, expected: "eu-de-3, eu-de-1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(zoneNames(tc.zones)).To(Equal(tc.expected))
		})
	}
}
