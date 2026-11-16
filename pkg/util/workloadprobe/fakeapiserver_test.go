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

// Test certificates are generated in-test and the API server is an httptest server: no live IBM certificate, key,
// identity or cluster is involved. Subjects use the shape of IBM's admin certificate (O=ibm-admins,
// CN=KubeCert#<caller-iam-id>) with synthetic placeholders.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes/scheme"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
)

const (
	// testToken is the bearer token the fake server accepts for a token user (user-provided Secret parity).
	testToken     = "in-test-bearer-token"
	adminGroup    = "ibm-admins"
	authenticated = "system:authenticated"
)

// testCA is an in-test certificate authority.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

// newTestCA returns a self-signed CA valid around now.
func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// pool returns a pool holding only this CA.
func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// issueClient returns a client certificate with subject O=org, CN=cn and its key.
func (ca *testCA) issueClient(t *testing.T, cn, org string) managedkubeconfig.ClientCredential {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{org}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(2, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating client certificate: %v", err)
	}
	return managedkubeconfig.ClientCredential{
		CertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:         keyPEM(t, key),
	}
}

// issueServer returns a serving certificate for 127.0.0.1 and localhost.
func (ca *testCA) issueServer(t *testing.T) tls.Certificate {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: "in-test-apiserver"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating server certificate: %v", err)
	}
	cert, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM(t, key))
	if err != nil {
		t.Fatalf("loading server key pair: %v", err)
	}
	return cert
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	return key
}

func keyPEM(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("encoding key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func randomSerial(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generating serial: %v", err)
	}
	return serial
}

// fakeAPIServer is an httptest TLS server that authenticates client certificates the way kube-apiserver does
// (request a certificate, verify it against the client CA, answer 401 otherwise) and serves the three probe endpoints.
type fakeAPIServer struct {
	*httptest.Server
	clientCAs *x509.CertPool
	requests  atomic.Int32

	gitVersion       string
	reviewStatus     int32  // when non-zero, SelfSubjectReview answers this status
	accessStatus     int32  // when non-zero, SelfSubjectAccessReview answers this status
	versionBody      string // when set, GET /version answers this body
	redirectReviewTo string // when set, SelfSubjectReview answers 307 to this base URL
	redirectVersion  bool   // when true, GET /version answers 302 to /version-moved on the same server
	stall            bool   // when true, every request waits until the client gives up
	// requireClientCert makes the TLS layer require and verify a client certificate, unlike kube-apiserver, so a
	// foreign certificate is rejected with a TLS alert (as a TLS-terminating proxy in front of a master might do).
	requireClientCert bool
}

// newFakeAPIServer starts a server whose certificate is issued by serverCA and which accepts client certificates
// issued by clientCA. configure may change the behaviour before the server starts.
func newFakeAPIServer(t *testing.T, serverCA, clientCA *testCA, configure func(*fakeAPIServer)) *fakeAPIServer {
	t.Helper()
	s := &fakeAPIServer{clientCAs: clientCA.pool(), gitVersion: "v1.36.5+IKS"}
	if configure != nil {
		configure(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /apis/authentication.k8s.io/v1/selfsubjectreviews", s.selfSubjectReview)
	mux.HandleFunc("GET /version", s.version)
	mux.HandleFunc("GET /version-moved", s.version)
	mux.HandleFunc("POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews", s.accessReview)

	release := make(chan struct{})
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		if s.stall {
			// net/http notices a closed client connection only after the request body was read, so read it first;
			// release ends the wait at test cleanup in any case, because Close waits for running handlers.
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		mux.ServeHTTP(w, r)
	}))
	s.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCA.issueServer(t)},
		ClientAuth:   tls.RequestClientCert, // kube-apiserver requests but does not require client certificates
		MinVersion:   tls.VersionTLS12,
	}
	if s.requireClientCert {
		s.TLS.ClientAuth = tls.RequireAndVerifyClientCert
		s.TLS.ClientCAs = s.clientCAs
	}
	s.StartTLS()
	// Cleanups run last-in first-out: release the stalled handlers before Close waits for them.
	t.Cleanup(s.Close)
	t.Cleanup(func() { close(release) })
	return s
}

// authenticate returns the user a request authenticates as, like kube-apiserver's x509 and token authenticators.
func (s *fakeAPIServer) authenticate(r *http.Request) (string, []string, bool) {
	if r.Header.Get("Authorization") == "Bearer "+testToken {
		return "system:serviceaccount:kube-system:robot", []string{"system:serviceaccounts", authenticated}, true
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return "", nil, false
	}
	leaf := r.TLS.PeerCertificates[0]
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: s.clientCAs, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return "", nil, false
	}
	return leaf.Subject.CommonName, append(slices.Clone(leaf.Subject.Organization), authenticated), true
}

func (s *fakeAPIServer) selfSubjectReview(w http.ResponseWriter, r *http.Request) {
	if s.redirectReviewTo != "" {
		http.Redirect(w, r, s.redirectReviewTo+r.URL.Path, http.StatusTemporaryRedirect) //nolint:gosec // the test redirects to another in-test server on purpose
		return
	}
	user, groups, ok := s.authenticate(r)
	switch {
	case !ok:
		writeStatus(w, http.StatusUnauthorized, metav1.StatusReasonUnauthorized)
	case s.reviewStatus != 0:
		writeStatus(w, s.reviewStatus, metav1.StatusReasonInternalError)
	default:
		writeJSON(w, &authenticationv1.SelfSubjectReview{
			TypeMeta: metav1.TypeMeta{Kind: "SelfSubjectReview", APIVersion: "authentication.k8s.io/v1"},
			Status:   authenticationv1.SelfSubjectReviewStatus{UserInfo: authenticationv1.UserInfo{Username: user, Groups: groups}},
		})
	}
}

func (s *fakeAPIServer) version(w http.ResponseWriter, r *http.Request) {
	switch {
	case s.redirectVersion && r.URL.Path == "/version":
		http.Redirect(w, r, "/version-moved", http.StatusFound)
	case s.versionBody != "":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(s.versionBody))
	default:
		writeJSON(w, &version.Info{GitVersion: s.gitVersion, Major: "1", Minor: "36"})
	}
}

func (s *fakeAPIServer) accessReview(w http.ResponseWriter, r *http.Request) {
	user, groups, ok := s.authenticate(r)
	if !ok {
		writeStatus(w, http.StatusUnauthorized, metav1.StatusReasonUnauthorized)
		return
	}
	if s.accessStatus != 0 {
		// The text kube-apiserver writes for a 403 (k8s.io/apiserver responsewriters forbiddenMessage): it names the
		// authenticated user, which for IBM's admin certificate is KubeCert#<caller-iam-id>.
		writeStatusMessage(w, s.accessStatus, metav1.StatusReasonForbidden, fmt.Sprintf(
			"selfsubjectaccessreviews.authorization.k8s.io is forbidden: User %q cannot create resource \"selfsubjectaccessreviews\" in API group \"authorization.k8s.io\" at the cluster scope",
			user))
		return
	}
	// client-go sends built-in types as protobuf; the universal deserializer reads protobuf and JSON alike.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest)
		return
	}
	var review authorizationv1.SelfSubjectAccessReview
	if _, _, err := scheme.Codecs.UniversalDeserializer().Decode(body, nil, &review); err != nil {
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest)
		return
	}
	attrs := review.Spec.ResourceAttributes
	everything := attrs != nil && attrs.Verb == "*" && attrs.Group == "*" && attrs.Resource == "*" && attrs.Namespace == ""
	review.Status.Allowed = everything && slices.Contains(groups, adminGroup)
	review.TypeMeta = metav1.TypeMeta{Kind: "SelfSubjectAccessReview", APIVersion: "authorization.k8s.io/v1"}
	writeJSON(w, &review)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// writeStatus answers a Status whose message is the HTTP status text.
func writeStatus(w http.ResponseWriter, code int32, reason metav1.StatusReason) {
	writeStatusMessage(w, code, reason, http.StatusText(int(code)))
}

// writeStatusMessage answers a Status with the given message.
func writeStatusMessage(w http.ResponseWriter, code int32, reason metav1.StatusReason, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(code))
	status := &metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Reason:   reason,
		Code:     code,
		Message:  message,
	}
	if err := json.NewEncoder(w).Encode(status); err != nil {
		// The status line is already sent; net/http recovers the panic and closes the connection, failing the probe.
		panic(err)
	}
}
