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
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// serverExample is the URL form named in error messages: the form of IBM's masterURL.
const serverExample = "https://cNNN.<region>.containers.cloud.ibm.com:<port>"

// ErrUnsupportedCredential is wrapped by every error of Inspect and ExtractClientCredential that rejects the
// kubeconfig's user entry because it is not an inline client certificate and key (a bearer token, exec, auth-provider,
// basic auth, file paths, impersonation, or missing data). The caller tells it apart with errors.Is: for the ownership
// proof it means "this Secret's credential type cannot be used for the proof" rather than "the Secret is broken".
var ErrUnsupportedCredential = errors.New("managedkubeconfig: the kubeconfig's user is not an inline client certificate and key")

// CATrust names how a kubeconfig client verifies the API server certificate. Its value is what the ca-mode annotation
// of the kubeconfig Secret records.
type CATrust string

const (
	// ClusterCA trusts only the cluster CA embedded as certificate-authority-data. IBM serves IKS masters with a
	// certificate issued by the cluster's own CA, which the system roots do not contain (observed on IBM Cloud).
	ClusterCA CATrust = "ClusterCA"
	// SystemRoots embeds no CA data, so the client trusts the system roots. ROKS "-e" masters present a publicly
	// trusted certificate that IBM's getCACert CA does not validate (observed on IBM Cloud), and CA data would replace
	// the system roots in every Kubernetes client.
	SystemRoots CATrust = "SystemRoots"
)

// Validate returns an error unless t is ClusterCA or SystemRoots. The error names the value and the accepted values.
func (t CATrust) Validate() error {
	switch t {
	case ClusterCA, SystemRoots:
		return nil
	default:
		return fmt.Errorf("managedkubeconfig: CA trust mode %q is not supported; use %q or %q", t, ClusterCA, SystemRoots)
	}
}

// Other returns the other trust mode, or "" when t is not a valid mode. The caller probes with the flavor's mode first
// and falls back to the other one, because the trust of private-only masters is not yet verified against IBM Cloud.
func (t CATrust) Other() CATrust {
	switch t {
	case ClusterCA:
		return SystemRoots
	case SystemRoots:
		return ClusterCA
	default:
		return ""
	}
}

// ClientCredential is a client certificate and its private key, PEM encoded, as IBM's admin kubeconfig carries them in
// client-certificate-data and client-key-data. It is secret material, so every generic way of printing or encoding it
// yields a fixed placeholder:
//
//   - fmt, with every verb (%v, %+v, %#v, %s, %x, %d, ...), also when the credential is a field of another value such as
//     Input: Format;
//   - logr loggers that are handed the credential itself: MarshalLog;
//   - encoding/json, which the component-base JSON log format that CAPIBM registers (cmd/main.go) uses for nested values
//     such as an Input passed to a logger: MarshalJSON.
//
// Printers that read struct fields through reflection without these methods (for example spew) are not covered; never
// pass a credential to them.
type ClientCredential struct {
	// CertificatePEM is the client certificate, optionally followed by intermediates; the first one is the leaf.
	CertificatePEM []byte
	// KeyPEM is the private key of the leaf certificate.
	KeyPEM []byte
}

// redactionPlaceholder is what every printed or encoded form of a ClientCredential shows.
const redactionPlaceholder = "ClientCredential{<redacted>}"

// String returns a fixed placeholder instead of the certificate and key.
func (ClientCredential) String() string {
	return redactionPlaceholder
}

// Format implements fmt.Formatter and writes the placeholder for every verb. fmt consults Stringer only for %v, %s, %q,
// %x and %X; without Format, %#v and %d would print the key bytes.
//
// Parameters: f is the fmt state written to; the verb is ignored on purpose.
func (ClientCredential) Format(f fmt.State, _ rune) {
	_, _ = fmt.Fprint(f, redactionPlaceholder)
}

// MarshalLog implements logr.Marshaler with the same placeholder, for a logger that is handed the credential itself.
func (ClientCredential) MarshalLog() any {
	return redactionPlaceholder
}

// MarshalJSON implements json.Marshaler and encodes the placeholder as a JSON string. Without it encoding/json would
// write both fields as base64, which is what the JSON log format does with a credential nested in another value.
func (ClientCredential) MarshalJSON() ([]byte, error) {
	return json.Marshal(redactionPlaceholder)
}

// Input holds the parts Build assembles into a kubeconfig.
type Input struct {
	// ClusterName is the CAPI Cluster name. It names the kubeconfig's cluster, its user ("<name>-admin") and its context
	// ("<name>-admin@<name>") the way CAPI's kubeconfig.New does, so tools that read CAPI kubeconfigs see familiar
	// names. Required.
	ClusterName string
	// Server is the API server URL, IBM's masterURL as selected for the cluster: an absolute https URL with a host.
	// Required.
	Server string
	// Trust selects how a client verifies the server certificate. ClusterCA embeds CertificateAuthorityPEM; SystemRoots
	// embeds no CA data. Required.
	Trust CATrust
	// CertificateAuthorityPEM is the cluster CA bundle (IKS: from getKubeconfig, cross-checked with getCACert).
	// Required with ClusterCA; must be empty with SystemRoots, because CA data would replace the system roots.
	CertificateAuthorityPEM []byte
	// Credential is the admin client certificate and key. Required.
	Credential ClientCredential
}

// Build validates in and returns a kubeconfig with exactly one cluster, one user and one context, all inline: the
// server, the CA data when in.Trust is ClusterCA, and the client certificate and key. It never writes exec,
// auth-provider, token, file paths or skipped TLS verification. The output is deterministic for equal input, so the
// caller can skip a Secret write when the bytes did not change.
//
// Parameters: in holds every part; Build returns an error naming the first invalid field and the accepted form.
func Build(in Input) ([]byte, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	userName := in.ClusterName + "-admin"
	contextName := userName + "@" + in.ClusterName
	cluster := &clientcmdapi.Cluster{Server: in.Server}
	if in.Trust == ClusterCA {
		cluster.CertificateAuthorityData = in.CertificateAuthorityPEM
	}
	cfg := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{in.ClusterName: cluster},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{userName: {
			ClientCertificateData: in.Credential.CertificatePEM,
			ClientKeyData:         in.Credential.KeyPEM,
		}},
		Contexts: map[string]*clientcmdapi.Context{contextName: {
			Cluster:  in.ClusterName,
			AuthInfo: userName,
		}},
		CurrentContext: contextName,
	}
	out, err := clientcmd.Write(cfg)
	if err != nil {
		return nil, fmt.Errorf("managedkubeconfig: serialising the kubeconfig of cluster %q: %w", in.ClusterName, err)
	}
	return out, nil
}

// validate checks every field of the Input and names the first invalid one.
func (in Input) validate() error {
	if in.ClusterName == "" {
		return errors.New("managedkubeconfig: Input.ClusterName is empty; pass the CAPI Cluster name")
	}
	if err := validateServer(in.Server); err != nil {
		return err
	}
	if err := in.Trust.Validate(); err != nil {
		return err
	}
	switch in.Trust {
	case ClusterCA:
		if len(in.CertificateAuthorityPEM) == 0 {
			return fmt.Errorf("managedkubeconfig: CA trust mode %q needs Input.CertificateAuthorityPEM (the cluster CA)", ClusterCA)
		}
		if _, err := parseCertificates(in.CertificateAuthorityPEM, "certificate authority"); err != nil {
			return err
		}
	case SystemRoots:
		// CA data replaces the system pool in client-go, so a ROKS kubeconfig carrying getCACert's CA would fail.
		if len(in.CertificateAuthorityPEM) != 0 {
			return fmt.Errorf("managedkubeconfig: CA trust mode %q embeds no CA data; leave Input.CertificateAuthorityPEM empty", SystemRoots)
		}
	}
	_, err := parseCredential(in.Credential)
	return err
}

// Info describes a kubeconfig Build wrote, without secret material and without the client certificate's subject.
// The subject of IBM's admin certificate names the IAM identity that fetched it; leaving it out keeps every decision
// built on Info identity-agnostic.
type Info struct {
	// Server is the cluster entry's server URL.
	Server string
	// Trust is ClusterCA when the cluster entry embeds CA data and SystemRoots otherwise.
	Trust CATrust
	// CA describes the embedded CA bundle; nil with SystemRoots.
	CA *CertificateInfo
	// Certificate describes the client (leaf) certificate.
	Certificate CertificateInfo
}

// CertificateInfo is the non-secret description of a certificate or a bundle, as the kubeconfig Secret's
// credential-* and ca-* annotations record it.
type CertificateInfo struct {
	// Fingerprint is "sha256:" followed by the lowercase hex SHA-256 of the certificates' DER bytes, concatenated in
	// order. For one certificate the hex part equals the Kubernetes audit credential ID "X509SHA256=<hex>", so an
	// operator can match the annotation with audit logs.
	Fingerprint string
	// NotBefore is the latest NotBefore in the bundle (the start of the window in which every certificate is valid).
	NotBefore time.Time
	// NotAfter is the earliest NotAfter in the bundle (the end of that window).
	NotAfter time.Time
}

// InspectPEM describes a PEM certificate or bundle, for example the CA that getCACert returns, so the caller can compare
// it with the CA stored in a kubeconfig. It returns an error when data holds no certificate, a block that is not a
// certificate, or trailing data.
//
// Parameters: data is the PEM text.
func InspectPEM(data []byte) (CertificateInfo, error) {
	certs, err := parseCertificates(data, "certificate")
	if err != nil {
		return CertificateInfo{}, err
	}
	return describe(certs), nil
}

// Inspect parses a kubeconfig that Build wrote and describes it. It accepts exactly that shape: one cluster, one user and
// one context; an https server; inline CA data or none; an inline client certificate and key that form a valid pair.
// Anything else (a token, exec, auth-provider, basic auth, impersonation, file paths, skipped TLS verification, a
// proxy or TLS server name override) is rejected, so a tampered CAPIBM Secret is treated as invalid and replaced
// rather than trusted.
//
// Parameters: kubeconfig is the Secret's value key.
func Inspect(kubeconfig []byte) (Info, error) {
	cfg, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return Info{}, fmt.Errorf("managedkubeconfig: the kubeconfig does not parse: %w", err)
	}
	if len(cfg.Clusters) != 1 || len(cfg.AuthInfos) != 1 || len(cfg.Contexts) != 1 {
		return Info{}, fmt.Errorf("managedkubeconfig: want exactly one cluster, one user and one context (the shape Build writes), got %d, %d and %d",
			len(cfg.Clusters), len(cfg.AuthInfos), len(cfg.Contexts))
	}
	cluster, user, userName, err := currentEntries(cfg)
	if err != nil {
		return Info{}, err
	}
	if err := checkCluster(cluster); err != nil {
		return Info{}, err
	}
	if err := checkUser(user, userName); err != nil {
		return Info{}, err
	}
	leaf, err := parseCredential(ClientCredential{CertificatePEM: user.ClientCertificateData, KeyPEM: user.ClientKeyData})
	if err != nil {
		return Info{}, err
	}

	info := Info{Server: cluster.Server, Trust: SystemRoots, Certificate: describe([]*x509.Certificate{leaf})}
	if len(cluster.CertificateAuthorityData) > 0 {
		caCerts, err := parseCertificates(cluster.CertificateAuthorityData, "certificate authority")
		if err != nil {
			return Info{}, err
		}
		ca := describe(caCerts)
		info.Trust = ClusterCA
		info.CA = &ca
	}
	return info, nil
}

// ExtractClientCredential returns the client certificate and key of the current context's user. The ownership proof
// (DecideBinding rows 2 and 5) rebuilds a kubeconfig from this credential with the candidate cluster's own masterURL
// and trust, and never trusts the stored server or CA. Unlike Inspect it ignores other clusters, users and contexts,
// because the Secret may be user-provided; the user entry itself must still carry only inline client-certificate-data
// and client-key-data that form a valid pair.
//
// Limitation: Build writes only a client certificate, so a user-provided Secret whose user authenticates in any other
// way, for example with a bearer token, cannot be rebuilt for the candidate's masterURL, and its binding cannot be
// proven after a move even though the prober accepts such a Secret for steady probing. The error then wraps
// ErrUnsupportedCredential, so the caller can report that the ownership proof needs a client-certificate credential
// instead of a generic invalid-kubeconfig message.
//
// Parameters: kubeconfig is the Secret's value key.
func ExtractClientCredential(kubeconfig []byte) (ClientCredential, error) {
	cfg, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return ClientCredential{}, fmt.Errorf("managedkubeconfig: the kubeconfig does not parse: %w", err)
	}
	_, user, userName, err := currentEntries(cfg)
	if err != nil {
		return ClientCredential{}, err
	}
	if err := checkUser(user, userName); err != nil {
		return ClientCredential{}, err
	}
	cred := ClientCredential{CertificatePEM: user.ClientCertificateData, KeyPEM: user.ClientKeyData}
	if _, err := parseCredential(cred); err != nil {
		return ClientCredential{}, err
	}
	return cred, nil
}

// currentEntries resolves the current context to its cluster and user entries.
func currentEntries(cfg *clientcmdapi.Config) (*clientcmdapi.Cluster, *clientcmdapi.AuthInfo, string, error) {
	if cfg.CurrentContext == "" {
		return nil, nil, "", errors.New("managedkubeconfig: the kubeconfig has no current-context")
	}
	kctx, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok || kctx == nil {
		return nil, nil, "", fmt.Errorf("managedkubeconfig: current-context %q is not defined", cfg.CurrentContext)
	}
	cluster, ok := cfg.Clusters[kctx.Cluster]
	if !ok || cluster == nil {
		return nil, nil, "", fmt.Errorf("managedkubeconfig: context %q names cluster %q, which is not defined", cfg.CurrentContext, kctx.Cluster)
	}
	user, ok := cfg.AuthInfos[kctx.AuthInfo]
	if !ok || user == nil {
		return nil, nil, "", fmt.Errorf("managedkubeconfig: context %q names user %q, which is not defined", cfg.CurrentContext, kctx.AuthInfo)
	}
	return cluster, user, kctx.AuthInfo, nil
}

// checkCluster accepts only the cluster fields Build writes: server and optional inline CA data.
func checkCluster(c *clientcmdapi.Cluster) error {
	if err := validateServer(c.Server); err != nil {
		return err
	}
	switch {
	case c.InsecureSkipTLSVerify:
		return errors.New("managedkubeconfig: the cluster entry disables TLS verification; CAPIBM kubeconfigs always verify the server")
	case c.CertificateAuthority != "":
		return errors.New("managedkubeconfig: the cluster entry references a CA file; CAPIBM kubeconfigs carry inline CA data or none")
	case c.ProxyURL != "", c.TLSServerName != "":
		return errors.New("managedkubeconfig: the cluster entry sets proxy-url or tls-server-name, which CAPIBM never writes")
	}
	return nil
}

// checkUser accepts only inline client-certificate-data and client-key-data. A token, exec, auth-provider or basic auth
// entry is a different credential type than IBM's admin certificate, the only credential CAPIBM uses; a file path
// would make the controller read its own file system; impersonation would change who the credential acts as. Every
// rejection wraps ErrUnsupportedCredential.
func checkUser(u *clientcmdapi.AuthInfo, name string) error {
	reject := func(what string) error {
		return fmt.Errorf("%w: user %q %s; CAPIBM kubeconfigs carry only inline client-certificate-data and client-key-data",
			ErrUnsupportedCredential, name, what)
	}
	switch {
	case u.Token != "" || u.TokenFile != "":
		return reject("uses a bearer token")
	case u.Exec != nil:
		return reject("uses an exec plugin")
	case u.AuthProvider != nil:
		return reject("uses an auth-provider")
	case u.Username != "" || u.Password != "":
		return reject("uses basic authentication")
	case u.ClientCertificate != "" || u.ClientKey != "":
		return reject("references certificate or key files")
	case u.Impersonate != "" || u.ImpersonateUID != "" || len(u.ImpersonateGroups) > 0 || len(u.ImpersonateUserExtra) > 0:
		return reject("impersonates another user")
	case len(u.ClientCertificateData) == 0 || len(u.ClientKeyData) == 0:
		return reject("lacks client-certificate-data or client-key-data")
	}
	return nil
}

// validateServer accepts an absolute https URL with a host and without user info.
func validateServer(server string) error {
	if server == "" {
		return fmt.Errorf("managedkubeconfig: the server URL is empty; want an https URL such as %s", serverExample)
	}
	u, err := url.Parse(server)
	if err != nil {
		return fmt.Errorf("managedkubeconfig: server %q does not parse (%w); want an https URL such as %s", server, err, serverExample)
	}
	if u.Scheme != "https" || u.Host == "" || u.User != nil {
		return fmt.Errorf("managedkubeconfig: server %q is not an https URL with a host and without user info; want for example %s", server, serverExample)
	}
	return nil
}

// parseCredential checks that the certificate and key are present and form a valid pair, and returns the leaf.
func parseCredential(c ClientCredential) (*x509.Certificate, error) {
	if len(c.CertificatePEM) == 0 || len(c.KeyPEM) == 0 {
		return nil, errors.New("managedkubeconfig: the client certificate or key is empty; IBM's admin kubeconfig carries both")
	}
	certs, err := parseCertificates(c.CertificatePEM, "client certificate")
	if err != nil {
		return nil, err
	}
	// tls.X509KeyPair checks that the key parses and matches the leaf's public key; its errors carry no key material.
	if _, err := tls.X509KeyPair(c.CertificatePEM, c.KeyPEM); err != nil {
		return nil, fmt.Errorf("managedkubeconfig: the client certificate and key do not form a valid pair: %w", err)
	}
	return certs[0], nil
}

// parseCertificates decodes every PEM block of data as an X.509 certificate. what names the data in errors.
func parseCertificates(data []byte, what string) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("managedkubeconfig: the %s holds a PEM block of type %q; want CERTIFICATE", what, block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("managedkubeconfig: the %s does not parse: %w", what, err)
		}
		if !cert.NotAfter.After(cert.NotBefore) {
			return nil, fmt.Errorf("managedkubeconfig: the %s has an empty validity period (notBefore %s, notAfter %s)",
				what, cert.NotBefore.UTC().Format(time.RFC3339), cert.NotAfter.UTC().Format(time.RFC3339))
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("managedkubeconfig: the %s holds no PEM certificate", what)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("managedkubeconfig: the %s has trailing data after its last PEM certificate", what)
	}
	return certs, nil
}

// describe returns the fingerprint and common validity window of certs (at least one).
func describe(certs []*x509.Certificate) CertificateInfo {
	h := sha256.New()
	info := CertificateInfo{NotBefore: certs[0].NotBefore, NotAfter: certs[0].NotAfter}
	for _, c := range certs {
		h.Write(c.Raw)
		if c.NotBefore.After(info.NotBefore) {
			info.NotBefore = c.NotBefore
		}
		if c.NotAfter.Before(info.NotAfter) {
			info.NotAfter = c.NotAfter
		}
	}
	info.Fingerprint = "sha256:" + hex.EncodeToString(h.Sum(nil))
	return info
}
