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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	authenticationv1client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	authorizationv1client "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// DefaultTimeout bounds one probe, all three requests together: long enough for three small requests to an IBM master
// in another region, short enough that a hanging master does not stall a control-plane reconcile.
const DefaultTimeout = 10 * time.Second

// maxRedirects matches net/http's default limit for the redirects the prober still follows (same host only).
const maxRedirects = 10

// Result is what a successful probe learned about the credential and the cluster.
type Result struct {
	// Username is the user the API server authenticated (SelfSubjectReview). For IBM's admin certificate it names the
	// IAM identity that fetched the certificate (KubeCert#<caller-iam-id>), so callers use it only for decisions, never in
	// status, events, annotations or logs above debug level.
	Username string
	// Groups are the authenticated user's groups (SelfSubjectReview), for example ibm-admins and
	// system:authenticated for IBM's admin certificate.
	Groups []string
	// GitVersion is the API server's /version gitVersion, for example v1.36.5+IKS; CAPIBM publishes it as the control
	// plane version.
	GitVersion string
	// ClusterAdmin is the answer of a SelfSubjectAccessReview for verb "*" on resource "*" in group "*" in all
	// namespaces. The ownership proof relies on it (ProvesOwnership), not on the user name.
	ClusterAdmin bool
}

// Prober verifies a kubeconfig without CAPIBM's ClusterCache. The managed scopes receive it through ScopeParams, so
// controller tests pass a fake.
type Prober interface {
	// Probe uses kubeconfig to run a SelfSubjectReview, GET /version and a SelfSubjectAccessReview for "*/*/*"
	// against the kubeconfig's server. Every error is a *ProbeError; ReasonOf classifies it.
	Probe(ctx context.Context, kubeconfig []byte) (Result, error)
}

// RESTProberOptions configures NewRESTProber.
type RESTProberOptions struct {
	// Timeout bounds one whole probe and each of its requests. Required, > 0; cmd/main.go passes DefaultTimeout.
	Timeout time.Duration
	// SystemRootsPEM, when set, replaces the system root pool for kubeconfigs that embed no CA data. It exists for
	// tests, which cannot add a CA to the host's trust store; production leaves it nil and the system roots apply.
	// Optional; when set it must hold at least one PEM certificate.
	SystemRootsPEM []byte
}

// RESTProber is the production Prober. It holds only its options, so one instance serves every reconcile.
type RESTProber struct {
	timeout        time.Duration
	systemRootsPEM []byte
}

var _ Prober = &RESTProber{}

// NewRESTProber validates opts and returns a RESTProber. It makes no request.
//
// Parameters: opts holds the timeout and, for tests, a replacement for the system roots.
func NewRESTProber(opts RESTProberOptions) (*RESTProber, error) {
	if opts.Timeout <= 0 {
		return nil, fmt.Errorf("workloadprobe: RESTProberOptions.Timeout is %s; want a positive duration such as %s", opts.Timeout, DefaultTimeout)
	}
	if len(opts.SystemRootsPEM) > 0 && !x509.NewCertPool().AppendCertsFromPEM(opts.SystemRootsPEM) {
		return nil, errors.New("workloadprobe: RESTProberOptions.SystemRootsPEM holds no PEM certificate; leave it empty to use the system roots")
	}
	return &RESTProber{timeout: opts.Timeout, systemRootsPEM: append([]byte(nil), opts.SystemRootsPEM...)}, nil
}

// Probe implements Prober. It builds a fresh client from kubeconfig, so it neither uses nor disturbs CAPIBM's
// ClusterCache, and runs, within the prober's timeout: a SelfSubjectReview (does the server accept the credential, and
// as whom), GET /version (the version CAPIBM reports), and a SelfSubjectAccessReview for "*/*/*" (is it
// cluster-admin). The order puts the authentication check first, so a 401 is reported as such.
//
// Parameters: ctx bounds the probe in addition to the timeout; kubeconfig is the kubeconfig to verify, CAPIBM-built or
// user-provided.
func (p *RESTProber) Probe(ctx context.Context, kubeconfig []byte) (Result, error) {
	cfg, err := p.restConfig(kubeconfig)
	if err != nil {
		return Result{}, &ProbeError{Reason: FailureInvalidKubeconfig, Step: stepKubeconfig, Err: err}
	}
	httpClient, err := p.httpClient(cfg)
	if err != nil {
		return Result{}, &ProbeError{Reason: FailureInvalidKubeconfig, Step: stepKubeconfig, Err: err}
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	authn, err := authenticationv1client.NewForConfigAndClient(cfg, httpClient)
	if err != nil {
		return Result{}, &ProbeError{Reason: FailureInvalidKubeconfig, Step: stepSelfSubjectReview, Err: err}
	}
	review, err := authn.SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return Result{}, classify(stepSelfSubjectReview, err)
	}
	result := Result{
		Username: review.Status.UserInfo.Username,
		Groups:   append([]string(nil), review.Status.UserInfo.Groups...),
	}

	if result.GitVersion, err = serverGitVersion(ctx, cfg, httpClient); err != nil {
		return Result{}, err
	}

	authz, err := authorizationv1client.NewForConfigAndClient(cfg, httpClient)
	if err != nil {
		return Result{}, &ProbeError{Reason: FailureInvalidKubeconfig, Step: stepAccessReview, Err: err}
	}
	access, err := authz.SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			// Namespace "" means all namespaces: only cluster-admin is allowed everything everywhere.
			ResourceAttributes: &authorizationv1.ResourceAttributes{Verb: "*", Group: "*", Resource: "*"},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return Result{}, classify(stepAccessReview, err)
	}
	result.ClusterAdmin = access.Status.Allowed && !access.Status.Denied
	return result, nil
}

// serverGitVersion reads /version the way discovery.ServerVersion does, but with the probe's context: ServerVersion
// takes none, so only the HTTP timeout would bound it.
func serverGitVersion(ctx context.Context, cfg *rest.Config, httpClient *http.Client) (string, error) {
	dc, err := discovery.NewDiscoveryClientForConfigAndClient(cfg, httpClient)
	if err != nil {
		return "", &ProbeError{Reason: FailureInvalidKubeconfig, Step: stepVersion, Err: err}
	}
	body, err := dc.RESTClient().Get().AbsPath("/version").Do(ctx).Raw()
	if err != nil {
		return "", classify(stepVersion, err)
	}
	var info version.Info
	if err := json.Unmarshal(body, &info); err != nil {
		return "", &ProbeError{Reason: FailureOther, Step: stepVersion, Err: fmt.Errorf("the response is not a version document: %w", err)}
	}
	return info.GitVersion, nil
}

// restConfig validates kubeconfig and turns it into a REST config the way CAPI's ClusterCache does
// (clientcmd.RESTConfigFromKubeConfig), applying the prober's timeout and, for kubeconfigs without CA data, the
// injected system roots.
func (p *RESTProber) restConfig(kubeconfig []byte) (*rest.Config, error) {
	raw, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("the kubeconfig does not parse: %w", err)
	}
	if err := checkKubeconfig(raw); err != nil {
		return nil, err
	}
	cfg, err := clientcmd.NewDefaultClientConfig(*raw, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("the kubeconfig is not usable: %w", err)
	}
	// Without CA data client-go trusts the system roots (a ROKS master serves a publicly trusted certificate); tests
	// substitute their own "public" CA here.
	if len(cfg.CAData) == 0 && len(p.systemRootsPEM) > 0 {
		cfg.CAData = p.systemRootsPEM
	}
	cfg.Timeout = p.timeout
	return cfg, nil
}

// httpClient returns a new http.Client over client-go's transport for cfg (TLS, credentials, proxy as ClusterCache
// would use them) that follows redirects only within the same scheme and host. A fresh client is built instead of
// rest.HTTPClientFor's, which may return the shared http.DefaultClient whose redirect policy must not be changed.
func (p *RESTProber) httpClient(cfg *rest.Config) (*http.Client, error) {
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("building the HTTP transport: %w", err)
	}
	return &http.Client{Transport: transport, Timeout: p.timeout, CheckRedirect: sameHostRedirectsOnly}, nil
}

// sameHostRedirectsOnly refuses a redirect to another scheme, host or port: the client certificate or token would be
// presented to a server the kubeconfig does not name.
func sameHostRedirectsOnly(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	first := via[0].URL
	if req.URL.Scheme != first.Scheme || req.URL.Host != first.Host {
		return fmt.Errorf("refusing a redirect from %s://%s to %s://%s: the probe talks only to the kubeconfig's server",
			first.Scheme, first.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

// checkKubeconfig rejects what the prober must not use, for the current context only (the one client-go uses). exec and
// auth-provider would run code or plugins and ClusterCache rejects them anyway; file paths would make the controller
// read its own file system and send what it finds; a non-https server or skipped TLS verification would remove the TLS
// check this package exists to make.
func checkKubeconfig(cfg *clientcmdapi.Config) error {
	kctx, ok := cfg.Contexts[cfg.CurrentContext]
	if cfg.CurrentContext == "" || !ok || kctx == nil {
		return fmt.Errorf("the kubeconfig's current-context %q is not defined", cfg.CurrentContext)
	}
	cluster, ok := cfg.Clusters[kctx.Cluster]
	if !ok || cluster == nil {
		return fmt.Errorf("context %q names cluster %q, which is not defined", cfg.CurrentContext, kctx.Cluster)
	}
	user, ok := cfg.AuthInfos[kctx.AuthInfo]
	if !ok || user == nil {
		return fmt.Errorf("context %q names user %q, which is not defined", cfg.CurrentContext, kctx.AuthInfo)
	}
	server, err := url.Parse(cluster.Server)
	if err != nil || server.Scheme != "https" || server.Host == "" {
		return fmt.Errorf("the cluster entry's server %q is not an https URL with a host", cluster.Server)
	}
	switch {
	case cluster.InsecureSkipTLSVerify:
		return errors.New("the cluster entry disables TLS verification")
	case cluster.CertificateAuthority != "":
		return errors.New("the cluster entry references a CA file; use certificate-authority-data")
	case user.Exec != nil:
		return errors.New("the user entry uses an exec plugin, which ClusterCache does not support")
	case user.AuthProvider != nil:
		return errors.New("the user entry uses an auth-provider, which ClusterCache does not support")
	case user.ClientCertificate != "" || user.ClientKey != "" || user.TokenFile != "":
		return errors.New("the user entry references certificate, key or token files; use inline data")
	}
	return nil
}
