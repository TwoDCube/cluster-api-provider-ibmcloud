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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/IBM-Cloud/container-services-go-sdk/kubernetesserviceapiv1"
	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"

	"k8s.io/utils/clock"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/endpoints"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"
)

// This file builds every IBM Cloud client of the managed controllers. Each client is built for one resolved
// credentials.Identity, so that a later per-Cluster identity is a new value, not a new parameter. ProdClientBuilder
// caches one authenticator per identity key and installs a small delegating authenticator in every client it returns,
// so a rebuilt authenticator (after an hour, a new Version or an IBM 401) reaches clients that already exist. Every IKS
// client waits on the one process-wide limiter, because IBM limits the request rate per source IP, not per identity.

// ClientBuilder builds IBM Cloud clients for the managed controllers, always for one resolved identity. Adding a
// second identity later adds a value of credentials.Identity, never a parameter. Tests inject a stub that returns
// gomock mocks.
type ClientBuilder interface {
	// KubernetesService returns the IKS client of identity id for region (IBMCloudManagedCluster.spec.region): the
	// --service-endpoint entry "<region>:containers=..." when present, else the global URL. Clients are cached per
	// (identity, service, URL) and share the one process-wide limiter.
	KubernetesService(ctx context.Context, id credentials.Identity, region string) (kubernetesservice.KubernetesService, error)
	// VPC returns the VPC client of identity id for region (vpc.NewServiceWithAuthenticator).
	VPC(ctx context.Context, id credentials.Identity, region string) (vpc.Vpc, error)
	// ResourceManager returns the Resource Manager client of identity id (only GetResourceGroup by ID is used).
	ResourceManager(ctx context.Context, id credentials.Identity) (resourcemanager.ResourceManager, error)
	// InvalidateAuthenticator drops the cached authenticator of one identity after an IBM 401 for that identity. The
	// next request of any of its clients builds a new one; other identities are untouched.
	InvalidateAuthenticator(key credentials.IdentityKey)
}

const (
	// authenticatorMaxAge is the age at which an authenticator is rebuilt: the controller identity's key lives in a
	// mounted Secret file, and rebuilding re-reads it, so a rotated key is picked up without a restart.
	authenticatorMaxAge = time.Hour
	// authenticatorIdleTime evicts identities that no Cluster used recently, together with their clients, so that the
	// caches stay bounded once a later release has many identities.
	authenticatorIdleTime = 2 * time.Hour
)

// ProdClientBuilderOptions configures NewProdClientBuilder. Every field except ServiceEndpoints is required;
// NewProdClientBuilder names each missing or invalid one.
type ProdClientBuilderOptions struct {
	// ServiceEndpoints is the --service-endpoint flag, parsed once in cmd/main.go (may be empty). The builder uses
	// the "containers" entry of a cluster's region, the "vpc" entry of a cluster's region and the "rm" entry; each of
	// them must be an https URL, because every request carries an IAM bearer token.
	ServiceEndpoints []endpoints.ServiceEndpoint
	// Limiter is the one process-wide limiter of IKS requests (--containers-api-qps, --containers-api-burst).
	Limiter kubernetesservice.RequestLimiter
	// HTTPTimeout bounds each IKS request, including the limiter wait; > 0.
	//
	// Known gap: VPC and Resource Manager requests have no such bound yet. vpc.NewServiceWithAuthenticator and
	// resourcemanager.NewService accept no HTTP client or timeout, go-sdk-core's default client has neither an overall
	// nor a response-header timeout, and the methods of both clients take no context. One response that never arrives
	// therefore blocks the IBMCloudManagedCluster controller, which runs a single worker, for every
	// IBMCloudManagedCluster of the manager. Closing the gap needs a timeout on both constructors, set from HTTPTimeout
	// here.
	HTTPTimeout time.Duration
	// UserAgent is sent on every IKS request, for example "cluster-api-provider-ibmcloud/<version>"; one line.
	UserAgent string
	// Clock ages authenticators; a fake clock in tests.
	Clock clock.PassiveClock
	// NewControllerAuthenticator builds the controller identity's authenticator. cmd/main.go passes
	// authenticator.GetAuthenticator, the one environment read, outside the lint scope of the managed packages; tests
	// pass a counting fake. The builder calls it on first use and then for every rebuild: after authenticatorMaxAge,
	// and after an IBM 401 (InvalidateAuthenticator).
	NewControllerAuthenticator func() (core.Authenticator, error)
}

// ProdClientBuilder is the production ClientBuilder and the only caller of the controller-identity factory; nothing in
// this package reads the environment (the forbidigo lint rule enforces it). Create it with NewProdClientBuilder. It is
// safe for concurrent use by the managed controllers.
type ProdClientBuilder struct {
	opts ProdClientBuilderOptions
	// resourceManagerURL is computed once from opts.ServiceEndpoints: Resource Manager is a global service. It is a
	// field rather than a function result so that the seam tests can point it at a plain-HTTP httptest server.
	resourceManagerURL string

	// mu guards auths and clients. It is never held while a client constructor runs, because constructors validate
	// the client's identityAuthenticator, which takes mu itself.
	mu      sync.Mutex
	auths   map[credentials.IdentityKey]*authEntry
	clients map[clientKey]any // kubernetesservice.KubernetesService, vpc.Vpc or resourcemanager.ResourceManager
}

var _ ClientBuilder = &ProdClientBuilder{}

// authEntry is the cached authenticator of one identity.
type authEntry struct {
	auth core.Authenticator
	// source and version are what auth was built from; identityAuthenticator rebuilds from them, because it knows only
	// the key. source is nil for the controller identity.
	source  credentials.CredentialSource
	version string
	builtAt time.Time
	usedAt  time.Time
	// invalidated is set by InvalidateAuthenticator: the next use rebuilds even when auth is younger than an hour.
	invalidated bool
}

// clientKey keys every cached client by identity, service and URL: the same URL used by two identities gives two
// clients, so a client never authenticates as another identity.
type clientKey struct {
	identity credentials.IdentityKey
	service  string // endpoints service ID: containers, vpc or rm
	url      string
}

// NewProdClientBuilder validates opts and returns the production ClientBuilder. It makes no IBM call and builds no
// authenticator: the first client of an identity does.
//
// Parameters:
//   - opts are the endpoints, the limiter, the IKS timeout and User-Agent, the clock and the controller-identity
//     factory; see ProdClientBuilderOptions.
func NewProdClientBuilder(opts ProdClientBuilderOptions) (*ProdClientBuilder, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	rmURL := endpoints.FetchEndpoints(string(endpoints.RM), opts.ServiceEndpoints)
	if rmURL == "" {
		rmURL = resourcemanagerv2.DefaultServiceURL
	}
	return &ProdClientBuilder{
		opts:               opts,
		resourceManagerURL: rmURL,
		auths:              map[credentials.IdentityKey]*authEntry{},
		clients:            map[clientKey]any{},
	}, nil
}

// validate returns one error that names every missing or invalid option and the accepted value.
func (o ProdClientBuilderOptions) validate() error {
	var errs []error
	if core.IsNil(o.Limiter) {
		errs = append(errs, errors.New("managed client builder: Limiter is nil; pass the process-wide limiter built from --containers-api-qps and --containers-api-burst"))
	}
	if o.HTTPTimeout <= 0 {
		errs = append(errs, fmt.Errorf("managed client builder: HTTPTimeout is %s; pass a positive duration, for example 30s", o.HTTPTimeout))
	}
	if o.UserAgent == "" || strings.ContainsAny(o.UserAgent, "\r\n") {
		errs = append(errs, fmt.Errorf("managed client builder: UserAgent %q is not a single-line product token; pass for example cluster-api-provider-ibmcloud/<version>", o.UserAgent))
	}
	if core.IsNil(o.Clock) {
		errs = append(errs, errors.New("managed client builder: Clock is nil; pass clock.RealClock{} (a fake clock in tests)"))
	}
	if o.NewControllerAuthenticator == nil {
		errs = append(errs, errors.New("managed client builder: NewControllerAuthenticator is nil; cmd/main.go passes authenticator.GetAuthenticator"))
	}
	for _, endpoint := range o.ServiceEndpoints {
		if err := validateBuilderEndpoint(endpoint); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// httpsScheme is the only URL scheme this package accepts for an IBM service endpoint, a master URL or a control plane
// endpoint: every request to them carries an IAM bearer token or a client certificate. Later files of the package
// compare against this constant instead of repeating the literal (goconst).
const httpsScheme = "https"

// validateBuilderEndpoint rejects a --service-endpoint entry of a service this builder uses (containers, vpc, rm)
// unless it is an absolute https URL. Entries of other services are not this builder's concern. endpoint is one parsed
// entry of the flag.
func validateBuilderEndpoint(endpoint endpoints.ServiceEndpoint) error {
	switch endpoint.ID {
	case string(endpoints.Containers), string(endpoints.VPC), string(endpoints.RM):
	default:
		return nil
	}
	parsed, err := url.Parse(endpoint.URL)
	if err != nil || parsed.Scheme != httpsScheme || parsed.Host == "" || parsed.User != nil {
		// Only the redacted form is printed: a URL typed with user information must not reach a log.
		shown := endpoint.URL
		if err == nil {
			shown = parsed.Redacted()
		}
		return fmt.Errorf("managed client builder: --service-endpoint entry %s:%s=%s is not an absolute https URL without user information; "+
			"every request carries an IAM bearer token, so pass for example %s:%s=https://<host>/<path>",
			endpoint.Region, endpoint.ID, shown, endpoint.Region, endpoint.ID)
	}
	return nil
}

// KubernetesService implements ClientBuilder.
//
// Parameters:
//   - ctx is unused today; a later CredentialSource may need it.
//   - id is the identity the client acts as; it must pass Identity.Validate.
//   - region is the IBM Cloud region of the cluster, for example us-south; it selects a regional --service-endpoint
//     entry.
func (b *ProdClientBuilder) KubernetesService(_ context.Context, id credentials.Identity, region string) (kubernetesservice.KubernetesService, error) {
	if err := validateClientRequest(id, region, true); err != nil {
		return nil, err
	}
	serviceURL := b.containersURL(region)
	key := clientKey{identity: id.Key, service: string(endpoints.Containers), url: serviceURL}
	built, err := b.cachedClient(id, key, func(auth core.Authenticator) (any, error) {
		return kubernetesservice.NewService(kubernetesservice.ServiceOptions{
			URL:           serviceURL,
			Authenticator: auth,
			RateLimiter:   b.limiterFor(id.Key),
			HTTPTimeout:   b.opts.HTTPTimeout,
			UserAgent:     b.opts.UserAgent,
		})
	})
	if err != nil {
		return nil, err
	}
	client, ok := built.(kubernetesservice.KubernetesService)
	if !ok {
		return nil, fmt.Errorf("managed client builder: the cached client %s is a %T, not an IKS client (programming error)", key.service, built)
	}
	return client, nil
}

// VPC implements ClientBuilder.
//
// Parameters:
//   - ctx is unused today; a later CredentialSource may need it.
//   - id is the identity the client acts as; it must pass Identity.Validate.
//   - region is the IBM Cloud region of the VPC, for example us-south; the VPC API is regional.
func (b *ProdClientBuilder) VPC(_ context.Context, id credentials.Identity, region string) (vpc.Vpc, error) {
	if err := validateClientRequest(id, region, true); err != nil {
		return nil, err
	}
	serviceURL := endpoints.FetchVPCEndpoint(region, b.opts.ServiceEndpoints)
	key := clientKey{identity: id.Key, service: string(endpoints.VPC), url: serviceURL}
	built, err := b.cachedClient(id, key, func(auth core.Authenticator) (any, error) {
		return vpc.NewServiceWithAuthenticator(serviceURL, auth)
	})
	if err != nil {
		return nil, err
	}
	client, ok := built.(vpc.Vpc)
	if !ok {
		return nil, fmt.Errorf("managed client builder: the cached client %s is a %T, not a VPC client (programming error)", key.service, built)
	}
	return client, nil
}

// ResourceManager implements ClientBuilder.
//
// Parameters:
//   - ctx is unused today; a later CredentialSource may need it.
//   - id is the identity the client acts as; it must pass Identity.Validate.
func (b *ProdClientBuilder) ResourceManager(_ context.Context, id credentials.Identity) (resourcemanager.ResourceManager, error) {
	if err := validateClientRequest(id, "", false); err != nil {
		return nil, err
	}
	serviceURL := b.resourceManagerURL
	key := clientKey{identity: id.Key, service: string(endpoints.RM), url: serviceURL}
	built, err := b.cachedClient(id, key, func(auth core.Authenticator) (any, error) {
		return newResourceManager(auth, serviceURL)
	})
	if err != nil {
		return nil, err
	}
	client, ok := built.(resourcemanager.ResourceManager)
	if !ok {
		return nil, fmt.Errorf("managed client builder: the cached client %s is a %T, not a Resource Manager client (programming error)", key.service, built)
	}
	return client, nil
}

// InvalidateAuthenticator implements ClientBuilder. key is the identity IBM answered 401 for; an unknown key is
// ignored.
func (b *ProdClientBuilder) InvalidateAuthenticator(key credentials.IdentityKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry, ok := b.auths[key]; ok {
		entry.invalidated = true
	}
}

// validateClientRequest checks the arguments every client method shares.
//
// Parameters:
//   - id is the identity of the request.
//   - region is the region of the request ("" when the service is global).
//   - regional tells whether the service needs a region.
func validateClientRequest(id credentials.Identity, region string, regional bool) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("managed client builder: %w", err)
	}
	if regional && !isRegionName(region) {
		return fmt.Errorf("managed client builder: region %q is not an IBM Cloud region name such as us-south or eu-de; "+
			"pass IBMCloudManagedCluster.spec.region", region)
	}
	return nil
}

// isRegionName reports whether region has the form of an IBM Cloud multizone region, two lower-case letters, a dash
// and lower-case letters (the CRD's spec.region pattern). The region becomes part of a host name, so anything else is
// refused before a URL is built from it.
func isRegionName(region string) bool {
	geography, area, found := strings.Cut(region, "-")
	if !found || len(geography) != 2 || area == "" {
		return false
	}
	for _, r := range geography + area {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}

// containersURL returns the IKS API URL for region: the --service-endpoint entry "<region>:containers=<url>" when
// present (for example a private endpoint), else the global URL. An entry for another region is never used, because the
// private IKS endpoints are regional. region is the cluster's region.
func (b *ProdClientBuilder) containersURL(region string) string {
	if regional := endpoints.FetchRegionalEndpoint(string(endpoints.Containers), region, b.opts.ServiceEndpoints); regional != "" {
		return regional
	}
	return kubernetesserviceapiv1.DefaultServiceURL
}

// limiterFor returns the limiter for the clients of one identity. It deliberately returns the single process-wide
// limiter: IBM limits the IKS API per source IP (https://cloud.ibm.com/docs/containers?topic=containers-limitations),
// so per-identity limiters would multiply the request rate past IBM's limit. A fairness layer between identities, if
// ever needed, is added here.
func (b *ProdClientBuilder) limiterFor(credentials.IdentityKey) kubernetesservice.RequestLimiter {
	return b.opts.Limiter
}

// cachedClient returns the cached client of key, or builds it with build over a new identityAuthenticator and caches
// it. It first makes sure id has a usable authenticator, so that a missing or failing credential is reported here,
// with the identity's description, instead of on the first request.
//
// Parameters:
//   - id is the identity the client acts as.
//   - key is the client's cache key; key.identity is id.Key.
//   - build constructs the client over the authenticator it is given.
func (b *ProdClientBuilder) cachedClient(id credentials.Identity, key clientKey, build func(core.Authenticator) (any, error)) (any, error) {
	if err := b.authFor(id); err != nil {
		return nil, err
	}
	b.mu.Lock()
	cached, ok := b.clients[key]
	b.mu.Unlock()
	if ok {
		return cached, nil
	}
	// Built without holding mu: the SDK constructors call identityAuthenticator.Validate, which takes mu.
	built, err := build(&identityAuthenticator{key: id.Key, b: b})
	if err != nil {
		return nil, fmt.Errorf("managed client builder: building the %s client for %s at %s: %w", key.service, id.Description, key.url, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Another controller may have built the same client meanwhile; keep the first, so that every caller shares it.
	if cached, ok := b.clients[key]; ok {
		return cached, nil
	}
	b.clients[key] = built
	return built, nil
}

// authFor makes sure that id has a usable cached authenticator, the one its clients' identityAuthenticator then uses.
// It builds one when none is cached, when the cached one was built for another Version, when it is older than
// authenticatorMaxAge, or after InvalidateAuthenticator; it first evicts every identity idle for authenticatorIdleTime.
// A failed build, and a factory that returns a nil authenticator, return an error and store nothing, so the next use
// tries again. id is a validated identity.
func (b *ProdClientBuilder) authFor(id credentials.Identity) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.opts.Clock.Now()
	b.evictIdleLocked(now)
	entry, ok := b.auths[id.Key]
	if ok && entry.version == id.Version && entry.usable(now) {
		entry.usedAt = now
		return nil
	}
	if ok && entry.version != id.Version {
		// A credential-forming input changed: the old authenticator must not sign another request of an existing client,
		// even when the new one cannot be built.
		delete(b.auths, id.Key)
	}
	_, err := b.rebuildLocked(id.Key, id.Description, id.Source, id.Version, now)
	return err
}

// currentAuthenticator returns the authenticator of key for a request of an existing client. It rebuilds an
// invalidated or aged authenticator from the source and version it was built from; the version only changes through
// authFor, which sees the resolved identity. key is the client's identity key.
func (b *ProdClientBuilder) currentAuthenticator(key credentials.IdentityKey) (core.Authenticator, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.opts.Clock.Now()
	entry, ok := b.auths[key]
	if !ok {
		return nil, fmt.Errorf("managed client builder: no usable authenticator is cached for identity %s (it was unused for %s, "+
			"or its credentials changed and the new authenticator could not be built); the next reconcile resolves the identity and builds it again",
			key, authenticatorIdleTime)
	}
	if entry.usable(now) {
		entry.usedAt = now
		return entry.auth, nil
	}
	return b.rebuildLocked(key, key.String(), entry.source, entry.version, now)
}

// usable reports whether the entry's authenticator may be used at now: not invalidated and younger than
// authenticatorMaxAge.
func (e *authEntry) usable(now time.Time) bool {
	return !e.invalidated && now.Sub(e.builtAt) < authenticatorMaxAge
}

// rebuildLocked builds a new authenticator and replaces the cache entry of key. On failure the old entry stays as it
// is (still unusable, so the next use tries again) and nothing new is stored. b.mu must be held.
//
// Parameters:
//   - key is the identity key.
//   - description names the identity in the error.
//   - source builds the authenticator; nil means the controller identity's injected factory.
//   - version is the Identity.Version the authenticator is built for.
//   - now is the build time.
func (b *ProdClientBuilder) rebuildLocked(key credentials.IdentityKey, description string, source credentials.CredentialSource,
	version string, now time.Time) (core.Authenticator, error) {
	auth, err := b.newAuthenticator(key, source)
	if err != nil {
		return nil, fmt.Errorf("managed client builder: building the authenticator of %s: %w", description, err)
	}
	b.auths[key] = &authEntry{auth: auth, source: source, version: version, builtAt: now, usedAt: now}
	return auth, nil
}

// newAuthenticator calls the factory of one identity: source when it is set, the injected controller factory for the
// controller identity. A nil result is an error, because go-sdk-core constructors that get a nil authenticator may fall
// back to the environment.
//
// Parameters:
//   - key is the identity key.
//   - source is the identity's CredentialSource (nil for the controller identity).
func (b *ProdClientBuilder) newAuthenticator(key credentials.IdentityKey, source credentials.CredentialSource) (core.Authenticator, error) {
	var auth core.Authenticator
	var err error
	switch {
	case !core.IsNil(source):
		auth, err = source.NewAuthenticator()
	case key == credentials.ControllerKey():
		auth, err = b.opts.NewControllerAuthenticator()
	default:
		return nil, fmt.Errorf("identity %s has no credential source; only the controller identity is built by the injected factory", key)
	}
	if err != nil {
		return nil, err
	}
	if core.IsNil(auth) {
		return nil, fmt.Errorf("the factory of identity %s returned no authenticator and no error; refusing to build clients without one", key)
	}
	return auth, nil
}

// evictIdleLocked forgets every identity unused for authenticatorIdleTime, with its clients. b.mu must be held. now is
// the current time.
func (b *ProdClientBuilder) evictIdleLocked(now time.Time) {
	for key, entry := range b.auths {
		if now.Sub(entry.usedAt) < authenticatorIdleTime {
			continue
		}
		delete(b.auths, key)
		for cached := range b.clients {
			if cached.identity == key {
				delete(b.clients, cached)
			}
		}
	}
}

// identityAuthenticator is installed in every client the builder returns (KubernetesService, VPC, ResourceManager).
// Authenticate asks the builder for the identity's current authenticator on every request, rebuilding it when it is
// older than authenticatorMaxAge or was invalidated, so a rebuild never leaves a client holding a stale authenticator.
// AuthenticationType and Validate delegate to the current authenticator.
type identityAuthenticator struct {
	key credentials.IdentityKey
	b   *ProdClientBuilder
}

var _ core.Authenticator = &identityAuthenticator{}

// Authenticate implements core.Authenticator. req is the outgoing request; the current authenticator adds its
// Authorization header.
func (a *identityAuthenticator) Authenticate(req *http.Request) error {
	auth, err := a.b.currentAuthenticator(a.key)
	if err != nil {
		return err
	}
	return auth.Authenticate(req)
}

// AuthenticationType implements core.Authenticator. It returns the current authenticator's type, or "" when none can
// be built (the next Authenticate reports why).
func (a *identityAuthenticator) AuthenticationType() string {
	auth, err := a.b.currentAuthenticator(a.key)
	if err != nil {
		return ""
	}
	return auth.AuthenticationType()
}

// Validate implements core.Authenticator; the SDK constructors call it once. It validates the current
// authenticator.
func (a *identityAuthenticator) Validate() error {
	auth, err := a.b.currentAuthenticator(a.key)
	if err != nil {
		return err
	}
	return auth.Validate()
}

// newResourceManager is the only call of resourcemanager.NewService on the managed path (the forbidigo rule exempts
// this file for that one name). resourcemanager.NewService silently builds an authenticator from the environment when
// options.Authenticator is nil (pkg/cloud/services/resourcemanager/service.go:40-46), so nil is rejected first: the
// managed path never reads credentials from the environment.
//
// Parameters:
//   - auth authenticates every request; nil (also a typed nil) is rejected.
//   - serviceURL is the Resource Manager API URL.
func newResourceManager(auth core.Authenticator, serviceURL string) (resourcemanager.ResourceManager, error) {
	if core.IsNil(auth) {
		return nil, errors.New("resource manager client: the authenticator is nil; refusing the environment fallback of resourcemanager.NewService")
	}
	return resourcemanager.NewService(&resourcemanagerv2.ResourceManagerV2Options{Authenticator: auth, URL: serviceURL})
}
