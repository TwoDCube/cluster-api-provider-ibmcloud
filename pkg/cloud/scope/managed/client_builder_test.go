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
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM-Cloud/container-services-go-sdk/kubernetesserviceapiv1"
	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"

	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/endpoints"

	. "github.com/onsi/gomega"
)

// This file tests the option validation and the endpoint selection of ProdClientBuilder, and holds the fakes that the
// seam tests (seams_test.go) share. No test here or there sends a request to IBM Cloud: the builder makes no call when
// it builds a client, and the tests that send requests point the client at an httptest server or stop the request in
// the limiter.

// testRegion is the region of the builder tests.
const testRegion = "us-south"

// countingSource is a credentials.CredentialSource, and with its NewAuthenticator method the controller-identity
// factory, that counts its calls. Each successful call returns a bearer-token authenticator whose token names the
// call ("<prefix>-<n>"), so a request shows which build signed it.
type countingSource struct {
	mu     sync.Mutex
	prefix string
	calls  int
	// failures are returned, in order, by the first calls; a nil entry means "succeed".
	failures []error
	// returnNil makes every call return (nil, nil).
	returnNil bool
}

// NewAuthenticator implements credentials.CredentialSource.
func (s *countingSource) NewAuthenticator() (core.Authenticator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.failures) > 0 {
		failure := s.failures[0]
		s.failures = s.failures[1:]
		if failure != nil {
			return nil, failure
		}
	}
	if s.returnNil {
		return nil, nil
	}
	return core.NewBearerTokenAuthenticator(fmt.Sprintf("%s-%d", s.prefix, s.calls))
}

// Calls returns how often NewAuthenticator ran.
func (s *countingSource) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// countingLimiter is a kubernetesservice.RequestLimiter that counts its waits and answers err (nil: admit).
type countingLimiter struct {
	mu    sync.Mutex
	waits int
	err   error
}

// Wait implements kubernetesservice.RequestLimiter.
func (l *countingLimiter) Wait(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waits++
	return l.err
}

// Waits returns how often Wait ran.
func (l *countingLimiter) Waits() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waits
}

// controllerIdentity is what credentials.ControllerOnly resolves.
func controllerIdentity() credentials.Identity {
	return credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}
}

// sourcedIdentity is a second identity that carries its own source, as a later per-Cluster identity would.
//
// Parameters:
//   - name names the identity.
//   - version is its Version.
//   - source builds its authenticator.
func sourcedIdentity(name, version string, source credentials.CredentialSource) credentials.Identity {
	return credentials.Identity{
		Key:         credentials.IdentityKey{Kind: "TestIdentity", Name: name, UID: types.UID("uid-" + name)},
		Version:     version,
		Description: "the test identity " + name,
		Source:      source,
	}
}

// validBuilderOptions returns options that NewProdClientBuilder accepts.
//
// Parameters:
//   - clk is the builder's clock.
//   - factory is the controller-identity factory.
//   - limiter is the process-wide limiter.
func validBuilderOptions(clk *clocktesting.FakeClock, factory *countingSource, limiter *countingLimiter) ProdClientBuilderOptions {
	return ProdClientBuilderOptions{
		Limiter:                    limiter,
		HTTPTimeout:                5 * time.Second,
		UserAgent:                  "cluster-api-provider-ibmcloud/test",
		Clock:                      clk,
		NewControllerAuthenticator: factory.NewAuthenticator,
	}
}

// newTestBuilder returns a builder over a fake clock, a counting controller factory and a counting limiter.
func newTestBuilder(t *testing.T) (*ProdClientBuilder, *clocktesting.FakeClock, *countingSource) {
	t.Helper()
	clk := clocktesting.NewFakeClock(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	factory := &countingSource{prefix: "controller"}
	limiter := &countingLimiter{}
	builder, err := NewProdClientBuilder(validBuilderOptions(clk, factory, limiter))
	if err != nil {
		t.Fatalf("building the test client builder: %v", err)
	}
	return builder, clk, factory
}

func TestNewProdClientBuilderValidation(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	factory := &countingSource{prefix: "controller"}
	limiter := &countingLimiter{}

	testCases := []struct {
		name    string
		mutate  func(*ProdClientBuilderOptions)
		wantErr []string
	}{
		{name: "valid options without endpoints", mutate: func(*ProdClientBuilderOptions) {}},
		{
			name:    "missing limiter is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.Limiter = nil },
			wantErr: []string{"Limiter is nil", "--containers-api-qps"},
		},
		{
			name:    "typed nil limiter is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.Limiter = (*countingLimiter)(nil) },
			wantErr: []string{"Limiter is nil"},
		},
		{
			name:    "zero timeout is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.HTTPTimeout = 0 },
			wantErr: []string{"HTTPTimeout is 0s", "30s"},
		},
		{
			name:    "empty user agent is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.UserAgent = "" },
			wantErr: []string{"UserAgent \"\" is not a single-line product token"},
		},
		{
			name:    "multi-line user agent is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.UserAgent = "capibm\r\nX-Injected: 1" },
			wantErr: []string{"is not a single-line product token"},
		},
		{
			name:    "missing clock is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.Clock = nil },
			wantErr: []string{"Clock is nil", "clock.RealClock{}"},
		},
		{
			name:    "missing controller factory is named",
			mutate:  func(o *ProdClientBuilderOptions) { o.NewControllerAuthenticator = nil },
			wantErr: []string{"NewControllerAuthenticator is nil", "authenticator.GetAuthenticator"},
		},
		{
			// The builder applies no rule of its own to a --service-endpoint URL: each one goes to the IBM SDK as it is.
			name: "service endpoints are taken as they are",
			mutate: func(o *ProdClientBuilderOptions) {
				o.ServiceEndpoints = []endpoints.ServiceEndpoint{
					{ID: string(endpoints.Containers), Region: "eu-de", URL: "http://private.eu-de.containers.cloud.ibm.com/global"},
					{ID: string(endpoints.VPC), Region: testRegion, URL: "https://us-south.private.iaas.cloud.ibm.com/v1"},
					{ID: string(endpoints.RM), Region: testRegion, URL: "https://private.resource-controller.cloud.ibm.com"},
					{ID: "powervs", Region: testRegion, URL: "http://example.com"},
				}
			},
		},
		{
			name: "every problem is reported at once",
			mutate: func(o *ProdClientBuilderOptions) {
				o.Limiter = nil
				o.Clock = nil
			},
			wantErr: []string{"Limiter is nil", "Clock is nil"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			opts := validBuilderOptions(clk, factory, limiter)
			tc.mutate(&opts)
			builder, err := NewProdClientBuilder(opts)
			if len(tc.wantErr) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(builder).NotTo(BeNil())
				return
			}
			g.Expect(err).To(HaveOccurred())
			g.Expect(builder).To(BeNil())
			for _, want := range tc.wantErr {
				g.Expect(err.Error()).To(ContainSubstring(want))
			}
		})
	}
	// Building the builder builds no authenticator: the first client does.
	NewWithT(t).Expect(factory.Calls()).To(BeZero())
}

func TestProdClientBuilderEndpointSelection(t *testing.T) {
	const privateEUDE = "https://private.eu-de.containers.cloud.ibm.com/global"
	const privateVPC = "https://eu-de.private.iaas.cloud.ibm.com/v1"
	const privateRM = "https://private.resource-controller.cloud.ibm.com"
	serviceEndpoints := []endpoints.ServiceEndpoint{
		{ID: string(endpoints.Containers), Region: "eu-de", URL: privateEUDE},
		{ID: string(endpoints.VPC), Region: "eu-de", URL: privateVPC},
		{ID: string(endpoints.RM), Region: "eu-de", URL: privateRM},
	}

	testCases := []struct {
		name           string
		endpoints      []endpoints.ServiceEndpoint
		region         string
		wantContainers string
		wantVPC        string
		wantRM         string
	}{
		{
			name:           "no service endpoints: global IKS, regional VPC, global Resource Manager",
			region:         testRegion,
			wantContainers: kubernetesserviceapiv1.DefaultServiceURL,
			wantVPC:        "https://us-south.iaas.cloud.ibm.com/v1",
			wantRM:         resourcemanagerv2.DefaultServiceURL,
		},
		{
			name:           "the region's entries win",
			endpoints:      serviceEndpoints,
			region:         "eu-de",
			wantContainers: privateEUDE,
			wantVPC:        privateVPC,
			wantRM:         privateRM,
		},
		{
			name:           "another region's IKS and VPC entries are never used; Resource Manager is global",
			endpoints:      serviceEndpoints,
			region:         testRegion,
			wantContainers: kubernetesserviceapiv1.DefaultServiceURL,
			wantVPC:        "https://us-south.iaas.cloud.ibm.com/v1",
			wantRM:         privateRM,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			clk := clocktesting.NewFakeClock(time.Now())
			limiter := &countingLimiter{}
			opts := validBuilderOptions(clk, &countingSource{prefix: "controller"}, limiter)
			opts.ServiceEndpoints = tc.endpoints
			builder, err := NewProdClientBuilder(opts)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(builder.containersURL(tc.region)).To(Equal(tc.wantContainers))
			g.Expect(builder.resourceManagerURL).To(Equal(tc.wantRM))

			// The clients are cached per URL: building them records the URLs the builder chose.
			ctx := context.Background()
			_, err = builder.KubernetesService(ctx, controllerIdentity(), tc.region)
			g.Expect(err).NotTo(HaveOccurred())
			_, err = builder.VPC(ctx, controllerIdentity(), tc.region)
			g.Expect(err).NotTo(HaveOccurred())
			_, err = builder.ResourceManager(ctx, controllerIdentity())
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(builder.clients).To(HaveKey(clientKey{identity: credentials.ControllerKey(), service: "containers", url: tc.wantContainers}))
			g.Expect(builder.clients).To(HaveKey(clientKey{identity: credentials.ControllerKey(), service: "vpc", url: tc.wantVPC}))
			g.Expect(builder.clients).To(HaveKey(clientKey{identity: credentials.ControllerKey(), service: "rm", url: tc.wantRM}))
			g.Expect(limiter.Waits()).To(BeZero(), "building a client sends no request")
		})
	}
}

func TestProdClientBuilderRejectsInvalidRequests(t *testing.T) {
	builder, _, factory := newTestBuilder(t)
	ctx := context.Background()

	testCases := []struct {
		name    string
		call    func() error
		wantErr string
	}{
		{
			name: "IKS client for an empty region",
			call: func() error {
				_, err := builder.KubernetesService(ctx, controllerIdentity(), "")
				return err
			},
			wantErr: `region "" is not an IBM Cloud region name`,
		},
		{
			name: "VPC client for a region that would change the host",
			call: func() error {
				_, err := builder.VPC(ctx, controllerIdentity(), "us-south.evil.example/")
				return err
			},
			wantErr: "is not an IBM Cloud region name",
		},
		{
			name: "VPC client for an upper-case region",
			call: func() error {
				_, err := builder.VPC(ctx, controllerIdentity(), "US-south")
				return err
			},
			wantErr: "is not an IBM Cloud region name",
		},
		{
			name: "Resource Manager client for an identity without key",
			call: func() error {
				_, err := builder.ResourceManager(ctx, credentials.Identity{Description: "nobody"})
				return err
			},
			wantErr: "no Key.Kind",
		},
		{
			name: "IKS client for a non-controller identity without source",
			call: func() error {
				_, err := builder.KubernetesService(ctx, credentials.Identity{
					Key: credentials.IdentityKey{Kind: "TestIdentity", Name: "x"}, Description: "the test identity x",
				}, testRegion)
				return err
			},
			wantErr: "has no Source",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.call()).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
	// A rejected request builds nothing.
	g := NewWithT(t)
	g.Expect(factory.Calls()).To(BeZero())
	g.Expect(builder.auths).To(BeEmpty())
	g.Expect(builder.clients).To(BeEmpty())
}

func TestIsRegionName(t *testing.T) {
	testCases := []struct {
		region string
		want   bool
	}{
		{region: "us-south", want: true},
		{region: "eu-de", want: true},
		{region: "jp-tok", want: true},
		{region: "", want: false},
		{region: "us", want: false},
		{region: "us-", want: false},
		{region: "usa-south", want: false},
		{region: "us-south-1", want: false},
		{region: "us-South", want: false},
		{region: "us-south.example.com", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.region, func(t *testing.T) {
			NewWithT(t).Expect(isRegionName(tc.region)).To(Equal(tc.want))
		})
	}
}

func TestProdClientBuilderIdleEviction(t *testing.T) {
	g := NewWithT(t)
	builder, clk, factory := newTestBuilder(t)
	ctx := context.Background()
	other := &countingSource{prefix: "other"}

	_, err := builder.ResourceManager(ctx, controllerIdentity())
	g.Expect(err).NotTo(HaveOccurred())
	_, err = builder.ResourceManager(ctx, sourcedIdentity("a", "", other))
	g.Expect(err).NotTo(HaveOccurred())

	// The controller identity stays in use; the other one is idle for two hours and is evicted with its client.
	clk.Step(90 * time.Minute)
	_, err = builder.ResourceManager(ctx, controllerIdentity())
	g.Expect(err).NotTo(HaveOccurred())
	clk.Step(30 * time.Minute)
	_, err = builder.ResourceManager(ctx, controllerIdentity())
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(builder.auths).To(HaveLen(1))
	g.Expect(builder.auths).To(HaveKey(credentials.ControllerKey()))
	g.Expect(builder.clients).To(HaveLen(1))
	g.Expect(factory.Calls()).To(Equal(2), "the controller authenticator is rebuilt once after an hour")
	g.Expect(other.Calls()).To(Equal(1))

	// An evicted identity's existing client fails with a message, and the next resolution builds it again.
	evicted := &identityAuthenticator{key: sourcedIdentity("a", "", other).Key, b: builder}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.invalid", http.NoBody)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(evicted.Authenticate(request)).To(MatchError(ContainSubstring("no usable authenticator is cached for identity TestIdentity/a")))
	g.Expect(evicted.AuthenticationType()).To(BeEmpty())
	g.Expect(evicted.Validate()).To(HaveOccurred())
	_, err = builder.ResourceManager(ctx, sourcedIdentity("a", "", other))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(other.Calls()).To(Equal(2))
}

func TestIdentityAuthenticatorDelegates(t *testing.T) {
	g := NewWithT(t)
	builder, _, _ := newTestBuilder(t)
	err := builder.authFor(controllerIdentity())
	g.Expect(err).NotTo(HaveOccurred())

	delegating := &identityAuthenticator{key: credentials.ControllerKey(), b: builder}
	g.Expect(delegating.AuthenticationType()).To(Equal(core.AUTHTYPE_BEARER_TOKEN))
	g.Expect(delegating.Validate()).To(Succeed())
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.invalid", http.NoBody)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(delegating.Authenticate(request)).To(Succeed())
	g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer controller-1"))
}

// TestProdClientBuilderConcurrentUse locks the mutex discipline of ProdClientBuilder: the managed controllers share one
// builder, and every client request calls back into it from the SDK's goroutine (identityAuthenticator.Authenticate
// -> currentAuthenticator). Run it with go test -race: the assertions check the cache invariants, the race detector
// checks the locking.
func TestProdClientBuilderConcurrentUse(t *testing.T) {
	g := NewWithT(t)
	builder, clk, factory := newTestBuilder(t)
	recorder := &authorizationRecorder{}
	server := httptest.NewServer(recorder)
	defer server.Close()
	// Resource Manager requests go to the plain-HTTP test server, so the SDK signs them through the client's
	// identityAuthenticator as in production. IKS and VPC clients are only built: their requests would leave the
	// process, so their delegating authenticator is called directly instead.
	builder.resourceManagerURL = server.URL
	ctx := context.Background()
	otherSource := &countingSource{prefix: "other"}
	identities := []credentials.Identity{controllerIdentity(), sourcedIdentity("a", "v1", otherSource)}
	// idle is used once, before the run, and then never again, so the stepping clock evicts it and its client while
	// the other identities are in use.
	idleSource := &countingSource{prefix: "idle"}
	idle := sourcedIdentity("idle", "", idleSource)
	_, err := builder.ResourceManager(ctx, idle)
	g.Expect(err).NotTo(HaveOccurred())

	var (
		mu    sync.Mutex
		first = map[string]any{} // the first client seen per identity and service
		errs  []error
	)
	report := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}
	sameClient := func(id credentials.Identity, service string, client any) {
		mu.Lock()
		defer mu.Unlock()
		name := id.Key.String() + "/" + service
		if seen, ok := first[name]; !ok {
			first[name] = client
		} else if seen != client {
			errs = append(errs, fmt.Errorf("%s: the builder returned a second client", name))
		}
	}
	// use builds the three clients of id, sends one Resource Manager request and authenticates one request the way
	// the IKS and VPC clients do.
	use := func(id credentials.Identity) error {
		iks, err := builder.KubernetesService(ctx, id, testRegion)
		if err != nil {
			return err
		}
		sameClient(id, string(endpoints.Containers), iks)
		vpcClient, err := builder.VPC(ctx, id, testRegion)
		if err != nil {
			return err
		}
		sameClient(id, string(endpoints.VPC), vpcClient)
		rm, err := builder.ResourceManager(ctx, id)
		if err != nil {
			return err
		}
		sameClient(id, string(endpoints.RM), rm)
		if err := getResourceGroup(rm); err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.invalid", http.NoBody)
		if err != nil {
			return err
		}
		return (&identityAuthenticator{key: id.Key, b: builder}).Authenticate(request)
	}

	// The workers run until the stepper is done, so every clock reading sees concurrent use; each of them also sends
	// an IBM 401 for one identity now and then.
	const workers, minRounds, steps = 8, 25, 40
	var stepping atomic.Bool
	stepping.Store(true)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for round := 0; round < minRounds || stepping.Load(); round++ {
				id := identities[(worker+round)%len(identities)]
				if round%5 == 0 {
					builder.InvalidateAuthenticator(id.Key)
				}
				if err := use(id); err != nil {
					report(err)
					return
				}
			}
		})
	}
	// The clock passes authenticatorMaxAge and authenticatorIdleTime several times (40 x 7m). After each step the
	// stepper uses both identities, so neither is idle for authenticatorIdleTime and each must keep its clients; only
	// idle is evicted.
	wg.Go(func() {
		defer stepping.Store(false)
		for range steps {
			clk.Step(7 * time.Minute)
			for _, id := range identities {
				if err := use(id); err != nil {
					report(err)
					return
				}
			}
		}
	})
	wg.Wait()

	g.Expect(errs).To(BeEmpty())
	// Two identities times three services, each the one instance every caller got; idle's client is gone.
	g.Expect(builder.clients).To(HaveLen(6))
	for key, client := range builder.clients {
		g.Expect(client).To(BeIdenticalTo(first[key.identity.String()+"/"+key.service]))
	}
	g.Expect(builder.auths).To(HaveLen(2))
	g.Expect(builder.auths).NotTo(HaveKey(idle.Key))
	// Ages and 401s rebuilt both authenticators during the run; idle was built once.
	g.Expect(factory.Calls()).To(BeNumerically(">", 1))
	g.Expect(otherSource.Calls()).To(BeNumerically(">", 1))
	g.Expect(idleSource.Calls()).To(Equal(1))
}

func TestInvalidateAuthenticatorOfUnknownKeyIsIgnored(t *testing.T) {
	g := NewWithT(t)
	builder, _, _ := newTestBuilder(t)
	builder.InvalidateAuthenticator(credentials.IdentityKey{Kind: "TestIdentity", Name: "unknown"})
	g.Expect(builder.auths).To(BeEmpty())
}

func TestNewAuthenticatorWrapsFactoryErrors(t *testing.T) {
	g := NewWithT(t)
	builder, _, factory := newTestBuilder(t)
	factory.failures = []error{errors.New("credentials file is unreadable")}
	_, err := builder.VPC(context.Background(), controllerIdentity(), testRegion)
	g.Expect(err).To(MatchError(ContainSubstring("building the authenticator of the CAPIBM controller identity: credentials file is unreadable")))
}

// blackholeAddress starts a TCP listener that accepts connections and never reads or writes on them, like an IBM Cloud
// endpoint whose response never arrives, and returns its host:port. A plain-HTTP request to it is sent and then waits
// for response headers; an https request waits in the TLS handshake. The test's cleanup closes the listener and every
// accepted connection.
func blackholeAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on a loopback port: %v", err)
	}
	var conns []net.Conn
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // the cleanup closed the listener
			}
			conns = append(conns, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		// Reading conns after the accept loop has ended needs no lock: closing acceptDone orders the two.
		<-acceptDone
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return listener.Addr().String()
}

// TestProdClientBuilderHTTPTimeoutBoundsEveryClient proves that HTTPTimeout reaches every client the builder returns:
// against an endpoint that accepts connections and never answers, a call of the IKS, the VPC and the Resource Manager
// client of one identity fails soon after the timeout instead of blocking the single-worker IBMCloudManagedCluster
// controller for good.
func TestProdClientBuilderHTTPTimeoutBoundsEveryClient(t *testing.T) {
	const timeout = 100 * time.Millisecond
	// margin is generous, so that a loaded machine or the race detector cannot fail the test, and still below the 10 s
	// TLS handshake timeout of go-sdk-core's transport, which would otherwise end the https calls: only HTTPTimeout ends
	// them this early. Without HTTPTimeout the plain-HTTP Resource Manager call would not end at all.
	const margin = 5 * time.Second
	address := blackholeAddress(t)

	g := NewWithT(t)
	clk := clocktesting.NewFakeClock(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	opts := validBuilderOptions(clk, &countingSource{prefix: "controller"}, &countingLimiter{})
	opts.HTTPTimeout = timeout
	opts.ServiceEndpoints = []endpoints.ServiceEndpoint{
		{ID: string(endpoints.Containers), Region: testRegion, URL: "https://" + address + "/global"},
		{ID: string(endpoints.VPC), Region: testRegion, URL: "https://" + address + "/v1"},
	}
	builder, err := NewProdClientBuilder(opts)
	g.Expect(err).NotTo(HaveOccurred())
	// Resource Manager goes over plain HTTP, so its request is sent and only the response is missing, the hang the
	// timeout exists for; the IKS and VPC requests wait in the TLS handshake that the endpoint never answers.
	builder.resourceManagerURL = "http://" + address
	ctx := context.Background()
	id := controllerIdentity()

	testCases := []struct {
		name string
		// call builds the client of id with builder and sends one request.
		call func() error
		// wantErr is a substring of the error; "" for the IKS client, which reports transport errors in its own words.
		wantErr string
	}{
		{
			name: "IKS client",
			call: func() error {
				client, err := builder.KubernetesService(ctx, id, testRegion)
				if err != nil {
					return fmt.Errorf("building the client: %w", err)
				}
				_, err = client.ListVersions(ctx)
				return err
			},
		},
		{
			name: "VPC client",
			call: func() error {
				client, err := builder.VPC(ctx, id, testRegion)
				if err != nil {
					return fmt.Errorf("building the client: %w", err)
				}
				_, _, err = client.GetVPC(&vpcv1.GetVPCOptions{ID: core.StringPtr("r006-00000000-0000-0000-0000-000000000000")})
				return err
			},
			wantErr: "Client.Timeout exceeded",
		},
		{
			name: "Resource Manager client",
			call: func() error {
				client, err := builder.ResourceManager(ctx, id)
				if err != nil {
					return fmt.Errorf("building the client: %w", err)
				}
				return getResourceGroup(client)
			},
			wantErr: "Client.Timeout exceeded",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			start := time.Now()
			err := tc.call()
			elapsed := time.Since(start)

			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).NotTo(ContainSubstring("building the client"))
			g.Expect(err.Error()).To(ContainSubstring(tc.wantErr))
			g.Expect(elapsed).To(BeNumerically(">=", timeout))
			g.Expect(elapsed).To(BeNumerically("<", timeout+margin))
		})
	}
}

// TestNewResourceManager covers the managed path's own guard in front of the Resource Manager constructor: nil is
// refused here, whatever the constructor does, and the constructor's own validation still applies.
func TestNewResourceManager(t *testing.T) {
	validAuth := &core.BearerTokenAuthenticator{BearerToken: "test-token"}

	testCases := []struct {
		name       string
		auth       core.Authenticator
		serviceURL string
		timeout    time.Duration
		wantErr    string // "" means success
	}{
		{name: "valid arguments", auth: validAuth, serviceURL: resourcemanagerv2.DefaultServiceURL, timeout: 30 * time.Second},
		{name: "nil authenticator is refused first", auth: nil, serviceURL: resourcemanagerv2.DefaultServiceURL, timeout: 0, wantErr: "refusing the environment fallback"},
		{name: "typed nil authenticator is refused", auth: (*core.IamAuthenticator)(nil), serviceURL: resourcemanagerv2.DefaultServiceURL, timeout: time.Second, wantErr: "refusing the environment fallback"},
		{name: "the constructor rejects a zero timeout", auth: validAuth, serviceURL: resourcemanagerv2.DefaultServiceURL, timeout: 0, wantErr: "resource manager client request timeout is 0s"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			client, err := newResourceManager(tc.auth, tc.serviceURL, tc.timeout)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(client).To(BeNil())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(client).NotTo(BeNil())
		})
	}
}
