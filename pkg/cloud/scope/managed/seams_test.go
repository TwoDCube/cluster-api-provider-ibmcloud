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
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"golang.org/x/time/rate"

	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"

	. "github.com/onsi/gomega"
)

// The seam tests lock the multi-tenancy readiness seams of the client builder and the versions cache: every cache is
// keyed by identity, one limiter is shared by every identity, a rebuild touches only its own identity and reaches
// existing clients, a failed build is never cached, and nothing reads the environment. A later per-Cluster identity
// must pass them unchanged.

// errStopped is what stopAfterWait returns after waiting, so that no request leaves the process.
var errStopped = errors.New("seam test: request stopped after the limiter wait")

// stopAfterWait waits on a real limiter, records the result of each wait, and then stops the request, so that the
// test can tell which request found a token without sending anything to IBM Cloud.
type stopAfterWait struct {
	limiter *rate.Limiter
	mu      sync.Mutex
	results []error
}

// Wait implements kubernetesservice.RequestLimiter.
func (l *stopAfterWait) Wait(ctx context.Context) error {
	err := l.limiter.Wait(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.results = append(l.results, err)
	return errStopped
}

// Results returns the wait results in order.
func (l *stopAfterWait) Results() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.results...)
}

// authorizationRecorder is an httptest handler that records the Authorization header of every request and answers
// a Resource Manager resource group.
type authorizationRecorder struct {
	mu      sync.Mutex
	headers []string
}

// ServeHTTP implements http.Handler.
func (r *authorizationRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.headers = append(r.headers, req.Header.Get("Authorization"))
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"id":"0123456789abcdef0123456789abcdef","name":"test-group"}`))
}

// Last returns the Authorization header of the last request.
func (r *authorizationRecorder) Last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.headers) == 0 {
		return ""
	}
	return r.headers[len(r.headers)-1]
}

// getResourceGroup sends one request with client, the way the InfraCluster scope does, and returns its error. It is
// safe to call from several goroutines.
func getResourceGroup(client resourcemanager.ResourceManager) error {
	_, _, err := client.GetResourceGroup(&resourcemanagerv2.GetResourceGroupOptions{ID: core.StringPtr("0123456789abcdef0123456789abcdef")})
	return err
}

// signedBy sends one request with client and returns the Authorization header the test server received for it, which
// names the authenticator build that signed it ("Bearer <prefix>-<n>").
//
// Parameters:
//   - g fails the test when the request fails.
//   - recorder is the test server's handler.
//   - client is a Resource Manager client pointed at that server.
func signedBy(g *WithT, recorder *authorizationRecorder, client resourcemanager.ResourceManager) string {
	g.Expect(getResourceGroup(client)).To(Succeed())
	return recorder.Last()
}

func TestClientBuilderSeams(t *testing.T) {
	t.Run("two identity keys share one limiter", func(t *testing.T) {
		g := NewWithT(t)
		clk := clocktesting.NewFakeClock(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
		factory := &countingSource{prefix: "controller"}
		// Burst 1 and one token an hour: the first request takes the only token, the second must wait for the next.
		limiter := &stopAfterWait{limiter: rate.NewLimiter(rate.Every(time.Hour), 1)}
		opts := validBuilderOptions(clk, factory, &countingLimiter{})
		opts.Limiter = limiter
		builder, err := NewProdClientBuilder(opts)
		g.Expect(err).NotTo(HaveOccurred())
		ctx := context.Background()
		otherSource := &countingSource{prefix: "other"}
		other := sourcedIdentity("a", "", otherSource)

		controllerClient, err := builder.KubernetesService(ctx, controllerIdentity(), testRegion)
		g.Expect(err).NotTo(HaveOccurred())
		otherClient, err := builder.KubernetesService(ctx, other, testRegion)
		g.Expect(err).NotTo(HaveOccurred())

		// Two keys: two authenticators and two clients, but one limiter.
		g.Expect(controllerClient).NotTo(BeIdenticalTo(otherClient))
		g.Expect(builder.auths).To(HaveLen(2))
		g.Expect(builder.auths[credentials.ControllerKey()].auth).NotTo(BeIdenticalTo(builder.auths[other.Key].auth))
		g.Expect(builder.limiterFor(credentials.ControllerKey())).To(BeIdenticalTo(builder.limiterFor(other.Key)))
		g.Expect(builder.limiterFor(other.Key)).To(BeIdenticalTo(kubernetesservice.RequestLimiter(limiter)))

		// The first client's request takes the token; the second client's request waits on the same limiter and, with
		// a short deadline, gives up instead of finding a token of its own.
		shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err = controllerClient.ListVersions(shortCtx)
		g.Expect(err).To(HaveOccurred())
		_, err = otherClient.ListVersions(shortCtx)
		g.Expect(err).To(HaveOccurred())
		results := limiter.Results()
		g.Expect(results).To(HaveLen(2))
		g.Expect(results[0]).NotTo(HaveOccurred(), "the first request finds the burst token")
		g.Expect(results[1]).To(HaveOccurred(), "the second identity's request waits on the same limiter")

		// The same key and Version reuse the authenticator and the client; each factory ran once.
		again, err := builder.KubernetesService(ctx, controllerIdentity(), testRegion)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(again).To(BeIdenticalTo(controllerClient))
		otherAgain, err := builder.KubernetesService(ctx, other, testRegion)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(otherAgain).To(BeIdenticalTo(otherClient))
		g.Expect(factory.Calls()).To(Equal(1))
		g.Expect(otherSource.Calls()).To(Equal(1))
	})

	t.Run("rebuild only that key", func(t *testing.T) {
		g := NewWithT(t)
		builder, clk, factory := newTestBuilder(t)
		recorder := &authorizationRecorder{}
		server := httptest.NewServer(recorder)
		defer server.Close()
		// The seam: Resource Manager over plain HTTP to the test server; every request is signed by the client's
		// identityAuthenticator, so the recorded header shows which build signed it.
		builder.resourceManagerURL = server.URL
		ctx := context.Background()
		otherSource := &countingSource{prefix: "other"}
		other := sourcedIdentity("a", "v1", otherSource)

		otherClient, err := builder.ResourceManager(ctx, other)
		g.Expect(err).NotTo(HaveOccurred())
		controllerClient, err := builder.ResourceManager(ctx, controllerIdentity())
		g.Expect(err).NotTo(HaveOccurred())
		controllerEntry := builder.auths[credentials.ControllerKey()]
		g.Expect(signedBy(g, recorder, otherClient)).To(Equal("Bearer other-1"))
		g.Expect(signedBy(g, recorder, controllerClient)).To(Equal("Bearer controller-1"))

		// controllerUntouched checks, while the controller identity's authenticator is still young, that a rebuild of
		// the other identity neither marked nor replaced it: its client and a new builder call still use the first
		// build. The entry is checked before the request, because a marked entry would be rebuilt by that request.
		controllerUntouched := func(after string) {
			g.Expect(builder.auths[credentials.ControllerKey()]).To(BeIdenticalTo(controllerEntry), after)
			g.Expect(controllerEntry.invalidated).To(BeFalse(), after)
			g.Expect(signedBy(g, recorder, controllerClient)).To(Equal("Bearer controller-1"), after)
			again, err := builder.ResourceManager(ctx, controllerIdentity())
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(again).To(BeIdenticalTo(controllerClient), after)
			g.Expect(builder.auths[credentials.ControllerKey()]).To(BeIdenticalTo(controllerEntry), after)
			g.Expect(factory.Calls()).To(Equal(1), after)
		}

		// After a 401 for the other key, only that key's next request rebuilds; the rebuilt authenticator is reused.
		builder.InvalidateAuthenticator(other.Key)
		controllerUntouched("a 401 for the other identity")
		g.Expect(signedBy(g, recorder, otherClient)).To(Equal("Bearer other-2"))
		g.Expect(signedBy(g, recorder, otherClient)).To(Equal("Bearer other-2"), "a rebuilt authenticator is reused")

		// A new Version rebuilds only that identity's authenticator; its client stays the same and signs with the new
		// one.
		other.Version = "v2"
		sameClient, err := builder.ResourceManager(ctx, other)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(sameClient).To(BeIdenticalTo(otherClient))
		controllerUntouched("a new Version of the other identity")
		g.Expect(signedBy(g, recorder, otherClient)).To(Equal("Bearer other-3"))

		// Age: the two authenticators are made thirty minutes apart (a second 401 for the other key at +30m). At +60m
		// only the controller identity's is one hour old: its existing client rebuilds on its next request, without a
		// builder call, and the younger other authenticator is kept.
		clk.Step(authenticatorMaxAge / 2)
		builder.InvalidateAuthenticator(other.Key)
		g.Expect(signedBy(g, recorder, otherClient)).To(Equal("Bearer other-4"))
		otherEntry := builder.auths[other.Key]
		clk.Step(authenticatorMaxAge / 2)
		g.Expect(signedBy(g, recorder, controllerClient)).To(Equal("Bearer controller-2"))
		g.Expect(factory.Calls()).To(Equal(2))
		g.Expect(builder.auths[other.Key]).To(BeIdenticalTo(otherEntry), "the younger identity is not rebuilt")
		g.Expect(otherEntry.invalidated).To(BeFalse())
		g.Expect(signedBy(g, recorder, otherClient)).To(Equal("Bearer other-4"))
		g.Expect(otherSource.Calls()).To(Equal(4))
	})

	t.Run("failure not cached", func(t *testing.T) {
		g := NewWithT(t)
		builder, _, factory := newTestBuilder(t)
		ctx := context.Background()

		factory.failures = []error{errors.New("credentials file is unreadable")}
		_, err := builder.ResourceManager(ctx, controllerIdentity())
		g.Expect(err).To(MatchError(ContainSubstring("credentials file is unreadable")))
		g.Expect(builder.auths).To(BeEmpty())
		g.Expect(builder.clients).To(BeEmpty())

		_, err = builder.ResourceManager(ctx, controllerIdentity())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(factory.Calls()).To(Equal(2), "the failure was not cached, so the second call built again")

		nilBuilder, _, nilFactory := newTestBuilder(t)
		nilFactory.returnNil = true
		_, err = nilBuilder.KubernetesService(ctx, controllerIdentity(), testRegion)
		g.Expect(err).To(MatchError(ContainSubstring("returned no authenticator and no error")))
		g.Expect(nilBuilder.auths).To(BeEmpty())
		g.Expect(nilBuilder.clients).To(BeEmpty())

		// A failed rebuild after a Version change leaves no authenticator behind for the existing client: the old one
		// belongs to credentials that changed.
		source := &countingSource{prefix: "other"}
		other := sourcedIdentity("a", "v1", source)
		client, err := builder.ResourceManager(ctx, other)
		g.Expect(err).NotTo(HaveOccurred())
		source.failures = []error{errors.New("new key not readable")}
		other.Version = "v2"
		_, err = builder.ResourceManager(ctx, other)
		g.Expect(err).To(MatchError(ContainSubstring("new key not readable")))
		g.Expect(builder.auths).NotTo(HaveKey(other.Key))
		_, _, err = client.GetResourceGroup(&resourcemanagerv2.GetResourceGroupOptions{ID: core.StringPtr("0123456789abcdef0123456789abcdef")})
		g.Expect(err).To(MatchError(ContainSubstring("no usable authenticator is cached")))
	})

	t.Run("no environment read", func(t *testing.T) {
		g := NewWithT(t)
		// An environment from which go-sdk-core would happily build a bearer-token authenticator. A managed client must
		// never use it: a nil factory result is an error, not a reason to look elsewhere.
		t.Setenv("IBM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "does-not-exist.env"))
		t.Setenv("IBMCLOUD_AUTH_TYPE", "bearerToken")
		t.Setenv("IBMCLOUD_BEARER_TOKEN", "environment-token")
		t.Setenv("IBMCLOUD_APIKEY", "environment-api-key")
		t.Setenv("IBMCLOUD_AUTH_URL", "https://iam.invalid")

		builder, _, factory := newTestBuilder(t)
		factory.returnNil = true
		ctx := context.Background()

		rm, err := builder.ResourceManager(ctx, controllerIdentity())
		g.Expect(err).To(MatchError(ContainSubstring("returned no authenticator")))
		g.Expect(rm).To(BeNil())
		iks, err := builder.KubernetesService(ctx, controllerIdentity(), testRegion)
		g.Expect(err).To(HaveOccurred())
		g.Expect(iks).To(BeNil())
		vpcClient, err := builder.VPC(ctx, controllerIdentity(), testRegion)
		g.Expect(err).To(HaveOccurred())
		g.Expect(vpcClient).To(BeNil())

		// newResourceManager refuses nil before resourcemanager.NewService, which would build an authenticator from the
		// environment above and succeed.
		rm, err = newResourceManager(nil, "https://resource-controller.cloud.ibm.com")
		g.Expect(err).To(MatchError(ContainSubstring("refusing the environment fallback")))
		g.Expect(rm).To(BeNil())
		rm, err = newResourceManager((*core.BearerTokenAuthenticator)(nil), "https://resource-controller.cloud.ibm.com")
		g.Expect(err).To(MatchError(ContainSubstring("refusing the environment fallback")))
		g.Expect(rm).To(BeNil())

		// The other two managed constructors reject nil as well; the tests of their own packages cover them fully.
		_, err = vpc.NewServiceWithAuthenticator("https://us-south.iaas.cloud.ibm.com/v1", nil)
		g.Expect(err).To(HaveOccurred())
		_, err = kubernetesservice.NewService(kubernetesservice.ServiceOptions{
			URL: "https://containers.cloud.ibm.com/global", RateLimiter: &countingLimiter{}, HTTPTimeout: time.Second,
		})
		g.Expect(err).To(HaveOccurred())
	})

	t.Run("caches keyed by identity", func(t *testing.T) {
		g := NewWithT(t)
		builder, clk, _ := newTestBuilder(t)
		ctx := context.Background()
		other := sourcedIdentity("a", "", &countingSource{prefix: "other"})

		// IKS clients: two keys and one region give two clients; one key twice gives one.
		first, err := builder.KubernetesService(ctx, controllerIdentity(), testRegion)
		g.Expect(err).NotTo(HaveOccurred())
		second, err := builder.KubernetesService(ctx, other, testRegion)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(second).NotTo(BeIdenticalTo(first))
		again, err := builder.KubernetesService(ctx, controllerIdentity(), testRegion)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(again).To(BeIdenticalTo(first))

		// Versions: two keys and one region give two ListVersions calls; one key twice within the TTL gives one.
		cache, err := NewVersionsCache(clk, time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		calls := map[credentials.IdentityKey]int{}
		listFor := func(key credentials.IdentityKey) func(context.Context) (*kubernetesservice.Versions, error) {
			return func(context.Context) (*kubernetesservice.Versions, error) {
				calls[key]++
				return &kubernetesservice.Versions{Kubernetes: []kubernetesservice.ReleaseVersion{{Major: 1, Minor: 36, Patch: calls[key]}}}, nil
			}
		}
		for _, key := range []credentials.IdentityKey{credentials.ControllerKey(), other.Key, credentials.ControllerKey()} {
			_, err := cache.Get(ctx, key, testRegion, listFor(key))
			g.Expect(err).NotTo(HaveOccurred())
		}
		g.Expect(calls).To(Equal(map[credentials.IdentityKey]int{credentials.ControllerKey(): 1, other.Key: 1}))

		clk.Step(59 * time.Minute)
		_, err = cache.Get(ctx, credentials.ControllerKey(), testRegion, listFor(credentials.ControllerKey()))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(calls[credentials.ControllerKey()]).To(Equal(1), "within one hour the cached answer is reused")
	})
}
