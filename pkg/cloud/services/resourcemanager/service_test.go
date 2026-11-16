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

package resourcemanager

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"

	. "github.com/onsi/gomega"
)

// The tests of this file cover the explicit-credentials, timeout-capable constructor of the managed Kubernetes
// controllers. No test sends a request to IBM Cloud: the constructor makes no call, and the tests that send requests
// point the client at an httptest server.

// testResourceGroupID is a synthetic resource group ID in the 32-hex-digit form IBM Cloud uses.
const testResourceGroupID = "0123456789abcdef0123456789abcdef"

func TestNewServiceWithAuthenticatorAndTimeout(t *testing.T) {
	validAuth := &core.BearerTokenAuthenticator{BearerToken: "test-token"}
	var typedNilAuth *core.IamAuthenticator

	testCases := []struct {
		name       string
		serviceURL string
		auth       core.Authenticator
		timeout    time.Duration
		wantErr    []string // substrings of the error; nil means success
		notInErr   []string // substrings the error must not contain (secrets)
	}{
		{
			name:       "public endpoint with the managed default timeout",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       validAuth,
			timeout:    30 * time.Second,
		},
		{
			name:       "private endpoint with a sub-second timeout",
			serviceURL: "https://private.resource-controller.cloud.ibm.com",
			auth:       validAuth,
			timeout:    250 * time.Millisecond,
		},
		{
			name:       "IAM API key authenticator",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       &core.IamAuthenticator{ApiKey: "test-api-key"},
			timeout:    30 * time.Second,
		},
		{
			// The scheme is the caller's policy; the managed client builder admits only https endpoints.
			name:       "plain http URL is the caller's choice",
			serviceURL: "http://127.0.0.1:8080",
			auth:       validAuth,
			timeout:    30 * time.Second,
		},
		{
			name:       "zero timeout is rejected",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       validAuth,
			timeout:    0,
			wantErr:    []string{"resource manager client request timeout is 0s", "pass a positive duration, for example 30s"},
		},
		{
			name:       "negative timeout is rejected",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       validAuth,
			timeout:    -time.Second,
			wantErr:    []string{"resource manager client request timeout is -1s"},
		},
		{
			name:       "nil authenticator is rejected",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       nil,
			timeout:    30 * time.Second,
			wantErr:    []string{"authenticator is nil", "never reads credentials from the environment"},
		},
		{
			name:       "typed nil authenticator is rejected",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       typedNilAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"authenticator is nil"},
		},
		{
			name:       "authenticator that fails its own validation",
			serviceURL: resourcemanagerv2.DefaultServiceURL,
			auth:       &core.BearerTokenAuthenticator{},
			timeout:    30 * time.Second,
			wantErr:    []string{"creating the Resource Manager client for " + resourcemanagerv2.DefaultServiceURL, "BearerToken"},
		},
		{
			name:       "empty URL is rejected instead of defaulted",
			serviceURL: "",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"resource manager service URL is empty", resourcemanagerv2.DefaultServiceURL},
		},
		{
			name:       "relative URL without scheme",
			serviceURL: "resource-controller.cloud.ibm.com",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{`"resource-controller.cloud.ibm.com" is not an absolute URL`},
		},
		{
			name:       "other scheme",
			serviceURL: "ftp://resource-controller.cloud.ibm.com",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"is not an absolute URL"},
		},
		{
			name:       "opaque https URL without host",
			serviceURL: "https:resource-controller.cloud.ibm.com",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"is not an absolute URL"},
		},
		{
			name:       "unparsable URL",
			serviceURL: "https://resource-controller.cloud.ibm.com:port",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"is not a valid URL", "invalid port"},
		},
		{ //nolint:gosec // G101: "hunter2" is a fake password; the case proves it is redacted from the error.
			name:       "user information in the URL is rejected and redacted",
			serviceURL: "https://user:hunter2@resource-controller.cloud.ibm.com",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"must not contain user information", "user:xxxxx@"},
			notInErr:   []string{"hunter2"},
		},
		{
			name:       "URL ending at a trailing slash",
			serviceURL: resourcemanagerv2.DefaultServiceURL + "/",
			auth:       validAuth,
			timeout:    30 * time.Second,
		},
		{
			// go-sdk-core would append /v2/resource_groups/<id> to the query, so every request would go to /.
			name:       "query in the URL is rejected and not printed",
			serviceURL: resourcemanagerv2.DefaultServiceURL + "?debug",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr: []string{"must end at the path", `after "` + resourcemanagerv2.DefaultServiceURL + `"`,
				resourcemanagerv2.DefaultServiceURL + " (the default"},
			notInErr: []string{"debug"},
		},
		{
			name:       "empty query in the URL is rejected",
			serviceURL: resourcemanagerv2.DefaultServiceURL + "?",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"must end at the path", `after "` + resourcemanagerv2.DefaultServiceURL + `"`},
		},
		{
			name:       "fragment in the URL is rejected and not printed",
			serviceURL: resourcemanagerv2.DefaultServiceURL + "#x",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"must end at the path", `after "` + resourcemanagerv2.DefaultServiceURL + `"`},
			notInErr:   []string{"#x"},
		},
		{
			// A bare "#" leaves no trace in the parsed URL but still swallows the operation path.
			name:       "empty fragment in the URL is rejected",
			serviceURL: resourcemanagerv2.DefaultServiceURL + "#",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"must end at the path", `after "` + resourcemanagerv2.DefaultServiceURL + `"`},
		},
		{
			// The scheme is checked before the query, so this error must not print the query either.
			name:       "query with a secret in a URL of another scheme is not printed",
			serviceURL: "ftp://resource-controller.cloud.ibm.com/?apikey=fake-query-key",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{`"ftp://resource-controller.cloud.ibm.com/" is not an absolute URL`},
			notInErr:   []string{"apikey", "fake-query-key"},
		},
		{
			name:       "fragment with a secret in a URL without host is not printed",
			serviceURL: "https:///#fake-fragment-token",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{`"https:///" is not an absolute URL`},
			notInErr:   []string{"fake-fragment-token"},
		},
		{ //nolint:gosec // G101: "hunter2" and the query key are fake; the case proves neither reaches the error.
			name:       "user information and a query with a secret are neither printed",
			serviceURL: "https://user:hunter2@resource-controller.cloud.ibm.com/?apikey=fake-query-key",
			auth:       validAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"must not contain user information", `"https://user:xxxxx@resource-controller.cloud.ibm.com/"`},
			notInErr:   []string{"hunter2", "apikey", "fake-query-key"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			svc, err := NewServiceWithAuthenticatorAndTimeout(tc.serviceURL, tc.auth, tc.timeout)
			if tc.wantErr != nil {
				g.Expect(err).To(HaveOccurred())
				g.Expect(svc).To(BeNil())
				for _, want := range tc.wantErr {
					g.Expect(err.Error()).To(ContainSubstring(want))
				}
				for _, secret := range tc.notInErr {
					g.Expect(err.Error()).ToNot(ContainSubstring(secret))
				}
				return
			}

			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(svc).To(BeAssignableToTypeOf(&Service{}))
			sdk := svc.(*Service).client
			g.Expect(sdk.GetServiceURL()).To(Equal(tc.serviceURL))
			g.Expect(sdk.Service.Options.Authenticator).To(BeIdenticalTo(tc.auth))

			httpClient := sdk.Service.GetHTTPClient()
			g.Expect(httpClient.Timeout).To(Equal(tc.timeout))
			// With SDK retries on, Service.Client would be a retrying wrapper around the installed client, and the
			// timeout would bound each attempt instead of the whole call.
			g.Expect(sdk.Service.Client).To(BeIdenticalTo(httpClient), "SDK retries stay off")
			// go-sdk-core's pooled transport and its minimum TLS version stay.
			transport, ok := httpClient.Transport.(*http.Transport)
			g.Expect(ok).To(BeTrue(), "the transport is go-sdk-core's *http.Transport, got %T", httpClient.Transport)
			g.Expect(transport.TLSClientConfig).ToNot(BeNil())
			g.Expect(transport.TLSClientConfig.MinVersion).To(BeNumerically("==", tls.VersionTLS12))
		})
	}

	t.Run("every service gets its own HTTP client", func(t *testing.T) {
		g := NewWithT(t)
		first, err := NewServiceWithAuthenticatorAndTimeout(resourcemanagerv2.DefaultServiceURL, validAuth, time.Second)
		g.Expect(err).ToNot(HaveOccurred())
		second, err := NewServiceWithAuthenticatorAndTimeout(resourcemanagerv2.DefaultServiceURL, validAuth, time.Minute)
		g.Expect(err).ToNot(HaveOccurred())

		firstClient := first.(*Service).client.Service.GetHTTPClient()
		secondClient := second.(*Service).client.Service.GetHTTPClient()
		g.Expect(firstClient).ToNot(BeIdenticalTo(secondClient))
		g.Expect(firstClient.Timeout).To(Equal(time.Second))
		g.Expect(secondClient.Timeout).To(Equal(time.Minute))
		// No process-wide default was changed: go-sdk-core still hands out clients without a timeout.
		g.Expect(core.DefaultHTTPClient().Timeout).To(BeZero())
	})
}

// TestPrintableURL covers the form in which URL errors print a rejected service URL: no password, query or fragment.
func TestPrintableURL(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "URL without anything to drop is unchanged",
			input: resourcemanagerv2.DefaultServiceURL,
			want:  resourcemanagerv2.DefaultServiceURL,
		},
		{
			name:  "query and fragment are dropped",
			input: "https://resource-controller.cloud.ibm.com/api?apikey=fake-query-key#fake-fragment",
			want:  "https://resource-controller.cloud.ibm.com/api",
		},
		{
			name:  "empty query and empty fragment are dropped",
			input: "https://resource-controller.cloud.ibm.com/api?#",
			want:  "https://resource-controller.cloud.ibm.com/api",
		},
		{ //nolint:gosec // G101: "hunter2" is a fake password; the case proves it is redacted.
			name:  "password is redacted and the user name kept",
			input: "https://user:hunter2@resource-controller.cloud.ibm.com?apikey=fake-query-key",
			want:  "https://user:xxxxx@resource-controller.cloud.ibm.com",
		},
		{
			name:  "opaque URL keeps its opaque part",
			input: "https:resource-controller.cloud.ibm.com?apikey=fake-query-key",
			want:  "https:resource-controller.cloud.ibm.com",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			parsed, err := url.Parse(tc.input)
			g.Expect(err).ToNot(HaveOccurred())
			before := *parsed

			g.Expect(printableURL(parsed)).To(Equal(tc.want))
			g.Expect(*parsed).To(Equal(before), "printableURL must not modify its argument")
		})
	}
}

// authorizationRecorder is an httptest handler that records the Authorization header of every request and answers
// with a resource group.
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
	_, _ = w.Write([]byte(`{"id":"` + testResourceGroupID + `","name":"test-group"}`))
}

// Headers returns the Authorization headers received so far, in order.
func (r *authorizationRecorder) Headers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.headers...)
}

// TestNewServiceWithAuthenticatorAndTimeoutIgnoresEnvironment proves that the constructor never falls back to the
// environment, which the managed Kubernetes controllers must never read credentials from: with an environment from
// which NewService builds a working client, a nil authenticator is still an error, and an explicit one signs every
// request. It cannot run in parallel (t.Setenv).
func TestNewServiceWithAuthenticatorAndTimeoutIgnoresEnvironment(t *testing.T) {
	t.Run("nil authenticator never falls back to the environment", func(t *testing.T) {
		g := NewWithT(t)

		// HOME points at an empty directory so that go-sdk-core's credentials-file fallback, ~/ibm-credentials.env, can
		// neither mask these values nor read a developer's real credentials file.
		t.Setenv("HOME", t.TempDir())
		t.Setenv("IBM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "does-not-exist.env"))
		t.Setenv("IBMCLOUD_AUTH_TYPE", "bearerToken")
		t.Setenv("IBMCLOUD_BEARER_TOKEN", "environment-token")

		recorder := &authorizationRecorder{}
		server := httptest.NewServer(recorder)
		t.Cleanup(server.Close)
		getOptions := &resourcemanagerv2.GetResourceGroupOptions{ID: core.StringPtr(testResourceGroupID)}

		// Precondition: the environment really yields credentials, so the environment-reading NewService builds a client
		// that signs with them. Without it this test could pass merely because the environment held nothing usable.
		fromEnvironment, err := NewService(&resourcemanagerv2.ResourceManagerV2Options{URL: server.URL})
		g.Expect(err).ToNot(HaveOccurred())
		_, _, err = fromEnvironment.GetResourceGroup(getOptions)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(recorder.Headers()).To(Equal([]string{"Bearer environment-token"}))

		for _, nilAuth := range []core.Authenticator{nil, (*core.BearerTokenAuthenticator)(nil)} {
			svc, err := NewServiceWithAuthenticatorAndTimeout(server.URL, nilAuth, time.Minute)
			g.Expect(err).To(MatchError(ContainSubstring("authenticator is nil")))
			g.Expect(svc).To(BeNil())
		}

		explicit, err := NewServiceWithAuthenticatorAndTimeout(server.URL, &core.BearerTokenAuthenticator{BearerToken: "explicit-token"}, time.Minute)
		g.Expect(err).ToNot(HaveOccurred())
		_, _, err = explicit.GetResourceGroup(getOptions)
		g.Expect(err).ToNot(HaveOccurred())
		// The rejected constructions sent nothing; the explicit client signed with its own token only.
		g.Expect(recorder.Headers()).To(Equal([]string{"Bearer environment-token", "Bearer explicit-token"}))
	})
}

// TestNewServiceWithAuthenticatorAndTimeoutBoundsHungResponse proves the point of the timeout: against a Resource
// Manager endpoint that accepts a request and never answers, a call fails soon after the timeout instead of blocking
// its caller for good.
func TestNewServiceWithAuthenticatorAndTimeoutBoundsHungResponse(t *testing.T) {
	g := NewWithT(t)
	const timeout = 200 * time.Millisecond
	// margin is generous, so that a loaded machine or the race detector cannot fail the test. Without the timeout the
	// call would block until the handler is released when the test ends.
	const margin = 5 * time.Second

	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		// Never answer: wait until the client gives up (its connection closes) or the test ends.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	// Cleanups run last-in first-out: the handler is released before Close, which waits for running handlers.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	svc, err := NewServiceWithAuthenticatorAndTimeout(server.URL, &core.BearerTokenAuthenticator{BearerToken: "explicit-token"}, timeout)
	g.Expect(err).ToNot(HaveOccurred())

	start := time.Now()
	_, _, err = svc.GetResourceGroup(&resourcemanagerv2.GetResourceGroupOptions{ID: core.StringPtr(testResourceGroupID)})
	elapsed := time.Since(start)

	g.Expect(err).To(MatchError(ContainSubstring("Client.Timeout exceeded")))
	g.Expect(arrived).To(Receive(), "the request reached the server, so the call waited for a response that never came")
	g.Expect(elapsed).To(BeNumerically(">=", timeout))
	g.Expect(elapsed).To(BeNumerically("<", timeout+margin))
}
