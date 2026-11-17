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

package vpc

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/vpc-go-sdk/vpcv1"

	. "github.com/onsi/gomega"
)

const testVPCURL = "https://us-south.iaas.cloud.ibm.com/v1"

// TestNewServiceWithAuthenticator covers the input validation of the explicit-credentials constructor. It makes no
// network call: go-sdk-core only validates the authenticator at construction time.
func TestNewServiceWithAuthenticator(t *testing.T) {
	validAuth := &core.BearerTokenAuthenticator{BearerToken: "test-token"}
	var typedNilAuth *core.IamAuthenticator

	testCases := []struct {
		name       string
		serviceURL string
		auth       core.Authenticator
		wantErr    []string // substrings of the error; nil means success
		// wantURL is the URL the SDK client ends up with; "" means serviceURL.
		wantURL string
	}{
		{
			name:       "regional public endpoint",
			serviceURL: testVPCURL,
			auth:       validAuth,
		},
		{
			name:       "regional private endpoint",
			serviceURL: "https://us-south.private.iaas.cloud.ibm.com/v1",
			auth:       validAuth,
		},
		{
			name:       "IAM API key authenticator",
			serviceURL: testVPCURL,
			auth:       &core.IamAuthenticator{ApiKey: "test-api-key"},
		},
		{
			name:       "nil authenticator is rejected",
			serviceURL: testVPCURL,
			auth:       nil,
			wantErr:    []string{"authenticator is nil", "never reads credentials from the environment"},
		},
		{
			name:       "typed nil authenticator is rejected",
			serviceURL: testVPCURL,
			auth:       typedNilAuth,
			wantErr:    []string{"authenticator is nil"},
		},
		{
			name:       "authenticator that fails its own validation",
			serviceURL: testVPCURL,
			auth:       &core.BearerTokenAuthenticator{},
			wantErr:    []string{"creating the VPC client for " + testVPCURL, "BearerToken"},
		},
		{
			name:       "an empty URL selects the SDK's default",
			serviceURL: "",
			auth:       validAuth,
			wantURL:    vpcv1.DefaultServiceURL,
		},
		{
			// No scheme or form rule of its own: the URL goes to the SDK as it is, as with NewService.
			name:       "a plain-http URL goes to the SDK as it is",
			serviceURL: "http://127.0.0.1:8080/v1",
			auth:       validAuth,
		},
		{
			// go-sdk-core refuses a URL in braces or quotes; the error names the URL and keeps the SDK's reason.
			name:       "a URL the SDK refuses is reported with the SDK's reason",
			serviceURL: "{" + testVPCURL + "}",
			auth:       validAuth,
			wantErr:    []string{"creating the VPC client for {" + testVPCURL + "}", "The URL property is invalid"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			svc, err := NewServiceWithAuthenticator(tc.serviceURL, tc.auth)
			if tc.wantErr != nil {
				g.Expect(err).To(HaveOccurred())
				g.Expect(svc).To(BeNil())
				for _, want := range tc.wantErr {
					g.Expect(err.Error()).To(ContainSubstring(want))
				}
				return
			}

			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(svc).To(BeAssignableToTypeOf(&Service{}))
			sdk := svc.(*Service).vpcService
			wantURL := tc.wantURL
			if wantURL == "" {
				wantURL = tc.serviceURL
			}
			g.Expect(sdk.GetServiceURL()).To(Equal(wantURL))
			g.Expect(sdk.Service.Options.Authenticator).To(BeIdenticalTo(tc.auth))
		})
	}
}

// TestNewServiceWithAuthenticatorIgnoresEnvironment proves that the constructor and the client it returns never read
// credentials or endpoints from the environment, which the managed Kubernetes controllers rely on: with every variable
// that go-sdk-core's environment path consults set to a value that breaks that path, the client still succeeds and
// sends exactly the passed token to exactly the passed URL. It cannot run in parallel (t.Setenv).
func TestNewServiceWithAuthenticatorIgnoresEnvironment(t *testing.T) {
	t.Run("bogus IBMCLOUD environment is never read", func(t *testing.T) {
		g := NewWithT(t)

		// HOME points at an empty directory so that go-sdk-core's last fallback, ~/ibm-credentials.env, can neither
		// mask the bogus values nor read a developer's real credentials file.
		t.Setenv("HOME", t.TempDir())
		t.Setenv("IBM_CREDENTIALS_FILE", "/nonexistent/ibm-credentials.env")
		t.Setenv("IBMCLOUD_AUTH_TYPE", "bogus")
		t.Setenv("IBMCLOUD_APIKEY", "bogus-api-key-from-environment")
		t.Setenv("VPC_AUTH_TYPE", "bogus")
		t.Setenv("VPC_URL", "https://vpc-url-from-environment.invalid/v1")

		// Precondition: the environment really is poisoned, so the environment-reading NewService fails. Without it this
		// test could pass merely because nothing in the environment mattered.
		_, err := NewService(testVPCURL)
		g.Expect(err).To(MatchError(ContainSubstring("bogus")))

		type seenRequest struct {
			authorization string
			path          string
		}
		seen := make(chan seenRequest, 1)
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen <- seenRequest{authorization: r.Header.Get("Authorization"), path: r.URL.Path}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"vpcs": []}`))
		}))
		defer server.Close()

		svc, err := NewServiceWithAuthenticator(server.URL+"/v1", &core.BearerTokenAuthenticator{BearerToken: "explicit-token"})
		g.Expect(err).ToNot(HaveOccurred())
		// Trust the test server's self-signed certificate; this replaces only the HTTP client, never the authenticator
		// or the URL under test.
		svc.(*Service).vpcService.Service.SetHTTPClient(server.Client())

		_, _, err = svc.ListVpcs(&vpcv1.ListVpcsOptions{})
		g.Expect(err).ToNot(HaveOccurred())

		var req seenRequest
		g.Expect(seen).To(Receive(&req))
		g.Expect(req.authorization).To(Equal("Bearer explicit-token"))
		g.Expect(req.path).To(Equal("/v1/vpcs"))
	})
}

// TestNewServiceWithAuthenticatorAndTimeout covers the validation of the timeout-capable constructor and the HTTP
// client it installs. It makes no network call.
func TestNewServiceWithAuthenticatorAndTimeout(t *testing.T) {
	validAuth := &core.BearerTokenAuthenticator{BearerToken: "test-token"}
	var typedNilAuth *core.IamAuthenticator

	testCases := []struct {
		name       string
		serviceURL string
		auth       core.Authenticator
		timeout    time.Duration
		wantErr    []string // substrings of the error; nil means success
	}{
		{
			name:       "the managed default timeout is installed",
			serviceURL: testVPCURL,
			auth:       validAuth,
			timeout:    30 * time.Second,
		},
		{
			name:       "a sub-second timeout is kept exactly",
			serviceURL: testVPCURL,
			auth:       validAuth,
			timeout:    250 * time.Millisecond,
		},
		{
			name:       "zero timeout is rejected",
			serviceURL: testVPCURL,
			auth:       validAuth,
			timeout:    0,
			wantErr:    []string{"VPC client request timeout is 0s", "pass a positive duration, for example 30s"},
		},
		{
			name:       "negative timeout is rejected",
			serviceURL: testVPCURL,
			auth:       validAuth,
			timeout:    -time.Second,
			wantErr:    []string{"VPC client request timeout is -1s"},
		},
		{
			name:       "nil authenticator is rejected",
			serviceURL: testVPCURL,
			auth:       nil,
			timeout:    30 * time.Second,
			wantErr:    []string{"authenticator is nil", "never reads credentials from the environment"},
		},
		{
			name:       "typed nil authenticator is rejected",
			serviceURL: testVPCURL,
			auth:       typedNilAuth,
			timeout:    30 * time.Second,
			wantErr:    []string{"authenticator is nil"},
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
				return
			}

			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(svc).To(BeAssignableToTypeOf(&Service{}))
			sdk := svc.(*Service).vpcService
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
		first, err := NewServiceWithAuthenticatorAndTimeout(testVPCURL, validAuth, time.Second)
		g.Expect(err).ToNot(HaveOccurred())
		second, err := NewServiceWithAuthenticatorAndTimeout(testVPCURL, validAuth, time.Minute)
		g.Expect(err).ToNot(HaveOccurred())

		firstClient := first.(*Service).vpcService.Service.GetHTTPClient()
		secondClient := second.(*Service).vpcService.Service.GetHTTPClient()
		g.Expect(firstClient).ToNot(BeIdenticalTo(secondClient))
		g.Expect(firstClient.Timeout).To(Equal(time.Second))
		g.Expect(secondClient.Timeout).To(Equal(time.Minute))
		// No process-wide default was changed: go-sdk-core still hands out clients without a timeout.
		g.Expect(core.DefaultHTTPClient().Timeout).To(BeZero())
	})
}

// TestNewServiceWithAuthenticatorAndTimeoutBoundsHungResponse proves the point of the timeout: against a VPC endpoint
// that accepts a request and never answers, a call fails soon after the timeout instead of blocking its caller for
// good.
func TestNewServiceWithAuthenticatorAndTimeoutBoundsHungResponse(t *testing.T) {
	g := NewWithT(t)
	const timeout = 200 * time.Millisecond
	// margin is generous, so that a loaded machine or the race detector cannot fail the test. Without the timeout the
	// call would block until the handler is released when the test ends.
	const margin = 5 * time.Second

	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
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

	svc, err := NewServiceWithAuthenticatorAndTimeout(server.URL+"/v1", &core.BearerTokenAuthenticator{BearerToken: "explicit-token"}, timeout)
	g.Expect(err).ToNot(HaveOccurred())
	// Trust the test server's self-signed certificate in the transport the constructor installed. The http.Client,
	// and with it the Timeout under test, stays the constructor's.
	transport, ok := svc.(*Service).vpcService.Service.GetHTTPClient().Transport.(*http.Transport)
	g.Expect(ok).To(BeTrue())
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport.TLSClientConfig.RootCAs = roots

	start := time.Now()
	_, _, err = svc.GetVPC(&vpcv1.GetVPCOptions{ID: core.StringPtr("r006-00000000-0000-0000-0000-000000000000")})
	elapsed := time.Since(start)

	g.Expect(err).To(MatchError(ContainSubstring("Client.Timeout exceeded")))
	g.Expect(arrived).To(Receive(), "the request reached the server, so the call waited for a response that never came")
	g.Expect(elapsed).To(BeNumerically(">=", timeout))
	g.Expect(elapsed).To(BeNumerically("<", timeout+margin))
}
