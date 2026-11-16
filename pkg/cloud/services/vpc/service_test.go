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
	"net/http"
	"net/http/httptest"
	"testing"

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
		notInErr   string   // a substring the error must not contain (secrets)
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
			name:       "empty URL",
			serviceURL: "",
			auth:       validAuth,
			wantErr:    []string{"VPC service URL is empty", "https://us-south.iaas.cloud.ibm.com/v1"},
		},
		{
			name:       "plain http URL",
			serviceURL: "http://us-south.iaas.cloud.ibm.com/v1",
			auth:       validAuth,
			wantErr:    []string{`"http://us-south.iaas.cloud.ibm.com/v1" is not an absolute https URL`},
		},
		{
			name:       "relative URL without scheme",
			serviceURL: "us-south.iaas.cloud.ibm.com/v1",
			auth:       validAuth,
			wantErr:    []string{"is not an absolute https URL"},
		},
		{
			name:       "https scheme without host",
			serviceURL: "https:///v1",
			auth:       validAuth,
			wantErr:    []string{"is not an absolute https URL"},
		},
		{
			name:       "opaque https URL without host",
			serviceURL: "https:us-south.iaas.cloud.ibm.com",
			auth:       validAuth,
			wantErr:    []string{"is not an absolute https URL"},
		},
		{
			name:       "unparsable URL",
			serviceURL: "https://us-south.iaas.cloud.ibm.com:port/v1",
			auth:       validAuth,
			wantErr:    []string{"is not a valid URL", "invalid port"},
		},
		{
			// go-sdk-core would append /vpcs to the query, so every request would go to /v1.
			name:       "query in the URL is rejected and not printed",
			serviceURL: testVPCURL + "?debug",
			auth:       validAuth,
			wantErr:    []string{"must end at the API version path", `after "` + testVPCURL + `"`, testVPCURL + " (the default"},
			notInErr:   "debug",
		},
		{
			name:       "empty query in the URL is rejected",
			serviceURL: testVPCURL + "?",
			auth:       validAuth,
			wantErr:    []string{"must end at the API version path", `after "` + testVPCURL + `"`},
		},
		{
			name:       "fragment in the URL is rejected and not printed",
			serviceURL: testVPCURL + "#x",
			auth:       validAuth,
			wantErr:    []string{"must end at the API version path", `after "` + testVPCURL + `"`},
			notInErr:   "#x",
		},
		{
			// A bare "#" leaves no trace in the parsed URL but still swallows the operation path.
			name:       "empty fragment in the URL is rejected",
			serviceURL: testVPCURL + "#",
			auth:       validAuth,
			wantErr:    []string{"must end at the API version path", `after "` + testVPCURL + `"`},
		},
		{ //nolint:gosec // G101: "hunter2" is a fake password; the case proves it is redacted from the error.
			name:       "user information in the URL is rejected and redacted",
			serviceURL: "https://user:hunter2@us-south.iaas.cloud.ibm.com/v1",
			auth:       validAuth,
			wantErr:    []string{"must not contain user information", "user:xxxxx@"},
			notInErr:   "hunter2",
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
				if tc.notInErr != "" {
					g.Expect(err.Error()).ToNot(ContainSubstring(tc.notInErr))
				}
				return
			}

			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(svc).To(BeAssignableToTypeOf(&Service{}))
			sdk := svc.(*Service).vpcService
			g.Expect(sdk.GetServiceURL()).To(Equal(tc.serviceURL))
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
