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

// Fixture provenance.
//
// The JSON files in testdata/ are answers of the IBM Cloud Kubernetes Service API captured by authenticated live
// probes on 2026-11-16 (IKS 1.36 and ROKS 4.21 on VPC Gen2, region eu-de). Before they were copied here they were
// scrubbed: every account, resource-group, cluster, VPC and subnet ID was replaced by a synthetic placeholder (cluster
// IDs iksclusterid00000001 and roksclusterid0000002, IDs of the form r010-00000000-0000-4000-8000-0000000000NN and
// 02b7-00000000-0000-4000-8000-0000000000NN; the account and resource group were placeholders already), and the
// OpenShift OAuth server field of getCluster was removed because CAPIBM never uses the OpenShift OAuth server: IBM's
// admin client certificate is its only credential. Everything else is as IBM sent it.
//
//   - createCluster-201-non-critical-errors.json: a 201 from POST /v2/vpc/createCluster that carries
//     non_critical_errors with code E49df (IBM answered so on both IKS and ROKS creates).
//   - error-P4102-subnet-not-found.json: the 400 of createCluster with a nonexistent subnet.
//   - error-E7278-roks-no-cos.json: the 400 of a ROKS createCluster without cosInstanceCRN.
//   - {iks,roks}-getCluster-{deploying,deployed,pending,normal}.json: GET /v2/vpc/getCluster in each state of the
//     create sequence, sampled every 60 s.
//   - versions.json: the anonymous versions list of 2026-11-16 (the same ReleaseVersion schema that /v1/versions
//     returns, a map of "kubernetes" and "openshift" lists).
//   - {iks,roks}-kubeconfig-shape.json: what the probes recorded about getKubeconfig?admin=true&format=json and
//     getCACert: the server, whether CA data is present, the user's keys, the admin certificate's subject shape
//     and validity, and the getCACert keys. They hold no key material; the probe's OpenShift OAuth block was removed
//     and the certificate fingerprint replaced by a placeholder. The tests synthesize the kubeconfig bodies from
//     these shapes with certificates generated in the test (compat_test.go).
//   - {iks,roks}-getWorkerPools-{deploying,deployed,pending,normal}.json: GET /v2/vpc/getWorkerPools in each create
//     state, an array. ListWorkerPools calls the SDK's /v2/getWorkerPools, whose answer on a VPC cluster is not yet
//     verified against IBM Cloud; until it is captured, the tests serve these /v2/vpc bodies on it.
//   - {iks,roks}-getWorkers-{deploying,deployed,pending,normal}.json: GET /v2/vpc/getWorkers in each create state,
//     with kubeVersion{desired, actual, target}; roks-getWorkers-deploying.json is IBM's empty list.
//   - error-E1437-{iks,roks}-resize.json: the 400 E1437 of a resize below the cluster minimum (IKS 1, ROKS 2 workers);
//     removing the only pool answers the same code and text, synchronously and without side effects.
//   - golden/createWorkerPool.json: not a capture; the body CreateWorkerPool must send for
//     iksCreateWorkerPoolRequest (zones with subnetID, no operatingSystem).

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
)

const (
	// testToken is the bearer token of the tests' authenticator.
	testToken = "test-token"
	// testUserAgent is the User-Agent the tests configure.
	testUserAgent = "cluster-api-provider-ibmcloud/v0.0.0-test"
	// testResourceGroupID is a synthetic resource group ID.
	testResourceGroupID = "0123456789abcdef0123456789abcdef"
	// testIKSClusterID is the scrubbed IKS cluster ID of the fixtures.
	testIKSClusterID = "iksclusterid00000001"
	// testROKSClusterID is the scrubbed ROKS cluster ID of the fixtures.
	testROKSClusterID = "roksclusterid0000002"
)

// fixture returns the content of testdata/name and fails the test when it cannot be read. os.DirFS confines name to
// testdata.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := fs.ReadFile(os.DirFS("testdata"), name)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return data
}

// recordedRequest is what the fake IBM Cloud Kubernetes Service received.
type recordedRequest struct {
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Body     []byte
}

// fakeIKS is an httptest TLS server that records every request and answers with its current handler.
type fakeIKS struct {
	server *httptest.Server

	mu       sync.Mutex
	requests []recordedRequest
	handler  http.HandlerFunc
}

// newFakeIKS starts a fake IBM Cloud Kubernetes Service that answers with handler and stops with the test.
func newFakeIKS(t *testing.T, handler http.HandlerFunc) *fakeIKS {
	t.Helper()
	fake := &fakeIKS{handler: handler}
	fake.server = httptest.NewTLSServer(http.HandlerFunc(fake.serve))
	t.Cleanup(fake.server.Close)
	return fake
}

// serve records r and hands it to the current handler.
func (f *fakeIKS) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Header:   r.Header.Clone(),
		Body:     body,
	})
	handler := f.handler
	f.mu.Unlock()
	handler(w, r)
}

// received returns a copy of the recorded requests.
func (f *fakeIKS) received() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

// last returns the last recorded request and fails the test when there is none.
func (f *fakeIKS) last(t *testing.T) recordedRequest {
	t.Helper()
	requests := f.received()
	if len(requests) == 0 {
		t.Fatal("the fake IBM Cloud Kubernetes Service received no request")
	}
	return requests[len(requests)-1]
}

// reply returns a handler that answers with status and a JSON body.
func reply(status int, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

// replyWithHeaders returns a handler that answers with status, extra headers and a JSON body.
func replyWithHeaders(status int, headers map[string]string, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for name, value := range headers {
			w.Header().Set(name, value)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

// countingLimiter is a RequestLimiter that counts its waits and never delays.
type countingLimiter struct {
	mu    sync.Mutex
	waits int
}

// Wait counts the wait and fails only when ctx has ended, like *rate.Limiter.
func (l *countingLimiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	l.waits++
	l.mu.Unlock()
	return ctx.Err()
}

// count returns the number of waits so far.
func (l *countingLimiter) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waits
}

// testOptions returns valid ServiceOptions for the fake server, with a bearer-token authenticator and limiter.
func testOptions(t *testing.T, fake *fakeIKS, limiter RequestLimiter) ServiceOptions {
	t.Helper()
	authenticator, err := core.NewBearerTokenAuthenticator(testToken)
	if err != nil {
		t.Fatalf("building the test authenticator: %v", err)
	}
	return ServiceOptions{
		URL:           fake.server.URL + "/global",
		Authenticator: authenticator,
		RateLimiter:   limiter,
		HTTPTimeout:   10 * time.Second,
		UserAgent:     testUserAgent,
	}
}

// newTestService returns a Service that talks to fake through the fake's TLS transport, and its counting limiter.
func newTestService(t *testing.T, fake *fakeIKS) (*Service, *countingLimiter) {
	t.Helper()
	limiter := &countingLimiter{}
	service, err := newService(testOptions(t, fake, limiter), fake.server.Client().Transport)
	if err != nil {
		t.Fatalf("building the test service: %v", err)
	}
	return service, limiter
}
