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

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/IBM/go-sdk-core/v5/core"
	"golang.org/x/time/rate"

	. "github.com/onsi/gomega"
)

// limiterCall is one KubernetesService call of the limiter tests, with the answer the fake server gives it.
type limiterCall struct {
	name   string
	answer http.HandlerFunc
	call   func(ctx context.Context, s *Service) error
}

// limiterCalls returns one call per KubernetesService method.
func limiterCalls(t *testing.T) []limiterCall {
	t.Helper()
	return []limiterCall{
		{
			name:   "GetCluster",
			answer: reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.GetCluster(ctx, testIKSClusterID, "")
				return err
			},
		},
		{
			name:   "CreateCluster",
			answer: reply(http.StatusCreated, fixture(t, "createCluster-201-non-critical-errors.json")),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.CreateCluster(ctx, iksCreateRequest())
				return err
			},
		},
		{
			name:   "ListVersions",
			answer: reply(http.StatusOK, fixture(t, "versions.json")),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.ListVersions(ctx)
				return err
			},
		},
		{
			name:   "DeleteCluster",
			answer: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, s *Service) error {
				return s.DeleteCluster(ctx, testIKSClusterID, "")
			},
		},
		{
			// The compat path that bypasses BaseService.Request still waits on the limiter.
			name:   "GetAdminKubeconfig",
			answer: reply(http.StatusOK, synthesizeKubeconfig(t, "iks-kubeconfig-shape.json", newTestCredentials(t), kubeconfigOptions{})),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.GetAdminKubeconfig(ctx, testIKSClusterID)
				return err
			},
		},
		{
			name:   "GetCACertificate",
			answer: reply(http.StatusOK, []byte(`{"caCert":"`+base64.StdEncoding.EncodeToString(newTestCredentials(t).caPEM)+`","rotatedAt":""}`)),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.GetCACertificate(ctx, testIKSClusterID)
				return err
			},
		},
		{
			name:   "ListWorkerPools",
			answer: reply(http.StatusOK, fixture(t, "iks-getWorkerPools-normal.json")),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.ListWorkerPools(ctx, testIKSClusterID)
				return err
			},
		},
		{
			name:   "CreateWorkerPool",
			answer: reply(http.StatusCreated, []byte(`{"workerPoolID":"p1"}`)),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.CreateWorkerPool(ctx, iksCreateWorkerPoolRequest())
				return err
			},
		},
		{
			name:   "ListWorkers",
			answer: reply(http.StatusOK, fixture(t, "iks-getWorkers-normal.json")),
			call: func(ctx context.Context, s *Service) error {
				_, err := s.ListWorkers(ctx, testIKSClusterID)
				return err
			},
		},
		{
			name:   "ReplaceWorker",
			answer: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, s *Service) error {
				return s.ReplaceWorker(ctx, testIKSClusterID, "w1", true)
			},
		},
	}
}

// TestLimiterWaitsOncePerRequest proves that every method's request waits on the injected limiter exactly once.
func TestLimiterWaitsOncePerRequest(t *testing.T) {
	for _, tc := range limiterCalls(t) {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, tc.answer)
			service, limiter := newTestService(t, fake)
			g.Expect(tc.call(context.Background(), service)).To(Succeed())
			g.Expect(fake.received()).To(HaveLen(1))
			g.Expect(limiter.count()).To(Equal(1))
		})
	}
}

// TestCancelledContextStopsAtTheLimiter uses a real *rate.Limiter: Wait returns the context's error before it
// takes a token, so the request never reaches the server. Deterministic, because the context is cancelled first.
func TestCancelledContextStopsAtTheLimiter(t *testing.T) {
	for _, tc := range limiterCalls(t) {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, tc.answer)
			opts := testOptions(t, fake, rate.NewLimiter(rate.Limit(10), 20))
			service, err := newService(opts, fake.server.Client().Transport)
			g.Expect(err).ToNot(HaveOccurred())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = tc.call(ctx, service)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring("the request was not sent"))
			g.Expect(Classify(Operation(tc.name), err)).To(Equal(ClassTransient))
			g.Expect(fake.received()).To(BeEmpty())
		})
	}
}

// TestUserAgent proves that IBM receives CAPIBM's User-Agent, not the SDK's hard-coded "go-sdk-template".
func TestUserAgent(t *testing.T) {
	t.Run("user agent on the SDK path", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")))
		service, _ := newTestService(t, fake)
		_, err := service.GetCluster(context.Background(), testIKSClusterID, "")
		g.Expect(err).ToNot(HaveOccurred())
		userAgent := fake.last(t).Header.Values("User-Agent")
		g.Expect(userAgent).To(Equal([]string{testUserAgent}))
	})

	t.Run("an empty UserAgent sends DefaultUserAgent", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "versions.json")))
		opts := testOptions(t, fake, &countingLimiter{})
		opts.UserAgent = ""
		service, err := newService(opts, fake.server.Client().Transport)
		g.Expect(err).ToNot(HaveOccurred())
		_, err = service.ListVersions(context.Background())
		g.Expect(err).ToNot(HaveOccurred())
		userAgent := fake.last(t).Header.Get("User-Agent")
		g.Expect(userAgent).To(Equal(DefaultUserAgent))
		g.Expect(strings.Contains(userAgent, "go-sdk-template")).To(BeFalse())
	})
}

// TestCredentialsRejectedBeforeSending covers an authenticator whose IAM token request fails. go-sdk-core then returns
// IAM's answer as the DetailedResponse; it must not be mistaken for an answer of IBM Cloud Kubernetes Service.
func TestCredentialsRejectedBeforeSending(t *testing.T) {
	testCases := []struct {
		name      string
		iamStatus int
		expected  ErrorClass
		rejected  bool
	}{
		{name: "IAM 400 (unknown API key) is Unauthorized, not BadRequest", iamStatus: http.StatusBadRequest, expected: ClassUnauthorized, rejected: true},
		{name: "IAM 401 is Unauthorized", iamStatus: http.StatusUnauthorized, expected: ClassUnauthorized, rejected: true},
		{name: "IAM 503 is Transient", iamStatus: http.StatusServiceUnavailable, expected: ClassTransient, rejected: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			var iamCalls atomic.Int32
			iam := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				iamCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.iamStatus)
				_, _ = w.Write([]byte(`{"errorCode":"BXNIM0415E","errorMessage":"Provided API key could not be found."}`))
			}))
			t.Cleanup(iam.Close)
			authenticator, err := core.NewIamAuthenticatorBuilder().
				SetApiKey("synthetic-api-key").
				SetURL(iam.URL).
				SetClient(iam.Client()).
				Build()
			g.Expect(err).ToNot(HaveOccurred())

			fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")))
			limiter := &countingLimiter{}
			opts := testOptions(t, fake, limiter)
			opts.Authenticator = authenticator
			service, err := newService(opts, fake.server.Client().Transport)
			g.Expect(err).ToNot(HaveOccurred())

			_, err = service.GetCluster(context.Background(), testIKSClusterID, "")
			g.Expect(Classify(OpGetCluster, err)).To(Equal(tc.expected))
			g.Expect(errors.Is(err, ErrCredentialsRejected)).To(Equal(tc.rejected))
			var apiErr *APIError
			g.Expect(errors.As(err, &apiErr)).To(BeFalse(), "an IAM answer is never an APIError")
			g.Expect(iamCalls.Load()).To(BeNumerically(">=", 1))
			g.Expect(fake.received()).To(BeEmpty())
			g.Expect(limiter.count()).To(BeZero())
		})
	}
}

func TestLimitedTransportClosesTheBodyWhenTheWaitFails(t *testing.T) {
	g := NewWithT(t)
	body := &closeRecorder{}
	transport := &limitedTransport{limiter: &countingLimiter{}, next: http.DefaultTransport}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://containers.invalid/global", body)
	g.Expect(err).ToNot(HaveOccurred())
	response, err := transport.RoundTrip(request) //nolint:bodyclose // no response is returned when the wait fails
	g.Expect(response).To(BeNil())
	g.Expect(err).To(MatchError(context.Canceled))
	g.Expect(body.closed).To(BeTrue())
}

// closeRecorder is a request body that records Close.
type closeRecorder struct {
	closed bool
}

// Read reports the end of the body.
func (c *closeRecorder) Read([]byte) (int, error) {
	return 0, errors.New("closeRecorder has no content")
}

// Close records the call.
func (c *closeRecorder) Close() error {
	c.closed = true
	return nil
}
