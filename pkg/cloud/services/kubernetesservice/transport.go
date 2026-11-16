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
	"crypto/tls"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// RequestLimiter is waited on before every IBM Cloud Kubernetes Service request. *rate.Limiter implements it. It is an
// interface so that the client builder can inject the one process-wide limiter per client and tests can count waits.
type RequestLimiter interface {
	// Wait blocks until one more request may be sent. It returns an error, and the request is not sent, when ctx
	// (the request's context) ends first or the request can never be admitted.
	Wait(ctx context.Context) error
}

// limitedTransport waits on the limiter before every request. It sits below go-sdk-core's BaseService.Request and
// below the compat path that bypasses it, so each HTTP request of the client is counted exactly once: IBM allows 200
// requests per 10 s per source IP, and one limiter is shared by every client.
type limitedTransport struct {
	limiter RequestLimiter
	next    http.RoundTripper
}

// RoundTrip implements http.RoundTripper. req is the outgoing request; its context bounds the limiter wait.
func (t *limitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.limiter.Wait(req.Context()); err != nil {
		// The http.RoundTripper contract requires closing the body even when no request is sent.
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("waiting for the IBM Cloud Kubernetes Service request limiter: %w", err)
	}
	markSent(req.Context())
	return t.next.RoundTrip(req)
}

// newHTTPClient returns the http.Client every request of one Service goes through.
//
// Parameters:
//   - timeout bounds each request, including the limiter wait (http.Client cancels the request context).
//   - limiter is waited on before every request.
//   - base is the transport that talks to the network.
func newHTTPClient(timeout time.Duration, limiter RequestLimiter, base http.RoundTripper) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: &limitedTransport{limiter: limiter, next: base},
	}
}

// newBaseTransport returns the production network transport: net/http's defaults (connection pooling, proxy
// variables, HTTP/2) with TLS 1.2 as the minimum, the floor go-sdk-core applies to its own clients
// (base_service.go setMinimumTLSVersion), which BaseService.SetHTTPClient cannot apply to a wrapping transport.
func newBaseTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return transport
}

// callTracker records whether a request of one call passed the limiter and left for IBM Cloud Kubernetes Service.
//
// It exists because go-sdk-core returns the IAM token service's response as the call's DetailedResponse when the
// authenticator fails (go-sdk-core v5.25.3 base_service.go:390-395), before anything is sent. Without the tracker a
// rejected API key (IAM answers 400) would look like an IKS 400 and be classified as a bad request.
type callTracker struct {
	sent atomic.Bool
}

// callTrackerKey is the context key of the callTracker of one call.
type callTrackerKey struct{}

// trackCall returns a context that carries a new callTracker, and the tracker. ctx is the caller's context.
func trackCall(ctx context.Context) (context.Context, *callTracker) {
	tracker := &callTracker{}
	return context.WithValue(ctx, callTrackerKey{}, tracker), tracker
}

// markSent marks the call of ctx as sent. ctx is the outgoing request's context; a context without a tracker is
// ignored.
func markSent(ctx context.Context) {
	if tracker, ok := ctx.Value(callTrackerKey{}).(*callTracker); ok {
		tracker.sent.Store(true)
	}
}
