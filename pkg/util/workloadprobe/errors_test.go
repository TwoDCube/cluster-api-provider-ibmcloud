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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"

	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// timeoutError is a net.Error that reports a timeout.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	inURL := func(err error) error { return &url.Error{Op: "Post", URL: "https://c100.example:31000/apis", Err: err} }
	ssr := schema.GroupResource{Group: "authentication.k8s.io", Resource: "selfsubjectreviews"}

	testCases := []struct {
		name string
		err  error
		want FailureReason
	}{
		{name: "certificate verification", err: inURL(&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}), want: FailureTLS},
		{name: "unknown authority", err: inURL(x509.UnknownAuthorityError{}), want: FailureTLS},
		{name: "host name mismatch", err: inURL(x509.HostnameError{Host: "c100.example"}), want: FailureTLS},
		{name: "expired certificate", err: inURL(x509.CertificateInvalidError{Reason: x509.Expired}), want: FailureTLS},
		{name: "401", err: apierrors.NewUnauthorized("Unauthorized"), want: FailureUnauthorized},
		{name: "403", err: apierrors.NewForbidden(ssr, "", errors.New("denied")), want: FailureOther},
		{name: "500", err: apierrors.NewInternalError(errors.New("boom")), want: FailureOther},
		{name: "context deadline", err: inURL(context.DeadlineExceeded), want: FailureUnreachable},
		{name: "network timeout", err: inURL(timeoutError{}), want: FailureUnreachable},
		{name: "connection refused", err: inURL(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}), want: FailureUnreachable},
		{name: "name resolution", err: inURL(&net.DNSError{Err: "no such host", Name: "c100.example", IsNotFound: true}), want: FailureUnreachable},
		// crypto/tls reports an alert as a *net.OpError (conn.go); the server answered, so it is not Unreachable.
		{name: "TLS alert sent by the server", err: inURL(&net.OpError{Op: "remote error", Err: errors.New("tls: unknown certificate authority")}), want: FailureOther},
		{name: "TLS alert sent by the client", err: inURL(&net.OpError{Op: "local error", Err: errors.New("tls: bad record MAC")}), want: FailureOther},
		{name: "anything else", err: errors.New("unexpected"), want: FailureOther},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := classify(stepSelfSubjectReview, tc.err)
			g.Expect(got.Reason).To(Equal(tc.want))
			g.Expect(got.Step).To(Equal(stepSelfSubjectReview))
			var status apierrors.APIStatus
			if errors.As(tc.err, &status) {
				// The server's message is withheld (TestClassifyWithholdsServerMessages); code and reason survive.
				g.Expect(apierrors.ReasonForError(got)).To(Equal(apierrors.ReasonForError(tc.err)))
				var gotStatus apierrors.APIStatus
				g.Expect(errors.As(got, &gotStatus)).To(BeTrue())
				g.Expect(gotStatus.Status().Code).To(Equal(status.Status().Code))
				return
			}
			g.Expect(errors.Is(got, tc.err)).To(BeTrue())
		})
	}
}

// TestClassifyWithholdsServerMessages proves that the API server's free-text message never reaches ProbeError: for
// IBM's admin certificate kube-apiserver's 403 text names the IAM identity, which conditions, events and logs must
// never reveal.
func TestClassifyWithholdsServerMessages(t *testing.T) {
	const user = "KubeCert#<synthetic-iam-id>"
	ssar := schema.GroupResource{Group: "authorization.k8s.io", Resource: "selfsubjectaccessreviews"}
	// The text kube-apiserver writes for a 403 (k8s.io/apiserver responsewriters forbiddenMessage).
	forbidden := apierrors.NewForbidden(ssar, "", fmt.Errorf("User %q cannot create resource %q in API group %q at the cluster scope",
		user, ssar.Resource, ssar.Group))
	internal := apierrors.NewInternalError(fmt.Errorf("admission webhook denied the request for user %q", user))

	testCases := []struct {
		name        string
		err         error
		wantMessage string
		isReason    func(error) bool
	}{
		{name: "403 naming the user", err: forbidden, isReason: apierrors.IsForbidden,
			wantMessage: "the API server answered HTTP 403 (Forbidden); its message is withheld because it can name the authenticated user"},
		{name: "500 naming the user", err: internal, isReason: apierrors.IsInternalError,
			wantMessage: "the API server answered HTTP 500 (InternalError); its message is withheld because it can name the authenticated user"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.err.Error()).To(ContainSubstring(user), "the test input must name the user")
			got := classify(stepAccessReview, tc.err)
			g.Expect(got.Reason).To(Equal(FailureOther))
			g.Expect(got.Error()).ToNot(ContainSubstring(user))
			g.Expect(got.Error()).ToNot(ContainSubstring("synthetic-iam-id"))
			g.Expect(got.Error()).To(Equal("workload probe: SelfSubjectAccessReview failed (Other): " + tc.wantMessage))
			g.Expect(tc.isReason(got)).To(BeTrue())
			g.Expect(errors.Is(got, tc.err)).To(BeFalse(), "the original error must not stay reachable through Unwrap")
		})
	}
}

func TestReasonOf(t *testing.T) {
	probeErr := &ProbeError{Reason: FailureTLS, Step: stepSelfSubjectReview, Err: errors.New("x509: certificate signed by unknown authority")}
	testCases := []struct {
		name string
		err  error
		want FailureReason
	}{
		{name: "nil", err: nil, want: ""},
		{name: "probe error", err: probeErr, want: FailureTLS},
		{name: "wrapped probe error", err: fmt.Errorf("verifying the new kubeconfig: %w", probeErr), want: FailureTLS},
		{name: "other error", err: errors.New("boom"), want: FailureOther},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(ReasonOf(tc.err)).To(Equal(tc.want))
		})
	}
}

func TestProbeError(t *testing.T) {
	g := NewWithT(t)
	cause := errors.New("x509: certificate signed by unknown authority")
	err := &ProbeError{Reason: FailureTLS, Step: stepVersion, Err: cause}
	g.Expect(err.Error()).To(Equal("workload probe: GET /version failed (TLS): x509: certificate signed by unknown authority"))
	g.Expect(errors.Unwrap(err)).To(Equal(cause))
}
