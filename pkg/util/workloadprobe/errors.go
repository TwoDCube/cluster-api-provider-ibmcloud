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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FailureReason classifies why a probe failed, so the caller can choose its next step: another trust mode, a
// credential refresh, a wait, or a report.
type FailureReason string

const (
	// FailureInvalidKubeconfig means: the bytes are not a kubeconfig the prober accepts; no request was sent. The caller
	// reports the kubeconfig as invalid.
	FailureInvalidKubeconfig FailureReason = "InvalidKubeconfig"
	// FailureTLS means: the server certificate did not verify with the kubeconfig's trust. The caller tries the other CA
	// trust mode.
	FailureTLS FailureReason = "TLS"
	// FailureUnauthorized means: the server answered 401, so it does not accept the credential. The caller refreshes it.
	FailureUnauthorized FailureReason = "Unauthorized"
	// FailureUnreachable means: the server did not answer in time, refused the connection or could not be resolved. The
	// caller waits and probes again.
	FailureUnreachable FailureReason = "Unreachable"
	// FailureOther means: anything else, for example a 403, a 5xx, a refused redirect, a response that does not decode,
	// or a TLS alert, such as the one a server that requires and rejects the client certificate sends during the
	// handshake. The server answered in each case, so neither waiting nor the other trust mode is the remedy.
	FailureOther FailureReason = "Other"
)

// Step names of a probe, used in ProbeError messages.
const (
	stepKubeconfig        = "kubeconfig validation"
	stepSelfSubjectReview = "SelfSubjectReview"
	stepVersion           = "GET /version"
	stepAccessReview      = "SelfSubjectAccessReview"
)

// ProbeError is the error Probe returns for every failure.
type ProbeError struct {
	// Reason classifies the failure.
	Reason FailureReason
	// Step names the probe step that failed: kubeconfig validation, SelfSubjectReview, GET /version or
	// SelfSubjectAccessReview.
	Step string
	// Err is the underlying error. It never contains key material: client-go and crypto/tls do not put it into errors.
	// For an answer of the API server (an apierrors.APIStatus) Err keeps only the HTTP code and reason, so the
	// apierrors predicates such as IsForbidden still work, but not the server's message: kube-apiserver's 403 text
	// names the authenticated user, which for IBM's admin certificate is KubeCert#<caller-iam-id> (observed on IBM Cloud),
	// and callers put this error into conditions, events and logs, which must never reveal an IAM identity.
	Err error
}

// Error implements error.
func (e *ProbeError) Error() string {
	return fmt.Sprintf("workload probe: %s failed (%s): %v", e.Step, e.Reason, e.Err)
}

// Unwrap returns the underlying error, so errors.Is and errors.As see through a ProbeError.
func (e *ProbeError) Unwrap() error {
	return e.Err
}

// ReasonOf returns the FailureReason of the first ProbeError in err's chain, FailureOther for any other non-nil error,
// and "" for nil.
//
// Parameters: err is an error returned by a Prober, possibly wrapped by the caller.
func ReasonOf(err error) FailureReason {
	if err == nil {
		return ""
	}
	var probeErr *ProbeError
	if errors.As(err, &probeErr) {
		return probeErr.Reason
	}
	return FailureOther
}

// classify wraps an error of one probe step into a ProbeError with its FailureReason. TLS verification is checked
// first: a certificate error is never a reachability problem, and the caller must try the other trust mode for it. A
// TLS alert is checked before reachability because crypto/tls reports it as a *net.OpError, which isUnreachable would
// otherwise match.
func classify(step string, err error) *ProbeError {
	reason := FailureOther
	switch {
	case isTLSVerificationError(err):
		reason = FailureTLS
	case apierrors.IsUnauthorized(err):
		reason = FailureUnauthorized
	case isTLSAlert(err):
		reason = FailureOther
	case isUnreachable(err):
		reason = FailureUnreachable
	}
	return &ProbeError{Reason: reason, Step: step, Err: withoutServerMessage(err)}
}

// withoutServerMessage returns err unchanged unless it is an answer of the API server (an apierrors.APIStatus). Such
// an answer is replaced by a Status error that keeps only the HTTP code and reason, because the server's free-text
// message can name the authenticated user (see ProbeError.Err). Details and causes are dropped for the same reason.
func withoutServerMessage(err error) error {
	var apiStatus apierrors.APIStatus
	if !errors.As(err, &apiStatus) {
		return err
	}
	status := apiStatus.Status()
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    status.Code,
		Reason:  status.Reason,
		Message: fmt.Sprintf("the API server answered HTTP %d (%s); its message is withheld because it can name the authenticated user", status.Code, status.Reason),
	}}
}

// isTLSVerificationError reports whether err says the server certificate did not verify. Since Go 1.20 the TLS client
// wraps verification failures in tls.CertificateVerificationError; the x509 types cover callers that unwrap it.
func isTLSVerificationError(err error) bool {
	var verification *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	return errors.As(err, &verification) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) || errors.As(err, &invalid)
}

// isTLSAlert reports whether err is a TLS alert. crypto/tls (conn.go) reports an alert the server sent as a
// *net.OpError with Op "remote error", for example "tls: unknown certificate authority" from a server that rejects the
// client certificate, and an alert it sent itself as Op "local error". Both mean the connection was made and the peer
// answered, so they are not a reachability problem.
func isTLSAlert(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && (opErr.Op == "remote error" || opErr.Op == "local error")
}

// isUnreachable reports whether err is a timeout, a deadline, or a dial, connection or name-resolution failure.
func isUnreachable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	return errors.As(err, &opErr) || errors.As(err, &dnsErr)
}
