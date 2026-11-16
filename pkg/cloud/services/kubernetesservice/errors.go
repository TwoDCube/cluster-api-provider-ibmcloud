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
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
)

// Operation names one KubernetesService method in APIError and Classify. Classify needs it because IBM gives the same
// status code different meanings per operation (the live swagger of 2026-11-16).
type Operation string

const (
	// OpGetCluster is KubernetesService.GetCluster.
	OpGetCluster Operation = "GetCluster"
	// OpCreateCluster is KubernetesService.CreateCluster.
	OpCreateCluster Operation = "CreateCluster"
	// OpListVersions is KubernetesService.ListVersions.
	OpListVersions Operation = "ListVersions"
	// OpDeleteCluster is KubernetesService.DeleteCluster. It has no operation-specific rows: the delete flow never
	// treats an answer as terminal and maps the generic classes itself.
	OpDeleteCluster Operation = "DeleteCluster"
)

// ErrorClass is what an error from KubernetesService means for a controller. Each controller decides what to do for
// a class; this package only classifies.
type ErrorClass string

const (
	// ClassNone is the class of a nil error.
	ClassNone ErrorClass = ""
	// ClassNotFound is a 404: the cluster (or pool) does not exist or is not visible; IBM was observed to answer 404 for
	// a deleted cluster about 5 minutes after the DELETE.
	ClassNotFound ErrorClass = "NotFound"
	// ClassClusterNameExists is code E0007 on CreateCluster: a cluster with that name exists, and this attempt
	// created nothing.
	ClassClusterNameExists ErrorClass = "ClusterNameExists"
	// ClassInfrastructureKeyNotCreatable is code A0656 on CreateCluster (any status): IBM could not create the
	// per-region and resource-group infrastructure API key with the caller's credentials. IBM checks this before it
	// validates the body, so nothing was created (observed on IBM Cloud).
	ClassInfrastructureKeyNotCreatable ErrorClass = "InfrastructureKeyNotCreatable"
	// ClassInfrastructureKeyDenied is a 403 on CreateCluster: the stored per-region and resource-group
	// infrastructure key lacks permissions. That key may belong to another identity: IBM stores the key of whoever
	// created the first cluster in the region and resource group, or last ran "ibmcloud ks api-key reset" there.
	ClassInfrastructureKeyDenied ErrorClass = "InfrastructureKeyDenied"
	// ClassBadRequest is a 400 or 422 without a more specific row (for example P4102 subnet not found, or E7278 ROKS
	// without COS, both observed on IBM Cloud), and every request this package refused to send (ErrInvalidRequest).
	ClassBadRequest ErrorClass = "BadRequest"
	// ClassUnauthorized is a 401 from IBM Cloud Kubernetes Service, or IAM rejecting the credentials before the
	// request was sent (ErrCredentialsRejected). The caller invalidates that identity's authenticator.
	ClassUnauthorized ErrorClass = "Unauthorized"
	// ClassPermissionDenied is a 403 without a more specific row.
	ClassPermissionDenied ErrorClass = "PermissionDenied"
	// ClassRateLimited is a 429; wait RateLimitBackoff before the next request.
	ClassRateLimited ErrorClass = "RateLimited"
	// ClassTransient is a 5xx (including code E0004), a timeout, a connection error, a limiter wait that ended with
	// the context, or IAM being unavailable. A create attempt with this outcome may or may not have created
	// something.
	ClassTransient ErrorClass = "Transient"
	// ClassUnknown is any other error; callers treat it like ClassTransient.
	ClassUnknown ErrorClass = "Unknown"
)

// IBM error codes Classify matches. The error bodies of most operations are not yet verified against IBM Cloud; the
// table grows as more codes are observed.
const (
	// codeClusterNameExists is IBM's "cluster name already exists" code on createCluster (409).
	codeClusterNameExists = "E0007"
	// codeInfrastructureKeyNotCreatable is IBM's "cannot create the infrastructure API key" code (observed on IBM Cloud).
	codeInfrastructureKeyNotCreatable = "A0656"
	// codeSubnetNotFound is IBM's "subnet cannot be found" code on createCluster (400, observed on IBM Cloud).
	codeSubnetNotFound = "P4102"
	// codeCOSInstanceRequired is IBM's "ROKS on VPC needs a COS instance CRN" code (400, observed on IBM Cloud).
	codeCOSInstanceRequired = "E7278"
	// codeInternalError is IBM's generic internal-error code; it comes with a 5xx.
	codeInternalError = "E0004"
)

// ErrInvalidRequest marks a request that KubernetesService refused to send because an argument is missing or
// malformed. Nothing reached IBM; Classify maps it to ClassBadRequest. The wrapping error names the operation, the
// argument and the accepted form.
var ErrInvalidRequest = errors.New("kubernetesservice: invalid request, nothing was sent to IBM Cloud")

// ErrCredentialsRejected marks a request that was never sent because the IAM token service refused the
// authenticator's credentials (HTTP 400, 401 or 403 from IAM, for example for a deleted API key). Classify maps it to
// ClassUnauthorized, so the caller rebuilds that identity's authenticator.
var ErrCredentialsRejected = errors.New("kubernetesservice: the IAM token service rejected the credentials, nothing was sent to IBM Cloud Kubernetes Service")

// MinRateLimitBackoff is the shortest wait after a 429. IBM's 429 body and Retry-After are not yet verified against
// IBM Cloud (a 429 is never provoked on purpose); 30 s keeps a rate-limited controller well below IBM's 200 requests
// per 10 s per source IP. Fallback: change this value.
const MinRateLimitBackoff = 30 * time.Second

// maxRetryAfter caps the Retry-After CAPIBM honours, so that a malformed header cannot park a cluster for days.
const maxRetryAfter = time.Hour

// APIError is an IBM Cloud Kubernetes Service answer outside 2xx. It is built from go-sdk-core's DetailedResponse,
// because errors.As on core.HTTPProblem does not work with this SDK generation.
type APIError struct {
	// Operation is the KubernetesService method that received the answer.
	Operation Operation
	// StatusCode is the HTTP status code.
	StatusCode int
	// Code is IBM's error code from the JSON body, for example "E0007"; "" when the body has none.
	Code string
	// Description is IBM's error description; IBM free text for messages and events, never matched alone.
	Description string
	// IncidentID is IBM's incident ID from the body, else the X-Request-Id response header; quote it to IBM support.
	IncidentID string
	// RetryAfter is the Retry-After response header as a duration (0 when absent or unparseable, at most one hour).
	RetryAfter time.Duration
}

// Error implements error. The message names the operation, the status, IBM's code and description, and the incident.
func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "IBM Cloud Kubernetes Service %s failed with HTTP %d", e.Operation, e.StatusCode)
	if e.Code != "" {
		fmt.Fprintf(&b, " (code %s)", e.Code)
	}
	if e.Description != "" {
		fmt.Fprintf(&b, ": %s", e.Description)
	}
	if e.IncidentID != "" {
		fmt.Fprintf(&b, " [incident %s]", e.IncidentID)
	}
	return b.String()
}

// Classify returns what err, returned by the KubernetesService method op, means for a controller.
// IBM error codes are matched first, then the status code; IBM's description text is never matched on its own.
//
// Parameters:
//   - op is the operation that returned err; the same status means different things per operation.
//   - err is the error the operation returned; nil gives ClassNone.
func Classify(op Operation, err error) ErrorClass {
	if err == nil {
		return ClassNone
	}
	if errors.Is(err, ErrInvalidRequest) {
		return ClassBadRequest
	}
	if errors.Is(err, ErrCredentialsRejected) {
		return ClassUnauthorized
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		// No answer from IBM Cloud Kubernetes Service: a timeout, a connection error, a limiter wait that ended with
		// the context, the IAM token service being unavailable, or an answer this package could not decode.
		return ClassTransient
	}
	if class := classifyOperation(op, apiErr); class != ClassUnknown {
		return class
	}
	return classifyStatus(apiErr)
}

// classifyOperation applies the rows that only hold for one operation. It returns ClassUnknown when no such row
// matches. op is the operation, apiErr its answer.
func classifyOperation(op Operation, apiErr *APIError) ErrorClass {
	switch op {
	case OpCreateCluster:
		return classifyCreateCluster(apiErr)
	default:
		return ClassUnknown
	}
}

// classifyCreateCluster applies the rows that only hold for CreateCluster to apiErr.
func classifyCreateCluster(apiErr *APIError) ErrorClass {
	switch {
	case apiErr.Code == codeInfrastructureKeyNotCreatable:
		// Any status: IBM checks the infrastructure key before it validates the request (observed on IBM Cloud).
		return ClassInfrastructureKeyNotCreatable
	case apiErr.Code == codeClusterNameExists:
		return ClassClusterNameExists
	case apiErr.Code == codeSubnetNotFound, apiErr.Code == codeCOSInstanceRequired:
		// Both are 400s that created nothing (observed on IBM Cloud); listed by code so the table states what was observed.
		return ClassBadRequest
	case apiErr.StatusCode == http.StatusForbidden:
		// The live swagger documents createCluster's 403 as the stored infrastructure key lacking permissions.
		return ClassInfrastructureKeyDenied
	default:
		return ClassUnknown
	}
}

// classifyStatus applies the status-code rows that hold for every operation to apiErr.
func classifyStatus(apiErr *APIError) ErrorClass {
	switch {
	case apiErr.Code == codeInternalError, apiErr.StatusCode >= http.StatusInternalServerError:
		return ClassTransient
	case apiErr.StatusCode == http.StatusNotFound:
		return ClassNotFound
	case apiErr.StatusCode == http.StatusBadRequest, apiErr.StatusCode == http.StatusUnprocessableEntity:
		return ClassBadRequest
	case apiErr.StatusCode == http.StatusUnauthorized:
		return ClassUnauthorized
	case apiErr.StatusCode == http.StatusForbidden:
		return ClassPermissionDenied
	case apiErr.StatusCode == http.StatusTooManyRequests:
		return ClassRateLimited
	default:
		return ClassUnknown
	}
}

// RateLimitBackoff returns how long to wait after err before the next request: the larger of the answer's
// Retry-After and MinRateLimitBackoff for an *APIError, MinRateLimitBackoff for any other error. err is the error a
// KubernetesService method returned.
func RateLimitBackoff(err error) time.Duration {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.RetryAfter > MinRateLimitBackoff {
		return apiErr.RetryAfter
	}
	return MinRateLimitBackoff
}

// invalidRequest returns the error of a request this package refuses to send.
//
// Parameters:
//   - op is the operation that refused.
//   - problem says what is wrong and what is accepted.
func invalidRequest(op Operation, problem string) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidRequest, op, problem)
}

// callError turns the response and error of a failed SDK or compat call into the error the caller sees.
//
// Parameters:
//   - op is the operation.
//   - tracker tells whether the request passed the limiter and was sent.
//   - response is go-sdk-core's DetailedResponse (may be nil).
//   - err is the error go-sdk-core or net/http returned; not nil.
func callError(op Operation, tracker *callTracker, response *core.DetailedResponse, err error) error {
	if !tracker.sent.Load() {
		// Nothing reached IBM Cloud Kubernetes Service. A response here is the IAM token service's answer
		// (go-sdk-core v5.25.3 base_service.go:390-395), so its status says nothing about IKS.
		if response != nil && credentialsRejected(response.StatusCode) {
			return fmt.Errorf("%w: %s: the IAM token service answered HTTP %d: %w", ErrCredentialsRejected, op, response.StatusCode, err)
		}
		return fmt.Errorf("kubernetesservice: %s: the request was not sent: %w", op, err)
	}
	if response != nil && (response.StatusCode < 200 || response.StatusCode > 299) {
		return newAPIError(op, response)
	}
	return fmt.Errorf("kubernetesservice: %s: %w", op, err)
}

// credentialsRejected reports whether a status of the IAM token service means that it refused the credentials
// themselves, as opposed to being unavailable or rate limiting. statusCode is the IAM answer's status.
func credentialsRejected(statusCode int) bool {
	return statusCode == http.StatusBadRequest || statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden
}

// newAPIError builds the APIError of an IKS answer outside 2xx.
//
// Parameters:
//   - op is the operation that received the answer.
//   - response is the answer; go-sdk-core puts a JSON error body into Result as a map.
func newAPIError(op Operation, response *core.DetailedResponse) *APIError {
	apiErr := &APIError{Operation: op, StatusCode: response.StatusCode}
	if body, ok := response.GetResultAsMap(); ok {
		apiErr.Code = stringField(body, "code")
		apiErr.Description = stringField(body, "description")
		apiErr.IncidentID = stringField(body, "incidentID")
	}
	if apiErr.IncidentID == "" {
		// IBM sets X-Request-Id to the incident ID; it is the only reference when the body is empty or not JSON.
		apiErr.IncidentID = response.Headers.Get("X-Request-Id")
	}
	apiErr.RetryAfter = parseRetryAfter(response.Headers)
	return apiErr
}

// stringField returns body[key] without surrounding white space when it is a string, else "". body is a decoded
// JSON object.
func stringField(body map[string]any, key string) string {
	if value, ok := body[key].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

// parseRetryAfter returns the Retry-After of headers as a duration, or 0 when it is absent or unparseable. Both RFC
// 9110 forms are accepted: delay-seconds, and an HTTP date, which is measured against the response's own Date
// header so that no local clock is read. The result is capped at maxRetryAfter. headers are the response headers
// (may be nil).
func parseRetryAfter(headers http.Header) time.Duration {
	value := strings.TrimSpace(headers.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return time.Duration(seconds) * time.Second
	}
	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	sentAt, err := http.ParseTime(headers.Get("Date"))
	if err != nil || !retryAt.After(sentAt) {
		return 0
	}
	return min(retryAt.Sub(sentAt), maxRetryAfter)
}
