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
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"

	. "github.com/onsi/gomega"
)

// apiError returns the APIError of operation op with status and IBM code.
func apiError(op Operation, status int, code string) error {
	return &APIError{Operation: op, StatusCode: status, Code: code}
}

// TestClassify is the classification table, operation by operation.
func TestClassify(t *testing.T) {
	testCases := []struct {
		name     string
		op       Operation
		err      error
		expected ErrorClass
	}{
		{name: "nil error", op: OpGetCluster, err: nil, expected: ClassNone},
		{name: "refused before sending", op: OpCreateCluster, err: invalidRequest(OpCreateCluster, "Name is empty"), expected: ClassBadRequest},
		{name: "IAM rejected the credentials", op: OpGetCluster, err: fmt.Errorf("%w: GetCluster", ErrCredentialsRejected), expected: ClassUnauthorized},
		{name: "connection error", op: OpGetCluster, err: errors.New("dial tcp: connection refused"), expected: ClassTransient},
		{name: "timeout", op: OpListVersions, err: fmt.Errorf("kubernetesservice: ListVersions: %w", context.DeadlineExceeded), expected: ClassTransient},
		{name: "wrapped APIError is found", op: OpGetCluster, err: fmt.Errorf("observing: %w", apiError(OpGetCluster, http.StatusNotFound, "")), expected: ClassNotFound},

		{name: "404 on GetCluster", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusNotFound, "G0004"), expected: ClassNotFound},
		{name: "400 without a specific row", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusBadRequest, ""), expected: ClassBadRequest},
		{name: "422", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusUnprocessableEntity, ""), expected: ClassBadRequest},
		{name: "401", op: OpListVersions, err: apiError(OpListVersions, http.StatusUnauthorized, "G0001"), expected: ClassUnauthorized},
		{name: "other 403 on GetCluster", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusForbidden, ""), expected: ClassPermissionDenied},
		{name: "429 without Retry-After", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusTooManyRequests, ""), expected: ClassRateLimited},
		{name: "500", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusInternalServerError, ""), expected: ClassTransient},
		{name: "503", op: OpListVersions, err: apiError(OpListVersions, http.StatusServiceUnavailable, ""), expected: ClassTransient},
		{name: "E0004 is Transient whatever the status", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusBadRequest, "E0004"), expected: ClassTransient},
		{name: "unexpected status", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusTeapot, ""), expected: ClassUnknown},
		{name: "409 on GetCluster has no meaning", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusConflict, ""), expected: ClassUnknown},

		{name: "A0656 on CreateCluster whatever the status", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusBadRequest, "A0656"), expected: ClassInfrastructureKeyNotCreatable},
		{name: "A0656 on CreateCluster with 401", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusUnauthorized, "A0656"), expected: ClassInfrastructureKeyNotCreatable},
		{name: "CreateCluster 403 without code is InfrastructureKeyDenied", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusForbidden, ""), expected: ClassInfrastructureKeyDenied},
		{name: "GetCluster 403 is PermissionDenied not InfrastructureKeyDenied", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusForbidden, ""), expected: ClassPermissionDenied},
		{name: "409 E0007 on CreateCluster", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusConflict, "E0007"), expected: ClassClusterNameExists},
		{name: "400 P4102 on CreateCluster", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusBadRequest, "P4102"), expected: ClassBadRequest},
		{name: "400 E7278 on CreateCluster", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusBadRequest, "E7278"), expected: ClassBadRequest},
		{name: "other 409 on CreateCluster", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusConflict, ""), expected: ClassUnknown},
		{name: "500 on CreateCluster", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusInternalServerError, ""), expected: ClassTransient},
		{name: "A0656 on another operation is only its status", op: OpGetCluster, err: apiError(OpGetCluster, http.StatusForbidden, "A0656"), expected: ClassPermissionDenied},

		{name: "409 E0d39 on CreateWorkerPool", op: OpCreateWorkerPool, err: apiError(OpCreateWorkerPool, http.StatusConflict, "E0d39"), expected: ClassPoolNameExists},
		{name: "other 409 on CreateWorkerPool is OperationInProgress", op: OpCreateWorkerPool, err: apiError(OpCreateWorkerPool, http.StatusConflict, "E9999"), expected: ClassOperationInProgress},
		{name: "403 on CreateWorkerPool is PermissionDenied", op: OpCreateWorkerPool, err: apiError(OpCreateWorkerPool, http.StatusForbidden, ""), expected: ClassPermissionDenied},
		{name: "E0d39 on CreateCluster has no pool meaning", op: OpCreateCluster, err: apiError(OpCreateCluster, http.StatusConflict, "E0d39"), expected: ClassUnknown},
		{name: "404 on ListWorkers", op: OpListWorkers, err: apiError(OpListWorkers, http.StatusNotFound, ""), expected: ClassNotFound},
		{name: "409 on ReplaceWorker has no specific meaning", op: OpReplaceWorker, err: apiError(OpReplaceWorker, http.StatusConflict, ""), expected: ClassUnknown},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(Classify(tc.op, tc.err)).To(Equal(tc.expected))
		})
	}
}

func TestAPIErrorError(t *testing.T) {
	testCases := []struct {
		name     string
		err      APIError
		expected string
	}{
		{
			name:     "all fields",
			err:      APIError{Operation: OpCreateCluster, StatusCode: 409, Code: "E0007", Description: "The cluster name is already in use.", IncidentID: "abc"},
			expected: "IBM Cloud Kubernetes Service CreateCluster failed with HTTP 409 (code E0007): The cluster name is already in use. [incident abc]",
		},
		{
			name:     "status only",
			err:      APIError{Operation: OpGetCluster, StatusCode: 503},
			expected: "IBM Cloud Kubernetes Service GetCluster failed with HTTP 503",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.err.Error()).To(Equal(tc.expected))
		})
	}
}

func TestNewAPIError(t *testing.T) {
	const date = "Mon, 16 Nov 2026 03:00:00 GMT"
	testCases := []struct {
		name     string
		response *core.DetailedResponse
		expected APIError
	}{
		{
			name: "JSON body",
			response: &core.DetailedResponse{
				StatusCode: http.StatusBadRequest,
				Result:     map[string]any{"code": "E1437", "description": " too few workers ", "incidentID": "inc-1", "type": "BadRequest"},
			},
			expected: APIError{Operation: OpGetCluster, StatusCode: http.StatusBadRequest, Code: "E1437", Description: "too few workers", IncidentID: "inc-1"},
		},
		{
			name: "no body: incident from X-Request-Id",
			response: &core.DetailedResponse{
				StatusCode: http.StatusBadGateway,
				Headers:    http.Header{"X-Request-Id": []string{"req-1"}},
			},
			expected: APIError{Operation: OpGetCluster, StatusCode: http.StatusBadGateway, IncidentID: "req-1"},
		},
		{
			name: "non-string fields are ignored",
			response: &core.DetailedResponse{
				StatusCode: http.StatusConflict,
				Result:     map[string]any{"code": 7, "description": nil},
			},
			expected: APIError{Operation: OpGetCluster, StatusCode: http.StatusConflict},
		},
		{
			name:     "Retry-After in seconds",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"45"}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429, RetryAfter: 45 * time.Second},
		},
		{
			name:     "Retry-After as an HTTP date, measured against the response Date",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"Mon, 16 Nov 2026 03:02:00 GMT"}, "Date": []string{date}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429, RetryAfter: 2 * time.Minute},
		},
		{
			name:     "Retry-After as an HTTP date without a Date header is ignored",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"Mon, 16 Nov 2026 03:02:00 GMT"}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429},
		},
		{
			name:     "Retry-After date in the past is ignored",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"Mon, 16 Nov 2026 02:00:00 GMT"}, "Date": []string{date}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429},
		},
		{
			name:     "Retry-After date too far ahead is capped",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"Tue, 17 Nov 2026 03:00:00 GMT"}, "Date": []string{date}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429, RetryAfter: time.Hour},
		},
		{
			name:     "Retry-After seconds too large are capped",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"999999999999"}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429, RetryAfter: time.Hour},
		},
		{
			name:     "negative Retry-After is ignored",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"-5"}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429},
		},
		{
			name:     "garbage Retry-After is ignored",
			response: &core.DetailedResponse{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"soon"}}},
			expected: APIError{Operation: OpGetCluster, StatusCode: 429},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(*newAPIError(OpGetCluster, tc.response)).To(Equal(tc.expected))
		})
	}
}

// TestRateLimitBackoff covers the 30 s floor of MinRateLimitBackoff with and without IBM's Retry-After.
func TestRateLimitBackoff(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected time.Duration
	}{
		{name: "429 without Retry-After waits the minimum", err: &APIError{StatusCode: 429}, expected: MinRateLimitBackoff},
		{name: "429 with a shorter Retry-After waits the minimum", err: &APIError{StatusCode: 429, RetryAfter: 5 * time.Second}, expected: MinRateLimitBackoff},
		{name: "429 with a longer Retry-After waits Retry-After", err: fmt.Errorf("wrapped: %w", &APIError{StatusCode: 429, RetryAfter: 2 * time.Minute}), expected: 2 * time.Minute},
		{name: "any other error waits the minimum", err: errors.New("connection reset"), expected: MinRateLimitBackoff},
		{name: "nil waits the minimum", err: nil, expected: MinRateLimitBackoff},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(RateLimitBackoff(tc.err)).To(Equal(tc.expected))
		})
	}
}

// TestRateLimitedAnswer drives a 429 with Retry-After through the HTTP stack.
func TestRateLimitedAnswer(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIKS(t, replyWithHeaders(http.StatusTooManyRequests, map[string]string{"Retry-After": "120"}, []byte(`{"description":"Too many requests."}`)))
	service, _ := newTestService(t, fake)
	_, err := service.GetCluster(context.Background(), testIKSClusterID, "")
	g.Expect(Classify(OpGetCluster, err)).To(Equal(ClassRateLimited))
	g.Expect(RateLimitBackoff(err)).To(Equal(2 * time.Minute))
}
