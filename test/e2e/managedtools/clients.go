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

package managedtools

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/IBM/go-sdk-core/v5/core"
)

// DefaultUserAgent is the User-Agent of every request of this package's clients, so that IBM's request logs tell the
// e2e tooling apart from the CAPIBM controllers.
const DefaultUserAgent = "cluster-api-provider-ibmcloud-e2e"

// ClientOptions are the explicit inputs of every IBM client of this package. Nothing is read from the environment:
// the lanes build the authenticator from the identity fixture's credentials and pass the service URL and the HTTP
// client themselves.
type ClientOptions struct {
	// URL is the base URL of the service, for example https://containers.cloud.ibm.com/global or
	// https://eu-de.iaas.cloud.ibm.com/v1. It goes to the SDK as it is; empty selects the SDK's default.
	URL string
	// Authenticator signs every request. Required; there is no fallback to the environment.
	Authenticator core.Authenticator
	// HTTPClient sends every request. Required, so that the caller chooses the timeout (the lanes use one with
	// Timeout set; the unit tests pass the httptest server's client).
	HTTPClient *http.Client
	// ResourceGroupID limits every listing to one resource group: both clients keep only the items whose resource
	// group equals it, whatever IBM's answer contains, and the IBM Cloud Kubernetes Service client also sends it with
	// every request as X-Auth-Resource-Group. Optional: "" means every resource the credentials can see.
	ResourceGroupID string
}

// validate returns one error naming every invalid field of o and the accepted form; the URL is the SDK's to check.
// service names the client in the messages, for example "IBM Cloud Kubernetes Service".
func (o ClientOptions) validate(service string) error {
	var errs []error
	if core.IsNil(o.Authenticator) {
		errs = append(errs, fmt.Errorf("%s client: the authenticator is nil; pass the authenticator of the identity "+
			"fixture's credentials (this package never reads credentials from the environment)", service))
	}
	if o.HTTPClient == nil {
		errs = append(errs, fmt.Errorf("%s client: the HTTP client is nil; pass one with a timeout, for example "+
			"&http.Client{Timeout: time.Minute}", service))
	}
	return errors.Join(errs...)
}

// APIError is an HTTP error answer of an IBM API: the status code and IBM's error code and description, never the
// request, so that no token or credential reaches a log.
type APIError struct {
	// Operation names the call, for example "DeleteCluster".
	Operation string
	// StatusCode is the HTTP status; 0 when no answer arrived (for example a timeout).
	StatusCode int
	// Message is the SDK's error text (IBM's description or the transport error).
	Message string
}

// Error implements error.
func (e *APIError) Error() string {
	if e.StatusCode == 0 {
		return fmt.Sprintf("%s: no answer from IBM Cloud: %s", e.Operation, e.Message)
	}
	return fmt.Sprintf("%s: IBM Cloud answered HTTP %d: %s", e.Operation, e.StatusCode, e.Message)
}

// IsNotFound reports whether err is an APIError with HTTP status 404.
//
// Parameters:
//   - err is any error; wrapped errors are unwrapped.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

// newAPIError turns an SDK result into an APIError.
//
// Parameters:
//   - operation names the call.
//   - response is the SDK's detailed response; nil when no answer arrived.
//   - err is the SDK's error; it must not be nil.
func newAPIError(operation string, response *core.DetailedResponse, err error) *APIError {
	apiErr := &APIError{Operation: operation, Message: err.Error()}
	if response != nil {
		apiErr.StatusCode = response.StatusCode
	}
	return apiErr
}
