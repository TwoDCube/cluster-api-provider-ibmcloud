/*
Copyright 2024 The Kubernetes Authors.

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

package resourcemanager

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/accounts"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/authenticator"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/serviceurl"
)

// Service holds the IBM Cloud Resource Manager Service specific information.
type Service struct {
	client *resourcemanagerv2.ResourceManagerV2
}

// NewService returns a new service for the resource manager. When options.Authenticator is nil it builds one from the
// environment (IBM_CREDENTIALS_FILE, IBMCLOUD_*), which the existing VPC and PowerVS callers rely on; the managed
// Kubernetes controllers use NewServiceWithAuthenticatorAndTimeout, which never does.
func NewService(options *resourcemanagerv2.ResourceManagerV2Options) (ResourceManager, error) {
	if options == nil {
		options = &resourcemanagerv2.ResourceManagerV2Options{}
	}
	if options.Authenticator == nil {
		auth, err := authenticator.GetAuthenticator()
		if err != nil {
			return nil, err
		}
		options.Authenticator = auth
	}
	rmClient, err := resourcemanagerv2.NewResourceManagerV2(options)
	if err != nil {
		return nil, err
	}
	return &Service{
		client: rmClient,
	}, nil
}

// serviceURLForm names the URL form NewServiceWithAuthenticatorAndTimeout accepts; every URL error repeats it so that
// the operator sees the expected value next to the rejected one.
const serviceURLForm = "an absolute URL of the Resource Manager API, for example " + resourcemanagerv2.DefaultServiceURL +
	" (the default, or the rm entry of --service-endpoint)"

// NewServiceWithAuthenticatorAndTimeout returns a Resource Manager Service that sends every request to serviceURL,
// authenticated by auth, and aborts each request, from dialing to reading the whole response body, after timeout. The
// managed Kubernetes controllers use it.
//
// Unlike NewService it never reads credentials from the environment: NewService builds an authenticator from
// IBM_CREDENTIALS_FILE or IBMCLOUD_* variables when it gets none, so a caller that lost its credentials would silently
// act as whatever identity the environment holds. Here a nil authenticator is an error, because the managed Kubernetes
// controllers must act only as the identity they resolved for the Cluster.
//
// Why a timeout: go-sdk-core's default HTTP client (core.DefaultHTTPClient, go-sdk-core v5.25.3
// base_service.go:782-786) has neither an overall Client.Timeout nor a ResponseHeaderTimeout, and the Resource Manager
// SDK methods take no context. Without a timeout, one response that never arrives blocks the caller for good; for the
// managed controllers that caller is the IBMCloudManagedCluster controller, which runs a single worker, so every
// IBMCloudManagedCluster of the manager would stall. The HTTP client is a fresh core.DefaultHTTPClient() with Timeout
// set, installed with BaseService.SetHTTPClient (base_service.go:244-256), so go-sdk-core's pooled transport and its
// minimum TLS version 1.2 stay as they are. SDK retries stay off, as go-sdk-core leaves them, so timeout bounds the
// whole call.
//
// Parameters:
//   - serviceURL is the Resource Manager API URL, for example resourcemanagerv2.DefaultServiceURL or the rm entry of
//     --service-endpoint. It must be an absolute http or https URL with a host and without user information; an "@"
//     after the host counts as user information (serviceurl.HasAtAfterHost). The scheme is the caller's policy, as for
//     NewService: the managed client builder admits only https --service-endpoint entries, because every request
//     carries a bearer token, and its tests send Resource Manager requests to plain-HTTP test servers. It must also end
//     at the path, without a query or fragment, because go-sdk-core appends each operation path (for example
//     /v2/resource_groups/{id}) to it.
//   - auth authenticates every request. It must not be nil, including a typed nil such as
//     (*core.IamAuthenticator)(nil): there is deliberately no fallback to the environment.
//   - timeout bounds each request (http.Client.Timeout) and must be positive. The managed client builder passes
//     ProdClientBuilderOptions.HTTPTimeout.
//
// It makes no network call; go-sdk-core validates auth (for example that an IAM authenticator has an API key) but
// fetches the first token only on the first request.
func NewServiceWithAuthenticatorAndTimeout(serviceURL string, auth core.Authenticator, timeout time.Duration) (ResourceManager, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("resource manager client request timeout is %s: pass a positive duration, for example 30s, "+
			"so that a Resource Manager response that never arrives cannot block the caller", timeout)
	}
	// core.IsNil also catches a typed nil inside the interface, which a plain auth == nil check would let through.
	if core.IsNil(auth) {
		return nil, errors.New("resource manager client authenticator is nil: pass the authenticator of the IBM Cloud " +
			"credentials the client acts as (a Resource Manager client built with an explicit authenticator never reads " +
			"credentials from the environment)")
	}
	if err := validateServiceURL(serviceURL); err != nil {
		return nil, err
	}

	rmClient, err := resourcemanagerv2.NewResourceManagerV2(&resourcemanagerv2.ResourceManagerV2Options{
		Authenticator: auth,
		URL:           serviceURL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating the Resource Manager client for %s: %w", serviceURL, err)
	}
	// A new client per service: setting Timeout on a shared client would change every other client that uses it.
	httpClient := core.DefaultHTTPClient()
	httpClient.Timeout = timeout
	rmClient.Service.SetHTTPClient(httpClient)
	return &Service{client: rmClient}, nil
}

// validateServiceURL returns an error unless serviceURL is an absolute http or https URL with a host and without user
// information, query or fragment. An empty URL is an error rather than the SDK's default, so that every caller states
// the endpoint it uses. The messages print the URL only in the form serviceurl.Printable returns, and a URL that does
// not parse only as serviceurl.ParseFailureReason describes it, so no user information, query or fragment typed into it
// (any of them may carry a token) reaches a log or a condition. A URL that passes holds no user information in any
// form, so NewServiceWithAuthenticatorAndTimeout may print it as it is.
func validateServiceURL(serviceURL string) error {
	if serviceURL == "" {
		return fmt.Errorf("resource manager service URL is empty: pass %s", serviceURLForm)
	}
	parsed, err := url.Parse(serviceURL)
	if err != nil {
		// url.Parse's error quotes the raw input, and several of the reasons it wraps quote a part of it (an invalid
		// port, for example, quotes the rest of a password that holds a "/"), so only the kind of problem is printed.
		return fmt.Errorf("resource manager service URL is not a valid URL (%s): pass %s", serviceurl.ParseFailureReason(err), serviceURLForm)
	}
	// User information that holds a "/", "?" or "#" leaves parsed.User nil: url.Parse ends the host there and leaves the
	// "@" after it (serviceurl.HasAtAfterHost). It is rejected here too, and Printable does not show it; otherwise
	// "http://<token>/<rest>@host" would pass (this client also admits http) and reach the logs as it is, through
	// NewServiceWithAuthenticatorAndTimeout's error that names the URL and through go-sdk-core's transport errors.
	if parsed.User != nil || serviceurl.HasAtAfterHost(parsed) {
		return fmt.Errorf("resource manager service URL %q must not contain user information (credentials come only from the authenticator): pass %s",
			serviceurl.Printable(parsed), serviceURLForm)
	}
	// url.Parse lower-cases the scheme; an opaque URL such as "https:host" has no Host.
	if (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return fmt.Errorf("resource manager service URL %q is not %s", serviceurl.Printable(parsed), serviceURLForm)
	}
	// go-sdk-core builds each request URL by appending the operation path to the service URL string (go-sdk-core
	// v5.25.3, core/request_builder.go, ResolveRequestURL), so with "https://host?debug" a GetResourceGroup call would
	// send GET / with the query "debug/v2/resource_groups/<id>", and a fragment would swallow the path the same way.
	// Such a URL fails only later, as a confusing 404, so it is rejected here, by the rule of the VPC client
	// (pkg/cloud/services/vpc). A bare trailing "#" leaves no trace in the parsed URL, which is why the raw string is
	// checked; url.Parse cuts the fragment at the first "#", so any "#" in it starts a fragment.
	if parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(serviceURL, "#") {
		return fmt.Errorf("resource manager service URL must end at the path: remove the query (?...) or fragment (#...) "+
			"after %q, because the client appends each operation path (for example /v2/resource_groups) to the URL; pass %s",
			serviceurl.Printable(parsed), serviceURLForm)
	}
	return nil
}

// GetResourceGroup returns a Resource Group.
func (s *Service) GetResourceGroup(getResourceGroupOptions *resourcemanagerv2.GetResourceGroupOptions) (*resourcemanagerv2.ResourceGroup, *core.DetailedResponse, error) {
	return s.client.GetResourceGroup(getResourceGroupOptions)
}

// ListResourceGroups lists the resource groups.
func (s *Service) ListResourceGroups(listResourceGroupsOptions *resourcemanagerv2.ListResourceGroupsOptions) (result *resourcemanagerv2.ResourceGroupList, response *core.DetailedResponse, err error) {
	return s.client.ListResourceGroups(listResourceGroupsOptions)
}

// GetResourceGroupByName returns the Resource Group with the provided name, if found.
func (s *Service) GetResourceGroupByName(rgName string) (*resourcemanagerv2.ResourceGroup, error) {
	accountID, err := accounts.GetAccountID()
	if err != nil {
		return nil, fmt.Errorf("failed getting account id for resource group lookup: %w", err)
	}

	listOptions := s.client.NewListResourceGroupsOptions()
	listOptions.SetAccountID(accountID)
	listOptions.SetName(rgName)

	result, response, err := s.ListResourceGroups(listOptions)
	if err != nil {
		return nil, fmt.Errorf("failed listing Resource Groups: %w", err)
	}
	if result == nil || result.Resources == nil || len(result.Resources) != 1 || (response != nil && response.StatusCode == http.StatusNotFound) {
		return nil, fmt.Errorf("failed to find Resource Group")
	}
	return &result.Resources[0], nil
}
