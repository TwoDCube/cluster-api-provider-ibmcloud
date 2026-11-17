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
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/accounts"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/authenticator"
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
//     --service-endpoint. It goes to the SDK as it is, as with NewService; empty selects the SDK's default,
//     resourcemanagerv2.DefaultServiceURL.
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
