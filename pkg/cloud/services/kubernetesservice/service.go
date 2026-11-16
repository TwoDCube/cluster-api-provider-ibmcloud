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
	"net/url"
	"strings"
	"time"

	"github.com/IBM-Cloud/container-services-go-sdk/kubernetesserviceapiv1"
	"github.com/IBM/go-sdk-core/v5/core"
)

// DefaultUserAgent is the User-Agent sent when ServiceOptions.UserAgent is empty, so that IBM never sees the SDK's
// generic "go-sdk-template" product token from CAPIBM.
const DefaultUserAgent = "cluster-api-provider-ibmcloud"

// providerVPCGen2 is the only cluster provider CAPIBM creates: VPC Gen2, never classic infrastructure or Satellite.
const providerVPCGen2 = "vpc-gen2"

var _ KubernetesService = &Service{}

// ServiceOptions configures NewService. Every field is explicit; NewService validates each one and never reads the
// environment or falls back to a global authenticator.
type ServiceOptions struct {
	// URL is the absolute https base URL, for example https://containers.cloud.ibm.com/global. Required.
	URL string
	// Authenticator signs every request. Required. The client builder passes its delegating identityAuthenticator.
	Authenticator core.Authenticator
	// RateLimiter is waited on before every request; the builder passes the one process-wide limiter. Required.
	RateLimiter RequestLimiter
	// HTTPTimeout bounds each request, including the limiter wait. Required, > 0.
	HTTPTimeout time.Duration
	// UserAgent is sent on every request, for example "cluster-api-provider-ibmcloud/<version>". Optional;
	// DefaultUserAgent when empty.
	UserAgent string
}

// Service implements KubernetesService on the IBM Cloud Kubernetes Service API. Create it with NewService.
type Service struct {
	// sdk is the generated SDK client; its BaseService carries the URL, the authenticator, the limited HTTP client
	// and the User-Agent default header, which the compat functions reuse.
	sdk *kubernetesserviceapiv1.KubernetesServiceApiV1
	// userAgent is the User-Agent of every request; the compat path that bypasses BaseService.Request sets it itself.
	userAgent string
}

// NewService validates opts and builds the client. It makes no request.
//
// It builds the SDK client with kubernetesserviceapiv1.NewKubernetesServiceApiV1 (never the UsingExternalConfig
// variant, which reads the environment), installs an http.Client{Timeout} whose transport waits on RateLimiter, and
// leaves SDK retries disabled (go-sdk-core would also retry POSTs). UserAgent is installed with
// BaseService.SetDefaultHeaders: every generated SDK method hard-sets "User-Agent: go-sdk-template-0.0.1 (...)"
// (SDK common/headers.go:40-45), BaseService.UserAgent is added only when the header is absent, and Request()
// applies the default headers with Header.Set, which overrides the SDK value (go-sdk-core v5.25.3
// base_service.go:361-380). The compat path that bypasses Request() sets the same header itself.
//
// Parameters:
//   - opts are the URL, authenticator, limiter, timeout and User-Agent; see ServiceOptions.
func NewService(opts ServiceOptions) (*Service, error) {
	return newService(opts, newBaseTransport())
}

// newService is NewService with an injectable network transport, so that tests can talk to an httptest TLS server.
//
// Parameters:
//   - opts are validated as in NewService.
//   - base is the transport below the limiter.
func newService(opts ServiceOptions, base http.RoundTripper) (*Service, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	sdk, err := kubernetesserviceapiv1.NewKubernetesServiceApiV1(&kubernetesserviceapiv1.KubernetesServiceApiV1Options{
		URL:           opts.URL,
		Authenticator: opts.Authenticator,
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: building the IBM Cloud Kubernetes Service SDK client for %s: %w", opts.URL, err)
	}
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	// Retries are off by default; disabling them explicitly documents the decision. go-sdk-core retries POSTs too, and
	// a resent createCluster or createWorkerPool is exactly what the create flow must control itself.
	sdk.Service.DisableRetries()
	sdk.Service.SetHTTPClient(newHTTPClient(opts.HTTPTimeout, opts.RateLimiter, base))
	sdk.Service.SetUserAgent(userAgent)
	sdk.Service.SetDefaultHeaders(http.Header{"User-Agent": []string{userAgent}})
	return &Service{sdk: sdk, userAgent: userAgent}, nil
}

// validate checks every field of o and returns one error that names each invalid field, its value where safe, and
// the accepted form.
func (o ServiceOptions) validate() error {
	var errs []error
	if err := validateServiceURL(o.URL); err != nil {
		errs = append(errs, err)
	}
	if core.IsNil(o.Authenticator) {
		errs = append(errs, errors.New("kubernetesservice: ServiceOptions.Authenticator is nil; pass the authenticator of the resolved identity (NewService never falls back to the environment)"))
	}
	if core.IsNil(o.RateLimiter) {
		errs = append(errs, errors.New("kubernetesservice: ServiceOptions.RateLimiter is nil; pass the process-wide limiter (for example a *rate.Limiter from --containers-api-qps and --containers-api-burst)"))
	}
	if o.HTTPTimeout <= 0 {
		errs = append(errs, fmt.Errorf("kubernetesservice: ServiceOptions.HTTPTimeout is %s; pass a positive duration, for example 30s", o.HTTPTimeout))
	}
	if strings.ContainsAny(o.UserAgent, "\r\n") {
		errs = append(errs, errors.New("kubernetesservice: ServiceOptions.UserAgent contains a line break; pass a single-line product token such as cluster-api-provider-ibmcloud/<version>"))
	}
	return errors.Join(errs...)
}

// validateServiceURL accepts only an absolute https URL without query or fragment. rawURL is ServiceOptions.URL.
func validateServiceURL(rawURL string) error {
	const accepted = "pass the absolute https URL of the IBM Cloud Kubernetes Service API, for example https://containers.cloud.ibm.com/global"
	if rawURL == "" {
		return fmt.Errorf("kubernetesservice: ServiceOptions.URL is empty; %s", accepted)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("kubernetesservice: ServiceOptions.URL %q is not an absolute https URL without query or fragment; %s", rawURL, accepted)
	}
	return nil
}

// GetCluster implements KubernetesService.
func (s *Service) GetCluster(ctx context.Context, idOrName, resourceGroupID string) (*Cluster, error) {
	if idOrName == "" {
		return nil, invalidRequest(OpGetCluster, "the cluster ID or name is empty; pass the IBM cluster ID, or its name for a lookup")
	}
	options := &kubernetesserviceapiv1.VpcGetClusterOptions{Cluster: core.StringPtr(idOrName)}
	if resourceGroupID != "" {
		options.XAuthResourceGroup = core.StringPtr(resourceGroupID)
	}
	ctx, tracker := trackCall(ctx)
	result, response, err := s.sdk.VpcGetClusterWithContext(ctx, options)
	if err != nil {
		return nil, callError(OpGetCluster, tracker, response, err)
	}
	if result == nil {
		return nil, emptyAnswer(OpGetCluster, response)
	}
	return clusterFromSDK(result), nil
}

// CreateCluster implements KubernetesService. It validates req and sends nothing when req is invalid.
func (s *Service) CreateCluster(ctx context.Context, req CreateClusterRequest) (*CreateClusterResult, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	ctx, tracker := trackCall(ctx)
	result, response, err := s.sdk.VpcCreateClusterWithContext(ctx, createClusterOptions(req))
	if err != nil {
		return nil, callError(OpCreateCluster, tracker, response, err)
	}
	if result == nil || core.StringNilMapper(result.ClusterID) == "" {
		// A success without an ID cannot be bound to this attempt; reporting it as an error that Classify treats as
		// transient keeps the attempt's outcome unknown, so the binding rules resolve it by name.
		return nil, fmt.Errorf("kubernetesservice: %s: IBM answered HTTP %d without a clusterID; the outcome of this create attempt is unknown", OpCreateCluster, statusCode(response))
	}
	return createClusterResult(result), nil
}

// DeleteCluster implements KubernetesService.
func (s *Service) DeleteCluster(ctx context.Context, clusterID, resourceGroupID string) error {
	if clusterID == "" {
		return invalidRequest(OpDeleteCluster, "the cluster ID is empty; pass status.clusterID of the IBMCloudManagedControlPlane (CAPIBM never deletes by name)")
	}
	options := &kubernetesserviceapiv1.RemoveClusterOptions{
		IdOrName: core.StringPtr(clusterID),
		// "false" keeps the persistent storage and VPC resources IBM would otherwise delete with the cluster: CAPIBM only
		// references the user's VPC.
		DeleteResources: core.StringPtr("false"),
	}
	if resourceGroupID != "" {
		options.XAuthResourceGroup = core.StringPtr(resourceGroupID)
	}
	ctx, tracker := trackCall(ctx)
	response, err := s.sdk.RemoveClusterWithContext(ctx, options)
	if err != nil {
		return callError(OpDeleteCluster, tracker, response, err)
	}
	return nil
}

// ListVersions implements KubernetesService.
func (s *Service) ListVersions(ctx context.Context) (*Versions, error) {
	ctx, tracker := trackCall(ctx)
	result, response, err := s.sdk.GetVersionsWithContext(ctx, &kubernetesserviceapiv1.GetVersionsOptions{})
	if err != nil {
		return nil, callError(OpListVersions, tracker, response, err)
	}
	if result == nil {
		return nil, emptyAnswer(OpListVersions, response)
	}
	return &Versions{
		Kubernetes: releaseVersions(result["kubernetes"]),
		OpenShift:  releaseVersions(result["openshift"]),
	}, nil
}

// emptyAnswer is the error of a 2xx answer without the body the operation needs.
//
// Parameters:
//   - op is the operation.
//   - response is the answer (may be nil).
func emptyAnswer(op Operation, response *core.DetailedResponse) error {
	return fmt.Errorf("kubernetesservice: %s: IBM answered HTTP %d without a body", op, statusCode(response))
}

// statusCode returns the status of response, or 0 when there is none.
func statusCode(response *core.DetailedResponse) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

// clusterFromSDK maps the SDK's getCluster model to Cluster. result is the decoded answer; nil nested objects map to
// empty fields.
func clusterFromSDK(result *kubernetesserviceapiv1.GetClusterResponse) *Cluster {
	cluster := &Cluster{
		ID:                core.StringNilMapper(result.ID),
		Name:              core.StringNilMapper(result.Name),
		CRN:               core.StringNilMapper(result.Crn),
		Type:              core.StringNilMapper(result.Type),
		Provider:          core.StringNilMapper(result.Provider),
		ResourceGroupID:   core.StringNilMapper(result.ResourceGroup),
		CreatedDate:       core.StringNilMapper(result.CreatedDate),
		State:             core.StringNilMapper(result.State),
		MasterKubeVersion: core.StringNilMapper(result.MasterKubeVersion),
		TargetVersion:     core.StringNilMapper(result.TargetVersion),
		VersionEOS:        core.StringNilMapper(result.VersionEOS),
		MasterURL:         core.StringNilMapper(result.MasterURL),
		PodSubnet:         core.StringNilMapper(result.PodSubnet),
		ServiceSubnet:     core.StringNilMapper(result.ServiceSubnet),
	}
	if lifecycle := result.Lifecycle; lifecycle != nil {
		cluster.MasterState = core.StringNilMapper(lifecycle.MasterState)
		cluster.MasterHealth = core.StringNilMapper(lifecycle.MasterHealth)
		cluster.MasterStatus = strings.TrimSpace(core.StringNilMapper(lifecycle.MasterStatus))
	}
	if endpoints := result.ServiceEndpoints; endpoints != nil {
		cluster.PublicServiceEndpointURL = core.StringNilMapper(endpoints.PublicServiceEndpointURL)
		cluster.PrivateServiceEndpointURL = core.StringNilMapper(endpoints.PrivateServiceEndpointURL)
	}
	if rotation := result.CaCertRotationStatus; rotation != nil {
		cluster.CARotationStatus = core.StringNilMapper(rotation.Status)
		cluster.CARotationCompletedDate = core.StringNilMapper(rotation.ActionCompletedDate)
	}
	return cluster
}

// createClusterOptions maps a validated CreateClusterRequest to the SDK's options. Optional fields are sent only when
// set, so that IBM applies its own defaults; the worker OS is never sent (IBM's defaults are right for both flavors).
func createClusterOptions(req CreateClusterRequest) *kubernetesserviceapiv1.VpcCreateClusterOptions {
	zones := make([]kubernetesserviceapiv1.VPCCreateClusterWorkerPoolZone, 0, len(req.WorkerPool.Zones))
	for _, zone := range req.WorkerPool.Zones {
		zones = append(zones, kubernetesserviceapiv1.VPCCreateClusterWorkerPoolZone{
			ID:       core.StringPtr(zone.Zone),
			SubnetID: core.StringPtr(zone.SubnetID),
		})
	}
	pool := &kubernetesserviceapiv1.VPCCreateClusterWorkerPool{
		Flavor:      core.StringPtr(req.WorkerPool.Flavor),
		VpcID:       core.StringPtr(req.WorkerPool.VPCID),
		WorkerCount: core.Int64Ptr(req.WorkerPool.WorkerCountPerZone),
		Zones:       zones,
	}
	if req.WorkerPool.Name != "" {
		pool.Name = core.StringPtr(req.WorkerPool.Name)
	}
	options := &kubernetesserviceapiv1.VpcCreateClusterOptions{
		XAuthResourceGroup:           core.StringPtr(req.ResourceGroupID),
		Name:                         core.StringPtr(req.Name),
		KubeVersion:                  core.StringPtr(req.KubeVersion),
		Provider:                     core.StringPtr(providerVPCGen2),
		DisablePublicServiceEndpoint: core.BoolPtr(req.DisablePublicServiceEndpoint),
		WorkerPool:                   pool,
	}
	if req.PodSubnet != "" {
		options.PodSubnet = core.StringPtr(req.PodSubnet)
	}
	if req.ServiceSubnet != "" {
		options.ServiceSubnet = core.StringPtr(req.ServiceSubnet)
	}
	if req.COSInstanceCRN != "" {
		options.CosInstanceCRN = core.StringPtr(req.COSInstanceCRN)
	}
	if req.DefaultWorkerPoolEntitlement != "" {
		options.DefaultWorkerPoolEntitlement = core.StringPtr(req.DefaultWorkerPoolEntitlement)
	}
	return options
}

// createClusterResult maps the SDK's createCluster answer, including non_critical_errors, which IBM may send with a
// 201. result has a non-empty ClusterID.
func createClusterResult(result *kubernetesserviceapiv1.CreateClusterResponse) *CreateClusterResult {
	created := &CreateClusterResult{ClusterID: core.StringNilMapper(result.ClusterID)}
	if result.NonCriticalErrors == nil {
		return created
	}
	created.IncidentID = core.StringNilMapper(result.NonCriticalErrors.IncidentID)
	for _, item := range result.NonCriticalErrors.Items {
		created.NonCriticalErrors = append(created.NonCriticalErrors, IBMMessage{
			Code:        core.StringNilMapper(item.Code),
			Type:        core.StringNilMapper(item.Type),
			Description: core.StringNilMapper(item.Description),
			RecoveryCLI: core.StringNilMapper(item.RecoveryCLI),
		})
	}
	return created
}

// releaseVersions maps one list of the versions answer. versions may be nil.
func releaseVersions(versions []kubernetesserviceapiv1.KubeVersion) []ReleaseVersion {
	mapped := make([]ReleaseVersion, 0, len(versions))
	for _, version := range versions {
		mapped = append(mapped, ReleaseVersion{
			Major:        int(int64Value(version.Major)),
			Minor:        int(int64Value(version.Minor)),
			Patch:        int(int64Value(version.Patch)),
			Default:      boolValue(version.Default),
			EndOfService: core.StringNilMapper(version.EndOfService),
		})
	}
	return mapped
}

// int64Value returns *v, or 0 when v is nil.
func int64Value(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// boolValue returns *v, or false when v is nil.
func boolValue(v *bool) bool {
	if v == nil {
		return false
	}
	return *v
}
