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

// The compat layer. Each function works around one defect of the pinned SDK on the SDK client's own BaseService, so
// it inherits the URL, the authenticator, the User-Agent, the limited transport and the error model, and maps IBM's
// answer to a domain type. Each carries a COMPAT header naming the defect and when to delete it; deleting one changes
// no caller. The SDK gaps are recorded in the project's own documentation; nothing is filed upstream.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/IBM-Cloud/container-services-go-sdk/kubernetesserviceapiv1"
	"github.com/IBM/go-sdk-core/v5/core"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// maxKubeconfigBytes caps the admin kubeconfig answer at 1 MiB. IBM's answer was observed to be a few KiB; the cap
// bounds the memory a misbehaving endpoint can make CAPIBM allocate.
const maxKubeconfigBytes = 1 << 20

// GetAdminKubeconfig implements KubernetesService.
//
// COMPAT(container-services-go-sdk@61af133): GetKubeconfig discards the response body (Request(request, nil)),
// requires XAuthRefreshToken (validate:"required") and has no endpointType. Delete when GetKubeconfigWithContext
// returns the body without a refresh token, and the replacement still keeps the body out of go-sdk-core's debug log;
// callers do not change.
//
// The request is the one exception to "execute through BaseService.Request": at debug level Request dumps whole
// response bodies, and this body holds the cluster-admin private key. So the request is built with
// core.NewRequestBuilder on the BaseService URL, signed with the BaseService authenticator, given the User-Agent,
// and executed with the BaseService http.Client (the limited transport). The body is read through a 1 MiB cap,
// parsed with clientcmd.Load (JSON is YAML), and never logged or quoted in an error.
func (s *Service) GetAdminKubeconfig(ctx context.Context, clusterID string) (*AdminKubeconfig, error) {
	if clusterID == "" {
		return nil, invalidRequest(OpGetAdminKubeconfig, "the cluster ID is empty; pass status.clusterID of the IBMCloudManagedControlPlane")
	}
	ctx, tracker := trackCall(ctx)
	builder := core.NewRequestBuilder(core.GET).WithContext(ctx)
	if _, err := builder.ResolveRequestURL(s.sdk.Service.GetServiceURL(), "/v2/getKubeconfig", nil); err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: building the request: %w", OpGetAdminKubeconfig, err)
	}
	builder.AddQuery("cluster", clusterID)
	builder.AddQuery("admin", "true")
	// format=json returns the admin client certificate and key inline (observed on IBM Cloud;
	// assumptions.AdminKubeconfigFormat).
	builder.AddQuery("format", "json")
	builder.AddHeader("Accept", "application/json")
	request, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: building the request: %w", OpGetAdminKubeconfig, err)
	}
	request.Header.Set("User-Agent", s.userAgent)
	if err := s.sdk.Service.Options.Authenticator.Authenticate(request); err != nil {
		return nil, authenticationError(OpGetAdminKubeconfig, err)
	}
	response, err := s.sdk.Service.GetHTTPClient().Do(request)
	if err != nil {
		return nil, callError(OpGetAdminKubeconfig, tracker, nil, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxKubeconfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: reading IBM's answer: %w", OpGetAdminKubeconfig, err)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, newAPIError(OpGetAdminKubeconfig, errorResponse(response, body))
	}
	if len(body) > maxKubeconfigBytes {
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's answer exceeds %d bytes; refusing it", OpGetAdminKubeconfig, maxKubeconfigBytes)
	}
	return parseAdminKubeconfig(body)
}

// errorResponse wraps a non-2xx answer of the compat path that bypasses BaseService.Request into the
// DetailedResponse shape newAPIError reads: a JSON object body goes into Result, as go-sdk-core does.
//
// Parameters:
//   - response is the answer.
//   - body is its (capped) body.
func errorResponse(response *http.Response, body []byte) *core.DetailedResponse {
	detailed := &core.DetailedResponse{StatusCode: response.StatusCode, Headers: response.Header}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err == nil {
		detailed.Result = decoded
	}
	return detailed
}

// parseAdminKubeconfig extracts the server, CA, client certificate and key of the current context from IBM's admin
// kubeconfig. No error quotes body: it holds a private key. body is IBM's JSON kubeconfig.
func parseAdminKubeconfig(body []byte) (*AdminKubeconfig, error) {
	config, err := clientcmd.Load(body)
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's answer (%d bytes) is not a kubeconfig", OpGetAdminKubeconfig, len(body))
	}
	cluster, user, err := currentClusterAndUser(config)
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's admin kubeconfig %w", OpGetAdminKubeconfig, err)
	}
	if user.Token != "" || user.TokenFile != "" || user.Exec != nil || user.AuthProvider != nil || user.Username != "" || user.Password != "" {
		// The admin client certificate is the only credential CAPIBM ever stores; never a token, plugin or OAuth path.
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's admin kubeconfig authenticates with a token, an exec plugin, an auth provider or a password; CAPIBM accepts only the admin client certificate", OpGetAdminKubeconfig)
	}
	if len(user.ClientCertificateData) == 0 || len(user.ClientKeyData) == 0 {
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's admin kubeconfig has no client-certificate-data and client-key-data; CAPIBM needs IBM's admin client certificate", OpGetAdminKubeconfig)
	}
	if cluster.Server == "" {
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's admin kubeconfig has no server", OpGetAdminKubeconfig)
	}
	if cluster.InsecureSkipTLSVerify {
		return nil, fmt.Errorf("kubernetesservice: %s: IBM's admin kubeconfig disables TLS verification; CAPIBM never connects insecurely", OpGetAdminKubeconfig)
	}
	admin := &AdminKubeconfig{
		Server:         cluster.Server,
		CertificatePEM: user.ClientCertificateData,
		KeyPEM:         user.ClientKeyData,
	}
	if len(cluster.CertificateAuthorityData) > 0 {
		admin.CertificateAuthorityPEM = cluster.CertificateAuthorityData
	}
	return admin, nil
}

// currentClusterAndUser returns the cluster and user of config's current context. When current-context is empty and
// config has exactly one context, that context is used: IBM's answer was observed with one context but no
// current-context, and a single context is unambiguous. The error completes the sentence "IBM's admin kubeconfig ...".
func currentClusterAndUser(config *clientcmdapi.Config) (*clientcmdapi.Cluster, *clientcmdapi.AuthInfo, error) {
	contextName := config.CurrentContext
	if contextName == "" && len(config.Contexts) == 1 {
		for name := range config.Contexts {
			contextName = name
		}
	}
	kubeContext, ok := config.Contexts[contextName]
	if contextName == "" || !ok || kubeContext == nil {
		return nil, nil, fmt.Errorf("has no usable current context (current-context %q, %d contexts)", config.CurrentContext, len(config.Contexts))
	}
	cluster, ok := config.Clusters[kubeContext.Cluster]
	if !ok || cluster == nil {
		return nil, nil, fmt.Errorf("context %q names cluster %q, which it does not contain", contextName, kubeContext.Cluster)
	}
	user, ok := config.AuthInfos[kubeContext.AuthInfo]
	if !ok || user == nil {
		return nil, nil, fmt.Errorf("context %q names user %q, which it does not contain", contextName, kubeContext.AuthInfo)
	}
	return cluster, user, nil
}

// caCertificateResponse is the body of GET /v2/getCACert, as observed on IBM Cloud.
type caCertificateResponse struct {
	// CACert is the base64-encoded PEM CA bundle.
	CACert string `json:"caCert"`
	// RotatedAt is IBM's rotation marker; kept raw because only the key, not the type of its value, was observed.
	RotatedAt json.RawMessage `json:"rotatedAt"`
}

// GetCACertificate implements KubernetesService.
//
// COMPAT(container-services-go-sdk@61af133): the SDK has no getCACert operation. Delete when the SDK adds getCACert;
// callers do not change.
//
// The answer holds no credential, so it goes through BaseService.Request like every SDK call.
func (s *Service) GetCACertificate(ctx context.Context, clusterID string) (*CACertificate, error) {
	if clusterID == "" {
		return nil, invalidRequest(OpGetCACertificate, "the cluster ID is empty; pass the IBM cluster ID")
	}
	ctx, tracker := trackCall(ctx)
	builder := core.NewRequestBuilder(core.GET).WithContext(ctx)
	if _, err := builder.ResolveRequestURL(s.sdk.Service.GetServiceURL(), "/v2/getCACert", nil); err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: building the request: %w", OpGetCACertificate, err)
	}
	builder.AddQuery("cluster", clusterID)
	builder.AddHeader("Accept", "application/json")
	request, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: building the request: %w", OpGetCACertificate, err)
	}
	var answer caCertificateResponse
	response, err := s.sdk.Service.Request(request, &answer)
	if err != nil {
		return nil, callError(OpGetCACertificate, tracker, response, err)
	}
	certificatePEM, err := decodeCACert(answer.CACert)
	if err != nil {
		return nil, fmt.Errorf("kubernetesservice: %s: %w", OpGetCACertificate, err)
	}
	return &CACertificate{PEM: certificatePEM, RotatedAt: rawText(answer.RotatedAt)}, nil
}

// decodeCACert decodes getCACert's caCert field: base64 of a PEM bundle whose first block is a certificate.
// encoded is the field's value.
func decodeCACert(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, fmt.Errorf("IBM's answer has no caCert")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("IBM's caCert is not base64: %w", err)
	}
	block, _ := pem.Decode(decoded)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("IBM's caCert does not hold a PEM certificate")
	}
	return decoded, nil
}

// createWorkerPoolBody is the body of POST /v2/vpc/createWorkerPool as CAPIBM sends it. It has no operatingSystem:
// IBM's defaults are right for both flavors (observed on IBM Cloud: UBUNTU_24_64 on IKS, RHCOS on ROKS).
type createWorkerPoolBody struct {
	Cluster     string                 `json:"cluster"`
	Name        string                 `json:"name"`
	Flavor      string                 `json:"flavor"`
	VPCID       string                 `json:"vpcID"`
	WorkerCount int64                  `json:"workerCount"`
	Zones       []createWorkerPoolZone `json:"zones"`
	Entitlement string                 `json:"entitlement,omitempty"`
}

// createWorkerPoolZone is one zone of createWorkerPoolBody, with the subnet the SDK's Zone model lacks.
type createWorkerPoolZone struct {
	ID       string `json:"id"`
	SubnetID string `json:"subnetID"`
}

// createWorkerPoolResponse is the 201 body of createWorkerPool (swagger CreateWorkerpoolResponse).
type createWorkerPoolResponse struct {
	WorkerPoolID string `json:"workerPoolID"`
}

// CreatePoolWithZones selects how CreateWorkerPool creates a pool. Not yet verified against IBM Cloud: true sends one
// POST /v2/vpc/createWorkerPool with zones[].subnetID, the call Terraform relies on, which never leaves a pool without
// its zones behind. Fallback if this call turns out not to provision the workers: set it to false, and
// CreateWorkerPool creates the pool without zones through the SDK's VpcCreateWorkerPool and then adds each zone with
// its subnet through the SDK's VpcCreateWorkerPoolZone (createWorkerPoolZoneByZone). Exported so that the managed
// scope's assumptions can list it with the other decision points that are not yet verified against IBM Cloud.
const CreatePoolWithZones = true

// CreateWorkerPool implements KubernetesService. It validates req and sends nothing when req is invalid.
//
// COMPAT(container-services-go-sdk@61af133): the Zone of VpcCreateWorkerPool has no subnetID, so the SDK cannot
// create a pool with its zones and subnets in one call. Delete when VpcCreateWorkerPool zones carry subnetID,
// together with the zone-by-zone fallback, which then has no reason left; callers do not change.
//
// CreatePoolWithZones selects the call: the single call is the default and the zone-less create plus one zone add per
// zone is the inactive fallback; switching is the one-line change of that constant. What the switch cannot give is the
// repair of a pool the fallback left half created (the create succeeded, then a zone add failed): the pool flow adopts
// the pool by name on its next reconcile and has no way to add the missing zones, because adding a zone to an
// existing pool is neither a KubernetesService method nor a controller step. That repair needs AddWorkerPoolZone in
// the interface and in the controller, which arrive with the support for adding and removing zones.
func (s *Service) CreateWorkerPool(ctx context.Context, req CreateWorkerPoolRequest) (string, error) {
	if err := req.validate(); err != nil {
		return "", err
	}
	if !CreatePoolWithZones {
		return s.createWorkerPoolZoneByZone(ctx, req)
	}
	ctx, tracker := trackCall(ctx)
	builder := core.NewRequestBuilder(core.POST).WithContext(ctx)
	if _, err := builder.ResolveRequestURL(s.sdk.Service.GetServiceURL(), "/v2/vpc/createWorkerPool", nil); err != nil {
		return "", fmt.Errorf("kubernetesservice: %s: building the request: %w", OpCreateWorkerPool, err)
	}
	builder.AddHeader("Accept", "application/json")
	builder.AddHeader("Content-Type", "application/json")
	if _, err := builder.SetBodyContentJSON(createWorkerPoolBodyFrom(req)); err != nil {
		return "", fmt.Errorf("kubernetesservice: %s: encoding the body: %w", OpCreateWorkerPool, err)
	}
	request, err := builder.Build()
	if err != nil {
		return "", fmt.Errorf("kubernetesservice: %s: building the request: %w", OpCreateWorkerPool, err)
	}
	var answer createWorkerPoolResponse
	response, err := s.sdk.Service.Request(request, &answer)
	if err != nil {
		return "", callError(OpCreateWorkerPool, tracker, response, err)
	}
	if answer.WorkerPoolID == "" {
		// Like CreateCluster: without an ID the attempt's outcome is unknown; the pool flow adopts by name.
		return "", fmt.Errorf("kubernetesservice: %s: IBM answered HTTP %d without a workerPoolID; the outcome of this create attempt is unknown", OpCreateWorkerPool, statusCode(response))
	}
	return answer.WorkerPoolID, nil
}

// createWorkerPoolBodyFrom maps a validated CreateWorkerPoolRequest to the wire body.
func createWorkerPoolBodyFrom(req CreateWorkerPoolRequest) createWorkerPoolBody {
	zones := make([]createWorkerPoolZone, 0, len(req.Zones))
	for _, zone := range req.Zones {
		zones = append(zones, createWorkerPoolZone{ID: zone.Zone, SubnetID: zone.SubnetID})
	}
	return createWorkerPoolBody{
		Cluster:     req.ClusterID,
		Name:        req.Name,
		Flavor:      req.Flavor,
		VPCID:       req.VPCID,
		WorkerCount: req.WorkerCountPerZone,
		Zones:       zones,
		Entitlement: req.Entitlement,
	}
}

// createWorkerPoolZoneByZone is the fallback of CreateWorkerPool, inactive while CreatePoolWithZones is true. It
// creates the pool without zones through the SDK's VpcCreateWorkerPool (POST /v2/vpc/createWorkerPool) and then adds
// each zone with its subnet through the SDK's VpcCreateWorkerPoolZone (POST /v2/vpc/createWorkerPoolZone {cluster,
// id, subnetID, workerpool}), in the order of req.Zones. Like the single call it sends each request once and never
// retries.
//
// When a zone add fails, the pool exists with only the zones added before it. The error then names the pool, its ID
// and the zones it lacks, and wraps the zone add's own error, so Classify applies its CreateWorkerPool and status rows
// to it. IBM's 409 "zone already exists in the worker pool" cannot answer a zone add here, because every zone is added
// to the pool this call has just created.
//
// Parameters:
//   - ctx bounds every request.
//   - req is the pool to create; CreateWorkerPool has validated it.
func (s *Service) createWorkerPoolZoneByZone(ctx context.Context, req CreateWorkerPoolRequest) (string, error) {
	createCtx, tracker := trackCall(ctx)
	options := &kubernetesserviceapiv1.VpcCreateWorkerPoolOptions{
		Cluster:     core.StringPtr(req.ClusterID),
		Name:        core.StringPtr(req.Name),
		Flavor:      core.StringPtr(req.Flavor),
		VpcID:       core.StringPtr(req.VPCID),
		WorkerCount: core.Int64Ptr(req.WorkerCountPerZone),
		// Zones stay nil: the SDK's Zone has no subnetID, so every zone is added below with its subnet. The SDK has no
		// operatingSystem option, which matches the single call (IBM's defaults are right for both flavors).
	}
	if req.Entitlement != "" {
		options.Entitlement = core.StringPtr(req.Entitlement)
	}
	result, response, err := s.sdk.VpcCreateWorkerPoolWithContext(createCtx, options)
	if err != nil {
		return "", callError(OpCreateWorkerPool, tracker, response, err)
	}
	if result == nil || core.StringNilMapper(result.WorkerPoolID) == "" {
		// Like the single call: without an ID the attempt's outcome is unknown; the pool flow adopts by name.
		return "", fmt.Errorf("kubernetesservice: %s: IBM answered HTTP %d without a workerPoolID; the outcome of this create attempt is unknown", OpCreateWorkerPool, statusCode(response))
	}
	poolID := *result.WorkerPoolID
	for i, zone := range req.Zones {
		zoneCtx, zoneTracker := trackCall(ctx)
		response, err := s.sdk.VpcCreateWorkerPoolZoneWithContext(zoneCtx, &kubernetesserviceapiv1.VpcCreateWorkerPoolZoneOptions{
			Cluster:    core.StringPtr(req.ClusterID),
			Workerpool: core.StringPtr(poolID),
			ID:         core.StringPtr(zone.Zone),
			SubnetID:   core.StringPtr(zone.SubnetID),
		})
		if err != nil {
			// The message tells the operator what exists and how to recover, because CAPIBM cannot add the zones
			// itself until it supports adding zones to a pool (see CreateWorkerPool).
			return "", fmt.Errorf("kubernetesservice: %s: IBM created pool %q with ID %s, but adding zone %s with subnet %s failed, so the pool lacks the zones %s; add them to the pool in IBM Cloud, or delete the pool there so that it is created again: %w",
				OpCreateWorkerPool, req.Name, poolID, zone.Zone, zone.SubnetID, zoneNames(req.Zones[i:]), callError(OpCreateWorkerPool, zoneTracker, response, err))
		}
	}
	return poolID, nil
}

// zoneNames returns the zone names of zones as a comma-separated list, for messages. zones are zone-subnet pairs.
func zoneNames(zones []ZoneSubnet) string {
	names := make([]string, 0, len(zones))
	for _, zone := range zones {
		names = append(names, zone.Zone)
	}
	return strings.Join(names, ", ")
}

// rawText returns a JSON value as text: a string without quotes, null or nothing as "", anything else as its JSON
// text. raw is the undecoded value.
func rawText(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		return text
	}
	return string(trimmed)
}
