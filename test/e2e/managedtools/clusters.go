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
	"context"
	"fmt"
	"net/http"
	"regexp"

	"github.com/IBM-Cloud/container-services-go-sdk/kubernetesserviceapiv1"
	"github.com/IBM/go-sdk-core/v5/core"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// Cluster is one IBM Cloud Kubernetes Service cluster (IKS or ROKS on VPC Gen2) as the cluster list returns it, with
// only the fields the janitor and the inventory diff need.
type Cluster struct {
	// ID is IBM's cluster ID, 20 lowercase letters and digits, for example "c0abcdefghijklmnopqr".
	ID string
	// Name is the cluster name, for example "capibm-e2e-k3x9q2-iks".
	Name string
	// CreatedDate is IBM's raw creation time, for example "2026-11-16T03:05:33+0000" (the form observed on IBM Cloud).
	CreatedDate string
	// State is the cluster state, for example "deploying", "normal" or "deleting".
	State string
	// ResourceGroupID is the ID of the cluster's resource group.
	ResourceGroupID string
}

// ClusterLister lists IBM Cloud Kubernetes Service clusters; the inventory needs only this. ClusterService implements
// it.
type ClusterLister interface {
	// ListClusters returns every VPC Gen2 cluster the credentials can see; when a resource group is set, only the
	// clusters whose resource group is that one, whatever IBM's answer contains.
	ListClusters(ctx context.Context) ([]Cluster, error)
}

// ClusterAPI is what the janitor needs from IBM Cloud Kubernetes Service. ClusterService implements it.
type ClusterAPI interface {
	ClusterLister
	// DeleteCluster deletes one cluster by its IBM cluster ID; a name is refused (see ValidateClusterID). storage is
	// what the DELETE asks IBM to do with the cluster's persistent storage; a value that is neither choice is refused
	// (see deleteResourcesQuery).
	DeleteCluster(ctx context.Context, clusterID string, storage kubernetesservice.PersistentStorage) error
}

// PersistentStorageVariable is the e2e configuration variable that the managed flavor templates read into the control
// plane's spec.deletion.persistentStorage.
const PersistentStorageVariable = "DELETION_PERSISTENT_STORAGE"

// ValidateLanePersistentStorage returns an error unless value, the value of PersistentStorageVariable, is "Delete". The
// templates default to Retain, which asks IBM to keep the persistent storage of a deleted cluster, but every e2e
// cluster is a test cluster: a volume it left behind would stay billed until someone deletes it by hand. Delete only
// asks IBM to delete the storage; IBM's documentation conflicts on whether VPC Block volumes go with the cluster.
//
// Parameters:
//   - value is the variable's value as the lane reads it from the e2e configuration or the environment.
func ValidateLanePersistentStorage(value string) error {
	if value != string(controlplanev1.PersistentStorageDelete) {
		return fmt.Errorf("%s is %q; the managed lanes need %q (test/e2e/config/ibmcloud-e2e-managed.yaml sets it), so "+
			"that the DELETE of every e2e cluster asks IBM to delete its persistent storage instead of keeping it "+
			"billed; unset it in the environment", PersistentStorageVariable, value,
			controlplanev1.PersistentStorageDelete)
	}
	return nil
}

// deleteResourcesQuery returns IBM's deleteResources query value for storage: "false" asks IBM to keep the cluster's
// persistent storage, "true" asks IBM to delete it (on VPC nothing else: IBM removes subnets with a cluster only on
// classic infrastructure). Any other value is an error, so that no DELETE leaves the storage to IBM's default or to a
// forgotten argument.
//
// Parameters:
//   - storage is the persistent storage choice of the deletion.
func deleteResourcesQuery(storage kubernetesservice.PersistentStorage) (string, error) {
	switch storage {
	case kubernetesservice.RetainPersistentStorage:
		return "false", nil
	case kubernetesservice.DeletePersistentStorage:
		return "true", nil
	default:
		return "", fmt.Errorf("the persistent storage choice %q is neither %q nor %q; the janitor and the backstop ask "+
			"IBM to delete the storage of every e2e cluster they delete", storage,
			kubernetesservice.RetainPersistentStorage, kubernetesservice.DeletePersistentStorage)
	}
}

// clusterIDPattern accepts IBM cluster IDs: lowercase letters and digits only (20 of them in every ID observed on IBM
// Cloud so far; the upper bound leaves room for a longer form). Every e2e cluster name contains at least two hyphens
// ("<prefix>-<run-id>-<lane>"), so no e2e cluster name can pass it: that is the structural guarantee behind "the
// janitor deletes by ID, never by name", because IBM's delete call accepts both.
var clusterIDPattern = regexp.MustCompile(`^[a-z0-9]{1,64}$`)

// providerVPCGen2 is the only cluster provider the managed lanes create (the managed kinds support VPC Gen2 only).
const providerVPCGen2 = "vpc-gen2"

// ValidateClusterID returns an error unless id has the form of an IBM cluster ID.
//
// Parameters:
//   - id is the value about to be sent as the cluster of a delete call.
func ValidateClusterID(id string) error {
	if !clusterIDPattern.MatchString(id) {
		return fmt.Errorf("%q is not an IBM cluster ID (lowercase letters and digits, for example "+
			"\"c0abcdefghijklmnopqr\"); the e2e tooling deletes clusters only by ID, never by name", id)
	}
	return nil
}

// ClusterService implements ClusterAPI on the IBM Cloud Kubernetes Service API. Create it with NewClusterService.
type ClusterService struct {
	// sdk is the generated SDK client, built with the explicit URL, authenticator and HTTP client.
	sdk *kubernetesserviceapiv1.KubernetesServiceApiV1
	// resourceGroupID is sent as X-Auth-Resource-Group when not empty.
	resourceGroupID string
}

var _ ClusterAPI = &ClusterService{}

// NewClusterService validates opts and builds the client. It makes no request.
//
// It uses kubernetesserviceapiv1.NewKubernetesServiceApiV1 (never the UsingExternalConfig variant, which reads the
// environment), installs opts.HTTPClient and leaves the SDK's retries off: a resent DELETE is decided by the caller.
//
// Parameters:
//   - opts are the URL (https://containers.cloud.ibm.com/global), the authenticator, the HTTP client and the
//     optional resource group; see ClientOptions.
func NewClusterService(opts ClientOptions) (*ClusterService, error) {
	const service = "IBM Cloud Kubernetes Service"
	if err := opts.validate(service); err != nil {
		return nil, err
	}
	sdk, err := kubernetesserviceapiv1.NewKubernetesServiceApiV1(&kubernetesserviceapiv1.KubernetesServiceApiV1Options{
		URL:           opts.URL,
		Authenticator: opts.Authenticator,
	})
	if err != nil {
		return nil, fmt.Errorf("%s client: building the SDK client: %w", service, err)
	}
	sdk.Service.SetHTTPClient(opts.HTTPClient)
	sdk.Service.SetDefaultHeaders(http.Header{"User-Agent": []string{DefaultUserAgent}})
	return &ClusterService{sdk: sdk, resourceGroupID: opts.ResourceGroupID}, nil
}

// ListClusters implements ClusterAPI with GET /v2/vpc/getClusters?provider=vpc-gen2.
//
// With a resource group set, the request carries it as X-Auth-Resource-Group, and the answer is filtered here as
// well: only clusters whose resource group equals it are returned, and a cluster IBM sent without a resource group is
// dropped. Nothing observed on IBM Cloud shows that IBM honours the header on this call, and the janitor deletes
// whatever this listing returns that matches its name and age rules, so the scope must not rest on the header alone
// (defence in depth; VPCService filters the same way).
func (s *ClusterService) ListClusters(ctx context.Context) ([]Cluster, error) {
	options := &kubernetesserviceapiv1.VpcGetClustersOptions{Provider: core.StringPtr(providerVPCGen2)}
	if s.resourceGroupID != "" {
		options.XAuthResourceGroup = core.StringPtr(s.resourceGroupID)
	}
	result, response, err := s.sdk.VpcGetClustersWithContext(ctx, options)
	if err != nil {
		return nil, newAPIError("ListClusters", response, err)
	}
	clusters := make([]Cluster, 0, len(result))
	for i := range result {
		cluster := Cluster{
			ID:              core.StringNilMapper(result[i].ID),
			Name:            core.StringNilMapper(result[i].Name),
			CreatedDate:     core.StringNilMapper(result[i].CreatedDate),
			State:           core.StringNilMapper(result[i].State),
			ResourceGroupID: core.StringNilMapper(result[i].ResourceGroup),
		}
		if s.resourceGroupID != "" && cluster.ResourceGroupID != s.resourceGroupID {
			continue
		}
		clusters = append(clusters, cluster)
	}
	return clusters, nil
}

// DeleteCluster implements ClusterAPI with DELETE /v1/clusters/{clusterID}?deleteResources=<false|true>. It refuses,
// without a request, every value that is not an IBM cluster ID (ValidateClusterID) and every storage choice that is
// neither Retain nor Delete (deleteResourcesQuery). On VPC, deleteResources=true asks IBM to delete only the cluster's
// persistent storage, so the VPC resources stay either way and the inventory diff reports what IBM leaves behind.
//
// Parameters:
//   - ctx bounds the request.
//   - clusterID is the IBM cluster ID from a ListClusters result, never its name.
//   - storage is what the DELETE asks IBM to do with the cluster's persistent storage: RetainPersistentStorage sends
//     deleteResources=false, DeletePersistentStorage deleteResources=true.
func (s *ClusterService) DeleteCluster(ctx context.Context, clusterID string, storage kubernetesservice.PersistentStorage) error {
	if err := ValidateClusterID(clusterID); err != nil {
		return err
	}
	deleteResources, err := deleteResourcesQuery(storage)
	if err != nil {
		return err
	}
	options := &kubernetesserviceapiv1.RemoveClusterOptions{
		IdOrName:        core.StringPtr(clusterID),
		DeleteResources: core.StringPtr(deleteResources),
	}
	if s.resourceGroupID != "" {
		options.XAuthResourceGroup = core.StringPtr(s.resourceGroupID)
	}
	response, err := s.sdk.RemoveClusterWithContext(ctx, options)
	if err != nil {
		return newAPIError("DeleteCluster", response, err)
	}
	return nil
}
