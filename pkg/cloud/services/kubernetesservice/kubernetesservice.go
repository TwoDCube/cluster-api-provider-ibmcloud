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
)

//go:generate ../../../../hack/tools/bin/mockgen -source=./kubernetesservice.go -destination=./mock/kubernetesservice_generated.go -package=mock
//go:generate /usr/bin/env bash -c "cat ../../../../hack/scripts/verify/boilerplate/boilerplate.generatego.txt ./mock/kubernetesservice_generated.go > ./mock/_kubernetesservice_generated.go && mv ./mock/_kubernetesservice_generated.go ./mock/kubernetesservice_generated.go"

// KubernetesService is the subset of the IBM Cloud Kubernetes Service API (IKS and ROKS on VPC Gen2) that CAPIBM
// uses. It speaks CAPIBM domain types, so a compat function can be replaced by a regenerated SDK method without
// touching callers. Every method honours ctx and returns *APIError for HTTP failures (classify them with Classify).
// No method reads the environment or global state.
type KubernetesService interface {
	// GetCluster returns a cluster by ID or name. resourceGroupID ("" = account default) scopes a name lookup; it is
	// sent as X-Auth-Resource-Group. SDK VpcGetClusterWithContext (GET /v2/vpc/getCluster).
	GetCluster(ctx context.Context, idOrName, resourceGroupID string) (*Cluster, error)
	// CreateCluster creates a VPC Gen2 cluster. Never retried by the client: a failed POST is reported once and the
	// caller decides. A 201 may carry non-critical errors (observed on IBM Cloud), returned in the result, never as an
	// error. SDK VpcCreateClusterWithContext (POST /v2/vpc/createCluster).
	CreateCluster(ctx context.Context, req CreateClusterRequest) (*CreateClusterResult, error)
	// DeleteCluster deletes a cluster by ID with deleteResources=false: CAPIBM never deletes the VPC resources or the
	// persistent storage of a cluster, because it only references the user's VPC. IBM answers 204 and was observed to
	// report 404 about 5 minutes later. resourceGroupID ("" = none) is sent as X-Auth-Resource-Group. SDK
	// RemoveClusterWithContext (DELETE /v1/clusters/{idOrName}).
	DeleteCluster(ctx context.Context, clusterID, resourceGroupID string) error
	// ListVersions returns the offered Kubernetes and OpenShift versions. SDK GetVersionsWithContext
	// (GET /v1/versions; the SDK's V2GetVersions decodes the wrong shape).
	ListVersions(ctx context.Context) (*Versions, error)

	// GetAdminKubeconfig returns the parts of IBM's admin kubeconfig (getKubeconfig?admin=true&format=json): the
	// server, the CA (IKS only) and the admin client certificate and key. It rejects a user entry that uses a token, an
	// exec plugin, an auth provider or a password: the admin certificate is the only credential CAPIBM accepts, never a
	// token or the OpenShift OAuth server. A compat function (compat.go).
	GetAdminKubeconfig(ctx context.Context, clusterID string) (*AdminKubeconfig, error)
	// GetCACertificate returns the cluster CA (PEM) and IBM's rotatedAt value. It never returns a credential, so the
	// ownership proof and the CA re-check may call it. A compat function (compat.go).
	GetCACertificate(ctx context.Context, clusterID string) (*CACertificate, error)

	// ListWorkerPools returns every pool of the cluster. The SDK method calls GET /v2/getWorkerPools, whose array shape
	// on VPC clusters only the swagger documents (GetCombinedWorkerPoolResponse; same zone schema as the VPC one); it is
	// not yet verified against IBM Cloud. GET /v2/vpc/getWorkerPools was observed to answer an array; fallback: a compat
	// decoder for that path, because the SDK's VpcGetWorkerPools decodes a single object (ListWorkerPoolsViaSDK). SDK
	// GetWorkerPools1WithContext.
	ListWorkerPools(ctx context.Context, clusterID string) ([]WorkerPool, error)
	// CreateWorkerPool creates a pool with its zones and subnets in one call and returns the new pool's ID. Never
	// retried by the client. A compat function (POST /v2/vpc/createWorkerPool); its fallback behind CreatePoolWithZones
	// is a zone-less SDK create plus one SDK zone add per zone.
	CreateWorkerPool(ctx context.Context, req CreateWorkerPoolRequest) (string, error)
	// ResizeWorkerPool sets the workers per zone of a pool; IBM picks the workers it removes when shrinking.
	// allowSingleOpenShiftWorker is never sent. IBM refuses a size below the cluster minimum with E1437 (observed on IBM
	// Cloud). SDK V2ResizeWorkerPoolWithContext (POST /v2/resizeWorkerPool).
	ResizeWorkerPool(ctx context.Context, clusterID, poolID string, sizePerZone int64) error

	// ListWorkers returns every worker of the cluster, including deleted ones (showDeleted=true), so that a worker
	// leaves providerIDList on IBM's "deleted" record rather than on its absence from one listing: dropping a live
	// worker's ID would make Cluster API delete its Node. SDK VpcGetWorkersWithContext (GET /v2/vpc/getWorkers).
	ListWorkers(ctx context.Context, clusterID string) ([]Worker, error)
	// ReplaceWorker replaces one worker; CAPIBM always sends update=true (newest fix pack of the master minor).
	// POST /v2/vpc/replaceWorker {cluster, update, workerID}; IBM answers 204 (observed on IBM Cloud). SDK
	// VpcReplaceWorkerWithContext.
	ReplaceWorker(ctx context.Context, clusterID, workerID string, update bool) error
}
