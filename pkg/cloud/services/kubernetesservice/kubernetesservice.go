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
	// ListVersions returns the offered Kubernetes and OpenShift versions. SDK GetVersionsWithContext
	// (GET /v1/versions; the SDK's V2GetVersions decodes the wrong shape).
	ListVersions(ctx context.Context) (*Versions, error)
}
