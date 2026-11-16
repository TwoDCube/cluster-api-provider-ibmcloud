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

package managed

import (
	"errors"
	"fmt"
	"strings"

	"github.com/IBM/go-sdk-core/v5/core"

	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api/controllers/clustercache"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"
)

// ScopeParams are the dependencies the managed scopes share. cmd/main.go creates each of them once and the reconcilers
// pass them in explicitly; no package-level variable holds per-cluster or per-identity data. Each scope constructor
// names the dependencies it needs and rejects the ones that are missing; the others may be left unset.
type ScopeParams struct {
	// Client is the manager's (cached) client; the scopes read owner objects with it.
	Client client.Client
	// APIReader reads uncached from the API server, for example the kubeconfig Secret (CAPIBM runs no cluster-wide
	// Secret informer).
	APIReader client.Reader
	// Recorder emits the scopes' events; the scopes, not the controllers, emit events, because they know what happened.
	Recorder record.EventRecorder
	// Resolver resolves the IBM Cloud identity of a Cluster, always from its IBMCloudManagedCluster; in this feature
	// credentials.ControllerOnly.
	Resolver credentials.Resolver
	// ClientBuilder builds every IBM Cloud client for a resolved identity.
	ClientBuilder ClientBuilder
	// Clock is the time source of every time-based decision; a fake clock in tests.
	Clock clock.PassiveClock
	// Prober verifies kubeconfigs against the workload API server.
	Prober workloadprobe.Prober
	// VersionsCache caches IBM's offered versions per identity and region, so that a listing made with one identity's
	// credentials is never served to another.
	VersionsCache *VersionsCache
	// ClusterCache gives the workload-cluster clients of managed Clusters; the pool scope gets a client per reconcile
	// and maps ErrClusterNotConnected to WaitingForWorkloadCluster.
	ClusterCache clustercache.ClusterCache
}

// dependency names one field of ScopeParams in validation errors.
type dependency string

const (
	depClient        dependency = "Client"
	depAPIReader     dependency = "APIReader"
	depRecorder      dependency = "Recorder"
	depResolver      dependency = "Resolver"
	depClientBuilder dependency = "ClientBuilder"
	depClock         dependency = "Clock"
	depProber        dependency = "Prober"
	depVersionsCache dependency = "VersionsCache"
	depClusterCache  dependency = "ClusterCache"
)

// validate returns an error that names every dependency of needs that is unset, or a nil params, so that a reconciler
// built without one fails with a clear message instead of a nil-pointer panic in the middle of a reconcile.
//
// Parameters:
//   - scope names the scope being built, for example "ClusterScope".
//   - needs are the dependencies that scope uses.
func (p *ScopeParams) validate(scope string, needs ...dependency) error {
	if p == nil {
		return fmt.Errorf("managed: %s needs ScopeParams, got nil", scope)
	}
	var missing []string
	for _, need := range needs {
		unset, err := p.unset(need)
		if err != nil {
			return err
		}
		if unset {
			missing = append(missing, string(need))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("managed: %s needs ScopeParams.%s; set them on the reconciler (cmd/main.go creates them once)",
			scope, strings.Join(missing, ", ScopeParams."))
	}
	return nil
}

// unset reports whether the dependency need is unset (nil, also as a typed nil inside an interface). An unknown name
// is a programming error in the calling constructor.
func (p *ScopeParams) unset(need dependency) (bool, error) {
	switch need {
	case depClient:
		return core.IsNil(p.Client), nil
	case depAPIReader:
		return core.IsNil(p.APIReader), nil
	case depRecorder:
		return core.IsNil(p.Recorder), nil
	case depResolver:
		return core.IsNil(p.Resolver), nil
	case depClientBuilder:
		return core.IsNil(p.ClientBuilder), nil
	case depClock:
		return core.IsNil(p.Clock), nil
	case depProber:
		return core.IsNil(p.Prober), nil
	case depVersionsCache:
		return p.VersionsCache == nil, nil
	case depClusterCache:
		return core.IsNil(p.ClusterCache), nil
	default:
		return false, errors.New("managed: unknown ScopeParams dependency " + string(need) + " (programming error)")
	}
}
