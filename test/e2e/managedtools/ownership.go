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
	"slices"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file holds the facts by which the IKS lane's move step proves that CAPIBM on the clusterctl move target
// re-adopted the cluster without writing to IBM. clusterctl move recreates every object from its spec and drops its
// status, so a fact read only from a copied spec field would match whatever the target does; the facts therefore pair
// what CAPIBM records (status IDs it must rediscover, spec.providerIDList it rewrites from IBM) with what IBM itself
// lists for the pools' workers.
//
// The step reads the facts again and again for a while after the re-adoption (a hold), and each read asks IBM once:
// CAPIBM's client sends one request per call and never retries. A hold fails on the first read that fails, so it must
// tell an IBM answer that may pass, on which CAPIBM's controllers simply requeue, from a fact that changed or an
// answer that will not change; the error helpers at the end of this file draw that line.

// OwnershipFacts are the facts about one cluster that a clusterctl move must not change: the target re-adopts the same
// IBM cluster and pools and removes, replaces or adds no worker. Two values compare with reflect.DeepEqual (Gomega's
// Equal); CollectOwnershipFacts builds them in one canonical form, sorted and with non-nil slices.
type OwnershipFacts struct {
	// ClusterID is the control plane's status.clusterID.
	ClusterID string
	// MasterMinor is the MAJOR.MINOR of the control plane's status.distributionVersion. The fix pack is left out on
	// purpose: IBM applies master patches on its own, and one can land between the facts taken on the source and those
	// taken on the target, while a minor change would be a write by CAPIBM.
	MasterMinor string
	// Pools maps each IBMCloudManagedMachinePool name to its pool's facts.
	Pools map[string]PoolFacts
}

// PoolFacts are one pool's ownership facts.
type PoolFacts struct {
	// PoolID is the pool's status.poolID.
	PoolID string
	// ProviderIDs is the pool's spec.providerIDList, sorted: CAPIBM's record of the pool's workers.
	ProviderIDs []string
	// Workers are the IDs of the IBM workers of the pool that IBM has not deleted, sorted: IBM's own record. A worker
	// that IBM is still deleting counts, and a replacement counts from the moment IBM lists it, so a replace or a
	// resize shows here before CAPIBM's providerIDList follows it.
	Workers []string
}

// CollectOwnershipFacts builds the ownership facts of one cluster. Every ID must be set, so that two sets of facts read
// before CAPIBM has (re)discovered the IBM objects can never compare equal. It is pure.
//
// Parameters:
//   - cp is the cluster's IBMCloudManagedControlPlane; it must have status.clusterID and a status.distributionVersion
//     that parses as an IBM version.
//   - pools are every IBMCloudManagedMachinePool of the cluster, at least one; each must have status.poolID, and no
//     two may share a name or a pool ID.
//   - workers is IBM's worker list of the cluster (KubernetesService.ListWorkers, deleted records included); workers
//     of pools that are not in pools, such as a pool deleted earlier, are ignored.
func CollectOwnershipFacts(cp *controlplanev1.IBMCloudManagedControlPlane, pools []infrav1.IBMCloudManagedMachinePool,
	workers []kubernetesservice.Worker) (OwnershipFacts, error) {
	if cp == nil {
		return OwnershipFacts{}, errors.New("no control plane: pass the cluster's IBMCloudManagedControlPlane")
	}
	if cp.Status.ClusterID == "" {
		return OwnershipFacts{}, fmt.Errorf("control plane %s has no status.clusterID yet", cp.Name)
	}
	minor, err := IBMVersionMinor(cp.Status.DistributionVersion)
	if err != nil {
		return OwnershipFacts{}, fmt.Errorf("control plane %s status.distributionVersion: %w", cp.Name, err)
	}
	if len(pools) == 0 {
		return OwnershipFacts{}, errors.New("no IBMCloudManagedMachinePool: pass every pool of the cluster, so that " +
			"the facts cover its workers")
	}
	facts := OwnershipFacts{ClusterID: cp.Status.ClusterID, MasterMinor: minor, Pools: make(map[string]PoolFacts, len(pools))}
	poolNames := make(map[string]string, len(pools))
	for i := range pools {
		pool := &pools[i]
		if pool.Status.PoolID == "" {
			return OwnershipFacts{}, fmt.Errorf("pool %s has no status.poolID yet", pool.Name)
		}
		if _, dup := facts.Pools[pool.Name]; dup {
			return OwnershipFacts{}, fmt.Errorf("pool %s is listed twice", pool.Name)
		}
		if other, dup := poolNames[pool.Status.PoolID]; dup {
			return OwnershipFacts{}, fmt.Errorf("pools %s and %s have the same status.poolID %s", other, pool.Name,
				pool.Status.PoolID)
		}
		poolNames[pool.Status.PoolID] = pool.Name
		ids := slices.Clone(pool.Spec.ProviderIDList)
		if ids == nil {
			ids = []string{}
		}
		slices.Sort(ids)
		facts.Pools[pool.Name] = PoolFacts{PoolID: pool.Status.PoolID, ProviderIDs: ids, Workers: []string{}}
	}
	for _, worker := range workers {
		name, ok := poolNames[worker.PoolID]
		if !ok || worker.ActualState == managed.WorkerStateDeleted {
			continue
		}
		pool := facts.Pools[name]
		pool.Workers = append(pool.Workers, worker.ID)
		facts.Pools[name] = pool
	}
	for name, pool := range facts.Pools {
		slices.Sort(pool.Workers)
		facts.Pools[name] = pool
	}
	return facts, nil
}

// ErrIBMWorkerListing marks an error that IBM's worker listing returned while ownership facts were read
// (IBMWorkerListingError), as opposed to an error of the management cluster or of the facts themselves, so that a
// check can tell an IBM answer that may pass from a broken fact (OwnershipFactsMayPass).
var ErrIBMWorkerListing = errors.New("listing the workers of IBM cluster")

// IBMWorkerListingError marks err, the error of KubernetesService.ListWorkers for the cluster clusterID, with
// ErrIBMWorkerListing. The message reads "listing the workers of IBM cluster <clusterID>: <err>", and err stays
// reachable through errors.Is and errors.As, which kubernetesservice.Classify needs. A nil err gives nil.
//
// Parameters:
//   - clusterID is the IBM cluster whose workers were listed.
//   - err is the error ListWorkers returned.
func IBMWorkerListingError(clusterID string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %s: %w", ErrIBMWorkerListing, clusterID, err)
}

// OwnershipFactsMayPass reports whether err, the error of reading ownership facts, may pass when the check reads them
// again, so that a hold waits it out instead of failing on it. Only an error of IBM's worker listing
// (ErrIBMWorkerListing) qualifies, and only one that CAPIBM's controllers requeue on (kubernetesservice.Classify, the
// same classes): a 5xx, a 429, a timeout, a connection error or an unclassified answer. IBM's final answers (the
// cluster is gone, the credentials or permissions are wrong, the request is malformed), the management cluster's
// errors, facts that cannot be built and a nil err do not qualify: a check must report them.
//
// Parameters:
//   - err is the error that reading the facts returned, wrapped or not.
func OwnershipFactsMayPass(err error) bool {
	if !errors.Is(err, ErrIBMWorkerListing) {
		return false
	}
	switch kubernetesservice.Classify(kubernetesservice.OpListWorkers, err) {
	case kubernetesservice.ClassTransient, kubernetesservice.ClassUnknown, kubernetesservice.ClassRateLimited:
		return true
	default:
		return false
	}
}
