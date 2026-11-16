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
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file predicts which workers IBM removes when CAPIBM shrinks a pool. resizeWorkerPool cannot name a victim, and
// removeWorker would disable IBM's auto-rebalance for the pool (ts-worker-autorebalance.md:37-50), so CAPIBM predicts
// IBM's victims from IBM's documented order, drains them before the resize, and verifies the prediction afterwards
// (UnexpectedScaleDownVictim). Whether resizeWorkerPool follows the documented order is not yet verified against IBM
// Cloud; the fallback, resizing without the prediction, is ScaleDownMode in assumptions.go. The order below is IBM's
// documentation, not a live fact, which is why it lives here.

// workerStateDeploying is the lifecycle.actualState of a worker IBM is deploying (observed on IBM Cloud: provisioning,
// then deploying, then deployed).
const workerStateDeploying = "deploying"

// scaleDownStateOrder returns the worker states that IBM removes before a deployed worker, in IBM's order (priority 1
// of worker-pool-scaledown.md:37-41, IKS and ROKS, lastupdated 2026-11-05). deployed comes after all of them.
func scaleDownStateOrder() []string {
	return []string{"provision_failed", "deploy_failed", WorkerStateDeleting, "provision_pending", "provisioning",
		workerStateDeploying, "provisioned", "reloading_failed", "reloading"}
}

// scaleDownHealthOrder returns the worker health states that IBM removes before a normal worker, in IBM's order
// (priority 2 of worker-pool-scaledown.md:37-41). normal comes after all of them.
func scaleDownHealthOrder() []string {
	return []string{"critical", "warning", "pending", "unsupported"}
}

// scaleDownRank returns the position of value in IBM's order: its index in before, len(before)+1 for last (deployed or
// normal), and len(before) for any other value. A state or health IBM does not document is thus taken as less
// healthy than a running worker but after every documented one: IBM removes broken and changing workers first, and the
// verification step of the scale-down reports a wrong guess.
//
// Parameters:
//   - before: IBM's documented values that are removed first, in order.
//   - last: the value of a healthy, running worker, which IBM removes last.
//   - value: the worker's value.
func scaleDownRank(before []string, last, value string) int {
	if value == last {
		return len(before) + 1
	}
	if i := slices.Index(before, value); i >= 0 {
		return i
	}
	return len(before)
}

// PredictScaleDownVictims predicts the workers IBM removes when the pool shrinks by k workers per zone. It is pure. In
// every zone of the pool, IBM's documented order (worker-pool-scaledown.md:37-41) applies to the pool's workers that
// IBM does not list as deleted:
//
//  1. state: provision_failed, deploy_failed, deleting, provision_pending, provisioning, deploying, provisioned,
//     reloading_failed, reloading, then deployed;
//  2. health: critical, warning, pending, unsupported, then normal;
//  3. version: the oldest kubeVersion.actual first, a version that does not parse (or none yet) before every other;
//  4. worker ID ascending: VPC worker IDs end in a zero-padded hexadecimal sequence, so ID order is age order and the
//     oldest worker goes first (observed on IBM Cloud: a replacement worker got a higher sequence number than the
//     worker it replaced; IBM's priority 5).
//
// IBM's priority 4, the dedicated-host flag, is ignored: CAPIBM creates no dedicated-host pools. The first k workers of
// each zone are the victims; a zone with fewer workers gives all of them.
//
// Parameters:
//   - workers: every worker of the cluster from one ListWorkers call (all pools); IBM picks victims only from this
//     pool.
//   - poolID: the IBM pool ID, status.poolID.
//   - zones: the pool's zones (IBMCloudManagedMachinePool spec.zones); a worker of the pool in another zone is never a
//     victim.
//   - k: the workers to remove per zone, sizePerZone - desired; 0 predicts none.
//
// It returns the victims' worker IDs, sorted, as status.disruption.workerIDs records them, or an error naming the
// argument to fix when poolID is empty, zones is empty or names a zone twice, or k is negative.
func PredictScaleDownVictims(workers []kubernetesservice.Worker, poolID string, zones []string, k int) ([]string, error) {
	switch {
	case poolID == "":
		return nil, errors.New("PredictScaleDownVictims needs the pool ID; pass the IBMCloudManagedMachinePool's status.poolID")
	case len(zones) == 0:
		return nil, errors.New("PredictScaleDownVictims needs the pool's zones; pass spec.zones of the IBMCloudManagedMachinePool")
	case k < 0:
		return nil, fmt.Errorf("PredictScaleDownVictims got k=%d; pass sizePerZone - desired of a shrink, which is at least 0", k)
	}
	byZone := make(map[string][]kubernetesservice.Worker, len(zones))
	for _, zone := range zones {
		if _, dup := byZone[zone]; dup {
			return nil, fmt.Errorf("PredictScaleDownVictims got zone %s twice; pass each zone of spec.zones once", zone)
		}
		byZone[zone] = nil
	}
	for _, w := range workers {
		if _, inPool := byZone[w.Zone]; inPool && w.PoolID == poolID && w.ActualState != WorkerStateDeleted {
			byZone[w.Zone] = append(byZone[w.Zone], w)
		}
	}
	victims := []string{}
	for _, candidates := range byZone {
		slices.SortFunc(candidates, compareScaleDownOrder)
		for _, w := range candidates[:min(k, len(candidates))] {
			victims = append(victims, w.ID)
		}
	}
	slices.Sort(victims)
	return victims, nil
}

// compareScaleDownOrder orders two workers of one zone in IBM's scale-down order: the worker IBM removes first sorts
// first.
//
// Parameters:
//   - a, b: the workers to compare.
func compareScaleDownOrder(a, b kubernetesservice.Worker) int {
	states, health := scaleDownStateOrder(), scaleDownHealthOrder()
	return cmp.Or(
		cmp.Compare(scaleDownRank(states, WorkerStateDeployed, a.ActualState), scaleDownRank(states, WorkerStateDeployed, b.ActualState)),
		cmp.Compare(scaleDownRank(health, "normal", a.HealthState), scaleDownRank(health, "normal", b.HealthState)),
		compareWorkerVersions(a, b),
		strings.Compare(a.ID, b.ID),
	)
}

// compareWorkerVersions orders two workers by kubeVersion.actual from the oldest to the newest, with an actual version
// that does not parse (or is empty, while the worker provisions) before every parsed one: an unknown version is taken
// as the oldest, IBM's first pick among workers of one state and health (worker-pool-scaledown.md:37-41).
//
// Parameters:
//   - a, b: the workers to compare; it returns -1 when a sorts first, 0 when they tie, +1 when b sorts first.
func compareWorkerVersions(a, b kubernetesservice.Worker) int {
	va, errA := ibmversion.Parse(a.KubeVersionActual)
	vb, errB := ibmversion.Parse(b.KubeVersionActual)
	switch {
	case errA != nil && errB != nil:
		return 0
	case errA != nil:
		return -1
	case errB != nil:
		return 1
	default:
		return compareIBMVersions(va, vb)
	}
}
