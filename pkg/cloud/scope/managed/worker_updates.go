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
	"slices"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file reports which IBM updates the workers of a pool can take: the worker version status of the pool
// (status.workerVersions, targetWorkerVersion, workersWithUpdateAvailable) whatever the update policy. Master and
// workers are linked only at the minor: IBM's worker fix packs are independent of its master patches (observed on IBM
// Cloud: IKS workers on 1.36.5_1540 under a 1.36.5_1537 master, ROKS workers on 4.21.40_1540_openshift under a
// 4.21.35_1537_openshift master), so a worker is compared with the master's minor and with its own
// kubeVersion.target, never with the master's fix pack. A worker rollout decides from the same classes which worker
// it replaces, and when.

// WorkerUpdateClass is what a worker would gain from a replace with update=true.
type WorkerUpdateClass string

const (
	// WorkerUpdateMinorBehind is a worker whose minor (of kubeVersion.actual) is below the master's minor. Such a worker
	// is always replaced: workers follow the master minor automatically.
	WorkerUpdateMinorBehind WorkerUpdateClass = "MinorBehind"

	// WorkerUpdatePatchAvailable is a worker for which IBM offers another fix pack of its minor (WorkerHasNewerFixPack;
	// how IBM moves kubeVersion.target when it publishes a fix pack is not yet verified against IBM Cloud). Such a
	// worker is replaced only under the Automatic policy, or by spec.rollout.after.
	WorkerUpdatePatchAvailable WorkerUpdateClass = "PatchAvailable"

	// WorkerUpdateUpToDate is a worker with no update available.
	WorkerUpdateUpToDate WorkerUpdateClass = "UpToDate"
)

// UpdateAvailability returns the update IBM offers a worker, in this order: MinorBehind first, then PatchAvailable,
// else UpToDate. It is pure.
//
// A worker that IBM lists as deleted, that has no kubeVersion.actual yet (it is still provisioning) or whose
// kubeVersion.actual does not parse is UpToDate: nothing known about it can be updated, and a decision that removes a
// worker must never rest on a version CAPIBM cannot read.
//
// Parameters:
//   - w: the worker as ListWorkers returned it.
//   - masterMinor: the minor of the master, parsed from IBMCloudManagedControlPlane status.distributionVersion (for
//     example 1.36 from "1.36.5_1537", 4.21 from "4.21.35_1537_openshift"). The zero Minor means the master version is
//     not known yet: then no worker is MinorBehind.
func UpdateAvailability(w kubernetesservice.Worker, masterMinor ibmversion.Minor) WorkerUpdateClass {
	if w.ActualState == WorkerStateDeleted {
		return WorkerUpdateUpToDate
	}
	actual, err := ibmversion.Parse(w.KubeVersionActual)
	if err != nil {
		return WorkerUpdateUpToDate
	}
	if actual.MinorVersion().Compare(masterMinor) < 0 {
		return WorkerUpdateMinorBehind
	}
	if WorkerHasNewerFixPack(w) {
		return WorkerUpdatePatchAvailable
	}
	return WorkerUpdateUpToDate
}

// WorkerVersionSummary is the worker version status of a pool (IBMCloudManagedMachinePool status).
type WorkerVersionSummary struct {
	// WorkerVersions is status.workerVersions: the number of the pool's workers per kubeVersion.actual, sorted from the
	// oldest to the newest version, at most maxWorkerVersions entries.
	WorkerVersions []infrav1.WorkerVersionCount

	// TargetWorkerVersion is status.targetWorkerVersion: the newest kubeVersion.target of the pool's workers; "" when
	// none is known, which the caller omits (status strings are never written as "": the API requires MinLength=1).
	TargetWorkerVersion string

	// WorkersWithUpdateAvailable is status.workersWithUpdateAvailable: the pool's workers that are MinorBehind or
	// PatchAvailable, whatever the update policy. Always published, including 0.
	WorkersWithUpdateAvailable int32
}

// maxWorkerVersions is the MaxItems of status.workerVersions: a longer list would make the API server reject the whole
// status patch.
const maxWorkerVersions = 16

// SummarizeWorkerVersions returns the worker version status of a pool. It is pure and counts the workers of the pool
// that IBM does not list as deleted.
//
// A version that ibmversion.Parse does not accept is left out of WorkerVersions and TargetWorkerVersion, so status
// never carries a value it cannot order, and such a worker is never counted as having an update (UpdateAvailability).
// IBM versions always parsed in the live probes on IBM Cloud. If a pool ever runs more than maxWorkerVersions
// versions, the oldest ones are kept, because those are the workers an update is about.
//
// Parameters:
//   - workers: every worker of the cluster from one ListWorkers call (all pools).
//   - poolID: the IBM pool ID (status.poolID); only its workers count.
//   - masterMinor: the master's minor, as for UpdateAvailability.
//
// It returns an error when poolID is empty.
func SummarizeWorkerVersions(workers []kubernetesservice.Worker, poolID string, masterMinor ibmversion.Minor) (WorkerVersionSummary, error) {
	if poolID == "" {
		return WorkerVersionSummary{}, errors.New("SummarizeWorkerVersions needs the pool ID; pass the " +
			"IBMCloudManagedMachinePool's status.poolID")
	}
	var summary WorkerVersionSummary
	counts := map[string]int32{}
	var versions []ibmversion.Version
	var target ibmversion.Version
	for _, w := range workers {
		if w.PoolID != poolID || w.ActualState == WorkerStateDeleted {
			continue
		}
		if UpdateAvailability(w, masterMinor) != WorkerUpdateUpToDate {
			summary.WorkersWithUpdateAvailable++
		}
		if actual, err := ibmversion.Parse(w.KubeVersionActual); err == nil {
			if counts[actual.String()] == 0 {
				versions = append(versions, actual)
			}
			counts[actual.String()]++
		}
		if t, err := ibmversion.Parse(w.KubeVersionTarget); err == nil &&
			(summary.TargetWorkerVersion == "" || compareIBMVersions(t, target) > 0) {
			target, summary.TargetWorkerVersion = t, t.String()
		}
	}
	slices.SortFunc(versions, compareIBMVersions)
	for _, v := range versions[:min(len(versions), maxWorkerVersions)] {
		summary.WorkerVersions = append(summary.WorkerVersions, infrav1.WorkerVersionCount{Version: v.String(), Count: counts[v.String()]})
	}
	return summary, nil
}

// compareIBMVersions orders two parsed IBM versions from the oldest to the newest: by minor, then patch, then build
// number; a minor-only version comes before every fix pack of its minor. Versions that tie on all of that (they differ
// only in the OpenShift suffix or the pending marker) are ordered by their text, so that every sort is deterministic.
// Unlike ibmversion.Version.CompareFixPack it orders versions of different minors and products too, which a status
// list of versions needs.
//
// Parameters:
//   - a, b: the versions to compare; it returns -1 when a is older, 0 when they are the same, +1 when a is newer.
func compareIBMVersions(a, b ibmversion.Version) int {
	return cmp.Or(
		a.MinorVersion().Compare(b.MinorVersion()),
		compareBool(a.HasFixPack, b.HasFixPack),
		cmp.Compare(a.Patch, b.Patch),
		cmp.Compare(a.Build, b.Build),
		cmp.Compare(a.String(), b.String()),
	)
}

// compareBool orders false before true.
//
// Parameters:
//   - a, b: the values to compare.
func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}
