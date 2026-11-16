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
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file builds the MachinePool contract field spec.providerIDList of an IBMCloudManagedMachinePool. Cluster API
// deletes every Node whose provider ID leaves the list and guards only an entirely empty list
// (machinepool_controller_noderef.go:165-197), so the list errs on the side of keeping IDs: a worker leaves it only
// when IBM lists it as deleted, or when two listings at least poolTransitionPoll apart both omit it. Both functions
// are pure; the pool controller passes one successful worker listing and the published state, and writes the result.

// ProviderIDListInput is what BuildProviderIDList reads: one successful worker listing and the state the pool
// published before it.
type ProviderIDListInput struct {
	// AccountID is the account of the IBM cluster, AccountIDFromCRN(status.clusterCRN of the control plane). Required.
	AccountID string

	// ClusterID is the IBM cluster ID, status.clusterID of the control plane. Required.
	ClusterID string

	// PoolID is the IBM pool ID, status.poolID of the IBMCloudManagedMachinePool. Required.
	PoolID string

	// Workers are every worker of the cluster from one successful ListWorkers call (all pools, showDeleted=true). The
	// caller never calls BuildProviderIDList for a failed listing; it keeps the published list then, because a list built
	// from a failed listing would drop live workers.
	Workers []kubernetesservice.Worker

	// Current is spec.providerIDList as read. Every entry must be a provider ID of this cluster
	// (ibm://<AccountID>///<ClusterID>/<workerID>), which is the only form CAPIBM writes.
	Current []string

	// Missing is status.missingWorkers as read: the workers of Current that an earlier listing omitted.
	Missing []infrav1.MissingWorker

	// Now is the controller's clock reading for this listing. Required.
	Now time.Time

	// DropAfter is poolTransitionPoll: an ID that a listing omits is dropped only by a listing taken at least this long
	// after the first listing that omitted it. It is an argument because the named requeue intervals belong to the
	// controllers. Required, greater than zero.
	DropAfter time.Duration
}

// ProviderIDListResult is what BuildProviderIDList publishes.
type ProviderIDListResult struct {
	// ProviderIDList is the new spec.providerIDList: sorted and de-duplicated, so that an unchanged pool always yields
	// the same list and the caller writes it only when it differs from the current one. The caller
	// publishes len(ProviderIDList) as status.replicas in the same patch.
	ProviderIDList []string

	// MissingWorkers is the new status.missingWorkers, sorted by worker ID: the workers that are still in
	// ProviderIDList although this listing omitted them.
	MissingWorkers []infrav1.MissingWorker
}

// BuildProviderIDList builds spec.providerIDList and status.missingWorkers of a pool from one successful worker
// listing. It is pure.
//
// The list holds ProviderID(AccountID, ClusterID, workerID) for every worker of the pool (PoolID) that IBM does not
// list as deleted: provisioning, deploying, deleting and failed workers included. An unknown ID is harmless to Cluster
// API, while a missing one deletes a Node; and IBM was observed to delete the Node of a replaced worker itself while
// the worker is deleting, so keeping the ID until IBM lists the worker as deleted never leaves a live Node behind. An
// ID already in Current leaves the list:
//
//   - at once, when IBM lists the worker as deleted (showDeleted=true, WorkerStateDeleted; how long IBM lists deleted
//     workers is not yet verified against IBM Cloud, so the next rule also covers a worker IBM no longer lists);
//   - when this listing omits it, or lists it in another pool, and the first listing that did so (its
//     missingWorkers entry) was at least DropAfter before Now. A first omission only records the worker in
//     MissingWorkers; a worker that reappears leaves MissingWorkers.
//
// Parameters:
//   - in: the listing and the published state; see ProviderIDListInput.
//
// It returns an error naming the field or the value to fix when in is invalid, when a worker of the pool has an ID
// that cannot form a provider ID, or when Current holds an entry that is not a provider ID of this cluster. The
// caller then keeps the published list and missingWorkers unchanged: dropping an ID it cannot read could make Cluster
// API delete a live Node.
func BuildProviderIDList(in ProviderIDListInput) (ProviderIDListResult, error) {
	if err := in.validate(); err != nil {
		return ProviderIDListResult{}, err
	}
	listed, deleted, err := in.poolWorkers()
	if err != nil {
		return ProviderIDListResult{}, err
	}
	current, err := in.currentWorkerIDs()
	if err != nil {
		return ProviderIDListResult{}, err
	}
	firstMissing := in.firstMissingTimes()

	keep := make(map[string]bool, len(listed)+len(current))
	for id := range listed {
		keep[id] = true
	}
	var missing []infrav1.MissingWorker
	for _, id := range current {
		if keep[id] || deleted[id] {
			// Listed again (it leaves missingWorkers), or listed as deleted (dropped at once).
			continue
		}
		since, wasMissing := firstMissing[id]
		if !wasMissing {
			since = metav1.NewTime(ceilToSecond(in.Now))
		}
		if wasMissing && in.Now.Sub(since.Time) >= in.DropAfter {
			// The second listing at least DropAfter after the first omission: the worker is gone.
			continue
		}
		keep[id] = true
		missing = append(missing, infrav1.MissingWorker{WorkerID: id, FirstMissingTime: since})
	}

	result := ProviderIDListResult{ProviderIDList: make([]string, 0, len(keep))}
	for id := range keep {
		result.ProviderIDList = append(result.ProviderIDList, ProviderID(in.AccountID, in.ClusterID, id))
	}
	slices.Sort(result.ProviderIDList)
	slices.SortFunc(missing, func(a, b infrav1.MissingWorker) int { return strings.Compare(a.WorkerID, b.WorkerID) })
	result.MissingWorkers = missing
	return result, nil
}

// validate returns an error naming the first invalid field of in.
func (in ProviderIDListInput) validate() error {
	switch {
	case in.AccountID == "" || !isAlphanumeric(in.AccountID):
		return fmt.Errorf("ProviderIDListInput.AccountID is %q; pass AccountIDFromCRN of the control plane's status.clusterCRN",
			in.AccountID)
	case in.ClusterID == "" || strings.Contains(in.ClusterID, "/"):
		return fmt.Errorf("ProviderIDListInput.ClusterID is %q; pass the control plane's status.clusterID", in.ClusterID)
	case in.PoolID == "":
		return errors.New("ProviderIDListInput.PoolID is empty; pass the IBMCloudManagedMachinePool's status.poolID")
	case in.Now.IsZero():
		return errors.New("ProviderIDListInput.Now is not set; pass the controller clock's current time")
	case in.DropAfter <= 0:
		return fmt.Errorf("ProviderIDListInput.DropAfter is %s; pass poolTransitionPoll, which is greater than zero", in.DropAfter)
	}
	return nil
}

// poolWorkers returns the IDs of the pool's workers that this listing shows and does not show as deleted (listed),
// and those it shows only as deleted. A worker listed both ways counts as listed, so that a duplicate deleted record
// can never drop a live worker.
func (in ProviderIDListInput) poolWorkers() (listed, deleted map[string]bool, err error) {
	listed, deleted = map[string]bool{}, map[string]bool{}
	for _, w := range in.Workers {
		if w.PoolID != in.PoolID {
			continue
		}
		if w.ID == "" || strings.Contains(w.ID, "/") {
			return nil, nil, fmt.Errorf("IBM listed a worker of pool %s with the ID %q, which cannot form a provider ID; "+
				"spec.providerIDList is left unchanged", in.PoolID, w.ID)
		}
		if w.ActualState == WorkerStateDeleted {
			deleted[w.ID] = true
		} else {
			listed[w.ID] = true
		}
	}
	for id := range listed {
		delete(deleted, id)
	}
	return listed, deleted, nil
}

// currentWorkerIDs returns the worker IDs of the entries of Current, de-duplicated, or an error naming the first entry
// that is not a provider ID of this cluster.
func (in ProviderIDListInput) currentWorkerIDs() ([]string, error) {
	prefix := ProviderID(in.AccountID, in.ClusterID, "")
	seen := make(map[string]bool, len(in.Current))
	ids := make([]string, 0, len(in.Current))
	for _, entry := range in.Current {
		id, found := strings.CutPrefix(entry, prefix)
		if !found || id == "" || strings.Contains(id, "/") {
			return nil, fmt.Errorf("spec.providerIDList holds %q, which is not a provider ID of IBM cluster %s "+
				"(%s<workerID>); CAPIBM writes only that form and leaves the list unchanged rather than drop an ID whose "+
				"Node Cluster API would then delete; remove the entry by hand if no Node of the workload cluster has this "+
				"provider ID", entry, in.ClusterID, prefix)
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// firstMissingTimes returns status.missingWorkers as a map from worker ID to the time of the first listing that
// omitted the worker. An entry without a worker ID or without a time is left out, so its worker counts as missing for
// the first time now: that can only delay a drop, never advance it. A worker named twice keeps the later time, for the
// same reason.
func (in ProviderIDListInput) firstMissingTimes() map[string]metav1.Time {
	times := make(map[string]metav1.Time, len(in.Missing))
	for _, m := range in.Missing {
		if m.WorkerID == "" || m.FirstMissingTime.IsZero() {
			continue
		}
		if earlier, found := times[m.WorkerID]; !found || m.FirstMissingTime.After(earlier.Time) {
			times[m.WorkerID] = m.FirstMissingTime
		}
	}
	return times
}

// ceilToSecond returns t rounded up to a whole second. status.missingWorkers stores firstMissingTime as a metav1.Time,
// which keeps whole seconds only; rounding up means that the round trip through the API server can only delay a drop,
// never let an ID go before DropAfter has really passed.
//
// Parameters:
//   - t: the time to round.
func ceilToSecond(t time.Time) time.Time {
	truncated := t.Truncate(time.Second)
	if truncated.Equal(t) {
		return t
	}
	return truncated.Add(time.Second)
}
