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
	"maps"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file is the first-seen map behind DisruptionCheck.TransitionalSince, which the settled-cluster rule of
// CheckDisruption reads. IBM's worker listing carries no time for a worker's state, so CheckDisruption alone cannot
// tell a worker that has just started provisioning from one that IBM has left provisioning for hours; without that time
// a single stuck worker would hold every worker removal of the cluster for ever. The pool reconciler therefore
// remembers, per Cluster and worker, when it first saw a worker transitional, and refreshes that from every ListWorkers
// of any pool of the Cluster. The map lives in memory only, which needs no API field and no write: after a controller
// restart it is empty, and a stuck worker blocks again for up to WorkerStuckAfter. Every uncertainty (a restart, a long
// pause, an out-of-order observation) makes an entry younger or removes it, never older, so the rule fails closed.

// transitionalObservationMaxGap is how old the latest recorded listing of a Cluster may be before its first-seen times
// are discarded. Between two listings CAPIBM cannot know that a worker stayed transitional: it may have settled and
// started changing again. A pool that observes its IBM pool lists the workers every few minutes (steadyStatePoll is
// 5 m), so a gap as long as WorkerStuckAfter means that nothing watched the Cluster for a whole stuck window (a paused
// Cluster, a stalled controller, pools that all wait for something else). Discarding treats such a gap like a
// restart (fails closed).
const transitionalObservationMaxGap = managedscope.WorkerStuckAfter

// transitionalWorkers is the pool reconciler's first-seen map: for each Cluster UID, the time each worker of its IBM
// cluster was first seen transitional (managedscope.IsWorkerTransitional). The zero value is ready to use, and it is
// safe for concurrent use by the reconciles of several pools of one Cluster. It must not be copied after first use.
type transitionalWorkers struct {
	mu sync.Mutex
	// clusters holds the entries per Cluster UID; nil until the first observation.
	clusters map[types.UID]clusterTransitions
}

// clusterTransitions are the first-seen times of the workers of one Cluster.
type clusterTransitions struct {
	// observed is when the latest listing of the Cluster's workers was recorded.
	observed time.Time
	// since maps a worker ID to the time the worker was first seen transitional; only transitional workers of the
	// latest listing have an entry.
	since map[string]time.Time
}

// snapshot returns a copy of the first-seen times of the workers of cluster, for MachinePoolScopeInput.TransitionalSince,
// so that the scope can read it while other reconciles update the map. It returns nil when nothing is recorded for
// cluster, when cluster is empty, and when the latest listing is older than transitionalObservationMaxGap (whose
// entries it drops).
//
// Parameters:
//   - cluster is the UID of the Cluster of the pool being reconciled.
//   - now is the reconcile's clock reading.
func (t *transitionalWorkers) snapshot(cluster types.UID, now time.Time) map[string]time.Time {
	if cluster == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return maps.Clone(t.current(cluster, now))
}

// observe records one successful ListWorkers of the IBM cluster of cluster for the settled-cluster rule of
// CheckDisruption: a transitional worker keeps its first-seen time or, when it has none, gets now; a worker that
// settled, was deleted or is no longer listed loses its entry. Entries of a listing older than
// transitionalObservationMaxGap are discarded first. An empty cluster records nothing, so that objects without a UID
// never share entries.
//
// Two reconciles of one Cluster may record their listings out of order. A worker the older listing still shows
// transitional after the newer one dropped it then gets now again, and a worker only the newer listing showed is
// dropped until the next listing: both make an entry younger, never older, so the order cannot make a worker look stuck
// early.
//
// Parameters:
//   - cluster is the UID of the Cluster whose IBM cluster was listed.
//   - workers are every worker of that IBM cluster from one successful ListWorkers call (all pools).
//   - now is the clock reading when the listing is recorded.
func (t *transitionalWorkers) observe(cluster types.UID, workers []kubernetesservice.Worker, now time.Time) {
	if cluster == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	previous := t.current(cluster, now)
	since := map[string]time.Time{}
	for _, w := range workers {
		if !managedscope.IsWorkerTransitional(w) {
			continue
		}
		first, seen := previous[w.ID]
		if !seen {
			first = now
		}
		since[w.ID] = first
	}
	if len(since) == 0 {
		// Nothing is transitional: no entry is kept, so a settled Cluster costs no memory.
		delete(t.clusters, cluster)
		return
	}
	if t.clusters == nil {
		t.clusters = map[types.UID]clusterTransitions{}
	}
	t.clusters[cluster] = clusterTransitions{observed: now, since: since}
}

// forget drops every entry of cluster. The pool reconciler calls it when the Cluster is being deleted: IBM removes
// every worker with the cluster, and no pool of a deleting Cluster calls IBM, so none lists workers again and its
// entries would otherwise stay until the controller restarts.
//
// Parameters:
//   - cluster is the UID of the Cluster.
func (t *transitionalWorkers) forget(cluster types.UID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.clusters, cluster)
}

// current returns the entries of cluster, or nil when none are recorded or the latest listing is older than
// transitionalObservationMaxGap; stale entries are dropped. The caller holds t.mu.
//
// Parameters:
//   - cluster is the Cluster UID.
//   - now is the clock reading.
func (t *transitionalWorkers) current(cluster types.UID, now time.Time) map[string]time.Time {
	entry, found := t.clusters[cluster]
	if !found {
		return nil
	}
	if now.Sub(entry.observed) > transitionalObservationMaxGap {
		delete(t.clusters, cluster)
		return nil
	}
	return entry.since
}
