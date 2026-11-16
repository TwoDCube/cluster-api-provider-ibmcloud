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
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

// The first-seen map tests drive transitionalWorkers listing by listing: a transitional worker keeps the time it was
// first seen, a settled, deleted or vanished one loses its entry, the Clusters never share entries, and every doubt (a
// stale map, an empty UID) leaves the map empty, which fails closed.

// twNow is the clock reading of the first listing in these tests.
var twNow = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// twWorker returns a worker of the IBM cluster with the given state and pending operation.
//
// Parameters:
//   - id is the worker ID.
//   - state is lifecycle.actualState.
//   - pending is lifecycle.pendingOperation ("" for none).
func twWorker(id, state, pending string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: "other-pool", ActualState: state, HealthState: "normal", PendingOperation: pending}
}

// listing is one ListWorkers answer recorded after offset from twNow.
type listing struct {
	offset  time.Duration
	workers []kubernetesservice.Worker
}

func TestTransitionalWorkersObserve(t *testing.T) {
	const cluster = types.UID("cluster-a")
	testCases := []struct {
		name     string
		listings []listing
		// at is when the snapshot is taken, after twNow.
		at   time.Duration
		want map[string]time.Time
	}{
		{name: "a provision_pending worker is recorded when first seen",
			listings: []listing{{0, []kubernetesservice.Worker{twWorker("o2", "provision_pending", ""), twWorker("w0", "deployed", "")}}},
			want:     map[string]time.Time{"o2": twNow}},
		{name: "a worker that stays transitional keeps its first time",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provision_pending", "")}},
				{30 * time.Minute, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
				{50 * time.Minute, []kubernetesservice.Worker{twWorker("o2", "deploying", "")}},
			},
			at: 50 * time.Minute, want: map[string]time.Time{"o2": twNow}},
		{name: "the entry is dropped once the worker is deployed",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provision_pending", "")}},
				{10 * time.Minute, []kubernetesservice.Worker{twWorker("o2", "deployed", "")}},
			},
			at: 10 * time.Minute, want: nil},
		{name: "a worker that settles and changes again starts over",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
				{10 * time.Minute, []kubernetesservice.Worker{twWorker("o2", "deployed", "")}},
				{20 * time.Minute, []kubernetesservice.Worker{twWorker("o2", "reloading", "")}},
			},
			at: 20 * time.Minute, want: map[string]time.Time{"o2": twNow.Add(20 * time.Minute)}},
		{name: "a failed worker is not transitional and gets no entry",
			listings: []listing{{0, []kubernetesservice.Worker{twWorker("o2", "provision_failed", "")}}},
			want:     nil},
		{name: "a deleted worker loses its entry",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "deleting", "")}},
				{5 * time.Minute, []kubernetesservice.Worker{twWorker("o2", managedscope.WorkerStateDeleted, "")}},
			},
			at: 5 * time.Minute, want: nil},
		{name: "a worker no longer listed loses its entry",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", ""), twWorker("o3", "provisioning", "")}},
				{5 * time.Minute, []kubernetesservice.Worker{twWorker("o3", "provisioning", "")}},
			},
			at: 5 * time.Minute, want: map[string]time.Time{"o3": twNow}},
		{name: "a pending operation counts as transitional (PendingOperationBlocks)",
			listings: []listing{{0, []kubernetesservice.Worker{twWorker("w0", "deployed", "reload")}}},
			want:     map[string]time.Time{"w0": twNow}},
		{name: "a worker first seen in a later listing gets that listing's time",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
				{7 * time.Minute, []kubernetesservice.Worker{twWorker("o2", "provisioning", ""), twWorker("o3", "provision_pending", "")}},
			},
			at: 7 * time.Minute, want: map[string]time.Time{"o2": twNow, "o3": twNow.Add(7 * time.Minute)}},
		{name: "a gap of exactly the maximum keeps the entries",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
				{transitionalObservationMaxGap, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
			},
			at: transitionalObservationMaxGap, want: map[string]time.Time{"o2": twNow}},
		{name: "a listing after a longer gap starts over (fails closed)",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
				{transitionalObservationMaxGap + time.Second, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
			},
			at: transitionalObservationMaxGap + time.Second, want: map[string]time.Time{"o2": twNow.Add(transitionalObservationMaxGap + time.Second)}},
		{name: "a snapshot after a longer gap is empty (fails closed)",
			listings: []listing{{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}}},
			at:       transitionalObservationMaxGap + time.Second, want: nil},
		{name: "an empty listing drops everything",
			listings: []listing{
				{0, []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}},
				{time.Minute, nil},
			},
			at: time.Minute, want: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			var tw transitionalWorkers
			for _, l := range tc.listings {
				tw.observe(cluster, l.workers, twNow.Add(l.offset))
			}
			g.Expect(tw.snapshot(cluster, twNow.Add(tc.at))).To(Equal(tc.want))
		})
	}
}

func TestTransitionalWorkersClusters(t *testing.T) {
	t.Run("the Clusters keep separate entries", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}, twNow)
		tw.observe("cluster-b", []kubernetesservice.Worker{twWorker("o2", "deployed", ""), twWorker("o3", "provisioning", "")}, twNow.Add(time.Minute))
		g.Expect(tw.snapshot("cluster-a", twNow.Add(time.Minute))).To(Equal(map[string]time.Time{"o2": twNow}))
		g.Expect(tw.snapshot("cluster-b", twNow.Add(time.Minute))).To(Equal(map[string]time.Time{"o3": twNow.Add(time.Minute)}))
	})
	t.Run("forget drops only that Cluster", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}, twNow)
		tw.observe("cluster-b", []kubernetesservice.Worker{twWorker("o3", "provisioning", "")}, twNow)
		tw.forget("cluster-a")
		g.Expect(tw.snapshot("cluster-a", twNow)).To(BeNil())
		g.Expect(tw.snapshot("cluster-b", twNow)).To(Equal(map[string]time.Time{"o3": twNow}))
		tw.forget("unknown")
	})
	t.Run("an empty Cluster UID records nothing", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("", []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}, twNow)
		g.Expect(tw.snapshot("", twNow)).To(BeNil())
		g.Expect(tw.clusters).To(BeEmpty())
	})
	t.Run("the zero value answers an empty snapshot", func(t *testing.T) {
		var tw transitionalWorkers
		NewWithT(t).Expect(tw.snapshot("cluster-a", twNow)).To(BeNil())
	})
	t.Run("a snapshot is a copy", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}, twNow)
		snapshot := tw.snapshot("cluster-a", twNow)
		snapshot["o2"] = twNow.Add(-24 * time.Hour)
		snapshot["o9"] = twNow
		g.Expect(tw.snapshot("cluster-a", twNow)).To(Equal(map[string]time.Time{"o2": twNow}))
	})
	t.Run("a settled Cluster keeps no entry", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}, twNow)
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "deployed", "")}, twNow.Add(time.Minute))
		g.Expect(tw.clusters).NotTo(HaveKey(types.UID("cluster-a")))
	})
	t.Run("a stale snapshot drops the stale entries", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", "")}, twNow)
		g.Expect(tw.snapshot("cluster-a", twNow.Add(transitionalObservationMaxGap+time.Second))).To(BeNil())
		g.Expect(tw.clusters).NotTo(HaveKey(types.UID("cluster-a")))
	})
	t.Run("an out-of-order older listing only makes entries younger", func(t *testing.T) {
		g := NewWithT(t)
		var tw transitionalWorkers
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", ""), twWorker("o3", "provisioning", "")}, twNow)
		// A newer listing saw o2 settle and o4 start; the older listing of another pool's reconcile is recorded after it.
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "deployed", ""), twWorker("o3", "provisioning", ""),
			twWorker("o4", "provisioning", "")}, twNow.Add(10*time.Minute))
		tw.observe("cluster-a", []kubernetesservice.Worker{twWorker("o2", "provisioning", ""), twWorker("o3", "provisioning", "")},
			twNow.Add(11*time.Minute))
		g.Expect(tw.snapshot("cluster-a", twNow.Add(11*time.Minute))).To(Equal(map[string]time.Time{
			"o2": twNow.Add(11 * time.Minute), // younger than its real start: fails closed
			"o3": twNow,
		}), "o4 waits for the next listing")
	})
}

// TestTransitionalWorkersConcurrentUse runs the reconciles of several pools of two Clusters at once against one map, as
// the pool controller does when it reconciles several pools of one Cluster concurrently; `go test -race` reports any
// unsynchronized access.
func TestTransitionalWorkersConcurrentUse(t *testing.T) {
	g := NewWithT(t)
	var tw transitionalWorkers
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cluster := types.UID(fmt.Sprintf("cluster-%d", i%2))
			for step := range 50 {
				now := twNow.Add(time.Duration(step) * time.Second)
				_ = tw.snapshot(cluster, now)
				tw.observe(cluster, []kubernetesservice.Worker{twWorker("o2", "provisioning", ""), twWorker(fmt.Sprintf("p%d", i), "deploying", "")}, now)
				if step == 25 && i == 7 {
					tw.forget(cluster)
				}
			}
		}()
	}
	wg.Wait()
	for _, cluster := range []types.UID{"cluster-0", "cluster-1"} {
		snapshot := tw.snapshot(cluster, twNow.Add(time.Minute))
		g.Expect(snapshot).To(HaveKey("o2"), "a worker every listing shows transitional keeps an entry")
		g.Expect(snapshot["o2"]).To(BeTemporally(">=", twNow))
	}
}
