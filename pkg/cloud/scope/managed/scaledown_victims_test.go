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
	"math/rand/v2"
	"slices"
	"testing"

	. "github.com/onsi/gomega"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// victimWorker returns a worker of pool "p1" in zone with the given state, health and actual version.
func victimWorker(id, zone, state, health, actual string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: "p1", Zone: zone, ActualState: state, HealthState: health,
		KubeVersionActual: actual}
}

// TestPredictScaleDownVictimsOrder checks every priority level of IBM's documented order
// (worker-pool-scaledown.md:37-41): the workers are given in an order that IBM's ranking reverses, and with IDs that
// sort against it, so growing k must take them exactly in the expected order.
func TestPredictScaleDownVictimsOrder(t *testing.T) {
	const deployed, normal, v = "deployed", "normal", "1.36.5_1540"
	testCases := []struct {
		name string
		// expected is in the order IBM removes the workers; each ID sorts after the next one.
		expected []kubernetesservice.Worker
	}{
		{
			name: "state rank, failed first",
			expected: []kubernetesservice.Worker{
				victimWorker("w11", "z1", "provision_failed", normal, v),
				victimWorker("w10", "z1", "deploy_failed", normal, v),
				victimWorker("w09", "z1", "deleting", normal, v),
				victimWorker("w08", "z1", "provision_pending", normal, v),
				victimWorker("w07", "z1", "provisioning", normal, v),
				victimWorker("w06", "z1", "deploying", normal, v),
				victimWorker("w05", "z1", "provisioned", normal, v),
				victimWorker("w04", "z1", "reloading_failed", normal, v),
				victimWorker("w03", "z1", "reloading", normal, v),
				victimWorker("w02", "z1", "reload_pending", normal, v), // not in IBM's list: before deployed
				victimWorker("w01", "z1", deployed, normal, v),
			},
		},
		{
			name: "health rank within one state",
			expected: []kubernetesservice.Worker{
				victimWorker("w06", "z1", deployed, "critical", v),
				victimWorker("w05", "z1", deployed, "warning", v),
				victimWorker("w04", "z1", deployed, "pending", v),
				victimWorker("w03", "z1", deployed, "unsupported", v),
				victimWorker("w02", "z1", deployed, "", v), // no health yet: before normal
				victimWorker("w01", "z1", deployed, normal, v),
			},
		},
		{
			name: "oldest version within one state and health, unknown versions first",
			expected: []kubernetesservice.Worker{
				victimWorker("w06", "z1", deployed, normal, ""),
				victimWorker("w05", "z1", deployed, normal, "1.35.9_1530"),
				victimWorker("w04", "z1", deployed, normal, "1.36.5_1537"),
				victimWorker("w03", "z1", deployed, normal, "1.36.5_1540"),
				victimWorker("w02", "z1", deployed, normal, "1.36.6_1500"),
				victimWorker("w01", "z1", deployed, normal, "1.36.10_1400"),
			},
		},
		{
			name: "state beats health beats version",
			expected: []kubernetesservice.Worker{
				victimWorker("w03", "z1", "provisioning", normal, "1.36.10_1400"),
				victimWorker("w02", "z1", deployed, "warning", "1.36.10_1400"),
				victimWorker("w01", "z1", deployed, normal, "1.35.9_1530"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			given := slices.Clone(tc.expected)
			slices.Reverse(given)
			for k := 0; k <= len(tc.expected); k++ {
				var want []string
				for _, w := range tc.expected[:k] {
					want = append(want, w.ID)
				}
				slices.Sort(want)
				victims, err := PredictScaleDownVictims(given, "p1", []string{"z1"}, k)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(victims).To(ConsistOf(want), "k=%d", k)
			}
		})
	}
}

func TestPredictScaleDownVictims(t *testing.T) {
	const deployed, normal, v = "deployed", "normal", "1.36.5_1540"
	steady := func(id, zone string) kubernetesservice.Worker { return victimWorker(id, zone, deployed, normal, v) }
	testCases := []struct {
		name    string
		workers []kubernetesservice.Worker
		zones   []string
		k       int
		want    []string
	}{
		{
			name:    "ties broken by ID: the oldest worker goes",
			workers: []kubernetesservice.Worker{steady("kube-c1-p1-00000257", "z1"), steady("kube-c1-p1-0000015f", "z1")},
			zones:   []string{"z1"},
			k:       1,
			want:    []string{"kube-c1-p1-0000015f"},
		},
		{
			name: "per zone: k from every zone",
			workers: []kubernetesservice.Worker{
				steady("a2", "z1"), steady("a1", "z1"), steady("b2", "z2"), steady("b1", "z2"), steady("c1", "z3"), steady("c2", "z3"),
			},
			zones: []string{"z1", "z2", "z3"},
			k:     1,
			want:  []string{"a1", "b1", "c1"},
		},
		{
			name: "k > 1 in every zone, failed first",
			workers: []kubernetesservice.Worker{
				steady("a1", "z1"), steady("a2", "z1"), steady("a3", "z1"), victimWorker("a9", "z1", "provision_failed", "critical", ""),
				steady("b1", "z2"), steady("b2", "z2"), steady("b3", "z2"),
			},
			zones: []string{"z1", "z2"},
			k:     2,
			want:  []string{"a1", "a9", "b1", "b2"},
		},
		{
			name:    "a zone with fewer workers than k gives all of them",
			workers: []kubernetesservice.Worker{steady("a1", "z1"), steady("b1", "z2"), steady("b2", "z2"), steady("b3", "z2")},
			zones:   []string{"z1", "z2"},
			k:       2,
			want:    []string{"a1", "b1", "b2"},
		},
		{
			name: "deleted workers, other pools and other zones are never victims",
			workers: []kubernetesservice.Worker{
				victimWorker("a0", "z1", WorkerStateDeleted, "", ""),
				{ID: "a00", PoolID: "p2", Zone: "z1", ActualState: "provision_failed"},
				victimWorker("x0", "z9", "provision_failed", "critical", ""),
				steady("a2", "z1"), steady("a1", "z1"),
			},
			zones: []string{"z1"},
			k:     1,
			want:  []string{"a1"},
		},
		{
			name:    "k 0 predicts no victim",
			workers: []kubernetesservice.Worker{steady("a1", "z1")},
			zones:   []string{"z1"},
			k:       0,
			want:    []string{},
		},
		{
			name:  "no workers",
			zones: []string{"z1"},
			k:     1,
			want:  []string{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			victims, err := PredictScaleDownVictims(tc.workers, "p1", tc.zones, tc.k)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(victims).To(Equal(tc.want))
		})
	}
}

// TestPredictScaleDownVictimsLiveCapture runs the prediction on the scrubbed ROKS capture from IBM Cloud: two deployed,
// normal workers on the same fix pack in one zone, so the older ID goes.
func TestPredictScaleDownVictimsLiveCapture(t *testing.T) {
	g := NewWithT(t)
	workers := loadWorkers(t, "roks-getWorkers-normal.json")
	g.Expect(workers).To(HaveLen(2))
	victims, err := PredictScaleDownVictims(workers, workers[0].PoolID, []string{workers[0].Zone}, 1)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(victims).To(Equal([]string{"kube-roksclusterid0000002-exampletest-default-000001ec"}))
}

// TestPredictScaleDownVictimsIsStable checks over generated inputs that the prediction does not depend on the order of
// the listing and takes min(k, workers) per zone.
func TestPredictScaleDownVictimsIsStable(t *testing.T) {
	g := NewWithT(t)
	rng := rand.New(rand.NewPCG(19, 3)) //nolint:gosec // G404: reproducible test inputs, not security relevant
	states := []string{"provision_failed", "deleting", "provisioning", "deployed", "deployed", "deleted", "something_new"}
	healths := []string{"critical", "warning", "normal", "normal", ""}
	versions := []string{"1.36.5_1540", "1.36.5_1537", "1.35.9_1530", ""}
	zones := []string{"z1", "z2", "z3"}
	for i := range 500 {
		count := rng.IntN(12)
		workers := make([]kubernetesservice.Worker, 0, count)
		perZone := map[string]int{}
		for n := range count {
			w := victimWorker(fmt.Sprintf("w%02d", n), zones[rng.IntN(len(zones))], states[rng.IntN(len(states))],
				healths[rng.IntN(len(healths))], versions[rng.IntN(len(versions))])
			if w.ActualState != WorkerStateDeleted {
				perZone[w.Zone]++
			}
			workers = append(workers, w)
		}
		k := rng.IntN(4)
		victims, err := PredictScaleDownVictims(workers, "p1", zones, k)
		g.Expect(err).NotTo(HaveOccurred())
		wantCount := 0
		for _, zone := range zones {
			wantCount += min(k, perZone[zone])
		}
		g.Expect(victims).To(HaveLen(wantCount), "case %d", i)
		g.Expect(slices.IsSorted(victims)).To(BeTrue(), "case %d", i)
		rng.Shuffle(len(workers), func(a, b int) { workers[a], workers[b] = workers[b], workers[a] })
		again, err := PredictScaleDownVictims(workers, "p1", zones, k)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(again).To(Equal(victims), "case %d", i)
	}
}

func TestPredictScaleDownVictimsRejects(t *testing.T) {
	testCases := []struct {
		name    string
		poolID  string
		zones   []string
		k       int
		wantErr string
	}{
		{name: "no pool ID", poolID: "", zones: []string{"z1"}, k: 1, wantErr: "needs the pool ID"},
		{name: "no zones", poolID: "p1", zones: nil, k: 1, wantErr: "needs the pool's zones"},
		{name: "negative k", poolID: "p1", zones: []string{"z1"}, k: -1, wantErr: "got k=-1"},
		{name: "zone twice", poolID: "p1", zones: []string{"z1", "z1"}, k: 1, wantErr: "got zone z1 twice"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			victims, err := PredictScaleDownVictims(nil, tc.poolID, tc.zones, tc.k)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(victims).To(BeNil())
		})
	}
}

func TestCompareWorkerVersions(t *testing.T) {
	g := NewWithT(t)
	with := func(actual string) kubernetesservice.Worker {
		return kubernetesservice.Worker{KubeVersionActual: actual}
	}
	g.Expect(compareWorkerVersions(with(""), with("garbage"))).To(Equal(0))
	g.Expect(compareWorkerVersions(with(""), with("1.36.5_1540"))).To(Equal(-1))
	g.Expect(compareWorkerVersions(with("1.36.5_1540"), with(""))).To(Equal(1))
	g.Expect(compareWorkerVersions(with("1.36.5_1537"), with("1.36.5_1540"))).To(Equal(-1))
	g.Expect(compareWorkerVersions(with("1.36.5_1540"), with("1.36.5_1540"))).To(Equal(0))
}
