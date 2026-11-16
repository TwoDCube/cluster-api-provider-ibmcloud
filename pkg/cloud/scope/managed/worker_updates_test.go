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
	"strconv"
	"testing"

	. "github.com/onsi/gomega"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// mustMinor parses a minor for a test and fails it when the minor does not parse.
func mustMinor(t *testing.T, s string) ibmversion.Minor {
	t.Helper()
	m, err := ibmversion.ParseMinor(s)
	if err != nil {
		t.Fatalf("parsing minor %q: %v", s, err)
	}
	return m
}

// versioned returns w with kubeVersion actual and target set.
func versioned(w kubernetesservice.Worker, actual, target string) kubernetesservice.Worker {
	w.KubeVersionActual, w.KubeVersionTarget = actual, target
	return w
}

func TestUpdateAvailability(t *testing.T) {
	iks := loadWorkers(t, "iks-getWorkers-normal.json")[0]
	roks := loadWorkers(t, "roks-getWorkers-normal.json")[0]
	provisioning := loadWorkers(t, "iks-getWorkers-deploying.json")[0]
	k8s136, ocp421 := mustMinor(t, "1.36"), mustMinor(t, "4.21")
	testCases := []struct {
		name   string
		worker kubernetesservice.Worker
		master ibmversion.Minor
		want   WorkerUpdateClass
	}{
		{name: "IKS live capture on the master minor, target equals actual", worker: iks, master: k8s136, want: WorkerUpdateUpToDate},
		{name: "ROKS live capture ahead of the master fix pack, same minor", worker: roks, master: ocp421, want: WorkerUpdateUpToDate},
		{name: "minor behind the master", worker: versioned(iks, "1.35.9_1530", "1.35.9_1530"), master: k8s136, want: WorkerUpdateMinorBehind},
		{name: "two minors behind", worker: versioned(iks, "1.34.2_1520", "1.34.2_1520"), master: k8s136, want: WorkerUpdateMinorBehind},
		{name: "ROKS minor behind", worker: versioned(roks, "4.20.9_1530_openshift", "4.20.9_1530_openshift"), master: ocp421, want: WorkerUpdateMinorBehind},
		{
			name:   "minor behind wins over a newer fix pack of its own minor",
			worker: versioned(iks, "1.35.9_1530", "1.35.9_1535"), master: k8s136, want: WorkerUpdateMinorBehind,
		},
		{name: "newer fix pack of the master minor", worker: versioned(iks, "1.36.5_1540", "1.36.5_1545"), master: k8s136, want: WorkerUpdatePatchAvailable},
		{name: "ROKS newer fix pack", worker: versioned(roks, "4.21.40_1540_openshift", "4.21.41_1545_openshift"), master: ocp421, want: WorkerUpdatePatchAvailable},
		{name: "worker ahead of the master minor is not behind", worker: versioned(iks, "1.37.1_1550", "1.37.1_1550"), master: k8s136, want: WorkerUpdateUpToDate},
		{name: "unknown master minor: nothing is behind", worker: versioned(iks, "1.35.9_1530", "1.35.9_1530"), master: ibmversion.Minor{}, want: WorkerUpdateUpToDate},
		{name: "unknown master minor: a fix pack is still available", worker: versioned(iks, "1.36.5_1540", "1.36.5_1545"), master: ibmversion.Minor{}, want: WorkerUpdatePatchAvailable},
		{name: "provisioning live capture without an actual version", worker: provisioning, master: k8s136, want: WorkerUpdateUpToDate},
		{name: "unparseable actual version", worker: versioned(iks, "v1.35.9+IKS", "1.36.5_1540"), master: k8s136, want: WorkerUpdateUpToDate},
		{
			name: "deleted worker has nothing to update",
			worker: func() kubernetesservice.Worker {
				w := versioned(iks, "1.35.9_1530", "1.35.9_1530")
				w.ActualState = WorkerStateDeleted
				return w
			}(),
			master: k8s136, want: WorkerUpdateUpToDate,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(UpdateAvailability(tc.worker, tc.master)).To(Equal(tc.want))
		})
	}
}

// summaryWorker returns a deployed worker of pool with the given versions.
func summaryWorker(id, pool, actual, target string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: pool, Zone: "eu-de-1", ActualState: WorkerStateDeployed,
		HealthState: "normal", KubeVersionActual: actual, KubeVersionTarget: target}
}

func TestSummarizeWorkerVersions(t *testing.T) {
	k8s136 := mustMinor(t, "1.36")
	testCases := []struct {
		name    string
		workers func() []kubernetesservice.Worker
		poolID  string
		master  ibmversion.Minor
		want    WorkerVersionSummary
	}{
		{
			name:    "IKS live capture",
			workers: func() []kubernetesservice.Worker { return loadWorkers(t, "iks-getWorkers-normal.json") },
			poolID:  "iksclusterid00000001-00000a1",
			master:  k8s136,
			want: WorkerVersionSummary{
				WorkerVersions:      []infrav1.WorkerVersionCount{{Version: "1.36.5_1540", Count: 1}},
				TargetWorkerVersion: "1.36.5_1540",
			},
		},
		{
			name:    "ROKS live capture",
			workers: func() []kubernetesservice.Worker { return loadWorkers(t, "roks-getWorkers-normal.json") },
			poolID:  "roksclusterid0000002-00000b2",
			master:  mustMinor(t, "4.21"),
			want: WorkerVersionSummary{
				WorkerVersions:      []infrav1.WorkerVersionCount{{Version: "4.21.40_1540_openshift", Count: 2}},
				TargetWorkerVersion: "4.21.40_1540_openshift",
			},
		},
		{
			name: "mixed versions are counted per version, sorted, with the newest target",
			workers: func() []kubernetesservice.Worker {
				return []kubernetesservice.Worker{
					summaryWorker("w1", "p1", "1.36.5_1540", "1.36.5_1545"),
					summaryWorker("w2", "p1", "1.36.10_1530", "1.36.10_1530"),
					summaryWorker("w3", "p1", "1.35.9_1530", "1.35.9_1530"),
					summaryWorker("w4", "p1", "1.36.5_1540", "1.36.5_1545"),
					summaryWorker("w5", "p1", "1.36.5_1545", "1.36.5_1545"),
					summaryWorker("w6", "p2", "1.30.1_1000", "1.36.11_1600"),
				}
			},
			poolID: "p1",
			master: k8s136,
			want: WorkerVersionSummary{
				WorkerVersions: []infrav1.WorkerVersionCount{
					{Version: "1.35.9_1530", Count: 1}, {Version: "1.36.5_1540", Count: 2}, {Version: "1.36.5_1545", Count: 1},
					{Version: "1.36.10_1530", Count: 1},
				},
				TargetWorkerVersion:        "1.36.10_1530",
				WorkersWithUpdateAvailable: 3,
			},
		},
		{
			name: "deleted workers, provisioning workers and unparseable versions are not counted",
			workers: func() []kubernetesservice.Worker {
				deleted := summaryWorker("w1", "p1", "1.35.9_1530", "1.35.9_1530")
				deleted.ActualState = WorkerStateDeleted
				provisioning := summaryWorker("w2", "p1", "", "")
				provisioning.ActualState = "provisioning"
				return []kubernetesservice.Worker{deleted, provisioning, summaryWorker("w3", "p1", "garbage", "also garbage")}
			},
			poolID: "p1",
			master: k8s136,
			want:   WorkerVersionSummary{},
		},
		{
			name:    "no worker: zero, published as 0 by the caller",
			workers: func() []kubernetesservice.Worker { return nil },
			poolID:  "p1",
			master:  k8s136,
			want:    WorkerVersionSummary{},
		},
		{
			name: "more versions than status allows keeps the oldest",
			workers: func() []kubernetesservice.Worker {
				var workers []kubernetesservice.Worker
				for build := 1500; build < 1500+maxWorkerVersions+2; build++ {
					v := "1.36.5_" + strconv.Itoa(build)
					workers = append(workers, summaryWorker("w"+strconv.Itoa(build), "p1", v, v))
				}
				return workers
			},
			poolID: "p1",
			master: k8s136,
			want: func() WorkerVersionSummary {
				var counts []infrav1.WorkerVersionCount
				for build := 1500; build < 1500+maxWorkerVersions; build++ {
					counts = append(counts, infrav1.WorkerVersionCount{Version: "1.36.5_" + strconv.Itoa(build), Count: 1})
				}
				return WorkerVersionSummary{WorkerVersions: counts, TargetWorkerVersion: "1.36.5_" + strconv.Itoa(1500+maxWorkerVersions+1)}
			}(),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := SummarizeWorkerVersions(tc.workers(), tc.poolID, tc.master)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
	t.Run("an empty pool ID is rejected", func(t *testing.T) {
		g := NewWithT(t)
		_, err := SummarizeWorkerVersions(nil, "", k8s136)
		g.Expect(err).To(MatchError(ContainSubstring("status.poolID")))
	})
}

func TestCompareIBMVersions(t *testing.T) {
	parse := func(s string) ibmversion.Version {
		v, err := ibmversion.Parse(s)
		if err != nil {
			t.Fatalf("parsing %q: %v", s, err)
		}
		return v
	}
	testCases := []struct {
		a, b string
		want int
	}{
		{a: "1.35.9_1530", b: "1.36.1_1500", want: -1},
		{a: "1.36.5_1540", b: "1.36.5_1537", want: 1},
		{a: "1.36.10_1530", b: "1.36.9_1600", want: 1},
		{a: "1.36", b: "1.36.0_1", want: -1},
		{a: "1.36.5_1540", b: "1.36.5_1540", want: 0},
		{a: "4.21.40_1540_openshift", b: "4.21.35_1537_openshift", want: 1},
		{a: "1.36.5_1540", b: "1.36.5_1540_openshift", want: -1},
		{a: "4.21.40_1540_openshift", b: "1.36.5_1540", want: 1},
	}
	for _, tc := range testCases {
		t.Run(tc.a+" vs "+tc.b, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(compareIBMVersions(parse(tc.a), parse(tc.b))).To(Equal(tc.want))
			g.Expect(compareIBMVersions(parse(tc.b), parse(tc.a))).To(Equal(-tc.want))
		})
	}
	g := NewWithT(t)
	g.Expect(compareBool(false, true)).To(Equal(-1))
	g.Expect(compareBool(true, false)).To(Equal(1))
	g.Expect(compareBool(true, true)).To(Equal(0))
}
