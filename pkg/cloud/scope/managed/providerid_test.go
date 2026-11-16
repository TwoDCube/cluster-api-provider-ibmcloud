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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// liveNodeProviderIDs returns Node.spec.providerID of every Node of the IKS and ROKS workload clusters probed on IBM
// Cloud, scrubbed the same way as the getCluster and getWorkers captures: the cluster IDs are the synthetic
// iksclusterid00000001 and roksclusterid0000002. The Node captures themselves are not in the repository, because
// BuildProviderIDList needs only these strings from them.
func liveNodeProviderIDs(flavor string) []string {
	if flavor == "iks" {
		return []string{"ibm://" + fixtureAccountID + "///iksclusterid00000001/kube-iksclusterid00000001-exampletest-example-0000015f"}
	}
	return []string{
		"ibm://" + fixtureAccountID + "///roksclusterid0000002/kube-roksclusterid0000002-exampletest-default-000001ec",
		"ibm://" + fixtureAccountID + "///roksclusterid0000002/kube-roksclusterid0000002-exampletest-default-0000029f",
	}
}

// TestBuildProviderIDListFromLiveCaptures derives the list from the scrubbed getCluster and getWorkers captures and
// compares it with the Nodes' provider IDs: the derivation must equal Node.spec.providerID exactly, or Cluster API
// would match no Node.
func TestBuildProviderIDListFromLiveCaptures(t *testing.T) {
	for _, flavor := range []string{"iks", "roks"} {
		t.Run(flavor, func(t *testing.T) {
			g := NewWithT(t)
			cluster := loadCluster(t, flavor+"-getCluster-normal.json")
			workers := loadWorkers(t, flavor+"-getWorkers-normal.json")
			g.Expect(workers).NotTo(BeEmpty())
			account, err := AccountIDFromCRN(cluster.CRN)
			g.Expect(err).NotTo(HaveOccurred())

			result, err := BuildProviderIDList(ProviderIDListInput{
				AccountID: account,
				ClusterID: cluster.ID,
				PoolID:    workers[0].PoolID,
				Workers:   workers,
				Now:       providerIDNow(),
				DropAfter: 30 * time.Second,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.ProviderIDList).To(Equal(liveNodeProviderIDs(flavor)))
			g.Expect(result.MissingWorkers).To(BeEmpty())
		})
	}
}

// providerIDNow is the fixed clock reading of the provider ID tests.
func providerIDNow() time.Time {
	return time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
}

// The pool, cluster and account of the synthetic provider ID tests.
const (
	pidAccount = "acct0123456789"
	pidCluster = "c0abcdefghijklmnopqr"
	pidPool    = "c0abcdefghijklmnopqr-aaaaaaa"
	pidOther   = "c0abcdefghijklmnopqr-bbbbbbb"
)

// pidWorker returns a worker of pool in state; the ID ends with seq.
func pidWorker(seq, pool, state string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: "kube-" + pidCluster + "-pool-" + seq, PoolID: pool, Zone: "eu-de-1",
		ActualState: state, HealthState: "normal"}
}

// pid returns the provider ID of the worker whose ID ends with seq.
func pid(seq string) string {
	return ProviderID(pidAccount, pidCluster, "kube-"+pidCluster+"-pool-"+seq)
}

// missing returns a missingWorkers entry for the worker whose ID ends with seq.
func missing(seq string, since time.Time) infrav1.MissingWorker {
	return infrav1.MissingWorker{WorkerID: "kube-" + pidCluster + "-pool-" + seq, FirstMissingTime: metav1.NewTime(since)}
}

// pidInput returns a valid input with no workers and nothing published.
func pidInput() ProviderIDListInput {
	return ProviderIDListInput{AccountID: pidAccount, ClusterID: pidCluster, PoolID: pidPool, Now: providerIDNow(),
		DropAfter: 30 * time.Second}
}

func TestBuildProviderIDList(t *testing.T) {
	now := providerIDNow()
	poll := 30 * time.Second
	testCases := []struct {
		name        string
		modify      func(in *ProviderIDListInput)
		wantList    []string
		wantMissing []infrav1.MissingWorker
	}{
		{
			name: "every non-deleted worker of the pool is listed, failed and provisioning ones included",
			modify: func(in *ProviderIDListInput) {
				in.Workers = []kubernetesservice.Worker{
					pidWorker("03", pidPool, "deployed"), pidWorker("01", pidPool, "provisioning"),
					pidWorker("02", pidPool, "provision_failed"), pidWorker("04", pidPool, "deploy_failed"),
					pidWorker("05", pidOther, "deployed"),
				}
			},
			wantList: []string{pid("01"), pid("02"), pid("03"), pid("04")},
		},
		{
			name:     "no worker and nothing published gives an empty list",
			modify:   func(*ProviderIDListInput) {},
			wantList: []string{},
		},
		{
			name: "first omission keeps the id and records it in missingWorkers",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed")}
			},
			wantList:    []string{pid("01"), pid("02")},
			wantMissing: []infrav1.MissingWorker{missing("02", now)},
		},
		{
			name: "omitted again poolTransitionPoll later is dropped",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Missing = []infrav1.MissingWorker{missing("02", now.Add(-poll))}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed")}
			},
			wantList: []string{pid("01")},
		},
		{
			name: "omitted again before poolTransitionPoll is kept",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Missing = []infrav1.MissingWorker{missing("02", now.Add(-poll+time.Second))}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed")}
			},
			wantList:    []string{pid("01"), pid("02")},
			wantMissing: []infrav1.MissingWorker{missing("02", now.Add(-poll+time.Second))},
		},
		{
			name: "worker listed as deleted is dropped at once",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), pidWorker("02", pidPool, "deleted")}
			},
			wantList: []string{pid("01")},
		},
		{
			name: "deleting worker with pods bound stays listed",
			modify: func(in *ProviderIDListInput) {
				// IBM was observed to delete the Node of a deleting worker itself; the ID stays until IBM lists it deleted.
				in.Current = []string{pid("01"), pid("02")}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), pidWorker("02", pidPool, "deleting")}
			},
			wantList: []string{pid("01"), pid("02")},
		},
		{
			name: "deleted worker is removed and leaves missingWorkers",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Missing = []infrav1.MissingWorker{missing("02", now.Add(-time.Second))}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), pidWorker("02", pidPool, "deleted")}
			},
			wantList: []string{pid("01")},
		},
		{
			name: "a deleted worker that was never published is not added",
			modify: func(in *ProviderIDListInput) {
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), pidWorker("02", pidPool, "deleted")}
			},
			wantList: []string{pid("01")},
		},
		{
			name: "a worker that reappears leaves missingWorkers",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Missing = []infrav1.MissingWorker{missing("02", now.Add(-time.Hour))}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), pidWorker("02", pidPool, "deployed")}
			},
			wantList: []string{pid("01"), pid("02")},
		},
		{
			name: "a worker listed in another pool counts as absent",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01"), pid("02")}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), pidWorker("02", pidOther, "deployed")}
			},
			wantList:    []string{pid("01"), pid("02")},
			wantMissing: []infrav1.MissingWorker{missing("02", now)},
		},
		{
			name: "a deleted record in another pool does not drop the ID",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01")}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidOther, "deleted")}
			},
			wantList:    []string{pid("01")},
			wantMissing: []infrav1.MissingWorker{missing("01", now)},
		},
		{
			name: "an empty listing drops nothing on its first omission",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("02"), pid("01")}
			},
			wantList:    []string{pid("01"), pid("02")},
			wantMissing: []infrav1.MissingWorker{missing("01", now), missing("02", now)},
		},
		{
			name: "a live and a deleted record of one worker keep it listed",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01")}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deleted"), pidWorker("01", pidPool, "deployed")}
			},
			wantList: []string{pid("01")},
		},
		{
			name: "duplicates in the listing and in the published list are written once, sorted",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("02"), pid("01"), pid("02")}
				in.Workers = []kubernetesservice.Worker{pidWorker("02", pidPool, "deployed"), pidWorker("01", pidPool, "deployed"),
					pidWorker("02", pidPool, "deployed")}
			},
			wantList: []string{pid("01"), pid("02")},
		},
		{
			name: "missingWorkers entries of IDs no longer published are cleared",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01")}
				in.Missing = []infrav1.MissingWorker{missing("09", now.Add(-time.Second))}
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed")}
			},
			wantList: []string{pid("01")},
		},
		{
			name: "a missingWorkers entry without a time counts as missing for the first time now",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01")}
				in.Missing = []infrav1.MissingWorker{{WorkerID: "kube-" + pidCluster + "-pool-01"}, {}}
			},
			wantList:    []string{pid("01")},
			wantMissing: []infrav1.MissingWorker{missing("01", now)},
		},
		{
			name: "a worker named twice in missingWorkers keeps the later time, which can only delay the drop",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01")}
				in.Missing = []infrav1.MissingWorker{missing("01", now.Add(-time.Second)), missing("01", now.Add(-time.Hour)),
					missing("01", now.Add(-2*time.Second))}
			},
			wantList:    []string{pid("01")},
			wantMissing: []infrav1.MissingWorker{missing("01", now.Add(-time.Second))},
		},
		{
			name: "a firstMissingTime in the future (clock skew) keeps the ID",
			modify: func(in *ProviderIDListInput) {
				in.Current = []string{pid("01")}
				in.Missing = []infrav1.MissingWorker{missing("01", now.Add(time.Hour))}
			},
			wantList:    []string{pid("01")},
			wantMissing: []infrav1.MissingWorker{missing("01", now.Add(time.Hour))},
		},
		{
			name: "a first omission recorded at a fraction of a second is rounded up, never down",
			modify: func(in *ProviderIDListInput) {
				in.Now = now.Add(200 * time.Millisecond)
				in.Current = []string{pid("01")}
			},
			wantList:    []string{pid("01")},
			wantMissing: []infrav1.MissingWorker{missing("01", now.Add(time.Second))},
		},
		{
			name: "workers of other pools are ignored, even with IDs that cannot form a provider ID",
			modify: func(in *ProviderIDListInput) {
				in.Workers = []kubernetesservice.Worker{pidWorker("01", pidPool, "deployed"), {ID: "", PoolID: pidOther},
					{ID: "a/b", PoolID: pidOther}}
			},
			wantList: []string{pid("01")},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := pidInput()
			tc.modify(&in)
			result, err := BuildProviderIDList(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.ProviderIDList).To(Equal(tc.wantList))
			g.Expect(result.MissingWorkers).To(Equal(tc.wantMissing))
		})
	}
}

func TestBuildProviderIDListRejects(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(in *ProviderIDListInput)
		wantErr []string
	}{
		{name: "empty account", modify: func(in *ProviderIDListInput) { in.AccountID = "" }, wantErr: []string{"AccountID", "AccountIDFromCRN"}},
		{name: "account with a separator", modify: func(in *ProviderIDListInput) { in.AccountID = "a/b" }, wantErr: []string{`AccountID is "a/b"`}},
		{name: "empty cluster ID", modify: func(in *ProviderIDListInput) { in.ClusterID = "" }, wantErr: []string{"ClusterID", "status.clusterID"}},
		{name: "cluster ID with a slash", modify: func(in *ProviderIDListInput) { in.ClusterID = "c/1" }, wantErr: []string{`ClusterID is "c/1"`}},
		{name: "empty pool ID", modify: func(in *ProviderIDListInput) { in.PoolID = "" }, wantErr: []string{"PoolID", "status.poolID"}},
		{name: "no clock reading", modify: func(in *ProviderIDListInput) { in.Now = time.Time{} }, wantErr: []string{"Now is not set"}},
		{name: "no drop delay", modify: func(in *ProviderIDListInput) { in.DropAfter = 0 }, wantErr: []string{"DropAfter is 0s", "poolTransitionPoll"}},
		{name: "negative drop delay", modify: func(in *ProviderIDListInput) { in.DropAfter = -time.Second }, wantErr: []string{"DropAfter is -1s"}},
		{
			name: "worker of the pool without an ID",
			modify: func(in *ProviderIDListInput) {
				in.Workers = []kubernetesservice.Worker{{PoolID: pidPool, ActualState: "deployed"}}
			},
			wantErr: []string{`IBM listed a worker of pool ` + pidPool + ` with the ID ""`, "left unchanged"},
		},
		{
			name:    "worker of the pool whose ID has a slash",
			modify:  func(in *ProviderIDListInput) { in.Workers = []kubernetesservice.Worker{{ID: "w/1", PoolID: pidPool}} },
			wantErr: []string{`with the ID "w/1"`},
		},
		{
			name:    "published entry of another account",
			modify:  func(in *ProviderIDListInput) { in.Current = []string{ProviderID("other", pidCluster, "w1")} },
			wantErr: []string{`spec.providerIDList holds "ibm://other///` + pidCluster + `/w1"`, "remove the entry by hand"},
		},
		{
			name:    "published entry of another cluster",
			modify:  func(in *ProviderIDListInput) { in.Current = []string{ProviderID(pidAccount, "c1other", "w1")} },
			wantErr: []string{"is not a provider ID of IBM cluster " + pidCluster},
		},
		{
			name:    "published entry without a worker ID",
			modify:  func(in *ProviderIDListInput) { in.Current = []string{ProviderID(pidAccount, pidCluster, "")} },
			wantErr: []string{"is not a provider ID"},
		},
		{
			name:    "published entry with a path after the worker ID",
			modify:  func(in *ProviderIDListInput) { in.Current = []string{ProviderID(pidAccount, pidCluster, "w1/x")} },
			wantErr: []string{"is not a provider ID"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := pidInput()
			tc.modify(&in)
			result, err := BuildProviderIDList(in)
			g.Expect(err).To(HaveOccurred())
			for _, want := range tc.wantErr {
				g.Expect(err.Error()).To(ContainSubstring(want))
			}
			g.Expect(result).To(Equal(ProviderIDListResult{}))
		})
	}
}

// TestBuildProviderIDListNeverDropsALiveWorker checks over generated inputs that no live worker loses its provider
// ID: every non-deleted worker of the pool is listed, and a published ID leaves the list only when IBM lists the
// worker as deleted or a missingWorkers entry shows a first omission at least DropAfter ago. It also checks that the
// result is sorted and stable.
func TestBuildProviderIDListNeverDropsALiveWorker(t *testing.T) {
	g := NewWithT(t)
	rng := rand.New(rand.NewPCG(17, 6)) //nolint:gosec // G404: reproducible test inputs, not security relevant
	states := []string{"provision_pending", "provisioning", "deploying", "deployed", "deleting", "deleted", "provision_failed"}
	now := providerIDNow()
	for i := range 2000 {
		in := pidInput()
		for seq := range 8 {
			id := fmt.Sprintf("%02d", seq)
			switch rng.IntN(4) {
			case 0: // not listed
			case 1:
				in.Workers = append(in.Workers, pidWorker(id, pidOther, states[rng.IntN(len(states))]))
			default:
				in.Workers = append(in.Workers, pidWorker(id, pidPool, states[rng.IntN(len(states))]))
			}
			if rng.IntN(2) == 0 {
				in.Current = append(in.Current, pid(id))
			}
			if rng.IntN(3) == 0 {
				in.Missing = append(in.Missing, missing(id, now.Add(time.Duration(rng.IntN(120)-60)*time.Second)))
			}
		}
		result, err := BuildProviderIDList(in)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(slices.IsSorted(result.ProviderIDList)).To(BeTrue(), "case %d", i)
		g.Expect(slices.Compact(slices.Clone(result.ProviderIDList))).To(Equal(result.ProviderIDList), "case %d", i)

		deletedOnly, live := map[string]bool{}, map[string]bool{}
		for _, w := range in.Workers {
			if w.PoolID == pidPool && w.ActualState == WorkerStateDeleted {
				deletedOnly[w.ID] = true
			}
			if w.PoolID == pidPool && w.ActualState != WorkerStateDeleted {
				live[w.ID] = true
			}
		}
		for id := range live {
			g.Expect(result.ProviderIDList).To(ContainElement(ProviderID(pidAccount, pidCluster, id)), "case %d: live worker %s", i, id)
		}
		for _, entry := range in.Current {
			if slices.Contains(result.ProviderIDList, entry) {
				continue
			}
			id := strings.TrimPrefix(entry, ProviderID(pidAccount, pidCluster, ""))
			droppable := deletedOnly[id] && !live[id]
			for _, m := range in.Missing {
				droppable = droppable || (m.WorkerID == id && now.Sub(m.FirstMissingTime.Time) >= in.DropAfter)
			}
			g.Expect(droppable).To(BeTrue(), "case %d: %s dropped without a deleted record or a second omission", i, id)
		}
		again, err := BuildProviderIDList(in)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(again).To(Equal(result), "case %d: the result must not depend on map order", i)
	}
}

func TestCeilToSecond(t *testing.T) {
	g := NewWithT(t)
	whole := providerIDNow()
	g.Expect(ceilToSecond(whole)).To(Equal(whole))
	g.Expect(ceilToSecond(whole.Add(time.Nanosecond))).To(Equal(whole.Add(time.Second)))
	g.Expect(ceilToSecond(whole.Add(999 * time.Millisecond))).To(Equal(whole.Add(time.Second)))
}
