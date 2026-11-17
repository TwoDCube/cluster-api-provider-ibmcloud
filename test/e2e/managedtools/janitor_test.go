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
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

// janitorNow is the fixed "now" of the janitor tests.
var janitorNow = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// ibmDate formats t as IBM's createdDate (the form observed on IBM Cloud).
func ibmDate(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05-0700")
}

// janitorFixture is a cluster list that holds one cluster for every rule of the janitor.
func janitorFixture() []map[string]any {
	old := ibmDate(janitorNow.Add(-5 * time.Hour))
	return []map[string]any{
		// Deleted: prefixed and older than 4 h.
		ibmCluster("c0stale1aaaaaaaaaaaa", "capibm-e2e-old111-iks", old, "normal"),
		ibmCluster("c0stale2aaaaaaaaaaaa", "capibm-e2e-old222-roks", ibmDate(janitorNow.Add(-JanitorMinimumAge)), "warning"),
		// Kept: young (a run that may still be going).
		ibmCluster("c0youngaaaaaaaaaaaaa", "capibm-e2e-new333-iks", ibmDate(janitorNow.Add(-3*time.Hour)), "deploying"),
		// Never selected: no prefix, a lookalike prefix, the prefix without its separator.
		ibmCluster("c0foreignaaaaaaaaaaa", "production-iks", old, "normal"),
		ibmCluster("c0lookalikeaaaaaaaaa", "capibm-e2e2-old444-iks", old, "normal"),
		ibmCluster("c0bareprefixaaaaaaaa", "capibm-e2e", old, "normal"),
		// Kept: already deleting, unknown age.
		ibmCluster("c0deletingaaaaaaaaaa", "capibm-e2e-old555-iks", old, "deleting"),
		ibmCluster("c0nodateaaaaaaaaaaaa", "capibm-e2e-old666-iks", "yesterday", "normal"),
	}
}

func TestRunJanitorDeletesOnlyStalePrefixedClustersByID(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = janitorFixture()

	report, err := RunJanitor(context.Background(), fake.clusterService("rg-e2e"), JanitorOptions{
		NamePrefix: DefaultNamePrefix, MinAge: JanitorMinimumAge, Now: janitorNow,
	})

	g.Expect(err).NotTo(HaveOccurred())
	// Exactly the two stale prefixed clusters, each addressed by its ID.
	g.Expect(fake.deletes()).To(ConsistOf("/global/v1/clusters/c0stale1aaaaaaaaaaaa", "/global/v1/clusters/c0stale2aaaaaaaaaaaa"))
	// The janitor asks IBM to keep the persistent storage: its selection by prefix and age proves less than the
	// backstop's.
	g.Expect(fake.deleteQueries()).To(Equal([]string{"deleteResources=false", "deleteResources=false"}))
	g.Expect(report.PersistentStorage).To(Equal(kubernetesservice.RetainPersistentStorage))
	g.Expect(report.String()).NotTo(ContainSubstring("persistent storage"))
	for _, path := range fake.deletes() {
		for _, cluster := range janitorFixture() {
			g.Expect(path).NotTo(HaveSuffix("/"+cluster["name"].(string)), "a DELETE addressed a cluster by name")
		}
	}
	g.Expect(names(report.Deleted)).To(ConsistOf("capibm-e2e-old111-iks", "capibm-e2e-old222-roks"))
	skipped := map[string]string{}
	for _, s := range report.Skipped {
		skipped[s.Cluster.Name] = s.Reason
	}
	g.Expect(skipped).To(HaveLen(3))
	g.Expect(skipped["capibm-e2e-new333-iks"]).To(Equal("created 3h0m0s ago, younger than 4h0m0s"))
	g.Expect(skipped["capibm-e2e-old555-iks"]).To(Equal("IBM is already deleting it (state deleting)"))
	g.Expect(skipped["capibm-e2e-old666-iks"]).To(HavePrefix("its age is unknown"))
	// The report never names clusters outside the prefix.
	g.Expect(report.String()).NotTo(ContainSubstring("production-iks"))
	g.Expect(report.String()).NotTo(ContainSubstring("capibm-e2e2"))
}

func TestJanitorAndBackstopNeverDeleteOutsideTheResourceGroup(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	// IBM ignores X-Auth-Resource-Group in this answer (the fake server always does): a prefixed cluster older than
	// 4 h, named exactly like the run's cluster and created after the run started, sits in another resource group.
	runStart := janitorNow.Add(-5 * time.Hour)
	created := ibmDate(runStart.Add(time.Minute))
	fake.clusters = []map[string]any{
		ibmClusterInGroup("c0otherrgaaaaaaaaaaa", "capibm-e2e-abc123-iks", created, "normal", "rg-other"),
		ibmClusterInGroup("c0nogroupaaaaaaaaaaa", "capibm-e2e-abc123-roks", created, "normal", ""),
	}
	service := fake.clusterService("rg-e2e")

	janitor, err := RunJanitor(context.Background(), service, JanitorOptions{
		NamePrefix: DefaultNamePrefix, MinAge: JanitorMinimumAge, Now: janitorNow,
	})
	g.Expect(err).NotTo(HaveOccurred())
	backstop, err := DeleteRunClusters(context.Background(), service,
		[]string{"capibm-e2e-abc123-iks", "capibm-e2e-abc123-roks"}, runStart, false)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(fake.deletes()).To(BeEmpty())
	g.Expect(janitor.Deleted).To(BeEmpty())
	g.Expect(backstop.Deleted).To(BeEmpty())
	// Clusters outside the scope are not even reported, like clusters without the prefix.
	g.Expect(janitor.String()).To(Equal("no matching cluster\n"))
	g.Expect(backstop.String()).To(Equal("no matching cluster\n"))
}

func TestRunJanitorDryRunSendsNoDelete(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = janitorFixture()

	report, err := RunJanitor(context.Background(), fake.clusterService(""), JanitorOptions{
		NamePrefix: DefaultNamePrefix, MinAge: JanitorMinimumAge, Now: janitorNow, DryRun: true,
	})

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(fake.deletes()).To(BeEmpty())
	g.Expect(report.DryRun).To(BeTrue())
	g.Expect(names(report.Deleted)).To(ConsistOf("capibm-e2e-old111-iks", "capibm-e2e-old222-roks"))
	g.Expect(report.String()).To(ContainSubstring("would delete (dry run) capibm-e2e-old111-iks (ID c0stale1aaaaaaaaaaaa"))
}

func TestRunJanitorRefusesAYoungMinimumAgeWithoutDeleting(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = janitorFixture()

	_, err := RunJanitor(context.Background(), fake.clusterService(""), JanitorOptions{
		NamePrefix: DefaultNamePrefix, MinAge: time.Hour, Now: janitorNow,
	})

	g.Expect(err).To(MatchError(ContainSubstring("janitor minimum age 1h0m0s is shorter than 4h0m0s")))
	g.Expect(fake.deletes()).To(BeEmpty())
}

func TestRunJanitorRefusesAnInvalidPrefixWithoutDeleting(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = janitorFixture()

	_, err := RunJanitor(context.Background(), fake.clusterService(""), JanitorOptions{
		NamePrefix: "c", MinAge: JanitorMinimumAge, Now: janitorNow,
	})

	g.Expect(err).To(MatchError(ContainSubstring(`E2E_NAME_PREFIX "c"`)))
	g.Expect(fake.deletes()).To(BeEmpty())
}

func TestRunJanitorListFailure(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.listStatus = http.StatusInternalServerError

	_, err := RunJanitor(context.Background(), fake.clusterService(""), JanitorOptions{
		NamePrefix: DefaultNamePrefix, MinAge: JanitorMinimumAge, Now: janitorNow,
	})

	g.Expect(err).To(MatchError(ContainSubstring("janitor: listing clusters")))
	g.Expect(fake.deletes()).To(BeEmpty())
}

func TestRunJanitorContinuesAfterARefusedDeletion(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = janitorFixture()
	fake.deleteStatus["c0stale1aaaaaaaaaaaa"] = http.StatusConflict
	fake.deleteStatus["c0stale2aaaaaaaaaaaa"] = http.StatusNotFound

	report, err := RunJanitor(context.Background(), fake.clusterService(""), JanitorOptions{
		NamePrefix: DefaultNamePrefix, MinAge: JanitorMinimumAge, Now: janitorNow,
	})

	g.Expect(err).To(MatchError(ContainSubstring("deleting cluster capibm-e2e-old111-iks (ID c0stale1aaaaaaaaaaaa): DeleteCluster: IBM Cloud answered HTTP 409")))
	g.Expect(fake.deletes()).To(HaveLen(2))
	g.Expect(report.Deleted).To(BeEmpty())
	g.Expect(names(report.AlreadyGone)).To(ConsistOf("capibm-e2e-old222-roks"))
	g.Expect(report.Failed).To(HaveLen(1))
	g.Expect(report.String()).To(ContainSubstring("NOT deleted: capibm-e2e-old111-iks"))
	g.Expect(report.String()).To(ContainSubstring("already gone: capibm-e2e-old222-roks"))
}

func TestPlanStaleClustersSkipsNameShapedIDs(t *testing.T) {
	g := NewWithT(t)
	old := ibmDate(janitorNow.Add(-5 * time.Hour))
	clusters := []Cluster{{ID: "capibm-e2e-old111-iks", Name: "capibm-e2e-old111-iks", CreatedDate: old, State: "normal"}}

	plan, err := PlanStaleClusters(clusters, DefaultNamePrefix, JanitorMinimumAge, janitorNow)

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(plan.Delete).To(BeEmpty())
	g.Expect(plan.Skipped).To(HaveLen(1))
	g.Expect(plan.Skipped[0].Reason).To(ContainSubstring("is not an IBM cluster ID"))
}

func TestDeleteRunClusters(t *testing.T) {
	runStart := janitorNow.Add(-2 * time.Hour)
	testCases := []struct {
		name        string
		clusters    []map[string]any
		wantDeletes []string
		wantSkipped []string
	}{
		{
			name: "the run's cluster, created after the run started",
			clusters: []map[string]any{
				ibmCluster("c0runaaaaaaaaaaaaaaa", "capibm-e2e-abc123-iks", ibmDate(runStart.Add(time.Minute)), "normal"),
			},
			wantDeletes: []string{"/global/v1/clusters/c0runaaaaaaaaaaaaaaa"},
		},
		{
			name: "clock skew within RunCleanupClockSkew",
			clusters: []map[string]any{
				ibmCluster("c0runaaaaaaaaaaaaaaa", "capibm-e2e-abc123-iks", ibmDate(runStart.Add(-RunCleanupClockSkew)), "normal"),
			},
			wantDeletes: []string{"/global/v1/clusters/c0runaaaaaaaaaaaaaaa"},
		},
		{
			name: "same name, created before the run started",
			clusters: []map[string]any{
				ibmCluster("c0earlieraaaaaaaaaaa", "capibm-e2e-abc123-iks", ibmDate(runStart.Add(-time.Hour)), "normal"),
			},
			wantSkipped: []string{"capibm-e2e-abc123-iks"},
		},
		{
			name: "only whole names: another run, a longer name and a foreign cluster stay",
			clusters: []map[string]any{
				ibmCluster("c0otherrunaaaaaaaaaa", "capibm-e2e-zzz999-iks", ibmDate(runStart.Add(time.Minute)), "normal"),
				ibmCluster("c0longeraaaaaaaaaaaa", "capibm-e2e-abc123-iks-2", ibmDate(runStart.Add(time.Minute)), "normal"),
				ibmCluster("c0foreignaaaaaaaaaaa", "production-iks", ibmDate(runStart.Add(time.Minute)), "normal"),
			},
		},
		{
			name: "already deleting",
			clusters: []map[string]any{
				ibmCluster("c0runaaaaaaaaaaaaaaa", "capibm-e2e-abc123-iks", ibmDate(runStart.Add(time.Minute)), "deleting"),
			},
			wantSkipped: []string{"capibm-e2e-abc123-iks"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)
			fake.clusters = tc.clusters

			report, err := DeleteRunClusters(context.Background(), fake.clusterService(""), []string{"capibm-e2e-abc123-iks"}, runStart, false)

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(fake.deletes()).To(ConsistOf(toAny(tc.wantDeletes)...))
			// The backstop asks IBM to delete the persistent storage of the run's own clusters, so that the run leaves no
			// volumes, and its report says that it asked, not that IBM deleted it.
			for _, query := range fake.deleteQueries() {
				g.Expect(query).To(Equal("deleteResources=true"))
			}
			if len(tc.wantDeletes) > 0 {
				g.Expect(report.String()).To(MatchRegexp(`^deleted capibm-e2e-abc123-iks \(ID c0runaaaaaaaaaaaaaaa, created [^)]+\), asking IBM to delete its persistent storage\n`))
			}
			skipped := make([]string, 0, len(report.Skipped))
			for _, s := range report.Skipped {
				skipped = append(skipped, s.Cluster.Name)
			}
			g.Expect(skipped).To(ConsistOf(toAny(tc.wantSkipped)...))
		})
	}
}

func TestPlanRunClustersValidation(t *testing.T) {
	g := NewWithT(t)
	_, err := PlanRunClusters(nil, nil, janitorNow)
	g.Expect(err).To(MatchError(ContainSubstring("run cleanup needs the full cluster names of the run")))
	_, err = PlanRunClusters(nil, []string{""}, janitorNow)
	g.Expect(err).To(MatchError(ContainSubstring("run cleanup needs the full cluster names of the run")))
	_, err = PlanRunClusters(nil, []string{"capibm-e2e-abc123-iks"}, time.Time{})
	g.Expect(err).To(MatchError(ContainSubstring("run cleanup needs the run's start time")))
}

func TestDeleteRunClustersListFailure(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.listStatus = http.StatusUnauthorized

	_, err := DeleteRunClusters(context.Background(), fake.clusterService(""), []string{"capibm-e2e-abc123-iks"}, janitorNow, false)

	g.Expect(err).To(MatchError(ContainSubstring("run cleanup: listing clusters")))
	g.Expect(fake.deletes()).To(BeEmpty())
}

func TestDeleteRunClustersInvalidNames(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = janitorFixture()

	_, err := DeleteRunClusters(context.Background(), fake.clusterService(""), nil, janitorNow, false)

	g.Expect(err).To(MatchError(ContainSubstring("run cleanup: run cleanup needs the full cluster names")))
	g.Expect(fake.deletes()).To(BeEmpty())
}

func TestExecutePlanWithFakeAPI(t *testing.T) {
	g := NewWithT(t)
	api := &recordingAPI{errs: map[string]error{"c0bbbbbbbbbbbbbbbbbb": errors.New("connection reset")}}
	plan := DeletionPlan{Delete: []Cluster{
		{ID: "c0aaaaaaaaaaaaaaaaaa", Name: "capibm-e2e-a-iks"},
		{ID: "c0bbbbbbbbbbbbbbbbbb", Name: "capibm-e2e-b-iks"},
	}, PersistentStorage: kubernetesservice.DeletePersistentStorage}

	report, err := ExecutePlan(context.Background(), api, plan, false)

	g.Expect(api.deleted).To(Equal([]string{"c0aaaaaaaaaaaaaaaaaa", "c0bbbbbbbbbbbbbbbbbb"}))
	g.Expect(api.storage).To(Equal([]kubernetesservice.PersistentStorage{kubernetesservice.DeletePersistentStorage,
		kubernetesservice.DeletePersistentStorage}), "every DELETE carries the plan's storage choice")
	g.Expect(err).To(MatchError(ContainSubstring("deleting cluster capibm-e2e-b-iks (ID c0bbbbbbbbbbbbbbbbbb): connection reset")))
	g.Expect(names(report.Deleted)).To(ConsistOf("capibm-e2e-a-iks"))
	g.Expect(report.Failed).To(HaveLen(1))
}

// TestExecutePlanRefusesAPlanWithoutAStorageChoice checks that a plan that does not say what happens to the
// persistent storage sends nothing, not even in a dry run: only PlanStaleClusters and PlanRunClusters build plans, and
// each sets the choice, so a plan without one is a programming error that must not reach IBM.
func TestExecutePlanRefusesAPlanWithoutAStorageChoice(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dry run %t", dryRun), func(t *testing.T) {
			g := NewWithT(t)
			api := &recordingAPI{}
			plan := DeletionPlan{Delete: []Cluster{{ID: "c0aaaaaaaaaaaaaaaaaa", Name: "capibm-e2e-a-iks"}}}

			report, err := ExecutePlan(context.Background(), api, plan, dryRun)

			g.Expect(err).To(MatchError(ContainSubstring(`deletion plan: the persistent storage choice "" is neither "Retain" nor "Delete"`)))
			g.Expect(api.deleted).To(BeEmpty())
			g.Expect(report.Deleted).To(BeEmpty())
		})
	}
}

// TestPlansChooseThePersistentStorage locks which plan asks IBM to delete the persistent storage: the backstop's plan of
// the run's own clusters asks IBM to delete it, the janitor's plan of stale clusters to keep it, also when they select
// nothing.
func TestPlansChooseThePersistentStorage(t *testing.T) {
	g := NewWithT(t)
	stale, err := PlanStaleClusters(nil, DefaultNamePrefix, JanitorMinimumAge, janitorNow)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(stale.PersistentStorage).To(Equal(kubernetesservice.RetainPersistentStorage))
	run, err := PlanRunClusters(nil, []string{"capibm-e2e-abc123-iks"}, janitorNow)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(run.PersistentStorage).To(Equal(kubernetesservice.DeletePersistentStorage))
}

func TestDeletionReportStringEmpty(t *testing.T) {
	g := NewWithT(t)
	g.Expect(DeletionReport{}.String()).To(Equal("no matching cluster\n"))
}

// recordingAPI is a ClusterAPI that records the deleted IDs and their storage choices and fails the IDs in errs.
type recordingAPI struct {
	deleted []string
	storage []kubernetesservice.PersistentStorage
	errs    map[string]error
}

func (a *recordingAPI) ListClusters(context.Context) ([]Cluster, error) { return nil, nil }

func (a *recordingAPI) DeleteCluster(_ context.Context, clusterID string, storage kubernetesservice.PersistentStorage) error {
	a.deleted = append(a.deleted, clusterID)
	a.storage = append(a.storage, storage)
	return a.errs[clusterID]
}

// names returns the cluster names.
func names(clusters []Cluster) []string {
	out := make([]string, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, c.Name)
	}
	return out
}

// toAny converts a want list for ConsistOf; an empty list means "none".
func toAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, v := range values {
		out = append(out, v)
	}
	return out
}
