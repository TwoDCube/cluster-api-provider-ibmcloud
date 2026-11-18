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
	"slices"
	"strings"
	"time"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

const (
	// JanitorMinimumAge is the youngest a cluster may be for the janitor to delete it. It equals the hard timeout of
	// the longest lane's steps (IKS and ROKS, 4 h each), so a cluster this old has left the steps of its run. The age
	// alone does not prove that the run has ended: the lane's cleanups may still hold the cluster for up to 145 m more
	// (CAPI deletion, IBM backstop, inventory diff), and the ROKS lane's registry bucket cleanup adds up to 45 m before
	// the inventory diff. That a run has ended rests on the Boskos lease: one managed run at a time per e2e account,
	// with the lease held until the run returns (heartbeat_managed_lease in hack/scripts/ci/boskos.sh, which ci-e2e.sh
	// starts); a run that loses it stops its steps (lease.go).
	JanitorMinimumAge = 4 * time.Hour

	// RunCleanupClockSkew is how much earlier than the run's own start a cluster of the run may report its creation:
	// IBM's createdDate and the clock of the machine that runs the suite are not synchronized.
	RunCleanupClockSkew = 5 * time.Minute
)

// SkippedCluster is a cluster that matched the janitor's name rule but is not deleted, and why.
type SkippedCluster struct {
	// Cluster is the skipped cluster.
	Cluster Cluster
	// Reason says why it stays, for example "created 1h0m0s ago, younger than 4h0m0s".
	Reason string
}

// DeletionPlan is what one janitor run will delete. Clusters whose name does not match are in neither list, so the
// report never mentions clusters of other users of the account.
type DeletionPlan struct {
	// Delete are the clusters to delete, by ID.
	Delete []Cluster
	// Skipped are the matching clusters that stay.
	Skipped []SkippedCluster
	// PersistentStorage is what each DELETE asks IBM to do with the persistent storage of every cluster in Delete.
	// Both plans set Delete, because every cluster they select is an e2e test cluster whose volumes must not outlive
	// it (IBM's documentation conflicts on whether VPC Block volumes go even then). For PlanStaleClusters this matters
	// most: the janitor runs before the lane takes its inventory baseline, so storage it kept for a crashed run's
	// cluster would never show in any leftover check and would stay billed until someone found it by hand. The price
	// is that a cluster the janitor deletes by mistake loses its volumes too, which cannot be undone; the selection
	// (the e2e name prefix, an age beyond the longest lane, the Boskos lease that holds the account for one managed run
	// at a time, and E2E_MANAGED_JANITOR=false for an account shared with other runs) is what guards against that. The
	// choice stays explicit in the plan, rather than implied by ExecutePlan, so that the report says what each DELETE
	// asked and ExecutePlan can refuse a plan that names neither Retain nor Delete.
	PersistentStorage kubernetesservice.PersistentStorage
}

// PlanStaleClusters selects the clusters a crashed or interrupted run left behind: the name starts with
// "<prefix>-", IBM's createdDate is at least minAge before now, and the cluster is not already being deleted.
// A cluster whose createdDate does not parse stays (fail safe). The plan asks IBM to delete the persistent storage of
// the clusters it deletes too, like the run's own deletions (see DeletionPlan.PersistentStorage).
//
// Parameters:
//   - clusters is a ListClusters result.
//   - prefix is E2E_NAME_PREFIX; it must pass ValidateNamePrefix.
//   - minAge is the minimum age; it must be at least JanitorMinimumAge, so that no caller can make the janitor delete
//     a young cluster.
//   - now is the current time (an argument, so that tests can fix it).
func PlanStaleClusters(clusters []Cluster, prefix string, minAge time.Duration, now time.Time) (DeletionPlan, error) {
	if err := ValidateNamePrefix(prefix); err != nil {
		return DeletionPlan{}, err
	}
	if minAge < JanitorMinimumAge {
		return DeletionPlan{}, fmt.Errorf("janitor minimum age %s is shorter than %s: the janitor deletes only clusters "+
			"of runs that have ended, never a young cluster", minAge, JanitorMinimumAge)
	}
	plan := DeletionPlan{PersistentStorage: kubernetesservice.DeletePersistentStorage}
	for _, cluster := range clusters {
		if !strings.HasPrefix(cluster.Name, prefix+"-") {
			continue
		}
		if reason := keepReason(cluster); reason != "" {
			plan.Skipped = append(plan.Skipped, SkippedCluster{Cluster: cluster, Reason: reason})
			continue
		}
		created, _ := managed.ParseCreatedDate(cluster.CreatedDate) // keepReason has rejected unparsable dates
		if age := now.Sub(created); age < minAge {
			plan.Skipped = append(plan.Skipped, SkippedCluster{Cluster: cluster,
				Reason: fmt.Sprintf("created %s ago, younger than %s", age.Round(time.Second), minAge)})
			continue
		}
		plan.Delete = append(plan.Delete, cluster)
	}
	return plan, nil
}

// PlanRunClusters selects the clusters of the current run for the backstop of a lane's cleanup: the name is exactly
// one of names, the cluster was created after the run started (less RunCleanupClockSkew), and it is not already being
// deleted. It is the only selection without a minimum age, so it matches whole names, never a prefix. The plan asks
// IBM to delete the persistent storage of the clusters it deletes, so that a run leaves no volumes behind.
//
// Parameters:
//   - clusters is a ListClusters result.
//   - names are the full cluster names of the run (ClusterName results); none may be empty.
//   - runStart is the time the run started, taken before its first cluster was created.
func PlanRunClusters(clusters []Cluster, names []string, runStart time.Time) (DeletionPlan, error) {
	if len(names) == 0 || slices.Contains(names, "") {
		return DeletionPlan{}, fmt.Errorf("run cleanup needs the full cluster names of the run, got %q", names)
	}
	if runStart.IsZero() {
		return DeletionPlan{}, errors.New("run cleanup needs the run's start time, got the zero time")
	}
	earliest := runStart.Add(-RunCleanupClockSkew)
	plan := DeletionPlan{PersistentStorage: kubernetesservice.DeletePersistentStorage}
	for _, cluster := range clusters {
		if !slices.Contains(names, cluster.Name) {
			continue
		}
		if reason := keepReason(cluster); reason != "" {
			plan.Skipped = append(plan.Skipped, SkippedCluster{Cluster: cluster, Reason: reason})
			continue
		}
		if created, _ := managed.ParseCreatedDate(cluster.CreatedDate); created.Before(earliest) {
			plan.Skipped = append(plan.Skipped, SkippedCluster{Cluster: cluster,
				Reason: fmt.Sprintf("created at %s, before this run started at %s", created.UTC().Format(time.RFC3339),
					runStart.UTC().Format(time.RFC3339))})
			continue
		}
		plan.Delete = append(plan.Delete, cluster)
	}
	return plan, nil
}

// keepReason returns why a matching cluster must stay regardless of its age, or "": IBM is already deleting it (a
// second DELETE would add nothing), its ID is not an ID, or its createdDate does not parse.
//
// Parameters:
//   - cluster is a matching cluster of a ListClusters result.
func keepReason(cluster Cluster) string {
	if cluster.State == "deleting" || cluster.State == "deleted" {
		return "IBM is already deleting it (state " + cluster.State + ")"
	}
	if err := ValidateClusterID(cluster.ID); err != nil {
		return err.Error()
	}
	if _, err := managed.ParseCreatedDate(cluster.CreatedDate); err != nil {
		return "its age is unknown: " + err.Error()
	}
	return ""
}

// FailedDeletion is a planned deletion that IBM did not accept.
type FailedDeletion struct {
	// Cluster is the cluster that stays.
	Cluster Cluster
	// Err is the error of the DELETE request.
	Err error
}

// DeletionReport is the outcome of ExecutePlan.
type DeletionReport struct {
	// DryRun is true when nothing was sent.
	DryRun bool
	// Deleted are the clusters IBM accepted a DELETE for (or, in a dry run, would have been sent one).
	Deleted []Cluster
	// AlreadyGone are the clusters IBM answered 404 for: deleted between the listing and the request.
	AlreadyGone []Cluster
	// Failed are the deletions IBM refused or that got no answer.
	Failed []FailedDeletion
	// Skipped are the plan's skipped clusters, repeated for the log.
	Skipped []SkippedCluster
	// PersistentStorage is what the plan's DELETEs asked IBM to do with the persistent storage of the deleted clusters.
	PersistentStorage kubernetesservice.PersistentStorage
}

// String renders the report for the suite log: names, IDs and reasons, one line per cluster. A deletion that also asks
// IBM to delete the persistent storage says so, because that is the one part of it that cannot be undone or
// recovered; it says "asking", because what IBM deletes is not certain (see kubernetesservice.DeletePersistentStorage).
func (r DeletionReport) String() string {
	var b strings.Builder
	verb := "deleted"
	if r.DryRun {
		verb = "would delete (dry run)"
	}
	storage := ""
	if r.PersistentStorage == kubernetesservice.DeletePersistentStorage {
		storage = ", asking IBM to delete its persistent storage"
	}
	for _, c := range r.Deleted {
		fmt.Fprintf(&b, "%s %s (ID %s, created %s)%s\n", verb, c.Name, c.ID, c.CreatedDate, storage)
	}
	for _, c := range r.AlreadyGone {
		fmt.Fprintf(&b, "already gone: %s (ID %s)\n", c.Name, c.ID)
	}
	for _, f := range r.Failed {
		fmt.Fprintf(&b, "NOT deleted: %s (ID %s): %v\n", f.Cluster.Name, f.Cluster.ID, f.Err)
	}
	for _, s := range r.Skipped {
		fmt.Fprintf(&b, "kept: %s (ID %s): %s\n", s.Cluster.Name, s.Cluster.ID, s.Reason)
	}
	if b.Len() == 0 {
		return "no matching cluster\n"
	}
	return b.String()
}

// ExecutePlan deletes every cluster of plan.Delete by its ID, one request each, and continues after a failure so
// that one refused deletion does not keep the others alive. The returned error joins every failure; a 404 is not a
// failure (the cluster is gone). A plan whose PersistentStorage is neither Retain nor Delete is refused before any
// request, also in a dry run, so that no DELETE ever leaves the fate of the data to a missing value.
//
// Parameters:
//   - ctx bounds the requests.
//   - api is the IBM Cloud Kubernetes Service client.
//   - plan comes from PlanStaleClusters or PlanRunClusters; its PersistentStorage goes with every DELETE.
//   - dryRun reports the plan without sending any request.
func ExecutePlan(ctx context.Context, api ClusterAPI, plan DeletionPlan, dryRun bool) (DeletionReport, error) {
	if _, err := deleteResourcesQuery(plan.PersistentStorage); err != nil {
		return DeletionReport{}, fmt.Errorf("deletion plan: %w", err)
	}
	report := DeletionReport{DryRun: dryRun, Skipped: plan.Skipped, PersistentStorage: plan.PersistentStorage}
	var errs []error
	for _, cluster := range plan.Delete {
		if dryRun {
			report.Deleted = append(report.Deleted, cluster)
			continue
		}
		err := api.DeleteCluster(ctx, cluster.ID, plan.PersistentStorage)
		switch {
		case err == nil:
			report.Deleted = append(report.Deleted, cluster)
		case IsNotFound(err):
			report.AlreadyGone = append(report.AlreadyGone, cluster)
		default:
			report.Failed = append(report.Failed, FailedDeletion{Cluster: cluster, Err: err})
			errs = append(errs, fmt.Errorf("deleting cluster %s (ID %s): %w", cluster.Name, cluster.ID, err))
		}
	}
	return report, errors.Join(errs...)
}

// JanitorOptions configures RunJanitor.
type JanitorOptions struct {
	// NamePrefix is E2E_NAME_PREFIX; only clusters named "<NamePrefix>-..." are candidates.
	NamePrefix string
	// MinAge is the minimum age of a deleted cluster; at least JanitorMinimumAge.
	MinAge time.Duration
	// Now is the current time.
	Now time.Time
	// DryRun reports what would be deleted and sends no DELETE.
	DryRun bool
}

// RunJanitor lists the clusters and deletes, by ID, those PlanStaleClusters selects, asking IBM to delete their
// persistent storage too.
//
// Parameters:
//   - ctx bounds the requests.
//   - api is the IBM Cloud Kubernetes Service client, built with the credentials that create the e2e clusters.
//   - opts are the name prefix, the minimum age, the current time and the dry-run switch.
func RunJanitor(ctx context.Context, api ClusterAPI, opts JanitorOptions) (DeletionReport, error) {
	clusters, err := api.ListClusters(ctx)
	if err != nil {
		return DeletionReport{}, fmt.Errorf("janitor: listing clusters: %w", err)
	}
	plan, err := PlanStaleClusters(clusters, opts.NamePrefix, opts.MinAge, opts.Now)
	if err != nil {
		return DeletionReport{}, fmt.Errorf("janitor: %w", err)
	}
	return ExecutePlan(ctx, api, plan, opts.DryRun)
}

// DeleteRunClusters lists the clusters and deletes, by ID, those PlanRunClusters selects: the backstop that removes
// the run's own clusters, asking IBM to delete their persistent storage too, when the lane's CAPI deletion did not
// finish.
//
// Parameters:
//   - ctx bounds the requests.
//   - api is the IBM Cloud Kubernetes Service client.
//   - names are the full cluster names of the run.
//   - runStart is the time the run started.
//   - dryRun reports what would be deleted and sends no DELETE.
func DeleteRunClusters(ctx context.Context, api ClusterAPI, names []string, runStart time.Time, dryRun bool) (DeletionReport, error) {
	clusters, err := api.ListClusters(ctx)
	if err != nil {
		return DeletionReport{}, fmt.Errorf("run cleanup: listing clusters: %w", err)
	}
	plan, err := PlanRunClusters(clusters, names, runStart)
	if err != nil {
		return DeletionReport{}, fmt.Errorf("run cleanup: %w", err)
	}
	return ExecutePlan(ctx, api, plan, dryRun)
}
