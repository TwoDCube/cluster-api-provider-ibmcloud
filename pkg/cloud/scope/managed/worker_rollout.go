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
	"time"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file decides which worker a rollout replaces next. Three triggers make a worker due: its minor is behind the
// master's (always: workers follow the master minor), IBM offers it a newer fix pack (only under the Automatic
// policy), or spec.rollout.after has passed and its Node is older (always: CAPI MachineDeployment semantics). Both
// functions are pure; the pool controller runs the replace through DecideDisruptionStep, one worker at a time.

// WorkerUpdateRolloutRequested is a worker whose Node was created before spec.rollout.after, once that time has
// passed. Such a worker is always replaced, whatever the policy and whether or not it has an update: rollout.after
// recycles workers that are up to date too, as a MachineDeployment's rolloutAfter does.
const WorkerUpdateRolloutRequested WorkerUpdateClass = "RolloutRequested"

// WorkerUpdateDecision is ClassifyWorkerUpdate's answer for one worker.
type WorkerUpdateDecision struct {
	// Class is the class the worker reports in messages and status: the first that matches in the order MinorBehind,
	// PatchAvailable, RolloutRequested, else UpToDate.
	Class WorkerUpdateClass

	// Due is true when any class of the worker makes it due: MinorBehind, PatchAvailable under the Automatic policy,
	// or RolloutRequested.
	Due bool

	// DueBecause is the first class, in the same order, that makes the worker due: the reason its replace reports.
	// "" when the worker is not due. It differs from Class for a worker with a newer fix pack under the Manual policy
	// whose Node is older than a passed spec.rollout.after: Class PatchAvailable, DueBecause RolloutRequested.
	DueBecause WorkerUpdateClass
}

// ClassifyWorkerUpdate classifies one worker for a rollout. It is pure.
//
// The worker is MinorBehind or PatchAvailable as UpdateAvailability says, and RolloutRequested when rolloutAfter is
// set and not after now and the worker's Node was created before rolloutAfter. The reported class is the first match;
// the worker is due when any class makes it due: MinorBehind always, PatchAvailable only under Automatic,
// RolloutRequested always. So Manual plus a newer fix pack plus a passed rollout.after on an older Node is due, with
// DueBecause RolloutRequested, as CAPI MachineDeployment semantics require. A worker IBM lists as deleted is UpToDate
// and never due.
//
// Parameters:
//   - w: the worker as ListWorkers returned it.
//   - masterMinor: the master's minor, as for UpdateAvailability.
//   - policy: the pool's spec.workerUpdates.policy; "" means Manual, the API's documented default.
//   - rolloutAfter: the pool's spec.rollout.after; the zero time means unset.
//   - nodeCreated: the creationTimestamp of the worker's Node, found through ClusterCache by its provider ID; the zero
//     time means that the worker has no Node, and such a worker is never RolloutRequested (there is nothing older to
//     recycle, and a worker still joining must not be replaced).
//   - now: the controller's clock reading.
//
// It returns an error naming the argument to fix when policy is not "", Manual or Automatic, or now is zero.
func ClassifyWorkerUpdate(w kubernetesservice.Worker, masterMinor ibmversion.Minor, policy infrav1.WorkerUpdatePolicy,
	rolloutAfter, nodeCreated, now time.Time) (WorkerUpdateDecision, error) {
	if err := validateRolloutSettings(policy, now); err != nil {
		return WorkerUpdateDecision{}, err
	}
	availability := UpdateAvailability(w, masterMinor)
	rolloutRequested := w.ActualState != WorkerStateDeleted && !rolloutAfter.IsZero() && !rolloutAfter.After(now) &&
		!nodeCreated.IsZero() && nodeCreated.Before(rolloutAfter)

	decision := WorkerUpdateDecision{Class: WorkerUpdateUpToDate}
	switch {
	case availability != WorkerUpdateUpToDate:
		decision.Class = availability
	case rolloutRequested:
		decision.Class = WorkerUpdateRolloutRequested
	}
	switch {
	case availability == WorkerUpdateMinorBehind:
		decision.DueBecause = WorkerUpdateMinorBehind
	case availability == WorkerUpdatePatchAvailable && policy == infrav1.WorkerUpdatePolicyAutomatic:
		decision.DueBecause = WorkerUpdatePatchAvailable
	case rolloutRequested:
		decision.DueBecause = WorkerUpdateRolloutRequested
	}
	decision.Due = decision.DueBecause != ""
	return decision, nil
}

// validateRolloutSettings returns an error naming the first invalid rollout input.
//
// Parameters:
//   - policy: spec.workerUpdates.policy.
//   - now: the controller's clock reading.
func validateRolloutSettings(policy infrav1.WorkerUpdatePolicy, now time.Time) error {
	switch policy {
	case "", infrav1.WorkerUpdatePolicyManual, infrav1.WorkerUpdatePolicyAutomatic:
	default:
		return fmt.Errorf("spec.workerUpdates.policy is %q; want %q (also when omitted) or %q", policy,
			infrav1.WorkerUpdatePolicyManual, infrav1.WorkerUpdatePolicyAutomatic)
	}
	if now.IsZero() {
		return errors.New("the rollout decision needs the controller clock's current time; now is not set")
	}
	return nil
}

// ReplacementCandidates is what PickReplacement reads.
type ReplacementCandidates struct {
	// Workers are every worker of the cluster from one ListWorkers call (all pools).
	Workers []kubernetesservice.Worker

	// PoolID is the IBM pool ID, status.poolID; only its workers are candidates. Required.
	PoolID string

	// MasterMinor is the master's minor, as for UpdateAvailability.
	MasterMinor ibmversion.Minor

	// Policy is the pool's spec.workerUpdates.policy; "" means Manual.
	Policy infrav1.WorkerUpdatePolicy

	// RolloutAfter is the pool's spec.rollout.after; the zero time means unset.
	RolloutAfter time.Time

	// NodeCreated maps a worker ID to the creationTimestamp of its Node, found through ClusterCache by provider ID. A
	// worker without an entry has no Node.
	NodeCreated map[string]time.Time

	// Now is the controller's clock reading. Required.
	Now time.Time
}

// ReplacementPick is the worker PickReplacement chose and why.
type ReplacementPick struct {
	// Worker is the worker to replace.
	Worker kubernetesservice.Worker

	// Decision is its ClassifyWorkerUpdate decision; Decision.DueBecause is the reason of the replace.
	Decision WorkerUpdateDecision
}

// PickReplacement returns the next due worker of the pool. It is pure. A candidate is a worker of the pool that
// ClassifyWorkerUpdate finds due and that IBM is not already removing or changing (WorkerBeingRemoved: deleting,
// deleted or a pending operation); among the candidates it picks in an order that mirrors the one IBM documents for
// the workers a pool resize removes (unhealthy first, older versions first):
//
//  1. an unhealthy worker (not IsWorkerReady) first: replacing it restores capacity instead of taking it away;
//  2. MinorBehind before every other class, because the master minor moves on and a worker more than the allowed skew
//     behind blocks the next master upgrade (BlockedByWorkerSkew);
//  3. the oldest kubeVersion.actual, a version that does not parse first;
//  4. the zone, then the worker ID, ascending, so that the choice is deterministic.
//
// Parameters:
//   - in: the workers and the pool's rollout settings; see ReplacementCandidates.
//
// It returns the pick and true, or false when no worker is due, and an error naming the field to fix when in is
// invalid.
func PickReplacement(in ReplacementCandidates) (ReplacementPick, bool, error) {
	if in.PoolID == "" {
		return ReplacementPick{}, false, errors.New("ReplacementCandidates.PoolID is empty; pass the " +
			"IBMCloudManagedMachinePool's status.poolID")
	}
	if err := validateRolloutSettings(in.Policy, in.Now); err != nil {
		return ReplacementPick{}, false, err
	}
	var candidates []ReplacementPick
	for _, w := range in.Workers {
		if w.PoolID != in.PoolID || WorkerBeingRemoved(w) {
			continue
		}
		decision, err := ClassifyWorkerUpdate(w, in.MasterMinor, in.Policy, in.RolloutAfter, in.NodeCreated[w.ID], in.Now)
		if err != nil {
			return ReplacementPick{}, false, err
		}
		if decision.Due {
			candidates = append(candidates, ReplacementPick{Worker: w, Decision: decision})
		}
	}
	if len(candidates) == 0 {
		return ReplacementPick{}, false, nil
	}
	return slices.MinFunc(candidates, compareReplacementOrder), true, nil
}

// compareReplacementOrder orders two due workers so that the one PickReplacement replaces first sorts first.
//
// Parameters:
//   - a, b: the candidates to compare.
func compareReplacementOrder(a, b ReplacementPick) int {
	// compareBool orders false before true: an unhealthy worker (ready false) and a MinorBehind worker (not
	// MinorBehind false) sort first.
	return cmp.Or(
		compareBool(IsWorkerReady(a.Worker), IsWorkerReady(b.Worker)),
		compareBool(a.Decision.Class != WorkerUpdateMinorBehind, b.Decision.Class != WorkerUpdateMinorBehind),
		compareWorkerVersions(a.Worker, b.Worker),
		strings.Compare(a.Worker.Zone, b.Worker.Zone),
		strings.Compare(a.Worker.ID, b.Worker.ID),
	)
}
