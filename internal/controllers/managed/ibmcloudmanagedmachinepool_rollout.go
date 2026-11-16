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
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

// This file is the controller's part of the worker rollout: the scope decides and carries out every step
// (pkg/cloud/scope/managed/machinepool_rollout.go); the controller only makes sure that the pool is reconciled when a
// decision changes by the clock alone. spec.rollout.after is a time trigger (CAPI MachineDeployment semantics) and a
// replacement's Node must have been Ready for minReadySeconds before the next worker starts; neither changes a watched
// object, so without this requeue the rollout would start up to one steadyStatePoll late.

// rolloutRecheckMargin is added to the recheck time, so that the reconcile runs after the time has passed and not a
// moment before it (the rollout then finds the trigger still in the future and waits one more interval). The requeue
// gets no jitter: it is a deadline, not a poll.
const rolloutRecheckMargin = time.Second

// requeueForRollout shortens the requeue of result so that the pool is reconciled right after at (the scope's
// MachinePoolScope.RolloutRecheckAt). A result that is already due sooner, an error (controller-runtime's backoff then
// decides), a rate-limited outcome, and an at that is zero or not in the future leave result as it is. IBM's
// Retry-After of a 429 is a lower bound: the reconcile that a shorter requeue would bring sends IBM calls again (the
// listings, perhaps the resize that was answered with 429) before IBM allows them.
//
// Parameters:
//   - result is the result of the reconcile's outcome (resultFor).
//   - err is the reconcile's error.
//   - kind is the kind of the reconcile's outcome; OutcomeRateLimited keeps result.
//   - at is when the rollout decides differently without a watched change; zero for never.
//   - now is the controller clock's current time.
func requeueForRollout(result ctrl.Result, err error, kind managedscope.OutcomeKind, at, now time.Time) ctrl.Result {
	if err != nil || kind == managedscope.OutcomeRateLimited || at.IsZero() || !at.After(now) {
		return result
	}
	wait := at.Sub(now) + rolloutRecheckMargin
	if result.RequeueAfter == 0 || wait < result.RequeueAfter {
		result.RequeueAfter = wait
	}
	return result
}
