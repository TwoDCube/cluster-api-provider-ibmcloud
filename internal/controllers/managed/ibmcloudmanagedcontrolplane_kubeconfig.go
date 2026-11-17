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
)

// This file is the control plane's requeue rule: requeue = min(state interval, kubeconfig deadline, caRotationPoll
// while rotating), never before IBM's Retry-After. The scope step's outcome names the state interval (resultFor); the
// kubeconfig deadline is managedkubeconfig.Decide's RequeueAfter of the same reconcile (a 401 backoff, a CA rotation
// poll, a certificate or CA re-check), a duration that no named interval expresses, so the controller applies it
// (resultWithMasterVersion), after the rate-limit rules: Decide runs before the master version step of the same
// reconcile and cannot know of a 429 that step gets.

// requeueAtKubeconfigDeadline returns result, requeued no later than deadline. An error is returned unchanged:
// controller-runtime then retries with backoff and ignores RequeueAfter. A deadline of 0 (Decide did not run) changes
// nothing. The deadline gets upward jitter only (jitterUp): requeued earlier, Decide would find nothing due yet and
// schedule the same deadline again. It does not look at the outcome kind, so a caller must not pass the result of a
// rate limit (resultWithMasterVersion keeps those as they are).
//
// Parameters:
//   - result and err are what resultFor returned for the step's outcome.
//   - deadline is ControlPlaneScope.KubeconfigRequeueAfter, or the earlier of it and the master version step's
//     recheck time.
func requeueAtKubeconfigDeadline(result ctrl.Result, err error, deadline time.Duration) (ctrl.Result, error) {
	if err != nil || deadline <= 0 {
		return result, err
	}
	if result.RequeueAfter == 0 || deadline < result.RequeueAfter {
		result.RequeueAfter = jitterUp(deadline)
	}
	return result, nil
}
