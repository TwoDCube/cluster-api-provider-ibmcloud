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
// while rotating). The scope step's outcome names the state interval (resultFor); the kubeconfig deadline is
// managedkubeconfig.Decide's RequeueAfter of the same reconcile (a 401 backoff, a CA rotation poll, a certificate or
// CA re-check), a duration that no named interval expresses, so the controller applies it here.

// requeueAtKubeconfigDeadline returns result, requeued no later than deadline. An error is returned unchanged:
// controller-runtime then retries with backoff and ignores RequeueAfter. A deadline of 0 (Decide did not run) changes
// nothing. The deadline gets upward jitter only (jitterUp): requeued earlier, Decide would find nothing due yet and
// schedule the same deadline again.
//
// Parameters:
//   - result and err are what resultFor returned for the step's outcome.
//   - deadline is ControlPlaneScope.KubeconfigRequeueAfter.
func requeueAtKubeconfigDeadline(result ctrl.Result, err error, deadline time.Duration) (ctrl.Result, error) {
	if err != nil || deadline <= 0 {
		return result, err
	}
	if result.RequeueAfter == 0 || deadline < result.RequeueAfter {
		result.RequeueAfter = jitterUp(deadline)
	}
	return result, nil
}
