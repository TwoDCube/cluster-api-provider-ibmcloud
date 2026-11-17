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
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

// This file is the control plane controller's part of the master minor update: the scope decides and calls IBM
// (ControlPlaneScope.reconcileMasterVersion); the controller owns the cadence, so it merges the step's outcome into
// the requeue of the reconcile and requeues no later than the time the decision may change on its own
// (MasterAction.RecheckAt): the end of the pending grace, or the retry time a held request's message names. Without
// that deadline, a request refused with 403 would be sent again only at the first poll after its retry time: the
// earlier of the flow's requeue (usually the 5 m steady poll) and the step's terminalRecheck, so up to one poll
// interval later than the time the condition promises. With it, a role granted after a 403 takes effect at the retry
// time the message names (up to 24 h away after repeated refusals), with no spec change and no manual trigger.

// resultWithMasterVersion maps the outcome of ReconcileNormal to a ctrl.Result (resultFor), merges the master version
// step into it and requeues no later than the kubeconfig deadline. It is the one place that composes the requeue of
// the normal flow, so that the rate-limit rule below holds for every deadline. The rules, in order:
//
//   - An error of either wins, so that controller-runtime retries with backoff (Transient).
//   - A rate limit of the flow is kept as it is: IBM's Retry-After is a lower bound for every call of the identity, and
//     the next reconcile starts with GetCluster, so neither the step, its recheck time nor the kubeconfig deadline may
//     bring the next reconcile forward. The scope already skips the step after a rate limit of the access steps
//     (ControlPlaneScope.reconcileMasterVersion); this rule keeps the requeue right whatever the step reports.
//   - A rate limit of the step replaces the flow's requeue for the same reason, and no deadline shortens it: the
//     kubeconfig step ran before the master version step in the same reconcile, so its deadline (the re-check of a CA
//     near expiry every minute, a 401 backoff of 1 to 10 minutes) knows nothing of IBM's later 429.
//   - A step that is Done, or did not run, leaves the flow's requeue (the steady poll, or a longer wait the flow chose
//     itself, such as terminalRecheck for a condition that needs a human), no later than the kubeconfig deadline.
//   - Otherwise the earlier of the two requeues, no later than the earlier of recheck and the kubeconfig deadline, so
//     that a held request is sent again at the retry time its message names.
//
// Both deadlines get upward jitter only (requeueAtKubeconfigDeadline): requeued earlier, the decision they belong to
// would find nothing due yet. A Terminal outcome of the step is logged like the flow's own.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - outcome is ReconcileNormal's outcome.
//   - master is the master version step's outcome (ControlPlaneScope.MasterVersionOutcome); Kind "" when it did not run.
//   - recheck is ControlPlaneScope.MasterVersionRequeueAfter; 0 when the decision names no recheck time.
//   - kubeconfigDeadline is ControlPlaneScope.KubeconfigRequeueAfter; 0 when managedkubeconfig.Decide did not run.
func resultWithMasterVersion(ctx context.Context, outcome, master managedscope.Outcome, recheck, kubeconfigDeadline time.Duration) (ctrl.Result, error) {
	result, err := resultFor(outcome, managedscope.IntervalSteadyState)
	if err != nil || outcome.Kind == managedscope.OutcomeRateLimited {
		return result, err
	}
	if master.Kind == "" || master.Kind == managedscope.OutcomeDone {
		return requeueAtKubeconfigDeadline(result, nil, kubeconfigDeadline)
	}
	logControlPlaneTerminal(ctx, master)
	masterResult, masterErr := resultFor(master, managedscope.IntervalSteadyState)
	if masterErr != nil || master.Kind == managedscope.OutcomeRateLimited {
		return masterResult, masterErr
	}
	if masterResult.RequeueAfter > 0 && masterResult.RequeueAfter < result.RequeueAfter {
		result = masterResult
	}
	deadline := kubeconfigDeadline
	if recheck > 0 && (deadline <= 0 || recheck < deadline) {
		deadline = recheck
	}
	return requeueAtKubeconfigDeadline(result, nil, deadline)
}
