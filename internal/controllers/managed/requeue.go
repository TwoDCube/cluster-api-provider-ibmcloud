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
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	ctrl "sigs.k8s.io/controller-runtime"

	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

// This file is the cadence of the managed controllers: the named intervals, each with the reason for its value, and
// the mapping from a scope outcome to a ctrl.Result. Every interval gets jitter, so that the clusters a controller
// manages do not all poll IBM in the same second after a restart.

// requeueInterval returns the duration of a named interval, before jitter. interval is the name a scope step chose; an
// unknown name is a programming error.
func requeueInterval(interval managedscope.Interval) (time.Duration, error) {
	switch interval {
	case managedscope.IntervalClusterProvisioning:
		// The master is Ready 16-25 minutes after the create and the cluster is normal after about 25 (IKS) or 33 (ROKS)
		// minutes (observed on IBM Cloud); one minute keeps the wait short at one GetCluster per minute.
		return time.Minute, nil
	case managedscope.IntervalMasterUpdate:
		// A master minor update takes 30-60 minutes.
		return 2 * time.Minute, nil
	case managedscope.IntervalPoolTransition:
		// Pool and worker states move within minutes (one worker replace took about 5.7 minutes on IBM Cloud).
		return 30 * time.Second, nil
	case managedscope.IntervalDrainRetry:
		// Cluster API parity: the Machine drain cadence (managedscope.DrainRetryInterval cites the line).
		return managedscope.DrainRetryInterval, nil
	case managedscope.IntervalSteadyState:
		// Nothing is in progress, but IBM applies patches on its own and fix packs, health, CA rotation and kubeconfig
		// deadlines change; five minutes stays below the default --sync-period of ten minutes.
		return 5 * time.Minute, nil
	case managedscope.IntervalCARotation:
		// CA rotation moves through several states within minutes (not yet verified against IBM Cloud).
		return time.Minute, nil
	case managedscope.IntervalGuarded:
		// A disruption waits for the cluster minimum, the settled gate, capacity or the disruption token.
		return 2 * time.Minute, nil
	case managedscope.IntervalDelete:
		// An IBM deletion is asynchronous: IBM Cloud answers 404 for the cluster about five minutes after the DELETE.
		return time.Minute, nil
	case managedscope.IntervalTerminalRecheck:
		// A condition needs a human; the recheck re-evaluates stateless decisions without spending the rate limit, and a
		// spec edit re-triggers earlier.
		return 15 * time.Minute, nil
	case managedscope.IntervalInfraRevalidate:
		// A bring-your-own VPC network rarely changes; this re-check notices a deleted subnet or VPC.
		return 30 * time.Minute, nil
	default:
		return 0, fmt.Errorf("managed: unknown requeue interval %q (programming error in a scope step)", interval)
	}
}

// jitter returns a random duration in [0.9*d, 1.1*d): ±10 % around d, built on wait.Jitter, which only adds. d is the
// interval before jitter.
func jitter(d time.Duration) time.Duration {
	return wait.Jitter(d*9/10, 2.0/9.0)
}

// jitterUp returns a random duration in [d, 1.1*d). A rate-limit backoff never shrinks below IBM's Retry-After or
// the minimum backoff, so its jitter only adds. d is the backoff.
func jitterUp(d time.Duration) time.Duration {
	return wait.Jitter(d, 0.1)
}

// resultFor maps the outcome of a scope step to what Reconcile returns: Done requeues at the controller's steady
// interval, NotDone after its interval, RateLimited after its backoff, Terminal after IntervalTerminalRecheck, all
// with a nil error; Transient returns its error, so controller-runtime retries with backoff.
//
// Parameters:
//   - outcome is the step's outcome.
//   - steady is the interval at which this controller re-checks a finished object, for example
//     IntervalInfraRevalidate for the IBMCloudManagedCluster.
func resultFor(outcome managedscope.Outcome, steady managedscope.Interval) (ctrl.Result, error) {
	var interval managedscope.Interval
	switch outcome.Kind {
	case managedscope.OutcomeDone:
		interval = steady
	case managedscope.OutcomeNotDone:
		interval = outcome.Interval
	case managedscope.OutcomeTerminal:
		interval = managedscope.IntervalTerminalRecheck
	case managedscope.OutcomeRateLimited:
		return ctrl.Result{RequeueAfter: jitterUp(outcome.RetryAfter)}, nil
	case managedscope.OutcomeTransient:
		return ctrl.Result{}, outcome.Err
	default:
		return ctrl.Result{}, fmt.Errorf("managed: unknown outcome kind %q (programming error in a scope step)", outcome.Kind)
	}
	after, err := requeueInterval(interval)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: jitter(after)}, nil
}
