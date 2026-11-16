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
	"errors"
	"time"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file is the outcome model of the managed scopes: every scope step says how its work ended, and the controller
// turns that into a ctrl.Result. Expected waits are outcomes with a nil error, so that a cluster that takes 25 minutes
// to provision does not fill the logs with errors or drive controller-runtime's exponential backoff; only failures
// that a retry may fix are returned as errors.

// OutcomeKind is the class of an Outcome.
type OutcomeKind string

const (
	// OutcomeDone means the step's work is complete; the controller requeues at its steady interval.
	OutcomeDone OutcomeKind = "Done"
	// OutcomeNotDone means IBM is working or the step waits for something; the controller requeues after the named
	// Interval with a nil error.
	OutcomeNotDone OutcomeKind = "NotDone"
	// OutcomeTransient means a failure a retry may fix (5xx, network, an unknown 409); the controller returns the
	// error, so controller-runtime retries with backoff.
	OutcomeTransient OutcomeKind = "Transient"
	// OutcomeRateLimited means IBM answered 429; the controller requeues after RetryAfter with a nil error.
	OutcomeRateLimited OutcomeKind = "RateLimited"
	// OutcomeTerminal means a human must act (a condition says what); the controller rechecks after
	// IntervalTerminalRecheck, and a spec edit re-triggers earlier.
	OutcomeTerminal OutcomeKind = "Terminal"
)

// Interval names one requeue interval of the managed controllers. A scope says which wait applies; the controllers own
// the durations and their jitter (internal/controllers/managed/requeue.go), so the whole cadence of the managed
// controllers is one table there.
type Interval string

const (
	// IntervalClusterProvisioning is clusterProvisioningPoll: a new IBM cluster is provisioning (observed on IBM Cloud:
	// the master is ready 16 to 25 minutes after the create).
	IntervalClusterProvisioning Interval = "ClusterProvisioning"
	// IntervalMasterUpdate is masterUpdatePoll: a master minor update is running.
	IntervalMasterUpdate Interval = "MasterUpdate"
	// IntervalPoolTransition is poolTransitionPoll: pool and worker states are moving (observed on IBM Cloud: they change
	// within minutes, one worker replacement took about 6 minutes).
	IntervalPoolTransition Interval = "PoolTransition"
	// IntervalDrainRetry is drainRetryInterval: a drain evicts again (CAPI parity, DrainRetryInterval).
	IntervalDrainRetry Interval = "DrainRetry"
	// IntervalSteadyState is steadyStatePoll: nothing is in progress; IBM may still change things on its own.
	IntervalSteadyState Interval = "SteadyState"
	// IntervalCARotation is caRotationPoll: a CA rotation is in progress.
	IntervalCARotation Interval = "CARotation"
	// IntervalGuarded is guardedPoll: a disruption waits for the cluster minimum, the settled gate, capacity or the
	// disruption token.
	IntervalGuarded Interval = "Guarded"
	// IntervalDelete is deletePoll: an IBM deletion is asynchronous (observed on IBM Cloud: a deleted cluster is answered
	// with 404 about 5 minutes after the DELETE).
	IntervalDelete Interval = "Delete"
	// IntervalTerminalRecheck is terminalRecheck: a condition needs a human.
	IntervalTerminalRecheck Interval = "TerminalRecheck"
	// IntervalInfraRevalidate is infraRevalidatePoll: the BYO VPC network of an IBMCloudManagedCluster is re-checked.
	IntervalInfraRevalidate Interval = "InfraRevalidate"
)

// Outcome is how one scope step ended. Build it with Done, NotDone, Transient, RateLimited or Terminal; the zero value
// is not a valid outcome.
type Outcome struct {
	// Kind is the class of the outcome.
	Kind OutcomeKind
	// Interval is the wait of a NotDone outcome.
	Interval Interval
	// RetryAfter is the wait of a RateLimited outcome; at least kubernetesservice.MinRateLimitBackoff.
	RetryAfter time.Duration
	// Reason is the condition reason of a NotDone or Terminal outcome, for logs; the step has already set the
	// condition on the object.
	Reason string
	// Message is the condition message of a Terminal outcome, for logs.
	Message string
	// Err is the error of a Transient outcome; never nil for that kind.
	Err error
}

// Done returns the outcome of a step whose work is complete.
func Done() Outcome {
	return Outcome{Kind: OutcomeDone}
}

// NotDone returns the outcome of a step that waits.
//
// Parameters:
//   - interval names the wait; the controller adds jitter.
//   - reason is the condition reason the step set, for logs.
func NotDone(interval Interval, reason string) Outcome {
	return Outcome{Kind: OutcomeNotDone, Interval: interval, Reason: reason}
}

// Transient returns the outcome of a failure that a retry may fix. err is the failure; nil is a programming error
// and is replaced by an error that says so, because a Transient outcome without an error would requeue nothing.
func Transient(err error) Outcome {
	if err == nil {
		err = errors.New("managed: Transient outcome built without an error (programming error)")
	}
	return Outcome{Kind: OutcomeTransient, Err: err}
}

// RateLimited returns the outcome of an IBM 429. retryAfter is IBM's Retry-After (0 when absent); the wait is never
// shorter than kubernetesservice.MinRateLimitBackoff, whether or not IBM sends a Retry-After (the shape of IBM's 429 is
// not yet verified against IBM Cloud), so a rate-limited controller backs off well below IBM's limit of 200 requests
// per 10 seconds per source IP.
func RateLimited(retryAfter time.Duration) Outcome {
	return Outcome{Kind: OutcomeRateLimited, RetryAfter: max(retryAfter, kubernetesservice.MinRateLimitBackoff)}
}

// Terminal returns the outcome of a condition that needs a human.
//
// Parameters:
//   - reason is the condition reason the step set.
//   - message is the condition message the step set; it says what to fix.
func Terminal(reason, message string) Outcome {
	return Outcome{Kind: OutcomeTerminal, Reason: reason, Message: message}
}
