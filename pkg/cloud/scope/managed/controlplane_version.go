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
	"fmt"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file is the master version step of the control plane flow: the master minor update. The decision is the pure
// DecideMasterAction (master_version.go); the scope only gathers its inputs, makes the IBM calls it names and writes
// its answer into status.masterUpdate and the MasterVersionUpToDate and RollingOut conditions:
//
//   - The inputs that cost a call are fetched only when the decision asks for them: IBM's offered versions through the
//     VersionsCache, keyed by identity and region (NeedsOfferedVersions), and the workers through one ListWorkers
//     (NeedsWorkers), each followed by a fresh decision.
//   - After the decision, the newer minors IBM offers go to status.availableDistributionVersions and into the True
//     MasterVersionUpToDate message (reconcileAvailableVersions). They are informational and never make the master
//     outdated. They need the offered versions on every run, which the VersionsCache answers: IBM is asked at most once
//     an hour per identity and region, and a failed listing at most once per VersionsFailureHold, or after a 429 once
//     IBM's Retry-After is over. A reconcile that requests nothing therefore makes GetCluster and, about once an hour,
//     ListVersions.
//   - A request first writes status.masterUpdate with MasterVersionUpToDate=False/UpgradePending and RollingOut=True in
//     a direct status patch (PersistFunc). Only when that write succeeded is UpdateMaster sent, so that every request IBM
//     may have received is on record and the grace and backoff rows hold the next one.
//   - IBM's answer (MasterUpdateCallOutcome) is folded back into the inputs and decided once more: a Terminal refusal
//     becomes the condition the refusal row keeps, an accepted request the UpgradePending of row 10. The conditions
//     this reconcile writes are therefore the ones the next reconcile computes from the same state, so the controller's
//     own status patch starts no second one, and a repeated refusal writes nothing, because the messages carry IBM's
//     error code only, never the incident ID that changes with every answer.
//   - The decision's RecheckAt (the end of the pending grace, or the retry time a message names) goes to the controller
//     through MasterVersionRequeueAfter, which requeues no later than it: a refused or failed request is sent again at
//     that time without a spec change or a manual trigger.
//   - A call for an input that IBM answers definitively after a raise of spec.distributionVersion (a 403, 400 or 404 to
//     ListVersions or ListWorkers) sets MasterVersionUpToDate=False with PermissionDenied or IBMAPIError
//     (masterInputFailure), so the condition no longer says the master runs the minor the spec asks for; the call is
//     made again at the next poll.
//
// The step runs only for an initialized control plane: until then IBM is still creating the master at
// spec.distributionVersion. It does not run in a reconcile in which IBM answered a call of the access steps with 429
// either: IBM's Retry-After holds its calls too.

// masterVersionStep is what the master version step of one reconcile left for the controller's requeue.
type masterVersionStep struct {
	// outcome is the step's outcome; the zero Outcome when the step did not run in this reconcile.
	outcome Outcome
	// recheckAt is MasterAction.RecheckAt of the last decision: when the decision may change without a watched change;
	// the zero time when no such time exists.
	recheckAt time.Time
}

// MasterVersionOutcome returns the outcome of the master version step (reconcileMasterVersion) in this reconcile: the
// zero Outcome (Kind "") when the step did not run, because the IBM cluster is not bound, does not exist yet, or the
// control plane is not initialized. The controller merges it with the outcome of ReconcileNormal: an error wins, and
// otherwise the earlier requeue.
func (s *ControlPlaneScope) MasterVersionOutcome() Outcome {
	return s.masterStep.outcome
}

// MasterVersionRequeueAfter returns how long from now the master version decision may change without any watched
// change (MasterAction.RecheckAt: the end of MasterUpdatePendingGrace, or the retry time of an update that has not taken
// effect or that IBM refused), or 0 when there is no such time. The controller requeues no later than this, so that a
// held request is sent again at the retry time its message names (DecideMasterAction rows 10 and 11 and the refusal
// row). A time already reached gives one second, the shortest wait that still lets the clock pass it.
func (s *ControlPlaneScope) MasterVersionRequeueAfter() time.Duration {
	recheck := s.masterStep.recheckAt
	if recheck.IsZero() {
		return 0
	}
	return max(recheck.Sub(s.params.Clock.Now()), time.Second)
}

// reconcileMasterVersion runs the master version step for a bound IBM cluster that exists, once the control plane is
// initialized, and keeps its outcome and recheck time for the controller (MasterVersionOutcome,
// MasterVersionRequeueAfter). It leaves status untouched, makes no IBM call and reports no outcome:
//
//   - before initialization;
//   - when IBM answered a call of the access steps of this reconcile with 429 (flow is RateLimited, for example a
//     kubeconfig refresh, fetchFailed). IBM's Retry-After is a lower bound for the next call, and the process-wide
//     request limiter is a token bucket that does not know it, so the step's ListVersions, ListWorkers or
//     UpdateMaster would go out inside IBM's wait. An UpdateMaster that IBM then refused with 429 would still be on
//     record in status.masterUpdate, so the grace and backoff rows would hold the next request for an hour and report
//     UpgradeFailed for a request IBM never took. The controller keeps the flow's Retry-After requeue
//     (resultWithMasterVersion), and the next reconcile runs the step from the persisted status and IBM's state, so
//     nothing is lost by waiting.
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is this reconcile's GetCluster answer.
//   - flow is the outcome of the access steps of this reconcile (reconcileAccess).
func (s *ControlPlaneScope) reconcileMasterVersion(ctx context.Context, c *kubernetesservice.Cluster, flow Outcome) {
	if !ptr.Deref(s.ControlPlane.Status.Initialization.ControlPlaneInitialized, false) || flow.Kind == OutcomeRateLimited {
		return
	}
	s.masterStep = s.masterVersion(ctx, c)
}

// masterVersion decides the master update, reports the newer minors IBM offers (reconcileAvailableVersions), writes the
// decision, and makes the request when it says so.
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is this reconcile's GetCluster answer.
func (s *ControlPlaneScope) masterVersion(ctx context.Context, c *kubernetesservice.Cluster) masterVersionStep {
	in := s.masterInput(c)
	action, failed, ok := s.decideMaster(ctx, &in)
	if !ok {
		return failed
	}
	if s.reconcileAvailableVersions(ctx, &in) && action.UpToDate.Status == metav1.ConditionTrue {
		// The True message names the newer minors (MasterVersionInput.Available), which have just changed. The input is
		// the one decided a moment ago, so the decision does not fail, and a True needs no input that costs a call.
		if decided, err := DecideMasterAction(in); err == nil {
			action = decided
		}
	}
	if action.RequestUpdate {
		return s.requestMasterUpdate(ctx, in, action)
	}
	s.applyMasterAction(action)
	return masterVersionStep{outcome: masterWaitOutcome(action), recheckAt: action.RecheckAt}
}

// masterInput returns the MasterVersionInput of this reconcile without the inputs that cost a call: the current
// MasterVersionUpToDate condition, the only record of IBM's terminal answer to an earlier UpdateMaster, and the
// generation it must have been written for. Now is the clock at the precision of the API (ControlPlaneScope.now), so
// that the lastRequestTime a request records equals the one the next reconcile reads back.
//
// Parameters:
//   - c is this reconcile's GetCluster answer.
func (s *ControlPlaneScope) masterInput(c *kubernetesservice.Cluster) MasterVersionInput {
	cp := s.ControlPlane
	return MasterVersionInput{
		Distribution:    cp.Spec.Distribution,
		DesiredVersion:  cp.Spec.DistributionVersion,
		Cluster:         c,
		MasterUpdate:    cp.Status.MasterUpdate,
		CurrentUpToDate: masterConditionCopy(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition),
		Generation:      cp.Generation,
		Available:       cp.Status.AvailableDistributionVersions,
		Now:             s.now().Time,
	}
}

// reconcileAvailableVersions sets status.availableDistributionVersions to the minors newer than the master's that IBM
// offers for its distribution (newerOfferedMinors), sets in.Available to the same list for the True
// MasterVersionUpToDate message, and reports whether the list changed; status is written only on a change. It runs
// after the decision and uses the offered versions the decision fetched (in.Offered), or else lists them through the
// VersionsCache (listOfferedVersions), so that a step lists at most once.
//
// The list never changes MasterVersionUpToDate. A listing that fails here is logged and changes nothing else: the list
// keeps its last value, without the minors the master has reached since (keptNewerMinors), and the VersionsCache holds
// the failure for VersionsFailureHold (a 429 for IBM's Retry-After), so that an IBM outage is not called on every
// reconcile. A failed listing that the decision itself needs, after a raise of spec.distributionVersion, ends the step
// before this runs (masterInputFailed), and the list keeps its value then too. Nothing is done when IBM's master
// version does not parse: the decision reports that as Unknown, and the minors newer than an unknown one are unknown.
//
// Parameters:
//   - ctx is the reconcile context.
//   - in is the decided input; Available is set on it.
func (s *ControlPlaneScope) reconcileAvailableVersions(ctx context.Context, in *MasterVersionInput) bool {
	v, err := in.parse()
	if err != nil || !v.masterParsed {
		return false
	}
	status := &s.ControlPlane.Status
	offered := in.Offered
	if offered == nil {
		versions, callErr, err := s.listOfferedVersions(ctx)
		switch {
		case err != nil:
			ctrl.LoggerFrom(ctx).Info("CAPIBM cannot list the versions IBM Cloud offers, so status.availableDistributionVersions "+
				"keeps its last value", "problem", err.Error())
		case callErr != nil:
			ctrl.LoggerFrom(ctx).Info("IBM Cloud did not list the versions it offers, so status.availableDistributionVersions "+
				"keeps its last value", "call", callErr.action, "error", callErr.err.Error())
		}
		offered = versions
	}
	var available []string
	if offered != nil {
		available = newerOfferedMinors(in.Distribution, v.observed, offered)
	} else {
		available = keptNewerMinors(status.AvailableDistributionVersions, v.observed)
	}
	in.Available = available
	if slices.Equal(status.AvailableDistributionVersions, available) {
		return false
	}
	status.AvailableDistributionVersions = available
	return true
}

// decideMaster runs DecideMasterAction and fetches the inputs it asks for, each followed by a fresh decision: the
// offered versions through the VersionsCache, then the workers through one ListWorkers. It writes the answers into in,
// so that a decision after the request sees them. ok is false when the step ends without a decision:
//
//   - An input call failed; failed says how to go on, and a definitive answer has set MasterVersionUpToDate
//     (masterInputFailed).
//   - DecideMasterAction rejected its input: one of CAPIBM's own (spec.distributionVersion, status.masterUpdate.version)
//     does not parse, which the webhooks and this step's own writes prevent. It is logged and nothing is written; the
//     step waits for the next steady poll. A masterKubeVersion of IBM's that does not parse is no error: the decision
//     reports it as MasterVersionUpToDate=Unknown.
//
// Parameters:
//   - ctx is the reconcile context.
//   - in is the input; the fetched inputs are set on it.
func (s *ControlPlaneScope) decideMaster(ctx context.Context, in *MasterVersionInput) (MasterAction, masterVersionStep, bool) {
	for {
		action, err := DecideMasterAction(*in)
		if err != nil {
			ctrl.LoggerFrom(ctx).Info("CAPIBM cannot decide the master version, so it requests no master update", "problem", err.Error())
			return MasterAction{}, masterVersionStep{outcome: Done()}, false
		}
		switch {
		case action.NeedsOfferedVersions && in.Offered == nil:
			versions, failed, ok := s.masterOfferedVersions(ctx, *in)
			if !ok {
				return MasterAction{}, failed, false
			}
			in.Offered = versions
		case action.NeedsWorkers && !in.WorkersListed:
			workers, failed, ok := s.masterWorkers(ctx, *in)
			if !ok {
				return MasterAction{}, failed, false
			}
			in.Workers, in.WorkersListed = workers, true
		case action.NeedsOfferedVersions || action.NeedsWorkers:
			// DecideMasterAction asks for each input only while it is missing; asking again would loop.
			return MasterAction{}, masterVersionStep{outcome: Transient(fmt.Errorf("managed: DecideMasterAction asked for an "+
				"input that is already set (offered %t, workers listed %t; programming error)", in.Offered != nil,
				in.WorkersListed))}, false
		default:
			return action, masterVersionStep{}, true
		}
	}
}

// masterOfferedVersions returns IBM's offered versions through the VersionsCache of the identity and region, so that
// IBM is asked at most once per cache lifetime whatever the number of control planes. ok is false when the list
// failed; failed then says how to go on (masterInputFailed).
//
// Parameters:
//   - ctx is the reconcile context.
//   - in is the input of the decision that asked for the versions.
func (s *ControlPlaneScope) masterOfferedVersions(ctx context.Context, in MasterVersionInput) (*kubernetesservice.Versions, masterVersionStep, bool) {
	versions, callErr, err := s.listOfferedVersions(ctx)
	switch {
	case err != nil:
		return nil, masterVersionStep{outcome: Transient(err)}, false
	case callErr != nil:
		return nil, masterVersionStep{outcome: s.masterInputFailed(ctx, in, callErr)}, false
	default:
		return versions, masterVersionStep{}, true
	}
}

// listOfferedVersions lists IBM's offered versions through the VersionsCache of the identity and region. It returns
// the versions, or the failed listing (callErr), or err when the IBM clients cannot be built.
//
// TODO: this lists what IBM offers for new clusters (/versions), and the master version step takes the master's next
// minor in it as the only update. IBM is developing a per-cluster upgrade-path endpoint (GraphQL, currently
// https://containers.cloud.ibm.com/unsafe/graphql), because after OpenShift 4.22 a master can move to 4.23 or to 5.0, so
// the upgrades form a graph and the next version is no longer "the next minor in /versions". Once it is stable, list
// the versions this cluster can move to there, for the update and for status.availableDistributionVersions; it is also
// the way to see a master patch that IBM offers but has not applied (see the TODO on ibmversion.Minor.Next).
//
// Parameters:
//   - ctx is the reconcile context.
func (s *ControlPlaneScope) listOfferedVersions(ctx context.Context) (*kubernetesservice.Versions, *controlPlaneCallError, error) {
	clients, err := s.ibm(ctx)
	if err != nil {
		return nil, nil, err
	}
	region := s.InfraCluster.Spec.Region
	versions, err := s.params.VersionsCache.Get(ctx, clients.identity.Key, region, clients.kubernetes.ListVersions)
	if err != nil {
		return nil, &controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpListVersions,
			action: "listing the versions IBM Cloud offers in region " + region, err: err}, nil
	}
	return versions, nil, nil
}

// masterWorkers lists every worker of the cluster with one ListWorkers call, for the worker skew guard of
// DecideMasterAction row 9. ok is false when the list failed; failed then says how to go on (masterInputFailed).
//
// Parameters:
//   - ctx is the reconcile context.
//   - in is the input of the decision that asked for the workers; in.Cluster.ID is the IBM cluster ID.
func (s *ControlPlaneScope) masterWorkers(ctx context.Context, in MasterVersionInput) ([]kubernetesservice.Worker, masterVersionStep, bool) {
	clients, err := s.ibm(ctx)
	if err != nil {
		return nil, masterVersionStep{outcome: Transient(err)}, false
	}
	clusterID := in.Cluster.ID
	workers, err := clients.kubernetes.ListWorkers(ctx, clusterID)
	if err != nil {
		return nil, masterVersionStep{outcome: s.masterInputFailed(ctx, in, &controlPlaneCallError{identity: clients.identity,
			op: kubernetesservice.OpListWorkers, action: "listing the workers of IBM cluster " + clusterID, err: err})}, false
	}
	return workers, masterVersionStep{}, true
}

// masterInputFailed reports a failed call for an input of DecideMasterAction (ListVersions, ListWorkers) and returns how
// to go on. A definitive answer after a raise of spec.distributionVersion sets MasterVersionUpToDate from
// masterInputFailure first, so that the condition no longer says the master runs the minor the spec asks for;
// masterCallFailed then logs IBM's whole answer and gives the outcome, whose reason is that condition's. The
// condition's message is the same for the same failure, whatever IBM's incident ID, so a repeated failure writes no
// status. Every other answer leaves the condition as it is (masterInputFailure).
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - in is the input of the decision that asked for the call.
//   - callErr is the failed call.
func (s *ControlPlaneScope) masterInputFailed(ctx context.Context, in MasterVersionInput, callErr *controlPlaneCallError) Outcome {
	class := kubernetesservice.Classify(callErr.op, callErr.err)
	if condition, ok := masterInputFailure(in, class, callErr.action, callErr.err, callErr.identity.Description); ok {
		conditions.Set(s.ControlPlane, condition)
	}
	return s.masterCallFailed(ctx, callErr)
}

// requestMasterUpdate is DecideMasterAction row 12: it records the request (status.masterUpdate, MasterVersionUpToDate
// UpgradePending, RollingOut True) with a direct status write, then sends UpdateMaster, never with force, and decides
// again with IBM's answer on record.
//
// When the write fails, nothing is sent, and status.masterUpdate is put back in memory, so that the deferred patch does
// not record a request that was never made; the error brings the next attempt through controller-runtime's backoff.
// The conditions stay as decided: they say that CAPIBM requests the update, which the next attempt does. The patch
// helper writes the conditions and the rest of status in separate requests, so a failed write may have stored either
// part. A stored record without a call only delays the request by the grace and backoff rows; it never sends a second
// one, so the failure stays on the safe side.
//
// Parameters:
//   - ctx is the reconcile context.
//   - in is the decision's input, with the offered versions and the workers.
//   - action is the RequestUpdate decision.
func (s *ControlPlaneScope) requestMasterUpdate(ctx context.Context, in MasterVersionInput, action MasterAction) masterVersionStep {
	cp := s.ControlPlane
	clients, err := s.ibm(ctx)
	if err != nil {
		return masterVersionStep{outcome: Transient(err)}
	}
	s.applyMasterAction(action)
	previous := cp.Status.MasterUpdate
	cp.Status.MasterUpdate = action.MasterUpdate
	if err := s.persist(ctx); err != nil {
		cp.Status.MasterUpdate = previous
		return masterVersionStep{outcome: Transient(fmt.Errorf("recording the master update of IBM cluster %s to %s before the "+
			"request: %w", in.Cluster.ID, action.UpdateVersion, err))}
	}
	ctrl.LoggerFrom(ctx).Info("Requesting the master minor update", "version", action.UpdateVersion,
		"attempt", action.MasterUpdate.Attempts)
	callErr := clients.kubernetes.UpdateMaster(ctx, in.Cluster.ID, action.UpdateVersion)
	result, err := MasterUpdateCallOutcome(callErr, action.MasterUpdate, clients.identity.Description)
	if err != nil {
		return masterVersionStep{outcome: Transient(err)}
	}
	failure := &controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpUpdateMaster,
		action: fmt.Sprintf("requesting the master update of IBM cluster %s to %s", in.Cluster.ID, action.UpdateVersion), err: callErr}
	if callErr != nil && result.Handled {
		// The condition carries IBM's code only, because the incident ID changes with every answer; the incident ID and
		// IBM's description are for IBM support.
		logIBMRefusal(ctx, failure)
	}
	if result.Terminal {
		conditions.Set(cp, masterCondition(result.Reason, result.Message))
	}
	return s.afterMasterRequest(ctx, in, action, result, failure)
}

// afterMasterRequest decides again with the request and IBM's answer on record, so that this reconcile writes what the
// next one computes: the refusal row keeps a Terminal answer, row 10 an accepted or failed request. It returns the
// outcome of the answer: Terminal for a refusal (the controller logs it; the control plane's own poll and the retry time
// bring the next look), the master update poll for an accepted request, and masterCallFailed's handling for any other
// answer, whose retry follows the grace and backoff rows because status.masterUpdate records the attempt.
//
// Parameters:
//   - ctx is the reconcile context.
//   - in is the decision's input of the request.
//   - action is the RequestUpdate decision.
//   - result is MasterUpdateCallOutcome's answer.
//   - failure is the failed call, used when result is not Handled.
func (s *ControlPlaneScope) afterMasterRequest(ctx context.Context, in MasterVersionInput, action MasterAction,
	result MasterUpdateCallResult, failure *controlPlaneCallError) masterVersionStep {
	in.MasterUpdate = action.MasterUpdate
	in.CurrentUpToDate = masterConditionCopy(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
	after, err := DecideMasterAction(in)
	if err != nil {
		// The same input was decided a moment ago; only a programming error gets here.
		return masterVersionStep{outcome: Transient(fmt.Errorf("deciding the master version after the request: %w", err))}
	}
	s.applyMasterAction(after)
	step := masterVersionStep{recheckAt: after.RecheckAt}
	switch {
	case result.Terminal:
		step.outcome = Terminal(result.Reason, result.Message)
	case result.Accepted:
		step.outcome = NotDone(IntervalMasterUpdate, after.UpToDate.Reason)
	default:
		step.outcome = s.masterCallFailed(ctx, failure)
	}
	return step
}

// applyMasterAction writes a decision into the control plane: MasterVersionUpToDate and RollingOut (util/conditions
// keeps their transition times), and the removal of status.masterUpdate once the master reached its minor
// (DecideMasterAction row 2). The record of a new request is written by requestMasterUpdate.
//
// Parameters:
//   - action is DecideMasterAction's answer.
func (s *ControlPlaneScope) applyMasterAction(action MasterAction) {
	cp := s.ControlPlane
	if action.ClearMasterUpdate {
		cp.Status.MasterUpdate = controlplanev1.MasterUpdateRequest{}
	}
	if action.UpToDate.Type != "" {
		conditions.Set(cp, action.UpToDate)
	}
	if action.RollingOut.Type != "" {
		conditions.Set(cp, action.RollingOut)
	}
}

// masterWaitOutcome returns the outcome of a decision that requests nothing, for the controller's requeue (the named
// intervals of outcome.go): masterUpdatePoll while IBM shows an update or a request awaits it (row 1, row 10); Done for
// an up-to-date master; Terminal for what only a human fixes (a spec IBM cannot reach, rows 6-8, and a kept refusal);
// the CA rotation poll while a rotation blocks the update (row 4); the steady interval for the other holds (rows 5, 9
// and 11) and for a master version that does not parse (Unknown), which change on IBM's schedule. RecheckAt, which the
// controller applies as well, brings a retry time forward.
//
// Parameters:
//   - action is DecideMasterAction's answer; RequestUpdate is false.
func masterWaitOutcome(action MasterAction) Outcome {
	reason := action.UpToDate.Reason
	switch {
	case action.UpdatePending:
		return NotDone(IntervalMasterUpdate, action.RollingOut.Reason)
	case action.UpToDate.Status == metav1.ConditionTrue:
		return Done()
	}
	switch reason {
	case controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateDowngradeNotAllowedReason,
		controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
		controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason,
		controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason:
		return Terminal(reason, action.UpToDate.Message)
	case controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason:
		return NotDone(IntervalMasterUpdate, reason)
	case controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason:
		return NotDone(IntervalCARotation, reason)
	default:
		return NotDone(IntervalSteadyState, reason)
	}
}

// masterCallFailed returns how to go on after a failed IBM call of the master version step (ListVersions, ListWorkers,
// or an UpdateMaster answer MasterUpdateCallOutcome leaves to the caller), by the class that kubernetesservice.Classify
// gives it. It writes no condition itself: masterInputFailed sets MasterVersionUpToDate for a failed input call before
// it calls this, the grace and backoff rows report a failed UpdateMaster, and IBMClusterProvisioned, which reports the
// calls of the binding, was set True by the GetCluster of this reconcile, so flipping it here would move its
// transition time on every reconcile. The outcome's reason is MasterVersionUpToDate's. The classes:
//
//   - 401: only this identity's authenticator is rebuilt; the next attempt follows at the steady interval, about 5
//     minutes later.
//   - 429: the rate-limit backoff.
//   - 5xx, timeouts, connection errors and anything unknown: the error is returned, for controller-runtime's backoff.
//   - Every definitive answer (masterCallDefinitive: 403, 400, 404) is logged with IBM's whole answer, incident ID
//     included, and the step asks for terminalRecheck. The controller merges that with the flow's requeue
//     (resultWithMasterVersion), so the step in fact runs again at the next poll of the control plane, usually the
//     5 m steady poll, and no later than RecheckAt when the decision named one: a failed ListVersions or ListWorkers
//     is then made again. An UpdateMaster answer of this kind is not sent again then; the grace and backoff rows hold
//     it, because status.masterUpdate records the attempt.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - callErr is the failed call.
func (s *ControlPlaneScope) masterCallFailed(ctx context.Context, callErr *controlPlaneCallError) Outcome {
	reason := conditions.GetReason(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
	switch class := kubernetesservice.Classify(callErr.op, callErr.err); {
	case class == kubernetesservice.ClassUnauthorized:
		s.params.ClientBuilder.InvalidateAuthenticator(callErr.identity.Key)
		logIBMRefusal(ctx, callErr)
		return NotDone(IntervalSteadyState, reason)
	case class == kubernetesservice.ClassRateLimited:
		return RateLimited(kubernetesservice.RateLimitBackoff(callErr.err))
	case masterCallDefinitive(class):
		logIBMRefusal(ctx, callErr)
		return NotDone(IntervalTerminalRecheck, reason)
	default:
		return Transient(callErr)
	}
}

// masterConditionCopy returns a copy of cp's condition of conditionType, or nil when it has none. util/conditions Get
// returns a pointer into the condition list, which a later Set changes in place; a decision input must keep the
// condition as it was read.
//
// Parameters:
//   - cp is the control plane.
//   - conditionType is the condition's type.
func masterConditionCopy(cp *controlplanev1.IBMCloudManagedControlPlane, conditionType string) *metav1.Condition {
	if condition := conditions.Get(cp, conditionType); condition != nil {
		return condition.DeepCopy()
	}
	return nil
}
