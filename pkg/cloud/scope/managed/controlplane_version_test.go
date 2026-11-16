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
	"errors"
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"

	. "github.com/onsi/gomega"
)

// The master version step tests cover the parts of controlplane_version.go that the controller tests
// (internal/controllers/managed) do not reach directly: the requeue mapping of every decision, the handling of every
// class of a failed call, the recheck time and the initialization gate. The flows themselves are tested through the
// controller.

func TestMasterWaitOutcome(t *testing.T) {
	condition := func(status metav1.ConditionStatus, reason string) metav1.Condition {
		return metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition, Status: status,
			Reason: reason, Message: "message of " + reason}
	}
	testCases := []struct {
		name         string
		action       MasterAction
		wantKind     OutcomeKind
		wantInterval Interval
	}{
		{name: "IBM shows an update", wantKind: OutcomeNotDone, wantInterval: IntervalMasterUpdate,
			action: MasterAction{UpdatePending: true, UpToDate: condition(metav1.ConditionTrue, "UpToDate"),
				RollingOut: masterRollingOut(true, "IBM is applying master patch 1.36.6_1541")}},
		{name: "up to date", wantKind: OutcomeDone, action: MasterAction{UpToDate: condition(metav1.ConditionTrue, "UpToDate")}},
		{name: "a request waits for IBM", wantKind: OutcomeNotDone, wantInterval: IntervalMasterUpdate,
			action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "UpgradePending")}},
		{name: "failed update waits for the retry", wantKind: OutcomeNotDone, wantInterval: IntervalSteadyState,
			action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "UpgradeFailed")}},
		{name: "CA rotation", wantKind: OutcomeNotDone, wantInterval: IntervalCARotation,
			action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "BlockedByCARotation")}},
		{name: "cluster state", wantKind: OutcomeNotDone, wantInterval: IntervalSteadyState,
			action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "BlockedByClusterState")}},
		{name: "worker skew", wantKind: OutcomeNotDone, wantInterval: IntervalSteadyState,
			action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "BlockedByWorkerSkew")}},
		{name: "version skip", wantKind: OutcomeTerminal, action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "VersionSkipNotAllowed")}},
		{name: "downgrade", wantKind: OutcomeTerminal, action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "DowngradeNotAllowed")}},
		{name: "minor not offered", wantKind: OutcomeTerminal, action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "VersionNotAvailable")}},
		{name: "kept refusal", wantKind: OutcomeTerminal, action: MasterAction{UpToDate: condition(metav1.ConditionFalse, "PermissionDenied")}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := masterWaitOutcome(tc.action)
			g.Expect(got.Kind).To(Equal(tc.wantKind))
			g.Expect(got.Interval).To(Equal(tc.wantInterval))
			if tc.wantKind == OutcomeTerminal {
				g.Expect(got.Reason).To(Equal(tc.action.UpToDate.Reason))
				g.Expect(got.Message).To(Equal(tc.action.UpToDate.Message))
			}
		})
	}
}

func TestMasterCallFailed(t *testing.T) {
	identity := credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}
	apiError := func(status int) error {
		return &kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkers, StatusCode: status, Code: "E1234", IncidentID: "inc-7",
			RetryAfter: 2 * time.Minute}
	}
	testCases := []struct {
		name           string
		err            error
		wantKind       OutcomeKind
		wantInterval   Interval
		wantRetryAfter time.Duration
		wantInvalidate bool
	}{
		{name: "401 rebuilds the identity's authenticator", err: apiError(http.StatusUnauthorized), wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState, wantInvalidate: true},
		{name: "429 waits for Retry-After", err: apiError(http.StatusTooManyRequests), wantKind: OutcomeRateLimited,
			wantRetryAfter: 2 * time.Minute},
		{name: "5xx is returned for the backoff", err: apiError(http.StatusBadGateway), wantKind: OutcomeTransient},
		{name: "a network error is returned for the backoff", err: errors.New("connection reset"), wantKind: OutcomeTransient},
		{name: "403 is looked at again at terminalRecheck", err: apiError(http.StatusForbidden), wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck},
		{name: "400 is looked at again at terminalRecheck", err: apiError(http.StatusBadRequest), wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck},
		{name: "404 is looked at again at terminalRecheck", err: apiError(http.StatusNotFound), wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			builder := &cpScopeBuilder{}
			cp := &controlplanev1.IBMCloudManagedControlPlane{}
			conditions.Set(cp, metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
				Status: metav1.ConditionFalse, Reason: "UpgradePending", Message: "m"})
			before := cp.DeepCopy()
			scope := &ControlPlaneScope{params: newCPScopeParams(builder), ControlPlane: cp}

			got := scope.masterCallFailed(context.Background(), &controlPlaneCallError{identity: identity,
				op: kubernetesservice.OpListWorkers, action: "listing the workers of IBM cluster c1", err: tc.err})

			g.Expect(got.Kind).To(Equal(tc.wantKind))
			g.Expect(got.Interval).To(Equal(tc.wantInterval))
			if tc.wantRetryAfter > 0 {
				g.Expect(got.RetryAfter).To(BeNumerically(">=", tc.wantRetryAfter))
			}
			if tc.wantKind == OutcomeTransient {
				g.Expect(got.Err).To(MatchError(ContainSubstring("listing the workers of IBM cluster c1")))
			}
			if tc.wantInvalidate {
				g.Expect(builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
			} else {
				g.Expect(builder.invalidated).To(BeEmpty())
			}
			g.Expect(cp).To(Equal(before), "a failed call of the step writes no condition")
		})
	}
}

func TestControlPlaneScopeMasterVersionRequeueAfter(t *testing.T) {
	now := time.Date(2026, 11, 16, 3, 5, 30, 0, time.UTC)
	testCases := []struct {
		name      string
		recheckAt time.Time
		want      time.Duration
	}{
		{name: "no recheck time", want: 0},
		{name: "a future recheck time", recheckAt: now.Add(10 * time.Minute), want: 10 * time.Minute},
		{name: "a recheck time already reached waits one second", recheckAt: now.Add(-time.Second), want: time.Second},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			params := newCPScopeParams(&cpScopeBuilder{})
			params.Clock = clocktesting.NewFakePassiveClock(now)
			scope := &ControlPlaneScope{params: params, masterStep: masterVersionStep{outcome: Done(), recheckAt: tc.recheckAt}}
			g.Expect(scope.MasterVersionRequeueAfter()).To(Equal(tc.want))
			g.Expect(scope.MasterVersionOutcome()).To(Equal(Done()))
		})
	}
}

// TestReconcileMasterVersionWaitsForInitialization checks that the master version step runs only after
// initialization: an uninitialized control plane gets no condition, no IBM call (gomock fails the test on one) and no
// outcome.
func TestReconcileMasterVersionWaitsForInitialization(t *testing.T) {
	for _, initialized := range []*bool{nil, ptr.To(false)} {
		g := NewWithT(t)
		kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
		cp := &controlplanev1.IBMCloudManagedControlPlane{Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.37"}}
		cp.Status.Initialization.ControlPlaneInitialized = initialized
		scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{kubernetes: kube}), &clusterv1.Cluster{}, cp, cpNoPersist)
		g.Expect(err).NotTo(HaveOccurred())
		scope.InfraCluster = &infrav1.IBMCloudManagedCluster{Spec: infrav1.IBMCloudManagedClusterSpec{Region: "us-south"}}

		scope.reconcileMasterVersion(context.Background(), &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "1.36.5_1537"}, Done())

		g.Expect(scope.MasterVersionOutcome()).To(Equal(Outcome{}))
		g.Expect(scope.MasterVersionRequeueAfter()).To(BeZero())
		g.Expect(cp.Status.Conditions).To(BeEmpty())
	}
}

// TestReconcileMasterVersionAfterTheAccessSteps checks that IBM's Retry-After of a call of the access steps holds the
// calls of the master version step as well: after a RateLimited flow the step makes no IBM call (gomock fails the test
// on one), writes no condition and reports no outcome, so the controller keeps the flow's requeue. Every other outcome
// of the access steps lets the step run; its first call, ListVersions for a raised spec, is answered with a 403 here
// so that the step ends at once.
func TestReconcileMasterVersionAfterTheAccessSteps(t *testing.T) {
	testCases := []struct {
		name    string
		flow    Outcome
		wantRun bool
	}{
		{name: "a rate limit of the access steps holds every call until Retry-After", flow: RateLimited(10 * time.Minute)},
		{name: "a steady flow runs the step", flow: Done(), wantRun: true},
		{name: "a waiting flow runs the step", flow: NotDone(IntervalTerminalRecheck, "CredentialFetchFailed"), wantRun: true},
		{name: "a transient failure of the flow runs the step", flow: Transient(errors.New("boom")), wantRun: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
			if tc.wantRun {
				kube.EXPECT().ListVersions(gomock.Any()).Return(nil, &kubernetesservice.APIError{Operation: kubernetesservice.OpListVersions,
					StatusCode: http.StatusForbidden})
			}
			cp := &controlplanev1.IBMCloudManagedControlPlane{Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
				Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.37"}}
			cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
			scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{kubernetes: kube}), &clusterv1.Cluster{}, cp, cpNoPersist)
			g.Expect(err).NotTo(HaveOccurred())
			scope.InfraCluster = &infrav1.IBMCloudManagedCluster{Spec: infrav1.IBMCloudManagedClusterSpec{Region: "us-south"}}

			scope.reconcileMasterVersion(context.Background(), &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "1.36.5_1537"}, tc.flow)

			if !tc.wantRun {
				g.Expect(scope.MasterVersionOutcome()).To(Equal(Outcome{}))
				g.Expect(scope.MasterVersionRequeueAfter()).To(BeZero())
				g.Expect(cp.Status.MasterUpdate).To(BeZero())
				g.Expect(cp.Status.Conditions).To(BeEmpty())
				return
			}
			g.Expect(scope.MasterVersionOutcome()).To(Equal(NotDone(IntervalTerminalRecheck,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason)),
				"the 403 of ListVersions ended the step (masterInputFailed)")
			g.Expect(conditions.GetReason(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)).To(
				Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason))
		})
	}
}

// TestReconcileMasterVersionUnparsableMaster checks in the scope that a masterKubeVersion CAPIBM cannot parse makes no
// IBM call (gomock fails the test on one), replaces an earlier True with
// MasterVersionUpToDate=Unknown/UnparsableMasterVersion, leaves RollingOut as it is, and waits for the steady poll
// instead of an error backoff. Once IBM reports a version that parses, the next run makes the normal decision: the
// Unknown is no record that holds anything.
func TestReconcileMasterVersionUnparsableMaster(t *testing.T) {
	g := NewWithT(t)
	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	cp := &controlplanev1.IBMCloudManagedControlPlane{Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
		Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.37"}}
	cp.Generation = 4
	cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	conditions.Set(cp, metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
		Status: metav1.ConditionTrue, Reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
		Message: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches itself"})
	conditions.Set(cp, metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition, Status: metav1.ConditionTrue,
		Reason:  controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason,
		Message: "IBM is updating the master to 1.37.1_1517"})
	rollingBefore := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition).DeepCopy()
	scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{kubernetes: kube}), &clusterv1.Cluster{}, cp, cpNoPersist)
	g.Expect(err).NotTo(HaveOccurred())

	scope.reconcileMasterVersion(context.Background(), &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "not-a-version"}, Done())

	reason := controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUnparsableMasterVersionReason
	g.Expect(scope.MasterVersionOutcome()).To(Equal(NotDone(IntervalSteadyState, reason)))
	g.Expect(scope.MasterVersionRequeueAfter()).To(BeZero())
	upToDate := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
	g.Expect(upToDate.Status).To(Equal(metav1.ConditionUnknown))
	g.Expect(upToDate.Reason).To(Equal(reason))
	g.Expect(upToDate.Message).To(Equal(`IBM reports master version "not-a-version" for IBM cluster c1, which CAPIBM cannot parse, ` +
		"so it cannot tell whether the master runs spec.distributionVersion 1.37; CAPIBM requests no master update until IBM " +
		"reports a version it can parse"))
	g.Expect(upToDate.ObservedGeneration).To(Equal(int64(4)))
	g.Expect(conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition)).To(Equal(rollingBefore),
		"RollingOut is left as it is")
	g.Expect(cp.Status.MasterUpdate).To(BeZero())

	// IBM reports a version that parses again, the minor spec.distributionVersion asks for: row 3 replaces the Unknown
	// with True, RollingOut is reported again, and still no IBM call is needed.
	scope.reconcileMasterVersion(context.Background(), &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "1.37.1_1517",
		TargetVersion: "1.37.1_1517"}, Done())

	g.Expect(scope.MasterVersionOutcome()).To(Equal(Done()))
	upToDate = conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
	g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason))
	g.Expect(upToDate.Message).To(HavePrefix("the master runs 1.37.1_1517, minor 1.37 as spec.distributionVersion asks"))
	rolling := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition)
	g.Expect(rolling.Status).To(Equal(metav1.ConditionFalse))
	g.Expect(rolling.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneNotRollingOutReason))
	g.Expect(cp.Status.MasterUpdate).To(BeZero())
}

// TestReconcileMasterVersionKeepsAVersionSkipRefusal checks the refusal row in the scope: IBM's version-skip refusal
// of the request on record, which the refusal row keeps until the spec changes, survives a failed ListVersions (the
// listing comes before that row), and once the listing works again it is still kept, after the retry time of row 11
// too, so no further UpdateMaster is sent (gomock fails the test on one).
func TestReconcileMasterVersionKeepsAVersionSkipRefusal(t *testing.T) {
	g := NewWithT(t)
	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	const refused = "IBM refused the master update to 1.37 as a version skip; set spec.distributionVersion to the master's " +
		"next minor; CAPIBM requests no update until the spec changes"
	cp := &controlplanev1.IBMCloudManagedControlPlane{Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
		Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.37"}}
	cp.Generation = 4
	cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	params := newCPScopeParams(&cpScopeBuilder{kubernetes: kube})
	clk := params.Clock.(*clocktesting.FakePassiveClock)
	cp.Status.MasterUpdate = controlplanev1.MasterUpdateRequest{Version: "1.37", LastRequestTime: metav1.NewTime(clk.Now().Add(-10 * time.Minute)),
		Attempts: 1}
	conditions.Set(cp, metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
		Status: metav1.ConditionFalse, Reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
		Message: refused})
	kept := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition).DeepCopy()
	run := func() Outcome {
		scope, err := NewControlPlaneScope(params, &clusterv1.Cluster{}, cp, cpNoPersist)
		g.Expect(err).NotTo(HaveOccurred())
		scope.InfraCluster = &infrav1.IBMCloudManagedCluster{Spec: infrav1.IBMCloudManagedClusterSpec{Region: "us-south"}}
		scope.reconcileMasterVersion(context.Background(), &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "1.36.5_1537",
			TargetVersion: "1.36.5_1537"}, Done())
		g.Expect(conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)).To(Equal(kept),
			"the refusal stays as IBM's answer wrote it")
		g.Expect(cp.Status.MasterUpdate.Attempts).To(Equal(int32(1)), "no UpdateMaster")
		return scope.MasterVersionOutcome()
	}

	kube.EXPECT().ListVersions(gomock.Any()).Return(nil, &kubernetesservice.APIError{Operation: kubernetesservice.OpListVersions,
		StatusCode: http.StatusForbidden, Code: "E3c1a"})
	g.Expect(run()).To(Equal(NotDone(IntervalTerminalRecheck, kept.Reason)), "the failed listing is looked at again at the next poll")

	// The listing works again two hours later, past the retry time of row 11 (1 hour after one attempt).
	clk.SetTime(clk.Now().Add(2 * time.Hour))
	kube.EXPECT().ListVersions(gomock.Any()).Return(&kubernetesservice.Versions{
		Kubernetes: []kubernetesservice.ReleaseVersion{{Major: 1, Minor: 36, Patch: 5}, {Major: 1, Minor: 37, Patch: 1}}}, nil)
	g.Expect(run()).To(Equal(Terminal(kept.Reason, refused)), "the refusal row keeps it until the spec changes")
}

// TestReconcileMasterVersionRejectedInput checks that an input of CAPIBM's own that does not parse (here a
// status.masterUpdate.version written by hand) makes no IBM call, writes no condition and waits for the steady poll
// instead of an error backoff.
func TestReconcileMasterVersionRejectedInput(t *testing.T) {
	g := NewWithT(t)
	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	cp := &controlplanev1.IBMCloudManagedControlPlane{Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
		Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.37"}}
	cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	cp.Status.MasterUpdate = controlplanev1.MasterUpdateRequest{Version: "v1.37", LastRequestTime: metav1.Now(), Attempts: 1}
	scope, err := NewControlPlaneScope(newCPScopeParams(&cpScopeBuilder{kubernetes: kube}), &clusterv1.Cluster{}, cp, cpNoPersist)
	g.Expect(err).NotTo(HaveOccurred())

	scope.reconcileMasterVersion(context.Background(), &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "1.36.5_1537"}, Done())

	g.Expect(scope.MasterVersionOutcome()).To(Equal(Done()))
	g.Expect(cp.Status.Conditions).To(BeEmpty())
}

// TestMasterInputFailed checks in the scope that a definitive failure of an input call after a raise of
// spec.distributionVersion replaces the stale True of the old minor with False (PermissionDenied or IBMAPIError) and
// requeues at terminalRecheck with that reason; every other answer leaves the condition as it is and keeps its own
// handling (masterCallFailed). The same failure with a new incident ID changes nothing.
func TestMasterInputFailed(t *testing.T) {
	identity := credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}
	answer := func(status int, incident string) error {
		return &kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkers, StatusCode: status, Code: "E1234",
			IncidentID: incident, RetryAfter: 2 * time.Minute}
	}
	const stale = "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches itself"
	const call = "listing the workers of IBM cluster c1"
	testCases := []struct {
		name    string
		desired string
		// err is IBM's answer; incident makes a second answer of the same kind with another incident ID.
		err      func(incident string) error
		wantKind OutcomeKind
		// wantInterval is the requeue of a NotDone outcome.
		wantInterval Interval
		// wantReason and wantMessage are the condition's; wantReason "" keeps the stale True.
		wantReason     string
		wantMessage    string
		wantInvalidate bool
	}{
		{name: "403 replaces the stale True with PermissionDenied", desired: "1.37",
			err: func(incident string) error { return answer(http.StatusForbidden, incident) }, wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck, wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + call + " with HTTP 403 (IBM code E1234), so the master update to " +
				"1.37 waits: the credentials of the CAPIBM controller identity need the Administrator platform role and the " +
				"Manager service role of Kubernetes Service on the cluster's resource group; CAPIBM tries again at the next poll " +
				"of the control plane"},
		{name: "404 replaces the stale True with IBMAPIError", desired: "1.37",
			err: func(incident string) error { return answer(http.StatusNotFound, incident) }, wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck, wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + call + " with HTTP 404 (IBM code E1234), so the master update to " +
				"1.37 waits; CAPIBM tries again at the next poll of the control plane, and the controller log has IBM's description"},
		{name: "400 replaces the stale True with IBMAPIError", desired: "1.37",
			err: func(incident string) error { return answer(http.StatusBadRequest, incident) }, wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck, wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + call + " with HTTP 400 (IBM code E1234), so the master update to " +
				"1.37 waits; CAPIBM tries again at the next poll of the control plane, and the controller log has IBM's description"},
		{name: "a 401 keeps the condition and rebuilds the authenticator", desired: "1.37", wantInvalidate: true,
			err: func(incident string) error { return answer(http.StatusUnauthorized, incident) }, wantKind: OutcomeNotDone,
			wantInterval: IntervalSteadyState},
		{name: "a 429 keeps the condition", desired: "1.37", wantKind: OutcomeRateLimited,
			err: func(incident string) error { return answer(http.StatusTooManyRequests, incident) }},
		{name: "a 5xx keeps the condition", desired: "1.37", wantKind: OutcomeTransient,
			err: func(incident string) error { return answer(http.StatusBadGateway, incident) }},
		{name: "a network error keeps the condition", desired: "1.37", wantKind: OutcomeTransient,
			err: func(string) error { return errors.New("connection reset") }},
		{name: "a spec on the master's minor keeps its condition", desired: "1.36", wantKind: OutcomeNotDone,
			wantInterval: IntervalTerminalRecheck, err: func(incident string) error { return answer(http.StatusForbidden, incident) }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := &controlplanev1.IBMCloudManagedControlPlane{}
			cp.Generation = 2
			conditions.Set(cp, metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
				Status: metav1.ConditionTrue, Reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason, Message: stale})
			cp.Generation = 3 // spec.distributionVersion was raised after the True was written
			builder := &cpScopeBuilder{}
			scope := &ControlPlaneScope{params: newCPScopeParams(builder), ControlPlane: cp}
			in := MasterVersionInput{Distribution: controlplanev1.DistributionKubernetes, DesiredVersion: tc.desired, Generation: 3,
				Cluster: &kubernetesservice.Cluster{ID: "c1", MasterKubeVersion: "1.36.5_1537"}, Now: time.Now()}
			failed := func(incident string) Outcome {
				return scope.masterInputFailed(context.Background(), in, &controlPlaneCallError{identity: identity,
					op: kubernetesservice.OpListWorkers, action: call, err: tc.err(incident)})
			}

			got := failed("incident-a")

			g.Expect(got.Kind).To(Equal(tc.wantKind))
			g.Expect(got.Interval).To(Equal(tc.wantInterval))
			if tc.wantInvalidate {
				g.Expect(builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
			} else {
				g.Expect(builder.invalidated).To(BeEmpty())
			}
			upToDate := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
			if tc.wantReason == "" {
				g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(upToDate.Message).To(Equal(stale), "the condition is left as it is")
				g.Expect(upToDate.ObservedGeneration).To(Equal(int64(2)))
				if got.Kind == OutcomeNotDone {
					g.Expect(got.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason))
				}
				return
			}
			g.Expect(upToDate.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(upToDate.Reason).To(Equal(tc.wantReason))
			g.Expect(upToDate.Message).To(Equal(tc.wantMessage))
			g.Expect(upToDate.ObservedGeneration).To(Equal(int64(3)))
			g.Expect(got.Reason).To(Equal(tc.wantReason), "the outcome carries the condition's reason")

			// The same failure with a new incident ID writes the same condition, so a repeat writes no status.
			written := cp.DeepCopy()
			g.Expect(failed("incident-b")).To(Equal(got))
			g.Expect(cp).To(Equal(written))
		})
	}
}

func TestMasterConditionCopy(t *testing.T) {
	g := NewWithT(t)
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	g.Expect(masterConditionCopy(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)).To(BeNil())
	conditions.Set(cp, masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason, "refused"))
	copied := masterConditionCopy(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
	conditions.Set(cp, masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason, "requested"))
	g.Expect(copied.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason),
		"a later Set does not change the copy a decision read")
	g.Expect(copied.Message).To(Equal("refused"))
}
