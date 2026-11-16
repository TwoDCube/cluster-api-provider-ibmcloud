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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// The master version tests run Reconcile of a bound, initialized control plane through the master minor update
// (ControlPlaneScope.reconcileMasterVersion, DecideMasterAction). The kubeconfig Secret is user-provided and passes the
// probe, and CAPIBM never refreshes a user-provided Secret, so the kubeconfig step makes no IBM call at any clock time:
// every IBM call a test sees is the GetCluster of the binding or a call of the master version step, and gomock fails a
// test on every call it does not expect. The one exception is the test of a rate-limited kubeconfig refresh, which
// needs a CAPIBM-owned Secret. The fake clock moves through the grace and the backoff.

// masterTestGeneration is the metadata.generation of the control plane in the master version tests.
const masterTestGeneration = 3

// masterTestRequested is the time of the first request in the master version tests, as the API keeps it.
var masterTestRequested = cpTestStart.UTC().Format(time.RFC3339)

// newMasterFixture returns the fixture of a bound, initialized control plane whose spec.distributionVersion is desired,
// with a user-provided kubeconfig Secret that passes the probe. setup changes the objects first (withOpenShift makes it
// a ROKS one); t is the test.
func newMasterFixture(t *testing.T, desired string, setup func(objs *cpObjects)) *cpFixture {
	t.Helper()
	creds := newKubeconfigCredentials(t, time.Hour)
	userProvided := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: cpTestName + "-kubeconfig",
			Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"value": buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)},
	}
	f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
		objs.cp.Generation = masterTestGeneration
		objs.cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
		objs.extra = append(objs.extra, userProvided)
		if setup != nil {
			setup(objs)
		}
		objs.cp.Spec.DistributionVersion = desired
	})
	prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
	return f
}

// masterTestVersions are the versions IBM offers in the master version tests: the next minors 1.37 and 4.22 are
// offered.
func masterTestVersions() *kubernetesservice.Versions {
	return &kubernetesservice.Versions{
		Kubernetes: []kubernetesservice.ReleaseVersion{{Major: 1, Minor: 35, Patch: 9}, {Major: 1, Minor: 36, Patch: 5, Default: true},
			{Major: 1, Minor: 37, Patch: 1}},
		OpenShift: []kubernetesservice.ReleaseVersion{{Major: 4, Minor: 21, Patch: 35}, {Major: 4, Minor: 22, Patch: 3}},
	}
}

// masterTestWorkers returns one deployed worker per version, with IDs w0, w1, ...
func masterTestWorkers(versions ...string) []kubernetesservice.Worker {
	workers := make([]kubernetesservice.Worker, 0, len(versions))
	for i, version := range versions {
		workers = append(workers, kubernetesservice.Worker{ID: "w" + string(rune('0'+i)), ActualState: managedscope.WorkerStateDeployed,
			HealthState: "normal", KubeVersionActual: version, KubeVersionTarget: version})
	}
	return workers
}

// masterRecorded returns a status.masterUpdate for version, requested at cpTestStart + at, attempts times.
func masterRecorded(version string, at time.Duration, attempts int32) controlplanev1.MasterUpdateRequest {
	return controlplanev1.MasterUpdateRequest{Version: version, LastRequestTime: metav1.NewTime(cpTestStart.Add(at)), Attempts: attempts}
}

// expectMasterUpdate checks status.masterUpdate against want. The times are compared as instants: the fake client
// returns them in the local time zone.
func expectMasterUpdate(g Gomega, got, want controlplanev1.MasterUpdateRequest) {
	g.ExpectWithOffset(1, got.Version).To(Equal(want.Version))
	g.ExpectWithOffset(1, got.Attempts).To(Equal(want.Attempts))
	g.ExpectWithOffset(1, got.LastRequestTime.Equal(&want.LastRequestTime)).To(BeTrue(), "lastRequestTime %s, want %s",
		got.LastRequestTime.UTC(), want.LastRequestTime.UTC())
}

// expectVersions expects one ListVersions, answered with masterTestVersions.
func (f *cpFixture) expectVersions() *gomock.Call {
	return f.kube.EXPECT().ListVersions(gomock.Any()).Return(masterTestVersions(), nil)
}

// expectWorkers expects one ListWorkers of the bound cluster, answered with workers.
func (f *cpFixture) expectWorkers(workers []kubernetesservice.Worker) *gomock.Call {
	return f.kube.EXPECT().ListWorkers(gomock.Any(), cpIKSClusterID).Return(workers, nil)
}

// expectUpdateMaster expects one UpdateMaster of the bound cluster to version, answered with answer. Before it answers,
// it checks that the request is already on record in the API server, as DecideMasterAction row 12 requires:
// status.masterUpdate for version, MasterVersionUpToDate=False/UpgradePending and RollingOut=True. The interface has
// no force parameter, so CAPIBM can never send force.
func (f *cpFixture) expectUpdateMaster(version string, answer error) *gomock.Call {
	return f.kube.EXPECT().UpdateMaster(gomock.Any(), cpIKSClusterID, version).DoAndReturn(
		func(context.Context, string, string) error {
			persisted := f.controlPlane()
			if persisted.Status.MasterUpdate.Version != version || !persisted.Status.MasterUpdate.LastRequestTime.Time.Equal(f.clock.Now()) {
				f.t.Errorf("UpdateMaster was called before status.masterUpdate was persisted: %+v", persisted.Status.MasterUpdate)
			}
			upToDate := conditions.Get(persisted, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
			if upToDate == nil || upToDate.Reason != controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason {
				f.t.Errorf("UpdateMaster was called before MasterVersionUpToDate=UpgradePending was persisted: %+v", upToDate)
			}
			if !conditions.IsTrue(persisted, controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition) {
				f.t.Errorf("UpdateMaster was called before RollingOut=True was persisted")
			}
			return answer
		})
}

// expectRequest expects the calls of one request: GetCluster answering c, ListWorkers answering workers, and
// UpdateMaster to version answering answer, in this order. The offered versions come from the cache or from an
// expectation the test sets.
func (f *cpFixture) expectRequest(c *kubernetesservice.Cluster, workers []kubernetesservice.Worker, version string, answer error) {
	gomock.InOrder(f.expectObserve(c), f.expectWorkers(workers), f.expectUpdateMaster(version, answer))
}

// masterConditions returns the MasterVersionUpToDate and RollingOut conditions of the persisted control plane.
func (f *cpFixture) masterConditions() (*metav1.Condition, *metav1.Condition) {
	cp := f.controlPlane()
	return conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition),
		conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition)
}

// expectRequeueWithin checks that result requeues after a positive time no later than limit.
func expectRequeueWithin(g Gomega, result ctrl.Result, limit time.Duration) {
	g.ExpectWithOffset(1, result.RequeueAfter).To(BeNumerically(">", 0))
	g.ExpectWithOffset(1, result.RequeueAfter).To(BeNumerically("<=", limit), "requeue after %s", result.RequeueAfter)
}

// capturedLog keeps every line a logger built by logger writes, for the tests that check what goes to the controller
// log rather than into status (IBM's incident ID, which changes with every answer). It is safe for concurrent use.
type capturedLog struct {
	mu    sync.Mutex
	lines []string
}

// logger returns a logger that formats each entry with funcr (`"msg"="..." "key"="value"` pairs) into the capture, at
// every verbosity.
func (c *capturedLog) logger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lines = append(c.lines, strings.TrimSpace(prefix+" "+args))
	}, funcr.Options{Verbosity: 10})
}

// matching returns the captured lines that contain every one of parts. parts are substrings of one line.
func (c *capturedLog) matching(parts ...string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var matched []string
	for _, line := range c.lines {
		all := true
		for _, part := range parts {
			all = all && strings.Contains(line, part)
		}
		if all {
			matched = append(matched, line)
		}
	}
	return matched
}

// reconcileLogged runs one Reconcile of the test control plane, like reconcile, with log as the logger of its context.
func (f *cpFixture) reconcileLogged(log logr.Logger) (ctrl.Result, error) {
	return f.r.Reconcile(ctrl.LoggerInto(ctx, log), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: cpTestNamespace, Name: cpTestName}})
}

func TestControlPlaneMasterUpdateRequest(t *testing.T) {
	t.Run("status masterUpdate is written before UpdateMaster without force and IBM accepts", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.expectVersions()
		f.expectRequest(cpCluster("normal"), masterTestWorkers("1.36.5_1540", "1.35.9_1530"), "1.37", nil)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		// masterUpdatePoll (2 m): before the end of the grace (5 m), IBM may show the update.
		expectRequeue(g, result, 2*time.Minute)
		cp := f.controlPlane()
		expectMasterUpdate(g, cp.Status.MasterUpdate, masterRecorded("1.37", 0, 1))
		upToDate, rolling := f.masterConditions()
		// The conditions are those the next reconcile computes (row 10), so its patch finds nothing to change.
		g.Expect(upToDate.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
		g.Expect(upToDate.Message).To(Equal("CAPIBM requested the master update to 1.37 at " + masterTestRequested +
			"; waiting for IBM to show it"))
		g.Expect(upToDate.ObservedGeneration).To(Equal(int64(masterTestGeneration)))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(rolling.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason))
		g.Expect(rolling.Message).To(Equal("CAPIBM requested the master update to 1.37"))
		patches := f.patches()
		// The patch helper writes the conditions and the rest of status in separate requests: both precede the call.
		g.Expect(patches[0]).To(ContainSubstring("CAPIBM requests the master update from 1.36.5_1537 to 1.37"))
		g.Expect(patches[1]).To(ContainSubstring(`"masterUpdate"`))

		// The reconcile that the controller's own status patch starts writes nothing and calls nothing but GetCluster.
		f.expectObserve(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.patches()).To(HaveLen(len(patches)))
	})

	t.Run("ROKS requests the openshift minor", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "4.22", withOpenShift)
		f.expectVersions()
		f.expectRequest(roksCluster(), masterTestWorkers("4.21.40_1540_openshift"), "4.22_openshift", nil)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectMasterUpdate(g, f.controlPlane().Status.MasterUpdate, masterRecorded("4.22_openshift", 0, 1))
	})

	t.Run("409 already requested is UpgradePending", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.expectVersions()
		f.expectRequest(cpCluster("normal"), nil, "1.37", ibmError(kubernetesservice.OpUpdateMaster, http.StatusConflict, ""))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred(), "a 409 is never an error")
		expectRequeue(g, result, 2*time.Minute)
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionTrue))
	})

	t.Run("intent patch failure sends no UpdateMaster", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.failStatusPatch = func(body string) bool { return strings.Contains(body, `"masterUpdate"`) }
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.expectVersions(), f.expectWorkers(nil)) // no UpdateMaster

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("recording the master update of IBM cluster iksclusterid00000001 to 1.37 before the request")))
		g.Expect(f.controlPlane().Status.MasterUpdate).To(BeZero(), "the deferred patch records no request that was never sent")

		// The next attempt, once the write succeeds, is the first request.
		f.failStatusPatch = nil
		_, err = f.reconcileAt(time.Second, func() { f.expectRequest(cpCluster("normal"), nil, "1.37", nil) })
		g.Expect(err).NotTo(HaveOccurred())
		expectMasterUpdate(g, f.controlPlane().Status.MasterUpdate, masterRecorded("1.37", time.Second, 1))
	})

	t.Run("a failed call is retried after the grace only", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.expectVersions()
		f.expectRequest(cpCluster("normal"), nil, "1.37", ibmError(kubernetesservice.OpUpdateMaster, http.StatusBadGateway, ""))

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("requesting the master update of IBM cluster iksclusterid00000001 to 1.37")))
		expectMasterUpdate(g, f.controlPlane().Status.MasterUpdate, masterRecorded("1.37", 0, 1)) // the attempt is on record
		// controller-runtime's backoff brings the reconcile back at once: the grace holds the request.
		_, err = f.reconcileAt(time.Second, func() { f.expectObserve(cpCluster("normal")) })
		g.Expect(err).NotTo(HaveOccurred())
		upToDate, _ := f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
	})

	t.Run("a 401 rebuilds only this identity's authenticator", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.expectVersions()
		f.expectRequest(cpCluster("normal"), nil, "1.37", ibmError(kubernetesservice.OpUpdateMaster, http.StatusUnauthorized, ""))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeueWithin(g, result, 5*time.Minute*11/10)
		g.Expect(f.builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
	})

	t.Run("a 429 waits for IBM's Retry-After", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.expectVersions()
		f.expectRequest(cpCluster("normal"), nil, "1.37", &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster,
			StatusCode: http.StatusTooManyRequests, RetryAfter: 10 * time.Minute})

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Minute), "never before Retry-After, not even for the grace end")
	})
}

func TestControlPlaneMasterUpdateBackoff(t *testing.T) {
	t.Run("no second UpdateMaster within the grace and retries back off 1h then 2h", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.kube.EXPECT().ListVersions(gomock.Any()).Return(masterTestVersions(), nil).AnyTimes()
		normal := cpCluster("normal")
		observeOnly := func() { f.expectObserve(normal) }
		request := func(version string) func() {
			return func() { f.expectRequest(normal, nil, version, nil) }
		}

		for _, step := range []struct {
			after        time.Duration
			expect       func()
			wantReason   string
			wantAttempts int32
			// wantRequeueBy bounds the requeue: never later than the decision's recheck time.
			wantRequeueBy time.Duration
		}{
			{after: 0, expect: request("1.37"), wantReason: "UpgradePending", wantAttempts: 1, wantRequeueBy: 2 * time.Minute * 11 / 10},
			{after: time.Minute, expect: observeOnly, wantReason: "UpgradePending", wantAttempts: 1, wantRequeueBy: 2 * time.Minute * 11 / 10},
			{after: 4 * time.Minute, expect: observeOnly, wantReason: "UpgradePending", wantAttempts: 1, wantRequeueBy: time.Minute * 11 / 10},
			{after: 5 * time.Minute, expect: observeOnly, wantReason: "UpgradeFailed", wantAttempts: 1, wantRequeueBy: 5 * time.Minute * 11 / 10},
			{after: 59 * time.Minute, expect: observeOnly, wantReason: "UpgradeFailed", wantAttempts: 1, wantRequeueBy: time.Minute * 11 / 10},
			{after: time.Hour, expect: request("1.37"), wantReason: "UpgradePending", wantAttempts: 2, wantRequeueBy: 2 * time.Minute * 11 / 10},
			{after: time.Hour + 6*time.Minute, expect: observeOnly, wantReason: "UpgradeFailed", wantAttempts: 2, wantRequeueBy: 5 * time.Minute * 11 / 10},
			{after: 3*time.Hour - time.Second, expect: observeOnly, wantReason: "UpgradeFailed", wantAttempts: 2, wantRequeueBy: 2 * time.Second},
			{after: 3 * time.Hour, expect: request("1.37"), wantReason: "UpgradePending", wantAttempts: 3, wantRequeueBy: 2 * time.Minute * 11 / 10},
		} {
			result, err := f.reconcileAt(step.after, step.expect)
			g.Expect(err).NotTo(HaveOccurred(), "after %s", step.after)
			expectRequeueWithin(g, result, step.wantRequeueBy)
			upToDate, _ := f.masterConditions()
			g.Expect(upToDate.Reason).To(Equal(step.wantReason), "after %s: %s", step.after, upToDate.Message)
			g.Expect(f.controlPlane().Status.MasterUpdate.Attempts).To(Equal(step.wantAttempts), "after %s", step.after)
		}
		upToDate, _ := f.masterConditions()
		g.Expect(upToDate.Message).To(Equal("CAPIBM requested the master update to 1.37 at 2026-11-16T06:05:30Z; waiting for IBM to show it"))
	})

	t.Run("failed update back on the old minor names the retry time", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.MasterUpdate = masterRecorded("1.37", -10*time.Minute, 1)
		})
		f.expectVersions()
		f.expectObserve(cpCluster("normal"))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason))
		g.Expect(upToDate.Message).To(Equal("the master update to 1.37 that CAPIBM requested at 2026-11-16T02:55:30Z has not taken " +
			"effect: the master still runs 1.36.5_1537 and IBM shows no update in progress; CAPIBM requests it again at " +
			"2026-11-16T03:55:30Z"))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionFalse))
	})
}

func TestControlPlaneMasterUpdateRefusal(t *testing.T) {
	const deniedMessage = "IBM Cloud Kubernetes Service refused the master update to 1.37 with HTTP 403 (IBM code E3c1a): the " +
		"credentials of the CAPIBM controller identity need the Administrator platform role and the Manager service role of " +
		"Kubernetes Service on the cluster's resource group. CAPIBM requests one minor at a time, so this is not IBM's " +
		"refusal to skip versions; CAPIBM requests the update again at 2026-11-16T04:05:30Z"
	forbidden := func(incident string) error {
		return ibmErrorWithIncident(kubernetesservice.OpUpdateMaster, http.StatusForbidden, "E3c1a", incident)
	}

	t.Run("403 without a skip code is PermissionDenied held until the retry time then one request", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.kube.EXPECT().ListVersions(gomock.Any()).Return(masterTestVersions(), nil).AnyTimes()
		normal := cpCluster("normal")
		f.expectRequest(normal, nil, "1.37", forbidden("incident-a"))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason),
			"a 403 is PermissionDenied, not VersionSkipNotAllowed")
		g.Expect(upToDate.Message).To(Equal(deniedMessage))
		expectNoIdentityDetails(g, upToDate.Message)
		g.Expect(rolling.Status).To(Equal(metav1.ConditionFalse), "IBM took nothing, so nothing rolls")
		settled := len(f.patches())

		// No UpdateMaster before the retry time the message names, and no status write either.
		for _, after := range []time.Duration{time.Second, 15 * time.Minute, 50 * time.Minute, time.Hour - time.Second} {
			result, err := f.reconcileAt(after, func() { f.expectObserve(normal) })
			g.Expect(err).NotTo(HaveOccurred())
			expectRequeueWithin(g, result, min(15*time.Minute*11/10, (time.Hour-after)*11/10+time.Second))
			upToDate, _ := f.masterConditions()
			g.Expect(upToDate.Message).To(Equal(deniedMessage), "after %s", after)
		}
		g.Expect(f.patches()).To(HaveLen(settled), "the kept refusal writes no status")

		// Exactly one request at the retry time; a role granted meanwhile takes effect then.
		_, err = f.reconcileAt(time.Hour, func() { f.expectRequest(normal, nil, "1.37", nil) })
		g.Expect(err).NotTo(HaveOccurred())
		expectMasterUpdate(g, f.controlPlane().Status.MasterUpdate, masterRecorded("1.37", time.Hour, 2))
		upToDate, _ = f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
		_, err = f.reconcileAt(time.Hour+time.Second, func() { f.expectObserve(normal) })
		g.Expect(err).NotTo(HaveOccurred())
	})

	t.Run("a new incident ID gives the same status", func(t *testing.T) {
		g := NewWithT(t)
		// Two control planes in the same state get the same refusal with different incident IDs. lastTransitionTime is
		// the real clock's (util/conditions), so the conditions are compared without it.
		type written struct {
			conditions map[string]string
			update     controlplanev1.MasterUpdateRequest
		}
		incidents := []string{"incident-a", "incident-b"}
		persisted := make([]written, 0, len(incidents))
		for _, incident := range incidents {
			f := newMasterFixture(t, "1.37", nil)
			f.expectVersions()
			f.expectRequest(cpCluster("normal"), nil, "1.37", forbidden(incident))
			_, err := f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			for _, body := range f.patches() {
				g.Expect(body).NotTo(ContainSubstring(incident), "the incident ID goes to the controller log only")
			}
			cp := f.controlPlane()
			w := written{conditions: map[string]string{}, update: cp.Status.MasterUpdate}
			for _, c := range cp.Status.Conditions {
				w.conditions[c.Type] = string(c.Status) + "/" + c.Reason + "/" + c.Message
			}
			persisted = append(persisted, w)
		}
		g.Expect(persisted[1]).To(Equal(persisted[0]), "the status does not depend on IBM's incident ID")
	})

	t.Run("the incident ID goes to the controller log and not into the condition", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		f.expectVersions()
		f.expectRequest(cpCluster("normal"), nil, "1.37", forbidden("incident-a"))
		captured := &capturedLog{}

		_, err := f.reconcileLogged(captured.logger())

		g.Expect(err).NotTo(HaveOccurred())
		// IBM support needs the incident ID: the controller log keeps IBM's whole answer (requestMasterUpdate).
		refusals := captured.matching(`"msg"="IBM Cloud did not complete a call"`,
			`"call"="requesting the master update of IBM cluster iksclusterid00000001 to 1.37"`)
		g.Expect(refusals).To(HaveLen(1), "captured log: %v", captured.lines)
		g.Expect(refusals[0]).To(ContainSubstring("[incident incident-a]"))
		g.Expect(refusals[0]).To(ContainSubstring("(code E3c1a)"))
		// The condition carries IBM's code only.
		upToDate, _ := f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason))
		g.Expect(upToDate.Message).To(Equal(deniedMessage))
		g.Expect(upToDate.Message).NotTo(ContainSubstring("incident"))
	})

	t.Run("a recorded version skip refusal stays until the generation changes", func(t *testing.T) {
		g := NewWithT(t)
		refusal := "IBM refused the master update to 1.37 as a version skip (IBM code E9999); set spec.distributionVersion to " +
			"the master's next minor; CAPIBM requests no update until the spec changes"
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.MasterUpdate = masterRecorded("1.37", -2*time.Hour, 1)
			objs.cp.Status.Conditions = append(objs.cp.Status.Conditions, metav1.Condition{
				Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition, Status: metav1.ConditionFalse,
				Reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
				Message: refusal, ObservedGeneration: masterTestGeneration, LastTransitionTime: metav1.NewTime(cpTestStart.Add(-2 * time.Hour))})
		})
		f.kube.EXPECT().ListVersions(gomock.Any()).Return(masterTestVersions(), nil).AnyTimes()

		result, err := f.reconcileAt(0, func() { f.expectObserve(cpCluster("normal")) })

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		upToDate, _ := f.masterConditions()
		g.Expect(upToDate.Message).To(Equal(refusal), "kept past the retry backoff, with no ListWorkers and no call")

		// A spec change raises metadata.generation (the fake client does not, so the test does).
		cp := f.controlPlane()
		cp.Generation++
		g.Expect(f.client.Update(ctx, cp)).To(Succeed())
		_, err = f.reconcileAt(time.Minute, func() { f.expectRequest(cpCluster("normal"), nil, "1.37", nil) })
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane().Status.MasterUpdate.Attempts).To(Equal(int32(2)))
	})
}

func TestControlPlaneMasterVersionCallBudget(t *testing.T) {
	t.Run("an up-to-date master costs only GetCluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.36", nil)
		f.expectObserve(cpCluster("normal"))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason))
		g.Expect(upToDate.Message).To(Equal("the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM " +
			"applies master patches itself"))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(rolling.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneNotRollingOutReason))
	})

	t.Run("a held request lists no workers and reads versions through the cache", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.MasterUpdate = masterRecorded("1.37", -time.Minute, 1)
		})
		f.expectVersions().Times(1) // one ListVersions for both reconciles: the per-identity cache answers the second

		for _, after := range []time.Duration{0, time.Minute} {
			_, err := f.reconcileAt(after, func() { f.expectObserve(cpCluster("normal")) })
			g.Expect(err).NotTo(HaveOccurred())
			upToDate, _ := f.masterConditions()
			g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
		}
	})

	t.Run("a 429 of a kubeconfig refresh holds every call of the step until Retry-After", func(t *testing.T) {
		g := NewWithT(t)
		// The Secret is CAPIBM's own and due for a refresh by age (a two-year certificate 366 days old, past half its
		// lifetime), so the kubeconfig step asks IBM for the admin kubeconfig, and IBM answers 429.
		stored := newKubeconfigCredentials(t, 366*24*time.Hour)
		value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, stored)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			objs.cp.Generation = masterTestGeneration
			objs.cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
			objs.cp.Spec.DistributionVersion = "1.37"
			objs.extra = append(objs.extra, ownedKubeconfigSecret(t, value, cpTestStart.Add(-366*24*time.Hour), caFetchedRecently()))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		rateLimited := &kubernetesservice.APIError{Operation: kubernetesservice.OpGetAdminKubeconfig, StatusCode: http.StatusTooManyRequests,
			RetryAfter: 10 * time.Minute}
		// No ListVersions, no ListWorkers and no UpdateMaster inside IBM's wait: gomock fails the test on any of them.
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().GetAdminKubeconfig(gomock.Any(), cpIKSClusterID).Return(nil, rateLimited))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Minute), "never before Retry-After")
		g.Expect(result.RequeueAfter).To(BeNumerically("<", 11*time.Minute))
		cp := f.controlPlane()
		g.Expect(cp.Status.MasterUpdate).To(BeZero(), "no attempt on record for a request that was never sent")
		g.Expect(conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)).To(BeNil(),
			"the step did not run")

		// Retry-After later the refresh attempt is on record, so the kubeconfig step waits for its own backoff, and the
		// step requests the update.
		_, err = f.reconcileAt(10*time.Minute, func() {
			f.expectVersions()
			f.expectRequest(cpCluster("normal"), nil, "1.37", nil)
		})
		g.Expect(err).NotTo(HaveOccurred())
		expectMasterUpdate(g, f.controlPlane().Status.MasterUpdate, masterRecorded("1.37", 10*time.Minute, 1))
	})

	t.Run("an IBM master patch is RollingOut with no call", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.36", nil)
		patching := cpCluster("normal")
		patching.TargetVersion = "1.36.6_1541"
		f.expectObserve(patching)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 2*time.Minute)
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue), "a patch never makes the master outdated")
		g.Expect(rolling.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(rolling.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason))
		g.Expect(rolling.Message).To(Equal("IBM is applying master patch 1.36.6_1541"))
	})

	t.Run("a newer patch on the same minor is UpToDate with no call", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.36", nil)
		patched := cpCluster("normal")
		patched.MasterKubeVersion, patched.TargetVersion = "1.36.6_1541", "1.36.6_1541"
		f.expectObserve(patched)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		upToDate, _ := f.masterConditions()
		g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(f.controlPlane().Status.DistributionVersion).To(Equal("1.36.6_1541"))
	})

	t.Run("the step waits for initialization", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.Initialization.ControlPlaneInitialized = nil
		})
		// The user's Secret does not pass the probe, so the control plane stays uninitialized: no ListVersions, no
		// ListWorkers and no condition of the master version step.
		f.r.Prober.(*kubeconfigProber).setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnreachable, "SelfSubjectReview")
		})
		f.expectObserve(cpCluster("normal"))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil())
		g.Expect(conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)).To(BeNil())
		g.Expect(cp.Status.MasterUpdate).To(BeZero())
	})
}

func TestControlPlaneMasterVersionGuards(t *testing.T) {
	testCases := []struct {
		name    string
		desired string
		// cluster changes the bound cluster IBM reports.
		cluster func(c *kubernetesservice.Cluster)
		// expect sets the IBM calls after the GetCluster.
		expect      func(f *cpFixture)
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
		wantRequeue time.Duration
	}{
		{name: "skip more than one minor makes no call", desired: "1.38",
			wantStatus: metav1.ConditionFalse, wantReason: "VersionSkipNotAllowed", wantRequeue: 5 * time.Minute,
			wantMessage: "spec.distributionVersion 1.38 is more than one minor above the master's minor 1.36; IBM updates a master " +
				"one minor at a time, so set spec.distributionVersion to 1.37 first"},
		{name: "downgrade makes no call", desired: "1.35",
			wantStatus: metav1.ConditionFalse, wantReason: "DowngradeNotAllowed", wantRequeue: 5 * time.Minute,
			wantMessage: "spec.distributionVersion 1.35 is below the master's minor 1.36 (IBM version 1.36.5_1537); IBM cannot roll " +
				"a master back, so set spec.distributionVersion to 1.36"},
		{name: "a minor IBM does not offer lists no workers", desired: "1.37",
			expect: func(f *cpFixture) {
				f.kube.EXPECT().ListVersions(gomock.Any()).Return(cpOfferedVersions(), nil)
			},
			wantStatus: metav1.ConditionFalse, wantReason: "VersionNotAvailable", wantRequeue: 5 * time.Minute,
			wantMessage: "IBM Cloud does not offer Kubernetes 1.37 (offered: 1.35, 1.36)"},
		{name: "worker skew blocks the request", desired: "1.37",
			expect: func(f *cpFixture) {
				gomock.InOrder(f.expectVersions(), f.expectWorkers(masterTestWorkers("1.36.5_1540", "1.34.9_1520")))
			},
			wantStatus: metav1.ConditionFalse, wantReason: "BlockedByWorkerSkew", wantRequeue: 5 * time.Minute,
			wantMessage: "IBM refuses a master update to 1.37 while a worker runs more than 2 minor(s) below it: worker w1; the update " +
				"waits until CAPIBM has replaced them with workers of the master's minor"},
		{name: "a CA rotation with an unknown status blocks the request", desired: "1.37",
			cluster:    func(c *kubernetesservice.Cluster) { c.CARotationStatus = "something IBM has not documented" },
			wantStatus: metav1.ConditionFalse, wantReason: "BlockedByCARotation", wantRequeue: time.Minute,
			wantMessage: "IBM reports a CA certificate rotation that has not completed; the master update to 1.37 waits until it completes"},
		{name: "an up-to-date master during a CA rotation is UpToDate", desired: "1.36",
			cluster:    func(c *kubernetesservice.Cluster) { c.CARotationStatus = "CA certificate rotation in progress" },
			wantStatus: metav1.ConditionTrue, wantReason: "UpToDate", wantRequeue: 5 * time.Minute},
		{name: "a deny-listed state blocks the request", desired: "1.37",
			cluster:    func(c *kubernetesservice.Cluster) { c.State = "critical" },
			wantStatus: metav1.ConditionFalse, wantReason: "BlockedByClusterState", wantRequeue: 5 * time.Minute,
			wantMessage: "IBM cluster state critical does not allow a master update; the update to 1.37 waits"},
		{name: "update_canceled is not blocked", desired: "1.37",
			cluster: func(c *kubernetesservice.Cluster) { c.State = "update_canceled" },
			expect: func(f *cpFixture) {
				gomock.InOrder(f.expectVersions(), f.expectWorkers(nil), f.expectUpdateMaster("1.37", nil))
			},
			wantStatus: metav1.ConditionFalse, wantReason: "UpgradePending", wantRequeue: 2 * time.Minute},
		{name: "unsupported is not blocked", desired: "1.37",
			cluster: func(c *kubernetesservice.Cluster) { c.State = "unsupported" },
			expect: func(f *cpFixture) {
				gomock.InOrder(f.expectVersions(), f.expectWorkers(nil), f.expectUpdateMaster("1.37", nil))
			},
			wantStatus: metav1.ConditionFalse, wantReason: "UpgradePending", wantRequeue: 2 * time.Minute},
		{name: "unsupported on the requested minor stays True with the note", desired: "1.36",
			cluster:    func(c *kubernetesservice.Cluster) { c.State = "unsupported" },
			wantStatus: metav1.ConditionTrue, wantReason: "UpToDate", wantRequeue: 5 * time.Minute,
			wantMessage: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches " +
				"itself; IBM no longer supports 1.36: upgrade one minor at a time, starting with spec.distributionVersion 1.37"},
		{name: "an end of support within 45 days is mentioned", desired: "1.36",
			cluster:    func(c *kubernetesservice.Cluster) { c.VersionEOS = "2026-12-20T23:59:59+0000" },
			wantStatus: metav1.ConditionTrue, wantReason: "UpToDate", wantRequeue: 5 * time.Minute,
			wantMessage: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches " +
				"itself; IBM ends support for 1.36 on 2026-12-20, so plan the update to 1.37"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newMasterFixture(t, tc.desired, nil)
			c := cpCluster("normal")
			if tc.cluster != nil {
				tc.cluster(c)
			}
			f.expectObserve(c)
			if tc.expect != nil {
				tc.expect(f)
			}

			result, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			expectRequeue(g, result, tc.wantRequeue)
			upToDate, _ := f.masterConditions()
			g.Expect(upToDate.Status).To(Equal(tc.wantStatus), upToDate.Message)
			g.Expect(upToDate.Reason).To(Equal(tc.wantReason), upToDate.Message)
			if tc.wantMessage != "" {
				g.Expect(upToDate.Message).To(Equal(tc.wantMessage))
			}
		})
	}
}

func TestControlPlaneMasterUpdateProgress(t *testing.T) {
	t.Run("a pending master writes no pending marker", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.MasterUpdate = masterRecorded("1.37", -10*time.Minute, 1)
		})
		pending := cpCluster("normal")
		pending.MasterKubeVersion, pending.TargetVersion = "1.37.1_1517 (pending)", "1.37.1_1517"
		f.expectObserve(pending)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 2*time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Status.DistributionVersion).To(Equal("1.37.1_1517"))
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(rolling.Message).To(Equal("IBM shows master version 1.37.1_1517 as pending"))
		for _, body := range f.patches() {
			g.Expect(body).NotTo(ContainSubstring("(pending)"), "no status write carries the (pending) marker")
		}
	})

	t.Run("IBM updating the master is RollingOut also while the master is not ready", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.MasterUpdate = masterRecorded("1.37", -10*time.Minute, 1)
		})
		updating := cpCluster("updating")
		updating.MasterState, updating.MasterStatus = "updating", "Version update in progress."
		updating.TargetVersion = "1.37.1_1517"
		f.expectObserve(updating)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute) // the master readiness poll is the shorter one
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(rolling.Message).To(Equal("IBM is updating the master to 1.37.1_1517"))
	})

	t.Run("the master on the requested minor clears masterUpdate", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", func(objs *cpObjects) {
			objs.cp.Status.MasterUpdate = masterRecorded("1.37", -40*time.Minute, 1)
		})
		done := cpCluster("normal")
		done.MasterKubeVersion, done.TargetVersion = "1.37.1_1517", "1.37.1_1517"
		f.expectObserve(done)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.MasterUpdate).To(BeZero())
		upToDate, rolling := f.masterConditions()
		g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(rolling.Status).To(Equal(metav1.ConditionFalse))
	})
}

func TestControlPlaneMasterVersionInputFailures(t *testing.T) {
	t.Run("a 403 on ListWorkers sends no UpdateMaster and changes no condition", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.expectVersions(),
			f.kube.EXPECT().ListWorkers(gomock.Any(), cpIKSClusterID).
				Return(nil, ibmErrorWithIncident(kubernetesservice.OpListWorkers, http.StatusForbidden, "", "incident-a")))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Status.MasterUpdate).To(BeZero())
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
	})

	t.Run("a 5xx on ListVersions is returned for the backoff", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(nil, ibmError(kubernetesservice.OpListVersions, http.StatusInternalServerError, "")))

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("listing the versions IBM Cloud offers in region us-south")))
	})

	t.Run("a 429 on ListVersions waits for Retry-After", func(t *testing.T) {
		g := NewWithT(t)
		f := newMasterFixture(t, "1.37", nil)
		gomock.InOrder(f.expectObserve(cpCluster("normal")),
			f.kube.EXPECT().ListVersions(gomock.Any()).Return(nil, &kubernetesservice.APIError{Operation: kubernetesservice.OpListVersions,
				StatusCode: http.StatusTooManyRequests, RetryAfter: 3 * time.Minute}))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically(">=", 3*time.Minute))
	})
}

func TestResultWithMasterVersion(t *testing.T) {
	transient := managedscope.Transient(errors.New("boom"))
	testCases := []struct {
		name    string
		flow    managedscope.Outcome
		master  managedscope.Outcome
		recheck time.Duration
		wantErr bool
		// wantMin and wantMax bound the requeue.
		wantMin, wantMax time.Duration
	}{
		{name: "the recheck time brings a terminal recheck forward", flow: managedscope.Done(),
			master: managedscope.Terminal("PermissionDenied", "m"), recheck: 90 * time.Second, wantMin: 90 * time.Second, wantMax: 99 * time.Second},
		{name: "a later recheck time changes nothing", flow: managedscope.Done(),
			master: managedscope.NotDone(managedscope.IntervalMasterUpdate, "UpgradePending"), recheck: 5 * time.Minute,
			wantMin: 100 * time.Second, wantMax: 140 * time.Second},
		{name: "the recheck time never shortens a rate limit", flow: managedscope.Done(),
			master: managedscope.RateLimited(10 * time.Minute), recheck: time.Minute, wantMin: 10 * time.Minute, wantMax: 11 * time.Minute},
		{name: "a step that did not run changes nothing", flow: managedscope.Done(),
			wantMin: 4 * time.Minute, wantMax: 6 * time.Minute},
		{name: "a Done step never shortens a terminal recheck", flow: managedscope.Terminal("PermissionDenied", "m"), master: managedscope.Done(),
			wantMin: 13 * time.Minute, wantMax: 17 * time.Minute},
		{name: "the master update poll is earlier than the steady poll", flow: managedscope.Done(),
			master: managedscope.NotDone(managedscope.IntervalMasterUpdate, "UpgradePending"), wantMin: 100 * time.Second, wantMax: 140 * time.Second},
		{name: "the flow's earlier poll stays", flow: managedscope.NotDone(managedscope.IntervalClusterProvisioning, "MasterNotReady"),
			master: managedscope.NotDone(managedscope.IntervalMasterUpdate, "UpgradePending"), wantMin: 50 * time.Second, wantMax: 70 * time.Second},
		{name: "a terminal step is rechecked no later than the steady poll", flow: managedscope.Done(),
			master: managedscope.Terminal("VersionNotAvailable", "m"), wantMin: 4 * time.Minute, wantMax: 6 * time.Minute},
		{name: "the flow's rate limit is never shortened", flow: managedscope.RateLimited(10 * time.Minute),
			master: managedscope.NotDone(managedscope.IntervalMasterUpdate, "UpgradePending"), wantMin: 10 * time.Minute, wantMax: 11 * time.Minute},
		{name: "the step's rate limit replaces the flow's poll", flow: managedscope.Done(),
			master: managedscope.RateLimited(10 * time.Minute), wantMin: 10 * time.Minute, wantMax: 11 * time.Minute},
		{name: "the flow's error wins", flow: transient, master: managedscope.NotDone(managedscope.IntervalMasterUpdate, "x"), wantErr: true},
		{name: "the step's error wins", flow: managedscope.Done(), master: transient, wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			result, err := resultWithMasterVersion(ctx, tc.flow, tc.master, tc.recheck)
			if tc.wantErr {
				g.Expect(err).To(MatchError("boom"))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(BeNumerically(">=", tc.wantMin))
			g.Expect(result.RequeueAfter).To(BeNumerically("<=", tc.wantMax))
		})
	}
}

// startMasterManager starts the control plane controller for namespace behind a manager whose cache sees only that
// namespace, so that the objects of other tests never reach it, with its real watches and a real clock; kube answers
// its IBM calls and prober its kubeconfig probes. The manager stops when the test ends, before the cleanups
// registered earlier run.
func startMasterManager(t *testing.T, namespace string, kube kubernetesservice.KubernetesService, prober *kubeconfigProber) {
	t.Helper()
	mgr := newNamespacedTestManager(t, namespace)
	r := newEnvtestControlPlaneReconciler(t)
	r.Client, r.APIReader = mgr.GetClient(), mgr.GetAPIReader()
	r.Resolver, r.ClientBuilder, r.Prober = &recordingResolver{}, &stubClientBuilder{kubernetes: kube}, prober
	r.Clock = clock.RealClock{}
	NewWithT(t).Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
	startTestManager(t, mgr)
}

// createEnvtestMasterControlPlane creates, in namespace, a bound and initialized IKS control plane on IBM version
// 1.36.5_1537 whose spec asks for 1.37, with its Cluster, IBMCloudManagedCluster and a user-provided kubeconfig Secret,
// and pauses the Cluster when the test ends, so that a later test whose manager watches every namespace leaves it
// alone. It returns the control plane's key.
func createEnvtestMasterControlPlane(t *testing.T, namespace string) client.ObjectKey {
	t.Helper()
	g := NewWithT(t)
	cluster := createTestCluster(t, namespace, cpTestName, managedControlPlaneRef(cpTestName))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
	createTestInfra(t, namespace, cpTestName, cluster)
	cp := createEnvtestControlPlane(t, namespace, cpTestName, cluster)
	key := client.ObjectKeyFromObject(cp)
	cp.Finalizers = []string{controlplanev1.IBMCloudManagedControlPlaneFinalizer}
	cp.Spec.ControlPlaneEndpoint = cpTestEndpoint
	g.Expect(testEnv.Update(ctx, cp)).To(Succeed())
	cp.Status.ClusterID, cp.Status.DistributionVersion = cpIKSClusterID, "1.36.5_1537"
	cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())
	// The webhook allows one minor above the observed one, so the spec is raised after status records 1.36.
	cp.Spec.DistributionVersion = "1.37"
	g.Expect(testEnv.Update(ctx, cp)).To(Succeed())
	creds := newKubeconfigCredentials(t, time.Hour)
	g.Expect(testEnv.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: cpTestName + "-kubeconfig",
			Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"value": buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)},
	})).To(Succeed())
	pauseAtCleanup(t, cluster, key)
	return key
}

// TestControlPlaneEnvtestMasterUpdateSettles runs a master update request behind a manager, with the real watches and
// a real clock, against the envtest API server with the CRD validation of every status write: the request's own status
// patches re-trigger the controller, which must neither send UpdateMaster again within MasterUpdatePendingGrace nor
// keep writing status, also when IBM refuses with a new incident ID on every answer.
func TestControlPlaneEnvtestMasterUpdateSettles(t *testing.T) {
	for _, tc := range []struct {
		name string
		// answer is IBM's answer to the n-th UpdateMaster.
		answer     func(n int64) error
		wantReason string
	}{
		{name: "accepted_request", answer: func(int64) error { return nil },
			wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason},
		{name: "UpdateMaster_403_with_a_new_incident_ID_on_every_answer", answer: func(n int64) error {
			return ibmErrorWithIncident(kubernetesservice.OpUpdateMaster, http.StatusForbidden, "E3c1a", fmt.Sprintf("incident-%d", n))
		}, wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason},
	} {
		t.Run(tc.name+"_manager_settles", func(t *testing.T) {
			t.Parallel()
			g := NewWithT(t)
			namespace := newTestNamespace(t, "cp-master")
			key := createEnvtestMasterControlPlane(t, namespace)

			var gets, updates atomic.Int64
			kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
			kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").DoAndReturn(
				func(context.Context, string, string) (*kubernetesservice.Cluster, error) {
					gets.Add(1)
					return cpCluster("normal"), nil
				}).AnyTimes()
			kube.EXPECT().ListVersions(gomock.Any()).Return(masterTestVersions(), nil).AnyTimes()
			kube.EXPECT().ListWorkers(gomock.Any(), cpIKSClusterID).Return(masterTestWorkers("1.36.5_1540"), nil).AnyTimes()
			kube.EXPECT().UpdateMaster(gomock.Any(), cpIKSClusterID, "1.37").DoAndReturn(
				func(context.Context, string, string) error {
					return tc.answer(updates.Add(1))
				}).AnyTimes()
			prober := &kubeconfigProber{t: t}
			prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
			startMasterManager(t, namespace, kube, prober)

			g.Eventually(func(g Gomega) {
				persisted := getEnvtestControlPlane(t, key)
				condition := conditions.Get(persisted, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Reason).To(Equal(tc.wantReason), condition.Message)
				g.Expect(persisted.Status.MasterUpdate.Version).To(Equal("1.37"))
			}, 30*time.Second, 100*time.Millisecond).Should(Succeed())

			// The start-up events of the Cluster and the control plane may add a reconcile or two; a hot loop makes
			// hundreds of GetCluster calls a second. The count must settle low and then stay put, with one request.
			settled := int64(-1)
			g.Eventually(func() bool {
				n := gets.Load()
				stable := n == settled
				settled = n
				return stable
			}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "GetCluster calls keep growing")
			g.Expect(settled).To(BeNumerically("<=", 6), "GetCluster calls before the status settled")
			g.Consistently(gets.Load, noHotLoopWindow, 100*time.Millisecond).Should(Equal(settled))
			g.Expect(updates.Load()).To(Equal(int64(1)), "exactly one UpdateMaster")
			message := conditions.GetMessage(getEnvtestControlPlane(t, key), controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
			g.Expect(message).NotTo(ContainSubstring("incident"))
		})
	}
}
