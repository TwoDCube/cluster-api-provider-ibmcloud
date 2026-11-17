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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// The master version tests run the rows of DecideMasterAction on scrubbed captures from IBM Cloud: an IKS master on
// 1.36.5_1537 and a ROKS master on 4.21.35_1537_openshift, with IBM's offered versions of 2026-11-16.

// masterNow is the clock of the master version tests.
var masterNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// masterOffered decodes the live getVersions capture (kubernetesservice testdata) into the domain type.
func masterOffered(t *testing.T) *kubernetesservice.Versions {
	t.Helper()
	data, err := os.ReadFile(fixturePath("versions.json"))
	if err != nil {
		t.Fatalf("reading versions fixture: %v", err)
	}
	type release struct {
		Major        int    `json:"major"`
		Minor        int    `json:"minor"`
		Patch        int    `json:"patch"`
		Default      bool   `json:"default"`
		EndOfService string `json:"end_of_service"`
	}
	var f struct {
		Kubernetes []release `json:"kubernetes"`
		OpenShift  []release `json:"openshift"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decoding versions fixture: %v", err)
	}
	convert := func(in []release) []kubernetesservice.ReleaseVersion {
		out := make([]kubernetesservice.ReleaseVersion, 0, len(in))
		for _, r := range in {
			out = append(out, kubernetesservice.ReleaseVersion{Major: r.Major, Minor: r.Minor, Patch: r.Patch,
				Default: r.Default, EndOfService: r.EndOfService})
		}
		return out
	}
	return &kubernetesservice.Versions{Kubernetes: convert(f.Kubernetes), OpenShift: convert(f.OpenShift)}
}

// masterFixtures holds the live captures the master version tests start from, loaded once per test so that the
// table's input functions need no *testing.T.
type masterFixtures struct {
	iksCluster, roksCluster kubernetesservice.Cluster
	offered                 *kubernetesservice.Versions
	iksWorkers, roksWorkers []kubernetesservice.Worker
}

// loadMasterFixtures loads the captures: the IKS master on 1.36.5_1537 with its worker on 1.36.5_1540, the ROKS master
// on 4.21.35_1537_openshift with two workers on 4.21.40_1540_openshift (worker fix packs run ahead of the master's, as
// captured on IBM Cloud), and IBM's offered versions.
func loadMasterFixtures(t *testing.T) masterFixtures {
	t.Helper()
	return masterFixtures{
		iksCluster:  loadCluster(t, "iks-getCluster-normal.json"),
		roksCluster: loadCluster(t, "roks-getCluster-normal.json"),
		offered:     masterOffered(t),
		iksWorkers:  loadWorkers(t, "iks-getWorkers-normal.json"),
		roksWorkers: loadWorkers(t, "roks-getWorkers-normal.json"),
	}
}

// iks returns the input of an IKS control plane whose spec asks for desired, with fresh copies of the captures, so
// that a test case may change them.
func (f masterFixtures) iks(desired string) MasterVersionInput {
	return f.input(controlplanev1.DistributionKubernetes, desired, f.iksCluster, f.iksWorkers)
}

// roks is iks for the ROKS captures.
func (f masterFixtures) roks(desired string) MasterVersionInput {
	return f.input(controlplanev1.DistributionOpenShift, desired, f.roksCluster, f.roksWorkers)
}

// input builds an input from copies of a cluster, the offered versions and the workers.
func (f masterFixtures) input(d controlplanev1.Distribution, desired string, c kubernetesservice.Cluster,
	workers []kubernetesservice.Worker,
) MasterVersionInput {
	return MasterVersionInput{
		Distribution:   d,
		DesiredVersion: desired,
		Cluster:        &c,
		Offered: &kubernetesservice.Versions{Kubernetes: slices.Clone(f.offered.Kubernetes),
			OpenShift: slices.Clone(f.offered.OpenShift)},
		Workers:       slices.Clone(workers),
		WorkersListed: true,
		Now:           masterNow,
	}
}

// masterRequested returns a status.masterUpdate for version, requested ago before masterNow, attempts times.
func masterRequested(version string, ago time.Duration, attempts int32) controlplanev1.MasterUpdateRequest {
	return controlplanev1.MasterUpdateRequest{Version: version, LastRequestTime: metav1.NewTime(masterNow.Add(-ago)),
		Attempts: attempts}
}

// masterWorker returns a deployed worker running actual.
func masterWorker(id, actual string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, ActualState: WorkerStateDeployed, HealthState: "normal",
		KubeVersionActual: actual, KubeVersionTarget: actual}
}

// masterGeneration is the metadata.generation of the control plane in the refusal tests.
const masterGeneration = 3

// masterDenied is the message MasterUpdateCallOutcome writes for an UpdateMaster 403 on a request of 1.37 sent 15
// minutes before masterNow (attempt 1, so the retry is 45 minutes after masterNow).
const masterDenied = "IBM Cloud Kubernetes Service refused the master update to 1.37 with HTTP 403: the credentials of " +
	"the CAPIBM controller identity need the Administrator platform role and the Manager service role of Kubernetes " +
	"Service on the cluster's resource group. CAPIBM requests one minor at a time, so this is not IBM's refusal to skip " +
	"versions; CAPIBM requests the update again at 2026-11-20T12:45:00Z"

// masterListDenied is the message masterInputFailure writes for a 403 with IBM code E3c1a to the ListVersions call of
// an IKS control plane whose spec asks for 1.37.
const masterListDenied = "IBM Cloud Kubernetes Service refused listing the versions IBM Cloud offers in region us-south " +
	"with HTTP 403 (IBM code E3c1a), so the master update to 1.37 waits: the credentials of the CAPIBM controller identity " +
	"need the Administrator platform role and the Manager service role of Kubernetes Service on the cluster's resource " +
	"group; CAPIBM tries again at the next poll of the control plane"

// masterCurrent returns a MasterVersionUpToDate=False condition with reason and message as util/conditions Set stores
// it for generation, the form MasterVersionInput.CurrentUpToDate takes.
func masterCurrent(reason, message string, generation int64) *metav1.Condition {
	return &metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
		Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(masterNow.Add(-time.Hour))}
}

func TestDecideMasterAction(t *testing.T) {
	fx := loadMasterFixtures(t)
	testCases := []struct {
		name   string
		in     func() MasterVersionInput
		reason string
		// status is the MasterVersionUpToDate status; False when empty.
		status    metav1.ConditionStatus
		rolling   bool
		pending   bool
		request   bool
		clear     bool
		attempts  int32
		recheckAt time.Time
		message   []string
	}{
		// Row 3: the master is on the requested minor, whatever patch IBM runs.
		{name: "up to date on the live capture", status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"the master runs 1.36.5_1537, minor 1.36"},
			in:      func() MasterVersionInput { return fx.iks("1.36") }},
		{name: "newer IBM patch on the same minor is up to date", status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"the master runs 1.36.6_1540"},
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.MasterKubeVersion, in.Cluster.TargetVersion = "1.36.6_1540", "1.36.6_1540"
				return in
			}},
		{name: "ROKS up to date", status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"the master runs 4.21.35_1537_openshift, minor 4.21"},
			in:      func() MasterVersionInput { return fx.roks("4.21") }},
		{name: "up to date wins over a CA rotation and a blocked state", status: metav1.ConditionTrue,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.CARotationStatus, in.Cluster.State = "CA certificate rotation in progress", "critical"
				return in
			}},
		{name: "unsupported minor is named", status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"IBM no longer supports 1.36: upgrade one minor at a time, starting with spec.distributionVersion 1.37"},
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.State = "unsupported"
				return in
			}},
		{name: "end of support within 45 days is mentioned", status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"IBM ends support for 1.36 on 2027-01-03, so plan the update to 1.37"},
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.VersionEOS = "2027-01-03T23:59:59+0000" // 44 days after masterNow
				return in
			}},
		{name: "end of support further away is not mentioned", status: metav1.ConditionTrue,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.VersionEOS = "2027-01-20T23:59:59+0000"
				return in
			}},
		{name: "an end of support CAPIBM cannot parse is named", status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"IBM reports the end of support of 1.36 as Jan 2027"},
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.VersionEOS = "Jan 2027"
				return in
			}},

		// Row 1: IBM shows an update.
		{name: "pending marker on the requested minor", rolling: true, pending: true,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message: []string{"IBM shows master version 1.37.1_1517 as pending"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.MasterKubeVersion, in.Cluster.TargetVersion = "1.37.1_1517 (pending)", "1.37.1_1517"
				in.MasterUpdate = masterRequested("1.37", 10*time.Minute, 1)
				return in
			}},
		{name: "pending target version on the next minor", rolling: true, pending: true,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message: []string{"IBM is updating the master to 1.37.1_1517"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.TargetVersion = "1.37.1_1517"
				in.MasterUpdate = masterRequested("1.37", 10*time.Minute, 1)
				return in
			}},
		{name: "pending state updating", rolling: true, pending: true,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message: []string{"IBM is updating the master from 1.36.5_1537 (cluster state updating)"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.State = "updating"
				in.MasterUpdate = masterRequested("1.37", 10*time.Minute, 1)
				return in
			}},
		{name: "IBM-driven patch is rolling out and up to date", rolling: true, pending: true, status: metav1.ConditionTrue,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			message: []string{"the master runs 1.36.5_1537"},
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.TargetVersion = "1.36.6_1540"
				return in
			}},
		{name: "IBM patch delays a new request", rolling: true, pending: true,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message: []string{"IBM is applying master patch 1.36.6_1540; the update to 1.37 waits until IBM finishes it"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.TargetVersion = "1.36.6_1540"
				return in
			}},
		{name: "pending with a skip in the spec", rolling: true, pending: true,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.38")
				in.Cluster.State = "updating"
				return in
			}},
		{name: "pending minor reached with a stale request reports the patch", rolling: true, pending: true, status: metav1.ConditionTrue,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.TargetVersion = "1.36.6_1540"
				in.MasterUpdate = masterRequested("1.36", 3*time.Hour, 1)
				return in
			}},

		// Row 2: the requested minor was reached.
		{name: "request reached is cleared", status: metav1.ConditionTrue, clear: true,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.MasterUpdate = masterRequested("1.36", time.Hour, 1)
				return in
			}},
		{name: "ROKS request reached is cleared", status: metav1.ConditionTrue, clear: true,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			in: func() MasterVersionInput {
				in := fx.roks("4.21")
				in.MasterUpdate = masterRequested("4.21_openshift", time.Hour, 2)
				return in
			}},

		// Rows 4 and 5: guards before an update.
		{name: "CA rotation blocks", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason,
			message: []string{"CA certificate rotation that has not completed"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.CARotationStatus = "CA certificate rotation in progress"
				return in
			}},
		{name: "unknown CA rotation status blocks fail-safe", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.CARotationStatus = "Rotating root of trust (phase 2)"
				return in
			}},
		{name: "completed CA rotation does not block", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.CARotationStatus = "CA certificate rotation complete. Rotated at 2026-11-01"
				return in
			}},
		{name: "unhealthy master blocks", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason,
			message: []string{"IBM reports master health unavailable"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.MasterHealth = "unavailable"
				return in
			}},
		{name: "unsupported state is allowed", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.State, in.Cluster.MasterHealth = "unsupported", "unsupported"
				return in
			}},
		{name: "update_canceled state is allowed", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.State = "update_canceled"
				return in
			}},

		// Rows 6 and 7: the spec asks for what IBM cannot do.
		{name: "downgrade", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateDowngradeNotAllowedReason,
			message: []string{"spec.distributionVersion 1.35 is below the master's minor 1.36 (IBM version 1.36.5_1537)", "set spec.distributionVersion to 1.36"},
			in:      func() MasterVersionInput { return fx.iks("1.35") }},
		{name: "version skip", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
			message: []string{"spec.distributionVersion 1.38 is more than one minor above the master's minor 1.36", "set spec.distributionVersion to 1.37 first"},
			in:      func() MasterVersionInput { return fx.iks("1.38") }},
		{name: "another major is a skip", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
			in: func() MasterVersionInput { return fx.iks("2.0") }},

		// Row 8: not offered.
		{name: "minor not offered", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason,
			message: []string{"IBM Cloud does not offer Kubernetes 1.37 (offered: 1.34, 1.35, 1.36)"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Offered.Kubernetes = in.Offered.Kubernetes[:3]
				return in
			}},
		{name: "ROKS not offered names the OpenShift minors", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason,
			message: []string{"IBM Cloud does not offer Red Hat OpenShift 4.22 (offered: none)"},
			in: func() MasterVersionInput {
				in := fx.roks("4.22")
				in.Offered.OpenShift = nil
				return in
			}},

		// Row 9: worker skew, IKS 2 and ROKS 1.
		{name: "IKS worker two minors behind the target is allowed", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Workers = append(in.Workers, masterWorker("w-135", "1.35.9_1530"))
				return in
			}},
		{name: "IKS worker three minors behind the target blocks", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason,
			message: []string{"IBM refuses a master update to 1.37 while a worker runs more than 2 minor(s) below it: worker w-134;"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Workers = append(in.Workers, masterWorker("w-134", "1.34.12_1520"))
				return in
			}},
		{name: "ROKS worker one minor behind the target is allowed", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in:     func() MasterVersionInput { return fx.roks("4.22") }},
		{name: "ROKS worker two minors behind the target blocks", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason,
			message: []string{"more than 1 minor(s) below it: workers w-a, w-b, w-c and 1 more;"},
			in: func() MasterVersionInput {
				in := fx.roks("4.22")
				in.Workers = append(in.Workers, masterWorker("w-d", "4.20.41_1530_openshift"), masterWorker("w-b", "4.20.41_1530_openshift"),
					masterWorker("w-a", "4.20.41_1530_openshift"), masterWorker("w-c", "garbage"))
				return in
			}},
		{name: "deleted and provisioning workers do not count", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				deleted := masterWorker("w-old", "1.33.1_1500")
				deleted.ActualState = WorkerStateDeleted
				provisioning := masterWorker("w-new", "")
				provisioning.ActualState = "provisioning"
				in.Workers = append(in.Workers, deleted, provisioning)
				return in
			}},

		// Rows 10 to 12: request, grace, backoff.
		{name: "request the next minor", request: true, rolling: true, attempts: 1,
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message: []string{"CAPIBM requests the master update from 1.36.5_1537 to 1.37"},
			in:      func() MasterVersionInput { return fx.iks("1.37") }},
		{name: "no second call within the pending grace", rolling: true, recheckAt: masterNow.Add(time.Minute),
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message: []string{"CAPIBM requested the master update to 1.37 at 2026-11-20T11:56:00Z; waiting for IBM to show it"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 4*time.Minute, 1)
				return in
			}},
		{name: "failed update back on the old minor waits 1h", recheckAt: masterNow.Add(55 * time.Minute),
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			message: []string{"the master update to 1.37 that CAPIBM requested at 2026-11-20T11:55:00Z has not taken effect: " +
				"the master still runs 1.36.5_1537 and IBM shows no update in progress; CAPIBM requests it again at 2026-11-20T12:55:00Z"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", MasterUpdatePendingGrace, 1)
				return in
			}},
		{name: "retry after 1h", request: true, rolling: true, attempts: 2,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", time.Hour, 1)
				return in
			}},
		{name: "second failure waits 2h", recheckAt: masterNow.Add(time.Minute),
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 2*time.Hour-time.Minute, 2)
				return in
			}},
		{name: "third failure waits 4h", recheckAt: masterNow.Add(time.Hour),
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 3*time.Hour, 3)
				return in
			}},
		{name: "backoff capped at 24h", request: true, rolling: true, attempts: 11,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 24*time.Hour, 10)
				return in
			}},
		{name: "a request for another version restarts the count", request: true, rolling: true, attempts: 1, clear: true,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.36", time.Minute, 4)
				return in
			}},
		{name: "ROKS requests the OpenShift form", request: true, rolling: true, attempts: 1,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in:     func() MasterVersionInput { return fx.roks("4.22") }},

		// The refusal row: IBM refused the request status.masterUpdate records, so IBM shows nothing pending, and the
		// condition written from that answer is the only record of it.
		{name: "UpdateMaster 403 is kept within the grace", recheckAt: masterNow.Add(58 * time.Minute),
			reason: masterPermissionDeniedReason, message: []string{masterDenied},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 2*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "UpdateMaster 403 is kept at the terminal recheck until the retry", recheckAt: masterNow.Add(45 * time.Minute),
			reason: masterPermissionDeniedReason, message: []string{masterDenied},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "UpdateMaster 403 is kept through a longer backoff", recheckAt: masterNow.Add(time.Minute),
			reason: masterPermissionDeniedReason, message: []string{masterDenied},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 4*time.Hour-time.Minute, 3)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "UpdateMaster 403 is retried at the backoff time", request: true, rolling: true, attempts: 2,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", time.Hour, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "UpdateMaster 403 of an earlier generation is not kept", recheckAt: masterNow.Add(45 * time.Minute),
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			message: []string{"has not taken effect"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration-1), masterGeneration
				return in
			}},
		{name: "UpdateMaster 403 for another version is not kept", request: true, rolling: true, attempts: 1, clear: true,
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.36", 15*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "ROKS UpdateMaster 403 is kept by its OpenShift version", recheckAt: masterNow.Add(45 * time.Minute),
			reason:  masterPermissionDeniedReason,
			message: []string{"refused the master update to 4.22_openshift with HTTP 403"},
			in: func() MasterVersionInput {
				in := fx.roks("4.22")
				in.MasterUpdate = masterRequested("4.22_openshift", 15*time.Minute, 1)
				refused := masterUpdateOutcome(kubernetesservice.ClassPermissionDenied, nil, in.MasterUpdate, "the CAPIBM controller identity")
				in.CurrentUpToDate, in.Generation = masterCurrent(refused.Reason, refused.Message, masterGeneration), masterGeneration
				return in
			}},
		// A 403 to the ListVersions call of this step has the reason PermissionDenied too, but it is no record of IBM's
		// refusal of UpdateMaster: once the listing works again, rows 10 and 11 report the request on record.
		{name: "a ListVersions 403 is not kept as a refusal after the grace", recheckAt: masterNow.Add(45 * time.Minute),
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			message: []string{"the master update to 1.37 that CAPIBM requested at 2026-11-20T11:45:00Z has not taken effect"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterListDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "a ListVersions 403 is not kept as a refusal within the grace", rolling: true,
			recheckAt: masterNow.Add(4 * time.Minute),
			reason:    controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			message:   []string{"CAPIBM requested the master update to 1.37 at 2026-11-20T11:59:00Z; waiting for IBM to show it"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterListDenied, masterGeneration), masterGeneration
				return in
			}},
		{name: "version skip refusal is kept until the spec changes",
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
			message: []string{"IBM refused the master update to 1.37 as a version skip"},
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 48*time.Hour, 1)
				in.CurrentUpToDate = masterCurrent(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
					"IBM refused the master update to 1.37 as a version skip; set spec.distributionVersion to the master's next minor",
					masterGeneration)
				in.Generation = masterGeneration
				return in
			}},
		{name: "row 7 skip of a changed spec is not a refusal", recheckAt: masterNow.Add(45 * time.Minute),
			reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			message: []string{"has not taken effect"},
			in: func() MasterVersionInput {
				// spec.distributionVersion went 1.37 -> 1.38 (row 7 wrote VersionSkipNotAllowed for generation 2) -> 1.37.
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate = masterCurrent(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
					"spec.distributionVersion 1.38 is more than one minor above the master's minor 1.36", masterGeneration-1)
				in.Generation = masterGeneration
				return in
			}},
		{name: "upgrade pending condition is not a refusal", recheckAt: masterNow.Add(45 * time.Minute),
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate = masterCurrent(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
					"CAPIBM requests the master update from 1.36.5_1537 to 1.37", masterGeneration)
				in.Generation = masterGeneration
				return in
			}},
		{name: "true condition is not a refusal", recheckAt: masterNow.Add(45 * time.Minute),
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration)
				in.CurrentUpToDate.Status, in.Generation = metav1.ConditionTrue, masterGeneration
				return in
			}},

		// Row 9 after the hold rows: it protects a request, so it is checked before every request but not while one is
		// held, which saves its ListWorkers call.
		{name: "skew is checked again before a retry", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", time.Hour, 1)
				in.Workers = append(in.Workers, masterWorker("w-134", "1.34.12_1520"))
				return in
			}},
		{name: "a held request reports the hold, not a later skew", recheckAt: masterNow.Add(45 * time.Minute),
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.Workers = append(in.Workers, masterWorker("w-134", "1.34.12_1520"))
				return in
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := tc.in()
			got, err := DecideMasterAction(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.NeedsOfferedVersions).To(BeFalse())
			g.Expect(got.NeedsWorkers).To(BeFalse())
			wantStatus := tc.status
			if wantStatus == "" {
				wantStatus = metav1.ConditionFalse
			}
			g.Expect(got.UpToDate.Type).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition))
			g.Expect(got.UpToDate.Status).To(Equal(wantStatus))
			g.Expect(got.UpToDate.Reason).To(Equal(tc.reason))
			g.Expect(got.UpToDate.Message).NotTo(BeEmpty())
			for _, part := range tc.message {
				g.Expect(got.UpToDate.Message).To(ContainSubstring(part))
			}
			g.Expect(got.RollingOut.Type).To(Equal(controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition))
			g.Expect(got.RollingOut.Status == metav1.ConditionTrue).To(Equal(tc.rolling))
			if tc.rolling {
				g.Expect(got.RollingOut.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason))
			} else {
				g.Expect(got.RollingOut.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneNotRollingOutReason))
			}
			g.Expect(got.UpdatePending).To(Equal(tc.pending))
			g.Expect(got.ClearMasterUpdate).To(Equal(tc.clear))
			g.Expect(got.RecheckAt).To(Equal(tc.recheckAt))
			g.Expect(got.RequestUpdate).To(Equal(tc.request))
			if tc.request {
				desired, err := ibmversion.ParseMinor(in.DesiredVersion)
				g.Expect(err).NotTo(HaveOccurred())
				wantVersion := MasterUpdateVersion(in.Distribution, desired)
				g.Expect(got.UpdateVersion).To(Equal(wantVersion))
				g.Expect(got.MasterUpdate).To(Equal(controlplanev1.MasterUpdateRequest{Version: wantVersion,
					LastRequestTime: metav1.NewTime(masterNow), Attempts: tc.attempts}))
			} else {
				g.Expect(got.UpdateVersion).To(BeEmpty())
				g.Expect(got.MasterUpdate).To(Equal(controlplanev1.MasterUpdateRequest{}))
			}
			// No condition ever carries IBM's pending marker.
			g.Expect(got.UpToDate.Message + got.RollingOut.Message).NotTo(ContainSubstring("pending)"))
		})
	}
}

// TestDecideMasterActionAsksForInputs shows that the decision asks for the offered versions only at row 8 and for the
// worker listing only right before a request, so that a reconcile that requests nothing makes no IBM call beyond
// GetCluster.
func TestDecideMasterActionAsksForInputs(t *testing.T) {
	fx := loadMasterFixtures(t)
	noWorkers := func(in *MasterVersionInput) { in.Workers, in.WorkersListed = nil, false }
	testCases := []struct {
		name   string
		mutate func(in *MasterVersionInput)
		// want is the input asked for: "offered", "workers", or "" for none.
		want string
		// reason is the MasterVersionUpToDate reason when nothing is asked for.
		reason string
	}{
		{name: "next minor without the offered versions asks for them, not for workers", want: "offered",
			mutate: func(in *MasterVersionInput) { in.Offered, in.Workers, in.WorkersListed = nil, nil, false }},
		{name: "next minor without the worker listing", mutate: noWorkers, want: "workers"},
		{name: "not offered never asks for workers",
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason,
			mutate: func(in *MasterVersionInput) {
				noWorkers(in)
				in.Offered.Kubernetes = in.Offered.Kubernetes[:3]
			}},
		{name: "pending grace never asks for workers",
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			mutate: func(in *MasterVersionInput) {
				noWorkers(in)
				in.MasterUpdate = masterRequested("1.37", time.Minute, 1)
			}},
		{name: "backoff never asks for workers",
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			mutate: func(in *MasterVersionInput) {
				noWorkers(in)
				in.MasterUpdate = masterRequested("1.37", 20*time.Hour, 6)
			}},
		{name: "kept refusal never asks for workers", reason: masterPermissionDeniedReason,
			mutate: func(in *MasterVersionInput) {
				noWorkers(in)
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
			}},
		{name: "a retry asks for workers", mutate: func(in *MasterVersionInput) {
			noWorkers(in)
			in.MasterUpdate = masterRequested("1.37", time.Hour, 1)
		}, want: "workers"},
		{name: "an empty listing is a listing", mutate: func(in *MasterVersionInput) { in.Workers = nil },
			reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason},
		{name: "up to date needs neither", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
			mutate: func(in *MasterVersionInput) { in.DesiredVersion, in.Offered, in.WorkersListed = "1.36", nil, false }},
		{name: "a skip needs neither", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
			mutate: func(in *MasterVersionInput) { in.DesiredVersion, in.Offered, in.WorkersListed = "1.38", nil, false }},
		{name: "a blocked state needs neither", reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason,
			mutate: func(in *MasterVersionInput) {
				in.Cluster.State, in.Offered, in.WorkersListed = "update_failed", nil, false
			}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := fx.iks("1.37")
			tc.mutate(&in)
			got, err := DecideMasterAction(in)
			g.Expect(err).NotTo(HaveOccurred())
			switch tc.want {
			case "offered":
				g.Expect(got).To(Equal(MasterAction{NeedsOfferedVersions: true}), "no other field is set")
			case "workers":
				g.Expect(got).To(Equal(MasterAction{NeedsWorkers: true}), "no other field is set")
			default:
				g.Expect(got.NeedsOfferedVersions).To(BeFalse())
				g.Expect(got.NeedsWorkers).To(BeFalse())
				g.Expect(got.UpToDate.Reason).To(Equal(tc.reason))
			}
		})
	}
}

func TestDecideMasterActionBlockedStates(t *testing.T) {
	fx := loadMasterFixtures(t)
	// Every state of the deny-list (MasterUpdateBlockedStates) blocks the update; the provisioning and updating states
	// are also pending or not reachable here, so the guard is checked on a master that IBM does not show as updating.
	for _, state := range MasterUpdateBlockedStates() {
		t.Run("IBM state "+state, func(t *testing.T) {
			g := NewWithT(t)
			in := fx.iks("1.37")
			in.Cluster.State = state
			got, err := DecideMasterAction(in)
			g.Expect(err).NotTo(HaveOccurred())
			if state == "updating" {
				g.Expect(got.UpdatePending).To(BeTrue(), "updating is IBM's own update in progress (row 1)")
				return
			}
			g.Expect(got.UpToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason))
			g.Expect(got.UpToDate.Message).To(ContainSubstring("IBM cluster state " + state + " does not allow a master update"))
			g.Expect(got.RequestUpdate).To(BeFalse())
		})
	}
	t.Run("master health error", func(t *testing.T) {
		g := NewWithT(t)
		in := fx.iks("1.37")
		in.Cluster.MasterHealth = "error"
		got, err := DecideMasterAction(in)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.UpToDate.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason))
	})
	t.Run("empty health right after ready is not unhealthy", func(t *testing.T) {
		g := NewWithT(t)
		in := fx.iks("1.37")
		in.Cluster.MasterHealth = ""
		got, err := DecideMasterAction(in)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.RequestUpdate).To(BeTrue())
	})
}

func TestDecideMasterActionValidation(t *testing.T) {
	fx := loadMasterFixtures(t)
	testCases := []struct {
		name   string
		mutate func(in *MasterVersionInput)
		want   string
	}{
		{name: "unknown distribution", mutate: func(in *MasterVersionInput) { in.Distribution = "" }, want: `MasterVersionInput.Distribution is ""`},
		{name: "no cluster", mutate: func(in *MasterVersionInput) { in.Cluster = nil }, want: "MasterVersionInput.Cluster is nil"},
		{name: "no clock", mutate: func(in *MasterVersionInput) { in.Now = time.Time{} }, want: "MasterVersionInput.Now is not set"},
		{name: "desired version with a patch", mutate: func(in *MasterVersionInput) { in.DesiredVersion = "1.37.1" },
			want: "MasterVersionInput.DesiredVersion (spec.distributionVersion): invalid minor version"},
		{name: "unparseable status.masterUpdate", mutate: func(in *MasterVersionInput) { in.MasterUpdate = masterRequested("v1.37", time.Hour, 1) },
			want: "MasterVersionInput.MasterUpdate.Version (status.masterUpdate.version)"},
		{name: "CAPIBM's own inputs are checked before IBM's masterKubeVersion", mutate: func(in *MasterVersionInput) {
			in.MasterUpdate = masterRequested("v1.37", time.Hour, 1)
			in.Cluster.MasterKubeVersion = "garbage"
		}, want: "MasterVersionInput.MasterUpdate.Version (status.masterUpdate.version)"},
		{name: "current condition of another type", mutate: func(in *MasterVersionInput) {
			in.CurrentUpToDate = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration)
			in.CurrentUpToDate.Type = controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition
		}, want: `MasterVersionInput.CurrentUpToDate is a "RollingOut" condition; pass the object's MasterVersionUpToDate condition, or nil when it has none`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := fx.iks("1.37")
			tc.mutate(&in)
			_, err := DecideMasterAction(in)
			g.Expect(err).To(MatchError(ContainSubstring(tc.want)))
		})
	}
}

// TestDecideMasterActionUnparsableMaster checks that a masterKubeVersion CAPIBM cannot parse (IBM's form during an
// update is not yet verified against IBM Cloud) makes MasterVersionUpToDate Unknown with the reason
// UnparsableMasterVersion, whatever IBM's state, status.masterUpdate or the current condition say. Nothing is requested
// or asked for, and RollingOut is left to the caller as it is (no Type).
func TestDecideMasterActionUnparsableMaster(t *testing.T) {
	fx := loadMasterFixtures(t)
	testCases := []struct {
		name   string
		master string
		// quoted is master as the message quotes it.
		quoted string
		mutate func(in *MasterVersionInput)
	}{
		{name: "a pending marker without its space", master: "1.37.1_1517(pending)", quoted: `"1.37.1_1517(pending)"`},
		{name: "no version at all", master: "", quoted: `""`},
		{name: "IBM shows an update of a version CAPIBM cannot parse", master: "garbage", quoted: `"garbage"`,
			mutate: func(in *MasterVersionInput) { in.Cluster.State, in.Cluster.TargetVersion = "updating", "1.37.1_1517" }},
		{name: "a request and a refusal on record", master: "v1.36.5", quoted: `"v1.36.5"`,
			mutate: func(in *MasterVersionInput) {
				in.MasterUpdate = masterRequested("1.37", 15*time.Minute, 1)
				in.CurrentUpToDate, in.Generation = masterCurrent(masterPermissionDeniedReason, masterDenied, masterGeneration), masterGeneration
			}},
		{name: "without the inputs that cost a call", master: "1.x", quoted: `"1.x"`,
			mutate: func(in *MasterVersionInput) { in.Offered, in.Workers, in.WorkersListed = nil, nil, false }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := fx.iks("1.37")
			in.Cluster.MasterKubeVersion = tc.master
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			got, err := DecideMasterAction(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(MasterAction{UpToDate: metav1.Condition{
				Type:   controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
				Status: metav1.ConditionUnknown,
				Reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUnparsableMasterVersionReason,
				Message: "IBM reports master version " + tc.quoted + " for IBM cluster iksclusterid00000001, which CAPIBM " +
					"cannot parse, so it cannot tell whether the master runs spec.distributionVersion 1.37; CAPIBM requests no " +
					"master update until IBM reports a version it can parse",
			}}), "no other field: no request, no input asked for, RollingOut left as it is")
		})
	}
}

// TestMasterCallDefinitive pins which classes of a failed call of the master version step are reported and looked at
// again at the next poll, and which ones their own handling retries; masterCallFailed and masterInputFailure both
// follow it.
func TestMasterCallDefinitive(t *testing.T) {
	g := NewWithT(t)
	for _, class := range []kubernetesservice.ErrorClass{kubernetesservice.ClassNotFound, kubernetesservice.ClassBadRequest,
		kubernetesservice.ClassPermissionDenied, kubernetesservice.ClassClusterNameExists,
		kubernetesservice.ClassInfrastructureKeyNotCreatable, kubernetesservice.ClassInfrastructureKeyDenied,
		kubernetesservice.ClassPoolNameExists, kubernetesservice.ClassOperationInProgress, kubernetesservice.ClassClusterMinimum,
		kubernetesservice.ClassAutoscaled, kubernetesservice.ClassMasterUpdateAlreadyRequested,
		kubernetesservice.ClassVersionSkipNotAllowed} {
		g.Expect(masterCallDefinitive(class)).To(BeTrue(), "class %q", class)
	}
	for _, class := range []kubernetesservice.ErrorClass{kubernetesservice.ClassNone, kubernetesservice.ClassUnauthorized,
		kubernetesservice.ClassRateLimited, kubernetesservice.ClassTransient, kubernetesservice.ClassUnknown} {
		g.Expect(masterCallDefinitive(class)).To(BeFalse(), "class %q", class)
	}
}

// TestMasterInputFailure checks the pure part of the report of a failed input call: after a raise of
// spec.distributionVersion, a definitive failure of the ListVersions or ListWorkers call of the master version step is
// MasterVersionUpToDate=False with PermissionDenied (403: the call, the Kubernetes Service roles and the identity, like
// the UpdateMaster 403) or IBMAPIError (other definitive answers), with IBM's code only; every other answer, a spec on
// the master's minor, and IBM's version-skip refusal of the request on record (which the refusal row keeps until the
// spec changes) write nothing, while a kept PermissionDenied of UpdateMaster is replaced.
func TestMasterInputFailure(t *testing.T) {
	fx := loadMasterFixtures(t)
	const (
		identity     = "the CAPIBM controller identity"
		versionsCall = "listing the versions IBM Cloud offers in region us-south"
		workersCall  = "listing the workers of IBM cluster iksclusterid00000001"
		retry        = "CAPIBM tries again at the next poll of the control plane"
		roles        = "need the Administrator platform role and the Manager service role of Kubernetes Service on the cluster's resource group"
		versionSkip  = controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason
		// skipRefused is the message MasterUpdateCallOutcome writes for IBM's version-skip refusal of 1.37.
		skipRefused = "IBM refused the master update to 1.37 as a version skip; set spec.distributionVersion to the master's " +
			"next minor; CAPIBM requests no update until the spec changes"
	)
	answer := func(op kubernetesservice.Operation, status int, code string) error {
		return &kubernetesservice.APIError{Operation: op, StatusCode: status, Code: code, IncidentID: "inc-0001",
			Description: "IBM's own words, which stay in the controller log"}
	}
	iks := func() MasterVersionInput { return fx.iks("1.37") }
	// withRecord returns in at masterGeneration with status.masterUpdate for version (requested 15 minutes before
	// masterNow, attempt 1; none for "") and a MasterVersionUpToDate=False with reason and message written for
	// generation.
	withRecord := func(in MasterVersionInput, version, reason, message string, generation int64) MasterVersionInput {
		if version != "" {
			in.MasterUpdate = masterRequested(version, 15*time.Minute, 1)
		}
		in.CurrentUpToDate, in.Generation = masterCurrent(reason, message, generation), masterGeneration
		return in
	}
	testCases := []struct {
		name string
		in   func() MasterVersionInput
		op   kubernetesservice.Operation
		call string
		err  error
		// wantReason is the condition's reason; "" when no condition is written.
		wantReason  string
		wantMessage string
	}{
		{name: "403 to ListVersions is PermissionDenied", in: iks, op: kubernetesservice.OpListVersions, call: versionsCall,
			err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"), wantReason: masterPermissionDeniedReason,
			wantMessage: masterListDenied},
		{name: "403 without a code to ListWorkers is PermissionDenied", in: iks, op: kubernetesservice.OpListWorkers,
			call: workersCall, err: answer(kubernetesservice.OpListWorkers, http.StatusForbidden, ""), wantReason: masterPermissionDeniedReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + workersCall + " with HTTP 403, so the master update to 1.37 waits: " +
				"the credentials of " + identity + " " + roles + "; " + retry},
		{name: "ROKS names the OpenShift minor", in: func() MasterVersionInput { return fx.roks("4.22") },
			op: kubernetesservice.OpListWorkers, call: workersCall, err: answer(kubernetesservice.OpListWorkers, http.StatusForbidden, "E3c1a"),
			wantReason: masterPermissionDeniedReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + workersCall + " with HTTP 403 (IBM code E3c1a), so the master " +
				"update to 4.22 waits: the credentials of " + identity + " " + roles + "; " + retry},
		{name: "404 to ListWorkers is IBMAPIError", in: iks, op: kubernetesservice.OpListWorkers, call: workersCall,
			err:        answer(kubernetesservice.OpListWorkers, http.StatusNotFound, "G0004"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + workersCall + " with HTTP 404 (IBM code G0004), so the master " +
				"update to 1.37 waits; " + retry + ", and the controller log has IBM's description"},
		{name: "400 to ListVersions is IBMAPIError", in: iks, op: kubernetesservice.OpListVersions, call: versionsCall,
			err:        answer(kubernetesservice.OpListVersions, http.StatusBadRequest, "E0001"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			wantMessage: "IBM Cloud Kubernetes Service refused " + versionsCall + " with HTTP 400 (IBM code E0001), so the master " +
				"update to 1.37 waits; " + retry + ", and the controller log has IBM's description"},
		{name: "a request kubernetesservice refused to send is IBMAPIError", in: iks, op: kubernetesservice.OpListWorkers,
			call: workersCall, err: fmt.Errorf("%w: ListWorkers: the cluster ID is empty", kubernetesservice.ErrInvalidRequest),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			wantMessage: "CAPIBM could not complete " + workersCall + ", so the master update to 1.37 waits; " + retry +
				", and the controller log names the cause"},
		{name: "a 401 is retried with a rebuilt authenticator", in: iks, op: kubernetesservice.OpListVersions, call: versionsCall,
			err: answer(kubernetesservice.OpListVersions, http.StatusUnauthorized, "")},
		{name: "credentials IAM rejected are retried with a rebuilt authenticator", in: iks, op: kubernetesservice.OpListVersions,
			call: versionsCall, err: kubernetesservice.ErrCredentialsRejected},
		{name: "a 429 is retried after Retry-After", in: iks, op: kubernetesservice.OpListWorkers, call: workersCall,
			err: answer(kubernetesservice.OpListWorkers, http.StatusTooManyRequests, "")},
		{name: "a 5xx is retried with backoff", in: iks, op: kubernetesservice.OpListWorkers, call: workersCall,
			err: answer(kubernetesservice.OpListWorkers, http.StatusServiceUnavailable, "")},
		{name: "a network error is retried with backoff", in: iks, op: kubernetesservice.OpListVersions, call: versionsCall,
			err: errors.New("connection reset by peer")},
		{name: "an unclassified answer is retried with backoff", in: iks, op: kubernetesservice.OpListWorkers, call: workersCall,
			err: answer(kubernetesservice.OpListWorkers, http.StatusConflict, "")},
		{name: "a spec on the master's minor keeps its condition", in: func() MasterVersionInput { return fx.iks("1.36") },
			op: kubernetesservice.OpListVersions, call: versionsCall, err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a")},
		{name: "a master version that does not parse is the decision's report", op: kubernetesservice.OpListVersions, call: versionsCall,
			err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"),
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.Cluster.MasterKubeVersion = "garbage"
				return in
			}},
		{name: "an input the decision rejects", op: kubernetesservice.OpListVersions, call: versionsCall,
			err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"),
			in:  func() MasterVersionInput { return fx.iks("1.37.1") }},
		{name: "IBM's version-skip refusal of the request on record stays until the spec changes",
			op: kubernetesservice.OpListVersions, call: versionsCall, err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"),
			in: func() MasterVersionInput {
				return withRecord(iks(), "1.37", versionSkip, skipRefused, masterGeneration)
			}},
		{name: "a ROKS version-skip refusal stays whatever the definitive answer", op: kubernetesservice.OpListWorkers,
			call: workersCall, err: answer(kubernetesservice.OpListWorkers, http.StatusNotFound, "G0004"),
			in: func() MasterVersionInput {
				return withRecord(fx.roks("4.22"), "4.22_openshift", versionSkip,
					"IBM refused the master update to 4.22_openshift as a version skip", masterGeneration)
			}},
		{name: "a version-skip refusal of an earlier generation is replaced", op: kubernetesservice.OpListVersions,
			call: versionsCall, err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"),
			in: func() MasterVersionInput {
				return withRecord(iks(), "1.37", versionSkip, skipRefused, masterGeneration-1)
			},
			wantReason: masterPermissionDeniedReason, wantMessage: masterListDenied},
		{name: "row 7's own version skip without a request for the desired minor is replaced",
			op: kubernetesservice.OpListVersions, call: versionsCall, err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"),
			in: func() MasterVersionInput {
				// The master moved from 1.35 to 1.36 outside CAPIBM within this generation; the request on record is older.
				return withRecord(iks(), "1.36", versionSkip, "spec.distributionVersion 1.37 is more than one minor above "+
					"the master's minor 1.35", masterGeneration)
			},
			wantReason: masterPermissionDeniedReason, wantMessage: masterListDenied},
		{name: "a kept PermissionDenied of UpdateMaster is replaced", op: kubernetesservice.OpListVersions,
			call: versionsCall, err: answer(kubernetesservice.OpListVersions, http.StatusForbidden, "E3c1a"),
			in: func() MasterVersionInput {
				return withRecord(iks(), "1.37", masterPermissionDeniedReason, masterDenied, masterGeneration)
			},
			wantReason: masterPermissionDeniedReason, wantMessage: masterListDenied},
	}
	identityDetail := regexp.MustCompile(`iam-Service[I]d-|IB[M]id-|KubeCert[#]|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+|([0-9a-f]{2}:){7,}`)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, ok := masterInputFailure(tc.in(), kubernetesservice.Classify(tc.op, tc.err), tc.call, tc.err, identity)
			if tc.wantReason == "" {
				g.Expect(ok).To(BeFalse())
				g.Expect(got).To(Equal(metav1.Condition{}))
				return
			}
			g.Expect(ok).To(BeTrue())
			g.Expect(got).To(Equal(masterCondition(tc.wantReason, tc.wantMessage)))
			g.Expect(identityDetail.MatchString(got.Message)).To(BeFalse(), got.Message)
			g.Expect(got.Message).NotTo(ContainSubstring("inc-0001"), "the incident ID goes to the controller log only")
			g.Expect(got.Message).NotTo(ContainSubstring("IBM's own words"), "IBM's description goes to the controller log only")
		})
	}

	t.Run("a new incident ID gives the same condition", func(t *testing.T) {
		g := NewWithT(t)
		first := &kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkers, StatusCode: http.StatusForbidden, Code: "E3c1a",
			IncidentID: "inc-0001"}
		second := *first
		second.IncidentID = "inc-0002"
		a, _ := masterInputFailure(fx.iks("1.37"), kubernetesservice.ClassPermissionDenied, workersCall, first, identity)
		b, _ := masterInputFailure(fx.iks("1.37"), kubernetesservice.ClassPermissionDenied, workersCall, &second, identity)
		g.Expect(b).To(Equal(a))
	})
}

// TestMasterUpdateDeniedPrefix pins the record of an UpdateMaster 403 that the refusal row reads: the start of the
// message MasterUpdateCallOutcome writes, which the PermissionDenied of a failed input call never starts with.
func TestMasterUpdateDeniedPrefix(t *testing.T) {
	g := NewWithT(t)
	forbidden := &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusForbidden, Code: "E3c1a"}
	for _, version := range []string{"1.37", "4.22_openshift"} {
		outcome, err := MasterUpdateCallOutcome(forbidden, masterRequested(version, 0, 1), "the CAPIBM controller identity")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(outcome.Message).To(HavePrefix(masterUpdateDeniedPrefix(version)))
	}
	g.Expect(masterUpdateDeniedPrefix("1.37")).To(Equal("IBM Cloud Kubernetes Service refused the master update to 1.37 with HTTP 403"))
	g.Expect(masterListDenied).NotTo(HavePrefix(masterUpdateDeniedPrefix("1.37")))
	g.Expect(masterDenied).NotTo(HavePrefix(masterUpdateDeniedPrefix("1.3")), "the version is followed by the status")
}

func TestMasterRetryDelay(t *testing.T) {
	testCases := []struct {
		attempts int32
		want     time.Duration
	}{
		{attempts: -1, want: time.Hour},
		{attempts: 0, want: time.Hour},
		{attempts: 1, want: time.Hour},
		{attempts: 2, want: 2 * time.Hour},
		{attempts: 3, want: 4 * time.Hour},
		{attempts: 5, want: 16 * time.Hour},
		{attempts: 6, want: 24 * time.Hour},
		{attempts: 100, want: 24 * time.Hour},
		{attempts: math.MaxInt32, want: 24 * time.Hour},
	}
	for _, tc := range testCases {
		NewWithT(t).Expect(masterRetryDelay(tc.attempts)).To(Equal(tc.want), "attempts %d", tc.attempts)
	}
}

func TestMasterMaxWorkerSkew(t *testing.T) {
	g := NewWithT(t)
	g.Expect(masterMaxWorkerSkew(controlplanev1.DistributionKubernetes)).To(Equal(2))
	g.Expect(masterMaxWorkerSkew(controlplanev1.DistributionOpenShift)).To(Equal(1))
	g.Expect(masterMaxWorkerSkew("Other")).To(Equal(1), "an unknown distribution gets the stricter skew")
}

func TestMasterUpdateCallOutcome(t *testing.T) {
	const identity = "the CAPIBM controller identity"
	forbidden := &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusForbidden,
		Code: "E3c1a", Description: "The master is more than two versions behind the targeted version.", IncidentID: "inc-0001"}
	// The request of the call: sent at masterNow for the third time, so a refusal is retried 4 hours later.
	request := masterRequested("1.37", 0, 3)
	testCases := []struct {
		name     string
		err      error
		class    kubernetesservice.ErrorClass
		handled  bool
		accepted bool
		terminal bool
		reason   string
		message  []string
	}{
		{name: "accepted", err: nil, class: kubernetesservice.ClassNone, handled: true, accepted: true,
			message: []string{"IBM accepted the master update to 1.37"}},
		{name: "409 already requested is upgrade pending", class: kubernetesservice.ClassMasterUpdateAlreadyRequested,
			err:     &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusConflict},
			handled: true, accepted: true, message: []string{"IBM already has the master update to 1.37"}},
		{name: "403 is PermissionDenied even with skip text", err: forbidden, class: kubernetesservice.ClassPermissionDenied,
			handled: true, terminal: true, reason: masterPermissionDeniedReason,
			message: []string{"refused the master update to 1.37 with HTTP 403 (IBM code E3c1a):",
				"the credentials of the CAPIBM controller identity need the Administrator platform role and the Manager service role of Kubernetes Service",
				"not IBM's refusal to skip versions; CAPIBM requests the update again at 2026-11-20T16:00:00Z"}},
		{name: "403 without code", class: kubernetesservice.ClassPermissionDenied, handled: true, terminal: true,
			reason:  masterPermissionDeniedReason,
			err:     &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusForbidden},
			message: []string{"with HTTP 403: the credentials of"}},
		{name: "401 is the caller's: invalidate the authenticator", class: kubernetesservice.ClassUnauthorized,
			err: &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusUnauthorized}},
		{name: "5xx is transient", class: kubernetesservice.ClassTransient,
			err: &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusBadGateway}},
		{name: "429 is rate limited", class: kubernetesservice.ClassRateLimited,
			err: &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusTooManyRequests}},
		{name: "network error is transient", class: kubernetesservice.ClassTransient, err: errors.New("connection reset")},
	}
	// A message names the credentials only by Identity.Description: no IAM ID, e-mail, key fingerprint or certificate
	// subject. The character classes keep the patterns themselves from matching a search of the source tree for such
	// identifiers.
	identityDetail := regexp.MustCompile(`iam-Service[I]d-|IB[M]id-|KubeCert[#]|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+|([0-9a-f]{2}:){7,}`)
	t.Run("the identity pattern finds synthetic identity details", func(t *testing.T) {
		g := NewWithT(t)
		for _, sample := range []string{"iam-" + "ServiceId-0000", "IBM" + "id-0000", "KubeCert" + "#<caller-iam-id>",
			"someone" + "@example.com", "ab:cd:ef:01:23:45:67:89"} {
			g.Expect(identityDetail.MatchString(sample)).To(BeTrue(), sample)
		}
	})
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := MasterUpdateCallOutcome(tc.err, request, identity)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Class).To(Equal(tc.class))
			g.Expect(got.Handled).To(Equal(tc.handled))
			g.Expect(got.Accepted).To(Equal(tc.accepted))
			g.Expect(got.Terminal).To(Equal(tc.terminal))
			g.Expect(got.Reason).To(Equal(tc.reason))
			for _, part := range tc.message {
				g.Expect(got.Message).To(ContainSubstring(part))
			}
			if !tc.handled {
				g.Expect(got.Message).To(BeEmpty())
			}
			g.Expect(identityDetail.MatchString(got.Message)).To(BeFalse(), got.Message)
			g.Expect(got.Message).NotTo(ContainSubstring("more than two versions"), "IBM's description text never reaches the message")
		})
	}

	t.Run("a recorded version-skip code is VersionSkipNotAllowed", func(t *testing.T) {
		g := NewWithT(t)
		got := masterUpdateOutcome(kubernetesservice.ClassVersionSkipNotAllowed, forbidden, masterRequested("4.22_openshift", 0, 1), identity)
		g.Expect(got.Handled).To(BeTrue())
		g.Expect(got.Terminal).To(BeTrue())
		g.Expect(got.Accepted).To(BeFalse())
		g.Expect(got.Reason).To(Equal(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason))
		g.Expect(got.Message).To(Equal("IBM refused the master update to 4.22_openshift as a version skip (IBM code E3c1a); " +
			"set spec.distributionVersion to the master's next minor; CAPIBM requests no update until the spec changes"))
	})

	t.Run("403 names the retry of the first attempt", func(t *testing.T) {
		g := NewWithT(t)
		got, err := MasterUpdateCallOutcome(forbidden, masterRequested("1.37", 15*time.Minute, 1), identity)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.Message).To(Equal("IBM Cloud Kubernetes Service refused the master update to 1.37 with HTTP 403 (IBM code " +
			"E3c1a): the credentials of the CAPIBM controller identity need the Administrator platform role and the Manager " +
			"service role of Kubernetes Service on the cluster's resource group. CAPIBM requests one minor at a time, so this " +
			"is not IBM's refusal to skip versions; CAPIBM requests the update again at 2026-11-20T12:45:00Z"))
	})

	t.Run("a new incident ID gives the same message", func(t *testing.T) {
		// IBM sends a new incident ID with every answer. The message names IBM's code only, so the same refusal gives
		// the same condition and writes no status; the incident ID goes to the controller log.
		g := NewWithT(t)
		request := masterRequested("1.37", 0, 1)
		for _, class := range []kubernetesservice.ErrorClass{kubernetesservice.ClassPermissionDenied, kubernetesservice.ClassVersionSkipNotAllowed} {
			first := *forbidden
			second := *forbidden
			second.IncidentID = "inc-0002"
			a := masterUpdateOutcome(class, &first, request, identity)
			b := masterUpdateOutcome(class, &second, request, identity)
			g.Expect(b.Message).To(Equal(a.Message), "class %s", class)
			g.Expect(a.Message).To(ContainSubstring("(IBM code E3c1a)"))
			g.Expect(a.Message).NotTo(ContainSubstring("inc-000"))
			g.Expect(a.Message).NotTo(ContainSubstring("incident"))
		}
	})

	t.Run("the IBM reference is the code only", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect(masterIBMReference(&kubernetesservice.APIError{StatusCode: http.StatusForbidden})).To(BeEmpty())
		g.Expect(masterIBMReference(errors.New("not an IBM answer"))).To(BeEmpty())
		g.Expect(masterIBMReference(&kubernetesservice.APIError{IncidentID: "inc-2"})).To(BeEmpty(), "never the incident ID")
		g.Expect(masterIBMReference(&kubernetesservice.APIError{Code: "E3c1a", IncidentID: "inc-2"})).To(Equal(" (IBM code E3c1a)"))
	})

	t.Run("validation", func(t *testing.T) {
		g := NewWithT(t)
		_, err := MasterUpdateCallOutcome(nil, controlplanev1.MasterUpdateRequest{LastRequestTime: metav1.NewTime(masterNow)}, identity)
		g.Expect(err).To(MatchError("MasterUpdateCallOutcome needs the status.masterUpdate written before the call " +
			"(MasterAction.MasterUpdate); its version is empty"))
		_, err = MasterUpdateCallOutcome(nil, controlplanev1.MasterUpdateRequest{Version: "1.37", Attempts: 1}, identity)
		g.Expect(err).To(MatchError(ContainSubstring("its lastRequestTime is not set")))
		_, err = MasterUpdateCallOutcome(nil, request, "")
		g.Expect(err).To(MatchError(ContainSubstring("needs the Description of the credentials.Identity")))
	})
}

// TestMasterUpdateRefusalIsKept follows an UpdateMaster 403 through the reconciles after it: the condition the caller
// writes from MasterUpdateCallOutcome is read back by DecideMasterAction and kept, without a call or a worker listing,
// until the retry time its message names; then the update is requested again.
func TestMasterUpdateRefusalIsKept(t *testing.T) {
	fx := loadMasterFixtures(t)
	g := NewWithT(t)
	forbidden := &kubernetesservice.APIError{Operation: kubernetesservice.OpUpdateMaster, StatusCode: http.StatusForbidden}

	// The reconcile that requests the update and gets the 403.
	in := fx.iks("1.37")
	in.Generation = masterGeneration
	requested, err := DecideMasterAction(in)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(requested.RequestUpdate).To(BeTrue())
	outcome, err := MasterUpdateCallOutcome(forbidden, requested.MasterUpdate, "the CAPIBM controller identity")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(outcome.Terminal).To(BeTrue())
	written := masterCurrent(outcome.Reason, outcome.Message, masterGeneration)

	for _, later := range []time.Duration{time.Minute, MasterUpdatePendingGrace, 15 * time.Minute, time.Hour - time.Second} {
		t.Run("UpdateMaster_403_kept_"+later.String()+"_later", func(t *testing.T) {
			g := NewWithT(t)
			next := fx.iks("1.37")
			next.Now, next.Generation = masterNow.Add(later), masterGeneration
			next.MasterUpdate, next.CurrentUpToDate = requested.MasterUpdate, written
			next.Workers, next.WorkersListed = nil, false
			got, err := DecideMasterAction(next)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.UpToDate).To(Equal(masterCondition(outcome.Reason, outcome.Message)))
			g.Expect(got.RollingOut.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(got.RequestUpdate).To(BeFalse())
			g.Expect(got.NeedsWorkers).To(BeFalse())
			g.Expect(got.RecheckAt).To(Equal(masterNow.Add(time.Hour)))
			g.Expect(outcome.Message).To(HaveSuffix("CAPIBM requests the update again at " + bindingTime(got.RecheckAt)))
		})
	}

	t.Run("UpdateMaster 403 retried at the named time", func(t *testing.T) {
		g := NewWithT(t)
		next := fx.iks("1.37")
		next.Now, next.Generation = masterNow.Add(time.Hour), masterGeneration
		next.MasterUpdate, next.CurrentUpToDate = requested.MasterUpdate, written
		got, err := DecideMasterAction(next)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.RequestUpdate).To(BeTrue())
		g.Expect(got.MasterUpdate.Attempts).To(Equal(int32(2)))
	})
}

// TestMasterMessagesNeverCarryThePendingMarker checks over every decision of a sweep of inputs that no condition of
// DecideMasterAction carries IBM's " (pending)" marker, whatever IBM reports.
func TestMasterMessagesNeverCarryThePendingMarker(t *testing.T) {
	fx := loadMasterFixtures(t)
	g := NewWithT(t)
	for _, master := range []string{"1.36.5_1537", "1.36.5_1537 (pending)", "1.37.1_1517 (pending)", "1.35.9_1530 (pending)"} {
		for _, target := range []string{"", "1.36.5_1537", "1.36.6_1540 (pending)", "1.37.1_1517 (pending)", "garbage"} {
			for _, desired := range []string{"1.35", "1.36", "1.37", "1.38"} {
				for _, state := range []string{"normal", "updating", "unsupported", "critical"} {
					in := fx.iks(desired)
					in.Cluster.MasterKubeVersion, in.Cluster.TargetVersion, in.Cluster.State = master, target, state
					in.MasterUpdate = masterRequested("1.37", 10*time.Minute, 1)
					got, err := DecideMasterAction(in)
					g.Expect(err).NotTo(HaveOccurred())
					text := got.UpToDate.Message + got.RollingOut.Message
					g.Expect(text).NotTo(ContainSubstring("(pending)"), "master %s target %s desired %s state %s", master, target, desired, state)
					g.Expect(strings.TrimSpace(got.UpToDate.Message)).NotTo(BeEmpty())
				}
			}
		}
	}
}

// masterNewerNote is availableNote's text for 1.37 and 1.38.
const masterNewerNote = "; update available: IBM offers 1.37, 1.38; raise spec.distributionVersion to update"

// TestDecideMasterActionAvailableNote checks that the newer minors IBM offers (MasterVersionInput.Available) are named
// only in a True MasterVersionUpToDate, after the end-of-support notes, and never turn it False or ask for a call.
func TestDecideMasterActionAvailableNote(t *testing.T) {
	fx := loadMasterFixtures(t)
	available := []string{"1.37", "1.38"}
	testCases := []struct {
		name string
		in   func() MasterVersionInput
		// wantStatus is the MasterVersionUpToDate status.
		wantStatus metav1.ConditionStatus
		// wantMessage is the whole message when set; wantNote is the note the message ends with, "" for none.
		wantMessage string
		wantNote    string
	}{
		{name: "the True message names the newer minors", wantStatus: metav1.ConditionTrue, wantNote: masterNewerNote,
			wantMessage: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches " +
				"itself" + masterNewerNote,
			in: func() MasterVersionInput { in := fx.iks("1.36"); in.Available = available; return in }},
		{name: "without newer minors the True message has no note", wantStatus: metav1.ConditionTrue,
			wantMessage: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches itself",
			in:          func() MasterVersionInput { in := fx.iks("1.36"); in.Available = []string{}; return in }},
		{name: "the note follows the end-of-support note", wantStatus: metav1.ConditionTrue, wantNote: masterNewerNote,
			wantMessage: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches " +
				"itself; IBM ends support for 1.36 on 2027-01-03, so plan the update to 1.37" + masterNewerNote,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.VersionEOS, in.Available = "2027-01-03T23:59:59+0000", available
				return in
			}},
		{name: "the note follows the unsupported note", wantStatus: metav1.ConditionTrue, wantNote: masterNewerNote,
			wantMessage: "the master runs 1.36.5_1537, minor 1.36 as spec.distributionVersion asks; IBM applies master patches " +
				"itself; IBM no longer supports 1.36: upgrade one minor at a time, starting with spec.distributionVersion 1.37" +
				masterNewerNote,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.State, in.Available = "unsupported", available
				return in
			}},
		{name: "an IBM patch in progress keeps the True with the note", wantStatus: metav1.ConditionTrue, wantNote: masterNewerNote,
			in: func() MasterVersionInput {
				in := fx.iks("1.36")
				in.Cluster.TargetVersion, in.Available = "1.36.6_1540", available
				return in
			}},
		{name: "ROKS names the OpenShift minors", wantStatus: metav1.ConditionTrue,
			wantNote: "; update available: IBM offers 4.22; raise spec.distributionVersion to update",
			wantMessage: "the master runs 4.21.35_1537_openshift, minor 4.21 as spec.distributionVersion asks; IBM applies master " +
				"patches itself; update available: IBM offers 4.22; raise spec.distributionVersion to update",
			in: func() MasterVersionInput { in := fx.roks("4.21"); in.Available = []string{"4.22"}; return in }},
		{name: "a raise is not up to date and names no note", wantStatus: metav1.ConditionFalse,
			in: func() MasterVersionInput {
				in := fx.iks("1.37")
				in.MasterUpdate, in.Available = masterRequested("1.37", time.Minute, 1), available
				return in
			}},
		{name: "a version skip names no note", wantStatus: metav1.ConditionFalse,
			in: func() MasterVersionInput { in := fx.iks("1.38"); in.Available = available; return in }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := DecideMasterAction(tc.in())
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.NeedsOfferedVersions || got.NeedsWorkers || got.RequestUpdate).To(BeFalse(), "the note asks for nothing")
			g.Expect(got.UpToDate.Status).To(Equal(tc.wantStatus), got.UpToDate.Message)
			if tc.wantMessage != "" {
				g.Expect(got.UpToDate.Message).To(Equal(tc.wantMessage))
			}
			if tc.wantNote != "" {
				g.Expect(got.UpToDate.Message).To(HaveSuffix(tc.wantNote))
			} else {
				g.Expect(got.UpToDate.Message).NotTo(ContainSubstring("update available"))
			}
		})
	}
}

func TestNewerOfferedMinors(t *testing.T) {
	fx := loadMasterFixtures(t)
	release := func(major, minor, patch int) kubernetesservice.ReleaseVersion {
		return kubernetesservice.ReleaseVersion{Major: major, Minor: minor, Patch: patch}
	}
	var many []kubernetesservice.ReleaseVersion
	var firstSixteen []string
	for minor := 56; minor >= 37; minor-- {
		many = append(many, release(1, minor, 0))
	}
	for minor := 37; minor < 37+maxAvailableDistributionVersions; minor++ {
		firstSixteen = append(firstSixteen, fmt.Sprintf("1.%d", minor))
	}
	testCases := []struct {
		name    string
		d       controlplanev1.Distribution
		master  ibmversion.Minor
		offered *kubernetesservice.Versions
		want    []string
	}{
		{name: "IKS on the live capture lists 1.37", d: controlplanev1.DistributionKubernetes, master: ibmversion.Minor{Major: 1, Minor: 36},
			offered: fx.offered, want: []string{"1.37"}},
		{name: "ROKS on the live capture lists 4.22", d: controlplanev1.DistributionOpenShift, master: ibmversion.Minor{Major: 4, Minor: 21},
			offered: fx.offered, want: []string{"4.22"}},
		{name: "an older master lists every newer minor of its distribution", d: controlplanev1.DistributionOpenShift,
			master: ibmversion.Minor{Major: 4, Minor: 18}, offered: fx.offered, want: []string{"4.19", "4.20", "4.21", "4.22"}},
		{name: "the other distribution's minors never count", d: controlplanev1.DistributionKubernetes,
			master: ibmversion.Minor{Major: 1, Minor: 36}, offered: &kubernetesservice.Versions{OpenShift: []kubernetesservice.ReleaseVersion{
				release(4, 22, 16)}}},
		{name: "minors are sorted and listed once", d: controlplanev1.DistributionKubernetes, master: ibmversion.Minor{Major: 1, Minor: 35},
			offered: &kubernetesservice.Versions{Kubernetes: []kubernetesservice.ReleaseVersion{release(1, 38, 0), release(1, 36, 5),
				release(1, 37, 1), release(1, 37, 2), release(1, 35, 9)}}, want: []string{"1.36", "1.37", "1.38"}},
		{name: "a master on the newest minor lists nothing", d: controlplanev1.DistributionKubernetes,
			master: ibmversion.Minor{Major: 1, Minor: 37}, offered: fx.offered},
		{name: "a master above every offered minor lists nothing", d: controlplanev1.DistributionKubernetes,
			master: ibmversion.Minor{Major: 1, Minor: 40}, offered: fx.offered},
		{name: "a new major counts as newer", d: controlplanev1.DistributionOpenShift, master: ibmversion.Minor{Major: 4, Minor: 22},
			offered: &kubernetesservice.Versions{OpenShift: []kubernetesservice.ReleaseVersion{release(5, 0, 1), release(4, 23, 0),
				release(4, 22, 16)}}, want: []string{"4.23", "5.0"}},
		{name: "no answer lists nothing", d: controlplanev1.DistributionKubernetes, master: ibmversion.Minor{Major: 1, Minor: 36}},
		{name: "the list stops at the API bound with the nearest minors", d: controlplanev1.DistributionKubernetes,
			master: ibmversion.Minor{Major: 1, Minor: 36}, offered: &kubernetesservice.Versions{Kubernetes: many}, want: firstSixteen},
		{name: "a minor longer than the API bound is left out", d: controlplanev1.DistributionKubernetes,
			master: ibmversion.Minor{Major: 1, Minor: 36}, offered: &kubernetesservice.Versions{Kubernetes: []kubernetesservice.ReleaseVersion{
				release(1, 123456789, 0), release(1, 37, 1)}}, want: []string{"1.37"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := newerOfferedMinors(tc.d, tc.master, tc.offered)
			g.Expect(got).To(Equal(tc.want))
			g.Expect(len(got)).To(BeNumerically("<=", maxAvailableDistributionVersions))
			for _, name := range got {
				g.Expect(len(name)).To(BeNumerically("<=", maxAvailableDistributionVersionLength))
				_, err := ibmversion.ParseMinor(name)
				g.Expect(err).NotTo(HaveOccurred(), "every entry is MAJOR.MINOR like spec.distributionVersion")
			}
		})
	}
}

func TestKeptNewerMinors(t *testing.T) {
	testCases := []struct {
		name   string
		kept   []string
		master ibmversion.Minor
		want   []string
	}{
		{name: "the master on the same minor keeps the list", kept: []string{"1.37", "1.38"},
			master: ibmversion.Minor{Major: 1, Minor: 36}, want: []string{"1.37", "1.38"}},
		{name: "a minor the master reached is dropped", kept: []string{"1.37", "1.38"},
			master: ibmversion.Minor{Major: 1, Minor: 37}, want: []string{"1.38"}},
		{name: "a master past every entry keeps nothing", kept: []string{"1.37", "1.38"},
			master: ibmversion.Minor{Major: 1, Minor: 38}},
		{name: "entries that do not parse are dropped and the rest sorted", kept: []string{"1.38", "garbage", "1.37.1_1517", "1.37"},
			master: ibmversion.Minor{Major: 1, Minor: 36}, want: []string{"1.37", "1.38"}},
		{name: "nothing kept keeps nothing", master: ibmversion.Minor{Major: 1, Minor: 36}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(keptNewerMinors(tc.kept, tc.master)).To(Equal(tc.want))
		})
	}
}

func TestAvailableNote(t *testing.T) {
	testCases := []struct {
		name      string
		available []string
		want      string
	}{
		{name: "none", want: ""},
		{name: "empty", available: []string{}, want: ""},
		{name: "one", available: []string{"4.22"}, want: "; update available: IBM offers 4.22; raise spec.distributionVersion to update"},
		{name: "two", available: []string{"1.37", "1.38"}, want: masterNewerNote},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(availableNote(tc.available)).To(Equal(tc.want))
		})
	}
}
