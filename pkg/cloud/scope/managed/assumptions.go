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
	"strings"
	"time"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file holds every fact about IBM's API that only a live probe can settle, in one place, so that a reviewer sees
// which IBM behaviour CAPIBM relies on and what changes when IBM behaves differently, together with the Cluster API
// parity values of the drain. Each declaration says where its value comes from, in one of three forms that
// TestAssumptionsCiteTheirSource checks:
//
//   - "Observed on IBM Cloud": confirmed by the live probes of 2026-11-16 against IKS 1.36 and ROKS 4.21 clusters on
//     VPC; the comment states what was observed;
//   - "Not yet verified against IBM Cloud": still open; the comment names the default and the fallback, and switching
//     to the fallback is a one-line change here plus its test;
//   - "CAPI parity": the value Cluster API v1.14.0 uses for the same step; the comment cites the line.
//
// The sections follow the flows that first use them. Lists are functions that return a fresh copy, so that no caller
// can change a fact for the whole process.

// Drain cadence: Cluster API parity, not live checks.

// DrainRetryInterval is how long a drain waits before it evicts again or looks again at the pods that are still
// terminating. CAPI parity: Cluster API v1.14.0 drains a Machine's Node at the same cadence
// (core/reconcilers/machine/machine_controller.go:73, drainRetryInterval).
const DrainRetryInterval = 20 * time.Second

// EvictionBudgetPerReconcile bounds the time one reconcile spends sending evictions (drain.Helper.EvictionBudget); the
// next reconcile continues where it stopped. CAPI parity: Cluster API v1.14.0 sends evictions for at most 10 seconds
// per drain call (core/reconcilers/machine/drain/drain.go:316, evictionTimeout).
const EvictionBudgetPerReconcile = 10 * time.Second

// DrainBlockedEventInterval is the shortest time between two DrainBlocked Warning events of one drain; the Draining
// condition names the blocking pods all the time. CAPI parity: Cluster API v1.14.0 emits no event while a
// PodDisruptionBudget blocks a drain, it only updates the Machine's conditions every drainRetryInterval
// (core/reconcilers/machine/machine_controller.go:930-948). CAPIBM adds the Warning so that a drain blocked for hours
// shows up in events, and bounds it to one per interval so that it does not flood them.
const DrainBlockedEventInterval = 30 * time.Minute

// Binding: which IBM cluster a control plane stands for (DecideBinding).

// createdDateLayout is the layout of IBM's createdDate. Observed on IBM Cloud: "2026-11-16T03:05:33+0000", a numeric
// zone without a colon, which time.RFC3339 does not accept.
const createdDateLayout = "2006-01-02T15:04:05-0700"

// CreatedDateLayouts returns the layouts that parse IBM's cluster createdDate, in the order ParseCreatedDate tries
// them: RFC 3339 first, in case IBM switches to it, then the layout IBM returned in the probes. Observed on IBM Cloud
// (see createdDateLayout). The read-after-create lag that the binding windows below allow for is not yet verified
// against IBM Cloud.
func CreatedDateLayouts() []string {
	return []string{time.RFC3339, createdDateLayout}
}

// ParseCreatedDate parses IBM's createdDate with CreatedDateLayouts. Observed on IBM Cloud (see createdDateLayout).
// The binding rules treat a cluster whose createdDate does not parse as one CAPIBM did not create (a name conflict),
// so the error names the value and the accepted forms.
//
// Parameters:
//   - createdDate: kubernetesservice.Cluster.CreatedDate as IBM returned it.
func ParseCreatedDate(createdDate string) (time.Time, error) {
	for _, layout := range CreatedDateLayouts() {
		if parsed, err := time.Parse(layout, createdDate); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("IBM createdDate %q has an unknown layout; known layouts are RFC 3339 "+
		"(2026-11-16T03:05:33Z) and %q (2026-11-16T03:05:33+0000)", createdDate, createdDateLayout)
}

// CreatedDateSkew is how much earlier than CAPIBM's first create attempt of unknown outcome IBM's createdDate may be
// for the cluster to still count as that attempt's (CAPIBM's own create whose response was lost): IBM's clock and the
// controller's may differ. Not yet verified against IBM Cloud: default 2 minutes; fallback: a wider window once the
// skew is measured.
const CreatedDateSkew = 2 * time.Minute

// CreatedDateSlack is how much later than CAPIBM's last create attempt of unknown outcome IBM's createdDate may be
// (the same window as CreatedDateSkew): IBM stamps the cluster after it accepted the request.
// Not yet verified against IBM Cloud: default 10 minutes; fallback: adjust once the lag is measured.
const CreatedDateSlack = 10 * time.Minute

// CreateGracePeriod is how long after a create attempt of unknown outcome a 404 still means "IBM does not list it yet"
// rather than "it does not exist", for a lookup by ID and by name alike. Not yet verified against IBM Cloud: default 5
// minutes; fallback: longer once the read-after-create lag is measured.
const CreateGracePeriod = 5 * time.Minute

// DeleteLookupGracePeriod replaces CreateGracePeriod while a control plane with a create intent is deleted and the
// lookup by name finds nothing: the finalizer stays this long, so that a cluster IBM lists late is still found and
// deleted. Not yet verified against IBM Cloud: default 30 minutes; fallback: longer.
const DeleteLookupGracePeriod = 30 * time.Minute

// AdoptByEndpointMatch lets DecideBinding adopt a same-named cluster whose master URL matches
// spec.controlPlaneEndpoint, still only after the ownership proof. Not yet verified against IBM Cloud: default false,
// because it is open whether a master URL is unique per cluster and stable over master updates (the two clusters of
// the live probes had distinct URLs); switch to true once both are confirmed.
const AdoptByEndpointMatch = false

// Create and observe (BuildCreateClusterRequest, IsMasterReady).

// InitialWorkerPoolName is the name of the worker pool CAPIBM embeds into createCluster: the IBMCloudManagedMachinePool
// with this poolName is the cluster's initial pool, and every pool, this one included, is then adopted by name.
// Observed on IBM Cloud: IBM honours a custom workerPool.name and names the pool "default" when none is sent, so
// CAPIBM's convention matches IBM's default.
const InitialWorkerPoolName = "default"

// ROKSWorkerOperatingSystem is the operatingSystem CAPIBM sends for new workers of either distribution: "" means the
// field is not sent and IBM picks its default. Observed on IBM Cloud: IBM picked UBUNTU_24_64 for IKS 1.36 and RHCOS
// for ROKS 4.21, the supported defaults, so no compat cluster create is needed for it.
const ROKSWorkerOperatingSystem = ""

// openShiftVersionSuffix marks a ROKS version in IBM requests ("4.21_openshift"). Observed on IBM Cloud: createCluster
// accepted "4.21_openshift".
const openShiftVersionSuffix = "_openshift"

// ibmMinorVersion returns the minor-only version string IBM accepts for a distribution: "1.36" for Kubernetes and
// "4.21_openshift" for OpenShift, or "" for an unknown distribution. Observed on IBM Cloud: createCluster accepted
// both forms.
//
// Parameters:
//   - d: the control plane's spec.distribution.
//   - minor: the minor to name.
func ibmMinorVersion(d controlplanev1.Distribution, minor ibmversion.Minor) string {
	switch d {
	case controlplanev1.DistributionKubernetes:
		return minor.String()
	case controlplanev1.DistributionOpenShift:
		return minor.String() + openShiftVersionSuffix
	default:
		return ""
	}
}

// CreateVersion returns the kubeVersion CAPIBM sends to createCluster: "1.36" for Kubernetes (IKS) and
// "4.21_openshift" for OpenShift (ROKS). Observed on IBM Cloud: IBM accepted both minor-only forms and created the
// newest patch of the minor, 1.36.5_1537 and 4.21.35_1537_openshift. An unknown distribution returns "", which
// kubernetesservice.CreateCluster rejects before sending anything.
//
// Parameters:
//   - d: the control plane's spec.distribution.
//   - minor: spec.distributionVersion, parsed with ibmversion.ParseMinor.
func CreateVersion(d controlplanev1.Distribution, minor ibmversion.Minor) string {
	return ibmMinorVersion(d, minor)
}

// MinWorkersPerZoneAtCreate returns the smallest number of workers per zone CAPIBM accepts for the initial worker pool
// of createCluster: 1 for Kubernetes and 2 for OpenShift. An unknown distribution gets the stricter 2.
// Not yet verified against IBM Cloud: IBM refused to take a ROKS cluster below 2 workers (E1437), but whether
// createCluster wants 2 per zone or 2 in total is open; default 2 per zone; fallback: a total of 2 across the zones.
//
// Parameters:
//   - d: the control plane's spec.distribution.
func MinWorkersPerZoneAtCreate(d controlplanev1.Distribution) int64 {
	if d == controlplanev1.DistributionKubernetes {
		return 1
	}
	return 2
}

// IsMasterReady reports whether the IBM master can be used: masterState "deployed", masterStatus starting with
// "Ready" and masterURL set. masterHealth is not part of it. Observed on IBM Cloud: the master was usable from state
// deployed, 16-25 minutes after the create; masterHealth stayed "" for 3-4 minutes after "Ready" on both flavors
// before it became normal. nil is not ready.
//
// Parameters:
//   - c: the cluster as GetCluster returned it.
func IsMasterReady(c *kubernetesservice.Cluster) bool {
	return c != nil &&
		c.MasterState == "deployed" &&
		strings.HasPrefix(strings.TrimSpace(c.MasterStatus), "Ready") &&
		c.MasterURL != ""
}

// ClearIntentErrorCodes returns the IBM error codes that prove a createCluster attempt created nothing, whatever the
// HTTP status: E0007 (the cluster name exists) and A0656 (IBM could not create the infrastructure API key). Such an
// answer clears the attempt from status.createIntent. Observed on IBM Cloud: IBM checks A0656 before it validates the
// request, and a refused create left nothing behind.
func ClearIntentErrorCodes() []string {
	return []string{"E0007", "A0656"}
}

// ClearIntentStatusCodes returns the HTTP statuses of a createCluster answer that prove nothing was created, so the
// attempt leaves status.createIntent. Not yet verified against IBM Cloud: a 400 with P4102 (unknown subnet) and one
// with E7278 (ROKS without a COS instance) left nothing behind, but whether every 4xx does is open; default
// {400, 401, 403, 422}; fallback: keep the intent for a code that turns out to leave a cluster behind.
func ClearIntentStatusCodes() []int {
	return []int{400, 401, 403, 422}
}

// Delete: the deletion of the IBM cluster.

// DeleteReissueAfter is how long after status.deletionRequestTime CAPIBM sends DELETE again while IBM still reports
// the cluster in a state other than deleting. Not yet verified against IBM Cloud: a DELETE was observed to get 204,
// then state deleting, then 404 after about 5 minutes on both flavors, but IBM's answer to a repeated DELETE is open;
// default 10 minutes; fallback: adjust once that answer is known.
const DeleteReissueAfter = 10 * time.Minute
