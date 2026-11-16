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
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/version"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"
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

// UnhealthyMasterHealth returns the masterHealth values that make the control plane unavailable (Available=False)
// and block master updates. Not yet verified against IBM Cloud: IBM documents masterHealth as error, normal,
// unavailable or unsupported; "" right after the master became Ready (observed on IBM Cloud) is not unhealthy, and
// unsupported means end of support, which MasterVersionUpToDate reports; fallback: adjust the list once a master
// update has been observed.
func UnhealthyMasterHealth() []string {
	return []string{"error", "unavailable"}
}

// IsMasterUnhealthy reports whether a cluster's masterHealth is in UnhealthyMasterHealth; nil is not unhealthy: the
// caller has no cluster to judge. Not yet verified against IBM Cloud (see UnhealthyMasterHealth).
func IsMasterUnhealthy(c *kubernetesservice.Cluster) bool {
	return c != nil && slices.Contains(UnhealthyMasterHealth(), c.MasterHealth)
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

// Kubeconfig, endpoint and versions.

// AdminKubeconfigFormat is the format CAPIBM requests IBM's admin kubeconfig in (kubernetesservice's
// GetAdminKubeconfig, getKubeconfig?admin=true&format=json). Observed on IBM Cloud: on both flavors the JSON's user
// holds client-certificate-data and client-key-data and no token, exec or auth-provider; on IKS it also carries the
// cluster CA. kubernetesservice sends this value; the constant records the decision with the other live facts.
const AdminKubeconfigFormat = "json"

// CATrustFor returns how the API server certificate of a distribution's public master is verified.
// Observed on IBM Cloud: IKS serves a certificate of the cluster CA, so the system roots fail and ClusterCA is used;
// ROKS's "-e" master serves a publicly trusted certificate that getCACert's CA does not validate, so SystemRoots is
// used. Private-only clusters are not yet verified against IBM Cloud; the caller therefore tries the other mode when
// this one fails and records the winner. An unknown distribution gets ClusterCA, the narrower trust.
//
// Parameters:
//   - d: the control plane's spec.distribution.
func CATrustFor(d controlplanev1.Distribution) managedkubeconfig.CATrust {
	if d == controlplanev1.DistributionOpenShift {
		return managedkubeconfig.SystemRoots
	}
	return managedkubeconfig.ClusterCA
}

// KubeconfigPolicy returns the refresh timings of the admin kubeconfig (managedkubeconfig.Decide).
// Observed on IBM Cloud: IBM's admin certificate is valid two years from its first fetch and IBM re-serves the same
// certificate (the same serial) on every fetch, so the credential is refreshed at half its lifetime (the kubeadm
// control plane's ratio, about one year), a refresh that got the stored certificate back is retried only after a day,
// and there is no fixed-age refresh. The other values bound requeues and backoffs: the expiry warning 30 days ahead
// (when IBM mints a new certificate near expiry is not yet verified against IBM Cloud), a CA re-check every 12 hours,
// a 1 minute poll while IBM rotates the CA, a 401 backoff from 1 to 10 minutes, and requeues between 1 minute and
// 12 hours.
func KubeconfigPolicy() managedkubeconfig.Policy {
	return managedkubeconfig.Policy{
		RefreshFraction:       0.5,
		SameCertificateRetry:  24 * time.Hour,
		ExpiryWarning:         30 * 24 * time.Hour,
		CARecheckInterval:     12 * time.Hour,
		RotationPollInterval:  time.Minute,
		AuthFailureBackoffMin: time.Minute,
		AuthFailureBackoffMax: 10 * time.Minute,
		MinRequeue:            time.Minute,
		MaxRequeue:            12 * time.Hour,
	}
}

// defaultMasterPort is the port of a master URL that names none. Observed on IBM Cloud: masterURL is https with an
// explicit port. IBM shows the port-443 control plane hostnames of ROKS 4.22 without a port
// (https://cloud.ibm.com/docs/openshift?topic=openshift-notice-cp-port-443); whether masterURL then names one is not
// yet verified against IBM Cloud.
const defaultMasterPort = "443"

// ManagementServerURL returns the API server URL CAPIBM uses for a cluster: IBM's masterURL, which is also the server
// of IBM's admin kubeconfig, normalised to https://<host>:<port> with the port always present. The port is 443 when IBM
// gives none, because Go's url.Port returns "" then and Cluster API copies only an endpoint with a non-zero port.
// Observed on IBM Cloud: masterURL equals the kubeconfig server on both flavors, with ports such as 30101 and 30102.
// Not yet verified against IBM Cloud: masterURL also for a private-only cluster; fallback: the private service
// endpoint URL.
//
// Parameters:
//   - d: the control plane's spec.distribution; both distributions use masterURL today, and the private-only check
//     may change that for one of them.
//   - c: the cluster as GetCluster returned it.
//
// It returns an error naming the URL and what is wrong when the distribution is unknown, c is nil, masterURL is empty
// or does not parse, is not https, carries user information, a path, a query or a fragment, or has a port outside
// 1-65535. The caller treats the error as terminal for the endpoint and writes nothing.
func ManagementServerURL(d controlplanev1.Distribution, c *kubernetesservice.Cluster) (string, error) {
	switch {
	case d != controlplanev1.DistributionKubernetes && d != controlplanev1.DistributionOpenShift:
		return "", fmt.Errorf("unknown distribution %q; want %q or %q", d, controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift)
	case c == nil:
		return "", errors.New("no IBM cluster to take the master URL from")
	case c.MasterURL == "":
		return "", fmt.Errorf("IBM reports no masterURL for cluster %s yet; it is set once the master is deployed", c.ID)
	}
	u, err := url.Parse(c.MasterURL)
	if err != nil {
		return "", fmt.Errorf("IBM masterURL %q of cluster %s is not a URL: %w", c.MasterURL, c.ID, err)
	}
	if problem := masterURLProblem(u); problem != "" {
		return "", fmt.Errorf("IBM masterURL %q of cluster %s %s", c.MasterURL, c.ID, problem)
	}
	port := u.Port()
	if port == "" {
		port = defaultMasterPort
	}
	return "https://" + net.JoinHostPort(u.Hostname(), port), nil
}

// masterURLProblem returns why a parsed master URL cannot be the API server URL, or "" when it can.
// Observed on IBM Cloud: IBM's masterURL is a plain https://<host>:<port>.
//
// Parameters:
//   - u: the parsed masterURL.
func masterURLProblem(u *url.URL) string {
	switch {
	case u.Scheme != "https":
		return fmt.Sprintf("uses scheme %q; CAPIBM connects to IBM masters only over https", u.Scheme)
	case u.User != nil:
		return "carries user information; a master URL names only a host and a port"
	case u.Hostname() == "":
		return "names no host"
	case u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "":
		return "carries a path, a query or a fragment; a master URL names only a host and a port"
	}
	if port := u.Port(); port != "" {
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return fmt.Sprintf("has port %q; want a port between 1 and 65535", port)
		}
	}
	return ""
}

// caRotationCompletePrefix starts IBM's caCertRotationStatus.status once a CA rotation has finished, as IBM's
// documentation words it. Not yet verified against IBM Cloud: no rotation has been observed.
const caRotationCompletePrefix = "CA certificate rotation complete"

// IsCARotationInProgress reports whether IBM reports a CA certificate rotation that has not completed.
// Not yet verified against IBM Cloud: the rotation status strings were not observed (a rotation is destructive), so
// the rule is fail-safe: every non-empty status that does not start with "CA certificate rotation complete" counts as
// in progress, and an unknown string blocks master updates and polls the kubeconfig faster instead of being ignored;
// fallback: poll faster.
//
// Parameters:
//   - status: caCertRotationStatus.status of the cluster (kubernetesservice.Cluster.CARotationStatus); "" when no
//     rotation ever ran.
func IsCARotationInProgress(status string) bool {
	status = strings.TrimSpace(status)
	return status != "" && !strings.HasPrefix(status, caRotationCompletePrefix)
}

// clusterStateUpdating is the cluster state while IBM updates the master, as IBM documents it.
// Not yet verified against IBM Cloud: the probes ran no master update.
const clusterStateUpdating = "updating"

// IsMasterUpdatePending reports whether IBM is updating the master or has an update queued: masterKubeVersion carries
// " (pending)", the state is "updating", or targetVersion names a newer version than masterKubeVersion (an IBM-driven
// patch included). Not yet verified against IBM Cloud: none of the three was observed yet (the probes ran no master
// update); fallback: a parser tweak for the marker IBM actually returns, and a longer MasterUpdatePendingGrace. It is
// fail-safe: a masterKubeVersion or targetVersion that does not parse counts as pending, because "pending" only delays
// worker removals and master update requests and keeps the reported Kubernetes version at the minimum. nil is not
// pending.
//
// Parameters:
//   - c: the cluster as GetCluster returned it.
func IsMasterUpdatePending(c *kubernetesservice.Cluster) bool {
	if c == nil {
		return false
	}
	if c.State == clusterStateUpdating {
		return true
	}
	master, err := ibmversion.Parse(c.MasterKubeVersion)
	if err != nil || master.Pending {
		return true
	}
	if c.TargetVersion == "" {
		return false
	}
	target, err := ibmversion.Parse(c.TargetVersion)
	if err != nil {
		return true
	}
	return isNewerIBMVersion(target, master)
}

// isNewerIBMVersion reports whether target is newer than current: a newer minor, or a newer fix pack of the same minor.
// When the fix packs cannot be compared (one is minor-only, or only one is an OpenShift version) it returns true, the
// fail-safe answer of IsMasterUpdatePending. Not yet verified against IBM Cloud: targetVersion was observed to equal
// masterKubeVersion when nothing is pending; how it moves during an update is open.
//
// Parameters:
//   - target: IBM's targetVersion.
//   - current: IBM's masterKubeVersion.
func isNewerIBMVersion(target, current ibmversion.Version) bool {
	if byMinor := target.MinorVersion().Compare(current.MinorVersion()); byMinor != 0 {
		return byMinor > 0
	}
	byFixPack, err := target.WithoutPending().CompareFixPack(current.WithoutPending())
	return err != nil || byFixPack > 0
}

// ReportedKubernetesVersion returns the Kubernetes version CAPIBM publishes in status.versions and status.version: the
// API server's gitVersion, except while the master is updated, when it is the lower of the published and the probed
// version. IBM updates its API server replicas one at a time, so one /version call can reach an old or a new replica,
// and the contract field is the minimum (control-plane.md:438-439). Not yet verified against IBM Cloud: whether
// /version alternates during an update is open; fallback: widen the rolling window.
//
// Parameters:
//   - current: the published version (status.versions[0].version); "" when none is published yet.
//   - probe: the gitVersion of this reconcile's successful probe, for example "v1.36.5+IKS" (IKS 1.36) or "v1.34.9"
//     (ROKS 4.21), as observed on IBM Cloud.
//   - rolling: IsMasterUpdatePending(c) or the IBM state "updating".
//
// An empty probe keeps current: a status field is never cleared by a failed read. While rolling, a version that does
// not parse keeps current, so status never flips between two strings it cannot order. Outside a rollout the probe
// wins even when it is lower, for example after IBM returned the master to the old patch.
func ReportedKubernetesVersion(current, probe string, rolling bool) string {
	if probe == "" {
		return current
	}
	if current == "" || !rolling {
		return probe
	}
	published, err := version.ParseSemantic(current)
	if err != nil {
		return current
	}
	probed, err := version.ParseSemantic(probe)
	if err != nil {
		return current
	}
	if probed.LessThan(published) {
		return probe
	}
	return current
}

// Ownership proof (DecideBinding).

// ProvesOwnership reports whether a probe result proves cluster-admin possession of a cluster, which DecideBinding
// requires before it binds a cluster that only the kubeconfig Secret or a matching endpoint names: the credential's
// SelfSubjectAccessReview for verb "*" on resource "*" in group "*" was allowed. The proof is
// credential-identity-agnostic: it never compares the certificate's subject or the SelfSubjectReview username with
// whoever CAPIBM currently acts as, because such a comparison would fail after a key rotation, on a clusterctl move
// target that runs with other credentials, and after any later change of identity.
// Not yet verified against IBM Cloud: whether IBM's admin certificate passes the access review; fallback: also
// require IBM's admin group ibm-admins in Groups (observed on IBM Cloud), still without any look at the username.
//
// Parameters:
//   - r: the result of workloadprobe.Prober.Probe for the candidate's kubeconfig, built from the Secret's user entry
//     with the candidate's masterURL and the flavor's trust, never with the Secret's server or CA.
func ProvesOwnership(r workloadprobe.Result) bool {
	return r.ClusterAdmin
}

// Pool service methods (kubernetesservice).

// CreatePoolWithZones records how kubernetesservice.CreateWorkerPool creates a pool: with its zones and subnets in one
// createWorkerPool call (zones[].subnetID). Not yet verified against IBM Cloud: whether that one call provisions the
// workers; fallback: a zone-less create plus one zone add per zone. The switch itself is
// kubernetesservice.CreatePoolWithZones, because the service package cannot import this one; this alias lists the
// decision point with the others and stops compiling if the switch is renamed or removed.
const CreatePoolWithZones = kubernetesservice.CreatePoolWithZones

// ListWorkerPoolsViaSDK records the source of kubernetesservice.ListWorkerPools: the SDK's GetWorkerPools1
// (GET /v2/getWorkerPools). Not yet verified against IBM Cloud: only the swagger documents that path's array shape on
// VPC clusters, while the live probes measured GET /v2/vpc/getWorkerPools; fallback: a compat decoder for
// /v2/vpc/getWorkerPools. As for CreatePoolWithZones, the switch is kubernetesservice.ListWorkerPoolsViaSDK.
const ListWorkerPoolsViaSDK = kubernetesservice.ListWorkerPoolsViaSDK

// Disruption guards and the drain (CheckDisruption, DecideDisruptionStep).

// ClusterMinimumWorkers returns IBM's minimum number of workers of a cluster: 1 for Kubernetes and 2 for OpenShift.
// CheckDisruption keeps the ready workers at or above it. An unknown distribution gets the higher value, 2.
// Observed on IBM Cloud: IBM refused a resize or a pool removal below these counts with E1437 on IKS and on ROKS.
//
// Parameters:
//   - d: the control plane's spec.distribution.
func ClusterMinimumWorkers(d controlplanev1.Distribution) int {
	if d == controlplanev1.DistributionKubernetes {
		return 1
	}
	return 2
}

// ClusterMinimumErrorCode is the IBM error code of a resize or pool removal below ClusterMinimumWorkers;
// kubernetesservice.Classify maps it to ClassClusterMinimum, and DisruptionCallOutcome matches it on every removal call,
// ReplaceWorker included. Observed on IBM Cloud: HTTP 400, synchronous and side-effect free, also for removing the
// only pool.
const ClusterMinimumErrorCode = "E1437"

// WorkerStateDeployed is the lifecycle.actualState of a running worker. Observed on IBM Cloud.
const WorkerStateDeployed = "deployed"

// WorkerStateDeleting is the lifecycle.actualState of a worker IBM is removing. Observed on IBM Cloud: the replaced
// worker was deleting while IBM deleted its Node, before the replacement provisioned.
const WorkerStateDeleting = "deleting"

// WorkerStateDeleted is the lifecycle.actualState of a worker IBM has removed; ListWorkers returns such records because
// it sends showDeleted=true. Not yet verified against IBM Cloud: whether and for how long IBM lists deleted records;
// fallback: drop a worker from providerIDList only once two listings poolTransitionPoll apart both omit it.
const WorkerStateDeleted = "deleted"

// ReadyWorkerHealthStates returns the health.state values of a ready worker. Not yet verified against IBM Cloud:
// every deployed worker of the live probes was "normal", but the full value set is open; fallback: extend the list.
func ReadyWorkerHealthStates() []string {
	return []string{"normal"}
}

// TransitionalWorkerStates returns the lifecycle.actualState values of a worker IBM is still changing; such a worker
// keeps the cluster unsettled (CheckDisruption rule 1). The failed states provision_failed and deploy_failed are not
// in it, so a broken worker cannot block every disruption. Not yet verified against IBM Cloud: the live probes saw
// provisioning, deploying and deleting, the others come from IBM's documentation
// (https://cloud.ibm.com/docs/containers?topic=containers-worker-node-state-reference); fallback: extend the list.
func TransitionalWorkerStates() []string {
	return []string{"provision_pending", "provisioning", "provisioned", "deploying", WorkerStateDeleting,
		"reload_pending", "reloading"}
}

// PendingOperationBlocks makes a worker with a non-empty lifecycle.pendingOperation count as transitional.
// Not yet verified against IBM Cloud: pendingOperation was "" on every steady worker of the live probes; whether IBM
// sets it during a replace is open; fallback: false, if IBM turns out to keep it set on steady workers.
const PendingOperationBlocks = true

// IsWorkerReady reports whether a worker counts as ready capacity: deployed, with a health in ReadyWorkerHealthStates.
// Not yet verified against IBM Cloud (see ReadyWorkerHealthStates; the live probes saw deployed and normal).
//
// Parameters:
//   - w: the worker as ListWorkers returned it.
func IsWorkerReady(w kubernetesservice.Worker) bool {
	return w.ActualState == WorkerStateDeployed && slices.Contains(ReadyWorkerHealthStates(), w.HealthState)
}

// IsWorkerTransitional reports whether IBM is still changing a worker: its actualState is in TransitionalWorkerStates,
// or, while PendingOperationBlocks is true, it has a pending operation. A deleted worker is never transitional.
// Not yet verified against IBM Cloud (see TransitionalWorkerStates and PendingOperationBlocks).
//
// Parameters:
//   - w: the worker as ListWorkers returned it.
func IsWorkerTransitional(w kubernetesservice.Worker) bool {
	if w.ActualState == WorkerStateDeleted {
		return false
	}
	return slices.Contains(TransitionalWorkerStates(), w.ActualState) || (PendingOperationBlocks && w.PendingOperation != "")
}

// DisruptionSettleDelay is how long after the newest lastDisruptionTime of the Cluster's pools no pool starts another
// worker removal (CheckDisruption rule 2): IBM's worker list shows a replacement or a resize only after a lag.
// Not yet verified against IBM Cloud: IBM listed the replacement of one IKS worker about 1.7 minutes after
// replaceWorker, but timings at scale are open; default 3 minutes; fallback: longer.
const DisruptionSettleDelay = 3 * time.Minute

// WorkerStuckAfter is how long a worker may stay transitional before CheckDisruption treats it like a failed worker (it
// stops blocking and the message names it), and how long after the IBM call a removal may take before
// DecideDisruptionStep reports WorkerOperationStuck. Not yet verified against IBM Cloud: one IKS replace took about
// 5.7 minutes; default 60 minutes; fallback: longer.
const WorkerStuckAfter = 60 * time.Minute

// ReplacementNotCreatedAfter is how long after the IBM call a replaced worker's zone may stay below its size before
// DecideDisruptionStep reports ReplacementNotCreated (auto-rebalance off, or no capacity in the zone).
// Not yet verified against IBM Cloud: IBM listed the replacement about 1.7 minutes after the call, for a Node IBM
// drained itself; the timing for a Node CAPIBM drained first is open; default 30 minutes; fallback: longer.
const ReplacementNotCreatedAfter = 30 * time.Minute

// ExcludeFromLoadBalancersDuringDrain makes the drain add node.kubernetes.io/exclude-from-external-load-balancers to
// every Node it cordons (drain.CordonOptions.ExcludeFromLoadBalancers), because a cordon alone does not take a Node out
// of cloud load balancer pools. Not yet verified against IBM Cloud: whether the IBM VPC load balancer honours the
// label, and how fast; fallback: false.
const ExcludeFromLoadBalancersDuringDrain = true

// Pool and worker states.

// ProviderID returns the Node spec.providerID of an IBM worker: ibm://<account>///<clusterID>/<workerID>.
// Observed on IBM Cloud: on IKS and on ROKS the derivation from getWorkers, with the account from the cluster CRN,
// equals Node.spec.providerID exactly. The account comes from the cluster CRN, never from process-wide credentials,
// so that per-cluster credentials would need no change here.
//
// Parameters:
//   - accountID: the account segment of the cluster CRN.
//   - clusterID: the IBM cluster ID.
//   - workerID: the worker ID from ListWorkers.
func ProviderID(accountID, clusterID, workerID string) string {
	return "ibm://" + accountID + "///" + clusterID + "/" + workerID
}

// AllowZeroReplicaPools allows MachinePool replicas 0, a pool without workers. Not yet verified against IBM Cloud:
// whether IBM accepts resizing a pool that is not the last one to 0; default false (ZeroReplicasNotSupported);
// fallback: true behind the cluster minimum guard.
const AllowZeroReplicaPools = false

// PoolNameReleasedStates returns the pool lifecycle.actualState values of a pool IBM is still removing: its name is not
// free yet (409 E0d39, reason PoolNameBeingReleased). Not yet verified against IBM Cloud: how long IBM lists a deleted
// pool and when it releases the name; default {deleting, deleted}; fallback: a longer wait.
func PoolNameReleasedStates() []string {
	return []string{"deleting", "deleted"}
}

// WorkerHasNewerFixPack reports whether IBM offers a worker another fix pack of its minor: kubeVersion.target is set
// and differs from kubeVersion.actual within the same minor, so a replace with update=true would install it
// (PatchAvailable). A worker without an actual version (still provisioning), with a version that does not parse, or
// with a target on another minor has none here: a minor behind the master is decided from the master's minor
// (MinorBehind). Not yet verified against IBM Cloud: the live probes showed kubeVersion{desired, actual, target} with
// target equal to actual on steady workers, and worker fix packs independent of the master's; how target moves when
// IBM publishes a fix pack is open; fallback: compare actual with the newest fix pack of getVersions.
//
// Parameters:
//   - w: the worker as ListWorkers returned it.
func WorkerHasNewerFixPack(w kubernetesservice.Worker) bool {
	if w.KubeVersionActual == "" || w.KubeVersionTarget == "" {
		return false
	}
	actual, err := ibmversion.Parse(w.KubeVersionActual)
	if err != nil {
		return false
	}
	target, err := ibmversion.Parse(w.KubeVersionTarget)
	if err != nil || target.MinorVersion() != actual.MinorVersion() {
		return false
	}
	order, err := target.CompareFixPack(actual)
	return err == nil && order != 0
}

// PoolOperation is a change to an IBM worker pool that the cluster state may forbid (PoolMutationAllowed).
// Not yet verified against IBM Cloud (see PoolMutationAllowed).
type PoolOperation string

const (
	// PoolOperationCreate creates a pool. Not yet verified against IBM Cloud.
	PoolOperationCreate PoolOperation = "Create"
	// PoolOperationGrow resizes a pool up. Not yet verified against IBM Cloud.
	PoolOperationGrow PoolOperation = "Grow"
	// PoolOperationShrink resizes a pool down. Not yet verified against IBM Cloud.
	PoolOperationShrink PoolOperation = "Shrink"
	// PoolOperationReplace replaces a worker. Not yet verified against IBM Cloud.
	PoolOperationReplace PoolOperation = "Replace"
	// PoolOperationRemove removes a pool. Not yet verified against IBM Cloud.
	PoolOperationRemove PoolOperation = "Remove"
)

// The cluster states that limit pool mutations and master updates. IBM documents them
// (https://cloud.ibm.com/docs/containers?topic=containers-cluster-states-reference); none was observed in the live
// probes.
const (
	// clusterStateUnavailable blocks every pool mutation. Not yet verified against IBM Cloud.
	clusterStateUnavailable = "unavailable"
	// clusterStateScaledDown blocks every pool mutation. Not yet verified against IBM Cloud.
	clusterStateScaledDown = "scaled_down"
	// clusterStateUnsupported blocks adding and reloading workers. Not yet verified against IBM Cloud.
	clusterStateUnsupported = "unsupported"
)

// PoolMutationAllowed reports whether CAPIBM may send a pool operation in a cluster state: no operation in unavailable
// or scaled_down; in unsupported no create, grow or replace (IBM: "you can't add or reload worker nodes"), while
// shrink and remove stay allowed so a user can still reduce cost; every other state allows everything. An unknown
// operation is never allowed. Without this gate IBM's refusal would be classified as an invalid configuration.
// Not yet verified against IBM Cloud: the update and failure states; fallback: adjust the table.
//
// Parameters:
//   - state: the IBM cluster state (IBMCloudManagedControlPlane status.state).
//   - op: the operation.
func PoolMutationAllowed(state string, op PoolOperation) bool {
	switch op {
	case PoolOperationCreate, PoolOperationGrow, PoolOperationShrink, PoolOperationReplace, PoolOperationRemove:
	default:
		return false
	}
	switch state {
	case clusterStateUnavailable, clusterStateScaledDown:
		return false
	case clusterStateUnsupported:
		return op == PoolOperationShrink || op == PoolOperationRemove
	default:
		return true
	}
}

// Scale-down.

// ScaleDownStrategy is how a pool shrinks. Not yet verified against IBM Cloud (see ScaleDownMode).
type ScaleDownStrategy string

const (
	// ScaleDownPredictThenResize predicts the workers IBM removes, drains them, resizes, and verifies the prediction.
	// Not yet verified against IBM Cloud (see ScaleDownMode).
	ScaleDownPredictThenResize ScaleDownStrategy = "PredictThenResize"
	// ScaleDownResizeOnly resizes without draining first; the book warns that PodDisruptionBudgets are then not
	// honoured. Not yet verified against IBM Cloud (see ScaleDownMode).
	ScaleDownResizeOnly ScaleDownStrategy = "ResizeOnly"
)

// ScaleDownMode is the strategy the pool controller uses. Not yet verified against IBM Cloud: whether
// resizeWorkerPool removes exactly the predicted victims, and whether a cordon changes IBM's ranking; default
// ScaleDownPredictThenResize; fallback: ScaleDownResizeOnly.
const ScaleDownMode = ScaleDownPredictThenResize

// Master minor update.

// MasterUpdateVersion returns the version CAPIBM sends to updateMaster for the next minor: "1.37" for Kubernetes and
// "4.22_openshift" for OpenShift. Not yet verified against IBM Cloud: whether updateMaster accepts a minor-only
// version; fallback: the newest patch of that minor from getVersions. An unknown distribution returns "", which
// kubernetesservice.UpdateMaster rejects before sending anything.
//
// Parameters:
//   - d: the control plane's spec.distribution.
//   - minor: the minor to update to (the observed minor's Next()).
func MasterUpdateVersion(d controlplanev1.Distribution, minor ibmversion.Minor) string {
	return ibmMinorVersion(d, minor)
}

// MasterUpdatePendingGrace is how long after an updateMaster request CAPIBM sends no second one while IBM does not
// show the update pending yet (UpgradePending). Not yet verified against IBM Cloud: the lag before IBM shows
// " (pending)"; default 5 minutes; fallback: longer.
const MasterUpdatePendingGrace = 5 * time.Minute

// MasterUpdateRetryAfter is the first wait before CAPIBM requests a master update again after IBM returned the master
// to the old minor; it doubles with every attempt up to MasterUpdateRetryMax (UpgradeFailed).
// Not yet verified against IBM Cloud: the state after a failed update; default 1 hour; fallback: adjust.
const MasterUpdateRetryAfter = time.Hour

// MasterUpdateRetryMax caps the doubling of MasterUpdateRetryAfter. Not yet verified against IBM Cloud: default
// 24 hours; fallback: adjust.
const MasterUpdateRetryMax = 24 * time.Hour

// MasterUpdateBlockedStates returns the cluster states in which CAPIBM sends no updateMaster (BlockedByClusterState).
// unsupported and update_canceled are not in it: in both IBM says the master must be updated.
// Not yet verified against IBM Cloud: the update and failure states; fallback: adjust the list.
func MasterUpdateBlockedStates() []string {
	return []string{"requested", "deploying", clusterStateUpdating, "update_failed", clusterStateScaledDown,
		clusterStateUnavailable, "critical", "deleting", "deleted", "aborted", "delete_failed", "deploy_failed",
		"create_failed"}
}
