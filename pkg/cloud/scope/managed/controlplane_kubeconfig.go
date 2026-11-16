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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/secret"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"
)

// This file is the access part of the control plane's reconcile: once the IBM cluster exists and its master is ready,
// the control plane records spec.controlPlaneEndpoint once, keeps the <cluster>-kubeconfig Secret verified,
// initializes, publishes the Kubernetes version the API server serves, and reports Available. The refresh decisions are
// pure (managedkubeconfig.Decide) and the kubeconfig is built from parts (managedkubeconfig.Build); this file gathers
// their inputs, makes the IBM calls and the probes they ask for, and writes the results.
//
// Trust: everything in the Secret, annotations included, can be written by anyone in the namespace. CAPIBM writes a
// credential only for a bound cluster (DecideBinding Known), only into a Secret it owns, and only after the new
// kubeconfig passed a probe (verify before write); a forged annotation only shifts a timer within the bounds of
// KubeconfigPolicy. A user-provided Secret is never modified.
//
// Secrets: condition messages, events and annotations carry no secret material and no API-server text. They name a
// failed probe by its step and class only (probeFailureText), never by workloadprobe.Result.Username, which names the
// IAM identity behind IBM's admin certificate; the identity CAPIBM acts as is named only by its
// credentials.Identity.Description.

// The annotations CAPIBM writes on a <cluster>-kubeconfig Secret it owns. They hold no secret material;
// controlplanev1.KubeconfigClusterIDAnnotation is the first of them. None of them records which IAM identity fetched
// the credential: the refresh does not depend on it, and a later release that lets Clusters use different credentials
// can add such a record without a migration.
const (
	// KubeconfigAccountIDAnnotation is the IBM Cloud account of the cluster, read from the cluster CRN, never from the
	// credentials CAPIBM runs with; the IBMClusterNotVisible message names it.
	KubeconfigAccountIDAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "account-id"
	// KubeconfigServerAnnotation is the API server URL of the stored kubeconfig; it follows IBM's master URL.
	KubeconfigServerAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "server"
	// KubeconfigCredentialTypeAnnotation names the credential type; always KubeconfigCredentialTypeClientCertificate.
	KubeconfigCredentialTypeAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "credential-type"
	// KubeconfigCredentialNotBeforeAnnotation is the client certificate's notBefore (RFC 3339).
	KubeconfigCredentialNotBeforeAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "credential-not-before"
	// KubeconfigCredentialNotAfterAnnotation is the client certificate's notAfter (RFC 3339).
	KubeconfigCredentialNotAfterAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "credential-not-after"
	// KubeconfigCredentialFingerprintAnnotation is the client certificate's SHA-256 fingerprint ("sha256:<hex>"), which
	// matches the Kubernetes audit credential ID.
	KubeconfigCredentialFingerprintAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "credential-fingerprint"
	// KubeconfigFetchedAtAnnotation is when CAPIBM fetched the stored credential from IBM (RFC 3339).
	KubeconfigFetchedAtAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "fetched-at"
	// KubeconfigRefreshCheckedAtAnnotation is when the latest refresh attempt left the stored credential in place: IBM
	// returned the same certificate (it returns the admin certificate it minted on every fetch, observed on IBM Cloud), or
	// the new one failed verification. managedkubeconfig.Decide's backoff waits from it.
	KubeconfigRefreshCheckedAtAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "refresh-checked-at"
	// KubeconfigCAModeAnnotation is the trust mode that verified the stored kubeconfig (managedkubeconfig.CATrust).
	KubeconfigCAModeAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "ca-mode"
	// KubeconfigCAFingerprintAnnotation is the embedded CA bundle's fingerprint; only with ClusterCA trust.
	KubeconfigCAFingerprintAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "ca-fingerprint"
	// KubeconfigCANotAfterAnnotation is the embedded CA bundle's earliest notAfter (RFC 3339); only with ClusterCA trust.
	KubeconfigCANotAfterAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "ca-not-after"
	// KubeconfigCAFetchedAtAnnotation is when the embedded CA was last fetched or confirmed (RFC 3339); only with
	// ClusterCA trust.
	KubeconfigCAFetchedAtAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "ca-fetched-at"
	// KubeconfigCARotationMarkerAnnotation is IBM's CA rotation marker the stored kubeconfig was refreshed for
	// (managedkubeconfig.CARotationMarker); absent while IBM reports no rotation.
	KubeconfigCARotationMarkerAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "ca-rotation-marker"
	// KubeconfigUnauthorizedSinceAnnotation is the start of the current run of 401 answers to the stored credential
	// (RFC 3339): set on the first 401 of a run and removed on the next successful probe, so that the refresh backoff
	// survives a controller restart without an API field. It is never derived from the KubeconfigReady lastTransitionTime,
	// which other False reasons also move. A forged or unparseable value only shifts the backoff within
	// [MinRequeue, AuthFailureBackoffMax] of KubeconfigPolicy. Only on CAPIBM-owned Secrets.
	KubeconfigUnauthorizedSinceAnnotation = controlplanev1.KubeconfigAnnotationPrefix + "kubeconfig-unauthorized-since"
)

// KubeconfigCredentialTypeClientCertificate is the value of KubeconfigCredentialTypeAnnotation: IBM's admin client
// certificate, the only credential CAPIBM writes. It is the one static credential IBM issues for both IKS and ROKS;
// CAPIBM never uses an OpenShift OAuth token instead.
const KubeconfigCredentialTypeClientCertificate = "ClientCertificate"

// kubeconfigOwnedAnnotations are every annotation CAPIBM manages on a Secret it owns. A write replaces exactly these
// keys and leaves every other annotation as it is.
func kubeconfigOwnedAnnotations() []string {
	return []string{
		controlplanev1.KubeconfigClusterIDAnnotation, KubeconfigAccountIDAnnotation, KubeconfigServerAnnotation,
		KubeconfigCredentialTypeAnnotation, KubeconfigCredentialNotBeforeAnnotation, KubeconfigCredentialNotAfterAnnotation,
		KubeconfigCredentialFingerprintAnnotation, KubeconfigFetchedAtAnnotation, KubeconfigRefreshCheckedAtAnnotation,
		KubeconfigCAModeAnnotation, KubeconfigCAFingerprintAnnotation, KubeconfigCANotAfterAnnotation,
		KubeconfigCAFetchedAtAnnotation, KubeconfigCARotationMarkerAnnotation, KubeconfigUnauthorizedSinceAnnotation,
	}
}

// EventReasonAdminCertificateExpiring is the Warning event of an admin certificate within ExpiryWarning of its notAfter
// while IBM still serves it. When IBM mints a new one on its own is not yet verified against IBM Cloud; a CA rotation
// re-issues it.
const EventReasonAdminCertificateExpiring = "AdminCertificateExpiring"

// controlPlaneKind is the kind of the control plane, for the kubeconfig Secret's controller owner reference.
const controlPlaneKind = "IBMCloudManagedControlPlane"

// EndpointFromServerURL returns spec.controlPlaneEndpoint for an API server URL that ManagementServerURL selected: its
// host and its port. Cluster API copies only an endpoint with a host and a non-zero port, so a URL without one of them
// is an error.
//
// Parameters:
//   - server: an https URL with a host and an explicit port, as ManagementServerURL returns it.
//
// It returns an error naming the URL when it does not parse or lacks the host or a port between 1 and 65535.
func EndpointFromServerURL(server string) (controlplanev1.APIEndpoint, error) {
	u, err := url.Parse(server)
	if err != nil {
		return controlplanev1.APIEndpoint{}, fmt.Errorf("API server URL %q does not parse: %w", server, err)
	}
	port, err := strconv.ParseInt(u.Port(), 10, 32)
	if u.Hostname() == "" || err != nil || port < 1 || port > 65535 {
		return controlplanev1.APIEndpoint{}, fmt.Errorf("API server URL %q needs a host and a port between 1 and 65535", server)
	}
	return controlplanev1.APIEndpoint{Host: u.Hostname(), Port: int32(port)}, nil
}

// KubeconfigReadyNote is a note of a True KubeconfigReady condition. Its values start the message parts, so that an
// operator (and the tests) can tell them apart.
type KubeconfigReadyNote string

const (
	// NoteEndpointChanged says that IBM serves the API server at another URL than spec.controlPlaneEndpoint; the
	// kubeconfig follows IBM.
	NoteEndpointChanged KubeconfigReadyNote = "EndpointChanged"
	// NoteAdminCertificateExpiring says that the admin certificate is within ExpiryWarning of its notAfter.
	NoteAdminCertificateExpiring KubeconfigReadyNote = "AdminCertificateExpiring"
)

// controlPlaneAccess is what the kubeconfig step learned in one reconcile; the initialization, version and Available
// steps read it.
type controlPlaneAccess struct {
	// verified is true when the Secret in place passed a probe in this reconcile, as it was stored or as CAPIBM has just
	// written it.
	verified bool
	// probe is that passing probe's result; its GitVersion feeds status.versions.
	probe workloadprobe.Result
	// failure is the last probe failure when verified is false; nil when no probe ran.
	failure error
	// clusterIDRecorded is true when the Secret in place may initialize the control plane: a CAPIBM-owned Secret carrying
	// cluster-id == status.clusterID, or a user-provided Secret.
	clusterIDRecorded bool
	// retry is the outcome of a refresh attempt that failed in this reconcile (afterFailedRefresh); the zero value when
	// none failed, or when Decide's backoff after the recorded attempt spaces the next one. It replaces the step's Done,
	// so that the failure's error class (kubernetesservice.Classify), not Decide's MinRequeue, brings the next reconcile.
	retry Outcome
}

// reconcileAccess runs the access steps for a bound IBM cluster that exists (class Exists): the endpoint, the
// kubeconfig Secret, initialization, the reported Kubernetes version and Available.
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is the cluster GetCluster returned.
func (s *ControlPlaneScope) reconcileAccess(ctx context.Context, c *kubernetesservice.Cluster) Outcome {
	if !IsMasterReady(c) {
		s.waitForMasterKubeconfig(c)
		return s.reportMaster(c)
	}
	server, err := ManagementServerURL(s.ControlPlane.Spec.Distribution, c)
	if err != nil {
		// A master URL CAPIBM cannot use (for example one that is not https) writes neither the endpoint nor a kubeconfig.
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			fmt.Sprintf("CAPIBM cannot use the master URL that IBM reports: %v; it writes no endpoint and no kubeconfig", err))
	}
	if s.endpointAsRead == (controlplanev1.APIEndpoint{}) {
		return s.recordEndpoint(c, server)
	}
	access, outcome, ok := s.reconcileKubeconfig(ctx, c, server)
	if !ok {
		return outcome
	}
	s.initialize(access)
	s.reportVersions(c, access)
	available := s.reportAvailable(c, access)
	if access.retry.Kind != "" {
		// A refresh attempt failed: its error class sets the next reconcile. The steps above still ran, because the stored
		// Secret may be verified and the cluster available while a refresh fails.
		return access.retry
	}
	return available
}

// recordEndpoint writes spec.controlPlaneEndpoint from the selected server URL in the reconcile that first sees the
// master ready, and returns: the deferred patch persists it, and the update event of that spec change brings the next
// reconcile at once. Returning here keeps initialization out of this reconcile, so controlPlaneInitialized is never
// persisted before the endpoint (initialize reads the endpoint as read).
//
// Parameters:
//   - c is the cluster.
//   - server is ManagementServerURL's answer.
func (s *ControlPlaneScope) recordEndpoint(c *kubernetesservice.Cluster, server string) Outcome {
	endpoint, err := EndpointFromServerURL(server)
	if err != nil {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			fmt.Sprintf("CAPIBM cannot use the master URL that IBM reports for cluster %s: %v", c.ID, err))
	}
	s.ControlPlane.Spec.ControlPlaneEndpoint = endpoint
	return s.reportMaster(c)
}

// waitForMasterKubeconfig reports that the kubeconfig waits for the master (WaitingForMaster), unless a verified Secret
// is already reported: a master that is briefly not ready does not invalidate a working Secret. c is the cluster.
func (s *ControlPlaneScope) waitForMasterKubeconfig(c *kubernetesservice.Cluster) {
	if conditions.IsTrue(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition) {
		return
	}
	s.setKubeconfigReady(metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyWaitingForMasterReason,
		fmt.Sprintf("The master of IBM cluster %s is not ready yet; CAPIBM writes the kubeconfig Secret once it is", c.ID))
}

// reconcileKubeconfig reads the Cluster's <cluster>-kubeconfig Secret and reconciles it as a CAPIBM-owned or a
// user-provided Secret. ok is false when outcome ends the reconcile (a failed read or write).
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is the bound cluster, master ready.
//   - server is ManagementServerURL's answer.
func (s *ControlPlaneScope) reconcileKubeconfig(ctx context.Context, c *kubernetesservice.Cluster, server string) (controlPlaneAccess, Outcome, bool) {
	stored, err := s.loadKubeconfigSecret(ctx)
	if err != nil {
		return controlPlaneAccess{}, s.kubeconfigTransient(err), false
	}
	if stored != nil && !kubeconfigSecretOwned(stored, s.ControlPlane) {
		return s.reconcileUserProvidedKubeconfig(ctx, stored), Outcome{}, true
	}
	return s.reconcileOwnedKubeconfig(ctx, c, server, stored)
}

// kubeconfigTransient reports a failed read or write of the kubeconfig Secret, which a retry may fix; the condition
// keeps CAPIBM's text and the controller log gets the error with the outcome. err is the failure.
func (s *ControlPlaneScope) kubeconfigTransient(err error) Outcome {
	if !conditions.IsTrue(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition) {
		s.setKubeconfigReady(metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason,
			fmt.Sprintf("CAPIBM could not read or write Secret %s; it retries, and the controller log names the cause", s.kubeconfigSecretName()))
	}
	return Transient(err)
}

// reconcileUserProvidedKubeconfig respects a Secret CAPIBM does not own: it never modifies it, accepts it only with the
// cluster-name label and a non-empty value key (CAPI's ClusterCache reads kubeconfigs only through a cache restricted
// to that label), and probes it; initialization needs a passing probe.
//
// Parameters:
//   - ctx is the reconcile context.
//   - stored is the Secret.
func (s *ControlPlaneScope) reconcileUserProvidedKubeconfig(ctx context.Context, stored *corev1.Secret) controlPlaneAccess {
	invalid := controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretInvalidReason
	if problem := UserProvidedSecretProblem(stored); problem != "" {
		s.setKubeconfigReady(metav1.ConditionFalse, invalid, problem)
		return controlPlaneAccess{}
	}
	result, err := s.params.Prober.Probe(ctx, stored.Data[secret.KubeconfigDataName])
	if err != nil {
		logProbeFailure(ctx, "user-provided kubeconfig", err)
		reason := kubeconfigProbeReason(err)
		if workloadprobe.ReasonOf(err) == workloadprobe.FailureInvalidKubeconfig {
			reason = invalid
		}
		s.setKubeconfigReady(metav1.ConditionFalse, reason, fmt.Sprintf("The user-provided Secret %s did not pass the probe: %s; "+
			"CAPIBM never modifies it, so put a working kubeconfig of the cluster into it", stored.Name, probeFailureText(err)))
		return controlPlaneAccess{failure: err, clusterIDRecorded: true}
	}
	s.setKubeconfigReady(metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretReason,
		fmt.Sprintf("Secret %s is provided by the user; CAPIBM probes it but never modifies it", stored.Name))
	return controlPlaneAccess{verified: true, probe: result, clusterIDRecorded: true}
}

// UserProvidedSecretProblem returns why a user-provided kubeconfig Secret cannot be used, or "" when it can (the
// UserProvidedSecretInvalid message): CAPI's ClusterCache reads kubeconfig Secrets only through a cache restricted to
// the cluster.x-k8s.io/cluster-name label (core/setup/setup.go:51-70), so without the label it would never connect
// while CAPIBM's own probe succeeds, and the kubeconfig lives in the value key.
//
// Parameters:
//   - s: the Secret; nil is reported as missing.
func UserProvidedSecretProblem(s *corev1.Secret) string {
	if s == nil {
		return "The kubeconfig Secret does not exist"
	}
	var missing []string
	if _, ok := s.Labels[clusterv1.ClusterNameLabel]; !ok {
		missing = append(missing, fmt.Sprintf("the label %s (CAPI's ClusterCache reads only labelled kubeconfig Secrets)",
			clusterv1.ClusterNameLabel))
	}
	if len(s.Data[secret.KubeconfigDataName]) == 0 {
		missing = append(missing, fmt.Sprintf("a non-empty %q key holding the kubeconfig", secret.KubeconfigDataName))
	}
	if len(missing) == 0 {
		return ""
	}
	return fmt.Sprintf("The user-provided Secret %s lacks %s; CAPIBM never modifies a user-provided Secret, so add it to the "+
		"Secret", s.Name, strings.Join(missing, " and "))
}

// kubeconfigSecretOwned reports whether CAPIBM manages the kubeconfig Secret: a Secret of type cluster.x-k8s.io/secret
// is CAPIBM's (the KubeadmControlPlane rule), unless another object is its controller, in which case CAPIBM leaves it
// alone like any user-provided Secret.
//
// Parameters:
//   - s is the Secret.
//   - cp is the control plane.
func kubeconfigSecretOwned(s *corev1.Secret, cp *controlplanev1.IBMCloudManagedControlPlane) bool {
	if s.Type != clusterv1.ClusterSecretType {
		return false
	}
	owner := metav1.GetControllerOf(s)
	if owner == nil {
		return true
	}
	if owner.UID != "" && cp.UID != "" {
		return owner.UID == cp.UID
	}
	return owner.Kind == controlPlaneKind && owner.Name == cp.Name
}

// loadKubeconfigSecret returns the Cluster's <cluster>-kubeconfig Secret, or nil when it does not exist, read once per
// reconcile with the API reader (no cluster-wide Secret informer). The binding and the kubeconfig step share the read.
// ctx is the reconcile context.
func (s *ControlPlaneScope) loadKubeconfigSecret(ctx context.Context) (*corev1.Secret, error) {
	if s.kubeconfigSecretLoaded {
		return s.kubeconfigSecret, nil
	}
	key := client.ObjectKey{Namespace: s.Cluster.Namespace, Name: s.kubeconfigSecretName()}
	stored := &corev1.Secret{}
	if err := s.params.APIReader.Get(ctx, key, stored); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("reading the kubeconfig Secret %s: %w", key, err)
		}
		stored = nil
	}
	s.kubeconfigSecret, s.kubeconfigSecretLoaded = stored, true
	return stored, nil
}

// kubeconfigSecretName is the name of the Cluster's kubeconfig Secret, <cluster>-kubeconfig (CAPI contract).
func (s *ControlPlaneScope) kubeconfigSecretName() string {
	return secret.Name(s.Cluster.Name, secret.Kubeconfig)
}

// initialize sets status.initialization.controlPlaneInitialized: only when the object as read from the API server
// already has spec.controlPlaneEndpoint, so that it is never persisted before the endpoint (the patch helper writes
// spec and status in separate requests), and the Secret in place is verified and (when CAPIBM owns it) carries
// cluster-id == status.clusterID. Sticky: it is never set back to false. access is the kubeconfig step's result.
func (s *ControlPlaneScope) initialize(access controlPlaneAccess) {
	if s.endpointAsRead == (controlplanev1.APIEndpoint{}) || !access.verified || !access.clusterIDRecorded {
		return
	}
	s.ControlPlane.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
}

// reportVersions publishes the API server's Kubernetes version in status.versions and status.version from a successful
// probe, through ReportedKubernetesVersion: while IBM rolls the master it keeps the lower of the published and the
// probed version, so alternating answers from old and new API server replicas never change status. A value is written
// only when it changes.
//
// Parameters:
//   - c is the cluster; it says whether the master is rolling.
//   - access is the kubeconfig step's result.
func (s *ControlPlaneScope) reportVersions(c *kubernetesservice.Cluster, access controlPlaneAccess) {
	if !access.verified || access.probe.GitVersion == "" {
		return
	}
	status := &s.ControlPlane.Status
	current := ""
	if len(status.Versions) > 0 {
		current = status.Versions[0].Version
	}
	rolling := IsMasterUpdatePending(c) || c.State == clusterStateUpdating
	reported := ReportedKubernetesVersion(current, access.probe.GitVersion, rolling)
	if reported == "" || (reported == current && status.Version == reported && len(status.Versions) == 1) {
		return
	}
	status.Versions = []controlplanev1.StatusVersion{{Version: reported}}
	status.Version = reported
}

// reportAvailable sets Available: True once the control plane is initialized, the last probe passed, the cluster exists
// and its master health is not in UnhealthyMasterHealth. The cluster's ingress status is never consulted: it stays
// warning or critical on a cluster without a public gateway, observed on IBM Cloud. It returns Done: the controller's
// requeue also honours the kubeconfig deadline (KubeconfigRequeueAfter).
//
// Parameters:
//   - c is the cluster.
//   - access is the kubeconfig step's result.
func (s *ControlPlaneScope) reportAvailable(c *kubernetesservice.Cluster, access controlPlaneAccess) Outcome {
	initialized := ptr.Deref(s.ControlPlane.Status.Initialization.ControlPlaneInitialized, false)
	switch {
	case IsMasterUnhealthy(c):
		return s.reportMaster(c)
	case !initialized:
		s.setAvailable(controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason,
			fmt.Sprintf("The master of IBM cluster %s is ready; the control plane becomes available once CAPIBM has verified its "+
				"kubeconfig and initialized it", c.ID))
	case !access.verified:
		s.setAvailable(controlplanev1.IBMCloudManagedControlPlaneAvailableWorkloadAPIUnreachableReason,
			fmt.Sprintf("The last probe of the API server of IBM cluster %s did not pass: %s", c.ID, accessFailureText(access)))
	default:
		conditions.Set(s.ControlPlane, metav1.Condition{
			Type:   controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.IBMCloudManagedControlPlaneAvailableReason,
		})
	}
	return Done()
}

// accessFailureText says why no probe passed: the probe failure, or that no verified kubeconfig exists. access is the
// kubeconfig step's result.
func accessFailureText(access controlPlaneAccess) string {
	if access.failure == nil {
		return "no verified kubeconfig Secret exists yet (see KubeconfigReady)"
	}
	return probeFailureText(access.failure)
}

// setKubeconfigReady sets KubeconfigReady.
//
// Parameters:
//   - status is the condition status.
//   - reason and message are the condition's; message may be empty.
func (s *ControlPlaneScope) setKubeconfigReady(status metav1.ConditionStatus, reason, message string) {
	conditions.Set(s.ControlPlane, metav1.Condition{
		Type:    controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// KubeconfigRequeueAfter returns when the kubeconfig must be looked at again, as managedkubeconfig.Decide answered in
// this reconcile (a 401 backoff, a CA rotation poll, a certificate or CA deadline); 0 when Decide did not run. The
// controller requeues at the earlier of this and the step's interval.
func (s *ControlPlaneScope) KubeconfigRequeueAfter() time.Duration {
	return s.kubeconfigRequeue
}

// probeFailureText describes a failed probe for a condition message by its step and class only, for example
// "SelfSubjectReview failed (Unauthorized)". Those are stable, so a repeated failure writes the same message and status
// is not patched again, and they carry no API-server text and no user name. The controller log gets the whole error
// (logProbeFailure).
//
// Parameters:
//   - err is the error of workloadprobe.Prober.Probe.
func probeFailureText(err error) string {
	var probeErr *workloadprobe.ProbeError
	if errors.As(err, &probeErr) {
		return fmt.Sprintf("%s failed (%s)", probeErr.Step, probeErr.Reason)
	}
	return "the probe failed"
}

// kubeconfigProbeReason maps a failed probe to the KubeconfigReady reason: an API server the management cluster cannot
// reach is EndpointUnreachable (private-only clusters need private connectivity), anything else means the credential or
// the trust did not verify. err is the error of workloadprobe.Prober.Probe.
func kubeconfigProbeReason(err error) string {
	if workloadprobe.ReasonOf(err) == workloadprobe.FailureUnreachable {
		return controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason
	}
	return controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason
}

// logProbeFailure logs a failed probe with its whole error, which carries no API-server message and no user name.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - what names the probed kubeconfig.
//   - err is the error of workloadprobe.Prober.Probe.
func logProbeFailure(ctx context.Context, what string, err error) {
	ctrl.LoggerFrom(ctx).Info("A kubeconfig did not pass the probe", "kubeconfig", what, "error", err.Error())
}

// endpointNote returns the EndpointChanged note when the selected server URL is not spec.controlPlaneEndpoint as read:
// the endpoint is written once and never changed, so the kubeconfig follows IBM and the Cluster's endpoint becomes
// informational. "" when they match. server is ManagementServerURL's answer.
func (s *ControlPlaneScope) endpointNote(server string) string {
	endpoint, err := EndpointFromServerURL(server)
	if err != nil || strings.EqualFold(endpoint.Host, s.endpointAsRead.Host) && endpoint.Port == s.endpointAsRead.Port {
		return ""
	}
	return fmt.Sprintf("%s: IBM serves the API server at %s, not at spec.controlPlaneEndpoint %s; the kubeconfig follows IBM, and "+
		"the Cluster's controlPlaneEndpoint is informational for managed clusters", NoteEndpointChanged, server,
		net.JoinHostPort(s.endpointAsRead.Host, strconv.Itoa(int(s.endpointAsRead.Port))))
}

// annotationTime parses an RFC 3339 annotation value; ok is false for an absent or unparseable value. v is the value.
func annotationTime(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, v)
	return t, err == nil
}

// formatAnnotationTime formats t for an annotation: RFC 3339 in UTC, seconds precision. t is the time.
func formatAnnotationTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// hasNote reports whether the current KubeconfigReady message already carries note, so that its event is emitted once
// per transition rather than on every reconcile. note is the note.
func (s *ControlPlaneScope) hasNote(note KubeconfigReadyNote) bool {
	condition := conditions.Get(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition)
	return condition != nil && slices.ContainsFunc(strings.Split(condition.Message, "; "), func(part string) bool {
		return strings.HasPrefix(part, string(note)+":")
	})
}

// The CAPIBM-owned Secret: reading it, probing it, deciding its refresh and recording it in annotations.

// storedKubeconfig is what CAPIBM reads from a kubeconfig Secret it owns.
type storedKubeconfig struct {
	// secret is the Secret; nil when it does not exist.
	secret *corev1.Secret
	// value is the Secret's value key.
	value []byte
	// info describes value; nil when there is no Secret, Inspect rejects value, or the cluster-id annotation names
	// another IBM cluster than the bound one. managedkubeconfig.Decide refreshes a nil kubeconfig at once.
	info *managedkubeconfig.Info
	// marker, refreshCheckedAt, caFetchedAt and fetchedAt are the annotations of the same names; zero when absent or
	// unparseable.
	marker           string
	refreshCheckedAt time.Time
	caFetchedAt      time.Time
	fetchedAt        time.Time
	// unauthorizedSince is the raw kubeconfig-unauthorized-since annotation.
	unauthorizedSince string
}

// readStoredKubeconfig reads a Secret CAPIBM owns. A Secret whose cluster-id annotation names another cluster than the
// bound one does not describe this cluster's credential and is treated as missing, so it is refreshed and rewritten.
//
// Parameters:
//   - stored is the Secret; nil when it does not exist.
//   - clusterID is the bound IBM cluster ID.
func readStoredKubeconfig(stored *corev1.Secret, clusterID string) storedKubeconfig {
	if stored == nil {
		return storedKubeconfig{}
	}
	out := storedKubeconfig{secret: stored, value: stored.Data[secret.KubeconfigDataName]}
	annotations := stored.Annotations
	out.marker = annotations[KubeconfigCARotationMarkerAnnotation]
	out.refreshCheckedAt, _ = annotationTime(annotations[KubeconfigRefreshCheckedAtAnnotation])
	out.caFetchedAt, _ = annotationTime(annotations[KubeconfigCAFetchedAtAnnotation])
	out.fetchedAt, _ = annotationTime(annotations[KubeconfigFetchedAtAnnotation])
	out.unauthorizedSince = annotations[KubeconfigUnauthorizedSinceAnnotation]
	if annotations[controlplanev1.KubeconfigClusterIDAnnotation] != clusterID {
		return out
	}
	if info, err := managedkubeconfig.Inspect(out.value); err == nil {
		out.info = &info
	}
	return out
}

// probeOutcome is the result of probing one kubeconfig.
type probeOutcome struct {
	// ran is false when nothing was probed.
	ran bool
	// result is the probe's result when err is nil.
	result workloadprobe.Result
	// err is the probe's error.
	err error
}

// passed reports whether the probe ran and passed.
func (p probeOutcome) passed() bool {
	return p.ran && p.err == nil
}

// unauthorized reports whether the probe ran and the API server answered 401.
func (p probeOutcome) unauthorized() bool {
	return p.ran && workloadprobe.ReasonOf(p.err) == workloadprobe.FailureUnauthorized
}

// failedTLS reports whether the probe ran and the server certificate did not verify with the kubeconfig's trust.
func (p probeOutcome) failedTLS() bool {
	return p.ran && workloadprobe.ReasonOf(p.err) == workloadprobe.FailureTLS
}

// StoredTLSRecheck decides the trust re-check of a stored kubeconfig whose probe failed TLS: CAPIBM then rebuilds it
// from the stored credential with IBM's cluster CA (GetCACertificate, IBM's getCACert) and with the system roots, and
// writes the mode that passes. managedkubeconfig.Decide has no such rule (its probe input is the 401 signal only, and
// its CA rule skips a kubeconfig without CA data, the SystemRoots mode of ROKS and of the IKS fallback), so the scope
// adds it; the trust of private-only masters is not yet verified against IBM Cloud, so a stored mode may need the other
// one. The re-check waits backoff after the latest attempt that left the stored credential in place (the
// refresh-checked-at annotation), so a kubeconfig that keeps failing TLS costs one getCACert per backoff, not one per
// reconcile. A missing time, or one after now (a forged value: no real attempt lies in the future), makes the re-check
// due at once, like managedkubeconfig.Decide treats such a refresh-checked-at.
//
// Parameters:
//   - refreshCheckedAt is the refresh-checked-at annotation; zero when absent or unparseable.
//   - now is the clock's time.
//   - backoff is the shortest time between two attempts; CAPIBM passes KubeconfigPolicy().AuthFailureBackoffMax.
//
// It returns whether the re-check is due now and, when it is not, how long until it is.
func StoredTLSRecheck(refreshCheckedAt, now time.Time, backoff time.Duration) (bool, time.Duration) {
	if refreshCheckedAt.IsZero() || refreshCheckedAt.After(now) {
		return true, 0
	}
	next := refreshCheckedAt.Add(backoff)
	if !now.Before(next) {
		return true, 0
	}
	return false, next.Sub(now)
}

// UnauthorizedSinceValue returns the kubeconfig-unauthorized-since annotation after a probe of the stored credential:
// "" (removed) after a passing probe, which ends the run of 401 answers; the stored value while the 401s go on, when it
// parses, so that a controller restart keeps the backoff; now on the first 401 of a run, and also when the stored value
// does not parse; and the stored value unchanged after any other outcome (another failure, or no probe), because only a
// successful probe ends the run. A forged value that parses is kept: managedkubeconfig.Decide bounds its effect to
// [MinRequeue, AuthFailureBackoffMax].
//
// Parameters:
//   - stored is the annotation's current value; "" when absent.
//   - passed reports whether the probe passed.
//   - unauthorized reports whether the API server answered the probe with 401.
//   - now is the clock's time.
func UnauthorizedSinceValue(stored string, passed, unauthorized bool, now time.Time) string {
	switch {
	case passed:
		return ""
	case !unauthorized:
		return stored
	}
	if _, ok := annotationTime(stored); ok {
		return stored
	}
	return formatAnnotationTime(now)
}

// KubeconfigAnnotationInput holds what the annotations of a freshly written kubeconfig record.
type KubeconfigAnnotationInput struct {
	// ClusterID is the bound IBM cluster ID. Required.
	ClusterID string
	// AccountID is the account from the cluster CRN (AccountIDFromCRN), never from CAPIBM's own credentials; "" leaves
	// account-id out.
	AccountID string
	// Info describes the written kubeconfig (managedkubeconfig.Inspect). Required.
	Info managedkubeconfig.Info
	// FetchedAt is when the credential was fetched from IBM. Required.
	FetchedAt time.Time
	// CAFetchedAt is when the embedded CA was fetched or confirmed; read only with ClusterCA trust, where it is required.
	CAFetchedAt time.Time
	// RefreshCheckedAt is set when a refresh returned the stored certificate again (IBM keeps returning the admin
	// certificate it minted), so that the next attempt waits SameCertificateRetry; zero leaves refresh-checked-at out.
	RefreshCheckedAt time.Time
	// CARotationMarker is IBM's CA rotation marker the kubeconfig was refreshed for; "" leaves ca-rotation-marker out.
	CARotationMarker string
}

// KubeconfigSecretAnnotations returns the annotations CAPIBM writes with a new kubeconfig: cluster-id, account-id,
// server, credential-*, fetched-at, ca-mode and, with ClusterCA trust, ca-fingerprint, ca-not-after and ca-fetched-at;
// refresh-checked-at and ca-rotation-marker when set. They hold no secret material, and none records which IAM identity
// fetched the credential. The start of a 401 run is not among them: a new kubeconfig is written only after it passed a
// probe, which ends the run. Times are RFC 3339 in UTC.
//
// Parameters:
//   - in: what the annotations record.
//
// It returns an error naming the field when ClusterID is empty, Info has no server or trust mode, FetchedAt is zero, or
// ClusterCA trust comes without its CA description or CAFetchedAt.
func KubeconfigSecretAnnotations(in KubeconfigAnnotationInput) (map[string]string, error) {
	switch {
	case in.ClusterID == "":
		return nil, errors.New("KubeconfigAnnotationInput.ClusterID is empty; pass status.clusterID")
	case in.Info.Server == "" || in.Info.Trust.Validate() != nil:
		return nil, errors.New("KubeconfigAnnotationInput.Info has no server or trust mode; pass managedkubeconfig.Inspect's answer")
	case in.FetchedAt.IsZero():
		return nil, errors.New("KubeconfigAnnotationInput.FetchedAt is zero; pass when the credential was fetched")
	case in.Info.Trust == managedkubeconfig.ClusterCA && (in.Info.CA == nil || in.CAFetchedAt.IsZero()):
		return nil, errors.New("KubeconfigAnnotationInput needs Info.CA and CAFetchedAt with ClusterCA trust")
	}
	cert := in.Info.Certificate
	annotations := map[string]string{
		controlplanev1.KubeconfigClusterIDAnnotation: in.ClusterID,
		KubeconfigServerAnnotation:                   in.Info.Server,
		KubeconfigCredentialTypeAnnotation:           KubeconfigCredentialTypeClientCertificate,
		KubeconfigCredentialNotBeforeAnnotation:      formatAnnotationTime(cert.NotBefore),
		KubeconfigCredentialNotAfterAnnotation:       formatAnnotationTime(cert.NotAfter),
		KubeconfigCredentialFingerprintAnnotation:    cert.Fingerprint,
		KubeconfigFetchedAtAnnotation:                formatAnnotationTime(in.FetchedAt),
		KubeconfigCAModeAnnotation:                   string(in.Info.Trust),
	}
	if in.Info.Trust == managedkubeconfig.ClusterCA {
		annotations[KubeconfigCAFingerprintAnnotation] = in.Info.CA.Fingerprint
		annotations[KubeconfigCANotAfterAnnotation] = formatAnnotationTime(in.Info.CA.NotAfter)
		annotations[KubeconfigCAFetchedAtAnnotation] = formatAnnotationTime(in.CAFetchedAt)
	}
	if in.AccountID != "" {
		annotations[KubeconfigAccountIDAnnotation] = in.AccountID
	}
	if !in.RefreshCheckedAt.IsZero() {
		annotations[KubeconfigRefreshCheckedAtAnnotation] = formatAnnotationTime(in.RefreshCheckedAt)
	}
	if in.CARotationMarker != "" {
		annotations[KubeconfigCARotationMarkerAnnotation] = in.CARotationMarker
	}
	return annotations, nil
}

// reconcileOwnedKubeconfig keeps a Secret CAPIBM owns verified and current: it probes the stored kubeconfig, asks
// managedkubeconfig.Decide what is due, refreshes when something is, and otherwise only records the start or end of a
// 401 run and the account. ok is false when outcome ends the reconcile.
//
// After a refresh attempt that failed, Decide's RequeueAfter of this reconcile is replaced (afterFailedRefresh): Decide
// returns MinRequeue whenever something is due, to look at the outcome of the attempt soon, which would repeat a
// failing attempt every minute.
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is the bound cluster, master ready.
//   - server is ManagementServerURL's answer.
//   - existing is the Secret; nil when it does not exist.
func (s *ControlPlaneScope) reconcileOwnedKubeconfig(ctx context.Context, c *kubernetesservice.Cluster, server string,
	existing *corev1.Secret) (controlPlaneAccess, Outcome, bool) {
	now := s.params.Clock.Now()
	stored := readStoredKubeconfig(existing, c.ID)
	probe := s.probeStoredKubeconfig(ctx, stored)
	since := UnauthorizedSinceValue(stored.unauthorizedSince, probe.passed(), probe.unauthorized(), now)
	plan, err := s.decideKubeconfig(c, server, stored, probe, since, now)
	if err != nil {
		return controlPlaneAccess{}, Transient(fmt.Errorf("deciding the kubeconfig refresh of %s: %w", s.objectName(), err)), false
	}
	s.kubeconfigRequeue = plan.RequeueAfter
	access := controlPlaneAccess{verified: probe.passed(), probe: probe.result, failure: probe.err, clusterIDRecorded: stored.info != nil}
	report := kubeconfigReport{}
	if plan.Refresh || plan.RefreshCA {
		refreshed, failure, err := s.refreshOwnedKubeconfig(ctx, c, server, stored, probe, since, plan.Decision)
		if err != nil {
			return controlPlaneAccess{}, s.kubeconfigTransient(err), false
		}
		if failure != nil {
			report = *failure
			access.retry, s.kubeconfigRequeue = s.afterFailedRefresh(c, server, stored, probe, since, now, *failure)
		} else {
			access = refreshed
		}
	} else if err := s.updateKubeconfigAnnotations(ctx, stored, s.steadyKubeconfigAnnotations(stored, since)); err != nil {
		return controlPlaneAccess{}, s.kubeconfigTransient(err), false
	}
	s.reportOwnedKubeconfig(ctx, c, server, access, report, plan)
	return access, Outcome{}, true
}

// probeStoredKubeconfig probes the stored kubeconfig, every reconcile: a 401 starts a refresh with backoff, a passing
// probe gives the version. A Secret Decide treats as missing is not probed. stored is the Secret.
//
// Parameters:
//   - ctx is the reconcile context.
//   - stored is what the Secret holds.
func (s *ControlPlaneScope) probeStoredKubeconfig(ctx context.Context, stored storedKubeconfig) probeOutcome {
	if stored.info == nil {
		return probeOutcome{}
	}
	result, err := s.params.Prober.Probe(ctx, stored.value)
	if err != nil {
		logProbeFailure(ctx, "stored kubeconfig", err)
	}
	return probeOutcome{ran: true, result: result, err: err}
}

// kubeconfigPlan is what the kubeconfig step does in one reconcile: managedkubeconfig.Decide's answer, completed with
// the trust re-check of a stored kubeconfig that fails TLS (StoredTLSRecheck).
type kubeconfigPlan struct {
	managedkubeconfig.Decision
	// held is true when no refresh runs in this reconcile only because a rule waits for its backoff after the latest
	// attempt that left the stored credential in place (refresh-checked-at). The report of that attempt then stays
	// until the next one (reportOwnedKubeconfig).
	held bool
}

// decideKubeconfig plans the kubeconfig step: it gathers the inputs of managedkubeconfig.Decide (which has no
// credential-identity input: which IAM identity fetched the stored certificate never triggers or blocks a refresh),
// adds the trust re-check of a stored kubeconfig that fails TLS (planTLSRecheck), and finds out whether a refresh is
// held by its backoff (refreshHeld).
//
// Parameters:
//   - c is the cluster.
//   - server is ManagementServerURL's answer.
//   - stored is what the Secret holds.
//   - probe is the stored kubeconfig's probe.
//   - since is the kubeconfig-unauthorized-since value after that probe (UnauthorizedSinceValue).
//   - now is the clock's time.
func (s *ControlPlaneScope) decideKubeconfig(c *kubernetesservice.Cluster, server string, stored storedKubeconfig, probe probeOutcome,
	since string, now time.Time) (kubeconfigPlan, error) {
	policy := KubeconfigPolicy()
	desired := managedkubeconfig.Desired{
		Server:               server,
		CARotationMarker:     managedkubeconfig.CARotationMarker(c.CARotationStatus, c.CARotationCompletedDate),
		CARotationInProgress: IsCARotationInProgress(c.CARotationStatus),
	}
	current := currentKubeconfig(stored, probe, since, now)
	decision, err := managedkubeconfig.Decide(now, desired, current, policy)
	if err != nil {
		return kubeconfigPlan{}, err
	}
	plan := kubeconfigPlan{Decision: decision}
	tlsHeld := probe.failedTLS() && planTLSRecheck(&plan, stored.refreshCheckedAt, now, policy)
	if plan.Refresh || plan.RefreshCA {
		return plan, nil
	}
	if tlsHeld {
		plan.held = true
		return plan, nil
	}
	plan.held, err = refreshHeld(now, desired, current, policy)
	return plan, err
}

// currentKubeconfig is the Current input of managedkubeconfig.Decide: the stored kubeconfig, its annotations and the
// 401 run of its probe (kubeconfig-unauthorized-since).
//
// Parameters:
//   - stored is what the Secret holds.
//   - probe is the stored kubeconfig's probe.
//   - since is the kubeconfig-unauthorized-since value after that probe (UnauthorizedSinceValue).
//   - now is the clock's time.
func currentKubeconfig(stored storedKubeconfig, probe probeOutcome, since string, now time.Time) managedkubeconfig.Current {
	current := managedkubeconfig.Current{
		Kubeconfig:       stored.info,
		CARotationMarker: stored.marker,
		RefreshCheckedAt: stored.refreshCheckedAt,
		CAFetchedAt:      stored.caFetchedAt,
	}
	if probe.unauthorized() {
		// UnauthorizedSinceValue returns a parseable time for a 401; now is only the fallback of a programming error.
		current.LastProbe = managedkubeconfig.ProbeStatus{Unauthorized: true, UnauthorizedSince: now}
		if t, ok := annotationTime(since); ok {
			current.LastProbe.UnauthorizedSince = t
		}
	}
	return current
}

// planTLSRecheck adds the trust re-check of a stored kubeconfig that fails TLS to plan (StoredTLSRecheck): when it is
// due, a CA re-check (RefreshCA, which tries both trust modes with the stored credential unless a credential refresh
// runs as well) and Decide's MinRequeue to look at its outcome; otherwise the requeue no later than when it is due. A
// refresh of everything (rule 2 of managedkubeconfig.Decide) already fetches the CA and needs no re-check. It returns
// whether the re-check waits for its backoff.
//
// Parameters:
//   - plan is the plan so far; planTLSRecheck changes it.
//   - refreshCheckedAt is the refresh-checked-at annotation.
//   - now is the clock's time.
//   - policy is KubeconfigPolicy.
func planTLSRecheck(plan *kubeconfigPlan, refreshCheckedAt, now time.Time, policy managedkubeconfig.Policy) bool {
	if plan.Refresh && plan.RefreshCA {
		return false
	}
	// The backoff is the longest one of a 401 run: a stored kubeconfig that fails TLS is as unusable as one answered
	// 401, and without a record of when the TLS failures began, no growing backoff can be measured.
	due, wait := StoredTLSRecheck(refreshCheckedAt, now, policy.AuthFailureBackoffMax)
	if !due {
		plan.RequeueAfter = min(plan.RequeueAfter, max(wait, policy.MinRequeue))
		return true
	}
	plan.RefreshCA = true
	plan.Reasons = append(plan.Reasons, managedkubeconfig.ReasonCARecheck)
	plan.RequeueAfter = policy.MinRequeue
	return false
}

// refreshHeld reports whether Decide holds back a credential refresh only because of the attempt recorded in
// refresh-checked-at (its rules 3 and 4: the 401 backoff and SameCertificateRetry): the same evaluation without that
// record asks for one. It reuses Decide rather than repeating its rules.
//
// Parameters:
//   - now is the clock's time.
//   - desired and current are Decide's inputs of this reconcile.
//   - policy is KubeconfigPolicy.
func refreshHeld(now time.Time, desired managedkubeconfig.Desired, current managedkubeconfig.Current, policy managedkubeconfig.Policy) (bool, error) {
	if current.RefreshCheckedAt.IsZero() {
		return false, nil
	}
	unrecorded := current
	unrecorded.RefreshCheckedAt = time.Time{}
	decision, err := managedkubeconfig.Decide(now, desired, unrecorded, policy)
	return decision.Refresh, err
}

// afterFailedRefresh returns the step's outcome and the kubeconfig deadline of a reconcile whose refresh attempt
// failed, in place of Decide's MinRequeue:
//   - A failed IBM call has its error class's outcome (kubeconfigReport.outcome), and no deadline applies to this
//     reconcile, so that a 403 is not asked again within its terminalRecheck.
//   - A new kubeconfig that failed the probe has no IBM error class. When the attempt is recorded in refresh-checked-at
//     and the rule that asked for it waits for that record (the 401 run, SameCertificateRetry, the TLS re-check),
//     Decide's next deadline with the attempt recorded applies, which keeps the 401 run's backoff of 1 to 10 minutes.
//     Otherwise (no Secret to record it in, or a rule that refreshes whatever was attempted: a missing kubeconfig, a
//     changed server or CA rotation marker, a CA re-check), the steady poll spaces the attempts.
//
// Parameters:
//   - c is the cluster.
//   - server is ManagementServerURL's answer.
//   - stored is what the Secret held before the attempt.
//   - probe is the stored kubeconfig's probe.
//   - since is the kubeconfig-unauthorized-since value after that probe.
//   - now is the clock's time, which refresh-checked-at records.
//   - failure is the failed attempt.
func (s *ControlPlaneScope) afterFailedRefresh(c *kubernetesservice.Cluster, server string, stored storedKubeconfig, probe probeOutcome,
	since string, now time.Time, failure kubeconfigReport) (Outcome, time.Duration) {
	if failure.outcome.Kind != "" {
		return failure.outcome, 0
	}
	if stored.secret != nil {
		recorded := stored
		// The annotation keeps whole seconds (formatAnnotationTime).
		recorded.refreshCheckedAt = now.Truncate(time.Second)
		if plan, err := s.decideKubeconfig(c, server, recorded, probe, since, now); err == nil && !plan.Refresh && !plan.RefreshCA {
			return Outcome{}, plan.RequeueAfter
		}
	}
	return NotDone(IntervalSteadyState, failure.reason), 0
}

// steadyKubeconfigAnnotations are the annotation changes of a reconcile that refreshes nothing: the start or end of a
// 401 run and the account from the cluster CRN. A nil value removes the key.
//
// Parameters:
//   - stored is what the Secret holds.
//   - since is the kubeconfig-unauthorized-since value after the probe.
func (s *ControlPlaneScope) steadyKubeconfigAnnotations(stored storedKubeconfig, since string) map[string]*string {
	changes := map[string]*string{KubeconfigUnauthorizedSinceAnnotation: optional(since)}
	if stored.info != nil {
		if account := s.accountID(); account != "" {
			changes[KubeconfigAccountIDAnnotation] = &account
		}
	}
	return changes
}

// optional returns nil for "" and a pointer to v otherwise; a nil annotation change removes the key. v is the value.
func optional(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

// accountID returns the account of the bound cluster from status.clusterCRN, never from CAPIBM's own credentials, or ""
// when the CRN does not name one; the annotation is then left out rather than guessed.
func (s *ControlPlaneScope) accountID() string {
	account, err := AccountIDFromCRN(s.ControlPlane.Status.ClusterCRN)
	if err != nil {
		return ""
	}
	return account
}

// updateKubeconfigAnnotations applies annotation changes to a Secret CAPIBM owns, with a patch only when something
// changes. A nil value removes the key. Nothing is written when the Secret does not exist.
//
// Parameters:
//   - ctx is the reconcile context.
//   - stored is what the Secret holds.
//   - changes maps annotation keys to their new values.
func (s *ControlPlaneScope) updateKubeconfigAnnotations(ctx context.Context, stored storedKubeconfig, changes map[string]*string) error {
	if stored.secret == nil {
		return nil
	}
	updated := stored.secret.DeepCopy()
	for key, value := range changes {
		if value == nil {
			delete(updated.Annotations, key)
			continue
		}
		metav1.SetMetaDataAnnotation(&updated.ObjectMeta, key, *value)
	}
	if equalAnnotations(updated.Annotations, stored.secret.Annotations) {
		return nil
	}
	return s.patchKubeconfigSecret(ctx, stored.secret, updated)
}

// equalAnnotations reports whether two annotation maps hold the same keys and values; nil equals empty.
//
// Parameters:
//   - a and b are the maps.
func equalAnnotations(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

// patchKubeconfigSecret writes updated over original with a merge patch that carries original's resourceVersion, so
// that a concurrent change fails the patch instead of being overwritten; the next reconcile reads the Secret again.
//
// Parameters:
//   - ctx is the reconcile context.
//   - original is the Secret as read.
//   - updated is the Secret to write.
func (s *ControlPlaneScope) patchKubeconfigSecret(ctx context.Context, original, updated *corev1.Secret) error {
	if err := s.params.Client.Patch(ctx, updated, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("patching the kubeconfig Secret %s: %w", client.ObjectKeyFromObject(original), err)
	}
	s.kubeconfigSecret = updated
	return nil
}

// kubeconfigReport is a refresh attempt that failed: its KubeconfigReady condition and the outcome of the step. The
// zero value means none.
type kubeconfigReport struct {
	// reason and message are the condition's; refreshOwnedKubeconfig ends the message with refreshFailureNote.
	reason  string
	message string
	// outcome brings the next reconcile after a failed IBM call or an unusable answer: RateLimited for a 429, NotDone at
	// the steady interval for a 401, at terminalRecheck for a 403, a 400 and an answer that does not build a kubeconfig,
	// and Transient (controller-runtime's backoff) for a 5xx or a network failure. It is the zero value for a new
	// kubeconfig that failed the probe, which has no IBM error class (afterFailedRefresh).
	outcome Outcome
	// transient marks a failure that says nothing about the cluster or the credential (a 429, a 5xx, a network
	// failure): it leaves the condition to a stored kubeconfig that passed the probe, so that an IBM blip during a
	// refresh that is not urgent does not turn the control plane's Ready False (the cpProblem rule of the binding step).
	transient bool
}

// reportOwnedKubeconfig sets KubeconfigReady for a Secret CAPIBM owns, in this order: Unknown CARotationInProgress
// while IBM rotates the CA; False with the reason of a refresh that failed in this reconcile (failure), unless it was
// transient and the Secret in place passed the probe; the report of the latest failed attempt as read, while the next
// attempt waits for its backoff (the reconcile that the failed attempt's own status patch starts must not report the
// stored Secret as Ready in between); False with the probe's reason when the Secret in place did not pass; otherwise
// True with the EndpointChanged and AdminCertificateExpiring notes. The AdminCertificateExpiring Warning event is
// emitted when that note appears.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - c is the cluster.
//   - server is ManagementServerURL's answer.
//   - access is the kubeconfig step's result.
//   - failure is a failed refresh; the zero value when none.
//   - plan is the step's plan (decideKubeconfig).
func (s *ControlPlaneScope) reportOwnedKubeconfig(ctx context.Context, c *kubernetesservice.Cluster, server string, access controlPlaneAccess,
	failure kubeconfigReport, plan kubeconfigPlan) {
	switch {
	case IsCARotationInProgress(c.CARotationStatus):
		s.setKubeconfigReady(metav1.ConditionUnknown, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCARotationInProgressReason,
			fmt.Sprintf("IBM rotates the CA of cluster %s; CAPIBM polls every minute and refreshes the kubeconfig when the rotation "+
				"completes", c.ID))
	case failure.reason != "" && (!failure.transient || !access.verified):
		s.setKubeconfigReady(metav1.ConditionFalse, failure.reason, failure.message)
	case plan.held && s.reportsFailedRefresh():
		// Kept as read: nothing was attempted since that report, and a passing probe of the stored Secret does not
		// answer what the attempt asked. The next attempt replaces it.
	case !access.verified:
		s.setKubeconfigReady(metav1.ConditionFalse, kubeconfigProbeReason(access.failure), s.storedProbeMessage(server, access))
	default:
		s.setKubeconfigReady(metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason,
			s.readyNotes(ctx, server, plan.Decision))
	}
}

// refreshFailureNote ends the KubeconfigReady message of every refresh attempt that failed (refreshOwnedKubeconfig).
// With a Secret in place it is keptSecretNote, by which reportsFailedRefresh recognizes the report while the next
// attempt waits; without one, nothing is kept, because every reconcile attempts again. The note carries no time, so
// that a repeated failure writes the same message and status is not patched again.
//
// Parameters:
//   - stored is what the Secret holds.
func (s *ControlPlaneScope) refreshFailureNote(stored storedKubeconfig) string {
	if stored.secret != nil {
		return s.keptSecretNote()
	}
	return fmt.Sprintf("CAPIBM writes Secret %s only once a new kubeconfig passes the probe, and tries again after a backoff",
		s.kubeconfigSecretName())
}

// keptSecretNote is the end of the message of a failed refresh attempt that left the Secret in place.
func (s *ControlPlaneScope) keptSecretNote() string {
	return fmt.Sprintf("CAPIBM keeps Secret %s as it is and tries the refresh again after a backoff", s.kubeconfigSecretName())
}

// reportsFailedRefresh reports whether KubeconfigReady, as read at the start of this reconcile, is the report of a
// refresh attempt that failed and left the Secret in place (it ends with keptSecretNote). The control plane is read
// uncached (with the API reader), so this is the report of the latest attempt. A forged condition can only keep
// KubeconfigReady False until the next attempt, which refresh-checked-at bounds.
func (s *ControlPlaneScope) reportsFailedRefresh() bool {
	condition := conditions.Get(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition)
	return condition != nil && condition.Status == metav1.ConditionFalse && strings.HasSuffix(condition.Message, s.keptSecretNote())
}

// storedProbeMessage says why the Secret in place is not verified. access is the kubeconfig step's result; server is
// the API server URL.
//
// Parameters:
//   - server is ManagementServerURL's answer.
//   - access is the kubeconfig step's result.
func (s *ControlPlaneScope) storedProbeMessage(server string, access controlPlaneAccess) string {
	switch {
	case access.failure == nil:
		return fmt.Sprintf("No verified kubeconfig of %s is stored in Secret %s yet; CAPIBM fetches IBM's admin credential and "+
			"writes it once it passed the probe", server, s.kubeconfigSecretName())
	case workloadprobe.ReasonOf(access.failure) == workloadprobe.FailureUnreachable:
		return fmt.Sprintf("The management cluster cannot reach %s (%s); private-only clusters need private connectivity",
			server, probeFailureText(access.failure))
	case workloadprobe.ReasonOf(access.failure) == workloadprobe.FailureUnauthorized:
		return fmt.Sprintf("The API server at %s rejects the credential stored in Secret %s (%s); CAPIBM fetches IBM's admin "+
			"credential again, with a backoff of 1 to 10 minutes", server, s.kubeconfigSecretName(), probeFailureText(access.failure))
	case workloadprobe.ReasonOf(access.failure) == workloadprobe.FailureTLS:
		// Whatever the stored ca-mode, both trust modes are tried again (planTLSRecheck), so the message names both. The
		// ten minutes are KubeconfigPolicy's AuthFailureBackoffMax, the backoff planTLSRecheck applies.
		return fmt.Sprintf("The server certificate of %s does not verify with the trust of the kubeconfig stored in Secret %s (%s); "+
			"CAPIBM keeps it and tries IBM's cluster CA and the system roots again, at most once every 10 minutes", server,
			s.kubeconfigSecretName(), probeFailureText(access.failure))
	default:
		// The API server answered, but not with success (FailureOther, for example a 403 or a 5xx): neither a new
		// credential nor another trust mode is the remedy (workloadprobe), so CAPIBM only probes again.
		return fmt.Sprintf("The kubeconfig stored in Secret %s did not pass the probe against %s: %s; CAPIBM keeps it and "+
			"probes it again at the next poll", s.kubeconfigSecretName(), server, probeFailureText(access.failure))
	}
}

// readyNotes returns the message of a True KubeconfigReady: the EndpointChanged note and the AdminCertificateExpiring
// note, "" when neither applies. It emits the AdminCertificateExpiring Warning when that note is new.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - server is ManagementServerURL's answer.
//   - decision is managedkubeconfig.Decide's answer.
func (s *ControlPlaneScope) readyNotes(ctx context.Context, server string, decision managedkubeconfig.Decision) string {
	var notes []string
	if note := s.endpointNote(server); note != "" {
		notes = append(notes, note)
	}
	if slices.Contains(decision.Warnings, managedkubeconfig.WarningCertificateExpiring) {
		note := s.expiryNote()
		if !s.hasNote(NoteAdminCertificateExpiring) {
			ctrl.LoggerFrom(ctx).Info("IBM's admin certificate expires soon", "secret", s.kubeconfigSecretName())
			s.params.Recorder.Event(s.ControlPlane, corev1.EventTypeWarning, EventReasonAdminCertificateExpiring, note)
		}
		notes = append(notes, note)
	}
	return strings.Join(notes, "; ")
}

// expiryNote is the AdminCertificateExpiring note: the stored certificate's notAfter, from the Secret in place, and
// what renews it (when IBM re-mints the certificate on its own is not yet verified against IBM Cloud).
func (s *ControlPlaneScope) expiryNote() string {
	notAfter := "its notAfter"
	if s.kubeconfigSecret != nil {
		if info, err := managedkubeconfig.Inspect(s.kubeconfigSecret.Data[secret.KubeconfigDataName]); err == nil {
			notAfter = formatAnnotationTime(info.Certificate.NotAfter)
		}
	}
	return fmt.Sprintf("%s: IBM's admin certificate in Secret %s expires at %s and IBM still serves the same certificate; a CA "+
		"rotation of the cluster re-issues it", NoteAdminCertificateExpiring, s.kubeconfigSecretName(), notAfter)
}

// The refresh: fetch, verify with the flavor's trust mode then the other one, write only what passed.

// kubeconfigParts are the pieces a new kubeconfig is built from.
type kubeconfigParts struct {
	// credential is IBM's admin client certificate and key: fetched (GetAdminKubeconfig) or, for a CA-only refresh, the
	// stored one.
	credential managedkubeconfig.ClientCredential
	// credentialFetched is true when credential came from IBM in this reconcile.
	credentialFetched bool
	// kubeconfigCA is the CA of IBM's admin kubeconfig (IKS); nil when it carries none (ROKS) or was not fetched.
	kubeconfigCA []byte
	// fetchCA asks for getCACert's CA (GetCACertificate) before any other: Decide's CA re-check, which also cross-checks
	// the CA of IKS's admin kubeconfig.
	fetchCA bool
	// ca is the CA in use once known, and caFetched is true when it came from IBM in this reconcile.
	ca        []byte
	caFetched bool
}

// kubeconfigCandidate is a new kubeconfig that passed the probe.
type kubeconfigCandidate struct {
	// value is the kubeconfig.
	value []byte
	// info describes it.
	info managedkubeconfig.Info
	// probe is the passing probe's result.
	probe workloadprobe.Result
	// caFetched is true when its embedded CA came from IBM in this reconcile.
	caFetched bool
}

// refreshOwnedKubeconfig carries out what Decide asked for: it fetches the parts, verifies the new kubeconfig with the
// flavor's trust mode and then the other one, and writes it only when it passed. When IBM returns what is stored (the
// same certificate, which IBM keeps returning once minted), only the annotations change. The returned report is non-nil
// when the refresh failed, and err is non-nil when a write failed.
//
// A failed attempt (a fetch that failed, an answer that does not build a kubeconfig, a new kubeconfig that failed the
// probe) never replaces the stored Secret, and is recorded in refresh-checked-at whenever a Secret exists: Decide's
// backoff (the 401 run, SameCertificateRetry) and the TLS re-check wait from it, so a failing refresh is not repeated
// by every reconcile that something else starts. Without a Secret nothing can be recorded; the report's outcome then
// alone spaces the attempts.
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is the bound cluster.
//   - server is ManagementServerURL's answer.
//   - stored is what the Secret holds.
//   - probe is the stored kubeconfig's probe.
//   - since is the kubeconfig-unauthorized-since value after that probe.
//   - decision is Decide's answer; Refresh or RefreshCA is set.
func (s *ControlPlaneScope) refreshOwnedKubeconfig(ctx context.Context, c *kubernetesservice.Cluster, server string, stored storedKubeconfig,
	probe probeOutcome, since string, decision managedkubeconfig.Decision) (controlPlaneAccess, *kubeconfigReport, error) {
	clients, err := s.ibm(ctx)
	if err != nil {
		return controlPlaneAccess{}, nil, err
	}
	parts, candidate, report := s.attemptRefresh(ctx, clients, c, server, stored, probe, decision)
	if report == nil {
		if err := s.storeKubeconfig(ctx, c, stored, candidate, parts, decision); err != nil {
			return controlPlaneAccess{}, nil, err
		}
		return controlPlaneAccess{verified: true, probe: candidate.probe, clusterIDRecorded: true}, nil, nil
	}
	report.message += "; " + s.refreshFailureNote(stored)
	changes := s.steadyKubeconfigAnnotations(stored, since)
	changes[KubeconfigRefreshCheckedAtAnnotation] = ptr.To(formatAnnotationTime(s.params.Clock.Now()))
	return controlPlaneAccess{}, report, s.updateKubeconfigAnnotations(ctx, stored, changes)
}

// attemptRefresh fetches the parts decision asks for and verifies the kubeconfig they build (fetchKubeconfigParts,
// verifyKubeconfig). The returned report is non-nil when a fetch failed or no kubeconfig passed; parts and the
// candidate are set otherwise.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - c is the bound cluster.
//   - server is ManagementServerURL's answer.
//   - stored is what the Secret holds.
//   - probe is the stored kubeconfig's probe.
//   - decision is Decide's answer.
func (s *ControlPlaneScope) attemptRefresh(ctx context.Context, clients *controlPlaneClients, c *kubernetesservice.Cluster, server string,
	stored storedKubeconfig, probe probeOutcome, decision managedkubeconfig.Decision) (*kubeconfigParts, kubeconfigCandidate, *kubeconfigReport) {
	parts, report := s.fetchKubeconfigParts(ctx, clients, c, stored, decision)
	if report != nil {
		return nil, kubeconfigCandidate{}, report
	}
	candidate, report := s.verifyKubeconfig(ctx, clients, c, server, stored, probe, parts)
	return parts, candidate, report
}

// fetchKubeconfigParts fetches what decision asks for: IBM's admin kubeconfig (GetAdminKubeconfig) for a credential
// refresh, otherwise the stored credential with a new CA. The CA itself is fetched by caFor, only for a trust mode that
// embeds one. The returned report is non-nil when a fetch failed.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - c is the bound cluster.
//   - stored is what the Secret holds.
//   - decision is Decide's answer.
func (s *ControlPlaneScope) fetchKubeconfigParts(ctx context.Context, clients *controlPlaneClients, c *kubernetesservice.Cluster,
	stored storedKubeconfig, decision managedkubeconfig.Decision) (*kubeconfigParts, *kubeconfigReport) {
	parts := &kubeconfigParts{fetchCA: decision.RefreshCA}
	if !decision.Refresh {
		credential, err := managedkubeconfig.ExtractClientCredential(stored.value)
		if err != nil {
			// Decide asks for a CA-only refresh only for a kubeconfig Inspect accepted, which holds a valid credential. No
			// IBM call failed: afterFailedRefresh spaces the next attempt.
			return nil, &kubeconfigReport{reason: controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason,
				message: fmt.Sprintf("The credential stored in Secret %s cannot be read again", s.kubeconfigSecretName())}
		}
		parts.credential = credential
		return parts, nil
	}
	admin, err := clients.kubernetes.GetAdminKubeconfig(ctx, c.ID)
	if err != nil {
		return nil, s.fetchFailed(ctx, clients, kubernetesservice.OpGetAdminKubeconfig, "fetching the admin kubeconfig of IBM cluster "+c.ID, err)
	}
	parts.credential = managedkubeconfig.ClientCredential{CertificatePEM: admin.CertificatePEM, KeyPEM: admin.KeyPEM}
	parts.credentialFetched = true
	parts.kubeconfigCA = admin.CertificateAuthorityPEM
	return parts, nil
}

// caFor returns the CA data of a trust mode: none for SystemRoots, whose CA data would replace the system roots; for
// ClusterCA the CA IBM returned in this reconcile: getCACert's (GetCACertificate) when Decide asked for a CA re-check,
// else the CA of IBM's admin kubeconfig, else getCACert's, fetched here. A getCACert CA that differs from the admin
// kubeconfig's is logged and used; the probe decides whether it verifies. The returned report is non-nil when the fetch
// failed.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - c is the bound cluster.
//   - trust is the trust mode being tried.
//   - parts are the pieces; caFor records the CA in them.
func (s *ControlPlaneScope) caFor(ctx context.Context, clients *controlPlaneClients, c *kubernetesservice.Cluster, trust managedkubeconfig.CATrust,
	parts *kubeconfigParts) ([]byte, *kubeconfigReport) {
	switch {
	case trust != managedkubeconfig.ClusterCA:
		return nil, nil
	case len(parts.ca) > 0:
		return parts.ca, nil
	case !parts.fetchCA && len(parts.kubeconfigCA) > 0:
		parts.ca, parts.caFetched = parts.kubeconfigCA, true
		return parts.ca, nil
	}
	ca, err := clients.kubernetes.GetCACertificate(ctx, c.ID)
	if err != nil {
		return nil, s.fetchFailed(ctx, clients, kubernetesservice.OpGetCACertificate, "fetching the CA of IBM cluster "+c.ID, err)
	}
	if len(parts.kubeconfigCA) > 0 && !samePEM(parts.kubeconfigCA, ca.PEM) {
		ctrl.LoggerFrom(ctx).Info("IBM's admin kubeconfig and getCACert carry different cluster CAs; CAPIBM verifies with getCACert's",
			"ibmCluster", c.ID)
	}
	parts.ca, parts.caFetched = ca.PEM, true
	return parts.ca, nil
}

// samePEM reports whether two PEM texts describe the same certificates (managedkubeconfig.InspectPEM fingerprints).
//
// Parameters:
//   - a and b are the PEM texts.
func samePEM(a, b []byte) bool {
	infoA, errA := managedkubeconfig.InspectPEM(a)
	infoB, errB := managedkubeconfig.InspectPEM(b)
	return errA == nil && errB == nil && infoA.Fingerprint == infoB.Fingerprint
}

// verifyKubeconfig builds the new kubeconfig with the flavor's trust mode (CATrustFor) and, after a TLS failure, with
// the other one (the trust of private-only masters is not yet verified against IBM Cloud), and returns the first one
// that passes the probe (verify before write). A kubeconfig equal to the stored one is not probed again: the stored
// probe of this reconcile is its answer. The returned report is non-nil when none passed or a part could not be used.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - c is the bound cluster.
//   - server is ManagementServerURL's answer.
//   - stored is what the Secret holds.
//   - storedProbe is the stored kubeconfig's probe.
//   - parts are the pieces.
func (s *ControlPlaneScope) verifyKubeconfig(ctx context.Context, clients *controlPlaneClients, c *kubernetesservice.Cluster, server string,
	stored storedKubeconfig, storedProbe probeOutcome, parts *kubeconfigParts) (kubeconfigCandidate, *kubeconfigReport) {
	primary := CATrustFor(s.ControlPlane.Spec.Distribution)
	var failure error
	for _, trust := range []managedkubeconfig.CATrust{primary, primary.Other()} {
		ca, report := s.caFor(ctx, clients, c, trust, parts)
		if report != nil {
			return kubeconfigCandidate{}, report
		}
		value, err := managedkubeconfig.Build(managedkubeconfig.Input{ClusterName: s.Cluster.Name, Server: server, Trust: trust,
			CertificateAuthorityPEM: ca, Credential: parts.credential})
		if err != nil {
			return kubeconfigCandidate{}, s.unusablePartsReport(ctx, c, err)
		}
		probe := storedProbe
		if !probe.ran || !bytes.Equal(value, stored.value) {
			result, err := s.params.Prober.Probe(ctx, value)
			probe = probeOutcome{ran: true, result: result, err: err}
		}
		if probe.passed() {
			info, err := managedkubeconfig.Inspect(value)
			if err != nil {
				return kubeconfigCandidate{}, s.unusablePartsReport(ctx, c, err)
			}
			return kubeconfigCandidate{value: value, info: info, probe: probe.result, caFetched: trust == managedkubeconfig.ClusterCA && parts.caFetched}, nil
		}
		failure = probe.err
		logProbeFailure(ctx, "new kubeconfig with trust "+string(trust), probe.err)
		if workloadprobe.ReasonOf(probe.err) != workloadprobe.FailureTLS {
			break
		}
	}
	// No IBM call failed, so the report has no error-class outcome; afterFailedRefresh spaces the next attempt.
	return kubeconfigCandidate{}, &kubeconfigReport{reason: kubeconfigProbeReason(failure),
		message: fmt.Sprintf("A new kubeconfig of IBM cluster %s did not pass the probe against %s: %s", c.ID, server,
			probeFailureText(failure))}
}

// unusablePartsReport reports parts of IBM's admin kubeconfig that do not build a kubeconfig (CredentialFetchFailed: no
// client certificate, a key that does not match it, a CA that does not parse). The error names the field; it goes to
// the controller log only. IBM's answer itself is unusable, which asking again soon does not change, so the next
// attempt waits for terminalRecheck.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - c is the bound cluster.
//   - err is the error of managedkubeconfig.Build or Inspect.
func (s *ControlPlaneScope) unusablePartsReport(ctx context.Context, c *kubernetesservice.Cluster, err error) *kubeconfigReport {
	ctrl.LoggerFrom(ctx).Info("IBM's admin kubeconfig cannot be used", "ibmCluster", c.ID, "error", err.Error())
	reason := controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason
	return &kubeconfigReport{reason: reason, outcome: NotDone(IntervalTerminalRecheck, reason),
		message: fmt.Sprintf("IBM's admin kubeconfig of cluster %s holds no usable client certificate, key or CA (the controller "+
			"log names the cause)", c.ID)}
}

// fetchFailed reports a failed GetAdminKubeconfig or GetCACertificate call as CredentialFetchFailed, with IBM's error
// code only (IBM's incident ID changes with every answer, so it goes to the controller log) and the identity by its
// Description, and maps its error class to the outcome: a 401 rebuilds only this identity's authenticator and is
// retried at the steady interval (5 m), a 403 or a 400 at terminalRecheck (15 m), a 429 after max(Retry-After, 30 s),
// and a 5xx or a network failure returns the error for controller-runtime's backoff. The stored Secret stays as it is.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - clients are this reconcile's IBM clients.
//   - op is the operation.
//   - action says what the call did.
//   - err is the call's error.
func (s *ControlPlaneScope) fetchFailed(ctx context.Context, clients *controlPlaneClients, op kubernetesservice.Operation, action string,
	err error) *kubeconfigReport {
	callErr := &controlPlaneCallError{identity: clients.identity, op: op, action: action, err: err}
	logIBMRefusal(ctx, callErr)
	identity := clients.identity.Description
	reason := controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason
	switch kubernetesservice.Classify(op, err) {
	case kubernetesservice.ClassUnauthorized:
		s.params.ClientBuilder.InvalidateAuthenticator(clients.identity.Key)
		return &kubeconfigReport{reason: reason, outcome: NotDone(IntervalSteadyState, reason),
			message: fmt.Sprintf("IBM Cloud rejected the credentials of %s while %s; CAPIBM rebuilds its authenticator; check "+
				"that the API key of %s is valid and not deleted", identity, action, identity)}
	case kubernetesservice.ClassPermissionDenied:
		return &kubeconfigReport{reason: reason, outcome: NotDone(IntervalTerminalRecheck, reason),
			message: cpPermissionDeniedMessage(identity, action, cpIBMCode(err), s.InfraCluster.Spec.ResourceGroup.ID)}
	case kubernetesservice.ClassBadRequest:
		return &kubeconfigReport{reason: reason, outcome: NotDone(IntervalTerminalRecheck, reason),
			message: fmt.Sprintf("IBM Cloud refused %s with HTTP %d%s; the controller log has IBM's description", action,
				cpIBMStatus(err), cpIBMCode(err))}
	case kubernetesservice.ClassRateLimited:
		return &kubeconfigReport{reason: reason, outcome: RateLimited(kubernetesservice.RateLimitBackoff(err)), transient: true,
			message: fmt.Sprintf("IBM Cloud rate limited CAPIBM (HTTP 429) while %s", action)}
	default:
		return &kubeconfigReport{reason: reason, outcome: Transient(callErr), transient: true,
			message: fmt.Sprintf("CAPIBM could not complete %s%s, and the controller log names the cause", action, cpIBMCode(err))}
	}
}

// storeKubeconfig writes a verified kubeconfig. When it equals the stored one, IBM returned what is stored (IBM keeps
// returning the admin certificate it minted): only the annotations change (refresh-checked-at after a credential
// refresh, ca-fetched-at after a CA fetch, the CA rotation marker, the end of a 401 run, the account). Otherwise the
// value and every CAPIBM annotation are written.
//
// Parameters:
//   - ctx is the reconcile context.
//   - c is the bound cluster.
//   - stored is what the Secret holds.
//   - candidate is the verified kubeconfig.
//   - parts are its pieces.
//   - decision is Decide's answer.
func (s *ControlPlaneScope) storeKubeconfig(ctx context.Context, c *kubernetesservice.Cluster, stored storedKubeconfig,
	candidate kubeconfigCandidate, parts *kubeconfigParts, decision managedkubeconfig.Decision) error {
	now := formatAnnotationTime(s.params.Clock.Now())
	marker := managedkubeconfig.CARotationMarker(c.CARotationStatus, c.CARotationCompletedDate)
	if stored.info != nil && bytes.Equal(candidate.value, stored.value) {
		changes := s.steadyKubeconfigAnnotations(stored, "")
		changes[KubeconfigCARotationMarkerAnnotation] = optional(marker)
		if decision.Refresh {
			changes[KubeconfigRefreshCheckedAtAnnotation] = &now
		}
		if candidate.caFetched {
			changes[KubeconfigCAFetchedAtAnnotation] = &now
		}
		return s.updateKubeconfigAnnotations(ctx, stored, changes)
	}
	annotations, err := KubeconfigSecretAnnotations(s.annotationInput(c, stored, candidate, parts, decision, marker))
	if err != nil {
		return fmt.Errorf("building the annotations of the kubeconfig Secret %s: %w", s.kubeconfigSecretName(), err)
	}
	return s.writeKubeconfigSecret(ctx, stored.secret, candidate.value, annotations)
}

// annotationInput gathers what the annotations of a new kubeconfig record: the fetch times carry over from the stored
// annotations when this reconcile did not fetch the part, and refresh-checked-at is set when a credential refresh got
// the stored certificate back, so that Decide's SameCertificateRetry applies: IBM keeps returning the admin certificate
// it minted.
//
// Parameters:
//   - c is the bound cluster.
//   - stored is what the Secret holds.
//   - candidate is the verified kubeconfig.
//   - parts are its pieces.
//   - decision is Decide's answer.
//   - marker is IBM's current CA rotation marker.
func (s *ControlPlaneScope) annotationInput(c *kubernetesservice.Cluster, stored storedKubeconfig, candidate kubeconfigCandidate,
	parts *kubeconfigParts, decision managedkubeconfig.Decision, marker string) KubeconfigAnnotationInput {
	now := s.params.Clock.Now()
	in := KubeconfigAnnotationInput{ClusterID: c.ID, AccountID: s.accountID(), Info: candidate.info, FetchedAt: stored.fetchedAt,
		CAFetchedAt: stored.caFetchedAt, CARotationMarker: marker}
	if parts.credentialFetched || in.FetchedAt.IsZero() {
		in.FetchedAt = now
	}
	if candidate.caFetched || in.CAFetchedAt.IsZero() {
		in.CAFetchedAt = now
	}
	if decision.Refresh && stored.info != nil && stored.info.Certificate.Fingerprint == candidate.info.Certificate.Fingerprint {
		in.RefreshCheckedAt = now
	}
	return in
}

// writeKubeconfigSecret creates the Secret of the CAPI contract (<cluster>-kubeconfig, type cluster.x-k8s.io/secret,
// the cluster-name label, key value, a controller owner reference to the control plane for clusterctl move and garbage
// collection; kubeconfig.GenerateSecretWithOwner) or patches the one CAPIBM owns: the value, the label, the owner
// reference when the Secret has no controller, and exactly the CAPIBM annotations.
//
// Parameters:
//   - ctx is the reconcile context.
//   - existing is the Secret as read; nil creates it.
//   - value is the verified kubeconfig.
//   - annotations are KubeconfigSecretAnnotations' answer.
func (s *ControlPlaneScope) writeKubeconfigSecret(ctx context.Context, existing *corev1.Secret, value []byte, annotations map[string]string) error {
	owner := *metav1.NewControllerRef(s.ControlPlane, controlplanev1.GroupVersion.WithKind(controlPlaneKind))
	if existing == nil {
		created := kubeconfig.GenerateSecretWithOwner(client.ObjectKey{Namespace: s.Cluster.Namespace, Name: s.Cluster.Name}, value, owner)
		created.Annotations = annotations
		if err := s.params.Client.Create(ctx, created); err != nil {
			return fmt.Errorf("creating the kubeconfig Secret %s: %w", client.ObjectKeyFromObject(created), err)
		}
		s.kubeconfigSecret = created
		return nil
	}
	updated := existing.DeepCopy()
	if updated.Data == nil {
		updated.Data = map[string][]byte{}
	}
	updated.Data[secret.KubeconfigDataName] = value
	metav1.SetMetaDataLabel(&updated.ObjectMeta, clusterv1.ClusterNameLabel, s.Cluster.Name)
	if metav1.GetControllerOf(updated) == nil {
		updated.OwnerReferences = append(updated.OwnerReferences, owner)
	}
	for _, key := range kubeconfigOwnedAnnotations() {
		delete(updated.Annotations, key)
	}
	for key, v := range annotations {
		metav1.SetMetaDataAnnotation(&updated.ObjectMeta, key, v)
	}
	return s.patchKubeconfigSecret(ctx, existing, updated)
}
