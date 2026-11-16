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
	"time"

	corev1 "k8s.io/api/core/v1"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/secret"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"
)

// This file is the proof of possession of DecideBinding rows 2 and 5 (ProvesOwnership) and the stored-kubeconfig probe
// of the delete flow (binding row 4, and the 404 of a later poll after CAPIBM's own DELETE).
//
// The proof answers "does the kubeconfig Secret hold cluster-admin on this candidate cluster?" without trusting anything
// in the Secret but its credential: the kubeconfig is rebuilt from the Secret's user entry with the candidate's own
// master URL and the flavor's trust (the candidate's getCACert CA or the system roots), so the credential is sent only
// to IBM's master of the candidate and a forged server or CA in the Secret cannot pass. No credential is fetched from
// IBM for a candidate that is not proven (getCACert returns a CA only). The proof is credential-identity-agnostic: it
// reads only the access review's answer, never the certificate's subject or the SelfSubjectReview user name, so it
// passes after a key rotation and on a clusterctl move target that runs with other credentials.

// proveOwnership runs the proof of possession for candidate c of DecideBinding rows 2 and 5 and returns its result and,
// when it failed, the failing check for the condition message (BindingInput.ProofFailure), which quotes neither the
// API server nor a user name: the user name of IBM's admin certificate names an IAM identity. The kubeconfig is
// rebuilt with managedkubeconfig.Build, which takes only an inline client certificate and key, so a Secret whose user
// holds a bearer token, an exec plugin or an auth-provider cannot be proven: that is reported as a failed proof that
// names the client-certificate kubeconfig the proof needs.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - c is the candidate GetCluster returned; it passed the candidate checks.
//
// It returns a *controlPlaneCallError when getCACert failed, and any other error when the Secret could not be read; the
// binding is then decided again on the next reconcile.
func (s *ControlPlaneScope) proveOwnership(ctx context.Context, clients *controlPlaneClients, c *kubernetesservice.Cluster) (OwnershipProof, string, error) {
	stored, err := s.loadKubeconfigSecret(ctx)
	if err != nil {
		return "", "", err
	}
	if stored == nil {
		return OwnershipProofFailed, fmt.Sprintf("there is no Secret %s", s.kubeconfigSecretName()), nil
	}
	credential, err := managedkubeconfig.ExtractClientCredential(stored.Data[secret.KubeconfigDataName])
	if err != nil {
		return OwnershipProofFailed, credentialProblem(err), nil
	}
	server, err := ManagementServerURL(s.ControlPlane.Spec.Distribution, c)
	if err != nil {
		return OwnershipProofFailed, masterURLProblemText(err), nil
	}
	return s.probeOwnership(ctx, clients, c, server, credential)
}

// masterURLProblemText says that the candidate has no master URL the proof can use. The URL itself is IBM's (it is
// named by the endpoint step once the cluster is bound), so the proof's text stays the same for every bad value. err is
// ManagementServerURL's error.
func masterURLProblemText(err error) string {
	if err == nil {
		return ""
	}
	return "IBM reports no master URL of the cluster that CAPIBM can use"
}

// credentialProblem describes a Secret whose credential the proof cannot use. err is the error of
// managedkubeconfig.ExtractClientCredential; its text names kubeconfig entries and stays in no condition.
func credentialProblem(err error) string {
	if errors.Is(err, managedkubeconfig.ErrUnsupportedCredential) {
		return "the Secret's kubeconfig user holds no inline client certificate and key; a bearer token, an exec plugin or an " +
			"auth-provider cannot be rebuilt for the cluster's master URL, so the proof needs a client-certificate kubeconfig " +
			"(ibmcloud ks cluster config --admin)"
	}
	return "the Secret's value is not a kubeconfig whose current context names a user"
}

// probeOwnership rebuilds the kubeconfig for the candidate with the flavor's trust mode and, after a TLS failure, the
// other one (the trust of a private-only master is not yet verified against IBM Cloud), probes it, and reads the
// cluster-admin access review (ProvesOwnership).
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - c is the candidate.
//   - server is the candidate's master URL (ManagementServerURL), never the Secret's server.
//   - credential is the Secret's client certificate and key.
func (s *ControlPlaneScope) probeOwnership(ctx context.Context, clients *controlPlaneClients, c *kubernetesservice.Cluster, server string,
	credential managedkubeconfig.ClientCredential) (OwnershipProof, string, error) {
	primary := CATrustFor(s.ControlPlane.Spec.Distribution)
	var ca []byte
	for _, trust := range []managedkubeconfig.CATrust{primary, primary.Other()} {
		in := managedkubeconfig.Input{ClusterName: s.Cluster.Name, Server: server, Trust: trust, Credential: credential}
		if trust == managedkubeconfig.ClusterCA {
			if ca == nil {
				answer, err := clients.kubernetes.GetCACertificate(ctx, c.ID)
				if err != nil {
					return "", "", &controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpGetCACertificate,
						action: "fetching the CA of IBM cluster " + c.ID + " for the ownership proof", err: err}
				}
				ca = answer.PEM
			}
			in.CertificateAuthorityPEM = ca
		}
		value, err := managedkubeconfig.Build(in)
		if err != nil {
			ctrl.LoggerFrom(ctx).Info("The ownership proof cannot build a kubeconfig", "ibmCluster", c.ID, "error", err.Error())
			return OwnershipProofFailed, "the Secret's credential and the cluster's CA do not form a usable kubeconfig", nil
		}
		result, err := s.params.Prober.Probe(ctx, value)
		if err == nil {
			if ProvesOwnership(result) {
				return OwnershipProofProven, "", nil
			}
			return OwnershipProofFailed, "the SelfSubjectAccessReview for verb * on resource * in group * was not allowed", nil
		}
		logProbeFailure(ctx, "ownership proof with trust "+string(trust), err)
		if workloadprobe.ReasonOf(err) != workloadprobe.FailureTLS {
			return OwnershipProofFailed, probeFailureText(err), nil
		}
	}
	return OwnershipProofFailed, fmt.Sprintf("the server certificate of %s did not verify with the cluster CA or the system roots", server), nil
}

// The delete flow's stored-kubeconfig probe: a 404 from IBM alone does not prove that a known cluster is gone.

// keepWhileKubeconfigAuthenticates keeps the finalizer of a deleting control plane whose IBM cluster ID IBM answers
// with 404 (binding row 4, and the 404 of a later poll after CAPIBM's own DELETE) while the CAPIBM-owned kubeconfig
// Secret recorded for that ID still authenticates against its stored server. IBM may answer 404 instead of 403 when
// the credentials CAPIBM acts with cannot see the cluster (another account or IAM scope, for example after a
// clusterctl move to a management cluster with other credentials; not yet verified against IBM Cloud), and removing
// the finalizer then would orphan a live cluster: Deleting is IBMClusterNotVisible, which names the identity by its
// Description, never by an IAM ID, and the account recorded on the Secret, and no DELETE is sent. It applies whether
// the ID came from status.clusterID (the trusted ID, row 1) or from the Secret's cluster-id with empty status (row 2).
// A probe that fails is judged by DecideNotFoundProbe: only an answer that the kubeconfig no longer authenticates
// removes the finalizer at once, unless CAPIBM's own DELETE preceded the 404 (keepWhileProbeInconclusive). kept is
// false when the finalizer may go; the caller then removes it as before.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
func (s *ControlPlaneScope) keepWhileKubeconfigAuthenticates(ctx context.Context, clients *controlPlaneClients) (Outcome, bool) {
	stored, err := s.loadKubeconfigSecret(ctx)
	if err != nil {
		// Without the Secret CAPIBM cannot tell a cluster it cannot see from one that is gone: keep the finalizer, retry.
		return s.deleteTransient(Transient(err), "CAPIBM could not read the kubeconfig Secret to check whether the IBM cluster "+
			"is gone; it retries"), true
	}
	clusterID := s.ControlPlane.Status.ClusterID
	if clusterID == "" && stored != nil {
		clusterID = stored.Annotations[controlplanev1.KubeconfigClusterIDAnnotation]
	}
	if !storedKubeconfigFor(stored, s.ControlPlane, clusterID) {
		return Outcome{}, false
	}
	if _, err := s.params.Prober.Probe(ctx, stored.Data[secret.KubeconfigDataName]); err != nil {
		logProbeFailure(ctx, "stored kubeconfig of a cluster IBM reports as not found", err)
		return s.keepWhileProbeInconclusive(clients, clusterID, stored, err)
	}
	reason := controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason
	message := IBMClusterNotVisibleMessage(clients.identity.Description, clusterID, recordedAccount(stored))
	interval := IntervalTerminalRecheck
	if requested := s.ControlPlane.Status.DeletionRequestTime; !requested.IsZero() {
		// CAPIBM's own DELETE preceded the 404: IBM was observed to answer 404 about 5 minutes after it, and the master
		// may answer for a while longer, so the check is repeated at the delete cadence rather than after a human's
		// terminalRecheck.
		interval = IntervalDelete
		message += fmt.Sprintf(". CAPIBM requested its deletion at %s; this clears once IBM has removed its master", bindingTime(requested.Time))
	}
	s.setDeleting(reason, message)
	return NotDone(interval, reason), true
}

// NotFoundProbeVerdict is what a failed probe of the stored kubeconfig means for a known IBM cluster that IBM answers
// with 404 in the delete flow (binding row 4, and the 404 of a later poll after CAPIBM's own DELETE).
type NotFoundProbeVerdict string

const (
	// NotFoundProbeGone means: the cluster counts as gone and the finalizer goes. The kubeconfig no longer authenticates
	// (401, a server certificate that no longer verifies, a value that is not a kubeconfig), or CAPIBM's own DELETE
	// preceded the 404, after which an API server that does not answer is the expected sign that IBM removed the master
	// (IBM was observed to answer 404 about 5 minutes after the DELETE).
	NotFoundProbeGone NotFoundProbeVerdict = "Gone"
	// NotFoundProbeStillNotVisible means: the kubeconfig authenticated against the cluster in an earlier reconcile
	// (IBMClusterNotVisible) and cannot be checked now. The cluster may still run, so the finalizer stays until the
	// kubeconfig is rejected or a human removes it.
	NotFoundProbeStillNotVisible NotFoundProbeVerdict = "StillNotVisible"
	// NotFoundProbeRecheck means: the kubeconfig could not be checked yet; within DeleteLookupGracePeriod of the start of
	// the deletion CAPIBM checks again at deletePoll before it counts the cluster as gone.
	NotFoundProbeRecheck NotFoundProbeVerdict = "Recheck"
)

// DecideNotFoundProbe judges a failed probe of the stored kubeconfig of a known IBM cluster that IBM answers with 404.
// IBM may answer 404 for a live cluster that the credentials CAPIBM acts with cannot see (not yet verified against IBM
// Cloud), so a probe that says nothing about the credential, because the API server did not answer (Unreachable: a
// timeout, a DNS failure) or did not answer with success (Other: a 5xx, a 429), must not orphan that cluster on a
// single network blip. Such an inconclusive probe keeps the finalizer when the kubeconfig was seen authenticating
// before, and otherwise while the deletion is younger than DeleteLookupGracePeriod; a cluster that was deleted out of
// band, whose master never answers again, then still loses its finalizer after that grace period.
//
// Parameters:
//   - failure is the probe failure's class (workloadprobe.ReasonOf).
//   - deleteRequested is true when CAPIBM's own DELETE preceded the 404 (status.deletionRequestTime is set).
//   - seenAuthenticating is true when an earlier reconcile reported IBMClusterNotVisible (the Deleting reason as read).
//   - withinGrace is true while the control plane's deletionTimestamp is less than DeleteLookupGracePeriod ago.
func DecideNotFoundProbe(failure workloadprobe.FailureReason, deleteRequested, seenAuthenticating, withinGrace bool) NotFoundProbeVerdict {
	inconclusive := failure == workloadprobe.FailureUnreachable || failure == workloadprobe.FailureOther
	switch {
	case deleteRequested || !inconclusive:
		return NotFoundProbeGone
	case seenAuthenticating:
		return NotFoundProbeStillNotVisible
	case withinGrace:
		return NotFoundProbeRecheck
	default:
		return NotFoundProbeGone
	}
}

// keepWhileProbeInconclusive applies DecideNotFoundProbe to a failed probe of the stored kubeconfig and reports a kept
// finalizer in Deleting: IBMClusterNotVisible (rechecked at terminalRecheck, like a passing probe) when the kubeconfig
// was seen authenticating, and WaitingForIBMDeletion with the end of the grace period (rechecked at deletePoll) when it
// was not yet. kept is false when the finalizer may go.
//
// Parameters:
//   - clients are this reconcile's IBM clients.
//   - clusterID is the known IBM cluster ID.
//   - stored is the kubeconfig Secret.
//   - probeErr is the probe's error.
func (s *ControlPlaneScope) keepWhileProbeInconclusive(clients *controlPlaneClients, clusterID string, stored *corev1.Secret,
	probeErr error) (Outcome, bool) {
	cp := s.ControlPlane
	notVisible := controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason
	// The control plane is read uncached (APIReader), so its Deleting reason is the report of the latest reconcile.
	seen := conditions.GetReason(cp, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition) == notVisible
	var graceEnds time.Time
	if cp.DeletionTimestamp != nil {
		graceEnds = cp.DeletionTimestamp.Add(DeleteLookupGracePeriod)
	}
	withinGrace := s.params.Clock.Now().Before(graceEnds)
	identity := clients.identity.Description
	switch DecideNotFoundProbe(workloadprobe.ReasonOf(probeErr), !cp.Status.DeletionRequestTime.IsZero(), seen, withinGrace) {
	case NotFoundProbeStillNotVisible:
		s.setDeleting(notVisible, IBMClusterNotCheckedMessage(identity, clusterID, recordedAccount(stored), probeFailureText(probeErr)))
		return NotDone(IntervalTerminalRecheck, notVisible), true
	case NotFoundProbeRecheck:
		waiting := controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason
		s.setDeleting(waiting, fmt.Sprintf("IBM answers 404 for cluster %s, and its kubeconfig Secret could not be checked against "+
			"it (%s), so CAPIBM cannot yet tell a cluster that the credentials of %s cannot see from one that is gone; it checks "+
			"again every minute until %s and then counts the cluster as gone", clusterID, probeFailureText(probeErr), identity,
			bindingTime(graceEnds)))
		return NotDone(IntervalDelete, waiting), true
	default:
		return Outcome{}, false
	}
}

// IBMClusterNotCheckedMessage is the Deleting message of a known IBM cluster that IBM answers with 404 while its stored
// kubeconfig, which authenticated against it before, cannot be checked now (DecideNotFoundProbe,
// NotFoundProbeStillNotVisible). Like IBMClusterNotVisibleMessage it names the identity only by its Description, never
// by an IAM ID or e-mail address, and the recorded account; the probe is named by its step and class only, never by
// the API server's answer.
//
// Parameters:
//   - identity is the Description of the identity CAPIBM acts with.
//   - clusterID is the known IBM cluster ID.
//   - accountID is the recorded account; "" when none is recorded.
//   - probeText is probeFailureText of the failed probe.
func IBMClusterNotCheckedMessage(identity, clusterID, accountID, probeText string) string {
	account := "its IBM Cloud account"
	if accountID != "" {
		account = "account " + accountID
	}
	return fmt.Sprintf("The credentials of %s cannot see IBM cluster %s in %s; its kubeconfig Secret authenticated against it "+
		"before but cannot be checked now (%s); check the account and IAM scope of those credentials. CAPIBM keeps the "+
		"finalizer %s and never deletes a cluster it cannot see; remove the finalizer by hand to orphan the IBM cluster",
		identity, clusterID, account, probeText, controlplanev1.IBMCloudManagedControlPlaneFinalizer)
}

// storedKubeconfigFor reports whether stored is a kubeconfig Secret CAPIBM owns, recorded for clusterID, with a
// kubeconfig to probe. A user-provided Secret proves nothing about which cluster its credential belongs to.
//
// Parameters:
//   - stored is the Secret; nil is none.
//   - cp is the control plane.
//   - clusterID is the known IBM cluster ID.
func storedKubeconfigFor(stored *corev1.Secret, cp *controlplanev1.IBMCloudManagedControlPlane, clusterID string) bool {
	return stored != nil && clusterID != "" && kubeconfigSecretOwned(stored, cp) &&
		stored.Annotations[controlplanev1.KubeconfigClusterIDAnnotation] == clusterID &&
		len(stored.Data[secret.KubeconfigDataName]) > 0
}

// maxRecordedAccountLength bounds the account-id annotation that a message quotes. IBM Cloud account IDs are 32
// hexadecimal characters; the bound leaves room for another shape, while an annotation (up to 256 KiB) could otherwise
// push the Deleting message past the CRD's limit of 32768 characters on a condition message, and every status patch of
// the deleting control plane would fail.
const maxRecordedAccountLength = 64

// recordedAccount returns the account-id annotation of a kubeconfig Secret for a message, or "" when it is absent or not
// an account ID (letters and digits only, at most maxRecordedAccountLength of them): anyone in the namespace can write
// the annotation. stored is the Secret.
func recordedAccount(stored *corev1.Secret) string {
	account := stored.Annotations[KubeconfigAccountIDAnnotation]
	if account == "" || len(account) > maxRecordedAccountLength || !isAlphanumeric(account) {
		return ""
	}
	return account
}

// IBMClusterNotVisibleMessage is the Deleting message of a known IBM cluster that IBM answers with 404 while the stored
// kubeconfig still authenticates against it. It names the identity only by its Description, never by an IAM ID or
// e-mail address, and the account from the Secret's account-id annotation, which CAPIBM read from the cluster CRN, and
// the two ways out.
//
// Parameters:
//   - identity is the Description of the identity CAPIBM acts with.
//   - clusterID is the known IBM cluster ID.
//   - accountID is the recorded account; "" when none is recorded.
func IBMClusterNotVisibleMessage(identity, clusterID, accountID string) string {
	account := "its IBM Cloud account"
	if accountID != "" {
		account = "account " + accountID
	}
	return fmt.Sprintf("The credentials of %s cannot see IBM cluster %s in %s, but its kubeconfig Secret still authenticates "+
		"against it; check the account and IAM scope of those credentials. CAPIBM keeps the finalizer %s and never deletes a "+
		"cluster it cannot see; remove the finalizer by hand to orphan the IBM cluster", identity, clusterID, account,
		controlplanev1.IBMCloudManagedControlPlaneFinalizer)
}
