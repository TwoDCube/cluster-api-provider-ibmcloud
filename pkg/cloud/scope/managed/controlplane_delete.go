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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file is the control plane's delete flow. Deleting the control plane deletes the IBM cluster, and IBM's DELETE
// cascades to every worker pool, so the flow first waits until no worker pool of the Cluster is left; in a Cluster
// deletion CAPI has already removed them, and only a control plane deleted on its own ever waits here. The IBM
// cluster to delete is the one DecideBinding binds in delete mode: never a same-named cluster CAPIBM cannot prove it
// created. Nothing in this flow is Terminal: every answer either removes the finalizer or is checked again later, and
// a human-needed state keeps the finalizer with a condition that names the way out.

// The events of the delete flow.
const (
	// EventReasonLeftoverResources is the Warning event, once the IBM cluster is gone, that names the VPC resources IKS
	// leaves behind and says what happened to the cluster's storage (LeftoverResourcesMessage).
	EventReasonLeftoverResources = "LeftoverResources"
	// EventReasonIBMClusterNotDeleted is the Warning event that names a same-named IBM cluster the deletion leaves alone,
	// because CAPIBM cannot prove it created it for this Cluster (DecideBinding rows 7 and 10).
	EventReasonIBMClusterNotDeleted = "IBMClusterNotDeleted"
)

// cpDeleteMaxNamedWorkers bounds the pool names in the WaitingForWorkersDeletion message, so that a Cluster with many
// pools does not produce an oversized condition.
const cpDeleteMaxNamedWorkers = 5

// ReconcileDelete runs the delete flow for a control plane whose deletion is not part of clusterctl move (the
// controller removes the finalizer for that without calling the scope). It reports progress in the Deleting
// condition (True, with the specific reason) and Available (False, Deleting). ctx is the reconcile context.
//
// It returns the outcome of the step and whether the finalizer may be removed: true once no IBM cluster of this control
// plane is left (nothing to delete, or IBM reports the cluster gone after CAPIBM's DELETE), or the cluster cannot be
// one of this control plane's. The finalizer stays while the IBM cluster may exist and CAPIBM cannot delete it.
func (s *ControlPlaneScope) ReconcileDelete(ctx context.Context) (Outcome, bool) {
	cp := s.ControlPlane
	s.setAvailable(controlplanev1.IBMCloudManagedControlPlaneAvailableDeletingReason, "The control plane is being deleted")
	if NoIBMClusterCanExist(cp) {
		// NoIBMClusterCanExist, the same test the admission webhooks apply to a delete: CAPIBM never sent a create for this
		// control plane and never bound it, so its deletion cannot leave an IBM cluster behind, and a same-named IBM cluster
		// is never this control plane's (DecideBinding rows 7 and 10). The finalizer goes whatever IBM answers, so that a
		// control plane that never reached IBM can be deleted even when its credentials or its IBMCloudManagedCluster are
		// broken; the lookup only names a same-named cluster.
		s.warnSameNamedCluster(ctx)
		return Done(), true
	}

	workers, err := s.workersBlockingDelete(ctx)
	if err != nil {
		return s.deleteTransient(Transient(err), "CAPIBM could not list the worker pools of the Cluster; it retries"), false
	}
	if len(workers) > 0 {
		s.setDeleting(controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason, cpWorkersMessage(workers, s.Cluster.Name))
		return NotDone(IntervalDelete, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason), false
	}

	infra, err := s.loadInfraCluster(ctx)
	var configErr *controlPlaneConfigError
	if err != nil && !errors.As(err, &configErr) {
		return s.deleteTransient(Transient(err), fmt.Sprintf("CAPIBM could not read %s; it retries", s.infraRefName())), false
	}
	if infra == nil {
		return s.deleteWithoutInfraCluster()
	}

	name, err := EffectiveClusterName(cp.Spec.IBMCloudClusterName, s.Cluster.Name)
	if err != nil {
		// No IBM cluster can carry a name IBM refuses: CAPIBM never sent a create for it, and every binding row checks the
		// name, so this control plane was never bound to an IBM cluster.
		return Done(), true
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.deleteTransient(Transient(err), "CAPIBM cannot resolve the IBM Cloud credentials of this control plane; the "+
			"controller log names the cause"), false
	}
	decision, err := s.decideBinding(ctx, clients, name, true)
	if err != nil {
		return s.deleteCallProblem(ctx, err), false
	}
	return s.actOnDeleteBinding(ctx, clients, decision, name)
}

// deleteWithoutInfraCluster handles a deletion whose IBMCloudManagedCluster is gone. Without it, CAPIBM has neither
// the identity holder nor the resource group of the candidate checks, so it cannot prove which IBM cluster to delete.
// ReconcileDelete has already removed the finalizer of a control plane that cannot have an IBM cluster
// (NoIBMClusterCanExist), so here one may exist: the finalizer stays, with no IBM call.
func (s *ControlPlaneScope) deleteWithoutInfraCluster() (Outcome, bool) {
	reason := controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason
	s.setDeleting(reason, fmt.Sprintf("%s is gone; CAPIBM needs its resource group to prove which IBM cluster to delete; "+
		"re-apply it from its manifest, or remove the finalizer %s by hand to orphan the IBM cluster", s.infraRefName(),
		controlplanev1.IBMCloudManagedControlPlaneFinalizer))
	return NotDone(IntervalDelete, reason), false
}

// actOnDeleteBinding carries out a delete-mode binding decision.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - decision is DecideBinding's answer in delete mode.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) actOnDeleteBinding(ctx context.Context, clients *controlPlaneClients, decision BindingDecision,
	name string) (Outcome, bool) {
	switch decision.Action {
	case BindingKnown:
		return s.deleteKnownCluster(ctx, clients, decision)
	case BindingWait:
		// Row 3 or 9: a create of CAPIBM's may still show up; DeleteLookupGracePeriod bounds both in delete mode.
		s.setDeleting(controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason, decision.Message)
		return NotDone(IntervalDelete, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason), false
	case BindingNothingToDelete:
		return Done(), true
	case BindingTerminal:
		return s.deleteUnprovenCluster(ctx, clients, decision, name)
	default:
		return Transient(fmt.Errorf("managed: binding decision %q (row %s) has no action in the delete flow (programming error)",
			decision.Action, decision.Row)), false
	}
}

// deleteUnprovenCluster handles a terminal binding decision in delete mode: the finalizer stays while the cluster may
// be this control plane's (PossibleOwnClusterFound, BindingMismatch, OwnershipNotProven), and goes when it cannot be
// (a cluster that is gone, or a same-named cluster with no create attempt of unknown outcome). Each kept finalizer's
// message names the ways out that can change the decision.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - decision is a BindingTerminal decision.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) deleteUnprovenCluster(ctx context.Context, clients *controlPlaneClients, decision BindingDecision,
	name string) (Outcome, bool) {
	finalizer := controlplanev1.IBMCloudManagedControlPlaneFinalizer
	switch decision.Reason {
	case controlplanev1.IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason:
		s.setDeleting(decision.Reason, decision.Message)
		return NotDone(IntervalTerminalRecheck, decision.Reason), false
	case controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason:
		// Row 1b is decided from status.clusterID and IBM's answer alone; no kubeconfig Secret and no proof enter it, so
		// a fresh admin kubeconfig changes nothing. The decision changes only when the cluster is deleted in IBM Cloud
		// (a later check then finds it gone, row 4), or the finalizer goes by hand.
		reason := controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason
		s.setDeleting(reason, decision.Message+fmt.Sprintf("; CAPIBM never deletes it. Check it in IBM Cloud: if it belongs to "+
			"this Cluster, delete it there, and CAPIBM removes the finalizer once it finds the cluster gone; otherwise remove "+
			"the finalizer %s by hand, which leaves the IBM cluster alone", finalizer))
		return NotDone(IntervalTerminalRecheck, reason), false
	case controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason:
		// Row 2b is decided by the proof of possession with the kubeconfig Secret's credential, so a fresh admin
		// kubeconfig of that cluster can still bind and then delete it.
		reason := controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason
		s.setDeleting(reason, decision.Message+fmt.Sprintf("; CAPIBM never deletes it: put a fresh admin kubeconfig (ibmcloud ks "+
			"cluster config --admin) into the kubeconfig Secret, or remove the finalizer %s by hand to orphan the IBM cluster",
			finalizer))
		return NotDone(IntervalTerminalRecheck, reason), false
	case controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason:
		if decision.Row == "4" {
			// A known ID that IBM answers with 404 is gone only if its stored kubeconfig no longer authenticates: the 404 may
			// only mean that the credentials cannot see the cluster.
			if outcome, kept := s.keepWhileKubeconfigAuthenticates(ctx, clients); kept {
				return outcome, false
			}
			s.reportGoneCluster(ctx, clients, name)
		}
		return Done(), true
	default:
		// Row 7 without an attempt of unknown outcome: a same-named cluster CAPIBM cannot prove it created.
		if decision.Cluster != nil {
			s.recordNotDeleted(decision.Cluster)
		}
		return Done(), true
	}
}

// deleteKnownCluster deletes the bound IBM cluster: it waits while IBM deletes it, never sends DELETE again for
// delete_failed, and otherwise records status.deletionRequestTime before it sends DELETE, at most once per
// DeleteReissueAfter. Each DELETE carries spec.deletion.persistentStorage as it is when the DELETE is sent
// (storageOnDelete): it asks IBM to keep the storage unless the user asked for Delete. On VPC, IBM's DELETE never
// deletes VPC resources either way, which matters because CAPIBM only references the user's VPC.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - decision is a BindingKnown decision.
func (s *ControlPlaneScope) deleteKnownCluster(ctx context.Context, clients *controlPlaneClients, decision BindingDecision) (Outcome, bool) {
	cp := s.ControlPlane
	c := decision.Cluster
	cp.Status.ClusterID = decision.ClusterID
	label := fmt.Sprintf("IBM cluster %s (ID %s)", c.Name, c.ID)
	waiting := controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason
	requested := controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason

	switch c.State {
	case ibmStateDeleting, ibmStateDeleted, ibmStateAborted:
		s.setDeleting(waiting, fmt.Sprintf("IBM is deleting %s (state %s)", label, c.State))
		return NotDone(IntervalDelete, waiting), false
	case ibmStateDeleteFailed:
		reason := controlplanev1.IBMCloudManagedControlPlaneDeletingDeleteFailedReason
		s.setDeleting(reason, fmt.Sprintf("IBM reports %s in state %s; CAPIBM never sends DELETE again: open an IBM support case "+
			"for cluster %s, and remove the finalizer %s by hand once IBM has deleted it", label, c.State, c.ID,
			controlplanev1.IBMCloudManagedControlPlaneFinalizer))
		return NotDone(IntervalTerminalRecheck, reason), false
	}

	last := cp.Status.DeletionRequestTime
	if !last.IsZero() && s.params.Clock.Now().Before(last.Add(DeleteReissueAfter)) {
		s.setDeleting(requested, fmt.Sprintf("CAPIBM requested the deletion of %s at %s; IBM reports state %s", label,
			bindingTime(last.Time), c.State))
		return NotDone(IntervalDelete, requested), false
	}

	cp.Status.DeletionRequestTime = s.now()
	s.setDeleting(requested, fmt.Sprintf("CAPIBM requests the deletion of %s at %s", label, bindingTime(cp.Status.DeletionRequestTime.Time)))
	if err := s.persist(ctx); err != nil {
		// No DELETE without its time on record: the reissue delay is measured from it.
		cp.Status.DeletionRequestTime = last
		return Transient(fmt.Errorf("recording the deletion request time of %s before the DELETE: %w", label, err)), false
	}
	storage := storageOnDelete(cp.Spec.Deletion.PersistentStorage)
	if err := clients.kubernetes.DeleteCluster(ctx, c.ID, s.InfraCluster.Spec.ResourceGroup.ID, storage); err != nil {
		return s.deleteCallFailed(ctx, clients, label, err), false
	}
	s.setDeleting(requested, fmt.Sprintf("CAPIBM requested the deletion of %s at %s; IBM reports state %s", label,
		bindingTime(cp.Status.DeletionRequestTime.Time), c.State))
	return NotDone(IntervalDelete, requested), false
}

// deleteCallFailed handles an error of DeleteCluster, which is never Terminal in the delete flow: 404 lets the next
// GetCluster decide; 400 or 409 most likely means IBM is already deleting the cluster or runs another operation on it
// (not yet verified against IBM Cloud), so CAPIBM waits; the other classes follow the delete flow's call handling.
// Messages carry IBM's code only (cpIBMCode): a message that changed with IBM's incident ID would rewrite status on
// every reconcile; the controller log has IBM's whole answer.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - clients are this reconcile's IBM clients.
//   - label names the IBM cluster in messages.
//   - err is DeleteCluster's error.
func (s *ControlPlaneScope) deleteCallFailed(ctx context.Context, clients *controlPlaneClients, label string, err error) Outcome {
	callErr := &controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpDeleteCluster, action: "deleting " + label, err: err}
	switch status := cpIBMStatus(err); status {
	case http.StatusNotFound:
		reason := controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason
		s.setDeleting(reason, fmt.Sprintf("IBM answered the deletion of %s with HTTP 404; the next check decides whether it is gone", label))
		return NotDone(IntervalDelete, reason)
	case http.StatusBadRequest, http.StatusConflict:
		logIBMRefusal(ctx, callErr)
		reason := controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason
		s.setDeleting(reason, fmt.Sprintf("IBM answered the deletion of %s with HTTP %d%s, most likely because it already deletes the "+
			"cluster or runs another operation on it; CAPIBM checks again", label, status, cpIBMCode(err)))
		return NotDone(IntervalDelete, reason)
	default:
		return s.deleteCallProblem(ctx, callErr)
	}
}

// deleteCallProblem reports a failed IBM call of the delete flow. Unlike the normal flow nothing is Terminal: a 401
// rebuilds the identity's authenticator and retries at the steady interval, a 403 is PermissionDenied and is checked
// again at terminalRecheck, a 429 backs off, and anything else is retried. Messages carry IBM's code only: a message
// that changed with IBM's incident ID would rewrite status on every reconcile and re-trigger it; the controller log
// has IBM's whole answer.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - err is the failure.
func (s *ControlPlaneScope) deleteCallProblem(ctx context.Context, err error) Outcome {
	var callErr *controlPlaneCallError
	if !errors.As(err, &callErr) {
		return s.deleteTransient(Transient(err), "CAPIBM could not decide which IBM cluster to delete; it retries, and the "+
			"controller log names the cause")
	}
	permissionDenied := controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason
	identity := callErr.identity.Description
	switch kubernetesservice.Classify(callErr.op, callErr.err) {
	case kubernetesservice.ClassUnauthorized:
		logIBMRefusal(ctx, callErr)
		s.params.ClientBuilder.InvalidateAuthenticator(callErr.identity.Key)
		s.setDeleting(permissionDenied, cpUnauthorizedMessage(identity, callErr.action, callErr.err))
		return NotDone(IntervalSteadyState, permissionDenied)
	case kubernetesservice.ClassPermissionDenied:
		logIBMRefusal(ctx, callErr)
		s.setDeleting(permissionDenied, cpPermissionDeniedMessage(identity, callErr.action, cpIBMCode(callErr.err),
			s.InfraCluster.Spec.ResourceGroup.ID))
		return NotDone(IntervalTerminalRecheck, permissionDenied)
	case kubernetesservice.ClassRateLimited:
		return s.deleteTransient(RateLimited(kubernetesservice.RateLimitBackoff(callErr.err)),
			fmt.Sprintf("IBM Cloud rate limited CAPIBM (HTTP 429) while %s; CAPIBM retries", callErr.action))
	default:
		return s.deleteTransient(Transient(callErr), fmt.Sprintf("CAPIBM could not complete %s%s; it retries, and the controller "+
			"log names the cause", callErr.action, cpIBMCode(callErr.err)))
	}
}

// deleteTransient returns outcome for a failure that a retry may fix. It keeps a Deleting reason that an earlier
// reconcile set, because a blip says nothing about the deletion; only when Deleting is not True yet (the first
// reconcile of the deletion) does it set WaitingForIBMDeletion with message, so that Deleting is True while deleting.
//
// Parameters:
//   - outcome is the step's outcome.
//   - message describes the failure.
func (s *ControlPlaneScope) deleteTransient(outcome Outcome, message string) Outcome {
	if !conditions.IsTrue(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition) {
		s.setDeleting(controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason, message)
	}
	return outcome
}

// reportGoneCluster reports binding row 4 in delete mode, an IBM cluster ID that IBM answers with 404, before the
// finalizer goes. What CAPIBM may say depends on where the ID came from:
//   - status.clusterID is CAPIBM's own record, so that cluster existed and IKS leaves its resources in the VPC whoever
//     deleted it (observed on IBM Cloud): the LeftoverResources Warning names them.
//   - The cluster-id annotation of the kubeconfig Secret is untrusted (anyone in the namespace can write it, and it
//     survives a move without status): it proves no cluster, so no deletion is claimed and no resource is named.
//
// Unless CAPIBM's own DELETE preceded the 404 (status.deletionRequestTime, the later-poll case of the delete flow),
// this is row 4 proper, and a same-named IBM cluster in the resource group is named as not deleted
// (recordSameNamedCluster).
// It runs only once keepWhileKubeconfigAuthenticates found no stored CAPIBM kubeconfig of that ID that still authenticates
// (IBMClusterNotVisible keeps the finalizer otherwise).
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) reportGoneCluster(ctx context.Context, clients *controlPlaneClients, name string) {
	cp := s.ControlPlane
	if cp.Status.ClusterID != "" {
		s.recordLeftovers(cp.Status.ClusterID)
	}
	if cp.Status.ClusterID == "" || cp.Status.DeletionRequestTime.IsZero() {
		s.recordSameNamedCluster(ctx, clients, name)
	}
}

// recordLeftovers emits the LeftoverResources Warning once the IBM cluster is gone. clusterID is the deleted cluster's
// ID, always status.clusterID: CAPIBM never names leftovers of a cluster it has no record of.
//
// What the event says about the cluster's storage depends on whether CAPIBM sent a DELETE. status.deletionRequestTime
// is recorded before every DELETE and kept when the DELETE fails, whatever IBM answers (400, 401, 403, 404, 409, 429,
// 5xx) and when the call times out, so it proves only that CAPIBM sent one, not that IBM accepted it: a refused DELETE
// followed by a deletion in the IBM Cloud console ends the same way as an accepted one. The event therefore names what
// CAPIBM's last DELETE asked for (spec.deletion.persistentStorage, read here again) and adds that whoever deleted the
// cluster decided if that DELETE failed; it claims no outcome. Without the time, the cluster was deleted outside
// CAPIBM. A change of the field after CAPIBM's last DELETE would be reported as if that DELETE had carried it; the
// field's documentation asks for it to be set before the Cluster is deleted. Both gaps would close only with a status
// record, written after IBM accepted the DELETE, of the choice that DELETE carried.
func (s *ControlPlaneScope) recordLeftovers(clusterID string) {
	vpcID := ""
	if s.InfraCluster != nil {
		vpcID = s.InfraCluster.Status.VPC.ID
	}
	var sent kubernetesservice.PersistentStorage
	if !s.ControlPlane.Status.DeletionRequestTime.IsZero() {
		sent = storageOnDelete(s.ControlPlane.Spec.Deletion.PersistentStorage)
	}
	s.params.Recorder.Event(s.ControlPlane, corev1.EventTypeWarning, EventReasonLeftoverResources,
		LeftoverResourcesMessage(clusterID, vpcID, s.ControlPlane.Spec.Distribution, sent))
}

// storageOnDelete maps spec.deletion.persistentStorage to the persistent storage choice of IBM's DELETE. Only an
// explicit Delete asks IBM to delete the storage: an omitted value means Retain, and a value the schema would refuse
// keeps the storage too, because deleted data cannot be recovered and must never be deleted on a guess.
//
// Parameters:
//   - policy is spec.deletion.persistentStorage.
func storageOnDelete(policy controlplanev1.PersistentStoragePolicy) kubernetesservice.PersistentStorage {
	if policy == controlplanev1.PersistentStorageDelete {
		return kubernetesservice.DeletePersistentStorage
	}
	return kubernetesservice.RetainPersistentStorage
}

// LeftoverResourcesMessage is the LeftoverResources event of a deleted IBM cluster. The IBMCloudManagedCluster only
// references the user's VPC, so CAPIBM deletes nothing in it and names what IKS leaves there: permanently the shared
// VPE gateways iks-*-<vpcID> and the security group kube-vpegw-<vpcID>, which IBM documents it never deletes, because
// other resources may use them, and possibly the VPE gateway for IAM that IKS adds to every VPC with a cluster since
// November 2026 (https://cloud.ibm.com/docs/containers?topic=containers-vpc-iam-vpe-gateway, which names neither the
// gateway nor its deletion, so the message says "possibly" and names no ID); for about 10 minutes the per-cluster
// load balancers, security groups and VPE gateway, which IKS removes by itself (observed on IBM Cloud). It ends with
// the cluster's data (leftoverStorageText): what CAPIBM's last DELETE asked IBM to do with the storage of its
// persistent volumes, never what IBM did, and that CAPIBM never asks IBM to delete the OpenShift registry bucket in
// the user's Cloud Object Storage instance.
//
// Parameters:
//   - clusterID is the deleted cluster's ID.
//   - vpcID is status.vpc.id of the IBMCloudManagedCluster; "" when unknown, then the names show <vpc-id>.
//   - distribution is spec.distribution; the registry bucket exists for OpenShift only.
//   - sent is the persistent storage choice of CAPIBM's last DELETE, which IBM may have refused, or "" when CAPIBM sent
//     no DELETE for the cluster.
func LeftoverResourcesMessage(clusterID, vpcID string, distribution controlplanev1.Distribution,
	sent kubernetesservice.PersistentStorage) string {
	if vpcID == "" {
		vpcID = "<vpc-id>"
	}
	return fmt.Sprintf("IBM cluster %[1]s is deleted. CAPIBM deletes nothing in VPC %[2]s, so these resources remain there. "+
		"Permanently, because IBM never deletes them, even when the last cluster leaves the VPC (delete them only when no "+
		"cluster or other resource in the VPC uses them): the VPE gateways iks-api-%[2]s, iks-cos-%[2]s, iks-cos-config-%[2]s, "+
		"iks-riaas-%[2]s and iks-registry-%[2]s and the security group kube-vpegw-%[2]s. Possibly also the VPE gateway for "+
		"IAM that IKS adds to every VPC with a cluster since November 2026 (IBM documents neither its name nor whether it "+
		"is ever deleted). Removed by IKS within about 10 minutes, and until then blocking the deletion of the subnets or "+
		"the VPC: the load balancers kube-%[1]s-*, the security groups kube-%[1]s, kube-lbaas-%[1]s and kube-vpegw-%[1]s "+
		"and the VPE gateway iks-%[1]s. %[3]s",
		clusterID, vpcID, leftoverStorageText(distribution, sent))
}

// leftoverStorageText is the last part of the LeftoverResources event: what CAPIBM asked IBM to do with the cluster's
// data. It never says what IBM did, for two reasons:
//   - CAPIBM cannot tell whether IBM accepted its last DELETE (see recordLeftovers), so every variant that names a
//     DELETE adds that whoever deleted the cluster decided if that DELETE failed.
//   - IBM's documentation conflicts on what deleteResources=true removes on VPC. "Removing Block Storage for VPC"
//     (https://cloud.ibm.com/docs/containers?topic=containers-storage-block-vpc-remove, last updated 2026-05-14) says
//     that the storage that a storage class with reclaimPolicy Delete provisioned is deleted with the cluster, while
//     the newer "Removing clusters" (https://cloud.ibm.com/docs/containers?topic=containers-remove, last updated
//     2026-08-03) says in its removal steps that VPC Block Storage is not removed automatically, even with a Delete
//     storage class. Both say that statically provisioned storage and storage of a reclaimPolicy Retain class remain.
//     A user who asked for the deletion must therefore learn that VPC Block volumes may remain, stay billed, and need
//     checking.
//
// Parameters:
//   - distribution is spec.distribution; the registry bucket exists for OpenShift only.
//   - sent is the persistent storage choice of CAPIBM's last DELETE, which IBM may have refused, or "" when CAPIBM sent
//     no DELETE for the cluster.
func leftoverStorageText(distribution controlplanev1.Distribution, sent kubernetesservice.PersistentStorage) string {
	decided := "what happened to the storage of its persistent volumes, such as VPC Block volumes"
	bucket := ""
	if distribution == controlplanev1.DistributionOpenShift {
		decided += ", and to the registry bucket in the Cloud Object Storage instance"
		bucket = ". CAPIBM never asks IBM to delete the registry bucket in the Cloud Object Storage instance"
	}
	// status.deletionRequestTime is kept when the DELETE fails, and a cluster deleted elsewhere afterwards disappears
	// all the same, so a sent DELETE proves nothing about the outcome.
	failed := ". If that DELETE failed, or the cluster was deleted elsewhere, whoever deleted it decided " + decided
	switch sent {
	case kubernetesservice.DeletePersistentStorage:
		return "CAPIBM's last DELETE asked IBM to delete the cluster's persistent storage (spec.deletion.persistentStorage " +
			"Delete, deleteResources=true), but VPC Block volumes may remain: IBM's documentation conflicts on whether they " +
			"go even when a storage class with reclaimPolicy Delete provisioned them, and statically provisioned storage " +
			"and storage of a storage class with reclaimPolicy Retain remain. Check the account for the VPC Block volumes " +
			"named after the cluster's PersistentVolumes (pvc-<uid>) and delete those that remain: they are billed" +
			bucket + failed
	case kubernetesservice.RetainPersistentStorage:
		return "CAPIBM's last DELETE asked IBM to keep the cluster's persistent storage (spec.deletion.persistentStorage " +
			"Retain, also when omitted; deleteResources=false), such as the VPC Block volumes of its persistent volumes, " +
			"which are billed until you delete them" + bucket + failed
	default:
		return "CAPIBM sent no DELETE for this cluster, so whoever deleted it decided " + decided
	}
}

// workersBlockingDelete returns, sorted, the worker pools of the Cluster that must be gone before the IBM cluster is
// deleted: every MachinePool of the Cluster and every IBMCloudManagedMachinePool that still carries the pool
// finalizer. A pool without the finalizer was never owned by a MachinePool, so an orphan cannot block. ctx is the
// reconcile context.
func (s *ControlPlaneScope) workersBlockingDelete(ctx context.Context) ([]string, error) {
	inCluster := []client.ListOption{client.InNamespace(s.Cluster.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: s.Cluster.Name}}
	machinePools := &clusterv1.MachinePoolList{}
	if err := s.params.Client.List(ctx, machinePools, inCluster...); err != nil {
		return nil, fmt.Errorf("listing the MachinePools of Cluster %s: %w", s.Cluster.Name, err)
	}
	pools := &infrav1.IBMCloudManagedMachinePoolList{}
	if err := s.params.Client.List(ctx, pools, inCluster...); err != nil {
		return nil, fmt.Errorf("listing the IBMCloudManagedMachinePools of Cluster %s: %w", s.Cluster.Name, err)
	}
	var workers []string
	for _, mp := range machinePools.Items {
		workers = append(workers, "MachinePool/"+mp.Name)
	}
	for i := range pools.Items {
		if controllerutil.ContainsFinalizer(&pools.Items[i], infrav1.IBMCloudManagedMachinePoolFinalizer) {
			workers = append(workers, "IBMCloudManagedMachinePool/"+pools.Items[i].Name)
		}
	}
	slices.Sort(workers)
	return workers, nil
}

// cpWorkersMessage is the WaitingForWorkersDeletion message; it names at most cpDeleteMaxNamedWorkers pools.
//
// Parameters:
//   - workers are the pools, sorted.
//   - clusterName is the Cluster's name.
func cpWorkersMessage(workers []string, clusterName string) string {
	named := strings.Join(workers, ", ")
	if len(workers) > cpDeleteMaxNamedWorkers {
		named = fmt.Sprintf("%s and %d more", strings.Join(workers[:cpDeleteMaxNamedWorkers], ", "), len(workers)-cpDeleteMaxNamedWorkers)
	}
	return fmt.Sprintf("Waiting for %s of Cluster %s to be deleted: deleting the IBM cluster deletes every worker pool, so it "+
		"is deleted only after them; delete Cluster %s to delete everything in order", named, clusterName, clusterName)
}

// setDeleting sets Deleting to True with reason and message (CAPI v1beta2 polarity).
//
// Parameters:
//   - reason is the Deleting reason.
//   - message describes the deletion's state.
func (s *ControlPlaneScope) setDeleting(reason, message string) {
	conditions.Set(s.ControlPlane, metav1.Condition{
		Type:    controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: message,
	})
}

// warnSameNamedCluster emits the IBMClusterNotDeleted Warning when an IBM cluster with this control plane's name exists
// in the IBMCloudManagedCluster's resource group, so that the operator knows the deletion left it alone. It is best
// effort: without the IBMCloudManagedCluster, credentials or an answer from IBM it only logs, because the deletion of
// a control plane that cannot have an IBM cluster never waits for IBM. ctx is the reconcile context.
func (s *ControlPlaneScope) warnSameNamedCluster(ctx context.Context) {
	log := ctrl.LoggerFrom(ctx)
	name, err := EffectiveClusterName(s.ControlPlane.Spec.IBMCloudClusterName, s.Cluster.Name)
	if err != nil {
		return // IBM refuses such a name, so no IBM cluster carries it.
	}
	infra, err := s.loadInfraCluster(ctx)
	if err != nil || infra == nil {
		log.V(4).Info("Not looking for a same-named IBM cluster: the IBMCloudManagedCluster is not available")
		return
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		log.Info("Not looking for a same-named IBM cluster: the IBM Cloud credentials did not resolve", "error", err.Error())
		return
	}
	s.recordSameNamedCluster(ctx, clients, name)
}

// recordSameNamedCluster looks up an IBM cluster named name in the IBMCloudManagedCluster's resource group and, when
// one exists, emits the IBMClusterNotDeleted Warning for it (DecideBinding rows 4, 7, 8 and 10). It is best effort: a
// failed lookup is only logged, because the deletion it accompanies does not depend on it.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) recordSameNamedCluster(ctx context.Context, clients *controlPlaneClients, name string) {
	c, err := clients.kubernetes.GetCluster(ctx, name, s.InfraCluster.Spec.ResourceGroup.ID)
	if err != nil {
		if kubernetesservice.Classify(kubernetesservice.OpGetCluster, err) != kubernetesservice.ClassNotFound {
			ctrl.LoggerFrom(ctx).Info("Could not look for a same-named IBM cluster", "error", err.Error())
		}
		return
	}
	s.recordNotDeleted(c)
}

// recordNotDeleted emits the IBMClusterNotDeleted Warning for a same-named IBM cluster the deletion leaves alone. c is
// that cluster.
func (s *ControlPlaneScope) recordNotDeleted(c *kubernetesservice.Cluster) {
	s.params.Recorder.Eventf(s.ControlPlane, corev1.EventTypeWarning, EventReasonIBMClusterNotDeleted,
		"IBM cluster %s (ID %s, created %s) was not deleted: CAPIBM cannot prove it created it for this Cluster", c.Name, c.ID,
		bindingCreatedDate(c))
}
