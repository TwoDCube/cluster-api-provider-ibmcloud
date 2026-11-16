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

package v1alpha1

import (
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// This file is the condition catalogue of IBMCloudManagedControlPlane. Only condition types and reasons live here;
// IBM text goes into the condition message (normalised, with the IBM incident ID), never into a reason, so reasons
// stay stable CamelCase tokens that tooling can match.
//
// Naming follows the CAPI v1beta2 constants and api/managed/v1alpha1:
//   - a condition type is IBMCloudManagedControlPlane<Condition>Condition;
//   - a reason is IBMCloudManagedControlPlane<Condition><Reason>Reason;
//   - where the reason repeats the condition's last word, that word is written once (Ready on KubeconfigReady is
//     IBMCloudManagedControlPlaneKubeconfigReadyReason);
//   - a reason Not<Condition> is IBMCloudManagedControlPlaneNot<Condition>Reason.
//
// The binding reasons (BindingMismatch, OwnershipNotProven) are about which IBM cluster this object stands for, never
// about IAM credentials. No condition or reason here names a credential identity, so that per-cluster credentials can
// be added later without renaming any of them.

// Ready and Paused.
const (
	// IBMCloudManagedControlPlaneReadyCondition is the summary of IBMClusterProvisioned, KubeconfigReady and Available,
	// computed with util/conditions.SetSummaryCondition.
	IBMCloudManagedControlPlaneReadyCondition = clusterv1.ReadyCondition

	// IBMCloudManagedControlPlanePausedCondition reports whether reconciliation is paused (util/paused).
	IBMCloudManagedControlPlanePausedCondition = clusterv1.PausedCondition
)

// Available is the CAPI control plane contract condition; CAPI mirrors it to the Cluster's ControlPlaneAvailable.
const (
	// IBMCloudManagedControlPlaneAvailableCondition is True when the control plane is initialized, the last workload probe
	// passed, the IBM cluster exists and the master health is not error or unavailable. Ingress health is never
	// consulted: IBM Cloud was observed to report it as warning or critical on healthy clusters with fewer than two
	// workers per zone or without a public gateway.
	IBMCloudManagedControlPlaneAvailableCondition = clusterv1.AvailableCondition

	// IBMCloudManagedControlPlaneAvailableReason surfaces when the control plane is available.
	IBMCloudManagedControlPlaneAvailableReason = clusterv1.AvailableReason

	// IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason surfaces while no IBM cluster exists yet or it is
	// still provisioning.
	IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason = "WaitingForIBMCluster"

	// IBMCloudManagedControlPlaneAvailableMasterNotReadyReason surfaces when IBM does not report the master ready, or
	// reports its health as error or unavailable.
	IBMCloudManagedControlPlaneAvailableMasterNotReadyReason = "MasterNotReady"

	// IBMCloudManagedControlPlaneAvailableWorkloadAPIUnreachableReason surfaces when the probe with the stored kubeconfig
	// fails (network, TLS, or a 401 while the credential is refreshed).
	IBMCloudManagedControlPlaneAvailableWorkloadAPIUnreachableReason = "WorkloadAPIUnreachable"

	// IBMCloudManagedControlPlaneAvailableDeletingReason surfaces while the control plane is being deleted.
	IBMCloudManagedControlPlaneAvailableDeletingReason = clusterv1.DeletingReason
)

// IBMClusterProvisioned reports whether the IBM cluster exists and is the one this object stands for.
const (
	// IBMCloudManagedControlPlaneIBMClusterProvisionedCondition is True once the IBM cluster exists and is bound to this
	// object.
	IBMCloudManagedControlPlaneIBMClusterProvisionedCondition = "IBMClusterProvisioned"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedReason surfaces when the IBM cluster exists and is ours; the message
	// lists the codes of the non-critical errors IBM returned with a successful create, if any.
	IBMCloudManagedControlPlaneIBMClusterProvisionedReason = "Provisioned"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason surfaces while the Cluster's
	// IBMCloudManagedCluster has not resolved the VPC and subnets.
	IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason = "WaitingForInfrastructure"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason surfaces while no
	// IBMCloudManagedMachinePool with spec.poolName "default" belongs to the Cluster (IBM creates the first pool with the
	// cluster).
	IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason = "WaitingForInitialWorkerPool"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason surfaces for an invalid name, CIDR or
	// initial pool, and for IBM 400 answers such as P4102 and E7278.
	IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason = "InvalidConfiguration"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedVersionNotAvailableReason surfaces when IBM does not offer
	// spec.distributionVersion.
	IBMCloudManagedControlPlaneIBMClusterProvisionedVersionNotAvailableReason = "VersionNotAvailable"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason surfaces when an IBM cluster with the name
	// exists and CAPIBM cannot prove it created it for this Cluster.
	IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason = "ClusterNameConflict"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason surfaces when status.clusterID names an IBM
	// cluster whose name, resource group, type or provider differ from this object; nothing is mutated.
	IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason = "BindingMismatch"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason surfaces when the kubeconfig Secret names a
	// cluster but its credential does not pass the cluster-admin access review there.
	IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason = "OwnershipNotProven"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason surfaces while IBM reports requested or deploying, or
	// during the read-after-create wait.
	IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason = "Creating"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedCreateFailedReason surfaces when IBM reports create_failed or
	// deploy_failed; CAPIBM never deletes the cluster on its own.
	IBMCloudManagedControlPlaneIBMClusterProvisionedCreateFailedReason = "CreateFailed"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason surfaces when a known cluster ID or endpoint
	// returns 404; the cluster is never recreated.
	IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason = "IBMClusterNotFound"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedDeletedOutOfBandReason surfaces when IBM reports the cluster deleting
	// or deleted while CAPI is not deleting it.
	IBMCloudManagedControlPlaneIBMClusterProvisionedDeletedOutOfBandReason = "DeletedOutOfBand"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason surfaces for IBM 403 answers and A0656; the
	// message names the missing role.
	IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason = "PermissionDenied"

	// IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason surfaces any other classified IBM Cloud API error.
	IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason = "IBMAPIError"
)

// KubeconfigReady reports the <cluster>-kubeconfig Secret.
const (
	// IBMCloudManagedControlPlaneKubeconfigReadyCondition is True while a verified kubeconfig Secret exists or a
	// user-provided Secret is respected.
	IBMCloudManagedControlPlaneKubeconfigReadyCondition = "KubeconfigReady"

	// IBMCloudManagedControlPlaneKubeconfigReadyReason surfaces when the controller-written Secret is verified.
	IBMCloudManagedControlPlaneKubeconfigReadyReason = clusterv1.ReadyReason

	// IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretReason surfaces when a user-provided Secret is respected
	// and never modified.
	IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretReason = "UserProvidedSecret"

	// IBMCloudManagedControlPlaneKubeconfigReadyWaitingForMasterReason surfaces while the master is not ready.
	IBMCloudManagedControlPlaneKubeconfigReadyWaitingForMasterReason = "WaitingForMaster"

	// IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason surfaces when IBM's admin kubeconfig or CA could
	// not be fetched, or the credential is not a client certificate.
	IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason = "CredentialFetchFailed" //nolint:gosec // G101: a condition reason token, not a credential.

	// IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason surfaces when a new credential failed the
	// SelfSubjectReview with every trust mode; the old Secret is kept.
	IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason = "VerificationFailed"

	// IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason surfaces when the management cluster cannot
	// connect to the master URL (private-only clusters need private connectivity).
	IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason = "EndpointUnreachable"

	// IBMCloudManagedControlPlaneKubeconfigReadyCARotationInProgressReason surfaces (with status Unknown) while IBM rotates
	// the cluster CA.
	IBMCloudManagedControlPlaneKubeconfigReadyCARotationInProgressReason = "CARotationInProgress"

	// IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretInvalidReason surfaces when a user-provided Secret lacks
	// the cluster-name label or a non-empty value key; the Secret is never modified.
	IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretInvalidReason = "UserProvidedSecretInvalid"
)

// MasterVersionUpToDate reports whether the master runs spec.distributionVersion.
const (
	// IBMCloudManagedControlPlaneMasterVersionUpToDateCondition is True when the observed master minor equals
	// spec.distributionVersion, whatever patch IBM runs.
	IBMCloudManagedControlPlaneMasterVersionUpToDateCondition = "MasterVersionUpToDate"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateReason surfaces when the master runs the requested minor.
	IBMCloudManagedControlPlaneMasterVersionUpToDateReason = clusterv1.UpToDateReason

	// IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason surfaces while a requested minor update is in
	// flight at IBM.
	IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason = "UpgradePending"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason surfaces when IBM returned the master to the old
	// minor after an update; the message names the retry time.
	IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason = "UpgradeFailed"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason surfaces when spec.distributionVersion is
	// more than one minor above the master.
	IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason = "VersionSkipNotAllowed"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateDowngradeNotAllowedReason surfaces when spec.distributionVersion is
	// below the master minor.
	IBMCloudManagedControlPlaneMasterVersionUpToDateDowngradeNotAllowedReason = "DowngradeNotAllowed"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason surfaces when IBM does not offer the
	// requested minor.
	IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason = "VersionNotAvailable"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason surfaces while a CA rotation blocks the
	// update.
	IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason = "BlockedByCARotation"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason surfaces while workers lag the master
	// further than the update allows.
	IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason = "BlockedByWorkerSkew"

	// IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason surfaces while the IBM cluster state
	// blocks master updates.
	IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason = "BlockedByClusterState"
)

// RollingOut is the CAPI contract condition that CAPI aggregates into the Cluster's RollingOut.
const (
	// IBMCloudManagedControlPlaneRollingOutCondition is True while IBM updates the master, for a requested minor or for a
	// patch IBM applies on its own.
	IBMCloudManagedControlPlaneRollingOutCondition = clusterv1.RollingOutCondition

	// IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason surfaces while a master update is pending or in
	// progress at IBM.
	IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason = "MasterUpdateInProgress"

	// IBMCloudManagedControlPlaneNotRollingOutReason surfaces when no master update is in progress.
	IBMCloudManagedControlPlaneNotRollingOutReason = clusterv1.NotRollingOutReason
)

// Deleting follows the CAPI v1beta2 polarity: False/NotDeleting without a deletionTimestamp, True with the specific
// reason while deleting.
const (
	// IBMCloudManagedControlPlaneDeletingCondition reports the progress of the deletion.
	IBMCloudManagedControlPlaneDeletingCondition = clusterv1.DeletingCondition

	// IBMCloudManagedControlPlaneNotDeletingReason surfaces while the object has no deletionTimestamp.
	IBMCloudManagedControlPlaneNotDeletingReason = clusterv1.NotDeletingReason

	// IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason surfaces while a MachinePool of the Cluster still
	// exists, so the IBM cluster is not deleted yet.
	IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason = "WaitingForWorkersDeletion"

	// IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason surfaces when the Cluster's IBMCloudManagedCluster is
	// gone while an IBM cluster may exist; the finalizer is kept, because the credentials and the resource group CAPIBM
	// needs to find and delete the IBM cluster come only from the IBMCloudManagedCluster.
	IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason = "WaitingForInfrastructure"

	// IBMCloudManagedControlPlaneDeletingDeletionRequestedReason surfaces after DELETE was sent.
	IBMCloudManagedControlPlaneDeletingDeletionRequestedReason = "DeletionRequested"

	// IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason surfaces while IBM deletes the cluster, including
	// when IBM answered a DELETE with 409 or 400.
	IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason = "WaitingForIBMDeletion"

	// IBMCloudManagedControlPlaneDeletingDeleteFailedReason surfaces when IBM reports delete_failed.
	IBMCloudManagedControlPlaneDeletingDeleteFailedReason = "DeleteFailed"

	// IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason surfaces when a same-named IBM cluster may have been
	// created by an attempt with an unknown outcome; the finalizer is kept and DELETE is never sent for it.
	IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason = "PossibleOwnClusterFound"

	// IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason surfaces when the cluster to delete cannot be proven to be
	// ours.
	IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason = "OwnershipNotProven"

	// IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason surfaces when a known cluster ID returns 404 while the
	// stored kubeconfig still authenticates, so the credentials cannot see the cluster.
	IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason = "IBMClusterNotVisible"

	// IBMCloudManagedControlPlaneDeletingPermissionDeniedReason surfaces for an IBM 403 during deletion.
	IBMCloudManagedControlPlaneDeletingPermissionDeniedReason = "PermissionDenied"
)
