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

// This file is the condition catalogue of the kinds in this package. Only condition types and reasons live here; IBM
// text goes into the condition message, never into a reason, so reasons stay stable CamelCase tokens that tooling can
// match.
//
// Naming follows the CAPI v1beta2 constants (for example ClusterInfrastructureReadyReason,
// ClusterDeletingWaitingForWorkersDeletionReason, KubeadmControlPlaneNotRollingOutReason):
//   - a condition type is <Kind><Condition>Condition;
//   - a reason is <Kind><Condition><Reason>Reason;
//   - where the reason starts with the condition's last word, that word is written once (Ready on NetworkReady is
//     IBMCloudManagedClusterNetworkReadyReason, DrainingNode on Draining is IBMCloudManagedMachinePoolDrainingNodeReason);
//   - a reason Not<Condition> is <Kind>Not<Condition>Reason.
//
// The binding reasons of the control plane are about which IBM cluster an object stands for, never about IAM
// credentials. No condition of this API names a credential identity, so that per-cluster credentials can be added
// later without renaming any of them.

// IBMCloudManagedCluster conditions and reasons.
const (
	// IBMCloudManagedClusterReadyCondition is the summary of IBMCloudManagedClusterNetworkReadyCondition, computed with
	// util/conditions.SetSummaryCondition.
	IBMCloudManagedClusterReadyCondition = clusterv1.ReadyCondition

	// IBMCloudManagedClusterNetworkReadyCondition is True once the resource group, the VPC and the subnet of every zone
	// resolved and match each other. Informational messages (no public gateway, fewer than 16 free addresses) do not
	// make it False.
	IBMCloudManagedClusterNetworkReadyCondition = "NetworkReady"

	// IBMCloudManagedClusterNetworkReadyReason surfaces when every network reference resolved and validated.
	IBMCloudManagedClusterNetworkReadyReason = clusterv1.ReadyReason

	// IBMCloudManagedClusterNetworkReadyResourceGroupNotFoundReason surfaces when spec.resourceGroup.id names no resource
	// group the controller can see.
	IBMCloudManagedClusterNetworkReadyResourceGroupNotFoundReason = "ResourceGroupNotFound"

	// IBMCloudManagedClusterNetworkReadyVPCNotFoundReason surfaces when spec.vpc names no VPC in the region.
	IBMCloudManagedClusterNetworkReadyVPCNotFoundReason = "VPCNotFound"

	// IBMCloudManagedClusterNetworkReadySubnetNotFoundReason surfaces when a zone's subnet does not exist.
	IBMCloudManagedClusterNetworkReadySubnetNotFoundReason = "SubnetNotFound"

	// IBMCloudManagedClusterNetworkReadySubnetNotInVPCReason surfaces when a zone's subnet belongs to another VPC.
	IBMCloudManagedClusterNetworkReadySubnetNotInVPCReason = "SubnetNotInVPC"

	// IBMCloudManagedClusterNetworkReadySubnetZoneMismatchReason surfaces when a zone's subnet lives in another zone.
	IBMCloudManagedClusterNetworkReadySubnetZoneMismatchReason = "SubnetZoneMismatch"

	// IBMCloudManagedClusterNetworkReadySubnetNameAmbiguousReason surfaces when a subnet given by name matches more than
	// one subnet, so the user has to give its id.
	IBMCloudManagedClusterNetworkReadySubnetNameAmbiguousReason = "SubnetNameAmbiguous"

	// IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason surfaces any other classified IBM Cloud API error; the message
	// carries the normalised IBM text and incident ID.
	IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason = "IBMAPIError"

	// IBMCloudManagedClusterPausedCondition reports whether reconciliation is paused (util/paused).
	IBMCloudManagedClusterPausedCondition = clusterv1.PausedCondition
)

// IBMCloudManagedMachinePool conditions and reasons: Ready and Paused.
const (
	// IBMCloudManagedMachinePoolReadyCondition is the summary of IBMCloudManagedMachinePoolWorkerPoolReadyCondition only: a
	// fix pack waiting under the Manual policy must not make a healthy pool look broken.
	IBMCloudManagedMachinePoolReadyCondition = clusterv1.ReadyCondition

	// IBMCloudManagedMachinePoolPausedCondition reports whether reconciliation is paused (util/paused).
	IBMCloudManagedMachinePoolPausedCondition = clusterv1.PausedCondition
)

// WorkerPoolReady reports whether the IBM pool exists with the desired size and ready workers.
const (
	// IBMCloudManagedMachinePoolWorkerPoolReadyCondition is True when the size per zone matches, every worker of the pool
	// is ready and the count equals the desired replicas.
	IBMCloudManagedMachinePoolWorkerPoolReadyCondition = "WorkerPoolReady"

	// IBMCloudManagedMachinePoolWorkerPoolReadyReason surfaces when the pool has the desired size and every worker is ready.
	IBMCloudManagedMachinePoolWorkerPoolReadyReason = clusterv1.ReadyReason

	// IBMCloudManagedMachinePoolWorkerPoolReadyExternallyAutoscaledReason surfaces (True) when IBM reports the pool as
	// autoscaled; CAPIBM then does not resize it.
	IBMCloudManagedMachinePoolWorkerPoolReadyExternallyAutoscaledReason = "ExternallyAutoscaled"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason surfaces while the control plane has no IBM
	// cluster ID yet, or, for a pool other than "default", is not initialized.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason = "WaitingForControlPlane"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason surfaces while a zone of the pool is not yet
	// in IBMCloudManagedCluster status.zones.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason = "WaitingForInfrastructure"

	// IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason surfaces when a zone of the pool is not a zone of the
	// IBMCloudManagedCluster.
	IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason = "UnknownZone"

	// IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason surfaces when MachinePool replicas is not a
	// multiple of the number of zones; no IBM call is made.
	IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason = "ReplicasNotDivisibleByZones"

	// IBMCloudManagedMachinePoolWorkerPoolReadyZeroReplicasNotSupportedReason surfaces for replicas 0 while zero-replica
	// pools are not allowed.
	IBMCloudManagedMachinePoolWorkerPoolReadyZeroReplicasNotSupportedReason = "ZeroReplicasNotSupported"

	// IBMCloudManagedMachinePoolWorkerPoolReadyInvalidForDistributionReason surfaces for a setting the cluster's
	// distribution does not support, such as entitlement on a Kubernetes cluster.
	IBMCloudManagedMachinePoolWorkerPoolReadyInvalidForDistributionReason = "InvalidForDistribution"

	// IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason surfaces on every IBMCloudManagedMachinePool of the
	// Cluster that claims the same poolName; none publishes or mutates.
	IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason = "PoolNameConflict"

	// IBMCloudManagedMachinePoolWorkerPoolReadyPoolConflictReason surfaces when the IBM pool with that name has another
	// flavor; it is not ours, so nothing is published, mutated or removed.
	IBMCloudManagedMachinePoolWorkerPoolReadyPoolConflictReason = "PoolConflict"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason surfaces while the IBM cluster state does not
	// allow the pool mutation.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason = "WaitingForClusterState"

	// IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameBeingReleasedReason surfaces while IBM still lists a deleting or
	// deleted pool with that name (E0d39 lag).
	IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameBeingReleasedReason = "PoolNameBeingReleased"

	// IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason surfaces while the pool is being created.
	IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason = "Creating"

	// IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason surfaces while workers of the pool provision.
	IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason = "Provisioning"

	// IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason surfaces while IBM resizes the pool.
	IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason = "Resizing"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWorkersNotReadyReason surfaces while some workers of the pool are not ready.
	IBMCloudManagedMachinePoolWorkerPoolReadyWorkersNotReadyReason = "WorkersNotReady"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForSettledClusterReason surfaces while another disruption of the
	// Cluster has not settled.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForSettledClusterReason = "WaitingForSettledCluster"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterMinimumReason surfaces while IBM's minimum worker count
	// (E1437) blocks the resize.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterMinimumReason = "WaitingForClusterMinimum"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForDisruptionTokenReason surfaces while another pool of the Cluster
	// holds the disruption token.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForDisruptionTokenReason = "WaitingForDisruptionToken"

	// IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason surfaces while the workload cluster is not
	// connected, before any cordon.
	IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason = "WaitingForWorkloadCluster"

	// IBMCloudManagedMachinePoolWorkerPoolReadyDrainingNodeReason surfaces while a scale-down drains the predicted victims.
	IBMCloudManagedMachinePoolWorkerPoolReadyDrainingNodeReason = "DrainingNode"
)

// WorkersUpToDate reports whether the pool's workers run the newest fix pack of the control plane minor. It is not
// part of the Ready summary.
const (
	// IBMCloudManagedMachinePoolWorkersUpToDateCondition is True when no worker is due for replacement and none has an
	// update available.
	IBMCloudManagedMachinePoolWorkersUpToDateCondition = "WorkersUpToDate"

	// IBMCloudManagedMachinePoolWorkersUpToDateReason surfaces when every worker is up to date.
	IBMCloudManagedMachinePoolWorkersUpToDateReason = clusterv1.UpToDateReason

	// IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason surfaces under the Manual policy while workers
	// can move to a newer fix pack and no due spec.rollout.after asks for it.
	IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason = "PatchUpdateAvailable"

	// IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason surfaces while a rollout is in progress between workers.
	IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason = clusterv1.RollingOutReason

	// IBMCloudManagedMachinePoolWorkersUpToDateDrainingNodeReason surfaces while the worker to replace is drained.
	IBMCloudManagedMachinePoolWorkersUpToDateDrainingNodeReason = "DrainingNode"

	// IBMCloudManagedMachinePoolWorkersUpToDateReplacingWorkerReason surfaces while IBM replaces a worker.
	IBMCloudManagedMachinePoolWorkersUpToDateReplacingWorkerReason = "ReplacingWorker"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForReplacementReason surfaces while the replacement Node is not yet
	// Ready for minReadySeconds.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForReplacementReason = "WaitingForReplacement"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForControlPlaneUpgradeReason surfaces while the master is pending or
	// updating (a requested minor or an IBM patch).
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForControlPlaneUpgradeReason = "WaitingForControlPlaneUpgrade"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForCapacityReason surfaces when the pool is too small for a rolling
	// replacement; the message names the MachinePool size that allows it.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForCapacityReason = "WaitingForCapacity"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForClusterMinimumReason surfaces when IBM answered the replace with
	// E1437; the Node stays cordoned.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForClusterMinimumReason = "WaitingForClusterMinimum"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForSettledClusterReason surfaces while another disruption of the
	// Cluster has not settled.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForSettledClusterReason = "WaitingForSettledCluster"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForClusterStateReason surfaces while the IBM cluster state does not
	// allow a replacement.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForClusterStateReason = "WaitingForClusterState"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForDisruptionTokenReason surfaces while another pool of the Cluster
	// holds the disruption token.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForDisruptionTokenReason = "WaitingForDisruptionToken"

	// IBMCloudManagedMachinePoolWorkersUpToDateWaitingForWorkloadClusterReason surfaces while the workload cluster is not
	// connected.
	IBMCloudManagedMachinePoolWorkersUpToDateWaitingForWorkloadClusterReason = "WaitingForWorkloadCluster"

	// IBMCloudManagedMachinePoolWorkersUpToDateAutoRebalanceDisabledReason surfaces when the victim's zone has IBM
	// auto-rebalance disabled; the message gives the CLI fix.
	IBMCloudManagedMachinePoolWorkersUpToDateAutoRebalanceDisabledReason = "AutoRebalanceDisabled"

	// IBMCloudManagedMachinePoolWorkersUpToDateReplacementNotCreatedReason surfaces when IBM created no new worker within
	// ReplacementNotCreatedAfter of deleting the old one.
	IBMCloudManagedMachinePoolWorkersUpToDateReplacementNotCreatedReason = "ReplacementNotCreated"

	// IBMCloudManagedMachinePoolWorkersUpToDateWorkerOperationStuckReason surfaces when a worker stays transitional for
	// longer than an hour.
	IBMCloudManagedMachinePoolWorkersUpToDateWorkerOperationStuckReason = "WorkerOperationStuck"
)

// Draining is an activity condition (v1beta2 style): True while CAPIBM drains Nodes of the pool.
const (
	// IBMCloudManagedMachinePoolDrainingCondition reports the drain of the pool's Nodes.
	IBMCloudManagedMachinePoolDrainingCondition = "Draining"

	// IBMCloudManagedMachinePoolDrainingNodeReason surfaces while Nodes are cordoned and their pods evicted.
	IBMCloudManagedMachinePoolDrainingNodeReason = "DrainingNode"

	// IBMCloudManagedMachinePoolDrainingDrainBlockedReason surfaces while a PodDisruptionBudget refuses an eviction; the
	// message uses CAPI's format.
	IBMCloudManagedMachinePoolDrainingDrainBlockedReason = "DrainBlocked"

	// IBMCloudManagedMachinePoolDrainingWaitingForVolumeDetachReason surfaces while drained Nodes still have attached
	// volumes.
	IBMCloudManagedMachinePoolDrainingWaitingForVolumeDetachReason = "WaitingForVolumeDetach"

	// IBMCloudManagedMachinePoolDrainingWaitingForWorkloadClusterReason surfaces while the workload cluster is not
	// connected during a drain.
	IBMCloudManagedMachinePoolDrainingWaitingForWorkloadClusterReason = "WaitingForWorkloadCluster"

	// IBMCloudManagedMachinePoolNotDrainingReason surfaces while no drain is in progress.
	IBMCloudManagedMachinePoolNotDrainingReason = "NotDraining"
)

// Deleting follows the CAPI v1beta2 polarity: False/NotDeleting without a deletionTimestamp, True with the specific
// reason while deleting.
const (
	// IBMCloudManagedMachinePoolDeletingCondition reports the progress of the deletion.
	IBMCloudManagedMachinePoolDeletingCondition = clusterv1.DeletingCondition

	// IBMCloudManagedMachinePoolNotDeletingReason surfaces while the object has no deletionTimestamp.
	IBMCloudManagedMachinePoolNotDeletingReason = clusterv1.NotDeletingReason

	// IBMCloudManagedMachinePoolDeletingWaitingForInfrastructureReason surfaces when the IBM cluster exists but the
	// Cluster's IBMCloudManagedCluster is gone; the finalizer is kept, because the credentials CAPIBM acts with are
	// resolved only from the IBMCloudManagedCluster.
	IBMCloudManagedMachinePoolDeletingWaitingForInfrastructureReason = "WaitingForInfrastructure"

	// IBMCloudManagedMachinePoolDeletingWaitingForClusterStateReason surfaces while the IBM cluster state does not allow
	// removing the pool.
	IBMCloudManagedMachinePoolDeletingWaitingForClusterStateReason = "WaitingForClusterState"

	// IBMCloudManagedMachinePoolDeletingWaitingForClusterMinimumReason surfaces while IBM's minimum worker count (E1437)
	// blocks removing the pool.
	IBMCloudManagedMachinePoolDeletingWaitingForClusterMinimumReason = "WaitingForClusterMinimum"

	// IBMCloudManagedMachinePoolDeletingWaitingForSettledClusterReason surfaces while another disruption of the Cluster
	// has not settled.
	IBMCloudManagedMachinePoolDeletingWaitingForSettledClusterReason = "WaitingForSettledCluster"

	// IBMCloudManagedMachinePoolDeletingWaitingForDisruptionTokenReason surfaces while another pool of the Cluster holds
	// the disruption token.
	IBMCloudManagedMachinePoolDeletingWaitingForDisruptionTokenReason = "WaitingForDisruptionToken"

	// IBMCloudManagedMachinePoolDeletingDrainingNodesReason surfaces while every Node of the pool is drained before the
	// pool is removed.
	IBMCloudManagedMachinePoolDeletingDrainingNodesReason = "DrainingNodes"

	// IBMCloudManagedMachinePoolDeletingRemovingWorkerPoolReason surfaces after removeWorkerPool was sent, until IBM no
	// longer lists the pool.
	IBMCloudManagedMachinePoolDeletingRemovingWorkerPoolReason = "RemovingWorkerPool"
)
