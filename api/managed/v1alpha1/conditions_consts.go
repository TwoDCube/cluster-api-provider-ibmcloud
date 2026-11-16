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
//   - where the reason repeats the condition's last word, that word is written once (Ready on NetworkReady is
//     IBMCloudManagedClusterNetworkReadyReason);
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
