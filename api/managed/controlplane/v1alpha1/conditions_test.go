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

package v1alpha1_test

import (
	"testing"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
)

// Compile-time proof that the kind works with CAPI's util/conditions and the patch helper. The assertion lives in a
// test file on purpose: util/conditions imports gomega in non-test code, and the API package must not pull that into
// every binary that only needs the types.
var (
	_ conditions.Setter = &controlplanev1.IBMCloudManagedControlPlane{}
)

// TestIBMCloudManagedControlPlaneConditions checks that SetConditions and GetConditions use status.conditions, through
// CAPI's util/conditions as the controller will.
func TestIBMCloudManagedControlPlaneConditions(t *testing.T) {
	g := NewWithT(t)
	obj := &controlplanev1.IBMCloudManagedControlPlane{}
	g.Expect(obj.GetConditions()).To(BeEmpty())

	conditions.Set(obj, metav1.Condition{
		Type:   controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
		Status: metav1.ConditionFalse,
		Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
	})

	g.Expect(obj.Status.Conditions).To(HaveLen(1))
	g.Expect(conditions.IsFalse(obj, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)).To(BeTrue())
	g.Expect(conditions.GetReason(obj, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)).
		To(Equal("BindingMismatch"))
}

// TestKubeconfigClusterIDAnnotation pins the annotation key: it is stored on kubeconfig Secrets and survives
// clusterctl move, so changing it would orphan the durable copy of status.clusterID.
func TestKubeconfigClusterIDAnnotation(t *testing.T) {
	g := NewWithT(t)
	g.Expect(controlplanev1.KubeconfigClusterIDAnnotation).To(Equal("ibmcloudmanaged.controlplane.cluster.x-k8s.io/cluster-id"))
}

// TestCreateHoldAnnotation pins the create-hold key: a control plane written by an earlier controller version (or
// carried by clusterctl move) keeps its hold only while the key stays the same.
func TestCreateHoldAnnotation(t *testing.T) {
	g := NewWithT(t)
	g.Expect(controlplanev1.CreateHoldAnnotation).To(Equal("ibmcloudmanaged.controlplane.cluster.x-k8s.io/create-hold-until"))
}

// TestConditionCatalogueIsValid runs every declared condition type and reason through the API server's own condition
// validation, so a reason the server would reject fails here instead of in a status patch.
func TestConditionCatalogueIsValid(t *testing.T) {
	catalogue := []struct {
		conditionType string
		reasons       []string
	}{
		{conditionType: controlplanev1.IBMCloudManagedControlPlaneReadyCondition},
		{conditionType: controlplanev1.IBMCloudManagedControlPlanePausedCondition},
		{
			conditionType: controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
			reasons: []string{
				controlplanev1.IBMCloudManagedControlPlaneAvailableReason,
				controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason,
				controlplanev1.IBMCloudManagedControlPlaneAvailableMasterNotReadyReason,
				controlplanev1.IBMCloudManagedControlPlaneAvailableWorkloadAPIUnreachableReason,
				controlplanev1.IBMCloudManagedControlPlaneAvailableDeletingReason,
			},
		},
		{
			conditionType: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			reasons: []string{
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedVersionNotAvailableReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreateFailedReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedDeletedOutOfBandReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
				controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			},
		},
		{
			conditionType: controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCondition,
			reasons: []string{
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyWaitingForMasterReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCredentialFetchFailedReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyVerificationFailedReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyEndpointUnreachableReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyCARotationInProgressReason,
				controlplanev1.IBMCloudManagedControlPlaneKubeconfigReadyUserProvidedSecretInvalidReason,
			},
		},
		{
			conditionType: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
			reasons: []string{
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateDowngradeNotAllowedReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUnparsableMasterVersionReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason,
				controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason,
			},
		},
		{
			conditionType: controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition,
			reasons: []string{
				controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason,
				controlplanev1.IBMCloudManagedControlPlaneNotRollingOutReason,
			},
		},
		{
			conditionType: controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
			reasons: []string{
				controlplanev1.IBMCloudManagedControlPlaneNotDeletingReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingDeleteFailedReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason,
				controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason,
			},
		},
	}

	for _, entry := range catalogue {
		t.Run(entry.conditionType, func(t *testing.T) {
			g := NewWithT(t)
			// A condition without its own reason list (Ready, Paused) is validated with a placeholder reason.
			reasons := entry.reasons
			if len(reasons) == 0 {
				reasons = []string{"Placeholder"}
			}
			for _, reason := range reasons {
				condition := metav1.Condition{
					Type:               entry.conditionType,
					Status:             metav1.ConditionFalse,
					Reason:             reason,
					LastTransitionTime: metav1.Now(),
				}
				errs := metav1validation.ValidateCondition(condition, field.NewPath("status", "conditions"))
				g.Expect(errs).To(BeEmpty(), "reason %q of condition %q", reason, entry.conditionType)
			}
		})
	}
}
