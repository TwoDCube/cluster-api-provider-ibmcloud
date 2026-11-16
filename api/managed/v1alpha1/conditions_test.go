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

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// Compile-time proof that the kinds work with CAPI's util/conditions and the patch helper. The assertions live in a
// test file on purpose: util/conditions imports gomega in non-test code, and the API package must not pull that into
// every binary that only needs the types.
var (
	_ conditions.Setter = &infrav1.IBMCloudManagedCluster{}
)

// TestIBMCloudManagedClusterConditions checks that SetConditions and GetConditions use status.conditions, through CAPI's
// util/conditions as the controllers will.
func TestIBMCloudManagedClusterConditions(t *testing.T) {
	g := NewWithT(t)
	obj := &infrav1.IBMCloudManagedCluster{}
	g.Expect(obj.GetConditions()).To(BeEmpty())

	conditions.Set(obj, metav1.Condition{
		Type:   infrav1.IBMCloudManagedClusterNetworkReadyCondition,
		Status: metav1.ConditionFalse,
		Reason: infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
	})

	g.Expect(obj.Status.Conditions).To(HaveLen(1))
	g.Expect(conditions.IsFalse(obj, infrav1.IBMCloudManagedClusterNetworkReadyCondition)).To(BeTrue())
	g.Expect(conditions.GetReason(obj, infrav1.IBMCloudManagedClusterNetworkReadyCondition)).To(Equal("VPCNotFound"))
}

// conditionCatalogue lists, per condition type, every reason this package declares. The test below runs each pair
// through the API server's own condition validation, so a reason the server would reject (a space, a leading digit,
// too long) fails here instead of in a status patch.
type conditionCatalogue struct {
	conditionType string
	reasons       []string
}

// managedInfraConditionCatalogue is the IBMCloudManagedCluster part of the catalogue.
var managedInfraConditionCatalogue = []conditionCatalogue{
	{conditionType: infrav1.IBMCloudManagedClusterReadyCondition},
	{conditionType: infrav1.IBMCloudManagedClusterPausedCondition},
	{
		conditionType: infrav1.IBMCloudManagedClusterNetworkReadyCondition,
		reasons: []string{
			infrav1.IBMCloudManagedClusterNetworkReadyReason,
			infrav1.IBMCloudManagedClusterNetworkReadyResourceGroupNotFoundReason,
			infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason,
			infrav1.IBMCloudManagedClusterNetworkReadySubnetNotInVPCReason,
			infrav1.IBMCloudManagedClusterNetworkReadySubnetZoneMismatchReason,
			infrav1.IBMCloudManagedClusterNetworkReadySubnetNameAmbiguousReason,
			infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason,
		},
	},
}

// TestConditionCatalogueIsValid validates every declared condition type and reason with the API server's rules.
func TestConditionCatalogueIsValid(t *testing.T) {
	for _, entry := range managedInfraConditionCatalogue {
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
