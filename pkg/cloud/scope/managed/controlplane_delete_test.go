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
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

// The delete-flow unit tests lock the texts an operator acts on (the LeftoverResources event, the pool list of
// WaitingForWorkersDeletion) and the rule that no IBM error is ever Terminal in the delete flow. The flow itself is
// tested through the controller (internal/controllers/managed).

func TestLeftoverResourcesMessage(t *testing.T) {
	testCases := []struct {
		name         string
		vpcID        string
		distribution controlplanev1.Distribution
		want         string
	}{
		{
			name: "IKS", vpcID: "r006-vpc", distribution: controlplanev1.DistributionKubernetes,
			want: "IBM cluster iksclusterid00000001 is deleted. CAPIBM deletes nothing in VPC r006-vpc, so these resources remain " +
				"there. Permanently, because IBM never deletes them, even when the last cluster leaves the VPC (delete them only " +
				"when no cluster or other resource in the VPC uses them): the VPE gateways iks-api-r006-vpc, iks-cos-r006-vpc, " +
				"iks-cos-config-r006-vpc, iks-riaas-r006-vpc and iks-registry-r006-vpc and the security group " +
				"kube-vpegw-r006-vpc. Possibly also the VPE gateway for IAM that IKS adds to every VPC with a cluster since " +
				"November 2026 (IBM documents neither its name nor whether it is ever deleted). Removed by IKS within about " +
				"10 minutes, and until then blocking the deletion of the subnets or the VPC: the load balancers kube-iksclusterid00000001-*, the security groups kube-iksclusterid00000001, " +
				"kube-lbaas-iksclusterid00000001 and kube-vpegw-iksclusterid00000001 and the VPE gateway iks-iksclusterid00000001. " +
				"Kept by design: the VPC Block volumes of its persistent volumes (deleteResources=false)",
		},
		{
			name: "ROKS keeps the registry bucket", vpcID: "r006-vpc", distribution: controlplanev1.DistributionOpenShift,
			want: "Kept by design: the VPC Block volumes of its persistent volumes (deleteResources=false) and the registry bucket " +
				"in the Cloud Object Storage instance",
		},
		{
			name: "an unknown VPC is named by placeholder", distribution: controlplanev1.DistributionKubernetes,
			want: "the security group kube-vpegw-<vpc-id>.",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			message := LeftoverResourcesMessage("iksclusterid00000001", tc.vpcID, tc.distribution)
			g.Expect(message).To(ContainSubstring(tc.want))
			g.Expect(strings.Count(message, "iks-")).To(Equal(6), "five shared VPE gateways and the per-cluster one")
		})
	}
}

func TestCPWorkersMessage(t *testing.T) {
	g := NewWithT(t)
	g.Expect(cpWorkersMessage([]string{"MachinePool/a"}, "dev")).To(Equal("Waiting for MachinePool/a of Cluster dev to be " +
		"deleted: deleting the IBM cluster deletes every worker pool, so it is deleted only after them; delete Cluster dev to " +
		"delete everything in order"))
	many := []string{"a", "b", "c", "d", "e", "f"}
	g.Expect(cpWorkersMessage(many, "dev")).To(HavePrefix("Waiting for a, b, c, d, e and 1 more of Cluster dev"))
}

func TestControlPlaneDeleteCallProblemIsNeverTerminal(t *testing.T) {
	identity := credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}
	callErr := func(status int) error {
		return &controlPlaneCallError{identity: identity, op: kubernetesservice.OpGetCluster, action: "getting IBM cluster x",
			err: &kubernetesservice.APIError{Operation: kubernetesservice.OpGetCluster, StatusCode: status, IncidentID: "inc-9"}}
	}
	testCases := []struct {
		name           string
		err            error
		wantKind       OutcomeKind
		wantReason     string
		wantInvalidate bool
	}{
		{name: "401", err: callErr(http.StatusUnauthorized), wantKind: OutcomeNotDone,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason, wantInvalidate: true},
		{name: "403", err: callErr(http.StatusForbidden), wantKind: OutcomeNotDone,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason},
		{name: "429", err: callErr(http.StatusTooManyRequests), wantKind: OutcomeRateLimited,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason},
		{name: "400", err: callErr(http.StatusBadRequest), wantKind: OutcomeTransient,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason},
		{name: "500", err: callErr(http.StatusInternalServerError), wantKind: OutcomeTransient,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason},
		{name: "not an IBM call", err: errors.New("the Secret could not be read"), wantKind: OutcomeTransient,
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			builder := &cpScopeBuilder{}
			scope := &ControlPlaneScope{
				params:       newCPScopeParams(builder),
				ControlPlane: &controlplanev1.IBMCloudManagedControlPlane{},
				InfraCluster: &infrav1.IBMCloudManagedCluster{Spec: infrav1.IBMCloudManagedClusterSpec{
					ResourceGroup: infrav1.ResourceGroupReference{ID: "0123456789abcdef0123456789abcdef"}}},
			}
			outcome := scope.deleteCallProblem(context.Background(), tc.err)
			g.Expect(outcome.Kind).To(Equal(tc.wantKind))
			g.Expect(outcome.Kind).NotTo(Equal(OutcomeTerminal), "nothing is Terminal in the delete flow")
			deleting := conditions.Get(scope.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition)
			g.Expect(deleting).NotTo(BeNil())
			g.Expect(deleting.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(deleting.Reason).To(Equal(tc.wantReason))
			g.Expect(deleting.Message).NotTo(ContainSubstring("inc-9"), "IBM's per-answer incident ID never reaches status")
			g.Expect(builder.invalidated).To(HaveLen(map[bool]int{true: 1, false: 0}[tc.wantInvalidate]))
		})
	}
}
