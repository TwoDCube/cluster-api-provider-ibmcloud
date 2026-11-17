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
	retain, deleteStorage := kubernetesservice.RetainPersistentStorage, kubernetesservice.DeletePersistentStorage
	// failedIKS and failedROKS end every message about a DELETE that CAPIBM sent: status.deletionRequestTime is also
	// kept when IBM refused that DELETE, so the message names only what CAPIBM asked for.
	const failedIKS = ". If that DELETE failed, or the cluster was deleted elsewhere, whoever deleted it decided what " +
		"happened to the storage of its persistent volumes, such as VPC Block volumes"
	const failedROKS = failedIKS + ", and to the registry bucket in the Cloud Object Storage instance"
	// outcomes are claims about what IBM did with the storage, which CAPIBM cannot know: it cannot tell whether IBM
	// accepted its DELETE, and IBM's documentation conflicts on whether Delete removes VPC Block volumes.
	outcomes := []string{"Deleted by IBM", "deleted by IBM", "Kept by design", "kept by design", "IBM deleted it", "IBM kept it"}
	testCases := []struct {
		name         string
		vpcID        string
		distribution controlplanev1.Distribution
		sent         kubernetesservice.PersistentStorage
		want         string
		// wantSuffix is how the message must end; "" skips the check.
		wantSuffix string
		// wantNot must not appear: a message must never claim what IBM did with the storage, nor claim that CAPIBM asked
		// for the other choice.
		wantNot []string
	}{
		{
			name: "IKS with Retain", vpcID: "r006-vpc", distribution: controlplanev1.DistributionKubernetes, sent: retain,
			want: "IBM cluster iksclusterid00000001 is deleted. CAPIBM deletes nothing in VPC r006-vpc, so these resources remain " +
				"there. Permanently, because IBM never deletes them, even when the last cluster leaves the VPC (delete them only " +
				"when no cluster or other resource in the VPC uses them): the VPE gateways iks-api-r006-vpc, iks-cos-r006-vpc, " +
				"iks-cos-config-r006-vpc, iks-riaas-r006-vpc and iks-registry-r006-vpc and the security group " +
				"kube-vpegw-r006-vpc. Possibly also the VPE gateway for IAM that IKS adds to every VPC with a cluster since " +
				"November 2026 (IBM documents neither its name nor whether it is ever deleted). Removed by IKS within about " +
				"10 minutes, and until then blocking the deletion of the subnets or the VPC: the load balancers kube-iksclusterid00000001-*, the security groups kube-iksclusterid00000001, " +
				"kube-lbaas-iksclusterid00000001 and kube-vpegw-iksclusterid00000001 and the VPE gateway iks-iksclusterid00000001. " +
				"CAPIBM's last DELETE asked IBM to keep the cluster's persistent storage (spec.deletion.persistentStorage " +
				"Retain, also when omitted; deleteResources=false), such as the VPC Block volumes of its persistent volumes, " +
				"which are billed until you delete them" + failedIKS,
			wantSuffix: failedIKS,
			wantNot:    append([]string{"asked IBM to delete", "registry bucket"}, outcomes...),
		},
		{
			name: "ROKS with Retain names the registry bucket", vpcID: "r006-vpc",
			distribution: controlplanev1.DistributionOpenShift, sent: retain,
			want: "and the VPE gateway iks-iksclusterid00000001. CAPIBM's last DELETE asked IBM to keep the cluster's " +
				"persistent storage (spec.deletion.persistentStorage Retain, also when omitted; deleteResources=false), such as " +
				"the VPC Block volumes of its persistent volumes, which are billed until you delete them. CAPIBM never asks " +
				"IBM to delete the registry bucket in the Cloud Object Storage instance" + failedROKS,
			wantSuffix: failedROKS,
			wantNot:    append([]string{"asked IBM to delete the cluster's"}, outcomes...),
		},
		{
			name: "IKS with Delete says that VPC Block volumes may remain", vpcID: "r006-vpc",
			distribution: controlplanev1.DistributionKubernetes, sent: deleteStorage,
			want: "and the VPE gateway iks-iksclusterid00000001. CAPIBM's last DELETE asked IBM to delete the cluster's " +
				"persistent storage (spec.deletion.persistentStorage Delete, deleteResources=true), but VPC Block volumes may " +
				"remain: IBM's documentation conflicts on whether they go even when a storage class with reclaimPolicy Delete " +
				"provisioned them, and statically provisioned storage and storage of a storage class with reclaimPolicy " +
				"Retain remain. Check the account for the VPC Block volumes named after the cluster's PersistentVolumes " +
				"(pvc-<uid>) and delete those that remain: they are billed" + failedIKS,
			wantSuffix: failedIKS,
			wantNot:    append([]string{"asked IBM to keep", "registry bucket"}, outcomes...),
		},
		{
			name: "ROKS with Delete never asks IBM to delete the registry bucket", vpcID: "r006-vpc",
			distribution: controlplanev1.DistributionOpenShift, sent: deleteStorage,
			want: "delete those that remain: they are billed. CAPIBM never asks IBM to delete the registry bucket in the " +
				"Cloud Object Storage instance" + failedROKS,
			wantSuffix: failedROKS,
			wantNot:    append([]string{"asked IBM to keep", "Retain, also when omitted"}, outcomes...),
		},
		{
			name: "IKS deleted outside CAPIBM claims nothing about the storage", vpcID: "r006-vpc",
			distribution: controlplanev1.DistributionKubernetes,
			want: "and the VPE gateway iks-iksclusterid00000001. CAPIBM sent no DELETE for this cluster, so whoever deleted it " +
				"decided what happened to the storage of its persistent volumes, such as VPC Block volumes",
			wantSuffix: "such as VPC Block volumes",
			wantNot:    append([]string{"asked IBM", "If that DELETE failed", "registry bucket"}, outcomes...),
		},
		{
			name: "ROKS deleted outside CAPIBM claims nothing about the registry bucket either", vpcID: "r006-vpc",
			distribution: controlplanev1.DistributionOpenShift,
			want:         "such as VPC Block volumes, and to the registry bucket in the Cloud Object Storage instance",
			wantSuffix:   "such as VPC Block volumes, and to the registry bucket in the Cloud Object Storage instance",
			wantNot:      append([]string{"asked IBM", "If that DELETE failed"}, outcomes...),
		},
		{
			name: "an unknown VPC is named by placeholder", distribution: controlplanev1.DistributionKubernetes, sent: retain,
			want: "the security group kube-vpegw-<vpc-id>.",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			message := LeftoverResourcesMessage("iksclusterid00000001", tc.vpcID, tc.distribution, tc.sent)
			g.Expect(message).To(ContainSubstring(tc.want))
			if tc.wantSuffix != "" {
				g.Expect(message).To(HaveSuffix(tc.wantSuffix))
			}
			for _, notWanted := range tc.wantNot {
				g.Expect(message).NotTo(ContainSubstring(notWanted))
			}
			g.Expect(strings.Count(message, "iks-")).To(Equal(6), "five shared VPE gateways and the per-cluster one")
		})
	}
}

// TestStorageOnDelete locks the mapping from spec.deletion.persistentStorage to IBM's DELETE: only an explicit Delete
// asks IBM to delete the storage; omitted, Retain and a value the schema would refuse all ask IBM to keep it.
func TestStorageOnDelete(t *testing.T) {
	testCases := []struct {
		name   string
		policy controlplanev1.PersistentStoragePolicy
		want   kubernetesservice.PersistentStorage
	}{
		{name: "omitted", policy: "", want: kubernetesservice.RetainPersistentStorage},
		{name: "Retain", policy: controlplanev1.PersistentStorageRetain, want: kubernetesservice.RetainPersistentStorage},
		{name: "Delete", policy: controlplanev1.PersistentStorageDelete, want: kubernetesservice.DeletePersistentStorage},
		{name: "a value the schema refuses", policy: "delete", want: kubernetesservice.RetainPersistentStorage},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(storageOnDelete(tc.policy)).To(Equal(tc.want))
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
