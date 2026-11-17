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
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmclustername"
)

// The binding tests run every row of DecideBinding on scrubbed getCluster captures of live IKS and ROKS clusters
// (2026-11-16), whose createdDate uses IBM's layout, for example 2026-11-16T03:05:33+0000.

// bindingFixtureExtra holds the getCluster fields that loadCluster (assumptions_test.go) does not copy but the
// candidate checks and EndpointMatches read.
type bindingFixtureExtra struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	Provider         string `json:"provider"`
	ResourceGroup    string `json:"resourceGroup"`
	ServiceEndpoints struct {
		Public  string `json:"publicServiceEndpointURL"`
		Private string `json:"privateServiceEndpointURL"`
	} `json:"serviceEndpoints"`
}

// bindingCluster decodes a getCluster capture with every field the binding reads.
func bindingCluster(t *testing.T, name string) *kubernetesservice.Cluster {
	t.Helper()
	c := loadCluster(t, name)
	data, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	var extra bindingFixtureExtra
	if err := json.Unmarshal(data, &extra); err != nil {
		t.Fatalf("decoding fixture %s: %v", name, err)
	}
	c.Name, c.Type, c.Provider, c.ResourceGroupID = extra.Name, extra.Type, extra.Provider, extra.ResourceGroup
	c.PublicServiceEndpointURL, c.PrivateServiceEndpointURL = extra.ServiceEndpoints.Public, extra.ServiceEndpoints.Private
	return &c
}

// bindingCreated is the createdDate of the IKS capture, 2026-11-16T03:05:33+0000.
var bindingCreated = time.Date(2026, 11, 16, 3, 5, 33, 0, time.UTC)

// bindingIKSInput returns the input of a control plane whose name, resource group and distribution match the IKS
// capture, with the clock 20 minutes after its creation and nothing else set.
func bindingIKSInput(found *kubernetesservice.Cluster) BindingInput {
	return BindingInput{
		Name:            "example-test-cluster-probe-iks",
		ResourceGroupID: "0123456789abcdef0123456789abcdef",
		Distribution:    controlplanev1.DistributionKubernetes,
		Found:           found,
		Now:             bindingCreated.Add(20 * time.Minute),
	}
}

// bindingUnknownIntent returns an intent for the IKS name whose attempts of unknown outcome were sent at first and
// last.
func bindingUnknownIntent(first, last time.Time) controlplanev1.CreateIntent {
	return controlplanev1.CreateIntent{
		ClusterName:             "example-test-cluster-probe-iks",
		FirstUnknownOutcomeTime: metav1.NewTime(first),
		LastUnknownOutcomeTime:  metav1.NewTime(last),
	}
}

// bindingWith returns a copy of c changed by mutate.
func bindingWith(c *kubernetesservice.Cluster, mutate func(*kubernetesservice.Cluster)) *kubernetesservice.Cluster {
	changed := *c
	mutate(&changed)
	return &changed
}

func TestDecideBinding(t *testing.T) {
	iks := bindingCluster(t, "iks-getCluster-normal.json")
	roks := bindingCluster(t, "roks-getCluster-normal.json")
	const iksID = "iksclusterid00000001"
	endpoint := controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 30101}
	otherRG := bindingWith(iks, func(c *kubernetesservice.Cluster) { c.ResourceGroupID = "fedcba9876543210fedcba9876543210" })
	otherName := bindingWith(iks, func(c *kubernetesservice.Cluster) { c.Name = "someone-elses-cluster" })
	// first and last are the bounds of two create attempts of unknown outcome; the capture's createdDate lies between.
	first, last := bindingCreated.Add(-30*time.Second), bindingCreated.Add(time.Minute)

	testCases := []struct {
		name        string
		in          func() BindingInput
		action      BindingAction
		row         string
		reason      string
		clusterID   string
		proofNeeded bool
		message     []string
	}{
		// Row 1 and 1b: status.clusterID is trusted, the candidate checks still run.
		{name: "row1_status_id_found_and_checks_pass", action: BindingKnown, row: "1", clusterID: iksID,
			in: func() BindingInput { in := bindingIKSInput(iks); in.StatusClusterID = iksID; return in }},
		{name: "row1_roks_status_id_found", action: BindingKnown, row: "1", clusterID: "roksclusterid0000002",
			in: func() BindingInput {
				in := bindingIKSInput(roks)
				in.Name, in.Distribution, in.StatusClusterID = "example-test-cluster-probe-roks", controlplanev1.DistributionOpenShift, "roksclusterid0000002"
				return in
			}},
		{name: "row1_status_wins_over_a_forged_secret_annotation", action: BindingKnown, row: "1", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.StatusClusterID, in.SecretClusterID = iksID, "forgedclusterid00009"
				return in
			}},
		{name: "row1b_wrong_resource_group", action: BindingTerminal, row: "1b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
			message: []string{"named by status.clusterID", "resource group fedcba9876543210fedcba9876543210", "changes nothing"},
			in:      func() BindingInput { in := bindingIKSInput(otherRG); in.StatusClusterID = iksID; return in }},
		{name: "row1b_wrong_name", action: BindingTerminal, row: "1b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
			message: []string{"it is named someone-elses-cluster, not example-test-cluster-probe-iks"},
			in:      func() BindingInput { in := bindingIKSInput(otherName); in.StatusClusterID = iksID; return in }},
		{name: "row1b_wrong_type", action: BindingTerminal, row: "1b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
			message: []string{"its type is kubernetes, not openshift (spec.distribution OpenShift)"},
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.StatusClusterID, in.Distribution = iksID, controlplanev1.DistributionOpenShift
				return in
			}},
		{name: "row1b_wrong_provider", action: BindingTerminal, row: "1b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
			message: []string{"its provider is classic, not vpc-gen2"},
			in: func() BindingInput {
				in := bindingIKSInput(bindingWith(iks, func(c *kubernetesservice.Cluster) { c.Provider = "classic" }))
				in.StatusClusterID = iksID
				return in
			}},
		{name: "row1b_ibm_returned_another_id", action: BindingTerminal, row: "1b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
			message: []string{"IBM returned cluster iksclusterid00000001 for the ID example-test-cluster-probe-iks"},
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.StatusClusterID = "example-test-cluster-probe-iks"
				return in
			}},

		// Row 2 and 2b: the Secret's cluster-id is only a candidate; it needs the proof of possession.
		{name: "row2_secret_id_with_proof", action: BindingKnown, row: "2", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.SecretClusterID, in.Proof = iksID, OwnershipProofProven
				return in
			}},
		{name: "row2b_forged_annotation_with_failing_proof", action: BindingTerminal, row: "2b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason,
			message: []string{"the cluster-id annotation of the kubeconfig Secret names IBM cluster iksclusterid00000001", "did not pass the cluster-admin access review on it (access review */*/* denied)", "ibmcloud ks cluster config --admin"},
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.SecretClusterID, in.Proof, in.ProofFailure = iksID, OwnershipProofFailed, "access review */*/* denied"
				return in
			}},
		{name: "row2b_failing_proof_without_detail", action: BindingTerminal, row: "2b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason,
			message: []string{"did not pass the cluster-admin access review on it."},
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.SecretClusterID, in.Proof = iksID, OwnershipProofFailed
				return in
			}},
		{name: "row2b_proof_not_attempted_asks_for_it", action: BindingTerminal, row: "2b", proofNeeded: true,
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason,
			message: []string{"has not run"},
			in:      func() BindingInput { in := bindingIKSInput(iks); in.SecretClusterID = iksID; return in }},
		{name: "row2b_wrong_resource_group_even_with_proof", action: BindingTerminal, row: "2b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason,
			message: []string{"resource group fedcba9876543210fedcba9876543210"},
			in: func() BindingInput {
				in := bindingIKSInput(otherRG)
				in.SecretClusterID, in.Proof = iksID, OwnershipProofProven
				return in
			}},
		{name: "row2b_wrong_name_even_with_proof", action: BindingTerminal, row: "2b",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason,
			message: []string{"it is named someone-elses-cluster"},
			in: func() BindingInput {
				in := bindingIKSInput(otherName)
				in.SecretClusterID, in.Proof = iksID, OwnershipProofProven
				return in
			}},

		// Rows 3 and 4: a known ID answers 404.
		{name: "row3_known_id_404_within_create_grace", action: BindingWait, row: "3",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			message: []string{"2026-11-16T03:05:00Z", "until 2026-11-16T03:10:00Z"},
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.StatusClusterID = iksID
				in.Intent = controlplanev1.CreateIntent{ClusterName: in.Name, InFlightAttemptTime: metav1.NewTime(time.Date(2026, 11, 16, 3, 5, 0, 0, time.UTC))}
				in.Now = time.Date(2026, 11, 16, 3, 9, 59, 0, time.UTC)
				return in
			}},
		{name: "row3_window_edge_is_row4", action: BindingTerminal, row: "4",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.StatusClusterID = iksID
				in.Intent = controlplanev1.CreateIntent{ClusterName: in.Name, InFlightAttemptTime: metav1.NewTime(time.Date(2026, 11, 16, 3, 5, 0, 0, time.UTC))}
				in.Now = time.Date(2026, 11, 16, 3, 10, 0, 0, time.UTC)
				return in
			}},
		{name: "row3_uses_the_later_unknown_attempt", action: BindingWait, row: "3",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.StatusClusterID = iksID
				in.Intent = bindingUnknownIntent(first, last)
				in.Now = last.Add(CreateGracePeriod - time.Second)
				return in
			}},
		// In delete mode the end of row 3 removes the finalizer without a DELETE (row 4), so a cluster IBM accepted
		// with 201 but lists late gets the delete lookup grace, like row 9, instead of CreateGracePeriod.
		{name: "row3_deleting_waits_the_delete_lookup_grace", action: BindingWait, row: "3",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			message: []string{"2026-11-16T03:05:00Z", "until 2026-11-16T03:35:00Z", "before it counts the cluster as gone"},
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.StatusClusterID, in.Deleting = iksID, true
				in.Intent = controlplanev1.CreateIntent{ClusterName: in.Name, InFlightAttemptTime: metav1.NewTime(time.Date(2026, 11, 16, 3, 5, 0, 0, time.UTC))}
				in.Now = time.Date(2026, 11, 16, 3, 11, 0, 0, time.UTC)
				return in
			}},
		{name: "row3_deleting_grace_edge_is_row4", action: BindingTerminal, row: "4",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.StatusClusterID, in.Deleting = iksID, true
				in.Intent = controlplanev1.CreateIntent{ClusterName: in.Name, InFlightAttemptTime: metav1.NewTime(time.Date(2026, 11, 16, 3, 5, 0, 0, time.UTC))}
				in.Now = time.Date(2026, 11, 16, 3, 35, 0, 0, time.UTC)
				return in
			}},
		{name: "row4_known_id_404_without_intent", action: BindingTerminal, row: "4",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			message: []string{"reports no cluster iksclusterid00000001, named by status.clusterID", "never creates a replacement"},
			in:      func() BindingInput { in := bindingIKSInput(nil); in.StatusClusterID = iksID; return in }},
		{name: "row4_secret_id_404", action: BindingTerminal, row: "4",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			message: []string{"named by the cluster-id annotation of the kubeconfig Secret"},
			in:      func() BindingInput { in := bindingIKSInput(nil); in.SecretClusterID = iksID; return in }},
		{name: "row4_intent_for_another_name_does_not_wait", action: BindingTerminal, row: "4",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.StatusClusterID = iksID
				in.Intent = controlplanev1.CreateIntent{ClusterName: "another-name", InFlightAttemptTime: metav1.NewTime(in.Now)}
				return in
			}},

		// Rows 5 to 7: found by name without an ID.
		{name: "row5_is_off_by_default_so_a_matching_endpoint_is_a_conflict", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Endpoint, in.Proof = endpoint, OwnershipProofProven
				return in
			}},
		{name: "row6_lost_create_response", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(first, last)
				return in
			}},
		{name: "row6_createdDate_at_lower_window_edge", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(bindingCreated.Add(CreatedDateSkew), bindingCreated.Add(CreatedDateSkew))
				return in
			}},
		{name: "row6_createdDate_just_below_window_is_row7", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(bindingCreated.Add(CreatedDateSkew+time.Second), bindingCreated.Add(CreatedDateSkew+time.Second))
				return in
			}},
		{name: "row6_createdDate_at_upper_window_edge", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(bindingCreated.Add(-CreatedDateSlack), bindingCreated.Add(-CreatedDateSlack))
				return in
			}},
		{name: "row6_createdDate_just_above_window_is_row7", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(bindingCreated.Add(-CreatedDateSlack-time.Second), bindingCreated.Add(-CreatedDateSlack-time.Second))
				return in
			}},
		{name: "row6_skewed_clock_ibm_stamps_before_our_attempt", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				// The controller's clock runs 90 s ahead of IBM's: IBM's createdDate is before CAPIBM's attempt.
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(bindingCreated.Add(90*time.Second), bindingCreated.Add(90*time.Second))
				return in
			}},
		{name: "row6_inflight_attempt_counts_as_unknown", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = controlplanev1.CreateIntent{ClusterName: in.Name, InFlightAttemptTime: metav1.NewTime(bindingCreated.Add(-5 * time.Second))}
				return in
			}},
		{name: "row6_only_last_bound_set", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = controlplanev1.CreateIntent{ClusterName: in.Name, LastUnknownOutcomeTime: metav1.NewTime(bindingCreated)}
				return in
			}},
		{name: "row6_wrong_resource_group_is_row7", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(otherRG)
				in.Intent = bindingUnknownIntent(first, last)
				return in
			}},
		{name: "row6_wrong_name_is_row7", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(otherName)
				in.Intent = bindingUnknownIntent(first, last)
				return in
			}},
		{name: "row7_unparseable_createdDate", action: BindingTerminal, row: "7",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			message: []string{"created 16.11.2026 03:05"},
			in: func() BindingInput {
				in := bindingIKSInput(bindingWith(iks, func(c *kubernetesservice.Cluster) { c.CreatedDate = "16.11.2026 03:05" }))
				in.Intent = bindingUnknownIntent(first, last)
				return in
			}},
		{name: "row7_missing_createdDate", action: BindingTerminal, row: "7",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			message: []string{"created at an unknown time"},
			in: func() BindingInput {
				in := bindingIKSInput(bindingWith(iks, func(c *kubernetesservice.Cluster) { c.CreatedDate = "" }))
				in.Intent = bindingUnknownIntent(first, last)
				return in
			}},
		{name: "row7_foreign_cluster_without_intent", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			message: []string{"IBM Cloud cluster example-test-cluster-probe-iks (ID iksclusterid00000001, created 2026-11-16T03:05:33+0000) already exists",
				"different spec.ibmCloudClusterName", "Recovering ownership"},
			in: func() BindingInput { return bindingIKSInput(iks) }},
		{name: "row7_endpoint_set_disables_row6", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent, in.Endpoint = bindingUnknownIntent(first, last), endpoint
				return in
			}},
		{name: "row7_intent_for_another_name", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent = bindingUnknownIntent(first, last)
				in.Intent.ClusterName = "another-name"
				return in
			}},
		{name: "row7_deleting_with_unknown_attempt_is_possible_own_cluster", action: BindingTerminal, row: "7",
			reason:  controlplanev1.IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason,
			message: []string{"between 2026-11-16T03:05:03Z and 2026-11-16T03:06:33Z", "does not delete it", controlplanev1.IBMCloudManagedControlPlaneFinalizer},
			in: func() BindingInput {
				in := bindingIKSInput(bindingWith(iks, func(c *kubernetesservice.Cluster) { c.CreatedDate = "garbage" }))
				in.Intent, in.Deleting = bindingUnknownIntent(first, last), true
				return in
			}},
		{name: "row6_deleting_binds_its_own_cluster", action: BindingKnown, row: "6", clusterID: iksID,
			in: func() BindingInput {
				in := bindingIKSInput(iks)
				in.Intent, in.Deleting = bindingUnknownIntent(first, last), true
				return in
			}},
		{name: "row7_deleting_without_unknown_attempt_is_a_conflict", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			in:     func() BindingInput { in := bindingIKSInput(iks); in.Deleting = true; return in }},

		// Rows 8 to 10: nothing found by name.
		{name: "row8_endpoint_set_and_lookup_404", action: BindingTerminal, row: "8",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			message: []string{"(c111.eu-de.containers.cloud.ibm.com:30101)", "never creates a replacement"},
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.Endpoint = endpoint
				in.Intent = bindingUnknownIntent(in.Now, in.Now) // row 8 comes before row 9
				return in
			}},
		{name: "row9_unknown_attempt_within_grace", action: BindingWait, row: "9",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			message: []string{"before it creates the cluster"},
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.Intent = bindingUnknownIntent(first, last)
				in.Now = last.Add(CreateGracePeriod - time.Second)
				return in
			}},
		{name: "row9_grace_edge_creates", action: BindingCreate, row: "10",
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.Intent = bindingUnknownIntent(first, last)
				in.Now = last.Add(CreateGracePeriod)
				return in
			}},
		{name: "row9_deleting_waits_the_delete_lookup_grace", action: BindingWait, row: "9",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			message: []string{"concludes that there is nothing to delete"},
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.Intent, in.Deleting = bindingUnknownIntent(first, last), true
				in.Now = last.Add(10 * time.Minute)
				return in
			}},
		{name: "row9_deleting_grace_edge_is_nothing_to_delete", action: BindingNothingToDelete, row: "10",
			in: func() BindingInput {
				in := bindingIKSInput(nil)
				in.Intent, in.Deleting = bindingUnknownIntent(first, last), true
				in.Now = last.Add(DeleteLookupGracePeriod)
				return in
			}},
		{name: "row10_create", action: BindingCreate, row: "10",
			in: func() BindingInput { return bindingIKSInput(nil) }},
		{name: "row10_deleting_nothing_to_delete", action: BindingNothingToDelete, row: "10",
			in: func() BindingInput { in := bindingIKSInput(nil); in.Deleting = true; return in }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := tc.in()
			got, err := DecideBinding(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Action).To(Equal(tc.action))
			g.Expect(got.Row).To(Equal(tc.row))
			g.Expect(got.Reason).To(Equal(tc.reason))
			g.Expect(got.ClusterID).To(Equal(tc.clusterID))
			g.Expect(got.ProofNeeded).To(Equal(tc.proofNeeded))
			for _, part := range tc.message {
				g.Expect(got.Message).To(ContainSubstring(part))
			}
			switch got.Action {
			case BindingKnown:
				// Never Known without a trusted record, a proof, or a matching create record.
				g.Expect(got.Cluster).To(Equal(in.Found))
				g.Expect(in.StatusClusterID != "" || in.Proof == OwnershipProofProven || got.Row == "6").To(BeTrue())
			case BindingWait:
				g.Expect(got.WaitUntil.After(in.Now)).To(BeTrue())
				g.Expect(got.Message).NotTo(BeEmpty())
			case BindingTerminal:
				g.Expect(got.Message).NotTo(BeEmpty())
			}
			// The binding is about IBM clusters, never about credentials, so no message mentions an identity.
			g.Expect(strings.ToLower(got.Message)).NotTo(ContainSubstring("identity"))
		})
	}
}

// TestDecideBindingRow5 covers row 5 with the endpoint-match switch on, as it will be once it is verified on IBM Cloud
// that master URLs are unique per cluster and stable over upgrades; DecideBinding passes AdoptByEndpointMatch, which is
// false today.
func TestDecideBindingRow5(t *testing.T) {
	iks := bindingCluster(t, "iks-getCluster-normal.json")
	endpoint := controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 30101}
	testCases := []struct {
		name        string
		mutate      func(*BindingInput)
		action      BindingAction
		row         string
		reason      string
		proofNeeded bool
		message     string
	}{
		{name: "row5_endpoint_match_with_proof", action: BindingKnown, row: "5",
			mutate: func(in *BindingInput) { in.Proof = OwnershipProofProven }},
		{name: "row5_secret_server_match_with_proof", action: BindingKnown, row: "5",
			mutate: func(in *BindingInput) {
				in.Endpoint = controlplanev1.APIEndpoint{Host: "old.example.com", Port: 443}
				in.SecretServer, in.Proof = "https://c111.private.eu-de.containers.cloud.ibm.com:30101", OwnershipProofProven
			}},
		{name: "row5_proof_not_attempted_asks_for_it", action: BindingTerminal, row: "7", proofNeeded: true,
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			message: "the master URL matches spec.controlPlaneEndpoint, but the cluster-admin access review with the Secret's credential has not run",
			mutate:  func(*BindingInput) {}},
		{name: "row5_failing_proof_is_a_conflict", action: BindingTerminal, row: "7",
			reason:  controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			message: "did not pass the cluster-admin access review on it (TLS verification failed)",
			mutate:  func(in *BindingInput) { in.Proof, in.ProofFailure = OwnershipProofFailed, "TLS verification failed" }},
		{name: "row5_endpoint_mismatch_is_a_conflict", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			mutate: func(in *BindingInput) {
				in.Endpoint = controlplanev1.APIEndpoint{Host: "c999.eu-de.containers.cloud.ibm.com", Port: 30101}
				in.Proof = OwnershipProofProven
			}},
		{name: "row5_wrong_resource_group_is_a_conflict", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			mutate: func(in *BindingInput) {
				in.ResourceGroupID, in.Proof = "fedcba9876543210fedcba9876543210", OwnershipProofProven
			}},
		{name: "row5_wrong_name_is_a_conflict", action: BindingTerminal, row: "7",
			reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			mutate: func(in *BindingInput) { in.Name, in.Proof = "other-name", OwnershipProofProven }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := bindingIKSInput(iks)
			in.Endpoint = endpoint
			tc.mutate(&in)
			g.Expect(in.validate()).To(Succeed())
			got := in.decideByName(true)
			g.Expect(got.Action).To(Equal(tc.action))
			g.Expect(got.Row).To(Equal(tc.row))
			g.Expect(got.Reason).To(Equal(tc.reason))
			g.Expect(got.ProofNeeded).To(Equal(tc.proofNeeded))
			g.Expect(got.Message).To(ContainSubstring(tc.message))
			if got.Action == BindingKnown {
				g.Expect(got.ClusterID).To(Equal(iks.ID))
			}
		})
	}
}

func TestDecideBindingValidation(t *testing.T) {
	valid := bindingIKSInput(nil)
	testCases := []struct {
		name   string
		mutate func(*BindingInput)
		want   string
	}{
		{name: "no name", mutate: func(in *BindingInput) { in.Name = "" }, want: "BindingInput.Name is empty"},
		{name: "no resource group", mutate: func(in *BindingInput) { in.ResourceGroupID = "" }, want: "BindingInput.ResourceGroupID is empty"},
		{name: "unknown distribution", mutate: func(in *BindingInput) { in.Distribution = "Other" }, want: `BindingInput.Distribution is "Other"`},
		{name: "no clock", mutate: func(in *BindingInput) { in.Now = time.Time{} }, want: "BindingInput.Now is not set"},
		{name: "unknown proof", mutate: func(in *BindingInput) { in.Proof = "Maybe" }, want: `BindingInput.Proof is "Maybe"`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := valid
			tc.mutate(&in)
			_, err := DecideBinding(in)
			g.Expect(err).To(MatchError(ContainSubstring(tc.want)))
		})
	}
	t.Run("the zero proof means not attempted", func(t *testing.T) {
		g := NewWithT(t)
		in := valid
		in.Proof = ""
		_, err := DecideBinding(in)
		g.Expect(err).NotTo(HaveOccurred())
	})
}

func TestBindingInputLookup(t *testing.T) {
	base := bindingIKSInput(nil)
	testCases := []struct {
		name   string
		mutate func(*BindingInput)
		want   BindingLookup
	}{
		{name: "status ID first, without resource group", mutate: func(in *BindingInput) { in.StatusClusterID, in.SecretClusterID = "status-id", "secret-id" },
			want: BindingLookup{IDOrName: "status-id", ByID: true}},
		{name: "secret ID after a move", mutate: func(in *BindingInput) { in.SecretClusterID = "secret-id" },
			want: BindingLookup{IDOrName: "secret-id", ByID: true}},
		{name: "name in the resource group", mutate: func(*BindingInput) {},
			want: BindingLookup{IDOrName: "example-test-cluster-probe-iks", ResourceGroupID: "0123456789abcdef0123456789abcdef"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := base
			tc.mutate(&in)
			g.Expect(in.Lookup()).To(Equal(tc.want))
		})
	}
}

func TestEffectiveClusterName(t *testing.T) {
	testCases := []struct {
		name        string
		ibmName     string
		clusterName string
		want        string
		wantErr     []string
	}{
		{name: "spec.ibmCloudClusterName wins", ibmName: "prod-eu.cluster-1", clusterName: "dev", want: "prod-eu.cluster-1"},
		{name: "Cluster name when omitted", clusterName: "dev-iks", want: "dev-iks"},
		{name: "32 characters, IBM's limit", clusterName: strings.Repeat("a", 32), want: strings.Repeat("a", 32)},
		// The name IBM refused with HTTP 400, code E0125, when the controller still allowed 35 characters.
		{name: "33-character Cluster name", clusterName: "example-test-cluster-e01-mgmt-iks", wantErr: []string{
			`the IBM cluster name "example-test-cluster-e01-mgmt-iks", taken from the Cluster's metadata.name ` +
				"(spec.ibmCloudClusterName is omitted), has 33 characters, more than 32; " + ibmclustername.Rule +
				"; CAPIBM sends no request to IBM with this name, and spec.ibmCloudClusterName cannot be added or " +
				"changed after creation, so delete this Cluster and create it again with spec.ibmCloudClusterName set " +
				"to a name that follows the rule"}},
		{name: "33-character spec.ibmCloudClusterName", ibmName: strings.Repeat("a", 33), clusterName: "dev",
			wantErr: []string{"taken from spec.ibmCloudClusterName, has 33 characters"}},
		{name: "Cluster name starting with a digit", clusterName: "1dev", wantErr: []string{`"1dev"`, "starts with '1' instead of a letter"}},
		{name: "underscore and dot are IBM name characters", ibmName: "dev_iks.eu", clusterName: "dev", want: "dev_iks.eu"},
		{name: "upper case is allowed", ibmName: "Dev-IKS", clusterName: "dev", want: "Dev-IKS"},
		{name: "invalid character", ibmName: "dev iks", wantErr: []string{`contains ' '`, "taken from spec.ibmCloudClusterName"}},
		{name: "unicode letter", ibmName: "clüster", wantErr: []string{`contains 'ü'`}},
		{name: "both empty", wantErr: []string{"is empty"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := EffectiveClusterName(tc.ibmName, tc.clusterName)
			if len(tc.wantErr) > 0 {
				g.Expect(err).To(HaveOccurred())
				for _, part := range tc.wantErr {
					g.Expect(err.Error()).To(ContainSubstring(part))
				}
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestEndpointMatches(t *testing.T) {
	iks := bindingCluster(t, "iks-getCluster-normal.json")
	roks := bindingCluster(t, "roks-getCluster-normal.json")
	noPort := bindingWith(iks, func(c *kubernetesservice.Cluster) {
		c.MasterURL, c.PublicServiceEndpointURL, c.PrivateServiceEndpointURL = "https://c200.eu-de.containers.cloud.ibm.com", "", "not a url\x7f"
	})
	testCases := []struct {
		name           string
		cluster        *kubernetesservice.Cluster
		endpoint       controlplanev1.APIEndpoint
		recordedServer string
		want           bool
	}{
		{name: "master URL equals the endpoint", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 30101}, want: true},
		{name: "ROKS -e master URL", cluster: roks, endpoint: controlplanev1.APIEndpoint{Host: "c113-e.eu-de.containers.cloud.ibm.com", Port: 30102}, want: true},
		{name: "private service endpoint equals the endpoint", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "c111.private.eu-de.containers.cloud.ibm.com", Port: 30101}, want: true},
		{name: "host compares without case", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "C111.EU-DE.containers.cloud.ibm.com", Port: 30101}, want: true},
		{name: "another port", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 443}, want: false},
		{name: "another host", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "c999.eu-de.containers.cloud.ibm.com", Port: 30101}, want: false},
		{name: "secret server follows an IBM URL change", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "old.example.com", Port: 6443},
			recordedServer: "https://c111.eu-de.containers.cloud.ibm.com:30101", want: true},
		{name: "URL without a port means 443", cluster: noPort, endpoint: controlplanev1.APIEndpoint{Host: "c200.eu-de.containers.cloud.ibm.com", Port: 443}, want: true},
		{name: "secret server without a port means 443", cluster: noPort, recordedServer: "https://c200.eu-de.containers.cloud.ibm.com", want: true},
		{name: "http secret server never matches", cluster: iks, recordedServer: "http://c111.eu-de.containers.cloud.ibm.com:30101", want: false},
		{name: "unparseable secret server never matches", cluster: iks, recordedServer: "https://c111.eu-de.containers.cloud.ibm.com:30101/\x7f", want: false},
		{name: "secret server without a host never matches", cluster: iks, recordedServer: "https://:30101", want: false},
		{name: "endpoint without a port never matches", cluster: iks, endpoint: controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com"}, want: false},
		{name: "nothing recorded", cluster: iks, want: false},
		{name: "deploying cluster has no URLs", cluster: bindingCluster(t, "iks-getCluster-deploying.json"),
			endpoint: controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 30101}, want: false},
		{name: "nil cluster", cluster: nil, endpoint: controlplanev1.APIEndpoint{Host: "c111.eu-de.containers.cloud.ibm.com", Port: 30101}, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(EndpointMatches(tc.cluster, tc.endpoint, tc.recordedServer)).To(Equal(tc.want))
		})
	}
}
