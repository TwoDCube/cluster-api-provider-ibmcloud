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
	"strings"
	"testing"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
)

// testCOSInstanceCRN is a synthetic Cloud Object Storage instance CRN; tests never carry real account data.
const testCOSInstanceCRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/00000000000000000000000000000000:" +
	"00000000-0000-0000-0000-000000000000::"

// Messages of the CEL rules under test, kept in one place so a reworded rule fails loudly in every case.
const (
	distributionRuleMessage    = "distribution Kubernetes needs a 1.x distributionVersion and no openShift"
	distributionVersionMessage = "distributionVersion must be MAJOR.MINOR"
	clusterNamePresenceMessage = "ibmCloudClusterName cannot be added or removed after creation"
	clusterNameRuleMessage     = "ibmCloudClusterName must start with a letter (a-z or A-Z) and contain only letters, digits, '-', '_' and '.'"
	publicEndpointMessage      = "endpoints.publicServiceEndpoint is immutable"
	endpointAddOnceMessage     = "controlPlaneEndpoint cannot be changed or removed once set"
	endpointHostPortMessage    = "controlPlaneEndpoint needs both host and port"
)

// newIKSControlPlane returns a valid IKS IBMCloudManagedControlPlane (distribution Kubernetes 1.36) in namespace.
func newIKSControlPlane(namespace string) *controlplanev1.IBMCloudManagedControlPlane {
	return &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "ibmcmcp-", Namespace: namespace},
		Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution:        controlplanev1.DistributionKubernetes,
			DistributionVersion: "1.36",
		},
	}
}

// newROKSControlPlane returns a valid ROKS IBMCloudManagedControlPlane (distribution OpenShift 4.21 with the required
// registry COS instance) in namespace.
func newROKSControlPlane(namespace string) *controlplanev1.IBMCloudManagedControlPlane {
	cp := newIKSControlPlane(namespace)
	cp.Spec.Distribution = controlplanev1.DistributionOpenShift
	cp.Spec.DistributionVersion = "4.21"
	cp.Spec.OpenShift = openShiftWithCOS(testCOSInstanceCRN)
	return cp
}

// withIBMCloudClusterName returns a constructor of a valid IKS control plane in namespace whose spec.ibmCloudClusterName
// is name, for the table rows of the IBM name rule.
func withIBMCloudClusterName(namespace, name string) func() *controlplanev1.IBMCloudManagedControlPlane {
	return func() *controlplanev1.IBMCloudManagedControlPlane {
		cp := newIKSControlPlane(namespace)
		cp.Spec.IBMCloudClusterName = name
		return cp
	}
}

// openShiftWithCOS returns the ROKS settings with the registry stored in the COS instance crn.
func openShiftWithCOS(crn string) controlplanev1.OpenShiftSpec {
	return controlplanev1.OpenShiftSpec{
		ImageRegistry: controlplanev1.ImageRegistrySpec{
			COS: controlplanev1.COSInstanceReference{InstanceCRN: crn},
		},
	}
}

// TestIBMCloudManagedControlPlaneCreateValidation covers the create-time CEL and OpenAPI rules: the distribution and
// distributionVersion pairing with openShift presence, the MAJOR.MINOR grammar, the required COS instance for
// OpenShift, the IBM name rules, the controlPlaneEndpoint shape (allowed at create for move), and the enum of
// deletion.persistentStorage.
func TestIBMCloudManagedControlPlaneCreateValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-create")

	testCases := []struct {
		name    string
		object  func() *controlplanev1.IBMCloudManagedControlPlane
		wantErr string
	}{
		{
			name:   "iks_valid",
			object: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
		},
		{
			name:   "roks_valid_with_cos",
			object: func() *controlplanev1.IBMCloudManagedControlPlane { return newROKSControlPlane(namespace) },
		},
		{
			name: "iks_with_all_optional_fields_valid",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.IBMCloudClusterName = "ns-1.my-cluster"
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointDisabled
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
				return cp
			},
		},
		{
			name: "persistent_storage_retain_valid",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageRetain
				return cp
			},
		},
		{
			name: "persistent_storage_delete_valid",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
				return cp
			},
		},
		{
			// IBM's own word for the parameter is a boolean; only the two enum values are accepted.
			name: "persistent_storage_not_in_enum_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Deletion.PersistentStorage = "true"
				return cp
			},
			wantErr: "Unsupported value: \"true\"",
		},
		{
			// Enum values are case-sensitive: a lower-case value is a typo, not Delete.
			name: "persistent_storage_lower_case_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Deletion.PersistentStorage = "delete"
				return cp
			},
			wantErr: "spec.deletion.persistentStorage",
		},
		{
			name: "kubernetes_with_openshift_major_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.DistributionVersion = "4.21"
				return cp
			},
			wantErr: distributionRuleMessage,
		},
		{
			name: "kubernetes_with_openshift_settings_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.OpenShift = openShiftWithCOS(testCOSInstanceCRN)
				return cp
			},
			wantErr: distributionRuleMessage,
		},
		{
			name: "kubernetes_major_ten_is_not_major_one_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.DistributionVersion = "10.1"
				return cp
			},
			wantErr: distributionRuleMessage,
		},
		{
			name: "openshift_with_kubernetes_major_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.DistributionVersion = "1.36"
				return cp
			},
			wantErr: distributionRuleMessage,
		},
		{
			// IBM rejects a ROKS create on VPC without a COS instance (HTTP 400, E7278), so the schema rejects it first.
			name: "openshift_without_cos_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.OpenShift = controlplanev1.OpenShiftSpec{}
				return cp
			},
			wantErr: distributionRuleMessage,
		},
		{
			name: "cos_crn_of_another_service_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.OpenShift = openShiftWithCOS(strings.Replace(testCOSInstanceCRN, "cloud-object-storage", "kms", 1))
				return cp
			},
			wantErr: "instanceCRN must be a Cloud Object Storage instance CRN",
		},
		{
			name: "cos_crn_without_crn_prefix_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.OpenShift = openShiftWithCOS("my-cos-instance:cloud-object-storage:")
				return cp
			},
			wantErr: "instanceCRN must be a Cloud Object Storage instance CRN",
		},
		{
			name: "distribution_not_in_enum_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Distribution = "EKS"
				return cp
			},
			wantErr: "Unsupported value: \"EKS\"",
		},
		{
			name: "distribution_version_missing_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.DistributionVersion = ""
				return cp
			},
			wantErr: "spec.distributionVersion: Required value",
		},
		{
			name: "ibm_cloud_cluster_name_starting_with_digit_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.IBMCloudClusterName = "1cluster"
				return cp
			},
			wantErr: clusterNameRuleMessage,
		},
		// IBM's rule as its API states it when it refuses a name: at most 32 characters, a letter first, then letters,
		// digits, '-', '_' and '.'. The schema must accept every name IBM accepts and refuse the others.
		{
			name:   "ibm_cloud_cluster_name_of_31_characters_allowed",
			object: withIBMCloudClusterName(namespace, "a"+strings.Repeat("b", 30)),
		},
		{
			name:   "ibm_cloud_cluster_name_of_32_characters_allowed",
			object: withIBMCloudClusterName(namespace, "a"+strings.Repeat("b", 31)),
		},
		{
			// The live refusal: IBM answered createCluster for a 33-character name with HTTP 400, code E0125.
			name:    "ibm_cloud_cluster_name_of_33_characters_rejected",
			object:  withIBMCloudClusterName(namespace, "a"+strings.Repeat("b", 32)),
			wantErr: "spec.ibmCloudClusterName: Too long: may not be more than 32",
		},
		{
			name:   "ibm_cloud_cluster_name_with_underscore_and_dot_allowed",
			object: withIBMCloudClusterName(namespace, "team_a.dev-1"),
		},
		{
			name:   "ibm_cloud_cluster_name_with_upper_case_allowed",
			object: withIBMCloudClusterName(namespace, "Team-A"),
		},
		{
			name:    "ibm_cloud_cluster_name_with_space_rejected",
			object:  withIBMCloudClusterName(namespace, "my cluster"),
			wantErr: clusterNameRuleMessage,
		},
		{
			name:    "ibm_cloud_cluster_name_with_unicode_letter_rejected",
			object:  withIBMCloudClusterName(namespace, "clüster"),
			wantErr: clusterNameRuleMessage,
		},
		{
			name:    "ibm_cloud_cluster_name_starting_with_hyphen_rejected",
			object:  withIBMCloudClusterName(namespace, "-cluster"),
			wantErr: clusterNameRuleMessage,
		},
		{
			name: "public_service_endpoint_not_in_enum_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Endpoints.PublicServiceEndpoint = "Private"
				return cp
			},
			wantErr: "Unsupported value: \"Private\"",
		},
		{
			// The add-once rule is a transition rule, so clusterctl move and restores can create with an endpoint.
			name: "control_plane_endpoint_at_create_allowed",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}
				return cp
			},
		},
		{
			name: "control_plane_endpoint_without_port_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com"}
				return cp
			},
			wantErr: endpointHostPortMessage,
		},
		{
			name: "control_plane_endpoint_without_host_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Port: 443}
				return cp
			},
			wantErr: endpointHostPortMessage,
		},
		{
			name: "control_plane_endpoint_port_out_of_range_rejected",
			object: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "example.com", Port: 65536}
				return cp
			},
			wantErr: "spec.controlPlaneEndpoint.port",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			expectAPIResult(g, testEnv.Create(ctx, tc.object()), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedControlPlaneEmptyDeletionRejected checks that spec.deletion is either omitted or names an option:
// "deletion: {}" says nothing and is refused, like an empty endpoints. A Go client never sends it (omitzero), so the
// object is sent unstructured, as kubectl apply of a manifest would send it.
func TestIBMCloudManagedControlPlaneEmptyDeletionRejected(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "ibmcmcp-deletion")
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newIKSControlPlane(namespace))
	g.Expect(err).NotTo(HaveOccurred())
	cp := &unstructured.Unstructured{Object: content}
	cp.SetAPIVersion(controlplanev1.GroupVersion.String())
	cp.SetKind("IBMCloudManagedControlPlane")
	g.Expect(unstructured.SetNestedMap(cp.Object, map[string]any{}, "spec", "deletion")).To(Succeed())

	expectAPIResult(g, testEnv.Create(ctx, cp), "spec.deletion: Invalid value")
}

// TestIBMCloudManagedControlPlaneDistributionVersionGrammar checks the MAJOR.MINOR rule of spec.distributionVersion: only
// an IBM product minor is accepted; patch versions, a v prefix, the IBM _openshift suffix and leading zeros are not.
func TestIBMCloudManagedControlPlaneDistributionVersionGrammar(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-version")

	testCases := []struct {
		name    string
		version string
		wantErr string
	}{
		{name: "minor_accepted", version: "1.36"},
		{name: "minor_zero_accepted", version: "1.0"},
		{name: "patch_version_rejected", version: "1.36.5", wantErr: distributionVersionMessage},
		{name: "v_prefix_rejected", version: "v1.36", wantErr: distributionVersionMessage},
		// Shorter than MinLength, so the OpenAPI length check may answer before the CEL rule: match the field path.
		{name: "major_only_rejected", version: "1.", wantErr: "spec.distributionVersion"},
		{name: "leading_zero_minor_rejected", version: "1.036", wantErr: distributionVersionMessage},
		{name: "leading_zero_major_rejected", version: "01.36", wantErr: distributionVersionMessage},
		{name: "build_suffix_rejected", version: "1.36_1537", wantErr: "spec.distributionVersion"},
		// Longer than MaxLength, so the OpenAPI length check may answer before the CEL rule: match the field path.
		{name: "openshift_suffix_rejected", version: "4.21_openshift", wantErr: "spec.distributionVersion"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := newIKSControlPlane(namespace)
			cp.Spec.DistributionVersion = tc.version
			expectAPIResult(g, testEnv.Create(ctx, cp), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedControlPlaneUpdateValidation covers the transition rules: immutability of distribution,
// ibmCloudClusterName and imageRegistry; presence symmetry of ibmCloudClusterName; effective-value immutability of
// endpoints.publicServiceEndpoint; controlPlaneEndpoint add-once; and that deletion.persistentStorage stays mutable
// within its enum. The minor step rule (+1 only, never down) needs the old and new values together with the gate and is
// the validating webhook's job, so here a raise and a lowering both pass CEL.
func TestIBMCloudManagedControlPlaneUpdateValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-update")
	endpoint := controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}

	testCases := []struct {
		name    string
		initial func() *controlplanev1.IBMCloudManagedControlPlane
		update  func(cp *controlplanev1.IBMCloudManagedControlPlane)
		wantErr string
	}{
		{
			name:    "distribution_immutable",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Distribution = controlplanev1.DistributionOpenShift
				cp.Spec.DistributionVersion = "4.21"
				cp.Spec.OpenShift = openShiftWithCOS(testCOSInstanceCRN)
			},
			wantErr: "distribution is immutable",
		},
		{
			name:    "distribution_version_raise_passes_cel",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.37" },
		},
		{
			// deletion.persistentStorage is mutable: CAPIBM reads it only when it sends IBM's DELETE, so it can be set
			// or changed at any time before the Cluster is deleted.
			name:    "persistent_storage_omitted_to_delete_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
			},
		},
		{
			name: "persistent_storage_delete_to_retain_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newROKSControlPlane(namespace)
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageRetain
			},
		},
		{
			name: "persistent_storage_delete_to_omitted_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.Deletion = controlplanev1.DeletionSpec{} },
		},
		{
			name:    "persistent_storage_not_in_enum_on_update_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.Deletion.PersistentStorage = "Keep" },
			wantErr: "Unsupported value: \"Keep\"",
		},
		{
			name:    "distribution_version_to_patch_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "1.37.1" },
			wantErr: distributionVersionMessage,
		},
		{
			name:    "distribution_version_to_other_major_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.DistributionVersion = "4.22" },
			wantErr: distributionRuleMessage,
		},
		{
			name: "ibm_cloud_cluster_name_immutable",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.IBMCloudClusterName = "first-name"
				return cp
			},
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.IBMCloudClusterName = "second-name" },
			wantErr: "ibmCloudClusterName is immutable",
		},
		{
			name:    "ibm_cloud_cluster_name_added_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.IBMCloudClusterName = "late-name" },
			wantErr: clusterNamePresenceMessage,
		},
		{
			name: "ibm_cloud_cluster_name_removed_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.IBMCloudClusterName = "first-name"
				return cp
			},
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.IBMCloudClusterName = "" },
			wantErr: clusterNamePresenceMessage,
		},
		{
			name:    "image_registry_immutable",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newROKSControlPlane(namespace) },
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.OpenShift = openShiftWithCOS(strings.Replace(testCOSInstanceCRN, "::", ":other::", 1))
			},
			wantErr: "imageRegistry is immutable",
		},
		{
			name:    "openshift_settings_removed_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newROKSControlPlane(namespace) },
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.OpenShift = controlplanev1.OpenShiftSpec{}
			},
			wantErr: distributionRuleMessage,
		},
		{
			// Omitted means Enabled, so writing the effective value explicitly is not a change.
			name:    "public_endpoint_omitted_to_enabled_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointEnabled
			},
		},
		{
			name: "public_endpoint_enabled_to_omitted_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointEnabled
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Endpoints = controlplanev1.EndpointsSpec{}
			},
		},
		{
			name:    "public_endpoint_omitted_to_disabled_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointDisabled
			},
			wantErr: publicEndpointMessage,
		},
		{
			name: "public_endpoint_disabled_to_omitted_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointDisabled
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Endpoints = controlplanev1.EndpointsSpec{}
			},
			wantErr: publicEndpointMessage,
		},
		{
			name: "public_endpoint_disabled_to_enabled_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointDisabled
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.Endpoints.PublicServiceEndpoint = controlplanev1.PublicServiceEndpointEnabled
			},
			wantErr: publicEndpointMessage,
		},
		{
			name:    "control_plane_endpoint_set_once_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane { return newIKSControlPlane(namespace) },
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.ControlPlaneEndpoint = endpoint },
		},
		{
			name: "control_plane_endpoint_rewritten_unchanged_allowed",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = endpoint
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.ControlPlaneEndpoint = endpoint },
		},
		{
			name: "control_plane_endpoint_changed_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = endpoint
				return cp
			},
			update:  func(cp *controlplanev1.IBMCloudManagedControlPlane) { cp.Spec.ControlPlaneEndpoint.Port = 443 },
			wantErr: endpointAddOnceMessage,
		},
		{
			name: "control_plane_endpoint_removed_rejected",
			initial: func() *controlplanev1.IBMCloudManagedControlPlane {
				cp := newIKSControlPlane(namespace)
				cp.Spec.ControlPlaneEndpoint = endpoint
				return cp
			},
			update: func(cp *controlplanev1.IBMCloudManagedControlPlane) {
				cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{}
			},
			wantErr: endpointAddOnceMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := tc.initial()
			g.Expect(testEnv.Create(ctx, cp)).To(Succeed())
			tc.update(cp)
			expectAPIResult(g, testEnv.Update(ctx, cp), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedControlPlaneStatusValidation proves that the status schema accepts every field the CAPI control plane
// contract and the controller write, and enforces the bounds the controller relies on.
func TestIBMCloudManagedControlPlaneStatusValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmcp-status")
	now := metav1.Now()

	testCases := []struct {
		name    string
		status  controlplanev1.IBMCloudManagedControlPlaneStatus
		wantErr string
	}{
		{
			name: "contract_and_cache_fields_accepted",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				Conditions: []metav1.Condition{{
					Type:               controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
					Status:             metav1.ConditionTrue,
					Reason:             controlplanev1.IBMCloudManagedControlPlaneAvailableReason,
					LastTransitionTime: now,
				}},
				Initialization:              controlplanev1.IBMCloudManagedControlPlaneInitializationStatus{ControlPlaneInitialized: ptr.To(true)},
				ExternalManagedControlPlane: ptr.To(true),
				Versions:                    []controlplanev1.StatusVersion{{Version: "v1.36.5+IKS"}},
				Version:                     "v1.36.5+IKS",
				ClusterID:                   "d0000000000000000000",
				ClusterCRN:                  "crn:v1:bluemix:public:containers-kubernetes:us-south:a/00000000000000000000000000000000:d0000000000000000000::",
				CreateIntent: controlplanev1.CreateIntent{
					ClusterName:         "my-cluster",
					InFlightAttemptTime: now,
				},
				NonCriticalCreateErrorCodes: []string{"E49df"},
				DeletionRequestTime:         now,
				MasterUpdate: controlplanev1.MasterUpdateRequest{
					Version:         "1.37",
					LastRequestTime: now,
					Attempts:        1,
				},
				State:                           "normal",
				Master:                          controlplanev1.MasterStatus{State: "deployed", Health: "normal"},
				DistributionVersion:             "1.36.5_1537",
				TargetDistributionVersion:       "1.36.5_1537",
				DistributionVersionEndOfSupport: "2027-06-30",
				AvailableDistributionVersions:   []string{"1.37", "1.38"},
			},
		},
		{
			// IBM Cloud was observed to report masterHealth "" right after the master reports Ready; the empty value is simply
			// omitted.
			name: "master_state_without_health_accepted",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				Master: controlplanev1.MasterStatus{State: "deployed"},
			},
		},
		{
			name: "two_versions_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				Versions: []controlplanev1.StatusVersion{{Version: "v1.36.5+IKS"}, {Version: "v1.36.6+IKS"}},
			},
			wantErr: "status.versions: Too many",
		},
		{
			name: "master_update_without_attempts_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				MasterUpdate: controlplanev1.MasterUpdateRequest{Version: "1.37", LastRequestTime: now},
			},
			wantErr: "status.masterUpdate.attempts: Required value",
		},
		{
			name: "create_intent_without_cluster_name_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				CreateIntent: controlplanev1.CreateIntent{InFlightAttemptTime: now},
			},
			wantErr: "status.createIntent.clusterName: Required value",
		},
		{
			// The intent records a name sent to IBM, and IBM refuses names longer than 32 characters.
			name: "create_intent_cluster_name_longer_than_ibm_allows_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				CreateIntent: controlplanev1.CreateIntent{ClusterName: "a" + strings.Repeat("b", 32), InFlightAttemptTime: now},
			},
			wantErr: "status.createIntent.clusterName: Too long",
		},
		{
			// The controller writes each offered minor once (the list is a set).
			name: "available_distribution_versions_with_a_duplicate_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				AvailableDistributionVersions: []string{"1.37", "1.37"},
			},
			wantErr: "status.availableDistributionVersions[1]: Duplicate value",
		},
		{
			name: "available_distribution_versions_beyond_the_bound_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				AvailableDistributionVersions: []string{"1.37", "1.38", "1.39", "1.40", "1.41", "1.42", "1.43", "1.44", "1.45",
					"1.46", "1.47", "1.48", "1.49", "1.50", "1.51", "1.52", "1.53"},
			},
			wantErr: "status.availableDistributionVersions: Too many",
		},
		{
			// MAJOR.MINOR only, as spec.distributionVersion: a fix pack is never listed.
			name: "available_distribution_version_longer_than_a_minor_rejected",
			status: controlplanev1.IBMCloudManagedControlPlaneStatus{
				AvailableDistributionVersions: []string{"1.37.1_1517"},
			},
			wantErr: "status.availableDistributionVersions[0]: Too long",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := newIKSControlPlane(namespace)
			g.Expect(testEnv.Create(ctx, cp)).To(Succeed())
			cp.Status = tc.status
			expectAPIResult(g, testEnv.Status().Update(ctx, cp), tc.wantErr)
		})
	}
}
