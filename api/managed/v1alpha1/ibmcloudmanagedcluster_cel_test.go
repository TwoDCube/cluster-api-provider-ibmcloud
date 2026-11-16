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
	"k8s.io/utils/ptr"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// The resource group ID below is synthetic (32 hex characters); tests never carry real account data.
const testResourceGroupID = "0123456789abcdef0123456789abcdef"

// newIBMCloudManagedCluster returns a valid IBMCloudManagedCluster in namespace: region us-south, a VPC and one subnet by name.
func newIBMCloudManagedCluster(namespace string) *infrav1.IBMCloudManagedCluster {
	return &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "ibmcmc-", Namespace: namespace},
		Spec: infrav1.IBMCloudManagedClusterSpec{
			Region:        "us-south",
			ResourceGroup: infrav1.ResourceGroupReference{ID: testResourceGroupID},
			VPC: infrav1.VPCSource{
				Type:      infrav1.SourceTypeReference,
				Reference: infrav1.ResourceIdentifier{Name: "test-vpc"},
			},
			Zones: []infrav1.ZoneSubnet{
				{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{Name: "test-subnet-1"}},
			},
		},
	}
}

// zone returns a ZoneSubnet whose subnet is given by name.
func zone(name, subnet string) infrav1.ZoneSubnet {
	return infrav1.ZoneSubnet{Name: name, Subnet: infrav1.ResourceIdentifier{Name: subnet}}
}

// TestIBMCloudManagedClusterCreateValidation covers the create-time CEL and OpenAPI rules of IBMCloudManagedCluster and of the
// shared ResourceIdentifier and VPCSource types.
func TestIBMCloudManagedClusterCreateValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmc-create")

	testCases := []struct {
		name    string
		mutate  func(c *infrav1.IBMCloudManagedCluster)
		wantErr string
	}{
		{
			name:   "minimal_valid_object_accepted",
			mutate: func(_ *infrav1.IBMCloudManagedCluster) {},
		},
		{
			name: "vpc_by_id_and_three_zones_accepted",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.VPC.Reference = infrav1.ResourceIdentifier{ID: "r006-00000000-0000-0000-0000-000000000000"}
				c.Spec.Zones = []infrav1.ZoneSubnet{
					zone("us-south-1", "subnet-1"), zone("us-south-2", "subnet-2"), zone("us-south-3", "subnet-3"),
				}
			},
		},
		{
			name: "zone_outside_region_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{zone("eu-de-1", "subnet-1")}
			},
			wantErr: "every zone must belong to region",
		},
		{
			// startsWith(region + '-') and not startsWith(region): us-southeast-1 is not a us-south zone.
			name: "zone_of_region_with_same_prefix_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{zone("us-southeast-1", "subnet-1")}
			},
			wantErr: "every zone must belong to region",
		},
		{
			name: "zone_name_not_a_zone_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{zone("us-south", "subnet-1")}
			},
			wantErr: "spec.zones[0].name",
		},
		{
			name:    "no_zones_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.Zones = nil },
			wantErr: "spec.zones: Required value",
		},
		{
			name: "four_zones_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{
					zone("us-south-1", "s1"), zone("us-south-2", "s2"), zone("us-south-3", "s3"), zone("us-south-4", "s4"),
				}
			},
			wantErr: "spec.zones: Too many",
		},
		{
			name: "duplicate_zone_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{zone("us-south-1", "s1"), zone("us-south-1", "s2")}
			},
			wantErr: "Duplicate value",
		},
		{
			name:    "region_not_a_region_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.Region = "us-south-1" },
			wantErr: "spec.region",
		},
		{
			name:    "resource_group_id_not_32_hex_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.ResourceGroup.ID = "Default" },
			wantErr: "spec.resourceGroup.id",
		},
		{
			name:    "resource_group_missing_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.ResourceGroup = infrav1.ResourceGroupReference{} },
			wantErr: "spec.resourceGroup: Required value",
		},
		{
			name:    "vpc_reference_missing_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.VPC.Reference = infrav1.ResourceIdentifier{} },
			wantErr: "reference is required when type is Reference",
		},
		{
			name:    "vpc_type_not_in_enum_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.VPC.Type = "Provision" },
			wantErr: "Unsupported value: \"Provision\"",
		},
		{
			name: "resource_identifier_with_id_and_name_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.VPC.Reference = infrav1.ResourceIdentifier{ID: "r006-1", Name: "test-vpc"}
			},
			wantErr: "exactly one of id or name must be specified",
		},
		{
			name: "subnet_with_id_and_name_rejected",
			mutate: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones[0].Subnet = infrav1.ResourceIdentifier{ID: "0717-1", Name: "test-subnet-1"}
			},
			wantErr: "exactly one of id or name must be specified",
		},
		{
			name:    "subnet_missing_rejected",
			mutate:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.Zones[0].Subnet = infrav1.ResourceIdentifier{} },
			wantErr: "spec.zones[0].subnet: Required value",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedCluster(namespace)
			tc.mutate(obj)
			expectAPIResult(g, testEnv.Create(ctx, obj), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedClusterUpdateValidation covers the transition rules: region, resource group and VPC are immutable,
// zones can only be added, and every zone, old or new, stays inside the region.
func TestIBMCloudManagedClusterUpdateValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmc-update")

	testCases := []struct {
		name    string
		initial func(c *infrav1.IBMCloudManagedCluster)
		update  func(c *infrav1.IBMCloudManagedCluster)
		wantErr string
	}{
		{
			name:   "metadata_change_allowed",
			update: func(c *infrav1.IBMCloudManagedCluster) { c.Labels = map[string]string{"example": "label"} },
		},
		{
			name:    "region_immutable",
			update:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.Region = "eu-de"; c.Spec.Zones[0].Name = "eu-de-1" },
			wantErr: "region is immutable",
		},
		{
			name:    "resource_group_immutable",
			update:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.ResourceGroup.ID = "fedcba9876543210fedcba9876543210" },
			wantErr: "resourceGroup is immutable",
		},
		{
			name:    "vpc_name_immutable",
			update:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.VPC.Reference.Name = "other-vpc" },
			wantErr: "vpc is immutable",
		},
		{
			name: "vpc_switch_from_name_to_id_immutable",
			update: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.VPC.Reference = infrav1.ResourceIdentifier{ID: "r006-00000000-0000-0000-0000-000000000000"}
			},
			wantErr: "vpc is immutable",
		},
		{
			name: "zone_added_allowed",
			update: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = append(c.Spec.Zones, zone("us-south-2", "test-subnet-2"))
			},
		},
		{
			name: "zone_added_outside_region_rejected",
			update: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = append(c.Spec.Zones, zone("eu-de-2", "test-subnet-2"))
			},
			wantErr: "every zone must belong to region",
		},
		{
			name: "zone_removed_rejected",
			initial: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = append(c.Spec.Zones, zone("us-south-2", "test-subnet-2"))
			},
			update:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.Zones = c.Spec.Zones[:1] },
			wantErr: "zones can be added; existing zones cannot be removed or changed",
		},
		{
			name:    "zone_subnet_changed_rejected",
			update:  func(c *infrav1.IBMCloudManagedCluster) { c.Spec.Zones[0].Subnet.Name = "other-subnet" },
			wantErr: "zones can be added; existing zones cannot be removed or changed",
		},
		{
			name: "zone_replaced_rejected",
			update: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{zone("us-south-2", "test-subnet-1")}
			},
			wantErr: "zones can be added; existing zones cannot be removed or changed",
		},
		{
			name: "zones_reordered_allowed",
			initial: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = append(c.Spec.Zones, zone("us-south-2", "test-subnet-2"))
			},
			update: func(c *infrav1.IBMCloudManagedCluster) {
				c.Spec.Zones = []infrav1.ZoneSubnet{c.Spec.Zones[1], c.Spec.Zones[0]}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedCluster(namespace)
			if tc.initial != nil {
				tc.initial(obj)
			}
			g.Expect(testEnv.Create(ctx, obj)).To(Succeed())
			tc.update(obj)
			expectAPIResult(g, testEnv.Update(ctx, obj), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedClusterStatusValidation proves that the status schema accepts every field the controller and the
// CAPI InfraCluster contract write (initialization.provisioned, failureDomains) and still validates enums.
func TestIBMCloudManagedClusterStatusValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmc-status")

	testCases := []struct {
		name    string
		status  infrav1.IBMCloudManagedClusterStatus
		wantErr string
	}{
		{
			name: "contract_and_observed_fields_accepted",
			status: infrav1.IBMCloudManagedClusterStatus{
				Conditions: []metav1.Condition{{
					Type:               infrav1.IBMCloudManagedClusterNetworkReadyCondition,
					Status:             metav1.ConditionTrue,
					Reason:             infrav1.IBMCloudManagedClusterNetworkReadyReason,
					LastTransitionTime: metav1.Now(),
				}},
				Initialization: infrav1.IBMCloudManagedClusterInitializationStatus{Provisioned: ptr.To(true)},
				FailureDomains: []infrav1.FailureDomain{{Name: "us-south-1"}},
				VPC:            infrav1.ResolvedResource{ID: "r006-1", Name: "test-vpc"},
				Zones: []infrav1.ResolvedZone{{
					Name:                   "us-south-1",
					Subnet:                 infrav1.ResolvedResource{ID: "0717-1", Name: "test-subnet-1"},
					IPv4CIDRBlock:          "10.240.0.0/24",
					PublicGateway:          infrav1.PublicGatewayStateNotAttached,
					AvailableIPv4Addresses: ptr.To[int32](0),
				}},
			},
		},
		{
			name: "public_gateway_not_in_enum_rejected",
			status: infrav1.IBMCloudManagedClusterStatus{
				Zones: []infrav1.ResolvedZone{{
					Name:          "us-south-1",
					Subnet:        infrav1.ResolvedResource{ID: "0717-1"},
					PublicGateway: "Yes",
				}},
			},
			wantErr: "Unsupported value: \"Yes\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedCluster(namespace)
			g.Expect(testEnv.Create(ctx, obj)).To(Succeed())
			obj.Status = tc.status
			expectAPIResult(g, testEnv.Status().Update(ctx, obj), tc.wantErr)
			if tc.wantErr == "" {
				g.Expect(obj.Status.Initialization.Provisioned).To(HaveValue(BeTrue()))
				g.Expect(obj.Status.Zones[0].AvailableIPv4Addresses).To(HaveValue(BeZero()))
			}
		})
	}
}
