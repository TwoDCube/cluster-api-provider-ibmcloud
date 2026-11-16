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

// This file holds the value types shared by the kinds of this package.

// SourceType selects how an IBM Cloud resource is sourced. v1alpha1 supports Reference only. Provision can be added
// later: adding an enum value relaxes validation, so existing objects stay valid.
// +kubebuilder:validation:Enum=Reference
type SourceType string

const (
	// SourceTypeReference uses an existing resource.
	SourceTypeReference SourceType = "Reference"
)

// ResourceIdentifier identifies an existing IBM Cloud resource by exactly one of id or name (PowerVS v1beta3 shape).
// +kubebuilder:validation:MinProperties=1
// +kubebuilder:validation:XValidation:rule="(has(self.id) ? 1 : 0) + (has(self.name) ? 1 : 0) == 1",message="exactly one of id or name must be specified"
type ResourceIdentifier struct {
	// id of the resource.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	ID string `json:"id,omitempty"`

	// name of the resource.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name,omitempty"`
}

// ResourceGroupReference identifies an existing resource group by ID. A name alternative can be added later with an
// "exactly one" rule; every existing object already has an id, so that change is compatible.
type ResourceGroupReference struct {
	// id is the 32-character resource group ID.
	// +required
	// +kubebuilder:validation:MinLength=32
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{32}$`
	ID string `json:"id,omitempty"`
}

// VPCSource sources the VPC Gen2 network.
// +kubebuilder:validation:XValidation:rule="self.type == 'Reference' ? has(self.reference) : !has(self.reference)",message="reference is required when type is Reference, and forbidden otherwise"
type VPCSource struct {
	// type is Reference (an existing VPC).
	// +required
	Type SourceType `json:"type,omitempty"`

	// reference identifies the existing VPC by id or name.
	// +optional
	Reference ResourceIdentifier `json:"reference,omitempty,omitzero"`
}

// ZoneSubnet binds a VPC zone to one existing subnet.
type ZoneSubnet struct {
	// name is the VPC zone, for example us-south-1.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Pattern=`^[a-z]{2}-[a-z]+-[0-9]+$`
	Name string `json:"name,omitempty"`

	// subnet is an existing subnet of spec.vpc in this zone. Give it at least 256 addresses; IBM cannot resize it later.
	// +required
	Subnet ResourceIdentifier `json:"subnet,omitempty,omitzero"`
}

// ResolvedResource is an observed IBM Cloud resource.
// +kubebuilder:validation:MinProperties=1
type ResolvedResource struct {
	// id of the resource.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	ID string `json:"id,omitempty"`

	// name of the resource.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name,omitempty"`
}

// FailureDomain is the CAPI v1beta2 InfraCluster failure-domain shape without the attributes map (KAL forbids maps).
type FailureDomain struct {
	// name is the VPC zone.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	Name string `json:"name,omitempty"`
}
