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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This file is the InfraCluster of a managed cluster. It only references an existing VPC and existing subnets: CAPIBM
// creates and deletes nothing in the VPC. It has no spec.controlPlaneEndpoint because the control plane publishes the
// endpoint, and the CAPI Cluster controller tolerates its absence on the InfraCluster
// (cluster_controller_phases.go:211-228).

func init() {
	objectTypes = append(objectTypes, &IBMCloudManagedCluster{}, &IBMCloudManagedClusterList{})
}

// IBMCloudManagedClusterSpec is the existing VPC Gen2 network an IKS or ROKS cluster uses. CAPIBM creates nothing here.
// +kubebuilder:validation:XValidation:rule="self.zones.all(z, z.name.startsWith(self.region + '-'))",message="every zone must belong to region, for example us-south-1 for us-south"
type IBMCloudManagedClusterSpec struct {
	// region is the IBM Cloud multizone region, for example us-south. Immutable.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z]{2}-[a-z]+$`
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="region is immutable"
	Region string `json:"region,omitempty"`

	// resourceGroup is the resource group of the IBM cluster (X-Auth-Resource-Group). Immutable.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="resourceGroup is immutable"
	ResourceGroup ResourceGroupReference `json:"resourceGroup,omitempty,omitzero"`

	// vpc is the existing VPC the workers attach to. Immutable.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="vpc is immutable"
	VPC VPCSource `json:"vpc,omitempty,omitzero"`

	// zones maps each zone the cluster may use to one subnet. Zones can be added. In v1alpha1 an existing zone can be
	// neither removed nor pointed at another subnet (IBM has no API to move a pool zone to another subnet).
	// +required
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:XValidation:rule="oldSelf.all(o, self.exists(z, z.name == o.name && z.subnet == o.subnet))",message="zones can be added; existing zones cannot be removed or changed in v1alpha1"
	Zones []ZoneSubnet `json:"zones,omitempty"`
}

// PublicGatewayState reports whether a subnet has a public gateway (needed for public egress).
// +kubebuilder:validation:Enum=Attached;NotAttached
type PublicGatewayState string

const (
	// PublicGatewayStateAttached means the subnet has a public gateway.
	PublicGatewayStateAttached PublicGatewayState = "Attached"
	// PublicGatewayStateNotAttached means the subnet has no public gateway, so its workers have no public egress.
	PublicGatewayStateNotAttached PublicGatewayState = "NotAttached"
)

// ResolvedZone is one validated zone.
type ResolvedZone struct {
	// name is the zone.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=40
	Name string `json:"name,omitempty"`

	// subnet is the resolved subnet.
	// +required
	Subnet ResolvedResource `json:"subnet,omitempty,omitzero"`

	// ipv4CIDRBlock of the subnet (used for overlap checks with Cluster.spec.clusterNetwork).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=43
	IPv4CIDRBlock string `json:"ipv4CIDRBlock,omitempty"`

	// publicGateway reports a public gateway on the subnet (informational).
	// +optional
	PublicGateway PublicGatewayState `json:"publicGateway,omitempty"`

	// availableIPv4Addresses at the last check; a condition message warns below 16.
	// +optional
	// +kubebuilder:validation:Minimum=0
	AvailableIPv4Addresses *int32 `json:"availableIPv4Addresses,omitempty"`
}

// IBMCloudManagedClusterStatus is fully re-derivable from spec plus VPC lookups (safe for clusterctl move).
// +kubebuilder:validation:MinProperties=1
type IBMCloudManagedClusterStatus struct {
	// conditions are Ready (the summary), NetworkReady and Paused.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=32
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// initialization is the CAPI v1beta2 contract block.
	// +optional
	Initialization IBMCloudManagedClusterInitializationStatus `json:"initialization,omitempty,omitzero"`

	// failureDomains lists the zones (CAPI InfraCluster contract, list shape).
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=3
	FailureDomains []FailureDomain `json:"failureDomains,omitempty"`

	// vpc is the resolved VPC.
	// +optional
	VPC ResolvedResource `json:"vpc,omitempty,omitzero"`

	// zones are the resolved zone subnets the control plane and pools read (explicit inheritance).
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=3
	Zones []ResolvedZone `json:"zones,omitempty"`
}

// IBMCloudManagedClusterInitializationStatus is the CAPI v1beta2 contract block.
// +kubebuilder:validation:MinProperties=1
type IBMCloudManagedClusterInitializationStatus struct {
	// provisioned is true once every reference resolved and validated. It never goes back to false.
	// It is a *bool because the CAPI InfraCluster contract defines it so (a KAL exclusion in .golangci-kal.yml).
	// +optional
	Provisioned *bool `json:"provisioned,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:path=ibmcloudmanagedclusters,scope=Namespaced,categories=cluster-api,shortName=ibmcmc
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".metadata.labels.cluster\\.x-k8s\\.io/cluster-name",description="Cluster to which this IBMCloudManagedCluster belongs"
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".spec.region",description="IBM Cloud region"
// +kubebuilder:printcolumn:name="VPC",type="string",JSONPath=".status.vpc.id",description="Resolved VPC ID"
// +kubebuilder:printcolumn:name="Provisioned",type="boolean",JSONPath=".status.initialization.provisioned",description="Every network reference resolved and validated"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time duration since creation of IBMCloudManagedCluster"

// IBMCloudManagedCluster is the InfraCluster of an IBM Cloud managed (IKS or ROKS) cluster.
type IBMCloudManagedCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec is the desired network inventory.
	// +required
	Spec IBMCloudManagedClusterSpec `json:"spec,omitempty,omitzero"`

	// status is the observed state.
	// +optional
	Status IBMCloudManagedClusterStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// IBMCloudManagedClusterList contains a list of IBMCloudManagedCluster.
type IBMCloudManagedClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []IBMCloudManagedCluster `json:"items"`
}

// GetConditions returns the v1beta2 conditions of the IBMCloudManagedCluster. It implements the CAPI conditions.Getter
// interface, so util/conditions and the patch helper can read the conditions.
func (r *IBMCloudManagedCluster) GetConditions() []metav1.Condition {
	return r.Status.Conditions
}

// SetConditions replaces the v1beta2 conditions of the IBMCloudManagedCluster with conditions. It implements the CAPI
// conditions.Setter interface; callers normally use util/conditions.Set instead of calling it directly.
func (r *IBMCloudManagedCluster) SetConditions(conditions []metav1.Condition) {
	r.Status.Conditions = conditions
}
