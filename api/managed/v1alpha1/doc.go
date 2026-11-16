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

// Package v1alpha1 contains the infrastructure.cluster.x-k8s.io/v1alpha1 API of IBM Cloud managed Kubernetes: the
// InfraCluster IBMCloudManagedCluster and the InfraMachinePool IBMCloudManagedMachinePool of an IBM Cloud Kubernetes Service
// (IKS) or Red Hat OpenShift on IBM Cloud (ROKS) cluster on VPC Gen2. The matching control plane kind lives in
// api/managed/controlplane/v1alpha1.
//
// Why a separate package and version: IBM runs the control plane and the workers of a managed cluster, so these kinds
// share nothing with the self-managed VPC (api/vpc) and PowerVS (api/powervs) kinds of the same API group. They start
// at v1alpha1 because the feature is new and sits behind the ManagedKubernetes feature gate.
//
// Conventions (the PowerVS v1beta3 style): value types with omitempty/omitzero instead of pointers wherever the zero
// value is not a valid setting; +required or +optional on every field; no defaulting markers (an omitted field is
// documented and resolved in code, so the stored object always shows what the user wrote); string enums instead of
// bools, except for the CAPI contract flags; list-maps instead of maps; MaxLength/MaxItems on every string and list;
// doc comments that start with the JSON name; create-only rules written at the spec level when a field-level
// transition rule would not run (a field added after creation). The scheme is hand-written (groupversion_info.go) and
// each types file appends its kinds to objectTypes, as in api/powervs/v1beta3.
//
// Why string fields carry a MinLength: a string is a value with omitempty, not a pointer, so the Kube API linter (its
// requiredfields and optionalfields rules) requires the schema to reject the empty string. Where a Pattern already
// rejects "", the MinLength changes nothing; the linter just does not derive a minimum from a pattern. The status
// strings that copy values observed in IBM Cloud (resolved IDs and names, zone names, CIDR blocks, the pool ID, the
// target worker version) have no Pattern, so there MinLength=1 also rejects an explicit "". CAPIBM never writes one:
// omitempty leaves an empty string out of the request, so a value IBM does not report shows as an absent field.
// +kubebuilder:object:generate=true
// +groupName=infrastructure.cluster.x-k8s.io
package v1alpha1
