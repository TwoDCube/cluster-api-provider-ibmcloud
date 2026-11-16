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

// Package v1alpha1 contains the controlplane.cluster.x-k8s.io/v1alpha1 API of IBM Cloud managed Kubernetes: the
// IBMCloudManagedControlPlane kind, which stands for one IBM Cloud Kubernetes Service (IKS) or Red Hat OpenShift on IBM
// Cloud (ROKS) cluster on VPC Gen2. IBM runs the master; this kind creates, observes, upgrades and deletes the IBM
// cluster and publishes the CAPI control plane contract (endpoint, kubeconfig Secret, versions, initialization).
//
// Why its own package: CAPI resolves a control plane through the controlplane.cluster.x-k8s.io group, so the kind
// cannot live with the infrastructure kinds of api/managed/v1alpha1. It starts at v1alpha1 because the feature is new
// and sits behind the ManagedKubernetes feature gate.
//
// Version model: the spec has no CAPI spec.version. IBM selects and applies master patches itself, so the only version
// a user controls is the IBM product minor in spec.distributionVersion; status reports the IBM version and,
// separately, the Kubernetes version the API server answers with.
//
// Conventions are those of api/managed/v1alpha1 and the PowerVS v1beta3 style: +required or +optional on every
// field, no defaulting markers, MaxLength/MaxItems everywhere, doc comments that start with the JSON name, create-only
// rules at the spec level where a field-level transition rule would not run, and a hand-written scheme
// (groupversion_info.go) that each types file extends through objectTypes.
//
// String fields carry a MinLength for the reason given in api/managed/v1alpha1: a string is a value with omitempty,
// not a pointer, so the Kube API linter (its requiredfields and optionalfields rules) requires the schema to reject
// the empty string. Where a Pattern already rejects "", the MinLength changes nothing; the linter just does not derive
// a minimum from a pattern. The status strings clusterID, clusterCRN, state, distributionVersion,
// targetDistributionVersion, distributionVersionEndOfSupport, master.state, master.health and createIntent.clusterName
// have no Pattern, so there MinLength=1 also rejects an explicit "". CAPIBM never writes one: omitempty leaves an
// empty string out of the request, so an empty IBM value (master.health, which IBM Cloud was observed to leave empty
// for the first minutes after the master is Ready, or a versionEOS that IBM does not report) shows as an absent field.
// +kubebuilder:object:generate=true
// +groupName=controlplane.cluster.x-k8s.io
package v1alpha1
