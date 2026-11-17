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

// This file is the control plane of a managed cluster. Rules that need other objects (the initial pool's size,
// entitlement only for OpenShift, a distributionVersion IBM offers) are controller guards, not schema rules, because
// CEL sees only this object.

func init() {
	objectTypes = append(objectTypes, &IBMCloudManagedControlPlane{}, &IBMCloudManagedControlPlaneList{})
}

const (
	// IBMCloudManagedControlPlaneFinalizer lets the controller delete the IBM cluster before the object goes away.
	IBMCloudManagedControlPlaneFinalizer = "ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io"

	// KubeconfigAnnotationPrefix prefixes the controller-owned metadata annotations on <cluster>-kubeconfig.
	KubeconfigAnnotationPrefix = "ibmcloudmanaged.controlplane.cluster.x-k8s.io/"

	// KubeconfigClusterIDAnnotation records the IBM cluster that issued the credential in the Secret. clusterctl move
	// carries the Secret with the control plane, so this is the durable copy of status.clusterID.
	KubeconfigClusterIDAnnotation = KubeconfigAnnotationPrefix + "cluster-id"

	// CreateHoldAnnotation is written by the IBMCloudManagedControlPlane controller, on the control plane, after IBM
	// definitively refused a create request (400, 401, 403, 422, A0656, or E0007 with no cluster found). Its value is a
	// time in RFC 3339 (UTC) before which the controller sends no new create request: the refusal changes status, and
	// without the hold the reconcile that change triggers would send the same request at once. The controller removes it
	// with its next create request and once the control plane is bound to an IBM cluster. It is an annotation, like the
	// kubeconfig annotations (KubeconfigAnnotationPrefix), because no status field is spent on it. Its effect is bounded:
	// the controller ignores a value more than 12 minutes ahead (its create refusal hold) and ends the hold on any spec
	// change, so an edit shifts the next create request by at most 12 minutes and can never stop creates.
	CreateHoldAnnotation = "ibmcloudmanaged.controlplane.cluster.x-k8s.io/create-hold-until"
)

// Distribution selects the IBM Cloud offering.
// +kubebuilder:validation:Enum=Kubernetes;OpenShift
type Distribution string

const (
	// DistributionKubernetes is IBM Cloud Kubernetes Service (IKS).
	DistributionKubernetes Distribution = "Kubernetes"
	// DistributionOpenShift is Red Hat OpenShift on IBM Cloud (ROKS).
	DistributionOpenShift Distribution = "OpenShift"
)

// PublicServiceEndpointPolicy enables or disables the public master endpoint at creation.
// +kubebuilder:validation:Enum=Enabled;Disabled
type PublicServiceEndpointPolicy string

const (
	// PublicServiceEndpointEnabled gives the master a public service endpoint; it is also the meaning of an omitted
	// endpoints.publicServiceEndpoint.
	PublicServiceEndpointEnabled PublicServiceEndpointPolicy = "Enabled"
	// PublicServiceEndpointDisabled makes the cluster private-only (not yet verified against IBM Cloud).
	PublicServiceEndpointDisabled PublicServiceEndpointPolicy = "Disabled"
)

// TODO: the distribution rule below admits only OpenShift 4.x, and the distributionVersion documentation says to raise
// it by exactly one minor. IBM's OpenShift line forks after 4.22 into 4.23 and 5.0, so the next version is no longer
// the next minor in IBM's /versions list but a node of an upgrade graph. IBM is developing a per-cluster upgrade-path
// endpoint for it (GraphQL, currently https://containers.cloud.ibm.com/unsafe/graphql). Once it is stable, admit
// OpenShift 5.x here and describe the step as "a version IBM offers as the next upgrade" (see the TODO on
// ibmversion.Minor.Next for the code that assumes the next minor).

// IBMCloudManagedControlPlaneSpec is one IKS or ROKS cluster on VPC Gen2. It has no CAPI spec.version: IBM selects the
// patch, so the only version a user controls is the IBM product minor in distributionVersion.
//
// The four rules below are spec-level on purpose: a field-level transition rule does not run when the field is added
// or removed after creation, which is exactly what the presence and effective-value rules must catch. The
// controlPlaneEndpoint rule is a transition rule and does not run on create, so a user CAN set controlPlaneEndpoint at
// create (clusterctl move and restores must). It is only a never-recreate marker: it never proves ownership.
// +kubebuilder:validation:XValidation:rule="self.distribution == 'Kubernetes' ? (self.distributionVersion.startsWith('1.') && !has(self.openShift)) : (self.distributionVersion.startsWith('4.') && has(self.openShift))",message="distribution Kubernetes needs a 1.x distributionVersion and no openShift; distribution OpenShift needs a 4.x distributionVersion and openShift.imageRegistry.cos.instanceCRN"
// +kubebuilder:validation:XValidation:rule="has(self.ibmCloudClusterName) == has(oldSelf.ibmCloudClusterName)",message="ibmCloudClusterName cannot be added or removed after creation"
// +kubebuilder:validation:XValidation:rule="(has(self.endpoints) && has(self.endpoints.publicServiceEndpoint) ? self.endpoints.publicServiceEndpoint : 'Enabled') == (has(oldSelf.endpoints) && has(oldSelf.endpoints.publicServiceEndpoint) ? oldSelf.endpoints.publicServiceEndpoint : 'Enabled')",message="endpoints.publicServiceEndpoint is immutable: IBM cannot switch a cluster between public and private-only"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.controlPlaneEndpoint) || (has(self.controlPlaneEndpoint) && self.controlPlaneEndpoint == oldSelf.controlPlaneEndpoint)",message="controlPlaneEndpoint cannot be changed or removed once set"
type IBMCloudManagedControlPlaneSpec struct {
	// distribution selects IKS (Kubernetes) or ROKS (OpenShift). Immutable.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="distribution is immutable"
	Distribution Distribution `json:"distribution,omitempty"`

	// distributionVersion is the IBM product minor, MAJOR.MINOR only: "1.36" for Kubernetes, "4.21" for OpenShift.
	// IBM deploys the newest patch of that minor and applies later master patches itself; a patch cannot be chosen,
	// and CAPIBM never requests or tracks master patches (it reports them in status). Raise it by exactly one minor to
	// upgrade the master; it can never go down. The controller never writes this field.
	// +required
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=8
	// +kubebuilder:validation:XValidation:rule="self.matches('^[1-9][0-9]*[.](0|[1-9][0-9]*)$')",message="distributionVersion must be MAJOR.MINOR, for example 1.36 or 4.21: IBM always applies the newest patch, so patch versions, a v prefix, _openshift and build suffixes are not accepted"
	DistributionVersion string `json:"distributionVersion,omitempty"`

	// openShift holds ROKS-only settings. Required for, and only valid with, distribution OpenShift.
	// +optional
	OpenShift OpenShiftSpec `json:"openShift,omitempty,omitzero"`

	// ibmCloudClusterName is the IBM Cloud cluster name. Omitted means the owning CAPI Cluster's metadata.name; that
	// derivation never changes in this API version. IBM's rule, which its API enforces when the cluster is created:
	// 1 to 32 characters, starting with a letter (a-z or A-Z), then letters, digits, '-', '_' and '.'; unique in the
	// account across regions. Keep the first 24 characters unique: re-creating the same prefix five times in seven
	// days hits the Let's Encrypt limit for the Ingress subdomain. Immutable.
	// When this field is omitted, the Cluster's name must follow IBM's rule too, and a Cluster name may be longer than
	// 32 characters or start with a digit: the admission webhook then rejects the control plane, or, when it cannot
	// tell the Cluster yet, the controller stops with InvalidConfiguration before any IBM call. A valid omitted name
	// gets an admission warning instead, because CAPI Cluster names are unique only per namespace; management clusters
	// with several namespaces should use a convention such as <namespace>-<name>.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-zA-Z][a-zA-Z0-9._-]*$')",message="ibmCloudClusterName must start with a letter (a-z or A-Z) and contain only letters, digits, '-', '_' and '.' (IBM's cluster name rule; at most 32 characters)"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ibmCloudClusterName is immutable"
	IBMCloudClusterName string `json:"ibmCloudClusterName,omitempty"`

	// endpoints configures the master service endpoints at creation.
	// +optional
	Endpoints EndpointsSpec `json:"endpoints,omitempty,omitzero"`

	// controlPlaneEndpoint is the CAPI contract field. The controller sets it once from the IBM master URL; do not set
	// it. Once set, CAPIBM treats the IBM cluster as existing and never creates one.
	// +optional
	ControlPlaneEndpoint APIEndpoint `json:"controlPlaneEndpoint,omitempty,omitzero"`
}

// OpenShiftSpec holds ROKS-only settings.
type OpenShiftSpec struct {
	// imageRegistry configures the storage of the OpenShift internal image registry. Required: IBM rejects a ROKS
	// create on VPC without a Cloud Object Storage instance (HTTP 400, E7278). Immutable: IBM attaches it only at
	// creation.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="imageRegistry is immutable: IBM attaches registry storage only at creation"
	ImageRegistry ImageRegistrySpec `json:"imageRegistry,omitempty,omitzero"`
}

// ImageRegistrySpec configures the internal registry. A struct (not a bare CRN) so that later options, such as
// deleting the bucket with the cluster, are additive.
type ImageRegistrySpec struct {
	// cos references the existing standard Cloud Object Storage instance in which IBM creates the registry bucket.
	// The bucket is retained when the cluster is deleted.
	// +required
	COS COSInstanceReference `json:"cos,omitempty,omitzero"`
}

// COSInstanceReference references a Cloud Object Storage instance.
type COSInstanceReference struct {
	// instanceCRN is the CRN of a standard COS instance (the CAPIBM service ID needs the COS Administrator role on it).
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.startsWith('crn:v1:') && self.contains(':cloud-object-storage:')",message="instanceCRN must be a Cloud Object Storage instance CRN"
	InstanceCRN string `json:"instanceCRN,omitempty"`
}

// EndpointsSpec configures master service endpoints.
// +kubebuilder:validation:MinProperties=1
type EndpointsSpec struct {
	// publicServiceEndpoint enables (when omitted) or disables the public master endpoint. With Disabled the cluster
	// is private-only and the management cluster must reach the private master URL (VPN, Direct Link, Transit Gateway
	// or running in IBM Cloud). Not yet verified against IBM Cloud. Immutable (spec-level rule compares effective values).
	// +optional
	PublicServiceEndpoint PublicServiceEndpointPolicy `json:"publicServiceEndpoint,omitempty"`
}

// APIEndpoint mirrors the CAPI APIEndpoint JSON shape. Both fields are required when it is set, because CAPI copies only
// a valid endpoint (host and non-zero port, cluster_types.go:1664-1666) and the add-once rule would freeze a partial one.
// +kubebuilder:validation:MinProperties=1
// +kubebuilder:validation:XValidation:rule="has(self.host) && has(self.port)",message="controlPlaneEndpoint needs both host and port"
type APIEndpoint struct {
	// host is the API server host name.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Host string `json:"host,omitempty"`

	// port is the API server port.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`
}

// IBMCloudManagedControlPlaneStatus holds the CAPI contract fields first; every other field is a re-derivable cache.
// +kubebuilder:validation:MinProperties=1
type IBMCloudManagedControlPlaneStatus struct {
	// conditions are Ready, Available, IBMClusterProvisioned, KubeconfigReady, MasterVersionUpToDate, RollingOut,
	// Deleting and Paused.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=32
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// initialization holds controlPlaneInitialized, the CAPI contract flag; sticky.
	// +optional
	Initialization IBMCloudManagedControlPlaneInitializationStatus `json:"initialization,omitempty,omitzero"`

	// externalManagedControlPlane is always true (CAPI contract for managed control planes). It is a *bool because
	// the CAPI control plane contract defines it so (a KAL exclusion in .golangci-kal.yml).
	// +optional
	ExternalManagedControlPlane *bool `json:"externalManagedControlPlane,omitempty"`

	// versions is the CAPI contract field: exactly one entry, the Kubernetes gitVersion the API server reports on
	// /version (IKS "v1.36.5+IKS", ROKS 4.21 "v1.34.9", as observed on IBM Cloud). Never an IBM or OpenShift version.
	// Empty until the API server answered once. While IBM rolls the master it keeps the lowest version seen (the
	// contract's minimum), so alternating answers from old and new API server replicas never flip it.
	// +optional
	// +listType=map
	// +listMapKey=version
	// +kubebuilder:validation:MaxItems=1
	Versions []StatusVersion `json:"versions,omitempty"`

	// version mirrors versions[0].version. It is the contract's deprecated fallback (control-plane.md:433-447),
	// kept so that tooling reading only status.version sees the same truthful Kubernetes version.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Version string `json:"version,omitempty"`

	// clusterID is the IBM cluster ID (cache; durable copy on the kubeconfig Secret).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	ClusterID string `json:"clusterID,omitempty"`

	// clusterCRN is the IBM cluster CRN; its account segment builds worker providerIDs.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	ClusterCRN string `json:"clusterCRN,omitempty"`

	// createIntent is written immediately BEFORE createCluster is sent and cleared once the cluster is observed by
	// ID, or once every attempt was answered definitively. It lets a lost response be told apart from a foreign
	// same-named cluster.
	// +optional
	CreateIntent CreateIntent `json:"createIntent,omitempty,omitzero"`

	// nonCriticalCreateErrorCodes are the IBM codes of the non_critical_errors a successful (201) createCluster
	// returned, for example E49df. The descriptions go to a Warning event; status keeps only stable codes.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=16
	NonCriticalCreateErrorCodes []string `json:"nonCriticalCreateErrorCodes,omitempty"`

	// deletionRequestTime is when the controller last sent DELETE for the IBM cluster. DeleteReissueAfter is measured
	// from it, never from a condition: lastTransitionTime does not move when only the reason changes (util/conditions
	// setter.go:64).
	// +optional
	DeletionRequestTime metav1.Time `json:"deletionRequestTime,omitempty,omitzero"`

	// masterUpdate records the latest master minor update CAPIBM requested. It is written BEFORE UpdateMaster is sent,
	// so a failed or lost update is retried with backoff instead of on every poll.
	// +optional
	MasterUpdate MasterUpdateRequest `json:"masterUpdate,omitempty,omitzero"`

	// state is IBM's cluster state token (deploying, deployed, pending, normal, warning, updating, ...).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	State string `json:"state,omitempty"`

	// master holds stable IBM master tokens. Free-text masterStatus is never stored.
	// +optional
	Master MasterStatus `json:"master,omitempty,omitzero"`

	// distributionVersion is IBM's master version without the "(pending)" marker, for example "1.36.5_1537" or
	// "4.21.35_1537_openshift". It moves when IBM applies a master patch on its own; CAPIBM never requests master patches.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	DistributionVersion string `json:"distributionVersion,omitempty"`

	// targetDistributionVersion is IBM's targetVersion for the master (equal to distributionVersion when no update is
	// pending, as observed on IBM Cloud).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	TargetDistributionVersion string `json:"targetDistributionVersion,omitempty"`

	// distributionVersionEndOfSupport is IBM's versionEOS for the master minor as IBM reports it (empty when IBM
	// reports none, as IBM Cloud was observed to do for 1.36 and 4.21).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	DistributionVersionEndOfSupport string `json:"distributionVersionEndOfSupport,omitempty"`

	// availableDistributionVersions lists the minors newer than the master's that IBM offers for this distribution,
	// MAJOR.MINOR in ascending order, for example ["1.37", "1.38"] or ["4.22"]. It is informational only: the master
	// stays up to date (MasterVersionUpToDate True) while it runs spec.distributionVersion, whatever this list holds,
	// and nothing is updated until spec.distributionVersion is raised, one step at a time. The list comes from IBM's
	// list of offered versions, which the controller reads at most once an hour per set of credentials and region, so
	// a new minor shows up here within about an hour. It is empty (omitted) when IBM offers nothing newer or the
	// controller has not read IBM's list yet; when IBM cannot be asked, the previous list is kept.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MinLength=3
	// +kubebuilder:validation:items:MaxLength=8
	AvailableDistributionVersions []string `json:"availableDistributionVersions,omitempty"`
}

// IBMCloudManagedControlPlaneInitializationStatus is the CAPI v1beta2 contract block.
// +kubebuilder:validation:MinProperties=1
type IBMCloudManagedControlPlaneInitializationStatus struct {
	// controlPlaneInitialized is true once spec.controlPlaneEndpoint is persisted and a verified kubeconfig Secret
	// carrying the cluster-id annotation exists. It never goes back to false. It is a *bool because the CAPI control
	// plane contract defines it so (a KAL exclusion in .golangci-kal.yml).
	// +optional
	ControlPlaneInitialized *bool `json:"controlPlaneInitialized,omitempty"`
}

// StatusVersion has the JSON shape of clusterv1.StatusVersion; replicas is never set for this control plane.
type StatusVersion struct {
	// version is a Kubernetes semantic version with the "v" prefix, as the API server reports it.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Version string `json:"version,omitempty"`
}

// CreateIntent records createCluster attempts whose outcome is not known. An attempt that IBM answered definitively
// (201, 409 E0007, ClearIntentStatusCodes, ClearIntentErrorCodes) never enters the adoption window; only attempts that
// may have created a cluster (timeout, 429, 5xx, a crash after the POST) bound it.
// +kubebuilder:validation:MinProperties=1
type CreateIntent struct {
	// clusterName is the IBM name the requests used.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	ClusterName string `json:"clusterName,omitempty"`

	// inFlightAttemptTime is written immediately BEFORE a POST. A definitive refusal clears it; a timeout, 429 or 5xx
	// moves it into the unknown-outcome bounds; a 201 leaves it (with status.clusterID) until the cluster is observed
	// by ID, as the reference for the read-after-create grace. If it is still set when no status.clusterID exists (a
	// crash or a lost status patch after the POST), that attempt's outcome is unknown and it counts in the window.
	// +optional
	InFlightAttemptTime metav1.Time `json:"inFlightAttemptTime,omitempty,omitzero"`

	// firstUnknownOutcomeTime is when the first attempt with an unknown outcome was sent.
	// +optional
	FirstUnknownOutcomeTime metav1.Time `json:"firstUnknownOutcomeTime,omitempty,omitzero"`

	// lastUnknownOutcomeTime is when the latest attempt with an unknown outcome was sent.
	// +optional
	LastUnknownOutcomeTime metav1.Time `json:"lastUnknownOutcomeTime,omitempty,omitzero"`
}

// MasterUpdateRequest is the latest master minor update CAPIBM sent.
// +kubebuilder:validation:MinProperties=1
type MasterUpdateRequest struct {
	// version is the IBM version string sent, for example "1.37" or "4.22_openshift".
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	Version string `json:"version,omitempty"`

	// lastRequestTime is when the latest request for this version was sent.
	// +required
	LastRequestTime metav1.Time `json:"lastRequestTime,omitempty,omitzero"`

	// attempts counts the requests sent for this version; the retry backoff doubles with each.
	// +required
	// +kubebuilder:validation:Minimum=1
	Attempts int32 `json:"attempts,omitempty"`
}

// MasterStatus holds stable IBM master tokens (the master version is status.distributionVersion).
// +kubebuilder:validation:MinProperties=1
type MasterStatus struct {
	// state is lifecycle.masterState (deploying, deployed, updating, ...).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	State string `json:"state,omitempty"`

	// health is lifecycle.masterHealth (IBM documents error, normal, unavailable, unsupported; IBM Cloud was observed to
	// report "" for the first minutes after the master reports Ready, and such a value is omitted here).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	Health string `json:"health,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:path=ibmcloudmanagedcontrolplanes,scope=Namespaced,categories=cluster-api,shortName=ibmcmcp
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".metadata.labels.cluster\\.x-k8s\\.io/cluster-name",description="Cluster to which this IBMCloudManagedControlPlane belongs"
// +kubebuilder:printcolumn:name="Distribution",type="string",JSONPath=".spec.distribution",description="Kubernetes (IKS) or OpenShift (ROKS)"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".status.distributionVersion",description="IBM master version"
// +kubebuilder:printcolumn:name="Kubernetes",type="string",JSONPath=".status.version",description="Kubernetes version the API server reports"
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state",description="IBM cluster state"
// +kubebuilder:printcolumn:name="Available",type="string",JSONPath=".status.conditions[?(@.type=='Available')].status",description="Control plane available"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time duration since creation of IBMCloudManagedControlPlane"
// +kubebuilder:printcolumn:name="ID",type="string",priority=1,JSONPath=".status.clusterID",description="IBM cluster ID"
// +kubebuilder:printcolumn:name="Endpoint",type="string",priority=1,JSONPath=".spec.controlPlaneEndpoint.host",description="Control plane endpoint host"
// +kubebuilder:printcolumn:name="Upgrades",type="string",priority=1,JSONPath=".status.availableDistributionVersions",description="Newer minors IBM offers for the master (informational)"

// IBMCloudManagedControlPlane is an IBM Cloud Kubernetes Service or Red Hat OpenShift on IBM Cloud cluster (VPC Gen2).
type IBMCloudManagedControlPlane struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec is the desired IBM cluster.
	// +required
	Spec IBMCloudManagedControlPlaneSpec `json:"spec,omitempty,omitzero"`

	// status is the observed state.
	// +optional
	Status IBMCloudManagedControlPlaneStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// IBMCloudManagedControlPlaneList contains a list of IBMCloudManagedControlPlane.
type IBMCloudManagedControlPlaneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []IBMCloudManagedControlPlane `json:"items"`
}

// GetConditions returns the v1beta2 conditions of the IBMCloudManagedControlPlane. It implements the CAPI
// conditions.Getter interface, so util/conditions and the patch helper can read the conditions.
func (r *IBMCloudManagedControlPlane) GetConditions() []metav1.Condition {
	return r.Status.Conditions
}

// SetConditions replaces the v1beta2 conditions of the IBMCloudManagedControlPlane with conditions. It implements the CAPI
// conditions.Setter interface; callers normally use util/conditions.Set instead of calling it directly.
func (r *IBMCloudManagedControlPlane) SetConditions(conditions []metav1.Condition) {
	r.Status.Conditions = conditions
}
