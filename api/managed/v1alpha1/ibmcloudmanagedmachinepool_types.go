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

// This file is the InfraMachinePool of a managed cluster: one IBM worker pool. The distribution comes from the control
// plane, the zone subnets from the IBMCloudManagedCluster.
//
// NEVER add the v1beta1 failure reason or failure message status fields: CAPI v1.14 crashes on them
// (machinepool_controller_phases.go:153-173).

func init() {
	objectTypes = append(objectTypes, &IBMCloudManagedMachinePool{}, &IBMCloudManagedMachinePoolList{})
}

const (
	// IBMCloudManagedMachinePoolFinalizer lets the controller remove the IBM pool on a standalone deletion.
	IBMCloudManagedMachinePoolFinalizer = "ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io"

	// NodeAnnotationPrefix prefixes the drain markers CAPIBM writes on workload-cluster Nodes: drain-start-time,
	// drain-owner, cordoned, excluded-from-lb. The marker names live with the drain library, which takes this prefix from
	// its caller so that it imports no CAPIBM API.
	NodeAnnotationPrefix = "ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/"
)

// Entitlement marks an OpenShift pool as covered by an existing OpenShift licence (Cloud Pak).
// +kubebuilder:validation:Enum=OCPEntitled
type Entitlement string

const (
	// EntitlementOCPEntitled means an existing Cloud Pak licence covers the OpenShift fee of the pool's workers.
	EntitlementOCPEntitled Entitlement = "OCPEntitled"
)

// PoolCreation selects whether IBM creates the pool together with the IBM cluster (the cluster's initial worker pool)
// or after it. It is an enum, not a bool, because the API linter forbids bools (nobools) and a later way of creating
// a pool can then be added as a value. The schema has no default marker (KAL), so "omitted means AfterCluster" is
// implemented in code.
// +kubebuilder:validation:Enum=WithCluster;AfterCluster
type PoolCreation string

const (
	// PoolCreationWithCluster marks the cluster's initial worker pool: CAPIBM sends it in IBM's createCluster request,
	// so IBM creates it together with the cluster. Exactly one pool of a Cluster has it when the IBM cluster is created.
	PoolCreationWithCluster PoolCreation = "WithCluster"
	// PoolCreationAfterCluster creates the pool with its own IBM request once the control plane is initialized;
	// it is also the meaning of an omitted spec.creation.
	PoolCreationAfterCluster PoolCreation = "AfterCluster"
)

// WorkerUpdatePolicy selects when CAPIBM replaces workers for which IBM offers a newer fix pack of the same minor.
// +kubebuilder:validation:Enum=Manual;Automatic
type WorkerUpdatePolicy string

const (
	// WorkerUpdatePolicyManual replaces workers only after spec.rollout.after has passed.
	WorkerUpdatePolicyManual WorkerUpdatePolicy = "Manual"
	// WorkerUpdatePolicyAutomatic replaces a worker as soon as IBM reports a newer fix pack for it.
	WorkerUpdatePolicyAutomatic WorkerUpdatePolicy = "Automatic"
)

// IBMCloudManagedMachinePoolSpec is one IKS or ROKS worker pool; the distribution comes from the control plane.
// MachinePool.spec.replicas is the total and must be divisible by len(zones) (IBM sizes pools per zone).
// MachinePool.spec.template.spec.deletion.{nodeDrainTimeoutSeconds,nodeVolumeDetachTimeoutSeconds} and
// MachinePool.spec.template.spec.minReadySeconds drive the drain;
// MachinePool.spec.template.spec.version is ignored (workers follow the control plane minor).
//
// The entitlement rule is spec-level because a field-level transition rule does not run when the field is added or
// removed after creation.
// +kubebuilder:validation:XValidation:rule="has(self.entitlement) == has(oldSelf.entitlement)",message="entitlement cannot be added or removed after creation"
type IBMCloudManagedMachinePoolSpec struct {
	// poolName is the IBM worker pool name, unique within the Cluster. CAPIBM finds, sizes and removes the IBM pool by
	// this name only, whatever spec.creation says. Any valid name works, also for the pool created together with the
	// IBM cluster. Immutable. IBM honours custom names (observed on IBM Cloud); MaxLength stays conservative.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolName is immutable"
	PoolName string `json:"poolName,omitempty"`

	// creation selects the cluster's initial worker pool. WithCluster: IBM creates this pool together with the IBM
	// cluster (the worker pool of createCluster, named spec.poolName); exactly one IBMCloudManagedMachinePool of the
	// Cluster must say so before the IBM cluster is created, otherwise the control plane waits
	// (WaitingForInitialWorkerPool, none) or stops (InvalidConfiguration, several). AfterCluster, also when omitted:
	// the pool is created once the control plane is initialized. The control plane reads it only until the IBM cluster
	// exists. Until the control plane is initialized, a WithCluster pool waits for IBM to list it, and finding it is no
	// adoption (no WorkerPoolAdopted event); afterwards a WithCluster pool is handled like an AfterCluster pool. It never
	// changes which IBM pool this object manages: that is always the pool named spec.poolName. Mutable.
	// +optional
	Creation PoolCreation `json:"creation,omitempty"`

	// flavor is the VPC worker flavor, for example bx2.4x16 (ROKS needs at least 4 vCPU and 16 GB). Immutable:
	// to change it, add a new pool and delete this one.
	// +required
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[a-z0-9]+(\.[a-z0-9]+)+$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="flavor is immutable; create a new pool"
	Flavor string `json:"flavor,omitempty"`

	// zones lists the pool's zones; each must be a zone of the IBMCloudManagedCluster. Immutable in v1alpha1.
	// For ROKS, give the WithCluster pool (spec.creation) every zone the cluster will ever use: IBM does not extend the
	// router load balancer to zones added later.
	// +required
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:XValidation:rule="self.size() == oldSelf.size() && self.all(z, oldSelf.exists(o, o.name == z.name))",message="zones are immutable in v1alpha1"
	Zones []WorkerPoolZone `json:"zones,omitempty"`

	// entitlement is OCPEntitled for OpenShift pools covered by a Cloud Pak licence; omitted means none.
	// OpenShift only (controller guard). Immutable.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="entitlement is immutable"
	Entitlement Entitlement `json:"entitlement,omitempty"`

	// workerUpdates configures how the pool takes newer IBM fix packs (patch updates) for its workers. Minor updates
	// are not configurable: every worker follows the control plane minor automatically.
	// +optional
	WorkerUpdates WorkerUpdatesSpec `json:"workerUpdates,omitempty,omitzero"`

	// rollout triggers worker replacements (CAPI MachineDeployment/KubeadmControlPlane idiom).
	// +optional
	Rollout PoolRolloutSpec `json:"rollout,omitempty,omitzero"`

	// providerIDList is the CAPI contract field, maintained by the controller: ibm://<account>///<clusterID>/<workerID>
	// for every IBM worker of the pool that is not deleted. A worker leaves the list only when IBM lists it as deleted,
	// or when two listings at least poolTransitionPoll apart omit it (status.missingWorkers). Never set it yourself.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=500
	// +kubebuilder:validation:items:MaxLength=256
	ProviderIDList []string `json:"providerIDList,omitempty"`
}

// WorkerPoolZone is one zone of a pool. A per-pool subnet override can be added later (additive optional field).
type WorkerPoolZone struct {
	// name is the VPC zone, for example us-south-1.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Pattern=`^[a-z]{2}-[a-z]+-[0-9]+$`
	Name string `json:"name,omitempty"`
}

// WorkerUpdatesSpec configures fix-pack (patch) updates of the pool's workers.
// +kubebuilder:validation:MinProperties=1
type WorkerUpdatesSpec struct {
	// policy is Manual (also when omitted) or Automatic. Manual: workers with a newer fix pack are only reported
	// (WorkersUpToDate=False, reason PatchUpdateAvailable) until spec.rollout.after is set to a time that has passed.
	// Automatic: CAPIBM replaces them, one at a time and drained, as soon as IBM reports a newer fix pack.
	// +optional
	Policy WorkerUpdatePolicy `json:"policy,omitempty"`
}

// PoolRolloutSpec controls worker rollouts. A surge setting (maxSurgePerZone) can be added later; maxUnavailable is
// fixed at one worker per Cluster and the name is reserved.
// +kubebuilder:validation:MinProperties=1
type PoolRolloutSpec struct {
	// after is a trigger time: once it has passed, every worker whose Node was created before it is replaced, one at
	// a time and drained, with update=true, so it comes back on the newest IBM fix pack of the control plane minor.
	// This is the MachineDeployment semantics (rollout even without changes), so it also recycles workers that are up
	// to date, for example after a CA rotation. Workers created after it are never replaced because of it. RFC 3339,
	// for example "2026-11-20T09:00:00Z". CAPI never reads this field (MachinePool has no rollout field, and the only
	// rollout.after accessor is the control plane's, used by topology); `clusterctl alpha rollout restart` supports
	// only MachineDeployment and KubeadmControlPlane, so users set it with kubectl patch (the book gives the command).
	// +optional
	After metav1.Time `json:"after,omitempty,omitzero"`
}

// IBMCloudManagedMachinePoolStatus lists the fields CAPI v1.14 reads first.
// +kubebuilder:validation:MinProperties=1
type IBMCloudManagedMachinePoolStatus struct {
	// conditions are Ready (the summary of WorkerPoolReady), WorkerPoolReady, WorkersUpToDate, Draining, Deleting and
	// Paused.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=32
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// initialization holds provisioned, the v1beta2 contract flag (read by CAPI >= v1.15); sticky.
	// +optional
	Initialization IBMCloudManagedMachinePoolInitializationStatus `json:"initialization,omitempty,omitzero"`

	// No Go "Deprecated:" paragraph on ready: the pool controller must write it for as long as CAPIBM supports CAPI
	// v1.14 (core/reconcilers/machinepool/machinepool_controller_phases.go reads it through external.IsReady), and
	// staticcheck SA1019 would flag every such write. The field is removed once CAPIBM requires a CAPI release that reads
	// initialization.provisioned.

	// ready duplicates initialization.provisioned because CAPI v1.14 reads only status.ready for InfraMachinePools and
	// re-evaluates it every reconcile. True once the IBM pool exists; never set back to false. It is a *bool because
	// the CAPI InfraMachinePool contract defines it so (a KAL exclusion in .golangci-kal.yml). It is kept only for CAPI
	// v1.14; remove it when CAPIBM requires a CAPI release that reads initialization.provisioned.
	// +optional
	Ready *bool `json:"ready,omitempty"`

	// replicas is the most recently observed number of IBM workers in the pool that are not deleted, which equals
	// len(spec.providerIDList) (InfraMachinePool contract, infra-machinepool.md:401; CAPG does the same). CAPI v1.14
	// also copies it into MachinePool readyReplicas and availableReplicas for pools without MachinePool Machines, so
	// those count provisioning workers too; worker readiness is reported by the WorkerPoolReady condition. Always
	// published, including 0.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// missingWorkers lists workers that are still in spec.providerIDList although the latest listing omitted them
	// without a deleted record. An ID is dropped only when a listing at least poolTransitionPoll after firstMissingTime
	// still omits it, because dropping a live worker's ID makes Cluster API delete its Node. Lost on move, which only
	// delays a drop.
	// +optional
	// +listType=map
	// +listMapKey=workerID
	// +kubebuilder:validation:MaxItems=500
	MissingWorkers []MissingWorker `json:"missingWorkers,omitempty"`

	// poolID is the IBM worker pool ID, "<clusterID>-<7 hex>" as observed on IBM Cloud (cache; rediscovered by poolName).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	PoolID string `json:"poolID,omitempty"`

	// sizePerZone is IBM's pool workerCount, which is per zone (observed on IBM Cloud).
	// +optional
	// +kubebuilder:validation:Minimum=0
	SizePerZone *int32 `json:"sizePerZone,omitempty"`

	// zones are the observed zones of the IBM pool.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=3
	Zones []ObservedPoolZone `json:"zones,omitempty"`

	// workerVersions counts the pool's workers (deleted ones excluded) per IBM version (kubeVersion.actual), for
	// example {version: "1.36.5_1540", count: 3}. Sorted by version.
	// +optional
	// +listType=map
	// +listMapKey=version
	// +kubebuilder:validation:MaxItems=16
	WorkerVersions []WorkerVersionCount `json:"workerVersions,omitempty"`

	// targetWorkerVersion is the newest IBM fix pack offered to the pool's workers (the highest kubeVersion.target).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	TargetWorkerVersion string `json:"targetWorkerVersion,omitempty"`

	// workersWithUpdateAvailable counts the workers that are behind the control plane minor or whose
	// kubeVersion.actual differs from kubeVersion.target (a newer fix pack). Always published, including 0,
	// whatever the policy.
	// +optional
	// +kubebuilder:validation:Minimum=0
	WorkersWithUpdateAvailable *int32 `json:"workersWithUpdateAvailable,omitempty"`

	// disruption describes the worker removal in flight for this pool; absent when idle. It is an observation cache:
	// the controller rebuilds it from the Node annotations and IBM worker states after a restart or a clusterctl move.
	// +optional
	Disruption WorkerDisruptionStatus `json:"disruption,omitempty,omitzero"`

	// lastDisruptionTime is when this controller last asked IBM to remove capacity from this pool (resize down,
	// replace, remove). Every pool of the Cluster waits DisruptionSettleDelay after the newest one. When status is empty
	// (a new object, or after clusterctl move or a restore), the first observation sets it to now, so no disruptive call
	// can start inside IBM's reflection lag of a disruption the old status recorded.
	// +optional
	LastDisruptionTime metav1.Time `json:"lastDisruptionTime,omitempty,omitzero"`
}

// WorkerVersionCount is the number of workers on one IBM version.
type WorkerVersionCount struct {
	// version is IBM's worker version, for example "1.36.5_1540" or "4.21.40_1540_openshift".
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	Version string `json:"version,omitempty"`

	// count is the number of workers on that version.
	// +required
	// +kubebuilder:validation:Minimum=1
	Count int32 `json:"count,omitempty"`
}

// WorkerDisruptionOperation is what removes the workers.
// +kubebuilder:validation:Enum=Replace;ScaleDown;DeletePool
type WorkerDisruptionOperation string

const (
	// WorkerDisruptionOperationReplace replaces one worker (rollout).
	WorkerDisruptionOperationReplace WorkerDisruptionOperation = "Replace"
	// WorkerDisruptionOperationScaleDown resizes the pool down.
	WorkerDisruptionOperationScaleDown WorkerDisruptionOperation = "ScaleDown"
	// WorkerDisruptionOperationDeletePool removes the whole pool.
	WorkerDisruptionOperationDeletePool WorkerDisruptionOperation = "DeletePool"
)

// WorkerDisruptionPhase is the current step of a disruption.
// +kubebuilder:validation:Enum=Draining;WaitingForVolumeDetach;WaitingForIBM;WaitingForRemoval;WaitingForReplacement
type WorkerDisruptionPhase string

const (
	// WorkerDisruptionPhaseDraining cordons the Nodes and evicts their pods.
	WorkerDisruptionPhaseDraining WorkerDisruptionPhase = "Draining"
	// WorkerDisruptionPhaseWaitingForVolumeDetach waits until the drained Nodes have no attached volumes.
	WorkerDisruptionPhaseWaitingForVolumeDetach WorkerDisruptionPhase = "WaitingForVolumeDetach"
	// WorkerDisruptionPhaseWaitingForIBM is the IBM call (replaceWorker, resizeWorkerPool or removeWorkerPool).
	WorkerDisruptionPhaseWaitingForIBM WorkerDisruptionPhase = "WaitingForIBM"
	// WorkerDisruptionPhaseWaitingForRemoval waits until IBM lists the workers as deleted.
	WorkerDisruptionPhaseWaitingForRemoval WorkerDisruptionPhase = "WaitingForRemoval"
	// WorkerDisruptionPhaseWaitingForReplacement waits until the replacement Node is Ready for minReadySeconds.
	WorkerDisruptionPhaseWaitingForReplacement WorkerDisruptionPhase = "WaitingForReplacement"
)

// WorkerDisruptionStatus is one worker-removing operation and its progress.
// +kubebuilder:validation:MinProperties=1
type WorkerDisruptionStatus struct {
	// operation is Replace (rollout), ScaleDown or DeletePool.
	// +required
	Operation WorkerDisruptionOperation `json:"operation,omitempty"`

	// phase is the current step. It is a bounded enum of the steps of one in-flight operation, not an object lifecycle
	// phase (the Draining, WorkersUpToDate and Deleting conditions carry the state), so .golangci-kal.yml excludes it
	// from the nophase rule.
	// +required
	Phase WorkerDisruptionPhase `json:"phase,omitempty"`

	// workerIDs are the IBM workers being removed (one for Replace; the predicted victims for ScaleDown; all for
	// DeletePool).
	// +required
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=500
	// +kubebuilder:validation:items:MaxLength=128
	WorkerIDs []string `json:"workerIDs,omitempty"`

	// drainStartTime is when the first Node of this operation was cordoned (also kept on the Node, in its
	// drain-start-time marker).
	// +optional
	DrainStartTime metav1.Time `json:"drainStartTime,omitempty,omitzero"`

	// volumeDetachStartTime is when CAPIBM began to wait for the volumes of the drained Nodes to detach: the first time
	// it recorded phase WaitingForVolumeDetach for this operation. MachinePool
	// spec.template.spec.deletion.nodeVolumeDetachTimeoutSeconds counts from it, as Cluster API counts it from a
	// Machine's status.deletion.waitForNodeVolumeDetachStartTime, so a long drain never uses up the detach wait. A pod
	// that comes back sends the operation to Draining again and keeps this time, as Cluster API keeps its own.
	// +optional
	VolumeDetachStartTime metav1.Time `json:"volumeDetachStartTime,omitempty,omitzero"`

	// ibmRequestTime is when CAPIBM sent the IBM call (replaceWorker, resizeWorkerPool or removeWorkerPool).
	// +optional
	IBMRequestTime metav1.Time `json:"ibmRequestTime,omitempty,omitzero"`
}

// MissingWorker is a worker the latest listing omitted without a deleted record.
type MissingWorker struct {
	// workerID is the IBM worker ID.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	WorkerID string `json:"workerID,omitempty"`

	// firstMissingTime is the time of the first listing that omitted it.
	// +required
	FirstMissingTime metav1.Time `json:"firstMissingTime,omitempty,omitzero"`
}

// ObservedPoolZone is one observed zone.
type ObservedPoolZone struct {
	// name is the zone.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=40
	Name string `json:"name,omitempty"`

	// workerCount is IBM's worker count for the zone (it lags pool.workerCount while workers provision, as observed on
	// IBM Cloud).
	// +optional
	// +kubebuilder:validation:Minimum=0
	WorkerCount *int32 `json:"workerCount,omitempty"`
}

// IBMCloudManagedMachinePoolInitializationStatus is the CAPI v1beta2 contract block.
// +kubebuilder:validation:MinProperties=1
type IBMCloudManagedMachinePoolInitializationStatus struct {
	// provisioned is true once the IBM pool exists; sticky. It is a *bool because the CAPI InfraMachinePool contract
	// defines it so (a KAL exclusion in .golangci-kal.yml).
	// +optional
	Provisioned *bool `json:"provisioned,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:path=ibmcloudmanagedmachinepools,scope=Namespaced,categories=cluster-api,shortName=ibmcmmp
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".metadata.labels.cluster\\.x-k8s\\.io/cluster-name",description="Cluster to which this IBMCloudManagedMachinePool belongs"
// +kubebuilder:printcolumn:name="Pool",type="string",JSONPath=".spec.poolName",description="IBM worker pool name"
// +kubebuilder:printcolumn:name="Flavor",type="string",JSONPath=".spec.flavor",description="VPC worker flavor"
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".status.replicas",description="IBM workers in the pool that are not deleted"
// +kubebuilder:printcolumn:name="Ready",type="boolean",JSONPath=".status.ready",description="The IBM pool exists"
// +kubebuilder:printcolumn:name="Updates",type="integer",JSONPath=".status.workersWithUpdateAvailable",description="Workers with a newer minor or fix pack available"
// +kubebuilder:printcolumn:name="Policy",type="string",priority=1,JSONPath=".spec.workerUpdates.policy",description="Fix-pack update policy (omitted means Manual)"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time duration since creation of IBMCloudManagedMachinePool"

// IBMCloudManagedMachinePool is the InfraMachinePool of an IBM Cloud managed (IKS or ROKS) cluster: one IBM worker pool.
type IBMCloudManagedMachinePool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec is the desired worker pool.
	// +required
	Spec IBMCloudManagedMachinePoolSpec `json:"spec,omitempty,omitzero"`

	// status is the observed state.
	// +optional
	Status IBMCloudManagedMachinePoolStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// IBMCloudManagedMachinePoolList contains a list of IBMCloudManagedMachinePool.
type IBMCloudManagedMachinePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []IBMCloudManagedMachinePool `json:"items"`
}

// GetConditions returns the v1beta2 conditions of the IBMCloudManagedMachinePool. It implements the CAPI
// conditions.Getter interface, so util/conditions and the patch helper can read the conditions.
func (r *IBMCloudManagedMachinePool) GetConditions() []metav1.Condition {
	return r.Status.Conditions
}

// SetConditions replaces the v1beta2 conditions of the IBMCloudManagedMachinePool with conditions. It implements the CAPI
// conditions.Setter interface; callers normally use util/conditions.Set instead of calling it directly.
func (r *IBMCloudManagedMachinePool) SetConditions(conditions []metav1.Condition) {
	r.Status.Conditions = conditions
}
