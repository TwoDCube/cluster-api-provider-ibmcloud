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
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

const (
	zonesImmutableMessage      = "zones are immutable in v1alpha1"
	entitlementPresenceMessage = "entitlement cannot be added or removed after creation"
)

// newIBMCloudManagedMachinePool returns a valid IBMCloudManagedMachinePool in namespace: poolName "default", flavor
// bx2.4x16, one zone, spec.creation omitted.
func newIBMCloudManagedMachinePool(namespace string) *infrav1.IBMCloudManagedMachinePool {
	return &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "ibmcmmp-", Namespace: namespace},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName: "default",
			Flavor:   "bx2.4x16",
			Zones:    []infrav1.WorkerPoolZone{{Name: "us-south-1"}},
		},
	}
}

// poolZones returns WorkerPoolZones for the given zone names.
func poolZones(names ...string) []infrav1.WorkerPoolZone {
	zones := make([]infrav1.WorkerPoolZone, 0, len(names))
	for _, name := range names {
		zones = append(zones, infrav1.WorkerPoolZone{Name: name})
	}
	return zones
}

// TestIBMCloudManagedMachinePoolCreateValidation covers the create-time CEL and OpenAPI rules of
// IBMCloudManagedMachinePool.
func TestIBMCloudManagedMachinePoolCreateValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmmp-create")

	testCases := []struct {
		name    string
		mutate  func(p *infrav1.IBMCloudManagedMachinePool)
		wantErr string
	}{
		{
			name:   "minimal_valid_object_accepted",
			mutate: func(_ *infrav1.IBMCloudManagedMachinePool) {},
		},
		{
			name: "all_optional_fields_accepted",
			mutate: func(p *infrav1.IBMCloudManagedMachinePool) {
				p.Spec.PoolName = "gpu-pool-1"
				p.Spec.Creation = infrav1.PoolCreationWithCluster
				p.Spec.Flavor = "gx3.16x80.l4"
				p.Spec.Zones = poolZones("us-south-1", "us-south-2", "us-south-3")
				p.Spec.Entitlement = infrav1.EntitlementOCPEntitled
				p.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyAutomatic
				p.Spec.Rollout.After = metav1.NewTime(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
				p.Spec.ProviderIDList = []string{"ibm://<account>///<clusterID>/<workerID>"}
			},
		},
		{
			name:    "pool_name_uppercase_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.PoolName = "Default" },
			wantErr: "spec.poolName",
		},
		{
			name:    "pool_name_leading_dash_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.PoolName = "-pool" },
			wantErr: "spec.poolName",
		},
		{
			name:    "pool_name_longer_than_32_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.PoolName = strings.Repeat("p", 33) },
			wantErr: "spec.poolName: Too long",
		},
		{
			name:    "flavor_without_dot_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Flavor = "bx2" },
			wantErr: "spec.flavor",
		},
		{
			name:    "flavor_uppercase_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Flavor = "BX2.4x16" },
			wantErr: "spec.flavor",
		},
		{
			name:    "no_zones_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = nil },
			wantErr: "spec.zones: Required value",
		},
		{
			name: "four_zones_rejected",
			mutate: func(p *infrav1.IBMCloudManagedMachinePool) {
				p.Spec.Zones = poolZones("us-south-1", "us-south-2", "us-south-3", "us-south-4")
			},
			wantErr: "spec.zones: Too many",
		},
		{
			name:    "duplicate_zone_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-1", "us-south-1") },
			wantErr: "Duplicate value",
		},
		{
			name:    "zone_name_not_a_zone_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south") },
			wantErr: "spec.zones[0].name",
		},
		{
			name:    "entitlement_not_in_enum_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Entitlement = "Yes" },
			wantErr: "Unsupported value: \"Yes\"",
		},
		{
			name:    "worker_update_policy_not_in_enum_rejected",
			mutate:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.WorkerUpdates.Policy = "Sometimes" },
			wantErr: "Unsupported value: \"Sometimes\"",
		},
		{
			name: "provider_id_too_long_rejected",
			mutate: func(p *infrav1.IBMCloudManagedMachinePool) {
				p.Spec.ProviderIDList = []string{strings.Repeat("i", 257)}
			},
			wantErr: "spec.providerIDList[0]: Too long",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedMachinePool(namespace)
			tc.mutate(obj)
			expectAPIResult(g, testEnv.Create(ctx, obj), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedMachinePoolUpdateValidation covers the transition rules: poolName, flavor, zones and entitlement
// are immutable, entitlement cannot be added or removed, and the user-owned and contract fields stay writable.
func TestIBMCloudManagedMachinePoolUpdateValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmmp-update")

	testCases := []struct {
		name    string
		initial func(p *infrav1.IBMCloudManagedMachinePool)
		update  func(p *infrav1.IBMCloudManagedMachinePool)
		wantErr string
	}{
		{
			name:    "pool_name_immutable",
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.PoolName = "other" },
			wantErr: "poolName is immutable",
		},
		{
			name:    "flavor_immutable",
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Flavor = "bx2.8x32" },
			wantErr: "flavor is immutable; create a new pool",
		},
		{
			name:    "zone_added_rejected",
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-1", "us-south-2") },
			wantErr: zonesImmutableMessage,
		},
		{
			name:    "zone_removed_rejected",
			initial: func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-1", "us-south-2") },
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-1") },
			wantErr: zonesImmutableMessage,
		},
		{
			name:    "zone_replaced_rejected",
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-2") },
			wantErr: zonesImmutableMessage,
		},
		{
			name:    "zones_reordered_allowed",
			initial: func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-1", "us-south-2") },
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Zones = poolZones("us-south-2", "us-south-1") },
		},
		{
			name:    "entitlement_added_rejected",
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Entitlement = infrav1.EntitlementOCPEntitled },
			wantErr: entitlementPresenceMessage,
		},
		{
			name:    "entitlement_removed_rejected",
			initial: func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Entitlement = infrav1.EntitlementOCPEntitled },
			update:  func(p *infrav1.IBMCloudManagedMachinePool) { p.Spec.Entitlement = "" },
			wantErr: entitlementPresenceMessage,
		},
		{
			name: "user_owned_fields_writable",
			update: func(p *infrav1.IBMCloudManagedMachinePool) {
				p.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyManual
				p.Spec.Rollout.After = metav1.NewTime(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
			},
		},
		{
			name: "provider_id_list_writable",
			update: func(p *infrav1.IBMCloudManagedMachinePool) {
				p.Spec.ProviderIDList = []string{"ibm://<account>///<clusterID>/<workerID-1>"}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedMachinePool(namespace)
			if tc.initial != nil {
				tc.initial(obj)
			}
			g.Expect(testEnv.Create(ctx, obj)).To(Succeed())
			tc.update(obj)
			expectAPIResult(g, testEnv.Update(ctx, obj), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedMachinePoolCreationMarker covers spec.creation: both values and an omitted field are accepted, any
// other value is rejected, an omitted field stays omitted (the schema has no default, so "omitted means AfterCluster"
// stays the controller's reading), and the field may change in every direction, because the control plane reads it
// only before the IBM cluster is created.
func TestIBMCloudManagedMachinePoolCreationMarker(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmmp-creation")

	createCases := []struct {
		name     string
		creation infrav1.PoolCreation
		wantErr  string
	}{
		{name: "creation_with_cluster_accepted", creation: infrav1.PoolCreationWithCluster},
		{name: "creation_after_cluster_accepted", creation: infrav1.PoolCreationAfterCluster},
		{name: "creation_omitted_accepted", creation: ""},
		{name: "creation_not_in_enum_rejected", creation: "Later", wantErr: `spec.creation: Unsupported value: "Later"`},
		{name: "creation_is_case_sensitive", creation: "withCluster", wantErr: `spec.creation: Unsupported value: "withCluster"`},
	}
	for _, tc := range createCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedMachinePool(namespace)
			obj.Spec.Creation = tc.creation
			expectAPIResult(g, testEnv.Create(ctx, obj), tc.wantErr)
			if tc.wantErr == "" {
				stored := &infrav1.IBMCloudManagedMachinePool{}
				g.Expect(testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(obj), stored)).To(Succeed())
				g.Expect(stored.Spec.Creation).To(Equal(tc.creation), "the API server stores the value as sent, without a default")
			}
		})
	}

	updateCases := []struct {
		name     string
		from, to infrav1.PoolCreation
		wantErr  string
	}{
		{name: "creation_with_cluster_to_after_cluster_allowed", from: infrav1.PoolCreationWithCluster, to: infrav1.PoolCreationAfterCluster},
		{name: "creation_after_cluster_to_with_cluster_allowed", from: infrav1.PoolCreationAfterCluster, to: infrav1.PoolCreationWithCluster},
		{name: "creation_added_allowed", from: "", to: infrav1.PoolCreationWithCluster},
		{name: "creation_removed_allowed", from: infrav1.PoolCreationWithCluster, to: ""},
		{name: "creation_changed_to_unknown_value_rejected", from: infrav1.PoolCreationWithCluster, to: "Never",
			wantErr: `spec.creation: Unsupported value: "Never"`},
	}
	for _, tc := range updateCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedMachinePool(namespace)
			obj.Spec.Creation = tc.from
			g.Expect(testEnv.Create(ctx, obj)).To(Succeed())
			obj.Spec.Creation = tc.to
			expectAPIResult(g, testEnv.Update(ctx, obj), tc.wantErr)
		})
	}
}

// TestIBMCloudManagedMachinePoolStatusValidation proves that the status schema accepts every field the CAPI
// InfraMachinePool contract and the controller write, including replicas 0 as a pointer, and enforces the bounds of
// the disruption block.
func TestIBMCloudManagedMachinePoolStatusValidation(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmmp-status")
	now := metav1.Now()

	testCases := []struct {
		name    string
		status  infrav1.IBMCloudManagedMachinePoolStatus
		wantErr string
	}{
		{
			name: "contract_and_cache_fields_accepted",
			status: infrav1.IBMCloudManagedMachinePoolStatus{
				Conditions: []metav1.Condition{{
					Type:               infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition,
					Status:             metav1.ConditionTrue,
					Reason:             infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReason,
					LastTransitionTime: now,
				}},
				Initialization: infrav1.IBMCloudManagedMachinePoolInitializationStatus{Provisioned: ptr.To(true)},
				Ready:          ptr.To(true),
				Replicas:       ptr.To[int32](0),
				MissingWorkers: []infrav1.MissingWorker{{WorkerID: "kube-worker-1", FirstMissingTime: now}},
				PoolID:         "d0000000000000000000-0000000",
				SizePerZone:    ptr.To[int32](2),
				Zones:          []infrav1.ObservedPoolZone{{Name: "us-south-1", WorkerCount: ptr.To[int32](0)}},
				WorkerVersions: []infrav1.WorkerVersionCount{
					{Version: "1.36.5_1537", Count: 1},
					{Version: "1.36.5_1540", Count: 2},
				},
				TargetWorkerVersion:        "1.36.5_1540",
				WorkersWithUpdateAvailable: ptr.To[int32](0),
				Disruption: infrav1.WorkerDisruptionStatus{
					Operation:      infrav1.WorkerDisruptionOperationReplace,
					Phase:          infrav1.WorkerDisruptionPhaseDraining,
					WorkerIDs:      []string{"kube-worker-1"},
					DrainStartTime: now,
				},
				LastDisruptionTime: now,
			},
		},
		{
			name: "disruption_without_worker_ids_rejected",
			status: infrav1.IBMCloudManagedMachinePoolStatus{
				Disruption: infrav1.WorkerDisruptionStatus{
					Operation: infrav1.WorkerDisruptionOperationScaleDown,
					Phase:     infrav1.WorkerDisruptionPhaseWaitingForIBM,
				},
			},
			wantErr: "status.disruption.workerIDs: Required value",
		},
		{
			name: "disruption_phase_not_in_enum_rejected",
			status: infrav1.IBMCloudManagedMachinePoolStatus{
				Disruption: infrav1.WorkerDisruptionStatus{
					Operation: infrav1.WorkerDisruptionOperationDeletePool,
					Phase:     "Evicting",
					WorkerIDs: []string{"kube-worker-1"},
				},
			},
			wantErr: "Unsupported value: \"Evicting\"",
		},
		{
			name: "worker_version_without_count_rejected",
			status: infrav1.IBMCloudManagedMachinePoolStatus{
				WorkerVersions: []infrav1.WorkerVersionCount{{Version: "1.36.5_1540"}},
			},
			wantErr: "status.workerVersions[0].count: Required value",
		},
		{
			name: "negative_replicas_rejected",
			status: infrav1.IBMCloudManagedMachinePoolStatus{
				Replicas: ptr.To[int32](-1),
			},
			wantErr: "status.replicas",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			obj := newIBMCloudManagedMachinePool(namespace)
			g.Expect(testEnv.Create(ctx, obj)).To(Succeed())
			obj.Status = tc.status
			expectAPIResult(g, testEnv.Status().Update(ctx, obj), tc.wantErr)
			if tc.wantErr == "" {
				// Zero counts survive the round trip as set pointers: the contract publishes replicas 0.
				g.Expect(obj.Status.Replicas).To(HaveValue(BeZero()))
				g.Expect(obj.Status.WorkersWithUpdateAvailable).To(HaveValue(BeZero()))
				g.Expect(obj.Status.Ready).To(HaveValue(BeTrue()))
			}
		})
	}
}

// TestFakeMachinePoolCRD proves that test/helpers installs a MachinePool CRD (cluster.x-k8s.io/v1beta2) with a
// status subresource, which the managed pool envtest suites need for the IBMCloudManagedMachinePool owner.
func TestFakeMachinePoolCRD(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "fake-machinepool")

	machinePool := &unstructured.Unstructured{}
	machinePool.SetAPIVersion("cluster.x-k8s.io/v1beta2")
	machinePool.SetKind("MachinePool")
	machinePool.SetNamespace(namespace)
	machinePool.SetName("pool")
	g.Expect(unstructured.SetNestedField(machinePool.Object, "my-cluster", "spec", "clusterName")).To(Succeed())
	g.Expect(testEnv.Create(ctx, machinePool)).To(Succeed())

	g.Expect(unstructured.SetNestedField(machinePool.Object, int64(3), "status", "replicas")).To(Succeed())
	g.Expect(testEnv.Status().Update(ctx, machinePool)).To(Succeed())

	replicas, found, err := unstructured.NestedInt64(machinePool.Object, "status", "replicas")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(found).To(BeTrue())
	g.Expect(replicas).To(Equal(int64(3)))
}
