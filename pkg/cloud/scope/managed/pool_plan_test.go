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
	"testing"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

func TestDesiredSizePerZone(t *testing.T) {
	testCases := []struct {
		name     string
		replicas int32
		zones    int
		want     int64
		wantErr  string
	}{
		{name: "HA default: 6 workers in 3 zones (IBM's HA topology, 2 workers per zone)", replicas: 6, zones: 3, want: 2},
		{name: "single zone", replicas: 2, zones: 1, want: 2},
		{name: "one per zone", replicas: 3, zones: 3, want: 1},
		{name: "zero is PoolGuards' decision", replicas: 0, zones: 3, want: 0},
		{name: "not a multiple: both neighbours", replicas: 4, zones: 3, wantErr: "replicas=4 is not a multiple of 3 zones; use 3 or 6 (IBM sizes worker pools per zone)"},
		{name: "not a multiple: no zero suggestion", replicas: 2, zones: 3, wantErr: "replicas=2 is not a multiple of 3 zones; use 3 (IBM"},
		{name: "two zones", replicas: 5, zones: 2, wantErr: "replicas=5 is not a multiple of 2 zones; use 4 or 6"},
		{name: "no zones", replicas: 3, zones: 0, wantErr: "the pool lists 0 zones; list at least one zone in spec.zones"},
		{name: "negative replicas", replicas: -1, zones: 1, wantErr: "replicas=-1 is negative"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := DesiredSizePerZone(tc.replicas, tc.zones)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(got).To(BeZero())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

// guardPool returns an IBMCloudManagedMachinePool in namespace ns that claims IBM pool poolName in zones.
func guardPool(name, poolName string, zones ...string) infrav1.IBMCloudManagedMachinePool {
	pool := infrav1.IBMCloudManagedMachinePool{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
	pool.Spec.PoolName = poolName
	pool.Spec.Flavor = "bx2.4x16"
	for _, z := range zones {
		pool.Spec.Zones = append(pool.Spec.Zones, infrav1.WorkerPoolZone{Name: z})
	}
	return pool
}

// guardInput returns valid guard input: pool "pool-a" (IBM pool "default") in three zones of an IBMCloudManagedCluster
// that has resolved all three, a MachinePool of 6 replicas and an IKS cluster with one other pool.
func guardInput() PoolGuardInput {
	zones := []string{"eu-de-1", "eu-de-2", "eu-de-3"}
	pool := guardPool("pool-a", "default", zones...)
	infra := &infrav1.IBMCloudManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "infra", Namespace: "ns"}}
	for _, z := range zones {
		infra.Spec.Zones = append(infra.Spec.Zones, infrav1.ZoneSubnet{Name: z})
		infra.Status.Zones = append(infra.Status.Zones, infrav1.ResolvedZone{Name: z})
	}
	mp := &clusterv1.MachinePool{ObjectMeta: metav1.ObjectMeta{Name: "mp-a", Namespace: "ns"}}
	mp.Spec.Replicas = ptr.To[int32](6)
	return PoolGuardInput{
		Pool:         &pool,
		MachinePool:  mp,
		InfraCluster: infra,
		Distribution: controlplanev1.DistributionKubernetes,
		ClusterPools: []infrav1.IBMCloudManagedMachinePool{pool, guardPool("pool-b", "workers", "eu-de-1")},
	}
}

func TestPoolGuards(t *testing.T) {
	testCases := []struct {
		name            string
		modify          func(in *PoolGuardInput)
		wantReason      string
		wantMessage     []string
		wantStops       bool
		wantDesired     int64
		wantVersion     string
		wantFailureDoms string
	}{
		{
			name:        "every guard passes",
			modify:      func(*PoolGuardInput) {},
			wantDesired: 2,
		},
		{
			name: "unset replicas default to 1 (CAPI's default)",
			modify: func(in *PoolGuardInput) {
				in.MachinePool.Spec.Replicas = nil
				in.Pool.Spec.Zones = in.Pool.Spec.Zones[:1]
			},
			wantDesired: 1,
		},
		{
			name: "two_claimants_of_one_poolName_both_get_PoolNameConflict_and_stop_publishing",
			modify: func(in *PoolGuardInput) {
				in.ClusterPools = append(in.ClusterPools, guardPool("pool-c", "default", "eu-de-1"))
			},
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason,
			// The whole message: spec.poolName cannot be changed, so the fix is a deletion of every claimant but the one
			// that already manages the IBM pool.
			wantMessage: []string{`IBMCloudManagedMachinePool pool-c also set spec.poolName "default"; while more than one ` +
				"object of the Cluster claims an IBM worker pool, none of them creates, sizes or publishes it; spec.poolName " +
				"is immutable, so delete the MachinePool of every claimant but one, and keep the one whose status.poolID is " +
				"set, if any"},
			wantStops: true,
		},
		{
			name: "PoolNameConflict_wins_over_every_other_failing_guard",
			modify: func(in *PoolGuardInput) {
				in.ClusterPools = append(in.ClusterPools, guardPool("pool-c", "default", "eu-de-1"))
				in.InfraCluster.Status.Zones = nil
				in.MachinePool.Spec.Replicas = ptr.To[int32](4)
			},
			wantReason:  infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason,
			wantMessage: []string{"pool-c"},
			wantStops:   true,
		},
		{
			name: "a_deleting_claimant_still_conflicts",
			modify: func(in *PoolGuardInput) {
				deleting := guardPool("pool-c", "default", "eu-de-1")
				deleting.DeletionTimestamp = ptr.To(metav1.Now())
				in.ClusterPools = append(in.ClusterPools, deleting)
			},
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason,
			wantStops:  true,
		},
		{
			name: "many claimants are named sorted and bounded",
			modify: func(in *PoolGuardInput) {
				for _, name := range []string{"pool-f", "pool-d", "pool-c", "pool-e", "pool-c"} {
					in.ClusterPools = append(in.ClusterPools, guardPool(name, "default", "eu-de-1"))
				}
			},
			wantReason:  infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason,
			wantMessage: []string{"IBMCloudManagedMachinePool pool-c, pool-d, pool-e and 1 more also set"},
			wantStops:   true,
		},
		{
			name: "a same-named object in another namespace is another claimant",
			modify: func(in *PoolGuardInput) {
				other := guardPool("pool-a", "default", "eu-de-1")
				other.Namespace = "elsewhere"
				in.ClusterPools = append(in.ClusterPools, other)
			},
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason,
			wantStops:  true,
		},
		{
			name:        "the pool itself in ClusterPools is not a conflict",
			modify:      func(in *PoolGuardInput) { in.ClusterPools = append(in.ClusterPools, *in.Pool) },
			wantDesired: 2,
		},
		{
			name: "a zone the IBMCloudManagedCluster does not have is UnknownZone",
			modify: func(in *PoolGuardInput) {
				in.Pool.Spec.Zones = append(in.Pool.Spec.Zones[:2], infrav1.WorkerPoolZone{Name: "us-south-1"})
			},
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason,
			wantMessage: []string{"zone us-south-1 of spec.zones is not a zone of IBMCloudManagedCluster infra " +
				"(its zones: eu-de-1, eu-de-2, eu-de-3)", "add the zone with its subnet"},
		},
		{
			name: "an unknown zone wins over a zone that is still resolving",
			modify: func(in *PoolGuardInput) {
				in.InfraCluster.Status.Zones = nil
				in.Pool.Spec.Zones = append(in.Pool.Spec.Zones, infrav1.WorkerPoolZone{Name: "us-south-1"})
			},
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason,
		},
		{
			name: "an IBMCloudManagedCluster without zones names none",
			modify: func(in *PoolGuardInput) {
				in.InfraCluster.Spec.Zones, in.InfraCluster.Status.Zones = nil, nil
			},
			wantReason:  infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason,
			wantMessage: []string{"(its zones: none)"},
		},
		{
			name:       "a zone not yet in status.zones is WaitingForInfrastructure",
			modify:     func(in *PoolGuardInput) { in.InfraCluster.Status.Zones = in.InfraCluster.Status.Zones[:2] },
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason,
			wantMessage: []string{"zone eu-de-3 is not resolved yet in IBMCloudManagedCluster infra status.zones; waiting for " +
				"its subnet to be validated"},
		},
		{
			name:       "replicas 0 is ZeroReplicasNotSupported while AllowZeroReplicaPools is off",
			modify:     func(in *PoolGuardInput) { in.MachinePool.Spec.Replicas = ptr.To[int32](0) },
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyZeroReplicasNotSupportedReason,
			wantMessage: []string{"MachinePool mp-a has replicas=0", "set replicas to a multiple of 3, or delete the MachinePool " +
				"to remove the pool"},
		},
		{
			name:        "replicas not divisible by the zones (the WorkerPoolReady message)",
			modify:      func(in *PoolGuardInput) { in.MachinePool.Spec.Replicas = ptr.To[int32](4) },
			wantReason:  infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason,
			wantMessage: []string{"replicas=4 is not a multiple of 3 zones; use 3 or 6"},
		},
		{
			name:       "entitlement on a Kubernetes cluster is InvalidForDistribution",
			modify:     func(in *PoolGuardInput) { in.Pool.Spec.Entitlement = infrav1.EntitlementOCPEntitled },
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyInvalidForDistributionReason,
			wantMessage: []string{"spec.entitlement is OCPEntitled, which only OpenShift (ROKS) pools accept", "distribution is " +
				"Kubernetes", "recreate this IBMCloudManagedMachinePool without it"},
		},
		{
			name: "entitlement on an OpenShift cluster passes",
			modify: func(in *PoolGuardInput) {
				in.Pool.Spec.Entitlement = infrav1.EntitlementOCPEntitled
				in.Distribution = controlplanev1.DistributionOpenShift
			},
			wantDesired: 2,
		},
		{
			name:        "MachinePool_version_is_ignored_with_a_non_blocking_message",
			modify:      func(in *PoolGuardInput) { in.MachinePool.Spec.Template.Spec.Version = "v1.37.0" },
			wantDesired: 2,
			wantVersion: VersionIgnoredMessage,
		},
		{
			name: "the non-blocking messages are also set when a guard fails",
			modify: func(in *PoolGuardInput) {
				in.MachinePool.Spec.Template.Spec.Version = "v1.37.0"
				in.MachinePool.Spec.FailureDomains = []string{"eu-de-1"}
				in.MachinePool.Spec.Replicas = ptr.To[int32](0)
			},
			wantReason:      infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyZeroReplicasNotSupportedReason,
			wantVersion:     VersionIgnoredMessage,
			wantFailureDoms: FailureDomainsIgnoredMessage,
		},
		{
			name:            "failureDomains that differ from the zones are ignored with a message",
			modify:          func(in *PoolGuardInput) { in.MachinePool.Spec.FailureDomains = []string{"eu-de-1", "eu-de-2"} },
			wantDesired:     2,
			wantFailureDoms: FailureDomainsIgnoredMessage,
		},
		{
			name: "failureDomains equal to the zones in another order give no message",
			modify: func(in *PoolGuardInput) {
				in.MachinePool.Spec.FailureDomains = []string{"eu-de-3", "eu-de-1", "eu-de-2", "eu-de-1"}
			},
			wantDesired: 2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := guardInput()
			tc.modify(&in)
			result, err := PoolGuards(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.Reason).To(Equal(tc.wantReason))
			g.Expect(result.Blocked()).To(Equal(tc.wantReason != ""))
			g.Expect(result.StopsPublication).To(Equal(tc.wantStops))
			g.Expect(result.DesiredSizePerZone).To(Equal(tc.wantDesired))
			g.Expect(result.VersionMessage).To(Equal(tc.wantVersion))
			g.Expect(result.FailureDomainsMessage).To(Equal(tc.wantFailureDoms))
			if tc.wantReason == "" {
				g.Expect(result.Message).To(BeEmpty())
			}
			for _, want := range tc.wantMessage {
				g.Expect(result.Message).To(ContainSubstring(want))
			}
		})
	}
}

// TestPoolGuardsConflictOnEveryClaimant runs the guards for each of two claimants of one IBM pool: both are blocked,
// so the outcome does not depend on which object came first.
func TestPoolGuardsConflictOnEveryClaimant(t *testing.T) {
	g := NewWithT(t)
	first, second := guardPool("pool-a", "default", "eu-de-1"), guardPool("pool-z", "default", "eu-de-1")
	all := []infrav1.IBMCloudManagedMachinePool{second, first}
	for _, self := range []infrav1.IBMCloudManagedMachinePool{first, second} {
		in := guardInput()
		in.Pool = &self
		in.ClusterPools = all
		in.MachinePool.Spec.Replicas = ptr.To[int32](1)
		result, err := PoolGuards(in)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason), self.Name)
		g.Expect(result.StopsPublication).To(BeTrue(), self.Name)
	}
}

func TestPoolGuardsRejectsInvalidInput(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(in *PoolGuardInput)
		wantErr string
	}{
		{name: "no pool", modify: func(in *PoolGuardInput) { in.Pool = nil }, wantErr: "PoolGuardInput.Pool is nil"},
		{name: "no MachinePool", modify: func(in *PoolGuardInput) { in.MachinePool = nil }, wantErr: "PoolGuardInput.MachinePool is nil"},
		{name: "no IBMCloudManagedCluster", modify: func(in *PoolGuardInput) { in.InfraCluster = nil }, wantErr: "PoolGuardInput.InfraCluster is nil"},
		{name: "unknown distribution", modify: func(in *PoolGuardInput) { in.Distribution = "EKS" }, wantErr: `PoolGuardInput.Distribution is "EKS"`},
		{name: "no poolName", modify: func(in *PoolGuardInput) { in.Pool.Spec.PoolName = "" }, wantErr: "IBMCloudManagedMachinePool pool-a has no spec.poolName"},
		{name: "no zones", modify: func(in *PoolGuardInput) { in.Pool.Spec.Zones = nil }, wantErr: "IBMCloudManagedMachinePool pool-a has no spec.zones"},
		{
			name:    "negative replicas",
			modify:  func(in *PoolGuardInput) { in.MachinePool.Spec.Replicas = ptr.To[int32](-2) },
			wantErr: "MachinePool mp-a has spec.replicas=-2; want 0 or more",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := guardInput()
			tc.modify(&in)
			result, err := PoolGuards(in)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(result).To(Equal(PoolGuardResult{}))
		})
	}
}

func TestBoundedList(t *testing.T) {
	g := NewWithT(t)
	g.Expect(boundedList([]string{"a"})).To(Equal("a"))
	g.Expect(boundedList([]string{"a", "b", "c"})).To(Equal("a, b, c"))
	g.Expect(boundedList([]string{"a", "b", "c", "d", "e"})).To(Equal("a, b, c and 2 more"))
	g.Expect(noneIfEmpty(nil)).To(Equal("none"))
	g.Expect(noneIfEmpty([]string{"x", "y"})).To(Equal("x, y"))
}
