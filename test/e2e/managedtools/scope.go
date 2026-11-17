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

package managedtools

import (
	"errors"
	"fmt"
	"strings"
)

// The two switches of the managed e2e configuration that let a lane share the e2e account with other runs.
//
// Their defaults keep the CI behaviour, where the Boskos lease of hack/scripts/ci/ci-e2e.sh guarantees one managed run
// per account at a time: the janitor deletes the clusters that crashed runs left behind, the inventory diff holds the
// lane responsible for every new cluster and VPC resource of the resource group, and the ROKS lane's bucket check
// (DiffBuckets) for every new bucket of the COS instance COS_INSTANCE_CRN. None of that holds next to other runs: the
// janitor cannot tell a crashed run's cluster from a running one's (both are old and carry the e2e prefix), and the
// checks would report the other runs' resources and buckets as the lane's leftovers. A run that shares the account
// therefore turns the janitor off and limits the checks to what can be attributed to the lane.
const (
	// JanitorVariable is the e2e configuration variable that switches the janitor on ("true", the default) or off
	// ("false").
	JanitorVariable = "E2E_MANAGED_JANITOR"
	// LeftoverScopeVariable is the e2e configuration variable that selects the LeftoverScope of the inventory diff and
	// of the ROKS lane's bucket check.
	LeftoverScopeVariable = "E2E_MANAGED_LEFTOVER_SCOPE"
)

// ParseJanitorSwitch returns whether a lane runs the janitor, from the value of JanitorVariable.
//
// Parameters:
//   - value is the variable's value: "true" or "false". "" (unset, or set to the empty string, which the configuration's
//     ${E2E_MANAGED_JANITOR:-true} form treats as unset too) means the default, "true".
func ParseJanitorSwitch(value string) (bool, error) {
	switch value {
	case "", "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s %q is not valid: use \"true\" (the default: before the lane, delete by ID the "+
			"clusters named <E2E_NAME_PREFIX>-* that are at least %s old, for an account that holds one managed run at "+
			"a time) or \"false\" (skip the janitor, for an account shared with other runs, whose clusters it must "+
			"never delete)", JanitorVariable, value, JanitorMinimumAge)
	}
}

// LeftoverScope selects which new resources a lane's leftover checks hold the lane responsible for: the inventory diff
// (clusters and VPC resources of the resource group, InventoryScope) and, in the ROKS lane, the bucket check of the
// COS instance COS_INSTANCE_CRN (DiffBuckets).
type LeftoverScope string

const (
	// LeftoverScopeAccount (the default) covers every cluster, load balancer, VPE gateway, security group and volume
	// of the resource group and, in the ROKS lane, every bucket of the COS instance COS_INSTANCE_CRN: anything new
	// after the lane is a leftover, whoever created it. The only new buckets that do not fail the lane are the run's
	// registry buckets, named roks-<ID of a cluster of the run>-..., which the lane deletes.
	LeftoverScopeAccount LeftoverScope = "account"
	// LeftoverScopeLane covers only the resources attributable to the lane: the clusters named
	// "<E2E_NAME_PREFIX>-<E2E_RUN_ID>-..." and the load balancers, VPE gateways and security groups of the lane's own
	// VPC. Volumes cannot be attributed and are left out (see InventoryScope.Includes). Of the new buckets of the COS
	// instance COS_INSTANCE_CRN, only the run's registry buckets, named roks-<ID of a cluster of the run>-..., count:
	// the ROKS lane deletes them and reports every other new bucket as not attributed to the run, without failing on
	// it and without touching it.
	LeftoverScopeLane LeftoverScope = "lane"
)

// ParseLeftoverScope returns the leftover scope named by the value of LeftoverScopeVariable.
//
// Parameters:
//   - value is the variable's value: "account" or "lane". "" (unset, or set to the empty string) means the default,
//     "account".
func ParseLeftoverScope(value string) (LeftoverScope, error) {
	switch LeftoverScope(value) {
	case "", LeftoverScopeAccount:
		return LeftoverScopeAccount, nil
	case LeftoverScopeLane:
		return LeftoverScopeLane, nil
	default:
		return "", fmt.Errorf("%s %q is not valid: use %q (the default: every new cluster and VPC resource of the "+
			"resource group and, in the ROKS lane, every new bucket of %s that is not the run's registry bucket is a "+
			"leftover, for an account that holds one managed run at a time) or %q (only the lane's own clusters, the "+
			"VPC resources of the VPC IBMCLOUD_VPC_NAME and the registry buckets named %s<ID of a cluster of the "+
			"run>-..., for an account shared with other runs that each use a VPC of their own)", LeftoverScopeVariable,
			value, LeftoverScopeAccount, COSInstanceCRNVariable, LeftoverScopeLane, registryBucketPrefix)
	}
}

// InventoryScope decides which resources of an inventory snapshot belong to a lane's leftover check. The lane applies
// the same scope to its baseline and to every later snapshot, so that the diff compares like with like. The zero value
// is the account scope; build a lane scope with NewLaneScope.
type InventoryScope struct {
	// lane is true for the lane scope; false is the account scope.
	lane bool
	// runNamePrefix is "<prefix>-<run-id>-", the start of every cluster name of the lane's run (lane scope only).
	runNamePrefix string
	// vpcID is the ID of the lane's own VPC (lane scope only).
	vpcID string
}

// AccountScope returns the account scope: every resource of a snapshot counts.
func AccountScope() InventoryScope {
	return InventoryScope{}
}

// NewLaneScope returns the lane scope of a run: its clusters are those named "<namePrefix>-<runID>-...", and its VPC
// resources those of the VPC vpcID. The VPC must be used by this run alone while the lane runs, because every load
// balancer, VPE gateway and security group in it is the lane's.
//
// Parameters:
//   - namePrefix is E2E_NAME_PREFIX (see ValidateNamePrefix).
//   - runID is E2E_RUN_ID, or the ID NewRunID generated; together with namePrefix it must pass RunNamePrefix.
//   - vpcID is the ID of the lane's VPC, IBMCLOUD_VPC_NAME resolved by VPCService.ResolveVPCID.
func NewLaneScope(namePrefix, runID, vpcID string) (InventoryScope, error) {
	runNamePrefix, err := RunNamePrefix(namePrefix, runID)
	if err != nil {
		return InventoryScope{}, fmt.Errorf("%s=%s: %w", LeftoverScopeVariable, LeftoverScopeLane, err)
	}
	if vpcID == "" || strings.ContainsAny(vpcID, " \t\n") {
		return InventoryScope{}, errors.New(LeftoverScopeVariable + "=" + string(LeftoverScopeLane) + ": the lane's VPC " +
			"ID is empty or contains white space; resolve IBMCLOUD_VPC_NAME to the ID of the existing e2e VPC first " +
			"(VPCService.ResolveVPCID)")
	}
	return InventoryScope{lane: true, runNamePrefix: runNamePrefix, vpcID: vpcID}, nil
}

// Scope returns LeftoverScopeLane for a lane scope and LeftoverScopeAccount otherwise.
func (s InventoryScope) Scope() LeftoverScope {
	if s.lane {
		return LeftoverScopeLane
	}
	return LeftoverScopeAccount
}

// NeedsLoadBalancerVPCs reports whether the scope selects load balancers by VPC, so that its snapshots must come from a
// VPCService built with WithLoadBalancerVPCs. Only the lane scope does; the account scope keeps every load balancer
// whatever its VPC (and Diff never reads a load balancer's VPC), so it spares the listing of the region's subnets.
func (s InventoryScope) NeedsLoadBalancerVPCs() bool {
	return s.lane
}

// Includes reports whether r belongs to the scope. The account scope includes every resource. The lane scope includes:
//   - a cluster whose name starts with the run's "<prefix>-<run-id>-" (the name contract makes that prefix unique to
//     the run);
//   - a load balancer, VPE gateway or security group of the lane's VPC, including the shared VPE gateways and security
//     group that IBM keeps in the VPC, which Diff tolerates. A load balancer's VPC is the VPC of its subnets, which
//     only a VPCService built with WithLoadBalancerVPCs fills in (NeedsLoadBalancerVPCs); from any other lister every
//     load balancer would fall outside the lane scope.
//
// It never includes a volume: IKS and ROKS create VPC block volumes only for PersistentVolumeClaims, through the IBM
// VPC Block CSI driver, which names the volume after the PersistentVolume (pvc-<uid>) and tags it only with the user
// tags of the storage class and of the claim's secret (ibm-vpc-block-csi-driver, pkg/ibmcsidriver/controller_helper.go,
// getVolumeParameters and overrideParams); the cluster ID goes only on the attachment. A detached leftover volume
// therefore carries nothing that names its cluster or VPC, so a lane cannot tell its own from another run's. The lanes
// create no PersistentVolumeClaim; whoever shares the account checks volumes account-wide once every run has ended.
// Unknown kinds are left out for the same reason: nothing proves they are the lane's.
//
// Parameters:
//   - r is a resource of an inventory snapshot.
func (s InventoryScope) Includes(r Resource) bool {
	if !s.lane {
		return true
	}
	switch r.Kind {
	case KindCluster:
		return strings.HasPrefix(r.Name, s.runNamePrefix)
	case KindLoadBalancer, KindEndpointGateway, KindSecurityGroup:
		return r.VPCID == s.vpcID
	default:
		return false
	}
}

// Apply returns snapshot with only the resources the scope includes, in their original order; the account scope
// returns it unchanged.
//
// Parameters:
//   - snapshot is a TakeSnapshot result.
func (s InventoryScope) Apply(snapshot Snapshot) Snapshot {
	if !s.lane {
		return snapshot
	}
	kept := make([]Resource, 0, len(snapshot.Resources))
	for _, r := range snapshot.Resources {
		if s.Includes(r) {
			kept = append(kept, r)
		}
	}
	return Snapshot{TakenAt: snapshot.TakenAt, Resources: kept}
}

// String describes the scope for the suite log.
func (s InventoryScope) String() string {
	if !s.lane {
		return fmt.Sprintf("%s: every cluster, load balancer, VPE gateway, security group and volume of the resource group",
			LeftoverScopeAccount)
	}
	return fmt.Sprintf("%s: the clusters named %s* and the load balancers, VPE gateways and security groups of VPC %s; "+
		"volumes are not checked (nothing on a volume names its cluster)", LeftoverScopeLane, s.runNamePrefix, s.vpcID)
}
