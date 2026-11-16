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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Snapshot is the inventory of the e2e account's region at one point in time: clusters, VPC load balancers, VPE
// gateways, security groups and volumes. Service IDs are not inventoried: the e2e tooling never reads IAM objects (IAM
// is customer-managed, and CAPIBM never creates or changes an IAM object), and a managed cluster creates none.
type Snapshot struct {
	// TakenAt is when the snapshot was taken.
	TakenAt time.Time
	// Resources are the inventoried resources.
	Resources []Resource
}

// TakeSnapshot lists the clusters and the VPC resources.
//
// Parameters:
//   - ctx bounds the requests.
//   - clusters lists the IBM Cloud Kubernetes Service clusters.
//   - vpc lists the VPC resources of the e2e region.
//   - now is the snapshot time (an argument, so that tests can fix it).
func TakeSnapshot(ctx context.Context, clusters ClusterLister, vpc VPCResourceLister, now time.Time) (Snapshot, error) {
	listed, err := clusters.ListClusters(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("inventory: listing clusters: %w", err)
	}
	resources := make([]Resource, 0, len(listed))
	for _, cluster := range listed {
		resources = append(resources, Resource{Kind: KindCluster, ID: cluster.ID, Name: cluster.Name})
	}
	vpcResources, err := vpc.ListVPCResources(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("inventory: %w", err)
	}
	return Snapshot{TakenAt: now, Resources: append(resources, vpcResources...)}, nil
}

// InventoryDiff is what a run added to the account.
type InventoryDiff struct {
	// Leftovers are the resources present after the run and not before it, except the tolerated ones. A clean run
	// has none.
	Leftovers []Resource
	// Tolerated are new resources that IBM keeps in a VPC on purpose (IsSharedVPCResource).
	Tolerated []Resource
}

// Clean reports whether the run left nothing behind.
func (d InventoryDiff) Clean() bool {
	return len(d.Leftovers) == 0
}

// String renders the diff for the suite log, one resource per line.
func (d InventoryDiff) String() string {
	var b strings.Builder
	for _, r := range d.Leftovers {
		fmt.Fprintf(&b, "leftover %s %q (ID %s)\n", r.Kind, r.Name, r.ID)
	}
	for _, r := range d.Tolerated {
		fmt.Fprintf(&b, "tolerated shared %s %q (ID %s): IBM keeps it in the VPC after its clusters are deleted\n", r.Kind, r.Name, r.ID)
	}
	if b.Len() == 0 {
		return "no new resources\n"
	}
	return b.String()
}

// Diff returns the resources of after that are not in before (same kind and ID), split into leftovers and tolerated
// shared VPC resources, each sorted by kind and name. Resources that disappeared during the run are not reported:
// the janitor or another cleanup may have removed them, which is not a leak.
//
// Parameters:
//   - before is the snapshot taken before the run's first cluster was created.
//   - after is the snapshot taken after the run's clusters were deleted.
func Diff(before, after Snapshot) InventoryDiff {
	known := make(map[string]bool, len(before.Resources))
	for _, r := range before.Resources {
		known[resourceKey(r)] = true
	}
	var diff InventoryDiff
	for _, r := range after.Resources {
		switch {
		case known[resourceKey(r)]:
		case IsSharedVPCResource(r):
			diff.Tolerated = append(diff.Tolerated, r)
		default:
			diff.Leftovers = append(diff.Leftovers, r)
		}
	}
	slices.SortFunc(diff.Leftovers, compareResources)
	slices.SortFunc(diff.Tolerated, compareResources)
	return diff
}

// IsSharedVPCResource reports whether r is one of the resources IBM creates once per VPC for all its clusters and
// never deletes, not even when the last cluster of the VPC is deleted (IBM docs cluster-remove.md and
// vpc-security-group2.md; observed on IBM Cloud, where they stayed while IKS removed the per-cluster ones within about
// 10 minutes):
//   - the shared VPE gateways iks-<service>-<vpcID>, for example iks-api-<vpcID> and iks-registry-<vpcID>;
//   - the security group kube-vpegw-<vpcID>.
//
// The VPC ID in the name must be the resource's own VPC, so a per-cluster resource such as the VPE gateway
// iks-<clusterID> or the security group kube-vpegw-<clusterID> is never tolerated.
//
// Parameters:
//   - r is a resource of an inventory snapshot.
func IsSharedVPCResource(r Resource) bool {
	if r.VPCID == "" {
		return false
	}
	switch r.Kind {
	case KindEndpointGateway:
		const prefix = "iks-"
		suffix := "-" + r.VPCID
		return strings.HasPrefix(r.Name, prefix) && strings.HasSuffix(r.Name, suffix) &&
			len(r.Name) > len(prefix)+len(suffix)
	case KindSecurityGroup:
		return r.Name == "kube-vpegw-"+r.VPCID
	default:
		return false
	}
}

// resourceKey identifies a resource across snapshots.
//
// Parameters:
//   - r is a resource of a snapshot.
func resourceKey(r Resource) string {
	return string(r.Kind) + "/" + r.ID
}

// compareResources orders resources by kind, then name, then ID.
//
// Parameters:
//   - a and b are the resources to order.
func compareResources(a, b Resource) int {
	return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
}
