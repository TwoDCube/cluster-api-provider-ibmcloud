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
	"context"
	"net/http"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// The VPC and cluster IDs of the inventory tests are made up; the shapes follow what IBM Cloud showed after a cluster
// delete.
const (
	testVPCID     = "r010-e2e-vpc"
	otherVPCID    = "r010-other-vpc"
	testClusterID = "c0runaaaaaaaaaaaaaaa"
)

func TestIsSharedVPCResource(t *testing.T) {
	testCases := []struct {
		name     string
		resource Resource
		want     bool
	}{
		{name: "shared VPE iks-api-<vpcID>", resource: Resource{Kind: KindEndpointGateway, Name: "iks-api-" + testVPCID, VPCID: testVPCID}, want: true},
		{name: "shared VPE iks-cos-config-<vpcID>", resource: Resource{Kind: KindEndpointGateway, Name: "iks-cos-config-" + testVPCID, VPCID: testVPCID}, want: true},
		{name: "shared VPE iks-registry-<vpcID>", resource: Resource{Kind: KindEndpointGateway, Name: "iks-registry-" + testVPCID, VPCID: testVPCID}, want: true},
		{name: "shared SG kube-vpegw-<vpcID>", resource: Resource{Kind: KindSecurityGroup, Name: "kube-vpegw-" + testVPCID, VPCID: testVPCID}, want: true},
		{name: "per-cluster VPE iks-<clusterID>", resource: Resource{Kind: KindEndpointGateway, Name: "iks-" + testClusterID, VPCID: testVPCID}},
		{name: "per-cluster SG kube-vpegw-<clusterID>", resource: Resource{Kind: KindSecurityGroup, Name: "kube-vpegw-" + testClusterID, VPCID: testVPCID}},
		{name: "per-cluster SG kube-<clusterID>", resource: Resource{Kind: KindSecurityGroup, Name: "kube-" + testClusterID, VPCID: testVPCID}},
		{name: "per-cluster SG kube-lbaas-<clusterID>", resource: Resource{Kind: KindSecurityGroup, Name: "kube-lbaas-" + testClusterID, VPCID: testVPCID}},
		{name: "shared name of another VPC", resource: Resource{Kind: KindEndpointGateway, Name: "iks-api-" + otherVPCID, VPCID: testVPCID}},
		{name: "kube-vpegw of another VPC", resource: Resource{Kind: KindSecurityGroup, Name: "kube-vpegw-" + otherVPCID, VPCID: testVPCID}},
		{name: "no service segment", resource: Resource{Kind: KindEndpointGateway, Name: "iks--" + testVPCID, VPCID: testVPCID}},
		{name: "no VPC reference", resource: Resource{Kind: KindEndpointGateway, Name: "iks-api-" + testVPCID}},
		{name: "load balancer with a shared-looking name", resource: Resource{Kind: KindLoadBalancer, Name: "iks-api-" + testVPCID, VPCID: testVPCID}},
		{name: "cluster", resource: Resource{Kind: KindCluster, Name: "kube-vpegw-" + testVPCID, VPCID: testVPCID}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(IsSharedVPCResource(tc.resource)).To(Equal(tc.want))
		})
	}
}

func TestDiff(t *testing.T) {
	g := NewWithT(t)
	before := Snapshot{Resources: []Resource{
		{Kind: KindCluster, ID: "c0preexistingaaaaaaa", Name: "someone-else"},
		{Kind: KindSecurityGroup, ID: "sg-default", Name: "default-sg", VPCID: testVPCID},
		{Kind: KindVolume, ID: "vol-gone", Name: "removed-during-run"},
	}}
	after := Snapshot{Resources: []Resource{
		{Kind: KindCluster, ID: "c0preexistingaaaaaaa", Name: "someone-else"},
		{Kind: KindSecurityGroup, ID: "sg-default", Name: "default-sg", VPCID: testVPCID},
		{Kind: KindVolume, ID: "vol-pvc", Name: "pvc-1234"},
		{Kind: KindEndpointGateway, ID: "vpe-api", Name: "iks-api-" + testVPCID, VPCID: testVPCID},
		{Kind: KindSecurityGroup, ID: "sg-vpegw", Name: "kube-vpegw-" + testVPCID, VPCID: testVPCID},
		{Kind: KindCluster, ID: testClusterID, Name: "capibm-e2e-abc123-iks"},
		{Kind: KindLoadBalancer, ID: "lb-1", Name: "kube-" + testClusterID + "-1234"},
	}}

	diff := Diff(before, after)

	g.Expect(diff.Clean()).To(BeFalse())
	g.Expect(diff.Leftovers).To(Equal([]Resource{
		{Kind: KindCluster, ID: testClusterID, Name: "capibm-e2e-abc123-iks"},
		{Kind: KindLoadBalancer, ID: "lb-1", Name: "kube-" + testClusterID + "-1234"},
		{Kind: KindVolume, ID: "vol-pvc", Name: "pvc-1234"},
	}))
	g.Expect(diff.Tolerated).To(Equal([]Resource{
		{Kind: KindEndpointGateway, ID: "vpe-api", Name: "iks-api-" + testVPCID, VPCID: testVPCID},
		{Kind: KindSecurityGroup, ID: "sg-vpegw", Name: "kube-vpegw-" + testVPCID, VPCID: testVPCID},
	}))
	g.Expect(diff.String()).To(ContainSubstring(`leftover volume "pvc-1234" (ID vol-pvc)`))
	g.Expect(diff.String()).To(ContainSubstring(`tolerated shared VPE gateway "iks-api-` + testVPCID + `"`))
}

func TestDiffClean(t *testing.T) {
	g := NewWithT(t)
	snapshot := Snapshot{Resources: []Resource{{Kind: KindVolume, ID: "vol-1", Name: "keep"}}}
	diff := Diff(snapshot, snapshot)
	g.Expect(diff.Clean()).To(BeTrue())
	g.Expect(diff.String()).To(Equal("no new resources\n"))
}

// vpcFixture fills the fake VPC API with the state after a deleted IKS cluster, as observed on IBM Cloud right after
// the 404: the shared gateways and security group of the VPC, the per-cluster ones IKS removes within about 10
// minutes, resources of another resource group, and the VPC's default security group.
func vpcFixture(fake *fakeIBM, perCluster bool) {
	fake.vpc["load_balancers"] = []map[string]any{vpcItem("lb-other-rg", "unrelated", "rg-other", "")}
	fake.vpc["endpoint_gateways"] = []map[string]any{
		vpcItem("vpe-api", "iks-api-"+testVPCID, "rg-e2e", testVPCID),
		vpcItem("vpe-cos", "iks-cos-"+testVPCID, "rg-e2e", testVPCID),
		vpcItem("vpe-cos-config", "iks-cos-config-"+testVPCID, "rg-e2e", testVPCID),
		vpcItem("vpe-riaas", "iks-riaas-"+testVPCID, "rg-e2e", testVPCID),
		vpcItem("vpe-registry", "iks-registry-"+testVPCID, "rg-e2e", testVPCID),
	}
	fake.vpc["security_groups"] = []map[string]any{
		vpcItem("sg-default", "default-sg", "rg-e2e", testVPCID),
		vpcItem("sg-vpegw", "kube-vpegw-"+testVPCID, "rg-e2e", testVPCID),
	}
	fake.vpc["volumes"] = nil
	if perCluster {
		fake.vpc["load_balancers"] = append(fake.vpc["load_balancers"], vpcItem("lb-ingress", "kube-"+testClusterID+"-ab12", "rg-e2e", ""))
		fake.vpc["endpoint_gateways"] = append(fake.vpc["endpoint_gateways"], vpcItem("vpe-cluster", "iks-"+testClusterID, "rg-e2e", testVPCID))
		fake.vpc["security_groups"] = append(fake.vpc["security_groups"],
			vpcItem("sg-cluster", "kube-"+testClusterID, "rg-e2e", testVPCID),
			vpcItem("sg-lbaas", "kube-lbaas-"+testClusterID, "rg-e2e", testVPCID),
			vpcItem("sg-cluster-vpegw", "kube-vpegw-"+testClusterID, "rg-e2e", testVPCID))
	}
}

func TestInventoryDiffToleratesSharedGatewaysOverHTTP(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.pageSize = 2 // the shared gateways span three pages
	ctx := context.Background()
	clusters, vpc := fake.clusterService("rg-e2e"), fake.vpcService("rg-e2e")

	// Before the run: an empty VPC except its default security group.
	fake.vpc["security_groups"] = []map[string]any{vpcItem("sg-default", "default-sg", "rg-e2e", testVPCID)}
	before, err := TakeSnapshot(ctx, clusters, vpc, janitorNow)
	g.Expect(err).NotTo(HaveOccurred())

	// Right after the cluster's 404 (as observed on IBM Cloud): the per-cluster resources are still there.
	vpcFixture(fake, true)
	early, err := TakeSnapshot(ctx, clusters, vpc, janitorNow.Add(time.Minute))
	g.Expect(err).NotTo(HaveOccurred())
	earlyDiff := Diff(before, early)
	g.Expect(earlyDiff.Clean()).To(BeFalse())
	g.Expect(resourceNames(earlyDiff.Leftovers)).To(ConsistOf("kube-"+testClusterID+"-ab12", "iks-"+testClusterID,
		"kube-"+testClusterID, "kube-lbaas-"+testClusterID, "kube-vpegw-"+testClusterID))

	// About 10 minutes later IKS has removed the per-cluster resources; only the shared ones stay, for good: IBM
	// documents that it never deletes them.
	vpcFixture(fake, false)
	late, err := TakeSnapshot(ctx, clusters, vpc, janitorNow.Add(11*time.Minute))
	g.Expect(err).NotTo(HaveOccurred())
	lateDiff := Diff(before, late)
	g.Expect(lateDiff.Clean()).To(BeTrue(), lateDiff.String())
	g.Expect(resourceNames(lateDiff.Tolerated)).To(ConsistOf("iks-api-"+testVPCID, "iks-cos-"+testVPCID,
		"iks-cos-config-"+testVPCID, "iks-riaas-"+testVPCID, "iks-registry-"+testVPCID, "kube-vpegw-"+testVPCID))
	// The other resource group's load balancer is outside the inventory.
	g.Expect(resourceNames(late.Resources)).NotTo(ContainElement("unrelated"))
}

func TestVPCServiceListVPCResourcesRequestsAndPages(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.pageSize = 1
	fake.vpc["volumes"] = []map[string]any{
		vpcItem("vol-1", "pvc-1", "rg-e2e", ""),
		vpcItem("vol-2", "pvc-2", "rg-e2e", ""),
		vpcItem("vol-3", "pvc-3", "rg-e2e", ""),
	}

	resources, err := fake.vpcService("").ListVPCResources(context.Background())

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(resources).To(Equal([]Resource{
		{Kind: KindVolume, ID: "vol-1", Name: "pvc-1"},
		{Kind: KindVolume, ID: "vol-2", Name: "pvc-2"},
		{Kind: KindVolume, ID: "vol-3", Name: "pvc-3"},
	}))
	for _, r := range fake.recorded() {
		g.Expect(r.Method).To(Equal(http.MethodGet))
		g.Expect(r.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(r.Header.Get("User-Agent")).To(Equal(DefaultUserAgent))
	}
	volumePages := fake.requestCount("/v1/volumes")
	g.Expect(volumePages).To(Equal(3))
	// Without WithLoadBalancerVPCs the four inventoried collections are all it asks for: no subnet listing.
	g.Expect(fake.requestCount(subnetsPath)).To(BeZero())
	g.Expect(len(fake.recorded())).To(Equal(len(vpcCollections) - 1 + volumePages))
}

func TestTakeSnapshotErrors(t *testing.T) {
	lane, err := NewLaneScope(DefaultNamePrefix, laneRunID, testVPCID)
	if err != nil {
		t.Fatalf("building the lane scope: %v", err)
	}
	testCases := []struct {
		name             string
		scope            InventoryScope
		listStatus       int
		vpcStatus        int
		collectionStatus map[string]int
		// wantErr is a part of the expected error; "" expects the snapshot to succeed.
		wantErr            string
		wantSubnetRequests int
	}{
		{name: "account scope: the cluster listing fails", scope: AccountScope(), listStatus: http.StatusInternalServerError,
			wantErr: "inventory: listing clusters"},
		{name: "lane scope: the cluster listing fails", scope: lane, listStatus: http.StatusInternalServerError,
			wantErr: "inventory: listing clusters"},
		{name: "account scope lists no subnets, so its first VPC listing to fail is the load balancers'", scope: AccountScope(),
			vpcStatus: http.StatusForbidden, wantErr: "inventory: listing VPC load balancers"},
		{name: "lane scope lists the subnets first", scope: lane, vpcStatus: http.StatusForbidden,
			wantErr: "inventory: listing VPC subnets", wantSubnetRequests: 1},
		{name: "account scope does not depend on the subnet listing", scope: AccountScope(),
			collectionStatus: map[string]int{"subnets": http.StatusForbidden}},
		{name: "lane scope fails without the subnet listing", scope: lane,
			collectionStatus: map[string]int{"subnets": http.StatusForbidden}, wantErr: "inventory: listing VPC subnets",
			wantSubnetRequests: 1},
		{name: "account scope: the load balancer listing fails", scope: AccountScope(),
			collectionStatus: map[string]int{"load_balancers": http.StatusForbidden}, wantErr: "inventory: listing VPC load balancers"},
		{name: "lane scope: the load balancer listing fails after the subnets", scope: lane,
			collectionStatus: map[string]int{"load_balancers": http.StatusForbidden}, wantErr: "inventory: listing VPC load balancers",
			wantSubnetRequests: 1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)
			fake.listStatus = tc.listStatus
			fake.vpcStatus = tc.vpcStatus
			for collection, status := range tc.collectionStatus {
				fake.vpcCollectionStatus[collection] = status
			}

			_, err := TakeSnapshot(context.Background(), fake.clusterService(""), fake.inventoryVPCService("", tc.scope), janitorNow)

			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
			} else {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			}
			g.Expect(fake.requestCount(subnetsPath)).To(Equal(tc.wantSubnetRequests))
		})
	}
}

// resourceNames returns the resource names.
func resourceNames(resources []Resource) []string {
	out := make([]string, 0, len(resources))
	for _, r := range resources {
		out = append(out, r.Name)
	}
	return out
}
