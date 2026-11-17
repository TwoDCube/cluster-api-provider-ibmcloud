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

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/vpc-go-sdk/vpcv1"

	. "github.com/onsi/gomega"
)

// The made-up lane and foreign run of the scope tests: the lane runs in testVPCID, a foreign run of the same account
// in otherVPCID; both use the default name prefix.
const (
	laneRunID       = "lane01"
	laneClusterName = "capibm-e2e-lane01-iks"
	laneClusterID   = "c0laneaaaaaaaaaaaaaa"
	laneSubnetID    = "sn-lane-zone1"
	// otherClusterName belongs to another run of the account; lookalikeClusterName to a run whose ID merely starts
	// with the lane's run ID.
	otherClusterName     = "capibm-e2e-other1-iks"
	otherClusterID       = "c0otheraaaaaaaaaaaaa"
	lookalikeClusterName = "capibm-e2e-lane012-iks"
	otherSubnetID        = "sn-other-zone1"
)

func TestParseJanitorSwitch(t *testing.T) {
	testCases := []struct {
		name    string
		value   string
		want    bool
		wantErr bool
	}{
		{name: "unset means the default, on", value: "", want: true},
		{name: "true", value: "true", want: true},
		{name: "false", value: "false", want: false},
		{name: "upper case is refused", value: "TRUE", wantErr: true},
		{name: "yes is refused", value: "yes", wantErr: true},
		{name: "0 is refused", value: "0", wantErr: true},
		{name: "white space is refused", value: " false", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ParseJanitorSwitch(tc.value)
			if tc.wantErr {
				g.Expect(err).To(MatchError(And(ContainSubstring(JanitorVariable), ContainSubstring(`use "true"`),
					ContainSubstring(`or "false"`))))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestParseLeftoverScope(t *testing.T) {
	testCases := []struct {
		name    string
		value   string
		want    LeftoverScope
		wantErr bool
	}{
		{name: "unset means the default, account", value: "", want: LeftoverScopeAccount},
		{name: "account", value: "account", want: LeftoverScopeAccount},
		{name: "lane", value: "lane", want: LeftoverScopeLane},
		{name: "upper case is refused", value: "Lane", wantErr: true},
		{name: "vpc is refused", value: "vpc", wantErr: true},
		{name: "white space is refused", value: "lane ", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ParseLeftoverScope(tc.value)
			if tc.wantErr {
				g.Expect(err).To(MatchError(And(ContainSubstring(LeftoverScopeVariable), ContainSubstring(`use "account"`),
					ContainSubstring(`or "lane"`))))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestNewLaneScope(t *testing.T) {
	testCases := []struct {
		name       string
		namePrefix string
		runID      string
		vpcID      string
		wantErr    string
	}{
		{name: "valid", namePrefix: DefaultNamePrefix, runID: laneRunID, vpcID: testVPCID},
		{name: "invalid prefix", namePrefix: "c", runID: laneRunID, vpcID: testVPCID, wantErr: "E2E_NAME_PREFIX"},
		{name: "invalid run ID", namePrefix: DefaultNamePrefix, runID: "Lane-01", vpcID: testVPCID, wantErr: "E2E_RUN_ID"},
		{name: "empty VPC ID", namePrefix: DefaultNamePrefix, runID: laneRunID, wantErr: "resolve IBMCLOUD_VPC_NAME"},
		{name: "VPC ID with white space", namePrefix: DefaultNamePrefix, runID: laneRunID, vpcID: "r010 x", wantErr: "white space"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			scope, err := NewLaneScope(tc.namePrefix, tc.runID, tc.vpcID)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(And(ContainSubstring(LeftoverScopeVariable+"=lane"), ContainSubstring(tc.wantErr))))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(scope.Scope()).To(Equal(LeftoverScopeLane))
			g.Expect(scope.String()).To(And(ContainSubstring("capibm-e2e-lane01-*"), ContainSubstring(testVPCID),
				ContainSubstring("volumes are not checked")))
		})
	}
}

func TestInventoryScopeIncludes(t *testing.T) {
	lane, err := NewLaneScope(DefaultNamePrefix, laneRunID, testVPCID)
	if err != nil {
		t.Fatalf("building the lane scope: %v", err)
	}
	testCases := []struct {
		name     string
		resource Resource
		inLane   bool
	}{
		{name: "the lane's cluster", resource: Resource{Kind: KindCluster, Name: laneClusterName}, inLane: true},
		{name: "another lane of the same run", resource: Resource{Kind: KindCluster, Name: "capibm-e2e-lane01-roks"}, inLane: true},
		{name: "a foreign run's cluster", resource: Resource{Kind: KindCluster, Name: otherClusterName}},
		{name: "a run ID that only starts with the lane's", resource: Resource{Kind: KindCluster, Name: lookalikeClusterName}},
		{name: "a cluster without the e2e prefix", resource: Resource{Kind: KindCluster, Name: "production-iks"}},
		{name: "a load balancer of the lane's VPC", resource: Resource{Kind: KindLoadBalancer, VPCID: testVPCID}, inLane: true},
		{name: "a load balancer of another VPC", resource: Resource{Kind: KindLoadBalancer, VPCID: otherVPCID}},
		{name: "a load balancer of no known VPC", resource: Resource{Kind: KindLoadBalancer}},
		{name: "a VPE gateway of the lane's VPC", resource: Resource{Kind: KindEndpointGateway, VPCID: testVPCID}, inLane: true},
		{name: "a VPE gateway of another VPC", resource: Resource{Kind: KindEndpointGateway, VPCID: otherVPCID}},
		{name: "a security group of the lane's VPC", resource: Resource{Kind: KindSecurityGroup, VPCID: testVPCID}, inLane: true},
		{name: "a security group of another VPC", resource: Resource{Kind: KindSecurityGroup, VPCID: otherVPCID}},
		{name: "a volume, never attributable", resource: Resource{Kind: KindVolume, Name: "pvc-1234"}},
		{name: "a kind the scope does not know", resource: Resource{Kind: "floating IP", VPCID: testVPCID}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(lane.Includes(tc.resource)).To(Equal(tc.inLane), "lane scope")
			g.Expect(AccountScope().Includes(tc.resource)).To(BeTrue(), "account scope")
			g.Expect(InventoryScope{}.Includes(tc.resource)).To(BeTrue(), "the zero value is the account scope")
		})
	}
}

func TestInventoryScopeApply(t *testing.T) {
	g := NewWithT(t)
	lane, err := NewLaneScope(DefaultNamePrefix, laneRunID, testVPCID)
	g.Expect(err).NotTo(HaveOccurred())
	snapshot := Snapshot{TakenAt: janitorNow, Resources: []Resource{
		{Kind: KindCluster, ID: otherClusterID, Name: otherClusterName},
		{Kind: KindSecurityGroup, ID: "sg-lane", Name: "kube-" + laneClusterID, VPCID: testVPCID},
		{Kind: KindCluster, ID: laneClusterID, Name: laneClusterName},
		{Kind: KindVolume, ID: "vol-1", Name: "pvc-1234"},
	}}

	g.Expect(AccountScope().Apply(snapshot)).To(Equal(snapshot))
	g.Expect(AccountScope().Scope()).To(Equal(LeftoverScopeAccount))
	g.Expect(AccountScope().String()).To(HavePrefix("account: "))
	g.Expect(AccountScope().NeedsLoadBalancerVPCs()).To(BeFalse(), "the account scope keeps every load balancer")
	g.Expect(InventoryScope{}.NeedsLoadBalancerVPCs()).To(BeFalse(), "the zero value is the account scope")
	g.Expect(lane.NeedsLoadBalancerVPCs()).To(BeTrue(), "the lane scope selects load balancers by VPC")
	g.Expect(lane.Apply(snapshot)).To(Equal(Snapshot{TakenAt: janitorNow, Resources: []Resource{
		{Kind: KindSecurityGroup, ID: "sg-lane", Name: "kube-" + laneClusterID, VPCID: testVPCID},
		{Kind: KindCluster, ID: laneClusterID, Name: laneClusterName},
	}}))
	g.Expect(snapshot.Resources).To(HaveLen(4), "Apply must not change its argument")
}

// accountPhase is the state of the shared account that sharedAccountFixture serves.
type accountPhase int

const (
	// beforeLane: someone's production cluster, the two VPCs' subnets and default security groups.
	beforeLane accountPhase = iota
	// afterCleanLane: the lane removed everything it created; meanwhile the foreign run created its cluster, load
	// balancer, VPE gateway and security group in its own VPC, a run with a lookalike run ID created a cluster, IBM
	// added the shared VPE gateway of the lane's VPC, and somebody created a volume.
	afterCleanLane
	// afterLaneLeftovers: as afterCleanLane, and the lane left its cluster (still deleting), a load balancer, its
	// per-cluster VPE gateway and its security group.
	afterLaneLeftovers
)

// sharedAccountFixture fills the fake IBM APIs with an account that a lane shares with a foreign run in another VPC.
//
// Parameters:
//   - fake is the fake IBM server.
//   - phase selects the state of the account.
func sharedAccountFixture(fake *fakeIBM, phase accountPhase) {
	created := ibmDate(janitorNow.Add(-time.Hour))
	fake.clusters = []map[string]any{ibmCluster("c0productionaaaaaaaa", "production-iks", ibmDate(janitorNow.Add(-48*time.Hour)), "normal")}
	fake.vpc["subnets"] = []map[string]any{
		vpcItem(laneSubnetID, "lane-zone1", "rg-e2e", testVPCID),
		vpcItem(otherSubnetID, "other-zone1", "rg-e2e", otherVPCID),
	}
	fake.vpc["security_groups"] = []map[string]any{
		vpcItem("sg-default-lane", "default-lane", "rg-e2e", testVPCID),
		vpcItem("sg-default-other", "default-other", "rg-e2e", otherVPCID),
	}
	fake.vpc["load_balancers"] = nil
	fake.vpc["endpoint_gateways"] = nil
	fake.vpc["volumes"] = nil
	if phase == beforeLane {
		return
	}
	fake.clusters = append(fake.clusters,
		ibmCluster(otherClusterID, otherClusterName, created, "normal"),
		ibmCluster("c0lookalikeaaaaaaaaa", lookalikeClusterName, created, "normal"))
	fake.vpc["load_balancers"] = []map[string]any{loadBalancerItem("lb-other", "kube-"+otherClusterID+"-cd34", "rg-e2e", otherSubnetID)}
	fake.vpc["endpoint_gateways"] = []map[string]any{
		vpcItem("vpe-api-lane", "iks-api-"+testVPCID, "rg-e2e", testVPCID),
		vpcItem("vpe-other", "iks-"+otherClusterID, "rg-e2e", otherVPCID),
	}
	fake.vpc["security_groups"] = append(fake.vpc["security_groups"], vpcItem("sg-other", "kube-"+otherClusterID, "rg-e2e", otherVPCID))
	fake.vpc["volumes"] = []map[string]any{vpcItem("vol-pvc", "pvc-1234", "rg-e2e", "")}
	if phase == afterCleanLane {
		return
	}
	fake.clusters = append(fake.clusters, ibmCluster(laneClusterID, laneClusterName, created, "deleting"))
	fake.vpc["load_balancers"] = append(fake.vpc["load_balancers"],
		loadBalancerItem("lb-lane", "kube-"+laneClusterID+"-ab12", "rg-e2e", laneSubnetID))
	fake.vpc["endpoint_gateways"] = append(fake.vpc["endpoint_gateways"], vpcItem("vpe-lane", "iks-"+laneClusterID, "rg-e2e", testVPCID))
	fake.vpc["security_groups"] = append(fake.vpc["security_groups"], vpcItem("sg-lane", "kube-"+laneClusterID, "rg-e2e", testVPCID))
}

func TestLeftoverScopesInASharedAccountOverHTTP(t *testing.T) {
	lane, err := NewLaneScope(DefaultNamePrefix, laneRunID, testVPCID)
	if err != nil {
		t.Fatalf("building the lane scope: %v", err)
	}
	// What the foreign run, the lookalike run and somebody else added: only the account scope reports it.
	foreign := []string{otherClusterName, lookalikeClusterName, "kube-" + otherClusterID + "-cd34", "iks-" + otherClusterID,
		"kube-" + otherClusterID, "pvc-1234"}
	// The lane's own leftovers: both scopes report them.
	own := []string{laneClusterName, "kube-" + laneClusterID + "-ab12", "iks-" + laneClusterID, "kube-" + laneClusterID}
	testCases := []struct {
		name          string
		scope         InventoryScope
		phase         accountPhase
		wantLeftovers []string
	}{
		{
			name:          "account scope reports the lane's leftovers and every foreign resource",
			scope:         AccountScope(),
			phase:         afterLaneLeftovers,
			wantLeftovers: append(append([]string{}, foreign...), own...),
		},
		{
			name:          "lane scope reports the lane's leftovers and ignores the foreign run's cluster, load balancer and security group",
			scope:         lane,
			phase:         afterLaneLeftovers,
			wantLeftovers: own,
		},
		{
			name:          "account scope fails a clean lane on the foreign resources",
			scope:         AccountScope(),
			phase:         afterCleanLane,
			wantLeftovers: foreign,
		},
		{
			name:  "lane scope passes a clean lane next to a foreign run",
			scope: lane,
			phase: afterCleanLane,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)
			ctx := context.Background()
			clusters, vpc := fake.clusterService("rg-e2e"), fake.inventoryVPCService("rg-e2e", tc.scope)

			sharedAccountFixture(fake, beforeLane)
			before, err := TakeSnapshot(ctx, clusters, vpc, janitorNow)
			g.Expect(err).NotTo(HaveOccurred())
			sharedAccountFixture(fake, tc.phase)
			after, err := TakeSnapshot(ctx, clusters, vpc, janitorNow.Add(time.Hour))
			g.Expect(err).NotTo(HaveOccurred())

			diff := Diff(tc.scope.Apply(before), tc.scope.Apply(after))

			g.Expect(diff.Clean()).To(Equal(len(tc.wantLeftovers) == 0), diff.String())
			g.Expect(resourceNames(diff.Leftovers)).To(ConsistOf(tc.wantLeftovers))
			// The shared VPE gateway IBM keeps in the lane's VPC is tolerated in both scopes.
			g.Expect(resourceNames(diff.Tolerated)).To(ConsistOf("iks-api-" + testVPCID))
			// Only the lane scope lists the subnets, once per snapshot; the account scope makes no extra request.
			wantSubnetRequests := 0
			if tc.scope.NeedsLoadBalancerVPCs() {
				wantSubnetRequests = 2
			}
			g.Expect(fake.requestCount(subnetsPath)).To(Equal(wantSubnetRequests))
		})
	}
}

func TestVPCServiceAttributesLoadBalancersToTheVPCOfTheirSubnets(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.pageSize = 1 // the subnets span two pages
	fake.vpc["subnets"] = []map[string]any{
		vpcItem(laneSubnetID, "lane-zone1", "rg-e2e", testVPCID),
		// A subnet of another resource group still attributes the resource group's load balancer.
		vpcItem("sn-lane-zone2", "lane-zone2", "rg-network", testVPCID),
	}
	fake.vpc["load_balancers"] = []map[string]any{
		loadBalancerItem("lb-lane", "kube-"+laneClusterID+"-ab12", "rg-e2e", laneSubnetID, "sn-lane-zone2"),
		loadBalancerItem("lb-unknown", "on-unlisted-subnets", "rg-e2e", "sn-gone"),
		loadBalancerItem("lb-other-rg", "unrelated", "rg-other", laneSubnetID),
	}

	base := fake.vpcService("rg-e2e")

	resources, err := base.WithLoadBalancerVPCs().ListVPCResources(context.Background())

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(resources).To(Equal([]Resource{
		{Kind: KindLoadBalancer, ID: "lb-lane", Name: "kube-" + laneClusterID + "-ab12", VPCID: testVPCID},
		{Kind: KindLoadBalancer, ID: "lb-unknown", Name: "on-unlisted-subnets"},
	}))
	for _, r := range fake.recorded() {
		if r.Path == subnetsPath {
			g.Expect(r.Query).NotTo(ContainSubstring("resource_group"), "the subnet listing must not be filtered by resource group")
		}
	}
	g.Expect(fake.requestCount(subnetsPath)).To(Equal(2))

	// WithLoadBalancerVPCs returns a copy: the service it came from still lists no subnets and leaves VPCID empty.
	resources, err = base.ListVPCResources(context.Background())

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(resources).To(Equal([]Resource{
		{Kind: KindLoadBalancer, ID: "lb-lane", Name: "kube-" + laneClusterID + "-ab12"},
		{Kind: KindLoadBalancer, ID: "lb-unknown", Name: "on-unlisted-subnets"},
	}))
	g.Expect(fake.requestCount(subnetsPath)).To(Equal(2), "no further subnet request")
}

func TestLoadBalancerVPC(t *testing.T) {
	subnetVPCs := map[string]string{"sn-a1": "vpc-a", "sn-a2": "vpc-a", "sn-b1": "vpc-b", "sn-novpc": ""}
	testCases := []struct {
		name    string
		subnets []string
		want    string
	}{
		{name: "no subnets", want: ""},
		{name: "one known subnet", subnets: []string{"sn-a1"}, want: "vpc-a"},
		{name: "two subnets of one VPC", subnets: []string{"sn-a1", "sn-a2"}, want: "vpc-a"},
		{name: "an unknown subnet next to a known one", subnets: []string{"sn-gone", "sn-a2"}, want: "vpc-a"},
		{name: "only unknown subnets", subnets: []string{"sn-gone"}, want: ""},
		{name: "a subnet listed without a VPC", subnets: []string{"sn-novpc"}, want: ""},
		{name: "subnets of two VPCs are not guessed", subnets: []string{"sn-a1", "sn-b1"}, want: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			refs := make([]vpcv1.SubnetReference, 0, len(tc.subnets))
			for _, id := range tc.subnets {
				refs = append(refs, vpcv1.SubnetReference{ID: core.StringPtr(id)})
			}
			g.Expect(loadBalancerVPC(refs, subnetVPCs)).To(Equal(tc.want))
		})
	}
}

func TestVPCServiceResolveVPCID(t *testing.T) {
	testCases := []struct {
		name    string
		vpcs    []map[string]any
		status  int
		vpcName string
		want    string
		wantErr string
	}{
		{
			name: "the named VPC, whatever its resource group",
			vpcs: []map[string]any{
				vpcItem(otherVPCID, "other-run-vpc", "rg-e2e", ""),
				vpcItem(testVPCID, "lane-vpc", "rg-network", ""),
			},
			vpcName: "lane-vpc",
			want:    testVPCID,
		},
		{name: "no such VPC", vpcs: []map[string]any{vpcItem(otherVPCID, "other-run-vpc", "rg-e2e", "")}, vpcName: "lane-vpc",
			wantErr: `no VPC of the region is named "lane-vpc"; set IBMCLOUD_VPC_NAME`},
		{
			name: "a name that matches two VPCs is not guessed",
			vpcs: []map[string]any{
				vpcItem(testVPCID, "lane-vpc", "rg-e2e", ""),
				vpcItem(otherVPCID, "lane-vpc", "rg-e2e", ""),
			},
			vpcName: "lane-vpc",
			wantErr: "2 VPCs are named \"lane-vpc\" (IDs " + testVPCID + ", " + otherVPCID + ")",
		},
		{name: "a VPC listed without an ID", vpcs: []map[string]any{{"name": "lane-vpc"}}, vpcName: "lane-vpc", wantErr: "without an ID"},
		{name: "the listing fails", status: http.StatusForbidden, vpcName: "lane-vpc", wantErr: `resolving the e2e VPC "lane-vpc": listing VPCs`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)
			fake.pageSize = 1
			fake.vpc["vpcs"] = tc.vpcs
			fake.vpcCollectionStatus["vpcs"] = tc.status

			got, err := fake.vpcService("rg-e2e").ResolveVPCID(context.Background(), tc.vpcName)

			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestVPCServiceResolveVPCIDEmptyNameSendsNoRequest(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	_, err := fake.vpcService("").ResolveVPCID(context.Background(), "")
	g.Expect(err).To(MatchError(ContainSubstring("IBMCLOUD_VPC_NAME is empty")))
	g.Expect(fake.recorded()).To(BeEmpty())
}
