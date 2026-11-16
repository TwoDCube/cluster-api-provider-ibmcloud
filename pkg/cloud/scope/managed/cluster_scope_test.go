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
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"go.uber.org/mock/gomock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	rmmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"
	vpcmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc/mock"

	. "github.com/onsi/gomega"
)

// The InfraCluster scope tests drive ReconcileNetwork against gomock VPC and Resource Manager clients. gomock fails a
// test on any call it does not expect, which also proves that the scope never calls GetResourceGroupByName or any
// mutating VPC method.

const (
	testResourceGroupID = "0123456789abcdef0123456789abcdef"
	testVPCID           = "r006-test-vpc"
	testVPCName         = "dev-vpc"
)

// stubBuilder is a ClientBuilder that returns fixed clients, or the configured build errors, and records
// invalidations.
type stubBuilder struct {
	vpc                vpc.Vpc
	resourceManager    resourcemanager.ResourceManager
	vpcErr             error
	resourceManagerErr error
	invalidated        []credentials.IdentityKey
}

// KubernetesService implements ClientBuilder; the InfraCluster scope never asks for it.
func (b *stubBuilder) KubernetesService(context.Context, credentials.Identity, string) (kubernetesservice.KubernetesService, error) {
	return nil, errors.New("stubBuilder: the InfraCluster scope must not build an IKS client")
}

// VPC implements ClientBuilder.
func (b *stubBuilder) VPC(context.Context, credentials.Identity, string) (vpc.Vpc, error) {
	if b.vpcErr != nil {
		return nil, b.vpcErr
	}
	return b.vpc, nil
}

// ResourceManager implements ClientBuilder.
func (b *stubBuilder) ResourceManager(context.Context, credentials.Identity) (resourcemanager.ResourceManager, error) {
	if b.resourceManagerErr != nil {
		return nil, b.resourceManagerErr
	}
	return b.resourceManager, nil
}

// InvalidateAuthenticator implements ClientBuilder.
func (b *stubBuilder) InvalidateAuthenticator(key credentials.IdentityKey) {
	b.invalidated = append(b.invalidated, key)
}

// recordingResolver is a credentials.Resolver that records every holder it is asked about.
type recordingResolver struct {
	holders []client.Object
	err     error
	// identity is what Resolve returns; the zero value means the controller identity.
	identity credentials.Identity
}

// Resolve implements credentials.Resolver.
func (r *recordingResolver) Resolve(_ context.Context, holder client.Object) (credentials.Identity, error) {
	r.holders = append(r.holders, holder)
	if r.err != nil {
		return credentials.Identity{}, r.err
	}
	if r.identity.Key.Kind != "" {
		return r.identity, nil
	}
	return credentials.Identity{Key: credentials.ControllerKey(), Description: credentials.ControllerDescription}, nil
}

// failingResolver and failingBuilder fail the test when any of their methods is called.
type failingResolver struct{ t *testing.T }

// Resolve implements credentials.Resolver.
func (r failingResolver) Resolve(context.Context, client.Object) (credentials.Identity, error) {
	r.t.Errorf("Resolver.Resolve called, but this path must not resolve an identity")
	return credentials.Identity{}, errors.New("unexpected Resolve")
}

type failingBuilder struct{ t *testing.T }

// KubernetesService implements ClientBuilder.
func (b failingBuilder) KubernetesService(context.Context, credentials.Identity, string) (kubernetesservice.KubernetesService, error) {
	b.t.Errorf("ClientBuilder.KubernetesService called, but this path must not build a client")
	return nil, errors.New("unexpected KubernetesService")
}

// VPC implements ClientBuilder.
func (b failingBuilder) VPC(context.Context, credentials.Identity, string) (vpc.Vpc, error) {
	b.t.Errorf("ClientBuilder.VPC called, but this path must not build a client")
	return nil, errors.New("unexpected VPC")
}

// ResourceManager implements ClientBuilder.
func (b failingBuilder) ResourceManager(context.Context, credentials.Identity) (resourcemanager.ResourceManager, error) {
	b.t.Errorf("ClientBuilder.ResourceManager called, but this path must not build a client")
	return nil, errors.New("unexpected ResourceManager")
}

// InvalidateAuthenticator implements ClientBuilder.
func (b failingBuilder) InvalidateAuthenticator(credentials.IdentityKey) {
	b.t.Errorf("ClientBuilder.InvalidateAuthenticator called, but this path must not call IBM")
}

// testInfra returns an IBMCloudManagedCluster with the VPC and two subnets given by ID.
func testInfra() *infrav1.IBMCloudManagedCluster {
	return &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dev", Generation: 1},
		Spec: infrav1.IBMCloudManagedClusterSpec{
			Region:        testRegion,
			ResourceGroup: infrav1.ResourceGroupReference{ID: testResourceGroupID},
			VPC:           infrav1.VPCSource{Type: infrav1.SourceTypeReference, Reference: infrav1.ResourceIdentifier{ID: testVPCID}},
			Zones: []infrav1.ZoneSubnet{
				{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{ID: "sn-1"}},
				{Name: "us-south-2", Subnet: infrav1.ResourceIdentifier{ID: "sn-2"}},
			},
		},
	}
}

// testSubnet returns a VPC subnet.
//
// Parameters:
//   - id, name name the subnet.
//   - vpcID is the VPC it belongs to.
//   - zone is its zone.
//   - gateway tells whether it has a public gateway.
//   - free is its free IPv4 address count.
func testSubnet(id, name, vpcID, zone string, gateway bool, free int64) *vpcv1.Subnet {
	subnet := &vpcv1.Subnet{
		ID:                        ptr.To(id),
		Name:                      ptr.To(name),
		Ipv4CIDRBlock:             ptr.To("10.240.0.0/24"),
		AvailableIpv4AddressCount: ptr.To(free),
		VPC:                       &vpcv1.VPCReference{ID: ptr.To(vpcID), Name: ptr.To(vpcID + "-name")},
		Zone:                      &vpcv1.ZoneReference{Name: ptr.To(zone)},
	}
	if gateway {
		subnet.PublicGateway = &vpcv1.PublicGatewayReference{ID: ptr.To("pgw-" + id)}
	}
	return subnet
}

// testVPC returns the VPC of the tests.
func testVPC() *vpcv1.VPC {
	return &vpcv1.VPC{ID: ptr.To(testVPCID), Name: ptr.To(testVPCName)}
}

// status returns a DetailedResponse with the HTTP status code.
func status(code int) *core.DetailedResponse {
	return &core.DetailedResponse{StatusCode: code}
}

// scopeFixture is one ClusterScope over gomock clients.
type scopeFixture struct {
	scope    *ClusterScope
	vpc      *vpcmock.MockVpc
	rm       *rmmock.MockResourceManager
	builder  *stubBuilder
	resolver *recordingResolver
}

// newScopeFixture returns a ClusterScope for infra whose clients are gomock mocks.
func newScopeFixture(t *testing.T, infra *infrav1.IBMCloudManagedCluster) *scopeFixture {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	fixture := &scopeFixture{vpc: vpcmock.NewMockVpc(mockCtrl), rm: rmmock.NewMockResourceManager(mockCtrl), resolver: &recordingResolver{}}
	fixture.builder = &stubBuilder{vpc: fixture.vpc, resourceManager: fixture.rm}
	params := &ScopeParams{Resolver: fixture.resolver, ClientBuilder: fixture.builder}
	scope, err := NewClusterScope(params, &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dev"}}, infra)
	if err != nil {
		t.Fatalf("NewClusterScope: %v", err)
	}
	fixture.scope = scope
	return fixture
}

// expectResourceGroup expects one GetResourceGroup by the test ID and answers response and err.
func (f *scopeFixture) expectResourceGroup(response *core.DetailedResponse, err error) {
	var group *resourcemanagerv2.ResourceGroup
	if err == nil {
		group = &resourcemanagerv2.ResourceGroup{ID: ptr.To(testResourceGroupID)}
	}
	f.rm.EXPECT().GetResourceGroup(gomock.Cond(func(o *resourcemanagerv2.GetResourceGroupOptions) bool {
		return ptr.Deref(o.ID, "") == testResourceGroupID
	})).Return(group, response, err)
}

// expectVPCByID expects one GetVPC of the test VPC and answers found, response and err.
func (f *scopeFixture) expectVPCByID(found *vpcv1.VPC, response *core.DetailedResponse, err error) {
	f.vpc.EXPECT().GetVPC(gomock.Cond(func(o *vpcv1.GetVPCOptions) bool { return ptr.Deref(o.ID, "") == testVPCID })).Return(found, response, err)
}

// expectSubnet expects one GetSubnet of id and answers subnet, response and err.
func (f *scopeFixture) expectSubnet(id string, subnet *vpcv1.Subnet, response *core.DetailedResponse, err error) {
	f.vpc.EXPECT().GetSubnet(gomock.Cond(func(o *vpcv1.GetSubnetOptions) bool { return ptr.Deref(o.ID, "") == id })).Return(subnet, response, err)
}

// networkReady returns the NetworkReady condition of infra.
func networkReady(infra *infrav1.IBMCloudManagedCluster) *metav1.Condition {
	return conditions.Get(infra, infrav1.IBMCloudManagedClusterNetworkReadyCondition)
}

func TestReconcileNetworkPublishesTheNetwork(t *testing.T) {
	g := NewWithT(t)
	infra := testInfra()
	f := newScopeFixture(t, infra)
	f.expectResourceGroup(status(http.StatusOK), nil)
	f.expectVPCByID(testVPC(), status(http.StatusOK), nil)
	f.expectSubnet("sn-1", testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250), status(http.StatusOK), nil)
	f.expectSubnet("sn-2", testSubnet("sn-2", "dev-sn-2", testVPCID, "us-south-2", false, 12), status(http.StatusOK), nil)

	outcome := f.scope.ReconcileNetwork(context.Background())

	g.Expect(outcome).To(Equal(Done()))
	g.Expect(infra.Status.VPC).To(Equal(infrav1.ResolvedResource{ID: testVPCID, Name: testVPCName}))
	g.Expect(infra.Status.Zones).To(Equal([]infrav1.ResolvedZone{
		{
			Name: "us-south-1", Subnet: infrav1.ResolvedResource{ID: "sn-1", Name: "dev-sn-1"}, IPv4CIDRBlock: "10.240.0.0/24",
			PublicGateway: infrav1.PublicGatewayStateAttached, AvailableIPv4Addresses: ptr.To[int32](250),
		},
		{
			Name: "us-south-2", Subnet: infrav1.ResolvedResource{ID: "sn-2", Name: "dev-sn-2"}, IPv4CIDRBlock: "10.240.0.0/24",
			PublicGateway: infrav1.PublicGatewayStateNotAttached, AvailableIPv4Addresses: ptr.To[int32](12),
		},
	}))
	g.Expect(infra.Status.FailureDomains).To(Equal([]infrav1.FailureDomain{{Name: "us-south-1"}, {Name: "us-south-2"}}))
	g.Expect(infra.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
	condition := networkReady(infra)
	g.Expect(condition).NotTo(BeNil())
	g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(condition.Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadyReason))
	g.Expect(condition.ObservedGeneration).To(Equal(int64(1)))
	// Informational only: a missing public gateway and few free addresses never make NetworkReady False.
	g.Expect(condition.Message).To(Equal("subnet dev-sn-2 (sn-2) (zone us-south-2) has no public gateway, so its workers have no public " +
		"egress, which the ROKS web console and OperatorHub need; subnet dev-sn-2 (sn-2) (zone us-south-2) has 12 free IPv4 addresses " +
		"(fewer than 16), so new workers may get none"))
}

func TestReconcileNetworkResolvesByName(t *testing.T) {
	g := NewWithT(t)
	infra := testInfra()
	infra.Spec.VPC.Reference = infrav1.ResourceIdentifier{Name: testVPCName}
	infra.Spec.Zones = []infrav1.ZoneSubnet{
		{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{Name: "dev-sn-1"}},
		{Name: "us-south-2", Subnet: infrav1.ResourceIdentifier{Name: "dev-sn-2"}},
	}
	f := newScopeFixture(t, infra)
	f.expectResourceGroup(status(http.StatusOK), nil)
	// Two pages of VPCs; the name is on the second page, next to a VPC of another name.
	nextPage := &vpcv1.PageLink{Href: ptr.To("https://us-south.iaas.cloud.ibm.com/v1/vpcs?start=page2&limit=100")}
	gomock.InOrder(
		f.vpc.EXPECT().ListVpcs(gomock.Cond(func(o *vpcv1.ListVpcsOptions) bool { return o.Start == nil && ptr.Deref(o.Limit, 0) == 100 })).
			Return(&vpcv1.VPCCollection{Vpcs: []vpcv1.VPC{{ID: ptr.To("r006-other"), Name: ptr.To("other")}}, Next: nextPage}, status(http.StatusOK), nil),
		f.vpc.EXPECT().ListVpcs(gomock.Cond(func(o *vpcv1.ListVpcsOptions) bool { return ptr.Deref(o.Start, "") == "page2" })).
			Return(&vpcv1.VPCCollection{Vpcs: []vpcv1.VPC{*testVPC()}}, status(http.StatusOK), nil),
	)
	// The subnets are listed once, inside the VPC only.
	f.vpc.EXPECT().ListSubnets(gomock.Cond(func(o *vpcv1.ListSubnetsOptions) bool { return ptr.Deref(o.VPCID, "") == testVPCID })).
		Return(&vpcv1.SubnetCollection{Subnets: []vpcv1.Subnet{
			*testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250),
			*testSubnet("sn-2", "dev-sn-2", testVPCID, "us-south-2", true, 250),
		}}, status(http.StatusOK), nil).Times(1)

	outcome := f.scope.ReconcileNetwork(context.Background())

	g.Expect(outcome).To(Equal(Done()))
	g.Expect(infra.Status.VPC.ID).To(Equal(testVPCID))
	g.Expect(infra.Status.Zones).To(HaveLen(2))
	g.Expect(infra.Status.Zones[1].Subnet).To(Equal(infrav1.ResolvedResource{ID: "sn-2", Name: "dev-sn-2"}))
	g.Expect(networkReady(infra).Message).To(BeEmpty())
}

func TestReconcileNetworkReportsSpecificProblems(t *testing.T) {
	type expectFunc func(f *scopeFixture)
	okRG := func(f *scopeFixture) { f.expectResourceGroup(status(http.StatusOK), nil) }
	okVPC := func(f *scopeFixture) { f.expectVPCByID(testVPC(), status(http.StatusOK), nil) }
	notFound := errors.New("Not Found")

	testCases := []struct {
		name        string
		mutate      func(*infrav1.IBMCloudManagedCluster)
		expect      []expectFunc
		wantReason  string
		wantMessage string
	}{
		{
			name:        "resource group not found",
			expect:      []expectFunc{func(f *scopeFixture) { f.expectResourceGroup(status(http.StatusNotFound), notFound) }},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadyResourceGroupNotFoundReason,
			wantMessage: "resource group 0123456789abcdef0123456789abcdef (spec.resourceGroup.id of IBMCloudManagedCluster/dev) does not exist or the CAPIBM controller identity cannot see it",
		},
		{
			name: "VPC by ID not found",
			expect: []expectFunc{okRG, func(f *scopeFixture) {
				f.expectVPCByID(nil, status(http.StatusNotFound), notFound)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			wantMessage: "VPC r006-test-vpc (spec.vpc.reference.id of IBMCloudManagedCluster/dev) does not exist in region us-south",
		},
		{
			name: "VPC by name not found",
			mutate: func(i *infrav1.IBMCloudManagedCluster) {
				i.Spec.VPC.Reference = infrav1.ResourceIdentifier{Name: "missing"}
			},
			expect: []expectFunc{okRG, func(f *scopeFixture) {
				f.vpc.EXPECT().ListVpcs(gomock.Any()).Return(&vpcv1.VPCCollection{Vpcs: []vpcv1.VPC{*testVPC()}}, status(http.StatusOK), nil)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			wantMessage: "no VPC named missing exists in region us-south (spec.vpc.reference.name of IBMCloudManagedCluster/dev)",
		},
		{
			name: "VPC name matching two VPCs is refused, not guessed",
			mutate: func(i *infrav1.IBMCloudManagedCluster) {
				i.Spec.VPC.Reference = infrav1.ResourceIdentifier{Name: testVPCName}
			},
			expect: []expectFunc{okRG, func(f *scopeFixture) {
				twin := vpcv1.VPC{ID: ptr.To("r006-twin"), Name: ptr.To(testVPCName)}
				f.vpc.EXPECT().ListVpcs(gomock.Any()).Return(&vpcv1.VPCCollection{Vpcs: []vpcv1.VPC{*testVPC(), twin}}, status(http.StatusOK), nil)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			wantMessage: "2 VPCs are named dev-vpc in region us-south; set spec.vpc.reference.id of IBMCloudManagedCluster/dev",
		},
		{
			name:        "VPC reference without id or name",
			mutate:      func(i *infrav1.IBMCloudManagedCluster) { i.Spec.VPC.Reference = infrav1.ResourceIdentifier{} },
			expect:      []expectFunc{okRG},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			wantMessage: "names neither an id nor a name",
		},
		{
			name: "subnet by ID not found",
			expect: []expectFunc{okRG, okVPC, func(f *scopeFixture) {
				f.expectSubnet("sn-1", nil, status(http.StatusNotFound), notFound)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason,
			wantMessage: "subnet sn-1 (zone us-south-1 of IBMCloudManagedCluster/dev) does not exist in region us-south",
		},
		{
			name: "subnet of another VPC",
			expect: []expectFunc{okRG, okVPC, func(f *scopeFixture) {
				f.expectSubnet("sn-1", testSubnet("sn-1", "dev-sn-1", "r006-other", "us-south-1", true, 250), status(http.StatusOK), nil)
			}},
			wantReason: infrav1.IBMCloudManagedClusterNetworkReadySubnetNotInVPCReason,
			wantMessage: "subnet dev-sn-1 (sn-1) is in VPC r006-other-name (r006-other), but IBMCloudManagedCluster/dev uses VPC " +
				"dev-vpc (r006-test-vpc); set a subnet of VPC dev-vpc (r006-test-vpc) for zone us-south-1",
		},
		{
			name: "subnet in another zone",
			expect: []expectFunc{okRG, okVPC, func(f *scopeFixture) {
				f.expectSubnet("sn-1", testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250), status(http.StatusOK), nil)
				f.expectSubnet("sn-2", testSubnet("sn-2", "dev-sn-2", testVPCID, "us-south-1", true, 250), status(http.StatusOK), nil)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetZoneMismatchReason,
			wantMessage: "subnet dev-sn-2 (sn-2) is in zone us-south-1, but IBMCloudManagedCluster/dev lists it for us-south-2",
		},
		{
			name: "subnet by name not in the VPC",
			mutate: func(i *infrav1.IBMCloudManagedCluster) {
				i.Spec.Zones = []infrav1.ZoneSubnet{{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{Name: "elsewhere"}}}
			},
			expect: []expectFunc{okRG, okVPC, func(f *scopeFixture) {
				f.vpc.EXPECT().ListSubnets(gomock.Any()).Return(&vpcv1.SubnetCollection{Subnets: []vpcv1.Subnet{
					*testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250),
				}}, status(http.StatusOK), nil)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason,
			wantMessage: "no subnet named elsewhere exists in VPC dev-vpc (r006-test-vpc) (zone us-south-1 of IBMCloudManagedCluster/dev)",
		},
		{
			name: "subnet name matching two subnets is refused, not guessed",
			mutate: func(i *infrav1.IBMCloudManagedCluster) {
				i.Spec.Zones = []infrav1.ZoneSubnet{{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{Name: "dev-sn-1"}}}
			},
			expect: []expectFunc{okRG, okVPC, func(f *scopeFixture) {
				f.vpc.EXPECT().ListSubnets(gomock.Any()).Return(&vpcv1.SubnetCollection{Subnets: []vpcv1.Subnet{
					*testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250),
					*testSubnet("sn-9", "dev-sn-1", testVPCID, "us-south-1", true, 250),
				}}, status(http.StatusOK), nil)
			}},
			wantReason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetNameAmbiguousReason,
			wantMessage: "2 subnets of VPC dev-vpc (r006-test-vpc) are named dev-sn-1; set the subnet id of zone us-south-1 in IBMCloudManagedCluster/dev",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			infra := testInfra()
			if tc.mutate != nil {
				tc.mutate(infra)
			}
			f := newScopeFixture(t, infra)
			for _, expect := range tc.expect {
				expect(f)
			}

			outcome := f.scope.ReconcileNetwork(context.Background())

			g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
			g.Expect(outcome.Reason).To(Equal(tc.wantReason))
			condition := networkReady(infra)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(condition.Reason).To(Equal(tc.wantReason))
			g.Expect(condition.Message).To(ContainSubstring(tc.wantMessage))
			g.Expect(outcome.Message).To(Equal(condition.Message))
			// Nothing is published and provisioned is not set by a failed validation.
			g.Expect(infra.Status.Zones).To(BeEmpty())
			g.Expect(infra.Status.Initialization.Provisioned).To(BeNil())
		})
	}
}

func TestReconcileNetworkClassifiesIBMErrors(t *testing.T) {
	failure := errors.New("IBM Cloud error")
	testCases := []struct {
		name            string
		response        *core.DetailedResponse
		readyBefore     bool
		wantKind        OutcomeKind
		wantInterval    Interval
		wantCondition   metav1.ConditionStatus
		wantMessage     string
		wantInvalidated bool
	}{
		{
			name:            "401 invalidates the identity's authenticator and rechecks at the steady interval",
			response:        status(http.StatusUnauthorized),
			wantKind:        OutcomeNotDone,
			wantInterval:    IntervalSteadyState,
			wantCondition:   metav1.ConditionFalse,
			wantMessage:     "IBM Cloud rejected the credentials of the CAPIBM controller identity (HTTP 401) while getting resource group",
			wantInvalidated: true,
		},
		{
			name:          "403 needs a human and names the identity and the IAM role of the resource group",
			response:      status(http.StatusForbidden),
			wantKind:      OutcomeTerminal,
			wantCondition: metav1.ConditionFalse,
			wantMessage: "IBM Cloud refused the credentials of the CAPIBM controller identity (HTTP 403) while getting resource group " +
				"0123456789abcdef0123456789abcdef; grant them the Viewer role on the resource group (the IAM roles of the managed Kubernetes book)",
		},
		{
			name:          "429 backs off and reports when NetworkReady is not True",
			response:      status(http.StatusTooManyRequests),
			wantKind:      OutcomeRateLimited,
			wantCondition: metav1.ConditionFalse,
			wantMessage:   "IBM Cloud rate limited CAPIBM (HTTP 429)",
		},
		{
			name:          "429 leaves a True NetworkReady as it is",
			response:      status(http.StatusTooManyRequests),
			readyBefore:   true,
			wantKind:      OutcomeRateLimited,
			wantCondition: metav1.ConditionTrue,
		},
		{
			name:          "5xx is transient and reported when NetworkReady is not True",
			response:      status(http.StatusServiceUnavailable),
			wantKind:      OutcomeTransient,
			wantCondition: metav1.ConditionFalse,
			wantMessage:   "IBM Cloud answered HTTP 503 while getting resource group 0123456789abcdef0123456789abcdef; CAPIBM retries",
		},
		{
			name:          "5xx leaves a True NetworkReady as it is",
			response:      status(http.StatusInternalServerError),
			readyBefore:   true,
			wantKind:      OutcomeTransient,
			wantCondition: metav1.ConditionTrue,
		},
		{
			name:          "no answer at all is transient",
			response:      nil,
			wantKind:      OutcomeTransient,
			wantCondition: metav1.ConditionFalse,
			wantMessage:   "CAPIBM could not reach IBM Cloud while getting resource group",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			infra := testInfra()
			if tc.readyBefore {
				conditions.Set(infra, metav1.Condition{
					Type: infrav1.IBMCloudManagedClusterNetworkReadyCondition, Status: metav1.ConditionTrue, Reason: infrav1.IBMCloudManagedClusterNetworkReadyReason,
				})
			}
			f := newScopeFixture(t, infra)
			f.expectResourceGroup(tc.response, failure)

			outcome := f.scope.ReconcileNetwork(context.Background())

			g.Expect(outcome.Kind).To(Equal(tc.wantKind))
			g.Expect(outcome.Interval).To(Equal(tc.wantInterval))
			if tc.wantKind == OutcomeTransient {
				g.Expect(outcome.Err).To(MatchError(failure))
			}
			condition := networkReady(infra)
			g.Expect(condition.Status).To(Equal(tc.wantCondition))
			if tc.wantCondition == metav1.ConditionFalse {
				g.Expect(condition.Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason))
				g.Expect(condition.Message).To(ContainSubstring(tc.wantMessage))
				g.Expect(condition.Message).NotTo(ContainSubstring(failure.Error()), "no IBM free text in status")
			}
			if tc.wantInvalidated {
				g.Expect(f.builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
			} else {
				g.Expect(f.builder.invalidated).To(BeEmpty())
			}
		})
	}
}

func TestReconcileNetworkProvisionedIsSticky(t *testing.T) {
	g := NewWithT(t)
	infra := testInfra()
	f := newScopeFixture(t, infra)
	f.expectResourceGroup(status(http.StatusOK), nil)
	f.expectVPCByID(testVPC(), status(http.StatusOK), nil)
	f.expectSubnet("sn-1", testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250), status(http.StatusOK), nil)
	f.expectSubnet("sn-2", testSubnet("sn-2", "dev-sn-2", testVPCID, "us-south-2", true, 250), status(http.StatusOK), nil)
	g.Expect(f.scope.ReconcileNetwork(context.Background())).To(Equal(Done()))
	published := infra.Status.DeepCopy()

	// Thirty minutes later a subnet is gone: NetworkReady flips, provisioned and the last published status stay.
	later := newScopeFixture(t, infra)
	later.expectResourceGroup(status(http.StatusOK), nil)
	later.expectVPCByID(testVPC(), status(http.StatusOK), nil)
	later.expectSubnet("sn-1", nil, status(http.StatusNotFound), errors.New("Not Found"))
	outcome := later.scope.ReconcileNetwork(context.Background())

	g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
	g.Expect(networkReady(infra).Status).To(Equal(metav1.ConditionFalse))
	g.Expect(networkReady(infra).Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason))
	g.Expect(infra.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
	g.Expect(infra.Status.Zones).To(Equal(published.Zones))
	g.Expect(infra.Status.VPC).To(Equal(published.VPC))
}

func TestReconcileNetworkListingFailures(t *testing.T) {
	t.Run("a listing whose pages never end is cut off", func(t *testing.T) {
		g := NewWithT(t)
		infra := testInfra()
		infra.Spec.VPC.Reference = infrav1.ResourceIdentifier{Name: testVPCName}
		f := newScopeFixture(t, infra)
		f.expectResourceGroup(status(http.StatusOK), nil)
		endless := &vpcv1.VPCCollection{Next: &vpcv1.PageLink{Href: ptr.To("https://us-south.iaas.cloud.ibm.com/v1/vpcs?start=again")}}
		f.vpc.EXPECT().ListVpcs(gomock.Any()).Return(endless, status(http.StatusOK), nil).Times(maxListPages)

		outcome := f.scope.ReconcileNetwork(context.Background())

		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(networkReady(infra).Message).To(ContainSubstring("more than 50 pages while listing the VPCs of region us-south"))
	})

	t.Run("an unreadable next-page link is transient", func(t *testing.T) {
		g := NewWithT(t)
		infra := testInfra()
		infra.Spec.Zones = []infrav1.ZoneSubnet{{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{Name: "dev-sn-1"}}}
		f := newScopeFixture(t, infra)
		f.expectResourceGroup(status(http.StatusOK), nil)
		f.expectVPCByID(testVPC(), status(http.StatusOK), nil)
		f.vpc.EXPECT().ListSubnets(gomock.Any()).
			Return(&vpcv1.SubnetCollection{Next: &vpcv1.PageLink{Href: ptr.To("::not a url")}}, status(http.StatusOK), nil)

		outcome := f.scope.ReconcileNetwork(context.Background())

		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(networkReady(infra).Message).To(ContainSubstring("while listing the subnets of VPC dev-vpc (r006-test-vpc)"))
	})

	t.Run("a listing error is classified like any call", func(t *testing.T) {
		g := NewWithT(t)
		infra := testInfra()
		infra.Spec.VPC.Reference = infrav1.ResourceIdentifier{Name: testVPCName}
		f := newScopeFixture(t, infra)
		f.expectResourceGroup(status(http.StatusOK), nil)
		f.vpc.EXPECT().ListVpcs(gomock.Any()).Return(nil, status(http.StatusForbidden), errors.New("Forbidden"))

		outcome := f.scope.ReconcileNetwork(context.Background())

		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		// A VPC call asks for the VPC Infrastructure Services role, not the resource group's.
		g.Expect(networkReady(infra).Message).To(Equal("IBM Cloud refused the credentials of the CAPIBM controller identity (HTTP 403) " +
			"while listing the VPCs of region us-south; grant them the Administrator platform role on VPC Infrastructure Services " +
			"(the IAM roles of the managed Kubernetes book)"))
	})
}

func TestReconcileNetworkCredentialFailures(t *testing.T) {
	t.Run("a resolver error is a condition and no IBM call", func(t *testing.T) {
		g := NewWithT(t)
		infra := testInfra()
		f := newScopeFixture(t, infra)
		f.resolver.err = errors.New("holder lost")

		outcome := f.scope.ReconcileNetwork(context.Background())

		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(outcome.Err).To(MatchError(ContainSubstring("resolving the IBM Cloud identity of IBMCloudManagedCluster/dev: holder lost")))
		g.Expect(networkReady(infra).Status).To(Equal(metav1.ConditionFalse))
		g.Expect(networkReady(infra).Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason))
		// No identity resolved, so none is named; the resolver's text stays in the log.
		g.Expect(networkReady(infra).Message).To(Equal(
			"CAPIBM cannot resolve the IBM Cloud credentials of this IBMCloudManagedCluster; the controller log names the cause"))
	})

	buildFailures := []struct {
		name    string
		vpcErr  error
		rmErr   error
		wantErr string
	}{
		{
			name:    "a VPC client build error names the identity",
			vpcErr:  errors.New("credentials file is unreadable"),
			wantErr: "building the VPC client of the test identity a for region us-south: credentials file is unreadable",
		},
		{
			name:    "a Resource Manager client build error names the identity",
			rmErr:   errors.New("credentials file is unreadable"),
			wantErr: "building the Resource Manager client of the test identity a: credentials file is unreadable",
		},
	}
	for _, tc := range buildFailures {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			infra := testInfra()
			f := newScopeFixture(t, infra)
			// A non-controller identity proves that the message is built from Identity.Description.
			f.resolver.identity = sourcedIdentity("a", "", &countingSource{prefix: "a"})
			f.builder.vpcErr = tc.vpcErr
			f.builder.resourceManagerErr = tc.rmErr

			outcome := f.scope.ReconcileNetwork(context.Background())

			g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
			g.Expect(outcome.Err).To(MatchError(tc.wantErr))
			condition := networkReady(infra)
			g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(condition.Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason))
			g.Expect(condition.Message).To(Equal(
				"CAPIBM cannot build the VPC and Resource Manager clients of the test identity a; the controller log names the cause"))
			g.Expect(condition.Message).NotTo(ContainSubstring("credentials file is unreadable"), "no error text in status")
			g.Expect(f.scope.clients).To(BeNil(), "a failed build is not memoized")
		})
	}

	t.Run("the clients are built once per reconcile", func(t *testing.T) {
		g := NewWithT(t)
		f := newScopeFixture(t, testInfra())
		first, err := f.scope.ibm(context.Background())
		g.Expect(err).NotTo(HaveOccurred())
		second, err := f.scope.ibm(context.Background())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(second).To(BeIdenticalTo(first))
		g.Expect(f.resolver.holders).To(HaveLen(1))
	})
}

func TestClusterScopeIdentitySeams(t *testing.T) {
	t.Run("the identity holder is the IBMCloudManagedCluster itself", func(t *testing.T) {
		g := NewWithT(t)
		infra := testInfra()
		f := newScopeFixture(t, infra)
		f.expectResourceGroup(status(http.StatusOK), nil)
		f.expectVPCByID(testVPC(), status(http.StatusOK), nil)
		f.expectSubnet("sn-1", testSubnet("sn-1", "dev-sn-1", testVPCID, "us-south-1", true, 250), status(http.StatusOK), nil)
		f.expectSubnet("sn-2", testSubnet("sn-2", "dev-sn-2", testVPCID, "us-south-2", true, 250), status(http.StatusOK), nil)

		g.Expect(f.scope.ReconcileNetwork(context.Background())).To(Equal(Done()))

		// The holder is the IBMCloudManagedCluster itself, never the Cluster.
		g.Expect(f.resolver.holders).To(HaveLen(1))
		g.Expect(f.resolver.holders[0]).To(BeIdenticalTo(infra))
	})

	t.Run("the scope constructor resolves no identity and builds no client", func(t *testing.T) {
		g := NewWithT(t)
		params := &ScopeParams{Resolver: failingResolver{t: t}, ClientBuilder: failingBuilder{t: t}}
		scope, err := NewClusterScope(params, &clusterv1.Cluster{}, testInfra())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(scope.clients).To(BeNil())
	})
}

func TestNewClusterScopeValidation(t *testing.T) {
	resolver := &recordingResolver{}
	builder := &stubBuilder{}
	testCases := []struct {
		name    string
		params  *ScopeParams
		cluster *clusterv1.Cluster
		infra   *infrav1.IBMCloudManagedCluster
		wantErr string
	}{
		{name: "nil params", params: nil, cluster: &clusterv1.Cluster{}, infra: testInfra(), wantErr: "ClusterScope needs ScopeParams, got nil"},
		{
			name: "missing resolver and builder are both named", params: &ScopeParams{}, cluster: &clusterv1.Cluster{}, infra: testInfra(),
			wantErr: "ClusterScope needs ScopeParams.Resolver, ScopeParams.ClientBuilder",
		},
		{
			name: "typed nil builder is missing", params: &ScopeParams{Resolver: resolver, ClientBuilder: (*stubBuilder)(nil)},
			cluster: &clusterv1.Cluster{}, infra: testInfra(), wantErr: "ScopeParams.ClientBuilder",
		},
		{
			name: "nil cluster", params: &ScopeParams{Resolver: resolver, ClientBuilder: builder}, cluster: nil, infra: testInfra(),
			wantErr: "needs the owning Cluster",
		},
		{
			name: "nil infra", params: &ScopeParams{Resolver: resolver, ClientBuilder: builder}, cluster: &clusterv1.Cluster{}, infra: nil,
			wantErr: "needs the IBMCloudManagedCluster",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			scope, err := NewClusterScope(tc.params, tc.cluster, tc.infra)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(scope).To(BeNil())
		})
	}
}

func TestScopeParamsValidate(t *testing.T) {
	all := []dependency{depClient, depAPIReader, depRecorder, depResolver, depClientBuilder, depClock, depProber, depVersionsCache}

	t.Run("every unset dependency is named in order", func(t *testing.T) {
		g := NewWithT(t)
		err := (&ScopeParams{}).validate("TestScope", all...)
		g.Expect(err).To(MatchError("managed: TestScope needs ScopeParams.Client, ScopeParams.APIReader, ScopeParams.Recorder, " +
			"ScopeParams.Resolver, ScopeParams.ClientBuilder, ScopeParams.Clock, ScopeParams.Prober, ScopeParams.VersionsCache; " +
			"set them on the reconciler (cmd/main.go creates them once)"))
	})

	t.Run("set dependencies pass and unneeded ones are not checked", func(t *testing.T) {
		g := NewWithT(t)
		params := &ScopeParams{Recorder: record.NewFakeRecorder(1), Resolver: &recordingResolver{}}
		g.Expect(params.validate("TestScope", depRecorder, depResolver)).To(Succeed())
	})

	t.Run("an unknown dependency is a programming error", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect((&ScopeParams{}).validate("TestScope", dependency("Nope"))).To(MatchError(ContainSubstring("unknown ScopeParams dependency Nope")))
	})
}

func TestNetworkLabels(t *testing.T) {
	testCases := []struct {
		name string
		got  string
		want string
	}{
		{name: "name and id", got: labelOf(ptr.To("dev"), ptr.To("r006-1")), want: "dev (r006-1)"},
		{name: "name only", got: labelOf(ptr.To("dev"), nil), want: "dev"},
		{name: "id only", got: labelOf(nil, ptr.To("r006-1")), want: "r006-1"},
		{name: "empty name", got: labelOf(ptr.To(""), ptr.To("r006-1")), want: "r006-1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(tc.got).To(Equal(tc.want))
		})
	}
}

func TestClampInt32(t *testing.T) {
	g := NewWithT(t)
	g.Expect(clampInt32(12)).To(Equal(int32(12)))
	g.Expect(clampInt32(-1)).To(Equal(int32(0)))
	g.Expect(clampInt32(1 << 40)).To(Equal(int32(2147483647)))
}
