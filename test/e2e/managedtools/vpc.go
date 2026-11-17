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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/vpc-go-sdk/vpcv1"
)

// ResourceKind names the kind of an inventoried resource.
type ResourceKind string

const (
	// KindCluster is an IBM Cloud Kubernetes Service cluster.
	KindCluster ResourceKind = "cluster"
	// KindLoadBalancer is a VPC load balancer (IKS creates one per Ingress or LoadBalancer Service).
	KindLoadBalancer ResourceKind = "load balancer"
	// KindEndpointGateway is a VPC virtual private endpoint (VPE) gateway.
	KindEndpointGateway ResourceKind = "VPE gateway"
	// KindSecurityGroup is a VPC security group.
	KindSecurityGroup ResourceKind = "security group"
	// KindVolume is a VPC block storage volume (a PersistentVolume of a cluster, for example).
	KindVolume ResourceKind = "volume"
)

// Resource is one inventoried IBM Cloud resource.
type Resource struct {
	// Kind is the resource kind.
	Kind ResourceKind
	// ID is the IBM ID; together with Kind it identifies the resource across snapshots.
	ID string
	// Name is the resource name.
	Name string
	// VPCID is the ID of the VPC the resource belongs to: the VPC reference of a VPE gateway or security group, and,
	// from a VPCService built with WithLoadBalancerVPCs, the VPC of a load balancer's subnets (the VPC API gives a load
	// balancer no VPC reference). It is "" for clusters and volumes, which belong to no VPC, for every load balancer of
	// a VPCService without WithLoadBalancerVPCs, and for a load balancer whose subnets are not in the subnet listing.
	VPCID string
}

// VPCResourceLister lists the VPC resources a managed cluster may leave behind. VPCService implements it.
type VPCResourceLister interface {
	// ListVPCResources returns the load balancers, VPE gateways, security groups and volumes of the region (in the
	// resource group, when one is set).
	ListVPCResources(ctx context.Context) ([]Resource, error)
}

// VPCService implements VPCResourceLister on the regional VPC API. Create it with NewVPCService.
type VPCService struct {
	// sdk is the generated SDK client, built with the explicit URL, authenticator and HTTP client.
	sdk *vpcv1.VpcV1
	// resourceGroupID, when not empty, keeps only the resources of that resource group.
	resourceGroupID string
	// loadBalancerVPCs, set by WithLoadBalancerVPCs, makes ListVPCResources list the region's subnets and give each
	// load balancer the VPC of its subnets.
	loadBalancerVPCs bool
}

var _ VPCResourceLister = &VPCService{}

// NewVPCService validates opts and builds the client. It makes no request.
//
// It uses vpcv1.NewVpcV1 with the explicit authenticator (never the environment-reading variant) and installs
// opts.HTTPClient; SDK retries stay off.
//
// Parameters:
//   - opts are the regional URL (https://<region>.iaas.cloud.ibm.com/v1), the authenticator, the HTTP client and the
//     optional resource group; see ClientOptions.
func NewVPCService(opts ClientOptions) (*VPCService, error) {
	const service = "VPC"
	if err := opts.validate(service); err != nil {
		return nil, err
	}
	sdk, err := vpcv1.NewVpcV1(&vpcv1.VpcV1Options{URL: opts.URL, Authenticator: opts.Authenticator})
	if err != nil {
		return nil, fmt.Errorf("%s client: building the SDK client: %w", service, err)
	}
	sdk.Service.SetHTTPClient(opts.HTTPClient)
	sdk.Service.SetDefaultHeaders(http.Header{"User-Agent": []string{DefaultUserAgent}})
	return &VPCService{sdk: sdk, resourceGroupID: opts.ResourceGroupID}, nil
}

// WithLoadBalancerVPCs returns a copy of s whose ListVPCResources also gives every load balancer the VPC of its
// subnets (Resource.VPCID), at the cost of listing every subnet of the region on each call. Only a leftover check that
// selects load balancers by VPC needs it (InventoryScope.NeedsLoadBalancerVPCs); without it ListVPCResources makes
// exactly the four requests of the inventoried kinds (and their pages), so the account scope that CI uses pays for no
// listing it would not read. s itself is unchanged.
func (s *VPCService) WithLoadBalancerVPCs() *VPCService {
	attributing := *s
	attributing.loadBalancerVPCs = true
	return &attributing
}

// ListVPCResources implements VPCResourceLister. It follows every page of the four collections. The VPC API filters
// only some collections by resource group, so the filter is applied here, the same way for all four.
//
// A load balancer has no VPC reference, only subnets, so with WithLoadBalancerVPCs the subnets of the region are
// listed first and a load balancer's VPCID is the VPC of its subnets. That listing is not filtered by resource group:
// a load balancer of the resource group may use subnets of another one, and the map only attributes resources, it
// never adds any. Without WithLoadBalancerVPCs no subnet is listed and every load balancer's VPCID stays "".
func (s *VPCService) ListVPCResources(ctx context.Context) ([]Resource, error) {
	var subnetVPCs map[string]string
	if s.loadBalancerVPCs {
		var err error
		if subnetVPCs, err = s.subnetVPCs(ctx); err != nil {
			return nil, err
		}
	}
	listLoadBalancers := func(ctx context.Context) ([]Resource, error) {
		return s.listLoadBalancers(ctx, subnetVPCs)
	}
	listers := []func(context.Context) ([]Resource, error){
		listLoadBalancers, s.listEndpointGateways, s.listSecurityGroups, s.listVolumes,
	}
	var all []Resource
	for _, list := range listers {
		resources, err := list(ctx)
		if err != nil {
			return nil, err
		}
		all = append(all, resources...)
	}
	return all, nil
}

// listLoadBalancers returns every load balancer of the resource group, each with the VPC of its subnets.
//
// Parameters:
//   - ctx bounds the requests.
//   - subnetVPCs maps subnet IDs to VPC IDs (subnetVPCs); nil, without WithLoadBalancerVPCs, leaves every VPCID "".
func (s *VPCService) listLoadBalancers(ctx context.Context, subnetVPCs map[string]string) ([]Resource, error) {
	pager, err := s.sdk.NewLoadBalancersPager(&vpcv1.ListLoadBalancersOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VPC load balancers: %w", err)
	}
	items, err := pager.GetAllWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing VPC load balancers: %w", err)
	}
	var resources []Resource
	for i := range items {
		if s.inScope(items[i].ResourceGroup) {
			resources = append(resources, Resource{Kind: KindLoadBalancer, ID: deref(items[i].ID), Name: deref(items[i].Name),
				VPCID: loadBalancerVPC(items[i].Subnets, subnetVPCs)})
		}
	}
	return resources, nil
}

// subnetVPCs maps the ID of every subnet of the region that the credentials can see to the ID of its VPC.
//
// Parameters:
//   - ctx bounds the requests.
func (s *VPCService) subnetVPCs(ctx context.Context) (map[string]string, error) {
	pager, err := s.sdk.NewSubnetsPager(&vpcv1.ListSubnetsOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VPC subnets: %w", err)
	}
	items, err := pager.GetAllWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing VPC subnets: %w", err)
	}
	vpcs := make(map[string]string, len(items))
	for i := range items {
		if id := deref(items[i].ID); id != "" {
			vpcs[id] = vpcID(items[i].VPC)
		}
	}
	return vpcs, nil
}

// loadBalancerVPC returns the VPC of a load balancer's subnets. IBM places all subnets of a load balancer in one VPC;
// should the known subnets still name two VPCs, or none be known, it returns "", so that the load balancer is
// attributed to no VPC rather than to a guessed one.
//
// Parameters:
//   - subnets are the load balancer's subnet references.
//   - subnetVPCs maps subnet IDs to VPC IDs (subnetVPCs).
func loadBalancerVPC(subnets []vpcv1.SubnetReference, subnetVPCs map[string]string) string {
	found := ""
	for i := range subnets {
		vpc := subnetVPCs[deref(subnets[i].ID)]
		switch {
		case vpc == "":
		case found == "":
			found = vpc
		case found != vpc:
			return ""
		}
	}
	return found
}

// ResolveVPCID returns the ID of the VPC named name in the region, found the way CAPIBM finds spec.vpc by name: among
// all VPCs of the region, whatever their resource group (IBM keeps VPC names unique per region). A name that matches
// several VPCs is still refused rather than guessed.
//
// Parameters:
//   - ctx bounds the requests.
//   - name is IBMCLOUD_VPC_NAME, the name of the existing e2e VPC.
func (s *VPCService) ResolveVPCID(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", errors.New("resolving the e2e VPC: IBMCLOUD_VPC_NAME is empty; set it to the name of the existing e2e VPC")
	}
	pager, err := s.sdk.NewVpcsPager(&vpcv1.ListVpcsOptions{})
	if err != nil {
		return "", fmt.Errorf("resolving the e2e VPC %q: listing VPCs: %w", name, err)
	}
	items, err := pager.GetAllWithContext(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving the e2e VPC %q: listing VPCs: %w", name, err)
	}
	var ids []string
	for i := range items {
		if deref(items[i].Name) == name {
			ids = append(ids, deref(items[i].ID))
		}
	}
	switch {
	case len(ids) == 0:
		return "", fmt.Errorf("resolving the e2e VPC: no VPC of the region is named %q; set IBMCLOUD_VPC_NAME to the "+
			"name of the existing e2e VPC (the VPC endpoint selects the region)", name)
	case len(ids) > 1:
		return "", fmt.Errorf("resolving the e2e VPC: %d VPCs are named %q (IDs %s); rename all but one", len(ids), name,
			strings.Join(ids, ", "))
	case ids[0] == "":
		return "", fmt.Errorf("resolving the e2e VPC: IBM listed the VPC %q without an ID", name)
	}
	return ids[0], nil
}

// listEndpointGateways returns every VPE gateway of the resource group.
func (s *VPCService) listEndpointGateways(ctx context.Context) ([]Resource, error) {
	pager, err := s.sdk.NewEndpointGatewaysPager(&vpcv1.ListEndpointGatewaysOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VPE gateways: %w", err)
	}
	items, err := pager.GetAllWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing VPE gateways: %w", err)
	}
	var resources []Resource
	for i := range items {
		if s.inScope(items[i].ResourceGroup) {
			resources = append(resources, Resource{Kind: KindEndpointGateway, ID: deref(items[i].ID),
				Name: deref(items[i].Name), VPCID: vpcID(items[i].VPC)})
		}
	}
	return resources, nil
}

// listSecurityGroups returns every security group of the resource group.
func (s *VPCService) listSecurityGroups(ctx context.Context) ([]Resource, error) {
	pager, err := s.sdk.NewSecurityGroupsPager(&vpcv1.ListSecurityGroupsOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VPC security groups: %w", err)
	}
	items, err := pager.GetAllWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing VPC security groups: %w", err)
	}
	var resources []Resource
	for i := range items {
		if s.inScope(items[i].ResourceGroup) {
			resources = append(resources, Resource{Kind: KindSecurityGroup, ID: deref(items[i].ID),
				Name: deref(items[i].Name), VPCID: vpcID(items[i].VPC)})
		}
	}
	return resources, nil
}

// listVolumes returns every block storage volume of the resource group.
func (s *VPCService) listVolumes(ctx context.Context) ([]Resource, error) {
	pager, err := s.sdk.NewVolumesPager(&vpcv1.ListVolumesOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing VPC volumes: %w", err)
	}
	items, err := pager.GetAllWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing VPC volumes: %w", err)
	}
	var resources []Resource
	for i := range items {
		if s.inScope(items[i].ResourceGroup) {
			resources = append(resources, Resource{Kind: KindVolume, ID: deref(items[i].ID), Name: deref(items[i].Name)})
		}
	}
	return resources, nil
}

// inScope reports whether a resource of the resource group group belongs to the inventory: always without a resource
// group filter, otherwise only when the IDs match.
//
// Parameters:
//   - group is the resource's resource group reference; nil when IBM sent none.
func (s *VPCService) inScope(group *vpcv1.ResourceGroupReference) bool {
	if s.resourceGroupID == "" {
		return true
	}
	return group != nil && deref(group.ID) == s.resourceGroupID
}

// vpcID returns the ID of a VPC reference, "" for nil.
//
// Parameters:
//   - vpc is the resource's VPC reference.
func vpcID(vpc *vpcv1.VPCReference) string {
	if vpc == nil {
		return ""
	}
	return deref(vpc.ID)
}

// deref returns *s, or "" for nil.
//
// Parameters:
//   - s is an optional SDK string field.
func deref(s *string) string {
	return core.StringNilMapper(s)
}
