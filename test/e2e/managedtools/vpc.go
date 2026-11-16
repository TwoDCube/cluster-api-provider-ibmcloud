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
	"fmt"
	"net/http"

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
	// VPCID is the ID of the VPC the resource belongs to; "" for kinds without one (clusters, load balancers, volumes).
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

// ListVPCResources implements VPCResourceLister. It follows every page of the four collections. The VPC API filters
// only some collections by resource group, so the filter is applied here, the same way for all four.
func (s *VPCService) ListVPCResources(ctx context.Context) ([]Resource, error) {
	listers := []func(context.Context) ([]Resource, error){
		s.listLoadBalancers, s.listEndpointGateways, s.listSecurityGroups, s.listVolumes,
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

// listLoadBalancers returns every load balancer of the resource group.
func (s *VPCService) listLoadBalancers(ctx context.Context) ([]Resource, error) {
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
			resources = append(resources, Resource{Kind: KindLoadBalancer, ID: deref(items[i].ID), Name: deref(items[i].Name)})
		}
	}
	return resources, nil
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
