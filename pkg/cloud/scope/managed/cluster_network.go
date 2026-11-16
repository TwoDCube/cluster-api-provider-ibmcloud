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
	"encoding/binary"
	"fmt"
	"net/netip"
	"strings"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
)

// This file checks Cluster.spec.clusterNetwork against IBM's rules for the pod and service subnets of a VPC cluster
// (IBM Cloud docs vpc-subnets.md of the containers docs, last updated 2026-10-06, and of the openshift docs, last
// updated 2026-07-27), so that a range IBM would refuse fails before CAPIBM sends createCluster, with a message that
// names the field, the value and the fix, instead of an IBM 400 after the create intent was recorded. The rules depend
// on the distribution: the two doc sets allow different 192.168 ranges. The templates omit clusterNetwork, so IBM
// picks free ranges; custom ranges cannot be shared between clusters, which only the book can say (IBM checks it).

// IBM's size rules for the subnets (vpc-subnets.md "Size requirements").
const (
	// networkPodMaxPrefix: a custom pod subnet is at least /23 (enough pod IPs for four workers).
	networkPodMaxPrefix = 23
	// networkServiceMaxPrefix: a custom service subnet is at least /24 (255 services).
	networkServiceMaxPrefix = 24
	// networkSizeOnlyMinPrefix is the largest size IBM picks for the pod form 0.0.0.0/X ("the maximum subnet is limited
	// to 13"); the smallest is networkPodMaxPrefix.
	networkSizeOnlyMinPrefix = 13
)

// networkRange is one range IBM allows for pod and service subnets; both ends are included.
type networkRange struct {
	// name is how messages name the range.
	name string
	// first is the lowest address of the range.
	first netip.Addr
	// last is the highest address of the range.
	last netip.Addr
}

// networkRanges returns the ranges IBM allows for the pod and service subnets of a distribution ("Range requirements"
// of vpc-subnets.md). The doc sets disagree on the 192.168 range only: the containers docs allow
// 192.168.0.0-192.168.255.255 for Kubernetes, while the openshift docs (vpc-subnets.md, and --pod-subnet and
// --service-subnet of openshift-cluster-create-vpc-gen2.md) allow 192.168.0.0-192.168.254.255 for OpenShift. It is a
// function so that no caller can change the list for the whole process.
//
// Parameters:
//   - d: spec.distribution; ValidateClusterNetwork accepts only Kubernetes and OpenShift.
func networkRanges(d controlplanev1.Distribution) []networkRange {
	ranges := []networkRange{
		{name: "172.17.0.0/16", first: netip.AddrFrom4([4]byte{172, 17, 0, 0}), last: netip.AddrFrom4([4]byte{172, 17, 255, 255})},
		{name: "172.21.0.0-172.31.255.255", first: netip.AddrFrom4([4]byte{172, 21, 0, 0}),
			last: netip.AddrFrom4([4]byte{172, 31, 255, 255})},
		{name: "192.168.0.0/16", first: netip.AddrFrom4([4]byte{192, 168, 0, 0}), last: netip.AddrFrom4([4]byte{192, 168, 255, 255})},
		{name: "198.18.0.0/15", first: netip.AddrFrom4([4]byte{198, 18, 0, 0}), last: netip.AddrFrom4([4]byte{198, 19, 255, 255})},
	}
	if d == controlplanev1.DistributionOpenShift {
		ranges[2] = networkRange{name: "192.168.0.0-192.168.254.255", first: netip.AddrFrom4([4]byte{192, 168, 0, 0}),
			last: netip.AddrFrom4([4]byte{192, 168, 254, 255})}
	}
	return ranges
}

// networkRangeNames names ranges for a message, for example "172.17.0.0/16, 172.21.0.0-172.31.255.255, 192.168.0.0/16
// or 198.18.0.0/15".
//
// Parameters:
//   - ranges: the ranges of networkRanges (always four).
func networkRangeNames(ranges []networkRange) string {
	names := make([]string, 0, len(ranges))
	for _, r := range ranges {
		names = append(names, r.name)
	}
	last := len(names) - 1
	return strings.Join(names[:last], ", ") + " or " + names[last]
}

// ClusterSubnets are the pod and service subnets CAPIBM sends to createCluster.
type ClusterSubnets struct {
	// PodSubnet is "" (IBM picks the next free /18 of 172.17.0.0/16), a CIDR block, or IBM's size-only form 0.0.0.0/X
	// (IBM picks a free range of that size).
	PodSubnet string
	// ServiceSubnet is "" (IBM's default 172.21.0.0/16) or a CIDR block.
	ServiceSubnet string
}

// ValidateClusterNetwork checks the pod and service CIDR blocks of a CAPI Cluster against IBM's rules for the
// distribution and returns the subnets to send. The rules are IBM's: at most one IPv4 block each; pods at least /23 and
// services at least /24; both inside 172.17.0.0/16, 172.21.0.0-172.31.255.255, 198.18.0.0/15, or 192.168.0.0/16 for
// Kubernetes and 192.168.0.0-192.168.254.255 for OpenShift; no overlap between them or between the pod subnet and the
// worker subnets of the VPC; and, for pods only, the size-only form 0.0.0.0/X with X from 13 to 23, for which IBM
// picks the range and no overlap is checked. A block must be written as its network address.
//
// Parameters:
//   - d: spec.distribution of the IBMCloudManagedControlPlane, Kubernetes or OpenShift; it selects IBM's ranges.
//   - network: Cluster.spec.clusterNetwork; empty blocks let IBM pick.
//   - workerSubnetCIDRs: the ipv4CIDRBlock of every zone in the IBMCloudManagedCluster status; an empty or malformed
//     entry is skipped (the InfraCluster reports its own subnets).
//
// It returns a *ConfigurationError that lists every problem, each naming the field, the value and the fix, or another
// error when d is neither Kubernetes nor OpenShift.
func ValidateClusterNetwork(d controlplanev1.Distribution, network clusterv1.ClusterNetwork, workerSubnetCIDRs []string) (ClusterSubnets, error) {
	if bindingClusterType(d) == "" {
		return ClusterSubnets{}, fmt.Errorf("ValidateClusterNetwork needs spec.distribution %q or %q to select IBM's "+
			"subnet ranges; got %q", controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift, d)
	}
	ranges := networkRanges(d)
	pods, podProblem := networkBlock("spec.clusterNetwork.pods.cidrBlocks", network.Pods.CIDRBlocks, networkPodMaxPrefix,
		"a pod subnet of at least /23 (enough for four workers), for example 172.17.0.0/18", true, ranges)
	services, serviceProblem := networkBlock("spec.clusterNetwork.services.cidrBlocks", network.Services.CIDRBlocks,
		networkServiceMaxPrefix, "a service subnet of at least /24 (255 services), for example 172.21.0.0/16", false, ranges)
	var problems []string
	for _, problem := range []string{podProblem, serviceProblem} {
		if problem != "" {
			problems = append(problems, problem)
		}
	}
	podSizeOnly := pods.IsValid() && networkSizeOnly(pods)
	if pods.IsValid() && !podSizeOnly && services.IsValid() && pods.Overlaps(services) {
		problems = append(problems, fmt.Sprintf("the pod subnet %s and the service subnet %s of Cluster "+
			"spec.clusterNetwork overlap; IBM needs separate ranges", pods, services))
	}
	if pods.IsValid() && !podSizeOnly {
		for _, cidr := range workerSubnetCIDRs {
			worker, err := netip.ParsePrefix(cidr)
			if err == nil && pods.Overlaps(worker) {
				problems = append(problems, fmt.Sprintf("the pod subnet %s of Cluster spec.clusterNetwork overlaps the "+
					"worker subnet %s of the IBMCloudManagedCluster; choose a pod range outside the VPC subnets", pods, worker))
			}
		}
	}
	if len(problems) > 0 {
		return ClusterSubnets{}, &ConfigurationError{Problems: problems}
	}
	subnets := ClusterSubnets{}
	if pods.IsValid() {
		subnets.PodSubnet = pods.String()
	}
	if services.IsValid() {
		subnets.ServiceSubnet = services.String()
	}
	return subnets, nil
}

// networkBlock checks the CIDR block list of one subnet. It returns the block (the zero prefix when the list is empty
// or invalid) and the problem, "" when there is none. The size-only form 0.0.0.0/X is returned as is when the subnet
// allows it and X is in IBM's range.
//
// Parameters:
//   - field: the field path in messages, for example "spec.clusterNetwork.pods.cidrBlocks".
//   - blocks: the CIDR blocks of that field.
//   - maxPrefix: the longest prefix IBM accepts for this subnet.
//   - need: what IBM needs, for the size message.
//   - sizeOnly: whether IBM accepts the size-only form 0.0.0.0/X for this subnet (pods only).
//   - ranges: the ranges IBM allows for the distribution (networkRanges).
func networkBlock(field string, blocks []string, maxPrefix int, need string, sizeOnly bool, ranges []networkRange) (netip.Prefix, string) {
	switch len(blocks) {
	case 0:
		return netip.Prefix{}, ""
	case 1:
	default:
		return netip.Prefix{}, fmt.Sprintf("Cluster %s lists %d blocks; IBM takes one subnet, so list one block, or "+
			"remove clusterNetwork so that IBM picks free ranges", field, len(blocks))
	}
	value := blocks[0]
	prefix, err := netip.ParsePrefix(value)
	switch {
	case err != nil:
		return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q is not a CIDR block; write an IPv4 block such as 172.17.0.0/18",
			field, value)
	case !prefix.Addr().Is4():
		return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q is not IPv4; IBM VPC clusters take IPv4 pod and service "+
			"subnets only", field, value)
	case networkSizeOnly(prefix):
		if !sizeOnly {
			return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q uses the size-only form 0.0.0.0/X, which IBM accepts for "+
				"the pod subnet only; give a block such as 172.21.0.0/16", field, value)
		}
		if prefix.Bits() < networkSizeOnlyMinPrefix || prefix.Bits() > networkPodMaxPrefix {
			return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q asks IBM to pick a subnet of size /%d; IBM picks sizes "+
				"from /%d to /%d", field, value, prefix.Bits(), networkSizeOnlyMinPrefix, networkPodMaxPrefix)
		}
		return prefix, ""
	case prefix.Masked() != prefix:
		return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q has host bits set; write it as %s", field, value, prefix.Masked())
	case prefix.Bits() > maxPrefix:
		return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q is smaller than /%d; IBM needs %s", field, value, maxPrefix, need)
	case !networkInAllowedRange(prefix, ranges):
		return netip.Prefix{}, fmt.Sprintf("Cluster %s[0] %q is outside the ranges IBM allows (%s)", field, value,
			networkRangeNames(ranges))
	}
	return prefix, ""
}

// networkSizeOnly reports whether prefix is IBM's size-only form 0.0.0.0/X.
//
// Parameters:
//   - prefix: a parsed IPv4 prefix.
func networkSizeOnly(prefix netip.Prefix) bool {
	return prefix.Addr() == netip.IPv4Unspecified()
}

// networkInAllowedRange reports whether every address of prefix lies inside one of ranges.
//
// Parameters:
//   - prefix: a parsed, masked IPv4 prefix.
//   - ranges: the ranges IBM allows for the distribution (networkRanges).
func networkInAllowedRange(prefix netip.Prefix, ranges []networkRange) bool {
	first := prefix.Addr()
	start := first.As4()
	// The last address sets every host bit. Bits() is between 0 and 32 for a valid IPv4 prefix, and a shift of a
	// uint32 by 32 is 0 in Go, so a /32 has no host bits.
	end := binary.BigEndian.Uint32(start[:]) | ^uint32(0)>>prefix.Bits()
	var last [4]byte
	binary.BigEndian.PutUint32(last[:], end)
	lastAddr := netip.AddrFrom4(last)
	for _, r := range ranges {
		if first.Compare(r.first) >= 0 && lastAddr.Compare(r.last) <= 0 {
			return true
		}
	}
	return false
}
