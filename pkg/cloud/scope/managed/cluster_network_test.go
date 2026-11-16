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
	"errors"
	"testing"

	. "github.com/onsi/gomega"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
)

// clusterNetworkOf returns a ClusterNetwork with the given pod and service blocks; nil leaves a list empty.
func clusterNetworkOf(pods, services []string) clusterv1.ClusterNetwork {
	return clusterv1.ClusterNetwork{
		Pods:     clusterv1.NetworkRanges{CIDRBlocks: pods},
		Services: clusterv1.NetworkRanges{CIDRBlocks: services},
	}
}

func TestValidateClusterNetwork(t *testing.T) {
	// The worker subnets of a typical VPC (10.243.0.0/24 per zone, as in the live probes).
	workers := []string{"10.243.0.0/24", "10.243.64.0/24", "", "not-a-cidr"}
	testCases := []struct {
		name string
		// distribution is spec.distribution; Kubernetes when empty.
		distribution controlplanev1.Distribution
		network      clusterv1.ClusterNetwork
		workers      []string
		want         ClusterSubnets
		problems     []string
	}{
		{name: "empty lets IBM pick both ranges", network: clusterNetworkOf(nil, nil), workers: workers, want: ClusterSubnets{}},
		{name: "IBM's own IKS defaults on a live cluster", network: clusterNetworkOf([]string{"172.17.0.0/18"}, []string{"172.21.0.0/16"}), workers: workers,
			want: ClusterSubnets{PodSubnet: "172.17.0.0/18", ServiceSubnet: "172.21.0.0/16"}},
		{name: "only pods", network: clusterNetworkOf([]string{"192.168.0.0/16"}, nil), want: ClusterSubnets{PodSubnet: "192.168.0.0/16"}},
		{name: "only services", network: clusterNetworkOf(nil, []string{"198.18.0.0/24"}), want: ClusterSubnets{ServiceSubnet: "198.18.0.0/24"}},

		// Sizes: pods at least /23, services at least /24.
		{name: "pods /23 is the smallest", network: clusterNetworkOf([]string{"172.17.2.0/23"}, nil), want: ClusterSubnets{PodSubnet: "172.17.2.0/23"}},
		{name: "pods /24 is too small", network: clusterNetworkOf([]string{"172.17.2.0/24"}, nil),
			problems: []string{`Cluster spec.clusterNetwork.pods.cidrBlocks[0] "172.17.2.0/24" is smaller than /23; IBM needs a pod subnet of at least /23`}},
		{name: "services /24 is the smallest", network: clusterNetworkOf(nil, []string{"172.21.5.0/24"}), want: ClusterSubnets{ServiceSubnet: "172.21.5.0/24"}},
		{name: "services /25 is too small", network: clusterNetworkOf(nil, []string{"172.21.5.0/25"}),
			problems: []string{`services.cidrBlocks[0] "172.21.5.0/25" is smaller than /24; IBM needs a service subnet of at least /24`}},

		// Ranges.
		{name: "172.21-172.31 as one range", network: clusterNetworkOf([]string{"172.24.0.0/13"}, []string{"172.22.0.0/16"}),
			want: ClusterSubnets{PodSubnet: "172.24.0.0/13", ServiceSubnet: "172.22.0.0/16"}},
		{name: "198.18.0.0/15 whole", network: clusterNetworkOf([]string{"198.18.0.0/15"}, nil), want: ClusterSubnets{PodSubnet: "198.18.0.0/15"}},
		{name: "upper edge of 172.31", network: clusterNetworkOf(nil, []string{"172.31.255.0/24"}), want: ClusterSubnets{ServiceSubnet: "172.31.255.0/24"}},
		{name: "172.16.0.0/12 reaches the prohibited 172.16-172.20", network: clusterNetworkOf([]string{"172.16.0.0/12"}, nil),
			problems: []string{`"172.16.0.0/12" is outside the ranges IBM allows (172.17.0.0/16, 172.21.0.0-172.31.255.255, 192.168.0.0/16 or 198.18.0.0/15)`}},
		{name: "prohibited 172.18.0.0/16", network: clusterNetworkOf(nil, []string{"172.18.0.0/16"}), problems: []string{`"172.18.0.0/16" is outside the ranges`}},
		{name: "172.17.0.0/15 spills into 172.16", network: clusterNetworkOf([]string{"172.16.0.0/15"}, nil), problems: []string{"is outside the ranges"}},
		{name: "10.0.0.0/16 is a VPC range", network: clusterNetworkOf([]string{"10.0.0.0/16"}, nil), problems: []string{"is outside the ranges"}},
		{name: "192.168.0.0/15 reaches into 192.169", network: clusterNetworkOf([]string{"192.168.0.0/15"}, nil), problems: []string{`"192.168.0.0/15" is outside the ranges`}},
		{name: "a /32 inside a range is still too small", network: clusterNetworkOf(nil, []string{"172.21.0.1/32"}), problems: []string{"is smaller than /24"}},

		// Form.
		{name: "two pod blocks", network: clusterNetworkOf([]string{"172.17.0.0/18", "172.17.64.0/18"}, nil),
			problems: []string{"Cluster spec.clusterNetwork.pods.cidrBlocks lists 2 blocks; IBM takes one subnet"}},
		{name: "two service blocks", network: clusterNetworkOf(nil, []string{"172.21.0.0/16", "172.22.0.0/16"}),
			problems: []string{"services.cidrBlocks lists 2 blocks"}},
		{name: "IPv6 pods", network: clusterNetworkOf([]string{"fd00::/48"}, nil), problems: []string{`"fd00::/48" is not IPv4`}},
		{name: "IPv4-mapped IPv6 services", network: clusterNetworkOf(nil, []string{"::ffff:172.21.0.0/112"}), problems: []string{"is not IPv4"}},
		{name: "not a CIDR", network: clusterNetworkOf([]string{"172.17.0.0"}, nil), problems: []string{`"172.17.0.0" is not a CIDR block`}},
		{name: "host bits set", network: clusterNetworkOf([]string{"172.17.1.0/18"}, nil), problems: []string{`"172.17.1.0/18" has host bits set; write it as 172.17.0.0/18`}},

		// IBM's size-only pod form 0.0.0.0/X, 13 <= X <= 23.
		{name: "size-only /13", network: clusterNetworkOf([]string{"0.0.0.0/13"}, nil), want: ClusterSubnets{PodSubnet: "0.0.0.0/13"}},
		{name: "size-only /23", network: clusterNetworkOf([]string{"0.0.0.0/23"}, []string{"172.21.0.0/16"}), workers: []string{"172.17.0.0/24"},
			want: ClusterSubnets{PodSubnet: "0.0.0.0/23", ServiceSubnet: "172.21.0.0/16"}},
		{name: "size-only /12 is too large", network: clusterNetworkOf([]string{"0.0.0.0/12"}, nil),
			problems: []string{`"0.0.0.0/12" asks IBM to pick a subnet of size /12; IBM picks sizes from /13 to /23`}},
		{name: "size-only /24 is too small", network: clusterNetworkOf([]string{"0.0.0.0/24"}, nil), problems: []string{"size /24"}},
		{name: "size-only for services", network: clusterNetworkOf([]string{"0.0.0.0/16"}, []string{"0.0.0.0/16"}),
			problems: []string{`services.cidrBlocks[0] "0.0.0.0/16" uses the size-only form 0.0.0.0/X, which IBM accepts for the pod subnet only`}},

		// Overlaps.
		{name: "pods and services overlap", network: clusterNetworkOf([]string{"172.21.0.0/18"}, []string{"172.21.0.0/16"}),
			problems: []string{"the pod subnet 172.21.0.0/18 and the service subnet 172.21.0.0/16 of Cluster spec.clusterNetwork overlap"}},
		{name: "pods overlap a worker subnet", network: clusterNetworkOf([]string{"192.168.0.0/18"}, nil), workers: []string{"192.168.10.0/24", "10.243.0.0/24"},
			problems: []string{"the pod subnet 192.168.0.0/18 of Cluster spec.clusterNetwork overlaps the worker subnet 192.168.10.0/24 of the IBMCloudManagedCluster"}},
		{name: "services may share a range with the worker subnets", network: clusterNetworkOf(nil, []string{"192.168.0.0/16"}), workers: []string{"192.168.10.0/24"},
			want: ClusterSubnets{ServiceSubnet: "192.168.0.0/16"}},
		{name: "every problem is listed", network: clusterNetworkOf([]string{"172.17.0.0/24"}, []string{"fd00::/64"}),
			problems: []string{"pods.cidrBlocks[0] \"172.17.0.0/24\" is smaller than /23", "services.cidrBlocks[0] \"fd00::/64\" is not IPv4"}},

		// OpenShift: IBM's openshift docs end the 192.168 range at 192.168.254.255; the other ranges are the same.
		{name: "ROKS IBM defaults on a live cluster", distribution: controlplanev1.DistributionOpenShift,
			network: clusterNetworkOf([]string{"172.17.64.0/18"}, []string{"172.21.0.0/16"}), workers: workers,
			want: ClusterSubnets{PodSubnet: "172.17.64.0/18", ServiceSubnet: "172.21.0.0/16"}},
		{name: "ROKS pods 192.168.0.0/16 reach 192.168.255", distribution: controlplanev1.DistributionOpenShift,
			network:  clusterNetworkOf([]string{"192.168.0.0/16"}, nil),
			problems: []string{`"192.168.0.0/16" is outside the ranges IBM allows (172.17.0.0/16, 172.21.0.0-172.31.255.255, 192.168.0.0-192.168.254.255 or 198.18.0.0/15)`}},
		{name: "ROKS services 192.168.255.0/24 are outside", distribution: controlplanev1.DistributionOpenShift,
			network: clusterNetworkOf(nil, []string{"192.168.255.0/24"}), problems: []string{`services.cidrBlocks[0] "192.168.255.0/24" is outside the ranges`}},
		{name: "ROKS pods 192.168.0.0/17 are inside", distribution: controlplanev1.DistributionOpenShift,
			network: clusterNetworkOf([]string{"192.168.0.0/17"}, nil), want: ClusterSubnets{PodSubnet: "192.168.0.0/17"}},
		{name: "ROKS upper edge 192.168.254.0/24", distribution: controlplanev1.DistributionOpenShift,
			network: clusterNetworkOf(nil, []string{"192.168.254.0/24"}), want: ClusterSubnets{ServiceSubnet: "192.168.254.0/24"}},
		{name: "IKS services 192.168.255.0/24 are inside", network: clusterNetworkOf(nil, []string{"192.168.255.0/24"}),
			want: ClusterSubnets{ServiceSubnet: "192.168.255.0/24"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			d := tc.distribution
			if d == "" {
				d = controlplanev1.DistributionKubernetes
			}
			got, err := ValidateClusterNetwork(d, tc.network, tc.workers)
			if len(tc.problems) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(Equal(tc.want))
				return
			}
			var configErr *ConfigurationError
			g.Expect(errors.As(err, &configErr)).To(BeTrue(), "want a ConfigurationError, got %v", err)
			g.Expect(configErr.Problems).To(HaveLen(len(tc.problems)))
			for i, part := range tc.problems {
				g.Expect(configErr.Problems[i]).To(ContainSubstring(part))
			}
			g.Expect(got).To(Equal(ClusterSubnets{}))
		})
	}
}

func TestValidateClusterNetworkDistribution(t *testing.T) {
	g := NewWithT(t)
	got, err := ValidateClusterNetwork("Other", clusterNetworkOf([]string{"172.17.0.0/18"}, nil), nil)
	g.Expect(err).To(MatchError(`ValidateClusterNetwork needs spec.distribution "Kubernetes" or "OpenShift" to select IBM's subnet ranges; got "Other"`))
	var configErr *ConfigurationError
	g.Expect(errors.As(err, &configErr)).To(BeFalse(), "a caller error, not a problem of the user's ranges")
	g.Expect(got).To(Equal(ClusterSubnets{}))
}

func TestNetworkRangesAreFresh(t *testing.T) {
	g := NewWithT(t)
	for _, d := range []controlplanev1.Distribution{controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift} {
		ranges := networkRanges(d)
		ranges[0].first = ranges[0].last
		ranges[2].last = ranges[2].first
		g.Expect(networkRanges(d)[0].first.String()).To(Equal("172.17.0.0"), "a caller cannot change IBM's ranges")
		g.Expect(networkRanges(d)[2].last.String()).To(HavePrefix("192.168.25"), "a caller cannot change IBM's ranges")
	}
	g.Expect(networkRanges(controlplanev1.DistributionKubernetes)[2].last.String()).To(Equal("192.168.255.255"))
	g.Expect(networkRanges(controlplanev1.DistributionOpenShift)[2].last.String()).To(Equal("192.168.254.255"))
}
