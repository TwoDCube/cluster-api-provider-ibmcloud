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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// createRequestObjects are the inputs of BuildCreateClusterRequest.
type createRequestObjects struct {
	cp      *controlplanev1.IBMCloudManagedControlPlane
	cluster *clusterv1.Cluster
	infra   *infrav1.IBMCloudManagedCluster
	pool    *infrav1.IBMCloudManagedMachinePool
	mp      *clusterv1.MachinePool
}

// build calls BuildCreateClusterRequest with the objects.
func (o createRequestObjects) build() (kubernetesservice.CreateClusterRequest, error) {
	return BuildCreateClusterRequest(o.cp, o.cluster, o.infra, o.pool, o.mp)
}

// createRequestIKS returns the objects of the HA IKS template (3 zones x 2 workers) without clusterNetwork, so that IBM
// picks the ranges. Every ID is a placeholder.
func createRequestIKS() createRequestObjects {
	meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name, Namespace: "default"} }
	zones := []string{"eu-de-1", "eu-de-2", "eu-de-3"}
	infra := &infrav1.IBMCloudManagedCluster{ObjectMeta: meta("dev-iks"), Spec: infrav1.IBMCloudManagedClusterSpec{
		Region:        "eu-de",
		ResourceGroup: infrav1.ResourceGroupReference{ID: "0123456789abcdef0123456789abcdef"},
		VPC:           infrav1.VPCSource{Type: infrav1.SourceTypeReference, Reference: infrav1.ResourceIdentifier{Name: "dev-vpc"}},
	}}
	infra.Status.VPC = infrav1.ResolvedResource{ID: "r010-00000000-0000-0000-0000-00000000000a", Name: "dev-vpc"}
	pool := &infrav1.IBMCloudManagedMachinePool{ObjectMeta: meta("dev-iks-default"), Spec: infrav1.IBMCloudManagedMachinePoolSpec{
		PoolName: InitialWorkerPoolName, Flavor: "bx2.4x16",
	}}
	for i, zone := range zones {
		subnetID := fmt.Sprintf("02b7-00000000-0000-0000-0000-%012d", i+1)
		infra.Spec.Zones = append(infra.Spec.Zones, infrav1.ZoneSubnet{Name: zone, Subnet: infrav1.ResourceIdentifier{ID: subnetID}})
		infra.Status.Zones = append(infra.Status.Zones, infrav1.ResolvedZone{Name: zone,
			Subnet: infrav1.ResolvedResource{ID: subnetID}, IPv4CIDRBlock: fmt.Sprintf("10.243.%d.0/24", i)})
		pool.Spec.Zones = append(pool.Spec.Zones, infrav1.WorkerPoolZone{Name: zone})
	}
	return createRequestObjects{
		cp: &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: meta("dev-iks"), Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.36",
		}},
		cluster: &clusterv1.Cluster{ObjectMeta: meta("dev-iks")},
		infra:   infra,
		pool:    pool,
		mp:      &clusterv1.MachinePool{ObjectMeta: meta("dev-iks-default"), Spec: clusterv1.MachinePoolSpec{Replicas: ptr.To[int32](6)}},
	}
}

// createRequestROKS returns the objects of a private-only ROKS cluster with a chosen name and custom ranges.
func createRequestROKS() createRequestObjects {
	o := createRequestIKS()
	o.cp.Spec = controlplanev1.IBMCloudManagedControlPlaneSpec{
		Distribution:        controlplanev1.DistributionOpenShift,
		DistributionVersion: "4.21",
		IBMCloudClusterName: "team-a-dev-roks",
		OpenShift: controlplanev1.OpenShiftSpec{ImageRegistry: controlplanev1.ImageRegistrySpec{COS: controlplanev1.COSInstanceReference{
			InstanceCRN: "crn:v1:bluemix:public:cloud-object-storage:global:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:00000000-0000-0000-0000-000000000c05::",
		}}},
		Endpoints: controlplanev1.EndpointsSpec{PublicServiceEndpoint: controlplanev1.PublicServiceEndpointDisabled},
	}
	o.cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
		Pods:     clusterv1.NetworkRanges{CIDRBlocks: []string{"172.17.64.0/18"}},
		Services: clusterv1.NetworkRanges{CIDRBlocks: []string{"172.21.0.0/16"}},
	}
	return o
}

// createRequestGoldenPath returns the path of a golden request file of this package's testdata.
func createRequestGoldenPath(name string) string {
	return filepath.Join("testdata", "create_request", name)
}

// createRequestGolden compares req, as indented JSON, with testdata/create_request/<name>. The domain request has no
// JSON tags, so the keys are the Go field names of kubernetesservice.CreateClusterRequest.
func createRequestGolden(t *testing.T, req kubernetesservice.CreateClusterRequest, name string) {
	t.Helper()
	g := NewWithT(t)
	got, err := json.MarshalIndent(req, "", "  ")
	g.Expect(err).NotTo(HaveOccurred())
	want, err := os.ReadFile(createRequestGoldenPath(name))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(string(got) + "\n").To(Equal(string(want)))
}

func TestBuildCreateClusterRequestGolden(t *testing.T) {
	t.Run("IKS default ranges, public and private endpoints", func(t *testing.T) {
		req, err := createRequestIKS().build()
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		createRequestGolden(t, req, "iks.json")
	})
	t.Run("ROKS private-only with COS and custom ranges", func(t *testing.T) {
		req, err := createRequestROKS().build()
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		createRequestGolden(t, req, "roks.json")
	})
	t.Run("ROKS entitled initial pool", func(t *testing.T) {
		o := createRequestROKS()
		o.pool.Spec.Entitlement = infrav1.EntitlementOCPEntitled
		req, err := o.build()
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		createRequestGolden(t, req, "roks-entitled.json")
	})
	t.Run("the request passes the service's own validation fields", func(t *testing.T) {
		g := NewWithT(t)
		req, err := createRequestIKS().build()
		g.Expect(err).NotTo(HaveOccurred())
		// No operating system, taints or labels exist in the request type; IBM picks its default OS (observed on IBM
		// Cloud).
		g.Expect(req.WorkerPool.Name).To(Equal("default"))
		g.Expect(req.WorkerPool.WorkerCountPerZone).To(BeEquivalentTo(2))
		g.Expect(req.KubeVersion).To(Equal("1.36"))
	})
}

func TestBuildCreateClusterRequestGuards(t *testing.T) {
	testCases := []struct {
		name   string
		objs   func() createRequestObjects
		mutate func(o createRequestObjects)
		// want are substrings of the error; count is the number of problems (one when 0).
		want  []string
		count int
	}{
		{name: "replicas not divisible by zones", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.mp.Spec.Replicas = ptr.To[int32](4) },
			want:   []string{"MachinePool dev-iks-default spec.replicas=4 is not a positive multiple of the 3 zone(s) of IBMCloudManagedMachinePool dev-iks-default; use 3 or 6"}},
		{name: "ROKS replicas not divisible suggest at least the minimum", objs: createRequestROKS,
			mutate: func(o createRequestObjects) { o.mp.Spec.Replicas = ptr.To[int32](4) },
			want:   []string{"spec.replicas=4 is not a positive multiple of the 3 zone(s)", "use 6 or 9"}},
		{name: "zero replicas", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.mp.Spec.Replicas = ptr.To[int32](0) },
			want:   []string{"spec.replicas=0 is not a positive multiple", "use 3 or 6"}},
		{name: "no replicas", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.mp.Spec.Replicas = nil },
			want:   []string{"MachinePool dev-iks-default has no spec.replicas"}},
		{name: "IKS one worker per zone is enough", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.mp.Spec.Replicas = ptr.To[int32](3) }},
		{name: "ROKS needs two workers per zone at create", objs: createRequestROKS,
			mutate: func(o createRequestObjects) { o.mp.Spec.Replicas = ptr.To[int32](3) },
			want:   []string{"spec.replicas=3 gives 1 worker(s) per zone in 3 zone(s); IBM needs at least 2 per zone for a new OpenShift cluster, so set spec.replicas to at least 6"}},
		{name: "pool zone not in the InfraCluster", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.pool.Spec.Zones[2].Name = "eu-de-4" },
			want:   []string{"lists zone eu-de-4 in spec.zones, which IBMCloudManagedCluster dev-iks does not list"}},
		{name: "pool without zones", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.pool.Spec.Zones = nil },
			want:   []string{"IBMCloudManagedMachinePool dev-iks-default lists no zones"}},
		{name: "entitlement on Kubernetes", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.pool.Spec.Entitlement = infrav1.EntitlementOCPEntitled },
			want:   []string{"sets spec.entitlement OCPEntitled, which only distribution OpenShift accepts"}},
		{name: "unknown entitlement", objs: createRequestROKS,
			mutate: func(o createRequestObjects) { o.pool.Spec.Entitlement = "Other" },
			want:   []string{`has spec.entitlement "Other"`}},
		{name: "OpenShift without COS", objs: createRequestROKS,
			mutate: func(o createRequestObjects) { o.cp.Spec.OpenShift = controlplanev1.OpenShiftSpec{} },
			want:   []string{"has no spec.openShift.imageRegistry.cos.instanceCRN", "E7278"}},
		{name: "Kubernetes with COS", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.cp.Spec.OpenShift = createRequestROKS().cp.Spec.OpenShift },
			want:   []string{"sets spec.openShift, which only distribution OpenShift accepts"}},
		{name: "invalid default name", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.cluster.Name = "1-dev" },
			want:   []string{`IBMCloudManagedControlPlane dev-iks: the IBM cluster name "1-dev"`}},
		{name: "unknown distribution", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.cp.Spec.Distribution = "Other" },
			want:   []string{`has spec.distribution "Other"`}},
		{name: "invalid distributionVersion", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.cp.Spec.DistributionVersion = "1.36.5" },
			want:   []string{"spec.distributionVersion: invalid minor version"}},
		{name: "no flavor", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.pool.Spec.Flavor = "" },
			want:   []string{"has no spec.flavor"}},
		{name: "no resource group", objs: createRequestIKS,
			mutate: func(o createRequestObjects) { o.infra.Spec.ResourceGroup.ID = "" },
			want:   []string{"has no spec.resourceGroup.id"}},
		{name: "pod range overlaps a worker subnet", objs: createRequestIKS,
			mutate: func(o createRequestObjects) {
				o.infra.Status.Zones[0].IPv4CIDRBlock = "172.17.0.0/24"
				o.cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"172.17.0.0/18"}
			},
			want: []string{"overlaps the worker subnet 172.17.0.0/24"}},
		{name: "ROKS service range in 192.168.255 is outside IBM's OpenShift ranges", objs: createRequestROKS,
			mutate: func(o createRequestObjects) {
				o.cluster.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"192.168.255.0/24"}
			},
			want: []string{`services.cidrBlocks[0] "192.168.255.0/24" is outside the ranges IBM allows`, "192.168.0.0-192.168.254.255"}},
		{name: "IKS service range in 192.168.255 is inside IBM's Kubernetes ranges", objs: createRequestIKS,
			mutate: func(o createRequestObjects) {
				o.cluster.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"192.168.255.0/24"}
			}},
		{name: "every problem is listed", objs: createRequestROKS,
			mutate: func(o createRequestObjects) {
				o.mp.Spec.Replicas = ptr.To[int32](5)
				o.cp.Spec.OpenShift = controlplanev1.OpenShiftSpec{}
				o.cluster.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"10.0.0.0/16"}
			},
			want: []string{"spec.replicas=5", "cos.instanceCRN", "services.cidrBlocks[0] \"10.0.0.0/16\" is outside the ranges"}, count: 3},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			o := tc.objs()
			tc.mutate(o)
			req, err := o.build()
			if len(tc.want) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(req.Name).NotTo(BeEmpty())
				return
			}
			var configErr *ConfigurationError
			g.Expect(errors.As(err, &configErr)).To(BeTrue(), "want a ConfigurationError, got %v", err)
			g.Expect(configErr.Problems).To(HaveLen(max(tc.count, 1)))
			for _, part := range tc.want {
				g.Expect(err.Error()).To(ContainSubstring(part))
			}
			g.Expect(req).To(Equal(kubernetesservice.CreateClusterRequest{}))
		})
	}
}

func TestBuildCreateClusterRequestInputs(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(o *createRequestObjects)
		want   string
	}{
		{name: "nil control plane", mutate: func(o *createRequestObjects) { o.cp = nil }, want: "needs the IBMCloudManagedControlPlane"},
		{name: "nil Cluster", mutate: func(o *createRequestObjects) { o.cluster = nil }, want: "needs the owning Cluster"},
		{name: "nil InfraCluster", mutate: func(o *createRequestObjects) { o.infra = nil }, want: "needs the Cluster's IBMCloudManagedCluster"},
		{name: "nil pool", mutate: func(o *createRequestObjects) { o.pool = nil }, want: "needs the initial IBMCloudManagedMachinePool"},
		{name: "nil MachinePool", mutate: func(o *createRequestObjects) { o.mp = nil }, want: "needs the MachinePool"},
		{name: "not the initial pool", mutate: func(o *createRequestObjects) { o.pool.Spec.PoolName = "gpu" },
			want: `has spec.poolName "gpu"; BuildCreateClusterRequest takes the initial pool`},
		{name: "VPC not resolved yet", mutate: func(o *createRequestObjects) { o.infra.Status.VPC = infrav1.ResolvedResource{} },
			want: "has not resolved its VPC"},
		{name: "zone declared but not resolved yet", mutate: func(o *createRequestObjects) { o.infra.Status.Zones = o.infra.Status.Zones[:1] },
			want: "has not resolved the subnet of zone(s) eu-de-2, eu-de-3 yet"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			o := createRequestIKS()
			tc.mutate(&o)
			_, err := o.build()
			g.Expect(err).To(MatchError(ContainSubstring(tc.want)))
			var configErr *ConfigurationError
			g.Expect(errors.As(err, &configErr)).To(BeFalse(), "a missing input or a pending InfraCluster is not the user's to fix")
		})
	}
}

func TestConfigurationError(t *testing.T) {
	g := NewWithT(t)
	err := &ConfigurationError{Problems: []string{"first", "second"}}
	g.Expect(err.Error()).To(Equal("first; second"))
	g.Expect(strings.Count(err.Error(), ";")).To(Equal(1))
}
