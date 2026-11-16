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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/kubeconfig"

	vpcinfrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/vpc/v1beta2"

	. "github.com/onsi/gomega"
)

// The ClusterCache tests lock its scope: only a Cluster whose control plane is an IBMCloudManagedControlPlane gets a
// workload connection, also when another Cluster has a valid kubeconfig Secret and provisioned infrastructure.

func TestIsIBMCloudManagedCluster(t *testing.T) {
	testCases := []struct {
		name    string
		cluster *clusterv1.Cluster
		want    bool
	}{
		{name: "nil cluster", cluster: nil, want: false},
		{name: "managed control plane", cluster: clusterWithControlPlane(managedControlPlaneRef("dev")), want: true},
		{
			name:    "another control plane kind of the same group",
			cluster: clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.cluster.x-k8s.io", Kind: "KubeadmControlPlane", Name: "dev"}),
			want:    false,
		},
		{
			name:    "the managed kind name in another group",
			cluster: clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.example.com", Kind: "IBMCloudManagedControlPlane", Name: "dev"}),
			want:    false,
		},
		{name: "no control plane", cluster: clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{}), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(IsIBMCloudManagedCluster(tc.cluster)).To(Equal(tc.want))
		})
	}
}

func TestClusterCacheOptions(t *testing.T) {
	g := NewWithT(t)
	reader := testEnv.GetAPIReader()

	opts := clusterCacheOptions(reader, ClusterCacheOptions{WatchFilterValue: "team-a"})

	g.Expect(opts.SecretClient).To(BeIdenticalTo(reader), "kubeconfig Secrets are read uncached")
	g.Expect(opts.WatchFilterValue).To(Equal("team-a"))
	g.Expect(opts.ClusterFilter).NotTo(BeNil())
	g.Expect(opts.ClusterFilter(clusterWithControlPlane(managedControlPlaneRef("dev")))).To(BeTrue())
	g.Expect(opts.ClusterFilter(clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{}))).To(BeFalse())
	g.Expect(opts.Cache.DefaultTransform).NotTo(BeNil(), "managed fields are stripped")
	g.Expect(opts.Cache.Indexes).To(ConsistOf(HaveField("Field", clustercache.NodeProviderIDIndex.Field)))
	g.Expect(opts.Client.UserAgent).To(HavePrefix("cluster-api-provider-ibmcloud/"))
	g.Expect(opts.Client.Cache.DisableFor).To(ConsistOf(
		BeAssignableToTypeOf(&corev1.Pod{}),
		BeAssignableToTypeOf(&appsv1.DaemonSet{}),
		BeAssignableToTypeOf(&storagev1.VolumeAttachment{}),
		BeAssignableToTypeOf(&corev1.PersistentVolume{}),
		BeAssignableToTypeOf(&corev1.ConfigMap{}),
		BeAssignableToTypeOf(&corev1.Secret{}),
	))
}

func TestSetupClusterCacheNeedsAManager(t *testing.T) {
	g := NewWithT(t)
	clusterCache, err := SetupClusterCache(ctx, nil, ClusterCacheOptions{})
	g.Expect(err).To(MatchError(ContainSubstring("needs the controller manager")))
	g.Expect(clusterCache).To(BeNil())
}

func TestClusterCacheConnectsOnlyManagedClusters(t *testing.T) {
	g := NewWithT(t)
	mgr := newTestManager(t)
	clusterCache, err := SetupClusterCache(ctx, mgr, ClusterCacheOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	startTestManager(t, mgr)
	shutdownClusterCacheOnCleanup(t, clusterCache)

	namespace := newTestNamespace(t, "clustercache")
	managed := createTestCluster(t, namespace, "managed", managedControlPlaneRef("managed"))
	// A CAPIBM VPC cluster with a kubeadm control plane: the ClusterCache must ignore it although it could connect.
	other := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "vpc"},
		Spec: clusterv1.ClusterSpec{
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: vpcinfrav1.GroupVersion.Group, Kind: "IBMVPCCluster", Name: "vpc"},
			ControlPlaneRef:   clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.cluster.x-k8s.io", Kind: "KubeadmControlPlane", Name: "vpc"},
		},
	}
	g.Expect(testEnv.Create(ctx, other)).To(Succeed())

	// Both get a valid kubeconfig Secret for the envtest API server and provisioned infrastructure, the two things the
	// ClusterCache needs to connect; the Secret comes first, so that no connect attempt fails and backs off.
	for _, cluster := range []*clusterv1.Cluster{managed, other} {
		g.Expect(testEnv.Create(ctx, kubeconfig.GenerateSecret(cluster, kubeconfig.FromEnvTestConfig(testEnv.Config, cluster)))).To(Succeed())
		cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
		g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
	}

	g.Eventually(func() error {
		_, err := clusterCache.GetClient(ctx, client.ObjectKeyFromObject(managed))
		return err
	}, 60*time.Second, 250*time.Millisecond).Should(Succeed(), "the managed Cluster connects")
	g.Consistently(func() bool {
		_, err := clusterCache.GetClient(ctx, client.ObjectKeyFromObject(other))
		return errors.Is(err, clustercache.ErrClusterNotConnected)
	}, 5*time.Second, 250*time.Millisecond).Should(BeTrue(), "the VPC Cluster never gets an accessor")
}

// clusterWithControlPlane returns a Cluster whose spec.controlPlaneRef is ref.
func clusterWithControlPlane(ref clusterv1.ContractVersionedObjectReference) *clusterv1.Cluster {
	return &clusterv1.Cluster{Spec: clusterv1.ClusterSpec{ControlPlaneRef: ref}}
}

// shutdownClusterCacheOnCleanup stops the per-cluster caches of clusterCache when the test ends. They run on the
// ClusterCache's own context, not on the manager's, so stopping the test manager leaves their watches open, and the
// envtest API server would then not stop. Cluster API's implementation has a Shutdown method for this; it is not part
// of the ClusterCache interface.
func shutdownClusterCacheOnCleanup(t *testing.T, clusterCache clustercache.ClusterCache) {
	t.Helper()
	shutdowner, ok := clusterCache.(interface{ Shutdown() })
	if !ok {
		t.Fatalf("the ClusterCache %T has no Shutdown method; the test cannot stop its caches", clusterCache)
	}
	t.Cleanup(shutdowner.Shutdown)
}
