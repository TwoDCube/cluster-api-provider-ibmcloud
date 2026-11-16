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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"

	. "github.com/onsi/gomega"
)

// This file holds the object builders and the envtest helpers of the managed controller tests. Later work packages
// reuse them.

const (
	// testResourceGroupID is a synthetic resource group ID of the right shape (32 hex characters).
	testResourceGroupID = "0123456789abcdef0123456789abcdef"
	// testVPCID and testVPCName name the synthetic VPC of the tests.
	testVPCID   = "r006-test-vpc"
	testVPCName = "dev-vpc"
	// testRegion is the region of the tests.
	testRegion = "us-south"
)

// newTestNamespace creates a namespace for one test and deletes it when the test ends. envtest runs no namespace
// controller, so the objects inside stay until the API server stops; every test uses its own namespace.
func newTestNamespace(t *testing.T, generateName string) string {
	t.Helper()
	g := NewWithT(t)
	ns, err := testEnv.CreateNamespace(ctx, generateName)
	g.Expect(err).NotTo(HaveOccurred())
	t.Cleanup(func() {
		g.Expect(testEnv.Cleanup(ctx, &corev1.Namespace{ObjectMeta: ns.ObjectMeta})).To(Succeed())
	})
	return ns.Name
}

// createTestCluster creates a CAPI Cluster whose infrastructureRef names the IBMCloudManagedCluster of the same name
// and whose controlPlaneRef is controlPlaneRef (zero: none).
//
// Parameters:
//   - t is the test.
//   - namespace and name name the Cluster.
//   - controlPlaneRef is the Cluster's spec.controlPlaneRef.
func createTestCluster(t *testing.T, namespace, name string, controlPlaneRef clusterv1.ContractVersionedObjectReference) *clusterv1.Cluster {
	t.Helper()
	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: clusterv1.ClusterSpec{
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group,
				Kind:     ibmCloudManagedClusterKind,
				Name:     name,
			},
			ControlPlaneRef: controlPlaneRef,
		},
	}
	NewWithT(t).Expect(testEnv.Create(ctx, cluster)).To(Succeed())
	return cluster
}

// testInfraSpec is a valid IBMCloudManagedCluster spec with the VPC and two subnets given by ID.
func testInfraSpec() infrav1.IBMCloudManagedClusterSpec {
	return infrav1.IBMCloudManagedClusterSpec{
		Region:        testRegion,
		ResourceGroup: infrav1.ResourceGroupReference{ID: testResourceGroupID},
		VPC:           infrav1.VPCSource{Type: infrav1.SourceTypeReference, Reference: infrav1.ResourceIdentifier{ID: testVPCID}},
		Zones: []infrav1.ZoneSubnet{
			{Name: "us-south-1", Subnet: infrav1.ResourceIdentifier{ID: "sn-1"}},
			{Name: "us-south-2", Subnet: infrav1.ResourceIdentifier{ID: "sn-2"}},
		},
	}
}

// createTestInfra creates an IBMCloudManagedCluster named like cluster; when cluster is not nil it is owned by it, as
// the Cluster controller would set it.
func createTestInfra(t *testing.T, namespace, name string, cluster *clusterv1.Cluster) *infrav1.IBMCloudManagedCluster {
	t.Helper()
	infra := &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       testInfraSpec(),
	}
	if cluster != nil {
		infra.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "Cluster",
			Name:       cluster.Name,
			UID:        cluster.UID,
		}}
	}
	NewWithT(t).Expect(testEnv.Create(ctx, infra)).To(Succeed())
	return infra
}

// getInfra reads the IBMCloudManagedCluster from the API server, uncached.
func getInfra(t *testing.T, key client.ObjectKey) *infrav1.IBMCloudManagedCluster {
	t.Helper()
	infra := &infrav1.IBMCloudManagedCluster{}
	NewWithT(t).Expect(testEnv.GetAPIReader().Get(ctx, key, infra)).To(Succeed())
	return infra
}

// uncachedClient returns a client that reads straight from the envtest API server. Tests that call Reconcile directly
// use it, so that an object a test has just written is always visible to the reconciler (the shared manager's cache
// may lag behind).
func uncachedClient(t *testing.T) client.Client {
	t.Helper()
	c, err := client.New(testEnv.Config, client.Options{Scheme: scheme.Scheme})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return c
}

// newTestManager returns a manager on the envtest API server for tests that need watches. Controller name validation
// is off, because controller-runtime keeps controller names process-wide and several tests register the same
// controller.
func newTestManager(t *testing.T) manager.Manager {
	t.Helper()
	mgr, err := ctrl.NewManager(testEnv.Config, ctrl.Options{
		Scheme:     scheme.Scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return mgr
}

// startTestManager starts mgr and stops it when the test ends.
func startTestManager(t *testing.T, mgr manager.Manager) {
	t.Helper()
	mgrCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(mgrCtx); err != nil {
			t.Errorf("test manager stopped with an error: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}
