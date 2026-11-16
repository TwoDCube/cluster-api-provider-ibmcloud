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
	"net/http"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"
	"go.uber.org/mock/gomock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	rmmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager/mock"
	vpcmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc/mock"

	. "github.com/onsi/gomega"
)

// The InfraCluster controller tests run Reconcile against the envtest API server, with gomock VPC and Resource Manager
// clients behind a stub ClientBuilder. They lock the CAPI InfraCluster contract (status.initialization.provisioned,
// failureDomains, conditions), the sticky provisioned flag, pause, the absence of a finalizer, and the identity seams:
// the holder is the IBMCloudManagedCluster itself, the scope constructor resolves no identity, a resolution error is
// persisted on the object, and no path that must not call IBM (a paused Cluster or object) resolves an identity.
// gomock fails a test on any unexpected IBM call.

// ibmFakes are the gomock IBM clients of one test behind a stub builder.
type ibmFakes struct {
	vpc     *vpcmock.MockVpc
	rm      *rmmock.MockResourceManager
	builder *stubClientBuilder
}

// newIBMFakes returns gomock VPC and Resource Manager clients and a stub builder that returns them.
func newIBMFakes(t *testing.T) *ibmFakes {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	fakes := &ibmFakes{vpc: vpcmock.NewMockVpc(mockCtrl), rm: rmmock.NewMockResourceManager(mockCtrl)}
	fakes.builder = &stubClientBuilder{vpc: fakes.vpc, resourceManager: fakes.rm}
	return fakes
}

// okResponse is the DetailedResponse of a successful call.
func okResponse() *core.DetailedResponse {
	return &core.DetailedResponse{StatusCode: http.StatusOK}
}

// subnet returns a subnet of the test VPC in zone, with a public gateway and enough free addresses.
func subnet(id, zone string) *vpcv1.Subnet {
	return &vpcv1.Subnet{
		ID:                        ptr.To(id),
		Name:                      ptr.To("dev-" + id),
		Ipv4CIDRBlock:             ptr.To("10.240.0.0/24"),
		AvailableIpv4AddressCount: ptr.To[int64](250),
		PublicGateway:             &vpcv1.PublicGatewayReference{ID: ptr.To("pgw-" + id)},
		VPC:                       &vpcv1.VPCReference{ID: ptr.To(testVPCID), Name: ptr.To(testVPCName)},
		Zone:                      &vpcv1.ZoneReference{Name: ptr.To(zone)},
	}
}

// expectNetwork expects one successful validation of testInfraSpec: the resource group, the VPC and both subnets.
// times is how often (gomock.AnyTimes() semantics when anyTimes is true).
func (f *ibmFakes) expectNetwork(anyTimes bool) {
	calls := []*gomock.Call{
		f.rm.EXPECT().GetResourceGroup(gomock.Cond(func(o *resourcemanagerv2.GetResourceGroupOptions) bool {
			return ptr.Deref(o.ID, "") == testResourceGroupID
		})).Return(&resourcemanagerv2.ResourceGroup{ID: ptr.To(testResourceGroupID)}, okResponse(), nil),
		f.vpc.EXPECT().GetVPC(gomock.Any()).Return(&vpcv1.VPC{ID: ptr.To(testVPCID), Name: ptr.To(testVPCName)}, okResponse(), nil),
		f.vpc.EXPECT().GetSubnet(gomock.Cond(func(o *vpcv1.GetSubnetOptions) bool { return ptr.Deref(o.ID, "") == "sn-1" })).
			Return(subnet("sn-1", "us-south-1"), okResponse(), nil),
		f.vpc.EXPECT().GetSubnet(gomock.Cond(func(o *vpcv1.GetSubnetOptions) bool { return ptr.Deref(o.ID, "") == "sn-2" })).
			Return(subnet("sn-2", "us-south-2"), okResponse(), nil),
	}
	if anyTimes {
		for _, call := range calls {
			call.AnyTimes()
		}
	}
}

// reconcileInfra runs one Reconcile of the IBMCloudManagedCluster key.
func reconcileInfra(r *IBMCloudManagedClusterReconciler, key client.ObjectKey) (ctrl.Result, error) {
	return r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
}

func TestIBMCloudManagedClusterReconcileContract(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "infra-contract")
	cluster := createTestCluster(t, namespace, "dev", clusterv1.ContractVersionedObjectReference{})
	infra := createTestInfra(t, namespace, "dev", cluster)
	key := client.ObjectKeyFromObject(infra)
	fakes := newIBMFakes(t)
	resolver := &recordingResolver{}
	r := &IBMCloudManagedClusterReconciler{Client: uncachedClient(t), Resolver: resolver, ClientBuilder: fakes.builder}

	// The first reconcile only records that the object is not paused; the update event brings it back (CAPI's
	// EnsurePausedCondition contract). No IBM call yet: gomock has no expectation.
	result, err := reconcileInfra(r, key)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result).To(Equal(ctrl.Result{}))
	g.Expect(resolver.Holders()).To(BeEmpty())

	fakes.expectNetwork(false)
	result, err = reconcileInfra(r, key)
	g.Expect(err).NotTo(HaveOccurred())
	// Done requeues at infraRevalidatePoll (30 m ± 10 %).
	g.Expect(result.RequeueAfter).To(BeNumerically(">=", 27*time.Minute))
	g.Expect(result.RequeueAfter).To(BeNumerically("<", 33*time.Minute))

	persisted := getInfra(t, key)
	g.Expect(persisted.Finalizers).To(BeEmpty(), "the InfraCluster has no finalizer: CAPIBM creates nothing in IBM Cloud for it")
	g.Expect(persisted.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
	g.Expect(persisted.Status.FailureDomains).To(Equal([]infrav1.FailureDomain{{Name: "us-south-1"}, {Name: "us-south-2"}}))
	g.Expect(persisted.Status.VPC).To(Equal(infrav1.ResolvedResource{ID: testVPCID, Name: testVPCName}))
	g.Expect(persisted.Status.Zones).To(HaveLen(2))
	g.Expect(persisted.Status.Zones[0].PublicGateway).To(Equal(infrav1.PublicGatewayStateAttached))
	g.Expect(persisted.Status.Zones[0].AvailableIPv4Addresses).To(Equal(ptr.To[int32](250)))
	for _, conditionType := range []string{infrav1.IBMCloudManagedClusterReadyCondition, infrav1.IBMCloudManagedClusterNetworkReadyCondition} {
		condition := conditions.Get(persisted, conditionType)
		g.Expect(condition).NotTo(BeNil(), conditionType)
		g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), conditionType)
		g.Expect(condition.ObservedGeneration).To(Equal(persisted.Generation), conditionType)
	}
	g.Expect(conditions.Get(persisted, infrav1.IBMCloudManagedClusterNetworkReadyCondition).Reason).
		To(Equal(infrav1.IBMCloudManagedClusterNetworkReadyReason))
	g.Expect(conditions.IsFalse(persisted, infrav1.IBMCloudManagedClusterPausedCondition)).To(BeTrue())

	t.Run("the identity holder is the IBMCloudManagedCluster itself", func(t *testing.T) {
		g := NewWithT(t)
		holders := resolver.Holders()
		g.Expect(holders).To(HaveLen(1))
		holder, ok := holders[0].(*infrav1.IBMCloudManagedCluster)
		g.Expect(ok).To(BeTrue(), "the holder is an IBMCloudManagedCluster, got %T", holders[0])
		g.Expect(client.ObjectKeyFromObject(holder)).To(Equal(key))
		g.Expect(fakes.builder.Identities()).To(HaveEach(HaveField("Key", credentials.ControllerKey())))
	})

	t.Run("a second reconcile with an unchanged network writes nothing", func(t *testing.T) {
		g := NewWithT(t)
		fakes.expectNetwork(false)
		before := getInfra(t, key)
		_, err := reconcileInfra(r, key)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(getInfra(t, key).ResourceVersion).To(Equal(before.ResourceVersion), "no churn: a repeated reconcile writes nothing")
	})

	t.Run("a reference that disappears flips NetworkReady but never provisioned", func(t *testing.T) {
		g := NewWithT(t)
		fakes.rm.EXPECT().GetResourceGroup(gomock.Any()).Return(&resourcemanagerv2.ResourceGroup{ID: ptr.To(testResourceGroupID)}, okResponse(), nil)
		fakes.vpc.EXPECT().GetVPC(gomock.Any()).Return(&vpcv1.VPC{ID: ptr.To(testVPCID), Name: ptr.To(testVPCName)}, okResponse(), nil)
		fakes.vpc.EXPECT().GetSubnet(gomock.Any()).Return(nil, &core.DetailedResponse{StatusCode: http.StatusNotFound}, errors.New("Not Found"))

		result, err := reconcileInfra(r, key)

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically(">=", 13*time.Minute+30*time.Second), "terminal recheck")
		persisted := getInfra(t, key)
		g.Expect(persisted.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
		g.Expect(persisted.Status.Zones).To(HaveLen(2), "the last confirmed network stays published")
		networkReady := conditions.Get(persisted, infrav1.IBMCloudManagedClusterNetworkReadyCondition)
		g.Expect(networkReady.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(networkReady.Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason))
		g.Expect(conditions.IsFalse(persisted, infrav1.IBMCloudManagedClusterReadyCondition)).To(BeTrue())
	})
}

func TestIBMCloudManagedClusterReconcileMakesNoIBMCall(t *testing.T) {
	newReconciler := func(t *testing.T) *IBMCloudManagedClusterReconciler {
		t.Helper()
		return &IBMCloudManagedClusterReconciler{Client: uncachedClient(t), Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t}}
	}

	t.Run("object not found", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "infra-missing")
		result, err := reconcileInfra(newReconciler(t), types.NamespacedName{Namespace: namespace, Name: "missing"})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
	})

	t.Run("object without owner gets nothing", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "infra-unowned")
		infra := createTestInfra(t, namespace, "dev", nil)
		result, err := reconcileInfra(newReconciler(t), client.ObjectKeyFromObject(infra))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		persisted := getInfra(t, client.ObjectKeyFromObject(infra))
		g.Expect(persisted.ResourceVersion).To(Equal(infra.ResourceVersion), "nothing is written before the owner is known")
	})

	t.Run("a paused Cluster makes no IBM call", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "infra-paused")
		cluster := createTestCluster(t, namespace, "dev", clusterv1.ContractVersionedObjectReference{})
		cluster.Spec.Paused = ptr.To(true)
		g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
		infra := createTestInfra(t, namespace, "dev", cluster)
		r := newReconciler(t)

		// The Cluster may be read from a cache that has not seen the update yet; the paused condition settles.
		g.Eventually(func(g Gomega) {
			_, err := reconcileInfra(r, client.ObjectKeyFromObject(infra))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(conditions.IsTrue(getInfra(t, client.ObjectKeyFromObject(infra)), infrav1.IBMCloudManagedClusterPausedCondition)).To(BeTrue())
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
		_, err := reconcileInfra(r, client.ObjectKeyFromObject(infra))
		g.Expect(err).NotTo(HaveOccurred())
		persisted := getInfra(t, client.ObjectKeyFromObject(infra))
		g.Expect(conditions.Get(persisted, infrav1.IBMCloudManagedClusterNetworkReadyCondition)).To(BeNil())
		g.Expect(persisted.Status.Initialization.Provisioned).To(BeNil())
	})

	t.Run("a paused annotation on the object makes no IBM call", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "infra-paused-annotation")
		cluster := createTestCluster(t, namespace, "dev", clusterv1.ContractVersionedObjectReference{})
		infra := &infrav1.IBMCloudManagedCluster{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   namespace,
				Name:        "dev",
				Annotations: map[string]string{clusterv1.PausedAnnotation: ""},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: cluster.Name, UID: cluster.UID,
				}},
			},
			Spec: testInfraSpec(),
		}
		g.Expect(testEnv.Create(ctx, infra)).To(Succeed())
		r := newReconciler(t)
		for range 2 {
			_, err := reconcileInfra(r, client.ObjectKeyFromObject(infra))
			g.Expect(err).NotTo(HaveOccurred())
		}
		g.Expect(conditions.IsTrue(getInfra(t, client.ObjectKeyFromObject(infra)), infrav1.IBMCloudManagedClusterPausedCondition)).To(BeTrue())
	})

	t.Run("a deleting object needs no cleanup", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "infra-deleting")
		cluster := createTestCluster(t, namespace, "dev", clusterv1.ContractVersionedObjectReference{})
		infra := createTestInfra(t, namespace, "dev", cluster)
		key := client.ObjectKeyFromObject(infra)
		r := newReconciler(t)
		// The Paused condition first, so that the deletion is the only thing left to look at.
		_, err := reconcileInfra(r, key)
		g.Expect(err).NotTo(HaveOccurred())

		// A test finalizer keeps the deleting object visible; the controller itself never adds one.
		const testFinalizer = "test.cluster.x-k8s.io/hold"
		held := getInfra(t, key)
		held.Finalizers = []string{testFinalizer}
		g.Expect(testEnv.Update(ctx, held)).To(Succeed())
		g.Expect(testEnv.Delete(ctx, held)).To(Succeed())
		deleting := getInfra(t, key)
		g.Expect(deleting.DeletionTimestamp.IsZero()).To(BeFalse())

		result, err := reconcileInfra(r, key)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		g.Expect(getInfra(t, key).Finalizers).To(Equal([]string{testFinalizer}))

		deleting.Finalizers = nil
		g.Expect(testEnv.Update(ctx, deleting)).To(Succeed())
	})
}

func TestIBMCloudManagedClusterLazyResolution(t *testing.T) {
	t.Run("the scope constructor resolves no identity and builds no client", func(t *testing.T) {
		g := NewWithT(t)
		params := &managedscope.ScopeParams{Resolver: failIfCalledResolver{t: t}, ClientBuilder: failIfCalledBuilder{t: t}}
		scope, err := managedscope.NewClusterScope(params, &clusterv1.Cluster{}, &infrav1.IBMCloudManagedCluster{Spec: testInfraSpec()})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(scope).NotTo(BeNil())
	})

	t.Run("an identity resolution error is persisted on the object", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "infra-resolve-error")
		cluster := createTestCluster(t, namespace, "dev", clusterv1.ContractVersionedObjectReference{})
		infra := createTestInfra(t, namespace, "dev", cluster)
		key := client.ObjectKeyFromObject(infra)
		fakes := newIBMFakes(t) // no expectations: any IBM call fails the test
		resolver := &recordingResolver{err: errors.New("the holder lost its identity")}
		r := &IBMCloudManagedClusterReconciler{Client: uncachedClient(t), Resolver: resolver, ClientBuilder: fakes.builder}
		_, err := reconcileInfra(r, key) // records Paused=False
		g.Expect(err).NotTo(HaveOccurred())

		_, err = reconcileInfra(r, key)

		g.Expect(err).To(MatchError(ContainSubstring("the holder lost its identity")))
		g.Expect(fakes.builder.Identities()).To(BeEmpty(), "no client is built without an identity")
		persisted := getInfra(t, key)
		networkReady := conditions.Get(persisted, infrav1.IBMCloudManagedClusterNetworkReadyCondition)
		g.Expect(networkReady).NotTo(BeNil(), "the deferred patch persists the condition the step set")
		g.Expect(networkReady.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(networkReady.Reason).To(Equal(infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason))
		g.Expect(networkReady.Message).To(ContainSubstring("cannot resolve the IBM Cloud credentials"))
	})
}

func TestIBMCloudManagedClusterReconcilerSetupWithManager(t *testing.T) {
	t.Run("missing dependencies are named", func(t *testing.T) {
		g := NewWithT(t)
		err := (&IBMCloudManagedClusterReconciler{}).SetupWithManager(ctx, newTestManager(t))
		g.Expect(err).To(MatchError(ContainSubstring("Client is nil")))
		g.Expect(err).To(MatchError(ContainSubstring("Resolver is nil; pass credentials.ControllerOnly{}")))
		g.Expect(err).To(MatchError(ContainSubstring("ClientBuilder is nil")))
	})

	t.Run("the watches reconcile a new object to provisioned", func(t *testing.T) {
		g := NewWithT(t)
		mgr := newTestManager(t)
		fakes := newIBMFakes(t)
		fakes.expectNetwork(true)
		r := &IBMCloudManagedClusterReconciler{Client: mgr.GetClient(), Resolver: &recordingResolver{}, ClientBuilder: fakes.builder}
		g.Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
		startTestManager(t, mgr)

		namespace := newTestNamespace(t, "infra-watch")
		cluster := createTestCluster(t, namespace, "dev", clusterv1.ContractVersionedObjectReference{})
		infra := createTestInfra(t, namespace, "dev", cluster)

		g.Eventually(func(g Gomega) {
			persisted := getInfra(t, client.ObjectKeyFromObject(infra))
			g.Expect(persisted.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
			g.Expect(conditions.IsTrue(persisted, infrav1.IBMCloudManagedClusterReadyCondition)).To(BeTrue())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		// Pausing the Cluster reaches the object through the Cluster watch.
		cluster.Spec.Paused = ptr.To(true)
		g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
		g.Eventually(func(g Gomega) {
			g.Expect(conditions.IsTrue(getInfra(t, client.ObjectKeyFromObject(infra)), infrav1.IBMCloudManagedClusterPausedCondition)).To(BeTrue())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})
}
