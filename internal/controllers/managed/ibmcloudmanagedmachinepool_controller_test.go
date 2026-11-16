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
	"time"

	"go.uber.org/mock/gomock"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"

	. "github.com/onsi/gomega"
)

// The pool controller tests call Reconcile on a fake management cluster with a gomock IBM Cloud Kubernetes Service and
// the identity seam fakes of fakes_test.go. They lock the skeleton: owners, pause, the deferred patch of every
// condition (and no failureReason or failureMessage on any path), the Deleting condition, which object is the
// identity holder, and that a resolver error is persisted without an IBM call. The pool logic itself is tested in
// pkg/cloud/scope/managed.

const (
	mpTestNamespace = "capi-managed"
	mpTestCluster   = "dev"
	mpTestClusterID = "c0testcluster0000001"
	mpTestPoolID    = mpTestClusterID + "-a1b2c3d"
	mpTestAccount   = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	mpTestFlavor    = "bx2.4x16"
)

// mpTestNow is the fake clock's start.
var mpTestNow = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// machinePoolFixture holds the management-cluster objects of one pool and the fakes of its reconciler.
type machinePoolFixture struct {
	t           *testing.T
	ibm         *ksmock.MockKubernetesService
	builder     *stubClientBuilder
	resolver    *recordingResolver
	recorder    *record.FakeRecorder
	clock       *clocktesting.FakeClock
	cluster     *clusterv1.Cluster
	machinePool *clusterv1.MachinePool
	pool        *infrav1.IBMCloudManagedMachinePool
	cp          *controlplanev1.IBMCloudManagedControlPlane
	infra       *infrav1.IBMCloudManagedCluster
	// funcs intercept the management client of the reconciler.
	funcs interceptor.Funcs
	// clusterCache is the reconciler's ClusterCache; the empty fake (never connected) unless a test sets one.
	clusterCache clustercache.ClusterCache
	// mgmt is the fake management cluster, read by the test without interceptors.
	mgmt client.Client
}

// newMachinePoolFixture returns an IKS Cluster with two zones whose IBM cluster exists, and a pool "workers" of two
// workers per zone owned by the MachinePool "workers".
func newMachinePoolFixture(t *testing.T) *machinePoolFixture {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	f := &machinePoolFixture{
		t:            t,
		ibm:          ksmock.NewMockKubernetesService(mockCtrl),
		resolver:     &recordingResolver{},
		recorder:     record.NewFakeRecorder(100),
		clock:        clocktesting.NewFakeClock(mpTestNow),
		clusterCache: clustercache.NewFakeEmptyClusterCache(),
	}
	f.builder = &stubClientBuilder{kubernetes: f.ibm}
	labels := map[string]string{clusterv1.ClusterNameLabel: mpTestCluster}
	f.cluster = &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: mpTestNamespace, Name: mpTestCluster, UID: "cluster-uid"},
		Spec: clusterv1.ClusterSpec{
			ControlPlaneRef: managedControlPlaneRef(mpTestCluster),
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedClusterKind, Name: mpTestCluster,
			},
		},
	}
	f.infra = &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: mpTestNamespace, Name: mpTestCluster, Labels: labels},
		Spec:       testInfraSpec(),
		Status: infrav1.IBMCloudManagedClusterStatus{
			VPC: infrav1.ResolvedResource{ID: testVPCID},
			Zones: []infrav1.ResolvedZone{
				{Name: "us-south-1", Subnet: infrav1.ResolvedResource{ID: "sn-1"}},
				{Name: "us-south-2", Subnet: infrav1.ResolvedResource{ID: "sn-2"}},
			},
		},
	}
	f.cp = &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: mpTestNamespace, Name: mpTestCluster, Labels: labels},
		Spec:       controlplanev1.IBMCloudManagedControlPlaneSpec{Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.36"},
		Status: controlplanev1.IBMCloudManagedControlPlaneStatus{
			ClusterID:           mpTestClusterID,
			ClusterCRN:          "crn:v1:bluemix:public:containers-kubernetes:us-south:a/" + mpTestAccount + ":" + mpTestClusterID + "::",
			State:               "normal",
			DistributionVersion: "1.36.5_1537",
			Initialization:      controlplanev1.IBMCloudManagedControlPlaneInitializationStatus{ControlPlaneInitialized: ptr.To(true)},
		},
	}
	f.machinePool = &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: mpTestNamespace, Name: "workers", Labels: labels, UID: "mp-uid"},
		Spec: clusterv1.MachinePoolSpec{
			ClusterName: mpTestCluster,
			Replicas:    ptr.To[int32](4),
			Template: clusterv1.MachineTemplateSpec{Spec: clusterv1.MachineSpec{
				ClusterName: mpTestCluster,
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedMachinePoolKind, Name: "workers",
				},
			}},
		},
	}
	f.pool = &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mpTestNamespace, Name: "workers", Labels: labels, UID: "pool-uid",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool", Name: "workers", UID: "mp-uid"}},
			Finalizers:      []string{infrav1.IBMCloudManagedMachinePoolFinalizer},
		},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName: "workers",
			Flavor:   mpTestFlavor,
			Zones:    []infrav1.WorkerPoolZone{{Name: "us-south-1"}, {Name: "us-south-2"}},
		},
		// Earlier reconciles added the finalizer and recorded that the pool is not paused: each of them only does that and
		// returns (CAPI's EnsureFinalizer and EnsurePausedCondition contracts), which the skeleton tests cover separately.
		Status: infrav1.IBMCloudManagedMachinePoolStatus{Conditions: []metav1.Condition{{
			Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse, Reason: clusterv1.NotPausedReason,
			LastTransitionTime: metav1.NewTime(mpTestNow),
		}}},
	}
	return f
}

// machinePoolScheme registers every type of the management cluster.
func machinePoolScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, clusterv1.AddToScheme, infrav1.AddToScheme, controlplanev1.AddToScheme} {
		NewWithT(t).Expect(add(scheme)).To(Succeed())
	}
	return scheme
}

// reconciler builds the fake management cluster from the fixture objects (nil ones left out) and returns the
// reconciler over it.
func (f *machinePoolFixture) reconciler() *IBMCloudManagedMachinePoolReconciler {
	f.t.Helper()
	var objs []client.Object
	for _, o := range []client.Object{f.cluster, f.machinePool, f.pool, f.cp, f.infra} {
		if o != nil && !isNilObject(o) {
			objs = append(objs, o)
		}
	}
	f.mgmt = fake.NewClientBuilder().WithScheme(machinePoolScheme(f.t)).WithObjects(objs...).
		WithStatusSubresource(&infrav1.IBMCloudManagedMachinePool{}, &clusterv1.Cluster{}).Build()
	c := interceptor.NewClient(f.mgmt.(client.WithWatch), f.funcs)
	return &IBMCloudManagedMachinePoolReconciler{
		Client: c, APIReader: c, Recorder: f.recorder, Resolver: f.resolver, ClientBuilder: f.builder,
		ClusterCache: f.clusterCache, Clock: f.clock,
	}
}

// isNilObject reports whether o is a typed nil pointer, as a fixture field set to nil is.
func isNilObject(o client.Object) bool {
	switch v := o.(type) {
	case *clusterv1.Cluster:
		return v == nil
	case *clusterv1.MachinePool:
		return v == nil
	case *infrav1.IBMCloudManagedMachinePool:
		return v == nil
	case *controlplanev1.IBMCloudManagedControlPlane:
		return v == nil
	case *infrav1.IBMCloudManagedCluster:
		return v == nil
	}
	return false
}

// reconcile runs one Reconcile of the pool.
func (f *machinePoolFixture) reconcile(r *IBMCloudManagedMachinePoolReconciler) (ctrl.Result, error) {
	return r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: mpTestNamespace, Name: "workers"}})
}

// stored reads the pool back from the fake management cluster.
func (f *machinePoolFixture) stored() *infrav1.IBMCloudManagedMachinePool {
	f.t.Helper()
	pool := &infrav1.IBMCloudManagedMachinePool{}
	NewWithT(f.t).Expect(f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: "workers"}, pool)).To(Succeed())
	return pool
}

// expectReadyPool expects the observation of a ready pool with four workers.
func (f *machinePoolFixture) expectReadyPool() {
	pool := kubernetesservice.WorkerPool{ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2,
		Zones: []kubernetesservice.WorkerPoolZone{{ID: "us-south-1", WorkerCount: 2}, {ID: "us-south-2", WorkerCount: 2}}}
	workers := make([]kubernetesservice.Worker, 0, 4)
	for i, zone := range []string{"us-south-1", "us-south-2", "us-south-1", "us-south-2"} {
		workers = append(workers, kubernetesservice.Worker{ID: "w" + string(rune('0'+i)), PoolID: mpTestPoolID, Zone: zone,
			ActualState: "deployed", HealthState: "normal", KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"})
	}
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{pool}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(workers, nil)
}

func TestIBMCloudManagedMachinePoolReconcilerValidate(t *testing.T) {
	g := NewWithT(t)
	err := (&IBMCloudManagedMachinePoolReconciler{}).validate()
	for _, field := range []string{"Client", "APIReader", "Recorder", "Resolver", "ClientBuilder", "ClusterCache", "Clock"} {
		g.Expect(err).To(MatchError(ContainSubstring("IBMCloudManagedMachinePoolReconciler." + field + " is nil")))
	}
	g.Expect(newMachinePoolFixture(t).reconciler().validate()).To(Succeed())
}

func TestRequireMachinePoolCRD(t *testing.T) {
	g := NewWithT(t)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{clusterv1.GroupVersion})
	g.Expect(requireMachinePoolCRD(mapper)).To(MatchError(ContainSubstring("needs the Cluster API MachinePool.cluster.x-k8s.io CRD")))
	mapper.Add(clusterv1.GroupVersion.WithKind("MachinePool"), meta.RESTScopeNamespace)
	g.Expect(requireMachinePoolCRD(mapper)).To(Succeed())
}

func TestIBMCloudManagedMachinePoolSetupWithManager(t *testing.T) {
	t.Run("fails fast without the MachinePool CRD", func(t *testing.T) {
		g := NewWithT(t)
		mgr, err := ctrl.NewManager(testEnv.Config, ctrl.Options{
			Scheme:     clientgoscheme.Scheme,
			Metrics:    metricsserver.Options{BindAddress: "0"},
			Controller: config.Controller{SkipNameValidation: ptr.To(true)},
			MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
				return meta.NewDefaultRESTMapper(nil), nil
			},
		})
		g.Expect(err).NotTo(HaveOccurred())
		err = newMachinePoolFixture(t).reconciler().SetupWithManager(ctx, mgr)
		g.Expect(err).To(MatchError(ContainSubstring("MachinePool")))
	})
	t.Run("fails fast without a dependency", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect((&IBMCloudManagedMachinePoolReconciler{}).SetupWithManager(ctx, newTestManager(t))).
			To(MatchError(ContainSubstring("ClusterCache is nil")))
	})
	t.Run("registers with the CRDs of envtest", func(t *testing.T) {
		g := NewWithT(t)
		mgr := newTestManager(t)
		clusterCache, err := SetupClusterCache(ctx, mgr, ClusterCacheOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		r := newMachinePoolFixture(t).reconciler()
		r.ClusterCache = clusterCache
		g.Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
	})
}

func TestIBMCloudManagedMachinePoolReconcileSkeleton(t *testing.T) {
	t.Run("a pool no MachinePool owns causes no call", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.pool.OwnerReferences, f.pool.Finalizers = nil, nil
		f.pool.Status.Conditions = nil
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		g.Expect(f.stored().Status.Conditions).To(BeEmpty(), "nothing is patched")
		g.Expect(f.resolver.Holders()).To(BeEmpty())
	})
	t.Run("the first reconcile only adds the finalizer", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.pool.Finalizers = nil
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		g.Expect(f.stored().Finalizers).To(ConsistOf(infrav1.IBMCloudManagedMachinePoolFinalizer))
		g.Expect(f.resolver.Holders()).To(BeEmpty())
	})
	t.Run("the first reconcile only records that the pool is not paused", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.pool.Status.Conditions = nil
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
		g.Expect(conditions.IsFalse(f.stored(), clusterv1.PausedCondition)).To(BeTrue())
		g.Expect(f.resolver.Holders()).To(BeEmpty())
	})
	t.Run("an owner MachinePool that does not exist yet is waited for", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.machinePool = nil
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.resolver.Holders()).To(BeEmpty())
	})
	t.Run("a MachinePool without its Cluster is waited for", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.cluster = nil
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.resolver.Holders()).To(BeEmpty())
	})
	t.Run("a pool that is gone is ignored", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.pool = nil
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{}))
	})
	t.Run("a paused Cluster makes zero IBM calls", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.cluster.Spec.Paused = ptr.To(true)
		f.resolver = nil
		r := f.reconciler()
		r.Resolver = failIfCalledResolver{t: t}
		r.ClientBuilder = failIfCalledBuilder{t: t}
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(conditions.IsTrue(f.stored(), clusterv1.PausedCondition)).To(BeTrue())
	})
	t.Run("a live pool is not deleting", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.expectReadyPool()
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically("~", 5*time.Minute, 30*time.Second), "steadyStatePoll")
		deleting := conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolDeletingCondition)
		g.Expect(deleting.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(deleting.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolNotDeletingReason))
	})
}

// TestIBMCloudManagedMachinePoolReconcileClusterDeletion reconciles a pool that is not being deleted itself while its
// Cluster or control plane is: CAPI deletes the MachinePool first, and that event reaches the pool before the
// MachinePool controller deletes it. Deleting the IBM cluster removes every worker, so such a reconcile makes zero IBM
// and zero workload-cluster calls.
func TestIBMCloudManagedMachinePoolReconcileClusterDeletion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(f *machinePoolFixture)
	}{
		{name: "a live pool of a deleting Cluster makes zero IBM calls", modify: func(f *machinePoolFixture) {
			f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
			f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
			f.machinePool.Spec.Replicas = ptr.To[int32](6)
		}},
		{name: "a live pool of a deleting control plane makes zero IBM calls", modify: func(f *machinePoolFixture) {
			f.cp.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
			f.cp.Finalizers = []string{"test"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newMachinePoolFixture(t)
			tc.modify(f)
			r := f.reconciler()
			r.Resolver, r.ClientBuilder, r.ClusterCache = failIfCalledResolver{t: t}, failIfCalledBuilder{t: t}, noWorkloadCallsClusterCache{t: t}
			// gomock fails on any IBM call: no ListWorkerPools, no CreateWorkerPool, no ResizeWorkerPool.
			result, err := f.reconcile(r)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(BeNumerically("~", 5*time.Minute, 30*time.Second), "steadyStatePoll; the pool's own deletion follows")
			g.Expect(conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)).To(BeNil(),
				"WorkerPoolReady is left as it was")
		})
	}
}

func TestIBMCloudManagedMachinePoolReconcilePublishes(t *testing.T) {
	t.Run("spec and status are persisted", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.expectReadyPool()
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		stored := f.stored()
		g.Expect(stored.Spec.ProviderIDList).To(HaveLen(4))
		g.Expect(stored.Spec.ProviderIDList[0]).To(Equal("ibm://" + mpTestAccount + "///" + mpTestClusterID + "/w0"))
		g.Expect(stored.Status.Replicas).To(Equal(ptr.To[int32](4)))
		g.Expect(stored.Status.Ready).To(Equal(ptr.To(true)))
		g.Expect(stored.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
		g.Expect(conditions.IsTrue(stored, infrav1.IBMCloudManagedMachinePoolReadyCondition)).To(BeTrue(), "Ready summarizes WorkerPoolReady")
	})
	t.Run("the holder is the IBMCloudManagedCluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.expectReadyPool()
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		holders := f.resolver.Holders()
		g.Expect(holders).To(HaveLen(1))
		g.Expect(holders[0]).To(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{}))
		g.Expect(holders[0].GetName()).To(Equal(mpTestCluster))
		g.Expect(f.builder.Identities()).To(ConsistOf(HaveField("Key", credentials.ControllerKey())))
	})
	t.Run("a resolver error is persisted without an IBM call", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.resolver.err = errors.New("no credentials")
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).To(MatchError(ContainSubstring("no credentials")))
		ready := conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(ready.Reason).To(Equal(managedscope.MachinePoolIBMAPIErrorReason))
		g.Expect(f.builder.Identities()).To(BeEmpty())
	})
	t.Run("a failed patch is returned", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.expectReadyPool()
		f.funcs = interceptor.Funcs{SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			return errors.New("status patch refused")
		}}
		_, err := f.reconcile(f.reconciler())
		g.Expect(err).To(MatchError(ContainSubstring("status patch refused")))
	})
}

func TestIBMCloudManagedMachinePoolReconcileHasNoFailureFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expect func(f *machinePoolFixture)
	}{
		{name: "ready pool", expect: func(f *machinePoolFixture) { f.expectReadyPool() }},
		{name: "permission denied", expect: func(f *machinePoolFixture) {
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return(nil,
				&kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkerPools, StatusCode: http.StatusForbidden})
		}},
		{name: "server error", expect: func(f *machinePoolFixture) {
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return(nil,
				&kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkerPools, StatusCode: http.StatusInternalServerError})
		}},
		{name: "create refused", expect: func(f *machinePoolFixture) {
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return(nil, nil)
			f.ibm.EXPECT().CreateWorkerPool(gomock.Any(), gomock.Any()).Return("",
				&kubernetesservice.APIError{Operation: kubernetesservice.OpCreateWorkerPool, StatusCode: http.StatusBadRequest, Code: "E0001"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newMachinePoolFixture(t)
			tc.expect(f)
			_, _ = f.reconcile(f.reconciler())
			stored := &unstructured.Unstructured{}
			stored.SetGroupVersionKind(infrav1.GroupVersion.WithKind(ibmCloudManagedMachinePoolKind))
			g.Expect(f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: "workers"}, stored)).To(Succeed())
			status, _, err := unstructured.NestedMap(stored.Object, "status")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(status).NotTo(BeEmpty(), "the reconcile patched status")
			g.Expect(status).NotTo(HaveKey("failureReason"))
			g.Expect(status).NotTo(HaveKey("failureMessage"))
		})
	}
}

func TestIBMCloudManagedMachinePoolMapFunctions(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	r := f.reconciler()
	other := f.pool.DeepCopy()
	other.Name, other.ResourceVersion = "another", ""
	g.Expect(f.mgmt.Create(context.Background(), other)).To(Succeed())
	foreign := f.pool.DeepCopy()
	foreign.Name, foreign.ResourceVersion, foreign.Labels = "foreign", "", map[string]string{clusterv1.ClusterNameLabel: "prod"}
	g.Expect(f.mgmt.Create(context.Background(), foreign)).To(Succeed())

	want := []ctrl.Request{
		{NamespacedName: client.ObjectKey{Namespace: mpTestNamespace, Name: "another"}},
		{NamespacedName: client.ObjectKey{Namespace: mpTestNamespace, Name: "workers"}},
	}
	g.Expect(r.clusterToIBMCloudManagedMachinePools(context.Background(), f.cluster)).To(Equal(want))
	g.Expect(r.controlPlaneToIBMCloudManagedMachinePools(context.Background(), f.cp)).To(Equal(want))
	unlabelled := f.cp.DeepCopy()
	unlabelled.Labels = nil
	g.Expect(r.controlPlaneToIBMCloudManagedMachinePools(context.Background(), unlabelled)).To(BeEmpty())
}
