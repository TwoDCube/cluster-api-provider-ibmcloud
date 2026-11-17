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
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"sigs.k8s.io/controller-runtime/pkg/event"
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

// mpTestOtherWorker is a worker of another IBM pool of the test cluster in state; it is not this pool's, but every
// pool's reconcile lists it, and while it is transitional it holds every worker removal of the cluster, because
// CheckDisruption removes workers only from a settled cluster.
func mpTestOtherWorker(state string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: "o2", PoolID: "other-pool", Zone: "us-south-1", ActualState: state,
		KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"}
}

// expectListing expects the observation of the ready test pool "workers" whose IBM cluster also lists extra.
func (f *machinePoolFixture) expectListing(extra ...kubernetesservice.Worker) {
	pool := kubernetesservice.WorkerPool{ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2,
		Zones: []kubernetesservice.WorkerPoolZone{{ID: "us-south-1", WorkerCount: 2}, {ID: "us-south-2", WorkerCount: 2}}}
	workers := make([]kubernetesservice.Worker, 0, 4+len(extra))
	for i, zone := range []string{"us-south-1", "us-south-2", "us-south-1", "us-south-2"} {
		workers = append(workers, kubernetesservice.Worker{ID: "w" + string(rune('0'+i)), PoolID: mpTestPoolID, Zone: zone,
			ActualState: "deployed", HealthState: "normal", KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"})
	}
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{pool}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(append(workers, extra...), nil)
}

// TestIBMCloudManagedMachinePoolFirstSeenTimes checks that the reconciler keeps its first-seen map behind
// MachinePoolScopeInput.TransitionalSince current from the listings of the normal flow, so that a worker of another
// pool that stays transitional stops blocking removals once WorkerStuckAfter has passed since it was first seen; the
// pool deletion's use of it is tested with the deletion.
func TestIBMCloudManagedMachinePoolFirstSeenTimes(t *testing.T) {
	const clusterUID = "cluster-uid"
	t.Run("another pool's provision_pending worker keeps its first-seen time until it is deployed", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		r := f.reconciler()

		f.expectListing(mpTestOtherWorker("provision_pending"))
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(r.transitional.snapshot(clusterUID, f.clock.Now())).To(Equal(map[string]time.Time{"o2": mpTestNow}))

		f.clock.Step(30 * time.Minute)
		f.expectListing(mpTestOtherWorker("provisioning"))
		_, err = f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(r.transitional.snapshot(clusterUID, f.clock.Now())).To(Equal(map[string]time.Time{"o2": mpTestNow}),
			"the first time is kept while the worker stays transitional")

		f.clock.Step(time.Minute)
		f.expectListing(mpTestOtherWorker("deployed"))
		_, err = f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(r.transitional.snapshot(clusterUID, f.clock.Now())).To(BeNil(), "the entry is dropped once the worker is deployed")
	})
	t.Run("a failed listing keeps the entries", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		r := f.reconciler()
		r.transitional.observe(clusterUID, []kubernetesservice.Worker{mpTestOtherWorker("provisioning")}, mpTestNow)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{{
			ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2,
		}}, nil)
		f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(nil,
			&kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkers, StatusCode: http.StatusInternalServerError})
		_, err := f.reconcile(r)
		g.Expect(err).To(HaveOccurred())
		g.Expect(r.transitional.snapshot(clusterUID, f.clock.Now())).To(Equal(map[string]time.Time{"o2": mpTestNow}))
	})
	t.Run("a deleting Cluster drops its entries without an IBM call", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
		f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
		r := f.reconciler()
		r.Resolver, r.ClientBuilder = failIfCalledResolver{t: t}, failIfCalledBuilder{t: t}
		r.transitional.observe(clusterUID, []kubernetesservice.Worker{mpTestOtherWorker("provisioning")}, mpTestNow)
		r.transitional.observe("another-cluster", []kubernetesservice.Worker{mpTestOtherWorker("provisioning")}, mpTestNow)
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(r.transitional.snapshot(clusterUID, f.clock.Now())).To(BeNil())
		g.Expect(r.transitional.snapshot("another-cluster", f.clock.Now())).To(HaveKey("o2"), "other Clusters keep theirs")
	})
	t.Run("concurrent reconciles of two pools of one Cluster share the map", testFirstSeenConcurrentPools)
}

// testFirstSeenConcurrentPools reconciles two pools of one Cluster in parallel with one reconciler, as a controller with
// several workers does; `go test -race` reports any unsynchronized access to the first-seen map.
func testFirstSeenConcurrentPools(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	f.pool.Status.PoolID = mpTestPoolID
	r := f.reconciler()
	spareMachinePool := f.machinePool.DeepCopy()
	spareMachinePool.Name, spareMachinePool.UID, spareMachinePool.ResourceVersion = "spare", "spare-mp-uid", ""
	spareMachinePool.Spec.Template.Spec.InfrastructureRef.Name = "spare"
	g.Expect(f.mgmt.Create(context.Background(), spareMachinePool)).To(Succeed())
	spare := f.pool.DeepCopy()
	spare.Name, spare.UID, spare.ResourceVersion, spare.Spec.PoolName = "spare", "spare-uid", "", "spare"
	spare.OwnerReferences = []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool", Name: "spare", UID: "spare-mp-uid"}}
	spare.Status.PoolID = mpTestPoolID + "-spare"
	g.Expect(f.mgmt.Create(context.Background(), spare)).To(Succeed())

	pools := []kubernetesservice.WorkerPool{
		{ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2},
		{ID: mpTestPoolID + "-spare", Name: "spare", Flavor: mpTestFlavor, SizePerZone: 2},
	}
	workers := []kubernetesservice.Worker{mpTestOtherWorker("provision_pending")}
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return(pools, nil).AnyTimes()
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(workers, nil).AnyTimes()

	const reconcilesPerPool = 10
	errs := make(chan error, 2*reconcilesPerPool)
	var wg sync.WaitGroup
	for _, name := range []string{"workers", "spare"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range reconcilesPerPool {
				_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: mpTestNamespace, Name: name}})
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		g.Expect(err).NotTo(HaveOccurred())
	}
	g.Expect(r.transitional.snapshot("cluster-uid", f.clock.Now())).To(Equal(map[string]time.Time{"o2": mpTestNow}))
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
	g.Expect(r.infraClusterToIBMCloudManagedMachinePools(context.Background(), f.infra)).To(Equal(want),
		"the IBMCloudManagedCluster's zones decide the zone guards of every pool of its Cluster")
	unlabelledInfra := f.infra.DeepCopy()
	unlabelledInfra.Labels = nil
	g.Expect(r.infraClusterToIBMCloudManagedMachinePools(context.Background(), unlabelledInfra)).To(BeEmpty())
}

// TestIBMCloudManagedMachinePoolPoolNameClaimants checks the map function of the watch that re-triggers the other
// claimants of a pool's spec.poolName: only the pools of the same Cluster with the same spec.poolName, never the pool
// itself, also for a pool the cache no longer holds (a delete event).
func TestIBMCloudManagedMachinePoolPoolNameClaimants(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	r := f.reconciler()
	create := func(name, poolName, clusterName string) *infrav1.IBMCloudManagedMachinePool {
		pool := f.pool.DeepCopy()
		pool.Name, pool.ResourceVersion, pool.Spec.PoolName = name, "", poolName
		pool.Labels = map[string]string{clusterv1.ClusterNameLabel: clusterName}
		g.Expect(f.mgmt.Create(context.Background(), pool)).To(Succeed())
		return pool
	}
	claimant := create("claimant", "workers", mpTestCluster)
	otherName := create("gpu", "gpu", mpTestCluster)
	create("foreign", "workers", "prod")
	request := func(name string) ctrl.Request {
		return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: mpTestNamespace, Name: name}}
	}

	g.Expect(r.poolNameClaimants(context.Background(), f.pool)).To(Equal([]ctrl.Request{request("claimant")}))
	g.Expect(r.poolNameClaimants(context.Background(), claimant)).To(Equal([]ctrl.Request{request("workers")}))
	g.Expect(r.poolNameClaimants(context.Background(), otherName)).To(BeEmpty(), "no other pool claims gpu")

	// A delete event carries the last state of a pool the cache no longer holds.
	gone := f.pool.DeepCopy()
	gone.Name = "gone"
	g.Expect(r.poolNameClaimants(context.Background(), gone)).To(Equal([]ctrl.Request{request("claimant"), request("workers")}))

	unlabelled := f.pool.DeepCopy()
	unlabelled.Labels = nil
	g.Expect(r.poolNameClaimants(context.Background(), unlabelled)).To(BeEmpty(), "a pool of no Cluster has no claimants")
	g.Expect(r.poolNameClaimants(context.Background(), f.infra)).To(BeEmpty(), "another kind maps to nothing")
}

// TestIBMCloudManagedMachinePoolWatchPredicates checks which events of an IBMCloudManagedCluster and of another pool
// reach the pools: the ones that can change what a guard decides or what a worker pool create request sends, and no
// other, so that the conditions and revalidations of the IBMCloudManagedCluster do not repeat every pool's IBM
// observation calls.
func TestIBMCloudManagedMachinePoolWatchPredicates(t *testing.T) {
	f := newMachinePoolFixture(t)

	t.Run("IBMCloudManagedCluster", func(t *testing.T) {
		g := NewWithT(t)
		p := infraClusterPoolInputsChanged()
		g.Expect(p.Create(event.CreateEvent{Object: f.infra})).To(BeTrue(), "a pool may wait for the IBMCloudManagedCluster to exist")
		g.Expect(p.Delete(event.DeleteEvent{Object: f.infra})).To(BeFalse())
		g.Expect(p.Generic(event.GenericEvent{Object: f.infra})).To(BeFalse())
		g.Expect(p.Update(event.UpdateEvent{ObjectOld: f.pool, ObjectNew: f.pool})).To(BeTrue(),
			"an object of another kind passes, so that a pool is never left waiting for its terminal recheck")

		// The status the IBMCloudManagedCluster controller publishes from a subnet read: besides the name and subnet ID
		// that the pools read, every zone carries the informational fields that a revalidation rewrites.
		published := f.infra.DeepCopy()
		published.Status.VPC.Name = "dev-vpc"
		cidrs := []string{"10.240.0.0/24", "10.240.64.0/24"}
		g.Expect(published.Status.Zones).To(HaveLen(len(cidrs)))
		for i := range published.Status.Zones {
			zone := &published.Status.Zones[i]
			zone.Subnet.Name = zone.Name + "-subnet"
			zone.IPv4CIDRBlock = cidrs[i]
			zone.PublicGateway = infrav1.PublicGatewayStateAttached
			zone.AvailableIPv4Addresses = ptr.To[int32](200)
		}
		for _, tc := range []struct {
			name string
			// before, when set, prepares the old object; change makes the new one from a copy of the published status.
			before func(infra *infrav1.IBMCloudManagedCluster)
			change func(infra *infrav1.IBMCloudManagedCluster)
			want   bool
		}{
			{name: "a zone added to spec.zones", want: true, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Spec.Zones = append(infra.Spec.Zones, infrav1.ZoneSubnet{Name: "us-south-3", Subnet: infrav1.ResourceIdentifier{ID: "sn-3"}})
			}},
			{name: "a zone resolved in status.zones", want: true, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Status.Zones = append(infra.Status.Zones, infrav1.ResolvedZone{Name: "us-south-3", Subnet: infrav1.ResolvedResource{ID: "sn-3"}})
			}},
			{name: "a zone resolved to another subnet ID", want: true, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Status.Zones[1].Subnet.ID = "sn-2-recreated"
			}},
			{name: "the VPC resolved in status.vpc", want: true,
				before: func(infra *infrav1.IBMCloudManagedCluster) { infra.Status.VPC = infrav1.ResolvedResource{} },
				change: func(*infrav1.IBMCloudManagedCluster) {}},
			{name: "the cluster-name label set", want: true, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Labels[clusterv1.ClusterNameLabel] = "renamed"
			}},
			{name: "a condition", want: false, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Status.Conditions = append(infra.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready"})
			}},
			{name: "a resync", want: false, change: func(infra *infrav1.IBMCloudManagedCluster) { infra.ResourceVersion = "2" }},
			{name: "no zone resolved, as a nil and as an empty list", want: false,
				before: func(infra *infrav1.IBMCloudManagedCluster) { infra.Status.Zones = nil },
				change: func(infra *infrav1.IBMCloudManagedCluster) { infra.Status.Zones = []infrav1.ResolvedZone{} }},
			{name: "a revalidation that counts fewer free addresses", want: false, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Status.Zones[0].AvailableIPv4Addresses = ptr.To[int32](197)
			}},
			{name: "a revalidation that finds the public gateway detached", want: false, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Status.Zones[0].PublicGateway = infrav1.PublicGatewayStateNotAttached
			}},
			{name: "a revalidation that finds the subnet renamed", want: false, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.Status.Zones[1].Subnet.Name = "renamed-in-ibm-cloud"
			}},
			{name: "a revalidation that warns of a nearly full subnet", want: false, change: func(infra *infrav1.IBMCloudManagedCluster) {
				infra.ResourceVersion = "3"
				infra.Status.VPC.Name = "renamed-vpc"
				infra.Status.Zones[0].AvailableIPv4Addresses = ptr.To[int32](12)
				infra.Status.Conditions = append(infra.Status.Conditions, metav1.Condition{
					Type: infrav1.IBMCloudManagedClusterNetworkReadyCondition, Status: metav1.ConditionTrue,
					Reason: infrav1.IBMCloudManagedClusterNetworkReadyReason, Message: "subnet sn-1 (zone us-south-1) has 12 free IPv4 addresses",
				})
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				old := published.DeepCopy()
				updated := old.DeepCopy()
				if tc.before != nil {
					tc.before(old)
				}
				tc.change(updated)
				NewWithT(t).Expect(p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated})).To(Equal(tc.want))
			})
		}
	})

	t.Run("IBMCloudManagedMachinePool", func(t *testing.T) {
		g := NewWithT(t)
		p := poolNameClaimsChanged()
		g.Expect(p.Create(event.CreateEvent{Object: f.pool})).To(BeTrue(), "a new claimant makes a conflict")
		g.Expect(p.Delete(event.DeleteEvent{Object: f.pool})).To(BeTrue(), "a claimant that is gone may end a conflict")
		g.Expect(p.Generic(event.GenericEvent{Object: f.pool})).To(BeFalse())
		unlabelled := f.pool.DeepCopy()
		unlabelled.Labels = nil
		g.Expect(p.Update(event.UpdateEvent{ObjectOld: unlabelled, ObjectNew: f.pool})).To(BeTrue(),
			"the label makes the pool a claimant in its Cluster")
		deleting := f.pool.DeepCopy()
		deleting.DeletionTimestamp = ptr.To(metav1.NewTime(mpTestNow))
		g.Expect(p.Update(event.UpdateEvent{ObjectOld: f.pool, ObjectNew: deleting})).To(BeFalse(),
			"a claimant being deleted still owns the IBM pool's workers")
		reported := f.pool.DeepCopy()
		reported.Status.Ready = ptr.To(true)
		g.Expect(p.Update(event.UpdateEvent{ObjectOld: f.pool, ObjectNew: reported})).To(BeFalse())
	})
}

// guardFixWindow is how long the guard tests behind a manager wait for a pool to resume after the edit that fixes its
// guard: far below the terminal recheck (about 15 minutes) and the pool transition poll (about 30 seconds), so that only
// a watch can bring the pool back in time.
const guardFixWindow = 10 * time.Second

// envtestGuardedCluster creates, in namespace, a managed Cluster "dev" with its IBMCloudManagedCluster (zones us-south-1
// and us-south-2, both resolved) and an IBMCloudManagedControlPlane whose IBM cluster does not exist yet, both labelled
// with the Cluster's name as the Cluster controller labels them. Without a cluster ID no pool of the Cluster makes an
// IBM call, so a pool whose guards all pass reports WaitingForControlPlane. It returns the Cluster and the
// IBMCloudManagedCluster.
func envtestGuardedCluster(t *testing.T, namespace string) (*clusterv1.Cluster, *infrav1.IBMCloudManagedCluster) {
	t.Helper()
	g := NewWithT(t)
	cluster := createTestCluster(t, namespace, mpTestCluster, managedControlPlaneRef(mpTestCluster))
	labels := map[string]string{clusterv1.ClusterNameLabel: mpTestCluster}
	infra := createTestInfra(t, namespace, mpTestCluster, cluster)
	infra.Labels = labels
	g.Expect(testEnv.Update(ctx, infra)).To(Succeed())
	infra.Status.VPC = infrav1.ResolvedResource{ID: testVPCID}
	infra.Status.Zones = []infrav1.ResolvedZone{
		{Name: "us-south-1", Subnet: infrav1.ResolvedResource{ID: "sn-1"}},
		{Name: "us-south-2", Subnet: infrav1.ResolvedResource{ID: "sn-2"}},
	}
	g.Expect(testEnv.Status().Update(ctx, infra)).To(Succeed())
	g.Expect(testEnv.Create(ctx, &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: mpTestCluster, Labels: labels},
		Spec:       controlplanev1.IBMCloudManagedControlPlaneSpec{Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.36"},
	})).To(Succeed())
	return cluster, infra
}

// createEnvtestPool creates, in namespace, a MachinePool of the Cluster "dev" with two replicas and the
// IBMCloudManagedMachinePool it owns, both named name; the pool claims poolName in zones. It returns the pool's key.
//
// Parameters:
//   - t is the test.
//   - namespace is the Cluster's namespace.
//   - name names the MachinePool and the pool.
//   - poolName is the pool's spec.poolName.
//   - zones are the pool's spec.zones.
func createEnvtestPool(t *testing.T, namespace, name, poolName string, zones ...string) client.ObjectKey {
	t.Helper()
	g := NewWithT(t)
	labels := map[string]string{clusterv1.ClusterNameLabel: mpTestCluster}
	machinePool := &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels},
		Spec: clusterv1.MachinePoolSpec{ClusterName: mpTestCluster, Replicas: ptr.To[int32](2), Template: clusterv1.MachineTemplateSpec{
			Spec: clusterv1.MachineSpec{ClusterName: mpTestCluster, InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedMachinePoolKind, Name: name,
			}},
		}},
	}
	g.Expect(testEnv.Create(ctx, machinePool)).To(Succeed())
	poolZones := make([]infrav1.WorkerPoolZone, 0, len(zones))
	for _, zone := range zones {
		poolZones = append(poolZones, infrav1.WorkerPoolZone{Name: zone})
	}
	pool := &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels, OwnerReferences: []metav1.OwnerReference{{
			APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool", Name: name, UID: machinePool.UID,
		}}},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{PoolName: poolName, Flavor: mpTestFlavor, Zones: poolZones},
	}
	g.Expect(testEnv.Create(ctx, pool)).To(Succeed())
	return client.ObjectKeyFromObject(pool)
}

// startGuardPoolManager starts the pool controller for namespace behind a manager whose cache, and so whose watches, see
// only that namespace, with its real watches (SetupWithManager). The pools of these tests make no IBM call and no
// workload-cluster call, so the resolver and the client builder fail the test when called. The manager stops when the
// test ends, before the cleanups registered earlier run.
func startGuardPoolManager(t *testing.T, namespace string) {
	t.Helper()
	g := NewWithT(t)
	mgr := newNamespacedTestManager(t, namespace)
	clusterCache, err := SetupClusterCache(ctx, mgr, ClusterCacheOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	shutdownClusterCacheOnCleanup(t, clusterCache)
	r := &IBMCloudManagedMachinePoolReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(),
		Recorder: mgr.GetEventRecorderFor("ibmcloudmanagedmachinepool-controller"), Resolver: failIfCalledResolver{t: t},
		ClientBuilder: failIfCalledBuilder{t: t}, ClusterCache: clusterCache, Clock: clocktesting.NewFakeClock(time.Now())}
	g.Expect(r.SetupWithManager(ctx, mgr)).To(Succeed())
	startTestManager(t, mgr)
}

// envtestPoolReason returns a function that reads the WorkerPoolReady reason of the pool key from the API server, ""
// while the pool has none or is gone.
func envtestPoolReason(t *testing.T, key client.ObjectKey) func() string {
	t.Helper()
	return func() string {
		pool := &infrav1.IBMCloudManagedMachinePool{}
		if err := testEnv.GetAPIReader().Get(ctx, key, pool); err != nil {
			return ""
		}
		return conditions.GetReason(pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
	}
}

// TestIBMCloudManagedMachinePoolGuardFixEnvtest runs the pool controller behind a manager with its real watches: a pool
// stopped by a guard whose fix is an edit of another object resumes within seconds of that edit, not at the terminal
// recheck about 15 minutes later. The Cluster is paused when the test ends, after the manager stopped, so that a
// manager of a later test leaves the objects alone.
func TestIBMCloudManagedMachinePoolGuardFixEnvtest(t *testing.T) {
	t.Run("a zone added to the IBMCloudManagedCluster ends UnknownZone", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "pool-zone-fix")
		cluster, infra := envtestGuardedCluster(t, namespace)
		pauseAtCleanup(t, cluster, client.ObjectKey{})
		key := createEnvtestPool(t, namespace, "workers", "workers", "us-south-1", "us-south-3")
		startGuardPoolManager(t, namespace)
		reason := envtestPoolReason(t, key)
		g.Eventually(reason, 30*time.Second, 200*time.Millisecond).Should(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason))

		// The fix the message names: the zone with its subnet in the IBMCloudManagedCluster's spec.zones. Its controller
		// does not run here, so the zone stays unresolved and the pool waits for it.
		latest := getInfra(t, client.ObjectKeyFromObject(infra))
		latest.Spec.Zones = append(latest.Spec.Zones, infrav1.ZoneSubnet{Name: "us-south-3", Subnet: infrav1.ResourceIdentifier{ID: "sn-3"}})
		g.Expect(testEnv.Update(ctx, latest)).To(Succeed())
		g.Eventually(reason, guardFixWindow, 100*time.Millisecond).Should(Equal(
			infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason), "the spec.zones edit re-triggers the pool")

		// The IBMCloudManagedCluster controller resolves the zone: every guard passes, and the pool waits for the IBM
		// cluster.
		latest = getInfra(t, client.ObjectKeyFromObject(infra))
		latest.Status.Zones = append(latest.Status.Zones, infrav1.ResolvedZone{Name: "us-south-3", Subnet: infrav1.ResolvedResource{ID: "sn-3"}})
		g.Expect(testEnv.Status().Update(ctx, latest)).To(Succeed())
		g.Eventually(reason, guardFixWindow, 100*time.Millisecond).Should(Equal(
			infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason), "the status.zones change re-triggers the pool")
	})

	t.Run("the deletion of the other claimant ends PoolNameConflict", func(t *testing.T) {
		g := NewWithT(t)
		namespace := newTestNamespace(t, "pool-name-fix")
		cluster, _ := envtestGuardedCluster(t, namespace)
		pauseAtCleanup(t, cluster, client.ObjectKey{})
		kept := createEnvtestPool(t, namespace, "workers-a", "workers", "us-south-1", "us-south-2")
		removed := createEnvtestPool(t, namespace, "workers-b", "workers", "us-south-1", "us-south-2")
		startGuardPoolManager(t, namespace)
		conflict := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason
		g.Eventually(envtestPoolReason(t, kept), 30*time.Second, 200*time.Millisecond).Should(Equal(conflict))
		g.Eventually(envtestPoolReason(t, removed), 30*time.Second, 200*time.Millisecond).Should(Equal(conflict))

		// The fix the message names: delete all but one claimant. The deleted one leaves the IBM pool to the other at
		// once (no IBM call), and its removal re-triggers the remaining claimant.
		pool := &infrav1.IBMCloudManagedMachinePool{}
		g.Expect(testEnv.GetAPIReader().Get(ctx, removed, pool)).To(Succeed())
		g.Expect(testEnv.Delete(ctx, pool)).To(Succeed())
		g.Eventually(func() bool {
			return apierrors.IsNotFound(testEnv.GetAPIReader().Get(ctx, removed, &infrav1.IBMCloudManagedMachinePool{}))
		}, 30*time.Second, 100*time.Millisecond).Should(BeTrue())
		g.Eventually(envtestPoolReason(t, kept), guardFixWindow, 100*time.Millisecond).Should(Equal(
			infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason), "the claimant's deletion re-triggers the pool")
	})
}
