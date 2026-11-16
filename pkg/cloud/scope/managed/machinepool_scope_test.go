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
	"fmt"
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"

	. "github.com/onsi/gomega"
)

// The pool scope tests drive ReconcileNormal against a fake management cluster and a gomock IBM Cloud Kubernetes
// Service. gomock fails a test on any call it does not expect, which is how "zero IBM calls" and "no mutation" are
// proven. The fixture is shared with the delete and drain tests of this package.

const (
	poolTestNamespace = "capi-managed"
	poolTestCluster   = "dev"
	poolTestClusterID = "c0testcluster0000001"
	poolTestPoolID    = poolTestClusterID + "-a1b2c3d"
	poolTestAccount   = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	poolTestFlavor    = "bx2.4x16"
)

// poolTestNow is the fake clock's start.
var poolTestNow = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// poolStubBuilder is a ClientBuilder that hands out the gomock IKS client and records every identity and invalidation.
type poolStubBuilder struct {
	kubernetes  kubernetesservice.KubernetesService
	err         error
	identities  []credentials.Identity
	invalidated []credentials.IdentityKey
}

// KubernetesService implements ClientBuilder.
func (b *poolStubBuilder) KubernetesService(_ context.Context, id credentials.Identity, _ string) (kubernetesservice.KubernetesService, error) {
	b.identities = append(b.identities, id)
	if b.err != nil {
		return nil, b.err
	}
	return b.kubernetes, nil
}

// VPC implements ClientBuilder; the pool scope never asks for it.
func (b *poolStubBuilder) VPC(context.Context, credentials.Identity, string) (vpc.Vpc, error) {
	return nil, errors.New("poolStubBuilder: the pool scope must not build a VPC client")
}

// ResourceManager implements ClientBuilder; the pool scope never asks for it.
func (b *poolStubBuilder) ResourceManager(context.Context, credentials.Identity) (resourcemanager.ResourceManager, error) {
	return nil, errors.New("poolStubBuilder: the pool scope must not build a Resource Manager client")
}

// InvalidateAuthenticator implements ClientBuilder.
func (b *poolStubBuilder) InvalidateAuthenticator(key credentials.IdentityKey) {
	b.invalidated = append(b.invalidated, key)
}

// poolFixture holds the objects of one pool reconcile, the fakes around them and, once built, the scope.
type poolFixture struct {
	t        *testing.T
	ibm      *ksmock.MockKubernetesService
	builder  *poolStubBuilder
	resolver *recordingResolver
	recorder *record.FakeRecorder
	clock    *clocktesting.FakeClock

	cluster     *clusterv1.Cluster
	machinePool *clusterv1.MachinePool
	pool        *infrav1.IBMCloudManagedMachinePool
	cp          *controlplanev1.IBMCloudManagedControlPlane
	infra       *infrav1.IBMCloudManagedCluster
	// others are more IBMCloudManagedMachinePools of the Cluster.
	others []client.Object
	// params are the scope params of the last scope; tests may replace dependencies before calling scope.
	params *ScopeParams
	// mgmt is the fake management-cluster client of the last scope.
	mgmt client.Client
	// clusterCache gives the workload cluster; the empty fake (never connected) unless a test sets one.
	clusterCache clustercache.ClusterCache
	// persisted records status.disruption at every Persist call; persistErr is what Persist returns.
	persisted  []infrav1.WorkerDisruptionStatus
	persistErr error
	// transitionalSince is the scope's MachinePoolScopeInput.TransitionalSince, the controller's first-seen times.
	transitionalSince map[string]time.Time
}

// newPoolFixture returns the objects of a healthy IKS Cluster with two zones and a pool "workers" of 2 workers per
// zone (MachinePool replicas 4) whose IBM cluster exists and whose control plane is initialized.
func newPoolFixture(t *testing.T) *poolFixture {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	f := &poolFixture{
		t:            t,
		ibm:          ksmock.NewMockKubernetesService(mockCtrl),
		resolver:     &recordingResolver{},
		recorder:     record.NewFakeRecorder(100),
		clock:        clocktesting.NewFakeClock(poolTestNow),
		clusterCache: clustercache.NewFakeEmptyClusterCache(),
	}
	f.builder = &poolStubBuilder{kubernetes: f.ibm}
	labels := map[string]string{clusterv1.ClusterNameLabel: poolTestCluster}
	f.cluster = &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: poolTestNamespace, Name: poolTestCluster, UID: "cluster-uid"},
		Spec: clusterv1.ClusterSpec{
			ControlPlaneRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: controlplanev1.GroupVersion.Group, Kind: poolControlPlaneKind, Name: poolTestCluster,
			},
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group, Kind: poolInfraClusterKind, Name: poolTestCluster,
			},
		},
	}
	f.infra = testInfra()
	f.infra.Namespace = poolTestNamespace
	f.infra.Name = poolTestCluster
	f.infra.Status.VPC = infrav1.ResolvedResource{ID: testVPCID, Name: testVPCName}
	f.infra.Status.Zones = []infrav1.ResolvedZone{
		{Name: "us-south-1", Subnet: infrav1.ResolvedResource{ID: "sn-1"}},
		{Name: "us-south-2", Subnet: infrav1.ResolvedResource{ID: "sn-2"}},
	}
	f.cp = &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: poolTestNamespace, Name: poolTestCluster, Labels: labels},
		Spec:       controlplanev1.IBMCloudManagedControlPlaneSpec{Distribution: controlplanev1.DistributionKubernetes, DistributionVersion: "1.36"},
		Status: controlplanev1.IBMCloudManagedControlPlaneStatus{
			ClusterID:           poolTestClusterID,
			ClusterCRN:          "crn:v1:bluemix:public:containers-kubernetes:us-south:a/" + poolTestAccount + ":" + poolTestClusterID + "::",
			State:               "normal",
			DistributionVersion: "1.36.5_1537",
			Initialization:      controlplanev1.IBMCloudManagedControlPlaneInitializationStatus{ControlPlaneInitialized: ptr.To(true)},
		},
	}
	f.machinePool = &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: poolTestNamespace, Name: "workers", Labels: labels},
		Spec:       clusterv1.MachinePoolSpec{ClusterName: poolTestCluster, Replicas: ptr.To[int32](4)},
	}
	f.pool = poolObject("workers", "workers")
	return f
}

// poolObject returns an IBMCloudManagedMachinePool of the test Cluster in both zones.
//
// Parameters:
//   - name is the object's name.
//   - poolName is spec.poolName.
func poolObject(name, poolName string) *infrav1.IBMCloudManagedMachinePool {
	return &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: poolTestNamespace, Name: name, UID: types.UID("uid-" + name),
			Labels: map[string]string{clusterv1.ClusterNameLabel: poolTestCluster},
		},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName: poolName,
			Flavor:   poolTestFlavor,
			Zones:    []infrav1.WorkerPoolZone{{Name: "us-south-1"}, {Name: "us-south-2"}},
		},
	}
}

// poolScheme registers every type the pool scope reads and writes.
func poolScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, clusterv1.AddToScheme, infrav1.AddToScheme, controlplanev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

// managementObjects returns the objects the fake management cluster starts with; a nil control plane or
// IBMCloudManagedCluster is left out.
func (f *poolFixture) managementObjects() []client.Object {
	objs := []client.Object{f.cluster, f.machinePool, f.pool}
	if f.cp != nil {
		objs = append(objs, f.cp)
	}
	if f.infra != nil {
		objs = append(objs, f.infra)
	}
	return append(objs, f.others...)
}

// scope returns a scope of f.pool. The first call builds the fake management cluster from the fixture objects; later
// calls reuse it, so that what one reconcile wrote there (the disruption token) is seen by the next.
func (f *poolFixture) scope() *MachinePoolScope {
	f.t.Helper()
	if f.mgmt == nil {
		f.mgmt = fake.NewClientBuilder().WithScheme(poolScheme(f.t)).WithObjects(f.managementObjects()...).
			WithStatusSubresource(&infrav1.IBMCloudManagedMachinePool{}).Build()
	}
	if f.params == nil {
		f.params = &ScopeParams{Client: f.mgmt, APIReader: f.mgmt, Recorder: f.recorder, Resolver: f.resolver,
			ClientBuilder: f.builder, Clock: f.clock, ClusterCache: f.clusterCache}
	}
	scope, err := NewMachinePoolScope(f.params, MachinePoolScopeInput{Cluster: f.cluster, MachinePool: f.machinePool,
		Pool: f.pool, TransitionalSince: f.transitionalSince, ProviderIDDropAfter: 30 * time.Second, Persist: f.persist})
	if err != nil {
		f.t.Fatalf("NewMachinePoolScope: %v", err)
	}
	return scope
}

// persist is the scope's Persist: it records status.disruption as it is when the scope persists.
func (f *poolFixture) persist(context.Context) error {
	f.persisted = append(f.persisted, *f.pool.Status.Disruption.DeepCopy())
	return f.persistErr
}

// ibmPool returns an IBM pool of the test cluster with sizePerZone workers in each zone.
//
// Parameters:
//   - name is the pool name.
//   - sizePerZone is IBM's workerCount.
//   - zones are the pool's zones.
func ibmPool(name string, sizePerZone int64, zones ...string) kubernetesservice.WorkerPool {
	pool := kubernetesservice.WorkerPool{ID: poolTestPoolID, Name: name, Flavor: poolTestFlavor, SizePerZone: sizePerZone}
	for _, z := range zones {
		pool.Zones = append(pool.Zones, kubernetesservice.WorkerPoolZone{ID: z, WorkerCount: sizePerZone, AutobalanceEnabled: true})
	}
	return pool
}

// poolWorker returns a worker of the test pool.
//
// Parameters:
//   - id is the worker ID.
//   - zone is its zone.
//   - state and health are lifecycle.actualState and health.state.
func poolWorker(id, zone, state, health string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: poolTestPoolID, Zone: zone, ActualState: state, HealthState: health,
		KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540", KubeVersionDesired: "1.36.5_1540"}
}

// readyWorkers returns n deployed, normal workers spread over both zones, named w0..w<n-1>.
func readyWorkers(n int) []kubernetesservice.Worker {
	workers := make([]kubernetesservice.Worker, 0, n)
	for i := range n {
		zone := "us-south-1"
		if i%2 == 1 {
			zone = "us-south-2"
		}
		workers = append(workers, poolWorker("w"+string(rune('0'+i)), zone, WorkerStateDeployed, "normal"))
	}
	return workers
}

// providerIDOf is the provider ID of a test worker.
func providerIDOf(workerID string) string {
	return ProviderID(poolTestAccount, poolTestClusterID, workerID)
}

// apiError returns an IBM answer of op with the HTTP status and IBM code.
//
// Parameters:
//   - op is the operation.
//   - status is the HTTP status code.
//   - code is IBM's code ("" for none).
func apiError(op kubernetesservice.Operation, status int, code string) error {
	return &kubernetesservice.APIError{Operation: op, StatusCode: status, Code: code}
}

// condition returns the condition of type conditionType of pool, or nil.
func condition(pool *infrav1.IBMCloudManagedMachinePool, conditionType string) *metav1.Condition {
	return conditions.Get(pool, conditionType)
}

// expectObservation expects one ListWorkerPools answering pools and one ListWorkers answering workers.
func (f *poolFixture) expectObservation(pools []kubernetesservice.WorkerPool, workers []kubernetesservice.Worker) {
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(pools, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(workers, nil)
}

// events drains the fake recorder.
func (f *poolFixture) events() []string {
	var events []string
	for {
		select {
		case e := <-f.recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func TestNewMachinePoolScope(t *testing.T) {
	f := newPoolFixture(t)
	valid := func() (*ScopeParams, MachinePoolScopeInput) {
		c := fake.NewClientBuilder().Build()
		return &ScopeParams{Client: c, APIReader: c, Recorder: f.recorder, Resolver: f.resolver, ClientBuilder: f.builder,
				Clock: f.clock, ClusterCache: f.clusterCache},
			MachinePoolScopeInput{Cluster: f.cluster, MachinePool: f.machinePool, Pool: f.pool, ProviderIDDropAfter: time.Second,
				Persist: f.persist}
	}
	testCases := []struct {
		name    string
		modify  func(p *ScopeParams, in *MachinePoolScopeInput) *ScopeParams
		wantErr string
	}{
		{name: "valid", modify: func(p *ScopeParams, _ *MachinePoolScopeInput) *ScopeParams { return p }},
		{name: "nil params", modify: func(*ScopeParams, *MachinePoolScopeInput) *ScopeParams { return nil }, wantErr: "needs ScopeParams, got nil"},
		{
			name: "missing dependencies are all named",
			modify: func(p *ScopeParams, _ *MachinePoolScopeInput) *ScopeParams {
				p.Resolver, p.ClientBuilder, p.Clock = nil, nil, nil
				return p
			},
			wantErr: "needs ScopeParams.Resolver, ScopeParams.ClientBuilder, ScopeParams.Clock",
		},
		{
			name: "missing objects are all named",
			modify: func(p *ScopeParams, in *MachinePoolScopeInput) *ScopeParams {
				in.Cluster, in.MachinePool, in.Pool = nil, nil, nil
				return p
			},
			wantErr: "Cluster is nil; pass the owning Cluster; MachinePool is nil",
		},
		{
			name: "no drop delay",
			modify: func(p *ScopeParams, in *MachinePoolScopeInput) *ScopeParams {
				in.ProviderIDDropAfter = 0
				return p
			},
			wantErr: "ProviderIDDropAfter is 0s",
		},
		{
			name: "no persist function",
			modify: func(p *ScopeParams, in *MachinePoolScopeInput) *ScopeParams {
				in.Persist = nil
				return p
			},
			wantErr: "Persist is nil",
		},
		{
			name: "the drain dependencies are named",
			modify: func(p *ScopeParams, _ *MachinePoolScopeInput) *ScopeParams {
				p.APIReader, p.ClusterCache = nil, nil
				return p
			},
			wantErr: "needs ScopeParams.APIReader, ScopeParams.ClusterCache",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			params, in := valid()
			scope, err := NewMachinePoolScope(tc.modify(params, &in), in)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(scope).To(BeNil())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(scope.Pool).To(BeIdenticalTo(f.pool))
		})
	}
}

func TestMachinePoolScopeIdentitySeams(t *testing.T) {
	t.Run("constructor_makes_no_call", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		c := fake.NewClientBuilder().Build()
		params := &ScopeParams{Client: c, APIReader: c, Recorder: f.recorder, Resolver: failingResolver{t: t},
			ClientBuilder: failingBuilder{t: t}, Clock: f.clock, ClusterCache: f.clusterCache}
		scope, err := NewMachinePoolScope(params, MachinePoolScopeInput{Cluster: f.cluster, MachinePool: f.machinePool,
			Pool: f.pool, ProviderIDDropAfter: time.Second, Persist: f.persist})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(scope.ControlPlane).To(BeNil(), "the constructor reads no object")
	})
	t.Run("resolver_error_is_reported_without_an_IBM_call", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.resolver.err = errors.New("no credentials")
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(ready.Reason).To(Equal(MachinePoolIBMAPIErrorReason))
		g.Expect(ready.Message).To(ContainSubstring("cannot resolve the IBM Cloud credentials of IBMCloudManagedCluster dev"))
		g.Expect(ready.Message).NotTo(ContainSubstring("no credentials"), "the error text goes to the log only")
		g.Expect(f.builder.identities).To(BeEmpty())
	})
	t.Run("client build error names the identity", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.builder.err = errors.New("broken")
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Message).
			To(ContainSubstring("Kubernetes Service client of " + credentials.ControllerDescription))
	})
	t.Run("holder_is_the_clusters_IBMCloudManagedCluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Status.PoolID = poolTestPoolID
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.resolver.holders).To(HaveLen(1))
		g.Expect(f.resolver.holders[0]).To(BeAssignableToTypeOf(&infrav1.IBMCloudManagedCluster{}))
		g.Expect(client.ObjectKeyFromObject(f.resolver.holders[0])).To(Equal(client.ObjectKey{Namespace: poolTestNamespace, Name: poolTestCluster}))
	})
}

func TestMachinePoolScopeOwners(t *testing.T) {
	testCases := []struct {
		name       string
		modify     func(f *poolFixture)
		wantKind   OutcomeKind
		wantReason string
	}{
		{
			name:       "the Cluster's control plane is another kind",
			modify:     func(f *poolFixture) { f.cluster.Spec.ControlPlaneRef.Kind = "KubeadmControlPlane" },
			wantKind:   OutcomeTerminal,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason,
		},
		{
			name:       "the control plane does not exist",
			modify:     func(f *poolFixture) { f.cp = nil },
			wantKind:   OutcomeNotDone,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason,
		},
		{
			name:       "the Cluster's infrastructure is another kind",
			modify:     func(f *poolFixture) { f.cluster.Spec.InfrastructureRef.Kind = "IBMVPCCluster" },
			wantKind:   OutcomeTerminal,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason,
		},
		{
			name:       "the IBMCloudManagedCluster does not exist",
			modify:     func(f *poolFixture) { f.infra = nil },
			wantKind:   OutcomeNotDone,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason,
		},
		{
			name:       "no IBM cluster ID yet",
			modify:     func(f *poolFixture) { f.cp.Status.ClusterID = "" },
			wantKind:   OutcomeNotDone,
			wantReason: infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			tc.modify(f)
			// gomock fails on any IBM call; the resolver must not even be asked.
			outcome := f.scope().ReconcileNormal(context.Background())
			g.Expect(outcome.Kind).To(Equal(tc.wantKind))
			g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Reason).To(Equal(tc.wantReason))
			g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolDeletingCondition).Reason).
				To(Equal(infrav1.IBMCloudManagedMachinePoolNotDeletingReason))
			g.Expect(f.resolver.holders).To(BeEmpty())
		})
	}
}

func TestMachinePoolScopeGuards(t *testing.T) {
	t.Run("two_claimants_both_conflict_and_neither_calls_IBM", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Status.PoolID = poolTestPoolID
		f.pool.Spec.ProviderIDList = []string{providerIDOf("w0")}
		f.others = []client.Object{poolObject("workers-copy", "workers")}
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(ready.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason))
		g.Expect(ready.Message).To(ContainSubstring("workers-copy"))
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0")}), "nothing is published")
		g.Expect(f.pool.Status.Ready).To(BeNil())
		g.Expect(f.resolver.holders).To(BeEmpty())
	})
	t.Run("non_divisible_new_pool_makes_zero_calls", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.machinePool.Spec.Replicas = ptr.To[int32](3)
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(ready.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason))
		g.Expect(ready.Message).To(ContainSubstring("replicas=3 is not a multiple of 2 zones; use 2 or 4"))
		g.Expect(f.resolver.holders).To(BeEmpty())
	})
	t.Run("non_divisible_existing_pool_is_observed_but_not_resized", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.machinePool.Spec.Replicas = ptr.To[int32](5)
		f.pool.Status.PoolID = poolTestPoolID
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Reason).
			To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason))
		g.Expect(f.pool.Spec.ProviderIDList).To(HaveLen(4), "observation still publishes")
		g.Expect(f.pool.Status.Ready).To(Equal(ptr.To(true)))
	})
	t.Run("a zone not resolved yet waits", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.infra.Status.Zones = f.infra.Status.Zones[:1]
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason)))
	})
	t.Run("an ignored failureDomains is noted on a ready pool", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.machinePool.Spec.FailureDomains = []string{"us-south-3"}
		f.pool.Status.PoolID = poolTestPoolID
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
		g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(Done()))
		ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(ready.Message).To(ContainSubstring(FailureDomainsIgnoredMessage))
	})
}

func TestMachinePoolScopeFindsThePool(t *testing.T) {
	t.Run("adopts a pool it did not create", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
		g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(Done()))
		g.Expect(f.pool.Status.PoolID).To(Equal(poolTestPoolID))
		g.Expect(f.events()).To(ConsistOf(ContainSubstring("Normal WorkerPoolAdopted CAPIBM now sizes IBM worker pool workers")))
	})
	// The WorkerPoolAdopted rule: finding the pool IBM creates with the cluster is no adoption. spec.creation, not the
	// pool name, tells that pool apart, and only while the control plane is not initialized: afterwards a WithCluster
	// pool is handled like any other, so one that names a pool made by hand takes it over with the event. A recorded
	// status.poolID means CAPIBM has seen the pool before. Every kind of pool is found by name and recorded alike.
	withCluster, afterCluster := infrav1.PoolCreationWithCluster, infrav1.PoolCreationAfterCluster
	for _, tc := range []struct {
		name     string
		poolName string
		creation infrav1.PoolCreation
		poolID   string
		// uninitialized leaves the control plane without status.initialization.controlPlaneInitialized.
		uninitialized bool
		wantEvent     bool
	}{
		{name: "a_WithCluster_pool_with_a_custom_name_is_not_adopted", poolName: "workers-a", creation: withCluster, uninitialized: true},
		{name: "a WithCluster pool named default is not adopted", poolName: "default", creation: withCluster, uninitialized: true},
		{name: "a pool named default without the marker is adopted", poolName: "default", wantEvent: true},
		{name: "a pool without the marker is adopted before the control plane is initialized", poolName: "default",
			uninitialized: true, wantEvent: true},
		{name: "an AfterCluster pool found without an ID is adopted", poolName: "workers", creation: afterCluster, wantEvent: true},
		{name: "a_WithCluster_pool_first_found_after_initialization_is_adopted", poolName: "workers-b", creation: withCluster,
			wantEvent: true},
		{name: "a known pool is not adopted again", poolName: "workers", poolID: poolTestPoolID},
		{name: "a known WithCluster pool is not adopted again", poolName: "workers-a", creation: withCluster, poolID: poolTestPoolID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.pool.Spec.PoolName, f.pool.Spec.Creation, f.pool.Status.PoolID = tc.poolName, tc.creation, tc.poolID
			if tc.uninitialized {
				f.cp.Status.Initialization.ControlPlaneInitialized = nil
			}
			f.expectObservation([]kubernetesservice.WorkerPool{ibmPool(tc.poolName, 2, "us-south-1", "us-south-2")}, readyWorkers(4))
			g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(Done()))
			g.Expect(f.pool.Status.PoolID).To(Equal(poolTestPoolID))
			if tc.wantEvent {
				g.Expect(f.events()).To(ConsistOf("Normal WorkerPoolAdopted CAPIBM now sizes IBM worker pool " + tc.poolName +
					" (" + poolTestPoolID + ") and removes it when this object is deleted"))
				return
			}
			g.Expect(f.events()).To(BeEmpty())
		})
	}
	t.Run("a_pool_of_another_flavor_is_not_ours", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.ProviderIDList = []string{providerIDOf("w0")}
		foreign := ibmPool("workers", 2, "us-south-1", "us-south-2")
		foreign.Flavor = "cx2.2x4"
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{foreign}, nil)
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Reason).
			To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolConflictReason))
		g.Expect(f.pool.Status.PoolID).To(BeEmpty())
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0")}))
		g.Expect(f.pool.Status.Ready).To(BeNil())
	})
	t.Run("a pool IBM still removes holds the name", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		releasing := ibmPool("workers", 2, "us-south-1")
		releasing.ActualState = "deleting"
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return([]kubernetesservice.WorkerPool{releasing}, nil)
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameBeingReleasedReason)))
	})
	t.Run("a live pool wins over a deleting one of the same name", func(t *testing.T) {
		g := NewWithT(t)
		releasing, live := ibmPool("workers", 2), ibmPool("workers", 2)
		releasing.ID, releasing.ActualState = "old", "deleted"
		g.Expect(poolNamed([]kubernetesservice.WorkerPool{releasing, live}, "workers").ID).To(Equal(poolTestPoolID))
		g.Expect(poolNamed([]kubernetesservice.WorkerPool{live, releasing}, "workers").ID).To(Equal(poolTestPoolID))
		g.Expect(poolNamed([]kubernetesservice.WorkerPool{live}, "other")).To(BeNil())
	})
	t.Run("zones that differ are noted, not changed", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Status.PoolID = poolTestPoolID
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2", "us-south-3")}, readyWorkers(4))
		f.scope().ReconcileNormal(context.Background())
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Message).
			To(ContainSubstring("IBM pool workers has the zones us-south-1, us-south-2, us-south-3 while spec.zones lists us-south-1, us-south-2"))
	})
}

func TestMachinePoolScopeCreatesThePool(t *testing.T) {
	t.Run("one request with the subnet of every zone", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.cp.Spec.Distribution = controlplanev1.DistributionOpenShift
		f.pool.Spec.Entitlement = infrav1.EntitlementOCPEntitled
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
		f.ibm.EXPECT().CreateWorkerPool(gomock.Any(), kubernetesservice.CreateWorkerPoolRequest{
			ClusterID: poolTestClusterID, Name: "workers", Flavor: poolTestFlavor, VPCID: testVPCID, WorkerCountPerZone: 2,
			Zones:       []kubernetesservice.ZoneSubnet{{Zone: "us-south-1", SubnetID: "sn-1"}, {Zone: "us-south-2", SubnetID: "sn-2"}},
			Entitlement: "ocp_entitled",
		}).Return(poolTestPoolID, nil)
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason)))
		g.Expect(f.pool.Status.PoolID).To(Equal(poolTestPoolID))
		g.Expect(f.pool.Status.Ready).To(BeNil(), "ready only once the pool is observed")
	})
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "E0d39 pool name exists", err: apiError(kubernetesservice.OpCreateWorkerPool, http.StatusConflict, "E0d39")},
		{name: "409 create in progress", err: apiError(kubernetesservice.OpCreateWorkerPool, http.StatusConflict, "")},
	} {
		t.Run(tc.name+" is adopted by name later", func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
			f.ibm.EXPECT().CreateWorkerPool(gomock.Any(), gomock.Any()).Return("", tc.err)
			g.Expect(f.scope().ReconcileNormal(context.Background())).
				To(Equal(NotDone(IntervalClusterProvisioning, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason)))
			g.Expect(f.pool.Status.PoolID).To(BeEmpty())
		})
	}
	t.Run("a refused request needs a human", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
		f.ibm.EXPECT().CreateWorkerPool(gomock.Any(), gomock.Any()).Return("", apiError(kubernetesservice.OpCreateWorkerPool, http.StatusBadRequest, "E0001"))
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Message).
			To(ContainSubstring("(HTTP 400, code E0001) while creating worker pool workers in IBM cluster " + poolTestClusterID))
	})
	// Before the control plane is initialized every pool IBM does not list waits, with no IBM mutation; only the
	// message depends on spec.creation, and it names the pool's own spec.poolName.
	notReady := "the master of IBM cluster " + poolTestClusterID + " is not ready yet; pool %s is created once " +
		"IBMCloudManagedControlPlane " + poolTestCluster + " is initialized"
	for _, tc := range []struct {
		name        string
		poolName    string
		creation    infrav1.PoolCreation
		wantMessage string
	}{
		{name: "a WithCluster pool comes with the cluster", poolName: "system", creation: infrav1.PoolCreationWithCluster,
			wantMessage: "IBM creates pool system together with cluster " + poolTestClusterID + " (spec.creation: WithCluster); " +
				"waiting for it to be listed"},
		{name: "an AfterCluster pool waits for the initialized control plane", poolName: "workers",
			creation: infrav1.PoolCreationAfterCluster, wantMessage: fmt.Sprintf(notReady, "workers")},
		{name: "a pool without the marker waits for the initialized control plane", poolName: "workers",
			wantMessage: fmt.Sprintf(notReady, "workers")},
		{name: "a pool named default without the marker is not the WithCluster pool", poolName: "default",
			wantMessage: fmt.Sprintf(notReady, "default")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.pool.Spec.PoolName, f.pool.Spec.Creation = tc.poolName, tc.creation
			f.cp.Status.Initialization.ControlPlaneInitialized = nil
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
			g.Expect(f.scope().ReconcileNormal(context.Background())).
				To(Equal(NotDone(IntervalClusterProvisioning, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason)))
			g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Message).To(Equal(tc.wantMessage))
		})
	}
	t.Run("a_WithCluster_pool_IBM_does_not_list_is_created_like_any_other_once_the_control_plane_is_initialized", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.PoolName, f.pool.Spec.Creation = "system", infrav1.PoolCreationWithCluster
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
		f.ibm.EXPECT().CreateWorkerPool(gomock.Any(), kubernetesservice.CreateWorkerPoolRequest{
			ClusterID: poolTestClusterID, Name: "system", Flavor: poolTestFlavor, VPCID: testVPCID, WorkerCountPerZone: 2,
			Zones: []kubernetesservice.ZoneSubnet{{Zone: "us-south-1", SubnetID: "sn-1"}, {Zone: "us-south-2", SubnetID: "sn-2"}},
		}).Return(poolTestPoolID, nil)
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason)))
		g.Expect(f.pool.Status.PoolID).To(Equal(poolTestPoolID))
		g.Expect(f.events()).To(BeEmpty(), "a pool CAPIBM creates is not adopted")
	})
	t.Run("an unresolved subnet waits for the IBMCloudManagedCluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.infra.Status.Zones[1].Subnet.ID = ""
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason)))
	})
}

// TestMachinePoolScopeMarkerMovedBetweenCreateAttempts is the pool side of a spec.creation marker that moves between
// two create attempts: the create attempt that timed out sent object a (poolName default, then marked), and before
// the cluster showed up the user moved spec.creation: WithCluster to object b (poolName workers). IBM lists only pool
// default. Each object touches only the IBM pool its own spec.poolName names: a, no longer marked, publishes and sizes
// default and reports the adoption; b never publishes, sizes or records default, waits while the control plane is not
// initialized and then creates its own pool workers. gomock fails a case on any IBM call it does not expect.
func TestMachinePoolScopeMarkerMovedBetweenCreateAttempts(t *testing.T) {
	listed := []kubernetesservice.WorkerPool{ibmPool("default", 2, "us-south-1", "us-south-2")}
	waiting := NotDone(IntervalClusterProvisioning, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason)
	t.Run("object_a_without_the_marker_adopts_the_pool_its_poolName_names", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.PoolName, f.pool.Spec.Creation = "default", infrav1.PoolCreationAfterCluster
		f.cp.Status.Initialization.ControlPlaneInitialized = nil
		f.expectObservation(listed, readyWorkers(4))
		g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(Done()))
		g.Expect(f.pool.Status.PoolID).To(Equal(poolTestPoolID))
		g.Expect(f.pool.Spec.ProviderIDList).To(HaveLen(4), "a publishes the workers of default")
		g.Expect(f.events()).To(ConsistOf("Normal WorkerPoolAdopted CAPIBM now sizes IBM worker pool default (" +
			poolTestPoolID + ") and removes it when this object is deleted"))
	})
	t.Run("object_b_with_the_marker_leaves_the_pool_it_does_not_name_alone", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.PoolName, f.pool.Spec.Creation = "workers", infrav1.PoolCreationWithCluster
		f.cp.Status.Initialization.ControlPlaneInitialized = nil
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(listed, nil)
		g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(waiting))
		g.Expect(f.pool.Status.PoolID).To(BeEmpty())
		g.Expect(f.pool.Spec.ProviderIDList).To(BeEmpty())
		g.Expect(f.pool.Status.Ready).To(BeNil())
		g.Expect(f.events()).To(BeEmpty())
	})
	t.Run("object_b_creates_its_own_pool_once_the_control_plane_is_initialized", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.PoolName, f.pool.Spec.Creation = "workers", infrav1.PoolCreationWithCluster
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(listed, nil)
		f.ibm.EXPECT().CreateWorkerPool(gomock.Any(), kubernetesservice.CreateWorkerPoolRequest{
			ClusterID: poolTestClusterID, Name: "workers", Flavor: poolTestFlavor, VPCID: testVPCID, WorkerCountPerZone: 2,
			Zones: []kubernetesservice.ZoneSubnet{{Zone: "us-south-1", SubnetID: "sn-1"}, {Zone: "us-south-2", SubnetID: "sn-2"}},
		}).Return(poolTestClusterID+"-workers", nil)
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCreatingReason)))
		g.Expect(f.pool.Status.PoolID).To(Equal(poolTestClusterID+"-workers"), "never the ID of default")
		g.Expect(f.pool.Spec.ProviderIDList).To(BeEmpty())
		g.Expect(f.events()).To(BeEmpty())
	})
}

func TestMachinePoolScopeClusterStateGate(t *testing.T) {
	for _, state := range []string{"unsupported", "unavailable", "scaled_down"} {
		t.Run("create_blocked_while_"+state, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.cp.Status.State = state
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
			g.Expect(f.scope().ReconcileNormal(context.Background())).
				To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason)))
		})
		t.Run("grow_blocked_while_"+state, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.cp.Status.State = state
			f.pool.Status.PoolID = poolTestPoolID
			f.machinePool.Spec.Replicas = ptr.To[int32](6)
			f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
			g.Expect(f.scope().ReconcileNormal(context.Background())).
				To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason)))
			g.Expect(f.pool.Status.Ready).To(Equal(ptr.To(true)), "observation still runs")
		})
	}
	t.Run("the unsupported message asks for a master upgrade", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.cp.Status.State = "unsupported"
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, nil)
		f.scope().ReconcileNormal(context.Background())
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition).Message).
			To(ContainSubstring("upgrade the master first"))
	})
}

func TestClusterStateMessage(t *testing.T) {
	testCases := []struct {
		name  string
		state string
		want  string
	}{
		{name: "unavailable accepts no pool change", state: "unavailable",
			want: "IBM cluster c1 is unavailable; IBM accepts no worker pool change in that state"},
		{name: "scaled_down accepts no pool change", state: "scaled_down",
			want: "IBM cluster c1 is scaled_down; IBM accepts no worker pool change in that state"},
		{name: "unsupported names the master upgrade", state: "unsupported",
			want: "IBM cluster c1 runs an unsupported version: IBM does not add or reload workers on an unsupported version; " +
				"upgrade the master first"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(clusterStateMessage("c1", tc.state)).To(Equal(tc.want))
		})
	}
}

func TestMachinePoolScopeObserves(t *testing.T) {
	pools := []kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-2", "us-south-1")}
	t.Run("no_worker_publishes_zero_replicas", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.expectObservation(pools, nil)
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Status.Replicas).To(Equal(ptr.To[int32](0)))
		g.Expect(f.pool.Spec.ProviderIDList).To(BeEmpty())
		g.Expect(f.pool.Status.Ready).To(Equal(ptr.To(true)), "the pool exists")
	})
	t.Run("two_provisioning_workers_are_two_replicas", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.expectObservation(pools, []kubernetesservice.Worker{
			poolWorker("w1", "us-south-1", "provisioning", ""), poolWorker("w0", "us-south-2", "provision_pending", ""),
		})
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome).To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason)))
		g.Expect(f.pool.Status.Replicas).To(Equal(ptr.To[int32](2)))
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0"), providerIDOf("w1")}), "sorted")
	})
	t.Run("ready_and_provisioned_stay_true_when_a_worker_fails", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.expectObservation(pools, readyWorkers(4))
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Status.Ready).To(Equal(ptr.To(true)))
		failed := readyWorkers(4)
		failed[0].ActualState, failed[0].HealthState = "provision_failed", "critical"
		f.expectObservation(pools, failed)
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWorkersNotReadyReason))
		g.Expect(f.pool.Status.Ready).To(Equal(ptr.To(true)))
		g.Expect(f.pool.Status.Initialization.Provisioned).To(Equal(ptr.To(true)))
	})
	t.Run("failed_listing_keeps_the_list_and_replicas", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.ProviderIDList = []string{providerIDOf("w0")}
		f.pool.Status.Replicas = ptr.To[int32](1)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(pools, nil)
		f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(nil, apiError(kubernetesservice.OpListWorkers, http.StatusInternalServerError, ""))
		outcome := f.scope().ReconcileNormal(context.Background())
		g.Expect(outcome.Kind).To(Equal(OutcomeTransient))
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0")}))
		g.Expect(f.pool.Status.Replicas).To(Equal(ptr.To[int32](1)))
		g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)).To(BeNil(), "a 5xx changes no condition")
	})
	t.Run("omitted_worker_is_kept_then_dropped_after_two_listings", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.ProviderIDList = []string{providerIDOf("w0"), providerIDOf("w1")}
		f.expectObservation(pools, readyWorkers(1))
		scope := f.scope()
		scope.ReconcileNormal(context.Background())
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0"), providerIDOf("w1")}))
		g.Expect(f.pool.Status.MissingWorkers).To(Equal([]infrav1.MissingWorker{{WorkerID: "w1", FirstMissingTime: metav1.NewTime(poolTestNow)}}))
		g.Expect(f.pool.Status.Replicas).To(Equal(ptr.To[int32](2)))

		f.clock.Step(30 * time.Second)
		f.expectObservation(pools, readyWorkers(1))
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0")}))
		g.Expect(f.pool.Status.MissingWorkers).To(BeEmpty())
		g.Expect(f.pool.Status.Replicas).To(Equal(ptr.To[int32](1)))
	})
	t.Run("deleted_worker_is_dropped_at_once", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.ProviderIDList = []string{providerIDOf("w0"), providerIDOf("w1")}
		workers := readyWorkers(2)
		workers[1].ActualState = WorkerStateDeleted
		f.expectObservation(pools, workers)
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0")}))
	})
	t.Run("the first observation starts the settle delay once", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.expectObservation(pools, readyWorkers(4))
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow))
		f.clock.Step(time.Hour)
		f.expectObservation(pools, readyWorkers(4))
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Status.LastDisruptionTime.Time).To(Equal(poolTestNow))
	})
	t.Run("pool facts and worker versions", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		workers := readyWorkers(4)
		workers[0].KubeVersionTarget = "1.36.5_1545"
		workers[1].KubeVersionActual, workers[1].KubeVersionTarget = "1.35.9_1530", "1.35.9_1530"
		f.expectObservation(pools, workers)
		f.scope().ReconcileNormal(context.Background())
		status := f.pool.Status
		g.Expect(status.PoolID).To(Equal(poolTestPoolID))
		g.Expect(status.SizePerZone).To(Equal(ptr.To[int32](2)))
		g.Expect(status.Zones).To(Equal([]infrav1.ObservedPoolZone{
			{Name: "us-south-1", WorkerCount: ptr.To[int32](2)}, {Name: "us-south-2", WorkerCount: ptr.To[int32](2)},
		}))
		g.Expect(status.WorkerVersions).To(Equal([]infrav1.WorkerVersionCount{{Version: "1.35.9_1530", Count: 1}, {Version: "1.36.5_1540", Count: 3}}))
		g.Expect(status.TargetWorkerVersion).To(Equal("1.36.5_1545"))
		g.Expect(status.WorkersWithUpdateAvailable).To(Equal(ptr.To[int32](2)), "one newer fix pack, one minor behind")
	})
	t.Run("no update is published as zero and no target as omitted", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		workers := readyWorkers(4)
		for i := range workers {
			workers[i].KubeVersionTarget = ""
		}
		f.expectObservation(pools, workers)
		f.scope().ReconcileNormal(context.Background())
		g.Expect(f.pool.Status.WorkersWithUpdateAvailable).To(Equal(ptr.To[int32](0)))
		g.Expect(f.pool.Status.TargetWorkerVersion).To(BeEmpty())
	})
	t.Run("without the cluster CRN the list is kept and replicas published", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.cp.Status.ClusterCRN = ""
		f.pool.Spec.ProviderIDList = []string{providerIDOf("w0")}
		f.expectObservation(pools, readyWorkers(4))
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalClusterProvisioning, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForControlPlaneReason)))
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{providerIDOf("w0")}))
		g.Expect(f.pool.Status.Replicas).To(Equal(ptr.To[int32](1)))
	})
	t.Run("an entry CAPIBM cannot read keeps the list", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.pool.Spec.ProviderIDList = []string{"aws:///us-east-1a/i-0"}
		f.expectObservation(pools, readyWorkers(4))
		g.Expect(f.scope().ReconcileNormal(context.Background()).Kind).To(Equal(OutcomeTransient))
		g.Expect(f.pool.Spec.ProviderIDList).To(Equal([]string{"aws:///us-east-1a/i-0"}))
	})
}

// TestMachinePoolScopeListedWorkers checks what the scope hands back for the pool controller's first-seen map of
// transitional workers (TransitionalSince): the workers of a successful listing, whatever the reconcile does next,
// and nothing when no listing succeeded, so that a failure never drops the map's entries.
func TestMachinePoolScopeListedWorkers(t *testing.T) {
	pools := []kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}
	t.Run("the_listing_of_every_pool_of_the_cluster_is_handed_back", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		workers := append(readyWorkers(4),
			kubernetesservice.Worker{ID: "o2", PoolID: "other-pool", Zone: "us-south-1", ActualState: "provision_pending"})
		f.expectObservation(pools, workers)
		scope := f.scope()
		listed, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeFalse(), "nothing is listed before the reconcile")
		g.Expect(listed).To(BeNil())
		scope.ReconcileNormal(context.Background())
		listed, ok = scope.ListedWorkers()
		g.Expect(ok).To(BeTrue())
		g.Expect(listed).To(Equal(workers), "another pool's provision_pending worker included")
	})
	t.Run("an empty listing is an answer", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.expectObservation(pools, nil)
		scope := f.scope()
		scope.ReconcileNormal(context.Background())
		listed, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeTrue())
		g.Expect(listed).To(BeEmpty())
	})
	t.Run("a listing the reconcile cannot publish is still handed back", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.cp.Status.ClusterCRN = ""
		f.expectObservation(pools, readyWorkers(4))
		scope := f.scope()
		g.Expect(scope.ReconcileNormal(context.Background()).Kind).To(Equal(OutcomeNotDone))
		_, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeTrue())
	})
	t.Run("a failed listing hands back nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(pools, nil)
		f.ibm.EXPECT().ListWorkers(gomock.Any(), poolTestClusterID).Return(nil, apiError(kubernetesservice.OpListWorkers, http.StatusInternalServerError, ""))
		scope := f.scope()
		scope.ReconcileNormal(context.Background())
		listed, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeFalse())
		g.Expect(listed).To(BeNil())
	})
	t.Run("a reconcile without IBM calls hands back nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.cp.Status.ClusterID = ""
		scope := f.scope()
		scope.ReconcileNormal(context.Background())
		_, ok := scope.ListedWorkers()
		g.Expect(ok).To(BeFalse())
	})
}

func TestMachinePoolScopeSize(t *testing.T) {
	t.Run("grow_is_requested_at_once", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.machinePool.Spec.Replicas = ptr.To[int32](6)
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
		f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(3)).Return(nil)
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyResizingReason)))
	})
	t.Run("an autoscaled pool is never resized", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.machinePool.Spec.Replicas = ptr.To[int32](6)
		autoscaled := ibmPool("workers", 2, "us-south-1", "us-south-2")
		autoscaled.AutoscaleEnabled = true
		f.expectObservation([]kubernetesservice.WorkerPool{autoscaled}, readyWorkers(4))
		g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(Done()))
		ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(ready.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyExternallyAutoscaledReason))
	})
	t.Run("a_smaller_MachinePool_starts_the_scale_down_behind_the_guards", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		f.machinePool.Spec.Replicas = ptr.To[int32](2)
		f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
		// gomock fails on a ResizeWorkerPool: the first observation of the pool starts the settle delay.
		g.Expect(f.scope().ReconcileNormal(context.Background())).
			To(Equal(NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForSettledClusterReason)))
		g.Expect(f.pool.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}), "nothing is recorded before the first cordon")
	})
	refusals := []struct {
		name string
		err  error
	}{
		{name: "a 403", err: apiError(kubernetesservice.OpResizeWorkerPool, http.StatusForbidden, "")},
		// The 403 IBM documents for an autoscaled pool, on a pool IBM does not report autoscaled, is a missing role,
		// not a transient failure retried with the fast backoff.
		{name: "the autoscaled-pool 403", err: &kubernetesservice.APIError{Operation: kubernetesservice.OpResizeWorkerPool,
			StatusCode: http.StatusForbidden, Description: "Forbidden. The request will not be permitted."}},
	}
	for _, tc := range refusals {
		t.Run("a refused grow is classified: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.machinePool.Spec.Replicas = ptr.To[int32](6)
			f.expectObservation([]kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}, readyWorkers(4))
			f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), poolTestClusterID, poolTestPoolID, int64(3)).Return(tc.err)
			outcome := f.scope().ReconcileNormal(context.Background())
			g.Expect(outcome.Kind).To(Equal(OutcomeTerminal))
			g.Expect(outcome.Reason).To(Equal(MachinePoolIBMAPIErrorReason))
			ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
			g.Expect(ready.Reason).To(Equal(MachinePoolIBMAPIErrorReason))
			g.Expect(ready.Message).To(ContainSubstring("(HTTP 403) while growing worker pool workers of IBM cluster " + poolTestClusterID +
				" to 3 workers per zone; grant it " + poolKubernetesServiceRoles))
		})
	}
}

func TestMachinePoolScopeClusterDeletion(t *testing.T) {
	testCases := []struct {
		name   string
		modify func(f *poolFixture)
	}{
		{name: "the_Cluster_is_being_deleted_while_the_pool_would_grow", modify: func(f *poolFixture) {
			f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
			f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
			f.machinePool.Spec.Replicas = ptr.To[int32](6)
			f.pool.Status.PoolID = poolTestPoolID
		}},
		{name: "the_Cluster_is_being_deleted_while_IBM_no_longer_lists_the_pool", modify: func(f *poolFixture) {
			f.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
			f.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
		}},
		{name: "the control plane is being deleted", modify: func(f *poolFixture) {
			f.cp.DeletionTimestamp = ptr.To(metav1.NewTime(poolTestNow))
			f.cp.Finalizers = []string{"test"}
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.clusterCache = failIfCalledClusterCache{t: t}
			tc.modify(f)
			// gomock fails on any IBM call: no ListWorkerPools, no CreateWorkerPool, no ResizeWorkerPool.
			g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(Done()))
			g.Expect(f.resolver.holders).To(BeEmpty(), "no credential is resolved for a cluster that goes away")
			g.Expect(f.builder.identities).To(BeEmpty())
			g.Expect(condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)).To(BeNil(),
				"WorkerPoolReady is left as it was")
		})
	}
	t.Run("a live Cluster and control plane are not a deletion", func(t *testing.T) {
		g := NewWithT(t)
		f := newPoolFixture(t)
		g.Expect(f.loadedScope(t).clusterDeletingReason()).To(BeEmpty())
		f = newPoolFixture(t)
		f.cp = nil
		g.Expect(f.loadedScope(t).clusterDeletingReason()).To(BeEmpty(), "a missing control plane is ownersMissing's case")
	})
}

// loadedScope returns a scope whose owners are loaded.
func (f *poolFixture) loadedScope(t *testing.T) *MachinePoolScope {
	t.Helper()
	scope := f.scope()
	NewWithT(t).Expect(scope.loadOwners(context.Background())).To(Succeed())
	return scope
}

func TestMachinePoolScopeReadiness(t *testing.T) {
	pools := []kubernetesservice.WorkerPool{ibmPool("workers", 2, "us-south-1", "us-south-2")}
	testCases := []struct {
		name        string
		workers     []kubernetesservice.Worker
		wantOutcome Outcome
		wantStatus  metav1.ConditionStatus
		wantMessage string
	}{
		{
			name: "every worker ready", workers: readyWorkers(4), wantOutcome: Done(), wantStatus: metav1.ConditionTrue,
			wantMessage: "4 of 4 workers of pool workers are ready; MachinePool workers asks for 4",
		},
		{
			name: "fewer workers than desired", workers: readyWorkers(3),
			wantOutcome: NotDone(IntervalPoolTransition, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyProvisioningReason),
			wantStatus:  metav1.ConditionFalse, wantMessage: "3 of 3 workers",
		},
		{
			name: "a worker with a warning", workers: append(readyWorkers(3), poolWorker("w3", "us-south-2", WorkerStateDeployed, "warning")),
			wantOutcome: NotDone(IntervalGuarded, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWorkersNotReadyReason),
			wantStatus:  metav1.ConditionFalse, wantMessage: "3 of 4 workers",
		},
		{
			name: "workers of other pools do not count",
			workers: append(readyWorkers(4), kubernetesservice.Worker{ID: "x", PoolID: "other", ActualState: "provisioning"},
				poolWorker("gone", "us-south-1", WorkerStateDeleted, "")),
			wantOutcome: Done(), wantStatus: metav1.ConditionTrue, wantMessage: "4 of 4 workers",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.expectObservation(pools, tc.workers)
			g.Expect(f.scope().ReconcileNormal(context.Background())).To(Equal(tc.wantOutcome))
			ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
			g.Expect(ready.Status).To(Equal(tc.wantStatus))
			g.Expect(ready.Message).To(ContainSubstring(tc.wantMessage))
		})
	}
}

func TestMachinePoolScopeIBMErrors(t *testing.T) {
	testCases := []struct {
		name          string
		err           error
		wantKind      OutcomeKind
		wantCondition string
		invalidated   bool
	}{
		{
			name: "401 rebuilds the identity's authenticator", err: apiError(kubernetesservice.OpListWorkerPools, http.StatusUnauthorized, ""),
			wantKind: OutcomeNotDone, wantCondition: "IBM Cloud rejected the credentials of the CAPIBM controller identity while listing the worker pools",
			invalidated: true,
		},
		{
			name: "403 names the roles", err: apiError(kubernetesservice.OpListWorkerPools, http.StatusForbidden, "E3e8a"),
			wantKind:      OutcomeTerminal,
			wantCondition: "IBM Cloud refused the CAPIBM controller identity (HTTP 403, code E3e8a) while listing the worker pools of IBM cluster " + poolTestClusterID + "; grant it the Administrator platform role and the Manager service role on Kubernetes Service",
		},
		{
			name: "404 looks again", err: apiError(kubernetesservice.OpListWorkerPools, http.StatusNotFound, ""),
			wantKind: OutcomeNotDone, wantCondition: "the IBM cluster or pool is gone",
		},
		{name: "429 backs off", err: apiError(kubernetesservice.OpListWorkerPools, http.StatusTooManyRequests, ""), wantKind: OutcomeRateLimited},
		{name: "5xx is transient", err: apiError(kubernetesservice.OpListWorkerPools, http.StatusServiceUnavailable, ""), wantKind: OutcomeTransient},
		{name: "a network error is transient", err: errors.New("connection reset"), wantKind: OutcomeTransient},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			f.ibm.EXPECT().ListWorkerPools(gomock.Any(), poolTestClusterID).Return(nil, tc.err)
			outcome := f.scope().ReconcileNormal(context.Background())
			g.Expect(outcome.Kind).To(Equal(tc.wantKind))
			ready := condition(f.pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
			if tc.wantCondition == "" {
				g.Expect(ready).To(BeNil(), "a transient failure changes no condition")
			} else {
				g.Expect(ready.Reason).To(Equal(MachinePoolIBMAPIErrorReason))
				g.Expect(ready.Message).To(ContainSubstring(tc.wantCondition))
			}
			if tc.invalidated {
				g.Expect(f.builder.invalidated).To(ConsistOf(credentials.ControllerKey()))
			} else {
				g.Expect(f.builder.invalidated).To(BeEmpty())
			}
		})
	}
}

// TestMachinePoolScopeIBMProblemOn checks that ibmProblemOn classifies like ibmProblem but reports through the function
// it is given, once, and leaves the pool's own conditions alone; a 429 and a transient failure report nothing.
func TestMachinePoolScopeIBMProblemOn(t *testing.T) {
	action := "replacing worker w0 of IBM cluster " + poolTestClusterID
	testCases := []struct {
		name        string
		err         error
		wantKind    OutcomeKind
		wantMessage string
		invalidated bool
	}{
		{name: "401 rebuilds the identity's authenticator", err: apiError(kubernetesservice.OpReplaceWorker, http.StatusUnauthorized, ""),
			wantKind: OutcomeNotDone, invalidated: true,
			wantMessage: "IBM Cloud rejected the credentials of the test identity while " + action + "; CAPIBM rebuilds them and retries"},
		{name: "403 names the roles and the code only", err: apiError(kubernetesservice.OpReplaceWorker, http.StatusForbidden, "E3e8a"),
			wantKind: OutcomeTerminal,
			wantMessage: "IBM Cloud refused the test identity (HTTP 403, code E3e8a) while " + action + "; grant it the Administrator " +
				"platform role and the Manager service role on Kubernetes Service (the IAM roles of the managed Kubernetes book)"},
		{name: "400 asks for a human", err: apiError(kubernetesservice.OpReplaceWorker, http.StatusBadRequest, "E9999"),
			wantKind: OutcomeTerminal,
			wantMessage: "IBM Cloud refused the request (HTTP 400, code E9999) while " + action + "; check spec.flavor and " +
				"spec.zones of IBMCloudManagedMachinePool/workers; the controller log has IBM's description"},
		{name: "429 reports nothing", err: apiError(kubernetesservice.OpReplaceWorker, http.StatusTooManyRequests, ""), wantKind: OutcomeRateLimited},
		{name: "5xx reports nothing", err: apiError(kubernetesservice.OpReplaceWorker, http.StatusInternalServerError, ""), wantKind: OutcomeTransient},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newPoolFixture(t)
			scope := f.scope()
			clients := &poolClients{identity: credentials.Identity{Key: credentials.ControllerKey(), Description: "the test identity"}, kubernetes: f.ibm}
			var reported []string
			outcome := scope.ibmProblemOn(func(reason, message string) { reported = append(reported, reason+": "+message) },
				clients, kubernetesservice.OpReplaceWorker, action, tc.err)
			g.Expect(outcome.Kind).To(Equal(tc.wantKind))
			if tc.wantMessage == "" {
				g.Expect(reported).To(BeEmpty())
			} else {
				g.Expect(reported).To(Equal([]string{MachinePoolIBMAPIErrorReason + ": " + tc.wantMessage}))
			}
			g.Expect(f.pool.Status.Conditions).To(BeEmpty(), "only the given report function writes")
			if tc.invalidated {
				g.Expect(f.builder.invalidated).To(ConsistOf(credentials.ControllerKey()))
			} else {
				g.Expect(f.builder.invalidated).To(BeEmpty())
			}
		})
	}
}

func TestObservedZonesAndNotes(t *testing.T) {
	g := NewWithT(t)
	g.Expect(observedZones([]kubernetesservice.WorkerPoolZone{{ID: "b", WorkerCount: 1}, {ID: ""}, {ID: "a", WorkerCount: 3}})).
		To(Equal([]infrav1.ObservedPoolZone{{Name: "a", WorkerCount: ptr.To[int32](3)}, {Name: "b", WorkerCount: ptr.To[int32](1)}}))
	pool := ibmPool("workers", 1, "us-south-2", "us-south-1")
	g.Expect(zonesNote(&pool, poolObject("workers", "workers"))).To(BeEmpty(), "order does not matter")
	pool.Zones = nil
	g.Expect(zonesNote(&pool, poolObject("workers", "workers"))).To(ContainSubstring("has the zones none"))
}
