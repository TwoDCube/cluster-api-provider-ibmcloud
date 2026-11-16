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
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/index"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"

	. "github.com/onsi/gomega"
)

// The rollout tests call Reconcile on pools whose workers are due for replacement. They prove what the scope tests
// cannot: every phase is persisted before the next one starts, the events reach the recorder, two pools of one Cluster
// share the disruption token, a restart resumes from the Node markers, and the reconcile is requeued when
// spec.rollout.after passes. The step logic itself is tested in pkg/cloud/scope/managed (machinepool_rollout_test.go).

// roNodeCreated is the creation time of every Node of the rollout workload: a day before the clock starts.
var roNodeCreated = mpTestNow.Add(-24 * time.Hour)

// roSparePoolID is the IBM pool ID of the second pool "spare" of the rollout tests, whose workers are o0 and o1.
const roSparePoolID = "other-pool"

// rolloutWorkload is the fake workload cluster of a rollout test: a Ready Node, created a day ago, and a pod for each of
// the workers w0..w3 of the pool and o0, o1 of the spare pool, with the Nodes indexed by provider ID as ClusterCache
// indexes them (NodeProviderIDIndex).
type rolloutWorkload struct {
	client client.Client
	calls  *orderedCalls
	// mu guards refusals, which a test may change between reconciles.
	mu sync.Mutex
	// refusals are the evictions a PodDisruptionBudget refuses, by pod name.
	refusals map[string]error
}

// roNodeName is the Node name of a test worker: 10.0.0.1-4 for w0-w3 and 10.0.1.1-2 for o0, o1.
func roNodeName(workerID string) string {
	return map[string]string{"w0": "10.0.0.1", "w1": "10.0.0.2", "w2": "10.0.0.3", "w3": "10.0.0.4",
		"o0": "10.0.1.1", "o1": "10.0.1.2", "w4": "10.0.0.5"}[workerID]
}

// roProviderID is the provider ID of a test worker.
func roProviderID(workerID string) string {
	return "ibm://" + mpTestAccount + "///" + mpTestClusterID + "/" + workerID
}

// roNode is a Ready Node of a test worker created at created.
func roNode(workerID string, created time.Time) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: roNodeName(workerID), CreationTimestamp: metav1.NewTime(created)},
		Spec:       corev1.NodeSpec{ProviderID: roProviderID(workerID)},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(created)}}},
	}
}

// rollingPool gives the fixture's pool its IBM pool ID, a last removal an hour ago and a connected rollout workload.
func (f *machinePoolFixture) rollingPool(t *testing.T) *rolloutWorkload {
	t.Helper()
	f.pool.Status.PoolID = mpTestPoolID
	f.pool.Status.LastDisruptionTime = metav1.NewTime(mpTestNow.Add(-time.Hour))
	w := &rolloutWorkload{calls: &orderedCalls{}, refusals: map[string]error{}}
	objs := make([]client.Object, 0, 12)
	for _, id := range []string{"w0", "w1", "w2", "w3", "o0", "o1"} {
		objs = append(objs, roNode(id, roNodeCreated),
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web-" + id}, Spec: corev1.PodSpec{NodeName: roNodeName(id)}})
	}
	w.client = fake.NewClientBuilder().WithScheme(machinePoolScheme(t)).WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithIndex(&corev1.Node{}, index.NodeProviderIDField, index.NodeByProviderID).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				w.calls.add("patch " + obj.GetName())
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourceCreate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
				w.calls.add("evict " + obj.GetName())
				w.mu.Lock()
				refusal := w.refusals[obj.GetName()]
				w.mu.Unlock()
				if refusal != nil {
					return refusal
				}
				return c.SubResource(subResource).Create(ctx, obj, sub, opts...)
			},
		}).Build()
	f.clusterCache = clustercache.NewFakeClusterCache(w.client, client.ObjectKey{Namespace: mpTestNamespace, Name: mpTestCluster})
	return w
}

// refuse makes a PodDisruptionBudget refuse the eviction of the pod, or allow it again with nil.
func (w *rolloutWorkload) refuse(pod string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.refusals[pod] = err
}

// node returns the Node of a test worker.
func (w *rolloutWorkload) node(t *testing.T, workerID string) *corev1.Node {
	t.Helper()
	node := &corev1.Node{}
	NewWithT(t).Expect(w.client.Get(context.Background(), client.ObjectKey{Name: roNodeName(workerID)}, node)).To(Succeed())
	return node
}

// cordoned returns the names of the cordoned Nodes, sorted.
func (w *rolloutWorkload) cordoned(t *testing.T) []string {
	t.Helper()
	nodes := &corev1.NodeList{}
	NewWithT(t).Expect(w.client.List(context.Background(), nodes)).To(Succeed())
	var names []string
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			names = append(names, node.Name)
		}
	}
	return names
}

// roPDBRefusal is the API server's answer to an eviction a PodDisruptionBudget refuses.
func roPDBRefusal() error {
	err := apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes,
		metav1.StatusCause{Type: policyv1.DisruptionBudgetCause, Message: "The disruption budget web needs 2 healthy pods and has 2 currently"})
	return err
}

// roWorker is a ready worker of pool poolID in zone on fix pack 1.36.5_1540 of the master minor.
func roWorker(id, poolID, zone string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: poolID, Zone: zone, ActualState: "deployed", HealthState: "normal",
		KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"}
}

// roIBM is the IBM side of a rollout test: the answers of ListWorkerPools and ListWorkers, which a test changes between
// reconciles, served to any number of reconciles of any pool.
type roIBM struct {
	mu      sync.Mutex
	pools   []kubernetesservice.WorkerPool
	workers []kubernetesservice.Worker
}

// newRoIBM answers ListWorkerPools with the pool "workers" (two workers per zone) and "spare" (one per zone), and
// ListWorkers with w0..w3 of "workers" (w0, w2 in us-south-1; w1, w3 in us-south-2) and o0, o1 of "spare", each one
// ready. behind names the workers that run the minor before the master's 1.36.
func newRoIBM(f *machinePoolFixture, behind ...string) *roIBM {
	zones := func(n int64) []kubernetesservice.WorkerPoolZone {
		return []kubernetesservice.WorkerPoolZone{{ID: "us-south-1", WorkerCount: n, AutobalanceEnabled: true},
			{ID: "us-south-2", WorkerCount: n, AutobalanceEnabled: true}}
	}
	ibm := &roIBM{
		pools: []kubernetesservice.WorkerPool{
			{ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: 2, Zones: zones(2)},
			{ID: roSparePoolID, Name: "spare", Flavor: mpTestFlavor, SizePerZone: 1, Zones: zones(1)},
		},
		workers: []kubernetesservice.Worker{roWorker("w0", mpTestPoolID, "us-south-1"), roWorker("w1", mpTestPoolID, "us-south-2"),
			roWorker("w2", mpTestPoolID, "us-south-1"), roWorker("w3", mpTestPoolID, "us-south-2"),
			roWorker("o0", roSparePoolID, "us-south-1"), roWorker("o1", roSparePoolID, "us-south-2")},
	}
	ibm.update(func(w *kubernetesservice.Worker) {
		for _, id := range behind {
			if w.ID == id {
				w.KubeVersionActual, w.KubeVersionTarget = "1.35.9_1530", "1.35.9_1530"
			}
		}
	})
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).DoAndReturn(func(context.Context, string) ([]kubernetesservice.WorkerPool, error) {
		ibm.mu.Lock()
		defer ibm.mu.Unlock()
		return append([]kubernetesservice.WorkerPool(nil), ibm.pools...), nil
	}).AnyTimes()
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).DoAndReturn(func(context.Context, string) ([]kubernetesservice.Worker, error) {
		ibm.mu.Lock()
		defer ibm.mu.Unlock()
		return append([]kubernetesservice.Worker(nil), ibm.workers...), nil
	}).AnyTimes()
	return ibm
}

// update changes every listed worker with change.
func (ibm *roIBM) update(change func(w *kubernetesservice.Worker)) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	for i := range ibm.workers {
		change(&ibm.workers[i])
	}
}

// setState puts the worker in state.
func (ibm *roIBM) setState(id, state string) {
	ibm.update(func(w *kubernetesservice.Worker) {
		if w.ID == id {
			w.ActualState = state
		}
	})
}

// addSparePool creates the second pool "spare" of the Cluster (IBM pool roSparePoolID, one worker per zone) with its
// MachinePool, as earlier reconciles left it (finalizer, not paused, a last removal an hour ago).
func (f *machinePoolFixture) addSparePool(t *testing.T) {
	t.Helper()
	g := NewWithT(t)
	machinePool := f.machinePool.DeepCopy()
	machinePool.Name, machinePool.UID, machinePool.ResourceVersion = "spare", "spare-mp-uid", ""
	machinePool.Spec.Replicas = ptr.To[int32](2)
	machinePool.Spec.Template.Spec.InfrastructureRef.Name = "spare"
	g.Expect(f.mgmt.Create(context.Background(), machinePool)).To(Succeed())
	spare := f.pool.DeepCopy()
	spare.Name, spare.UID, spare.ResourceVersion, spare.Spec.PoolName = "spare", "spare-uid", "", "spare"
	spare.OwnerReferences = []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "MachinePool", Name: "spare", UID: "spare-mp-uid"}}
	status := infrav1.IBMCloudManagedMachinePoolStatus{PoolID: roSparePoolID, LastDisruptionTime: metav1.NewTime(mpTestNow.Add(-time.Hour)),
		Conditions: f.pool.Status.Conditions}
	// Create drops the status of an object with a status subresource; the update writes it.
	g.Expect(f.mgmt.Create(context.Background(), spare)).To(Succeed())
	spare.Status = status
	g.Expect(f.mgmt.Status().Update(context.Background(), spare)).To(Succeed())
}

// reconcileNamed runs one Reconcile of the pool named name and expects no error.
func (f *machinePoolFixture) reconcileNamed(r *IBMCloudManagedMachinePoolReconciler, name string) ctrl.Result {
	f.t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: mpTestNamespace, Name: name}})
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return result
}

// storedNamed reads the pool named name back from the fake management cluster.
func (f *machinePoolFixture) storedNamed(name string) *infrav1.IBMCloudManagedMachinePool {
	f.t.Helper()
	pool := &infrav1.IBMCloudManagedMachinePool{}
	NewWithT(f.t).Expect(f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: name}, pool)).To(Succeed())
	return pool
}

// upToDate returns the stored WorkersUpToDate condition of the pool named name.
func (f *machinePoolFixture) upToDate(name string) *metav1.Condition {
	return conditions.Get(f.storedNamed(name), infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition)
}

// leaseHolder returns the holder of the Cluster's disruption token, or "" when nobody holds it.
func (f *machinePoolFixture) leaseHolder() string {
	f.t.Helper()
	lease := &coordinationv1.Lease{}
	err := f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: mpTestCluster + "-ibm-worker-disruption"}, lease)
	if apierrors.IsNotFound(err) {
		return ""
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return ptr.Deref(lease.Spec.HolderIdentity, "")
}

func TestRequeueForRollout(t *testing.T) {
	now := mpTestNow
	steady := ctrl.Result{RequeueAfter: 5 * time.Minute}
	retryAfter := ctrl.Result{RequeueAfter: 90 * time.Second}
	testCases := []struct {
		name   string
		result ctrl.Result
		err    error
		kind   managedscope.OutcomeKind
		at     time.Time
		want   ctrl.Result
	}{
		{name: "no recheck time keeps the result", result: steady, want: steady},
		{name: "a recheck time in the past keeps the result", result: steady, at: now.Add(-time.Minute), want: steady},
		{name: "a recheck time before the requeue shortens it, just past the time", result: steady, at: now.Add(2 * time.Minute),
			want: ctrl.Result{RequeueAfter: 2*time.Minute + time.Second}},
		{name: "a recheck time after the requeue keeps it", result: steady, at: now.Add(time.Hour), want: steady},
		{name: "a result without a requeue gets one", at: now.Add(time.Hour), want: ctrl.Result{RequeueAfter: time.Hour + time.Second}},
		{name: "an error keeps controller-runtime's backoff", err: errors.New("transient"), kind: managedscope.OutcomeTransient,
			at: now.Add(time.Minute)},
		{name: "IBM's Retry-After stays a lower bound", result: retryAfter, kind: managedscope.OutcomeRateLimited,
			at: now.Add(10 * time.Second), want: retryAfter},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kind := tc.kind
			if kind == "" {
				kind = managedscope.OutcomeNotDone
			}
			NewWithT(t).Expect(requeueForRollout(tc.result, tc.err, kind, tc.at, now)).To(Equal(tc.want))
		})
	}
}

func TestIBMCloudManagedMachinePoolReconcileRollout(t *testing.T) {
	t.Run("one replacement per settled cluster, always with update=true, and minor follow for every pool", testRolloutOnePerSettledCluster)
	t.Run("a 1-worker IKS cluster waits for capacity", testRolloutWaitsForCapacity)
	t.Run("the MachinePool version is ignored and reported", testRolloutVersionIgnored)
	t.Run("Manual with a fix pack reports PatchUpdateAvailable and replaces nothing", testRolloutPatchUpdateAvailable)
	t.Run("a blocking budget holds the replace until the drain time limit", testRolloutDrainBlocked)
	t.Run("the second Node waits for the replacement to be Ready for minReadySeconds", testRolloutMinReadySeconds)
	t.Run("a restart mid-drain resumes from the Node markers", testRolloutResumesFromMarkers)
	t.Run("only the Lease holder cordons", testRolloutTwoPools)
	t.Run("an abort reverts only CAPIBM's cordon and load balancer label", testRolloutAbortKeepsHumanCordon)
	t.Run("the reconcile is requeued when spec.rollout.after passes", testRolloutRequeuesAtRolloutAfter)
	t.Run("a paused Cluster stops a replace in flight", testRolloutPaused)
}

// testRolloutOnePerSettledCluster follows the replace of w0 through Reconcile while the spare pool's workers are due
// too (minor follow for every pool, no hold): every phase is persisted before the next, ReplaceWorker goes out once
// with update=true after the drain, and the spare pool replaces nothing while w0's replace runs and IBM removes it.
func testRolloutOnePerSettledCluster(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	r := f.reconciler()
	f.addSparePool(t)
	ibm := newRoIBM(f, "w0", "w1", "w2", "w3", "o0", "o1")
	f.ibm.EXPECT().ReplaceWorker(gomock.Any(), mpTestClusterID, "w0", true).DoAndReturn(func(context.Context, string, string, bool) error {
		persisted := f.stored().Status.Disruption
		g.Expect(persisted.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM), "persisted before the call")
		g.Expect(persisted.IBMRequestTime.IsZero()).To(BeFalse())
		return nil
	}).Times(1)

	phases := make([]infrav1.WorkerDisruptionPhase, 0, 3)
	for _, step := range []time.Duration{0, 20 * time.Second, 20 * time.Second} {
		f.clock.Step(step)
		f.reconcileNamed(r, "workers")
		phases = append(phases, f.stored().Status.Disruption.Phase)
		// The spare pool's workers are due under the Manual policy as well, but one removal runs at a time.
		f.reconcileNamed(r, "spare")
		g.Expect(f.storedNamed("spare").Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
		g.Expect(f.upToDate("spare").Reason).To(BeElementOf(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForDisruptionTokenReason,
			infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForSettledClusterReason))
	}
	g.Expect(phases).To(Equal([]infrav1.WorkerDisruptionPhase{infrav1.WorkerDisruptionPhaseDraining,
		infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, infrav1.WorkerDisruptionPhaseWaitingForRemoval}))
	g.Expect(workload.cordoned(t)).To(Equal([]string{"10.0.0.1"}), "only w0's Node")
	g.Expect(f.drainEvents()).To(ContainElement("Normal WorkerReplaceRequested asked IBM to replace worker w0 of pool workers " +
		"with update=true (MinorBehind)"))

	// IBM deletes w0: the cluster is not settled, so no pool starts another replace, and ReplaceWorker is not repeated.
	ibm.setState("w0", "deleting")
	f.clock.Step(30 * time.Second)
	f.reconcileNamed(r, "workers")
	g.Expect(f.upToDate("workers").Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateReplacingWorkerReason))
	f.reconcileNamed(r, "spare")
	g.Expect(f.upToDate("spare").Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForSettledClusterReason))
	g.Expect(f.upToDate("spare").Message).To(ContainSubstring("worker w0 is deleting"))
	g.Expect(workload.cordoned(t)).To(Equal([]string{"10.0.0.1"}))
	g.Expect(f.leaseHolder()).To(Equal("workers/pool-uid"))
}

// testRolloutWaitsForCapacity checks that a 1-worker IKS cluster can roll no worker (a replace would leave fewer ready
// workers than IBM's minimum of one), and that the message names the MachinePool size that allows a rolling
// replacement.
func testRolloutWaitsForCapacity(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	f.pool.Spec.Zones = []infrav1.WorkerPoolZone{{Name: "us-south-1"}}
	f.machinePool.Spec.Replicas = ptr.To[int32](1)
	r := f.reconciler()
	worker := roWorker("w0", mpTestPoolID, "us-south-1")
	worker.KubeVersionActual = "1.35.9_1530"
	f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{{ID: mpTestPoolID, Name: "workers",
		Flavor: mpTestFlavor, SizePerZone: 1, Zones: []kubernetesservice.WorkerPoolZone{{ID: "us-south-1", WorkerCount: 1, AutobalanceEnabled: true}}}}, nil)
	f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.Worker{worker}, nil)
	result := f.reconcileNamed(r, "workers")
	g.Expect(result.RequeueAfter).To(BeNumerically("~", 2*time.Minute, 15*time.Second), "guardedPoll")
	upToDate := f.upToDate("workers")
	g.Expect(upToDate.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForCapacityReason))
	g.Expect(upToDate.Message).To(ContainSubstring("scale MachinePool workers to 2 to allow a rolling replacement"))
	g.Expect(workload.cordoned(t)).To(BeEmpty())
	g.Expect(conditions.IsTrue(f.stored(), infrav1.IBMCloudManagedMachinePoolReadyCondition)).To(BeTrue(), "the pool itself is ready")
}

// testRolloutVersionIgnored checks that MachinePool spec.template.spec.version has no effect, and that
// WorkersUpToDate says so.
func testRolloutVersionIgnored(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	f.machinePool.Spec.Template.Spec.Version = "v1.30.0"
	r := f.reconciler()
	newRoIBM(f)
	f.reconcileNamed(r, "workers")
	upToDate := f.upToDate("workers")
	g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(upToDate.Message).To(HaveSuffix(managedscope.VersionIgnoredMessage))
	g.Expect(workload.calls.all()).To(BeEmpty(), "no workload write")
}

// testRolloutPatchUpdateAvailable checks that under the Manual policy a newer fix pack is only reported.
func testRolloutPatchUpdateAvailable(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	r := f.reconciler()
	ibm := newRoIBM(f)
	ibm.update(func(w *kubernetesservice.Worker) {
		if w.ID == "w0" || w.ID == "w1" || w.ID == "w2" {
			w.KubeVersionTarget = "1.36.5_1545"
		}
	})
	result := f.reconcileNamed(r, "workers")
	g.Expect(result.RequeueAfter).To(BeNumerically("~", 5*time.Minute, 30*time.Second), "steadyStatePoll")
	stored := f.stored()
	upToDate := conditions.Get(stored, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition)
	g.Expect(upToDate.Status).To(Equal(metav1.ConditionFalse))
	g.Expect(upToDate.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason))
	g.Expect(upToDate.Message).To(Equal("3 of 4 workers can move to IBM fix pack 1.36.5_1545; set spec.rollout.after to roll them, " +
		"or set spec.workerUpdates.policy: Automatic"))
	g.Expect(stored.Status.WorkersWithUpdateAvailable).To(Equal(ptr.To[int32](3)))
	g.Expect(stored.Status.TargetWorkerVersion).To(Equal("1.36.5_1545"))
	g.Expect(conditions.IsTrue(stored, infrav1.IBMCloudManagedMachinePoolReadyCondition)).To(BeTrue(), "the pool stays Ready")
	g.Expect(workload.cordoned(t)).To(BeEmpty())
	g.Expect(f.leaseHolder()).To(BeEmpty())
}

// testRolloutDrainBlocked checks that without a drain time limit a blocking budget holds the replace with DrainBlocked
// and no ReplaceWorker; once a limit is set and exceeded, the pods left are named in DrainTimeoutExceeded and exactly
// one ReplaceWorker goes out.
func testRolloutDrainBlocked(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	workload.refuse("web-w0", roPDBRefusal())
	r := f.reconciler()
	newRoIBM(f, "w0")
	for _, step := range []time.Duration{0, 20 * time.Second, 20 * time.Second, 10 * time.Minute} {
		f.clock.Step(step)
		f.reconcileNamed(r, "workers")
		stored := f.stored()
		g.Expect(stored.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining), "no ReplaceWorker while a budget blocks")
		draining := conditions.Get(stored, infrav1.IBMCloudManagedMachinePoolDrainingCondition)
		g.Expect(draining.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(draining.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolDrainingDrainBlockedReason))
	}
	g.Expect(f.drainEvents()).To(ContainElement(HavePrefix("Warning DrainBlocked")))

	// The user sets a drain time limit that has already passed since the drain started.
	machinePool := &clusterv1.MachinePool{}
	g.Expect(f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: "workers"}, machinePool)).To(Succeed())
	machinePool.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](300)
	g.Expect(f.mgmt.Update(context.Background(), machinePool)).To(Succeed())
	f.clock.Step(20 * time.Second)
	f.reconcileNamed(r, "workers")
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	g.Expect(f.drainEvents()).To(ContainElement(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 300s")))

	f.ibm.EXPECT().ReplaceWorker(gomock.Any(), mpTestClusterID, "w0", true).Return(nil).Times(1)
	for _, step := range []time.Duration{20 * time.Second, 0, time.Minute} {
		f.clock.Step(step)
		f.reconcileNamed(r, "workers")
	}
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
	g.Expect(workload.node(t, "w0").Spec.Unschedulable).To(BeTrue(), "the Node stays cordoned for IBM")
}

// testRolloutMinReadySeconds checks that, with w0 replaced by w4 and w1 due next, w1's Node is not cordoned until w4's
// Node has been Ready for minReadySeconds, and that the reconcile is requeued for the end of that soak.
func testRolloutMinReadySeconds(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	f.machinePool.Spec.Template.Spec.MinReadySeconds = ptr.To[int32](300)
	sent := metav1.NewTime(mpTestNow.Add(-10 * time.Minute))
	f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace,
		Phase: infrav1.WorkerDisruptionPhaseWaitingForReplacement, WorkerIDs: []string{"w0"},
		DrainStartTime: metav1.NewTime(mpTestNow.Add(-15 * time.Minute)), IBMRequestTime: sent}
	f.pool.Status.LastDisruptionTime = sent
	r := f.reconciler()
	g.Expect(f.mgmt.Create(context.Background(), &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: mpTestNamespace, Name: mpTestCluster + "-ibm-worker-disruption"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: ptr.To("workers/pool-uid")}})).To(Succeed())
	ibm := newRoIBM(f, "w1")
	ibm.setState("w0", "deleted")
	ibm.update(func(*kubernetesservice.Worker) {})
	ibm.mu.Lock()
	ibm.workers = append(ibm.workers, roWorker("w4", mpTestPoolID, "us-south-1"))
	ibm.mu.Unlock()
	g.Expect(workload.client.Delete(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: roNodeName("w0")}})).To(Succeed())
	g.Expect(workload.client.Create(context.Background(), roNode("w4", mpTestNow.Add(-time.Minute)))).To(Succeed())

	result := f.reconcileNamed(r, "workers")
	g.Expect(result.RequeueAfter).To(BeNumerically("~", 30*time.Second, 3*time.Second), "poolTransitionPoll comes before the soak ends")
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForReplacement))
	g.Expect(workload.cordoned(t)).To(BeEmpty(), "w1 is not touched while w4 soaks")

	f.clock.Step(4*time.Minute + time.Second)
	f.reconcileNamed(r, "workers")
	g.Expect(f.stored().Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(f.upToDate("workers").Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason))
	g.Expect(workload.cordoned(t)).To(BeEmpty(), "the next worker starts from idle")

	f.clock.Step(30 * time.Second)
	f.reconcileNamed(r, "workers")
	g.Expect(workload.cordoned(t)).To(Equal([]string{"10.0.0.2"}), "then w1")
}

// testRolloutResumesFromMarkers checks a restart mid-drain: a clusterctl move or a lost status patch cleared
// status.disruption and the Lease while w0's Node carries this pool's drain markers; the replace resumes at Draining
// with the drain start of the marker, re-acquires the token and does not announce the drain again.
func testRolloutResumesFromMarkers(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	r := f.reconciler()
	newRoIBM(f, "w0", "w1")
	start := mpTestNow.Add(-5 * time.Minute)
	helper := &drain.Helper{Client: workload.client, Clock: clocktesting.NewFakeClock(start), EvictionBudget: time.Second,
		AnnotationPrefix: infrav1.NodeAnnotationPrefix}
	g.Expect(helper.Cordon(context.Background(), workload.node(t, "w0"), drain.CordonOptions{
		Owner: managedscope.DrainOwner(f.pool), ExcludeFromLoadBalancers: true})).To(Succeed())

	f.reconcileNamed(r, "workers")
	stored := f.stored()
	g.Expect(stored.Status.Disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationReplace))
	g.Expect(stored.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	g.Expect(stored.Status.Disruption.WorkerIDs).To(Equal([]string{"w0"}))
	g.Expect(stored.Status.Disruption.DrainStartTime.Time).To(BeTemporally("==", start), "the original drain start")
	g.Expect(f.leaseHolder()).To(Equal("workers/pool-uid"), "the token is re-acquired")
	g.Expect(f.drainEvents()).NotTo(ContainElement(HavePrefix("Normal DrainStarted")))
	g.Expect(workload.cordoned(t)).To(Equal([]string{"10.0.0.1"}))
}

// testRolloutTwoPools checks that only the Lease holder cordons: both pools of the Cluster have a due worker; the pool
// that takes the Lease cordons its worker's Node, the other reports WaitingForDisruptionToken and cordons nothing.
func testRolloutTwoPools(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	r := f.reconciler()
	f.addSparePool(t)
	newRoIBM(f, "w0", "o0")
	f.reconcileNamed(r, "spare")
	f.reconcileNamed(r, "workers")
	g.Expect(f.leaseHolder()).To(Equal("spare/spare-uid"))
	g.Expect(workload.cordoned(t)).To(Equal([]string{"10.0.1.1"}), "only the Lease holder cordons")
	g.Expect(f.storedNamed("workers").Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	upToDate := f.upToDate("workers")
	g.Expect(upToDate.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateWaitingForDisruptionTokenReason))
	g.Expect(upToDate.Message).To(ContainSubstring("worker w0 (MinorBehind)"))
}

// testRolloutAbortKeepsHumanCordon checks an abort: a human cordoned w0's Node before the replace; once the replace is
// no longer wanted before its IBM call (the policy set back to Manual), CAPIBM reverts its own load balancer label and
// markers, keeps the human's cordon, releases the token and clears status.disruption.
func testRolloutAbortKeepsHumanCordon(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	f.pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyAutomatic
	r := f.reconciler()
	ibm := newRoIBM(f)
	ibm.update(func(w *kubernetesservice.Worker) {
		if w.ID == "w0" {
			w.KubeVersionTarget = "1.36.5_1545"
		}
	})
	human := workload.node(t, "w0")
	human.Spec.Unschedulable = true
	g.Expect(workload.client.Update(context.Background(), human)).To(Succeed())

	f.reconcileNamed(r, "workers")
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	g.Expect(workload.node(t, "w0").Labels).To(HaveKey(corev1.LabelNodeExcludeBalancers), "CAPIBM's load balancer exclusion")

	pool := f.stored()
	pool.Spec.WorkerUpdates.Policy = infrav1.WorkerUpdatePolicyManual
	g.Expect(f.mgmt.Update(context.Background(), pool)).To(Succeed())
	f.clock.Step(20 * time.Second)
	f.reconcileNamed(r, "workers")
	node := workload.node(t, "w0")
	g.Expect(node.Spec.Unschedulable).To(BeTrue(), "the human's cordon stays")
	g.Expect(node.Labels).NotTo(HaveKey(corev1.LabelNodeExcludeBalancers))
	g.Expect(node.Annotations).NotTo(HaveKey(infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner))
	g.Expect(f.stored().Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(f.leaseHolder()).To(BeEmpty())
	g.Expect(f.drainEvents()).To(ContainElement(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 1 Nodes of pool workers")))
	g.Expect(f.upToDate("workers").Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDatePatchUpdateAvailableReason))
}

// testRolloutRequeuesAtRolloutAfter checks the requeue at the time rollout.after passes: the up-to-date pool is
// requeued just after spec.rollout.after, and that reconcile starts the replace of the oldest Node's worker.
func testRolloutRequeuesAtRolloutAfter(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	f.pool.Spec.Rollout.After = metav1.NewTime(mpTestNow.Add(2 * time.Minute))
	r := f.reconciler()
	newRoIBM(f)
	result := f.reconcileNamed(r, "workers")
	g.Expect(result.RequeueAfter).To(Equal(2*time.Minute+time.Second), "no jitter: a deadline, not a poll")
	g.Expect(f.upToDate("workers").Status).To(Equal(metav1.ConditionTrue))
	g.Expect(workload.cordoned(t)).To(BeEmpty())

	f.clock.Step(result.RequeueAfter)
	f.reconcileNamed(r, "workers")
	g.Expect(f.stored().Status.Disruption.WorkerIDs).To(Equal([]string{"w0"}))
	g.Expect(f.upToDate("workers").Message).To(HavePrefix("replaced 0 of 4; worker w0 (RolloutRequested): "))
	g.Expect(workload.cordoned(t)).To(Equal([]string{"10.0.0.1"}))
	g.Expect(workload.calls.all()).NotTo(BeEmpty())
}

// testRolloutPaused checks that a paused Cluster makes no IBM call and no workload-cluster write, also with a replace
// in flight (gomock fails on any IBM call; the workload records none).
func testRolloutPaused(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.rollingPool(t)
	f.cluster.Spec.Paused = ptr.To(true)
	f.pool.Status.Disruption = infrav1.WorkerDisruptionStatus{Operation: infrav1.WorkerDisruptionOperationReplace,
		Phase: infrav1.WorkerDisruptionPhaseDraining, WorkerIDs: []string{"w0"}, DrainStartTime: metav1.NewTime(mpTestNow)}
	r := f.reconciler()
	_, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.calls.all()).To(BeEmpty())
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
}

// roIndexedClient answers a Node listing by provider ID, which the cached client of ClusterCache answers from its
// NodeProviderIDIndex, by filtering the API server's listing: the API server has no field selector on
// spec.providerID. Every other call goes to the API server unchanged, so cordons, evictions and pod listings are real.
type roIndexedClient struct {
	client.Client
}

// List implements client.Reader.
func (c roIndexedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	nodes, isNodes := list.(*corev1.NodeList)
	listOpts := &client.ListOptions{}
	listOpts.ApplyOptions(opts)
	if !isNodes || listOpts.FieldSelector == nil {
		return c.Client.List(ctx, list, opts...)
	}
	providerID, found := listOpts.FieldSelector.RequiresExactMatch(index.NodeProviderIDField)
	if !found {
		return c.Client.List(ctx, list, opts...)
	}
	all := &corev1.NodeList{}
	if err := c.Client.List(ctx, all); err != nil {
		return err
	}
	for i := range all.Items {
		if all.Items[i].Spec.ProviderID == providerID {
			nodes.Items = append(nodes.Items, all.Items[i])
		}
	}
	return nil
}

// TestIBMCloudManagedMachinePoolRolloutEnvtest runs one worker replace against the envtest API server, which also plays
// the workload cluster: the disruption token is a real Lease, the cordon a real Node patch, the eviction goes through
// the real Eviction subresource, every persisted status.disruption of a Replace (WaitingForReplacement included)
// passes the CRD schema, and ReplaceWorker is sent once, with update=true, only after the evicted pod is gone.
// Reconcile is called directly, so no manager runs; the Cluster is paused at cleanup all the same, so that a manager
// of a later test that watches every namespace leaves this test's objects alone. Nodes are cluster-scoped, so their
// names and worker IDs are unique to this test.
func TestIBMCloudManagedMachinePoolRolloutEnvtest(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "pool-rollout")
	cluster, pool := envtestDeletablePool(t, namespace, "dev")
	pauseAtCleanup(t, cluster, client.ObjectKeyFromObject(pool))
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	g.Expect(testEnv.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "dev"}, cp)).To(Succeed())
	cp.Status.DistributionVersion = "1.36.5_1537"
	g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())

	// Every worker runs the minor before the master's, so all four are due (workers follow the master minor); IBM's
	// order picks ro-w0 first.
	var mu sync.Mutex
	workers := make([]kubernetesservice.Worker, 0, 5)
	for i, id := range []string{"ro-w0", "ro-w1", "ro-w2", "ro-w3"} {
		w := roWorker(id, mpTestPoolID, []string{"us-south-1", "us-south-2"}[i%2])
		w.KubeVersionActual, w.KubeVersionTarget = "1.35.9_1530", "1.35.9_1530"
		workers = append(workers, w)
		node := "10.0.8." + string(rune('1'+i))
		envtestScaleDownNode(t, node, id)
		envtestRunningPod(t, namespace, "web-"+id, node)
	}
	ibm := ksmock.NewMockKubernetesService(gomock.NewController(t))
	ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return([]kubernetesservice.WorkerPool{{ID: mpTestPoolID, Name: "workers",
		Flavor: mpTestFlavor, SizePerZone: 2, Zones: []kubernetesservice.WorkerPoolZone{{ID: "us-south-1", WorkerCount: 2, AutobalanceEnabled: true},
			{ID: "us-south-2", WorkerCount: 2, AutobalanceEnabled: true}}}}, nil).AnyTimes()
	ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).DoAndReturn(func(context.Context, string) ([]kubernetesservice.Worker, error) {
		mu.Lock()
		defer mu.Unlock()
		return append([]kubernetesservice.Worker(nil), workers...), nil
	}).AnyTimes()
	reader := testEnv.GetAPIReader()
	key := client.ObjectKeyFromObject(pool)
	stored := func() *infrav1.IBMCloudManagedMachinePool {
		latest := &infrav1.IBMCloudManagedMachinePool{}
		g.Expect(reader.Get(ctx, key, latest)).To(Succeed())
		return latest
	}
	podsOn := func(node string) []corev1.Pod {
		pods := &corev1.PodList{}
		g.Expect(reader.List(ctx, pods, client.InNamespace(namespace), client.MatchingFields{"spec.nodeName": node})).To(Succeed())
		return pods.Items
	}
	ibm.EXPECT().ReplaceWorker(gomock.Any(), mpTestClusterID, "ro-w0", true).DoAndReturn(func(context.Context, string, string, bool) error {
		g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM), "persisted before the call")
		g.Expect(podsOn("10.0.8.1")).To(BeEmpty(), "the victim's pod is gone before the IBM call")
		return nil
	}).Times(1)

	clock := clocktesting.NewFakeClock(time.Now())
	r := &IBMCloudManagedMachinePoolReconciler{Client: uncachedClient(t), APIReader: reader, Recorder: newMachinePoolFixture(t).recorder,
		Resolver: &recordingResolver{}, ClientBuilder: &stubClientBuilder{kubernetes: ibm},
		ClusterCache: clustercache.NewFakeClusterCache(roIndexedClient{uncachedClient(t)}, client.ObjectKeyFromObject(cluster)), Clock: clock}
	reconcile := func(step time.Duration) {
		t.Helper()
		clock.Step(step)
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
	}
	// The first two reconciles add the finalizer and record the pause state; the third observes the pool and, as its
	// first observation, sets lastDisruptionTime, which starts the settle delay.
	for range 3 {
		reconcile(0)
	}
	g.Expect(stored().Status.Disruption.Operation).To(BeEmpty())

	// Draining: ro-w0 cordoned, its pod evicted through the API server and Terminating.
	reconcile(managedscope.DisruptionSettleDelay + time.Second)
	disruption := stored().Status.Disruption
	g.Expect(disruption.Operation).To(Equal(infrav1.WorkerDisruptionOperationReplace))
	g.Expect(disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	g.Expect(disruption.WorkerIDs).To(Equal([]string{"ro-w0"}))
	g.Expect(reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "dev-ibm-worker-disruption"}, &coordinationv1.Lease{})).To(Succeed())
	cordoned := &corev1.Node{}
	g.Expect(reader.Get(ctx, client.ObjectKey{Name: "10.0.8.1"}, cordoned)).To(Succeed())
	g.Expect(cordoned.Spec.Unschedulable).To(BeTrue())
	pods := podsOn("10.0.8.1")
	g.Expect(pods).To(HaveLen(1))
	g.Expect(pods[0].DeletionTimestamp).NotTo(BeNil(), "the eviction was accepted")

	// An accepted eviction is not enough: the drain waits while the pod terminates.
	reconcile(20 * time.Second)
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))

	// The kubelet's final delete (played by the test) finishes the drain; then the detach wait, then ReplaceWorker.
	for _, pod := range podsOn("10.0.8.1") {
		g.Expect(testEnv.Delete(ctx, &pod, client.GracePeriodSeconds(0))).To(Succeed())
	}
	g.Eventually(func() []corev1.Pod { return podsOn("10.0.8.1") }, 10*time.Second, 100*time.Millisecond).Should(BeEmpty())
	reconcile(20 * time.Second)
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	reconcile(20 * time.Second)
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))

	// IBM deletes ro-w0 and its Node and lists the replacement ro-w4, whose Node is Ready.
	mu.Lock()
	workers[0].ActualState = "deleted"
	workers = append(workers, roWorker("ro-w4", mpTestPoolID, "us-south-1"))
	mu.Unlock()
	g.Expect(testEnv.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "10.0.8.1"}})).To(Succeed())
	envtestScaleDownNode(t, "10.0.8.5", "ro-w4")
	reconcile(30 * time.Second)
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForReplacement))

	// The replacement is deployed, normal and Ready: the token goes and status.disruption is cleared.
	reconcile(30 * time.Second)
	final := stored()
	g.Expect(final.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(apierrors.IsNotFound(reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "dev-ibm-worker-disruption"},
		&coordinationv1.Lease{}))).To(BeTrue())
	upToDate := conditions.Get(final, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition)
	g.Expect(upToDate.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkersUpToDateRollingOutReason))
	g.Expect(upToDate.Message).To(Equal("replaced 1 of 4; the next worker starts once the cluster has settled"))
	g.Expect(podsOn("10.0.8.2")).To(HaveLen(1), "the other workers' pods were never touched")
}
