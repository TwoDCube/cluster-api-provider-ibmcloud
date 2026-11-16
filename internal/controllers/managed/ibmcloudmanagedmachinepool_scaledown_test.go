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
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
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

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"

	. "github.com/onsi/gomega"
)

// The scale-down tests call Reconcile on a pool whose MachinePool asks for one worker per zone less than IBM's pool
// has. They prove what the scope tests cannot: the controller persists every phase before the next one starts, the
// reconcile that the pool's own status patch triggers at once sends no second resize, the Warning reaches the event
// recorder, and the reconciler's first-seen map decides whether another pool's stuck worker holds the scale-down. The
// step logic itself is tested in pkg/cloud/scope/managed.

// sdTestNode returns the Node name of a test worker: 10.0.0.1-4 for w0-w3.
func sdTestNode(workerID string) string {
	return "10.0.0." + string(rune('1'+workerID[1]-'0'))
}

// sdTestWorkers are four ready workers w0..w3 of the test pool (w0, w2 in us-south-1; w1, w3 in us-south-2) and two
// ready workers o0, o1 of another pool, each in states[id] when given ("deployed" otherwise). IBM's order picks w0 and
// w1 when the pool shrinks by one worker per zone.
func sdTestWorkers(states map[string]string) []kubernetesservice.Worker {
	workers := make([]kubernetesservice.Worker, 0, 6)
	for i, id := range []string{"w0", "w1", "w2", "w3", "o0", "o1"} {
		poolID, zone := mpTestPoolID, "us-south-1"
		if i > 3 {
			poolID = "other-pool"
		}
		if i%2 == 1 {
			zone = "us-south-2"
		}
		state := "deployed"
		if s, found := states[id]; found {
			state = s
		}
		workers = append(workers, kubernetesservice.Worker{ID: id, PoolID: poolID, Zone: zone, ActualState: state, HealthState: "normal",
			KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"})
	}
	return workers
}

// sdTestPool is the IBM pool of the fixture with sizePerZone workers in each of its two zones.
func sdTestPool(sizePerZone int64) []kubernetesservice.WorkerPool {
	return []kubernetesservice.WorkerPool{{ID: mpTestPoolID, Name: "workers", Flavor: mpTestFlavor, SizePerZone: sizePerZone,
		Zones: []kubernetesservice.WorkerPoolZone{
			{ID: "us-south-1", WorkerCount: sizePerZone, AutobalanceEnabled: true},
			{ID: "us-south-2", WorkerCount: sizePerZone, AutobalanceEnabled: true},
		}}}
}

// scaleDownWorkload is the fixture's workload cluster for a scale-down.
type scaleDownWorkload struct {
	client client.Client
	calls  *orderedCalls
}

// shrinkingMachinePool asks the fixture's MachinePool for one worker per zone (replicas 2) while IBM's pool has two,
// with the last removal an hour ago, and gives the pool a connected workload cluster with one Node and one pod per
// worker w0..w3.
func (f *machinePoolFixture) shrinkingMachinePool(t *testing.T) *scaleDownWorkload {
	t.Helper()
	f.machinePool.Spec.Replicas = ptr.To[int32](2)
	f.pool.Status.PoolID = mpTestPoolID
	f.pool.Status.LastDisruptionTime = metav1.NewTime(mpTestNow.Add(-time.Hour))
	w := &scaleDownWorkload{calls: &orderedCalls{}}
	objs := make([]client.Object, 0, 8)
	for _, id := range []string{"w0", "w1", "w2", "w3"} {
		objs = append(objs,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: sdTestNode(id)}, Spec: corev1.NodeSpec{ProviderID: "ibm://" + mpTestAccount + "///" + mpTestClusterID + "/" + id}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web-" + id}, Spec: corev1.PodSpec{NodeName: sdTestNode(id)}})
	}
	w.client = fake.NewClientBuilder().WithScheme(machinePoolScheme(t)).WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				w.calls.add("patch " + obj.GetName())
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourceCreate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
				w.calls.add("evict " + obj.GetName())
				return c.SubResource(subResource).Create(ctx, obj, sub, opts...)
			},
		}).Build()
	f.clusterCache = clustercache.NewFakeClusterCache(w.client, client.ObjectKey{Namespace: mpTestNamespace, Name: mpTestCluster})
	return w
}

// unschedulable returns the names of the cordoned Nodes of the workload cluster, in name order.
func (w *scaleDownWorkload) unschedulable(t *testing.T) []string {
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

// podsOn returns the pods bound to node.
func (w *scaleDownWorkload) podsOn(t *testing.T, node string) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	NewWithT(t).Expect(w.client.List(context.Background(), pods, client.MatchingFields{"spec.nodeName": node})).To(Succeed())
	return pods.Items
}

// listings returns the gomock calls of one reconcile's IBM reads: the pool with sizePerZone workers per zone and the
// workers.
func (f *machinePoolFixture) listings(sizePerZone int64, workers []kubernetesservice.Worker) []any {
	return []any{
		f.ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).Return(sdTestPool(sizePerZone), nil),
		f.ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).Return(workers, nil),
	}
}

// workerPoolReady returns the stored WorkerPoolReady condition.
func (f *machinePoolFixture) workerPoolReady() *metav1.Condition {
	return conditions.Get(f.stored(), infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
}

// drainEvents returns the events recorded so far.
func (f *machinePoolFixture) drainEvents() []string {
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

func TestIBMCloudManagedMachinePoolReconcileScaleDown(t *testing.T) {
	t.Run("every phase is persisted and ResizeWorkerPool comes only after the drain", testScaleDownPersistsEveryPhase)
	t.Run("a racing E1437 sends no second resize within DisruptionSettleDelay", testScaleDownRacingE1437)
	t.Run("a mispredicted victim is reported and uncordoned", testScaleDownMispredicted)
	t.Run("a shrink is allowed on an unsupported cluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.cp.Status.State = "unsupported"
		workload := f.shrinkingMachinePool(t)
		f.listings(2, sdTestWorkers(nil))
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically("~", 20*time.Second, 3*time.Second), "drainRetryInterval")
		g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
		g.Expect(workload.unschedulable(t)).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
	})
	t.Run("no shrink while the cluster is unavailable", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.cp.Status.State = "unavailable"
		workload := f.shrinkingMachinePool(t)
		f.listings(2, sdTestWorkers(nil))
		result, err := f.reconcile(f.reconciler())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically("~", 2*time.Minute, 15*time.Second), "guardedPoll")
		g.Expect(f.workerPoolReady().Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason))
		g.Expect(workload.calls.all()).To(BeEmpty(), "nothing is cordoned")
	})
}

// testScaleDownPersistsEveryPhase follows one scale-down through Reconcile. gomock.InOrder proves that
// ResizeWorkerPool is sent only in the third reconcile, after the drain and the detach wait; its DoAndReturn proves that
// the victims' pods are gone and that phase WaitingForIBM is already persisted then. The stored phases run Draining →
// WaitingForVolumeDetach → WaitingForIBM → WaitingForRemoval, then status.disruption is cleared and the token released.
func testScaleDownPersistsEveryPhase(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.shrinkingMachinePool(t)
	r := f.reconciler()
	steady := sdTestWorkers(nil)
	resize := f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID, int64(1)).DoAndReturn(
		func(context.Context, string, string, int64) error {
			persisted := f.stored().Status.Disruption
			g.Expect(persisted.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM), "persisted before the call")
			g.Expect(persisted.IBMRequestTime.IsZero()).To(BeFalse())
			g.Expect(workload.podsOn(t, "10.0.0.1")).To(BeEmpty(), "the victims are drained before the IBM call")
			g.Expect(workload.podsOn(t, "10.0.0.2")).To(BeEmpty())
			return nil
		})
	calls := make([]any, 0, 11)
	calls = append(calls, f.listings(2, steady)...)
	calls = append(calls, f.listings(2, steady)...)
	calls = append(calls, f.listings(2, steady)...)
	calls = append(calls, resize)
	calls = append(calls, f.listings(1, sdTestWorkers(map[string]string{"w0": "deleting", "w1": "deleting"}))...)
	calls = append(calls, f.listings(1, sdTestWorkers(map[string]string{"w0": "deleted", "w1": "deleted"}))...)
	gomock.InOrder(calls...)

	phases := make([]infrav1.WorkerDisruptionPhase, 0, 4)
	for _, step := range []time.Duration{0, 20 * time.Second, 20 * time.Second, 30 * time.Second} {
		f.clock.Step(step)
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		phases = append(phases, f.stored().Status.Disruption.Phase)
	}
	g.Expect(phases).To(Equal([]infrav1.WorkerDisruptionPhase{infrav1.WorkerDisruptionPhaseDraining,
		infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, infrav1.WorkerDisruptionPhaseWaitingForRemoval,
		infrav1.WorkerDisruptionPhaseWaitingForRemoval}))
	g.Expect(workload.calls.all()[:4]).To(Equal([]string{"patch 10.0.0.1", "patch 10.0.0.2", "evict web-w0", "evict web-w1"}),
		"both victims are cordoned before the first eviction; w2 and w3 are untouched")
	g.Expect(f.stored().Status.LastDisruptionTime.Time).To(BeTemporally("==", mpTestNow.Add(40*time.Second)))

	f.clock.Step(30 * time.Second)
	result, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(BeNumerically("~", 5*time.Minute, 30*time.Second), "steadyStatePoll once the pool is ready")
	stored := f.stored()
	g.Expect(stored.Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(stored.Spec.ProviderIDList).To(HaveLen(2))
	g.Expect(conditions.IsTrue(stored, infrav1.IBMCloudManagedMachinePoolReadyCondition)).To(BeTrue())
	g.Expect(conditions.IsFalse(stored, infrav1.IBMCloudManagedMachinePoolDrainingCondition)).To(BeTrue())
	err = f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: mpTestCluster + "-ibm-worker-disruption"}, &coordinationv1.Lease{})
	g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the disruption token is released")
	g.Expect(f.drainEvents()).To(Equal([]string{"Normal DrainStarted cordoning 2 Nodes of pool workers, then evicting their pods",
		"Normal DrainCompleted the 2 Nodes of pool workers are drained"}))
}

// testScaleDownRacingE1437 lets IBM refuse the resize with E1437, the synchronous, side-effect-free refusal IBM returns
// when a change would take the cluster below its minimum workers: the pool holds as WaitingForClusterMinimum with its
// victims cordoned, and neither the reconcile its own status patch triggers at once nor one within
// DisruptionSettleDelay sends the resize again; the next one after the delay does.
func testScaleDownRacingE1437(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.shrinkingMachinePool(t)
	r := f.reconciler()
	e1437 := &kubernetesservice.APIError{Operation: kubernetesservice.OpResizeWorkerPool, StatusCode: http.StatusBadRequest, Code: "E1437"}
	steady := sdTestWorkers(nil)
	for range 3 {
		f.listings(2, steady)
	}
	f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID, int64(1)).Return(e1437)
	for _, step := range []time.Duration{0, 20 * time.Second, 20 * time.Second} {
		f.clock.Step(step)
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
	}
	minimum := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterMinimumReason
	g.Expect(f.workerPoolReady().Reason).To(Equal(minimum))
	g.Expect(f.stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))
	refusedAt := f.stored().Status.Disruption.IBMRequestTime

	// The reconcile the status patch triggers at once, and the requeues within the settle delay: no second call.
	for _, step := range []time.Duration{0, time.Minute, time.Minute} {
		f.clock.Step(step)
		f.listings(2, steady)
		result, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeNumerically("~", 2*time.Minute, 15*time.Second), "guardedPoll, never an error storm")
		g.Expect(f.workerPoolReady().Reason).To(Equal(minimum))
		g.Expect(workload.unschedulable(t)).To(Equal([]string{"10.0.0.1", "10.0.0.2"}), "the Nodes stay cordoned")
	}

	f.clock.Step(2 * time.Minute)
	f.listings(2, steady)
	f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID, int64(1)).Return(e1437)
	_, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.stored().Status.Disruption.IBMRequestTime.Time).To(BeTemporally(">", refusedAt.Time.Add(managedscope.DisruptionSettleDelay)))
}

// testScaleDownMispredicted lets IBM's resize remove w2 instead of the predicted w0: the Warning
// UnexpectedScaleDownVictim names both sets, w0's Node is uncordoned and the removal waits for w1 and w2. That IBM's
// resize removes the predicted victims is not yet verified against IBM Cloud, so a misprediction must be handled.
func testScaleDownMispredicted(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.shrinkingMachinePool(t)
	r := f.reconciler()
	steady := sdTestWorkers(nil)
	for range 3 {
		f.listings(2, steady)
	}
	f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID, int64(1)).Return(nil)
	for _, step := range []time.Duration{0, 20 * time.Second, 20 * time.Second} {
		f.clock.Step(step)
		_, err := f.reconcile(r)
		g.Expect(err).NotTo(HaveOccurred())
	}
	f.drainEvents()

	f.clock.Step(30 * time.Second)
	f.listings(1, sdTestWorkers(map[string]string{"w1": "deleting", "w2": "deleting"}))
	_, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.drainEvents()).To(ConsistOf("Warning UnexpectedScaleDownVictim IBM's resize of pool workers removes workers w1, w2, " +
		"not the predicted workers w0, w1; CAPIBM waits for the workers IBM removes and uncordoned the Nodes of the spared worker w0"))
	g.Expect(workload.unschedulable(t)).To(Equal([]string{"10.0.0.2"}))
	stored := f.stored()
	g.Expect(stored.Status.Disruption.WorkerIDs).To(Equal([]string{"w1", "w2"}))
	g.Expect(stored.Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))
}

// TestIBMCloudManagedMachinePoolScaleDownStuckWorker checks that a worker IBM has left transitional for longer than
// WorkerStuckAfter no longer holds a scale-down. IBM reports no time for a transition, so the reconciler's first-seen
// map, fed by its own listings, decides whether another pool's provision_pending worker still holds the scale-down's
// gates.
func TestIBMCloudManagedMachinePoolScaleDownStuckWorker(t *testing.T) {
	settled := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForSettledClusterReason
	draining := infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyDrainingNodeReason
	stuck := sdTestWorkers(nil)
	stuck = append(stuck, kubernetesservice.Worker{ID: "o2", PoolID: "other-pool", Zone: "us-south-1", ActualState: "provision_pending",
		KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"})
	reconcileAt := func(t *testing.T, f *machinePoolFixture, r *IBMCloudManagedMachinePoolReconciler, at time.Time) string {
		t.Helper()
		f.clock.SetTime(at)
		f.listings(2, stuck)
		_, err := f.reconcile(r)
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		return f.workerPoolReady().Reason
	}
	t.Run("a worker of another pool transitional for more than 60m no longer blocks the scale-down", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		workload := f.shrinkingMachinePool(t)
		r := f.reconciler()
		// The listings every 10 minutes keep the map current; the scale-down holds while o2 is changing for up to 60m.
		for at := time.Duration(0); at < managedscope.WorkerStuckAfter; at += 10 * time.Minute {
			g.Expect(reconcileAt(t, f, r, mpTestNow.Add(at))).To(Equal(settled), "held at +%s", at)
		}
		g.Expect(workload.calls.all()).To(BeEmpty(), "nothing is cordoned while the gate holds")
		g.Expect(reconcileAt(t, f, r, mpTestNow.Add(managedscope.WorkerStuckAfter+time.Minute))).To(Equal(draining))
		g.Expect(f.workerPoolReady().Message).To(ContainSubstring("WorkerOperationStuck: IBM has been changing worker o2 for more than 60 minutes"))
		g.Expect(workload.unschedulable(t)).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))
	})
	t.Run("after a restart it blocks again until WorkerStuckAfter has passed", func(t *testing.T) {
		g := NewWithT(t)
		f := newMachinePoolFixture(t)
		f.shrinkingMachinePool(t)
		r := f.reconciler()
		for at := time.Duration(0); at < managedscope.WorkerStuckAfter; at += 10 * time.Minute {
			reconcileAt(t, f, r, mpTestNow.Add(at))
		}
		r = restarted(r)
		g.Expect(reconcileAt(t, f, r, mpTestNow.Add(managedscope.WorkerStuckAfter+time.Minute))).To(Equal(settled),
			"the empty map fails closed")
	})
}

// TestIBMCloudManagedMachinePoolScaleDownFirstReconcile checks through Reconcile that a removal in flight goes first: a
// scale-down whose replicas went back up before its resize is given up (its cordons reverted, the token released)
// before the pool grows, in a reconcile that makes no resize call.
func TestIBMCloudManagedMachinePoolScaleDownFirstReconcile(t *testing.T) {
	g := NewWithT(t)
	f := newMachinePoolFixture(t)
	workload := f.shrinkingMachinePool(t)
	r := f.reconciler()
	f.listings(2, sdTestWorkers(nil))
	_, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.unschedulable(t)).To(Equal([]string{"10.0.0.1", "10.0.0.2"}))

	// The user scales back up beyond IBM's pool before the resize.
	machinePool := &clusterv1.MachinePool{}
	g.Expect(f.mgmt.Get(context.Background(), client.ObjectKey{Namespace: mpTestNamespace, Name: "workers"}, machinePool)).To(Succeed())
	machinePool.Spec.Replicas = ptr.To[int32](6)
	g.Expect(f.mgmt.Update(context.Background(), machinePool)).To(Succeed())
	f.clock.Step(20 * time.Second)
	f.listings(2, sdTestWorkers(nil))
	_, err = f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.unschedulable(t)).To(BeEmpty())
	g.Expect(f.stored().Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(f.drainEvents()).To(ContainElement(HavePrefix("Normal NodeUncordoned reverted CAPIBM's cordon of 2 Nodes of pool workers")))

	// The next reconcile grows the pool.
	f.listings(2, sdTestWorkers(nil))
	f.ibm.EXPECT().ResizeWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID, int64(3)).Return(nil)
	result, err := f.reconcile(r)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result).NotTo(Equal(ctrl.Result{}))
}

// envtestScaleDownNode creates a Ready Node of the envtest API server for worker id of the test cluster and deletes it
// when the test ends. Nodes are cluster-scoped, so the names and worker IDs are unique to this test: no other test's
// pool matches their provider IDs.
func envtestScaleDownNode(t *testing.T, name, workerID string) {
	t.Helper()
	g := NewWithT(t)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.NodeSpec{ProviderID: "ibm://" + mpTestAccount + "///" + mpTestClusterID + "/" + workerID}}
	g.Expect(testEnv.Create(ctx, node)).To(Succeed())
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	g.Expect(testEnv.Status().Update(ctx, node)).To(Succeed())
	t.Cleanup(func() {
		g.Expect(client.IgnoreNotFound(testEnv.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}))).To(Succeed())
	})
}

// envtestRunningPod creates a Running pod bound to node; without a kubelet, an evicted pod stays Terminating until
// the test deletes it with grace 0.
func envtestRunningPod(t *testing.T, namespace, name, node string) {
	t.Helper()
	g := NewWithT(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "app", Image: "registry.example/app:1"}}}}
	g.Expect(testEnv.Create(ctx, pod)).To(Succeed())
	pod.Status.Phase = corev1.PodRunning
	g.Expect(testEnv.Status().Update(ctx, pod)).To(Succeed())
}

// TestIBMCloudManagedMachinePoolScaleDownEnvtest runs one scale-down against the envtest API server, which also plays
// the workload cluster: the disruption token is a real Lease, the cordons are real Node patches with a resourceVersion
// precondition, the evictions go through the real Eviction subresource, every persisted status.disruption passes the
// CRD schema, and the resize is sent only once the evicted pods are gone, not when the evictions were accepted.
// Reconcile is called directly, so no manager runs; the Cluster is paused at cleanup all the same, so that no manager
// of another test reconciles it on the envtest API server all tests of the package share.
func TestIBMCloudManagedMachinePoolScaleDownEnvtest(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "pool-scaledown")
	cluster, pool := envtestDeletablePool(t, namespace, "dev")
	pauseAtCleanup(t, cluster, client.ObjectKeyFromObject(pool))
	machinePool := &clusterv1.MachinePool{}
	g.Expect(testEnv.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "workers"}, machinePool)).To(Succeed())
	machinePool.Spec.Replicas = ptr.To[int32](2)
	g.Expect(testEnv.Update(ctx, machinePool)).To(Succeed())

	// IBM's pool has 2 workers per zone; IBM's order picks sd-w0 and sd-w1.
	workers := make([]kubernetesservice.Worker, 0, 4)
	for i, id := range []string{"sd-w0", "sd-w1", "sd-w2", "sd-w3"} {
		zone := []string{"us-south-1", "us-south-2"}[i%2]
		workers = append(workers, kubernetesservice.Worker{ID: id, PoolID: mpTestPoolID, Zone: zone, ActualState: "deployed",
			HealthState: "normal", KubeVersionActual: "1.36.5_1540", KubeVersionTarget: "1.36.5_1540"})
		node := "10.0.9." + string(rune('1'+i))
		envtestScaleDownNode(t, node, id)
		envtestRunningPod(t, namespace, "web-"+id, node)
	}
	sizePerZone := int64(2)
	ibm := ksmock.NewMockKubernetesService(gomock.NewController(t))
	ibm.EXPECT().ListWorkerPools(gomock.Any(), mpTestClusterID).DoAndReturn(func(context.Context, string) ([]kubernetesservice.WorkerPool, error) {
		return sdTestPool(sizePerZone), nil
	}).AnyTimes()
	ibm.EXPECT().ListWorkers(gomock.Any(), mpTestClusterID).DoAndReturn(func(context.Context, string) ([]kubernetesservice.Worker, error) {
		return workers, nil
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
	ibm.EXPECT().ResizeWorkerPool(gomock.Any(), mpTestClusterID, mpTestPoolID, int64(1)).DoAndReturn(func(context.Context, string, string, int64) error {
		g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM), "persisted before the call")
		g.Expect(podsOn("10.0.9.1")).To(BeEmpty(), "the victims' pods are gone before the IBM call")
		g.Expect(podsOn("10.0.9.2")).To(BeEmpty())
		sizePerZone = 1
		return nil
	}).Times(1)

	clock := clocktesting.NewFakeClock(time.Now())
	workload := uncachedClient(t)
	r := &IBMCloudManagedMachinePoolReconciler{Client: uncachedClient(t), APIReader: reader, Recorder: newMachinePoolFixture(t).recorder,
		Resolver: &recordingResolver{}, ClientBuilder: &stubClientBuilder{kubernetes: ibm},
		ClusterCache: clustercache.NewFakeClusterCache(workload, client.ObjectKeyFromObject(cluster)), Clock: clock}
	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())
	}
	// The first two reconciles add the finalizer and record the pause state; the third observes the pool and, as every
	// first observation of a pool does, sets lastDisruptionTime, which starts the settle delay.
	for range 3 {
		reconcile()
	}
	g.Expect(stored().Status.Disruption.Operation).To(BeEmpty())
	clock.Step(managedscope.DisruptionSettleDelay + time.Second)

	// Draining: both victims cordoned, both pods evicted through the API server and Terminating.
	reconcile()
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
	g.Expect(stored().Status.Disruption.WorkerIDs).To(Equal([]string{"sd-w0", "sd-w1"}))
	lease := &coordinationv1.Lease{}
	g.Expect(reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "dev-ibm-worker-disruption"}, lease)).To(Succeed())
	for _, node := range []string{"10.0.9.1", "10.0.9.2"} {
		cordoned := &corev1.Node{}
		g.Expect(reader.Get(ctx, client.ObjectKey{Name: node}, cordoned)).To(Succeed())
		g.Expect(cordoned.Spec.Unschedulable).To(BeTrue(), node)
		pods := podsOn(node)
		g.Expect(pods).To(HaveLen(1))
		g.Expect(pods[0].DeletionTimestamp).NotTo(BeNil(), "the eviction was accepted")
	}

	// An accepted eviction is not enough: the drain waits while the pods terminate.
	clock.Step(20 * time.Second)
	reconcile()
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))

	// The kubelet's final delete (played by the test) finishes the drain; then the detach wait, then the resize.
	for _, node := range []string{"10.0.9.1", "10.0.9.2"} {
		for _, pod := range podsOn(node) {
			g.Expect(testEnv.Delete(ctx, &pod, client.GracePeriodSeconds(0))).To(Succeed())
		}
	}
	g.Eventually(func() int { return len(podsOn("10.0.9.1")) + len(podsOn("10.0.9.2")) }, 10*time.Second, 100*time.Millisecond).Should(BeZero())
	clock.Step(20 * time.Second)
	reconcile()
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach))
	clock.Step(20 * time.Second)
	reconcile()
	g.Expect(stored().Status.Disruption.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval))

	// IBM lists the victims deleted: the token goes and status.disruption is cleared.
	workers[0].ActualState, workers[1].ActualState = "deleted", "deleted"
	clock.Step(30 * time.Second)
	reconcile()
	g.Expect(stored().Status.Disruption).To(Equal(infrav1.WorkerDisruptionStatus{}))
	g.Expect(apierrors.IsNotFound(reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "dev-ibm-worker-disruption"},
		&coordinationv1.Lease{}))).To(BeTrue())
	g.Expect(podsOn("10.0.9.3")).To(HaveLen(1), "the other workers' pods were never touched")
}
