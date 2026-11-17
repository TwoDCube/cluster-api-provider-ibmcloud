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
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/index"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"

	. "github.com/onsi/gomega"
)

// The drain step tests run the step on a fake workload cluster whose interceptors record every Node patch, eviction
// and pod listing, in order. The fake's eviction has no PodDisruptionBudget check, so a refusal is injected per pod.

// workloadCalls records the workload-cluster calls of a drain, in order: "cordon <node>", "evict <namespace>/<pod>",
// "list pods <node>", "list <kind>".
type workloadCalls struct {
	mu    sync.Mutex
	calls []string
}

// add records one call.
func (w *workloadCalls) add(call string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, call)
}

// all returns the calls so far and forgets them.
func (w *workloadCalls) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	calls := w.calls
	w.calls = nil
	return calls
}

// count returns how many recorded calls start with prefix.
func (w *workloadCalls) count(prefix string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, c := range w.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

// newWorkloadClient returns a fake workload cluster holding objs that records its calls in calls; an eviction of a pod
// named in refusals answers that error, every other eviction deletes the pod. Nodes are indexed by provider ID, as
// ClusterCache indexes them (NodeProviderIDIndex), and a lookup through that index is recorded as "find node <id>".
func newWorkloadClient(t *testing.T, calls *workloadCalls, refusals map[string]error, objs ...client.Object) client.Client {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(poolScheme(t)).WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithIndex(&corev1.Node{}, index.NodeProviderIDField, index.NodeByProviderID).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, isNode := obj.(*corev1.Node); isNode {
					calls.add("cordon " + obj.GetName())
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourceCreate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
				calls.add("evict " + obj.GetNamespace() + "/" + obj.GetName())
				if err, refused := refusals[obj.GetName()]; refused {
					return err
				}
				return c.SubResource(subResource).Create(ctx, obj, sub, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				switch list.(type) {
				case *corev1.PodList:
					listOpts := &client.ListOptions{}
					listOpts.ApplyOptions(opts)
					node := ""
					if listOpts.FieldSelector != nil {
						node, _ = listOpts.FieldSelector.RequiresExactMatch("spec.nodeName")
					}
					calls.add("list pods " + node)
				case *corev1.NodeList:
					calls.add(nodeListCall(opts))
				case *storagev1.VolumeAttachmentList:
					calls.add("list volumeattachments")
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()
	return c
}

// victimNode returns a Ready, schedulable Node of a test worker.
func victimNode(name, workerID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{ProviderID: providerIDOf(workerID)},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

// workloadPod returns a running pod of a ReplicaSet bound to node.
func workloadPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name, OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: name + "-rs", UID: "rs-uid", Controller: ptr.To(true),
		}}},
		Spec:   corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "app", Image: "registry.example/app:1"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// attachedVolume returns a VolumeAttachment of the persistent volume pv-data that is attached to node.
func attachedVolume(node string) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-" + node},
		Spec: storagev1.VolumeAttachmentSpec{Attacher: "vpc.block.csi.ibm.io", NodeName: node,
			Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To("pv-data")}},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
}

// pdbRefusal is the API server's answer to an eviction a PodDisruptionBudget refuses.
func pdbRefusal() error {
	err := apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes,
		metav1.StatusCause{Type: policyv1.DisruptionBudgetCause, Message: "The disruption budget web needs 2 healthy pods and has 2 currently"})
	return err
}

// readNodes returns the Nodes of the workload cluster, as the pool reads them.
func readNodes(t *testing.T, c client.Client) []corev1.Node {
	t.Helper()
	nodes := &corev1.NodeList{}
	NewWithT(t).Expect(c.List(context.Background(), nodes)).To(Succeed())
	return nodes.Items
}

// drainStepFixture is one drain step over a fake workload cluster.
type drainStepFixture struct {
	calls    *workloadCalls
	workload client.Client
	recorder *record.FakeRecorder
	clock    *clocktesting.FakeClock
	pool     *infrav1.IBMCloudManagedMachinePool
}

// newDrainStepFixture returns a fake workload cluster with objs and the pool that drains it.
func newDrainStepFixture(t *testing.T, refusals map[string]error, objs ...client.Object) *drainStepFixture {
	t.Helper()
	f := &drainStepFixture{calls: &workloadCalls{}, recorder: record.NewFakeRecorder(100),
		clock: clocktesting.NewFakeClock(poolTestNow), pool: poolObject("workers", "workers")}
	f.workload = newWorkloadClient(t, f.calls, refusals, objs...)
	return f
}

// step returns the drain step with knobs.
func (f *drainStepFixture) step(t *testing.T, knobs DrainKnobs) *DrainStep {
	t.Helper()
	step, err := NewDrainStep(DrainStepInput{WorkloadClient: f.workload, Clock: f.clock, Recorder: f.recorder, Pool: f.pool, Knobs: knobs})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return step
}

// events drains the fake recorder.
func (f *drainStepFixture) events() []string {
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

func TestDrainKnobsFor(t *testing.T) {
	g := NewWithT(t)
	mp := &clusterv1.MachinePool{}
	pool := poolObject("workers", "workers")
	g.Expect(DrainKnobsFor(mp, pool)).To(Equal(DrainKnobs{}))

	mp.Spec.Template.Spec.Deletion.NodeDrainTimeoutSeconds = ptr.To[int32](300)
	mp.Spec.Template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds = ptr.To[int32](120)
	mp.Spec.Template.Annotations = map[string]string{clusterv1.ExcludeNodeDrainingAnnotation: ""}
	pool.Annotations = map[string]string{clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation: "false"}
	g.Expect(DrainKnobsFor(mp, pool)).To(Equal(DrainKnobs{
		NodeDrainTimeoutSeconds:        ptr.To[int32](300),
		NodeVolumeDetachTimeoutSeconds: ptr.To[int32](120),
		ExcludeDraining:                true,
		ExcludeVolumeDetachWait:        true,
	}), "an annotation counts by its presence, on the template or on the pool")
}

func TestNewDrainStep(t *testing.T) {
	g := NewWithT(t)
	_, err := NewDrainStep(DrainStepInput{})
	g.Expect(err).To(MatchError(ContainSubstring("WorkloadClient is nil")))
	g.Expect(err).To(MatchError(ContainSubstring("Clock is nil")))
	g.Expect(err).To(MatchError(ContainSubstring("Recorder is nil")))
	g.Expect(err).To(MatchError(ContainSubstring("Pool must be the IBMCloudManagedMachinePool")))
}

func TestDrainStepEvict(t *testing.T) {
	t.Run("every victim is cordoned before the first eviction", func(t *testing.T) {
		g := NewWithT(t)
		f := newDrainStepFixture(t, nil, victimNode("10.0.0.1", "w0"), victimNode("10.0.0.2", "w1"),
			workloadPod("web-0", "10.0.0.1"), workloadPod("web-1", "10.0.0.2"))
		result, err := f.step(t, DrainKnobs{}).Evict(context.Background(), readNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.calls.all()).To(Equal([]string{"list nodes", "cordon 10.0.0.1", "cordon 10.0.0.2",
			"list pods 10.0.0.1", "evict apps/web-0", "list pods 10.0.0.2", "evict apps/web-1"}))
		g.Expect(result.Progress.Done).To(BeFalse(), "an accepted eviction is not enough: the pod must be gone")
		g.Expect(result.Message).To(ContainSubstring("Node 10.0.0.1: Drain not completed yet"))
		g.Expect(result.StartTime).To(Equal(poolTestNow))

		nodes := readNodes(t, f.workload)
		for _, node := range nodes {
			g.Expect(node.Spec.Unschedulable).To(BeTrue())
			g.Expect(node.Annotations).To(HaveKeyWithValue(infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner, DrainOwner(f.pool)))
		}
		result, err = f.step(t, DrainKnobs{}).Evict(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true}))
		g.Expect(result.Message).To(BeEmpty())
	})
	t.Run("an excluded drain touches nothing", func(t *testing.T) {
		g := NewWithT(t)
		f := newDrainStepFixture(t, nil, victimNode("10.0.0.1", "w0"), workloadPod("web-0", "10.0.0.1"))
		nodes := readNodes(t, f.workload)
		f.calls.all()
		result, err := f.step(t, DrainKnobs{ExcludeDraining: true}).Evict(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Excluded: true}))
		g.Expect(f.calls.all()).To(BeEmpty())
	})
	t.Run("a victim without Nodes is drained", func(t *testing.T) {
		g := NewWithT(t)
		f := newDrainStepFixture(t, nil)
		result, err := f.step(t, DrainKnobs{}).Evict(context.Background(), nil)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true}))
	})
	t.Run("a PodDisruptionBudget refusal is blocked and reported on entry only", func(t *testing.T) {
		g := NewWithT(t)
		f := newDrainStepFixture(t, map[string]error{"web-0": pdbRefusal()}, victimNode("10.0.0.1", "w0"), workloadPod("web-0", "10.0.0.1"))
		result, err := f.step(t, DrainKnobs{}).Evict(context.Background(), readNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Blocked: true}))
		g.Expect(result.Message).To(ContainSubstring("disruption budget"))
		g.Expect(f.events()).To(ConsistOf(HavePrefix("Warning DrainBlocked a PodDisruptionBudget refuses an eviction")))

		conditions.Set(f.pool, metav1.Condition{Type: infrav1.IBMCloudManagedMachinePoolDrainingCondition, Status: metav1.ConditionTrue,
			Reason: string(ReasonDrainBlocked)})
		f.clock.Step(DrainRetryInterval)
		_, err = f.step(t, DrainKnobs{}).Evict(context.Background(), readNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.events()).To(BeEmpty(), "no repeat before DrainBlockedEventInterval")
	})
	t.Run("after the time limit no eviction but one listing and a warning", func(t *testing.T) {
		g := NewWithT(t)
		f := newDrainStepFixture(t, map[string]error{"web-0": pdbRefusal()}, victimNode("10.0.0.1", "w0"), workloadPod("web-0", "10.0.0.1"))
		knobs := DrainKnobs{NodeDrainTimeoutSeconds: ptr.To[int32](60)}
		_, err := f.step(t, knobs).Evict(context.Background(), readNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		f.events()

		f.clock.Step(61 * time.Second)
		nodes := readNodes(t, f.workload)
		f.calls.all()
		result, err := f.step(t, knobs).Evict(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.calls.all()).To(Equal([]string{"list pods 10.0.0.1"}), "no eviction, no cordon change, one RemainingPods listing")
		g.Expect(result.Progress).To(Equal(DrainProgress{TimedOut: true}), "the step moves on to volume detach")

		helper := &drain.Helper{Client: f.workload, Clock: f.clock, EvictionBudget: time.Second, AnnotationPrefix: infrav1.NodeAnnotationPrefix}
		remaining, err := helper.RemainingPods(context.Background(), &nodes[0])
		g.Expect(err).NotTo(HaveOccurred())
		events := f.events()
		g.Expect(events).To(HaveLen(1))
		g.Expect(events[0]).To(HavePrefix("Warning DrainTimeoutExceeded the drain time limit of 60s"))
		g.Expect(events[0]).To(HaveSuffix(remaining.ConditionMessage(poolTestNow, 3)))
		g.Expect(events[0]).To(ContainSubstring("not evicted"))
	})
	t.Run("a Node of another pool is never taken over", func(t *testing.T) {
		g := NewWithT(t)
		node := victimNode("10.0.0.1", "w0")
		node.Annotations = map[string]string{infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner: "capi-managed/other"}
		f := newDrainStepFixture(t, nil, node)
		_, err := f.step(t, DrainKnobs{}).Evict(context.Background(), readNodes(t, f.workload))
		g.Expect(err).To(MatchError(drain.ErrDrainOwnerConflict))
	})
}

func TestDrainStepCheck(t *testing.T) {
	drained := func(t *testing.T, objs ...client.Object) (*drainStepFixture, []corev1.Node) {
		t.Helper()
		f := newDrainStepFixture(t, nil, objs...)
		_, err := f.step(t, DrainKnobs{}).Evict(context.Background(), readNodes(t, f.workload))
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		nodes := readNodes(t, f.workload)
		f.calls.all()
		f.events()
		return f, nodes
	}
	t.Run("checks without evicting and sends a returned pod back to the drain", func(t *testing.T) {
		g := NewWithT(t)
		f, nodes := drained(t, victimNode("10.0.0.1", "w0"))
		g.Expect(f.workload.Create(context.Background(), workloadPod("late-0", "10.0.0.1"))).To(Succeed())
		result, err := f.step(t, DrainKnobs{}).Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{}))
		g.Expect(f.calls.count("evict")).To(BeZero())
	})
	t.Run("waits for attached volumes", func(t *testing.T) {
		g := NewWithT(t)
		f, nodes := drained(t, victimNode("10.0.0.1", "w0"), attachedVolume("10.0.0.1"))
		result, err := f.step(t, DrainKnobs{}).Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true}))
		g.Expect(result.Message).To(ContainSubstring("Node 10.0.0.1: "))
		g.Expect(result.Message).To(ContainSubstring("pv-data"))

		result, err = f.step(t, DrainKnobs{ExcludeVolumeDetachWait: true}).Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true, DetachExcluded: true}))

		f.clock.Step(2 * time.Minute)
		result, err = f.step(t, DrainKnobs{NodeVolumeDetachTimeoutSeconds: ptr.To[int32](60)}).Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true, DetachTimedOut: true}), "the limit counts from the drain start")
	})
	t.Run("detached volumes finish the step", func(t *testing.T) {
		g := NewWithT(t)
		f, nodes := drained(t, victimNode("10.0.0.1", "w0"))
		result, err := f.step(t, DrainKnobs{}).Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true, VolumesDetached: true}))
	})
	t.Run("an excluded drain still waits for detach", func(t *testing.T) {
		g := NewWithT(t)
		f := newDrainStepFixture(t, nil, victimNode("10.0.0.1", "w0"))
		result, err := f.step(t, DrainKnobs{ExcludeDraining: true}).Check(context.Background(), readNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Excluded: true, VolumesDetached: true}))
	})
	t.Run("an excluded drain's detach limit counts from the recorded start", func(t *testing.T) {
		g := NewWithT(t)
		// Nothing is cordoned, so no Node carries a drain-start-time marker; without the recorded start a detach limit
		// would never run out.
		f := newDrainStepFixture(t, nil, victimNode("10.0.0.1", "w0"), attachedVolume("10.0.0.1"))
		nodes := readNodes(t, f.workload)
		knobs := DrainKnobs{ExcludeDraining: true, NodeVolumeDetachTimeoutSeconds: ptr.To[int32](60)}
		check := func(recorded time.Time) DrainStepResult {
			step, err := NewDrainStep(DrainStepInput{WorkloadClient: f.workload, Clock: f.clock, Recorder: f.recorder, Pool: f.pool,
				Knobs: knobs, RecordedStartTime: recorded})
			g.Expect(err).NotTo(HaveOccurred())
			result, err := step.Check(context.Background(), nodes)
			g.Expect(err).NotTo(HaveOccurred())
			return result
		}
		result := check(poolTestNow.Add(-59 * time.Second))
		g.Expect(result.Progress).To(Equal(DrainProgress{Excluded: true}), "within the limit the volume is waited for")
		g.Expect(result.Message).To(ContainSubstring("pv-data"))
		g.Expect(check(poolTestNow.Add(-60 * time.Second)).Progress).To(Equal(DrainProgress{Excluded: true, DetachTimedOut: true}))
		g.Expect(check(time.Time{}).Progress).To(Equal(DrainProgress{Excluded: true}), "no recorded start, no limit")
		g.Expect(f.calls.count("cordon")).To(BeZero())
	})
	t.Run("a Node marker wins over the recorded start", func(t *testing.T) {
		g := NewWithT(t)
		f, nodes := drained(t, victimNode("10.0.0.1", "w0"), attachedVolume("10.0.0.1"))
		step, err := NewDrainStep(DrainStepInput{WorkloadClient: f.workload, Clock: f.clock, Recorder: f.recorder, Pool: f.pool,
			Knobs: DrainKnobs{NodeVolumeDetachTimeoutSeconds: ptr.To[int32](60)}, RecordedStartTime: poolTestNow.Add(-time.Hour)})
		g.Expect(err).NotTo(HaveOccurred())
		result, err := step.Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true}), "the drain started now, as the Node says")
	})
	t.Run("a timed-out Node counts as drained without a call", func(t *testing.T) {
		g := NewWithT(t)
		f, nodes := drained(t, victimNode("10.0.0.1", "w0"), workloadPod("web-0", "10.0.0.1"))
		f.clock.Step(2 * time.Minute)
		result, err := f.step(t, DrainKnobs{NodeDrainTimeoutSeconds: ptr.To[int32](60)}).Check(context.Background(), nodes)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress.TimedOut).To(BeTrue())
		g.Expect(f.calls.count("list pods")).To(BeZero(), "no RemainingPods once the step moved on")
		g.Expect(f.events()).To(BeEmpty(), "the Warning was emitted in the Draining phase")
	})
}

// TestDrainStepMessageIsBounded drains many victim Nodes at once, as the deletion of a large pool does: the Draining
// message names the first Nodes in name order, counts the others and stays far below the CRD's limit of 32768
// characters on a condition message. One block per Node passes that limit with the 130 Nodes here, whose evictions a
// PodDisruptionBudget refuses, and the API server would then reject every status patch of the pool.
func TestDrainStepMessageIsBounded(t *testing.T) {
	victims := func(n int, withPods bool) ([]client.Object, map[string]error) {
		objs := make([]client.Object, 0, 3*n)
		refusals := map[string]error{}
		for i := range n {
			name := fmt.Sprintf("node-%03d", i)
			objs = append(objs, victimNode(name, fmt.Sprintf("w%03d", i)))
			if !withPods {
				objs = append(objs, attachedVolume(name))
				continue
			}
			for _, pod := range []string{"web-a-" + name, "web-b-" + name} {
				objs = append(objs, workloadPod(pod, name))
				refusals[pod] = pdbRefusal()
			}
		}
		return objs, refusals
	}
	sortedNodes := func(t *testing.T, c client.Client) []corev1.Node {
		t.Helper()
		nodes := readNodes(t, c)
		slices.SortFunc(nodes, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
		return nodes
	}
	t.Run("130_Nodes_blocked_by_a_PodDisruptionBudget", func(t *testing.T) {
		g := NewWithT(t)
		objs, refusals := victims(130, true)
		f := newDrainStepFixture(t, refusals, objs...)
		result, err := f.step(t, DrainKnobs{}).Evict(context.Background(), sortedNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Blocked: true}))
		g.Expect(len(result.Message)).To(BeNumerically("<=", maxDrainMessageLength))
		for _, named := range []string{"node-000", "node-001", "node-002"} {
			g.Expect(result.Message).To(ContainSubstring("Node " + named + ": Drain not completed yet"))
		}
		g.Expect(result.Message).NotTo(ContainSubstring("node-003"))
		g.Expect(result.Message).To(HaveSuffix("\nand 127 more Nodes are not drained yet"))
		g.Expect(f.events()).To(ConsistOf(And(HavePrefix("Warning DrainBlocked"), HaveSuffix("and 127 more Nodes are not drained yet"))),
			"the Warning carries the same bounded text")
	})
	t.Run("Nodes_with_attached_volumes", func(t *testing.T) {
		g := NewWithT(t)
		objs, _ := victims(5, false)
		f := newDrainStepFixture(t, nil, objs...)
		result, err := f.step(t, DrainKnobs{}).Check(context.Background(), sortedNodes(t, f.workload))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Progress).To(Equal(DrainProgress{Done: true}))
		g.Expect(strings.Count(result.Message, "Waiting for Node volumes to be detached")).To(Equal(3))
		g.Expect(result.Message).To(HavePrefix("Node node-000: "))
		g.Expect(result.Message).NotTo(ContainSubstring("node-003"))
		g.Expect(result.Message).To(HaveSuffix("\nand 2 more Nodes still have attached volumes"))
	})
	t.Run("as_many_Nodes_as_named_are_not_counted", func(t *testing.T) {
		g := NewWithT(t)
		lines := []string{"Node a: x", "Node b: y", "Node c: z"}
		g.Expect(nodeLinesMessage(lines, "are not drained yet")).To(Equal("Node a: x\nNode b: y\nNode c: z"))
		g.Expect(nodeLinesMessage(nil, "are not drained yet")).To(BeEmpty())
	})
}

func TestTruncateMessage(t *testing.T) {
	const marker = "\n... (cut to stay within the condition message limit)"
	g := NewWithT(t)
	g.Expect(truncateMessage("short", 100)).To(Equal("short"))
	long := strings.Repeat("a", 200)
	got := truncateMessage(long, 100)
	g.Expect(got).To(HaveLen(100))
	g.Expect(got).To(Equal(strings.Repeat("a", 100-len(marker)) + marker))
	g.Expect(truncateMessage(long, 100)).To(Equal(got), "the same text is cut the same way")

	// A multi-byte character across the cut is left out whole, so the message stays valid UTF-8.
	multi := strings.Repeat("a", 100-len(marker)-1) + strings.Repeat("ü", 50)
	got = truncateMessage(multi, 100)
	g.Expect(utf8.ValidString(got)).To(BeTrue())
	g.Expect(len(got)).To(BeNumerically("<=", 100))
	g.Expect(got).To(Equal(strings.Repeat("a", 100-len(marker)-1) + marker))
}

func TestDrainBlockedEventDue(t *testing.T) {
	start := poolTestNow
	testCases := []struct {
		name     string
		previous string
		start    time.Time
		now      time.Time
		want     bool
	}{
		{name: "entry", previous: string(ReasonDrainingNode), start: start, now: start.Add(time.Minute), want: true},
		{name: "entry without a start", previous: "", now: start, want: true},
		{name: "no repeat without a start", previous: string(ReasonDrainBlocked), now: start, want: false},
		{name: "repeat within the interval", previous: string(ReasonDrainBlocked), start: start, now: start.Add(10 * time.Minute), want: false},
		{name: "first reconcile after the interval", previous: string(ReasonDrainBlocked), start: start, now: start.Add(30*time.Minute + 5*time.Second), want: true},
		{name: "second reconcile after the interval", previous: string(ReasonDrainBlocked), start: start, now: start.Add(30*time.Minute + 25*time.Second), want: false},
		{name: "after two intervals", previous: string(ReasonDrainBlocked), start: start, now: start.Add(60 * time.Minute), want: true},
		{name: "start in the future", previous: string(ReasonDrainBlocked), start: start, now: start.Add(-time.Hour), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(drainBlockedEventDue(tc.previous, tc.start, tc.now)).To(Equal(tc.want))
		})
	}
}

// TestRecordDrainPhaseChange locks the texts of the drain events (golden), with and without exclude-node-draining: an
// excluded drain never says "cordon".
func TestRecordDrainPhaseChange(t *testing.T) {
	testCases := []struct {
		name     string
		from, to infrav1.WorkerDisruptionPhase
		excluded bool
		want     []string
	}{
		{name: "start", from: "", to: infrav1.WorkerDisruptionPhaseDraining, want: []string{"Normal DrainStarted cordoning 2 Nodes of pool workers, then evicting their pods"}},
		{name: "completed", from: infrav1.WorkerDisruptionPhaseDraining, to: infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach,
			want: []string{"Normal DrainCompleted the 2 Nodes of pool workers are drained"}},
		{name: "start of an excluded drain", from: "", to: infrav1.WorkerDisruptionPhaseDraining, excluded: true,
			want: []string{"Normal DrainStarted the drain of the 2 Nodes of pool workers is excluded " +
				"(machine.cluster.x-k8s.io/exclude-node-draining): CAPIBM leaves them schedulable and evicts no pod"}},
		{name: "end of an excluded drain", from: infrav1.WorkerDisruptionPhaseDraining, to: infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach,
			excluded: true, want: []string{"Normal DrainCompleted the 2 Nodes of pool workers are not drained: " +
				"machine.cluster.x-k8s.io/exclude-node-draining is set, so CAPIBM evicts no more pods"}},
		{name: "back to draining", from: infrav1.WorkerDisruptionPhaseWaitingForIBM, to: infrav1.WorkerDisruptionPhaseDraining},
		{name: "no change", from: infrav1.WorkerDisruptionPhaseDraining, to: infrav1.WorkerDisruptionPhaseDraining},
		{name: "no change of an excluded drain", from: infrav1.WorkerDisruptionPhaseDraining, to: infrav1.WorkerDisruptionPhaseDraining, excluded: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			recorder := record.NewFakeRecorder(10)
			RecordDrainPhaseChange(recorder, poolObject("workers", "workers"), tc.from, tc.to, 2, tc.excluded)
			close(recorder.Events)
			var events []string
			for e := range recorder.Events {
				events = append(events, e)
			}
			g.Expect(events).To(Equal(tc.want))
			if tc.excluded {
				g.Expect(events).NotTo(ContainElement(ContainSubstring("cordon")), "the drain is excluded: nothing is cordoned")
			}
		})
	}
}

func TestVictimNodes(t *testing.T) {
	g := NewWithT(t)
	other := victimNode("10.0.0.9", "x")
	other.Spec.ProviderID = "ibm://" + poolTestAccount + "///another-cluster/w0"
	nodes := []corev1.Node{*victimNode("10.0.0.2", "w1"), *other, *victimNode("10.0.0.1", "w0"), {ObjectMeta: metav1.ObjectMeta{Name: "unregistered"}}}
	victims := VictimNodes(nodes, poolTestAccount, poolTestClusterID, []string{"w0", "w1", "w-gone"})
	g.Expect(victims).To(HaveLen(2))
	g.Expect(victims[0].Name).To(Equal("10.0.0.1"))
	g.Expect(victims[1].Name).To(Equal("10.0.0.2"))
	g.Expect(VictimNodes(nodes, poolTestAccount, poolTestClusterID, nil)).To(BeEmpty())
}

func TestEarliest(t *testing.T) {
	g := NewWithT(t)
	later := poolTestNow.Add(time.Minute)
	g.Expect(earliest(time.Time{}, later)).To(Equal(later))
	g.Expect(earliest(later, time.Time{})).To(Equal(later))
	g.Expect(earliest(poolTestNow, later)).To(Equal(poolTestNow))
	g.Expect(earliest(later, poolTestNow)).To(Equal(poolTestNow))
	g.Expect(ptrValue(nil)).To(BeZero())
}

// nodeListCall names a Node listing for workloadCalls: "find node <provider ID>" for a lookup through the provider ID
// index, "list nodes" otherwise.
func nodeListCall(opts []client.ListOption) string {
	listOpts := &client.ListOptions{}
	listOpts.ApplyOptions(opts)
	if listOpts.FieldSelector != nil {
		if providerID, found := listOpts.FieldSelector.RequiresExactMatch(index.NodeProviderIDField); found {
			return "find node " + providerID
		}
	}
	return "list nodes"
}
