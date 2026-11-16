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

package drain

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// envHelper returns a Helper on the envtest API server with the real clock.
func envHelper() *Helper {
	return newTestHelper(envClient, clock.RealClock{})
}

// envNamespace creates a namespace for one test. envtest runs no namespace controller, so it is never cleaned up;
// generated names keep the tests independent.
func envNamespace(t *testing.T, g *WithT) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "drain-"}}
	g.Expect(envClient.Create(context.Background(), ns)).To(Succeed())
	return ns.Name
}

// envNode creates a Ready Node and returns it as stored.
func envNode(t *testing.T, g *WithT, name string) *corev1.Node {
	t.Helper()
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	g.Expect(envClient.Create(ctx, node)).To(Succeed())
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	g.Expect(envClient.Status().Update(ctx, node)).To(Succeed())
	t.Cleanup(func() {
		_ = envClient.Delete(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	})
	return node
}

// envRunningPod creates a pod bound to nodeName with the label app=<app> and marks it Running and Ready, so the API
// server checks its PodDisruptionBudget on eviction (Pending pods skip the check, eviction.go canIgnorePDB).
func envRunningPod(t *testing.T, g *WithT, namespace, name, nodeName, app string) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{"app": app}},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "app", Image: "registry.example/app:1"}},
		},
	}
	g.Expect(envClient.Create(ctx, pod)).To(Succeed())
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	g.Expect(envClient.Status().Update(ctx, pod)).To(Succeed())
	return pod
}

// setPDBStatus writes the status the disruption controller would write.
func setPDBStatus(g *WithT, key client.ObjectKey, allowed, current, desired int32) {
	ctx := context.Background()
	pdb := &policyv1.PodDisruptionBudget{}
	g.Expect(envClient.Get(ctx, key, pdb)).To(Succeed())
	pdb.Status.ObservedGeneration = pdb.Generation
	pdb.Status.DisruptionsAllowed = allowed
	pdb.Status.CurrentHealthy = current
	pdb.Status.DesiredHealthy = desired
	pdb.Status.ExpectedPods = current
	g.Expect(envClient.Status().Update(ctx, pdb)).To(Succeed())
}

// TestDrainEnvtestPodDisruptionBudget runs the drain against the real Eviction subresource: a PDB with a stale status
// and then with no disruption allowed blocks the eviction, relaxing it lets the eviction through, the drain waits while
// the pod terminates, and only the kubelet's final delete (played by the test) makes it done.
func TestDrainEnvtestPodDisruptionBudget(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	ns := envNamespace(t, g)
	nodeName := "10.0.1.11"
	node := envNode(t, g, nodeName)
	h := envHelper()

	g.Expect(h.Cordon(ctx, node, CordonOptions{Owner: ns + "/pool-a", ExcludeFromLoadBalancers: true})).To(Succeed())
	start, found, err := h.DrainStartTime(node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(found).To(BeTrue())

	pod := envRunningPod(t, g, ns, "web-0", nodeName, "web")
	envRunningPod(t, g, ns, "elsewhere", "10.0.1.12", "web")
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "web"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: ptr.To(intstr.FromInt32(1)),
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	g.Expect(envClient.Create(ctx, pdb)).To(Succeed())

	// No disruption controller runs, so the PDB status is stale: the API server answers 429 "still being processed"
	// with Retry-After 10 s, which client-go waits out by itself. The eviction therefore never finishes within the
	// budget and the pod is reported as Failed with the budget message, to be retried; it is not evicted. A short
	// budget keeps the test fast.
	staleHelper := *h
	staleHelper.EvictionBudget = time.Second
	result, err := staleHelper.Drain(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(result.Failed).To(Equal([]PodRef{{Namespace: ns, Name: "web-0", Message: "eviction timeout of 1s reached, eviction will be retried"}}))
	g.Expect(result.Evicted).To(BeEmpty())
	g.Expect(result.Done).To(BeFalse())

	// A PDB that allows no disruption blocks with the budget's numbers in the message.
	setPDBStatus(g, client.ObjectKeyFromObject(pdb), 0, 1, 1)
	result, err = h.Drain(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(result.Blocked)).To(Equal([]string{ns + "/web-0"}))
	g.Expect(result.Blocked[0].Message).To(Equal("cannot evict pod as it would violate the pod's disruption budget. " +
		"The disruption budget web needs 1 healthy pods and has 1 currently"))
	g.Expect(result.ConditionMessage(start, 3)).To(ContainSubstring("* Pod " + ns + "/web-0: cannot evict pod as it would violate"))

	// Relaxing the PDB lets the eviction through; the pod keeps running for its grace period, so the drain is not done.
	setPDBStatus(g, client.ObjectKeyFromObject(pdb), 1, 2, 1)
	result, err = h.Drain(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(result.Evicted)).To(Equal([]string{ns + "/web-0"}))
	g.Expect(result.Done).To(BeFalse())

	stored := &corev1.Pod{}
	g.Expect(envClient.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
	g.Expect(stored.DeletionTimestamp).ToNot(BeNil(), "the eviction deletes gracefully")
	g.Expect(stored.DeletionGracePeriodSeconds).To(Equal(ptr.To[int64](corev1.DefaultTerminationGracePeriodSeconds)),
		"the pod's own grace period is used")

	// Still terminating on the next call: accepted is not done.
	result, err = h.Drain(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(result.Terminating)).To(Equal([]string{ns + "/web-0"}))
	g.Expect(result.Evicted).To(BeEmpty())
	g.Expect(result.Done).To(BeFalse())

	// The kubelet confirms the termination; the pod on the other Node is never touched.
	g.Expect(envClient.Delete(ctx, stored, client.GracePeriodSeconds(0))).To(Succeed())
	g.Eventually(func(g Gomega) {
		result, err := h.Drain(ctx, node)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(result.Done).To(BeTrue())
	}).Should(Succeed())
	other := &corev1.Pod{}
	g.Expect(envClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: "elsewhere"}, other)).To(Succeed())
	g.Expect(other.DeletionTimestamp).To(BeNil())
}

// envSetController makes a DaemonSet called dsName with dsUID the controller of pod, as the DaemonSet controller does
// for the pods it creates. blockOwnerDeletion is left unset: it needs no extra permission and the drain ignores it.
func envSetController(g *WithT, pod *corev1.Pod, dsName string, dsUID types.UID) {
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: appsv1.SchemeGroupVersion.String(),
		Kind:       "DaemonSet",
		Name:       dsName,
		UID:        dsUID,
		Controller: ptr.To(true),
	}}
	g.Expect(envClient.Update(context.Background(), pod)).To(Succeed())
}

// TestDrainEnvtestSkippedAndUnmanagedPods checks the filters against the API server, with the DaemonSet read through
// the uncached client as in production: a pod whose DaemonSet exists and a drain=skip pod stay, while an orphaned
// DaemonSet pod (its DaemonSet is gone) and a pod without a controller are evicted (no PDB, so the eviction deletes them
// gracefully). RemainingPods, called first, lists the same pods and evicts none of them.
func TestDrainEnvtestSkippedAndUnmanagedPods(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	ns := envNamespace(t, g)
	nodeName := "10.0.1.21"
	node := envNode(t, g, nodeName)
	h := envHelper()
	g.Expect(h.Cordon(ctx, node, CordonOptions{Owner: ns + "/pool-a"})).To(Succeed())

	daemonSet := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "agent"},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "agent"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "agent"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "registry.example/agent:1"}}},
			},
		},
	}
	g.Expect(envClient.Create(ctx, daemonSet)).To(Succeed())
	envSetController(g, envRunningPod(t, g, ns, "agent-live", nodeName, "agent"), daemonSet.Name, daemonSet.UID)
	// envtest runs no garbage collector, so a pod whose DaemonSet does not exist stays bound as an orphan.
	envSetController(g, envRunningPod(t, g, ns, "agent-orphan", nodeName, "agent"), "gone",
		types.UID("00000000-0000-0000-0000-00000000d5e7"))

	skipped := envRunningPod(t, g, ns, "pinned", nodeName, "pinned")
	skipped.Labels["cluster.x-k8s.io/drain"] = "skip"
	g.Expect(envClient.Update(ctx, skipped)).To(Succeed())
	envRunningPod(t, g, ns, "bare", nodeName, "bare")
	wantEvicted := []string{ns + "/agent-orphan", ns + "/bare"}

	// Once the drain time limit has passed, the caller lists what is left: nothing may be evicted by that.
	remaining, err := h.RemainingPods(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(remaining.NotEvicted)).To(Equal(wantEvicted))
	pods := &corev1.PodList{}
	g.Expect(envClient.List(ctx, pods, client.InNamespace(ns))).To(Succeed())
	g.Expect(pods.Items).To(HaveLen(4))
	for _, pod := range pods.Items {
		g.Expect(pod.DeletionTimestamp).To(BeNil(), "RemainingPods evicted %s", pod.Name)
	}

	result, err := h.Drain(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(result.Evicted)).To(Equal(wantEvicted))

	for _, name := range []string{"agent-live", "pinned"} {
		stored := &corev1.Pod{}
		g.Expect(envClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, stored)).To(Succeed())
		g.Expect(stored.DeletionTimestamp).To(BeNil(), "skipped pod %s is never evicted", name)
	}
	for _, name := range []string{"agent-orphan", "bare"} {
		stored := &corev1.Pod{}
		g.Expect(envClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, stored)).To(Succeed())
		g.Expect(stored.DeletionTimestamp).ToNot(BeNil(), "evicted pod %s is deleting gracefully", name)
	}
}

// TestVolumesDetachedEnvtest follows a volume from attached to detached: Node status and VolumeAttachment both hold
// the wait until each is cleared.
func TestVolumesDetachedEnvtest(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	ns := envNamespace(t, g)
	nodeName := "10.0.1.31"
	node := envNode(t, g, nodeName)
	h := envHelper()

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-" + ns},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: testCSIDriver, VolumeHandle: "vol-" + ns},
			},
			ClaimRef: &corev1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: ns, Name: "data"},
		},
	}
	g.Expect(envClient.Create(ctx, pv)).To(Succeed())
	t.Cleanup(func() { _ = envClient.Delete(context.Background(), pv) })

	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-" + ns},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: testCSIDriver,
			NodeName: nodeName,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To(pv.Name)},
		},
	}
	g.Expect(envClient.Create(ctx, attachment)).To(Succeed())
	t.Cleanup(func() { _ = envClient.Delete(context.Background(), attachment) })
	attachment.Status.Attached = true
	g.Expect(envClient.Status().Update(ctx, attachment)).To(Succeed())

	node.Status.VolumesAttached = []corev1.AttachedVolume{{Name: corev1.UniqueVolumeName("kubernetes.io/csi/" + testCSIDriver + "^vol-" + ns)}}
	g.Expect(envClient.Status().Update(ctx, node)).To(Succeed())

	detached, message, err := h.VolumesDetached(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(detached).To(BeFalse())
	g.Expect(message).To(Equal("Waiting for Node volumes to be detached:\n* PersistentVolumeClaims: " + ns + "/data"))

	// The attach-detach controller clears the attachment first; the Node status still lists the volume.
	attachment.Status.Attached = false
	g.Expect(envClient.Status().Update(ctx, attachment)).To(Succeed())
	detached, _, err = h.VolumesDetached(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(detached).To(BeFalse())

	// Then the kubelet drops it from the Node status.
	node.Status.VolumesAttached = nil
	g.Expect(envClient.Status().Update(ctx, node)).To(Succeed())
	detached, message, err = h.VolumesDetached(ctx, node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(detached).To(BeTrue())
	g.Expect(message).To(BeEmpty())
}

// TestCordonUncordonEnvtest checks a cordon and an abort's uncordon with real merge patches: the markers are written
// and removed, the human's cordon survives an abort, and a stale Node copy gets a conflict.
func TestCordonUncordonEnvtest(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	h := envHelper()

	t.Run("CAPIBM cordon reverted against the API server", func(t *testing.T) {
		g := NewWithT(t)
		node := envNode(t, g, "10.0.1.41")
		g.Expect(h.Cordon(ctx, node, CordonOptions{Owner: "default/pool-a", ExcludeFromLoadBalancers: true})).To(Succeed())

		stored := &corev1.Node{}
		g.Expect(envClient.Get(ctx, client.ObjectKey{Name: node.Name}, stored)).To(Succeed())
		g.Expect(stored.Spec.Unschedulable).To(BeTrue())
		g.Expect(stored.Labels).To(HaveKeyWithValue(corev1.LabelNodeExcludeBalancers, "true"))
		g.Expect(stored.Annotations).To(HaveKeyWithValue(keyCordoned, "true"))
		g.Expect(stored.Annotations).To(HaveKeyWithValue(keyOwner, "default/pool-a"))

		g.Expect(h.Uncordon(ctx, stored)).To(Succeed())
		g.Expect(envClient.Get(ctx, client.ObjectKey{Name: node.Name}, stored)).To(Succeed())
		g.Expect(stored.Spec.Unschedulable).To(BeFalse())
		g.Expect(stored.Labels).ToNot(HaveKey(corev1.LabelNodeExcludeBalancers))
		for _, key := range []string{keyStart, keyOwner, keyCordoned, keyLB} {
			g.Expect(stored.Annotations).ToNot(HaveKey(key))
		}
	})

	t.Run("human cordon kept against the API server", func(t *testing.T) {
		g := NewWithT(t)
		node := envNode(t, g, "10.0.1.42")
		node.Spec.Unschedulable = true
		g.Expect(envClient.Update(ctx, node)).To(Succeed())

		g.Expect(h.Cordon(ctx, node, CordonOptions{Owner: "default/pool-a", ExcludeFromLoadBalancers: true})).To(Succeed())
		g.Expect(h.Uncordon(ctx, node)).To(Succeed())

		stored := &corev1.Node{}
		g.Expect(envClient.Get(ctx, client.ObjectKey{Name: node.Name}, stored)).To(Succeed())
		g.Expect(stored.Spec.Unschedulable).To(BeTrue())
		g.Expect(stored.Labels).ToNot(HaveKey(corev1.LabelNodeExcludeBalancers))
	})

	t.Run("stale Node copy conflicts", func(t *testing.T) {
		g := NewWithT(t)
		node := envNode(t, g, "10.0.1.43")
		stale := node.DeepCopy()
		node.Spec.Unschedulable = true
		g.Expect(envClient.Update(ctx, node)).To(Succeed())

		err := h.Cordon(ctx, stale, CordonOptions{Owner: "default/pool-a"})
		g.Expect(apierrors.IsConflict(err)).To(BeTrue(), "got %v", err)
	})

	g.Expect(h.validate()).To(Succeed())
}
