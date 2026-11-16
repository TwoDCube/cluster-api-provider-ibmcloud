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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Shared builders for the unit tests. Names and addresses are synthetic: Node names follow the IKS convention of the
// worker's private IP (observed on IBM Cloud) with documentation-only values.

const (
	testPrefix = "ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/"
	testOwner  = "default/pool-a"
	testNode   = "10.0.0.11"
)

var testNow = time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

// newFakeClient returns a controller-runtime fake client with the spec.nodeName pod index the drain's field
// selector needs, the given interceptors and objects.
func newFakeClient(funcs interceptor.Funcs, objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(funcs).
		Build()
}

// newTestHelper returns a valid Helper over c and clk with a 10 s eviction budget.
func newTestHelper(c client.Client, clk clock.PassiveClock) *Helper {
	return &Helper{Client: c, Clock: clk, EvictionBudget: 10 * time.Second, AnnotationPrefix: testPrefix}
}

// getNode reads the Node back, so the caller holds the stored resourceVersion like a real reconciler.
func getNode(t *testing.T, c client.Client, name string) *corev1.Node {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, node); err != nil {
		t.Fatalf("get Node %s: %v", name, err)
	}
	return node
}

// newNode returns a Ready, schedulable Node.
func newNode(name string, mutate ...func(*corev1.Node)) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, m := range mutate {
		m(node)
	}
	return node
}

// cordonedNode marks the Node unschedulable (cordoned before the drain step, by CAPIBM or a human).
func cordonedNode(node *corev1.Node) {
	node.Spec.Unschedulable = true
}

// unreachableNode sets the Node's Ready condition to Unknown, as the node lifecycle controller does when the kubelet
// stops reporting.
func unreachableNode(node *corev1.Node) {
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}
}

// withAnnotation sets one annotation on the Node.
func withAnnotation(key, value string) func(*corev1.Node) {
	return func(node *corev1.Node) {
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		node.Annotations[key] = value
	}
}

// withNodeLabel sets one label on the Node.
func withNodeLabel(key, value string) func(*corev1.Node) {
	return func(node *corev1.Node) {
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels[key] = value
	}
}

// newPod returns a running pod bound to nodeName and owned by a ReplicaSet.
func newPod(namespace, name, nodeName string, mutate ...func(*corev1.Pod)) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "app", Image: "registry.example/app:1"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	withController("ReplicaSet", name+"-rs")(pod)
	for _, m := range mutate {
		m(pod)
	}
	return pod
}

// withController replaces the pod's controller owner reference.
func withController(kind, name string) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       kind,
			Name:       name,
			UID:        types.UID(name + "-uid"),
			Controller: ptr.To(true),
		}}
	}
}

// withoutController removes every owner reference.
func withoutController(pod *corev1.Pod) {
	pod.OwnerReferences = nil
}

// withPodLabel sets one label on the pod.
func withPodLabel(key, value string) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[key] = value
	}
}

// withMirror marks the pod as a static (mirror) pod.
func withMirror(pod *corev1.Pod) {
	pod.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "hash"}
	pod.OwnerReferences = nil
}

// withPhase sets the pod phase.
func withPhase(phase corev1.PodPhase) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		pod.Status.Phase = phase
	}
}

// withEmptyDir adds an emptyDir volume.
func withEmptyDir(pod *corev1.Pod) {
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         "scratch",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
}

// withClaim adds a PersistentVolumeClaim volume.
func withClaim(claim string) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name:         claim,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}},
		})
	}
}

// withDeletion marks the pod as deleting since at. The fake client only stores a deletionTimestamp on an object with
// a finalizer, so one is added.
func withDeletion(at time.Time) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		pod.DeletionTimestamp = &metav1.Time{Time: at}
		pod.Finalizers = []string{"example.com/hold"}
	}
}

// refNames returns "namespace/name" of each ref, for compact assertions.
func refNames(refs []PodRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.String())
	}
	return names
}
