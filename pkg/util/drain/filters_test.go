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
	"errors"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// TestPodBehavior covers every pod filter in Cluster API's order: DaemonSet pods with and without their DaemonSet,
// mirror pods, emptyDir and controller-less pods (evicted, CAPI parity), the drain label values, terminating pods on
// reachable and unreachable Nodes, and finished pods.
func TestPodBehavior(t *testing.T) {
	daemonSet := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "agent"}}

	testCases := []struct {
		name        string
		pod         *corev1.Pod
		objects     []client.Object
		unreachable bool
		getErr      error
		want        podBehavior
		wantErr     string
	}{
		{
			name: "ReplicaSet pod is evicted",
			pod:  newPod("apps", "web-0", testNode),
			want: behaviorEvict,
		},
		{
			name:    "DaemonSet pod whose DaemonSet exists is skipped",
			pod:     newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent")),
			objects: []client.Object{daemonSet},
			want:    behaviorSkip,
		},
		{
			name: "orphaned DaemonSet pod is evicted",
			pod:  newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent")),
			want: behaviorEvict,
		},
		{
			name:    "finished DaemonSet pod is evicted without reading the DaemonSet",
			pod:     newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent"), withPhase(corev1.PodSucceeded)),
			objects: []client.Object{daemonSet},
			getErr:  errors.New("must not be called"),
			want:    behaviorEvict,
		},
		{
			name:    "DaemonSet read error is returned",
			pod:     newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent")),
			getErr:  errors.New("connection refused"),
			wantErr: "get DaemonSet kube-system/agent of Pod kube-system/agent-x: connection refused",
		},
		{
			name: "mirror pod is skipped",
			pod:  newPod("kube-system", "static-x", testNode, withMirror),
			want: behaviorSkip,
		},
		{
			name: "emptyDir pod is evicted",
			pod:  newPod("apps", "cache-0", testNode, withEmptyDir),
			want: behaviorEvict,
		},
		{
			name: "pod without a controller is evicted",
			pod:  newPod("apps", "bare", testNode, withoutController),
			want: behaviorEvict,
		},
		{
			name: "drain label skip is skipped",
			pod:  newPod("apps", "web-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "skip")),
			want: behaviorSkip,
		},
		{
			name: "drain label Skip is case-insensitive",
			pod:  newPod("apps", "web-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "Skip")),
			want: behaviorSkip,
		},
		{
			name: "drain label wait-completed is waited for",
			pod:  newPod("batch", "job-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "wait-completed")),
			want: behaviorWaitCompleted,
		},
		{
			name: "drain label WaitCompleted without dash is waited for",
			pod:  newPod("batch", "job-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "WaitCompleted")),
			want: behaviorWaitCompleted,
		},
		{
			name: "invalid drain label value is evicted",
			pod:  newPod("apps", "web-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "never")),
			want: behaviorEvict,
		},
		{
			name:    "DaemonSet filter wins over the wait-completed label",
			pod:     newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent"), withPodLabel(clusterv1.PodDrainLabel, "wait-completed")),
			objects: []client.Object{daemonSet},
			want:    behaviorSkip,
		},
		{
			name: "terminating pod on a reachable Node stays to be waited for",
			pod:  newPod("apps", "web-0", testNode, withDeletion(testNow.Add(-time.Hour))),
			want: behaviorEvict,
		},
		{
			name:        "terminating pod on an unreachable Node for more than 1 s is skipped",
			pod:         newPod("apps", "web-0", testNode, withDeletion(testNow.Add(-2*time.Second))),
			unreachable: true,
			want:        behaviorSkip,
		},
		{
			name:        "terminating pod on an unreachable Node for 1 s is still waited for",
			pod:         newPod("apps", "web-0", testNode, withDeletion(testNow.Add(-1500*time.Millisecond))),
			unreachable: true,
			want:        behaviorEvict,
		},
		{
			name:        "running pod on an unreachable Node is evicted",
			pod:         newPod("apps", "web-0", testNode),
			unreachable: true,
			want:        behaviorEvict,
		},
		{
			name: "Succeeded pod is evicted",
			pod:  newPod("batch", "done", testNode, withoutController, withPhase(corev1.PodSucceeded)),
			want: behaviorEvict,
		},
		{
			name: "Failed pod is evicted",
			pod:  newPod("batch", "crashed", testNode, withoutController, withEmptyDir, withPhase(corev1.PodFailed)),
			want: behaviorEvict,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			funcs := interceptor.Funcs{}
			if tc.getErr != nil {
				funcs.Get = func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
					return tc.getErr
				}
			}
			h := newTestHelper(newFakeClient(funcs, tc.objects...), clocktesting.NewFakePassiveClock(testNow))

			got, err := h.podBehavior(context.Background(), tc.pod, tc.unreachable)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

// TestEvictionWarning checks Cluster API's informational warnings for pods that are evicted anyway.
func TestEvictionWarning(t *testing.T) {
	testCases := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{name: "managed pod without local storage", pod: newPod("apps", "web-0", testNode), want: ""},
		{name: "emptyDir pod", pod: newPod("apps", "cache-0", testNode, withEmptyDir), want: "evicting Pod with local storage"},
		{name: "pod without controller", pod: newPod("apps", "bare", testNode, withoutController), want: "evicting Pod that has no controller"},
		{
			name: "both",
			pod:  newPod("apps", "bare", testNode, withoutController, withEmptyDir),
			want: "evicting Pod with local storage, evicting Pod that has no controller",
		},
		{name: "finished pod has no warning", pod: newPod("apps", "bare", testNode, withoutController, withPhase(corev1.PodFailed)), want: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(evictionWarning(tc.pod)).To(Equal(tc.want))
		})
	}
}
