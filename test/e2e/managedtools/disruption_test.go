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

package managedtools

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	. "github.com/onsi/gomega"
)

// probePod returns a probe pod; reason, when not "", adds a True DisruptionTarget condition with that reason.
func probePod(name string, terminating bool, reason string) corev1.Pod {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)}}
	if terminating {
		pod.DeletionTimestamp = &metav1.Time{Time: janitorNow}
	}
	if reason != "" {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: reason}}
	}
	return pod
}

// budget returns a PodDisruptionBudget with the given health counts, evaluated for its generation.
func budget(name string, current, desired int32) policyv1.PodDisruptionBudget {
	return policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 2},
		Status:     policyv1.PodDisruptionBudgetStatus{ObservedGeneration: 2, CurrentHealthy: current, DesiredHealthy: desired},
	}
}

func TestDisruptionRecorder(t *testing.T) {
	testCases := []struct {
		name         string
		pods         [][]corev1.Pod
		budgets      []policyv1.PodDisruptionBudget
		wantEvicted  int
		wantProblems []string
	}{
		{
			name: "evicted pods are counted, replacements are new pods",
			pods: [][]corev1.Pod{
				{probePod("a", false, ""), probePod("b", false, "")},
				{probePod("a", true, EvictionReason), probePod("b", false, "")},
				{probePod("b", false, ""), probePod("c", false, "")},
			},
			budgets:     []policyv1.PodDisruptionBudget{budget("spread", 5, 5)},
			wantEvicted: 1,
		},
		{
			name: "the reason may appear only on a later listing",
			pods: [][]corev1.Pod{
				{probePod("a", false, "")},
				{probePod("a", true, "")},
				{probePod("a", true, EvictionReason)},
				{},
			},
			wantEvicted: 1,
		},
		{
			name: "a pod deleted without the Eviction API",
			pods: [][]corev1.Pod{
				{probePod("a", false, "")},
				{probePod("a", true, "DeletionByTaintManager")},
				{},
			},
			wantProblems: []string{`pod a terminated with DisruptionTarget reason "DeletionByTaintManager", not EvictionByEvictionAPI`},
		},
		{
			name: "a pod terminating without any DisruptionTarget condition",
			pods: [][]corev1.Pod{
				{probePod("a", true, "")},
			},
			wantProblems: []string{`pod a terminated with DisruptionTarget reason "", not EvictionByEvictionAPI`},
		},
		{
			name: "a pod that vanished unseen",
			pods: [][]corev1.Pod{
				{probePod("a", false, ""), probePod("b", false, "")},
				{probePod("b", false, "")},
			},
			wantProblems: []string{"pod a disappeared without being seen terminating"},
		},
		{
			name:         "a violated budget",
			budgets:      []policyv1.PodDisruptionBudget{budget("spread", 4, 5)},
			wantProblems: []string{"PodDisruptionBudget spread had 4 healthy pods at 2026-11-16T12:00:00Z, fewer than the 5 it requires"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			recorder := NewDisruptionRecorder()
			for _, listing := range tc.pods {
				recorder.ObservePods(listing)
			}
			recorder.ObserveBudgets(tc.budgets, janitorNow)

			result := recorder.Result()

			g.Expect(result.Polls).To(Equal(len(tc.pods)))
			g.Expect(result.Evicted).To(Equal(tc.wantEvicted))
			g.Expect(result.Problems).To(HaveLen(len(tc.wantProblems)))
			for i, want := range tc.wantProblems {
				g.Expect(result.Problems[i]).To(HavePrefix(want))
			}
		})
	}
}

func TestDisruptionRecorderIgnoresStaleBudgets(t *testing.T) {
	g := NewWithT(t)
	recorder := NewDisruptionRecorder()
	stale := budget("blocker", 2, 3)
	stale.Generation = 3 // just relaxed; the controller has not recomputed DesiredHealthy yet

	recorder.ObserveBudgets([]policyv1.PodDisruptionBudget{stale}, janitorNow.Add(time.Second))

	g.Expect(recorder.Result().Problems).To(BeEmpty())
}

func TestDisruptionRecorderIgnoresFalseDisruptionTarget(t *testing.T) {
	g := NewWithT(t)
	pod := probePod("a", true, "")
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionFalse, Reason: EvictionReason}}
	recorder := NewDisruptionRecorder()

	recorder.ObservePods([]corev1.Pod{pod})

	g.Expect(recorder.Result().Problems).To(ConsistOf(HavePrefix(`pod a terminated with DisruptionTarget reason ""`)))
}
