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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	. "github.com/onsi/gomega"
)

// testPin returns the pinned probe pod pinned-<n> on Node 10.0.0.<n> of worker ibm://w<n>, with the UID uid-pinned-<n>.
func testPin(n string) PinnedPod {
	return PinnedPod{Name: "pinned-" + n, UID: types.UID("uid-pinned-" + n), Node: "10.0.0." + n, ProviderID: "ibm://w" + n}
}

// testPins returns testPin(n) for every n.
func testPins(ns ...string) []PinnedPod {
	result := make([]PinnedPod, 0, len(ns))
	for _, n := range ns {
		result = append(result, testPin(n))
	}
	return result
}

// workerNode returns a Node with a name and a provider ID.
func workerNode(name, providerID string) corev1.Node {
	return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{ProviderID: providerID}}
}

// fates returns a fate function that answers from a map of pin numbers and PodNotObserved for any other pod.
func fates(byPin map[string]PodFate) func(types.UID) PodFate {
	return func(uid types.UID) PodFate {
		for n, fate := range byPin {
			if testPin(n).UID == uid {
				return fate
			}
		}
		return PodNotObserved
	}
}

func TestEvaluatePins(t *testing.T) {
	// The scale-down pins all 6 workers of the initial pool and removes 1, 3 and 5; the other three stay.
	scaleDownNodes := []corev1.Node{
		workerNode("10.0.0.2", "ibm://w2"), workerNode("10.0.0.4", "ibm://w4"), workerNode("10.0.0.6", "ibm://w6"),
		// The added pool's worker; no pin is on it.
		workerNode("10.0.0.9", "ibm://w9"),
	}
	scaleDownFates := map[string]PodFate{"1": PodEvicted, "2": PodRunning, "3": PodEvicted, "4": PodRunning,
		"5": PodEvicted, "6": PodRunning}
	testCases := []struct {
		name         string
		pins         []PinnedPod
		fates        map[string]PodFate
		nodesAfter   []corev1.Node
		wantRemoved  int
		wantEvicted  int
		wantKept     int
		wantProblems []string
	}{
		{
			name: "scale-down: every removed worker drained, every kept worker untouched",
			pins: testPins("1", "2", "3", "4", "5", "6"), fates: scaleDownFates, nodesAfter: scaleDownNodes,
			wantRemoved: 3, wantEvicted: 3, wantKept: 3,
		},
		{
			name: "every pinned worker replaced, a new Node without a provider ID yet counts as no worker",
			pins: testPins("1", "2"), fates: map[string]PodFate{"1": PodEvicted, "2": PodEvicted},
			nodesAfter:  []corev1.Node{workerNode("10.0.0.7", "ibm://w7"), workerNode("10.0.0.8", "")},
			wantRemoved: 2, wantEvicted: 2,
		},
		{
			name: "a replacement under the Node name of the worker it replaced is still a removed worker",
			pins: testPins("1"), fates: map[string]PodFate{"1": PodEvicted},
			nodesAfter:  []corev1.Node{workerNode("10.0.0.1", "ibm://w7")},
			wantRemoved: 1, wantEvicted: 1,
		},
		{
			name: "a removed worker whose pod still runs was not drained",
			pins: testPins("1", "2"), fates: map[string]PodFate{"1": PodEvicted, "2": PodRunning},
			wantRemoved: 2, wantEvicted: 1,
			wantProblems: []string{"pinned probe pod pinned-2 on Node 10.0.0.2 (worker ibm://w2), which the step " +
				"removed, was running untouched, not evicted through the Eviction API: the worker went away without a " +
				"drain that evicted its pods"},
		},
		{
			name: "a removed worker whose pod was deleted without the Eviction API",
			pins: testPins("1"), fates: map[string]PodFate{"1": PodDeletedOtherwise}, wantRemoved: 1,
			wantProblems: []string{"pinned probe pod pinned-1 on Node 10.0.0.1 (worker ibm://w1), which the step " +
				"removed, was deleted without the Eviction API, not evicted through the Eviction API"},
		},
		{
			name: "a removed worker whose pod vanished unseen",
			pins: testPins("1"), fates: map[string]PodFate{"1": PodVanished}, wantRemoved: 1,
			wantProblems: []string{"pinned probe pod pinned-1 on Node 10.0.0.1 (worker ibm://w1), which the step " +
				"removed, was gone without being seen terminating"},
		},
		{
			name: "a removed worker whose pod the probe never listed",
			pins: testPins("1"), fates: map[string]PodFate{}, wantRemoved: 1,
			wantProblems: []string{"pinned probe pod pinned-1 on Node 10.0.0.1 (worker ibm://w1), which the step " +
				"removed, was never listed by the disruption probe"},
		},
		{
			name: "a wrong victim: the kept worker was drained and the removed one was not, sorted",
			pins: testPins("1", "2"), fates: map[string]PodFate{"1": PodRunning, "2": PodEvicted},
			nodesAfter: []corev1.Node{workerNode("10.0.0.2", "ibm://w2")}, wantRemoved: 1,
			wantProblems: []string{
				"pinned probe pod pinned-1 on Node 10.0.0.1 (worker ibm://w1), which the step removed, was running " +
					"untouched, not evicted through the Eviction API",
				"pinned probe pod pinned-2 on Node 10.0.0.2 (worker ibm://w2), which the step kept, was evicted " +
					"through the Eviction API, not running untouched: the drain reached a worker that stays",
			},
		},
		{
			name: "a kept worker whose pod vanished",
			pins: testPins("1", "2"), fates: map[string]PodFate{"1": PodEvicted, "2": PodVanished},
			nodesAfter: []corev1.Node{workerNode("10.0.0.2", "ibm://w2")}, wantRemoved: 1, wantEvicted: 1,
			wantProblems: []string{"pinned probe pod pinned-2 on Node 10.0.0.2 (worker ibm://w2), which the step " +
				"kept, was gone without being seen terminating, not running untouched"},
		},
		{
			name: "fewer pinned workers removed than the step removes",
			pins: testPins("1", "2", "3"), fates: map[string]PodFate{"1": PodEvicted, "2": PodRunning, "3": PodRunning},
			nodesAfter:  []corev1.Node{workerNode("10.0.0.2", "ibm://w2"), workerNode("10.0.0.3", "ibm://w3")},
			wantRemoved: 2, wantEvicted: 1, wantKept: 2,
			wantProblems: []string{"the step removed 1 of the 3 pinned workers, not 2"},
		},
		{
			name: "more pinned workers removed than the step removes",
			pins: testPins("1", "2"), fates: map[string]PodFate{"1": PodEvicted, "2": PodEvicted},
			wantRemoved: 1, wantEvicted: 2,
			wantProblems: []string{"the step removed 2 of the 2 pinned workers, not 1"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			got, err := EvaluatePins(tc.pins, fates(tc.fates), tc.nodesAfter, tc.wantRemoved)

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Evicted).To(Equal(tc.wantEvicted), "evicted")
			g.Expect(got.Kept).To(Equal(tc.wantKept), "kept")
			g.Expect(got.Problems).To(HaveLen(len(tc.wantProblems)), "problems: %v", got.Problems)
			for i, want := range tc.wantProblems {
				g.Expect(got.Problems[i]).To(HavePrefix(want))
			}
		})
	}
}

// TestEvaluatePinsWithRecorder runs the pinned check on what a DisruptionRecorder saw, as the IKS lane does: the
// recorder's Fate method is the fate function.
func TestEvaluatePinsWithRecorder(t *testing.T) {
	g := NewWithT(t)
	recorder := NewDisruptionRecorder()
	running := func(name string) corev1.Pod { return probePod(name, false, "") }
	recorder.ObservePods([]corev1.Pod{running("pinned-1"), running("pinned-2"), running("spread-a")})
	recorder.ObservePods([]corev1.Pod{probePod("pinned-1", true, EvictionReason), running("pinned-2"),
		probePod("spread-a", true, EvictionReason)})
	recorder.ObservePods([]corev1.Pod{running("pinned-2"), running("spread-b")})

	// probePod and testPin both give the pod pinned-<n> the UID uid-pinned-<n>, so the recorder knows the pins.
	got, err := EvaluatePins(testPins("1", "2"), recorder.Fate, []corev1.Node{workerNode("10.0.0.2", "ibm://w2")}, 1)

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(got).To(Equal(PinResult{Evicted: 1, Kept: 1}))
}

func TestEvaluatePinsRejectsInvalidArguments(t *testing.T) {
	withPin := func(n string, change func(*PinnedPod)) []PinnedPod {
		changed := testPin(n)
		change(&changed)
		return []PinnedPod{testPin("1"), changed}
	}
	running := fates(map[string]PodFate{})
	testCases := []struct {
		name        string
		pins        []PinnedPod
		fate        func(types.UID) PodFate
		wantRemoved int
		wantErr     string
	}{
		{name: "no pins", fate: running, wantRemoved: 1,
			wantErr: "no pinned probe pod: a drain step must pin one to every worker it may remove"},
		{name: "no fate function", pins: testPins("1"), wantRemoved: 1, wantErr: "no fate function"},
		{name: "a step that removes no pinned worker", pins: testPins("1"), fate: running, wantRemoved: 0,
			wantErr: "wantRemoved is 0; it must be between 1 and the 1 pinned workers"},
		{name: "more removed workers than pins", pins: testPins("1", "2"), fate: running, wantRemoved: 3,
			wantErr: "wantRemoved is 3; it must be between 1 and the 2 pinned workers"},
		{name: "a pin without a name", pins: withPin("2", func(p *PinnedPod) { p.Name = "" }), fate: running,
			wantRemoved: 1, wantErr: "pinned probe pod 1 has no name"},
		{name: "a pin without a UID", pins: withPin("2", func(p *PinnedPod) { p.UID = "" }), fate: running,
			wantRemoved: 1, wantErr: "pinned probe pod pinned-2 has no UID"},
		{name: "a pin without a Node", pins: withPin("2", func(p *PinnedPod) { p.Node = "" }), fate: running,
			wantRemoved: 1, wantErr: "pinned probe pod pinned-2 has no Node"},
		{name: "a pin without a provider ID", pins: withPin("2", func(p *PinnedPod) { p.ProviderID = "" }),
			fate: running, wantRemoved: 1,
			wantErr: "pinned probe pod pinned-2 has no provider ID: take spec.providerID of Node 10.0.0.2"},
		{name: "two pins with one UID", pins: withPin("2", func(p *PinnedPod) { p.UID = testPin("1").UID }),
			fate: running, wantRemoved: 1, wantErr: "pinned probe pods pinned-1 and pinned-2 have the same UID uid-pinned-1"},
		{name: "two pins on one worker", pins: withPin("2", func(p *PinnedPod) { p.ProviderID = "ibm://w1" }),
			fate: running, wantRemoved: 1, wantErr: "pinned probe pods pinned-1 and pinned-2 are on the same worker ibm://w1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			got, err := EvaluatePins(tc.pins, tc.fate, nil, tc.wantRemoved)

			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(got).To(Equal(PinResult{}))
		})
	}
}
