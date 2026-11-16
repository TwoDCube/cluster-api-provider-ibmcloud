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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

const testPDBMessage = "cannot evict pod as it would violate the pod's disruption budget. " +
	"The disruption budget db needs 2 healthy pods and has 2 currently"

func ref(namespace, name string) PodRef {
	return PodRef{Namespace: namespace, Name: name}
}

func refWith(namespace, name, message string) PodRef {
	return PodRef{Namespace: namespace, Name: name, Message: message}
}

// TestConditionMessage checks the Draining condition text: sorted, truncated with "and N more", refusals grouped by
// reason, at most five reasons, and nothing once done.
func TestConditionMessage(t *testing.T) {
	start := time.Date(2026, 11, 16, 11, 0, 0, 0, time.FixedZone("CET", 3600))

	testCases := []struct {
		name    string
		result  DrainResult
		start   time.Time
		maxPods int
		want    string
	}{
		{
			name:    "done renders nothing",
			result:  DrainResult{Done: true},
			start:   start,
			maxPods: 3,
			want:    "",
		},
		{
			name:    "empty result renders nothing",
			result:  DrainResult{},
			start:   start,
			maxPods: 3,
			want:    "",
		},
		{
			name: "every category, start rendered in UTC",
			result: DrainResult{
				Evicted:       []PodRef{ref("apps", "web-1")},
				Terminating:   []PodRef{ref("apps", "web-0")},
				Blocked:       []PodRef{refWith("data", "db-1", testPDBMessage), refWith("data", "db-0", testPDBMessage)},
				Failed:        []PodRef{refWith("apps", "x", "connection reset by peer")},
				WaitCompleted: []PodRef{ref("batch", "job-0")},
			},
			start:   start,
			maxPods: 3,
			want: "Drain not completed yet (started at 2026-11-16T10:00:00Z):\n" +
				"* Pods apps/web-0, apps/web-1: deletionTimestamp set, but still not removed from the Node\n" +
				"* Pods data/db-0, data/db-1: " + testPDBMessage + "\n" +
				"* Pod apps/x: failed to evict Pod, connection reset by peer\n" +
				"* Pod batch/job-0: waiting for completion",
		},
		{
			name: "pods RemainingPods did not evict come last, sorted and truncated",
			result: DrainResult{
				NotEvicted:    []PodRef{ref("a", "p4"), ref("a", "p3"), ref("a", "p2"), ref("a", "p1")},
				WaitCompleted: []PodRef{ref("batch", "job-0")},
			},
			start:   start,
			maxPods: 3,
			want: "Drain not completed yet (started at 2026-11-16T10:00:00Z):\n" +
				"* Pod batch/job-0: waiting for completion\n" +
				"* Pods a/p1, a/p2, a/p3 and 1 more: not evicted",
		},
		{
			name:    "zero start is left out",
			result:  DrainResult{WaitCompleted: []PodRef{ref("batch", "job-0")}},
			maxPods: 3,
			want:    "Drain not completed yet:\n* Pod batch/job-0: waiting for completion",
		},
		{
			name: "lists are truncated with and N more",
			result: DrainResult{
				Terminating: []PodRef{ref("a", "p5"), ref("a", "p4"), ref("a", "p3"), ref("a", "p2"), ref("a", "p1")},
			},
			start:   start,
			maxPods: 3,
			want: "Drain not completed yet (started at 2026-11-16T10:00:00Z):\n" +
				"* Pods a/p1, a/p2, a/p3 and 2 more: deletionTimestamp set, but still not removed from the Node",
		},
		{
			name:    "maxPodsPerList below 1 shows one pod",
			result:  DrainResult{Evicted: []PodRef{ref("a", "p2"), ref("a", "p1")}},
			start:   start,
			maxPods: 0,
			want: "Drain not completed yet (started at 2026-11-16T10:00:00Z):\n" +
				"* Pods a/p1 and 1 more: deletionTimestamp set, but still not removed from the Node",
		},
		{
			name: "more than five reasons are summarised",
			result: DrainResult{
				Blocked: []PodRef{refWith("a", "b1", testPDBMessage)},
				Failed: []PodRef{
					refWith("a", "f1", "e1"), refWith("a", "f2", "e2"), refWith("a", "f3", "e3"),
					refWith("a", "f4", "e4"), refWith("a", "f5", "e5"), refWith("a", "f6", "e6"), refWith("a", "f7", "e6"),
				},
			},
			start:   start,
			maxPods: 3,
			want: "Drain not completed yet (started at 2026-11-16T10:00:00Z):\n" +
				"* Pod a/b1: " + testPDBMessage + "\n" +
				"* Pod a/f1: failed to evict Pod, e1\n" +
				"* Pod a/f2: failed to evict Pod, e2\n" +
				"* Pod a/f3: failed to evict Pod, e3\n" +
				"* Pod a/f4: failed to evict Pod, e4\n" +
				"* 3 Pods with other issues",
		},
		{
			name: "one pod with other issues",
			result: DrainResult{
				Failed: []PodRef{
					refWith("a", "f1", "e1"), refWith("a", "f2", "e2"), refWith("a", "f3", "e3"),
					refWith("a", "f4", "e4"), refWith("a", "f5", "e5"), refWith("a", "f6", "e6"),
				},
			},
			start:   start,
			maxPods: 3,
			want: "Drain not completed yet (started at 2026-11-16T10:00:00Z):\n" +
				"* Pod a/f1: failed to evict Pod, e1\n" +
				"* Pod a/f2: failed to evict Pod, e2\n" +
				"* Pod a/f3: failed to evict Pod, e3\n" +
				"* Pod a/f4: failed to evict Pod, e4\n" +
				"* Pod a/f5: failed to evict Pod, e5\n" +
				"* 1 Pod with other issues",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.result.ConditionMessage(tc.start, tc.maxPods)).To(Equal(tc.want))
		})
	}
}

// TestConditionMessageIsDeterministic proves that the drain's condition message never churns: the same pods in any
// input order render the same bytes, repeatedly, and rendering never reorders the caller's slices.
func TestConditionMessageIsDeterministic(t *testing.T) {
	g := NewWithT(t)
	build := func(reverse bool) DrainResult {
		terminating := make([]PodRef, 0, 8)
		blocked := make([]PodRef, 0, 8)
		for i := range 8 {
			terminating = append(terminating, ref("apps", fmt.Sprintf("web-%d", i)))
			blocked = append(blocked, refWith("data", fmt.Sprintf("db-%d", i), testPDBMessage))
		}
		if reverse {
			slices.Reverse(terminating)
			slices.Reverse(blocked)
		}
		return DrainResult{Terminating: terminating, Blocked: blocked}
	}
	start := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)

	forward := build(false)
	first := forward.ConditionMessage(start, 3)
	g.Expect(forward.ConditionMessage(start, 3)).To(Equal(first))
	g.Expect(build(true).ConditionMessage(start, 3)).To(Equal(first))
	g.Expect(first).To(ContainSubstring("* Pods apps/web-0, apps/web-1, apps/web-2 and 5 more:"))
	g.Expect(first).To(ContainSubstring("* Pods data/db-0, data/db-1, data/db-2 and 5 more: " + testPDBMessage))

	reversed := build(true)
	before := slices.Clone(reversed.Terminating)
	_ = reversed.ConditionMessage(start, 3)
	g.Expect(reversed.Terminating).To(Equal(before), "the caller's slice is not sorted in place")
}

func TestPodRefString(t *testing.T) {
	g := NewWithT(t)
	g.Expect(ref("kube-system", "agent-x").String()).To(Equal("kube-system/agent-x"))
}

func TestNamesPhrase(t *testing.T) {
	testCases := []struct {
		names []string
		max   int
		want  string
	}{
		{names: nil, max: 3, want: ""},
		{names: []string{"a"}, max: 3, want: "a"},
		{names: []string{"a", "b", "c"}, max: 3, want: "a, b, c"},
		{names: []string{"a", "b", "c", "d"}, max: 3, want: "a, b, c and 1 more"},
	}
	for _, tc := range testCases {
		t.Run(strings.Join(tc.names, ","), func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(namesPhrase(tc.names, tc.max)).To(Equal(tc.want))
		})
	}
}
