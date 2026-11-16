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
	"testing"
	"time"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestTimeoutExceeded covers the timeout arithmetic: unset, 0, negative, not exceeded, exactly at the limit, exceeded,
// no start, and a start in the future (clock skew).
func TestTimeoutExceeded(t *testing.T) {
	start := testNow
	testCases := []struct {
		name    string
		now     time.Time
		start   time.Time
		timeout *int32
		want    bool
	}{
		{name: "unset limit never expires", now: start.Add(100 * time.Hour), start: start, timeout: nil, want: false},
		{name: "zero limit means no limit", now: start.Add(100 * time.Hour), start: start, timeout: ptr.To[int32](0), want: false},
		{name: "negative limit means no limit", now: start.Add(100 * time.Hour), start: start, timeout: ptr.To[int32](-5), want: false},
		{name: "not exceeded", now: start.Add(59 * time.Second), start: start, timeout: ptr.To[int32](60), want: false},
		{name: "exactly at the limit is exceeded", now: start.Add(60 * time.Second), start: start, timeout: ptr.To[int32](60), want: true},
		{name: "exceeded", now: start.Add(2 * time.Hour), start: start, timeout: ptr.To[int32](3600), want: true},
		{name: "no start recorded", now: start.Add(2 * time.Hour), start: time.Time{}, timeout: ptr.To[int32](60), want: false},
		{name: "start in the future (skew) is not exceeded", now: start, start: start.Add(10 * time.Minute), timeout: ptr.To[int32](60), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(TimeoutExceeded(tc.now, tc.start, tc.timeout)).To(Equal(tc.want))
		})
	}
}

func TestDrainStartTime(t *testing.T) {
	testCases := []struct {
		name      string
		node      *corev1.Node
		wantStart time.Time
		wantFound bool
		wantErr   string
	}{
		{name: "nil Node", node: nil},
		{name: "no marker", node: newNode(testNode)},
		{
			name:      "valid marker",
			node:      newNode(testNode, withAnnotation(keyStart, "2026-11-16T08:30:00Z")),
			wantStart: time.Date(2026, 11, 16, 8, 30, 0, 0, time.UTC),
			wantFound: true,
		},
		{
			name:      "marker with an offset",
			node:      newNode(testNode, withAnnotation(keyStart, "2026-11-16T09:30:00+01:00")),
			wantStart: time.Date(2026, 11, 16, 8, 30, 0, 0, time.UTC),
			wantFound: true,
		},
		{
			name:      "invalid marker names the annotation and the format",
			node:      newNode(testNode, withAnnotation(keyStart, "16/11/2026")),
			wantFound: true,
			wantErr:   `annotation ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/drain-start-time is "16/11/2026", want an RFC 3339 time`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			h := &Helper{AnnotationPrefix: testPrefix}
			start, found, err := h.DrainStartTime(tc.node)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			} else {
				g.Expect(err).ToNot(HaveOccurred())
			}
			g.Expect(found).To(Equal(tc.wantFound))
			g.Expect(start.Equal(tc.wantStart)).To(BeTrue(), "start %s", start)
		})
	}
}

// TestDrainTimeoutExceeded reads the start from the Node marker, so a restart or a move never resets the limit.
func TestDrainTimeoutExceeded(t *testing.T) {
	testCases := []struct {
		name    string
		node    *corev1.Node
		now     time.Time
		timeout *int32
		want    bool
		wantErr string
	}{
		{
			name:    "start from the annotation, limit not reached",
			node:    newNode(testNode, withAnnotation(keyStart, "2026-11-16T09:30:00Z")),
			now:     testNow,
			timeout: ptr.To[int32](3600),
			want:    false,
		},
		{
			name:    "start from the annotation, limit exceeded after a restart",
			node:    newNode(testNode, withAnnotation(keyStart, "2026-11-16T08:00:00Z")),
			now:     testNow,
			timeout: ptr.To[int32](3600),
			want:    true,
		},
		{
			name:    "no marker yet",
			node:    newNode(testNode),
			now:     testNow,
			timeout: ptr.To[int32](1),
			want:    false,
		},
		{
			name:    "unset limit",
			node:    newNode(testNode, withAnnotation(keyStart, "2026-11-01T00:00:00Z")),
			now:     testNow,
			timeout: nil,
			want:    false,
		},
		{
			name:    "invalid marker",
			node:    newNode(testNode, withAnnotation(keyStart, "soon")),
			now:     testNow,
			timeout: ptr.To[int32](1),
			wantErr: "want an RFC 3339 time",
		},
		{
			name:    "nil Node",
			now:     testNow,
			timeout: ptr.To[int32](1),
			wantErr: "DrainTimeoutExceeded: node is nil",
		},
		{
			name:    "Node without a name",
			node:    newNode("", withAnnotation(keyStart, "2026-11-16T08:00:00Z")),
			now:     testNow,
			timeout: ptr.To[int32](1),
			wantErr: "DrainTimeoutExceeded: node has no name: pass the Node read from the workload cluster",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			h := newTestHelper(newFakeClient(interceptor.Funcs{}), clocktesting.NewFakePassiveClock(tc.now))
			got, err := h.DrainTimeoutExceeded(tc.node, tc.timeout)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestDrainTimeoutExceededRejectsInvalidHelper(t *testing.T) {
	g := NewWithT(t)
	h := &Helper{AnnotationPrefix: testPrefix}
	_, err := h.DrainTimeoutExceeded(newNode(testNode), ptr.To[int32](1))
	g.Expect(err).To(MatchError(ContainSubstring("Helper.Client is nil")))
}
