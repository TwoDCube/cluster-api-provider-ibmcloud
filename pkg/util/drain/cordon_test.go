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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	keyStart    = testPrefix + MarkerDrainStartTime
	keyOwner    = testPrefix + MarkerDrainOwner
	keyCordoned = testPrefix + MarkerCordoned
	keyLB       = testPrefix + MarkerExcludedFromLB
)

// countPatches returns interceptors that count Node patches and fail them with patchErr when it is set.
func countPatches(count *int, patchErr error) interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			*count++
			if patchErr != nil {
				return patchErr
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

func TestCordon(t *testing.T) {
	startRFC3339 := testNow.Format(time.RFC3339)

	testCases := []struct {
		name            string
		node            *corev1.Node
		opts            CordonOptions
		wantPatches     int
		wantUnsched     bool
		wantAnnotations map[string]string
		wantLabels      map[string]string
	}{
		{
			name:        "schedulable Node gets cordon, markers and LB exclusion",
			node:        newNode(testNode),
			opts:        CordonOptions{Owner: testOwner, ExcludeFromLoadBalancers: true},
			wantPatches: 1,
			wantUnsched: true,
			wantAnnotations: map[string]string{
				keyStart: startRFC3339, keyOwner: testOwner, keyCordoned: "true", keyLB: "true",
			},
			wantLabels: map[string]string{corev1.LabelNodeExcludeBalancers: "true"},
		},
		{
			name:            "LB exclusion switched off adds no label",
			node:            newNode(testNode),
			opts:            CordonOptions{Owner: testOwner},
			wantPatches:     1,
			wantUnsched:     true,
			wantAnnotations: map[string]string{keyStart: startRFC3339, keyOwner: testOwner, keyCordoned: "true"},
		},
		{
			name:            "human-cordoned Node gets no cordoned marker",
			node:            newNode(testNode, cordonedNode),
			opts:            CordonOptions{Owner: testOwner, ExcludeFromLoadBalancers: true},
			wantPatches:     1,
			wantUnsched:     true,
			wantAnnotations: map[string]string{keyStart: startRFC3339, keyOwner: testOwner, keyLB: "true"},
			wantLabels:      map[string]string{corev1.LabelNodeExcludeBalancers: "true"},
		},
		{
			name:            "existing LB label gets no excluded marker",
			node:            newNode(testNode, withNodeLabel(corev1.LabelNodeExcludeBalancers, "false")),
			opts:            CordonOptions{Owner: testOwner, ExcludeFromLoadBalancers: true},
			wantPatches:     1,
			wantUnsched:     true,
			wantAnnotations: map[string]string{keyStart: startRFC3339, keyOwner: testOwner, keyCordoned: "true"},
			wantLabels:      map[string]string{corev1.LabelNodeExcludeBalancers: "false"},
		},
		{
			name: "already cordoned by this pool sends nothing and keeps the start",
			node: newNode(testNode, cordonedNode,
				withAnnotation(keyStart, "2026-11-16T08:00:00Z"), withAnnotation(keyOwner, testOwner),
				withAnnotation(keyCordoned, "true"), withAnnotation(keyLB, "true"),
				withNodeLabel(corev1.LabelNodeExcludeBalancers, "true")),
			opts:        CordonOptions{Owner: testOwner, ExcludeFromLoadBalancers: true},
			wantPatches: 0,
			wantUnsched: true,
			wantAnnotations: map[string]string{
				keyStart: "2026-11-16T08:00:00Z", keyOwner: testOwner, keyCordoned: "true", keyLB: "true",
			},
			wantLabels: map[string]string{corev1.LabelNodeExcludeBalancers: "true"},
		},
		{
			name: "restart keeps the original start time and re-cordons a Node someone uncordoned",
			node: newNode(testNode,
				withAnnotation(keyStart, "2026-11-16T08:00:00Z"), withAnnotation(keyOwner, testOwner),
				withAnnotation(keyCordoned, "true")),
			opts:            CordonOptions{Owner: testOwner},
			wantPatches:     1,
			wantUnsched:     true,
			wantAnnotations: map[string]string{keyStart: "2026-11-16T08:00:00Z", keyOwner: testOwner, keyCordoned: "true"},
		},
		{
			name:            "invalid start marker is replaced",
			node:            newNode(testNode, cordonedNode, withAnnotation(keyStart, "yesterday"), withAnnotation(keyOwner, testOwner)),
			opts:            CordonOptions{Owner: testOwner},
			wantPatches:     1,
			wantUnsched:     true,
			wantAnnotations: map[string]string{keyStart: startRFC3339, keyOwner: testOwner},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			patches := 0
			c := newFakeClient(countPatches(&patches, nil), tc.node)
			h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))
			node := getNode(t, c, testNode)

			g.Expect(h.Cordon(context.Background(), node, tc.opts)).To(Succeed())
			g.Expect(patches).To(Equal(tc.wantPatches))

			stored := getNode(t, c, testNode)
			g.Expect(stored.Spec.Unschedulable).To(Equal(tc.wantUnsched))
			g.Expect(stored.Annotations).To(Equal(tc.wantAnnotations))
			if tc.wantLabels == nil {
				g.Expect(stored.Labels).To(BeEmpty())
			} else {
				g.Expect(stored.Labels).To(Equal(tc.wantLabels))
			}
			g.Expect(node.Annotations).To(Equal(stored.Annotations), "the caller's Node reflects the patch")
		})
	}
}

// TestCordonIsIdempotent calls Cordon twice with the clock moved between the calls: the second call sends nothing and
// the drain start stays the first one.
func TestCordonIsIdempotent(t *testing.T) {
	g := NewWithT(t)
	patches := 0
	clk := clocktesting.NewFakePassiveClock(testNow)
	c := newFakeClient(countPatches(&patches, nil), newNode(testNode))
	h := newTestHelper(c, clk)
	opts := CordonOptions{Owner: testOwner, ExcludeFromLoadBalancers: true}

	g.Expect(h.Cordon(context.Background(), getNode(t, c, testNode), opts)).To(Succeed())
	clk.SetTime(testNow.Add(time.Hour))
	g.Expect(h.Cordon(context.Background(), getNode(t, c, testNode), opts)).To(Succeed())

	g.Expect(patches).To(Equal(1))
	g.Expect(getNode(t, c, testNode).Annotations[keyStart]).To(Equal(testNow.Format(time.RFC3339)))
}

func TestCordonErrors(t *testing.T) {
	testCases := []struct {
		name          string
		node          *corev1.Node
		nilNode       bool
		nameless      bool
		invalidHelper bool
		opts          CordonOptions
		patchErr      error
		wantErr       string
		wantErrIs     error
		wantPatches   int
	}{
		{
			name:          "invalid helper",
			node:          newNode(testNode),
			invalidHelper: true,
			opts:          CordonOptions{Owner: testOwner},
			wantErr:       "Helper.EvictionBudget is 0s",
		},
		{
			name:        "another pool's drain is never taken over",
			node:        newNode(testNode, cordonedNode, withAnnotation(keyOwner, "default/pool-b")),
			opts:        CordonOptions{Owner: testOwner},
			wantErrIs:   ErrDrainOwnerConflict,
			wantErr:     `the Node is drained by another pool "default/pool-b"`,
			wantPatches: 0,
		},
		{
			name:    "empty owner",
			node:    newNode(testNode),
			opts:    CordonOptions{},
			wantErr: `CordonOptions.Owner "" must be "<namespace>/<IBMCloudManagedMachinePool name>"`,
		},
		{
			name:    "owner without namespace",
			node:    newNode(testNode),
			opts:    CordonOptions{Owner: "pool-a"},
			wantErr: "must be \"<namespace>/<IBMCloudManagedMachinePool name>\"",
		},
		{
			name:    "owner with two slashes",
			node:    newNode(testNode),
			opts:    CordonOptions{Owner: "default/pool/a"},
			wantErr: "must be \"<namespace>/<IBMCloudManagedMachinePool name>\"",
		},
		{
			name:    "nil Node",
			nilNode: true,
			opts:    CordonOptions{Owner: testOwner},
			wantErr: "Cordon: node is nil",
		},
		{
			name:     "Node without a name",
			node:     newNode(testNode),
			nameless: true,
			opts:     CordonOptions{Owner: testOwner},
			wantErr:  "Cordon: node has no name: pass the Node read from the workload cluster",
		},
		{
			name:        "patch error leaves the caller's Node unchanged",
			node:        newNode(testNode),
			opts:        CordonOptions{Owner: testOwner},
			patchErr:    errors.New("connection refused"),
			wantErr:     "drain: cordon Node 10.0.0.11: connection refused",
			wantPatches: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			patches := 0
			objs := []client.Object{}
			if tc.node != nil {
				objs = append(objs, tc.node)
			}
			c := newFakeClient(countPatches(&patches, tc.patchErr), objs...)
			h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))
			if tc.invalidHelper {
				h.EvictionBudget = 0
			}
			var node *corev1.Node
			if !tc.nilNode {
				node = getNode(t, c, testNode)
			}
			if tc.nameless {
				node.Name = ""
			}
			before := node.DeepCopy()

			err := h.Cordon(context.Background(), node, tc.opts)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			if tc.wantErrIs != nil {
				g.Expect(errors.Is(err, tc.wantErrIs)).To(BeTrue())
			}
			g.Expect(patches).To(Equal(tc.wantPatches))
			g.Expect(node).To(Equal(before))
		})
	}
}

// TestCordonOptimisticLock proves that a Node changed after the caller read it (for example cordoned by a human) makes
// the patch fail with a conflict instead of recording a cordon CAPIBM did not make.
func TestCordonOptimisticLock(t *testing.T) {
	g := NewWithT(t)
	c := newFakeClient(interceptor.Funcs{}, newNode(testNode))
	h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))
	stale := getNode(t, c, testNode)

	human := getNode(t, c, testNode)
	human.Spec.Unschedulable = true
	g.Expect(c.Update(context.Background(), human)).To(Succeed())

	err := h.Cordon(context.Background(), stale, CordonOptions{Owner: testOwner})
	g.Expect(apierrors.IsConflict(err)).To(BeTrue(), "got %v", err)
	g.Expect(getNode(t, c, testNode).Annotations).ToNot(HaveKey(keyCordoned))
}

// TestUncordon checks that an abort before the IBM call reverts CAPIBM's cordon and LB label and keeps a human's
// cordon and label.
func TestUncordon(t *testing.T) {
	testCases := []struct {
		name        string
		node        *corev1.Node
		wantPatches int
		wantUnsched bool
		wantLabels  map[string]string
	}{
		{
			name: "CAPIBM's cordon and LB label are reverted",
			node: newNode(testNode, cordonedNode,
				withAnnotation(keyStart, "2026-11-16T08:00:00Z"), withAnnotation(keyOwner, testOwner),
				withAnnotation(keyCordoned, "true"), withAnnotation(keyLB, "true"),
				withNodeLabel(corev1.LabelNodeExcludeBalancers, "true"), withNodeLabel("topology.kubernetes.io/zone", "zone-1")),
			wantPatches: 1,
			wantUnsched: false,
			wantLabels:  map[string]string{"topology.kubernetes.io/zone": "zone-1"},
		},
		{
			name: "a human cordon is kept",
			node: newNode(testNode, cordonedNode,
				withAnnotation(keyStart, "2026-11-16T08:00:00Z"), withAnnotation(keyOwner, testOwner),
				withAnnotation(keyLB, "true"), withNodeLabel(corev1.LabelNodeExcludeBalancers, "true")),
			wantPatches: 1,
			wantUnsched: true,
			wantLabels:  map[string]string{},
		},
		{
			name: "a human LB label is kept",
			node: newNode(testNode, cordonedNode,
				withAnnotation(keyStart, "2026-11-16T08:00:00Z"), withAnnotation(keyOwner, testOwner),
				withAnnotation(keyCordoned, "true"), withNodeLabel(corev1.LabelNodeExcludeBalancers, "true")),
			wantPatches: 1,
			wantUnsched: false,
			wantLabels:  map[string]string{corev1.LabelNodeExcludeBalancers: "true"},
		},
		{
			name:        "Node without markers is left alone",
			node:        newNode(testNode, cordonedNode, withNodeLabel(corev1.LabelNodeExcludeBalancers, "true")),
			wantPatches: 0,
			wantUnsched: true,
			wantLabels:  map[string]string{corev1.LabelNodeExcludeBalancers: "true"},
		},
		{
			name:        "unexpected marker values are removed without reverting anything",
			node:        newNode(testNode, cordonedNode, withAnnotation(keyCordoned, "yes"), withAnnotation(keyLB, ""), withNodeLabel(corev1.LabelNodeExcludeBalancers, "true")),
			wantPatches: 1,
			wantUnsched: true,
			wantLabels:  map[string]string{corev1.LabelNodeExcludeBalancers: "true"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			patches := 0
			c := newFakeClient(countPatches(&patches, nil), tc.node)
			h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))
			node := getNode(t, c, testNode)

			g.Expect(h.Uncordon(context.Background(), node)).To(Succeed())
			g.Expect(patches).To(Equal(tc.wantPatches))

			stored := getNode(t, c, testNode)
			g.Expect(stored.Spec.Unschedulable).To(Equal(tc.wantUnsched))
			for _, key := range []string{keyStart, keyOwner, keyCordoned, keyLB} {
				g.Expect(stored.Annotations).ToNot(HaveKey(key))
			}
			if len(tc.wantLabels) == 0 {
				g.Expect(stored.Labels).To(BeEmpty())
			} else {
				g.Expect(stored.Labels).To(Equal(tc.wantLabels))
			}
		})
	}
}

// TestCordonThenUncordon runs a cordon and an abort's uncordon end to end on one Node a human cordoned and one CAPIBM
// cordoned.
func TestCordonThenUncordon(t *testing.T) {
	testCases := []struct {
		name        string
		node        *corev1.Node
		wantUnsched bool
	}{
		{name: "CAPIBM cordon round trip restores the Node", node: newNode(testNode), wantUnsched: false},
		{name: "human cordon survives the round trip", node: newNode(testNode, cordonedNode), wantUnsched: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			c := newFakeClient(interceptor.Funcs{}, tc.node)
			h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))

			node := getNode(t, c, testNode)
			g.Expect(h.Cordon(context.Background(), node, CordonOptions{Owner: testOwner, ExcludeFromLoadBalancers: true})).To(Succeed())
			g.Expect(node.Spec.Unschedulable).To(BeTrue())
			g.Expect(node.Labels).To(HaveKey(corev1.LabelNodeExcludeBalancers))

			g.Expect(h.Uncordon(context.Background(), node)).To(Succeed())
			stored := getNode(t, c, testNode)
			g.Expect(stored.Spec.Unschedulable).To(Equal(tc.wantUnsched))
			g.Expect(stored.Labels).ToNot(HaveKey(corev1.LabelNodeExcludeBalancers))
			g.Expect(stored.Annotations).To(BeEmpty())
		})
	}
}

func TestUncordonErrors(t *testing.T) {
	testCases := []struct {
		name        string
		nilNode     bool
		nameless    bool
		helper      func(c client.Client) *Helper
		patchErr    error
		wantErr     string
		wantPatches int
	}{
		{name: "nil Node", nilNode: true, wantErr: "Uncordon: node is nil"},
		{name: "Node without a name", nameless: true, wantErr: "Uncordon: node has no name: pass the Node read from the workload cluster"},
		{name: "invalid helper", helper: func(c client.Client) *Helper { return &Helper{Client: c} }, wantErr: "Helper.Clock is nil"},
		{name: "patch error", patchErr: errors.New("timeout"), wantErr: "drain: uncordon Node 10.0.0.11: timeout", wantPatches: 1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			patches := 0
			c := newFakeClient(countPatches(&patches, tc.patchErr), newNode(testNode, cordonedNode, withAnnotation(keyCordoned, "true")))
			h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))
			if tc.helper != nil {
				h = tc.helper(c)
			}
			var node *corev1.Node
			if !tc.nilNode {
				node = getNode(t, c, testNode)
			}
			if tc.nameless {
				node.Name = ""
			}
			before := node.DeepCopy()
			g.Expect(h.Uncordon(context.Background(), node)).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(node).To(Equal(before))
			g.Expect(patches).To(Equal(tc.wantPatches))
		})
	}
}

func TestMarkerKey(t *testing.T) {
	g := NewWithT(t)
	h := &Helper{AnnotationPrefix: testPrefix}
	g.Expect(h.MarkerKey(MarkerDrainStartTime)).To(Equal("ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/drain-start-time"))
	g.Expect(h.MarkerKey(MarkerDrainOwner)).To(Equal("ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/drain-owner"))
	g.Expect(h.MarkerKey(MarkerCordoned)).To(Equal("ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/cordoned"))
	g.Expect(h.MarkerKey(MarkerExcludedFromLB)).To(Equal("ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/excluded-from-lb"))
}
