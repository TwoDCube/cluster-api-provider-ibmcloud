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
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// API server answers to an eviction, as k8s pkg/registry/core/pod/storage/eviction.go builds them.

func pdbBlockedErr(cause string) error {
	err := apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes,
		metav1.StatusCause{Type: policyv1.DisruptionBudgetCause, Message: cause})
	return err
}

func throttledErr() error {
	return apierrors.NewTooManyRequests("Too many requests, please try again later.", 1)
}

func negativeBudgetErr() error {
	return apierrors.NewForbidden(schema.GroupResource{Group: "policy", Resource: "poddisruptionbudget"}, "web",
		errors.New("pdb disruptions allowed is negative"))
}

func namespaceTerminatingErr() error {
	err := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "web-0",
		errors.New("unable to create new content in namespace apps because it is being terminated"))
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes,
		metav1.StatusCause{Type: corev1.NamespaceTerminatingCause, Message: "namespace apps is being terminated"})
	return err
}

func multiplePDBErr() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    500,
		Reason:  metav1.StatusReasonInternalError,
		Message: "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support.",
	}}
}

func notFoundErr(name string) error {
	return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
}

// evictionAnswers returns interceptors whose eviction returns errs[pod name] and otherwise lets the fake client delete
// the pod (the fake's eviction has no PDB check). Every evicted pod name is appended to calls.
func evictionAnswers(errs map[string]error, calls *[]string) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceCreate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
			*calls = append(*calls, obj.GetNamespace()+"/"+obj.GetName())
			if err, found := errs[obj.GetName()]; found {
				return err
			}
			return c.SubResource(subResource).Create(ctx, obj, sub, opts...)
		},
	}
}

func TestHelperValidate(t *testing.T) {
	c := newFakeClient(interceptor.Funcs{})
	clk := clocktesting.NewFakePassiveClock(testNow)
	testCases := []struct {
		name    string
		helper  *Helper
		wantErr string
	}{
		{name: "nil helper", helper: nil, wantErr: "Helper is nil"},
		{name: "nil client", helper: &Helper{Clock: clk, EvictionBudget: time.Second, AnnotationPrefix: testPrefix}, wantErr: "Helper.Client is nil"},
		{name: "nil clock", helper: &Helper{Client: c, EvictionBudget: time.Second, AnnotationPrefix: testPrefix}, wantErr: "Helper.Clock is nil"},
		{name: "zero budget", helper: &Helper{Client: c, Clock: clk, AnnotationPrefix: testPrefix}, wantErr: "Helper.EvictionBudget is 0s"},
		{name: "negative budget", helper: &Helper{Client: c, Clock: clk, EvictionBudget: -time.Second, AnnotationPrefix: testPrefix}, wantErr: "pass a positive duration"},
		{name: "empty prefix", helper: &Helper{Client: c, Clock: clk, EvictionBudget: time.Second}, wantErr: `AnnotationPrefix "" must end with "/"`},
		{name: "prefix without slash", helper: &Helper{Client: c, Clock: clk, EvictionBudget: time.Second, AnnotationPrefix: "example.com"}, wantErr: "must end with"},
		{name: "invalid prefix", helper: &Helper{Client: c, Clock: clk, EvictionBudget: time.Second, AnnotationPrefix: "Not_A_Domain/"}, wantErr: "does not form a valid annotation key"},
		{name: "valid", helper: newTestHelper(c, clk)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := tc.helper.validate()
			if tc.wantErr == "" {
				g.Expect(err).ToNot(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

func TestIsNodeUnreachable(t *testing.T) {
	testCases := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{name: "nil Node", node: nil, want: false},
		{name: "Ready True", node: newNode(testNode), want: false},
		{name: "Ready Unknown", node: newNode(testNode, unreachableNode), want: true},
		{name: "Ready False", node: newNode(testNode, func(n *corev1.Node) {
			n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		}), want: false},
		{name: "no Ready condition", node: newNode(testNode, func(n *corev1.Node) { n.Status.Conditions = nil }), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(IsNodeUnreachable(tc.node)).To(Equal(tc.want))
		})
	}
}

// TestDrain drives one Drain call per case over a fake workload cluster and checks how Drain classifies each eviction
// answer: success, NotFound, 429 with a DisruptionBudget cause, 429 "still being processed", 429 without a cause,
// Forbidden (negative budget, terminating namespace), 500, transport errors, terminating and wait-completed pods.
func TestDrain(t *testing.T) {
	pdbCause := "The disruption budget web needs 2 healthy pods and has 2 currently"
	staleCause := "The disruption budget web is still being processed by the server."

	testCases := []struct {
		name            string
		node            *corev1.Node
		pods            []*corev1.Pod
		evictionErrs    map[string]error
		wantEvicted     []string
		wantBlocked     []string
		wantFailed      []string
		wantTerminating []string
		wantWait        []string
		wantMessages    map[string]string
		wantCalls       []string
		wantDone        bool
	}{
		{
			name:      "no pods on the Node is done",
			node:      newNode(testNode, cordonedNode),
			wantDone:  true,
			wantCalls: []string{},
		},
		{
			name:      "pods on other Nodes are not touched",
			node:      newNode(testNode, cordonedNode),
			pods:      []*corev1.Pod{newPod("apps", "elsewhere", "10.0.0.12")},
			wantDone:  true,
			wantCalls: []string{},
		},
		{
			name: "accepted evictions are not done until the pods are gone",
			node: newNode(testNode, cordonedNode),
			pods: []*corev1.Pod{
				newPod("apps", "web-1", testNode),
				newPod("apps", "web-0", testNode),
				newPod("apps", "bare", testNode, withoutController, withEmptyDir),
			},
			wantEvicted: []string{"apps/bare", "apps/web-0", "apps/web-1"},
			wantCalls:   []string{"apps/bare", "apps/web-0", "apps/web-1"},
		},
		{
			name:         "NotFound means the pod is gone",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": notFoundErr("web-0")},
			wantCalls:    []string{"apps/web-0"},
			wantDone:     true,
		},
		{
			name:         "429 with a DisruptionBudget cause is blocked",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": pdbBlockedErr(pdbCause)},
			wantBlocked:  []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": "cannot evict pod as it would violate the pod's disruption budget. " + pdbCause},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:         "429 while the PDB status is stale is blocked",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": pdbBlockedErr(staleCause)},
			wantBlocked:  []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": "cannot evict pod as it would violate the pod's disruption budget. " + staleCause},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:         "429 without a DisruptionBudget cause fails and is retried",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": throttledErr()},
			wantFailed:   []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": "Too many requests, please try again later."},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:         "Forbidden for a negative budget fails",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": negativeBudgetErr()},
			wantFailed:   []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": negativeBudgetErr().Error()},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:         "Forbidden in a terminating namespace fails with Cluster API's message",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": namespaceTerminatingErr()},
			wantFailed:   []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": namespaceTerminatingMessage},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:         "500 for several PDBs fails",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": multiplePDBErr()},
			wantFailed:   []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support."},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:         "transport error fails",
			node:         newNode(testNode, cordonedNode),
			pods:         []*corev1.Pod{newPod("apps", "web-0", testNode)},
			evictionErrs: map[string]error{"web-0": errors.New("connection reset by peer")},
			wantFailed:   []string{"apps/web-0"},
			wantMessages: map[string]string{"apps/web-0": "connection reset by peer"},
			wantCalls:    []string{"apps/web-0"},
		},
		{
			name:            "terminating pod is waited for and not evicted again",
			node:            newNode(testNode, cordonedNode),
			pods:            []*corev1.Pod{newPod("apps", "web-0", testNode, withDeletion(testNow.Add(-time.Minute)))},
			wantTerminating: []string{"apps/web-0"},
			wantCalls:       []string{},
		},
		{
			name:      "wait-completed pod is waited for and never evicted",
			node:      newNode(testNode, cordonedNode),
			pods:      []*corev1.Pod{newPod("batch", "job-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "wait-completed"))},
			wantWait:  []string{"batch/job-0"},
			wantCalls: []string{},
		},
		{
			name: "skipped pods do not hold the drain",
			node: newNode(testNode, cordonedNode),
			pods: []*corev1.Pod{
				newPod("kube-system", "static-x", testNode, withMirror),
				newPod("apps", "pinned", testNode, withPodLabel(clusterv1.PodDrainLabel, "skip")),
			},
			wantDone:  true,
			wantCalls: []string{},
		},
		{
			name:      "unreachable Node skips pods deleting for more than 1 s",
			node:      newNode(testNode, cordonedNode, unreachableNode),
			pods:      []*corev1.Pod{newPod("apps", "web-0", testNode, withDeletion(testNow.Add(-time.Minute)))},
			wantDone:  true,
			wantCalls: []string{},
		},
		{
			name: "mixed outcomes are each reported once",
			node: newNode(testNode, cordonedNode),
			pods: []*corev1.Pod{
				newPod("apps", "a-evicted", testNode),
				newPod("apps", "b-blocked", testNode),
				newPod("apps", "c-failed", testNode),
				newPod("apps", "d-terminating", testNode, withDeletion(testNow.Add(-time.Minute))),
				newPod("batch", "e-wait", testNode, withPodLabel(clusterv1.PodDrainLabel, "wait-completed")),
				newPod("apps", "f-gone", testNode),
			},
			evictionErrs: map[string]error{
				"b-blocked": pdbBlockedErr(pdbCause),
				"c-failed":  errors.New("boom"),
				"f-gone":    notFoundErr("f-gone"),
			},
			wantEvicted:     []string{"apps/a-evicted"},
			wantBlocked:     []string{"apps/b-blocked"},
			wantFailed:      []string{"apps/c-failed"},
			wantTerminating: []string{"apps/d-terminating"},
			wantWait:        []string{"batch/e-wait"},
			wantMessages: map[string]string{
				"apps/b-blocked": "cannot evict pod as it would violate the pod's disruption budget. " + pdbCause,
				"apps/c-failed":  "boom",
			},
			wantCalls: []string{"apps/a-evicted", "apps/b-blocked", "apps/c-failed", "apps/f-gone"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := []client.Object{tc.node}
			for _, p := range tc.pods {
				objs = append(objs, p)
			}
			calls := []string{}
			h := newTestHelper(newFakeClient(evictionAnswers(tc.evictionErrs, &calls), objs...), clocktesting.NewFakePassiveClock(testNow))

			result, err := h.Drain(context.Background(), tc.node)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(refNames(result.Evicted)).To(ConsistOf(asAny(tc.wantEvicted)...))
			g.Expect(refNames(result.Blocked)).To(ConsistOf(asAny(tc.wantBlocked)...))
			g.Expect(refNames(result.Failed)).To(ConsistOf(asAny(tc.wantFailed)...))
			g.Expect(refNames(result.Terminating)).To(ConsistOf(asAny(tc.wantTerminating)...))
			g.Expect(refNames(result.WaitCompleted)).To(ConsistOf(asAny(tc.wantWait)...))
			g.Expect(result.Done).To(Equal(tc.wantDone))
			g.Expect(calls).To(Equal(tc.wantCalls), "evictions are sent in namespace/name order")
			for _, ref := range append(result.Blocked, result.Failed...) {
				g.Expect(ref.Message).To(Equal(tc.wantMessages[ref.String()]), ref.String())
			}
		})
	}
}

// asAny converts expected names for ConsistOf, which treats an empty call as "empty".
func asAny(names []string) []any {
	out := make([]any, 0, len(names))
	for _, n := range names {
		out = append(out, n)
	}
	return out
}

func TestDrainRejectsInvalidInput(t *testing.T) {
	testCases := []struct {
		name    string
		helper  func(c client.Client) *Helper
		node    *corev1.Node
		wantErr string
	}{
		{
			name:    "invalid helper",
			helper:  func(c client.Client) *Helper { return &Helper{Client: c} },
			node:    newNode(testNode, cordonedNode),
			wantErr: "Helper.Clock is nil",
		},
		{
			name:    "nil Node",
			helper:  func(c client.Client) *Helper { return newTestHelper(c, clocktesting.NewFakePassiveClock(testNow)) },
			wantErr: "Drain: node is nil",
		},
		{
			// An empty name would list pods with the field selector spec.nodeName= and so evict every unscheduled pod
			// of the workload cluster, although a drain may touch only the pods of the Nodes it was given.
			name:    "Node without a name",
			helper:  func(c client.Client) *Helper { return newTestHelper(c, clocktesting.NewFakePassiveClock(testNow)) },
			node:    newNode("", cordonedNode),
			wantErr: "Drain: node has no name: pass the Node read from the workload cluster",
		},
		{
			name:    "Node not cordoned",
			helper:  func(c client.Client) *Helper { return newTestHelper(c, clocktesting.NewFakePassiveClock(testNow)) },
			node:    newNode(testNode),
			wantErr: "Node 10.0.0.11 is not cordoned: call Cordon on every victim Node before Drain",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			calls := []string{}
			// The unscheduled pod is what a nameless Node would match (spec.nodeName is empty).
			c := newFakeClient(evictionAnswers(nil, &calls), newPod("apps", "web-0", testNode), newPod("other", "unscheduled", ""))
			_, err := tc.helper(c).Drain(context.Background(), tc.node)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(calls).To(BeEmpty(), "nothing is evicted on invalid input")
		})
	}
}

func TestDrainListAndDaemonSetErrors(t *testing.T) {
	testCases := []struct {
		name    string
		funcs   interceptor.Funcs
		pods    []*corev1.Pod
		wantErr string
	}{
		{
			name: "pod list error",
			funcs: interceptor.Funcs{List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return errors.New("etcd timeout")
			}},
			wantErr: "drain: Node 10.0.0.11: list Pods on the Node: etcd timeout",
		},
		{
			name: "DaemonSet read error",
			funcs: interceptor.Funcs{Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return errors.New("forbidden")
			}},
			pods:    []*corev1.Pod{newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent"))},
			wantErr: "get DaemonSet kube-system/agent of Pod kube-system/agent-x: forbidden",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := []client.Object{}
			for _, p := range tc.pods {
				objs = append(objs, p)
			}
			h := newTestHelper(newFakeClient(tc.funcs, objs...), clocktesting.NewFakePassiveClock(testNow))
			_, err := h.Drain(context.Background(), newNode(testNode, cordonedNode))
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

// TestDrainUntilGone plays the kubelet: the first call evicts (the fake's eviction deletes the pods), the next call
// finds nothing left and is done. Done never comes from an accepted eviction alone.
func TestDrainUntilGone(t *testing.T) {
	g := NewWithT(t)
	node := newNode(testNode, cordonedNode)
	calls := []string{}
	h := newTestHelper(newFakeClient(evictionAnswers(nil, &calls), node, newPod("apps", "web-0", testNode)),
		clocktesting.NewFakePassiveClock(testNow))

	first, err := h.Drain(context.Background(), node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(first.Evicted)).To(Equal([]string{"apps/web-0"}))
	g.Expect(first.Done).To(BeFalse())

	second, err := h.Drain(context.Background(), node)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(second.Done).To(BeTrue())
	g.Expect(calls).To(Equal([]string{"apps/web-0"}), "a gone pod is not evicted again")
}

// TestDrainEvictionOptions checks the eviction grace period: the pod's own on a reachable Node (no override), 1 second
// on an unreachable one.
func TestDrainEvictionOptions(t *testing.T) {
	testCases := []struct {
		name      string
		node      *corev1.Node
		wantGrace *int64
	}{
		{name: "reachable Node uses the pod's own grace period", node: newNode(testNode, cordonedNode), wantGrace: nil},
		{name: "unreachable Node uses 1 second", node: newNode(testNode, cordonedNode, unreachableNode), wantGrace: ptrInt64(1)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			var got *policyv1.Eviction
			funcs := interceptor.Funcs{
				SubResourceCreate: func(_ context.Context, _ client.Client, subResource string, _ client.Object, sub client.Object, _ ...client.SubResourceCreateOption) error {
					g.Expect(subResource).To(Equal("eviction"))
					got = sub.(*policyv1.Eviction)
					return nil
				},
			}
			h := newTestHelper(newFakeClient(funcs, newPod("apps", "web-0", testNode)), clocktesting.NewFakePassiveClock(testNow))

			_, err := h.Drain(context.Background(), tc.node)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(got).ToNot(BeNil())
			g.Expect(got.Namespace).To(Equal("apps"))
			g.Expect(got.Name).To(Equal("web-0"))
			g.Expect(got.DeleteOptions).ToNot(BeNil())
			g.Expect(got.DeleteOptions.GracePeriodSeconds).To(Equal(tc.wantGrace))
		})
	}
}

func ptrInt64(v int64) *int64 { return &v }

// TestDrainEvictionBudget proves that one call stops sending evictions once EvictionBudget is used up and reports the
// rest as Failed, to be retried: each eviction advances the fake clock by 6 s against a 10 s budget.
func TestDrainEvictionBudget(t *testing.T) {
	g := NewWithT(t)
	clk := clocktesting.NewFakeClock(testNow)
	calls := []string{}
	funcs := interceptor.Funcs{
		SubResourceCreate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
			calls = append(calls, obj.GetName())
			clk.Step(6 * time.Second)
			return nil
		},
	}
	pods := make([]client.Object, 0, 4)
	for i := range 4 {
		pods = append(pods, newPod("apps", fmt.Sprintf("web-%d", i), testNode))
	}
	h := newTestHelper(newFakeClient(funcs, pods...), clk)

	result, err := h.Drain(context.Background(), newNode(testNode, cordonedNode))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(calls).To(Equal([]string{"web-0", "web-1"}))
	g.Expect(refNames(result.Evicted)).To(Equal([]string{"apps/web-0", "apps/web-1"}))
	g.Expect(refNames(result.Failed)).To(Equal([]string{"apps/web-2", "apps/web-3"}))
	for _, ref := range result.Failed {
		g.Expect(ref.Message).To(Equal("eviction timeout of 10s reached, eviction will be retried"))
	}
	g.Expect(result.Done).To(BeFalse())
}

// TestDrainEvictionContextDeadline proves that a hanging eviction call is cancelled by the budget deadline and that
// the pods after it are not sent.
func TestDrainEvictionContextDeadline(t *testing.T) {
	g := NewWithT(t)
	calls := []string{}
	funcs := interceptor.Funcs{
		SubResourceCreate: func(ctx context.Context, _ client.Client, _ string, obj client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
			calls = append(calls, obj.GetName())
			<-ctx.Done()
			return ctx.Err()
		},
	}
	h := newTestHelper(newFakeClient(funcs, newPod("apps", "web-0", testNode), newPod("apps", "web-1", testNode)),
		clocktesting.NewFakePassiveClock(testNow))
	h.EvictionBudget = 20 * time.Millisecond

	result, err := h.Drain(context.Background(), newNode(testNode, cordonedNode))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(calls).To(Equal([]string{"web-0"}))
	g.Expect(result.Failed).To(Equal([]PodRef{
		{Namespace: "apps", Name: "web-0", Message: "eviction timeout of 20ms reached, eviction will be retried"},
		{Namespace: "apps", Name: "web-1", Message: "eviction timeout of 20ms reached, eviction will be retried"},
	}))
}

// TestDrainPagesPodList proves the pod list uses the spec.nodeName field selector, 100 per page, and follows the
// continue token.
func TestDrainPagesPodList(t *testing.T) {
	g := NewWithT(t)
	listCalls := []client.ListOptions{}
	funcs := interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			lo := client.ListOptions{}
			lo.ApplyOptions(opts)
			listCalls = append(listCalls, lo)
			podList := list.(*corev1.PodList)
			if lo.Continue == "" {
				*podList = corev1.PodList{Items: []corev1.Pod{*newPod("apps", "page1", testNode)}}
				podList.Continue = "token-2"
				return nil
			}
			*podList = corev1.PodList{Items: []corev1.Pod{*newPod("apps", "page2", testNode)}}
			return nil
		},
		SubResourceCreate: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
			return nil
		},
	}
	h := newTestHelper(newFakeClient(funcs), clocktesting.NewFakePassiveClock(testNow))

	result, err := h.Drain(context.Background(), newNode(testNode, cordonedNode))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(refNames(result.Evicted)).To(Equal([]string{"apps/page1", "apps/page2"}))
	g.Expect(listCalls).To(HaveLen(2))
	g.Expect(listCalls[0].Limit).To(Equal(int64(100)))
	g.Expect(listCalls[0].FieldSelector.String()).To(Equal("spec.nodeName=" + testNode))
	g.Expect(listCalls[1].Continue).To(Equal("token-2"))
}

func TestClassifyEvictionError(t *testing.T) {
	testCases := []struct {
		name        string
		err         error
		wantBlocked bool
		wantMessage string
	}{
		{
			name:        "429 DisruptionBudget",
			err:         pdbBlockedErr("The disruption budget web needs 2 healthy pods and has 2 currently"),
			wantBlocked: true,
			wantMessage: "cannot evict pod as it would violate the pod's disruption budget. The disruption budget web needs 2 healthy pods and has 2 currently",
		},
		{
			name:        "429 stale PDB",
			err:         pdbBlockedErr("The disruption budget web is still being processed by the server."),
			wantBlocked: true,
			wantMessage: "cannot evict pod as it would violate the pod's disruption budget. The disruption budget web is still being processed by the server.",
		},
		{
			name:        "429 without causes",
			err:         throttledErr(),
			wantMessage: "Too many requests, please try again later.",
		},
		{
			name:        "Forbidden negative budget",
			err:         negativeBudgetErr(),
			wantMessage: negativeBudgetErr().Error(),
		},
		{
			name:        "Forbidden namespace terminating",
			err:         namespaceTerminatingErr(),
			wantMessage: namespaceTerminatingMessage,
		},
		{
			name:        "500 several PDBs",
			err:         multiplePDBErr(),
			wantMessage: "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support.",
		},
		{
			name:        "wrapped status error keeps its class",
			err:         fmt.Errorf("evict: %w", pdbBlockedErr("The disruption budget db does not allow evicting pods currently")),
			wantBlocked: true,
			wantMessage: "cannot evict pod as it would violate the pod's disruption budget. The disruption budget db does not allow evicting pods currently",
		},
		{
			name:        "plain error",
			err:         errors.New("i/o timeout"),
			wantMessage: "i/o timeout",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			blocked, message := classifyEvictionError(tc.err)
			g.Expect(blocked).To(Equal(tc.wantBlocked))
			g.Expect(message).To(Equal(tc.wantMessage))
		})
	}
}

// TestRemainingPods lists what is still on a Node once the drain time limit has passed, without sending a single
// eviction: the same filters as Drain, pods Drain would evict in NotEvicted, and a ConditionMessage that can serve as
// the DrainTimeoutExceeded Warning.
func TestRemainingPods(t *testing.T) {
	daemonSet := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "agent"}}
	start := testNow.Add(-time.Hour)

	testCases := []struct {
		name            string
		node            *corev1.Node
		objects         []client.Object
		wantNotEvicted  []string
		wantTerminating []string
		wantWait        []string
		wantDone        bool
		wantMessage     string
	}{
		{
			name:     "no pods on the Node is done",
			node:     newNode(testNode, cordonedNode),
			objects:  []client.Object{newPod("apps", "elsewhere", "10.0.0.12")},
			wantDone: true,
		},
		{
			name: "every kind of pod lands in its list in namespace/name order",
			node: newNode(testNode, cordonedNode),
			objects: []client.Object{
				newPod("apps", "web-1", testNode),
				newPod("apps", "web-0", testNode),
				newPod("apps", "bare", testNode, withoutController, withEmptyDir),
				newPod("apps", "d-terminating", testNode, withDeletion(testNow.Add(-time.Minute))),
				newPod("batch", "job-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "wait-completed")),
				newPod("apps", "pinned", testNode, withPodLabel(clusterv1.PodDrainLabel, "skip")),
				newPod("kube-system", "static-x", testNode, withMirror),
				newPod("apps", "elsewhere", "10.0.0.12"),
			},
			wantNotEvicted:  []string{"apps/bare", "apps/web-0", "apps/web-1"},
			wantTerminating: []string{"apps/d-terminating"},
			wantWait:        []string{"batch/job-0"},
			wantMessage: "Drain not completed yet (started at 2026-11-16T09:00:00Z):\n" +
				"* Pod apps/d-terminating: deletionTimestamp set, but still not removed from the Node\n" +
				"* Pod batch/job-0: waiting for completion\n" +
				"* Pods apps/bare, apps/web-0, apps/web-1: not evicted",
		},
		{
			name: "DaemonSet pod is left out while its DaemonSet exists, an orphan is listed",
			node: newNode(testNode, cordonedNode),
			objects: []client.Object{
				daemonSet,
				newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent")),
				newPod("kube-system", "orphan-x", testNode, withController("DaemonSet", "gone")),
			},
			wantNotEvicted: []string{"kube-system/orphan-x"},
		},
		{
			name: "unreachable Node leaves out pods deleting for more than 1 s",
			node: newNode(testNode, cordonedNode, unreachableNode),
			objects: []client.Object{
				newPod("apps", "web-0", testNode, withDeletion(testNow.Add(-time.Minute))),
				newPod("apps", "web-1", testNode),
			},
			wantNotEvicted: []string{"apps/web-1"},
		},
		{
			name:           "a Node that is not cordoned is read as well, since nothing is evicted",
			node:           newNode(testNode),
			objects:        []client.Object{newPod("apps", "web-0", testNode)},
			wantNotEvicted: []string{"apps/web-0"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			calls := []string{}
			h := newTestHelper(newFakeClient(evictionAnswers(nil, &calls), append(tc.objects, tc.node)...),
				clocktesting.NewFakePassiveClock(testNow))

			result, err := h.RemainingPods(context.Background(), tc.node)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(calls).To(BeEmpty(), "RemainingPods never sends an eviction")
			g.Expect(refNames(result.NotEvicted)).To(Equal(orEmpty(tc.wantNotEvicted)))
			g.Expect(refNames(result.Terminating)).To(Equal(orEmpty(tc.wantTerminating)))
			g.Expect(refNames(result.WaitCompleted)).To(Equal(orEmpty(tc.wantWait)))
			g.Expect(result.Evicted).To(BeEmpty())
			g.Expect(result.Blocked).To(BeEmpty())
			g.Expect(result.Failed).To(BeEmpty())
			g.Expect(result.Done).To(Equal(tc.wantDone))
			if tc.wantMessage != "" {
				g.Expect(result.ConditionMessage(start, 3)).To(Equal(tc.wantMessage))
			}
		})
	}
}

func TestRemainingPodsRejectsInvalidInput(t *testing.T) {
	testCases := []struct {
		name    string
		helper  func(c client.Client) *Helper
		node    *corev1.Node
		funcs   interceptor.Funcs
		wantErr string
	}{
		{
			name:    "invalid helper",
			helper:  func(c client.Client) *Helper { return &Helper{Client: c} },
			node:    newNode(testNode, cordonedNode),
			wantErr: "Helper.Clock is nil",
		},
		{
			name:    "nil Node",
			wantErr: "RemainingPods: node is nil",
		},
		{
			name:    "Node without a name",
			node:    newNode("", cordonedNode),
			wantErr: "RemainingPods: node has no name: pass the Node read from the workload cluster",
		},
		{
			name: "pod list error",
			node: newNode(testNode, cordonedNode),
			funcs: interceptor.Funcs{List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return errors.New("etcd timeout")
			}},
			wantErr: "drain: Node 10.0.0.11: list Pods on the Node: etcd timeout",
		},
		{
			name: "DaemonSet read error",
			node: newNode(testNode, cordonedNode),
			funcs: interceptor.Funcs{Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return errors.New("forbidden")
			}},
			wantErr: "get DaemonSet kube-system/agent of Pod kube-system/agent-x: forbidden",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			c := newFakeClient(tc.funcs,
				newPod("kube-system", "agent-x", testNode, withController("DaemonSet", "agent")),
				newPod("other", "unscheduled", ""))
			h := newTestHelper(c, clocktesting.NewFakePassiveClock(testNow))
			if tc.helper != nil {
				h = tc.helper(c)
			}
			_, err := h.RemainingPods(context.Background(), tc.node)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

// orEmpty turns a nil expectation into an empty slice, because refNames never returns nil.
func orEmpty(names []string) []string {
	if names == nil {
		return []string{}
	}
	return names
}
