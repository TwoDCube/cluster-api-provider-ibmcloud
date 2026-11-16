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

// Attribution: the pod listing, the eviction loop and the eviction error handling in this file are adapted from
// Cluster API v1.14.0, core/reconcilers/machine/drain/drain.go (Copyright 2024 The Kubernetes Authors, Apache License
// 2.0, https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.0/core/reconcilers/machine/drain/drain.go), and the
// unreachable-Node handling from core/reconcilers/machine/machine_controller.go of the same release. Changes: no
// management-cluster client and no MachineDrainRules (not supported yet), so every pod has drain order 0; the result
// is a DrainResult of PodRefs instead of pod pointers; the eviction budget is measured with an injected clock as well
// as a context deadline; pods blocked by a PodDisruptionBudget are reported apart from other failures.

package drain

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/clock"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// podsPerPage is the page size of every pod list, so a Node with many pods never needs one huge response
	// (Cluster API drain.go:87-110).
	podsPerPage = 100

	// unreachableGracePeriodSeconds is the eviction grace period on a Node whose kubelet is unreachable: the kubelet
	// will never confirm the termination, so a long grace period would only delay the drain (Cluster API
	// machine_controller.go:882-893).
	unreachableGracePeriodSeconds = int64(1)

	// pdbBlockedMessage is the API server's 429 text for an eviction a PodDisruptionBudget refused, lower-cased the way
	// Cluster API renders it in its condition message (drain.go:501).
	pdbBlockedMessage = "cannot evict pod as it would violate the pod's disruption budget."

	// namespaceTerminatingMessage replaces the API server's Forbidden text for an eviction in a terminating namespace;
	// kube-controller-manager sets the pod's deletionTimestamp soon, so the next call treats it as terminating
	// (Cluster API drain.go:390-397).
	namespaceTerminatingMessage = "Cannot evict pod from terminating namespace: unable to create eviction " +
		"(kube-controller-manager should set deletionTimestamp)"
)

// Helper drains Nodes of one workload cluster in short, non-blocking steps with Cluster API v1.14 Machine-drain
// semantics, for a caller that requeues until DrainResult.Done.
//
// All fields are required; every method validates them first and returns an error that names the field to fix.
type Helper struct {
	// Client is the workload-cluster client, normally ClusterCache.GetClient for the Cluster. Pods, DaemonSets,
	// VolumeAttachments and PersistentVolumes should be uncached (ClusterCache DisableFor), so lists hit the API server
	// with a spec.nodeName field selector instead of holding every pod in memory.
	Client client.Client

	// Clock is the time source for the drain start marker, the time limits and the eviction budget; tests inject a
	// fake clock. Production callers pass clock.RealClock{}.
	Clock clock.PassiveClock

	// EvictionBudget bounds the time one Drain call spends sending evictions; pods not reached in time are reported
	// as Failed and retried on the next call (Cluster API: 10 seconds, drain.go:316; CAPIBM:
	// assumptions.EvictionBudgetPerReconcile). Must be positive.
	EvictionBudget time.Duration

	// AnnotationPrefix prefixes the Node marker annotations (MarkerDrainStartTime and the others), for example
	// infrav1.NodeAnnotationPrefix "ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/". It is passed in so that
	// this package imports no CAPIBM API. It must end with "/" and form valid annotation keys.
	AnnotationPrefix string
}

// validate checks the Helper's fields and returns an error that names the field and the expected value.
func (h *Helper) validate() error {
	if h == nil {
		return errors.New("drain: Helper is nil: construct a drain.Helper with Client, Clock, EvictionBudget and AnnotationPrefix")
	}
	if h.Client == nil {
		return errors.New("drain: Helper.Client is nil: pass the workload-cluster client (ClusterCache.GetClient)")
	}
	if h.Clock == nil {
		return errors.New("drain: Helper.Clock is nil: pass clock.RealClock{} (or a fake clock in tests)")
	}
	if h.EvictionBudget <= 0 {
		return fmt.Errorf("drain: Helper.EvictionBudget is %s: pass a positive duration such as 10s (Cluster API's eviction budget)", h.EvictionBudget)
	}
	if !strings.HasSuffix(h.AnnotationPrefix, "/") {
		return fmt.Errorf("drain: Helper.AnnotationPrefix %q must end with \"/\", for example %q",
			h.AnnotationPrefix, "ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/")
	}
	if errs := validation.IsQualifiedName(h.MarkerKey(MarkerDrainStartTime)); len(errs) > 0 {
		return fmt.Errorf("drain: Helper.AnnotationPrefix %q does not form a valid annotation key: %s",
			h.AnnotationPrefix, strings.Join(errs, "; "))
	}
	return nil
}

// validateNode returns an error when node is nil or has no name, naming the method that received it.
//
// The name check is a safety boundary, not a formality: every pod and VolumeAttachment lookup matches on the Node
// name, and an empty name matches the field selector spec.nodeName= of every unscheduled pod in the workload cluster.
// Drain would evict those pods, which belong to no Node of the pool being drained, and VolumesDetached would report a
// false "detached" that lets the IBM call remove a worker whose volumes are still attached.
func validateNode(method string, node *corev1.Node) error {
	if node == nil {
		return fmt.Errorf("drain: %s: node is nil: pass the workload-cluster Node of the worker", method)
	}
	if node.Name == "" {
		return fmt.Errorf("drain: %s: node has no name: pass the Node read from the workload cluster "+
			"(found by its spec.providerID), not a Node built in memory", method)
	}
	return nil
}

// IsNodeUnreachable reports whether the Node's Ready condition is Unknown, which the node lifecycle controller sets
// when the kubelet stops reporting. On such a Node evictions use a 1 second grace period, pods deleting for more than
// a second are skipped and the volume-detach wait is skipped, because the kubelet will never confirm anything
// (Cluster API machine_controller.go:882-903, 978-984; controllers/noderefutil/util.go:77).
//
// Parameters:
//   - node: the Node to inspect; nil is reported as reachable.
func IsNodeUnreachable(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionUnknown
		}
	}
	return false
}

// Drain evicts the evictable pods of one cordoned Node and reports what is left. It never deletes a pod: every pod
// goes through the policy/v1 Eviction subresource, so PodDisruptionBudgets are enforced and each pod keeps its own
// terminationGracePeriodSeconds (1 second on an unreachable Node). Pods that already terminate are not evicted again.
// DrainResult.Done is true only when no pod that needs eviction or completion is left on the Node. The caller cordons
// every victim Node first, so evicted pods never land on another victim; Drain refuses a Node that is not cordoned.
//
// Parameters:
//   - ctx: carries the logger and the caller's deadline; Drain derives the EvictionBudget deadline from it.
//   - node: the Node to drain, as read from the workload cluster; it must be cordoned (spec.unschedulable true).
//
// It returns an error, and no result, when the inputs are invalid, the pods cannot be listed or a pod's DaemonSet
// cannot be read. Refused evictions are not errors; they are reported in DrainResult.Blocked or DrainResult.Failed.
func (h *Helper) Drain(ctx context.Context, node *corev1.Node) (DrainResult, error) {
	if err := h.validate(); err != nil {
		return DrainResult{}, err
	}
	if err := validateNode("Drain", node); err != nil {
		return DrainResult{}, err
	}
	if !node.Spec.Unschedulable {
		return DrainResult{}, fmt.Errorf("drain: Node %s is not cordoned: call Cordon on every victim Node before Drain, "+
			"so evicted pods cannot be scheduled back onto it", node.Name)
	}

	unreachable := IsNodeUnreachable(node)
	pods, err := h.classifyPodsOnNode(ctx, node.Name, unreachable)
	if err != nil {
		return DrainResult{}, fmt.Errorf("drain: Node %s: %w", node.Name, err)
	}

	log := ctrl.LoggerFrom(ctx).WithValues("Node", node.Name)
	result := h.evictPods(ctrl.LoggerInto(ctx, log), pods, unreachable)
	if result.Done {
		log.V(2).Info("Drain completed: no pod left that needs eviction or completion")
	}
	return result, nil
}

// RemainingPods lists the pods that still keep a Node from being drained, without evicting any of them. The caller
// uses it once DrainTimeoutExceeded reports the user's drain time limit as passed: the caller then stops evicting and
// emits the Warning DrainTimeoutExceeded listing the remaining pods, which Drain cannot provide because it always
// sends evictions. The pods go through the same filters as in Drain: skipped pods are left out, wait-completed pods
// are in WaitCompleted, pods with a deletionTimestamp in Terminating, and every pod Drain would evict in NotEvicted.
// Evicted, Blocked and Failed stay empty because no eviction is sent. DrainResult.ConditionMessage renders the result.
//
// Parameters:
//   - ctx: carries the logger and the deadline of the API calls.
//   - node: the Node, as read from the workload cluster. Unlike for Drain it need not be cordoned, since nothing is
//     evicted.
//
// It returns an error, and no result, when the inputs are invalid, the pods cannot be listed or a pod's DaemonSet
// cannot be read.
func (h *Helper) RemainingPods(ctx context.Context, node *corev1.Node) (DrainResult, error) {
	if err := h.validate(); err != nil {
		return DrainResult{}, err
	}
	if err := validateNode("RemainingPods", node); err != nil {
		return DrainResult{}, err
	}
	pods, err := h.classifyPodsOnNode(ctx, node.Name, IsNodeUnreachable(node))
	if err != nil {
		return DrainResult{}, fmt.Errorf("drain: Node %s: %w", node.Name, err)
	}

	result := DrainResult{}
	for _, cp := range pods {
		if !recordWithoutEviction(ctx, &result, cp) {
			result.NotEvicted = append(result.NotEvicted, cp.ref())
		}
	}
	result.Done = result.pendingCount() == 0
	return result, nil
}

// classifyPodsOnNode lists the pods bound to nodeName and decides for each one whether it is evicted, skipped or
// waited for. Shared by Drain, RemainingPods and VolumesDetached, so all three apply the same filters. The pods are
// returned in namespace/name order, so the same Node state always yields the same evictions and the same result
// (Cluster API drain.go:268-272).
func (h *Helper) classifyPodsOnNode(ctx context.Context, nodeName string, unreachable bool) ([]classifiedPod, error) {
	pods, err := h.listPodsOnNode(ctx, nodeName)
	if err != nil {
		return nil, err
	}
	classified := make([]classifiedPod, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		behavior, err := h.podBehavior(ctx, pod, unreachable)
		if err != nil {
			return nil, err
		}
		classified = append(classified, classifiedPod{pod: pod, behavior: behavior})
	}
	slices.SortFunc(classified, func(a, b classifiedPod) int {
		return strings.Compare(a.key(), b.key())
	})
	return classified, nil
}

// recordWithoutEviction files a pod that gets no eviction call into result and reports whether it did: a skipped pod
// is left out, a wait-completed pod goes to WaitCompleted and a pod that already terminates to Terminating. It returns
// false, and records nothing, for a pod that still needs an eviction. Drain and RemainingPods both use it, so a pod
// lands in the same list whichever of them is called.
func recordWithoutEviction(ctx context.Context, result *DrainResult, cp classifiedPod) bool {
	switch {
	case cp.behavior == behaviorSkip:
		ctrl.LoggerFrom(ctx).V(4).Info("Skip evicting Pod", "Pod", cp.key())
		return true
	case cp.behavior == behaviorWaitCompleted:
		result.WaitCompleted = append(result.WaitCompleted, cp.ref())
		return true
	case !cp.pod.DeletionTimestamp.IsZero():
		result.Terminating = append(result.Terminating, cp.ref())
		return true
	default:
		return false
	}
}

// listPodsOnNode lists every pod bound to nodeName in all namespaces, 100 per page, with a spec.nodeName field
// selector (Cluster API drain.go:87-108).
func (h *Helper) listPodsOnNode(ctx context.Context, nodeName string) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	podList := &corev1.PodList{}
	for {
		listOpts := []client.ListOption{
			client.MatchingFields{"spec.nodeName": nodeName},
			client.Limit(podsPerPage),
			client.Continue(podList.Continue),
		}
		if err := h.Client.List(ctx, podList, listOpts...); err != nil {
			return nil, fmt.Errorf("list Pods on the Node: %w", err)
		}
		pods = append(pods, podList.Items...)
		if podList.Continue == "" {
			return pods, nil
		}
	}
}

// evictPods sends the evictions this call may send and sorts every pod that still matters into the DrainResult
// lists. The pods arrive in namespace/name order from classifyPodsOnNode, so the evictions go out in that order.
func (h *Helper) evictPods(ctx context.Context, pods []classifiedPod, unreachable bool) DrainResult {
	// Two bounds on the eviction time, both Cluster API's 10 seconds by default: the context deadline cancels an
	// eviction call that hangs (Cluster API drain.go:316-318), and the clock check lets tests with a fake clock prove
	// that pods left over after the budget are reported and retried.
	budgetStart := h.Clock.Now()
	evictCtx, cancel := context.WithTimeout(ctx, h.EvictionBudget)
	defer cancel()

	result := DrainResult{}
	for _, cp := range pods {
		if recordWithoutEviction(ctx, &result, cp) {
			continue // skipped, waited for, or already terminating: no eviction is sent
		}
		ref := cp.ref()
		if evictCtx.Err() != nil || h.Clock.Since(budgetStart) >= h.EvictionBudget {
			ref.Message = h.budgetMessage()
			result.Failed = append(result.Failed, ref)
			continue
		}
		h.evictOne(evictCtx, cp.pod, ref, unreachable, &result)
	}
	result.Done = result.pendingCount() == 0
	return result
}

// evictOne sends one eviction and records its outcome in result.
func (h *Helper) evictOne(ctx context.Context, pod *corev1.Pod, ref PodRef, unreachable bool, result *DrainResult) {
	log := ctrl.LoggerFrom(ctx).WithValues("Pod", ref.String())
	err := h.evictPod(ctx, pod, unreachable)
	switch {
	case err == nil:
		log.V(4).Info("Pod eviction accepted; waiting for the Pod to go away")
		result.Evicted = append(result.Evicted, ref)
	case apierrors.IsNotFound(err):
		// The pod went away in the meantime: nothing left to wait for.
		log.V(4).Info("Eviction not needed, Pod does not exist anymore")
	case ctx.Err() != nil:
		// The eviction budget ran out while this call was in flight. This is also how a PodDisruptionBudget whose
		// status is still being processed shows up with a real client: the API server sends that 429 with
		// Retry-After 10 s (k8s eviction.go createTooManyRequestsError) and client-go waits it out transparently
		// (client-go rest/with_retry.go checkWait) until the budget deadline cancels the call. The pod is retried on
		// the next call; the fixed budget message, rather than the transport error text, keeps the condition stable.
		log.V(4).Info("Pod eviction did not finish within the eviction budget", "err", err.Error())
		ref.Message = h.budgetMessage()
		result.Failed = append(result.Failed, ref)
	default:
		blocked, message := classifyEvictionError(err)
		ref.Message = message
		log.V(4).Info("Pod eviction refused", "blockedByPodDisruptionBudget", blocked, "reason", message)
		if blocked {
			result.Blocked = append(result.Blocked, ref)
		} else {
			result.Failed = append(result.Failed, ref)
		}
	}
}

// budgetMessage is the Failed reason of a pod whose eviction was not sent, or did not finish, within EvictionBudget
// (Cluster API drain.go:352 wording).
func (h *Helper) budgetMessage() string {
	return fmt.Sprintf("eviction timeout of %s reached, eviction will be retried", h.EvictionBudget)
}

// evictPod sends a policy/v1 Eviction for pod. Without a grace period override the API server uses the pod's own
// terminationGracePeriodSeconds (Cluster API passes GracePeriodSeconds -1, machine_controller.go:876-880); on an
// unreachable Node the grace period is 1 second (machine_controller.go:882-893).
func (h *Helper) evictPod(ctx context.Context, pod *corev1.Pod, unreachable bool) error {
	deleteOptions := metav1.DeleteOptions{}
	if unreachable {
		grace := unreachableGracePeriodSeconds
		deleteOptions.GracePeriodSeconds = &grace
	}
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		DeleteOptions: &deleteOptions,
	}
	return h.Client.SubResource("eviction").Create(ctx, pod, eviction)
}

// classifyEvictionError decides whether a refused eviction was blocked by a PodDisruptionBudget and renders the
// reason for the condition message.
//
// Blocked means HTTP 429 with a DisruptionBudget cause: the budget allows no disruption now, or its status is stale
// ("still being processed by the server", k8s pkg/registry/core/pod/storage/eviction.go:414-436). The stale answer
// carries Retry-After, which client-go retries by itself, so it arrives here only once client-go stops retrying
// within the budget; until then evictOne reports the budget timeout. Everything else is
// a failure that the next call retries: 429 without a budget cause (API priority and fairness), Forbidden (negative
// budget, terminating namespace), 500 (more than one budget matches the pod) and transport errors. The message
// keeps the causes, so it names the budget and its numbers (Cluster API drain.go:370-386).
func classifyEvictionError(err error) (bool, string) {
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return false, err.Error()
	}
	status := statusErr.Status()

	if apierrors.IsForbidden(err) && apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
		return false, namespaceTerminatingMessage
	}
	if !apierrors.IsTooManyRequests(err) {
		return false, err.Error()
	}

	blocked := false
	causes := []string{}
	if status.Details != nil {
		for _, cause := range status.Details.Causes {
			if cause.Type == policyv1.DisruptionBudgetCause {
				blocked = true
			}
			causes = append(causes, cause.Message)
		}
	}
	message := status.Message
	if blocked && strings.EqualFold(message, pdbBlockedMessage) {
		message = pdbBlockedMessage
	}
	if len(causes) > 0 {
		message = message + " " + strings.Join(causes, ",")
	}
	return blocked, message
}
