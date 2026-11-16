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

// Attribution: the pod filters in this file are adapted from Cluster API v1.14.0,
// core/reconcilers/machine/drain/filters.go and the filter order of drain.go (Copyright 2024 The Kubernetes Authors,
// Apache License 2.0, https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.0/core/reconcilers/machine/drain/filters.go),
// which in turn follow k8s.io/kubectl/pkg/drain/filters.go (The Kubernetes Authors, Apache License 2.0). Changes: the
// filters return one behaviour instead of a PodDeleteStatus chain, the MachineDrainRule filter is left out
// (MachineDrainRules are not supported yet), and the "deleting for more than 1 second" check uses the injected clock.

package drain

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// skipDeletedAfterSeconds is how long a pod on an unreachable Node may have been deleting before the drain stops
// waiting for it: the kubelet will never confirm its termination (Cluster API machine_controller.go:882-893 sets
// SkipWaitForDeleteTimeoutSeconds to 1).
const skipDeletedAfterSeconds = 1

// podBehavior is what the drain does with one pod.
type podBehavior int

const (
	// behaviorEvict: the pod must go away before the Node counts as drained; it is evicted unless it already
	// terminates.
	behaviorEvict podBehavior = iota
	// behaviorSkip: the pod is ignored (running DaemonSet pods, mirror pods, the "skip" label, pods long deleting on
	// an unreachable Node). Volumes of skipped pods are also ignored by VolumesDetached.
	behaviorSkip
	// behaviorWaitCompleted: the pod is never evicted, but the drain waits until it is gone (the "wait-completed"
	// label).
	behaviorWaitCompleted
)

// classifiedPod is a pod and the behaviour the filters chose for it.
type classifiedPod struct {
	pod      *corev1.Pod
	behavior podBehavior
}

// key returns "namespace/name", the sort key of the eviction order.
func (c classifiedPod) key() string {
	return c.pod.Namespace + "/" + c.pod.Name
}

// ref returns the pod's PodRef without a message, as it goes into a DrainResult list.
func (c classifiedPod) ref() PodRef {
	return PodRef{Namespace: c.pod.Namespace, Name: c.pod.Name}
}

// podBehavior runs Cluster API v1.14.0's filters in Cluster API's order (drain.go:132-160) and returns the first
// decision that skips or waits; a pod no filter skips is evicted. The local-storage and no-controller filters only add
// warnings in Cluster API, so they are logged here and never change the outcome: emptyDir pods and pods without a
// controller are evicted (CAPI parity; kubectl drain would refuse them).
//
// It returns an error only when a pod's DaemonSet cannot be read for a reason other than NotFound, as Cluster API
// does (filters.go:219-227).
func (h *Helper) podBehavior(ctx context.Context, pod *corev1.Pod, unreachable bool) (podBehavior, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("Pod", pod.Namespace+"/"+pod.Name)

	if unreachable && h.deletingLongerThan(pod, skipDeletedAfterSeconds) {
		log.V(4).Info(fmt.Sprintf("Skip evicting Pod: the Node is unreachable and the Pod has been deleting for more than %ds", skipDeletedAfterSeconds))
		return behaviorSkip, nil
	}

	skip, err := h.isRunningDaemonSetPod(ctx, pod)
	if err != nil {
		return behaviorEvict, err
	}
	if skip {
		log.V(4).Info("Skip evicting DaemonSet Pod")
		return behaviorSkip, nil
	}

	if _, found := pod.Annotations[corev1.MirrorPodAnnotationKey]; found {
		log.V(4).Info("Skip evicting static Pod")
		return behaviorSkip, nil
	}

	if warning := evictionWarning(pod); warning != "" {
		log.V(4).Info("Pod will be evicted", "warning", warning)
	}

	return drainLabelBehavior(ctx, pod), nil
}

// deletingLongerThan reports whether the pod has a deletionTimestamp more than seconds whole seconds ago. Whole
// seconds, as in Cluster API filters.go:269-273, so "more than 1 second" means at least 2.
func (h *Helper) deletingLongerThan(pod *corev1.Pod, seconds int) bool {
	if pod.DeletionTimestamp.IsZero() {
		return false
	}
	return int(h.Clock.Since(pod.DeletionTimestamp.Time).Seconds()) > seconds
}

// isRunningDaemonSetPod reports whether the pod belongs to a DaemonSet that still exists and has not finished. Such a
// pod is skipped: its DaemonSet would recreate it at once, and it dies with the Node. A finished DaemonSet pod and an
// orphan whose DaemonSet is gone are evicted (Cluster API filters.go:204-232).
func (h *Helper) isRunningDaemonSetPod(ctx context.Context, pod *corev1.Pod) (bool, error) {
	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil || controllerRef.Kind != "DaemonSet" {
		return false, nil
	}
	if podFinished(pod) {
		return false, nil
	}
	key := client.ObjectKey{Namespace: pod.Namespace, Name: controllerRef.Name}
	if err := h.Client.Get(ctx, key, &appsv1.DaemonSet{}); err != nil {
		if apierrors.IsNotFound(err) {
			ctrl.LoggerFrom(ctx).V(4).Info("Evicting orphaned DaemonSet-managed Pod", "Pod", pod.Namespace+"/"+pod.Name)
			return false, nil
		}
		return false, fmt.Errorf("get DaemonSet %s of Pod %s/%s: %w", key, pod.Namespace, pod.Name, err)
	}
	return true, nil
}

// evictionWarning returns Cluster API's warning for a running pod with an emptyDir volume or without a controller,
// or "" (filters.go:243-267). The warning is informational: the pod is evicted either way.
func evictionWarning(pod *corev1.Pod) string {
	if podFinished(pod) {
		return ""
	}
	warnings := []string{}
	for _, volume := range pod.Spec.Volumes {
		if volume.EmptyDir != nil {
			warnings = append(warnings, "evicting Pod with local storage")
			break
		}
	}
	if metav1.GetControllerOf(pod) == nil {
		warnings = append(warnings, "evicting Pod that has no controller")
	}
	return strings.Join(warnings, ", ")
}

// drainLabelBehavior applies the cluster.x-k8s.io/drain pod label: "skip" skips the pod, "wait-completed" waits for
// it without evicting it, both case-insensitive and "wait-completed" also without its dash; any other value is
// logged and the pod is evicted (Cluster API filters.go:284-299).
func drainLabelBehavior(ctx context.Context, pod *corev1.Pod) podBehavior {
	value, found := pod.Labels[clusterv1.PodDrainLabel]
	if !found {
		return behaviorEvict
	}
	switch {
	case strings.EqualFold(value, string(clusterv1.MachineDrainRuleDrainBehaviorSkip)):
		return behaviorSkip
	case strings.EqualFold(strings.Replace(value, "-", "", 1), string(clusterv1.MachineDrainRuleDrainBehaviorWaitCompleted)):
		return behaviorWaitCompleted
	default:
		ctrl.LoggerFrom(ctx).V(4).Info(fmt.Sprintf("Pod has the %s label with the unknown value %q; evicting it",
			clusterv1.PodDrainLabel, value), "Pod", pod.Namespace+"/"+pod.Name)
		return behaviorEvict
	}
}

// podFinished reports whether the pod has terminated (Succeeded or Failed); such pods can always be removed.
func podFinished(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}
