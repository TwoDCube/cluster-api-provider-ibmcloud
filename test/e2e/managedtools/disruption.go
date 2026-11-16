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
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
)

// EvictionReason is the reason of the DisruptionTarget condition the API server sets on a pod that the Eviction API
// evicts (Kubernetes pod disruption conditions, GA in 1.31). A pod deleted by any other path (a forced node shutdown,
// the taint manager, a direct DELETE) carries another reason or none.
const EvictionReason = "EvictionByEvictionAPI"

// podRecord is what the recorder knows about one probe pod.
type podRecord struct {
	// name is the pod name, for messages.
	name string
	// terminating is true once the pod was seen with a deletionTimestamp.
	terminating bool
	// disruptionReason is the reason of the pod's DisruptionTarget condition when it was seen terminating.
	disruptionReason string
	// vanished is true when the pod disappeared without ever being seen terminating.
	vanished bool
}

// DisruptionRecorder collects what the PodDisruptionBudget probe of a lane observes while CAPIBM drains workers: a lane
// requires zero PDB violations and the DisruptionTarget reason EvictionByEvictionAPI on every evicted pod. The lane
// polls the probe pods and budgets every few seconds and feeds each successful listing in; Result evaluates them. It
// is not safe for concurrent use: one goroutine feeds it and reads the result after it stopped.
//
// Why polling and not a watch: the probe runs through a master upgrade, when watches of the workload API server
// break; a missed poll only widens the window, and a pod deleted inside a window is still reported (as vanished).
// The probe pods keep a termination grace period far longer than the poll interval, so an evicted pod is seen
// terminating.
type DisruptionRecorder struct {
	pods       map[types.UID]*podRecord
	violations []string
	polls      int
}

// NewDisruptionRecorder returns an empty recorder.
func NewDisruptionRecorder() *DisruptionRecorder {
	return &DisruptionRecorder{pods: map[types.UID]*podRecord{}}
}

// ObservePods records one complete listing of the probe pods. A pod seen earlier and missing now, never seen
// terminating, is recorded as vanished. Pass only listings that succeeded: an empty listing from a failed call would
// mark every pod as vanished.
//
// Parameters:
//   - pods are all probe pods (selected by the probe label) at one moment.
func (r *DisruptionRecorder) ObservePods(pods []corev1.Pod) {
	r.polls++
	present := make(map[types.UID]bool, len(pods))
	for i := range pods {
		pod := &pods[i]
		present[pod.UID] = true
		record, ok := r.pods[pod.UID]
		if !ok {
			record = &podRecord{name: pod.Name}
			r.pods[pod.UID] = record
		}
		if pod.DeletionTimestamp != nil {
			record.terminating = true
			if reason := disruptionTargetReason(pod); reason != "" {
				record.disruptionReason = reason
			}
		}
	}
	for uid, record := range r.pods {
		if !present[uid] && !record.terminating {
			record.vanished = true
		}
	}
}

// ObserveBudgets records one listing of the probe's PodDisruptionBudgets and notes every budget whose healthy pods
// are fewer than it requires: a pod left without the budget's permission.
//
// Parameters:
//   - budgets are the probe's PodDisruptionBudgets at one moment.
//   - at is the observation time, for the message.
func (r *DisruptionRecorder) ObserveBudgets(budgets []policyv1.PodDisruptionBudget, at time.Time) {
	for i := range budgets {
		status := budgets[i].Status
		// A budget the controller has not evaluated since its last change has stale numbers.
		if status.ObservedGeneration < budgets[i].Generation {
			continue
		}
		if status.CurrentHealthy < status.DesiredHealthy {
			r.violations = append(r.violations, fmt.Sprintf("PodDisruptionBudget %s had %d healthy pods at %s, "+
				"fewer than the %d it requires", budgets[i].Name, status.CurrentHealthy,
				at.UTC().Format(time.RFC3339), status.DesiredHealthy))
		}
	}
}

// DisruptionResult is the evaluation of a recorder.
type DisruptionResult struct {
	// Polls is the number of pod listings recorded.
	Polls int
	// Evicted is the number of pods evicted through the Eviction API.
	Evicted int
	// Problems are the violations, one sentence each, sorted; empty when the drain respected every budget.
	Problems []string
}

// Result evaluates everything recorded so far: every pod that terminated must carry the DisruptionTarget reason
// EvictionReason, no pod may vanish unseen, and no budget may have been violated.
func (r *DisruptionRecorder) Result() DisruptionResult {
	result := DisruptionResult{Polls: r.polls, Problems: slices.Clone(r.violations)}
	for _, record := range r.pods {
		switch {
		case record.terminating && record.disruptionReason == EvictionReason:
			result.Evicted++
		case record.terminating:
			result.Problems = append(result.Problems, fmt.Sprintf("pod %s terminated with DisruptionTarget reason %q, "+
				"not %s: it was removed without the Eviction API, so no PodDisruptionBudget was asked",
				record.name, record.disruptionReason, EvictionReason))
		case record.vanished:
			result.Problems = append(result.Problems, fmt.Sprintf("pod %s disappeared without being seen terminating: "+
				"it was force-deleted, or the workload API server was unreachable for longer than its grace period",
				record.name))
		}
	}
	slices.Sort(result.Problems)
	return result
}

// disruptionTargetReason returns the reason of the pod's DisruptionTarget condition when its status is True, or "".
//
// Parameters:
//   - pod is a probe pod.
func disruptionTargetReason(pod *corev1.Pod) string {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.DisruptionTarget && condition.Status == corev1.ConditionTrue {
			return condition.Reason
		}
	}
	return ""
}
