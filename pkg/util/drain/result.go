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

// Attribution: DrainResult and its condition message in this file are adapted from Cluster API v1.14.0,
// core/reconcilers/machine/drain/drain.go (EvictionResult, DrainCompleted, ConditionMessage and PodListToString,
// lines 450-554; Copyright 2024 The Kubernetes Authors, Apache License 2.0,
// https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.0/core/reconcilers/machine/drain/drain.go). Kept: the
// header text, the "deletionTimestamp set, but still not removed from the Node" and "waiting for completion" lines, the
// lower-cased PodDisruptionBudget refusal, the "failed to evict Pod, " prefix, the limit of five refusal reasons and
// the "N Pods with other issues" summary. Changes: the result holds PodRefs instead of pod pointers and keeps
// PodDisruptionBudget refusals (Blocked, listed first) apart from other failures (Failed); accepted evictions and pods
// that were already terminating share one line; the start time is optional and rendered in UTC; the number of pods
// per line is a parameter and the rest is counted as "and N more" instead of "... (N more)"; the "After above Pods
// have been removed" lines are left out because there is no drain order (MachineDrainRules are not supported yet); the
// NotEvicted list and its line, filled by RemainingPods, are new.

package drain

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// maxFailureMessages is how many distinct eviction refusals ConditionMessage lists before it summarises the rest as
// "N Pods with other issues" (Cluster API drain.go:489-492).
const maxFailureMessages = 5

// PodRef names a pod that is still in the way of a drain and, for refused evictions, why.
type PodRef struct {
	// Namespace is the pod's namespace.
	Namespace string
	// Name is the pod's name.
	Name string
	// Message is the API server's reason for a refused eviction (DrainResult.Blocked and DrainResult.Failed), with the
	// PodDisruptionBudget causes appended; empty for the other lists.
	Message string
}

// String returns "namespace/name".
func (p PodRef) String() string {
	return p.Namespace + "/" + p.Name
}

// DrainResult classifies the pods that one Drain or RemainingPods call found still in the way on a Node. Each list is in
// namespace/name order.
type DrainResult struct { //nolint:revive // the stuttering name is kept: the consumers in the pool controller use it.
	// Evicted are pods whose eviction this call sent and the API server accepted; they still have to go away.
	Evicted []PodRef
	// Blocked are pods whose eviction a PodDisruptionBudget refused (HTTP 429); retried on the next call.
	Blocked []PodRef
	// Failed are pods whose eviction failed for another reason, or was not sent because the eviction budget was used
	// up; retried on the next call.
	Failed []PodRef
	// Terminating are pods that already had a deletionTimestamp; the drain waits for them to go away.
	Terminating []PodRef
	// WaitCompleted are pods labelled cluster.x-k8s.io/drain=wait-completed; never evicted, waited for until gone.
	WaitCompleted []PodRef
	// NotEvicted are pods that still need an eviction that was not sent. Only RemainingPods fills it, for the Warning
	// DrainTimeoutExceeded after the drain time limit has passed; Drain sends those evictions instead.
	NotEvicted []PodRef
	// Done is true when no pod that needs eviction or completion is left on the Node: every list above is empty. An
	// accepted eviction is not enough; the pod must be gone, which is what honours its grace period.
	Done bool
}

// pendingCount returns how many pods are still in the way.
func (r DrainResult) pendingCount() int {
	return len(r.Evicted) + len(r.Blocked) + len(r.Failed) + len(r.Terminating) + len(r.WaitCompleted) +
		len(r.NotEvicted)
}

// ConditionMessage renders the result for the Draining condition. It returns "" once the drain is done. The text
// follows Cluster API v1.14.0's drain condition message (drain.go:469-540) and is deterministic: pods are sorted, each
// list shows at most maxPodsPerList pods plus "and N more", refusals are grouped by reason in sorted order, and the
// time is rendered in UTC. The same Node state therefore always yields the same bytes, so a repeat reconcile writes
// nothing (status is written only on change). Evicted and Terminating pods share one line, because a pod moves from
// the first to the second between two calls without anything changing for the user. NotEvicted pods (RemainingPods)
// come last, on a "not evicted" line, so the same text also serves the DrainTimeoutExceeded Warning.
//
// Parameters:
//   - start: the drain start (the Node's drain-start-time marker); the zero time leaves the start out.
//   - maxPodsPerList: how many pods each line names (the pool controller passes 3); values below 1 are treated as 1.
func (r DrainResult) ConditionMessage(start time.Time, maxPodsPerList int) string {
	if r.Done || r.pendingCount() == 0 {
		return ""
	}
	if maxPodsPerList < 1 {
		maxPodsPerList = 1
	}

	header := "Drain not completed yet:"
	if !start.IsZero() {
		header = fmt.Sprintf("Drain not completed yet (started at %s):", start.UTC().Format(time.RFC3339))
	}
	lines := []string{header}

	deleting := sortedRefs(slices.Concat(r.Evicted, r.Terminating))
	if len(deleting) > 0 {
		lines = append(lines, fmt.Sprintf("* %s: deletionTimestamp set, but still not removed from the Node",
			podsPhrase(deleting, maxPodsPerList)))
	}
	lines = append(lines, refusalLines(r.Blocked, r.Failed, maxPodsPerList)...)
	if len(r.WaitCompleted) > 0 {
		lines = append(lines, fmt.Sprintf("* %s: waiting for completion",
			podsPhrase(sortedRefs(r.WaitCompleted), maxPodsPerList)))
	}
	if len(r.NotEvicted) > 0 {
		lines = append(lines, fmt.Sprintf("* %s: not evicted", podsPhrase(sortedRefs(r.NotEvicted), maxPodsPerList)))
	}
	return strings.Join(lines, "\n")
}

// refusalGroup is the pods refused for one reason.
type refusalGroup struct {
	reason string
	pods   []PodRef
}

// refusalLines renders one line per distinct refusal reason, PodDisruptionBudget refusals first, each kind in sorted
// reason order, at most maxFailureMessages lines and then a count of the pods with other issues (Cluster API
// drain.go:487-529). Failed reasons get Cluster API's "failed to evict Pod, " prefix.
func refusalLines(blocked, failed []PodRef, maxPodsPerList int) []string {
	groups := slices.Concat(groupByReason(blocked, ""), groupByReason(failed, "failed to evict Pod, "))

	lines := []string{}
	otherPods := 0
	for i, g := range groups {
		if i >= maxFailureMessages {
			otherPods += len(g.pods)
			continue
		}
		lines = append(lines, fmt.Sprintf("* %s: %s", podsPhrase(g.pods, maxPodsPerList), g.reason))
	}
	switch {
	case otherPods == 1:
		lines = append(lines, "* 1 Pod with other issues")
	case otherPods > 1:
		lines = append(lines, fmt.Sprintf("* %d Pods with other issues", otherPods))
	}
	return lines
}

// groupByReason groups refs by prefix+Message, in sorted reason order with sorted pods.
func groupByReason(refs []PodRef, prefix string) []refusalGroup {
	byReason := map[string][]PodRef{}
	for _, ref := range refs {
		reason := prefix + ref.Message
		byReason[reason] = append(byReason[reason], ref)
	}
	reasons := make([]string, 0, len(byReason))
	for reason := range byReason {
		reasons = append(reasons, reason)
	}
	slices.Sort(reasons)

	groups := make([]refusalGroup, 0, len(reasons))
	for _, reason := range reasons {
		groups = append(groups, refusalGroup{reason: reason, pods: sortedRefs(byReason[reason])})
	}
	return groups
}

// sortedRefs returns a copy of refs sorted by namespace/name, so the caller's slice is never reordered.
func sortedRefs(refs []PodRef) []PodRef {
	sorted := slices.Clone(refs)
	slices.SortFunc(sorted, func(a, b PodRef) int {
		return strings.Compare(a.String(), b.String())
	})
	return sorted
}

// podsPhrase renders "Pod a/b" or "Pods a/b, c/d, e/f and 2 more": at most maxPods names, then the count of the
// rest. The caller passes refs already sorted.
func podsPhrase(refs []PodRef, maxPods int) string {
	kind := "Pod"
	if len(refs) > 1 {
		kind = "Pods"
	}
	return kind + " " + namesPhrase(podNames(refs), maxPods)
}

// podNames returns "namespace/name" for every ref, in order.
func podNames(refs []PodRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.String())
	}
	return names
}

// namesPhrase joins at most maxNames names with ", " and appends " and N more" for the rest.
func namesPhrase(names []string, maxNames int) string {
	if len(names) <= maxNames {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxNames], ", "), len(names)-maxNames)
}
