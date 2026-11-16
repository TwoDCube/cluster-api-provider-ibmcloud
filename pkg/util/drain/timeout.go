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
	"time"

	corev1 "k8s.io/api/core/v1"
)

// TimeoutExceeded reports whether a Cluster API drain time limit has passed, with the semantics of Cluster API
// v1.14.0 nodeDrainTimeoutExceeded and nodeVolumeDetachTimeoutExceeded (machine_controller.go:724-756), which the
// caller applies to MachinePool.spec.template.spec.deletion.nodeDrainTimeoutSeconds and
// nodeVolumeDetachTimeoutSeconds:
//   - no limit (nil, 0 or negative): never exceeded, so a blocking PodDisruptionBudget holds the operation, as it does
//     for a MachineDeployment;
//   - no start recorded (zero time): not exceeded;
//   - otherwise exceeded once now - start >= limit. A start in the future (clock skew between the writer of the marker
//     and this controller) gives a negative elapsed time and is not exceeded: skew can only make the drain wait longer.
//
// Parameters:
//   - now: the current time, from the caller's clock.
//   - start: when the measured step started, for example DrainStartTime of the Node.
//   - timeoutSeconds: the user's limit in seconds; nil means unset.
func TimeoutExceeded(now, start time.Time, timeoutSeconds *int32) bool {
	if timeoutSeconds == nil || *timeoutSeconds <= 0 || start.IsZero() {
		return false
	}
	return now.Sub(start) >= time.Duration(*timeoutSeconds)*time.Second
}

// DrainStartTime reads the drain-start-time marker that Cordon wrote on the Node. Because the marker lives on the Node,
// the drain time limit keeps counting across controller restarts and clusterctl moves.
//
// Parameters:
//   - node: the Node to read; nil has no marker.
//
// It returns found false when the marker is absent, and an error naming the annotation and the expected RFC 3339
// form when it cannot be parsed (Cordon replaces such a value on its next call).
func (h *Helper) DrainStartTime(node *corev1.Node) (time.Time, bool, error) {
	if node == nil {
		return time.Time{}, false, nil
	}
	key := h.MarkerKey(MarkerDrainStartTime)
	value, found := node.Annotations[key]
	if !found {
		return time.Time{}, false, nil
	}
	start, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, true, fmt.Errorf("drain: Node %s annotation %s is %q, want an RFC 3339 time such as %q; "+
			"Cordon replaces it, or remove the annotation to restart the drain clock",
			node.Name, key, value, "2026-11-16T10:00:00Z")
	}
	return start, true, nil
}

// DrainTimeoutExceeded reports whether the user's nodeDrainTimeoutSeconds has passed since the Node's
// drain-start-time marker, measured with Helper.Clock. A Node without the marker has not started draining, so the
// limit is not exceeded. Once it is exceeded the caller stops calling Drain, lists the pods still on the Node with
// RemainingPods (which sends no eviction), emits the Warning DrainTimeoutExceeded with that list and proceeds.
//
// Parameters:
//   - node: the Node being drained.
//   - timeoutSeconds: MachinePool.spec.template.spec.deletion.nodeDrainTimeoutSeconds, read on every reconcile so an
//     edit takes effect at once; nil means unset (no limit).
//
// It returns an error when the Helper is invalid or the marker cannot be parsed.
func (h *Helper) DrainTimeoutExceeded(node *corev1.Node, timeoutSeconds *int32) (bool, error) {
	if err := h.validate(); err != nil {
		return false, err
	}
	if err := validateNode("DrainTimeoutExceeded", node); err != nil {
		return false, err
	}
	start, found, err := h.DrainStartTime(node)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	return TimeoutExceeded(h.Clock.Now(), start, timeoutSeconds), nil
}
