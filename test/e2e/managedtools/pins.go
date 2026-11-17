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
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// This file holds the worker-by-worker proof of the IKS lane's drains. A drain step pins one probe pod (no controller,
// no PodDisruptionBudget) to every worker it may remove before it starts; afterwards the pod of every removed worker
// must have been evicted through the Eviction API and the pod of every kept worker must still run. The spread probe
// cannot give that proof: its spread over the workers is only a preference, and in one run of the lane the scale-down
// drained its 3 victims while every spread pod ran on the 3 workers it kept, so no spread pod was evicted.

// PinnedPod is one probe pod that a drain step pinned to one worker before the step.
type PinnedPod struct {
	// Name is the pod's name, for messages.
	Name string
	// UID identifies the pod in the DisruptionRecorder (DisruptionRecorder.Fate).
	UID types.UID
	// Node is the name of the Node the pod runs on, for messages.
	Node string
	// ProviderID is that Node's spec.providerID, the IBM worker's identity. It, not the Node name, tells a kept worker
	// from a removed one: a replacement worker always gets a new provider ID, while its Node name, the worker's private
	// IP address on IKS, may come back.
	ProviderID string
}

// PinResult is the evaluation of the pinned probe pods of one drain step.
type PinResult struct {
	// Evicted is the number of pinned pods whose worker the step removed and that the Eviction API evicted.
	Evicted int
	// Kept is the number of pinned pods whose worker the step kept and that still ran untouched.
	Kept int
	// Problems are the failed expectations, one sentence each, sorted; empty when the pins prove the drain.
	Problems []string
}

// EvaluatePins checks the pinned probe pods of a drain step once the step has settled: the pod of every pinned worker
// that the step removed or replaced must have been evicted through the Eviction API (a pod deleted otherwise, gone
// unseen, never listed or still running after its worker went away proves that the worker was not drained), the pod
// of every pinned worker that the step kept must still run untouched (a drain of a worker that stays is a wrong
// victim), and exactly wantRemoved pinned workers must be gone. It is pure.
//
// Parameters:
//   - pins are the step's pinned pods, one per worker the step may remove: at least one, each with a name, a UID, a
//     Node and a provider ID, and no two with the same UID or on the same worker.
//   - fate tells what the disruption probe saw happen to a pod during the step, normally DisruptionRecorder.Fate of the
//     recorder that ran through the step; it must not be nil.
//   - nodesAfter are every Node of the workload cluster after the step; a pinned worker counts as kept when one of
//     them has its provider ID, and as removed otherwise.
//   - wantRemoved is the number of pinned workers the step removes or replaces: at least 1, because a step that removes
//     none of them proves no drain, and at most len(pins).
//
// It returns an error naming the argument to fix, and no result, when the arguments break these rules.
func EvaluatePins(pins []PinnedPod, fate func(types.UID) PodFate, nodesAfter []corev1.Node, wantRemoved int) (PinResult, error) {
	if err := validatePins(pins, fate, wantRemoved); err != nil {
		return PinResult{}, err
	}
	kept := make(map[string]bool, len(nodesAfter))
	for i := range nodesAfter {
		if id := nodesAfter[i].Spec.ProviderID; id != "" {
			kept[id] = true
		}
	}
	var result PinResult
	removed := 0
	for _, pin := range pins {
		got := fate(pin.UID)
		if kept[pin.ProviderID] {
			if got == PodRunning {
				result.Kept++
				continue
			}
			result.Problems = append(result.Problems, fmt.Sprintf("pinned probe pod %s on Node %s (worker %s), which "+
				"the step kept, was %s, not %s: the drain reached a worker that stays", pin.Name, pin.Node,
				pin.ProviderID, got, PodRunning))
			continue
		}
		removed++
		if got == PodEvicted {
			result.Evicted++
			continue
		}
		result.Problems = append(result.Problems, fmt.Sprintf("pinned probe pod %s on Node %s (worker %s), which the "+
			"step removed, was %s, not %s: the worker went away without a drain that evicted its pods", pin.Name,
			pin.Node, pin.ProviderID, got, PodEvicted))
	}
	if removed != wantRemoved {
		result.Problems = append(result.Problems, fmt.Sprintf("the step removed %d of the %d pinned workers, not %d",
			removed, len(pins), wantRemoved))
	}
	slices.Sort(result.Problems)
	return result, nil
}

// validatePins checks EvaluatePins' arguments and names the one to fix.
//
// Parameters:
//   - pins, fate and wantRemoved are EvaluatePins' arguments.
func validatePins(pins []PinnedPod, fate func(types.UID) PodFate, wantRemoved int) error {
	if len(pins) == 0 {
		return errors.New("no pinned probe pod: a drain step must pin one to every worker it may remove before it starts")
	}
	if fate == nil {
		return errors.New("no fate function: pass the Fate method of the disruption recorder that ran through the step")
	}
	if wantRemoved < 1 || wantRemoved > len(pins) {
		return fmt.Errorf("wantRemoved is %d; it must be between 1 and the %d pinned workers: a drain step removes at "+
			"least one of the workers it pinned, and no others", wantRemoved, len(pins))
	}
	uids := make(map[types.UID]string, len(pins))
	workers := make(map[string]string, len(pins))
	for i, pin := range pins {
		switch {
		case pin.Name == "":
			return fmt.Errorf("pinned probe pod %d has no name", i)
		case pin.UID == "":
			return fmt.Errorf("pinned probe pod %s has no UID: take it from the pod once it runs", pin.Name)
		case pin.Node == "":
			return fmt.Errorf("pinned probe pod %s has no Node", pin.Name)
		case pin.ProviderID == "":
			return fmt.Errorf("pinned probe pod %s has no provider ID: take spec.providerID of Node %s", pin.Name, pin.Node)
		}
		if other, dup := uids[pin.UID]; dup {
			return fmt.Errorf("pinned probe pods %s and %s have the same UID %s", other, pin.Name, pin.UID)
		}
		if other, dup := workers[pin.ProviderID]; dup {
			return fmt.Errorf("pinned probe pods %s and %s are on the same worker %s; pin one pod per worker, so that "+
				"each removed worker counts once", other, pin.Name, pin.ProviderID)
		}
		uids[pin.UID], workers[pin.ProviderID] = pin.Name, pin.Name
	}
	return nil
}
