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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Node marker names. The annotation key is Helper.AnnotationPrefix followed by the marker name, for example
// "ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/drain-start-time". The markers live on the Node, not in
// CAPIBM's status, so they survive a controller restart and a clusterctl move.
const (
	// MarkerDrainStartTime holds the RFC 3339 time of the first cordon. The drain time limit is measured from it, so it
	// never resets across restarts or moves.
	MarkerDrainStartTime = "drain-start-time"
	// MarkerDrainOwner holds "<namespace>/<IBMCloudManagedMachinePool name>" of the pool that drains the Node. Other pools
	// defer to it before taking the disruption token.
	MarkerDrainOwner = "drain-owner"
	// MarkerCordoned is "true" only when CAPIBM flipped spec.unschedulable from false to true; Uncordon reverts the
	// cordon only then.
	MarkerCordoned = "cordoned"
	// MarkerExcludedFromLB is "true" only when CAPIBM added the node.kubernetes.io/exclude-from-external-load-balancers
	// label; Uncordon removes the label only then.
	MarkerExcludedFromLB = "excluded-from-lb"
)

// markerTrue is the value of the MarkerCordoned and MarkerExcludedFromLB annotations.
const markerTrue = "true"

// ErrDrainOwnerConflict is returned (wrapped) by Cordon when the Node's drain-owner marker names a different pool.
// CAPIBM never takes over another pool's drain; the operator removes the stale marker if that pool is gone.
var ErrDrainOwnerConflict = errors.New("the Node is drained by another pool")

// CordonOptions are the per-call inputs of Cordon.
type CordonOptions struct {
	// Owner is the drain-owner marker value, "<namespace>/<IBMCloudManagedMachinePool name>". Required.
	Owner string

	// ExcludeFromLoadBalancers adds the label node.kubernetes.io/exclude-from-external-load-balancers=true, because a
	// cordon alone does not take a Node out of cloud load-balancer pools (k8s cloud-provider service controller
	// controller.go:994-1034; cloud-provider-ibm builds VPC LB pools from that node list). The caller passes
	// assumptions.ExcludeFromLoadBalancersDuringDrain (whether the IBM VPC load balancer honours the label is not yet
	// verified against IBM Cloud; false switches the label off).
	ExcludeFromLoadBalancers bool
}

// validate checks the options and names the field to fix.
func (o CordonOptions) validate() error {
	namespace, name, found := strings.Cut(o.Owner, "/")
	if !found || namespace == "" || name == "" || strings.Contains(name, "/") {
		return fmt.Errorf("drain: CordonOptions.Owner %q must be \"<namespace>/<IBMCloudManagedMachinePool name>\"", o.Owner)
	}
	return nil
}

// MarkerKey returns the annotation key of a Node marker: AnnotationPrefix followed by marker.
//
// Parameters:
//   - marker: one of MarkerDrainStartTime, MarkerDrainOwner, MarkerCordoned, MarkerExcludedFromLB.
func (h *Helper) MarkerKey(marker string) string {
	return h.AnnotationPrefix + marker
}

// Cordon marks the Node unschedulable and records CAPIBM's drain markers on it in one merge patch. It is idempotent:
// when everything is already in place it sends nothing, and an existing drain-start-time is kept so the time limit
// never resets. It records MarkerCordoned only if it flipped spec.unschedulable itself and MarkerExcludedFromLB only
// if it added the load-balancer exclusion label, so Uncordon never undoes a human's change.
//
// The patch carries the Node's resourceVersion (optimistic lock): if a human cordons the Node between the read and
// the patch, the patch fails with a conflict instead of recording a cordon CAPIBM did not make; the caller retries
// on the next reconcile with a fresh Node.
//
// Parameters:
//   - ctx: carries the logger and the deadline of the API call.
//   - node: the Node as read from the workload cluster. On success it is updated to the patched object; on error it is
//     left as passed.
//   - opts: the drain owner and whether to exclude the Node from load balancers.
//
// It returns an error wrapping ErrDrainOwnerConflict when another pool's drain-owner marker is on the Node.
func (h *Helper) Cordon(ctx context.Context, node *corev1.Node, opts CordonOptions) error {
	if err := h.validate(); err != nil {
		return err
	}
	if err := validateNode("Cordon", node); err != nil {
		return err
	}
	if err := opts.validate(); err != nil {
		return err
	}
	ownerKey := h.MarkerKey(MarkerDrainOwner)
	if owner, found := node.Annotations[ownerKey]; found && owner != opts.Owner {
		return fmt.Errorf("drain: Cordon Node %s for %s: %w %q (annotation %s); if that pool no longer exists, "+
			"remove the annotation", node.Name, opts.Owner, ErrDrainOwnerConflict, owner, ownerKey)
	}

	updated := node.DeepCopy()
	if !h.applyCordon(ctx, updated, opts) {
		return nil
	}
	if err := h.patchNode(ctx, node, updated); err != nil {
		return fmt.Errorf("drain: cordon Node %s: %w", node.Name, err)
	}
	ctrl.LoggerFrom(ctx).Info("Cordoned Node for drain", "Node", node.Name, "owner", opts.Owner)
	return nil
}

// applyCordon sets the cordon, the markers and the optional label on node and reports whether anything changed.
func (h *Helper) applyCordon(ctx context.Context, node *corev1.Node, opts CordonOptions) bool {
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	changed := false
	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		node.Annotations[h.MarkerKey(MarkerCordoned)] = markerTrue
		changed = true
	}
	if _, found, err := h.DrainStartTime(node); !found || err != nil {
		// A missing marker starts the clock. An unparsable one (edited by hand) is replaced: restarting the clock can
		// only make the drain wait longer, never cut it short.
		if err != nil {
			ctrl.LoggerFrom(ctx).Info("Replacing an invalid drain start marker", "Node", node.Name, "err", err.Error())
		}
		node.Annotations[h.MarkerKey(MarkerDrainStartTime)] = h.Clock.Now().UTC().Format(time.RFC3339)
		changed = true
	}
	if node.Annotations[h.MarkerKey(MarkerDrainOwner)] != opts.Owner {
		node.Annotations[h.MarkerKey(MarkerDrainOwner)] = opts.Owner
		changed = true
	}
	if opts.ExcludeFromLoadBalancers {
		// The service controller checks only that the label exists, so a label someone else set (any value) already
		// does the job and is left alone.
		if _, found := node.Labels[corev1.LabelNodeExcludeBalancers]; !found {
			if node.Labels == nil {
				node.Labels = map[string]string{}
			}
			node.Labels[corev1.LabelNodeExcludeBalancers] = markerTrue
			node.Annotations[h.MarkerKey(MarkerExcludedFromLB)] = markerTrue
			changed = true
		}
	}
	return changed
}

// Uncordon reverts what Cordon changed, as recorded in the Node markers, and removes every CAPIBM drain marker: the
// cordon only if MarkerCordoned is "true", the load-balancer exclusion label only if MarkerExcludedFromLB is "true".
// A Node a human cordoned before the drain stays cordoned. It is idempotent and sends nothing when no marker is
// present. Used when a drain is aborted before the IBM call and for spared scale-down victims.
//
// Like Cordon, the patch carries the Node's resourceVersion, so a concurrent change fails with a conflict that the
// caller retries.
//
// Parameters:
//   - ctx: carries the logger and the deadline of the API call.
//   - node: the Node as read from the workload cluster. On success it is updated to the patched object; on error it is
//     left as passed.
func (h *Helper) Uncordon(ctx context.Context, node *corev1.Node) error {
	if err := h.validate(); err != nil {
		return err
	}
	if err := validateNode("Uncordon", node); err != nil {
		return err
	}

	updated := node.DeepCopy()
	changed := false
	if value, found := updated.Annotations[h.MarkerKey(MarkerCordoned)]; found {
		if value == markerTrue {
			updated.Spec.Unschedulable = false
		}
		changed = true
	}
	if value, found := updated.Annotations[h.MarkerKey(MarkerExcludedFromLB)]; found {
		if value == markerTrue {
			delete(updated.Labels, corev1.LabelNodeExcludeBalancers)
		}
		changed = true
	}
	for _, marker := range []string{MarkerCordoned, MarkerExcludedFromLB, MarkerDrainStartTime, MarkerDrainOwner} {
		if _, found := updated.Annotations[h.MarkerKey(marker)]; found {
			delete(updated.Annotations, h.MarkerKey(marker))
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := h.patchNode(ctx, node, updated); err != nil {
		return fmt.Errorf("drain: uncordon Node %s: %w", node.Name, err)
	}
	ctrl.LoggerFrom(ctx).Info("Reverted CAPIBM's drain changes on Node", "Node", node.Name,
		"schedulable", !node.Spec.Unschedulable)
	return nil
}

// patchNode sends a merge patch from original to updated with the original's resourceVersion as a precondition, and
// on success copies the server's answer into original.
func (h *Helper) patchNode(ctx context.Context, original, updated *corev1.Node) error {
	patch := client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})
	if err := h.Client.Patch(ctx, updated, patch); err != nil {
		return err
	}
	*original = *updated
	return nil
}
