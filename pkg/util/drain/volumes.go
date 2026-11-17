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

// Attribution: the volume-detach check in this file is adapted from Cluster API v1.14.0,
// core/reconcilers/machine/machine_controller.go (shouldWaitForNodeVolumes and its helpers, lines 956-1026 and
// 1181-1420; Copyright The Kubernetes Authors, Apache License 2.0,
// https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.0/core/reconcilers/machine/machine_controller.go).
// Changes: the skipped pods come from this package's filters instead of a Machine drain, VolumeAttachments are always
// considered (MachineWaitForVolumeDetachConsiderVolumeAttachments is GA and on in v1.14.0), and the message has no
// start time because the caller owns it.

package drain

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// listPageSize is the page size of the VolumeAttachment and PersistentVolume lists (Cluster API
	// machine_controller.go:1247, 1364).
	listPageSize = 100

	// maxVolumesPerLine is how many names each line of the volume message shows before "and N more" (Cluster API
	// util/log StringListToString shows five).
	maxVolumesPerLine = 5
)

// ErrVolumeAttachmentWithoutPersistentVolume is returned (wrapped) by VolumesDetached when a VolumeAttachment attached
// to the Node has no spec.source.persistentVolumeName, as for an inline volume under CSI migration. The drain cannot
// tell which claim such a volume belongs to, so, like Cluster API, it reports an error instead of guessing whether the
// volume may stay attached. The error repeats until the volume is detached or the caller stops waiting, and how the
// caller's users stop the wait (an annotation, a time limit) is the caller's setting, which this package does not
// know: the caller matches the error with errors.Is and adds that remedy in its own terms.
var ErrVolumeAttachmentWithoutPersistentVolume = errors.New("a VolumeAttachment attached to the Node names no PersistentVolume")

// VolumesDetached reports whether the Node has no volume left attached that the drain has to wait for, so the caller
// may ask IBM to remove the worker without risking file-system corruption (IBM documents that fsck corruption
// typically follows node reloads or updates right after a drain that did not verify that the volumes are detached).
// A volume counts as attached while it is listed in Node.status.volumesAttached or a VolumeAttachment with
// spec.nodeName equal to the Node is attached.
// Volumes of pods the drain deliberately skips (DaemonSet, static, cluster.x-k8s.io/drain=skip) are ignored, because
// those pods stay until the server goes. On an unreachable Node the wait is skipped, since nothing can detach there
// (Cluster API machine_controller.go:978-984).
//
// How long IBM VPC Block storage takes to detach has not been measured on IBM Cloud yet; the caller bounds the wait
// with nodeVolumeDetachTimeoutSeconds, unset meaning no limit.
//
// Parameters:
//   - ctx: carries the logger and the deadline of the API calls.
//   - node: the Node as read from the workload cluster (its status.volumesAttached is used as passed).
//
// It returns true and "" when nothing is left to wait for; otherwise false and a deterministic message listing the
// PersistentVolumeClaims and volumes still attached, sorted and truncated. It returns an error when the inputs are
// invalid, a list fails, or a VolumeAttachment does not name a PersistentVolume (as Cluster API does, rather than
// guessing); that last error wraps ErrVolumeAttachmentWithoutPersistentVolume.
func (h *Helper) VolumesDetached(ctx context.Context, node *corev1.Node) (bool, string, error) {
	if err := h.validate(); err != nil {
		return false, "", err
	}
	if err := validateNode("VolumesDetached", node); err != nil {
		return false, "", err
	}
	if IsNodeUnreachable(node) {
		return true, "", nil
	}

	nodeVolumeNames := sets.New[string]()
	for _, attached := range node.Status.VolumesAttached {
		nodeVolumeNames.Insert(string(attached.Name))
	}
	attachmentPVNames, err := h.attachedPersistentVolumeNames(ctx, node.Name)
	if err != nil {
		return false, "", fmt.Errorf("drain: Node %s: %w", node.Name, err)
	}
	if nodeVolumeNames.Len() == 0 && attachmentPVNames.Len() == 0 {
		return true, "", nil
	}

	ignoredClaims, err := h.claimsOfSkippedPods(ctx, node.Name)
	if err != nil {
		return false, "", fmt.Errorf("drain: Node %s: %w", node.Name, err)
	}
	waiting, err := h.volumesWaitingForDetach(ctx, nodeVolumeNames, attachmentPVNames, ignoredClaims)
	if err != nil {
		return false, "", fmt.Errorf("drain: Node %s: %w", node.Name, err)
	}
	if waiting.isEmpty() {
		return true, "", nil
	}
	return false, waiting.message(), nil
}

// attachedPersistentVolumeNames lists every VolumeAttachment, 100 per page, and returns the PersistentVolume names
// of those attached to nodeName (Cluster API machine_controller.go:1181-1270). VolumeAttachment has no nodeName field
// selector, so the filter runs here.
func (h *Helper) attachedPersistentVolumeNames(ctx context.Context, nodeName string) (sets.Set[string], error) {
	names := sets.New[string]()
	list := &storagev1.VolumeAttachmentList{}
	for {
		if err := h.Client.List(ctx, list, client.Limit(listPageSize), client.Continue(list.Continue)); err != nil {
			return nil, fmt.Errorf("list VolumeAttachments: %w", err)
		}
		for i := range list.Items {
			attachment := &list.Items[i]
			if !attachment.Status.Attached || attachment.Spec.NodeName != nodeName {
				continue
			}
			if attachment.Spec.Source.PersistentVolumeName == nil {
				// The remedy is left to the caller (ErrVolumeAttachmentWithoutPersistentVolume): only it knows where its
				// users skip or bound the wait.
				return nil, fmt.Errorf("%w: VolumeAttachment %s has no spec.source.persistentVolumeName, so the drain "+
					"cannot tell which claim its volume belongs to", ErrVolumeAttachmentWithoutPersistentVolume, attachment.Name)
			}
			names.Insert(*attachment.Spec.Source.PersistentVolumeName)
		}
		if list.Continue == "" {
			return names, nil
		}
	}
}

// claimsOfSkippedPods returns "namespace/name" of every PersistentVolumeClaim used by a pod the drain skips on
// nodeName (Cluster API machine_controller.go:1210-1238).
func (h *Helper) claimsOfSkippedPods(ctx context.Context, nodeName string) (sets.Set[string], error) {
	// The detach wait never runs on an unreachable Node, so the pods are classified as on a reachable one.
	pods, err := h.classifyPodsOnNode(ctx, nodeName, false)
	if err != nil {
		return nil, err
	}
	claims := sets.New[string]()
	for _, cp := range pods {
		if cp.behavior != behaviorSkip {
			continue
		}
		for _, volume := range cp.pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			claims.Insert(types.NamespacedName{Namespace: cp.pod.Namespace, Name: volume.PersistentVolumeClaim.ClaimName}.String())
		}
	}
	return claims, nil
}

// waitingVolumes is what VolumesDetached still waits for, by kind of evidence.
type waitingVolumes struct {
	// claims are attached PersistentVolumeClaims ("namespace/name") not used by a skipped pod.
	claims []string
	// volumesWithoutClaim are attached PersistentVolumes without a claimRef to a PersistentVolumeClaim.
	volumesWithoutClaim []string
	// nodeVolumesWithoutPV are Node.status.volumesAttached names that match no PersistentVolume.
	nodeVolumesWithoutPV []string
	// attachmentsWithoutPV are VolumeAttachment PersistentVolume names that match no PersistentVolume.
	attachmentsWithoutPV []string
}

// isEmpty reports whether nothing is left to wait for.
func (w waitingVolumes) isEmpty() bool {
	return len(w.claims) == 0 && len(w.volumesWithoutClaim) == 0 &&
		len(w.nodeVolumesWithoutPV) == 0 && len(w.attachmentsWithoutPV) == 0
}

// message renders the wait with Cluster API's wording (machine_controller.go:1318-1346): one line per kind, names
// sorted and truncated, so the same state always gives the same bytes.
func (w waitingVolumes) message() string {
	lines := []string{"Waiting for Node volumes to be detached:"}
	add := func(title string, names []string) {
		if len(names) == 0 {
			return
		}
		sorted := slices.Clone(names)
		slices.Sort(sorted)
		lines = append(lines, fmt.Sprintf("* %s: %s", title, namesPhrase(sorted, maxVolumesPerLine)))
	}
	add("PersistentVolumeClaims", w.claims)
	add("PersistentVolumes without a .spec.claimRef to a PersistentVolumeClaim", w.volumesWithoutClaim)
	add("Node with .status.volumesAttached entries not matching a PersistentVolume", w.nodeVolumesWithoutPV)
	add("VolumeAttachment with .spec.source.persistentVolumeName not matching a PersistentVolume", w.attachmentsWithoutPV)
	return strings.Join(lines, "\n")
}

// volumesWaitingForDetach lists every PersistentVolume, 100 per page, matches it against the attached volume names
// from the Node status (CSI "kubernetes.io/csi/<driver>^<handle>") and from the VolumeAttachments, and returns what is
// still attached, minus the claims of skipped pods (Cluster API machine_controller.go:1351-1420).
func (h *Helper) volumesWaitingForDetach(ctx context.Context, nodeVolumeNames, attachmentPVNames, ignoredClaims sets.Set[string]) (waitingVolumes, error) {
	claims := sets.New[string]()
	withoutClaim := []string{}
	foundNodeVolumes := sets.New[string]()
	foundAttachmentPVs := sets.New[string]()

	list := &corev1.PersistentVolumeList{}
	for {
		if err := h.Client.List(ctx, list, client.Limit(listPageSize), client.Continue(list.Continue)); err != nil {
			return waitingVolumes{}, fmt.Errorf("list PersistentVolumes: %w", err)
		}
		for i := range list.Items {
			pv := &list.Items[i]
			found := false
			if pv.Spec.CSI != nil {
				name := fmt.Sprintf("kubernetes.io/csi/%s^%s", pv.Spec.CSI.Driver, pv.Spec.CSI.VolumeHandle)
				if nodeVolumeNames.Has(name) {
					foundNodeVolumes.Insert(name)
					found = true
				}
			}
			if attachmentPVNames.Has(pv.Name) {
				foundAttachmentPVs.Insert(pv.Name)
				found = true
			}
			if !found {
				continue
			}
			// An attached volume should always be bound; one that is not, or is bound to something other than a claim,
			// is reported by its own name.
			if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Kind != "PersistentVolumeClaim" {
				withoutClaim = append(withoutClaim, pv.Name)
				continue
			}
			claims.Insert(types.NamespacedName{Namespace: pv.Spec.ClaimRef.Namespace, Name: pv.Spec.ClaimRef.Name}.String())
		}
		if list.Continue == "" {
			break
		}
	}

	return waitingVolumes{
		claims:               sets.List(claims.Difference(ignoredClaims)),
		volumesWithoutClaim:  withoutClaim,
		nodeVolumesWithoutPV: sets.List(nodeVolumeNames.Difference(foundNodeVolumes)),
		attachmentsWithoutPV: sets.List(attachmentPVNames.Difference(foundAttachmentPVs)),
	}, nil
}
