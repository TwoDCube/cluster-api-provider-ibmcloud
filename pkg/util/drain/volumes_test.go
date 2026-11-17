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

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// testCSIDriver is the IBM VPC Block CSI driver name; the volume handles below are synthetic.
const testCSIDriver = "vpc.block.csi.ibm.io"

func csiVolumeName(handle string) corev1.UniqueVolumeName {
	return corev1.UniqueVolumeName(fmt.Sprintf("kubernetes.io/csi/%s^%s", testCSIDriver, handle))
}

// withVolumesAttached sets Node.status.volumesAttached to the CSI volumes of the given handles.
func withVolumesAttached(handles ...string) func(*corev1.Node) {
	return func(node *corev1.Node) {
		for _, h := range handles {
			node.Status.VolumesAttached = append(node.Status.VolumesAttached, corev1.AttachedVolume{Name: csiVolumeName(h)})
		}
	}
}

// newPV returns a CSI PersistentVolume bound to namespace/claim (no claimRef when claim is "").
func newPV(name, handle, namespace, claim string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: testCSIDriver, VolumeHandle: handle},
			},
		},
	}
	if claim != "" {
		pv.Spec.ClaimRef = &corev1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: namespace, Name: claim}
	}
	return pv
}

// newAttachment returns a VolumeAttachment of pvName on nodeName.
func newAttachment(name, nodeName, pvName string, attached bool) *storagev1.VolumeAttachment {
	attachment := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: testCSIDriver,
			NodeName: nodeName,
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: attached},
	}
	if pvName != "" {
		attachment.Spec.Source.PersistentVolumeName = ptr.To(pvName)
	}
	return attachment
}

// TestVolumesDetached covers the wait for volume detach: Node status and VolumeAttachments, PVCs of skipped pods
// ignored, the unreachable-Node skip, and the volumes reported without a matching PersistentVolume or claim.
func TestVolumesDetached(t *testing.T) {
	testCases := []struct {
		name         string
		node         *corev1.Node
		objects      []client.Object
		wantDetached bool
		wantMessage  string
	}{
		{
			name:         "nothing attached",
			node:         newNode(testNode),
			wantDetached: true,
		},
		{
			name:         "Node status lists a bound volume",
			node:         newNode(testNode, withVolumesAttached("vol-1")),
			objects:      []client.Object{newPV("pv-1", "vol-1", "data", "db-data-0")},
			wantDetached: false,
			wantMessage:  "Waiting for Node volumes to be detached:\n* PersistentVolumeClaims: data/db-data-0",
		},
		{
			name: "attached VolumeAttachment of this Node",
			node: newNode(testNode),
			objects: []client.Object{
				newPV("pv-1", "vol-1", "data", "db-data-0"),
				newAttachment("csi-1", testNode, "pv-1", true),
			},
			wantDetached: false,
			wantMessage:  "Waiting for Node volumes to be detached:\n* PersistentVolumeClaims: data/db-data-0",
		},
		{
			name: "detached VolumeAttachment and other Nodes' attachments are ignored",
			node: newNode(testNode),
			objects: []client.Object{
				newPV("pv-1", "vol-1", "data", "db-data-0"),
				newAttachment("csi-1", testNode, "pv-1", false),
				newAttachment("csi-2", "10.0.0.12", "pv-1", true),
			},
			wantDetached: true,
		},
		{
			name: "volume of a skipped pod is ignored",
			node: newNode(testNode, withVolumesAttached("vol-1")),
			objects: []client.Object{
				newPV("pv-1", "vol-1", "data", "db-data-0"),
				newPV("pv-unrelated", "vol-9", "data", "other"),
				newPod("data", "db-0", testNode, withPodLabel(clusterv1.PodDrainLabel, "skip"), withEmptyDir, withClaim("db-data-0")),
			},
			wantDetached: true,
		},
		{
			name: "volume of a pod that is still evicted is waited for",
			node: newNode(testNode, withVolumesAttached("vol-1")),
			objects: []client.Object{
				newPV("pv-1", "vol-1", "data", "db-data-0"),
				newPod("data", "db-0", testNode, withClaim("db-data-0")),
			},
			wantDetached: false,
			wantMessage:  "Waiting for Node volumes to be detached:\n* PersistentVolumeClaims: data/db-data-0",
		},
		{
			name:         "unreachable Node skips the wait",
			node:         newNode(testNode, unreachableNode, withVolumesAttached("vol-1")),
			objects:      []client.Object{newPV("pv-1", "vol-1", "data", "db-data-0")},
			wantDetached: true,
		},
		{
			name: "volumes without a PersistentVolume or claim are reported by name",
			node: newNode(testNode, withVolumesAttached("vol-1", "vol-orphan")),
			objects: []client.Object{
				newPV("pv-1", "vol-1", "", ""),
				newAttachment("csi-2", testNode, "pv-gone", true),
			},
			wantDetached: false,
			wantMessage: "Waiting for Node volumes to be detached:\n" +
				"* PersistentVolumes without a .spec.claimRef to a PersistentVolumeClaim: pv-1\n" +
				"* Node with .status.volumesAttached entries not matching a PersistentVolume: kubernetes.io/csi/vpc.block.csi.ibm.io^vol-orphan\n" +
				"* VolumeAttachment with .spec.source.persistentVolumeName not matching a PersistentVolume: pv-gone",
		},
		{
			name: "claims are sorted and truncated",
			node: newNode(testNode, withVolumesAttached("v6", "v5", "v4", "v3", "v2", "v1")),
			objects: []client.Object{
				newPV("pv-1", "v1", "data", "c1"), newPV("pv-2", "v2", "data", "c2"), newPV("pv-3", "v3", "data", "c3"),
				newPV("pv-4", "v4", "data", "c4"), newPV("pv-5", "v5", "data", "c5"), newPV("pv-6", "v6", "data", "c6"),
			},
			wantDetached: false,
			wantMessage: "Waiting for Node volumes to be detached:\n" +
				"* PersistentVolumeClaims: data/c1, data/c2, data/c3, data/c4, data/c5 and 1 more",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			h := newTestHelper(newFakeClient(interceptor.Funcs{}, tc.objects...), clocktesting.NewFakePassiveClock(testNow))

			detached, message, err := h.VolumesDetached(context.Background(), tc.node)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(detached).To(Equal(tc.wantDetached))
			g.Expect(message).To(Equal(tc.wantMessage))
		})
	}
}

func TestVolumesDetachedErrors(t *testing.T) {
	listErr := func(kind string) interceptor.Funcs {
		return interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if fmt.Sprintf("%T", list) == kind {
				return errors.New("apiserver unavailable")
			}
			return c.List(ctx, list, opts...)
		}}
	}
	testCases := []struct {
		name    string
		node    *corev1.Node
		nilNode bool
		objects []client.Object
		funcs   interceptor.Funcs
		wantErr string
		// wantUnnamed is true when the error must match ErrVolumeAttachmentWithoutPersistentVolume, which the caller
		// matches to add its own remedy; every other error must not.
		wantUnnamed bool
	}{
		{name: "nil Node", nilNode: true, wantErr: "VolumesDetached: node is nil"},
		{
			// Without the check a nameless Node matches no VolumeAttachment and would report "detached", a false signal
			// that IBM may now remove the worker.
			name:    "Node without a name",
			node:    newNode(""),
			wantErr: "VolumesDetached: node has no name: pass the Node read from the workload cluster",
		},
		{
			name:    "VolumeAttachment without a PersistentVolume name",
			node:    newNode(testNode),
			objects: []client.Object{newAttachment("csi-1", testNode, "", true)},
			wantErr: "drain: Node 10.0.0.11: a VolumeAttachment attached to the Node names no PersistentVolume: " +
				"VolumeAttachment csi-1 has no spec.source.persistentVolumeName, so the drain cannot tell which claim its volume belongs to",
			wantUnnamed: true,
		},
		{
			// Only an attached VolumeAttachment of this Node holds the drain; a detached one or one of another Node is
			// ignored even without a PersistentVolume name.
			name: "VolumeAttachment without a PersistentVolume name that is detached or elsewhere",
			node: newNode(testNode),
			objects: []client.Object{newAttachment("csi-detached", testNode, "", false),
				newAttachment("csi-other", "10.0.0.99", "", true)},
		},
		{
			name:    "VolumeAttachment list error",
			node:    newNode(testNode),
			funcs:   listErr("*v1.VolumeAttachmentList"),
			wantErr: "drain: Node 10.0.0.11: list VolumeAttachments: apiserver unavailable",
		},
		{
			name:    "PersistentVolume list error",
			node:    newNode(testNode, withVolumesAttached("vol-1")),
			funcs:   listErr("*v1.PersistentVolumeList"),
			wantErr: "drain: Node 10.0.0.11: list PersistentVolumes: apiserver unavailable",
		},
		{
			name:    "pod list error",
			node:    newNode(testNode, withVolumesAttached("vol-1")),
			funcs:   listErr("*v1.PodList"),
			wantErr: "drain: Node 10.0.0.11: list Pods on the Node: apiserver unavailable",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			h := newTestHelper(newFakeClient(tc.funcs, tc.objects...), clocktesting.NewFakePassiveClock(testNow))
			node := tc.node
			if tc.nilNode {
				node = nil
			}
			_, _, err := h.VolumesDetached(context.Background(), node)
			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(errors.Is(err, ErrVolumeAttachmentWithoutPersistentVolume)).To(Equal(tc.wantUnnamed))
		})
	}
}

func TestVolumesDetachedRejectsInvalidHelper(t *testing.T) {
	g := NewWithT(t)
	h := &Helper{Client: newFakeClient(interceptor.Funcs{}), AnnotationPrefix: testPrefix}
	_, _, err := h.VolumesDetached(context.Background(), newNode(testNode))
	g.Expect(err).To(MatchError(ContainSubstring("Helper.Clock is nil")))
}

// TestVolumesDetachedPagesLists proves the VolumeAttachment and PersistentVolume lists are paged by 100 and follow
// the continue token.
func TestVolumesDetachedPagesLists(t *testing.T) {
	g := NewWithT(t)
	limits := map[string][]int64{}
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			lo := client.ListOptions{}
			lo.ApplyOptions(opts)
			kind := fmt.Sprintf("%T", list)
			limits[kind] = append(limits[kind], lo.Limit)
			switch l := list.(type) {
			case *storagev1.VolumeAttachmentList:
				if lo.Continue == "" {
					*l = storagev1.VolumeAttachmentList{Items: []storagev1.VolumeAttachment{*newAttachment("csi-1", testNode, "pv-1", true)}}
					l.Continue = "va-2"
					return nil
				}
				*l = storagev1.VolumeAttachmentList{Items: []storagev1.VolumeAttachment{*newAttachment("csi-2", testNode, "pv-2", true)}}
				return nil
			case *corev1.PersistentVolumeList:
				if lo.Continue == "" {
					*l = corev1.PersistentVolumeList{Items: []corev1.PersistentVolume{*newPV("pv-1", "v1", "data", "c1")}}
					l.Continue = "pv-2"
					return nil
				}
				*l = corev1.PersistentVolumeList{Items: []corev1.PersistentVolume{*newPV("pv-2", "v2", "data", "c2")}}
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	}
	h := newTestHelper(newFakeClient(funcs), clocktesting.NewFakePassiveClock(testNow))

	detached, message, err := h.VolumesDetached(context.Background(), newNode(testNode))
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(detached).To(BeFalse())
	g.Expect(message).To(Equal("Waiting for Node volumes to be detached:\n* PersistentVolumeClaims: data/c1, data/c2"))
	g.Expect(limits["*v1.VolumeAttachmentList"]).To(Equal([]int64{100, 100}))
	g.Expect(limits["*v1.PersistentVolumeList"]).To(Equal([]int64{100, 100}))
}
