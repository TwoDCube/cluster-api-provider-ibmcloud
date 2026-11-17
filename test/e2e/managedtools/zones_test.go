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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/gomega"
)

// zoneNode returns a Node in zone (no zone label when zone is "") with the given Ready condition status; an empty
// ready leaves the Node without a Ready condition.
func zoneNode(name, zone string, ready corev1.ConditionStatus) corev1.Node {
	node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
	if zone != "" {
		node.Labels[corev1.LabelTopologyZone] = zone
	}
	if ready != "" {
		node.Status.Conditions = []corev1.NodeCondition{
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			{Type: corev1.NodeReady, Status: ready},
		}
	}
	return node
}

func TestZonesWithTooFewReadyNodes(t *testing.T) {
	const (
		ready    = corev1.ConditionTrue
		notReady = corev1.ConditionFalse
		unknown  = corev1.ConditionUnknown
	)
	testCases := []struct {
		name    string
		nodes   []corev1.Node
		minimum int
		want    []string
		wantErr string
	}{
		{name: "3 zones x 2 Ready workers", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("a2", "eu-de-1", ready),
			zoneNode("b1", "eu-de-2", ready), zoneNode("b2", "eu-de-2", ready),
			zoneNode("c1", "eu-de-3", ready), zoneNode("c2", "eu-de-3", ready),
		}},
		{name: "a zone with more workers than the minimum", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("a2", "eu-de-1", ready), zoneNode("a3", "eu-de-1", ready),
		}},
		{name: "1 worker per zone, sorted by zone", minimum: 2, nodes: []corev1.Node{
			zoneNode("c1", "eu-de-3", ready), zoneNode("a1", "eu-de-1", ready), zoneNode("b1", "eu-de-2", ready),
		}, want: []string{
			"zone eu-de-1 has 1 Ready of 1 Nodes, fewer than 2",
			"zone eu-de-2 has 1 Ready of 1 Nodes, fewer than 2",
			"zone eu-de-3 has 1 Ready of 1 Nodes, fewer than 2",
		}},
		{name: "only the thin zone is reported", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("a2", "eu-de-1", ready), zoneNode("b1", "eu-de-2", ready),
		}, want: []string{"zone eu-de-2 has 1 Ready of 1 Nodes, fewer than 2"}},
		{name: "a NotReady Node does not count", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("a2", "eu-de-1", notReady),
		}, want: []string{"zone eu-de-1 has 1 Ready of 2 Nodes, fewer than 2"}},
		{name: "a Ready condition of Unknown does not count", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("a2", "eu-de-1", unknown),
		}, want: []string{"zone eu-de-1 has 1 Ready of 2 Nodes, fewer than 2"}},
		{name: "a Node without a Ready condition does not count", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("a2", "eu-de-1", ""),
		}, want: []string{"zone eu-de-1 has 1 Ready of 2 Nodes, fewer than 2"}},
		{name: "a zone whose Nodes are all NotReady is reported", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", notReady), zoneNode("a2", "eu-de-1", notReady),
		}, want: []string{"zone eu-de-1 has 0 Ready of 2 Nodes, fewer than 2"}},
		{name: "no Nodes: no zone holds workers", minimum: 2, nodes: nil},
		{name: "minimum 1", minimum: 1, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("b1", "eu-de-2", notReady),
		}, want: []string{"zone eu-de-2 has 0 Ready of 1 Nodes, fewer than 1"}},
		{name: "Node without a zone label", minimum: 2, nodes: []corev1.Node{
			zoneNode("a1", "eu-de-1", ready), zoneNode("10.243.0.12", "", ready),
		}, wantErr: "the zone of Node 10.243.0.12 is unknown: it has no topology.kubernetes.io/zone label"},
		{name: "minimum 0", minimum: 0, nodes: []corev1.Node{zoneNode("a1", "eu-de-1", ready)},
			wantErr: "the minimum of Ready Nodes per zone is 0; it must be at least 1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ZonesWithTooFewReadyNodes(tc.nodes, tc.minimum)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}
