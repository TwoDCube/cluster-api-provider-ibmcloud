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
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
)

// ZonesWithTooFewReadyNodes returns one sentence for every zone that holds Nodes but fewer than minimum Ready ones,
// sorted by zone, or nil when every zone that holds Nodes has enough. A zone without any Node is never reported:
// nothing in it can be drained. The IKS lane checks it before every step that drains workers, because IBM runs each
// zone's Ingress ALB with 2 replicas, at most one per worker, and a PodDisruptionBudget that keeps one of them
// available: in a zone with a single worker only one replica runs, no drain may evict it, and the step would wait
// for its whole timeout in DrainBlocked instead of failing with the zone's name.
//
// Parameters:
//   - nodes are every Node of the workload cluster at one moment. Each must carry the zone label
//     (corev1.LabelTopologyZone), which IBM's cloud provider sets on every IKS and ROKS worker. A Node counts as Ready
//     when its Ready condition is True.
//   - minimum is the fewest Ready Nodes a zone that holds Nodes needs; it must be at least 1.
//
// It returns an error naming the Node when a Node has no zone label, and an error when minimum is below 1.
func ZonesWithTooFewReadyNodes(nodes []corev1.Node, minimum int) ([]string, error) {
	if minimum < 1 {
		return nil, fmt.Errorf("the minimum of Ready Nodes per zone is %d; it must be at least 1", minimum)
	}
	type zoneCount struct{ nodes, ready int }
	zones := map[string]*zoneCount{}
	for i := range nodes {
		zone := nodes[i].Labels[corev1.LabelTopologyZone]
		if zone == "" {
			return nil, fmt.Errorf("the zone of Node %s is unknown: it has no %s label, which IBM sets on every worker",
				nodes[i].Name, corev1.LabelTopologyZone)
		}
		count, ok := zones[zone]
		if !ok {
			count = &zoneCount{}
			zones[zone] = count
		}
		count.nodes++
		if isNodeReady(&nodes[i]) {
			count.ready++
		}
	}
	var short []string
	for _, zone := range slices.Sorted(maps.Keys(zones)) {
		if count := zones[zone]; count.ready < minimum {
			short = append(short, fmt.Sprintf("zone %s has %d Ready of %d Nodes, fewer than %d", zone, count.ready,
				count.nodes, minimum))
		}
	}
	return short, nil
}

// isNodeReady reports whether the Node's Ready condition is True; a Node without the condition is not Ready.
//
// Parameters:
//   - node is the Node.
func isNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
