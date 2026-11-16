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

// Package drain cordons and drains Nodes of an IBM Cloud managed (IKS or ROKS) workload cluster before CAPIBM asks IBM
// to remove the workers behind them, with the semantics of the Cluster API Machine drain.
//
// # Why this package exists
//
// Cluster API drains a Node only while it deletes a Machine. CAPIBM's managed worker pools are MachinePools without
// MachinePool Machines, so Cluster API never cordons, evicts or waits for volume detach on them. IBM's own drain on
// replaceWorker is not PDB-safe either: on IKS 1.36 it was observed to evict what the PodDisruptionBudgets allowed and
// to kill the remaining pods about 60 seconds later. CAPIBM therefore drains each worker itself before every IBM call
// that removes it (replaceWorker, resizeWorkerPool down, removeWorkerPool).
//
// The Cluster API drain package (core/reconcilers/machine/drain) is not reused because /core carries no stability
// guarantee and its API needs a *Machine; k8s.io/kubectl/pkg/drain is not reused because it blocks until the drain
// ends and needs a clientset. This package is instead a small library on the controller-runtime client that keeps
// Cluster API v1.14.0's filters, eviction call and messages. The adapted files (drain.go, filters.go, result.go,
// volumes.go) carry an attribution to Cluster API v1.14.0.
//
// # Conventions
//
//   - Non-blocking. Every method does a bounded amount of work and returns; the caller (the IBMCloudManagedMachinePool
//     controller) requeues every 20 seconds until DrainResult.Done, like Cluster API's drainRetryInterval. Evictions
//     stop after Helper.EvictionBudget per call (Cluster API: 10 seconds).
//   - Every input is explicit. The workload-cluster client, the clock and the Node annotation prefix are fields of
//     Helper; nothing is read from the environment or from globals, and the package imports no CAPIBM API type (the
//     caller passes infrav1.NodeAnnotationPrefix), so it can be tested with a fake client and a fake clock.
//   - Pods are only ever evicted through the policy/v1 Eviction subresource, which enforces PodDisruptionBudgets and
//     gives each pod its own terminationGracePeriodSeconds. The package never deletes a pod and never deletes a Node.
//     A drain is done only when the pods are gone, not when their eviction was accepted. Once the user's drain time
//     limit has passed (DrainTimeoutExceeded), the caller stops calling Drain and lists what is left with
//     RemainingPods, which reads only.
//   - Cordon, Uncordon, Drain, RemainingPods, VolumesDetached and DrainTimeoutExceeded refuse a nil Node and a Node
//     without a name: the pod and volume lookups match on the name, and an empty name would match every unscheduled
//     pod of the workload cluster.
//   - State lives on the Node. Cordon records the drain start, the owning pool and exactly what it changed in
//     annotations, so a controller restart or a clusterctl move resumes with the original drain start, and Uncordon
//     reverts only CAPIBM's changes (a Node a human cordoned stays cordoned).
//   - IBM behaviour not yet verified against IBM Cloud stays a one-line switch in the caller: whether Cordon adds the
//     node.kubernetes.io/exclude-from-external-load-balancers label is CordonOptions.ExcludeFromLoadBalancers, fed
//     from assumptions.ExcludeFromLoadBalancersDuringDrain (whether the IBM VPC load balancer honours the label is not
//     yet verified). How long IBM VPC Block storage needs to detach after an eviction has not been measured yet; until
//     it is, VolumesDetached waits as long as the caller lets it (nodeVolumeDetachTimeoutSeconds unset means no limit).
//
// MachineDrainRule support (drain order and per-rule behaviour) is not implemented yet; until it is, every pod has
// drain order 0 and only the cluster.x-k8s.io/drain pod label changes the behaviour of a single pod.
package drain
