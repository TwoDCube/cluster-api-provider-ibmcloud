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

// Package managed holds the controllers of the IBM Cloud managed Kubernetes kinds (IKS and ROKS on VPC Gen2):
// IBMCloudManagedCluster, IBMCloudManagedControlPlane and IBMCloudManagedMachinePool.
//
// # Why this package exists
//
// The controllers are the Cluster API side of the feature: they find the owning objects, honour pause and deletion,
// own the patch of each object, and turn the outcome of a scope step into a requeue. The cloud logic lives in
// pkg/cloud/scope/managed, so each controller reads as the list of the steps it runs. Every controller runs only with
// the ManagedKubernetes feature gate on; cmd/main.go registers them in its gated block.
//
// # Conventions
//
//   - Skeleton in Cluster API's KubeadmControlPlane order: get the object, find its owner (a missing owner returns
//     and waits for the watch), add a finalizer only after the owner is known, check pause before any deletion
//     handling, create the patch helper, and only then build the scope and run reconcileNormal or reconcileDelete. So
//     an object no owner references never gets a finalizer and never causes an IBM call, and a paused object causes
//     none either.
//   - Every dependency is explicit: the reconcilers receive the client, the credentials.Resolver, the
//     scope.ClientBuilder and the other shared objects from cmd/main.go and hand them to the scopes through
//     ScopeParams. Nothing here reads the environment or a package-level variable: the credentials of a call come
//     only from the identity resolved for its Cluster, and every shared object is passed in.
//   - Scope steps return outcomes; requeue.go turns them into results with its named, jittered intervals, so
//     expected waits never surface as errors.
package managed
