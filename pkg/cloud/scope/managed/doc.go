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

// Package managed holds the cloud logic of the IBM Cloud managed Kubernetes kinds (IKS and ROKS on VPC Gen2):
// IBMCloudManagedCluster, IBMCloudManagedControlPlane and IBMCloudManagedMachinePool.
//
// # Why this package exists
//
// The managed controllers orchestrate; the decisions and the IBM calls live here, in scopes and in pure functions.
// Keeping them out of the controllers lets each decision be tested on its own, with table tests for the pure functions
// and gomock fakes of the IBM services for the scopes, and keeps the controllers short enough to read as a list of
// steps. The VPC and PowerVS scopes are not reused: they read process-wide credentials and configuration, which the
// managed kinds never do.
//
// # Conventions
//
//   - Every live-only fact is in one file. assumptions.go holds each fact about IBM's API that only a live probe can
//     settle: a confirmed fact is a constant or a tiny function whose comment says it was observed on IBM Cloud, an
//     open one is a named decision point whose comment says it is not yet verified against IBM Cloud and names its
//     fallback, and the Cluster API parity values cite the Cluster API line. A test parses the file and fails when a
//     declaration cites none of them. Switching a decision point after a live check is a one-line change plus its
//     test.
//   - Decisions are pure. A decision function takes everything it decides on as arguments, reads no clock and makes
//     no call, so a controller can explain every IBM call it makes by the inputs it observed. Invalid inputs are
//     programming errors and are returned as errors that name the field to fix.
//   - Dependencies are explicit. A scope receives every client, clock and cache it uses from its caller; nothing here
//     reads the environment, a global configuration or a package-level variable, and no package-level variable holds
//     per-cluster or per-credential data. Lists of live facts are therefore functions that return a fresh copy, not
//     package-level slices that a caller could modify. IBM clients are built for an explicitly resolved IBM Cloud IAM
//     credential (the identity) of the Cluster's IBMCloudManagedCluster, lazily, by the first step that needs IBM, so
//     that a scope constructor makes no IBM call and a resolution failure is persisted as a condition. "Identity" in
//     this package means those IAM credentials only; which IBM cluster a control plane stands for is its binding.
//   - Steps report outcomes, not errors, for expected waits. A scope step returns Done, NotDone with an interval and a
//     condition reason, Transient for errors that controller-runtime should retry with backoff, or Terminal for
//     conditions that need a human. Condition messages never carry IBM free text, and status is written only when it
//     changes, so that an unchanged answer from IBM causes no status write and no further reconcile.
//   - Worker removals are serialized per Cluster. A replace, a scale-down and a pool deletion each hold the Cluster's
//     disruption token, a Lease, from the first cordon until the removal settled (DisruptionToken), and advance one
//     step per reconcile as DecideDisruptionStep decides it from what the pool controller observed; CheckDisruption
//     holds the cluster-wide gates. Their tests check over generated inputs that two disruptions never overlap, that
//     no removal takes the cluster below IBM's minimum, and that IBM is never asked to remove a worker whose Node
//     still runs evictable pods, unless the drain time limit expired or the drain is excluded.
package managed
