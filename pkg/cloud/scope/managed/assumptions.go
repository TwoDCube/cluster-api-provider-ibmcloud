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

package managed

import (
	"time"
)

// This file holds every fact about IBM's API that only a live probe can settle, in one place, so that a reviewer sees
// which IBM behaviour CAPIBM relies on and what changes when IBM behaves differently, together with the Cluster API
// parity values of the drain. Each declaration says where its value comes from, in one of three forms that
// TestAssumptionsCiteTheirSource checks:
//
//   - "Observed on IBM Cloud": confirmed by the live probes of 2026-11-16 against IKS 1.36 and ROKS 4.21 clusters on
//     VPC; the comment states what was observed;
//   - "Not yet verified against IBM Cloud": still open; the comment names the default and the fallback, and switching
//     to the fallback is a one-line change here plus its test;
//   - "CAPI parity": the value Cluster API v1.14.0 uses for the same step; the comment cites the line.
//
// The sections follow the flows that first use them. Lists are functions that return a fresh copy, so that no caller
// can change a fact for the whole process.

// Drain cadence: Cluster API parity, not live checks.

// DrainRetryInterval is how long a drain waits before it evicts again or looks again at the pods that are still
// terminating. CAPI parity: Cluster API v1.14.0 drains a Machine's Node at the same cadence
// (core/reconcilers/machine/machine_controller.go:73, drainRetryInterval).
const DrainRetryInterval = 20 * time.Second

// EvictionBudgetPerReconcile bounds the time one reconcile spends sending evictions (drain.Helper.EvictionBudget); the
// next reconcile continues where it stopped. CAPI parity: Cluster API v1.14.0 sends evictions for at most 10 seconds
// per drain call (core/reconcilers/machine/drain/drain.go:316, evictionTimeout).
const EvictionBudgetPerReconcile = 10 * time.Second

// DrainBlockedEventInterval is the shortest time between two DrainBlocked Warning events of one drain; the Draining
// condition names the blocking pods all the time. CAPI parity: Cluster API v1.14.0 emits no event while a
// PodDisruptionBudget blocks a drain, it only updates the Machine's conditions every drainRetryInterval
// (core/reconcilers/machine/machine_controller.go:930-948). CAPIBM adds the Warning so that a drain blocked for hours
// shows up in events, and bounds it to one per interval so that it does not flood them.
const DrainBlockedEventInterval = 30 * time.Minute
