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
	"fmt"
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

// Binding: which IBM cluster a control plane stands for (DecideBinding).

// createdDateLayout is the layout of IBM's createdDate. Observed on IBM Cloud: "2026-11-16T03:05:33+0000", a numeric
// zone without a colon, which time.RFC3339 does not accept.
const createdDateLayout = "2006-01-02T15:04:05-0700"

// CreatedDateLayouts returns the layouts that parse IBM's cluster createdDate, in the order ParseCreatedDate tries
// them: RFC 3339 first, in case IBM switches to it, then the layout IBM returned in the probes. Observed on IBM Cloud
// (see createdDateLayout). The read-after-create lag that the binding windows below allow for is not yet verified
// against IBM Cloud.
func CreatedDateLayouts() []string {
	return []string{time.RFC3339, createdDateLayout}
}

// ParseCreatedDate parses IBM's createdDate with CreatedDateLayouts. Observed on IBM Cloud (see createdDateLayout).
// The binding rules treat a cluster whose createdDate does not parse as one CAPIBM did not create (a name conflict),
// so the error names the value and the accepted forms.
//
// Parameters:
//   - createdDate: kubernetesservice.Cluster.CreatedDate as IBM returned it.
func ParseCreatedDate(createdDate string) (time.Time, error) {
	for _, layout := range CreatedDateLayouts() {
		if parsed, err := time.Parse(layout, createdDate); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("IBM createdDate %q has an unknown layout; known layouts are RFC 3339 "+
		"(2026-11-16T03:05:33Z) and %q (2026-11-16T03:05:33+0000)", createdDate, createdDateLayout)
}

// CreatedDateSkew is how much earlier than CAPIBM's first create attempt of unknown outcome IBM's createdDate may be
// for the cluster to still count as that attempt's (CAPIBM's own create whose response was lost): IBM's clock and the
// controller's may differ. Not yet verified against IBM Cloud: default 2 minutes; fallback: a wider window once the
// skew is measured.
const CreatedDateSkew = 2 * time.Minute

// CreatedDateSlack is how much later than CAPIBM's last create attempt of unknown outcome IBM's createdDate may be
// (the same window as CreatedDateSkew): IBM stamps the cluster after it accepted the request.
// Not yet verified against IBM Cloud: default 10 minutes; fallback: adjust once the lag is measured.
const CreatedDateSlack = 10 * time.Minute

// CreateGracePeriod is how long after a create attempt of unknown outcome a 404 still means "IBM does not list it yet"
// rather than "it does not exist", for a lookup by ID and by name alike. Not yet verified against IBM Cloud: default 5
// minutes; fallback: longer once the read-after-create lag is measured.
const CreateGracePeriod = 5 * time.Minute

// DeleteLookupGracePeriod replaces CreateGracePeriod while a control plane with a create intent is deleted and the
// lookup by name finds nothing: the finalizer stays this long, so that a cluster IBM lists late is still found and
// deleted. Not yet verified against IBM Cloud: default 30 minutes; fallback: longer.
const DeleteLookupGracePeriod = 30 * time.Minute

// AdoptByEndpointMatch lets DecideBinding adopt a same-named cluster whose master URL matches
// spec.controlPlaneEndpoint, still only after the ownership proof. Not yet verified against IBM Cloud: default false,
// because it is open whether a master URL is unique per cluster and stable over master updates (the two clusters of
// the live probes had distinct URLs); switch to true once both are confirmed.
const AdoptByEndpointMatch = false
