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
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// This file holds the guards a pool passes before CAPIBM makes any IBM call for it and the size arithmetic between a
// CAPI MachinePool and an IBM worker pool. Both are pure: they read the objects the pool controller already loaded and
// make no IBM call, so a misconfigured pool costs nothing and is explained by its WorkerPoolReady condition.

// DesiredSizePerZone returns IBM's workerCount for a MachinePool size: MachinePool.spec.replicas is the total of the
// pool, while IBM sizes a pool per zone (its workerCount is the number of workers in each zone), so the size per zone
// is replicas / zoneCount. A total that is not a multiple of the zone count is an error, never rounded: rounding would
// silently run a different number of workers than the MachinePool asks for.
//
// Parameters:
//   - replicas: the MachinePool's spec.replicas, defaulted to 1 when unset (CAPI's default); 0 is allowed here and is
//     PoolGuards' decision (AllowZeroReplicaPools).
//   - zoneCount: the number of zones of the IBMCloudManagedMachinePool (spec.zones), at least 1.
//
// It returns an error when zoneCount is below 1 or replicas is negative, and, when replicas is not a multiple of
// zoneCount, an error whose text is the WorkerPoolReady message of ReplicasNotDivisibleByZones with the nearest
// sizes that are ("replicas=4 is not a multiple of 3 zones; use 3 or 6").
func DesiredSizePerZone(replicas int32, zoneCount int) (int64, error) {
	switch {
	case zoneCount < 1:
		return 0, fmt.Errorf("the pool lists %d zones; list at least one zone in spec.zones", zoneCount)
	case replicas < 0:
		return 0, fmt.Errorf("replicas=%d is negative; set MachinePool spec.replicas to a multiple of %d zones", replicas, zoneCount)
	}
	if message := replicasNotDivisibleMessage(replicas, zoneCount); message != "" {
		return 0, errors.New(message)
	}
	return int64(replicas) / int64(zoneCount), nil
}

// replicasNotDivisibleMessage returns the WorkerPoolReady message of ReplicasNotDivisibleByZones with the nearest
// totals that are multiples of the zone count, or "" when replicas is one.
//
// Parameters:
//   - replicas: the MachinePool's replicas, at least 0.
//   - zoneCount: the pool's number of zones, at least 1.
func replicasNotDivisibleMessage(replicas int32, zoneCount int) string {
	total, zones := int64(replicas), int64(zoneCount)
	if total%zones == 0 {
		return ""
	}
	lower, upper := total/zones*zones, (total/zones+1)*zones
	use := fmt.Sprintf("%d or %d", lower, upper)
	if lower == 0 {
		// A pool needs at least one worker per zone, so 0 is no suggestion.
		use = fmt.Sprint(upper)
	}
	return fmt.Sprintf("replicas=%d is not a multiple of %d zones; use %s (IBM sizes worker pools per zone)", total, zones, use)
}

// PoolGuardInput is what PoolGuards reads: objects the pool controller has already loaded.
type PoolGuardInput struct {
	// Pool is the IBMCloudManagedMachinePool to check. Required.
	Pool *infrav1.IBMCloudManagedMachinePool

	// MachinePool is the CAPI MachinePool that owns Pool; its spec.replicas, spec.template.spec.version and
	// spec.failureDomains are checked. Required.
	MachinePool *clusterv1.MachinePool

	// InfraCluster is the Cluster's IBMCloudManagedCluster; its spec.zones and status.zones are the zones a pool may
	// use. Required.
	InfraCluster *infrav1.IBMCloudManagedCluster

	// Distribution is the control plane's spec.distribution. Required.
	Distribution controlplanev1.Distribution

	// ClusterPools are every IBMCloudManagedMachinePool of the Cluster (by the cluster-name label), deleting ones
	// included; Pool itself may be among them. They decide PoolNameConflict.
	ClusterPools []infrav1.IBMCloudManagedMachinePool
}

// PoolGuardResult is PoolGuards' answer.
type PoolGuardResult struct {
	// Reason is the WorkerPoolReady reason of the first guard that fails; "" when every guard passes. Any reason
	// blocks every IBM mutation of the pool (create, resize, replace); observation still runs unless
	// StopsPublication is true.
	Reason string

	// Message explains Reason with the fix; deterministic and free of IBM text, so it never churns. "" when Reason is "".
	Message string

	// StopsPublication is true for PoolNameConflict: the pool also stops publishing providerIDList, replicas and
	// ready, so that two objects never publish the same workers.
	StopsPublication bool

	// DesiredSizePerZone is DesiredSizePerZone(replicas, len(zones)), IBM's workerCount for the pool; meaningful only
	// when Reason is "".
	DesiredSizePerZone int64

	// VersionMessage is a non-blocking WorkersUpToDate message when MachinePool spec.template.spec.version is set; ""
	// otherwise.
	VersionMessage string

	// FailureDomainsMessage is a non-blocking WorkerPoolReady message when MachinePool spec.failureDomains is set and
	// differs from the pool's zones; "" otherwise.
	FailureDomainsMessage string
}

// Blocked reports whether a guard failed, so that the pool controller makes no IBM mutation for the pool.
func (r PoolGuardResult) Blocked() bool {
	return r.Reason != ""
}

// VersionIgnoredMessage is PoolGuardResult.VersionMessage, the message of the VersionIgnored reason: the master minor
// drives the workers' minor and IBM chooses the fix pack, so the MachinePool's version has no effect on either flavor.
const VersionIgnoredMessage = "VersionIgnored: MachinePool spec.template.spec.version is ignored: workers follow the " +
	"control plane minor; fix packs follow spec.workerUpdates"

// FailureDomainsIgnoredMessage is PoolGuardResult.FailureDomainsMessage: IBM places a pool's workers in the zones of
// the IBM pool, which CAPIBM creates from IBMCloudManagedMachinePool spec.zones.
const FailureDomainsIgnoredMessage = "MachinePool spec.failureDomains is ignored; IBMCloudManagedMachinePool.spec.zones is used"

// PoolGuards checks a pool before any IBM call. It is pure and returns the first failing guard in this order:
//
//  1. PoolNameConflict: another IBMCloudManagedMachinePool of the Cluster has the same spec.poolName. Every claimant
//     gets it and none wins, so the outcome never depends on creationTimestamp, which clusterctl move resets
//     (mover.go:1002-1060). It comes first because it is the only guard that also stops publication
//     (StopsPublication): reported behind another failing guard, the claimants would keep publishing the same
//     workers. Deleting claimants count too: a pool being deleted still owns the IBM pool's workers until its own
//     deletion finishes.
//  2. UnknownZone: a zone of spec.zones is not in the IBMCloudManagedCluster's spec.zones (needs a human: pool zones
//     are immutable, so the fix is adding the zone to the IBMCloudManagedCluster).
//  3. WaitingForInfrastructure: such a zone is in the IBMCloudManagedCluster's spec.zones but not yet resolved in its
//     status.zones.
//  4. ZeroReplicasNotSupported: replicas is 0 while AllowZeroReplicaPools is false (whether IBM accepts a pool
//     resized to 0 workers is not yet verified against IBM Cloud).
//  5. ReplicasNotDivisibleByZones: replicas is not a multiple of the zone count (DesiredSizePerZone).
//  6. InvalidForDistribution: spec.entitlement on a Kubernetes (IKS) cluster.
//
// The two non-blocking messages are computed whatever the guards decide.
//
// Parameters:
//   - in: the objects to check; see PoolGuardInput.
//
// It returns an error naming the field to fix when in is invalid.
func PoolGuards(in PoolGuardInput) (PoolGuardResult, error) {
	if err := in.validate(); err != nil {
		return PoolGuardResult{}, err
	}
	zones := poolZoneNames(in.Pool)
	result := PoolGuardResult{
		VersionMessage:        versionMessage(in.MachinePool),
		FailureDomainsMessage: failureDomainsMessage(in.MachinePool, zones),
	}
	if others := in.otherClaimants(); len(others) > 0 {
		result.Reason = infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyPoolNameConflictReason
		result.StopsPublication = true
		// The fix is a deletion, because spec.poolName is immutable. The message names the claimant to keep: the one
		// whose status.poolID is set already manages the IBM pool, so keeping it changes nothing for that pool.
		result.Message = fmt.Sprintf("IBMCloudManagedMachinePool %s also set spec.poolName %q; while more than one "+
			"object of the Cluster claims an IBM worker pool, none of them creates, sizes or publishes it; spec.poolName is "+
			"immutable, so delete the MachinePool of every claimant but one, and keep the one whose status.poolID is set, "+
			"if any", boundedList(others), in.Pool.Spec.PoolName)
		return result, nil
	}
	if reason, message := in.zoneGuard(zones); reason != "" {
		result.Reason, result.Message = reason, message
		return result, nil
	}
	replicas := ptr.Deref(in.MachinePool.Spec.Replicas, 1)
	if replicas == 0 && !AllowZeroReplicaPools {
		result.Reason = infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyZeroReplicasNotSupportedReason
		result.Message = fmt.Sprintf("MachinePool %s has replicas=0, but an IBM worker pool needs at least one worker per "+
			"zone; set replicas to a multiple of %d, or delete the MachinePool to remove the pool", in.MachinePool.Name, len(zones))
		return result, nil
	}
	if message := replicasNotDivisibleMessage(replicas, len(zones)); message != "" {
		result.Reason = infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyReplicasNotDivisibleByZonesReason
		result.Message = message
		return result, nil
	}
	if in.Pool.Spec.Entitlement != "" && in.Distribution != controlplanev1.DistributionOpenShift {
		result.Reason = infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyInvalidForDistributionReason
		result.Message = fmt.Sprintf("spec.entitlement is %s, which only OpenShift (ROKS) pools accept, but the cluster's "+
			"distribution is %s; entitlement cannot be removed, so recreate this IBMCloudManagedMachinePool without it",
			in.Pool.Spec.Entitlement, in.Distribution)
		return result, nil
	}
	desired, err := DesiredSizePerZone(replicas, len(zones))
	if err != nil {
		// validate and the replica guards above rule out every error of DesiredSizePerZone; an error here is a
		// programming error and is returned as one, never turned into a condition.
		return PoolGuardResult{}, err
	}
	result.DesiredSizePerZone = desired
	return result, nil
}

// validate returns an error naming the first invalid field of in. A negative replica count is rejected here too:
// the MachinePool API does not accept one, so it can only be a programming error.
func (in PoolGuardInput) validate() error {
	switch {
	case in.Pool == nil:
		return errors.New("PoolGuardInput.Pool is nil; pass the IBMCloudManagedMachinePool being reconciled")
	case in.MachinePool == nil:
		return errors.New("PoolGuardInput.MachinePool is nil; pass the MachinePool that owns the IBMCloudManagedMachinePool")
	case in.InfraCluster == nil:
		return errors.New("PoolGuardInput.InfraCluster is nil; pass the Cluster's IBMCloudManagedCluster")
	case in.Distribution != controlplanev1.DistributionKubernetes && in.Distribution != controlplanev1.DistributionOpenShift:
		return fmt.Errorf("PoolGuardInput.Distribution is %q; want %q or %q", in.Distribution,
			controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift)
	case in.Pool.Spec.PoolName == "":
		return fmt.Errorf("IBMCloudManagedMachinePool %s has no spec.poolName; the API requires one", in.Pool.Name)
	case len(in.Pool.Spec.Zones) == 0:
		return fmt.Errorf("IBMCloudManagedMachinePool %s has no spec.zones; the API requires at least one", in.Pool.Name)
	case ptr.Deref(in.MachinePool.Spec.Replicas, 1) < 0:
		return fmt.Errorf("MachinePool %s has spec.replicas=%d; want 0 or more", in.MachinePool.Name, *in.MachinePool.Spec.Replicas)
	}
	return nil
}

// otherClaimants returns the sorted names of the other IBMCloudManagedMachinePools of ClusterPools that set the same
// spec.poolName as Pool. Pool is recognized by namespace and name, because the pools of a Cluster share its namespace.
func (in PoolGuardInput) otherClaimants() []string {
	var others []string
	for i := range in.ClusterPools {
		other := &in.ClusterPools[i]
		if other.Namespace == in.Pool.Namespace && other.Name == in.Pool.Name {
			continue
		}
		if other.Spec.PoolName == in.Pool.Spec.PoolName && !slices.Contains(others, other.Name) {
			others = append(others, other.Name)
		}
	}
	slices.Sort(others)
	return others
}

// zoneGuard returns UnknownZone or WaitingForInfrastructure with its message for the first zone of the pool, in spec
// order, that is not a resolved zone of the IBMCloudManagedCluster; "" when every zone is resolved. An unknown zone
// anywhere in the list wins, because it needs a human, while a zone that is still being resolved only needs time.
//
// Parameters:
//   - zones: the pool's zone names in spec order.
func (in PoolGuardInput) zoneGuard(zones []string) (string, string) {
	declared := make([]string, 0, len(in.InfraCluster.Spec.Zones))
	resolved := make([]string, 0, len(in.InfraCluster.Status.Zones))
	for _, z := range in.InfraCluster.Spec.Zones {
		declared = append(declared, z.Name)
	}
	for _, z := range in.InfraCluster.Status.Zones {
		resolved = append(resolved, z.Name)
	}
	for _, zone := range zones {
		if !slices.Contains(declared, zone) {
			return infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyUnknownZoneReason, fmt.Sprintf("zone %s of spec.zones "+
				"is not a zone of IBMCloudManagedCluster %s (its zones: %s); add the zone with its subnet to that "+
				"IBMCloudManagedCluster's spec.zones", zone, in.InfraCluster.Name, noneIfEmpty(declared))
		}
	}
	for _, zone := range zones {
		if !slices.Contains(resolved, zone) {
			return infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForInfrastructureReason, fmt.Sprintf("zone %s is "+
				"not resolved yet in IBMCloudManagedCluster %s status.zones; waiting for its subnet to be validated",
				zone, in.InfraCluster.Name)
		}
	}
	return "", ""
}

// poolZoneNames returns the names of the pool's spec.zones in spec order.
//
// Parameters:
//   - pool: the IBMCloudManagedMachinePool.
func poolZoneNames(pool *infrav1.IBMCloudManagedMachinePool) []string {
	names := make([]string, 0, len(pool.Spec.Zones))
	for _, z := range pool.Spec.Zones {
		names = append(names, z.Name)
	}
	return names
}

// versionMessage returns VersionIgnoredMessage when the MachinePool sets spec.template.spec.version, else "".
//
// Parameters:
//   - mp: the owning MachinePool.
func versionMessage(mp *clusterv1.MachinePool) string {
	if mp.Spec.Template.Spec.Version == "" {
		return ""
	}
	return VersionIgnoredMessage
}

// failureDomainsMessage returns FailureDomainsIgnoredMessage when the MachinePool sets spec.failureDomains to another
// set of zones than the pool's, else "". The same zones in another order are the same set.
//
// Parameters:
//   - mp: the owning MachinePool.
//   - zones: the pool's zone names.
func failureDomainsMessage(mp *clusterv1.MachinePool, zones []string) string {
	if len(mp.Spec.FailureDomains) == 0 {
		return ""
	}
	want, got := slices.Clone(zones), slices.Clone(mp.Spec.FailureDomains)
	slices.Sort(want)
	slices.Sort(got)
	if slices.Equal(slices.Compact(want), slices.Compact(got)) {
		return ""
	}
	return FailureDomainsIgnoredMessage
}

// boundedList joins sorted names for a condition message: "a", "a, b, c" or "a, b, c and 2 more", so that a long list
// cannot grow the message without bound.
//
// Parameters:
//   - names: the sorted names; at least one.
func boundedList(names []string) string {
	const maxNamed = 3
	if len(names) <= maxNamed {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxNamed], ", "), len(names)-maxNamed)
}

// noneIfEmpty joins names with ", ", or returns "none" for an empty list.
//
// Parameters:
//   - names: the names in their order.
func noneIfEmpty(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}
