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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file decides the master minor update. The master minor is the user's: it is requested by raising
// spec.distributionVersion by one. Master patches are IBM's: IBM applies them on its own schedule and CAPIBM only
// reports them, so no row here ever requests a patch. DecideMasterAction evaluates the guards of its rows and names at
// most one IBM call, UpdateMaster; MasterUpdateCallOutcome maps IBM's answer to it, with an UpdateMaster 403 meaning a
// missing role, not a version skip: CAPIBM requests only the master's next minor, so IBM's documented refusal to skip
// versions cannot be the cause. IBM shows nothing after it refuses a call, so the refusal outlives the reconcile that
// made the call only in the condition the caller wrote from that answer: DecideMasterAction reads it back
// (MasterVersionInput.CurrentUpToDate) and keeps it until the retry, instead of reporting an update IBM never took. The
// inputs that may cost a call, the offered versions (VersionsCache) and the worker listing, are asked for only when a
// row needs them; the worker listing is therefore made only right before a request. (The caller reads the offered
// versions on every run anyway, for the newer minors in status; the VersionsCache asks IBM for them at most once an
// hour per identity and region.) Every version named in a condition comes from masterKubeVersion or targetVersion
// through ibmversion.Version.WithoutPending, so IBM's " (pending)" marker never reaches status. Two answers keep
// MasterVersionUpToDate from claiming more than CAPIBM knows: a masterKubeVersion that does not parse is Unknown, and
// masterInputFailure reports a call for an input that IBM refused after a raise of spec.distributionVersion, where the
// condition of the old minor no longer holds. A newer minor that IBM offers never makes the master outdated: the True
// condition only names it (MasterVersionInput.Available, status.availableDistributionVersions), because the master runs
// what the spec asks for and only the user decides when to raise it.

// masterEndOfSupportNotice is how long before IBM's versionEOS the MasterVersionUpToDate message mentions the end of
// support; a CAPIBM choice, not an IBM fact.
const masterEndOfSupportNotice = 45 * 24 * time.Hour

// masterPermissionDeniedReason is the MasterVersionUpToDate reason of an UpdateMaster 403: the credentials lack the
// Kubernetes Service role. A 403 to a call for an input of the decision (masterInputFailure) has it too; only the
// message tells them apart (masterUpdateDeniedPrefix).
const masterPermissionDeniedReason = controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDatePermissionDeniedReason

// masterMaxWorkerSkew returns how many minors IBM lets workers run behind the master: 2 for Kubernetes (IKS worker
// nodes may be up to two minors behind the master) and 1 for OpenShift (ROKS: one minor), so a master update to target
// is refused while a worker runs a minor below target - skew (BlockedByWorkerSkew). CAPIBM checks it before the call
// because the only way past IBM's check is force, which CAPIBM never sends. An unknown distribution gets the stricter
// 1.
//
// Parameters:
//   - d: spec.distribution.
func masterMaxWorkerSkew(d controlplanev1.Distribution) int {
	if d == controlplanev1.DistributionKubernetes {
		return 2
	}
	return 1
}

// MasterVersionInput is everything DecideMasterAction decides on.
type MasterVersionInput struct {
	// Distribution is spec.distribution. Required.
	Distribution controlplanev1.Distribution

	// DesiredVersion is spec.distributionVersion, MAJOR.MINOR such as "1.37" or "4.22". Required.
	DesiredVersion string

	// Cluster is this reconcile's GetCluster answer: state, masterKubeVersion, targetVersion, masterHealth,
	// caCertRotationStatus and versionEOS are read. Required.
	Cluster *kubernetesservice.Cluster

	// MasterUpdate is status.masterUpdate, the latest update CAPIBM requested; the zero value when none.
	MasterUpdate controlplanev1.MasterUpdateRequest

	// CurrentUpToDate is the MasterVersionUpToDate condition the object carries now (util/conditions Get; nil when it
	// has none). It is the only record of IBM's terminal answer to UpdateMaster: IBM shows nothing after it refuses a
	// call, and status.masterUpdate only records that the call was sent. When it carries the Reason of a Terminal
	// MasterUpdateCallResult (PermissionDenied or VersionSkipNotAllowed), was written for Generation, and
	// status.masterUpdate is for the version spec.distributionVersion asks for, DecideMasterAction keeps it instead of
	// reporting UpgradePending and then UpgradeFailed, which would send the operator after a failed IBM update instead
	// of the missing Kubernetes Service role.
	CurrentUpToDate *metav1.Condition

	// Generation is the object's metadata.generation. A refusal in CurrentUpToDate is kept only while its
	// observedGeneration (which util/conditions Set records) equals it: a version-skip refusal lasts until the spec
	// changes, and row 7 writes the same VersionSkipNotAllowed reason for a spec that may since have been changed back
	// to the next minor, which must not be taken for IBM's refusal.
	Generation int64

	// Offered are the versions IBM offers (ListVersions through the VersionsCache, cached per identity and region).
	// Read only when spec.distributionVersion is the master's next minor and every guard before row 8 passed; nil
	// otherwise. When it is needed and nil, the decision is NeedsOfferedVersions.
	Offered *kubernetesservice.Versions

	// Workers are every worker of the cluster from one ListWorkers call, for the worker skew guard. Read only right
	// before a request; when they are needed and not listed, the decision is NeedsWorkers.
	Workers []kubernetesservice.Worker

	// WorkersListed is true when Workers holds the answer of a ListWorkers call (an empty list then means no workers).
	WorkersListed bool

	// Available is status.availableDistributionVersions as this reconcile leaves it: the minors newer than the master's
	// that IBM offers for the distribution (newerOfferedMinors). The True MasterVersionUpToDate message names them, so
	// that an available update is visible while the condition stays True; nil or empty names nothing. Optional.
	Available []string

	// Now is the caller's clock reading. Required.
	Now time.Time
}

// MasterAction is DecideMasterAction's answer.
type MasterAction struct {
	// NeedsOfferedVersions is true when spec.distributionVersion is the master's next minor, every guard before row 8
	// passed, and Offered is nil: the caller sets Offered from the VersionsCache and decides again. No other field is
	// set then.
	NeedsOfferedVersions bool

	// NeedsWorkers is true when the decision would request the update and WorkersListed is false: the caller lists the
	// workers (one ListWorkers call), sets Workers and WorkersListed, and decides again. No other field is set then. It
	// is never set for a minor IBM does not offer, or for a request that waits out its grace, backoff or refusal, so
	// those reconciles make no ListWorkers call.
	NeedsWorkers bool

	// RequestUpdate is true when the caller requests the update: first a direct status patch of status.masterUpdate to
	// MasterUpdate, which must succeed, then UpdateMaster(clusterID, UpdateVersion) without force, and
	// MasterUpdateCallOutcome on its answer (row 12 of DecideMasterAction).
	RequestUpdate bool

	// UpdateVersion is the version UpdateMaster sends, MasterUpdateVersion(distribution, desired minor), for example
	// "1.37" or "4.22_openshift" (that IBM accepts this minor-only form is not yet verified against IBM Cloud); set
	// with RequestUpdate.
	UpdateVersion string

	// MasterUpdate is the status.masterUpdate to write before the call: this version, now, and the attempts for this
	// version plus one; set with RequestUpdate.
	MasterUpdate controlplanev1.MasterUpdateRequest

	// ClearMasterUpdate is true when the master reached the minor of status.masterUpdate, so the record is removed (row
	// 2 of DecideMasterAction).
	ClearMasterUpdate bool

	// UpdatePending is true while IBM shows a master update (IsMasterUpdatePending), a requested minor or a patch IBM
	// applies on its own: the caller polls at masterUpdatePoll and makes no call.
	UpdatePending bool

	// UpToDate is the MasterVersionUpToDate condition (type, status, reason, message; the caller sets it with
	// util/conditions, which keeps the transition time). It is Unknown with the reason UnparsableMasterVersion when IBM's
	// masterKubeVersion does not parse; no other field is set then.
	UpToDate metav1.Condition

	// RollingOut is the RollingOut contract condition. Its Type is empty, and the caller leaves the condition as it is,
	// when the decision does not know whether a master update is in progress (an unparsable masterKubeVersion).
	RollingOut metav1.Condition

	// RecheckAt is when the decision may change without any watched change: the end of MasterUpdatePendingGrace
	// (UpgradePending), or the retry time (UpgradeFailed, or a kept PermissionDenied refusal). The zero time means the
	// caller's own interval.
	RecheckAt time.Time
}

// masterVersions holds the parsed versions of a MasterVersionInput.
type masterVersions struct {
	// desired is the minor of spec.distributionVersion.
	desired ibmversion.Minor
	// master is IBM's masterKubeVersion without the " (pending)" marker, the form every message uses, so the marker
	// never reaches status.
	master ibmversion.Version
	// marked is true when masterKubeVersion carried the " (pending)" marker.
	marked bool
	// observed is the master's minor, parsed from masterKubeVersion.
	observed ibmversion.Minor
	// masterParsed is true when masterKubeVersion parsed; master, marked and observed are meaningful only then.
	masterParsed bool
	// requested is the minor of status.masterUpdate.version; meaningful only when requestedOK.
	requested ibmversion.Minor
	// requestedOK is true when status.masterUpdate names a version.
	requestedOK bool
}

// DecideMasterAction decides the master minor update by the rows below, in this order (the code and its tests cite the
// row numbers; the refusal row and row 9 are out of numeric order on purpose, see below):
//
//   - IBM's masterKubeVersion does not parse (IBM's form of masterKubeVersion during an update is not yet verified
//     against IBM Cloud, so a form the parser does not know is possible): CAPIBM cannot tell which minor the master
//     runs, so MasterVersionUpToDate is Unknown with the reason UnparsableMasterVersion and the version IBM reported,
//     nothing is requested, and RollingOut is left as it is. A True of an earlier reconcile would claim a minor CAPIBM
//     no longer knows.
//   - Row 1. IBM shows an update (IsMasterUpdatePending: " (pending)", state updating, or targetVersion above the
//     master, including a patch IBM applies on its own): wait, RollingOut=True MasterUpdateInProgress naming the
//     target.
//   - Row 2. status.masterUpdate names a minor the master reached: clear it, continue.
//   - Row 3. The master runs the minor of spec.distributionVersion: UpToDate, whatever patch IBM runs, mentioning an
//     end of support within 45 days, and for state unsupported that IBM no longer supports the minor. This row is
//     checked before the guards below because they only block an update, and UpToDate means "observed minor =
//     spec.distributionVersion"; a CA rotation or a cluster state must not make an up-to-date master look outdated.
//   - Row 4. A CA rotation in progress (IsCARotationInProgress, fail safe): BlockedByCARotation.
//   - Row 5. A state in MasterUpdateBlockedStates or master health in UnhealthyMasterHealth: BlockedByClusterState.
//     unsupported and update_canceled are not blocked: in both IBM says the master must be updated.
//   - Row 6. spec.distributionVersion below the master's minor: DowngradeNotAllowed (IBM cannot roll back).
//   - Row 7. More than one minor above: VersionSkipNotAllowed.
//   - Row 8. The next minor, not offered by IBM: VersionNotAvailable.
//   - Refusal. status.masterUpdate is for this version and CurrentUpToDate holds IBM's terminal answer to it, written
//     for this generation: keep it, no call; a PermissionDenied until the retry time of row 11, a version-skip refusal
//     until the spec changes.
//   - Row 10. Requested less than MasterUpdatePendingGrace ago: UpgradePending, no call.
//   - Row 11. Requested before, nothing pending, master still on the old minor, within the retry backoff (1 h doubling
//     per attempt, at most 24 h): UpgradeFailed, no call.
//   - Row 9. A worker more than masterMaxWorkerSkew minors below the target: BlockedByWorkerSkew (never force).
//   - Row 12. Otherwise: request the update (RequestUpdate).
//
// Row 9 comes after the rows that hold a request (refusal, 10, 11) because it needs a ListWorkers call and protects
// only a request: those rows send nothing, and row 9 is evaluated again before every request, so the order changes no
// call, only that a hold is reported instead of a skew that appeared after the request, and a reconcile that holds a
// request makes no ListWorkers call.
//
// Parameters:
//   - in: the observation; see MasterVersionInput.
//
// It returns an error naming the field when in is invalid: a missing input, or a spec.distributionVersion or
// status.masterUpdate.version that does not parse; no IBM call is then made.
func DecideMasterAction(in MasterVersionInput) (MasterAction, error) {
	v, err := in.parse()
	if err != nil {
		return MasterAction{}, err
	}
	if !v.masterParsed {
		return MasterAction{UpToDate: in.unparsableMasterCondition(v)}, nil
	}
	if IsMasterUpdatePending(in.Cluster) {
		return in.decidePending(v), nil
	}
	action := MasterAction{
		ClearMasterUpdate: v.requestedOK && v.requested.Compare(v.observed) <= 0,
		RollingOut:        masterRollingOut(false, ""),
	}
	if v.desired == v.observed {
		action.UpToDate = in.upToDateCondition(v)
		return action, nil
	}
	target := v.desired.String()
	switch {
	case IsCARotationInProgress(in.Cluster.CARotationStatus):
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByCARotationReason,
			fmt.Sprintf("IBM reports a CA certificate rotation that has not completed; the master update to %s waits "+
				"until it completes", target))
		return action, nil
	case slices.Contains(MasterUpdateBlockedStates(), in.Cluster.State):
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason,
			fmt.Sprintf("IBM cluster state %s does not allow a master update; the update to %s waits", in.Cluster.State, target))
		return action, nil
	case IsMasterUnhealthy(in.Cluster):
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByClusterStateReason,
			fmt.Sprintf("IBM reports master health %s; the master update to %s waits until it recovers",
				in.Cluster.MasterHealth, target))
		return action, nil
	}
	if condition, ok := in.specProblem(v); ok {
		// Rows 6 and 7: the spec asks for something IBM cannot do.
		action.UpToDate = condition
		return action, nil
	}
	return in.decideUpdate(v, action), nil
}

// parse validates in and parses its versions. CAPIBM's own inputs (spec.distributionVersion, status.masterUpdate) are
// validated first and fail with an error. IBM's masterKubeVersion is parsed last: when it does not parse, the result
// has masterParsed false and no error, because that is IBM's answer, which DecideMasterAction reports as a condition.
func (in MasterVersionInput) parse() (masterVersions, error) {
	switch {
	case bindingClusterType(in.Distribution) == "":
		return masterVersions{}, fmt.Errorf("MasterVersionInput.Distribution is %q; want %q or %q", in.Distribution,
			controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift)
	case in.Cluster == nil:
		return masterVersions{}, errors.New("MasterVersionInput.Cluster is nil; pass this reconcile's GetCluster answer")
	case in.Now.IsZero():
		return masterVersions{}, errors.New("MasterVersionInput.Now is not set; pass the controller clock's current time")
	case in.CurrentUpToDate != nil &&
		in.CurrentUpToDate.Type != controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition:
		return masterVersions{}, fmt.Errorf("MasterVersionInput.CurrentUpToDate is a %q condition; pass the object's %s "+
			"condition, or nil when it has none", in.CurrentUpToDate.Type,
			controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
	}
	desired, err := ibmversion.ParseMinor(in.DesiredVersion)
	if err != nil {
		return masterVersions{}, fmt.Errorf("MasterVersionInput.DesiredVersion (spec.distributionVersion): %w", err)
	}
	v := masterVersions{desired: desired}
	if in.MasterUpdate.Version != "" {
		requested, err := ibmversion.Parse(in.MasterUpdate.Version)
		if err != nil {
			return masterVersions{}, fmt.Errorf("MasterVersionInput.MasterUpdate.Version (status.masterUpdate.version): %w", err)
		}
		v.requested, v.requestedOK = requested.MinorVersion(), true
	}
	if master, err := ibmversion.Parse(in.Cluster.MasterKubeVersion); err == nil {
		v.master, v.marked, v.observed, v.masterParsed = master.WithoutPending(), master.Pending, master.MinorVersion(), true
	}
	return v, nil
}

// unparsableMasterCondition returns MasterVersionUpToDate=Unknown for a masterKubeVersion that does not parse. The
// message quotes IBM's value, which stays the same while IBM reports it, so a repeated reconcile writes the same
// condition and no status; the parse error itself is left out, since it only restates the value.
//
// Parameters:
//   - v: the parsed versions; masterParsed is false.
func (in MasterVersionInput) unparsableMasterCondition(v masterVersions) metav1.Condition {
	return metav1.Condition{
		Type:   controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
		Status: metav1.ConditionUnknown,
		Reason: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUnparsableMasterVersionReason,
		Message: fmt.Sprintf("IBM reports master version %q for IBM cluster %s, which CAPIBM cannot parse, so it cannot tell "+
			"whether the master runs spec.distributionVersion %s; CAPIBM requests no master update until IBM reports a "+
			"version it can parse", in.Cluster.MasterKubeVersion, in.Cluster.ID, v.desired),
	}
}

// upToDateCondition returns MasterVersionUpToDate=True for a master on the minor of spec.distributionVersion (row 3).
//
// Parameters:
//   - v: the parsed versions.
func (in MasterVersionInput) upToDateCondition(v masterVersions) metav1.Condition {
	return metav1.Condition{
		Type:    controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
		Status:  metav1.ConditionTrue,
		Reason:  controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateReason,
		Message: in.upToDateMessage(v),
	}
}

// specProblem returns the MasterVersionUpToDate condition of a spec.distributionVersion IBM cannot reach (rows 6 and
// 7): DowngradeNotAllowed below the master's minor, VersionSkipNotAllowed more than one minor above it. ok is false
// for the master's minor and its next minor.
//
// TODO: the skip rule (row 7) and its message assume that the master's next version is its next minor. After
// OpenShift 4.22 a master can move to 4.23 or to 5.0; once IBM's per-cluster upgrade-path endpoint is stable, accept
// every version it lists for the cluster and name those in the message (see the TODO on ibmversion.Minor.Next).
//
// Parameters:
//   - v: the parsed versions.
func (in MasterVersionInput) specProblem(v masterVersions) (metav1.Condition, bool) {
	switch {
	case v.desired == v.observed:
		return metav1.Condition{}, false
	case v.desired.Compare(v.observed) < 0:
		return masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateDowngradeNotAllowedReason,
			fmt.Sprintf("spec.distributionVersion %s is below the master's minor %s (IBM version %s); IBM cannot roll a "+
				"master back, so set spec.distributionVersion to %s", v.desired, v.observed, v.master, v.observed)), true
	case v.desired != v.observed.Next():
		return masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason,
			fmt.Sprintf("spec.distributionVersion %s is more than one minor above the master's minor %s; IBM updates a "+
				"master one minor at a time, so set spec.distributionVersion to %s first", v.desired, v.observed,
				v.observed.Next())), true
	default:
		return metav1.Condition{}, false
	}
}

// upToDateMessage is the message of MasterVersionUpToDate=True: the IBM version the master runs, and, when they apply,
// that IBM no longer supports the minor (state unsupported) or ends its support within 45 days (supportNote), and the
// newer minors IBM offers (availableNote).
//
// Parameters:
//   - v: the parsed versions.
func (in MasterVersionInput) upToDateMessage(v masterVersions) string {
	return fmt.Sprintf("the master runs %s, minor %s as spec.distributionVersion asks; IBM applies master patches itself",
		v.master, v.observed) + in.supportNote(v) + availableNote(in.Available)
}

// supportNote returns the part of the True MasterVersionUpToDate message about the end of IBM's support of the master's
// minor: that IBM no longer supports it (state unsupported), or the end of support when it is less than 45 days away
// or cannot be parsed; "" otherwise.
//
// TODO: both notes name the master's next minor as the version to update to. After OpenShift 4.22 a master can move to
// 4.23 or to 5.0; once IBM's per-cluster upgrade-path endpoint is stable, name the versions it lists for the cluster
// (see the TODO on ibmversion.Minor.Next).
//
// Parameters:
//   - v: the parsed versions.
func (in MasterVersionInput) supportNote(v masterVersions) string {
	if in.Cluster.State == clusterStateUnsupported {
		return fmt.Sprintf("; IBM no longer supports %s: upgrade one minor at a time, starting with "+
			"spec.distributionVersion %s", v.observed, v.observed.Next())
	}
	eos := in.Cluster.VersionEOS
	if eos == "" {
		return ""
	}
	end, err := ParseCreatedDate(eos)
	switch {
	case err != nil:
		return fmt.Sprintf("; IBM reports the end of support of %s as %s", v.observed, eos)
	case end.Before(in.Now.Add(masterEndOfSupportNotice)):
		return fmt.Sprintf("; IBM ends support for %s on %s, so plan the update to %s", v.observed,
			end.UTC().Format(time.DateOnly), v.observed.Next())
	default:
		return ""
	}
}

// availableNote returns the part of the True MasterVersionUpToDate message that names the newer minors IBM offers
// (status.availableDistributionVersions), or "" when there are none. It only informs: the condition stays True because
// the master runs the minor the spec asks for, and the master changes only when the user raises
// spec.distributionVersion.
//
// Parameters:
//   - available: the newer minors, MAJOR.MINOR in ascending order (MasterVersionInput.Available).
func availableNote(available []string) string {
	if len(available) == 0 {
		return ""
	}
	return fmt.Sprintf("; update available: IBM offers %s; raise spec.distributionVersion to update",
		strings.Join(available, ", "))
}

// maxAvailableDistributionVersions is the MaxItems bound of status.availableDistributionVersions. IBM offers about
// five minors per distribution, so the bound is never reached in practice; should it be, the nearest minors are kept,
// because an upgrade starts with them.
const maxAvailableDistributionVersions = 16

// maxAvailableDistributionVersionLength is the items MaxLength bound of status.availableDistributionVersions. A minor
// whose MAJOR.MINOR is longer is left out, because the API server would refuse the whole status write for it.
const maxAvailableDistributionVersionLength = 8

// newerOfferedMinors returns the minors that IBM offers for distribution d above the master's minor, MAJOR.MINOR in
// ascending order and each once, for status.availableDistributionVersions. Only the list of the master's distribution
// counts (Kubernetes minors for IKS, OpenShift minors for ROKS), and a minor is newer when Minor.Compare puts it above
// the master's, so a new major counts too. It is pure.
//
// TODO: the list is every newer minor of IBM's /versions list, which is what IBM offers for new clusters. Once IBM's
// per-cluster upgrade-path endpoint is stable, list the versions it offers this cluster instead (after OpenShift 4.22
// both 4.23 and 5.0), and also report a master patch that IBM offers but has not applied (see the TODO on
// ibmversion.Minor.Next).
//
// Parameters:
//   - d: spec.distribution.
//   - master: the master's minor.
//   - offered: IBM's offered versions; nil gives nil.
func newerOfferedMinors(d controlplanev1.Distribution, master ibmversion.Minor, offered *kubernetesservice.Versions) []string {
	if offered == nil {
		return nil
	}
	in := MasterVersionInput{Distribution: d, Offered: offered}
	var newer []ibmversion.Minor
	for _, version := range in.offeredVersions() {
		if minor := (ibmversion.Minor{Major: version.Major, Minor: version.Minor}); minor.Compare(master) > 0 {
			newer = append(newer, minor)
		}
	}
	return availableMinorNames(newer)
}

// keptNewerMinors returns the entries of an earlier status.availableDistributionVersions that are still above the
// master's minor, in the same form as newerOfferedMinors. It is the list a reconcile keeps when IBM cannot be asked: the
// last answer, without the minors the master has reached since, which are no longer an update. An entry that does not
// parse is dropped; only CAPIBM writes the field. It is pure.
//
// Parameters:
//   - kept: status.availableDistributionVersions as the reconcile read it.
//   - master: the master's minor.
func keptNewerMinors(kept []string, master ibmversion.Minor) []string {
	var newer []ibmversion.Minor
	for _, entry := range kept {
		if minor, err := ibmversion.ParseMinor(entry); err == nil && minor.Compare(master) > 0 {
			newer = append(newer, minor)
		}
	}
	return availableMinorNames(newer)
}

// availableMinorNames sorts minors in ascending order, drops duplicates and names that the API bounds would refuse
// (maxAvailableDistributionVersionLength), and returns at most maxAvailableDistributionVersions of them, the lowest
// first, as MAJOR.MINOR; nil when none is left.
//
// Parameters:
//   - minors: the minors to name; the slice is sorted in place.
func availableMinorNames(minors []ibmversion.Minor) []string {
	slices.SortFunc(minors, ibmversion.Minor.Compare)
	var names []string
	for _, minor := range slices.Compact(minors) {
		name := minor.String()
		if len(name) > maxAvailableDistributionVersionLength {
			continue
		}
		names = append(names, name)
		if len(names) == maxAvailableDistributionVersions {
			break
		}
	}
	return names
}

// decidePending decides row 1: IBM shows a master update, so CAPIBM waits and reports it.
//
// Parameters:
//   - v: the parsed versions.
func (in MasterVersionInput) decidePending(v masterVersions) MasterAction {
	// The requested minor is in flight while the master is below it, or already names it with the marker (whether a
	// pending masterKubeVersion names the old or the new version is not yet verified against IBM Cloud).
	inFlight := v.requestedOK && v.requested == v.desired &&
		(v.requested.Compare(v.observed) > 0 || (v.requested == v.observed && v.marked))
	action := MasterAction{UpdatePending: true, RollingOut: masterRollingOut(true, in.pendingMessage(v, inFlight))}
	rolling := action.RollingOut.Message
	problem, isProblem := in.specProblem(v)
	switch {
	case inFlight:
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason, rolling)
	case v.desired == v.observed:
		action.UpToDate = in.upToDateCondition(v)
	case isProblem:
		action.UpToDate = problem
	default:
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			fmt.Sprintf("%s; the update to %s waits until IBM finishes it", rolling, v.desired))
	}
	return action
}

// pendingMessage names the master update IBM shows (row 1: the message names the target; for an IBM patch it says "IBM
// is applying master patch <target>"). Every version in it is without the " (pending)" marker.
//
// Parameters:
//   - v: the parsed versions.
//   - inFlight: whether the update is the minor CAPIBM requested.
func (in MasterVersionInput) pendingMessage(v masterVersions, inFlight bool) string {
	target, err := ibmversion.Parse(in.Cluster.TargetVersion)
	switch {
	case err == nil && isNewerIBMVersion(target, v.master) && target.MinorVersion() == v.observed && !inFlight:
		return fmt.Sprintf("IBM is applying master patch %s", target.WithoutPending())
	case err == nil && isNewerIBMVersion(target, v.master):
		return fmt.Sprintf("IBM is updating the master to %s", target.WithoutPending())
	case v.marked:
		return fmt.Sprintf("IBM shows master version %s as pending", v.master)
	default:
		return fmt.Sprintf("IBM is updating the master from %s (cluster state %s)", v.master, in.Cluster.State)
	}
}

// decideUpdate decides the rows for the master's next minor: offered (8), the rows that hold a request already sent
// (refusal, 10 and 11; holdRequest), worker skew (9) and the request (12). It asks for the offered versions and the
// worker listing only when a row needs them.
//
// Parameters:
//   - v: the parsed versions.
//   - action: the decision so far (ClearMasterUpdate and RollingOut set).
func (in MasterVersionInput) decideUpdate(v masterVersions, action MasterAction) MasterAction {
	d := in.Distribution
	if in.Offered == nil {
		return MasterAction{NeedsOfferedVersions: true}
	}
	if !in.offers(v.desired) {
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionNotAvailableReason,
			fmt.Sprintf("IBM Cloud does not offer %s %s (offered: %s)", masterProduct(d), v.desired, in.offeredMinors()))
		return action
	}
	version := MasterUpdateVersion(d, v.desired)
	attempts := int32(1)
	if in.MasterUpdate.Version == version {
		if held, ok := in.holdRequest(v, action); ok {
			return held
		}
		attempts = max(in.MasterUpdate.Attempts, 0) + 1
	}
	// Row 9 only now: it costs a ListWorkers call and protects only the request below.
	if !in.WorkersListed {
		return MasterAction{NeedsWorkers: true}
	}
	if skewed := in.skewedWorkers(v.desired); len(skewed) > 0 {
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateBlockedByWorkerSkewReason,
			fmt.Sprintf("IBM refuses a master update to %s while a worker runs more than %d minor(s) below it: %s; the "+
				"update waits until CAPIBM has replaced them with workers of the master's minor", v.desired,
				masterMaxWorkerSkew(d), listWorkers(skewed)))
		return action
	}
	action.RequestUpdate, action.UpdateVersion = true, version
	action.MasterUpdate = controlplanev1.MasterUpdateRequest{Version: version, LastRequestTime: metav1.NewTime(in.Now),
		Attempts: attempts}
	action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
		fmt.Sprintf("CAPIBM requests the master update from %s to %s", v.master, v.desired))
	action.RollingOut = masterRollingOut(true, fmt.Sprintf("CAPIBM requests the master update to %s", v.desired))
	return action
}

// holdRequest decides the rows that hold the request status.masterUpdate records for the desired version, while IBM
// shows nothing pending and the master is still on the old minor: IBM's refusal of it, the pending grace (row 10) and
// the retry backoff (row 11). ok is false when none of them holds, and the update is then requested again.
//
// Parameters:
//   - v: the parsed versions.
//   - action: the decision so far (ClearMasterUpdate and RollingOut set).
func (in MasterVersionInput) holdRequest(v masterVersions, action MasterAction) (MasterAction, bool) {
	requestedAt := in.MasterUpdate.LastRequestTime.Time
	retry := masterRetryTime(in.MasterUpdate)
	if refusal, ok := in.refusal(); ok {
		// IBM refused the call, so it shows nothing pending, and rows 10 and 11 would report an update IBM never took.
		// A missing role is retried on the backoff of row 11, so that a role granted later takes effect without a spec
		// change; a version skip only after the spec changes.
		kept := masterCondition(refusal.Reason, refusal.Message)
		switch {
		case refusal.Reason == controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason:
			action.UpToDate = kept
			return action, true
		case in.Now.Before(retry):
			action.UpToDate, action.RecheckAt = kept, retry
			return action, true
		default:
			// The retry time has passed (and with it the grace and the backoff): request the update again.
			return MasterAction{}, false
		}
	}
	if grace := requestedAt.Add(MasterUpdatePendingGrace); in.Now.Before(grace) {
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradePendingReason,
			fmt.Sprintf("CAPIBM requested the master update to %s at %s; waiting for IBM to show it", v.desired,
				bindingTime(requestedAt)))
		action.RollingOut = masterRollingOut(true, fmt.Sprintf("CAPIBM requested the master update to %s", v.desired))
		action.RecheckAt = grace
		return action, true
	}
	if in.Now.Before(retry) {
		// The message names no cause, because this state follows an update IBM took and rolled back as well as a call
		// that failed in transit (Transient, RateLimited) or a refusal recorded for an earlier generation.
		action.UpToDate = masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateUpgradeFailedReason,
			fmt.Sprintf("the master update to %s that CAPIBM requested at %s has not taken effect: the master still runs "+
				"%s and IBM shows no update in progress; CAPIBM requests it again at %s", v.desired, bindingTime(requestedAt),
				v.master, bindingTime(retry)))
		action.RecheckAt = retry
		return action, true
	}
	return MasterAction{}, false
}

// refusal returns CurrentUpToDate when it records IBM's terminal answer to UpdateMaster for the current generation: a
// False condition with a Reason that MasterUpdateCallOutcome sets for a Terminal answer. ok is false otherwise. The
// caller has checked that status.masterUpdate is for the desired version, the version that answer refused.
//
// A PermissionDenied counts only when its message starts with masterUpdateDeniedPrefix of that version: a 403 to the
// ListVersions call of this step has the same reason (masterInputFailure), and taking it for IBM's refusal of
// UpdateMaster would keep its message, which names a listing that may work again by now, until the retry time, instead
// of rows 10 and 11. The message is the record because status has no field for IBM's answer, as with the removal
// runner's refusalRecord.
func (in MasterVersionInput) refusal() (metav1.Condition, bool) {
	c := in.CurrentUpToDate
	if c == nil || c.Status != metav1.ConditionFalse || c.ObservedGeneration != in.Generation {
		return metav1.Condition{}, false
	}
	switch c.Reason {
	case masterPermissionDeniedReason:
		if !strings.HasPrefix(c.Message, masterUpdateDeniedPrefix(in.MasterUpdate.Version)) {
			return metav1.Condition{}, false
		}
		return *c, true
	case controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason:
		return *c, true
	default:
		return metav1.Condition{}, false
	}
}

// keptVersionSkip reports whether CurrentUpToDate records IBM's version-skip refusal (ClassVersionSkipNotAllowed, whose
// IBM error code is not yet verified against IBM Cloud) of the request that status.masterUpdate records for the desired
// minor: the record the refusal row keeps until the spec changes (holdRequest). The version check is the one
// decideUpdate makes before it asks refusal; it also tells IBM's refusal apart from row 7's own VersionSkipNotAllowed,
// which is written for a desired minor CAPIBM never requests. It is pure.
//
// Parameters:
//   - v: the parsed versions; masterParsed is true.
func (in MasterVersionInput) keptVersionSkip(v masterVersions) bool {
	if in.MasterUpdate.Version != MasterUpdateVersion(in.Distribution, v.desired) {
		return false
	}
	refusal, ok := in.refusal()
	return ok && refusal.Reason == controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason
}

// masterRetryTime returns when CAPIBM requests the update of a status.masterUpdate again while the master stays on the
// old minor: its lastRequestTime plus masterRetryDelay of its attempts (row 11).
//
// Parameters:
//   - request: status.masterUpdate.
func masterRetryTime(request controlplanev1.MasterUpdateRequest) time.Time {
	return request.LastRequestTime.Add(masterRetryDelay(request.Attempts))
}

// offers reports whether IBM offers minor for the distribution (row 8).
//
// Parameters:
//   - minor: the requested minor.
func (in MasterVersionInput) offers(minor ibmversion.Minor) bool {
	for _, offered := range in.offeredVersions() {
		if offered.Major == minor.Major && offered.Minor == minor.Minor {
			return true
		}
	}
	return false
}

// offeredVersions returns IBM's offered versions of the distribution.
func (in MasterVersionInput) offeredVersions() []kubernetesservice.ReleaseVersion {
	if in.Distribution == controlplanev1.DistributionOpenShift {
		return in.Offered.OpenShift
	}
	return in.Offered.Kubernetes
}

// offeredMinors lists IBM's offered minors of the distribution in ascending order, for the VersionNotAvailable message
// (for example "IBM Cloud does not offer Kubernetes 1.33 (offered: 1.34, 1.35, 1.36, 1.37)").
func (in MasterVersionInput) offeredMinors() string {
	minors := make([]ibmversion.Minor, 0, len(in.offeredVersions()))
	for _, offered := range in.offeredVersions() {
		minors = append(minors, ibmversion.Minor{Major: offered.Major, Minor: offered.Minor})
	}
	slices.SortFunc(minors, ibmversion.Minor.Compare)
	minors = slices.Compact(minors)
	names := make([]string, 0, len(minors))
	for _, minor := range minors {
		names = append(names, minor.String())
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// skewedWorkers returns, sorted, the IDs of the workers that run a minor more than masterMaxWorkerSkew below target, or
// whose version does not parse (fail safe: CAPIBM cannot show they are close enough). Deleted workers and workers
// without an actual version yet (provisioning) are left out.
//
// TODO: the lowest allowed minor is counted back within the target's major, which assumes that versions follow each
// other minor by minor. A master update from OpenShift 4.22 to 5.0 would find every 4.x worker skewed. Once IBM's
// per-cluster upgrade-path endpoint is stable, take the allowed worker versions from IBM's upgrade graph (see the TODO on
// ibmversion.Minor.Next).
//
// Parameters:
//   - target: the minor the master would be updated to.
func (in MasterVersionInput) skewedWorkers(target ibmversion.Minor) []string {
	lowest := ibmversion.Minor{Major: target.Major, Minor: target.Minor - masterMaxWorkerSkew(in.Distribution)}
	var skewed []string
	for _, w := range in.Workers {
		if w.ActualState == WorkerStateDeleted || w.KubeVersionActual == "" {
			continue
		}
		actual, err := ibmversion.Parse(w.KubeVersionActual)
		if err != nil || actual.MinorVersion().Compare(lowest) < 0 {
			skewed = append(skewed, w.ID)
		}
	}
	slices.Sort(skewed)
	return skewed
}

// masterRetryDelay returns how long after the attempts-th request for a version CAPIBM requests it again while the
// master stays on the old minor: MasterUpdateRetryAfter, doubled for every attempt after the first, at most
// MasterUpdateRetryMax (1 h, 2 h, 4 h, ..., 24 h; these defaults are not yet verified against IBM Cloud). The doubling
// stops at the cap, so a large count cannot overflow.
//
// Parameters:
//   - attempts: status.masterUpdate.attempts; values below 1 count as 1.
func masterRetryDelay(attempts int32) time.Duration {
	delay := MasterUpdateRetryAfter
	for i := int32(1); i < attempts && delay < MasterUpdateRetryMax; i++ {
		delay *= 2
	}
	return min(delay, MasterUpdateRetryMax)
}

// masterProduct names the IBM product of a distribution in messages.
//
// Parameters:
//   - d: spec.distribution.
func masterProduct(d controlplanev1.Distribution) string {
	if d == controlplanev1.DistributionOpenShift {
		return "Red Hat OpenShift"
	}
	return "Kubernetes"
}

// masterCondition returns a MasterVersionUpToDate=False condition.
//
// Parameters:
//   - reason: the reason constant.
//   - message: the message.
func masterCondition(reason, message string) metav1.Condition {
	return metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition,
		Status: metav1.ConditionFalse, Reason: reason, Message: message}
}

// masterRollingOut returns the RollingOut condition: True with MasterUpdateInProgress and the message while a master
// update is requested or in progress, False with NotRollingOut otherwise.
//
// Parameters:
//   - rolling: whether a master update is requested or in progress.
//   - message: what is rolling, for the True condition.
func masterRollingOut(rolling bool, message string) metav1.Condition {
	if rolling {
		return metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition, Status: metav1.ConditionTrue,
			Reason: controlplanev1.IBMCloudManagedControlPlaneRollingOutMasterUpdateInProgressReason, Message: message}
	}
	return metav1.Condition{Type: controlplanev1.IBMCloudManagedControlPlaneRollingOutCondition, Status: metav1.ConditionFalse,
		Reason: controlplanev1.IBMCloudManagedControlPlaneNotRollingOutReason}
}

// MasterUpdateCallResult is MasterUpdateCallOutcome's answer.
type MasterUpdateCallResult struct {
	// Class is kubernetesservice.Classify(OpUpdateMaster, err).
	Class kubernetesservice.ErrorClass

	// Handled is false when the caller applies its common error handling for Class (Transient, RateLimited,
	// Unauthorized with InvalidateAuthenticator, BadRequest, NotFound); status.masterUpdate already records the attempt,
	// so the retry follows the grace and backoff rows of DecideMasterAction.
	Handled bool

	// Accepted is true when IBM took the request (204) or already had it (409, ClassMasterUpdateAlreadyRequested):
	// MasterVersionUpToDate=False UpgradePending, RollingOut=True, poll at masterUpdatePoll.
	Accepted bool

	// Terminal is true when a human must act (ClassPermissionDenied, ClassVersionSkipNotAllowed): the caller sets
	// MasterVersionUpToDate=False with Reason and Message in place of the UpgradePending of MasterAction.UpToDate, and
	// decides again with the request on record. From then on the refusal row of DecideMasterAction reads that condition
	// back (MasterVersionInput.CurrentUpToDate) and keeps it, with no call: a missing role until the retry time the
	// Message names, which the decision returns as MasterAction.RecheckAt, so that the control plane is requeued no
	// later than that time and requests the update again then; a version skip until the spec changes, looked at again
	// at the control plane's own polls.
	Terminal bool

	// Reason is the MasterVersionUpToDate reason of a Terminal answer: PermissionDenied for a missing role,
	// VersionSkipNotAllowed for IBM's version-skip code; "" otherwise.
	Reason string

	// Message explains the outcome for the condition message; for a 403 it names the missing Kubernetes Service roles,
	// the credentials by Identity.Description, never an IAM ID, key or e-mail, and when CAPIBM requests the update
	// again. It names IBM's error code only, never the incident ID or IBM's description, so that a repeated refusal
	// gives the same message (masterIBMReference).
	Message string
}

// MasterUpdateCallOutcome maps IBM's answer to UpdateMaster. A 403 is ClassPermissionDenied, a missing Kubernetes
// Service role, unless IBM sent an error code recorded as its version-skip refusal (kubernetesservice.Classify):
// DecideMasterAction only ever requests the master's next minor, so IBM's documented skip refusal cannot be the cause,
// and VersionSkipNotAllowed would send the operator after the wrong cause.
//
// Parameters:
//   - err: the error UpdateMaster returned; nil for success.
//   - request: the status.masterUpdate written before the call (MasterAction.MasterUpdate): its version is the one
//     requested, and its lastRequestTime and attempts give the retry time a refusal message names.
//   - identityDescription: credentials.Identity.Description of the identity the call used, for example "the CAPIBM
//     controller identity".
//
// It returns an error when request has no version or no lastRequestTime, or identityDescription is empty.
func MasterUpdateCallOutcome(err error, request controlplanev1.MasterUpdateRequest, identityDescription string) (MasterUpdateCallResult, error) {
	switch {
	case request.Version == "":
		return MasterUpdateCallResult{}, errors.New("MasterUpdateCallOutcome needs the status.masterUpdate written before " +
			"the call (MasterAction.MasterUpdate); its version is empty")
	case request.LastRequestTime.IsZero():
		return MasterUpdateCallResult{}, errors.New("MasterUpdateCallOutcome needs the status.masterUpdate written before " +
			"the call (MasterAction.MasterUpdate); its lastRequestTime is not set")
	case identityDescription == "":
		return MasterUpdateCallResult{}, errors.New("MasterUpdateCallOutcome needs the Description of the credentials.Identity " +
			"the call used, for the permission message")
	}
	return masterUpdateOutcome(kubernetesservice.Classify(kubernetesservice.OpUpdateMaster, err), err, request,
		identityDescription), nil
}

// masterUpdateOutcome builds MasterUpdateCallOutcome's answer for a class. It takes the class as a parameter so that
// the tests can show the VersionSkipNotAllowed row, which Classify returns only once IBM's error code for the skip
// refusal is recorded (it is not yet verified against IBM Cloud).
//
// TODO: both refusal messages assume that the master's next version is its next minor ("CAPIBM requests one minor at a
// time", "set spec.distributionVersion to the master's next minor"). After OpenShift 4.22 a master can move to 4.23 or
// to 5.0; once IBM's per-cluster upgrade-path endpoint is stable, name the versions it lists for the cluster (see the
// TODO on ibmversion.Minor.Next).
//
// Parameters:
//   - class: the class of IBM's answer.
//   - err: IBM's answer, for its code in the message (masterIBMReference; never the incident ID).
//   - request: the status.masterUpdate written before the call.
//   - identityDescription: credentials.Identity.Description of the identity the call used.
func masterUpdateOutcome(class kubernetesservice.ErrorClass, err error, request controlplanev1.MasterUpdateRequest,
	identityDescription string,
) MasterUpdateCallResult {
	version := request.Version
	result := MasterUpdateCallResult{Class: class, Handled: true}
	switch class {
	case kubernetesservice.ClassNone:
		result.Accepted = true
		result.Message = fmt.Sprintf("IBM accepted the master update to %s", version)
	case kubernetesservice.ClassMasterUpdateAlreadyRequested:
		result.Accepted = true
		result.Message = fmt.Sprintf("IBM already has the master update to %s; waiting for it to start", version)
	case kubernetesservice.ClassPermissionDenied:
		result.Terminal, result.Reason = true, masterPermissionDeniedReason
		result.Message = masterUpdateDeniedPrefix(version) + fmt.Sprintf("%s: the credentials of %s need the Administrator "+
			"platform role and the Manager service role of Kubernetes Service on the cluster's resource group. CAPIBM "+
			"requests one minor at a time, so this is not IBM's refusal to skip versions; CAPIBM requests the update "+
			"again at %s", masterIBMReference(err), identityDescription, bindingTime(masterRetryTime(request)))
	case kubernetesservice.ClassVersionSkipNotAllowed:
		result.Terminal = true
		result.Reason = controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateVersionSkipNotAllowedReason
		result.Message = fmt.Sprintf("IBM refused the master update to %s as a version skip%s; set "+
			"spec.distributionVersion to the master's next minor; CAPIBM requests no update until the spec changes",
			version, masterIBMReference(err))
	default:
		result.Handled = false
	}
	return result
}

// masterUpdateDeniedPrefix returns the start of the PermissionDenied message of IBM's 403 to UpdateMaster for version
// (masterUpdateOutcome). It is the record of that refusal, which the refusal row reads (MasterVersionInput.refusal):
// a 403 to a call for an input of the decision has the same reason but another message (masterInputFailure).
//
// Parameters:
//   - version: the version UpdateMaster was sent, status.masterUpdate.version.
func masterUpdateDeniedPrefix(version string) string {
	return fmt.Sprintf("IBM Cloud Kubernetes Service refused the master update to %s with HTTP 403", version)
}

// masterCallDefinitive reports whether IBM's answer of class to a call of the master version step is one that a retry
// with backoff does not fix (a 403, 400 or 404, or a request kubernetesservice refused to send), so that it is reported
// and looked at again at the next poll. The other classes are retried by their own handling
// (ControlPlaneScope.masterCallFailed): 401 with a rebuilt authenticator, 429 after Retry-After, and 5xx, network
// errors and unclassified answers with controller-runtime's backoff. ClassNone is no failure. It is the single rule for
// both masterCallFailed and masterInputFailure, so the condition and the requeue always agree.
//
// Parameters:
//   - class: kubernetesservice.Classify of the call's error.
func masterCallDefinitive(class kubernetesservice.ErrorClass) bool {
	switch class {
	case kubernetesservice.ClassNone, kubernetesservice.ClassUnauthorized, kubernetesservice.ClassRateLimited,
		kubernetesservice.ClassTransient, kubernetesservice.ClassUnknown:
		return false
	default:
		return true
	}
}

// masterInputFailure returns the MasterVersionUpToDate condition for a call that fetched an input of DecideMasterAction
// (ListVersions for NeedsOfferedVersions, ListWorkers for NeedsWorkers) and failed with a definitive answer
// (masterCallDefinitive), and true. DecideMasterAction asks for these inputs only after a raise of
// spec.distributionVersion, when the condition of an earlier reconcile may still be the True of the old minor ("the
// master runs ... as spec.distributionVersion asks"), which would stay until the call succeeds. A 403 is
// PermissionDenied, named like the UpdateMaster 403: the call, the Kubernetes Service roles (the Administrator platform
// role and the Manager service role) and the identity by its Description. Every other definitive answer (400, 404, a
// request kubernetesservice refused to send) is IBMAPIError. The messages carry IBM's HTTP status and error code only,
// never the incident ID or IBM's description (masterIBMReference), and no time, so the same failure writes the same
// condition on every reconcile and no status; the caller logs IBM's whole answer. It is pure.
//
// It returns false, and the caller writes no condition, for:
//   - an answer that its own handling retries soon (401, 429, 5xx, network errors, unclassified answers): such a blip
//     says nothing about the update, and a condition written for it would flap;
//   - a desired minor equal to the observed one: row 3's condition is still accurate. DecideMasterAction asks for no
//     input then; the check keeps the condition right should that ever change;
//   - an input that DecideMasterAction itself rejects or reports (parse);
//   - IBM's version-skip refusal of the request on record, which the refusal row keeps until the spec changes
//     (keptVersionSkip). The version listing comes before that row (row 8), so a failed listing would otherwise replace
//     the record: once the listing worked again, the refusal row would no longer find it, and row 11 would send
//     UpdateMaster once more at its retry time, only for IBM to refuse it again. The failure is still logged
//     (masterCallFailed), and the call is made again at the next poll.
//
// A kept PermissionDenied refusal of UpdateMaster, by contrast, is replaced, and the condition then names the input
// call that fails now: the refusal row keeps that record only until the retry time of row 11, and once the listing
// works again rows 10 and 11 hold the request until that same time, so losing the record costs no UpdateMaster.
//
// Parameters:
//   - in: the input of the decision that asked for the call.
//   - class: kubernetesservice.Classify of the call's error.
//   - action: what the call did, for example "listing the workers of IBM cluster <id>".
//   - err: the call's error, for IBM's HTTP status and code.
//   - identityDescription: credentials.Identity.Description of the identity the call used, for example "the CAPIBM
//     controller identity".
func masterInputFailure(in MasterVersionInput, class kubernetesservice.ErrorClass, action string, err error,
	identityDescription string,
) (metav1.Condition, bool) {
	if !masterCallDefinitive(class) {
		return metav1.Condition{}, false
	}
	v, parseErr := in.parse()
	if parseErr != nil || !v.masterParsed || v.desired == v.observed || in.keptVersionSkip(v) {
		return metav1.Condition{}, false
	}
	const retry = "CAPIBM tries again at the next poll of the control plane"
	reference := masterIBMReference(err)
	switch status := cpIBMStatus(err); {
	case class == kubernetesservice.ClassPermissionDenied:
		return masterCondition(masterPermissionDeniedReason, fmt.Sprintf("IBM Cloud Kubernetes Service refused %s with "+
			"HTTP 403%s, so the master update to %s waits: the credentials of %s need %s on the cluster's resource group; %s",
			action, reference, v.desired, identityDescription, cpKubernetesServiceRoles, retry)), true
	case status != 0:
		return masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			fmt.Sprintf("IBM Cloud Kubernetes Service refused %s with HTTP %d%s, so the master update to %s waits; %s, and "+
				"the controller log has IBM's description", action, status, reference, v.desired, retry)), true
	default:
		// A request that kubernetesservice refused to send (ErrInvalidRequest) has no HTTP status.
		return masterCondition(controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateIBMAPIErrorReason,
			fmt.Sprintf("CAPIBM could not complete %s, so the master update to %s waits; %s, and the controller log names "+
				"the cause", action, v.desired, retry)), true
	}
}

// masterIBMReference returns " (IBM code <c>)" for an IBM answer that carries a code, else "": the same form as every
// other condition message of the control plane (cpIBMCode). It never names IBM's incident ID: IBM sends a new incident
// ID with every answer, so a MasterVersionUpToDate message carrying it would change with each refusal and rewrite
// status, whose own watch event starts the next reconcile. The caller logs IBM's whole answer, incident ID included,
// for IBM support (ControlPlaneScope.requestMasterUpdate).
//
// Parameters:
//   - err: the error UpdateMaster returned.
func masterIBMReference(err error) string {
	return cpIBMCode(err)
}
