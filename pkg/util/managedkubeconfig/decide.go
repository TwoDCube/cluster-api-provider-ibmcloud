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

package managedkubeconfig

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Policy holds the refresh timings Decide applies. The values CAPIBM uses live in one place, KubeconfigPolicy in
// pkg/cloud/scope/managed/assumptions.go, together with what was observed of IBM's admin certificate to justify each.
type Policy struct {
	// RefreshFraction is the share of the certificate lifetime after which the credential is fetched again: 0.5 is the
	// kubeadm control plane's ratio, about one year for IBM's two-year admin certificate. Must be in (0, 1).
	RefreshFraction float64
	// SameCertificateRetry is how long Decide waits after a refresh attempt that left the stored credential in place
	// (IBM re-served the same certificate, as observed on IBM Cloud) before it attempts the next one. Must be > 0.
	SameCertificateRetry time.Duration
	// ExpiryWarning is how long before the certificate's notAfter Decide adds WarningCertificateExpiring. Must be > 0.
	ExpiryWarning time.Duration
	// CARecheckInterval is both the age after which the embedded CA is fetched again and the distance to the CA's
	// notAfter from which it is fetched every MinRequeue (IBM silently re-issues CA certificates). Must be > 0.
	CARecheckInterval time.Duration
	// RotationPollInterval is the requeue while IBM reports a CA rotation in progress. Must be > 0.
	RotationPollInterval time.Duration
	// AuthFailureBackoffMin is the shortest wait between refresh attempts while the stored credential is answered
	// 401. Must be > 0.
	AuthFailureBackoffMin time.Duration
	// AuthFailureBackoffMax is the longest such wait. Must be >= AuthFailureBackoffMin.
	AuthFailureBackoffMax time.Duration
	// MinRequeue is the shortest RequeueAfter Decide returns. Must be > 0.
	MinRequeue time.Duration
	// MaxRequeue is the longest RequeueAfter Decide returns. Must be >= MinRequeue.
	MaxRequeue time.Duration
}

// Validate returns an error naming the first field of p that is out of range.
func (p Policy) Validate() error {
	// NaN is checked by name because it fails every comparison: a plain range check would accept it, and Decide would
	// then treat every certificate as due (a fixed-age refresh, which would only fetch IBM's same cached certificate).
	if math.IsNaN(p.RefreshFraction) || p.RefreshFraction <= 0 || p.RefreshFraction >= 1 {
		return fmt.Errorf("managedkubeconfig: Policy.RefreshFraction is %v; want a value between 0 and 1 exclusive, for example 0.5", p.RefreshFraction)
	}
	durations := []struct {
		name  string
		value time.Duration
	}{
		{"SameCertificateRetry", p.SameCertificateRetry},
		{"ExpiryWarning", p.ExpiryWarning},
		{"CARecheckInterval", p.CARecheckInterval},
		{"RotationPollInterval", p.RotationPollInterval},
		{"AuthFailureBackoffMin", p.AuthFailureBackoffMin},
		{"AuthFailureBackoffMax", p.AuthFailureBackoffMax},
		{"MinRequeue", p.MinRequeue},
		{"MaxRequeue", p.MaxRequeue},
	}
	for _, d := range durations {
		if d.value <= 0 {
			return fmt.Errorf("managedkubeconfig: Policy.%s is %s; want a positive duration", d.name, d.value)
		}
	}
	if p.AuthFailureBackoffMax < p.AuthFailureBackoffMin {
		return fmt.Errorf("managedkubeconfig: Policy.AuthFailureBackoffMax (%s) is below AuthFailureBackoffMin (%s)", p.AuthFailureBackoffMax, p.AuthFailureBackoffMin)
	}
	if p.MaxRequeue < p.MinRequeue {
		return fmt.Errorf("managedkubeconfig: Policy.MaxRequeue (%s) is below MinRequeue (%s)", p.MaxRequeue, p.MinRequeue)
	}
	return nil
}

// Desired is what IBM reports for the cluster now. It deliberately has no credential-identity field: no refresh
// decision may depend on which IAM identity fetched the certificate.
type Desired struct {
	// Server is the API server URL CAPIBM selected from IBM's masterURL. Required.
	Server string
	// CARotationMarker is CARotationMarker(status, actionCompletedDate) of the cluster's caCertRotationStatus; "" when
	// IBM reports no rotation.
	CARotationMarker string
	// CARotationInProgress is true while IBM reports a CA rotation that has not completed (the caller decides it with
	// the fail-safe rule of IsCARotationInProgress). It shortens RequeueAfter to RotationPollInterval.
	CARotationInProgress bool
}

// Current is what the kubeconfig Secret holds and what the last probe of it found. It deliberately has no
// credential-identity field: no refresh decision may depend on which IAM identity fetched the certificate.
type Current struct {
	// UserProvided is true when the Secret exists and is not of type cluster.x-k8s.io/secret. CAPIBM never modifies such
	// a Secret, so Decide returns no refresh.
	UserProvided bool
	// Kubeconfig is Inspect's description of the Secret's value; nil when there is no Secret or its value is not a
	// kubeconfig Inspect accepts. nil means "missing" and is refreshed at once.
	Kubeconfig *Info
	// CARotationMarker is the ca-rotation-marker annotation: the Desired.CARotationMarker for which the caller last
	// carried out a refresh. A difference triggers a refresh of the CA and the credential.
	CARotationMarker string
	// RefreshCheckedAt is the refresh-checked-at annotation: the time of the latest refresh attempt that left the
	// stored credential in place (IBM returned the same certificate, or the new one failed verification). Zero when
	// there was none since the credential was written. A time after now counts as zero (absent): it cannot record a
	// real attempt, and the caller's next write replaces it.
	RefreshCheckedAt time.Time
	// CAFetchedAt is the ca-fetched-at annotation: when the embedded CA was last fetched or confirmed. Zero means
	// unknown and makes a CA re-check due. A time after now counts as zero for the same reason as RefreshCheckedAt.
	CAFetchedAt time.Time
	// LastProbe is the outcome of the latest probe of the stored kubeconfig.
	LastProbe ProbeStatus
}

// ProbeStatus is the part of the latest probe of the stored kubeconfig that Decide uses.
type ProbeStatus struct {
	// Unauthorized is true when the latest probe was answered 401.
	Unauthorized bool
	// UnauthorizedSince is when the caller first observed a 401 for the stored credential in the current run of 401
	// answers (the run ends with the first probe of that credential that is not answered 401). The backoff of rule 3
	// is measured from it, so it must be recorded when the probe outcome changes to 401, from a source the caller
	// trusts. Do not pass the lastTransitionTime of the KubeconfigReady condition: CAPI keeps that time while the
	// condition stays False (util/conditions/setter.go, v1.14.0), so after an earlier False reason such as
	// EndpointUnreachable it predates the 401 run and the backoff would start at AuthFailureBackoffMax instead of
	// AuthFailureBackoffMin. Required when Unauthorized is true. A time after now counts as now, which can only make the
	// next attempt earlier.
	UnauthorizedSince time.Time
}

// Reason says why Decide asks for a refresh. Its values are stable and may appear in condition messages and logs.
type Reason string

const (
	// ReasonMissing means: there is no Secret, or its value is not a kubeconfig Inspect accepts.
	ReasonMissing Reason = "KubeconfigMissing"
	// ReasonServerChanged means: IBM's selected master URL differs from the stored server.
	ReasonServerChanged Reason = "ServerChanged"
	// ReasonCARotation means: IBM's CA rotation marker differs from the one the stored kubeconfig was refreshed for.
	ReasonCARotation Reason = "CARotationMarkerChanged"
	// ReasonUnauthorized means: the stored credential was answered 401 and the backoff has passed.
	ReasonUnauthorized Reason = "Unauthorized"
	// ReasonCertificateAging means: RefreshFraction of the certificate lifetime has passed.
	ReasonCertificateAging Reason = "CertificateAging"
	// ReasonCARecheck means: the embedded CA was fetched CARecheckInterval ago or longer.
	ReasonCARecheck Reason = "CARecheckDue"
	// ReasonCANearExpiry means: the embedded CA expires within CARecheckInterval.
	ReasonCANearExpiry Reason = "CANearExpiry"
)

// Warning is an observation the caller reports without failing; its value is the Warning event reason.
type Warning string

const (
	// WarningCertificateExpiring means: the stored certificate expires within ExpiryWarning while IBM still serves it. The
	// caller keeps KubeconfigReady True with a message and emits a Warning event. When IBM mints a new admin certificate
	// is not yet verified against IBM Cloud.
	WarningCertificateExpiring Warning = "AdminCertificateExpiring"
)

// Decision is Decide's answer.
type Decision struct {
	// Refresh asks the caller to fetch the admin kubeconfig again (GetAdminKubeconfig), verify it, and write it only when
	// it passes and its certificate differs from the stored one; otherwise only refresh-checked-at is written (IBM may
	// serve the same certificate again).
	Refresh bool
	// RefreshCA asks the caller to fetch the CA again (GetCACertificate), which exposes no credential.
	RefreshCA bool
	// Reasons lists why, in rule order; empty when nothing is due.
	Reasons []Reason
	// Warnings lists observations to report; they never block.
	Warnings []Warning
	// RequeueAfter is when the caller should evaluate Decide again; always in [MinRequeue, MaxRequeue]. It is
	// MinRequeue when something is due, so the outcome of the caller's action is evaluated soon.
	RequeueAfter time.Duration
}

// CARotationMarker joins the two fields of IBM's caCertRotationStatus into the value Desired.CARotationMarker and the
// ca-rotation-marker annotation carry: "<status>|<actionCompletedDate>", or "" when both are empty (no rotation ever
// reported). Both fields are part of the marker because a second rotation can end with the same status text.
//
// Parameters: status and actionCompletedDate are the raw IBM values.
func CARotationMarker(status, actionCompletedDate string) string {
	if status == "" && actionCompletedDate == "" {
		return ""
	}
	return status + "|" + actionCompletedDate
}

// Decide applies the refresh rules below in order and returns what to fetch and when to look again. It is pure: it
// reads no clock, makes no call and has no credential-identity input, so the decision never depends on which IAM
// identity fetched the stored certificate. There is no fixed-age refresh: IBM would return the same cached
// certificate (observed on IBM Cloud).
//
// Rules, in order:
//  1. A user-provided Secret is never refreshed.
//  2. A missing or unacceptable kubeconfig, a changed server, or a changed CA rotation marker refreshes the CA and the
//     credential at once.
//  3. A 401 on the last probe refreshes the credential with a backoff from AuthFailureBackoffMin to
//     AuthFailureBackoffMax; while it lasts, rule 4 does not apply.
//  4. Once RefreshFraction of the certificate lifetime has passed, the credential is refreshed, and again
//     SameCertificateRetry after each attempt that left the stored certificate in place.
//  5. The embedded CA (ClusterCA trust only) is fetched again CARecheckInterval after the last fetch, and every
//     MinRequeue once it is within CARecheckInterval of its notAfter.
//  6. Within ExpiryWarning of the certificate's notAfter, WarningCertificateExpiring is added.
//
// Parameters: now is the caller's clock reading; desired is IBM's current view; current is the stored Secret and the
// last probe; policy holds the timings and must pass Policy.Validate. Decide returns an error only for invalid input.
func Decide(now time.Time, desired Desired, current Current, policy Policy) (Decision, error) {
	if err := validateDecideInput(now, desired, current, policy); err != nil {
		return Decision{}, err
	}
	if current.UserProvided {
		// A user-provided Secret is never modified; the caller still probes it every steady poll.
		return Decision{RequeueAfter: policy.MaxRequeue}, nil
	}

	kc := current.Kubeconfig
	switch {
	case kc == nil:
		return refreshEverything(ReasonMissing, policy), nil
	case kc.Server != desired.Server:
		return refreshEverything(ReasonServerChanged, policy), nil
	case current.CARotationMarker != desired.CARotationMarker:
		return refreshEverything(ReasonCARotation, policy), nil
	}

	var d Decision
	next := newSchedule(now, policy.MaxRequeue)
	decideCredential(&d, next, now, current, policy)
	decideCA(&d, next, now, current, policy)

	warnAt := kc.Certificate.NotAfter.Add(-policy.ExpiryWarning)
	if now.Before(warnAt) {
		next.at(warnAt)
	} else {
		d.Warnings = append(d.Warnings, WarningCertificateExpiring)
	}
	if desired.CARotationInProgress {
		next.in(policy.RotationPollInterval)
	}
	if d.Refresh || d.RefreshCA {
		next.in(policy.MinRequeue)
	}
	d.RequeueAfter = clamp(next.wait, policy.MinRequeue, policy.MaxRequeue)
	return d, nil
}

// refreshEverything is rule 2: nothing stored can be trusted to match IBM's view, so both the CA and the credential are
// fetched. The CA rotation poll needs no extra handling: MinRequeue is the shortest requeue anyway.
func refreshEverything(reason Reason, policy Policy) Decision {
	return Decision{Refresh: true, RefreshCA: true, Reasons: []Reason{reason}, RequeueAfter: policy.MinRequeue}
}

// decideCredential applies rules 3 and 4. current.Kubeconfig is set: Decide returned earlier otherwise.
func decideCredential(d *Decision, next *schedule, now time.Time, current Current, policy Policy) {
	lastAttempt := pastOrZero(current.RefreshCheckedAt, now)

	if current.LastProbe.Unauthorized {
		// Rule 3. The wait after an attempt equals how long the 401s had lasted at that attempt, bounded by the
		// policy: it doubles with every attempt (1m, 1m, 2m, 4m, 8m, 10m, ... for 1m→10m) without a stored counter,
		// because the annotations of the kubeconfig Secret hold times, not counts.
		since := notAfterNow(current.LastProbe.UnauthorizedSince, now)
		if lastAttempt.IsZero() || lastAttempt.Before(since) {
			markRefresh(d, ReasonUnauthorized)
			return
		}
		retryAt := lastAttempt.Add(clamp(lastAttempt.Sub(since), policy.AuthFailureBackoffMin, policy.AuthFailureBackoffMax))
		if now.Before(retryAt) {
			next.at(retryAt)
			return
		}
		markRefresh(d, ReasonUnauthorized)
		return
	}

	// Rule 4. Multiplying the lifetime by the fraction in float64 is exact enough: nanosecond rounding on a two-year
	// lifetime is irrelevant against a one-minute requeue granularity.
	cert := current.Kubeconfig.Certificate
	lifetime := cert.NotAfter.Sub(cert.NotBefore)
	refreshAt := cert.NotBefore.Add(time.Duration(float64(lifetime) * policy.RefreshFraction))
	if now.Before(refreshAt) {
		next.at(refreshAt)
		return
	}
	if !lastAttempt.IsZero() {
		retryAt := lastAttempt.Add(policy.SameCertificateRetry)
		if now.Before(retryAt) {
			next.at(retryAt)
			return
		}
	}
	markRefresh(d, ReasonCertificateAging)
}

// decideCA applies rule 5; current.Kubeconfig is set. A kubeconfig without CA data (SystemRoots) embeds nothing that
// could go stale.
func decideCA(d *Decision, next *schedule, now time.Time, current Current, policy Policy) {
	ca := current.Kubeconfig.CA
	if ca == nil {
		return
	}
	reason := ReasonCARecheck
	interval := policy.CARecheckInterval
	nearExpiryAt := ca.NotAfter.Add(-policy.CARecheckInterval)
	if now.Before(nearExpiryAt) {
		next.at(nearExpiryAt)
	} else {
		// Close to the CA's notAfter, poll for IBM's re-issued CA every MinRequeue instead of every 12 hours.
		reason = ReasonCANearExpiry
		interval = policy.MinRequeue
	}
	fetchedAt := pastOrZero(current.CAFetchedAt, now)
	if !fetchedAt.IsZero() && now.Before(fetchedAt.Add(interval)) {
		next.at(fetchedAt.Add(interval))
		return
	}
	d.RefreshCA = true
	d.Reasons = append(d.Reasons, reason)
}

// validateDecideInput rejects input that would make Decide's answer meaningless.
func validateDecideInput(now time.Time, desired Desired, current Current, policy Policy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if now.IsZero() {
		return errors.New("managedkubeconfig: Decide needs the current time; now is zero")
	}
	if current.UserProvided {
		return nil
	}
	if desired.Server == "" {
		return errors.New("managedkubeconfig: Desired.Server is empty; pass the API server URL selected from IBM's masterURL")
	}
	if current.LastProbe.Unauthorized && current.LastProbe.UnauthorizedSince.IsZero() {
		return errors.New("managedkubeconfig: Current.LastProbe.UnauthorizedSince is zero although Unauthorized is true; pass when the caller first observed the current run of 401 answers")
	}
	return nil
}

// markRefresh records a credential refresh and its reason.
func markRefresh(d *Decision, reason Reason) {
	d.Refresh = true
	d.Reasons = append(d.Reasons, reason)
}

// pastOrZero returns t, or the zero time when t lies in the future. It reads the refresh-checked-at and ca-fetched-at
// annotations, which anyone who can write the Secret can edit. A future time cannot be a real attempt or fetch, so it
// is treated like a missing annotation: the rule it guards fires, and the caller's next write replaces it with a real
// time. Treating it as now instead would postpone that rule on every evaluation, until the future time is reached.
func pastOrZero(t, now time.Time) time.Time {
	if t.After(now) {
		return time.Time{}
	}
	return t
}

// notAfterNow returns t, or now when t lies in the future. It is used only for ProbeStatus.UnauthorizedSince, which the
// caller supplies and Validate requires to be set: a future value (for example clock skew between controller
// replicas) means the 401 run has just begun, which makes the next attempt earlier, never later.
func notAfterNow(t, now time.Time) time.Time {
	if t.After(now) {
		return now
	}
	return t
}

// clamp bounds d to [lowest, highest].
func clamp(d, lowest, highest time.Duration) time.Duration {
	if d < lowest {
		return lowest
	}
	if d > highest {
		return highest
	}
	return d
}

// schedule tracks the earliest future moment at which a rule's outcome changes.
type schedule struct {
	now  time.Time
	wait time.Duration
}

// newSchedule starts a schedule whose wait is longest.
func newSchedule(now time.Time, longest time.Duration) *schedule {
	return &schedule{now: now, wait: longest}
}

// at records that something changes at t.
func (s *schedule) at(t time.Time) {
	s.in(t.Sub(s.now))
}

// in records that something changes after d.
func (s *schedule) in(d time.Duration) {
	if d < s.wait {
		s.wait = d
	}
}
