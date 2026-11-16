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
	"math"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	clocktesting "k8s.io/utils/clock/testing"
)

const (
	day                = 24 * time.Hour
	rotationInProgress = "CA certificate rotation in progress"
)

// decideFunc is the signature Decide must keep: no credential-identity input.
type decideFunc func(time.Time, Desired, Current, Policy) (Decision, error)

// issuedAt is when the in-test admin certificate starts; like IBM's admin certificate it is valid exactly two years.
var issuedAt = time.Date(2026, 11, 16, 3, 40, 8, 0, time.UTC)

// farFuture plays an annotation someone edited to a time that cannot be a real attempt or fetch.
var farFuture = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

// testPolicy returns the values KubeconfigPolicy holds in pkg/cloud/scope/managed/assumptions.go.
func testPolicy() Policy {
	return Policy{
		RefreshFraction:       0.5,
		SameCertificateRetry:  24 * time.Hour,
		ExpiryWarning:         30 * day,
		CARecheckInterval:     12 * time.Hour,
		RotationPollInterval:  time.Minute,
		AuthFailureBackoffMin: time.Minute,
		AuthFailureBackoffMax: 10 * time.Minute,
		MinRequeue:            time.Minute,
		MaxRequeue:            12 * time.Hour,
	}
}

// inspected builds and inspects in, returning the Info Decide receives for a stored CAPIBM Secret.
func inspected(t *testing.T, in Input) *Info {
	t.Helper()
	info, err := Inspect(mustBuild(t, in))
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	return &info
}

// withCANotAfter returns a copy of info whose CA expires at notAfter.
func withCANotAfter(info *Info, notAfter time.Time) *Info {
	out := *info
	ca := *info.CA
	ca.NotAfter = notAfter
	out.CA = &ca
	return &out
}

func TestDecide(t *testing.T) {
	f := newFixture(t)
	iks := inspected(t, f.iksInput())
	roks := inspected(t, f.roksInput())
	notAfter := iks.Certificate.NotAfter
	marker := CARotationMarker(rotationInProgress, "")

	testCases := []struct {
		name    string
		now     time.Time
		desired Desired
		current Current
		want    Decision
	}{
		{
			name:    "missing secret refreshes CA and credential",
			now:     issuedAt,
			desired: Desired{Server: testServer},
			current: Current{},
			want:    Decision{Refresh: true, RefreshCA: true, Reasons: []Reason{ReasonMissing}, RequeueAfter: time.Minute},
		},
		{
			name:    "user-provided secret is never refreshed",
			now:     issuedAt.Add(400 * day),
			desired: Desired{Server: testServer},
			current: Current{UserProvided: true, Kubeconfig: iks, LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: issuedAt}},
			want:    Decision{RequeueAfter: 12 * time.Hour},
		},
		{
			name:    "user-provided secret needs no desired server",
			now:     issuedAt,
			current: Current{UserProvided: true},
			want:    Decision{RequeueAfter: 12 * time.Hour},
		},
		{
			name:    "server change refreshes CA and credential",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: "https://c100.us-south.containers.cloud.ibm.com:443"},
			current: Current{Kubeconfig: iks, CAFetchedAt: issuedAt},
			want:    Decision{Refresh: true, RefreshCA: true, Reasons: []Reason{ReasonServerChanged}, RequeueAfter: time.Minute},
		},
		{
			name:    "CA marker change refreshes CA and credential and polls 1m",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer, CARotationMarker: marker, CARotationInProgress: true},
			current: Current{Kubeconfig: iks, CAFetchedAt: issuedAt},
			want:    Decision{Refresh: true, RefreshCA: true, Reasons: []Reason{ReasonCARotation}, RequeueAfter: time.Minute},
		},
		{
			name:    "CA rotation in progress after the refresh polls 1m",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer, CARotationMarker: marker, CARotationInProgress: true},
			current: Current{Kubeconfig: iks, CARotationMarker: marker, RefreshCheckedAt: issuedAt.Add(time.Hour), CAFetchedAt: issuedAt.Add(time.Hour)},
			want:    Decision{RequeueAfter: time.Minute},
		},
		{
			name:    "CA rotation completed refreshes again",
			now:     issuedAt.Add(2 * time.Hour),
			desired: Desired{Server: testServer, CARotationMarker: CARotationMarker("CA certificate rotation complete", "2026-11-16T05:40:08+0000")},
			current: Current{Kubeconfig: iks, CARotationMarker: marker, RefreshCheckedAt: issuedAt.Add(time.Hour), CAFetchedAt: issuedAt.Add(time.Hour)},
			want:    Decision{Refresh: true, RefreshCA: true, Reasons: []Reason{ReasonCARotation}, RequeueAfter: time.Minute},
		},
		{
			name:    "2y certificate fetched 1h ago no refresh requeue MaxRequeue",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks},
			want:    Decision{RequeueAfter: 12 * time.Hour},
		},
		{
			// With an embedded CA the 12 h CA re-check comes before MaxRequeue.
			name:    "2y certificate fetched 1h ago with cluster CA requeues at the CA recheck",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: iks, CAFetchedAt: issuedAt},
			want:    Decision{RequeueAfter: 11 * time.Hour},
		},
		{
			name:    "2y certificate at 364 days no refresh",
			now:     issuedAt.Add(364 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks},
			want:    Decision{RequeueAfter: 12 * time.Hour},
		},
		{
			name:    "2y certificate at 366 days refresh",
			now:     issuedAt.Add(366 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks},
			want:    Decision{Refresh: true, Reasons: []Reason{ReasonCertificateAging}, RequeueAfter: time.Minute},
		},
		{
			name:    "same certificate returned again no refresh until SameCertificateRetry",
			now:     issuedAt.Add(366 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: issuedAt.Add(366 * day)},
			want:    Decision{RequeueAfter: 12 * time.Hour},
		},
		{
			name:    "same certificate retried SameCertificateRetry later",
			now:     issuedAt.Add(367 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: issuedAt.Add(366 * day)},
			want:    Decision{Refresh: true, Reasons: []Reason{ReasonCertificateAging}, RequeueAfter: time.Minute},
		},
		{
			name:    "30 days before notAfter warns while IBM serves the same certificate",
			now:     notAfter.Add(-30 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: notAfter.Add(-30*day - time.Hour)},
			want:    Decision{Warnings: []Warning{WarningCertificateExpiring}, RequeueAfter: 12 * time.Hour},
		},
		{
			name:    "31 days before notAfter does not warn",
			now:     notAfter.Add(-31 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: notAfter.Add(-31*day - time.Hour)},
			want:    Decision{RequeueAfter: 12 * time.Hour},
		},
		{
			name:    "401 first attempt is immediate",
			now:     issuedAt.Add(10 * day),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: iks, CAFetchedAt: issuedAt.Add(10 * day), LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: issuedAt.Add(10 * day)}},
			want:    Decision{Refresh: true, Reasons: []Reason{ReasonUnauthorized}, RequeueAfter: time.Minute},
		},
		{
			// 401s since 5 minutes, last attempt 1 minute ago (4 minutes into the run): the next one is 4 minutes after
			// it. The half-lifetime rule is due as well but must not bypass the backoff.
			name:    "401 backoff wins over the half-lifetime rule",
			now:     issuedAt.Add(400 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: issuedAt.Add(400*day - time.Minute),
				LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: issuedAt.Add(400*day - 5*time.Minute)}},
			want: Decision{RequeueAfter: 3 * time.Minute},
		},
		{
			name:    "CA recheck 12h after the CA fetch",
			now:     issuedAt.Add(13 * time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: iks, CAFetchedAt: issuedAt.Add(time.Hour)},
			want:    Decision{RefreshCA: true, Reasons: []Reason{ReasonCARecheck}, RequeueAfter: time.Minute},
		},
		{
			name:    "CA recheck when the CA fetch time is unknown",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: iks},
			want:    Decision{RefreshCA: true, Reasons: []Reason{ReasonCARecheck}, RequeueAfter: time.Minute},
		},
		{
			name:    "CA near expiry is fetched again",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: withCANotAfter(iks, issuedAt.Add(7*time.Hour)), CAFetchedAt: issuedAt.Add(58 * time.Minute)},
			want:    Decision{RefreshCA: true, Reasons: []Reason{ReasonCANearExpiry}, RequeueAfter: time.Minute},
		},
		{
			name:    "CA near expiry fetched seconds ago waits MinRequeue",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: withCANotAfter(iks, issuedAt.Add(7*time.Hour)), CAFetchedAt: issuedAt.Add(time.Hour - 30*time.Second)},
			want:    Decision{RequeueAfter: time.Minute},
		},
		{
			name:    "CA approaching its near expiry window requeues at its start",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: withCANotAfter(iks, issuedAt.Add(15*time.Hour)), CAFetchedAt: issuedAt.Add(time.Hour)},
			want:    Decision{RequeueAfter: 2 * time.Hour},
		},
		{
			// An edited annotation must not postpone the half-lifetime refresh: a future refresh-checked-at counts as
			// absent, and the caller's next write replaces it with a real time.
			name:    "refresh-checked-at in the future counts as absent",
			now:     issuedAt.Add(366 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: farFuture},
			want:    Decision{Refresh: true, Reasons: []Reason{ReasonCertificateAging}, RequeueAfter: time.Minute},
		},
		{
			name:    "401 with refresh-checked-at in the future refreshes at once",
			now:     issuedAt.Add(10 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: farFuture,
				LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: issuedAt.Add(10*day - time.Hour)}},
			want: Decision{Refresh: true, Reasons: []Reason{ReasonUnauthorized}, RequeueAfter: time.Minute},
		},
		{
			name:    "ca-fetched-at in the future counts as absent",
			now:     issuedAt.Add(time.Hour),
			desired: Desired{Server: testServer},
			current: Current{Kubeconfig: iks, CAFetchedAt: farFuture},
			want:    Decision{RefreshCA: true, Reasons: []Reason{ReasonCARecheck}, RequeueAfter: time.Minute},
		},
		{
			// UnauthorizedSince comes from the caller, not from an annotation; a future value (clock skew) counts as
			// now, which can only make the next attempt earlier, never later.
			name:    "UnauthorizedSince in the future counts as now",
			now:     issuedAt.Add(10 * day),
			desired: Desired{Server: roks.Server},
			current: Current{Kubeconfig: roks, RefreshCheckedAt: issuedAt.Add(10*day - 30*time.Second),
				LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: farFuture}},
			want: Decision{Refresh: true, Reasons: []Reason{ReasonUnauthorized}, RequeueAfter: time.Minute},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := Decide(tc.now, tc.desired, tc.current, testPolicy())
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestDecideOverTime(t *testing.T) {
	f := newFixture(t)
	roks := inspected(t, f.roksInput())
	desired := Desired{Server: roks.Server}

	t.Run("same certificate is retried after 24h and not earlier", func(t *testing.T) {
		g := NewWithT(t)
		clk := clocktesting.NewFakeClock(issuedAt.Add(366 * day))
		current := Current{Kubeconfig: roks}

		d, err := Decide(clk.Now(), desired, current, testPolicy())
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(d.Refresh).To(BeTrue())
		// The caller fetched; IBM returned the same certificate: only refresh-checked-at is written.
		firstAttempt := clk.Now()
		current.RefreshCheckedAt = firstAttempt

		for {
			clk.Step(d.RequeueAfter)
			d, err = Decide(clk.Now(), desired, current, testPolicy())
			g.Expect(err).ToNot(HaveOccurred())
			if d.Refresh {
				break
			}
			g.Expect(clk.Now().Sub(firstAttempt)).To(BeNumerically("<", 24*time.Hour))
		}
		g.Expect(clk.Now().Sub(firstAttempt)).To(Equal(24 * time.Hour))
	})

	t.Run("401 backoff grows from 1m to 10m", func(t *testing.T) {
		g := NewWithT(t)
		since := issuedAt.Add(30 * day)
		clk := clocktesting.NewFakeClock(since)
		current := Current{Kubeconfig: roks, LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: since}}

		var attempts []time.Duration
		for len(attempts) < 8 {
			d, err := Decide(clk.Now(), desired, current, testPolicy())
			g.Expect(err).ToNot(HaveOccurred())
			if d.Refresh {
				g.Expect(d.Reasons).To(Equal([]Reason{ReasonUnauthorized}))
				attempts = append(attempts, clk.Now().Sub(since))
				// IBM returned the same certificate and the stored one is still answered 401.
				current.RefreshCheckedAt = clk.Now()
			}
			clk.Step(d.RequeueAfter)
		}
		// Waits between attempts: 1m, 1m, 2m, 4m, 8m, then capped at 10m.
		g.Expect(attempts).To(Equal([]time.Duration{0, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
			16 * time.Minute, 26 * time.Minute, 36 * time.Minute}))
	})

	t.Run("future annotations never postpone a 401 refresh or the CA recheck", func(t *testing.T) {
		g := NewWithT(t)
		iks := inspected(t, f.iksInput())
		since := issuedAt.Add(time.Hour)
		current := Current{Kubeconfig: iks, RefreshCheckedAt: farFuture, CAFetchedAt: farFuture,
			LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: since}}
		for _, lasted := range []time.Duration{0, time.Hour, 30 * day, 700 * day} {
			d, err := Decide(since.Add(lasted), Desired{Server: testServer}, current, testPolicy())
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(d.Refresh).To(BeTrue(), "401 for %s", lasted)
			g.Expect(d.RefreshCA).To(BeTrue(), "401 for %s", lasted)
			g.Expect(d.Reasons).To(HaveExactElements(ReasonUnauthorized, ReasonCARecheck), "401 for %s", lasted)
			g.Expect(d.RequeueAfter).To(Equal(time.Minute))
		}
	})
}

// TestDecideIgnoresCredentialIdentity proves that no refresh decision depends on which IAM identity fetched the
// certificate: Decide has no identity input, and certificates that differ only in their subject's CN give the same
// decision.
func TestDecideIgnoresCredentialIdentity(t *testing.T) {
	t.Run("Decide has no identity parameter", func(t *testing.T) {
		g := NewWithT(t)
		// Compile-time: adding a parameter to Decide breaks this conversion. That no input type carries the certificate
		// subject is shown by behaviour below: two certificates differing only in their CN give equal Info and equal
		// decisions.
		decide := decideFunc(Decide)
		g.Expect(decide).ToNot(BeNil())
	})

	t.Run("certificates differing only in CN give the same decision", func(t *testing.T) {
		g := NewWithT(t)
		f := newFixture(t)
		notAfter := issuedAt.AddDate(2, 0, 0)
		credA := f.ca.issueClient(t, "KubeCert#<synthetic-iam-id-a>", issuedAt, notAfter)
		credB := f.ca.issueClient(t, "KubeCert#<synthetic-iam-id-b>", issuedAt, notAfter)
		inA, inB := f.iksInput(), f.iksInput()
		inA.Credential, inB.Credential = credA, credB
		infoA, infoB := inspected(t, inA), inspected(t, inB)

		// Inspect exposes nothing that tells the two apart except the fingerprint of the certificate bytes.
		g.Expect(infoA.Certificate.Fingerprint).ToNot(Equal(infoB.Certificate.Fingerprint))
		strippedA, strippedB := *infoA, *infoB
		strippedA.Certificate.Fingerprint, strippedB.Certificate.Fingerprint = "", ""
		g.Expect(strippedA).To(Equal(strippedB))

		scenarios := []struct {
			now     time.Time
			current Current
		}{
			{now: issuedAt.Add(time.Hour), current: Current{CAFetchedAt: issuedAt}},
			{now: issuedAt.Add(366 * day), current: Current{CAFetchedAt: issuedAt.Add(366 * day)}},
			{now: issuedAt.Add(366 * day), current: Current{RefreshCheckedAt: issuedAt.Add(366 * day)}},
			{now: notAfter.Add(-10 * day), current: Current{RefreshCheckedAt: notAfter.Add(-10*day - time.Hour)}},
			{now: issuedAt.Add(5 * day), current: Current{LastProbe: ProbeStatus{Unauthorized: true, UnauthorizedSince: issuedAt.Add(5*day - 3*time.Minute)},
				RefreshCheckedAt: issuedAt.Add(5*day - time.Minute)}},
		}
		for _, s := range scenarios {
			currentA, currentB := s.current, s.current
			currentA.Kubeconfig, currentB.Kubeconfig = infoA, infoB
			decisionA, errA := Decide(s.now, Desired{Server: testServer}, currentA, testPolicy())
			decisionB, errB := Decide(s.now, Desired{Server: testServer}, currentB, testPolicy())
			g.Expect(errA).ToNot(HaveOccurred())
			g.Expect(errB).ToNot(HaveOccurred())
			g.Expect(decisionA).To(Equal(decisionB))
		}
	})
}

func TestDecideRejectsInvalidInput(t *testing.T) {
	f := newFixture(t)
	iks := inspected(t, f.iksInput())
	badPolicy := testPolicy()
	badPolicy.MinRequeue = 0

	testCases := []struct {
		name    string
		now     time.Time
		desired Desired
		current Current
		policy  Policy
		wantErr string
	}{
		{name: "invalid policy", now: issuedAt, desired: Desired{Server: testServer}, policy: badPolicy, wantErr: "Policy.MinRequeue"},
		{name: "zero now", desired: Desired{Server: testServer}, policy: testPolicy(), wantErr: "now is zero"},
		{name: "empty desired server", now: issuedAt, current: Current{Kubeconfig: iks}, policy: testPolicy(), wantErr: "Desired.Server is empty"},
		{
			name: "401 without its start", now: issuedAt, desired: Desired{Server: testServer},
			current: Current{Kubeconfig: iks, LastProbe: ProbeStatus{Unauthorized: true}}, policy: testPolicy(),
			wantErr: "UnauthorizedSince is zero",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			d, err := Decide(tc.now, tc.desired, tc.current, tc.policy)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(d).To(Equal(Decision{}))
		})
	}
}

func TestPolicyValidate(t *testing.T) {
	testCases := []struct {
		name    string
		change  func(*Policy)
		wantErr string
	}{
		{name: "valid", change: func(*Policy) {}},
		{name: "zero fraction", change: func(p *Policy) { p.RefreshFraction = 0 }, wantErr: "RefreshFraction"},
		{name: "fraction of one", change: func(p *Policy) { p.RefreshFraction = 1 }, wantErr: "RefreshFraction"},
		// NaN fails every comparison, so a plain range check would accept it and refresh every certificate at once.
		{name: "NaN fraction", change: func(p *Policy) { p.RefreshFraction = math.NaN() }, wantErr: "RefreshFraction is NaN"},
		{name: "infinite fraction", change: func(p *Policy) { p.RefreshFraction = math.Inf(1) }, wantErr: "RefreshFraction"},
		{name: "zero same-certificate retry", change: func(p *Policy) { p.SameCertificateRetry = 0 }, wantErr: "Policy.SameCertificateRetry"},
		{name: "negative expiry warning", change: func(p *Policy) { p.ExpiryWarning = -time.Hour }, wantErr: "Policy.ExpiryWarning"},
		{name: "zero CA recheck", change: func(p *Policy) { p.CARecheckInterval = 0 }, wantErr: "Policy.CARecheckInterval"},
		{name: "zero rotation poll", change: func(p *Policy) { p.RotationPollInterval = 0 }, wantErr: "Policy.RotationPollInterval"},
		{name: "zero backoff min", change: func(p *Policy) { p.AuthFailureBackoffMin = 0 }, wantErr: "Policy.AuthFailureBackoffMin"},
		{name: "zero backoff max", change: func(p *Policy) { p.AuthFailureBackoffMax = 0 }, wantErr: "Policy.AuthFailureBackoffMax"},
		{name: "backoff max below min", change: func(p *Policy) { p.AuthFailureBackoffMax = time.Second }, wantErr: "is below AuthFailureBackoffMin"},
		{name: "zero max requeue", change: func(p *Policy) { p.MaxRequeue = 0 }, wantErr: "Policy.MaxRequeue"},
		{name: "max requeue below min", change: func(p *Policy) { p.MaxRequeue = time.Second }, wantErr: "is below MinRequeue"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			p := testPolicy()
			tc.change(&p)
			err := p.Validate()
			if tc.wantErr == "" {
				g.Expect(err).ToNot(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

func TestCARotationMarker(t *testing.T) {
	testCases := []struct {
		name, status, completed, want string
	}{
		{name: "no rotation ever", want: ""},
		{name: "in progress", status: rotationInProgress, want: rotationInProgress + "|"},
		{name: "completed", status: "CA certificate rotation complete", completed: "2026-11-16T05:40:08+0000",
			want: "CA certificate rotation complete|2026-11-16T05:40:08+0000"},
		{name: "date only", completed: "2026-11-16T05:40:08+0000", want: "|2026-11-16T05:40:08+0000"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(CARotationMarker(tc.status, tc.completed)).To(Equal(tc.want))
		})
	}
}
