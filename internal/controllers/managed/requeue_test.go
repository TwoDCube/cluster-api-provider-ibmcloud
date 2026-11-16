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
	"testing"
	"time"

	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"

	. "github.com/onsi/gomega"
)

// samples is how often the jitter tests draw; enough to make a bound violation visible.
const samples = 2000

func TestRequeueInterval(t *testing.T) {
	// Every named interval and its value before jitter.
	testCases := []struct {
		interval managedscope.Interval
		want     time.Duration
	}{
		{interval: managedscope.IntervalClusterProvisioning, want: time.Minute},
		{interval: managedscope.IntervalMasterUpdate, want: 2 * time.Minute},
		{interval: managedscope.IntervalPoolTransition, want: 30 * time.Second},
		{interval: managedscope.IntervalDrainRetry, want: 20 * time.Second},
		{interval: managedscope.IntervalSteadyState, want: 5 * time.Minute},
		{interval: managedscope.IntervalCARotation, want: time.Minute},
		{interval: managedscope.IntervalGuarded, want: 2 * time.Minute},
		{interval: managedscope.IntervalDelete, want: time.Minute},
		{interval: managedscope.IntervalTerminalRecheck, want: 15 * time.Minute},
		{interval: managedscope.IntervalInfraRevalidate, want: 30 * time.Minute},
	}
	for _, tc := range testCases {
		t.Run(string(tc.interval), func(t *testing.T) {
			g := NewWithT(t)
			got, err := requeueInterval(tc.interval)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}

	t.Run("unknown interval", func(t *testing.T) {
		g := NewWithT(t)
		_, err := requeueInterval("Sometimes")
		g.Expect(err).To(MatchError(ContainSubstring(`unknown requeue interval "Sometimes"`)))
	})
}

func TestJitterBounds(t *testing.T) {
	g := NewWithT(t)
	base := 30 * time.Minute
	low, high := base*9/10, base*11/10
	var below, above bool
	for range samples {
		got := jitter(base)
		g.Expect(got).To(BeNumerically(">=", low))
		g.Expect(got).To(BeNumerically("<", high))
		below = below || got < base
		above = above || got > base
	}
	// ±10 %: both sides of the base are used, so restarted controllers spread out instead of only drifting later.
	g.Expect(below).To(BeTrue())
	g.Expect(above).To(BeTrue())

	for range samples {
		got := jitterUp(base)
		g.Expect(got).To(BeNumerically(">=", base))
		g.Expect(got).To(BeNumerically("<", high))
	}
}

func TestResultFor(t *testing.T) {
	failure := errors.New("IBM Cloud answered HTTP 503")
	testCases := []struct {
		name     string
		outcome  managedscope.Outcome
		wantBase time.Duration
		wantUp   bool // the jitter only adds
		wantErr  string
	}{
		{name: "Done requeues at the controller's steady interval", outcome: managedscope.Done(), wantBase: 30 * time.Minute},
		{
			name:     "NotDone requeues at its interval",
			outcome:  managedscope.NotDone(managedscope.IntervalClusterProvisioning, "Creating"),
			wantBase: time.Minute,
		},
		{name: "Terminal rechecks after fifteen minutes", outcome: managedscope.Terminal("SubnetNotFound", "fix it"), wantBase: 15 * time.Minute},
		{name: "RateLimited never requeues earlier than its backoff", outcome: managedscope.RateLimited(0), wantBase: 30 * time.Second, wantUp: true},
		{name: "Transient returns its error", outcome: managedscope.Transient(failure), wantErr: failure.Error()},
		{name: "unknown outcome kind", outcome: managedscope.Outcome{Kind: "Maybe"}, wantErr: `unknown outcome kind "Maybe"`},
		{name: "unknown interval", outcome: managedscope.NotDone("Sometimes", "x"), wantErr: `unknown requeue interval "Sometimes"`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			result, err := resultFor(tc.outcome, managedscope.IntervalInfraRevalidate)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(result.RequeueAfter).To(BeZero())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			low := tc.wantBase * 9 / 10
			if tc.wantUp {
				low = tc.wantBase
			}
			g.Expect(result.RequeueAfter).To(BeNumerically(">=", low))
			g.Expect(result.RequeueAfter).To(BeNumerically("<", tc.wantBase*11/10))
		})
	}
}
