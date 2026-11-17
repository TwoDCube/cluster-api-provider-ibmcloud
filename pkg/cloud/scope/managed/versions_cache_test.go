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
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

func TestNewVersionsCacheValidation(t *testing.T) {
	testCases := []struct {
		name    string
		clock   clock.PassiveClock
		ttl     time.Duration
		wantErr string
	}{
		{name: "valid", clock: clocktesting.NewFakeClock(time.Now()), ttl: time.Hour},
		{name: "nil clock", clock: nil, ttl: time.Hour, wantErr: "needs a clock"},
		{name: "typed nil clock", clock: (*clocktesting.FakeClock)(nil), ttl: time.Hour, wantErr: "needs a clock"},
		{name: "zero TTL", clock: clocktesting.NewFakeClock(time.Now()), ttl: 0, wantErr: "TTL is 0s"},
		{name: "negative TTL", clock: clocktesting.NewFakeClock(time.Now()), ttl: -time.Minute, wantErr: "TTL is -1m0s"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			cache, err := NewVersionsCache(tc.clock, tc.ttl)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(cache).To(BeNil())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cache).NotTo(BeNil())
		})
	}
}

// versionsLister counts its calls and answers the scripted results in order.
type versionsLister struct {
	calls   int
	results []error // nil: answer one version whose patch is the call number
}

// list implements the list function of VersionsCache.Get.
func (l *versionsLister) list(context.Context) (*kubernetesservice.Versions, error) {
	l.calls++
	if len(l.results) >= l.calls && l.results[l.calls-1] != nil {
		return nil, l.results[l.calls-1]
	}
	return &kubernetesservice.Versions{Kubernetes: []kubernetesservice.ReleaseVersion{{Major: 1, Minor: 36, Patch: l.calls}}}, nil
}

func TestVersionsCacheGet(t *testing.T) {
	ctx := context.Background()
	key := credentials.ControllerKey()

	t.Run("a hit within the TTL reuses the answer, an aged entry is fetched again", func(t *testing.T) {
		g := NewWithT(t)
		clk := clocktesting.NewFakeClock(time.Now())
		cache, err := NewVersionsCache(clk, time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{}

		first, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(first.Kubernetes[0].Patch).To(Equal(1))
		clk.Step(59 * time.Minute)
		second, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(second.Kubernetes[0].Patch).To(Equal(1))
		g.Expect(lister.calls).To(Equal(1))

		clk.Step(time.Minute)
		third, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(third.Kubernetes[0].Patch).To(Equal(2))
		g.Expect(lister.calls).To(Equal(2))
	})

	t.Run("regions are cached apart", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{}
		_, err = cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		_, err = cache.Get(ctx, key, "eu-de", lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(lister.calls).To(Equal(2))
	})

	t.Run("a nil answer without error is an error", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		_, err = cache.Get(ctx, key, testRegion, func(context.Context) (*kubernetesservice.Versions, error) { return nil, nil })
		g.Expect(err).To(MatchError(ContainSubstring("returned no versions and no error")))
	})

	t.Run("callers cannot change the cached answer", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{}
		first, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		first.Kubernetes[0].Minor = 99
		second, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(second.Kubernetes[0].Minor).To(Equal(36))
	})

	t.Run("invalid arguments are named", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{}
		_, err = cache.Get(ctx, credentials.IdentityKey{}, testRegion, lister.list)
		g.Expect(err).To(MatchError(ContainSubstring("needs an identity key, a region and a list function")))
		_, err = cache.Get(ctx, key, "", lister.list)
		g.Expect(err).To(MatchError(ContainSubstring(`region ""`)))
		_, err = cache.Get(ctx, key, testRegion, nil)
		g.Expect(err).To(MatchError(ContainSubstring("list set false")))
		g.Expect(lister.calls).To(BeZero())
	})
}

// TestVersionsCacheFailureHold checks that a failed listing is answered from the cache for VersionsFailureHold, and a
// 429 for exactly its Retry-After, so that an IBM outage is not called on every reconcile, and that IBM is asked again
// once the hold is over.
func TestVersionsCacheFailureHold(t *testing.T) {
	ctx := context.Background()
	key := credentials.ControllerKey()
	outage := errors.New("IBM Cloud answered HTTP 503")
	rateLimited := func(retryAfter time.Duration) error {
		return &kubernetesservice.APIError{Operation: kubernetesservice.OpListVersions, StatusCode: http.StatusTooManyRequests,
			RetryAfter: retryAfter}
	}
	testCases := []struct {
		name string
		// ttl is the cache's TTL.
		ttl time.Duration
		// results are the lister's answers in order; nil answers versions.
		results []error
		// steps are the clock steps before each Get after the first.
		steps []time.Duration
		// wantErrs are the errors of the Gets in order; nil means versions.
		wantErrs []error
		// wantCalls is how often IBM was asked in all.
		wantCalls int
	}{
		{name: "a failure is held until the hold is over, then IBM is asked again", ttl: time.Hour,
			results: []error{outage}, steps: []time.Duration{time.Second, VersionsFailureHold - 2*time.Second, time.Second},
			wantErrs: []error{outage, outage, outage, nil}, wantCalls: 2},
		{name: "a failure after an aged answer is held too, the aged answer is not served", ttl: time.Hour,
			results: []error{nil, outage}, steps: []time.Duration{time.Hour, time.Minute},
			wantErrs: []error{nil, outage, outage}, wantCalls: 2},
		{name: "a failure is held no longer than a TTL shorter than the hold", ttl: time.Minute,
			results: []error{outage}, steps: []time.Duration{59 * time.Second, time.Second},
			wantErrs: []error{outage, outage, nil}, wantCalls: 2},
		{name: "a success after the hold replaces the failure for the TTL", ttl: time.Hour,
			results: []error{outage}, steps: []time.Duration{VersionsFailureHold, 59 * time.Minute},
			wantErrs: []error{outage, nil, nil}, wantCalls: 2},
		{name: "a 429 is held until its Retry-After when that is longer than VersionsFailureHold", ttl: time.Hour,
			results: []error{rateLimited(10 * time.Minute)}, steps: []time.Duration{VersionsFailureHold, 6*time.Minute - time.Second, time.Second},
			wantErrs:  []error{rateLimited(10 * time.Minute), rateLimited(10 * time.Minute), rateLimited(10 * time.Minute), nil},
			wantCalls: 2},
		// The callers requeue a 429 after its Retry-After; a hold longer than that would answer each of those requeues
		// with the same 429.
		{name: "a 429 with a Retry-After shorter than VersionsFailureHold is held only until its Retry-After", ttl: time.Hour,
			results: []error{rateLimited(time.Minute)}, steps: []time.Duration{time.Minute - time.Second, time.Second},
			wantErrs: []error{rateLimited(time.Minute), rateLimited(time.Minute), nil}, wantCalls: 2},
		{name: "a 429 without a Retry-After is held for the minimum rate-limit backoff", ttl: time.Hour,
			results:  []error{rateLimited(0)},
			steps:    []time.Duration{kubernetesservice.MinRateLimitBackoff - time.Second, time.Second},
			wantErrs: []error{rateLimited(0), rateLimited(0), nil}, wantCalls: 2},
		{name: "a 429 with a Retry-After below the minimum rate-limit backoff is held for the minimum", ttl: time.Hour,
			results:  []error{rateLimited(5 * time.Second)},
			steps:    []time.Duration{kubernetesservice.MinRateLimitBackoff - time.Second, time.Second},
			wantErrs: []error{rateLimited(5 * time.Second), rateLimited(5 * time.Second), nil}, wantCalls: 2},
		{name: "a 429's Retry-After wins over a shorter TTL", ttl: time.Minute,
			results: []error{rateLimited(3 * time.Minute)}, steps: []time.Duration{3*time.Minute - time.Second, time.Second},
			wantErrs: []error{rateLimited(3 * time.Minute), rateLimited(3 * time.Minute), nil}, wantCalls: 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			clk := clocktesting.NewFakeClock(time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC))
			cache, err := NewVersionsCache(clk, tc.ttl)
			g.Expect(err).NotTo(HaveOccurred())
			lister := &versionsLister{results: tc.results}
			for i, wantErr := range tc.wantErrs {
				if i > 0 {
					clk.Step(tc.steps[i-1])
				}
				versions, err := cache.Get(ctx, key, testRegion, lister.list)
				if wantErr != nil {
					g.Expect(err).To(MatchError(wantErr), "Get %d", i)
					g.Expect(versions).To(BeNil())
					continue
				}
				g.Expect(err).NotTo(HaveOccurred(), "Get %d", i)
				g.Expect(versions.Kubernetes).To(HaveLen(1))
			}
			g.Expect(lister.calls).To(Equal(tc.wantCalls))
		})
	}

	t.Run("a nil answer without error is held like a failure", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		calls := 0
		empty := func(context.Context) (*kubernetesservice.Versions, error) { calls++; return nil, nil }
		for range 2 {
			_, err = cache.Get(ctx, key, testRegion, empty)
			g.Expect(err).To(MatchError(ContainSubstring("returned no versions and no error")))
		}
		g.Expect(calls).To(Equal(1))
	})

	t.Run("a failure is held per identity and region", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{results: []error{outage}}
		_, err = cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).To(MatchError(outage))
		_, err = cache.Get(ctx, key, "eu-de", lister.list)
		g.Expect(err).NotTo(HaveOccurred(), "another region is asked")
		_, err = cache.Get(ctx, credentials.IdentityKey{Kind: "TestIdentity", Name: "a"}, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred(), "another identity is asked")
		g.Expect(lister.calls).To(Equal(3))
	})

	t.Run("the end of the caller's context is not held", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = cache.Get(cancelled, key, testRegion, func(ctx context.Context) (*kubernetesservice.Versions, error) {
			lister.calls++
			return nil, ctx.Err()
		})
		g.Expect(err).To(MatchError(context.Canceled))
		versions, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred(), "the next caller asks IBM")
		g.Expect(versions.Kubernetes).To(HaveLen(1))
		g.Expect(lister.calls).To(Equal(2))
	})
}

// TestVersionsCacheConcurrentGet locks the locking of VersionsCache: every control plane reconcile calls Get, and
// list runs outside the lock. Run it with go test -race. Each caller writes to the copy it got, so a Get that handed
// out the cached slices would race with the other callers.
func TestVersionsCacheConcurrentGet(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	clk := clocktesting.NewFakeClock(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	cache, err := NewVersionsCache(clk, time.Hour)
	g.Expect(err).NotTo(HaveOccurred())
	var calls atomic.Int64
	list := func(ctx context.Context) (*kubernetesservice.Versions, error) {
		calls.Add(1)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return &kubernetesservice.Versions{Kubernetes: []kubernetesservice.ReleaseVersion{{Major: 1, Minor: 36, Patch: 3}}}, nil
	}
	keys := []credentials.IdentityKey{credentials.ControllerKey(), {Kind: "TestIdentity", Name: "a"}}
	regions := []string{testRegion, "eu-de"}

	var (
		mu   sync.Mutex
		errs []error
	)
	get := func(key credentials.IdentityKey, region string) bool {
		versions, err := cache.Get(ctx, key, region, list)
		if err == nil && (len(versions.Kubernetes) != 1 || versions.Kubernetes[0].Minor != 36) {
			err = fmt.Errorf("Get(%s, %s) returned %+v, not the listed versions", key, region, versions)
		}
		if err != nil {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
			return false
		}
		versions.Kubernetes[0].Minor = 99 // the caller owns its copy
		return true
	}

	// The workers run until the stepper is done. The stepper moves the clock 20 minutes at a time, 200 minutes in
	// all, and gets every (key, region) pair after each step.
	const workers, minRounds, steps = 8, 50, 10
	var stepping atomic.Bool
	stepping.Store(true)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for round := 0; round < minRounds || stepping.Load(); round++ {
				if !get(keys[(worker+round)%len(keys)], regions[round%len(regions)]) {
					return
				}
			}
		})
	}
	wg.Go(func() {
		defer stepping.Store(false)
		for range steps {
			clk.Step(20 * time.Minute)
			for _, key := range keys {
				for _, region := range regions {
					if !get(key, region) {
						return
					}
				}
			}
		}
	})
	wg.Wait()

	g.Expect(errs).To(BeEmpty())
	// An answer lives one hour and the stepper asks every 20 minutes, so each of the four pairs is listed at least at
	// 0-20, 60-100 and 120-180 minutes: three times in 200 minutes. Concurrent misses may list more often.
	g.Expect(calls.Load()).To(BeNumerically(">=", 3*len(keys)*len(regions)))
}
