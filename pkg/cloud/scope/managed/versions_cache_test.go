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

	t.Run("an error is returned and not cached", func(t *testing.T) {
		g := NewWithT(t)
		cache, err := NewVersionsCache(clocktesting.NewFakeClock(time.Now()), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		lister := &versionsLister{results: []error{errors.New("IBM Cloud answered HTTP 503")}}
		_, err = cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).To(MatchError("IBM Cloud answered HTTP 503"))
		versions, err := cache.Get(ctx, key, testRegion, lister.list)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(versions.Kubernetes).To(HaveLen(1))
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
