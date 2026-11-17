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
	"slices"
	"sync"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"

	"k8s.io/utils/clock"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// VersionsFailureHold is how long VersionsCache answers a failed listing with the same error instead of asking IBM
// again; IBM's 429 is the exception and is held for its Retry-After (failureHold). Control plane reconciles read the
// offered versions often, so without the hold an IBM outage would be called on every status change, watch event and
// backoff retry of every control plane of the identity and region. It is shorter than steadyStatePoll (5 minutes)
// minus its 10 % jitter, so that each poll of a control plane asks IBM again: the messages that promise a retry "at
// the next poll of the control plane" stay true, and a role granted after a 403 takes effect at that poll.
const VersionsFailureHold = 4 * time.Minute

// VersionsCache caches IBM's offered versions (ListVersions) for a TTL, per identity and region. Control plane
// reconciles need them often but they change rarely, and one ListVersions per reconcile would spend IBM's rate limit
// for nothing. A failed listing is held as well, for VersionsFailureHold or, after a 429, for IBM's Retry-After, so
// that an IBM outage is not called on every reconcile either. The key is (identity, region) because data fetched with
// one identity's client must never answer another identity's question: the region selects the client URL through the
// start-up --service-endpoint list, so (identity, region) equals the client's cache key. It is safe for concurrent
// use.
type VersionsCache struct {
	clock clock.PassiveClock
	ttl   time.Duration

	mu      sync.Mutex
	entries map[versionsKey]versionsEntry
}

// versionsKey keys one cached answer.
type versionsKey struct {
	identity credentials.IdentityKey
	region   string
}

// versionsEntry is one cached answer and the time it was fetched: the versions, or the error of a failed listing and
// how long that error is held.
type versionsEntry struct {
	versions  kubernetesservice.Versions
	err       error
	hold      time.Duration
	fetchedAt time.Time
}

// NewVersionsCache returns an empty cache.
//
// Parameters:
//   - clk ages the entries; clock.RealClock{} in production, a fake clock in tests.
//   - ttl is how long an answer is reused; cmd/main.go passes one hour. A failed listing is held for
//     VersionsFailureHold, or for ttl when that is shorter, and a 429 for its Retry-After whatever ttl is (failureHold).
func NewVersionsCache(clk clock.PassiveClock, ttl time.Duration) (*VersionsCache, error) {
	if core.IsNil(clk) {
		return nil, errors.New("managed: NewVersionsCache needs a clock; pass clock.RealClock{} (a fake clock in tests)")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("managed: NewVersionsCache TTL is %s; pass a positive duration, for example 1h", ttl)
	}
	return &VersionsCache{clock: clk, ttl: ttl, entries: map[versionsKey]versionsEntry{}}, nil
}

// Get returns the cached versions of (key, region), calling list on a miss or once the entry is ttl old. An error from
// list is returned and held (failureHold): until the hold is over Get returns the same error without calling list, and
// the next call after it asks IBM again. Two errors are not held, because they say nothing about IBM: the caller's own
// context ending (cancelled or past its deadline), which must not fail the other callers, and an error Get returns for
// its own arguments. The result is a copy: a caller that changes it does not change the cache.
//
// Parameters:
//   - ctx is passed to list.
//   - key is the identity whose client list uses; it must not be the zero key.
//   - region is the IBM Cloud region of that client; it must not be empty.
//   - list fetches the versions, normally the identity's KubernetesService.ListVersions.
func (c *VersionsCache) Get(ctx context.Context, key credentials.IdentityKey, region string,
	list func(context.Context) (*kubernetesservice.Versions, error)) (*kubernetesservice.Versions, error) {
	if key.Kind == "" || region == "" || list == nil {
		return nil, fmt.Errorf("managed: VersionsCache.Get needs an identity key, a region and a list function, got key %q, region %q, list set %t",
			key, region, list != nil)
	}
	cacheKey := versionsKey{identity: key, region: region}

	c.mu.Lock()
	entry, ok := c.entries[cacheKey]
	c.mu.Unlock()
	if ok {
		age := c.clock.Since(entry.fetchedAt)
		switch {
		case entry.err != nil && age < entry.hold:
			return nil, entry.err
		case entry.err == nil && age < c.ttl:
			return copyVersions(entry.versions), nil
		}
	}

	// list runs without the lock: it is an IBM call, and one slow region must not block the others. Two concurrent
	// misses both call IBM once, which is harmless.
	versions, err := list(ctx)
	if err == nil && versions == nil {
		err = fmt.Errorf("managed: listing the versions of region %s returned no versions and no error", region)
	}
	if err != nil {
		if ctx.Err() == nil {
			c.store(cacheKey, versionsEntry{err: err, hold: c.failureHold(err), fetchedAt: c.clock.Now()})
		}
		return nil, err
	}
	stored := copyVersions(*versions)
	c.store(cacheKey, versionsEntry{versions: *stored, fetchedAt: c.clock.Now()})
	return copyVersions(*stored), nil
}

// failureHold returns how long a failed listing is held.
//
//   - IBM's 429 is held for exactly the wait RateLimitBackoff gives (its Retry-After, at least MinRateLimitBackoff),
//     also when the TTL is shorter. Retry-After is the earliest time IBM accepts the next call, so no listing may go out
//     inside it. A longer hold would cost more than it saves: the callers that need the listing (the create check and
//     the master update after a raise) requeue a 429 after this same wait (RateLimited), and each of those requeues
//     would find the 429 still held and could only requeue again, so the create or the update would wait for the end
//     of the hold instead of the end of IBM's Retry-After.
//   - Any other failure is held for VersionsFailureHold, or for the TTL when that is shorter, so that a failure is never
//     held longer than an answer.
//
// Parameters:
//   - err is the error of the failed listing.
func (c *VersionsCache) failureHold(err error) time.Duration {
	if kubernetesservice.Classify(kubernetesservice.OpListVersions, err) == kubernetesservice.ClassRateLimited {
		return kubernetesservice.RateLimitBackoff(err)
	}
	return min(VersionsFailureHold, c.ttl)
}

// store records entry as the answer of key.
//
// Parameters:
//   - key is the (identity, region) the answer belongs to.
//   - entry is the answer: the versions, or the error of a failed listing.
func (c *VersionsCache) store(key versionsKey, entry versionsEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = entry
}

// copyVersions returns a copy of v whose slices share nothing with v.
func copyVersions(v kubernetesservice.Versions) *kubernetesservice.Versions {
	return &kubernetesservice.Versions{
		Kubernetes: slices.Clone(v.Kubernetes),
		OpenShift:  slices.Clone(v.OpenShift),
	}
}
