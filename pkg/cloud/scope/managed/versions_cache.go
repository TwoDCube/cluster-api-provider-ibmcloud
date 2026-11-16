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

// VersionsCache caches IBM's offered versions (ListVersions) for a TTL, per identity and region. Every control plane
// reconcile needs them but they change rarely, and one ListVersions per reconcile would spend IBM's rate limit for
// nothing. The key is (identity, region) because data fetched with one identity's client must never answer another
// identity's question: the region selects the client URL through the start-up --service-endpoint list, so (identity,
// region) equals the client's cache key. It is safe for concurrent use.
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

// versionsEntry is one cached answer and the time it was fetched.
type versionsEntry struct {
	versions  kubernetesservice.Versions
	fetchedAt time.Time
}

// NewVersionsCache returns an empty cache.
//
// Parameters:
//   - clk ages the entries; clock.RealClock{} in production, a fake clock in tests.
//   - ttl is how long an answer is reused; cmd/main.go passes one hour.
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
// list is returned and not cached, so the next Get asks IBM again. The result is a copy: a caller that changes it does
// not change the cache.
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
	if ok && c.clock.Since(entry.fetchedAt) < c.ttl {
		return copyVersions(entry.versions), nil
	}

	// list runs without the lock: it is an IBM call, and one slow region must not block the others. Two concurrent
	// misses both call IBM once, which is harmless.
	versions, err := list(ctx)
	if err != nil {
		return nil, err
	}
	if versions == nil {
		return nil, fmt.Errorf("managed: listing the versions of region %s returned no versions and no error", region)
	}
	stored := copyVersions(*versions)

	c.mu.Lock()
	c.entries[cacheKey] = versionsEntry{versions: *stored, fetchedAt: c.clock.Now()}
	c.mu.Unlock()
	return copyVersions(*stored), nil
}

// copyVersions returns a copy of v whose slices share nothing with v.
func copyVersions(v kubernetesservice.Versions) *kubernetesservice.Versions {
	return &kubernetesservice.Versions{
		Kubernetes: slices.Clone(v.Kubernetes),
		OpenShift:  slices.Clone(v.OpenShift),
	}
}
