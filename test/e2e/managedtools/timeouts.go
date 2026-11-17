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

package managedtools

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// LimitTimeout returns a copy of an e2e config interval pair whose timeout is at most remaining, so that no wait of a
// lane outlasts the lane's hard timeout. The lanes need it because the CAPI framework helpers they call (for example
// WaitForMachinePoolNodesToExist and WaitForClusterDeleted) poll with an Eventually that has no context: once the
// lane's context has ended, their requests fail at once, but they keep polling for the whole configured timeout. The
// polling interval, and any further element, is kept as it is.
//
// Parameters:
//   - intervals is an E2EConfig.GetIntervals result: the Eventually timeout first, usually followed by the polling
//     interval; each is a duration string such as "60m" or a time.Duration. It is not modified.
//   - remaining is the time left until the hard timeout (time.Until(deadline)); it must be positive, because a step
//     that starts after its lane's hard timeout must not wait at all.
func LimitTimeout(intervals []interface{}, remaining time.Duration) ([]interface{}, error) {
	if len(intervals) == 0 {
		return nil, errors.New("no interval to limit: the e2e config needs <spec>/<key> or default/<key> with a " +
			`timeout and a polling interval, for example ["10m", "10s"]`)
	}
	if remaining <= 0 {
		return nil, fmt.Errorf("the lane's hard timeout passed %s ago; no step may start another wait",
			(-remaining).Round(time.Second))
	}
	timeout, err := intervalDuration(intervals[0])
	if err != nil {
		return nil, fmt.Errorf("the interval's timeout: %w", err)
	}
	limited := slices.Clone(intervals)
	if timeout > remaining {
		limited[0] = remaining
	}
	return limited, nil
}

// ValidateWaitWithinNodeTimeout returns an error unless the timeout of an e2e config interval plus reserve fits into
// nodeTimeout. A cleanup node waits with the interval's full timeout (the cleanups run after the lane's hard timeout,
// so LimitTimeout does not lower it) and then still has work to do. Ginkgo ends the node at its NodeTimeout whatever
// the wait's own timeout, so an interval tuned longer in the e2e config would silently take the time of that work, and
// a wait that ends at the NodeTimeout leaves the work undone. The lanes check this before they create anything.
//
// Parameters:
//   - key is the interval's name, for example "wait-registry-cluster-gone"; the error names it.
//   - intervals is the E2EConfig.GetIntervals result for key: the Eventually timeout first, as for LimitTimeout.
//   - reserve is the time the node needs after the wait; it must not be negative.
//   - nodeTimeout is the node's NodeTimeout; it must be longer than reserve.
func ValidateWaitWithinNodeTimeout(key string, intervals []interface{}, reserve, nodeTimeout time.Duration) error {
	if reserve < 0 || nodeTimeout <= reserve {
		return fmt.Errorf("the cleanup that waits with interval %s needs a node timeout (%s) longer than the time it "+
			"keeps for the work after the wait (%s), and that time must not be negative", key, nodeTimeout, reserve)
	}
	if len(intervals) == 0 {
		return fmt.Errorf("the e2e config has no %s interval: set <spec>/%s or default/%s to a timeout and a polling "+
			`interval, for example ["30m", "30s"]`, key, key, key)
	}
	timeout, err := intervalDuration(intervals[0])
	if err != nil {
		return fmt.Errorf("the timeout of interval %s: %w", key, err)
	}
	if timeout+reserve > nodeTimeout {
		return fmt.Errorf("interval %s waits up to %s, but its cleanup has %s in all and keeps %s for the work after "+
			"the wait: lower the interval's timeout in the e2e config to at most %s, or raise the cleanup's node "+
			"timeout with it", key, timeout, nodeTimeout, reserve, nodeTimeout-reserve)
	}
	return nil
}

// intervalDuration returns the positive duration of one interval element, as Gomega would read it.
//
// Parameters:
//   - value is a duration string such as "60m" or a time.Duration.
func intervalDuration(value interface{}) (time.Duration, error) {
	var duration time.Duration
	switch v := value.(type) {
	case time.Duration:
		duration = v
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("%q is not a duration such as \"60m\": %w", v, err)
		}
		duration = parsed
	default:
		return 0, fmt.Errorf("%v (%T) is neither a duration string such as \"60m\" nor a time.Duration", value, value)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%v is not a positive duration", value)
	}
	return duration, nil
}
