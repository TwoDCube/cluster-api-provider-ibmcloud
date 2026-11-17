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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// This file lets the managed lanes stop when the CI run loses its Boskos lease. The janitor and the account-wide
// inventory diff assume that one managed run at a time uses an e2e account, and in CI only the Boskos lease of
// hack/scripts/ci/ci-e2e.sh guarantees that. Its heartbeat (heartbeat_managed_lease in hack/scripts/ci/boskos.sh)
// retries every error that may pass; when Boskos refuses the renewal outright, the account may already be leased to
// another run, so the heartbeat writes a file and the lanes, which check it before and during every step, stop their
// steps and go straight to their cleanups, which delete the run's own cluster.

// LeaseLostFileVariable is the e2e config variable with the path of the file that heartbeat_managed_lease writes once
// Boskos refuses to renew the run's lease. ci-e2e.sh sets it for every managed run that leases its account; it is empty
// for a run without a Boskos lease, such as a local one.
const LeaseLostFileVariable = "E2E_MANAGED_LEASE_LOST_FILE"

// ErrLeaseLost is wrapped by the error of CheckLease once the heartbeat has reported the lease as lost.
var ErrLeaseLost = errors.New("the Boskos lease of this e2e run is lost")

// CheckLease reports whether the run may go on: it returns nil while path is empty (no lease to lose) or no file
// exists there. Once the heartbeat has written the file, it returns an error that wraps ErrLeaseLost and quotes the
// file, which holds Boskos's answer. A file that cannot be read is an error too, one that names the path: a lane that
// cannot tell whether it still holds its lease must not start a step either.
//
// The file's existence is the signal, and WatchLease may read it at any moment, so its writer must make it appear with
// its content in one step, as heartbeat_managed_lease does by writing a temporary file and renaming it. A file that is
// created first and written afterwards still stops the run, but the error may quote an empty answer.
//
// Parameters:
//   - path is the value of LeaseLostFileVariable.
func CheckLease(path string) error {
	if path == "" {
		return nil
	}
	content, err := os.ReadFile(filepath.Clean(path))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("checking the Boskos lease through %s=%s: %w; fix the path or its permissions", LeaseLostFileVariable,
			path, err)
	}
	return fmt.Errorf("%w (%s): %s; the account may already serve another managed run, so this run stops its steps "+
		"and only cleans up", ErrLeaseLost, path, strings.TrimSpace(string(content)))
}

// WatchLease returns a copy of parent that ends, with CheckLease's error as its cause, once CheckLease fails. It checks
// every poll in a goroutine that ends with the returned context. Gomega's Eventually and Consistently report the cause
// when their context ends, so a step stopped this way fails with Boskos's answer. With an empty path there is nothing
// to watch and no goroutine is started.
//
// Parameters:
//   - parent is the step's context.
//   - path is the value of LeaseLostFileVariable.
//   - poll is the time between two checks; it must be positive.
//
// The returned CancelFunc ends the context and the watch; call it when the step ends.
func WatchLease(parent context.Context, path string, poll time.Duration) (context.Context, context.CancelFunc, error) {
	if poll <= 0 {
		return nil, nil, fmt.Errorf("lease poll interval %s is not positive", poll)
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop := func() { cancel(nil) }
	if path == "" {
		return ctx, stop, nil
	}
	go func() {
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := CheckLease(path); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return ctx, stop, nil
}
