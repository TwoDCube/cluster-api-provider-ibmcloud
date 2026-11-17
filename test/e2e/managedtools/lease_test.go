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
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestCheckLease(t *testing.T) {
	dir := t.TempDir()
	lost := filepath.Join(dir, "boskos-lease-lost")
	if err := os.WriteFile(lost, []byte("Boskos answered HTTP 409 to the renewal of capibm-vpc-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		name     string
		path     string
		wantLost bool
		wantErr  string
	}{
		{name: "no lease to lose", path: ""},
		{name: "lease held", path: filepath.Join(dir, "absent")},
		{name: "lease lost", path: lost, wantLost: true,
			wantErr: "the Boskos lease of this e2e run is lost (" + lost + "): Boskos answered HTTP 409 to the renewal of capibm-vpc-1; the account may already serve another managed run"},
		{name: "unreadable path", path: dir, wantErr: "checking the Boskos lease through " + LeaseLostFileVariable + "=" + dir},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			err := CheckLease(tc.path)
			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(errors.Is(err, ErrLeaseLost)).To(Equal(tc.wantLost))
		})
	}
}

// writeLostLease writes the lost-lease file at path with content the way heartbeat_managed_lease does: into a
// temporary file next to it, which is then renamed over path. CheckLease takes the file's existence as the signal and
// quotes its content, so it relies on the file appearing with its content in one step. os.WriteFile creates the file
// empty before it writes, and a watch that polls in between would end the step quoting an empty answer.
func writeLostLease(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func TestWatchLeaseEndsTheStepOnceTheLeaseIsLost(t *testing.T) {
	g := NewWithT(t)
	lost := filepath.Join(t.TempDir(), "boskos-lease-lost")
	ctx, stop, err := WatchLease(context.Background(), lost, 10*time.Millisecond)
	g.Expect(err).NotTo(HaveOccurred())
	defer stop()
	g.Consistently(ctx.Done()).WithTimeout(50 * time.Millisecond).ShouldNot(BeClosed())

	writeLostLease(t, lost, "Boskos answered HTTP 401\n")
	g.Eventually(ctx.Done()).WithTimeout(5 * time.Second).Should(BeClosed())
	g.Expect(context.Cause(ctx)).To(MatchError(ErrLeaseLost))
	g.Expect(context.Cause(ctx)).To(MatchError(ContainSubstring("Boskos answered HTTP 401")))
}

func TestWatchLeaseFollowsItsParentAndItsStop(t *testing.T) {
	t.Run("parent ends", func(t *testing.T) {
		g := NewWithT(t)
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, stop, err := WatchLease(parent, filepath.Join(t.TempDir(), "absent"), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		defer stop()
		cancelParent()
		g.Eventually(ctx.Done()).Should(BeClosed())
		g.Expect(context.Cause(ctx)).To(MatchError(context.Canceled))
	})
	t.Run("stopped", func(t *testing.T) {
		g := NewWithT(t)
		ctx, stop, err := WatchLease(context.Background(), filepath.Join(t.TempDir(), "absent"), time.Hour)
		g.Expect(err).NotTo(HaveOccurred())
		stop()
		g.Eventually(ctx.Done()).Should(BeClosed())
		g.Expect(context.Cause(ctx)).To(MatchError(context.Canceled))
	})
	t.Run("no lease to watch", func(t *testing.T) {
		g := NewWithT(t)
		ctx, stop, err := WatchLease(context.Background(), "", time.Millisecond)
		g.Expect(err).NotTo(HaveOccurred())
		g.Consistently(ctx.Done()).WithTimeout(20 * time.Millisecond).ShouldNot(BeClosed())
		stop()
		g.Eventually(ctx.Done()).Should(BeClosed())
	})
}

func TestWatchLeaseRejectsANonPositivePoll(t *testing.T) {
	g := NewWithT(t)
	_, _, err := WatchLease(context.Background(), "lost", 0)
	g.Expect(err).To(MatchError("lease poll interval 0s is not positive"))
}

// heartbeatRun is what one run of heartbeat_managed_lease (hack/scripts/ci/boskos.sh) against stubbed curl and sleep
// did.
type heartbeatRun struct {
	// exitCode is the shell's exit status, which is the function's.
	exitCode int
	// stderr is the shell's standard error.
	stderr string
	// curlArgs are the arguments of each curl call, one line per call.
	curlArgs []string
	// sleeps are the arguments of each sleep call, in seconds.
	sleeps []int
	// lostFile is the content of the file the function writes once Boskos refuses the renewal; "" when absent.
	lostFile string
	// tmpLeft reports whether the temporary file of that write was left behind.
	tmpLeft bool
}

// runHeartbeat runs heartbeat_managed_lease in bash with errexit, as ci-e2e.sh's background shell does, with curl
// and sleep replaced by shell functions. Each response is "<HTTP code> <curl exit status>", the stub's answer to one
// call, in order; once they are used up the stub answers 404, which ends the loop, so a run that calls curl more often
// than expected shows in the call count instead of hanging.
//
// Parameters:
//   - t is the test.
//   - responses are the stub's answers.
//   - args are the function's arguments; the first "$LOST" is replaced by the path of the lost-lease file.
func runHeartbeat(t *testing.T, responses []string, args ...string) heartbeatRun {
	t.Helper()
	bash, boskos := bashAndBoskos(t)
	stub := t.TempDir()
	lost := filepath.Join(stub, "boskos-lease-lost")
	for name, content := range map[string]string{
		"count":     "0\n",
		"responses": strings.Join(responses, "\n") + "\n",
		"curl-args": "",
		"sleeps":    "",
	} {
		if err := os.WriteFile(filepath.Join(stub, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for i, arg := range args {
		if arg == "$LOST" {
			args[i] = lost
		}
	}
	const script = `
set -o errexit -o nounset -o pipefail
source "${BOSKOS_SH}"
curl(){
    local n response
    n=$(( $(cat "${STUB}/count") + 1 ))
    echo "${n}" > "${STUB}/count"
    printf '%s\n' "$*" >> "${STUB}/curl-args"
    response=$(sed -n "${n}p" "${STUB}/responses")
    if [[ -z "${response}" ]]; then
        printf '404'
        return 0
    fi
    printf '%s' "${response%% *}"
    return "${response##* }"
}
sleep(){
    printf '%s\n' "$1" >> "${STUB}/sleeps"
}
heartbeat_managed_lease "$@"
`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// bash comes from PATH, the script is the constant above and the arguments are the test's own.
	cmd := exec.CommandContext(ctx, bash, append([]string{"-c", script, "heartbeat"}, args...)...) //nolint:gosec // G204: see above
	cmd.Env = append(os.Environ(), "BOSKOS_SH="+boskos, "STUB="+stub, "BOSKOS_HOST=boskos.test",
		"BOSKOS_RESOURCE_NAME=capibm-vpc-1", "USER=e2e-job")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatalf("running heartbeat_managed_lease: %v (stderr: %s)", err, stderr.String())
	}
	if ctx.Err() != nil {
		t.Fatalf("heartbeat_managed_lease did not return within 30 s (stderr: %s)", stderr.String())
	}
	run := heartbeatRun{exitCode: cmd.ProcessState.ExitCode(), stderr: stderr.String()}
	run.curlArgs = readLines(t, filepath.Join(stub, "curl-args"))
	for _, line := range readLines(t, filepath.Join(stub, "sleeps")) {
		seconds, err := strconv.Atoi(line)
		if err != nil {
			t.Fatalf("sleep was called with %q, not a number of seconds", line)
		}
		run.sleeps = append(run.sleeps, seconds)
	}
	if content, err := os.ReadFile(filepath.Clean(lost)); err == nil {
		run.lostFile = string(content)
	}
	_, err = os.Stat(lost + ".tmp")
	run.tmpLeft = err == nil
	return run
}

// bashAndBoskos returns the path of bash and the absolute path of hack/scripts/ci/boskos.sh, which the tests source.
func bashAndBoskos(t *testing.T) (string, string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatalf("the Boskos script tests need bash, which runs hack/scripts/ci/boskos.sh in CI: %v", err)
	}
	boskos, err := filepath.Abs(filepath.Join("..", "..", "..", "hack", "scripts", "ci", "boskos.sh"))
	if err != nil {
		t.Fatal(err)
	}
	return bash, boskos
}

// readLines returns the non-empty lines of a file.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	content, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, line := range strings.Split(string(content), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestManagedHeartbeatRetriesWhatMayPass is the reproducer of a lease that a single failed renewal used to give up: a
// connection error ended the heartbeat for good, the Boskos reaper then reclaimed the lease, and a second managed run
// could get the account while this one still owned clusters there.
func TestManagedHeartbeatRetriesWhatMayPass(t *testing.T) {
	g := NewWithT(t)
	run := runHeartbeat(t, []string{
		"000 7",  // connection refused
		"000 28", // --max-time reached
		"503 0",
		"429 0",
		"408 0",
		"200 0",
		"502 0",
		"200 0",
		"409 0", // the lease was reclaimed meanwhile
	}, "$LOST")
	g.Expect(run.curlArgs).To(HaveLen(9), "curl calls; stderr: %s", run.stderr)
	for _, args := range run.curlArgs {
		g.Expect(args).To(ContainSubstring("--max-time 30"), "every renewal is bounded, so a hung request cannot stop the heartbeat")
		g.Expect(args).To(HaveSuffix("-X POST http://boskos.test/update?name=capibm-vpc-1&state=busy&owner=e2e-job"))
	}
	// Retries back off from 5 s up to the 60 s cadence, and a renewal resets the backoff.
	g.Expect(run.sleeps).To(Equal([]int{5, 10, 20, 40, 60, 60, 5, 60}))
	g.Expect(run.exitCode).To(Equal(1), "the loop ends only on the refusal")
	g.Expect(run.lostFile).To(HavePrefix("Boskos answered HTTP 409 to the renewal of capibm-vpc-1 at "))
	g.Expect(run.tmpLeft).To(BeFalse())
}

func TestManagedHeartbeatCapsTheBackoffAtTheCadence(t *testing.T) {
	g := NewWithT(t)
	run := runHeartbeat(t, []string{"500 0", "500 0", "500 0", "500 0", "500 0", "500 0", "500 0", "403 0"}, "$LOST")
	g.Expect(run.curlArgs).To(HaveLen(8), "curl calls; stderr: %s", run.stderr)
	g.Expect(run.sleeps).To(Equal([]int{5, 10, 20, 40, 60, 60, 60}))
	g.Expect(run.lostFile).To(ContainSubstring("HTTP 403"))
}

func TestManagedHeartbeatStopsWhenBoskosRefusesTheRenewal(t *testing.T) {
	for _, code := range []string{"400", "401", "403", "404", "409"} {
		t.Run(code, func(t *testing.T) {
			g := NewWithT(t)
			run := runHeartbeat(t, []string{code + " 0"}, "$LOST")
			g.Expect(run.curlArgs).To(HaveLen(1), "curl calls; stderr: %s", run.stderr)
			g.Expect(run.sleeps).To(BeEmpty())
			g.Expect(run.exitCode).To(Equal(1))
			g.Expect(run.lostFile).To(HavePrefix("Boskos answered HTTP " + code + " to the renewal of capibm-vpc-1 at "))
			g.Expect(run.tmpLeft).To(BeFalse())
		})
	}
}

func TestManagedHeartbeatNeedsTheLostLeaseFile(t *testing.T) {
	g := NewWithT(t)
	run := runHeartbeat(t, []string{"200 0"})
	g.Expect(run.exitCode).NotTo(Equal(0))
	g.Expect(run.stderr).To(ContainSubstring("heartbeat_managed_lease needs the file to write when Boskos refuses the renewal"))
	g.Expect(run.curlArgs).To(BeEmpty(), "no renewal without a way to report its refusal")
}

// TestReportLostManagedLease checks the EXIT trap of ci-e2e.sh: a job whose lease Boskos refused to renew fails even
// when every spec passed, and a job that failed anyway keeps its own exit status.
func TestReportLostManagedLease(t *testing.T) {
	bash, boskos := bashAndBoskos(t)
	dir := t.TempDir()
	lost := filepath.Join(dir, "boskos-lease-lost")
	if err := os.WriteFile(lost, []byte("Boskos answered HTTP 409 to the renewal of capibm-vpc-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	const script = `
set -o errexit -o nounset -o pipefail
source "${BOSKOS_SH}"
trap 'report_lost_managed_lease "$?" "${LOST}"' EXIT
exit "${STATUS}"
`
	testCases := []struct {
		name       string
		lost       string
		status     string
		wantStatus int
		wantStderr string
	}{
		{name: "no lease", lost: "", status: "0", wantStatus: 0},
		{name: "lease held", lost: filepath.Join(dir, "absent"), status: "0", wantStatus: 0},
		{name: "lease lost, specs passed", lost: lost, status: "0", wantStatus: 1,
			wantStderr: "managed e2e: the Boskos lease of capibm-vpc-1 was lost during the run: Boskos answered HTTP 409 to the renewal of capibm-vpc-1. The lanes stopped their steps"},
		{name: "lease lost, job failed anyway", lost: lost, status: "2", wantStatus: 2,
			wantStderr: "the Boskos lease of capibm-vpc-1 was lost during the run"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bash, "-c", script)
			cmd.Env = append(os.Environ(), "BOSKOS_SH="+boskos, "LOST="+tc.lost, "STATUS="+tc.status,
				"BOSKOS_RESOURCE_NAME=capibm-vpc-1")
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			err := cmd.Run()
			var exitErr *exec.ExitError
			if err != nil && !errors.As(err, &exitErr) {
				t.Fatalf("running report_lost_managed_lease: %v (stderr: %s)", err, stderr.String())
			}
			g.Expect(cmd.ProcessState.ExitCode()).To(Equal(tc.wantStatus), "stderr: %s", stderr.String())
			if tc.wantStderr == "" {
				g.Expect(stderr.String()).To(BeEmpty())
				return
			}
			g.Expect(stderr.String()).To(ContainSubstring(tc.wantStderr))
		})
	}
}
