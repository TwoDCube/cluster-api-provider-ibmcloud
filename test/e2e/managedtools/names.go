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
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"regexp"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmclustername"
)

// The name contract of the managed e2e lanes: every cluster is named <prefix>-<run-id>-<lane>, for example
// capibm-e2e-k3x9q2-iks, or capibm-e2e-periodics-x7k-iks with E2E_NAME_PREFIX=capibm-e2e-periodics.
// The lanes do not set spec.ibmCloudClusterName, so the whole name is the IBM cluster name and must follow IBM's rule:
// at most MaxClusterNameLength (32) characters, a letter first.
const (
	// DefaultNamePrefix is the prefix when E2E_NAME_PREFIX is not set.
	DefaultNamePrefix = "capibm-e2e"

	// DefaultRunIDLength is the length of the random run ID when E2E_RUN_ID is not set. With DefaultNamePrefix the
	// unique part "capibm-e2e-<6 characters>" is 17 characters, well inside UniqueNameLength.
	DefaultRunIDLength = 6

	// UniqueNameLength is how many leading characters of a cluster name must already be unique to the run, so
	// "<prefix>-<run-id>" must fit into it. IBM builds the Ingress subdomain from the start of the cluster name, and
	// re-creating the same leading characters five times in seven days hits the Let's Encrypt rate limit for that
	// subdomain (IBMCloudManagedControlPlane spec.ibmCloudClusterName documentation).
	UniqueNameLength = 24

	// MaxClusterNameLength is IBM's limit for a cluster name: IBM refuses createCluster for a longer name (HTTP 400,
	// code E0125). The lanes leave spec.ibmCloudClusterName unset, so the cluster name is the IBM name.
	MaxClusterNameLength = ibmclustername.MaxLength

	// minNamePrefixLength keeps the janitor's prefix from matching far more than e2e clusters: a one- or
	// two-character prefix such as "c" would select "c-anything".
	minNamePrefixLength = 3

	// runIDAlphabet is the alphabet of generated run IDs: lowercase letters and digits, valid in a Kubernetes object
	// name and in an IBM cluster name.
	runIDAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// Lane names the e2e lane a cluster belongs to; it is the last segment of the cluster name.
type Lane string

const (
	// LaneIKS is the IKS lane (managed-iks flavor).
	LaneIKS Lane = "iks"
	// LaneROKS is the ROKS lane (managed-roks-single-zone flavor).
	LaneROKS Lane = "roks"
)

var (
	// namePrefixPattern: a lowercase letter first (IBM requires a letter; a Kubernetes object name requires lowercase),
	// then lowercase letters, digits and hyphens, and no trailing hyphen, because the contract adds one itself.
	namePrefixPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)
	// runIDPattern: lowercase letters and digits only, so that the run ID never contains the separator.
	runIDPattern = regexp.MustCompile(`^[a-z0-9]+$`)
)

// ValidateNamePrefix returns an error unless prefix can start e2e cluster names and select them in the janitor.
//
// Parameters:
//   - prefix is E2E_NAME_PREFIX, for example "capibm-e2e" or "capibm-e2e-periodics".
func ValidateNamePrefix(prefix string) error {
	if len(prefix) < minNamePrefixLength || !namePrefixPattern.MatchString(prefix) {
		return fmt.Errorf("E2E_NAME_PREFIX %q is not a valid cluster name prefix: use at least %d characters, start "+
			"with a lowercase letter, then lowercase letters, digits and hyphens, and do not end with a hyphen "+
			"(for example %q)", prefix, minNamePrefixLength, DefaultNamePrefix)
	}
	return nil
}

// RunNamePrefix returns "<prefix>-<run-id>-", the part every cluster name of one run starts with. It validates the
// prefix, the run ID and the contract that "<prefix>-<run-id>" fits into the first UniqueNameLength characters.
//
// Parameters:
//   - prefix is E2E_NAME_PREFIX (see ValidateNamePrefix).
//   - runID is E2E_RUN_ID, or the ID NewRunID generated.
func RunNamePrefix(prefix, runID string) (string, error) {
	if err := ValidateNamePrefix(prefix); err != nil {
		return "", err
	}
	if !runIDPattern.MatchString(runID) {
		return "", fmt.Errorf("E2E_RUN_ID %q is not a valid run ID: use lowercase letters and digits only, for "+
			"example %q (leave it unset for a random %d-character ID)", runID, "x7k", DefaultRunIDLength)
	}
	unique := prefix + "-" + runID
	if len(unique) > UniqueNameLength {
		return "", fmt.Errorf("%q (E2E_NAME_PREFIX %q, E2E_RUN_ID %q) has %d characters; it must fit into the first "+
			"%d characters of the cluster name, which IBM uses for the Ingress subdomain: shorten E2E_NAME_PREFIX or "+
			"E2E_RUN_ID by %d characters", unique, prefix, runID, len(unique), UniqueNameLength, len(unique)-UniqueNameLength)
	}
	return unique + "-", nil
}

// ClusterName returns the name of the lane's cluster, "<prefix>-<run-id>-<lane>", after checking the whole name
// contract: valid prefix and run ID, "<prefix>-<run-id>" within the first UniqueNameLength characters, and the full
// name a valid IBM cluster name of at most MaxClusterNameLength characters. The lane calls it first in its set-up,
// before the janitor or any other IBM call, so a name IBM would refuse fails the lane with this error at once instead
// of an InvalidConfiguration (or an IBM refusal) after the cluster objects were applied.
//
// Parameters:
//   - prefix is E2E_NAME_PREFIX (see ValidateNamePrefix).
//   - runID is E2E_RUN_ID, or the ID NewRunID generated.
//   - lane is LaneIKS or LaneROKS.
func ClusterName(prefix, runID string, lane Lane) (string, error) {
	if lane != LaneIKS && lane != LaneROKS {
		return "", fmt.Errorf("lane %q is not an e2e lane: use %q or %q", lane, LaneIKS, LaneROKS)
	}
	runPrefix, err := RunNamePrefix(prefix, runID)
	if err != nil {
		return "", err
	}
	name := runPrefix + string(lane)
	if err := validateIBMClusterName(name); err != nil {
		return "", err
	}
	return name, nil
}

// validateIBMClusterName returns an error when name breaks IBM's cluster name rule (ibmclustername.Problem), the rule
// CAPIBM's admission webhook and controller apply to the same name. With today's lanes it cannot fail after
// RunNamePrefix passed: the prefix and run ID patterns allow only valid characters, and the longest name is
// 24 + len("-roks") = 29 characters. It keeps the contract explicit for a lane with a longer name, and its message
// says by how much to shorten the variables.
//
// Parameters:
//   - name is a full cluster name.
func validateIBMClusterName(name string) error {
	problem := ibmclustername.Problem(name)
	if problem == "" {
		return nil
	}
	if n := len([]rune(name)); n > MaxClusterNameLength {
		excess, unit := n-MaxClusterNameLength, "characters"
		if excess == 1 {
			unit = "character"
		}
		return fmt.Errorf("cluster name %q has %d characters, more than IBM's limit of %d (%s): shorten "+
			"E2E_NAME_PREFIX or E2E_RUN_ID by %d %s", name, n, MaxClusterNameLength, ibmclustername.Rule, excess, unit)
	}
	return fmt.Errorf("cluster name %q %s, so IBM would refuse it (%s): change E2E_NAME_PREFIX or E2E_RUN_ID",
		name, problem, ibmclustername.Rule)
}

// NewRunID returns a random run ID of length lowercase letters and digits.
//
// Parameters:
//   - length is the number of characters, at least 1 (DefaultRunIDLength for the lanes).
//   - source supplies the randomness; nil means crypto/rand.Reader. Tests pass a fixed reader.
func NewRunID(length int, source io.Reader) (string, error) {
	if length < 1 {
		return "", fmt.Errorf("run ID length %d is not positive: pass at least 1, for example %d", length, DefaultRunIDLength)
	}
	if source == nil {
		source = rand.Reader
	}
	alphabetSize := big.NewInt(int64(len(runIDAlphabet)))
	id := make([]byte, length)
	for i := range id {
		n, err := rand.Int(source, alphabetSize)
		if err != nil {
			return "", fmt.Errorf("generating a random run ID (set E2E_RUN_ID to choose one): %w", err)
		}
		id[i] = runIDAlphabet[n.Int64()]
	}
	return string(id), nil
}
