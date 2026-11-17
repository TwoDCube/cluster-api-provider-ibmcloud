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

package ibmversion

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
)

const (
	// openShiftSuffix marks a ROKS version ("4.21.35_1537_openshift", "4.21_openshift"). IBM writes it in lower case
	// only, so the match is case-sensitive.
	openShiftSuffix = "openshift"

	// pendingMarker is what IBM is assumed to append to masterKubeVersion while a master update is in progress
	// (IsMasterUpdatePending). No live probe has seen it yet: every masterKubeVersion observed on IBM Cloud was a plain
	// fix pack. The form is an assumption (Terraform's IBM provider only checks that the string contains "(pending)").
	// The leading space is part of the marker, and Parse matches it exactly. Not yet verified against IBM Cloud: assumed
	// form " (pending)"; fallback: parser tweak (accept the form IBM actually returns).
	pendingMarker = " (pending)"

	// maxDigits bounds every number of a version. IBM's numbers have at most four digits today (build 1537); nine
	// digits keep every value, and Next's +1, inside 32 bits, so no arithmetic in this package can overflow.
	maxDigits = 9

	// acceptedForms ends every Parse error, so that the reader sees what a valid IBM version looks like.
	acceptedForms = "accepted forms are X.Y.Z_BUILD[_openshift][ (pending)], for example 1.36.5_1537 or " +
		"4.21.35_1537_openshift, and X.Y[_openshift], for example 1.36 or 4.21_openshift"

	// acceptedMinorForm ends every ParseMinor error.
	acceptedMinorForm = "expected MAJOR.MINOR, for example 1.36 (Kubernetes) or 4.21 (OpenShift)"
)

// Minor is an IBM product minor version, MAJOR.MINOR: "1.36" for IKS (a Kubernetes minor) or "4.21" for ROKS (an
// OpenShift minor). It is the only version a user chooses (spec.distributionVersion), because IBM always applies the
// newest patch and fix pack of a minor itself (on IBM Cloud, "1.36" was created as "1.36.5_1537").
type Minor struct {
	// Major is the major version, 1 for Kubernetes and 4 for OpenShift. This package does not check which major
	// belongs to which product (see the package documentation).
	Major int

	// Minor is the minor version within Major.
	Minor int
}

// ParseMinor parses a minor version of the form MAJOR.MINOR, such as spec.distributionVersion.
//
// s is the text to parse, for example "1.36" or "4.21". Both numbers are decimal without leading zeros and the major
// is at least 1: the same rule as the CEL validation of spec.distributionVersion, so every value the API server
// accepted parses. A patch, a "v" prefix and IBM suffixes such as "_openshift" are rejected, because IBM chooses the
// patch and the distribution field, not the version, selects OpenShift. The error names s and the accepted form.
func ParseMinor(s string) (Minor, error) {
	reason := formReason(s)
	if reason == "" && strings.Contains(s, "_") {
		reason = "IBM suffixes such as _openshift or a _BUILD number are not part of a minor " +
			"(the distribution, not the version, selects OpenShift)"
	}
	var numbers []int
	if reason == "" {
		numbers, reason = parseNumbers(s)
	}
	if reason == "" && len(numbers) == 3 {
		reason = "a patch cannot be chosen, IBM always applies its newest patch of the minor"
	}
	if reason != "" {
		return Minor{}, fmt.Errorf("invalid minor version %q: %s; %s", s, reason, acceptedMinorForm)
	}
	return Minor{Major: numbers[0], Minor: numbers[1]}, nil
}

// Compare orders two minors: it returns -1 when m is older than other, 0 when they are equal and +1 when m is newer.
// The major decides first, then the minor.
//
// other is the minor to compare m with.
func (m Minor) Compare(other Minor) int {
	if m.Major != other.Major {
		return cmp.Compare(m.Major, other.Major)
	}
	return cmp.Compare(m.Minor, other.Minor)
}

// Next returns the following minor of the same major ("1.36" → "1.37", "4.21" → "4.22"), the only step IBM allows for
// a master update. It never moves to a new major: a new major is a new product line that IBM announces on its own
// terms, and the controllers must not guess its first minor.
//
// TODO: Next assumes that the version after a minor is the next minor of IBM's /versions list. That stops holding with
// OpenShift 5.0: from OpenShift 4.22 a cluster can move to 4.23 or to 5.0, so the upgrades form a graph per cluster.
// IBM is developing a per-cluster upgrade-path endpoint (GraphQL, currently
// https://containers.cloud.ibm.com/unsafe/graphql) that lists the versions a given cluster can move to. Once it is
// stable, the master version step, its skip rule and messages, and the admission webhook should ask it for the
// allowed next versions instead of calling Next, and the version listing should move to it as well. The same endpoint
// is also the way to see a master patch that IBM offers but has not applied, which /versions cannot show.
func (m Minor) Next() Minor {
	return Minor{Major: m.Major, Minor: m.Minor + 1}
}

// String returns m as MAJOR.MINOR, the form of spec.distributionVersion, for example "1.36".
func (m Minor) String() string {
	return strconv.Itoa(m.Major) + "." + strconv.Itoa(m.Minor)
}

// Version is a parsed IBM version string, either a full fix pack X.Y.Z_BUILD[_openshift][ (pending)] as IBM reports it
// for masters and workers, or a minor X.Y[_openshift] as CAPIBM sends it to IBM.
type Version struct {
	// Major is the major version: 1 for IKS and 4 for ROKS.
	Major int

	// Minor is the minor version within Major. MinorVersion returns Major and Minor together as a Minor.
	Minor int

	// Patch is the patch version (5 in "1.36.5_1537"); 0 when HasFixPack is false.
	Patch int

	// Build is IBM's build number after the underscore (1537 in "1.36.5_1537"); 0 when HasFixPack is false.
	Build int

	// HasFixPack is true for the full form X.Y.Z_BUILD and false for the minor-only form X.Y. The flag exists because
	// patch 0 is a real patch, so a zero Patch cannot mean "no patch".
	HasFixPack bool

	// OpenShift is true when the string carries the "_openshift" suffix (ROKS).
	OpenShift bool

	// Pending is true when the string ends in " (pending)", the assumed form of IBM's marker for a master update in
	// progress (not yet verified against IBM Cloud, see pendingMarker). It is not part of the fix pack: CompareFixPack
	// ignores it, and WithoutPending clears it before a master version is written to status.distributionVersion.
	Pending bool
}

// Parse parses an IBM version string as IBM returns it (masterKubeVersion, targetVersion, a worker's
// kubeVersion.desired/actual/target, the worker-version Node label) or as CAPIBM sends it in create and master update
// requests.
//
// s is the text to parse. Accepted forms are X.Y.Z_BUILD[_openshift][ (pending)], for example "1.36.5_1537",
// "4.21.35_1537_openshift" or "1.36.5_1537 (pending)" (the marker's form is an assumption, not yet verified against
// IBM Cloud), and X.Y[_openshift], for example "1.36" or "4.21_openshift".
// Numbers are decimal without leading zeros and the major is at least 1. Everything else is rejected with an error
// that names s, the reason and the accepted forms, including a patch without its build number ("1.36.5"), which no
// IBM response has been seen to contain and which no CAPIBM request sends. Parse applies no distribution rules: it
// does not check that an "_openshift" version has major 4.
func Parse(s string) (Version, error) {
	v, reason := parseVersion(s)
	if reason != "" {
		return Version{}, fmt.Errorf("invalid IBM version %q: %s; %s", s, reason, acceptedForms)
	}
	return v, nil
}

// MinorVersion returns the minor of v, for example 1.36 for "1.36.5_1537" and 4.21 for "4.21_openshift". The master
// and its workers are linked only at the minor, so this is the value most decisions compare first.
// (Go does not allow a method named Minor next to the Minor field, hence the longer name.)
func (v Version) MinorVersion() Minor {
	return Minor{Major: v.Major, Minor: v.Minor}
}

// CompareFixPack orders two fix packs of one minor and product: it returns -1 when v is older than other, 0 when they
// are the same fix pack and +1 when v is newer. The patch decides first, then the build number, so
// "4.21.40_1540_openshift" is newer than "4.21.35_1537_openshift" (ROKS workers were observed to run ahead of their
// master) and "1.36.5_1540" is newer than "1.36.5_1537". The " (pending)" marker is ignored.
//
// other is the fix pack to compare v with. Fix packs are ordered only within one minor: the method returns an error
// when the minors differ (callers compare MinorVersion first), when exactly one of the two is an OpenShift version, and
// when either is a minor-only version, which names no fix pack.
func (v Version) CompareFixPack(other Version) (int, error) {
	if !v.HasFixPack || !other.HasFixPack {
		return 0, fmt.Errorf("cannot compare the fix packs of %q and %q: both must be full versions X.Y.Z_BUILD, "+
			"a minor-only version X.Y names no fix pack", v, other)
	}
	if v.OpenShift != other.OpenShift {
		return 0, fmt.Errorf("cannot compare the fix packs of %q and %q: only one of them is an OpenShift version",
			v, other)
	}
	if v.MinorVersion() != other.MinorVersion() {
		return 0, fmt.Errorf("cannot compare the fix packs of %q and %q: they belong to different minors (%s and %s), "+
			"compare MinorVersion first", v, other, v.MinorVersion(), other.MinorVersion())
	}
	if v.Patch != other.Patch {
		return cmp.Compare(v.Patch, other.Patch), nil
	}
	return cmp.Compare(v.Build, other.Build), nil
}

// WithoutPending returns a copy of v with Pending cleared: the same version without IBM's " (pending)" marker. Call it
// before a master version goes to status.distributionVersion, which the API defines as masterKubeVersion without the
// marker. String writes the marker back, and the field's MaxLength of 64 would accept "1.37.1_1517 (pending)" without
// complaint, so nothing else catches the leak. The receiver itself is not changed.
func (v Version) WithoutPending() Version {
	v.Pending = false
	return v
}

// String returns v in IBM's form. For every string s that Parse accepts, the parsed version's String returns s
// unchanged, including a " (pending)" marker; that is the form for messages and logs. For status.distributionVersion
// use v.WithoutPending().String(), because that field never carries the marker.
func (v Version) String() string {
	var b strings.Builder
	b.WriteString(v.MinorVersion().String())
	if v.HasFixPack {
		b.WriteString("." + strconv.Itoa(v.Patch) + "_" + strconv.Itoa(v.Build))
	}
	if v.OpenShift {
		b.WriteString("_" + openShiftSuffix)
	}
	if v.Pending {
		b.WriteString(pendingMarker)
	}
	return b.String()
}

// parseVersion does the work of Parse. It returns the version, or a non-empty reason why s is not an IBM version.
func parseVersion(s string) (Version, string) {
	rest, pending := strings.CutSuffix(s, pendingMarker)
	if reason := formReason(rest); reason != "" {
		return Version{}, reason
	}
	// Everything after the first underscore is a suffix: the build number and/or "openshift".
	fields := strings.Split(rest, "_")
	suffix, reason := parseSuffixes(fields[1:])
	if reason != "" {
		return Version{}, reason
	}
	numbers, reason := parseNumbers(fields[0])
	if reason != "" {
		return Version{}, reason
	}
	v := Version{Major: numbers[0], Minor: numbers[1], OpenShift: suffix.openShift, Pending: pending}
	if len(numbers) == 2 {
		switch {
		case suffix.hasBuild:
			return Version{}, "a _BUILD number needs a patch (X.Y.Z_BUILD); the minor-only form is X.Y[_openshift]"
		case pending:
			return Version{}, "the (pending) marker only follows a full version X.Y.Z_BUILD"
		}
		return v, ""
	}
	if !suffix.hasBuild {
		// IBM reports a patch only together with its build number (every version string observed on IBM Cloud has
		// one), so "1.36.5" or "4.21.35_openshift" is not something IBM returned and must not be ordered as if it were a
		// fix pack.
		return Version{}, fmt.Sprintf("the patch %d has no _BUILD number (X.Y.Z_BUILD, for example 1.36.5_1537)",
			numbers[2])
	}
	build, reason := parseNumber(suffix.build, "build number")
	if reason != "" {
		return Version{}, reason
	}
	v.Patch, v.Build, v.HasFixPack = numbers[2], build, true
	return v, ""
}

// suffixes holds the underscore-separated parts that follow the numbers of an IBM version.
type suffixes struct {
	// build is the text of the build number, still to be parsed; only meaningful when hasBuild is true.
	build string
	// hasBuild is true when a build number was present (it may still turn out to be empty or not a number).
	hasBuild bool
	// openShift is true when the "_openshift" suffix was present.
	openShift bool
}

// parseSuffixes reads the parts after the first underscore: none, "openshift", BUILD, or BUILD and "openshift" in that
// order. parts is the result of splitting the version on "_" without the leading numbers. It returns the suffixes, or
// a non-empty reason why they are invalid.
func parseSuffixes(parts []string) (suffixes, string) {
	switch {
	case len(parts) == 0:
		return suffixes{}, ""
	case parts[0] == openShiftSuffix && len(parts) == 1:
		return suffixes{openShift: true}, ""
	case parts[0] == openShiftSuffix:
		return suffixes{}, "_openshift must be the last suffix; the build number comes before it, as in 4.21.35_1537_openshift"
	case len(parts) == 1 && strings.EqualFold(parts[0], openShiftSuffix):
		return suffixes{}, unknownSuffix(parts[0])
	case len(parts) == 1:
		return suffixes{build: parts[0], hasBuild: true}, ""
	case len(parts) == 2 && parts[1] == openShiftSuffix:
		return suffixes{build: parts[0], hasBuild: true, openShift: true}, ""
	case len(parts) == 2:
		return suffixes{}, unknownSuffix(parts[1])
	default:
		return suffixes{}, fmt.Sprintf("found %d underscore-separated suffixes, at most two are allowed (_BUILD, then _openshift)",
			len(parts))
	}
}

// unknownSuffix returns the reason for a suffix other than "openshift"; part is the suffix without its underscore.
func unknownSuffix(part string) string {
	return fmt.Sprintf("unknown suffix _%s, the only suffix besides the build number is _openshift in lower case", part)
}

// formReason returns why s cannot be an IBM version or minor at all, or "" when it may be one. These are the checks
// that Parse and ParseMinor share; each reason points at the form the writer probably confused it with.
func formReason(s string) string {
	if s == "" {
		return "the version is empty"
	}
	if s[0] == 'v' || s[0] == 'V' {
		return "IBM versions have no v prefix (v1.36.5+IKS is the Kubernetes form that kubelets and API servers report)"
	}
	for _, r := range s {
		switch {
		case isDigit(r), r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '.', r == '_':
			// Letters are allowed here so that a misspelt suffix gets its own, more precise reason later.
		case r == '+':
			return "'+' starts Kubernetes build metadata such as +IKS, which IBM versions do not carry " +
				"(IBM's build number follows an underscore)"
		case r == '*':
			return "'*' is a decoration of the ibmcloud CLI output, not part of the IBM API value"
		case r == ' ':
			return "unexpected space (leading, trailing or misplaced)"
		default:
			return fmt.Sprintf("unexpected character %q", r)
		}
	}
	return ""
}

// parseNumbers parses the dot-separated numbers of a version: MAJOR.MINOR or MAJOR.MINOR.PATCH. s is the text before
// the first underscore. It returns two or three numbers, or a non-empty reason why s is not of that form.
func parseNumbers(s string) ([]int, string) {
	names := []string{"major", "minor", "patch"}
	fields := strings.Split(s, ".")
	if len(fields) < 2 {
		return nil, "the minor is missing (MAJOR.MINOR is the shortest form)"
	}
	if len(fields) > len(names) {
		return nil, fmt.Sprintf("found %d dot-separated numbers, at most three are allowed (MAJOR.MINOR.PATCH)",
			len(fields))
	}
	numbers := make([]int, 0, len(fields))
	for i, field := range fields {
		n, reason := parseNumber(field, names[i])
		if reason != "" {
			return nil, reason
		}
		numbers = append(numbers, n)
	}
	if numbers[0] == 0 {
		// Same rule as the CEL validation of spec.distributionVersion; IBM has no major 0.
		return nil, "the major must be at least 1"
	}
	return numbers, ""
}

// parseNumber parses one number of a version: ASCII decimal digits, no sign, no leading zero, at most maxDigits
// digits. s is the text and what names it in the reason ("major", "build number"). It returns the value, or a
// non-empty reason why s is not a valid number.
func parseNumber(s, what string) (int, string) {
	if s == "" {
		return 0, "the " + what + " is empty"
	}
	if strings.ContainsFunc(s, func(r rune) bool { return !isDigit(r) }) {
		return 0, fmt.Sprintf("the %s %q is not a decimal number", what, s)
	}
	if len(s) > 1 && s[0] == '0' {
		return 0, fmt.Sprintf("the %s %q has a leading zero", what, s)
	}
	if len(s) > maxDigits {
		return 0, fmt.Sprintf("the %s %q has more than %d digits", what, s, maxDigits)
	}
	n := 0
	for _, r := range s {
		n = n*10 + int(r-'0') // s is at most maxDigits ASCII digits, so this cannot overflow
	}
	return n, ""
}

// isDigit reports whether r is an ASCII decimal digit; other Unicode digits never appear in IBM versions.
func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}
