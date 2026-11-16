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
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
)

// The vectors below are the version forms IBM documents and the strings the live probes on IBM Cloud returned. A
// patch without its build number is rejected: no IBM response has been seen to contain one. The " (pending)" vectors
// are not live: they test the assumed form of the marker, which is not yet verified against IBM Cloud (see
// pendingMarker), and change with pendingMarker once it is.

func TestParse(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  Version
	}{
		{
			name:  "live IKS master fix pack",
			input: "1.36.5_1537",
			want:  Version{Major: 1, Minor: 36, Patch: 5, Build: 1537, HasFixPack: true},
		},
		{
			name:  "live IKS worker fix pack",
			input: "1.36.5_1540",
			want:  Version{Major: 1, Minor: 36, Patch: 5, Build: 1540, HasFixPack: true},
		},
		{
			name:  "live ROKS master fix pack",
			input: "4.21.35_1537_openshift",
			want:  Version{Major: 4, Minor: 21, Patch: 35, Build: 1537, HasFixPack: true, OpenShift: true},
		},
		{
			name:  "live ROKS worker fix pack",
			input: "4.21.40_1540_openshift",
			want:  Version{Major: 4, Minor: 21, Patch: 40, Build: 1540, HasFixPack: true, OpenShift: true},
		},
		{
			name:  "master update in progress carries the assumed pending marker",
			input: "1.36.5_1537 (pending)",
			want:  Version{Major: 1, Minor: 36, Patch: 5, Build: 1537, HasFixPack: true, Pending: true},
		},
		{
			name:  "ROKS master update in progress (assumed form)",
			input: "4.22.20_1529_openshift (pending)",
			want:  Version{Major: 4, Minor: 22, Patch: 20, Build: 1529, HasFixPack: true, OpenShift: true, Pending: true},
		},
		{
			name:  "two-digit patch",
			input: "1.34.12_1573",
			want:  Version{Major: 1, Minor: 34, Patch: 12, Build: 1573, HasFixPack: true},
		},
		{
			name:  "IKS fix pack of the next minor",
			input: "1.37.1_1517",
			want:  Version{Major: 1, Minor: 37, Patch: 1, Build: 1517, HasFixPack: true},
		},
		{
			name:  "ROKS 4.22 worker fix pack",
			input: "4.22.20_1529_openshift",
			want:  Version{Major: 4, Minor: 22, Patch: 20, Build: 1529, HasFixPack: true, OpenShift: true},
		},
		{
			name:  "patch 0 is a real patch",
			input: "1.37.0_1501",
			want:  Version{Major: 1, Minor: 37, Patch: 0, Build: 1501, HasFixPack: true},
		},
		{
			name:  "IKS create input",
			input: "1.36",
			want:  Version{Major: 1, Minor: 36},
		},
		{
			name:  "ROKS create input",
			input: "4.21_openshift",
			want:  Version{Major: 4, Minor: 21, OpenShift: true},
		},
		{
			name:  "minor 0",
			input: "1.0",
			want:  Version{Major: 1, Minor: 0},
		},
		{
			name:  "nine-digit numbers are the upper bound",
			input: "999999999.999999999.999999999_999999999",
			want: Version{Major: 999999999, Minor: 999999999, Patch: 999999999, Build: 999999999,
				HasFixPack: true},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := Parse(tc.input)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
			g.Expect(got.String()).To(Equal(tc.input), "String must give back the IBM string unchanged")
		})
	}
}

func TestParseRejects(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		reason string
	}{
		// Malformed and foreign forms.
		{name: "empty", input: "", reason: "the version is empty"},
		{name: "pending marker alone", input: " (pending)", reason: "the version is empty"},
		{name: "v prefix on a minor", input: "v1.36", reason: "no v prefix"},
		{name: "v prefix on a patch", input: "v1.36.5", reason: "no v prefix"},
		{name: "upper-case V prefix", input: "V1.36", reason: "no v prefix"},
		{name: "Kubernetes build metadata", input: "1.36.5+IKS", reason: "'+' starts Kubernetes build metadata"},
		{name: "text after _openshift", input: "4.21_openshift_x", reason: "_openshift must be the last suffix"},
		{name: "build after _openshift", input: "4.21.35_openshift_1544", reason: "_openshift must be the last suffix"},
		{name: "major only", input: "1", reason: "the minor is missing"},
		{name: "four numbers (IBM doc typo v1.36.5.2)", input: "1.36.5.2", reason: "found 4 dot-separated numbers"},
		{name: "empty build number", input: "1.36.5_", reason: "the build number is empty"},
		{name: "non-numeric build number", input: "1.36.5_abc", reason: `the build number "abc" is not a decimal number`},
		{name: "unknown suffix", input: "1.36.5_1537_rhel", reason: "unknown suffix _rhel"},
		{name: "suffix is case-sensitive", input: "4.21.35_1544_OpenShift", reason: "unknown suffix _OpenShift"},
		{name: "minor-only suffix is case-sensitive", input: "4.21_OpenShift", reason: "unknown suffix _OpenShift"},
		{name: "CLI decoration", input: "1.32.5*", reason: "decoration of the ibmcloud CLI"},
		{name: "leading zero in the major of a version without build", input: "01.36.5", reason: `the major "01" has a leading zero`},
		{name: "leading zero in the major", input: "01.36.5_1537", reason: `the major "01" has a leading zero`},
		// A patch without its build number is not an IBM version: no IBM response has been seen to contain one.
		{name: "patch without build", input: "1.36.5", reason: "the patch 5 has no _BUILD number"},
		{name: "OpenShift patch without build", input: "4.21.35_openshift", reason: "the patch 35 has no _BUILD number"},
		// Further grammar edges.
		{name: "build without patch", input: "1.36_1537", reason: "a _BUILD number needs a patch"},
		{name: "OpenShift build without patch", input: "4.21_1537_openshift", reason: "a _BUILD number needs a patch"},
		{name: "pending on a minor", input: "1.36 (pending)", reason: "the (pending) marker only follows a full version"},
		{name: "pending on an OpenShift minor", input: "4.21_openshift (pending)", reason: "only follows a full version"},
		{name: "pending without space", input: "1.36.5_1537(pending)", reason: "unexpected character '('"},
		{name: "pending in upper case", input: "1.36.5_1537 (Pending)", reason: "unexpected space"},
		{name: "pending twice", input: "1.36.5_1537 (pending) (pending)", reason: "unexpected space"},
		{name: "leading space", input: " 1.36.5_1537", reason: "unexpected space"},
		{name: "trailing space", input: "1.36.5_1537 ", reason: "unexpected space"},
		{name: "trailing newline", input: "1.36.5_1537\n", reason: `unexpected character '\n'`},
		{name: "pre-release", input: "1.36.5-rc.1", reason: "unexpected character '-'"},
		{name: "non-ASCII digit", input: "1.３6", reason: "unexpected character '３'"},
		{name: "too many suffixes", input: "1.36.5_1537_openshift_openshift", reason: "found 3 underscore-separated suffixes"},
		{name: "double underscore", input: "1.36.5__1537", reason: "unknown suffix _1537"},
		{name: "empty major", input: ".36", reason: "the major is empty"},
		{name: "empty minor", input: "1.", reason: "the minor is empty"},
		{name: "empty patch", input: "1.36._1537", reason: "the patch is empty"},
		{name: "non-numeric minor", input: "1.x", reason: `the minor "x" is not a decimal number`},
		{name: "leading zero in the minor", input: "1.036", reason: `the minor "036" has a leading zero`},
		{name: "leading zero in the patch", input: "1.36.05_1537", reason: `the patch "05" has a leading zero`},
		{name: "leading zero in the build", input: "1.36.5_01537", reason: `the build number "01537" has a leading zero`},
		{name: "major 0", input: "0.36", reason: "the major must be at least 1"},
		{name: "ten-digit number", input: "1.36.5_1234567890", reason: "has more than 9 digits"},
		{name: "minor only, underscore without suffix", input: "1.36_", reason: "a _BUILD number needs a patch"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := Parse(tc.input)
			g.Expect(err).To(HaveOccurred())
			g.Expect(got).To(Equal(Version{}))
			g.Expect(err.Error()).To(ContainSubstring(tc.reason))
			// Every error names the input and the accepted forms, so that the reader knows what to fix.
			g.Expect(err.Error()).To(HavePrefix(fmt.Sprintf("invalid IBM version %q: ", tc.input)))
			g.Expect(err.Error()).To(HaveSuffix(acceptedForms))
		})
	}
}

func TestParseMinor(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  Minor
	}{
		{name: "IKS minor", input: "1.36", want: Minor{Major: 1, Minor: 36}},
		{name: "ROKS minor", input: "4.21", want: Minor{Major: 4, Minor: 21}},
		{name: "minor 0", input: "1.0", want: Minor{Major: 1, Minor: 0}},
		{name: "multi-digit major", input: "10.100", want: Minor{Major: 10, Minor: 100}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ParseMinor(tc.input)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
			g.Expect(got.String()).To(Equal(tc.input))
		})
	}
}

func TestParseMinorRejects(t *testing.T) {
	// The first three are values the CEL rule of spec.distributionVersion rejects; ParseMinor must agree.
	testCases := []struct {
		name   string
		input  string
		reason string
	}{
		{name: "patch", input: "1.36.5", reason: "a patch cannot be chosen"},
		{name: "v prefix", input: "v1.36", reason: "no v prefix"},
		{name: "OpenShift suffix", input: "4.21_openshift", reason: "suffixes such as _openshift"},
		{name: "IBM fix pack", input: "1.36.5_1537", reason: "suffixes such as _openshift or a _BUILD number"},
		{name: "empty", input: "", reason: "the version is empty"},
		{name: "major only", input: "1", reason: "the minor is missing"},
		{name: "four numbers", input: "1.36.5.2", reason: "found 4 dot-separated numbers"},
		{name: "Kubernetes build metadata", input: "1.36+IKS", reason: "'+' starts Kubernetes build metadata"},
		{name: "non-numeric minor", input: "1.x", reason: `the minor "x" is not a decimal number`},
		{name: "leading zero in the major", input: "01.36", reason: `the major "01" has a leading zero`},
		{name: "leading zero in the minor", input: "1.036", reason: `the minor "036" has a leading zero`},
		{name: "major 0", input: "0.36", reason: "the major must be at least 1"},
		{name: "empty minor", input: "1.", reason: "the minor is empty"},
		{name: "empty major", input: ".36", reason: "the major is empty"},
		{name: "leading space", input: " 1.36", reason: "unexpected space"},
		{name: "trailing space", input: "1.36 ", reason: "unexpected space"},
		{name: "ten-digit minor", input: "1.1234567890", reason: "has more than 9 digits"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ParseMinor(tc.input)
			g.Expect(err).To(HaveOccurred())
			g.Expect(got).To(Equal(Minor{}))
			g.Expect(err.Error()).To(ContainSubstring(tc.reason))
			g.Expect(err.Error()).To(HavePrefix(fmt.Sprintf("invalid minor version %q: ", tc.input)))
			g.Expect(err.Error()).To(HaveSuffix(acceptedMinorForm))
		})
	}
}

func TestMinorCompare(t *testing.T) {
	testCases := []struct {
		name  string
		m     Minor
		other Minor
		want  int
	}{
		{name: "equal", m: Minor{Major: 1, Minor: 36}, other: Minor{Major: 1, Minor: 36}, want: 0},
		{name: "older minor", m: Minor{Major: 1, Minor: 36}, other: Minor{Major: 1, Minor: 37}, want: -1},
		{name: "newer minor", m: Minor{Major: 4, Minor: 22}, other: Minor{Major: 4, Minor: 21}, want: 1},
		{name: "numeric, not lexical (1.9 < 1.10)", m: Minor{Major: 1, Minor: 9}, other: Minor{Major: 1, Minor: 10}, want: -1},
		{name: "major decides before minor", m: Minor{Major: 1, Minor: 99}, other: Minor{Major: 4, Minor: 1}, want: -1},
		{name: "higher major is newer", m: Minor{Major: 4, Minor: 1}, other: Minor{Major: 1, Minor: 99}, want: 1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.m.Compare(tc.other)).To(Equal(tc.want))
			g.Expect(tc.other.Compare(tc.m)).To(Equal(-tc.want), "Compare must be antisymmetric")
		})
	}
}

func TestMinorNext(t *testing.T) {
	testCases := []struct {
		name string
		m    Minor
		want Minor
	}{
		{name: "IKS", m: Minor{Major: 1, Minor: 36}, want: Minor{Major: 1, Minor: 37}},
		{name: "ROKS", m: Minor{Major: 4, Minor: 21}, want: Minor{Major: 4, Minor: 22}},
		{name: "minor 0", m: Minor{Major: 1, Minor: 0}, want: Minor{Major: 1, Minor: 1}},
		{name: "carries into a third digit, never into the major", m: Minor{Major: 1, Minor: 99}, want: Minor{Major: 1, Minor: 100}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.m.Next()).To(Equal(tc.want))
			g.Expect(tc.m.Next().Compare(tc.m)).To(Equal(1), "Next must be newer than its receiver")
		})
	}
}

func TestMinorString(t *testing.T) {
	testCases := []struct {
		name string
		m    Minor
		want string
	}{
		{name: "IKS", m: Minor{Major: 1, Minor: 36}, want: "1.36"},
		{name: "ROKS", m: Minor{Major: 4, Minor: 21}, want: "4.21"},
		{name: "minor 0", m: Minor{Major: 1, Minor: 0}, want: "1.0"},
		{name: "zero value", m: Minor{}, want: "0.0"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.m.String()).To(Equal(tc.want))
		})
	}
}

func TestVersionMinorVersion(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  Minor
	}{
		{name: "IKS master", input: "1.36.5_1537", want: Minor{Major: 1, Minor: 36}},
		{name: "ROKS worker", input: "4.21.40_1540_openshift", want: Minor{Major: 4, Minor: 21}},
		{name: "pending master", input: "1.36.5_1537 (pending)", want: Minor{Major: 1, Minor: 36}},
		{name: "IKS minor-only", input: "1.37", want: Minor{Major: 1, Minor: 37}},
		{name: "ROKS minor-only", input: "4.22_openshift", want: Minor{Major: 4, Minor: 22}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			v, err := Parse(tc.input)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(v.MinorVersion()).To(Equal(tc.want))
		})
	}
}

func TestVersionCompareFixPack(t *testing.T) {
	testCases := []struct {
		name  string
		v     string
		other string
		want  int
	}{
		{name: "IKS worker fix pack runs ahead of the master", v: "1.36.5_1540", other: "1.36.5_1537", want: 1},
		{name: "ROKS worker runs ahead of the master", v: "4.21.40_1540_openshift", other: "4.21.35_1537_openshift", want: 1},
		{name: "same fix pack", v: "1.36.5_1537", other: "1.36.5_1537", want: 0},
		{name: "pending marker is ignored", v: "1.36.5_1537 (pending)", other: "1.36.5_1537", want: 0},
		{name: "older build of the same patch", v: "4.21.35_1537_openshift", other: "4.21.35_1540_openshift", want: -1},
		{name: "patch decides before the build number", v: "1.36.6_1500", other: "1.36.5_1540", want: 1},
		{name: "numeric, not lexical (patch 9 < patch 10)", v: "1.36.9_1537", other: "1.36.10_1537", want: -1},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			v, err := Parse(tc.v)
			g.Expect(err).NotTo(HaveOccurred())
			other, err := Parse(tc.other)
			g.Expect(err).NotTo(HaveOccurred())

			got, err := v.CompareFixPack(other)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))

			reverse, err := other.CompareFixPack(v)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reverse).To(Equal(-tc.want), "CompareFixPack must be antisymmetric")
		})
	}
}

func TestVersionCompareFixPackRejects(t *testing.T) {
	testCases := []struct {
		name   string
		v      string
		other  string
		reason string
	}{
		{name: "different minors", v: "1.37.1_1517", other: "1.36.5_1540", reason: "different minors (1.37 and 1.36), compare MinorVersion first"},
		{name: "different OpenShift minors", v: "4.22.20_1529_openshift", other: "4.21.40_1540_openshift", reason: "different minors (4.22 and 4.21)"},
		{name: "receiver is minor-only", v: "1.36", other: "1.36.5_1537", reason: "a minor-only version X.Y names no fix pack"},
		{name: "argument is minor-only", v: "4.21.35_1537_openshift", other: "4.21_openshift", reason: "a minor-only version X.Y names no fix pack"},
		{name: "only one is OpenShift", v: "4.21.35_1537_openshift", other: "4.21.35_1537", reason: "only one of them is an OpenShift version"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			v, err := Parse(tc.v)
			g.Expect(err).NotTo(HaveOccurred())
			other, err := Parse(tc.other)
			g.Expect(err).NotTo(HaveOccurred())

			got, err := v.CompareFixPack(other)
			g.Expect(err).To(HaveOccurred())
			g.Expect(got).To(Equal(0))
			g.Expect(err.Error()).To(ContainSubstring(tc.reason))
			// The error names both versions in IBM's form.
			g.Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%q and %q", tc.v, tc.other)))
		})
	}
}

func TestVersionWithoutPending(t *testing.T) {
	// want is what status.distributionVersion receives: masterKubeVersion without the marker.
	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "pending IKS master loses the marker", input: "1.37.1_1517 (pending)", want: "1.37.1_1517"},
		{name: "pending ROKS master loses the marker", input: "4.22.20_1529_openshift (pending)", want: "4.22.20_1529_openshift"},
		{name: "settled IKS master is unchanged", input: "1.36.5_1537", want: "1.36.5_1537"},
		{name: "settled ROKS master is unchanged", input: "4.21.35_1537_openshift", want: "4.21.35_1537_openshift"},
		{name: "minor-only version is unchanged", input: "4.21_openshift", want: "4.21_openshift"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			v, err := Parse(tc.input)
			g.Expect(err).NotTo(HaveOccurred())

			got := v.WithoutPending()
			g.Expect(got.Pending).To(BeFalse())
			g.Expect(got.String()).To(Equal(tc.want))
			// Only the marker goes: every other field stays the same, and the receiver keeps its marker.
			wantVersion := v
			wantVersion.Pending = false
			g.Expect(got).To(Equal(wantVersion))
			g.Expect(v.String()).To(Equal(tc.input))
		})
	}
}

func TestVersionString(t *testing.T) {
	testCases := []struct {
		name string
		v    Version
		want string
	}{
		{name: "IKS fix pack", v: Version{Major: 1, Minor: 36, Patch: 5, Build: 1537, HasFixPack: true}, want: "1.36.5_1537"},
		{name: "ROKS fix pack", v: Version{Major: 4, Minor: 21, Patch: 35, Build: 1537, HasFixPack: true, OpenShift: true}, want: "4.21.35_1537_openshift"},
		{name: "pending", v: Version{Major: 1, Minor: 36, Patch: 5, Build: 1537, HasFixPack: true, Pending: true}, want: "1.36.5_1537 (pending)"},
		{name: "ROKS minor-only", v: Version{Major: 4, Minor: 21, OpenShift: true}, want: "4.21_openshift"},
		{name: "patch and build are not written without HasFixPack", v: Version{Major: 1, Minor: 36, Patch: 5, Build: 1537}, want: "1.36"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.v.String()).To(Equal(tc.want))
		})
	}
}

// FuzzParse checks two properties for any input: Parse never panics, and every accepted string is already in canonical
// form (String gives it back unchanged), which is what makes the grammar strict. The seeds run with every `go test`.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{"1.36.5_1537", "4.21.40_1540_openshift", "1.36.5_1537 (pending)", "1.36",
		"4.21_openshift", "1.36.5", "v1.36", "1.36.5+IKS", "4.21_openshift_x", "", "01.36", "01.36.5", "1.32.5*"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		v, err := Parse(s)
		if err != nil {
			return
		}
		if got := v.String(); got != s {
			t.Errorf("Parse(%q).String() = %q, want the input unchanged", s, got)
		}
	})
}

// FuzzParseMinor checks the same properties for ParseMinor.
func FuzzParseMinor(f *testing.F) {
	for _, seed := range []string{"1.36", "4.21", "1.0", "1.36.5", "v1.36", "4.21_openshift", "", "01.36"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		m, err := ParseMinor(s)
		if err != nil {
			return
		}
		if got := m.String(); got != s {
			t.Errorf("ParseMinor(%q).String() = %q, want the input unchanged", s, got)
		}
	})
}
