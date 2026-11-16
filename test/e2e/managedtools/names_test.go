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
	"errors"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

func TestClusterName(t *testing.T) {
	testCases := []struct {
		name    string
		prefix  string
		runID   string
		lane    Lane
		want    string
		wantErr string
	}{
		{name: "default prefix, IKS", prefix: DefaultNamePrefix, runID: "k3x9q2", lane: LaneIKS, want: "capibm-e2e-k3x9q2-iks"},
		{name: "default prefix, ROKS", prefix: DefaultNamePrefix, runID: "k3x9q2", lane: LaneROKS, want: "capibm-e2e-k3x9q2-roks"},
		// A 20-character prefix: 20 + 1 + 3 = 24 characters, exactly the unique part, 28 and 29 in total.
		{name: "20-character prefix, IKS", prefix: "capibm-e2e-periodics", runID: "x7k", lane: LaneIKS, want: "capibm-e2e-periodics-x7k-iks"},
		{name: "20-character prefix, ROKS", prefix: "capibm-e2e-periodics", runID: "x7k", lane: LaneROKS, want: "capibm-e2e-periodics-x7k-roks"},
		{name: "unique part one character too long", prefix: "capibm-e2e-periodics", runID: "x7k2", lane: LaneIKS,
			wantErr: `"capibm-e2e-periodics-x7k2" (E2E_NAME_PREFIX "capibm-e2e-periodics", E2E_RUN_ID "x7k2") has 25 characters; it must fit into the first 24 characters`},
		{name: "empty prefix", prefix: "", runID: "abc", lane: LaneIKS, wantErr: `E2E_NAME_PREFIX ""`},
		{name: "two-character prefix", prefix: "ab", runID: "abc", lane: LaneIKS, wantErr: `E2E_NAME_PREFIX "ab"`},
		{name: "upper-case prefix", prefix: "Capibm", runID: "abc", lane: LaneIKS, wantErr: `E2E_NAME_PREFIX "Capibm"`},
		{name: "prefix starts with a digit", prefix: "1capibm", runID: "abc", lane: LaneIKS, wantErr: `E2E_NAME_PREFIX "1capibm"`},
		{name: "prefix ends with a hyphen", prefix: "capibm-", runID: "abc", lane: LaneIKS, wantErr: `E2E_NAME_PREFIX "capibm-"`},
		{name: "prefix with underscore", prefix: "capi_bm", runID: "abc", lane: LaneIKS, wantErr: `E2E_NAME_PREFIX "capi_bm"`},
		{name: "empty run ID", prefix: DefaultNamePrefix, runID: "", lane: LaneIKS, wantErr: `E2E_RUN_ID ""`},
		{name: "run ID with hyphen", prefix: DefaultNamePrefix, runID: "x-7", lane: LaneIKS, wantErr: `E2E_RUN_ID "x-7"`},
		{name: "upper-case run ID", prefix: DefaultNamePrefix, runID: "X7K", lane: LaneIKS, wantErr: `E2E_RUN_ID "X7K"`},
		{name: "unknown lane", prefix: DefaultNamePrefix, runID: "abc", lane: Lane("eks"), wantErr: `lane "eks" is not an e2e lane`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ClusterName(tc.prefix, tc.runID, tc.lane)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				g.Expect(got).To(BeEmpty())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
			g.Expect(len(got)).To(BeNumerically("<=", MaxClusterNameLength))
		})
	}
}

func TestClusterNameLongestUniquePartFitsIBMLimit(t *testing.T) {
	g := NewWithT(t)
	// A 24-character unique part with the longest lane is the longest name the contract allows.
	prefix := "a" + strings.Repeat("b", UniqueNameLength-1-1-1) // prefix + "-" + 1-character run ID = 24
	name, err := ClusterName(prefix, "z", LaneROKS)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(name).To(HaveLen(UniqueNameLength + len("-roks")))
	g.Expect(len(name)).To(BeNumerically("<=", MaxClusterNameLength))
}

func TestValidateNameLength(t *testing.T) {
	g := NewWithT(t)
	g.Expect(validateNameLength(strings.Repeat("a", MaxClusterNameLength))).To(Succeed())
	g.Expect(validateNameLength(strings.Repeat("a", MaxClusterNameLength+2))).To(
		MatchError(ContainSubstring("has 37 characters, more than IBM's limit of 35: shorten E2E_NAME_PREFIX or E2E_RUN_ID by 2 characters")))
}

func TestRunNamePrefix(t *testing.T) {
	g := NewWithT(t)
	got, err := RunNamePrefix("capibm-e2e-periodics", "x7k")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(got).To(Equal("capibm-e2e-periodics-x7k-"))

	_, err = RunNamePrefix("capibm-e2e-periodics", "x7k99")
	g.Expect(err).To(MatchError(ContainSubstring("shorten E2E_NAME_PREFIX or E2E_RUN_ID by 2 characters")))
}

func TestValidateNamePrefix(t *testing.T) {
	g := NewWithT(t)
	g.Expect(ValidateNamePrefix(DefaultNamePrefix)).To(Succeed())
	g.Expect(ValidateNamePrefix("capibm-e2e-periodics")).To(Succeed())
	g.Expect(ValidateNamePrefix("abc")).To(Succeed())
	g.Expect(ValidateNamePrefix("c")).To(MatchError(ContainSubstring("use at least 3 characters")))
}

func TestNewRunID(t *testing.T) {
	testCases := []struct {
		name    string
		length  int
		source  *bytes.Reader
		want    string
		wantErr string
	}{
		// rand.Int draws 6 bits per character from each byte (the alphabet has 36 letters); 0 is 'a', 35 is '9'.
		{name: "fixed source", length: 4, source: bytes.NewReader([]byte{0, 1, 25, 35}), want: "abz9"},
		// 40 (6 bits) is outside the alphabet, so rand.Int rejects it and reads the next byte.
		{name: "rejection sampling", length: 1, source: bytes.NewReader([]byte{40, 2}), want: "c"},
		{name: "zero length", length: 0, wantErr: "run ID length 0 is not positive"},
		{name: "exhausted source", length: 3, source: bytes.NewReader([]byte{1}), wantErr: "generating a random run ID (set E2E_RUN_ID to choose one)"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			var got string
			var err error
			if tc.source == nil {
				got, err = NewRunID(tc.length, nil)
			} else {
				got, err = NewRunID(tc.length, tc.source)
			}
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestNewRunIDDefaultSource(t *testing.T) {
	g := NewWithT(t)
	id, err := NewRunID(DefaultRunIDLength, nil)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(id).To(MatchRegexp(`^[a-z0-9]{6}$`))
	// Every generated ID satisfies the name contract with the default prefix.
	_, err = ClusterName(DefaultNamePrefix, id, LaneROKS)
	g.Expect(err).NotTo(HaveOccurred())
}

func TestNewRunIDReaderError(t *testing.T) {
	g := NewWithT(t)
	_, err := NewRunID(2, failingReader{})
	g.Expect(err).To(MatchError(ContainSubstring("entropy unavailable")))
}

// failingReader is an io.Reader that always fails.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }
