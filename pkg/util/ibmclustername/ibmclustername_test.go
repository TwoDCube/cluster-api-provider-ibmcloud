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

package ibmclustername

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

// nameOfLength returns a valid name of n characters (n >= 1): a letter, then letters and digits.
func nameOfLength(n int) string {
	return "a" + strings.Repeat("b1", n)[:n-1]
}

func TestProblem(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "31 characters", input: nameOfLength(31)},
		{name: "32 characters, IBM's limit", input: nameOfLength(32)},
		{name: "33 characters", input: nameOfLength(33), want: "has 33 characters, more than 32"},
		// The name of the live refusal: IBM answered HTTP 400 E0125.
		{name: "33-character name IBM refused", input: "example-test-cluster-e01-mgmt-iks", want: "has 33 characters, more than 32"},
		{name: "one letter", input: "a"},
		{name: "empty", input: "", want: "is empty"},
		{name: "digit first", input: "1cluster", want: "starts with '1' instead of a letter"},
		{name: "hyphen first", input: "-cluster", want: "starts with '-' instead of a letter"},
		{name: "underscore first", input: "_cluster", want: "starts with '_' instead of a letter"},
		{name: "dot first", input: ".cluster", want: "starts with '.' instead of a letter"},
		{name: "underscore inside", input: "team_a"},
		{name: "dot inside", input: "team-a.dev"},
		{name: "digits, hyphen, underscore and dot", input: "a0-_.9"},
		{name: "trailing hyphen, IBM's rule does not forbid it", input: "dev-"},
		{name: "upper case", input: "Team-A"},
		{name: "upper case only", input: "IKS"},
		{name: "space", input: "my cluster", want: `contains ' ', which is not a letter, digit, '-', '_' or '.'`},
		{name: "slash", input: "team/dev", want: `contains '/', which is not a letter, digit, '-', '_' or '.'`},
		{name: "unicode letter inside", input: "clüster", want: `contains 'ü', which is not a letter, digit, '-', '_' or '.'`},
		{name: "unicode letter first", input: "écluster", want: "starts with 'é' instead of a letter"},
		{name: "unicode digit", input: "dev٣", want: `contains '٣', which is not a letter, digit, '-', '_' or '.'`},
		{name: "control character", input: "dev\x00", want: `contains '\x00', which is not a letter, digit, '-', '_' or '.'`},
		{name: "invalid UTF-8", input: "dev\xff", want: `contains '�', which is not a letter, digit, '-', '_' or '.'`},
		// Characters, not bytes: 32 two-byte characters are 64 bytes but within the length, so the character is named.
		{name: "32 non-ASCII characters", input: "a" + strings.Repeat("ü", 31), want: `contains 'ü'`},
		{name: "33 non-ASCII characters", input: "a" + strings.Repeat("ü", 32), want: "has 33 characters, more than 32"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got := Problem(tc.input)
			if tc.want == "" {
				g.Expect(got).To(BeEmpty())
				return
			}
			g.Expect(got).To(HavePrefix(tc.want))
		})
	}
}

// TestProblemMatchesTheCRDRule proves that Problem accepts exactly the names the CRD of IBMCloudManagedControlPlane
// accepts for spec.ibmCloudClusterName: MinLength 1, MaxLength 32 and the pattern below, which must stay equal to the
// pattern of that field's CEL rule. Every string of up to three characters over an alphabet with one character of each
// class is compared, then names around the length limit.
func TestProblemMatchesTheCRDRule(t *testing.T) {
	g := NewWithT(t)
	crdPattern := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9._-]*$`)
	crdAccepts := func(name string) bool {
		n := len([]rune(name))
		return n >= 1 && n <= MaxLength && crdPattern.MatchString(name)
	}
	alphabet := []string{"a", "Z", "0", "-", "_", ".", " ", "/", "é", "\x00"}
	names := []string{""}
	level := []string{""}
	for range 3 {
		next := make([]string, 0, len(level)*len(alphabet))
		for _, prefix := range level {
			for _, c := range alphabet {
				next = append(next, prefix+c)
			}
		}
		names = append(names, next...)
		level = next
	}
	for n := MaxLength - 2; n <= MaxLength+2; n++ {
		names = append(names, nameOfLength(n), "1"+nameOfLength(n)[1:], nameOfLength(n)[:n-1]+"_")
	}
	for _, name := range names {
		g.Expect(Problem(name) == "").To(Equal(crdAccepts(name)), "name %q: Problem says %q", name, Problem(name))
	}
}

func TestRuleNamesMaxLength(t *testing.T) {
	g := NewWithT(t)
	g.Expect(Rule).To(ContainSubstring("1 to " + strconv.Itoa(MaxLength) + " characters"))
	g.Expect(Rule).To(ContainSubstring("'-', '_' and '.'"))
}
