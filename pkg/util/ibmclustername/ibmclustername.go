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
	"fmt"
	"unicode/utf8"
)

// MaxLength is the most characters IBM accepts in a cluster name.
const MaxLength = 32

// Rule is IBM's cluster name rule as one clause, for the messages that tell a user how to choose a valid name. It
// spells out MaxLength (a constant string cannot format it in); the tests keep the two in step.
const Rule = "IBM cluster names have 1 to 32 characters, start with a letter (a-z or A-Z) and contain only letters, " +
	"digits, '-', '_' and '.'"

// Problem is the validator of IBM's cluster name rule: 1 to MaxLength characters, an ASCII letter first, then ASCII
// letters, digits, '-', '_' and '.'. It returns "" when name follows the rule and otherwise the first problem found, as
// a predicate phrase without the name, for example "has 33 characters, more than 32". The phrase leaves out the name
// and the rule so that each caller can say where the name came from (a field, the Cluster's name, an e2e variable)
// and add Rule and the fix that fits that source; a Kubernetes field error, for one, already quotes the value.
//
// Characters are counted as Unicode code points, so a name with a non-ASCII character reports its visible length and
// then the character itself; a byte that is not valid UTF-8 is reported as the replacement character U+FFFD.
//
// Parameters:
//   - name is the candidate IBM cluster name, exactly as it would be sent to IBM.
func Problem(name string) string {
	if name == "" {
		return "is empty"
	}
	if n := utf8.RuneCountInString(name); n > MaxLength {
		return fmt.Sprintf("has %d characters, more than %d", n, MaxLength)
	}
	for i, r := range name {
		switch {
		case isASCIILetter(r):
		case i == 0:
			return fmt.Sprintf("starts with %q instead of a letter", r)
		case isASCIIDigit(r) || r == '-' || r == '_' || r == '.':
		default:
			return fmt.Sprintf("contains %q, which is not a letter, digit, '-', '_' or '.'", r)
		}
	}
	return ""
}

// isASCIILetter reports whether r is a letter a-z or A-Z.
//
// Parameters:
//   - r is the character to test.
func isASCIILetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// isASCIIDigit reports whether r is a digit 0-9.
//
// Parameters:
//   - r is the character to test.
func isASCIIDigit(r rune) bool {
	return r >= '0' && r <= '9'
}
