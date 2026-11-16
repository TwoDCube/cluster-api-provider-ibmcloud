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

package serviceurl

import (
	"errors"
	"net/url"
	"testing"

	. "github.com/onsi/gomega"
)

// testSecret stands for an API key or token typed into a service URL; no output of this package may contain it. It
// starts with a letter that is not a hexadecimal digit, so "%" + testSecret is an invalid escape.
const testSecret = "planted-s3cr3t"

// TestPrintable covers the form in which errors print a rejected service URL: scheme, host and path, and nothing
// that may carry a credential.
func TestPrintable(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "a URL without anything to drop is unchanged", input: "https://us-south.iaas.cloud.ibm.com/v1",
			want: "https://us-south.iaas.cloud.ibm.com/v1"},
		{name: "a port is kept", input: "https://h:8443/x", want: "https://h:8443/x"},
		{name: "a plain http URL is kept", input: "http://h/x", want: "http://h/x"},
		{name: "user name and password are both redacted", input: "https://u:p@h/x", want: "https://xxxxx@h/x"}, //nolint:gosec // G101: a fake password.
		{name: "a user name without password is redacted", input: "https://u@h/x", want: "https://xxxxx@h/x"},
		{name: "an API key typed as the user name is redacted", input: "https://" + testSecret + "@h/x", want: "https://xxxxx@h/x"},
		{name: "an empty user name is redacted", input: "https://@h/x", want: "https://xxxxx@h/x"},
		{name: "a query is dropped", input: "https://h/x?apikey=S", want: "https://h/x"},
		{name: "a fragment is dropped", input: "https://h/x#S", want: "https://h/x"},
		{name: "an empty query is dropped", input: "https://h/x?", want: "https://h/x"},
		{name: "an empty query and an empty fragment are dropped", input: "https://h/x?#", want: "https://h/x"},
		{name: "user information, query and fragment are dropped together", input: "https://u:p@h/x?apikey=S#S", //nolint:gosec // G101: a fake password.
			want: "https://xxxxx@h/x"},
		{
			// url.ParseRequestURI (the --service-endpoint flag parser) keeps a typed fragment in the path as %23.
			name: "a percent-encoded # in the path cuts the path", input: "https://h/v1%23" + testSecret, want: "https://h/v1",
		},
		{
			// The path keeps an escaped "/" in RawPath; the cut must not bring the cut-off part back through it.
			name: "a cut path with a raw encoding drops the raw form", input: "https://h/a%2Fb%23" + testSecret, want: "https://h/a/b",
		},
		{name: "a path with an escaped slash and no # keeps its encoding", input: "https://h/a%2Fb", want: "https://h/a%2Fb"},
		{name: "a URL without a host is not shown", input: "https:///v1", want: withoutHost},
		{name: "an opaque URL is not shown", input: "https:h?apikey=S", want: withoutHost},
		{
			// Without "//", the user name becomes the scheme and the password the opaque part.
			name: "a scheme-less URL with user information is not shown", input: "user:" + testSecret + "@h/v1", want: withoutHost,
		},
		{name: "a scheme-less URL with a user name is not shown", input: testSecret + "@h/v1", want: withoutHost},
		{name: "user information without a host is not shown", input: "https://u:p@/x", want: withoutHost},
		{
			// url.Parse ends the host at the "/", so the first part of the token is the host and the rest the path.
			name: "user information holding a slash is not shown", input: "http://" + testSecret + "/" + testSecret + "@h/x",
			want: withAtAfterHost,
		},
		{
			name: "user information holding a question mark is not shown", input: "https://" + testSecret + "?" + testSecret + "@h/x",
			want: withAtAfterHost,
		},
		{
			name: "user information holding a hash is not shown", input: "https://" + testSecret + "#" + testSecret + "@h/x",
			want: withAtAfterHost,
		},
		{
			// A password whose part before the "/" is all digits parses as a valid port, so neither the user name nor
			// the digits may be shown.
			name: "a password with leading digits and a slash is not shown", input: "https://u:1234/" + testSecret + "@h/x?",
			want: withAtAfterHost,
		},
		{name: "an @ in the path is not shown", input: "https://h/v1@x", want: withAtAfterHost},
		{name: "a percent-encoded @ in the path is not shown", input: "https://h/v1%40x", want: withAtAfterHost},
		{name: "an @ in the query is not shown", input: "https://h/x?apikey=a@b", want: withAtAfterHost},
		{name: "an IPv6 host with a port keeps both", input: "https://u:p@[2001:db8::1]:8443/x?apikey=S#S", want: "https://xxxxx@[2001:db8::1]:8443/x"},
		{name: "percent-encoded user information is redacted", input: "https://%75ser:%70ass@h/x", want: "https://xxxxx@h/x"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			parsed, err := url.Parse(tc.input)
			g.Expect(err).NotTo(HaveOccurred())
			before := *parsed
			g.Expect(Printable(parsed)).To(Equal(tc.want))
			g.Expect(*parsed).To(Equal(before), "Printable must not modify its argument")
		})
	}
	t.Run("a nil URL is printed like one without a host", func(t *testing.T) {
		NewWithT(t).Expect(Printable(nil)).To(Equal(withoutHost))
	})
	t.Run("a fragment typed into the flag parser's URL form is cut", func(t *testing.T) {
		g := NewWithT(t)
		// The --service-endpoint parser (pkg/cloud/endpoints) stores url.ParseRequestURI(value).String(), which turns
		// the fragment into path text; its users parse that string again.
		requestURI, err := url.ParseRequestURI("https://h/v1#" + testSecret)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(requestURI.String()).To(ContainSubstring("%23" + testSecret))
		reparsed, err := url.Parse(requestURI.String())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(Printable(reparsed)).To(Equal("https://h/v1"))
	})
}

// TestHasAtAfterHost covers the detection of user information that url.Parse split off behind the host: an "@" in
// the path, query or fragment, in decoded or raw form. Parsed user information, a URL without "@" after the host and
// a URL without a host do not count.
func TestHasAtAfterHost(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "a plain service URL", input: "https://us-south.iaas.cloud.ibm.com/v1", want: false},
		{name: "parsed user information", input: "https://u:p@h/x", want: false}, //nolint:gosec // G101: a fake password.
		{name: "a password holding an @ is parsed user information", input: "https://u:p@ss@h/x", want: false},
		{name: "user information holding a slash", input: "https://S1/S2@h/x", want: true},
		{name: "user information holding a question mark", input: "https://S1?S2@h/x", want: true},
		{name: "user information holding a hash", input: "https://S1#S2@h/x", want: true},
		{name: "a password with leading digits and a slash", input: "https://u:1234/S@h/x?", want: true},
		{name: "a percent-encoded @ in the path", input: "https://h/a%40b", want: true},
		{name: "a percent-encoded @ in the fragment", input: "https://h/x#a%40b", want: true},
		{name: "a percent-encoded @ in the query stays encoded", input: "https://h/x?a=b%40c", want: false},
		{name: "a URL with a path, query and fragment but no @", input: "https://h/x?a=b#c", want: false},
		{name: "an opaque URL has no host to follow", input: "user:pw@h/v1", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			parsed, err := url.Parse(tc.input)
			g.Expect(err).NotTo(HaveOccurred())
			before := *parsed
			g.Expect(HasAtAfterHost(parsed)).To(Equal(tc.want))
			g.Expect(*parsed).To(Equal(before), "HasAtAfterHost must not modify its argument")
		})
	}
	t.Run("a nil URL has nothing after a host", func(t *testing.T) {
		NewWithT(t).Expect(HasAtAfterHost(nil)).To(BeFalse())
	})
}

// TestParseFailureReason covers how a URL that url.Parse rejects is described: by the kind of problem only, never by a
// part of the input, because net/url quotes the input in its own error and in several of the reasons it wraps.
func TestParseFailureReason(t *testing.T) {
	const escapeReason = "invalid percent-encoding: every % must start an escape of two hexadecimal digits"
	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "a bad escape after a password", input: "https://u:S@h/%zz", want: escapeReason},      //nolint:gosec // G101: a fake password.
		{name: "a bad escape in the password", input: "https://u:s3%zzcret@h/x", want: escapeReason}, //nolint:gosec // G101: a fake password.
		{name: "a bad escape that starts with secret characters", input: "https://u:%" + testSecret + "@h/x", want: escapeReason},
		{name: "a bad escape at the end", input: "https://h/x%2", want: escapeReason},
		{name: "a bad escape in the fragment", input: "https://h/x#tok%zzen", want: escapeReason},
		{name: "a bad host character", input: "https://h{st/x", want: "invalid character in the host name"},
		{name: "a bad port", input: "https://h:port/x", want: "invalid port after the host"},
		{
			// A "/" in the password ends the authority early, so net/url reads the rest of the password as the port.
			name: "a password holding a slash becomes a bad port", input: "https://u:" + testSecret + "/x@h/y",
			want: "invalid port after the host",
		},
		{
			name: "a password holding a question mark becomes a bad port", input: "https://u:" + testSecret + "?x@h/y",
			want: "invalid port after the host",
		},
		{name: "a fixed net/url text is kept", input: "://h/x", want: "missing protocol scheme"},
		{name: "a colon in the first path segment", input: "1user:" + testSecret + "@h", want: "first path segment in URL cannot contain colon"},
		{name: "a control character", input: "https://h/\x00", want: "net/url: invalid control character in URL"},
		{name: "a control character in the password", input: "https://u:" + testSecret + "\x7f@h/x", want: "net/url: invalid control character in URL"},
		{name: "an IPv4 address in brackets", input: "https://[10.0.0.1]/x", want: "invalid IP-literal"},
		{name: "a missing bracket", input: "https://[::1/x", want: "missing ']' in host"},
		{name: "a reason that may quote the input is replaced", input: "https://[fe80::zz]/x", want: malformedURL},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			_, err := url.Parse(tc.input)
			g.Expect(err).To(HaveOccurred())
			reason := ParseFailureReason(err)
			g.Expect(reason).To(Equal(tc.want))
			g.Expect(reason).NotTo(ContainSubstring(testSecret))
		})
	}
	t.Run("an error that wraps nothing is matched by its own text", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect(ParseFailureReason(errors.New("missing protocol scheme"))).To(Equal("missing protocol scheme"))
		g.Expect(ParseFailureReason(errors.New(`parse "https://u:` + testSecret + `@h": something new`))).To(Equal(malformedURL))
	})
	t.Run("a nil error is described without panicking", func(t *testing.T) {
		NewWithT(t).Expect(ParseFailureReason(nil)).To(Equal(malformedURL))
	})
}

// TestNoOutputCarriesTheSecret proves the package's promise on inputs that put a secret into every part of a URL that
// may carry one: whether the URL parses (Printable) or not (ParseFailureReason), the text an error would print never
// contains the secret. The first six inputs put a recognizable secret into the password, the user name, a query
// parameter, the fragment, the bare query and the user information of a URL with an invalid escape; the next seven
// add a password that net/url reads as a port, an invalid escape, a fragment that the flag parser kept in the path and
// URLs without a host; the next four add IPv6 hosts and percent-encoded user information; the last five put user
// information that holds a "/", "?" or "#" before the "@", which url.Parse splits into a host and a path, query or
// fragment; the fourth of them, a password with leading digits, even leaves a valid host and port.
func TestNoOutputCarriesTheSecret(t *testing.T) {
	for _, input := range []string{
		"https://u:" + testSecret + "@h/x",
		"https://" + testSecret + "@h/x",
		"https://h/x?apikey=" + testSecret,
		"https://h/x#" + testSecret,
		"https://h/x?" + testSecret,
		"https://u:" + testSecret + "@h/%zz",
		"https://u:" + testSecret + "/x@h/y",
		"https://u:%" + testSecret + "@h/x",
		"https://h/v1%23" + testSecret,
		"https:" + testSecret,
		"https:///x#" + testSecret,
		"user:" + testSecret + "@h/v1",
		testSecret + "@h/v1",
		"https://u:" + testSecret + "@[2001:db8::1]:8443/v1?apikey=" + testSecret,
		"https://u:" + testSecret + "@[2001:db8::1/v1",
		"https://u:" + testSecret + "@[fe80::zz]/v1",
		"https://u:%70" + testSecret + "@h/v1",
		"https://" + testSecret + "?" + testSecret + "@h/x",
		"https://" + testSecret + "#" + testSecret + "@h/x",
		"http://" + testSecret + "/" + testSecret + "@h/x",
		"https://u:1234/" + testSecret + "@h/x?",
		"https://" + testSecret + "/" + testSecret + "@h/v1",
	} {
		t.Run(input, func(t *testing.T) {
			g := NewWithT(t)
			parsed, err := url.Parse(input)
			if err != nil {
				g.Expect(ParseFailureReason(err)).NotTo(ContainSubstring(testSecret))
				return
			}
			g.Expect(Printable(parsed)).NotTo(ContainSubstring(testSecret))
		})
	}
}
