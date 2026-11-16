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
	"strings"
)

const (
	// redactedUserInfo replaces the whole user information of a printed URL. It is the marker url.URL.Redacted uses
	// for a password, so an operator who has seen one recognizes the other.
	redactedUserInfo = "xxxxx"
	// withoutHost is printed instead of a URL that has no host; Printable explains why.
	withoutHost = "(URL without a host, not shown)"
	// withAtAfterHost is printed instead of a URL with an "@" after its host; HasAtAfterHost explains why. It holds no
	// double quote, so that a caller's %q prints it without escapes.
	withAtAfterHost = "(URL with an @ after the host, not shown)"
	// malformedURL describes a url.Parse failure whose reason ParseFailureReason does not know, so cannot vouch for.
	malformedURL = "malformed URL"
)

// Printable returns the form of a rejected service URL that an error may print: the scheme, host and path, so that
// the operator still recognizes the value that was rejected, and nothing that may carry a credential.
//
//   - The whole user information becomes "xxxxx". The user name goes too, not only the password (url.URL.Redacted
//     keeps the user name), because an API key is often typed into the user slot ("https://<api key>@host").
//   - The query and the fragment are dropped, because both are arbitrary text that may carry a token.
//   - The path is cut at a "#", which it can only hold percent-encoded as %23: url.ParseRequestURI, which the
//     --service-endpoint flag parser uses (pkg/cloud/endpoints), assumes that there is no fragment and keeps a typed
//     "#<token>" in the path, so the flag value "https://host/v1#<token>" reaches its users as "https://host/v1%23<token>".
//   - A URL without a host is not printed at all. Without the "//" that starts the authority, url.Parse cannot tell
//     user information from the rest: "user:password@host/v1" parses as the scheme "user" and the opaque part
//     "password@host/v1", and "user@host/v1" as a relative path. Every caller rejects a URL without a host anyway, and
//     its message states the accepted form.
//   - A URL with an "@" after the host (HasAtAfterHost) is not printed at all either. url.Parse ends the host at the
//     first "/", "?" or "#", so user information that holds one of them is split there and parsed.User stays nil:
//     "https://<token>/<rest>@host/v1" parses as the host "<token>" and the path "/<rest>@host/v1", and
//     "https://user:1234/<rest>@host/v1" as the host and port "user:1234". Neither the host nor the path can be shown
//     then. Every caller rejects such a URL as one with user information.
//
// Parameters:
//   - parsed is the URL as url.Parse returned it; it is not modified. A nil URL is printed like one without a host.
func Printable(parsed *url.URL) string {
	if parsed == nil || parsed.Host == "" {
		return withoutHost
	}
	if HasAtAfterHost(parsed) {
		return withAtAfterHost
	}
	shown := *parsed
	if shown.User != nil {
		shown.User = url.User(redactedUserInfo)
	}
	shown.RawQuery = ""
	shown.ForceQuery = false
	shown.Fragment = ""
	shown.RawFragment = ""
	if path, _, found := strings.Cut(shown.Path, "#"); found {
		shown.Path = path
		// RawPath is only a hint for the encoding of Path; once Path is cut it no longer matches, and String would
		// ignore it anyway. Clearing it keeps the cut-off part out of every form of the URL.
		shown.RawPath = ""
	}
	return shown.String()
}

// HasAtAfterHost reports whether an "@" follows the host of a parsed URL: in its path, query or fragment. url.Parse
// ends the authority (user information, host and port) at the first "/", "?" or "#", so user information that holds
// one of them, as an API key or a password may, is split there: its first part becomes the host (with a port when
// the part after a ":" is all digits), and the rest, up to and including the "@", starts the path, the query or the
// fragment. parsed.User is nil then, so a check of parsed.User alone admits the URL, and Printable would print the
// first part of the secret as the host. A service URL check therefore rejects a URL for which HasAtAfterHost is true
// as one with user information. The rule costs no valid endpoint: no path of the containers, VPC or Resource Manager
// APIs contains an "@", and their URLs carry no query or fragment.
//
// Both the decoded and the raw forms are checked, so the result does not depend on which of them net/url fills for an
// input; a percent-encoded "%40" in the path counts too, because the decoded path holds an "@" then.
//
// Parameters:
//   - parsed is the URL as url.Parse returned it; it is not modified. A nil URL, and an opaque one such as
//     "user:password@host" (which has no host to follow), report false: Printable does not show a URL without a host,
//     and every caller rejects it.
func HasAtAfterHost(parsed *url.URL) bool {
	if parsed == nil {
		return false
	}
	for _, part := range []string{parsed.Path, parsed.RawPath, parsed.RawQuery, parsed.Fragment, parsed.RawFragment} {
		if strings.Contains(part, "@") {
			return true
		}
	}
	return false
}

// ParseFailureReason describes why url.Parse rejected a service URL without quoting any part of it. url.Parse's own
// error quotes the whole input, and several of the reasons it wraps quote a part of it: an invalid escape up to three
// characters ("%zz", from a password, path or fragment), an invalid host character one, an invalid port everything
// after the host's colon (which is part of the password when the password holds a "/" or a "?"). So only the kind of
// problem is returned, and a reason this function does not know becomes "malformed URL".
//
// Parameters:
//   - err is the error url.Parse returned; an error that wraps nothing is matched by its own text, and a nil error
//     (a caller bug) is described as "malformed URL" rather than panicking inside an error path.
func ParseFailureReason(err error) string {
	var escapeErr url.EscapeError
	var hostErr url.InvalidHostError
	switch {
	case err == nil:
		return malformedURL
	case errors.As(err, &escapeErr):
		return "invalid percent-encoding: every % must start an escape of two hexadecimal digits"
	case errors.As(err, &hostErr):
		return "invalid character in the host name"
	}
	reason := err
	if unwrapped := errors.Unwrap(err); unwrapped != nil {
		reason = unwrapped
	}
	switch text := reason.Error(); {
	case strings.HasPrefix(text, "invalid port "):
		return "invalid port after the host"
	case text == "missing protocol scheme", text == "first path segment in URL cannot contain colon",
		text == "net/url: invalid control character in URL", text == "net/url: invalid userinfo",
		text == "invalid IP-literal", text == "missing ']' in host":
		// net/url's fixed texts, which quote nothing.
		return text
	default:
		return malformedURL
	}
}
