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

// Package serviceurl turns a rejected IBM Cloud service URL, or the error url.Parse returned for it, into text that
// an error message may carry without leaking a credential.
//
// Why it exists: the clients that take an explicit service URL (the VPC and Resource Manager constructors, the IBM
// Cloud Kubernetes Service client and the managed client builder's --service-endpoint check) reject a URL with user
// information, a query or a fragment, and their errors reach the controller log, events and conditions. Any of those
// parts may hold an API key or a token, and so may the reasons url.Parse gives for an unparsable URL: its *url.Error
// quotes the whole input, and several of the reasons it wraps quote a part of it. One implementation keeps the rule
// the same for every client and is reviewed in one place.
//
// The convention: an error names the URL only through Printable, and a URL that does not parse only through
// ParseFailureReason. Neither ever returns user information, a query or a fragment, so a caller does not need to
// reason about which part of its input is safe. That includes user information that holds a "/", "?" or "#": url.Parse
// ends the host there, so the first part of the secret becomes the host and the rest the path, query or fragment, with
// no parsed user information left. HasAtAfterHost detects such a URL by the "@" after its host; Printable does not
// show it, and every service URL check rejects it as a URL with user information, so that no URL a check admits (and
// a client later prints as it is) holds user information in any form. Following CAPIBM's code organisation, this pure
// helper lives under pkg/util: no IBM SDK, no I/O, no state.
package serviceurl
