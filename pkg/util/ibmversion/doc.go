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

// Package ibmversion parses and orders the version strings of IBM Cloud Kubernetes Service (IKS) and Red Hat
// OpenShift on IBM Cloud (ROKS).
//
// Why a dedicated grammar: IBM's strings are not semantic versions. A fix pack is "1.36.5_1537" (a patch plus an IBM
// build number after an underscore), ROKS appends "_openshift" ("4.21.35_1537_openshift"), create and master update
// requests name the minor alone ("1.36", "4.21_openshift"); all of these forms were observed on IBM Cloud. While IBM
// applies a master update, the master version is also assumed to carry a " (pending)" marker (Terraform's IBM provider
// checks for "(pending)"). No live probe has seen that marker yet, so it is not yet verified against IBM Cloud
// (fallback: a parser tweak of pendingMarker). A semver parser rejects the underscore forms or silently drops parts of
// them, and the Kubernetes forms that kubelets and API servers report ("v1.36.5+IKS", "v1.34.9") are a different
// grammar that this package deliberately does not accept.
//
// Why strict: the managed controllers decide the direction of a master update and whether a worker runs an older fix
// pack from these strings. A string they do not understand must stop that decision with a clear error instead of being
// guessed at, so Parse accepts exactly the forms IBM has been seen to return and the forms CAPIBM sends, and every error
// names the input and the accepted forms.
//
// What is not here: the package holds no distribution rules. Which major belongs to which product, the
// OpenShift-to-Kubernetes minor table and the version strings sent to IBM live with the other IBM product assumptions
// in pkg/cloud/scope/managed (assumptions.go), so they are reviewed in one place. Following CAPIBM's code
// organisation, this SDK-free, pure helper lives under pkg/util: no IBM SDK, no I/O, no state.
package ibmversion
