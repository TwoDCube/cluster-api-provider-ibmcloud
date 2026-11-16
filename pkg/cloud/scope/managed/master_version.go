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

package managed

import (
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// MasterVersionInput is everything DecideMasterAction decides on.
type MasterVersionInput struct {
	// Distribution is spec.distribution. Required.
	Distribution controlplanev1.Distribution

	// DesiredVersion is spec.distributionVersion, MAJOR.MINOR such as "1.37" or "4.22". Required.
	DesiredVersion string

	// Cluster is this reconcile's GetCluster answer: state, masterKubeVersion, targetVersion, masterHealth,
	// caCertRotationStatus and versionEOS are read. Required.
	Cluster *kubernetesservice.Cluster

	// MasterUpdate is status.masterUpdate, the latest update CAPIBM requested; the zero value when none.
	MasterUpdate controlplanev1.MasterUpdateRequest

	// CurrentUpToDate is the MasterVersionUpToDate condition the object carries now (util/conditions Get; nil when it
	// has none). It is the only record of IBM's terminal answer to UpdateMaster: IBM shows nothing after it refuses a
	// call, and status.masterUpdate only records that the call was sent. When it carries the Reason of a Terminal
	// MasterUpdateCallResult (PermissionDenied or VersionSkipNotAllowed), was written for Generation, and
	// status.masterUpdate is for the version spec.distributionVersion asks for, DecideMasterAction keeps it instead of
	// reporting UpgradePending and then UpgradeFailed, which would send the operator after a failed IBM update instead
	// of the missing Kubernetes Service role.
	CurrentUpToDate *metav1.Condition

	// Generation is the object's metadata.generation. A refusal in CurrentUpToDate is kept only while its
	// observedGeneration (which util/conditions Set records) equals it: a version-skip refusal lasts until the spec
	// changes, and row 7 writes the same VersionSkipNotAllowed reason for a spec that may since have been changed back
	// to the next minor, which must not be taken for IBM's refusal.
	Generation int64

	// Offered are the versions IBM offers (ListVersions through the VersionsCache, cached per identity and region).
	// Read only when spec.distributionVersion is the master's next minor and every guard before row 8 passed; nil
	// otherwise. When it is needed and nil, the decision is NeedsOfferedVersions.
	Offered *kubernetesservice.Versions

	// Now is the caller's clock reading. Required.
	Now time.Time
}

// offers reports whether IBM offers minor for the distribution (row 8).
//
// Parameters:
//   - minor: the requested minor.
func (in MasterVersionInput) offers(minor ibmversion.Minor) bool {
	for _, offered := range in.offeredVersions() {
		if offered.Major == minor.Major && offered.Minor == minor.Minor {
			return true
		}
	}
	return false
}

// offeredVersions returns IBM's offered versions of the distribution.
func (in MasterVersionInput) offeredVersions() []kubernetesservice.ReleaseVersion {
	if in.Distribution == controlplanev1.DistributionOpenShift {
		return in.Offered.OpenShift
	}
	return in.Offered.Kubernetes
}

// offeredMinors lists IBM's offered minors of the distribution in ascending order, for the VersionNotAvailable message
// (for example "IBM Cloud does not offer Kubernetes 1.33 (offered: 1.34, 1.35, 1.36, 1.37)").
func (in MasterVersionInput) offeredMinors() string {
	minors := make([]ibmversion.Minor, 0, len(in.offeredVersions()))
	for _, offered := range in.offeredVersions() {
		minors = append(minors, ibmversion.Minor{Major: offered.Major, Minor: offered.Minor})
	}
	slices.SortFunc(minors, ibmversion.Minor.Compare)
	minors = slices.Compact(minors)
	names := make([]string, 0, len(minors))
	for _, minor := range minors {
		names = append(names, minor.String())
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// masterProduct names the IBM product of a distribution in messages.
//
// Parameters:
//   - d: spec.distribution.
func masterProduct(d controlplanev1.Distribution) string {
	if d == controlplanev1.DistributionOpenShift {
		return "Red Hat OpenShift"
	}
	return "Kubernetes"
}
