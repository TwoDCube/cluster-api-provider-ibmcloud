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
	"fmt"

	"k8s.io/apimachinery/pkg/util/version"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// ValidateUpgradeStep returns an error unless to is the minor right after from: the IKS lane creates its cluster
// at the previous minor and upgrades it by exactly one minor, the only step IBM allows, so a misconfigured pair fails
// before a cluster is created instead of hours later.
//
// Parameters:
//   - from is the create version (IKS_DISTRIBUTION_VERSION), MAJOR.MINOR, for example "1.35".
//   - to is the upgrade version (IKS_DISTRIBUTION_VERSION_UPGRADE_TO), MAJOR.MINOR, for example "1.36".
func ValidateUpgradeStep(from, to string) error {
	fromMinor, err := ibmversion.ParseMinor(from)
	if err != nil {
		return fmt.Errorf("IKS_DISTRIBUTION_VERSION: %w", err)
	}
	toMinor, err := ibmversion.ParseMinor(to)
	if err != nil {
		return fmt.Errorf("IKS_DISTRIBUTION_VERSION_UPGRADE_TO: %w", err)
	}
	if toMinor != fromMinor.Next() {
		return fmt.Errorf("IKS_DISTRIBUTION_VERSION_UPGRADE_TO %q must be the minor after IKS_DISTRIBUTION_VERSION %q, "+
			"that is %q: IBM upgrades a master one minor at a time", to, from, fromMinor.Next())
	}
	return nil
}

// IBMVersionMinor returns the MAJOR.MINOR of an IBM version string such as status.distributionVersion
// ("1.36.5_1537" or "4.21.35_1537_openshift").
//
// Parameters:
//   - ibmVersion is the version as IBM reports it.
func IBMVersionMinor(ibmVersion string) (string, error) {
	parsed, err := ibmversion.Parse(ibmVersion)
	if err != nil {
		return "", err
	}
	return parsed.MinorVersion().String(), nil
}

// KubeletMinor returns the MAJOR.MINOR of a Node's kubelet version, for example "1.36" for "v1.36.5+IKS".
//
// Parameters:
//   - kubeletVersion is Node.status.nodeInfo.kubeletVersion.
func KubeletMinor(kubeletVersion string) (string, error) {
	parsed, err := version.ParseGeneric(kubeletVersion)
	if err != nil {
		return "", fmt.Errorf("kubelet version %q does not parse: %w", kubeletVersion, err)
	}
	return fmt.Sprintf("%d.%d", parsed.Major(), parsed.Minor()), nil
}

// VersionCounts counts how many times each version occurs, the shape of a MachinePool's status.versions (CAPI
// aggregates the kubelet versions of the pool's Nodes into version and replicas).
//
// Parameters:
//   - versions are the kubelet versions of the pool's Nodes, one per Node.
func VersionCounts(versions []string) map[string]int32 {
	counts := make(map[string]int32, len(versions))
	for _, v := range versions {
		counts[v]++
	}
	return counts
}
