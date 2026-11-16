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
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
)

const (
	// ibmCloudManagedControlPlaneKind is the control plane kind that marks a managed Cluster.
	ibmCloudManagedControlPlaneKind = "IBMCloudManagedControlPlane"
)

// IsIBMCloudManagedCluster reports whether cluster's spec.controlPlaneRef names an IBMCloudManagedControlPlane (group
// controlplane.cluster.x-k8s.io). It is the ClusterCache's ClusterFilter. cluster is the Cluster the ClusterCache
// reconciles; nil is not managed.
func IsIBMCloudManagedCluster(cluster *clusterv1.Cluster) bool {
	if cluster == nil {
		return false
	}
	ref := cluster.Spec.ControlPlaneRef
	return ref.APIGroup == controlplanev1.GroupVersion.Group && ref.Kind == ibmCloudManagedControlPlaneKind
}
