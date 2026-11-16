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
	"testing"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	. "github.com/onsi/gomega"
)

func TestIsIBMCloudManagedCluster(t *testing.T) {
	testCases := []struct {
		name    string
		cluster *clusterv1.Cluster
		want    bool
	}{
		{name: "nil cluster", cluster: nil, want: false},
		{name: "managed control plane", cluster: clusterWithControlPlane(managedControlPlaneRef("dev")), want: true},
		{
			name:    "another control plane kind of the same group",
			cluster: clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.cluster.x-k8s.io", Kind: "KubeadmControlPlane", Name: "dev"}),
			want:    false,
		},
		{
			name:    "the managed kind name in another group",
			cluster: clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.example.com", Kind: "IBMCloudManagedControlPlane", Name: "dev"}),
			want:    false,
		},
		{name: "no control plane", cluster: clusterWithControlPlane(clusterv1.ContractVersionedObjectReference{}), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			NewWithT(t).Expect(IsIBMCloudManagedCluster(tc.cluster)).To(Equal(tc.want))
		})
	}
}

// clusterWithControlPlane returns a Cluster whose spec.controlPlaneRef is ref.
func clusterWithControlPlane(ref clusterv1.ContractVersionedObjectReference) *clusterv1.Cluster {
	return &clusterv1.Cluster{Spec: clusterv1.ClusterSpec{ControlPlaneRef: ref}}
}
