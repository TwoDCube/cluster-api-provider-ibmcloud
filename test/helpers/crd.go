/*
Copyright 2022 The Kubernetes Authors.

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

package helpers

import (
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const (
	clusterAPIGroup       = "cluster.x-k8s.io"
	clusterAPITestVersion = "v1beta2"
	objectType            = "object"
)

var (
	// TestClusterCRD will generate a Cluster CustomResourceDefinition.
	TestClusterCRD = generateTestClusterAPICRD("cluster", "clusters")

	// TestMachineCRD will generate a Machine CustomResourceDefinition.
	TestMachineCRD = generateTestClusterAPICRD("machine", "machines")

	// TestMachinePoolCRD will generate a MachinePool CustomResourceDefinition with a status subresource. The managed
	// (IKS and ROKS) kinds use MachinePools as the owners of IBMCloudManagedMachinePools, so envtest suites need the kind.
	// The kind is passed in its final casing because the title-casing below keeps every letter after the first.
	TestMachinePoolCRD = generateTestClusterAPICRD("MachinePool", "machinepools")
)

// Generates fake CRDs of Cluster API kinds (clusters, machines, machine pools) that accept any spec and status.
func generateTestClusterAPICRD(kind, pluralKind string) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apiextensionsv1.SchemeGroupVersion.String(),
			Kind:       "CustomResourceDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: pluralKind + "." + clusterAPIGroup,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "cluster.x-k8s.io",
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:   cases.Title(language.Und, cases.NoLower).String(kind),
				Plural: pluralKind,
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    clusterAPITestVersion,
					Served:  true,
					Storage: true,
					Subresources: &apiextensionsv1.CustomResourceSubresources{
						Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					},
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type: objectType,
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"spec": {
									Type:                   objectType,
									XPreserveUnknownFields: ptr.To(true),
								},
								"status": {
									Type:                   objectType,
									XPreserveUnknownFields: ptr.To(true),
								},
							},
						},
					},
				},
			},
		},
	}
}
