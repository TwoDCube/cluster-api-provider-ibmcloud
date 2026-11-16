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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group and version of the managed control plane kind.
	GroupVersion = schema.GroupVersion{Group: "controlplane.cluster.x-k8s.io", Version: "v1alpha1"}

	// schemeBuilder registers the kinds of this package; each types file appends its kinds to objectTypes in init().
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the kinds of this group and version to the given scheme. cmd/main.go and every envtest suite
	// that works with these kinds call it.
	AddToScheme = schemeBuilder.AddToScheme

	// objectTypes is filled by the init() function of every types file, so adding a kind touches only its own file.
	objectTypes = []runtime.Object{}
)

// addKnownTypes registers objectTypes and the shared meta types for GroupVersion in scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, objectTypes...)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
