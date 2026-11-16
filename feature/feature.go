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

// Package feature holds the feature gates of the IBM Cloud infrastructure provider.
//
// Why it exists: the provider had no gate mechanism before the managed Kubernetes work (IKS and ROKS on VPC Gen2). The
// managed APIs (IBMCloudManagedCluster, IBMCloudManagedControlPlane, IBMCloudManagedMachinePool) start at v1alpha1 and
// are merged in small pieces that must stay dormant until they are complete, without changing anything for an existing
// VPC or PowerVS installation. They therefore sit behind the Alpha gate ManagedKubernetes, off by default, the same way
// peer providers ship managed APIs (CAPA's EKS and ROSA gates; CAPZ's AKS gate, Alpha from v0.4 until GA in v1.8). The
// APIs and the gate stay at this level until the IKS and ROKS e2e tests have passed for four consecutive weeks, users
// have given feedback for one release, Cluster API reads status.initialization.provisioned, and ClusterClass support is
// decided; promotion then moves the APIs to v1beta1 with conversion.
//
// What the gate controls: with the gate off the CRDs and schemes stay installed and the managed webhooks stay
// registered but reject create, so a user gets a clear "turn the gate on" message instead of an opaque "webhook not
// found" error; the managed controllers start only with the gate on.
//
// Convention: the shape of Cluster API's own feature package (k8s.io/component-base/featuregate). MutableGates is the
// one set that cmd/main.go binds to the --feature-gates flag at startup; Gates is the read-only view that every other
// caller consults. The manager manifest passes --feature-gates=ManagedKubernetes=${EXP_MANAGED_KUBERNETES:=false}, so a
// clusterctl user turns the gate on with the EXP_MANAGED_KUBERNETES variable.
//
// The set is this provider's own and does not extend Cluster API's gate set the way CAPA and CAPZ do: no code in this
// provider consults a Cluster API gate, so a shared set would make --feature-gates accept names such as MachinePool
// that change nothing in this process. A dedicated set rejects them as unrecognized, which is the honest answer.
//
// MutableGates and Gates are the only process-wide mutable state here, and only flag parsing in cmd/main.go mutates
// them. Code that can receive the gate value as an explicit argument does so (for example the webhook structs), so its
// tests set the value per object; tests that must change the process-wide value use
// k8s.io/component-base/featuregate/testing.SetFeatureGateDuringTest on Gates, which restores it when the test ends.
package feature

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
)

// Every feature gate is declared here as a constant and registered in defaultFeatureGates, following Cluster API's
// template:
//
//	// MyFeature is a feature gate for <functionality>.
//	MyFeature featuregate.Feature = "MyFeature"
const (
	// ManagedKubernetes is the feature gate for the IBM Cloud managed Kubernetes APIs and controllers
	// (IBMCloudManagedCluster, IBMCloudManagedControlPlane and IBMCloudManagedMachinePool), which manage IKS and ROKS
	// clusters on VPC Gen2. It is Alpha and off by default while those APIs are v1alpha1, so that merging them changes
	// nothing for an installation that does not turn it on.
	ManagedKubernetes featuregate.Feature = "ManagedKubernetes"
)

var (
	// MutableGates is the provider's feature gate set. Only top-level setup (cmd/main.go, through AddFlag on the
	// --feature-gates flag) and k8s.io/component-base/featuregate/testing may change it.
	MutableGates featuregate.MutableFeatureGate = newFeatureGate()

	// Gates is the read-only view of MutableGates that all other code consults, for example
	// feature.Gates.Enabled(feature.ManagedKubernetes).
	Gates featuregate.FeatureGate = MutableGates
)

// defaultFeatureGates returns the spec of every known gate at its default. It is a function rather than a package-level
// map so that no caller can change a default by writing to a shared map.
func defaultFeatureGates() map[featuregate.Feature]featuregate.FeatureSpec {
	return map[featuregate.Feature]featuregate.FeatureSpec{
		ManagedKubernetes: {Default: false, PreRelease: featuregate.Alpha},
	}
}

// newFeatureGate returns a new gate set that knows every gate of defaultFeatureGates. MutableGates is built by it, and
// tests build their own sets with it so that parsing a flag never mutates (or closes) the process-wide set.
func newFeatureGate() featuregate.MutableFeatureGate {
	gate := featuregate.NewFeatureGate()
	// Add fails only on a malformed spec or a duplicate name, which is a programming error in this file; Cluster API
	// panics the same way in its init.
	runtime.Must(gate.Add(defaultFeatureGates()))
	return gate
}
