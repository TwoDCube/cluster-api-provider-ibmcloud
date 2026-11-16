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

// Package credentials decides which IBM Cloud IAM identity CAPIBM acts as for one CAPI Cluster.
//
// # Why this package exists
//
// Every IBM Cloud call the managed Kubernetes controllers make is made with credentials. Today there is exactly one
// set, the CAPIBM controller's own (the manager-bootstrap-credentials Secret), and this feature ships no way to choose
// another: a per-Cluster identity would need an identity kind and its Secret in the clusterctl move graph, a namespace
// allow-list and a review of its own. The package still exists because choosing credentials per Cluster later must be a
// purely additive change: new code and new optional fields, but no changed signature, cache key, condition reason,
// message or test. That only holds if every caller already asks one seam, a Resolver, for the identity of the Cluster
// it works on, and builds every IBM client for the identity it got back. ControllerOnly is the only Resolver of this
// feature: it always answers with the controller identity.
//
// # Conventions
//
//   - "Identity" here always means IBM Cloud IAM credentials, never the mapping of a CAPI object to an IBM cluster,
//     which the managed control plane calls the binding (DecideBinding, the BindingMismatch reason).
//   - The identity is resolved from exactly one object, the Cluster's InfraCluster (the holder), on every path,
//     including deletion. No control plane or pool carries an identity of its own.
//   - An IdentityKey holds no secret material, so it can key caches and appear in logs. Key material reaches
//     go-sdk-core only through a CredentialSource.
//   - Nothing here reads the environment, a credentials file or a package-level variable. The controller identity's
//     authenticator is built only by the managed client builder, with the factory cmd/main.go injects.
//   - Kubernetes objects are read only by Resolver implementations; ControllerOnly reads none.
//   - The package is shared by every CAPIBM API family and imports no scope package, so the VPC and PowerVS kinds can
//     adopt it later without importing the managed scope.
package credentials
