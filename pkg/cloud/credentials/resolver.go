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

package credentials

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resolver returns the identity for every IBM call made on behalf of the Cluster whose InfraCluster is holder. It is
// the one seam through which a later release can give Clusters different credentials.
//
// Callers resolve once per reconcile (the scopes' lazy ibm method) and never keep the result across reconciles, so a
// changed identity takes effect on the next reconcile. They call it only after the patch helper exists, so that an
// error can be persisted as a condition, and never for a paused object; scope constructors therefore make no IBM call.
type Resolver interface {
	// Resolve returns the identity of the Cluster whose InfraCluster is holder.
	//
	// Parameters:
	//   - ctx carries the reconcile's deadline and logger; a Resolver that reads Kubernetes objects uses it.
	//   - holder is the Cluster's InfraCluster (for the managed kinds an IBMCloudManagedCluster), never a control plane or
	//     a pool, so that every object of a Cluster resolves the same identity on every path, including deletion. nil is a
	//     programming error in the caller.
	Resolve(ctx context.Context, holder client.Object) (Identity, error)
}
