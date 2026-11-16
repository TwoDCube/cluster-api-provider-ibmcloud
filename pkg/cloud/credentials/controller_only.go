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
	"errors"

	"github.com/IBM/go-sdk-core/v5/core"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ControllerOnly is the Resolver of this feature: every Cluster uses CAPIBM's controller identity, the credentials of
// the manager-bootstrap-credentials Secret. It still takes the InfraCluster, so every caller is already written against
// the Resolver contract a later per-Cluster identity needs.
type ControllerOnly struct{}

var _ Resolver = ControllerOnly{}

// Resolve returns the controller identity: ControllerKey(), an empty Version (the client builder rebuilds the
// controller authenticator every hour, which re-reads the mounted Secret file), ControllerDescription and no Source
// (the client builder builds it with the injected factory).
//
// Parameters:
//   - ctx is unused; the controller identity depends on no Kubernetes object.
//   - holder is the Cluster's InfraCluster. It is not read, but nil (also a typed nil pointer) is rejected, because it
//     means the caller lost the holder, which a later per-Cluster Resolver could not survive: it resolves the identity
//     from that object on every path, including deletion.
func (ControllerOnly) Resolve(_ context.Context, holder client.Object) (Identity, error) {
	// core.IsNil also catches a typed nil pointer inside the interface, which holder == nil would let through.
	if core.IsNil(holder) {
		return Identity{}, errors.New("credentials: Resolve needs the Cluster's InfraCluster as holder, got nil")
	}
	return Identity{Key: ControllerKey(), Description: ControllerDescription}, nil
}
