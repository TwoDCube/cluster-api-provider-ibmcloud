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
	"context"
	"errors"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"
)

// This file holds the identity seam fakes of the managed controller tests, which check that every IBM call of a
// Cluster resolves its identity from the Cluster's IBMCloudManagedCluster only, and never in a scope constructor: a
// Resolver that records every holder, a ClientBuilder that hands out gomock clients, and a Resolver and a
// ClientBuilder that fail the test when called. Every managed controller test can reuse them.
// All of them are safe for concurrent use, so a test may run them behind a manager.

// recordingResolver is a credentials.Resolver that records every holder it is asked about and answers the controller
// identity, or err when set.
type recordingResolver struct {
	mu      sync.Mutex
	holders []client.Object
	err     error
}

var _ credentials.Resolver = &recordingResolver{}

// Resolve implements credentials.Resolver.
func (r *recordingResolver) Resolve(_ context.Context, holder client.Object) (credentials.Identity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.holders = append(r.holders, holder.DeepCopyObject().(client.Object))
	if r.err != nil {
		return credentials.Identity{}, r.err
	}
	return credentials.ControllerOnly{}.Resolve(context.Background(), holder)
}

// Holders returns copies of the holders seen so far, in call order.
func (r *recordingResolver) Holders() []client.Object {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]client.Object(nil), r.holders...)
}

// failIfCalledResolver is a credentials.Resolver for paths that must not resolve an identity: a paused object, a
// deleting object, a scope constructor (identities are resolved lazily, after the patch helper exists).
type failIfCalledResolver struct {
	t *testing.T
}

var _ credentials.Resolver = failIfCalledResolver{}

// Resolve implements credentials.Resolver; it fails the test.
func (r failIfCalledResolver) Resolve(_ context.Context, holder client.Object) (credentials.Identity, error) {
	r.t.Errorf("Resolver.Resolve called for %s, but this path must make no IBM call", client.ObjectKeyFromObject(holder))
	return credentials.Identity{}, errors.New("failIfCalledResolver: unexpected Resolve")
}

// failIfCalledBuilder is a managedscope.ClientBuilder for paths that must not build an IBM client.
type failIfCalledBuilder struct {
	t *testing.T
}

var _ managedscope.ClientBuilder = failIfCalledBuilder{}

// KubernetesService implements managedscope.ClientBuilder; it fails the test.
func (b failIfCalledBuilder) KubernetesService(context.Context, credentials.Identity, string) (kubernetesservice.KubernetesService, error) {
	b.t.Errorf("ClientBuilder.KubernetesService called, but this path must make no IBM call")
	return nil, errors.New("failIfCalledBuilder: unexpected KubernetesService")
}

// VPC implements managedscope.ClientBuilder; it fails the test.
func (b failIfCalledBuilder) VPC(context.Context, credentials.Identity, string) (vpc.Vpc, error) {
	b.t.Errorf("ClientBuilder.VPC called, but this path must make no IBM call")
	return nil, errors.New("failIfCalledBuilder: unexpected VPC")
}

// ResourceManager implements managedscope.ClientBuilder; it fails the test.
func (b failIfCalledBuilder) ResourceManager(context.Context, credentials.Identity) (resourcemanager.ResourceManager, error) {
	b.t.Errorf("ClientBuilder.ResourceManager called, but this path must make no IBM call")
	return nil, errors.New("failIfCalledBuilder: unexpected ResourceManager")
}

// InvalidateAuthenticator implements managedscope.ClientBuilder; it fails the test.
func (b failIfCalledBuilder) InvalidateAuthenticator(credentials.IdentityKey) {
	b.t.Errorf("ClientBuilder.InvalidateAuthenticator called, but this path must make no IBM call")
}

// stubClientBuilder is a managedscope.ClientBuilder that returns the gomock clients it holds and records every
// identity it builds for and every invalidation. A nil client field makes the method fail, as a broken credential
// would.
type stubClientBuilder struct {
	kubernetes      kubernetesservice.KubernetesService
	vpc             vpc.Vpc
	resourceManager resourcemanager.ResourceManager

	mu          sync.Mutex
	identities  []credentials.Identity
	invalidated []credentials.IdentityKey
}

var _ managedscope.ClientBuilder = &stubClientBuilder{}

// KubernetesService implements managedscope.ClientBuilder.
func (b *stubClientBuilder) KubernetesService(_ context.Context, id credentials.Identity, _ string) (kubernetesservice.KubernetesService, error) {
	b.record(id)
	if b.kubernetes == nil {
		return nil, errors.New("stubClientBuilder: no IKS client configured")
	}
	return b.kubernetes, nil
}

// VPC implements managedscope.ClientBuilder.
func (b *stubClientBuilder) VPC(_ context.Context, id credentials.Identity, _ string) (vpc.Vpc, error) {
	b.record(id)
	if b.vpc == nil {
		return nil, errors.New("stubClientBuilder: no VPC client configured")
	}
	return b.vpc, nil
}

// ResourceManager implements managedscope.ClientBuilder.
func (b *stubClientBuilder) ResourceManager(_ context.Context, id credentials.Identity) (resourcemanager.ResourceManager, error) {
	b.record(id)
	if b.resourceManager == nil {
		return nil, errors.New("stubClientBuilder: no Resource Manager client configured")
	}
	return b.resourceManager, nil
}

// InvalidateAuthenticator implements managedscope.ClientBuilder.
func (b *stubClientBuilder) InvalidateAuthenticator(key credentials.IdentityKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.invalidated = append(b.invalidated, key)
}

// record remembers the identity of one build.
func (b *stubClientBuilder) record(id credentials.Identity) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.identities = append(b.identities, id)
}

// Identities returns the identities the builder built clients for, in call order.
func (b *stubClientBuilder) Identities() []credentials.Identity {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]credentials.Identity(nil), b.identities...)
}
