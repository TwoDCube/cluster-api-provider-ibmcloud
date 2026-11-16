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
	"fmt"
	"sort"
	"strings"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
)

// This file is the disruption token: one coordination.k8s.io/v1 Lease per Cluster that a pool holds from its first
// cordon until its worker removal settled, so that two pools never remove workers at the same time. The gates of
// CheckDisruption read cached state, and the cordon of a Node is not visible in IBM's worker states, so without the
// Lease two pools could pass the gates in the same instant; a Create on the API server cannot race. It is a mutex, not
// leader election: there is no renewal and no expiry. A holder that no longer exists (its IBMCloudManagedMachinePool
// was deleted or recreated) is replaced, with UID and resourceVersion preconditions so that two pools replacing the
// same stale holder cannot both win. clusterctl move does not move the Lease, so a pool also defers to any Node that
// carries another pool's drain-owner marker (OtherDrainOwners).

// disruptionLeaseSuffix ends the name of every disruption token Lease: <cluster-name>-ibm-worker-disruption.
const disruptionLeaseSuffix = "-ibm-worker-disruption"

// maxTokenAttempts bounds the create, read and replace rounds of one Acquire. Each round ends because another writer
// changed the Lease in between; after this many the caller requeues instead of spinning.
const maxTokenAttempts = 3

// TokenState is what a pool knows about its Cluster's disruption token.
type TokenState string

const (
	// TokenNotHeld means no live pool holds the token: there is no Lease, or its holder no longer exists. Acquire can
	// take it.
	TokenNotHeld TokenState = "NotHeld"
	// TokenHeld means this pool holds the token.
	TokenHeld TokenState = "Held"
	// TokenHeldByOther means another pool holds the token, or the Lease names a holder CAPIBM cannot parse (a human
	// removes such a Lease).
	TokenHeldByOther TokenState = "HeldByOther"
)

// TokenStatus is the state of the token and the holder the Lease names.
type TokenStatus struct {
	// State is the token's state for the calling pool.
	State TokenState
	// Holder is the Lease's holderIdentity, "<IBMCloudManagedMachinePool name>/<uid>"; "" when there is no Lease. Messages
	// may name it: it holds no credential.
	Holder string
}

// DisruptionTokenParams are the inputs of NewDisruptionToken.
type DisruptionTokenParams struct {
	// Client writes the Lease (Create, Delete) on the management cluster. Required.
	Client client.Client
	// APIReader reads the Lease and the holder pool without a cache (the manager's API reader): a cached read could
	// report a just-created holder pool as gone and delete a live holder's token, and a cached Get of a Lease would
	// start an informer on every Lease of the management cluster. Required.
	APIReader client.Reader
	// Clock stamps the Lease's acquireTime, which tells a human how long the token has been held. Required.
	Clock clock.PassiveClock
	// Cluster owns the token: the Lease is named after it, lives in its namespace and carries a non-controller owner
	// reference to it, so it is garbage-collected with the Cluster. Required, with name, namespace and UID.
	Cluster *clusterv1.Cluster
	// Pool is the IBMCloudManagedMachinePool that acquires and releases the token. Required, with name and UID, in the
	// Cluster's namespace.
	Pool *infrav1.IBMCloudManagedMachinePool
}

// DisruptionToken acquires, inspects and releases one pool's claim on its Cluster's disruption token. Create it per
// reconcile with NewDisruptionToken; it keeps no state between calls.
type DisruptionToken struct {
	// params are the validated inputs.
	params DisruptionTokenParams
	// key is the namespace and name of the Lease.
	key types.NamespacedName
	// holder is this pool's holderIdentity.
	holder string
}

// NewDisruptionToken validates params and returns the token handle of params.Pool. It makes no API call.
//
// Parameters:
//   - params: the clients, the clock, the owning Cluster and the pool; see DisruptionTokenParams.
//
// It returns an error naming every missing or inconsistent input.
func NewDisruptionToken(params DisruptionTokenParams) (*DisruptionToken, error) {
	var problems []string
	if params.Client == nil {
		problems = append(problems, "Client is nil; pass the manager's client")
	}
	if params.APIReader == nil {
		problems = append(problems, "APIReader is nil; pass the manager's API reader (mgr.GetAPIReader())")
	}
	if params.Clock == nil {
		problems = append(problems, "Clock is nil; pass the controller's clock")
	}
	if params.Cluster == nil || params.Cluster.Name == "" || params.Cluster.Namespace == "" || params.Cluster.UID == "" {
		problems = append(problems, "Cluster must be the owning Cluster with name, namespace and UID")
	}
	if params.Pool == nil || params.Pool.Name == "" || params.Pool.UID == "" {
		problems = append(problems, "Pool must be the IBMCloudManagedMachinePool with name and UID")
	}
	if len(problems) == 0 && params.Pool.Namespace != params.Cluster.Namespace {
		problems = append(problems, fmt.Sprintf("Pool is in namespace %q but its Cluster in %q; a pool and its Cluster share a namespace",
			params.Pool.Namespace, params.Cluster.Namespace))
	}
	if len(problems) > 0 {
		return nil, errors.New("disruption token: " + strings.Join(problems, "; "))
	}
	return &DisruptionToken{
		params: params,
		key:    types.NamespacedName{Namespace: params.Cluster.Namespace, Name: DisruptionTokenName(params.Cluster.Name)},
		holder: DisruptionTokenHolder(params.Pool),
	}, nil
}

// DisruptionTokenName returns the name of a Cluster's disruption token Lease, "<cluster-name>-ibm-worker-disruption".
//
// Parameters:
//   - clusterName: the Cluster's name.
func DisruptionTokenName(clusterName string) string {
	return clusterName + disruptionLeaseSuffix
}

// DisruptionTokenHolder returns a pool's holderIdentity, "<IBMCloudManagedMachinePool name>/<uid>". The UID tells a pool
// apart from a recreated pool of the same name, whose token is stale.
//
// Parameters:
//   - pool: the IBMCloudManagedMachinePool.
func DisruptionTokenHolder(pool *infrav1.IBMCloudManagedMachinePool) string {
	return pool.Name + "/" + string(pool.UID)
}

// Status reports the token's state for this pool without changing anything: TokenHeld when the Lease names this pool,
// TokenHeldByOther when it names another live pool or an unparseable holder, and TokenNotHeld when there is no Lease
// or its holder no longer exists (Acquire replaces such a Lease).
//
// Parameters:
//   - ctx: bounds the API calls.
func (t *DisruptionToken) Status(ctx context.Context) (TokenStatus, error) {
	lease, err := t.get(ctx)
	if apierrors.IsNotFound(err) {
		return TokenStatus{State: TokenNotHeld}, nil
	}
	if err != nil {
		return TokenStatus{}, err
	}
	holder := leaseHolder(lease)
	stale, err := t.isStale(ctx, holder)
	if err != nil {
		return TokenStatus{}, err
	}
	if stale {
		return TokenStatus{State: TokenNotHeld, Holder: holder}, nil
	}
	return TokenStatus{State: t.stateOf(holder), Holder: holder}, nil
}

// Acquire takes the token for this pool when no live pool holds it: it creates the Lease; when the Lease exists, this
// pool keeps it, a stale holder's Lease is deleted with UID and resourceVersion preconditions and created anew, and a
// live other holder is reported as TokenHeldByOther. Acquiring a token the pool already holds is a no-op, so a
// restarted controller re-acquires its own token.
//
// Parameters:
//   - ctx: bounds the API calls.
//
// It returns an error when an API call fails, and when other writers changed the Lease in each of maxTokenAttempts
// rounds; the caller requeues.
func (t *DisruptionToken) Acquire(ctx context.Context) (TokenStatus, error) {
	for range maxTokenAttempts {
		err := t.params.Client.Create(ctx, t.newLease())
		if err == nil {
			return TokenStatus{State: TokenHeld, Holder: t.holder}, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return TokenStatus{}, createError(t.key, err)
		}
		lease, err := t.get(ctx)
		if apierrors.IsNotFound(err) {
			continue // released in between: create again
		}
		if err != nil {
			return TokenStatus{}, err
		}
		holder := leaseHolder(lease)
		stale, err := t.isStale(ctx, holder)
		if err != nil {
			return TokenStatus{}, err
		}
		if !stale {
			return TokenStatus{State: t.stateOf(holder), Holder: holder}, nil
		}
		if err := t.deleteExact(ctx, lease); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return TokenStatus{}, fmt.Errorf("replacing the stale disruption token Lease %s of %q: %w", t.key, holder, err)
		}
		// Deleted, or changed by another writer in between: the next round sees the current state.
	}
	return TokenStatus{}, fmt.Errorf("the disruption token Lease %s changed during each of %d attempts to acquire it; "+
		"retrying on the next reconcile", t.key, maxTokenAttempts)
}

// Release gives the token back when this pool holds it (when the pool returns to idle, and in reconcileDelete before
// the finalizer goes). It never deletes another pool's token, and a missing Lease is already released.
//
// Parameters:
//   - ctx: bounds the API calls.
//
// It returns an error when an API call fails or the Lease changed between the read and the delete; the caller requeues.
func (t *DisruptionToken) Release(ctx context.Context) error {
	lease, err := t.get(ctx)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if leaseHolder(lease) != t.holder {
		return nil
	}
	if err := t.deleteExact(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("releasing the disruption token Lease %s: %w", t.key, err)
	}
	return nil
}

// newLease returns the Lease that represents this pool's token.
func (t *DisruptionToken) newLease() *coordinationv1.Lease {
	cluster := t.params.Cluster
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t.key.Namespace,
			Name:      t.key.Name,
			Labels:    map[string]string{clusterv1.ClusterNameLabel: cluster.Name},
			// Not a controller reference: the Cluster does not manage the Lease, it only takes it along when deleted.
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: clusterv1.GroupVersion.String(),
				Kind:       "Cluster",
				Name:       cluster.Name,
				UID:        cluster.UID,
			}},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: ptr.To(t.holder),
			AcquireTime:    ptr.To(metav1.NewMicroTime(t.params.Clock.Now())),
		},
	}
}

// get reads the Lease through the API reader.
//
// Parameters:
//   - ctx: bounds the call.
func (t *DisruptionToken) get(ctx context.Context) (*coordinationv1.Lease, error) {
	lease := &coordinationv1.Lease{}
	if err := t.params.APIReader.Get(ctx, t.key, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, fmt.Errorf("reading the disruption token Lease %s: %w", t.key, err)
	}
	return lease, nil
}

// deleteExact deletes lease only if it is still the object that was read (same UID and resourceVersion), so a token
// another pool took in between is never deleted.
//
// Parameters:
//   - ctx: bounds the call.
//   - lease: the Lease as read.
func (t *DisruptionToken) deleteExact(ctx context.Context, lease *coordinationv1.Lease) error {
	return t.params.Client.Delete(ctx, lease, client.Preconditions{
		UID:             ptr.To(lease.UID),
		ResourceVersion: ptr.To(lease.ResourceVersion),
	})
}

// isStale reports whether a holder no longer exists: an empty holder, or a pool that is gone or was recreated with
// another UID. An unparseable holder is not stale: CAPIBM did not write it, so a human decides.
//
// Parameters:
//   - ctx: bounds the call.
//   - holder: the Lease's holderIdentity.
func (t *DisruptionToken) isStale(ctx context.Context, holder string) (bool, error) {
	if holder == "" {
		return true, nil
	}
	if holder == t.holder {
		return false, nil
	}
	name, uid, ok := parseTokenHolder(holder)
	if !ok {
		return false, nil
	}
	pool := &infrav1.IBMCloudManagedMachinePool{}
	err := t.params.APIReader.Get(ctx, types.NamespacedName{Namespace: t.key.Namespace, Name: name}, pool)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading IBMCloudManagedMachinePool %s/%s, the holder of the disruption token Lease %s: %w",
			t.key.Namespace, name, t.key, err)
	}
	return string(pool.UID) != uid, nil
}

// stateOf returns TokenHeld for this pool's holder and TokenHeldByOther for any other.
//
// Parameters:
//   - holder: a holder that is not stale.
func (t *DisruptionToken) stateOf(holder string) TokenState {
	if holder == t.holder {
		return TokenHeld
	}
	return TokenHeldByOther
}

// createError wraps an error of the Lease create; a forbidden answer names the RBAC rule CAPIBM needs.
//
// Parameters:
//   - key: the Lease.
//   - err: the create error.
func createError(key types.NamespacedName, err error) error {
	if apierrors.IsForbidden(err) {
		return fmt.Errorf("creating the disruption token Lease %s: %w; CAPIBM needs get, create and delete on "+
			"coordination.k8s.io leases", key, err)
	}
	return fmt.Errorf("creating the disruption token Lease %s: %w", key, err)
}

// leaseHolder returns the Lease's holderIdentity, "" when unset.
//
// Parameters:
//   - lease: the Lease as read.
func leaseHolder(lease *coordinationv1.Lease) string {
	return ptr.Deref(lease.Spec.HolderIdentity, "")
}

// parseTokenHolder splits "<name>/<uid>"; ok is false for any other form.
//
// Parameters:
//   - holder: a holderIdentity.
func parseTokenHolder(holder string) (name, uid string, ok bool) {
	name, uid, found := strings.Cut(holder, "/")
	if !found || name == "" || uid == "" || strings.Contains(uid, "/") {
		return "", "", false
	}
	return name, uid, true
}

// DrainOwner returns the drain-owner marker value of a pool, "<namespace>/<IBMCloudManagedMachinePool name>"
// (drain.CordonOptions.Owner).
//
// Parameters:
//   - pool: the IBMCloudManagedMachinePool that drains.
func DrainOwner(pool *infrav1.IBMCloudManagedMachinePool) string {
	return pool.Namespace + "/" + pool.Name
}

// OtherDrainOwners returns, sorted and without duplicates, the drain-owner markers on nodes that name another pool than
// self. Before it takes the token, a pool defers to such a Node: clusterctl move does not move the Lease, so after a
// move the marker is what still shows that another pool's drain is half done.
//
// Parameters:
//   - nodes: every Node of the workload cluster.
//   - self: the calling pool's DrainOwner.
func OtherDrainOwners(nodes []corev1.Node, self string) []string {
	key := infrav1.NodeAnnotationPrefix + drain.MarkerDrainOwner
	seen := map[string]bool{}
	var others []string
	for i := range nodes {
		owner, found := nodes[i].Annotations[key]
		if !found || owner == "" || owner == self || seen[owner] {
			continue
		}
		seen[owner] = true
		others = append(others, owner)
	}
	sort.Strings(others)
	return others
}
