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
	"testing"
	"time"

	. "github.com/onsi/gomega"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// The token tests run against the controller-runtime fake client; interceptor.Funcs play the other writers of the races
// that the preconditions guard against.

const tokenNamespace = "capi-managed"

// tokenCluster is the Cluster that owns the token.
func tokenCluster() *clusterv1.Cluster {
	return &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: tokenNamespace, UID: "cluster-uid"}}
}

// tokenPool returns an IBMCloudManagedMachinePool of the token's Cluster.
func tokenPool(name, uid string) *infrav1.IBMCloudManagedMachinePool {
	return &infrav1.IBMCloudManagedMachinePool{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tokenNamespace, UID: types.UID(uid)}}
}

// tokenLease returns the token Lease with the given holder (nil: no holderIdentity).
func tokenLease(holder *string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-ibm-worker-disruption", Namespace: tokenNamespace},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: holder},
	}
}

// tokenKey is the namespace and name of the token Lease.
func tokenKey() types.NamespacedName {
	return types.NamespacedName{Namespace: tokenNamespace, Name: "dev-ibm-worker-disruption"}
}

// tokenScheme registers the types the token reads and writes.
func tokenScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := infrav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// newTestToken returns the token of pool over a fake client holding objs, with funcs intercepting the token's calls,
// and the fake client itself, which the test reads without the interceptors.
func newTestToken(t *testing.T, pool *infrav1.IBMCloudManagedMachinePool, funcs interceptor.Funcs, objs ...client.Object) (*DisruptionToken, client.Client, *clocktesting.FakePassiveClock) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(tokenScheme(t)).WithObjects(objs...).Build()
	intercepted := interceptor.NewClient(c, funcs)
	clock := clocktesting.NewFakePassiveClock(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	token, err := NewDisruptionToken(DisruptionTokenParams{Client: intercepted, APIReader: intercepted, Clock: clock, Cluster: tokenCluster(), Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	return token, c, clock
}

// readLease returns the token Lease from c, or nil when it does not exist.
func readLease(g Gomega, c client.Client) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{}
	err := c.Get(context.Background(), tokenKey(), lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	g.Expect(err).NotTo(HaveOccurred())
	return lease
}

func TestNewDisruptionToken(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(tokenScheme(t)).Build()
	clock := clocktesting.NewFakePassiveClock(time.Now())
	valid := func() DisruptionTokenParams {
		return DisruptionTokenParams{Client: c, APIReader: c, Clock: clock, Cluster: tokenCluster(), Pool: tokenPool("pool-a", "uid-a")}
	}
	testCases := []struct {
		name    string
		modify  func(p *DisruptionTokenParams)
		wantErr []string
	}{
		{name: "valid", modify: func(*DisruptionTokenParams) {}},
		{name: "every missing input is named", modify: func(p *DisruptionTokenParams) { *p = DisruptionTokenParams{} },
			wantErr: []string{"Client is nil", "APIReader is nil", "Clock is nil", "Cluster must be", "Pool must be"}},
		{name: "Cluster without a UID", modify: func(p *DisruptionTokenParams) { p.Cluster.UID = "" }, wantErr: []string{"Cluster must be the owning Cluster with name, namespace and UID"}},
		{name: "Pool without a UID", modify: func(p *DisruptionTokenParams) { p.Pool.UID = "" }, wantErr: []string{"Pool must be the IBMCloudManagedMachinePool with name and UID"}},
		{name: "Pool in another namespace", modify: func(p *DisruptionTokenParams) { p.Pool.Namespace = "other" }, wantErr: []string{`Pool is in namespace "other" but its Cluster in "capi-managed"`}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			params := valid()
			tc.modify(&params)
			token, err := NewDisruptionToken(params)
			if len(tc.wantErr) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(token).NotTo(BeNil())
				return
			}
			g.Expect(token).To(BeNil())
			for _, part := range tc.wantErr {
				g.Expect(err).To(MatchError(ContainSubstring(part)))
			}
		})
	}
}

func TestDisruptionTokenNames(t *testing.T) {
	g := NewWithT(t)
	pool := tokenPool("pool-a", "uid-a")
	g.Expect(DisruptionTokenName("dev")).To(Equal("dev-ibm-worker-disruption"))
	g.Expect(DisruptionTokenHolder(pool)).To(Equal("pool-a/uid-a"))
	g.Expect(DrainOwner(pool)).To(Equal("capi-managed/pool-a"))
}

func TestDisruptionTokenAcquire(t *testing.T) {
	poolA := tokenPool("pool-a", "uid-a")
	poolB := tokenPool("pool-b", "uid-b")
	testCases := []struct {
		name       string
		objects    []client.Object
		funcs      func(calls *int) interceptor.Funcs
		want       TokenState
		wantHolder string
		wantLease  *string // the Lease's holder afterwards; nil: no Lease
		wantErr    string
	}{
		{name: "no Lease: created", want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a")},
		{name: "own Lease: re-acquired by the holder", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))},
			want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a")},
		{name: "a second pool waits while the first holds the token", objects: []client.Object{poolB, tokenLease(ptr.To("pool-b/uid-b"))},
			want: TokenHeldByOther, wantHolder: "pool-b/uid-b", wantLease: ptr.To("pool-b/uid-b")},
		{name: "stale holder: pool gone, replaced", objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))},
			want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a")},
		{name: "stale holder: pool recreated with another UID, replaced", objects: []client.Object{tokenPool("pool-b", "uid-b2"), tokenLease(ptr.To("pool-b/uid-b"))},
			want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a")},
		{name: "stale holder: this pool's earlier incarnation, replaced", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-old"))},
			want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a")},
		{name: "empty holder: replaced", objects: []client.Object{tokenLease(nil)},
			want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a")},
		{name: "unparseable holder: left to a human", objects: []client.Object{tokenLease(ptr.To("written-by-someone-else"))},
			want: TokenHeldByOther, wantHolder: "written-by-someone-else", wantLease: ptr.To("written-by-someone-else")},
		{
			name: "race: the Lease is released between the create and the read",
			funcs: func(calls *int) interceptor.Funcs {
				return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					*calls++
					if *calls == 1 {
						return apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName())
					}
					return c.Create(ctx, obj, opts...)
				}}
			},
			want: TokenHeld, wantHolder: "pool-a/uid-a", wantLease: ptr.To("pool-a/uid-a"),
		},
		{
			name:    "race: another pool replaces the stale holder first (conflict), then holds the token",
			objects: []client.Object{poolB, tokenLease(ptr.To("pool-gone/uid-gone"))},
			funcs: func(*int) interceptor.Funcs {
				return interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					// Pool B replaced the stale Lease between our read and our delete.
					lease := &coordinationv1.Lease{}
					if err := c.Get(ctx, client.ObjectKeyFromObject(obj), lease); err != nil {
						return err
					}
					lease.Spec.HolderIdentity = ptr.To("pool-b/uid-b")
					if err := c.Update(ctx, lease); err != nil {
						return err
					}
					return c.Delete(ctx, obj, opts...)
				}}
			},
			want: TokenHeldByOther, wantHolder: "pool-b/uid-b", wantLease: ptr.To("pool-b/uid-b"),
		},
		{
			name: "persistent contention gives up after maxTokenAttempts",
			funcs: func(calls *int) interceptor.Funcs {
				return interceptor.Funcs{
					Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
						*calls++
						return apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName())
					},
				}
			},
			wantErr: "changed during each of 3 attempts",
		},
		{
			name: "create forbidden names the RBAC the token needs",
			funcs: func(*int) interceptor.Funcs {
				return interceptor.Funcs{Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
					return apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), errors.New("no RBAC"))
				}}
			},
			wantErr: "needs get, create and delete on coordination.k8s.io leases",
		},
		{
			name: "another create error is wrapped without the RBAC hint",
			funcs: func(*int) interceptor.Funcs {
				return interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
					return errors.New("namespace capi-managed is terminating")
				}}
			},
			wantErr: "creating the disruption token Lease capi-managed/dev-ibm-worker-disruption: namespace capi-managed is terminating",
		},
		{
			name:    "reading the Lease fails",
			objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))},
			funcs: func(*int) interceptor.Funcs {
				return interceptor.Funcs{Get: failLeaseReads()}
			},
			wantErr: "reading the disruption token Lease capi-managed/dev-ibm-worker-disruption",
		},
		{
			name:    "reading the holder pool fails",
			objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))},
			funcs: func(*int) interceptor.Funcs {
				return interceptor.Funcs{Get: failPoolReads()}
			},
			wantErr: "reading IBMCloudManagedMachinePool capi-managed/pool-b, the holder of the disruption token Lease",
		},
		{
			name:    "deleting a stale Lease fails",
			objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))},
			funcs: func(*int) interceptor.Funcs {
				return interceptor.Funcs{Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					return errors.New("API server unavailable")
				}}
			},
			wantErr: `replacing the stale disruption token Lease capi-managed/dev-ibm-worker-disruption of "pool-b/uid-b"`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			calls := 0
			funcs := interceptor.Funcs{}
			if tc.funcs != nil {
				funcs = tc.funcs(&calls)
			}
			token, c, clock := newTestToken(t, poolA, funcs, tc.objects...)

			got, err := token.Acquire(context.Background())
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(TokenStatus{State: tc.want, Holder: tc.wantHolder}))

			lease := readLease(g, c)
			g.Expect(lease).NotTo(BeNil())
			g.Expect(lease.Spec.HolderIdentity).To(Equal(tc.wantLease))
			if got.State == TokenHeld && len(tc.objects) == 0 {
				// A Lease this pool created: owned by the Cluster without being controlled by it, labelled with the
				// Cluster name, never renewed.
				g.Expect(lease.OwnerReferences).To(ConsistOf(metav1.OwnerReference{
					APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: "dev", UID: "cluster-uid",
				}))
				g.Expect(lease.Labels).To(HaveKeyWithValue(clusterv1.ClusterNameLabel, "dev"))
				g.Expect(lease.Spec.AcquireTime.Time.Equal(clock.Now())).To(BeTrue())
				g.Expect(lease.Spec.LeaseDurationSeconds).To(BeNil())
				g.Expect(lease.Spec.RenewTime).To(BeNil())
			}
		})
	}
}

// getInterceptor is the signature of interceptor.Funcs.Get.
type getInterceptor = func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error

// failLeaseReads returns a Get interceptor that fails every read of a Lease.
func failLeaseReads() getInterceptor {
	return failReadsOf(func(obj client.Object) bool {
		_, isLease := obj.(*coordinationv1.Lease)
		return isLease
	})
}

// failPoolReads returns a Get interceptor that fails every read of an IBMCloudManagedMachinePool.
func failPoolReads() getInterceptor {
	return failReadsOf(func(obj client.Object) bool {
		_, isPool := obj.(*infrav1.IBMCloudManagedMachinePool)
		return isPool
	})
}

// failReadsOf returns a Get interceptor that fails the reads of the objects match selects and passes every other read
// through.
func failReadsOf(match func(client.Object) bool) getInterceptor {
	return func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if match(obj) {
			return errors.New("API server unavailable")
		}
		return c.Get(ctx, key, obj, opts...)
	}
}

// TestDisruptionTokenDeletesOnlyWhatItRead proves the delete preconditions of the token: a stale Lease is deleted only
// with its UID and resourceVersion, so a Lease another pool wrote in between is never deleted.
func TestDisruptionTokenDeletesOnlyWhatItRead(t *testing.T) {
	g := NewWithT(t)
	var preconditions []*metav1.Preconditions
	funcs := interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		options := &client.DeleteOptions{}
		options.ApplyOptions(opts)
		preconditions = append(preconditions, options.Preconditions)
		return c.Delete(ctx, obj, opts...)
	}}
	token, c, _ := newTestToken(t, tokenPool("pool-a", "uid-a"), funcs, tokenLease(ptr.To("pool-gone/uid-gone")))
	stale := readLease(g, c)

	got, err := token.Acquire(context.Background())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(got.State).To(Equal(TokenHeld))
	g.Expect(preconditions).To(HaveLen(1))
	g.Expect(preconditions[0]).NotTo(BeNil())
	g.Expect(preconditions[0].UID).To(Equal(ptr.To(stale.UID)))
	g.Expect(preconditions[0].ResourceVersion).To(Equal(ptr.To(stale.ResourceVersion)))
}

func TestDisruptionTokenStatus(t *testing.T) {
	poolA := tokenPool("pool-a", "uid-a")
	testCases := []struct {
		name    string
		objects []client.Object
		funcs   interceptor.Funcs
		want    TokenStatus
		wantErr string
	}{
		{name: "no Lease", want: TokenStatus{State: TokenNotHeld}},
		{name: "own Lease", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))}, want: TokenStatus{State: TokenHeld, Holder: "pool-a/uid-a"}},
		{name: "live other holder", objects: []client.Object{tokenPool("pool-b", "uid-b"), tokenLease(ptr.To("pool-b/uid-b"))},
			want: TokenStatus{State: TokenHeldByOther, Holder: "pool-b/uid-b"}},
		{name: "stale holder can be taken", objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))},
			want: TokenStatus{State: TokenNotHeld, Holder: "pool-b/uid-b"}},
		{name: "reading the Lease fails", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))},
			funcs: interceptor.Funcs{Get: failLeaseReads()}, wantErr: "reading the disruption token Lease"},
		{name: "reading the holder pool fails", objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))},
			funcs: interceptor.Funcs{Get: failPoolReads()}, wantErr: "reading IBMCloudManagedMachinePool capi-managed/pool-b"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			token, c, _ := newTestToken(t, poolA, tc.funcs, tc.objects...)
			before := readLease(g, c)

			got, err := token.Status(context.Background())
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
			g.Expect(readLease(g, c)).To(Equal(before), "Status never writes")
		})
	}
}

func TestDisruptionTokenRelease(t *testing.T) {
	poolA := tokenPool("pool-a", "uid-a")
	testCases := []struct {
		name      string
		objects   []client.Object
		funcs     interceptor.Funcs
		wantLease *string
		wantErr   string
	}{
		{name: "own Lease is deleted", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))}},
		{name: "no Lease is already released"},
		{name: "another pool's Lease is never deleted", objects: []client.Object{tokenLease(ptr.To("pool-b/uid-b"))}, wantLease: ptr.To("pool-b/uid-b")},
		{name: "a Lease that changed between read and delete is kept", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))},
			funcs: interceptor.Funcs{Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "dev-ibm-worker-disruption", errors.New("changed"))
			}},
			wantLease: ptr.To("pool-a/uid-a"), wantErr: "releasing the disruption token Lease capi-managed/dev-ibm-worker-disruption"},
		{name: "a Lease deleted in between is released", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))},
			funcs: interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
				if err := c.Delete(ctx, obj); err != nil {
					return err
				}
				return apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName())
			}}},
		{name: "reading the Lease fails", objects: []client.Object{tokenLease(ptr.To("pool-a/uid-a"))},
			funcs: interceptor.Funcs{Get: failLeaseReads()}, wantLease: ptr.To("pool-a/uid-a"), wantErr: "reading the disruption token Lease"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			token, c, _ := newTestToken(t, poolA, tc.funcs, tc.objects...)

			err := token.Release(context.Background())
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}
			lease := readLease(g, c)
			if tc.wantLease == nil {
				g.Expect(lease).To(BeNil())
				return
			}
			g.Expect(lease).NotTo(BeNil())
			g.Expect(lease.Spec.HolderIdentity).To(Equal(tc.wantLease))
		})
	}
}

// TestDisruptionTokenSerializesPools plays two pools of one Cluster through a removal each: only one holds the token at
// a time, and the second gets it once the first released it.
func TestDisruptionTokenSerializesPools(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	poolA, poolB := tokenPool("pool-a", "uid-a"), tokenPool("pool-b", "uid-b")
	c := fake.NewClientBuilder().WithScheme(tokenScheme(t)).WithObjects(poolA, poolB).Build()
	clock := clocktesting.NewFakePassiveClock(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	tokenOf := func(pool *infrav1.IBMCloudManagedMachinePool) *DisruptionToken {
		token, err := NewDisruptionToken(DisruptionTokenParams{Client: c, APIReader: c, Clock: clock, Cluster: tokenCluster(), Pool: pool})
		g.Expect(err).NotTo(HaveOccurred())
		return token
	}
	a, b := tokenOf(poolA), tokenOf(poolB)

	g.Expect(a.Acquire(ctx)).To(Equal(TokenStatus{State: TokenHeld, Holder: "pool-a/uid-a"}))
	t.Run("the second pool waits for the token", func(t *testing.T) {
		g := NewWithT(t)
		g.Expect(b.Acquire(ctx)).To(Equal(TokenStatus{State: TokenHeldByOther, Holder: "pool-a/uid-a"}))
		g.Expect(b.Release(ctx)).To(Succeed(), "releasing a token another pool holds is a no-op")
		g.Expect(a.Status(ctx)).To(Equal(TokenStatus{State: TokenHeld, Holder: "pool-a/uid-a"}))
	})

	g.Expect(a.Release(ctx)).To(Succeed())
	g.Expect(b.Acquire(ctx)).To(Equal(TokenStatus{State: TokenHeld, Holder: "pool-b/uid-b"}))
	g.Expect(a.Status(ctx)).To(Equal(TokenStatus{State: TokenHeldByOther, Holder: "pool-b/uid-b"}))
}

func TestOtherDrainOwners(t *testing.T) {
	key := infrav1.NodeAnnotationPrefix + "drain-owner"
	node := func(name, owner string) corev1.Node {
		n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if owner != "" {
			n.Annotations = map[string]string{key: owner}
		}
		return n
	}
	testCases := []struct {
		name  string
		nodes []corev1.Node
		want  []string
	}{
		{name: "no markers", nodes: []corev1.Node{node("10.0.0.1", ""), node("10.0.0.2", "")}, want: nil},
		{name: "only this pool's marker", nodes: []corev1.Node{node("10.0.0.1", "ns/pool-a")}, want: nil},
		{name: "another pool's marker (a drain half done before a move)", nodes: []corev1.Node{node("10.0.0.1", "ns/pool-a"), node("10.0.0.2", "ns/pool-b")},
			want: []string{"ns/pool-b"}},
		{name: "sorted and without duplicates", nodes: []corev1.Node{node("10.0.0.1", "ns/pool-c"), node("10.0.0.2", "ns/pool-b"), node("10.0.0.3", "ns/pool-c")},
			want: []string{"ns/pool-b", "ns/pool-c"}},
		{name: "an empty marker names no pool", nodes: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "10.0.0.1", Annotations: map[string]string{key: ""}}}}, want: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(OtherDrainOwners(tc.nodes, "ns/pool-a")).To(Equal(tc.want))
		})
	}
}

// TestDisruptionTokenDrainingPools checks that a drain-owner marker counts only while its pool exists: the marker of a
// pool deleted mid-drain would otherwise hold every worker removal of the Cluster until a human removed it.
func TestDisruptionTokenDrainingPools(t *testing.T) {
	key := infrav1.NodeAnnotationPrefix + "drain-owner"
	marked := func(name, owner string) corev1.Node {
		return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{key: owner}}}
	}
	nodes := []corev1.Node{marked("10.0.0.1", tokenNamespace+"/pool-a"), marked("10.0.0.2", tokenNamespace+"/pool-b"),
		marked("10.0.0.3", tokenNamespace+"/pool-gone"), marked("10.0.0.4", "written-by-hand"), marked("10.0.0.5", "elsewhere/pool-x")}
	testCases := []struct {
		name    string
		objects []client.Object
		funcs   interceptor.Funcs
		want    []string
		wantErr string
	}{
		{name: "a live pool is kept, a deleted one is left out, a marker no pool of the Cluster wrote is kept for a human",
			objects: []client.Object{tokenPool("pool-b", "uid-b")}, want: []string{tokenNamespace + "/pool-b", "elsewhere/pool-x", "written-by-hand"}},
		{name: "a recreated pool of the same name still owns its marker", objects: []client.Object{tokenPool("pool-b", "uid-new")},
			want: []string{tokenNamespace + "/pool-b", "elsewhere/pool-x", "written-by-hand"}},
		{name: "a failed read is no proof that the pool is gone", objects: []client.Object{tokenPool("pool-b", "uid-b")},
			funcs: interceptor.Funcs{Get: failPoolReads()}, wantErr: "reading IBMCloudManagedMachinePool capi-managed/pool-b, whose drain-owner " +
				"marker is on a Node of Cluster dev"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			token, _, _ := newTestToken(t, tokenPool("pool-a", "uid-a"), tc.funcs, tc.objects...)
			got, err := token.DrainingPools(context.Background(), nodes)
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
	t.Run("no other marker makes no call", func(t *testing.T) {
		g := NewWithT(t)
		token, _, _ := newTestToken(t, tokenPool("pool-a", "uid-a"), interceptor.Funcs{Get: failPoolReads()})
		g.Expect(token.DrainingPools(context.Background(), nodes[:1])).To(BeEmpty())
	})
}

func TestCreateError(t *testing.T) {
	g := NewWithT(t)
	key := tokenKey()
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, key.Name, errors.New("no RBAC"))
	g.Expect(createError(key, forbidden)).To(MatchError(HaveSuffix("; CAPIBM needs get, create and delete on coordination.k8s.io leases")))
	g.Expect(errors.Is(createError(key, forbidden), forbidden)).To(BeTrue())

	other := errors.New("namespace capi-managed is terminating")
	g.Expect(createError(key, other)).To(MatchError("creating the disruption token Lease capi-managed/dev-ibm-worker-disruption: " +
		"namespace capi-managed is terminating"))
}
