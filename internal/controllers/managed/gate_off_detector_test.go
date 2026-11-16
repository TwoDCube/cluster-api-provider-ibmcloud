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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"

	. "github.com/onsi/gomega"
)

// The gate-off detector tests run the one-shot runnable against a fake client: one Warning event per
// IBMCloudManagedControlPlane, none without any, retries of a failed list, and nothing that could stop the manager
// except a missing dependency.

// newGateOffFixture returns a detector over a fake client holding objs, with fast retries, and its event recorder.
//
// Parameters:
//   - t is the test.
//   - list, when set, replaces the fake client's List (to inject failures).
//   - objs are the stored objects.
func newGateOffFixture(t *testing.T, list func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error,
	objs ...client.Object) (*GateOffDetector, *record.FakeRecorder) {
	t.Helper()
	s := runtime.NewScheme()
	NewWithT(t).Expect(controlplanev1.AddToScheme(s)).To(Succeed())
	builder := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...)
	if list != nil {
		builder = builder.WithInterceptorFuncs(interceptor.Funcs{List: list})
	}
	recorder := record.NewFakeRecorder(10)
	detector := NewGateOffDetector(builder.Build(), recorder)
	detector.retryInterval = time.Millisecond
	return detector, recorder
}

// gateOffControlPlane returns a control plane named name in namespace.
func gateOffControlPlane(namespace, name string) *controlplanev1.IBMCloudManagedControlPlane {
	return &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// drainEvents returns the events the recorder holds.
func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func TestGateOffDetector(t *testing.T) {
	want := "Warning ManagedKubernetesDisabled feature gate ManagedKubernetes is off: this cluster is not reconciled and " +
		"cannot be deleted; set EXP_MANAGED_KUBERNETES=true"

	t.Run("one warning per control plane", func(t *testing.T) {
		g := NewWithT(t)
		detector, recorder := newGateOffFixture(t, nil, gateOffControlPlane("a", "dev"), gateOffControlPlane("b", "prod"))

		g.Expect(detector.Start(ctx)).To(Succeed())

		g.Expect(drainEvents(recorder)).To(Equal([]string{want, want}))
	})

	t.Run("no warning without a control plane", func(t *testing.T) {
		g := NewWithT(t)
		detector, recorder := newGateOffFixture(t, nil)
		g.Expect(detector.Start(ctx)).To(Succeed())
		g.Expect(drainEvents(recorder)).To(BeEmpty())
	})

	t.Run("a failed list is retried", func(t *testing.T) {
		g := NewWithT(t)
		failures := 2
		list := func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if failures > 0 {
				failures--
				return errors.New("the API server is starting")
			}
			return c.List(ctx, list, opts...)
		}
		detector, recorder := newGateOffFixture(t, list, gateOffControlPlane("a", "dev"))

		g.Expect(detector.Start(ctx)).To(Succeed())

		g.Expect(drainEvents(recorder)).To(Equal([]string{want}))
	})

	t.Run("a list that keeps failing is given up without stopping the manager", func(t *testing.T) {
		g := NewWithT(t)
		calls := 0
		list := func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			calls++
			return errors.New("forbidden")
		}
		detector, recorder := newGateOffFixture(t, list, gateOffControlPlane("a", "dev"))

		g.Expect(detector.Start(ctx)).To(Succeed())

		g.Expect(calls).To(Equal(gateOffListAttempts))
		g.Expect(drainEvents(recorder)).To(BeEmpty())
	})

	t.Run("a missing CRD means no control plane", func(t *testing.T) {
		g := NewWithT(t)
		list := func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: controlplanev1.GroupVersion.Group, Kind: "IBMCloudManagedControlPlane"}}
		}
		detector, recorder := newGateOffFixture(t, list)

		g.Expect(detector.Start(ctx)).To(Succeed())
		g.Expect(drainEvents(recorder)).To(BeEmpty())
	})

	t.Run("pages are followed", func(t *testing.T) {
		g := NewWithT(t)
		pages := 0
		list := func(_ context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			pages++
			options := &client.ListOptions{}
			options.ApplyOptions(opts)
			g.Expect(options.Limit).To(Equal(int64(gateOffListPageSize)))
			page := list.(*controlplanev1.IBMCloudManagedControlPlaneList)
			page.Items = []controlplanev1.IBMCloudManagedControlPlane{*gateOffControlPlane("ns", fmt.Sprintf("cp-%d", pages))}
			if pages == 1 {
				page.Continue = "next"
			} else {
				g.Expect(options.Continue).To(Equal("next"))
				page.Continue = ""
			}
			return nil
		}
		detector, recorder := newGateOffFixture(t, list)

		g.Expect(detector.Start(ctx)).To(Succeed())

		g.Expect(pages).To(Equal(2))
		g.Expect(drainEvents(recorder)).To(HaveLen(2))
	})

	t.Run("a context that ends stops the retries", func(t *testing.T) {
		g := NewWithT(t)
		list := func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("unavailable")
		}
		detector, _ := newGateOffFixture(t, list)
		detector.retryInterval = time.Hour
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		g.Expect(detector.Start(cancelled)).To(Succeed(), "the detector only warns")
	})

	t.Run("missing dependencies stop the start-up with a message", func(t *testing.T) {
		g := NewWithT(t)
		err := NewGateOffDetector(nil, nil).Start(ctx)
		g.Expect(err).To(MatchError(ContainSubstring("GateOffDetector needs a reader; pass mgr.GetAPIReader()")))
		g.Expect(err).To(MatchError(ContainSubstring("GateOffDetector needs an event recorder")))
	})

	t.Run("only the leader warns", func(t *testing.T) {
		NewWithT(t).Expect(NewGateOffDetector(nil, nil).NeedLeaderElection()).To(BeTrue())
	})
}
