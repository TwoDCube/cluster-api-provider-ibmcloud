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

	"github.com/go-logr/logr/funcr"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"

	. "github.com/onsi/gomega"
)

// The gate-off detector tests run the one-shot runnable against a fake client: one Warning event per
// IBMCloudManagedControlPlane of the manager's --namespace and --watch-filter scope, none without any, retries of a
// failed list, and nothing that could stop the manager except a missing dependency.

// newGateOffFixture returns a detector of the whole management cluster over a fake client holding objs, with fast
// retries, and its event recorder.
//
// Parameters:
//   - t is the test.
//   - list, when set, replaces the fake client's List (to inject failures).
//   - objs are the stored objects.
func newGateOffFixture(t *testing.T, list func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error,
	objs ...client.Object) (*GateOffDetector, *record.FakeRecorder) {
	t.Helper()
	return newScopedGateOffFixture(t, GateOffDetectorOptions{}, list, objs...)
}

// newScopedGateOffFixture is newGateOffFixture for a detector with the watch scope options.
//
// Parameters:
//   - t is the test.
//   - options is the detector's watch scope.
//   - list, when set, replaces the fake client's List.
//   - objs are the stored objects.
func newScopedGateOffFixture(t *testing.T, options GateOffDetectorOptions,
	list func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error, objs ...client.Object) (*GateOffDetector, *record.FakeRecorder) {
	t.Helper()
	s := runtime.NewScheme()
	NewWithT(t).Expect(controlplanev1.AddToScheme(s)).To(Succeed())
	builder := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...)
	if list != nil {
		builder = builder.WithInterceptorFuncs(interceptor.Funcs{List: list})
	}
	recorder := record.NewFakeRecorder(10)
	detector := NewGateOffDetector(builder.Build(), recorder, options)
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
		err := NewGateOffDetector(nil, nil, GateOffDetectorOptions{}).Start(ctx)
		g.Expect(err).To(MatchError(ContainSubstring("GateOffDetector needs a reader; pass mgr.GetAPIReader()")))
		g.Expect(err).To(MatchError(ContainSubstring("GateOffDetector needs an event recorder")))
	})

	t.Run("only the leader warns", func(t *testing.T) {
		NewWithT(t).Expect(NewGateOffDetector(nil, nil, GateOffDetectorOptions{}).NeedLeaderElection()).To(BeTrue())
	})
}

// warnedControlPlanes lists through c like the fake client and records the namespace/name of every control plane the
// list returns, which are the control planes the detector warns about; the options of each list call go to calls.
//
// Parameters:
//   - warned receives the listed control planes.
//   - calls receives the options of each list call.
func warnedControlPlanes(warned *[]string, calls *[]client.ListOptions) func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
	return func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		options := client.ListOptions{}
		options.ApplyOptions(opts)
		*calls = append(*calls, options)
		if err := c.List(ctx, list, opts...); err != nil {
			return err
		}
		for _, cp := range list.(*controlplanev1.IBMCloudManagedControlPlaneList).Items {
			*warned = append(*warned, cp.Namespace+"/"+cp.Name)
		}
		return nil
	}
}

// filtered returns a control plane named name in namespace whose cluster.x-k8s.io/watch-filter label is value.
//
// Parameters:
//   - namespace and name name the control plane.
//   - value is the watch-filter label value.
func filtered(namespace, name, value string) *controlplanev1.IBMCloudManagedControlPlane {
	cp := gateOffControlPlane(namespace, name)
	cp.Labels = map[string]string{clusterv1.WatchLabel: value}
	return cp
}

// TestGateOffDetectorScope checks that the detector warns only about the control planes this manager would reconcile
// with the gate on: those of its --namespace and with its --watch-filter label. Another instance with another scope may
// reconcile the rest, and a warning that they cannot be deleted would be a false alarm.
func TestGateOffDetectorScope(t *testing.T) {
	objects := func() []client.Object {
		return []client.Object{gateOffControlPlane("team-a", "dev"), gateOffControlPlane("team-b", "prod"),
			filtered("team-a", "filtered-a", "a"), filtered("team-b", "filtered-b", "b"), filtered("team-b", "filtered-a", "a")}
	}
	for _, tc := range []struct {
		name    string
		options GateOffDetectorOptions
		want    []string
	}{
		{name: "no scope checks every control plane", options: GateOffDetectorOptions{},
			want: []string{"team-a/dev", "team-a/filtered-a", "team-b/filtered-a", "team-b/filtered-b", "team-b/prod"}},
		{name: "--namespace checks only its namespace", options: GateOffDetectorOptions{WatchNamespace: "team-b"},
			want: []string{"team-b/filtered-a", "team-b/filtered-b", "team-b/prod"}},
		{name: "--watch-filter checks only the control planes with its label", options: GateOffDetectorOptions{WatchFilterValue: "a"},
			want: []string{"team-a/filtered-a", "team-b/filtered-a"}},
		{name: "both together", options: GateOffDetectorOptions{WatchNamespace: "team-b", WatchFilterValue: "b"},
			want: []string{"team-b/filtered-b"}},
		{name: "a scope without control planes warns about none", options: GateOffDetectorOptions{WatchNamespace: "team-c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			var warned []string
			var calls []client.ListOptions
			detector, recorder := newScopedGateOffFixture(t, tc.options, warnedControlPlanes(&warned, &calls), objects()...)

			g.Expect(detector.Start(ctx)).To(Succeed())

			g.Expect(warned).To(ConsistOf(tc.want))
			g.Expect(drainEvents(recorder)).To(HaveLen(len(tc.want)), "one warning per control plane of the scope")
			g.Expect(calls).To(HaveLen(1))
			g.Expect(calls[0].Namespace).To(Equal(tc.options.WatchNamespace))
		})
	}

	t.Run("every page keeps the scope", func(t *testing.T) {
		g := NewWithT(t)
		pages := 0
		list := func(_ context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			pages++
			options := &client.ListOptions{}
			options.ApplyOptions(opts)
			// The API server ties a continue token to the query: a later page must repeat the namespace and the selector.
			g.Expect(options.Namespace).To(Equal("team-b"), "page %d", pages)
			g.Expect(options.LabelSelector).NotTo(BeNil(), "page %d", pages)
			g.Expect(options.LabelSelector.String()).To(Equal(clusterv1.WatchLabel+"=b"), "page %d", pages)
			page := list.(*controlplanev1.IBMCloudManagedControlPlaneList)
			page.Items = []controlplanev1.IBMCloudManagedControlPlane{*filtered("team-b", fmt.Sprintf("cp-%d", pages), "b")}
			if pages == 1 {
				page.Continue = "next"
			} else {
				g.Expect(options.Continue).To(Equal("next"))
				page.Continue = ""
			}
			return nil
		}
		detector, recorder := newScopedGateOffFixture(t, GateOffDetectorOptions{WatchNamespace: "team-b", WatchFilterValue: "b"}, list)

		g.Expect(detector.Start(ctx)).To(Succeed())

		g.Expect(pages).To(Equal(2))
		g.Expect(drainEvents(recorder)).To(HaveLen(2))
	})

	t.Run("a scope no object can match is logged with the flag to fix and lists nothing", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			options GateOffDetectorOptions
			flag    string
		}{
			{name: "namespace", options: GateOffDetectorOptions{WatchNamespace: "Team_B"}, flag: `--namespace \"Team_B\" is not a valid namespace name`},
			{name: "watch-filter", options: GateOffDetectorOptions{WatchFilterValue: "team b"}, flag: `--watch-filter \"team b\" is not a valid label value`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				g := NewWithT(t)
				var warned []string
				var calls []client.ListOptions
				detector, recorder := newScopedGateOffFixture(t, tc.options, warnedControlPlanes(&warned, &calls), objects()...)
				// Start logs synchronously, so the lines need no lock.
				var logged []string
				logger := funcr.New(func(prefix, args string) { logged = append(logged, prefix+" "+args) }, funcr.Options{})

				g.Expect(detector.Start(ctrl.LoggerInto(ctx, logger))).To(Succeed(), "the detector only warns")

				g.Expect(calls).To(BeEmpty(), "no object can be in the scope")
				g.Expect(drainEvents(recorder)).To(BeEmpty())
				g.Expect(logged).To(ConsistOf(And(ContainSubstring("Cannot check for IBMCloudManagedControlPlane objects"),
					ContainSubstring(tc.flag), ContainSubstring("fix the flag"))))
			})
		}
	})
}
