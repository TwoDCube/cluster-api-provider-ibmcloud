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

// The CEL tests of this package run against a real API server (envtest) with the generated CRDs, because CEL and
// OpenAPI validation exist only there. They live next to the types, in the external test package, so that they test
// the schema on its own, without the admission webhooks.
package v1alpha1_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/test/helpers"
)

var (
	testEnv *helpers.TestEnvironment
	ctx     = ctrl.SetupSignalHandler()
)

func TestMain(m *testing.M) {
	setup()
	result := m.Run()
	teardown()
	os.Exit(result)
}

// setup starts envtest with every CRD of config/crd/bases, so the CRDs under test are exactly the generated ones.
func setup() {
	utilruntime.Must(controlplanev1.AddToScheme(scheme.Scheme))
	testEnvConfig := helpers.NewTestEnvironmentConfiguration([]string{
		path.Join("config", "crd", "bases"),
	})
	var err error
	testEnv, err = testEnvConfig.Build()
	if err != nil {
		panic(err)
	}
	// The manager is started only because TestEnvironment.Stop cancels its context; no controller or webhook runs.
	go func() {
		if err := testEnv.StartManager(ctx); err != nil {
			panic(fmt.Sprintf("Failed to start the envtest manager: %v", err))
		}
	}()
	// StartManager stores the cancel function that Stop calls, in the goroutine above. The manager closes Elected()
	// once it runs (at once, without leader election), so waiting for it orders that store before teardown's Stop;
	// without the wait, go test -race reports the store and Stop's read as a data race.
	<-testEnv.Elected()
}

func teardown() {
	if err := testEnv.Stop(); err != nil {
		panic(fmt.Sprintf("Failed to stop envtest: %v", err))
	}
}

// newTestNamespace creates a namespace for one test and deletes it when the test ends.
func newTestNamespace(t *testing.T, generateName string) string {
	t.Helper()
	g := NewWithT(t)
	ns, err := testEnv.CreateNamespace(ctx, generateName)
	g.Expect(err).NotTo(HaveOccurred())
	t.Cleanup(func() {
		g.Expect(testEnv.Cleanup(ctx, &corev1.Namespace{ObjectMeta: ns.ObjectMeta})).To(Succeed())
	})
	return ns.Name
}

// expectAPIResult checks the outcome of an API server call: success when wantErr is empty, otherwise an error whose
// text contains wantErr (the CEL rule message or the OpenAPI violation).
func expectAPIResult(g *WithT, err error, wantErr string) {
	if wantErr == "" {
		g.Expect(err).NotTo(HaveOccurred())
		return
	}
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring(wantErr))
}
