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
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/test/helpers"
)

// The envtest suite of this package runs the three validators behind a real API server, with the generated CRDs and
// the generated webhook configuration (config/webhook/manifests.yaml), so the markers, the CEL rules and the
// validators are tested together. The update tests of each kind carry rows that only a CEL rule rejects
// (expectRejectedByCEL), which re-test CEL with the webhooks registered; the CEL rules alone are covered next to the
// types (api/managed/...). The validators run with the feature gate on; the gate-off behaviour is covered by unit
// tests on the typed validators, because one API server can register only one validator per webhook path.

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

// setup starts envtest with every CRD of config/crd/bases (the fake Cluster API CRDs come with the helper) and
// registers the managed validators with the gate on and the manager's API reader, as cmd/main.go does.
func setup() {
	utilruntime.Must(infrav1.AddToScheme(scheme.Scheme))
	utilruntime.Must(controlplanev1.AddToScheme(scheme.Scheme))
	testEnvConfig := helpers.NewTestEnvironmentConfiguration([]string{
		path.Join("config", "crd", "bases"),
	}).WithWebhookConfiguration("managed", path.Join("config", "webhook", "manifests.yaml"))
	var err error
	testEnv, err = testEnvConfig.Build()
	if err != nil {
		panic(err)
	}
	reader := testEnv.GetAPIReader()
	if err := (&IBMCloudManagedCluster{ManagedKubernetesEnabled: true, Reader: reader}).SetupWebhookWithManager(testEnv); err != nil {
		panic(fmt.Sprintf("Unable to setup IBMCloudManagedCluster webhook: %v", err))
	}
	if err := (&IBMCloudManagedControlPlane{ManagedKubernetesEnabled: true, Reader: reader}).SetupWebhookWithManager(testEnv); err != nil {
		panic(fmt.Sprintf("Unable to setup IBMCloudManagedControlPlane webhook: %v", err))
	}
	if err := (&IBMCloudManagedMachinePool{ManagedKubernetesEnabled: true}).SetupWebhookWithManager(testEnv); err != nil {
		panic(fmt.Sprintf("Unable to setup IBMCloudManagedMachinePool webhook: %v", err))
	}

	go func() {
		if err := testEnv.StartManager(ctx); err != nil {
			panic(fmt.Sprintf("Failed to start the envtest manager: %v", err))
		}
	}()
	testEnv.WaitForWebhooks()
}

func teardown() {
	if err := testEnv.Stop(); err != nil {
		panic(fmt.Sprintf("Failed to stop envtest: %v", err))
	}
}

// newTestNamespace creates a namespace for one test and deletes it when the test ends. envtest runs no namespace
// controller, so the objects inside stay until the API server stops; every test uses its own namespace and names.
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
