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

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"

	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/test/helpers"
	managedwebhooks "sigs.k8s.io/cluster-api-provider-ibmcloud/webhooks/managed/admission"
)

// The envtest suite of the managed controllers runs one API server with every CRD of config/crd/bases, the fake
// Cluster API CRDs of test/helpers (Cluster, Machine, MachinePool) and the managed admission webhooks, registered with
// the feature gate on as cmd/main.go does, so that objects pass the same CEL rules and validators as in production.
//
// Tests call Reconcile directly, so that each step and its IBM calls are deterministic; the shared manager runs no
// managed controller. A test that needs watches (a controller's own SetupWithManager) builds a
// short-lived manager from testEnv.Config with newTestManager (helpers_test.go). Later work packages add test files
// only.

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

// setup starts envtest and the shared manager, which serves the managed webhooks.
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
	if err := (&managedwebhooks.IBMCloudManagedCluster{ManagedKubernetesEnabled: true, Reader: reader}).SetupWebhookWithManager(testEnv); err != nil {
		panic(fmt.Sprintf("Unable to setup IBMCloudManagedCluster webhook: %v", err))
	}
	if err := (&managedwebhooks.IBMCloudManagedControlPlane{ManagedKubernetesEnabled: true, Reader: reader}).SetupWebhookWithManager(testEnv); err != nil {
		panic(fmt.Sprintf("Unable to setup IBMCloudManagedControlPlane webhook: %v", err))
	}
	if err := (&managedwebhooks.IBMCloudManagedMachinePool{ManagedKubernetesEnabled: true}).SetupWebhookWithManager(testEnv); err != nil {
		panic(fmt.Sprintf("Unable to setup IBMCloudManagedMachinePool webhook: %v", err))
	}

	go func() {
		if err := testEnv.StartManager(ctx); err != nil {
			panic(fmt.Sprintf("Failed to start the envtest manager: %v", err))
		}
	}()
	testEnv.WaitForWebhooks()
}

// teardown stops envtest.
func teardown() {
	if err := testEnv.Stop(); err != nil {
		panic(fmt.Sprintf("Failed to stop envtest: %v", err))
	}
}
