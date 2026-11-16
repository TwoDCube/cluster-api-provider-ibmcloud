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

package drain

import (
	"fmt"
	"os"
	"testing"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// The envtest API server plays the workload cluster, as in Cluster API's drain tests: it runs the real Eviction
// subresource with its PodDisruptionBudget checks, but no kubelet, no scheduler and no disruption controller. The
// tests therefore bind pods with spec.nodeName, write PDB and VolumeAttachment status themselves, and delete evicted
// pods to play the kubelet. No CAPIBM CRD is needed: the package works on core Kubernetes objects only.

// envClient talks to the envtest API server; set by TestMain.
var envClient client.Client

func TestMain(m *testing.M) {
	ctrl.SetLogger(klog.Background())

	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		panic(fmt.Sprintf("failed to start envtest (KUBEBUILDER_ASSETS must point at the envtest binaries, "+
			"see hack/tools/bin/setup-envtest): %v", err))
	}
	envClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		_ = env.Stop()
		panic(fmt.Sprintf("failed to create the envtest client: %v", err))
	}

	code := m.Run()

	if err := env.Stop(); err != nil {
		panic(fmt.Sprintf("failed to stop envtest: %v", err))
	}
	os.Exit(code)
}
