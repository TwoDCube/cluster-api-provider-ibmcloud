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
	"fmt"
	"testing"

	. "github.com/onsi/gomega"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// TestIBMCloudManagedMachinePoolValidator covers the pool validator with the gate on: every operation is allowed (the
// gate-off create rejection is in TestGateOff).
func TestIBMCloudManagedMachinePoolValidator(t *testing.T) {
	g := NewWithT(t)
	validator := &IBMCloudManagedMachinePool{ManagedKubernetesEnabled: true}
	pool := newMachinePool(unitNamespace, "dev-pool", "dev")

	warnings, err := validator.ValidateCreate(context.Background(), pool)
	expectAllowed(g, warnings, err)
	warnings, err = validator.ValidateUpdate(context.Background(), pool, pool.DeepCopy())
	expectAllowed(g, warnings, err)
	warnings, err = validator.ValidateDelete(context.Background(), pool)
	expectAllowed(g, warnings, err)
}

// TestIBMCloudManagedMachinePoolWebhook checks through the API server that a pool is created with the gate on and that its
// deletion is never rejected, even while its Cluster lives and the control plane owns an IBM cluster.
func TestIBMCloudManagedMachinePoolWebhook(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmmp-webhook")

	t.Run("pool delete never rejected", func(t *testing.T) {
		g := NewWithT(t)
		clusterName := "pool-owner"
		createCluster(t, namespace, clusterName, false)
		cp := newControlPlane(namespace, clusterName)
		withEndpoint(cp)
		createControlPlane(g, cp, nil)
		pool := newMachinePool(namespace, clusterName+"-default", clusterName)

		g.Expect(testEnv.Create(ctx, pool)).To(Succeed())
		g.Expect(testEnv.Delete(ctx, pool)).To(Succeed())
	})
}

// TestIBMCloudManagedMachinePoolWebhookUpdate re-tests the field rules of IBMCloudManagedMachinePool through the API server with
// the webhook registered (the CEL tests of api/managed/v1alpha1 test CEL without webhooks). ValidateUpdate allows
// every update, so each rejection must come from CEL, and the allowed row proves the webhook does not block what CEL
// accepts.
func TestIBMCloudManagedMachinePoolWebhookUpdate(t *testing.T) {
	namespace := newTestNamespace(t, "ibmcmmp-webhook-update")

	testCases := []struct {
		name    string
		mutate  func(pool *infrav1.IBMCloudManagedMachinePool)
		wantErr string
	}{
		{
			name:   "label update allowed",
			mutate: func(pool *infrav1.IBMCloudManagedMachinePool) { pool.Labels["example.com/touched"] = "true" },
		},
		{
			name:    "CEL rejects a flavor change",
			mutate:  func(pool *infrav1.IBMCloudManagedMachinePool) { pool.Spec.Flavor = "bx2.8x32" },
			wantErr: "flavor is immutable; create a new pool",
		},
		{
			name:    "CEL rejects a poolName change",
			mutate:  func(pool *infrav1.IBMCloudManagedMachinePool) { pool.Spec.PoolName = "renamed" },
			wantErr: "poolName is immutable",
		},
		{
			name: "CEL rejects a zone change",
			mutate: func(pool *infrav1.IBMCloudManagedMachinePool) {
				pool.Spec.Zones = append(pool.Spec.Zones, infrav1.WorkerPoolZone{Name: "us-south-2"})
			},
			wantErr: "zones are immutable in v1alpha1",
		},
	}
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			pool := newMachinePool(namespace, fmt.Sprintf("update-%d", i), "pool-update")
			g.Expect(testEnv.Create(ctx, pool)).To(Succeed())

			tc.mutate(pool)
			err := testEnv.Update(ctx, pool)

			if tc.wantErr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			expectRejectedByCEL(g, err, tc.wantErr)
		})
	}
}
