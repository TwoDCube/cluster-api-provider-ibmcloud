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

package managedtools

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	. "github.com/onsi/gomega"
)

// managedE2EConfig is the part of test/e2e/config/ibmcloud-e2e-managed.yaml these tests read.
type managedE2EConfig struct {
	Intervals map[string][]string `json:"intervals"`
}

// loadManagedE2EConfig reads the managed e2e configuration that the managed lanes run with.
func loadManagedE2EConfig(t *testing.T) managedE2EConfig {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "config", "ibmcloud-e2e-managed.yaml"))
	if err != nil {
		t.Fatalf("reading the managed e2e config: %v", err)
	}
	var cfg managedE2EConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parsing the managed e2e config: %v", err)
	}
	return cfg
}

// timeout returns the timeout of the interval the e2e framework resolves for spec and key: "<spec>/<key>" when the
// configuration has it, otherwise "default/<key>" (the lookup of clusterctl.E2EConfig.GetIntervals).
func (c managedE2EConfig) timeout(t *testing.T, spec, key string) time.Duration {
	t.Helper()
	interval, ok := c.Intervals[spec+"/"+key]
	if !ok {
		interval, ok = c.Intervals["default/"+key]
	}
	if !ok || len(interval) == 0 {
		t.Fatalf("the managed e2e config has neither %s/%s nor default/%s", spec, key, key)
	}
	d, err := time.ParseDuration(interval[0])
	if err != nil {
		t.Fatalf("interval %s/%s has an invalid timeout %q: %v", spec, key, interval[0], err)
	}
	return d
}

// TestManagedClusterWaitCoversIBMProvisioning guards the interval that ApplyClusterTemplateAndWait uses to wait for the
// Cluster's Provisioned phase. A managed Cluster reaches that phase only after IBM has created the cluster, which takes
// about as long as the control plane wait allows, so falling back to the short default cluster wait fails every lane
// while IBM is still provisioning.
func TestManagedClusterWaitCoversIBMProvisioning(t *testing.T) {
	g := NewWithT(t)
	cfg := loadManagedE2EConfig(t)
	for _, spec := range []string{"managed-iks", "managed-roks"} {
		cluster := cfg.timeout(t, spec, "wait-cluster")
		controlPlane := cfg.timeout(t, spec, "wait-control-plane")
		g.Expect(cluster).To(BeNumerically(">=", controlPlane),
			"%s/wait-cluster (%s) must be at least %s/wait-control-plane (%s): the Cluster is Provisioned only once IBM has created the cluster",
			spec, cluster, spec, controlPlane)
	}
}
