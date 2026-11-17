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

	"k8s.io/apimachinery/pkg/util/version"
	"sigs.k8s.io/yaml"

	. "github.com/onsi/gomega"
)

// managedE2EConfig is the part of test/e2e/config/ibmcloud-e2e-managed.yaml these tests read.
type managedE2EConfig struct {
	Variables map[string]string   `json:"variables"`
	Intervals map[string][]string `json:"intervals"`
	Providers []struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Versions []struct {
			Name string `json:"name"`
		} `json:"versions"`
	} `json:"providers"`
}

// minimumClusterAPIVersion is the oldest Cluster API release the managed controllers support: v1.14.0 ignores the
// ClusterCache's cluster filter that keeps CAPIBM from connecting to clusters it does not manage (see
// internal/controllers/managed/clustercache.go and the book's versions page).
const minimumClusterAPIVersion = "v1.14.1"

// TestManagedE2EInstallsASupportedClusterAPI guards the Cluster API providers the managed lanes install: every
// cluster-api and kubeadm version must be a supported release, and all of them the same release, because clusterctl
// move refuses a target whose providers are older than the source's, so a mixed set breaks the move spec, and an
// unsupported core provider would test a combination the book tells users not to run.
func TestManagedE2EInstallsASupportedClusterAPI(t *testing.T) {
	g := NewWithT(t)
	cfg := loadManagedE2EConfig(t)
	minimum := version.MustParseSemantic(minimumClusterAPIVersion)
	seen := map[string]bool{}
	for _, p := range cfg.Providers {
		if p.Name != "cluster-api" && p.Name != "kubeadm" {
			continue
		}
		g.Expect(p.Versions).NotTo(BeEmpty(), "provider %s (%s) lists no version", p.Name, p.Type)
		for _, v := range p.Versions {
			parsed, err := version.ParseSemantic(v.Name)
			g.Expect(err).NotTo(HaveOccurred(), "provider %s (%s) version %q", p.Name, p.Type, v.Name)
			g.Expect(parsed.AtLeast(minimum)).To(BeTrue(),
				"provider %s (%s) is %s; the managed controllers need Cluster API %s or newer", p.Name, p.Type, v.Name,
				minimumClusterAPIVersion)
			seen[v.Name] = true
		}
	}
	g.Expect(seen).To(HaveLen(1), "the cluster-api and kubeadm providers must use one release, got %v", seen)
}

// TestManagedE2EDeletesPersistentStorage guards the variable that the managed flavor templates read into the control
// plane's spec.deletion.persistentStorage: the lanes must delete their clusters with Delete, so that their DELETEs ask
// IBM to delete the persistent storage too. The configuration sets a fixed value, not a ${...:-default} form, because
// no run has a reason to keep the volumes; an environment variable of that name still wins in the lanes, which then
// stop before they create anything (ValidateLanePersistentStorage in validateVariables).
func TestManagedE2EDeletesPersistentStorage(t *testing.T) {
	g := NewWithT(t)
	cfg := loadManagedE2EConfig(t)
	value, ok := cfg.Variables[PersistentStorageVariable]
	g.Expect(ok).To(BeTrue(), "the managed e2e config must set %s", PersistentStorageVariable)
	g.Expect(ValidateLanePersistentStorage(value)).To(Succeed())
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

// TestIKSWorkerRolloutWaitCoversEveryReplacement guards the interval that the IKS lane's worker rollouts wait with. The
// longest of them is the worker follow of the master upgrade: it runs while the lane has both of its pools, so that
// every zone keeps a second worker during each drain, and it replaces all their workers (the initial pool scaled down
// to 1 per zone plus the added pool's 1 per zone, scaledDownReplicas + extraPoolReplicas in managed_test.go), one at
// a time per Cluster. A replacement takes about 8 to 10 minutes (the arithmetic is in the e2e config), so each gets
// twice the upper estimate; a shorter wait would fail a rollout that is merely slow.
func TestIKSWorkerRolloutWaitCoversEveryReplacement(t *testing.T) {
	const (
		// upgradeReplacements is the number of workers the IKS lane's upgrade replaces: 3 zones x 2 pools.
		upgradeReplacements = 6
		// replacementBudget is the share of the wait each replacement gets: twice the upper estimate of 10 minutes.
		replacementBudget = 20 * time.Minute
	)
	g := NewWithT(t)
	cfg := loadManagedE2EConfig(t)
	rollout := cfg.timeout(t, "managed-iks", "wait-worker-rollout")
	g.Expect(rollout).To(BeNumerically(">=", upgradeReplacements*replacementBudget),
		"managed-iks/wait-worker-rollout (%s) must cover %d worker replacements of %s each: the upgrade replaces every "+
			"worker of both pools, one at a time", rollout, upgradeReplacements, replacementBudget)
}

// TestRegistryClusterGoneWaitCoversROKSDeletion guards the interval with which the ROKS lane's registry bucket cleanup
// waits for IBM to answer 404 for the run's clusters. In the case the IBM backstop exists for, CAPIBM never deleted
// the cluster and the backstop sends the DELETE just before that wait, so the wait must cover a whole ROKS deletion
// (measured at 18 minutes) with room for a slower IBM; a shorter wait fails the cleanup before it deletes the run's
// registry bucket, and the next run's baseline then hides that bucket. It is its own interval, not the delete step's
// wait-ibm-404, so that tuning one never changes the other.
func TestRegistryClusterGoneWaitCoversROKSDeletion(t *testing.T) {
	const (
		// measuredROKSDeletion is how long IBM took to delete a ROKS cluster when measured.
		measuredROKSDeletion = 18 * time.Minute
		// minimumWait is half again the measured deletion.
		minimumWait = measuredROKSDeletion + measuredROKSDeletion/2
	)
	g := NewWithT(t)
	cfg := loadManagedE2EConfig(t)
	wait := cfg.timeout(t, "managed-roks", "wait-registry-cluster-gone")
	g.Expect(wait).To(BeNumerically(">=", minimumWait),
		"managed-roks/wait-registry-cluster-gone (%s) must be at least %s: the backstop's DELETE may start just before "+
			"it, and a ROKS deletion took %s when measured", wait, minimumWait, measuredROKSDeletion)
}
