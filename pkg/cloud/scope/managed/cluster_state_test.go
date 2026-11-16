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
	"encoding/json"
	"strings"
	"testing"

	. "github.com/onsi/gomega"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

func TestClassifyClusterState(t *testing.T) {
	testCases := []struct {
		state string
		want  ClusterStateClass
	}{
		// Every cluster state IBM documents.
		{state: "requested", want: ClusterStateProvisioning},
		{state: "deploying", want: ClusterStateProvisioning},
		{state: "deployed", want: ClusterStateExists},
		{state: "pending", want: ClusterStateExists},
		{state: "normal", want: ClusterStateExists},
		{state: "warning", want: ClusterStateExists},
		{state: "critical", want: ClusterStateExists},
		{state: "error", want: ClusterStateExists},
		{state: "unavailable", want: ClusterStateExists},
		{state: "scaled_down", want: ClusterStateExists},
		{state: "updating", want: ClusterStateExists},
		{state: "update_failed", want: ClusterStateExists},
		{state: "update_canceled", want: ClusterStateExists},
		{state: "unsupported", want: ClusterStateExists},
		{state: "create_failed", want: ClusterStateFailed},
		{state: "deploy_failed", want: ClusterStateFailed},
		{state: "deleting", want: ClusterStateGoneOutOfBand},
		{state: "deleted", want: ClusterStateGoneOutOfBand},
		{state: "aborted", want: ClusterStateGoneOutOfBand},
		{state: "delete_failed", want: ClusterStateGoneOutOfBand},
	}
	for _, tc := range testCases {
		t.Run(tc.state, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(ClassifyClusterState(tc.state)).To(Equal(tc.want))
			g.Expect(KnownClusterState(tc.state)).To(BeTrue())
		})
	}

	t.Run("unknown tokens exist and are reported as unknown", func(t *testing.T) {
		g := NewWithT(t)
		for _, state := range []string{"hibernating", "", "Normal", "normal "} {
			g.Expect(ClassifyClusterState(state)).To(Equal(ClusterStateExists), state)
			g.Expect(KnownClusterState(state)).To(BeFalse(), state)
		}
	})

	t.Run("create_sequence_of_the_live_captures", func(t *testing.T) {
		// The create sequence deploying -> deployed -> pending -> normal observed on IBM Cloud, on both flavors.
		for _, flavor := range []string{"iks", "roks"} {
			steps := []string{"deploying", "deployed", "pending", "normal"}
			classes := make([]ClusterStateClass, 0, len(steps))
			for _, step := range steps {
				c := loadCluster(t, flavor+"-getCluster-"+step+".json")
				NewWithT(t).Expect(c.State).To(Equal(step))
				classes = append(classes, ClassifyClusterState(c.State))
			}
			NewWithT(t).Expect(classes).To(Equal([]ClusterStateClass{ClusterStateProvisioning, ClusterStateExists,
				ClusterStateExists, ClusterStateExists}), flavor)
		}
	})
}

func TestObserveCluster(t *testing.T) {
	iks := loadCluster(t, "iks-getCluster-normal.json")
	roksDeployed := loadCluster(t, "roks-getCluster-deployed.json")
	with := func(c kubernetesservice.Cluster, mutate func(*kubernetesservice.Cluster)) *kubernetesservice.Cluster {
		mutate(&c)
		return &c
	}
	testCases := []struct {
		name    string
		cluster *kubernetesservice.Cluster
		want    ObservedCluster
		wantErr []string
	}{
		{name: "IKS live capture", cluster: &iks, want: ObservedCluster{
			State: "normal", MasterState: "deployed", MasterHealth: "normal", DistributionVersion: "1.36.5_1537",
			TargetDistributionVersion: "1.36.5_1537",
			ClusterCRN:                "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:iksclusterid00000001::"}},
		{name: "ROKS deployed capture keeps the empty master health out", cluster: &roksDeployed, want: ObservedCluster{
			State: "deployed", MasterState: "deployed", DistributionVersion: "4.21.35_1537_openshift",
			TargetDistributionVersion: "4.21.35_1537_openshift",
			ClusterCRN:                "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:roksclusterid0000002::"}},
		{name: "pending_marker_never_reaches_status", cluster: with(iks, func(c *kubernetesservice.Cluster) {
			c.State, c.MasterKubeVersion, c.TargetVersion, c.VersionEOS = "updating", "1.37.1_1517 (pending)", "1.37.1_1517 (pending)", "2027-01-20T23:59:59+0000"
		}), want: ObservedCluster{
			State: "updating", MasterState: "deployed", MasterHealth: "normal", DistributionVersion: "1.37.1_1517",
			TargetDistributionVersion: "1.37.1_1517", DistributionVersionEndOfSupport: "2027-01-20T23:59:59+0000",
			ClusterCRN: iks.CRN}},
		{name: "requested cluster without versions", cluster: with(iks, func(c *kubernetesservice.Cluster) {
			c.State, c.MasterState, c.MasterHealth, c.MasterKubeVersion, c.TargetVersion = "requested", "", "", "", ""
		}), want: ObservedCluster{State: "requested", ClusterCRN: iks.CRN}},
		{name: "unparseable versions are left out and named", cluster: with(iks, func(c *kubernetesservice.Cluster) {
			c.MasterKubeVersion, c.TargetVersion = "1.37.1_1517(pending)", "v1.37"
		}), want: ObservedCluster{State: "normal", MasterState: "deployed", MasterHealth: "normal", ClusterCRN: iks.CRN},
			wantErr: []string{`masterKubeVersion "1.37.1_1517(pending)"`, `targetVersion "v1.37"`, "not recorded"}},
		{name: "values longer than their status fields are left out", cluster: with(iks, func(c *kubernetesservice.Cluster) {
			c.State, c.VersionEOS, c.CRN = strings.Repeat("s", 33), strings.Repeat("e", 33), strings.Repeat("c", 513)
		}), want: ObservedCluster{MasterState: "deployed", MasterHealth: "normal", DistributionVersion: "1.36.5_1537",
			TargetDistributionVersion: "1.36.5_1537"},
			wantErr: []string{"IBM reports state", "IBM reports versionEOS", "IBM reports crn", "longer than the 32 characters"}},
		{name: "nil cluster", cluster: nil, wantErr: []string{"got nil"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ObserveCluster(tc.cluster)
			if len(tc.wantErr) > 0 {
				g.Expect(err).To(HaveOccurred())
				for _, part := range tc.wantErr {
					g.Expect(err.Error()).To(ContainSubstring(part))
				}
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}
			g.Expect(got).To(Equal(tc.want))
			// No field, and so no status string, ever carries IBM's pending marker.
			data, jsonErr := json.Marshal(got)
			g.Expect(jsonErr).NotTo(HaveOccurred())
			g.Expect(string(data)).NotTo(ContainSubstring("pending)"))
		})
	}
}

func TestObservedClusterApplyTo(t *testing.T) {
	previous := controlplanev1.IBMCloudManagedControlPlaneStatus{
		State:                           "normal",
		Master:                          controlplanev1.MasterStatus{State: "deployed", Health: "normal"},
		DistributionVersion:             "1.36.5_1537",
		TargetDistributionVersion:       "1.36.5_1537",
		DistributionVersionEndOfSupport: "2027-01-20T23:59:59+0000",
		ClusterCRN:                      "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:iksclusterid00000001::",
		ClusterID:                       "iksclusterid00000001",
	}

	t.Run("status_follows_ibm_without_the_pending_marker", func(t *testing.T) {
		g := NewWithT(t)
		c := loadCluster(t, "iks-getCluster-normal.json")
		c.State, c.MasterKubeVersion, c.TargetVersion = "updating", "1.37.1_1517 (pending)", "1.37.1_1517"
		observed, err := ObserveCluster(&c)
		g.Expect(err).NotTo(HaveOccurred())
		status := previous
		observed.ApplyTo(&status)
		g.Expect(status.State).To(Equal("updating"))
		g.Expect(status.DistributionVersion).To(Equal("1.37.1_1517"))
		g.Expect(status.TargetDistributionVersion).To(Equal("1.37.1_1517"))
		g.Expect(status.ClusterID).To(Equal("iksclusterid00000001"), "fields ObserveCluster does not own are kept")
		// IBM no longer reports an end of support: the field is omitted, never written as "".
		g.Expect(status.DistributionVersionEndOfSupport).To(BeEmpty())
		data, err := json.Marshal(status)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(string(data)).NotTo(ContainSubstring("pending)"))
		g.Expect(string(data)).NotTo(ContainSubstring(`"distributionVersionEndOfSupport"`))
	})

	t.Run("a failed read keeps the versions and the CRN", func(t *testing.T) {
		g := NewWithT(t)
		status := previous
		ObservedCluster{State: "normal", MasterState: "deployed"}.ApplyTo(&status)
		g.Expect(status.DistributionVersion).To(Equal(previous.DistributionVersion))
		g.Expect(status.TargetDistributionVersion).To(Equal(previous.TargetDistributionVersion))
		g.Expect(status.ClusterCRN).To(Equal(previous.ClusterCRN))
		// The master health IBM leaves empty after Ready is omitted, never written as "".
		g.Expect(status.Master).To(Equal(controlplanev1.MasterStatus{State: "deployed"}))
		data, err := json.Marshal(status.Master)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(string(data)).To(Equal(`{"state":"deployed"}`))
	})

	t.Run("nil status is ignored", func(*testing.T) {
		ObservedCluster{State: "normal"}.ApplyTo(nil)
	})
}
