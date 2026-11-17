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
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/IBM/go-sdk-core/v5/core"

	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

func TestClientOptionsValidate(t *testing.T) {
	auth, err := core.NewBearerTokenAuthenticator(testToken)
	if err != nil {
		t.Fatal(err)
	}
	valid := ClientOptions{URL: "https://containers.cloud.ibm.com/global", Authenticator: auth, HTTPClient: &http.Client{}}
	testCases := []struct {
		name    string
		mutate  func(*ClientOptions)
		wantErr []string
	}{
		{name: "valid", mutate: func(*ClientOptions) {}},
		// The URL goes to the SDK as it is: no scheme or form rule of this package, and empty selects the SDK default.
		{name: "a plain-http URL is the caller's choice", mutate: func(o *ClientOptions) { o.URL = "http://127.0.0.1:8080/global" }},
		{name: "an empty URL is left to the SDK", mutate: func(o *ClientOptions) { o.URL = "" }},
		{name: "nil authenticator and nil client are both reported", mutate: func(o *ClientOptions) {
			o.Authenticator = nil
			o.HTTPClient = nil
		}, wantErr: []string{"the authenticator is nil", "the HTTP client is nil"}},
		{name: "typed nil authenticator", mutate: func(o *ClientOptions) { o.Authenticator = (*core.BearerTokenAuthenticator)(nil) },
			wantErr: []string{"the authenticator is nil"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			opts := valid
			tc.mutate(&opts)
			err := opts.validate("IBM Cloud Kubernetes Service")
			if len(tc.wantErr) == 0 {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			for _, want := range tc.wantErr {
				g.Expect(err).To(MatchError(ContainSubstring(want)))
			}
		})
	}
}

func TestNewServicesRejectInvalidOptions(t *testing.T) {
	g := NewWithT(t)
	_, err := NewClusterService(ClientOptions{})
	g.Expect(err).To(MatchError(ContainSubstring("IBM Cloud Kubernetes Service client: the authenticator is nil")))
	_, err = NewVPCService(ClientOptions{})
	g.Expect(err).To(MatchError(ContainSubstring("VPC client: the authenticator is nil")))
}

func TestClusterServiceListClusters(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.clusters = []map[string]any{
		ibmCluster("c0aaaaaaaaaaaaaaaaaa", "capibm-e2e-abc123-iks", "2026-11-16T03:05:33+0000", "normal"),
		ibmCluster("c0bbbbbbbbbbbbbbbbbb", "someone-else", "2026-11-15T03:05:33+0000", "deploying"),
	}

	clusters, err := fake.clusterService("rg-e2e").ListClusters(context.Background())

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(clusters).To(Equal([]Cluster{
		{ID: "c0aaaaaaaaaaaaaaaaaa", Name: "capibm-e2e-abc123-iks", CreatedDate: "2026-11-16T03:05:33+0000", State: "normal", ResourceGroupID: "rg-e2e"},
		{ID: "c0bbbbbbbbbbbbbbbbbb", Name: "someone-else", CreatedDate: "2026-11-15T03:05:33+0000", State: "deploying", ResourceGroupID: "rg-e2e"},
	}))
	requests := fake.recorded()
	g.Expect(requests).To(HaveLen(1))
	g.Expect(requests[0].Method).To(Equal(http.MethodGet))
	g.Expect(requests[0].Path).To(Equal("/global/v2/vpc/getClusters"))
	g.Expect(requests[0].Query).To(Equal("provider=vpc-gen2"))
	g.Expect(requests[0].Header.Get("Authorization")).To(Equal("Bearer " + testToken))
	g.Expect(requests[0].Header.Get("X-Auth-Resource-Group")).To(Equal("rg-e2e"))
	g.Expect(requests[0].Header.Get("User-Agent")).To(Equal(DefaultUserAgent))
}

func TestClusterServiceListClustersWithoutResourceGroup(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)

	clusters, err := fake.clusterService("").ListClusters(context.Background())

	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(clusters).To(BeEmpty())
	g.Expect(fake.recorded()[0].Header.Values("X-Auth-Resource-Group")).To(BeEmpty())
}

func TestClusterServiceListClustersKeepsOnlyTheResourceGroupWhateverIBMAnswers(t *testing.T) {
	created := "2026-11-16T03:05:33+0000"
	fixture := func() []map[string]any {
		// The fake server ignores X-Auth-Resource-Group, as IBM might: the answer holds clusters of other groups.
		return []map[string]any{
			ibmClusterInGroup("c0ownaaaaaaaaaaaaaaa", "capibm-e2e-abc123-iks", created, "normal", "rg-e2e"),
			ibmClusterInGroup("c0otheraaaaaaaaaaaaa", "capibm-e2e-def456-iks", created, "normal", "rg-other"),
			ibmClusterInGroup("c0nogroupaaaaaaaaaaa", "capibm-e2e-ghi789-iks", created, "normal", ""),
		}
	}
	testCases := []struct {
		name            string
		resourceGroupID string
		wantIDs         []string
	}{
		{name: "scoped: only the resource group, never a cluster without one", resourceGroupID: "rg-e2e",
			wantIDs: []string{"c0ownaaaaaaaaaaaaaaa"}},
		{name: "unscoped: every cluster the credentials see", resourceGroupID: "",
			wantIDs: []string{"c0ownaaaaaaaaaaaaaaa", "c0otheraaaaaaaaaaaaa", "c0nogroupaaaaaaaaaaa"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)
			fake.clusters = fixture()

			clusters, err := fake.clusterService(tc.resourceGroupID).ListClusters(context.Background())

			g.Expect(err).NotTo(HaveOccurred())
			ids := make([]string, 0, len(clusters))
			for _, cluster := range clusters {
				ids = append(ids, cluster.ID)
			}
			g.Expect(ids).To(Equal(tc.wantIDs))
		})
	}
}

func TestClusterServiceListClustersError(t *testing.T) {
	g := NewWithT(t)
	fake := newFakeIBM(t)
	fake.listStatus = http.StatusForbidden

	_, err := fake.clusterService("").ListClusters(context.Background())

	var apiErr *APIError
	g.Expect(errors.As(err, &apiErr)).To(BeTrue())
	g.Expect(apiErr.StatusCode).To(Equal(http.StatusForbidden))
	g.Expect(err).To(MatchError(ContainSubstring("ListClusters: IBM Cloud answered HTTP 403")))
	g.Expect(err.Error()).NotTo(ContainSubstring(testToken))
}

func TestClusterServiceDeleteCluster(t *testing.T) {
	testCases := []struct {
		name      string
		storage   kubernetesservice.PersistentStorage
		wantQuery string
	}{
		// The janitor asks IBM to keep the persistent storage of stale clusters; the inventory diff reports what IBM leaves.
		{name: "Retain asks IBM to keep the storage", storage: kubernetesservice.RetainPersistentStorage, wantQuery: "deleteResources=false"},
		// The backstop asks IBM to delete the storage of the run's own clusters, so that a run leaves no volumes behind.
		{name: "Delete asks IBM to delete the storage", storage: kubernetesservice.DeletePersistentStorage, wantQuery: "deleteResources=true"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)

			err := fake.clusterService("rg-e2e").DeleteCluster(context.Background(), "c0aaaaaaaaaaaaaaaaaa", tc.storage)

			g.Expect(err).NotTo(HaveOccurred())
			requests := fake.recorded()
			g.Expect(requests).To(HaveLen(1))
			g.Expect(requests[0].Method).To(Equal(http.MethodDelete))
			g.Expect(requests[0].Path).To(Equal("/global/v1/clusters/c0aaaaaaaaaaaaaaaaaa"))
			g.Expect(requests[0].Query).To(Equal(tc.wantQuery))
			g.Expect(requests[0].Header.Get("Authorization")).To(Equal("Bearer " + testToken))
			g.Expect(requests[0].Header.Get("X-Auth-Resource-Group")).To(Equal("rg-e2e"))
		})
	}
}

func TestClusterServiceDeleteClusterRefusesAnInvalidStorageChoiceWithoutARequest(t *testing.T) {
	for _, storage := range []kubernetesservice.PersistentStorage{"", "delete", "true"} {
		t.Run(fmt.Sprintf("%q", storage), func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)

			err := fake.clusterService("").DeleteCluster(context.Background(), "c0aaaaaaaaaaaaaaaaaa", storage)

			g.Expect(err).To(MatchError(ContainSubstring(fmt.Sprintf(`the persistent storage choice %q is neither "Retain" `+
				`nor "Delete"`, storage))))
			g.Expect(fake.recorded()).To(BeEmpty())
		})
	}
}

func TestClusterServiceDeleteClusterRefusesNamesWithoutARequest(t *testing.T) {
	for _, value := range []string{"capibm-e2e-abc123-iks", "capibm-e2e-periodics-x7k-roks", "", "C0AAAAAAAAAAAAAAAAAA", "c0aaaa/../x", "my.cluster"} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)

			err := fake.clusterService("").DeleteCluster(context.Background(), value, kubernetesservice.RetainPersistentStorage)

			g.Expect(err).To(MatchError(ContainSubstring("is not an IBM cluster ID")))
			g.Expect(err).To(MatchError(ContainSubstring("never by name")))
			g.Expect(fake.recorded()).To(BeEmpty())
		})
	}
}

func TestClusterServiceDeleteClusterErrors(t *testing.T) {
	testCases := []struct {
		name         string
		status       int
		wantNotFound bool
	}{
		{name: "404 is NotFound", status: http.StatusNotFound, wantNotFound: true},
		{name: "409 is not NotFound", status: http.StatusConflict},
		{name: "500 is not NotFound", status: http.StatusInternalServerError},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIBM(t)
			fake.deleteStatus["c0aaaaaaaaaaaaaaaaaa"] = tc.status

			err := fake.clusterService("").DeleteCluster(context.Background(), "c0aaaaaaaaaaaaaaaaaa", kubernetesservice.RetainPersistentStorage)

			g.Expect(err).To(MatchError(ContainSubstring(fmt.Sprintf("DeleteCluster: IBM Cloud answered HTTP %d", tc.status))))
			g.Expect(IsNotFound(err)).To(Equal(tc.wantNotFound))
		})
	}
}

func TestAPIErrorWithoutAnswer(t *testing.T) {
	g := NewWithT(t)
	err := newAPIError("ListClusters", nil, errors.New("context deadline exceeded"))
	g.Expect(err.Error()).To(Equal("ListClusters: no answer from IBM Cloud: context deadline exceeded"))
	g.Expect(IsNotFound(err)).To(BeFalse())
	g.Expect(IsNotFound(fmt.Errorf("wrapped: %w", &APIError{StatusCode: http.StatusNotFound}))).To(BeTrue())
	g.Expect(IsNotFound(nil)).To(BeFalse())
}

func TestValidateClusterID(t *testing.T) {
	g := NewWithT(t)
	g.Expect(ValidateClusterID("c0aaaaaaaaaaaaaaaaaa")).To(Succeed())
	// Every name of the contract contains hyphens, so ClusterName never yields a valid ID.
	name, err := ClusterName(DefaultNamePrefix, "abc123", LaneIKS)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ValidateClusterID(name)).NotTo(Succeed())
}

func TestValidateLanePersistentStorage(t *testing.T) {
	g := NewWithT(t)
	g.Expect(ValidateLanePersistentStorage("Delete")).To(Succeed())
	// The templates' default, an empty value (the variable unset in the configuration) and a typo all ask IBM to keep
	// the volumes or fail the template, so each stops the lane before it creates anything.
	for _, value := range []string{"Retain", "", "delete"} {
		err := ValidateLanePersistentStorage(value)
		g.Expect(err).To(MatchError(ContainSubstring(fmt.Sprintf(`DELETION_PERSISTENT_STORAGE is %q; the managed lanes need "Delete"`, value))))
		g.Expect(err).To(MatchError(ContainSubstring("test/e2e/config/ibmcloud-e2e-managed.yaml sets it")))
	}
}
