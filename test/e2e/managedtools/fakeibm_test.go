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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/IBM/go-sdk-core/v5/core"
)

// testToken is the bearer token of the tests' authenticator; every request must carry it.
const testToken = "e2e-test-token" //nolint:gosec // G101: a fixed test value, not a credential.

// vpcCollections are the VPC collections the inventory lists, by URL path segment.
var vpcCollections = []string{"load_balancers", "endpoint_gateways", "security_groups", "volumes"}

// recordedRequest is one request the fake IBM server received.
type recordedRequest struct {
	Method string
	Path   string
	Query  string
	Header http.Header
}

// fakeIBM is an httptest TLS server that answers the IBM Cloud Kubernetes Service and VPC calls of this package from
// fixed data and records every request, so that tests assert on the exact requests (method, path, query, headers).
type fakeIBM struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	requests []recordedRequest

	// clusters is the answer of GET /v2/vpc/getClusters.
	clusters []map[string]any
	// listStatus, when not 0, is the status of every cluster list answer.
	listStatus int
	// deleteStatus maps a cluster ID to the status of its DELETE; missing IDs answer 204.
	deleteStatus map[string]int
	// vpc maps a VPC collection to its items.
	vpc map[string][]map[string]any
	// vpcStatus, when not 0, is the status of every VPC answer.
	vpcStatus int
	// pageSize is the number of VPC items per page; 0 means all on one page.
	pageSize int
}

// newFakeIBM starts a fake IBM server and stops it when the test ends.
//
// Parameters:
//   - t is the test.
func newFakeIBM(t *testing.T) *fakeIBM {
	t.Helper()
	f := &fakeIBM{t: t, deleteStatus: map[string]int{}, vpc: map[string][]map[string]any{}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

// options returns client options for the fake server: the base path of the service, a bearer-token authenticator and
// the server's TLS-trusting client.
//
// Parameters:
//   - basePath is "/global" for IBM Cloud Kubernetes Service or "/v1" for VPC.
//   - resourceGroupID is the optional resource group scope.
func (f *fakeIBM) options(basePath, resourceGroupID string) ClientOptions {
	auth, err := core.NewBearerTokenAuthenticator(testToken)
	if err != nil {
		f.t.Fatalf("building the test authenticator: %v", err)
	}
	return ClientOptions{URL: f.server.URL + basePath, Authenticator: auth, HTTPClient: f.server.Client(),
		ResourceGroupID: resourceGroupID}
}

// clusterService returns a ClusterService for the fake server.
//
// Parameters:
//   - resourceGroupID is the optional resource group scope.
func (f *fakeIBM) clusterService(resourceGroupID string) *ClusterService {
	service, err := NewClusterService(f.options("/global", resourceGroupID))
	if err != nil {
		f.t.Fatalf("building the cluster service: %v", err)
	}
	return service
}

// vpcService returns a VPCService for the fake server.
//
// Parameters:
//   - resourceGroupID is the optional resource group scope.
func (f *fakeIBM) vpcService(resourceGroupID string) *VPCService {
	service, err := NewVPCService(f.options("/v1", resourceGroupID))
	if err != nil {
		f.t.Fatalf("building the VPC service: %v", err)
	}
	return service
}

// recorded returns a copy of the recorded requests.
func (f *fakeIBM) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

// deletes returns the paths of the recorded DELETE requests.
func (f *fakeIBM) deletes() []string {
	var paths []string
	for _, r := range f.recorded() {
		if r.Method == http.MethodDelete {
			paths = append(paths, r.Path)
		}
	}
	return paths
}

// serve routes one request.
func (f *fakeIBM) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Header: r.Header.Clone()})
	f.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/global/v2/vpc/getClusters":
		if f.listStatus != 0 {
			writeError(w, f.listStatus)
			return
		}
		clusters := f.clusters
		if clusters == nil {
			clusters = []map[string]any{} // IBM answers an empty array, never null
		}
		writeJSON(w, http.StatusOK, clusters)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/global/v1/clusters/"):
		status, ok := f.deleteStatus[strings.TrimPrefix(r.URL.Path, "/global/v1/clusters/")]
		if !ok {
			status = http.StatusNoContent
		}
		if status >= 300 {
			writeError(w, status)
			return
		}
		w.WriteHeader(status)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/"):
		f.serveVPC(w, r, strings.TrimPrefix(r.URL.Path, "/v1/"))
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		writeError(w, http.StatusNotImplemented)
	}
}

// serveVPC answers one page of a VPC collection; the start token is the index of the page's first item.
func (f *fakeIBM) serveVPC(w http.ResponseWriter, r *http.Request, collection string) {
	if f.vpcStatus != 0 {
		writeError(w, f.vpcStatus)
		return
	}
	items := f.vpc[collection]
	start, _ := strconv.Atoi(r.URL.Query().Get("start"))
	end := len(items)
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
	}
	page := items[start:end]
	if page == nil {
		page = []map[string]any{}
	}
	body := map[string]any{
		collection:    page,
		"limit":       50,
		"total_count": len(items),
		"first":       map[string]any{"href": f.server.URL + "/v1/" + collection},
	}
	if end < len(items) {
		body["next"] = map[string]any{"href": fmt.Sprintf("%s/v1/%s?start=%d", f.server.URL, collection, end)}
	}
	writeJSON(w, http.StatusOK, body)
}

// writeJSON writes body as a JSON answer; a body that does not encode becomes a 500, which fails the test's request.
func writeJSON(w http.ResponseWriter, status int, body any) {
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// writeError writes an IBM-style error answer.
func writeError(w http.ResponseWriter, status int) {
	writeJSON(w, status, map[string]any{
		"code":        fmt.Sprintf("E%04d", status),
		"description": http.StatusText(status),
	})
}

// ibmCluster returns one entry of a getClusters answer in the tests' resource group, rg-e2e.
//
// Parameters:
//   - id, name, createdDate and state are the cluster's fields.
func ibmCluster(id, name, createdDate, state string) map[string]any {
	return ibmClusterInGroup(id, name, createdDate, state, "rg-e2e")
}

// ibmClusterInGroup returns one entry of a getClusters answer in a given resource group. The fake server ignores
// X-Auth-Resource-Group, so a test can make IBM's answer contain clusters outside the requested resource group.
//
// Parameters:
//   - id, name, createdDate and state are the cluster's fields.
//   - resourceGroupID is the cluster's resource group; "" omits the field, as an answer without one would.
func ibmClusterInGroup(id, name, createdDate, state, resourceGroupID string) map[string]any {
	cluster := map[string]any{"id": id, "name": name, "createdDate": createdDate, "state": state, "provider": "vpc-gen2"}
	if resourceGroupID != "" {
		cluster["resourceGroup"] = resourceGroupID
	}
	return cluster
}

// vpcItem returns one item of a VPC collection.
//
// Parameters:
//   - id and name are the resource's fields.
//   - resourceGroupID is its resource group.
//   - vpcID is its VPC; "" omits the reference.
func vpcItem(id, name, resourceGroupID, vpcID string) map[string]any {
	item := map[string]any{"id": id, "name": name, "resource_group": map[string]any{"id": resourceGroupID}}
	if vpcID != "" {
		item["vpc"] = map[string]any{"id": vpcID}
	}
	return item
}
