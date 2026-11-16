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

package kubernetesservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"golang.org/x/time/rate"

	. "github.com/onsi/gomega"
)

// *rate.Limiter is what cmd/main.go injects; this assertion keeps RequestLimiter compatible with it.
var _ RequestLimiter = (*rate.Limiter)(nil)

func TestNewService(t *testing.T) {
	authenticator, err := core.NewBearerTokenAuthenticator(testToken)
	if err != nil {
		t.Fatal(err)
	}
	valid := ServiceOptions{
		URL:           "https://containers.cloud.ibm.com/global",
		Authenticator: authenticator,
		RateLimiter:   rate.NewLimiter(rate.Limit(10), 20),
		HTTPTimeout:   30 * time.Second,
		UserAgent:     testUserAgent,
	}
	testCases := []struct {
		name        string
		mutate      func(o *ServiceOptions)
		expectError []string
	}{
		{
			name:   "valid options",
			mutate: func(*ServiceOptions) {},
		},
		{
			name: "valid options with a private regional URL and no User-Agent",
			mutate: func(o *ServiceOptions) {
				o.URL = "https://private.eu-de.containers.cloud.ibm.com/global"
				o.UserAgent = ""
			},
		},
		{
			name:        "empty URL",
			mutate:      func(o *ServiceOptions) { o.URL = "" },
			expectError: []string{"ServiceOptions.URL is empty", "https://containers.cloud.ibm.com/global"},
		},
		{
			name:        "http URL",
			mutate:      func(o *ServiceOptions) { o.URL = "http://containers.cloud.ibm.com/global" },
			expectError: []string{`ServiceOptions.URL "http://containers.cloud.ibm.com/global" is not an absolute https URL`},
		},
		{
			name:        "relative URL",
			mutate:      func(o *ServiceOptions) { o.URL = "containers.cloud.ibm.com/global" },
			expectError: []string{"is not an absolute https URL"},
		},
		{
			name:        "URL with a query",
			mutate:      func(o *ServiceOptions) { o.URL = "https://containers.cloud.ibm.com/global?x=1" },
			expectError: []string{"without query or fragment"},
		},
		{
			name:        "nil authenticator",
			mutate:      func(o *ServiceOptions) { o.Authenticator = nil },
			expectError: []string{"ServiceOptions.Authenticator is nil", "never falls back to the environment"},
		},
		{
			name:        "nil limiter",
			mutate:      func(o *ServiceOptions) { o.RateLimiter = nil },
			expectError: []string{"ServiceOptions.RateLimiter is nil"},
		},
		{
			name:        "typed nil limiter",
			mutate:      func(o *ServiceOptions) { o.RateLimiter = (*rate.Limiter)(nil) },
			expectError: []string{"ServiceOptions.RateLimiter is nil"},
		},
		{
			name:        "zero timeout",
			mutate:      func(o *ServiceOptions) { o.HTTPTimeout = 0 },
			expectError: []string{"ServiceOptions.HTTPTimeout is 0s", "positive duration"},
		},
		{
			name:        "negative timeout",
			mutate:      func(o *ServiceOptions) { o.HTTPTimeout = -time.Second },
			expectError: []string{"ServiceOptions.HTTPTimeout is -1s"},
		},
		{
			name:        "User-Agent with a line break",
			mutate:      func(o *ServiceOptions) { o.UserAgent = "capibm\r\nX-Injected: 1" },
			expectError: []string{"ServiceOptions.UserAgent contains a line break"},
		},
		{
			name: "every field invalid names every field",
			mutate: func(o *ServiceOptions) {
				*o = ServiceOptions{URL: "ftp://x"}
			},
			expectError: []string{"ServiceOptions.URL", "ServiceOptions.Authenticator", "ServiceOptions.RateLimiter", "ServiceOptions.HTTPTimeout"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			opts := valid
			tc.mutate(&opts)
			service, err := NewService(opts)
			if len(tc.expectError) == 0 {
				g.Expect(err).ToNot(HaveOccurred())
				g.Expect(service).ToNot(BeNil())
				return
			}
			g.Expect(err).To(HaveOccurred())
			g.Expect(service).To(BeNil())
			for _, fragment := range tc.expectError {
				g.Expect(err.Error()).To(ContainSubstring(fragment))
			}
		})
	}
}

// TestNewServiceIgnoresEnvironment proves that neither NewService nor a request reads go-sdk-core's external
// configuration: the variables below would select another URL and authenticator if anything consulted them.
func TestNewServiceIgnoresEnvironment(t *testing.T) {
	t.Run("kubernetesservice reads no environment", func(t *testing.T) {
		g := NewWithT(t)
		t.Setenv("IBM_CREDENTIALS_FILE", "/nonexistent")
		t.Setenv("IBMCLOUD_AUTH_TYPE", "bogus")
		t.Setenv("IBMCLOUD_APIKEY", "bogus")
		t.Setenv("KUBERNETES_SERVICE_API_AUTH_TYPE", "bogus")
		t.Setenv("KUBERNETES_SERVICE_API_URL", "https://environment.invalid/global")

		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")))
		opts := testOptions(t, fake, &countingLimiter{})

		withoutAuthenticator := opts
		withoutAuthenticator.Authenticator = nil
		_, err := newService(withoutAuthenticator, fake.server.Client().Transport)
		g.Expect(err).To(MatchError(ContainSubstring("ServiceOptions.Authenticator is nil")))

		service, err := newService(opts, fake.server.Client().Transport)
		g.Expect(err).ToNot(HaveOccurred())
		cluster, err := service.GetCluster(context.Background(), testIKSClusterID, "")
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(cluster.ID).To(Equal(testIKSClusterID))
		request := fake.last(t)
		g.Expect(request.Path).To(Equal("/global/v2/vpc/getCluster"))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
	})
}

func TestGetCluster(t *testing.T) {
	iksDeployed := Cluster{
		ID:                        testIKSClusterID,
		Name:                      "example-test-cluster-probe-iks",
		CRN:                       "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:" + testIKSClusterID + "::",
		Type:                      "kubernetes",
		Provider:                  "vpc-gen2",
		ResourceGroupID:           testResourceGroupID,
		CreatedDate:               "2026-11-16T03:05:33+0000",
		State:                     "deployed",
		MasterState:               "deployed",
		MasterHealth:              "",
		MasterStatus:              "Ready",
		MasterKubeVersion:         "1.36.5_1537",
		TargetVersion:             "1.36.5_1537",
		VersionEOS:                "",
		MasterURL:                 "https://c111.eu-de.containers.cloud.ibm.com:30101",
		PublicServiceEndpointURL:  "https://c111.eu-de.containers.cloud.ibm.com:30101",
		PrivateServiceEndpointURL: "https://c111.private.eu-de.containers.cloud.ibm.com:30101",
		PodSubnet:                 "172.17.0.0/18",
		ServiceSubnet:             "172.21.0.0/16",
	}
	roksNormal := Cluster{
		ID:                        testROKSClusterID,
		Name:                      "example-test-cluster-probe-roks",
		CRN:                       "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:" + testROKSClusterID + "::",
		Type:                      "openshift",
		Provider:                  "vpc-gen2",
		ResourceGroupID:           testResourceGroupID,
		CreatedDate:               "2026-11-16T03:06:29+0000",
		State:                     "normal",
		MasterState:               "deployed",
		MasterHealth:              "normal",
		MasterStatus:              "Ready",
		MasterKubeVersion:         "4.21.35_1537_openshift",
		TargetVersion:             "4.21.35_1537_openshift",
		MasterURL:                 "https://c113-e.eu-de.containers.cloud.ibm.com:30102",
		PublicServiceEndpointURL:  "https://c113-e.eu-de.containers.cloud.ibm.com:30102",
		PrivateServiceEndpointURL: "https://c113.private.eu-de.containers.cloud.ibm.com:30102",
		PodSubnet:                 "172.17.64.0/18",
		ServiceSubnet:             "172.21.0.0/16",
	}

	t.Run("full decode of the live deployed IKS and normal ROKS answers", func(t *testing.T) {
		for _, tc := range []struct {
			fixture   string
			clusterID string
			expected  Cluster
		}{
			{fixture: "iks-getCluster-deployed.json", clusterID: testIKSClusterID, expected: iksDeployed},
			{fixture: "roks-getCluster-normal.json", clusterID: testROKSClusterID, expected: roksNormal},
		} {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, tc.fixture)))
			service, _ := newTestService(t, fake)
			cluster, err := service.GetCluster(context.Background(), tc.clusterID, "")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(*cluster).To(Equal(tc.expected), tc.fixture)
		}
	})

	// Every state of the create sequence observed on IBM Cloud: the lifecycle fields that IsMasterReady and the state
	// classification read, and the master URL that appears only once the master is deployed.
	lifecycleCases := []struct {
		fixture      string
		state        string
		masterState  string
		masterHealth string
		masterStatus string
		masterURL    string
	}{
		{"iks-getCluster-deploying.json", "deploying", "deploying", "", "Deploy in progress.", ""},
		{"iks-getCluster-deployed.json", "deployed", "deployed", "", "Ready", "https://c111.eu-de.containers.cloud.ibm.com:30101"},
		{"iks-getCluster-pending.json", "pending", "deployed", "normal", "Ready", "https://c111.eu-de.containers.cloud.ibm.com:30101"},
		{"iks-getCluster-normal.json", "normal", "deployed", "normal", "Ready", "https://c111.eu-de.containers.cloud.ibm.com:30101"},
		{"roks-getCluster-deploying.json", "deploying", "deploying", "", "Deploy in progress.", ""},
		{"roks-getCluster-deployed.json", "deployed", "deployed", "", "Ready", "https://c113-e.eu-de.containers.cloud.ibm.com:30102"},
		{"roks-getCluster-pending.json", "pending", "deployed", "normal", "Ready", "https://c113-e.eu-de.containers.cloud.ibm.com:30102"},
		{"roks-getCluster-normal.json", "normal", "deployed", "normal", "Ready", "https://c113-e.eu-de.containers.cloud.ibm.com:30102"},
	}
	for _, tc := range lifecycleCases {
		t.Run("lifecycle of "+tc.fixture, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, tc.fixture)))
			service, _ := newTestService(t, fake)
			cluster, err := service.GetCluster(context.Background(), "some-cluster", "")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cluster.State).To(Equal(tc.state))
			g.Expect(cluster.MasterState).To(Equal(tc.masterState))
			g.Expect(cluster.MasterHealth).To(Equal(tc.masterHealth))
			g.Expect(cluster.MasterStatus).To(Equal(tc.masterStatus))
			g.Expect(cluster.MasterURL).To(Equal(tc.masterURL))
		})
	}

	t.Run("request by ID without a resource group", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")))
		service, _ := newTestService(t, fake)
		_, err := service.GetCluster(context.Background(), testIKSClusterID, "")
		g.Expect(err).ToNot(HaveOccurred())
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodGet))
		g.Expect(request.Path).To(Equal("/global/v2/vpc/getCluster"))
		g.Expect(request.RawQuery).To(Equal("cluster=" + testIKSClusterID))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Values("X-Auth-Resource-Group")).To(BeEmpty())
	})

	t.Run("name lookup scoped by X-Auth-Resource-Group", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")))
		service, _ := newTestService(t, fake)
		_, err := service.GetCluster(context.Background(), "example-test-cluster-probe-iks", testResourceGroupID)
		g.Expect(err).ToNot(HaveOccurred())
		request := fake.last(t)
		g.Expect(request.RawQuery).To(Equal("cluster=example-test-cluster-probe-iks"))
		g.Expect(request.Header.Get("X-Auth-Resource-Group")).To(Equal(testResourceGroupID))
	})

	t.Run("404 is NotFound with IBM's code and incident", func(t *testing.T) {
		g := NewWithT(t)
		body := []byte(`{"incidentID":"00000000-0000-4000-8000-0000000000aa","code":"G0004","description":"The specified cluster could not be found.","type":"General"}`)
		fake := newFakeIKS(t, reply(http.StatusNotFound, body))
		service, _ := newTestService(t, fake)
		_, err := service.GetCluster(context.Background(), testIKSClusterID, "")
		g.Expect(Classify(OpGetCluster, err)).To(Equal(ClassNotFound))
		var apiErr *APIError
		g.Expect(errors.As(err, &apiErr)).To(BeTrue())
		g.Expect(*apiErr).To(Equal(APIError{
			Operation:   OpGetCluster,
			StatusCode:  http.StatusNotFound,
			Code:        "G0004",
			Description: "The specified cluster could not be found.",
			IncidentID:  "00000000-0000-4000-8000-0000000000aa",
		}))
	})

	t.Run("an empty ID or name is refused before sending", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "iks-getCluster-normal.json")))
		service, limiter := newTestService(t, fake)
		_, err := service.GetCluster(context.Background(), "", testResourceGroupID)
		g.Expect(err).To(MatchError(ErrInvalidRequest))
		g.Expect(err.Error()).To(ContainSubstring("GetCluster: the cluster ID or name is empty"))
		g.Expect(Classify(OpGetCluster, err)).To(Equal(ClassBadRequest))
		g.Expect(fake.received()).To(BeEmpty())
		g.Expect(limiter.count()).To(BeZero())
	})

	t.Run("a 200 without a body is an error, not an empty cluster", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		service, _ := newTestService(t, fake)
		cluster, err := service.GetCluster(context.Background(), testIKSClusterID, "")
		g.Expect(cluster).To(BeNil())
		g.Expect(err).To(MatchError(ContainSubstring("kubernetesservice: GetCluster: ")))
		g.Expect(Classify(OpGetCluster, err)).To(Equal(ClassTransient))
	})
}

const (
	// testVPCID and testSubnetID are synthetic VPC and subnet IDs.
	testVPCID    = "r010-00000000-0000-4000-8000-000000000011"
	testSubnetID = "02b7-00000000-0000-4000-8000-000000000002"
	// testCOSCRN is a synthetic Cloud Object Storage instance CRN.
	testCOSCRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/00000000000000000000000000000000:00000000-0000-4000-8000-000000000020::"
	// synthesizedA0656 is an A0656 body. The code is the one observed on IBM Cloud; the description and incident are
	// synthesized, because no A0656 body was kept and Classify reads only the code.
	synthesizedA0656 = `{"incidentID":"00000000-0000-4000-8000-0000000000bb","code":"A0656","description":"synthesized: the infrastructure API key cannot be created","type":"Authentication"}`
)

// iksCreateRequest is a valid IKS CreateClusterRequest.
func iksCreateRequest() CreateClusterRequest {
	return CreateClusterRequest{
		Name:            "managed-iks",
		ResourceGroupID: testResourceGroupID,
		KubeVersion:     "1.36",
		WorkerPool: InitialWorkerPool{
			Name:               "default",
			Flavor:             "bx2.4x16",
			VPCID:              testVPCID,
			WorkerCountPerZone: 1,
			Zones:              []ZoneSubnet{{Zone: "eu-de-1", SubnetID: testSubnetID}},
		},
	}
}

func TestCreateCluster(t *testing.T) {
	created := fixture(t, "createCluster-201-non-critical-errors.json")

	t.Run("IKS body and headers", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusCreated, []byte(`{"clusterID":"`+testIKSClusterID+`"}`)))
		service, _ := newTestService(t, fake)
		result, err := service.CreateCluster(context.Background(), iksCreateRequest())
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(*result).To(Equal(CreateClusterResult{ClusterID: testIKSClusterID}))
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodPost))
		g.Expect(request.Path).To(Equal("/global/v2/vpc/createCluster"))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Get("X-Auth-Resource-Group")).To(Equal(testResourceGroupID))
		g.Expect(request.Header.Get("Content-Type")).To(Equal("application/json"))
		g.Expect(string(request.Body)).To(MatchJSON(`{
			"name": "managed-iks",
			"kubeVersion": "1.36",
			"provider": "vpc-gen2",
			"disablePublicServiceEndpoint": false,
			"workerPool": {
				"name": "default",
				"flavor": "bx2.4x16",
				"vpcID": "` + testVPCID + `",
				"workerCount": 1,
				"zones": [{"id": "eu-de-1", "subnetID": "` + testSubnetID + `"}]
			}
		}`))
	})

	t.Run("ROKS body carries COS, entitlement and networks, and no pool name or OS", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusCreated, []byte(`{"clusterID":"`+testROKSClusterID+`"}`)))
		service, _ := newTestService(t, fake)
		req := iksCreateRequest()
		req.Name = "managed-roks"
		req.KubeVersion = "4.21_openshift"
		req.DisablePublicServiceEndpoint = true
		req.PodSubnet = "172.17.64.0/18"
		req.ServiceSubnet = "172.21.0.0/16"
		req.COSInstanceCRN = testCOSCRN
		req.DefaultWorkerPoolEntitlement = "ocp_entitled"
		req.WorkerPool.Name = ""
		req.WorkerPool.WorkerCountPerZone = 2
		req.WorkerPool.Zones = append(req.WorkerPool.Zones, ZoneSubnet{Zone: "eu-de-2", SubnetID: "02b7-00000000-0000-4000-8000-000000000003"})
		result, err := service.CreateCluster(context.Background(), req)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(result.ClusterID).To(Equal(testROKSClusterID))
		g.Expect(string(fake.last(t).Body)).To(MatchJSON(`{
			"name": "managed-roks",
			"kubeVersion": "4.21_openshift",
			"provider": "vpc-gen2",
			"disablePublicServiceEndpoint": true,
			"podSubnet": "172.17.64.0/18",
			"serviceSubnet": "172.21.0.0/16",
			"cosInstanceCRN": "` + testCOSCRN + `",
			"defaultWorkerPoolEntitlement": "ocp_entitled",
			"workerPool": {
				"flavor": "bx2.4x16",
				"vpcID": "` + testVPCID + `",
				"workerCount": 2,
				"zones": [
					{"id": "eu-de-1", "subnetID": "` + testSubnetID + `"},
					{"id": "eu-de-2", "subnetID": "02b7-00000000-0000-4000-8000-000000000003"}
				]
			}
		}`))
	})

	t.Run("createCluster 201 non-critical errors decoded", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusCreated, created))
		service, _ := newTestService(t, fake)
		result, err := service.CreateCluster(context.Background(), iksCreateRequest())
		g.Expect(err).ToNot(HaveOccurred(), "a 201 with non_critical_errors is a success: IBM created the cluster")
		g.Expect(result.ClusterID).To(Equal(testIKSClusterID))
		g.Expect(result.IncidentID).To(Equal("00000000-0000-4000-8000-0000000000e1"))
		g.Expect(result.NonCriticalErrors).To(HaveLen(1))
		message := result.NonCriticalErrors[0]
		g.Expect(message.Code).To(Equal("E49df"))
		g.Expect(message.Type).To(Equal("Provisioning"))
		g.Expect(message.Description).To(HavePrefix("Your cluster cannot pull images from the IBM Cloud Container Registry"))
		g.Expect(message.RecoveryCLI).ToNot(BeEmpty())
	})

	errorCases := []struct {
		name     string
		status   int
		body     []byte
		expected ErrorClass
	}{
		{name: "createCluster A0656 with 400 is InfrastructureKeyNotCreatable", status: http.StatusBadRequest, body: []byte(synthesizedA0656), expected: ClassInfrastructureKeyNotCreatable},
		{name: "createCluster A0656 with 403 is InfrastructureKeyNotCreatable", status: http.StatusForbidden, body: []byte(synthesizedA0656), expected: ClassInfrastructureKeyNotCreatable},
		{name: "createCluster 403 is InfrastructureKeyDenied", status: http.StatusForbidden, body: []byte(`{"code":"","description":"The credentials set for the region and resource group have insufficient permissions."}`), expected: ClassInfrastructureKeyDenied},
		{name: "409 E0007 is ClusterNameExists", status: http.StatusConflict, body: []byte(`{"code":"E0007","description":"The cluster name is already in use."}`), expected: ClassClusterNameExists},
		{name: "live 400 P4102 subnet not found is BadRequest", status: http.StatusBadRequest, body: fixture(t, "error-P4102-subnet-not-found.json"), expected: ClassBadRequest},
		{name: "live 400 E7278 ROKS without COS is BadRequest", status: http.StatusBadRequest, body: fixture(t, "error-E7278-roks-no-cos.json"), expected: ClassBadRequest},
		{name: "401 is Unauthorized", status: http.StatusUnauthorized, body: []byte(`{"code":"G0001","description":"You must specify an IAM token as an Authorization header."}`), expected: ClassUnauthorized},
		{name: "429 is RateLimited", status: http.StatusTooManyRequests, body: nil, expected: ClassRateLimited},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(tc.status, tc.body))
			service, _ := newTestService(t, fake)
			result, err := service.CreateCluster(context.Background(), iksCreateRequest())
			g.Expect(result).To(BeNil())
			g.Expect(Classify(OpCreateCluster, err)).To(Equal(tc.expected))
			g.Expect(fake.received()).To(HaveLen(1), "CreateCluster is sent exactly once")
		})
	}

	t.Run("a POST answered 500 is sent exactly once and is Transient", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusInternalServerError, []byte(`{"code":"E0004","description":"Internal error."}`)))
		service, limiter := newTestService(t, fake)
		_, err := service.CreateCluster(context.Background(), iksCreateRequest())
		g.Expect(Classify(OpCreateCluster, err)).To(Equal(ClassTransient))
		g.Expect(fake.received()).To(HaveLen(1))
		g.Expect(limiter.count()).To(Equal(1))
	})

	t.Run("a 201 without clusterID leaves the outcome unknown", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusCreated, []byte(`{}`)))
		service, _ := newTestService(t, fake)
		result, err := service.CreateCluster(context.Background(), iksCreateRequest())
		g.Expect(result).To(BeNil())
		g.Expect(err).To(MatchError(ContainSubstring("without a clusterID; the outcome of this create attempt is unknown")))
		g.Expect(Classify(OpCreateCluster, err)).To(Equal(ClassTransient))
	})

	validationCases := []struct {
		name     string
		mutate   func(r *CreateClusterRequest)
		messages []string
	}{
		{name: "empty name", mutate: func(r *CreateClusterRequest) { r.Name = "" }, messages: []string{"Name is empty"}},
		{name: "empty resource group", mutate: func(r *CreateClusterRequest) { r.ResourceGroupID = "" }, messages: []string{"ResourceGroupID is empty", "X-Auth-Resource-Group"}},
		{name: "empty version", mutate: func(r *CreateClusterRequest) { r.KubeVersion = "" }, messages: []string{`KubeVersion is empty; pass a minor such as "1.36"`}},
		{name: "unknown entitlement", mutate: func(r *CreateClusterRequest) { r.DefaultWorkerPoolEntitlement = "cloud_pak" }, messages: []string{`DefaultWorkerPoolEntitlement "cloud_pak" is not supported`}},
		{name: "empty flavor", mutate: func(r *CreateClusterRequest) { r.WorkerPool.Flavor = "" }, messages: []string{"WorkerPool.Flavor is empty"}},
		{name: "empty VPC", mutate: func(r *CreateClusterRequest) { r.WorkerPool.VPCID = "" }, messages: []string{"WorkerPool.VPCID is empty"}},
		{name: "zero workers per zone", mutate: func(r *CreateClusterRequest) { r.WorkerPool.WorkerCountPerZone = 0 }, messages: []string{"WorkerPool.WorkerCountPerZone is 0; pass at least 1"}},
		{name: "no zones", mutate: func(r *CreateClusterRequest) { r.WorkerPool.Zones = nil }, messages: []string{"WorkerPool.Zones is empty"}},
		{name: "zone without name and subnet", mutate: func(r *CreateClusterRequest) { r.WorkerPool.Zones = []ZoneSubnet{{}} }, messages: []string{"WorkerPool.Zones[0].Zone is empty", "WorkerPool.Zones[0].SubnetID is empty"}},
		{
			name: "zone listed twice",
			mutate: func(r *CreateClusterRequest) {
				r.WorkerPool.Zones = append(r.WorkerPool.Zones, ZoneSubnet{Zone: "eu-de-1", SubnetID: testSubnetID})
			},
			messages: []string{`WorkerPool.Zones[1].Zone "eu-de-1" appears twice`},
		},
		{
			name:     "every problem is reported at once",
			mutate:   func(r *CreateClusterRequest) { *r = CreateClusterRequest{} },
			messages: []string{"Name is empty", "ResourceGroupID is empty", "KubeVersion is empty", "WorkerPool.Flavor is empty", "WorkerPool.Zones is empty"},
		},
	}
	for _, tc := range validationCases {
		t.Run("refused before sending: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(http.StatusCreated, created))
			service, limiter := newTestService(t, fake)
			req := iksCreateRequest()
			tc.mutate(&req)
			_, err := service.CreateCluster(context.Background(), req)
			g.Expect(err).To(MatchError(ErrInvalidRequest))
			for _, message := range tc.messages {
				g.Expect(err.Error()).To(ContainSubstring(message))
			}
			g.Expect(Classify(OpCreateCluster, err)).To(Equal(ClassBadRequest))
			g.Expect(fake.received()).To(BeEmpty())
			g.Expect(limiter.count()).To(BeZero())
		})
	}
}

func TestListVersions(t *testing.T) {
	t.Run("decodes the live versions list", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, fixture(t, "versions.json")))
		service, _ := newTestService(t, fake)
		versions, err := service.ListVersions(context.Background())
		g.Expect(err).ToNot(HaveOccurred())
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodGet))
		g.Expect(request.Path).To(Equal("/global/v1/versions"))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(versions.Kubernetes).To(HaveLen(4))
		g.Expect(versions.OpenShift).To(HaveLen(5))
		g.Expect(versions.Kubernetes[0]).To(Equal(ReleaseVersion{Major: 1, Minor: 34, Patch: 12, EndOfService: "2027-01-20T23:59:59+0000"}))
		g.Expect(versions.OpenShift[0]).To(Equal(ReleaseVersion{Major: 4, Minor: 18, Patch: 56}))
		g.Expect(versions.Kubernetes).To(ContainElement(ReleaseVersion{Major: 1, Minor: 36, Patch: 5, Default: true}))
	})

	t.Run("missing lists decode as empty", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, []byte(`{"kubernetes":[]}`)))
		service, _ := newTestService(t, fake)
		versions, err := service.ListVersions(context.Background())
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(versions.Kubernetes).To(BeEmpty())
		g.Expect(versions.OpenShift).To(BeEmpty())
	})

	t.Run("5xx is Transient", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusServiceUnavailable, nil))
		service, _ := newTestService(t, fake)
		_, err := service.ListVersions(context.Background())
		g.Expect(Classify(OpListVersions, err)).To(Equal(ClassTransient))
	})
}

func TestDeleteCluster(t *testing.T) {
	t.Run("DELETE by ID with deleteResources=false and the resource group", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		service, _ := newTestService(t, fake)
		g.Expect(service.DeleteCluster(context.Background(), testIKSClusterID, testResourceGroupID)).To(Succeed())
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodDelete))
		g.Expect(request.Path).To(Equal("/global/v1/clusters/" + testIKSClusterID))
		g.Expect(request.RawQuery).To(Equal("deleteResources=false"))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Get("X-Auth-Resource-Group")).To(Equal(testResourceGroupID))
		g.Expect(request.Body).To(BeEmpty())
	})

	t.Run("no resource group header when none is given", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		service, _ := newTestService(t, fake)
		g.Expect(service.DeleteCluster(context.Background(), testROKSClusterID, "")).To(Succeed())
		g.Expect(fake.last(t).Header.Values("X-Auth-Resource-Group")).To(BeEmpty())
	})

	// The delete flow maps these classes itself and never treats one as terminal.
	errorCases := []struct {
		name     string
		status   int
		body     []byte
		expected ErrorClass
	}{
		{name: "404 is NotFound", status: http.StatusNotFound, body: []byte(`{"code":"G0004","description":"The specified cluster could not be found."}`), expected: ClassNotFound},
		{name: "409 keeps its status for WaitingForIBMDeletion", status: http.StatusConflict, body: []byte(`{"code":"E0d02","description":"synthesized conflict"}`), expected: ClassUnknown},
		{name: "400 is BadRequest", status: http.StatusBadRequest, body: nil, expected: ClassBadRequest},
		{name: "403 is PermissionDenied", status: http.StatusForbidden, body: nil, expected: ClassPermissionDenied},
		{name: "500 is Transient", status: http.StatusInternalServerError, body: nil, expected: ClassTransient},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, reply(tc.status, tc.body))
			service, _ := newTestService(t, fake)
			err := service.DeleteCluster(context.Background(), testIKSClusterID, "")
			g.Expect(Classify(OpDeleteCluster, err)).To(Equal(tc.expected))
			var apiErr *APIError
			g.Expect(errors.As(err, &apiErr)).To(BeTrue())
			g.Expect(apiErr.StatusCode).To(Equal(tc.status))
			g.Expect(apiErr.Operation).To(Equal(OpDeleteCluster))
			g.Expect(fake.received()).To(HaveLen(1), "DeleteCluster is sent exactly once")
		})
	}

	t.Run("an empty cluster ID is refused before sending", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		service, limiter := newTestService(t, fake)
		err := service.DeleteCluster(context.Background(), "", testResourceGroupID)
		g.Expect(err).To(MatchError(ErrInvalidRequest))
		g.Expect(err.Error()).To(ContainSubstring("DeleteCluster: the cluster ID is empty"))
		g.Expect(fake.received()).To(BeEmpty())
		g.Expect(limiter.count()).To(BeZero())
	})
}

// servePath returns a handler that answers status and body on path and 404 on any other path, so that a test also
// proves which path a method calls.
func servePath(path string, status int, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			reply(http.StatusNotFound, []byte(`{"code":"test","description":"unexpected path"}`))(w, r)
			return
		}
		reply(status, body)(w, r)
	}
}

// The getWorkerPools fixtures were captured on GET /v2/vpc/getWorkerPools, while ListWorkerPools calls the SDK's
// GET /v2/getWorkerPools. Until that path is captured on IBM Cloud, the fake serves the /v2/vpc bodies on the SDK's
// path; a capture of it then replaces them.
func TestListWorkerPools(t *testing.T) {
	iksPool := WorkerPool{
		ID:              testIKSClusterID + "-00000a1",
		Name:            "example-test-cluster-pool",
		Flavor:          "cx2.2x4",
		SizePerZone:     1,
		Zones:           []WorkerPoolZone{{ID: "eu-de-1", WorkerCount: 1, AutobalanceEnabled: true}},
		IsBalanced:      true,
		OperatingSystem: "UBUNTU_24_64",
	}
	roksDeploying := WorkerPool{
		ID:              testROKSClusterID + "-00000b2",
		Name:            "default",
		Flavor:          "bx2.4x16",
		SizePerZone:     2,
		Zones:           []WorkerPoolZone{{ID: "eu-de-1", WorkerCount: 0, AutobalanceEnabled: true}},
		IsBalanced:      true,
		OperatingSystem: "RHCOS",
	}
	decodeCases := []struct {
		fixture  string
		expected WorkerPool
	}{
		{fixture: "iks-getWorkerPools-normal.json", expected: iksPool},
		{fixture: "iks-getWorkerPools-deploying.json", expected: iksPool},
		{fixture: "iks-getWorkerPools-deployed.json", expected: iksPool},
		{fixture: "iks-getWorkerPools-pending.json", expected: iksPool},
		// Right after create the zone count lags the pool's per-zone size (observed on IBM Cloud).
		{fixture: "roks-getWorkerPools-deploying.json", expected: roksDeploying},
	}
	for _, tc := range decodeCases {
		t.Run("decodes "+tc.fixture+" as an array", func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, servePath("/global/v2/getWorkerPools", http.StatusOK, fixture(t, tc.fixture)))
			service, _ := newTestService(t, fake)
			pools, err := service.ListWorkerPools(context.Background(), "some-cluster")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(pools).To(Equal([]WorkerPool{tc.expected}))
		})
	}

	t.Run("request shape", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, servePath("/global/v2/getWorkerPools", http.StatusOK, fixture(t, "roks-getWorkerPools-normal.json")))
		service, _ := newTestService(t, fake)
		pools, err := service.ListWorkerPools(context.Background(), testROKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(pools).To(HaveLen(1))
		g.Expect(pools[0].Zones).To(Equal([]WorkerPoolZone{{ID: "eu-de-1", WorkerCount: 2, AutobalanceEnabled: true}}))
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodGet))
		g.Expect(request.RawQuery).To(Equal("cluster=" + testROKSClusterID))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Values("X-Region")).To(BeEmpty())
	})

	t.Run("pool lifecycle and autoscale flags", func(t *testing.T) {
		g := NewWithT(t)
		body := []byte(`[{"id":"p1","poolName":"gone","lifecycle":{"actualState":"deleting"},"autoscaleEnabled":true,"isBalanced":false,"workerCount":3}]`)
		fake := newFakeIKS(t, servePath("/global/v2/getWorkerPools", http.StatusOK, body))
		service, _ := newTestService(t, fake)
		pools, err := service.ListWorkerPools(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(pools).To(Equal([]WorkerPool{{ID: "p1", Name: "gone", ActualState: "deleting", SizePerZone: 3, AutoscaleEnabled: true}}))
	})

	t.Run("no pools", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, servePath("/global/v2/getWorkerPools", http.StatusOK, []byte(`[]`)))
		service, _ := newTestService(t, fake)
		pools, err := service.ListWorkerPools(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(pools).To(BeEmpty())
	})

	t.Run("404 is NotFound", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusNotFound, []byte(`{"code":"G0004","description":"The specified cluster could not be found."}`)))
		service, _ := newTestService(t, fake)
		_, err := service.ListWorkerPools(context.Background(), testIKSClusterID)
		g.Expect(Classify(OpListWorkerPools, err)).To(Equal(ClassNotFound))
	})

	t.Run("an empty cluster ID is refused before sending", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, []byte(`[]`)))
		service, limiter := newTestService(t, fake)
		_, err := service.ListWorkerPools(context.Background(), "")
		g.Expect(err).To(MatchError(ErrInvalidRequest))
		g.Expect(fake.received()).To(BeEmpty())
		g.Expect(limiter.count()).To(BeZero())
	})

	// The /v2/vpc/getWorkerPools fallback decodes the path observed on IBM Cloud; it is inactive while
	// ListWorkerPoolsViaSDK is true, and tested directly so that switching is the one-line change it is meant to be.
	t.Run("fallback decodes /v2/vpc/getWorkerPools as an array", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, servePath("/global/v2/vpc/getWorkerPools", http.StatusOK, fixture(t, "iks-getWorkerPools-normal.json")))
		service, limiter := newTestService(t, fake)
		pools, err := service.listWorkerPoolsFromVPCPath(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(pools).To(Equal([]WorkerPool{iksPool}))
		request := fake.last(t)
		g.Expect(request.RawQuery).To(Equal("cluster=" + testIKSClusterID))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
		g.Expect(request.Header.Get("User-Agent")).To(Equal(testUserAgent))
		g.Expect(limiter.count()).To(Equal(1))
	})

	t.Run("fallback for /v2/vpc/getWorkerPools maps errors", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusInternalServerError, nil))
		service, _ := newTestService(t, fake)
		_, err := service.listWorkerPoolsFromVPCPath(context.Background(), testIKSClusterID)
		g.Expect(Classify(OpListWorkerPools, err)).To(Equal(ClassTransient))
	})
}

func TestListWorkers(t *testing.T) {
	iksNormal := Worker{
		ID:                 "kube-" + testIKSClusterID + "-exampletest-example-0000015f",
		PoolID:             testIKSClusterID + "-00000a1",
		Zone:               "eu-de-1",
		ActualState:        "deployed",
		HealthState:        "normal",
		KubeVersionActual:  "1.36.5_1540",
		KubeVersionTarget:  "1.36.5_1540",
		KubeVersionDesired: "1.36.5_1540",
	}
	roksPending := func(suffix string) Worker {
		return Worker{
			ID:                 "kube-" + testROKSClusterID + "-exampletest-default-" + suffix,
			PoolID:             testROKSClusterID + "-00000b2",
			Zone:               "eu-de-1",
			ActualState:        "provision_pending",
			KubeVersionTarget:  "4.21.40_1540_openshift",
			KubeVersionDesired: "4.21.40_1540_openshift",
		}
	}
	decodeCases := []struct {
		fixture  string
		expected []Worker
	}{
		{fixture: "iks-getWorkers-normal.json", expected: []Worker{iksNormal}},
		{fixture: "roks-getWorkers-deployed.json", expected: []Worker{roksPending("000001ec"), roksPending("0000029f")}},
		{fixture: "roks-getWorkers-deploying.json", expected: []Worker{}},
	}
	for _, tc := range decodeCases {
		t.Run("decodes "+tc.fixture, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, servePath("/global/v2/vpc/getWorkers", http.StatusOK, fixture(t, tc.fixture)))
			service, _ := newTestService(t, fake)
			workers, err := service.ListWorkers(context.Background(), "some-cluster")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(workers).To(Equal(tc.expected))
		})
	}

	stateCases := []struct {
		fixture string
		states  []string
	}{
		{fixture: "iks-getWorkers-deploying.json", states: []string{"provisioning"}},
		{fixture: "iks-getWorkers-deployed.json", states: []string{"provisioning"}},
		{fixture: "iks-getWorkers-pending.json", states: []string{"provisioning"}},
		{fixture: "roks-getWorkers-pending.json", states: []string{"provisioning", "provisioning"}},
		{fixture: "roks-getWorkers-normal.json", states: []string{"deployed", "deployed"}},
	}
	for _, tc := range stateCases {
		t.Run("states of "+tc.fixture, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, servePath("/global/v2/vpc/getWorkers", http.StatusOK, fixture(t, tc.fixture)))
			service, _ := newTestService(t, fake)
			workers, err := service.ListWorkers(context.Background(), "some-cluster")
			g.Expect(err).ToNot(HaveOccurred())
			states := make([]string, 0, len(workers))
			for _, worker := range workers {
				states = append(states, worker.ActualState)
				g.Expect(worker.PendingOperation).To(BeEmpty(), "pendingOperation is empty on every probed worker")
			}
			g.Expect(states).To(Equal(tc.states))
		})
	}

	t.Run("request asks for deleted workers", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, servePath("/global/v2/vpc/getWorkers", http.StatusOK, fixture(t, "iks-getWorkers-normal.json")))
		service, _ := newTestService(t, fake)
		_, err := service.ListWorkers(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		request := fake.last(t)
		g.Expect(request.Method).To(Equal(http.MethodGet))
		g.Expect(request.RawQuery).To(Equal("cluster=" + testIKSClusterID + "&showDeleted=true"))
		g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
	})

	t.Run("a deleted record decodes with its state", func(t *testing.T) {
		g := NewWithT(t)
		var records []map[string]any
		g.Expect(json.Unmarshal(fixture(t, "iks-getWorkers-normal.json"), &records)).To(Succeed())
		records[0]["lifecycle"].(map[string]any)["actualState"] = "deleted"
		records[0]["lifecycle"].(map[string]any)["pendingOperation"] = "deleting"
		body, err := json.Marshal(records)
		g.Expect(err).ToNot(HaveOccurred())
		fake := newFakeIKS(t, reply(http.StatusOK, body))
		service, _ := newTestService(t, fake)
		workers, err := service.ListWorkers(context.Background(), testIKSClusterID)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(workers).To(HaveLen(1))
		g.Expect(workers[0].ActualState).To(Equal("deleted"))
		g.Expect(workers[0].PendingOperation).To(Equal("deleting"))
	})

	t.Run("5xx is Transient", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusBadGateway, nil))
		service, _ := newTestService(t, fake)
		_, err := service.ListWorkers(context.Background(), testIKSClusterID)
		g.Expect(Classify(OpListWorkers, err)).To(Equal(ClassTransient))
	})

	t.Run("an empty cluster ID is refused before sending", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusOK, []byte(`[]`)))
		service, limiter := newTestService(t, fake)
		_, err := service.ListWorkers(context.Background(), "")
		g.Expect(err).To(MatchError(ErrInvalidRequest))
		g.Expect(fake.received()).To(BeEmpty())
		g.Expect(limiter.count()).To(BeZero())
	})
}

func TestReplaceWorker(t *testing.T) {
	workerID := "kube-" + testIKSClusterID + "-exampletest-example-0000015f"
	noContent := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }

	for _, update := range []bool{true, false} {
		t.Run(fmt.Sprintf("posts {cluster, update: %t, workerID} and accepts 204", update), func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, noContent)
			service, _ := newTestService(t, fake)
			g.Expect(service.ReplaceWorker(context.Background(), testIKSClusterID, workerID, update)).To(Succeed())
			request := fake.last(t)
			g.Expect(request.Method).To(Equal(http.MethodPost))
			g.Expect(request.Path).To(Equal("/global/v2/vpc/replaceWorker"))
			g.Expect(request.Header.Get("Authorization")).To(Equal("Bearer " + testToken))
			g.Expect(string(request.Body)).To(MatchJSON(fmt.Sprintf(`{"cluster": %q, "update": %t, "workerID": %q}`, testIKSClusterID, update, workerID)))
		})
	}

	t.Run("404 is NotFound", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusNotFound, []byte(`{"code":"G0005","description":"synthesized: worker not found"}`)))
		service, _ := newTestService(t, fake)
		err := service.ReplaceWorker(context.Background(), testIKSClusterID, workerID, true)
		g.Expect(Classify(OpReplaceWorker, err)).To(Equal(ClassNotFound))
	})

	t.Run("a POST answered 500 is sent exactly once", func(t *testing.T) {
		g := NewWithT(t)
		fake := newFakeIKS(t, reply(http.StatusInternalServerError, nil))
		service, _ := newTestService(t, fake)
		err := service.ReplaceWorker(context.Background(), testIKSClusterID, workerID, true)
		g.Expect(Classify(OpReplaceWorker, err)).To(Equal(ClassTransient))
		g.Expect(fake.received()).To(HaveLen(1))
	})

	refused := []struct {
		name      string
		clusterID string
		workerID  string
		message   string
	}{
		{name: "empty cluster ID", clusterID: "", workerID: workerID, message: "ReplaceWorker: the cluster ID is empty"},
		{name: "empty worker ID", clusterID: testIKSClusterID, workerID: "", message: "ReplaceWorker: the worker ID is empty"},
	}
	for _, tc := range refused {
		t.Run("refused before sending: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			fake := newFakeIKS(t, noContent)
			service, limiter := newTestService(t, fake)
			err := service.ReplaceWorker(context.Background(), tc.clusterID, tc.workerID, true)
			g.Expect(err).To(MatchError(ErrInvalidRequest))
			g.Expect(err.Error()).To(ContainSubstring(tc.message))
			g.Expect(fake.received()).To(BeEmpty())
			g.Expect(limiter.count()).To(BeZero())
		})
	}
}
