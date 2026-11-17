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
	"slices"
	"syscall"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"

	. "github.com/onsi/gomega"
)

// testControlPlane returns a control plane that IBM runs as cluster c1 at version.
func testControlPlane(version string) *controlplanev1.IBMCloudManagedControlPlane {
	return &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "c-control-plane"},
		Status:     controlplanev1.IBMCloudManagedControlPlaneStatus{ClusterID: "c1", DistributionVersion: version},
	}
}

// testPool returns the IBMCloudManagedMachinePool name of IBM pool poolID with the provider IDs.
func testPool(name, poolID string, providerIDs ...string) infrav1.IBMCloudManagedMachinePool {
	return infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       infrav1.IBMCloudManagedMachinePoolSpec{ProviderIDList: providerIDs},
		Status:     infrav1.IBMCloudManagedMachinePoolStatus{PoolID: poolID},
	}
}

// testWorker returns IBM worker id of pool poolID in state.
func testWorker(id, poolID, state string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: poolID, ActualState: state}
}

func TestCollectOwnershipFacts(t *testing.T) {
	g := NewWithT(t)
	pools := []infrav1.IBMCloudManagedMachinePool{
		testPool("c-extra", "p2", "ibm://a///c1/w5", "ibm://a///c1/w4"),
		testPool("c-default", "p1", "ibm://a///c1/w2", "ibm://a///c1/w1"),
	}
	workers := []kubernetesservice.Worker{
		testWorker("w5", "p2", "deployed"),
		testWorker("w4", "p2", "deployed"),
		testWorker("w3", "p2", "deleted"),
		testWorker("w2", "p1", "deployed"),
		testWorker("w1", "p1", "deleting"),
		// A worker of a pool deleted earlier, which has no IBMCloudManagedMachinePool any more.
		testWorker("w0", "p0", "deleting"),
	}
	facts, err := CollectOwnershipFacts(testControlPlane("1.36.5_1537"), pools, workers)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(facts).To(Equal(OwnershipFacts{
		ClusterID:   "c1",
		MasterMinor: "1.36",
		Pools: map[string]PoolFacts{
			"c-default": {PoolID: "p1", ProviderIDs: []string{"ibm://a///c1/w1", "ibm://a///c1/w2"}, Workers: []string{"w1", "w2"}},
			"c-extra":   {PoolID: "p2", ProviderIDs: []string{"ibm://a///c1/w4", "ibm://a///c1/w5"}, Workers: []string{"w4", "w5"}},
		},
	}))
}

// TestOwnershipFactsIgnoreIBMMasterPatches is the reproducer of a move step that failed although the move changed
// nothing: IBM applied a master patch between the facts taken on the source and those taken on the target.
func TestOwnershipFactsIgnoreIBMMasterPatches(t *testing.T) {
	g := NewWithT(t)
	pools := []infrav1.IBMCloudManagedMachinePool{testPool("c-default", "p1", "ibm://a///c1/w1")}
	workers := []kubernetesservice.Worker{testWorker("w1", "p1", "deployed")}
	before, err := CollectOwnershipFacts(testControlPlane("1.36.5_1537"), pools, workers)
	g.Expect(err).NotTo(HaveOccurred())

	patched, err := CollectOwnershipFacts(testControlPlane("1.36.6_1540"), pools, workers)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(patched).To(Equal(before), "a master patch that IBM applies on its own is no write by the move")

	upgraded, err := CollectOwnershipFacts(testControlPlane("1.37.0_1541"), pools, workers)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(upgraded).NotTo(Equal(before), "a minor change is a write")
}

// TestOwnershipFactsSeeIBMWorkerChangesBeforeCAPIBM shows why the facts carry IBM's own worker list: a replacement or
// a resize that IBM has started shows there while CAPIBM's providerIDList, copied by the move, still holds the old IDs.
func TestOwnershipFactsSeeIBMWorkerChangesBeforeCAPIBM(t *testing.T) {
	cp := testControlPlane("1.36.5_1537")
	pools := []infrav1.IBMCloudManagedMachinePool{testPool("c-default", "p1", "ibm://a///c1/w1", "ibm://a///c1/w2")}
	steady := []kubernetesservice.Worker{testWorker("w1", "p1", "deployed"), testWorker("w2", "p1", "deployed")}
	testCases := []struct {
		name    string
		workers []kubernetesservice.Worker
	}{
		{name: "replacement listed", workers: slices.Concat(steady, []kubernetesservice.Worker{testWorker("w3", "p1", "provisioning")})},
		{name: "worker deleted", workers: []kubernetesservice.Worker{testWorker("w1", "p1", "deployed"), testWorker("w2", "p1", "deleted")}},
		{name: "worker missing", workers: steady[:1]},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			before, err := CollectOwnershipFacts(cp, pools, steady)
			g.Expect(err).NotTo(HaveOccurred())
			after, err := CollectOwnershipFacts(cp, pools, tc.workers)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(after).NotTo(Equal(before))
		})
	}
}

// TestOwnershipFactsCompareEmptyListsEqually guards the canonical form: a pool without provider IDs or live workers
// must give the same facts however its empty lists were built, or two identical states would compare unequal.
func TestOwnershipFactsCompareEmptyListsEqually(t *testing.T) {
	g := NewWithT(t)
	cp := testControlPlane("1.36.5_1537")
	withNil := []infrav1.IBMCloudManagedMachinePool{testPool("c-default", "p1")}
	withEmpty := []infrav1.IBMCloudManagedMachinePool{testPool("c-default", "p1")}
	withEmpty[0].Spec.ProviderIDList = []string{}
	a, err := CollectOwnershipFacts(cp, withNil, nil)
	g.Expect(err).NotTo(HaveOccurred())
	b, err := CollectOwnershipFacts(cp, withEmpty, []kubernetesservice.Worker{testWorker("w1", "p1", "deleted")})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(a).To(Equal(b))
}

func TestCollectOwnershipFactsRejectsIncompleteInput(t *testing.T) {
	pool := testPool("c-default", "p1", "ibm://a///c1/w1")
	testCases := []struct {
		name    string
		cp      *controlplanev1.IBMCloudManagedControlPlane
		pools   []infrav1.IBMCloudManagedMachinePool
		wantErr string
	}{
		{name: "no control plane", pools: []infrav1.IBMCloudManagedMachinePool{pool},
			wantErr: "no control plane: pass the cluster's IBMCloudManagedControlPlane"},
		{name: "no cluster ID", cp: &controlplanev1.IBMCloudManagedControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "c-control-plane"}},
			pools: []infrav1.IBMCloudManagedMachinePool{pool}, wantErr: "control plane c-control-plane has no status.clusterID yet"},
		{name: "no version", cp: testControlPlane(""), pools: []infrav1.IBMCloudManagedMachinePool{pool},
			wantErr: "control plane c-control-plane status.distributionVersion"},
		{name: "no pool", cp: testControlPlane("1.36.5_1537"), wantErr: "no IBMCloudManagedMachinePool"},
		{name: "no pool ID", cp: testControlPlane("1.36.5_1537"),
			pools:   []infrav1.IBMCloudManagedMachinePool{pool, testPool("c-extra", "")},
			wantErr: "pool c-extra has no status.poolID yet"},
		{name: "pool listed twice", cp: testControlPlane("1.36.5_1537"),
			pools: []infrav1.IBMCloudManagedMachinePool{pool, pool}, wantErr: "pool c-default is listed twice"},
		{name: "shared pool ID", cp: testControlPlane("1.36.5_1537"),
			pools:   []infrav1.IBMCloudManagedMachinePool{pool, testPool("c-extra", "p1")},
			wantErr: "pools c-default and c-extra have the same status.poolID p1"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			_, err := CollectOwnershipFacts(tc.cp, tc.pools, nil)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

// testListingError returns the error that reading the facts of cluster c1 gives when IBM's worker listing answers with
// status and code.
func testListingError(status int, code string) error {
	return IBMWorkerListingError("c1", &kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkers,
		StatusCode: status, Code: code})
}

func TestIBMWorkerListingError(t *testing.T) {
	g := NewWithT(t)
	g.Expect(IBMWorkerListingError("c1", nil)).To(Succeed())

	apiErr := &kubernetesservice.APIError{Operation: kubernetesservice.OpListWorkers, StatusCode: http.StatusBadGateway}
	err := IBMWorkerListingError("c1", apiErr)
	g.Expect(err).To(MatchError("listing the workers of IBM cluster c1: IBM Cloud Kubernetes Service ListWorkers failed with HTTP 502"))
	g.Expect(err).To(MatchError(ErrIBMWorkerListing))
	var got *kubernetesservice.APIError
	g.Expect(errors.As(err, &got)).To(BeTrue(), "Classify reads IBM's answer through the mark")
	g.Expect(got).To(BeIdenticalTo(apiErr))
}

// TestOwnershipFactsMayPass is the reproducer of a move step that failed although the move changed nothing: the hold
// after the re-adoption read IBM's worker list once per poll, CAPIBM's client does not retry, and Consistently failed
// the hold on the first 5xx. The hold now waits out every answer that CAPIBM's controllers requeue on, and still fails
// on IBM's final answers and on everything that is not IBM's answer.
func TestOwnershipFactsMayPass(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "no error", err: nil},
		// Classify calls any error without an IBM answer transient, so only the listing's own errors may count as IBM's.
		{name: "management cluster error", err: errors.New("dial tcp 127.0.0.1:6443: connect: connection refused")},
		{name: "facts that cannot be built", err: errors.New("pool c-extra has no status.poolID yet")},
		{name: "IBM 500", err: testListingError(http.StatusInternalServerError, ""), want: true},
		{name: "IBM 503", err: testListingError(http.StatusServiceUnavailable, ""), want: true},
		{name: "IBM internal error code", err: testListingError(http.StatusBadRequest, "E0004"), want: true},
		{name: "IBM rate limit", err: testListingError(http.StatusTooManyRequests, ""), want: true},
		{name: "IBM unclassified status", err: testListingError(http.StatusTeapot, ""), want: true},
		{name: "no answer from IBM", want: true,
			err: IBMWorkerListingError("c1", fmt.Errorf("kubernetesservice: ListWorkers: %w", syscall.ECONNREFUSED))},
		{name: "no answer in time", err: IBMWorkerListingError("c1", context.DeadlineExceeded), want: true},
		{name: "marked error wrapped again", err: fmt.Errorf("move: %w", testListingError(http.StatusGatewayTimeout, "")),
			want: true},
		{name: "cluster gone", err: testListingError(http.StatusNotFound, "")},
		{name: "credentials rejected by IBM", err: testListingError(http.StatusUnauthorized, "")},
		{name: "permission missing", err: testListingError(http.StatusForbidden, "")},
		{name: "malformed request", err: testListingError(http.StatusBadRequest, "")},
		{name: "request refused before sending",
			err: IBMWorkerListingError("c1", fmt.Errorf("%w: ListWorkers: clusterID is empty", kubernetesservice.ErrInvalidRequest))},
		{name: "credentials rejected by IAM", err: IBMWorkerListingError("c1", kubernetesservice.ErrCredentialsRejected)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(OwnershipFactsMayPass(tc.err)).To(Equal(tc.want))
		})
	}
}
