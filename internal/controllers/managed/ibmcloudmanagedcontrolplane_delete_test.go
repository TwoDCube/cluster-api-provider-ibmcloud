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
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"

	. "github.com/onsi/gomega"
)

// The delete tests run the control plane's delete flow through Reconcile with the fake-client fixture of
// ibmcloudmanagedcontrolplane_controller_test.go: gomock fails a test on every IBM call it does not expect, so "zero
// IBM calls" and "DeleteCluster never called" are checked by the absence of an expectation. An envtest test at the
// end runs one deletion against the real API server.

// newDeletingCPObjects returns the objects of a deleting control plane bound to cpIKSClusterID.
func newDeletingCPObjects() *cpObjects {
	objs := newCPObjects()
	objs.cp.Status.ClusterID = cpIKSClusterID
	objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart.Add(-time.Minute)))
	return objs
}

// expectGetByID expects one GetCluster by cpIKSClusterID; c nil answers 404.
func (f *cpFixture) expectGetByID(c *kubernetesservice.Cluster) *gomock.Call {
	call := f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "")
	if c == nil {
		return call.Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
	}
	return call.Return(c, nil)
}

// expectDelete expects one DeleteCluster of cpIKSClusterID in the test resource group that keeps the persistent storage,
// as every DELETE of a control plane without spec.deletion must (newCPObjects sets none); see expectDeleteWith.
func (f *cpFixture) expectDelete(err error) *gomock.Call {
	return f.expectDeleteWith(kubernetesservice.RetainPersistentStorage, err)
}

// expectDeleteWith expects one DeleteCluster of cpIKSClusterID in the test resource group with the persistent storage
// choice storage; before it answers err, it checks that status.deletionRequestTime is already on record (the time is
// persisted before the DELETE).
func (f *cpFixture) expectDeleteWith(storage kubernetesservice.PersistentStorage, err error) *gomock.Call {
	return f.kube.EXPECT().DeleteCluster(gomock.Any(), cpIKSClusterID, testResourceGroupID, storage).DoAndReturn(
		func(context.Context, string, string, kubernetesservice.PersistentStorage) error {
			if cp := f.controlPlane(); cp == nil || cp.Status.DeletionRequestTime.IsZero() {
				f.t.Errorf("DeleteCluster was called before status.deletionRequestTime was persisted")
			}
			return err
		})
}

// expectDeleting checks the persisted control plane: still there (the finalizer kept), Deleting True with reason, and
// Available False with Deleting.
func expectDeleting(g Gomega, f *cpFixture, reason string) *controlplanev1.IBMCloudManagedControlPlane {
	cp := f.controlPlane()
	g.ExpectWithOffset(1, cp).NotTo(BeNil(), "the finalizer is kept")
	g.ExpectWithOffset(1, cp.Finalizers).To(ContainElement(controlplanev1.IBMCloudManagedControlPlaneFinalizer))
	expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue, reason)
	expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionFalse,
		controlplanev1.IBMCloudManagedControlPlaneAvailableDeletingReason)
	return cp
}

func TestControlPlaneDeleteKnownCluster(t *testing.T) {
	t.Run("the holder on delete is the IBMCloudManagedCluster", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDelete(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectHolderIsInfraCluster(g, f)
	})

	t.Run("a deleting Cluster calls DeleteCluster once, then waits for 404", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart.Add(-2 * time.Minute)))
		objs.cluster.Finalizers = []string{clusterv1.ClusterFinalizer}
		f := newCPFixture(t, objs)
		gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDelete(nil).Times(1))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		cp := expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
		g.Expect(cp.Status.DeletionRequestTime.Time).To(BeTemporally("==", cpTestStart))
		expectHolderIsInfraCluster(g, f)

		// IBM deletes: no second DELETE.
		f.clock.SetTime(cpTestStart.Add(2 * time.Minute))
		f.expectGetByID(cpCluster("deleting"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason)

		// 404 about five minutes after the DELETE, as observed on IBM Cloud: the leftovers are named once and the finalizer
		// goes.
		f.clock.SetTime(cpTestStart.Add(6 * time.Minute))
		f.expectGetByID(nil)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed")
		events := f.events()
		g.Expect(events).To(HaveLen(1))
		g.Expect(events[0]).To(HavePrefix("Warning LeftoverResources IBM cluster iksclusterid00000001 is deleted. CAPIBM deletes nothing in VPC r006-test-vpc"))
	})

	// spec.deletion.persistentStorage decides what IBM's DELETE asks for, and the LeftoverResources event names what the
	// DELETE asked for, never the other choice.
	storageCases := []struct {
		name        string
		policy      controlplanev1.PersistentStoragePolicy
		wantStorage kubernetesservice.PersistentStorage
		wantEvent   string
		wantNot     string
	}{
		{name: "an omitted persistentStorage asks IBM to keep the storage", wantStorage: kubernetesservice.RetainPersistentStorage,
			wantEvent: "CAPIBM's last DELETE asked IBM to keep the cluster's persistent storage (spec.deletion.persistentStorage " +
				"Retain, also when omitted; deleteResources=false)", wantNot: "asked IBM to delete"},
		{name: "persistentStorage Retain asks IBM to keep the storage", policy: controlplanev1.PersistentStorageRetain,
			wantStorage: kubernetesservice.RetainPersistentStorage,
			wantEvent: "CAPIBM's last DELETE asked IBM to keep the cluster's persistent storage (spec.deletion.persistentStorage " +
				"Retain, also when omitted; deleteResources=false)", wantNot: "asked IBM to delete"},
		{name: "persistentStorage Delete asks IBM to delete the storage", policy: controlplanev1.PersistentStorageDelete,
			wantStorage: kubernetesservice.DeletePersistentStorage,
			wantEvent: "CAPIBM's last DELETE asked IBM to delete the cluster's persistent storage (spec.deletion.persistentStorage " +
				"Delete, deleteResources=true), but VPC Block volumes may remain", wantNot: "asked IBM to keep"},
	}
	for _, tc := range storageCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newDeletingCPObjects()
			objs.cp.Spec.Deletion.PersistentStorage = tc.policy
			f := newCPFixture(t, objs)
			gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDeleteWith(tc.wantStorage, nil).Times(1))

			_, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
			f.clock.SetTime(cpTestStart.Add(6 * time.Minute))
			f.expectGetByID(nil)
			_, err = f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed")
			events := f.events()
			g.Expect(events).To(HaveLen(1))
			g.Expect(events[0]).To(HavePrefix("Warning LeftoverResources IBM cluster iksclusterid00000001 is deleted."))
			g.Expect(events[0]).To(ContainSubstring(tc.wantEvent))
			g.Expect(events[0]).NotTo(ContainSubstring(tc.wantNot))
		})
	}

	// status.deletionRequestTime is recorded before every DELETE and kept when IBM refuses it or the call fails, so it
	// proves only that CAPIBM sent a DELETE. When IBM refused that DELETE and the cluster then disappears, someone else
	// deleted it: the event names what CAPIBM's DELETE asked for, says that whoever deleted the cluster decided if that
	// DELETE failed, and claims no outcome for either choice.
	refusedCases := []struct {
		name        string
		policy      controlplanev1.PersistentStoragePolicy
		wantStorage kubernetesservice.PersistentStorage
		answer      error
		wantErr     bool
		wantEvent   string
	}{
		{name: "Delete refused with 403", policy: controlplanev1.PersistentStorageDelete,
			wantStorage: kubernetesservice.DeletePersistentStorage,
			answer:      ibmError(kubernetesservice.OpDeleteCluster, http.StatusForbidden, ""),
			wantEvent:   "CAPIBM's last DELETE asked IBM to delete the cluster's persistent storage"},
		{name: "Retain refused with 403", policy: controlplanev1.PersistentStorageRetain,
			wantStorage: kubernetesservice.RetainPersistentStorage,
			answer:      ibmError(kubernetesservice.OpDeleteCluster, http.StatusForbidden, ""),
			wantEvent:   "CAPIBM's last DELETE asked IBM to keep the cluster's persistent storage"},
		{name: "Delete answered 409 while a deletion elsewhere runs", policy: controlplanev1.PersistentStorageDelete,
			wantStorage: kubernetesservice.DeletePersistentStorage,
			answer:      ibmError(kubernetesservice.OpDeleteCluster, http.StatusConflict, ""),
			wantEvent:   "CAPIBM's last DELETE asked IBM to delete the cluster's persistent storage"},
		{name: "omitted, the DELETE failing with 500", wantStorage: kubernetesservice.RetainPersistentStorage,
			answer:    ibmError(kubernetesservice.OpDeleteCluster, http.StatusInternalServerError, ""),
			wantErr:   true,
			wantEvent: "CAPIBM's last DELETE asked IBM to keep the cluster's persistent storage"},
	}
	for _, tc := range refusedCases {
		t.Run("a failed DELETE: "+tc.name, func(t *testing.T) {
			g := NewWithT(t)
			objs := newDeletingCPObjects()
			objs.cp.Spec.Deletion.PersistentStorage = tc.policy
			f := newCPFixture(t, objs)
			gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDeleteWith(tc.wantStorage, tc.answer).Times(1))

			_, err := f.reconcile()

			if tc.wantErr {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}
			g.Expect(f.controlPlane().Status.DeletionRequestTime.IsZero()).To(BeFalse(), "the request time stays after a failed DELETE")

			// The cluster is deleted elsewhere, for example in the IBM Cloud console, before CAPIBM sends the DELETE again.
			f.clock.SetTime(cpTestStart.Add(6 * time.Minute))
			f.expectGetByID(nil)
			_, err = f.reconcile()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed")
			events := f.events()
			g.Expect(events).To(HaveLen(1))
			g.Expect(events[0]).To(HavePrefix("Warning LeftoverResources IBM cluster iksclusterid00000001 is deleted."))
			g.Expect(events[0]).To(ContainSubstring(tc.wantEvent))
			g.Expect(events[0]).To(HaveSuffix(". If that DELETE failed, or the cluster was deleted elsewhere, whoever deleted " +
				"it decided what happened to the storage of its persistent volumes, such as VPC Block volumes"))
			for _, claim := range []string{"Deleted by IBM", "Kept by design", "deleted by IBM", "kept by design"} {
				g.Expect(events[0]).NotTo(ContainSubstring(claim), "a failed DELETE proves no outcome")
			}
		})
	}

	t.Run("a reissued DELETE carries persistentStorage as it is when it is sent", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDeleteWith(kubernetesservice.RetainPersistentStorage, nil))
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())

		// The user asks for Delete while IBM still reports the cluster normal; CAPIBM reads the field again when it sends
		// the DELETE again after DeleteReissueAfter.
		cp := f.controlPlane()
		cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
		g.Expect(f.client.Update(context.Background(), cp)).To(Succeed())
		f.clock.SetTime(cpTestStart.Add(11 * time.Minute))
		gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDeleteWith(kubernetesservice.DeletePersistentStorage, nil))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
	})

	t.Run("reissue only after the delay, and 409 is never terminal", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		deletes := 0
		gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDelete(nil).Do(func(context.Context, string, string, kubernetesservice.PersistentStorage) { deletes++ }))
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)

		// Five minutes later IBM still reports normal: no DELETE within DeleteReissueAfter.
		f.clock.SetTime(cpTestStart.Add(5 * time.Minute))
		f.expectGetByID(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)

		// After the delay the DELETE is sent again; IBM answers 409, which is a wait, never terminal.
		f.clock.SetTime(cpTestStart.Add(11 * time.Minute))
		gomock.InOrder(f.expectGetByID(cpCluster("normal")),
			f.expectDelete(ibmError(kubernetesservice.OpDeleteCluster, http.StatusConflict, "")).Do(func(context.Context, string, string, kubernetesservice.PersistentStorage) { deletes++ }))
		result, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		cp := expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason)
		g.Expect(cp.Status.DeletionRequestTime.Time).To(BeTemporally("==", cpTestStart.Add(11*time.Minute)))

		// The reason changed, but the delay is keyed on status.deletionRequestTime: no DELETE four minutes later.
		f.clock.SetTime(cpTestStart.Add(15 * time.Minute))
		f.expectGetByID(cpCluster("normal"))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(deletes).To(Equal(2))
	})

	deleteAnswers := []struct {
		name        string
		err         error
		wantReason  string
		wantErr     bool
		wantRequeue time.Duration
		wantMessage string
	}{
		{name: "a 400 is waiting for IBM", err: ibmError(kubernetesservice.OpDeleteCluster, http.StatusBadRequest, "E1234"),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason, wantRequeue: time.Minute,
			wantMessage: "IBM answered the deletion of IBM cluster dev (ID iksclusterid00000001) with HTTP 400 (IBM code E1234), " +
				"most likely because it already deletes the cluster or runs another operation on it; CAPIBM checks again"},
		{name: "a 404 lets the next check decide", err: ibmError(kubernetesservice.OpDeleteCluster, http.StatusNotFound, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason, wantRequeue: time.Minute},
		{name: "a 403 is permission denied and rechecked", err: ibmError(kubernetesservice.OpDeleteCluster, http.StatusForbidden, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason, wantRequeue: 15 * time.Minute,
			wantMessage: "IBM Cloud refused deleting IBM cluster dev (ID iksclusterid00000001) with HTTP 403: " +
				"the credentials of the CAPIBM controller identity need the Administrator platform role and the Manager service " +
				"role of Kubernetes Service on resource group 0123456789abcdef0123456789abcdef; the IAM page of the managed " +
				"Kubernetes book lists every role"},
		{name: "a 500 is retried with backoff", err: ibmError(kubernetesservice.OpDeleteCluster, http.StatusInternalServerError, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason, wantErr: true},
		{name: "a 429 backs off", err: ibmError(kubernetesservice.OpDeleteCluster, http.StatusTooManyRequests, ""),
			wantReason: controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason, wantRequeue: 31 * time.Second},
	}
	for _, tc := range deleteAnswers {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			f := newCPFixture(t, newDeletingCPObjects())
			gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDelete(tc.err).Times(1))

			result, err := f.reconcile()

			if tc.wantErr {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				expectRequeue(g, result, tc.wantRequeue)
			}
			cp := expectDeleting(g, f, tc.wantReason)
			g.Expect(cp.Status.DeletionRequestTime.IsZero()).To(BeFalse(), "the request time stays, so the reissue waits for the delay")
			if tc.wantMessage != "" {
				condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue, tc.wantReason)
				g.Expect(condition.Message).To(Equal(tc.wantMessage))
				expectNoIdentityDetails(g, condition.Message)
			}
		})
	}

	t.Run("delete_failed is never deleted again", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		f.expectGetByID(cpCluster("delete_failed"))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeleteFailedReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingDeleteFailedReason)
		g.Expect(condition.Message).To(ContainSubstring("open an IBM support case"))
	})

	t.Run("no DELETE without the request time on record", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		f.failStatusPatch = func(body string) bool { return strings.Contains(body, `"deletionRequestTime"`) }
		f.expectGetByID(cpCluster("normal")) // no DeleteCluster expectation: gomock fails the test if it is called

		_, err := f.reconcile()

		g.Expect(err).To(MatchError(ContainSubstring("recording the deletion request time")))
		g.Expect(f.controlPlane().Status.DeletionRequestTime.IsZero()).To(BeTrue())
	})

	t.Run("row 6 in delete mode deletes our own cluster whose create response was lost", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: cpTestName, InFlightAttemptTime: metav1.NewTime(cpTestStart.Add(-time.Second))}
		f := newCPFixture(t, objs)
		gomock.InOrder(f.expectLookupByName(cpCluster("deploying")), f.expectDelete(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID))
	})
}

func TestControlPlaneDeleteGuards(t *testing.T) {
	t.Run("a MachinePool of the Cluster blocks the delete", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		withInitialPool(objs)
		f := newCPFixture(t, objs) // no IBM expectation: zero IBM calls

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason)
		g.Expect(condition.Message).To(Equal("Waiting for MachinePool/dev-default of Cluster dev to be deleted: deleting the IBM " +
			"cluster deletes every worker pool, so it is deleted only after them; delete Cluster dev to delete everything in order"))
		g.Expect(f.resolver.Holders()).To(BeEmpty())
	})

	t.Run("an owned pool with the finalizer blocks the delete", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		withInitialPool(objs)
		objs.extra = objs.extra[1:] // the MachinePool is gone, the pool still holds its finalizer
		objs.extra[0].SetFinalizers([]string{infrav1.IBMCloudManagedMachinePoolFinalizer})
		f := newCPFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason)
		g.Expect(condition.Message).To(HavePrefix("Waiting for IBMCloudManagedMachinePool/dev-default of Cluster dev"))
	})

	t.Run("an unowned pool without the finalizer does not block", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		withInitialPool(objs)
		objs.extra = objs.extra[1:]
		objs.extra[0].SetOwnerReferences(nil)
		f := newCPFixture(t, objs)
		gomock.InOrder(f.expectGetByID(cpCluster("normal")), f.expectDelete(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
	})

	t.Run("many pools are named up to a bound", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		for _, name := range []string{"a", "b", "c", "d", "e", "f", "g"} {
			objs.extra = append(objs.extra, &clusterv1.MachinePool{ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: name,
				Labels: map[string]string{clusterv1.ClusterNameLabel: cpTestName}}})
		}
		f := newCPFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
			metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForWorkersDeletionReason)
		g.Expect(condition.Message).To(HavePrefix("Waiting for MachinePool/a, MachinePool/b, MachinePool/c, MachinePool/d, MachinePool/e and 2 more of Cluster dev"))
	})

	t.Run("a missing IBMCloudManagedCluster keeps the finalizer while an IBM cluster may exist", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cluster.Spec.InfrastructureRef.Name = "gone"
		f := newCPFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForInfrastructureReason)
		g.Expect(condition.Message).To(Equal("IBMCloudManagedCluster/gone is gone; CAPIBM needs its resource group to prove which IBM " +
			"cluster to delete; re-apply it from its manifest, or remove the finalizer " +
			"ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io by hand to orphan the IBM cluster"))
		g.Expect(f.resolver.Holders()).To(BeEmpty(), "zero IBM calls")
	})

	t.Run("a missing IBMCloudManagedCluster with no possible IBM cluster removes the finalizer", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cluster.Spec.InfrastructureRef.Name = "gone"
		f := newCPFixture(t, objs)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
		g.Expect(f.resolver.Holders()).To(BeEmpty(), "zero IBM calls")
	})
}

func TestControlPlaneDeleteBinding(t *testing.T) {
	t.Run("a foreign same-named cluster is named and left alone", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		f := newCPFixture(t, objs)
		f.expectLookupByName(cpCluster("normal")) // no DeleteCluster expectation

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed")
		g.Expect(f.events()).To(Equal([]string{"Warning IBMClusterNotDeleted IBM cluster dev (ID iksclusterid00000001, created " +
			"2026-11-16T03:05:33+0000) was not deleted: CAPIBM cannot prove it created it for this Cluster"}))
	})

	t.Run("a control plane that never reached IBM goes even when IBM does not answer", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		f := newCPFixture(t, objs)
		f.kube.EXPECT().GetCluster(gomock.Any(), cpTestName, testResourceGroupID).
			Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusServiceUnavailable, ""))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
		g.Expect(f.events()).To(BeEmpty())
	})

	t.Run("row 7 with an endpoint and no intent removes the finalizer with a warning", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c999.us-south.containers.cloud.ibm.com", Port: 31000}
		f := newCPFixture(t, objs)
		f.expectLookupByName(cpCluster("normal"))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
		g.Expect(f.events()).To(HaveLen(1))
	})

	// Row 4 through the kubeconfig Secret's cluster-id (status.clusterID lost, for example after a move): that ID is
	// untrusted, so its 404 proves no cluster and CAPIBM claims no deletion; the delete flow's row-4 handling names a
	// same-named IBM cluster instead.
	row4BySecret := func() *cpObjects {
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}
		objs.extra = append(objs.extra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Namespace: cpTestNamespace, Name: cpTestName + "-kubeconfig",
			Annotations: map[string]string{controlplanev1.KubeconfigClusterIDAnnotation: cpIKSClusterID},
		}})
		return objs
	}
	t.Run("row 4 by the Secret's cluster-id names no leftovers", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, row4BySecret())
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil)) // no DeleteCluster expectation

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed (the stored-kubeconfig probe comes before this)")
		g.Expect(f.events()).To(BeEmpty(), "no LeftoverResources for a cluster CAPIBM has no record of")
	})

	t.Run("row 4 by the Secret's cluster-id names a same-named cluster as not deleted", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, row4BySecret())
		other := cpCluster("normal")
		other.ID = "iksclusterid00000002"
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(other))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
		g.Expect(f.events()).To(Equal([]string{"Warning IBMClusterNotDeleted IBM cluster dev (ID iksclusterid00000002, created " +
			"2026-11-16T03:05:33+0000) was not deleted: CAPIBM cannot prove it created it for this Cluster"}))
	})

	t.Run("row 4 by status.clusterID before any DELETE names the leftovers and a same-named cluster", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		// Delete is set, but CAPIBM sent no DELETE, so the event must not claim that CAPIBM asked IBM to delete the storage.
		objs.cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
		f := newCPFixture(t, objs)
		other := cpCluster("normal")
		other.ID = "iksclusterid00000002"
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(other))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
		events := f.events()
		g.Expect(events).To(HaveLen(2))
		g.Expect(events[0]).To(HavePrefix("Warning LeftoverResources IBM cluster iksclusterid00000001 is deleted."))
		g.Expect(events[0]).To(ContainSubstring("CAPIBM sent no DELETE for this cluster, so whoever deleted it decided what " +
			"happened to the storage of its persistent volumes"))
		g.Expect(events[0]).NotTo(ContainSubstring("asked IBM"))
		g.Expect(events[1]).To(HavePrefix("Warning IBMClusterNotDeleted IBM cluster dev (ID iksclusterid00000002"))
	})

	t.Run("row 8 endpoint set and nothing found removes the finalizer", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c999.us-south.containers.cloud.ibm.com", Port: 31000}
		f := newCPFixture(t, objs)
		f.expectLookupByName(nil)

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
	})

	t.Run("an unknown attempt keeps the finalizer for the delete lookup grace", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: cpTestName,
			FirstUnknownOutcomeTime: metav1.NewTime(cpTestStart), LastUnknownOutcomeTime: metav1.NewTime(cpTestStart)}
		f := newCPFixture(t, objs)

		// Ten minutes of 404: the attempt may still show up, so the finalizer stays.
		f.clock.SetTime(cpTestStart.Add(10 * time.Minute))
		f.expectLookupByName(nil)
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason)

		// After DeleteLookupGracePeriod (30 m) there is nothing to delete.
		f.clock.SetTime(cpTestStart.Add(31 * time.Minute))
		f.expectLookupByName(nil)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
	})

	for _, createdDate := range []string{"2026-11-16T03:45:00+0000", "not-a-date"} {
		t.Run("a possible own cluster keeps the finalizer, createdDate "+createdDate, func(t *testing.T) {
			g := NewWithT(t)
			objs := newDeletingCPObjects()
			objs.cp.Status.ClusterID = ""
			objs.cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: cpTestName,
				FirstUnknownOutcomeTime: metav1.NewTime(cpTestStart), LastUnknownOutcomeTime: metav1.NewTime(cpTestStart)}
			f := newCPFixture(t, objs)
			found := cpCluster("normal")
			found.CreatedDate = createdDate
			f.expectLookupByName(found) // no DeleteCluster expectation

			result, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			expectRequeue(g, result, 15*time.Minute)
			expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason)
		})
	}

	t.Run("ownership not proven keeps the finalizer and never deletes", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.ClusterID = ""
		objs.cp.Spec.ControlPlaneEndpoint = controlplanev1.APIEndpoint{Host: "c111.us-south.containers.cloud.ibm.com", Port: 30101}
		objs.extra = append(objs.extra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Namespace: cpTestNamespace, Name: cpTestName + "-kubeconfig",
			Annotations: map[string]string{controlplanev1.KubeconfigClusterIDAnnotation: cpIKSClusterID},
		}})
		f := newCPFixture(t, objs)
		f.expectGetByID(cpCluster("normal"))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason)
		// The Secret's cluster-id decided, so a fresh admin kubeconfig can still prove ownership: the message offers it.
		g.Expect(condition.Message).To(ContainSubstring("put a fresh admin kubeconfig (ibmcloud ks cluster config --admin) into the kubeconfig Secret"))
		g.Expect(condition.Message).To(HaveSuffix("or remove the finalizer ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io by hand to orphan the IBM cluster"))
	})

	t.Run("a binding mismatch keeps the finalizer, never deletes and names only the ways out that work", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		other := cpCluster("normal")
		other.Type = "openshift"
		f.expectGetByID(other)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason)
		// status.clusterID decides this case, not the kubeconfig Secret, so a new Secret is never offered as the fix.
		g.Expect(condition.Message).To(Equal("IBM cluster iksclusterid00000001, named by status.clusterID, does not match this " +
			"control plane: its type is openshift, not kubernetes (spec.distribution Kubernetes); CAPIBM changes nothing on it " +
			"and writes no kubeconfig Secret; CAPIBM never deletes it. Check it in IBM Cloud: if it belongs to this Cluster, " +
			"delete it there, and CAPIBM removes the finalizer once it finds the cluster gone; otherwise remove the finalizer " +
			"ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io by hand, which leaves the IBM cluster alone"))
		g.Expect(condition.Message).NotTo(ContainSubstring("admin kubeconfig"))
	})

	t.Run("after a binding mismatch, deleting the IBM cluster removes the finalizer", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.Conditions = append(objs.cp.Status.Conditions, metav1.Condition{
			Type: controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, Status: metav1.ConditionTrue,
			Reason: controlplanev1.IBMCloudManagedControlPlaneDeletingOwnershipNotProvenReason, LastTransitionTime: metav1.NewTime(cpTestStart),
		})
		f := newCPFixture(t, objs)
		// The operator deleted the mismatched cluster in IBM Cloud: its ID answers 404 and no same-named cluster exists.
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the way out the message names works")
	})

	// IBM accepted the create with 201 (status.clusterID recorded, the attempt still in flight) and the Cluster is
	// deleted at once. While IBM does not list the new ID yet, the finalizer is all that stops the cluster from being
	// orphaned, so the delete flow waits as long as for a lookup by name, not just CreateGracePeriod.
	t.Run("a cluster IBM accepted but does not list yet keeps the finalizer for the delete lookup grace", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		attempt := cpTestStart.Add(-6 * time.Minute)
		objs.cp.Status.CreateIntent = controlplanev1.CreateIntent{ClusterName: cpTestName, InFlightAttemptTime: metav1.NewTime(attempt)}
		f := newCPFixture(t, objs)
		f.expectGetByID(nil) // no DeleteCluster expectation, and no lookup by name: row 3 waits

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason)
		g.Expect(condition.Message).To(Equal("IBM Cloud does not list cluster iksclusterid00000001 yet; CAPIBM sent its create " +
			"request at 2026-11-16T02:59:30Z and waits until 2026-11-16T03:29:30Z for IBM to list it before it counts the cluster as gone"))
		g.Expect(f.events()).To(BeEmpty(), "no deletion is claimed while IBM may still list the cluster")

		// One second before the grace ends it still waits; at its end IBM's 404 counts, and the finalizer goes.
		f.clock.SetTime(attempt.Add(managedscope.DeleteLookupGracePeriod - time.Second))
		f.expectGetByID(nil)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason)

		f.clock.SetTime(attempt.Add(managedscope.DeleteLookupGracePeriod))
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
	})

	t.Run("a 401 on delete rebuilds only that identity", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusUnauthorized, ""))

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 5*time.Minute)
		g.Expect(f.builder.invalidated).To(Equal([]credentials.IdentityKey{credentials.ControllerKey()}))
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingPermissionDeniedReason)
		expectNoIdentityDetails(g, condition.Message)
	})

	t.Run("a transient lookup failure keeps an earlier Deleting reason", func(t *testing.T) {
		g := NewWithT(t)
		objs := newDeletingCPObjects()
		objs.cp.Status.Conditions = append(objs.cp.Status.Conditions, metav1.Condition{
			Type: controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, Status: metav1.ConditionTrue,
			Reason: controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason, Message: "earlier",
			LastTransitionTime: metav1.NewTime(cpTestStart),
		})
		f := newCPFixture(t, objs)
		f.kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusBadGateway, ""))

		_, err := f.reconcile()

		g.Expect(err).To(HaveOccurred())
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
		g.Expect(condition.Message).To(Equal("earlier"))
	})

	t.Run("a resolution error on delete is persisted", func(t *testing.T) {
		g := NewWithT(t)
		f := newCPFixture(t, newDeletingCPObjects())
		f.resolver.err = context.Canceled

		_, err := f.reconcile()

		g.Expect(err).To(HaveOccurred())
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason)
		g.Expect(condition.Message).To(ContainSubstring("cannot resolve the IBM Cloud credentials"))
		g.Expect(f.builder.Identities()).To(BeEmpty())
	})
}

func TestControlPlaneEnvtestDelete(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "cp-delete")
	cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
	infra := createTestInfra(t, namespace, "dev", cluster)
	infra.Status = newCPObjects().infra.Status
	g.Expect(testEnv.Status().Update(ctx, infra)).To(Succeed())
	cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
	key := client.ObjectKeyFromObject(cp)
	cp.Finalizers = []string{controlplanev1.IBMCloudManagedControlPlaneFinalizer}
	// Set through the real API server and its webhooks, persistentStorage Delete must reach IBM's DELETE.
	cp.Spec.Deletion.PersistentStorage = controlplanev1.PersistentStorageDelete
	g.Expect(testEnv.Update(ctx, cp)).To(Succeed())
	cp.Status.ClusterID = cpIKSClusterID
	cp.Status.Conditions = []metav1.Condition{{Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse,
		Reason: clusterv1.NotPausedReason, LastTransitionTime: metav1.NewTime(cpTestStart)}}
	g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())

	// The webhook rejects deleting a bound control plane while its Cluster lives; the Cluster deletion, held by a test
	// finalizer, allows it, as in a real Cluster deletion.
	g.Expect(testEnv.Delete(ctx, getEnvtestControlPlane(t, key))).NotTo(Succeed())
	const holdCluster = "test.cluster.x-k8s.io/hold"
	cluster.Finalizers = []string{holdCluster}
	g.Expect(testEnv.Update(ctx, cluster)).To(Succeed())
	g.Expect(testEnv.Delete(ctx, cluster)).To(Succeed())
	g.Eventually(func() error { return testEnv.Delete(ctx, getEnvtestControlPlane(t, key)) }, 10*time.Second, 100*time.Millisecond).Should(Succeed())
	t.Cleanup(func() {
		held := &clusterv1.Cluster{}
		if err := testEnv.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(cluster), held); err == nil {
			held.Finalizers = nil
			_ = testEnv.Update(ctx, held)
		}
	})

	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	r := newEnvtestControlPlaneReconciler(t)
	resolver := &recordingResolver{}
	r.Resolver, r.ClientBuilder = resolver, &stubClientBuilder{kubernetes: kube}
	gomock.InOrder(
		kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(cpCluster("normal"), nil),
		kube.EXPECT().DeleteCluster(gomock.Any(), cpIKSClusterID, testResourceGroupID, kubernetesservice.DeletePersistentStorage).Return(nil),
	)

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})

	g.Expect(err).NotTo(HaveOccurred())
	persisted := getEnvtestControlPlane(t, key)
	g.Expect(persisted.Status.DeletionRequestTime.IsZero()).To(BeFalse(), "the request time passes the CRD validation")
	expectCondition(g, persisted, controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
		controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
	g.Expect(resolver.Holders()).To(HaveLen(1))
	g.Expect(resolver.Holders()[0].GetName()).To(Equal(infra.Name), "the holder on delete is the IBMCloudManagedCluster")

	// IBM reports the cluster gone: the finalizer goes and the object with it.
	kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(nil, ibmError(kubernetesservice.OpGetCluster, http.StatusNotFound, ""))
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() *controlplanev1.IBMCloudManagedControlPlane { return getEnvtestControlPlane(t, key) },
		10*time.Second, 100*time.Millisecond).Should(BeNil())
}
