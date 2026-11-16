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
	"net/http"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/managedkubeconfig"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// The move tests cover what changes on the target of clusterctl move: the control plane's status is empty, and its
// binding comes back through the kubeconfig Secret's cluster-id annotation and the proof of possession with the moved
// credential (DecideBinding row 2). The proof does not depend on the identity CAPIBM acts as (the moved admin
// certificate names the source's identity), and it needs an inline client certificate (a bearer token is never
// proven). They also cover the delete flow's stored-kubeconfig probe, which keeps the finalizer when IBM answers 404
// for a cluster the moved credential still reaches, and the credential messages, which name the identity only by its
// description. gomock has no GetAdminKubeconfig expectation in any case that is not proven: no credential is fetched
// for an unproven candidate. An envtest test at the end runs the move against the real API server.

// movedSecret returns the CAPIBM-owned kubeconfig Secret that clusterctl move carries to the target: the source's
// kubeconfig of creds (IKS, cluster CA) with its annotations, owned by the control plane.
//
// Parameters:
//   - t is the test.
//   - creds are the credentials of the source.
func movedSecret(t *testing.T, creds kubeconfigCredentials) *corev1.Secret {
	t.Helper()
	value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
	return ownedKubeconfigSecret(t, value, cpTestStart.Add(-time.Hour), nil)
}

// withMovedStatus makes objs the target of clusterctl move: the endpoint is in the moved spec, status is empty, and
// moved is the Secret clusterctl carried along.
//
// Parameters:
//   - objs are the test objects.
//   - moved is the kubeconfig Secret.
func withMovedStatus(objs *cpObjects, moved *corev1.Secret) {
	objs.cp.Status.ClusterID = ""
	objs.cp.Spec.ControlPlaneEndpoint = cpTestEndpoint
	objs.extra = append(objs.extra, moved)
}

// expectCAForProof expects the getCACert call of the proof (IKS: cluster CA trust), answered with ca.
func (f *cpFixture) expectCAForProof(ca *testClusterCA) *gomock.Call {
	return f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: ca.pem}, nil)
}

func TestControlPlaneMoveBinding(t *testing.T) {
	t.Run("moved_secret_that_passes_the_access_review_binds_without_a_create_or_a_secret_write", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { withMovedStatus(objs, movedSecret(t, creds)) })
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		// One lookup by the Secret's cluster-id, the CA of the proof; no create, no ListVersions, no credential fetch.
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.expectCAForProof(creds.ca))
		before := f.kubeconfigSecret()

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID), "row 2: Known through the proof")
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)), "initialization is set again")
		g.Expect(f.kubeconfigSecret()).To(Equal(before), "a valid moved Secret is not rewritten")
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason)
		expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneAvailableReason)
		probes := prober.calls()
		g.Expect(probes).To(HaveLen(2), "the proof, then the stored kubeconfig")
		g.Expect(probes[0].server).To(Equal(cpTestServer), "the proof uses the candidate's master URL")
	})

	t.Run("moved_secret_with_an_expired_credential_is_ownership_not_proven_with_zero_mutations", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { withMovedStatus(objs, movedSecret(t, creds)) })
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnauthorized, "SelfSubjectReview")
		})
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.expectCAForProof(creds.ca))
		before := f.kubeconfigSecret()

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(BeEmpty())
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(BeNil())
		g.Expect(f.kubeconfigSecret()).To(Equal(before), "no Secret write")
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason)
		g.Expect(condition.Message).To(ContainSubstring("(SelfSubjectReview failed (Unauthorized))"))
		g.Expect(condition.Message).To(ContainSubstring("ibmcloud ks cluster config --admin"))
	})

	t.Run("moved_secret_whose_admin_certificate_names_another_identity_is_adopted", func(t *testing.T) {
		g := NewWithT(t)
		ca := newTestClusterCA(t, cpTestStart.AddDate(9, 0, 0))
		const sourceCN = "KubeCert#<synthetic-iam-id-of-the-source-management-cluster>"
		creds := kubeconfigCredentials{ca: ca, admin: ca.issueAdmin(t, sourceCN, cpTestStart.Add(-time.Hour), cpTestStart.AddDate(2, 0, 0))}
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { withMovedStatus(objs, movedSecret(t, creds)) })
		// The API server names the source's identity; CAPIBM acts as another one. Only the access review counts.
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			result := passing("v1.36.5+IKS")
			result.Username = sourceCN
			return result, nil
		})
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.expectCAForProof(ca))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID))
		for _, condition := range cp.Status.Conditions {
			g.Expect(condition.Message).NotTo(ContainSubstring("KubeCert"), condition.Type)
		}
	})

	t.Run("moved_user_provided_secret_with_a_bearer_token_is_ownership_not_proven", func(t *testing.T) {
		g := NewWithT(t)
		userProvided := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: cpTestNamespace, Name: "dev-kubeconfig",
				Labels:      map[string]string{clusterv1.ClusterNameLabel: cpTestName},
				Annotations: map[string]string{controlplanev1.KubeconfigClusterIDAnnotation: cpIKSClusterID}},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"value": tokenKubeconfigValue(t)},
		}
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) { withMovedStatus(objs, userProvided) })
		// The lookup only: no getCACert, no admin kubeconfig, no probe.
		f.expectObserve(cpCluster("normal"))
		before := f.kubeconfigSecret()

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := f.controlPlane()
		g.Expect(cp.Status.ClusterID).To(BeEmpty())
		condition := expectCondition(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition, metav1.ConditionFalse,
			controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason)
		g.Expect(condition.Message).To(ContainSubstring("the proof needs a client-certificate kubeconfig (ibmcloud ks cluster config --admin)"))
		g.Expect(condition.Message).NotTo(ContainSubstring("synthetic-token"))
		g.Expect(prober.calls()).To(BeEmpty())
		g.Expect(f.kubeconfigSecret()).To(Equal(before))
	})

	t.Run("a_moved_control_plane_that_is_deleted_deletes_its_cluster_once_proven", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, prober := newKubeconfigFixture(t, func(objs *cpObjects) {
			withMovedStatus(objs, movedSecret(t, creds))
			objs.cp.DeletionTimestamp = ptr.To(metav1.NewTime(cpTestStart.Add(-time.Minute)))
		})
		prober.setAnswer(alwaysPassing("v1.36.5+IKS"))
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.expectCAForProof(creds.ca), f.expectDelete(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		cp := expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingDeletionRequestedReason)
		g.Expect(cp.Status.ClusterID).To(Equal(cpIKSClusterID))
	})
}

// tokenKubeconfigValue is a kubeconfig whose user holds a bearer token, as a user-provided Secret may. The ownership
// proof accepts only an inline client certificate, so such a Secret never proves ownership.
// t is the test.
func tokenKubeconfigValue(t *testing.T) []byte {
	t.Helper()
	value, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"dev": {Server: cpTestServer}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"dev-admin": {Token: "synthetic-token"}},
		Contexts:       map[string]*clientcmdapi.Context{"dev": {Cluster: "dev", AuthInfo: "dev-admin"}},
		CurrentContext: "dev",
	})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return value
}

func TestControlPlaneDeleteStoredKubeconfigProbe(t *testing.T) {
	// notVisible is the IBMClusterNotVisible message of the test cluster; it names the credentials only by the
	// description of the identity CAPIBM acts as.
	notVisible := "The credentials of the CAPIBM controller identity cannot see IBM cluster iksclusterid00000001 in account " +
		cpTestAccount + ", but its kubeconfig Secret still authenticates against it; check the account and IAM scope of those " +
		"credentials. CAPIBM keeps the finalizer ibmcloudmanagedcontrolplane.controlplane.cluster.x-k8s.io and never deletes a " +
		"cluster it cannot see; remove the finalizer by hand to orphan the IBM cluster"

	for _, tc := range []struct {
		name  string
		setup func(objs *cpObjects, moved *corev1.Secret)
	}{
		{name: "trusted_ID_from_status", setup: func(objs *cpObjects, moved *corev1.Secret) {
			objs.extra = append(objs.extra, moved)
		}},
		{name: "ID_from_the_secret_with_empty_status", setup: func(objs *cpObjects, moved *corev1.Secret) {
			withMovedStatus(objs, moved)
		}},
	} {
		t.Run(tc.name+"_404_while_the_stored_kubeconfig_authenticates_keeps_the_finalizer", func(t *testing.T) {
			g := NewWithT(t)
			creds := newKubeconfigCredentials(t, time.Hour)
			objs := newDeletingCPObjects()
			tc.setup(objs, movedSecret(t, creds))
			f := newCPFixture(t, objs)
			prober := &kubeconfigProber{t: t, answer: alwaysPassing("v1.36.5+IKS")}
			f.r.Prober = prober
			f.expectGetByID(nil) // 404; no DeleteCluster, no lookup by name

			result, err := f.reconcile()

			g.Expect(err).NotTo(HaveOccurred())
			expectRequeue(g, result, 15*time.Minute)
			condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason),
				controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
				controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason)
			g.Expect(condition.Message).To(Equal(notVisible))
			expectNoIdentityDetails(g, condition.Message) // no IAM ID, e-mail or key fingerprint
			g.Expect(f.events()).To(BeEmpty(), "no LeftoverResources for a cluster that may still exist")
			g.Expect(prober.calls()).To(HaveLen(1))
			g.Expect(prober.calls()[0].server).To(Equal(cpTestServer), "the stored kubeconfig against its stored server")
		})
	}

	t.Run("a_kubeconfig_that_no_longer_authenticates_removes_the_finalizer_with_the_warning", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		objs := newDeletingCPObjects()
		objs.extra = append(objs.extra, movedSecret(t, creds))
		f := newCPFixture(t, objs)
		f.r.Prober = &kubeconfigProber{t: t, answer: func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnauthorized, "SelfSubjectReview")
		}}
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed")
		events := f.events()
		g.Expect(events).To(HaveLen(1))
		g.Expect(events[0]).To(HavePrefix("Warning LeftoverResources IBM cluster iksclusterid00000001 is deleted."))
	})

	t.Run("later_poll_404_after_the_DELETE_rechecks_at_the_delete_cadence", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		objs := newDeletingCPObjects()
		objs.cp.Status.DeletionRequestTime = metav1.NewTime(cpTestStart.Add(-6 * time.Minute))
		objs.extra = append(objs.extra, movedSecret(t, creds))
		f := newCPFixture(t, objs)
		prober := &kubeconfigProber{t: t, answer: alwaysPassing("v1.36.5+IKS")}
		f.r.Prober = prober
		f.expectGetByID(nil)

		result, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason),
			controlplanev1.IBMCloudManagedControlPlaneDeletingCondition, metav1.ConditionTrue,
			controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason)
		g.Expect(condition.Message).To(HaveSuffix(". CAPIBM requested its deletion at 2026-11-16T02:59:30Z; this clears once IBM has removed its master"))

		// The master is gone a minute later: the finalizer goes with the leftovers named (no same-named lookup after
		// CAPIBM's own DELETE).
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnreachable, "SelfSubjectReview")
		})
		f.clock.SetTime(cpTestStart.Add(time.Minute))
		f.expectGetByID(nil)
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
		g.Expect(f.events()).To(HaveLen(1))
	})

	// unreachable answers every probe as an API server the management cluster cannot reach.
	unreachable := func(probedKubeconfig) (workloadprobe.Result, error) {
		return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnreachable, "SelfSubjectReview")
	}

	t.Run("an_unreachable_stored_kubeconfig_without_a_DELETE_is_checked_again_for_the_grace_period", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		objs := newDeletingCPObjects() // deleting since 03:04:30, no DELETE sent
		objs.extra = append(objs.extra, movedSecret(t, creds))
		f := newCPFixture(t, objs)
		f.r.Prober = &kubeconfigProber{t: t, answer: unreachable}
		f.expectGetByID(nil) // no DeleteCluster, no lookup by name

		result, err := f.reconcile()

		// A single network blip must not orphan a live cluster that the credentials cannot see.
		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, time.Minute)
		waiting := controlplanev1.IBMCloudManagedControlPlaneDeletingWaitingForIBMDeletionReason
		condition := expectCondition(g, expectDeleting(g, f, waiting), controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
			metav1.ConditionTrue, waiting)
		g.Expect(condition.Message).To(Equal("IBM answers 404 for cluster iksclusterid00000001, and its kubeconfig Secret could not be " +
			"checked against it (SelfSubjectReview failed (Unreachable)), so CAPIBM cannot yet tell a cluster that the credentials of " +
			"the CAPIBM controller identity cannot see from one that is gone; it checks again every minute until " +
			"2026-11-16T03:34:30Z and then counts the cluster as gone"))
		g.Expect(f.events()).To(BeEmpty(), "no LeftoverResources for a cluster that may still exist")

		// The grace period has passed and the master never answered: a cluster deleted out of band, whose finalizer goes as
		// for row 4 proper.
		f.clock.SetTime(cpTestStart.Add(30 * time.Minute))
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil(), "the finalizer is removed")
		g.Expect(f.events()).To(HaveLen(1))
	})

	t.Run("a_kubeconfig_seen_authenticating_keeps_the_finalizer_while_it_cannot_be_checked", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		objs := newDeletingCPObjects()
		objs.extra = append(objs.extra, movedSecret(t, creds))
		f := newCPFixture(t, objs)
		prober := &kubeconfigProber{t: t, answer: alwaysPassing("v1.36.5+IKS")}
		f.r.Prober = prober
		notVisible := controlplanev1.IBMCloudManagedControlPlaneDeletingIBMClusterNotVisibleReason
		f.expectGetByID(nil)
		_, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectDeleting(g, f, notVisible)

		// Long after the grace period, the API server does not answer: the cluster may still run, so the finalizer stays.
		prober.setAnswer(unreachable)
		f.clock.SetTime(cpTestStart.Add(2 * time.Hour))
		f.expectGetByID(nil)
		result, err := f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		expectRequeue(g, result, 15*time.Minute)
		condition := expectCondition(g, expectDeleting(g, f, notVisible), controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
			metav1.ConditionTrue, notVisible)
		g.Expect(condition.Message).To(Equal(managedscope.IBMClusterNotCheckedMessage("the CAPIBM controller identity",
			cpIKSClusterID, cpTestAccount, "SelfSubjectReview failed (Unreachable)")))
		expectNoIdentityDetails(g, condition.Message) // no IAM ID, e-mail or key fingerprint
		g.Expect(f.events()).To(BeEmpty())

		// The kubeconfig is rejected (401): it no longer authenticates, so the cluster counts as gone and the
		// finalizer goes.
		prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) {
			return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnauthorized, "SelfSubjectReview")
		})
		f.clock.SetTime(cpTestStart.Add(2*time.Hour + 15*time.Minute))
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil))
		_, err = f.reconcile()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
	})

	t.Run("a user-provided Secret is not probed and the finalizer goes", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		userProvided := movedSecret(t, creds)
		userProvided.Type = corev1.SecretTypeOpaque
		userProvided.OwnerReferences = nil
		objs := newDeletingCPObjects()
		objs.extra = append(objs.extra, userProvided)
		f := newCPFixture(t, objs) // failIfCalledProber
		gomock.InOrder(f.expectGetByID(nil), f.expectLookupByName(nil))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(f.controlPlane()).To(BeNil())
	})

	t.Run("a_GetCACertificate_403_during_the_proof_names_the_roles_and_the_identity", func(t *testing.T) {
		g := NewWithT(t)
		creds := newKubeconfigCredentials(t, time.Hour)
		f, _ := newKubeconfigFixture(t, func(objs *cpObjects) { withMovedStatus(objs, movedSecret(t, creds)) })
		gomock.InOrder(f.expectObserve(cpCluster("normal")), f.kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).
			Return(nil, ibmError(kubernetesservice.OpGetCACertificate, http.StatusForbidden, "")))

		_, err := f.reconcile()

		g.Expect(err).NotTo(HaveOccurred())
		condition := expectCondition(g, f.controlPlane(), controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
			metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason)
		g.Expect(condition.Message).To(ContainSubstring("the credentials of the CAPIBM controller identity need"))
		expectNoIdentityDetails(g, condition.Message)
	})
}

// TestControlPlaneEnvtestMove runs the target side of clusterctl move against the real API server: the control plane
// arrives with its spec (the endpoint) and without status, the kubeconfig Secret arrives with its annotations and an
// owner reference to the new control plane. One reconcile binds through the proof, sends no create, leaves the valid
// Secret as it is and initializes again. No manager runs; the Cluster is paused at cleanup so that a later envtest
// manager watching every namespace leaves the bound control plane alone.
func TestControlPlaneEnvtestMove(t *testing.T) {
	g := NewWithT(t)
	namespace := newTestNamespace(t, "cp-move")
	cluster := createTestCluster(t, namespace, "dev", managedControlPlaneRef("dev"))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	g.Expect(testEnv.Status().Update(ctx, cluster)).To(Succeed())
	t.Cleanup(func() { pauseTestCluster(t, cluster) })
	infra := createTestInfra(t, namespace, "dev", cluster)
	infra.Status = newCPObjects().infra.Status
	g.Expect(testEnv.Status().Update(ctx, infra)).To(Succeed())
	cp := createEnvtestControlPlane(t, namespace, "dev", cluster)
	key := client.ObjectKeyFromObject(cp)
	cp.Finalizers = []string{controlplanev1.IBMCloudManagedControlPlaneFinalizer}
	cp.Spec.ControlPlaneEndpoint = cpTestEndpoint
	g.Expect(testEnv.Update(ctx, cp)).To(Succeed())
	cp.Status.Conditions = []metav1.Condition{{Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse,
		Reason: clusterv1.NotPausedReason, LastTransitionTime: metav1.NewTime(cpTestStart)}}
	g.Expect(testEnv.Status().Update(ctx, cp)).To(Succeed())

	// clusterctl move creates the Secret with its annotations and an owner reference to the new control plane's UID.
	creds := newKubeconfigCredentials(t, time.Hour)
	value := buildTestKubeconfig(t, cpTestServer, managedkubeconfig.ClusterCA, creds)
	annotations, err := managedscope.KubeconfigSecretAnnotations(managedscope.KubeconfigAnnotationInput{ClusterID: cpIKSClusterID,
		AccountID: cpTestAccount, Info: mustInspect(t, value), FetchedAt: cpTestStart.Add(-time.Hour), CAFetchedAt: cpTestStart.Add(-time.Hour)})
	g.Expect(err).NotTo(HaveOccurred())
	moved := kubeconfig.GenerateSecretWithOwner(client.ObjectKey{Namespace: namespace, Name: "dev"}, value,
		*metav1.NewControllerRef(cp, controlplanev1.GroupVersion.WithKind("IBMCloudManagedControlPlane")))
	moved.Annotations = annotations
	g.Expect(testEnv.Create(ctx, moved)).To(Succeed())
	secretKey := client.ObjectKeyFromObject(moved)
	movedVersion := moved.ResourceVersion

	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	r := newEnvtestControlPlaneReconciler(t)
	r.Resolver, r.ClientBuilder = &recordingResolver{}, &stubClientBuilder{kubernetes: kube}
	r.Prober = &kubeconfigProber{t: t, answer: alwaysPassing("v1.36.5+IKS")}
	c := cpCluster("normal")
	gomock.InOrder(
		kube.EXPECT().GetCluster(gomock.Any(), cpIKSClusterID, "").Return(c, nil),
		kube.EXPECT().GetCACertificate(gomock.Any(), cpIKSClusterID).Return(&kubernetesservice.CACertificate{PEM: creds.ca.pem}, nil),
	)

	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})

	g.Expect(err).NotTo(HaveOccurred())
	persisted := getEnvtestControlPlane(t, key)
	g.Expect(persisted.Status.ClusterID).To(Equal(cpIKSClusterID), "Known through row 2, no create")
	g.Expect(persisted.Status.Initialization.ControlPlaneInitialized).To(Equal(ptr.To(true)))
	g.Expect(persisted.Status.Version).To(Equal("v1.36.5+IKS"))
	g.Expect(conditions.IsTrue(persisted, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition)).To(BeTrue())
	after := &corev1.Secret{}
	g.Expect(testEnv.GetAPIReader().Get(ctx, secretKey, after)).To(Succeed())
	g.Expect(after.ResourceVersion).To(Equal(movedVersion), "the valid moved Secret is not rewritten")
}
