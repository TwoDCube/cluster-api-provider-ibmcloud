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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/secret"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// This file is the control plane scope: it finds the Cluster's IBMCloudManagedCluster, decides which IBM cluster the
// IBMCloudManagedControlPlane stands for (the binding, DecideBinding), and reports what IBM says about that cluster.
// The decisions themselves are pure functions of this package (binding.go, cluster_state.go); the scope only gathers
// their inputs, makes the IBM calls they name and writes their results into status and conditions.
//
// Credentials: every IBM call of a Cluster is made with the identity resolved from the Cluster's
// IBMCloudManagedCluster, the identity holder. The control plane never carries an identity of its own. The scope
// resolves it lazily in ibm, never in its constructor, so that a resolution failure happens after the controller's
// patch helper exists and is persisted as a condition.

// cpInfraClusterKind is the kind a Cluster's spec.infrastructureRef must name for a managed control plane.
const cpInfraClusterKind = "IBMCloudManagedCluster"

// The IAM roles a credential message asks for. They are documented role names only; which identity holds them is
// never recorded, and messages name the identity by its Description only.
const (
	// cpKubernetesServiceRoles is what every Kubernetes Service call of the control plane needs.
	cpKubernetesServiceRoles = "the Administrator platform role and the Manager service role of Kubernetes Service"
	// cpIAMPageHint points to the IAM page of the book, which lists every role the managed kinds need.
	cpIAMPageHint = "the IAM page of the managed Kubernetes book lists every role"
)

// ControlPlaneScope is the scope of one IBMCloudManagedControlPlane reconcile. Create it with NewControlPlaneScope; it
// lives for one reconcile only.
type ControlPlaneScope struct {
	params *ScopeParams

	// Cluster is the owning CAPI Cluster.
	Cluster *clusterv1.Cluster
	// ControlPlane is the object being reconciled; the scope writes its status and conditions, and the controller's
	// patch persists them.
	ControlPlane *controlplanev1.IBMCloudManagedControlPlane
	// InfraCluster is the Cluster's IBMCloudManagedCluster once a step has loaded it (loadInfraCluster); nil before. It
	// is the identity holder of every IBM call and gives the region and the resource group.
	InfraCluster *infrav1.IBMCloudManagedCluster

	// clients are the IBM clients of this reconcile, built by ibm on first use.
	clients *controlPlaneClients
}

// controlPlaneClients are one reconcile's IBM clients for the Cluster's identity.
type controlPlaneClients struct {
	// identity is the resolved identity; credential messages name it by its Description, never by an IAM ID, and a 401
	// invalidates only its authenticator.
	identity credentials.Identity
	// kubernetes is the IBM Cloud Kubernetes Service client of that identity for the InfraCluster's region.
	kubernetes kubernetesservice.KubernetesService
}

// NewControlPlaneScope returns the scope of one IBMCloudManagedControlPlane reconcile. It only stores its inputs: it
// reads no object, resolves no identity, builds no client and makes no call, so that every failure happens in a step,
// after the controller's patch helper exists.
//
// Parameters:
//   - params are the shared dependencies; the scope needs Client, APIReader, Recorder, Resolver, ClientBuilder and Clock.
//   - cluster is the owning CAPI Cluster.
//   - cp is the IBMCloudManagedControlPlane being reconciled.
func NewControlPlaneScope(params *ScopeParams, cluster *clusterv1.Cluster, cp *controlplanev1.IBMCloudManagedControlPlane) (*ControlPlaneScope, error) {
	if err := params.validate("ControlPlaneScope", depClient, depAPIReader, depRecorder, depResolver, depClientBuilder, depClock); err != nil {
		return nil, err
	}
	if cluster == nil {
		return nil, errors.New("managed: ControlPlaneScope needs the owning Cluster, got nil")
	}
	if cp == nil {
		return nil, errors.New("managed: ControlPlaneScope needs the IBMCloudManagedControlPlane, got nil")
	}
	return &ControlPlaneScope{params: params, Cluster: cluster, ControlPlane: cp}, nil
}

// ibm resolves the Cluster's identity with the IBMCloudManagedCluster as the holder and builds its Kubernetes Service
// client for the InfraCluster's region, on first use, memoized for this reconcile only: the InfraCluster is the only
// holder of a Cluster's identity, and a lazy resolution fails after the patch helper exists, so its condition is
// persisted. A client that cannot be built is reported as a *clientBuildError, which keeps the resolved identity for
// the message. ctx is the reconcile context; loadInfraCluster must have found the InfraCluster.
func (s *ControlPlaneScope) ibm(ctx context.Context) (*controlPlaneClients, error) {
	if s.clients != nil {
		return s.clients, nil
	}
	if s.InfraCluster == nil {
		return nil, errors.New("managed: ControlPlaneScope.ibm called before the IBMCloudManagedCluster was loaded (programming error)")
	}
	identity, err := s.params.Resolver.Resolve(ctx, s.InfraCluster)
	if err != nil {
		return nil, fmt.Errorf("resolving the IBM Cloud identity of %s through IBMCloudManagedCluster/%s: %w",
			s.objectName(), s.InfraCluster.Name, err)
	}
	kubernetes, err := s.params.ClientBuilder.KubernetesService(ctx, identity, s.InfraCluster.Spec.Region)
	if err != nil {
		return nil, &clientBuildError{identity: identity, err: fmt.Errorf("building the Kubernetes Service client of %s for region %s: %w",
			identity.Description, s.InfraCluster.Spec.Region, err)}
	}
	s.clients = &controlPlaneClients{identity: identity, kubernetes: kubernetes}
	return s.clients, nil
}

// ReconcileNormal runs the normal flow of the control plane: it marks the control plane as externally managed, waits
// for the Cluster's infrastructure, loads the IBMCloudManagedCluster (the identity holder), derives the IBM cluster
// name, decides the binding and acts on it. Status carries IBM's stable tokens only, never IBM free text. ctx is the
// reconcile context.
func (s *ControlPlaneScope) ReconcileNormal(ctx context.Context) Outcome {
	cp := s.ControlPlane
	cp.Status.ExternalManagedControlPlane = ptr.To(true)
	conditions.Set(cp, metav1.Condition{
		Type:   controlplanev1.IBMCloudManagedControlPlaneDeletingCondition,
		Status: metav1.ConditionFalse,
		Reason: controlplanev1.IBMCloudManagedControlPlaneNotDeletingReason,
	})

	if !ptr.Deref(s.Cluster.Status.Initialization.InfrastructureProvisioned, false) {
		return s.waitForInfrastructure(fmt.Sprintf("Waiting for the infrastructure of Cluster %s (%s) to resolve the VPC and subnets",
			s.Cluster.Name, s.infraRefName()))
	}
	infra, err := s.loadInfraCluster(ctx)
	if err != nil {
		return s.infraClusterProblem(err)
	}
	if infra == nil {
		return s.waitForInfrastructure(fmt.Sprintf("Waiting for %s, named by spec.infrastructureRef of Cluster %s, to exist",
			s.infraRefName(), s.Cluster.Name))
	}

	name, err := EffectiveClusterName(cp.Spec.IBMCloudClusterName, s.Cluster.Name)
	if err != nil {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason, err.Error())
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.clientsProblem(err)
	}
	decision, err := s.decideBinding(ctx, clients, name, false)
	if err != nil {
		return s.callProblem(ctx, err)
	}
	return s.actOnBinding(ctx, decision, name)
}

// actOnBinding carries out a binding decision of the normal flow: it observes a bound cluster, waits for one CAPIBM
// may have created, acts on a Create decision, or reports a decision that needs a human.
//
// Parameters:
//   - ctx is the reconcile context.
//   - decision is DecideBinding's answer.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) actOnBinding(_ context.Context, decision BindingDecision, name string) Outcome {
	switch decision.Action {
	case BindingKnown:
		return s.observe(decision)
	case BindingWait:
		// Rows 3 and 9: a create of CAPIBM's may still show up; no mutation until the window ends.
		s.setProvisioned(metav1.ConditionFalse, decision.Reason, decision.Message)
		s.setAvailableWaiting()
		return NotDone(IntervalClusterProvisioning, decision.Reason)
	case BindingCreate:
		message := fmt.Sprintf("IBM Cloud lists no cluster named %s in resource group %s; this CAPIBM build does not create IBM clusters",
			name, s.InfraCluster.Spec.ResourceGroup.ID)
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason, message)
	case BindingTerminal:
		return s.notProvisioned(decision.Reason, decision.Message)
	default:
		return Transient(fmt.Errorf("managed: binding decision %q (row %s) has no action in the normal flow (programming error)",
			decision.Action, decision.Row))
	}
}

// observe records the bound IBM cluster and reports its state (ClassifyClusterState). status.clusterID is written for
// every Known row; the create intent is cleared only when status.clusterID itself found the cluster (row 1), because
// until then the intent is the only record of an attempt whose response may have been lost.
//
// Parameters:
//   - decision is a BindingKnown decision; its Cluster is the cluster GetCluster returned.
func (s *ControlPlaneScope) observe(decision BindingDecision) Outcome {
	cp := s.ControlPlane
	c := decision.Cluster
	cp.Status.ClusterID = decision.ClusterID
	if decision.Row == "1" {
		cp.Status.CreateIntent = controlplanev1.CreateIntent{}
	}

	label := fmt.Sprintf("IBM cluster %s (ID %s)", c.Name, c.ID)
	switch ClassifyClusterState(c.State) {
	case ClusterStateProvisioning:
		s.setProvisioned(metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			fmt.Sprintf("IBM is creating %s (state %s)", label, c.State))
		s.setAvailableWaiting()
		return NotDone(IntervalClusterProvisioning, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
	case ClusterStateFailed:
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreateFailedReason,
			fmt.Sprintf("IBM reports %s in state %s; CAPIBM never deletes it on its own: delete this Cluster and create it again", label, c.State))
	case ClusterStateGoneOutOfBand:
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedDeletedOutOfBandReason,
			fmt.Sprintf("IBM reports %s in state %s although this Cluster is not being deleted; CAPIBM never recreates it: "+
				"delete this Cluster", label, c.State))
	default:
		message := ""
		if !KnownClusterState(c.State) {
			// Forward compatible: a new IBM state never stops a working cluster, but it is named.
			message = fmt.Sprintf("unknown IBM state %s", c.State)
		}
		s.setProvisioned(metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason, message)
		return Done()
	}
}

// decideBinding gathers the inputs of DecideBinding, makes the one GetCluster call it names and returns its decision
// (rows 1-10, in delete mode for the delete flow). The proof of possession is not run in this release of the control
// plane (OwnershipProofNotAttempted), so rows 2 and 5 end terminal instead of binding.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - name is the effective IBM cluster name.
//   - deleting selects the delete mode of DecideBinding.
//
// It returns a *controlPlaneCallError when GetCluster failed with anything but 404, and any other error for a read
// of the kubeconfig Secret that failed or an input DecideBinding rejected.
func (s *ControlPlaneScope) decideBinding(ctx context.Context, clients *controlPlaneClients, name string, deleting bool) (BindingDecision, error) {
	cp := s.ControlPlane
	secretClusterID, err := s.kubeconfigSecretClusterID(ctx)
	if err != nil {
		return BindingDecision{}, err
	}
	in := BindingInput{
		StatusClusterID: cp.Status.ClusterID,
		SecretClusterID: secretClusterID,
		Endpoint:        cp.Spec.ControlPlaneEndpoint,
		Intent:          cp.Status.CreateIntent,
		Name:            name,
		ResourceGroupID: s.InfraCluster.Spec.ResourceGroup.ID,
		Distribution:    cp.Spec.Distribution,
		Proof:           OwnershipProofNotAttempted,
		Now:             s.params.Clock.Now(),
		Deleting:        deleting,
	}
	lookup := in.Lookup()
	found, err := clients.kubernetes.GetCluster(ctx, lookup.IDOrName, lookup.ResourceGroupID)
	if err != nil {
		if kubernetesservice.Classify(kubernetesservice.OpGetCluster, err) != kubernetesservice.ClassNotFound {
			return BindingDecision{}, &controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpGetCluster,
				action: "getting IBM cluster " + lookup.IDOrName, err: err}
		}
		found = nil
	}
	in.Found = found
	decision, err := DecideBinding(in)
	if err != nil {
		return BindingDecision{}, fmt.Errorf("deciding the binding of %s: %w", s.objectName(), err)
	}
	return decision, nil
}

// kubeconfigSecretClusterID returns the cluster-id annotation of the Cluster's <cluster>-kubeconfig Secret, or "" when
// the Secret or the annotation does not exist. The value is untrusted (anyone in the namespace can write it): it only
// selects the candidate of DecideBinding row 2, which binds only with the proof of possession. The Secret is read
// uncached with the API reader, because the manager has no cluster-wide Secret informer. ctx is the reconcile context.
func (s *ControlPlaneScope) kubeconfigSecretClusterID(ctx context.Context) (string, error) {
	key := client.ObjectKey{Namespace: s.Cluster.Namespace, Name: secret.Name(s.Cluster.Name, secret.Kubeconfig)}
	kubeconfig := &corev1.Secret{}
	if err := s.params.APIReader.Get(ctx, key, kubeconfig); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading the kubeconfig Secret %s: %w", key, err)
	}
	return kubeconfig.Annotations[controlplanev1.KubeconfigClusterIDAnnotation], nil
}

// loadInfraCluster reads the IBMCloudManagedCluster that the Cluster's spec.infrastructureRef names and stores it in
// InfraCluster. It returns nil and no error when that object does not exist, a *controlPlaneConfigError when the
// reference names another kind, and any other error for a failed read. ctx is the reconcile context.
func (s *ControlPlaneScope) loadInfraCluster(ctx context.Context) (*infrav1.IBMCloudManagedCluster, error) {
	ref := s.Cluster.Spec.InfrastructureRef
	if ref.APIGroup != infrav1.GroupVersion.Group || ref.Kind != cpInfraClusterKind {
		return nil, &controlPlaneConfigError{message: fmt.Sprintf("spec.infrastructureRef of Cluster %s names %s.%s %s; an "+
			"IBMCloudManagedControlPlane needs an %s (group %s)", s.Cluster.Name, ref.Kind, ref.APIGroup, ref.Name,
			cpInfraClusterKind, infrav1.GroupVersion.Group)}
	}
	infra := &infrav1.IBMCloudManagedCluster{}
	key := client.ObjectKey{Namespace: s.Cluster.Namespace, Name: ref.Name}
	if err := s.params.Client.Get(ctx, key, infra); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s/%s: %w", cpInfraClusterKind, key, err)
	}
	s.InfraCluster = infra
	return infra, nil
}

// controlPlaneConfigError is a problem in the user's objects that only a spec change fixes; its message names the
// object, the field and the fix.
type controlPlaneConfigError struct {
	message string
}

// Error implements error.
func (e *controlPlaneConfigError) Error() string {
	return e.message
}

// controlPlaneCallError is a failed IBM call: the operation, what the call did, the identity it used and IBM's
// answer. Its handlers turn it into a condition and an outcome by the class kubernetesservice.Classify gives it.
type controlPlaneCallError struct {
	// identity is the identity the call used.
	identity credentials.Identity
	// op is the KubernetesService operation; Classify needs it.
	op kubernetesservice.Operation
	// action says what the call did, for example "getting IBM cluster dev".
	action string
	// err is the error the operation returned.
	err error
}

// Error implements error; it carries IBM's answer for the controller log.
func (e *controlPlaneCallError) Error() string {
	return e.action + ": " + e.err.Error()
}

// Unwrap returns the operation's error.
func (e *controlPlaneCallError) Unwrap() error {
	return e.err
}

// cpProblem is how one failure is reported: a condition reason and message, and the outcome of the step.
type cpProblem struct {
	// reason and message are the condition's.
	reason  string
	message string
	// outcome is what the controller does next.
	outcome Outcome
	// transient marks a failure that says nothing about the cluster (5xx, network, 429); it leaves a True condition as
	// it is, so that a blip does not ripple into the Cluster's conditions.
	transient bool
}

// classifyCall maps a failed IBM call of the normal flow to its report by the class kubernetesservice.Classify gives
// it. Messages name the identity by its Description and carry IBM's code only (cpIBMCode), never IBM's free text or
// incident ID: IBM sends a new incident ID with every answer, and a message that changes with each answer would
// rewrite status on every reconcile, whose own watch event would start the next one at once. The caller logs the
// whole answer (logIBMRefusal) or returns it with a Transient outcome. A 401 invalidates only this identity's
// authenticator.
//
// Parameters:
//   - callErr is the failed call.
//   - resourceGroupID is the resource group the roles are needed on.
func (s *ControlPlaneScope) classifyCall(callErr *controlPlaneCallError, resourceGroupID string) cpProblem {
	identity := callErr.identity.Description
	reference := cpIBMCode(callErr.err)
	switch kubernetesservice.Classify(callErr.op, callErr.err) {
	case kubernetesservice.ClassUnauthorized:
		s.params.ClientBuilder.InvalidateAuthenticator(callErr.identity.Key)
		reason := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason
		return cpProblem{reason: reason, message: cpUnauthorizedMessage(identity, callErr.action), outcome: NotDone(IntervalSteadyState, reason)}
	case kubernetesservice.ClassPermissionDenied, kubernetesservice.ClassInfrastructureKeyDenied:
		reason := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason
		message := cpPermissionDeniedMessage(identity, callErr.action, reference, resourceGroupID)
		return cpProblem{reason: reason, message: message, outcome: Terminal(reason, message)}
	case kubernetesservice.ClassRateLimited:
		return cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			message:   fmt.Sprintf("IBM Cloud rate limited CAPIBM (HTTP 429) while %s; CAPIBM retries", callErr.action),
			outcome:   RateLimited(kubernetesservice.RateLimitBackoff(callErr.err)),
			transient: true}
	case kubernetesservice.ClassBadRequest:
		reason := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason
		message := fmt.Sprintf("IBM Cloud refused %s with HTTP %d%s; the controller log has IBM's description",
			callErr.action, cpIBMStatus(callErr.err), reference)
		return cpProblem{reason: reason, message: message, outcome: Terminal(reason, message)}
	default:
		return cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
			message:   fmt.Sprintf("CAPIBM could not complete %s%s; it retries, and the controller log names the cause", callErr.action, cpIBMCode(callErr.err)),
			outcome:   Transient(callErr),
			transient: true}
	}
}

// cpUnauthorizedMessage is the condition message of an IBM 401: only the identity's authenticator is rebuilt, and the
// next attempt follows at the steady interval.
//
// Parameters:
//   - identity is the Description of the identity IBM rejected.
//   - action says what the call did.
func cpUnauthorizedMessage(identity, action string) string {
	return fmt.Sprintf("IBM Cloud rejected the credentials of %s while %s; CAPIBM rebuilds its authenticator and retries "+
		"in about 5 minutes; check that the API key of %s is valid and not deleted", identity, action, identity)
}

// cpPermissionDeniedMessage is the condition message of an IBM 403 outside CreateCluster: it names the missing roles
// and the identity by its Description.
//
// Parameters:
//   - identity is the Description of the identity IBM refused.
//   - action says what the call did.
//   - reference is IBM's code (cpIBMCode); "" when IBM sent none.
//   - resourceGroupID is the resource group the roles are needed on.
func cpPermissionDeniedMessage(identity, action, reference, resourceGroupID string) string {
	return fmt.Sprintf("IBM Cloud refused %s with HTTP 403%s: the credentials of %s need %s on resource group %s; %s",
		action, reference, identity, cpKubernetesServiceRoles, resourceGroupID, cpIAMPageHint)
}

// cpIBMStatus returns the HTTP status of an IBM answer, or 0 when err is not one.
//
// Parameters:
//   - err is the error a KubernetesService method returned.
func cpIBMStatus(err error) int {
	var apiErr *kubernetesservice.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}
	return 0
}

// callProblem reports a failure of decideBinding in the normal flow: an IBM call through classifyCall, anything else
// (a Secret read, a rejected input) as a retried IBMAPIError whose cause is in the controller log.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - err is the failure.
func (s *ControlPlaneScope) callProblem(ctx context.Context, err error) Outcome {
	var callErr *controlPlaneCallError
	if errors.As(err, &callErr) {
		return s.reportCall(ctx, callErr)
	}
	return s.report(cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message:   "CAPIBM could not decide which IBM cluster this control plane stands for; it retries, and the controller log names the cause",
		outcome:   Transient(err),
		transient: true})
}

// clientsProblem reports a failure of ibm, before any IBM call. The message names the identity by its Description
// when it resolved, but never carries the error text, which goes to the controller log through the outcome, so the
// condition stays the same while the same failure repeats. err is the error of ibm.
func (s *ControlPlaneScope) clientsProblem(err error) Outcome {
	message := fmt.Sprintf("CAPIBM cannot resolve the IBM Cloud credentials of this control plane through IBMCloudManagedCluster/%s; "+
		"the controller log names the cause", s.InfraCluster.Name)
	var buildErr *clientBuildError
	if errors.As(err, &buildErr) {
		message = fmt.Sprintf("CAPIBM cannot build the Kubernetes Service client of %s for region %s; the controller log names the cause",
			buildErr.identity.Description, s.InfraCluster.Spec.Region)
	}
	return s.report(cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message: message, outcome: Transient(err)})
}

// infraClusterProblem reports a failure of loadInfraCluster: a reference to another kind needs a spec change
// (Terminal InvalidConfiguration); a failed read is retried. err is the error of loadInfraCluster.
func (s *ControlPlaneScope) infraClusterProblem(err error) Outcome {
	var configErr *controlPlaneConfigError
	if errors.As(err, &configErr) {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason, configErr.message)
	}
	return s.report(cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason,
		message:   fmt.Sprintf("CAPIBM could not read %s; it retries, and the controller log names the cause", s.infraRefName()),
		outcome:   Transient(err),
		transient: true})
}

// report writes problem into IBMClusterProvisioned, unless it is transient and the condition is already True, and
// returns its outcome. problem is the failure to report.
func (s *ControlPlaneScope) report(problem cpProblem) Outcome {
	if problem.transient && conditions.IsTrue(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition) {
		return problem.outcome
	}
	s.setProvisioned(metav1.ConditionFalse, problem.reason, problem.message)
	if !conditions.IsTrue(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition) {
		s.setAvailableWaiting()
	}
	return problem.outcome
}

// waitForInfrastructure reports that the Cluster's infrastructure is not ready yet; the Cluster watch brings the
// control plane back when it is. message names what is awaited.
func (s *ControlPlaneScope) waitForInfrastructure(message string) Outcome {
	reason := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason
	s.setProvisioned(metav1.ConditionFalse, reason, message)
	s.setAvailableWaiting()
	return NotDone(IntervalClusterProvisioning, reason)
}

// notProvisioned reports a condition that needs a human: IBMClusterProvisioned False with reason and message, and a
// Terminal outcome, rechecked at terminalRecheck.
//
// reportCall reports a failed IBM call of the normal flow through classifyCall. A failure that is not retried with
// its error (a Terminal or a rechecked condition) is logged with IBM's whole answer, because the condition carries
// only IBM's code, so that it does not change with every answer.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - callErr is the failed call.
func (s *ControlPlaneScope) reportCall(ctx context.Context, callErr *controlPlaneCallError) Outcome {
	problem := s.classifyCall(callErr, s.InfraCluster.Spec.ResourceGroup.ID)
	if problem.outcome.Kind != OutcomeTransient {
		logIBMRefusal(ctx, callErr)
	}
	return s.report(problem)
}

// logIBMRefusal logs IBM's whole answer to a failed call, with its description and incident ID, which conditions
// never carry because they change with every answer; an operator quotes the incident ID to IBM support.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - callErr is the failed call.
func logIBMRefusal(ctx context.Context, callErr *controlPlaneCallError) {
	ctrl.LoggerFrom(ctx).Info("IBM Cloud did not complete a call", "call", callErr.action, "error", callErr.err.Error())
}

// Parameters:
//   - reason is the IBMClusterProvisioned reason.
//   - message says what to fix.
func (s *ControlPlaneScope) notProvisioned(reason, message string) Outcome {
	s.setProvisioned(metav1.ConditionFalse, reason, message)
	s.setAvailableWaiting()
	return Terminal(reason, message)
}

// setProvisioned sets IBMClusterProvisioned.
//
// Parameters:
//   - status is the condition status.
//   - reason and message are the condition's; message may be empty.
func (s *ControlPlaneScope) setProvisioned(status metav1.ConditionStatus, reason, message string) {
	conditions.Set(s.ControlPlane, metav1.Condition{
		Type:    controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// setAvailableWaiting sets Available False with WaitingForIBMCluster: no IBM cluster is bound or it is still being
// created.
func (s *ControlPlaneScope) setAvailableWaiting() {
	conditions.Set(s.ControlPlane, metav1.Condition{
		Type:    controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
		Status:  metav1.ConditionFalse,
		Reason:  controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason,
		Message: "No IBM cluster is provisioned for this control plane yet; see IBMClusterProvisioned",
	})
}

// objectName names the reconciled object in messages, for example IBMCloudManagedControlPlane/dev.
func (s *ControlPlaneScope) objectName() string {
	return "IBMCloudManagedControlPlane/" + s.ControlPlane.Name
}

// infraRefName names the object the Cluster's spec.infrastructureRef points at, for example
// IBMCloudManagedCluster/dev.
func (s *ControlPlaneScope) infraRefName() string {
	ref := s.Cluster.Spec.InfrastructureRef
	return ref.Kind + "/" + ref.Name
}

// NoIBMClusterCanExist reports whether a control plane cannot be bound to an IBM cluster: it has no status.clusterID,
// no spec.controlPlaneEndpoint and no status.createIntent. CAPIBM writes the create intent immediately before it sends
// createCluster, and the endpoint is the never-recreate marker that clusterctl move and restores carry, so a control
// plane without all three never sent a create and was never bound; deleting it cannot leave an IBM cluster behind.
// The admission webhooks use the same test for the delete guards of IBMCloudManagedControlPlane and
// IBMCloudManagedCluster.
//
// Parameters:
//   - cp is the control plane; nil cannot be bound.
func NoIBMClusterCanExist(cp *controlplanev1.IBMCloudManagedControlPlane) bool {
	if cp == nil {
		return true
	}
	return cp.Status.ClusterID == "" &&
		cp.Spec.ControlPlaneEndpoint == (controlplanev1.APIEndpoint{}) &&
		!cpHasCreateIntent(cp.Status.CreateIntent)
}

// cpHasCreateIntent reports whether any field of a status.createIntent is set.
//
// Parameters:
//   - intent is the control plane's status.createIntent.
func cpHasCreateIntent(intent controlplanev1.CreateIntent) bool {
	return intent.ClusterName != "" || !intent.InFlightAttemptTime.IsZero() ||
		!intent.FirstUnknownOutcomeTime.IsZero() || !intent.LastUnknownOutcomeTime.IsZero()
}

// cpIBMCode returns " (IBM code <c>)" for an IBM answer that carries a code, else "". Every condition message of the
// control plane names only the code, never the incident ID: IBM sends a new incident ID with every answer (the live
// fixtures of one error carry different ones), and a message that changed with each answer would rewrite status on
// every reconcile and re-trigger it through the controller's own watch; the controller log keeps the whole answer
// (logIBMRefusal).
//
// Parameters:
//   - err is the error a KubernetesService method returned.
func cpIBMCode(err error) string {
	var apiErr *kubernetesservice.APIError
	if errors.As(err, &apiErr) && apiErr.Code != "" {
		return " (IBM code " + apiErr.Code + ")"
	}
	return ""
}
