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
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file is the control plane scope: it finds the Cluster's IBMCloudManagedCluster, decides which IBM cluster the
// IBMCloudManagedControlPlane stands for (the binding, DecideBinding), creates that cluster when none exists
// (BuildCreateClusterRequest), and reports what IBM says about it (ClassifyClusterState, ObserveCluster).
// The decisions themselves are pure functions of this package (binding.go, cluster_state.go, create_request.go); the
// scope only gathers their inputs, makes the IBM calls they name and writes their results into status and conditions.
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

	// persist writes the control plane to the API server at once, before an IBM call whose effect must be recorded
	// first (PersistFunc).
	persist PersistFunc

	// clients are the IBM clients of this reconcile, built by ibm on first use.
	clients *controlPlaneClients

	// endpointAsRead is spec.controlPlaneEndpoint as read from the API server. Initialization reads it rather than the
	// in-memory object, so that controlPlaneInitialized is never persisted before the endpoint: the patch helper writes
	// spec and status in separate requests.
	endpointAsRead controlplanev1.APIEndpoint

	// kubeconfigSecret is the <cluster>-kubeconfig Secret once loadKubeconfigSecret has read it (nil when it does not
	// exist), and kubeconfigSecretLoaded records that read, so that the binding and the kubeconfig step share one GET.
	kubeconfigSecret       *corev1.Secret
	kubeconfigSecretLoaded bool

	// kubeconfigRequeue is managedkubeconfig.Decide's RequeueAfter of this reconcile (KubeconfigRequeueAfter).
	kubeconfigRequeue time.Duration

	// masterStep is what the master version step (reconcileMasterVersion) of this reconcile left for the controller's
	// requeue (MasterVersionOutcome, MasterVersionRequeueAfter); the zero value when it did not run.
	masterStep masterVersionStep
}

// PersistFunc writes the control plane's in-memory changes to the API server at once. The scope calls it before an IBM
// call whose effect must be on record first: the create intent before CreateCluster and the deletion request time
// before DeleteCluster. When it fails, the IBM call is not made. The controller passes its patcher, which then takes
// the written object as its baseline, so that a field cleared later in the same reconcile (an intent that a definitive
// answer clears) is cleared on the API server as well. ctx is the reconcile context.
type PersistFunc func(ctx context.Context) error

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
//   - params are the shared dependencies; the scope needs Client, APIReader, Recorder, Resolver, ClientBuilder, Clock,
//     Prober (the kubeconfig probe and the ownership proof) and VersionsCache.
//   - cluster is the owning CAPI Cluster.
//   - cp is the IBMCloudManagedControlPlane being reconciled, as read from the API server.
//   - persist writes cp to the API server at once (PersistFunc); the controller passes its patcher.
func NewControlPlaneScope(params *ScopeParams, cluster *clusterv1.Cluster, cp *controlplanev1.IBMCloudManagedControlPlane,
	persist PersistFunc) (*ControlPlaneScope, error) {
	if err := params.validate("ControlPlaneScope", depClient, depAPIReader, depRecorder, depResolver, depClientBuilder, depClock,
		depProber, depVersionsCache); err != nil {
		return nil, err
	}
	if cluster == nil {
		return nil, errors.New("managed: ControlPlaneScope needs the owning Cluster, got nil")
	}
	if cp == nil {
		return nil, errors.New("managed: ControlPlaneScope needs the IBMCloudManagedControlPlane, got nil")
	}
	if persist == nil {
		return nil, errors.New("managed: ControlPlaneScope needs a PersistFunc that writes the control plane before an IBM mutation, got nil")
	}
	return &ControlPlaneScope{params: params, Cluster: cluster, ControlPlane: cp, persist: persist,
		endpointAsRead: cp.Spec.ControlPlaneEndpoint}, nil
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
		return s.infraClusterProblem(ctx, err)
	}
	if infra == nil {
		return s.waitForInfrastructure(fmt.Sprintf("Waiting for %s, named by spec.infrastructureRef of Cluster %s, to exist",
			s.infraRefName(), s.Cluster.Name))
	}

	name, err := EffectiveClusterName(cp.Spec.IBMCloudClusterName, s.Cluster.Name)
	if err != nil {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason, err.Error())
	}
	// A control plane that cannot be bound to any IBM cluster has nothing to look up while a refusal holds its creates,
	// so the hold applies before any IBM call. One with an earlier create attempt of unknown outcome on record still
	// runs the binding, because that attempt's cluster may show up (rows 6 and 9); createCluster applies the hold there,
	// and report keeps the refusal's condition when the credentials, the Secret read or the lookup fail before it, so
	// that such a failure does not end the hold.
	if NoIBMClusterCanExist(cp) {
		if outcome, held := s.createHeld(ctx); held {
			return outcome
		}
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.clientsProblem(ctx, err)
	}
	decision, err := s.decideBinding(ctx, clients, name, false)
	if err != nil {
		return s.callProblem(ctx, err)
	}
	return s.actOnBinding(ctx, decision, name)
}

// createHeld applies the hold after a definitive create refusal, so that a refused create is sent again only at the
// recheck its outcome scheduled (about 15 minutes later, 5 after a 401), never in a hot loop. IBM's refusal changes
// status (the attempt taken out of the intent, the refusal condition), and the For() watch has no predicate, so this
// controller's own patch brings the control plane straight back; without the hold that reconcile would decide binding
// row 10 again and send the next create at once. It is checked at two points: in ReconcileNormal, before any IBM
// call, for a control plane that cannot be bound (NoIBMClusterCanExist), and in createCluster, where every create
// would be sent, which also covers one whose intent still records an earlier attempt of unknown outcome: such a
// control plane is looked up by name on every reconcile, and the lookup decides row 10 again once that attempt is
// older than CreateGracePeriod.
// While cpCreateHold holds, the reconcile sends no create and writes nothing: at most it looks the name up once, a
// failure of that lookup (or of anything before it) leaves the refusal's condition in place (report), and only a
// lookup that finds a cluster is reported. So the refusal condition stays as IBM's last answer and nothing
// re-triggers. held is false when no hold applies. ctx carries the reconcile's logger.
func (s *ControlPlaneScope) createHeld(ctx context.Context) (Outcome, bool) {
	until, held := cpCreateHold(s.ControlPlane, s.params.Clock.Now())
	if !held {
		return Outcome{}, false
	}
	ctrl.LoggerFrom(ctx).V(4).Info("IBM refused the last create request; no create request before the hold ends", "until", bindingTime(until))
	// The steady interval is shorter than CreateRefusalHold, so a long hold is looked at a few times without an IBM
	// call; CreateRefusalHold and CreateUnauthorizedHold end before the recheck the refusal itself scheduled.
	reason := conditions.GetReason(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)
	return NotDone(IntervalSteadyState, reason), true
}

// actOnBinding carries out a binding decision of the normal flow: it observes a bound cluster, waits for one CAPIBM
// may have created, acts on a Create decision, or reports a decision that needs a human.
//
// Parameters:
//   - ctx is the reconcile context.
//   - decision is DecideBinding's answer.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) actOnBinding(ctx context.Context, decision BindingDecision, name string) Outcome {
	switch decision.Action {
	case BindingKnown:
		return s.observe(ctx, decision)
	case BindingWait:
		// Rows 3 and 9: a create of CAPIBM's may still show up; no mutation until the window ends.
		s.setProvisioned(metav1.ConditionFalse, decision.Reason, decision.Message)
		s.setAvailableWaiting()
		return NotDone(IntervalClusterProvisioning, decision.Reason)
	case BindingCreate:
		return s.createCluster(ctx, name)
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
// Status receives IBM's stable tokens through ObserveCluster, which strips the " (pending)" marker from every version
// and omits a value it cannot record rather than writing "", which the API's MinLength=1 rejects.
//
// For a cluster that exists it then runs the access steps (reconcileAccess) and the master version step
// (reconcileMasterVersion), unless IBM rate limited the access steps.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - decision is a BindingKnown decision; its Cluster is the cluster GetCluster returned.
func (s *ControlPlaneScope) observe(ctx context.Context, decision BindingDecision) Outcome {
	cp := s.ControlPlane
	c := decision.Cluster
	cp.Status.ClusterID = decision.ClusterID
	// A bound control plane sends no create request, so a leftover hold (cpCreateHold) has nothing left to hold.
	delete(cp.Annotations, controlplanev1.CreateHoldAnnotation)
	if decision.Row == "1" {
		cp.Status.CreateIntent = controlplanev1.CreateIntent{}
	}
	observed, err := ObserveCluster(c)
	observed.ApplyTo(&cp.Status)
	if err != nil {
		// The other fields are valid; the value that was left out is named, because the forms IBM reports during a
		// master update are not yet verified against IBM Cloud and the parser may need to learn one.
		ctrl.LoggerFrom(ctx).Info("IBM reported a cluster value that CAPIBM does not record", "problem", err.Error())
	}

	label := fmt.Sprintf("IBM cluster %s (ID %s)", c.Name, c.ID)
	switch ClassifyClusterState(c.State) {
	case ClusterStateProvisioning:
		s.setProvisioned(metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
			fmt.Sprintf("IBM is creating %s (state %s)%s", label, c.State, cpNonCriticalNote(cp.Status.NonCriticalCreateErrorCodes)))
		s.setAvailableWaiting()
		s.waitForMasterKubeconfig(c)
		return NotDone(IntervalClusterProvisioning, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
	case ClusterStateFailed:
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreateFailedReason,
			fmt.Sprintf("IBM reports %s in state %s; CAPIBM never deletes it on its own: delete this Cluster and create it again", label, c.State))
	case ClusterStateGoneOutOfBand:
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedDeletedOutOfBandReason,
			fmt.Sprintf("IBM reports %s in state %s although this Cluster is not being deleted; CAPIBM never recreates it: "+
				"delete this Cluster", label, c.State))
	default:
		var notes []string
		if !KnownClusterState(c.State) {
			// Forward compatible: a new IBM state never stops a working cluster, but it is named.
			notes = append(notes, fmt.Sprintf("unknown IBM state %s", c.State))
		}
		if codes := cp.Status.NonCriticalCreateErrorCodes; len(codes) > 0 {
			notes = append(notes, cpNonCriticalSentence(codes))
		}
		s.setProvisioned(metav1.ConditionTrue, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedReason, strings.Join(notes, "; "))
		access := s.reconcileAccess(ctx, c)
		// The master version step, after the steps that may initialize the control plane; it does nothing before
		// initialization. It runs whatever else the kubeconfig steps returned, also while IBM reports the master as not
		// ready, which it may well do while it updates the master (IBM's states during a master update are not yet
		// verified against IBM Cloud): RollingOut must say so then (DecideMasterAction row 1). After a 429 of those
		// steps it waits for IBM's Retry-After (reconcileMasterVersion). Its outcome goes to the controller separately
		// (MasterVersionOutcome).
		s.reconcileMasterVersion(ctx, c, access)
		return access
	}
}

// reportMaster reports the master of an existing cluster in Available: it is usable once masterState is deployed,
// masterStatus is Ready and masterURL is set (IsMasterReady); masterHealth stays "" for minutes after that and does
// not count, and only error or unavailable health makes it unusable again (IsMasterUnhealthy), as observed on IBM
// Cloud. The cluster's ingress status is never consulted: IBM reports it as warning or critical without a public
// gateway or with fewer than two workers per zone. It covers the reconciles before the kubeconfig step (the master
// not ready yet, the one that records the endpoint) and an unhealthy master; reportAvailable reports the rest. c is
// the cluster GetCluster returned.
func (s *ControlPlaneScope) reportMaster(c *kubernetesservice.Cluster) Outcome {
	reason := controlplanev1.IBMCloudManagedControlPlaneAvailableMasterNotReadyReason
	switch {
	case !IsMasterReady(c):
		s.setAvailable(reason, fmt.Sprintf("The master of IBM cluster %s is not ready yet (masterState %s)", c.ID, cpToken(c.MasterState)))
		// IBM Cloud showed a usable master 16-25 minutes after the create; poll at the provisioning cadence until then.
		return NotDone(IntervalClusterProvisioning, reason)
	case IsMasterUnhealthy(c):
		s.setAvailable(reason, fmt.Sprintf("IBM reports the master health of cluster %s as %s", c.ID, c.MasterHealth))
		return Done()
	default:
		s.setAvailable(controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason,
			fmt.Sprintf("The master of IBM cluster %s is ready; the control plane becomes available once CAPIBM has verified its "+
				"kubeconfig and initialized it", c.ID))
		return Done()
	}
}

// setAvailable sets Available to False with reason and message.
//
// Parameters:
//   - reason is the Available reason.
//   - message says what is awaited.
func (s *ControlPlaneScope) setAvailable(reason, message string) {
	conditions.Set(s.ControlPlane, metav1.Condition{
		Type:    controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	})
}

// cpToken returns an IBM token for a message, or "not reported" when IBM sent none. token is the IBM value.
func cpToken(token string) string {
	if token == "" {
		return "not reported"
	}
	return token
}

// decideBinding gathers the inputs of DecideBinding, makes the one GetCluster call it names and returns its decision
// (rows 1-10, in delete mode for the delete flow). When only the proof of possession stands between a candidate of
// rows 2 or 5 and its binding (ProofNeeded), it runs the proof (proveOwnership) and decides again with its result; no
// IBM credential is fetched for a candidate before it is proven.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - name is the effective IBM cluster name.
//   - deleting selects the delete mode of DecideBinding.
//
// It returns a *controlPlaneCallError when GetCluster failed with anything but 404 or getCACert failed during the
// proof, and any other error for a read of the kubeconfig Secret that failed or an input DecideBinding rejected.
func (s *ControlPlaneScope) decideBinding(ctx context.Context, clients *controlPlaneClients, name string, deleting bool) (BindingDecision, error) {
	cp := s.ControlPlane
	kubeconfig, err := s.loadKubeconfigSecret(ctx)
	if err != nil {
		return BindingDecision{}, err
	}
	secretClusterID, secretServer := "", ""
	if kubeconfig != nil {
		secretClusterID = kubeconfig.Annotations[controlplanev1.KubeconfigClusterIDAnnotation]
		secretServer = kubeconfig.Annotations[KubeconfigServerAnnotation]
	}
	// The Secret's annotations are untrusted (anyone in the namespace can write them): the cluster-id only selects the
	// candidate of row 2 and the server only feeds EndpointMatches of row 5, both of which bind only with the proof.
	in := BindingInput{
		StatusClusterID: cp.Status.ClusterID,
		SecretClusterID: secretClusterID,
		SecretServer:    secretServer,
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
	if err != nil || !decision.ProofNeeded {
		return decision, s.bindingError(err)
	}
	if in.Proof, in.ProofFailure, err = s.proveOwnership(ctx, clients, decision.Cluster); err != nil {
		return BindingDecision{}, err
	}
	decision, err = DecideBinding(in)
	return decision, s.bindingError(err)
}

// bindingError names the control plane in an input error of DecideBinding, a programming error; nil stays nil. err is
// DecideBinding's error.
func (s *ControlPlaneScope) bindingError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("deciding the binding of %s: %w", s.objectName(), err)
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
	return s.report(ctx, cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message:   "CAPIBM could not decide which IBM cluster this control plane stands for; it retries, and the controller log names the cause",
		outcome:   Transient(err),
		transient: true})
}

// clientsProblem reports a failure of ibm, before any IBM call. The message names the identity by its Description
// when it resolved, but never carries the error text, which goes to the controller log through the outcome, so the
// condition stays the same while the same failure repeats.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - err is the error of ibm.
func (s *ControlPlaneScope) clientsProblem(ctx context.Context, err error) Outcome {
	message := fmt.Sprintf("CAPIBM cannot resolve the IBM Cloud credentials of this control plane through IBMCloudManagedCluster/%s; "+
		"the controller log names the cause", s.InfraCluster.Name)
	var buildErr *clientBuildError
	if errors.As(err, &buildErr) {
		message = fmt.Sprintf("CAPIBM cannot build the Kubernetes Service client of %s for region %s; the controller log names the cause",
			buildErr.identity.Description, s.InfraCluster.Spec.Region)
	}
	return s.report(ctx, cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMAPIErrorReason,
		message: message, outcome: Transient(err)})
}

// infraClusterProblem reports a failure of loadInfraCluster: a reference to another kind needs a spec change
// (Terminal InvalidConfiguration); a failed read is retried.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - err is the error of loadInfraCluster.
func (s *ControlPlaneScope) infraClusterProblem(ctx context.Context, err error) Outcome {
	var configErr *controlPlaneConfigError
	if errors.As(err, &configErr) {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason, configErr.message)
	}
	return s.report(ctx, cpProblem{reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason,
		message:   fmt.Sprintf("CAPIBM could not read %s; it retries, and the controller log names the cause", s.infraRefName()),
		outcome:   Transient(err),
		transient: true})
}

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
	return s.report(ctx, problem)
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

// report writes problem into IBMClusterProvisioned (and Available WaitingForIBMCluster) and returns its outcome. It
// keeps the condition instead in two cases, because the condition then records more than the problem says:
//   - a True condition, when problem is transient: a blip says nothing about the cluster and must not ripple into the
//     Cluster's conditions;
//   - the condition of a definitive create refusal while that refusal holds the next create request (cpCreateHold),
//     for every problem. Only a failure before a create can be reported then (resolving the credentials, reading the
//     IBMCloudManagedCluster or the kubeconfig Secret, the lookup by name that an earlier create attempt of unknown
//     outcome needs): createCluster sends nothing while the hold holds, and every create removes the hold before it
//     is sent. Such a failure says nothing about why IBM refused the create, but writing it would end the hold, which
//     lasts only while the refusal's condition does; the write itself then triggers the next reconcile through the
//     For() watch, whose lookup may decide binding row 10 again and send the refused create at once. Lookups that fail
//     on and off (the 429s of the shared IKS rate limit) would turn that into a loop of refused creates. The failure
//     is logged instead and retried at its outcome's pace, and it is reported as usual once the hold is over.
//
// Parameters:
//   - ctx carries the reconcile's logger.
//   - problem is the failure to report.
func (s *ControlPlaneScope) report(ctx context.Context, problem cpProblem) Outcome {
	if problem.transient && conditions.IsTrue(s.ControlPlane, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition) {
		return problem.outcome
	}
	if until, held := cpCreateHold(s.ControlPlane, s.params.Clock.Now()); held {
		// The message is CAPIBM's own text without IBM's description, as in a condition; a failed IBM call has its whole
		// answer in the log already (logIBMRefusal) or in the error that a Transient outcome returns.
		ctrl.LoggerFrom(ctx).Info("A create refusal holds the next create request; this failure is retried without replacing "+
			"the refusal's condition", "reason", problem.reason, "message", problem.message, "until", bindingTime(until))
		return problem.outcome
	}
	s.setProvisioned(metav1.ConditionFalse, problem.reason, problem.message)
	s.setAvailableWaiting()
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

// setAvailableWaiting sets Available False with WaitingForIBMCluster. Every step that sets IBMClusterProvisioned False
// calls it, so it covers more than the wait its reason names: no IBM cluster is bound yet or IBM is still creating it,
// but also a bound cluster that CAPIBM cannot read (a 401, a 403, an IBM error it does not retry) or that IBM reports
// as failed, deleted or not found, even on a control plane that was available. The message therefore does not claim
// that no cluster exists yet, which would send an operator to wait while the credentials or the cluster need a fix;
// it sends the reader to IBMClusterProvisioned, whose reason tells these cases apart.
func (s *ControlPlaneScope) setAvailableWaiting() {
	s.setAvailable(controlplanev1.IBMCloudManagedControlPlaneAvailableWaitingForIBMClusterReason,
		"The IBM cluster of this control plane is not provisioned or CAPIBM cannot read it; IBMClusterProvisioned says why")
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

// Create (only after DecideBinding decided Create: row 10, no IBM cluster of this control plane exists).

// The events of the create flow.
const (
	// EventReasonSuccessfulCreateIBMCluster is the Normal event of a create request IBM accepted.
	EventReasonSuccessfulCreateIBMCluster = "SuccessfulCreateIBMCluster"
	// EventReasonIBMCreateWarnings is the Warning event of one non-critical error of an accepted create request (IBM
	// answers 201 with non_critical_errors, observed on IBM Cloud for IKS and ROKS). It carries IBM's description and
	// recovery text, which never go into status.
	EventReasonIBMCreateWarnings = "IBMCreateWarnings"
)

// The IBM error codes whose create refusal gets a specific message (both observed on IBM Cloud). kubernetesservice
// keeps its copies unexported; these only select the message, Classify decides the class.
const (
	// cpCodeSubnetNotFound is IBM's "subnet cannot be found" (HTTP 400; it creates nothing).
	cpCodeSubnetNotFound = "P4102"
	// cpCodeCOSInstanceRequired is IBM's "ROKS on VPC needs a COS instance CRN" (HTTP 400; it creates nothing).
	cpCodeCOSInstanceRequired = "E7278"
)

// The bounds of status.nonCriticalCreateErrorCodes (api/managed/controlplane/v1alpha1): a longer list or code would make
// the API server reject the whole status patch, so such values stay in the events only.
const (
	cpNonCriticalCodesMaxItems  = 16
	cpNonCriticalCodesMaxLength = 16
)

// createCluster creates the IBM cluster of the control plane. A create hold that still holds (createHeld) stops it
// first, before anything is read or written. The configuration is checked next (the initial pool,
// BuildCreateClusterRequest, the offered versions), so a configuration IBM would refuse records nothing and sends no
// create. Then the attempt is persisted in status.createIntent, and only after that write succeeded is the request
// sent: a lost response can then be told apart from a foreign same-named cluster (DecideBinding rows 3, 6, 9).
//
// Parameters:
//   - ctx is the reconcile context.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) createCluster(ctx context.Context, name string) Outcome {
	// Every create request goes through here, so this check holds a refused create whatever led to row 10, also when
	// an earlier attempt of unknown outcome kept ReconcileNormal from applying the hold before the lookup.
	if outcome, held := s.createHeld(ctx); held {
		return outcome
	}
	pool, mp, outcome, ok := s.initialPool(ctx)
	if !ok {
		return outcome
	}
	request, err := BuildCreateClusterRequest(s.ControlPlane, s.Cluster, s.InfraCluster, pool, mp)
	if err != nil {
		return s.createRequestProblem(err)
	}
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.clientsProblem(ctx, err)
	}
	if outcome, ok := s.checkVersionOffered(ctx, clients); !ok {
		return outcome
	}

	cp := s.ControlPlane
	previous := cp.Status.CreateIntent
	cp.Status.CreateIntent = cpIntentForAttempt(previous, name, s.now())
	// A hold that reaches this point is over (createHeld returned above while it held); it goes with the intent's write.
	delete(cp.Annotations, controlplanev1.CreateHoldAnnotation)
	s.setProvisioned(metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
		fmt.Sprintf("CAPIBM creates IBM cluster %s in resource group %s", name, s.InfraCluster.Spec.ResourceGroup.ID))
	s.setAvailableWaiting()
	if err := s.persist(ctx); err != nil {
		// No POST without the intent on record, so that a create whose answer is lost is still recognised by the
		// binding. The intent is taken back, so that the deferred patch does not record an attempt that was never sent.
		cp.Status.CreateIntent = previous
		return Transient(fmt.Errorf("recording the create intent of IBM cluster %s before the create request: %w", name, err))
	}

	result, err := clients.kubernetes.CreateCluster(ctx, request)
	if err != nil {
		return s.createFailed(ctx, clients, name, err)
	}
	return s.created(name, result)
}

// initialPool returns the IBMCloudManagedMachinePool of the Cluster that is created together with the IBM cluster (the
// one pool with spec.creation WithCluster, withClusterPools) and the MachinePool that owns it. ok is false when the
// outcome says to wait (no such pool, or CAPI has not linked it to its MachinePool yet) or to stop (more than one);
// neither makes an IBM call. The pool watch brings the control plane back when a pool is added, linked or re-marked
// (machinePoolToControlPlane). It runs only for binding row 10, after the binding lookup, so the marker never changes
// which IBM cluster the control plane stands for. ctx is the reconcile context.
func (s *ControlPlaneScope) initialPool(ctx context.Context) (*infrav1.IBMCloudManagedMachinePool, *clusterv1.MachinePool, Outcome, bool) {
	pools := &infrav1.IBMCloudManagedMachinePoolList{}
	if err := s.params.Client.List(ctx, pools, client.InNamespace(s.Cluster.Namespace),
		client.MatchingLabels{clusterv1.ClusterNameLabel: s.Cluster.Name}); err != nil {
		return nil, nil, Transient(fmt.Errorf("listing the IBMCloudManagedMachinePools of Cluster %s: %w", s.Cluster.Name, err)), false
	}
	initial := withClusterPools(pools.Items)
	waiting := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInitialWorkerPoolReason
	switch len(initial) {
	case 0:
		return nil, nil, s.wait(waiting, fmt.Sprintf("No IBMCloudManagedMachinePool of Cluster %s has spec.creation: %s; "+
			"mark exactly one pool, it is created together with the IBM cluster", s.Cluster.Name,
			infrav1.PoolCreationWithCluster)), false
	case 1:
	default:
		names := make([]string, 0, len(initial))
		for _, pool := range initial {
			names = append(names, pool.Name)
		}
		slices.Sort(names)
		return nil, nil, s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
			fmt.Sprintf("IBMCloudManagedMachinePools %s of Cluster %s all have spec.creation: %s; IBM creates the cluster with "+
				"one initial pool, so keep exactly one of them %s and set spec.creation: %s on the others (or omit it)",
				strings.Join(names, ", "), s.Cluster.Name, infrav1.PoolCreationWithCluster,
				infrav1.PoolCreationWithCluster, infrav1.PoolCreationAfterCluster)), false
	}
	pool := initial[0]
	mp, err := util.GetOwnerMachinePool(ctx, s.params.Client, pool.ObjectMeta)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, nil, Transient(fmt.Errorf("reading the MachinePool that owns IBMCloudManagedMachinePool %s: %w", pool.Name, err)), false
	}
	if mp == nil {
		return nil, nil, s.wait(waiting, fmt.Sprintf("IBMCloudManagedMachinePool %s is not linked to its MachinePool yet; CAPI sets "+
			"the owner reference, and the MachinePool's spec.replicas sizes the first pool", pool.Name)), false
	}
	return pool, mp, Outcome{}, true
}

// createRequestProblem reports a failure of BuildCreateClusterRequest: a *ConfigurationError needs a spec change
// (Terminal InvalidConfiguration, nothing recorded, no IBM call); any other error means an input is not resolved yet,
// for example a zone of the IBMCloudManagedCluster, and is waited for. err is the error of BuildCreateClusterRequest.
func (s *ControlPlaneScope) createRequestProblem(err error) Outcome {
	var configErr *ConfigurationError
	if errors.As(err, &configErr) {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason, configErr.Error())
	}
	return s.wait(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedWaitingForInfrastructureReason,
		fmt.Sprintf("CAPIBM cannot build the create request yet: %v", err))
}

// checkVersionOffered confirms that IBM offers spec.distributionVersion for new clusters, from the VersionsCache of the
// identity and region, so that a create is not refused for a minor IBM dropped. ok is false when the outcome stops
// the create.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
func (s *ControlPlaneScope) checkVersionOffered(ctx context.Context, clients *controlPlaneClients) (Outcome, bool) {
	cp := s.ControlPlane
	minor, err := ibmversion.ParseMinor(cp.Spec.DistributionVersion)
	if err != nil {
		// BuildCreateClusterRequest has already refused such a value; this keeps the step safe on its own.
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
			fmt.Sprintf("spec.distributionVersion: %v", err)), false
	}
	region := s.InfraCluster.Spec.Region
	versions, err := s.params.VersionsCache.Get(ctx, clients.identity.Key, region, clients.kubernetes.ListVersions)
	if err != nil {
		return s.reportCall(ctx, &controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpListVersions,
			action: "listing the versions IBM Cloud offers in region " + region, err: err}), false
	}
	offered := MasterVersionInput{Distribution: cp.Spec.Distribution, Offered: versions}
	if !offered.offers(minor) {
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedVersionNotAvailableReason,
			fmt.Sprintf("IBM Cloud does not offer %s %s (offered: %s)", masterProduct(cp.Spec.Distribution), minor,
				offered.offeredMinors())), false
	}
	return Outcome{}, true
}

// created records a create request IBM accepted (201): status.clusterID, the codes of IBM's non-critical errors
// (non_critical_errors of the answer) and the events. The intent stays until DecideBinding row 1 observes the cluster
// by ID, which clears it; until then it is the reference of the read-after-create wait (row 3).
//
// Parameters:
//   - name is the effective IBM cluster name.
//   - result is CreateCluster's answer.
func (s *ControlPlaneScope) created(name string, result *kubernetesservice.CreateClusterResult) Outcome {
	cp := s.ControlPlane
	cp.Status.ClusterID = result.ClusterID
	cp.Status.NonCriticalCreateErrorCodes = cpNonCriticalCodes(result.NonCriticalErrors)
	s.params.Recorder.Eventf(cp, corev1.EventTypeNormal, EventReasonSuccessfulCreateIBMCluster,
		"Created IBM cluster %s (ID %s)", name, result.ClusterID)
	for _, item := range result.NonCriticalErrors {
		// A non-critical error is never a failure: IBM created the cluster but could not finish something, for example
		// the registry access policy (E49df). Its text goes into the event only.
		s.params.Recorder.Event(cp, corev1.EventTypeWarning, EventReasonIBMCreateWarnings,
			cpNonCriticalEventMessage(name, result.ClusterID, result.IncidentID, item))
	}
	s.setProvisioned(metav1.ConditionFalse, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
		fmt.Sprintf("IBM accepted the create request of cluster %s (ID %s)%s", name, result.ClusterID,
			cpNonCriticalNote(cp.Status.NonCriticalCreateErrorCodes)))
	s.setAvailableWaiting()
	return NotDone(IntervalClusterProvisioning, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason)
}

// createFailed handles a create request IBM did not accept. An answer that proves nothing was created
// (ClearIntentErrorCodes, ClearIntentStatusCodes, or a request that was never sent) takes the attempt out of the intent
// and, unless it is E0007, holds the next create request (holdCreates); any other answer (429, 5xx, timeout) makes its
// outcome unknown, and DecideBinding rows 3, 6 and 9 resolve it. IBM's description and incident ID go to the
// controller log; status keeps CAPIBM's text with IBM's code only, so that it does not change with every answer.
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - name is the effective IBM cluster name.
//   - err is CreateCluster's error.
func (s *ControlPlaneScope) createFailed(ctx context.Context, clients *controlPlaneClients, name string, err error) Outcome {
	cp := s.ControlPlane
	createdNothing := cpCreatedNothing(err)
	if createdNothing {
		cp.Status.CreateIntent = cpClearAttempt(cp.Status.CreateIntent)
	} else {
		cp.Status.CreateIntent = cpFoldInFlight(cp.Status.CreateIntent)
	}
	ctrl.LoggerFrom(ctx).Info("IBM Cloud did not accept the create request", "ibmCluster", name, "error", err.Error())

	class := kubernetesservice.Classify(kubernetesservice.OpCreateCluster, err)
	if hold, ok := cpCreateHoldFor(class); ok && createdNothing {
		// A definitive refusal: the same request would be refused again, so the next one waits for the recheck that the
		// outcome below schedules (about 15 minutes later, 5 after a 401), never in a hot loop.
		s.holdCreates(hold)
	}
	rg := s.InfraCluster.Spec.ResourceGroup.ID
	region := s.InfraCluster.Spec.Region
	reference := cpIBMCode(err)
	permissionDenied := controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason
	switch class {
	case kubernetesservice.ClassClusterNameExists:
		return s.afterNameExists(ctx, clients, name)
	case kubernetesservice.ClassInfrastructureKeyNotCreatable:
		return s.notProvisioned(permissionDenied, InfrastructureKeyNotCreatableMessage(region, rg, clients.identity.Description, reference))
	case kubernetesservice.ClassInfrastructureKeyDenied:
		return s.notProvisioned(permissionDenied, cpInfrastructureKeyDeniedMessage(name, region, rg, clients.identity.Description, reference))
	case kubernetesservice.ClassBadRequest:
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
			s.createRefusedMessage(name, err))
	default:
		// createFailed has logged IBM's whole answer already, so classifyCall is reported without reportCall's log.
		return s.report(ctx, s.classifyCall(&controlPlaneCallError{identity: clients.identity, op: kubernetesservice.OpCreateCluster,
			action: "creating IBM cluster " + name, err: err}, rg))
	}
}

// afterNameExists decides again after IBM answered E0007 (a cluster with the name exists): this attempt created
// nothing and has left the intent, so DecideBinding tests a found cluster against the earlier attempts of unknown
// outcome only; without one it is ClusterNameConflict (row 7), even when another namespace or management cluster
// created it seconds ago. A lookup that still finds nothing is ClusterNameConflict as well: IBM cluster names are
// unique in the account, and the lookup, scoped to the resource group, may not see a cluster of another resource
// group that holds the name (not yet verified against IBM Cloud). A new create would only be refused again, so the
// next one is held like any other definitive refusal (holdCreates).
//
// Parameters:
//   - ctx is the reconcile context.
//   - clients are this reconcile's IBM clients.
//   - name is the effective IBM cluster name.
func (s *ControlPlaneScope) afterNameExists(ctx context.Context, clients *controlPlaneClients, name string) Outcome {
	decision, err := s.decideBinding(ctx, clients, name, false)
	if err != nil {
		return s.callProblem(ctx, err)
	}
	if decision.Action == BindingCreate {
		s.holdCreates(CreateRefusalHold)
		return s.notProvisioned(controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
			fmt.Sprintf("IBM Cloud refused to create cluster %s because a cluster with that name already exists (E0007), but "+
				"CAPIBM finds none in resource group %s, so it may be in another resource group of the account and CAPIBM cannot "+
				"prove it created it for this Cluster. If it does not belong to this Cluster, delete this Cluster and create it "+
				"again with a different spec.ibmCloudClusterName (the field cannot be changed in place; nothing was created for "+
				"this Cluster)", name, s.InfraCluster.Spec.ResourceGroup.ID))
	}
	return s.actOnBinding(ctx, decision, name)
}

// createRefusedMessage is the InvalidConfiguration message of a create request that IBM refused with 400 or 422, or
// that CAPIBM refused to send. It names what to fix for the codes observed on IBM Cloud (P4102, E7278) and IBM's code
// (cpIBMCode), never IBM's description, which the controller log has.
//
// Parameters:
//   - name is the effective IBM cluster name.
//   - err is CreateCluster's error.
func (s *ControlPlaneScope) createRefusedMessage(name string, err error) string {
	if errors.Is(err, kubernetesservice.ErrInvalidRequest) {
		// CAPIBM's own text: the request validation names the field and the accepted form.
		return fmt.Sprintf("CAPIBM did not send the create request of IBM cluster %s: %v", name, err)
	}
	reference := cpIBMCode(err)
	var apiErr *kubernetesservice.APIError
	if !errors.As(err, &apiErr) {
		return fmt.Sprintf("IBM Cloud refused to create cluster %s; the controller log has the cause", name)
	}
	switch apiErr.Code {
	case cpCodeSubnetNotFound:
		return fmt.Sprintf("IBM Cloud refused to create cluster %s because a subnet of the request does not exist%s; check the "+
			"subnets of IBMCloudManagedCluster %s (status.zones)", name, reference, s.InfraCluster.Name)
	case cpCodeCOSInstanceRequired:
		return fmt.Sprintf("IBM Cloud refused to create cluster %s because an OpenShift cluster on VPC needs a standard Cloud "+
			"Object Storage instance for its registry%s; set spec.openShift.imageRegistry.cos.instanceCRN", name, reference)
	default:
		return fmt.Sprintf("IBM Cloud refused to create cluster %s with HTTP %d%s; the controller log has IBM's description of "+
			"the configuration to fix", name, apiErr.StatusCode, reference)
	}
}

// InfrastructureKeyNotCreatableMessage is the PermissionDenied message of a create that IBM answered with A0656: IBM
// could not create the per-region and resource-group infrastructure API key with the caller's credentials (observed on
// IBM Cloud; IBM checks this before it validates the request). It names the identity only by its Description.
//
// Parameters:
//   - region is the IBM Cloud region of the cluster.
//   - resourceGroupID is the resource group of the cluster.
//   - identity is the Description of the credentials.Identity the request used.
//   - reference is IBM's code (cpIBMCode); "" when IBM sent none.
func InfrastructureKeyNotCreatableMessage(region, resourceGroupID, identity, reference string) string {
	return fmt.Sprintf("IBM cannot create the infrastructure API key for region %s and resource group %s with the credentials "+
		"of %s%s; a service ID needs the IAM Identity Service roles (Service ID creator, User API key creator, Viewer and "+
		"Operator), or an account administrator pre-sets the key with `ibmcloud ks api-key reset --region %s`; %s",
		region, resourceGroupID, identity, reference, region, cpIAMPageHint)
}

// cpInfrastructureKeyDeniedMessage is the PermissionDenied message of a create that IBM answered with 403: the
// infrastructure API key IBM stores for the region and resource group, which may belong to another identity, or the
// caller's credentials lack permissions.
//
// Parameters:
//   - name is the effective IBM cluster name.
//   - region is the IBM Cloud region.
//   - resourceGroupID is the resource group.
//   - identity is the Description of the credentials.Identity the request used.
//   - reference is IBM's code (cpIBMCode); "" when IBM sent none.
func cpInfrastructureKeyDeniedMessage(name, region, resourceGroupID, identity, reference string) string {
	return fmt.Sprintf("IBM Cloud refused to create cluster %s with HTTP 403%s: the infrastructure API key IBM stores for region "+
		"%s and resource group %s, or the credentials of %s, lack permissions; the credentials need %s, and `ibmcloud ks "+
		"api-key info --cluster <a cluster of that region and resource group>` shows whose key IBM stores, which `ibmcloud "+
		"ks api-key reset --region %s` replaces; %s", name, reference, region, resourceGroupID, identity,
		cpKubernetesServiceRoles, region, cpIAMPageHint)
}

// cpCreatedNothing reports whether a CreateCluster error proves that no cluster was created: IBM's code or status says
// so (ClearIntentErrorCodes, ClearIntentStatusCodes), or the request never reached IBM (CAPIBM's request validation,
// or IAM refusing the credentials). err is CreateCluster's error.
func cpCreatedNothing(err error) bool {
	if errors.Is(err, kubernetesservice.ErrInvalidRequest) || errors.Is(err, kubernetesservice.ErrCredentialsRejected) {
		return true
	}
	var apiErr *kubernetesservice.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return slices.Contains(ClearIntentErrorCodes(), apiErr.Code) || slices.Contains(ClearIntentStatusCodes(), apiErr.StatusCode)
}

// cpIntentForAttempt returns the intent to persist before a create request: a leftover inFlightAttemptTime (a crash or a
// lost status patch after an earlier POST) is first folded into the unknown-outcome bounds, because its outcome is
// unknown, and then the new attempt is in flight. An intent recorded for another name cannot have created a cluster
// with this one and is replaced.
//
// Parameters:
//   - previous is status.createIntent as read.
//   - name is the effective IBM cluster name.
//   - now is the attempt's time.
func cpIntentForAttempt(previous controlplanev1.CreateIntent, name string, now metav1.Time) controlplanev1.CreateIntent {
	intent := previous
	if intent.ClusterName != name {
		intent = controlplanev1.CreateIntent{}
	}
	intent = cpFoldInFlight(intent)
	intent.ClusterName = name
	intent.InFlightAttemptTime = now
	return intent
}

// cpFoldInFlight moves an inFlightAttemptTime into [firstUnknownOutcomeTime, lastUnknownOutcomeTime]: the attempt may
// have created a cluster. intent is the intent to change; an intent without an attempt in flight is returned as it is.
func cpFoldInFlight(intent controlplanev1.CreateIntent) controlplanev1.CreateIntent {
	inFlight := intent.InFlightAttemptTime
	if inFlight.IsZero() {
		return intent
	}
	if intent.FirstUnknownOutcomeTime.IsZero() || inFlight.Before(&intent.FirstUnknownOutcomeTime) {
		intent.FirstUnknownOutcomeTime = inFlight
	}
	if intent.LastUnknownOutcomeTime.IsZero() || intent.LastUnknownOutcomeTime.Before(&inFlight) {
		intent.LastUnknownOutcomeTime = inFlight
	}
	intent.InFlightAttemptTime = metav1.Time{}
	return intent
}

// cpClearAttempt takes the attempt in flight out of the intent after an answer that proves it created nothing, and
// clears the whole intent when no attempt of unknown outcome remains. intent is the intent to change.
func cpClearAttempt(intent controlplanev1.CreateIntent) controlplanev1.CreateIntent {
	intent.InFlightAttemptTime = metav1.Time{}
	if intent.FirstUnknownOutcomeTime.IsZero() && intent.LastUnknownOutcomeTime.IsZero() {
		return controlplanev1.CreateIntent{}
	}
	return intent
}

// cpNonCriticalCodes returns the codes of IBM's non-critical create errors for status.nonCriticalCreateErrorCodes:
// sorted and without duplicates, so the field is deterministic; a code that the field cannot hold, or one beyond its
// item limit, stays in the events only. items are the non-critical errors; none gives nil, which omits the field.
func cpNonCriticalCodes(items []kubernetesservice.IBMMessage) []string {
	var codes []string
	for _, item := range items {
		if item.Code != "" && len(item.Code) <= cpNonCriticalCodesMaxLength {
			codes = append(codes, item.Code)
		}
	}
	slices.Sort(codes)
	codes = slices.Compact(codes)
	if len(codes) > cpNonCriticalCodesMaxItems {
		codes = codes[:cpNonCriticalCodesMaxItems]
	}
	return codes
}

// cpNonCriticalSentence says that IBM created the cluster with non-critical errors. codes are their codes.
func cpNonCriticalSentence(codes []string) string {
	return fmt.Sprintf("IBM created the cluster with non-critical errors %s (see the %s events)", strings.Join(codes, ", "),
		EventReasonIBMCreateWarnings)
}

// cpNonCriticalNote returns "; " plus cpNonCriticalSentence, or "" when there are no codes. codes are the codes.
func cpNonCriticalNote(codes []string) string {
	if len(codes) == 0 {
		return ""
	}
	return "; " + cpNonCriticalSentence(codes)
}

// cpNonCriticalEventMessage is the IBMCreateWarnings event of one non-critical create error, with IBM's own text.
//
// Parameters:
//   - name is the effective IBM cluster name.
//   - clusterID is the new cluster's ID.
//   - incidentID is IBM's incident ID of the non-critical errors; "" when IBM sent none.
//   - item is the error.
func cpNonCriticalEventMessage(name, clusterID, incidentID string, item kubernetesservice.IBMMessage) string {
	message := fmt.Sprintf("IBM created cluster %s (ID %s) with the non-critical error %s", name, clusterID, item.Code)
	if item.Type != "" {
		message += " (" + item.Type + ")"
	}
	if item.Description != "" {
		message += ": " + item.Description
	}
	if item.RecoveryCLI != "" {
		message += " Recovery: " + item.RecoveryCLI
	}
	if incidentID != "" {
		message += " [incident " + incidentID + "]"
	}
	return message
}

// The hold after a definitive create refusal: a refused create is rechecked after about 15 minutes (5 minutes after a
// 401), never in a hot loop. A refusal that proves nothing was created takes its attempt out of the intent, so the
// next reconcile would decide binding row 10 again and send the same request, which IBM refuses again; an earlier
// attempt of unknown outcome that the intent still records changes nothing once it is older than CreateGracePeriod.
// The controller's own status patch brings that reconcile at once (the For() watch has no predicate), so without a
// hold one refusal turns into a create request every few milliseconds, each spending the shared IKS rate limit of
// every other cluster and pool.
//
// The hold is recorded in controlplanev1.CreateHoldAnnotation (RFC 3339, UTC). The key lives in the API package with
// the other controller-owned annotations, so that tooling and the book can name it without importing the controller.
// Anyone who can edit the control plane can change it, which shifts the next create request by at most
// CreateRefusalHold (cpCreateHold ignores a later time).
const (
	// CreateRefusalHold is how long a Terminal create refusal (A0656, 403, 400, 422, E0007 with no cluster found)
	// holds the next create request. It is shorter than terminalRecheck (15 minutes, internal/controllers/managed
	// requeue.go) minus its 10 % jitter, so that the recheck the refusal's Terminal outcome scheduled always finds the
	// hold over and sends the next request, about 15 minutes after the refusal.
	CreateRefusalHold = 12 * time.Minute

	// CreateUnauthorizedHold is how long a 401 (IBM or the IAM token service rejected the credentials) holds the next
	// create request. It is shorter than steadyStatePoll (5 minutes) minus its jitter, so that the recheck of the 401's
	// outcome sends the next request about 5 minutes later with a rebuilt authenticator.
	CreateUnauthorizedHold = 4 * time.Minute
)

// cpCreateRefusalReasons are the IBMClusterProvisioned reasons a definitive create refusal leaves (createFailed,
// afterNameExists).
var cpCreateRefusalReasons = []string{
	controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedPermissionDeniedReason,
	controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedInvalidConfigurationReason,
	controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
}

// cpCreateHoldFor returns how long a create answer of class holds the next create request, and whether it holds at
// all. ClassClusterNameExists is not held here: E0007 is held only when the lookup after it finds no cluster
// (afterNameExists); a found cluster is decided by the binding on every reconcile. class is Classify's class of the
// CreateCluster error.
func cpCreateHoldFor(class kubernetesservice.ErrorClass) (time.Duration, bool) {
	switch class {
	case kubernetesservice.ClassUnauthorized:
		return CreateUnauthorizedHold, true
	case kubernetesservice.ClassInfrastructureKeyNotCreatable, kubernetesservice.ClassInfrastructureKeyDenied,
		kubernetesservice.ClassPermissionDenied, kubernetesservice.ClassBadRequest:
		return CreateRefusalHold, true
	default:
		return 0, false
	}
}

// holdCreates records in controlplanev1.CreateHoldAnnotation that no create request is sent before now plus hold; the
// deferred patch persists it with the refusal's condition. hold is CreateRefusalHold or CreateUnauthorizedHold.
func (s *ControlPlaneScope) holdCreates(hold time.Duration) {
	until := s.now().Add(hold).Format(time.RFC3339)
	metav1.SetMetaDataAnnotation(&s.ControlPlane.ObjectMeta, controlplanev1.CreateHoldAnnotation, until)
}

// cpCreateHold reports whether a definitive create refusal still holds the next create request of cp, and until when.
// It holds while all of these are true, so that only the refusal itself, unchanged, keeps CAPIBM from asking again:
//   - no IBM cluster is bound: no status.clusterID and no endpoint, because a bound control plane sends no create. An
//     earlier create attempt of unknown outcome does not end the hold: it only makes the caller run the binding first
//     (createHeld), so that the cluster that attempt may have created is still bound or waited for;
//   - controlplanev1.CreateHoldAnnotation parses and lies after now, but at most CreateRefusalHold after it; a later
//     value was not written by CAPIBM (or by a clock far ahead) and is ignored, so that no edit of the annotation stops
//     creates;
//   - IBMClusterProvisioned is still the refusal's condition: False with a reason of cpCreateRefusalReasons, observed
//     at the current generation. A spec change therefore ends the hold at once (a Terminal outcome waits for a spec
//     edit or its recheck), and so does a later state that replaced the refusal's condition since, such as
//     infrastructure that is no longer provisioned or a cluster the lookup by name found. A failure does not replace it
//     while the hold holds (report), because the failure alone would then end the hold.
//
// Parameters:
//   - cp is the control plane as read.
//   - now is the clock's time.
func cpCreateHold(cp *controlplanev1.IBMCloudManagedControlPlane, now time.Time) (time.Time, bool) {
	if cp == nil || cp.Status.ClusterID != "" || cp.Spec.ControlPlaneEndpoint != (controlplanev1.APIEndpoint{}) ||
		cp.Annotations[controlplanev1.CreateHoldAnnotation] == "" {
		return time.Time{}, false
	}
	until, err := time.Parse(time.RFC3339, cp.Annotations[controlplanev1.CreateHoldAnnotation])
	if err != nil || !now.Before(until) || until.Sub(now) > CreateRefusalHold {
		return time.Time{}, false
	}
	condition := conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.ObservedGeneration != cp.Generation ||
		!slices.Contains(cpCreateRefusalReasons, condition.Reason) {
		return time.Time{}, false
	}
	return until, true
}

// wait reports an expected wait in IBMClusterProvisioned and polls at clusterProvisioningPoll.
//
// Parameters:
//   - reason is the IBMClusterProvisioned reason.
//   - message names what is awaited.
func (s *ControlPlaneScope) wait(reason, message string) Outcome {
	s.setProvisioned(metav1.ConditionFalse, reason, message)
	s.setAvailableWaiting()
	return NotDone(IntervalClusterProvisioning, reason)
}

// now returns the clock's time at the precision the API keeps (seconds), so that the time written into status equals
// the time that is read back on the next reconcile.
func (s *ControlPlaneScope) now() metav1.Time {
	return metav1.NewTime(s.params.Clock.Now().UTC().Truncate(time.Second))
}
