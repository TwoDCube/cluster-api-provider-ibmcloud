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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmclustername"
)

// This file decides the binding of an IBMCloudManagedControlPlane: which IBM cluster ID it stands for (the rows that
// DecideBinding lists, and their delete mode). The binding is never about credentials; the IAM identity CAPIBM acts as
// is resolved separately, through the Cluster's IBMCloudManagedCluster, so DecideBinding has no credential input. It
// is pure: the control plane controller makes the one GetCluster call that BindingInput.Lookup names, runs the
// ownership proof when a decision asks for it, and carries out the decision.
//
// Trust: status is written only by controllers and IBM's answers are IBM's, so both are trusted; Secret annotations,
// Secret contents and spec.controlPlaneEndpoint can be written by anyone in the namespace, so a cluster they point at
// is never bound without the candidate checks and, for rows 2 and 5, the proof of possession.

// The IBM cluster attributes the candidate checks expect. kubernetesservice keeps its own copy of the provider
// unexported, so the values are repeated here; IBM's getCluster returned exactly these tokens for IKS and ROKS clusters
// (observed on IBM Cloud).
const (
	// bindingTypeKubernetes is the getCluster type of an IKS cluster.
	bindingTypeKubernetes = "kubernetes"
	// bindingTypeOpenShift is the getCluster type of a ROKS cluster.
	bindingTypeOpenShift = "openshift"
	// bindingProviderVPCGen2 is the only provider CAPIBM manages: VPC Gen2 (no classic infrastructure, no Satellite).
	bindingProviderVPCGen2 = "vpc-gen2"
)

// bindingDefaultPort is the port of an https URL that names none. It equals assumptions.go's defaultMasterPort, which
// is added together with the kubeconfig step, so the binding keeps its own copy.
const bindingDefaultPort = "443"

// EffectiveClusterName returns the IBM cluster name of a control plane: spec.ibmCloudClusterName when set, otherwise
// the CAPI Cluster's metadata.name. The result is checked against IBM's cluster name rule (ibmclustername.Problem)
// before any IBM call uses it, because IBM refuses createCluster for any other name (HTTP 400, code E0125), and a
// name IBM refuses can never match an IBM cluster. The admission webhook checks the same rule, but it cannot always
// tell the Cluster whose name is the default, and a Cluster name is a DNS-1123 name that may break IBM's rule (longer
// than 32 characters, or a digit first); an explicit spec.ibmCloudClusterName reaches here unchecked only from an
// object stored before the rule was enforced.
//
// Parameters:
//   - ibmCloudClusterName: spec.ibmCloudClusterName of the IBMCloudManagedControlPlane; "" when omitted.
//   - clusterName: metadata.name of the owning CAPI Cluster.
//
// It returns an error that names the value, where it came from, what breaks IBM's rule, the rule and the fix when the
// name is not acceptable; the caller reports it as Terminal(InvalidConfiguration).
func EffectiveClusterName(ibmCloudClusterName, clusterName string) (string, error) {
	name, source := ibmCloudClusterName, "spec.ibmCloudClusterName"
	if name == "" {
		name, source = clusterName, "the Cluster's metadata.name (spec.ibmCloudClusterName is omitted)"
	}
	if problem := ibmclustername.Problem(name); problem != "" {
		// Both sources are fixed for the life of the object: the Cluster's name is immutable, and
		// spec.ibmCloudClusterName can be neither changed nor added after creation. The only fix is a new Cluster,
		// which is cheap because CAPIBM never sent this name to IBM.
		return "", fmt.Errorf("the IBM cluster name %q, taken from %s, %s; %s; CAPIBM sends no request to IBM "+
			"with this name, and spec.ibmCloudClusterName cannot be added or changed after creation, so delete this "+
			"Cluster and create it again with spec.ibmCloudClusterName set to a name that follows the rule",
			name, source, problem, ibmclustername.Rule)
	}
	return name, nil
}

// OwnershipProof is the outcome of the proof of possession for a candidate of rows 2 and 5 (ProvesOwnership): the
// kubeconfig Secret's credential passed, or failed, the cluster-admin access review against the candidate's own master
// URL. The proof does not depend on the identity CAPIBM acts as: comparing the certificate's subject with it would fail
// after a key rotation, or on a clusterctl move target that runs with other credentials.
type OwnershipProof string

const (
	// OwnershipProofNotAttempted means the caller did not run the proof; the zero value "" means the same. A caller that
	// does not run the proof passes it, and rows 2 and 5 then never bind; a decision with ProofNeeded tells the caller
	// that running the proof could change it.
	OwnershipProofNotAttempted OwnershipProof = "NotAttempted"
	// OwnershipProofProven means ProvesOwnership returned true for the candidate.
	OwnershipProofProven OwnershipProof = "Proven"
	// OwnershipProofFailed means the proof ran and did not pass (TLS, 401, access review denied, or a Secret without an
	// inline client certificate, such as one that holds a bearer token).
	OwnershipProofFailed OwnershipProof = "Failed"
)

// BindingInput is everything DecideBinding decides on. Only status fields and IBM's answer are trusted (see the file
// comment).
type BindingInput struct {
	// StatusClusterID is status.clusterID; "" when no cluster is bound yet or after clusterctl move.
	StatusClusterID string

	// SecretClusterID is the cluster-id annotation of the <cluster>-kubeconfig Secret (CAPIBM-owned or
	// user-provided); "" when there is no Secret or no annotation. Untrusted: it only selects the candidate of row 2.
	SecretClusterID string

	// Endpoint is spec.controlPlaneEndpoint; the zero value when unset. Untrusted at create time: it only marks the
	// cluster as existing (rows 5 and 8), it never proves ownership.
	Endpoint controlplanev1.APIEndpoint

	// SecretServer is the server CAPIBM recorded in the annotations of its kubeconfig Secret, which follows IBM's master
	// URL when it changes; "" when unknown. EndpointMatches compares it as well.
	SecretServer string

	// Intent is status.createIntent: the create attempts whose outcome is not known.
	Intent controlplanev1.CreateIntent

	// Name is the effective IBM cluster name (EffectiveClusterName). Required.
	Name string

	// ResourceGroupID is spec.resourceGroup.id of the Cluster's IBMCloudManagedCluster. Required.
	ResourceGroupID string

	// Distribution is spec.distribution; it selects the IBM cluster type the candidate must have. Required.
	Distribution controlplanev1.Distribution

	// Found is the cluster the one GetCluster call named by Lookup returned; nil when IBM answered 404
	// (kubernetesservice.ClassNotFound). Any other error of that call is the caller's to handle (by its
	// kubernetesservice.Classify class, for example Transient) and never reaches DecideBinding.
	Found *kubernetesservice.Cluster

	// Proof is the outcome of the proof of possession for Found; read only for rows 2 and 5.
	Proof OwnershipProof

	// ProofFailure names the failing check when Proof is OwnershipProofFailed, for the condition message (for example
	// "the SelfSubjectAccessReview for */*/* was denied"). It must carry no API server text or user name;
	// workloadprobe.ProbeError.Error() carries neither. Optional.
	ProofFailure string

	// Now is the caller's clock reading. Required.
	Now time.Time

	// Deleting selects the delete mode, which the delete flow uses: row 9 waits DeleteLookupGracePeriod, row 10 means
	// there is nothing to delete, and a same-named cluster that an attempt of unknown outcome may have created is
	// PossibleOwnClusterFound.
	Deleting bool
}

// BindingLookup is the one GetCluster call DecideBinding needs.
type BindingLookup struct {
	// IDOrName is GetCluster's first argument: a cluster ID for rows 1 to 4, the effective name for rows 5 to 10.
	IDOrName string

	// ResourceGroupID is GetCluster's second argument: "" for a lookup by ID, so that a cluster in another resource
	// group is still found and reported by the candidate checks (row 1b or 2b) instead of looking deleted (row 4); the
	// IBMCloudManagedCluster's resource group for a lookup by name, which IBM scopes by it.
	ResourceGroupID string

	// ByID is true for a lookup by ID.
	ByID bool
}

// Lookup returns the GetCluster call that DecideBinding needs for in: by status.clusterID when set (trusted), else by
// the Secret's cluster-id (a candidate after clusterctl move), else by name in the resource group.
func (in BindingInput) Lookup() BindingLookup {
	switch {
	case in.StatusClusterID != "":
		return BindingLookup{IDOrName: in.StatusClusterID, ByID: true}
	case in.SecretClusterID != "":
		return BindingLookup{IDOrName: in.SecretClusterID, ByID: true}
	default:
		return BindingLookup{IDOrName: in.Name, ResourceGroupID: in.ResourceGroupID}
	}
}

// BindingAction is what the caller does with a binding decision.
type BindingAction string

const (
	// BindingKnown binds the control plane to BindingDecision.ClusterID (rows 1, 2, 5, 6). The caller writes
	// status.clusterID and goes on to observe the cluster; in delete mode it deletes that cluster.
	BindingKnown BindingAction = "Known"
	// BindingWait waits for IBM to list a cluster CAPIBM may have created (rows 3 and 9); no mutation. The condition
	// reason is Creating (normal mode) or the delete flow's own wait.
	BindingWait BindingAction = "Wait"
	// BindingCreate creates the cluster (row 10, normal mode only).
	BindingCreate BindingAction = "Create"
	// BindingNothingToDelete means no IBM cluster of this control plane exists (row 10 in delete mode).
	BindingNothingToDelete BindingAction = "NothingToDelete"
	// BindingTerminal needs a human (rows 1b, 2b, 4, 7, 8): zero IBM mutations, no Secret write; the reason and the
	// message say why.
	BindingTerminal BindingAction = "Terminal"
)

// BindingDecision is DecideBinding's answer.
type BindingDecision struct {
	// Action is what the caller does.
	Action BindingAction

	// Row is the row of the DecideBinding table that decided, for example "1", "1b", "6" or "10"; for logs and tests.
	Row string

	// ClusterID is the bound IBM cluster ID; set only for BindingKnown.
	ClusterID string

	// Cluster is the IBM cluster the decision is about: the bound cluster (BindingKnown) or the candidate a terminal
	// decision refers to (rows 1b, 2b, 7). nil otherwise.
	Cluster *kubernetesservice.Cluster

	// Reason is the condition reason of a BindingTerminal or BindingWait decision: BindingMismatch,
	// OwnershipNotProven, IBMClusterNotFound, ClusterNameConflict, PossibleOwnClusterFound (delete mode) or Creating.
	// The tokens are those of api/managed/controlplane/v1alpha1; the delete flow maps them onto its Deleting reasons.
	// "" for the other actions.
	Reason string

	// Message explains the decision for the condition message. It is deterministic (times in RFC 3339, no elapsed
	// counters) and carries no IBM free text, so an unchanged decision writes nothing new.
	Message string

	// ProofNeeded is true when the candidate passed the candidate checks and only the proof of possession, which the
	// caller did not run (OwnershipProofNotAttempted), stops it from being bound. The caller may run the proof against
	// Cluster and decide again; a caller that does not run it keeps the terminal decision.
	ProofNeeded bool

	// WaitUntil is when the read-after-create window of a BindingWait ends; the caller requeues no later than this.
	WaitUntil time.Time
}

// DecideBinding decides which IBM cluster a control plane stands for. It evaluates these rows in order:
//
//   - 1: status.clusterID is set, GetCluster(id) finds the cluster and the candidate checks pass: Known.
//   - 1b: as row 1, but a candidate check fails: Terminal(BindingMismatch).
//   - 2: status is empty (for example after clusterctl move), the Secret's cluster-id is set, GetCluster(that id)
//     finds the cluster, and the candidate checks and the proof of possession pass: Known.
//   - 2b: as row 2, but a candidate check or the proof fails: Terminal(OwnershipNotProven).
//   - 3: row 1 or 2 got a 404 within CreateGracePeriod of the latest create attempt: Wait (read-after-create lag).
//   - 4: row 1 or 2 got a 404 otherwise: Terminal(IBMClusterNotFound); a cluster that existed is never recreated.
//   - 5: no ID; the same-named cluster passes the candidate checks, spec.controlPlaneEndpoint is set and matches it
//     (EndpointMatches, only while AdoptByEndpointMatch is on), and the proof passes: Known.
//   - 6: no ID; the same-named cluster passes the candidate checks, the endpoint is not set, and IBM created it within
//     the window of CAPIBM's create attempts of unknown outcome: Known (CAPIBM's own create whose response was lost).
//   - 7: no ID; a same-named cluster in any other case: Terminal(ClusterNameConflict); in delete mode, with an attempt
//     of unknown outcome recorded, PossibleOwnClusterFound.
//   - 8: no ID; the lookup by name gets a 404, and the endpoint is set: Terminal(IBMClusterNotFound).
//   - 9: no ID; 404; a create attempt of unknown outcome within CreateGracePeriod (DeleteLookupGracePeriod in delete
//     mode): Wait.
//   - 10: no ID; 404 otherwise: Create (in delete mode: nothing to delete).
//
// It never binds a cluster without a trusted record (status.clusterID), a proof of possession (rows 2 and 5), or a
// matching record of CAPIBM's own create attempt (row 6), and it never asks for a create while a create of unknown
// outcome may still show up (rows 3 and 9).
//
// Parameters:
//   - in: the observation; see BindingInput. in.Found must be the answer to the call in.Lookup() names.
//
// It returns an error naming the field to fix when in is invalid (a programming error of the caller).
func DecideBinding(in BindingInput) (BindingDecision, error) {
	if err := in.validate(); err != nil {
		return BindingDecision{}, err
	}
	switch {
	case in.StatusClusterID != "":
		return in.decideByID(in.StatusClusterID, true), nil
	case in.SecretClusterID != "":
		return in.decideByID(in.SecretClusterID, false), nil
	default:
		return in.decideByName(AdoptByEndpointMatch), nil
	}
}

// validate returns an error naming the first invalid field of in.
func (in BindingInput) validate() error {
	switch {
	case in.Name == "":
		return errors.New("BindingInput.Name is empty; pass the effective IBM cluster name (EffectiveClusterName)")
	case in.ResourceGroupID == "":
		return errors.New("BindingInput.ResourceGroupID is empty; pass spec.resourceGroup.id of the Cluster's IBMCloudManagedCluster")
	case bindingClusterType(in.Distribution) == "":
		return fmt.Errorf("BindingInput.Distribution is %q; want %q or %q", in.Distribution,
			controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift)
	case in.Now.IsZero():
		return errors.New("BindingInput.Now is not set; pass the controller clock's current time")
	}
	switch in.Proof {
	case "", OwnershipProofNotAttempted, OwnershipProofProven, OwnershipProofFailed:
		return nil
	default:
		return fmt.Errorf("BindingInput.Proof is %q; want %q, %q or %q", in.Proof, OwnershipProofNotAttempted,
			OwnershipProofProven, OwnershipProofFailed)
	}
}

// decideByID applies rows 1 to 4: a cluster ID is known from status (trusted) or from the Secret (a candidate).
//
// Parameters:
//   - id: the cluster ID that was looked up.
//   - fromStatus: true when id is status.clusterID, false when it is the Secret's cluster-id annotation.
func (in BindingInput) decideByID(id string, fromStatus bool) BindingDecision {
	source := "status.clusterID"
	if !fromStatus {
		source = "the cluster-id annotation of the kubeconfig Secret"
	}
	if in.Found == nil {
		// Row 3: a 201 recorded the ID, but IBM does not list the cluster yet (read-after-create lag).
		if latest, ok := in.latestAttempt(); ok && in.Now.Sub(latest) < CreateGracePeriod {
			until := latest.Add(CreateGracePeriod)
			return BindingDecision{Action: BindingWait, Row: "3", WaitUntil: until,
				Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
				Message: fmt.Sprintf("IBM Cloud does not list cluster %s yet; CAPIBM sent its create request at %s and "+
					"waits until %s for IBM to list it", id, bindingTime(latest), bindingTime(until))}
		}
		// Row 4: the cluster existed and is gone; it is never recreated.
		return BindingDecision{Action: BindingTerminal, Row: "4",
			Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
			Message: fmt.Sprintf("IBM Cloud reports no cluster %s, named by %s; CAPIBM never creates a replacement for "+
				"a cluster that existed. If it was deleted on purpose, delete this Cluster", id, source)}
	}
	problems := in.candidateProblems(in.Found, id)
	if fromStatus {
		if len(problems) == 0 {
			return BindingDecision{Action: BindingKnown, Row: "1", ClusterID: id, Cluster: in.Found}
		}
		// Row 1b: status names a cluster that is not this control plane's; touch nothing.
		return BindingDecision{Action: BindingTerminal, Row: "1b", Cluster: in.Found,
			Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedBindingMismatchReason,
			Message: fmt.Sprintf("IBM cluster %s, named by status.clusterID, does not match this control plane: %s; "+
				"CAPIBM changes nothing on it and writes no kubeconfig Secret", id, strings.Join(problems, "; "))}
	}
	if len(problems) == 0 && in.Proof == OwnershipProofProven {
		return BindingDecision{Action: BindingKnown, Row: "2", ClusterID: id, Cluster: in.Found}
	}
	// Row 2b: the Secret points at a cluster that fails a candidate check or the proof.
	decision := BindingDecision{Action: BindingTerminal, Row: "2b", Cluster: in.Found,
		Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedOwnershipNotProvenReason}
	failing := strings.Join(problems, "; ")
	if len(problems) == 0 {
		failing, decision.ProofNeeded = in.proofFailure()
	}
	decision.Message = fmt.Sprintf("%s names IBM cluster %s, but CAPIBM cannot prove that this control plane owns it: "+
		"%s. CAPIBM changes nothing on it and writes no kubeconfig Secret; to recover, put a fresh admin kubeconfig "+
		"of that cluster (ibmcloud ks cluster config --admin) into the Secret", source, id, failing)
	return decision
}

// proofFailure describes why the proof of possession did not bind a candidate that passed the candidate checks, and
// reports whether running the proof could still bind it.
func (in BindingInput) proofFailure() (string, bool) {
	if in.Proof == OwnershipProofFailed {
		if in.ProofFailure != "" {
			return "the Secret's credential did not pass the cluster-admin access review on it (" + in.ProofFailure + ")", false
		}
		return "the Secret's credential did not pass the cluster-admin access review on it", false
	}
	return "the cluster-admin access review with the Secret's credential has not run", true
}

// decideByName applies rows 5 to 10: no cluster ID is known, so the cluster was looked up by name in the resource
// group.
//
// Parameters:
//   - adoptByEndpointMatch: enables row 5. DecideBinding passes AdoptByEndpointMatch, which stays false until it is
//     verified on IBM Cloud that master URLs are unique and stable; the tests pass true to cover the row.
func (in BindingInput) decideByName(adoptByEndpointMatch bool) BindingDecision {
	first, last, unknown := in.unknownOutcomeWindow()
	endpointSet := in.Endpoint != controlplanev1.APIEndpoint{}
	if in.Found == nil {
		grace := CreateGracePeriod
		if in.Deleting {
			grace = DeleteLookupGracePeriod
		}
		switch {
		case endpointSet:
			// Row 8: the endpoint is only set once the cluster existed.
			return BindingDecision{Action: BindingTerminal, Row: "8",
				Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedIBMClusterNotFoundReason,
				Message: fmt.Sprintf("spec.controlPlaneEndpoint is set (%s), so an IBM cluster existed, but IBM Cloud "+
					"reports no cluster named %s in resource group %s; CAPIBM never creates a replacement. If it was "+
					"deleted on purpose, delete this Cluster", bindingEndpoint(in.Endpoint), in.Name, in.ResourceGroupID)}
		case unknown && in.Now.Sub(last) < grace:
			// Row 9: an attempt of unknown outcome may still show up.
			until := last.Add(grace)
			next := "creates the cluster"
			if in.Deleting {
				next = "concludes that there is nothing to delete"
			}
			return BindingDecision{Action: BindingWait, Row: "9", WaitUntil: until,
				Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCreatingReason,
				Message: fmt.Sprintf("IBM Cloud lists no cluster named %s yet; a create request sent at %s may still "+
					"have created it, so CAPIBM waits until %s before it %s", in.Name, bindingTime(last),
					bindingTime(until), next)}
		case in.Deleting:
			return BindingDecision{Action: BindingNothingToDelete, Row: "10"}
		default:
			return BindingDecision{Action: BindingCreate, Row: "10"}
		}
	}
	problems := in.candidateProblems(in.Found, "")
	if len(problems) == 0 && endpointSet && adoptByEndpointMatch && EndpointMatches(in.Found, in.Endpoint, in.SecretServer) {
		// Row 5: a user-provided Secret without the cluster-id annotation; the endpoint alone is never proof.
		if in.Proof == OwnershipProofProven {
			return BindingDecision{Action: BindingKnown, Row: "5", ClusterID: in.Found.ID, Cluster: in.Found}
		}
		decision := in.nameConflict(unknown, first, last)
		failing, needed := in.proofFailure()
		decision.ProofNeeded = needed
		decision.Message += " (the master URL matches spec.controlPlaneEndpoint, but " + failing + ")"
		return decision
	}
	if len(problems) == 0 && !endpointSet && unknown && in.createdInWindow(first, last) {
		// Row 6: our own create whose response was lost.
		return BindingDecision{Action: BindingKnown, Row: "6", ClusterID: in.Found.ID, Cluster: in.Found}
	}
	return in.nameConflict(unknown, first, last)
}

// nameConflict returns row 7: a same-named cluster that CAPIBM cannot prove it created. In delete mode, while an
// attempt of unknown outcome is recorded, the cluster may be ours, so the finalizer stays (PossibleOwnClusterFound);
// otherwise it is ClusterNameConflict, whose message never suggests deleting the other cluster.
//
// Parameters:
//   - unknown: whether the intent records an attempt of unknown outcome.
//   - first, last: the bounds of those attempts.
func (in BindingInput) nameConflict(unknown bool, first, last time.Time) BindingDecision {
	c := in.Found
	if in.Deleting && unknown {
		return BindingDecision{Action: BindingTerminal, Row: "7", Cluster: c,
			Reason: controlplanev1.IBMCloudManagedControlPlaneDeletingPossibleOwnClusterFoundReason,
			Message: fmt.Sprintf("IBM Cloud cluster %s (ID %s, created %s) may have been created by this control "+
				"plane's create requests of unknown outcome sent between %s and %s, but CAPIBM cannot prove it; CAPIBM "+
				"does not delete it and keeps the finalizer. If the cluster belongs to this Cluster, delete it in IBM "+
				"Cloud; otherwise remove the finalizer %s by hand", c.Name, c.ID, bindingCreatedDate(c), bindingTime(first),
				bindingTime(last), controlplanev1.IBMCloudManagedControlPlaneFinalizer)}
	}
	return BindingDecision{Action: BindingTerminal, Row: "7", Cluster: c,
		Reason: controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedClusterNameConflictReason,
		Message: fmt.Sprintf("IBM Cloud cluster %s (ID %s, created %s) already exists and CAPIBM cannot prove it "+
			"created it for this Cluster. Check in IBM Cloud whether it belongs to this Cluster. If it does not, delete "+
			"this Cluster and create it again with a different spec.ibmCloudClusterName (the field cannot be changed in "+
			"place; nothing was created for this Cluster). If it does, follow \"Recovering ownership\" on the "+
			"clusterctl move page of the CAPIBM book", c.Name, c.ID, bindingCreatedDate(c))}
}

// candidateProblems runs the candidate checks on c (name, resource group, the cluster type of the distribution, and
// the provider vpc-gen2) and returns one sentence per failed check; none means c may be this control plane's cluster.
// They run before any mutation or credential fetch.
//
// Parameters:
//   - c: the cluster IBM returned.
//   - wantID: the ID that was looked up; "" for a lookup by name.
func (in BindingInput) candidateProblems(c *kubernetesservice.Cluster, wantID string) []string {
	var problems []string
	if wantID != "" && c.ID != wantID {
		problems = append(problems, fmt.Sprintf("IBM returned cluster %s for the ID %s", c.ID, wantID))
	}
	if c.Name != in.Name {
		problems = append(problems, fmt.Sprintf("it is named %s, not %s", c.Name, in.Name))
	}
	if c.ResourceGroupID != in.ResourceGroupID {
		problems = append(problems, fmt.Sprintf("it is in resource group %s, not in %s (spec.resourceGroup.id of the "+
			"IBMCloudManagedCluster)", c.ResourceGroupID, in.ResourceGroupID))
	}
	if want := bindingClusterType(in.Distribution); c.Type != want {
		problems = append(problems, fmt.Sprintf("its type is %s, not %s (spec.distribution %s)", c.Type, want, in.Distribution))
	}
	if c.Provider != bindingProviderVPCGen2 {
		problems = append(problems, fmt.Sprintf("its provider is %s, not %s", c.Provider, bindingProviderVPCGen2))
	}
	return problems
}

// relevantIntent reports whether status.createIntent records attempts for this control plane's name. An intent for
// another name cannot have created a cluster with this one.
func (in BindingInput) relevantIntent() bool {
	return in.Intent.ClusterName != "" && in.Intent.ClusterName == in.Name
}

// latestAttempt returns the latest create attempt of the intent (row 3): inFlightAttemptTime or
// lastUnknownOutcomeTime, whichever is later. ok is false when the intent records none.
func (in BindingInput) latestAttempt() (time.Time, bool) {
	if !in.relevantIntent() {
		return time.Time{}, false
	}
	latest := in.Intent.LastUnknownOutcomeTime.Time
	if inFlight := in.Intent.InFlightAttemptTime.Time; inFlight.After(latest) {
		latest = inFlight
	}
	return latest, !latest.IsZero()
}

// unknownOutcomeWindow returns the earliest and latest create attempt whose outcome is unknown (rows 6, 7 in delete
// mode, and 9). Without a cluster ID an inFlightAttemptTime counts as one: it is left only by a crash or a lost status
// patch after the POST. A missing bound takes the other's value. ok is false when there is no such attempt.
func (in BindingInput) unknownOutcomeWindow() (time.Time, time.Time, bool) {
	if !in.relevantIntent() {
		return time.Time{}, time.Time{}, false
	}
	var first, last time.Time
	for _, t := range []time.Time{in.Intent.FirstUnknownOutcomeTime.Time, in.Intent.LastUnknownOutcomeTime.Time,
		in.Intent.InFlightAttemptTime.Time} {
		if t.IsZero() {
			continue
		}
		if first.IsZero() || t.Before(first) {
			first = t
		}
		if last.IsZero() || t.After(last) {
			last = t
		}
	}
	return first, last, !first.IsZero()
}

// createdInWindow reports whether IBM's createdDate of the found cluster lies in the window of row 6:
// first - CreatedDateSkew <= createdDate <= last + CreatedDateSlack, both edges included. A createdDate that does not
// parse is outside (row 7).
//
// Parameters:
//   - first, last: the bounds of the attempts of unknown outcome.
func (in BindingInput) createdInWindow(first, last time.Time) bool {
	created, err := ParseCreatedDate(in.Found.CreatedDate)
	if err != nil {
		return false
	}
	return !created.Before(first.Add(-CreatedDateSkew)) && !created.After(last.Add(CreatedDateSlack))
}

// bindingClusterType returns the IBM cluster type of a distribution, or "" for an unknown one.
//
// Parameters:
//   - d: spec.distribution.
func bindingClusterType(d controlplanev1.Distribution) string {
	switch d {
	case controlplanev1.DistributionKubernetes:
		return bindingTypeKubernetes
	case controlplanev1.DistributionOpenShift:
		return bindingTypeOpenShift
	default:
		return ""
	}
}

// bindingTime formats a time for a binding message: RFC 3339 in UTC, so that a repeated reconcile writes the same text.
//
// Parameters:
//   - t: the time.
func bindingTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// bindingCreatedDate returns IBM's createdDate of c for a message, or "at an unknown time" when IBM gave none.
//
// Parameters:
//   - c: the cluster.
func bindingCreatedDate(c *kubernetesservice.Cluster) string {
	if c.CreatedDate == "" {
		return "at an unknown time"
	}
	return c.CreatedDate
}

// bindingEndpoint formats spec.controlPlaneEndpoint as host:port.
//
// Parameters:
//   - e: the endpoint.
func bindingEndpoint(e controlplanev1.APIEndpoint) string {
	return net.JoinHostPort(e.Host, strconv.Itoa(int(e.Port)))
}

// EndpointMatches reports whether an IBM cluster serves the API endpoint a control plane recorded (row 5 of
// DecideBinding): whether the host:port of its masterURL, its public service endpoint URL or its private service
// endpoint URL equals the host:port of spec.controlPlaneEndpoint or of the server recorded on the kubeconfig Secret,
// which follows IBM's URL changes. Hosts compare without case; a URL without a port means 443. It is never proof on
// its own: row 5 also needs ProvesOwnership, and stays off (AdoptByEndpointMatch) until it is verified on IBM Cloud
// that master URLs are unique per cluster and stable over upgrades.
//
// Parameters:
//   - c: the IBM cluster; nil never matches.
//   - endpoint: spec.controlPlaneEndpoint; the zero value matches nothing.
//   - secretServer: the server recorded on the kubeconfig Secret, an https URL; "" matches nothing.
func EndpointMatches(c *kubernetesservice.Cluster, endpoint controlplanev1.APIEndpoint, secretServer string) bool {
	if c == nil {
		return false
	}
	recorded := map[string]bool{}
	if endpoint.Host != "" && endpoint.Port > 0 {
		recorded[strings.ToLower(bindingEndpoint(endpoint))] = true
	}
	if hostPort, ok := bindingURLHostPort(secretServer); ok {
		recorded[hostPort] = true
	}
	for _, ibmURL := range []string{c.MasterURL, c.PublicServiceEndpointURL, c.PrivateServiceEndpointURL} {
		if hostPort, ok := bindingURLHostPort(ibmURL); ok && recorded[hostPort] {
			return true
		}
	}
	return false
}

// bindingURLHostPort returns the lower-case host:port of an https URL, with 443 when the URL names no port. ok is
// false for "", a URL that does not parse, another scheme, or a URL without a host.
//
// Parameters:
//   - raw: the URL.
func bindingURLHostPort(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" {
		return "", false
	}
	port := u.Port()
	if port == "" {
		port = bindingDefaultPort
	}
	return net.JoinHostPort(strings.ToLower(u.Hostname()), port), true
}
