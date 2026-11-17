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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmclustername"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// Ensure IBMCloudManagedControlPlane implements the typed validator interface. There is no defaulter (package doc).
var _ admission.Validator[*controlplanev1.IBMCloudManagedControlPlane] = &IBMCloudManagedControlPlane{}

//+kubebuilder:webhook:verbs=create;update;delete,path=/validate-controlplane-cluster-x-k8s-io-v1alpha1-ibmcloudmanagedcontrolplane,mutating=false,failurePolicy=fail,groups=controlplane.cluster.x-k8s.io,resources=ibmcloudmanagedcontrolplanes,versions=v1alpha1,name=vibmcloudmanagedcontrolplane.kb.io,sideEffects=None,admissionReviewVersions=v1;v1beta1

// IBMCloudManagedControlPlane validates IBMCloudManagedControlPlane objects: on create the feature gate and the IBM
// cluster name (IBM's rule, and a warning when the name defaults to the Cluster's), on update the master minor step,
// and on delete the delete guard. It never calls IBM Cloud.
type IBMCloudManagedControlPlane struct {
	// ManagedKubernetesEnabled is the value of the ManagedKubernetes feature gate. When false, CREATE is rejected and
	// every other operation is allowed unchecked: no controller reconciles the object, and the controller repeats the
	// minor-step check once the gate is on again.
	ManagedKubernetesEnabled bool

	// Reader reads the owning Cluster for the delete guard and, on create, lists the Clusters of the namespace to find
	// the Cluster whose name becomes the IBM cluster name when spec.ibmCloudClusterName is omitted. It must be the
	// manager's API reader (mgr.GetAPIReader()), see the package documentation; nil is a construction error.
	Reader client.Reader
}

// SetupWebhookWithManager registers the validator with mgr's webhook server. It fails when Reader is nil, so that a
// wiring mistake stops the manager at startup instead of failing admission requests.
//
// mgr is the manager whose webhook server serves the validator.
func (r *IBMCloudManagedControlPlane) SetupWebhookWithManager(mgr ctrl.Manager) error {
	if err := requireReader("IBMCloudManagedControlPlane", r.Reader); err != nil {
		return err
	}
	return ctrl.NewWebhookManagedBy(mgr, &controlplanev1.IBMCloudManagedControlPlane{}).
		WithValidator(r).
		Complete()
}

// ValidateCreate rejects the create while the feature gate is off. Otherwise it checks the IBM cluster name the
// control plane will use against IBM's rule (ibmclustername.Problem), because IBM refuses createCluster for any other
// name (HTTP 400, code E0125) and the name cannot be changed once the control plane exists:
//   - spec.ibmCloudClusterName, when set, must follow the rule. The CRD schema checks the same rule before any
//     validating webhook runs; this check uses the same validator and rule text as the controller
//     (ibmclustername.Problem and Rule), so the webhook's answer stays right on its own if the schema is older.
//   - When it is omitted, the IBM name is the Cluster's metadata.name. A Cluster name is a DNS name that IBM may refuse
//     (longer than 32 characters, or a digit first), so when the Cluster is known the create is rejected with the
//     advice to set spec.ibmCloudClusterName. A valid Cluster name is allowed with a warning, because it is unique
//     only per namespace while IBM names are unique per account. When the Cluster is not known yet, the warning is
//     generic and the controller checks the name before it calls IBM.
//
// ctx is the admission request's context, used only to find the Cluster that names the IBM cluster; cp is the
// control plane being created.
func (r *IBMCloudManagedControlPlane) ValidateCreate(ctx context.Context, cp *controlplanev1.IBMCloudManagedControlPlane) (admission.Warnings, error) {
	if !r.ManagedKubernetesEnabled {
		return nil, gateOffCreateError(controlPlaneResource, "IBMCloudManagedControlPlane", cp.Name)
	}
	if cp.Spec.IBMCloudClusterName != "" {
		if err := validateIBMCloudClusterName(cp.Spec.IBMCloudClusterName); err != nil {
			return nil, apierrors.NewInvalid(controlPlaneGroupKind, cp.Name, field.ErrorList{err})
		}
		return nil, nil
	}
	clusterName := r.owningClusterName(ctx, cp)
	if err := validateDefaultedClusterName(clusterName); err != nil {
		return nil, apierrors.NewInvalid(controlPlaneGroupKind, cp.Name, field.ErrorList{err})
	}
	return admission.Warnings{ibmCloudClusterNameWarning(clusterName)}, nil
}

// validateIBMCloudClusterName checks an explicit spec.ibmCloudClusterName against IBM's cluster name rule. It returns
// nil when the name follows the rule.
//
// name is the non-empty spec.ibmCloudClusterName of the control plane being created.
func validateIBMCloudClusterName(name string) *field.Error {
	problem := ibmclustername.Problem(name)
	if problem == "" {
		return nil
	}
	return field.Invalid(field.NewPath("spec", "ibmCloudClusterName"), name, fmt.Sprintf(
		"IBM would refuse to create a cluster with this name: it %s; %s", problem, ibmclustername.Rule))
}

// validateDefaultedClusterName checks the Cluster name that becomes the IBM cluster name when spec.ibmCloudClusterName
// is omitted. It returns nil when the name follows IBM's rule or is not known. The error is a Required error on
// spec.ibmCloudClusterName, because setting that field is the fix: the Cluster's name is immutable, and the field
// cannot be added after the control plane is created.
//
// clusterName is the Cluster's metadata.name (owningClusterName); "" when the Cluster is not known yet.
func validateDefaultedClusterName(clusterName string) *field.Error {
	if clusterName == "" {
		return nil
	}
	problem := ibmclustername.Problem(clusterName)
	if problem == "" {
		return nil
	}
	return field.Required(field.NewPath("spec", "ibmCloudClusterName"), fmt.Sprintf(
		"without it the IBM cluster would be named after Cluster %q, and IBM would refuse that name: it %s; %s; set "+
			"spec.ibmCloudClusterName to a name that follows this rule (it cannot be added after the control plane is "+
			"created)", clusterName, problem, ibmclustername.Rule))
}

// ValidateUpdate checks the master minor step, and only when spec.distributionVersion changed: an unrelated
// update must never be blocked because status moved after the version was admitted, for example while IBM applied
// the previous update. With the feature gate off every update is allowed.
//
// ctx is unused; oldCP is the stored object, whose status.distributionVersion is the observed master version; newCP is
// the object as the request would store it.
func (r *IBMCloudManagedControlPlane) ValidateUpdate(_ context.Context, oldCP, newCP *controlplanev1.IBMCloudManagedControlPlane) (admission.Warnings, error) {
	if !r.ManagedKubernetesEnabled {
		return nil, nil
	}
	if oldCP.Spec.DistributionVersion == newCP.Spec.DistributionVersion {
		return nil, nil
	}
	if err := validateMinorStep(oldCP.Status.DistributionVersion, newCP.Spec.DistributionVersion); err != nil {
		return nil, apierrors.NewInvalid(controlPlaneGroupKind, newCP.Name, field.ErrorList{err})
	}
	return nil, nil
}

// ValidateDelete is the delete guard of IBMCloudManagedControlPlane: deleting the control plane deletes the IBM
// cluster with every worker pool, so a control plane deleted on its own (kubectl, a GitOps prune or a forced replace)
// would destroy the cluster while the CAPI objects remain. The deletion is allowed when the gate is off, when
// clusterctl move deletes the object (delete-for-move), when no IBM cluster can exist yet, and when the owning
// Cluster is gone or being deleted; otherwise it is rejected with "delete Cluster <name> instead". The checks that
// need no read run first, so most allowed deletions read nothing.
//
// ctx is the admission request's context, used for the Cluster read; cp is the control plane being deleted.
func (r *IBMCloudManagedControlPlane) ValidateDelete(ctx context.Context, cp *controlplanev1.IBMCloudManagedControlPlane) (admission.Warnings, error) {
	if !r.ManagedKubernetesEnabled || isBeingMoved(cp) || noIBMClusterCanExist(cp) {
		return nil, nil
	}
	if err := requireReader("IBMCloudManagedControlPlane", r.Reader); err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	cluster, err := liveOwnerCluster(ctx, r.Reader, cp, "IBMCloudManagedControlPlane")
	if err != nil || cluster == nil {
		return nil, err
	}
	return nil, apierrors.NewForbidden(controlPlaneResource, cp.Name, fmt.Errorf(
		"delete Cluster %s instead; deleting the control plane alone would delete the IBM cluster", cluster.Name))
}

// validateMinorStep checks a new spec.distributionVersion against the master's observed minor: it may not go below
// that minor, because IBM cannot downgrade a master, and may be at most one minor above it, because IBM refuses
// larger skips and CAPIBM runs no unattended upgrade chains. It returns nil when the step is allowed.
//
// observed is the stored status.distributionVersion in IBM's grammar ("1.36.5_1537", "4.21.35_1537_openshift"); empty
// means no master was observed yet, so there is nothing to step from and every value passes. desired is the new
// spec.distributionVersion (MAJOR.MINOR, already checked by CEL). An observed value that does not parse is rejected
// rather than skipped: the controller writes only values that parse (IBM's version without the " (pending)" marker),
// so an unparsable one was written by hand, and letting a version change through unchecked could start a two-minor
// skip.
func validateMinorStep(observed, desired string) *field.Error {
	path := field.NewPath("spec", "distributionVersion")
	if observed == "" {
		return nil
	}
	want, err := ibmversion.ParseMinor(desired)
	if err != nil {
		return field.Invalid(path, desired, err.Error())
	}
	current, err := ibmversion.Parse(observed)
	if err != nil {
		return field.Invalid(path, desired, fmt.Sprintf("cannot check the minor step against status.distributionVersion "+
			"%q (%v); the controller rewrites status.distributionVersion from IBM on its next poll, retry the change "+
			"after that", observed, err))
	}
	currentMinor := current.MinorVersion()
	// TODO: the highest allowed value and its message assume that the master's next version is its next minor. After
	// OpenShift 4.22 a master can move to 4.23 or to 5.0; once IBM's per-cluster upgrade-path endpoint is stable, allow
	// the versions it lists for the cluster instead (see the TODO on ibmversion.Minor.Next).
	switch {
	case want.Compare(currentMinor) < 0:
		return field.Invalid(path, desired, fmt.Sprintf("a master cannot be downgraded: it runs %s "+
			"(status.distributionVersion %s), so the lowest allowed value is %s", currentMinor, observed, currentMinor))
	case want.Compare(currentMinor.Next()) > 0:
		return field.Invalid(path, desired, fmt.Sprintf("the master moves up one minor at a time: it runs %s "+
			"(status.distributionVersion %s), so the highest allowed value is %s; raise it again once "+
			"status.distributionVersion reports %s", currentMinor, observed, currentMinor.Next(), currentMinor.Next()))
	}
	return nil
}

// owningClusterName returns the name of the Cluster that cp belongs to, whose metadata.name becomes the IBM cluster
// name when spec.ibmCloudClusterName is omitted, or "" when it cannot be told. The cluster-name label or the Cluster
// owner reference answers without a read (clusterNameOf). A control plane created from a clusterctl template has
// neither yet (the template sets only a name, and CAPI adds the owner reference when its Cluster controller first
// reconciles), so the Clusters of the namespace are listed and the one whose spec.controlPlaneRef names cp is taken;
// kubectl apply of a template creates the Cluster first, because it is the first document. The lookup itself never
// fails the request: a list error, no match, or more than one match (the owner is then not known) gives "", which
// leaves the name check to the controller and makes the warning generic.
//
// ctx is the admission request's context, used for the list and its logger; cp is the control plane being created.
func (r *IBMCloudManagedControlPlane) owningClusterName(ctx context.Context, cp *controlplanev1.IBMCloudManagedControlPlane) string {
	if name := clusterNameOf(cp); name != "" {
		return name
	}
	// A nil Reader is a construction error that SetupWebhookWithManager reports; a request never reaches it in a
	// manager, so it only guards direct calls. A Cluster cannot reference a control plane without a name.
	if r.Reader == nil || cp.Name == "" {
		return ""
	}
	clusters := &clusterv1.ClusterList{}
	if err := r.Reader.List(ctx, clusters, client.InNamespace(cp.Namespace)); err != nil {
		ctrl.LoggerFrom(ctx).V(4).Info("Cannot list Clusters to find the Cluster that names the IBM cluster; the "+
			"controller checks the name, and the ibmCloudClusterName warning uses the generic text",
			"namespace", cp.Namespace, "error", err.Error())
		return ""
	}
	name := ""
	for i := range clusters.Items {
		if !refersTo(clusters.Items[i].Spec.ControlPlaneRef, controlPlaneGroupKind, cp.Name) {
			continue
		}
		if name != "" {
			return ""
		}
		name = clusters.Items[i].Name
	}
	return name
}

// ibmCloudClusterNameWarning is the admission warning for a control plane without spec.ibmCloudClusterName. Its
// middle part says what the omission means ("the IBM cluster will be named <Cluster name>; IBM names are unique per
// account, so other namespaces or management clusters using the same name will conflict"); the prefix names the
// omitted field and the suffix tells the user how to choose a name.
//
// clusterName is the name of the Cluster that the control plane belongs to (owningClusterName); when it is empty,
// the text says that the name comes from the Cluster instead of naming it.
func ibmCloudClusterNameWarning(clusterName string) string {
	target := clusterName
	if target == "" {
		target = "after its Cluster (the Cluster's metadata.name)"
	}
	return fmt.Sprintf("spec.ibmCloudClusterName is omitted: the IBM cluster will be named %s; IBM names are unique "+
		"per account, so other namespaces or management clusters using the same name will conflict; set "+
		"spec.ibmCloudClusterName (immutable) to choose a name", target)
}
