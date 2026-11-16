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

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file turns one GetCluster answer into what the control plane controller acts on and reports: the class of
// IBM's cluster state and the stable status tokens. Both are pure.

// ClusterStateClass is what an IBM cluster state means for the control plane controller.
type ClusterStateClass string

const (
	// ClusterStateProvisioning is a cluster IBM is still creating (requested, deploying): NotDone(Creating), polled
	// every clusterProvisioningPoll.
	ClusterStateProvisioning ClusterStateClass = "Provisioning"
	// ClusterStateExists is a cluster that exists: the normal flow continues. Every unknown state is in this class, so
	// a state IBM adds later never stops a working cluster (forward compatible).
	ClusterStateExists ClusterStateClass = "Exists"
	// ClusterStateFailed is a create that failed (create_failed, deploy_failed): Terminal(CreateFailed); CAPIBM never
	// deletes the cluster on its own.
	ClusterStateFailed ClusterStateClass = "Failed"
	// ClusterStateGoneOutOfBand is a cluster being deleted or gone (deleting, deleted, aborted, delete_failed) while
	// CAPI is not deleting it: Terminal(DeletedOutOfBand). The delete flow handles these states itself.
	ClusterStateGoneOutOfBand ClusterStateClass = "GoneOutOfBand"
)

// The IBM cluster states as IBM documents them
// (https://cloud.ibm.com/docs/containers?topic=containers-cluster-states-reference). assumptions.go names four of them
// again (clusterStateUpdating and others) next to the other IBM decision points, for the pool and master update
// guards; these constants serve the classification.
const (
	ibmStateRequested      = "requested"
	ibmStateDeploying      = "deploying"
	ibmStateDeployed       = "deployed"
	ibmStatePending        = "pending"
	ibmStateNormal         = "normal"
	ibmStateWarning        = "warning"
	ibmStateCritical       = "critical"
	ibmStateError          = "error"
	ibmStateUnavailable    = "unavailable"
	ibmStateScaledDown     = "scaled_down"
	ibmStateUpdating       = "updating"
	ibmStateUpdateFailed   = "update_failed"
	ibmStateUpdateCanceled = "update_canceled"
	ibmStateUnsupported    = "unsupported"
	ibmStateCreateFailed   = "create_failed"
	ibmStateDeployFailed   = "deploy_failed"
	ibmStateDeleting       = "deleting"
	ibmStateDeleted        = "deleted"
	ibmStateAborted        = "aborted"
	ibmStateDeleteFailed   = "delete_failed"
)

// ClassifyClusterState returns the class of an IBM cluster state. Observed on IBM Cloud: the create sequence
// deploying -> deployed -> pending -> normal on both flavors; the master is usable from deployed. Not yet verified
// against IBM Cloud: the update and failure states are IBM's documented tokens, not observed yet; fallback: adjust
// this table. A state that is not in the table is ClusterStateExists; KnownClusterState tells the caller to name it
// in a message.
//
// Parameters:
//   - state: IBM's cluster state (kubernetesservice.Cluster.State).
func ClassifyClusterState(state string) ClusterStateClass {
	switch state {
	case ibmStateRequested, ibmStateDeploying:
		return ClusterStateProvisioning
	case ibmStateCreateFailed, ibmStateDeployFailed:
		return ClusterStateFailed
	case ibmStateDeleting, ibmStateDeleted, ibmStateAborted, ibmStateDeleteFailed:
		return ClusterStateGoneOutOfBand
	default:
		return ClusterStateExists
	}
}

// KnownClusterState reports whether state is one of the cluster states IBM documents. An unknown state is still
// ClusterStateExists; the caller names it in a condition message ("unknown IBM state <x>") so that a new IBM token is
// noticed. The update and failure states are not yet verified against IBM Cloud, as for ClassifyClusterState.
//
// Parameters:
//   - state: IBM's cluster state.
func KnownClusterState(state string) bool {
	switch state {
	case ibmStateRequested, ibmStateDeploying, ibmStateDeployed, ibmStatePending, ibmStateNormal, ibmStateWarning,
		ibmStateCritical, ibmStateError, ibmStateUnavailable, ibmStateScaledDown, ibmStateUpdating, ibmStateUpdateFailed,
		ibmStateUpdateCanceled, ibmStateUnsupported, ibmStateCreateFailed, ibmStateDeployFailed, ibmStateDeleting,
		ibmStateDeleted, ibmStateAborted, ibmStateDeleteFailed:
		return true
	default:
		return false
	}
}

// The MaxLength of each status field ObserveCluster fills (api/managed/controlplane/v1alpha1). A longer IBM value would
// make the API server reject the whole status patch, so ObserveCluster leaves such a value out instead.
const (
	// observedTokenMaxLength bounds status.state, status.master.state and status.master.health.
	observedTokenMaxLength = 32
	// observedVersionMaxLength bounds status.distributionVersion and status.targetDistributionVersion.
	observedVersionMaxLength = 64
	// observedEndOfSupportMaxLength bounds status.distributionVersionEndOfSupport.
	observedEndOfSupportMaxLength = 32
	// observedCRNMaxLength bounds status.clusterCRN.
	observedCRNMaxLength = 512
)

// ObservedCluster is the part of IBMCloudManagedControlPlane status that one GetCluster answer determines: stable IBM
// tokens only, never IBM free text such as masterStatus. An empty field means IBM reported nothing usable for it;
// ApplyTo then omits the field, never writing "", which the status string fields do not accept.
type ObservedCluster struct {
	// State is IBM's cluster state token, for status.state.
	State string
	// MasterState is lifecycle.masterState, for status.master.state.
	MasterState string
	// MasterHealth is lifecycle.masterHealth, for status.master.health; "" for the first minutes after the master
	// is Ready, as observed on IBM Cloud.
	MasterHealth string
	// DistributionVersion is masterKubeVersion without IBM's " (pending)" marker, for status.distributionVersion,
	// which never carries the marker.
	DistributionVersion string
	// TargetDistributionVersion is targetVersion without the marker, for status.targetDistributionVersion.
	TargetDistributionVersion string
	// DistributionVersionEndOfSupport is versionEOS as IBM reports it, for status.distributionVersionEndOfSupport.
	DistributionVersionEndOfSupport string
	// ClusterCRN is the cluster CRN, for status.clusterCRN; its account segment builds worker provider IDs.
	ClusterCRN string
}

// ObserveCluster derives the control plane's status tokens from one GetCluster answer. Every value taken from
// masterKubeVersion or targetVersion goes through ibmversion.Version.WithoutPending().String(), so IBM's " (pending)"
// marker never reaches status; the master's minor that DecideMasterAction compares is parsed from the same value.
//
// Parameters:
//   - c: the cluster as GetCluster returned it.
//
// It returns an error when c is nil. When IBM reports a version that does not parse, or a value longer than its
// status field allows, that field is left empty and the error names the value; every other field is still valid, and
// the caller applies the result and reports the error (the form of the " (pending)" marker is not yet verified
// against IBM Cloud, so a parser tweak may be due).
func ObserveCluster(c *kubernetesservice.Cluster) (ObservedCluster, error) {
	if c == nil {
		return ObservedCluster{}, errors.New("ObserveCluster needs the cluster GetCluster returned; got nil")
	}
	var errs []error
	keep := func(field, value string, maxLength int) string {
		if len(value) > maxLength {
			errs = append(errs, fmt.Errorf("IBM reports %s %q, longer than the %d characters its status field allows; "+
				"it is not recorded", field, value, maxLength))
			return ""
		}
		return value
	}
	version := func(field, value string) string {
		if value == "" {
			return ""
		}
		parsed, err := ibmversion.Parse(value)
		if err != nil {
			errs = append(errs, fmt.Errorf("IBM reports %s %q, which is not an IBM version; it is not recorded: %w",
				field, value, err))
			return ""
		}
		return keep(field, parsed.WithoutPending().String(), observedVersionMaxLength)
	}
	observed := ObservedCluster{
		State:                           keep("state", c.State, observedTokenMaxLength),
		MasterState:                     keep("masterState", c.MasterState, observedTokenMaxLength),
		MasterHealth:                    keep("masterHealth", c.MasterHealth, observedTokenMaxLength),
		DistributionVersion:             version("masterKubeVersion", c.MasterKubeVersion),
		TargetDistributionVersion:       version("targetVersion", c.TargetVersion),
		DistributionVersionEndOfSupport: keep("versionEOS", c.VersionEOS, observedEndOfSupportMaxLength),
		ClusterCRN:                      keep("crn", c.CRN, observedCRNMaxLength),
	}
	return observed, errors.Join(errs...)
}

// ApplyTo writes o into a control plane status. The tokens that IBM may legitimately clear (state, master state and
// health, end of support) follow IBM, and an empty one is omitted. The master versions and the CRN are only written
// when o has a value: IBM never takes a master version or a CRN back, so an empty value comes from a failed parse or a
// cluster that is still being requested, and a failed read never clears a status field.
//
// Parameters:
//   - status: the status to update; nil is ignored.
func (o ObservedCluster) ApplyTo(status *controlplanev1.IBMCloudManagedControlPlaneStatus) {
	if status == nil {
		return
	}
	status.State = o.State
	status.Master = controlplanev1.MasterStatus{State: o.MasterState, Health: o.MasterHealth}
	status.DistributionVersionEndOfSupport = o.DistributionVersionEndOfSupport
	if o.DistributionVersion != "" {
		status.DistributionVersion = o.DistributionVersion
	}
	if o.TargetDistributionVersion != "" {
		status.TargetDistributionVersion = o.TargetDistributionVersion
	}
	if o.ClusterCRN != "" {
		status.ClusterCRN = o.ClusterCRN
	}
}
