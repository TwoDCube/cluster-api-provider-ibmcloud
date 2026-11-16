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

// Package managed implements the validating admission webhooks of the IBM Cloud managed Kubernetes kinds
// (IBMCloudManagedCluster, IBMCloudManagedControlPlane, IBMCloudManagedMachinePool; IKS and ROKS on VPC Gen2).
//
// Why it exists: every rule that fits into a CRD schema is a CEL rule on the types (api/managed/...), where it is
// tested next to the fields it guards. Four rules do not fit there, so they live here:
//
//   - The feature gate. With the ManagedKubernetes gate off the controllers do not run, but the webhooks stay
//     registered and reject only CREATE, naming EXP_MANAGED_KUBERNETES. An unregistered webhook would give an opaque
//     "webhook not found" error and block updates and deletes of objects that already exist.
//   - The master minor step. spec.distributionVersion may rise by one minor at a time and never go below the master's
//     minor. The observed minor is in status.distributionVersion, written in IBM's version grammar
//     (pkg/util/ibmversion), which a CEL rule on spec can neither see nor parse.
//   - The ibmCloudClusterName warning. When spec.ibmCloudClusterName is omitted, the IBM cluster is named after the
//     Cluster, which is unique only per namespace, while IBM names are unique per account. CEL can only reject, never
//     warn, and the warning names the Cluster, which a control plane created from a template does not know yet (only
//     the Cluster's spec.controlPlaneRef tells).
//   - The delete guards of IBMCloudManagedControlPlane and IBMCloudManagedCluster. Whether a deletion is safe
//     depends on other objects: the owning Cluster and, for the InfraCluster, whether the Cluster's
//     spec.infrastructureRef still names it and the Cluster's control plane. A control plane deleted on its own would
//     delete the IBM cluster while the CAPI objects remain; an InfraCluster deleted on its own (it has no finalizer)
//     would take away the resource group and the credentials holder that the control plane needs to find and delete
//     its IBM cluster.
//
// Pool deletions are never rejected: blocking them is the regret CAPZ documented, and the pool controller drains
// before it removes workers.
//
// Conventions: CAPIBM's typed validators (controller-runtime admission.Validator[T]) registered with
// ctrl.NewWebhookManagedBy, one file per kind, and the CAPIBM marker style (path /validate-<group>-<version>-<kind>,
// failurePolicy=fail, sideEffects=None). There is no defaulter: the managed API has no defaults (no defaulted version
// either: spec.distributionVersion names only a minor, and IBM picks the patch; KAL forbids +kubebuilder:default), and
// "omitted means X" is resolved by the controllers. Dependencies are explicit fields of the validator structs, never
// globals: the gate value (so tests set it per object, and cmd/main.go passes feature.Gates once) and, for the two
// delete guards and the ibmCloudClusterName warning, a client.Reader. The reader must be the manager's API reader,
// because a cached read could miss a deletionTimestamp that was set a moment ago and because the webhook should not
// start informers of its own. The webhooks never call IBM Cloud, and when a read for a delete guard fails they reject
// the deletion (fail closed): the guards exist to prevent an unrecoverable IBM cluster deletion, so "allowed because
// the check could not run" would defeat them. The warning's lookup is the opposite: it is advice, so a failed list only
// makes the warning generic.
package managed
