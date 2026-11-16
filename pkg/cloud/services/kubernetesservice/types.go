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
	"fmt"
	"strings"
)

// The domain types below are CAPIBM's view of the IBM Cloud Kubernetes Service API. Every field was checked against
// the SDK structs at commit 61af133 and against the live fixtures in testdata/. They carry no IBM free text that a
// controller would store in status: MasterStatus feeds readiness checks and messages only, and IBMMessage feeds
// events only.

// Cluster is one IKS or ROKS cluster on VPC Gen2 as GetCluster reports it.
type Cluster struct {
	// ID is IBM's cluster ID, for example "c0abcdefghijklmnopqr" (20 characters).
	ID string
	// Name is the cluster name, unique per account and region.
	Name string
	// CRN is the cluster CRN; its account segment yields the account ID of the Nodes' provider IDs.
	CRN string
	// Type is "kubernetes" (IKS) or "openshift" (ROKS).
	Type string
	// Provider is "vpc-gen2" for every cluster CAPIBM manages.
	Provider string
	// ResourceGroupID is the ID of the cluster's resource group.
	ResourceGroupID string
	// CreatedDate is IBM's raw creation time, for example "2026-11-16T03:05:33+0000" (observed on IBM Cloud). It is
	// kept raw because the binding rules parse it with the layouts of assumptions.CreatedDateLayouts.
	CreatedDate string
	// State is the cluster state, for example "deploying", "deployed", "pending", "normal", "deleting".
	State string
	// MasterState is lifecycle.masterState, for example "deploying" or "deployed".
	MasterState string
	// MasterHealth is lifecycle.masterHealth; it was observed to stay "" for minutes after the master is ready.
	MasterHealth string
	// MasterStatus is lifecycle.masterStatus with surrounding white space removed, for example "Ready" or "Deploy in
	// progress.". It feeds IsMasterReady and messages only, never status fields.
	MasterStatus string
	// MasterKubeVersion is the master version, for example "1.36.5_1537" or "4.21.35_1537_openshift".
	MasterKubeVersion string
	// TargetVersion is the version IBM is updating the master to; it equals MasterKubeVersion when no update runs.
	TargetVersion string
	// VersionEOS is the end-of-service date of the master version; "" when IBM reports none.
	VersionEOS string
	// MasterURL is the URL CAPIBM talks to; "" until the master is deployed.
	MasterURL string
	// PublicServiceEndpointURL is the public service endpoint URL; "" until deployed or when disabled.
	PublicServiceEndpointURL string
	// PrivateServiceEndpointURL is the private service endpoint URL; "" until deployed.
	PrivateServiceEndpointURL string
	// CARotationStatus is caCertRotationStatus.status; "" when no rotation ever ran (its other values are not yet
	// verified against IBM Cloud).
	CARotationStatus string
	// CARotationCompletedDate is caCertRotationStatus.actionCompletedDate; "" when no rotation ever completed.
	CARotationCompletedDate string
	// PodSubnet is the pod CIDR, for example "172.17.0.0/18".
	PodSubnet string
	// ServiceSubnet is the service CIDR, for example "172.21.0.0/16".
	ServiceSubnet string
}

// CreateClusterRequest is the input of CreateCluster. CreateCluster validates it before sending anything.
type CreateClusterRequest struct {
	// Name is the IBM cluster name. Required.
	Name string
	// ResourceGroupID is the resource group the cluster is created in, sent as X-Auth-Resource-Group. Required: the
	// SDK marks the header as required and CAPIBM always knows the group (IBMCloudManagedCluster spec.resourceGroup).
	ResourceGroupID string
	// KubeVersion is the minor to create, for example "1.36" or "4.21_openshift"; IBM picks the newest patch (observed
	// on IBM Cloud). Required.
	KubeVersion string
	// DisablePublicServiceEndpoint creates a cluster with only the private service endpoint. Always sent.
	DisablePublicServiceEndpoint bool
	// PodSubnet is the pod CIDR; "" lets IBM choose its default.
	PodSubnet string
	// ServiceSubnet is the service CIDR; "" lets IBM choose its default.
	ServiceSubnet string
	// COSInstanceCRN is the Cloud Object Storage instance that backs the ROKS internal registry. ROKS on VPC requires
	// it (IBM refuses the create with E7278 without it; enforced by CEL); "" for IKS.
	COSInstanceCRN string
	// DefaultWorkerPoolEntitlement is "" or "ocp_entitled" (ROKS with an OpenShift entitlement).
	DefaultWorkerPoolEntitlement string
	// WorkerPool is the cluster's initial worker pool. Required.
	WorkerPool InitialWorkerPool
}

// CreateClusterResult is what a successful CreateCluster returns.
type CreateClusterResult struct {
	// ClusterID is the ID of the new cluster; never "" (CreateCluster reports a 201 without an ID as an error).
	ClusterID string
	// IncidentID is the incident ID IBM attached to NonCriticalErrors; "" when there are none.
	IncidentID string
	// NonCriticalErrors are the items of IBM's non_critical_errors: the cluster was created, but IBM could not
	// complete something, for example the registry access policy (code E49df). They feed events and condition
	// messages; they are never a failure.
	NonCriticalErrors []IBMMessage
}

// IBMMessage is one item of an IBM message list. Description and RecoveryCLI are IBM free text: they go into events
// only and are never stored in status.
type IBMMessage struct {
	// Code is IBM's message code, for example "E49df".
	Code string
	// Type is IBM's message type, for example "Provisioning".
	Type string
	// Description is IBM's explanation.
	Description string
	// RecoveryCLI is IBM's suggested remedy.
	RecoveryCLI string
}

// InitialWorkerPool is the worker pool CreateCluster creates together with the cluster.
type InitialWorkerPool struct {
	// Name is the pool name; "" lets IBM name it "default".
	Name string
	// Flavor is the worker flavor, for example "bx2.4x16". Required.
	Flavor string
	// VPCID is the VPC the workers attach to. Required.
	VPCID string
	// WorkerCountPerZone is the number of workers in each zone. Required, at least 1.
	WorkerCountPerZone int64
	// Zones lists the zones and the subnet of each. Required, at least one, no zone twice.
	Zones []ZoneSubnet
}

// ZoneSubnet pairs a zone with the VPC subnet the workers of that zone use.
type ZoneSubnet struct {
	// Zone is the VPC zone, for example "eu-de-1".
	Zone string
	// SubnetID is the ID of a VPC subnet in Zone.
	SubnetID string
}

// Versions are the versions IBM offers for new clusters and master updates.
type Versions struct {
	// Kubernetes are the IKS versions.
	Kubernetes []ReleaseVersion
	// OpenShift are the ROKS versions; Major and Minor are OpenShift's, for example 4.21.
	OpenShift []ReleaseVersion
}

// ReleaseVersion is one offered version.
type ReleaseVersion struct {
	// Major is the major version.
	Major int
	// Minor is the minor version.
	Minor int
	// Patch is the newest patch IBM offers for this minor.
	Patch int
	// Default is true for the version IBM uses when none is requested.
	Default bool
	// EndOfService is IBM's end-of-service date, for example "2027-01-20T23:59:59+0000"; "" when none is announced.
	EndOfService string
}

// AdminKubeconfig holds the parts of IBM's admin kubeconfig that CAPIBM uses. KeyPEM is a cluster-admin private key:
// never log it; String and GoString redact it.
type AdminKubeconfig struct {
	// Server is the kubeconfig's server, which equals the cluster's masterURL (observed on IBM Cloud).
	Server string
	// CertificateAuthorityPEM is the cluster CA from certificate-authority-data. IKS: the cluster CA; ROKS: nil,
	// because the ROKS master serves a publicly trusted certificate (observed on IBM Cloud).
	CertificateAuthorityPEM []byte
	// CertificatePEM is client-certificate-data: IBM's admin client certificate (O=ibm-admins, valid two years).
	CertificatePEM []byte
	// KeyPEM is client-key-data: the private key of CertificatePEM.
	KeyPEM []byte
}

// String describes k without its private key, so that a stray log line cannot leak it.
func (k AdminKubeconfig) String() string {
	return fmt.Sprintf("AdminKubeconfig{Server: %q, CertificateAuthorityPEM: %d bytes, CertificatePEM: %d bytes, KeyPEM: <redacted>}",
		k.Server, len(k.CertificateAuthorityPEM), len(k.CertificatePEM))
}

// GoString redacts the private key in %#v output as well.
func (k AdminKubeconfig) GoString() string {
	return k.String()
}

// CACertificate is the cluster CA as getCACert returns it.
type CACertificate struct {
	// PEM is the CA certificate bundle, decoded from IBM's base64 caCert field.
	PEM []byte
	// RotatedAt is IBM's rotatedAt value as text; "" when IBM sends none.
	RotatedAt string
}

// CreateWorkerPoolRequest is the input of CreateWorkerPool. CreateWorkerPool validates it before sending anything.
type CreateWorkerPoolRequest struct {
	// ClusterID is the IBM cluster ID. Required.
	ClusterID string
	// Name is the pool name (IBMCloudManagedMachinePool spec.poolName). Required.
	Name string
	// Flavor is the worker flavor, for example "bx2.4x16". Required.
	Flavor string
	// VPCID is the VPC the workers attach to. Required.
	VPCID string
	// WorkerCountPerZone is the number of workers in each zone. Required, at least 1.
	WorkerCountPerZone int64
	// Zones lists the zones and the subnet of each; they are created with the pool in the same call, or added one by
	// one right after it when the zone-by-zone fallback is switched on (CreatePoolWithZones). Required, at least one, no
	// zone twice.
	Zones []ZoneSubnet
	// Entitlement is "" or "ocp_entitled" (ROKS with an OpenShift entitlement).
	Entitlement string
}

// WorkerPool is one worker pool as ListWorkerPools reports it.
type WorkerPool struct {
	// ID is IBM's pool ID, "<clusterID>-<7 hex characters>" (observed on IBM Cloud).
	ID string
	// Name is the pool name.
	Name string
	// Flavor is the worker flavor.
	Flavor string
	// ActualState is lifecycle.actualState; "" on a steady pool, "deleting" or "deleted" while it goes.
	ActualState string
	// SizePerZone is the pool's workerCount: the desired workers per zone.
	SizePerZone int64
	// Zones are the pool's zones.
	Zones []WorkerPoolZone
	// AutoscaleEnabled is true when IBM's cluster autoscaler owns the pool size; CAPIBM then never resizes it.
	AutoscaleEnabled bool
	// IsBalanced is IBM's balance flag.
	IsBalanced bool
	// OperatingSystem is informational, for example "UBUNTU_24_64" or "RHCOS".
	OperatingSystem string
}

// WorkerPoolZone is one zone of a worker pool.
type WorkerPoolZone struct {
	// ID is the zone, for example "eu-de-1".
	ID string
	// WorkerCount is IBM's current worker count in the zone; it was observed to lag the pool's SizePerZone after create.
	WorkerCount int64
	// AutobalanceEnabled gates worker replacement; SDK WorkerPoolZoneResponse.AutobalanceEnabled.
	AutobalanceEnabled bool
}

// Worker is one worker as ListWorkers reports it.
type Worker struct {
	// ID is IBM's worker ID; the Node's provider ID ends with it.
	ID string
	// PoolID is the ID of the worker's pool.
	PoolID string
	// Zone is the worker's zone (IBM's location).
	Zone string
	// ActualState is lifecycle.actualState, for example "provisioning", "deployed", "deleting", "deleted".
	ActualState string
	// PendingOperation is lifecycle.pendingOperation; "" on every steady worker observed on IBM Cloud.
	PendingOperation string
	// HealthState is health.state, for example "normal".
	HealthState string
	// KubeVersionActual is the version the worker runs, for example "1.36.5_1540"; "" while provisioning.
	KubeVersionActual string
	// KubeVersionTarget is the version IBM would move the worker to.
	KubeVersionTarget string
	// KubeVersionDesired is the version IBM provisions the worker with.
	KubeVersionDesired string
}

// entitlementOCPEntitled is the one worker-pool entitlement besides none: the customer brings its own OpenShift
// license (ROKS only).
const entitlementOCPEntitled = "ocp_entitled"

// validate checks r before CreateCluster sends it. The error names every missing or malformed field and the accepted
// form, and wraps ErrInvalidRequest.
func (r CreateClusterRequest) validate() error {
	var problems []string
	if r.Name == "" {
		problems = append(problems, "Name is empty; pass the IBM cluster name")
	}
	if r.ResourceGroupID == "" {
		problems = append(problems, "ResourceGroupID is empty; pass the ID of the cluster's resource group (IBMCloudManagedCluster spec.resourceGroup), which IBM requires as X-Auth-Resource-Group")
	}
	if r.KubeVersion == "" {
		problems = append(problems, `KubeVersion is empty; pass a minor such as "1.36" or "4.21_openshift"`)
	}
	if r.DefaultWorkerPoolEntitlement != "" && r.DefaultWorkerPoolEntitlement != entitlementOCPEntitled {
		problems = append(problems, fmt.Sprintf("DefaultWorkerPoolEntitlement %q is not supported; pass \"\" or %q", r.DefaultWorkerPoolEntitlement, entitlementOCPEntitled))
	}
	problems = append(problems, r.WorkerPool.problems()...)
	if len(problems) == 0 {
		return nil
	}
	return invalidRequest(OpCreateCluster, strings.Join(problems, "; "))
}

// validate checks r before CreateWorkerPool sends it. The error names every missing or malformed field and the
// accepted form, and wraps ErrInvalidRequest.
func (r CreateWorkerPoolRequest) validate() error {
	var problems []string
	if r.ClusterID == "" {
		problems = append(problems, "ClusterID is empty; pass status.clusterID of the IBMCloudManagedControlPlane")
	}
	if r.Name == "" {
		problems = append(problems, "Name is empty; pass the pool name (IBMCloudManagedMachinePool spec.poolName)")
	}
	if r.Flavor == "" {
		problems = append(problems, "Flavor is empty; pass a VPC worker flavor such as bx2.4x16")
	}
	if r.VPCID == "" {
		problems = append(problems, "VPCID is empty; pass the ID of the VPC the workers attach to")
	}
	if r.WorkerCountPerZone < 1 {
		problems = append(problems, fmt.Sprintf("WorkerCountPerZone is %d; pass at least 1", r.WorkerCountPerZone))
	}
	if r.Entitlement != "" && r.Entitlement != entitlementOCPEntitled {
		problems = append(problems, fmt.Sprintf("Entitlement %q is not supported; pass \"\" or %q", r.Entitlement, entitlementOCPEntitled))
	}
	problems = append(problems, zoneProblems("Zones", r.Zones)...)
	if len(problems) == 0 {
		return nil
	}
	return invalidRequest(OpCreateWorkerPool, strings.Join(problems, "; "))
}

// problems lists what is missing or malformed in p, each with the accepted form.
func (p InitialWorkerPool) problems() []string {
	var problems []string
	if p.Flavor == "" {
		problems = append(problems, "WorkerPool.Flavor is empty; pass a VPC worker flavor such as bx2.4x16")
	}
	if p.VPCID == "" {
		problems = append(problems, "WorkerPool.VPCID is empty; pass the ID of the VPC the workers attach to")
	}
	if p.WorkerCountPerZone < 1 {
		problems = append(problems, fmt.Sprintf("WorkerPool.WorkerCountPerZone is %d; pass at least 1", p.WorkerCountPerZone))
	}
	return append(problems, zoneProblems("WorkerPool.Zones", p.Zones)...)
}

// zoneProblems lists what is missing or malformed in a zone list: it must name at least one zone, each zone once,
// each with a subnet.
//
// Parameters:
//   - field is the name of the list in messages, for example "WorkerPool.Zones".
//   - zones is the list.
func zoneProblems(field string, zones []ZoneSubnet) []string {
	if len(zones) == 0 {
		return []string{field + " is empty; pass at least one zone with the ID of its subnet"}
	}
	var problems []string
	seen := make(map[string]bool, len(zones))
	for i, zone := range zones {
		if zone.Zone == "" {
			problems = append(problems, fmt.Sprintf("%s[%d].Zone is empty; pass a VPC zone such as eu-de-1", field, i))
		} else if seen[zone.Zone] {
			problems = append(problems, fmt.Sprintf("%s[%d].Zone %q appears twice; list each zone once", field, i, zone.Zone))
		}
		seen[zone.Zone] = true
		if zone.SubnetID == "" {
			problems = append(problems, fmt.Sprintf("%s[%d].SubnetID is empty; pass the ID of a VPC subnet in zone %q", field, i, zone.Zone))
		}
	}
	return problems
}
