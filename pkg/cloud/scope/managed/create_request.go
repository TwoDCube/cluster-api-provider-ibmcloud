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
	"strings"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// This file maps the CAPI objects of a new cluster onto IBM's createCluster request. The mapping is pure and
// golden-tested, and its guards run before the create intent is recorded, so a configuration IBM would refuse costs no
// IBM call and leaves no intent behind.

// createRequestOCPEntitled is IBM's defaultWorkerPoolEntitlement value for an OpenShift pool covered by an existing
// licence. kubernetesservice keeps its own copy unexported, so the value is repeated here.
const createRequestOCPEntitled = "ocp_entitled"

// ConfigurationError lists every problem in the user's objects that stops CAPIBM from building an IBM request. The
// control plane controller reports it as Terminal(InvalidConfiguration), with Error() as the condition message, and
// makes no IBM mutation. Errors of any other type from the same functions are not the user's to fix (a missing input,
// an InfraCluster that has not resolved a zone yet); the caller retries them.
type ConfigurationError struct {
	// Problems are the operator messages, one per problem, each naming the object, the field, the value and the fix.
	Problems []string
}

// Error joins the problems into one condition message.
func (e *ConfigurationError) Error() string {
	return strings.Join(e.Problems, "; ")
}

// BuildCreateClusterRequest builds the createCluster request of a control plane: the effective name; provider vpc-gen2
// (kubernetesservice adds it); kubeVersion CreateVersion(distribution, distributionVersion), which IBM resolves to the
// newest patch (observed on IBM Cloud); the InfraCluster's resource group; the public service endpoint switch; the pod
// and service subnets of Cluster.spec.clusterNetwork (ValidateClusterNetwork); for OpenShift the COS instance CRN,
// which IBM requires for a ROKS cluster on VPC, and ocp_entitled when the initial pool is entitled; and the initial
// worker pool named InitialWorkerPoolName with its flavor, the VPC, the workers per zone (replicas / zones: IBM's
// workerCount is per zone) and each zone's subnet from the InfraCluster status. Taints, labels and the worker operating
// system are never sent: IBM picks its default OS (observed on IBM Cloud; ROKSWorkerOperatingSystem).
//
// Guards: the replicas must divide by the zones; each zone gets at least MinWorkersPerZoneAtCreate workers (whether
// IBM's ROKS minimum at create counts per zone or in total is not yet verified against IBM Cloud, so the guard asks for
// it in every zone); the pool's zones must be InfraCluster zones; an entitlement only for OpenShift; the COS CRN
// exactly for OpenShift.
//
// Parameters:
//   - cp: the IBMCloudManagedControlPlane.
//   - cluster: its owning CAPI Cluster.
//   - infra: the Cluster's IBMCloudManagedCluster, provisioned (status.vpc and status.zones resolved).
//   - pool: the IBMCloudManagedMachinePool of the Cluster whose spec.poolName is InitialWorkerPoolName.
//   - mp: the MachinePool that owns pool; its spec.replicas is the pool's total size.
//
// It returns a *ConfigurationError listing every problem the user must fix, or another error when an input is missing,
// the pool is not the initial pool, or the InfraCluster has not resolved the VPC or a zone of the pool yet.
func BuildCreateClusterRequest(cp *controlplanev1.IBMCloudManagedControlPlane, cluster *clusterv1.Cluster,
	infra *infrav1.IBMCloudManagedCluster, pool *infrav1.IBMCloudManagedMachinePool, mp *clusterv1.MachinePool,
) (kubernetesservice.CreateClusterRequest, error) {
	if err := createRequestInputs(cp, cluster, infra, pool, mp); err != nil {
		return kubernetesservice.CreateClusterRequest{}, err
	}
	zones, problems, err := createRequestZones(infra, pool)
	if err != nil {
		return kubernetesservice.CreateClusterRequest{}, err
	}
	name, err := EffectiveClusterName(cp.Spec.IBMCloudClusterName, cluster.Name)
	if err != nil {
		problems = append(problems, fmt.Sprintf("IBMCloudManagedControlPlane %s: %v", cp.Name, err))
	}
	d := cp.Spec.Distribution
	kubeVersion, versionProblems := createRequestKubeVersion(cp)
	problems = append(problems, versionProblems...)
	cos, cosProblems := createRequestCOS(cp)
	problems = append(problems, cosProblems...)
	entitlement, entitlementProblems := createRequestEntitlement(d, pool)
	problems = append(problems, entitlementProblems...)
	perZone, sizeProblems := createRequestWorkersPerZone(d, pool, mp)
	problems = append(problems, sizeProblems...)
	if pool.Spec.Flavor == "" {
		problems = append(problems, fmt.Sprintf("IBMCloudManagedMachinePool %s has no spec.flavor; set a VPC worker "+
			"flavor such as bx2.4x16", pool.Name))
	}
	if infra.Spec.ResourceGroup.ID == "" {
		problems = append(problems, fmt.Sprintf("IBMCloudManagedCluster %s has no spec.resourceGroup.id; set the ID of "+
			"the resource group the IBM cluster is created in", infra.Name))
	}
	// IBM's subnet ranges depend on the distribution (the 192.168 range of OpenShift is smaller), so the network is
	// checked only for a known one; an unknown distribution is already a problem of createRequestKubeVersion.
	var subnets ClusterSubnets
	if bindingClusterType(d) != "" {
		var networkErr *ConfigurationError
		subnets, err = ValidateClusterNetwork(d, cluster.Spec.ClusterNetwork, createRequestWorkerCIDRs(infra))
		switch {
		case errors.As(err, &networkErr):
			problems = append(problems, networkErr.Problems...)
		case err != nil:
			return kubernetesservice.CreateClusterRequest{}, err
		}
	}
	if len(problems) > 0 {
		return kubernetesservice.CreateClusterRequest{}, &ConfigurationError{Problems: problems}
	}
	return kubernetesservice.CreateClusterRequest{
		Name:                         name,
		ResourceGroupID:              infra.Spec.ResourceGroup.ID,
		KubeVersion:                  kubeVersion,
		DisablePublicServiceEndpoint: cp.Spec.Endpoints.PublicServiceEndpoint == controlplanev1.PublicServiceEndpointDisabled,
		PodSubnet:                    subnets.PodSubnet,
		ServiceSubnet:                subnets.ServiceSubnet,
		COSInstanceCRN:               cos,
		DefaultWorkerPoolEntitlement: entitlement,
		WorkerPool: kubernetesservice.InitialWorkerPool{
			Name:               InitialWorkerPoolName,
			Flavor:             pool.Spec.Flavor,
			VPCID:              infra.Status.VPC.ID,
			WorkerCountPerZone: perZone,
			Zones:              zones,
		},
	}, nil
}

// createRequestInputs returns an error when an input of BuildCreateClusterRequest is missing, when pool is not the
// initial pool, or when the InfraCluster has not resolved its VPC: these are the caller's to fix or wait for, not the
// user's.
//
// Parameters: the inputs of BuildCreateClusterRequest.
func createRequestInputs(cp *controlplanev1.IBMCloudManagedControlPlane, cluster *clusterv1.Cluster,
	infra *infrav1.IBMCloudManagedCluster, pool *infrav1.IBMCloudManagedMachinePool, mp *clusterv1.MachinePool,
) error {
	switch {
	case cp == nil:
		return errors.New("BuildCreateClusterRequest needs the IBMCloudManagedControlPlane; got nil")
	case cluster == nil:
		return errors.New("BuildCreateClusterRequest needs the owning Cluster; got nil")
	case infra == nil:
		return errors.New("BuildCreateClusterRequest needs the Cluster's IBMCloudManagedCluster; got nil")
	case pool == nil:
		return errors.New("BuildCreateClusterRequest needs the initial IBMCloudManagedMachinePool; got nil")
	case mp == nil:
		return errors.New("BuildCreateClusterRequest needs the MachinePool that owns the initial pool; got nil")
	case pool.Spec.PoolName != InitialWorkerPoolName:
		return fmt.Errorf("IBMCloudManagedMachinePool %s has spec.poolName %q; BuildCreateClusterRequest takes the "+
			"initial pool, whose spec.poolName is %q", pool.Name, pool.Spec.PoolName, InitialWorkerPoolName)
	case infra.Status.VPC.ID == "":
		return fmt.Errorf("IBMCloudManagedCluster %s has not resolved its VPC (status.vpc.id); wait until "+
			"status.initialization.provisioned is true", infra.Name)
	}
	return nil
}

// createRequestZones maps the pool's zones to the subnets the InfraCluster resolved. It returns the zones, the problems
// the user must fix (no zone, or a zone the InfraCluster does not list), and an error when the InfraCluster lists a
// zone in its spec but has not resolved its subnet yet.
//
// Parameters:
//   - infra: the IBMCloudManagedCluster.
//   - pool: the initial pool.
func createRequestZones(infra *infrav1.IBMCloudManagedCluster, pool *infrav1.IBMCloudManagedMachinePool) ([]kubernetesservice.ZoneSubnet, []string, error) {
	if len(pool.Spec.Zones) == 0 {
		return nil, []string{fmt.Sprintf("IBMCloudManagedMachinePool %s lists no zones in spec.zones; list the zones of "+
			"IBMCloudManagedCluster %s it uses", pool.Name, infra.Name)}, nil
	}
	resolved := map[string]string{}
	for _, zone := range infra.Status.Zones {
		resolved[zone.Name] = zone.Subnet.ID
	}
	declared := map[string]bool{}
	for _, zone := range infra.Spec.Zones {
		declared[zone.Name] = true
	}
	zones := make([]kubernetesservice.ZoneSubnet, 0, len(pool.Spec.Zones))
	var problems, unresolved []string
	for _, zone := range pool.Spec.Zones {
		switch subnetID := resolved[zone.Name]; {
		case subnetID != "":
			zones = append(zones, kubernetesservice.ZoneSubnet{Zone: zone.Name, SubnetID: subnetID})
		case declared[zone.Name]:
			unresolved = append(unresolved, zone.Name)
		default:
			problems = append(problems, fmt.Sprintf("IBMCloudManagedMachinePool %s lists zone %s in spec.zones, which "+
				"IBMCloudManagedCluster %s does not list in spec.zones; add the zone and its subnet there, or remove it "+
				"from the pool", pool.Name, zone.Name, infra.Name))
		}
	}
	if len(problems) == 0 && len(unresolved) > 0 {
		return nil, nil, fmt.Errorf("IBMCloudManagedCluster %s has not resolved the subnet of zone(s) %s yet (status.zones)",
			infra.Name, strings.Join(unresolved, ", "))
	}
	return zones, problems, nil
}

// createRequestKubeVersion returns the kubeVersion of the request, CreateVersion(distribution, distributionVersion),
// or the problems that prevent it.
//
// Parameters:
//   - cp: the control plane.
func createRequestKubeVersion(cp *controlplanev1.IBMCloudManagedControlPlane) (string, []string) {
	d := cp.Spec.Distribution
	if bindingClusterType(d) == "" {
		return "", []string{fmt.Sprintf("IBMCloudManagedControlPlane %s has spec.distribution %q; set %q or %q", cp.Name, d,
			controlplanev1.DistributionKubernetes, controlplanev1.DistributionOpenShift)}
	}
	minor, err := ibmversion.ParseMinor(cp.Spec.DistributionVersion)
	if err != nil {
		return "", []string{fmt.Sprintf("IBMCloudManagedControlPlane %s spec.distributionVersion: %v", cp.Name, err)}
	}
	return CreateVersion(d, minor), nil
}

// createRequestCOS returns the COS instance CRN of the request: required for OpenShift, because IBM refuses a ROKS
// create on VPC without it (HTTP 400 E7278, observed on IBM Cloud), and never sent for Kubernetes. CEL enforces both;
// the guard keeps a request IBM would refuse from being sent.
//
// Parameters:
//   - cp: the control plane.
func createRequestCOS(cp *controlplanev1.IBMCloudManagedControlPlane) (string, []string) {
	crn := cp.Spec.OpenShift.ImageRegistry.COS.InstanceCRN
	switch {
	case cp.Spec.Distribution == controlplanev1.DistributionOpenShift && crn == "":
		return "", []string{fmt.Sprintf("IBMCloudManagedControlPlane %s has no spec.openShift.imageRegistry.cos.instanceCRN; "+
			"IBM needs a standard Cloud Object Storage instance for the OpenShift registry (E7278)", cp.Name)}
	case cp.Spec.Distribution != controlplanev1.DistributionOpenShift && crn != "":
		return "", []string{fmt.Sprintf("IBMCloudManagedControlPlane %s sets spec.openShift, which only distribution %s "+
			"accepts", cp.Name, controlplanev1.DistributionOpenShift)}
	default:
		return crn, nil
	}
}

// createRequestEntitlement returns the defaultWorkerPoolEntitlement of the request: ocp_entitled for an entitled
// OpenShift pool, "" otherwise; an entitlement is valid for OpenShift only.
//
// Parameters:
//   - d: the distribution.
//   - pool: the initial pool.
func createRequestEntitlement(d controlplanev1.Distribution, pool *infrav1.IBMCloudManagedMachinePool) (string, []string) {
	switch pool.Spec.Entitlement {
	case "":
		return "", nil
	case infrav1.EntitlementOCPEntitled:
		if d != controlplanev1.DistributionOpenShift {
			return "", []string{fmt.Sprintf("IBMCloudManagedMachinePool %s sets spec.entitlement %s, which only "+
				"distribution %s accepts; create the pool without it", pool.Name, pool.Spec.Entitlement,
				controlplanev1.DistributionOpenShift)}
		}
		return createRequestOCPEntitled, nil
	default:
		return "", []string{fmt.Sprintf("IBMCloudManagedMachinePool %s has spec.entitlement %q; the only entitlement is %s",
			pool.Name, pool.Spec.Entitlement, infrav1.EntitlementOCPEntitled)}
	}
}

// createRequestWorkersPerZone returns IBM's workerCount of the initial pool: the MachinePool's replicas divided by the
// pool's zones (IBM's workerCount is per zone, observed on IBM Cloud), or the problems that prevent it. Totals are
// never floored to a multiple of the zones: a total that does not divide is reported for the user to fix, so that the
// cluster never quietly gets fewer workers than MachinePool.spec.replicas asks for.
//
// Parameters:
//   - d: the distribution; it selects MinWorkersPerZoneAtCreate.
//   - pool: the initial pool.
//   - mp: its MachinePool.
func createRequestWorkersPerZone(d controlplanev1.Distribution, pool *infrav1.IBMCloudManagedMachinePool, mp *clusterv1.MachinePool) (int64, []string) {
	zones := int64(len(pool.Spec.Zones))
	minimum := MinWorkersPerZoneAtCreate(d)
	if mp.Spec.Replicas == nil {
		return 0, []string{fmt.Sprintf("MachinePool %s has no spec.replicas; set the total number of workers, a "+
			"multiple of the %d zone(s) of IBMCloudManagedMachinePool %s", mp.Name, zones, pool.Name)}
	}
	replicas := int64(*mp.Spec.Replicas)
	if zones == 0 {
		return 0, nil // createRequestZones reports the missing zones
	}
	if replicas%zones != 0 || replicas <= 0 {
		lower := max(replicas/zones, minimum) * zones
		return 0, []string{fmt.Sprintf("MachinePool %s spec.replicas=%d is not a positive multiple of the %d zone(s) "+
			"of IBMCloudManagedMachinePool %s; use %d or %d", mp.Name, replicas, zones, pool.Name, lower, lower+zones)}
	}
	perZone := replicas / zones
	if perZone < minimum {
		return 0, []string{fmt.Sprintf("MachinePool %s spec.replicas=%d gives %d worker(s) per zone in %d zone(s); IBM "+
			"needs at least %d per zone for a new %s cluster, so set spec.replicas to at least %d", mp.Name, replicas,
			perZone, zones, minimum, d, minimum*zones)}
	}
	return perZone, nil
}

// createRequestWorkerCIDRs returns the CIDR blocks of the InfraCluster's resolved zone subnets, for the pod subnet
// overlap check.
//
// Parameters:
//   - infra: the IBMCloudManagedCluster.
func createRequestWorkerCIDRs(infra *infrav1.IBMCloudManagedCluster) []string {
	cidrs := make([]string, 0, len(infra.Status.Zones))
	for _, zone := range infra.Status.Zones {
		cidrs = append(cidrs, zone.IPv4CIDRBlock)
	}
	return cidrs
}
