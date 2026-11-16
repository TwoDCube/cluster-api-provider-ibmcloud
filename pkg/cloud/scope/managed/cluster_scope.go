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
	"math"
	"net/http"
	"strings"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcemanagerv2"
	"github.com/IBM/vpc-go-sdk/vpcv1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/credentials"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/resourcemanager"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/vpc"
)

// This file is the InfraCluster scope. An IBMCloudManagedCluster only references an existing VPC network: CAPIBM
// creates and deletes nothing in it. The scope checks that the resource group, the VPC and the subnet of every zone
// exist and fit together, and publishes what it found for the control plane and the pools. It reads only:
// GetResourceGroup by ID (never GetResourceGroupByName, which reads the account from the environment), the VPC by ID
// or by name in the region, and each subnet by ID or by name inside that VPC.

const (
	// lowFreeIPv4Addresses is the free-address count below which NetworkReady carries an informational warning: every
	// new worker needs one address, and IBM cannot resize a subnet.
	lowFreeIPv4Addresses = 16
	// listPageLimit is the page size of the VPC and subnet listings.
	listPageLimit int64 = 100
	// maxListPages stops a listing whose pages never end (a server that keeps returning a next page), instead of
	// looping forever inside a reconcile. 50 pages are 5000 VPCs or subnets, far more than one account has in a region.
	maxListPages = 50

	// resourceGroupRole and vpcRole are the IAM roles that a 403 message asks for, one per IBM service the scope
	// calls. VPC Infrastructure Services needs Administrator although this scope only reads: it is IBM's prerequisite
	// for creating the cluster (IKS orders workers, VPEs and load balancers with the stored infrastructure key), so a
	// narrower role would turn NetworkReady True and then fail CreateCluster.
	resourceGroupRole = "the Viewer role on the resource group"
	vpcRole           = "the Administrator platform role on VPC Infrastructure Services"
)

// ClusterScope is the scope of one IBMCloudManagedCluster reconcile. Create it with NewClusterScope; it lives for one
// reconcile only.
type ClusterScope struct {
	params *ScopeParams
	// Cluster is the owning CAPI Cluster.
	Cluster *clusterv1.Cluster
	// IBMCloudManagedCluster is the object being reconciled; the scope writes its status and conditions, and the
	// controller's deferred patch persists them.
	IBMCloudManagedCluster *infrav1.IBMCloudManagedCluster

	// clients are the IBM clients of this reconcile, built by ibm on first use.
	clients *infraClients
}

// infraClients are one reconcile's IBM clients for the Cluster's identity.
type infraClients struct {
	identity        credentials.Identity
	vpc             vpc.Vpc
	resourceManager resourcemanager.ResourceManager
}

// clientBuildError is the error of ibm when the identity resolved but one of its clients could not be built. It keeps
// the identity, so that NetworkReady can name it by its Description; a resolver error has no identity to name.
type clientBuildError struct {
	// identity is the resolved identity whose client failed.
	identity credentials.Identity
	// err is the wrapped builder error, for the controller log.
	err error
}

// Error implements error.
func (e *clientBuildError) Error() string {
	return e.err.Error()
}

// Unwrap returns the builder error, so that errors.Is and errors.As see through clientBuildError.
func (e *clientBuildError) Unwrap() error {
	return e.err
}

// networkProblem is why the network of an IBMCloudManagedCluster cannot be confirmed now.
type networkProblem struct {
	// reason and message are the NetworkReady=False reason and message.
	reason  string
	message string
	// outcome is what the controller does next.
	outcome Outcome
	// transient marks a failure that says nothing about the network (5xx, network, 429). It leaves a True
	// NetworkReady as it is: flipping Ready on a blip would ripple to the Cluster's InfrastructureReady, and the
	// last confirmed network is still the best knowledge.
	transient bool
}

// NewClusterScope returns the scope of one IBMCloudManagedCluster reconcile. It only stores its inputs: it resolves
// no identity, builds no client and makes no call, so that a failure in any of those happens in a reconcile step,
// after the controller's patch helper exists, and is persisted as a condition.
//
// Parameters:
//   - params are the shared dependencies; the scope needs Resolver and ClientBuilder.
//   - cluster is the owning CAPI Cluster.
//   - infra is the IBMCloudManagedCluster being reconciled; it is also the identity holder of its IBM calls.
func NewClusterScope(params *ScopeParams, cluster *clusterv1.Cluster, infra *infrav1.IBMCloudManagedCluster) (*ClusterScope, error) {
	if err := params.validate("ClusterScope", depResolver, depClientBuilder); err != nil {
		return nil, err
	}
	if cluster == nil {
		return nil, errors.New("managed: ClusterScope needs the owning Cluster, got nil")
	}
	if infra == nil {
		return nil, errors.New("managed: ClusterScope needs the IBMCloudManagedCluster, got nil")
	}
	return &ClusterScope{params: params, Cluster: cluster, IBMCloudManagedCluster: infra}, nil
}

// ibm resolves the Cluster's identity with the IBMCloudManagedCluster itself as the holder and builds its VPC and
// Resource Manager clients, on first use, memoized for this reconcile only, so that the scope constructor makes no
// call. A client that cannot be built is reported as a *clientBuildError, which names the resolved identity. ctx is
// the reconcile context.
func (s *ClusterScope) ibm(ctx context.Context) (*infraClients, error) {
	if s.clients != nil {
		return s.clients, nil
	}
	identity, err := s.params.Resolver.Resolve(ctx, s.IBMCloudManagedCluster)
	if err != nil {
		return nil, fmt.Errorf("resolving the IBM Cloud identity of %s: %w", s.objectName(), err)
	}
	vpcClient, err := s.params.ClientBuilder.VPC(ctx, identity, s.IBMCloudManagedCluster.Spec.Region)
	if err != nil {
		return nil, &clientBuildError{identity: identity, err: fmt.Errorf("building the VPC client of %s for region %s: %w",
			identity.Description, s.IBMCloudManagedCluster.Spec.Region, err)}
	}
	rmClient, err := s.params.ClientBuilder.ResourceManager(ctx, identity)
	if err != nil {
		return nil, &clientBuildError{identity: identity, err: fmt.Errorf("building the Resource Manager client of %s: %w", identity.Description, err)}
	}
	s.clients = &infraClients{identity: identity, vpc: vpcClient, resourceManager: rmClient}
	return s.clients, nil
}

// ReconcileNetwork validates the network references and publishes the result: status.vpc, status.zones,
// status.failureDomains, NetworkReady and the sticky status.initialization.provisioned. A reference that disappears
// later flips NetworkReady to False but never provisioned back; the last published status stays as it was. ctx is
// the reconcile context.
func (s *ClusterScope) ReconcileNetwork(ctx context.Context) Outcome {
	clients, err := s.ibm(ctx)
	if err != nil {
		return s.fail(clientsProblem(err))
	}
	if problem := s.checkResourceGroup(clients); problem != nil {
		return s.fail(problem)
	}
	vpcFound, problem := s.resolveVPC(clients)
	if problem != nil {
		return s.fail(problem)
	}
	zones, problem := s.resolveZones(clients, vpcFound)
	if problem != nil {
		return s.fail(problem)
	}
	s.publish(vpcFound, zones)
	return Done()
}

// clientsProblem is the problem of an ibm failure, before any IBM call. The message names the identity by its
// Description when it resolved but never carries the error text, which the outcome hands to the controller log, so
// the condition stays the same while the same failure repeats. err is the error of ibm.
func clientsProblem(err error) *networkProblem {
	message := "CAPIBM cannot resolve the IBM Cloud credentials of this IBMCloudManagedCluster; the controller log names the cause"
	var buildErr *clientBuildError
	if errors.As(err, &buildErr) {
		message = fmt.Sprintf("CAPIBM cannot build the VPC and Resource Manager clients of %s; the controller log names the cause",
			buildErr.identity.Description)
	}
	return &networkProblem{
		reason:  infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason,
		message: message,
		outcome: Transient(err),
	}
}

// fail records problem in NetworkReady, unless it is transient and NetworkReady is already True, and returns its
// outcome. Status fields and provisioned are left as they are. problem is the reason the network is not confirmed.
func (s *ClusterScope) fail(problem *networkProblem) Outcome {
	if problem.transient && conditions.IsTrue(s.IBMCloudManagedCluster, infrav1.IBMCloudManagedClusterNetworkReadyCondition) {
		return problem.outcome
	}
	conditions.Set(s.IBMCloudManagedCluster, metav1.Condition{
		Type:    infrav1.IBMCloudManagedClusterNetworkReadyCondition,
		Status:  metav1.ConditionFalse,
		Reason:  problem.reason,
		Message: problem.message,
	})
	return problem.outcome
}

// checkResourceGroup confirms that spec.resourceGroup.id names a resource group the identity can see, by ID only.
// clients are this reconcile's IBM clients.
func (s *ClusterScope) checkResourceGroup(clients *infraClients) *networkProblem {
	id := s.IBMCloudManagedCluster.Spec.ResourceGroup.ID
	action := fmt.Sprintf("getting resource group %s", id)
	group, response, err := clients.resourceManager.GetResourceGroup(&resourcemanagerv2.GetResourceGroupOptions{ID: core.StringPtr(id)})
	if err != nil || group == nil {
		return s.callProblem(clients.identity, action, resourceGroupRole, response, err, &networkProblem{
			reason: infrav1.IBMCloudManagedClusterNetworkReadyResourceGroupNotFoundReason,
			message: fmt.Sprintf("resource group %s (spec.resourceGroup.id of %s) does not exist or %s cannot see it; "+
				"set the ID of an existing resource group (ibmcloud resource groups)", id, s.objectName(), clients.identity.Description),
		})
	}
	return nil
}

// resolveVPC finds the VPC of spec.vpc.reference by ID, or by name among the VPCs of the region. clients are this
// reconcile's IBM clients.
func (s *ClusterScope) resolveVPC(clients *infraClients) (*vpcv1.VPC, *networkProblem) {
	reference := s.IBMCloudManagedCluster.Spec.VPC.Reference
	region := s.IBMCloudManagedCluster.Spec.Region
	switch {
	case reference.ID != "":
		action := fmt.Sprintf("getting VPC %s", reference.ID)
		found, response, err := clients.vpc.GetVPC(&vpcv1.GetVPCOptions{ID: core.StringPtr(reference.ID)})
		if err != nil || found == nil {
			return nil, s.callProblem(clients.identity, action, vpcRole, response, err, &networkProblem{
				reason: infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
				message: fmt.Sprintf("VPC %s (spec.vpc.reference.id of %s) does not exist in region %s; set the ID of an existing VPC of that region",
					reference.ID, s.objectName(), region),
			})
		}
		return found, nil
	case reference.Name != "":
		return s.findVPCByName(clients, reference.Name)
	default:
		// The CRD requires exactly one of id or name (CEL), so this is only reached by an object the API server did not
		// validate, for example in a unit test.
		message := fmt.Sprintf("spec.vpc.reference of %s names neither an id nor a name; set one of them", s.objectName())
		return nil, &networkProblem{
			reason:  infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			message: message,
			outcome: Terminal(infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason, message),
		}
	}
}

// findVPCByName lists the VPCs of the region and returns the one named name. IBM keeps VPC names unique per region,
// but a name that matches several VPCs is still refused rather than guessed. clients are this reconcile's IBM
// clients; name is spec.vpc.reference.name.
func (s *ClusterScope) findVPCByName(clients *infraClients, name string) (*vpcv1.VPC, *networkProblem) {
	region := s.IBMCloudManagedCluster.Spec.Region
	action := fmt.Sprintf("listing the VPCs of region %s", region)
	var matches []vpcv1.VPC
	options := &vpcv1.ListVpcsOptions{Limit: ptr.To(listPageLimit)}
	for page := 0; ; page++ {
		if page == maxListPages {
			return nil, s.pagingProblem(action)
		}
		list, response, err := clients.vpc.ListVpcs(options)
		if err != nil || list == nil {
			return nil, s.callProblem(clients.identity, action, vpcRole, response, err, nil)
		}
		for _, candidate := range list.Vpcs {
			if ptr.Deref(candidate.Name, "") == name {
				matches = append(matches, candidate)
			}
		}
		next, err := list.GetNextStart()
		if err != nil {
			return nil, s.callProblem(clients.identity, action, vpcRole, nil, err, nil)
		}
		if next == nil {
			break
		}
		options.Start = next
	}

	notFound := func(message string) *networkProblem {
		return &networkProblem{
			reason:  infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason,
			message: message,
			outcome: Terminal(infrav1.IBMCloudManagedClusterNetworkReadyVPCNotFoundReason, message),
		}
	}
	switch len(matches) {
	case 0:
		return nil, notFound(fmt.Sprintf("no VPC named %s exists in region %s (spec.vpc.reference.name of %s); "+
			"check the name or set spec.vpc.reference.id", name, region, s.objectName()))
	case 1:
		return &matches[0], nil
	default:
		return nil, notFound(fmt.Sprintf("%d VPCs are named %s in region %s; set spec.vpc.reference.id of %s to the one to use",
			len(matches), name, region, s.objectName()))
	}
}

// resolveZones resolves the subnet of every spec zone, in spec order, and checks that it is in the VPC and in that
// zone. The subnets of the VPC are listed at most once, and only when a subnet is given by name.
//
// Parameters:
//   - clients are this reconcile's IBM clients.
//   - vpcFound is the resolved VPC.
func (s *ClusterScope) resolveZones(clients *infraClients, vpcFound *vpcv1.VPC) ([]infrav1.ResolvedZone, *networkProblem) {
	var vpcSubnets []vpcv1.Subnet
	listed := false
	zones := make([]infrav1.ResolvedZone, 0, len(s.IBMCloudManagedCluster.Spec.Zones))
	for _, zone := range s.IBMCloudManagedCluster.Spec.Zones {
		var subnet *vpcv1.Subnet
		var problem *networkProblem
		if zone.Subnet.ID != "" {
			subnet, problem = s.getSubnet(clients, zone)
		} else {
			if !listed {
				vpcSubnets, problem = s.listVPCSubnets(clients, vpcFound)
				if problem != nil {
					return nil, problem
				}
				listed = true
			}
			subnet, problem = s.subnetByName(zone, vpcFound, vpcSubnets)
		}
		if problem != nil {
			return nil, problem
		}
		if problem := s.checkSubnetPlacement(zone, subnet, vpcFound); problem != nil {
			return nil, problem
		}
		zones = append(zones, resolvedZone(zone.Name, subnet))
	}
	return zones, nil
}

// getSubnet gets the subnet of zone by ID.
//
// Parameters:
//   - clients are this reconcile's IBM clients.
//   - zone is the spec zone; zone.Subnet.ID is set.
func (s *ClusterScope) getSubnet(clients *infraClients, zone infrav1.ZoneSubnet) (*vpcv1.Subnet, *networkProblem) {
	action := fmt.Sprintf("getting subnet %s of zone %s", zone.Subnet.ID, zone.Name)
	subnet, response, err := clients.vpc.GetSubnet(&vpcv1.GetSubnetOptions{ID: core.StringPtr(zone.Subnet.ID)})
	if err != nil || subnet == nil {
		return nil, s.callProblem(clients.identity, action, vpcRole, response, err, &networkProblem{
			reason: infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason,
			message: fmt.Sprintf("subnet %s (zone %s of %s) does not exist in region %s; set the ID of an existing subnet of the VPC in that zone",
				zone.Subnet.ID, zone.Name, s.objectName(), s.IBMCloudManagedCluster.Spec.Region),
		})
	}
	return subnet, nil
}

// listVPCSubnets lists every subnet of the VPC.
//
// Parameters:
//   - clients are this reconcile's IBM clients.
//   - vpcFound is the resolved VPC.
func (s *ClusterScope) listVPCSubnets(clients *infraClients, vpcFound *vpcv1.VPC) ([]vpcv1.Subnet, *networkProblem) {
	action := fmt.Sprintf("listing the subnets of VPC %s", vpcLabel(vpcFound))
	var subnets []vpcv1.Subnet
	options := &vpcv1.ListSubnetsOptions{VPCID: vpcFound.ID, Limit: ptr.To(listPageLimit)}
	for page := 0; ; page++ {
		if page == maxListPages {
			return nil, s.pagingProblem(action)
		}
		list, response, err := clients.vpc.ListSubnets(options)
		if err != nil || list == nil {
			return nil, s.callProblem(clients.identity, action, vpcRole, response, err, nil)
		}
		subnets = append(subnets, list.Subnets...)
		next, err := list.GetNextStart()
		if err != nil {
			return nil, s.callProblem(clients.identity, action, vpcRole, nil, err, nil)
		}
		if next == nil {
			return subnets, nil
		}
		options.Start = next
	}
}

// subnetByName returns the subnet of the VPC named zone.Subnet.Name. Names are matched inside the VPC only, and a
// name that matches several subnets is an error, never a guess.
//
// Parameters:
//   - zone is the spec zone; zone.Subnet.Name is set.
//   - vpcFound is the resolved VPC.
//   - vpcSubnets are the subnets of that VPC.
func (s *ClusterScope) subnetByName(zone infrav1.ZoneSubnet, vpcFound *vpcv1.VPC, vpcSubnets []vpcv1.Subnet) (*vpcv1.Subnet, *networkProblem) {
	var matches []*vpcv1.Subnet
	for i := range vpcSubnets {
		if ptr.Deref(vpcSubnets[i].Name, "") == zone.Subnet.Name {
			matches = append(matches, &vpcSubnets[i])
		}
	}
	switch len(matches) {
	case 0:
		message := fmt.Sprintf("no subnet named %s exists in VPC %s (zone %s of %s); create it or correct the name",
			zone.Subnet.Name, vpcLabel(vpcFound), zone.Name, s.objectName())
		return nil, &networkProblem{
			reason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason,
			message: message,
			outcome: Terminal(infrav1.IBMCloudManagedClusterNetworkReadySubnetNotFoundReason, message),
		}
	case 1:
		return matches[0], nil
	default:
		message := fmt.Sprintf("%d subnets of VPC %s are named %s; set the subnet id of zone %s in %s",
			len(matches), vpcLabel(vpcFound), zone.Subnet.Name, zone.Name, s.objectName())
		return nil, &networkProblem{
			reason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetNameAmbiguousReason,
			message: message,
			outcome: Terminal(infrav1.IBMCloudManagedClusterNetworkReadySubnetNameAmbiguousReason, message),
		}
	}
}

// checkSubnetPlacement confirms that subnet belongs to the VPC and to the zone it is listed for.
//
// Parameters:
//   - zone is the spec zone.
//   - subnet is the subnet found for it.
//   - vpcFound is the resolved VPC.
func (s *ClusterScope) checkSubnetPlacement(zone infrav1.ZoneSubnet, subnet *vpcv1.Subnet, vpcFound *vpcv1.VPC) *networkProblem {
	subnetVPCID := ""
	subnetVPCLabel := "an unknown VPC"
	if subnet.VPC != nil {
		subnetVPCID = ptr.Deref(subnet.VPC.ID, "")
		subnetVPCLabel = labelOf(subnet.VPC.Name, subnet.VPC.ID)
	}
	if subnetVPCID != ptr.Deref(vpcFound.ID, "") {
		message := fmt.Sprintf("subnet %s is in VPC %s, but %s uses VPC %s; set a subnet of VPC %s for zone %s",
			subnetLabel(subnet), subnetVPCLabel, s.objectName(), vpcLabel(vpcFound), vpcLabel(vpcFound), zone.Name)
		return &networkProblem{
			reason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetNotInVPCReason,
			message: message,
			outcome: Terminal(infrav1.IBMCloudManagedClusterNetworkReadySubnetNotInVPCReason, message),
		}
	}
	subnetZone := ""
	if subnet.Zone != nil {
		subnetZone = ptr.Deref(subnet.Zone.Name, "")
	}
	if subnetZone != zone.Name {
		message := fmt.Sprintf("subnet %s is in zone %s, but %s lists it for %s", subnetLabel(subnet), subnetZone, s.objectName(), zone.Name)
		return &networkProblem{
			reason:  infrav1.IBMCloudManagedClusterNetworkReadySubnetZoneMismatchReason,
			message: message,
			outcome: Terminal(infrav1.IBMCloudManagedClusterNetworkReadySubnetZoneMismatchReason, message),
		}
	}
	return nil
}

// publish writes the confirmed network into status, sets NetworkReady to True with the informational messages, and
// sets the sticky provisioned flag.
//
// Parameters:
//   - vpcFound is the resolved VPC.
//   - zones are the resolved zones, in spec order.
func (s *ClusterScope) publish(vpcFound *vpcv1.VPC, zones []infrav1.ResolvedZone) {
	infra := s.IBMCloudManagedCluster
	infra.Status.VPC = infrav1.ResolvedResource{ID: ptr.Deref(vpcFound.ID, ""), Name: ptr.Deref(vpcFound.Name, "")}
	infra.Status.Zones = zones
	failureDomains := make([]infrav1.FailureDomain, 0, len(zones))
	for _, zone := range zones {
		failureDomains = append(failureDomains, infrav1.FailureDomain{Name: zone.Name})
	}
	infra.Status.FailureDomains = failureDomains
	conditions.Set(infra, metav1.Condition{
		Type:    infrav1.IBMCloudManagedClusterNetworkReadyCondition,
		Status:  metav1.ConditionTrue,
		Reason:  infrav1.IBMCloudManagedClusterNetworkReadyReason,
		Message: networkNotes(zones),
	})
	// Sticky: once every reference resolved, the Cluster's infrastructure stays provisioned (CAPI InfraCluster
	// contract); a later problem only shows in NetworkReady.
	infra.Status.Initialization.Provisioned = ptr.To(true)
}

// callProblem turns a failed VPC or Resource Manager call into a networkProblem. Messages carry the status code but
// no IBM free text and no trace ID, so a repeated failure writes nothing new; the returned outcome carries the full
// error for the controller log.
//
// Parameters:
//   - identity is the identity the call was made with; 401 invalidates its authenticator, and credential messages
//     name it by its description.
//   - action says what the call did, for example "getting VPC r006-...".
//   - role is the IAM role the call's service needs (resourceGroupRole or vpcRole); a 403 message asks for it.
//   - response is the SDK's DetailedResponse (may be nil).
//   - err is the SDK's error (may be nil when the SDK returned no result).
//   - notFound is the problem of an HTTP 404; nil when a 404 has no specific meaning for the call.
func (s *ClusterScope) callProblem(identity credentials.Identity, action, role string, response *core.DetailedResponse, err error,
	notFound *networkProblem) *networkProblem {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	if err == nil {
		err = fmt.Errorf("IBM Cloud answered HTTP %d without a result", status)
	}
	reason := infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason
	switch {
	case status == http.StatusNotFound && notFound != nil:
		notFound.outcome = Terminal(notFound.reason, notFound.message)
		return notFound
	case status == http.StatusUnauthorized:
		// Only this identity's authenticator is rebuilt, on its next use; other identities keep theirs.
		s.params.ClientBuilder.InvalidateAuthenticator(identity.Key)
		return &networkProblem{
			reason:  reason,
			message: fmt.Sprintf("IBM Cloud rejected the credentials of %s (HTTP 401) while %s; CAPIBM rebuilds them and retries", identity.Description, action),
			outcome: NotDone(IntervalSteadyState, reason),
		}
	case status == http.StatusForbidden:
		message := fmt.Sprintf("IBM Cloud refused the credentials of %s (HTTP 403) while %s; grant them %s "+
			"(the IAM roles of the managed Kubernetes book)", identity.Description, action, role)
		return &networkProblem{reason: reason, message: message, outcome: Terminal(reason, message)}
	case status == http.StatusTooManyRequests:
		return &networkProblem{
			reason:    reason,
			message:   fmt.Sprintf("IBM Cloud rate limited CAPIBM (HTTP 429) while %s; CAPIBM retries", action),
			outcome:   RateLimited(0),
			transient: true,
		}
	case status != 0:
		return &networkProblem{
			reason:    reason,
			message:   fmt.Sprintf("IBM Cloud answered HTTP %d while %s; CAPIBM retries", status, action),
			outcome:   Transient(fmt.Errorf("%s: HTTP %d: %w", action, status, err)),
			transient: true,
		}
	default:
		return &networkProblem{
			reason:    reason,
			message:   fmt.Sprintf("CAPIBM could not reach IBM Cloud while %s; it retries, and the controller log names the cause", action),
			outcome:   Transient(fmt.Errorf("%s: %w", action, err)),
			transient: true,
		}
	}
}

// pagingProblem is the problem of a listing that did not end after maxListPages pages. action names the listing.
func (s *ClusterScope) pagingProblem(action string) *networkProblem {
	return &networkProblem{
		reason:    infrav1.IBMCloudManagedClusterNetworkReadyIBMAPIErrorReason,
		message:   fmt.Sprintf("IBM Cloud returned more than %d pages while %s; CAPIBM retries", maxListPages, action),
		outcome:   Transient(fmt.Errorf("%s: no last page after %d pages", action, maxListPages)),
		transient: true,
	}
}

// objectName names the reconciled object in messages, for example IBMCloudManagedCluster/dev.
func (s *ClusterScope) objectName() string {
	return "IBMCloudManagedCluster/" + s.IBMCloudManagedCluster.Name
}

// resolvedZone is the status entry of one zone.
//
// Parameters:
//   - zoneName is the spec zone name.
//   - subnet is its resolved subnet.
func resolvedZone(zoneName string, subnet *vpcv1.Subnet) infrav1.ResolvedZone {
	resolved := infrav1.ResolvedZone{
		Name:          zoneName,
		Subnet:        infrav1.ResolvedResource{ID: ptr.Deref(subnet.ID, ""), Name: ptr.Deref(subnet.Name, "")},
		IPv4CIDRBlock: ptr.Deref(subnet.Ipv4CIDRBlock, ""),
		PublicGateway: infrav1.PublicGatewayStateNotAttached,
	}
	if subnet.PublicGateway != nil {
		resolved.PublicGateway = infrav1.PublicGatewayStateAttached
	}
	if subnet.AvailableIpv4AddressCount != nil {
		resolved.AvailableIPv4Addresses = ptr.To(clampInt32(*subnet.AvailableIpv4AddressCount))
	}
	return resolved
}

// networkNotes is the informational message of a True NetworkReady: zones without a public gateway (the ROKS web
// console and OperatorHub need egress) and subnets with fewer than lowFreeIPv4Addresses free addresses. zones are the
// resolved zones in spec order, so the message is the same on every reconcile.
func networkNotes(zones []infrav1.ResolvedZone) string {
	var notes []string
	for _, zone := range zones {
		if zone.PublicGateway == infrav1.PublicGatewayStateNotAttached {
			notes = append(notes, fmt.Sprintf("subnet %s (zone %s) has no public gateway, so its workers have no public egress, "+
				"which the ROKS web console and OperatorHub need", resourceLabel(zone.Subnet), zone.Name))
		}
		if zone.AvailableIPv4Addresses != nil && *zone.AvailableIPv4Addresses < lowFreeIPv4Addresses {
			notes = append(notes, fmt.Sprintf("subnet %s (zone %s) has %d free IPv4 addresses (fewer than %d), so new workers may get none",
				resourceLabel(zone.Subnet), zone.Name, *zone.AvailableIPv4Addresses, lowFreeIPv4Addresses))
		}
	}
	return strings.Join(notes, "; ")
}

// clampInt32 converts an IBM count to the int32 of the API, saturating instead of wrapping. value is the count.
func clampInt32(value int64) int32 {
	switch {
	case value > math.MaxInt32:
		return math.MaxInt32
	case value < 0:
		return 0
	default:
		return int32(value)
	}
}

// vpcLabel names a VPC in messages by name and ID. vpcFound is the VPC.
func vpcLabel(vpcFound *vpcv1.VPC) string {
	return labelOf(vpcFound.Name, vpcFound.ID)
}

// subnetLabel names a subnet in messages by name and ID. subnet is the subnet.
func subnetLabel(subnet *vpcv1.Subnet) string {
	return labelOf(subnet.Name, subnet.ID)
}

// resourceLabel names a resolved resource in messages by name and ID. resource is the resolved resource.
func resourceLabel(resource infrav1.ResolvedResource) string {
	return labelOf(&resource.Name, &resource.ID)
}

// labelOf returns "name (id)", or whichever of the two is known.
//
// Parameters:
//   - name is the resource name (may be nil or empty).
//   - id is the resource ID (may be nil or empty).
func labelOf(name, id *string) string {
	n, i := ptr.Deref(name, ""), ptr.Deref(id, "")
	switch {
	case n != "" && i != "":
		return fmt.Sprintf("%s (%s)", n, i)
	case n != "":
		return n
	default:
		return i
	}
}
