//go:build e2e

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

package e2e

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/bootstrap"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/test/e2e/managedtools"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The managed Kubernetes lanes. Each lane is an Ordered container: one cluster per lane, created in the first spec,
// exercised by the following ones and deleted in the last one; the cleanups that BeforeAll registers delete it in
// every other outcome. A lane runs only when -e2e.flavor (E2E_FLAVOR) names its flavor, so one CI job runs one lane.
// By default a lane assumes that the e2e account holds one managed run at a time (the Boskos lease of ci-e2e.sh): its
// janitor deletes stale e2e clusters of any run, and its leftover check covers the whole resource group.
// E2E_MANAGED_JANITOR=false and E2E_MANAGED_LEFTOVER_SCOPE=lane let a lane share the account with other runs that
// each use a VPC of their own (managedtools.InventoryScope).
const (
	// iksFlavor is the IKS lane's flavor: IBM's HA topology, 3 zones x 2 cx2.2x4 workers, the flavor's default, so
	// that the lane tests what users get.
	iksFlavor = "managed-iks"
	// roksFlavor is the ROKS lane's flavor: 1 zone x 2 bx2.4x16 workers with a COS instance, to cap cost (not HA).
	roksFlavor = "managed-roks-single-zone"

	// iksWorkerMachineCount and roksWorkerMachineCount are passed explicitly: clusterctl sets WORKER_MACHINE_COUNT
	// to 0 when neither the flag nor the variable is given, so the flavors' defaults never apply.
	iksWorkerMachineCount  int64 = 6
	roksWorkerMachineCount int64 = 2

	// iksLaneTimeout and roksLaneTimeout are the hard timeouts of the lanes' steps, counted from the lane's
	// BeforeAll. No step waits past them: every step's context ends there, and l.intervals lowers every Eventually
	// timeout to the time left, including those of the CAPI framework helpers that poll without a context. The
	// cleanups that BeforeAll registers run after the steps, each within its own NodeTimeout (CAPI deletion 90 m, IBM
	// backstop 10 m, inventory diff 45 m), so a lane can hold its cluster for up to its hard timeout plus 145 m.
	// Ginkgo's --timeout (ci-e2e.sh) does not cut that short where it matters: Ginkgo v2 gives a cleanup node that
	// starts after the suite timeout its full NodeTimeout, so the IBM backstop and the inventory diff still run.
	// The IKS lane's steps take about 165 to 180 m: set-up 1, create 23 (measured 20 to 23), scale-down 4, pool add 5,
	// master upgrade 19 (measured 18.5 from 1.35 to 1.36) and 2 more until the pools see it, the blocked-drain check
	// and hold 6, pool deletion 3, move 10 (not measured yet) and cluster deletion 18 (measured on ROKS), plus 9 worker
	// replacements (6 when the workers of both pools follow the upgrade, 3 for rollout.after) of 8 to 10 m each (the
	// estimate in test/e2e/config/ibmcloud-e2e-managed.yaml). 4 h leaves about an hour for a slower IBM. It must not
	// exceed managedtools.JanitorMinimumAge, which assumes that no lane's steps last longer.
	iksLaneTimeout  = 4 * time.Hour
	roksLaneTimeout = 4 * time.Hour

	// initialPoolName is the IBM pool name of the flavors' initial pool (poolName: default); the flavors also name its
	// MachinePool and IBMCloudManagedMachinePool "<cluster>-default".
	initialPoolName = "default"
	// extraPoolName is the IBM pool the IKS lane adds. With 1 worker per zone it gives every zone the second worker
	// that the drains of the upgrade and of the rollout.after recycle need (iksRollingPools), and it keeps the cluster
	// above IKS's minimum of 1 worker (IBM refuses to go below it with E1437) when the initial pool is deleted.
	extraPoolName = "extra"
	// extraPoolReplicas is the extra pool's size: 1 worker in each of the 3 zones.
	extraPoolReplicas int32 = 3
	// scaledDownReplicas is the initial pool's size after the scale-down: 1 worker in each of the 3 zones.
	// TestIKSWorkerRolloutWaitCoversEveryReplacement (managedtools/config_test.go) counts on the upgrade replacing
	// scaledDownReplicas + extraPoolReplicas workers; change it with them.
	scaledDownReplicas int32 = 3
	// probeReplicas is the size of the spread probe Deployment, as many pods as the 6 workers it starts on. Its
	// topologySpreadConstraint is only a preference (whenUnsatisfiable: ScheduleAnyway), and the scheduler does place
	// the pods unevenly: a scale-down of 3 of the 6 workers has evicted a single pod. Only the scale-down counts on the
	// spread for its evictions (it fails if the scheduler put all 6 pods on the 3 workers it keeps); the upgrade
	// replaces the workers that hold every pod after the scale-down, the rollout.after recycle drains workers that each
	// hold a blocker pod, and the pool deletion drains the worker that holds the pinned probe pod.
	probeReplicas int32 = 6
	// blockedDrainHold is how long the IKS lane checks that no worker is replaced while a PodDisruptionBudget blocks
	// the drain: longer than CAPIBM's DisruptionSettleDelay (3 m), so that a replace sent despite the block shows.
	blockedDrainHold = 4 * time.Minute

	// ibmRequestTimeout bounds every IBM request of the lanes' own clients.
	ibmRequestTimeout = 2 * time.Minute
	// defaultContainersURL is the global IBM Cloud Kubernetes Service endpoint.
	defaultContainersURL = "https://containers.cloud.ibm.com/global"
)

// Variables of the managed e2e configuration (test/e2e/config/ibmcloud-e2e-managed.yaml); each may also come from the
// environment, which wins (E2EConfig.GetVariableOrEmpty). The probe image variable, varProbeImage, is declared with
// the probe constants in managed_waiters.go, whose messages name it.
const (
	varNamePrefix      = "E2E_NAME_PREFIX"
	varRunID           = "E2E_RUN_ID"
	varRegion          = "IBMCLOUD_REGION"
	varResourceGroupID = "IBMCLOUD_RESOURCE_GROUP_ID"
	varVPCName         = "IBMCLOUD_VPC_NAME"
	varIKSVersion      = "IKS_DISTRIBUTION_VERSION"
	varIKSUpgradeTo    = "IKS_DISTRIBUTION_VERSION_UPGRADE_TO"
	varContainersURL   = "IBMCLOUD_CONTAINERS_URL"
	varVPCURL          = "IBMCLOUD_VPC_URL"
)

// rollingPool is one of the IKS lane's worker pools while the lane has two.
type rollingPool struct {
	// name is the pool's IBM name (spec.poolName) and the suffix of its MachinePool and IBMCloudManagedMachinePool.
	name string
	// replicas is the pool's size.
	replicas int32
}

// iksRollingPools are the IKS lane's pools from addPool until deleteInitialPool, 1 worker per zone each: together they
// give every zone the 2 workers that a drain through IBM's ALB PodDisruptionBudgets needs (drainWorkersPerZone), so the
// upgrade and the rollout.after recycle run while both exist.
var iksRollingPools = []rollingPool{
	{name: initialPoolName, replicas: scaledDownReplicas},
	{name: extraPoolName, replicas: extraPoolReplicas},
}

var _ = Describe("Managed Kubernetes (IKS and ROKS)", Label("managed"), func() {
	Context("IKS lane on the managed-iks flavor", Ordered, Label("iks"), func() {
		lane := &managedLane{input: managedLaneInput{
			SpecName:           "managed-iks",
			Lane:               managedtools.LaneIKS,
			Flavor:             iksFlavor,
			WorkerMachineCount: iksWorkerMachineCount,
			Timeout:            iksLaneTimeout,
			Identity:           ControllerIdentityFixture{},
			MoveTargetIdentity: ControllerIdentityFixture{},
			RequiredVariables: []string{varRegion, varResourceGroupID, varVPCName,
				"IBMCLOUD_ZONE_1", "IBMCLOUD_ZONE_2", "IBMCLOUD_ZONE_3",
				"IBMCLOUD_SUBNET_NAME_1", "IBMCLOUD_SUBNET_NAME_2", "IBMCLOUD_SUBNET_NAME_3",
				varIKSVersion, varIKSUpgradeTo, varProbeImage},
		}}
		BeforeAll(lane.setUp)
		// Every drain of the lane runs while every zone that holds workers has at least 2 of them (each drain step
		// checks it first, waitForDrainableZones): the scale-down leaves the initial pool 1 worker per zone, the added
		// pool gives every zone its second worker, and the initial pool is deleted only after the upgrade and the
		// rollout.after recycle, as the last drain. A zone with 1 worker could never drain it through IBM's ALB budget.
		It("creates an IKS cluster with 3 zones x 2 workers from the managed-iks flavor", lane.create)
		It("has IBM's HA topology: three zones, two workers per zone, six provider IDs", lane.expectHATopology)
		It("fulfils the CAPI control plane and MachinePool contracts", lane.expectContract)
		It("scales the initial pool down to 1 worker per zone without a PodDisruptionBudget violation", lane.scaleDownInitialPool)
		It("adds a worker pool", lane.addPool)
		It("upgrades the master by one minor and the workers of both pools follow without a PodDisruptionBudget violation", lane.upgrade)
		It("holds a worker replacement while a PodDisruptionBudget blocks the drain, then recycles every worker of the added pool after rollout.after", lane.rolloutAfterWithBlockedDrain)
		It("deletes the initial worker pool after draining its workers", lane.deleteInitialPool)
		It("re-adopts the cluster after clusterctl move without IBM writes", lane.move)
		It("deletes the cluster: IBM answers 404 and the LeftoverResources event is recorded", lane.delete)
	})

	Context("ROKS lane on the managed-roks-single-zone flavor", Ordered, Label("roks", "weekly"), func() {
		lane := &managedLane{input: managedLaneInput{
			SpecName:           "managed-roks",
			Lane:               managedtools.LaneROKS,
			Flavor:             roksFlavor,
			WorkerMachineCount: roksWorkerMachineCount,
			Timeout:            roksLaneTimeout,
			Identity:           ControllerIdentityFixture{},
			RequiredVariables: []string{varRegion, varResourceGroupID, varVPCName, "IBMCLOUD_ZONE_1",
				"IBMCLOUD_SUBNET_NAME_1", "ROKS_DISTRIBUTION_VERSION", "COS_INSTANCE_CRN"},
		}}
		BeforeAll(lane.setUp)
		It("creates a ROKS cluster with 1 zone x 2 workers from the managed-roks-single-zone flavor", lane.create)
		It("fulfils the CAPI control plane and MachinePool contracts", lane.expectContract)
		It("deletes the cluster: IBM answers 404 and the LeftoverResources event is recorded", lane.delete)
	})
})

// managedLaneInput describes one lane.
type managedLaneInput struct {
	// SpecName prefixes the namespace and selects the lane's intervals in the e2e config ("<SpecName>/<key>").
	SpecName string
	// Lane is the last segment of the cluster name.
	Lane managedtools.Lane
	// Flavor is the clusterctl flavor; the lane runs only when -e2e.flavor equals it.
	Flavor string
	// WorkerMachineCount is passed to clusterctl as --worker-machine-count.
	WorkerMachineCount int64
	// Timeout is the lane's hard timeout.
	Timeout time.Duration
	// Identity provides the credentials the cluster is created with, on the first management cluster.
	Identity IdentityFixture
	// MoveTargetIdentity provides the credentials on the clusterctl move target; nil for lanes without a move.
	MoveTargetIdentity IdentityFixture
	// RequiredVariables must be set (non-empty) in the e2e config or the environment.
	RequiredVariables []string
}

// managedLane is the state one lane shares between its specs.
type managedLane struct {
	input managedLaneInput

	// deadline is the end of the lane's hard timeout.
	deadline time.Time
	// runStart is taken before the lane creates anything; the IBM backstop deletes only clusters created after it.
	runStart time.Time
	// namePrefix and clusterName follow the name contract (managedtools.ClusterName).
	namePrefix  string
	clusterName string

	namespace           *corev1.Namespace
	cancelWatches       context.CancelFunc
	clusterctlLogFolder string
	clusterctlVariables map[string]string

	// mgmt is the management cluster that owns the cluster now; it changes at the move.
	mgmt framework.ClusterProxy
	// moved is true once the cluster lives on the move target.
	moved bool
	// moveTargetProvider is the kind cluster of the move target, disposed at the end of the lane.
	moveTargetProvider bootstrap.ClusterProvider
	moveTargetProxy    framework.ClusterProxy

	result         *clusterctl.ApplyClusterTemplateAndWaitResult
	clusterDeleted bool

	ibm           laneIBMClients
	baseline      managedtools.Snapshot
	baselineTaken bool

	// janitor is E2E_MANAGED_JANITOR: whether setUp runs the janitor.
	janitor bool
	// leftoverScope is E2E_MANAGED_LEFTOVER_SCOPE; scope is built from it once the IBM clients exist.
	leftoverScope managedtools.LeftoverScope
	// scope is what the inventory baseline and the final leftover check cover.
	scope managedtools.InventoryScope
}

// laneIBMClients are the lane's IBM clients, built with the identity fixture's authenticator.
type laneIBMClients struct {
	// clusters lists and deletes clusters for the janitor, the backstop and the inventory.
	clusters *managedtools.ClusterService
	// vpc lists the VPC resources for the inventory; in the lane scope it also gives each load balancer its VPC
	// (buildInventoryScope).
	vpc *managedtools.VPCService
	// service is CAPIBM's own IBM Cloud Kubernetes Service client, for the 404 assertion.
	service kubernetesservice.KubernetesService
	// resourceGroupID scopes every IBM call.
	resourceGroupID string
}

var (
	// managedRunID is the run ID shared by the lanes of one suite run.
	managedRunID     string
	managedRunIDErr  error
	managedRunIDOnce sync.Once
)

// runID returns E2E_RUN_ID, or a random ID generated once per suite run.
func runID() (string, error) {
	managedRunIDOnce.Do(func() {
		managedRunID = e2eConfig.GetVariableOrEmpty(varRunID)
		if managedRunID == "" {
			managedRunID, managedRunIDErr = managedtools.NewRunID(managedtools.DefaultRunIDLength, nil)
		}
	})
	return managedRunID, managedRunIDErr
}

// stepContext returns a context that ends at the lane's hard timeout or with the spec, whichever comes first.
//
// Parameters:
//   - specCtx is the spec's context.
func (l *managedLane) stepContext(specCtx context.Context) context.Context {
	ctx, cancel := context.WithDeadline(specCtx, l.deadline)
	DeferCleanup(cancel)
	return ctx
}

// intervals returns the lane's Eventually intervals for key, with the timeout lowered to the time left until the
// lane's hard timeout. The step contexts end at the deadline too, but the CAPI framework helpers the steps call
// (DiscoveryAndWaitForCluster, WaitForMachinePoolNodesToExist, WaitForClusterDeleted) poll without a context: after
// the deadline their requests fail at once, yet Eventually would keep polling for its whole configured timeout (up to
// 90 m). A step asks for each interval right where it waits, so that the cap uses the time actually left; once the
// deadline has passed, the step fails here. The cleanups use configuredIntervals instead.
//
// Parameters:
//   - key is the interval name, for example "wait-control-plane".
func (l *managedLane) intervals(key string) []interface{} {
	intervals, err := managedtools.LimitTimeout(l.configuredIntervals(key), time.Until(l.deadline))
	Expect(err).NotTo(HaveOccurred(), "the %s lane's %s wait", l.input.Lane, key)
	return intervals
}

// configuredIntervals returns the lane's Eventually intervals for key as the e2e config sets them. Only the cleanups
// use them directly: they run after the hard timeout, each bounded by its own NodeTimeout.
//
// Parameters:
//   - key is the interval name, for example "wait-inventory-clean".
func (l *managedLane) configuredIntervals(key string) []interface{} {
	intervals := e2eConfig.GetIntervals(l.input.SpecName, key)
	Expect(intervals).NotTo(BeEmpty(), "the e2e config has no %s/%s or default/%s interval", l.input.SpecName, key, key)
	return intervals
}

// variable returns an e2e config variable, or fallback when it is empty.
//
// Parameters:
//   - name is the variable name.
//   - fallback is the value for an empty or unset variable.
func variable(name, fallback string) string {
	if value := e2eConfig.GetVariableOrEmpty(name); value != "" {
		return value
	}
	return fallback
}

// setUp validates the lane's configuration, runs the janitor (unless E2E_MANAGED_JANITOR=false), sets the leftover
// scope, takes the inventory baseline, creates the namespace, prepares the identity fixture and registers the cleanups.
// Every input problem fails here, before IBM is asked to create anything.
func (l *managedLane) setUp(specCtx SpecContext) {
	Expect(e2eConfig).NotTo(BeNil(), "e2eConfig is required for the %s lane", l.input.Lane)
	Expect(clusterctlConfigPath).To(BeAnExistingFile(), "clusterctlConfigPath must be an existing file")
	Expect(bootstrapClusterProxy).NotTo(BeNil(), "bootstrapClusterProxy is required for the %s lane", l.input.Lane)
	// A managed flavor that no lane runs (for example managed-roks) would skip both lanes and pass having tested
	// nothing, so it fails here instead (ci-e2e.sh refuses it before leasing anything).
	if strings.HasPrefix(flavor, "managed") && flavor != iksFlavor && flavor != roksFlavor {
		Fail(fmt.Sprintf("-e2e.flavor (E2E_FLAVOR) %q runs no managed lane; use %s (IKS lane) or %s (ROKS lane)",
			flavor, iksFlavor, roksFlavor))
	}
	if flavor != l.input.Flavor {
		Skip(fmt.Sprintf("the %s lane runs only with -e2e.flavor=%s (E2E_FLAVOR); this run uses %q",
			l.input.Lane, l.input.Flavor, flavor))
	}
	l.deadline = time.Now().Add(l.input.Timeout)
	ctx, cancel := context.WithDeadline(specCtx, l.deadline)
	defer cancel()

	// The cleanups are registered before anything is created, and each checks what exists, so that a set-up that
	// fails half-way is cleaned up too. DeferCleanup runs them in reverse order: the CAPI deletion first, then the
	// IBM backstop, then the inventory diff. Each is its own node with its own timeout, so an interrupted CAPI deletion
	// still leaves time for the backstop.
	DeferCleanup(l.finalCleanup, NodeTimeout(45*time.Minute))
	DeferCleanup(l.ibmBackstop, NodeTimeout(10*time.Minute))
	DeferCleanup(l.capiCleanup, NodeTimeout(90*time.Minute))

	l.validateVariables()
	l.buildNames()
	l.buildIBMClients()
	l.runJanitor(ctx)
	l.buildInventoryScope(ctx)
	l.takeBaseline(ctx)

	l.runStart = time.Now()
	l.mgmt = bootstrapClusterProxy
	// The namespace watches must outlive this node, so they do not use its context; cancelWatches stops them.
	l.namespace, l.cancelWatches = setupSpecNamespace(context.Background(), l.input.SpecName, l.mgmt, artifactFolder)
	// Keep clusterctl's logs, which contain the rendered template, out of the artifacts (suite convention).
	l.clusterctlLogFolder = filepath.Join(os.TempDir(), "clusters", l.mgmt.GetName())
	vars, err := l.input.Identity.Setup(ctx, l.mgmt, l.namespace.Name)
	Expect(err).NotTo(HaveOccurred(), "setting up the identity fixture %T", l.input.Identity)
	l.clusterctlVariables = vars
}

// validateVariables fails unless every required variable is set, the janitor switch and the leftover scope are valid
// and, for the IKS lane, the upgrade is one minor.
func (l *managedLane) validateVariables() {
	var missing []string
	for _, name := range l.input.RequiredVariables {
		if e2eConfig.GetVariableOrEmpty(name) == "" {
			missing = append(missing, name)
		}
	}
	Expect(missing).To(BeEmpty(), "the %s lane needs these variables in the e2e config or the environment "+
		"(test/e2e/config/ibmcloud-e2e-managed.yaml lists them): %v", l.input.Lane, missing)
	var err error
	l.janitor, err = managedtools.ParseJanitorSwitch(e2eConfig.GetVariableOrEmpty(managedtools.JanitorVariable))
	Expect(err).NotTo(HaveOccurred())
	l.leftoverScope, err = managedtools.ParseLeftoverScope(e2eConfig.GetVariableOrEmpty(managedtools.LeftoverScopeVariable))
	Expect(err).NotTo(HaveOccurred())
	if l.input.Lane == managedtools.LaneIKS {
		Expect(managedtools.ValidateUpgradeStep(e2eConfig.GetVariableOrEmpty(varIKSVersion),
			e2eConfig.GetVariableOrEmpty(varIKSUpgradeTo))).To(Succeed())
	}
}

// buildNames applies the name contract: <E2E_NAME_PREFIX>-<E2E_RUN_ID>-<lane>.
func (l *managedLane) buildNames() {
	l.namePrefix = variable(varNamePrefix, managedtools.DefaultNamePrefix)
	id, err := runID()
	Expect(err).NotTo(HaveOccurred())
	l.clusterName, err = managedtools.ClusterName(l.namePrefix, id, l.input.Lane)
	Expect(err).NotTo(HaveOccurred(), "the e2e cluster name <%s>-<%s>-<lane> breaks the name contract", varNamePrefix, varRunID)
	Byf("The %s lane's cluster is %s", l.input.Lane, l.clusterName)
}

// buildIBMClients builds the lane's IBM clients with the identity fixture's authenticator: the same credentials
// that create the run's clusters.
func (l *managedLane) buildIBMClients() {
	auth, err := l.input.Identity.Authenticator()
	Expect(err).NotTo(HaveOccurred())
	region := e2eConfig.GetVariableOrEmpty(varRegion)
	containersURL := variable(varContainersURL, defaultContainersURL)
	options := managedtools.ClientOptions{
		URL:             containersURL,
		Authenticator:   auth,
		HTTPClient:      &http.Client{Timeout: ibmRequestTimeout},
		ResourceGroupID: e2eConfig.GetVariableOrEmpty(varResourceGroupID),
	}
	l.ibm.resourceGroupID = options.ResourceGroupID
	l.ibm.clusters, err = managedtools.NewClusterService(options)
	Expect(err).NotTo(HaveOccurred())
	options.URL = variable(varVPCURL, fmt.Sprintf("https://%s.iaas.cloud.ibm.com/v1", region))
	l.ibm.vpc, err = managedtools.NewVPCService(options)
	Expect(err).NotTo(HaveOccurred())
	l.ibm.service, err = kubernetesservice.NewService(kubernetesservice.ServiceOptions{
		URL:           containersURL,
		Authenticator: auth,
		RateLimiter:   rate.NewLimiter(rate.Limit(1), 5),
		HTTPTimeout:   ibmRequestTimeout,
		UserAgent:     managedtools.DefaultUserAgent,
	})
	Expect(err).NotTo(HaveOccurred())
}

// runJanitor deletes, by ID, the clusters of earlier runs that are older than managedtools.JanitorMinimumAge. A
// failure stops the lane: a stale cluster nobody can delete costs money until an operator looks at it. With
// E2E_MANAGED_JANITOR=false it deletes nothing: in an account shared with other runs, an old cluster with the e2e
// prefix may belong to a run that is still going, and whoever shares the account cleans up the crashed runs instead.
func (l *managedLane) runJanitor(ctx context.Context) {
	if !l.janitor {
		Byf("Skipping the janitor (%s=false): the account is shared with other runs, whose clusters it must not delete",
			managedtools.JanitorVariable)
		return
	}
	report, err := managedtools.RunJanitor(ctx, l.ibm.clusters, managedtools.JanitorOptions{
		NamePrefix: l.namePrefix,
		MinAge:     managedtools.JanitorMinimumAge,
		Now:        time.Now(),
	})
	Byf("Janitor for clusters named %s-* older than %s:\n%s", l.namePrefix, managedtools.JanitorMinimumAge, report)
	Expect(err).NotTo(HaveOccurred(), "the janitor could not delete every stale e2e cluster")
}

// buildInventoryScope sets what the inventory baseline and the final leftover check cover: the whole resource group
// (E2E_MANAGED_LEFTOVER_SCOPE=account), or only the run's own clusters and the VPC resources of the lane's VPC (lane),
// for which it resolves IBMCLOUD_VPC_NAME to the VPC's ID and switches the VPC client to attributing load balancers to
// their VPC.
//
// Parameters:
//   - ctx bounds the VPC lookup.
func (l *managedLane) buildInventoryScope(ctx context.Context) {
	l.scope = managedtools.AccountScope()
	if l.leftoverScope == managedtools.LeftoverScopeLane {
		vpcID, err := l.ibm.vpc.ResolveVPCID(ctx, e2eConfig.GetVariableOrEmpty(varVPCName))
		Expect(err).NotTo(HaveOccurred(), "%s=lane needs the ID of the lane's VPC", managedtools.LeftoverScopeVariable)
		id, err := runID()
		Expect(err).NotTo(HaveOccurred())
		l.scope, err = managedtools.NewLaneScope(l.namePrefix, id, vpcID)
		Expect(err).NotTo(HaveOccurred())
	}
	// Set before the baseline, so that the baseline and every later snapshot see load balancers the same way. Only
	// the lane scope pays for listing the region's subnets: the account scope keeps every load balancer anyway.
	if l.scope.NeedsLoadBalancerVPCs() {
		l.ibm.vpc = l.ibm.vpc.WithLoadBalancerVPCs()
	}
	Byf("Leftover check of the %s lane: %s", l.input.Lane, l.scope)
}

// takeBaseline takes the inventory snapshot that the final diff compares with.
func (l *managedLane) takeBaseline(ctx context.Context) {
	var err error
	l.baseline, err = l.snapshot(ctx)
	Expect(err).NotTo(HaveOccurred())
	l.baselineTaken = true
}

// snapshot takes an inventory snapshot and keeps only the resources of the lane's leftover scope. The baseline and the
// final diff both come from here, so they always cover the same resources.
//
// Parameters:
//   - ctx bounds the requests.
func (l *managedLane) snapshot(ctx context.Context) (managedtools.Snapshot, error) {
	snapshot, err := managedtools.TakeSnapshot(ctx, l.ibm.clusters, l.ibm.vpc, time.Now())
	if err != nil {
		return managedtools.Snapshot{}, err
	}
	return l.scope.Apply(snapshot), nil
}

// create applies the lane's flavor and waits for the control plane and the initial MachinePool.
func (l *managedLane) create(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	l.result = new(clusterctl.ApplyClusterTemplateAndWaitResult)
	clusterctl.ApplyClusterTemplateAndWait(ctx, clusterctl.ApplyClusterTemplateAndWaitInput{
		ClusterProxy: l.mgmt,
		ConfigCluster: clusterctl.ConfigClusterInput{
			LogFolder:              l.clusterctlLogFolder,
			ClusterctlConfigPath:   clusterctlConfigPath,
			KubeconfigPath:         l.mgmt.GetKubeconfigPath(),
			InfrastructureProvider: clusterctl.DefaultInfrastructureProvider,
			Flavor:                 l.input.Flavor,
			Namespace:              l.namespace.Name,
			ClusterName:            l.clusterName,
			// The managed flavors never read CONTROL_PLANE_MACHINE_COUNT, but the framework dereferences it and
			// clusterctl rejects 0.
			ControlPlaneMachineCount: ptr.To[int64](1),
			WorkerMachineCount:       ptr.To(l.input.WorkerMachineCount),
			ClusterctlVariables:      l.clusterctlVariables,
		},
		// The framework takes all three intervals when the step starts, so each is capped at the time left then, not
		// at the start of its own wait. The step still ends by the hard timeout while their sum stays below it, as the
		// IKS lane's 60 + 60 + 60 m of 4 h do. The ROKS lane's 120 + 120 + 90 m = 330 m exceed its 4 h: the control
		// plane wait ends with this step's context, but the machine pool wait polls without it, so a ROKS control plane
		// that becomes available later than 150 m after the lane's set-up can leave that wait running up to 90 m past
		// the hard timeout.
		WaitForClusterIntervals:      l.intervals("wait-cluster"),
		WaitForControlPlaneIntervals: l.intervals("wait-control-plane"),
		WaitForMachinePools:          l.intervals("wait-worker-nodes"),
		ControlPlaneWaiters:          managedControlPlaneWaiters(),
	}, l.result)
	Expect(l.result.MachinePools).To(HaveLen(1), "the managed flavors have exactly one MachinePool")
}

// cluster returns the lane's CAPI Cluster as the create step left it (name and namespace).
func (l *managedLane) cluster() *clusterv1.Cluster {
	Expect(l.result).NotTo(BeNil(), "the create step has not run")
	Expect(l.result.Cluster).NotTo(BeNil(), "the create step has not run")
	return l.result.Cluster
}

// poolKey returns the key of the MachinePool and IBMCloudManagedMachinePool with the name suffix (the flavors and
// the lane name both objects "<cluster>-<suffix>").
//
// Parameters:
//   - suffix is initialPoolName or extraPoolName.
func (l *managedLane) poolKey(suffix string) client.ObjectKey {
	return client.ObjectKey{Namespace: l.namespace.Name, Name: l.clusterName + "-" + suffix}
}

// workload returns a client of the workload cluster built from the CAPIBM-written kubeconfig Secret, and its proxy.
//
// Parameters:
//   - ctx bounds the requests.
func (l *managedLane) workload(ctx context.Context) (framework.ClusterProxy, client.Client) {
	proxy := l.mgmt.GetWorkloadCluster(ctx, l.namespace.Name, l.clusterName)
	DeferCleanup(proxy.Dispose)
	return proxy, managedClient(proxy)
}

// expectHATopology checks IBM's HA topology of the managed-iks flavor: three zones in the pool status with two
// workers each, sizePerZone 2 and six provider IDs.
func (l *managedLane) expectHATopology(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	eventually(ctx, func(g Gomega) {
		pool, err := getPool(ctx, c, l.poolKey(initialPoolName))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pool.Status.Zones).To(HaveLen(3), "status.zones")
		for _, zone := range pool.Status.Zones {
			g.Expect(zone.WorkerCount).To(HaveValue(Equal(int32(2))), "workers in zone %s", zone.Name)
		}
		g.Expect(pool.Status.SizePerZone).To(HaveValue(Equal(int32(2))), "status.sizePerZone")
		g.Expect(pool.Spec.ProviderIDList).To(HaveLen(int(iksWorkerMachineCount)), "spec.providerIDList")
	}, l.intervals("wait-contract")).Should(Succeed())
}

// expectContract checks the CAPI contract fields against the workload cluster itself: the control plane is
// initialized and externally managed, the endpoint reached the Cluster, the kubeconfig Secret exists,
// status.versions[0] is the API server's /version, and for every MachinePool the providerIDList, the NodeRefs and the
// versions match the pool's Nodes (IBM ships worker fix packs independently of the master's, so the two are compared
// separately).
func (l *managedLane) expectContract(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	workloadProxy, workload := l.workload(ctx)
	eventually(ctx, func(g Gomega) {
		cluster := &clusterv1.Cluster{}
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(l.cluster()), cluster)).To(Succeed())
		cp, err := getControlPlane(ctx, c, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(cp.Status.Initialization.ControlPlaneInitialized).To(HaveValue(BeTrue()), "status.initialization.controlPlaneInitialized")
		g.Expect(cp.Status.ExternalManagedControlPlane).To(HaveValue(BeTrue()), "status.externalManagedControlPlane")
		g.Expect(cp.Spec.ControlPlaneEndpoint.Host).NotTo(BeEmpty(), "spec.controlPlaneEndpoint.host")
		g.Expect(cluster.Spec.ControlPlaneEndpoint.Host).To(Equal(cp.Spec.ControlPlaneEndpoint.Host))
		g.Expect(cluster.Spec.ControlPlaneEndpoint.Port).To(Equal(cp.Spec.ControlPlaneEndpoint.Port))
		g.Expect(c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-kubeconfig"}, &corev1.Secret{})).To(Succeed())
		serverVersion, err := workloadProxy.GetClientSet().Discovery().ServerVersion()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(cp.Status.Versions).NotTo(BeEmpty(), "status.versions")
		g.Expect(cp.Status.Versions[0].Version).To(Equal(serverVersion.GitVersion), "status.versions[0] against /version")

		machinePools := &clusterv1.MachinePoolList{}
		g.Expect(c.List(ctx, machinePools, client.InNamespace(cluster.Namespace),
			client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name})).To(Succeed())
		g.Expect(machinePools.Items).NotTo(BeEmpty())
		for i := range machinePools.Items {
			expectMachinePoolContract(ctx, g, c, workload, &machinePools.Items[i])
		}
	}, l.intervals("wait-contract")).Should(Succeed())
}

// expectMachinePoolContract checks one MachinePool against the Nodes of its IBM pool.
//
// Parameters:
//   - ctx bounds the requests.
//   - g is the Gomega of the enclosing Eventually.
//   - c is a management cluster client with managedScheme.
//   - workload is a workload cluster client.
//   - mp is the MachinePool.
func expectMachinePoolContract(ctx context.Context, g Gomega, c, workload client.Client, mp *clusterv1.MachinePool) {
	pool, err := getPool(ctx, c, client.ObjectKey{Namespace: mp.Namespace, Name: mp.Spec.Template.Spec.InfrastructureRef.Name})
	g.Expect(err).NotTo(HaveOccurred())
	nodes, err := poolNodes(ctx, workload, pool.Spec.PoolName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(nodes).NotTo(BeEmpty(), "Nodes of IBM pool %s", pool.Spec.PoolName)
	providerIDs := make([]string, 0, len(nodes))
	names := make([]string, 0, len(nodes))
	kubelets := make([]string, 0, len(nodes))
	for i := range nodes {
		providerIDs = append(providerIDs, nodes[i].Spec.ProviderID)
		names = append(names, nodes[i].Name)
		kubelets = append(kubelets, nodes[i].Status.NodeInfo.KubeletVersion)
	}
	g.Expect(pool.Spec.ProviderIDList).To(ConsistOf(providerIDs), "IBMCloudManagedMachinePool %s spec.providerIDList", pool.Name)
	g.Expect(mp.Spec.ProviderIDList).To(ConsistOf(providerIDs), "MachinePool %s spec.providerIDList", mp.Name)
	nodeRefs := make([]string, 0, len(mp.Status.NodeRefs))
	for _, ref := range mp.Status.NodeRefs {
		nodeRefs = append(nodeRefs, ref.Name)
	}
	g.Expect(nodeRefs).To(ConsistOf(names), "MachinePool %s status.nodeRefs", mp.Name)
	versions := map[string]int32{}
	for _, v := range mp.Status.Versions {
		versions[v.Version] = v.Replicas
	}
	g.Expect(versions).To(Equal(managedtools.VersionCounts(kubelets)), "MachinePool %s status.versions against the kubelets", mp.Name)
}

// deployProbe applies the spread probe (Deployment and PodDisruptionBudget, maxUnavailable 1) once and waits until it
// is healthy, so that a step starts its disruption probe from a settled budget.
//
// Parameters:
//   - ctx bounds the wait.
//   - workloadProxy and workload are the workload cluster's proxy and client.
func (l *managedLane) deployProbe(ctx context.Context, workloadProxy framework.ClusterProxy, workload client.Client) {
	applyProbeManifest(ctx, workloadProxy, "pdb-probe.yaml", map[string]string{
		"PROBE_NAMESPACE": probeNamespace,
		"PROBE_LABEL":     probeLabel,
		"PROBE_IMAGE":     e2eConfig.GetVariableOrEmpty(varProbeImage),
		"PROBE_REPLICAS":  fmt.Sprint(probeReplicas),
	})
	waitForProbeHealthy(ctx, workload, "spread", probeReplicas, l.intervals("wait-probe")...)
}

// pinProbePod applies the pinned probe Pod (no controller, no PodDisruptionBudget, a nodeSelector on the IBM pool) and
// waits until it runs on a worker of poolName, so that a step that drains every worker of that pool evicts at least
// this probe pod, wherever the scheduler has put the spread probe's pods. Nothing recreates the Pod once evicted.
//
// Parameters:
//   - ctx bounds the wait.
//   - workloadProxy and workload are the workload cluster's proxy and client.
//   - poolName is the IBM pool name (spec.poolName) whose workers must hold the Pod.
func (l *managedLane) pinProbePod(ctx context.Context, workloadProxy framework.ClusterProxy, workload client.Client,
	poolName string) {
	applyProbeManifest(ctx, workloadProxy, "pinned-probe.yaml", map[string]string{
		"PROBE_NAMESPACE":   probeNamespace,
		"PROBE_LABEL":       probeLabel,
		"PROBE_IMAGE":       e2eConfig.GetVariableOrEmpty(varProbeImage),
		"WORKER_POOL_LABEL": workerPoolNameLabel,
		"WORKER_POOL":       poolName,
	})
	waitForProbePodOnPool(ctx, workload, pinnedProbeName, poolName, l.intervals("wait-probe")...)
}

// scaleMachinePool sets a MachinePool's replicas.
//
// Parameters:
//   - ctx bounds the request.
//   - c is a management cluster client.
//   - key names the MachinePool.
//   - replicas is the new replica count.
func scaleMachinePool(ctx context.Context, c client.Client, key client.ObjectKey, replicas int32) {
	Byf("Scaling MachinePool %s to %d", key.Name, replicas)
	mp := &clusterv1.MachinePool{}
	Expect(c.Get(ctx, key, mp)).To(Succeed())
	patch := client.MergeFrom(mp.DeepCopy())
	mp.Spec.Replicas = ptr.To(replicas)
	Expect(c.Patch(ctx, mp, patch)).To(Succeed())
}

// waitForPoolNodes waits until the IBM pool has exactly count Nodes in the workload cluster.
//
// Parameters:
//   - ctx bounds the wait.
//   - workload is a workload cluster client.
//   - poolName is the IBM pool name.
//   - count is the expected number of Nodes.
//   - intervals are the Eventually timeout and polling interval.
func waitForPoolNodes(ctx context.Context, workload client.Client, poolName string, count int, intervals ...interface{}) {
	eventually(ctx, func(g Gomega) {
		nodes, err := poolNodes(ctx, workload, poolName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(nodes).To(HaveLen(count), "Nodes of IBM pool %s", poolName)
	}, intervals).Should(Succeed())
}

// scaleDownInitialPool scales the initial pool from 6 to 3 workers while the spread probe runs: every removed worker
// is drained first, through the Eviction API and within the probe's PodDisruptionBudget. Each zone keeps its other
// worker while its victim drains.
func (l *managedLane) scaleDownInitialPool(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	workloadProxy, workload := l.workload(ctx)
	waitForDrainableZones(ctx, workload, l.intervals("wait-contract")...)
	l.deployProbe(ctx, workloadProxy, workload)

	probe := startDisruptionProbe(ctx, workload)
	scaleMachinePool(ctx, c, l.poolKey(initialPoolName), scaledDownReplicas)
	pool := waitForPoolSettled(ctx, c, l.poolKey(initialPoolName), int(scaledDownReplicas), l.intervals("wait-pool-change")...)
	Expect(pool.Status.SizePerZone).To(HaveValue(Equal(int32(1))), "status.sizePerZone after the scale-down")
	waitForPoolNodes(ctx, workload, pool.Spec.PoolName, int(scaledDownReplicas), l.intervals("wait-pool-change")...)
	probe.stopAndExpectNoViolations("scale-down of the initial pool")
}

// addPool adds a pool of 1 worker per zone with the initial pool's flavor and zones.
func (l *managedLane) addPool(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	_, workload := l.workload(ctx)
	initial, err := getPool(ctx, c, l.poolKey(initialPoolName))
	Expect(err).NotTo(HaveOccurred())

	machinePool, infraPool := extraPoolObjects(l.cluster(), initial, extraPoolReplicas)
	Byf("Creating worker pool %s (%d workers)", infraPool.Spec.PoolName, extraPoolReplicas)
	Expect(c.Create(ctx, infraPool)).To(Succeed())
	Expect(c.Create(ctx, machinePool)).To(Succeed())

	waitForPoolSettled(ctx, c, l.poolKey(extraPoolName), int(extraPoolReplicas), l.intervals("wait-pool-change")...)
	waitForPoolNodes(ctx, workload, extraPoolName, int(extraPoolReplicas), l.intervals("wait-pool-change")...)
	framework.WaitForMachinePoolNodesToExist(ctx, framework.WaitForMachinePoolNodesToExistInput{
		Getter: c, MachinePool: machinePool,
	}, l.intervals("wait-worker-nodes")...)
}

// extraPoolObjects returns the MachinePool and IBMCloudManagedMachinePool of the added pool: the template pool's
// flavor and zones, created after the cluster (it carries no initial-pool marker), fix packs only on request.
//
// Parameters:
//   - cluster is the CAPI Cluster.
//   - template is the initial pool, whose flavor and zones the new pool copies.
//   - replicas is the new pool's size, a multiple of its zone count.
func extraPoolObjects(cluster *clusterv1.Cluster, template *infrav1.IBMCloudManagedMachinePool, replicas int32) (*clusterv1.MachinePool, *infrav1.IBMCloudManagedMachinePool) {
	name := cluster.Name + "-" + extraPoolName
	// The cluster-name label, which CAPI would add later, is set at creation, so that the label selection of
	// expectContract finds the new pool at once.
	labels := map[string]string{clusterv1.ClusterNameLabel: cluster.Name}
	infraPool := &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName:      extraPoolName,
			Creation:      infrav1.PoolCreationAfterCluster,
			Flavor:        template.Spec.Flavor,
			Zones:         slices.Clone(template.Spec.Zones),
			WorkerUpdates: infrav1.WorkerUpdatesSpec{Policy: infrav1.WorkerUpdatePolicyManual},
		},
	}
	machinePool := &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: maps.Clone(labels)},
		Spec: clusterv1.MachinePoolSpec{
			ClusterName: cluster.Name,
			Replicas:    ptr.To(replicas),
			Template: clusterv1.MachineTemplateSpec{Spec: clusterv1.MachineSpec{
				ClusterName: cluster.Name,
				// IBM bootstraps the workers; the empty Secret name tells CAPI that no bootstrap provider is involved.
				Bootstrap: clusterv1.Bootstrap{DataSecretName: ptr.To("")},
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group,
					Kind:     "IBMCloudManagedMachinePool",
					Name:     name,
				},
			}},
		},
	}
	return machinePool, infraPool
}

// deleteInitialPool deletes the initial pool's MachinePool while the spread probe runs: CAPIBM drains every worker
// of the pool, then removes the IBM pool; the added pool keeps the cluster above IKS's minimum of 1 worker. It is the
// lane's last drain: while it drains, every zone still holds the added pool's worker next to the initial pool's, and
// the 1 worker per zone it leaves could not be drained through IBM's ALB budgets. Whether spread pods sit on the
// initial pool's workers by now depends on where the scheduler put the pods that the upgrade and the rollout.after
// evicted, and their spread is only a preference, so the step first places the pinned probe pod on one of those
// workers: the drain has to evict it, and stopAndExpectNoViolations requires that eviction to go through the Eviction
// API instead of hoping for a spread pod there.
func (l *managedLane) deleteInitialPool(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	workloadProxy, workload := l.workload(ctx)
	waitForDrainableZones(ctx, workload, l.intervals("wait-contract")...)
	waitForProbeHealthy(ctx, workload, "spread", probeReplicas, l.intervals("wait-probe")...)
	l.pinProbePod(ctx, workloadProxy, workload, initialPoolName)

	probe := startDisruptionProbe(ctx, workload)
	Byf("Deleting the initial MachinePool %s", l.poolKey(initialPoolName).Name)
	Expect(c.Delete(ctx, &clusterv1.MachinePool{ObjectMeta: metav1.ObjectMeta{
		Namespace: l.namespace.Name, Name: l.poolKey(initialPoolName).Name,
	}})).To(Succeed())
	waitForPoolGone(ctx, c, l.poolKey(initialPoolName), l.intervals("wait-pool-change")...)
	waitForPoolNodes(ctx, workload, initialPoolName, 0, l.intervals("wait-pool-change")...)
	probe.stopAndExpectNoViolations("deletion of the initial pool")
	waitForPoolSettled(ctx, c, l.poolKey(extraPoolName), int(extraPoolReplicas), l.intervals("wait-contract")...)
}

// upgrade raises spec.distributionVersion by one minor and waits for the master, then for the workers of both pools,
// which follow the master's minor on their own, while the spread probe runs. CAPIBM replaces one worker at a time per
// Cluster (its disruption token), so the follow replaces all 6 workers in turn, and while one is replaced, the other
// pool's worker in its zone keeps the zone's ALB replica running. The initial pool's workers hold every probe pod
// since the scale-down, so the follow evicts some.
func (l *managedLane) upgrade(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	_, workload := l.workload(ctx)
	target := e2eConfig.GetVariableOrEmpty(varIKSUpgradeTo)
	waitForDrainableZones(ctx, workload, l.intervals("wait-contract")...)
	waitForProbeHealthy(ctx, workload, "spread", probeReplicas, l.intervals("wait-probe")...)

	probe := startDisruptionProbe(ctx, workload)
	cp, err := getControlPlane(ctx, c, l.cluster())
	Expect(err).NotTo(HaveOccurred())
	Byf("Upgrading the master from %s to %s", cp.Spec.DistributionVersion, target)
	patch := client.MergeFrom(cp.DeepCopy())
	cp.Spec.DistributionVersion = target
	Expect(c.Patch(ctx, cp, patch)).To(Succeed())

	eventually(ctx, func(g Gomega) {
		current, err := getControlPlane(ctx, c, l.cluster())
		g.Expect(err).NotTo(HaveOccurred())
		expectConditionTrue(g, current, controlplanev1.IBMCloudManagedControlPlaneMasterVersionUpToDateCondition)
		minor, err := managedtools.IBMVersionMinor(current.Status.DistributionVersion)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(minor).To(Equal(target), "status.distributionVersion %s", current.Status.DistributionVersion)
	}, l.intervals("wait-master-upgrade")).Should(Succeed())

	Byf("Waiting for the workers of pools %s and %s to follow the master to %s", initialPoolName, extraPoolName, target)
	eventually(ctx, func(g Gomega) {
		for _, pool := range iksRollingPools {
			expectWorkersOnMinor(ctx, g, c, workload, l.poolKey(pool.name), int(pool.replicas), target)
		}
	}, l.intervals("wait-worker-rollout")).Should(Succeed())
	probe.stopAndExpectNoViolations("master upgrade and worker follow")
	l.expectContract(specCtx)
}

// expectWorkersOnMinor checks that one pool has followed the master: WorkersUpToDate is True for the pool's current
// generation, no worker disruption is in flight, and each of its replicas Nodes runs a kubelet of the target minor.
// The messages name the pool, because the upgrade checks both pools in one wait.
//
// Parameters:
//   - ctx bounds the requests.
//   - g is the Gomega of the enclosing Eventually.
//   - c is a management cluster client with managedScheme.
//   - workload is a workload cluster client.
//   - key names the IBMCloudManagedMachinePool.
//   - replicas is the pool's size.
//   - target is the master's minor, for example "1.36".
func expectWorkersOnMinor(ctx context.Context, g Gomega, c, workload client.Client, key client.ObjectKey, replicas int,
	target string) {
	pool, err := getPool(ctx, c, key)
	g.Expect(err).NotTo(HaveOccurred())
	upToDate := currentCondition(pool, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition)
	g.Expect(upToDate).NotTo(BeNil(), "pool %s condition %s for generation %d", pool.Name,
		infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition, pool.Generation)
	g.Expect(upToDate.Status).To(Equal(metav1.ConditionTrue), "pool %s condition %s: %s %s", pool.Name,
		infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition, upToDate.Reason, upToDate.Message)
	g.Expect(pool.Status.Disruption.Operation).To(BeEmpty(), "pool %s status.disruption.operation", pool.Name)
	nodes, err := poolNodes(ctx, workload, pool.Spec.PoolName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(nodes).To(HaveLen(replicas), "Nodes of IBM pool %s", pool.Spec.PoolName)
	for i := range nodes {
		minor, err := managedtools.KubeletMinor(nodes[i].Status.NodeInfo.KubeletVersion)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(minor).To(Equal(target), "kubelet of Node %s of IBM pool %s", nodes[i].Name, pool.Spec.PoolName)
	}
}

// rolloutAfterWithBlockedDrain sets spec.rollout.after on the added pool while a PodDisruptionBudget allows no
// disruption on any worker: CAPIBM must report DrainBlocked and replace nothing; once the budget is relaxed, every
// worker of the added pool whose Node is older than rollout.after is replaced, one at a time, drained. The initial
// pool's worker in each zone keeps the zone's ALB replica running meanwhile. Every drained worker holds a blocker pod,
// which carries the probe label, so the recycle evicts probe pods whatever the spread pods' placement.
func (l *managedLane) rolloutAfterWithBlockedDrain(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	workloadProxy, workload := l.workload(ctx)
	// One blocker pod per worker of the cluster (its required anti-affinity places one on each), so that each of the
	// added pool's workers holds one: both pools have settled at their sizes, and they hold every worker of the cluster.
	var blockers int32
	for _, pool := range iksRollingPools {
		waitForPoolNodes(ctx, workload, pool.name, int(pool.replicas), l.intervals("wait-contract")...)
		blockers += pool.replicas
	}
	waitForDrainableZones(ctx, workload, l.intervals("wait-contract")...)
	applyProbeManifest(ctx, workloadProxy, "pdb-blocker.yaml", map[string]string{
		"PROBE_NAMESPACE":  probeNamespace,
		"PROBE_LABEL":      probeLabel,
		"PROBE_IMAGE":      e2eConfig.GetVariableOrEmpty(varProbeImage),
		"BLOCKER_REPLICAS": fmt.Sprint(blockers),
	})
	waitForProbeHealthy(ctx, workload, "blocker", blockers, l.intervals("wait-probe")...)
	waitForProbeHealthy(ctx, workload, "spread", probeReplicas, l.intervals("wait-probe")...)

	pool, err := getPool(ctx, c, l.poolKey(extraPoolName))
	Expect(err).NotTo(HaveOccurred())
	before := slices.Clone(pool.Spec.ProviderIDList)
	probe := startDisruptionProbe(ctx, workload)
	after := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	Byf("Setting rollout.after of pool %s to %s", extraPoolName, after.Format(time.RFC3339))
	patch := client.MergeFrom(pool.DeepCopy())
	pool.Spec.Rollout.After = after
	Expect(c.Patch(ctx, pool, patch)).To(Succeed())

	drainBlocked := func(g Gomega) {
		current, err := getPool(ctx, c, l.poolKey(extraPoolName))
		g.Expect(err).NotTo(HaveOccurred())
		draining := conditions.Get(current, infrav1.IBMCloudManagedMachinePoolDrainingCondition)
		g.Expect(draining).NotTo(BeNil(), "condition %s", infrav1.IBMCloudManagedMachinePoolDrainingCondition)
		g.Expect(draining.Reason).To(Equal(infrav1.IBMCloudManagedMachinePoolDrainingDrainBlockedReason), "Draining: %s", draining.Message)
		g.Expect(current.Spec.ProviderIDList).To(ConsistOf(before), "a worker was replaced while its drain was blocked")
	}
	eventually(ctx, drainBlocked, l.intervals("wait-drain-blocked")).Should(Succeed())
	Consistently(ctx, drainBlocked, blockedDrainHold, 20*time.Second).Should(Succeed())

	Byf("Relaxing PodDisruptionBudget blocker to allow one disruption")
	relaxBlocker(ctx, workload, blockers-1)
	eventually(ctx, func(g Gomega) {
		current, err := getPool(ctx, c, l.poolKey(extraPoolName))
		g.Expect(err).NotTo(HaveOccurred())
		expectConditionTrue(g, current, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition)
		g.Expect(current.Status.Disruption.Operation).To(BeEmpty())
		g.Expect(current.Spec.ProviderIDList).To(HaveLen(len(before)))
		for _, id := range before {
			g.Expect(current.Spec.ProviderIDList).NotTo(ContainElement(id), "worker %s was not replaced", id)
		}
		replaced, err := poolNodes(ctx, workload, extraPoolName)
		g.Expect(err).NotTo(HaveOccurred())
		for i := range replaced {
			g.Expect(replaced[i].CreationTimestamp.Before(&after)).To(BeFalse(), "Node %s predates rollout.after", replaced[i].Name)
		}
	}, l.intervals("wait-worker-rollout")).Should(Succeed())
	probe.stopAndExpectNoViolations("rollout.after recycle with a blocking PodDisruptionBudget")
	deleteBlocker(ctx, workload, l.intervals("wait-probe")...)
}

// move moves the cluster to a second kind management cluster with clusterctl and checks that CAPIBM there re-adopts
// it without any IBM write: the same IBM cluster ID and master version, the same IBM pools and workers, no create
// event, and still exactly one IBM cluster with the name (the target binds through the ownership proof with the
// moved kubeconfig).
func (l *managedLane) move(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	Expect(l.input.MoveTargetIdentity).NotTo(BeNil(), "the %s lane has no move target identity", l.input.Lane)
	before, err := collectOwnershipFacts(ctx, managedClient(l.mgmt), l.cluster())
	Expect(err).NotTo(HaveOccurred(), "IBM facts before the move")

	l.createMoveTarget(ctx)
	// A move renders no template, so the target fixture's variables have no use here; it only prepares credentials.
	_, err = l.input.MoveTargetIdentity.Setup(ctx, l.moveTargetProxy, l.namespace.Name)
	Expect(err).NotTo(HaveOccurred(), "setting up the target identity fixture %T", l.input.MoveTargetIdentity)
	source := l.mgmt
	clusterctl.Move(ctx, clusterctl.MoveInput{
		LogFolder:            filepath.Join(artifactFolder, "clusters", l.moveTargetProxy.GetName()),
		ClusterctlConfigPath: clusterctlConfigPath,
		FromKubeconfigPath:   source.GetKubeconfigPath(),
		ToKubeconfigPath:     l.moveTargetProxy.GetKubeconfigPath(),
		Namespace:            l.namespace.Name,
	})
	// The CAPI objects live on the target from here on. Switch before anything else can fail, so that the cleanups
	// delete the Cluster through CAPI where it is and tear down the target's fixture, even when the source fixture's
	// Teardown below fails (a fixture other than ControllerIdentityFixture may).
	l.mgmt, l.moved = l.moveTargetProxy, true
	Expect(l.input.Identity.Teardown(ctx, source)).To(Succeed(), "tearing down the source identity fixture")

	c := managedClient(l.mgmt)
	waitForControlPlaneAvailable(ctx, c, l.cluster(), l.intervals("wait-move")...)
	var after ownershipFacts
	eventually(ctx, func(g Gomega) {
		cp, err := getControlPlane(ctx, c, l.cluster())
		g.Expect(err).NotTo(HaveOccurred())
		expectConditionTrue(g, cp, controlplanev1.IBMCloudManagedControlPlaneIBMClusterProvisionedCondition)
		after, err = collectOwnershipFacts(ctx, c, l.cluster())
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(after).To(Equal(before), "IBM facts after the move")
	}, l.intervals("wait-move")).Should(Succeed())
	expectNoEventWithReason(ctx, c, l.namespace.Name, managed.EventReasonSuccessfulCreateIBMCluster)

	clusters, err := l.ibm.clusters.ListClusters(ctx)
	Expect(err).NotTo(HaveOccurred())
	var ids []string
	for _, cluster := range clusters {
		if cluster.Name == l.clusterName {
			ids = append(ids, cluster.ID)
		}
	}
	Expect(ids).To(Equal([]string{before.ClusterID}), "IBM clusters named %s after the move", l.clusterName)
}

// createMoveTarget creates the second kind management cluster and installs the providers, as the suite does for the
// first one (suite_test.go setupBootstrapCluster, initBootstrapCluster).
func (l *managedLane) createMoveTarget(ctx context.Context) {
	name := e2eConfig.ManagementClusterName + "-move"
	input := bootstrap.CreateKindBootstrapClusterAndLoadImagesInput{
		Name:      name,
		Images:    e2eConfig.Images,
		LogFolder: filepath.Join(artifactFolder, "kind", name),
	}
	if image := e2eConfig.GetVariableOrEmpty(CustomKindNodeImage); image != "" {
		input.CustomNodeImage = image
	}
	l.moveTargetProvider = bootstrap.CreateKindBootstrapClusterAndLoadImages(ctx, input)
	Expect(l.moveTargetProvider).NotTo(BeNil(), "creating the move target %s", name)
	l.moveTargetProxy = framework.NewClusterProxy(name, l.moveTargetProvider.GetKubeconfigPath(), initScheme())
	clusterctl.InitManagementClusterAndWatchControllerLogs(ctx, clusterctl.InitManagementClusterAndWatchControllerLogsInput{
		ClusterProxy:            l.moveTargetProxy,
		ClusterctlConfigPath:    clusterctlConfigPath,
		InfrastructureProviders: e2eConfig.InfrastructureProviders(),
		LogFolder:               filepath.Join(artifactFolder, "clusters", name),
	}, l.intervals("wait-controllers")...)
}

// delete deletes the Cluster and checks that IBM reports the cluster as gone (404 from CAPIBM's own client) and that
// CAPIBM recorded the LeftoverResources warning naming what IKS leaves in the VPC (the shared VPE gateways and
// security group, which IKS never deletes).
func (l *managedLane) delete(specCtx SpecContext) {
	ctx := l.stepContext(specCtx)
	c := managedClient(l.mgmt)
	cp, err := getControlPlane(ctx, c, l.cluster())
	Expect(err).NotTo(HaveOccurred())
	clusterID := cp.Status.ClusterID
	Expect(clusterID).NotTo(BeEmpty(), "status.clusterID before the deletion")

	framework.DeleteCluster(ctx, framework.DeleteClusterInput{Deleter: c, Cluster: l.cluster()})
	framework.WaitForClusterDeleted(ctx, framework.WaitForClusterDeletedInput{
		ClusterProxy:         l.mgmt,
		ClusterctlConfigPath: clusterctlConfigPath,
		Cluster:              l.cluster(),
		ArtifactFolder:       artifactFolder,
	}, l.intervals("wait-delete-cluster")...)
	l.clusterDeleted = true

	Byf("Checking that IBM answers 404 for cluster %s", clusterID)
	eventually(ctx, func(g Gomega) {
		_, err := l.ibm.service.GetCluster(ctx, clusterID, l.ibm.resourceGroupID)
		g.Expect(kubernetesservice.Classify(kubernetesservice.OpGetCluster, err)).To(Equal(kubernetesservice.ClassNotFound),
			"GetCluster %s: %v", clusterID, err)
	}, l.intervals("wait-ibm-404")).Should(Succeed())

	event, err := findEvent(ctx, c, l.namespace.Name, cp.Name, managed.EventReasonLeftoverResources)
	Expect(err).NotTo(HaveOccurred())
	Expect(event.Message).To(ContainSubstring(clusterID), "the LeftoverResources event names the deleted cluster")
}

// capiCleanup dumps the namespace's resources and, unless -e2e.skip-resource-cleanup is set, deletes every Cluster
// of the namespace through CAPI and then the namespace, whatever the outcome of the lane's specs.
func (l *managedLane) capiCleanup(ctx SpecContext) {
	if l.namespace == nil {
		return
	}
	var cluster *clusterv1.Cluster
	if l.result != nil {
		cluster = l.result.Cluster
	}
	dumpSpecResourcesAndCleanup(ctx, cleanupInput{
		SpecName:          l.input.SpecName,
		Cluster:           cluster,
		ClusterProxy:      l.mgmt,
		ClusterConfigPath: clusterctlConfigPath,
		ArtifactFolder:    artifactFolder,
		Namespace:         l.namespace,
		CancelWatches:     l.cancelWatches,
		IntervalsGetter:   e2eConfig.GetIntervals,
		SkipCleanup:       skipCleanup,
	})
}

// ibmBackstop deletes, by ID, the run's own IBM cluster if it still exists after the CAPI deletion: an interrupted
// or failed lane must not leave a billable cluster until the janitor's next run. It fails the lane when the CAPI
// deletion had reported success, because then CAPIBM let the Cluster go while IBM still had the cluster.
func (l *managedLane) ibmBackstop(ctx SpecContext) {
	if l.runStart.IsZero() {
		return
	}
	if skipCleanup {
		if l.janitor {
			Byf("Skipping the IBM backstop (-e2e.skip-resource-cleanup); the janitor deletes %s after %s",
				l.clusterName, managedtools.JanitorMinimumAge)
		} else {
			Byf("Skipping the IBM backstop (-e2e.skip-resource-cleanup); the janitor is off (%s=false), so %s stays "+
				"until whoever shares the account deletes it", managedtools.JanitorVariable, l.clusterName)
		}
		return
	}
	report, err := managedtools.DeleteRunClusters(ctx, l.ibm.clusters, []string{l.clusterName}, l.runStart, false)
	Byf("IBM backstop for %s:\n%s", l.clusterName, report)
	Expect(err).NotTo(HaveOccurred(), "the IBM backstop could not delete the run's cluster")
	if l.clusterDeleted {
		Expect(report.Deleted).To(BeEmpty(), "the IBM cluster outlived the deletion of its CAPI Cluster")
	}
}

// finalCleanup compares the inventory with the baseline, within the lane's leftover scope, until the run left nothing
// behind, then tears down the identity fixture and the move target.
func (l *managedLane) finalCleanup(ctx SpecContext) {
	defer l.disposeMoveTarget(ctx)
	defer l.teardownIdentity(ctx)
	if !l.baselineTaken || skipCleanup {
		return
	}
	Byf("Comparing the IBM inventory with the baseline of %s", l.baseline.TakenAt.Format(time.RFC3339))
	eventually(ctx, func(g Gomega) {
		after, err := l.snapshot(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		diff := managedtools.Diff(l.baseline, after)
		g.Expect(diff.Clean()).To(BeTrue(), "resources the run left behind (IKS removes per-cluster ones within "+
			"about 10 minutes):\n%s", diff)
	}, l.configuredIntervals("wait-inventory-clean")).Should(Succeed())
}

// teardownIdentity tears down the identity fixture of the management cluster that owns the lane's namespace now.
func (l *managedLane) teardownIdentity(ctx context.Context) {
	if l.namespace == nil {
		return
	}
	identity := l.input.Identity
	if l.moved {
		identity = l.input.MoveTargetIdentity
	}
	Expect(identity.Teardown(ctx, l.mgmt)).To(Succeed(), "tearing down the identity fixture %T", identity)
}

// disposeMoveTarget deletes the move target's kind cluster, unless -e2e.skip-resource-cleanup keeps it for debugging.
func (l *managedLane) disposeMoveTarget(ctx context.Context) {
	if l.moveTargetProxy == nil || skipCleanup {
		return
	}
	l.moveTargetProxy.Dispose(ctx)
	l.moveTargetProvider.Dispose(ctx)
}
