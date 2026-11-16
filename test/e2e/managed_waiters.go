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

// Package e2e holds the CAPIBM end-to-end suites: the VPC and PowerVS workload cluster specs (e2e_test.go,
// suite_test.go) and the managed Kubernetes (IKS and ROKS) lanes (managed_test.go with these waiters and the identity
// fixture of managed_identity.go). They run with the e2e build tag against a real IBM Cloud account.
package e2e

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/test/e2e/managedtools"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// probeNamespace is the workload-cluster namespace of the PodDisruptionBudget probe workloads
	// (test/e2e/data/managed).
	probeNamespace = "capibm-e2e-probe"
	// probeLabel marks every probe pod; the disruption probe lists pods by it.
	probeLabel = "capibm-e2e/probe"
	// varProbeImage is the e2e config variable with the probe pods' image (test/e2e/config/ibmcloud-e2e-managed.yaml).
	// It is declared here, not with the other variables in managed_test.go, because waitForProbeHealthy names it in
	// its failure message and this non-test file must not depend on a _test.go file.
	varProbeImage = "MANAGED_E2E_PROBE_IMAGE"
	// probePollInterval is how often the disruption probe lists the probe pods and budgets. The probe pods stay
	// terminating for about 20 seconds after an eviction (their SIGTERM handler), ten times this interval.
	probePollInterval = 2 * time.Second
	// workerPoolNameLabel is the Node label that names a worker's IBM pool (IBM sets it on IKS and ROKS Nodes).
	workerPoolNameLabel = "ibm-cloud.kubernetes.io/worker-pool-name"
	// managedDataDir holds the probe manifests. Ginkgo runs the suite with the package directory as the working
	// directory, so the path is relative to test/e2e.
	managedDataDir = "data/managed"
)

// managedScheme returns a scheme with the Kubernetes, CAPI and CAPIBM managed kinds. The bootstrap proxy's scheme
// (suite_test.go initScheme) lacks the managed kinds and the policy group, and the managed specs keep that suite file
// unchanged, so they build their own clients with this scheme.
func managedScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	framework.TryAddDefaultSchemes(scheme)
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(infrav1.AddToScheme(scheme)).To(Succeed())
	Expect(controlplanev1.AddToScheme(scheme)).To(Succeed())
	return scheme
}

// managedClient returns an uncached client for proxy's cluster with managedScheme.
//
// Parameters:
//   - proxy is a management or workload cluster proxy.
func managedClient(proxy framework.ClusterProxy) client.Client {
	c, err := client.New(proxy.GetRESTConfig(), client.Options{Scheme: managedScheme()})
	Expect(err).NotTo(HaveOccurred(), "building a client for cluster %s", proxy.GetName())
	return c
}

// managedControlPlaneWaiters replaces the framework's KubeadmControlPlane waiters (clusterctl_helpers.go setDefaults)
// for the managed flavors, as CAPZ's AKS and CAPA's EKS suites do: the control plane is initialized when the
// Cluster's ControlPlaneInitialized condition and the IBMCloudManagedControlPlane's Available condition are True, and
// there are no control plane Machines to wait for, because IBM runs the masters.
func managedControlPlaneWaiters() clusterctl.ControlPlaneWaiters {
	return clusterctl.ControlPlaneWaiters{
		WaitForControlPlaneInitialized: func(ctx context.Context, input clusterctl.ApplyCustomClusterTemplateAndWaitInput,
			result *clusterctl.ApplyCustomClusterTemplateAndWaitResult) {
			waitForControlPlaneAvailable(ctx, managedClient(input.ClusterProxy), result.Cluster, input.WaitForControlPlaneIntervals...)
		},
		WaitForControlPlaneMachinesReady: func(context.Context, clusterctl.ApplyCustomClusterTemplateAndWaitInput,
			*clusterctl.ApplyCustomClusterTemplateAndWaitResult) {
		},
	}
}

// getControlPlane returns the cluster's IBMCloudManagedControlPlane.
//
// Parameters:
//   - ctx bounds the request.
//   - c is a management cluster client with managedScheme.
//   - cluster is the CAPI Cluster; its controlPlaneRef names the control plane.
func getControlPlane(ctx context.Context, c client.Client, cluster *clusterv1.Cluster) (*controlplanev1.IBMCloudManagedControlPlane, error) {
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.ControlPlaneRef.Name}
	if key.Name == "" {
		key.Name = cluster.Name + "-control-plane"
	}
	return cp, c.Get(ctx, key, cp)
}

// waitForControlPlaneAvailable waits until the Cluster reports ControlPlaneInitialized and the control plane
// reports Available.
//
// Parameters:
//   - ctx bounds the wait.
//   - c is a management cluster client with managedScheme.
//   - cluster is the CAPI Cluster (only its namespace and name are used).
//   - intervals are the Eventually timeout and polling interval.
func waitForControlPlaneAvailable(ctx context.Context, c client.Client, cluster *clusterv1.Cluster, intervals ...interface{}) {
	// By(fmt.Sprintf(...)), not Byf: Byf lives in e2e_test.go, which a build of this non-test file does not include.
	By(fmt.Sprintf("Waiting for the IBM cluster of %s to be created and available", cluster.Name))
	eventually(ctx, func(g Gomega) {
		current := &clusterv1.Cluster{}
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(cluster), current)).To(Succeed())
		g.Expect(conditions.IsTrue(current, clusterv1.ClusterControlPlaneInitializedCondition)).To(BeTrue(),
			"Cluster condition %s", clusterv1.ClusterControlPlaneInitializedCondition)
		cp, err := getControlPlane(ctx, c, current)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(conditions.IsTrue(cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition)).To(BeTrue(),
			"control plane condition %s: %v", controlplanev1.IBMCloudManagedControlPlaneAvailableCondition,
			conditions.Get(cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition))
	}, intervals).Should(Succeed())
}

// currentCondition returns the condition of conditionType when the controller has evaluated the object's current
// generation, so that a condition left from before a spec change is not mistaken for its outcome; otherwise nil.
//
// Parameters:
//   - obj is the object with conditions.
//   - conditionType is the condition type.
func currentCondition(obj interface {
	conditions.Getter
	GetGeneration() int64
}, conditionType string) *metav1.Condition {
	condition := conditions.Get(obj, conditionType)
	if condition == nil || condition.ObservedGeneration < obj.GetGeneration() {
		return nil
	}
	return condition
}

// expectConditionTrue fails g unless the object's condition of conditionType is True for its current generation.
//
// Parameters:
//   - g is the Gomega of the enclosing Eventually.
//   - obj is the object with conditions.
//   - conditionType is the condition type.
func expectConditionTrue(g Gomega, obj interface {
	conditions.Getter
	GetGeneration() int64
}, conditionType string) {
	condition := currentCondition(obj, conditionType)
	g.Expect(condition).NotTo(BeNil(), "condition %s for generation %d", conditionType, obj.GetGeneration())
	g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), "condition %s: %s %s", conditionType, condition.Reason, condition.Message)
}

// getPool returns an IBMCloudManagedMachinePool.
//
// Parameters:
//   - ctx bounds the request.
//   - c is a management cluster client with managedScheme.
//   - key names the pool.
func getPool(ctx context.Context, c client.Client, key client.ObjectKey) (*infrav1.IBMCloudManagedMachinePool, error) {
	pool := &infrav1.IBMCloudManagedMachinePool{}
	return pool, c.Get(ctx, key, pool)
}

// waitForPoolSettled waits until a pool has replicas workers in its providerIDList, reports WorkerPoolReady for its
// current generation and has no worker disruption in flight, and returns it.
//
// Parameters:
//   - ctx bounds the wait.
//   - c is a management cluster client with managedScheme.
//   - key names the IBMCloudManagedMachinePool.
//   - replicas is the expected number of workers.
//   - intervals are the Eventually timeout and polling interval.
func waitForPoolSettled(ctx context.Context, c client.Client, key client.ObjectKey, replicas int, intervals ...interface{}) *infrav1.IBMCloudManagedMachinePool {
	By(fmt.Sprintf("Waiting for pool %s to settle at %d workers", key.Name, replicas))
	var pool *infrav1.IBMCloudManagedMachinePool
	eventually(ctx, func(g Gomega) {
		var err error
		pool, err = getPool(ctx, c, key)
		g.Expect(err).NotTo(HaveOccurred())
		expectConditionTrue(g, pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition)
		g.Expect(pool.Spec.ProviderIDList).To(HaveLen(replicas), "spec.providerIDList")
		g.Expect(pool.Status.Disruption.Operation).To(BeEmpty(), "status.disruption.operation")
	}, intervals).Should(Succeed())
	return pool
}

// waitForPoolGone waits until a MachinePool and its IBMCloudManagedMachinePool no longer exist.
//
// Parameters:
//   - ctx bounds the wait.
//   - c is a management cluster client with managedScheme.
//   - key names both objects (the managed flavors give them the same name).
//   - intervals are the Eventually timeout and polling interval.
func waitForPoolGone(ctx context.Context, c client.Client, key client.ObjectKey, intervals ...interface{}) {
	By(fmt.Sprintf("Waiting for pool %s to be removed", key.Name))
	eventually(ctx, func(g Gomega) {
		g.Expect(isNotFound(c.Get(ctx, key, &clusterv1.MachinePool{}))).To(BeTrue(), "MachinePool %s", key.Name)
		g.Expect(isNotFound(c.Get(ctx, key, &infrav1.IBMCloudManagedMachinePool{}))).To(BeTrue(), "IBMCloudManagedMachinePool %s", key.Name)
	}, intervals).Should(Succeed())
}

// isNotFound reports whether err is a Kubernetes NotFound error.
func isNotFound(err error) bool {
	return client.IgnoreNotFound(err) == nil && err != nil
}

// poolNodes returns the workload cluster's Nodes of one IBM worker pool.
//
// Parameters:
//   - ctx bounds the request.
//   - workload is a workload cluster client.
//   - poolName is the IBM pool name (spec.poolName).
func poolNodes(ctx context.Context, workload client.Client, poolName string) ([]corev1.Node, error) {
	nodes := &corev1.NodeList{}
	if err := workload.List(ctx, nodes, client.MatchingLabels{workerPoolNameLabel: poolName}); err != nil {
		return nil, err
	}
	return nodes.Items, nil
}

// applyProbeManifest applies one probe manifest of test/e2e/data/managed to the workload cluster after filling its
// placeholders from vars (managedtools.ExpandPlaceholders; no environment variable is read).
//
// Parameters:
//   - ctx bounds the requests.
//   - workload is the workload cluster proxy.
//   - file is the manifest's file name in test/e2e/data/managed, for example "pdb-probe.yaml"; os.DirFS keeps it
//     inside that directory.
//   - vars are the placeholder values; a placeholder without a value fails the step.
func applyProbeManifest(ctx context.Context, workload framework.ClusterProxy, file string, vars map[string]string) {
	raw, err := fs.ReadFile(os.DirFS(managedDataDir), file)
	Expect(err).NotTo(HaveOccurred(), "reading probe manifest %s", file)
	manifest, err := managedtools.ExpandPlaceholders(string(raw), vars)
	Expect(err).NotTo(HaveOccurred(), "probe manifest %s", file)
	Expect(workload.CreateOrUpdate(ctx, []byte(manifest))).To(Succeed(), "applying probe manifest %s", file)
}

// waitForProbeHealthy waits until the probe Deployment named name has replicas available pods and its
// PodDisruptionBudget (same name) counts all of them as healthy, so that a probe started afterwards sees no start-up
// gap as a violation.
//
// Parameters:
//   - ctx bounds the wait.
//   - workload is a workload cluster client with managedScheme.
//   - name names the Deployment and its PodDisruptionBudget.
//   - replicas is the Deployment's replica count.
//   - intervals are the Eventually timeout and polling interval.
func waitForProbeHealthy(ctx context.Context, workload client.Client, name string, replicas int32, intervals ...interface{}) {
	By(fmt.Sprintf("Waiting for probe workload %s to be healthy", name))
	key := client.ObjectKey{Namespace: probeNamespace, Name: name}
	eventually(ctx, func(g Gomega) {
		budget := &policyv1.PodDisruptionBudget{}
		g.Expect(workload.Get(ctx, key, budget)).To(Succeed())
		g.Expect(budget.Status.ObservedGeneration).To(BeNumerically(">=", budget.Generation))
		g.Expect(budget.Status.CurrentHealthy).To(Equal(replicas), "PodDisruptionBudget %s healthy pods (if the "+
			"probe pods cannot pull their image because the workers have no public egress, set %s to a copy the "+
			"workers reach over private endpoints)", name, varProbeImage)
		g.Expect(budget.Status.ExpectedPods).To(Equal(replicas), "PodDisruptionBudget %s expected pods", name)
	}, intervals).Should(Succeed())
}

// disruptionProbe polls the probe pods and budgets of a workload cluster in the background while a step drains
// workers, and feeds a managedtools.DisruptionRecorder. Start it with startDisruptionProbe and end it with
// stopAndExpectNoViolations.
type disruptionProbe struct {
	cancel     context.CancelFunc
	done       chan struct{}
	recorder   *managedtools.DisruptionRecorder
	listErrors int
}

// startDisruptionProbe starts polling. The goroutine is the e2e's only background work; it ends when ctx ends or the
// probe is stopped.
//
// Parameters:
//   - ctx bounds the probe.
//   - workload is a workload cluster client with managedScheme.
func startDisruptionProbe(ctx context.Context, workload client.Client) *disruptionProbe {
	ctx, cancel := context.WithCancel(ctx)
	probe := &disruptionProbe{cancel: cancel, done: make(chan struct{}), recorder: managedtools.NewDisruptionRecorder()}
	go func() {
		defer GinkgoRecover()
		defer close(probe.done)
		ticker := time.NewTicker(probePollInterval)
		defer ticker.Stop()
		for {
			probe.poll(ctx, workload)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return probe
}

// poll records one listing of the probe pods and budgets. A failed listing is counted and skipped: the recorder must
// never see a partial or empty listing (see managedtools.DisruptionRecorder.ObservePods).
func (p *disruptionProbe) poll(ctx context.Context, workload client.Client) {
	pods := &corev1.PodList{}
	if err := workload.List(ctx, pods, client.InNamespace(probeNamespace), client.HasLabels{probeLabel}); err != nil {
		p.listErrors++
		return
	}
	budgets := &policyv1.PodDisruptionBudgetList{}
	if err := workload.List(ctx, budgets, client.InNamespace(probeNamespace)); err != nil {
		p.listErrors++
		return
	}
	p.recorder.ObservePods(pods.Items)
	p.recorder.ObserveBudgets(budgets.Items, time.Now())
}

// stopAndExpectNoViolations stops the probe and fails the step unless every budget held, every removed probe pod was
// evicted through the Eviction API, and at least one probe pod was evicted (a step that evicted nothing proved
// nothing about draining).
//
// Parameters:
//   - step names the step in the failure message.
func (p *disruptionProbe) stopAndExpectNoViolations(step string) {
	p.cancel()
	<-p.done
	result := p.recorder.Result()
	By(fmt.Sprintf("PodDisruptionBudget probe for %q: %d listings, %d failed listings, %d pods evicted", step,
		result.Polls, p.listErrors, result.Evicted))
	Expect(result.Problems).To(BeEmpty(), "PodDisruptionBudget violations during %q:\n%s", step,
		strings.Join(result.Problems, "\n"))
	Expect(result.Evicted).To(BeNumerically(">", 0), "no probe pod was evicted during %q, so the step did not "+
		"exercise the drain", step)
}

// expectNoEventWithReason fails unless no Event in the namespace has the reason.
//
// Parameters:
//   - ctx bounds the request.
//   - c is a management cluster client.
//   - namespace is the namespace of the events.
//   - reason is the event reason, for example SuccessfulCreateIBMCluster.
func expectNoEventWithReason(ctx context.Context, c client.Client, namespace, reason string) {
	events := &corev1.EventList{}
	Expect(c.List(ctx, events, client.InNamespace(namespace))).To(Succeed())
	for _, event := range events.Items {
		Expect(event.Reason).NotTo(Equal(reason), "event %s on %s/%s: %s", reason, event.InvolvedObject.Kind,
			event.InvolvedObject.Name, event.Message)
	}
}

// findEvent returns the first Event in the namespace with the reason about the named object, or an error.
//
// Parameters:
//   - ctx bounds the request.
//   - c is a management cluster client.
//   - namespace is the namespace of the events.
//   - objectName is the involved object's name.
//   - reason is the event reason.
func findEvent(ctx context.Context, c client.Client, namespace, objectName, reason string) (*corev1.Event, error) {
	events := &corev1.EventList{}
	if err := c.List(ctx, events, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for i := range events.Items {
		if events.Items[i].Reason == reason && events.Items[i].InvolvedObject.Name == objectName {
			return &events.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no %s event about %s in namespace %s", reason, objectName, namespace)
}

// relaxBlocker lowers the blocker PodDisruptionBudget's minAvailable so that the drain may evict one blocker pod.
//
// Parameters:
//   - ctx bounds the request.
//   - workload is a workload cluster client with managedScheme.
//   - minAvailable is the new minimum, one less than the blocker's replicas.
func relaxBlocker(ctx context.Context, workload client.Client, minAvailable int32) {
	budget := &policyv1.PodDisruptionBudget{}
	Expect(workload.Get(ctx, client.ObjectKey{Namespace: probeNamespace, Name: "blocker"}, budget)).To(Succeed())
	patch := client.MergeFrom(budget.DeepCopy())
	budget.Spec.MinAvailable = ptr.To(intstr.FromInt32(minAvailable))
	Expect(workload.Patch(ctx, budget, patch)).To(Succeed())
}

// deleteBlocker deletes the blocker Deployment and its PodDisruptionBudget.
//
// Parameters:
//   - ctx bounds the requests.
//   - workload is a workload cluster client with managedScheme.
func deleteBlocker(ctx context.Context, workload client.Client) {
	meta := metav1.ObjectMeta{Namespace: probeNamespace, Name: "blocker"}
	Expect(client.IgnoreNotFound(workload.Delete(ctx, &appsv1.Deployment{ObjectMeta: meta}))).To(Succeed())
	Expect(client.IgnoreNotFound(workload.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: meta}))).To(Succeed())
}

// ownershipFacts are the IBM facts that a clusterctl move must not change: CAPIBM re-adopts the same IBM cluster and
// pools, and replaces, adds or removes no worker.
type ownershipFacts struct {
	// ClusterID is the control plane's status.clusterID.
	ClusterID string
	// MasterVersion is the control plane's status.distributionVersion.
	MasterVersion string
	// Pools maps each IBMCloudManagedMachinePool name to its IBM pool.
	Pools map[string]poolFacts
}

// poolFacts are one pool's IBM facts.
type poolFacts struct {
	// PoolID is status.poolID.
	PoolID string
	// ProviderIDs is spec.providerIDList, sorted.
	ProviderIDs []string
}

// collectOwnershipFacts reads the ownership facts of a cluster; every ID must be set, so that two empty fact sets can
// never compare equal.
//
// Parameters:
//   - ctx bounds the requests.
//   - c is a management cluster client with managedScheme.
//   - cluster is the CAPI Cluster; its namespace holds only this cluster's objects.
func collectOwnershipFacts(ctx context.Context, c client.Client, cluster *clusterv1.Cluster) (ownershipFacts, error) {
	cp, err := getControlPlane(ctx, c, cluster)
	if err != nil {
		return ownershipFacts{}, err
	}
	if cp.Status.ClusterID == "" {
		return ownershipFacts{}, fmt.Errorf("control plane %s has no status.clusterID yet", cp.Name)
	}
	pools := &infrav1.IBMCloudManagedMachinePoolList{}
	if err := c.List(ctx, pools, client.InNamespace(cluster.Namespace)); err != nil {
		return ownershipFacts{}, err
	}
	facts := ownershipFacts{ClusterID: cp.Status.ClusterID, MasterVersion: cp.Status.DistributionVersion,
		Pools: map[string]poolFacts{}}
	for i := range pools.Items {
		pool := &pools.Items[i]
		if pool.Status.PoolID == "" {
			return ownershipFacts{}, fmt.Errorf("pool %s has no status.poolID yet", pool.Name)
		}
		ids := slices.Clone(pool.Spec.ProviderIDList)
		slices.Sort(ids)
		facts.Pools[pool.Name] = poolFacts{PoolID: pool.Status.PoolID, ProviderIDs: ids}
	}
	return facts, nil
}

// eventually is Eventually(ctx, f) with the timeout and polling interval of an e2e config interval. Go does not allow
// Eventually(ctx, f, intervals...), so the arguments are assembled here.
//
// Parameters:
//   - ctx ends the polling early (the lane's hard timeout).
//   - f is the polled function, typically func(g Gomega).
//   - intervals are the timeout and polling interval from E2EConfig.GetIntervals.
func eventually(ctx context.Context, f interface{}, intervals []interface{}) AsyncAssertion {
	return Eventually(ctx, append([]interface{}{f}, intervals...)...)
}
