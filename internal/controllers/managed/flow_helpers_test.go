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
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/index"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	ksmock "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice/mock"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/workloadprobe"

	. "github.com/onsi/gomega"
)

// This file is the harness of the cross-cutting tests (crosscutting_test.go): one managed Cluster driven end to end
// through the three managed controllers against the envtest API server, the way a GitOps tool, Cluster API and IBM
// Cloud drive it in production.
//
//   - The user is a GitOps tool: it declares the objects with server-side apply, under its own field manager and user
//     agent ("user"), and applies its unchanged manifests again after every step, without forcing ownership.
//   - Cluster API is simulated by a third client ("capi"): owner references, cluster-name labels, its finalizers,
//     status.initialization.infrastructureProvisioned and the deletion order of a Cluster (MachinePools, then the
//     control plane, then the InfraCluster).
//   - IBM Cloud is a stateful simulator behind the gomock Kubernetes Service mock (flowIBM): what CAPIBM asks for
//     changes its state, and in each round IBM moves one step further (a cluster or a worker becomes ready, a replaced
//     worker is deleted and its successor provisioned), with the Nodes of the workload cluster following.
//   - The workload cluster is a controller-runtime fake client behind interceptors that record every call
//     (flowWorkload), handed to the pool controller through the fake ClusterCache.
//   - The three controllers run with a client whose user agent is "controller", so that the API server's managedFields
//     name every field CAPIBM wrote, and an interceptor records every write that client makes (flowWrites).
//
// Reconcile is called directly, once per controller and object in each round, with one fake clock that a round moves
// by flowRoundStep, so a flow is deterministic. No manager runs in a flow; the Cluster is paused at cleanup all the
// same, so that a later test whose manager watches every namespace leaves the objects of a failed flow alone.

// The user agents of the flow, which are also the field managers the API server records: a client-side write is
// recorded under the first token of its user agent, a server-side apply under its field owner.
const (
	// flowUserManager is the GitOps tool that declares the objects.
	flowUserManager = "user"
	// flowControllerManager is CAPIBM's three managed controllers.
	flowControllerManager = "controller"
	// flowCAPIManager is the simulated Cluster API core controllers.
	flowCAPIManager = "capi"
)

// The objects of a flow, named like the clusterctl flavor managed-iks names them (templates/).
const (
	flowClusterName = "dev"
	flowCPName      = "dev-control-plane"
	// flowDefaultPool names the initial pool (spec.creation WithCluster, IBM pool "default") and its MachinePool.
	flowDefaultPool = "dev-default"
	// flowSparePool names the optional second pool (IBM pool "spare", created after the cluster) and its MachinePool.
	flowSparePool = "dev-spare"
	// flowZone is the one zone of every pool; the IBMCloudManagedCluster has two (testInfraSpec).
	flowZone = "us-south-1"
	// flowClusterID and flowClusterCRN identify the IBM cluster IBM creates; the account is the scrubbed placeholder.
	flowClusterID  = "c0flowcluster0000001"
	flowClusterCRN = "crn:v1:bluemix:public:containers-kubernetes:us-south:a/" + mpTestAccount + ":" + flowClusterID + "::"
	// flowPodNamespace is the workload namespace of the pods on the workers' Nodes.
	flowPodNamespace = "apps"
)

// The finalizers the simulated Cluster API sets, so that a deleted Cluster and MachinePool go in Cluster API's order.
const (
	flowClusterFinalizer     = "cluster.cluster.x-k8s.io"
	flowMachinePoolFinalizer = "machinepool.cluster.x-k8s.io"
)

// flowRoundStep is how far one round moves the clock: shorter than every poll a flow waits for (the settle delay of
// three minutes, the master update grace of five), so that each wait takes a few rounds.
const flowRoundStep = time.Minute

// flowMasterFixPacks are the master versions IBM runs for a requested minor, and flowWorkerFixPacks the fix packs it
// installs on a new or replaced worker of a minor until offerFixPack changes them. The worker fix packs are independent
// of the master's: on IBM Cloud, new workers were observed on a newer fix pack than the master of the same minor.
var (
	flowMasterFixPacks = map[string]string{"1.36": "1.36.5_1537", "1.37": "1.37.1_1549"}
	flowWorkerFixPacks = map[string]string{"1.36": "1.36.5_1540", "1.37": "1.37.1_1550"}
)

// flowOptions shape a flow.
type flowOptions struct {
	// namespace is the prefix of the test namespace.
	namespace string
	// replicas is spec.replicas of the initial pool's MachinePool; 0 means 1.
	replicas int32
	// sparePool adds a second pool of two workers, created by the pool controller after the cluster.
	sparePool bool
	// pods puts one pod on every Node of the workload cluster, so that a drain has something to evict.
	pods bool
}

// managedFlow is one managed Cluster driven end to end; see the comment at the top of this file.
type managedFlow struct {
	t         *testing.T
	namespace string
	clock     *clocktesting.FakeClock
	ibm       *flowIBM
	workload  *flowWorkload
	// user, capi and reader are the GitOps tool, the simulated Cluster API and an uncached reader for the checks.
	user   client.Client
	capi   client.Client
	reader client.Reader
	// writes records every write of the controllers' client.
	writes   *flowWrites
	recorder *record.FakeRecorder
	// events are the events the controllers emitted so far ("<type> <reason> <message>").
	events []string
	prober *kubeconfigProber
	infraR *IBMCloudManagedClusterReconciler
	cpR    *IBMCloudManagedControlPlaneReconciler
	poolR  *IBMCloudManagedMachinePoolReconciler
	// manifests are the objects the user declares, by object key; reapply applies them again.
	manifests map[string]client.Object
	// manifestOrder is the order the user applied the manifests in.
	manifestOrder []string
	// reversePools reconciles the pools in reverse name order.
	reversePools bool
	// alternatePools flips reversePools at the start of every round, so that each pool reconciles first in turn.
	alternatePools bool
	// afterReconcile, when set, runs after every single Reconcile call of a round (the drain-safety invariants).
	afterReconcile func()
}

// newManagedFlow returns a flow in a new namespace with the user's manifests prepared but not yet applied, and the
// three controllers wired to the simulated IBM Cloud and workload cluster.
//
// Parameters:
//   - t is the test.
//   - opts shape the flow.
func newManagedFlow(t *testing.T, opts flowOptions) *managedFlow {
	t.Helper()
	g := NewWithT(t)
	namespace := newTestNamespace(t, opts.namespace)
	// Registered before anything else of the flow, so that it runs last (t.Cleanup runs last in, first out).
	t.Cleanup(func() {
		pauseTestCluster(t, &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: flowClusterName}})
	})
	f := &managedFlow{
		t:         t,
		namespace: namespace,
		clock:     clocktesting.NewFakeClock(cpTestStart),
		user:      flowClient(t, flowUserManager, nil),
		capi:      flowClient(t, flowCAPIManager, nil),
		reader:    testEnv.GetAPIReader(),
		writes:    &flowWrites{},
		recorder:  record.NewFakeRecorder(1000),
		prober:    &kubeconfigProber{t: t},
		manifests: map[string]client.Object{},
	}
	f.workload = newFlowWorkload(t, f.clock, opts.pods)
	kube := ksmock.NewMockKubernetesService(gomock.NewController(t))
	f.ibm = newFlowIBM(t, kube, f.clock, f.workload)
	f.prober.setAnswer(func(probedKubeconfig) (workloadprobe.Result, error) { return f.ibm.probe() })
	network := newIBMFakes(t)
	network.expectNetwork(true)
	builder := &stubClientBuilder{kubernetes: kube, vpc: network.vpc, resourceManager: network.rm}
	versions, err := managedscope.NewVersionsCache(f.clock, time.Hour)
	g.Expect(err).NotTo(HaveOccurred())

	controller := flowClient(t, flowControllerManager, f.writes)
	resolver := &recordingResolver{}
	f.infraR = &IBMCloudManagedClusterReconciler{Client: controller, Resolver: resolver, ClientBuilder: builder}
	f.cpR = &IBMCloudManagedControlPlaneReconciler{Client: controller, APIReader: controller, Recorder: f.recorder,
		Resolver: resolver, ClientBuilder: builder, Prober: f.prober, VersionsCache: versions, Clock: f.clock}
	f.poolR = &IBMCloudManagedMachinePoolReconciler{Client: controller, APIReader: controller, Recorder: f.recorder,
		Resolver: resolver, ClientBuilder: builder, Clock: f.clock,
		ClusterCache: clustercache.NewFakeClusterCache(f.workload.client, client.ObjectKey{Namespace: namespace, Name: flowClusterName})}

	replicas := opts.replicas
	if replicas == 0 {
		replicas = 1
	}
	f.declare(f.clusterManifest(), f.infraManifest(), f.controlPlaneManifest("1.36"),
		f.machinePoolManifest(flowDefaultPool, replicas), f.poolManifest(flowDefaultPool, "default", infrav1.PoolCreationWithCluster, ""))
	if opts.sparePool {
		f.declare(f.machinePoolManifest(flowSparePool, 2), f.poolManifest(flowSparePool, "spare", "", ""))
	}
	return f
}

// flowClient returns a client of the envtest API server whose requests carry agent as their user agent. When writes is
// not nil, every write the client makes is recorded there, with the managedFields of the API server's answer.
//
// Parameters:
//   - t is the test.
//   - agent is the user agent, and so the field manager of every client-side write.
//   - writes receives the writes; nil records none.
func flowClient(t *testing.T, agent string, writes *flowWrites) client.Client {
	t.Helper()
	config := rest.CopyConfig(testEnv.Config)
	config.UserAgent = agent
	c, err := client.NewWithWatch(config, client.Options{Scheme: scheme.Scheme})
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	if writes == nil {
		return c
	}
	return interceptor.NewClient(c, writes.funcs())
}

// The user's manifests: the objects of the flavor managed-iks with one zone per pool.

// clusterManifest is the CAPI Cluster as the user declares it.
func (f *managedFlow) clusterManifest() *clusterv1.Cluster {
	return &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.namespace, Name: flowClusterName},
		Spec: clusterv1.ClusterSpec{
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedClusterKind, Name: flowClusterName,
			},
			ControlPlaneRef: managedControlPlaneRef(flowCPName),
		},
	}
}

// infraManifest is the IBMCloudManagedCluster as the user declares it: the VPC and two subnets given by ID.
func (f *managedFlow) infraManifest() *infrav1.IBMCloudManagedCluster {
	return &infrav1.IBMCloudManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.namespace, Name: flowClusterName},
		Spec:       testInfraSpec(),
	}
}

// controlPlaneManifest is the IKS control plane as the user declares it. version is spec.distributionVersion.
func (f *managedFlow) controlPlaneManifest(version string) *controlplanev1.IBMCloudManagedControlPlane {
	return &controlplanev1.IBMCloudManagedControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.namespace, Name: flowCPName},
		Spec: controlplanev1.IBMCloudManagedControlPlaneSpec{
			Distribution:        controlplanev1.DistributionKubernetes,
			DistributionVersion: version,
		},
	}
}

// machinePoolManifest is a MachinePool of the Cluster whose infrastructure is the IBMCloudManagedMachinePool of the
// same name.
//
// Parameters:
//   - name names both objects.
//   - replicas is spec.replicas.
func (f *managedFlow) machinePoolManifest(name string, replicas int32) *clusterv1.MachinePool {
	return &clusterv1.MachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.namespace, Name: name},
		Spec: clusterv1.MachinePoolSpec{
			ClusterName: flowClusterName,
			Replicas:    ptr.To(replicas),
			Template: clusterv1.MachineTemplateSpec{Spec: clusterv1.MachineSpec{
				ClusterName: flowClusterName,
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{
					APIGroup: infrav1.GroupVersion.Group, Kind: ibmCloudManagedMachinePoolKind, Name: name,
				},
			}},
		},
	}
}

// poolManifest is an IBMCloudManagedMachinePool in flowZone as the user declares it.
//
// Parameters:
//   - name is the object's name.
//   - poolName is the IBM pool name.
//   - creation is spec.creation ("" omits it).
//   - policy is spec.workerUpdates.policy ("" omits it: Manual).
func (f *managedFlow) poolManifest(name, poolName string, creation infrav1.PoolCreation, policy infrav1.WorkerUpdatePolicy) *infrav1.IBMCloudManagedMachinePool {
	return &infrav1.IBMCloudManagedMachinePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.namespace, Name: name},
		Spec: infrav1.IBMCloudManagedMachinePoolSpec{
			PoolName:      poolName,
			Creation:      creation,
			Flavor:        mpTestFlavor,
			Zones:         []infrav1.WorkerPoolZone{{Name: flowZone}},
			WorkerUpdates: infrav1.WorkerUpdatesSpec{Policy: policy},
		},
	}
}

// declare records objs as the user's manifests, without applying them; a later declaration of the same object
// replaces the earlier one. objs are the manifests.
func (f *managedFlow) declare(objs ...client.Object) {
	for _, obj := range objs {
		key := f.manifestKey(obj)
		if _, known := f.manifests[key]; !known {
			f.manifestOrder = append(f.manifestOrder, key)
		}
		f.manifests[key] = obj
	}
}

// manifestKey names a manifest by kind and name. obj is the manifest.
func (f *managedFlow) manifestKey(obj client.Object) string {
	gvk, err := apiutil.GVKForObject(obj, scheme.Scheme)
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return gvk.Kind + "/" + obj.GetName()
}

// apply declares objs and applies them as the user (server-side apply under the user's field manager, never forcing
// ownership: a field another manager owns with a different value fails the apply with a conflict). objs are the
// manifests.
func (f *managedFlow) apply(objs ...client.Object) {
	f.t.Helper()
	f.declare(objs...)
	for _, obj := range objs {
		f.applyOne(obj)
	}
}

// reapply applies every declared manifest again, unchanged, as a GitOps tool does on every sync: it must not
// conflict with anything CAPIBM wrote.
func (f *managedFlow) reapply() {
	f.t.Helper()
	for _, key := range f.manifestOrder {
		f.applyOne(f.manifests[key])
	}
}

// applyOne applies one manifest as the user. The manifest is turned into its JSON form, which encoding/json writes
// with the API's omitempty and omitzero rules, without status and without the server-set metadata. obj is the
// manifest.
func (f *managedFlow) applyOne(obj client.Object) {
	f.t.Helper()
	g := NewWithT(f.t)
	gvk, err := apiutil.GVKForObject(obj, scheme.Scheme)
	g.Expect(err).NotTo(HaveOccurred())
	data, err := json.Marshal(obj)
	g.Expect(err).NotTo(HaveOccurred())
	u := &unstructured.Unstructured{}
	g.Expect(json.Unmarshal(data, &u.Object)).To(Succeed())
	u.SetGroupVersionKind(gvk)
	unstructured.RemoveNestedField(u.Object, "status")
	unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
	g.Expect(f.user.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(flowUserManager))).
		To(Succeed(), "the user's server-side apply of %s %s", gvk.Kind, obj.GetName())
}

// deleteAsUser deletes the object as the user, the way a GitOps tool or kubectl deletes a Cluster. obj names it.
func (f *managedFlow) deleteAsUser(obj client.Object) {
	f.t.Helper()
	NewWithT(f.t).Expect(f.user.Delete(ctx, obj)).To(Succeed())
}

// drive runs rounds until done holds, at most maxRounds of them, and fails the test with the state of the flow when it
// does not.
//
// Parameters:
//   - what describes what done waits for.
//   - maxRounds bounds the rounds.
//   - done reports whether the flow has got there.
func (f *managedFlow) drive(what string, maxRounds int, done func() bool) {
	f.t.Helper()
	for range maxRounds {
		if done() {
			return
		}
		f.round()
	}
	if !done() {
		f.t.Fatalf("%s: not reached after %d rounds of %s\n%s", what, maxRounds, flowRoundStep, f.describe())
	}
}

// round is one step of the whole system: the clock moves by flowRoundStep, IBM moves one step, Cluster API reacts,
// then every controller reconciles every object of the Cluster once and the events are collected.
func (f *managedFlow) round() {
	f.t.Helper()
	if f.alternatePools {
		f.reversePools = !f.reversePools
	}
	f.clock.Step(flowRoundStep)
	f.ibm.progress()
	f.capiStep()
	f.reconcileAll()
}

// reconcileAll reconciles the IBMCloudManagedCluster, the control plane and every pool once, in that order; the pools
// in name order, or reversed when reversePools is set.
func (f *managedFlow) reconcileAll() {
	f.t.Helper()
	f.reconcile(f.infraR, flowClusterName)
	f.reconcile(f.cpR, flowCPName)
	pools := f.poolNames()
	if f.reversePools {
		slices.Reverse(pools)
	}
	for _, name := range pools {
		f.reconcile(f.poolR, name)
	}
}

// flowReconciler is one of the three managed reconcilers.
type flowReconciler interface {
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}

// reconcile runs one Reconcile of the object name of the flow's namespace, expects no error and collects the events.
//
// Parameters:
//   - r is the reconciler.
//   - name is the object's name.
func (f *managedFlow) reconcile(r flowReconciler, name string) {
	f.t.Helper()
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: f.namespace, Name: name}})
	if err != nil {
		// The flows drive no failure: a Transient outcome here is a defect of the flow or of the controller.
		f.t.Fatalf("Reconcile %T %s: %v\n%s", r, name, err, f.describe())
	}
	f.collectEvents()
	if f.afterReconcile != nil {
		f.afterReconcile()
	}
}

// collectEvents moves the recorded events into events, so that the recorder's buffer never fills up.
func (f *managedFlow) collectEvents() {
	for {
		select {
		case event := <-f.recorder.Events:
			f.events = append(f.events, event)
		default:
			return
		}
	}
}

// poolNames returns the names of the flow's IBMCloudManagedMachinePools, sorted.
func (f *managedFlow) poolNames() []string {
	f.t.Helper()
	pools := &infrav1.IBMCloudManagedMachinePoolList{}
	NewWithT(f.t).Expect(f.reader.List(ctx, pools, client.InNamespace(f.namespace))).To(Succeed())
	names := make([]string, 0, len(pools.Items))
	for i := range pools.Items {
		names = append(names, pools.Items[i].Name)
	}
	sort.Strings(names)
	return names
}

// The simulated Cluster API.

// capiStep does what Cluster API's core controllers would have done by now: it links the objects of the Cluster to
// their owners, marks the infrastructure provisioned, and deletes a deleting Cluster's objects in Cluster API's order.
func (f *managedFlow) capiStep() {
	f.t.Helper()
	cluster := &clusterv1.Cluster{}
	if !f.get(flowClusterName, cluster) {
		return
	}
	if !cluster.DeletionTimestamp.IsZero() {
		f.capiDelete(cluster)
		return
	}
	f.capiPatch(cluster, func() { addFinalizer(cluster, flowClusterFinalizer) })
	clusterRef := ownerRef(cluster, "Cluster")
	infra := &infrav1.IBMCloudManagedCluster{}
	if f.get(flowClusterName, infra) {
		f.capiPatch(infra, func() { linkTo(infra, clusterRef) })
		if ptr.Deref(infra.Status.Initialization.Provisioned, false) && !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) {
			before := cluster.DeepCopy()
			cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
			NewWithT(f.t).Expect(f.capi.Status().Patch(ctx, cluster, client.MergeFrom(before))).To(Succeed())
		}
	}
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	if f.get(flowCPName, cp) {
		f.capiPatch(cp, func() { linkTo(cp, clusterRef) })
	}
	for _, mp := range f.machinePools() {
		f.capiPatch(mp, func() {
			linkTo(mp, clusterRef)
			addFinalizer(mp, flowMachinePoolFinalizer)
		})
		pool := &infrav1.IBMCloudManagedMachinePool{}
		if f.get(mp.Spec.Template.Spec.InfrastructureRef.Name, pool) {
			f.capiPatch(pool, func() { linkTo(pool, ownerRef(mp, "MachinePool")) })
		}
	}
}

// capiDelete is one step of Cluster API's deletion of a Cluster: the MachinePools first (each one deletes its
// infrastructure pool and goes once that is gone), then the control plane, then the InfraCluster, and the Cluster's
// finalizer last. cluster is the deleting Cluster.
func (f *managedFlow) capiDelete(cluster *clusterv1.Cluster) {
	f.t.Helper()
	machinePools := f.machinePools()
	for _, mp := range machinePools {
		if mp.DeletionTimestamp.IsZero() {
			f.capiDeleteObject(mp)
			continue
		}
		pool := &infrav1.IBMCloudManagedMachinePool{}
		if !f.get(mp.Spec.Template.Spec.InfrastructureRef.Name, pool) {
			f.capiPatch(mp, func() { mp.Finalizers = slices.DeleteFunc(mp.Finalizers, isString(flowMachinePoolFinalizer)) })
			continue
		}
		if pool.DeletionTimestamp.IsZero() {
			f.capiDeleteObject(pool)
		}
	}
	if len(machinePools) > 0 {
		return
	}
	if f.deleteInTurn(flowCPName, &controlplanev1.IBMCloudManagedControlPlane{}) ||
		f.deleteInTurn(flowClusterName, &infrav1.IBMCloudManagedCluster{}) {
		return
	}
	f.capiPatch(cluster, func() { cluster.Finalizers = slices.DeleteFunc(cluster.Finalizers, isString(flowClusterFinalizer)) })
}

// deleteInTurn deletes the object name as Cluster API when it exists and is not deleting yet, and reports whether it
// still exists: Cluster API waits for it before it deletes the next object.
//
// Parameters:
//   - name is the object's name.
//   - obj receives it.
func (f *managedFlow) deleteInTurn(name string, obj client.Object) bool {
	f.t.Helper()
	if !f.get(name, obj) {
		return false
	}
	if obj.GetDeletionTimestamp().IsZero() {
		f.capiDeleteObject(obj)
	}
	return true
}

// isString returns a predicate that matches value. value is the string to match.
func isString(value string) func(string) bool {
	return func(s string) bool { return s == value }
}

// capiDeleteObject deletes obj as Cluster API; an object that is already gone is fine.
func (f *managedFlow) capiDeleteObject(obj client.Object) {
	f.t.Helper()
	NewWithT(f.t).Expect(client.IgnoreNotFound(f.capi.Delete(ctx, obj))).To(Succeed())
}

// capiPatch applies change to obj and patches the difference as Cluster API, when there is one.
//
// Parameters:
//   - obj is the object as read.
//   - change changes obj in place.
func (f *managedFlow) capiPatch(obj client.Object, change func()) {
	f.t.Helper()
	before := obj.DeepCopyObject().(client.Object)
	change()
	patch := client.MergeFrom(before)
	data, err := patch.Data(obj)
	g := NewWithT(f.t)
	g.Expect(err).NotTo(HaveOccurred())
	if string(data) == "{}" {
		return
	}
	g.Expect(client.IgnoreNotFound(f.capi.Patch(ctx, obj, patch))).To(Succeed())
}

// ownerRef is the owner reference Cluster API sets to owner, a Cluster or a MachinePool.
//
// Parameters:
//   - owner is the owner.
//   - kind is its kind.
func ownerRef(owner client.Object, kind string) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: clusterv1.GroupVersion.String(), Kind: kind, Name: owner.GetName(), UID: owner.GetUID()}
}

// linkTo sets the cluster-name label and the owner reference ref on obj, as Cluster API does for the objects a Cluster
// or a MachinePool references.
//
// Parameters:
//   - obj is the referenced object.
//   - ref is the owner reference.
func linkTo(obj client.Object, ref metav1.OwnerReference) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[clusterv1.ClusterNameLabel] = flowClusterName
	obj.SetLabels(labels)
	for _, existing := range obj.GetOwnerReferences() {
		if existing.UID == ref.UID {
			return
		}
	}
	obj.SetOwnerReferences(append(obj.GetOwnerReferences(), ref))
}

// addFinalizer adds finalizer to obj unless it is there. obj is the object; finalizer the finalizer.
func addFinalizer(obj client.Object, finalizer string) {
	if !slices.Contains(obj.GetFinalizers(), finalizer) {
		obj.SetFinalizers(append(obj.GetFinalizers(), finalizer))
	}
}

// machinePools returns the MachinePools of the flow's namespace.
func (f *managedFlow) machinePools() []*clusterv1.MachinePool {
	f.t.Helper()
	list := &clusterv1.MachinePoolList{}
	NewWithT(f.t).Expect(f.reader.List(ctx, list, client.InNamespace(f.namespace))).To(Succeed())
	pools := make([]*clusterv1.MachinePool, 0, len(list.Items))
	for i := range list.Items {
		pools = append(pools, &list.Items[i])
	}
	return pools
}

// get reads the object name of the flow's namespace, uncached, into obj and reports whether it exists.
//
// Parameters:
//   - name is the object's name.
//   - obj receives it.
func (f *managedFlow) get(name string, obj client.Object) bool {
	f.t.Helper()
	err := f.reader.Get(ctx, client.ObjectKey{Namespace: f.namespace, Name: name}, obj)
	if apierrors.IsNotFound(err) {
		return false
	}
	NewWithT(f.t).Expect(err).NotTo(HaveOccurred())
	return true
}

// Reading the flow's state.

// controlPlane returns the control plane, or nil when it is gone.
func (f *managedFlow) controlPlane() *controlplanev1.IBMCloudManagedControlPlane {
	cp := &controlplanev1.IBMCloudManagedControlPlane{}
	if !f.get(flowCPName, cp) {
		return nil
	}
	return cp
}

// pool returns the IBMCloudManagedMachinePool name, or nil when it is gone. name is the object's name.
func (f *managedFlow) pool(name string) *infrav1.IBMCloudManagedMachinePool {
	pool := &infrav1.IBMCloudManagedMachinePool{}
	if !f.get(name, pool) {
		return nil
	}
	return pool
}

// clusterReady reports whether the control plane is initialized and available and every pool is ready with as many
// published workers as its MachinePool asks for.
func (f *managedFlow) clusterReady() bool {
	cp := f.controlPlane()
	if cp == nil || !ptr.Deref(cp.Status.Initialization.ControlPlaneInitialized, false) ||
		!conditions.IsTrue(cp, controlplanev1.IBMCloudManagedControlPlaneAvailableCondition) {
		return false
	}
	for _, name := range f.poolNames() {
		if !f.poolReady(name) {
			return false
		}
	}
	return true
}

// poolReady reports whether the pool name is Ready, publishes as many workers as its MachinePool asks for, and has no
// removal in flight. name is the pool's object name.
func (f *managedFlow) poolReady(name string) bool {
	pool := f.pool(name)
	mp := &clusterv1.MachinePool{}
	if pool == nil || !f.get(name, mp) {
		return false
	}
	return conditions.IsTrue(pool, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyCondition) &&
		len(pool.Spec.ProviderIDList) == int(ptr.Deref(mp.Spec.Replicas, 0)) &&
		pool.Status.Disruption.Operation == ""
}

// poolsUpToDate reports whether every pool is ready and its WorkersUpToDate condition is True.
func (f *managedFlow) poolsUpToDate() bool {
	for _, name := range f.poolNames() {
		pool := f.pool(name)
		if !f.poolReady(name) || !conditions.IsTrue(pool, infrav1.IBMCloudManagedMachinePoolWorkersUpToDateCondition) {
			return false
		}
	}
	return true
}

// allGone reports whether every object of the Cluster is gone from the API server.
func (f *managedFlow) allGone() bool {
	for _, obj := range []client.Object{&clusterv1.Cluster{}, &infrav1.IBMCloudManagedCluster{}} {
		if f.get(flowClusterName, obj) {
			return false
		}
	}
	return f.controlPlane() == nil && len(f.poolNames()) == 0 && len(f.machinePools()) == 0
}

// describe summarizes the flow's state for a failure message: the conditions of the control plane and the pools, the
// IBM state and the last events.
func (f *managedFlow) describe() string {
	var lines []string
	if cp := f.controlPlane(); cp != nil {
		lines = append(lines, fmt.Sprintf("control plane: clusterID %q version %q conditions %s", cp.Status.ClusterID,
			cp.Status.DistributionVersion, conditionSummary(cp.Status.Conditions)))
	}
	for _, name := range f.poolNames() {
		if pool := f.pool(name); pool != nil {
			lines = append(lines, fmt.Sprintf("pool %s: providerIDs %d disruption %+v conditions %s", name, len(pool.Spec.ProviderIDList),
				pool.Status.Disruption, conditionSummary(pool.Status.Conditions)))
		}
	}
	lines = append(lines, "IBM: "+f.ibm.describe())
	events := f.events[max(0, len(f.events)-5):]
	lines = append(lines, "last events: "+strings.Join(events, " | "))
	return strings.Join(lines, "\n")
}

// conditionSummary renders conditions as "Type=Status/Reason: message", one per line.
func conditionSummary(list []metav1.Condition) string {
	parts := make([]string, 0, len(list))
	for _, c := range list {
		parts = append(parts, fmt.Sprintf("\n  %s=%s/%s: %s", c.Type, c.Status, c.Reason, c.Message))
	}
	return strings.Join(parts, "")
}

// The controllers' writes and their managedFields.

// flowWrite is one write request of the controllers' client.
type flowWrite struct {
	// verb is create, update, patch or delete, with "/<subresource>" for a subresource write.
	verb string
	// kind and name name the object.
	kind, name string
	// managedFields are the object's managedFields in the API server's answer; none for a delete or a failed write.
	managedFields []metav1.ManagedFieldsEntry
	// body is the body of a patch request; empty for the other requests.
	body string
	// err is the write's error, nil when the API server accepted it.
	err error
}

// String renders the write for a failure message.
func (w flowWrite) String() string {
	return fmt.Sprintf("%s %s/%s", w.verb, w.kind, w.name)
}

// GomegaString renders the write in a failed expectation: the request and a patch's body, without the managedFields.
func (w flowWrite) GomegaString() string {
	return strings.TrimSpace(w.String() + " " + w.body)
}

// flowWrites records the writes of one client. It is safe for concurrent use.
type flowWrites struct {
	mu     sync.Mutex
	writes []flowWrite
}

// funcs returns the interceptor functions that pass every write through and record it.
func (w *flowWrites) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return w.record("create", obj, "", c.Create(ctx, obj, opts...))
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			return w.record("update", obj, "", c.Update(ctx, obj, opts...))
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			body := patchBody(patch, obj)
			return w.record("patch", obj, body, c.Patch(ctx, obj, patch, opts...))
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return w.record("delete", obj, "", c.Delete(ctx, obj, opts...))
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			return w.record("update/"+sub, obj, "", c.SubResource(sub).Update(ctx, obj, opts...))
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			body := patchBody(patch, obj)
			return w.record("patch/"+sub, obj, body, c.SubResource(sub).Patch(ctx, obj, patch, opts...))
		},
	}
}

// patchBody returns the body patch sends for obj, or the error that kept it from being computed.
//
// Parameters:
//   - patch is the patch.
//   - obj is the object it applies to, before the request.
func patchBody(patch client.Patch, obj client.Object) string {
	data, err := patch.Data(obj)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(data)
}

// record keeps one write and returns its error unchanged.
//
// Parameters:
//   - verb is the request.
//   - obj is the object, updated with the API server's answer when the write succeeded.
//   - body is a patch's body; "" for the other requests.
//   - err is the write's error.
func (w *flowWrites) record(verb string, obj client.Object, body string, err error) error {
	write := flowWrite{verb: verb, kind: kindOf(obj), name: obj.GetName(), body: body, err: err}
	if err == nil && verb != "delete" {
		write.managedFields = append([]metav1.ManagedFieldsEntry(nil), obj.GetManagedFields()...)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, write)
	return err
}

// count returns the number of writes so far.
func (w *flowWrites) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

// since returns the writes after the first n. n is a count returned by count.
func (w *flowWrites) since(n int) []flowWrite {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]flowWrite(nil), w.writes[n:]...)
}

// kindOf returns the kind of a typed object of the flow's scheme.
func kindOf(obj client.Object) string {
	gvk, err := apiutil.GVKForObject(obj, scheme.Scheme)
	if err != nil {
		return fmt.Sprintf("%T", obj)
	}
	return gvk.Kind
}

// flowControllerSpecFields are the spec fields CAPIBM may write on each kind a GitOps tool declares:
// spec.controlPlaneEndpoint of the control plane (the Cluster API contract field, written once) and
// spec.providerIDList of the pool (the InfraMachinePool contract field). Besides those, CAPIBM may write only status
// and its finalizers. Cluster and MachinePool are declared too (flowDeclaredKinds), but CAPIBM writes no field of
// them at all, so they are not listed.
var flowControllerSpecFields = map[string][]string{
	"IBMCloudManagedControlPlane": {"f:controlPlaneEndpoint"},
	"IBMCloudManagedMachinePool":  {"f:providerIDList"},
	"IBMCloudManagedCluster":      nil,
}

// ownedPaths returns the fields an entry of managedFields owns, two levels deep ("f:spec.f:providerIDList"), sorted;
// a field owned with no child is returned alone ("f:status"), and the "." markers (the field itself) are left out.
// entry is the managedFields entry.
func ownedPaths(entry metav1.ManagedFieldsEntry) ([]string, error) {
	var fields map[string]any
	if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
		return nil, fmt.Errorf("decoding the managedFields of %s: %w", entry.Manager, err)
	}
	var paths []string
	for top, child := range fields {
		children, _ := child.(map[string]any)
		found := false
		for key := range children {
			if key != "." {
				paths = append(paths, top+"."+key)
				found = true
			}
		}
		if !found {
			paths = append(paths, top)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// controllerFields returns the fields the controller manager owns on an object of kind, each as
// "<kind> <subresource or main>: <path>", and the subset of them that the GitOps contract forbids.
//
// Parameters:
//   - kind is the object's kind.
//   - entries are its managedFields.
func controllerFields(kind string, entries []metav1.ManagedFieldsEntry) (owned, forbidden []string, err error) {
	allowedSpec, declared := flowControllerSpecFields[kind]
	for _, entry := range entries {
		if entry.Manager != flowControllerManager {
			continue
		}
		paths, err := ownedPaths(entry)
		if err != nil {
			return nil, nil, err
		}
		where := "main"
		if entry.Subresource != "" {
			where = entry.Subresource
		}
		for _, path := range paths {
			field := fmt.Sprintf("%s %s: %s", kind, where, path)
			owned = append(owned, field)
			if !declared || !controllerMayOwn(entry.Subresource, path, allowedSpec) {
				forbidden = append(forbidden, field)
			}
		}
	}
	return owned, forbidden, nil
}

// controllerMayOwn reports whether CAPIBM may own path of a declared object: anything of the status subresource, its
// finalizers, and the kind's contract spec fields.
//
// Parameters:
//   - subresource is the managedFields entry's subresource ("" for the object itself).
//   - path is the owned path (ownedPaths).
//   - allowedSpec are the kind's spec fields CAPIBM may write.
func controllerMayOwn(subresource, path string, allowedSpec []string) bool {
	if subresource == "status" {
		return path == "f:status" || strings.HasPrefix(path, "f:status.")
	}
	if subresource != "" {
		return false
	}
	if path == "f:metadata.f:finalizers" {
		return true
	}
	field, isSpec := strings.CutPrefix(path, "f:spec.")
	return isSpec && slices.Contains(allowedSpec, field)
}

// flowDeclaredKinds are the kinds the user declares in the flows.
var flowDeclaredKinds = []string{"Cluster", "IBMCloudManagedCluster", "IBMCloudManagedControlPlane", "MachinePool", "IBMCloudManagedMachinePool"}

// gitOpsFindings checks the fields CAPIBM owns on the user's objects against flowControllerSpecFields, from the
// managedFields of every write since the write fromWrite (the API server's answer to each one, so a field that a later
// write gave up is still seen) and of every declared object that exists now. It returns the owned and the forbidden
// fields (controllerFields), and every write to a Cluster or a MachinePool as forbidden. fromWrite is a count returned
// by flowWrites.count.
func (f *managedFlow) gitOpsFindings(fromWrite int) (owned, forbidden []string) {
	f.t.Helper()
	g := NewWithT(f.t)
	add := func(kind string, entries []metav1.ManagedFieldsEntry) {
		o, fb, err := controllerFields(kind, entries)
		g.Expect(err).NotTo(HaveOccurred())
		owned, forbidden = append(owned, o...), append(forbidden, fb...)
	}
	for _, write := range f.writes.since(fromWrite) {
		if !slices.Contains(flowDeclaredKinds, write.kind) {
			continue
		}
		if _, writable := flowControllerSpecFields[write.kind]; !writable {
			forbidden = append(forbidden, "the controller wrote: "+write.String())
		}
		add(write.kind, write.managedFields)
	}
	declared := []client.ObjectList{&clusterv1.ClusterList{}, &infrav1.IBMCloudManagedClusterList{},
		&controlplanev1.IBMCloudManagedControlPlaneList{}, &clusterv1.MachinePoolList{}, &infrav1.IBMCloudManagedMachinePoolList{}}
	for _, list := range declared {
		g.Expect(f.reader.List(ctx, list, client.InNamespace(f.namespace))).To(Succeed())
		items, err := meta.ExtractList(list)
		g.Expect(err).NotTo(HaveOccurred())
		for _, item := range items {
			obj := item.(client.Object)
			add(kindOf(obj), obj.GetManagedFields())
		}
	}
	return owned, forbidden
}

// managers returns the field managers and operations of obj's managedFields ("<manager> <operation>[ /<subresource>]"),
// sorted and without duplicates. obj is the object as read.
func managers(obj client.Object) []string {
	found := make([]string, 0, len(obj.GetManagedFields()))
	for _, entry := range obj.GetManagedFields() {
		item := entry.Manager + " " + string(entry.Operation)
		if entry.Subresource != "" {
			item += " /" + entry.Subresource
		}
		found = append(found, item)
	}
	slices.Sort(found)
	return slices.Compact(found)
}

// resourceVersions returns the resourceVersion of every object of the Cluster in the management cluster, by
// "<kind>/<name>": a changed version is a write, whoever made it.
func (f *managedFlow) resourceVersions() map[string]string {
	f.t.Helper()
	g := NewWithT(f.t)
	versions := map[string]string{}
	lists := []client.ObjectList{&clusterv1.ClusterList{}, &infrav1.IBMCloudManagedClusterList{}, &controlplanev1.IBMCloudManagedControlPlaneList{},
		&clusterv1.MachinePoolList{}, &infrav1.IBMCloudManagedMachinePoolList{}, &corev1.SecretList{}, &coordinationv1.LeaseList{}}
	for _, list := range lists {
		g.Expect(f.reader.List(ctx, list, client.InNamespace(f.namespace))).To(Succeed())
		items, err := meta.ExtractList(list)
		g.Expect(err).NotTo(HaveOccurred())
		for _, item := range items {
			obj := item.(client.Object)
			versions[kindOf(obj)+"/"+obj.GetName()] = obj.GetResourceVersion()
		}
	}
	return versions
}

// The simulated workload cluster.

// flowWorkload is the workload cluster of a flow: a fake client with the indexes ClusterCache provides (pods by Node,
// Nodes by provider ID), seen by CAPIBM through interceptors that record every call and let a test refuse evictions as
// a PodDisruptionBudget would. It is safe for concurrent use.
type flowWorkload struct {
	// raw is the fake client without interceptors, for the simulator and the test's own reads.
	raw client.WithWatch
	// client is what CAPIBM gets from the ClusterCache.
	client client.Client
	clock  clock.PassiveClock
	pods   bool

	mu sync.Mutex
	// calls are CAPIBM's calls, in order ("<verb> <Kind>[/<name>]").
	calls []string
	// refuseAll, when set, refuses every eviction with this error.
	refuseAll error
}

// newFlowWorkload returns an empty workload cluster.
//
// Parameters:
//   - t is the test.
//   - clk stamps the Nodes' creation and readiness.
//   - pods puts one pod on every Node the simulator adds.
func newFlowWorkload(t *testing.T, clk clock.PassiveClock, pods bool) *flowWorkload {
	t.Helper()
	w := &flowWorkload{clock: clk, pods: pods}
	w.raw = fake.NewClientBuilder().WithScheme(machinePoolScheme(t)).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }).
		WithIndex(&corev1.Node{}, index.NodeProviderIDField, index.NodeByProviderID).Build()
	w.client = interceptor.NewClient(w.raw, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			w.addCall(fmt.Sprintf("get %s/%s", kindOf(obj), key.Name))
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			w.addCall(fmt.Sprintf("list %T", list))
			return c.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			w.add("create", obj)
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			w.add("update", obj)
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			w.add("patch", obj)
			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			w.add("delete", obj)
			return c.Delete(ctx, obj, opts...)
		},
		SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
			w.add(sub, obj)
			w.mu.Lock()
			refusal := w.refuseAll
			w.mu.Unlock()
			if sub == "eviction" && refusal != nil {
				return refusal
			}
			return c.SubResource(sub).Create(ctx, obj, subObj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			w.add("patch/"+sub, obj)
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			w.add("update/"+sub, obj)
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	return w
}

// add records one call on obj. verb is the request.
func (w *flowWorkload) add(verb string, obj client.Object) {
	w.addCall(fmt.Sprintf("%s %s/%s", verb, kindOf(obj), obj.GetName()))
}

// addCall records one call. call describes it.
func (w *flowWorkload) addCall(call string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, call)
}

// callCount returns the number of CAPIBM's calls so far.
func (w *flowWorkload) callCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.calls)
}

// callsSince returns CAPIBM's calls after the first n. n is a count returned by callCount.
func (w *flowWorkload) callsSince(n int) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.calls[n:]...)
}

// refuseEvictions makes a PodDisruptionBudget refuse every eviction (err), or allow them again (nil).
func (w *flowWorkload) refuseEvictions(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.refuseAll = err
}

// flowNodeName is the Node name of a worker.
func flowNodeName(workerID string) string {
	return "node-" + workerID
}

// flowProviderID is the provider ID of a worker of the flow's IBM cluster.
func flowProviderID(workerID string) string {
	return "ibm://" + mpTestAccount + "///" + flowClusterID + "/" + workerID
}

// addNode creates the Ready Node of a worker that IBM has just deployed, with its pod when the flow has pods.
// workerID is the worker.
func (w *flowWorkload) addNode(workerID string) error {
	now := metav1.NewTime(w.clock.Now())
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: flowNodeName(workerID), CreationTimestamp: now},
		Spec:       corev1.NodeSpec{ProviderID: flowProviderID(workerID)},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastTransitionTime: now}}},
	}
	if err := w.raw.Create(context.Background(), node); err != nil {
		return err
	}
	if !w.pods {
		return nil
	}
	return w.raw.Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: flowPodNamespace, Name: "web-" + workerID},
		Spec:       corev1.PodSpec{NodeName: flowNodeName(workerID)},
	})
}

// removeNode deletes the Node of a worker that IBM has deleted, and the pods left on it. workerID is the worker.
func (w *flowWorkload) removeNode(workerID string) error {
	pods := &corev1.PodList{}
	if err := w.raw.List(context.Background(), pods, client.MatchingFields{"spec.nodeName": flowNodeName(workerID)}); err != nil {
		return err
	}
	for i := range pods.Items {
		if err := client.IgnoreNotFound(w.raw.Delete(context.Background(), &pods.Items[i])); err != nil {
			return err
		}
	}
	return client.IgnoreNotFound(w.raw.Delete(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: flowNodeName(workerID)}}))
}

// cordoned returns the cordoned Nodes, by name, with the drain owner CAPIBM marked on each ("" when none).
func (w *flowWorkload) cordoned(t *testing.T) map[string]string {
	t.Helper()
	nodes := &corev1.NodeList{}
	NewWithT(t).Expect(w.raw.List(context.Background(), nodes)).To(Succeed())
	cordoned := map[string]string{}
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			cordoned[node.Name] = node.Annotations[infrav1.NodeAnnotationPrefix+drain.MarkerDrainOwner]
		}
	}
	return cordoned
}

// The simulated IBM Cloud.

// flowIBM is a stateful IBM Cloud Kubernetes Service for one cluster, behind a gomock mock: every method of the mock
// answers from the state and records the call, and the mutations change the state the way IBM was observed to change
// it: CreateCluster starts a deploying cluster with its initial pool, ResizeWorkerPool and CreateWorkerPool provision
// workers, UpdateMaster sets the master's target version, ReplaceWorker (always with update=true) deletes the worker
// and, once it is deleted, provisions its successor on the newest fix pack of the master minor, DeleteCluster deletes
// the cluster. progress moves each of those one step. It is safe for concurrent use.
type flowIBM struct {
	t        *testing.T
	clock    clock.PassiveClock
	workload *flowWorkload
	creds    kubeconfigCredentials

	mu      sync.Mutex
	cluster *kubernetesservice.Cluster
	// readySince is when the master became ready; masterStatus counts the minutes since, as IBM does.
	readySince time.Time
	// masterStatus, when set, is the masterStatus IBM reports instead.
	masterStatus string
	// statusPerCall makes every GetCluster answer report one minute more in masterStatus, whatever the clock says.
	statusPerCall bool
	// statusCalls counts the GetCluster answers of a ready master.
	statusCalls int
	// lastMasterStatus is the masterStatus of the last GetCluster answer.
	lastMasterStatus string
	pools            []kubernetesservice.WorkerPool
	workers          []kubernetesservice.Worker
	// replaced are the workers ReplaceWorker deletes; each gets a successor once it is deleted.
	replaced map[string]bool
	// workerFixPacks are the fix packs IBM installs on a new worker, per minor.
	workerFixPacks map[string]string
	nextWorker     int
	calls          []string
}

// newFlowIBM returns the simulator and makes it answer every method of mock, any number of times.
//
// Parameters:
//   - t is the test.
//   - mock is the gomock Kubernetes Service client the controllers' ClientBuilder hands out.
//   - clk is the flow's clock.
//   - workload is the workload cluster whose Nodes follow the workers.
func newFlowIBM(t *testing.T, mock *ksmock.MockKubernetesService, clk clock.PassiveClock, workload *flowWorkload) *flowIBM {
	t.Helper()
	ibm := &flowIBM{t: t, clock: clk, workload: workload, creds: newKubeconfigCredentials(t, time.Hour),
		replaced: map[string]bool{}, workerFixPacks: map[string]string{}}
	for minor, version := range flowWorkerFixPacks {
		ibm.workerFixPacks[minor] = version
	}
	mock.EXPECT().GetCluster(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(ibm.getCluster).AnyTimes()
	mock.EXPECT().CreateCluster(gomock.Any(), gomock.Any()).DoAndReturn(ibm.createCluster).AnyTimes()
	mock.EXPECT().DeleteCluster(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(ibm.deleteCluster).AnyTimes()
	mock.EXPECT().UpdateMaster(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(ibm.updateMaster).AnyTimes()
	mock.EXPECT().ListVersions(gomock.Any()).DoAndReturn(ibm.listVersions).AnyTimes()
	mock.EXPECT().GetAdminKubeconfig(gomock.Any(), gomock.Any()).DoAndReturn(ibm.getAdminKubeconfig).AnyTimes()
	mock.EXPECT().GetCACertificate(gomock.Any(), gomock.Any()).DoAndReturn(ibm.getCACertificate).AnyTimes()
	mock.EXPECT().ListWorkerPools(gomock.Any(), gomock.Any()).DoAndReturn(ibm.listWorkerPools).AnyTimes()
	mock.EXPECT().CreateWorkerPool(gomock.Any(), gomock.Any()).DoAndReturn(ibm.createWorkerPool).AnyTimes()
	mock.EXPECT().ResizeWorkerPool(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(ibm.resizeWorkerPool).AnyTimes()
	mock.EXPECT().RemoveWorkerPool(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(ibm.removeWorkerPool).AnyTimes()
	mock.EXPECT().ListWorkers(gomock.Any(), gomock.Any()).DoAndReturn(ibm.listWorkers).AnyTimes()
	mock.EXPECT().ReplaceWorker(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(ibm.replaceWorker).AnyTimes()
	return ibm
}

// record keeps one call; the caller holds mu. call names the operation and its arguments.
func (ibm *flowIBM) record(call string) {
	ibm.calls = append(ibm.calls, call)
}

// callCount returns the number of calls so far.
func (ibm *flowIBM) callCount() int {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	return len(ibm.calls)
}

// callsSince returns the calls after the first n. n is a count returned by callCount.
func (ibm *flowIBM) callsSince(n int) []string {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	return append([]string(nil), ibm.calls[n:]...)
}

// callsOf returns the calls of one operation, with their arguments. op is the operation's method name.
func (ibm *flowIBM) callsOf(op string) []string {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	var calls []string
	for _, call := range ibm.calls {
		if call == op || strings.HasPrefix(call, op+" ") {
			calls = append(calls, call)
		}
	}
	return calls
}

// notFound is IBM's 404 answer to op.
func notFound(op kubernetesservice.Operation) error {
	return &kubernetesservice.APIError{Operation: op, StatusCode: http.StatusNotFound, Code: "A0006", IncidentID: "incident-404"}
}

// getCluster answers GetCluster by ID or by name.
func (ibm *flowIBM) getCluster(_ context.Context, idOrName, _ string) (*kubernetesservice.Cluster, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("GetCluster " + idOrName)
	if ibm.cluster == nil || (idOrName != ibm.cluster.ID && idOrName != ibm.cluster.Name) {
		return nil, notFound(kubernetesservice.OpGetCluster)
	}
	answer := *ibm.cluster
	if answer.MasterState == "deployed" {
		ibm.statusCalls++
		minutes := max(1, int(ibm.clock.Now().Sub(ibm.readySince)/time.Minute))
		if ibm.statusPerCall {
			minutes = ibm.statusCalls
		}
		answer.MasterStatus = ibm.masterStatus
		if answer.MasterStatus == "" {
			answer.MasterStatus = fmt.Sprintf("Ready (%d min ago)", minutes)
		}
	}
	ibm.lastMasterStatus = answer.MasterStatus
	return &answer, nil
}

// probe answers a probe of the cluster's admin kubeconfig: the API server reports the master's Kubernetes version
// ("v1.36.5+IKS" for master 1.36.5_1537) while the cluster exists, and does not answer once IBM has deleted it.
func (ibm *flowIBM) probe() (workloadprobe.Result, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	if ibm.cluster == nil {
		return workloadprobe.Result{}, probeFailure(workloadprobe.FailureUnreachable, "SelfSubjectReview")
	}
	version, _, _ := strings.Cut(ibm.cluster.MasterKubeVersion, "_")
	return passing("v" + version + "+IKS"), nil
}

// createCluster creates the cluster and its initial pool: IBM deploys both.
func (ibm *flowIBM) createCluster(_ context.Context, req kubernetesservice.CreateClusterRequest) (*kubernetesservice.CreateClusterResult, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("CreateCluster " + req.Name)
	if ibm.cluster != nil {
		ibm.t.Errorf("CreateCluster %s, but IBM cluster %s exists", req.Name, ibm.cluster.Name)
		return nil, &kubernetesservice.APIError{Operation: kubernetesservice.OpCreateCluster, StatusCode: http.StatusConflict, Code: "E0007"}
	}
	master := flowMasterFixPacks[req.KubeVersion]
	ibm.cluster = &kubernetesservice.Cluster{ID: flowClusterID, Name: req.Name, CRN: flowClusterCRN, Type: "kubernetes",
		Provider: "vpc-gen2", ResourceGroupID: req.ResourceGroupID, CreatedDate: ibm.clock.Now().UTC().Format("2006-01-02T15:04:05-0700"),
		State: "deploying", MasterState: "deploying", MasterStatus: "Deploy in progress.", MasterKubeVersion: master, TargetVersion: master}
	ibm.addPool(req.WorkerPool.Name, req.WorkerPool.Flavor, req.WorkerPool.WorkerCountPerZone, req.WorkerPool.Zones)
	return &kubernetesservice.CreateClusterResult{ClusterID: flowClusterID}, nil
}

// addPool adds a pool with its provisioning workers; the caller holds mu.
//
// Parameters:
//   - name, flavor and perZone are the pool's name, flavor and workers per zone.
//   - zones are its zones.
func (ibm *flowIBM) addPool(name, flavor string, perZone int64, zones []kubernetesservice.ZoneSubnet) string {
	pool := kubernetesservice.WorkerPool{ID: flowClusterID + "-" + name, Name: name, Flavor: flavor, ActualState: "active",
		SizePerZone: perZone}
	for _, zone := range zones {
		pool.Zones = append(pool.Zones, kubernetesservice.WorkerPoolZone{ID: zone.Zone, WorkerCount: perZone, AutobalanceEnabled: true})
		for range perZone {
			ibm.addWorker(pool.ID, zone.Zone)
		}
	}
	ibm.pools = append(ibm.pools, pool)
	return pool.ID
}

// addWorker adds a provisioning worker of pool in zone on the fix pack IBM installs for the master's minor; the caller
// holds mu.
//
// Parameters:
//   - poolID is the IBM pool.
//   - zone is the zone.
func (ibm *flowIBM) addWorker(poolID, zone string) {
	ibm.nextWorker++
	version := ibm.workerFixPacks[ibm.masterMinor()]
	ibm.workers = append(ibm.workers, kubernetesservice.Worker{ID: fmt.Sprintf("flow-w%02d", ibm.nextWorker), PoolID: poolID,
		Zone: zone, ActualState: "provisioning", HealthState: "normal", KubeVersionActual: version, KubeVersionTarget: version})
}

// masterMinor returns the minor of the master version ("1.36"); the caller holds mu.
func (ibm *flowIBM) masterMinor() string {
	parts := strings.SplitN(ibm.cluster.MasterKubeVersion, ".", 3)
	return parts[0] + "." + parts[1]
}

// deleteCluster starts the deletion of the cluster. The flows do not model persistent storage, so the storage choice is
// not recorded; the control plane delete tests check it.
func (ibm *flowIBM) deleteCluster(_ context.Context, clusterID, _ string, _ kubernetesservice.PersistentStorage) error {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("DeleteCluster " + clusterID)
	if ibm.cluster == nil || clusterID != ibm.cluster.ID {
		return notFound(kubernetesservice.OpDeleteCluster)
	}
	ibm.cluster.State = "deleting"
	return nil
}

// updateMaster starts the master update to the newest master version of minor.
func (ibm *flowIBM) updateMaster(_ context.Context, clusterID, minor string) error {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("UpdateMaster " + minor)
	if ibm.cluster == nil || clusterID != ibm.cluster.ID || flowMasterFixPacks[minor] == "" {
		ibm.t.Errorf("UpdateMaster %s %s of an unknown cluster or minor", clusterID, minor)
		return notFound(kubernetesservice.OpUpdateMaster)
	}
	ibm.cluster.TargetVersion = flowMasterFixPacks[minor]
	return nil
}

// listVersions offers the minors of flowMasterFixPacks.
func (ibm *flowIBM) listVersions(context.Context) (*kubernetesservice.Versions, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("ListVersions")
	return &kubernetesservice.Versions{Kubernetes: []kubernetesservice.ReleaseVersion{
		{Major: 1, Minor: 36, Patch: 5, Default: true}, {Major: 1, Minor: 37, Patch: 1}}}, nil
}

// getAdminKubeconfig serves the admin kubeconfig parts of the cluster (GetAdminKubeconfig, the IKS shape with the CA).
func (ibm *flowIBM) getAdminKubeconfig(_ context.Context, clusterID string) (*kubernetesservice.AdminKubeconfig, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("GetAdminKubeconfig " + clusterID)
	return adminAnswer(ibm.creds, true), nil
}

// getCACertificate serves the cluster CA (GetCACertificate).
func (ibm *flowIBM) getCACertificate(_ context.Context, clusterID string) (*kubernetesservice.CACertificate, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("GetCACertificate " + clusterID)
	return &kubernetesservice.CACertificate{PEM: ibm.creds.ca.pem}, nil
}

// listWorkerPools lists the pools.
func (ibm *flowIBM) listWorkerPools(_ context.Context, clusterID string) ([]kubernetesservice.WorkerPool, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("ListWorkerPools " + clusterID)
	if ibm.cluster == nil {
		return nil, notFound(kubernetesservice.OpListWorkerPools)
	}
	pools := make([]kubernetesservice.WorkerPool, 0, len(ibm.pools))
	for _, pool := range ibm.pools {
		pool.Zones = slices.Clone(pool.Zones)
		pools = append(pools, pool)
	}
	return pools, nil
}

// createWorkerPool creates a pool with its provisioning workers.
func (ibm *flowIBM) createWorkerPool(_ context.Context, req kubernetesservice.CreateWorkerPoolRequest) (string, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("CreateWorkerPool " + req.Name)
	return ibm.addPool(req.Name, req.Flavor, req.WorkerCountPerZone, req.Zones), nil
}

// resizeWorkerPool grows a pool; the flows never shrink one.
func (ibm *flowIBM) resizeWorkerPool(_ context.Context, _, poolID string, sizePerZone int64) error {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record(fmt.Sprintf("ResizeWorkerPool %s %d", poolID, sizePerZone))
	for i := range ibm.pools {
		pool := &ibm.pools[i]
		if pool.ID != poolID {
			continue
		}
		if sizePerZone < pool.SizePerZone {
			ibm.t.Errorf("ResizeWorkerPool %s from %d to %d: the flows do not simulate a shrink", poolID, pool.SizePerZone, sizePerZone)
			return nil
		}
		for z := range pool.Zones {
			for range sizePerZone - pool.Zones[z].WorkerCount {
				ibm.addWorker(poolID, pool.Zones[z].ID)
			}
			pool.Zones[z].WorkerCount = sizePerZone
		}
		pool.SizePerZone = sizePerZone
		return nil
	}
	return notFound(kubernetesservice.OpResizeWorkerPool)
}

// removeWorkerPool deletes a pool and its workers.
func (ibm *flowIBM) removeWorkerPool(_ context.Context, _, poolID string) error {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("RemoveWorkerPool " + poolID)
	ibm.pools = slices.DeleteFunc(ibm.pools, func(p kubernetesservice.WorkerPool) bool { return p.ID == poolID })
	for i := range ibm.workers {
		if ibm.workers[i].PoolID == poolID && ibm.workers[i].ActualState != managedscope.WorkerStateDeleted {
			ibm.workers[i].ActualState = managedscope.WorkerStateDeleting
		}
	}
	return nil
}

// listWorkers lists every worker, the deleted ones included (showDeleted=true).
func (ibm *flowIBM) listWorkers(_ context.Context, clusterID string) ([]kubernetesservice.Worker, error) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("ListWorkers " + clusterID)
	if ibm.cluster == nil {
		return nil, notFound(kubernetesservice.OpListWorkers)
	}
	return slices.Clone(ibm.workers), nil
}

// replaceWorker deletes a worker; once it is deleted, progress provisions its successor.
func (ibm *flowIBM) replaceWorker(_ context.Context, _, workerID string, update bool) error {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.record("ReplaceWorker " + workerID)
	if !update {
		ibm.t.Errorf("ReplaceWorker %s with update=false; CAPIBM always sends update=true", workerID)
	}
	for i := range ibm.workers {
		if ibm.workers[i].ID == workerID {
			ibm.workers[i].ActualState = managedscope.WorkerStateDeleting
			ibm.replaced[workerID] = true
			return nil
		}
	}
	return notFound(kubernetesservice.OpReplaceWorker)
}

// progress moves IBM one step: a deploying cluster becomes ready, a requested master update takes effect, a deleting
// cluster is gone; a provisioning worker is deployed (its Node joins the workload cluster), a deleting worker is
// deleted (its Node leaves) and a replaced one gets its successor.
func (ibm *flowIBM) progress() {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	if ibm.cluster == nil {
		return
	}
	switch ibm.cluster.State {
	case "deploying":
		ibm.cluster.State, ibm.cluster.MasterState, ibm.cluster.MasterURL = "normal", "deployed", cpTestServer
		ibm.readySince = ibm.clock.Now()
	case "deleting":
		ibm.cluster, ibm.pools, ibm.workers = nil, nil, nil
		return
	}
	ibm.cluster.MasterKubeVersion = ibm.cluster.TargetVersion
	for i := range len(ibm.workers) {
		w := &ibm.workers[i]
		switch w.ActualState {
		case "provisioning":
			w.ActualState = managedscope.WorkerStateDeployed
			NewWithT(ibm.t).Expect(ibm.workload.addNode(w.ID)).To(Succeed())
		case managedscope.WorkerStateDeleting:
			w.ActualState = managedscope.WorkerStateDeleted
			NewWithT(ibm.t).Expect(ibm.workload.removeNode(w.ID)).To(Succeed())
			if ibm.replaced[w.ID] {
				poolID, zone := w.PoolID, w.Zone
				ibm.addWorker(poolID, zone)
			}
		}
	}
}

// offerFixPack makes IBM offer version, a newer fix pack of minor, for every worker of that minor, and install it on
// new workers.
//
// Parameters:
//   - minor is the minor ("1.37").
//   - version is the fix pack ("1.37.2_1560").
func (ibm *flowIBM) offerFixPack(minor, version string) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.workerFixPacks[minor] = version
	for i := range ibm.workers {
		if strings.HasPrefix(ibm.workers[i].KubeVersionActual, minor+".") && ibm.workers[i].ActualState != managedscope.WorkerStateDeleted {
			ibm.workers[i].KubeVersionTarget = version
		}
	}
}

// setMasterStatus makes IBM report status as masterStatus from now on. status is IBM's text.
func (ibm *flowIBM) setMasterStatus(status string) {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.masterStatus = status
}

// countMinutesPerCall makes every GetCluster answer report one minute more in masterStatus than the one before, so
// that no two answers are equal even while the clock stands still.
func (ibm *flowIBM) countMinutesPerCall() {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	ibm.masterStatus, ibm.statusPerCall = "", true
}

// lastStatus returns the masterStatus of the last GetCluster answer.
func (ibm *flowIBM) lastStatus() string {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	return ibm.lastMasterStatus
}

// allWorkersOn reports whether every worker IBM does not list as deleted is deployed and runs version.
func (ibm *flowIBM) allWorkersOn(version string) bool {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	for _, w := range ibm.workers {
		if w.ActualState != managedscope.WorkerStateDeleted && (w.ActualState != managedscope.WorkerStateDeployed || w.KubeVersionActual != version) {
			return false
		}
	}
	return len(ibm.workers) > 0
}

// describe summarizes the IBM state for a failure message.
func (ibm *flowIBM) describe() string {
	ibm.mu.Lock()
	defer ibm.mu.Unlock()
	if ibm.cluster == nil {
		return "no cluster"
	}
	var workers []string
	for _, w := range ibm.workers {
		workers = append(workers, fmt.Sprintf("%s(%s %s %s->%s)", w.ID, w.PoolID, w.ActualState, w.KubeVersionActual, w.KubeVersionTarget))
	}
	return fmt.Sprintf("cluster %s master %s target %s; workers %s; last calls %s", ibm.cluster.State, ibm.cluster.MasterKubeVersion,
		ibm.cluster.TargetVersion, strings.Join(workers, " "), strings.Join(ibm.calls[max(0, len(ibm.calls)-6):], " | "))
}

// leaseHolder returns the holder of the Cluster's disruption token, or "" when nobody holds it.
func (f *managedFlow) leaseHolder() string {
	lease := &coordinationv1.Lease{}
	if !f.get(managedscope.DisruptionTokenName(flowClusterName), lease) {
		return ""
	}
	return ptr.Deref(lease.Spec.HolderIdentity, "")
}
