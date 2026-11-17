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
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"

	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/drain"
)

// This file is the drain step shared by every flow that removes workers: the pool deletion, the scale-down and the
// worker replace of a rollout. It turns the drain library's per-Node results into the DrainProgress that
// DecideDisruptionStep reads and into the Draining condition message, and it emits the drain events. Which step runs,
// and when, is DecideDisruptionStep's decision; this file only carries it out on the victim Nodes: cordon all of them
// before evicting from any, evict with Cluster API's semantics, stop evicting once the user's time limit passed, and
// wait for volume detach.

// Drain events, emitted on the IBMCloudManagedMachinePool.
const (
	// EventDrainStarted is the Normal event emitted when a removal cordons its first Node.
	EventDrainStarted = "DrainStarted"
	// EventDrainCompleted is the Normal event emitted when the victims are drained (or the drain is excluded or timed
	// out) and the removal moves on to the volume detach wait.
	EventDrainCompleted = "DrainCompleted"
	// EventDrainBlocked is the Warning event emitted when a PodDisruptionBudget refuses an eviction: on entry and then
	// every DrainBlockedEventInterval while it lasts.
	EventDrainBlocked = "DrainBlocked"
	// EventDrainTimeoutExceeded is the Warning event emitted when the user's nodeDrainTimeoutSeconds has passed: it
	// lists the pods still on the Node, which then go with the server (Cluster API semantics).
	EventDrainTimeoutExceeded = "DrainTimeoutExceeded"
)

// drainMessagePodsPerList is how many pods each line of a drain message names before "and N more": at most 3 pods per
// category, so that the Draining message is deterministic and bounded.
const drainMessagePodsPerList = 3

// drainMessageNodes is how many Nodes the Draining message describes, in Node name order, before it counts the rest
// ("and N more Nodes ..."). Every Node adds a block of several lines, and a pool deletion drains every Node of the pool
// (the API allows 500 workers): without a bound across Nodes the message would outgrow the CRD's limit of 32768
// characters on a condition message, the API server would reject every status patch of the pool, and DrainBlocked,
// the signal an operator needs, would never reach status. The first Nodes are enough to see what blocks the drain.
const drainMessageNodes = 3

// maxDrainMessageLength bounds the Draining message in bytes, well below the CRD's limit of 32768 characters on a
// condition message. drainMessageNodes, drainMessagePodsPerList and the drain library's limit on refusal reasons bound
// the message's shape, but a refusal reason is the API server's text and a pod name may have 253 characters, so the
// message is also cut at this length as a last resort (truncateMessage).
const maxDrainMessageLength = 8192

// DrainKnobs are the drain settings of a pool: the Cluster API fields of the owning MachinePool's machine template,
// and the two exclusion annotations, which are honoured on the MachinePool template (steady setting) and on the
// IBMCloudManagedMachinePool (emergency override, writable while the MachinePool is being deleted). They are read on
// every reconcile, so an edit takes effect mid-drain, as in Cluster API.
type DrainKnobs struct {
	// NodeDrainTimeoutSeconds is MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds; nil means no limit,
	// so a blocking PodDisruptionBudget holds the removal, as for a MachineDeployment.
	NodeDrainTimeoutSeconds *int32
	// NodeVolumeDetachTimeoutSeconds is spec.template.spec.deletion.nodeVolumeDetachTimeoutSeconds; nil means no limit.
	NodeVolumeDetachTimeoutSeconds *int32
	// ExcludeDraining is true when machine.cluster.x-k8s.io/exclude-node-draining is set: no cordon, no eviction.
	ExcludeDraining bool
	// ExcludeVolumeDetachWait is true when machine.cluster.x-k8s.io/exclude-wait-for-node-volume-detach is set.
	ExcludeVolumeDetachWait bool
}

// DrainKnobsFor reads the drain settings of a pool. An exclusion annotation counts when it is present, whatever its
// value, as in Cluster API.
//
// Parameters:
//   - machinePool is the owning MachinePool; its machine template carries the time limits and the steady annotations.
//   - pool is the IBMCloudManagedMachinePool; its annotations override.
func DrainKnobsFor(machinePool *clusterv1.MachinePool, pool *infrav1.IBMCloudManagedMachinePool) DrainKnobs {
	template := machinePool.Spec.Template
	has := func(annotation string) bool {
		_, onTemplate := template.Annotations[annotation]
		_, onPool := pool.Annotations[annotation]
		return onTemplate || onPool
	}
	return DrainKnobs{
		NodeDrainTimeoutSeconds:        template.Spec.Deletion.NodeDrainTimeoutSeconds,
		NodeVolumeDetachTimeoutSeconds: template.Spec.Deletion.NodeVolumeDetachTimeoutSeconds,
		ExcludeDraining:                has(clusterv1.ExcludeNodeDrainingAnnotation),
		ExcludeVolumeDetachWait:        has(clusterv1.ExcludeWaitForNodeVolumeDetachAnnotation),
	}
}

// DrainStepInput are the inputs of NewDrainStep.
type DrainStepInput struct {
	// WorkloadClient is the workload cluster's client from ClusterCache.GetClient. Required.
	WorkloadClient client.Client
	// Clock is the controller's clock. Required.
	Clock clock.PassiveClock
	// Recorder emits the drain events on Pool. Required.
	Recorder record.EventRecorder
	// Pool is the IBMCloudManagedMachinePool that drains; it owns the drain (drain-owner marker) and receives the
	// events. Its Draining condition, as read, tells whether DrainBlocked is new. Required.
	Pool *infrav1.IBMCloudManagedMachinePool
	// Knobs are the pool's drain settings (DrainKnobsFor).
	Knobs DrainKnobs
	// RecordedStartTime is the drain start the removal recorded (status.disruption.drainStartTime); zero when none is
	// recorded yet. The volume detach time limit counts from it when no victim Node carries a drain-start-time marker:
	// the drain is excluded, so nothing was cordoned, or no victim has a Node. Without it a limit the user set would
	// never run out in those cases. Optional.
	RecordedStartTime time.Time
}

// DrainStep cordons and drains one set of victim Nodes and waits for their volumes to detach, one reconcile at a time.
// Create it with NewDrainStep.
type DrainStep struct {
	helper   *drain.Helper
	recorder record.EventRecorder
	pool     *infrav1.IBMCloudManagedMachinePool
	knobs    DrainKnobs
	// recordedStart is DrainStepInput.RecordedStartTime.
	recordedStart time.Time
}

// DrainStepResult is what one Evict or Check observed.
type DrainStepResult struct {
	// Progress is the drain progress of the victims, for DisruptionObservation.Drain.
	Progress DrainProgress
	// Message is the Draining condition message: one line block per Node that still has pods or attached volumes, in
	// Node name order, for at most drainMessageNodes Nodes and then a count of the others, cut at
	// maxDrainMessageLength; deterministic, so a repeated reconcile writes nothing new. "" when nothing is left.
	Message string
	// StartTime is the earliest drain-start-time marker of the victims (when the drain started); zero when none is
	// recorded yet.
	StartTime time.Time
}

// NewDrainStep validates in and returns the drain step of one reconcile.
//
// Parameters:
//   - in are the clients, the clock, the pool and its knobs; see DrainStepInput.
//
// It returns an error naming every missing input.
func NewDrainStep(in DrainStepInput) (*DrainStep, error) {
	var problems []string
	if in.WorkloadClient == nil {
		problems = append(problems, "WorkloadClient is nil; pass ClusterCache.GetClient for the Cluster")
	}
	if in.Clock == nil {
		problems = append(problems, "Clock is nil; pass the controller's clock")
	}
	if in.Recorder == nil {
		problems = append(problems, "Recorder is nil; pass the controller's event recorder")
	}
	if in.Pool == nil || in.Pool.Name == "" || in.Pool.Namespace == "" {
		problems = append(problems, "Pool must be the IBMCloudManagedMachinePool with name and namespace")
	}
	if len(problems) > 0 {
		return nil, errors.New("managed: drain step: " + strings.Join(problems, "; "))
	}
	return &DrainStep{
		helper: &drain.Helper{
			Client:           in.WorkloadClient,
			Clock:            in.Clock,
			EvictionBudget:   EvictionBudgetPerReconcile,
			AnnotationPrefix: infrav1.NodeAnnotationPrefix,
		},
		recorder:      in.Recorder,
		pool:          in.Pool,
		knobs:         in.Knobs,
		recordedStart: in.RecordedStartTime,
	}, nil
}

// Evict carries out the Draining phase: unless the drain is excluded, it cordons every victim before it evicts from
// any, so that evicted pods never land on another victim, and then drains each one. Once a Node's drain time limit has
// passed it stops evicting there: it lists what is left with RemainingPods, which sends no eviction, and emits
// DrainTimeoutExceeded with that list; the removal then moves on to the volume detach wait. A PodDisruptionBudget
// refusal is never forced: the Node stays cordoned and DrainBlocked is reported.
//
// Parameters:
//   - ctx is the reconcile context.
//   - nodes are the victim Nodes as read from the workload cluster; a victim without a Node counts as drained.
//
// It returns an error when a cordon or a pod listing fails; the caller requeues.
func (d *DrainStep) Evict(ctx context.Context, nodes []corev1.Node) (DrainStepResult, error) {
	if d.knobs.ExcludeDraining {
		return DrainStepResult{Progress: DrainProgress{Excluded: true}}, nil
	}
	owner := DrainOwner(d.pool)
	for i := range nodes {
		// Cordon is idempotent and keeps an existing drain-start-time, so the time limit never restarts.
		if err := d.helper.Cordon(ctx, &nodes[i], drain.CordonOptions{Owner: owner, ExcludeFromLoadBalancers: ExcludeFromLoadBalancersDuringDrain}); err != nil {
			return DrainStepResult{}, err
		}
	}
	return d.drainNodes(ctx, nodes, true)
}

// Check observes the drain in the phases after Draining, without evicting: a pod that came back on a drained Node
// sends the removal back to Draining, so the IBM call is never made while evictable pods remain. A Node whose time
// limit has passed counts as drained without any call. Once every victim is drained, it checks volume detach, unless
// that wait is excluded or its time limit has passed.
//
// Parameters:
//   - ctx is the reconcile context.
//   - nodes are the victim Nodes as read from the workload cluster.
//
// It returns an error when a pod or volume listing fails.
func (d *DrainStep) Check(ctx context.Context, nodes []corev1.Node) (DrainStepResult, error) {
	result := DrainStepResult{Progress: DrainProgress{Excluded: d.knobs.ExcludeDraining}}
	if !d.knobs.ExcludeDraining {
		var err error
		if result, err = d.drainNodes(ctx, nodes, false); err != nil {
			return DrainStepResult{}, err
		}
		if !result.Progress.Done && !result.Progress.TimedOut {
			return result, nil
		}
	}
	return d.checkDetach(ctx, nodes, result)
}

// drainNodes drains (evict true) or checks (evict false) every victim and aggregates the result: Done when every Node
// is drained, TimedOut when every Node is drained or past its time limit and at least one is past it (then the
// remaining pods go with the server, Cluster API semantics).
//
// Parameters:
//   - ctx is the reconcile context.
//   - nodes are the victim Nodes; cordoned when evict is true.
//   - evict selects Drain (evictions) or RemainingPods (no eviction) for the Nodes within their time limit.
func (d *DrainStep) drainNodes(ctx context.Context, nodes []corev1.Node, evict bool) (DrainStepResult, error) {
	var result DrainStepResult
	pending, timedOut := 0, 0
	var lines []string
	for i := range nodes {
		node := &nodes[i]
		start, _, err := d.helper.DrainStartTime(node)
		if err != nil {
			return DrainStepResult{}, err
		}
		result.StartTime = earliest(result.StartTime, start)
		exceeded, err := d.helper.DrainTimeoutExceeded(node, d.knobs.NodeDrainTimeoutSeconds)
		if err != nil {
			return DrainStepResult{}, err
		}
		if exceeded {
			timedOut++
			if evict {
				if err := d.reportTimeout(ctx, node, start); err != nil {
					return DrainStepResult{}, err
				}
			}
			continue
		}
		nodeResult, err := d.podsLeft(ctx, node, evict)
		if err != nil {
			return DrainStepResult{}, err
		}
		if !nodeResult.Done {
			pending++
			lines = append(lines, fmt.Sprintf("Node %s: %s", node.Name, nodeResult.ConditionMessage(start, drainMessagePodsPerList)))
		}
		result.Progress.Blocked = result.Progress.Blocked || len(nodeResult.Blocked) > 0
	}
	result.Progress.Done = pending == 0 && timedOut == 0
	result.Progress.TimedOut = pending == 0 && timedOut > 0
	result.Message = nodeLinesMessage(lines, "are not drained yet")
	if evict && result.Progress.Blocked {
		d.reportBlocked(result)
	}
	return result, nil
}

// podsLeft drains node (evict true) or lists the pods still on it without evicting (evict false).
//
// Parameters:
//   - ctx is the reconcile context.
//   - node is the victim Node.
//   - evict selects Drain or RemainingPods.
func (d *DrainStep) podsLeft(ctx context.Context, node *corev1.Node, evict bool) (drain.DrainResult, error) {
	if evict {
		return d.helper.Drain(ctx, node)
	}
	return d.helper.RemainingPods(ctx, node)
}

// reportTimeout emits DrainTimeoutExceeded for a Node whose drain time limit has passed, naming the pods still on it:
// RemainingPods lists them without evicting, and Drain is not called for the Node again. PodDisruptionBudget
// refusals are only in the last Draining message, so the Warning names the pods, not the budgets.
//
// Parameters:
//   - ctx is the reconcile context.
//   - node is the Node.
//   - start is its drain start.
func (d *DrainStep) reportTimeout(ctx context.Context, node *corev1.Node, start time.Time) error {
	remaining, err := d.helper.RemainingPods(ctx, node)
	if err != nil {
		return err
	}
	if remaining.Done {
		return nil
	}
	d.recorder.Eventf(d.pool, corev1.EventTypeWarning, EventDrainTimeoutExceeded,
		"the drain time limit of %ds (MachinePool spec.template.spec.deletion.nodeDrainTimeoutSeconds) has passed for Node %s; "+
			"CAPIBM stops evicting and proceeds: %s", ptrValue(d.knobs.NodeDrainTimeoutSeconds), node.Name,
		remaining.ConditionMessage(start, drainMessagePodsPerList))
	return nil
}

// reportBlocked emits DrainBlocked when a PodDisruptionBudget refused an eviction, on entry (the Draining condition as
// read had another reason) and then once every DrainBlockedEventInterval after the drain start.
// result is the drain result of this reconcile.
func (d *DrainStep) reportBlocked(result DrainStepResult) {
	previous := ""
	if draining := conditions.Get(d.pool, infrav1.IBMCloudManagedMachinePoolDrainingCondition); draining != nil {
		previous = draining.Reason
	}
	if !drainBlockedEventDue(previous, result.StartTime, d.helper.Clock.Now()) {
		return
	}
	d.recorder.Eventf(d.pool, corev1.EventTypeWarning, EventDrainBlocked,
		"a PodDisruptionBudget refuses an eviction; the Nodes stay cordoned and the evictions are retried, never forced: %s",
		result.Message)
}

// drainBlockedEventDue reports whether DrainBlocked is emitted in this reconcile: on entry, when the Draining condition
// had another reason, and then in the first reconcile after each DrainBlockedEventInterval since the drain start. The
// drain reconciles every DrainRetryInterval, so "the first reconcile after a multiple of the interval" is the one whose
// time since the start is less than DrainRetryInterval past that multiple. A jittered requeue can make that window
// hold for two reconciles or for none; the event recorder merges a repeat, and the condition always carries the
// state, so the event only needs to be roughly periodic.
//
// Parameters:
//   - previousReason is the Draining condition's reason as read.
//   - start is the drain start; zero emits only on entry.
//   - now is the current time.
func drainBlockedEventDue(previousReason string, start, now time.Time) bool {
	if previousReason != string(ReasonDrainBlocked) {
		return true
	}
	if start.IsZero() || now.Before(start) {
		return false
	}
	sinceBoundary := now.Sub(start) % DrainBlockedEventInterval
	return now.Sub(start) >= DrainBlockedEventInterval && sinceBoundary < DrainRetryInterval
}

// checkDetach adds the volume detach wait to a finished drain. The detach time limit counts from the drain start, the
// earliest drain-start-time marker of the victims, or the start the removal recorded when no victim carries one (the
// drain is excluded, or no victim has a Node). Neither the Node markers nor status.disruption record when the detach
// wait began, so a limit the user set may end the wait earlier than Cluster API would, never later.
//
// Parameters:
//   - ctx is the reconcile context.
//   - nodes are the victim Nodes.
//   - result is the drain result so far.
func (d *DrainStep) checkDetach(ctx context.Context, nodes []corev1.Node, result DrainStepResult) (DrainStepResult, error) {
	if d.knobs.ExcludeVolumeDetachWait {
		result.Progress.DetachExcluded = true
		return result, nil
	}
	start := result.StartTime
	if start.IsZero() {
		start = d.recordedStart
	}
	if drain.TimeoutExceeded(d.helper.Clock.Now(), start, d.knobs.NodeVolumeDetachTimeoutSeconds) {
		result.Progress.DetachTimedOut = true
		return result, nil
	}
	var lines []string
	for i := range nodes {
		detached, message, err := d.helper.VolumesDetached(ctx, &nodes[i])
		if err != nil {
			return DrainStepResult{}, err
		}
		if !detached {
			lines = append(lines, fmt.Sprintf("Node %s: %s", nodes[i].Name, message))
		}
	}
	result.Progress.VolumesDetached = len(lines) == 0
	result.Message = nodeLinesMessage(lines, "still have attached volumes")
	return result, nil
}

// nodeLinesMessage joins the per-Node blocks of a drain message: the first drainMessageNodes blocks, then one line that
// counts the other Nodes, so that the message stays the same size however many Nodes a removal drains, and the whole
// text cut at maxDrainMessageLength. The blocks come in Node name order, so the same Nodes are named on every reconcile.
//
// Parameters:
//   - lines are the blocks, one per Node that is not done, in Node name order.
//   - pending says what the counted Nodes still have, for example "are not drained yet".
func nodeLinesMessage(lines []string, pending string) string {
	if len(lines) > drainMessageNodes {
		more := len(lines) - drainMessageNodes
		lines = append(slices.Clip(lines[:drainMessageNodes]), fmt.Sprintf("and %d more Nodes %s", more, pending))
	}
	return truncateMessage(strings.Join(lines, "\n"), maxDrainMessageLength)
}

// truncateMessage returns message when it has at most limit bytes, and otherwise its longest prefix that ends on a
// character boundary and leaves room for a marker that says the rest was cut, followed by that marker. The result is
// never longer than limit and depends only on message, so a repeated reconcile writes the same text.
//
// Parameters:
//   - message is the text.
//   - limit is the maximum length in bytes; it must exceed the marker's length.
func truncateMessage(message string, limit int) string {
	const marker = "\n... (cut to stay within the condition message limit)"
	if len(message) <= limit {
		return message
	}
	cut := limit - len(marker)
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut] + marker
}

// RecordDrainPhaseChange emits the Normal events of a drain phase change: DrainStarted when a removal enters Draining
// from no recorded phase, DrainCompleted when it leaves Draining for the volume detach wait. Under
// exclude-node-draining nothing is cordoned and nothing is evicted, so the texts say that instead of "cordoning" and
// "drained". The reasons stay the same, so that whoever follows the phase changes sees the same sequence of events
// either way.
//
// Parameters:
//   - recorder emits the event.
//   - pool receives it.
//   - from is status.disruption.phase before the step; "" when none was recorded.
//   - to is the phase the step records.
//   - nodes is the number of victim Nodes.
//   - drainExcluded is DrainKnobs.ExcludeDraining as read in this reconcile.
func RecordDrainPhaseChange(recorder record.EventRecorder, pool *infrav1.IBMCloudManagedMachinePool, from, to infrav1.WorkerDisruptionPhase,
	nodes int, drainExcluded bool) {
	started := from == "" && to == infrav1.WorkerDisruptionPhaseDraining
	completed := from == infrav1.WorkerDisruptionPhaseDraining && to == infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach
	switch {
	case started && drainExcluded:
		recorder.Eventf(pool, corev1.EventTypeNormal, EventDrainStarted, "the drain of the %d Nodes of pool %s is excluded (%s): "+
			"CAPIBM leaves them schedulable and evicts no pod", nodes, pool.Spec.PoolName, clusterv1.ExcludeNodeDrainingAnnotation)
	case started:
		recorder.Eventf(pool, corev1.EventTypeNormal, EventDrainStarted, "cordoning %d Nodes of pool %s, then evicting their pods",
			nodes, pool.Spec.PoolName)
	case completed && drainExcluded:
		// The annotation may have been set mid-drain, after some evictions, so the text speaks of no more evictions.
		recorder.Eventf(pool, corev1.EventTypeNormal, EventDrainCompleted, "the %d Nodes of pool %s are not drained: %s is set, "+
			"so CAPIBM evicts no more pods", nodes, pool.Spec.PoolName, clusterv1.ExcludeNodeDrainingAnnotation)
	case completed:
		recorder.Eventf(pool, corev1.EventTypeNormal, EventDrainCompleted, "the %d Nodes of pool %s are drained", nodes, pool.Spec.PoolName)
	}
}

// VictimNodes returns the Nodes whose spec.providerID is the provider ID of one of the workers, sorted by name. It
// touches only Nodes of this IBM pool: on IKS and ROKS a Node's spec.providerID was observed to equal exactly the
// provider ID built from its worker (ibm://<account>///<clusterID>/<workerID>).
//
// Parameters:
//   - nodes are every Node of the workload cluster.
//   - accountID, clusterID name the IBM cluster (AccountIDFromCRN, status.clusterID).
//   - workerIDs are the IBM workers being removed.
func VictimNodes(nodes []corev1.Node, accountID, clusterID string, workerIDs []string) []corev1.Node {
	wanted := make(map[string]bool, len(workerIDs))
	for _, id := range workerIDs {
		wanted[ProviderID(accountID, clusterID, id)] = true
	}
	var victims []corev1.Node
	for i := range nodes {
		if nodes[i].Spec.ProviderID != "" && wanted[nodes[i].Spec.ProviderID] {
			victims = append(victims, nodes[i])
		}
	}
	slices.SortFunc(victims, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	return victims
}

// earliest returns the earlier of two times, ignoring a zero one.
//
// Parameters:
//   - a, b are the times.
func earliest(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero() || a.Before(b):
		return a
	default:
		return b
	}
}

// ptrValue returns *v, or 0 for nil, for messages.
func ptrValue(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
