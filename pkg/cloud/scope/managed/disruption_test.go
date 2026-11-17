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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
)

// disruptionNow is the fixed clock reading of the disruption tests.
func disruptionNow() time.Time {
	return time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
}

// newDisruptionWorker returns a worker of pool in zone eu-de-1 with the given state and health.
func newDisruptionWorker(id, pool, state, health string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: pool, Zone: "eu-de-1", ActualState: state, HealthState: health}
}

// newDisruptionPool returns an IBMCloudManagedMachinePool whose last disruption was at last (zero: never).
func newDisruptionPool(name string, last time.Time) infrav1.IBMCloudManagedMachinePool {
	pool := infrav1.IBMCloudManagedMachinePool{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
	pool.Status.LastDisruptionTime = metav1.NewTime(last)
	return pool
}

// disruptionSteadyWorkers are four ready workers in two pools.
func disruptionSteadyWorkers() []kubernetesservice.Worker {
	return []kubernetesservice.Worker{
		newDisruptionWorker("kube-c1-pool-a-00000001", "c1-aaaaaaa", "deployed", "normal"),
		newDisruptionWorker("kube-c1-pool-a-00000002", "c1-aaaaaaa", "deployed", "normal"),
		newDisruptionWorker("kube-c1-pool-b-00000003", "c1-bbbbbbb", "deployed", "normal"),
		newDisruptionWorker("kube-c1-pool-b-00000004", "c1-bbbbbbb", "deployed", "normal"),
	}
}

// replaceCheckInput is a replace of one worker of pool-a on an IKS cluster with steady workers.
func replaceCheckInput() DisruptionCheck {
	return DisruptionCheck{
		Operation:       infrav1.WorkerDisruptionOperationReplace,
		Distribution:    controlplanev1.DistributionKubernetes,
		Now:             disruptionNow(),
		Workers:         disruptionSteadyWorkers(),
		Pools:           []infrav1.IBMCloudManagedMachinePool{newDisruptionPool("pool-a", time.Time{}), newDisruptionPool("pool-b", time.Time{})},
		RemovedReady:    1,
		MachinePoolName: "mp-a",
		Replicas:        2,
		ZoneCount:       1,
	}
}

func TestCheckDisruption(t *testing.T) {
	now := disruptionNow()
	testCases := []struct {
		name        string
		modify      func(in *DisruptionCheck)
		wantAllowed bool
		wantReason  DisruptionReason
		wantMessage []string
		wantStuck   []string
	}{
		{
			name:        "steady cluster allows a replace",
			modify:      func(*DisruptionCheck) {},
			wantAllowed: true,
		},
		{
			name: "provisioning worker in another pool blocks",
			modify: func(in *DisruptionCheck) {
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-b-00000005", "c1-bbbbbbb", "provisioning", ""))
			},
			wantReason:  ReasonWaitingForSettledCluster,
			wantMessage: []string{"worker kube-c1-pool-b-00000005 is provisioning"},
		},
		{
			name: "settle delay from another pool's last disruption blocks",
			modify: func(in *DisruptionCheck) {
				in.Pools[1] = newDisruptionPool("pool-b", now.Add(-time.Minute))
			},
			wantReason:  ReasonWaitingForSettledCluster,
			wantMessage: []string{"IBMCloudManagedMachinePool pool-b removed workers at 2026-11-16T11:59:00Z", "waits until 2026-11-16T12:02:00Z"},
		},
		{
			name: "capacity blocks a replace with the size that allows it",
			modify: func(in *DisruptionCheck) {
				in.Distribution = controlplanev1.DistributionOpenShift
				in.Workers = in.Workers[:2]
			},
			wantReason:  ReasonWaitingForCapacity,
			wantMessage: []string{"the cluster has 2 ready workers; removing 1 would leave 1, below IBM's minimum of 2", "E1437", "scale MachinePool mp-a to 3"},
		},
		{
			name: "failed worker does not block",
			modify: func(in *DisruptionCheck) {
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-b-00000005", "c1-bbbbbbb", "provision_failed", "critical"))
			},
			wantAllowed: true,
		},
		{
			name: "worker transitional longer than WorkerStuckAfter does not block",
			modify: func(in *DisruptionCheck) {
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-b-00000005", "c1-bbbbbbb", "provisioning", ""))
				in.TransitionalSince = map[string]time.Time{"kube-c1-pool-b-00000005": now.Add(-WorkerStuckAfter - time.Second)}
			},
			wantAllowed: true,
			wantMessage: []string{"WorkerOperationStuck: IBM has been changing worker kube-c1-pool-b-00000005 for more than 60 minutes"},
			wantStuck:   []string{"kube-c1-pool-b-00000005"},
		},
		{
			name: "removal of a pool with no ready workers while its workers provision is allowed",
			modify: func(in *DisruptionCheck) {
				in.Operation = infrav1.WorkerDisruptionOperationDeletePool
				in.RemovedReady = 0
				in.Workers = append(in.Workers,
					newDisruptionWorker("kube-c1-pool-c-00000005", "c1-ccccccc", "provisioning", ""),
					newDisruptionWorker("kube-c1-pool-c-00000006", "c1-ccccccc", "provision_pending", ""))
				in.Pools = append(in.Pools, newDisruptionPool("pool-c", now.Add(-time.Minute)))
			},
			wantAllowed: true,
		},
		{
			name: "shrink of a pool with no ready workers while its workers provision is allowed",
			modify: func(in *DisruptionCheck) {
				in.Operation = infrav1.WorkerDisruptionOperationScaleDown
				in.RemovedReady = 0
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-c-00000005", "c1-ccccccc", "deploying", ""))
			},
			wantAllowed: true,
		},
		{
			name: "transitional worker just under WorkerStuckAfter still blocks",
			modify: func(in *DisruptionCheck) {
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-b-00000005", "c1-bbbbbbb", "deploying", ""))
				in.TransitionalSince = map[string]time.Time{"kube-c1-pool-b-00000005": now.Add(-WorkerStuckAfter)}
			},
			wantReason:  ReasonWaitingForSettledCluster,
			wantMessage: []string{"is deploying"},
		},
		{
			name: "pending operation blocks without naming IBM's text",
			modify: func(in *DisruptionCheck) {
				in.Workers[3].PendingOperation = "some IBM operation text"
			},
			wantReason:  ReasonWaitingForSettledCluster,
			wantMessage: []string{"worker kube-c1-pool-b-00000004 is running an IBM operation"},
		},
		{
			name: "several changing workers name the first and count the rest",
			modify: func(in *DisruptionCheck) {
				in.Workers = append([]kubernetesservice.Worker{
					newDisruptionWorker("kube-c1-pool-b-00000009", "c1-bbbbbbb", "deleting", "normal"),
					newDisruptionWorker("kube-c1-pool-b-00000007", "c1-bbbbbbb", "reloading", ""),
					newDisruptionWorker("kube-c1-pool-b-00000008", "c1-bbbbbbb", "provisioned", ""),
				}, in.Workers...)
			},
			wantReason:  ReasonWaitingForSettledCluster,
			wantMessage: []string{"worker kube-c1-pool-b-00000007 is reloading (and 2 more changing workers)"},
		},
		{
			name: "deleted records neither block nor count as ready",
			modify: func(in *DisruptionCheck) {
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-b-00000005", "c1-bbbbbbb", "deleted", ""))
				in.Workers[3].PendingOperation = ""
			},
			wantAllowed: true,
		},
		{
			name: "settle delay ends exactly DisruptionSettleDelay after the newest disruption",
			modify: func(in *DisruptionCheck) {
				in.Pools[0] = newDisruptionPool("pool-a", now.Add(-DisruptionSettleDelay))
				in.Pools[1] = newDisruptionPool("pool-b", now.Add(-time.Hour))
			},
			wantAllowed: true,
		},
		{
			name: "settle delay names the newest of equal times by name",
			modify: func(in *DisruptionCheck) {
				in.Pools = []infrav1.IBMCloudManagedMachinePool{newDisruptionPool("pool-z", now.Add(-time.Minute)), newDisruptionPool("pool-b", now.Add(-time.Minute))}
			},
			wantReason:  ReasonWaitingForSettledCluster,
			wantMessage: []string{"IBMCloudManagedMachinePool pool-b removed workers"},
		},
		{
			name: "pre-call gate skips the settle delay",
			modify: func(in *DisruptionCheck) {
				in.Pools[0] = newDisruptionPool("pool-a", now.Add(-time.Minute))
				in.SkipSettleDelay = true
			},
			wantAllowed: true,
		},
		{
			name: "pre-call gate still checks the settled cluster",
			modify: func(in *DisruptionCheck) {
				in.SkipSettleDelay = true
				in.Workers = append(in.Workers, newDisruptionWorker("kube-c1-pool-b-00000005", "c1-bbbbbbb", "provisioning", ""))
			},
			wantReason: ReasonWaitingForSettledCluster,
		},
		{
			name: "capacity blocks a scale-down with WaitingForClusterMinimum",
			modify: func(in *DisruptionCheck) {
				in.Operation = infrav1.WorkerDisruptionOperationScaleDown
				in.RemovedReady = 4
			},
			wantReason:  ReasonWaitingForClusterMinimum,
			wantMessage: []string{"removing 4 would leave 0, below IBM's minimum of 1 workers for a Kubernetes cluster", "keep more workers"},
		},
		{
			name: "capacity blocks a pool deletion with WaitingForClusterMinimum",
			modify: func(in *DisruptionCheck) {
				in.Operation = infrav1.WorkerDisruptionOperationDeletePool
				in.Distribution = controlplanev1.DistributionOpenShift
				in.RemovedReady = 3
			},
			wantReason:  ReasonWaitingForClusterMinimum,
			wantMessage: []string{"removing 3 would leave 1, below IBM's minimum of 2 workers for a OpenShift cluster"},
		},
		{
			name: "the one-worker IKS cluster cannot roll",
			modify: func(in *DisruptionCheck) {
				in.Workers = in.Workers[:1]
				in.Replicas = 1
			},
			wantReason:  ReasonWaitingForCapacity,
			wantMessage: []string{"removing 1 would leave 0", "scale MachinePool mp-a to 2"},
		},
		{
			name: "capacity suggestion adds whole workers per zone",
			modify: func(in *DisruptionCheck) {
				in.Distribution = controlplanev1.DistributionOpenShift
				in.Workers = in.Workers[:1]
				in.Replicas = 3
				in.ZoneCount = 3
			},
			wantReason:  ReasonWaitingForCapacity,
			wantMessage: []string{"removing 1 would leave 0", "scale MachinePool mp-a to 6"},
		},
		{
			name: "capacity message without a MachinePool name gives no size",
			modify: func(in *DisruptionCheck) {
				in.Workers = in.Workers[:1]
				in.MachinePoolName = ""
				in.ZoneCount = 0
			},
			wantReason:  ReasonWaitingForCapacity,
			wantMessage: []string{"add ready workers to allow a rolling replacement"},
		},
		{
			name: "the zero-ready exemption still checks the capacity",
			modify: func(in *DisruptionCheck) {
				in.Operation = infrav1.WorkerDisruptionOperationDeletePool
				in.RemovedReady = 0
				in.Workers = []kubernetesservice.Worker{newDisruptionWorker("kube-c1-pool-a-00000001", "c1-aaaaaaa", "provisioning", "")}
			},
			wantReason: ReasonWaitingForClusterMinimum,
		},
		{
			name: "stuck workers are listed when the capacity blocks",
			modify: func(in *DisruptionCheck) {
				in.Workers = []kubernetesservice.Worker{
					newDisruptionWorker("kube-c1-pool-a-00000001", "c1-aaaaaaa", "deployed", "normal"),
					newDisruptionWorker("kube-c1-pool-a-00000002", "c1-aaaaaaa", "provisioning", ""),
				}
				in.TransitionalSince = map[string]time.Time{"kube-c1-pool-a-00000002": now.Add(-2 * time.Hour)}
			},
			wantReason: ReasonWaitingForCapacity,
			wantStuck:  []string{"kube-c1-pool-a-00000002"},
		},
		{
			name: "more than three stuck workers are counted",
			modify: func(in *DisruptionCheck) {
				in.TransitionalSince = map[string]time.Time{}
				for _, id := range []string{"w-5", "w-1", "w-4", "w-2", "w-3"} {
					in.Workers = append(in.Workers, newDisruptionWorker(id, "c1-bbbbbbb", "provisioning", ""))
					in.TransitionalSince[id] = now.Add(-2 * time.Hour)
				}
			},
			wantAllowed: true,
			wantMessage: []string{"workers w-1, w-2, w-3 and 2 more for more than 60 minutes"},
			wantStuck:   []string{"w-1", "w-2", "w-3", "w-4", "w-5"},
		},
		{
			name: "two stuck workers are both named",
			modify: func(in *DisruptionCheck) {
				in.TransitionalSince = map[string]time.Time{}
				for _, id := range []string{"w-2", "w-1"} {
					in.Workers = append(in.Workers, newDisruptionWorker(id, "c1-bbbbbbb", "reload_pending", ""))
					in.TransitionalSince[id] = now.Add(-2 * time.Hour)
				}
			},
			wantAllowed: true,
			wantMessage: []string{"workers w-1, w-2 for more than"},
			wantStuck:   []string{"w-1", "w-2"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := replaceCheckInput()
			tc.modify(&in)
			got, err := CheckDisruption(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Allowed).To(Equal(tc.wantAllowed), got.Message)
			g.Expect(got.Reason).To(Equal(tc.wantReason))
			for _, part := range tc.wantMessage {
				g.Expect(got.Message).To(ContainSubstring(part))
			}
			g.Expect(got.StuckWorkerIDs).To(Equal(tc.wantStuck))
			if tc.wantAllowed && len(tc.wantStuck) == 0 {
				g.Expect(got.Message).To(BeEmpty())
			}

			// No churn: the same observation in another order gives the same answer, byte for byte.
			reversed := in
			reversed.Workers = make([]kubernetesservice.Worker, len(in.Workers))
			for i, w := range in.Workers {
				reversed.Workers[len(in.Workers)-1-i] = w
			}
			again, err := CheckDisruption(reversed)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(again).To(Equal(got))
		})
	}
}

func TestCheckDisruptionValidation(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(in *DisruptionCheck)
		wantErr string
	}{
		{name: "unknown operation", modify: func(in *DisruptionCheck) { in.Operation = "Reboot" }, wantErr: `DisruptionCheck.Operation is "Reboot"`},
		{name: "empty operation", modify: func(in *DisruptionCheck) { in.Operation = "" }, wantErr: "DisruptionCheck.Operation"},
		{name: "unknown distribution", modify: func(in *DisruptionCheck) { in.Distribution = "Other" }, wantErr: `DisruptionCheck.Distribution is "Other"`},
		{name: "no clock reading", modify: func(in *DisruptionCheck) { in.Now = time.Time{} }, wantErr: "DisruptionCheck.Now is not set"},
		{name: "negative removed workers", modify: func(in *DisruptionCheck) { in.RemovedReady = -1 }, wantErr: "DisruptionCheck.RemovedReady is -1"},
		{name: "MachinePool without zones", modify: func(in *DisruptionCheck) { in.ZoneCount = 0 }, wantErr: "DisruptionCheck.ZoneCount is 0 for MachinePool mp-a"},
		{name: "negative replicas", modify: func(in *DisruptionCheck) { in.Replicas = -2 }, wantErr: "DisruptionCheck.Replicas is -2"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := replaceCheckInput()
			tc.modify(&in)
			_, err := CheckDisruption(in)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

// TestDisruptionReasonsMatchTheAPI keeps the reason tokens equal to the API's constants of every condition that defines
// them, so that the caller can set a decision's reason on the condition of its flow.
func TestDisruptionReasonsMatchTheAPI(t *testing.T) {
	g := NewWithT(t)
	sameToken := func(reason DisruptionReason, apiReasons ...string) {
		for _, apiReason := range apiReasons {
			g.Expect(string(reason)).To(Equal(apiReason))
		}
	}
	sameToken(ReasonWaitingForClusterState, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterStateReason,
		infrav1.IBMCloudManagedMachinePoolDeletingWaitingForClusterStateReason)
	sameToken(ReasonWaitingForSettledCluster, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForSettledClusterReason,
		infrav1.IBMCloudManagedMachinePoolDeletingWaitingForSettledClusterReason)
	sameToken(ReasonWaitingForClusterMinimum, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForClusterMinimumReason,
		infrav1.IBMCloudManagedMachinePoolDeletingWaitingForClusterMinimumReason)
	sameToken(ReasonWaitingForDisruptionToken, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForDisruptionTokenReason,
		infrav1.IBMCloudManagedMachinePoolDeletingWaitingForDisruptionTokenReason)
	sameToken(ReasonWaitingForWorkloadCluster, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyWaitingForWorkloadClusterReason,
		infrav1.IBMCloudManagedMachinePoolDrainingWaitingForWorkloadClusterReason)
	sameToken(ReasonDrainingNode, infrav1.IBMCloudManagedMachinePoolWorkerPoolReadyDrainingNodeReason,
		infrav1.IBMCloudManagedMachinePoolDrainingNodeReason)
	sameToken(ReasonWaitingForCapacity, "WaitingForCapacity")
	sameToken(ReasonWorkerOperationStuck, "WorkerOperationStuck")
}

func TestWorkerBeingRemoved(t *testing.T) {
	testCases := []struct {
		name    string
		state   string
		pending string
		want    bool
	}{
		{name: "deployed", state: "deployed", want: false},
		{name: "deleting", state: "deleting", want: true},
		{name: "deleted", state: "deleted", want: true},
		{name: "pending operation", state: "deployed", pending: "replace", want: true},
		{name: "provisioning replacement", state: "provisioning", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			w := newDisruptionWorker("kube-c1-pool-a-00000001", "c1-aaaaaaa", tc.state, "normal")
			w.PendingOperation = tc.pending
			g.Expect(WorkerBeingRemoved(w)).To(Equal(tc.want))
		})
	}
}

// passingGates returns gates that all pass.
func passingGates() DisruptionGates {
	return DisruptionGates{ClusterStateAllows: true, Disruption: DisruptionCheckResult{Allowed: true}, AutobalanceEnabled: true,
		OthersAvailable: true}
}

// passingObservation returns an observation in which every gate and check passes, for op in phase.
func passingObservation(op infrav1.WorkerDisruptionOperation, phase infrav1.WorkerDisruptionPhase) DisruptionObservation {
	return DisruptionObservation{
		Operation:                op,
		Phase:                    phase,
		Now:                      disruptionNow(),
		TriggerActive:            true,
		Token:                    TokenHeld,
		WorkloadClusterConnected: true,
		Gates:                    passingGates(),
		Drain:                    DrainProgress{Done: true, VolumesDetached: true},
		PreCallGate:              DisruptionCheckResult{Allowed: true},
	}
}

func TestDecideDisruptionStep(t *testing.T) {
	const (
		replace   = infrav1.WorkerDisruptionOperationReplace
		scaleDown = infrav1.WorkerDisruptionOperationScaleDown
		deletion  = infrav1.WorkerDisruptionOperationDeletePool

		gated       = infrav1.WorkerDisruptionPhase("")
		draining    = infrav1.WorkerDisruptionPhaseDraining
		detach      = infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach
		waitIBM     = infrav1.WorkerDisruptionPhaseWaitingForIBM
		removal     = infrav1.WorkerDisruptionPhaseWaitingForRemoval
		replacement = infrav1.WorkerDisruptionPhaseWaitingForReplacement
	)
	now := disruptionNow()
	testCases := []struct {
		name        string
		op          infrav1.WorkerDisruptionOperation
		phase       infrav1.WorkerDisruptionPhase
		modify      func(o *DisruptionObservation)
		want        DisruptionStep
		wantPhase   infrav1.WorkerDisruptionPhase
		wantReason  DisruptionReason
		wantDrain   DisruptionReason
		wantRelease bool
		wantMessage string
	}{
		// Every phase: a Cluster deletion stops everything.
		{name: "cluster deletion stops a gated replace", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.ClusterDeleting = true }, want: StepStop},
		{name: "cluster deletion stops a drain without uncordon", op: deletion, phase: draining, modify: func(o *DisruptionObservation) { o.ClusterDeleting = true; o.TriggerActive = false }, want: StepStop, wantPhase: draining},

		// Gated: the gates in order, then the token.
		{name: "no trigger and no removal in flight is nothing to do", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.TriggerActive = false; o.Token = TokenNotHeld }, want: StepNone},
		{name: "no trigger releases a held token", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) { o.TriggerActive = false }, want: StepNone, wantRelease: true},
		{name: "a pool deletion is always triggered", op: deletion, phase: gated, modify: func(o *DisruptionObservation) { o.TriggerActive = false; o.Token = TokenNotHeld }, want: StepAcquireToken},
		{name: "a gated replace of a deleting pool hands over", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.PoolDeleting = true }, want: StepHandOver},
		{name: "cluster state gate", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.ClusterStateAllows = false }, want: StepWait, wantReason: ReasonWaitingForClusterState, wantRelease: true},
		{name: "master update blocks a replace", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.MasterUpdating = true; o.Token = TokenNotHeld }, want: StepWait, wantReason: ReasonWaitingForControlPlaneUpgrade},
		{name: "master update does not block a scale-down", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.MasterUpdating = true }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "disruption guard reason and message pass through", op: replace, phase: gated, modify: func(o *DisruptionObservation) {
			o.Gates.Disruption = DisruptionCheckResult{Reason: ReasonWaitingForCapacity, Message: "scale MachinePool mp-a to 3"}
		}, want: StepWait, wantReason: ReasonWaitingForCapacity, wantRelease: true, wantMessage: "scale MachinePool mp-a to 3"},
		{name: "an unevaluated disruption guard fails closed", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.Disruption = DisruptionCheckResult{} }, want: StepWait, wantReason: ReasonWaitingForSettledCluster, wantRelease: true},
		{name: "autoscaled pool is not replaced", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.Autoscaled = true }, want: StepWait, wantReason: ReasonExternallyAutoscaled, wantRelease: true},
		{name: "autoscaled pool is not shrunk", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.Autoscaled = true }, want: StepWait, wantReason: ReasonExternallyAutoscaled, wantRelease: true},
		{name: "autoscaled pool is still drained before deletion", op: deletion, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.Autoscaled = true }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNodes, wantDrain: ReasonDrainingNode},
		{name: "auto-rebalance off blocks a replace", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.AutobalanceEnabled = false }, want: StepWait, wantReason: ReasonAutoRebalanceDisabled, wantRelease: true, wantMessage: "ibmcloud ks worker-pool rebalance"},
		{name: "auto-rebalance does not matter for a scale-down", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.AutobalanceEnabled = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "disconnected workload cluster blocks before any cordon", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) { o.WorkloadClusterConnected = false }, want: StepWait, wantReason: ReasonWaitingForWorkloadCluster, wantRelease: true},
		{name: "a broken worker of the pool holds a replace before its first cordon", op: replace, phase: gated, modify: func(o *DisruptionObservation) {
			o.Gates.OthersAvailable, o.Gates.UnavailableWorkers = false, []string{"w3"}
		}, want: StepWait, wantReason: ReasonWorkersNotReady, wantRelease: true, wantMessage: "waiting for worker w3 of the pool; repair or replace"},
		{name: "unevaluated other workers fail closed", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.OthersAvailable = false; o.Token = TokenNotHeld },
			want: StepWait, wantReason: ReasonWorkersNotReady, wantMessage: "the other workers of the pool were not evaluated"},
		{name: "the workload cluster connection is reported before the other workers", op: replace, phase: gated, modify: func(o *DisruptionObservation) {
			o.WorkloadClusterConnected, o.Gates.OthersAvailable = false, false
		}, want: StepWait, wantReason: ReasonWaitingForWorkloadCluster, wantRelease: true},
		{name: "other workers do not matter for a scale-down", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) { o.Gates.OthersAvailable = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "other workers do not matter for a replace in flight", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.Gates.OthersAvailable = false }, want: StepWaitForVolumeDetach, wantPhase: detach, wantReason: ReasonDrainingNode, wantDrain: ReasonWaitingForVolumeDetach},
		{name: "another pool's drain-owner marker defers", op: deletion, phase: gated, modify: func(o *DisruptionObservation) {
			o.Gates.OtherDrainOwners = []string{"ns/pool-b", "ns/pool-c"}
			o.Token = TokenNotHeld
		}, want: StepWait, wantReason: ReasonWaitingForDisruptionToken, wantMessage: "IBMCloudManagedMachinePool ns/pool-b, ns/pool-c"},
		{name: "another pool holds the token", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Token = TokenHeldByOther }, want: StepWait, wantReason: ReasonWaitingForDisruptionToken},
		{name: "gates pass without the token: acquire it", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Token = TokenNotHeld }, want: StepAcquireToken},
		{name: "the zero token state counts as not held", op: replace, phase: gated, modify: func(o *DisruptionObservation) { o.Token = "" }, want: StepAcquireToken},
		{name: "gates pass with the token: cordon and drain", op: replace, phase: gated, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},

		// Draining.
		{name: "pods remain: keep draining", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.Drain.Done = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "PDB refusal: keep draining, never forced", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.Drain.Done = false; o.Drain.Blocked = true }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainBlocked, wantMessage: "never forced"},
		{name: "pool deletion drains with DrainingNodes", op: deletion, phase: draining, modify: func(o *DisruptionObservation) { o.Drain.Done = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNodes, wantDrain: ReasonDrainingNode},
		{name: "drain done: wait for volume detach", op: replace, phase: draining, want: StepWaitForVolumeDetach, wantPhase: detach, wantReason: ReasonDrainingNode, wantDrain: ReasonWaitingForVolumeDetach},
		{name: "drain timed out: proceed to volume detach", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.Drain.Done = false; o.Drain.TimedOut = true }, want: StepWaitForVolumeDetach, wantPhase: detach, wantReason: ReasonDrainingNode, wantDrain: ReasonWaitingForVolumeDetach},
		{name: "drain excluded: proceed to volume detach", op: deletion, phase: draining, modify: func(o *DisruptionObservation) { o.Drain.Done = false; o.Drain.Excluded = true }, want: StepWaitForVolumeDetach, wantPhase: detach, wantReason: ReasonDrainingNodes, wantDrain: ReasonWaitingForVolumeDetach},
		{name: "workload cluster unreachable: hold, no IBM call", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.WorkloadClusterConnected = false; o.Drain.Done = false }, want: StepHold, wantPhase: draining, wantReason: ReasonWaitingForWorkloadCluster, wantDrain: ReasonWaitingForWorkloadCluster},
		{name: "a done flag without a connection is not trusted", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.WorkloadClusterConnected = false }, want: StepHold, wantPhase: draining, wantReason: ReasonWaitingForWorkloadCluster, wantDrain: ReasonWaitingForWorkloadCluster},
		{name: "unreachable workload cluster with the time limit passed proceeds", op: replace, phase: draining, modify: func(o *DisruptionObservation) {
			o.WorkloadClusterConnected = false
			o.Drain.Done = false
			o.Drain.TimedOut = true
		}, want: StepWaitForVolumeDetach, wantPhase: detach, wantReason: ReasonDrainingNode, wantDrain: ReasonWaitingForVolumeDetach},
		{name: "trigger gone while draining aborts", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.TriggerActive = false }, want: StepAbort, wantPhase: draining, wantRelease: true},
		{name: "replicas raised during a scale-down drain aborts", op: scaleDown, phase: draining, modify: func(o *DisruptionObservation) { o.TriggerActive = false; o.Drain.Done = false }, want: StepAbort, wantPhase: draining, wantRelease: true},
		{name: "a pool deletion never aborts", op: deletion, phase: draining, modify: func(o *DisruptionObservation) { o.TriggerActive = false; o.Drain.Done = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNodes, wantDrain: ReasonDrainingNode},
		{name: "pool deletion during a replace drain hands over, keeping the cordon", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.PoolDeleting = true; o.TriggerActive = false }, want: StepHandOver, wantPhase: draining},
		{name: "restart without the token re-acquires it", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.Token = TokenNotHeld; o.Drain.Done = false }, want: StepAcquireToken, wantPhase: draining},
		{name: "another holder of the token holds the drain cordoned", op: replace, phase: draining, modify: func(o *DisruptionObservation) { o.Token = TokenHeldByOther }, want: StepHold, wantPhase: draining, wantReason: ReasonWaitingForDisruptionToken},
		{name: "move cleared status: resume from the Node markers", op: replace, phase: gated, modify: func(o *DisruptionObservation) {
			o.ResumeFromNodeMarkers = true
			o.Drain.Done = false
			o.Gates = DisruptionGates{} // after a move the gates would fail (lastDisruptionTime = now), but a resumed drain skips them
		}, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "resume after a move whose trigger went away aborts", op: scaleDown, phase: gated, modify: func(o *DisruptionObservation) {
			o.ResumeFromNodeMarkers = true
			o.TriggerActive = false
		}, want: StepAbort, wantPhase: draining, wantRelease: true},

		// WaitingForVolumeDetach, then the pre-call gate and the IBM call.
		{name: "volumes still attached: wait", op: replace, phase: detach, modify: func(o *DisruptionObservation) { o.Drain.VolumesDetached = false }, want: StepWaitForVolumeDetach, wantPhase: detach, wantReason: ReasonDrainingNode, wantDrain: ReasonWaitingForVolumeDetach},
		{name: "volumes detached: call IBM", op: replace, phase: detach, want: StepCallIBM, wantPhase: waitIBM, wantReason: ReasonReplacingWorker},
		{name: "detach timed out: call IBM", op: scaleDown, phase: detach, modify: func(o *DisruptionObservation) { o.Drain.VolumesDetached = false; o.Drain.DetachTimedOut = true }, want: StepCallIBM, wantPhase: waitIBM, wantReason: ReasonResizing},
		{name: "detach excluded: call IBM", op: deletion, phase: detach, modify: func(o *DisruptionObservation) { o.Drain.VolumesDetached = false; o.Drain.DetachExcluded = true }, want: StepCallIBM, wantPhase: waitIBM, wantReason: ReasonRemovingWorkerPool},
		{name: "volume check without a connection holds", op: replace, phase: detach, modify: func(o *DisruptionObservation) { o.WorkloadClusterConnected = false; o.Drain.TimedOut = true }, want: StepHold, wantPhase: detach, wantReason: ReasonWaitingForWorkloadCluster, wantDrain: ReasonWaitingForWorkloadCluster},
		{name: "pods returned to a drained Node: drain again", op: replace, phase: detach, modify: func(o *DisruptionObservation) { o.Drain.Done = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "drain re-check without a connection holds", op: replace, phase: detach, modify: func(o *DisruptionObservation) { o.WorkloadClusterConnected = false }, want: StepHold, wantPhase: detach, wantReason: ReasonWaitingForWorkloadCluster, wantDrain: ReasonWaitingForWorkloadCluster},
		{name: "pre-call gate fails: hold cordoned", op: replace, phase: detach, modify: func(o *DisruptionObservation) {
			o.PreCallGate = DisruptionCheckResult{Reason: ReasonWaitingForSettledCluster, Message: "worker x is provisioning"}
		}, want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForSettledCluster, wantMessage: "the drained Nodes stay cordoned"},
		{name: "an unevaluated pre-call gate fails closed", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) { o.PreCallGate = DisruptionCheckResult{} }, want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForSettledCluster},
		{name: "IBM already removes the victim after a call attempt: skip the call", op: replace, phase: detach, modify: func(o *DisruptionObservation) {
			o.IBM.Removing = true
			o.IBMRequestTime = now.Add(-10 * time.Minute)
		}, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},
		{name: "first call from WaitingForIBM", op: replace, phase: waitIBM, want: StepCallIBM, wantPhase: waitIBM, wantReason: ReasonReplacingWorker},
		{name: "restart after the call: IBM shows the removal, no second call", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) { o.IBM.Removing = true; o.IBMRequestTime = now.Add(-time.Minute) }, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},
		{name: "never repeat the call within DisruptionSettleDelay", op: scaleDown, phase: waitIBM, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-time.Minute) }, want: StepWaitForIBM, wantPhase: waitIBM, wantReason: ReasonResizing, wantMessage: "without a second call before 2026-11-16T12:02:00Z"},
		{name: "IBM shows nothing after DisruptionSettleDelay: call again", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-DisruptionSettleDelay) }, want: StepCallIBM, wantPhase: waitIBM, wantReason: ReasonReplacingWorker},
		{name: "a retry re-checks the drain", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-time.Hour); o.Drain.Done = false }, want: StepDrain, wantPhase: draining, wantReason: ReasonDrainingNode, wantDrain: ReasonDrainingNode},
		{name: "trigger gone before the call aborts", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) { o.TriggerActive = false }, want: StepAbort, wantPhase: waitIBM, wantRelease: true},
		{name: "trigger gone after the call: no second call, no uncordon yet", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) {
			o.TriggerActive = false
			o.IBMRequestTime = now.Add(-10 * time.Minute)
		}, want: StepWaitForIBM, wantPhase: waitIBM, wantReason: ReasonReplacingWorker, wantMessage: "no second call is made"},
		{name: "trigger gone after a call IBM refused: abort at once", op: scaleDown, phase: waitIBM, modify: func(o *DisruptionObservation) {
			o.TriggerActive = false
			o.IBMRequestTime = now.Add(-time.Minute)
			o.LastCallRefused = true
		}, want: StepAbort, wantPhase: waitIBM, wantRelease: true, wantMessage: "after IBM refused the call of 2026-11-16T11:59:00Z"},
		{name: "a refused call while the trigger holds is retried after DisruptionSettleDelay, not aborted", op: scaleDown, phase: waitIBM,
			modify: func(o *DisruptionObservation) {
				o.IBMRequestTime = now.Add(-DisruptionSettleDelay)
				o.LastCallRefused = true
			}, want: StepCallIBM, wantPhase: waitIBM, wantReason: ReasonResizing},
		{name: "trigger gone after a refused call that IBM carries out anyway: follow IBM", op: scaleDown, phase: waitIBM,
			modify: func(o *DisruptionObservation) {
				o.TriggerActive = false
				o.IBMRequestTime = now.Add(-time.Minute)
				o.LastCallRefused = true
				o.IBM.Removing = true
			}, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonResizing},
		{name: "trigger gone and IBM ignored the call for WorkerStuckAfter: abort", op: scaleDown, phase: waitIBM, modify: func(o *DisruptionObservation) {
			o.TriggerActive = false
			o.IBMRequestTime = now.Add(-WorkerStuckAfter)
		}, want: StepAbort, wantPhase: waitIBM, wantRelease: true},
		{name: "trigger gone after the call but IBM removes the victim: follow IBM", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) {
			o.TriggerActive = false
			o.IBM.Removing = true
			o.IBMRequestTime = now.Add(-time.Minute)
		}, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},

		// Victims IBM removes before CAPIBM's call (workers that vanished out of band).
		{name: "IBM removes the victim out of band while it drains: no call, hold cordoned", op: replace, phase: draining, modify: func(o *DisruptionObservation) {
			o.IBM.Removing = true
			o.Drain.Done = false
		}, want: StepHold, wantPhase: draining, wantReason: ReasonReplacingWorker, wantMessage: "without a CAPIBM call"},
		{name: "victim gone out of band once the trigger went away: abort and release the token", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) {
			o.TriggerActive = false
			o.IBM.Removing = true
			o.IBM.Removed = true
		}, want: StepAbort, wantPhase: waitIBM, wantRelease: true, wantMessage: "releasing the disruption token"},
		{name: "victim gone out of band while still due: abort, no replacement wait", op: replace, phase: detach, modify: func(o *DisruptionObservation) {
			o.IBM.Removing = true
			o.IBM.Removed = true
		}, want: StepAbort, wantPhase: detach, wantRelease: true},
		{name: "scale-down victims gone out of band: abort", op: scaleDown, phase: draining, modify: func(o *DisruptionObservation) {
			o.IBM.Removing = true
			o.IBM.Removed = true
		}, want: StepAbort, wantPhase: draining, wantRelease: true},
		{name: "victim gone after CAPIBM's call: follow IBM", op: replace, phase: waitIBM, modify: func(o *DisruptionObservation) {
			o.IBM.Removing = true
			o.IBM.Removed = true
			o.IBMRequestTime = now.Add(-time.Minute)
		}, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},
		{name: "a pool removed out of band before the call: follow IBM", op: deletion, phase: waitIBM, modify: func(o *DisruptionObservation) { o.IBM.Removing = true }, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonRemovingWorkerPool},

		// WaitingForRemoval.
		{name: "IBM still removes the worker", op: replace, phase: removal, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-2 * time.Minute) }, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},
		{name: "IBM still removes the pool", op: deletion, phase: removal, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-2 * time.Minute) }, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonRemovingWorkerPool},
		{name: "removal takes longer than WorkerStuckAfter", op: replace, phase: removal, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-WorkerStuckAfter - time.Minute) }, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonWorkerOperationStuck},
		{name: "after the call the trigger no longer matters", op: replace, phase: removal, modify: func(o *DisruptionObservation) { o.TriggerActive = false; o.IBMRequestTime = now.Add(-2 * time.Minute) }, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},
		{name: "replaced worker deleted: wait for the replacement", op: replace, phase: removal, modify: func(o *DisruptionObservation) { o.IBM.Removed = true }, want: StepWaitForReplacement, wantPhase: replacement, wantReason: ReasonWaitingForReplacement},
		{name: "scale-down victims deleted: complete", op: scaleDown, phase: removal, modify: func(o *DisruptionObservation) { o.IBM.Removed = true }, want: StepComplete, wantRelease: true},
		{name: "pool gone: complete", op: deletion, phase: removal, modify: func(o *DisruptionObservation) { o.IBM.Removed = true }, want: StepComplete, wantRelease: true},
		{name: "pool deletion while IBM removes a replaced worker restarts at Draining", op: replace, phase: removal, modify: func(o *DisruptionObservation) { o.PoolDeleting = true }, want: StepHandOver, wantPhase: draining},
		{name: "scale-down: IBM removes other workers, correct the victims", op: scaleDown, phase: removal, modify: func(o *DisruptionObservation) {
			o.IBMRequestTime = now.Add(-2 * time.Minute)
			o.IBM.UnexpectedVictims = true
		}, want: StepCorrectVictims, wantPhase: removal, wantReason: ReasonResizing, wantMessage: "the spared ones are uncordoned"},
		{name: "scale-down with corrected victims completes once they are deleted", op: scaleDown, phase: removal, modify: func(o *DisruptionObservation) {
			o.IBMRequestTime = now.Add(-5 * time.Minute)
			o.IBM.Removed = true
		}, want: StepComplete, wantRelease: true},
		{name: "unexpected victims do not apply to a replace", op: replace, phase: removal, modify: func(o *DisruptionObservation) {
			o.IBMRequestTime = now.Add(-2 * time.Minute)
			o.IBM.UnexpectedVictims = true
		}, want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker},

		// WaitingForReplacement.
		{name: "replacement not there yet", op: replace, phase: replacement, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-5 * time.Minute) }, want: StepWaitForReplacement, wantPhase: replacement, wantReason: ReasonWaitingForReplacement},
		{name: "no replacement after ReplacementNotCreatedAfter", op: replace, phase: replacement, modify: func(o *DisruptionObservation) { o.IBMRequestTime = now.Add(-ReplacementNotCreatedAfter) }, want: StepWaitForReplacement, wantPhase: replacement, wantReason: ReasonReplacementNotCreated, wantMessage: "ibmcloud ks worker-pool rebalance"},
		{name: "replacement created but not ready after 30 minutes", op: replace, phase: replacement, modify: func(o *DisruptionObservation) {
			o.IBMRequestTime = now.Add(-time.Hour)
			o.IBM.ReplacementCreated = true
		}, want: StepWaitForReplacement, wantPhase: replacement, wantReason: ReasonWaitingForReplacement},
		{name: "replacement ready for minReadySeconds completes", op: replace, phase: replacement, modify: func(o *DisruptionObservation) {
			o.IBM.ReplacementCreated = true
			o.IBM.ReplacementReady = true
		}, want: StepComplete, wantRelease: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			o := passingObservation(tc.op, tc.phase)
			if tc.modify != nil {
				tc.modify(&o)
			}
			got, err := DecideDisruptionStep(o)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Step).To(Equal(tc.want), got.Message)
			g.Expect(got.Phase).To(Equal(tc.wantPhase))
			g.Expect(got.Reason).To(Equal(tc.wantReason))
			g.Expect(got.DrainingReason).To(Equal(tc.wantDrain))
			g.Expect(got.ReleaseToken).To(Equal(tc.wantRelease))
			g.Expect(got.CallsIBM()).To(Equal(tc.want == StepCallIBM))
			if tc.wantMessage != "" {
				g.Expect(got.Message).To(ContainSubstring(tc.wantMessage))
			}
		})
	}
}

func TestDecideDisruptionStepValidation(t *testing.T) {
	testCases := []struct {
		name    string
		modify  func(o *DisruptionObservation)
		wantErr string
	}{
		{name: "unknown operation", modify: func(o *DisruptionObservation) { o.Operation = "Reboot" }, wantErr: `DisruptionObservation.Operation is "Reboot"`},
		{name: "unknown phase", modify: func(o *DisruptionObservation) { o.Phase = "Sleeping" }, wantErr: `DisruptionObservation.Phase is "Sleeping"`},
		{name: "replacement phase of a scale-down", modify: func(o *DisruptionObservation) {
			o.Operation = infrav1.WorkerDisruptionOperationScaleDown
			o.Phase = infrav1.WorkerDisruptionPhaseWaitingForReplacement
		}, wantErr: "only a Replace waits for a replacement"},
		{name: "unknown token state", modify: func(o *DisruptionObservation) { o.Token = "Maybe" }, wantErr: `DisruptionObservation.Token is "Maybe"`},
		{name: "no clock reading", modify: func(o *DisruptionObservation) { o.Now = time.Time{} }, wantErr: "DisruptionObservation.Now is not set"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			o := passingObservation(infrav1.WorkerDisruptionOperationReplace, infrav1.WorkerDisruptionPhaseDraining)
			tc.modify(&o)
			_, err := DecideDisruptionStep(o)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
		})
	}
}

// TestDecideDisruptionStepFirstDrainMessage locks the message of the step that starts a drain (golden texts): it says
// what happens to the victim Nodes, never "cordon" under exclude-node-draining, and it names the workers
// CheckDisruption let pass as stuck.
func TestDecideDisruptionStepFirstDrainMessage(t *testing.T) {
	const stuckWorker = "kube-c1-pool-b-00000005"
	stuckNote := "WorkerOperationStuck: IBM has been changing worker " + stuckWorker + " for more than 60 minutes; " +
		"treated like a failed worker, it no longer blocks worker removals"
	// stuckGate is CheckDisruption's answer for a pool deletion while a worker of another pool has been provisioning for
	// longer than WorkerStuckAfter, as the pool reconciler's first-seen map reports it.
	in := replaceCheckInput()
	in.Operation = infrav1.WorkerDisruptionOperationDeletePool
	in.MachinePoolName = ""
	in.Workers = append(in.Workers, newDisruptionWorker(stuckWorker, "c1-bbbbbbb", "provision_pending", ""))
	in.TransitionalSince = map[string]time.Time{stuckWorker: disruptionNow().Add(-WorkerStuckAfter - time.Minute)}
	stuckGate, err := CheckDisruption(in)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	NewWithT(t).Expect(stuckGate.Allowed).To(BeTrue(), stuckGate.Message)
	testCases := []struct {
		name        string
		modify      func(o *DisruptionObservation)
		wantMessage string
	}{
		{name: "the drain cordons and evicts", modify: func(*DisruptionObservation) {},
			wantMessage: "cordoning every victim Node, then evicting their pods"},
		{name: "an excluded drain neither cordons nor evicts", modify: func(o *DisruptionObservation) {
			o.Drain.Excluded = true
		}, wantMessage: "the drain is excluded (machine.cluster.x-k8s.io/exclude-node-draining): CAPIBM leaves the victim " +
			"Nodes schedulable and evicts no pod"},
		{name: "a stuck worker that no longer blocks is named", modify: func(o *DisruptionObservation) {
			o.Gates.Disruption = stuckGate
		}, wantMessage: "cordoning every victim Node, then evicting their pods; " + stuckNote},
		{name: "an excluded drain names the stuck worker too", modify: func(o *DisruptionObservation) {
			o.Drain.Excluded = true
			o.Gates.Disruption = stuckGate
		}, wantMessage: "the drain is excluded (machine.cluster.x-k8s.io/exclude-node-draining): CAPIBM leaves the victim " +
			"Nodes schedulable and evicts no pod; " + stuckNote},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			o := passingObservation(infrav1.WorkerDisruptionOperationDeletePool, "")
			tc.modify(&o)
			got, err := DecideDisruptionStep(o)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Step).To(Equal(StepDrain))
			g.Expect(got.Reason).To(Equal(ReasonDrainingNodes))
			g.Expect(got.Message).To(Equal(tc.wantMessage))
			if o.Drain.Excluded {
				g.Expect(got.Message).NotTo(ContainSubstring("cordon"), "the drain is excluded: nothing is cordoned")
			}
		})
	}
}

// TestDecideDisruptionStepHandOver follows a replace or scale-down that a pool deletion interrupts, from every phase,
// through the decisions of the pool deletion as the caller records them (StepHandOver): it drains every Node of the
// pool, holds while IBM still carries out the earlier call, and only then removes the pool.
func TestDecideDisruptionStepHandOver(t *testing.T) {
	now := disruptionNow()
	afterCall := now.Add(-10 * time.Minute)
	testCases := []struct {
		name           string
		op             infrav1.WorkerDisruptionOperation
		phase          infrav1.WorkerDisruptionPhase
		ibmRequestTime time.Time
	}{
		{name: "replace while draining", op: infrav1.WorkerDisruptionOperationReplace, phase: infrav1.WorkerDisruptionPhaseDraining},
		{name: "replace while volumes detach", op: infrav1.WorkerDisruptionOperationReplace, phase: infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach},
		{name: "replace before its IBM call", op: infrav1.WorkerDisruptionOperationReplace, phase: infrav1.WorkerDisruptionPhaseWaitingForIBM},
		{name: "replace after its IBM call", op: infrav1.WorkerDisruptionOperationReplace, phase: infrav1.WorkerDisruptionPhaseWaitingForIBM, ibmRequestTime: afterCall},
		{name: "replace while IBM removes the worker", op: infrav1.WorkerDisruptionOperationReplace, phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval, ibmRequestTime: afterCall},
		{name: "replace while waiting for the replacement", op: infrav1.WorkerDisruptionOperationReplace, phase: infrav1.WorkerDisruptionPhaseWaitingForReplacement, ibmRequestTime: afterCall},
		{name: "scale-down while draining", op: infrav1.WorkerDisruptionOperationScaleDown, phase: infrav1.WorkerDisruptionPhaseDraining},
		{name: "scale-down while IBM resizes", op: infrav1.WorkerDisruptionOperationScaleDown, phase: infrav1.WorkerDisruptionPhaseWaitingForRemoval, ibmRequestTime: afterCall},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			o := passingObservation(tc.op, tc.phase)
			o.PoolDeleting = true
			o.IBMRequestTime = tc.ibmRequestTime
			handOver, err := DecideDisruptionStep(o)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(handOver.Step).To(Equal(StepHandOver))
			g.Expect(handOver.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
			g.Expect(handOver.ReleaseToken).To(BeFalse(), "the token stays with the pool deletion")

			// The caller records operation DeletePool, the decided phase and every worker of the pool, keeping
			// ibmRequestTime. The other Nodes of the pool still have pods.
			next := passingObservation(infrav1.WorkerDisruptionOperationDeletePool, handOver.Phase)
			next.PoolDeleting = true
			next.IBMRequestTime = tc.ibmRequestTime
			next.Drain.Done = false
			d, err := DecideDisruptionStep(next)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(d.Step).To(Equal(StepDrain), d.Message)
			g.Expect(d.Phase).To(Equal(infrav1.WorkerDisruptionPhaseDraining))
			g.Expect(d.Reason).To(Equal(ReasonDrainingNodes))

			// Every Node is drained and detached, but IBM still carries out the earlier call: hold cordoned.
			next.Phase = infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach
			next.Drain.Done = true
			next.PreCallGate = DisruptionCheckResult{Reason: ReasonWaitingForSettledCluster, Message: "worker x is deleting"}
			d, err = DecideDisruptionStep(next)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(d.Step).To(Equal(StepHold), d.Message)
			g.Expect(d.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForIBM))

			// IBM settled: the pool deletion makes its own call.
			next.Phase = d.Phase
			next.PreCallGate = DisruptionCheckResult{Allowed: true}
			d, err = DecideDisruptionStep(next)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(d.Step).To(Equal(StepCallIBM), d.Message)
			g.Expect(d.Reason).To(Equal(ReasonRemovingWorkerPool))
		})
	}
}

// TestDecideDisruptionStepSafetyProperties checks the safety properties of DecideDisruptionStep over generated
// observations: the IBM call is never decided while evictable pods may remain, unless the drain timed out or is
// excluded; the call and every cordon need the token; a replace starts only while every other worker of its pool is
// available; the call is never repeated within DisruptionSettleDelay; a removal is aborted only before the IBM call or
// after IBM ignored it for WorkerStuckAfter; a Cluster deletion stops everything; a hand-over restarts at Draining; and
// every recorded phase is valid for the next decision. The generator is seeded, so a failure reproduces.
func TestDecideDisruptionStepSafetyProperties(t *testing.T) {
	g := NewWithT(t)
	// A seeded generator, so that a failing input reproduces; nothing here needs unpredictable numbers.
	rng := rand.New(rand.NewPCG(20261116, 9)) //nolint:gosec // G404: reproducible test inputs, not security relevant
	operations := []infrav1.WorkerDisruptionOperation{infrav1.WorkerDisruptionOperationReplace,
		infrav1.WorkerDisruptionOperationScaleDown, infrav1.WorkerDisruptionOperationDeletePool}
	phases := []infrav1.WorkerDisruptionPhase{"", infrav1.WorkerDisruptionPhaseDraining,
		infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, infrav1.WorkerDisruptionPhaseWaitingForIBM,
		infrav1.WorkerDisruptionPhaseWaitingForRemoval, infrav1.WorkerDisruptionPhaseWaitingForReplacement}
	tokens := []TokenState{"", TokenNotHeld, TokenHeld, TokenHeldByOther}
	requestAges := []time.Duration{-1, time.Minute, DisruptionSettleDelay, 10 * time.Minute, WorkerStuckAfter, 2 * time.Hour}
	coin := func() bool { return rng.IntN(2) == 0 }
	check := func() DisruptionCheckResult {
		if coin() {
			return DisruptionCheckResult{Allowed: true}
		}
		return DisruptionCheckResult{Reason: ReasonWaitingForSettledCluster}
	}
	now := disruptionNow()
	steps := map[DisruptionStep]int{}
	for range 50000 {
		o := DisruptionObservation{
			Operation: operations[rng.IntN(len(operations))], Phase: phases[rng.IntN(len(phases))],
			ResumeFromNodeMarkers: coin(), Now: now, LastCallRefused: rng.IntN(4) == 0, ClusterDeleting: rng.IntN(8) == 0,
			PoolDeleting: rng.IntN(4) == 0, TriggerActive: coin(), Token: tokens[rng.IntN(len(tokens))],
			WorkloadClusterConnected: coin(),
			Gates: DisruptionGates{ClusterStateAllows: coin(), MasterUpdating: coin(), Disruption: check(),
				Autoscaled: coin(), AutobalanceEnabled: coin(), OthersAvailable: coin()},
			Drain: DrainProgress{Excluded: rng.IntN(4) == 0, TimedOut: rng.IntN(4) == 0, Done: coin(), Blocked: coin(),
				DetachExcluded: rng.IntN(4) == 0, DetachTimedOut: rng.IntN(4) == 0, VolumesDetached: coin()},
			PreCallGate: check(),
			IBM: RemovalProgress{Removing: coin(), Removed: coin(), UnexpectedVictims: rng.IntN(4) == 0,
				ReplacementCreated: coin(), ReplacementReady: coin()},
		}
		if age := requestAges[rng.IntN(len(requestAges))]; age >= 0 {
			o.IBMRequestTime = now.Add(-age)
		}
		d, err := DecideDisruptionStep(o)
		if err != nil {
			// The generator also builds the one invalid combination: a replacement wait of another operation.
			g.Expect(o.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForReplacement))
			continue
		}
		steps[d.Step]++
		phase := o.Phase
		if phase == "" && o.ResumeFromNodeMarkers {
			phase = infrav1.WorkerDisruptionPhaseDraining
		}

		if o.ClusterDeleting {
			g.Expect(d.Step).To(Equal(StepStop), "%+v", o)
			continue
		}
		if d.CallsIBM() {
			drained := o.Drain.Excluded || o.Drain.TimedOut || (o.WorkloadClusterConnected && o.Drain.Done)
			g.Expect(drained).To(BeTrue(), "IBM call while evictable pods may remain: %+v", o)
			detached := o.Drain.DetachExcluded || o.Drain.DetachTimedOut || (o.WorkloadClusterConnected && o.Drain.VolumesDetached)
			g.Expect(detached).To(BeTrue(), "IBM call before the volumes detached: %+v", o)
			g.Expect(o.Token).To(Equal(TokenHeld), "IBM call without the disruption token: %+v", o)
			g.Expect(o.PreCallGate.Allowed).To(BeTrue(), "IBM call without the pre-call gate: %+v", o)
			g.Expect(o.IBM.Removing).To(BeFalse(), "IBM call for victims IBM already removes: %+v", o)
			g.Expect(o.TriggerActive || o.Operation == infrav1.WorkerDisruptionOperationDeletePool).To(BeTrue(),
				"IBM call for a removal nobody wants: %+v", o)
			g.Expect(o.IBMRequestTime.IsZero() || !now.Before(o.IBMRequestTime.Add(DisruptionSettleDelay))).To(BeTrue(),
				"IBM call repeated within DisruptionSettleDelay: %+v", o)
			g.Expect(phase).To(BeElementOf(infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach, infrav1.WorkerDisruptionPhaseWaitingForIBM),
				"IBM call outside the pre-call phases: %+v", o)
		}
		if d.Step == StepDrain {
			g.Expect(o.Token).To(Equal(TokenHeld), "cordon without the disruption token: %+v", o)
			g.Expect(o.WorkloadClusterConnected).To(BeTrue(), "drain without a workload cluster connection: %+v", o)
			// A replace that starts next to a worker it would wait for forever would hold the token for as long.
			g.Expect(o.Operation != infrav1.WorkerDisruptionOperationReplace || phase != "" || o.Gates.OthersAvailable).To(BeTrue(),
				"a replace starts while another worker of the pool is not available: %+v", o)
		}
		if d.Step == StepAbort {
			g.Expect(o.Operation).NotTo(Equal(infrav1.WorkerDisruptionOperationDeletePool), "%+v", o)
			g.Expect(phase).To(BeElementOf(infrav1.WorkerDisruptionPhaseDraining, infrav1.WorkerDisruptionPhaseWaitingForVolumeDetach,
				infrav1.WorkerDisruptionPhaseWaitingForIBM), "abort after the IBM call: %+v", o)
			// A call IBM refused is one IBM never acts on (LastCallRefused).
			g.Expect(o.IBMRequestTime.IsZero() || o.LastCallRefused || !now.Before(o.IBMRequestTime.Add(WorkerStuckAfter))).To(BeTrue(),
				"abort while IBM may still act on the call: %+v", o)
			// The one abort while IBM removes the victims: they are gone and CAPIBM never called.
			g.Expect(!o.IBM.Removing || (o.IBM.Removed && o.IBMRequestTime.IsZero())).To(BeTrue(),
				"abort while IBM removes the victims: %+v", o)
		}
		if d.Step == StepHandOver {
			g.Expect(o.PoolDeleting).To(BeTrue(), "%+v", o)
			g.Expect(o.Operation).NotTo(Equal(infrav1.WorkerDisruptionOperationDeletePool), "%+v", o)
			g.Expect(d.Phase).To(BeElementOf(infrav1.WorkerDisruptionPhase(""), infrav1.WorkerDisruptionPhaseDraining),
				"hand-over to a later phase than Draining: %+v", o)
		}
		if d.Step == StepCorrectVictims {
			g.Expect(o.Operation).To(Equal(infrav1.WorkerDisruptionOperationScaleDown), "%+v", o)
			g.Expect(d.Phase).To(Equal(infrav1.WorkerDisruptionPhaseWaitingForRemoval), "%+v", o)
		}
		if d.ReleaseToken {
			g.Expect(d.Step).To(BeElementOf(StepNone, StepWait, StepAbort, StepComplete), "token released mid-removal: %+v", o)
		}

		// The phase a decision records is valid for the operation the caller records with it, so the next decision
		// never fails validation (a hand-over records DeletePool).
		next := o
		next.Phase = d.Phase
		next.ResumeFromNodeMarkers = false
		if d.Step == StepHandOver {
			next.Operation = infrav1.WorkerDisruptionOperationDeletePool
		}
		_, err = DecideDisruptionStep(next)
		g.Expect(err).NotTo(HaveOccurred(), "the recorded phase %q is invalid after %s: %+v", d.Phase, d.Step, o)
	}
	// The generator reaches every step, so no property above holds vacuously.
	for _, step := range []DisruptionStep{StepNone, StepWait, StepAcquireToken, StepDrain, StepWaitForVolumeDetach,
		StepHold, StepCallIBM, StepWaitForIBM, StepWaitForRemoval, StepWaitForReplacement, StepComplete, StepAbort,
		StepHandOver, StepCorrectVictims, StepStop} {
		g.Expect(steps).To(HaveKeyWithValue(step, BeNumerically(">", 0)), "no generated observation led to %s", step)
	}
}

// liveAPIError returns a scrubbed live error capture of kubernetesservice/testdata as the *APIError that the service
// builds for op's answer with the given status (kubernetesservice newAPIError).
//
// Parameters:
//   - t: the test.
//   - name: the capture's file name.
//   - op: the operation that received the answer.
//   - status: the HTTP status of the answer.
func liveAPIError(t *testing.T, name string, op kubernetesservice.Operation, status int) *kubernetesservice.APIError {
	t.Helper()
	data, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	var body struct {
		Code        string `json:"code"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("decoding fixture %s: %v", name, err)
	}
	return &kubernetesservice.APIError{Operation: op, StatusCode: status, Code: body.Code, Description: body.Description}
}

// TestDisruptionCallOutcome feeds DisruptionCallOutcome the errors the service returns for each removal call, so every
// answer goes through kubernetesservice.Classify as it does in the controller.
func TestDisruptionCallOutcome(t *testing.T) {
	// IBM's E1437 answers, observed on IBM Cloud for both flavors, carry ClusterMinimumErrorCode.
	for _, name := range []string{"error-E1437-iks-resize.json", "error-E1437-roks-resize.json"} {
		NewWithT(t).Expect(liveAPIError(t, name, kubernetesservice.OpRemoveWorkerPool, http.StatusBadRequest).Code).
			To(Equal(ClusterMinimumErrorCode), name)
	}
	const (
		replaceCall = kubernetesservice.OpReplaceWorker
		removeCall  = kubernetesservice.OpRemoveWorkerPool

		waitIBM = infrav1.WorkerDisruptionPhaseWaitingForIBM
		removal = infrav1.WorkerDisruptionPhaseWaitingForRemoval
	)
	apiError := func(op kubernetesservice.Operation, status int, code, description string) error {
		return &kubernetesservice.APIError{Operation: op, StatusCode: status, Code: code, Description: description}
	}
	autoscaledRefusal := "Forbidden. The request will not be permitted."
	testCases := []struct {
		name             string
		call             kubernetesservice.Operation
		err              error
		autoscaleEnabled bool
		want             DisruptionStep
		wantPhase        infrav1.WorkerDisruptionPhase
		wantReason       DisruptionReason
		wantClass        kubernetesservice.ErrorClass
		wantHandled      bool
	}{
		{name: "204 on replaceWorker", call: replaceCall, err: nil,
			want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassNone, wantHandled: true},
		{name: "404 on replaceWorker: the worker is already gone", call: replaceCall, err: apiError(replaceCall, http.StatusNotFound, "", ""),
			want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassNotFound, wantHandled: true},
		{name: "pool removal accepted", call: removeCall, err: nil,
			want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassNone, wantHandled: true},
		{name: "404 on removeWorkerPool: the pool is already gone", call: removeCall, err: apiError(removeCall, http.StatusNotFound, "", ""),
			want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassNotFound, wantHandled: true},
		{name: "live IKS E1437 on removeWorkerPool holds the Nodes cordoned", call: removeCall,
			err:  liveAPIError(t, "error-E1437-iks-resize.json", removeCall, http.StatusBadRequest),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForClusterMinimum, wantClass: kubernetesservice.ClassClusterMinimum, wantHandled: true},
		{name: "live ROKS E1437 on removeWorkerPool holds the Nodes cordoned", call: removeCall,
			err:  liveAPIError(t, "error-E1437-roks-resize.json", removeCall, http.StatusBadRequest),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForClusterMinimum, wantClass: kubernetesservice.ClassClusterMinimum, wantHandled: true},
		{name: "E1437 on replaceWorker holds too, never a terminal BadRequest", call: replaceCall,
			err:  liveAPIError(t, "error-E1437-iks-resize.json", replaceCall, http.StatusBadRequest),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForClusterMinimum, wantClass: kubernetesservice.ClassClusterMinimum, wantHandled: true},
		{name: "409 on replaceWorker waits for IBM", call: replaceCall, err: apiError(replaceCall, http.StatusConflict, "", "operation in progress"),
			want: StepWaitForIBM, wantPhase: waitIBM, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassOperationInProgress, wantHandled: true},
		{name: "409 on removeWorkerPool waits for IBM", call: removeCall, err: apiError(removeCall, http.StatusConflict, "", ""),
			want: StepWaitForIBM, wantPhase: waitIBM, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassOperationInProgress, wantHandled: true},
		{name: "autoscaled refusal of an autoscaled pool holds", call: removeCall, autoscaleEnabled: true,
			err:  apiError(removeCall, http.StatusForbidden, "", autoscaledRefusal),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonExternallyAutoscaled, wantClass: kubernetesservice.ClassAutoscaled, wantHandled: true},
		{name: "autoscaled refusal of a pool that is not autoscaled is PermissionDenied", call: removeCall,
			err:  apiError(removeCall, http.StatusForbidden, "", autoscaledRefusal),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassPermissionDenied, wantHandled: false},
		{name: "other 403 on replaceWorker is left to the caller", call: replaceCall, autoscaleEnabled: true,
			err:  apiError(replaceCall, http.StatusForbidden, "", "Forbidden"),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassPermissionDenied, wantHandled: false},
		{name: "other 400 on replaceWorker is left to the caller", call: replaceCall, err: apiError(replaceCall, http.StatusBadRequest, "E9999", ""),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassBadRequest, wantHandled: false},
		{name: "5xx is left to the caller", call: removeCall, err: apiError(removeCall, http.StatusServiceUnavailable, "", ""),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassTransient, wantHandled: false},
		{name: "401 is left to the caller", call: replaceCall, err: apiError(replaceCall, http.StatusUnauthorized, "", ""),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassUnauthorized, wantHandled: false},
		{name: "429 is left to the caller", call: removeCall, err: apiError(removeCall, http.StatusTooManyRequests, "", ""),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassRateLimited, wantHandled: false},
		{name: "no answer from IBM is left to the caller", call: replaceCall, err: errors.New("connection reset"),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonReplacingWorker, wantClass: kubernetesservice.ClassTransient, wantHandled: false},
		{name: "a request the service refused to send is left to the caller", call: removeCall,
			err:  fmt.Errorf("%w: RemoveWorkerPool: the pool ID is empty", kubernetesservice.ErrInvalidRequest),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonRemovingWorkerPool, wantClass: kubernetesservice.ClassBadRequest, wantHandled: false},

		// ResizeWorkerPool, the call of a scale-down.
		{name: "resize accepted", call: kubernetesservice.OpResizeWorkerPool, err: nil,
			want: StepWaitForRemoval, wantPhase: removal, wantReason: ReasonResizing, wantClass: kubernetesservice.ClassNone, wantHandled: true},
		{name: "live IKS E1437 on resizeWorkerPool holds the Nodes cordoned", call: kubernetesservice.OpResizeWorkerPool,
			err:  liveAPIError(t, "error-E1437-iks-resize.json", kubernetesservice.OpResizeWorkerPool, http.StatusBadRequest),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForClusterMinimum, wantClass: kubernetesservice.ClassClusterMinimum, wantHandled: true},
		{name: "live ROKS E1437 on resizeWorkerPool holds the Nodes cordoned", call: kubernetesservice.OpResizeWorkerPool,
			err:  liveAPIError(t, "error-E1437-roks-resize.json", kubernetesservice.OpResizeWorkerPool, http.StatusBadRequest),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonWaitingForClusterMinimum, wantClass: kubernetesservice.ClassClusterMinimum, wantHandled: true},
		{name: "409 on resizeWorkerPool waits for IBM", call: kubernetesservice.OpResizeWorkerPool,
			err:  apiError(kubernetesservice.OpResizeWorkerPool, http.StatusConflict, "", ""),
			want: StepWaitForIBM, wantPhase: waitIBM, wantReason: ReasonResizing, wantClass: kubernetesservice.ClassOperationInProgress, wantHandled: true},
		{name: "autoscaled refusal of a resize of an autoscaled pool holds", call: kubernetesservice.OpResizeWorkerPool, autoscaleEnabled: true,
			err:  apiError(kubernetesservice.OpResizeWorkerPool, http.StatusForbidden, "", autoscaledRefusal),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonExternallyAutoscaled, wantClass: kubernetesservice.ClassAutoscaled, wantHandled: true},
		{name: "autoscaled refusal of a resize of a pool that is not autoscaled is PermissionDenied", call: kubernetesservice.OpResizeWorkerPool,
			err:  apiError(kubernetesservice.OpResizeWorkerPool, http.StatusForbidden, "", autoscaledRefusal),
			want: StepHold, wantPhase: waitIBM, wantReason: ReasonResizing, wantClass: kubernetesservice.ClassPermissionDenied, wantHandled: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := DisruptionCallOutcome(tc.call, tc.err, tc.autoscaleEnabled)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Handled).To(Equal(tc.wantHandled))
			g.Expect(got.Class).To(Equal(tc.wantClass))
			g.Expect(got.Decision.Step).To(Equal(tc.want))
			g.Expect(got.Decision.Phase).To(Equal(tc.wantPhase))
			g.Expect(got.Decision.Reason).To(Equal(tc.wantReason))
			g.Expect(got.Decision.ReleaseToken).To(BeFalse(), "the token is kept until the removal settled")
		})
	}
}

func TestDisruptionCallOutcomeValidation(t *testing.T) {
	g := NewWithT(t)
	_, err := DisruptionCallOutcome(kubernetesservice.OpListWorkers, nil, false)
	g.Expect(err).To(MatchError(ContainSubstring(`DisruptionCallOutcome call is "ListWorkers"`)))
	_, err = DisruptionCallOutcome("", nil, false)
	g.Expect(err).To(MatchError(ContainSubstring(`DisruptionCallOutcome call is ""`)))
}
