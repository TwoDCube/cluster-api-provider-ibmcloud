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
	"testing"
	"time"

	. "github.com/onsi/gomega"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/services/kubernetesservice"
	"sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/util/ibmversion"
)

// rolloutNow is the fixed clock reading of the rollout tests.
func rolloutNow() time.Time {
	return time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
}

func TestClassifyWorkerUpdate(t *testing.T) {
	now := rolloutNow()
	upToDate := loadWorkers(t, "iks-getWorkers-normal.json")[0] // 1.36.5_1540, target equal (as captured on IBM Cloud)
	minorBehind := versioned(upToDate, "1.35.9_1530", "1.35.9_1530")
	patch := versioned(upToDate, "1.36.5_1540", "1.36.5_1545")
	master := mustMinor(t, "1.36")
	olderNode, newerNode := now.Add(-48*time.Hour), now.Add(-time.Hour)
	passed, future := now.Add(-2*time.Hour), now.Add(time.Hour)
	manual, automatic := infrav1.WorkerUpdatePolicyManual, infrav1.WorkerUpdatePolicyAutomatic

	testCases := []struct {
		name         string
		worker       kubernetesservice.Worker
		policy       infrav1.WorkerUpdatePolicy
		rolloutAfter time.Time
		nodeCreated  time.Time
		want         WorkerUpdateDecision
	}{
		{
			name:   "worker minor below the master's is MinorBehind and due under Manual",
			worker: minorBehind, policy: manual, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateMinorBehind, Due: true, DueBecause: WorkerUpdateMinorBehind},
		},
		{
			name:   "worker minor below the master's is due under Automatic",
			worker: minorBehind, policy: automatic, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateMinorBehind, Due: true, DueBecause: WorkerUpdateMinorBehind},
		},
		{
			name:   "same minor with a newer fix pack under Manual is PatchAvailable, not due",
			worker: patch, policy: manual, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable},
		},
		{
			name:   "same minor with a newer fix pack under Automatic is due",
			worker: patch, policy: automatic, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable, Due: true, DueBecause: WorkerUpdatePatchAvailable},
		},
		{
			name:   "omitted policy is Manual",
			worker: patch, policy: "", nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable},
		},
		{
			name:   "rollout.after in the future is not due",
			worker: upToDate, policy: manual, rolloutAfter: future, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateUpToDate},
		},
		{
			name:   "rollout.after passed with an older Node is RolloutRequested and due",
			worker: upToDate, policy: manual, rolloutAfter: passed, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateRolloutRequested, Due: true, DueBecause: WorkerUpdateRolloutRequested},
		},
		{
			name:   "rollout.after passed with an older Node is due under Automatic too",
			worker: upToDate, policy: automatic, rolloutAfter: passed, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateRolloutRequested, Due: true, DueBecause: WorkerUpdateRolloutRequested},
		},
		{
			name:   "rollout.after passed with a newer Node is not due",
			worker: upToDate, policy: manual, rolloutAfter: passed, nodeCreated: newerNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateUpToDate},
		},
		{
			name:   "worker without a Node is never RolloutRequested",
			worker: upToDate, policy: manual, rolloutAfter: passed, nodeCreated: time.Time{},
			want: WorkerUpdateDecision{Class: WorkerUpdateUpToDate},
		},
		{
			name:   "worker without a Node with a fix pack under Manual is not due",
			worker: patch, policy: manual, rolloutAfter: passed, nodeCreated: time.Time{},
			want: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable},
		},
		{
			// The reported class is the first match, but any class makes the worker due: a passed rollout.after recycles an
			// older Node under Manual too (CAPI MachineDeployment semantics).
			name:   "Manual patch with a passed rollout.after is due",
			worker: patch, policy: manual, rolloutAfter: passed, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable, Due: true, DueBecause: WorkerUpdateRolloutRequested},
		},
		{
			name:   "worker in two classes reports and is due for the first",
			worker: minorBehind, policy: manual, rolloutAfter: passed, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateMinorBehind, Due: true, DueBecause: WorkerUpdateMinorBehind},
		},
		{
			name:   "automatic patch with a passed rollout.after is due for the patch",
			worker: patch, policy: automatic, rolloutAfter: passed, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdatePatchAvailable, Due: true, DueBecause: WorkerUpdatePatchAvailable},
		},
		{
			name:   "rollout.after exactly now has passed",
			worker: upToDate, policy: manual, rolloutAfter: now, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateRolloutRequested, Due: true, DueBecause: WorkerUpdateRolloutRequested},
		},
		{
			name:   "a Node created exactly at rollout.after is not older",
			worker: upToDate, policy: manual, rolloutAfter: passed, nodeCreated: passed,
			want: WorkerUpdateDecision{Class: WorkerUpdateUpToDate},
		},
		{
			name: "a deleted worker is never due",
			worker: func() kubernetesservice.Worker {
				w := minorBehind
				w.ActualState = WorkerStateDeleted
				return w
			}(),
			policy: manual, rolloutAfter: passed, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateUpToDate},
		},
		{
			name:   "an up-to-date worker without rollout.after is not due",
			worker: upToDate, policy: automatic, nodeCreated: olderNode,
			want: WorkerUpdateDecision{Class: WorkerUpdateUpToDate},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := ClassifyWorkerUpdate(tc.worker, master, tc.policy, tc.rolloutAfter, tc.nodeCreated, now)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal(tc.want))
		})
	}
}

func TestClassifyWorkerUpdateRejects(t *testing.T) {
	g := NewWithT(t)
	w := loadWorkers(t, "iks-getWorkers-normal.json")[0]
	_, err := ClassifyWorkerUpdate(w, mustMinor(t, "1.36"), "Sometimes", time.Time{}, time.Time{}, rolloutNow())
	g.Expect(err).To(MatchError(ContainSubstring(`spec.workerUpdates.policy is "Sometimes"; want "Manual" (also when omitted) or "Automatic"`)))
	_, err = ClassifyWorkerUpdate(w, mustMinor(t, "1.36"), infrav1.WorkerUpdatePolicyManual, time.Time{}, time.Time{}, time.Time{})
	g.Expect(err).To(MatchError(ContainSubstring("now is not set")))
}

// rolloutWorker returns a deployed, normal worker of pool "p1" with actual version actual in zone.
func rolloutWorker(id, zone, actual string) kubernetesservice.Worker {
	return kubernetesservice.Worker{ID: id, PoolID: "p1", Zone: zone, ActualState: WorkerStateDeployed, HealthState: "normal",
		KubeVersionActual: actual, KubeVersionTarget: actual}
}

func TestPickReplacement(t *testing.T) {
	now := rolloutNow()
	master := mustMinor(t, "1.36")
	unhealthy := func(w kubernetesservice.Worker) kubernetesservice.Worker { w.HealthState = "critical"; return w }
	withState := func(w kubernetesservice.Worker, state, pending string) kubernetesservice.Worker {
		w.ActualState, w.PendingOperation = state, pending
		return w
	}
	testCases := []struct {
		name       string
		modify     func(in *ReplacementCandidates)
		wantFound  bool
		wantID     string
		wantReason WorkerUpdateClass
	}{
		{
			name: "nothing is due",
			modify: func(in *ReplacementCandidates) {
				in.Workers = []kubernetesservice.Worker{rolloutWorker("w1", "z1", "1.36.5_1540")}
			},
		},
		{
			name: "under Manual a fix pack alone makes no worker due",
			modify: func(in *ReplacementCandidates) {
				in.Workers = []kubernetesservice.Worker{versioned(rolloutWorker("w1", "z1", ""), "1.36.5_1540", "1.36.5_1545")}
			},
		},
		{
			name: "an unhealthy worker goes first, before a MinorBehind one",
			modify: func(in *ReplacementCandidates) {
				in.Policy = infrav1.WorkerUpdatePolicyAutomatic
				in.Workers = []kubernetesservice.Worker{
					rolloutWorker("w1", "z1", "1.35.9_1530"),
					unhealthy(versioned(rolloutWorker("w2", "z2", ""), "1.36.5_1540", "1.36.5_1545")),
				}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdatePatchAvailable,
		},
		{
			// For parseable versions MinorBehind already implies the older version (its minor is below the master's,
			// every other due worker's is not, and compareIBMVersions compares the minor first), so the oldest-version
			// rule would pick w2 here too. The two cases below are the ones only the MinorBehind rule decides.
			name: "MinorBehind goes before a worker with a fix pack",
			modify: func(in *ReplacementCandidates) {
				in.Policy = infrav1.WorkerUpdatePolicyAutomatic
				in.Workers = []kubernetesservice.Worker{
					versioned(rolloutWorker("w1", "z1", ""), "1.36.1_1500", "1.36.5_1545"),
					rolloutWorker("w2", "z2", "1.35.9_1530"),
				}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdateMinorBehind,
		},
		{
			// MinorBehind goes before the other classes: w1 has no kubeVersion.actual, so compareWorkerVersions sorts it as the
			// oldest, and it comes first by zone and ID too; only the MinorBehind rule picks w2.
			name: "MinorBehind goes before a RolloutRequested worker without a version",
			modify: func(in *ReplacementCandidates) {
				in.RolloutAfter = now.Add(-time.Hour)
				in.Workers = []kubernetesservice.Worker{rolloutWorker("w1", "z1", ""), rolloutWorker("w2", "z2", "1.35.9_1530")}
				in.NodeCreated = map[string]time.Time{"w1": now.Add(-48 * time.Hour)}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdateMinorBehind,
		},
		{
			// As above, with a kubeVersion.actual that does not parse, which compareWorkerVersions also sorts first.
			name: "MinorBehind goes before a RolloutRequested worker whose version does not parse",
			modify: func(in *ReplacementCandidates) {
				in.RolloutAfter = now.Add(-time.Hour)
				in.Workers = []kubernetesservice.Worker{rolloutWorker("w1", "z1", "unknown"), rolloutWorker("w2", "z2", "1.35.9_1530")}
				in.NodeCreated = map[string]time.Time{"w1": now.Add(-48 * time.Hour)}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdateMinorBehind,
		},
		{
			name: "then the oldest version",
			modify: func(in *ReplacementCandidates) {
				in.Policy = infrav1.WorkerUpdatePolicyAutomatic
				in.Workers = []kubernetesservice.Worker{
					versioned(rolloutWorker("w1", "z1", ""), "1.36.5_1540", "1.36.5_1545"),
					versioned(rolloutWorker("w2", "z2", ""), "1.36.5_1537", "1.36.5_1545"),
				}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdatePatchAvailable,
		},
		{
			name: "then the zone",
			modify: func(in *ReplacementCandidates) {
				in.Workers = []kubernetesservice.Worker{rolloutWorker("w1", "z2", "1.35.9_1530"), rolloutWorker("w2", "z1", "1.35.9_1530")}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdateMinorBehind,
		},
		{
			name: "then the ID",
			modify: func(in *ReplacementCandidates) {
				in.Workers = []kubernetesservice.Worker{rolloutWorker("w2", "z1", "1.35.9_1530"), rolloutWorker("w1", "z1", "1.35.9_1530")}
			},
			wantFound: true, wantID: "w1", wantReason: WorkerUpdateMinorBehind,
		},
		{
			name: "rollout.after recycles the worker whose Node is older, up to date or not",
			modify: func(in *ReplacementCandidates) {
				in.RolloutAfter = now.Add(-time.Hour)
				in.Workers = []kubernetesservice.Worker{rolloutWorker("w1", "z1", "1.36.5_1540"), rolloutWorker("w2", "z1", "1.36.5_1540")}
				in.NodeCreated = map[string]time.Time{"w1": now.Add(-time.Minute), "w2": now.Add(-48 * time.Hour)}
			},
			wantFound: true, wantID: "w2", wantReason: WorkerUpdateRolloutRequested,
		},
		{
			name: "Manual patch with a passed rollout.after is due and picked",
			modify: func(in *ReplacementCandidates) {
				in.RolloutAfter = now.Add(-time.Hour)
				in.Workers = []kubernetesservice.Worker{versioned(rolloutWorker("w1", "z1", ""), "1.36.5_1540", "1.36.5_1545")}
				in.NodeCreated = map[string]time.Time{"w1": now.Add(-48 * time.Hour)}
			},
			wantFound: true, wantID: "w1", wantReason: WorkerUpdateRolloutRequested,
		},
		{
			name: "workers IBM is removing or changing, deleted workers and other pools are never picked",
			modify: func(in *ReplacementCandidates) {
				other := rolloutWorker("w0", "z1", "1.35.9_1530")
				other.PoolID = "p2"
				in.Workers = []kubernetesservice.Worker{
					other,
					withState(rolloutWorker("w1", "z1", "1.35.9_1530"), WorkerStateDeleting, ""),
					withState(rolloutWorker("w2", "z1", "1.35.9_1530"), WorkerStateDeleted, ""),
					withState(rolloutWorker("w3", "z1", "1.35.9_1530"), WorkerStateDeployed, "reload"),
					rolloutWorker("w4", "z1", "1.35.9_1530"),
				}
			},
			wantFound: true, wantID: "w4", wantReason: WorkerUpdateMinorBehind,
		},
		{
			name:   "no workers",
			modify: func(*ReplacementCandidates) {},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			in := ReplacementCandidates{PoolID: "p1", MasterMinor: master, Policy: infrav1.WorkerUpdatePolicyManual, Now: now}
			tc.modify(&in)
			pick, found, err := PickReplacement(in)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(found).To(Equal(tc.wantFound))
			if !tc.wantFound {
				g.Expect(pick).To(Equal(ReplacementPick{}))
				return
			}
			g.Expect(pick.Worker.ID).To(Equal(tc.wantID))
			g.Expect(pick.Decision.Due).To(BeTrue())
			g.Expect(pick.Decision.DueBecause).To(Equal(tc.wantReason))
		})
	}
}

// TestPickReplacementLiveCapture picks from the scrubbed ROKS capture (recorded on IBM Cloud) after the master moved to
// the next minor: both workers are MinorBehind, healthy and on one version in one zone, so the older ID goes first.
func TestPickReplacementLiveCapture(t *testing.T) {
	g := NewWithT(t)
	workers := loadWorkers(t, "roks-getWorkers-normal.json")
	pick, found, err := PickReplacement(ReplacementCandidates{Workers: workers, PoolID: workers[0].PoolID,
		MasterMinor: ibmversion.Minor{Major: 4, Minor: 22}, Now: rolloutNow()})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(found).To(BeTrue())
	g.Expect(pick.Worker.ID).To(Equal("kube-roksclusterid0000002-exampletest-default-000001ec"))
	g.Expect(pick.Decision).To(Equal(WorkerUpdateDecision{Class: WorkerUpdateMinorBehind, Due: true, DueBecause: WorkerUpdateMinorBehind}))
}

func TestPickReplacementRejects(t *testing.T) {
	testCases := []struct {
		name    string
		in      ReplacementCandidates
		wantErr string
	}{
		{name: "no pool ID", in: ReplacementCandidates{Now: rolloutNow()}, wantErr: "ReplacementCandidates.PoolID is empty"},
		{name: "unknown policy", in: ReplacementCandidates{PoolID: "p1", Policy: "Never", Now: rolloutNow()}, wantErr: `policy is "Never"`},
		{name: "no clock reading", in: ReplacementCandidates{PoolID: "p1"}, wantErr: "now is not set"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			pick, found, err := PickReplacement(tc.in)
			g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
			g.Expect(found).To(BeFalse())
			g.Expect(pick).To(Equal(ReplacementPick{}))
		})
	}
}
