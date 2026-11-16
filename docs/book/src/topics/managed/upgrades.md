# Upgrades and update policy

IBM manages versions differently from self-managed Kubernetes:

* You choose the **minor** of the master (`spec.distributionVersion`). IBM always deploys the newest patch of it and
  applies later **master patches** on its own schedule; CAPIBM never requests them and only reports them.
* **Workers** are linked to the master only by the minor. They follow the master minor automatically. Their **fix
  packs** (IBM worker patch levels, independent of master patches) roll according to the pool's update policy.
* Every worker update is an IBM **replace**: the worker is deleted and a new one is provisioned. CAPIBM drains each
  worker first and replaces one worker at a time per Cluster.

## Upgrading the master minor

Raise `spec.distributionVersion` by exactly **one** minor:

```console
kubectl patch ibmcmcp my-iks-control-plane -n default --type merge -p '{"spec":{"distributionVersion":"1.37"}}'
```

For ROKS, for example `"4.22"` (OpenShift 4.22 is not verified yet). The admission webhook rejects a value below the
running minor or more than one minor above it, when the field changes; the controller checks again, because the master
can move after admission. A minor can never go down: IBM cannot roll a master back.

CAPIBM then:

1. checks that the cluster is in a state that allows the update: no CA rotation in progress (`BlockedByCARotation`), a
   cluster state and master health that allow it (`BlockedByClusterState`; IBM's `unsupported` and `update_canceled`
   states do allow it), IBM offers the minor (`VersionNotAvailable`), and no worker is too far behind
   (`BlockedByWorkerSkew`: workers may lag the target by at most 2 minors on IKS and 1 on ROKS; CAPIBM never forces);
2. records the request in `status.masterUpdate` and sets `MasterVersionUpToDate=False` (`UpgradePending`) and
   `RollingOut=True` (`MasterUpdateInProgress`) **before** it calls IBM, so a failed or lost request is retried with a
   backoff instead of on every poll;
3. sends one `UpdateMaster` request and waits while IBM updates the master (30 to 60 minutes);
4. sets `MasterVersionUpToDate=True` (`UpToDate`) once the master runs the new minor. The workers then follow
   ([below](#workers-follow-the-master-minor)).

`MasterVersionUpToDate` is `True` whenever the master runs the requested minor, whatever patch IBM chose. When IBM's
end of support for the minor is less than 45 days away, the message mentions it; once IBM marks the version
`unsupported` the message says "IBM no longer supports <minor>: upgrade one minor at a time, starting with
spec.distributionVersion <next-minor>". Minors reach their end
of service on IBM's schedule (in November 2026 IBM listed 2027-01-20 for IKS 1.34), so plan regular upgrades; on an
`unsupported` cluster IBM adds and replaces no workers until the master is upgraded.

### When IBM does not take the update

* If the master is still on the old minor some time after the request and IBM shows no update in progress, the reason
  is `UpgradeFailed`: "the master update to <new> that CAPIBM requested at <time> has not taken effect: the master
  still runs <old> and IBM shows no update in progress; CAPIBM requests it again at <retry time>". CAPIBM requests the
  update again with a backoff of 1 hour, then 2, 4, ... hours, at most 24 hours between attempts.
* If IBM refuses the request with HTTP 403, the reason is `PermissionDenied`. The message names the missing Kubernetes
  Service role, the credentials ("the CAPIBM controller identity") and the time of the next attempt.

<aside class="note warning">

<h1>Accepted limitation: a granted role takes effect at the next retry time</h1>

After an `UpdateMaster` 403 (`MasterVersionUpToDate=False`, reason `PermissionDenied`), CAPIBM sends no new request
until the retry time named in the condition message. The retry backs off 1 hour, 2 hours, 4 hours and so on, up to
24 hours, after repeated refusals. A role granted in the meantime therefore takes effect at that retry time, without a
spec change. There is **no manual trigger** to retry earlier.

</aside>

## Workers follow the master minor

When the master minor moves, every worker of every pool that is behind it is replaced automatically, one at a time per
Cluster and drained, under both update policies. `MachinePool.spec.template.spec.version` is ignored (the
`WorkersUpToDate` message says so); IBM chooses the worker version. Until the master update is done, worker replacements
wait (`WaitingForControlPlaneUpgrade`), and the same applies while IBM applies a master patch.

## Worker fix packs and the update policy

IBM publishes worker fix packs independently of master patches (workers ran `1.36.5_1540` while the master ran
`1.36.5_1537`). `IBMCloudManagedMachinePool.spec.workerUpdates.policy` decides what happens when a newer fix pack of
the master minor is available for a worker:

| Policy | Behaviour |
|---|---|
| `Manual` (also when omitted) | Workers are only reported: `WorkersUpToDate=False`, reason `PatchUpdateAvailable`, for example "3 of 6 workers can move to IBM fix pack 1.36.5_1545; set spec.rollout.after to roll them, or set spec.workerUpdates.policy: Automatic". The pool's `Ready` stays `True`. |
| `Automatic` | CAPIBM replaces such workers as soon as IBM reports the newer fix pack, one at a time per Cluster, drained. |

For each worker CAPIBM decides one class; a worker in several classes is reported with the first that applies, but it
is replaced when any of them makes it due:

| Class | When | Replaced |
|---|---|---|
| `MinorBehind` | the worker's minor is below the master minor | always |
| `PatchAvailable` | same minor, and IBM offers a newer fix pack for it | under `Automatic` |
| `RolloutRequested` | `spec.rollout.after` has passed and the worker's Node was created before it | always |
| `UpToDate` | none of the above | never |

When several workers are due, the next one is picked in IBM's own order: unhealthy workers first, then `MinorBehind`
before the others, then the oldest version, then by zone and worker ID.

Whatever the policy, `status.workersWithUpdateAvailable` (the `Updates` column) counts the workers that are behind the
master minor or have a newer fix pack, `status.workerVersions` counts the workers per IBM version, and
`status.targetWorkerVersion` is the newest fix pack IBM offers. Cluster API's `MachinePool.status` shows only kubelet
versions, never IBM fix packs.

## Rolling workers on request: `spec.rollout.after`

`spec.rollout.after` has the Cluster API MachineDeployment semantics: once the time has passed, **every** worker whose
Node was created before it is replaced, one at a time and drained, on the newest fix pack of the master minor, under
both policies and also when it is up to date (for example after a CA rotation). Workers created after that time are
never replaced because of it.

`clusterctl alpha rollout restart` supports only MachineDeployments and KubeadmControlPlanes, so set the field with
`kubectl patch` (or in Git, see [GitOps](./gitops.md)):

```console
kubectl patch ibmcloudmanagedmachinepool <name> --type merge -p '{"spec":{"rollout":{"after":"<RFC 3339 now>"}}}'
```

for example, with the current time filled in by the shell:

```console
kubectl patch ibmcloudmanagedmachinepool my-iks-default -n default --type merge \
  -p '{"spec":{"rollout":{"after":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"}}}'
```

The value is an RFC 3339 time, for example `"2026-11-20T09:00:00Z"`; a time in the future schedules the rollout.

## Capacity: the +1 worker a rollout needs

A replacement first removes the worker (IBM deletes, then creates; there is no surge), so the ready workers that remain
during the replacement must still hold IBM's cluster minimum (1 on IKS, 2 on ROKS):

* An **IKS** cluster needs at least **2** workers to roll a worker; a **ROKS** cluster at least **3**.
* Below that, replacements wait with `WorkersUpToDate=False`, reason `WaitingForCapacity`, and the message names the fix:
  "scale MachinePool <name> to <n+1> to allow a rolling replacement". Scale up by one, let the rollout finish, then
  scale back down.
* A 1-worker IKS cluster can never roll a worker, and after two master minors its next master upgrade is
  `BlockedByWorkerSkew`. A 2-worker ROKS cluster (for example `managed-roks-single-zone`) needs the temporary third
  worker for every rollout.

The HA flavors (3 zones × 2 workers) need no extra worker. Expect a pool of N workers to roll in roughly N × (drain
time + 6 minutes); the next worker starts only after the replacement is deployed, healthy and its Node has been `Ready`
for `minReadySeconds`.

## Progress and blockers

While a rollout runs, `WorkersUpToDate` carries the step (`DrainingNode`, `ReplacingWorker`, `WaitingForReplacement`)
or `RollingOut` between workers, with a message such as "replaced 2 of 6". It waits with the reasons of the
[disruption checks](./worker-pools.md#one-disruption-at-a-time) (`WaitingForSettledCluster`,
`WaitingForDisruptionToken`, `WaitingForClusterState`, `WaitingForWorkloadCluster`), and reports `AutoRebalanceDisabled`
(IBM auto-rebalance is off in the worker's zone, so IBM would create no replacement; fix: `ibmcloud ks worker-pool
rebalance`), `ReplacementNotCreated` (no new worker 30 minutes after the old one was deleted) and
`WorkerOperationStuck`. If a rollout is no longer wanted before CAPIBM's IBM call (policy set back to `Manual`, for
example), CAPIBM uncordons the Node and stops.

## Versions at a glance

| | IKS (`Kubernetes`) | ROKS (`OpenShift`) |
|---|---|---|
| `spec.distributionVersion` | `"1.36"` | `"4.21"` |
| Sent to IBM on create | `1.36` (IBM deploys `1.36.5_1537`) | `4.21_openshift` (IBM deploys `4.21.35_1537_openshift`) |
| Master minor upgrade | raise to `"1.37"` | raise to `"4.22"` |
| Master patches | applied by IBM, reported by CAPIBM | same |
| `status.distributionVersion` (`Version` column) | `1.36.5_1537` | `4.21.35_1537_openshift` |
| `status.targetDistributionVersion` | IBM's target version (equal to the above when nothing is pending) | same |
| `status.distributionVersionEndOfSupport` | IBM's end of service for the minor, when IBM reports one | same |
| `status.versions[0]`, `status.version` (`Kubernetes` column) | `v1.36.5+IKS`, read from the API server | `v1.34.9`, read from the API server; never an OpenShift version |
| Worker versions (pool status) | `1.36.5_1540` | `4.21.40_1540_openshift` |
| `MachinePool.status` versions (Cluster API, from kubelets) | `v1.36.5+IKS` | `v1.34.9` |

While IBM updates the master, `status.versions` keeps the lowest version the API server reported, so it never flips
between old and new API server replicas.

## Why there is no `spec.version`

The control plane has no Cluster API `spec.version`. The contract's version fields mean **Kubernetes** versions, while
IBM lets you choose only a product **minor** (IBM picks the patch), and for ROKS the Kubernetes version follows from the
OpenShift version (OpenShift 4.21.35 runs Kubernetes 1.34.9). The contract makes `spec.version` mandatory only "if
control plane allows direct management of the Kubernetes version in use" and for ClusterClass: ROKS users choose an
OpenShift minor, so ROKS is compliant; IKS users choose the Kubernetes minor but IBM chooses and changes the patch, so
IKS is arguable. The hard consequence is that **ClusterClass is not supported**. The status side of the contract is met:
`status.versions` and `status.version` carry the Kubernetes version the API server actually serves.

How other providers handle managed and OpenShift control planes:

| Provider | `spec.version` | `status.version(s)` | Product version lives in | ClusterClass |
|---|---|---|---|---|
| OpenShift Assisted control plane provider (self-managed OpenShift) | none | `status.version` = the Kubernetes version of the target release image | `spec.distributionVersion`, `status.distributionVersion` | no |
| CAPA ROSA | OpenShift version, for example `4.14.5` | `status.version` = the OpenShift version | the Cluster API fields themselves | no |
| CAPZ ARO HCP (proposed, not in CAPZ main) | none | `status.version` = the OpenShift version | the embedded Azure resource | no |
| CAPA EKS (not OpenShift) | Kubernetes, `MAJOR.MINOR` allowed | `status.version` = the EKS minor unless a patch is pinned | — | yes |
| CAPIBM managed | none | `status.versions[0]` and `status.version` = the Kubernetes version the API server reports | `spec.distributionVersion` (minor), `status.distributionVersion` (IBM's string) | no |

CAPIBM follows the OpenShift Assisted control plane provider (same field names, no `spec.version`, a Kubernetes version
in the contract status), and reports the version the API server serves rather than the target's. ROSA and ARO put
OpenShift versions into fields that Cluster API reads as Kubernetes versions; CAPIBM keeps those fields truthful and puts
IBM's product version into fields that say what they are.

Because Cluster API skips its version-based checks for a control plane without `spec.version`, `clusterctl describe
cluster` shows no version for it; use `kubectl get ibmcmcp` (`Version` and `Kubernetes` columns).
