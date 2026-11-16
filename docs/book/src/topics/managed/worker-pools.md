# Worker pools and drain

Each IBM worker pool is one Cluster API `MachinePool` plus one `IBMCloudManagedMachinePool`. The `MachinePool` owns the
size (`spec.replicas`, the **total** across the pool's zones) and the drain settings; the `IBMCloudManagedMachinePool`
owns the IBM settings. CAPIBM finds the IBM pool by `spec.poolName` only and never touches an IBM pool that no
`IBMCloudManagedMachinePool` names.

## Adding a pool

```yaml
apiVersion: cluster.x-k8s.io/v1beta2
kind: MachinePool
metadata:
  name: my-iks-compute
  namespace: default
spec:
  clusterName: my-iks
  replicas: 3                          # total; a multiple of the zone count (here 1 per zone)
  template:
    spec:
      clusterName: my-iks
      bootstrap:
        dataSecretName: ""             # managed workers need no bootstrap data
      infrastructureRef:
        apiGroup: infrastructure.cluster.x-k8s.io
        kind: IBMCloudManagedMachinePool
        name: my-iks-compute
---
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: IBMCloudManagedMachinePool
metadata:
  name: my-iks-compute
  namespace: default
spec:
  poolName: compute                    # IBM pool name: unique in the Cluster, lowercase, at most 32 characters, immutable
  flavor: bx2.4x16                     # immutable
  zones:                               # zones of the IBMCloudManagedCluster; immutable
  - name: us-south-1
  - name: us-south-2
  - name: us-south-3
  workerUpdates:
    policy: Manual                     # see Upgrades
```

CAPIBM creates the IBM pool (with one subnet per zone, taken from the `IBMCloudManagedCluster`) once the control plane
is initialized, then publishes the workers in `spec.providerIDList` and the count in `status.replicas`.
`status.ready` (the `Ready` column) becomes `true` once the IBM pool exists and stays `true`; the `WorkerPoolReady`
condition reports whether the size is reached and every worker is ready.

Rules checked before any IBM call (each a `WorkerPoolReady=False` reason, see the [reference](./reference.md)):

* `replicas` must be a multiple of the zone count (`ReplicasNotDivisibleByZones`: "replicas=4 is not a multiple of 3
  zones; use 3 or 6"); IBM sizes pools per zone. `replicas: 0` is not supported (`ZeroReplicasNotSupported`).
* Every zone must be a zone of the `IBMCloudManagedCluster` (`UnknownZone`, or `WaitingForInfrastructure` while it is
  being resolved).
* `entitlement` is OpenShift only (`InvalidForDistribution`).
* No other `IBMCloudManagedMachinePool` of the Cluster may use the same `poolName` (`PoolNameConflict` on **every**
  object that claims it; none of them publishes or changes anything until one is renamed or removed).
* `MachinePool.spec.template.spec.version` is ignored (workers follow the master minor), and so is
  `MachinePool.spec.failureDomains` (`spec.zones` is used); the condition message says so.
* IBM's cluster state can block pool changes: `unavailable` and `scaled_down` block every change, `unsupported` blocks
  create, grow and replace ("upgrade the master first"); the reason is `WaitingForClusterState`.

The flavor and the zones cannot change. To change them, add a new pool and then remove the old one (in GitOps, in two
commits; see [GitOps](./gitops.md#changing-a-pool-flavor)).

## Taking over a hand-made IBM pool

An `IBMCloudManagedMachinePool` whose `poolName` matches an IBM pool that already exists in the cluster (for example
one created with the IBM Cloud CLI) takes it over:

* **Same flavor**: CAPIBM adopts it and emits the Normal event `WorkerPoolAdopted` ("CAPIBM now sizes IBM worker pool
  <name> (<pool-id>) and removes it when this object is deleted"). From then on CAPIBM sizes it to
  `MachinePool.spec.replicas` (a smaller value drains and removes workers), rolls its workers and **removes the IBM pool
  when the object is deleted**. Set `replicas` to the pool's current size and list the same zones before you apply.
* **Another flavor**: `WorkerPoolReady=False`, reason `PoolConflict`. CAPIBM treats the pool as not its own: it
  publishes nothing, changes nothing, and deleting the object never removes the IBM pool.

The initial pool (`creation: WithCluster`) is found the same way, without the `WorkerPoolAdopted` event.

## Scaling

Change the total on the `MachinePool`:

```console
kubectl scale machinepool my-iks-compute -n default --replicas=6
```

* **Growing** is immediate: one `resizeWorkerPool` call (`WorkerPoolReady=False`, reason `Resizing`, then
  `Provisioning`).
* **Shrinking** removes workers, so it is a disruption (below). IBM's resize call cannot name the workers to remove, and
  removing single workers would turn off IBM's auto-rebalance, so CAPIBM predicts IBM's victims from IBM's documented
  order (failed and provisioning workers first, then unhealthy ones, then the oldest), drains them, resizes and checks
  the result. If IBM removes a different worker, CAPIBM emits the Warning event `UnexpectedScaleDownVictim` naming
  both sets and uncordons the spared Nodes. Raising `replicas` again before the resize call cancels the scale-down and
  uncordons what CAPIBM cordoned.
* IBM refuses to go below the cluster minimum, **1 worker on IKS and 2 on ROKS** (`E1437`). CAPIBM checks that first
  and reports `WaitingForClusterMinimum`; if IBM still refuses, CAPIBM keeps the Nodes cordoned and retries at most
  once every 3 minutes.
* If IBM reports the pool as autoscaled (the IBM cluster autoscaler add-on), CAPIBM does not resize it
  (`WorkerPoolReady=True`, reason `ExternallyAutoscaled`). Cluster API's autoscaler integration is not supported.

## Removing a pool

Delete the `MachinePool` (Cluster API deletes the `IBMCloudManagedMachinePool` with it):

```console
kubectl delete machinepool my-iks-compute -n default
```

CAPIBM drains every Node of the pool, then calls `removeWorkerPool` and waits until IBM no longer lists the pool
(`Deleting=True` with reasons `DrainingNodes`, then `RemovingWorkerPool`). Pods move to the other pools; if those have
no room, the pods stay `Pending`, their PodDisruptionBudgets stop allowing evictions and the drain reports
`DrainBlocked`, which asks for capacity instead of breaking the workload. No webhook ever rejects a pool deletion.

* The **last** pool cannot be removed: the remaining workers would be below the cluster minimum. The deletion stops in
  `WaitingForClusterMinimum` **before any drain or IBM call**, and the objects stay `Terminating`. The only ways out are
  adding another pool that holds the minimum, or deleting the Cluster. In GitOps this matters; see
  [GitOps](./gitops.md#protect-pool-objects-from-pruning).
* The initial pool can be removed like any other once other pools hold the minimum.
* A pool deletion takes over a replacement or scale-down of the same pool that is still in flight, keeping its cordons.
* When the whole Cluster is deleted, pools make no IBM call and drain nothing: deleting the IBM cluster removes every
  worker ([Deleting a cluster](./deleting.md)).

## One disruption at a time

Scale-down, pool removal and worker replacement remove workers. Before each, CAPIBM checks, for the whole Cluster:

1. **Settled cluster**: no worker of any pool is provisioning, deploying, reloading or deleting, or has a pending IBM
   operation (`WaitingForSettledCluster`). A worker stuck in such a state for more than 60 minutes no longer blocks
   and is named in the message (`WorkerOperationStuck`). This relief is kept in the controller's memory, so after a
   controller restart a stuck worker blocks again for up to 60 minutes.
2. **Settle delay**: at least 3 minutes since the last disruptive IBM call of any pool of the Cluster, so that IBM's
   lists catch up. A pool that has no status yet (new, or after `clusterctl move`) starts this delay on its first
   observation.
3. **Capacity**: the ready workers that remain must hold the cluster minimum (1 on IKS, 2 on ROKS).

and it must hold the Cluster's **disruption token**: a `coordination.k8s.io` Lease named
`<cluster>-ibm-worker-disruption` in the Cluster's namespace, so that two pools never disrupt at the same time
(`WaitingForDisruptionToken`). An operation that removes no ready worker (for example stopping a stalled scale-up by
scaling back down) skips checks 1 and 2.

## How CAPIBM drains

Cluster API drains Nodes only when it deletes Machines, and these MachinePools have none. IBM's own drain on a worker
replacement is a best effort of about 60 seconds that then stops the worker regardless of PodDisruptionBudgets
(measured on IKS 1.36). CAPIBM therefore drains each worker itself, with Cluster API's Machine drain semantics, before
every IBM call that removes it:

1. **Cordon every Node to remove first**, then evict, so evicted pods never land on another Node that is going away.
   CAPIBM also sets the label `node.kubernetes.io/exclude-from-external-load-balancers=true`, because a cordon alone does
   not take a Node out of cloud load balancer pools; the label asks the VPC load balancer to stop sending traffic to the
   Node. Whether IBM's VPC load balancer honours the label, and how fast, is **not verified** yet.
2. **Evict** pods through the Eviction API, honouring PodDisruptionBudgets and each pod's grace period, every
   20 seconds until done. DaemonSet pods, mirror pods and pods labelled `cluster.x-k8s.io/drain: skip` are left alone;
   pods labelled `cluster.x-k8s.io/drain: wait-completed` are waited for; `emptyDir` and controller-less pods are evicted
   (Cluster API parity). A pod is never force-deleted.
3. **Wait for volumes to detach** from the Node.
4. Only then does CAPIBM call IBM. A worker without a Node counts as drained.

A PodDisruptionBudget that refuses an eviction holds the operation, with the Node cordoned: `Draining=True`, reason
`DrainBlocked`, listing the pods and budgets (a Warning event `DrainBlocked` on entry and every 30 minutes). Without a
time limit this waits as long as the budget requires, as for a MachineDeployment.

### Drain settings

All settings are Cluster API fields and annotations; CAPIBM adds nothing to its own API for them. They are read on every
reconcile, so an edit takes effect during a drain.

| Setting | Where | Default | Effect |
|---|---|---|---|
| Drain time limit | `MachinePool.spec.template.spec.deletion.nodeDrainTimeoutSeconds` | no limit | after it, CAPIBM stops evicting, emits the Warning `DrainTimeoutExceeded` listing the remaining pods, and continues |
| Volume detach time limit | `MachinePool.spec.template.spec.deletion.nodeVolumeDetachTimeoutSeconds` | no limit | the same for the detach wait |
| Replacement soak | `MachinePool.spec.template.spec.minReadySeconds` | 0 | a replacement Node must be `Ready` this long before the next worker is touched |
| Skip drain | annotation `machine.cluster.x-k8s.io/exclude-node-draining` | absent | no cordon, no load balancer label, no eviction |
| Skip volume detach wait | annotation `machine.cluster.x-k8s.io/exclude-wait-for-node-volume-detach` | absent | no wait for volume detach |
| Per pod | Pod label `cluster.x-k8s.io/drain: skip` or `wait-completed` | absent | never evicted, or waited for until completed |

An exclusion annotation counts when present, whatever its value. Put it on the `MachinePool`'s
`spec.template.metadata.annotations` as a steady setting, or on the `IBMCloudManagedMachinePool` as an emergency
override (it can be written even while the `MachinePool` is being deleted). With **both** annotations, a **pool
removal** needs no access to the workload cluster at all; that is the escape hatch for a workload cluster CAPIBM cannot
reach ([Troubleshooting](./troubleshooting.md#a-pool-removal-waits-in-waitingforworkloadcluster)). A scale-down or a
worker replacement still needs the workload cluster, annotations or not.

### What survives restarts and moves

CAPIBM records the drain on each Node with annotations (prefix
`ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/`): `drain-start-time` (time limits never restart),
`drain-owner` (`<namespace>/<pool>`; a second pool waits for it), and `cordoned` and `excluded-from-lb`, which record
what CAPIBM changed. After a controller restart or `clusterctl move`, the drain resumes from these markers and IBM's
worker states. An aborted operation reverts only CAPIBM's own changes: a Node a person cordoned stays cordoned. A paused
Cluster stops evictions, uncordons and IBM calls.

Events: `DrainStarted`, `DrainCompleted`, `NodeUncordoned`, `WorkerReplaceRequested` (Normal); `DrainBlocked`,
`DrainTimeoutExceeded`, `UnexpectedScaleDownVictim` (Warning).

## Things to know

* Labels and taints set on individual Nodes are lost when IBM replaces the worker; pool-level labels and taints are not
  supported yet.
* A replacement worker gets a new worker ID, Node name, IP address and provider ID.
* OpenShift Data Foundation: IBM requires scaling its mon and osd components down before a worker update. CAPIBM's
  drain never forces, so ODF's PodDisruptionBudgets block it safely (`DrainBlocked`); keep `workerUpdates.policy:
  Manual` on ODF pools and follow IBM's procedure. Portworx volumes must be reattached by hand after a replacement.
* ROKS platform components (router, registry, monitoring) bring their own PodDisruptionBudgets, which may allow no
  disruption on very small clusters until a replacement exists. Use the HA topology or add a temporary worker.
