# GitOps

Managed clusters work with GitOps tools such as Argo CD and Flux. This page lists what belongs in Git, what the
controllers write, the one accident Git cannot undo and how to prevent it, and the sync settings that are unsafe.

## What belongs in Git

Keep the objects a flavor template renders in Git: the `Cluster`, the `IBMCloudManagedCluster`, the
`IBMCloudManagedControlPlane`, and one `MachinePool` plus one `IBMCloudManagedMachinePool` per worker pool.

* **The initial pool is declared in Git** like every other pool, marked `creation: WithCluster`. No controller generates
  a `MachinePool` or an `IBMCloudManagedMachinePool`: the IBM pool that IBM creates together with the cluster is managed
  by the pool object in Git that names it.
* The spec fields you own include `distributionVersion`, `workerUpdates.policy`, `rollout.after`, `creation` and the
  `MachinePool`'s `replicas` and drain settings. If Git sets a value, change it in Git: a `kubectl patch` or
  `kubectl scale` is reverted by the next sync with self-healing. Commit `rollout.after` to Git to roll a pool.
* Do not keep the `<cluster>-kubeconfig` Secret in Git. It is written by the control plane and holds a cluster-admin
  credential ([Kubeconfig and security](./kubeconfig-and-security.md)).

### What the controllers write

The controllers write exactly two spec fields, both required by the Cluster API contract:

* `IBMCloudManagedControlPlane.spec.controlPlaneEndpoint`, once, when the master is ready;
* `IBMCloudManagedMachinePool.spec.providerIDList`, continuously.

Besides status and their finalizers, the only metadata they write on these objects is the transient
`ibmcloudmanaged.controlplane.cluster.x-k8s.io/create-hold-until` annotation on the control plane after a definitive
create refusal, never once an IBM cluster can exist ([Troubleshooting](./troubleshooting.md#the-create-hold-after-a-refusal)).
Leave all of these out of Git. If your tool reports drift on the two spec fields, tell it to ignore them (for example
Argo CD `ignoreDifferences` with the JSON pointers `/spec/controlPlaneEndpoint` and `/spec/providerIDList`).

## Protect pool objects from pruning

A GitOps tool deletes ("prunes") objects that disappear from Git. For worker pools this has one case Git cannot undo:

* If the **last** pool of a Cluster is pruned, CAPIBM stops the pool deletion **before any drain or IBM call**, because
  the remaining workers would be below IBM's cluster minimum: `Deleting=True`, reason `WaitingForClusterMinimum`. The
  workers keep running.
* But the pruned objects stay `Terminating`. Restoring them in Git does not bring them back: a deleted object cannot be
  undeleted, and the tool cannot re-create it while it still exists. The only exits are **adding another pool** that
  holds the minimum (the old pool is then drained and removed, as requested), or **deleting the Cluster**.
* Pruning a pool while other pools hold the minimum is not refused: CAPIBM drains its Nodes and removes the IBM pool and
  its workers.

Pruning either object of a pool deletes the pool (deleting a `MachinePool` deletes its `IBMCloudManagedMachinePool`).
We therefore recommend **per-object prune protection on both objects of every pool**:

```yaml
apiVersion: cluster.x-k8s.io/v1beta2
kind: MachinePool
metadata:
  name: my-iks-default
  annotations:
    argocd.argoproj.io/sync-options: Prune=false     # Argo CD
    kustomize.toolkit.fluxcd.io/prune: disabled      # Flux
---
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: IBMCloudManagedMachinePool
metadata:
  name: my-iks-default
  annotations:
    argocd.argoproj.io/sync-options: Prune=false
    kustomize.toolkit.fluxcd.io/prune: disabled
```

Use the line for your tool; the other one is ignored.

* **Argo CD**: the resource annotation `argocd.argoproj.io/sync-options: Prune=false` makes Argo CD skip pruning the
  resource; the sync status panel shows that pruning was skipped and why, and the application is out of sync while Argo
  CD expects the resource to be pruned
  ([Argo CD "Sync Options", "No Prune Resources"](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/#no-prune-resources),
  stable docs read on 2026-11-16).
* **Flux**: the kustomize-controller skips garbage collection of resources labelled or annotated with
  `kustomize.toolkit.fluxcd.io/prune: disabled`
  ([Flux "Kustomization", section "Prune"](https://fluxcd.io/flux/components/kustomize/kustomizations/#prune), and the
  [Flux FAQ](https://fluxcd.io/flux/faq/), which also shows `kustomize.toolkit.fluxcd.io/prune: enabled` to turn
  garbage collection back on; both read on 2026-11-16, Flux v2.10 docs). The controller's table of apply policies
  writes the value as `Disabled`; the examples use `disabled`, as above.

To remove a protected pool on purpose, first commit the removal of the annotation (Argo CD), or set it to `enabled`
(Flux), and sync; then remove the objects from Git. Alternatively remove them from Git and delete the `MachinePool` by
hand with `kubectl delete machinepool <name>`.

Deleting the tool's own object (an Argo CD `Application` with cascading deletion, a Flux `Kustomization` with pruning
enabled) can delete its resources, including the `Cluster` and therefore the IBM cluster. `Prune=false` is about
pruning during a sync; to keep a resource when the application is deleted, Argo CD has
`argocd.argoproj.io/sync-options: Delete=false` (same Argo CD page). Decide deliberately which objects of a cluster
should survive the deletion of the tool's own object.

## Changing a pool flavor

A pool's flavor and zones are immutable, so a flavor change is a new pool. Do it in **two commits**:

1. **Add** the new pool (a new `MachinePool` and `IBMCloudManagedMachinePool` with a new `poolName`, the new flavor,
   and no `creation` field or `creation: AfterCluster`). Wait until its `WorkerPoolReady` is `True`.
2. **Remove** the old pool (after lifting its prune protection, above). CAPIBM drains its Nodes, so the pods move to
   the new pool, and then removes the IBM pool.

Doing both in one commit makes the removal race the creation: the old pool's Nodes are drained while the new pool has
no ready workers yet, so pods stay `Pending` and the drain waits in `DrainBlocked`, or, if the old pool was the only
one, the removal waits in `WaitingForClusterMinimum`.

The initial pool's `creation: WithCluster` marker has no effect after the IBM cluster exists, so removing the initial
pool this way needs no change to the marker.

## Taking over a hand-made pool

A manifest whose `IBMCloudManagedMachinePool.spec.poolName` names an IBM pool that already exists in the cluster, for
example one created with the IBM Cloud CLI:

* with the **same flavor**, takes it over: the Normal event `WorkerPoolAdopted` says "CAPIBM now sizes IBM worker
  pool <name> (<pool-id>) and removes it when this object is deleted". From then on the `MachinePool`'s `replicas`
  decides its size, and pruning or deleting the objects **removes the IBM pool**. Commit the pool's current size and
  zones.
* with **another flavor**, is `PoolConflict`: CAPIBM publishes nothing, changes nothing and never removes that IBM pool.

## Deleting a cluster

Remove the cluster's objects from Git, or delete the `Cluster`. The validating webhooks reject a deletion of the
`IBMCloudManagedControlPlane` or of the `IBMCloudManagedCluster` **alone** while their `Cluster` exists and is not
being deleted ("delete Cluster <name> instead; ..."), because deleting the control plane deletes the IBM cluster. When a
tool prunes everything at once, the control plane's rejection lasts only until the `Cluster`'s deletion has started.
The `IBMCloudManagedCluster`'s lasts until the control plane has deleted the IBM cluster and is gone ("leave this
deletion to Cluster API: ..."), because the control plane needs it for that. Cluster API then deletes it by itself, so
a later sync finds nothing left to prune. See [Deleting a cluster](./deleting.md).

## Unsafe sync settings

Do **not** use these for managed cluster objects:

* **Argo CD `Replace=true`**, alone or with **`Force=true`**, and **Flux `spec.force: true`** (or `kubectl replace
  --force`). A replace sends the object as it is in Git, without the controller-written `spec.controlPlaneEndpoint`;
  the API server rejects that ("controlPlaneEndpoint cannot be changed or removed once set"). With force enabled the
  tool then falls back to **deleting and re-creating** the object. For the control plane and the
  `IBMCloudManagedCluster` the webhook rejects the delete while the `Cluster` lives; for a pool nothing rejects it, so a
  forced replace drains the pool and removes the IBM pool and its workers before a new pool is created.
* Pruning without per-object protection on pool objects (above).
* Managing the `status` subresource, finalizers or the controller-written fields from Git.

Argo CD's own documentation calls `Replace=true` potentially destructive and `Force=true` a destructive action, and Flux
recommends `spec.force` only temporarily; for these objects they are never needed.
