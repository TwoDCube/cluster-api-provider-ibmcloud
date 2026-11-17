# Troubleshooting

Start with the conditions and events of the object that is stuck; every reason is listed in the
[reference](./reference.md):

```console
kubectl get cluster,ibmcmc,ibmcmcp,machinepool,ibmcmmp -n <namespace>
kubectl describe ibmcmcp <name> -n <namespace>
kubectl get events -n <namespace> --field-selector involvedObject.name=<name>
kubectl logs -n capi-ibmcloud-system deployment/capi-ibmcloud-controller-manager
```

Condition messages name IBM errors by their **code** (for example "IBM code P4102"); IBM's description and incident ID
are in the controller log. Quote both when you open an IBM support case.

## Installation

### Creating a managed object is rejected

"... objects cannot be created while the ManagedKubernetes feature gate is off ...": install or upgrade the provider
with `EXP_MANAGED_KUBERNETES=true` ([Prerequisites](./prerequisites.md#enable-the-feature-gate)).

### ManagedKubernetesDisabled events, nothing is reconciled, deletions hang

The gate was turned off, most often by a `clusterctl upgrade` run without `EXP_MANAGED_KUBERNETES=true`. Run the
upgrade again with the variable set.

## The network check fails (IBMCloudManagedCluster)

Before the IBM cluster exists, `NetworkReady=False` stops everything, and nothing is billed. Later, a reference that
disappears sets `NetworkReady` to `False` but does not affect the running cluster.

| Reason | Fix |
|---|---|
| `ResourceGroupNotFound` | `spec.resourceGroup.id` names no resource group the CAPIBM credentials can see; check the ID and the Viewer role |
| `VPCNotFound` | no VPC with that name or ID in `spec.region` |
| `SubnetNotFound` | a zone's subnet does not exist in the VPC |
| `SubnetNotInVPC` | the subnet belongs to another VPC |
| `SubnetZoneMismatch` | the subnet lies in another zone than the one it is listed for, for example "subnet dev-sn-2 (<subnet-id>) is in zone us-south-1, but IBMCloudManagedCluster/dev lists it for us-south-2" |
| `SubnetNameAmbiguous` | several subnets in the VPC have that name; give the subnet `id` instead |
| `IBMAPIError` | another IBM error; see the code in the message and the controller log |

Region, resource group and VPC cannot change after creation; zones can only be added.

## The IBM cluster is not created

### WaitingForInfrastructure

The control plane waits until the `IBMCloudManagedCluster` has resolved the network (see above).

### WaitingForInitialWorkerPool or InvalidConfiguration about spec.creation

Exactly one `IBMCloudManagedMachinePool` of the Cluster must have `spec.creation: WithCluster` before the IBM cluster is
created:

* `WaitingForInitialWorkerPool`, "No IBMCloudManagedMachinePool of Cluster <name> has spec.creation: WithCluster ...":
  set the marker on the pool IBM should create with the cluster. The control plane continues at once.
* `WaitingForInitialWorkerPool`, "IBMCloudManagedMachinePool <p> is not linked to its MachinePool yet ...": the pool's
  `MachinePool` is missing, or Cluster API has not set the owner reference yet. Check that a `MachinePool` references
  the pool in `spec.template.spec.infrastructureRef`.
* `InvalidConfiguration`, "IBMCloudManagedMachinePools <a>, <b> of Cluster <name> all have spec.creation:
  WithCluster ...": keep the marker on one pool.

None of these makes an IBM call. Once the IBM cluster exists the marker is ignored. See
[Creating a cluster](./creating-a-cluster.md#the-initial-worker-pool-creation-withcluster).

### InvalidConfiguration for other reasons

The message names the field and the fix: an invalid cluster name, pod or service ranges outside IBM's rules, an initial
pool whose replicas are not a multiple of its zones or below the per-zone minimum (1 on IKS, 2 on ROKS), a zone that is
not in the `IBMCloudManagedCluster`, `entitlement` on IKS, or an IBM 400 or 422 answer such as `P4102` (subnet not found)
or `E7278` (ROKS without a Cloud Object Storage instance). CAPIBM checks again every 15 minutes and at once when you
change the spec.

### PermissionDenied on create

* **`A0656`**: "IBM cannot create the infrastructure API key for region <r> and resource group <rg> with the credentials
  of the CAPIBM controller identity (IBM code A0656); ...". The service ID lacks the IAM Identity Service roles, or an
  account administrator has to pre-set the key with `ibmcloud ks api-key reset`
  ([Prerequisites](./prerequisites.md#the-infrastructure-key-of-a-region-and-resource-group)).
* **403**: a missing role ([IAM permissions](./prerequisites.md#iam-permissions)), or the stored infrastructure key of
  the region and resource group belongs to an identity with insufficient permissions. Check whose key it is with
  `ibmcloud ks api-key info --cluster <any-cluster-in-that-region-and-resource-group>`; `ibmcloud ks api-key reset
  --region <region>` re-binds it to the caller.

After a refusal CAPIBM holds further creates ([below](#the-create-hold-after-a-refusal)) and checks again every
15 minutes, so a granted role takes effect within about 15 minutes.

### Check permissions without creating anything

IBM checks whether it can create the infrastructure key for the caller **before** it validates the request. A create
request that names a subnet that does not exist therefore costs nothing and tells you whether the permissions are in
place: `A0656` means the IAM Identity Service roles (or a pre-set key) are missing; `P4102` ("subnet not found") means
that permission is fine. Run it with the **CAPIBM** credentials, in a separate CLI home so your own login is untouched.
The CLI keeps its plug-ins in that home too, so install the Kubernetes Service plug-in (`ks`) into it first:

```console
export IBMCLOUD_HOME=$(mktemp -d)
ibmcloud plugin install container-service -f
ibmcloud login --apikey @<file-with-the-capibm-api-key> -r <region> -g <resource-group>
ibmcloud ks cluster create vpc-gen2 --name permission-check --zone <zone> --vpc-id <existing-vpc-id> \
  --subnet-id <id-that-does-not-exist> --flavor cx2.2x4 --workers 1
rm -rf "$IBMCLOUD_HOME"
```

The command must fail. Read the error code: `A0656` or a 403 means a missing permission, `P4102` means the create
permission is in place. The API behaviour behind the check (a create naming a missing subnet ends with `P4102` and
creates nothing) was observed live; this CLI form of the check, and the `A0656` answer to it, are **not verified** yet.

### The create hold after a refusal

After a definitive create refusal (`A0656`, 400, 403, 422, or `E0007` "name exists" when no such cluster can be found)
the control plane carries the controller-owned annotation
`ibmcloudmanaged.controlplane.cluster.x-k8s.io/create-hold-until` with an RFC 3339 time. Before that time CAPIBM sends no
new create request, makes no IBM call for that control plane and writes nothing:

* the hold is **12 minutes** after a refusal and **4 minutes** after a 401, so the next check (about 15 minutes after a
  refusal, 5 minutes after a 401) sends exactly one new request;
* a **spec change ends the hold at once**, so fix the cause in the spec, or grant the missing role and wait for the
  next check;
* the hold exists only while no IBM cluster can exist; the next create attempt and any binding remove it, and it never
  appears on an initialized or moved control plane;
* do **not** edit it: CAPIBM ignores a value more than 12 minutes ahead, an unparseable value, and any value once the
  refusal's condition is gone, so an edit shifts the next create by at most 12 minutes.

The hold exists because IBM's API limit is shared by every cluster of the manager: without it, a refusal would be
repeated within milliseconds.

### ClusterNameConflict

"IBM Cloud cluster <name> (ID <id>, created <date>) already exists and CAPIBM cannot prove it created it for this
Cluster ...". IBM names are unique per account, so another namespace or management cluster (or a person) uses the name.
CAPIBM created nothing and never touches that cluster.

* If it **does not** belong to this Cluster: delete this Cluster and create it again with another
  `spec.ibmCloudClusterName` (the field cannot be changed in place). Do not delete the other cluster.
* If it **does** belong to this Cluster (for example objects re-applied from Git after the old management cluster was
  lost, or restored from a backup), follow
  [Recovering ownership](./clusterctl-move.md#recovering-ownership) on the `clusterctl move` page, as the condition
  message says.

### The binding is refused

| Reason | Meaning | Fix |
|---|---|---|
| `BindingMismatch` | `status.clusterID` names an IBM cluster whose name, resource group, distribution or provider differ from this control plane | nothing is changed; check whether the spec or the IBM cluster was edited |
| `OwnershipNotProven` | the Secret's `cluster-id` names a cluster, but its credential does not pass the cluster-admin check there | [Recovering from OwnershipNotProven](./clusterctl-move.md#recovering-from-ownershipnotproven) |
| `IBMClusterNotFound` | a known cluster ID or the endpoint exists, but IBM returns 404 | CAPIBM never re-creates a cluster that existed; if the cluster was deleted, delete the Cluster |
| `DeletedOutOfBand` | IBM shows the cluster as deleting or deleted while the Cluster is not being deleted | delete the Cluster |
| `CreateFailed` | IBM reports `create_failed` or `deploy_failed` | delete this Cluster and create it again; CAPIBM never deletes the failed cluster on its own |

### VersionNotAvailable or IBMAPIError

`VersionNotAvailable` lists the minors IBM offers ("IBM Cloud does not offer Kubernetes 1.33 (offered: ...)"); pick one.
`IBMAPIError` names the operation, the HTTP status and IBM's code; the controller log has IBM's description.

## The control plane is not available

| Condition and reason | Meaning |
|---|---|
| `KubeconfigReady=False` `WaitingForMaster` | normal for 16 to 25 minutes after the create |
| `KubeconfigReady=False` `EndpointUnreachable` | "The management cluster cannot reach <url> (...); private-only clusters need private connectivity": check firewalls, CBR rules on the cluster API, and private connectivity for private-only clusters |
| `KubeconfigReady=False` `CredentialFetchFailed` | IBM's admin kubeconfig or CA could not be fetched (often the Administrator role is missing; the message carries IBM's code only, and CAPIBM tries again after 5 minutes for a 401 and 15 minutes for a 403 or 400), or it held no client certificate |
| `KubeconfigReady=False` `VerificationFailed` | a new credential failed the check against the master (the previous Secret is kept), the stored credential is rejected with 401 (CAPIBM fetches IBM's admin credential again with a backoff of 1 to 10 minutes), or the master's certificate no longer verifies with the stored trust (both trust modes are tried again at most once every 10 minutes) |
| `KubeconfigReady=False` `UserProvidedSecretInvalid` | your Secret lacks the `cluster.x-k8s.io/cluster-name` label or a non-empty `value` key |
| `KubeconfigReady=Unknown` `CARotationInProgress` | IBM rotates the cluster CA; CAPIBM fetches the new credential when it is done |
| `Available=False` `MasterNotReady` | IBM does not report the master ready, or reports its health as `error` or `unavailable` |
| `Available=False` `WorkloadAPIUnreachable` | the stored kubeconfig's probe fails (network, TLS, or a 401 while the credential is refreshed) |

While the master provisions, Cluster API's MachinePool controller logs ClusterCache connection errors. That is
expected for the first 16 to 25 minutes.

**IBM shows the ingress as `warning` or `critical`.** Expected without a public gateway and with fewer than 2 workers
per zone ([Topology](./topology.md#smaller-topologies-are-not-ha)). CAPIBM ignores the ingress status.

## The master upgrade does not happen

* **`PermissionDenied`** after the upgrade request: IBM refused `UpdateMaster` with 403; the message names the missing
  Kubernetes Service role and when CAPIBM asks again. **Accepted limitation**: a role granted meanwhile takes effect
  only at that retry time, which backs off 1, 2, 4, ... hours up to 24 hours after repeated refusals; there is no manual
  trigger ([Upgrades](./upgrades.md#when-ibm-does-not-take-the-update)).
* **`PermissionDenied`** or **`IBMAPIError`** naming a listing ("refused listing the versions IBM Cloud offers in region
  ..." or "refused listing the workers of IBM cluster ..."): IBM refused a call CAPIBM makes before the upgrade request,
  so no request was sent. For a 403, grant the roles the message names; for a 400 or 404, check IBM's code and the
  description in the controller log. CAPIBM tries the listing again at every poll, so a fix takes effect within minutes
  ([Upgrades](./upgrades.md#when-ibm-does-not-take-the-update)).
* **`UpgradeFailed`**: the requested minor has not taken effect and IBM shows no update in progress; CAPIBM asks again
  at the time in the message. Check the cluster in IBM Cloud. It also follows a refused `UpdateMaster` whose
  `PermissionDenied` a failed listing replaced, once the listing works again; the retry time stays the same.
* **`UnparsableMasterVersion`** (`MasterVersionUpToDate=Unknown`): IBM reports a master version CAPIBM cannot parse,
  and the message quotes it; no master update is requested until IBM reports a version CAPIBM can parse. If it stays,
  see [Upgrades](./upgrades.md#when-ibm-does-not-take-the-update).
* **`BlockedByWorkerSkew`**: some workers are too many minors behind; let the workers catch up (they follow the master
  minor automatically; see `WaitingForCapacity` below).
* **`BlockedByCARotation`**, **`BlockedByClusterState`**: wait for the CA rotation or for IBM's cluster state; the
  message names the state.
* **`VersionSkipNotAllowed`**, **`DowngradeNotAllowed`**: raise `spec.distributionVersion` by exactly one minor.

## Workers do not roll or scale

* **`WaitingForCapacity`**: the cluster is too small to remove a worker. Scale the `MachinePool` to the count the message
  names (one more worker per zone of the pool), then back down after the rollout
  ([Upgrades](./upgrades.md#capacity-the-1-worker-a-rollout-needs)).
* **`PatchUpdateAvailable`**: not an error; the `Manual` policy waits for `spec.rollout.after`.
* **`DrainBlocked`** (`Draining` condition): a PodDisruptionBudget refuses an eviction; the message lists the pods and
  budgets. Add capacity, fix the budget, or set `nodeDrainTimeoutSeconds` on the `MachinePool`. CAPIBM never forces.
* **`WaitingForSettledCluster`**: another worker of the Cluster is changing; a worker stuck for more than 60 minutes is
  named with `WorkerOperationStuck` and no longer blocks. Check it in IBM Cloud.
* **`WaitingForDisruptionToken`**: another pool of the Cluster is disrupting; it continues when that one is done.
* **`WaitingForClusterState`**: IBM's cluster state blocks pool changes (for example `unsupported`: upgrade the master
  first).
* **`WaitingForClusterMinimum`**: the change would go below IBM's minimum (1 worker on IKS, 2 on ROKS); add workers or
  another pool first.
* **`AutoRebalanceDisabled`**: IBM's auto-rebalance is off in the worker's zone, so IBM would create no replacement. Run
  `ibmcloud ks worker-pool rebalance --cluster <cluster> --worker-pool <pool>`.
* **`ReplacementNotCreated`**: IBM created no new worker 30 minutes after deleting the old one; check auto-rebalance and
  the zone's capacity and quota.
* **`PoolNameConflict`**: two pool objects of the Cluster use the same `poolName`; rename or remove one.
* **`PoolConflict`**: an IBM pool with that name has another flavor; it is not CAPIBM's and is never touched. Use
  another `poolName`.
* **`PoolNameBeingReleased`**: IBM still lists a deleted pool with that name; it clears by itself.
* **`ExternallyAutoscaled`**: IBM's autoscaler add-on manages the pool; CAPIBM does not resize it.

### A pool removal or scale-down waits in WaitingForWorkloadCluster

CAPIBM drains Nodes through the workload cluster's API. If it cannot reach the workload cluster:

* when the cluster was reachable when the drain started (Nodes cordoned), a drain or volume detach time limit
  (`nodeDrainTimeoutSeconds`, `nodeVolumeDetachTimeoutSeconds`) still runs out and then lets the removal continue;
* when CAPIBM **never reached** it for this removal, the removal holds before anything is cordoned (reason
  `WaitingForWorkloadCluster`: `Deleting=True` for a pool removal, `WorkerPoolReady=False` for a scale-down) and **a
  time limit alone does not end the wait**, because no drain has started. This fails safe on purpose.

The escape hatch is to set **both** exclusion annotations on the `IBMCloudManagedMachinePool`:

```console
kubectl annotate ibmcloudmanagedmachinepool <name> -n <namespace> \
  machine.cluster.x-k8s.io/exclude-node-draining="" \
  machine.cluster.x-k8s.io/exclude-wait-for-node-volume-detach=""
```

With both, a pool removal and a scale-down need no workload cluster access: CAPIBM cordons nothing, evicts nothing and
does not wait for volumes to detach, and calls IBM. The pods on those workers are not drained by CAPIBM; they stop with
the workers (IBM's own drain gives them about 60 seconds). A whole-Cluster deletion needs neither: it never drains.

The escape hatch does **not** cover a **worker replacement**. A replacement waits in `WaitingForWorkloadCluster`
(`WorkersUpToDate=False`) before its IBM call while CAPIBM cannot reach the workload cluster, with or without the
annotations, because it has to see its new Node become `Ready` before the next worker. If the workload cluster went
away after the drain had started, a drain or detach time limit that runs out still lets the replacement call IBM; the
replacement then waits in `WaitingForReplacement` until the workload cluster is reachable again. Restore access to the
workload cluster (network, CBR rules, a valid kubeconfig Secret), or, if the pool can go, remove the whole pool with
the annotations above.

## Pool objects stuck in Terminating

* The last pool: `WaitingForClusterMinimum`; add another pool or delete the Cluster
  ([GitOps](./gitops.md#protect-pool-objects-from-pruning)).
* `WaitingForInfrastructure`: the `IBMCloudManagedCluster` is gone while the IBM cluster exists; re-apply it.
* `WaitingForControlPlane`: after a move, the control plane has not re-bound its IBM cluster yet; see the control
  plane's conditions.
* `DrainingNodes` for a long time: see `DrainBlocked` and the `Draining` condition.
