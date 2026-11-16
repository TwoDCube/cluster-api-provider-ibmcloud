# Deleting a cluster

## Delete the Cluster, nothing else

```console
kubectl delete cluster my-iks -n default
```

Cluster API deletes the `MachinePool`s first, then the control plane, then the `IBMCloudManagedCluster`:

1. **Pools**: because the whole Cluster is going away, the pools make **no IBM call and drain nothing**; deleting the IBM
   cluster removes every worker. Their finalizers go at once.
2. **Control plane** (`Deleting=True` with these reasons): CAPIBM checks once more that the IBM cluster is the one this
   control plane created or proved (the same binding checks as always), records the time and sends **one** `DELETE`
   (`DeletionRequested`), then waits while IBM deletes it (`WaitingForIBMDeletion`). IBM answers `204` at once and stops
   listing the cluster about 5 minutes later on both distributions. If IBM still does not show the cluster as deleting
   10 minutes after the request, CAPIBM sends the `DELETE` again. When the cluster is gone, CAPIBM emits the Warning event
   `LeftoverResources` (below) and removes the finalizer. The kubeconfig Secret is garbage-collected with the control
   plane.
3. **`IBMCloudManagedCluster`**: nothing to do in IBM Cloud; it never created anything.

Do **not** delete the `IBMCloudManagedControlPlane` or the `IBMCloudManagedCluster` on their own. The validating webhooks
reject that while their `Cluster` exists and is not being deleted:

* control plane: "delete Cluster <name> instead; deleting the control plane alone would delete the IBM cluster";
* `IBMCloudManagedCluster`: "delete Cluster <name> instead; IBMCloudManagedControlPlane/<cp> still needs this
  IBMCloudManagedCluster to manage and delete its IBM cluster".

Both are allowed when the `Cluster` is being deleted or is gone, during `clusterctl move`, and while no IBM cluster can
exist yet; the `IBMCloudManagedCluster` also when the `Cluster` no longer references it. If a control plane is deleted
alone anyway (the webhook does not guard deletes while the feature gate is off), CAPIBM waits until all `MachinePool`s
of its Cluster are gone (`WaitingForWorkersDeletion`) and deletes nothing before that.

Pool deletions are never rejected; see [Worker pools](./worker-pools.md#removing-a-pool).

## What stays behind

CAPIBM deletes the IBM cluster with `deleteResources=false`, and it only **references** your VPC, so some resources
remain. The `LeftoverResources` event lists them by name.

**Kept on purpose:**

* VPC Block Storage volumes of the cluster's persistent volumes (`deleteResources=false`).
* For ROKS, the internal registry bucket in your Cloud Object Storage instance.

**Removed by IKS within about 10 minutes** of the cluster's disappearance (measured: all were gone at the first sample
10 minutes after the 404, while nothing else in the VPC changed for 3 hours):

* the VPC load balancers `kube-<clusterID>-*`;
* the security groups `kube-<clusterID>`, `kube-lbaas-<clusterID>` and `kube-vpegw-<clusterID>`;
* the cluster's VPE gateway `iks-<clusterID>`.

Until IKS has removed them, deleting the cluster's subnets or the VPC fails with `409`. Wait and retry.

**Never removed by IBM** (shared by every cluster in the VPC):

* the VPE gateways `iks-api-<vpcID>`, `iks-cos-<vpcID>`, `iks-cos-config-<vpcID>`, `iks-riaas-<vpcID>` and
  `iks-registry-<vpcID>`;
* the security group `kube-vpegw-<vpcID>`.

IBM: "These shared VPE Gateways are not deleted, even if all the clusters in a VPC are deleted, in case other resources
are using them. If you no longer have any VPC clusters in the VPC, and if no other resources are using these shared VPE
Gateways, then you can delete these from the VPC portal." Delete them only when no cluster or other resource in the VPC
uses them.

## When CAPIBM does not delete

CAPIBM never sends `DELETE` for a cluster it cannot prove it owns, and it never removes its finalizer while it may still
own a cluster it cannot see. These states need a person:

| `Deleting` reason | Meaning | What to do |
|---|---|---|
| `OwnershipNotProven` | the IBM cluster the Secret names cannot be proven to be this control plane's (binding mismatch or failed proof) | put a fresh `ibmcloud ks cluster config --admin` kubeconfig into the Secret ([clusterctl move](./clusterctl-move.md#recovering-from-ownershipnotproven)), or remove the finalizer by hand to leave the IBM cluster alone |
| `PossibleOwnClusterFound` | a create attempt's outcome was unknown, and a same-named IBM cluster appeared whose creation time does not prove it is ours | check in IBM Cloud: if it is this Cluster's, delete it there; otherwise remove the finalizer by hand |
| `IBMClusterNotVisible` | IBM says the cluster does not exist, but the stored kubeconfig still works against it: "the credentials of the CAPIBM controller identity cannot see cluster <id> in account <account-id>; check the account and IAM scope" | give CAPIBM's credentials access to that account and resource group (for example after a move to a management cluster with other credentials) |
| `WaitingForInfrastructure` | the `IBMCloudManagedCluster` is gone while an IBM cluster may exist; CAPIBM needs its resource group to prove which cluster to delete | re-apply it from its manifest, or remove the finalizer by hand to leave the IBM cluster alone |
| `DeleteFailed` | IBM reports `delete_failed` | open an IBM support case; CAPIBM never sends `DELETE` again for it |
| `PermissionDenied` | IBM refused the `DELETE` (401 or 403) | grant the Kubernetes Service role ([Prerequisites](./prerequisites.md#iam-permissions)); CAPIBM checks again every 15 minutes |

Removing a finalizer by hand (`kubectl patch ibmcmcp <name> -n <namespace> --type json -p
'[{"op":"remove","path":"/metadata/finalizers"}]'`) deletes the object without touching IBM Cloud: the IBM cluster, if
any, keeps running and billing until you delete it in IBM Cloud.

If an IBM cluster with the Cluster's name exists that CAPIBM did not create (for example after `ClusterNameConflict`),
deleting the Cluster removes the finalizer with a Warning event `IBMClusterNotDeleted` naming that cluster ("IBM cluster
<name> (ID <id>, created <date>) was not deleted: CAPIBM cannot prove it created it for this Cluster"). Delete it in IBM
Cloud yourself if it is yours.

Likewise, deleting a pool object leaves the IBM pool alone, with the Warning event `WorkerPoolNotRemoved`, when another
`IBMCloudManagedMachinePool` of the Cluster still claims the same `poolName`, or when the IBM pool has another flavor
and so is not this object's pool (`PoolConflict`).

## Keep the gate on until the deletion is done

With the `ManagedKubernetes` gate off, nothing removes the control plane finalizer: the deletion hangs while IBM keeps
billing ([the clusterctl upgrade trap](./prerequisites.md#enable-the-feature-gate)).
