# clusterctl move

`clusterctl move` moves managed clusters between management clusters, for example from a bootstrap kind cluster to a
permanent one. The IBM clusters keep running; only the Cluster API objects move.

## Before you move

* The **target** management cluster runs the same Cluster API and CAPIBM versions, installed with
  `EXP_MANAGED_KUBERNETES=true` ([Prerequisites](./prerequisites.md#enable-the-feature-gate)). With the gate off on the
  target, the moved objects are not reconciled and cannot be deleted.
* The target's CAPIBM credentials must reach the **same IBM account and resource group**. They may be a different API
  key or even a different IAM identity: nothing CAPIBM stores or checks depends on which identity fetched the moved
  kubeconfig.
* The target must reach the master URLs of the moved clusters (and be allowed by any CBR rule on the cluster API).
* Every control plane must be **initialized**: `clusterctl move` refuses to move a control plane whose
  `status.initialization.controlPlaneInitialized` is not `true`. Wait until the cluster's kubeconfig exists.
* Let running worker operations finish if you can. They are safe to interrupt (below), but a move in the middle of a
  drain makes it take longer.

```console
clusterctl move --to-kubeconfig=<target-kubeconfig> -n <namespace>
```

## What moves and what does not

| Moves | Does not move (rebuilt on the target) |
|---|---|
| `Cluster`, `IBMCloudManagedCluster`, `IBMCloudManagedControlPlane` (spec, incl. `controlPlaneEndpoint`) | the status of every object |
| `MachinePool`s and `IBMCloudManagedMachinePool`s (spec, incl. `providerIDList`) | the disruption Lease `<cluster>-ibm-worker-disruption` |
| the `<cluster>-kubeconfig` Secret, with its annotations (it is owned by the control plane) | CAPIBM's credentials (each management cluster has its own) |

The IBM cluster ID that status held is also kept durably on the moved Secret as the annotation
`ibmcloudmanaged.controlplane.cluster.x-k8s.io/cluster-id`, which is why only initialized control planes (with that
Secret) can be moved.

## The binding on the target

On the source, clusterctl pauses the Cluster and deletes the objects with the
`clusterctl.cluster.x-k8s.io/delete-for-move` annotation; CAPIBM then makes no IBM call and deletes nothing.

On the target, each control plane starts with an empty status and re-establishes its **binding**, the fact that it
stands for one particular IBM cluster. It reads the cluster ID from the moved Secret, but annotations can be written by
anyone in the namespace, so it does not simply trust it:

1. It reads that IBM cluster and checks that its name, resource group, distribution and provider are the ones this
   control plane expects.
2. It **proves possession**: the Secret's credential must pass a cluster-admin access review against that cluster's
   IBM master URL (with TLS verified against the cluster's own CA on IKS, the system roots on ROKS). A forged annotation
   or a copied endpoint cannot produce such a credential; a moved Secret holds one.
3. Only then does it bind, rebuild its status and set `controlPlaneInitialized` again. The Secret is not rewritten if
   it is still valid.

If a check fails, `IBMClusterProvisioned=False` with reason `OwnershipNotProven` (the message names the failing check:
TLS, 401, access review, name or resource group) and CAPIBM changes nothing in IBM Cloud and writes no Secret.

<aside class="note warning">

<h1>A kubeconfig Secret you provided must hold a client certificate</h1>

The proof uses the Secret's own credential and can only use an inline client certificate and key
(`client-certificate-data`, `client-key-data`), as `ibmcloud ks cluster config --admin` produces. If you replaced
`<cluster>-kubeconfig` with your own Secret and its user holds a **bearer token** (or an `exec` plugin, an auth provider
or file paths), the moved control plane ends in `OwnershipNotProven` with a message that says a client-certificate
kubeconfig is needed. CAPIBM never rebuilds a token kubeconfig. Secrets written by CAPIBM always hold IBM's admin
certificate. See [Kubeconfig and security](./kubeconfig-and-security.md#a-kubeconfig-secret-you-provide).

</aside>

The pools wait until their control plane is bound, find their IBM pools again by `spec.poolName` and become ready
again. Each pool starts with an empty status and therefore waits one settle delay (3 minutes) before any disruptive IBM
call, because the source's record of recent disruptions did not move. Until then Cluster API keeps the moved
`providerIDList`, so no Node is touched.

Work in flight resumes on the target:

* A master upgrade in progress is observed as such and waited out.
* A drain or worker replacement resumes from the annotations CAPIBM keeps on the Nodes (drain start time, owner,
  cordon) and from IBM's worker states; the drain time limit does not restart. The `drain-owner` annotation stops a
  second pool from starting while the Lease is gone.

## Recovering from `OwnershipNotProven`

Common causes: the admin certificate expired during a very long pause (it is valid for two years), the cluster's CA was
rotated after the Secret was written, or a user-provided Secret holds a token. Put a fresh admin kubeconfig into the
Secret, keeping its type, labels and annotations:

```console
ibmcloud ks cluster config --cluster <ibm-cluster-id> --admin
kubectl config view --minify --flatten --context <context-of-that-cluster> > admin.kubeconfig
kubectl -n <namespace> patch secret <cluster>-kubeconfig --type merge \
  -p "{\"data\":{\"value\":\"$(base64 -w0 admin.kubeconfig)\"}}"
```

The control plane checks again within 15 minutes, or at once when the control plane object itself changes. Do
not edit the `cluster-id` annotation to "fix" a mismatch: if the checks of step 1 fail, the control plane is bound to
the wrong cluster or the cluster changed, and a person has to look.

## Do not copy manifests instead of moving

Exporting the objects with `kubectl get -o yaml` and applying them on another management cluster is not a move. Without
the Secret, the new control plane finds an IBM cluster with its name that it cannot prove it created, and stops with
`ClusterNameConflict`; it never adopts or changes it. Copying the kubeconfig Secret as well hands over cluster-admin
and leaves two management clusters acting on one IBM cluster. Use `clusterctl move`. The same applies to restoring a
backup of objects whose control plane was not yet initialized: the restored control plane ends in
`ClusterNameConflict` and needs a person to decide ([Troubleshooting](./troubleshooting.md#clusternameconflict)).
If the IBM cluster does belong to the Cluster, [recover its ownership](#recovering-ownership).

## Recovering ownership

This is the procedure the `ClusterNameConflict` message points to. It is a different one from
[Recovering from `OwnershipNotProven`](#recovering-from-ownershipnotproven): there the Secret exists and only its
credential is replaced; here no Secret exists yet and you create it, with the cluster ID annotation.

CAPIBM binds a control plane to an existing IBM cluster only when the namespace proves it holds a cluster-admin
credential for it. To hand an existing cluster to a control plane that stopped in `ClusterNameConflict`, create its
kubeconfig Secret yourself with IBM's admin kubeconfig and the cluster's ID:

```console
ibmcloud ks cluster config --cluster <ibm-cluster-id> --admin
kubectl config view --minify --flatten --context <context-of-that-cluster> > admin.kubeconfig
kubectl -n <namespace> create secret generic <cluster>-kubeconfig \
  --type=cluster.x-k8s.io/secret --from-file=value=admin.kubeconfig
kubectl -n <namespace> label secret <cluster>-kubeconfig cluster.x-k8s.io/cluster-name=<cluster>
kubectl -n <namespace> annotate secret <cluster>-kubeconfig \
  ibmcloudmanaged.controlplane.cluster.x-k8s.io/cluster-id=<ibm-cluster-id>
rm admin.kubeconfig
```

CAPIBM then checks the cluster's name, resource group and distribution against the control plane, proves possession
with the Secret's credential and binds; from then on it manages the cluster and the Secret, and **deleting the Cluster
deletes that IBM cluster**. Do this only for a cluster that belongs to this Cluster. The control plane checks again
within 15 minutes, or at once when you change it.
