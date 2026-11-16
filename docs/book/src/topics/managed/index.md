# Managed Kubernetes (IKS and ROKS)

<aside class="note warning">

<h1>Experimental</h1>

Managed Kubernetes support is **experimental**. Its API is `v1alpha1` and can change between releases, and it runs
only when the `ManagedKubernetes` feature gate is on (Alpha, off by default; see
[Prerequisites](./prerequisites.md#enable-the-feature-gate)). Red Hat OpenShift 4.22 clusters and clusters with a
private-only master endpoint are **not verified** yet.

</aside>

Cluster API Provider IBM Cloud (CAPIBM) can manage clusters of the two IBM Cloud managed Kubernetes offerings on
**VPC Gen2**:

* **IBM Cloud Kubernetes Service (IKS)**, `distribution: Kubernetes`;
* **Red Hat OpenShift on IBM Cloud (ROKS)**, `distribution: OpenShift`.

IBM runs and patches the master. CAPIBM creates the IBM cluster, publishes its endpoint and an admin kubeconfig,
upgrades the master one minor version at a time, manages worker pools (create, scale, roll, remove, each worker removal
drained first) and deletes the cluster, all through the IBM Cloud Kubernetes Service API. Classic infrastructure and
Satellite are not supported.

Like the rest of CAPIBM, the managed Kubernetes support is released under the Apache License 2.0.

## Kinds

All three kinds ship in the `infrastructure-ibmcloud` clusterctl provider.

| Kind | API group | Cluster API role | Short name | Represents |
|---|---|---|---|---|
| `IBMCloudManagedControlPlane` | `controlplane.cluster.x-k8s.io/v1alpha1` | control plane | `ibmcmcp` | one IKS or ROKS cluster (the master, the endpoint, the kubeconfig) |
| `IBMCloudManagedCluster` | `infrastructure.cluster.x-k8s.io/v1alpha1` | InfraCluster | `ibmcmc` | the **existing** region, resource group, VPC and one subnet per zone the cluster uses |
| `IBMCloudManagedMachinePool` | `infrastructure.cluster.x-k8s.io/v1alpha1` | InfraMachinePool | `ibmcmmp` | one IBM worker pool, owned by one Cluster API `MachinePool` |

```text
Cluster ──infrastructureRef──> IBMCloudManagedCluster      (VPC, subnets, resource group: referenced, never created)
        ──controlPlaneRef────> IBMCloudManagedControlPlane (the IBM cluster; writes <cluster>-kubeconfig)
MachinePool ──infrastructureRef──> IBMCloudManagedMachinePool  (one IBM worker pool, found by spec.poolName)
```

The `MachinePool`s have no Machines and no bootstrap data (`bootstrap.dataSecretName: ""`), so there are no
MachineHealthChecks; IBM's autorecovery looks after worker health. ClusterClass is not supported: the control plane has
no `spec.version` (see [Upgrades](./upgrades.md#why-there-is-no-specversion)), and the topology controller needs one.
Use the clusterctl flavor templates described in [Topology and flavors](./topology.md).

## What CAPIBM does, and what it never does

* It **references** an existing VPC, existing subnets and an existing resource group. It never creates or deletes a VPC
  resource; IBM's own leftovers after a delete are listed in [Deleting a cluster](./deleting.md).
* It acts only on an IBM cluster it created itself, or one whose admin credential the namespace proves it holds (for
  example after `clusterctl move`). An IBM cluster that merely has the same name is never adopted, changed or deleted
  (see [Trust model](./trust-model.md)). Importing a cluster that CAPIBM did not create is not supported.
* It drains a worker's Node, respecting PodDisruptionBudgets, before every IBM call that removes the worker
  (rollout, scale-down, pool removal). It never force-deletes a pod and never deletes a Node object.
* It runs one worker disruption per Cluster at a time and never goes below IBM's minimum worker count.
* It writes only two spec fields, both required by Cluster API: `IBMCloudManagedControlPlane.spec.controlPlaneEndpoint`
  (once) and `IBMCloudManagedMachinePool.spec.providerIDList`. Everything else in spec is yours (see
  [GitOps](./gitops.md)).
* It never creates or changes IAM objects (policies, service IDs, API keys, trusted profiles). The permissions it needs
  are listed in [Prerequisites](./prerequisites.md#iam-permissions).
* The kubeconfig it publishes holds IBM's admin client certificate on both distributions. This is a documented
  deviation from the Cluster API contract; see [Kubeconfig and security](./kubeconfig-and-security.md).

Not supported in this release (each one can be added later without breaking the API): worker pool labels and taints,
adding or removing zones of a pool, cluster autoscaling (IBM's autoscaler add-on is detected and respected, but not
configured), per-pool subnets and security groups, deletion options (CAPIBM keeps storage volumes and the ROKS registry
bucket), private or VPE management endpoints, surge rollouts, `MachineDrainRule`, ClusterClass, and per-cluster
credentials.

## Status at a glance

```console
kubectl get cluster,ibmcmc,ibmcmcp,machinepool,ibmcmmp -n <namespace>
kubectl get ibmcmcp -n <namespace> -o wide     # adds the IBM cluster ID and the endpoint host
```

* `IBMCloudManagedControlPlane` columns: `Distribution`, `Version` (IBM's master version, for example `1.36.5_1537` or
  `4.21.35_1537_openshift`), `Kubernetes` (the version the API server reports, for example `v1.36.5+IKS` or `v1.34.9`),
  `State` (IBM's cluster state) and `Available`.
* `IBMCloudManagedMachinePool` columns: `Pool` (the IBM pool name), `Flavor`, `Replicas`, `Ready` and `Updates` (workers
  with a newer minor or fix pack available).

<aside class="note">

<h1>Which conditions to trust</h1>

Cluster API v1.14 leaves MachinePools out of the Cluster's `WorkersAvailable`, `RollingOut`, `ScalingUp` and
`ScalingDown` conditions. For a cluster that has only MachinePools, `WorkersAvailable` is `True` (reason `NoWorkers`),
so the Cluster can show `Available=True` while a worker pool is not ready, and the Cluster's `RollingOut` shows only
master updates. The authoritative signals are the control plane's `Available` and the pools' `WorkerPoolReady`,
`WorkersUpToDate` and `Draining` conditions ([reference](./reference.md)). `clusterctl describe cluster` prints no
version for this control plane, because it shows only a control plane `spec.version`; use the `ibmcmcp` columns above.

</aside>

## Contents

* [Prerequisites](./prerequisites.md): management cluster, feature gate and the `clusterctl upgrade` trap, IAM
  permissions, network and VPC.
* [Topology and flavors](./topology.md): the four clusterctl flavors, IBM's HA topology and what smaller topologies
  give up.
* [Creating a cluster](./creating-a-cluster.md): the initial worker pool, naming, networks, ROKS specifics.
* [Worker pools and drain](./worker-pools.md): adding, scaling, adopting and removing pools; how CAPIBM drains.
* [Upgrades and update policy](./upgrades.md): master minor upgrades, worker fix packs, `rollout.after`, versions.
* [Kubeconfig and security](./kubeconfig-and-security.md): the admin certificate, the contract deviation and its
  mitigations.
* [Trust model](./trust-model.md): who can make CAPIBM do what, and what later releases keep compatible.
* [GitOps](./gitops.md): what to keep in Git, prune protection for pools, unsafe sync settings.
* [clusterctl move](./clusterctl-move.md): moving managed clusters and the binding proof.
* [Deleting a cluster](./deleting.md): deletion order and what stays in your VPC.
* [Troubleshooting](./troubleshooting.md): symptoms, causes and fixes.
* [Conditions, events and annotations](./reference.md): every reason, event and annotation.
