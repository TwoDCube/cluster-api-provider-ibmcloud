# Creating a cluster

This page assumes the [prerequisites](./prerequisites.md) are in place and the `ManagedKubernetes` gate is on.

## 1. Render and apply a flavor

Pick a flavor ([Topology and flavors](./topology.md)) and set its variables. All values below are placeholders:

```console
export IBMCLOUD_REGION=us-south
export IBMCLOUD_RESOURCE_GROUP_ID=<resource-group-id>
export IBMCLOUD_VPC_NAME=<vpc-name>
export IBMCLOUD_ZONE_1=us-south-1 IBMCLOUD_SUBNET_NAME_1=<subnet-in-us-south-1>
export IBMCLOUD_ZONE_2=us-south-2 IBMCLOUD_SUBNET_NAME_2=<subnet-in-us-south-2>
export IBMCLOUD_ZONE_3=us-south-3 IBMCLOUD_SUBNET_NAME_3=<subnet-in-us-south-3>
export IKS_DISTRIBUTION_VERSION=1.36

clusterctl generate cluster my-iks --flavor managed-iks --worker-machine-count 6 \
  --target-namespace default > my-iks.yaml
kubectl apply -f my-iks.yaml
```

Always pass `--worker-machine-count` (6 for the HA flavors, 2 for the single-zone flavors, or another multiple of
the zone count): without the flag or a `WORKER_MACHINE_COUNT` variable, `clusterctl generate cluster` sets
`WORKER_MACHINE_COUNT` to 0, so the template default does not apply and the control plane stops in
`InvalidConfiguration` ("MachinePool <name> spec.replicas=0 is not a positive multiple of the 3 zone(s) ...") before
any IBM call.

For ROKS use `--flavor managed-roks`, `ROKS_DISTRIBUTION_VERSION` (for example `4.21`) and
`COS_INSTANCE_CRN=<crn-of-your-cos-instance>`.

> **Working from the main branch?** Replace `--flavor managed-iks` with
> `--from ./templates/cluster-template-managed-iks.yaml` (see [Troubleshooting](../../user/troubleshooting.md)).

The API server returns an admission warning for the control plane, because the templates do not set
`spec.ibmCloudClusterName`:

```text
Warning: spec.ibmCloudClusterName is omitted: the IBM cluster will be named my-iks; IBM names are unique per account,
so other namespaces or management clusters using the same name will conflict; set spec.ibmCloudClusterName
(immutable) to choose a name
```

The cluster name you pass to `clusterctl generate cluster` then becomes the IBM cluster name, so it must follow
IBM's name rule: **at most 32 characters, starting with a letter** ([Naming the IBM cluster](#naming-the-ibm-cluster)).
A longer name, or one starting with a digit, is valid for Kubernetes but not for IBM: the API server rejects the
control plane ("spec.ibmCloudClusterName: Required value: without it the IBM cluster would be named after Cluster
...") and nothing is created at IBM. Either set `spec.ibmCloudClusterName` in the control plane of `my-iks.yaml` and
apply the file again, or delete the applied objects (`kubectl delete -f my-iks.yaml`) and generate the cluster again
with a shorter name.

## 2. What happens

1. The `IBMCloudManagedCluster` checks the resource group, the VPC and the subnet of every zone (it exists, is in the
   VPC and lies in that zone) before anything is billed, and sets `NetworkReady=True`. Nothing is created in your VPC.
2. The control plane finds the cluster's initial worker pool (the one pool with `spec.creation: WithCluster`, below),
   checks that IBM offers `spec.distributionVersion`, records the create attempt in its status and sends one
   `createCluster` request, which carries the initial pool. Event `SuccessfulCreateIBMCluster`.
3. IBM provisions the master. The endpoint and the kubeconfig become available 16 to 25 minutes after the request; the
   control plane then sets `spec.controlPlaneEndpoint`, writes the `<cluster>-kubeconfig` Secret after verifying it,
   and sets `status.initialization.controlPlaneInitialized`. Cluster API copies the endpoint into the `Cluster`.
4. IBM provisions the initial workers. The IBM cluster state goes `deploying`, `deployed`, `pending`, `normal`; it
   reaches `normal` after about 25 minutes (IKS) or 33 minutes (ROKS).
5. The initial pool's `IBMCloudManagedMachinePool` finds its IBM pool by name, publishes the workers in
   `spec.providerIDList` and becomes ready. Every other pool of the Cluster is created once the control plane is
   initialized.

A `201` answer can carry IBM "non-critical errors" (for example `E49df`: images cannot be pulled from `icr.io`, usually a
missing Container Registry role). The cluster is still created: CAPIBM emits one Warning event `IBMCreateWarnings` per
item and lists the codes in `status.nonCriticalCreateErrorCodes` and in the `IBMClusterProvisioned` message.

Follow the progress with:

```console
kubectl get cluster,ibmcmc,ibmcmcp,machinepool,ibmcmmp -n default
kubectl describe ibmcmcp my-iks-control-plane -n default      # conditions and events
```

While the master provisions, Cluster API's MachinePool controller logs ClusterCache connection errors for the cluster.
That is expected until the master is ready.

## 3. Use the cluster

```console
clusterctl get kubeconfig my-iks -n default > my-iks.kubeconfig
kubectl --kubeconfig my-iks.kubeconfig get nodes
```

The kubeconfig holds IBM's **admin client certificate**: it is cluster-admin and valid for two years. Read
[Kubeconfig and security](./kubeconfig-and-security.md) before you share it.

## The initial worker pool: `creation: WithCluster`

IBM creates every cluster together with exactly one worker pool. CAPIBM takes that pool from the one
`IBMCloudManagedMachinePool` of the Cluster that says so:

```yaml
apiVersion: infrastructure.cluster.x-k8s.io/v1alpha1
kind: IBMCloudManagedMachinePool
metadata:
  name: my-iks-default
spec:
  poolName: default          # becomes the IBM pool name; any valid name works
  creation: WithCluster      # the initial pool: IBM creates it together with the cluster
  flavor: cx2.2x4
  zones:
  - name: us-south-1
  - name: us-south-2
  - name: us-south-3
```

* **Exactly one** pool of the Cluster must carry `creation: WithCluster` before the IBM cluster is created. Other pools
  omit the field or say `AfterCluster` (the same thing); they are created once the control plane is initialized.
* Its `poolName` becomes the IBM pool name (IBM honours custom names), and its `MachinePool.spec.replicas` divided by
  its zone count becomes IBM's per-zone worker count.
* **None marked**: `IBMClusterProvisioned=False`, reason `WaitingForInitialWorkerPool`, message "No
  IBMCloudManagedMachinePool of Cluster <name> has spec.creation: WithCluster; mark exactly one pool, it is created
  together with the IBM cluster". Fix: set `creation: WithCluster` on one pool; the control plane reacts at once.
* **Not yet linked**: the same reason while Cluster API has not yet set the pool's owner reference to its `MachinePool`;
  it clears by itself.
* **Several marked**: `IBMClusterProvisioned=False`, reason `InvalidConfiguration`, message
  "IBMCloudManagedMachinePools <a>, <b> of Cluster <name> all have spec.creation: WithCluster; IBM creates the cluster
  with one initial pool, so keep exactly one of them WithCluster and set spec.creation: AfterCluster on the others (or
  omit it)". Fix: remove the marker from all but one.
* In all three cases CAPIBM makes no IBM call.
* The marker is read **only until the IBM cluster exists**. Adding, moving or removing it later changes nothing (a
  second marked pool after the create is not an error), and it never decides which IBM pool an object manages: every
  pool, the initial one included, is found by `spec.poolName` only. The field is mutable.
* The initial pool must satisfy the create rules: replicas a multiple of its zone count, at least 1 worker per zone on
  IKS and **2 per zone on ROKS**, and its zones must be zones of the `IBMCloudManagedCluster`. For ROKS, give it every
  zone the cluster will ever use ([Topology](./topology.md#zones-are-chosen-at-create-time)).
* After the create it is an ordinary pool. It can be deleted once other pools hold the cluster minimum.

## Naming the IBM cluster

`spec.ibmCloudClusterName` is the IBM cluster name; omitted, it is the `Cluster`'s `metadata.name`. IBM cluster names
are unique per **account**, across regions, while Cluster API names are unique per namespace, hence the admission
warning. On management clusters with several namespaces, use a convention such as `<namespace>-<name>`. Keep the first
24 characters unique: re-creating the same prefix five times in seven days hits the Let's Encrypt limit for the Ingress
subdomain. The field is immutable and cannot be added or removed after creation.

IBM's name rule, which IBM's API enforces when it creates the cluster:

* 1 to **32** characters;
* the first character is a letter (`a-z`, `A-Z`);
* the others are letters, digits, `-`, `_` and `.`.

IBM refuses any other name (HTTP 400, IBM code `E0125`), and older IBM documentation that allows 35 characters is out
of date. CAPIBM checks the rule before it calls IBM, at three points:

* The CRD rejects a `spec.ibmCloudClusterName` that breaks the rule.
* When `spec.ibmCloudClusterName` is omitted, the admission webhook checks the `Cluster`'s name and rejects the control
  plane if it breaks the rule. Kubernetes allows `Cluster` names of up to 63 characters that may start with a digit,
  so a name such as `team-a-production-iks-eu-de-cluster-01` (38 characters) or `1st-cluster` is rejected.
* If the webhook cannot tell the `Cluster` yet (a control plane created before its `Cluster`, with neither a cluster
  label nor an owner reference), the control plane stops with `InvalidConfiguration` before any IBM call
  ([Troubleshooting](./troubleshooting.md#invalidconfiguration-for-other-reasons)).

If an IBM cluster with the name already exists and CAPIBM cannot prove it created it for this Cluster, the control
plane stops with `ClusterNameConflict` and creates nothing ([Troubleshooting](./troubleshooting.md#clusternameconflict)).

## Versions

`spec.distributionVersion` is the IBM product **minor**, `MAJOR.MINOR` only: `"1.36"` for IKS, `"4.21"` for ROKS. IBM
deploys the newest patch of that minor (for example `1.36.5_1537` or `4.21.35_1537_openshift`) and applies later master
patches by itself. A patch version, a `v` prefix or an `_openshift` suffix is rejected. If IBM does not offer the minor,
the condition says so (`VersionNotAvailable`, listing the offered minors). See [Upgrades](./upgrades.md).

## Pod and service networks

The templates omit `Cluster.spec.clusterNetwork`, and IBM then picks free ranges. To choose them, set the first CIDR
block of `clusterNetwork.pods` and `clusterNetwork.services` (one block each, IPv4) before the cluster is created; they
cannot change later. CAPIBM checks IBM's documented rules before it sends anything:

* pods at least `/23`, services at least `/24`;
* both inside `172.17.0.0/16`, `172.21.0.0` to `172.31.255.255`, `198.18.0.0/15`, or the 192.168 range of the
  distribution (IKS `192.168.0.0/16`, OpenShift `192.168.0.0` to `192.168.254.255`);
* no overlap with each other, and the pod range no overlap with the cluster's subnets;
* pods may use the size-only form `0.0.0.0/X` (13 ≤ X ≤ 23), which IBM fills with a free range.

Custom ranges cannot be shared between clusters, so a fixed range in a template makes the second cluster from it fail.

## Private-only clusters

`spec.endpoints.publicServiceEndpoint: Disabled` creates a cluster without a public master endpoint. The management
cluster must then reach the private master URL (VPN, Direct Link, Transit Gateway, or running inside IBM Cloud;
[Prerequisites](./prerequisites.md#network)). The setting is immutable: IBM cannot switch a cluster between public and
private-only.

Verified on IBM Cloud: a private-only IKS cluster created, managed and deleted by CAPIBM running on an IKS management
cluster in another zone and subnet of the same VPC. IBM's `masterURL` named the private service endpoint with its port,
and the management cluster reached it without any security group rule added for it. Private-only OpenShift clusters,
and management clusters that reach the private URL from outside the VPC, are **not verified** yet.

## ROKS specifics

* `spec.openShift.imageRegistry.cos.instanceCRN` is required and immutable (IBM rejects a ROKS cluster on VPC without
  it, `E7278`). The CAPIBM service ID needs the Cloud Object Storage Administrator role on the instance.
* Worker flavors need at least 4 vCPU and 16 GB memory (for example `bx2.4x16`).
* `entitlement: OCPEntitled` on a pool marks it as covered by an existing OpenShift entitlement (Cloud Pak); it is
  OpenShift only and immutable.
* OperatorHub and public registries need public egress: a public gateway on the subnets and IBM's outbound traffic
  protection turned off ([Prerequisites](./prerequisites.md#vpc-and-other-ibm-resources)). Without a public gateway, IBM
  reports the ingress as `critical`, which CAPIBM ignores.
* IBM's default worker operating system is used (RHCOS on ROKS 4.21, Ubuntu 24 on IKS 1.36).
* OpenShift 4.22 adds port-443 host names for the master next to the NodePort. IBM's `masterURL` still names the
  NodePort, and CAPIBM's kubeconfig uses it, so nothing changes for CAPIBM.
