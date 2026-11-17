# Prerequisites

## Tools and management cluster

* `kubectl` and `clusterctl` matching the Cluster API release CAPIBM is built against (v1.14).
* A management cluster (for example kind, see [Getting Started](../../getting-started.md)) that can reach the IBM Cloud
  APIs and, later, the master endpoint of every cluster it manages (see [Network](#network)).
* Cluster API's `MachinePool` feature must stay enabled. It is Beta and on by default in Cluster API v1.14
  (`EXP_MACHINE_POOL`); with the managed gate on, CAPIBM refuses to start its controllers when the `MachinePool` CRD is
  missing.
* The IBM Cloud CLI with the `container-service` (`ks`) plug-in is useful for the checks on this page and in
  [Troubleshooting](./troubleshooting.md); CAPIBM itself does not need it.

## Enable the feature gate

The managed kinds are behind the `ManagedKubernetes` feature gate (Alpha, off by default). Enable it with the
`EXP_MANAGED_KUBERNETES` variable when you install the provider. The manager reads its IBM Cloud API key from
`IBMCLOUD_API_KEY` at install time, as for the VPC and PowerVS providers:

```console
export IBMCLOUD_API_KEY=<api-key-of-the-capibm-service-id>
export EXP_MANAGED_KUBERNETES=true
clusterctl init --infrastructure ibmcloud
```

The manifest passes the variable to the manager as `--feature-gates=ManagedKubernetes=${EXP_MANAGED_KUBERNETES:=false}`.
`clusterctl init` also installs the kubeadm bootstrap and control plane providers by default; managed clusters do not
use them and they stay idle.

With the gate off, the CRDs are still installed and the managed webhooks still run. For the gate they reject only
**create** requests, with a message that names `EXP_MANAGED_KUBERNETES`; existing objects stay updatable, and their
deletions pass the same checks as with the gate on, because a deletion cannot be taken back: a control plane keeps its
finalizer and CAPIBM carries out its deletion once the gate is on again, while the `IBMCloudManagedCluster`, which has
no finalizer, goes at once and leaves the control plane unable to manage or delete its IBM cluster
([Deleting a cluster](./deleting.md)). No managed controller runs.

<aside class="note warning">

<h1>The clusterctl upgrade trap</h1>

`clusterctl upgrade` substitutes the template variables from the **current** environment again. An upgrade run without
`EXP_MANAGED_KUBERNETES=true` therefore silently turns the gate **off**: managed clusters are no longer reconciled,
Cluster deletions hang on the control plane finalizer while IBM keeps billing, and kubeconfig refresh and CA rotation
handling stop. Always upgrade with the variable set (and `IBMCLOUD_API_KEY`, which is substituted the same way):

```console
export IBMCLOUD_API_KEY=<api-key-of-the-capibm-service-id>
export EXP_MANAGED_KUBERNETES=true
clusterctl upgrade apply --contract v1beta2
```

If it happens anyway, the manager logs an error at start-up and emits a Warning event `ManagedKubernetesDisabled` on
every `IBMCloudManagedControlPlane` it would reconcile: "feature gate ManagedKubernetes is off: this cluster is not
reconciled and cannot be deleted; set EXP_MANAGED_KUBERNETES=true". Run the upgrade again with the variable set. A
manager started with `--namespace` or `--watch-filter` checks only the control planes of that namespace or with that
`cluster.x-k8s.io/watch-filter` label, so it never warns about control planes that another CAPIBM instance reconciles.

</aside>

## IAM permissions

CAPIBM uses one IBM Cloud API key per installation, normally that of a service ID that your account administrator
creates for it. CAPIBM never creates or changes an IAM object; you grant the roles below. They follow IBM's documented
roles and are not narrowed by experiments.

| Service | Role | Scope | Why |
|---|---|---|---|
| Kubernetes Service (`containers-kubernetes`) | **Administrator** platform role and **Manager** service role | the resource group of the clusters | create, delete and update clusters, pools and workers; the admin kubeconfig needs Administrator |
| VPC Infrastructure Services | **Administrator** platform role | the resource group, or the VPC resources | IBM's prerequisite for creating a cluster (IKS orders workers, VPE gateways and load balancers with the stored infrastructure key); CAPIBM itself only reads the VPC and subnets |
| Container Registry | **Administrator** platform role | the account (IBM: do not limit it to the resource group) | cluster creation sets up image pull access; without it IBM still creates the cluster but reports the non-critical error `E49df` |
| Cloud Object Storage | **Administrator** platform role | the COS instance named in the control plane | ROKS only: the internal registry bucket |
| Resource group | **Viewer** | the resource group of the clusters | creating clusters outside the default group; CAPIBM's resource group check |
| IAM Identity Service | **Service ID creator**, **User API key creator**, **Viewer**, **Operator** | account, or the resource group of the clusters | IKS creates the infrastructure "cluster API key" for the region and resource group; without these roles IBM refuses the create with `A0656`, unless an account administrator pre-set that key (below) |
| Global Tagging | none | | not needed |

This section is what condition messages mean by "the IAM page of the managed Kubernetes book" or "the IAM roles of
the managed Kubernetes book". Error messages that point to this table name the credentials as "the CAPIBM controller
identity"; CAPIBM never prints an IAM ID, an e-mail address or a key fingerprint.

### The infrastructure key of a region and resource group

IKS keeps one infrastructure API key per region and resource group. It belongs to the first user (or service ID) that
created a cluster there, or to whoever last ran `ibmcloud ks api-key reset` for that region and resource group
([IBM docs: "Understanding how API keys are used"](https://cloud.ibm.com/docs/containers?topic=containers-access-creds),
last updated 2026-07-27), and IBM orders the workers, load balancers and volumes of **every** cluster there with it.
This is a customer decision, and CAPIBM takes no side:

* If CAPIBM creates the first cluster in a region and resource group, its service ID's credentials become that key.
  Rotating the CAPIBM API key later does not rotate the stored key; `ibmcloud ks api-key reset` does.
* An account administrator can pre-set the key (`ibmcloud target -g <resource-group>` and then
  `ibmcloud ks api-key reset --region <region>`), so that the CAPIBM service ID does not need the IAM Identity Service
  roles. This is IBM-documented and was not tested with CAPIBM.
* Some customers give CAPIBM a dedicated functional ID for this reason.

To see whose key a cluster uses, run `ibmcloud ks api-key info --cluster <cluster-name-or-id>`. See also the
[trust model](./trust-model.md#the-iks-infrastructure-key).

## Network

* **Management cluster to IBM Cloud.** The manager calls the IAM token service, `https://iam.cloud.ibm.com` (or the
  URL set in `IBMCLOUD_AUTH_URL` at install time), for the access token that every other call carries; when it cannot
  reach IAM, every IBM call fails. With that token it calls `https://containers.cloud.ibm.com/global` (Kubernetes
  Service), the regional VPC API (`https://<region>.iaas.cloud.ibm.com/v1`) and the Resource Manager API
  (`https://resource-controller.cloud.ibm.com`), unless `SERVICE_ENDPOINT` moves them
  ([below](#private-service-endpoints)). An egress allowlist must name all four. Requests to the Kubernetes Service API
  are limited by the manager flags `--containers-api-qps` (default 10) and `--containers-api-burst` (default 20); IBM
  allows 200 requests per 10 seconds per source IP address, shared with every other tool behind the same address.
* **Management cluster to each master.** CAPIBM probes the cluster's master URL to verify the kubeconfig, and Cluster
  API's ClusterCache and CAPIBM's drain use it too. The management cluster must reach the public master URL (default)
  or, for a private-only cluster, the private one (VPN, Direct Link, Transit Gateway, or running inside IBM Cloud).
  Without it `KubeconfigReady` is `False` with reason `EndpointUnreachable`.
* **Context-based restrictions.** If CBR rules protect the cluster API (`api-type:cluster`, which controls access to
  the cluster API server, including every `kubectl` command) or the management API (`api-type:management`, which
  protects provisioning and managing clusters and worker pools), allow the management cluster's egress addresses there
  ([IBM docs: "Protecting Kubernetes Service resources with context-based restrictions"](https://cloud.ibm.com/docs/containers?topic=containers-cbr),
  last updated 2026-11-04). Restricting the cluster API to the management cluster is also the main mitigation for the
  long-lived admin certificate ([Kubeconfig and security](./kubeconfig-and-security.md#mitigations)).

### Private service endpoints

To send the Kubernetes Service, VPC or Resource Manager calls to another URL (for example IBM's private endpoint, or a
staging endpoint), set `SERVICE_ENDPOINT` at install time; it becomes the manager's `--service-endpoint` flag. The
service IDs are `containers`, `vpc` and `rm`:

```console
export SERVICE_ENDPOINT='us-south:containers=https://private.us-south.containers.cloud.ibm.com/global'
```

Each URL is used as given. A `containers` or `vpc` entry applies only to the clusters of the region it is listed
under: a region without a `containers` entry uses `https://containers.cloud.ibm.com/global`, and one without a `vpc`
entry `https://<region>.iaas.cloud.ibm.com/v1`. Resource Manager is a global service, so an `rm` entry is not per
region: CAPIBM uses the first `rm` entry, whatever region it is listed under, for the clusters of every region, and
`https://resource-controller.cloud.ibm.com` when there is none.

`SERVICE_ENDPOINT` does not move the IAM token requests. Set their URL with `IBMCLOUD_AUTH_URL` at install time
instead, for example IBM's private IAM endpoint:

```console
export IBMCLOUD_AUTH_URL=https://private.iam.cloud.ibm.com
```

A management cluster that reaches IBM Cloud only through private endpoints needs both variables. `clusterctl upgrade`
substitutes them again, like `IBMCLOUD_API_KEY`, so set them for every upgrade too.

## VPC and other IBM resources

CAPIBM creates none of these; they must exist before you create a cluster.

* A **resource group** (the templates take its ID, 32 hexadecimal characters).
* A **VPC Gen2** in the region, and **one subnet per zone** the cluster will use, in that VPC. Subnet names are looked
  up inside the VPC; a name that matches more than one subnet is rejected (`SubnetNameAmbiguous`), so give the subnet ID
  instead. Custom pod and service ranges must not overlap the subnets ([Creating a cluster](./creating-a-cluster.md#pod-and-service-networks)).
* A **public gateway** on the subnets if the cluster needs public egress (for example ROKS OperatorHub or public image
  registries). It is optional: without one, `NetworkReady` stays `True` with an informational message, and IBM reports
  the ingress status as `warning` or `critical`, which CAPIBM ignores.
* A public gateway alone does not give the workers public egress: IBM turns on **outbound traffic protection** for new
  VPC clusters (IKS 1.30 and later, ROKS 4.15 and later), which blocks it. CAPIBM does not set or report this setting
  yet and leaves it as IBM or you set it. To allow public egress, turn it off once IBM has created the cluster
  (`ibmcloud ks vpc outbound-traffic-protection disable --cluster <cluster ID>`), or mirror the images to IBM Cloud
  Container Registry instead; ROKS OperatorHub also needs a change on the cluster. See IBM's
  [Managing outbound traffic protection in VPC clusters](https://cloud.ibm.com/docs/containers?topic=containers-sbd-allow-outbound)
  ([ROKS](https://cloud.ibm.com/docs/openshift?topic=openshift-sbd-allow-outbound)).
* For **ROKS**: a standard **Cloud Object Storage** instance for the internal image registry. IBM rejects a ROKS cluster
  on VPC without one (`E7278`), so `spec.openShift.imageRegistry.cos.instanceCRN` is required. IBM creates a bucket in
  it; the bucket is kept when the cluster is deleted.
* Enough VPC quota and free addresses in each subnet for the workers (`NetworkReady` mentions subnets with fewer than 16
  free addresses).
