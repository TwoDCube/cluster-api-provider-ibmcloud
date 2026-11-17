Render the template via clusterctl
==================================

## VPC

```
IBMVPC_REGION=us-south-1 \
IBMVPC_ZONE=us-south-1 \
IBMVPC_RESOURCEGROUP=4f15679623607b855b1a27a67f20e1c7 \
IBMVPC_NAME=ibm-vpc-1 \
IBMVPC_IMAGE_NAME=capibm-vpc-ubuntu-2004-kube-v1-25-2 \
IBMVPC_PROFILE=bx2-4x16 \
IBMVPC_SSHKEY_NAME=capi-vpc-key \
clusterctl generate cluster ibm-vpc-1 --kubernetes-version v1.14.3 \
--target-namespace default \
--control-plane-machine-count=1 \
--worker-machine-count=2 \
--from ./cluster-template.yaml
```

## Power VS

```
IBMPOWERVS_SSHKEY_NAME="my-pub-key" \
IBMPOWERVS_VIP="192.168.167.6" \
IBMPOWERVS_VIP_EXTERNAL="163.68.65.6" \
IBMPOWERVS_VIP_CIDR="29" \
IBMPOWERVS_IMAGE_NAME="capibm-powervs-centos-streams8-1-26-2" \
IBMPOWERVS_WORKSPACE_ID="3229a94c-af54-4212-bf60-6202b6fd0a07" \
IBMPOWERVS_NETWORK_NAME="capi-test" \
IBMACCOUNT_ID="ibm-accountid" \
IBMPOWERVS_REGION="osa" \
IBMPOWERVS_ZONE="osa21" \
BASE64_API_KEY=$(echo -n $IBMCLOUD_API_KEY | base64) \
clusterctl generate cluster ibm-powervs-1 --kubernetes-version v1.26.2 \
--target-namespace default \
--control-plane-machine-count=3 \
--worker-machine-count=1 \
--from ./cluster-template-powervs.yaml
```

### Additional parameters for modifying PowerVS Control-Plane spec
```
IBMPOWERVS_CONTROL_PLANE_MEMORY
IBMPOWERVS_CONTROL_PLANE_PROCESSORS
IBMPOWERVS_CONTROL_PLANE_SYSTYPE
IBMPOWERVS_CONTROL_PLANE_PROCTYPE
```

### Additional parameters for modifying PowerVS Compute node spec
```
IBMPOWERVS_COMPUTE_MEMORY
IBMPOWERVS_COMPUTE_PROCESSORS
IBMPOWERVS_COMPUTE_SYSTYPE
IBMPOWERVS_COMPUTE_PROCTYPE
```

### Additional parameters for modifying PowerVS Cluster API server port
```
API_SERVER_PORT
```

## IBM Cloud managed Kubernetes (IKS and ROKS)

Four flavors create a cluster whose control plane IBM manages, on VPC Gen2, in an existing VPC with one existing
subnet per zone (CAPIBM never creates or deletes VPC resources). The provider must run with the `ManagedKubernetes`
feature gate (`EXP_MANAGED_KUBERNETES=true` when the provider is installed). The templates carry no credential
variable: CAPIBM calls IBM Cloud with the credentials of its controller.

| Flavor | Distribution | Topology |
|---|---|---|
| `managed-iks` | IBM Cloud Kubernetes Service | 3 zones x 2 workers per zone, IBM's HA topology |
| `managed-roks` | Red Hat OpenShift on IBM Cloud | 3 zones x 2 workers per zone, IBM's HA topology |
| `managed-iks-single-zone` | IBM Cloud Kubernetes Service | 1 zone x 2 workers, for development and tests, not HA |
| `managed-roks-single-zone` | Red Hat OpenShift on IBM Cloud | 1 zone x 2 workers, for development and tests, not HA |

The HA flavors follow IBM's guidance: "Plan to have at least two worker nodes per zone, so six nodes across three
zones in total" (IBM docs `strategy.md`, 2026-11-05) and "Ingress requires at least two worker nodes per zone" (IBM
docs `ts-ingress-alb-pods-nodes.md`, 2026-04-06). The single-zone flavors are not HA: a zone outage stops every worker,
and IBM may report the ingress as `warning` or `critical`, which is expected and informational. A
`managed-roks-single-zone` cluster keeps single-zone router ingress for its lifetime, because IBM does not extend the
router load balancer to zones added later.

### IKS, HA

```
IBMCLOUD_REGION=us-south \
IBMCLOUD_RESOURCE_GROUP_ID=<resource-group-id> \
IBMCLOUD_VPC_NAME=<vpc-name> \
IBMCLOUD_ZONE_1=us-south-1 IBMCLOUD_SUBNET_NAME_1=<subnet-in-us-south-1> \
IBMCLOUD_ZONE_2=us-south-2 IBMCLOUD_SUBNET_NAME_2=<subnet-in-us-south-2> \
IBMCLOUD_ZONE_3=us-south-3 IBMCLOUD_SUBNET_NAME_3=<subnet-in-us-south-3> \
clusterctl generate cluster my-iks \
--target-namespace default \
--worker-machine-count=6 \
--from ./cluster-template-managed-iks.yaml
```

### ROKS, single zone

```
IBMCLOUD_REGION=us-south \
IBMCLOUD_RESOURCE_GROUP_ID=<resource-group-id> \
IBMCLOUD_VPC_NAME=<vpc-name> \
IBMCLOUD_ZONE_1=us-south-1 IBMCLOUD_SUBNET_NAME_1=<subnet-in-us-south-1> \
COS_INSTANCE_CRN=<cloud-object-storage-instance-crn> \
clusterctl generate cluster my-roks \
--target-namespace default \
--worker-machine-count=2 \
--from ./cluster-template-managed-roks-single-zone.yaml
```

`--flavor <flavor>` instead of `--from` reads the same template from a CAPIBM release. The other two flavors take the
same variables: `managed-roks` those of `managed-iks` plus `COS_INSTANCE_CRN`, `managed-iks-single-zone` those of
`managed-roks-single-zone` without `COS_INSTANCE_CRN`.

### Variables

| Variable | Default | Flavors | Meaning |
|---|---|---|---|
| `CLUSTER_NAME` | required | all | Name of the Cluster and prefix of the other objects; the cluster name argument of `clusterctl generate cluster`. The IBM cluster gets the same name (see below). |
| `IBMCLOUD_REGION` | required | all | IBM Cloud region of the cluster, for example `us-south`. |
| `IBMCLOUD_RESOURCE_GROUP_ID` | required | all | ID (32 hexadecimal characters) of the resource group that the IBM cluster is created in. |
| `IBMCLOUD_VPC_NAME` | required | all | Name of the existing VPC. |
| `IBMCLOUD_ZONE_1` | required | all | First zone of the region, for example `us-south-1`. |
| `IBMCLOUD_SUBNET_NAME_1` | required | all | Name of an existing subnet of the VPC in `IBMCLOUD_ZONE_1`; give it at least 256 addresses, IBM cannot resize it later. |
| `IBMCLOUD_ZONE_2` | required | `managed-iks`, `managed-roks` | Second zone of the region. |
| `IBMCLOUD_SUBNET_NAME_2` | required | `managed-iks`, `managed-roks` | Existing subnet in `IBMCLOUD_ZONE_2`. |
| `IBMCLOUD_ZONE_3` | required | `managed-iks`, `managed-roks` | Third zone of the region. |
| `IBMCLOUD_SUBNET_NAME_3` | required | `managed-iks`, `managed-roks` | Existing subnet in `IBMCLOUD_ZONE_3`. |
| `IKS_DISTRIBUTION_VERSION` | `1.36` | `managed-iks`, `managed-iks-single-zone` | Kubernetes version as MAJOR.MINOR; IBM applies the newest patch. |
| `ROKS_DISTRIBUTION_VERSION` | `4.21` | `managed-roks`, `managed-roks-single-zone` | OpenShift version as MAJOR.MINOR; IBM applies the newest patch. |
| `COS_INSTANCE_CRN` | required | `managed-roks`, `managed-roks-single-zone` | CRN of an existing Cloud Object Storage instance for the internal image registry; IBM rejects a ROKS cluster on VPC without it. |
| `WORKER_MACHINE_COUNT` | `6` (HA flavors), `2` (single-zone flavors) | all | Total number of workers of the initial pool, a multiple of the zone count; set with `--worker-machine-count` (see below). |
| `IBMCLOUD_WORKER_FLAVOR` | `cx2.2x4` (IKS), `bx2.4x16` (ROKS) | all | VPC worker flavor; ROKS needs at least 4 vCPU and 16 GB. Immutable: to change it, add a new pool and delete the old one. |
| `WORKER_UPDATE_POLICY` | `Manual` | all | `Manual`: workers with a newer fix pack are replaced only after `spec.rollout.after` of the pool; `Automatic`: as soon as IBM offers the fix pack. |
| `DELETION_PERSISTENT_STORAGE` | `Retain` | all | What CAPIBM asks IBM to do with the persistent storage of the cluster, such as the VPC Block volumes of its persistent volumes, when the Cluster is deleted: `Retain` asks IBM to keep it; `Delete` asks IBM to delete it, and deleted data cannot be recovered, but IBM's documentation conflicts on whether VPC Block volumes go with the cluster, so check for them after the deletion and delete those that remain. Sets `spec.deletion.persistentStorage` of the control plane, which can still be changed before the Cluster is deleted. The ROKS registry bucket is kept either way. |

### Notes

* Pass `--worker-machine-count`: `clusterctl generate cluster` sets `WORKER_MACHINE_COUNT` to 0 when neither the flag
  nor the variable is given, and the defaults above apply only where the variable is not set at all (for example
  `clusterctl generate yaml`). A `MachinePool` with 0 replicas cannot create the IBM cluster; the control plane reports
  it.
* `--kubernetes-version` and `--control-plane-machine-count` are not used: IBM runs the control plane, and the version
  comes from `IKS_DISTRIBUTION_VERSION` or `ROKS_DISTRIBUTION_VERSION`.
* Each template has exactly one `IBMCloudManagedMachinePool`, with `poolName: default` and `creation: WithCluster`: IBM
  creates it together with the cluster. Further pools are added as more `MachinePool` and `IBMCloudManagedMachinePool`
  pairs without `creation` (or with `creation: AfterCluster`).
* No template sets `spec.ibmCloudClusterName`, so the IBM cluster is named after the Cluster, and creating the control
  plane returns an admission warning: IBM cluster names are unique per account, while Cluster names are unique only per
  namespace. To choose a name, set `spec.ibmCloudClusterName` (immutable) in the generated control plane. Without it,
  the Cluster name must follow IBM's cluster name rule, at most 32 characters with a letter first (then letters,
  digits, `-`, `_` and `.`): the API server rejects the control plane of a Cluster whose name breaks the rule.
* No template sets `spec.clusterNetwork`: IBM then picks free pod and service ranges. Custom ranges cannot be shared
  between clusters; the book page
  [Creating a cluster](../docs/book/src/topics/managed/creating-a-cluster.md#pod-and-service-networks) shows how to set
  them.
* Keep the quotes when editing a generated cluster: an unquoted `distributionVersion: 1.30` is the YAML number 1.3,
  which the API server rejects, and an unquoted Cloud Object Storage CRN ends in `::`, which is not valid YAML.

### How these templates are built

The four `cluster-template-managed-*.yaml` files are hand-written, like `cluster-template-vpc-with-network-spec.yaml`,
and `make generate-templates` does not touch them: a kustomize build would drop the quotes above. Their comments only
point to the pages of the book ([docs/book/src/topics/managed](../docs/book/src/topics/managed)), which explain the
settings.

Several flavors carry the same document, and every copy must stay byte-identical, comments included:

| Document | Flavors that share it |
|---|---|
| `Cluster` | all four |
| `IBMCloudManagedCluster` | `managed-iks` and `managed-roks` (three zones); `managed-iks-single-zone` and `managed-roks-single-zone` (one zone) |
| `IBMCloudManagedControlPlane` | `managed-iks` and `managed-iks-single-zone`; `managed-roks` and `managed-roks-single-zone` |

Make an edit to a shared document in every flavor that has it: `TestManagedTemplatesShareDocuments` in
`webhooks/managed/admission/templates_test.go` fails otherwise and names the flavors and the first line that differs.
