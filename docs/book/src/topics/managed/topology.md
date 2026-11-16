# Topology and flavors

CAPIBM ships four clusterctl flavor templates for managed clusters. They are plain templates (not ClusterClass) and
each one creates a `Cluster`, an `IBMCloudManagedCluster`, an `IBMCloudManagedControlPlane`, one `MachinePool` and one
`IBMCloudManagedMachinePool`: the cluster's initial worker pool, `poolName: default` with `creation: WithCluster`.

| Flavor | Distribution | Zones × workers per zone | `--worker-machine-count` | Default worker flavor | HA |
|---|---|---|---|---|---|
| `managed-iks` | Kubernetes (IKS) | 3 × 2 | 6 | `cx2.2x4` | yes, IBM's HA topology |
| `managed-roks` | OpenShift (ROKS) | 3 × 2 | 6 | `bx2.4x16` | yes, IBM's HA topology |
| `managed-iks-single-zone` | Kubernetes (IKS) | 1 × 2 | 2 | `cx2.2x4` | **no**: development and tests |
| `managed-roks-single-zone` | OpenShift (ROKS) | 1 × 2 | 2 | `bx2.4x16` | **no**: development and tests |

## Choosing a flavor

* Production: `managed-iks` or `managed-roks`. Give the cluster three zones of the region, each with an existing subnet.
* Development and tests: `managed-iks-single-zone` or `managed-roks-single-zone`. They read only the first zone and
  subnet variables, so one set of variables serves all four flavors.
* Fewer zones are **not** a variable of the HA flavors: clusterctl's variable substitution can neither drop a list
  entry nor take a default that contains `}`, so the single-zone topology is its own flavor. Fewer workers are a
  variable (`WORKER_MACHINE_COUNT`, a multiple of the zone count).

## Variables

| Variable | Read by | Meaning |
|---|---|---|
| `IBMCLOUD_REGION` | all | VPC region, for example `us-south` |
| `IBMCLOUD_RESOURCE_GROUP_ID` | all | ID of the existing resource group (32 hexadecimal characters) |
| `IBMCLOUD_VPC_NAME` | all | name of the existing VPC in the region |
| `IBMCLOUD_ZONE_1`, `IBMCLOUD_SUBNET_NAME_1` | all | first zone and its existing subnet |
| `IBMCLOUD_ZONE_2`, `IBMCLOUD_SUBNET_NAME_2`, `IBMCLOUD_ZONE_3`, `IBMCLOUD_SUBNET_NAME_3` | HA flavors | second and third zone and their subnets |
| `IKS_DISTRIBUTION_VERSION` | IKS flavors | Kubernetes minor, `MAJOR.MINOR` only (default `1.36`) |
| `ROKS_DISTRIBUTION_VERSION` | ROKS flavors | OpenShift minor, `MAJOR.MINOR` only (default `4.21`) |
| `COS_INSTANCE_CRN` | ROKS flavors | CRN of the standard Cloud Object Storage instance for the internal registry (required) |
| `WORKER_MACHINE_COUNT` | all | total workers of the initial pool across its zones; pass it as `--worker-machine-count` (6 for the HA flavors, 2 for the single-zone flavors). `clusterctl generate cluster` sets it to 0 when neither the flag nor the variable is given, so the template default (6 or 2) applies only to other renderers such as `envsubst` |
| `IBMCLOUD_WORKER_FLAVOR` | all | VPC worker flavor (default `cx2.2x4` for IKS, `bx2.4x16` for ROKS; ROKS needs at least 4 vCPU and 16 GB) |
| `WORKER_UPDATE_POLICY` | all | `Manual` (default) or `Automatic` fix-pack updates ([Upgrades](./upgrades.md#worker-fix-packs-and-the-update-policy)) |

The templates carry no credential variable: the API key belongs to the manager ([Prerequisites](./prerequisites.md)).
They set neither `Cluster.spec.clusterNetwork` (IBM picks free pod and service ranges) nor `ibmCloudClusterName`
(the IBM cluster is named after the Cluster, with an admission warning; see
[Creating a cluster](./creating-a-cluster.md#naming-the-ibm-cluster)). `templates/README.md` lists every variable.

## IBM's HA topology: 3 zones × 2 workers per zone

Both HA flavors default to the topology IBM recommends for both IKS and ROKS:

* "Plan to have at least two worker nodes per zone, so six nodes across three zones in total"
  ([IBM docs: "Creating a highly available cluster strategy"](https://cloud.ibm.com/docs/containers?topic=containers-strategy),
  `strategy.md`, last updated 2026-11-05, in both the IKS and the OpenShift docs; the same page ties full coverage
  under IBM's HA SLA to "at least 6 worker nodes, two worker nodes per zone that are evenly spread across three
  zones").
* "Ingress requires at least two worker nodes per zone to ensure high availability and apply periodic updates" (IBM
  docs `ts-ingress-alb-pods-nodes.md`, last updated 2026-04-06): ALB pods have anti-affinity, one per worker.
* "By default, each ALB has 2 replicas" (IBM docs `cs_managed_traefik_ingress_alb.md`, last updated 2026-10-12), which
  is why one worker per zone leaves an ALB replica unscheduled.

It is also the smallest topology in which every worker rollout passes CAPIBM's capacity check on both distributions and
every zone keeps a worker while one worker is replaced ([Upgrades](./upgrades.md#capacity-the-1-worker-a-rollout-needs)).

## Smaller topologies are not HA

The single-zone flavors and every topology smaller than 3 zones × 2 workers are **not highly available**:

* A zone outage stops every worker of a single-zone cluster.
* IBM reports the cluster's **ingress status as `warning` or `critical`**. This is expected and informational: it
  always happens with fewer than 2 workers per zone, and also on any topology without a public gateway. CAPIBM never
  looks at the ingress status; it does not affect `Available` or any other condition.
* A **1-worker cluster can never replace a worker**: IKS keeps at least 1 worker, so fix packs, the automatic minor
  follow and `rollout.after` wait in `WaitingForCapacity`, and after two master minors the next master upgrade is
  blocked by the worker skew (`BlockedByWorkerSkew`). `WORKER_MACHINE_COUNT=1` (IKS single zone only) gives such a
  cluster.
* An IKS cluster needs at least 2 workers for a rollout, a ROKS cluster at least 3 (ROKS keeps at least 2). A
  `managed-roks-single-zone` cluster therefore needs a temporary third worker for every rollout; the
  `WaitingForCapacity` message names the MachinePool size that allows it.
* ROKS keeps **at least 2 workers per zone** in the initial pool at create time, so the smallest `managed-roks`
  cluster has 6 workers (2 in each of 3 zones), and fewer ROKS workers mean fewer zones, that is
  `managed-roks-single-zone`.
* With 3 zones, `WORKER_MACHINE_COUNT=3` gives one IKS worker per zone (not HA: ingress `warning`).

## Zones are chosen at create time

* Zones can be **added** to an `IBMCloudManagedCluster` later, never removed. A pool's zones are immutable: to use a new
  zone, add it to the `IBMCloudManagedCluster` and create a new pool there.
* For **ROKS**, give the initial pool (`creation: WithCluster`) every zone the cluster will ever use: IBM extends the
  OpenShift router's load balancer only to the zones of the cluster at create time. A `managed-roks-single-zone`
  cluster keeps single-zone router ingress for its whole lifetime.
