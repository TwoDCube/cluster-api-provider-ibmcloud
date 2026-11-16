# Trust model

In this book, **identity** always means the IBM Cloud IAM credentials CAPIBM authenticates as. Which IBM cluster an
`IBMCloudManagedControlPlane` stands for is its **binding**.

## One trust domain per management cluster

CAPIBM uses one IBM Cloud API key per installation, normally a service ID's. Anyone who can create or change
`IBMCloudManagedControlPlane`, `IBMCloudManagedCluster` or `IBMCloudManagedMachinePool` objects, or Secrets, in a
namespace that CAPIBM watches can make CAPIBM create, scale, upgrade and delete IBM clusters in every resource group
that service ID can reach. This is the same as for CAPIBM's VPC and PowerVS providers today.

Restrict it with:

* the **IAM scope** of the service ID (its roles, limited to the resource groups it should use; see
  [Prerequisites](./prerequisites.md#iam-permissions));
* Kubernetes RBAC on the managed kinds and on Secrets in the watched namespaces;
* the manager flags `--watch-filter` (only Cluster API objects with the matching `cluster.x-k8s.io/watch-filter` label)
  and `--namespace` (one namespace only).

## No reach into clusters the namespace does not own

Unlike the VPC and PowerVS providers, this provider hands out credentials (the admin kubeconfig). It therefore never
acts on an existing IBM cluster because of something a namespace user can write:

* **Trusted**: object status, written only by controllers, and IBM's own answers. Do not grant users `update` on the
  `status` subresource of these kinds.
* **Not trusted**: Secret annotations and contents, and spec values at create time (including
  `spec.controlPlaneEndpoint`, which a user can set on a new object).
* CAPIBM binds a control plane to an IBM cluster only if it created that cluster itself (the ID from its own create
  answer, or a lost answer matched against its own recorded create attempts within a bounded time window), or if the
  namespace's kubeconfig Secret **proves possession** of a cluster-admin credential for that cluster: the Secret's
  credential must pass a cluster-admin access review against that cluster's IBM master URL, with TLS verified. Every
  cluster found by ID or by name must also have the expected name, resource group, distribution and provider.
* A same-named IBM cluster that CAPIBM cannot prove it created ends in `ClusterNameConflict`; it is never adopted,
  changed or deleted.
* Residual risk: someone who already holds cluster-admin on a workload cluster, without IBM rights, could make CAPIBM
  adopt that cluster into a namespace they control. The proof may later be tightened to IBM's admin group
  `ibm-admins`; it never depends on which IAM identity fetched the credential.

## No multi-tenancy in this release

Different teams with different IBM credentials on one management cluster are **not** a supported setup. There is no
per-cluster credential reference, no list of allowed namespaces and no resource-group restriction flag. Teams that need
separation run one management cluster per trust domain, or one namespace-scoped CAPIBM installation (`--namespace`,
`--watch-filter`) with its own credentials per trust domain; Cluster API documents several provider instances as its
best-effort alternative to multi-tenancy.

## What later releases keep compatible

The code is prepared so that per-cluster credentials can be added later without breaking anything that works today:

* A future `identityRef` will be **optional** and sit on the InfraCluster (`IBMCloudManagedCluster`; later the same rule
  for `IBMVPCCluster` and `IBMPowerVSCluster`); the control plane and the pools inherit it. **Omitted means the
  controller's credentials, forever**: a resolution rule, never a value written into your spec, so GitOps manifests
  without it stay valid.
* The tenancy target is your choice: tenants may share a resource group, use different resource groups or different
  accounts. CAPIBM makes no assumption about which account or resource group a set of credentials reaches; IAM decides.
  Each tenant would supply either an API key or a trusted profile.
* IAM objects (policies, service IDs, API keys, trusted profiles, cross-account or service-to-service trust) are created
  and changed only by you, never by CAPIBM. This book documents the permissions CAPIBM needs.
* These names are reserved for that work and are not used by anything in this release: `spec.identityRef` and the type
  `IBMCloudIdentityReference` on the InfraCluster kinds, the cluster-scoped kind `IBMCloudClusterIdentity` (short name
  `ibmci`) and its finalizer, the condition `IdentityReady`, the manager flag `--controller-identity`, the Secret
  annotation `ibmcloudmanaged.controlplane.cluster.x-k8s.io/credential-identity-key`, and
  `spec.clusterComponentsIdentity` on `IBMCloudManagedControlPlane`.

## The IKS infrastructure key

IKS binds one infrastructure "cluster API key" per (region, resource group) to the identity that created the first
cluster there, or that last ran `ibmcloud ks api-key reset`, and orders the workers, load balancers and volumes of
every cluster there with it. Consider this when you plan resource groups:

* Clusters that share a region and resource group share that key's rights, whoever created them.
* `ibmcloud ks api-key info --cluster <cluster-name-or-id>` shows whose key is in use.
* `ibmcloud ks api-key reset --region <region>` (with the resource group targeted) re-binds every cluster of that region
  and resource group to the caller's credentials.
* CAPIBM never calls `api-key reset` and does not check who owns the key. If the stored key lacks permissions, IBM
  refuses a create with 403 and the `PermissionDenied` message names both commands.

See [Prerequisites](./prerequisites.md#the-infrastructure-key-of-a-region-and-resource-group) for the options.

## Revocation

CA rotation is the only way to revoke the admin certificates CAPIBM stored, also after the credentials that fetched
them were removed or rotated ([Kubeconfig and security](./kubeconfig-and-security.md#mitigations)).
