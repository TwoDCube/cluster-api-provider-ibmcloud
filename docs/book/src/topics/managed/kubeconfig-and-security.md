# Kubeconfig and security

## The kubeconfig Secret

The control plane writes the Cluster API kubeconfig Secret:

* name `<cluster>-kubeconfig` in the Cluster's namespace, type `cluster.x-k8s.io/secret`, label
  `cluster.x-k8s.io/cluster-name`, key `value`;
* a controller owner reference to the `IBMCloudManagedControlPlane`, so it is garbage-collected with the control plane
  and `clusterctl move` carries it along;
* contents: one cluster, one user, one context. The server is IBM's master URL. The user holds IBM's **admin client
  certificate** and key inline (`client-certificate-data`, `client-key-data`), on IKS **and** ROKS. IKS kubeconfigs
  embed the cluster CA; ROKS kubeconfigs embed none, because the ROKS master serves a publicly trusted certificate. No
  token, `exec` plugin, file path or `insecure-skip-tls-verify` is ever written.

CAPIBM fetches the credential from IBM's admin kubeconfig API and **verifies** every new kubeconfig (a
SelfSubjectReview and `/version` against the master) before it writes it; a credential that fails never replaces a
working one (`KubeconfigReady=False`, reason `VerificationFailed`). The control plane is initialized only after a
verified Secret exists, so Cluster API, `clusterctl get kubeconfig` and `clusterctl move` always see a working one.

ROKS clusters use the same IBM admin client certificate as IKS clusters; there is no other credential type.

### What the admin certificate is

* Subject `O=ibm-admins, CN=KubeCert#<caller-iam-id>`; the Kubernetes groups are `ibm-admins` and
  `system:authenticated`. It is **cluster-admin**.
* It is valid for **two years** from its first fetch, and IBM returns the **same** certificate on every later fetch.
* It keeps working after the IAM credentials that fetched it are removed or rotated.

### Refresh

CAPIBM refreshes the credential only when there is a reason, never per reconcile:

* when the Secret is missing or IBM's master URL changed;
* when IBM rotates the cluster CA (`KubeconfigReady` is `Unknown`, reason `CARotationInProgress`, while the rotation
  runs; CAPIBM checks every minute);
* after the stored credential is answered with 401 (with a backoff from 1 to 10 minutes);
* at half of its lifetime (about one year).

When IBM returns the certificate already stored, nothing is written except the `refresh-checked-at` annotation, and the
next attempt is a day later. Thirty days before the certificate expires, while IBM still serves the same one, CAPIBM
keeps `KubeconfigReady=True`, warns in the message and emits the Warning event `AdminCertificateExpiring`. The CA is
re-read every 12 hours and before it expires. Every steady reconcile also probes the stored Secret; a probe failure
makes `Available` `False` (`WorkloadAPIUnreachable`).

If IBM's master URL changes after the cluster was created (for example to the port-443 host names that ROKS 4.22
added), the kubeconfig follows IBM and `KubeconfigReady` says `EndpointChanged`; `spec.controlPlaneEndpoint` and the
Cluster's endpoint are never changed, and Cluster API uses the Secret, so the old value is informational only.

### Annotations on the Secret

CAPIBM keeps its bookkeeping in annotations with the prefix `ibmcloudmanaged.controlplane.cluster.x-k8s.io/`; none of
them holds secret material:

| Annotation | Meaning |
|---|---|
| `cluster-id` | the IBM cluster the credential belongs to; the durable record of the binding that `clusterctl move` carries |
| `account-id` | the IBM account of the cluster, taken from the cluster CRN; named in `IBMClusterNotVisible` messages |
| `server` | the master URL the kubeconfig uses |
| `credential-type`, `credential-not-before`, `credential-not-after`, `credential-fingerprint` | the stored certificate |
| `fetched-at`, `refresh-checked-at` | when the credential was fetched, and when a refresh last returned the same one |
| `ca-mode`, `ca-fingerprint`, `ca-not-after`, `ca-fetched-at` | how the master's certificate is trusted (cluster CA on IKS, system roots on ROKS) and the stored CA |
| `ca-rotation-marker` | IBM's CA rotation status the Secret was built for |
| `kubeconfig-unauthorized-since` | start of a run of 401 answers (RFC 3339); removed on the next successful probe |

Do not edit them. A forged value cannot hand out a credential: CAPIBM still checks every candidate cluster and proves
possession before it trusts a `cluster-id` ([Trust model](./trust-model.md)).

## A kubeconfig Secret you provide

A Secret named `<cluster>-kubeconfig` that exists before CAPIBM writes one and whose type is **not**
`cluster.x-k8s.io/secret` is treated as user-provided and never modified. It must carry the
`cluster.x-k8s.io/cluster-name` label and a non-empty `value` key, because Cluster API's ClusterCache reads only labelled
kubeconfig Secrets; otherwise `KubeconfigReady=False`, reason `UserProvidedSecretInvalid`, names what is missing. When
valid, `KubeconfigReady=True`, reason `UserProvidedSecret`, and the control plane is initialized after the Secret's
credential passes the probe.

**Use an inline client certificate.** After `clusterctl move` or a restore, CAPIBM proves which IBM cluster the Secret
belongs to with the Secret's own credential ([clusterctl move](./clusterctl-move.md)). It can do that only with an
inline client certificate and key (`client-certificate-data`, `client-key-data`), as IBM's admin kubeconfig has. A
Secret whose user holds a bearer token (or an `exec` plugin, an auth provider or file paths) cannot be proven: the
control plane stops in `OwnershipNotProven` with a message that a client-certificate kubeconfig (`ibmcloud ks cluster
config --admin`) is needed. To produce one:

```console
ibmcloud ks cluster config --cluster <cluster-name-or-id> --admin
kubectl config view --minify --flatten --context <context-of-that-cluster> > admin.kubeconfig
# check: the user entry has client-certificate-data and client-key-data, and no token
```

`--flatten` inlines certificate files, so the result carries the `*-data` fields.

## Documented deviation from the Cluster API contract

The Cluster API control plane contract says (`control-plane.md`, Cluster API v1.14.0):

> "Important! If a control plane provider uses client certificates for authentication in these Kubeconfigs, the client
> certificate MUST be kept with a reasonably short expiration period and periodically regenerated to keep a valid set
> of credentials available. As an example, the Kubeadm Control Plane provider uses a year of validity and refreshes
> the certificate after 6 months."

IBM's admin certificate is valid for two years from its first fetch, and IBM returns the same certificate on repeated
fetches. CAPIBM can neither shorten IBM's lifetime nor make IBM issue a new certificate, so fetching more often would
not shorten the exposure. It is the one static credential IBM issues for both IKS and ROKS, it works through the public
master, and it needs only the role that creating clusters already requires. CAPIBM therefore refreshes at half the
lifetime (Kubeadm's ratio), after a CA rotation and after a 401, verifies every new credential before writing it, and
documents the deviation with these mitigations.

### Mitigations

* **Restrict the cluster API with context-based restrictions.** A CBR rule on the cluster API type (`api-type:cluster`)
  that allows only the management cluster's egress addresses (and the networks your administrators use) makes a leaked
  certificate useless elsewhere
  ([IBM docs](https://cloud.ibm.com/docs/containers?topic=containers-cbr)).
* **Keep RBAC on the Secret tight.** Only the CAPI and CAPIBM controllers and the few people who need cluster-admin
  should be able to read `<cluster>-kubeconfig`; the contract also says the Secret must not be shared. Remember that
  anyone who can create pods or read Secrets in the namespace can read it, and that every fetch that returns a new
  certificate leaves one more valid credential in etcd history, snapshots and backups. Treat backups of the management
  cluster as cluster-admin credentials.
* **CA rotation is the only revocation.** Rotating the cluster CA invalidates every admin certificate issued before,
  whoever fetched it, also after the IAM credentials that fetched it were removed. Follow IBM's CA rotation procedure
  (`ibmcloud ks cluster ca create`, then `ibmcloud ks cluster ca rotate`). IBM's procedure replaces the VPC workers;
  with CAPIBM, set [`spec.rollout.after`](./upgrades.md#rolling-workers-on-request-specrolloutafter) on each pool to
  replace them drained. CAPIBM detects the rotation, holds master upgrades while it runs (`BlockedByCARotation`) and
  fetches the new credential afterwards.
* **Never copy the Secret to move a cluster.** Copying it hands over cluster-admin. Use
  [clusterctl move](./clusterctl-move.md).

## No credentials in logs

CAPIBM never logs the kubeconfig or its key. Condition messages carry IBM error **codes** only (for example "IBM code
P4102"); IBM's error descriptions and incident IDs go to the controller log and, for the non-critical errors of a
create, to the `IBMCreateWarnings` events. No message, event or annotation carries an IAM ID, e-mail address or key
fingerprint: messages name the credentials CAPIBM acts as "the CAPIBM controller identity".
