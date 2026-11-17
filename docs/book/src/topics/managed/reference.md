# Conditions, events and annotations

Condition types and reasons are fixed CamelCase tokens that tooling can match. IBM's own text appears only in
messages, normalised, and an IBM error appears there as its code ("IBM code P4102"), never with IBM's incident ID, which
changes with every answer (it is in the controller log). Messages name the object, the numbers and the fix.

`Deleting` follows the Cluster API v1beta2 polarity: `False` with reason `NotDeleting` while the object has no
deletion timestamp, `True` with the specific reason while it is being deleted. `Paused` is set by Cluster API's
`util/paused` for a paused Cluster (`Cluster.spec.paused` or the `cluster.x-k8s.io/paused` annotation); while paused,
CAPIBM makes no IBM call and writes nothing to the workload cluster.

## IBMCloudManagedControlPlane

| Condition | Status | Reason | Meaning |
|---|---|---|---|
| `Ready` | summary | | of `IBMClusterProvisioned`, `KubeconfigReady` and `Available` |
| `Available` (contract; Cluster API mirrors it to the Cluster's `ControlPlaneAvailable`) | True | `Available` | initialized, the last probe of the stored kubeconfig passed, the IBM cluster exists and its master health is not `error` or `unavailable`; IBM's ingress status is never consulted |
| | False | `WaitingForIBMCluster` | no IBM cluster yet, or it is still provisioning |
| | False | `MasterNotReady` | IBM does not report the master ready (state `deployed`, status "Ready", a master URL), or its health is `error` or `unavailable` |
| | False | `WorkloadAPIUnreachable` | the probe with the stored kubeconfig fails (network, TLS, or a 401 while the credential is refreshed) |
| | False | `Deleting` | the control plane is being deleted |
| `IBMClusterProvisioned` | True | `Provisioned` | the IBM cluster exists and is bound to this object; lists non-critical create error codes, if any ("IBM created the cluster with non-critical errors E49df (see the IBMCreateWarnings events)") |
| | False | `WaitingForInfrastructure` | the `IBMCloudManagedCluster` has not resolved the VPC and subnets yet |
| | False | `WaitingForInitialWorkerPool` | no pool of the Cluster has `spec.creation: WithCluster`, or the marked pool is not linked to its `MachinePool` yet; no IBM call |
| | False | `InvalidConfiguration` | a name that breaks IBM's cluster name rule (1 to 32 characters, a letter first, then letters, digits, `-`, `_` and `.`; checked before any IBM call), invalid pod or service ranges, or initial pool (including several pools with `spec.creation: WithCluster`); IBM 400 and 422 answers by code (`P4102`, `E7278`) |
| | False | `VersionNotAvailable` | IBM does not offer `spec.distributionVersion`; the message lists the offered minors |
| | False | `ClusterNameConflict` | an IBM cluster with the name exists and CAPIBM cannot prove it created it for this Cluster; nothing is created, changed or deleted |
| | False | `BindingMismatch` | `status.clusterID` names an IBM cluster whose name, resource group, distribution or provider differ; nothing is changed |
| | False | `OwnershipNotProven` | the kubeconfig Secret's `cluster-id` names a cluster, but its credential does not pass the cluster-admin access review there (the message names the failing check), or the Secret holds no inline client certificate |
| | False | `Creating` | IBM reports `requested` or `deploying`, or CAPIBM waits for IBM to list a cluster it just created |
| | False | `CreateFailed` | IBM reports `create_failed` or `deploy_failed`: delete this Cluster and create it again (never deleted automatically) |
| | False | `IBMClusterNotFound` | a known cluster ID or the endpoint exists, but IBM returns 404; never re-created |
| | False | `DeletedOutOfBand` | IBM reports `deleting`, `deleted`, `aborted` or `delete_failed` while the Cluster is not being deleted |
| | False | `PermissionDenied` | IBM 403 or `A0656`; the message names the missing role and, for creates, `ibmcloud ks api-key info` and `api-key reset`; checked again every 15 minutes |
| | False | `IBMAPIError` | any other classified IBM error: the operation, the HTTP status and IBM's code |
| `KubeconfigReady` | True | `Ready` | the verified `<cluster>-kubeconfig` Secret exists; within 30 days of the certificate's expiry the message warns |
| | True | `UserProvidedSecret` | a user-provided Secret is valid and respected, never modified |
| | True | message `EndpointChanged` | IBM's master URL differs from `spec.controlPlaneEndpoint`; the kubeconfig follows IBM |
| | False | `WaitingForMaster` | the master is not ready yet |
| | False | `CredentialFetchFailed` | IBM's admin kubeconfig or CA could not be fetched, or its user has no client certificate (or has a token, `exec` or auth provider) |
| | False | `VerificationFailed` | a new credential failed the check against the master with every trust mode (the old Secret is kept), the stored credential is rejected with 401 (fetched again with a backoff of 1 to 10 minutes), or the master's certificate no longer verifies with the stored trust (both trust modes are tried again at most once every 10 minutes) |
| | False | `EndpointUnreachable` | "The management cluster cannot reach <url> (...); private-only clusters need private connectivity" |
| | Unknown | `CARotationInProgress` | IBM rotates the cluster CA; checked every minute |
| | False | `UserProvidedSecretInvalid` | a user-provided Secret lacks the `cluster.x-k8s.io/cluster-name` label or a non-empty `value` key |
| `MasterVersionUpToDate` | True | `UpToDate` | the master runs the minor of `spec.distributionVersion`, whatever patch; an end of support within 45 days is mentioned; while IBM offers newer minors (`status.availableDistributionVersions`) the message ends with "update available: IBM offers <minors>; raise spec.distributionVersion to update", which never makes the condition `False` |
| | False | `UpgradePending` | a requested minor update is in flight at IBM |
| | False | `UpgradeFailed` | the requested update has not taken effect and IBM shows no update in progress; the message names the retry time. Also after a refused `UpdateMaster` whose `PermissionDenied` a failed listing (below) replaced, with the same retry time |
| | False | `VersionSkipNotAllowed` | `spec.distributionVersion` is more than one minor above the master |
| | False | `DowngradeNotAllowed` | `spec.distributionVersion` is below the master minor |
| | False | `VersionNotAvailable` | IBM does not offer the requested minor |
| | False | `BlockedByCARotation` | a CA rotation is in progress |
| | False | `BlockedByWorkerSkew` | workers lag the target minor by more than IBM allows (2 minors on IKS, 1 on ROKS) |
| | False | `BlockedByClusterState` | IBM's cluster state or master health blocks the update |
| | False | `PermissionDenied` | IBM refused `UpdateMaster` with 403; the message names the Kubernetes Service role and the retry time (up to 24 hours after repeated refusals; no manual trigger) |
| | False | `PermissionDenied` | after a raise of `spec.distributionVersion`, IBM refused with 403 a call the update needs first, the listing of the versions IBM offers or of the cluster's workers: "IBM Cloud Kubernetes Service refused listing the workers of IBM cluster <id> with HTTP 403 (IBM code <c>), so the master update to <minor> waits: the credentials of the CAPIBM controller identity need the Administrator platform role and the Manager service role of Kubernetes Service on the cluster's resource group; CAPIBM tries again at the next poll of the control plane" (for the version listing: "listing the versions IBM Cloud offers in region <region>"); no `UpdateMaster` is sent |
| | False | `IBMAPIError` | after a raise of `spec.distributionVersion`, IBM answered one of those listings with 400 or 404: "IBM Cloud Kubernetes Service refused listing the versions IBM Cloud offers in region <region> with HTTP 400 (IBM code <c>), so the master update to <minor> waits; CAPIBM tries again at the next poll of the control plane, and the controller log has IBM's description"; a listing CAPIBM could not send at all reads "CAPIBM could not complete <call>, so the master update to <minor> waits; CAPIBM tries again at the next poll of the control plane, and the controller log names the cause"; no `UpdateMaster` is sent |
| | Unknown | `UnparsableMasterVersion` | IBM reports a master version CAPIBM cannot parse: "IBM reports master version "<value>" for IBM cluster <id>, which CAPIBM cannot parse, so it cannot tell whether the master runs spec.distributionVersion <minor>; CAPIBM requests no master update until IBM reports a version it can parse"; `RollingOut` is left as it is |
| `RollingOut` (contract; aggregated into the Cluster's `RollingOut`) | True | `MasterUpdateInProgress` | IBM updates the master, for a requested minor or for a patch IBM applies on its own ("IBM is applying master patch <target>") |
| | False | `NotRollingOut` | no master update in progress |
| `Deleting` | False | `NotDeleting` | not being deleted |
| | True | `WaitingForWorkersDeletion` | `MachinePool`s of the Cluster still exist; nothing is deleted yet |
| | True | `WaitingForInfrastructure` | the `IBMCloudManagedCluster` is gone while an IBM cluster may exist; the finalizer is kept |
| | True | `DeletionRequested` | `DELETE` was sent |
| | True | `WaitingForIBMDeletion` | IBM deletes the cluster (also after IBM answered a `DELETE` with 400 or 409); or IBM answers 404 for a known cluster before CAPIBM sent its `DELETE` and the stored kubeconfig could not be checked against it (a timeout or a 5xx): "IBM answers 404 for cluster <id>, and its kubeconfig Secret could not be checked against it (...), so CAPIBM cannot yet tell a cluster that the credentials of the CAPIBM controller identity cannot see from one that is gone; it checks again every minute until <time> and then counts the cluster as gone" (30 minutes after the deletion started); or IBM does not list yet a cluster that a create request of CAPIBM may have created, whose ID IBM returned ("IBM Cloud does not list cluster <id> yet; CAPIBM sent its create request at <time> and waits until <time> for IBM to list it before it counts the cluster as gone") or whose answer was lost; the finalizer stays until 30 minutes after that create request |
| | True | `DeleteFailed` | IBM reports `delete_failed`; open an IBM support case |
| | True | `PossibleOwnClusterFound` | a same-named cluster may come from a create attempt with an unknown outcome; the finalizer is kept and `DELETE` is never sent for it |
| | True | `OwnershipNotProven` | the cluster to delete cannot be proven to be this control plane's: the Secret's `cluster-id` names it and a check or the proof fails, or `status.clusterID` names it and it does not match (then a new Secret does not help; [What to do](./deleting.md#when-capibm-does-not-delete)); never deleted |
| | True | `IBMClusterNotVisible` | IBM returns 404 for a known cluster while the stored kubeconfig still works: "The credentials of the CAPIBM controller identity cannot see IBM cluster <id> in account <account-id>, but its kubeconfig Secret still authenticates against it; check the account and IAM scope of those credentials. CAPIBM keeps the finalizer ... and never deletes a cluster it cannot see; remove the finalizer by hand to orphan the IBM cluster"; once reported, it stays (checked every 15 minutes) also while the kubeconfig cannot be checked ("... its kubeconfig Secret authenticated against it before but cannot be checked now (...); ...") until the kubeconfig is rejected or the finalizer is removed by hand |
| | True | `PermissionDenied` | IBM refused the `DELETE` with 401 or 403; checked again every 15 minutes |
| `Paused` | | | `util/paused` |

Events: `SuccessfulCreateIBMCluster` (Normal); `IBMCreateWarnings` (Warning, one per non-critical create error, with
IBM's code, description, recovery hint and incident ID), `LeftoverResources` (Warning, once the IBM cluster is gone),
`IBMClusterNotDeleted` (Warning, a same-named IBM cluster the deletion leaves alone), `AdminCertificateExpiring`
(Warning, 30 days before the admin certificate expires), `ManagedKubernetesDisabled` (Warning, the feature gate is
off).

## IBMCloudManagedCluster

| Condition | Status | Reason | Meaning |
|---|---|---|---|
| `Ready` | summary | | of `NetworkReady` (informational messages: no public gateway, fewer than 16 free addresses in a subnet) |
| `NetworkReady` | True | `Ready` | the resource group, the VPC and every zone's subnet resolved and match |
| | False | `ResourceGroupNotFound` | no resource group with `spec.resourceGroup.id` is visible |
| | False | `VPCNotFound` | no such VPC in the region |
| | False | `SubnetNotFound` | a zone's subnet does not exist |
| | False | `SubnetNotInVPC` | a zone's subnet belongs to another VPC |
| | False | `SubnetZoneMismatch` | a zone's subnet lies in another zone |
| | False | `SubnetNameAmbiguous` | a subnet name matches several subnets; give its ID |
| | False | `IBMAPIError` | any other classified IBM error, or credentials CAPIBM cannot resolve; the message names the call and, when IBM answered it, the HTTP status. When the IAM token service rejected the credentials before the call was sent, the message says so and has neither an HTTP status nor an IBM code ([what to do](./troubleshooting.md#the-network-check-fails-ibmcloudmanagedcluster)) |
| `Paused` | | | `util/paused` |

The `IBMCloudManagedCluster` has no finalizer and no events; a stand-alone delete is guarded by its webhook.

## IBMCloudManagedMachinePool

| Condition | Status | Reason | Meaning |
|---|---|---|---|
| `Ready` | summary | | of `WorkerPoolReady` only: a fix pack waiting under the `Manual` policy does not make a healthy pool look broken |
| `WorkerPoolReady` | True | `Ready` | the size per zone matches, every worker is ready, the count equals the desired replicas |
| | True | `ExternallyAutoscaled` | IBM reports the pool as autoscaled; CAPIBM does not resize it |
| | False | `WaitingForControlPlane` | no IBM cluster ID yet; or the control plane is not initialized (a `creation: WithCluster` pool waits for IBM to list it: "IBM creates pool <name> together with cluster <id> (spec.creation: WithCluster); waiting for it to be listed") |
| | False | `WaitingForInfrastructure` | a zone of the pool is not yet resolved by the `IBMCloudManagedCluster` |
| | False | `UnknownZone` | a zone of the pool is not a zone of the `IBMCloudManagedCluster` |
| | False | `ReplicasNotDivisibleByZones` | "replicas=4 is not a multiple of 3 zones; use 3 or 6 (IBM sizes worker pools per zone)" |
| | False | `ZeroReplicasNotSupported` | `replicas: 0` |
| | False | `InvalidForDistribution` | a setting the distribution does not support, such as `entitlement` on IKS |
| | False | `PoolNameConflict` | another pool object of the Cluster uses the same `poolName`; set on every claimant, none publishes or changes anything |
| | False | `PoolConflict` | the IBM pool with that name has another flavor; not CAPIBM's: nothing published, changed or removed |
| | False | `WaitingForClusterState` | IBM's cluster state does not allow the change (`unavailable`, `scaled_down`; `unsupported` for create, grow and replace) |
| | False | `PoolNameBeingReleased` | IBM still lists a deleting or deleted pool with that name |
| | False | `Creating` | the IBM pool is being created |
| | False | `Provisioning` | workers of the pool provision |
| | False | `Resizing` | IBM resizes the pool |
| | False | `WorkersNotReady` | some workers of the pool are not ready |
| | False | `WaitingForSettledCluster` | another worker of the Cluster is changing, or the 3-minute settle delay runs |
| | False | `WaitingForClusterMinimum` | a scale-down would go below IBM's minimum (1 on IKS, 2 on ROKS), or IBM refused it with `E1437` |
| | False | `WaitingForDisruptionToken` | another pool of the Cluster holds the disruption token |
| | False | `WaitingForWorkloadCluster` | the workload cluster is not connected; checked before anything is cordoned |
| | False | `DrainingNode` | a scale-down drains the predicted workers |
| | False | `IBMAPIError` | IBM answered a call for the pool with a 401, a 403 for a missing role, a 400 or a 404, or CAPIBM cannot resolve its IBM credentials; the message names the call and IBM's code only. For a scale-down's resize call the report stays, with the Nodes cordoned and the disruption token held, and the call is repeated at most once every 3 minutes. A growth IBM refuses with a 400 or a 403 while a worker replacement runs is asked again 15 minutes later, at the time the message ends with ("...; while a worker of the pool is replaced, CAPIBM asks IBM again for 3 workers per zone at <time>"), or at once when the MachinePool's `replicas` or the pool object's spec changes |
| `WorkersUpToDate` | True | `UpToDate` | no worker is due for replacement and none has an update available |
| | False | `PatchUpdateAvailable` | policy `Manual` and no due `spec.rollout.after`: "3 of 6 workers can move to IBM fix pack 1.36.5_1545; set spec.rollout.after to roll them, or set spec.workerUpdates.policy: Automatic" |
| | False | `RollingOut` | a rollout is in progress between workers ("replaced 2 of 6; ...") |
| | False | `DrainingNode` | the worker to replace is drained |
| | False | `ReplacingWorker` | IBM replaces the worker |
| | False | `WaitingForReplacement` | the replacement is not yet deployed, healthy and `Ready` for `minReadySeconds` |
| | False | `WorkersNotReady` | a replacement waits to start because another worker of the pool is not deployed, healthy and `Ready` for `minReadySeconds`; the message names it. A replacement completes only once every worker of the pool is, so a worker that stays broken holds only this pool's rollout, never the disruption token. Repair or replace it (`ibmcloud ks worker replace`) |
| | False | `WaitingForControlPlaneUpgrade` | the master is updating (a requested minor or an IBM patch) |
| | False | `WaitingForCapacity` | "...; scale MachinePool <name> to <n> to allow a rolling replacement" (`<n>` adds whole workers per zone until the cluster minimum holds during the replacement) |
| | False | `WaitingForClusterMinimum` | IBM refused the replacement with `E1437`; the Node stays cordoned |
| | False | `WaitingForSettledCluster`, `WaitingForClusterState`, `WaitingForDisruptionToken`, `WaitingForWorkloadCluster` | the [disruption checks](./worker-pools.md#one-disruption-at-a-time); a replacement waits in `WaitingForWorkloadCluster` in every step before its IBM call, also with both exclusion annotations |
| | False | `ExternallyAutoscaled` | IBM reports the pool as autoscaled, so CAPIBM replaces none of its workers; also after IBM refused the replacement with its autoscaled-pool 403 |
| | False | `IBMAPIError` | IBM answered the replacement call with a 401, a 403 for a missing role or a 400 other than `E1437`; the message names the call and IBM's code only; the Node stays cordoned and the call is repeated at most once every 3 minutes |
| | False | `AutoRebalanceDisabled` | IBM auto-rebalance is off in the worker's zone; fix: `ibmcloud ks worker-pool rebalance` |
| | False | `ReplacementNotCreated` | no new worker 30 minutes after the old one was deleted |
| | False | `WorkerOperationStuck` | a worker has been changing for more than 60 minutes |
| | any | message `VersionIgnored` | `MachinePool.spec.template.spec.version` is set; the message ends with "...; VersionIgnored: MachinePool spec.template.spec.version is ignored: workers follow the control plane minor; fix packs follow spec.workerUpdates" |
| `Draining` (activity) | True | `DrainingNode` | Nodes are cordoned and their pods evicted |
| | True | `DrainBlocked` | a PodDisruptionBudget refuses an eviction; the message lists pods and budgets (at most 3 pods per category plus "and N more", for the first 3 Nodes plus "and N more Nodes are not drained yet") |
| | True | `WaitingForVolumeDetach` | drained Nodes still have attached volumes |
| | True | `WaitingForWorkloadCluster` | the workload cluster became unreachable during a drain |
| | False | `NotDraining` | no drain in progress |
| `Deleting` | False | `NotDeleting` | not being deleted |
| | True | `WaitingForInfrastructure` | the IBM cluster exists but the `IBMCloudManagedCluster` is gone; the finalizer is kept |
| | True | `WaitingForControlPlane` | the control plane has not yet re-bound the IBM cluster (for example right after a move) |
| | True | `WaitingForClusterState` | IBM's cluster state does not allow removing the pool |
| | True | `WaitingForClusterMinimum` | removing the pool would go below IBM's minimum, or IBM refused it with `E1437`; retried at most every 3 minutes |
| | True | `ExternallyAutoscaled` | IBM refused removing an autoscaled pool; retried at most every 3 minutes |
| | True | `WaitingForSettledCluster` | another worker of the Cluster is changing |
| | True | `WaitingForDisruptionToken` | another pool holds the disruption token |
| | True | `WaitingForWorkloadCluster` | the workload cluster was never reached for this removal ([escape hatch](./troubleshooting.md#a-pool-removal-or-scale-down-waits-in-waitingforworkloadcluster)); or the deletion leaves the IBM pool alone (another object claims it, or it has another flavor) and must first revert the cordons of this pool's unfinished scale-down or replacement, which it does once the workload cluster is connected (both exclusion annotations let the finalizer go without it) |
| | True | `DrainingNodes` | every Node of the pool is drained |
| | True | `RemovingWorkerPool` | `removeWorkerPool` was sent; waiting until IBM no longer lists the pool |
| | True | `WaitingForWorkerRemoval` | the deletion leaves the IBM pool alone (another object claims it, or it has another flavor), and this pool's unfinished scale-down or replacement already asked IBM to remove workers: their Nodes stay cordoned until IBM has removed them or 60 minutes have passed since the call; then CAPIBM reverts the cordons and the finalizer goes |
| | True | `IBMAPIError` | IBM answered a call of the removal with a 401, a 403 for a missing role, a 400 or a 404; the message names the call and IBM's code only; for `removeWorkerPool` the Nodes stay cordoned and the call is repeated at most once every 3 minutes |
| `Paused` | | | `util/paused` |

Events: `WorkerPoolAdopted`, `DrainStarted`, `DrainCompleted`, `NodeUncordoned`, `WorkerReplaceRequested` (Normal);
`DrainBlocked` (on entry and every 30 minutes), `DrainTimeoutExceeded`, `UnexpectedScaleDownVictim`,
`WorkerPoolNotRemoved` (a deletion that leaves the IBM pool alone because it is not this object's) (Warning).

## Status fields worth knowing

`IBMCloudManagedControlPlane.status`:

| Field | Meaning |
|---|---|
| `clusterID`, `clusterCRN` | the bound IBM cluster |
| `state`, `master.state`, `master.health` | IBM's cluster state and master tokens (IBM's free text is never stored) |
| `distributionVersion`, `targetDistributionVersion`, `distributionVersionEndOfSupport` | IBM's master version, target version and end of service |
| `availableDistributionVersions` | the minors newer than the master's that IBM offers for the distribution, ascending, for example `["1.37", "1.38"]` (the `Upgrades` column of `kubectl get ibmcmcp -o wide`); information only, CAPIBM requests none of them; read from IBM at most once an hour per set of credentials and region, and kept when IBM cannot be asked |
| `versions[0].version`, `version` | the Kubernetes version the API server reports (contract fields) |
| `initialization.controlPlaneInitialized`, `externalManagedControlPlane` | contract fields |
| `createIntent` | create attempts whose outcome is not known yet; lets a lost create answer be told apart from a foreign cluster |
| `nonCriticalCreateErrorCodes` | IBM's non-critical error codes of the create answer |
| `masterUpdate` | the last master minor update CAPIBM requested: `version`, `lastRequestTime`, `attempts` (the retry backoff doubles with each) |
| `deletionRequestTime` | when CAPIBM last sent `DELETE` |

`IBMCloudManagedMachinePool.status`:

| Field | Meaning |
|---|---|
| `ready`, `initialization.provisioned` | the IBM pool exists (sticky; Cluster API v1.14 reads `ready`) |
| `replicas` | IBM workers of the pool that are not deleted; equals the length of `spec.providerIDList` |
| `poolID`, `sizePerZone`, `zones` | the IBM pool, IBM's per-zone worker count, and the observed zones |
| `workerVersions`, `targetWorkerVersion`, `workersWithUpdateAvailable` | workers per IBM version, the newest fix pack offered, and how many workers have an update |
| `missingWorkers` | workers that one listing omitted; a worker leaves `providerIDList` only when IBM lists it as deleted or two listings at least 30 seconds apart omit it, so a glitch in IBM's answer never makes Cluster API delete a live Node |
| `disruption` | the worker removal in flight, absent when idle: `operation` (`Replace`, `ScaleDown`, `DeletePool`), `phase` (`Draining`, `WaitingForVolumeDetach`, `WaitingForIBM`, `WaitingForRemoval`, `WaitingForReplacement`), `workerIDs`, `drainStartTime`, `ibmRequestTime` |
| `lastDisruptionTime` | when this pool last asked IBM to remove capacity; every pool of the Cluster waits 3 minutes after the newest one |

In phase `WaitingForIBM`, CAPIBM has finished the drain and is about to call IBM, or IBM refused the call: with `E1437`
the pool reports `WaitingForClusterMinimum`, and with a 409 (`OperationInProgress`, IBM is busy with the cluster)
CAPIBM waits without a second call. The Nodes stay cordoned in both cases, and the call is repeated at most once every
3 minutes, after the workers were listed again.

## IBM cluster states

CAPIBM reads IBM's cluster `state` (the `State` column) and acts on it as follows. Unknown states are treated as an
existing cluster, with the state named in messages.

| IBM state | CAPIBM treats the cluster as | Effect |
|---|---|---|
| `requested`, `deploying` | Provisioning | `Creating`, checked every minute |
| `deployed`, `pending`, `normal` | Exists | normal operation; the master is usable from `deployed` |
| `warning`, `critical`, `error`, `unavailable`, `scaled_down` | Exists | reported; `Available` is `False` only when the master health is `error` or `unavailable` or the probe fails; `unavailable` and `scaled_down` block pool changes |
| `updating`, `update_failed` | Exists | `RollingOut` while updating; both block a master update |
| `update_canceled` | Exists | a master update is allowed when the master health allows it |
| `unsupported` | Exists | `MasterVersionUpToDate` stays `True` while the master runs the requested minor, and its message adds "IBM no longer supports <minor>: upgrade one minor at a time, starting with spec.distributionVersion <next-minor>"; a master update is allowed; pool create, grow and replace are blocked |
| `create_failed`, `deploy_failed` | Failed | `CreateFailed` |
| `deleting`, `deleted`, `aborted`, `delete_failed` | GoneOutOfBand (outside a deletion) | `DeletedOutOfBand` |

## Annotations and labels

Written by CAPIBM (do not edit):

| Name | On | Meaning |
|---|---|---|
| `ibmcloudmanaged.controlplane.cluster.x-k8s.io/create-hold-until` | `IBMCloudManagedControlPlane` | RFC 3339 time before which no new create request is sent after a definitive refusal ([details](./troubleshooting.md#the-create-hold-after-a-refusal)) |
| `ibmcloudmanaged.controlplane.cluster.x-k8s.io/cluster-id`, `account-id`, `server`, `credential-*`, `fetched-at`, `refresh-checked-at`, `ca-*`, `kubeconfig-unauthorized-since` | `<cluster>-kubeconfig` Secret | binding record and credential bookkeeping ([details](./kubeconfig-and-security.md#annotations-on-the-secret)) |
| `ibmcloudmanagedmachinepool.infrastructure.cluster.x-k8s.io/drain-start-time`, `drain-owner`, `cordoned`, `excluded-from-lb` | workload cluster Nodes | drain bookkeeping ([details](./worker-pools.md#what-survives-restarts-and-moves)) |
| label `node.kubernetes.io/exclude-from-external-load-balancers=true` | workload cluster Nodes being drained | asks the VPC load balancer to stop sending traffic to the Node (whether IBM honours it, and how fast, is **not verified** yet); removed again if CAPIBM added it and the operation is aborted |

Read by CAPIBM (set them yourself):

| Name | On | Effect |
|---|---|---|
| `machine.cluster.x-k8s.io/exclude-node-draining` | `MachinePool` template metadata or `IBMCloudManagedMachinePool` | no cordon, no eviction before a worker removal |
| `machine.cluster.x-k8s.io/exclude-wait-for-node-volume-detach` | the same | no wait for volume detach |
| label `cluster.x-k8s.io/drain: skip` or `wait-completed` | Pods | never evicted, or waited for until completed |
| `cluster.x-k8s.io/paused` | Cluster API objects | pauses reconciliation (Cluster API) |
| `argocd.argoproj.io/sync-options: Prune=false`, `kustomize.toolkit.fluxcd.io/prune: disabled` | pool objects | prune protection for GitOps tools ([GitOps](./gitops.md#protect-pool-objects-from-pruning)); CAPIBM ignores them |

Other objects CAPIBM writes: the Lease `<cluster>-ibm-worker-disruption` (the disruption token, owned by the Cluster)
and the `<cluster>-kubeconfig` Secret.
