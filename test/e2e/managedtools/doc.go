/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package managedtools holds the cost controls and the pure checks of the managed Kubernetes (IKS and ROKS) e2e lanes
// (test/e2e/managed_test.go). It has no build tag, so `go test ./...` runs its unit tests offline against httptest
// servers, while the lanes themselves need the e2e tag and an IBM Cloud account.
//
// What it contains, and why each part is here rather than in the lanes:
//
//   - The name contract (names.go): every e2e cluster is named <prefix>-<run-id>-<lane>, and the janitor relies on
//     that shape, so both sides share one validated builder.
//   - The janitor (janitor.go): it deletes, by IBM cluster ID only, the clusters a crashed run left behind (name
//     starts with "<prefix>-", older than four hours) and, as the backstop of a lane's cleanup, the run's own clusters.
//     The backstop asks IBM to delete the persistent storage of the run's clusters too; the janitor asks IBM to keep
//     that of stale clusters, whose selection by prefix and age proves less.
//     A wrong deletion in a shared account is the most expensive mistake an e2e tool can make, so the selection rules
//     are pure functions with table tests and the HTTP calls are tested against httptest servers.
//   - The inventory diff (inventory.go): a snapshot of clusters, VPC load balancers, VPE gateways, security groups
//     and volumes before and after a run; the diff tolerates the shared iks-<service>-<vpcID> gateways and the
//     kube-vpegw-<vpcID> security group that IBM keeps in a VPC for good (IBM documents this; observed on IBM Cloud).
//   - The sharing switches (scope.go): E2E_MANAGED_JANITOR and E2E_MANAGED_LEFTOVER_SCOPE. By default the janitor and
//     the inventory diff cover every e2e cluster and VPC resource of the resource group, which is right for an account
//     that holds one managed run at a time. With the janitor off and the lane scope, a lane answers only for its run's
//     clusters and the VPC resources of its own VPC, so it can run next to other runs that use VPCs of their own.
//   - The disruption recorder (disruption.go) and the version helpers (versions.go): the pure parts of the
//     PodDisruptionBudget probe and of the upgrade checks, so that the lanes only orchestrate.
//   - The pinned check (pins.go): the worker-by-worker proof of a drain. A drain step pins a probe pod to every worker
//     it may remove; the pod of each removed worker must have been evicted through the Eviction API and the pod of
//     each kept worker left alone, wherever the scheduler put the spread probe's pods.
//   - The timeout limit (timeouts.go): it lowers every wait's timeout to the time left until the lane's hard timeout,
//     because the CAPI framework helpers the lanes call poll without a context.
//   - The lease check (lease.go): in CI only the Boskos lease keeps a second managed run out of the account, so the
//     lanes stop their steps once the CI heartbeat reports that Boskos refused to renew it.
//   - The detachable context (contexts.go): work that a step starts but that must outlive it, such as the move
//     target's controller log streams, stays within the step's deadline while it starts and runs on afterwards.
//   - The ownership facts (ownership.go): what a clusterctl move must not change, taken from CAPIBM's objects and from
//     IBM's own worker list, so that a copied spec field cannot make the move look clean. The same file tells an IBM
//     answer that may pass from IBM's final answer, so that the move step's hold over the facts waits out a single 5xx
//     instead of failing on it.
//   - The zone check (zones.go): the Ready workers per zone that a drain through IBM's Ingress ALB budgets needs, so
//     that a step whose topology cannot drain fails with the zone's name instead of timing out in DrainBlocked.
//
// Convention: credentials and configuration are explicit arguments. The IBM clients of this package take the service
// URL, the core.Authenticator and the *http.Client from their caller (the lanes build the authenticator from the
// identity fixture's credentials, the same credentials that created the run's clusters) and never read the
// environment, a credentials file or a global authenticator. They are built on the IBM SDK packages that CAPIBM's own
// clients wrap (kubernetesserviceapiv1 and vpcv1): the CAPIBM interfaces have no cluster list, VPE gateway list or
// volume list, and their HTTP transport cannot be replaced from another package, which the httptest tests need.
package managedtools
