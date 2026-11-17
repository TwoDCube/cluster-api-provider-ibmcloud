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

// Package workloadprobe verifies a workload-cluster kubeconfig by using it, before CAPIBM writes it and on every steady
// reconcile afterwards.
//
// Why the package exists: CAPIBM never writes a kubeconfig it has not seen work, it must find out which TLS trust mode
// an IBM master needs (cluster CA for IKS, public or private-only; system roots for ROKS's public master; a
// private-only ROKS master is not yet verified against IBM Cloud), it must notice a credential the API server stopped
// accepting (401 → refresh), and the ownership proof after a `clusterctl move` needs to know whether a stored
// credential is cluster-admin on a candidate cluster. CAPI's ClusterCache cannot answer these questions: it connects
// only to Clusters whose kubeconfig Secret is already written, caches the connection, and hides why a connection
// failed.
//
// Conventions this package follows:
//
//   - The prober builds a fresh client from the kubeconfig bytes for every probe and keeps no state between probes, so
//     a probe never uses or disturbs CAPIBM's ClusterCache connection and the package holds no package-level state.
//   - It accepts the kubeconfigs ClusterCache accepts, minus everything that would make the controller run a program
//     (exec, auth-provider: ClusterCache rejects these too), read its own file system (certificate, key, CA or token
//     file paths) or skip TLS verification. A user-provided Secret is probed with the same rules.
//   - Requests go only to the kubeconfig's server: a redirect to another host is refused, so a credential is never
//     presented to a host it was not meant for.
//   - Failures are classified (TLS, 401, unreachable, invalid kubeconfig, other) because the caller's next step differs
//     for each: try the other trust mode, refresh the credential, wait, or report.
//   - The result is credential-identity agnostic: ClusterAdmin comes from a SelfSubjectAccessReview, never from comparing
//     the user name with whoever CAPIBM acts as, so the proof holds whichever IAM identity fetched the certificate.
package workloadprobe
