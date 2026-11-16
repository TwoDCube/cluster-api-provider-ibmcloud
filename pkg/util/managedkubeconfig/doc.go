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

// Package managedkubeconfig builds, inspects and schedules the refresh of the workload-cluster kubeconfig that CAPIBM
// writes for an IBM Cloud managed cluster (IKS or ROKS on VPC Gen2).
//
// Why the package exists: the CAPI control-plane contract asks every control-plane provider to publish a
// `<cluster>-kubeconfig` Secret, and CAPI's ClusterCache reads it to reach the workload cluster. IBM Cloud issues the
// credential (an admin client certificate, valid two years and re-served unchanged on repeated fetches, as observed
// on IBM Cloud), so CAPIBM cannot mint or shorten it the way the kubeadm control plane does. CAPIBM therefore decides
// *when* fetching again is worth it (half the certificate lifetime, a CA rotation, a 401, a changed API server URL)
// and *what* to write (a kubeconfig built from parts, never IBM's file copied verbatim).
//
// Conventions this package follows:
//
//   - Pure functions only. Nothing here talks to IBM Cloud, the management cluster or the workload cluster, reads the
//     environment or the file system, or keeps package-level state; the one package-level variable,
//     ErrUnsupportedCredential, is a sentinel error that is never changed. Time is an argument (`now`), so tests drive
//     it with a fake clock.
//   - Build produces exactly one cluster, one user and one context with inline data: the server, optional CA data, and
//     the client certificate and key. Never exec, auth-provider, token, file paths or skipped TLS verification, because
//     CAPI's ClusterCache rejects the first two and the others would weaken or redirect authentication.
//   - Inspect accepts exactly the shape Build produces and describes it without secret material and without the client
//     certificate's subject. The subject of IBM's admin certificate names the IAM identity that fetched it
//     (CN=KubeCert#<caller-iam-id>); refresh decisions must never depend on it, so it is not even exposed.
//   - Decide takes no credential-identity input: which IAM identity fetched the stored certificate never triggers or
//     blocks a refresh.
package managedkubeconfig
