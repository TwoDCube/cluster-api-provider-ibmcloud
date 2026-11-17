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

// Package ibmclustername holds IBM Cloud's rule for the name of an IBM Cloud Kubernetes Service (IKS) or Red Hat
// OpenShift on IBM Cloud (ROKS) cluster, so that CAPIBM can refuse a name before it asks IBM to create the cluster.
//
// Why it exists: IBM refuses createCluster with HTTP 400 and error code E0125 when the name breaks its rule. The
// refusal names the rule ("The name must be 32 or fewer characters, begin with a letter, and contain only alphanumeric
// characters, '-', '_' and '.'"), but by then the user has applied the objects, and the name of a control plane cannot
// be changed in place. Every layer that sees the name therefore checks it with this one validator, so they can never
// disagree: the IBMCloudManagedControlPlane admission webhook, which rejects the object; the control plane controller,
// which stops before any IBM call when admission could not tell the name; and the e2e name contract. The CRD of
// IBMCloudManagedControlPlane repeats the rule as a schema rule on spec.ibmCloudClusterName, because a kubebuilder
// marker cannot call Go.
//
// Why the rule is IBM's API answer and not IBM's documentation: older IBM documentation gives 35 characters and no
// underscore. The API refused a 33-character name and accepts '_', so the rule here is the one quoted in the API's own
// refusal. A letter is an ASCII letter: IBM speaks of alphanumeric characters and its names end up in DNS names, so a
// non-ASCII letter is refused here rather than sent to IBM on a guess.
//
// Following CAPIBM's code organisation, this SDK-free, pure helper lives under pkg/util: no IBM SDK, no I/O, no state.
package ibmclustername
