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

package managed

import (
	"errors"
	"fmt"
	"strings"
)

// This file reads the IBM Cloud account of a cluster from the cluster's own CRN. Two places need the account: the
// account-id annotation of the kubeconfig Secret, which names the account in IBMClusterNotVisible messages, and the
// first segment of every Node provider ID (ProviderID). The account never comes from the credentials CAPIBM acts with
// (accounts.GetAccountID reads the environment and is banned on the managed path by the forbidigo rules of
// .golangci.yml), so it stays right for any identity and survives clusterctl move with the Secret.

// crnSegments is the number of colon-separated segments of an IBM CRN:
// crn:version:cname:ctype:service-name:location:scope:service-instance:resource-type:resource.
const crnSegments = 10

// crnScopeSegment is the index of the scope segment, "a/<account-id>" for an account-scoped resource such as a
// cluster.
const crnScopeSegment = 6

// crnAccountScopePrefix starts the scope segment of an account-scoped CRN.
const crnAccountScopePrefix = "a/"

// AccountIDFromCRN returns the account ID of an IBM Cloud Kubernetes Service cluster from its CRN, for example
// "a1b2c3d4e5f60718293a4b5c6d7e8f90" from
// "crn:v1:bluemix:public:containers-kubernetes:eu-de:a/a1b2c3d4e5f60718293a4b5c6d7e8f90:<clusterID>::". The account is
// the first segment of every Node provider ID (ProviderID; observed on IBM Cloud for IKS and ROKS) and the value of
// the kubeconfig Secret's account-id annotation. It comes from the cluster's own CRN, never from the credentials
// CAPIBM acts with, so it stays right for any identity and after a clusterctl move.
//
// Parameters:
//   - crn: the cluster CRN, IBMCloudManagedControlPlane status.clusterCRN (kubernetesservice.Cluster.CRN).
//
// It returns an error naming the CRN and the expected form when crn is empty, is not a ten-segment CRN, or has no
// account scope "a/<account-id>" whose ID consists of letters and digits only. A provider ID built from anything else
// would match no Node, so the caller keeps the published list unchanged instead; the kubeconfig Secret then carries no
// account-id annotation.
func AccountIDFromCRN(crn string) (string, error) {
	if crn == "" {
		return "", errors.New("the cluster CRN is empty; it is IBMCloudManagedControlPlane status.clusterCRN, which " +
			"the control plane controller writes once it has observed the IBM cluster")
	}
	segments := strings.Split(crn, ":")
	if len(segments) != crnSegments || segments[0] != "crn" {
		return "", fmt.Errorf("cluster CRN %q is not a CRN; want the form "+
			"crn:v1:<cname>:<ctype>:containers-kubernetes:<region>:a/<account-id>:<cluster-id>:: with ten colon-separated segments", crn)
	}
	account, found := strings.CutPrefix(segments[crnScopeSegment], crnAccountScopePrefix)
	if !found || account == "" || !isAlphanumeric(account) {
		return "", fmt.Errorf("cluster CRN %q has the scope %q; a cluster CRN names its account as a/<account-id>, "+
			"with an ID of letters and digits", crn, segments[crnScopeSegment])
	}
	return account, nil
}

// isAlphanumeric reports whether s consists of ASCII letters and digits only, which every IBM account ID does. It
// keeps separators such as "/" out of a provider ID and out of an annotation value that a message quotes.
//
// Parameters:
//   - s: the text to check.
func isAlphanumeric(s string) bool {
	return !strings.ContainsFunc(s, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
	})
}
