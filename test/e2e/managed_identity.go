//go:build e2e

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

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/IBM/go-sdk-core/v5/core"

	"sigs.k8s.io/cluster-api/test/framework"
)

// controllerAPIKeyVariable is the environment variable that holds the controller's IBM Cloud API key. The CI script
// exports it (Boskos hands it out), and clusterctl init substitutes it into the manager-bootstrap-credentials Secret
// (config/default/credentials.yaml). It is deliberately not an e2e config variable: those are written into the
// clusterctl configuration file in the artifacts folder.
const controllerAPIKeyVariable = "IBMCLOUD_API_KEY" //nolint:gosec // G101: the name of the variable, not a credential.

// IdentityFixture provides the IBM Cloud credentials that the clusters of a managed spec are created with. Every
// managed spec and the clusterctl move step (source and target) take one as a parameter, so that a later credential
// source (for example a per-cluster credential) plugs in without changing the specs. "Identity" means the IBM Cloud
// IAM credentials only, never the mapping of a CAPI object to an IBM cluster. A fixture makes no assumption about
// which account or resource group its credentials reach.
type IdentityFixture interface {
	// Setup prepares the credentials on the management cluster for the clusters of one namespace and returns the
	// clusterctl variables the managed flavors need for them.
	//
	// Parameters:
	//   - ctx bounds the work.
	//   - mgmt is the management cluster that will own the clusters.
	//   - namespace is the namespace of the clusters.
	Setup(ctx context.Context, mgmt framework.ClusterProxy, namespace string) (map[string]string, error)

	// Teardown removes what Setup created on the management cluster.
	//
	// Parameters:
	//   - ctx bounds the work.
	//   - mgmt is the management cluster Setup prepared.
	Teardown(ctx context.Context, mgmt framework.ClusterProxy) error

	// Authenticator returns an authenticator for the same credentials. The janitor, the inventory diff and the IBM
	// assertions of the lanes use it, so that they act exactly as the identity that created the run's clusters.
	Authenticator() (core.Authenticator, error)
}

// ControllerIdentityFixture is the only fixture of this feature: the clusters are created with the controller's own
// credentials. Setup creates nothing and adds no template variable (the managed flavors contain no credential
// variable, because IBM runs the cloud controller manager of a managed cluster), and Teardown removes nothing.
type ControllerIdentityFixture struct{}

var _ IdentityFixture = ControllerIdentityFixture{}

// Setup implements IdentityFixture: the controller's credentials need no preparation.
func (ControllerIdentityFixture) Setup(context.Context, framework.ClusterProxy, string) (map[string]string, error) {
	return map[string]string{}, nil
}

// Teardown implements IdentityFixture: Setup created nothing.
func (ControllerIdentityFixture) Teardown(context.Context, framework.ClusterProxy) error {
	return nil
}

// Authenticator implements IdentityFixture with an IAM authenticator for the controller's API key. It reads the key
// from the environment here, in the suite, so that test/e2e/managedtools only ever receives an explicit authenticator.
// It makes no network call; the first request fetches the IAM token.
func (ControllerIdentityFixture) Authenticator() (core.Authenticator, error) {
	apiKey := os.Getenv(controllerAPIKeyVariable)
	if apiKey == "" {
		return nil, fmt.Errorf("%s is not set: export the IBM Cloud API key the CAPIBM controller runs with "+
			"(clusterctl init puts the same value into the manager-bootstrap-credentials Secret)", controllerAPIKeyVariable)
	}
	authenticator, err := core.NewIamAuthenticatorBuilder().SetApiKey(apiKey).Build()
	if err != nil {
		// The SDK's message names the problem (for example quotes or braces in the key) without the key itself.
		return nil, errors.Join(fmt.Errorf("%s does not hold a usable IBM Cloud API key", controllerAPIKeyVariable), err)
	}
	return authenticator, nil
}
