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
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
)

// Ensure IBMCloudManagedMachinePool implements the typed validator interface. There is no defaulter (package doc).
var _ admission.Validator[*infrav1.IBMCloudManagedMachinePool] = &IBMCloudManagedMachinePool{}

// The pool's webhook does not list delete: no webhook ever rejects a pool deletion.
//+kubebuilder:webhook:verbs=create;update,path=/validate-infrastructure-cluster-x-k8s-io-v1alpha1-ibmcloudmanagedmachinepool,mutating=false,failurePolicy=fail,groups=infrastructure.cluster.x-k8s.io,resources=ibmcloudmanagedmachinepools,versions=v1alpha1,name=vibmcloudmanagedmachinepool.kb.io,sideEffects=None,admissionReviewVersions=v1;v1beta1

// IBMCloudManagedMachinePool validates IBMCloudManagedMachinePool objects. Its only rule is the feature gate on create;
// every field rule is a CEL rule on the type, and pool deletions are never rejected.
type IBMCloudManagedMachinePool struct {
	// ManagedKubernetesEnabled is the value of the ManagedKubernetes feature gate. When false, CREATE is rejected and
	// every other operation is allowed.
	ManagedKubernetesEnabled bool
}

// SetupWebhookWithManager registers the validator with mgr's webhook server.
//
// mgr is the manager whose webhook server serves the validator.
func (r *IBMCloudManagedMachinePool) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &infrav1.IBMCloudManagedMachinePool{}).
		WithValidator(r).
		Complete()
}

// ValidateCreate rejects the create while the feature gate is off and otherwise allows it.
//
// ctx is unused; pool is the IBMCloudManagedMachinePool being created.
func (r *IBMCloudManagedMachinePool) ValidateCreate(_ context.Context, pool *infrav1.IBMCloudManagedMachinePool) (admission.Warnings, error) {
	if !r.ManagedKubernetesEnabled {
		return nil, gateOffCreateError(machinePoolResource, "IBMCloudManagedMachinePool", pool.Name)
	}
	return nil, nil
}

// ValidateUpdate allows every update: the field rules (immutable pool name, flavor and zones) are CEL rules on the
// type.
//
// ctx, oldPool and newPool are unused.
func (r *IBMCloudManagedMachinePool) ValidateUpdate(_ context.Context, _, _ *infrav1.IBMCloudManagedMachinePool) (admission.Warnings, error) {
	return nil, nil
}

// ValidateDelete allows every deletion: the pool controller drains before it removes workers, and blocking pool
// deletions is a regret other providers documented. The marker does not register delete, so the API server never
// calls it; it exists because the validator interface requires it.
//
// ctx and pool are unused.
func (r *IBMCloudManagedMachinePool) ValidateDelete(_ context.Context, _ *infrav1.IBMCloudManagedMachinePool) (admission.Warnings, error) {
	return nil, nil
}
