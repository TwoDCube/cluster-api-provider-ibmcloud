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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	infrav1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

// reconcileDelete runs the pool deletion and removes the finalizer once the scope allows it: right away when the
// Cluster, the control plane or a clusterctl move deletes the pool (no drain, no IBM call), otherwise once IBM no
// longer lists the pool, after every Node of it was drained. The deferred patch persists the finalizer removal and
// every condition. Until then it requeues at deletePoll or at the wait the step named. scope is this reconcile's scope.
func (r *IBMCloudManagedMachinePoolReconciler) reconcileDelete(ctx context.Context, scope *managedscope.MachinePoolScope) (ctrl.Result, error) {
	outcome, removeFinalizer := scope.ReconcileDelete(ctx)
	if removeFinalizer {
		controllerutil.RemoveFinalizer(scope.Pool, infrav1.IBMCloudManagedMachinePoolFinalizer)
		return ctrl.Result{}, nil
	}
	logMachinePoolTerminal(ctx, "The deletion of the IBMCloudManagedMachinePool needs attention", outcome)
	return resultFor(outcome, managedscope.IntervalDelete)
}
