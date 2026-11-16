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

	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
	managedscope "sigs.k8s.io/cluster-api-provider-ibmcloud/pkg/cloud/scope/managed"
)

// reconcileDelete deletes the IBM cluster of a deleting control plane and removes the finalizer once the scope reports
// that no IBM cluster of this control plane is left. clusterctl move is the exception: it deletes the source copy
// after the target has taken over the IBM cluster, so the finalizer goes at once and nothing calls IBM. Pause has
// already returned before this point (Reconcile), so a paused deletion makes no call either. scope is this
// reconcile's scope.
func (r *IBMCloudManagedControlPlaneReconciler) reconcileDelete(ctx context.Context, scope *managedscope.ControlPlaneScope) (ctrl.Result, error) {
	cp := scope.ControlPlane
	if controlPlaneBeingMoved(cp) {
		ctrl.LoggerFrom(ctx).Info("clusterctl move deletes the source copy; the IBM cluster stays with the target")
		controllerutil.RemoveFinalizer(cp, controlplanev1.IBMCloudManagedControlPlaneFinalizer)
		return ctrl.Result{}, nil
	}
	outcome, done := scope.ReconcileDelete(ctx)
	if done {
		controllerutil.RemoveFinalizer(cp, controlplanev1.IBMCloudManagedControlPlaneFinalizer)
		return ctrl.Result{}, nil
	}
	return resultFor(outcome, managedscope.IntervalDelete)
}

// controlPlaneBeingMoved reports whether clusterctl move is deleting cp from the source management cluster: clusterctl
// sets the delete-for-move annotation on every object it moves before it deletes it there, and the copy on the target
// takes over the IBM cluster, so the source must make no IBM call. cp is the control plane.
func controlPlaneBeingMoved(cp *controlplanev1.IBMCloudManagedControlPlane) bool {
	_, ok := cp.Annotations[clusterctlv1.DeleteForMoveAnnotation]
	return ok
}
