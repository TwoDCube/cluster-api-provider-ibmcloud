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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "sigs.k8s.io/cluster-api-provider-ibmcloud/api/managed/controlplane/v1alpha1"
)

// This file holds the gate-off detector. clusterctl upgrade substitutes the template variables from the current
// environment again, so an upgrade run without EXP_MANAGED_KUBERNETES=true silently turns the ManagedKubernetes gate
// off. The managed controllers then do not run: Cluster deletions hang on the control plane finalizer while IBM keeps
// billing, and kubeconfig and CA rotation handling stop. With the gate off, cmd/main.go adds this one-shot runnable
// instead of the controllers; it tells the operator, once per start, which objects are affected: the control planes
// this manager would reconcile with the gate on, within its --namespace and --watch-filter scope. Another instance
// with another scope may reconcile the rest, and a warning on those would be a false alarm.

const (
	// EventReasonManagedKubernetesDisabled is the reason of the Warning event that the detector emits on every
	// IBMCloudManagedControlPlane in its scope while the gate is off.
	EventReasonManagedKubernetesDisabled = "ManagedKubernetesDisabled"

	// ManagedKubernetesDisabledMessage is the text of that event; it names the variable that turns the gate on again.
	ManagedKubernetesDisabledMessage = "feature gate ManagedKubernetes is off: this cluster is not reconciled and cannot be " +
		"deleted; set EXP_MANAGED_KUBERNETES=true"

	// gateOffListPageSize bounds one list request, so that a management cluster with many control planes is read in
	// pages instead of one large response.
	gateOffListPageSize = 100
	// gateOffListAttempts and gateOffRetryInterval bound the retries of a failed list: right after a start the API
	// server may be briefly unreachable, and without a retry the warning would be lost until the next start.
	gateOffListAttempts  = 5
	gateOffRetryInterval = 10 * time.Second
)

// GateOffDetectorOptions is the watch scope of the manager, which the detector honours as the managed controllers do
// with the gate on, so that it warns only about the control planes this manager would reconcile.
type GateOffDetectorOptions struct {
	// WatchNamespace is the --namespace flag: only the control planes in this namespace are checked, as the manager's
	// cache holds no other; "" checks every namespace.
	WatchNamespace string

	// WatchFilterValue is the --watch-filter flag: only the control planes whose cluster.x-k8s.io/watch-filter label has
	// this value are checked, as predicates.ResourceHasFilterLabel admits only those to the managed controllers; ""
	// checks them all.
	WatchFilterValue string
}

// validate returns an error naming the flag to fix when an option is a value that no object can match: a namespace
// name or a label value the API server rejects.
func (o GateOffDetectorOptions) validate() error {
	var errs []error
	if o.WatchNamespace != "" {
		if problems := validation.IsDNS1123Label(o.WatchNamespace); len(problems) > 0 {
			errs = append(errs, fmt.Errorf("--namespace %q is not a valid namespace name (%s); fix the flag", o.WatchNamespace,
				strings.Join(problems, "; ")))
		}
	}
	if problems := validation.IsValidLabelValue(o.WatchFilterValue); len(problems) > 0 {
		errs = append(errs, fmt.Errorf("--watch-filter %q is not a valid label value (%s); fix the flag", o.WatchFilterValue,
			strings.Join(problems, "; ")))
	}
	return errors.Join(errs...)
}

// listOptions returns the list options of the scope: the namespace and the watch-filter label selector, each only when
// set. Every page of a paginated list must repeat them, because the API server ties a continue token to its query.
func (o GateOffDetectorOptions) listOptions() []client.ListOption {
	options := []client.ListOption{client.Limit(gateOffListPageSize)}
	if o.WatchNamespace != "" {
		options = append(options, client.InNamespace(o.WatchNamespace))
	}
	if o.WatchFilterValue != "" {
		options = append(options, client.MatchingLabels{clusterv1.WatchLabel: o.WatchFilterValue})
	}
	return options
}

// GateOffDetector is a one-shot manager.Runnable: when the ManagedKubernetes gate is off it lists the
// IBMCloudManagedControlPlane objects of its scope (GateOffDetectorOptions) with the API reader and, if any exist,
// logs an error and emits one ManagedKubernetesDisabled Warning event on each. It reconciles nothing. Create it with
// NewGateOffDetector.
type GateOffDetector struct {
	reader   client.Reader
	recorder record.EventRecorder
	options  GateOffDetectorOptions

	// attempts and retryInterval bound the retries of a failed list; tests shorten them.
	attempts      int
	retryInterval time.Duration
}

var (
	_ manager.Runnable               = &GateOffDetector{}
	_ manager.LeaderElectionRunnable = &GateOffDetector{}
)

// NewGateOffDetector returns the detector that cmd/main.go adds to the manager when the ManagedKubernetes gate is off.
// It does nothing until the manager starts it; a nil reader or recorder makes Start fail, so that the manager stops
// at start-up with a message instead of losing the warning.
//
// Parameters:
//   - reader lists the control planes; pass mgr.GetAPIReader(), because no informer for the managed kinds runs while
//     the gate is off. It is uncached and not bound to the cache's namespaces, so the scope comes from options.
//   - recorder emits the events; pass mgr.GetEventRecorderFor("ibmcloudmanaged-gate").
//   - options is the manager's watch scope: pass its --namespace and --watch-filter values.
func NewGateOffDetector(reader client.Reader, recorder record.EventRecorder, options GateOffDetectorOptions) *GateOffDetector {
	return &GateOffDetector{reader: reader, recorder: recorder, options: options, attempts: gateOffListAttempts,
		retryInterval: gateOffRetryInterval}
}

// NeedLeaderElection reports true: only the leader warns, so that the replicas of a highly available manager do not
// emit the same events several times.
func (d *GateOffDetector) NeedLeaderElection() bool {
	return true
}

// Start runs the detector once and returns. A list that keeps failing is logged and given up, because the detector
// only warns; it never stops the manager for that. A scope that no object can match (an invalid --namespace or
// --watch-filter) is logged the same way: no control plane can be in it, and the managed controllers would reconcile
// nothing with it either. ctx is the manager's context.
func (d *GateOffDetector) Start(ctx context.Context) error {
	if err := d.validate(); err != nil {
		return err
	}
	log := ctrl.LoggerFrom(ctx).WithName("ibmcloudmanaged-gate").WithValues(
		"namespace", d.options.WatchNamespace, "watchFilter", d.options.WatchFilterValue)
	if err := d.options.validate(); err != nil {
		log.Error(err, "Cannot check for IBMCloudManagedControlPlane objects while the ManagedKubernetes feature gate is off")
		return nil
	}
	controlPlanes, err := d.listControlPlanes(ctx)
	if err != nil {
		log.Error(err, "Cannot check whether IBMCloudManagedControlPlane objects exist while the ManagedKubernetes feature gate is off")
		return nil
	}
	if len(controlPlanes) == 0 {
		return nil
	}
	log.Error(nil, "IBMCloudManagedControlPlane objects exist while the ManagedKubernetes feature gate is off; they are not "+
		"reconciled and their Clusters cannot be deleted; set EXP_MANAGED_KUBERNETES=true", "count", len(controlPlanes))
	for i := range controlPlanes {
		d.recorder.Event(&controlPlanes[i], corev1.EventTypeWarning, EventReasonManagedKubernetesDisabled, ManagedKubernetesDisabledMessage)
	}
	return nil
}

// validate names every missing dependency of the detector.
func (d *GateOffDetector) validate() error {
	var errs []error
	if core.IsNil(d.reader) {
		errs = append(errs, errors.New("GateOffDetector needs a reader; pass mgr.GetAPIReader() to NewGateOffDetector"))
	}
	if core.IsNil(d.recorder) {
		errs = append(errs, errors.New(`GateOffDetector needs an event recorder; pass mgr.GetEventRecorderFor("ibmcloudmanaged-gate") to NewGateOffDetector`))
	}
	return errors.Join(errs...)
}

// listControlPlanes lists the IBMCloudManagedControlPlanes of the detector's scope, retrying a failed list up to
// attempts times. A missing CRD means no such object can exist. ctx is the manager's context; its end stops the retries.
func (d *GateOffDetector) listControlPlanes(ctx context.Context) ([]controlplanev1.IBMCloudManagedControlPlane, error) {
	var lastErr error
	for attempt := 1; attempt <= d.attempts; attempt++ {
		controlPlanes, err := d.listAllPages(ctx)
		switch {
		case err == nil:
			return controlPlanes, nil
		case meta.IsNoMatchError(err):
			return nil, nil
		}
		lastErr = err
		if attempt == d.attempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.retryInterval):
		}
	}
	return nil, fmt.Errorf("listing IBMCloudManagedControlPlane objects (%d attempts): %w", d.attempts, lastErr)
}

// listAllPages lists the IBMCloudManagedControlPlanes of the detector's scope in pages of gateOffListPageSize. ctx is
// the manager's context.
func (d *GateOffDetector) listAllPages(ctx context.Context) ([]controlplanev1.IBMCloudManagedControlPlane, error) {
	var all []controlplanev1.IBMCloudManagedControlPlane
	options := d.options.listOptions()
	for {
		page := &controlplanev1.IBMCloudManagedControlPlaneList{}
		if err := d.reader.List(ctx, page, options...); err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.Continue == "" {
			return all, nil
		}
		options = append(d.options.listOptions(), client.Continue(page.Continue))
	}
}
