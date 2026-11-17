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

package managedtools

import "context"

// DetachableContext returns a context for work that a step starts and that must outlive the step, such as the
// controller log streams and the event watch of a management cluster that the step creates. The CAPI framework
// helpers that start such work use one context both for their own waits and for the work they leave running, so
// neither the step's context (the work would stop with the step) nor a background context (the waits would ignore the
// lane's hard timeout) fits. Until detach is called, the returned context ends when parent ends, with parent's cause,
// so the start-up stays within the step's deadline; after detach, it ends only when cancel is called. It keeps
// parent's values.
//
// Parameters:
//   - parent is the step's context.
//
// It returns the context, detach (call it once the start-up has returned; later calls do nothing) and cancel (call it
// when the work must stop, for example before the management cluster is deleted; it also detaches).
func DetachableContext(parent context.Context) (context.Context, func(), context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, func() { cancel(context.Cause(parent)) })
	detach := func() { stop() }
	return ctx, detach, func() {
		stop()
		cancel(nil)
	}
}
