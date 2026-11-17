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

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

// contextKey is the type of the value key the tests store in a parent context.
type contextKey struct{}

// TestDetachableContextOutlivesTheStepOnceDetached is the reproducer of the move target's controller logs that
// stopped streaming when the step that started them ended: work started with the step's context dies with the step,
// while a detached context keeps it running until the lane cancels it.
func TestDetachableContextOutlivesTheStepOnceDetached(t *testing.T) {
	g := NewWithT(t)
	step, endStep := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "lane"))
	ctx, detach, cancel := DetachableContext(step)
	detach()
	endStep()
	g.Consistently(ctx.Done()).WithTimeout(50*time.Millisecond).ShouldNot(BeClosed(), "detached work must outlive the step")
	g.Expect(ctx.Value(contextKey{})).To(Equal("lane"), "the parent's values are kept")

	cancel()
	g.Eventually(ctx.Done()).Should(BeClosed())
	g.Expect(context.Cause(ctx)).To(MatchError(context.Canceled))
}

// TestDetachableContextEndsWithTheStepUntilDetached guards the other half: while the step still starts the work, the
// step's deadline bounds it, so a start-up that hangs cannot carry the step past the lane's hard timeout.
func TestDetachableContextEndsWithTheStepUntilDetached(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		g := NewWithT(t)
		step, endStep := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer endStep()
		ctx, _, cancel := DetachableContext(step)
		defer cancel()
		g.Eventually(ctx.Done()).WithTimeout(5 * time.Second).Should(BeClosed())
		g.Expect(context.Cause(ctx)).To(MatchError(context.DeadlineExceeded))
	})
	t.Run("cancelled with a cause", func(t *testing.T) {
		g := NewWithT(t)
		lost := errors.New("the Boskos lease of this e2e run is lost")
		step, endStep := context.WithCancelCause(context.Background())
		ctx, _, cancel := DetachableContext(step)
		defer cancel()
		endStep(lost)
		g.Eventually(ctx.Done()).Should(BeClosed())
		g.Expect(context.Cause(ctx)).To(MatchError(lost))
	})
	t.Run("already ended", func(t *testing.T) {
		g := NewWithT(t)
		step, endStep := context.WithCancel(context.Background())
		endStep()
		ctx, detach, cancel := DetachableContext(step)
		defer cancel()
		g.Eventually(ctx.Done()).Should(BeClosed())
		detach()
	})
}

func TestDetachableContextCancelBeforeDetach(t *testing.T) {
	g := NewWithT(t)
	step, endStep := context.WithCancel(context.Background())
	defer endStep()
	ctx, detach, cancel := DetachableContext(step)
	cancel()
	g.Eventually(ctx.Done()).Should(BeClosed())
	g.Expect(context.Cause(ctx)).To(MatchError(context.Canceled))
	detach()
	cancel()
	g.Expect(step.Err()).NotTo(HaveOccurred(), "cancelling the detachable context never ends the step")
}
