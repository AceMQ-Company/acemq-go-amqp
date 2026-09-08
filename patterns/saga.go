// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package patterns

import (
	"context"
	"fmt"
	"strings"
)

// SagaStep is one step of a saga: a name, the work, and optionally how to undo
// it.
//
// Undo is allowed to be nil, and a nil Undo on a step that ran is skipped
// rather than treated as an error — a step that only read something, or one
// whose effect is harmless, needs no compensation. It is a mistake on a step
// that changed the world, and nothing here can tell the two apart, which is the
// argument for writing Undo first and Do second.
type SagaStep[T any] struct {
	// Name identifies the step in the result. It has to be unique within the
	// saga, because the compensation report names steps rather than indexes
	// them.
	Name string

	// Do is the work. Returning an error stops the saga and compensates
	// everything that already ran.
	Do func(ctx context.Context, subject T) error

	// Undo reverses Do. Optional; see the type documentation.
	Undo func(ctx context.Context, subject T) error
}

// Saga runs steps in order and undoes them in reverse when one fails.
//
// The pattern is for work that spans services, where a database transaction is
// not available and the alternative — doing half of it and hoping — is how a
// customer ends up charged for an order that was never placed:
//
//	saga, err := patterns.NewSaga("place-order", []patterns.SagaStep[*Order]{
//		{Name: "reserve-stock", Do: reserveStock, Undo: releaseStock},
//		{Name: "take-payment", Do: takePayment, Undo: refund},
//		{Name: "confirm", Do: confirm},
//	})
//	if err != nil {
//		return err
//	}
//
//	result := saga.Run(ctx, order)
//	if result.HasUnresolved() {
//		alert(result) // see [SagaResult.Unresolved]
//	}
//
// This is the in-process half of the pattern and has no wire contract: nothing
// is published and no header is set, so a Go saga and a Java one are the same
// idea rather than two ends of one conversation. What matches across the
// libraries is the behaviour — reverse-order compensation, a missing
// compensation skipped, a failing compensation logged and collected rather than
// allowed to stop the rest.
//
// # Concurrency
//
// A Saga is immutable once built and safe to share. One Run holds no state on
// it; the state is the subject, which is the caller's.
type Saga[T any] struct {
	name  string
	steps []SagaStep[T]
	log   func(format string, args ...any)
}

// SagaOption adjusts a saga.
type SagaOption[T any] func(*Saga[T])

// SagaLog gives the saga somewhere to report what it did.
//
// Worth setting for the one line that matters: a compensation that failed. It
// is in [SagaResult.Unresolved] as well, so a caller that reads the result
// misses nothing by leaving this unset — this is for the case where the result
// is handled somewhere far from where the saga ran.
//
//	patterns.SagaLog[*Order](log.Printf)
func SagaLog[T any](printf func(format string, args ...any)) SagaOption[T] {
	return func(s *Saga[T]) { s.log = printf }
}

// NewSaga builds a saga from its steps, in the order they run.
//
// It refuses a saga with no steps, a step with no name or no action, and two
// steps sharing a name — the last because names identify a step in the
// compensation report, and two of them would make that report ambiguous.
func NewSaga[T any](name string, steps []SagaStep[T], opts ...SagaOption[T]) (*Saga[T], error) {
	if name == "" {
		return nil, fmt.Errorf("acemq: a saga needs a name")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("acemq: saga %q has no steps", name)
	}

	seen := make(map[string]struct{}, len(steps))
	for i, step := range steps {
		if step.Name == "" {
			return nil, fmt.Errorf("acemq: step %d of saga %q has no name", i, name)
		}
		if step.Do == nil {
			return nil, fmt.Errorf("acemq: step %q of saga %q has no action", step.Name, name)
		}
		if _, dup := seen[step.Name]; dup {
			return nil, fmt.Errorf(
				"acemq: saga %q already has a step called %q. Names identify a step in the"+
					" compensation report, so two of them would make that report ambiguous",
				name, step.Name)
		}
		seen[step.Name] = struct{}{}
	}

	s := &Saga[T]{name: name, steps: append([]SagaStep[T](nil), steps...)}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Name is what this saga is called, in logs and in its result.
func (s *Saga[T]) Name() string { return s.name }

// Steps is how many steps it has.
func (s *Saga[T]) Steps() int { return len(s.steps) }

// Run executes the steps in order, compensating in reverse if one fails.
//
// It returns a result rather than an error, because a failed saga is not an
// exceptional condition to a caller that has to decide what happens next — and
// because the interesting part is not the failure but
// [SagaResult.Unresolved], the list of things that could not be undone.
//
// The context is passed to every action and every compensation, including the
// compensations that run after a failure. A caller whose failure is a
// cancellation should hand the compensations a context that outlives it:
//
//	result := saga.Run(context.WithoutCancel(ctx), order)
//
// otherwise the compensations are handed a context that is already dead and
// every one of them lands in [SagaResult.Unresolved].
//
// A step that panics is treated as a step that failed, so that the compensation
// for everything before it still runs. The panic value is the failure; it is
// not re-raised, because unwinding past the compensation is the one outcome
// this pattern exists to prevent.
func (s *Saga[T]) Run(ctx context.Context, subject T) SagaResult {
	completed := make([]string, 0, len(s.steps))

	for _, step := range s.steps {
		if err := s.invoke(ctx, step.Do, subject); err != nil {
			s.logf("saga %s failed at step %s: %v", s.name, step.Name, err)
			unresolved := s.compensate(ctx, subject, completed)
			return SagaResult{
				Saga:       s.name,
				FailedAt:   step.Name,
				Failure:    err,
				Completed:  completed,
				Unresolved: unresolved,
			}
		}
		completed = append(completed, step.Name)
		s.logf("saga %s completed step %s", s.name, step.Name)
	}

	return SagaResult{Saga: s.name, Completed: completed}
}

// compensate undoes what was done, most recent first.
//
// Reverse order because that is the order the world was changed in, and a
// compensation often depends on the state a later step has not yet altered.
//
// A compensation that fails does not stop the others. It is reported and its
// step collected, because stopping here would leave more undone than
// continuing, and the caller is told exactly which ones did not come back.
func (s *Saga[T]) compensate(ctx context.Context, subject T, completed []string) []string {
	var unresolved []string

	for i := len(completed) - 1; i >= 0; i-- {
		name := completed[i]
		step, ok := s.stepNamed(name)
		if !ok || step.Undo == nil {
			// Nothing to undo, which is legitimate. See [SagaStep].
			continue
		}
		if err := s.invoke(ctx, step.Undo, subject); err != nil {
			s.logf("saga %s could not compensate step %s: %v", s.name, name, err)
			unresolved = append(unresolved, name)
			continue
		}
		s.logf("saga %s compensated step %s", s.name, name)
	}
	return unresolved
}

func (s *Saga[T]) stepNamed(name string) (SagaStep[T], bool) {
	for _, step := range s.steps {
		if step.Name == name {
			return step, true
		}
	}
	return SagaStep[T]{}, false
}

// invoke runs one action or compensation, turning a panic into an error.
func (s *Saga[T]) invoke(
	ctx context.Context, fn func(context.Context, T) error, subject T,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if panicked, ok := r.(error); ok {
				err = fmt.Errorf("acemq: saga %s panicked: %w", s.name, panicked)
				return
			}
			err = fmt.Errorf("acemq: saga %s panicked: %v", s.name, r)
		}
	}()
	return fn(ctx, subject)
}

func (s *Saga[T]) logf(format string, args ...any) {
	if s.log != nil {
		s.log(format, args...)
	}
}

// SagaResult is what a saga did.
//
// Returned rather than raised, for the reason given on [Saga.Run].
type SagaResult struct {
	// Saga is the saga's name.
	Saga string

	// FailedAt is the step whose action failed, and empty when none did.
	FailedAt string

	// Failure is why it failed, and nil when nothing did.
	Failure error

	// Completed is the steps that ran, in order, before the failure.
	Completed []string

	// Unresolved is the steps whose compensation failed.
	//
	// This is the list to alert on. Everything else a saga reports is
	// recoverable by construction; these are real-world effects that happened,
	// were meant to be undone, and were not. Nothing else in the system knows
	// about them, and no retry will resolve them — a person has to.
	//
	// The order is the order compensation was attempted, which is the reverse
	// of the order the steps ran in.
	Unresolved []string
}

// Complete reports whether every step ran.
func (r SagaResult) Complete() bool { return r.FailedAt == "" }

// Compensated reports whether a step failed and the earlier ones were undone.
func (r SagaResult) Compensated() bool { return r.FailedAt != "" }

// HasUnresolved reports whether anything was left in a state nobody intended.
//
// It is the flag an operator needs: true means a compensation did not happen
// and no part of the system is going to try again.
func (r SagaResult) HasUnresolved() bool { return len(r.Unresolved) > 0 }

// String makes a result readable in a log line.
func (r SagaResult) String() string {
	if r.Complete() {
		return fmt.Sprintf("SagaResult{%s completed: %s}", r.Saga, strings.Join(r.Completed, " -> "))
	}
	out := fmt.Sprintf("SagaResult{%s failed at %s, compensated %v", r.Saga, r.FailedAt, r.Completed)
	if r.HasUnresolved() {
		out += fmt.Sprintf(", UNRESOLVED %v", r.Unresolved)
	}
	return out + "}"
}
