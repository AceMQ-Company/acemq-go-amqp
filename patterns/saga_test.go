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

package patterns_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// trail is what a saga's steps did, in the order they did it.
type trail struct {
	events []string
}

func (tr *trail) record(what string) func(context.Context, *trail) error {
	return func(context.Context, *trail) error {
		tr.events = append(tr.events, what)
		return nil
	}
}

func failing(what string, err error) func(context.Context, *trail) error {
	return func(_ context.Context, tr *trail) error {
		tr.events = append(tr.events, what)
		return err
	}
}

func newSaga(t *testing.T, name string, steps []patterns.SagaStep[*trail]) *patterns.Saga[*trail] {
	t.Helper()
	saga, err := patterns.NewSaga(name, steps)
	if err != nil {
		t.Fatal(err)
	}
	return saga
}

func TestASagaThatSucceedsListsEveryStep(t *testing.T) {
	tr := &trail{}
	saga := newSaga(t, "place-order", []patterns.SagaStep[*trail]{
		{Name: "reserve", Do: tr.record("reserve"), Undo: tr.record("release")},
		{Name: "charge", Do: tr.record("charge"), Undo: tr.record("refund")},
		{Name: "confirm", Do: tr.record("confirm")},
	})

	result := saga.Run(context.Background(), tr)

	if !result.Complete() {
		t.Fatalf("saga did not complete: %s", result)
	}
	if result.Compensated() {
		t.Error("a saga that completed reported itself compensated")
	}
	if result.HasUnresolved() {
		t.Errorf("unresolved %v, want none", result.Unresolved)
	}
	if result.Failure != nil {
		t.Errorf("failure %v, want none", result.Failure)
	}
	want := []string{"reserve", "charge", "confirm"}
	if !slices.Equal(result.Completed, want) {
		t.Errorf("completed %v, want %v", result.Completed, want)
	}
	if !slices.Equal(tr.events, want) {
		t.Errorf("ran %v, want %v", tr.events, want)
	}
}

// The behaviour the pattern exists for: what already happened is undone
// backwards, because that is the order the world was changed in.
func TestAFailedStepCompensatesInReverseOrder(t *testing.T) {
	tr := &trail{}
	boom := errors.New("the card was declined")
	saga := newSaga(t, "place-order", []patterns.SagaStep[*trail]{
		{Name: "reserve", Do: tr.record("reserve"), Undo: tr.record("release")},
		{Name: "invoice", Do: tr.record("invoice"), Undo: tr.record("void")},
		{Name: "charge", Do: failing("charge", boom), Undo: tr.record("refund")},
		{Name: "confirm", Do: tr.record("confirm")},
	})

	result := saga.Run(context.Background(), tr)

	if result.Complete() {
		t.Fatalf("saga reported itself complete: %s", result)
	}
	if result.FailedAt != "charge" {
		t.Errorf("failed at %q, want %q", result.FailedAt, "charge")
	}
	if !errors.Is(result.Failure, boom) {
		t.Errorf("failure %v, want %v", result.Failure, boom)
	}
	if want := []string{"reserve", "invoice"}; !slices.Equal(result.Completed, want) {
		t.Errorf("completed %v, want %v", result.Completed, want)
	}
	if result.HasUnresolved() {
		t.Errorf("unresolved %v, want none", result.Unresolved)
	}

	// The compensations are the last two, and they are in the reverse of the
	// order the steps ran. The failing step's own compensation is not among
	// them: its action never finished, so there is nothing of it to undo.
	want := []string{"reserve", "invoice", "charge", "void", "release"}
	if !slices.Equal(tr.events, want) {
		t.Errorf("ran %v, want %v", tr.events, want)
	}
}

// A step that only read something needs no undo, and the absence of one is not
// an error.
func TestACompletedStepWithNoCompensationIsSkipped(t *testing.T) {
	tr := &trail{}
	saga := newSaga(t, "quote", []patterns.SagaStep[*trail]{
		{Name: "reserve", Do: tr.record("reserve"), Undo: tr.record("release")},
		{Name: "read-price", Do: tr.record("read-price")}, // nothing to undo
		{Name: "charge", Do: failing("charge", errors.New("no"))},
	})

	result := saga.Run(context.Background(), tr)

	if result.HasUnresolved() {
		t.Errorf("a step with no compensation was reported unresolved: %v", result.Unresolved)
	}
	want := []string{"reserve", "read-price", "charge", "release"}
	if !slices.Equal(tr.events, want) {
		t.Errorf("ran %v, want %v", tr.events, want)
	}
}

// Stopping at the first failing compensation would leave more undone than
// carrying on, so the rest still run and the failure is collected instead.
func TestAFailingCompensationDoesNotStopTheOthers(t *testing.T) {
	tr := &trail{}
	var logged []string
	saga, err := patterns.NewSaga("place-order", []patterns.SagaStep[*trail]{
		{Name: "reserve", Do: tr.record("reserve"), Undo: tr.record("release")},
		{Name: "invoice", Do: tr.record("invoice"), Undo: failing("void", errors.New("the ledger is closed"))},
		{Name: "notify", Do: tr.record("notify"), Undo: tr.record("retract")},
		{Name: "charge", Do: failing("charge", errors.New("the card was declined"))},
	}, patterns.SagaLog[*trail](func(format string, args ...any) {
		logged = append(logged, fmt.Sprintf(format, args...))
	}))
	if err != nil {
		t.Fatal(err)
	}

	result := saga.Run(context.Background(), tr)

	// Every compensation was attempted, in reverse, including the two after the
	// one that failed.
	want := []string{"reserve", "invoice", "notify", "charge", "retract", "void", "release"}
	if !slices.Equal(tr.events, want) {
		t.Errorf("ran %v, want %v", tr.events, want)
	}
	if !result.HasUnresolved() {
		t.Fatalf("a compensation failed and nothing was reported unresolved: %s", result)
	}
	if want := []string{"invoice"}; !slices.Equal(result.Unresolved, want) {
		t.Errorf("unresolved %v, want %v", result.Unresolved, want)
	}
	if !strings.Contains(result.String(), "UNRESOLVED") {
		t.Errorf("%q does not say what an operator has to look at", result.String())
	}

	var complained bool
	for _, line := range logged {
		if strings.Contains(line, "could not compensate step invoice") {
			complained = true
		}
	}
	if !complained {
		t.Errorf("nothing was logged about the compensation that failed: %v", logged)
	}
}

// Two compensations failing are both reported, in the order they were tried.
func TestEveryFailingCompensationIsReported(t *testing.T) {
	tr := &trail{}
	saga := newSaga(t, "place-order", []patterns.SagaStep[*trail]{
		{Name: "reserve", Do: tr.record("reserve"), Undo: failing("release", errors.New("gone"))},
		{Name: "invoice", Do: tr.record("invoice"), Undo: failing("void", errors.New("closed"))},
		{Name: "charge", Do: failing("charge", errors.New("declined"))},
	})

	result := saga.Run(context.Background(), tr)

	if want := []string{"invoice", "reserve"}; !slices.Equal(result.Unresolved, want) {
		t.Errorf("unresolved %v, want %v — the order compensation was attempted in", result.Unresolved, want)
	}
}

// A panic is a failure like any other, because unwinding past the compensation
// is the one outcome this pattern exists to prevent.
func TestAPanickingStepStillCompensates(t *testing.T) {
	tr := &trail{}
	saga := newSaga(t, "place-order", []patterns.SagaStep[*trail]{
		{Name: "reserve", Do: tr.record("reserve"), Undo: tr.record("release")},
		{Name: "charge", Do: func(context.Context, *trail) error { panic("a nil map somewhere") }},
	})

	result := saga.Run(context.Background(), tr)

	if result.FailedAt != "charge" {
		t.Errorf("failed at %q, want %q", result.FailedAt, "charge")
	}
	if result.Failure == nil || !strings.Contains(result.Failure.Error(), "a nil map somewhere") {
		t.Errorf("failure %v does not carry the panic", result.Failure)
	}
	if want := []string{"reserve", "release"}; !slices.Equal(tr.events, want) {
		t.Errorf("ran %v, want %v", tr.events, want)
	}
}

func TestASagaRefusesToBeBuiltWrong(t *testing.T) {
	ok := func(_ context.Context, _ *trail) error { return nil }

	for _, c := range []struct {
		name  string
		saga  string
		steps []patterns.SagaStep[*trail]
		says  string
	}{
		{"no steps", "empty", nil, "no steps"},
		{"no name", "", []patterns.SagaStep[*trail]{{Name: "a", Do: ok}}, "needs a name"},
		{"unnamed step", "s", []patterns.SagaStep[*trail]{{Do: ok}}, "has no name"},
		{"no action", "s", []patterns.SagaStep[*trail]{{Name: "a"}}, "has no action"},
		{
			"two steps with one name", "s",
			[]patterns.SagaStep[*trail]{{Name: "a", Do: ok}, {Name: "a", Do: ok}},
			"already has a step called",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := patterns.NewSaga(c.saga, c.steps)
			if err == nil {
				t.Fatalf("built a saga that should have been refused")
			}
			if !strings.Contains(err.Error(), c.says) {
				t.Errorf("%q does not say %q", err, c.says)
			}
		})
	}
}

// The subject is the caller's, and a saga is only worth anything if the steps
// can see what the ones before them did.
func TestTheStepsShareTheSubject(t *testing.T) {
	type order struct {
		reserved bool
		charged  bool
	}

	saga, err := patterns.NewSaga("place-order", []patterns.SagaStep[*order]{
		{
			Name: "reserve",
			Do:   func(_ context.Context, o *order) error { o.reserved = true; return nil },
			Undo: func(_ context.Context, o *order) error { o.reserved = false; return nil },
		},
		{
			Name: "charge",
			Do: func(_ context.Context, o *order) error {
				if !o.reserved {
					return errors.New("charging something nobody reserved")
				}
				o.charged = true
				return errors.New("the card was declined")
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	o := &order{}
	result := saga.Run(context.Background(), o)

	if result.Complete() {
		t.Fatal("the saga completed, and the second step was meant to fail")
	}
	if o.reserved {
		t.Error("the reservation was not released")
	}
	if !o.charged {
		t.Error("the second step never saw the first step's work")
	}
}
