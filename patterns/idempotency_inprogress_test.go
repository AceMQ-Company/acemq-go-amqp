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
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// stickyStore is a store whose Release fails: what a handler that died, and
// whose clean-up could not reach the store, leaves behind.
type stickyStore struct {
	*patterns.InMemoryIdempotencyStore
}

func (stickyStore) Release(context.Context, string) error {
	return errors.New("the store is not answering")
}

func TestTheMemoryStoreTellsAClaimFromADuplicateFromWorkInProgress(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryIdempotencyStore(time.Hour)

	for _, step := range []struct {
		do   func()
		want patterns.ClaimResult
	}{
		{nil, patterns.Claimed},
		{nil, patterns.InProgress},
		{func() { _ = store.Confirm(ctx, "k") }, patterns.Duplicate},
	} {
		if step.do != nil {
			step.do()
		}
		got, err := store.TryClaim(ctx, "k")
		if err != nil || got != step.want {
			t.Fatalf("TryClaim = %v, %v; want %v", got, err, step.want)
		}
	}

	// A lease nobody confirmed ends, and then the key is retaken.
	store.SetClaimTimeout(time.Millisecond)
	_, _ = store.TryClaim(ctx, "abandoned")
	time.Sleep(10 * time.Millisecond)
	if got, _ := store.TryClaim(ctx, "abandoned"); got != patterns.Claimed {
		t.Fatalf("an expired claim answered %v, want claimed", got)
	}

	// Claim keeps its two-valued answer.
	if first, _ := store.Claim(ctx, "k"); first {
		t.Error("Claim took a confirmed key")
	}
}

// The bug: the claim outlived the failure, and the redelivery was acknowledged
// as a duplicate of work that never happened.
func TestAFailedHandlerWhoseReleaseFailedIsPutBackNotAccepted(t *testing.T) {
	ctx := context.Background()
	ran := 0
	handler := patterns.Idempotent(stickyStore{patterns.NewInMemoryIdempotencyStore(time.Hour)},
		func(context.Context, acemq.Message[string]) acemq.Ack {
			ran++
			return acemq.Retry(errors.New("died mid-work"))
		})
	msg := acemq.Message[string]{Envelope: acemq.Envelope{ID: "order-1"}}

	if ack := handler(ctx, msg); !ack.IsRetry() {
		t.Fatalf("first delivery: %v", ack)
	}
	again := handler(ctx, msg)

	if again != acemq.InProgress(acemq.DefaultInProgressDelay) {
		t.Fatalf("the redelivery answered %v, want in_progress after the default delay", again)
	}
	if ran != 1 {
		t.Errorf("the handler ran %d times, want 1", ran)
	}
}

func TestTheInProgressDelayIsTheOneAskedFor(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryIdempotencyStore(time.Hour)
	_, _ = store.TryClaim(ctx, "order-1")

	never := func(context.Context, acemq.Message[string]) acemq.Ack {
		t.Fatal("the handler should not have been reached")
		return acemq.Accept()
	}
	msg := acemq.Message[string]{Envelope: acemq.Envelope{ID: "order-1"}}

	if got := patterns.Idempotent(store, never, patterns.InProgressDelay(3*time.Second))(ctx, msg); got != acemq.InProgress(3*time.Second) {
		t.Errorf("Idempotent answered %v", got)
	}
	by := patterns.IdempotentBy(store, func(m acemq.Message[string]) string { return m.Envelope.ID },
		never, patterns.InProgressDelay(time.Second))
	if got := by(ctx, msg); got != acemq.InProgress(time.Second) {
		t.Errorf("IdempotentBy answered %v", got)
	}
}

// legacyStore implements only the two-valued Claim, as a store written before
// the three-way answer did. It keeps its old meaning: refused is a duplicate.
type legacyStore struct{ patterns.IdempotencyStore }

func TestAStoreWithoutTryClaimIsTreatedAsBefore(t *testing.T) {
	ctx := context.Background()
	inner := patterns.NewInMemoryIdempotencyStore(time.Hour)
	_, _ = inner.TryClaim(ctx, "order-1")

	got := patterns.Idempotent(legacyStore{inner},
		func(context.Context, acemq.Message[string]) acemq.Ack { return acemq.Retry(nil) })(
		ctx, acemq.Message[string]{Envelope: acemq.Envelope{ID: "order-1"}})

	if got.String() != "accept" {
		t.Errorf("a refused legacy claim answered %v, want accept", got)
	}
}
