// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package patterns_test

import (
	"context"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// What a process that died mid-handler leaves behind, and what must happen next.
//
// The sequence this pins is the one an OOM kill produces. A delivery arrives, the key
// is claimed, the handler starts, and the process is gone before it finishes -- so no
// deferred call runs, nothing is released, and the claim is still in the store when the
// broker redelivers the message to whoever takes over.
//
// The work has not been done. So the redelivery must run the handler. A store that
// cannot tell a claim from a completion answers "already handled", the wrapper accepts
// the message, and the work is lost -- not duplicated, which is the failure this
// pattern is allowed to have, but *lost*, which is the one it exists to prevent.
//
// Java's IdempotencyStore javadoc names this exact trap: "mark before the handler runs
// and a crash mid-handler means the work never happens and never can, because the
// message now looks handled". Java, Python and Ruby each answer it with a claim that
// expires. This is the test that says Go does too.
func TestAClaimAbandonedByACrashDoesNotSuppressTheRedelivery(t *testing.T) {
	ctx := context.Background()

	// A claim window short enough for a test to outlive. Production wants minutes;
	// what matters here is that a window exists at all.
	store := patterns.NewInMemoryIdempotencyStore(time.Hour)
	if claimer, ok := any(store).(interface{ SetClaimTimeout(time.Duration) }); ok {
		claimer.SetClaimTimeout(time.Millisecond)
	}

	const key = "order-A-1"

	// The process that is about to die takes the claim.
	claimed, err := store.Claim(ctx, key)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !claimed {
		t.Fatal("a fresh key must be claimable")
	}

	// ...and dies here. Nothing releases, nothing confirms.

	// Long enough for the abandoned claim to expire.
	time.Sleep(20 * time.Millisecond)

	// The broker redelivers to a consumer that took over.
	handled := 0
	handler := patterns.Idempotent(store,
		func(_ context.Context, _ acemq.Message[map[string]any]) acemq.Ack {
			handled++
			return acemq.Accept()
		})

	ack := handler(ctx, acemq.Message[map[string]any]{
		Envelope: acemq.Envelope{ID: key},
	})

	if handled != 1 {
		t.Fatalf("the redelivery ran the handler %d times, want 1: the work a crashed "+
			"process never finished has been silently dropped", handled)
	}
	if ack.String() != "accept" {
		t.Errorf("ack = %q, want accept", ack.String())
	}
}

// The other half, which must not regress while the first is being fixed: a key that was
// confirmed is a completion, and the redelivery of it must not run the work again.
func TestAConfirmedKeySuppressesTheRedelivery(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryIdempotencyStore(time.Hour)

	const key = "order-A-2"

	handled := 0
	handler := patterns.Idempotent(store,
		func(_ context.Context, _ acemq.Message[map[string]any]) acemq.Ack {
			handled++
			return acemq.Accept()
		})

	msg := acemq.Message[map[string]any]{Envelope: acemq.Envelope{ID: key}}

	if ack := handler(ctx, msg); ack.String() != "accept" {
		t.Fatalf("first delivery: ack = %q", ack.String())
	}
	if handled != 1 {
		t.Fatalf("first delivery ran the handler %d times, want 1", handled)
	}

	// The same message again, as a broker redelivering something already done.
	if ack := handler(ctx, msg); ack.String() != "accept" {
		t.Fatalf("redelivery: ack = %q, want accept", ack.String())
	}
	if handled != 1 {
		t.Fatalf("the handler ran %d times across a delivery and its duplicate, want 1", handled)
	}
}

// A handler that fails releases the claim, so the retry can actually run. This already
// worked through Forget and must keep working through Release.
func TestAFailedHandlerReleasesTheClaim(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryIdempotencyStore(time.Hour)

	const key = "order-A-3"

	attempts := 0
	handler := patterns.Idempotent(store,
		func(_ context.Context, _ acemq.Message[map[string]any]) acemq.Ack {
			attempts++
			if attempts == 1 {
				return acemq.Retry(context.DeadlineExceeded)
			}
			return acemq.Accept()
		})

	msg := acemq.Message[map[string]any]{Envelope: acemq.Envelope{ID: key}}

	if ack := handler(ctx, msg); ack.String() == "accept" {
		t.Fatal("the first attempt failed and must not be accepted")
	}
	if ack := handler(ctx, msg); ack.String() != "accept" {
		t.Fatalf("the retry must run and succeed, got %q", ack.String())
	}
	if attempts != 2 {
		t.Fatalf("handler ran %d times, want 2", attempts)
	}
}
