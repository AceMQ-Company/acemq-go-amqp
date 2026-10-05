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

package acemq

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestAMessageInProgressIsPutBackWithoutSpendingAnAttempt: a message somebody
// else holds is neither a success nor a failure of this one. One attempt in the
// policy, put back three times, and it is still on attempt 1 and never
// dead-lettered.
func TestAMessageInProgressIsPutBackWithoutSpendingAnAttempt(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(1, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	var attempts []int
	var outcomes []Settlement
	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, m Message[OrderPlaced]) Ack {
			OnSettled(ctx, func(s Settlement) {
				mu.Lock()
				outcomes = append(outcomes, s)
				mu.Unlock()
			})
			mu.Lock()
			attempts = append(attempts, m.Envelope.Attempt)
			n := len(attempts)
			mu.Unlock()
			if n <= 3 {
				return InProgress(5 * time.Millisecond)
			}
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}, MessageID("m-1")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "four deliveries, the last accepted", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(outcomes) == 4
	})

	mu.Lock()
	defer mu.Unlock()
	for i, a := range attempts {
		if a != 1 {
			t.Errorf("delivery %d was on attempt %d, want 1 every time (all: %v)", i+1, a, attempts)
		}
	}
	for i, s := range outcomes[:3] {
		if s.Outcome != OutcomeInProgress || s.Action != SettledInProgress || s.Delay != 5*time.Millisecond {
			t.Errorf("settlement %d = %+v, want in_progress after 5ms", i+1, s)
		}
	}
	if outcomes[3].Outcome != OutcomeAcked {
		t.Errorf("the last settlement was %q, want acked", outcomes[3].Outcome)
	}
	if n, err := mq.MessageCount(ctx, "orders.dlq"); err != nil || n != 0 {
		t.Errorf("the dead-letter queue holds %d (%v), want none", n, err)
	}
}

func TestInProgressIsNamedInProgress(t *testing.T) {
	if got := InProgress(time.Second).String(); got != "in_progress" {
		t.Errorf("String = %q", got)
	}
	if InProgress(time.Second).IsRetry() {
		t.Error("in_progress reads as a retry")
	}
}
