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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// settlements collects what the engine said, from whichever goroutine said it.
type settlements struct {
	mu  sync.Mutex
	all []Settlement
}

func (s *settlements) record(got Settlement) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.all = append(s.all, got)
}

func (s *settlements) snapshot() []Settlement {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Settlement(nil), s.all...)
}

func (s *settlements) count() int {
	return len(s.snapshot())
}

// TestAMessageThatRanOutOfAttemptsIsSettledAsDeadLettered is the gap this seam
// exists to close. The handler asked for a retry and there was none left to
// give, so what happened to the message is a dead letter — and anything that
// stopped at the handler's answer would report a retry that never occurred.
func TestAMessageThatRanOutOfAttemptsIsSettledAsDeadLettered(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(1, 0)))
	declare(t, mq, "orders")

	var settled settlements
	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, _ Message[OrderPlaced]) Ack {
			OnSettled(ctx, settled.record)
			return Retry(errors.New("the pricing service is down"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the settlement", func() bool { return settled.count() == 1 })

	got := settled.snapshot()[0]
	if got.Action != SettledDeadLettered {
		t.Errorf("Action = %q, want %q", got.Action, SettledDeadLettered)
	}
	if got.Queue != "orders" {
		t.Errorf("Queue = %q, want orders", got.Queue)
	}
	if !strings.Contains(got.Reason, "exhausted 1 attempt") {
		t.Errorf("Reason = %q, want the reason it was given up on", got.Reason)
	}
	if !strings.Contains(got.Reason, "the pricing service is down") {
		t.Errorf("Reason = %q, want the handler's reason in it", got.Reason)
	}
}

// The delay has to be the one the engine chose rather than the one the policy
// was written with: they differ once jitter or a rung queue is involved, and the
// chosen one is what a reader is trying to account for.
func TestARetryIsSettledWithTheDelayItChose(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(3, 20*time.Millisecond)))
	declare(t, mq, "orders")

	var settled settlements
	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, m Message[OrderPlaced]) Ack {
			OnSettled(ctx, settled.record)
			if m.Envelope.Attempt == 1 {
				return Retry(errors.New("the pricing service is down"))
			}
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both settlements", func() bool { return settled.count() == 2 })

	all := settled.snapshot()
	if all[0].Action != SettledRetried {
		t.Errorf("the first settlement is %q, want %q", all[0].Action, SettledRetried)
	}
	if all[0].Delay != 20*time.Millisecond {
		t.Errorf("Delay = %s, want 20ms", all[0].Delay)
	}
	if all[0].Envelope.Attempt != 2 {
		t.Errorf("Attempt = %d, want the 2 the message goes back as",
			all[0].Envelope.Attempt)
	}
	if all[1].Action != SettledAccepted {
		t.Errorf("the second settlement is %q, want %q", all[1].Action, SettledAccepted)
	}
}

// A handler run by something other than a consumer — a test, or code of your
// own — gets no engine, and anything it was going to finish on the settlement
// has to be finished by hand instead. Saying so is what lets a caller tell the
// two apart.
func TestOnSettledSaysNoWhenThereIsNoEngine(t *testing.T) {
	if OnSettled(context.Background(), func(Settlement) {}) {
		t.Error("a plain context claimed to have an engine behind it")
	}

	mq := brokerFor(t)
	declare(t, mq, "orders")

	registered := make(chan bool, 1)
	sub, err := Consume(context.Background(), mq, "orders",
		func(ctx context.Context, _ Message[OrderPlaced]) Ack {
			registered <- OnSettled(ctx, func(Settlement) {})
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(context.Background(), OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-registered:
		if !ok {
			t.Error("a handler running under a consumer was given no settlement to register on")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the handler never ran")
	}
}

// A registration belongs to the delivery it was made on. Workers are reused, so
// one that outlived its message would report the next one's fate to the wrong
// reader.
func TestARegistrationDoesNotOutliveItsDelivery(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	var settled settlements
	seen := make(chan struct{}, 4)

	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, m Message[OrderPlaced]) Ack {
			// Only the first message registers; the second must settle to
			// nobody.
			if m.Payload.OrderID == "o-1" {
				OnSettled(ctx, settled.record)
			}
			seen <- struct{}{}
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	for _, id := range []string{"o-1", "o-2"} {
		if err := pub.Send(ctx, OrderPlaced{OrderID: id}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-seen:
		case <-time.After(3 * time.Second):
			t.Fatal("the messages did not both arrive")
		}
	}
	waitFor(t, "the one settlement", func() bool { return settled.count() == 1 })

	// A moment for a second call to arrive, if the hook were leaking.
	time.Sleep(50 * time.Millisecond)
	if got := settled.count(); got != 1 {
		t.Errorf("%d settlements, want 1: the registration outlived its delivery", got)
	}
}
