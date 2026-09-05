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

type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}

// brokerFor gives each test its own in-memory broker, so tests can run in
// parallel without seeing each other's messages.
func brokerFor(t *testing.T, opts ...ConnOption) *Conn {
	t.Helper()
	mq, err := Connect(context.Background(), "memory://"+t.Name(), opts...)
	if err != nil {
		t.Fatalf("cannot connect: %v", err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

func declare(t *testing.T, mq *Conn, queue string) {
	t.Helper()
	if err := mq.DeclareQueue(context.Background(), queue); err != nil {
		t.Fatalf("cannot declare %q: %v", queue, err)
	}
}

// waitFor gives a condition a bounded time to come true, so a failing test says
// what did not happen rather than hanging.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestAMessageGoesRoundTrip(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	got := make(chan Message[OrderPlaced], 1)
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			got <- m
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250}); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.Payload.OrderID != "o-1" || m.Payload.TotalCents != 4250 {
			t.Errorf("payload = %+v", m.Payload)
		}
		if m.Envelope.Type != "orders" {
			t.Errorf("Type = %q, want the routing key", m.Envelope.Type)
		}
		if m.Envelope.Attempt != 1 {
			t.Errorf("Attempt = %d, want 1 on a first delivery", m.Envelope.Attempt)
		}
		if m.ContentType != JSONContentType {
			t.Errorf("ContentType = %q", m.ContentType)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the message never arrived")
	}
}

// TestTheAttemptCounterAdvancesOnRedelivery is the one that matters most here.
//
// A broker requeues the bytes it was given, so the attempt header on the wire
// still reads 1 however many times a message has come back. Counting the header
// rather than the redelivery makes a retry limit that never trips, and the
// message goes round for ever.
func TestTheAttemptCounterAdvancesOnRedelivery(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(3, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	var attempts []int

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			attempts = append(attempts, m.Envelope.Attempt)
			n := len(attempts)
			mu.Unlock()

			if n < 3 {
				return Retry(errors.New("not yet"))
			}
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "three deliveries", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 3
	})

	mu.Lock()
	defer mu.Unlock()
	want := []int{1, 2, 3}
	for i, w := range want {
		if attempts[i] != w {
			t.Errorf("delivery %d reported attempt %d, want %d (the whole sequence was %v)",
				i+1, attempts[i], w, attempts)
		}
	}
}

func TestRetryingStopsWhenTheAttemptsRunOut(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(2, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			return Retry(errors.New("still broken"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "both attempts", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})

	// Two attempts and no more: the message is dead-lettered rather than going
	// round for ever.
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("the handler ran %d times, want exactly 2", calls)
	}
}

func TestAFatalReasonSkipsTheRemainingAttempts(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			// Asking for a retry but marking the reason as one that will not
			// change. The mark wins: four more attempts would fail identically.
			return Retry(Fatal(errors.New("this order has no customer")))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the first attempt", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 1
	})
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("the handler ran %d times, want exactly 1 despite 5 attempts being allowed", calls)
	}
}

func TestRejectingDoesNotTryAgain(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			return Reject(errors.New("no such customer"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the delivery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 1
	})
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("the handler ran %d times, want exactly 1", calls)
	}
}

func TestABodyThatWillNotDecodeIsNotRetried(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Published straight at the transport so the body is not what the codec
	// would have written.
	_, err = mq.transport.Publish(ctx, "", "orders", Outbound{
		Body:        []byte("this is not json"),
		ContentType: JSONContentType,
		MessageID:   "m-1",
		Headers:     map[string]any{HeaderID: "m-1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Errorf("the handler saw %d messages; a body that will not decode should never reach it", calls)
	}
}

func TestAPanickingHandlerDoesNotStopTheConsumer(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	var mu sync.Mutex
	var seen []string

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			seen = append(seen, m.Payload.OrderID)
			mu.Unlock()

			if m.Payload.OrderID == "boom" {
				panic("a bug in the handler")
			}
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "boom"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Send(ctx, OrderPlaced{OrderID: "fine"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message after the panic", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 2
	})

	mu.Lock()
	defer mu.Unlock()
	if seen[1] != "fine" {
		t.Errorf("after the panic the consumer saw %q, want it still working", seen[1])
	}
	// And the message that panicked is not retried, because a bug repeats.
	time.Sleep(150 * time.Millisecond)
	count := 0
	for _, id := range seen {
		if id == "boom" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("the panicking message was delivered %d times, want 1", count)
	}
}

func TestClosingWaitsForAHandlerAlreadyRunning(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	started := make(chan struct{})
	finished := make(chan struct{})

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			close(started)
			time.Sleep(200 * time.Millisecond)
			close(finished)
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	<-started
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-finished:
	default:
		t.Error("Close returned while the handler was still running; the message would be redone elsewhere")
	}
}

func TestClosingTwiceIsNotAnError(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() })
	if err != nil {
		t.Fatal(err)
	}

	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("the second Close returned %v", err)
	}
}

func TestApplicationHeadersReachTheHandler(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	got := make(chan Message[OrderPlaced], 1)
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			got <- m
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	err = pub.Send(ctx, OrderPlaced{OrderID: "o-1"},
		CorrelationID("corr-1"),
		CausationID("cause-1"),
		Header("x-tenant", "acme"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.Envelope.CorrelationID != "corr-1" {
			t.Errorf("CorrelationID = %q", m.Envelope.CorrelationID)
		}
		if m.Envelope.CausationID != "cause-1" {
			t.Errorf("CausationID = %q", m.Envelope.CausationID)
		}
		if m.Envelope.Headers["x-tenant"] != "acme" {
			t.Errorf("headers = %v", m.Envelope.Headers)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the message never arrived")
	}
}

func TestSendingARefusedHeaderFailsTheSend(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"}, Header("x-acemq-id", "mine"))

	if err == nil {
		t.Fatal("a reserved header name was accepted")
	}
}

func TestConcurrentHandlersRunAtOnce(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	var mu sync.Mutex
	inFlight, peak := 0, 0
	release := make(chan struct{})

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			inFlight++
			if inFlight > peak {
				peak = inFlight
			}
			mu.Unlock()

			<-release

			mu.Lock()
			inFlight--
			mu.Unlock()
			return Accept()
		}, Concurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	for i := range 4 {
		if err := pub.Send(ctx, OrderPlaced{OrderID: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "several handlers at once", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return peak > 1
	})
	close(release)
}

func TestConsumingFromAQueueThatIsNotThereSaysSo(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	_, err := Consume(ctx, mq, "never-declared",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() })

	if err == nil {
		t.Fatal("consuming from an undeclared queue succeeded")
	}
}

func TestAnUnknownSchemeSaysWhatToImport(t *testing.T) {
	_, err := Connect(context.Background(), "amqp://localhost:5672/")

	if err == nil {
		t.Fatal("connecting with no transport registered succeeded")
	}
	// The fix is a blank import, and an error that does not say so leaves
	// somebody guessing at a registry they cannot see.
	if got := err.Error(); !strings.Contains(got, "acemq-go-amqp/rabbitmq") {
		t.Errorf("the error does not say what to import: %v", got)
	}
}

// ---- publishing results ----------------------------------------------

func TestAnUnroutableMandatoryMessageIsAnError(t *testing.T) {
	// The quietest failure in messaging: the publisher succeeds, no queue is
	// bound, the broker drops the message, and the consumer waits for ever with
	// nothing anywhere saying why.
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareExchange(ctx, "events", "topic"); err != nil {
		t.Fatal(err)
	}

	pub := NewPublisher[OrderPlaced](mq, "events", "nothing.listens.here", Mandatory[OrderPlaced]())
	err := pub.Send(ctx, OrderPlaced{OrderID: "o-1"})

	if err == nil {
		t.Fatal("an unroutable message was published without complaint")
	}
	if !strings.Contains(err.Error(), "reached no queue") {
		t.Errorf("the error does not say what happened: %v", err)
	}
}

func TestAnUnroutableMessageIsSilentWithoutMandatory(t *testing.T) {
	// Not a bug: it is what AMQP does, and what most publishers want. The test
	// records the difference so that Mandatory has something to be different
	// from.
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareExchange(ctx, "events", "topic"); err != nil {
		t.Fatal(err)
	}

	err := NewPublisher[OrderPlaced](mq, "events", "nothing.listens.here").
		Send(ctx, OrderPlaced{OrderID: "o-1"})

	if err != nil {
		t.Fatalf("publishing without Mandatory reported a routing problem: %v", err)
	}
}

func TestTheResultSaysWhereTheMessageWent(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	result, err := NewPublisher[OrderPlaced](mq, "", "orders").
		SendResult(ctx, OrderPlaced{OrderID: "o-1"}, MessageID("m-1"))
	if err != nil {
		t.Fatal(err)
	}

	if result.MessageID != "m-1" {
		t.Errorf("MessageID = %q", result.MessageID)
	}
	if !result.Confirmed {
		t.Error("the in-memory broker did not confirm a message it already holds")
	}
	if !result.Routed {
		t.Error("Routed is false for a message that reached a queue")
	}
}

func TestTheResultReportsAnUnroutableMessage(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareExchange(ctx, "events", "topic"); err != nil {
		t.Fatal(err)
	}

	result, _ := NewPublisher[OrderPlaced](mq, "events", "nothing.listens.here").
		SendResult(ctx, OrderPlaced{OrderID: "o-1"})

	if result.Routed {
		t.Error("Routed is true for a message no queue received")
	}
	if result.ReturnReason == "" {
		t.Error("nothing explains why it was not routed")
	}
}
