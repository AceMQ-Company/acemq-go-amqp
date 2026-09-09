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

// ---- where a failed message goes -------------------------------------

// TestARetryIsRepublishedRatherThanRequeued watches for the difference from the
// outside.
//
// A requeued message comes back marked redelivered and still carrying the
// attempt the publisher wrote. A republished one arrives as a new message, one
// attempt further on, which is what makes the count survive a consumer that
// restarts and a fleet that shares the queue.
func TestARetryIsRepublishedRatherThanRequeued(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(3, 0)))
	declare(t, mq, "orders")

	type delivery struct {
		attempt     int
		redelivered bool
	}
	var mu sync.Mutex
	var seen []delivery

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			mu.Lock()
			seen = append(seen, delivery{m.Envelope.Attempt, m.Redelivered})
			n := len(seen)
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

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "three deliveries", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 3
	})

	mu.Lock()
	defer mu.Unlock()
	for i, want := range []int{1, 2, 3} {
		if seen[i].attempt != want {
			t.Errorf("delivery %d reported attempt %d, want %d (all: %v)",
				i+1, seen[i].attempt, want, seen)
		}
		if seen[i].redelivered {
			t.Errorf("delivery %d arrived marked redelivered, so it was requeued rather than "+
				"republished and the attempt header cannot have advanced", i+1)
		}
	}
}

// TestAConsumerReadsTheAttemptOffTheWire is the other half of the same point.
//
// This consumer has never seen the message before. If the count lived in its
// memory it would call this the first attempt and give the message a full
// schedule of its own, which is how a message survives a policy that says three
// attempts: every consumer it lands on starts again.
func TestAConsumerReadsTheAttemptOffTheWire(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	got := make(chan int, 1)
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			got <- m.Envelope.Attempt
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	_, err = mq.PublishRaw(ctx, "", "orders", Outbound{
		Body:        []byte(`{"orderId":"o-1"}`),
		ContentType: JSONContentType,
		MessageID:   "m-1",
		Headers:     map[string]any{HeaderID: "m-1", HeaderAttempt: 4},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case attempt := <-got:
		if attempt != 4 {
			t.Errorf("Attempt = %d, want the 4 the message arrived with", attempt)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the message never arrived")
	}
}

func TestGivingUpPutsTheMessageInTheDeadLetterQueueWithTheReason(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(2, 0)))
	declare(t, mq, "orders")
	// orders.dlq is deliberately not declared here. Consume declares it, and
	// every other test below leans on the same thing.

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			return Retry(errors.New("the database timed out"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}, MessageID("m-1")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to reach the dead-letter queue", func() bool {
		n, err := mq.MessageCount(ctx, "orders.dlq")
		return err == nil && n == 1
	})

	dead, found, err := mq.Pull(ctx, "orders.dlq")
	if err != nil || !found {
		t.Fatalf("Pull: %v, found=%v", err, found)
	}
	defer func() { _ = dead.Ack() }()

	// Republished with the reason attached rather than nacked, so that whoever
	// drains this queue can see why without reading the consumer's logs — and so
	// that the message goes where this library chose rather than wherever the
	// broker's own dead-lettering points.
	if !strings.Contains(dead.Envelope.Error, "exhausted 2 attempts") {
		t.Errorf("Error = %q, want it to say the attempts ran out", dead.Envelope.Error)
	}
	if !strings.Contains(dead.Envelope.Error, "the database timed out") {
		t.Errorf("Error = %q, want the handler's reason kept", dead.Envelope.Error)
	}
	if dead.Envelope.ID != "m-1" {
		t.Errorf("ID = %q, want the message's own", dead.Envelope.ID)
	}

	// And it is gone from the queue it failed on, rather than sitting there
	// unacknowledged or coming round again.
	if n, err := mq.MessageCount(ctx, "orders"); err != nil || n != 0 {
		t.Errorf("the source queue holds %d messages (%v), want none", n, err)
	}
}

func TestARejectedMessageSaysWhoRejectedIt(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			return Reject(errors.New("no such customer"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the rejection", func() bool {
		n, err := mq.MessageCount(ctx, "orders.dlq")
		return err == nil && n == 1
	})

	dead, _, err := mq.Pull(ctx, "orders.dlq")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dead.Ack() }()

	if !strings.Contains(dead.Envelope.Error, "no such customer") {
		t.Errorf("Error = %q", dead.Envelope.Error)
	}
}

func TestABodyThatWillNotDecodeIsParkedRatherThanDeadLettered(t *testing.T) {
	// Different problems with different answers: a message that failed five
	// times is usually the world, and a message nothing could read is usually a
	// producer. Whoever drains the dead letters should not have to sort them by
	// hand.
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)))
	declare(t, mq, "orders")

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	_, err = mq.transport.Publish(ctx, "", "orders", Outbound{
		Body:        []byte("this is not json"),
		ContentType: JSONContentType,
		MessageID:   "m-1",
		Headers:     map[string]any{HeaderID: "m-1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to be parked", func() bool {
		n, err := mq.MessageCount(ctx, "orders.parked")
		return err == nil && n == 1
	})

	parked, _, err := mq.Pull(ctx, "orders.parked")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parked.Ack() }()

	if !strings.Contains(parked.Envelope.Error, "could not be decoded") {
		t.Errorf("Error = %q", parked.Envelope.Error)
	}
	if n, err := mq.MessageCount(ctx, "orders.dlq"); err != nil || n != 0 {
		t.Errorf("the dead-letter queue holds %d messages (%v), want the parking lot to have it",
			n, err)
	}
}

func TestALongWaitIsHandedToTheBrokerAndNotHeldHere(t *testing.T) {
	ctx := context.Background()
	policy := FixedRetry(3, time.Minute)
	mq := brokerFor(t, WithRetry(policy))
	declare(t, mq, "orders")

	ladder := LadderFor("orders", policy)
	if err := ladder.Declare(ctx, mq); err != nil {
		t.Fatal(err)
	}

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			return Retry(errors.New("the payment gateway is down"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to reach the rung", func() bool {
		n, err := mq.MessageCount(ctx, "orders.retry.1m")
		return err == nil && n == 1
	})

	// The point of the whole arrangement: the minute is the broker's. Nothing is
	// held here, so a restart in the next fifty-nine seconds costs nothing.
	if n, err := mq.MessageCount(ctx, "orders"); err != nil || n != 0 {
		t.Errorf("the source queue holds %d messages (%v); the wait is being held here", n, err)
	}

	waiting, _, err := mq.Pull(ctx, "orders.retry.1m")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = waiting.Ack() }()

	if waiting.Envelope.Attempt != 2 {
		t.Errorf("Attempt = %d on the rung, want 2: a message that has been round the broker "+
			"is no less on its second attempt than one that waited here",
			waiting.Envelope.Attempt)
	}
}

func TestARungThatIsNotThereFallsBackToWaitingHere(t *testing.T) {
	// Degraded rather than fatal. The message is still deliverable and waiting
	// for it here is what this library did before there were rungs — but it is
	// counted, because a broker that has lost a rung otherwise looks like it
	// works.
	ctx := context.Background()
	metrics := NewMetrics()
	policy := FixedRetry(2, 50*time.Millisecond).WaitInBrokerFrom(10 * time.Millisecond)
	mq := brokerFor(t, WithRetry(policy), WithObserver(metrics))
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

	// Removed after the consumer started, because starting is now the one moment
	// the rung is certain to be there: Consume declares the whole ladder. A rung
	// still goes missing — somebody deletes it, a policy changes, a broker is
	// restored from a backup taken before it existed — and this is that.
	rung, ok := LadderFor("orders", policy).RungFor(50 * time.Millisecond)
	if !ok {
		t.Fatal("the policy has no rung to remove, so this test proves nothing")
	}
	if err := mq.DeleteQueue(ctx, rung); err != nil {
		t.Fatalf("cannot remove the rung %q: %v", rung, err)
	}

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the retry that had nowhere to wait", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})

	if total := countOf(metrics, MetricRungMissing); total == 0 {
		t.Errorf("%s was never counted, so a missing rung is silent", MetricRungMissing)
	}
}

// countOf adds up a metric across whatever labels it was counted with.
func countOf(m *Metrics, metric string) int64 {
	var total int64
	for key, value := range m.Counts() {
		if key == metric || strings.HasPrefix(key, metric+"{") {
			total += value
		}
	}
	return total
}

// TestAHandlerCanParkAMessageItCannotRead is the other half of the parking
// lot. The engine parks a body the codec refused; a handler that decoded fine
// and only then found the message unreadable — a field naming a schema nobody
// deployed, a reference into a system that never had one — used to have to
// reject it into the dead letters, which is the queue for work that failed
// rather than for messages nobody could read.
func TestAHandlerCanParkAMessageItCannotRead(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithRetry(FixedRetry(5, 0)), WithObserver(metrics))
	declare(t, mq, "orders")

	var settled settlements
	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, _ Message[OrderPlaced]) Ack {
			OnSettled(ctx, settled.record)
			return Park(errors.New("schema version 9 is from a future nobody deployed"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to be parked", func() bool {
		n, err := mq.MessageCount(ctx, "orders.parked")
		return err == nil && n == 1
	})

	parked, _, err := mq.Pull(ctx, "orders.parked")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = parked.Ack() }()

	if !strings.Contains(parked.Envelope.Error, "schema version 9") {
		t.Errorf("Error = %q, want the handler's reason", parked.Envelope.Error)
	}
	if n, err := mq.MessageCount(ctx, "orders.dlq"); err != nil || n != 0 {
		t.Errorf("the dead-letter queue holds %d messages (%v); parking must not dead-letter",
			n, err)
	}

	// The counter and the span have to agree, so the settlement carries the
	// same word the counter is tagged with.
	waitFor(t, "the settlement", func() bool { return settled.count() == 1 })
	got := settled.snapshot()[0]
	if got.Action != SettledParked {
		t.Errorf("Action = %q, want %q", got.Action, SettledParked)
	}
	if got.Outcome != OutcomeParked {
		t.Errorf("Outcome = %q, want %q", got.Outcome, OutcomeParked)
	}

	if total := countOf(metrics, MetricParked); total != 1 {
		t.Errorf("%s = %d, want 1", MetricParked, total)
	}
	if total := countOf(metrics, MetricRejected); total != 0 {
		t.Errorf("%s = %d, want 0: a park is not a rejection", MetricRejected, total)
	}
	if total := countOf(metrics, MetricDeadLettered); total != 0 {
		t.Errorf("%s = %d, want 0", MetricDeadLettered, total)
	}
	if total := countOf(metrics, MetricConsumed); total != 1 {
		t.Errorf("%s = %d, want 1", MetricConsumed, total)
	}
}

func TestParkReadsAsParkInALogLine(t *testing.T) {
	if got := Park(errors.New("unreadable")).String(); got != "park" {
		t.Errorf("String() = %q, want park", got)
	}
	if got := Park(errors.New("unreadable")).Err(); got == nil {
		t.Error("Err() lost the reason the handler gave")
	}
}

// TestThePublishCounterIsTaggedRoutingKey pins the tag name the family agreed
// on. It was key here and in Python and routing.key in Java and .NET; a
// dashboard that groups publishes by it cannot be right in both spellings.
func TestThePublishCounterIsTaggedRoutingKey(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithObserver(metrics))
	declare(t, mq, "orders")

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	if TagRoutingKey != "routing.key" {
		t.Errorf("TagRoutingKey = %q, want routing.key", TagRoutingKey)
	}
	want := MetricPublished + "{exchange=}{routing.key=orders}"
	if got := metrics.Counts()[want]; got != 1 {
		t.Errorf("no counter keyed %q; got %v", want, metrics.Counts())
	}
}
