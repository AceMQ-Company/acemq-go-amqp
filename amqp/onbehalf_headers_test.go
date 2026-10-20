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

// What a message keeps when the library republishes it on the caller's behalf,
// what a consumer with no retry policy does, and what a publisher does with a
// message nothing is bound to receive: TCK cases RETRY-004, PARK-001, PARK-002
// and PUB-003 as unit tests.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// foreign is what a newer or non-AceMQ publisher might send: a full envelope,
// one x-acemq- header this version has no field for, and an application header.
func foreign(id string, attempt int) map[string]any {
	return map[string]any{
		HeaderID: id, HeaderType: "order.placed", HeaderVersion: 2,
		HeaderCorrelation: "corr-1", HeaderCausation: "cause-1", HeaderAttempt: attempt,
		HeaderFirstSeen: int64(1767323045678), HeaderOrigin: "orders@host-7",
		"x-acemq-future": "kept", "x-tenant": "acme",
	}
}

// pullWire takes the one message off a queue and returns the headers it would
// carry on the wire.
func pullWire(t *testing.T, mq *Conn, queue string) (*Pulled, map[string]any) {
	t.Helper()
	m, found, err := mq.Pull(context.Background(), queue)
	if err != nil || !found {
		t.Fatalf("Pull %s: %v, found=%v", queue, err, found)
	}
	t.Cleanup(func() { _ = m.Ack() })
	return m, m.Envelope.ToWire()
}

func TestAnUnknownReservedHeaderIsWrittenBackButNotShown(t *testing.T) {
	env := EnvelopeFromWire(foreign("m-1", 1), "orders", "m-1")
	if _, shown := env.Headers["x-acemq-future"]; shown {
		t.Error("an unknown reserved header reached the application's headers")
	}
	if got := env.ToWire()["x-acemq-future"]; got != "kept" {
		t.Errorf("ToWire wrote x-acemq-future = %v, want it carried unchanged", got)
	}
	// A known field always wins over a stale copy.
	next := env.NextAttempt()
	if got := next.ToWire()[HeaderAttempt]; got != 2 {
		t.Errorf("attempt on the wire = %v, want 2", got)
	}
}

// RETRY-004: no retry policy is one attempt, then {queue}.dlq with every header.
func TestAConsumerWithNoRetryPolicyDeadLettersAfterOneAttempt(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t) // no WithRetry, no RetryWith
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0
	sub, err := Consume(ctx, mq, "orders", func(_ context.Context, _ Message[OrderPlaced]) Ack {
		mu.Lock()
		calls++
		mu.Unlock()
		return Retry(errors.New("still broken"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if _, err := mq.transport.Publish(ctx, "", "orders", Outbound{
		Body: []byte(`{"orderId":"o-1"}`), ContentType: JSONContentType, MessageID: "m-1",
		Headers: foreign("m-1", 1),
	}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the dead letter", func() bool {
		n, err := mq.MessageCount(ctx, "orders.dlq")
		return err == nil && n == 1
	})
	dead, wire := pullWire(t, mq, "orders.dlq")

	mu.Lock()
	if calls != 1 {
		t.Errorf("the handler ran %d times, want 1", calls)
	}
	mu.Unlock()
	if wire[HeaderAttempt] != 1 {
		t.Errorf("attempt = %v, want 1", wire[HeaderAttempt])
	}
	if wire["x-acemq-future"] != "kept" || wire["x-tenant"] != "acme" {
		t.Errorf("headers were dropped on the dead letter: %v", wire)
	}
	if !strings.Contains(dead.Envelope.Error, "no retry policy") {
		t.Errorf("Error = %q, want it to say there was no policy", dead.Envelope.Error)
	}
	if n, _ := mq.MessageCount(ctx, "orders"); n != 0 {
		t.Errorf("the source queue holds %d, want none", n)
	}
}

// PARK-001: an undecodable body is parked once, every header unchanged.
func TestAnUndecodableBodyIsParkedWithEveryHeaderItArrivedWith(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, WithRetry(FixedRetry(3, 0)))
	declare(t, mq, "orders")

	sub, err := Consume(ctx, mq, "orders", func(_ context.Context, _ Message[OrderPlaced]) Ack {
		t.Error("the handler saw a body that does not decode")
		return Accept()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if _, err := mq.transport.Publish(ctx, "", "orders", Outbound{
		Body: []byte("not json{"), ContentType: JSONContentType, MessageID: "bad-1",
		Headers: foreign("bad-1", 2),
	}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the park", func() bool {
		n, err := mq.MessageCount(ctx, "orders.parked")
		return err == nil && n == 1
	})
	parked, wire := pullWire(t, mq, "orders.parked")

	for name, want := range foreign("bad-1", 2) {
		if got := wire[name]; got != want {
			t.Errorf("parked %s = %v (%T), want %v (%T)", name, got, got, want, want)
		}
	}
	if string(parked.Body) != "not json{" || parked.ContentType != JSONContentType {
		t.Errorf("body or content type changed: %q %q", parked.Body, parked.ContentType)
	}
	if parked.Envelope.Error == "" {
		t.Error("the parked copy carries no reason")
	}
}

// PARK-002: a park the broker does not take requeues, and parks once it can.
func TestAParkThatDoesNotLandGoesBackToTheQueue(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithRetry(FixedRetry(3, 0)), WithObserver(metrics))
	declare(t, mq, "orders")

	sub, err := Consume(ctx, mq, "orders", func(_ context.Context, _ Message[OrderPlaced]) Ack {
		return Accept()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Consume declared orders.parked; take it away so the park is unroutable.
	if err := mq.DeleteQueue(ctx, "orders.parked"); err != nil {
		t.Fatal(err)
	}
	if _, err := mq.transport.Publish(ctx, "", "orders", Outbound{
		Body: []byte("not json{"), ContentType: JSONContentType, MessageID: "bad-2",
		Headers: map[string]any{HeaderID: "bad-2", HeaderAttempt: 1},
	}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the refused park", func() bool { return countOf(metrics, MetricSetAsideFailed) > 0 })
	declare(t, mq, "orders.parked")
	waitFor(t, "the park to land once the queue is back", func() bool {
		n, err := mq.MessageCount(ctx, "orders.parked")
		return err == nil && n == 1
	})
	waitFor(t, "the source queue to empty", func() bool {
		n, err := mq.MessageCount(ctx, "orders")
		return err == nil && n == 0
	})
	if n, _ := mq.MessageCount(ctx, "orders.dlq"); n != 0 {
		t.Errorf("orders.dlq holds %d, want none", n)
	}
}

// PUB-003 and PUB-006: mandatory by default, AllowUnroutable to opt out.
func TestAPublisherIsMandatoryUnlessItOptsOut(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	err := NewPublisher[OrderPlaced](mq, "", "nowhere").Send(ctx, OrderPlaced{OrderID: "o-1"})
	var failed *PublishFailedError
	if !errors.As(err, &failed) || !failed.Unroutable {
		t.Fatalf("a default publish to nowhere returned %v, want an unroutable *PublishFailedError", err)
	}

	if err := NewPublisher[OrderPlaced](mq, "", "nowhere", AllowUnroutable[OrderPlaced]()).
		Send(ctx, OrderPlaced{OrderID: "o-2"}); err != nil {
		t.Errorf("AllowUnroutable still failed: %v", err)
	}
	if err := NewPublisher[OrderPlaced](mq, "", "nowhere", AllowUnroutable[OrderPlaced](),
		Mandatory[OrderPlaced]()).Send(ctx, OrderPlaced{OrderID: "o-3"}); err == nil {
		t.Error("Mandatory after AllowUnroutable did not turn it back on")
	}
}
