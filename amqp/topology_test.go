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
	"strings"
	"testing"
	"time"
)

func aTopology() *Topology {
	return NewTopology().
		Exchange("orders-events", "topic").
		Queue("shipping-orders", DeadLetterTo("shipping-dead")).
		Queue("shipping-dead").
		Binding("shipping-orders", "orders-events", "order.placed").
		Binding("shipping-orders", "orders-events", "order.cancelled")
}

func TestATopologyDeclaresEverythingInIt(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// The proof is that a message routes end to end, not that the calls
	// returned nil.
	got := make(chan string, 1)
	sub, err := Consume(ctx, mq, "shipping-orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			got <- m.Payload.OrderID
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	err = NewPublisher[OrderPlaced](mq, "orders-events", "order.placed").
		Send(ctx, OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-got:
		if id != "o-1" {
			t.Errorf("got %q", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the topology was applied but nothing routed through it")
	}
}

func TestApplyingTwiceIsFine(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}
	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatalf("applying an unchanged topology a second time failed: %v", err)
	}
}

// TestABindingToAnUndeclaredQueueIsCaughtBeforeTheBroker is the mistake worth
// catching in the description itself.
//
// The broker would accept it whenever the queue happens to exist already, and
// the service would then quietly depend on something nothing declares — until
// a fresh environment, where it fails at start-up for reasons nobody can see.
func TestABindingToAnUndeclaredQueueIsCaughtBeforeTheBroker(t *testing.T) {
	topology := NewTopology().
		Exchange("orders-events", "topic").
		Binding("a-queue-nobody-declared", "orders-events", "order.placed")

	err := topology.Validate()

	if err == nil {
		t.Fatal("a binding to an undeclared queue was accepted")
	}
	if !strings.Contains(err.Error(), "a-queue-nobody-declared") {
		t.Errorf("the error does not name the queue: %v", err)
	}
}

func TestABindingToAnUndeclaredExchangeIsCaught(t *testing.T) {
	topology := NewTopology().
		Queue("orders").
		Binding("orders", "an-exchange-nobody-declared", "#")

	if topology.Validate() == nil {
		t.Fatal("a binding to an undeclared exchange was accepted")
	}
}

func TestBindingToTheDefaultExchangeIsRefused(t *testing.T) {
	// It cannot be bound to, and a topology that says otherwise is a
	// misunderstanding worth naming rather than a call that fails obscurely.
	topology := NewTopology().Queue("orders").Binding("orders", "", "orders")

	err := topology.Validate()

	if err == nil {
		t.Fatal("binding to the default exchange was accepted")
	}
	if !strings.Contains(err.Error(), "default exchange") {
		t.Errorf("the error does not explain: %v", err)
	}
}

func TestDeclaringTheSameThingTwiceInOneTopologyIsRefused(t *testing.T) {
	if NewTopology().Queue("orders").Queue("orders").Validate() == nil {
		t.Error("a queue declared twice was accepted")
	}
	if NewTopology().Exchange("e", "topic").Exchange("e", "topic").Validate() == nil {
		t.Error("an exchange declared twice was accepted")
	}
}

func TestAnExchangeWithoutAKindIsRefused(t *testing.T) {
	if NewTopology().Exchange("events", "").Validate() == nil {
		t.Error("an exchange with no kind was accepted")
	}
}

func TestApplyRefusesAnInvalidTopologyWithoutTouchingTheBroker(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	topology := NewTopology().
		Exchange("orders-events", "topic").
		Binding("never-declared", "orders-events", "#")

	if err := topology.Apply(ctx, mq); err == nil {
		t.Fatal("an invalid topology was applied")
	}

	// And nothing was created on the way to finding out.
	if _, err := Consume(ctx, mq, "never-declared",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() }); err == nil {
		t.Error("the queue was created despite the topology being refused")
	}
}

func TestThePlanSaysWhatWouldHappen(t *testing.T) {
	actions, err := aTopology().Plan()
	if err != nil {
		t.Fatal(err)
	}

	if len(actions) != 5 {
		t.Fatalf("the plan has %d actions, want 5", len(actions))
	}
	// Exchanges before queues before bindings, which is the order a broker
	// needs and the order somebody reading the plan expects.
	if actions[0].Kind != "exchange" || actions[1].Kind != "queue" {
		t.Errorf("the plan is out of order: %v", actions)
	}

	rendered := aTopology().String()
	for _, want := range []string{"orders-events", "shipping-orders", "x-dead-letter-exchange", "order.placed"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the rendered plan does not mention %s:\n%s", want, rendered)
		}
	}
}

func TestAnInvalidTopologyRendersAsInvalidRatherThanPanicking(t *testing.T) {
	rendered := NewTopology().Queue("orders").Binding("missing", "e", "#").String()

	if !strings.Contains(rendered, "invalid") {
		t.Errorf("an invalid topology rendered as if it were fine:\n%s", rendered)
	}
}

// ---- drift -----------------------------------------------------------

func TestCheckIsQuietWhenTheBrokerAgrees(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	reports, err := aTopology().Check(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Errorf("drift reported against a broker that was just given this topology: %v", reports)
	}
}

// TestCheckNoticesAQueueThatDoesNotMatch is what the whole thing is for.
//
// A service and its broker disagreeing about a queue is the failure that shows
// up as messages going somewhere nobody is looking — a dead-letter exchange
// that was changed, a durability that was not.
func TestCheckNoticesAQueueThatDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "shipping-orders", DeadLetterTo("somewhere-else")); err != nil {
		t.Fatal(err)
	}

	reports, err := aTopology().Check(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}

	if len(reports) == 0 {
		t.Fatal("a queue with a different dead-letter exchange was reported as matching")
	}
	found := false
	for _, r := range reports {
		if r.Name == "shipping-orders" && strings.Contains(r.Reason, "x-dead-letter-exchange") {
			found = true
		}
	}
	if !found {
		t.Errorf("the drift does not name the setting that differs: %v", reports)
	}
}

func TestRedeclaringWithDifferentSettingsIsRefusedInMemoryToo(t *testing.T) {
	// The in-memory transport has to be no kinder than RabbitMQ here, or a test
	// suite would certify a deployment that fails against the real broker.
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	err := mq.DeclareQueue(ctx, "orders", Transient())

	if err == nil {
		t.Fatal("redeclaring with different settings was accepted")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("the refusal does not read like the broker's: %v", err)
	}
}

func TestRedeclaringWithTheSameSettingsIsFine(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orders", DeadLetterTo("dead")); err != nil {
		t.Fatal(err)
	}
	if err := mq.DeclareQueue(ctx, "orders", DeadLetterTo("dead")); err != nil {
		t.Fatalf("an identical redeclaration was refused: %v", err)
	}
}
