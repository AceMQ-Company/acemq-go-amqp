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

package rabbitmq_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

// These tests need a broker. Point ACEMQ_TEST_AMQP_URL at one:
//
//	docker run -d -p 5672:5672 rabbitmq:4-alpine
//	ACEMQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/ go test ./rabbitmq/
//
// They are skipped rather than failed when it is unset, so that `go test ./...`
// works on a machine with no broker. CI sets it, so the skip does not become a
// way for these to quietly stop running.
func brokerURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}
	return url
}

type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}

func connect(t *testing.T, opts ...acemq.ConnOption) *acemq.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mq, err := acemq.Connect(ctx, brokerURL(t), opts...)
	if err != nil {
		t.Fatalf("cannot reach the broker: %v", err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

// queueName keeps tests from colliding on a broker that outlives them.
func queueName(t *testing.T) string {
	t.Helper()
	name := "acemq-go-test-" + strings.ToLower(t.Name())
	return strings.NewReplacer("/", "-", " ", "-").Replace(name)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestAMessageGoesThroughARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	got := make(chan acemq.Message[OrderPlaced], 1)
	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := acemq.NewPublisher[OrderPlaced](mq, "", queue)
	err = pub.Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250},
		acemq.CorrelationID("corr-1"),
		acemq.Header("x-tenant", "acme"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.Payload.OrderID != "o-1" || m.Payload.TotalCents != 4250 {
			t.Errorf("payload = %+v", m.Payload)
		}
		// Every envelope field has to survive a real broker's header table,
		// which is where a port usually differs from the original.
		if m.Envelope.CorrelationID != "corr-1" {
			t.Errorf("CorrelationID = %q", m.Envelope.CorrelationID)
		}
		if m.Envelope.Type != queue {
			t.Errorf("Type = %q, want the routing key %q", m.Envelope.Type, queue)
		}
		if m.Envelope.Attempt != 1 {
			t.Errorf("Attempt = %d, want 1", m.Envelope.Attempt)
		}
		if m.Envelope.Headers["x-tenant"] != "acme" {
			t.Errorf("application headers = %v", m.Envelope.Headers)
		}
		if !strings.HasPrefix(m.Envelope.Origin, "acemq@") {
			t.Errorf("Origin = %q", m.Envelope.Origin)
		}
		if m.Envelope.FirstSeen.IsZero() {
			t.Error("FirstSeen did not survive the broker")
		}
		for k := range m.Envelope.Headers {
			if acemq.IsAceHeader(k) {
				t.Errorf("reserved header %q reached the application", k)
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the message never arrived")
	}
}

// TestTheAttemptCounterAdvancesAgainstARealBroker is the one worth having a
// broker for.
//
// The in-memory transport is written to behave this way, so on its own it
// proves only that it matches its own design. This proves the thing the design
// is about: RabbitMQ requeues the bytes it was given, the attempt header still
// reads 1, and the count has to come from the redelivery flag.
func TestTheAttemptCounterAdvancesAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t, acemq.WithRetry(acemq.FixedRetry(3, 0)))
	queue := queueName(t)

	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var attempts []int

	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			attempts = append(attempts, m.Envelope.Attempt)
			n := len(attempts)
			mu.Unlock()

			if n < 3 {
				return acemq.Retry(errors.New("not yet"))
			}
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "three deliveries", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) >= 3
	})

	mu.Lock()
	defer mu.Unlock()
	for i, want := range []int{1, 2, 3} {
		if attempts[i] != want {
			t.Errorf("delivery %d reported attempt %d, want %d (all: %v)",
				i+1, attempts[i], want, attempts)
		}
	}
}

func TestRetriesStopAndTheMessageLeavesTheQueue(t *testing.T) {
	ctx := context.Background()
	mq := connect(t, acemq.WithRetry(acemq.FixedRetry(2, 0)))
	queue := queueName(t)

	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	calls := 0

	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			return acemq.Retry(errors.New("still broken"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "both attempts", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("the handler ran %d times against a real broker, want exactly 2", calls)
	}
}

func TestATopicExchangeRoutesOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	exchange := queueName(t) + "-x"
	euQueue := queueName(t) + "-eu"

	if err := mq.DeclareExchange(ctx, exchange, "topic", acemq.TransientExchange()); err != nil {
		t.Fatal(err)
	}
	if err := mq.DeclareQueue(ctx, euQueue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}
	if err := mq.Bind(ctx, euQueue, exchange, "orders.*.eu"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var keys []string
	sub, err := acemq.Consume(ctx, mq, euQueue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			keys = append(keys, m.RoutingKey)
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	for _, key := range []string{"orders.created.eu", "orders.created.us"} {
		if err := acemq.NewPublisher[OrderPlaced](mq, exchange, key).
			Send(ctx, OrderPlaced{OrderID: key}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "the eu message", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(keys) >= 1
	})
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// The broker's own matching, not this library's. If the two disagreed, the
	// in-memory transport would be certifying behaviour that does not happen.
	if len(keys) != 1 || keys[0] != "orders.created.eu" {
		t.Errorf("the eu-bound queue received %v, want just orders.created.eu", keys)
	}
}

func TestDeclaringAQueueTwiceWithDifferentSettingsIsRefused(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	// A second connection, because the failed declaration kills the channel it
	// was made on.
	other := connect(t)
	err := other.DeclareQueue(ctx, queue, acemq.Transient(), acemq.AutoDelete())

	if err == nil {
		t.Fatal("redeclaring a queue with different settings was accepted; " +
			"the code and the broker would silently disagree about what the queue is")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("the error does not name the broker's refusal: %v", err)
	}
}

func TestClosingWaitsForAHandlerAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	var finished bool
	var mu sync.Mutex

	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			close(started)
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			finished = true
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}

	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	<-started
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !finished {
		t.Error("Close returned while the handler was still running")
	}
}

// ---- publisher confirms ----------------------------------------------

// TestTheBrokerActuallyConfirms is the difference between "the bytes left this
// process" and "the broker has taken responsibility".
//
// Only a real broker can demonstrate it: the in-memory transport confirms
// everything because everything has already happened by the time it answers.
func TestTheBrokerActuallyConfirms(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	result, err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		SendResult(ctx, OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	if !result.Confirmed {
		t.Error("the broker did not confirm the message, so nothing was promised about it")
	}
}

func TestAnUnroutableMandatoryMessageIsAnErrorAgainstARealBroker(t *testing.T) {
	// RabbitMQ returns the message and confirms it, in that order. Getting the
	// ordering wrong would make this pass by accident on a fast broker and fail
	// on a slow one, so it is worth having against the real thing.
	ctx := context.Background()
	mq := connect(t)
	exchange := queueName(t) + "-x"

	if err := mq.DeclareExchange(ctx, exchange, "topic", acemq.TransientExchange()); err != nil {
		t.Fatal(err)
	}

	err := acemq.NewPublisher[OrderPlaced](mq, exchange, "nothing.listens.here",
		acemq.Mandatory[OrderPlaced]()).
		Send(ctx, OrderPlaced{OrderID: "o-1"})

	if err == nil {
		t.Fatal("an unroutable message was published without complaint")
	}
	if !strings.Contains(err.Error(), "reached no queue") {
		t.Errorf("the error does not say what happened: %v", err)
	}
	if !strings.Contains(err.Error(), "NO_ROUTE") {
		t.Errorf("the broker's own reason is missing: %v", err)
	}
}

func TestARoutableMandatoryMessageIsNotMistakenForAnUnroutableOne(t *testing.T) {
	// The other half of the return correlation: a message that did arrive must
	// not pick up a return meant for something else.
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)
	exchange := queueName(t) + "-x"

	if err := mq.DeclareExchange(ctx, exchange, "topic", acemq.TransientExchange()); err != nil {
		t.Fatal(err)
	}
	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}
	if err := mq.Bind(ctx, queue, exchange, "orders.placed"); err != nil {
		t.Fatal(err)
	}

	pub := acemq.NewPublisher[OrderPlaced](mq, exchange, "orders.placed",
		acemq.Mandatory[OrderPlaced]())
	unroutable := acemq.NewPublisher[OrderPlaced](mq, exchange, "nobody.listens",
		acemq.Mandatory[OrderPlaced]())

	// Interleaved, on the same channel, so a return for one is in flight while
	// the other is being confirmed.
	for i := range 5 {
		if err := unroutable.Send(ctx, OrderPlaced{OrderID: "dropped"}); err == nil {
			t.Fatal("the unroutable publisher succeeded")
		}
		result, err := pub.SendResult(ctx, OrderPlaced{OrderID: "kept"})
		if err != nil {
			t.Fatalf("round %d: a routable message was reported as unroutable: %v", i, err)
		}
		if !result.Routed {
			t.Fatalf("round %d: Routed is false for a message that reached a bound queue", i)
		}
	}
}

func TestConfirmsCanBeTurnedOff(t *testing.T) {
	url := brokerURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{WithoutConfirms: true})
	if err != nil {
		t.Fatal(err)
	}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatal(err)
	}
	defer mq.Close()

	queue := queueName(t)
	if err := mq.DeclareQueue(ctx, queue, acemq.AutoDelete()); err != nil {
		t.Fatal(err)
	}

	result, err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		SendResult(ctx, OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	// Nothing was promised, so nothing is claimed. Reporting Confirmed here
	// would be a lie that looks like a guarantee.
	if result.Confirmed {
		t.Error("Confirmed is true with confirms turned off")
	}
}

// ---- topology --------------------------------------------------------

func TestATopologyAppliesToARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	prefix := queueName(t)

	topology := acemq.NewTopology().
		Exchange(prefix+"-x", "topic", acemq.TransientExchange()).
		Queue(prefix+"-q", acemq.AutoDelete()).
		Binding(prefix+"-q", prefix+"-x", "order.placed")

	if err := topology.Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	got := make(chan string, 1)
	sub, err := acemq.Consume(ctx, mq, prefix+"-q",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m.Payload.OrderID
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, prefix+"-x", "order.placed").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-got:
	case <-time.After(15 * time.Second):
		t.Fatal("the topology was applied but nothing routed through it")
	}
}

// TestCheckNoticesDriftOnARealBroker is the one that needs a broker.
//
// PRECONDITION_FAILED is the only way AMQP will report drift without the
// management API, and a failed declaration kills the channel it was made on —
// which is why the check uses one of its own. If it did not, this test would
// take the connection down with it and everything after would fail obscurely.
func TestCheckNoticesDriftOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	// Durable on the broker, transient in the topology.
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		other := connect(t)
		_ = other.DeclareQueue(context.Background(), queue)
	})

	topology := acemq.NewTopology().Queue(queue, acemq.Transient())

	reports, err := topology.Check(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}

	if len(reports) == 0 {
		t.Fatal("the broker's disagreement about durability was not reported")
	}
	if !strings.Contains(reports[0].Reason, "PRECONDITION_FAILED") {
		t.Errorf("the report does not carry the broker's own reason: %v", reports[0])
	}

	// The connection still works. Checking on the shared channel would have
	// killed it, and everything after this point would fail for reasons that
	// looked nothing like the cause.
	if err := mq.DeclareQueue(ctx, queueName(t)+"-after", acemq.AutoDelete()); err != nil {
		t.Errorf("the drift check took the connection down with it: %v", err)
	}
}

func TestCheckIsQuietWhenTheRealBrokerAgrees(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	topology := acemq.NewTopology().Queue(queue, acemq.AutoDelete())

	if err := topology.Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}
	reports, err := topology.Check(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}

	if len(reports) != 0 {
		t.Errorf("drift reported against a broker just given this topology: %v", reports)
	}
}
