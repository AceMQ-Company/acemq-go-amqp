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
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
	amqp091 "github.com/rabbitmq/amqp091-go"
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

// removeAtEnd deletes what a test left on the broker, so the test can be run
// twice. A queue that survives a run makes the next one assert against the
// leftovers of the last, which is a failure that looks like a bug in the
// library.
func removeAtEnd(t *testing.T, queues []string, exchanges []string) {
	t.Helper()
	t.Cleanup(func() {
		conn, err := amqp091.Dial(brokerURL(t))
		if err != nil {
			t.Logf("cannot clean up: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			t.Logf("cannot clean up: %v", err)
			return
		}
		defer func() { _ = ch.Close() }()
		// Every one of them, even after a failure: giving up at the first leaves
		// the rest behind, and a queue that survives a run makes the next one
		// assert against the leftovers of the last.
		for _, q := range queues {
			if _, err := ch.QueueDelete(q, false, false, false); err != nil {
				t.Logf("cannot delete queue %s: %v", q, err)
			}
		}
		for _, e := range exchanges {
			if err := ch.ExchangeDelete(e, false, false); err != nil {
				t.Logf("cannot delete exchange %s: %v", e, err)
			}
		}
	})
}

// declaredElsewhere declares a queue from a connection of its own and returns
// what the broker said.
//
// The only way to ask AMQP what a queue looks like is to declare it and see
// whether the answer is PRECONDITION_FAILED, and a refusal kills the channel it
// arrived on — so every question gets a connection to itself. It is also the
// honest shape of the question being asked: this is a second service starting
// up against a queue somebody else created.
func declaredElsewhere(t *testing.T, queue string, args amqp091.Table) error {
	t.Helper()
	conn, err := amqp091.Dial(brokerURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()

	_, err = ch.QueueDeclare(queue, true, false, false, false, args)
	return err
}

// mustBeQuorum fails unless the broker holds this queue as a quorum queue.
//
// Asked by declaring it as quorum and requiring acceptance, then as classic and
// requiring PRECONDITION_FAILED. The second half is what makes the first mean
// anything: a broker that ignored x-queue-type would accept both.
func mustBeQuorum(t *testing.T, queue string, args amqp091.Table) {
	t.Helper()

	quorum := amqp091.Table{"x-queue-type": "quorum"}
	classic := amqp091.Table{}
	for k, v := range args {
		quorum[k] = v
		classic[k] = v
	}

	if err := declaredElsewhere(t, queue, quorum); err != nil {
		t.Fatalf("%s is not a quorum queue: declaring it as one was refused with %v", queue, err)
	}
	err := declaredElsewhere(t, queue, classic)
	if err == nil || !strings.Contains(err.Error(), "x-queue-type") {
		t.Fatalf("declaring %s as classic was answered %v, want PRECONDITION_FAILED naming "+
			"x-queue-type; without that refusal the acceptance above proves nothing", queue, err)
	}
	t.Logf("%s is quorum on the broker, and a classic declaration of it is refused: %v", queue, err)
}

// mustBeClassic fails unless the broker holds this queue as a classic queue.
//
// Classic on the wire is the absence of x-queue-type, which is what Java's
// QueueType.CLASSIC sends, so that is what is offered here.
func mustBeClassic(t *testing.T, queue string, args amqp091.Table) {
	t.Helper()

	classic := amqp091.Table{}
	quorum := amqp091.Table{"x-queue-type": "quorum"}
	for k, v := range args {
		classic[k] = v
		quorum[k] = v
	}

	if err := declaredElsewhere(t, queue, classic); err != nil {
		t.Fatalf("%s is not a classic queue: declaring it as one was refused with %v", queue, err)
	}
	err := declaredElsewhere(t, queue, quorum)
	if err == nil || !strings.Contains(err.Error(), "x-queue-type") {
		t.Fatalf("declaring %s as quorum was answered %v, want PRECONDITION_FAILED naming "+
			"x-queue-type", queue, err)
	}
	t.Logf("%s is classic on the broker, and a quorum declaration of it is refused: %v", queue, err)
}

// printTopology puts the whole plan in the test output, so that the queues,
// their types, their arguments, both exchanges and every binding can be read
// beside the same topology in the other four languages.
func printTopology(t *testing.T, what string, topology *acemq.Topology) {
	t.Helper()
	plan, err := topology.Plan()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the topology this library declares for %s:", what)
	for _, action := range plan {
		t.Logf("  %s", action)
	}
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
// is about: the counter rides on the message through a real broker's header
// table, because each retry is a fresh publish carrying the advanced header —
// where a requeue would have handed back the bytes the publisher wrote, leaving
// the header on 1 for ever.
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
	dlq := acemq.DeadLetterQueue(queue)
	removeAtEnd(t, []string{queue, dlq, acemq.ParkedQueue(queue)}, nil)

	err := acemq.NewTopology().Queue(queue).DeadLetters(queue).Apply(ctx, mq)
	if err != nil {
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
	if calls != 2 {
		t.Errorf("the handler ran %d times against a real broker, want exactly 2", calls)
	}
	mu.Unlock()

	// Republished into the dead-letter queue with the reason attached, and the
	// original acknowledged, rather than rejected and left to whatever the
	// broker's own dead-lettering happens to point at.
	waitFor(t, "the message to reach "+dlq, func() bool {
		n, err := mq.MessageCount(ctx, dlq)
		return err == nil && n == 1
	})

	dead, found, err := mq.Pull(ctx, dlq)
	if err != nil || !found {
		t.Fatalf("Pull: %v, found=%v", err, found)
	}
	defer func() { _ = dead.Ack() }()

	if !strings.Contains(dead.Envelope.Error, "exhausted 2 attempts") {
		t.Errorf("Error = %q, want it to say why", dead.Envelope.Error)
	}
	if !strings.Contains(dead.Envelope.Error, "still broken") {
		t.Errorf("Error = %q, want the handler's own reason kept", dead.Envelope.Error)
	}
	if n, err := mq.MessageCount(ctx, queue); err != nil || n != 0 {
		t.Errorf("the source queue holds %d messages (%v), want none", n, err)
	}
}

// TestALongRetryWaitsInTheBrokerAndComesBack is the test the whole rung
// mechanism exists for, and the one a fake cannot stand in for.
//
// It proves three things in order, against a real broker. The message is on the
// rung queue and not on the source queue. Nothing is holding it: the consumer is
// closed before the count is taken, so anything it still held unacknowledged
// would have been returned to the source queue and counted there. And when the
// rung's time-to-live expires the broker sends it home by itself, one attempt
// further on, with no consumer involved in the waiting at any point.
//
// The delay is deliberately short and the threshold moved down to match, so that
// the test takes seconds. Nothing else about the arrangement changes: the same
// code path runs for a five-minute rung.
func TestALongRetryWaitsInTheBrokerAndComesBack(t *testing.T) {
	ctx := context.Background()
	policy := acemq.FixedRetry(3, 3*time.Second).WaitInBrokerFrom(time.Second)
	mq := connect(t, acemq.WithRetry(policy))

	queue := queueName(t)
	ladder := acemq.LadderFor(queue, policy)
	rung := ladder.Queues()[0]
	removeAtEnd(t,
		[]string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue), rung},
		[]string{acemq.RetryExchange})

	// Printed so the declaration can be put beside the Python and Ruby
	// libraries' by eye.
	t.Logf("%s", ladder)
	for _, r := range ladder.Rungs {
		t.Logf("declare queue %s durable, %s=%v, %s=%q, %s=%q",
			r.Queue,
			acemq.ArgMessageTTL, r.Args[acemq.ArgMessageTTL],
			acemq.ArgDeadLetterExchange, r.Args[acemq.ArgDeadLetterExchange],
			acemq.ArgDeadLetterRoutingKey, r.Args[acemq.ArgDeadLetterRoutingKey])
	}

	topology := acemq.NewTopology().Queue(queue).DeadLetters(queue).Retries(queue, policy)
	printTopology(t, queue, topology)
	if err := topology.Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// The types, before any of the mechanism runs. The source queue is quorum
	// and every queue underneath it is classic, and a quorum queue is not a
	// classic one with more replicas: it dead-letters through its own
	// implementation, and whether an expired rung message comes home to one is
	// a question only the broker can answer. The rest of this test is that
	// answer.
	mustBeQuorum(t, queue, amqp091.Table{
		acemq.ArgDeadLetterExchange:   acemq.DeadLetterExchange,
		acemq.ArgDeadLetterRoutingKey: acemq.DeadLetterQueue(queue),
	})
	mustBeClassic(t, rung, amqp091.Table(ladder.Rungs[0].Args))
	mustBeClassic(t, acemq.DeadLetterQueue(queue), nil)
	mustBeClassic(t, acemq.ParkedQueue(queue), nil)

	failed := make(chan int, 4)
	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			failed <- m.Envelope.Attempt
			return acemq.Retry(errors.New("the payment gateway is down"))
		})
	if err != nil {
		t.Fatal(err)
	}

	published := time.Now()
	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250}, acemq.MessageID("m-1")); err != nil {
		t.Fatal(err)
	}

	select {
	case attempt := <-failed:
		if attempt != 1 {
			t.Fatalf("the first delivery reported attempt %d", attempt)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the message never reached the handler")
	}

	// Closed before anything is counted. A consumer that was holding the message
	// unacknowledged gives it back here, and it would be counted on the source
	// queue below rather than on the rung.
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to reach "+rung, func() bool {
		n, err := mq.MessageCount(ctx, rung)
		return err == nil && n == 1
	})
	onRung := time.Now()

	source, err := mq.MessageCount(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	if source != 0 {
		t.Errorf("%s holds %d messages while the wait is on; the wait is not the broker's", queue, source)
	}
	t.Logf("after %s: %s holds 1, %s holds %d, and no consumer is attached",
		onRung.Sub(published).Round(time.Millisecond), rung, queue, source)

	// Nothing consumes a rung. The time-to-live is the only thing that ever
	// takes a message out of one, and this is it happening.
	waitFor(t, "the broker to send it home when the time-to-live expires", func() bool {
		n, err := mq.MessageCount(ctx, queue)
		return err == nil && n == 1
	})
	back := time.Now()

	if n, err := mq.MessageCount(ctx, rung); err != nil || n != 0 {
		t.Errorf("%s still holds %d messages (%v)", rung, n, err)
	}
	t.Logf("after a further %s the broker returned it to %s by itself",
		back.Sub(onRung).Round(time.Millisecond), queue)

	if waited := back.Sub(onRung); waited < 2*time.Second {
		t.Errorf("the message came home after %s, which is less than the rung's three-second "+
			"time-to-live; it cannot have waited there", waited)
	}

	returned, found, err := mq.Pull(ctx, queue)
	if err != nil || !found {
		t.Fatalf("Pull: %v, found=%v", err, found)
	}
	defer func() { _ = returned.Ack() }()

	if returned.Envelope.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2: a message that has been round the broker is no less "+
			"on its second attempt than one that waited in the consumer",
			returned.Envelope.Attempt)
	}
	if returned.Envelope.ID != "m-1" {
		t.Errorf("ID = %q, want the message's own", returned.Envelope.ID)
	}
	if !strings.Contains(string(returned.Body), "o-1") {
		t.Errorf("body = %q, want the bytes that were published", returned.Body)
	}
}

// TestARungIsDeclaredIdenticallyEverywhere puts the argument table in front of a
// real broker, which is the only thing that can answer for it.
//
// A rung declared with different arguments by another service is refused with
// PRECONDITION_FAILED, and that service cannot consume at all. This declares the
// rung as this library does, then declares it again with a different
// time-to-live and requires the broker to refuse — which is what would happen to
// the second of two services if this table ever drifted.
func TestARungIsDeclaredIdenticallyEverywhere(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)

	queue := queueName(t)
	policy := acemq.FixedRetry(2, time.Minute)
	ladder := acemq.LadderFor(queue, policy)
	rung := ladder.Queues()[0]
	removeAtEnd(t, []string{queue, rung}, []string{acemq.RetryExchange})

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}
	if err := ladder.Declare(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// The same declaration again is how AMQP is meant to be used, and has to be
	// accepted or a second service on the same queue could never start.
	if err := ladder.Declare(ctx, mq); err != nil {
		t.Errorf("the same rung declared twice was refused: %v", err)
	}

	// A different one is refused, on its own channel so the refusal does not take
	// the connection with it.
	conn, err := amqp091.Dial(brokerURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()

	_, err = ch.QueueDeclare(rung, true, false, false, false, amqp091.Table{
		"x-message-ttl":             int64(1),
		"x-dead-letter-exchange":    acemq.RetryExchange,
		"x-dead-letter-routing-key": queue,
	})
	if err == nil {
		t.Fatal("the broker accepted a rung with a different time-to-live, which means the " +
			"argument table is not what makes two services agree")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("the broker refused with %v, want PRECONDITION_FAILED", err)
	}
}

func TestATopicExchangeRoutesOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	exchange := queueName(t) + "-x"
	euQueue := queueName(t) + "-eu"
	removeAtEnd(t, []string{euQueue}, []string{exchange})

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
	removeAtEnd(t, []string{queue}, nil)

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
	removeAtEnd(t, []string{queue}, nil)

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
	removeAtEnd(t, nil, []string{exchange})

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
	removeAtEnd(t, []string{queue}, []string{exchange})

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
	removeAtEnd(t, []string{queue}, nil)
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
	removeAtEnd(t, []string{prefix + "-q"}, []string{prefix + "-x"})

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

	// Registered first so that it runs last: clean-ups run in reverse, and the
	// one below puts the queue back before this takes it away for good.
	removeAtEnd(t, []string{queue, queue + "-after"}, nil)

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
	removeAtEnd(t, []string{queue}, nil)

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

// ---- connection recovery ---------------------------------------------

// TestTheConsumerComesBackAfterTheBrokerRestarts is the test that needed
// writing most.
//
// Before recovery existed, a dropped connection was the quietest failure in the
// library: the delivery channel closed, the consumer goroutine ended, and the
// Consumer object still looked alive. The service consumed nothing, for ever,
// and said nothing about it.
//
// It needs a broker it is allowed to restart, which is not the shared one, so
// it runs only when ACEMQ_TEST_RESTARTABLE_CONTAINER names one.
func TestTheConsumerComesBackAfterTheBrokerRestarts(t *testing.T) {
	container := os.Getenv("ACEMQ_TEST_RESTARTABLE_CONTAINER")
	url := os.Getenv("ACEMQ_TEST_RESTARTABLE_URL")
	if container == "" || url == "" {
		t.Skip("ACEMQ_TEST_RESTARTABLE_CONTAINER and _URL are not set; skipping the recovery test")
	}

	ctx := context.Background()

	var eventsMu sync.Mutex
	var events []string
	transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{
		RecoveryDelay: 500 * time.Millisecond,
		OnRecovery: func(e rabbitmq.RecoveryEvent) {
			eventsMu.Lock()
			events = append(events, e.Kind)
			eventsMu.Unlock()
		},
	})
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

	var mu sync.Mutex
	received := 0
	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			received++
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := acemq.NewPublisher[OrderPlaced](mq, "", queue)
	if err := pub.Send(ctx, OrderPlaced{OrderID: "before"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first message", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return received == 1
	})

	// Take the broker away and bring it back.
	if out, err := exec.Command("docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("cannot restart the broker: %v: %s", err, out)
	}

	// Publishing has to work again, which means the connection, the channel,
	// the queue and the consumer all came back.
	deadline := time.Now().Add(90 * time.Second)
	var sendErr error
	for time.Now().Before(deadline) {
		if sendErr = pub.Send(ctx, OrderPlaced{OrderID: "after"}); sendErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if sendErr != nil {
		t.Fatalf("publishing never recovered: %v", sendErr)
	}

	waitFor(t, "a message consumed after the restart", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return received >= 2
	})

	// And the application was told, rather than having to notice.
	eventsMu.Lock()
	defer eventsMu.Unlock()
	if !slices.Contains(events, "lost") {
		t.Errorf("nothing reported the connection being lost: %v", events)
	}
	if !slices.Contains(events, "recovered") {
		t.Errorf("nothing reported the recovery: %v", events)
	}
}

// TestADryRunAgainstARealBrokerCreatesNothing is the claim that has to hold on
// a broker rather than in a test double.
//
// A passive declare is the only read-only question AMQP offers, and it closes
// the channel it is asked on when the queue is missing. Doing that wrong would
// either create the queues the plan describes or take the connection down
// while describing them, and both would show up here.
func TestADryRunAgainstARealBrokerCreatesNothing(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	removeAtEnd(t, []string{queue}, []string{queue + "-events"})

	topology := acemq.NewTopology().
		Exchange(queue+"-events", "topic").
		Queue(queue).
		Binding(queue, queue+"-events", "#")

	plan, err := topology.ApplyWith(ctx, mq, acemq.DryRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range plan {
		if a.Kind == "queue" && !strings.Contains(a.Detail, "would create") {
			t.Errorf("queue %s reads %q, want it to be missing", a.Name, a.Detail)
		}
	}

	// The queue is still not there: consuming from it is refused.
	if _, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack { return acemq.Accept() }); err == nil {
		t.Error("the dry run created the queue")
	}

	// And the connection survived being asked, which is the other half of it.
	if err := topology.Apply(ctx, mq); err != nil {
		t.Fatalf("the connection did not survive the dry run: %v", err)
	}

	plan, err = topology.ApplyWith(ctx, mq, acemq.DryRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range plan {
		if a.Kind == "queue" && !strings.Contains(a.Detail, "matches") {
			t.Errorf("after applying, queue %s reads %q", a.Name, a.Detail)
		}
	}
}

func TestADryRunReportsARealBrokersDrift(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)

	removeAtEnd(t, []string{queue}, nil)

	// Durable on the broker, transient in the topology.
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}

	plan, err := acemq.NewTopology().Queue(queue, acemq.Transient()).ApplyWith(ctx, mq, acemq.DryRun)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, a := range plan {
		if a.Name == queue && strings.Contains(a.Detail, "differs") {
			found = true
		}
	}
	if !found {
		t.Errorf("the difference was not reported: %v", plan)
	}
}

func TestCountingAndDeletingAQueueOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}
	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	// The broker counts what it has, which takes a moment to settle after a
	// confirm.
	var count int64
	waitFor(t, "the message to be counted", func() bool {
		got, err := mq.MessageCount(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}
		count = got
		return count == 1
	})

	if err := mq.DeleteQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}

	// And the connection still works afterwards: both of these ask on a channel
	// of their own, because the broker closes the channel it answers NOT_FOUND
	// on.
	exists, err := mq.QueueExists(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("the queue is still there after being deleted")
	}
	if err := mq.DeleteQueue(ctx, queue); err != nil {
		t.Errorf("deleting it a second time failed: %v", err)
	}
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("the connection did not survive: %v", err)
	}
}

// TestReplayTakesEveryMatchOnARealBroker is the difference between a replay
// somebody can trust and one that quietly stops early.
//
// A message the filter declines is put back, and RabbitMQ puts it back where it
// was — at the head of the queue. A replay that read the queue by consuming it
// would then be handed that same message again immediately, decide it had come
// full circle, and stop with everything behind it unexamined.
func TestReplayTakesEveryMatchOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)
	dead := queue + "-dead"
	removeAtEnd(t, []string{queue, dead}, nil)

	for _, q := range []string{queue, dead} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	publisher := acemq.NewPublisher[OrderPlaced](mq, "", dead)
	for _, id := range []string{"keep-1", "drop-1", "keep-2", "drop-2", "keep-3"} {
		if err := publisher.Send(ctx, OrderPlaced{OrderID: id}); err != nil {
			t.Fatal(err)
		}
	}

	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue:      dead,
		RoutingKey: queue,
		Limit:      10,
		Filter: func(_ acemq.Envelope, body []byte) bool {
			return strings.Contains(string(body), "keep")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Moved != 3 {
		t.Errorf("moved %d of the 3 that matched (%s)", result.Moved, result)
	}
	if result.Skipped != 2 {
		t.Errorf("skipped %d, want the 2 that did not match (%s)", result.Skipped, result)
	}

	// The ones it declined are still there, not lost.
	waitFor(t, "the declined messages to be back on the queue", func() bool {
		count, err := mq.MessageCount(ctx, dead)
		if err != nil {
			t.Fatal(err)
		}
		return count == 2
	})

	// And the ones it moved arrived.
	count, err := mq.MessageCount(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("%d messages were replayed onto %s, want 3", count, queue)
	}
}

// TestTheSourceQueueIsDeclaredIdenticallyInEveryLanguage puts the source queue's
// own argument table in front of a real broker.
//
// This is the declaration two services actually collide on. An orders queue is
// consumed by a Go service and a Python one, both of them declare orders, and
// AMQP resolves a disagreement about arguments by refusing the second declarer
// with PRECONDITION_FAILED — so the service that started second cannot consume
// at all. The test declares the topology from this library, then declares the
// same queue again from a second connection using the table the Python, Ruby,
// .NET and Java libraries write, and requires the broker to accept it. The
// second half is the same declaration with one argument changed, which has to be
// refused: without that, an accepted declaration would prove only that the
// broker was not looking.
func TestTheSourceQueueIsDeclaredIdenticallyInEveryLanguage(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)

	queue := queueName(t)
	dlq := acemq.DeadLetterQueue(queue)
	parked := acemq.ParkedQueue(queue)
	// The two AceMQ exchanges are shared by every queue on the broker and are
	// deliberately left behind: deleting acemq.dlx at the end of one test would
	// unbind the dead letters of every other service using the same broker.
	removeAtEnd(t, []string{queue, dlq, parked}, nil)

	topology := acemq.NewTopology().Queue(queue).DeadLetters(queue)

	// Printed whole so it can be put beside the other four libraries' by eye:
	// every queue with its type, both exchanges, every binding, and the
	// arguments on the source queue.
	printTopology(t, queue, topology)

	if err := topology.Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// The two queues underneath it are classic, as they are in Java, and this is
	// the broker being asked rather than the plan being reread.
	mustBeClassic(t, dlq, nil)
	mustBeClassic(t, parked, nil)

	// What every other AceMQ library writes for this queue, spelled out here
	// rather than taken from the constants, so that a change to the constants
	// cannot quietly change what this claims to be compatible with.
	//
	// x-queue-type is in the table because Java has always put it there:
	// Topology.Builder.queue declares a durable quorum queue, and a Java service
	// with an orders queue already has a quorum one. The other three libraries
	// followed Java rather than the other way round, because a queue that exists
	// as quorum cannot be redeclared as anything else — there was no version of
	// this agreement where the four of them chose classic.
	otherLibraries := amqp091.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "acemq.dlx",
		"x-dead-letter-routing-key": queue + ".dlq",
	}

	conn, err := amqp091.Dial(brokerURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// A channel of its own for each declaration: a refused declaration kills the
	// channel it was made on, so sharing one would make the second failure a
	// consequence of the first.
	agreeing, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = agreeing.Close() }()

	if _, err := agreeing.QueueDeclare(queue, true, false, false, false, otherLibraries); err != nil {
		t.Fatalf("a second service declaring %s the way the other four libraries do was "+
			"refused: %v", queue, err)
	}
	t.Logf("a second connection declared %s with %v and the broker accepted it",
		queue, otherLibraries)

	// And one argument different is refused, which is what would happen to the
	// second of two services if this table ever drifted again.
	disagreeing, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = disagreeing.Close() }()

	_, err = disagreeing.QueueDeclare(queue, true, false, false, false, amqp091.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "acemq.dlx",
		"x-dead-letter-routing-key": queue + ".dead",
	})
	if err == nil {
		t.Fatal("the broker accepted a different dead-letter routing key for the same queue, " +
			"which means these arguments are not what makes two services agree")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("the broker refused with %v, want PRECONDITION_FAILED", err)
	}
	t.Logf("the same queue with x-dead-letter-routing-key=%s.dead was refused: %v", queue, err)

	// The type is the argument this library changed, so it gets its own refusal.
	// Declaring the same queue classic — which is what this library did before
	// the five agreed, and what a service still on an older release would send —
	// has to be rejected, or the acceptance above would prove only that the
	// broker ignores x-queue-type.
	asClassic, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asClassic.Close() }()

	_, err = asClassic.QueueDeclare(queue, true, false, false, false, amqp091.Table{
		"x-dead-letter-exchange":    "acemq.dlx",
		"x-dead-letter-routing-key": queue + ".dlq",
	})
	if err == nil {
		t.Fatal("the broker accepted a classic declaration of a quorum queue, which would mean " +
			"x-queue-type is not part of what two services have to agree about")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") ||
		!strings.Contains(err.Error(), "x-queue-type") {
		t.Errorf("the broker refused with %v, want PRECONDITION_FAILED naming x-queue-type", err)
	}
	t.Logf("the same queue declared classic was refused: %v", err)
}

// TestSomethingElseRejectingAMessageStillReachesTheDeadLetterQueue is the
// backstop working.
//
// This library never takes this path: a consumer that gives up republishes to
// {queue}.dlq with the reason in x-acemq-error and acknowledges the original,
// which is the only way the reason survives. The broker-side route exists for
// what the library never sees — a message expiring against the source queue's
// own time-to-live, one dropped by x-max-length, or as here a rejection from a
// consumer that is not this library at all. Without the two arguments on the
// source queue those messages are discarded and nothing anywhere records that
// they existed.
//
// It also proves the routing key is doing its job. The message arrives on the
// source queue under the queue's own name, so a dead-letter route that did not
// override the key would deliver it to acemq.dlx as that name, match no binding
// and drop it — which looks exactly like dead-lettering that was never
// configured.
func TestSomethingElseRejectingAMessageStillReachesTheDeadLetterQueue(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)

	queue := queueName(t)
	dlq := acemq.DeadLetterQueue(queue)
	removeAtEnd(t, []string{queue, dlq, acemq.ParkedQueue(queue)}, nil)

	if err := acemq.NewTopology().Queue(queue).DeadLetters(queue).Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1"}, acemq.MessageID("m-1")); err != nil {
		t.Fatal(err)
	}

	conn, err := amqp091.Dial(brokerURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()

	var delivery amqp091.Delivery
	waitFor(t, "the message to reach "+queue, func() bool {
		d, ok, err := ch.Get(queue, false)
		if err != nil || !ok {
			return false
		}
		delivery = d
		return true
	})

	// A plain AMQP consumer, rejecting without requeue. Nothing about this knows
	// AceMQ exists.
	if err := delivery.Nack(false, false); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the broker to dead-letter it into "+dlq, func() bool {
		n, err := mq.MessageCount(ctx, dlq)
		return err == nil && n == 1
	})

	dead, found, err := mq.Pull(ctx, dlq)
	if err != nil || !found {
		t.Fatalf("Pull: %v, found=%v", err, found)
	}
	defer func() { _ = dead.Ack() }()

	if dead.Envelope.ID != "m-1" {
		t.Errorf("the message in %s is %q, not the one that was rejected",
			dlq, dead.Envelope.ID)
	}
	t.Logf("a rejection from something that is not this library reached %s by itself", dlq)
}
