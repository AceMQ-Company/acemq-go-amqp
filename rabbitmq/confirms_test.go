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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

// These are the tests for pipelined confirms: the write is serialised on the
// channel, the wait for the confirm is not, and how many publishes may be
// unconfirmed at once is a number in the configuration rather than an accident
// of where the lock happened to be released.
//
// They need a broker, like every other test in this package. See brokerURL.

// batchSize is enough concurrent publishes that a pipelining transport cannot
// help overlapping some of them, on any machine.
const batchSize = 200

// dialTransport opens a transport directly, so a test can set what only Config
// carries and read what only the transport counts.
func dialTransport(t *testing.T, cfg rabbitmq.Config) (*acemq.Conn, *rabbitmq.Transport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport, err := rabbitmq.Dial(ctx, brokerURL(t), cfg)
	if err != nil {
		t.Fatalf("cannot reach the broker: %v", err)
	}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatalf("cannot build a connection: %v", err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq, transport
}

// sendBatch publishes batchSize messages with SendAll and returns how many of
// the writes went out while the write before was still unconfirmed.
func sendBatch(t *testing.T, cfg rabbitmq.Config) int64 {
	t.Helper()
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	mq, transport := dialTransport(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare: %v", err)
	}

	payloads := make([]OrderPlaced, batchSize)
	for i := range payloads {
		payloads[i] = OrderPlaced{OrderID: fmt.Sprintf("order-%d", i), TotalCents: int64(i)}
	}
	results, err := acemq.NewPublisher[OrderPlaced](mq, "", queue).SendAll(ctx, payloads)
	if err != nil {
		t.Fatalf("SendAll: %v", err)
	}
	if len(results) != batchSize {
		t.Fatalf("SendAll returned %d results, want %d", len(results), batchSize)
	}
	for i, r := range results {
		if !r.Confirmed {
			t.Fatalf("result %d was not confirmed", i)
		}
	}
	return rabbitmq.Overlapped(transport)
}

// TestABatchCostsFarLessThanOneRoundTripPerMessage is the proof that the
// confirm wait left the channel lock.
//
// Before that change Transport.Publish held the publishing mutex across
// DeferredConfirmation.WaitContext, so SendAll pipelined at the library layer
// and then queued underneath: every message paid a full broker round trip, one
// after another, and a batch of N cost N of them. The library's own batch tests
// could not see it, because the in-memory transport has no round trip to pay.
//
// It used to time the batch against one median round trip and fail on a busy CI
// runner, because the two do not slow down together under load. It now counts
// what the speedup comes from: writes that went out while the write before was
// still unconfirmed. A transport that waits for each confirm before the next
// write counts exactly none, however fast or slow the machine is, and the bound
// of one outstanding publish below is that transport.
func TestABatchCostsFarLessThanOneRoundTripPerMessage(t *testing.T) {
	overlapped := sendBatch(t, rabbitmq.Config{})
	t.Logf("%d of %d writes went out before the previous confirm", overlapped, batchSize)
	if overlapped == 0 {
		t.Errorf("none of %d writes went out while another was unconfirmed; "+
			"the confirm wait is serialising publishes again", batchSize)
	}

	if serial := sendBatch(t, rabbitmq.Config{MaxOutstandingPublishes: 1}); serial != 0 {
		t.Errorf("with one outstanding publish %d writes overlapped, want 0; "+
			"the count does not measure what this test relies on", serial)
	}
}

// TestABatchArrivesWholeAndInOrder is the other half of the same change: going
// faster is only worth anything if the messages still all arrive, once each.
//
// Order is asserted on the queue rather than on the results, because that is
// what a consumer sees. A single channel writing under one lock keeps the
// broker's arrival order the same as the order the writes were issued in; what
// the confirms then do cannot change it.
func TestABatchArrivesWholeAndInOrder(t *testing.T) {
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare: %v", err)
	}

	const count = 50
	pub := acemq.NewPublisher[OrderPlaced](mq, "", queue)

	// Sent one at a time so the writes have a defined order to check against.
	// SendAll issues its writes from a goroutine each, and a batch whose
	// members race to the lock has no order for a test to assert on.
	ids := make([]string, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("in-order-%d", i)
		ids[i] = id
		if err := pub.Send(ctx, OrderPlaced{OrderID: id}, acemq.MessageID(id)); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	seen := make([]string, 0, count)
	done := make(chan struct{})
	var once sync.Once

	consumer, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			seen = append(seen, m.Payload.OrderID)
			if len(seen) == count {
				once.Do(func() { close(done) })
			}
			return acemq.Accept()
		})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer func() { _ = consumer.Close() }()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("only %d of %d messages arrived", len(seen), count)
	}

	for i, id := range ids {
		if seen[i] != id {
			t.Fatalf("message %d is %q, want %q; the batch did not arrive in the order it was written",
				i, seen[i], id)
		}
	}
}

// TestEveryUnroutableMessageInABatchGetsItsOwnReturn is the ordering that had
// to survive pipelining.
//
// A basic.return arrives before the confirm for the same publish, which is what
// lets a publisher drain rather than wait for one. With a single publish
// outstanding that was all there was to it. With a hundred, a publisher
// draining the channel meets other publishers' returns, and the old code put
// those back into a buffered channel that drops what it cannot hold. Filing
// them by message id instead is what keeps each return with its own publish.
//
// Half the batch is routable and half is not, so a bug that loses returns and a
// bug that invents them both fail here.
func TestEveryUnroutableMessageInABatchGetsItsOwnReturn(t *testing.T) {
	queue := queueName(t)
	exchange := queue + "-x"
	removeAtEnd(t, []string{queue}, []string{exchange})

	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := mq.DeclareExchange(ctx, exchange, "direct", acemq.TransientExchange()); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := mq.Bind(ctx, queue, exchange, "bound"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	const pairs = 60

	type outcome struct {
		id         string
		unroutable bool
		reason     string
	}

	results := make([]outcome, pairs*2)
	var wg sync.WaitGroup

	// Published concurrently on purpose: this is the case the returns bookkeeping
	// exists for, and a sequential version would pass against the old code too.
	for i := 0; i < pairs*2; i++ {
		i := i
		routable := i%2 == 0
		key := "unbound"
		if routable {
			key = "bound"
		}
		id := fmt.Sprintf("mandatory-%d", i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := acemq.NewPublisher[OrderPlaced](mq, exchange, key, acemq.Mandatory[OrderPlaced]())
			result, err := pub.SendResult(ctx, OrderPlaced{OrderID: id}, acemq.MessageID(id))
			results[i] = outcome{id: result.MessageID, unroutable: !result.Routed, reason: result.ReturnReason}
			if routable && err != nil {
				t.Errorf("message %s was bound and should have gone: %v", id, err)
			}
			if !routable && err == nil {
				t.Errorf("message %s was unbound and should have been returned", id)
			}
		}()
	}
	wg.Wait()

	returned, delivered := 0, 0
	for i, r := range results {
		if r.id != fmt.Sprintf("mandatory-%d", i) {
			t.Fatalf("result %d is for %q; a result was filed under the wrong publish", i, r.id)
		}
		if i%2 == 0 {
			if r.unroutable {
				t.Errorf("message %s was bound but came back as unroutable", r.id)
			}
			delivered++
			continue
		}
		if !r.unroutable {
			t.Errorf("message %s was unbound but no return reached its publish", r.id)
			continue
		}
		if !strings.Contains(r.reason, "NO_ROUTE") {
			t.Errorf("message %s came back with reason %q, want the broker's NO_ROUTE", r.id, r.reason)
		}
		returned++
	}

	if returned != pairs {
		t.Errorf("%d of %d unroutable messages were returned", returned, pairs)
	}
	if delivered != pairs {
		t.Errorf("%d of %d routable messages went through", delivered, pairs)
	}
}

// TestOneOutstandingPublishIsTheOldBehaviour pins what the configuration means.
//
// MaxOutstandingPublishes of 1 permits exactly one unconfirmed publish, which
// is a round trip per message — what this transport did before the change, now
// available on request rather than by accident. Correctness is the assertion
// here; the overlap count above is what says the default does better.
func TestOneOutstandingPublishIsTheOldBehaviour(t *testing.T) {
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	mq, _ := dialTransport(t, rabbitmq.Config{MaxOutstandingPublishes: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare: %v", err)
	}

	const count = 20
	payloads := make([]OrderPlaced, count)
	for i := range payloads {
		payloads[i] = OrderPlaced{OrderID: fmt.Sprintf("bounded-%d", i)}
	}

	results, err := acemq.NewPublisher[OrderPlaced](mq, "", queue).SendAll(ctx, payloads)
	if err != nil {
		t.Fatalf("SendAll with one outstanding publish: %v", err)
	}
	for i, r := range results {
		if !r.Confirmed {
			t.Errorf("result %d was not confirmed", i)
		}
	}

	count64, err := mq.MessageCount(ctx, queue)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count64 != count {
		t.Errorf("the queue holds %d messages, want %d", count64, count)
	}
}
