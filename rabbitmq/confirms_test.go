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

// batchSize is large enough that the difference between waiting once and
// waiting N times is not noise on any machine, and small enough that the test
// is quick even when it is slow.
const batchSize = 200

// roundTripSamples is how many single sends are timed to establish what one
// broker round trip costs on the machine the test is running on. Hard-coding a
// duration would make this test a measurement of somebody's laptop.
const roundTripSamples = 20

// dialTransport opens a transport directly, so a test can set what only Config
// carries.
func dialTransport(t *testing.T, cfg rabbitmq.Config) *acemq.Conn {
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
	return mq
}

// oneRoundTrip is the median cost of publishing a single message and waiting
// for its confirm, which is what a publish used to cost every time.
func oneRoundTrip(t *testing.T, ctx context.Context, pub *acemq.Publisher[OrderPlaced]) time.Duration {
	t.Helper()

	samples := make([]time.Duration, 0, roundTripSamples)
	for i := 0; i < roundTripSamples; i++ {
		started := time.Now()
		if err := pub.Send(ctx, OrderPlaced{OrderID: fmt.Sprintf("warm-%d", i)}); err != nil {
			t.Fatalf("warm-up send %d: %v", i, err)
		}
		samples = append(samples, time.Since(started))
	}

	// The median rather than the mean: one scheduling hiccup in twenty would
	// otherwise raise the bar this test has to clear, which is the wrong way
	// for a timing test to be flaky.
	for i := 1; i < len(samples); i++ {
		for j := i; j > 0 && samples[j] < samples[j-1]; j-- {
			samples[j], samples[j-1] = samples[j-1], samples[j]
		}
	}
	return samples[len(samples)/2]
}

// TestABatchCostsFarLessThanOneRoundTripPerMessage is the proof that the
// confirm wait left the channel lock.
//
// Before this change Transport.Publish held the publishing mutex across
// DeferredConfirmation.WaitContext, so SendAll pipelined at the library layer
// and then queued underneath: every message paid a full broker round trip, one
// after another, and a batch of N cost N of them. The library's own batch tests
// could not see it, because the in-memory transport has no round trip to pay.
//
// The assertion is deliberately generous — a quarter of what the serialised
// version would cost — because the point is not to measure a speedup precisely
// but to fail loudly if the serialisation ever comes back. Running this against
// the previous implementation gives a ratio of about 1.0.
func TestABatchCostsFarLessThanOneRoundTripPerMessage(t *testing.T) {
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare: %v", err)
	}

	pub := acemq.NewPublisher[OrderPlaced](mq, "", queue)
	roundTrip := oneRoundTrip(t, ctx, pub)

	payloads := make([]OrderPlaced, batchSize)
	for i := range payloads {
		payloads[i] = OrderPlaced{OrderID: fmt.Sprintf("order-%d", i), TotalCents: int64(i)}
	}

	started := time.Now()
	results, err := pub.SendAll(ctx, payloads)
	batch := time.Since(started)
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

	serialised := time.Duration(batchSize) * roundTrip
	budget := serialised / 4
	t.Logf("one round trip %v, %d serialised would be %v, the batch took %v (%.1fx faster)",
		roundTrip, batchSize, serialised, batch, float64(serialised)/float64(batch))
	if batch > budget {
		t.Errorf(
			"a batch of %d took %v, which is more than a quarter of the %v it would cost "+
				"at one round trip per message; the confirm wait is serialising publishes again",
			batchSize, batch, serialised)
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
// here; the timing assertion above is what says the default does better.
func TestOneOutstandingPublishIsTheOldBehaviour(t *testing.T) {
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	mq := dialTransport(t, rabbitmq.Config{MaxOutstandingPublishes: 1})
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
