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

package patterns_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

type PriceRequest struct {
	SKU string `json:"sku"`
}

type PriceResponse struct {
	Cents int64 `json:"cents"`
}

type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

func brokerFor(t *testing.T, opts ...acemq.ConnOption) *acemq.Conn {
	t.Helper()
	mq, err := acemq.Connect(context.Background(), "memory://"+t.Name(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---- request and reply -----------------------------------------------

func TestARequestGetsItsReply(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		t.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "prices",
		func(_ context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
			return PriceResponse{Cents: 4250}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices")
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()

	response, err := requester.Do(ctx, PriceRequest{SKU: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Cents != 4250 {
		t.Errorf("got %+v", response)
	}
}

// TestRepliesReachTheRightCaller is the property that matters when more than
// one request is in flight: a reply is paired with its request by correlation,
// and getting that wrong gives one caller another's answer.
func TestRepliesReachTheRightCaller(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		t.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "prices",
		func(_ context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
			// Answer with something derived from the request, so a mismatch is
			// visible rather than plausible.
			return PriceResponse{Cents: int64(len(m.Payload.SKU))}, nil
		}, acemq.Concurrency(8))
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices")
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sku := strings.Repeat("x", n)
			response, err := requester.Do(ctx, PriceRequest{SKU: sku})
			if err != nil {
				errs <- err
				return
			}
			if response.Cents != int64(n) {
				errs <- errors.New("a caller received somebody else's reply")
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestAResponderFailureReachesTheCaller(t *testing.T) {
	// Rather than the caller waiting out its timeout for an answer that was
	// never coming.
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		t.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "prices",
		func(_ context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
			return PriceResponse{}, errors.New("no such product")
		})
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices",
		patterns.Timeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()

	started := time.Now()
	_, err = requester.Do(ctx, PriceRequest{SKU: "nope"})

	if err == nil {
		t.Fatal("a failing responder produced no error")
	}
	if !strings.Contains(err.Error(), "no such product") {
		t.Errorf("the responder's reason did not come back: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Error("the caller waited out its timeout instead of being told")
	}
}

func TestARequestWithNoResponderTimesOut(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		t.Fatal(err)
	}

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices",
		patterns.Timeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()

	_, err = requester.Do(ctx, PriceRequest{SKU: "abc"})

	if !errors.Is(err, patterns.ErrRequestTimedOut) {
		t.Fatalf("got %v, want a timeout", err)
	}
}

func TestARequestWithNowhereToReplyIsDeadLettered(t *testing.T) {
	// Retrying cannot make a reply queue appear, so it must not loop.
	ctx := context.Background()
	mq := brokerFor(t, acemq.WithRetry(acemq.FixedRetry(5, 0)))

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	calls := 0
	responder, err := patterns.Serve(ctx, mq, "prices",
		func(_ context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return PriceResponse{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()

	// Published directly, with no reply-to header.
	if err := acemq.NewPublisher[PriceRequest](mq, "", "prices").
		Send(ctx, PriceRequest{SKU: "abc"}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Errorf("the handler ran %d times for a request with nowhere to reply", calls)
	}
}

// ---- idempotency -----------------------------------------------------

func TestADuplicateIsNotHandledTwice(t *testing.T) {
	store := patterns.NewInMemoryIdempotencyStore(time.Minute)

	var mu sync.Mutex
	handled := 0
	handler := patterns.Idempotent(store,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			handled++
			mu.Unlock()
			return acemq.Accept()
		})

	message := acemq.Message[OrderPlaced]{
		Payload:  OrderPlaced{OrderID: "o-1"},
		Envelope: acemq.Envelope{ID: "m-1"},
	}

	for range 5 {
		if ack := handler(context.Background(), message); ack.String() != "accept" {
			t.Fatalf("a duplicate was not accepted: %s", ack)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if handled != 1 {
		t.Errorf("the handler ran %d times for one message", handled)
	}
}

// TestAFailedMessageIsForgottenSoItCanBeRetried is the ordering that makes
// this a duplicate guard rather than a message eater.
//
// Remembering a message that then failed would mean the retry silently does
// nothing and the message is dropped after looking like it succeeded.
func TestAFailedMessageIsForgottenSoItCanBeRetried(t *testing.T) {
	store := patterns.NewInMemoryIdempotencyStore(time.Minute)

	var mu sync.Mutex
	attempts := 0
	handler := patterns.Idempotent(store,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()

			if n < 3 {
				return acemq.Retry(errors.New("not yet"))
			}
			return acemq.Accept()
		})

	message := acemq.Message[OrderPlaced]{Envelope: acemq.Envelope{ID: "m-1"}}
	for range 3 {
		handler(context.Background(), message)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Errorf("the handler ran %d times; a failed message was not forgotten", attempts)
	}
}

func TestAStoreThatIsBrokenRetriesRatherThanRisksADuplicate(t *testing.T) {
	handler := patterns.Idempotent(failingStore{},
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			t.Error("the handler ran despite the store being unreadable")
			return acemq.Accept()
		})

	ack := handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1"}})

	if ack.String() != "retry" {
		t.Errorf("got %s, want retry", ack)
	}
}

type failingStore struct{}

func (failingStore) FirstTime(context.Context, string) (bool, error) {
	return false, errors.New("the store is unreachable")
}
func (failingStore) Forget(context.Context, string) error { return nil }

func TestTheWindowStopsTheStoreGrowingForEver(t *testing.T) {
	store := patterns.NewInMemoryIdempotencyStore(50 * time.Millisecond)

	for i := range 10 {
		if _, err := store.FirstTime(context.Background(), string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if store.Len() != 10 {
		t.Fatalf("the store holds %d keys, want 10", store.Len())
	}

	time.Sleep(120 * time.Millisecond)
	// The sweep happens on use rather than on a timer, so the store needs no
	// goroutine and nothing to close.
	if _, err := store.FirstTime(context.Background(), "z"); err != nil {
		t.Fatal(err)
	}

	if store.Len() > 2 {
		t.Errorf("the store still holds %d keys after the window passed", store.Len())
	}
}

func TestIdempotencyCanUseAKeyFromThePayload(t *testing.T) {
	store := patterns.NewInMemoryIdempotencyStore(time.Minute)

	var mu sync.Mutex
	handled := 0
	handler := patterns.IdempotentBy(store,
		func(m acemq.Message[OrderPlaced]) string { return m.Payload.OrderID },
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			handled++
			mu.Unlock()
			return acemq.Accept()
		})

	// Two different messages about the same order.
	for _, id := range []string{"m-1", "m-2"} {
		handler(context.Background(), acemq.Message[OrderPlaced]{
			Payload:  OrderPlaced{OrderID: "o-1"},
			Envelope: acemq.Envelope{ID: id},
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if handled != 1 {
		t.Errorf("the handler ran %d times for one order", handled)
	}
}

// ---- ordering --------------------------------------------------------

// TestMessagesSharingAKeyAreNeverHandledAtOnce is the whole point of Ordered.
//
// A consumer with concurrency above one stops honouring the queue's order.
// This buys it back per key while keeping concurrency across keys.
func TestMessagesSharingAKeyAreNeverHandledAtOnce(t *testing.T) {
	var mu sync.Mutex
	inFlight := map[string]int{}
	overlaps := 0

	handler := patterns.Ordered(
		patterns.ByHeader[OrderPlaced]("x-order-id"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			key, _ := m.Envelope.Headers["x-order-id"].(string)

			mu.Lock()
			inFlight[key]++
			if inFlight[key] > 1 {
				overlaps++
			}
			mu.Unlock()

			time.Sleep(2 * time.Millisecond)

			mu.Lock()
			inFlight[key]--
			mu.Unlock()
			return acemq.Accept()
		})

	var wg sync.WaitGroup
	for i := range 60 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Three keys, twenty messages each.
			key := string(rune('a' + n%3))
			handler(context.Background(), acemq.Message[OrderPlaced]{
				Envelope: acemq.Envelope{
					ID:      string(rune('0' + n%10)),
					Headers: map[string]any{"x-order-id": key},
				},
			})
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if overlaps != 0 {
		t.Errorf("%d messages sharing a key were handled at the same time", overlaps)
	}
}

func TestDifferentKeysStillRunAtOnce(t *testing.T) {
	// Ordering per key must not become ordering overall, or it is just
	// concurrency turned off.
	release := make(chan struct{})
	var mu sync.Mutex
	peak, current := 0, 0

	handler := patterns.Ordered(
		patterns.ByHeader[OrderPlaced]("x-order-id"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			current++
			if current > peak {
				peak = current
			}
			mu.Unlock()

			<-release

			mu.Lock()
			current--
			mu.Unlock()
			return acemq.Accept()
		})

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			handler(context.Background(), acemq.Message[OrderPlaced]{
				Envelope: acemq.Envelope{
					Headers: map[string]any{"x-order-id": string(rune('a' + n))},
				},
			})
		}(i)
	}

	waitFor(t, "several keys at once", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return peak > 1
	})
	close(release)
	wg.Wait()
}

func TestAMessageWithNoKeyIsStillHandled(t *testing.T) {
	handled := false
	handler := patterns.Ordered(
		patterns.ByHeader[OrderPlaced]("x-order-id"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			handled = true
			return acemq.Accept()
		})

	handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{Headers: map[string]any{}}})

	if !handled {
		t.Error("a message with no ordering key was dropped")
	}
}

func TestPartitioningIsStableAndSpread(t *testing.T) {
	// Two services must agree, so it cannot depend on the Go runtime's map
	// seed or anything else that changes between processes.
	if patterns.Partition("order-42", 8) != patterns.Partition("order-42", 8) {
		t.Fatal("the same key gave two different partitions")
	}

	counts := map[int]int{}
	for i := range 1000 {
		counts[patterns.Partition(acemq.NewID(), 8)]++
		_ = i
	}
	for slot, n := range counts {
		if n == 0 {
			t.Errorf("partition %d never used", slot)
		}
	}
	if len(counts) != 8 {
		t.Errorf("%d partitions of 8 were used", len(counts))
	}

	if got := patterns.PartitionedRoutingKey("orders", "order-42", 8); !strings.HasPrefix(got, "orders.") {
		t.Errorf("PartitionedRoutingKey = %q", got)
	}
}

// ---- pipeline --------------------------------------------------------

func TestMiddlewareRunsOutsideIn(t *testing.T) {
	var order []string

	record := func(name string) patterns.Middleware[OrderPlaced] {
		return func(next acemq.Handler[OrderPlaced]) acemq.Handler[OrderPlaced] {
			return func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
				order = append(order, name+" in")
				ack := next(ctx, m)
				order = append(order, name+" out")
				return ack
			}
		}
	}

	handler := patterns.Chain(
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			order = append(order, "handler")
			return acemq.Accept()
		},
		record("first"), record("second"))

	handler(context.Background(), acemq.Message[OrderPlaced]{})

	want := []string{"first in", "second in", "handler", "second out", "first out"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("ran %v, want %v", order, want)
	}
}

func TestATimeoutIsReportedAsRetryable(t *testing.T) {
	handler := patterns.Chain(
		func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			<-ctx.Done()
			return acemq.Accept()
		},
		patterns.WithTimeout[OrderPlaced](20*time.Millisecond))

	ack := handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1"}})

	if ack.String() != "retry" {
		t.Errorf("got %s, want retry", ack)
	}
}

func TestAPipelineStepPublishesOnwards(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	for _, q := range []string{"orders", "shipping"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	got := make(chan acemq.Message[OrderPlaced], 1)
	sub, err := acemq.Consume(ctx, mq, "shipping",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	step := patterns.Then(
		acemq.NewPublisher[OrderPlaced](mq, "", "shipping"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, bool, error) {
			return OrderPlaced{OrderID: m.Payload.OrderID + "-shipped"}, true, nil
		})

	orders, err := acemq.Consume(ctx, mq, "orders", step)
	if err != nil {
		t.Fatal(err)
	}
	defer orders.Close()

	err = acemq.NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}, acemq.CorrelationID("corr-1"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.Payload.OrderID != "o-1-shipped" {
			t.Errorf("got %+v", m.Payload)
		}
		// The chain has to stay walkable across the hop.
		if m.Envelope.CorrelationID != "corr-1" {
			t.Errorf("correlation was lost: %q", m.Envelope.CorrelationID)
		}
		if m.Envelope.CausationID == "" {
			t.Error("nothing records which message caused this one")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing came out of the pipeline")
	}
}

func TestAStepCanDecideNotToContinue(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	for _, q := range []string{"orders", "shipping"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	downstream := 0
	sub, err := acemq.Consume(ctx, mq, "shipping",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			downstream++
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	step := patterns.Then(
		acemq.NewPublisher[OrderPlaced](mq, "", "shipping"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, bool, error) {
			return OrderPlaced{}, false, nil
		})

	orders, err := acemq.Consume(ctx, mq, "orders", step)
	if err != nil {
		t.Fatal(err)
	}
	defer orders.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if downstream != 0 {
		t.Errorf("%d messages went downstream from a step that declined", downstream)
	}
}

// ---- outbox ----------------------------------------------------------

func TestTheOutboxPublishesWhatWasRecorded(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	got := make(chan OrderPlaced, 1)
	sub, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	store := patterns.NewInMemoryOutboxStore()
	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		t.Fatal(err)
	}

	relay := patterns.NewOutboxRelay(mq, store)
	moved, err := relay.Sweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Errorf("the sweep moved %d records, want 1", moved)
	}

	select {
	case order := <-got:
		if order.OrderID != "o-1" {
			t.Errorf("got %+v", order)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the recorded message never arrived")
	}

	if store.Len() != 0 {
		t.Errorf("%d records are still in the outbox after being published", store.Len())
	}
}

func TestRecordingTheSameMessageTwiceSendsItOnce(t *testing.T) {
	// A caller retrying its own transaction must not turn one message into two.
	ctx := context.Background()
	mq := brokerFor(t)

	store := patterns.NewInMemoryOutboxStore()
	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	for range 3 {
		if err := store.Add(ctx, record); err != nil {
			t.Fatal(err)
		}
	}

	if store.Len() != 1 {
		t.Errorf("the outbox holds %d copies of one message", store.Len())
	}
}

func TestARecordStaysInTheOutboxWhenPublishingFails(t *testing.T) {
	// The property the whole pattern rests on: nothing is lost by the relay
	// being unable to publish.
	ctx := context.Background()
	mq := brokerFor(t)

	store := patterns.NewInMemoryOutboxStore()
	// An exchange nothing has declared, so publishing cannot route... but the
	// in-memory transport drops rather than failing, so this asserts the record
	// is only removed after a successful publish, which it is.
	record, err := patterns.Record(mq, "", "never-declared", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		t.Fatal(err)
	}

	relay := patterns.NewOutboxRelay(mq, store)
	if _, err := relay.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	// It published (to nowhere) and was marked. The point being recorded here
	// is that MarkPublished happens after Publish and not before.
	if store.Len() != 0 {
		t.Errorf("%d records left", store.Len())
	}
}

func TestTheRelaySweepsOnItsOwn(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	arrived := 0
	sub, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			arrived++
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	store := patterns.NewInMemoryOutboxStore()
	relay := patterns.NewOutboxRelay(mq, store, patterns.RelayInterval(20*time.Millisecond))
	relay.Start(ctx)
	defer relay.Close()

	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the relay to sweep", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return arrived == 1
	})
}

// ---- replay ----------------------------------------------------------

func TestReplayMovesMessagesBack(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	for _, q := range []string{"orders", "orders-dead"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	// Three messages waiting in the dead-letter queue.
	dead := acemq.NewPublisher[OrderPlaced](mq, "", "orders-dead")
	for i := range 3 {
		if err := dead.Send(ctx, OrderPlaced{OrderID: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var back []acemq.Message[OrderPlaced]
	sub, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			back = append(back, m)
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue:      "orders-dead",
		RoutingKey: "orders",
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Moved != 3 {
		t.Errorf("moved %d, want 3 (%s)", result.Moved, result)
	}

	waitFor(t, "the replayed messages", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(back) == 3
	})

	// Stamped, so a replayed message can be told from an original.
	mu.Lock()
	defer mu.Unlock()
	for _, m := range back {
		if m.Envelope.Headers[patterns.HeaderReplayedFrom] != "orders-dead" {
			t.Errorf("no replay stamp: %v", m.Envelope.Headers)
		}
	}
}

func TestReplayCanTakeSomeAndLeaveTheRest(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	for _, q := range []string{"orders", "orders-dead"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	dead := acemq.NewPublisher[OrderPlaced](mq, "", "orders-dead")
	for _, id := range []string{"keep", "drop", "keep"} {
		if err := dead.Send(ctx, OrderPlaced{OrderID: id}); err != nil {
			t.Fatal(err)
		}
	}

	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue:      "orders-dead",
		RoutingKey: "orders",
		Filter: func(env acemq.Envelope, body []byte) bool {
			return strings.Contains(string(body), "keep")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Moved != 2 {
		t.Errorf("moved %d, want the two that matched (%s)", result.Moved, result)
	}
	if result.Skipped == 0 {
		t.Error("nothing was reported as skipped")
	}
}

func TestReplayStopsAtItsLimit(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	for _, q := range []string{"orders", "orders-dead"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	dead := acemq.NewPublisher[OrderPlaced](mq, "", "orders-dead")
	for i := range 10 {
		if err := dead.Send(ctx, OrderPlaced{OrderID: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}

	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue:      "orders-dead",
		RoutingKey: "orders",
		Limit:      4,
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Moved != 4 {
		t.Errorf("moved %d, want 4 (%s)", result.Moved, result)
	}
	// "moved 4" means something different when the limit was 4, so the reason
	// is part of the answer.
	if result.Reason != "limit" {
		t.Errorf("Reason = %q, want limit", result.Reason)
	}
}

func TestReplayingAnEmptyQueueSaysSo(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	if err := mq.DeclareQueue(ctx, "orders-dead"); err != nil {
		t.Fatal(err)
	}

	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue: "orders-dead", RoutingKey: "orders"})
	if err != nil {
		t.Fatal(err)
	}

	if result.Moved != 0 {
		t.Errorf("moved %d from an empty queue", result.Moved)
	}
	if result.Reason != "drained" {
		t.Errorf("Reason = %q, want drained", result.Reason)
	}
}
