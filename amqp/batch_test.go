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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// batchItem is a payload that says where it was in the batch, so a test can
// follow one message from the order it was given in to the order it was
// confirmed in.
type batchItem struct {
	Index int `json:"index"`
}

// gateTransport is a broker that holds every publish until the test answers it.
//
// The in-memory transport confirms a message before Publish returns, which
// makes a pipelined batch and a loop over Send indistinguishable: both finish,
// both in order, both pass. This one parks each publish on a channel of its
// own, so a test can require that every message reached the broker before any
// of them was answered — a state a loop that awaits each confirm in turn can
// never reach, because its second publish does not happen until its first
// confirm has.
type gateTransport struct {
	mu sync.Mutex

	// arrived is the payload indexes the broker saw, in the order it saw them.
	arrived []int

	// answered is the payload indexes the broker confirmed, in the order it
	// confirmed them, which a test deliberately makes differ from arrived.
	answered []int

	// ids is the message identifier each payload went out with, which is how a
	// result is matched back to the payload that produced it.
	ids map[int]string

	// gates is where each publish waits. The value sent is what the broker
	// answers with: nil for a confirm, an error for a refusal.
	gates map[int]chan error
}

func newGateTransport() *gateTransport {
	return &gateTransport{ids: map[int]string{}, gates: map[int]chan error{}}
}

func (g *gateTransport) DeclareQueue(context.Context, string, QueueSpec) error       { return nil }
func (g *gateTransport) DeclareExchange(context.Context, string, ExchangeSpec) error { return nil }
func (g *gateTransport) Bind(context.Context, string, string, string) error          { return nil }
func (g *gateTransport) Close() error                                                { return nil }

func (g *gateTransport) Consume(
	context.Context, string, ConsumeSpec, func(Delivery),
) (Subscription, error) {
	return nil, errors.New("acemq: the gate transport does not consume")
}

func (g *gateTransport) Publish(
	ctx context.Context, _, _ string, msg Outbound,
) (PublishResult, error) {
	var item batchItem
	if err := json.Unmarshal(msg.Body, &item); err != nil {
		return PublishResult{}, fmt.Errorf("gate transport was sent %q: %w", msg.Body, err)
	}

	gate := make(chan error, 1)
	g.mu.Lock()
	g.arrived = append(g.arrived, item.Index)
	g.ids[item.Index] = msg.MessageID
	g.gates[item.Index] = gate
	g.mu.Unlock()

	result := PublishResult{MessageID: msg.MessageID}
	select {
	case err := <-gate:
		if err != nil {
			return result, err
		}
		result.Confirmed = true
		result.Routed = true
		return result, nil
	case <-ctx.Done():
		return result, ctx.Err()
	}
}

// waitForArrivals blocks until n publishes are waiting at the gate.
//
// This is the assertion the whole fake exists for: reaching n means n messages
// were on their way to the broker at one moment, none of them yet answered.
func (g *gateTransport) waitForArrivals(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		got := len(g.arrived)
		g.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	g.mu.Lock()
	got := len(g.arrived)
	g.mu.Unlock()
	t.Fatalf("only %d of %d messages reached the broker before any confirm was answered; "+
		"the batch is awaiting each confirm where it publishes instead of pipelining", got, n)
}

// answer releases one waiting publish, confirming it or refusing it.
func (g *gateTransport) answer(t *testing.T, index int, err error) {
	t.Helper()
	g.mu.Lock()
	gate, waiting := g.gates[index]
	if waiting {
		g.answered = append(g.answered, index)
	}
	g.mu.Unlock()
	if !waiting {
		t.Fatalf("message %d is not waiting at the broker", index)
	}
	gate <- err
}

func (g *gateTransport) arrivals() []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int(nil), g.arrived...)
}

func (g *gateTransport) messageID(index int) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ids[index]
}

// batchOutcome is what SendAll answered, carried off the goroutine it ran on.
type batchOutcome struct {
	results []PublishResult
	err     error
}

// gatedBatch starts a batch of n messages against a gating broker and hands
// back the fake and a channel the outcome will arrive on.
func gatedBatch(t *testing.T, n int) (*gateTransport, <-chan batchOutcome) {
	t.Helper()

	gate := newGateTransport()
	mq, err := NewConn(gate)
	if err != nil {
		t.Fatalf("cannot build a connection: %v", err)
	}
	t.Cleanup(func() { _ = mq.Close() })

	// Cancelled on the way out so a publish still parked at the gate when a test
	// fails is released rather than left holding a goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	payloads := make([]batchItem, n)
	for i := range payloads {
		payloads[i] = batchItem{Index: i}
	}

	pub := NewPublisher[batchItem](mq, "", "batch")
	done := make(chan batchOutcome, 1)
	go func() {
		results, err := pub.SendAll(ctx, payloads)
		done <- batchOutcome{results: results, err: err}
	}()
	return gate, done
}

// awaitBatch takes the outcome, or says the batch never finished.
func awaitBatch(t *testing.T, done <-chan batchOutcome) batchOutcome {
	t.Helper()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(3 * time.Second):
		t.Fatal("SendAll did not return after every confirm was answered")
		return batchOutcome{}
	}
}

// TestSendAllPublishesEverythingBeforeAwaitingAnyConfirm is the test the
// feature exists for.
//
// Every message is held at the broker unanswered. A batch that pipelines gets
// all five there; a loop that waits for each confirm as it publishes gets one
// there and stops, and this times out on the second.
func TestSendAllPublishesEverythingBeforeAwaitingAnyConfirm(t *testing.T) {
	gate, done := gatedBatch(t, 5)

	gate.waitForArrivals(t, 5)

	for _, i := range []int{0, 1, 2, 3, 4} {
		gate.answer(t, i, nil)
	}

	outcome := awaitBatch(t, done)
	if outcome.err != nil {
		t.Fatalf("SendAll: %v", outcome.err)
	}
	if len(outcome.results) != 5 {
		t.Fatalf("got %d results, want 5", len(outcome.results))
	}
	for i, r := range outcome.results {
		if !r.Confirmed {
			t.Errorf("result %d was not confirmed: %+v", i, r)
		}
	}
}

// TestSendAllReturnsResultsInPayloadOrder answers the confirms backwards, which
// is the broker's right, and expects the results to come back forwards.
func TestSendAllReturnsResultsInPayloadOrder(t *testing.T) {
	gate, done := gatedBatch(t, 4)

	gate.waitForArrivals(t, 4)
	for _, i := range []int{3, 1, 2, 0} {
		gate.answer(t, i, nil)
	}

	outcome := awaitBatch(t, done)
	if outcome.err != nil {
		t.Fatalf("SendAll: %v", outcome.err)
	}
	if len(outcome.results) != 4 {
		t.Fatalf("got %d results, want 4", len(outcome.results))
	}

	// The identifier is what ties a result to the payload that produced it, so
	// this fails if the results were collected in confirmation order.
	for i, r := range outcome.results {
		if want := gate.messageID(i); r.MessageID != want {
			t.Errorf("result %d is message %q, want %q (the results are in the order the "+
				"broker answered, not the order the payloads were given)", i, r.MessageID, want)
		}
	}
}

// TestSendAllAwaitsTheRestAfterAFailure fails the second message of four and
// expects the other three to have been published and awaited anyway — the count
// of what arrived is the thing a caller needs in order not to send them twice.
func TestSendAllAwaitsTheRestAfterAFailure(t *testing.T) {
	gate, done := gatedBatch(t, 4)

	gate.waitForArrivals(t, 4)
	refused := errors.New("the broker refused it")
	gate.answer(t, 1, refused)
	for _, i := range []int{0, 2, 3} {
		gate.answer(t, i, nil)
	}

	outcome := awaitBatch(t, done)

	var failure *BatchPublishFailedError
	if !errors.As(outcome.err, &failure) {
		t.Fatalf("SendAll returned %v, want a *BatchPublishFailedError", outcome.err)
	}
	if failure.Total != 4 || failure.Failed != 1 || failure.Confirmed != 3 {
		t.Errorf("counts are total=%d failed=%d confirmed=%d, want 4, 1 and 3",
			failure.Total, failure.Failed, failure.Confirmed)
	}
	if !errors.Is(outcome.err, refused) {
		t.Errorf("the error does not unwrap to the broker's refusal: %v", outcome.err)
	}

	want := "1 of 4 messages were not confirmed; 3 were. " +
		"The first failure was: the broker refused it"
	if got := outcome.err.Error(); got != want {
		t.Errorf("message is\n\t%q\nwant\n\t%q", got, want)
	}

	// Every message was published, including the three after the one that
	// failed, and the failure is recorded against the payload it belongs to.
	if got := len(gate.arrivals()); got != 4 {
		t.Errorf("%d messages reached the broker, want 4: a failure cut the batch short", got)
	}
	if len(failure.Errors) != 4 {
		t.Fatalf("got %d per-payload errors, want 4", len(failure.Errors))
	}
	for i, err := range failure.Errors {
		if (i == 1) != (err != nil) {
			t.Errorf("payload %d has error %v, which is not where the failure was", i, err)
		}
	}
	for i, r := range outcome.results {
		if confirmed := r.Confirmed; confirmed == (i == 1) {
			t.Errorf("result %d is confirmed=%v", i, confirmed)
		}
	}
}

// TestSendAllReportsTheFirstFailureInPayloadOrder has the broker refuse the
// fourth message before it refuses the second. The one named is the second: the
// order the payloads were given in is the only order a caller can reason about.
func TestSendAllReportsTheFirstFailureInPayloadOrder(t *testing.T) {
	gate, done := gatedBatch(t, 5)

	gate.waitForArrivals(t, 5)
	gate.answer(t, 3, errors.New("answered first, later in the batch"))
	gate.answer(t, 1, errors.New("answered second, earlier in the batch"))
	for _, i := range []int{0, 2, 4} {
		gate.answer(t, i, nil)
	}

	outcome := awaitBatch(t, done)

	var failure *BatchPublishFailedError
	if !errors.As(outcome.err, &failure) {
		t.Fatalf("SendAll returned %v, want a *BatchPublishFailedError", outcome.err)
	}
	if failure.First == nil || failure.First.Error() != "answered second, earlier in the batch" {
		t.Errorf("first failure is %v, want the one at payload 1", failure.First)
	}

	want := "2 of 5 messages were not confirmed; 3 were. " +
		"The first failure was: answered second, earlier in the batch"
	if got := outcome.err.Error(); got != want {
		t.Errorf("message is\n\t%q\nwant\n\t%q", got, want)
	}
}

// TestSendAllRefusesAnEnvelopeItCannotBuild covers the failure that happens
// before anything is published: a reserved header is refused per payload, and
// the batch still reports counts rather than the first error on its own.
func TestSendAllRefusesAnEnvelopeItCannotBuild(t *testing.T) {
	gate := newGateTransport()
	mq, err := NewConn(gate)
	if err != nil {
		t.Fatalf("cannot build a connection: %v", err)
	}
	defer func() { _ = mq.Close() }()

	pub := NewPublisher[batchItem](mq, "", "batch")
	results, err := pub.SendAll(context.Background(),
		[]batchItem{{Index: 0}, {Index: 1}, {Index: 2}}, Header("x-acemq-id", "no"))

	var failure *BatchPublishFailedError
	if !errors.As(err, &failure) {
		t.Fatalf("SendAll returned %v, want a *BatchPublishFailedError", err)
	}
	if failure.Total != 3 || failure.Failed != 3 || failure.Confirmed != 0 {
		t.Errorf("counts are total=%d failed=%d confirmed=%d, want 3, 3 and 0",
			failure.Total, failure.Failed, failure.Confirmed)
	}
	if len(results) != 3 {
		t.Errorf("got %d results, want one per payload", len(results))
	}
	if got := len(gate.arrivals()); got != 0 {
		t.Errorf("%d messages reached the broker, want none", got)
	}
}

// TestSendAllOfNothingSucceeds because a batch with nothing in it has nothing
// that failed, and a caller looping over an empty slice should not have to
// guard the call.
func TestSendAllOfNothingSucceeds(t *testing.T) {
	mq := brokerFor(t)
	declare(t, mq, "batch")

	pub := NewPublisher[batchItem](mq, "", "batch")
	results, err := pub.SendAll(context.Background(), nil)
	if err != nil {
		t.Fatalf("SendAll of nothing: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want none", len(results))
	}
}

// TestSendAllDeliversEveryMessage checks the ordinary path end to end against
// the in-memory broker, which the gating fake deliberately does not exercise.
func TestSendAllDeliversEveryMessage(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "batch")

	payloads := make([]batchItem, 50)
	for i := range payloads {
		payloads[i] = batchItem{Index: i}
	}

	pub := NewPublisher[batchItem](mq, "", "batch")
	results, err := pub.SendAll(ctx, payloads)
	if err != nil {
		t.Fatalf("SendAll: %v", err)
	}

	seen := map[string]bool{}
	for i, r := range results {
		if !r.Confirmed {
			t.Errorf("result %d was not confirmed", i)
		}
		if seen[r.MessageID] {
			t.Errorf("result %d reuses message id %q", i, r.MessageID)
		}
		seen[r.MessageID] = true
	}

	count, err := mq.MessageCount(ctx, "batch")
	if err != nil {
		t.Fatalf("cannot count the queue: %v", err)
	}
	if count != int64(len(payloads)) {
		t.Errorf("the queue holds %d messages, want %d", count, len(payloads))
	}
}
