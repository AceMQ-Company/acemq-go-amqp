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

// What a requester and a responder count.
//
// Both were silent: acemq.MetricRequestDuration and acemq.MetricRequestTotal
// were names this library declared and never wrote, and a Responder reported
// nothing at all where Java reports answered() and unanswerable(). The ordering
// is the part worth testing rather than the arithmetic — answered has to be
// visible to anybody already holding the reply, which is a statement about
// interleavings and not about a total.

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

type Quote struct {
	Widget string `json:"widget"`
}

type Price struct {
	Cents int64 `json:"cents"`
}

func countOf(t *testing.T, metrics *acemq.Metrics, metric, routingKey, outcome string) int64 {
	t.Helper()
	key := metric + "{outcome=" + outcome + "}{routing.key=" + routingKey + "}"
	return metrics.Counts()[key]
}

func durationOf(t *testing.T, metrics *acemq.Metrics, metric, routingKey, outcome string) acemq.DurationSummary {
	t.Helper()
	key := metric + "{outcome=" + outcome + "}{routing.key=" + routingKey + "}"
	return metrics.Durations()[key]
}

// TestAnAnsweredRoundTripIsCountedAndTimed is the metric the page said did not
// exist.
//
// The publish was already timed, and the reply's delivery was already timed on
// the responder's side. Neither of those is what the caller waited for, and that
// number — the one a blocked goroutine is actually holding — is what this adds.
func TestAnAnsweredRoundTripIsCountedAndTimed(t *testing.T) {
	ctx := context.Background()
	metrics := acemq.NewMetrics()
	mq := brokerFor(t, acemq.WithObserver(metrics))

	if err := mq.DeclareQueue(ctx, "pricing"); err != nil {
		t.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "pricing",
		func(_ context.Context, m acemq.Message[Quote]) (Price, error) {
			return Price{Cents: 4250}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = responder.Close() }()

	requester, err := patterns.NewRequester[Quote, Price](ctx, mq, "", "pricing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = requester.Close() }()

	answer, err := requester.Do(ctx, Quote{Widget: "widget"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if answer.Cents != 4250 {
		t.Fatalf("the answer is %d", answer.Cents)
	}

	if got := countOf(t, metrics, acemq.MetricRequestTotal, "pricing", acemq.OutcomeAnswered); got != 1 {
		t.Errorf("%s{answered} is %d, want 1", acemq.MetricRequestTotal, got)
	}
	took := durationOf(t, metrics, acemq.MetricRequestDuration, "pricing", acemq.OutcomeAnswered)
	if took.Count != 1 {
		t.Errorf("%s{answered} recorded %d samples, want 1", acemq.MetricRequestDuration, took.Count)
	}
	if took.Sum <= 0 {
		t.Errorf("the round trip was recorded as %v seconds", took.Sum)
	}
}

// TestATimeoutIsAnOutcomeRatherThanAMissingMeasurement is the case most worth
// graphing and the easiest one to leave out.
//
// timed_out and not failed, deliberately: the work may well have been done, and
// a counter that called a timeout a failure sends somebody looking for one that
// did not happen. The tracing adapter has drawn this distinction since it was
// written; now the counter does too, with the same word.
func TestATimeoutIsAnOutcomeRatherThanAMissingMeasurement(t *testing.T) {
	ctx := context.Background()
	metrics := acemq.NewMetrics()
	mq := brokerFor(t, acemq.WithObserver(metrics))

	if err := mq.DeclareQueue(ctx, "unanswered"); err != nil {
		t.Fatal(err)
	}

	// Nobody is serving it.
	requester, err := patterns.NewRequester[Quote, Price](
		ctx, mq, "", "unanswered", patterns.Timeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = requester.Close() }()

	if _, err := requester.Do(ctx, Quote{Widget: "widget"}); !errors.Is(err, patterns.ErrRequestTimedOut) {
		t.Fatalf("Do returned %v, want a timeout", err)
	}

	if got := countOf(t, metrics, acemq.MetricRequestTotal, "unanswered", acemq.OutcomeTimedOut); got != 1 {
		t.Errorf("%s{timed_out} is %d, want 1", acemq.MetricRequestTotal, got)
	}
	if got := countOf(t, metrics, acemq.MetricRequestTotal, "unanswered", acemq.OutcomeFailed); got != 0 {
		t.Errorf("a timeout was also counted as failed %d times", got)
	}

	// The wait is part of the measurement. A timeout recorded at zero seconds
	// would drag down the average of a metric read as "how long do round trips
	// take" with the trips that took longest.
	took := durationOf(t, metrics, acemq.MetricRequestDuration, "unanswered", acemq.OutcomeTimedOut)
	if took.Count != 1 {
		t.Fatalf("%s{timed_out} recorded %d samples, want 1", acemq.MetricRequestDuration, took.Count)
	}
	if took.Sum < 0.15 {
		t.Errorf("a 200ms timeout was recorded as %v seconds", took.Sum)
	}
}

// TestAResponderFailureIsFailedAndNotAnswered is the third outcome.
//
// The round trip completed and the answer was bad news, which is a different
// thing from no answer — and a different thing again from an answer. All three
// have to be tellable apart from a dashboard.
func TestAResponderFailureIsFailedAndNotAnswered(t *testing.T) {
	ctx := context.Background()
	metrics := acemq.NewMetrics()
	mq := brokerFor(t, acemq.WithObserver(metrics))

	if err := mq.DeclareQueue(ctx, "failing"); err != nil {
		t.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "failing",
		func(_ context.Context, m acemq.Message[Quote]) (Price, error) {
			return Price{}, errors.New("the pricing service is down")
		})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = responder.Close() }()

	requester, err := patterns.NewRequester[Quote, Price](
		ctx, mq, "", "failing", patterns.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = requester.Close() }()

	if _, err := requester.Do(ctx, Quote{Widget: "widget"}); err == nil {
		t.Fatal("a failing responder answered successfully")
	}

	if got := countOf(t, metrics, acemq.MetricRequestTotal, "failing", acemq.OutcomeFailed); got != 1 {
		t.Errorf("%s{failed} is %d, want 1", acemq.MetricRequestTotal, got)
	}
	if got := countOf(t, metrics, acemq.MetricRequestTotal, "failing", acemq.OutcomeAnswered); got != 0 {
		t.Errorf("a failure was counted as answered %d times", got)
	}

	// And the responder did not count it either. A responder that fails every
	// request must not look like one that works.
	if got := responder.Answered(); got != 0 {
		t.Errorf("the responder answered %d requests while failing all of them", got)
	}
}

// TestAnsweredIsVisibleToWhoeverIsHoldingTheReply is the ordering, and the whole
// reason the counter is incremented where it is.
//
// The increment happens before the publish, so there is no interleaving in which
// the answer is in the caller's hands and the number is still zero. The other
// order looks more natural and is wrong: it leaves a window that reads as an
// idle service which is demonstrably working. No sleep here on purpose — code
// that has to sleep before reading a counter is working around a defect, and
// this asserts there is not one.
func TestAnsweredIsVisibleToWhoeverIsHoldingTheReply(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "immediate"); err != nil {
		t.Fatal(err)
	}

	responder, err := patterns.Serve(ctx, mq, "immediate",
		func(_ context.Context, m acemq.Message[Quote]) (Price, error) {
			return Price{Cents: 1}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = responder.Close() }()

	requester, err := patterns.NewRequester[Quote, Price](ctx, mq, "", "immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = requester.Close() }()

	for i := 1; i <= 5; i++ {
		if _, err := requester.Do(ctx, Quote{Widget: "widget"}); err != nil {
			t.Fatalf("round trip %d: %v", i, err)
		}
		// Read immediately, with nothing in between.
		if got := responder.Answered(); got < int64(i) {
			t.Fatalf("the caller holds answer %d and the responder says %d were answered", i, got)
		}
	}

	if got := responder.Answered(); got != 5 {
		t.Errorf("the responder answered %d of 5", got)
	}
	if got := responder.Unanswerable(); got != 0 {
		t.Errorf("%d requests were counted as unanswerable", got)
	}
}

// TestARequestWithNowhereToReplyIsCounted is the number that says a caller is
// publishing where it means to request.
//
// Anything above zero is a broken sender, and without the counter there is
// nothing to look at: the request is dead-lettered, so it does not show up as a
// timeout on anybody's side and the responder's queue just looks quiet.
func TestARequestWithNowhereToReplyIsCounted(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orphaned"); err != nil {
		t.Fatal(err)
	}
	responder, err := patterns.Serve(ctx, mq, "orphaned",
		func(_ context.Context, m acemq.Message[Quote]) (Price, error) {
			t.Error("the handler ran for a request that named nowhere to reply")
			return Price{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = responder.Close() }()

	// Published rather than requested: no reply-to property and no
	// acemq-reply-to header, which is exactly what publish-where-you-meant-
	// request looks like on the wire.
	if err := acemq.NewPublisher[Quote](mq, "", "orphaned").Send(ctx, Quote{Widget: "widget"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the unanswerable request to be counted", func() bool {
		return responder.Unanswerable() == 1
	})
	if got := responder.Answered(); got != 0 {
		t.Errorf("a request nobody could answer was counted as answered %d times", got)
	}
}

// TestTheCountersExistBeforeTheResponderSubscribes is the defect .NET had and
// had to lift its counters out of the responder to fix.
//
// A broker may hand the first request over from inside the subscribe — what a
// queue with a backlog looks like from in here — and the handler reads the
// counters on that very delivery. Counters reached through a variable the
// subscribe has not assigned yet are a nil dereference or a lost count,
// depending on the language. Here the transport delivers during Consume, which
// is the moment being tested.
func TestTheCountersExistBeforeTheResponderSubscribes(t *testing.T) {
	transport := &eagerTransport{}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.Close() }()

	responder, err := patterns.Serve(context.Background(), mq, "backlog",
		func(_ context.Context, m acemq.Message[Quote]) (Price, error) {
			return Price{Cents: 99}, nil
		})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer func() { _ = responder.Close() }()

	// The request was handled inside acemq.Consume, before Serve returned.
	if got := responder.Answered(); got != 1 {
		t.Errorf("a request delivered during start-up was counted %d times, want 1", got)
	}
}

// settledOnce closes a channel the first time a delivery is settled either way,
// so a fake transport can hold Consume open until the engine has finished with
// the message it handed over.
type settledOnce struct {
	once sync.Once
	done chan struct{}
}

func newSettled() *settledOnce { return &settledOnce{done: make(chan struct{})} }

func (s *settledOnce) settle() { s.once.Do(func() { close(s.done) }) }

func (s *settledOnce) wait(timeout time.Duration) {
	select {
	case <-s.done:
	case <-time.After(timeout):
	}
}

// eagerTransport hands a request over from inside Consume, before it returns.
type eagerTransport struct {
	mu        sync.Mutex
	published []string
}

func (e *eagerTransport) DeclareQueue(context.Context, string, acemq.QueueSpec) error { return nil }

func (e *eagerTransport) DeclareExchange(context.Context, string, acemq.ExchangeSpec) error {
	return nil
}

func (e *eagerTransport) Bind(context.Context, string, string, string) error { return nil }

func (e *eagerTransport) Publish(
	_ context.Context, _, routingKey string, msg acemq.Outbound,
) (acemq.PublishResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.published = append(e.published, routingKey)
	return acemq.PublishResult{MessageID: msg.MessageID, Confirmed: true, Routed: true}, nil
}

func (e *eagerTransport) Consume(
	_ context.Context, queue string, _ acemq.ConsumeSpec, deliver func(acemq.Delivery),
) (acemq.Subscription, error) {
	// Handed over and finished with before this returns, which is the point of
	// this type: everything the handler did happened while Serve was still
	// inside acemq.Consume and had not built its Responder yet.
	settled := newSettled()
	deliver(acemq.Delivery{
		Body:        []byte(`{"widget":"widget"}`),
		ContentType: "application/json",
		RoutingKey:  queue,
		MessageID:   "during-startup",
		ReplyTo:     "somewhere",
		Headers:     map[string]any{acemq.HeaderID: "during-startup"},
		Ack:         func() error { settled.settle(); return nil },
		Nack:        func(bool) error { settled.settle(); return nil },
	})
	settled.wait(10 * time.Second)
	return fakeSubscription{}, nil
}

func (e *eagerTransport) Close() error { return nil }

// TestAFailedReplyHandsItsIncrementBack is what makes counting early honest.
//
// Incrementing before the publish is the only ordering a caller can rely on, and
// on its own it would introduce a failure of its own: a send that never happened
// counted as an answer. The increment is taken back when the publish fails, so
// this counts replies that were sent rather than replies that were attempted.
func TestAFailedReplyHandsItsIncrementBack(t *testing.T) {
	transport := &refusingReplies{}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.Close() }()

	responder, err := patterns.Serve(context.Background(), mq, "pricing",
		func(_ context.Context, m acemq.Message[Quote]) (Price, error) {
			return Price{Cents: 4250}, nil
		})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer func() { _ = responder.Close() }()

	if got := responder.Answered(); got != 0 {
		t.Errorf("a reply that never left was counted as an answer %d times", got)
	}
	if !transport.attempted() {
		t.Error("the reply was never attempted, so this proves nothing")
	}
}

// refusingReplies delivers one request during Consume and then refuses to
// publish the reply.
type refusingReplies struct {
	mu    sync.Mutex
	tried bool
}

func (r *refusingReplies) attempted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tried
}

func (r *refusingReplies) DeclareQueue(context.Context, string, acemq.QueueSpec) error { return nil }

func (r *refusingReplies) DeclareExchange(context.Context, string, acemq.ExchangeSpec) error {
	return nil
}

func (r *refusingReplies) Bind(context.Context, string, string, string) error { return nil }

func (r *refusingReplies) Publish(
	_ context.Context, _, routingKey string, _ acemq.Outbound,
) (acemq.PublishResult, error) {
	if strings.HasPrefix(routingKey, "somewhere") {
		r.mu.Lock()
		r.tried = true
		r.mu.Unlock()
		return acemq.PublishResult{}, errors.New("the reply queue is gone")
	}
	return acemq.PublishResult{Confirmed: true, Routed: true}, nil
}

func (r *refusingReplies) Consume(
	_ context.Context, queue string, _ acemq.ConsumeSpec, deliver func(acemq.Delivery),
) (acemq.Subscription, error) {
	settled := newSettled()
	deliver(acemq.Delivery{
		Body:        []byte(`{"widget":"widget"}`),
		ContentType: "application/json",
		RoutingKey:  queue,
		MessageID:   "one",
		ReplyTo:     "somewhere",
		Headers:     map[string]any{acemq.HeaderID: "one"},
		Ack:         func() error { settled.settle(); return nil },
		Nack:        func(bool) error { settled.settle(); return nil },
	})
	settled.wait(10 * time.Second)
	return fakeSubscription{}, nil
}

func (r *refusingReplies) Close() error { return nil }
