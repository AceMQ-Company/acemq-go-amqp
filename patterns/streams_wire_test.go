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

// What DeclareStream and ReadStream put on the wire, and what happens to a
// delivery once it reaches a handler.
//
// The in-memory transport refuses streams on purpose — falling back to an
// ordinary queue would quietly lose replay, retention and every consumer's
// independent position — so for a long time nothing in patterns/streams.go was
// exercised at all. The gap is closed from both ends: this file stands a
// transport up that says it supports streams and records every argument, and
// rabbitmq/streams_test.go reads a real one.
//
// What this file can prove is everything between the caller and the transport:
// the retention arguments, the offset argument, the prefetch, the consumer tag,
// and where a message goes when a handler fails. What it cannot prove is that
// RabbitMQ agrees about any of it, which is what the broker-backed tests are for.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

type streamEvent struct {
	OrderID string `json:"orderId"`
}

// fakeStream is a transport that claims streams and writes everything down.
type fakeStream struct {
	mu sync.Mutex

	queues    map[string]acemq.QueueSpec
	consumed  map[string]acemq.ConsumeSpec
	published []publishedTo

	deliver func(acemq.Delivery)
}

type publishedTo struct {
	exchange   string
	routingKey string
	headers    map[string]any
	body       []byte
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		queues:   map[string]acemq.QueueSpec{},
		consumed: map[string]acemq.ConsumeSpec{},
	}
}

func (f *fakeStream) Supports(c acemq.Capability) bool {
	return c == acemq.CapabilityStreams || c == acemq.CapabilityPublisherConfirms
}

func (f *fakeStream) DeclareQueue(_ context.Context, name string, spec acemq.QueueSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queues[name] = spec
	return nil
}

func (f *fakeStream) DeclareExchange(context.Context, string, acemq.ExchangeSpec) error { return nil }

func (f *fakeStream) Bind(context.Context, string, string, string) error { return nil }

func (f *fakeStream) Publish(
	_ context.Context, exchange, routingKey string, msg acemq.Outbound,
) (acemq.PublishResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, publishedTo{
		exchange: exchange, routingKey: routingKey, headers: msg.Headers, body: msg.Body})
	return acemq.PublishResult{MessageID: msg.MessageID, Confirmed: true, Routed: true}, nil
}

func (f *fakeStream) Consume(
	_ context.Context, queue string, spec acemq.ConsumeSpec, deliver func(acemq.Delivery),
) (acemq.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumed[queue] = spec
	f.deliver = deliver
	return fakeSubscription{}, nil
}

func (f *fakeStream) Close() error { return nil }

type fakeSubscription struct{}

func (fakeSubscription) Close() error { return nil }

// hand pushes one delivery at the handler, the way a broker would.
func (f *fakeStream) hand(id string, offset any, payload streamEvent) {
	body, _ := json.Marshal(payload)

	headers := map[string]any{acemq.HeaderID: id, acemq.HeaderType: "order.placed"}
	if offset != nil {
		headers["x-stream-offset"] = offset
	}

	f.mu.Lock()
	deliver := f.deliver
	f.mu.Unlock()

	deliver(acemq.Delivery{
		Body:        body,
		ContentType: "application/json",
		RoutingKey:  "orders.log",
		MessageID:   id,
		Headers:     headers,
		Ack:         func() error { return nil },
		Nack:        func(bool) error { return nil },
	})
}

func (f *fakeStream) sentTo(queue string) []publishedTo {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []publishedTo
	for _, p := range f.published {
		if p.exchange == "" && p.routingKey == queue {
			out = append(out, p)
		}
	}
	return out
}

func streamConn(t *testing.T, transport acemq.Transport) *acemq.Conn {
	t.Helper()
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

// TestDeclareStreamSpellsRetentionTheWayRabbitMqWantsIt is the declaration.
//
// Every one of these argument names is a wire contract shared with the Java,
// .NET, Python and Ruby libraries: a queue redeclared with an argument the first
// declaration did not carry is answered with PRECONDITION_FAILED rather than
// adjusted, so a name invented here would break every stream first declared by
// one of the others.
func TestDeclareStreamSpellsRetentionTheWayRabbitMqWantsIt(t *testing.T) {
	transport := newFakeStream()
	mq := streamConn(t, transport)

	err := patterns.DeclareStream(context.Background(), mq, "orders.log", patterns.StreamRetention{
		MaxAge:       7 * 24 * time.Hour,
		MaxBytes:     10 << 30,
		SegmentBytes: 100 << 20,
	})
	if err != nil {
		t.Fatalf("DeclareStream: %v", err)
	}

	spec := transport.queues["orders.log"]
	if spec.Args["x-queue-type"] != string(acemq.QueueStream) {
		t.Errorf("x-queue-type is %v, want stream", spec.Args["x-queue-type"])
	}
	if spec.Args["x-max-age"] != "7D" {
		t.Errorf("x-max-age is %v, want 7D", spec.Args["x-max-age"])
	}
	if spec.Args["x-max-length-bytes"] != int64(10<<30) {
		t.Errorf("x-max-length-bytes is %v", spec.Args["x-max-length-bytes"])
	}
	if spec.Args["x-stream-max-segment-size-bytes"] != int64(100<<20) {
		t.Errorf("x-stream-max-segment-size-bytes is %v", spec.Args["x-stream-max-segment-size-bytes"])
	}

	// RabbitMQ refuses a stream that is any of these, and the error it gives
	// does not mention streams.
	if !spec.Durable || spec.Exclusive || spec.AutoDelete {
		t.Errorf("a stream was declared durable=%v exclusive=%v autoDelete=%v",
			spec.Durable, spec.Exclusive, spec.AutoDelete)
	}
}

// TestRetentionNobodySetIsRetentionNobodyDeclares is the other half of the
// argument contract, and the more dangerous one.
//
// The broker has its own segment size, tuned for its storage rather than for any
// one stream. A default invented here would be an argument the first declaration
// carried and a Java-declared stream did not, which is PRECONDITION_FAILED for
// whichever service starts second.
func TestRetentionNobodySetIsRetentionNobodyDeclares(t *testing.T) {
	transport := newFakeStream()
	mq := streamConn(t, transport)

	if err := patterns.DeclareStream(
		context.Background(), mq, "orders.log", patterns.StreamRetention{}); err != nil {
		t.Fatalf("DeclareStream: %v", err)
	}

	spec := transport.queues["orders.log"]
	for _, absent := range []string{"x-max-age", "x-max-length-bytes", "x-stream-max-segment-size-bytes"} {
		if _, present := spec.Args[absent]; present {
			t.Errorf("%s was declared for a stream that asked for no retention", absent)
		}
	}
	if spec.Args["x-queue-type"] != string(acemq.QueueStream) {
		t.Error("a stream with no retention was not declared as a stream")
	}
}

// TestMaxAgeIsRenderedInTheLargestExactUnit pins the rendering, because
// comparing arguments against a stream somebody declared by hand needs to know
// it: 24 hours is 1D and not 24h, and the two are different strings to a broker
// deciding whether a redeclaration matches.
func TestMaxAgeIsRenderedInTheLargestExactUnit(t *testing.T) {
	for _, c := range []struct {
		age  time.Duration
		want string
	}{
		{7 * 24 * time.Hour, "7D"},
		{24 * time.Hour, "1D"},
		{3 * time.Hour, "3h"},
		{90 * time.Minute, "90m"},
		{45 * time.Second, "45s"},
		{1500 * time.Millisecond, "1s"},
	} {
		transport := newFakeStream()
		mq := streamConn(t, transport)

		if err := patterns.DeclareStream(context.Background(), mq, "s",
			patterns.StreamRetention{MaxAge: c.age}); err != nil {
			t.Fatal(err)
		}
		if got := transport.queues["s"].Args["x-max-age"]; got != c.want {
			t.Errorf("%v rendered as %v, want %s", c.age, got, c.want)
		}
	}
}

// TestEveryStartingPositionReachesTheBrokerAsAnArgument is the offset contract.
//
// ReadStream always names an explicit x-stream-offset, including for the
// default: sending nothing and letting the broker decide is how a restarted
// reader silently starts somewhere the code never asked for.
func TestEveryStartingPositionReachesTheBrokerAsAnArgument(t *testing.T) {
	at := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)

	for _, c := range []struct {
		name   string
		offset patterns.StreamOffset
		want   any
	}{
		{"first", patterns.FromFirst(), "first"},
		{"next", patterns.FromNext(), "next"},
		{"last", patterns.FromLast(), "last"},
		{"exact", patterns.FromOffset(41337), int64(41337)},
		{"timestamp", patterns.FromTimestamp(at), at},
		{"unset means next", patterns.StreamOffset{}, "next"},
	} {
		transport := newFakeStream()
		mq := streamConn(t, transport)

		sub, err := patterns.ReadStream(context.Background(), mq, "orders.log",
			func(context.Context, acemq.Message[streamEvent]) acemq.Ack { return acemq.Accept() },
			patterns.StreamOptions{Offset: c.offset, Prefetch: 100})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		_ = sub.Close()

		if got := transport.consumed["orders.log"].Args["x-stream-offset"]; got != c.want {
			t.Errorf("%s reached the broker as %v (%T), want %v", c.name, got, got, c.want)
		}
	}
}

// TestAStreamConsumerAlwaysGetsAPrefetchAndCanBeNamed covers the two remaining
// consume arguments.
//
// RabbitMQ refuses a stream consumer without a prefetch and the error does not
// explain why, so ReadStream supplies one. The number it supplies is this
// library's own choice, not part of the cross-language contract — Java and .NET
// use 100 — and it is pinned here so that changing it is a deliberate act.
func TestAStreamConsumerAlwaysGetsAPrefetchAndCanBeNamed(t *testing.T) {
	for _, c := range []struct {
		name string
		opts patterns.StreamOptions
		want int
	}{
		{"unset falls back", patterns.StreamOptions{}, 10},
		{"zero falls back", patterns.StreamOptions{Prefetch: 0}, 10},
		{"negative falls back", patterns.StreamOptions{Prefetch: -5}, 10},
		{"set is honoured", patterns.StreamOptions{Prefetch: 250}, 250},
	} {
		transport := newFakeStream()
		mq := streamConn(t, transport)

		sub, err := patterns.ReadStream(context.Background(), mq, "orders.log",
			func(context.Context, acemq.Message[streamEvent]) acemq.Ack { return acemq.Accept() },
			c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		_ = sub.Close()

		if got := transport.consumed["orders.log"].Prefetch; got != c.want {
			t.Errorf("%s: prefetch is %d, want %d", c.name, got, c.want)
		}
	}

	transport := newFakeStream()
	mq := streamConn(t, transport)

	sub, err := patterns.ReadStream(context.Background(), mq, "orders.log",
		func(context.Context, acemq.Message[streamEvent]) acemq.Ack { return acemq.Accept() },
		patterns.StreamOptions{Name: "projection-a", Prefetch: 100})
	if err != nil {
		t.Fatal(err)
	}
	_ = sub.Close()

	if got := transport.consumed["orders.log"].Tag; got != "projection-a" {
		t.Errorf("the consumer tag is %q, want the name the caller gave", got)
	}
}

// TestReadStreamRefusesATransportWithoutStreams is the refusal the docs promise.
//
// Falling back to an ordinary queue would lose replay, retention and every
// consumer's independent position — the three reasons to want a stream — and
// would do it quietly. The in-memory transport says no, and this says the same
// no in a test that does not need one.
func TestReadStreamRefusesATransportWithoutStreams(t *testing.T) {
	mq, err := acemq.Connect(context.Background(), "memory://"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.Close() }()

	_, err = patterns.ReadStream(context.Background(), mq, "orders.log",
		func(context.Context, acemq.Message[streamEvent]) acemq.Ack { return acemq.Accept() },
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
	if err == nil {
		t.Fatal("the in-memory transport accepted a stream consumer")
	}
	for _, want := range []string{"does not support streams", "in-memory transport has no equivalent"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// TestDeclareStreamAgainstMemoryStillSucceeds is the asymmetry the page warns
// about, pinned so it cannot change by accident.
//
// The in-memory transport records queue arguments without interpreting them, so
// a test can build the whole topology and only discover the gap when it tries to
// read. That is worth knowing about before it happens rather than after.
func TestDeclareStreamAgainstMemoryStillSucceeds(t *testing.T) {
	mq, err := acemq.Connect(context.Background(), "memory://"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.Close() }()

	if err := patterns.DeclareStream(context.Background(), mq, "orders.log",
		patterns.StreamRetention{MaxAge: time.Hour}); err != nil {
		t.Errorf("DeclareStream against memory:// failed: %v", err)
	}
}

// TestARetryFromAStreamHandlerLandsInTheParkedQueue is the refusal end to end.
//
// The unit test next door proves the Ack is rewritten. This proves what the
// engine then does with it: the message is republished to {stream}.parked with
// the reason on its envelope, and nothing at all goes back to the stream.
func TestARetryFromAStreamHandlerLandsInTheParkedQueue(t *testing.T) {
	transport := newFakeStream()
	mq := streamConn(t, transport)

	handled := make(chan struct{}, 1)
	sub, err := patterns.ReadStream(context.Background(), mq, "orders.log",
		func(context.Context, acemq.Message[streamEvent]) acemq.Ack {
			defer func() { handled <- struct{}{} }()
			return acemq.Retry(errors.New("the projection store is down"))
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	transport.hand("order-1", int64(41337), streamEvent{OrderID: "order-1"})
	<-handled

	// The handler returning and the engine having settled are two different
	// moments, and only the second one publishes anything.
	waitFor(t, "the message to reach orders.log.parked", func() bool {
		return len(transport.sentTo("orders.log.parked")) == 1
	})
	parked := transport.sentTo("orders.log.parked")

	reason, _ := parked[0].headers[acemq.HeaderError].(string)
	for _, want := range []string{"acemq.Retry", "acemq.Park", "x-stream-offset", "offset 41337"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the parked message's reason does not mention %q:\n%s", want, reason)
		}
	}

	// The point of the whole refusal: nothing went back to the log.
	if back := transport.sentTo("orders.log"); len(back) != 0 {
		t.Errorf("%d copies of the message were appended to the stream", len(back))
	}
	if dlq := transport.sentTo("orders.log.dlq"); len(dlq) != 0 {
		t.Errorf("%d messages reached the dead-letter queue, which a retry is not", len(dlq))
	}
}

// TestAcceptingOnAStreamPublishesNothingAnywhere is what the ordinary path has
// to look like, and it is worth asserting rather than assuming: on a stream an
// acknowledgement moves this consumer's position and nothing else, so a handler
// that succeeded must leave no trace on the broker at all.
func TestAcceptingOnAStreamPublishesNothingAnywhere(t *testing.T) {
	transport := newFakeStream()
	mq := streamConn(t, transport)

	var seen []uint64
	handled := make(chan struct{}, 3)

	sub, err := patterns.ReadStream(context.Background(), mq, "orders.log",
		func(_ context.Context, m acemq.Message[streamEvent]) acemq.Ack {
			// The checkpoint path, as the page describes it: read the offset off
			// the envelope and record it before settling.
			if offset, ok := patterns.StreamOffsetOf(m.Envelope); ok {
				seen = append(seen, offset)
			}
			handled <- struct{}{}
			return acemq.Accept()
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	for i, id := range []string{"a", "b", "c"} {
		transport.hand(id, int64(100+i), streamEvent{OrderID: id})
		<-handled
	}

	// Nothing is published on the happy path, and a moment is given for
	// anything that would have been.
	time.Sleep(50 * time.Millisecond)
	transport.mu.Lock()
	published := len(transport.published)
	transport.mu.Unlock()
	if published != 0 {
		t.Errorf("%d messages were published while three deliveries were accepted", published)
	}

	want := []uint64{100, 101, 102}
	if len(seen) != len(want) {
		t.Fatalf("the handler recorded %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("the handler recorded %v, want %v", seen, want)
		}
	}
}

// TestRejectingOnAStreamCopiesRatherThanMoves is the difference from a queue
// that the page spends a section on.
//
// A stream carries no x-dead-letter-exchange and nothing can be removed from it,
// so this library republishes to {stream}.dlq and acknowledges the original —
// which on a stream means advancing past it. The message lands somewhere an
// operator can find it **and stays in the stream**, so anything that replays the
// stream reads it again.
func TestRejectingOnAStreamCopiesRatherThanMoves(t *testing.T) {
	transport := newFakeStream()
	mq := streamConn(t, transport)

	handled := make(chan struct{}, 1)
	sub, err := patterns.ReadStream(context.Background(), mq, "orders.log",
		func(context.Context, acemq.Message[streamEvent]) acemq.Ack {
			defer func() { handled <- struct{}{} }()
			return acemq.Reject(errors.New("the order id is not a uuid"))
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	transport.hand("order-2", int64(7), streamEvent{OrderID: "order-2"})
	<-handled

	waitFor(t, "the message to reach orders.log.dlq", func() bool {
		return len(transport.sentTo("orders.log.dlq")) == 1
	})
	dlq := transport.sentTo("orders.log.dlq")
	if reason, _ := dlq[0].headers[acemq.HeaderError].(string); !strings.Contains(reason, "not a uuid") {
		t.Errorf("the dead letter's reason is %q", reason)
	}
	if back := transport.sentTo("orders.log"); len(back) != 0 {
		t.Errorf("rejecting put %d copies back into the stream", len(back))
	}
}
