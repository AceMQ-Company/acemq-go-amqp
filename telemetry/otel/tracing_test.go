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

package otel_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	tracing "github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// OrderPlaced is the message these tests send.
type OrderPlaced struct {
	OrderID string `json:"order_id"`
}

// recorder is a real SDK writing into memory, so every assertion below lands on
// a span that was actually exported rather than on a fake that agreed with the
// adapter by construction.
type recorder struct {
	exporter *tracetest.InMemoryExporter
	tracing  *tracing.Tracing
}

func record(t *testing.T, opts ...tracing.Option) *recorder {
	t.Helper()

	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	all := append([]tracing.Option{tracing.WithTracerProvider(provider)}, opts...)
	return &recorder{exporter: exporter, tracing: tracing.New(all...)}
}

// spans are the ones exported so far, in the order they ended.
func (r *recorder) spans() tracetest.SpanStubs { return r.exporter.GetSpans() }

func (r *recorder) named(t *testing.T, name string) tracetest.SpanStub {
	t.Helper()
	for _, span := range r.spans() {
		if span.Name == name {
			return span
		}
	}
	var names []string
	for _, span := range r.spans() {
		names = append(names, span.Name)
	}
	t.Fatalf("no span named %q; there are %v", name, names)
	return tracetest.SpanStub{}
}

func attr(t *testing.T, span tracetest.SpanStub, key string) attribute.Value {
	t.Helper()
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	t.Fatalf("span %q has no %s attribute; it has %v", span.Name, key, span.Attributes)
	return attribute.Value{}
}

func brokerFor(t *testing.T, opts ...acemq.ConnOption) *acemq.Conn {
	t.Helper()
	mq, err := acemq.Connect(context.Background(), "memory://"+t.Name(), opts...)
	if err != nil {
		t.Fatalf("cannot connect: %v", err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	if err := mq.DeclareQueue(context.Background(), "orders"); err != nil {
		t.Fatalf("cannot declare orders: %v", err)
	}
	return mq
}

func TestAPublishSpanIsNamedAfterItsDestinationAndIsAProducer(t *testing.T) {
	r := record(t)
	mq := brokerFor(t, acemq.WithPublishInterceptor(r.tracing.PublishInterceptor()))

	orders := tracing.NewPublisher[OrderPlaced](r.tracing, mq, "", "orders")
	if err := orders.Send(context.Background(), OrderPlaced{OrderID: "1"}); err != nil {
		t.Fatal(err)
	}

	span := r.named(t, "orders publish")
	if span.SpanKind != trace.SpanKindProducer {
		t.Errorf("kind = %v, want producer", span.SpanKind)
	}
	if got := attr(t, span, tracing.AttrSystem).AsString(); got != "rabbitmq" {
		t.Errorf("%s = %q, want rabbitmq", tracing.AttrSystem, got)
	}
	if got := attr(t, span, tracing.AttrDestination).AsString(); got != "orders" {
		t.Errorf("%s = %q, want orders", tracing.AttrDestination, got)
	}
	if got := attr(t, span, tracing.AttrOperation).AsString(); got != "publish" {
		t.Errorf("%s = %q, want publish", tracing.AttrOperation, got)
	}
	if got := attr(t, span, tracing.AttrRoutingKey).AsString(); got != "orders" {
		t.Errorf("%s = %q, want orders", tracing.AttrRoutingKey, got)
	}
	// Filled in by the interceptor, because the envelope does not exist when
	// the span is opened.
	if got := attr(t, span, tracing.AttrMessageID).AsString(); got == "" {
		t.Error("the publish span carries no message id")
	}
	if got := attr(t, span, tracing.AttrMessageType).AsString(); got != "orders" {
		t.Errorf("%s = %q, want orders", tracing.AttrMessageType, got)
	}
	// Confirmed rather than published: the in-memory transport takes
	// responsibility for the message, the same as a broker with publisher
	// confirms on. Without them the outcome is published, which promises less.
	if got := attr(t, span, tracing.AttrOutcome).AsString(); got != tracing.OutcomeConfirmed {
		t.Errorf("%s = %q, want %q", tracing.AttrOutcome, got, tracing.OutcomeConfirmed)
	}
	if span.Status.Code != codes.Unset {
		t.Errorf("status = %v, want unset on a publish that worked", span.Status.Code)
	}
}

// A publish writes the W3C names, and only those. An x-acemq- prefixed header
// would be invisible to every other tracing tool on the network.
func TestThePublishWritesTheW3CHeadersOntoTheMessage(t *testing.T) {
	r := record(t)

	var seen map[string]any
	capture := func(_ context.Context, c *acemq.PublishContext) error {
		seen = map[string]any{}
		for name, value := range c.Envelope.Headers {
			seen[name] = value
		}
		return nil
	}

	mq := brokerFor(t,
		acemq.WithPublishInterceptor(r.tracing.PublishInterceptor()),
		acemq.WithPublishInterceptor(capture))

	orders := tracing.NewPublisher[OrderPlaced](r.tracing, mq, "", "orders")
	if err := orders.Send(context.Background(), OrderPlaced{OrderID: "1"}); err != nil {
		t.Fatal(err)
	}

	span := r.named(t, "orders publish")
	traceparent, ok := seen[acemq.HeaderTraceParent].(string)
	if !ok {
		t.Fatalf("the message carries no %s; it has %v", acemq.HeaderTraceParent, seen)
	}
	if want := span.SpanContext.TraceID().String(); !strings.Contains(traceparent, want) {
		t.Errorf("traceparent %q does not name the publish trace %s", traceparent, want)
	}
	if want := span.SpanContext.SpanID().String(); !strings.Contains(traceparent, want) {
		t.Errorf("traceparent %q does not name the publish span %s", traceparent, want)
	}
	if _, prefixed := seen["x-acemq-traceparent"]; prefixed {
		t.Error("the trace context was written under the reserved prefix")
	}
}

// The join is the whole point: a handler's span belongs to the trace of the
// publish that caused it, which happened in another process.
func TestAConsumerSpanIsAChildOfThePublishThatCausedIt(t *testing.T) {
	ctx := context.Background()
	r := record(t)
	mq := brokerFor(t, acemq.WithPublishInterceptor(r.tracing.PublishInterceptor()))

	arrived := make(chan struct{}, 1)
	handle := func(_ context.Context, _ acemq.Message[OrderPlaced]) acemq.Ack {
		arrived <- struct{}{}
		return acemq.Accept()
	}

	sub, err := acemq.Consume(ctx, mq, "orders", tracing.Handle(r.tracing, "orders", handle))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	orders := tracing.NewPublisher[OrderPlaced](r.tracing, mq, "", "orders")
	if err := orders.Send(ctx, OrderPlaced{OrderID: "1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("the message did not arrive")
	}
	waitForSpans(t, r, 2)

	published := r.named(t, "orders publish")
	processed := r.named(t, "orders process")

	if processed.SpanKind != trace.SpanKindConsumer {
		t.Errorf("kind = %v, want consumer", processed.SpanKind)
	}
	if published.SpanContext.TraceID() != processed.SpanContext.TraceID() {
		t.Errorf("the delivery is in trace %s and the publish in %s; they are not joined",
			processed.SpanContext.TraceID(), published.SpanContext.TraceID())
	}
	if processed.Parent.SpanID() != published.SpanContext.SpanID() {
		t.Errorf("the delivery's parent is %s, want the publish %s",
			processed.Parent.SpanID(), published.SpanContext.SpanID())
	}
	if got := attr(t, processed, tracing.AttrOperation).AsString(); got != "process" {
		t.Errorf("%s = %q, want process", tracing.AttrOperation, got)
	}
	if got := attr(t, processed, tracing.AttrAttempt).AsInt64(); got != 1 {
		t.Errorf("%s = %d, want 1", tracing.AttrAttempt, got)
	}
	if got := attr(t, processed, tracing.AttrOutcome).AsString(); got != tracing.OutcomeAcked {
		t.Errorf("%s = %q, want acked", tracing.AttrOutcome, got)
	}
}

// The decisive one. The message carries one trace and the goroutine handling it
// is inside another, which is what happens when a consumer loop is itself
// traced or when a handler runs under some unrelated ambient span.
//
// The parent has to be the one in the headers. Reading the ambient context
// instead produces a trace that looks plausible — spans, parents, everything —
// and joins nothing across the process boundary, which is the only thing this
// adapter is for.
func TestTheParentComesFromTheHeadersAndNotFromAmbientContext(t *testing.T) {
	r := record(t)

	// The publisher's trace, minutes ago and somewhere else. Only its headers
	// survive.
	publisherCtx, publisherSpan := r.tracing.Tracer().Start(context.Background(), "somewhere else")
	headers := map[string]any{}
	for name, value := range r.tracing.PropagationHeaders(publisherCtx) {
		headers[name] = value
	}
	publisherSpan.End()
	if _, ok := headers[acemq.HeaderTraceParent]; !ok {
		t.Fatalf("nothing to extract: the carrier is %v", headers)
	}

	// A live, unrelated span, current on the goroutine that is about to handle
	// the delivery. Every part of this is a plausible parent and none of it is
	// the right one.
	ambientCtx, ambientSpan := r.tracing.Tracer().Start(context.Background(), "the wrong parent")
	defer ambientSpan.End()

	handler := tracing.Handle(r.tracing, "orders",
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack { return acemq.Accept() })
	handler(ambientCtx, acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1", Type: "order.placed", Attempt: 1, Headers: headers},
	})

	processed := r.named(t, "orders process")
	if got, want := processed.SpanContext.TraceID(), publisherSpan.SpanContext().TraceID(); got != want {
		t.Errorf("the delivery is in trace %s, want the publisher's %s", got, want)
	}
	if got := processed.SpanContext.TraceID(); got == ambientSpan.SpanContext().TraceID() {
		t.Errorf("the delivery joined the ambient trace %s instead of the message's", got)
	}
	if got, want := processed.Parent.SpanID(), publisherSpan.SpanContext().SpanID(); got != want {
		t.Errorf("the delivery's parent is %s, want the publisher's %s", got, want)
	}
	if got := processed.Parent.SpanID(); got == ambientSpan.SpanContext().SpanID() {
		t.Error("the delivery's parent is the ambient span")
	}
}

// A traceparent off the wire is not always a Go string: amqp091 hands over
// long-string header values as bytes, and a header from another client can be
// anything the encoder chose.
func TestATraceparentThatArrivedAsBytesStillJoins(t *testing.T) {
	r := record(t)

	publisherCtx, publisherSpan := r.tracing.Tracer().Start(context.Background(), "somewhere else")
	headers := map[string]any{}
	for name, value := range r.tracing.PropagationHeaders(publisherCtx) {
		headers[name] = []byte(value)
	}
	publisherSpan.End()

	handler := tracing.Handle(r.tracing, "orders",
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack { return acemq.Accept() })
	handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1", Attempt: 1, Headers: headers},
	})

	processed := r.named(t, "orders process")
	if got, want := processed.SpanContext.TraceID(), publisherSpan.SpanContext().TraceID(); got != want {
		t.Errorf("the delivery is in trace %s, want %s", got, want)
	}
}

// A delivery whose headers carry nothing starts a trace of its own rather than
// being dropped or attached to whatever was lying around.
func TestADeliveryWithNoTraceContextStartsItsOwnTrace(t *testing.T) {
	r := record(t)

	handler := tracing.Handle(r.tracing, "orders",
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack { return acemq.Accept() })
	handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1", Attempt: 1},
	})

	processed := r.named(t, "orders process")
	if processed.Parent.IsValid() {
		t.Errorf("the delivery has parent %s, want none", processed.Parent.SpanID())
	}
	if !processed.SpanContext.TraceID().IsValid() {
		t.Error("the delivery is not in a trace at all")
	}
}

func TestWhichOutcomesAreErrors(t *testing.T) {
	failing := []string{
		tracing.OutcomeUnroutable,
		tracing.OutcomeFailed,
		tracing.OutcomeDeadLettered,
	}
	fine := []string{
		tracing.OutcomeAcked,
		tracing.OutcomeRetried,
		tracing.OutcomeRejected,
		tracing.OutcomeConfirmed,
		tracing.OutcomePublished,
		tracing.OutcomeAnswered,
		tracing.OutcomeTimedOut,
	}

	for _, outcome := range failing {
		t.Run(outcome, func(t *testing.T) {
			r := record(t)
			_, span := r.tracing.StartPublish(context.Background(), "", "orders")
			span.Outcome(outcome)
			span.End()

			got := r.named(t, "orders publish")
			if got.Status.Code != codes.Error {
				t.Errorf("status = %v, want error for %q", got.Status.Code, outcome)
			}
			if got.Status.Description != outcome {
				t.Errorf("description = %q, want %q", got.Status.Description, outcome)
			}
		})
	}

	for _, outcome := range fine {
		t.Run(outcome, func(t *testing.T) {
			r := record(t)
			_, span := r.tracing.StartPublish(context.Background(), "", "orders")
			span.Outcome(outcome)
			span.End()

			got := r.named(t, "orders publish")
			if got.Status.Code != codes.Unset {
				t.Errorf("status = %v, want unset for %q", got.Status.Code, outcome)
			}
			if value := attr(t, got, tracing.AttrOutcome).AsString(); value != outcome {
				t.Errorf("%s = %q, want %q", tracing.AttrOutcome, value, outcome)
			}
		})
	}
}

func TestAHandlerThatRetriesIsNotAFailure(t *testing.T) {
	r := record(t)

	handler := tracing.Handle(r.tracing, "orders",
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack {
			return acemq.Retry(errors.New("the pricing service is down"))
		})
	handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1", Attempt: 2},
	})

	span := r.named(t, "orders process")
	if got := attr(t, span, tracing.AttrOutcome).AsString(); got != tracing.OutcomeRetried {
		t.Errorf("%s = %q, want retried", tracing.AttrOutcome, got)
	}
	if span.Status.Code != codes.Unset {
		t.Errorf("status = %v, want unset: a retry is the system working", span.Status.Code)
	}
	// The reason is still carried, as an event rather than a failure.
	if !hasEvent(span, "exception") {
		t.Error("the retry reason was not recorded on the span")
	}
}

func TestAHandlerThatRejectsIsNotAFailureEither(t *testing.T) {
	r := record(t)

	handler := tracing.Handle(r.tracing, "orders",
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack {
			return acemq.Reject(errors.New("no such customer"))
		})
	handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1", Attempt: 1},
	})

	span := r.named(t, "orders process")
	if got := attr(t, span, tracing.AttrOutcome).AsString(); got != tracing.OutcomeRejected {
		t.Errorf("%s = %q, want rejected", tracing.AttrOutcome, got)
	}
	if span.Status.Code != codes.Unset {
		t.Errorf("status = %v, want unset: the handler decided", span.Status.Code)
	}
}

func TestAHandlerThatPanicsFailsTheSpanAndKeepsPanicking(t *testing.T) {
	r := record(t)

	handler := tracing.Handle(r.tracing, "orders",
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack {
			panic("nil map")
		})

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic was swallowed")
			}
		}()
		handler(context.Background(), acemq.Message[OrderPlaced]{
			Envelope: acemq.Envelope{ID: "m-1", Attempt: 1},
		})
	}()

	span := r.named(t, "orders process")
	if got := attr(t, span, tracing.AttrOutcome).AsString(); got != tracing.OutcomeFailed {
		t.Errorf("%s = %q, want failed", tracing.AttrOutcome, got)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("status = %v, want error", span.Status.Code)
	}
}

func TestAnUnroutableMessageIsAnError(t *testing.T) {
	r := record(t)
	mq := brokerFor(t, acemq.WithPublishInterceptor(r.tracing.PublishInterceptor()))

	nowhere := tracing.NewPublisher[OrderPlaced](r.tracing, mq, "", "nobody.is.listening",
		acemq.Mandatory[OrderPlaced]())
	if err := nowhere.Send(context.Background(), OrderPlaced{OrderID: "1"}); err == nil {
		t.Fatal("publishing to nothing succeeded")
	}

	span := r.named(t, "nobody.is.listening publish")
	if got := attr(t, span, tracing.AttrOutcome).AsString(); got != tracing.OutcomeUnroutable {
		t.Errorf("%s = %q, want unroutable", tracing.AttrOutcome, got)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("status = %v, want error", span.Status.Code)
	}
}

func TestARequestSpanIsAClientBecauseItWaits(t *testing.T) {
	r := record(t)

	_, span := r.tracing.StartRequest(context.Background(), "pricing")
	span.Outcome(tracing.OutcomeAnswered)
	span.End()

	got := r.named(t, "pricing request")
	if got.SpanKind != trace.SpanKindClient {
		t.Errorf("kind = %v, want client: this span waits for an answer", got.SpanKind)
	}
	if value := attr(t, got, tracing.AttrOperation).AsString(); value != "request" {
		t.Errorf("%s = %q, want request", tracing.AttrOperation, value)
	}
}

// asker stands in for a patterns.Requester, so a request can be traced without
// a broker and a responder.
type asker struct {
	answer string
	err    error
}

func (a asker) Do(_ context.Context, _ string, _ ...acemq.EnvelopeOption) (string, error) {
	return a.answer, a.err
}

func TestAskRecordsWhetherTheAnswerCame(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		outcome string
		status  codes.Code
	}{
		{"answered", nil, tracing.OutcomeAnswered, codes.Unset},
		// The work may well have been done. A timeout is the absence of an
		// answer, not evidence that nothing happened, so it is not an error.
		{"timed out", patterns.ErrRequestTimedOut, tracing.OutcomeTimedOut, codes.Unset},
		{"failed", errors.New("the responder blew up"), tracing.OutcomeFailed, codes.Error},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := record(t)

			_, err := tracing.Ask[string, string](context.Background(), r.tracing, "pricing",
				asker{answer: "42", err: c.err}, "how much")
			if !errors.Is(err, c.err) {
				t.Fatalf("err = %v, want %v", err, c.err)
			}

			span := r.named(t, "pricing request")
			if got := attr(t, span, tracing.AttrOutcome).AsString(); got != c.outcome {
				t.Errorf("%s = %q, want %q", tracing.AttrOutcome, got, c.outcome)
			}
			if span.Status.Code != c.status {
				t.Errorf("status = %v, want %v", span.Status.Code, c.status)
			}
		})
	}
}

// Four things that are events on the span already open, not spans of their own.
func TestTheEventsAreEventsAndNotSpans(t *testing.T) {
	r := record(t)

	ctx, span := r.tracing.StartConsume(context.Background(), "orders",
		acemq.Envelope{ID: "m-1", Attempt: 3})

	env := acemq.Envelope{ID: "m-1", Attempt: 3}
	r.tracing.MessageRetried(ctx, "orders", env, 30*time.Second)
	r.tracing.MessageDeadLettered(ctx, "orders", env, "out of attempts")
	r.tracing.OutboxPublishFailed(ctx, "orders", "the broker is down")
	r.tracing.PipelineRunFinished(ctx, "checkout", "charge", "ended_early", time.Minute)
	span.End()

	if got := len(r.spans()); got != 1 {
		t.Fatalf("%d spans were emitted, want 1: these are events, not spans", got)
	}
	got := r.named(t, "orders process")
	for _, name := range []string{
		tracing.EventMessageRetried,
		tracing.EventMessageDeadLettered,
		tracing.EventOutboxPublishFailed,
		tracing.EventPipelineRunFinished,
	} {
		if !hasEvent(got, name) {
			t.Errorf("no %s event on the span", name)
		}
	}

	retried := eventNamed(t, got, tracing.EventMessageRetried)
	if value := eventAttr(t, retried, tracing.AttrRetryDelayMs); value.AsInt64() != 30000 {
		t.Errorf("%s = %d, want 30000", tracing.AttrRetryDelayMs, value.AsInt64())
	}
	if value := eventAttr(t, retried, tracing.AttrAttempt); value.AsInt64() != 3 {
		t.Errorf("%s = %d, want 3", tracing.AttrAttempt, value.AsInt64())
	}

	// Bare pipeline and step, which is what the Java and Ruby adapters write.
	finished := eventNamed(t, got, tracing.EventPipelineRunFinished)
	if value := eventAttr(t, finished, tracing.AttrPipeline); value.AsString() != "checkout" {
		t.Errorf("%s = %q, want checkout", tracing.AttrPipeline, value.AsString())
	}
	if value := eventAttr(t, finished, tracing.AttrEventOutcome); value.AsString() != "ended_early" {
		t.Errorf("%s = %q, want ended_early", tracing.AttrEventOutcome, value.AsString())
	}
}

// An event with nowhere to go is dropped rather than opening a span for itself.
func TestAnEventWithNoSpanOpenIsDropped(t *testing.T) {
	r := record(t)

	r.tracing.MessageDeadLettered(context.Background(), "orders",
		acemq.Envelope{ID: "m-1", Attempt: 5}, "out of attempts")

	if got := len(r.spans()); got != 0 {
		t.Errorf("%d spans were emitted, want none", got)
	}
}

func TestPropagationHeadersInjectTheCurrentContext(t *testing.T) {
	r := record(t)

	ctx, span := r.tracing.Tracer().Start(context.Background(), "anything")
	defer span.End()

	carrier := r.tracing.PropagationHeaders(ctx)
	traceparent, ok := carrier[acemq.HeaderTraceParent]
	if !ok {
		t.Fatalf("the carrier is %v", carrier)
	}
	if !strings.Contains(traceparent, span.SpanContext().TraceID().String()) {
		t.Errorf("traceparent %q does not name trace %s",
			traceparent, span.SpanContext().TraceID())
	}
	// A fresh carrier every time: injecting into one somebody else owns is how
	// a stale traceparent ends up on a message.
	carrier[acemq.HeaderTraceParent] = "tampered"
	if again := r.tracing.PropagationHeaders(ctx); again[acemq.HeaderTraceParent] == "tampered" {
		t.Error("the carrier is shared between calls")
	}
}

func TestPropagationHeadersAreEmptyWhenNothingIsTraced(t *testing.T) {
	r := record(t)

	if got := r.tracing.PropagationHeaders(context.Background()); len(got) != 0 {
		t.Errorf("carrier = %v, want empty when there is no span", got)
	}
}

func TestInjectPutsTheTraceOnAnEnvelopeBuiltByHand(t *testing.T) {
	r := record(t)

	ctx, span := r.tracing.Tracer().Start(context.Background(), "anything")
	defer span.End()

	env := acemq.Envelope{ID: "m-1"}
	r.tracing.Inject(ctx, &env)

	if _, ok := env.Headers[acemq.HeaderTraceParent]; !ok {
		t.Fatalf("the envelope carries %v", env.Headers)
	}
	extracted := trace.SpanContextFromContext(r.tracing.Extract(context.Background(), env.Headers))
	if extracted.TraceID() != span.SpanContext().TraceID() {
		t.Errorf("extracted trace %s, want %s", extracted.TraceID(), span.SpanContext().TraceID())
	}
}

// The propagator is the process's when it has one, and W3C when it has not,
// because a no-op propagator writes no traceparent and this contract is two
// header names.
func TestTheDefaultPropagatorWritesW3C(t *testing.T) {
	r := record(t)

	ctx, span := r.tracing.Tracer().Start(context.Background(), "anything")
	defer span.End()

	carrier := r.tracing.PropagationHeaders(ctx)
	if _, ok := carrier[acemq.HeaderTraceParent]; !ok {
		t.Errorf("carrier = %v, want a traceparent", carrier)
	}
}

func TestAPropagatorCanBeReplaced(t *testing.T) {
	r := record(t, tracing.WithPropagator(propagation.Baggage{}))

	ctx, span := r.tracing.Tracer().Start(context.Background(), "anything")
	defer span.End()

	if _, ok := r.tracing.PropagationHeaders(ctx)[acemq.HeaderTraceParent]; ok {
		t.Error("a traceparent was written by a propagator that does not write one")
	}
}

func TestTheSystemAttributeCanBeChanged(t *testing.T) {
	r := record(t, tracing.WithSystem("amqp"))

	_, span := r.tracing.StartPublish(context.Background(), "", "orders")
	span.End()

	if got := attr(t, r.named(t, "orders publish"), tracing.AttrSystem).AsString(); got != "amqp" {
		t.Errorf("%s = %q, want amqp", tracing.AttrSystem, got)
	}
}

func TestEndingASpanTwiceEndsItOnce(t *testing.T) {
	r := record(t)

	_, span := r.tracing.StartPublish(context.Background(), "", "orders")
	span.End()
	span.End()

	if got := len(r.spans()); got != 1 {
		t.Errorf("%d spans were exported, want 1", got)
	}
}

// The interceptor must not write messaging attributes onto a span that is not
// about a message — an HTTP server's, say, which is very often what is current
// when something publishes.
func TestTheInterceptorLeavesSomebodyElsesSpanAlone(t *testing.T) {
	r := record(t)
	mq := brokerFor(t, acemq.WithPublishInterceptor(r.tracing.PublishInterceptor()))

	ctx, server := r.tracing.Tracer().Start(context.Background(), "POST /orders")
	plain := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
	if err := plain.Send(ctx, OrderPlaced{OrderID: "1"}); err != nil {
		t.Fatal(err)
	}
	server.End()

	got := r.named(t, "POST /orders")
	for _, kv := range got.Attributes {
		if string(kv.Key) == tracing.AttrMessageID {
			t.Errorf("the interceptor wrote %s onto a span that is not a publish", kv.Key)
		}
	}
}

// Publishing without a traced publisher still carries the trace: the
// interceptor is what writes the header, and it runs on every publish.
func TestAPlainPublisherStillCarriesTheTrace(t *testing.T) {
	r := record(t)

	var seen map[string]any
	capture := func(_ context.Context, c *acemq.PublishContext) error {
		seen = c.Envelope.Headers
		return nil
	}
	mq := brokerFor(t,
		acemq.WithPublishInterceptor(r.tracing.PublishInterceptor()),
		acemq.WithPublishInterceptor(capture))

	ctx, server := r.tracing.Tracer().Start(context.Background(), "POST /orders")
	defer server.End()

	plain := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
	if err := plain.Send(ctx, OrderPlaced{OrderID: "1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := seen[acemq.HeaderTraceParent]; !ok {
		t.Errorf("the message carries %v, want a traceparent", seen)
	}
}

// Nothing is emitted and nothing breaks when the application has configured no
// SDK, which is what a library that traces by default has to be able to say.
func TestNothingIsEmittedWithoutATracerProvider(t *testing.T) {
	quiet := tracing.New(tracing.WithTracerProvider(noop.NewTracerProvider()))

	ctx, span := quiet.StartPublish(context.Background(), "", "orders")
	span.Outcome(tracing.OutcomeFailed)
	span.Failed(errors.New("nothing to record on"))
	quiet.MessageRetried(ctx, "orders", acemq.Envelope{ID: "m-1"}, time.Second)
	span.End()

	if got := quiet.PropagationHeaders(ctx); len(got) != 0 {
		t.Errorf("carrier = %v, want empty: there is nothing to propagate", got)
	}
}

func hasEvent(span tracetest.SpanStub, name string) bool {
	for _, event := range span.Events {
		if event.Name == name {
			return true
		}
	}
	return false
}

func eventNamed(t *testing.T, span tracetest.SpanStub, name string) sdktrace.Event {
	t.Helper()
	for _, event := range span.Events {
		if event.Name == name {
			return event
		}
	}
	t.Fatalf("no %s event on %q", name, span.Name)
	return sdktrace.Event{}
}

func eventAttr(t *testing.T, event sdktrace.Event, key string) attribute.Value {
	t.Helper()
	for _, kv := range event.Attributes {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	t.Fatalf("event %q has no %s; it has %v", event.Name, key, event.Attributes)
	return attribute.Value{}
}

// waitForSpans gives the exporter a bounded time to see everything, because a
// handler's span ends on the consumer's goroutine.
func waitForSpans(t *testing.T, r *recorder, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(r.spans()) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("only %d spans were exported, want %d", len(r.spans()), want)
}
