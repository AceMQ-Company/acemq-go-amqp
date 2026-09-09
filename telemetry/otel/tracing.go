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

// Package otel emits OpenTelemetry spans for publishes and deliveries, and
// joins them up across processes.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
//
//	tracing := otel.New()
//
//	mq, err := acemq.Connect(ctx, url,
//		acemq.WithPublishInterceptor(tracing.PublishInterceptor()))
//
//	orders := otel.NewPublisher[OrderPlaced](tracing, mq, "", "orders")
//	err = orders.Send(ctx, order)
//
//	_, err = acemq.Consume(ctx, mq, "orders",
//		otel.Handle(tracing, "orders", handle))
//
// [acemq.Observer] answers how much; this answers what happened to this
// message. They are different questions and neither substitutes for the other:
// a counter says a thousand messages were dead-lettered, and a trace says this
// one was published by the checkout service, retried twice over four minutes
// and given up on — which is the question somebody is actually holding when
// they open a dashboard.
//
// # The join across processes is the point
//
// A consumer's span is a child of the publish that caused it, taken from the
// message's own headers rather than from whatever context happened to be
// current when the delivery arrived. Those are different traces, minutes and
// machines apart, and joining them is the one thing a messaging system needs
// from tracing that an HTTP client does not.
//
// # The names on the wire
//
// traceparent and tracestate, which are the W3C names and are deliberately not
// x-acemq- prefixed: other tooling already knows them, and renaming them would
// make this library's traces invisible to everything that did not know to look.
// The Java, .NET, Python and Ruby libraries write the same two, so a Go
// consumer joins a Java producer's trace without either side being configured
// for the other. They are [acemq.HeaderTraceParent] and [acemq.HeaderTraceState].
//
// # Span names, kinds and attributes
//
// Shared with the Java adapter, which follows the OpenTelemetry messaging
// conventions:
//
//	<destination> publish   PRODUCER
//	<queue> process         CONSUMER
//	<destination> request   CLIENT
//
// CLIENT for a request rather than PRODUCER because that span waits for an
// answer. A reader who cannot tell the two apart cannot tell a slow broker from
// a slow responder.
//
// # Events rather than spans
//
// A retry, a dead letter, an outbox failure and a finished pipeline run are
// events on the span that is already open. A zero-length span at the end of a
// trace adds a row and no information.
//
// # A module of its own
//
// go.opentelemetry.io/otel is required by this module and by nothing else, the
// same way the codec modules keep their formats out of the core. A process that
// publishes messages and traces nothing depends on nothing new.
package otel

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentationName is what the tracer is registered under, in every AceMQ
// library. A dashboard filtering on it sees the Java, .NET, Python, Ruby and Go
// services together.
const InstrumentationName = "org.acemq.amqp"

// DefaultSystem is the messaging.system attribute unless [WithSystem] says
// otherwise, and the same value the other libraries write.
const DefaultSystem = "rabbitmq"

// The span name suffixes, appended to the destination with a space:
// orders publish, orders.new process, pricing request.
const (
	SpanPublishSuffix = " publish"
	SpanProcessSuffix = " process"
	SpanRequestSuffix = " request"
)

// The attribute names.
//
// The OpenTelemetry semantic conventions for messaging, plus three of this
// library's own under an acemq namespace — attempt, message type and outcome
// have no standard names and are the three things most often wanted.
const (
	AttrSystem         = "messaging.system"
	AttrDestination    = "messaging.destination.name"
	AttrOperation      = "messaging.operation"
	AttrMessageID      = "messaging.message.id"
	AttrConversationID = "messaging.message.conversation_id"
	AttrRoutingKey     = "messaging.rabbitmq.destination.routing_key"
	AttrMessageType    = "messaging.acemq.message_type"
	AttrAttempt        = "messaging.acemq.attempt"
	AttrOutcome        = "messaging.acemq.outcome"
	AttrReason         = "messaging.acemq.reason"
	AttrRetryDelayMs   = "messaging.acemq.retry_delay_ms"
	AttrRunAgeMs       = "messaging.acemq.run_age_ms"
	AttrOutboxLagMs    = "messaging.acemq.outbox_lag_ms"
)

// The attribute names on a pipeline.run_finished event.
//
// Bare rather than namespaced, which is what the Java and Ruby adapters write.
// The Python one namespaces them; that is a difference between the libraries
// and not a decision this one gets to take on its own.
const (
	AttrPipeline     = "pipeline"
	AttrStep         = "step"
	AttrEventOutcome = "outcome"
)

// The outcomes, which are the values of the messaging.acemq.outcome attribute
// and are shared with the metric tag of the same name in every AceMQ library.
//
// The consume outcomes are taken from the core package rather than written out
// again here. The engine decides them, tags [acemq.MetricConsumed] with them and
// hands them to this adapter on the [acemq.Settlement]; a second copy of the
// spellings would be a second thing to keep in step, and a counter and a span
// that disagree about one delivery is the bug this arrangement exists to
// prevent.
const (
	// OutcomeConfirmed is the broker taking responsibility for the message.
	OutcomeConfirmed = "confirmed"

	// OutcomePublished is a message that went out with nothing promised about
	// it. Publisher confirms were not on.
	OutcomePublished = acemq.OutcomePublished

	// OutcomeUnroutable is a message that reached no queue at all, which the
	// broker does not consider an error and which is very often the whole
	// problem.
	OutcomeUnroutable = "unroutable"

	// OutcomeFailed is a publish or a handler that returned an error or panicked.
	OutcomeFailed = acemq.OutcomeFailed

	// OutcomeAcked is a handler accepting the message.
	OutcomeAcked = acemq.OutcomeAcked

	// OutcomeRetried is another attempt actually being scheduled.
	OutcomeRetried = acemq.OutcomeRetried

	// OutcomeRejected is a handler refusing the message outright.
	OutcomeRejected = acemq.OutcomeRejected

	// OutcomeDeadLettered is a message that ran out of attempts or was given up on.
	OutcomeDeadLettered = acemq.OutcomeDeadLettered

	// OutcomeParked is a message nothing could read: one the codec could not
	// decode, which never reaches a handler at all, or one a handler returned
	// [acemq.Park] for because it got further before finding out. Only the
	// second appears on a consume span, because only the second had a handler
	// to open one.
	OutcomeParked = acemq.OutcomeParked

	// OutcomeAnswered is a request that got its reply.
	OutcomeAnswered = "answered"

	// OutcomeTimedOut is a request that did not, which is the absence of an
	// answer rather than evidence that nothing happened.
	OutcomeTimedOut = "timed_out"

	// OutcomeCompleted and OutcomeEndedEarly are how a pipeline run finished:
	// through its last step, or stopped before it by a step that decided this
	// message does not continue.
	OutcomeCompleted  = patterns.OutcomeCompleted
	OutcomeEndedEarly = patterns.OutcomeEndedEarly
)

// The event names.
const (
	EventOutboxPublishFailed = "outbox.publish_failed"
	EventPipelineRunFinished = "pipeline.run_finished"
	EventMessageRetried      = "message.retried"
	EventMessageDeadLettered = "message.dead_lettered"
)

// failingOutcomes are the outcomes that make a span an error, and only these.
//
// retried and rejected are deliberately absent. A retry is the system working —
// the message will be tried again and very often succeeds — and a message the
// handler refused on purpose is a decision rather than a fault. Marking either
// as an error is how a trace view fills with red and stops meaning anything.
var failingOutcomes = map[string]bool{
	OutcomeUnroutable:   true,
	OutcomeFailed:       true,
	OutcomeDeadLettered: true,
}

// Tracing emits the spans. Build one at start-up and keep it; it is safe for
// concurrent use.
type Tracing struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	system     string
}

type config struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
	system     string
}

// Option configures a [Tracing].
type Option func(*config)

// WithTracerProvider takes spans from a particular provider rather than the
// process's global one. What a test uses.
func WithTracerProvider(p trace.TracerProvider) Option {
	return func(c *config) { c.provider = p }
}

// WithPropagator reads and writes trace context some other way than the
// default. Pass it a B3 or Jaeger propagator to interoperate with a fleet that
// has not moved to W3C yet; the other AceMQ libraries will not follow, so both
// ends have to be configured together.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) { c.propagator = p }
}

// WithSystem sets the messaging.system attribute. Leave it alone unless the
// broker is not RabbitMQ.
func WithSystem(name string) Option {
	return func(c *config) { c.system = name }
}

// New builds the adapter.
//
// Without options it takes the process's tracer provider, so it emits nothing
// until the application configures an SDK — which is the right default for a
// library: the exporter and the sampler are the application's business.
func New(opts ...Option) *Tracing {
	cfg := config{system: DefaultSystem}
	for _, opt := range opts {
		opt(&cfg)
	}

	provider := cfg.provider
	if provider == nil {
		provider = otelapi.GetTracerProvider()
	}
	propagator := cfg.propagator
	if propagator == nil {
		propagator = defaultPropagator()
	}
	system := cfg.system
	if system == "" {
		system = DefaultSystem
	}

	return &Tracing{
		tracer: provider.Tracer(InstrumentationName,
			trace.WithInstrumentationVersion(acemq.Version)),
		propagator: propagator,
		system:     system,
	}
}

// defaultPropagator is the process's, when it has set one, and W3C trace
// context when it has not.
//
// Go's global propagator is a no-op until an application assigns one, which is
// a well-known way to end up with tracing that works locally and writes no
// header in production. Falling back to W3C rather than to nothing is what the
// two header names in this contract already mean, and matches what the Python
// and Ruby libraries get from their own defaults.
func defaultPropagator() propagation.TextMapPropagator {
	global := otelapi.GetTextMapPropagator()
	if len(global.Fields()) > 0 {
		return global
	}
	return propagation.TraceContext{}
}

// Tracer is what this emits through, for a caller that wants a span of its own
// inside one of these.
func (t *Tracing) Tracer() trace.Tracer { return t.tracer }

// System is the messaging.system attribute being written.
func (t *Tracing) System() string { return t.system }

// Span is an operation in progress.
//
// Ending it without an outcome is allowed and means nobody said how it went,
// which is worth seeing as such rather than being guessed at.
type Span struct {
	span trace.Span
	once sync.Once

	// named records that somebody called [Span.Outcome], so that [Span.Failed]
	// does not write failed over a word that was chosen deliberately. A request
	// that ran out of time is timed_out and not failed, and the caller is the
	// only one that knows which.
	named atomic.Bool
}

// Span is the OpenTelemetry span underneath, for an attribute this adapter does
// not write.
func (s *Span) Span() trace.Span { return s.span }

// Outcome records how the operation ended, and marks the span an error when it
// was one. See [failingOutcomes] for which are which.
func (s *Span) Outcome(outcome string) *Span {
	if s == nil || !s.span.IsRecording() {
		return s
	}
	s.named.Store(true)
	s.span.SetAttributes(attribute.String(AttrOutcome, outcome))
	if failingOutcomes[outcome] {
		s.span.SetStatus(codes.Error, outcome)
	}
	return s
}

// Failed records that the operation threw, as an exception event, an error
// status, and an outcome of failed.
//
// The outcome is written here because a span without one is a span a dashboard
// cannot find. A publish that threw used to leave the counter tagged failed and
// the span carrying no messaging.acemq.outcome at all, so the two disagreed
// about the same message — which is the whole thing this vocabulary exists to
// prevent.
//
// An outcome named through [Span.Outcome] wins, whichever order the two are
// called in. A request that ran out of time is timed_out and a message that
// reached no queue is unroutable, and both are more useful than failed; only a
// failure nobody has a better word for is called failed.
func (s *Span) Failed(err error) *Span {
	if s == nil || !s.span.IsRecording() {
		return s
	}
	if !s.named.Load() {
		s.span.SetAttributes(attribute.String(AttrOutcome, OutcomeFailed))
	}
	if err == nil {
		s.span.SetStatus(codes.Error, "failed")
		return s
	}
	s.span.RecordError(err)
	s.span.SetStatus(codes.Error, err.Error())
	return s
}

// Note records that something went wrong without saying the operation failed.
// What a rejected message's reason is: the handler decided, and the span is not
// an error for it.
func (s *Span) Note(err error) *Span {
	if s == nil || err == nil || !s.span.IsRecording() {
		return s
	}
	s.span.RecordError(err)
	return s
}

// End finishes the span. Calling it twice does nothing the second time, so a
// deferred End and an explicit one cannot end it twice.
func (s *Span) End() {
	if s == nil {
		return
	}
	s.once.Do(func() { s.span.End() })
}

// StartPublish opens the span covering a publish and makes it current.
//
// The destination is the exchange, or the routing key when publishing to the
// default exchange, which is what the Java adapter names the span after.
//
// The message's own attributes — identifier, conversation, type — are filled in
// by [Tracing.PublishInterceptor] when the envelope exists, which is after this
// is called: the envelope is built inside the publish. Register the interceptor
// on the connection or the span carries the destination and nothing about the
// message.
func (t *Tracing) StartPublish(
	ctx context.Context, exchange, routingKey string,
) (context.Context, *Span) {
	destination := exchange
	if destination == "" {
		destination = routingKey
	}

	ctx, span := t.tracer.Start(ctx, destination+SpanPublishSuffix,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String(AttrSystem, t.system),
			attribute.String(AttrDestination, destination),
			attribute.String(AttrOperation, "publish"),
			attribute.String(AttrRoutingKey, routingKey),
		))

	wrapped := &Span{span: span}
	return withSink(ctx, &sink{span: span, routingKey: true}), wrapped
}

// StartConsume opens the span covering a handler, joined to the publish that
// caused it.
//
// The parent comes out of the message's own headers rather than out of whatever
// this goroutine happened to be doing, which is the whole point: the publish
// happened in another process, possibly minutes ago, and nothing here remembers
// it. Ambient context is not consulted at all on this path — a delivery arrives
// on a goroutine the consumer owns, and anything current on it belongs to
// another message.
func (t *Tracing) StartConsume(
	ctx context.Context, queue string, env acemq.Envelope,
) (context.Context, *Span) {
	parent := t.Extract(ctx, env.Headers)

	ctx, span := t.tracer.Start(parent, queue+SpanProcessSuffix,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(AttrSystem, t.system),
			attribute.String(AttrDestination, queue),
			attribute.String(AttrOperation, "process"),
			attribute.String(AttrMessageID, env.ID),
			attribute.String(AttrConversationID, env.CorrelationID),
			attribute.String(AttrMessageType, env.Type),
			attribute.Int(AttrAttempt, env.Attempt),
		))

	return ctx, &Span{span: span}
}

// StartRequest opens the span covering a request that waits for its reply.
//
//	ctx, span := tracing.StartRequest(ctx, "pricing")
//	defer span.End()
//
//	answer, err := requester.Do(ctx, question)
//
// CLIENT rather than PRODUCER because this one waits: its duration is a round
// trip, not a handoff. The span is made current, so the publish inside it and
// the reply's delivery become its children rather than three unrelated hops.
func (t *Tracing) StartRequest(ctx context.Context, destination string) (context.Context, *Span) {
	ctx, span := t.tracer.Start(ctx, destination+SpanRequestSuffix,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(AttrSystem, t.system),
			attribute.String(AttrDestination, destination),
			attribute.String(AttrOperation, "request"),
		))

	return withSink(ctx, &sink{span: span}), &Span{span: span}
}

// OutboxPublishFailed records that a relay could not publish a record it had
// already committed.
//
// An event on whatever span is open, and nothing when none is. Nothing is a
// legitimate answer: a relay running on its own goroutine with no delivery in
// flight has no span to hang an event on, and opening one for the event alone
// would produce exactly the zero-length span this adapter avoids.
func (t *Tracing) OutboxPublishFailed(ctx context.Context, destination, reason string) {
	addEvent(ctx, EventOutboxPublishFailed,
		attribute.String(AttrDestination, destination),
		attribute.String(AttrReason, reason))
}

// OutboxPublished records how far behind the relay is running.
//
// An attribute on the current span rather than an event: it measures the
// publish that is happening, not something that happened during it.
func (t *Tracing) OutboxPublished(ctx context.Context, lag time.Duration) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.Int64(AttrOutboxLagMs, lag.Milliseconds()))
	}
}

// PipelineRunFinished records that a message left a pipeline, whether it
// finished or stopped early.
//
// A pipeline's steps are each traced as ordinary hops, so the stages are
// visible. What is not visible from them is the run: how long the whole thing
// took, and how often it ends before the last step.
func (t *Tracing) PipelineRunFinished(
	ctx context.Context, pipeline, step, outcome string, age time.Duration,
) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrPipeline, pipeline),
		attribute.String(AttrStep, step),
		attribute.String(AttrEventOutcome, outcome),
	}
	if age > 0 {
		attrs = append(attrs, attribute.Int64(AttrRunAgeMs, age.Milliseconds()))
	}
	addEvent(ctx, EventPipelineRunFinished, attrs...)
}

// MessageRetried records that a message will be tried again, and how long from
// now when that is known. A zero delay omits the attribute rather than claiming
// the message goes back immediately.
func (t *Tracing) MessageRetried(
	ctx context.Context, queue string, env acemq.Envelope, delay time.Duration,
) {
	attrs := []attribute.KeyValue{
		attribute.String(AttrDestination, queue),
		attribute.Int(AttrAttempt, env.Attempt),
	}
	if delay > 0 {
		attrs = append(attrs, attribute.Int64(AttrRetryDelayMs, delay.Milliseconds()))
	}
	addEvent(ctx, EventMessageRetried, attrs...)
}

// MessageDeadLettered records that a message was given up on.
//
// The reason is unbounded text, which a span tolerates and a metric does not.
func (t *Tracing) MessageDeadLettered(
	ctx context.Context, queue string, env acemq.Envelope, reason string,
) {
	addEvent(ctx, EventMessageDeadLettered,
		attribute.String(AttrDestination, queue),
		attribute.String(AttrReason, reason),
		attribute.Int(AttrAttempt, env.Attempt))
}

func addEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// PropagationHeaders is the current trace context in a fresh carrier, for
// putting on a message this library does not publish.
//
// Empty when nothing is being traced, so a caller can merge it unconditionally.
func (t *Tracing) PropagationHeaders(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	t.propagator.Inject(ctx, carrier)
	return carrier
}

// Inject writes the current trace context onto an envelope's headers.
//
// [Tracing.PublishInterceptor] does this for every message published on a
// connection. Call it directly only for a message assembled by hand — a record
// going into an outbox, say, whose publish happens later and elsewhere.
func (t *Tracing) Inject(ctx context.Context, env *acemq.Envelope) {
	if env == nil {
		return
	}
	for name, value := range t.PropagationHeaders(ctx) {
		if env.Headers == nil {
			env.Headers = map[string]any{}
		}
		env.Headers[name] = value
	}
}

// Extract reads the trace context out of a message's headers.
//
// The headers are an any-valued map because that is what comes off the wire: a
// traceparent written by another client can arrive as a string or as bytes, and
// both are the same header.
func (t *Tracing) Extract(ctx context.Context, headers map[string]any) context.Context {
	return t.propagator.Extract(ctx, headerCarrier(headers))
}

// headerCarrier reads W3C trace context out of an AceMQ envelope's headers.
type headerCarrier map[string]any

func (c headerCarrier) Get(key string) string {
	switch value := c[key].(type) {
	case nil:
		return ""
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

func (c headerCarrier) Set(key, value string) {
	if c != nil {
		c[key] = value
	}
}

// Keys is sorted, so a propagator that iterates them behaves the same way twice.
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// sink is a span waiting for the envelope that does not exist yet.
//
// A publish span is opened before the envelope is built — the identifier, the
// conversation and the type are decided inside the publish — so the interceptor
// finishes the span's attributes when it finally sees one. Carried in the
// context rather than found by [trace.SpanFromContext] so that an interceptor
// running under somebody else's span, an HTTP server's say, does not write
// messaging attributes onto it.
type sink struct {
	span       trace.Span
	routingKey bool
	once       sync.Once
}

type sinkKey struct{}

func withSink(ctx context.Context, s *sink) context.Context {
	return context.WithValue(ctx, sinkKey{}, s)
}

func sinkFrom(ctx context.Context) *sink {
	s, _ := ctx.Value(sinkKey{}).(*sink)
	return s
}

// fill writes the message's attributes, once. A requester publishes once inside
// its span and the first publish is the request; anything after it is somebody
// else's message and must not overwrite these.
func (s *sink) fill(c *acemq.PublishContext) {
	if s == nil || c.Envelope == nil || !s.span.IsRecording() {
		return
	}
	s.once.Do(func() {
		attrs := []attribute.KeyValue{
			attribute.String(AttrMessageID, c.Envelope.ID),
			attribute.String(AttrConversationID, c.Envelope.CorrelationID),
			attribute.String(AttrMessageType, c.Envelope.Type),
		}
		if s.routingKey {
			attrs = append(attrs, attribute.String(AttrRoutingKey, c.RoutingKey))
		}
		s.span.SetAttributes(attrs...)
	})
}
