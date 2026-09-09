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

package otel

import (
	"context"
	"errors"
	"fmt"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	"go.opentelemetry.io/otel/attribute"
)

// PublishInterceptor writes the current trace context onto every message
// published on a connection.
//
//	mq, err := acemq.Connect(ctx, url,
//		acemq.WithPublishInterceptor(tracing.PublishInterceptor()))
//
// Register it once and every publisher on that connection carries the trace,
// including the ones inside [patterns.Requester] and the outbox relay. It
// writes nothing when nothing is being traced, so a process with no SDK
// configured publishes exactly the headers it did before.
//
// It also completes the attributes of a span opened by [Tracing.StartPublish]
// or [Tracing.StartRequest], which are opened before the envelope exists. A
// span belonging to something else — an HTTP server's, say — is left alone:
// messaging attributes on a span that is not about a message are worse than
// none.
func (t *Tracing) PublishInterceptor() acemq.PublishInterceptor {
	return func(ctx context.Context, c *acemq.PublishContext) error {
		sinkFrom(ctx).fill(c)
		for name, value := range t.PropagationHeaders(ctx) {
			c.SetHeader(name, value)
		}
		return nil
	}
}

// Publisher is an [acemq.Publisher] with a span around every send.
//
//	orders := otel.NewPublisher[OrderPlaced](tracing, mq, "", "orders")
//	err := orders.Send(ctx, order)
//
// A publisher rather than a wrapper function because the span is named after
// the destination, and the destination is what a publisher knows and a caller
// would otherwise have to repeat at every call site.
type Publisher[T any] struct {
	tracing    *Tracing
	inner      *acemq.Publisher[T]
	exchange   string
	routingKey string
}

// NewPublisher builds a traced publisher for an exchange and routing key. The
// options are [acemq.NewPublisher]'s and mean the same things.
func NewPublisher[T any](
	t *Tracing, conn *acemq.Conn, exchange, routingKey string, opts ...acemq.PublisherOption[T],
) *Publisher[T] {
	return &Publisher[T]{
		tracing:    t,
		inner:      acemq.NewPublisher[T](conn, exchange, routingKey, opts...),
		exchange:   exchange,
		routingKey: routingKey,
	}
}

// Wrap traces a publisher that already exists, for one built somewhere this
// adapter is not.
func Wrap[T any](t *Tracing, inner *acemq.Publisher[T], exchange, routingKey string) *Publisher[T] {
	return &Publisher[T]{tracing: t, inner: inner, exchange: exchange, routingKey: routingKey}
}

// Inner is the publisher underneath.
func (p *Publisher[T]) Inner() *acemq.Publisher[T] { return p.inner }

// Send publishes one message inside a span.
func (p *Publisher[T]) Send(ctx context.Context, payload T, opts ...acemq.EnvelopeOption) error {
	_, err := p.SendResult(ctx, payload, opts...)
	return err
}

// SendResult publishes one message inside a span and returns what the broker
// said about it.
func (p *Publisher[T]) SendResult(
	ctx context.Context, payload T, opts ...acemq.EnvelopeOption,
) (acemq.PublishResult, error) {
	ctx, span := p.tracing.StartPublish(ctx, p.exchange, p.routingKey)
	defer span.End()

	result, err := p.inner.SendResult(ctx, payload, opts...)
	finishPublish(span, result, err)
	return result, err
}

// SendEnvelope publishes a message with an envelope built by the caller, inside
// a span.
//
// The outcome is published rather than confirmed even when the broker did
// confirm it: [acemq.Publisher.SendEnvelope] returns an error and nothing else,
// so there is nothing here to read. Use [Publisher.SendResult] when the
// difference matters.
func (p *Publisher[T]) SendEnvelope(ctx context.Context, payload T, env acemq.Envelope) error {
	ctx, span := p.tracing.StartPublish(ctx, p.exchange, p.routingKey)
	defer span.End()

	err := p.inner.SendEnvelope(ctx, payload, env)
	finishPublish(span, acemq.PublishResult{}, err)
	return err
}

// finishPublish records how a publish ended.
//
// A message that reached no queue is called out separately from one the broker
// refused, because the broker does not consider it an error at all and it is
// very often the whole problem: the publisher succeeded, the consumer waited,
// and nothing anywhere said why.
func finishPublish(span *Span, result acemq.PublishResult, err error) {
	if err != nil {
		var failed *acemq.PublishFailedError
		if errors.As(err, &failed) && failed.Unroutable {
			span.Outcome(OutcomeUnroutable)
			if failed.Err != nil {
				span.Span().SetAttributes(
					attribute.String(AttrReason, failed.Err.Error()))
			}
		} else {
			span.Outcome(OutcomeFailed)
		}
		span.Failed(err)
		return
	}
	if result.Confirmed {
		span.Outcome(OutcomeConfirmed)
		return
	}
	span.Outcome(OutcomePublished)
}

// Handle wraps a handler so every delivery gets a span, parented by the publish
// that caused it.
//
//	_, err := acemq.Consume(ctx, mq, "orders",
//		otel.Handle(tracing, "orders", handle))
//
// The queue is passed rather than read off the delivery because a handler is
// given a message, not the subscription it arrived on, and the span is named
// after the queue.
//
// # The engine has the last word
//
// A handler asking for a retry is a request, not an outcome: the engine still
// has to look at the policy, and a message on its last attempt is dead-lettered
// instead. A span ended when the handler returned would be closed before that
// happened, and would say retried for a message nobody will ever try again —
// which is exactly what somebody searching a trace backend for dead letters
// fails to find.
//
// So the span's ending is handed to the engine through [acemq.OnSettled], and
// the retry and dead-letter events are written where the decision is taken. A
// handler called directly rather than run by a consumer gets no engine and no
// handover, and its span ends here as it always did.
// # One word for the counter and the span
//
// The outcome written here is [acemq.Settlement.Outcome] — the same string the
// engine tags acemq.messages.consumed with for this delivery. It is not derived
// again from the handler's Ack, so a dashboard filtered to dead-lettered
// messages and a trace search for the same thing cannot return different sets.
func Handle[T any](t *Tracing, queue string, next acemq.Handler[T]) acemq.Handler[T] {
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		ctx, span := t.StartConsume(ctx, queue, m.Envelope)

		// A pipeline step running inside this handler has a span to write
		// pipeline.run_finished onto, and this is where it is offered one.
		ctx = patterns.WithRunObserver(ctx, t.PipelineRunFinished)

		engine := acemq.OnSettled(ctx, func(s acemq.Settlement) {
			t.settled(ctx, span, s)
		})
		if !engine {
			defer span.End()
		}

		// A handler that panics is recorded as an exception here and settled by
		// the engine as a rejection, so the outcome stays the engine's word and
		// the span is still an error. Ending it here instead would close it
		// before the settlement arrived, and would leave the span saying failed
		// where the counter said rejected. With no engine there is nothing to
		// wait for and failed is all there is to say.
		defer func() {
			if r := recover(); r != nil {
				if !engine {
					span.Outcome(OutcomeFailed)
				}
				span.Failed(fmt.Errorf("acemq: the handler panicked: %v", r))
				panic(r)
			}
		}()

		ack := next(ctx, m)

		// What the handler asked for, on a span that has to be finished here
		// because nothing else will. Under an engine the settlement below
		// replaces all of this with what actually happened; Ack keeps its
		// decision unexported — returning a value rather than calling a method
		// is what stops a handler forgetting to decide — so String is what there
		// is to read it by.
		if !engine {
			switch ack.String() {
			case "retry":
				// Not an error. The message will be tried again, and very often
				// succeeds; the reason is still worth carrying, as an exception
				// event without an error status.
				span.Outcome(OutcomeRetried).Note(ack.Err())
			case "reject":
				span.Outcome(OutcomeRejected).Note(ack.Err())
			default:
				span.Outcome(OutcomeAcked)
			}
			return ack
		}
		span.Note(ack.Err())
		return ack
	}
}

// settled writes what the engine did onto the handler's span and ends it.
//
// The outcome comes off the settlement rather than being worked out again from
// the handler's Ack. Working it out twice is how the two came apart: a message
// that used its last attempt asked to be retried and was dead-lettered, and a
// span that reported the request said retried for a message nobody would ever
// try again. A rejection still reads rejected rather than dead_lettered — both
// end in the dead-letter queue, and the engine keeps them apart on the
// settlement so that this adapter does not have to.
func (t *Tracing) settled(ctx context.Context, span *Span, s acemq.Settlement) {
	switch s.Action {
	case acemq.SettledRetried:
		t.MessageRetried(ctx, s.Queue, s.Envelope, s.Delay)

	case acemq.SettledDeadLettered, acemq.SettledParked:
		t.MessageDeadLettered(ctx, s.Queue, s.Envelope, s.Reason)
	}
	span.Outcome(s.Outcome)
	span.End()
}

// Middleware is [Handle] as a [patterns.Middleware], so tracing can sit in a
// [patterns.Chain] with everything else that wraps a handler.
//
//	handler := patterns.Chain(handle,
//		otel.Middleware[OrderPlaced](tracing, "orders"),
//		patterns.WithTimeout[OrderPlaced](10*time.Second))
//
// Outermost is the place for it: a span that does not cover the timeout and the
// idempotency guard cannot show what they decided.
func Middleware[T any](t *Tracing, queue string) patterns.Middleware[T] {
	return func(next acemq.Handler[T]) acemq.Handler[T] {
		return Handle(t, queue, next)
	}
}

// Asker is what a [patterns.Requester] does. An interface so [Ask] can be used
// against a stub, and so this package does not have to own the requester.
type Asker[Req, Resp any] interface {
	Do(ctx context.Context, request Req, opts ...acemq.EnvelopeOption) (Resp, error)
}

// Ask makes a request inside a CLIENT span.
//
//	answer, err := otel.Ask[Question, Answer](ctx, tracing, "pricing", requester, question)
//
// A request that times out is recorded as timed_out rather than failed: the
// work may well have been done, and a trace that says otherwise sends somebody
// looking for a failure that did not happen.
func Ask[Req, Resp any](
	ctx context.Context,
	t *Tracing,
	destination string,
	asker Asker[Req, Resp],
	request Req,
	opts ...acemq.EnvelopeOption,
) (Resp, error) {
	ctx, span := t.StartRequest(ctx, destination)
	defer span.End()

	answer, err := asker.Do(ctx, request, opts...)
	switch {
	case err == nil:
		span.Outcome(OutcomeAnswered)
	case errors.Is(err, patterns.ErrRequestTimedOut), errors.Is(err, context.DeadlineExceeded):
		span.Outcome(OutcomeTimedOut)
		span.Note(err)
	default:
		span.Outcome(OutcomeFailed)
		span.Failed(err)
	}
	return answer, err
}
