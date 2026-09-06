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

package patterns

import (
	"context"
	"fmt"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// Middleware wraps a handler.
//
// The order matters and reads outside-in: the first middleware given is the
// outermost, so it sees the message first and the result last.
type Middleware[T any] func(acemq.Handler[T]) acemq.Handler[T]

// Chain wraps a handler in middleware.
//
//	handler := patterns.Chain(handle,
//		patterns.WithLogging[OrderPlaced](log.Printf),
//		patterns.WithTimeout[OrderPlaced](10*time.Second),
//		patterns.Idempotent[OrderPlaced](store))
//
// Logging is outermost, so it records what the timeout and the idempotency
// guard decided.
func Chain[T any](handler acemq.Handler[T], middleware ...Middleware[T]) acemq.Handler[T] {
	// Applied in reverse so the first named ends up outermost, which is the
	// order somebody reading the list expects.
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}

// WithTimeout gives the handler a deadline.
//
// A handler that runs past its deadline is reported as a retry, whatever it
// says about itself. That is a deliberate choice between two imperfect
// answers: retrying work that may have succeeded risks doing it twice, and
// accepting work that may have failed loses it. Duplicates are a problem you
// can solve — see [Idempotent] — and a lost message is not.
//
// The deadline is enforced by cancelling the context, so a handler that
// honours it stops promptly. One that ignores its context will not stop, and
// this cannot make it: the message stays held until the handler returns, and
// only then is the overrun reported. A wrapper cannot interrupt a goroutine
// that will not be interrupted.
func WithTimeout[T any](d time.Duration) Middleware[T] {
	return func(next acemq.Handler[T]) acemq.Handler[T] {
		return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			ack := next(ctx, m)
			if ctx.Err() != nil {
				return acemq.Retry(fmt.Errorf(
					"acemq: handling message %s did not finish within %s: %w",
					m.Envelope.ID, d, ctx.Err()))
			}
			return ack
		}
	}
}

// WithLogging records what happened to each message.
//
// Takes a printf-shaped function so it fits the standard library, a structured
// logger, or a test — without this package choosing a logging library for
// anybody.
func WithLogging[T any](printf func(format string, args ...any)) Middleware[T] {
	return func(next acemq.Handler[T]) acemq.Handler[T] {
		return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
			started := time.Now()
			ack := next(ctx, m)

			if err := ack.Err(); err != nil {
				printf("acemq %s type=%s attempt=%d took=%s %s: %v",
					m.Envelope.ID, m.Envelope.Type, m.Envelope.Attempt,
					time.Since(started).Round(time.Millisecond), ack, err)
				return ack
			}
			printf("acemq %s type=%s attempt=%d took=%s %s",
				m.Envelope.ID, m.Envelope.Type, m.Envelope.Attempt,
				time.Since(started).Round(time.Millisecond), ack)
			return ack
		}
	}
}

// WithRecovery turns a panic into a rejection.
//
// The consumer already does this, so it is here only for a handler called
// outside one — in a test, or behind a pipeline that runs handlers itself.
func WithRecovery[T any]() Middleware[T] {
	return func(next acemq.Handler[T]) acemq.Handler[T] {
		return func(ctx context.Context, m acemq.Message[T]) (ack acemq.Ack) {
			defer func() {
				if r := recover(); r != nil {
					ack = acemq.Reject(acemq.Fatalf(
						"acemq: the handler panicked on message %s: %v", m.Envelope.ID, r))
				}
			}()
			return next(ctx, m)
		}
	}
}

// WithIdempotency is [Idempotent] as middleware, so it can sit in a [Chain].
func WithIdempotency[T any](store IdempotencyStore) Middleware[T] {
	return func(next acemq.Handler[T]) acemq.Handler[T] {
		return Idempotent(store, next)
	}
}

// WithOrdering is [Ordered] as middleware.
func WithOrdering[T any](key PartitionKey[T]) Middleware[T] {
	return func(next acemq.Handler[T]) acemq.Handler[T] {
		return Ordered(key, next)
	}
}

// Then publishes the result of handling a message onwards.
//
// The step that makes a pipeline out of a chain of services: this one consumes,
// does its work, and publishes what comes out, carrying the correlation forward
// and recording what caused what.
//
//	handler := patterns.Then(
//		acemq.NewPublisher[Shipment](mq, "shipping-events", "shipment.requested"),
//		func(ctx context.Context, m acemq.Message[OrderPlaced]) (Shipment, bool, error) {
//			if m.Payload.Digital {
//				return Shipment{}, false, nil   // nothing to ship
//			}
//			return Shipment{OrderID: m.Payload.OrderID}, true, nil
//		})
//
// Returning false publishes nothing and accepts the message, which is how a
// step says "this one does not continue" without inventing an empty message.
//
// The message is accepted only once the next one is published. If publishing
// fails the input is retried, so the work runs again — which is why a step that
// changes anything should be idempotent.
func Then[In, Out any](
	publisher *acemq.Publisher[Out],
	step func(context.Context, acemq.Message[In]) (Out, bool, error),
) acemq.Handler[In] {
	return func(ctx context.Context, m acemq.Message[In]) acemq.Ack {
		out, publish, err := step(ctx, m)
		if err != nil {
			if acemq.IsFatal(err) {
				return acemq.Reject(err)
			}
			return acemq.Retry(err)
		}
		if !publish {
			return acemq.Accept()
		}

		err = publisher.Send(ctx, out,
			acemq.CorrelationID(m.Envelope.CorrelationID),
			acemq.CausationID(m.Envelope.ID))
		if err != nil {
			return acemq.Retry(fmt.Errorf(
				"acemq: the work for message %s is done but the next message did not go out: %w",
				m.Envelope.ID, err))
		}
		return acemq.Accept()
	}
}
