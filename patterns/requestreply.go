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

// Package patterns holds the message-flow patterns that sit above publishing
// and consuming: request and reply, idempotency, the outbox, pipelines, ordered
// consumption and replay.
//
// They are here rather than in the core package because none of them is needed
// to send a message, and a core that carries everything is a core nobody can
// read.
package patterns

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// ErrRequestTimedOut is returned when no reply arrived in time.
//
// It says nothing about whether the request was handled. A timeout is the
// absence of an answer, and the work may well have been done — which is why a
// request that changes anything should be idempotent. See [IdempotencyStore].
var ErrRequestTimedOut = errors.New("acemq: no reply arrived before the deadline")

// Requester sends a request and waits for the reply.
//
// Messaging is asynchronous, and request-reply is a synchronous shape drawn on
// top of it. That is a real cost: a caller blocked on a reply is a caller
// holding a goroutine, a connection and a deadline, and a queue that backs up
// turns into a service that stops responding. Reach for it where a caller
// genuinely cannot proceed without the answer, and publish an event otherwise.
type Requester[Req, Resp any] struct {
	conn         *acemq.Conn
	exchange     string
	routingKey   string
	replyQueue   string
	timeout      time.Duration
	consumer     *acemq.Consumer
	publisher    *acemq.Publisher[Req]
	pendingMu    sync.Mutex
	pending      map[string]chan acemq.Message[Resp]
	closeOnce    sync.Once
	closeErr     error
	replyQueueMu sync.Mutex
}

// RequesterOption configures a requester.
type RequesterOption func(*requesterConfig)

type requesterConfig struct {
	replyQueue string
	timeout    time.Duration
}

// ReplyTo names the queue replies come back on.
//
// One is generated when this is not given: an exclusive, auto-deleting queue
// belonging to this process, which is what most callers want. Name one only
// when replies must survive a restart.
func ReplyTo(queue string) RequesterOption {
	return func(c *requesterConfig) { c.replyQueue = queue }
}

// Timeout sets how long to wait for a reply. Thirty seconds by default.
func Timeout(d time.Duration) RequesterOption {
	return func(c *requesterConfig) { c.timeout = d }
}

// NewRequester starts a requester and its reply consumer.
//
// Close it when done; it holds a queue and a consumer.
func NewRequester[Req, Resp any](
	ctx context.Context, conn *acemq.Conn, exchange, routingKey string, opts ...RequesterOption,
) (*Requester[Req, Resp], error) {
	cfg := requesterConfig{timeout: 30 * time.Second}
	for _, opt := range opts {
		opt(&cfg)
	}

	replyQueue := cfg.replyQueue
	generated := replyQueue == ""
	if generated {
		replyQueue = "acemq-reply-" + acemq.NewID()
	}

	declareOpts := []acemq.QueueOption{}
	if generated {
		// Belongs to this process and goes away with it. A reply queue that
		// outlived its requester would collect answers nobody is waiting for.
		declareOpts = append(declareOpts, acemq.Transient(), acemq.AutoDelete(), acemq.Exclusive())
	}
	if err := conn.DeclareQueue(ctx, replyQueue, declareOpts...); err != nil {
		return nil, fmt.Errorf("acemq: cannot declare the reply queue %q: %w", replyQueue, err)
	}

	r := &Requester[Req, Resp]{
		conn:       conn,
		exchange:   exchange,
		routingKey: routingKey,
		replyQueue: replyQueue,
		timeout:    cfg.timeout,
		pending:    map[string]chan acemq.Message[Resp]{},
	}
	r.publisher = acemq.NewPublisher[Req](conn, exchange, routingKey)

	consumer, err := acemq.Consume(ctx, conn, replyQueue,
		func(_ context.Context, m acemq.Message[Resp]) acemq.Ack {
			r.deliver(m)
			return acemq.Accept()
		})
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot consume replies from %q: %w", replyQueue, err)
	}
	r.consumer = consumer
	return r, nil
}

// deliver hands a reply to whoever is waiting for it.
//
// A reply nobody is waiting for is dropped, which is what a reply to a request
// that already timed out is. Blocking here would stall the reply consumer for
// everybody else.
func (r *Requester[Req, Resp]) deliver(m acemq.Message[Resp]) {
	key := m.Envelope.CorrelationID

	r.pendingMu.Lock()
	waiter, waiting := r.pending[key]
	if waiting {
		delete(r.pending, key)
	}
	r.pendingMu.Unlock()

	if !waiting {
		return
	}
	select {
	case waiter <- m:
	default:
	}
}

// Do sends a request and waits for its reply.
func (r *Requester[Req, Resp]) Do(ctx context.Context, request Req, opts ...acemq.EnvelopeOption) (Resp, error) {
	var zero Resp

	// The correlation identifier is what pairs a reply with its request, so it
	// is generated here rather than taken from the caller's options.
	correlation := acemq.NewID()
	waiter := make(chan acemq.Message[Resp], 1)

	r.pendingMu.Lock()
	r.pending[correlation] = waiter
	r.pendingMu.Unlock()

	defer func() {
		r.pendingMu.Lock()
		delete(r.pending, correlation)
		r.pendingMu.Unlock()
	}()

	all := make([]acemq.EnvelopeOption, 0, len(opts)+2)
	all = append(all, opts...)
	all = append(all,
		acemq.CorrelationID(correlation),
		acemq.Header(HeaderReplyTo, r.replyQueue))

	if err := r.publisher.Send(ctx, request, all...); err != nil {
		return zero, err
	}

	timeout := time.NewTimer(r.timeout)
	defer timeout.Stop()

	select {
	case m := <-waiter:
		if failure := m.Envelope.Headers[HeaderError]; failure != nil {
			return zero, fmt.Errorf("acemq: the responder failed: %v", failure)
		}
		return m.Payload, nil
	case <-timeout.C:
		// The work may well have been done. A timeout is the absence of an
		// answer, not evidence that nothing happened.
		return zero, fmt.Errorf("%w after %s (correlation %s)",
			ErrRequestTimedOut, r.timeout, correlation)
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// ReplyQueue is the queue replies arrive on.
func (r *Requester[Req, Resp]) ReplyQueue() string { return r.replyQueue }

// Close stops the reply consumer.
func (r *Requester[Req, Resp]) Close() error {
	r.closeOnce.Do(func() {
		if r.consumer != nil {
			r.closeErr = r.consumer.Close()
		}
	})
	return r.closeErr
}

// HeaderReplyTo names the queue a responder should reply to.
//
// An application header rather than AMQP's own reply-to property, because it
// then travels through the same envelope machinery as everything else and
// survives a hop through a service that rebuilds the message.
//
// It deliberately does not carry the x-acemq- prefix. That namespace belongs to
// the engine: headers in it are stripped before a handler sees them, so a
// responder could never read this one.
const HeaderReplyTo = "acemq-reply-to"

// HeaderError carries a responder's failure back to the requester.
const HeaderError = "acemq-error"

// Responder answers requests.
//
// The handler returns a response or an error. An error is sent back to the
// requester rather than being swallowed, because a caller blocked on a reply
// should learn that it failed rather than wait out the timeout.
type Responder struct {
	consumer *acemq.Consumer
}

// Serve consumes requests from a queue and replies to each one.
//
//	responder, err := patterns.Serve(ctx, mq, "price-requests",
//		func(ctx context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
//			return price(ctx, m.Payload)
//		})
//	defer responder.Close()
func Serve[Req, Resp any](
	ctx context.Context, conn *acemq.Conn, queue string,
	handle func(context.Context, acemq.Message[Req]) (Resp, error),
	opts ...acemq.ConsumeOption,
) (*Responder, error) {
	consumer, err := acemq.Consume(ctx, conn, queue,
		func(ctx context.Context, m acemq.Message[Req]) acemq.Ack {
			replyTo, _ := m.Envelope.Headers[HeaderReplyTo].(string)
			if replyTo == "" {
				// Nothing to reply to. Retrying cannot make a reply queue
				// appear, so this is dead-lettered rather than looped.
				return acemq.Reject(acemq.Fatalf(
					"acemq: request %s carries no %s header, so there is nowhere to reply",
					m.Envelope.ID, HeaderReplyTo))
			}

			response, err := handle(ctx, m)
			if err != nil {
				// The failure goes back to the caller, then the request is
				// settled: replying and then retrying would answer twice.
				if sendErr := replyWithError[Resp](ctx, conn, replyTo, m, err); sendErr != nil {
					return acemq.Retry(sendErr)
				}
				if acemq.IsFatal(err) {
					return acemq.Reject(err)
				}
				return acemq.Reject(err)
			}

			if err := reply(ctx, conn, replyTo, m, response); err != nil {
				// The work is done but the answer did not get out. Retrying
				// repeats the work, which is why a responder should be
				// idempotent.
				return acemq.Retry(err)
			}
			return acemq.Accept()
		}, opts...)
	if err != nil {
		return nil, err
	}
	return &Responder{consumer: consumer}, nil
}

func reply[Req, Resp any](
	ctx context.Context, conn *acemq.Conn, replyTo string, request acemq.Message[Req], response Resp,
) error {
	return acemq.NewPublisher[Resp](conn, "", replyTo).Send(ctx, response,
		acemq.CorrelationID(request.Envelope.CorrelationID),
		acemq.CausationID(request.Envelope.ID))
}

func replyWithError[Resp any, Req any](
	ctx context.Context, conn *acemq.Conn, replyTo string, request acemq.Message[Req], cause error,
) error {
	var empty Resp
	return acemq.NewPublisher[Resp](conn, "", replyTo).Send(ctx, empty,
		acemq.CorrelationID(request.Envelope.CorrelationID),
		acemq.CausationID(request.Envelope.ID),
		acemq.Header(HeaderError, cause.Error()))
}

// Close stops answering.
func (r *Responder) Close() error { return r.consumer.Close() }
