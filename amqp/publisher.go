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
	"fmt"
)

// Publisher sends messages of one type to one destination.
//
// It is safe for concurrent use and cheap to keep: build one per message type
// at start-up rather than one per message.
type Publisher[T any] struct {
	conn       *Conn
	exchange   string
	routingKey string
	codec      Codec
	persistent bool
	mandatory  bool
}

// NewPublisher builds a publisher for an exchange and routing key.
//
// Publish to a queue by name by leaving the exchange empty: on RabbitMQ the
// default exchange routes to the queue whose name matches the routing key.
//
//	pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
//
// It is a function rather than a method on [Conn] because a method cannot have
// its own type parameter before Go 1.27, and this module supports earlier
// toolchains. See the package documentation.
func NewPublisher[T any](conn *Conn, exchange, routingKey string, opts ...PublisherOption[T]) *Publisher[T] {
	p := &Publisher[T]{
		conn:       conn,
		exchange:   exchange,
		routingKey: routingKey,
		codec:      conn.codec,
		persistent: true,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PublisherOption configures a publisher.
type PublisherOption[T any] func(*Publisher[T])

// PublishWith uses a codec other than the connection's for this publisher, so
// one connection can send JSON to one queue and something denser to another.
func PublishWith[T any](c Codec) PublisherOption[T] {
	return func(p *Publisher[T]) { p.codec = c }
}

// NotPersistent asks the broker not to write these messages to disk. Faster,
// and gone if the broker restarts.
func NotPersistent[T any]() PublisherOption[T] {
	return func(p *Publisher[T]) { p.persistent = false }
}

// Mandatory makes [Publisher.Send] fail when a message reaches no queue.
//
// Without it the broker drops an unroutable message silently, which is the
// quietest way to discover a binding was never made — the publisher succeeds,
// the consumer waits, and nothing anywhere says why. With it, a message nothing
// is bound to receive is an error at the point of publishing.
//
// It costs a round trip only when the message is actually unroutable.
func Mandatory[T any]() PublisherOption[T] {
	return func(p *Publisher[T]) { p.mandatory = true }
}

// Send publishes one message.
//
// The envelope is built here: an identifier, a type defaulting to the routing
// key, a correlation defaulting to the identifier, this connection's origin,
// and the current time. Options override any of those.
func (p *Publisher[T]) Send(ctx context.Context, payload T, opts ...EnvelopeOption) error {
	_, err := p.SendResult(ctx, payload, opts...)
	return err
}

// SendResult publishes one message and returns what the broker said about it.
//
// Use it when the answer matters beyond success or failure — whether the
// message was confirmed, whether it reached a queue, which identifier it went
// out with.
func (p *Publisher[T]) SendResult(
	ctx context.Context, payload T, opts ...EnvelopeOption,
) (PublishResult, error) {
	env, err := p.envelope(opts)
	if err != nil {
		return PublishResult{}, err
	}
	return p.publish(ctx, payload, env)
}

// SendEnvelope publishes a message with an envelope you built yourself, for
// when one message's metadata is derived from another's.
func (p *Publisher[T]) SendEnvelope(ctx context.Context, payload T, env Envelope) error {
	_, err := p.publish(ctx, payload, env)
	return err
}

func (p *Publisher[T]) publish(ctx context.Context, payload T, env Envelope) (PublishResult, error) {
	body, err := p.codec.Encode(payload)
	if err != nil {
		return PublishResult{}, fmt.Errorf(
			"acemq: cannot encode a %T for %q: %w", payload, p.routingKey, err)
	}

	result, err := p.conn.transport.Publish(ctx, p.exchange, p.routingKey, Outbound{
		Body:        body,
		ContentType: p.codec.ContentType(),
		MessageID:   env.ID,
		Headers:     env.ToWire(),
		Persistent:  p.persistent,
		Mandatory:   p.mandatory,
	})
	if err != nil {
		return result, err
	}

	// Reported as an error rather than left in the result, because a caller
	// using Send never sees the result and would otherwise carry on believing
	// the message went somewhere.
	if p.mandatory && !result.Routed {
		reason := result.ReturnReason
		if reason == "" {
			reason = "no queue is bound to receive it"
		}
		return result, fmt.Errorf(
			"acemq: message %s to exchange %q with key %q reached no queue (%s); "+
				"the broker dropped it", env.ID, p.exchange, p.routingKey, reason)
	}
	return result, nil
}

func (p *Publisher[T]) envelope(opts []EnvelopeOption) (Envelope, error) {
	// The routing key is the default message type, and this connection's origin
	// the default origin, so both are applied before the caller's options and
	// can be overridden by them.
	all := make([]EnvelopeOption, 0, len(opts)+1)
	all = append(all, Origin(p.conn.origin))
	all = append(all, opts...)
	return NewEnvelope(p.routingKey, all...)
}
