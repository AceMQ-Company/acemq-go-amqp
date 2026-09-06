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
	"sync"
)

// PublishContext is a message on its way out, before it is encoded.
//
// An interceptor may change the envelope — adding a header, stamping a tenant,
// injecting a trace context — and may stop the publish entirely by returning an
// error.
type PublishContext struct {
	// Exchange and RoutingKey are where it is going. Changing them redirects
	// the message.
	Exchange   string
	RoutingKey string

	// Envelope is the metadata. Headers may be added or changed; the reserved
	// namespace is still refused when the envelope is rendered.
	Envelope *Envelope

	// Payload is the value about to be encoded, as an any. Interceptors that
	// need the concrete type should assert it.
	Payload any
}

// SetHeader adds an application header to the message being published.
func (c *PublishContext) SetHeader(name string, value any) {
	if c.Envelope.Headers == nil {
		c.Envelope.Headers = map[string]any{}
	}
	c.Envelope.Headers[name] = value
}

// ConsumeContext is a message that has arrived, before the handler sees it.
type ConsumeContext struct {
	// Queue is where it came from.
	Queue string

	// Envelope is the metadata, already read off the wire.
	Envelope *Envelope

	// Body is the undecoded payload.
	Body []byte

	// ContentType is what the sender said it was.
	ContentType string

	// Redelivered is the broker saying it has handed this over before.
	Redelivered bool
}

// PublishInterceptor runs before a message is published.
//
// Returning an error stops the publish and the caller sees it. That is the
// point of being able to intercept rather than only observe: a message that
// must not go out can be stopped here rather than in every publisher.
//
//	mq, err := acemq.Connect(ctx, url,
//		acemq.WithPublishInterceptor(func(ctx context.Context, c *acemq.PublishContext) error {
//			c.SetHeader("x-tenant", tenantFrom(ctx))
//			return nil
//		}))
type PublishInterceptor func(ctx context.Context, c *PublishContext) error

// ConsumeInterceptor runs before a handler sees a message.
//
// Returning an error rejects the message without running the handler. Use it
// for cross-cutting refusals — a tenant this process does not serve, a schema
// version it cannot read — rather than repeating the check in every handler.
type ConsumeInterceptor func(ctx context.Context, c *ConsumeContext) error

// WithPublishInterceptor adds an interceptor to every publish on this
// connection. Several may be added; they run in the order given.
func WithPublishInterceptor(interceptor PublishInterceptor) ConnOption {
	return func(cfg *connConfig) error {
		if interceptor == nil {
			return errNilInterceptor
		}
		cfg.publishInterceptors = append(cfg.publishInterceptors, interceptor)
		return nil
	}
}

// WithConsumeInterceptor adds an interceptor to every consumer on this
// connection. Several may be added; they run in the order given.
func WithConsumeInterceptor(interceptor ConsumeInterceptor) ConnOption {
	return func(cfg *connConfig) error {
		if interceptor == nil {
			return errNilInterceptor
		}
		cfg.consumeInterceptors = append(cfg.consumeInterceptors, interceptor)
		return nil
	}
}

// Capability is something a transport can or cannot do.
//
// Asking rather than assuming is what lets code written against one broker fail
// clearly on another rather than mysteriously. The in-memory transport supports
// less than RabbitMQ, deliberately.
type Capability string

const (
	// CapabilityPublisherConfirms is the broker acknowledging publishes.
	CapabilityPublisherConfirms Capability = "publisher-confirms"

	// CapabilityTransactions is AMQP transactions. Neither transport here
	// implements them: confirms are faster and cover what most people reach
	// for transactions to get.
	CapabilityTransactions Capability = "transactions"

	// CapabilityDeadLettering is messages moving to another exchange when
	// rejected.
	CapabilityDeadLettering Capability = "dead-lettering"

	// CapabilityQuorumQueues is RabbitMQ's replicated queue type.
	CapabilityQuorumQueues Capability = "quorum-queues"

	// CapabilityStreams is RabbitMQ's stream queue type, which keeps messages
	// after they are read.
	CapabilityStreams Capability = "streams"

	// CapabilityPriority is per-message priority.
	CapabilityPriority Capability = "priority"

	// CapabilityDelayedDelivery is publishing a message to arrive later. It
	// needs a plugin on RabbitMQ, so it is reported as absent unless one is
	// there.
	CapabilityDelayedDelivery Capability = "delayed-delivery"

	// CapabilityRecovery is the transport reconnecting by itself.
	CapabilityRecovery Capability = "recovery"
)

// CapabilityReporter is a transport that says what it can do.
//
// A transport that does not implement it is assumed to do nothing beyond the
// basics, which is the safe assumption.
type CapabilityReporter interface {
	Supports(c Capability) bool
}

// Supports reports whether this connection's transport can do something.
func (c *Conn) Supports(capability Capability) bool {
	reporter, ok := c.transport.(CapabilityReporter)
	if !ok {
		return false
	}
	return reporter.Supports(capability)
}

// QueueType is the kind of queue to declare.
//
// Classic queues live on one node. Quorum queues are replicated and survive a
// node dying, at the cost of throughput and memory. Streams keep messages after
// they are read, so several consumers can each read from their own position.
type QueueType string

const (
	// QueueClassic is the default: one node, fastest, lost if that node is.
	QueueClassic QueueType = "classic"

	// QueueQuorum is replicated across nodes. What to use for anything that
	// must not be lost with a single machine.
	QueueQuorum QueueType = "quorum"

	// QueueStream keeps messages after reading. See the streams pattern.
	QueueStream QueueType = "stream"
)

// OfType declares a queue of a particular kind.
//
//	mq.DeclareQueue(ctx, "orders", acemq.OfType(acemq.QueueQuorum))
//
// A quorum queue must be durable and cannot be exclusive or auto-deleting;
// those are set here rather than left to fail at the broker with a message that
// does not mention quorum at all.
func OfType(t QueueType) QueueOption {
	return func(s *QueueSpec) {
		if s.Args == nil {
			s.Args = map[string]any{}
		}
		s.Args["x-queue-type"] = string(t)
		if t == QueueQuorum || t == QueueStream {
			s.Durable = true
			s.Exclusive = false
			s.AutoDelete = false
		}
	}
}

// Pulled is one message fetched without a consumer.
//
// [Conn.Pull] returns it. The message is held by the broker until it is
// acknowledged, exactly as a consumed one is.
type Pulled struct {
	// Envelope is the metadata.
	Envelope Envelope

	// Body is the undecoded payload.
	Body []byte

	// ContentType is what the sender said it was.
	ContentType string

	// RoutingKey is the key it arrived under.
	RoutingKey string

	// Redelivered is the broker saying it has handed this over before.
	Redelivered bool

	settle   func(bool, bool) error
	settled  sync.Once
	settleMu sync.Mutex
}

// Ack confirms the message and removes it from the queue.
func (p *Pulled) Ack() error { return p.settleOnce(true, false) }

// Nack returns the message to the queue.
func (p *Pulled) Nack(requeue bool) error { return p.settleOnce(false, requeue) }

func (p *Pulled) settleOnce(ack, requeue bool) error {
	var err error
	p.settled.Do(func() {
		p.settleMu.Lock()
		defer p.settleMu.Unlock()
		if p.settle != nil {
			err = p.settle(ack, requeue)
		}
	})
	return err
}

// Puller is a transport that can fetch one message on demand.
type Puller interface {
	// Pull fetches one message, or returns ok false when the queue is empty.
	Pull(ctx context.Context, queue string) (Delivery, bool, error)
}

// Pull fetches one message from a queue without starting a consumer.
//
// For a tool that drains a dead-letter queue, a test that wants one message, or
// a job that runs on a schedule rather than continuously. It is the wrong shape
// for ordinary work: polling a queue costs a round trip per message whether one
// is there or not, and [Consume] is both faster and kinder to the broker.
//
// The message is held until it is acknowledged. Losing the returned value
// without acknowledging it leaves the message unacknowledged until the
// connection closes.
func (c *Conn) Pull(ctx context.Context, queue string) (*Pulled, bool, error) {
	puller, ok := c.transport.(Puller)
	if !ok {
		return nil, false, Fatalf("acemq: the %T transport cannot pull messages", c.transport)
	}

	delivery, found, err := puller.Pull(ctx, queue)
	if err != nil || !found {
		return nil, found, err
	}

	return &Pulled{
		Envelope:    EnvelopeFromWire(delivery.Headers, delivery.RoutingKey, delivery.MessageID),
		Body:        delivery.Body,
		ContentType: delivery.ContentType,
		RoutingKey:  delivery.RoutingKey,
		Redelivered: delivery.Redelivered,
		settle: func(ack, requeue bool) error {
			if ack {
				if delivery.Ack != nil {
					return delivery.Ack()
				}
				return nil
			}
			if delivery.Nack != nil {
				return delivery.Nack(requeue)
			}
			return nil
		},
	}, true, nil
}

// PullInto fetches one message and decodes it with the connection's codec.
func PullInto[T any](ctx context.Context, conn *Conn, queue string) (*Pulled, T, bool, error) {
	var payload T

	pulled, found, err := conn.Pull(ctx, queue)
	if err != nil || !found {
		return nil, payload, found, err
	}
	if err := conn.codec.Decode(pulled.Body, &payload); err != nil {
		// Left unacknowledged deliberately: the caller decides whether a body
		// it cannot read should go back or be dropped.
		return pulled, payload, true, err
	}
	return pulled, payload, true, nil
}

// Version is this library's version, for logging what a service is running.
//
// Set at release time. It goes back to "dev" on the main branch after a
// release, so a build from source never claims to be a version somebody could
// look up.
const Version = "0.1.3"

var errNilInterceptor = errorString("acemq: the interceptor is nil")

type errorString string

func (e errorString) Error() string { return string(e) }
