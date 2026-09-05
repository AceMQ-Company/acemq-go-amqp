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
	"net/url"
	"sort"
	"sync"

	"github.com/AceMQ-Company/acemq-go-amqp/security"
)

// Transport is what a broker has to provide. The in-memory transport in this
// package and the RabbitMQ transport in the rabbitmq subpackage both implement
// it, and everything above this interface is shared between them.
type Transport interface {
	// DeclareQueue creates a queue if it is not already there.
	DeclareQueue(ctx context.Context, name string, spec QueueSpec) error

	// DeclareExchange creates an exchange if it is not already there.
	DeclareExchange(ctx context.Context, name string, spec ExchangeSpec) error

	// Bind routes messages from an exchange to a queue.
	Bind(ctx context.Context, queue, exchange, routingKey string) error

	// Publish sends one message and reports what the broker said about it.
	Publish(ctx context.Context, exchange, routingKey string, msg Outbound) (PublishResult, error)

	// Consume delivers messages to deliver until the returned Subscription is
	// closed. Each delivery arrives on its own goroutine or in sequence
	// depending on spec.Prefetch; the caller must not assume either.
	Consume(ctx context.Context, queue string, spec ConsumeSpec, deliver func(Delivery)) (Subscription, error)

	// Close releases the connection.
	Close() error
}

// Subscription is a running consumer. Closing it stops delivery and waits for
// handlers already in flight.
type Subscription interface {
	Close() error
}

// QueueSpec is how a queue should be declared.
type QueueSpec struct {
	// Durable survives a broker restart. True by default via [DeclareQueue].
	Durable bool

	// AutoDelete removes the queue when its last consumer goes away.
	AutoDelete bool

	// Exclusive limits the queue to the declaring connection.
	Exclusive bool

	// Args are broker-specific arguments, such as x-dead-letter-exchange.
	Args map[string]any
}

// ExchangeSpec is how an exchange should be declared.
type ExchangeSpec struct {
	// Kind is direct, topic, fanout or headers.
	Kind string

	// Durable survives a broker restart.
	Durable bool

	// AutoDelete removes the exchange when its last binding goes away.
	AutoDelete bool

	// Args are broker-specific arguments.
	Args map[string]any
}

// ConsumeSpec is how a consumer should be set up.
type ConsumeSpec struct {
	// Prefetch is how many unacknowledged messages the broker will send. Zero
	// means the transport's default.
	Prefetch int

	// Tag names the consumer to the broker. Generated when empty.
	Tag string
}

// PublishResult is what the broker said about a published message.
type PublishResult struct {
	// MessageID is the identifier the message was sent with.
	MessageID string

	// Confirmed is the broker saying it has taken responsibility for the
	// message. Without confirms this is false and nothing has been promised —
	// the message reached the socket, which is not the same thing.
	Confirmed bool

	// Routed is false when the message was published as mandatory and reached
	// no queue at all. A message nothing is bound to receive is not an error to
	// the broker; it is dropped, silently, which is exactly the failure worth
	// hearing about.
	//
	// Meaningless unless the publisher asked for [Mandatory].
	Routed bool

	// ReturnReason is the broker's explanation when Routed is false.
	ReturnReason string
}

// Outbound is a message on its way to the broker.
type Outbound struct {
	Body        []byte
	ContentType string
	MessageID   string
	Headers     map[string]any

	// Persistent asks the broker to write the message to disk. It is not a
	// guarantee on its own: a persistent message in a queue that is not durable
	// still dies with the broker.
	Persistent bool

	// Mandatory asks the broker to return the message rather than drop it when
	// no queue is bound to receive it.
	Mandatory bool
}

// Delivery is a message as it arrived, before any codec has looked at it.
type Delivery struct {
	Body        []byte
	ContentType string
	RoutingKey  string
	MessageID   string
	Headers     map[string]any

	// Redelivered is the broker saying it has handed this message over before.
	//
	// This is the only signal that a delivery is a retry. The attempt counter in
	// the headers cannot serve: a broker requeues the bytes it was given, so the
	// header still reads whatever the publisher wrote no matter how many times
	// the message has come round.
	Redelivered bool

	// Ack confirms the message and removes it from the queue.
	Ack func() error

	// Nack returns the message, requeued or not.
	Nack func(requeue bool) error
}

// DialOptions are what a transport needs that a URL cannot carry.
type DialOptions struct {
	// Security is how to reach the broker safely: TLS mode, trusted authority,
	// client certificate, credentials. Nil means the transport's own defaults,
	// which for RabbitMQ is TLS only if the URL says amqps and verification
	// against the system trust store.
	Security *security.Options
}

var (
	transportsMu sync.RWMutex
	transports   = map[string]DialFunc{}
)

// DialFunc opens a connection to a broker.
type DialFunc func(ctx context.Context, url string, opts DialOptions) (Transport, error)

// RegisterTransport associates a URL scheme with a way of dialling it.
//
// This is how the RabbitMQ transport reaches [Connect] without this package
// importing it, and so without every program that uses AceMQ compiling in an
// AMQP client it may not want. The subpackage registers itself, the same
// arrangement database/sql uses for its drivers:
//
//	import _ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
func RegisterTransport(scheme string, dial DialFunc) {
	transportsMu.Lock()
	defer transportsMu.Unlock()
	transports[scheme] = dial
}

// TransportSchemes lists the registered URL schemes, sorted.
func TransportSchemes() []string {
	transportsMu.RLock()
	defer transportsMu.RUnlock()
	schemes := make([]string, 0, len(transports))
	for s := range transports {
		schemes = append(schemes, s)
	}
	sort.Strings(schemes)
	return schemes
}

func dialTransport(ctx context.Context, rawURL string, opts DialOptions) (Transport, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("acemq: %q is not a usable broker URL: %w", rawURL, err)
	}

	transportsMu.RLock()
	dial, ok := transports[parsed.Scheme]
	transportsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf(
			"acemq: no transport is registered for %q; known schemes are %v. "+
				"For a broker, add the blank import "+
				"_ \"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq\"",
			parsed.Scheme, TransportSchemes())
	}
	return dial(ctx, rawURL, opts)
}
