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
	"errors"
	"fmt"
	"sync"

	"github.com/AceMQ-Company/acemq-go-amqp/security"
)

// Conn is a connection to a broker. It is safe for concurrent use.
type Conn struct {
	transport Transport
	codec     Codec
	origin    string
	retry     RetryPolicy
	prefetch  int
	observer  Observer
	onPublish []PublishInterceptor
	onConsume []ConsumeInterceptor

	mu     sync.Mutex
	closed bool
	subs   []*Consumer
}

// connConfig is what the options build up.
//
// The options configure this rather than the connection itself, because
// [WithSecurity] has to be known before the transport is dialled — by the time
// a *Conn exists, the handshake has already happened.
type connConfig struct {
	codec               Codec
	origin              string
	retry               RetryPolicy
	prefetch            int
	security            *security.Options
	observer            Observer
	publishInterceptors []PublishInterceptor
	consumeInterceptors []ConsumeInterceptor
}

func defaultConfig() connConfig {
	return connConfig{
		codec:    JSONCodec{},
		origin:   defaultOrigin(),
		prefetch: 20,
		observer: NopObserver{},
	}
}

// ConnOption configures a connection.
type ConnOption func(*connConfig) error

// WithCodec sets the codec used for payloads. The default is [JSONCodec].
func WithCodec(c Codec) ConnOption {
	return func(cfg *connConfig) error {
		if c == nil {
			return errors.New("acemq: WithCodec was given a nil codec")
		}
		cfg.codec = c
		return nil
	}
}

// WithOrigin sets the origin stamped on published messages, conventionally
// service@host. The default is acemq@{hostname}, which names the machine but
// not the service.
func WithOrigin(origin string) ConnOption {
	return func(cfg *connConfig) error { cfg.origin = origin; return nil }
}

// WithRetry sets the retry policy consumers use by default.
//
// Without one a message returned by [Retry] is republished onto its own queue at
// once, with its attempt counter advanced and nothing to stop it, and the broker
// will hand it back as fast as it can. That is rarely what anybody wants for
// long, so set a policy for anything that is not a toy.
//
// A policy with waits at or past its [RetryPolicy.BrokerWaitThreshold] needs its
// rung queues declared — see [Topology.Retries]. A consumer whose rungs are
// missing still retries, but waits in the process instead and counts
// [MetricRungMissing] each time.
func WithRetry(p RetryPolicy) ConnOption {
	return func(cfg *connConfig) error {
		if err := p.Validate(); err != nil {
			return err
		}
		cfg.retry = p
		return nil
	}
}

// WithPrefetch sets how many unacknowledged messages a consumer will hold.
func WithPrefetch(n int) ConnOption {
	return func(cfg *connConfig) error {
		if n < 0 {
			return fmt.Errorf("acemq: WithPrefetch must not be negative, got %d", n)
		}
		cfg.prefetch = n
		return nil
	}
}

// WithSecurity sets how the broker is reached: TLS mode, trusted authority,
// client certificate and credentials.
//
//	mq, err := acemq.Connect(ctx, "amqps://broker:5671/",
//		acemq.WithSecurity(security.Required().
//			TrustCertificateAuthorityFile("certs/ca.crt").
//			WithCredentials(security.EnvironmentCredentials("MQ_USER", "MQ_PASSWORD"))))
//
// Credentials given here override anything in the URL, which is how a password
// stays out of logs and process listings.
func WithSecurity(s *security.Options) ConnOption {
	return func(cfg *connConfig) error {
		if s == nil {
			return errors.New("acemq: WithSecurity was given no options")
		}
		if err := s.Err(); err != nil {
			return err
		}
		cfg.security = s
		return nil
	}
}

// Connect opens a connection to a broker.
//
// The URL's scheme picks the transport. memory:// is built in and needs no
// broker, which is what tests should use. amqp:// and amqps:// need the
// RabbitMQ transport, which registers itself when imported:
//
//	import _ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
//
// Every memory:// URL with a different host is a different broker, so tests
// that run in parallel can each have their own.
func Connect(ctx context.Context, url string, opts ...ConnOption) (*Conn, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	// Dialled after the options are read, because the security configuration
	// has to be in hand before the handshake rather than after it.
	transport, err := dialTransport(ctx, url, DialOptions{Security: cfg.security})
	if err != nil {
		return nil, err
	}
	return newConn(transport, cfg), nil
}

// NewConn wraps a transport you built yourself.
//
// Connect covers the ordinary case. This is for a transport that is not reached
// by a URL — a fake in a test, or one built with configuration a URL cannot
// carry.
func NewConn(transport Transport, opts ...ConnOption) (*Conn, error) {
	if transport == nil {
		return nil, errors.New("acemq: NewConn was given a nil transport")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.security != nil {
		// Silently ignoring it would leave somebody believing a connection was
		// verified when the transport they built themselves decided otherwise.
		return nil, errors.New(
			"acemq: WithSecurity has no effect on NewConn, because the transport is already " +
				"connected; configure security where the transport is built, such as rabbitmq.Dial")
	}
	return newConn(transport, cfg), nil
}

func newConn(transport Transport, cfg connConfig) *Conn {
	return &Conn{
		transport: transport,
		codec:     cfg.codec,
		origin:    cfg.origin,
		retry:     cfg.retry,
		prefetch:  cfg.prefetch,
		observer:  cfg.observer,
		onPublish: cfg.publishInterceptors,
		onConsume: cfg.consumeInterceptors,
	}
}

// Observer is where this connection reports what it does.
func (c *Conn) Observer() Observer { return c.observer }

var errNilObserver = errors.New("acemq: WithObserver was given no observer")

// Codec is the codec this connection publishes with.
func (c *Conn) Codec() Codec { return c.codec }

// DeclareQueue creates a durable quorum queue if it is not already there.
//
// Quorum, not classic, and that is the same default the Java, .NET, Python and
// Ruby libraries apply — a queue two services in two languages both declare has
// to be the same queue, and x-queue-type is compared as strictly as any other
// argument. Say [OfType] with [QueueClassic] for a queue that should not be
// replicated; a queue asked to be [Transient], [Exclusive] or [AutoDelete] is
// classic already, because RabbitMQ allows a quorum queue to be none of those,
// and a declaration that named its own type is left exactly as it reads.
//
// A queue that already exists as classic is not converted by this. The broker
// answers PRECONDITION_FAILED, because a queue's type cannot be changed after
// it is created; drain it and recreate it, or declare it [QueueClassic] here.
func (c *Conn) DeclareQueue(ctx context.Context, name string, opts ...QueueOption) error {
	spec := QueueSpec{Durable: true}
	for _, opt := range opts {
		opt(&spec)
	}
	quorumByDefault(&spec)
	return c.transport.DeclareQueue(ctx, name, spec)
}

// DeclareExchange creates a durable exchange if it is not already there.
func (c *Conn) DeclareExchange(ctx context.Context, name, kind string, opts ...ExchangeOption) error {
	spec := ExchangeSpec{Kind: kind, Durable: true}
	for _, opt := range opts {
		opt(&spec)
	}
	return c.transport.DeclareExchange(ctx, name, spec)
}

// Bind routes messages matching a routing key from an exchange to a queue.
func (c *Conn) Bind(ctx context.Context, queue, exchange, routingKey string) error {
	return c.transport.Bind(ctx, queue, exchange, routingKey)
}

// QueueOption adjusts how a queue is declared.
type QueueOption func(*QueueSpec)

// Transient declares a queue that does not survive a broker restart.
func Transient() QueueOption { return func(s *QueueSpec) { s.Durable = false } }

// AutoDelete removes the queue when its last consumer goes away.
func AutoDelete() QueueOption { return func(s *QueueSpec) { s.AutoDelete = true } }

// Exclusive limits the queue to this connection.
func Exclusive() QueueOption { return func(s *QueueSpec) { s.Exclusive = true } }

// QueueArg sets a broker-specific argument, such as x-dead-letter-exchange.
func QueueArg(name string, value any) QueueOption {
	return func(s *QueueSpec) {
		if s.Args == nil {
			s.Args = map[string]any{}
		}
		s.Args[name] = value
	}
}

// DeadLetterTo sends rejected and expired messages from this queue to an
// exchange of the caller's choosing.
//
// It is the way out of the arrangement [Topology.DeadLetters] sets up, and the
// two are mutually exclusive: DeadLetters writes x-dead-letter-exchange as
// acemq.dlx and x-dead-letter-routing-key as {queue}.dlq, and a topology that
// asks for both on one queue is refused rather than resolved, because either
// answer would be a guess about which of two conflicting instructions was
// meant. A service that wants its own dead-letter exchange uses this and leaves
// DeadLetters alone.
//
// It sets the exchange only. Nothing overrides the routing key, so a message
// arrives at that exchange under the key it was published with — which is what
// a caller pointing at an exchange of their own usually wants, and is why
// DeadLetters, which shares one exchange between every queue on the broker, has
// to set the key as well.
//
// Neither is how this library itself gives up on a message: a consumer that has
// run out of attempts republishes to {queue}.dlq with the reason attached and
// then acknowledges the original, so that the reason survives and the
// destination is one it chose. The broker's own dead-lettering is the backstop
// under that, for what the library never sees.
func DeadLetterTo(exchange string) QueueOption {
	return QueueArg(ArgDeadLetterExchange, exchange)
}

// ExchangeOption adjusts how an exchange is declared.
type ExchangeOption func(*ExchangeSpec)

// TransientExchange declares an exchange that does not survive a broker restart.
func TransientExchange() ExchangeOption { return func(s *ExchangeSpec) { s.Durable = false } }

// ExchangeArg sets a broker-specific argument.
func ExchangeArg(name string, value any) ExchangeOption {
	return func(s *ExchangeSpec) {
		if s.Args == nil {
			s.Args = map[string]any{}
		}
		s.Args[name] = value
	}
}

// Close stops every consumer on this connection and releases it.
//
// Consumers are closed first and their handlers allowed to finish, so a message
// being worked on when Close is called is acknowledged rather than returned to
// the queue for somebody else to redo.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	subs := c.subs
	c.subs = nil
	c.mu.Unlock()

	var errs []error
	for _, sub := range subs {
		if err := sub.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := c.transport.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (c *Conn) track(sub *Consumer) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("acemq: the connection is closed")
	}
	c.subs = append(c.subs, sub)
	return nil
}

func (c *Conn) untrack(sub *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, s := range c.subs {
		if s == sub {
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			return
		}
	}
}

// PublishRaw sends bytes that are already encoded, with headers already
// rendered.
//
// For the outbox relay and anything else replaying a message recorded earlier:
// the payload's Go type may not exist any more, and re-encoding it would
// produce different bytes from the ones that were committed.
//
// Ordinary publishing goes through [Publisher], which builds the envelope and
// encodes the payload.
func (c *Conn) PublishRaw(
	ctx context.Context, exchange, routingKey string, msg Outbound,
) (PublishResult, error) {
	return c.transport.Publish(ctx, exchange, routingKey, msg)
}
