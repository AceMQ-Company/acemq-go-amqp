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

// Package rabbitmq is the RabbitMQ transport for AceMQ.
//
// Importing it registers the amqp and amqps schemes, after which
// [github.com/AceMQ-Company/acemq-go-amqp/amqp.Connect] can reach a broker:
//
//	import (
//		acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
//		_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
//	)
//
//	mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
//
// The blank import is what keeps the AMQP client out of programs that only use
// the in-memory transport. For a connection that needs more than a URL can say
// — a private certificate authority, a client certificate — build the transport
// with [Dial] and hand it to acemq.NewConn.
package rabbitmq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	neturl "net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/security"
)

// applyCredentials puts the configured login into the URL.
//
// The client takes credentials from the URL and nowhere else, so this is where
// a password read from a file or an environment variable joins the connection.
// It replaces whatever the URL carried: a caller who configured a provider
// meant it to be used.
func applyCredentials(rawURL string, sec *security.Options) (string, error) {
	creds, present, err := sec.Credentials()
	if err != nil {
		return "", err
	}
	if !present {
		return rawURL, nil
	}

	parsed, err := neturl.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("acemq: %q is not a usable broker URL: %w", rawURL, err)
	}
	parsed.User = neturl.UserPassword(creds.Username(), creds.Secret())
	return parsed.String(), nil
}

// checkScheme refuses a combination that cannot mean what it says.
//
// Asking for TLS and then connecting to amqp:// produces a plaintext
// connection with no warning at all, which is the quietest possible way to
// lose the thing that was configured most deliberately.
func checkScheme(rawURL string, sec *security.Options) error {
	parsed, err := neturl.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("acemq: %q is not a usable broker URL: %w", rawURL, err)
	}
	switch {
	case sec.Mode() != security.ModeDisabled && parsed.Scheme == "amqp":
		return fmt.Errorf(
			"acemq: security is set to %s but the URL is amqp://, which is plaintext. "+
				"Use amqps://, or security.Disabled() if plaintext is what you meant",
			sec.Mode())
	case sec.Mode() == security.ModeDisabled && parsed.Scheme == "amqps":
		return fmt.Errorf(
			"acemq: security is disabled but the URL is amqps://, which is encrypted. " +
				"Use amqp://, or drop security.Disabled()")
	}
	return nil
}

func init() {
	acemq.RegisterTransport("amqp", dial)
	acemq.RegisterTransport("amqps", dial)
}

func dial(ctx context.Context, url string, opts acemq.DialOptions) (acemq.Transport, error) {
	return Dial(ctx, url, Config{Security: opts.Security})
}

// dialTimeout bounds both reaching the broker and the handshake that follows,
// matching the client's own default.
const dialTimeout = 30 * time.Second

// contextDialer connects with the caller's context honoured, which the client's
// default dialer cannot do — it takes a timeout and nothing else, so a
// cancelled context would not stop a connection attempt.
//
// The deadline is for the TLS and AMQP handshakes only. The client clears it
// once the connection is open, after which heartbeats do the work of noticing a
// dead peer; leaving it set would kill every long-lived connection after thirty
// seconds.
func contextDialer(ctx context.Context, timeout time.Duration) func(string, string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: timeout}
		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return conn, nil
	}
}

// Config is what a URL cannot carry.
type Config struct {
	// Security is how to reach the broker safely: TLS mode, trusted authority,
	// client certificate and credentials. Prefer it to TLS below, which it
	// overrides when both are given.
	Security *security.Options

	// TLS configures an amqps:// connection directly, for a caller who would
	// rather build the *tls.Config themselves. Nil means the client's default,
	// which verifies against the system roots.
	TLS *tls.Config

	// Name identifies this connection in RabbitMQ's management interface, which
	// is what somebody looks at when working out who is holding a message.
	Name string
}

// Transport is a connection to a RabbitMQ broker.
type Transport struct {
	conn *amqp.Connection

	// One channel for publishing and topology, guarded by a mutex. An AMQP
	// channel is not safe for concurrent use, and sharing one under a lock is
	// simpler to reason about than a pool for the traffic a publisher generates.
	mu      sync.Mutex
	channel *amqp.Channel

	subsMu sync.Mutex
	subs   []*subscription
	closed bool
}

// Dial opens a connection to a broker.
func Dial(ctx context.Context, url string, cfg ...Config) (*Transport, error) {
	var c Config
	if len(cfg) > 0 {
		c = cfg[0]
	}

	props := amqp.Table{"connection_name": c.Name}
	if c.Name == "" {
		props = amqp.Table{"product": "AceMQ for Go"}
	}

	tlsConfig := c.TLS
	if c.Security != nil {
		if err := c.Security.Err(); err != nil {
			return nil, err
		}
		built, err := c.Security.TLSConfig()
		if err != nil {
			return nil, err
		}
		// nil here means the security options asked for plaintext, and that has
		// to win over an inherited *tls.Config rather than being ignored.
		tlsConfig = built

		var err2 error
		url, err2 = applyCredentials(url, c.Security)
		if err2 != nil {
			return nil, err2
		}
		if err := checkScheme(url, c.Security); err != nil {
			return nil, err
		}
	}

	conn, err := amqp.DialConfig(url, amqp.Config{
		TLSClientConfig: tlsConfig,
		Properties:      props,
		Dial:            contextDialer(ctx, dialTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot reach the broker: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acemq: connected to the broker but cannot open a channel: %w", err)
	}

	return &Transport{conn: conn, channel: ch}, nil
}

// DeclareQueue creates a queue if it is not already there.
//
// A queue that exists with different settings is a mismatch the broker refuses
// with PRECONDITION_FAILED, and that refusal is passed on rather than
// swallowed: it means the code and the broker disagree about what the queue is,
// which is worth stopping for.
func (t *Transport) DeclareQueue(_ context.Context, name string, spec acemq.QueueSpec) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, err := t.channel.QueueDeclare(
		name, spec.Durable, spec.AutoDelete, spec.Exclusive, false, amqp.Table(spec.Args))
	if err != nil {
		return fmt.Errorf("acemq: cannot declare queue %q: %w", name, err)
	}
	return nil
}

// DeclareExchange creates an exchange if it is not already there.
func (t *Transport) DeclareExchange(_ context.Context, name string, spec acemq.ExchangeSpec) error {
	kind := spec.Kind
	if kind == "" {
		kind = "direct"
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.channel.ExchangeDeclare(
		name, kind, spec.Durable, spec.AutoDelete, false, false, amqp.Table(spec.Args))
	if err != nil {
		return fmt.Errorf("acemq: cannot declare exchange %q: %w", name, err)
	}
	return nil
}

// Bind routes messages from an exchange to a queue.
func (t *Transport) Bind(_ context.Context, queue, exchange, routingKey string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.channel.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return fmt.Errorf("acemq: cannot bind %q to %q: %w", queue, exchange, err)
	}
	return nil
}

// Publish sends one message.
func (t *Transport) Publish(
	ctx context.Context, exchange, routingKey string, msg acemq.Outbound,
) error {
	delivery := amqp.Transient
	if msg.Persistent {
		delivery = amqp.Persistent
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.channel.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		Body:         msg.Body,
		ContentType:  msg.ContentType,
		MessageId:    msg.MessageID,
		Headers:      amqp.Table(msg.Headers),
		DeliveryMode: delivery,
	})
	if err != nil {
		return fmt.Errorf("acemq: cannot publish to %q with key %q: %w", exchange, routingKey, err)
	}
	return nil
}

// Consume reads from a queue on a channel of its own.
func (t *Transport) Consume(
	_ context.Context, queue string, spec acemq.ConsumeSpec, deliver func(acemq.Delivery),
) (acemq.Subscription, error) {
	t.subsMu.Lock()
	if t.closed {
		t.subsMu.Unlock()
		return nil, fmt.Errorf("acemq: the connection is closed")
	}
	t.subsMu.Unlock()

	// A channel per consumer, so one consumer's prefetch and its cancellation
	// do not touch another's.
	ch, err := t.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot open a channel for %q: %w", queue, err)
	}

	prefetch := spec.Prefetch
	if prefetch <= 0 {
		prefetch = 20
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("acemq: cannot set prefetch on %q: %w", queue, err)
	}

	tag := spec.Tag
	if tag == "" {
		tag = "acemq-" + queue
	}

	// autoAck is false throughout: a message is acknowledged when the handler
	// says so and not before, which is the whole point of returning an Ack.
	deliveries, err := ch.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("acemq: cannot consume from %q: %w", queue, err)
	}

	sub := &subscription{channel: ch, tag: tag}
	sub.wg.Add(1)
	go sub.run(deliveries, deliver)

	t.subsMu.Lock()
	t.subs = append(t.subs, sub)
	t.subsMu.Unlock()
	return sub, nil
}

// Close stops every consumer and closes the connection.
func (t *Transport) Close() error {
	t.subsMu.Lock()
	if t.closed {
		t.subsMu.Unlock()
		return nil
	}
	t.closed = true
	subs := t.subs
	t.subs = nil
	t.subsMu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}

	t.mu.Lock()
	if t.channel != nil {
		_ = t.channel.Close()
	}
	t.mu.Unlock()

	return t.conn.Close()
}

type subscription struct {
	channel   *amqp.Channel
	tag       string
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func (s *subscription) run(deliveries <-chan amqp.Delivery, deliver func(acemq.Delivery)) {
	defer s.wg.Done()

	for d := range deliveries {
		msg := d
		deliver(acemq.Delivery{
			Body:        msg.Body,
			ContentType: msg.ContentType,
			RoutingKey:  msg.RoutingKey,
			MessageID:   msg.MessageId,
			Headers:     map[string]any(msg.Headers),
			Redelivered: msg.Redelivered,
			Ack: func() error {
				return msg.Ack(false)
			},
			Nack: func(requeue bool) error {
				return msg.Nack(false, requeue)
			},
		})
	}
}

// Close cancels the consumer and waits for the deliveries already dispatched.
//
// Cancelling closes the delivery channel once the broker has acknowledged it,
// which ends the loop above; waiting on that is what lets the caller rely on no
// further deliveries once this returns.
func (s *subscription) Close() error {
	s.closeOnce.Do(func() {
		if err := s.channel.Cancel(s.tag, false); err != nil {
			// The channel may already be gone, in which case the loop has
			// ended anyway and there is nothing to report.
			s.closeErr = nil
		}
		s.wg.Wait()
		if err := s.channel.Close(); err != nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}
