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
	"errors"
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

	// WithoutConfirms turns publisher confirms off.
	//
	// They are on by default. Turning them off makes publishing faster and
	// makes a successful publish mean nothing more than "the bytes left this
	// process", so it is worth doing only where losing a message costs less
	// than the round trip.
	WithoutConfirms bool

	// WithoutRecovery stops the transport reconnecting when the connection
	// drops.
	//
	// Recovery is on by default because the alternative is a service that
	// silently stops consuming and says nothing: the delivery channel closes,
	// the consumer goroutine ends, and the object still looks alive. Turn it
	// off only where something outside the process is expected to restart it.
	WithoutRecovery bool

	// RecoveryDelay is how long to wait between attempts to reconnect. One
	// second by default, doubling to at most thirty.
	RecoveryDelay time.Duration

	// OnRecovery is told about every connection loss and every attempt to come
	// back, so a service can log or alert on it. Recovery that nobody can see
	// is only half an improvement on dying quietly.
	OnRecovery func(RecoveryEvent)
}

// RecoveryEvent is something that happened to the connection.
type RecoveryEvent struct {
	// Kind is "lost", "retrying", "recovered" or "gave-up".
	Kind string

	// Attempt counts from 1 for each reconnection.
	Attempt int

	// Err is why the connection went, or why an attempt failed.
	Err error
}

func (e RecoveryEvent) String() string {
	if e.Err == nil {
		return fmt.Sprintf("connection %s (attempt %d)", e.Kind, e.Attempt)
	}
	return fmt.Sprintf("connection %s (attempt %d): %v", e.Kind, e.Attempt, e.Err)
}

// Transport is a connection to a RabbitMQ broker.
type Transport struct {
	// url and cfg are kept so the connection can be made again. A transport
	// that cannot redial is a transport that dies with the first network
	// hiccup.
	url string
	cfg Config

	conn *amqp.Connection

	// One channel for publishing and topology, guarded by a mutex. An AMQP
	// channel is not safe for concurrent use, and sharing one under a lock is
	// simpler to reason about than a pool for the traffic a publisher generates.
	mu      sync.Mutex
	channel *amqp.Channel

	// returns carries messages the broker could not route. It is drained after
	// a confirm rather than watched, because the protocol guarantees a return
	// arrives before the confirm for the same publish.
	returns    chan amqp.Return
	confirming bool

	subsMu sync.Mutex
	subs   []*subscription
	closed bool

	// The topology this transport declared, replayed after a reconnection.
	// The broker may have restarted and lost everything that was not durable,
	// and a consumer reattached to a queue that no longer exists receives
	// nothing for ever.
	topoMu    sync.Mutex
	queues    []queueDecl
	exchanges []exchangeDecl
	binds     []bindDecl

	recovering sync.WaitGroup
	stopRecov  chan struct{}
	recovOnce  sync.Once

	// blocked is the broker's last connection.blocked, if it has not been
	// followed by an unblock. Publishing while blocked would hang rather than
	// fail, which looks identical to a wedged process.
	blockedMu sync.RWMutex
	blocked   string
}

// watchBlocked records the broker asking publishers to stop.
//
// RabbitMQ sends connection.blocked when it is low on memory or disk. Every
// publish then blocks until it unblocks, so a publisher that ignores this looks
// exactly like one that has hung — and the usual response, restarting the
// service, does not help.
func (t *Transport) watchBlocked(blockings <-chan amqp.Blocking) {
	for b := range blockings {
		t.blockedMu.Lock()
		if b.Active {
			t.blocked = b.Reason
		} else {
			t.blocked = ""
		}
		t.blockedMu.Unlock()

		if b.Active {
			t.report(RecoveryEvent{Kind: "blocked", Err: &acemq.ConnectionBlockedError{Reason: b.Reason}})
		} else {
			t.report(RecoveryEvent{Kind: "unblocked"})
		}
	}
}

// BlockedReason is the broker's reason for blocking this connection, or empty
// when it is not blocked.
func (t *Transport) BlockedReason() string {
	t.blockedMu.RLock()
	defer t.blockedMu.RUnlock()
	return t.blocked
}

type queueDecl struct {
	name string
	spec acemq.QueueSpec
}

type exchangeDecl struct {
	name string
	spec acemq.ExchangeSpec
}

type bindDecl struct {
	queue, exchange, routingKey string
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

	// Publisher confirms, unless asked not to. Without them a publish that
	// returns nil means the bytes reached the socket, which is not the same as
	// the broker having taken responsibility for them — and the difference is
	// only ever noticed after messages have been lost.
	transport := &Transport{url: url, cfg: c, conn: conn, channel: ch, stopRecov: make(chan struct{})}
	if !c.WithoutConfirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, fmt.Errorf("acemq: the broker will not enable publisher confirms: %w", err)
		}
		transport.confirming = true
		transport.returns = ch.NotifyReturn(make(chan amqp.Return, 64))
	}

	go transport.watchBlocked(conn.NotifyBlocked(make(chan amqp.Blocking, 4)))

	if !c.WithoutRecovery {
		transport.recovering.Add(1)
		go transport.recover(conn.NotifyClose(make(chan *amqp.Error, 1)))
	}
	return transport, nil
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
	t.remember(func() { t.queues = append(t.queues, queueDecl{name: name, spec: spec}) })
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
	spec.Kind = kind
	t.remember(func() { t.exchanges = append(t.exchanges, exchangeDecl{name: name, spec: spec}) })
	return nil
}

// Bind routes messages from an exchange to a queue.
func (t *Transport) Bind(_ context.Context, queue, exchange, routingKey string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.channel.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return fmt.Errorf("acemq: cannot bind %q to %q: %w", queue, exchange, err)
	}
	t.remember(func() {
		t.binds = append(t.binds, bindDecl{queue: queue, exchange: exchange, routingKey: routingKey})
	})
	return nil
}

// remember records a declaration under the topology lock.
func (t *Transport) remember(record func()) {
	t.topoMu.Lock()
	defer t.topoMu.Unlock()
	record()
}

// Publish sends one message.
func (t *Transport) Publish(
	ctx context.Context, exchange, routingKey string, msg acemq.Outbound,
) (acemq.PublishResult, error) {
	delivery := amqp.Transient
	if msg.Persistent {
		delivery = amqp.Persistent
	}

	publishing := amqp.Publishing{
		Body:         msg.Body,
		ContentType:  msg.ContentType,
		MessageId:    msg.MessageID,
		Headers:      amqp.Table(msg.Headers),
		DeliveryMode: delivery,
	}
	result := acemq.PublishResult{MessageID: msg.MessageID, Routed: true}

	// Refused rather than blocked. A publish that hangs indefinitely is
	// indistinguishable from a wedged process, and a service that knows it can
	// shed load, buffer, or fail the request instead.
	if reason := t.BlockedReason(); reason != "" {
		return result, &acemq.PublishingPausedError{Reason: reason}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.confirming {
		err := t.channel.PublishWithContext(
			ctx, exchange, routingKey, msg.Mandatory, false, publishing)
		if err != nil {
			return result, &acemq.PublishFailedError{
				MessageID: msg.MessageID, Exchange: exchange, RoutingKey: routingKey, Err: err}
		}
		// Nothing was promised, so nothing is claimed. Routed stays true
		// because without a confirm there is no moment at which a return is
		// known to have arrived.
		return result, nil
	}

	confirmation, err := t.channel.PublishWithDeferredConfirmWithContext(
		ctx, exchange, routingKey, msg.Mandatory, false, publishing)
	if err != nil {
		return result, &acemq.PublishFailedError{
			MessageID: msg.MessageID, Exchange: exchange, RoutingKey: routingKey, Err: err}
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return result, fmt.Errorf(
			"acemq: waiting for the broker to confirm message %s: %w", msg.MessageID, err)
	}
	if !acked {
		// A nack is the broker saying it could not take the message. Retrying
		// is the caller's decision; losing it quietly is not on offer.
		return result, &acemq.PublishFailedError{
			MessageID: msg.MessageID, Exchange: exchange, RoutingKey: routingKey,
			Err: errors.New("the broker refused it")}
	}
	result.Confirmed = true

	if msg.Mandatory {
		// The protocol sends basic.return before the confirm for the same
		// publish, so by now any return is already in the channel and this
		// does not need to wait for one.
		if reason, returned := t.takeReturn(msg.MessageID); returned {
			result.Routed = false
			result.ReturnReason = reason
		}
	}
	return result, nil
}

// takeReturn looks for a return matching a message just confirmed.
//
// Drained rather than waited on: the broker sends basic.return before the
// confirm, so anything that was coming has arrived. Returns for other messages
// are put back, because a publisher sharing this channel is entitled to its own.
func (t *Transport) takeReturn(messageID string) (string, bool) {
	var others []amqp.Return
	defer func() {
		for _, r := range others {
			select {
			case t.returns <- r:
			default:
				// The buffer is full and this return is for a message nobody
				// asked about. Dropping it loses a diagnostic, not a message.
			}
		}
	}()

	for {
		select {
		case r, ok := <-t.returns:
			if !ok {
				return "", false
			}
			if r.MessageId == messageID {
				return fmt.Sprintf("%d %s", r.ReplyCode, r.ReplyText), true
			}
			others = append(others, r)
		default:
			return "", false
		}
	}
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

	sub := &subscription{
		queue:   queue,
		spec:    spec,
		deliver: deliver,
		channel: ch,
		tag:     tag,
	}
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

	// Stopped before the connection goes, so the recovery loop sees a
	// deliberate close rather than treating it as an outage to reconnect from.
	t.recovOnce.Do(func() { close(t.stopRecov) })

	for _, sub := range subs {
		_ = sub.Close()
	}
	t.recovering.Wait()

	t.mu.Lock()
	if t.channel != nil {
		_ = t.channel.Close()
	}
	t.mu.Unlock()

	return t.conn.Close()
}

type subscription struct {
	// Everything needed to set this consumer up again on a new connection.
	queue   string
	spec    acemq.ConsumeSpec
	deliver func(acemq.Delivery)

	mu      sync.Mutex
	channel *amqp.Channel
	tag     string

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
	closed    bool
}

// reattach consumes again on a freshly reconnected transport.
//
// The old channel is gone with the connection, so there is nothing to close;
// the goroutine reading its deliveries has already ended, because the client
// closes that channel when the connection drops.
func (s *subscription) reattach(_ context.Context, t *Transport) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	queue, spec, deliver, tag := s.queue, s.spec, s.deliver, s.tag
	s.mu.Unlock()

	t.mu.Lock()
	conn := t.conn
	t.mu.Unlock()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("acemq: cannot reopen a channel for %q after reconnecting: %w", queue, err)
	}

	prefetch := spec.Prefetch
	if prefetch <= 0 {
		prefetch = 20
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("acemq: cannot set prefetch on %q after reconnecting: %w", queue, err)
	}

	deliveries, err := ch.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("acemq: cannot consume from %q after reconnecting: %w", queue, err)
	}

	s.mu.Lock()
	s.channel = ch
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run(deliveries, deliver)
	return nil
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
		s.mu.Lock()
		s.closed = true
		ch, tag := s.channel, s.tag
		s.mu.Unlock()

		if err := ch.Cancel(tag, false); err != nil {
			// The channel may already be gone — a connection that dropped
			// takes it — in which case the loop has ended anyway and there is
			// nothing to report.
			s.closeErr = nil
		}
		s.wg.Wait()

		// Read again: a recovery may have replaced the channel between the
		// cancel and the wait.
		s.mu.Lock()
		current := s.channel
		s.mu.Unlock()
		if err := current.Close(); err != nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// CheckQueue reports whether the broker already agrees about a queue.
//
// AMQP has no way to ask what a queue looks like — the management API is a
// separate HTTP service. The only question the protocol offers is a
// declaration, and the broker answers PRECONDITION_FAILED when the queue
// exists with different settings.
//
// A failed declaration kills the channel it was made on, so this uses one of
// its own and throws it away. Doing it on the shared publishing channel would
// take the connection's publishing with it, which is a high price for a
// question.
//
// A queue that does not exist is created by this, because a declaration is the
// only question available. That makes it safe before applying a topology and
// not a read-only operation.
func (t *Transport) CheckQueue(_ context.Context, name string, spec acemq.QueueSpec) error {
	ch, err := t.conn.Channel()
	if err != nil {
		return fmt.Errorf("acemq: cannot open a channel to check queue %q: %w", name, err)
	}
	defer func() {
		// Already dead if the declaration failed; closing again is harmless and
		// closing a survivor is necessary.
		_ = ch.Close()
	}()

	_, err = ch.QueueDeclare(
		name, spec.Durable, spec.AutoDelete, spec.Exclusive, false, amqp.Table(spec.Args))
	if err != nil {
		return fmt.Errorf("the broker refused the declaration: %w", err)
	}
	return nil
}

// recover reconnects when the connection drops, and puts everything back.
//
// Without this a dropped connection is the quietest failure in the library: the
// delivery channel closes, every consumer goroutine ends, and the Consumer
// objects still look alive. The service consumes nothing, for ever, and says
// nothing about it. That is the failure this whole library exists to argue
// against, so leaving it in the transport would have been indefensible.
func (t *Transport) recover(closed chan *amqp.Error) {
	defer t.recovering.Done()

	for {
		var reason *amqp.Error
		select {
		case reason = <-closed:
		case <-t.stopRecov:
			return
		}

		t.subsMu.Lock()
		deliberate := t.closed
		t.subsMu.Unlock()
		if deliberate || reason == nil {
			// Closed on purpose. Nothing to recover from.
			return
		}

		t.report(RecoveryEvent{Kind: "lost", Err: reason})

		next, err := t.reconnect()
		if err != nil {
			t.report(RecoveryEvent{Kind: "gave-up", Err: err})
			return
		}
		closed = next
	}
}

// reconnect dials until it succeeds, rebuilds the topology and restarts every
// consumer. It returns the close channel of the new connection.
func (t *Transport) reconnect() (chan *amqp.Error, error) {
	delay := t.cfg.RecoveryDelay
	if delay <= 0 {
		delay = time.Second
	}
	const maxDelay = 30 * time.Second

	for attempt := 1; ; attempt++ {
		select {
		case <-t.stopRecov:
			return nil, errors.New("acemq: the transport was closed while reconnecting")
		case <-time.After(delay):
		}

		t.report(RecoveryEvent{Kind: "retrying", Attempt: attempt})

		closed, err := t.reconnectOnce()
		if err == nil {
			t.report(RecoveryEvent{Kind: "recovered", Attempt: attempt})
			return closed, nil
		}
		t.report(RecoveryEvent{Kind: "retrying", Attempt: attempt, Err: err})

		// Backed off rather than hammering a broker that is probably still
		// starting up, and capped so recovery after a long outage is not
		// half an hour late.
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

func (t *Transport) reconnectOnce() (chan *amqp.Error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	fresh, err := Dial(ctx, t.url, Config{
		Security:        t.cfg.Security,
		TLS:             t.cfg.TLS,
		Name:            t.cfg.Name,
		WithoutConfirms: t.cfg.WithoutConfirms,
		// The replacement must not start a recovery loop of its own; this one
		// takes over its connection and keeps watching.
		WithoutRecovery: true,
	})
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.conn = fresh.conn
	t.channel = fresh.channel
	t.returns = fresh.returns
	t.confirming = fresh.confirming
	t.mu.Unlock()

	if err := t.redeclare(ctx); err != nil {
		_ = fresh.conn.Close()
		return nil, err
	}
	if err := t.restartConsumers(ctx); err != nil {
		_ = fresh.conn.Close()
		return nil, err
	}

	return fresh.conn.NotifyClose(make(chan *amqp.Error, 1)), nil
}

// redeclare puts the topology back.
//
// A broker that restarted has lost everything that was not durable, and a
// consumer reattached to a queue that no longer exists receives nothing for
// ever — which looks exactly like a quiet queue.
func (t *Transport) redeclare(ctx context.Context) error {
	t.topoMu.Lock()
	exchanges := append([]exchangeDecl(nil), t.exchanges...)
	queues := append([]queueDecl(nil), t.queues...)
	binds := append([]bindDecl(nil), t.binds...)
	t.topoMu.Unlock()

	// Declared through the channel directly, because the public methods record
	// what they declare and would grow these lists on every recovery.
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, e := range exchanges {
		kind := e.spec.Kind
		if kind == "" {
			kind = "direct"
		}
		err := t.channel.ExchangeDeclare(
			e.name, kind, e.spec.Durable, e.spec.AutoDelete, false, false, amqp.Table(e.spec.Args))
		if err != nil {
			return fmt.Errorf("acemq: cannot redeclare exchange %q after reconnecting: %w", e.name, err)
		}
	}
	for _, q := range queues {
		_, err := t.channel.QueueDeclare(
			q.name, q.spec.Durable, q.spec.AutoDelete, q.spec.Exclusive, false, amqp.Table(q.spec.Args))
		if err != nil {
			return fmt.Errorf("acemq: cannot redeclare queue %q after reconnecting: %w", q.name, err)
		}
	}
	for _, b := range binds {
		if err := t.channel.QueueBind(b.queue, b.routingKey, b.exchange, false, nil); err != nil {
			return fmt.Errorf("acemq: cannot rebind %q after reconnecting: %w", b.queue, err)
		}
	}
	_ = ctx
	return nil
}

// restartConsumers reattaches every subscription to the new connection.
func (t *Transport) restartConsumers(ctx context.Context) error {
	t.subsMu.Lock()
	subs := append([]*subscription(nil), t.subs...)
	t.subsMu.Unlock()

	for _, sub := range subs {
		if err := sub.reattach(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transport) report(event RecoveryEvent) {
	if t.cfg.OnRecovery != nil {
		t.cfg.OnRecovery(event)
	}
}

// Supports says what this transport can do against RabbitMQ.
//
// Delayed delivery needs a plugin, and asking the broker whether it has one
// costs a management-API call this transport does not make, so it is reported
// as absent. Better to say no and be wrong than to say yes and have messages
// arrive immediately.
func (t *Transport) Supports(c acemq.Capability) bool {
	switch c {
	case acemq.CapabilityPublisherConfirms:
		return t.confirming
	case acemq.CapabilityRecovery:
		return !t.cfg.WithoutRecovery
	case acemq.CapabilityDeadLettering,
		acemq.CapabilityQuorumQueues,
		acemq.CapabilityStreams,
		acemq.CapabilityPriority:
		return true
	default:
		return false
	}
}

// Pull fetches one message with basic.get, without starting a consumer.
func (t *Transport) Pull(_ context.Context, queue string) (acemq.Delivery, bool, error) {
	t.mu.Lock()
	ch := t.channel
	t.mu.Unlock()

	msg, found, err := ch.Get(queue, false)
	if err != nil {
		return acemq.Delivery{}, false, acemq.Transportf(err, "cannot pull from %q", queue)
	}
	if !found {
		return acemq.Delivery{}, false, nil
	}

	delivery := msg
	return acemq.Delivery{
		Body:        delivery.Body,
		ContentType: delivery.ContentType,
		RoutingKey:  delivery.RoutingKey,
		MessageID:   delivery.MessageId,
		Headers:     map[string]any(delivery.Headers),
		Redelivered: delivery.Redelivered,
		Ack:         func() error { return delivery.Ack(false) },
		Nack:        func(requeue bool) error { return delivery.Nack(false, requeue) },
	}, true, nil
}
