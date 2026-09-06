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
	"sync"
	"sync/atomic"
	"time"
)

// Message is a delivery that has been decoded.
type Message[T any] struct {
	// Payload is the body, read through the codec.
	Payload T

	// Envelope is the metadata that travelled with it. Attempt is the count for
	// this delivery, which the engine works out; the rest came off the wire.
	Envelope Envelope

	// RoutingKey is the key the message arrived under.
	RoutingKey string

	// ContentType is what the sender said the body was.
	ContentType string

	// Redelivered is the broker saying it has handed this message over before.
	Redelivered bool

	// Body is the undecoded body, for a handler that wants to see the bytes.
	Body []byte
}

// Handler decides what happens to a message. Returning is the decision: see [Ack].
type Handler[T any] func(ctx context.Context, m Message[T]) Ack

// Consumer is a running subscription. Close it to stop.
type Consumer struct {
	conn      *Conn
	queue     string
	transport Subscription
	work      chan Delivery
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	attemptsMu sync.Mutex
	attempts   map[string]int
	flight     int64
}

// inFlight adjusts and returns how many messages this consumer is handling.
func (c *Consumer) inFlight(delta int64) int64 {
	return atomic.AddInt64(&c.flight, delta)
}

type consumeConfig struct {
	codec       Codec
	retry       RetryPolicy
	prefetch    int
	concurrency int
	tag         string
}

// ConsumeOption configures a consumer.
type ConsumeOption func(*consumeConfig)

// ConsumeWith uses a codec other than the connection's for this consumer.
func ConsumeWith(c Codec) ConsumeOption {
	return func(cfg *consumeConfig) { cfg.codec = c }
}

// RetryWith uses a retry policy other than the connection's for this consumer.
func RetryWith(p RetryPolicy) ConsumeOption {
	return func(cfg *consumeConfig) { cfg.retry = p }
}

// Prefetch sets how many unacknowledged messages this consumer will hold.
func Prefetch(n int) ConsumeOption {
	return func(cfg *consumeConfig) { cfg.prefetch = n }
}

// Concurrency sets how many messages this consumer works on at once.
//
// One by default, which keeps a queue's messages in order. Raising it gives up
// that order in exchange for throughput, and is the right trade for handlers
// that spend their time waiting on something else.
func Concurrency(n int) ConsumeOption {
	return func(cfg *consumeConfig) { cfg.concurrency = n }
}

// ConsumerTag names this consumer to the broker, which is what shows up in the
// management interface when somebody is working out who is holding a message.
func ConsumerTag(tag string) ConsumeOption {
	return func(cfg *consumeConfig) { cfg.tag = tag }
}

// Consume reads messages from a queue until the returned [Consumer] is closed.
//
//	sub, err := acemq.Consume(ctx, mq, "orders",
//		func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
//			if err := place(ctx, m.Payload); err != nil {
//				return acemq.Retry(err)
//			}
//			return acemq.Accept()
//		})
//	defer sub.Close()
//
// It is a function rather than a method on [Conn] for the same reason
// [NewPublisher] is: a method cannot have its own type parameter before Go
// 1.27. See the package documentation.
func Consume[T any](
	ctx context.Context, conn *Conn, queue string, handler Handler[T], opts ...ConsumeOption,
) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("acemq: Consume on %q was given a nil handler", queue)
	}

	cfg := consumeConfig{
		codec:       conn.codec,
		retry:       conn.retry,
		prefetch:    conn.prefetch,
		concurrency: 1,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.concurrency < 1 {
		return nil, fmt.Errorf("acemq: Concurrency must be at least 1, got %d", cfg.concurrency)
	}

	c := &Consumer{
		conn:     conn,
		queue:    queue,
		work:     make(chan Delivery, cfg.concurrency),
		attempts: map[string]int{},
	}

	if err := conn.track(c); err != nil {
		return nil, err
	}

	c.wg.Add(cfg.concurrency)
	for range cfg.concurrency {
		go func() {
			defer c.wg.Done()
			for d := range c.work {
				handleDelivery(c, ctx, d, handler, cfg)
			}
		}()
	}

	sub, err := conn.transport.Consume(ctx, queue, ConsumeSpec{
		Prefetch: cfg.prefetch,
		Tag:      cfg.tag,
	}, func(d Delivery) {
		c.work <- d
	})
	if err != nil {
		close(c.work)
		c.wg.Wait()
		conn.untrack(c)
		return nil, fmt.Errorf("acemq: cannot consume from %q: %w", queue, err)
	}
	c.transport = sub
	return c, nil
}

// handleDelivery runs one delivery through the codec, the handler and the retry
// policy.
//
// A function taking the consumer rather than a method on it, for the same
// reason [Consume] is: a generic method needs go1.27.
func handleDelivery[T any](
	c *Consumer, ctx context.Context, d Delivery, handler Handler[T], cfg consumeConfig,
) {
	env := EnvelopeFromWire(d.Headers, d.RoutingKey, d.MessageID)
	env.Attempt = c.attemptFor(env.ID, d.Redelivered)

	observer := c.conn.observer
	labels := map[string]string{"queue": c.queue}
	observer.Count(MetricConsumed, 1, labels)
	observer.Gauge(MetricInFlight, c.inFlight(1), labels)
	defer observer.Gauge(MetricInFlight, c.inFlight(-1), labels)

	var payload T
	if err := cfg.codec.Decode(d.Body, &payload); err != nil {
		// A body that will not decode decodes no better next time, so this is
		// dead-lettered rather than retried.
		observer.Count(MetricRejected, 1, labels)
		observer.Count(MetricDeadLettered, 1, labels)
		c.forget(env.ID)
		c.nack(d, false)
		return
	}

	started := time.Now()

	ack := callHandler(ctx, handler, Message[T]{
		Payload:     payload,
		Envelope:    env,
		RoutingKey:  d.RoutingKey,
		ContentType: d.ContentType,
		Redelivered: d.Redelivered,
		Body:        d.Body,
	})

	observeConsume(observer, c.queue, ack, time.Since(started))

	switch ack.action {
	case ackAccept:
		c.forget(env.ID)
		if d.Ack != nil {
			_ = d.Ack()
		}

	case ackReject:
		c.forget(env.ID)
		c.nack(d, false)

	case ackRetry:
		if IsFatal(ack.err) {
			// The handler asked for a retry but marked the reason as one that
			// will not change. Honouring the mark rather than the request is
			// the point of having it.
			c.forget(env.ID)
			c.nack(d, false)
			return
		}

		delay, again := cfg.retry.NextDelay(env.Attempt, env.Age())
		if cfg.retry.MaxAttempts == 0 {
			// No policy configured: requeue and let the broker decide how fast
			// to bring it back. Documented on WithRetry as rarely what anybody
			// wants for long.
			delay, again = 0, true
		}
		if !again {
			observer.Count(MetricDeadLettered, 1, labels)
			c.forget(env.ID)
			c.nack(d, false)
			return
		}
		if delay > 0 {
			// Waiting here holds the delivery, and so holds one of this
			// consumer's prefetch slots. That is the honest cost of delaying a
			// retry without a delay queue: the alternative is to acknowledge
			// and republish, which turns one message into two and loses the
			// broker's redelivery flag.
			select {
			case <-time.After(delay):
			case <-ctx.Done():
			}
		}
		c.nack(d, true)
	}
}

// callHandler runs the handler, turning a panic into a rejection rather than
// letting it take the worker down. A handler that panics is a bug, and a bug
// repeats, so the message is dead-lettered rather than retried.
func callHandler[T any](ctx context.Context, handler Handler[T], m Message[T]) (ack Ack) {
	defer func() {
		if r := recover(); r != nil {
			ack = Reject(Fatalf("acemq: the handler panicked on message %s: %v", m.Envelope.ID, r))
		}
	}()
	return handler(ctx, m)
}

func (c *Consumer) nack(d Delivery, requeue bool) {
	if d.Nack != nil {
		_ = d.Nack(requeue)
	}
}

// attemptFor works out which attempt this delivery is.
//
// The attempt header cannot answer: a broker requeues the bytes it was given,
// so the header still reads 1 however many times the message has come back.
// The redelivery flag is the only signal there is, and it is counted here,
// per consumer, keyed by message id.
func (c *Consumer) attemptFor(id string, redelivered bool) int {
	c.attemptsMu.Lock()
	defer c.attemptsMu.Unlock()

	if !redelivered {
		c.attempts[id] = 1
		return 1
	}
	n := c.attempts[id] + 1
	c.attempts[id] = n
	return n
}

// forget drops a message's attempt count once it is settled, so the map holds
// only what is in flight.
func (c *Consumer) forget(id string) {
	c.attemptsMu.Lock()
	defer c.attemptsMu.Unlock()
	delete(c.attempts, id)
}

// Close stops the consumer and waits for handlers already running.
//
// A message being worked on when Close is called is finished and acknowledged,
// rather than abandoned for the broker to hand to somebody else.
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		if c.transport != nil {
			// The transport guarantees no further deliveries once this returns,
			// which is what makes closing the work channel safe.
			c.closeErr = c.transport.Close()
		}
		close(c.work)
		c.wg.Wait()
		c.conn.untrack(c)
	})
	return c.closeErr
}
