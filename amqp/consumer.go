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
	retry     RetryPolicy
	ladder    RetryLadder
	transport Subscription
	work      chan Delivery
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	flight int64
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
	args        map[string]any
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

// ConsumeArg sets a broker-specific consumer argument.
//
// Needed for a stream, which says where to start reading with x-stream-offset,
// and for anything else a broker offers that this library does not name.
func ConsumeArg(name string, value any) ConsumeOption {
	return func(cfg *consumeConfig) {
		if cfg.args == nil {
			cfg.args = map[string]any{}
		}
		cfg.args[name] = value
	}
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
		conn:   conn,
		queue:  queue,
		retry:  cfg.retry,
		ladder: LadderFor(queue, cfg.retry),
		work:   make(chan Delivery, cfg.concurrency),
	}

	// The consumer's half of the topology, before the first delivery can arrive.
	//
	// {queue}.dlq and {queue}.parked are where this consumer puts a message it
	// gives up on, and it puts them there by publishing to a queue by name. A
	// publish to a queue nobody declared is unroutable, and an unroutable
	// message is discarded by the broker without a trace — so a service that
	// never applied its [Topology] would lose exactly the messages it had
	// already decided were worth keeping. Declaring here costs a few idempotent
	// declarations once per consumer and removes that path entirely. Java's
	// consumer has always done this; see [RetryLadder.Declare].
	//
	// It is refused rather than ignored on failure. A declaration this consumer
	// cannot make is one that disagrees with what is on the broker, and a
	// consumer that started anyway would be running against a topology it does
	// not understand.
	if err := c.ladder.Declare(ctx, conn); err != nil {
		return nil, err
	}

	if err := conn.track(c); err != nil {
		return nil, err
	}

	c.wg.Add(cfg.concurrency)
	for range cfg.concurrency {
		go func() {
			defer c.wg.Done()

			// One settlement hook per worker, and one context carrying it,
			// built here rather than per delivery: a worker handles one message
			// at a time, so the hook can be cleared and reused. Nothing on the
			// message path allocates for it, and a consumer nobody has
			// instrumented never calls through it. See [OnSettled].
			hook := &settlementHook{}
			hookCtx := withSettlementHook(ctx, hook)

			for d := range c.work {
				hook.reset()
				handleDelivery(c, hookCtx, d, handler, cfg, hook)
			}
		}()
	}

	sub, err := conn.transport.Consume(ctx, queue, ConsumeSpec{
		Prefetch: cfg.prefetch,
		Tag:      cfg.tag,
		Args:     cfg.args,
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
//
// hook is how anything the handler registered through [OnSettled] hears what
// was decided. Every path out of here settles exactly once, because a caller
// holding something that has to be finished — a span, most obviously — has
// nowhere else to hear that the message is done with.
func handleDelivery[T any](
	c *Consumer, ctx context.Context, d Delivery, handler Handler[T], cfg consumeConfig,
	hook *settlementHook,
) {
	// The attempt comes off the wire, because that is where a retry put it. An
	// earlier version counted redeliveries in a map on this consumer, which is
	// the only thing a requeue leaves to count: a requeue hands back the bytes
	// the broker was given, so the header still reads what the publisher wrote
	// however many times the message has come round. The map is per-process,
	// unbounded across a fleet, and empty again after the restart that the
	// failing service was about to have. Republishing instead of requeueing is
	// what lets the counter live on the message, where it belongs.
	env := envelopeFromDelivery(d)

	observer := c.conn.observer
	labels := map[string]string{TagQueue: c.queue}
	observer.Gauge(MetricInFlight, c.inFlight(1), labels)
	defer observer.Gauge(MetricInFlight, c.inFlight(-1), labels)

	if len(c.conn.onConsume) > 0 {
		cc := &ConsumeContext{
			Queue:       c.queue,
			Envelope:    &env,
			Body:        d.Body,
			ContentType: d.ContentType,
			Redelivered: d.Redelivered,
		}
		for _, intercept := range c.conn.onConsume {
			if err := intercept(ctx, cc); err != nil {
				// Refused before the handler ran. Dead-lettered rather than
				// retried, because an interceptor that says no will say no
				// again to the same message.
				reason := "an interceptor refused it: " + describe(err)
				c.deadLetter(ctx, d, env, reason)
				c.settled(hook, Settlement{
					Queue:    c.queue,
					Action:   SettledDeadLettered,
					Outcome:  OutcomeDeadLettered,
					Envelope: env,
					Reason:   reason,
				})
				return
			}
		}
	}

	var payload T
	if err := decodeWith(cfg.codec, d.ContentType, d.Body, &payload); err != nil {
		// A body that will not decode decodes no better next time, so it does not
		// go round the retry schedule until it ages out.
		//
		// Parked rather than dead-lettered: a message that failed five times and a
		// message nothing could read are different problems with different answers
		// — one is usually the world, the other is usually a producer — and
		// whoever drains the dead letters should not have to sort them by hand.
		reason := "could not be decoded: " + describe(err)
		c.park(ctx, d, env, reason)
		c.settled(hook, Settlement{
			Queue:    c.queue,
			Action:   SettledParked,
			Outcome:  OutcomeParked,
			Envelope: env,
			Reason:   reason,
		})
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

	// How long the handler took, kept until the engine has decided: the
	// duration is tagged with the outcome, and the outcome does not exist yet.
	// Read before the settling below rather than after, so a short backoff
	// waited in this process is not folded into "how long the handler took" —
	// which would make every retrying consumer look slow.
	took := time.Since(started)

	switch ack.action {
	case ackAccept:
		if d.Ack != nil {
			_ = d.Ack()
		}
		c.settledAfterHandler(hook, Settlement{
			Queue:    c.queue,
			Action:   SettledAccepted,
			Outcome:  OutcomeAcked,
			Envelope: env,
		}, took)

	case ackReject:
		// Dead-lettered, but counted and traced as rejected. Both end in the
		// dead-letter queue and only the word keeps them apart: one is a
		// decision somebody took about this message, the other is the engine
		// running out of room to try again.
		reason := "the handler rejected it: " + describe(ack.err)
		c.deadLetter(ctx, d, env, reason)
		c.settledAfterHandler(hook, Settlement{
			Queue:    c.queue,
			Action:   SettledDeadLettered,
			Outcome:  OutcomeRejected,
			Envelope: env,
			Reason:   reason,
		}, took)

	case ackPark:
		// The handler read far enough to know the message is unreadable. The
		// same destination and the same word as a body the codec could not
		// decode: whoever drains {queue}.parked is looking for the producer
		// either way, and which layer noticed is not their question.
		reason := "the handler parked it: " + describe(ack.err)
		c.park(ctx, d, env, reason)
		c.settledAfterHandler(hook, Settlement{
			Queue:    c.queue,
			Action:   SettledParked,
			Outcome:  OutcomeParked,
			Envelope: env,
			Reason:   reason,
		}, took)

	case ackRetry:
		if IsFatal(ack.err) {
			// The handler asked for a retry but marked the reason as one that
			// will not change. Honouring the mark rather than the request is
			// the point of having it.
			reason := "the handler reported an unprocessable message: " + describe(ack.err)
			c.deadLetter(ctx, d, env, reason)
			c.settledAfterHandler(hook, Settlement{
				Queue:    c.queue,
				Action:   SettledDeadLettered,
				Outcome:  OutcomeDeadLettered,
				Envelope: env,
				Reason:   reason,
			}, took)
			return
		}

		wait, again := c.retry.NextWait(env.Attempt, env.Age())
		if c.retry.MaxAttempts == 0 {
			// No policy configured: send it straight back and let the broker
			// decide how fast to bring it round. Documented on WithRetry as
			// rarely what anybody wants for long.
			wait, again = Wait{}, true
		}
		if !again {
			// The message asked for another attempt and there is none left.
			// This is the moment the two things a reader wants diverge: the
			// handler said retry, and what happened is a dead letter. Anything
			// that stops at the handler reports the first and never the second.
			reason := c.exhausted(env) + ": " + describe(ack.err)
			c.deadLetter(ctx, d, env, reason)
			c.settledAfterHandler(hook, Settlement{
				Queue:    c.queue,
				Action:   SettledDeadLettered,
				Outcome:  OutcomeDeadLettered,
				Envelope: env,
				Reason:   reason,
			}, took)
			return
		}

		// Said before the retry rather than after it, so the delay reported is
		// the one just chosen and not one already spent — and so a span covering
		// the handler is not held open across a wait the handler is not doing.
		c.settledAfterHandler(hook, Settlement{
			Queue:    c.queue,
			Action:   SettledRetried,
			Outcome:  OutcomeRetried,
			Envelope: env.NextAttempt(),
			Delay:    wait.Delay,
		}, took)

		if wait.InBroker && c.retryInBroker(ctx, d, env, wait.Delay) {
			return
		}

		if wait.Delay > 0 {
			// Waiting here holds the delivery, and so holds one of this
			// consumer's prefetch slots. For a wait of a few seconds that is the
			// cheaper of the two costs; for a longer one it is not, which is why
			// the policy has a threshold and the long waits went to a rung queue
			// a few lines above.
			select {
			case <-time.After(wait.Delay):
			case <-ctx.Done():
			}
		}
		c.retryAgain(ctx, d, env)
	}
}

// settled reports one delivery's fate to both of the places that have to agree
// about it: the counters, and whatever the handler registered through
// [OnSettled].
//
// One function rather than a pair of calls at each of the six paths out of
// [handleDelivery], because a path that reported to one and forgot the other is
// exactly how the counters and the spans came to say different things about the
// same message. Called after the delivery has physically been settled, so the
// outcome is what happened rather than what was about to be attempted.
func (c *Consumer) settled(hook *settlementHook, s Settlement) {
	observeConsume(c.conn.observer, s.Queue, s.Outcome)
	hook.settled(s)
}

// settledAfterHandler is [Consumer.settled] for a delivery that reached a
// handler, and records how long that took under the outcome the engine chose.
func (c *Consumer) settledAfterHandler(hook *settlementHook, s Settlement, took time.Duration) {
	observeHandler(c.conn.observer, s.Queue, s.Outcome, took)
	c.settled(hook, s)
}

// exhausted says why there is no next attempt, in words an operator can act on.
func (c *Consumer) exhausted(env Envelope) string {
	if env.Attempt >= c.retry.MaxAttempts {
		attempts := "attempts"
		if c.retry.MaxAttempts == 1 {
			attempts = "attempt"
		}
		return fmt.Sprintf("exhausted %d %s", c.retry.MaxAttempts, attempts)
	}
	return fmt.Sprintf("exceeded the maximum message age of %s", c.retry.MaxMessageAge)
}

// retryInBroker puts the message on a rung queue and lets the broker return it,
// reporting whether the rung took it.
//
// The rung's x-message-ttl is the delay and its dead-letter target is this
// queue, so the wait costs this process nothing: no delivery held, no prefetch
// slot spent, and — the reason it exists — nothing lost when this process
// restarts halfway through. A consumer sleeping on a five-minute backoff that
// dies at minute one does not resume at minute one; the broker redelivers the
// unacknowledged message immediately, and the policy that said five minutes
// delivers in none.
//
// The attempt advances here exactly as it does on an immediate retry: the
// counter belongs to the message, and a message that has been round the broker
// is no less on its second attempt than one that waited here.
//
// False means the rung is not on the broker, and the caller should fall back to
// waiting here. Degraded rather than fatal: the message is still deliverable,
// and waiting for it here is what this library did before there were rungs.
func (c *Consumer) retryInBroker(
	ctx context.Context, d Delivery, env Envelope, delay time.Duration,
) bool {
	rung, ok := c.ladder.RungFor(delay)
	if !ok {
		return false
	}

	routed, err := c.republish(ctx, d, rung, env.NextAttempt())
	if err != nil || !routed {
		// Counted rather than logged, because this library writes no log lines:
		// a topology that declares the queue without its rungs otherwise looks
		// like it works, right up until a long backoff quietly becomes a held
		// prefetch slot.
		c.conn.observer.Count(MetricRungMissing, 1, map[string]string{
			TagQueue: c.queue, TagRung: rung})
		return false
	}

	// Not counted as a retry here. The settlement above already counted this
	// message once, and counting it again under a rung label would make
	// acemq.messages.retried sum to twice the number of retries for every
	// consumer whose policy uses rungs. Which rung a wait went to is what
	// [MetricRungMissing] answers.
	if d.Ack != nil {
		_ = d.Ack()
	}
	return true
}

// retryAgain puts the message back on its own queue, one attempt further on.
//
// Republished rather than requeued, because a requeue returns the bytes the
// broker was given: the attempt header would still read what the publisher wrote
// however many times the message had come round, and the count would live only
// in this process's memory, which is the one place it is lost when the process
// that has been failing restarts.
//
// The cost is that the message goes to the back of the queue rather than the
// front, so a retry is no longer in order with its neighbours. For a message
// that has already failed once, that is the better trade.
func (c *Consumer) retryAgain(ctx context.Context, d Delivery, env Envelope) {
	routed, err := c.republish(ctx, d, c.queue, env.NextAttempt())
	if err != nil || !routed {
		// The queue this consumer reads has gone, or the connection has. Returned
		// to the broker rather than acknowledged, because dropping it here would
		// lose a message over a broker change nobody told this consumer about.
		c.nack(d, true)
		return
	}
	if d.Ack != nil {
		_ = d.Ack()
	}
}

// park sends the message to {queue}.parked with the reason attached.
//
// Where a message goes when it never reached the handler at all. Somebody has to
// look at it, and what they need to know first is that it was unreadable rather
// than unlucky.
func (c *Consumer) park(ctx context.Context, d Delivery, env Envelope, reason string) {
	c.setAside(ctx, d, env, ParkedQueue(c.queue), reason)
}

// deadLetter sends the message to {queue}.dlq with the reason attached.
func (c *Consumer) deadLetter(ctx context.Context, d Delivery, env Envelope, reason string) {
	c.setAside(ctx, d, env, DeadLetterQueue(c.queue), reason)
}

// setAside republishes to a queue with the reason recorded, then acknowledges
// the original.
//
// Acknowledging a message that failed looks wrong and is what makes this
// reliable: the message has already been safely republished somewhere else, so
// acknowledging the original is removing the copy that has been dealt with.
// Rejecting it instead would either requeue it into a hot loop or, with a
// dead-letter exchange configured on the queue, send it somewhere this consumer
// did not choose and without the reason.
//
// The reason travels as an envelope field, so a consumer of the dead-letter
// queue reads it back through the API rather than having to know the wire header
// name.
func (c *Consumer) setAside(ctx context.Context, d Delivery, env Envelope, target, reason string) {
	failed := env
	failed.Error = reason

	routed, err := c.republish(ctx, d, target, failed)
	if err != nil || !routed {
		// Rejected rather than acknowledged: without a queue to put it in, the
		// broker's own dead-lettering is the last thing left between this message
		// and nothing.
		c.conn.observer.Count(MetricSetAsideFailed, 1, map[string]string{
			TagQueue: c.queue, TagTarget: target})
		c.nack(d, false)
		return
	}
	if d.Ack != nil {
		_ = d.Ack()
	}
}

// republish sends the original bytes to a queue by name, and says whether they
// arrived.
//
// Through the default exchange, which routes to the queue whose name matches the
// routing key, and mandatory so that a queue that is not there is an answer
// rather than a silence. The body goes back exactly as it came: re-encoding
// through a type that has since changed would replace what was committed with
// something else.
func (c *Consumer) republish(
	ctx context.Context, d Delivery, queue string, env Envelope,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		// The consumer is shutting down. Publishing on a cancelled context would
		// fail anyway, and saying so here lets the caller give the message back
		// to the broker rather than lose it.
		return false, fmt.Errorf("acemq: cannot republish onto %q: %w", queue, err)
	}

	result, err := c.conn.PublishRaw(ctx, "", queue, Outbound{
		Body:        d.Body,
		ContentType: d.ContentType,
		MessageID:   env.ID,
		Headers:     env.ToWire(),
		ReplyTo:     env.ReplyTo,
		Persistent:  true,
		Mandatory:   true,
	})
	if err != nil {
		return false, fmt.Errorf("acemq: cannot republish onto %q: %w", queue, err)
	}
	return result.Routed, nil
}

// describe renders a failure as the sentence that goes on the message.
func describe(err error) string {
	if err == nil {
		return "no reason given"
	}
	return err.Error()
}

// decodeWith reads a body, letting a codec that chooses by content type see it.
func decodeWith(codec Codec, contentType string, body []byte, dst any) error {
	if chooser, ok := codec.(contentTypeDecoder); ok {
		return chooser.DecodeAs(contentType, body, dst)
	}
	return codec.Decode(body, dst)
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

// Close stops the consumer and waits for handlers already running.
//
// A message being worked on when Close is called is finished and settled, rather
// than abandoned for the broker to hand to somebody else.
//
// The subscription is released last, after everything has been settled, because
// a settlement travels on the channel its delivery arrived on. Releasing it
// first — which is what this did until the rung tests caught it — leaves every
// message in flight acknowledged into a channel that has gone: the broker hears
// nothing, hands the message to another consumer, and one that had already been
// republished for its next attempt is now on the queue twice. See [Stopper].
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		stopper, canStop := c.transport.(Stopper)
		if c.transport != nil && canStop {
			// No further deliveries once this returns, which is what makes
			// closing the work channel safe, but the channel is still open so
			// the handlers below can settle what they hold.
			c.closeErr = stopper.Stop()
		} else if c.transport != nil {
			c.closeErr = c.transport.Close()
		}

		close(c.work)
		c.wg.Wait()

		if c.transport != nil && canStop {
			if err := c.transport.Close(); err != nil && c.closeErr == nil {
				c.closeErr = err
			}
		}
		c.conn.untrack(c)
	})
	return c.closeErr
}
