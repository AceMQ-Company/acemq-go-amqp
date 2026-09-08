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
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// The names the scheduler owns on the broker.
//
// They are a cross-language contract rather than a preference. Java, .NET,
// Python, Ruby and this library all declare these objects with these names and
// these arguments, so a Go service and a Java service scheduling against one
// broker have to agree on every one of them: a queue redeclared with a
// different argument table is answered PRECONDITION_FAILED and the second
// service cannot start at all.
const (
	// ScheduleExchange is the direct exchange every rung and the control queue
	// are bound to.
	ScheduleExchange = "acemq.schedule"

	// ScheduleControlQueue is where a rung dead-letters an expired message, and
	// the only queue the scheduler consumes.
	ScheduleControlQueue = "acemq.schedule.due"
)

// The headers a scheduled message carries.
//
// Deliberately not the reserved x-acemq- prefix. That one is the engine's:
// [acemq.IsAceHeader] matches it, [acemq.Header] refuses it, and
// [acemq.EnvelopeFromWire] drops every header carrying it from the
// application's view on the way in, because engine headers are materialised as
// envelope fields instead. A scheduler header using it would be written on
// publish and gone on consume.
const (
	// HeaderScheduleExchange is the exchange the message is eventually for.
	HeaderScheduleExchange = "x-schedule-exchange"

	// HeaderScheduleRoutingKey is the routing key it will eventually carry.
	HeaderScheduleRoutingKey = "x-schedule-routing-key"

	// HeaderScheduleDueAt is when it is due, as milliseconds since the Unix
	// epoch — the same integer Java writes with Instant.toEpochMilli. A string
	// would have to agree on a format as well as a value, and this one has no
	// format to disagree about.
	HeaderScheduleDueAt = "x-schedule-due-at"

	// HeaderScheduleContentType is what the payload was encoded as when it was
	// scheduled.
	//
	// Carried because the scheduler republishes bytes rather than values, and a
	// consumer picks its codec from the content type. Publishing pre-encoded
	// bytes under application/octet-stream produces a message the intended
	// consumer cannot decode — it arrives, it is the right bytes, and nothing
	// can read it.
	HeaderScheduleContentType = "x-schedule-content-type"
)

// ScheduledMessageType is the envelope type a scheduled message travels under,
// both between the rungs and on the delivery at the end.
const ScheduledMessageType = "ScheduledMessage"

// schedulePrefetch is how many expired messages the control consumer holds.
// Fifty, as in Java: enough that a burst of expiries is not one round trip
// each, small enough that a restart redelivers a handful rather than a backlog.
const schedulePrefetch = 50

// scheduleRungs is the ladder, longest first.
//
// Five of them, spanning a second to an hour. More rungs mean finer accuracy
// and more queues; fewer mean more hops for a long delay. This spread delivers
// a one-day message in twenty-four hops and a one-minute message in one, which
// is the right way round.
var scheduleRungs = []time.Duration{
	time.Hour,
	10 * time.Minute,
	time.Minute,
	10 * time.Second,
	time.Second,
}

// ScheduleRungs lists the rungs, longest first.
func ScheduleRungs() []time.Duration {
	return append([]time.Duration(nil), scheduleRungs...)
}

// ScheduleRungName is the queue a rung of this length is held in.
//
// The rendering is part of the contract: a duration divisible by an hour reads
// as {n}h, one divisible by a minute as {n}m, and anything else as {n}s. So the
// five rungs are acemq.schedule.1h, .10m, .1m, .10s and .1s.
func ScheduleRungName(rung time.Duration) string {
	return ScheduleExchange + "." + describeRung(rung)
}

func describeRung(rung time.Duration) string {
	millis := rung.Milliseconds()
	if millis%3_600_000 == 0 {
		return fmt.Sprintf("%dh", millis/3_600_000)
	}
	if millis%60_000 == 0 {
		return fmt.Sprintf("%dm", millis/60_000)
	}
	return fmt.Sprintf("%ds", millis/1_000)
}

// ScheduleRungArgs is the argument table a rung queue must be declared with.
//
// x-message-ttl and never a per-message expiration, for the reason
// [acemq.RungArgs] gives about the retry ladder and for the same reason here:
// RabbitMQ expires messages only from the head of a classic queue, so one queue
// holding mixed delays releases nothing while a long one sits at the front. A
// four-hour message in front of a one-minute message delivers the one-minute
// message in four hours, and nothing reports it. Every message in a rung has
// the same delay, so the head is always the one due soonest, and head-of-line
// expiry is harmless.
func ScheduleRungArgs(rung time.Duration) map[string]any {
	return map[string]any{
		acemq.ArgMessageTTL:           rung.Milliseconds(),
		acemq.ArgDeadLetterExchange:   ScheduleExchange,
		acemq.ArgDeadLetterRoutingKey: ScheduleControlQueue,
	}
}

// ScheduleTopology is everything the scheduler needs on the broker.
//
// [NewScheduler] applies it. It is exported so that a service which declares
// its topology up front, or one comparing what this library declares with what
// another one does, can see the whole thing:
//
//	fmt.Println(patterns.ScheduleTopology())
func ScheduleTopology() *acemq.Topology {
	t := acemq.NewTopology().Exchange(ScheduleExchange, "direct")

	for _, rung := range scheduleRungs {
		name := ScheduleRungName(rung)
		opts := []acemq.QueueOption{acemq.OfType(acemq.QueueClassic)}
		for arg, value := range ScheduleRungArgs(rung) {
			opts = append(opts, acemq.QueueArg(arg, value))
		}
		t.Queue(name, opts...).Binding(name, ScheduleExchange, name)
	}

	t.Queue(ScheduleControlQueue, acemq.OfType(acemq.QueueClassic)).
		Binding(ScheduleControlQueue, ScheduleExchange, ScheduleControlQueue)
	return t
}

// Scheduler delivers a message later.
//
//	scheduler, err := patterns.NewScheduler(ctx, mq)
//	if err != nil {
//		return err
//	}
//	defer scheduler.Close()
//
//	scheduler.In(ctx, 4*time.Hour, "billing", "invoice.due", invoice)
//	scheduler.At(ctx, renewal, "policies", "policy.renew", policy)
//
// # Why not a per-message time to live
//
// The obvious implementation is to set an expiration on the message, drop it in
// a queue nobody consumes, and let it dead-letter to its destination. It is
// what most articles suggest and it is wrong for anything but a single fixed
// delay, because a classic queue expires messages only at its head. Put a
// four-hour message in, then a one-minute message behind it, and the one-minute
// message is delivered in four hours. Nothing reports this: the queue looks
// healthy, the message is not lost, it is simply late by a factor nobody
// predicted. It fails in production under mixed load rather than in testing
// under uniform load.
//
// # What this does instead
//
// A ladder of queues, each with a uniform time to live, and a message hops
// through them until it is due:
//
//	acemq.schedule.1s  acemq.schedule.10s  acemq.schedule.1m  acemq.schedule.10m  acemq.schedule.1h
//
// Every message in a rung has the same delay, so the head is always the message
// due soonest. Each expiry dead-letters the message back to
// [ScheduleControlQueue], where this scheduler either delivers it or puts it in
// the largest rung that does not overshoot what is left.
//
// The cost is honest and worth stating: a long delay is several broker round
// trips rather than one, and delivery is accurate to about the smallest rung
// rather than to the second. Something that must fire at 09:00:00.000 exactly
// is a scheduler, not a message broker.
//
// The alternative is RabbitMQ's delayed-message-exchange plugin, which does
// this properly and is a plugin — so it is not available everywhere, and a
// library that silently required it would be a library that works on your
// laptop.
type Scheduler struct {
	conn *acemq.Conn
	due  *acemq.Consumer

	scheduled atomic.Int64
	delivered atomic.Int64
	hops      atomic.Int64
}

// NewScheduler declares the scheduler's topology and starts consuming the
// control queue.
//
// The consumer reads raw bytes and never decodes a payload. A scheduler that
// decodes acquires opinions about message formats it has no business having,
// and would fail on the first message written by something it does not know how
// to read.
func NewScheduler(ctx context.Context, conn *acemq.Conn) (*Scheduler, error) {
	if conn == nil {
		return nil, fmt.Errorf("acemq: NewScheduler was given no connection")
	}
	if err := ScheduleTopology().Apply(ctx, conn); err != nil {
		return nil, fmt.Errorf("acemq: cannot declare the scheduler's topology: %w", err)
	}

	s := &Scheduler{conn: conn}

	due, err := acemq.Consume(ctx, conn, ScheduleControlQueue,
		func(ctx context.Context, m acemq.Message[[]byte]) acemq.Ack {
			if err := s.forward(ctx, m.Body, m.Envelope.Headers); err != nil {
				return acemq.Reject(err)
			}
			return acemq.Accept()
		},
		acemq.ConsumeWith(acemq.BytesCodec{}),
		acemq.Prefetch(schedulePrefetch))
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot consume %s: %w", ScheduleControlQueue, err)
	}
	s.due = due
	return s, nil
}

// In delivers a message after a delay. A delay of zero or less delivers now.
func (s *Scheduler) In(
	ctx context.Context, delay time.Duration, exchange, routingKey string, payload any,
) error {
	return s.At(ctx, time.Now().Add(delay), exchange, routingKey, payload)
}

// At delivers a message at a moment. A moment in the past delivers now.
//
// The payload is encoded once, here, with the connection's codec, and carried
// as bytes from then on. The content type goes with it in
// [HeaderScheduleContentType], because that is how the eventual consumer
// chooses a codec.
func (s *Scheduler) At(
	ctx context.Context, when time.Time, exchange, routingKey string, payload any,
) error {
	if routingKey == "" {
		return fmt.Errorf("acemq: a scheduled message needs a routing key")
	}

	codec := s.conn.Codec()
	body, err := codec.Encode(payload)
	if err != nil {
		return fmt.Errorf("acemq: cannot encode a %T to schedule for %q: %w", payload, routingKey, err)
	}

	headers := map[string]any{
		HeaderScheduleExchange:    exchange,
		HeaderScheduleRoutingKey:  routingKey,
		HeaderScheduleDueAt:       when.UnixMilli(),
		HeaderScheduleContentType: codec.ContentType(),
	}

	s.scheduled.Add(1)
	return s.route(ctx, body, headers, when)
}

// forward handles one message that has come out of a rung.
func (s *Scheduler) forward(ctx context.Context, body []byte, headers map[string]any) error {
	_, hasExchange := headers[HeaderScheduleExchange]
	_, hasRoutingKey := headers[HeaderScheduleRoutingKey]
	dueAt, hasDueAt := headers[HeaderScheduleDueAt]
	if !hasExchange || !hasRoutingKey || !hasDueAt {
		return acemq.Fatalf(
			"acemq: a message reached %s without the headers a scheduled message carries."+
				" Something else is publishing into the scheduler's queues, which it must not:"+
				" they are an implementation detail of this pattern", ScheduleControlQueue)
	}

	millis, ok := headerMillis(dueAt)
	if !ok {
		return acemq.Fatalf("acemq: %s on a message in %s is %v, which is not a moment",
			HeaderScheduleDueAt, ScheduleControlQueue, dueAt)
	}
	return s.route(ctx, body, headers, time.UnixMilli(millis))
}

// route delivers the message if it is due, and otherwise puts it in the largest
// rung that does not overshoot what is left.
func (s *Scheduler) route(
	ctx context.Context, body []byte, headers map[string]any, when time.Time,
) error {
	remaining := time.Until(when)
	shortest := scheduleRungs[len(scheduleRungs)-1]

	if remaining < shortest {
		// Due, or so nearly due that another hop would cost more than the
		// accuracy it buys.
		return s.deliver(ctx, body, headers)
	}

	rung := shortest
	for _, candidate := range scheduleRungs {
		if candidate <= remaining {
			rung = candidate
			break
		}
	}

	env, err := scheduleEnvelope(headers)
	if err != nil {
		return err
	}

	s.hops.Add(1)
	_, err = s.conn.PublishRaw(ctx, ScheduleExchange, ScheduleRungName(rung), acemq.Outbound{
		Body:        body,
		ContentType: acemq.BytesContentType,
		MessageID:   env.ID,
		Headers:     env.ToWire(),
		Persistent:  true,
	})
	if err != nil {
		return fmt.Errorf("acemq: cannot put a scheduled message on %s: %w", ScheduleRungName(rung), err)
	}
	return nil
}

// deliver publishes the message to where it was always going.
//
// The bytes go out unchanged, under the content type they were encoded as.
// Re-encoding them would produce JSON containing JSON; publishing them as plain
// bytes would lose the content type, and what arrives could not be decoded by
// the consumer that was waiting for it.
func (s *Scheduler) deliver(ctx context.Context, body []byte, headers map[string]any) error {
	exchange := headerText(headers[HeaderScheduleExchange])
	routingKey := headerText(headers[HeaderScheduleRoutingKey])

	contentType := headerText(headers[HeaderScheduleContentType])
	if contentType == "" {
		contentType = acemq.JSONContentType
	}

	// The scheduler's own headers are not passed on: they are bookkeeping, and
	// a consumer that started depending on them would be depending on how a
	// message got to it.
	env, err := acemq.NewEnvelope(ScheduledMessageType)
	if err != nil {
		return err
	}

	s.delivered.Add(1)
	_, err = s.conn.PublishRaw(ctx, exchange, routingKey, acemq.Outbound{
		Body:        body,
		ContentType: contentType,
		MessageID:   env.ID,
		Headers:     env.ToWire(),
		Persistent:  true,
	})
	if err != nil {
		return fmt.Errorf("acemq: cannot deliver a scheduled message to %q/%q: %w",
			exchange, routingKey, err)
	}
	return nil
}

// scheduleEnvelope builds the envelope a message carries between rungs, with
// the scheduler's headers on it.
func scheduleEnvelope(headers map[string]any) (acemq.Envelope, error) {
	opts := make([]acemq.EnvelopeOption, 0, len(headers))
	for name, value := range headers {
		opts = append(opts, acemq.Header(name, value))
	}
	env, err := acemq.NewEnvelope(ScheduledMessageType, opts...)
	if err != nil {
		return acemq.Envelope{}, fmt.Errorf("acemq: cannot build a scheduled message: %w", err)
	}
	return env, nil
}

// Scheduled is how many messages have been handed to this scheduler.
func (s *Scheduler) Scheduled() int64 { return s.scheduled.Load() }

// Delivered is how many have reached their destination.
func (s *Scheduler) Delivered() int64 { return s.delivered.Load() }

// Hops is how many times a message has moved between rungs.
//
// Divided by [Scheduler.Delivered] it is the average number of hops, which is
// the number to look at when the scheduler is busier than expected: long delays
// cost hops.
func (s *Scheduler) Hops() int64 { return s.hops.Load() }

// Close stops consuming the control queue. The queues stay, with whatever is
// still waiting on them.
func (s *Scheduler) Close() error {
	if s.due == nil {
		return nil
	}
	return s.due.Close()
}

// String makes a scheduler readable in a log line.
func (s *Scheduler) String() string {
	return fmt.Sprintf("Scheduler{rungs=%v, scheduled=%d, delivered=%d}",
		scheduleRungs, s.Scheduled(), s.Delivered())
}

// headerMillis reads an epoch-milliseconds header, which arrives as whichever
// integer the peer's client library chose.
func headerMillis(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case float32:
		return int64(t), true
	case float64:
		return int64(t), true
	default:
		return 0, false
	}
}

// headerText reads a string header, which a broker client may hand over as
// bytes rather than as a string.
func headerText(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}
