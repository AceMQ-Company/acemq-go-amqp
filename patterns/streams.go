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
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// StreamOffset is where a stream consumer starts reading.
//
// The difference from a queue: a stream keeps its messages after they are read,
// so a consumer chooses a position rather than taking whatever is at the front.
// Several consumers can read the same stream independently, and a new one can
// start from the beginning.
type StreamOffset struct {
	kind  string
	value any
}

// FromFirst starts at the oldest message the stream still holds.
//
// For building a projection from scratch, or a new consumer that needs the
// history. Note "still holds": a stream has a retention policy, and the oldest
// message is not necessarily the first one ever written.
func FromFirst() StreamOffset { return StreamOffset{kind: "first"} }

// FromNext starts at the next message published, ignoring everything already
// there. The default, and what most consumers want.
func FromNext() StreamOffset { return StreamOffset{kind: "next"} }

// FromLast starts at the last chunk the stream holds, which is roughly "the
// recent past" rather than an exact number of messages.
func FromLast() StreamOffset { return StreamOffset{kind: "last"} }

// FromOffset starts at an exact position, which is what a consumer that records
// its progress uses to carry on where it left off.
func FromOffset(offset uint64) StreamOffset {
	return StreamOffset{kind: "offset", value: offset}
}

// FromTimestamp starts at the first message published at or after a time.
func FromTimestamp(at time.Time) StreamOffset {
	return StreamOffset{kind: "timestamp", value: at}
}

// arg is what the broker is told.
//
// An exact offset goes out as an int64 and not as the uint64 [FromOffset] takes.
// AMQP's field table has no unsigned long, and the client refuses to encode one
// — "table field x-stream-offset value uint64 not supported" — so a consumer
// built with FromOffset could not start at all. The public type stays unsigned
// because an offset is a position in a log and cannot be negative; the
// conversion belongs here, where the wire is.
func (o StreamOffset) arg() any {
	switch o.kind {
	case "offset":
		if offset, ok := o.value.(uint64); ok {
			// Past the signed range there is no honest encoding, and a stream
			// that far along does not exist. Clamping is better than wrapping
			// to a negative the broker reads as something else entirely.
			if offset > math.MaxInt64 {
				return int64(math.MaxInt64)
			}
			return int64(offset)
		}
		return o.value
	case "timestamp":
		if t, ok := o.value.(time.Time); ok {
			return t
		}
		return nil
	case "":
		return "next"
	default:
		return o.kind
	}
}

func (o StreamOffset) String() string {
	if o.value != nil {
		return fmt.Sprintf("%s(%v)", o.kind, o.value)
	}
	if o.kind == "" {
		return "next"
	}
	return o.kind
}

// StreamOptions configure reading a stream.
type StreamOptions struct {
	// Offset is where to start. FromNext when not set.
	Offset StreamOffset

	// Prefetch must be set for a stream consumer, and RabbitMQ refuses one
	// without it. Ten by default.
	Prefetch int

	// Name identifies a consumer to the broker, and is what makes server-side
	// offset tracking possible.
	Name string

	// OnFailure is what a failed handler does to the reader. [StopOnFailure]
	// when not set.
	OnFailure StreamFailure

	// OnStop, when set, is called once after a failed handler stopped the
	// reader, with the handler's reason. The reader is closed by then.
	OnStop func(error)
}

// StreamFailure is what a stream reader does when its handler fails: returns
// acemq.Retry or acemq.InProgress, or panics.
//
// acemq.Reject and acemq.Park are not failures in this sense. They are the
// handler setting one message aside on purpose, and are honoured either way:
// a copy goes to {stream}.dlq or {stream}.parked and the reader goes on.
type StreamFailure int

const (
	// StopOnFailure closes the reader at the failed message: nothing after it
	// is handled and no copy of it is written anywhere. The stream is the
	// record, so a reader restarted at the same offset after a fix sees the
	// message again. The default (AMQP-STREAM-004).
	StopOnFailure StreamFailure = iota

	// ParkOnFailure parks a copy to {stream}.parked with a
	// [*RetryOnStreamError] and reads on. What every failure did before
	// StopOnFailure became the default.
	ParkOnFailure
)

// DeclareStream declares a queue that keeps its messages.
//
//	err := patterns.DeclareStream(ctx, mq, "events", patterns.StreamRetention{
//		MaxAge:   7 * 24 * time.Hour,
//		MaxBytes: 10 << 30,
//	})
//
// A stream is durable and cannot be exclusive or auto-deleting; those are set
// here rather than left to fail at the broker with a message that does not
// mention streams.
func DeclareStream(ctx context.Context, conn *acemq.Conn, name string, retention StreamRetention) error {
	opts := []acemq.QueueOption{acemq.OfType(acemq.QueueStream)}

	if retention.MaxAge > 0 {
		// RabbitMQ wants a duration with a unit suffix rather than a number.
		opts = append(opts, acemq.QueueArg("x-max-age", durationArg(retention.MaxAge)))
	}
	if retention.MaxBytes > 0 {
		opts = append(opts, acemq.QueueArg("x-max-length-bytes", retention.MaxBytes))
	}
	if retention.SegmentBytes > 0 {
		opts = append(opts, acemq.QueueArg("x-stream-max-segment-size-bytes", retention.SegmentBytes))
	}

	return conn.DeclareQueue(ctx, name, opts...)
}

// StreamRetention is how much of a stream to keep.
//
// Unbounded by default, which for a stream means "until the disk is full". Set
// at least one of these on anything that will run for long.
type StreamRetention struct {
	// MaxAge discards messages older than this.
	MaxAge time.Duration

	// MaxBytes discards the oldest messages once the stream exceeds this.
	MaxBytes int64

	// SegmentBytes is how large each file on disk gets. Retention happens a
	// segment at a time, so a very large segment means retention is coarse.
	SegmentBytes int64
}

func durationArg(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dD", int64(d/(24*time.Hour)))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	default:
		return fmt.Sprintf("%ds", int64(d/time.Second))
	}
}

// ReadStream consumes a stream from a chosen position.
//
//	sub, err := patterns.ReadStream(ctx, mq, "events",
//		func(ctx context.Context, m acemq.Message[Event]) acemq.Ack {
//			return project(ctx, m.Payload)
//		},
//		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
//
// # How this differs from consuming a queue
//
// Acknowledging does not remove the message: a stream keeps everything until
// its retention policy discards it. What an acknowledgement does is move this
// run forward, and nothing more — it does not record a position anywhere the
// broker will give back. This function always names an explicit x-stream-offset
// on the wire, StreamOptions.Offset's, so where a run starts is that option and
// never a position the broker remembered. A restarted consumer on FromNext sees
// what is published from then on rather than carrying on where it stopped;
// resuming exactly means recording the x-stream-offset header as you go and
// starting from FromOffset.
//
// A failed handler — acemq.Retry, acemq.InProgress or a panic — stops the
// reader by default: the subscription is closed at that message, nothing after
// it is handled, and no copy of it is written anywhere. A retry is never
// obeyed, because it republishes the message onto the queue it came from,
// which on a stream appends a second copy to the log at a new offset for every
// consumer to read. Set StreamOptions.OnFailure to [ParkOnFailure] to park the
// message with a [RetryOnStreamError] and read on instead, and
// StreamOptions.OnStop to hear that the reader stopped.
//
// Rejecting does not dead-letter in the broker's sense, because a stream carries
// no x-dead-letter-exchange and nothing can be removed from it. The engine's own
// path still runs: acemq.Reject republishes a copy to {stream}.dlq and
// acemq.Park to {stream}.parked, and the original stays in the stream. A message
// that cannot be handled otherwise has to be dealt with by the handler — logged,
// counted, copied elsewhere — and the stream moves on regardless. That is the
// trade a stream makes: nothing is lost, and nothing is retried for you.
func ReadStream[T any](
	ctx context.Context, conn *acemq.Conn, stream string,
	handler acemq.Handler[T], opts StreamOptions,
) (*acemq.Consumer, error) {
	if !conn.Supports(acemq.CapabilityStreams) {
		return nil, fmt.Errorf(
			"acemq: this transport does not support streams; " +
				"they are a RabbitMQ queue type and the in-memory transport has no equivalent")
	}

	prefetch := opts.Prefetch
	if prefetch <= 0 {
		// RabbitMQ refuses a stream consumer without one, and the error it
		// gives does not explain why. The number is this library's own choice
		// and not part of the cross-language contract; see docs/streams.md.
		prefetch = 10
	}

	consumeOpts := []acemq.ConsumeOption{
		acemq.Prefetch(prefetch),
		acemq.ConsumeArg("x-stream-offset", opts.Offset.arg()),
	}
	if opts.Name != "" {
		consumeOpts = append(consumeOpts, acemq.ConsumerTag(opts.Name))
	}

	if opts.OnFailure == ParkOnFailure {
		return acemq.Consume(ctx, conn, stream, refuseRetry(stream, handler), consumeOpts...)
	}

	// The handler stops the reader from inside a delivery, so the close runs on
	// its own goroutine: Close waits for running handlers, this one included.
	var (
		reader  *acemq.Consumer
		started = make(chan struct{})
		stopped atomic.Bool
	)
	stop := func(reason error) {
		if !stopped.CompareAndSwap(false, true) {
			return
		}
		go func() {
			<-started
			if reader != nil {
				_ = reader.Close()
			}
			if opts.OnStop != nil {
				opts.OnStop(reason)
			}
		}()
	}
	var err error
	reader, err = acemq.Consume(ctx, conn, stream, stopOnFailure(stream, handler, &stopped, stop), consumeOpts...)
	close(started)
	return reader, err
}

// stopOnFailure wraps a stream handler so a failure stops the reader rather
// than moving past the message.
//
// The failed delivery and any already prefetched behind it are acknowledged
// without being handled: on a stream an acknowledgement removes nothing and
// only moves this run's credit, and the reader is closing.
func stopOnFailure[T any](
	stream string, handler acemq.Handler[T], stopped *atomic.Bool, stop func(error),
) acemq.Handler[T] {
	return func(ctx context.Context, m acemq.Message[T]) (ack acemq.Ack) {
		if stopped.Load() {
			return acemq.Accept()
		}
		defer func() {
			if r := recover(); r != nil {
				stop(fmt.Errorf("acemq: the handler on stream %q panicked on message %s: %v",
					stream, m.Envelope.ID, r))
				ack = acemq.Accept()
			}
		}()
		ack = handler(ctx, m)
		if !ack.IsRetry() && !ack.IsInProgress() {
			return ack
		}
		reason := ack.Err()
		if reason == nil {
			reason = errors.New("acemq: the message is in progress elsewhere")
		}
		stop(fmt.Errorf("acemq: the handler on stream %q failed on message %s, so the reader"+
			" stopped there: %w", stream, m.Envelope.ID, reason))
		return acemq.Accept()
	}
}

// RetryOnStreamError is what a stream handler gets for returning acemq.Retry.
//
// Java draws this line in the type system: StreamConsumer is a separate type
// from MessageConsumer precisely so that the outcomes a stream cannot honour are
// not offered, and a single type covering both would be one where half the
// methods throw. Go's handler signature is shared — acemq.Handler is one type
// and [ReadStream] takes it — so the line is drawn here instead, at the moment
// the verb is used rather than at the moment it is spelled.
//
// Under [ParkOnFailure] the message is parked: republished to {stream}.parked,
// which does not touch the stream, and the run moves on. The error says which
// two choices are on offer, and both are one line in the handler:
//
//   - Park it. Return acemq.Park(err) yourself. The message goes to
//     {stream}.parked as a copy and somebody looks at the queue later. The
//     original is still in the stream, because nothing is ever removed from one.
//
//   - Checkpoint and move on. Record the x-stream-offset header, return
//     acemq.Accept(), and count the gap — nothing else records it. See
//     [StreamOffsetOf].
//
// Stopping is the third choice, and what [StopOnFailure], the default, does.
type RetryOnStreamError struct {
	// Stream is the stream the handler was reading.
	Stream string

	// Offset is where in the log the message sat, when the delivery carried one.
	Offset uint64

	// MessageID is the message the handler gave up on.
	MessageID string

	// Err is the reason the handler passed to acemq.Retry.
	Err error
}

func (e *RetryOnStreamError) Error() string {
	where := e.MessageID
	if e.Offset > 0 {
		where = fmt.Sprintf("%s at offset %d", e.MessageID, e.Offset)
	}
	return fmt.Sprintf(
		"acemq: a handler on stream %q returned acemq.Retry for message %s, which this library"+
			" refuses: a retry republishes the message onto the queue it came from, and on a"+
			" stream that appends a second copy to the log at a new offset — for every consumer"+
			" to read now and on every replay afterwards. It was parked on %s.parked instead."+
			" A stream handler has two honest choices: park it deliberately with acemq.Park(err),"+
			" or record the x-stream-offset header and return acemq.Accept() to checkpoint and"+
			" move on. The handler's reason was: %v",
		e.Stream, where, e.Stream, e.Err)
}

// Unwrap gives back the reason the handler passed to acemq.Retry, so a handler's
// own sentinel survives errors.Is through the refusal.
func (e *RetryOnStreamError) Unwrap() error { return e.Err }

// refuseRetry wraps a stream handler so a retry becomes a park with an
// explanation rather than a second copy in the log.
//
// The wrapping happens once, at subscribe, rather than being checked on the
// settlement path: everywhere else in this library an Ack means what it says,
// and putting a stream-shaped exception into the engine would make every queue
// consumer pay for it.
func refuseRetry[T any](stream string, handler acemq.Handler[T]) acemq.Handler[T] {
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		ack := handler(ctx, m)
		if !ack.IsRetry() && !ack.IsInProgress() {
			return ack
		}
		reason := ack.Err()
		if reason == nil && ack.IsInProgress() {
			// Putting a message back on a stream appends a second copy, the
			// same as a retry, so in progress is refused the same way.
			reason = errors.New("acemq: the message is in progress elsewhere")
		}
		offset, _ := StreamOffsetOf(m.Envelope)
		return acemq.Park(&RetryOnStreamError{
			Stream:    stream,
			Offset:    offset,
			MessageID: m.Envelope.ID,
			Err:       reason,
		})
	}
}

// StreamOffsetOf is where in the log a stream delivery sat, and whether it said.
//
// RabbitMQ stamps every stream delivery with an x-stream-offset header. That
// name is outside the reserved x-acemq- namespace, so the engine leaves it on
// the envelope rather than stripping it, and this is the number to record if a
// reader has to resume where it stopped: start the next run at FromOffset(n+1).
//
// The width the broker writes it as depends on the client, which is why this
// reads several rather than asserting one. False means the delivery carried no
// offset at all — a message from an ordinary queue, or a stream delivery a hop
// rebuilt and did not restamp — and a checkpoint must not be written from it.
func StreamOffsetOf(env acemq.Envelope) (uint64, bool) {
	switch v := env.Headers["x-stream-offset"].(type) {
	case uint64:
		return v, true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	default:
		return 0, false
	}
}
