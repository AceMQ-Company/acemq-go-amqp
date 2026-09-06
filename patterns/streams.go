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
func (o StreamOffset) arg() any {
	switch o.kind {
	case "offset":
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
}

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
// its retention policy discards it. What an acknowledgement does is advance
// this consumer's position, so restarting from FromNext carries on rather than
// re-reading.
//
// Rejecting a message does not dead-letter it either, because there is nothing
// to remove it from. A message that cannot be handled has to be dealt with by
// the handler — logged, copied to another queue, counted — and the stream moves
// on regardless. That is the trade a stream makes: nothing is lost, and nothing
// is retried for you.
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
		// gives does not explain why.
		prefetch = 10
	}

	consumeOpts := []acemq.ConsumeOption{
		acemq.Prefetch(prefetch),
		acemq.ConsumeArg("x-stream-offset", opts.Offset.arg()),
	}
	if opts.Name != "" {
		consumeOpts = append(consumeOpts, acemq.ConsumerTag(opts.Name))
	}

	return acemq.Consume(ctx, conn, stream, handler, consumeOpts...)
}
