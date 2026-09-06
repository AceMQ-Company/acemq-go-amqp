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

// ReplayResult is what a replay did.
type ReplayResult struct {
	// Moved is how many messages were republished.
	Moved int

	// Skipped is how many the filter declined.
	Skipped int

	// Reason is why the replay stopped: "drained", "limit", "deadline" or
	// "cancelled". Worth reporting, because "moved 500" means something quite
	// different when the limit was 500.
	Reason string
}

func (r ReplayResult) String() string {
	return fmt.Sprintf("moved %d, skipped %d (%s)", r.Moved, r.Skipped, r.Reason)
}

// ReplayFilter decides whether a dead-lettered message should go back.
//
// Returning false leaves it where it is, which is what makes a replay something
// that can be done in stages rather than all at once.
type ReplayFilter func(acemq.Envelope, []byte) bool

// Replay moves messages from one queue back to an exchange.
//
// The thing somebody actually does at three in the morning: a dead-letter queue
// has two thousand messages in it, the bug is fixed, and they need to go back
// through — but not all of them, and not silently.
//
//	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
//		Queue:      "orders-dead",
//		Exchange:   "orders-events",
//		Limit:      500,
//		Filter: func(env acemq.Envelope, _ []byte) bool {
//			return strings.Contains(env.Error, "timeout")
//		},
//	})
//
// Each message is stamped so a replayed message can be told from an original:
// [HeaderReplayedFrom], [HeaderReplayedAt] and [HeaderReplayCount]. A consumer
// that needs to treat them differently can, and one that does not is unaffected.
//
// The routing key is the message's own unless [ReplayFrom.RoutingKey] overrides
// it, so a message goes back where it came from rather than everywhere.
type ReplayFrom struct {
	// Queue is where the messages are now, usually a dead-letter queue.
	Queue string

	// Exchange is where they go back to. Empty publishes to a queue by name.
	Exchange string

	// RoutingKey overrides the message's own key. Empty keeps it.
	RoutingKey string

	// Limit stops after this many messages. Zero means no limit, which against
	// a queue somebody is still writing to means it may not stop at all —
	// prefer a limit.
	Limit int

	// Deadline stops the replay after a period. Zero means no deadline.
	Deadline time.Duration

	// Filter decides which messages go. Nil takes all of them.
	Filter ReplayFilter
}

// HeaderReplayedFrom names the queue a message was replayed out of.
const HeaderReplayedFrom = "acemq-replayed-from"

// HeaderReplayedAt is when it was replayed, as RFC 3339.
const HeaderReplayedAt = "acemq-replayed-at"

// HeaderReplayCount is how many times it has been replayed.
const HeaderReplayCount = "acemq-replay-count"

// Replay moves messages from a queue back onto an exchange.
func Replay(ctx context.Context, conn *acemq.Conn, from ReplayFrom) (ReplayResult, error) {
	if from.Queue == "" {
		return ReplayResult{}, fmt.Errorf("acemq: a replay needs a queue to read from")
	}

	if from.Deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, from.Deadline)
		defer cancel()
	}

	var result ReplayResult

	// Messages the filter declines are held, not returned one at a time.
	//
	// Returning one immediately is what an earlier version did, and it does not
	// work: RabbitMQ puts a returned message back where it was, at the head of
	// the queue, so the next read hands back the same message and everything
	// behind it is never looked at. Holding them unacknowledged takes them out
	// of the way for the length of the pass; the broker keeps them, so a crash
	// half way through returns them rather than losing them.
	var declined []*acemq.Pulled
	defer func() {
		for _, message := range declined {
			// Nothing to report: the pass is over, and a message that cannot be
			// returned is one the broker will return itself when this
			// connection closes.
			_ = message.Nack(true)
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			result.Reason = "cancelled"
			if from.Deadline > 0 {
				result.Reason = "deadline"
			}
			return result, nil
		}

		if from.Limit > 0 && result.Moved >= from.Limit {
			result.Reason = "limit"
			return result, nil
		}

		message, found, err := conn.Pull(ctx, from.Queue)
		if err != nil {
			result.Reason = "failed"
			return result, fmt.Errorf("acemq: cannot read %q to replay it: %w", from.Queue, err)
		}
		if !found {
			// Everything available has been offered. What is left is what this
			// pass declined, and it goes back when the pass ends.
			result.Reason = "drained"
			return result, nil
		}

		if from.Filter != nil && !from.Filter(message.Envelope, message.Body) {
			result.Skipped++
			declined = append(declined, message)
			continue
		}

		routingKey := from.RoutingKey
		if routingKey == "" {
			routingKey = message.RoutingKey
		}

		headers := message.Envelope.ToWire()
		headers[HeaderReplayedFrom] = from.Queue
		headers[HeaderReplayedAt] = time.Now().UTC().Format(time.RFC3339)
		headers[HeaderReplayCount] = replayCount(message.Envelope) + 1

		_, err = conn.PublishRaw(ctx, from.Exchange, routingKey, acemq.Outbound{
			Body:        message.Body,
			ContentType: message.ContentType,
			MessageID:   message.Envelope.ID,
			Headers:     headers,
			Persistent:  true,
		})
		if err != nil {
			// Returned rather than dropped, and the replay stops. A replay that
			// loses messages is worse than one that stops early.
			_ = message.Nack(true)
			result.Reason = "failed"
			return result, fmt.Errorf(
				"acemq: cannot republish a message from %q: %w", from.Queue, err)
		}

		// Acknowledged only after the broker has confirmed the new copy, so a
		// failure between the two leaves the message where it was. The cost is
		// that a crash in the gap replays it twice, which is the right way round
		// for a dead-letter queue.
		if err := message.Ack(); err != nil {
			result.Reason = "failed"
			return result, fmt.Errorf(
				"acemq: a message was replayed but could not be acknowledged, "+
					"so it is still on %q and will be replayed again: %w", from.Queue, err)
		}
		result.Moved++
	}
}

func replayCount(env acemq.Envelope) int {
	switch v := env.Headers[HeaderReplayCount].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}
