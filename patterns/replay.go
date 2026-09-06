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
	"sync"
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

	var (
		mu       sync.Mutex
		result   ReplayResult
		finished = make(chan struct{})
		closed   sync.Once

		// seen is how a replay knows it has finished.
		//
		// A message this replay declines — filtered out, or past the limit — is
		// left in the queue, and leaving it means returning it, and a returned
		// message comes straight back. Without a memory that is an infinite
		// loop: the same messages go round for ever and the replay never ends.
		//
		// Seeing an identifier twice means the queue has come full circle and
		// everything left has been considered. That is a real end, not a guess.
		seen = map[string]bool{}

		// idle is the second answer, for a queue somebody is still writing to,
		// where the identifiers never repeat. AMQP has no "that was the last
		// one", so silence has to stand in for it.
		idle = time.NewTimer(2 * time.Second)
	)
	defer idle.Stop()

	stop := func(reason string) {
		closed.Do(func() {
			mu.Lock()
			if result.Reason == "" {
				result.Reason = reason
			}
			mu.Unlock()
			close(finished)
		})
	}

	// Read as bytes rather than through the connection's codec: these messages
	// are going back out unchanged, and a body that will not decode is exactly
	// the kind that ends up in a dead-letter queue.
	consumer, err := acemq.Consume(ctx, conn, from.Queue,
		func(ctx context.Context, m acemq.Message[[]byte]) acemq.Ack {
			idle.Reset(2 * time.Second)

			mu.Lock()
			if seen[m.Envelope.ID] {
				// Round the queue once. Everything still here has been offered
				// and declined, so there is nothing left to do.
				mu.Unlock()
				stop("drained")
				return acemq.Retry(errReplayFinished)
			}
			seen[m.Envelope.ID] = true

			if from.Limit > 0 && result.Moved >= from.Limit {
				mu.Unlock()
				stop("limit")
				// Left where it was: the replay is over and this message was
				// not part of it.
				return acemq.Retry(errReplayFinished)
			}
			mu.Unlock()

			if from.Filter != nil && !from.Filter(m.Envelope, m.Body) {
				mu.Lock()
				result.Skipped++
				mu.Unlock()
				// Not taken, so it stays. Rejecting would drop it, which is the
				// opposite of what declining to replay means.
				return acemq.Retry(errNotSelected)
			}

			routingKey := from.RoutingKey
			if routingKey == "" {
				routingKey = m.RoutingKey
			}

			headers := m.Envelope.ToWire()
			headers[HeaderReplayedFrom] = from.Queue
			headers[HeaderReplayedAt] = time.Now().UTC().Format(time.RFC3339)
			headers[HeaderReplayCount] = replayCount(m.Envelope) + 1

			_, err := conn.PublishRaw(ctx, from.Exchange, routingKey, acemq.Outbound{
				Body:        m.Body,
				ContentType: m.ContentType,
				MessageID:   m.Envelope.ID,
				Headers:     headers,
				Persistent:  true,
			})
			if err != nil {
				// Left in place. A replay that loses messages is worse than one
				// that stops early.
				stop("failed")
				return acemq.Retry(err)
			}

			mu.Lock()
			result.Moved++
			reached := from.Limit > 0 && result.Moved >= from.Limit
			mu.Unlock()

			if reached {
				stop("limit")
			}
			return acemq.Accept()
		}, acemq.Prefetch(1), acemq.ConsumeWith(acemq.BytesCodec{}))
	if err != nil {
		return result, fmt.Errorf("acemq: cannot read %q to replay it: %w", from.Queue, err)
	}

	select {
	case <-finished:
	case <-idle.C:
		stop("drained")
	case <-ctx.Done():
		stop("cancelled")
	}

	if err := consumer.Close(); err != nil {
		return result, err
	}

	mu.Lock()
	defer mu.Unlock()
	if result.Reason == "cancelled" && from.Deadline > 0 {
		result.Reason = "deadline"
	}
	return result, nil
}

// errReplayFinished and errNotSelected are returned to leave a message where it
// is. They are values rather than new errors each time so that a retry policy
// counting them sees the same thing, and so nothing allocates in the loop.
var (
	errReplayFinished = errors.New("acemq: the replay has finished; this message was left in place")
	errNotSelected    = errors.New("acemq: not selected for replay; left in place")
)

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
