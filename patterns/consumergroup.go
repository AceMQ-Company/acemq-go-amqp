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

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// ConsumerGroup runs several consumers over one queue and stops them together.
//
// Two things it saves you from. Starting workers by hand means remembering to
// close every one, and a partial shutdown leaves messages held by a consumer
// nobody is waiting for. And a group can be sized from configuration, which is
// the number most often changed after a service is running.
//
//	group, err := patterns.NewConsumerGroup(ctx, mq, "orders", 4, handle)
//	defer group.Close()
//
// # Concurrency, or a group?
//
// [acemq.Concurrency] runs several handlers on one consumer and one channel. A
// group runs several consumers, each with its own channel and its own prefetch.
// The group is what to use when handlers are slow enough that one channel's
// prefetch becomes the limit, or when a fair share across processes matters:
// the broker round-robins between consumers, so four consumers here compete
// evenly with four in another instance.
type ConsumerGroup struct {
	queue     string
	consumers []*acemq.Consumer

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

// NewConsumerGroup starts size consumers over one queue.
//
// If any fails to start, those already started are closed before the error is
// returned — a half-started group would hold messages nothing is going to
// handle.
func NewConsumerGroup[T any](
	ctx context.Context, conn *acemq.Conn, queue string, size int,
	handler acemq.Handler[T], opts ...acemq.ConsumeOption,
) (*ConsumerGroup, error) {
	if size < 1 {
		return nil, fmt.Errorf("acemq: a consumer group needs at least one consumer, got %d", size)
	}

	group := &ConsumerGroup{queue: queue}

	for i := range size {
		// Each consumer is named, so the management interface shows which of
		// them is holding a message rather than four identical rows.
		tagged := append(append([]acemq.ConsumeOption(nil), opts...),
			acemq.ConsumerTag(fmt.Sprintf("acemq-%s-%d", queue, i+1)))

		consumer, err := acemq.Consume(ctx, conn, queue, handler, tagged...)
		if err != nil {
			_ = group.Close()
			return nil, fmt.Errorf(
				"acemq: cannot start consumer %d of %d on %q: %w", i+1, size, queue, err)
		}
		group.consumers = append(group.consumers, consumer)
	}
	return group, nil
}

// Size is how many consumers are running.
func (g *ConsumerGroup) Size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.consumers)
}

// Queue is what they are reading.
func (g *ConsumerGroup) Queue() string { return g.queue }

// Close stops every consumer and waits for handlers already running.
//
// All of them are closed even if one fails, because leaving the rest running
// after a failed shutdown is worse than the failure. The errors are joined.
func (g *ConsumerGroup) Close() error {
	g.closeOnce.Do(func() {
		g.mu.Lock()
		g.closed = true
		consumers := g.consumers
		g.consumers = nil
		g.mu.Unlock()

		var errs []error
		for _, consumer := range consumers {
			if err := consumer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		g.closeErr = errors.Join(errs...)
	})
	return g.closeErr
}
