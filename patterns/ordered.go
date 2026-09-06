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
	"hash/fnv"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// PartitionKey picks the ordering key for a message.
//
// Messages sharing a key are handled one at a time and in the order they
// arrived. Messages with different keys may be handled at once.
type PartitionKey[T any] func(acemq.Message[T]) string

// ByHeader orders messages by an application header — a tenant, a customer, an
// aggregate identifier.
func ByHeader[T any](name string) PartitionKey[T] {
	return func(m acemq.Message[T]) string {
		if v, ok := m.Envelope.Headers[name]; ok {
			return fmt.Sprint(v)
		}
		return ""
	}
}

// ByCorrelation orders messages by their correlation identifier, which keeps
// one business action's messages in sequence.
func ByCorrelation[T any]() PartitionKey[T] {
	return func(m acemq.Message[T]) string { return m.Envelope.CorrelationID }
}

// Ordered wraps a handler so messages sharing a key are never handled at once.
//
// A queue delivers in order; a consumer with [acemq.Concurrency] above one
// stops honouring it. That is usually the right trade, and it is the wrong one
// where later messages about the same entity must not overtake earlier ones —
// an "order cancelled" arriving before the "order placed" it cancels.
//
// This buys ordering per key while keeping concurrency across keys:
//
//	sub, err := acemq.Consume(ctx, mq, "orders",
//		patterns.Ordered(patterns.ByHeader[OrderEvent]("x-order-id"), handle),
//		acemq.Concurrency(16))
//
// # What it does not do
//
// It orders the handling of messages that have already been delivered. It
// cannot reorder messages the broker delivered out of order, and with multiple
// consumers on one queue it orders only within each process. Ordering across
// consumers needs the messages to reach the same consumer in the first place,
// which is a routing decision — a consistent hash exchange, or a queue per
// partition.
//
// A message whose key is empty is handled without any ordering, since there is
// nothing to order it against.
func Ordered[T any](key PartitionKey[T], handler acemq.Handler[T]) acemq.Handler[T] {
	locks := &keyedLocks{held: map[string]*sync.Mutex{}}

	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		k := key(m)
		if k == "" {
			return handler(ctx, m)
		}

		unlock := locks.lock(k)
		defer unlock()
		return handler(ctx, m)
	}
}

// keyedLocks hands out one mutex per key, and forgets a key once nothing holds
// it — otherwise the map grows for every key ever seen, which for a per-order
// key means for ever.
type keyedLocks struct {
	mu   sync.Mutex
	held map[string]*sync.Mutex
	refs map[string]int
}

func (l *keyedLocks) lock(key string) func() {
	l.mu.Lock()
	if l.refs == nil {
		l.refs = map[string]int{}
	}
	m, ok := l.held[key]
	if !ok {
		m = &sync.Mutex{}
		l.held[key] = m
	}
	l.refs[key]++
	l.mu.Unlock()

	m.Lock()

	return func() {
		m.Unlock()

		l.mu.Lock()
		l.refs[key]--
		if l.refs[key] == 0 {
			delete(l.refs, key)
			delete(l.held, key)
		}
		l.mu.Unlock()
	}
}

// Partition maps a key onto one of n slots.
//
// For deciding which queue or which worker a message belongs to when ordering
// has to hold across processes rather than within one. The same key always
// gives the same slot, and the mapping does not depend on the Go runtime, so
// two services agree.
func Partition(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// PartitionedRoutingKey appends a partition to a routing key, for publishing
// into a queue-per-partition arrangement.
//
//	key := patterns.PartitionedRoutingKey("orders", order.ID, 8) // "orders.3"
func PartitionedRoutingKey(base, key string, partitions int) string {
	return fmt.Sprintf("%s.%d", base, Partition(key, partitions))
}
