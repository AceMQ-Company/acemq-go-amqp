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
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// IdempotencyStore remembers which messages have been handled.
//
// Retries and redeliveries mean a message can arrive more than once, so a
// handler that changes anything needs to be able to tell. [acemq.Envelope.ID]
// is stable across every redelivery of the same message and is the natural key.
type IdempotencyStore interface {
	// FirstTime records the key and reports whether this is the first time it
	// has been seen. It must be atomic: two consumers handling the same message
	// at once must not both be told they are first.
	FirstTime(ctx context.Context, key string) (bool, error)

	// Forget removes a key, so a message that failed can be tried again.
	Forget(ctx context.Context, key string) error
}

// InMemoryIdempotencyStore remembers keys in this process.
//
// Right for a single consumer, and wrong the moment there are two: each has its
// own memory, so both will believe they are first. It is also lost on restart,
// which turns every message in flight into a duplicate.
//
// Use it in tests, and behind one worker. Anything else wants a store the
// workers share — the same database the work is written to, ideally in the same
// transaction, which is the only arrangement that actually holds.
type InMemoryIdempotencyStore struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
}

// NewInMemoryIdempotencyStore remembers keys for a window.
//
// The window matters: without one the map grows for as long as the process
// lives. It should be comfortably longer than the longest a message can take to
// stop being retried.
func NewInMemoryIdempotencyStore(ttl time.Duration) *InMemoryIdempotencyStore {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &InMemoryIdempotencyStore{seen: map[string]time.Time{}, ttl: ttl}
}

// FirstTime records a key and says whether it is new.
func (s *InMemoryIdempotencyStore) FirstTime(_ context.Context, key string) (bool, error) {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Swept here rather than on a timer, so the store needs no goroutine and
	// nothing to close.
	for k, at := range s.seen {
		if now.Sub(at) > s.ttl {
			delete(s.seen, k)
		}
	}

	if _, present := s.seen[key]; present {
		return false, nil
	}
	s.seen[key] = now
	return true, nil
}

// Forget removes a key.
func (s *InMemoryIdempotencyStore) Forget(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seen, key)
	return nil
}

// Len is how many keys are remembered, for a test that wants to know the
// window is doing its job.
func (s *InMemoryIdempotencyStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// Idempotent wraps a handler so a message already handled is accepted without
// running it again.
//
//	sub, err := acemq.Consume(ctx, mq, "orders",
//		patterns.Idempotent(store, func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
//			return place(ctx, m.Payload)
//		}))
//
// A duplicate is accepted rather than rejected: the work was done, so the
// message has been handled, and dead-lettering it would raise an alarm about
// something that went right.
//
// When the handler fails, the key is forgotten so the retry can run. That is
// the honest ordering — remembering a message that then failed would mean a
// retry silently does nothing — and it is why this is a guard against
// duplicates rather than a guarantee of exactly-once. Between the handler
// finishing and the acknowledgement reaching the broker, a crash still leaves a
// message that will be delivered again. Only a store written in the same
// transaction as the work closes that gap.
func Idempotent[T any](
	store IdempotencyStore, handler acemq.Handler[T],
) acemq.Handler[T] {
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		key := m.Envelope.ID

		first, err := store.FirstTime(ctx, key)
		if err != nil {
			// The store is the thing that is broken, not the message. Retrying
			// is right; carrying on and risking a duplicate is not.
			return acemq.Retry(err)
		}
		if !first {
			return acemq.Accept()
		}

		ack := handler(ctx, m)
		if ack.String() != "accept" {
			// It did not work, so it has not been handled. Forgetting lets the
			// retry actually run.
			_ = store.Forget(ctx, key)
		}
		return ack
	}
}

// IdempotentBy is [Idempotent] with a key of your own.
//
// Use it when the natural key is in the payload rather than the envelope: an
// order identifier that two different messages both carry, where handling
// either one twice is the thing to prevent.
func IdempotentBy[T any](
	store IdempotencyStore, key func(acemq.Message[T]) string, handler acemq.Handler[T],
) acemq.Handler[T] {
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		k := key(m)
		if k == "" {
			return acemq.Reject(acemq.Fatalf(
				"acemq: message %s produced an empty idempotency key", m.Envelope.ID))
		}

		first, err := store.FirstTime(ctx, k)
		if err != nil {
			return acemq.Retry(err)
		}
		if !first {
			return acemq.Accept()
		}

		ack := handler(ctx, m)
		if ack.String() != "accept" {
			_ = store.Forget(ctx, k)
		}
		return ack
	}
}
