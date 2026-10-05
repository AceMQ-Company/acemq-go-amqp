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
// The three-step shape is deliberate, and it is what makes this safe under
// failure. A store offering "have I seen this?" and "record that I have" loses
// either way round: record before the handler and a crash mid-handler means the
// work never happens and never can, because the message now looks handled;
// record after and two concurrent deliveries both pass the check. Claiming,
// then confirming only on success and releasing on failure, closes both.
//
// This interface had two methods until 0.8.0 and was the first of the five
// libraries to have it wrong. A claim was an insert with no expiry and no
// confirm, so a process killed between claiming and finishing left a row
// indistinguishable from a completion -- and the redelivery was accepted
// without the work ever being done. Java, .NET, Python and Ruby all had a lease;
// Go did not, and nothing detected it because the loss is silent: no error, no
// duplicate, no metric, just work that never happened.
type IdempotencyStore interface {
	// Claim takes ownership of a key, and reports whether this caller got it.
	//
	// It must be atomic: two consumers handling the same message at once must not
	// both be told they own it. A claim that nobody confirms or releases must
	// become claimable again after a while, because the only thing that abandons
	// one is a process that died, and its work still needs doing.
	Claim(ctx context.Context, key string) (bool, error)

	// Confirm records that the work for a key is done. A key confirmed is a key
	// whose redelivery must be accepted without running the handler again.
	Confirm(ctx context.Context, key string) error

	// Release gives up a claim without confirming it, so a message that failed
	// can be tried again.
	Release(ctx context.Context, key string) error
}

// ClaimResult is what a store found when it was asked to claim a key.
type ClaimResult int

const (
	// Claimed: nobody had the key, or the last holder's lease had run out. It is
	// this caller's to run.
	Claimed ClaimResult = iota
	// Duplicate: the work was done and confirmed. Accept it without running
	// anything.
	Duplicate
	// InProgress: somebody holds a live claim and has not confirmed. Neither run
	// it nor accept it — put it back and look again later.
	InProgress
)

// String is the name every AceMQ library uses for it.
func (r ClaimResult) String() string {
	switch r {
	case Claimed:
		return "claimed"
	case Duplicate:
		return "duplicate"
	case InProgress:
		return "in_progress"
	default:
		return "unknown"
	}
}

// ClaimReporter is an [IdempotencyStore] that can say why a claim was refused.
//
// [IdempotencyStore.Claim]'s false cannot tell work that is done from work that
// is still being done, and treating both as done loses a message whose handler
// failed and could not release its claim: the redelivery is accepted as a
// duplicate of work that never happened. [Idempotent] asks a store that
// implements this for the three-way answer and puts an in-progress message back
// instead. Both stores in this package implement it; a store that does not is
// treated as before, with a refused claim read as a duplicate.
type ClaimReporter interface {
	TryClaim(ctx context.Context, key string) (ClaimResult, error)
}

// claim asks a store for the three-way answer, from one that can give it.
func claim(ctx context.Context, store IdempotencyStore, key string) (ClaimResult, error) {
	if reporter, ok := store.(ClaimReporter); ok {
		return reporter.TryClaim(ctx, key)
	}
	first, err := store.Claim(ctx, key)
	if err != nil || first {
		return Claimed, err
	}
	return Duplicate, nil
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
	seen map[string]entry
	ttl  time.Duration

	// claimTimeout is how long a claim nobody confirmed stays somebody's.
	//
	// It exists because the alternative is losing work: a claim with no expiry,
	// left behind by a process that was killed, suppresses the redelivery for
	// ever. Five minutes matches Java, Python and Ruby.
	claimTimeout time.Duration
}

// entry is one key, and whether the work for it finished.
type entry struct {
	at        time.Time
	confirmed bool
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
	return &InMemoryIdempotencyStore{
		seen:         map[string]entry{},
		ttl:          ttl,
		claimTimeout: DefaultClaimTimeout,
	}
}

// DefaultClaimTimeout is how long an unconfirmed claim is honoured before another
// consumer may take it. The same five minutes Java, Python and Ruby use.
const DefaultClaimTimeout = 5 * time.Minute

// SetClaimTimeout changes how long an unconfirmed claim is honoured.
//
// Worth shortening in a test that wants to watch a claim expire; worth leaving
// alone otherwise. It must comfortably exceed the longest a handler can take, or
// a slow handler's message is handed to a second consumer while the first is
// still working on it.
func (s *InMemoryIdempotencyStore) SetClaimTimeout(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d <= 0 {
		d = DefaultClaimTimeout
	}
	s.claimTimeout = d
}

// Claim takes a key, unless it is confirmed or claimed by somebody still inside
// the claim window.
func (s *InMemoryIdempotencyStore) Claim(ctx context.Context, key string) (bool, error) {
	result, err := s.TryClaim(ctx, key)
	return result == Claimed, err
}

// TryClaim is [InMemoryIdempotencyStore.Claim] saying why a claim was refused:
// the key is confirmed ([Duplicate]) or held inside the claim window
// ([InProgress]).
func (s *InMemoryIdempotencyStore) TryClaim(_ context.Context, key string) (ClaimResult, error) {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Swept here rather than on a timer, so the store needs no goroutine and
	// nothing to close.
	for k, e := range s.seen {
		if now.Sub(e.at) > s.ttl {
			delete(s.seen, k)
		}
	}

	if e, present := s.seen[key]; present {
		// Confirmed is done, and stays done for the retention window.
		if e.confirmed {
			return Duplicate, nil
		}
		// An unconfirmed claim inside its window belongs to whoever took it.
		if now.Sub(e.at) <= s.claimTimeout {
			return InProgress, nil
		}
		// Outside the window: whoever held it is not coming back, and the work
		// still has to happen.
	}
	s.seen[key] = entry{at: now}
	return Claimed, nil
}

// Confirm records that the work is done.
func (s *InMemoryIdempotencyStore) Confirm(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[key] = entry{at: time.Now(), confirmed: true}
	return nil
}

// Release gives up a claim without confirming it.
func (s *InMemoryIdempotencyStore) Release(_ context.Context, key string) error {
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
// A redelivery that finds the message in progress — claimed and not confirmed,
// by a handler still running or by one that failed and could not release its
// claim — is neither run nor accepted. It is answered with [acemq.InProgress],
// which puts it back after [InProgressDelay] (five seconds by default) without
// spending a retry attempt. Accepting it would lose the message if that claim
// never became a completion. This needs a store that implements
// [ClaimReporter]; both in this package do.
//
// When the handler fails, the key is forgotten so the retry can run. That is
// the honest ordering — remembering a message that then failed would mean a
// retry silently does nothing — and it is why this is a guard against
// duplicates rather than a guarantee of exactly-once. Between the handler
// finishing and the acknowledgement reaching the broker, a crash still leaves a
// message that will be delivered again. Only a store written in the same
// transaction as the work closes that gap.
func Idempotent[T any](
	store IdempotencyStore, handler acemq.Handler[T], opts ...IdempotencyOption,
) acemq.Handler[T] {
	delay := inProgressDelay(opts)
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		return guard(ctx, store, m.Envelope.ID, delay, handler, m)
	}
}

// IdempotencyOption tunes [Idempotent], [IdempotentBy] and [WithIdempotency].
type IdempotencyOption func(*time.Duration)

// InProgressDelay is how long a message found in progress waits before it is
// put back and looked at again. Keep it well under the store's claim timeout.
// The default is [acemq.DefaultInProgressDelay].
func InProgressDelay(d time.Duration) IdempotencyOption {
	return func(delay *time.Duration) { *delay = d }
}

func inProgressDelay(opts []IdempotencyOption) time.Duration {
	delay := acemq.DefaultInProgressDelay
	for _, opt := range opts {
		opt(&delay)
	}
	return delay
}

// guard is the claim, run, confirm-or-release sequence both wrappers share.
func guard[T any](
	ctx context.Context, store IdempotencyStore, key string, delay time.Duration,
	handler acemq.Handler[T], m acemq.Message[T],
) acemq.Ack {
	result, err := claim(ctx, store, key)
	if err != nil {
		// The store is the thing that is broken, not the message. Retrying
		// is right; carrying on and risking a duplicate is not.
		return acemq.Retry(err)
	}
	switch result {
	case Duplicate:
		return acemq.Accept()
	case InProgress:
		return acemq.InProgress(delay)
	}

	ack := handler(ctx, m)
	if ack.String() == "accept" {
		// Done, and recorded as done rather than merely claimed -- which is
		// what lets a claim left behind by a crash expire without this one
		// expiring with it.
		_ = store.Confirm(ctx, key)
	} else {
		// It did not work, so it has not been handled. Releasing lets the
		// retry actually run. If the release fails, the claim stays until its
		// lease runs out, and every redelivery until then is put back as in
		// progress rather than run or accepted.
		_ = store.Release(ctx, key)
	}
	return ack
}

// IdempotentBy is [Idempotent] with a key of your own.
//
// Use it when the natural key is in the payload rather than the envelope: an
// order identifier that two different messages both carry, where handling
// either one twice is the thing to prevent.
func IdempotentBy[T any](
	store IdempotencyStore, key func(acemq.Message[T]) string, handler acemq.Handler[T],
	opts ...IdempotencyOption,
) acemq.Handler[T] {
	delay := inProgressDelay(opts)
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		k := key(m)
		if k == "" {
			return acemq.Reject(acemq.Fatalf(
				"acemq: message %s produced an empty idempotency key", m.Envelope.ID))
		}
		return guard(ctx, store, k, delay, handler, m)
	}
}
