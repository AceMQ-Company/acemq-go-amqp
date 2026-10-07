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
	"sort"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// OutboxRecord is a message waiting to be published.
type OutboxRecord struct {
	// ID is the message identifier, and what stops the relay publishing the
	// same record twice.
	ID string

	// Exchange and RoutingKey are where it is going.
	Exchange   string
	RoutingKey string

	// Body is the already-encoded payload. The outbox stores bytes rather than
	// a value, because the record outlives the process that wrote it and the
	// type may not survive a deployment.
	Body []byte

	// ContentType is what the body was encoded as.
	ContentType string

	// Headers are the envelope, rendered.
	Headers map[string]any

	// CreatedAt is when it was written.
	CreatedAt time.Time

	// Attempts is how many times publishing this record has failed.
	//
	// The relay publishes in the order records were written and stops at the
	// first failure, so a record the broker will never take — an exchange
	// somebody deleted, a payload a policy will always refuse — would otherwise
	// be retried on every sweep and hold up everything written after it for
	// ever. Once this reaches the store's maximum the record stops being
	// offered, and the queue behind it moves.
	Attempts int

	// LastError is what went wrong the last time, so an operator finding a stuck
	// record reads the reason off the record instead of correlating a log line
	// from ten sweeps ago.
	LastError string
}

// DefaultOutboxMaxAttempts is how many failures a record takes before a store
// stops offering it to the relay. The same ten Python and Ruby use, so a record
// that is stuck is stuck after the same number of tries in all three.
const DefaultOutboxMaxAttempts = 10

// OutboxStore holds messages that have been decided but not yet published.
//
// The point of the pattern: a service that writes to a database and then
// publishes has two things that can fail independently, and the gap between
// them is where messages are lost or invented. Writing the message into the
// same transaction as the work removes the gap — the record and the work commit
// together or neither does — and a relay publishes what was committed.
//
// An implementation is only worth having if Add can join the caller's
// transaction. A store with its own connection has the gap back.
type OutboxStore interface {
	// Add records a message to be published.
	Add(ctx context.Context, record OutboxRecord) error

	// Pending returns records waiting to be published, oldest first.
	Pending(ctx context.Context, limit int) ([]OutboxRecord, error)

	// MarkPublished removes a record once the broker has confirmed it.
	MarkPublished(ctx context.Context, id string) error
}

// outboxFailureRecorder is the part of a store that bounds how long the relay
// stops at a record nothing can publish.
//
// Deliberately not part of [OutboxStore]. Adding a method to that interface
// would stop every custom store compiling, and the cost of that is higher than
// the cost of a store without this keeping the behaviour it already has: a
// record the broker will never take is retried on every sweep and holds up
// everything behind it. The relay type-asserts for this and calls it when it is
// there; both stores here have it.
//
// A custom store should implement it, and exclude records at the limit from
// Pending:
//
//	func (s *MyStore) RecordFailure(ctx context.Context, id, cause string) error
type outboxFailureRecorder interface {
	RecordFailure(ctx context.Context, id, cause string) error
}

// InMemoryOutboxStore is an outbox in this process.
//
// It has none of the property the pattern exists for: nothing here shares a
// transaction with your database, so a crash between the work committing and
// the record being written loses the message exactly as publishing directly
// would. It is for tests and for seeing the shape of the thing.
type InMemoryOutboxStore struct {
	mu      sync.Mutex
	records map[string]OutboxRecord

	// maxAttempts is how many failures a record takes before it stops being
	// offered. See [DefaultOutboxMaxAttempts].
	maxAttempts int
}

// NewInMemoryOutboxStore returns an empty store.
func NewInMemoryOutboxStore() *InMemoryOutboxStore {
	return &InMemoryOutboxStore{
		records:     map[string]OutboxRecord{},
		maxAttempts: DefaultOutboxMaxAttempts,
	}
}

// SetMaxAttempts changes how many failures a record takes before it stops being
// offered to the relay.
//
// Worth shortening in a test that wants to watch a record retire; worth leaving
// alone otherwise. Anything below one is read as the default, because a store
// that retires a record before trying it once is an outbox that publishes
// nothing.
func (s *InMemoryOutboxStore) SetMaxAttempts(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 1 {
		n = DefaultOutboxMaxAttempts
	}
	s.maxAttempts = n
}

// Add records a message.
func (s *InMemoryOutboxStore) Add(_ context.Context, record OutboxRecord) error {
	if record.ID == "" {
		return fmt.Errorf("acemq: an outbox record needs an ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, present := s.records[record.ID]; present {
		// Already recorded. Adding twice is not an error — the caller may be
		// retrying its own transaction — but it must not become two messages.
		return nil
	}
	s.records[record.ID] = record
	return nil
}

// Pending returns waiting records, oldest first.
func (s *InMemoryOutboxStore) Pending(_ context.Context, limit int) ([]OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]OutboxRecord, 0, len(s.records))
	for _, r := range s.records {
		// A record that has run out of attempts is skipped, which is what lets
		// the relay past it: it stops at the first failure to keep the order the
		// writer chose, and without this bound it stops at the same record for
		// ever.
		if r.Attempts >= s.maxAttempts {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// MarkPublished removes a record.
func (s *InMemoryOutboxStore) MarkPublished(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

// RecordFailure counts a failed publish against a record, and says why.
//
// The record is kept either way. One nothing could publish is evidence: somebody
// has to be able to read it, fix whatever refuses it and release it, and
// deleting it would be the silent loss this pattern exists to prevent arrived at
// from another direction.
func (s *InMemoryOutboxStore) RecordFailure(_ context.Context, id, cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, present := s.records[id]
	if !present {
		return nil
	}
	record.Attempts++
	record.LastError = cause
	s.records[id] = record
	return nil
}

// Retired is the records the relay has stopped trying.
//
// Nothing else surfaces them: Pending exists to skip them, so without this they
// are invisible.
func (s *InMemoryOutboxStore) Retired() []OutboxRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []OutboxRecord
	for _, r := range s.records {
		if r.Attempts >= s.maxAttempts {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// Len is how many records are held, retired ones included.
func (s *InMemoryOutboxStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// Record encodes a payload into an outbox record, ready for [OutboxStore.Add].
//
// Call it inside the transaction that does the work:
//
//	tx, _ := db.Begin()
//	placeOrder(tx, order)
//	record, _ := patterns.Record(mq, "orders-events", "order.placed", event)
//	store.Add(ctx, record)   // the same tx
//	tx.Commit()
func Record[T any](
	conn *acemq.Conn, exchange, routingKey string, payload T, opts ...acemq.EnvelopeOption,
) (OutboxRecord, error) {
	env, err := acemq.NewEnvelope(routingKey, opts...)
	if err != nil {
		return OutboxRecord{}, err
	}

	codec := conn.Codec()
	body, err := codec.Encode(payload)
	if err != nil {
		return OutboxRecord{}, fmt.Errorf(
			"acemq: cannot encode a %T for the outbox: %w", payload, err)
	}

	return OutboxRecord{
		ID:          env.ID,
		Exchange:    exchange,
		RoutingKey:  routingKey,
		Body:        body,
		ContentType: codec.ContentType(),
		Headers:     env.ToWire(),
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// OutboxRelay publishes what the outbox holds.
//
// It is deliberately at-least-once. A record is removed only after the broker
// has confirmed the message, so a crash in between republishes it — which is
// why consumers of anything sent this way need to be idempotent. The
// alternative, removing first, loses messages instead, and a lost message is
// worse than a repeated one.
type OutboxRelay struct {
	conn     *acemq.Conn
	store    OutboxStore
	interval time.Duration
	batch    int

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// RelayOption configures a relay.
type RelayOption func(*OutboxRelay)

// RelayInterval is how often the outbox is swept. One second by default.
func RelayInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// RelayBatch is how many records are published per sweep. A hundred by default.
func RelayBatch(n int) RelayOption {
	return func(r *OutboxRelay) { r.batch = n }
}

// NewOutboxRelay builds a relay. It does nothing until [OutboxRelay.Start].
func NewOutboxRelay(conn *acemq.Conn, store OutboxStore, opts ...RelayOption) *OutboxRelay {
	r := &OutboxRelay{
		conn:     conn,
		store:    store,
		interval: time.Second,
		batch:    100,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Start sweeps the outbox until [OutboxRelay.Close].
func (r *OutboxRelay) Start(ctx context.Context) {
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// A failed sweep is not fatal: the records are still there and
				// the next tick tries again. That is the whole point of the
				// outbox — nothing is lost by the relay being down.
				_, _ = r.Sweep(ctx)
			}
		}
	}()
}

// Sweep publishes one batch and returns how many went out.
//
// Exported so a test can drive the relay without waiting for a tick, and so an
// application can flush the outbox on demand.
//
// # What it reports, and what it cannot
//
// Every record is counted through the connection's [acemq.Observer], published
// or failed, and a published one records how long it waited — see
// [acemq.MetricOutboxLag]. That is the whole of the relay's telemetry, and
// deliberately so: the tracing adapter has an outbox.publish_failed event and an
// outbox_lag_ms attribute, and neither can be written from here. A sweep runs on
// a goroutine of the relay's own with no span open, and opening one per record
// would produce exactly the zero-length spans that adapter exists to avoid. An
// application that wants the trace side calls Sweep itself, from inside a span
// of its own, and calls the adapter's methods with what this returns.
func (r *OutboxRelay) Sweep(ctx context.Context) (int, error) {
	records, err := r.store.Pending(ctx, r.batch)
	if err != nil {
		return 0, fmt.Errorf("acemq: cannot read the outbox: %w", err)
	}

	observer := r.conn.Observer()
	published := 0
	for _, record := range records {
		result, err := r.conn.PublishRaw(ctx, record.Exchange, record.RoutingKey, acemq.Outbound{
			Body:        record.Body,
			ContentType: record.ContentType,
			MessageID:   record.ID,
			Headers:     record.Headers,
			Persistent:  true,
			// Mandatory, as Java's relay is. Without it a record nothing is bound
			// to receive is dropped by the broker without a word, and marked
			// published here: the one message the outbox exists to keep, gone,
			// and counted as sent. A binding that does not exist yet is an
			// ordinary moment in a deployment, not a reason to lose the event.
			Mandatory: true,
		})
		if err == nil && !result.Routed {
			// Failed, not unroutable, in the metric: the outcome Java's relay
			// counts it under.
			err = fmt.Errorf("acemq: nothing is bound to exchange %q for routing key %q (%s)",
				record.Exchange, record.RoutingKey, result.ReturnReason)
		}
		if err != nil {
			// Left in the outbox. Stopping rather than continuing keeps the
			// order records were written in, which is usually what the writer
			// intended.
			acemq.ObserveOutbox(observer, record.Exchange, record.RoutingKey,
				acemq.OutcomeFailed, 0)

			// Counted against the record too, so stopping here is bounded. A
			// record the broker will never take would otherwise hold up
			// everything written after it on every tick, for ever.
			//
			// Optional on the store, so a custom one keeps compiling and keeps
			// the behaviour it already has — see [outboxFailureRecorder]. A
			// failure to write the count is not worth losing the publish error
			// over: that error is what the caller needs, and the next sweep will
			// try to count again.
			if recorder, ok := r.store.(outboxFailureRecorder); ok {
				_ = recorder.RecordFailure(ctx, record.ID, err.Error())
			}
			return published, fmt.Errorf(
				"acemq: cannot publish outbox record %s: %w", record.ID, err)
		}

		// Measured from when the record was written rather than from when this
		// sweep claimed it. What a lag answers is how long somebody has been
		// owed this message, and the wait for a sweep is part of the answer
		// rather than the start of it.
		acemq.ObserveOutbox(observer, record.Exchange, record.RoutingKey,
			acemq.OutcomePublished, time.Since(record.CreatedAt))

		if err := r.store.MarkPublished(ctx, record.ID); err != nil {
			// Published but not marked. The next sweep will publish it again,
			// which is the at-least-once this pattern promises.
			return published, fmt.Errorf(
				"acemq: outbox record %s was published but not marked, so it will be sent again: %w",
				record.ID, err)
		}
		published++
	}
	return published, nil
}

// Close stops the relay and waits for the sweep in progress.
func (r *OutboxRelay) Close() error {
	r.closeOnce.Do(func() {
		close(r.stop)
		<-r.done
	})
	return nil
}
