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
}

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

// InMemoryOutboxStore is an outbox in this process.
//
// It has none of the property the pattern exists for: nothing here shares a
// transaction with your database, so a crash between the work committing and
// the record being written loses the message exactly as publishing directly
// would. It is for tests and for seeing the shape of the thing.
type InMemoryOutboxStore struct {
	mu      sync.Mutex
	records map[string]OutboxRecord
}

// NewInMemoryOutboxStore returns an empty store.
func NewInMemoryOutboxStore() *InMemoryOutboxStore {
	return &InMemoryOutboxStore{records: map[string]OutboxRecord{}}
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

// Len is how many records are waiting.
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
func (r *OutboxRelay) Sweep(ctx context.Context) (int, error) {
	records, err := r.store.Pending(ctx, r.batch)
	if err != nil {
		return 0, fmt.Errorf("acemq: cannot read the outbox: %w", err)
	}

	published := 0
	for _, record := range records {
		result, err := r.conn.PublishRaw(ctx, record.Exchange, record.RoutingKey, acemq.Outbound{
			Body:        record.Body,
			ContentType: record.ContentType,
			MessageID:   record.ID,
			Headers:     record.Headers,
			Persistent:  true,
		})
		if err != nil {
			// Left in the outbox. Stopping rather than continuing keeps the
			// order records were written in, which is usually what the writer
			// intended.
			return published, fmt.Errorf(
				"acemq: cannot publish outbox record %s: %w", record.ID, err)
		}
		_ = result

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
