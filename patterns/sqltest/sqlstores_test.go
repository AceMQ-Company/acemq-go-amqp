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

// Package sqltest exercises the database-backed stores against a real database.
//
// It is a module of its own so the driver's newer Go requirement stays here
// rather than becoming the whole library's.
package sqltest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	_ "modernc.org/sqlite"
)

type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	// One connection, so the shared in-memory database is not dropped when an
	// idle connection is reaped and the schema vanishes mid-test.
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func apply(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("cannot apply %q: %v", statement, err)
		}
	}
}

func memoryBroker(t *testing.T) *acemq.Conn {
	t.Helper()
	mq, err := acemq.Connect(context.Background(), "memory://"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

// ---- idempotency -----------------------------------------------------

// TestAKeyIsClaimedOnce is the property the store exists for.
//
// The in-memory store cannot have it across processes: each has its own memory
// and both consumers believe they are first. Here the claim is an insert, and
// the database decides.
func TestAKeyIsClaimedOnce(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())

	first, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}

	if !first {
		t.Error("the first claim was refused")
	}
	if second {
		t.Error("the same key was claimed twice; two consumers would both handle the message")
	}
}

func TestAReleasedKeyCanBeClaimedAgain(t *testing.T) {
	// The ordering that makes this a duplicate guard rather than a message
	// eater: a failed message has to be able to run again.
	ctx := context.Background()
	db := openDB(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())

	if _, err := store.Claim(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}

	again, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if !again {
		t.Error("a released key was still held, so the retry would silently do nothing")
	}
}

func TestClaimingInATransactionHoldsWithTheWork(t *testing.T) {
	// The arrangement that actually closes the gap: the key and the work commit
	// together, or neither does. A rolled-back transaction must leave the key
	// unclaimed.
	ctx := context.Background()
	db := openDB(t)
	apply(t, db, patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect).Schema())

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	inTx := patterns.NewSQLIdempotencyStore(tx, patterns.SQLiteDialect)
	if _, err := inTx.Claim(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	after := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	first, err := after.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Error("the key survived a rolled-back transaction, so the work would never be done")
	}
}

func TestPruningRemovesOldKeys(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())

	if _, err := store.Claim(ctx, "old"); err != nil {
		t.Fatal(err)
	}

	// Nothing prunes on a timer, so this is the only way rows go away.
	removed, err := store.Prune(ctx, -time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("pruned %d rows, want 1", removed)
	}
}

// ---- outbox ----------------------------------------------------------

func TestTheOutboxRoundTripsThroughTheDatabase(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	mq := memoryBroker(t)

	store := patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())

	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		t.Fatal(err)
	}

	pending, err := store.Pending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("%d records pending, want 1", len(pending))
	}
	if pending[0].ID != record.ID || pending[0].RoutingKey != "orders" {
		t.Errorf("read back %+v", pending[0])
	}
	// The envelope has to survive the database, or a replayed message loses
	// its identity and its correlation.
	if pending[0].Headers[acemq.HeaderID] == nil {
		t.Errorf("the envelope did not survive: %v", pending[0].Headers)
	}
	if string(pending[0].Body) != string(record.Body) {
		t.Error("the body changed on the way through")
	}
}

func TestRecordingTwiceInTheDatabaseStillSendsOnce(t *testing.T) {
	// A caller retrying its own transaction must not turn one message into two.
	ctx := context.Background()
	db := openDB(t)
	mq := memoryBroker(t)

	store := patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())

	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := store.Add(ctx, record); err != nil {
			t.Fatalf("adding the same record again failed: %v", err)
		}
	}

	pending, err := store.Pending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Errorf("the outbox holds %d copies of one message", len(pending))
	}
}

// TestTheRecordCommitsWithTheWork is the whole reason the pattern exists.
func TestTheRecordCommitsWithTheWork(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	mq := memoryBroker(t)

	store := patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())
	if _, err := db.ExecContext(ctx, `CREATE TABLE orders (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	// The work and the message, together, and then abandoned.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO orders (id) VALUES (?)`, "o-1"); err != nil {
		t.Fatal(err)
	}
	if err := patterns.NewSQLOutboxStore(tx, patterns.SQLiteDialect).Add(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	pending, err := store.Pending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Neither happened. Publishing directly would have sent a message about an
	// order that does not exist.
	if len(pending) != 0 {
		t.Errorf("%d messages survived a transaction that was rolled back", len(pending))
	}

	var orders int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orders`).Scan(&orders); err != nil {
		t.Fatal(err)
	}
	if orders != 0 {
		t.Errorf("%d orders survived the rollback", orders)
	}
}

func TestTheRelayPublishesFromTheDatabase(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	mq := memoryBroker(t)
	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	store := patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())

	got := make(chan OrderPlaced, 1)
	sub, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		t.Fatal(err)
	}

	moved, err := patterns.NewOutboxRelay(mq, store).Sweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Errorf("the sweep moved %d records", moved)
	}

	select {
	case order := <-got:
		if order.OrderID != "o-1" {
			t.Errorf("got %+v", order)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the message never arrived")
	}

	remaining, err := store.Pending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Errorf("%d records left after publishing", len(remaining))
	}
}

// ---- schema registry -------------------------------------------------

func TestTheSchemaRegistryRoundTrips(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	registry := patterns.NewSQLSchemaRegistry(db, patterns.SQLiteDialect)
	apply(t, db, registry.Schema())

	first, err := registry.Register(ctx, "order.placed", "avro", `{"v":1}`)
	if err != nil {
		t.Fatal(err)
	}
	// A service that registers on start-up must not add a version per restart.
	same, err := registry.Register(ctx, "order.placed", "avro", `{"v":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != same.ID {
		t.Errorf("the same schema got two identifiers: %d and %d", first.ID, same.ID)
	}

	second, err := registry.Register(ctx, "order.placed", "avro", `{"v":2}`)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != 2 {
		t.Errorf("Version = %d, want 2", second.Version)
	}

	found, err := registry.ByID(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found.Definition != `{"v":1}` {
		t.Errorf("ByID returned %+v", found)
	}

	latest, err := registry.Latest(ctx, "order.placed")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != 2 {
		t.Errorf("Latest is version %d", latest.Version)
	}

	versions, err := registry.Versions(ctx, "order.placed")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 || versions[0].Version != 1 {
		t.Errorf("Versions = %v", versions)
	}
}

func TestALookupThatFindsNothingSaysSo(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	registry := patterns.NewSQLSchemaRegistry(db, patterns.SQLiteDialect)
	apply(t, db, registry.Schema())

	if _, err := registry.ByID(ctx, 999); !errors.Is(err, patterns.ErrSchemaNotFound) {
		t.Errorf("got %v, want ErrSchemaNotFound", err)
	}
	if _, err := registry.Latest(ctx, "nothing"); !errors.Is(err, patterns.ErrSchemaNotFound) {
		t.Errorf("got %v, want ErrSchemaNotFound", err)
	}
}

// The claim a killed process leaves behind, against the real store.
//
// This is the defect 0.8.0 fixed, and the table is where it lived: a row used to say
// only "somebody has seen this key", so a process killed mid-handler left one that
// suppressed the redelivery for ever and the work was silently lost. The `confirmed`
// column is what tells a claim from a completion, and the claim timeout is what lets
// an abandoned one go.
func TestAnAbandonedClaimExpiresAndTheWorkCanBeDone(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())
	store.SetClaimTimeout(time.Millisecond)

	// The process that is about to be killed claims the key, and never confirms.
	claimed, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("a fresh key must be claimable")
	}

	time.Sleep(20 * time.Millisecond)

	// The consumer that took over gets the redelivery.
	again, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if !again {
		t.Fatal("an abandoned claim still suppresses the redelivery: the work a killed " +
			"process never finished would be silently dropped")
	}
}

// A confirmed key is a completion, and no timeout reopens it.
func TestAConfirmedKeyIsNotReclaimedWhenTheClaimWindowPasses(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())
	store.SetClaimTimeout(time.Millisecond)

	if _, err := store.Claim(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Confirm(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)

	again, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Error("a confirmed key was reclaimed after the claim window, so completed work " +
			"would be done twice")
	}
}

// A live claim is not stealable, which is the other half of the window: two consumers
// must not both hold the same key while the first is still working.
func TestALiveClaimIsNotStolen(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
	apply(t, db, store.Schema())
	store.SetClaimTimeout(time.Hour)

	if _, err := store.Claim(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}

	again, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Error("a claim inside its window was taken by a second caller")
	}
}
