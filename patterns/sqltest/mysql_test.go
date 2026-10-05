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

package sqltest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AceMQ-Company/acemq-go-amqp/patterns"

	_ "github.com/go-sql-driver/mysql"
)

// openMySQL connects to the MySQL named by ACEMQ_TEST_MYSQL_DSN, or skips.
//
// A DSN in go-sql-driver form, for example
// root:acemq@tcp(127.0.0.1:33306)/acemq. Each test gets a table of its own so
// they cannot see each other's keys.
func openMySQL(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("ACEMQ_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ACEMQ_TEST_MYSQL_DSN is not set; no MySQL to test against")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	table := fmt.Sprintf("acemq_idem_%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	return db, table
}

// TestTheMySQLDialectClaimsConfirmsAndReleases is every idempotency statement
// against a real MySQL. Until it existed none of them had ever run there: the
// schema named a column `key`, which MySQL reserves, and the insert's
// ON DUPLICATE KEY UPDATE named an `id` column the table does not have, so every
// claim failed.
func TestTheMySQLDialectClaimsConfirmsAndReleases(t *testing.T) {
	ctx := context.Background()
	db, table := openMySQL(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.MySQLDialect, table)
	apply(t, db, store.Schema())

	first, err := store.Claim(ctx, "m-1")
	if err != nil {
		t.Fatalf("the first claim failed: %v", err)
	}
	if !first {
		t.Fatal("the first claim was refused")
	}
	if again, err := store.Claim(ctx, "m-1"); err != nil || again {
		t.Fatalf("a live claim was taken twice (%v, %v)", again, err)
	}

	if err := store.Release(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}
	if again, err := store.Claim(ctx, "m-1"); err != nil || !again {
		t.Fatalf("a released key could not be claimed again (%v, %v)", again, err)
	}

	if err := store.Confirm(ctx, "m-1"); err != nil {
		t.Fatal(err)
	}
	store.SetClaimTimeout(time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if again, err := store.Claim(ctx, "m-1"); err != nil || again {
		t.Fatalf("a confirmed key was claimed again (%v, %v)", again, err)
	}

	// An unconfirmed claim older than the timeout is taken over.
	if _, err := store.Claim(ctx, "m-2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if stolen, err := store.Claim(ctx, "m-2"); err != nil || !stolen {
		t.Fatalf("an abandoned claim was not taken over (%v, %v)", stolen, err)
	}

	removed, err := store.Prune(ctx, -time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("pruned %d rows, want 2", removed)
	}
}

func TestTheMySQLStoreTellsAClaimFromADuplicateFromWorkInProgress(t *testing.T) {
	db, table := openMySQL(t)
	store := patterns.NewSQLIdempotencyStore(db, patterns.MySQLDialect, table)
	apply(t, db, store.Schema())
	claimsThreeWays(t, store)
}

// The quoting is MySQL's alone: the SQLite and Postgres schemas, which people
// already have in migrations, must not change.
func TestOnlyTheMySQLSchemaQuotesTheKeyColumn(t *testing.T) {
	for _, d := range []patterns.Dialect{patterns.PostgresDialect, patterns.SQLiteDialect} {
		schema := patterns.NewSQLIdempotencyStore(nil, d).Schema()
		if strings.Contains(schema, "`") || !strings.Contains(schema, "  key ") {
			t.Errorf("%s schema changed:\n%s", d.Name, schema)
		}
	}
	if schema := patterns.NewSQLIdempotencyStore(nil, patterns.MySQLDialect).Schema(); !strings.Contains(schema, "`key`") {
		t.Errorf("the MySQL schema does not quote the reserved column:\n%s", schema)
	}
}

// TestTheMySQLOutboxAddsListsFailsAndPublishes is every outbox statement against
// a real MySQL. Its schema used CREATE INDEX IF NOT EXISTS, which MySQL does not
// have, so the table could not be created there from the documented DDL.
func TestTheMySQLOutboxAddsListsFailsAndPublishes(t *testing.T) {
	ctx := context.Background()
	db, table := openMySQL(t)
	store := patterns.NewSQLOutboxStore(db, patterns.MySQLDialect, table)
	store.SetMaxAttempts(2)
	apply(t, db, store.Schema())

	base := time.Now().UTC().Truncate(time.Second)
	for i, id := range []string{"o-1", "o-2", "o-3"} {
		record := patterns.OutboxRecord{
			ID: id, Exchange: "orders", RoutingKey: "order.placed",
			Body: []byte(`{"n":` + fmt.Sprint(i) + `}`), ContentType: "application/json",
			Headers: map[string]any{"tenant": "t-1"},
			// Inside one second: the order survives only if the column keeps
			// the fraction.
			CreatedAt: base.Add(time.Duration(i) * time.Millisecond),
		}
		if err := store.Add(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	// A retried transaction adds the same record again; it must stay one.
	if err := store.Add(ctx, patterns.OutboxRecord{ID: "o-1", Exchange: "orders", Body: []byte("x")}); err != nil {
		t.Fatalf("adding a record twice failed: %v", err)
	}

	pending, err := store.Pending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(pending); got != "o-1,o-2,o-3" {
		t.Fatalf("pending = %s, want o-1,o-2,o-3 in order", got)
	}
	first := pending[0]
	if string(first.Body) != `{"n":0}` || first.ContentType != "application/json" ||
		first.Headers["tenant"] != "t-1" || !first.CreatedAt.Equal(base) {
		t.Errorf("the record did not round-trip: %+v", first)
	}
	if limited, err := store.Pending(ctx, 1); err != nil || len(limited) != 1 {
		t.Fatalf("the limit was not applied (%d, %v)", len(limited), err)
	}

	for range 2 {
		if err := store.RecordFailure(ctx, "o-2", "NOT_FOUND - no exchange"); err != nil {
			t.Fatal(err)
		}
	}
	if pending, err = store.Pending(ctx, 10); err != nil || ids(pending) != "o-1,o-3" {
		t.Fatalf("a retired record is still offered (%s, %v)", ids(pending), err)
	}
	retired, err := store.Retired(ctx)
	if err != nil || len(retired) != 1 || retired[0].LastError != "NOT_FOUND - no exchange" {
		t.Fatalf("the retired record was not kept with its reason (%+v, %v)", retired, err)
	}

	for _, id := range []string{"o-1", "o-3"} {
		if err := store.MarkPublished(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if pending, err = store.Pending(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("published records are still pending (%s, %v)", ids(pending), err)
	}
}

// TestTheMySQLSchemaRegistryRegistersAndLooksUp is every registry statement
// against a real MySQL. Its schema said AUTOINCREMENT, which is SQLite's
// spelling; MySQL's is AUTO_INCREMENT, so the table could not be created there.
func TestTheMySQLSchemaRegistryRegistersAndLooksUp(t *testing.T) {
	ctx := context.Background()
	db, table := openMySQL(t)
	registry := patterns.NewSQLSchemaRegistry(db, patterns.MySQLDialect, table)
	apply(t, db, registry.Schema())

	v1, err := registry.Register(ctx, "order.placed", "json-schema", `{"type":"object"}`)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Version != 1 || v1.ID == 0 || v1.RegisteredAt.IsZero() {
		t.Fatalf("first registration = %+v", v1)
	}
	again, err := registry.Register(ctx, "order.placed", "json-schema", `{"type":"object"}`)
	if err != nil || again.ID != v1.ID || again.Version != 1 {
		t.Fatalf("re-registering the same definition added a version (%+v, %v)", again, err)
	}
	v2, err := registry.Register(ctx, "order.placed", "json-schema", `{"type":"object","required":["id"]}`)
	if err != nil || v2.Version != 2 || v2.ID == v1.ID {
		t.Fatalf("second definition = %+v, %v", v2, err)
	}

	if byID, err := registry.ByID(ctx, v1.ID); err != nil || byID.Definition != `{"type":"object"}` {
		t.Fatalf("ByID = %+v, %v", byID, err)
	}
	if latest, err := registry.Latest(ctx, "order.placed"); err != nil || latest.ID != v2.ID {
		t.Fatalf("Latest = %+v, %v", latest, err)
	}
	versions, err := registry.Versions(ctx, "order.placed")
	if err != nil || len(versions) != 2 || versions[0].Version != 1 || versions[1].Version != 2 {
		t.Fatalf("Versions = %+v, %v", versions, err)
	}
	if _, err := registry.Latest(ctx, "nobody"); !errors.Is(err, patterns.ErrSchemaNotFound) {
		t.Fatalf("an unknown subject = %v, want ErrSchemaNotFound", err)
	}
}

// The SQLite and Postgres DDL is in people's migrations already and must not
// change; only MySQL gets its own.
func TestOnlyTheMySQLSchemasDiffer(t *testing.T) {
	for _, d := range []patterns.Dialect{patterns.PostgresDialect, patterns.SQLiteDialect} {
		if s := patterns.NewSQLOutboxStore(nil, d).Schema(); !strings.Contains(s, "CREATE INDEX IF NOT EXISTS") {
			t.Errorf("%s outbox schema changed:\n%s", d.Name, s)
		}
		if s := patterns.NewSQLSchemaRegistry(nil, d).Schema(); !strings.Contains(s, "AUTOINCREMENT") {
			t.Errorf("%s registry schema changed:\n%s", d.Name, s)
		}
	}
	for _, s := range []string{
		patterns.NewSQLOutboxStore(nil, patterns.MySQLDialect).Schema(),
		patterns.NewSQLSchemaRegistry(nil, patterns.MySQLDialect).Schema(),
	} {
		if strings.Contains(s, "CREATE INDEX") ||
			strings.Contains(s, "AUTOINCREMENT") {
			t.Errorf("the MySQL schema has syntax MySQL rejects:\n%s", s)
		}
	}
}

func ids(records []patterns.OutboxRecord) string {
	out := make([]string, len(records))
	for i, r := range records {
		out[i] = r.ID
	}
	return strings.Join(out, ",")
}
