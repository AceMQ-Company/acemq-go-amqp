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
