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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DB is the part of database/sql these stores use.
//
// Both *sql.DB and *sql.Tx satisfy it, which is the whole point: the outbox
// only works if a record can be written in the caller's transaction, and an
// idempotency key only holds if it is claimed in the same one as the work.
//
//	tx, _ := db.BeginTx(ctx, nil)
//	placeOrder(ctx, tx, order)
//	store := patterns.NewSQLOutboxStore(tx, patterns.PostgresDialect)
//	store.Add(ctx, record)
//	tx.Commit()
//
// A store built on *sql.DB has its own connection and so has the gap back: the
// work commits, the process dies, and the message was never recorded.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Dialect is the handful of things that differ between databases.
//
// Not an abstraction over SQL — the statements here are ordinary — but the
// placeholder syntax and the way each spells "insert unless it is already
// there" genuinely differ, and getting either wrong fails at runtime on one
// database and not another.
type Dialect struct {
	// Name is for error messages.
	Name string

	// Placeholder renders the nth parameter, counting from 1.
	Placeholder func(n int) string

	// InsertIgnoreSuffix is appended to an INSERT to make a duplicate key a
	// no-op rather than an error.
	InsertIgnoreSuffix string
}

// PostgresDialect uses $1 placeholders and ON CONFLICT DO NOTHING.
var PostgresDialect = Dialect{
	Name:               "postgres",
	Placeholder:        func(n int) string { return fmt.Sprintf("$%d", n) },
	InsertIgnoreSuffix: " ON CONFLICT DO NOTHING",
}

// MySQLDialect uses ? placeholders and INSERT IGNORE semantics.
var MySQLDialect = Dialect{
	Name:               "mysql",
	Placeholder:        func(int) string { return "?" },
	InsertIgnoreSuffix: " ON DUPLICATE KEY UPDATE id = id",
}

// SQLiteDialect uses ? placeholders and ON CONFLICT DO NOTHING.
var SQLiteDialect = Dialect{
	Name:               "sqlite",
	Placeholder:        func(int) string { return "?" },
	InsertIgnoreSuffix: " ON CONFLICT DO NOTHING",
}

func (d Dialect) args(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = d.Placeholder(i + 1)
	}
	return out
}

// SQLIdempotencyStore remembers handled messages in a database table.
//
// Unlike the in-memory one this is shared between processes, which is what
// makes it hold when more than one consumer is running. Give it the same
// transaction as the work, and a duplicate cannot slip between claiming the key
// and doing the thing.
type SQLIdempotencyStore struct {
	db      DB
	dialect Dialect
	table   string
}

// NewSQLIdempotencyStore uses the table named by [SQLIdempotencyStore.Schema].
func NewSQLIdempotencyStore(db DB, dialect Dialect, table ...string) *SQLIdempotencyStore {
	name := "acemq_idempotency"
	if len(table) > 0 && table[0] != "" {
		name = table[0]
	}
	return &SQLIdempotencyStore{db: db, dialect: dialect, table: name}
}

// Schema is the table this store needs.
//
// Returned rather than created, because a library that runs DDL against
// somebody's database on start-up is a library that fights their migration
// tool. Put it in a migration.
func (s *SQLIdempotencyStore) Schema() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  key         VARCHAR(255) PRIMARY KEY,
  handled_at  TIMESTAMP    NOT NULL
);
CREATE INDEX IF NOT EXISTS %s_handled_at ON %s (handled_at);`, s.table, s.table, s.table)
}

// FirstTime claims a key, and reports whether this caller got it.
//
// The claim is an insert that does nothing when the key is already there, and
// the answer is whether a row was written. That makes it atomic in the database
// rather than in this process, which is what two consumers racing requires.
func (s *SQLIdempotencyStore) FirstTime(ctx context.Context, key string) (bool, error) {
	args := s.dialect.args(2)
	query := fmt.Sprintf("INSERT INTO %s (key, handled_at) VALUES (%s, %s)%s",
		s.table, args[0], args[1], s.dialect.InsertIgnoreSuffix)

	result, err := s.db.ExecContext(ctx, query, key, time.Now().UTC())
	if err != nil {
		return false, fmt.Errorf("acemq: cannot claim idempotency key %q: %w", key, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// Some drivers do not report it. Saying "not first" would drop the
		// message; saying "first" risks a duplicate, which is the recoverable
		// half of the choice.
		return true, nil
	}
	return affected > 0, nil
}

// Forget releases a key so a failed message can be tried again.
func (s *SQLIdempotencyStore) Forget(ctx context.Context, key string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE key = %s", s.table, s.dialect.Placeholder(1))
	if _, err := s.db.ExecContext(ctx, query, key); err != nil {
		return fmt.Errorf("acemq: cannot release idempotency key %q: %w", key, err)
	}
	return nil
}

// Prune deletes keys older than a cutoff.
//
// Nothing does this on a timer: a library that deleted rows from somebody's
// database on a schedule they did not set would be overstepping. Call it from a
// job, or leave the rows and let the table grow.
func (s *SQLIdempotencyStore) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE handled_at < %s", s.table, s.dialect.Placeholder(1))
	result, err := s.db.ExecContext(ctx, query, time.Now().UTC().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("acemq: cannot prune %s: %w", s.table, err)
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

// SQLOutboxStore holds messages in a database table until they are published.
//
// This is the one that makes the outbox worth having. Give it the transaction
// that does the work and the record commits with it, so there is no moment
// where the work happened and the message does not exist.
type SQLOutboxStore struct {
	db      DB
	dialect Dialect
	table   string
}

// NewSQLOutboxStore uses the table named by [SQLOutboxStore.Schema].
func NewSQLOutboxStore(db DB, dialect Dialect, table ...string) *SQLOutboxStore {
	name := "acemq_outbox"
	if len(table) > 0 && table[0] != "" {
		name = table[0]
	}
	return &SQLOutboxStore{db: db, dialect: dialect, table: name}
}

// Schema is the table this store needs. Put it in a migration.
func (s *SQLOutboxStore) Schema() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id            VARCHAR(255) PRIMARY KEY,
  exchange      VARCHAR(255) NOT NULL,
  routing_key   VARCHAR(255) NOT NULL,
  body          BLOB         NOT NULL,
  content_type  VARCHAR(255) NOT NULL,
  headers       TEXT         NOT NULL,
  created_at    TIMESTAMP    NOT NULL
);
CREATE INDEX IF NOT EXISTS %s_created_at ON %s (created_at);`, s.table, s.table, s.table)
}

// Add records a message to be published.
//
// A record that is already there is left alone rather than being an error: a
// caller retrying its own transaction must not turn one message into two.
func (s *SQLOutboxStore) Add(ctx context.Context, record OutboxRecord) error {
	if record.ID == "" {
		return errors.New("acemq: an outbox record needs an ID")
	}

	headers, err := json.Marshal(record.Headers)
	if err != nil {
		return fmt.Errorf("acemq: cannot write the headers of outbox record %s: %w", record.ID, err)
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}

	args := s.dialect.args(7)
	query := fmt.Sprintf(
		"INSERT INTO %s (id, exchange, routing_key, body, content_type, headers, created_at) "+
			"VALUES (%s, %s, %s, %s, %s, %s, %s)%s",
		s.table, args[0], args[1], args[2], args[3], args[4], args[5], args[6],
		s.dialect.InsertIgnoreSuffix)

	_, err = s.db.ExecContext(ctx, query,
		record.ID, record.Exchange, record.RoutingKey,
		record.Body, record.ContentType, string(headers), record.CreatedAt)
	if err != nil {
		return fmt.Errorf("acemq: cannot record outbox message %s: %w", record.ID, err)
	}
	return nil
}

// Pending returns records waiting to be published, oldest first.
func (s *SQLOutboxStore) Pending(ctx context.Context, limit int) ([]OutboxRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	query := fmt.Sprintf(
		"SELECT id, exchange, routing_key, body, content_type, headers, created_at "+
			"FROM %s ORDER BY created_at ASC LIMIT %s", s.table, s.dialect.Placeholder(1))

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot read the outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []OutboxRecord
	for rows.Next() {
		var record OutboxRecord
		var headers string
		if err := rows.Scan(&record.ID, &record.Exchange, &record.RoutingKey,
			&record.Body, &record.ContentType, &headers, &record.CreatedAt); err != nil {
			return nil, fmt.Errorf("acemq: cannot read an outbox row: %w", err)
		}
		if headers != "" {
			if err := json.Unmarshal([]byte(headers), &record.Headers); err != nil {
				return nil, fmt.Errorf(
					"acemq: cannot read the headers of outbox record %s: %w", record.ID, err)
			}
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("acemq: cannot read the outbox: %w", err)
	}
	return out, nil
}

// MarkPublished removes a record once the broker has confirmed it.
func (s *SQLOutboxStore) MarkPublished(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", s.table, s.dialect.Placeholder(1))
	if _, err := s.db.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("acemq: cannot mark outbox record %s published: %w", id, err)
	}
	return nil
}

// SQLSchemaRegistry keeps schemas in a database table, so producers and
// consumers in different processes can agree about them.
type SQLSchemaRegistry struct {
	db      DB
	dialect Dialect
	table   string
}

// NewSQLSchemaRegistry uses the table named by [SQLSchemaRegistry.Schema].
func NewSQLSchemaRegistry(db DB, dialect Dialect, table ...string) *SQLSchemaRegistry {
	name := "acemq_schemas"
	if len(table) > 0 && table[0] != "" {
		name = table[0]
	}
	return &SQLSchemaRegistry{db: db, dialect: dialect, table: name}
}

// Schema is the table this registry needs. Put it in a migration.
func (r *SQLSchemaRegistry) Schema() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id            INTEGER      PRIMARY KEY AUTOINCREMENT,
  subject       VARCHAR(255) NOT NULL,
  version       INTEGER      NOT NULL,
  format        VARCHAR(64)  NOT NULL,
  definition    TEXT         NOT NULL,
  fingerprint   VARCHAR(64)  NOT NULL,
  registered_at TIMESTAMP    NOT NULL,
  UNIQUE (subject, fingerprint)
);`, r.table)
}

// Register records a schema, or returns the existing one when it is identical.
func (r *SQLSchemaRegistry) Register(
	ctx context.Context, subject, format, definition string,
) (SchemaDefinition, error) {
	if subject == "" || definition == "" {
		return SchemaDefinition{}, errors.New("acemq: a schema needs a subject and a definition")
	}
	fingerprint := Fingerprint(definition)

	// Already there? The same definition is the same schema, and registering on
	// every start must not add a version per restart.
	existing, err := r.byFingerprint(ctx, subject, fingerprint)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, ErrSchemaNotFound) {
		return SchemaDefinition{}, err
	}

	var version int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE subject = %s",
		r.table, r.dialect.Placeholder(1))
	if err := r.db.QueryRowContext(ctx, countQuery, subject).Scan(&version); err != nil {
		return SchemaDefinition{}, fmt.Errorf("acemq: cannot count schema versions: %w", err)
	}
	version++

	args := r.dialect.args(6)
	insert := fmt.Sprintf(
		"INSERT INTO %s (subject, version, format, definition, fingerprint, registered_at) "+
			"VALUES (%s, %s, %s, %s, %s, %s)%s",
		r.table, args[0], args[1], args[2], args[3], args[4], args[5],
		r.dialect.InsertIgnoreSuffix)

	now := time.Now().UTC()
	if _, err := r.db.ExecContext(ctx, insert,
		subject, version, format, definition, fingerprint, now); err != nil {
		return SchemaDefinition{}, fmt.Errorf("acemq: cannot register schema for %q: %w", subject, err)
	}

	// Read back rather than trusting LastInsertId, which not every driver
	// supports and which says nothing when the insert was ignored.
	return r.byFingerprint(ctx, subject, fingerprint)
}

func (r *SQLSchemaRegistry) byFingerprint(
	ctx context.Context, subject, fingerprint string,
) (SchemaDefinition, error) {
	args := r.dialect.args(2)
	query := fmt.Sprintf(
		"SELECT id, subject, version, format, definition, fingerprint, registered_at "+
			"FROM %s WHERE subject = %s AND fingerprint = %s", r.table, args[0], args[1])

	return r.scanOne(r.db.QueryRowContext(ctx, query, subject, fingerprint))
}

// ByID looks a schema up by its identifier.
func (r *SQLSchemaRegistry) ByID(ctx context.Context, id int) (SchemaDefinition, error) {
	query := fmt.Sprintf(
		"SELECT id, subject, version, format, definition, fingerprint, registered_at "+
			"FROM %s WHERE id = %s", r.table, r.dialect.Placeholder(1))

	schema, err := r.scanOne(r.db.QueryRowContext(ctx, query, id))
	if errors.Is(err, ErrSchemaNotFound) {
		return schema, fmt.Errorf("%w with id %d", ErrSchemaNotFound, id)
	}
	return schema, err
}

// Latest is the newest version of a subject.
func (r *SQLSchemaRegistry) Latest(ctx context.Context, subject string) (SchemaDefinition, error) {
	query := fmt.Sprintf(
		"SELECT id, subject, version, format, definition, fingerprint, registered_at "+
			"FROM %s WHERE subject = %s ORDER BY version DESC LIMIT 1",
		r.table, r.dialect.Placeholder(1))

	schema, err := r.scanOne(r.db.QueryRowContext(ctx, query, subject))
	if errors.Is(err, ErrSchemaNotFound) {
		return schema, fmt.Errorf("%w for subject %q", ErrSchemaNotFound, subject)
	}
	return schema, err
}

// Versions lists a subject's schemas, oldest first.
func (r *SQLSchemaRegistry) Versions(ctx context.Context, subject string) ([]SchemaDefinition, error) {
	query := fmt.Sprintf(
		"SELECT id, subject, version, format, definition, fingerprint, registered_at "+
			"FROM %s WHERE subject = %s ORDER BY version ASC", r.table, r.dialect.Placeholder(1))

	rows, err := r.db.QueryContext(ctx, query, subject)
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot read schemas for %q: %w", subject, err)
	}
	defer func() { _ = rows.Close() }()

	var out []SchemaDefinition
	for rows.Next() {
		var s SchemaDefinition
		if err := rows.Scan(&s.ID, &s.Subject, &s.Version, &s.Format,
			&s.Definition, &s.Fingerprint, &s.RegisteredAt); err != nil {
			return nil, fmt.Errorf("acemq: cannot read a schema row: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (r *SQLSchemaRegistry) scanOne(row *sql.Row) (SchemaDefinition, error) {
	var s SchemaDefinition
	err := row.Scan(&s.ID, &s.Subject, &s.Version, &s.Format,
		&s.Definition, &s.Fingerprint, &s.RegisteredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return SchemaDefinition{}, ErrSchemaNotFound
	}
	if err != nil {
		return SchemaDefinition{}, fmt.Errorf("acemq: cannot read a schema: %w", err)
	}
	return s, nil
}
