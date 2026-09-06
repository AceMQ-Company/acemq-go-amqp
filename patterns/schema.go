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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// SchemaDefinition is one version of a message's shape.
type SchemaDefinition struct {
	// ID is assigned by the registry and is what goes on the wire.
	ID int

	// Subject groups the versions of one message type, conventionally the
	// message type itself: "order.placed".
	Subject string

	// Version counts from 1 within a subject.
	Version int

	// Format is "avro", "protobuf", "json-schema" — whatever the schema is
	// written in. The registry does not interpret it.
	Format string

	// Definition is the schema itself.
	Definition string

	// Fingerprint is a hash of the definition, so the same schema registered
	// twice gets the same identifier rather than a second version.
	Fingerprint string

	// RegisteredAt is when it was first seen.
	RegisteredAt time.Time
}

// SchemaRegistry remembers message shapes so a producer and a consumer can
// disagree about their version without disagreeing about their meaning.
//
// A message carries a small identifier rather than its whole schema, and the
// consumer looks it up. That is what lets a producer add a field without every
// consumer being redeployed the same afternoon.
type SchemaRegistry interface {
	// Register records a schema and returns it with an identifier. Registering
	// the same definition twice returns the same identifier rather than making
	// a second version.
	Register(ctx context.Context, subject, format, definition string) (SchemaDefinition, error)

	// ByID looks a schema up by the identifier on a message.
	ByID(ctx context.Context, id int) (SchemaDefinition, error)

	// Latest is the newest version of a subject.
	Latest(ctx context.Context, subject string) (SchemaDefinition, error)

	// Versions lists every version of a subject, oldest first.
	Versions(ctx context.Context, subject string) ([]SchemaDefinition, error)
}

// ErrSchemaNotFound is returned when a lookup finds nothing.
//
// A distinct error rather than a zero value, because a consumer reading a
// message whose schema it cannot find has a real problem — usually a producer
// that registered against a different registry — and should not carry on with
// an empty definition.
var ErrSchemaNotFound = fmt.Errorf("acemq: no such schema")

// InMemorySchemaRegistry keeps schemas in this process.
//
// For tests, and for a single service that wants the shape of the thing. It is
// not a registry in the sense that matters: nothing is shared between
// processes, so a consumer cannot look up a schema a producer registered
// elsewhere, which is the entire point of having one. Use a database-backed
// registry, or Confluent's, for anything real.
type InMemorySchemaRegistry struct {
	mu        sync.RWMutex
	byID      map[int]SchemaDefinition
	bySubject map[string][]SchemaDefinition
	byPrint   map[string]SchemaDefinition
	nextID    int
}

// NewInMemorySchemaRegistry returns an empty registry.
func NewInMemorySchemaRegistry() *InMemorySchemaRegistry {
	return &InMemorySchemaRegistry{
		byID:      map[int]SchemaDefinition{},
		bySubject: map[string][]SchemaDefinition{},
		byPrint:   map[string]SchemaDefinition{},
		nextID:    1,
	}
}

// Register records a schema, or returns the existing one when it is identical.
func (r *InMemorySchemaRegistry) Register(
	_ context.Context, subject, format, definition string,
) (SchemaDefinition, error) {
	if subject == "" || definition == "" {
		return SchemaDefinition{}, fmt.Errorf("acemq: a schema needs a subject and a definition")
	}

	fingerprint := Fingerprint(definition)

	r.mu.Lock()
	defer r.mu.Unlock()

	// The same definition registered twice is the same schema. Without this a
	// service that registers on every start would add a version per restart.
	if existing, present := r.byPrint[subject+"/"+fingerprint]; present {
		return existing, nil
	}

	schema := SchemaDefinition{
		ID:           r.nextID,
		Subject:      subject,
		Version:      len(r.bySubject[subject]) + 1,
		Format:       format,
		Definition:   definition,
		Fingerprint:  fingerprint,
		RegisteredAt: time.Now().UTC(),
	}
	r.nextID++

	r.byID[schema.ID] = schema
	r.bySubject[subject] = append(r.bySubject[subject], schema)
	r.byPrint[subject+"/"+fingerprint] = schema
	return schema, nil
}

// ByID looks up a schema.
func (r *InMemorySchemaRegistry) ByID(_ context.Context, id int) (SchemaDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schema, present := r.byID[id]
	if !present {
		return SchemaDefinition{}, fmt.Errorf("%w with id %d", ErrSchemaNotFound, id)
	}
	return schema, nil
}

// Latest is the newest version of a subject.
func (r *InMemorySchemaRegistry) Latest(_ context.Context, subject string) (SchemaDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := r.bySubject[subject]
	if len(versions) == 0 {
		return SchemaDefinition{}, fmt.Errorf("%w for subject %q", ErrSchemaNotFound, subject)
	}
	return versions[len(versions)-1], nil
}

// Versions lists a subject's schemas, oldest first.
func (r *InMemorySchemaRegistry) Versions(_ context.Context, subject string) ([]SchemaDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := append([]SchemaDefinition(nil), r.bySubject[subject]...)
	sort.Slice(versions, func(i, j int) bool { return versions[i].Version < versions[j].Version })
	return versions, nil
}

// Fingerprint hashes a schema definition.
//
// SHA-256 of the exact bytes: two definitions that differ only in whitespace
// hash differently and are treated as different schemas. Normalising would need
// a parser per format, and a registry that silently treated two definitions as
// one because it mis-parsed them would be worse than one that is strict.
func Fingerprint(definition string) string {
	sum := sha256.Sum256([]byte(definition))
	return hex.EncodeToString(sum[:])
}
