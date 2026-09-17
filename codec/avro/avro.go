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

// Package avro is the Avro codec.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/avro
//
// Compact on the wire and, unlike protobuf, able to resolve a writer's schema
// against a reader's — which is what lets a producer add a field without every
// consumer being redeployed the same afternoon.
//
// Two modes. [Of] carries no schema on the wire and expects both ends to hold
// the same one, which is smallest and most brittle. [Registered] frames each
// message with a schema identifier, the way Confluent's clients do, so a
// consumer can look up what a message was written with.
//
// Looking it up is half of what evolution needs. The other half is [ReadAs],
// which hands Avro a reader schema alongside the writer's and lets it resolve
// the two: a field the writer added and this consumer has never heard of is
// skipped, and a field the writer has not started sending yet is filled in from
// the reader's default. Without it a consumer reads whatever shape the producer
// happened to send, which is fine until the shapes differ.
package avro

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"

	"github.com/hamba/avro/v2"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// FixedContentType is what a codec with a fixed schema writes.
const FixedContentType = "avro/binary"

// RegisteredContentType is what a codec framing a schema identifier writes.
const RegisteredContentType = "application/vnd.acemq.avro"

// magic and frameSize are Confluent's wire framing: one zero byte, then four
// bytes of schema identifier, big-endian, then the body. Matching it is what
// lets Confluent's clients, the Java library and this one read each other.
const (
	magic     = 0x00
	frameSize = 5
)

// Codec reads and writes Avro.
type Codec struct {
	schema     avro.Schema
	reader     avro.Schema
	resolver   *avro.SchemaCompatibility
	registry   patterns.SchemaRegistry
	subject    string
	registered bool

	mu       sync.RWMutex
	schemaID int
	byID     map[int]avro.Schema
	resolved map[int]avro.Schema
}

// Of reads and writes with one schema, carrying nothing on the wire.
//
// The smallest option, and the most brittle: a consumer must already hold the
// schema the producer used, so changing it means deploying both ends together.
func Of(schema string) (*Codec, error) {
	parsed, err := avro.Parse(schema)
	if err != nil {
		return nil, acemq.Fatalf("acemq: this is not a usable Avro schema: %v", err)
	}
	return &Codec{
		schema:   parsed,
		byID:     map[int]avro.Schema{},
		resolved: map[int]avro.Schema{},
	}, nil
}

// Registered frames each message with a schema identifier from a registry.
//
// The mode to use where producers and consumers are deployed independently. The
// subject groups the versions of one message type, conventionally the message
// type itself.
//
// Pass [ReadAs] to read every message against a schema of this consumer's own
// rather than the producer's.
func Registered(
	registry patterns.SchemaRegistry, subject, schema string, options ...Option,
) (*Codec, error) {
	codec, err := Of(schema)
	if err != nil {
		return nil, err
	}
	codec.registry = registry
	codec.subject = subject
	codec.registered = true
	for _, option := range options {
		if err := option(codec); err != nil {
			return nil, err
		}
	}
	return codec, nil
}

// Option configures a codec built with [Registered].
type Option func(*Codec) error

// ReadAs resolves every message onto a schema of this consumer's own, instead
// of reading it with the schema the producer wrote it with.
//
// This is the half of schema evolution a registry alone does not give you. A
// registered codec without it decodes each message against the writer's schema,
// so the consumer sees whatever shape the producer sent: a field it has never
// heard of arrives, and a field it expects is simply absent — read back as the
// zero value — until the producer starts sending it. Given both schemas Avro
// resolves the difference, so an unknown field is skipped and an absent one is
// filled in from the reader's own default. The consumer then sees the shape it
// was written against, whichever version wrote the message.
//
//	consumer, err := avro.Registered(registry, "order.placed", schema,
//	    avro.ReadAs(schema))
//
// Passing the codec's own schema, as above, is the ordinary case: a consumer
// reads what it was compiled against. The two are separate arguments because
// they are separate schemas — the first is what this codec writes, the second
// is what it reads — and a process that both publishes and consumes may want
// them to differ.
//
// Resolution happens once per writer schema and is remembered, so the cost is
// paid on the first message carrying an identifier and not again. A writer
// schema that cannot be resolved onto this one is an error naming both, rather
// than a decode that returns the wrong values.
func ReadAs(schema string) Option {
	return func(c *Codec) error {
		parsed, err := avro.Parse(schema)
		if err != nil {
			return acemq.Fatalf("acemq: this is not a usable Avro reader schema: %v", err)
		}
		c.reader = parsed
		c.resolver = avro.NewSchemaCompatibility()
		return nil
	}
}

// ContentType depends on which mode this codec is in.
func (c *Codec) ContentType() string {
	if c.registered {
		return RegisteredContentType
	}
	return FixedContentType
}

// IsRegistered reports whether messages carry a schema identifier.
func (c *Codec) IsRegistered() bool { return c.registered }

// Encode marshals a payload.
func (c *Codec) Encode(payload any) ([]byte, error) {
	body, err := avro.Marshal(c.schema, payload)
	if err != nil {
		return nil, acemq.Fatalf("acemq: cannot write a %T as Avro: %v", payload, err)
	}
	if !c.registered {
		return body, nil
	}

	id, err := c.currentID()
	if err != nil {
		return nil, err
	}

	framed := make([]byte, frameSize, frameSize+len(body))
	framed[0] = magic
	binary.BigEndian.PutUint32(framed[1:frameSize], uint32(id))
	return append(framed, body...), nil
}

// Decode unmarshals into dst.
func (c *Codec) Decode(body []byte, dst any) error {
	schema := c.schema

	if c.registered {
		if len(body) < frameSize || body[0] != magic {
			// The two modes produce different bytes, and mixing them is a
			// configuration mistake worth naming rather than a decode that
			// quietly returns rubbish.
			return acemq.Fatalf(
				"acemq: this message carries no schema identifier, so it was written by a " +
					"codec built with Of rather than Registered")
		}
		id := int(binary.BigEndian.Uint32(body[1:frameSize]))
		writer, err := c.schemaByID(id)
		if err != nil {
			return err
		}
		schema = writer
		if c.reader != nil {
			// Both schemas go to Avro, which is the whole point of ReadAs: it
			// resolves the difference, so a field the writer added and this
			// reader does not know is skipped rather than shifting every field
			// after it, and one the writer omitted comes back as the reader's
			// default rather than a zero value that means nothing.
			schema, err = c.resolveOnto(id, writer)
			if err != nil {
				return err
			}
		}
		body = body[frameSize:]
	}

	if err := avro.Unmarshal(schema, body, dst); err != nil {
		return acemq.Fatalf("acemq: this message is not Avro that reads as %T: %v", dst, err)
	}
	return nil
}

// CanDecode accepts the Avro content types this codec's mode can actually read,
// and never an absent one.
//
// The mode matters here and the two spellings are not interchangeable. A
// fixed-schema codec that claimed a registry-framed message would hand the five
// Confluent framing bytes to the Avro reader as though they were the start of
// the first field, and Avro does not object: it reads the shifted bytes as
// whatever they happen to mean and returns a record where every value is wrong,
// with no error and no log line. Refusing the content type turns silent
// corruption into a message no codec claimed, which is a thing somebody can
// see. The Java, .NET, Python and Ruby libraries gate the same way.
//
//   - [Of] claims [FixedContentType] and refuses [RegisteredContentType].
//   - [Registered] claims [RegisteredContentType] and refuses [FixedContentType].
//
// application/avro and any +avro suffix type say Avro without saying which
// framing, so both modes take them: a producer writing one of those has said
// nothing about how it framed the body, and refusing them would leave a message
// nothing would read.
func (c *Codec) CanDecode(contentType string) bool {
	if contentType == "" {
		return false
	}
	lower := strings.ToLower(contentType)
	switch {
	case strings.HasPrefix(lower, RegisteredContentType):
		return c.registered
	case strings.HasPrefix(lower, FixedContentType):
		return !c.registered
	}
	return strings.HasPrefix(lower, "application/avro") ||
		strings.Contains(lower, "+avro")
}

// currentID registers this codec's schema once and remembers the identifier.
func (c *Codec) currentID() (int, error) {
	c.mu.RLock()
	id := c.schemaID
	c.mu.RUnlock()
	if id != 0 {
		return id, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemaID != 0 {
		return c.schemaID, nil
	}

	definition, err := c.registry.Register(
		context.Background(), c.subject, "avro", c.schema.String())
	if err != nil {
		return 0, err
	}
	c.schemaID = definition.ID
	return c.schemaID, nil
}

// schemaByID looks up the schema a message was written with, and remembers it.
func (c *Codec) schemaByID(id int) (avro.Schema, error) {
	c.mu.RLock()
	cached, present := c.byID[id]
	c.mu.RUnlock()
	if present {
		return cached, nil
	}

	definition, err := c.registry.ByID(context.Background(), id)
	if err != nil {
		return nil, err
	}
	parsed, err := avro.Parse(definition.Definition)
	if err != nil {
		return nil, acemq.Fatalf("acemq: schema %d in the registry is not usable Avro: %v", id, err)
	}

	c.mu.Lock()
	c.byID[id] = parsed
	c.mu.Unlock()
	return parsed, nil
}

// resolveOnto composes the writer's schema with this codec's reader schema, and
// remembers the result: an identifier stands for one schema forever, so the
// answer never changes and resolving is not cheap enough to redo per message.
func (c *Codec) resolveOnto(id int, writer avro.Schema) (avro.Schema, error) {
	c.mu.RLock()
	cached, present := c.resolved[id]
	c.mu.RUnlock()
	if present {
		return cached, nil
	}

	composite, err := c.resolver.Resolve(c.reader, writer)
	if err != nil {
		// Both schemas, because neither on its own says what went wrong: the
		// two are usually different versions of one record and share a name.
		return nil, acemq.Fatalf(
			"acemq: schema %d cannot be read as this consumer's schema: %v\n"+
				"  written with: %s\n"+
				"  read as: %s",
			id, err, writer.String(), c.reader.String())
	}

	c.mu.Lock()
	c.resolved[id] = composite
	c.mu.Unlock()
	return composite, nil
}
