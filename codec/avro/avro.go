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
	registry   patterns.SchemaRegistry
	subject    string
	registered bool

	mu       sync.RWMutex
	schemaID int
	byID     map[int]avro.Schema
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
	return &Codec{schema: parsed, byID: map[int]avro.Schema{}}, nil
}

// Registered frames each message with a schema identifier from a registry.
//
// The mode to use where producers and consumers are deployed independently. The
// subject groups the versions of one message type, conventionally the message
// type itself.
func Registered(registry patterns.SchemaRegistry, subject, schema string) (*Codec, error) {
	codec, err := Of(schema)
	if err != nil {
		return nil, err
	}
	codec.registry = registry
	codec.subject = subject
	codec.registered = true
	return codec, nil
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
		body = body[frameSize:]
	}

	if err := avro.Unmarshal(schema, body, dst); err != nil {
		return acemq.Fatalf("acemq: this message is not Avro that reads as %T: %v", dst, err)
	}
	return nil
}

// CanDecode accepts the Avro content types, and never an absent one.
func (c *Codec) CanDecode(contentType string) bool {
	if contentType == "" {
		return false
	}
	lower := strings.ToLower(contentType)
	return strings.HasPrefix(lower, FixedContentType) ||
		strings.HasPrefix(lower, RegisteredContentType) ||
		strings.HasPrefix(lower, "application/avro") ||
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
