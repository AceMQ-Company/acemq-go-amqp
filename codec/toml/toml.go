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

// Package toml is the TOML codec.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/toml
//
// The same audience as YAML — a message a person reads and edits — with the
// ambiguity removed. One way to write a string, no significant indentation, and
// no Norway problem: in YAML "country: NO" is the boolean false, while here an
// unquoted NO is a parse error rather than a country that quietly became false.
// Where a human edits the message and a machine acts on it, that matters more
// than terseness.
package toml

import (
	"bytes"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// ContentType is what this codec writes, and what Java and .NET write.
const ContentType = "application/toml"

// Codec reads and writes TOML.
//
// TOML is a table format, so a message body must be an object at the top level.
// A bare list or number is not a TOML document, and this says so rather than
// emitting something that is not TOML.
type Codec struct{}

// ContentType returns application/toml.
func (Codec) ContentType() string { return ContentType }

// Encode marshals a payload.
//
// A payload that is not table-shaped is refused here rather than at the far
// end. The encoder will happily write a bare list as ["a", "b"] and a number as
// 42, neither of which is a TOML document — so a message would go out that
// nothing can read back, and the failure would surface in the consumer with no
// clue where it came from.
func (Codec) Encode(payload any) ([]byte, error) {
	if err := mustBeTable(payload); err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if err := toml.NewEncoder(&out).Encode(payload); err != nil {
		return nil, acemq.Fatalf("acemq: cannot write a %T as TOML: %v", payload, err)
	}
	return out.Bytes(), nil
}

// mustBeTable rejects a payload that cannot be a TOML document.
func mustBeTable(payload any) error {
	if payload == nil {
		return acemq.Fatalf("acemq: cannot write nil as TOML")
	}

	t := reflect.TypeOf(payload)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Struct, reflect.Map:
		return nil
	default:
		return acemq.Fatalf(
			"acemq: a %s cannot be written as TOML. TOML is a table format, so a message "+
				"body has to be a struct or a map at the top level; use JSONCodec where the "+
				"payload is a list, a scalar, or a deep tree", t.Kind())
	}
}

// Decode unmarshals into dst.
func (Codec) Decode(body []byte, dst any) error {
	if _, err := toml.Decode(string(body), dst); err != nil {
		return acemq.Fatalf("acemq: this message is not TOML that reads as %T: %v", dst, err)
	}
	return nil
}

// CanDecode accepts the content types that say TOML, and never an absent one.
func (Codec) CanDecode(contentType string) bool {
	if contentType == "" {
		return false
	}
	lower := strings.ToLower(contentType)
	return strings.HasPrefix(lower, ContentType) ||
		strings.HasPrefix(lower, "text/toml") ||
		strings.Contains(lower, "+toml")
}

func init() {
	acemq.RegisterCodec("toml", func() acemq.Codec { return Codec{} })
}
