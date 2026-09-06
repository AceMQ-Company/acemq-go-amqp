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

// Package protobuf is the Protocol Buffers codec.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/protobuf
//
// For high volume, and for a contract shared with services in other languages.
// Messages are much smaller than JSON and parse faster, at the cost of being
// unreadable without the schema — which is the trade, not an oversight.
//
// Payloads must be generated protobuf types. Anything else is refused rather
// than encoded some other way, because a queue where half the messages are
// protobuf and half are JSON is a queue nobody can read.
package protobuf

import (
	"strings"

	"google.golang.org/protobuf/proto"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// ContentType is what this codec writes, and what Java and .NET write.
const ContentType = "application/x-protobuf"

// Codec reads and writes Protocol Buffers.
type Codec struct{}

// ContentType returns application/x-protobuf.
func (Codec) ContentType() string { return ContentType }

// Encode marshals a generated protobuf message.
func (Codec) Encode(payload any) ([]byte, error) {
	message, ok := payload.(proto.Message)
	if !ok {
		return nil, acemq.Fatalf(
			"acemq: %T is not a protobuf message; this codec needs a generated type "+
				"(one with a ProtoReflect method)", payload)
	}
	encoded, err := proto.Marshal(message)
	if err != nil {
		return nil, acemq.Fatalf("acemq: cannot write a %T as protobuf: %v", payload, err)
	}
	return encoded, nil
}

// Decode unmarshals into dst, which must be a generated protobuf message.
func (Codec) Decode(body []byte, dst any) error {
	message, ok := dst.(proto.Message)
	if !ok {
		return acemq.Fatalf(
			"acemq: %T is not a protobuf message; this codec needs a generated type", dst)
	}
	if err := proto.Unmarshal(body, message); err != nil {
		return acemq.Fatalf("acemq: this message is not protobuf that reads as %T: %v", dst, err)
	}
	return nil
}

// CanDecode accepts the protobuf content types, and never an absent one.
//
// Protobuf is not self-describing: any bytes may parse as some message without
// error, so answering for an untyped message would produce a value that looks
// fine and is not.
func (Codec) CanDecode(contentType string) bool {
	if contentType == "" {
		return false
	}
	lower := strings.ToLower(contentType)
	return strings.HasPrefix(lower, ContentType) ||
		strings.HasPrefix(lower, "application/protobuf") ||
		strings.HasPrefix(lower, "application/vnd.google.protobuf") ||
		strings.Contains(lower, "+protobuf")
}

func init() {
	acemq.RegisterCodec("protobuf", func() acemq.Codec { return Codec{} })
}
