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

// Package xml is the XML codec.
//
// It needs nothing outside the standard library, but lives in its own package
// anyway so the core's list of formats does not grow by one every time somebody
// wants a different one.
//
// Reach for it when talking to something that already speaks XML — an older
// system, a partner's integration — rather than by choice. It is larger on the
// wire than JSON and slower to parse.
package xml

import (
	"encoding/xml"
	"strings"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// ContentType is what this codec writes, and what Java and .NET write.
const ContentType = "application/xml"

// Codec reads and writes XML.
//
// Field names come from the struct's own xml tags, exactly as the JSON codec
// takes them from json tags. A field with no tag goes on the wire under its Go
// name, which will not match what another language writes.
type Codec struct{}

// ContentType returns application/xml.
func (Codec) ContentType() string { return ContentType }

// Encode marshals a payload.
func (Codec) Encode(payload any) ([]byte, error) {
	encoded, err := xml.Marshal(payload)
	if err != nil {
		return nil, acemq.Fatalf("acemq: cannot write a %T as XML: %v", payload, err)
	}
	return encoded, nil
}

// Decode unmarshals into dst.
//
// A body that is not XML, or that does not fit the destination, fails the same
// way every time, so this is fatal and the message is dead-lettered rather than
// retried until it ages out.
func (Codec) Decode(body []byte, dst any) error {
	if err := xml.Unmarshal(body, dst); err != nil {
		return acemq.Fatalf("acemq: this message is not XML that reads as %T: %v", dst, err)
	}
	return nil
}

// CanDecode accepts application/xml, text/xml and any +xml media type.
//
// It never answers for a message whose sender set no content type. XML is not
// the default format, and claiming an untyped message would record traffic
// under a format nobody sent.
func (Codec) CanDecode(contentType string) bool {
	if contentType == "" {
		return false
	}
	lower := strings.ToLower(contentType)
	return strings.HasPrefix(lower, ContentType) ||
		strings.HasPrefix(lower, "text/xml") ||
		strings.Contains(lower, "+xml")
}

func init() {
	acemq.RegisterCodec("xml", func() acemq.Codec { return Codec{} })
}
