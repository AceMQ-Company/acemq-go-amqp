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

package acemq

import (
	"encoding/json"
	"strings"
)

// JSONContentType is what the JSON codec writes, and what Java and .NET write.
const JSONContentType = "application/json"

// JSONCodec reads and writes JSON. It is the default, and the one format every
// AceMQ library has without an extra dependency.
//
// Field names come from the struct's own json tags. Unlike the Java and .NET
// codecs there is no camelCase policy to configure, because Go already puts the
// wire name in the tag:
//
//	type OrderPlaced struct {
//		ID         string `json:"orderId"`
//		TotalCents int64  `json:"totalCents"`
//	}
//
// A field with no tag goes on the wire under its Go name, capital and all, and
// will not match what Java writes. Tag the fields.
type JSONCodec struct{}

// ContentType returns application/json.
func (JSONCodec) ContentType() string { return JSONContentType }

// Encode marshals a payload.
func (JSONCodec) Encode(payload any) ([]byte, error) {
	return json.Marshal(payload)
}

// Decode unmarshals into dst.
//
// A body that is not JSON, or that does not fit the destination, fails the same
// way on every attempt, so the error is marked fatal and the message is
// dead-lettered rather than retried until it ages out.
func (JSONCodec) Decode(body []byte, dst any) error {
	if err := json.Unmarshal(body, dst); err != nil {
		return Fatalf("acemq: this message is not JSON that reads as %T: %w", dst, err)
	}
	return nil
}

// CanDecode accepts application/json and any +json media type.
//
// Unlike the YAML and TOML codecs in the other libraries, this one does answer
// for a message whose sender set no content type: JSON is the default format,
// so an untyped message is far more likely to be JSON than anything else, and
// something has to read it.
func (JSONCodec) CanDecode(contentType string) bool {
	if contentType == "" {
		return true
	}
	lower := strings.ToLower(contentType)
	return strings.HasPrefix(lower, JSONContentType) ||
		strings.HasPrefix(lower, "text/json") ||
		strings.Contains(lower, "+json")
}
