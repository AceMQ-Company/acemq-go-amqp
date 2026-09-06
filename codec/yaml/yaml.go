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

// Package yaml is the YAML codec.
//
//	go get github.com/AceMQ-Company/acemq-go-amqp/codec/yaml
//
// Chosen when a message is meant to be read by a person as much as by a
// program: a configuration change broadcast to a fleet, a deployment
// instruction, a command replayed by hand from a dead-letter queue. It costs
// more to parse than JSON and is a poor choice for high volume; it earns its
// place where somebody will actually look at the message.
package yaml

import (
	"strings"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"gopkg.in/yaml.v3"
)

// ContentType is what this codec writes, and what Java and .NET write.
const ContentType = "application/yaml"

// Codec reads and writes YAML.
//
// It never volunteers for a message whose sender set no content type. YAML is a
// superset of JSON, so its parser accepts JSON bytes quite happily and would
// answer for messages meant for the JSON codec — giving the right value while
// recording that a YAML message arrived, which is the sort of wrong discovered
// much later.
type Codec struct{}

// ContentType returns application/yaml.
func (Codec) ContentType() string { return ContentType }

// Encode marshals a payload in block style, which is the whole reason to pick
// YAML: flow style produces something all but indistinguishable from JSON.
func (Codec) Encode(payload any) ([]byte, error) {
	encoded, err := yaml.Marshal(payload)
	if err != nil {
		return nil, acemq.Fatalf("acemq: cannot write a %T as YAML: %v", payload, err)
	}
	return encoded, nil
}

// Decode unmarshals into dst.
func (Codec) Decode(body []byte, dst any) error {
	if err := yaml.Unmarshal(body, dst); err != nil {
		return acemq.Fatalf("acemq: this message is not YAML that reads as %T: %v", dst, err)
	}
	return nil
}

// CanDecode accepts the content types that say YAML, and never an absent one.
func (Codec) CanDecode(contentType string) bool {
	if contentType == "" {
		return false
	}
	lower := strings.ToLower(contentType)
	return strings.HasPrefix(lower, ContentType) ||
		// Both were in use long before application/yaml was registered.
		strings.HasPrefix(lower, "application/x-yaml") ||
		strings.HasPrefix(lower, "text/yaml") ||
		strings.Contains(lower, "+yaml")
}

func init() {
	acemq.RegisterCodec("yaml", func() acemq.Codec { return Codec{} })
}
