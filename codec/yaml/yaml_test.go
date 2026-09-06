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

package yaml

import (
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type Deployment struct {
	Service string   `yaml:"service"`
	Version string   `yaml:"version"`
	Regions []string `yaml:"regions"`
}

func TestARoundTrip(t *testing.T) {
	codec := Codec{}
	body, err := codec.Encode(Deployment{
		Service: "orders", Version: "1.4.2", Regions: []string{"eu-west-1", "us-east-1"}})
	if err != nil {
		t.Fatal(err)
	}

	var back Deployment
	if err := codec.Decode(body, &back); err != nil {
		t.Fatal(err)
	}
	if back.Service != "orders" || len(back.Regions) != 2 {
		t.Errorf("decoded %+v", back)
	}
}

func TestItWritesBlockStyleBecauseThatIsThePoint(t *testing.T) {
	// Flow style would produce something all but indistinguishable from JSON,
	// leaving nothing to justify YAML costing more to parse.
	body, err := Codec{}.Encode(Deployment{Service: "orders", Regions: []string{"eu-west-1"}})
	if err != nil {
		t.Fatal(err)
	}

	out := string(body)
	if !strings.Contains(out, "service: orders") {
		t.Errorf("wrote %s", out)
	}
	if !strings.Contains(out, "- eu-west-1") {
		t.Errorf("the list is not in block style: %s", out)
	}
}

// TestItNeverAnswersForAnUntypedMessage guards the trap this codec has.
//
// YAML is a superset of JSON, so this parser reads JSON bytes happily.
// Answering for an untyped message would give the right value from the wrong
// codec, and be discovered much later as traffic recorded under a format nobody
// sent.
func TestItNeverAnswersForAnUntypedMessage(t *testing.T) {
	codec := Codec{}

	if codec.CanDecode("") {
		t.Error("it claimed a message with no content type")
	}
	if codec.CanDecode("application/json") {
		t.Error("it claimed a JSON message")
	}
	for _, ct := range []string{"application/yaml", "application/x-yaml", "text/yaml", "application/vnd.acme+yaml"} {
		if !codec.CanDecode(ct) {
			t.Errorf("it refused %s", ct)
		}
	}
}

func TestItReallyDoesParseJsonWhichIsWhyThatMatters(t *testing.T) {
	var back Deployment
	if err := (Codec{}).Decode([]byte(`{"service":"orders"}`), &back); err != nil {
		t.Fatal(err)
	}
	if back.Service != "orders" {
		t.Error("the demonstration failed; the hazard may no longer exist")
	}
}

func TestAMalformedBodyIsFatal(t *testing.T) {
	var back Deployment
	err := Codec{}.Decode([]byte("service: orders\n  bad: [indent\n"), &back)

	if err == nil {
		t.Fatal("malformed YAML decoded without complaint")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so the message would be retried for ever")
	}
}

func TestItIsAvailableByName(t *testing.T) {
	codec, err := acemq.CodecByName("yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := codec.(Codec); !ok {
		t.Errorf("CodecByName returned %T", codec)
	}
}
