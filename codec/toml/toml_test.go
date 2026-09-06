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

package toml

import (
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type Flags struct {
	Service string   `toml:"service"`
	Enabled bool     `toml:"enabled"`
	Regions []string `toml:"regions"`
}

func TestARoundTrip(t *testing.T) {
	codec := Codec{}
	body, err := codec.Encode(Flags{Service: "orders", Enabled: true, Regions: []string{"eu-west-1"}})
	if err != nil {
		t.Fatal(err)
	}

	var back Flags
	if err := codec.Decode(body, &back); err != nil {
		t.Fatal(err)
	}
	if back.Service != "orders" || !back.Enabled {
		t.Errorf("decoded %+v", back)
	}
}

// TestThereIsNoNorwayProblem is the reason to prefer TOML over YAML for
// something a person edits.
//
// In YAML "country: NO" is the boolean false. Here an unquoted NO is not a
// value at all, so the mistake is a parse error rather than a country turning
// into false somewhere downstream.
func TestThereIsNoNorwayProblem(t *testing.T) {
	var back struct {
		Country string `toml:"country"`
	}
	err := Codec{}.Decode([]byte("country = NO\n"), &back)

	if err == nil {
		t.Fatal("an unquoted NO was accepted")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so the message would be retried for ever")
	}
}

func TestADuplicateKeyIsRefusedRatherThanResolved(t *testing.T) {
	// Silently taking the first or the last would produce a message that means
	// something other than it looks like.
	var back Flags
	if err := (Codec{}).Decode([]byte("service = \"a\"\nservice = \"b\"\n"), &back); err == nil {
		t.Fatal("a duplicate key was accepted")
	}
}

func TestItNeverAnswersForAnUntypedMessage(t *testing.T) {
	codec := Codec{}
	if codec.CanDecode("") {
		t.Error("it claimed a message with no content type")
	}
	if codec.CanDecode("application/json") {
		t.Error("it claimed a JSON message")
	}
	for _, ct := range []string{"application/toml", "text/toml", "application/vnd.acme+toml"} {
		if !codec.CanDecode(ct) {
			t.Errorf("it refused %s", ct)
		}
	}
}

func TestSomethingThatIsNotTableShapedSaysSo(t *testing.T) {
	// TOML is a table format, and saying so is better than emitting something
	// that is not TOML.
	_, err := Codec{}.Encode([]string{"a", "b"})

	if err == nil {
		t.Fatal("a bare list was encoded as TOML")
	}
	if !strings.Contains(err.Error(), "table format") {
		t.Errorf("the error does not explain: %v", err)
	}
}

func TestItIsAvailableByName(t *testing.T) {
	codec, err := acemq.CodecByName("toml")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := codec.(Codec); !ok {
		t.Errorf("CodecByName returned %T", codec)
	}
}
