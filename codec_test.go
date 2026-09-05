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
	"slices"
	"strings"
	"testing"
)

func TestJSONRoundTrip(t *testing.T) {
	codec := JSONCodec{}

	body, err := codec.Encode(OrderPlaced{OrderID: "o-1", TotalCents: 4250})
	if err != nil {
		t.Fatal(err)
	}

	var back OrderPlaced
	if err := codec.Decode(body, &back); err != nil {
		t.Fatal(err)
	}
	if back.OrderID != "o-1" || back.TotalCents != 4250 {
		t.Errorf("decoded %+v", back)
	}
}

func TestJSONWritesTheNamesInTheStructTags(t *testing.T) {
	// The tag is what makes a Go field and a Java field the same field. Without
	// one the name goes on the wire capitalised and nothing else reads it.
	body, err := JSONCodec{}.Encode(OrderPlaced{OrderID: "o-1", TotalCents: 4250})
	if err != nil {
		t.Fatal(err)
	}

	got := string(body)
	if !strings.Contains(got, `"orderId"`) {
		t.Errorf("wrote %s, want the json tag name", got)
	}
	if strings.Contains(got, `"OrderID"`) {
		t.Errorf("wrote the Go field name: %s", got)
	}
}

func TestAMalformedBodyIsFatalRatherThanRetryable(t *testing.T) {
	// The same bytes fail the same way every time, so the message is
	// dead-lettered rather than retried until it ages out.
	var back OrderPlaced
	err := JSONCodec{}.Decode([]byte("this is not json"), &back)

	if err == nil {
		t.Fatal("a malformed body decoded without complaint")
	}
	if !IsFatal(err) {
		t.Errorf("the error is not marked fatal, so the message would be retried for ever: %v", err)
	}
}

func TestJSONAnswersForAMessageWithNoContentType(t *testing.T) {
	// Unlike the YAML and TOML codecs in the other libraries, this one does
	// answer for an untyped message: JSON is the default, and something has to
	// read a message whose sender set nothing.
	codec := JSONCodec{}

	if !codec.CanDecode("") {
		t.Error("the default codec refused a message with no content type")
	}
	if !codec.CanDecode("application/json") {
		t.Error("application/json was refused")
	}
	if !codec.CanDecode("application/vnd.acme.order+json") {
		t.Error("a +json media type was refused")
	}
	if codec.CanDecode("application/x-protobuf") {
		t.Error("the JSON codec claimed a protobuf message")
	}
}

func TestTheRegistryFindsACodecByName(t *testing.T) {
	got, err := CodecByName("json")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(JSONCodec); !ok {
		t.Errorf("CodecByName(\"json\") returned %T", got)
	}
	if !slices.Contains(CodecNames(), "json") {
		t.Errorf("CodecNames() = %v, want it to include json", CodecNames())
	}
}

func TestAnUnknownCodecNameListsTheKnownOnes(t *testing.T) {
	_, err := CodecByName("smoke-signals")

	if err == nil {
		t.Fatal("an unregistered codec name was accepted")
	}
	// Somebody who typoed a name cannot see the registry, so the error has to
	// show it.
	if !strings.Contains(err.Error(), "json") {
		t.Errorf("the error does not list what is available: %v", err)
	}
}

func TestFatalSurvivesWrapping(t *testing.T) {
	wrapped := Fatalf("outer: %w", Fatal(errFor("inner")))

	if !IsFatal(wrapped) {
		t.Error("a fatal error stopped reading as fatal once wrapped")
	}
	if IsFatal(errFor("ordinary")) {
		t.Error("an ordinary error read as fatal")
	}
	if IsFatal(nil) {
		t.Error("nil read as fatal")
	}
}

type simpleError string

func (e simpleError) Error() string { return string(e) }

func errFor(s string) error { return simpleError(s) }
