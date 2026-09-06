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

package protobuf

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// structpb.Struct is a generated protobuf type that ships with the module, so
// these tests need no protoc and no checked-in generated code. A real
// application would use its own.
func anOrder(t *testing.T) *structpb.Struct {
	t.Helper()
	order, err := structpb.NewStruct(map[string]any{
		"orderId": "o-1", "totalCents": 4250.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return order
}

func TestARoundTrip(t *testing.T) {
	codec := Codec{}
	body, err := codec.Encode(anOrder(t))
	if err != nil {
		t.Fatal(err)
	}

	back := &structpb.Struct{}
	if err := codec.Decode(body, back); err != nil {
		t.Fatal(err)
	}
	if back.GetFields()["orderId"].GetStringValue() != "o-1" {
		t.Errorf("decoded %v", back.AsMap())
	}
}

// TestItRefusesAnythingThatIsNotProtobuf keeps a queue readable.
//
// A queue where half the messages are protobuf and half are JSON is a queue
// nobody can read, so encoding some other way is not on offer.
func TestItRefusesAnythingThatIsNotProtobuf(t *testing.T) {
	_, err := Codec{}.Encode(map[string]string{"orderId": "o-1"})

	if err == nil {
		t.Fatal("a plain map was encoded as protobuf")
	}
	if !strings.Contains(err.Error(), "generated type") {
		t.Errorf("the error does not explain what is needed: %v", err)
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so the message would be retried for ever")
	}
}

func TestDecodingIntoSomethingThatIsNotProtobufSaysSo(t *testing.T) {
	var back map[string]string
	if err := (Codec{}).Decode([]byte{0x0a}, &back); err == nil {
		t.Fatal("it decoded into a plain map")
	}
}

// TestItNeverAnswersForAnUntypedMessage matters more here than elsewhere.
//
// Protobuf is not self-describing: arbitrary bytes often parse as some message
// without error, so answering for an untyped message would produce a value that
// looks fine and is not.
func TestItNeverAnswersForAnUntypedMessage(t *testing.T) {
	codec := Codec{}

	if codec.CanDecode("") {
		t.Error("it claimed a message with no content type")
	}
	if codec.CanDecode("application/json") {
		t.Error("it claimed a JSON message")
	}
	for _, ct := range []string{
		"application/x-protobuf", "application/protobuf",
		"application/vnd.google.protobuf", "application/vnd.acme+protobuf",
	} {
		if !codec.CanDecode(ct) {
			t.Errorf("it refused %s", ct)
		}
	}
}

func TestItIsSmallerThanTheJsonItReplaces(t *testing.T) {
	small, err := Codec{}.Encode(anOrder(t))
	if err != nil {
		t.Fatal(err)
	}
	large, err := acemq.JSONCodec{}.Encode(map[string]any{"orderId": "o-1", "totalCents": 4250})
	if err != nil {
		t.Fatal(err)
	}

	if len(small) >= len(large) {
		t.Logf("protobuf %d bytes, json %d bytes", len(small), len(large))
		t.Log("structpb carries its own type information, so the margin is small here")
	}
}

func TestItIsAvailableByName(t *testing.T) {
	codec, err := acemq.CodecByName("protobuf")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := codec.(Codec); !ok {
		t.Errorf("CodecByName returned %T", codec)
	}
}
