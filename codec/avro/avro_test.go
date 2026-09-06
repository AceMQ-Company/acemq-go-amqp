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

package avro

import (
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

const v1 = `{"type":"record","name":"OrderPlaced","namespace":"acemq.test","fields":[
  {"name":"orderId","type":"string"},
  {"name":"totalCents","type":"long"}]}`

// A field added, with a default, which is what makes it readable by a consumer
// that has never heard of it.
const v2 = `{"type":"record","name":"OrderPlaced","namespace":"acemq.test","fields":[
  {"name":"orderId","type":"string"},
  {"name":"totalCents","type":"long"},
  {"name":"tenant","type":"string","default":""}]}`

type Order struct {
	OrderID    string `avro:"orderId"`
	TotalCents int64  `avro:"totalCents"`
}

type OrderV2 struct {
	OrderID    string `avro:"orderId"`
	TotalCents int64  `avro:"totalCents"`
	Tenant     string `avro:"tenant"`
}

func TestARoundTripWithAFixedSchema(t *testing.T) {
	codec, err := Of(v1)
	if err != nil {
		t.Fatal(err)
	}

	body, err := codec.Encode(Order{OrderID: "A-1", TotalCents: 4250})
	if err != nil {
		t.Fatal(err)
	}

	var back Order
	if err := codec.Decode(body, &back); err != nil {
		t.Fatal(err)
	}
	if back.OrderID != "A-1" || back.TotalCents != 4250 {
		t.Errorf("decoded %+v", back)
	}
	if codec.IsRegistered() {
		t.Error("a fixed-schema codec says it is registered")
	}
}

func TestAFixedSchemaCarriesNoFraming(t *testing.T) {
	// Nothing but the Avro body. The reader is expected to hold the schema,
	// which is the whole trade this mode makes.
	codec, err := Of(v1)
	if err != nil {
		t.Fatal(err)
	}
	body, err := codec.Encode(Order{OrderID: "A-1", TotalCents: 4250})
	if err != nil {
		t.Fatal(err)
	}

	if body[0] == magic {
		t.Error("the body starts with the framing byte, so it looks registered")
	}
}

func TestItIsSmallerThanTheJsonItReplaces(t *testing.T) {
	codec, err := Of(v1)
	if err != nil {
		t.Fatal(err)
	}
	small, err := codec.Encode(Order{OrderID: "A-1", TotalCents: 4250})
	if err != nil {
		t.Fatal(err)
	}
	large, err := acemq.JSONCodec{}.Encode(map[string]any{"orderId": "A-1", "totalCents": 4250})
	if err != nil {
		t.Fatal(err)
	}

	if len(small) >= len(large) {
		t.Errorf("avro %d bytes, json %d bytes", len(small), len(large))
	}
}

// TestItFramesTheWayConfluentDoes is what lets Confluent's clients, the Java
// library and this one read each other.
func TestItFramesTheWayConfluentDoes(t *testing.T) {
	registry := patterns.NewInMemorySchemaRegistry()
	codec, err := Registered(registry, "order.placed", v1)
	if err != nil {
		t.Fatal(err)
	}

	body, err := codec.Encode(Order{OrderID: "A-1"})
	if err != nil {
		t.Fatal(err)
	}

	// One zero byte, then four bytes of identifier, big-endian.
	if body[0] != magic {
		t.Errorf("the first byte is %#x, want %#x", body[0], magic)
	}
	id := int(body[1])<<24 | int(body[2])<<16 | int(body[3])<<8 | int(body[4])
	if id <= 0 {
		t.Errorf("the schema identifier is %d", id)
	}
	if !codec.IsRegistered() {
		t.Error("a registered codec says it is not")
	}
}

// TestAProducerCanAddAFieldWithoutBreakingAnOlderConsumer is the reason the
// registered mode exists.
func TestAProducerCanAddAFieldWithoutBreakingAnOlderConsumer(t *testing.T) {
	registry := patterns.NewInMemorySchemaRegistry()

	producer, err := Registered(registry, "order.placed", v2)
	if err != nil {
		t.Fatal(err)
	}
	body, err := producer.Encode(OrderV2{OrderID: "A-1", TotalCents: 4250, Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}

	// The consumer still holds v1 and has never been redeployed.
	consumer, err := Registered(registry, "order.placed", v1)
	if err != nil {
		t.Fatal(err)
	}
	var back Order
	if err := consumer.Decode(body, &back); err != nil {
		t.Fatal(err)
	}

	if back.OrderID != "A-1" || back.TotalCents != 4250 {
		t.Errorf("decoded %+v", back)
	}
}

func TestTheSameSchemaGetsOneIdentifier(t *testing.T) {
	registry := patterns.NewInMemorySchemaRegistry()
	codec, err := Registered(registry, "order.placed", v1)
	if err != nil {
		t.Fatal(err)
	}

	first, err := codec.Encode(Order{OrderID: "A-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.Encode(Order{OrderID: "A-2"})
	if err != nil {
		t.Fatal(err)
	}

	if string(first[:frameSize]) != string(second[:frameSize]) {
		t.Error("the same schema was registered twice")
	}
}

func TestMixingTheTwoModesSaysSo(t *testing.T) {
	// A configuration mistake worth naming rather than a decode that quietly
	// returns rubbish.
	fixed, err := Of(v1)
	if err != nil {
		t.Fatal(err)
	}
	unframed, err := fixed.Encode(Order{OrderID: "A-1"})
	if err != nil {
		t.Fatal(err)
	}

	framed, err := Registered(patterns.NewInMemorySchemaRegistry(), "order.placed", v1)
	if err != nil {
		t.Fatal(err)
	}

	var back Order
	err = framed.Decode(unframed, &back)
	if err == nil {
		t.Fatal("an unframed message decoded through a registered codec")
	}
	if !strings.Contains(err.Error(), "no schema identifier") {
		t.Errorf("the error does not explain: %v", err)
	}
}

func TestItNeverAnswersForAnUntypedMessage(t *testing.T) {
	codec, err := Of(v1)
	if err != nil {
		t.Fatal(err)
	}

	if codec.CanDecode("") {
		t.Error("it claimed a message with no content type")
	}
	if codec.CanDecode("application/json") {
		t.Error("it claimed a JSON message")
	}
	if !codec.CanDecode(FixedContentType) || !codec.CanDecode(RegisteredContentType) {
		t.Error("it refuses its own content types")
	}
}

func TestABadSchemaIsRefusedAtConstruction(t *testing.T) {
	if _, err := Of(`{not a schema`); err == nil {
		t.Fatal("an unparseable schema was accepted")
	}
}
