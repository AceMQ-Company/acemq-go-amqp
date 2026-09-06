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

package xml

import (
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type Order struct {
	OrderID    string `xml:"orderId"`
	TotalCents int64  `xml:"totalCents"`
}

func TestARoundTrip(t *testing.T) {
	codec := Codec{}
	body, err := codec.Encode(Order{OrderID: "o-1", TotalCents: 4250})
	if err != nil {
		t.Fatal(err)
	}

	var back Order
	if err := codec.Decode(body, &back); err != nil {
		t.Fatal(err)
	}
	if back.OrderID != "o-1" || back.TotalCents != 4250 {
		t.Errorf("decoded %+v", back)
	}
}

func TestItUsesTheStructTags(t *testing.T) {
	body, err := Codec{}.Encode(Order{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "<orderId>") {
		t.Errorf("wrote %s", body)
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
	for _, ct := range []string{"application/xml", "text/xml", "application/soap+xml"} {
		if !codec.CanDecode(ct) {
			t.Errorf("it refused %s", ct)
		}
	}
}

func TestAMalformedBodyIsFatal(t *testing.T) {
	var back Order
	err := Codec{}.Decode([]byte("<Order><orderId>unclosed"), &back)

	if err == nil {
		t.Fatal("malformed XML decoded without complaint")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so the message would be retried for ever")
	}
}

func TestItIsAvailableByName(t *testing.T) {
	codec, err := acemq.CodecByName("xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := codec.(Codec); !ok {
		t.Errorf("CodecByName returned %T", codec)
	}
}
