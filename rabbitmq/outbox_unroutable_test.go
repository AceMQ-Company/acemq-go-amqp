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

package rabbitmq_test

// An outbox record nothing is bound to receive.
//
// The relay published without mandatory, so the broker dropped such a record
// and said nothing, and the relay marked it published: the one message the
// outbox exists to keep was gone, and the outbox metrics counted it as sent.
// Java's relay publishes mandatory and keeps the record. A binding that does
// not exist yet is an ordinary moment in a deployment -- the service that
// wants the event has not started -- and it must not cost the event.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

func TestAnOutboxRecordNothingIsBoundToIsKeptUntilSomethingIs(t *testing.T) {
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}
	ctx := context.Background()
	mq, err := acemq.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.Close() }()

	name := fmt.Sprintf("acemq-outbox-unroutable-%d", time.Now().UnixNano())
	if err := mq.DeclareExchange(ctx, name, "topic", acemq.TransientExchange()); err != nil {
		t.Fatal(err)
	}

	store := patterns.NewInMemoryOutboxStore()
	record, err := patterns.Record(mq, name, "order.placed", map[string]string{"orderId": "A-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, record); err != nil {
		t.Fatal(err)
	}
	relay := patterns.NewOutboxRelay(mq, store)

	published, err := relay.Sweep(ctx)
	if err == nil || published != 0 {
		t.Errorf("a record nothing is bound to was swept with %d published and error %v;"+
			" expected an error and nothing published", published, err)
	}
	if store.Len() != 1 {
		t.Fatalf("the outbox holds %d records after the broker had nowhere to put one; expected it kept", store.Len())
	}

	// The binding arrives, and the next sweep delivers the record it kept.
	if err := mq.DeclareQueue(ctx, name, acemq.OfType(acemq.QueueClassic)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.DeleteQueue(context.Background(), name) }()
	if err := mq.Bind(ctx, name, name, "order.#"); err != nil {
		t.Fatal(err)
	}
	if published, err := relay.Sweep(ctx); err != nil || published != 1 {
		t.Fatalf("once bound, the sweep published %d with error %v; expected 1", published, err)
	}
	if store.Len() != 0 {
		t.Errorf("the outbox still holds %d records after they were delivered", store.Len())
	}
	if count, err := mq.MessageCount(ctx, name); err != nil || count != 1 {
		t.Errorf("the queue holds %d messages (error %v); expected the one record", count, err)
	}
}
