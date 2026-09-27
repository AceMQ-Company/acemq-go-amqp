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

// One record the broker will never take must not stop every record behind it.
//
// The relay publishes in the order the records were written and stops at the
// first failure, which is right: the writer chose that order, and skipping ahead
// would deliver later messages before earlier ones.
//
// Stopping for ever is a different thing. A record the broker refuses
// permanently — an exchange somebody deleted, a payload one policy will always
// reject — is retried on every sweep and holds up everything behind it
// indefinitely. Python and Ruby bound this by counting attempts and leaving a
// record alone after ten; this is that bound for Go.
package patterns_test

import (
	"context"
	"errors"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// refusingBroker takes everything except one routing key, which it refuses for
// ever — the way a broker answers a publish to an exchange that is not there.
type refusingBroker struct {
	poison    string
	delivered []string
}

func (b *refusingBroker) DeclareQueue(context.Context, string, acemq.QueueSpec) error { return nil }

func (b *refusingBroker) DeclareExchange(context.Context, string, acemq.ExchangeSpec) error {
	return nil
}

func (b *refusingBroker) Bind(context.Context, string, string, string) error { return nil }

func (b *refusingBroker) Publish(
	_ context.Context, _, routingKey string, msg acemq.Outbound,
) (acemq.PublishResult, error) {
	if routingKey == b.poison {
		return acemq.PublishResult{MessageID: msg.MessageID}, &acemq.PublishFailedError{
			MessageID:  msg.MessageID,
			RoutingKey: routingKey,
			Err:        errors.New("the broker refused it"),
		}
	}
	b.delivered = append(b.delivered, routingKey)
	return acemq.PublishResult{MessageID: msg.MessageID, Confirmed: true, Routed: true}, nil
}

func (b *refusingBroker) Consume(
	context.Context, string, acemq.ConsumeSpec, func(acemq.Delivery),
) (acemq.Subscription, error) {
	return fakeSubscription{}, nil
}

func (b *refusingBroker) Close() error { return nil }

// refusing wires a connection onto a broker that refuses one routing key.
func refusing(t *testing.T, poison string) (*acemq.Conn, *refusingBroker) {
	t.Helper()
	broker := &refusingBroker{poison: poison}
	mq, err := acemq.NewConn(broker)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq, broker
}

func outboxRecord(t *testing.T, mq *acemq.Conn, routingKey string) patterns.OutboxRecord {
	t.Helper()
	record, err := patterns.Record(mq, "", routingKey, OrderPlaced{OrderID: routingKey})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// TestAPoisonRecordIsLeftAloneAndTheRestGoOut is the whole point.
//
// Each sweep stops at the poison record, as it should: order is what the writer
// asked for. After the store's maximum it is left alone and the queue moves.
func TestAPoisonRecordIsLeftAloneAndTheRestGoOut(t *testing.T) {
	ctx := context.Background()
	mq, broker := refusing(t, "always-refused")

	store := patterns.NewInMemoryOutboxStore()
	store.SetMaxAttempts(3)

	// Written in this order, and the relay has to respect it: the poison record
	// is first, so nothing behind it can go out until the relay is allowed past
	// it.
	for _, record := range []patterns.OutboxRecord{
		outboxRecord(t, mq, "always-refused"),
		outboxRecord(t, mq, "orders"),
		outboxRecord(t, mq, "orders"),
	} {
		if err := store.Add(ctx, record); err != nil {
			t.Fatal(err)
		}
	}

	relay := patterns.NewOutboxRelay(mq, store)
	for attempt := range 3 {
		if _, err := relay.Sweep(ctx); err == nil {
			t.Fatalf("sweep %d published the record the broker refuses", attempt+1)
		}
	}

	published, err := relay.Sweep(ctx)
	if err != nil {
		t.Fatalf("the sweep after the record retired still failed: %v", err)
	}
	if published != 2 {
		t.Errorf("the sweep published %d records, want 2: one record the broker "+
			"will never take is blocking the entire outbox", published)
	}
	if len(broker.delivered) != 2 {
		t.Errorf("the broker took %d messages, want 2", len(broker.delivered))
	}
}

// TestARetiredRecordIsKeptRatherThanDeleted — left alone, not thrown away.
//
// A record nothing could publish is evidence: somebody has to be able to read
// it, fix whatever refuses it, and release it. Deleting it would be the silent
// loss this pattern exists to prevent, arrived at by a different route.
func TestARetiredRecordIsKeptRatherThanDeleted(t *testing.T) {
	ctx := context.Background()
	mq, _ := refusing(t, "always-refused")

	store := patterns.NewInMemoryOutboxStore()
	store.SetMaxAttempts(2)
	if err := store.Add(ctx, outboxRecord(t, mq, "always-refused")); err != nil {
		t.Fatal(err)
	}

	relay := patterns.NewOutboxRelay(mq, store)
	for range 2 {
		if _, err := relay.Sweep(ctx); err == nil {
			t.Fatal("the sweep did not fail on a record the broker refuses")
		}
	}

	published, err := relay.Sweep(ctx)
	if err != nil {
		t.Fatalf("the sweep after the record retired still failed: %v", err)
	}
	if published != 0 {
		t.Errorf("the sweep published %d records from an outbox holding one poison record", published)
	}
	if store.Len() != 1 {
		t.Errorf("the outbox holds %d records: the one the broker refused was thrown away", store.Len())
	}
}

// TestTheFailureIsRecordedOnTheRecord — why it was left alone, readable from the
// record itself.
//
// An operator finding a stuck record needs the broker's reason without having to
// correlate a log line from ten sweeps ago.
func TestTheFailureIsRecordedOnTheRecord(t *testing.T) {
	ctx := context.Background()
	mq, _ := refusing(t, "always-refused")

	store := patterns.NewInMemoryOutboxStore()
	store.SetMaxAttempts(1)
	if err := store.Add(ctx, outboxRecord(t, mq, "always-refused")); err != nil {
		t.Fatal(err)
	}

	if _, err := patterns.NewOutboxRelay(mq, store).Sweep(ctx); err == nil {
		t.Fatal("the sweep did not fail on a record the broker refuses")
	}

	retired := store.Retired()
	if len(retired) != 1 {
		t.Fatalf("%d records retired, want 1", len(retired))
	}
	if retired[0].Attempts < 1 {
		t.Errorf("the record says it has been tried %d times", retired[0].Attempts)
	}
	if retired[0].LastError == "" {
		t.Error("nothing on the record says why it was left alone")
	}
}

// TestARecordThatSucceedsIsNotCountedAgainstItsAttempts keeps the good path the
// good path.
func TestARecordThatSucceedsIsNotCountedAgainstItsAttempts(t *testing.T) {
	ctx := context.Background()
	mq, _ := refusing(t, "nothing-is-poison-here")

	store := patterns.NewInMemoryOutboxStore()
	store.SetMaxAttempts(1)
	if err := store.Add(ctx, outboxRecord(t, mq, "orders")); err != nil {
		t.Fatal(err)
	}

	published, err := patterns.NewOutboxRelay(mq, store).Sweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if published != 1 {
		t.Errorf("the sweep published %d records, want 1", published)
	}
	if store.Len() != 0 {
		t.Errorf("%d records are still in the outbox after being published", store.Len())
	}
}
