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

package patterns_test

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

type Invoice struct {
	Number string `json:"number"`
	Cents  int64  `json:"cents"`
}

// The rung names are a cross-language contract: a Go service and a Java service
// scheduling on one broker declare the same five queues by these names, and a
// name this library got wrong is a queue the other one never reads.
func TestTheRungsAreNamedAsJavaNamesThem(t *testing.T) {
	rungs := patterns.ScheduleRungs()
	want := []time.Duration{
		time.Hour, 10 * time.Minute, time.Minute, 10 * time.Second, time.Second,
	}
	if !slices.Equal(rungs, want) {
		t.Fatalf("rungs %v, want %v — longest first", rungs, want)
	}

	names := make([]string, 0, len(rungs))
	for _, rung := range rungs {
		names = append(names, patterns.ScheduleRungName(rung))
	}
	wantNames := []string{
		"acemq.schedule.1h",
		"acemq.schedule.10m",
		"acemq.schedule.1m",
		"acemq.schedule.10s",
		"acemq.schedule.1s",
	}
	if !slices.Equal(names, wantNames) {
		t.Errorf("rung queues %v, want %v", names, wantNames)
	}

	// The rendering rather than the five rungs: hours, then minutes, then
	// seconds, whichever divides.
	for _, c := range []struct {
		rung time.Duration
		want string
	}{
		{24 * time.Hour, "acemq.schedule.24h"},
		{90 * time.Minute, "acemq.schedule.90m"},
		{90 * time.Second, "acemq.schedule.90s"},
	} {
		if got := patterns.ScheduleRungName(c.rung); got != c.want {
			t.Errorf("%s renders as %q, want %q", c.rung, got, c.want)
		}
	}
}

// Exactly these three arguments, and nothing else. A rung declared with a
// fourth, or with x-queue-type, is a queue the broker refuses to let the next
// library redeclare.
func TestARungIsDeclaredWithJavasArgumentTable(t *testing.T) {
	args := patterns.ScheduleRungArgs(10 * time.Minute)

	want := map[string]any{
		"x-message-ttl":             int64(600_000),
		"x-dead-letter-exchange":    "acemq.schedule",
		"x-dead-letter-routing-key": "acemq.schedule.due",
	}
	if len(args) != len(want) {
		t.Fatalf("argument table %v, want exactly %v", args, want)
	}
	for name, value := range want {
		if args[name] != value {
			t.Errorf("%s is %#v, want %#v", name, args[name], value)
		}
	}
}

func TestTheSchedulerDeclaresItsWholeTopology(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	for _, queue := range []string{
		"acemq.schedule.1h", "acemq.schedule.10m", "acemq.schedule.1m",
		"acemq.schedule.10s", "acemq.schedule.1s", "acemq.schedule.due",
	} {
		exists, err := mq.QueueExists(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("%s was not declared", queue)
		}
	}

	// Redeclaring what the scheduler declared has to be accepted, because that
	// is what a second service starting against the same broker does.
	if err := patterns.ScheduleTopology().Apply(ctx, mq); err != nil {
		t.Errorf("the scheduler's own topology was refused on redeclaration: %v", err)
	}
}

// A message that is already due goes straight out, under the content type it
// was encoded as and with none of the scheduler's bookkeeping on it.
func TestAMessageDueNowIsDeliveredNow(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "invoices"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var got []acemq.Message[Invoice]
	sub, err := acemq.Consume(ctx, mq, "invoices",
		func(_ context.Context, m acemq.Message[Invoice]) acemq.Ack {
			mu.Lock()
			got = append(got, m)
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	invoice := Invoice{Number: "INV-1", Cents: 4250}
	if err := scheduler.In(ctx, 0, "", "invoices", invoice); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the invoice", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1
	})

	mu.Lock()
	m := got[0]
	mu.Unlock()

	if m.Payload != invoice {
		t.Errorf("payload %+v, want %+v", m.Payload, invoice)
	}
	if m.ContentType != acemq.JSONContentType {
		t.Errorf("content type %q, want %q", m.ContentType, acemq.JSONContentType)
	}
	if m.Envelope.Type != patterns.ScheduledMessageType {
		t.Errorf("message type %q, want %q", m.Envelope.Type, patterns.ScheduledMessageType)
	}
	for _, header := range []string{
		patterns.HeaderScheduleExchange, patterns.HeaderScheduleRoutingKey,
		patterns.HeaderScheduleDueAt, patterns.HeaderScheduleContentType,
	} {
		if _, carried := m.Envelope.Headers[header]; carried {
			t.Errorf("%s was passed on to the consumer; it is the scheduler's bookkeeping", header)
		}
	}
	if scheduler.Delivered() != 1 {
		t.Errorf("delivered %d, want 1", scheduler.Delivered())
	}
	if scheduler.Hops() != 0 {
		t.Errorf("hops %d, want 0 — a message due now takes none", scheduler.Hops())
	}
}

// The rung a delay lands on, and the headers it carries while it waits there.
func TestADelayGoesOnTheLargestRungThatDoesNotOvershoot(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		delay time.Duration
		rung  string
	}{
		{4 * time.Hour, "acemq.schedule.1h"},
		{61 * time.Minute, "acemq.schedule.1h"},
		// Exactly an hour lands on the rung below, because the delay is turned
		// into a deadline and a moment has passed by the time the rung is
		// chosen. Java does the same thing for the same reason, and the message
		// is no later for it — the ten-minute rung hops it up to the hour.
		{time.Hour, "acemq.schedule.10m"},
		{45 * time.Minute, "acemq.schedule.10m"},
		{90 * time.Second, "acemq.schedule.1m"},
		{45 * time.Second, "acemq.schedule.10s"},
		{5 * time.Second, "acemq.schedule.1s"},
	} {
		t.Run(c.delay.String(), func(t *testing.T) {
			mq := brokerFor(t)
			scheduler, err := patterns.NewScheduler(ctx, mq)
			if err != nil {
				t.Fatal(err)
			}
			defer scheduler.Close()

			before := time.Now()
			invoice := Invoice{Number: "INV-2", Cents: 900}
			if err := scheduler.In(ctx, c.delay, "billing", "invoice.due", invoice); err != nil {
				t.Fatal(err)
			}
			if scheduler.Hops() != 1 {
				t.Fatalf("hops %d, want 1", scheduler.Hops())
			}

			waiting, ok, err := mq.Pull(ctx, c.rung)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatalf("nothing is waiting on %s", c.rung)
			}
			defer waiting.Ack()

			headers := waiting.Envelope.Headers
			if got := headers[patterns.HeaderScheduleExchange]; got != "billing" {
				t.Errorf("%s is %v, want billing", patterns.HeaderScheduleExchange, got)
			}
			if got := headers[patterns.HeaderScheduleRoutingKey]; got != "invoice.due" {
				t.Errorf("%s is %v, want invoice.due", patterns.HeaderScheduleRoutingKey, got)
			}
			if got := headers[patterns.HeaderScheduleContentType]; got != acemq.JSONContentType {
				t.Errorf("%s is %v, want %s",
					patterns.HeaderScheduleContentType, got, acemq.JSONContentType)
			}

			// Epoch milliseconds, as an integer: the same thing Java's
			// Instant.toEpochMilli writes.
			due, isMillis := headers[patterns.HeaderScheduleDueAt].(int64)
			if !isMillis {
				t.Fatalf("%s is %#v, want an int64 of epoch milliseconds",
					patterns.HeaderScheduleDueAt, headers[patterns.HeaderScheduleDueAt])
			}
			wanted := before.Add(c.delay)
			if drift := time.UnixMilli(due).Sub(wanted); drift < -time.Second || drift > time.Second {
				t.Errorf("due at %s, want about %s", time.UnixMilli(due), wanted)
			}

			// The bytes are the payload, encoded once, and the rung carries them
			// as bytes because the scheduler must not decode them.
			var carried Invoice
			if err := json.Unmarshal(waiting.Body, &carried); err != nil {
				t.Fatalf("the rung is not carrying the encoded payload: %v", err)
			}
			if carried != invoice {
				t.Errorf("the rung carries %+v, want %+v", carried, invoice)
			}
			if waiting.ContentType != acemq.BytesContentType {
				t.Errorf("between rungs the body travels as %q, want %q",
					waiting.ContentType, acemq.BytesContentType)
			}
			if waiting.Envelope.Type != patterns.ScheduledMessageType {
				t.Errorf("message type %q, want %q",
					waiting.Envelope.Type, patterns.ScheduledMessageType)
			}
		})
	}
}

// What the control queue does with a message that came out of a rung: put it on
// the next rung down, or deliver it. This is the hop, exercised without waiting
// for a real time to live to expire.
func TestTheControlQueueHopsAMessageDownTheLadder(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	// As if a rung had just expired it: still 45 minutes to run, so the next
	// rung down is the ten-minute one.
	due := time.Now().Add(45 * time.Minute)
	env, err := acemq.NewEnvelope(patterns.ScheduledMessageType,
		acemq.Header(patterns.HeaderScheduleExchange, "billing"),
		acemq.Header(patterns.HeaderScheduleRoutingKey, "invoice.due"),
		acemq.Header(patterns.HeaderScheduleDueAt, due.UnixMilli()),
		acemq.Header(patterns.HeaderScheduleContentType, acemq.JSONContentType))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"number":"INV-3","cents":100}`)
	if _, err := mq.PublishRaw(ctx, patterns.ScheduleExchange, patterns.ScheduleControlQueue,
		acemq.Outbound{
			Body:        body,
			ContentType: acemq.BytesContentType,
			MessageID:   env.ID,
			Headers:     env.ToWire(),
			Persistent:  true,
		}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the hop onto the ten-minute rung", func() bool {
		n, err := mq.MessageCount(ctx, "acemq.schedule.10m")
		return err == nil && n == 1
	})

	waiting, ok, err := mq.Pull(ctx, "acemq.schedule.10m")
	if err != nil || !ok {
		t.Fatalf("nothing on the ten-minute rung: %v", err)
	}
	defer waiting.Ack()

	if string(waiting.Body) != string(body) {
		t.Errorf("the hop changed the body: %q", waiting.Body)
	}
	if got := waiting.Envelope.Headers[patterns.HeaderScheduleDueAt]; got != due.UnixMilli() {
		t.Errorf("%s is %v, want %d — the deadline does not move when a message hops",
			patterns.HeaderScheduleDueAt, got, due.UnixMilli())
	}
	if scheduler.Hops() != 1 {
		t.Errorf("hops %d, want 1", scheduler.Hops())
	}
	if scheduler.Delivered() != 0 {
		t.Errorf("delivered %d, want 0 — it is not due for another 45 minutes", scheduler.Delivered())
	}
}

// A message that has arrived at the control queue already due goes to its
// destination rather than round the ladder again.
func TestTheControlQueueDeliversWhatIsDue(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "invoices"); err != nil {
		t.Fatal(err)
	}

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	env, err := acemq.NewEnvelope(patterns.ScheduledMessageType,
		acemq.Header(patterns.HeaderScheduleExchange, ""),
		acemq.Header(patterns.HeaderScheduleRoutingKey, "invoices"),
		acemq.Header(patterns.HeaderScheduleDueAt, time.Now().Add(-time.Minute).UnixMilli()),
		acemq.Header(patterns.HeaderScheduleContentType, "application/xml"))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`<invoice number="INV-4"/>`)
	if _, err := mq.PublishRaw(ctx, patterns.ScheduleExchange, patterns.ScheduleControlQueue,
		acemq.Outbound{
			Body:        body,
			ContentType: acemq.BytesContentType,
			MessageID:   env.ID,
			Headers:     env.ToWire(),
			Persistent:  true,
		}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the delivery", func() bool {
		n, err := mq.MessageCount(ctx, "invoices")
		return err == nil && n == 1
	})

	delivered, ok, err := mq.Pull(ctx, "invoices")
	if err != nil || !ok {
		t.Fatalf("nothing was delivered: %v", err)
	}
	defer delivered.Ack()

	if string(delivered.Body) != string(body) {
		t.Errorf("delivered %q, want %q", delivered.Body, body)
	}
	// The content type the payload was encoded as, carried across, because the
	// scheduler republishes bytes and the consumer picks its codec from this.
	if delivered.ContentType != "application/xml" {
		t.Errorf("content type %q, want application/xml", delivered.ContentType)
	}
	if scheduler.Delivered() != 1 {
		t.Errorf("delivered %d, want 1", scheduler.Delivered())
	}
}
