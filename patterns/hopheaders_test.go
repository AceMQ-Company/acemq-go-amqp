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
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

// wireOf waits for one message on queue and returns its headers as written.
func wireOf(t *testing.T, mq *acemq.Conn, queue string) map[string]any {
	t.Helper()
	got := make(chan map[string]any, 1)
	sub, err := acemq.Consume(context.Background(), mq, queue,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m.Envelope.ToWire()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	select {
	case wire := <-got:
		return wire
	case <-time.After(5 * time.Second):
		t.Fatalf("nothing arrived on %s", queue)
		return nil
	}
}

func assertCarried(t *testing.T, wire map[string]any, incomingID string) {
	t.Helper()
	for name, want := range map[string]any{
		"x-tenant": "acme", "traceparent": traceparent, "x-acemq-future": "kept",
		acemq.HeaderCorrelation: "corr-h",
	} {
		if wire[name] != want {
			t.Errorf("%s = %v on the next hop, want %v", name, wire[name], want)
		}
	}
	// The hop is a new message, caused by the incoming one.
	if wire[acemq.HeaderCausation] != incomingID {
		t.Errorf("causation = %v, want %s", wire[acemq.HeaderCausation], incomingID)
	}
	if wire[acemq.HeaderID] == incomingID {
		t.Error("the hop reused the incoming message's id")
	}
}

func TestASlipHopCarriesHeadersOn(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	for _, q := range []string{"a", "b"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := acemq.Consume(ctx, mq, "a", patterns.FollowSlip(mq,
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
			return m.Payload, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	slip, err := patterns.NewRoutingSlip().Then("", "a", "a").Then("", "b", "b").Header()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mq.PublishRaw(ctx, "", "a", acemq.Outbound{
		Body: []byte(`{"orderId":"o-1"}`), ContentType: "application/json", MessageID: "h-1",
		Headers: map[string]any{
			acemq.HeaderID: "h-1", acemq.HeaderCorrelation: "corr-h", acemq.HeaderAttempt: 1,
			patterns.HeaderRoutingSlip: slip, "x-tenant": "acme", "traceparent": traceparent,
			"x-acemq-future": "kept",
		},
	}); err != nil {
		t.Fatal(err)
	}

	wire := wireOf(t, mq, "b")
	assertCarried(t, wire, "h-1")
	// The slip on the hop is the advanced one, not the one a arrived with.
	if wire[patterns.HeaderRoutingSlip] == slip {
		t.Error("the hop carried the incoming slip instead of the advanced one")
	}
}

func TestAThenHopCarriesHeadersOn(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	for _, q := range []string{"a", "b"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := acemq.Consume(ctx, mq, "a", patterns.Then(acemq.NewPublisher[OrderPlaced](mq, "", "b"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, bool, error) {
			return m.Payload, true, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if _, err := mq.PublishRaw(ctx, "", "a", acemq.Outbound{
		Body: []byte(`{"orderId":"o-1"}`), ContentType: "application/json", MessageID: "h-2",
		Headers: map[string]any{
			acemq.HeaderID: "h-2", acemq.HeaderCorrelation: "corr-h", acemq.HeaderAttempt: 1,
			"x-tenant": "acme", "traceparent": traceparent, "x-acemq-future": "kept",
		},
	}); err != nil {
		t.Fatal(err)
	}
	assertCarried(t, wireOf(t, mq, "b"), "h-2")
}

func TestAPipelineHopCarriesApplicationHeadersOn(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	seen := make(chan map[string]any, 1)
	pipeline, err := patterns.NewPipeline[OrderPlaced](ctx, mq, "carry",
		patterns.PipelineStep("first",
			func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, bool, error) {
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("second",
			func(_ context.Context, m acemq.Message[OrderPlaced]) (struct{}, bool, error) {
				seen <- m.Envelope.Headers
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	if _, err := pipeline.Send(ctx, OrderPlaced{OrderID: "o-1"},
		acemq.Header("x-tenant", "acme"), acemq.Header("traceparent", traceparent)); err != nil {
		t.Fatal(err)
	}
	select {
	case headers := <-seen:
		if headers["x-tenant"] != "acme" || headers["traceparent"] != traceparent {
			t.Errorf("the second step saw headers %v", headers)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second step never ran")
	}
}

func TestARecordCarriesTheConnectionsOrigin(t *testing.T) {
	mq := brokerFor(t, acemq.WithOrigin("billing@host-1"))
	record, err := patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := record.Headers[acemq.HeaderOrigin]; got != "billing@host-1" {
		t.Errorf("origin = %v, want the connection's billing@host-1", got)
	}

	record, err = patterns.Record(mq, "", "orders", OrderPlaced{OrderID: "o-1"}, acemq.Origin("mine@here"))
	if err != nil {
		t.Fatal(err)
	}
	if got := record.Headers[acemq.HeaderOrigin]; got != "mine@here" {
		t.Errorf("origin = %v, an explicit origin should win", got)
	}
}
