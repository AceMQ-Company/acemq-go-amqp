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
	"errors"
	"strings"
	"sync"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// ---- routing slips ---------------------------------------------------

func TestAMessageFollowsItsSlip(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	for _, q := range []string{"validate", "charge", "ship"} {
		if err := mq.DeclareQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var visited []string

	stage := func(name string) acemq.Handler[OrderPlaced] {
		return patterns.FollowSlip(mq,
			func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
				mu.Lock()
				visited = append(visited, name)
				mu.Unlock()
				return m.Payload, nil
			})
	}

	for _, name := range []string{"validate", "charge", "ship"} {
		sub, err := acemq.Consume(ctx, mq, name, stage(name))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
	}

	slip := patterns.NewRoutingSlip().
		Then("", "validate", "validate").
		Then("", "charge", "charge").
		Then("", "ship", "ship")

	if err := patterns.Start(ctx, mq, slip, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to visit every stop", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(visited) == 3
	})

	mu.Lock()
	defer mu.Unlock()
	// The order is the point: an itinerary that arrives out of order is not an
	// itinerary.
	if strings.Join(visited, ",") != "validate,charge,ship" {
		t.Errorf("visited %v", visited)
	}
}

func TestASlipRecordsWhereItHasBeen(t *testing.T) {
	slip := patterns.NewRoutingSlip().
		Then("x", "a", "first").
		Then("x", "b", "second")

	advanced := slip.Advance()

	if len(advanced.Done) != 1 || advanced.Done[0].Name != "first" {
		t.Errorf("Done = %v", advanced.Done)
	}
	if advanced.Done[0].CompletedAt == "" {
		t.Error("a completed step does not say when")
	}
	next, ok := advanced.Next()
	if !ok || next.Name != "second" {
		t.Errorf("Next = %v", next)
	}
	// The original is untouched, so a step that fails can be retried against
	// the slip it received.
	if len(slip.Done) != 0 || len(slip.Steps) != 2 {
		t.Error("Advance modified the slip it was given")
	}
}

func TestAFinishedSlipStopsRatherThanLooping(t *testing.T) {
	slip := patterns.NewRoutingSlip().Then("x", "a")

	finished := slip.Advance()

	if !finished.Finished() {
		t.Error("a slip with no steps left says it is not finished")
	}
	if _, ok := finished.Next(); ok {
		t.Error("a finished slip offered another step")
	}
}

func TestAMessageWithNoSlipIsRejected(t *testing.T) {
	handler := patterns.FollowSlip[OrderPlaced](nil,
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
			t.Error("the step ran for a message with no slip")
			return m.Payload, nil
		})

	ack := handler(context.Background(), acemq.Message[OrderPlaced]{
		Envelope: acemq.Envelope{ID: "m-1", Headers: map[string]any{}}})

	if ack.String() != "reject" {
		t.Errorf("got %s, want reject", ack)
	}
}

func TestASlipThatWillNotParseIsFatal(t *testing.T) {
	_, present, err := patterns.SlipFrom(acemq.Envelope{
		ID:      "m-1",
		Headers: map[string]any{patterns.HeaderRoutingSlip: "{not json"},
	})

	if !present {
		t.Fatal("the slip header was not seen at all")
	}
	if !acemq.IsFatal(err) {
		t.Errorf("a slip that will not parse is not fatal, so it would be retried for ever: %v", err)
	}
}

// ---- consumer groups -------------------------------------------------

func TestAGroupSpreadsWorkAndStopsTogether(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	handled := 0
	group, err := patterns.NewConsumerGroup(ctx, mq, "orders", 4,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			mu.Lock()
			handled++
			mu.Unlock()
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}

	if group.Size() != 4 {
		t.Errorf("Size = %d, want 4", group.Size())
	}

	pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
	for i := range 20 {
		if err := pub.Send(ctx, OrderPlaced{OrderID: string(rune('a' + i%26))}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "every message", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled == 20
	})

	if err := group.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Closing twice must not be an error, since a deferred Close beside an
	// explicit one is the normal shape.
	if err := group.Close(); err != nil {
		t.Fatalf("the second Close returned %v", err)
	}
}

func TestAGroupThatCannotStartLeavesNothingRunning(t *testing.T) {
	// A half-started group would hold messages nothing is going to handle.
	ctx := context.Background()
	mq := brokerFor(t)

	_, err := patterns.NewConsumerGroup(ctx, mq, "never-declared", 3,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack { return acemq.Accept() })

	if err == nil {
		t.Fatal("a group started against a queue that does not exist")
	}
}

func TestAGroupNeedsAtLeastOneConsumer(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if _, err := patterns.NewConsumerGroup(ctx, mq, "orders", 0,
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			return acemq.Accept()
		}); err == nil {
		t.Fatal("a group of zero consumers was accepted")
	}
}

// ---- schema registry -------------------------------------------------

func TestRegisteringTheSameSchemaTwiceIsOneVersion(t *testing.T) {
	// A service that registers on start-up must not add a version per restart.
	ctx := context.Background()
	registry := patterns.NewInMemorySchemaRegistry()
	const definition = `{"type":"record","name":"OrderPlaced"}`

	first, err := registry.Register(ctx, "order.placed", "avro", definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Register(ctx, "order.placed", "avro", definition)
	if err != nil {
		t.Fatal(err)
	}

	if first.ID != second.ID {
		t.Errorf("the same schema got two identifiers: %d and %d", first.ID, second.ID)
	}
	versions, err := registry.Versions(ctx, "order.placed")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 {
		t.Errorf("%d versions for one schema", len(versions))
	}
}

func TestADifferentSchemaIsANewVersion(t *testing.T) {
	ctx := context.Background()
	registry := patterns.NewInMemorySchemaRegistry()

	if _, err := registry.Register(ctx, "order.placed", "avro", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	second, err := registry.Register(ctx, "order.placed", "avro", `{"v":2}`)
	if err != nil {
		t.Fatal(err)
	}

	if second.Version != 2 {
		t.Errorf("Version = %d, want 2", second.Version)
	}
	latest, err := registry.Latest(ctx, "order.placed")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != 2 {
		t.Errorf("Latest is version %d", latest.Version)
	}
}

func TestALookupThatFindsNothingSaysSo(t *testing.T) {
	// Not a zero value: a consumer that cannot find its schema has a real
	// problem and must not carry on with an empty definition.
	ctx := context.Background()
	registry := patterns.NewInMemorySchemaRegistry()

	if _, err := registry.ByID(ctx, 99); !errors.Is(err, patterns.ErrSchemaNotFound) {
		t.Errorf("got %v, want ErrSchemaNotFound", err)
	}
	if _, err := registry.Latest(ctx, "nothing"); !errors.Is(err, patterns.ErrSchemaNotFound) {
		t.Errorf("got %v, want ErrSchemaNotFound", err)
	}
}

func TestDialectsRenderTheirOwnPlaceholders(t *testing.T) {
	// Getting this wrong fails at runtime on one database and not another,
	// which is the worst way to find out.
	if patterns.PostgresDialect.Placeholder(3) != "$3" {
		t.Errorf("postgres placeholder = %q", patterns.PostgresDialect.Placeholder(3))
	}
	if patterns.MySQLDialect.Placeholder(3) != "?" {
		t.Errorf("mysql placeholder = %q", patterns.MySQLDialect.Placeholder(3))
	}
	if patterns.SQLiteDialect.Placeholder(1) != "?" {
		t.Errorf("sqlite placeholder = %q", patterns.SQLiteDialect.Placeholder(1))
	}
}
