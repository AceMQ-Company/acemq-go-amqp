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
	"time"

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

// ---- the Java-declared form ------------------------------------------

// javaRouteHeaders is what org.acemq.amqp.api.RoutingSlip.toHeaders() puts on a
// message: the step names comma-joined, the position as an integer, the run
// identifier as a string.
//
// Written out here rather than produced by this library's own Route, because
// what is being proved is that Go reads what *Java* writes. A test that
// round-trips Go's own output through Go's own reader proves the two halves
// agree with each other and nothing about whether either matches the wire.
func javaRouteHeaders(route string, position int, runID string) map[string]any {
	return map[string]any{
		"x-acemq-route":          route,
		"x-acemq-route-position": position,
		"x-acemq-route-id":       runID,
		"x-acemq-id":             "m-java-1",
		"x-acemq-type":           "order.placed",
	}
}

// TestAJavaShapedRouteSurvivesTheWire is the first half of reading a Java slip,
// and it is the half that used to fail invisibly.
//
// x-acemq- is a reserved prefix: the engine materialises a header it knows onto
// the Envelope and drops every other one from the application's headers. Go knew
// nothing of x-acemq-route, so a Java-declared route arrived on the wire and was
// thrown away before any handler could see it. Reading it required the envelope
// to carry it, not just the slip code to look for it.
func TestAJavaShapedRouteSurvivesTheWire(t *testing.T) {
	headers := javaRouteHeaders("validate,charge,ship", 1, "run-7")

	env := acemq.EnvelopeFromWire(headers, "charge", "m-java-1")

	if env.Route != "validate,charge,ship" {
		t.Errorf("Route = %q, want the comma-joined step names", env.Route)
	}
	if env.RoutePosition != 1 {
		t.Errorf("RoutePosition = %d, want 1", env.RoutePosition)
	}
	if env.RouteID != "run-7" {
		t.Errorf("RouteID = %q, want run-7", env.RouteID)
	}
	if got := env.RouteSteps(); strings.Join(got, "|") != "validate|charge|ship" {
		t.Errorf("RouteSteps = %v", got)
	}
	// And it stays out of the application's headers, like every reserved header.
	if _, leaked := env.Headers["x-acemq-route"]; leaked {
		t.Error("a reserved header reached the application's headers")
	}
}

// TestAJavaShapedRouteIsReadAsASlip is the second half: the three headers become
// the same RoutingSlip a JSON slip would, positioned where Java left it.
func TestAJavaShapedRouteIsReadAsASlip(t *testing.T) {
	route := patterns.NewRoute("orders", "validate", "charge", "ship")
	env := acemq.EnvelopeFromWire(
		javaRouteHeaders("validate,charge,ship", 1, "run-7"), "charge", "m-java-1")

	slip, present, err := patterns.SlipFrom(env, route)
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Fatal("a message carrying a declared route was read as having no slip")
	}

	// Position 1 means this message is for charge: validate is behind it and
	// charge and ship are still to come.
	if len(slip.Done) != 1 || slip.Done[0].Name != "validate" {
		t.Errorf("Done = %v, want validate behind it", slip.Done)
	}
	next, ok := slip.Next()
	if !ok || next.Name != "charge" {
		t.Errorf("Next = %v, want charge", next)
	}
	// The names resolve against the declaration: the pipeline's name is the
	// exchange and the step's name is the routing key, which is how Java's
	// Pipeline publishes between its steps.
	if next.Exchange != "orders" || next.RoutingKey != "charge" {
		t.Errorf("next step is %s/%s, want orders/charge", next.Exchange, next.RoutingKey)
	}
	if slip.Form() != patterns.FormSteps {
		t.Errorf("Form = %s, want steps: a slip has to remember what it arrived as",
			slip.Form())
	}
	if slip.RunID() != "run-7" {
		t.Errorf("RunID = %q, want run-7 carried through", slip.RunID())
	}
}

// TestAGoStepFollowsAJavaDeclaredPipeline is the whole point of reading the
// other form: a Go consumer sits in the middle of a pipeline Java declared, does
// its step, and hands the message to the Java step after it in the shape that
// step is waiting for.
//
// The message is put in at orders.charge with position 1, exactly as a Java
// validate step would have published it. What comes out has to arrive at
// orders.ship carrying x-acemq-route at position 2 — not a JSON slip, which the
// Java step would not recognise at all.
func TestAGoStepFollowsAJavaDeclaredPipeline(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	route := patterns.NewRoute("orders", "validate", "charge", "ship")

	if err := mq.DeclareExchange(ctx, route.Name(), "direct"); err != nil {
		t.Fatal(err)
	}
	for _, step := range route.Steps() {
		queue := route.QueueFor(step)
		if err := mq.DeclareQueue(ctx, queue); err != nil {
			t.Fatal(err)
		}
		if err := mq.Bind(ctx, queue, route.Name(), step); err != nil {
			t.Fatal(err)
		}
	}

	// The Go step: the charge step of a Java pipeline.
	charged := make(chan struct{}, 1)
	sub, err := acemq.Consume(ctx, mq, route.QueueFor("charge"),
		patterns.FollowSlip(mq,
			func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
				charged <- struct{}{}
				return m.Payload, nil
			}, patterns.AlongRoute(route)))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Standing in for the Java step after it, so what it receives is what a Java
	// step would have received.
	arrived := make(chan acemq.Envelope, 1)
	shipped, err := acemq.Consume(ctx, mq, route.QueueFor("ship"),
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- m.Envelope
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer shipped.Close()

	// Published the way Java's validate step would have: to the pipeline's
	// exchange, keyed on the next step's name, carrying the route at position 1.
	err = acemq.NewPublisher[OrderPlaced](mq, route.Name(), "charge").Send(ctx,
		OrderPlaced{OrderID: "o-1"},
		acemq.Route([]string{"validate", "charge", "ship"}, 1, "run-7"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-charged:
	case <-time.After(3 * time.Second):
		t.Fatal("the Go step never received the message")
	}

	var onwards acemq.Envelope
	select {
	case onwards = <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("the Go step did not send the message on to the next step")
	}

	if onwards.Route != "validate,charge,ship" {
		t.Errorf("Route = %q, want the route carried through unchanged", onwards.Route)
	}
	// Two, not one: charge is done. A position that did not advance would send
	// the message round the same step for ever.
	if onwards.RoutePosition != 2 {
		t.Errorf("RoutePosition = %d, want 2 now that charge is done", onwards.RoutePosition)
	}
	// The run identifier is what ties every hop of one run together in a trace,
	// so a step that minted a fresh one would split the run in two.
	if onwards.RouteID != "run-7" {
		t.Errorf("RouteID = %q, want run-7 carried through", onwards.RouteID)
	}
	// And in the form it arrived in. A JSON slip here would be correct Go and
	// unreadable to the Java step that receives it.
	if _, wrongForm := onwards.Headers[patterns.HeaderRoutingSlip]; wrongForm {
		t.Errorf("the message went on as a JSON slip; a Java step would not follow it")
	}
}

// TestADeclaredRouteWithNoDeclarationIsRefused is the failure that has to be
// loud. A slip in the declared form carries names and no destinations; publishing
// with an empty exchange would send the message to a queue named for the step
// rather than to the pipeline's, quietly and to the wrong place.
func TestADeclaredRouteWithNoDeclarationIsRefused(t *testing.T) {
	handler := patterns.FollowSlip[OrderPlaced](nil,
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
			return m.Payload, nil
		}) // no AlongRoute

	env := acemq.EnvelopeFromWire(
		javaRouteHeaders("validate,charge,ship", 1, "run-7"), "charge", "m-java-1")
	ack := handler(context.Background(), acemq.Message[OrderPlaced]{Envelope: env})

	if ack.String() != "reject" {
		t.Errorf("got %s, want reject", ack)
	}
	// Fatal, because a missing declaration will still be missing on the fourth
	// attempt and a message carrying one has to stop rather than circle.
	if !acemq.IsFatal(ack.Err()) {
		t.Errorf("not fatal, so it would be retried for ever: %v", ack.Err())
	}
	if !strings.Contains(ack.Err().Error(), "AlongRoute") {
		t.Errorf("the error does not say how to fix it: %v", ack.Err())
	}
}

// TestTheJSONSlipIsStillWhatGoWrites pins the default. Three of the five
// libraries write the JSON slip and it is the self-describing one, so gaining
// the ability to write the other form must not have changed which one is used
// when nobody asks.
func TestTheJSONSlipIsStillWhatGoWrites(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "validate"); err != nil {
		t.Fatal(err)
	}

	arrived := make(chan acemq.Envelope, 1)
	sub, err := acemq.Consume(ctx, mq, "validate",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			arrived <- m.Envelope
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	slip := patterns.NewRoutingSlip().Then("", "validate", "validate")
	if slip.Form() != patterns.FormJSON {
		t.Errorf("a fresh slip is in %s form, want json", slip.Form())
	}
	if err := patterns.Start(ctx, mq, slip, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	var env acemq.Envelope
	select {
	case env = <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("the message never arrived")
	}

	if _, ok := env.Headers[patterns.HeaderRoutingSlip]; !ok {
		t.Errorf("no JSON slip on the message; headers were %v", env.Headers)
	}
	if env.Route != "" {
		t.Errorf("Route = %q, want empty: a JSON slip writes no declared route", env.Route)
	}
}

// TestASlipCanBeAskedForTheDeclaredForm covers the other direction — a Go
// publisher starting a run that Java steps will follow.
func TestASlipCanBeAskedForTheDeclaredForm(t *testing.T) {
	route := patterns.NewRoute("orders", "validate", "charge", "ship")

	started := route.Start()
	if started.Form() != patterns.FormSteps {
		t.Errorf("Route.Start gave a %s slip, want steps", started.Form())
	}
	if started.RunID() == "" {
		t.Error("a started route has no run identifier, so its hops cannot be tied together")
	}
	next, ok := started.Next()
	if !ok || next.Exchange != "orders" || next.RoutingKey != "validate" {
		t.Errorf("first step is %v, want orders/validate", next)
	}

	// Two runs of the same route are two runs.
	if route.Start().RunID() == started.RunID() {
		t.Error("two runs of the same route share a run identifier")
	}

	// And converting a hand-built slip over.
	converted := patterns.NewRoutingSlip().
		Then("orders", "charge", "charge").
		Then("orders", "ship", "ship").
		AsSteps(route)
	if converted.Form() != patterns.FormSteps || converted.RunID() == "" {
		t.Errorf("AsSteps gave form %s and run %q", converted.Form(), converted.RunID())
	}
	// AsJSON goes back, so neither form is a one-way door.
	if converted.AsJSON().Form() != patterns.FormJSON {
		t.Error("AsJSON did not return a JSON slip")
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
