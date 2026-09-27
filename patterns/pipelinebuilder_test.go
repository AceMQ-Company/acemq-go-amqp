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

// A pipeline Go declares itself, rather than one it merely joins.
//
// Before this, a four-step flow in Go was four services each naming the same
// pipeline through InPipeline/AtStep and each declaring its own queue, with
// nothing holding the shape in one place. Java, .NET, Python and Ruby all have
// the declaration. These tests pin the two things that matter: that the flow
// works end to end, and that the topology is Java's to the letter — because a
// pipeline whose steps are in different languages is the reason it exists.
package patterns_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

type pipeOrder struct {
	ID      string `json:"id"`
	Digital bool   `json:"digital"`
}

type pipeReservation struct {
	OrderID string `json:"orderId"`
}

func TestAPipelineRunsEveryStepInOrder(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	var order atomic.Value
	reserved := make(chan pipeReservation, 1)

	pipeline, err := patterns.NewPipeline[pipeOrder](ctx, mq, "fulfilment",
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeOrder, bool, error) {
				order.Store(m.Payload)
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("reserve",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeReservation, bool, error) {
				return pipeReservation{OrderID: m.Payload.ID}, true, nil
			}),
		patterns.PipelineStep("dispatch",
			func(_ context.Context, m acemq.Message[pipeReservation]) (struct{}, bool, error) {
				reserved <- m.Payload
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	runID, err := pipeline.Send(ctx, pipeOrder{ID: "A-1"})
	if err != nil {
		t.Fatal(err)
	}
	if runID == "" {
		t.Error("a run with no identifier cannot be found again")
	}

	select {
	case got := <-reserved:
		if got.OrderID != "A-1" {
			t.Errorf("the last step received %+v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the message never reached the last step")
	}

	if first, ok := order.Load().(pipeOrder); !ok || first.ID != "A-1" {
		t.Errorf("the first step saw %v", order.Load())
	}
}

// TestThePipelineDeclaresJavasTopology is the cross-language contract.
//
// Every one of these names is shared with the Java, .NET, Python and Ruby
// libraries: the exchange is the pipeline, the queue is {pipeline}.{step}, and
// the binding key is the step's name. A Go step that declared any of them
// differently could not be one hop of a Java-declared pipeline, which is the
// whole reason a pipeline is worth declaring rather than hand-wiring.
func TestThePipelineDeclaresJavasTopology(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	pipeline, err := patterns.NewPipeline[pipeOrder](ctx, mq, "orders",
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeOrder, bool, error) {
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("charge",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	if got := pipeline.QueueFor("charge"); got != "orders.charge" {
		t.Errorf("the queue behind a step is %q, want orders.charge", got)
	}
	if got := pipeline.Name(); got != "orders" {
		t.Errorf("the pipeline is called %q", got)
	}
	// The Route is what another service joins the pipeline with, and it has to
	// name the same steps in the same order.
	if got := strings.Join(pipeline.Route().Steps(), ","); got != "validate,charge" {
		t.Errorf("the route is %q", got)
	}

	// Both queues exist, because a publish to a queue nobody declared is
	// unroutable and discarded without a trace.
	for _, queue := range []string{"orders.validate", "orders.charge"} {
		if _, err := mq.MessageCount(ctx, queue); err != nil {
			t.Errorf("queue %s was not declared: %v", queue, err)
		}
	}
}

// TestTheHopCarriesTheDeclaredRoute is the other half of the cross-language
// contract: not just where a step publishes, but what the message says about
// where it is.
//
// A Java step reads x-acemq-route and x-acemq-route-position to know what comes
// next. The serialisation itself is pinned in newpatterns_test.go and shared with
// FollowSlip and Start; what this asserts is that a hop published by a declared
// pipeline actually carries it, advanced, with the run identifier intact — which
// is what a Java consumer downstream would find.
func TestTheHopCarriesTheDeclaredRoute(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	arrived := make(chan acemq.Envelope, 1)

	pipeline, err := patterns.NewPipeline[pipeOrder](ctx, mq, "carried",
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeOrder, bool, error) {
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("charge",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				arrived <- m.Envelope
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	runID, err := pipeline.Send(ctx, pipeOrder{ID: "A-9"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-arrived:
		if env.Route != "validate,charge" {
			t.Errorf("x-acemq-route arrived as %q, want validate,charge", env.Route)
		}
		if env.RoutePosition != 1 {
			t.Errorf("x-acemq-route-position = %d at the second step, want 1", env.RoutePosition)
		}
		if env.RouteID != runID {
			t.Errorf("the run identifier changed between hops: %q then %q", runID, env.RouteID)
		}
		// The causation names the message that caused this one, which is what
		// makes a run readable backwards from its last hop.
		if env.CausationID == "" {
			t.Error("the hop does not say what caused it")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second step never received anything")
	}
}

func TestAStepThatStopsTheRunPublishesNothingFurther(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	var reachedLast atomic.Int64
	stopped := make(chan struct{}, 1)

	pipeline, err := patterns.NewPipeline[pipeOrder](ctx, mq, "digital",
		patterns.PipelineStep("reserve",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeReservation, bool, error) {
				if m.Payload.Digital {
					// Nothing to reserve. A decision rather than a failure, which is
					// what the false is for.
					stopped <- struct{}{}
					return pipeReservation{}, false, nil
				}
				return pipeReservation{OrderID: m.Payload.ID}, true, nil
			}),
		patterns.PipelineStep("ship",
			func(_ context.Context, m acemq.Message[pipeReservation]) (struct{}, bool, error) {
				reachedLast.Add(1)
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	if _, err := pipeline.Send(ctx, pipeOrder{ID: "D-1", Digital: true}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the first step never ran")
	}

	// Long enough that a message which was going to arrive would have.
	time.Sleep(300 * time.Millisecond)
	if n := reachedLast.Load(); n != 0 {
		t.Errorf("the step after the one that stopped ran %d times", n)
	}
}

// TestTheChainIsCheckedWhenItIsAssembled is the guarantee Go cannot get from the
// compiler.
//
// Java's builder threads the types through: a step producing a Reservation can
// only be followed by one consuming a Reservation. Go's generics cannot express
// that, because a method cannot introduce type parameters — so the check is made
// with reflection at construction, and has to be loud and specific rather than a
// message that arrives at runtime and does not decode.
func TestTheChainIsCheckedWhenItIsAssembled(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	_, err := patterns.NewPipeline[pipeOrder](ctx, mq, "mismatched",
		patterns.PipelineStep("first",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeReservation, bool, error) {
				return pipeReservation{}, true, nil
			}),
		// Consumes an order, but the step before it produces a reservation.
		patterns.PipelineStep("second",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err == nil {
		t.Fatal("a pipeline whose steps do not fit together was assembled anyway")
	}
	for _, want := range []string{"first", "second", "pipeReservation", "pipeOrder"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q, so it does not say which boundary is wrong: %v",
				want, err)
		}
	}
}

func TestAPipelineRefusesWhatCannotBeARoutingKey(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	// A step name is a routing key and a queue suffix, not a label. Java
	// restricts it to the same characters, and a name that disagrees between two
	// libraries is a message published where nothing is listening.
	_, err := patterns.NewPipeline[pipeOrder](ctx, mq, "bad-names",
		patterns.PipelineStep("hold stock",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err == nil {
		t.Error("a step named with a space was accepted, and its queue would be unreachable")
	}

	_, err = patterns.NewPipeline[pipeOrder](ctx, mq, "twice",
		patterns.PipelineStep("charge",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeOrder, bool, error) {
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("charge",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err == nil {
		t.Error("two steps by one name are one queue with two handlers, and were accepted")
	}

	_, err = patterns.NewPipeline[pipeOrder](ctx, mq, "empty")
	if err == nil {
		t.Error("a pipeline with no steps was accepted")
	}
}

// TestTheEntryTypeMustBeTheFirstStepsInput catches the mistake the type
// parameter exists to catch.
func TestTheEntryTypeMustBeTheFirstStepsInput(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	_, err := patterns.NewPipeline[pipeReservation](ctx, mq, "wrong-entry",
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err == nil {
		t.Fatal("Send would have published a type the first step cannot decode")
	}
	if !strings.Contains(err.Error(), "validate") {
		t.Errorf("the error does not name the step: %v", err)
	}
}

func TestDescribeNamesEveryStepAndItsDescription(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	pipeline, err := patterns.NewPipeline[pipeOrder](ctx, mq, "described",
		patterns.PipelineStep("reserve",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			},
			patterns.StepDescribedAs("hold stock for 15 minutes")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	// Returned rather than logged: nothing in this package owns a logger, because
	// a library that picks one picks it for every application that imports it.
	got := pipeline.Describe()
	for _, want := range []string{"described", "reserve", "hold stock for 15 minutes"} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe() = %q, missing %q", got, want)
		}
	}
}
