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

package patterns

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// HeaderRoutingSlip carries the itinerary on the message.
const HeaderRoutingSlip = "acemq-routing-slip"

// Step is one stop on a routing slip.
type Step struct {
	// Exchange and RoutingKey are where this step's message goes.
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routingKey"`

	// Name is for reading a slip in a log. Optional.
	Name string `json:"name,omitempty"`

	// CompletedAt is when this step finished, set as the slip advances.
	CompletedAt string `json:"completedAt,omitempty"`
}

func (s Step) String() string {
	if s.Name != "" {
		return s.Name
	}
	return s.Exchange + "/" + s.RoutingKey
}

// RoutingSlip is an itinerary a message carries with it.
//
// The alternative to a central orchestrator. Each service does its part and
// sends the message to the next stop on the slip, so the route is decided once,
// by whoever started the work, and travels with the message rather than living
// in a component every service has to talk to.
//
//	slip := patterns.NewRoutingSlip().
//		Then("orders-events", "order.validate", "validate").
//		Then("orders-events", "order.charge", "charge").
//		Then("orders-events", "order.ship", "ship")
//
//	err := patterns.Start(ctx, mq, slip, order)
//
// What it costs: no single place says what the whole route is at runtime, so a
// route that is wrong is discovered one hop at a time. Worth it when the steps
// vary per message, and not worth it when every message goes the same way — a
// fixed chain of consumers is simpler and easier to follow.
type RoutingSlip struct {
	// Steps still to do, in order.
	Steps []Step `json:"steps"`

	// Done is what has already happened, oldest first, so a slip that fails
	// halfway says how far it got.
	Done []Step `json:"done,omitempty"`
}

// NewRoutingSlip starts an empty itinerary.
func NewRoutingSlip() *RoutingSlip { return &RoutingSlip{} }

// Then adds a stop.
func (s *RoutingSlip) Then(exchange, routingKey string, name ...string) *RoutingSlip {
	step := Step{Exchange: exchange, RoutingKey: routingKey}
	if len(name) > 0 {
		step.Name = name[0]
	}
	s.Steps = append(s.Steps, step)
	return s
}

// Next is the stop this message is going to, if there is one left.
func (s *RoutingSlip) Next() (Step, bool) {
	if len(s.Steps) == 0 {
		return Step{}, false
	}
	return s.Steps[0], true
}

// Advance returns a copy with the first step moved to Done.
func (s *RoutingSlip) Advance() *RoutingSlip {
	if len(s.Steps) == 0 {
		return s
	}
	completed := s.Steps[0]
	completed.CompletedAt = time.Now().UTC().Format(time.RFC3339)

	return &RoutingSlip{
		Steps: append([]Step(nil), s.Steps[1:]...),
		Done:  append(append([]Step(nil), s.Done...), completed),
	}
}

// Finished reports whether every step has been done.
func (s *RoutingSlip) Finished() bool { return len(s.Steps) == 0 }

func (s *RoutingSlip) String() string {
	done := make([]string, 0, len(s.Done))
	for _, step := range s.Done {
		done = append(done, step.String())
	}
	todo := make([]string, 0, len(s.Steps))
	for _, step := range s.Steps {
		todo = append(todo, step.String())
	}
	return fmt.Sprintf("RoutingSlip[done: %s | next: %s]",
		strings.Join(done, " -> "), strings.Join(todo, " -> "))
}

// Header renders the slip for the wire.
func (s *RoutingSlip) Header() (string, error) {
	encoded, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("acemq: cannot write the routing slip: %w", err)
	}
	return string(encoded), nil
}

// SlipFrom reads the itinerary off a message, if it has one.
func SlipFrom(env acemq.Envelope) (*RoutingSlip, bool, error) {
	raw, present := env.Headers[HeaderRoutingSlip]
	if !present {
		return nil, false, nil
	}

	var text string
	switch v := raw.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	default:
		return nil, true, acemq.Fatalf(
			"acemq: the routing slip on message %s is a %T, not text", env.ID, raw)
	}

	var slip RoutingSlip
	if err := json.Unmarshal([]byte(text), &slip); err != nil {
		// A slip that will not parse will not parse next time either.
		return nil, true, acemq.Fatalf(
			"acemq: cannot read the routing slip on message %s: %v", env.ID, err)
	}
	return &slip, true, nil
}

// Start sends a payload to the first stop on a slip.
func Start[T any](
	ctx context.Context, conn *acemq.Conn, slip *RoutingSlip, payload T,
	opts ...acemq.EnvelopeOption,
) error {
	step, ok := slip.Next()
	if !ok {
		return fmt.Errorf("acemq: this routing slip has no steps in it")
	}

	header, err := slip.Header()
	if err != nil {
		return err
	}

	all := append(append([]acemq.EnvelopeOption(nil), opts...), acemq.Header(HeaderRoutingSlip, header))
	return acemq.NewPublisher[T](conn, step.Exchange, step.RoutingKey).Send(ctx, payload, all...)
}

// FollowSlip wraps a handler so the message continues to its next stop.
//
//	sub, err := acemq.Consume(ctx, mq, "charge-queue",
//		patterns.FollowSlip(mq, func(ctx context.Context, m acemq.Message[Order]) (Order, error) {
//			return charge(ctx, m.Payload)
//		}))
//
// The handler returns the payload to send onwards, which may be the one it
// received or a changed copy. When the slip has no steps left the work is
// finished and nothing more is published.
//
// The message is accepted only once the next one is out, so a failure to
// publish retries the step — which is why a step that changes anything should
// be idempotent.
func FollowSlip[T any](
	conn *acemq.Conn, step func(context.Context, acemq.Message[T]) (T, error),
) acemq.Handler[T] {
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		slip, present, err := SlipFrom(m.Envelope)
		if err != nil {
			return acemq.Reject(err)
		}
		if !present {
			return acemq.Reject(acemq.Fatalf(
				"acemq: message %s has no routing slip, so there is nowhere to send it next",
				m.Envelope.ID))
		}

		payload, err := step(ctx, m)
		if err != nil {
			if acemq.IsFatal(err) {
				return acemq.Reject(err)
			}
			return acemq.Retry(err)
		}

		advanced := slip.Advance()
		next, more := advanced.Next()
		if !more {
			// The end of the itinerary. Nothing to publish, and the work is
			// done.
			return acemq.Accept()
		}

		header, err := advanced.Header()
		if err != nil {
			return acemq.Reject(acemq.Fatal(err))
		}

		err = acemq.NewPublisher[T](conn, next.Exchange, next.RoutingKey).Send(ctx, payload,
			acemq.CorrelationID(m.Envelope.CorrelationID),
			acemq.CausationID(m.Envelope.ID),
			acemq.Header(HeaderRoutingSlip, header))
		if err != nil {
			return acemq.Retry(fmt.Errorf(
				"acemq: %s is done for message %s but the next step did not go out: %w",
				slip.Steps[0], m.Envelope.ID, err))
		}
		return acemq.Accept()
	}
}
