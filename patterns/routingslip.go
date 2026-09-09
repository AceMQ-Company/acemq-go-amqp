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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// HeaderRoutingSlip carries the itinerary on the message as JSON.
//
// This library's own form, and its default. Python and Ruby write the same
// header with the same shape, so three of the five libraries speak it.
const HeaderRoutingSlip = "acemq-routing-slip"

// SlipForm is how a slip is written on the wire.
//
// There are two, and both are read. That is not indecision: they answer
// different questions, and each is the right answer to its own.
type SlipForm int

const (
	// FormJSON writes [HeaderRoutingSlip], a JSON document holding every step's
	// exchange and routing key. The default, and what [NewRoutingSlip] produces.
	//
	// Self-describing: a message carries its whole itinerary, so any consumer can
	// send it onwards with nothing declared anywhere and an operator reading a
	// dead-letter queue can see where it was going. The route can also differ per
	// message, which is the reason to use a slip rather than a fixed chain of
	// consumers at all.
	FormJSON SlipForm = iota

	// FormSteps writes [acemq.HeaderRoute], [acemq.HeaderRoutePosition] and
	// [acemq.HeaderRouteID] — the ordered step names, the position, and the run
	// identifier. The form the Java library writes.
	//
	// Names only, resolved against a [Route] declared in code. That is smaller on
	// the wire and stays readable in a management console, and it is what a Go
	// step writes when it is one step of a pipeline Java declared: the Java steps
	// after it are looking for these three headers and would not find a JSON
	// slip.
	//
	// It cannot express a route that varies per message, because the names mean
	// nothing without the declaration.
	FormSteps
)

func (f SlipForm) String() string {
	if f == FormSteps {
		return "steps"
	}
	return "json"
}

// Route is a pipeline declared in code, the way the Java library declares one.
//
//	route := patterns.NewRoute("orders", "validate", "charge", "ship")
//
// The name is the exchange, a step name is the routing key, and the queue behind
// a step is {route}.{step} — which is how Java's Pipeline lays out its topology,
// so a Go consumer bound to orders.charge is the charge step of a Java pipeline
// called orders.
//
// A [SlipForm] of [FormSteps] carries only names, so this is what turns those
// names back into somewhere to publish. Reading such a slip without one is
// possible — the names are on the message — but sending it onwards is not.
type Route struct {
	name  string
	steps []string
}

// NewRoute declares a pipeline: its name, which is the exchange, and its steps
// in order.
func NewRoute(name string, steps ...string) *Route {
	return &Route{name: name, steps: append([]string(nil), steps...)}
}

// Name is the pipeline's name, which is also its exchange.
func (r *Route) Name() string { return r.name }

// Steps are the step names, in order.
func (r *Route) Steps() []string { return append([]string(nil), r.steps...) }

// QueueFor is the queue behind a step: {route}.{step}, which is what Java's
// Pipeline declares and binds.
func (r *Route) QueueFor(step string) string { return r.name + "." + step }

// Start begins a run of this route, at its first step, with a fresh run
// identifier and written in [FormSteps].
func (r *Route) Start() *RoutingSlip {
	slip := &RoutingSlip{form: FormSteps, runID: newRunID(), route: r}
	for _, step := range r.steps {
		slip.Steps = append(slip.Steps, Step{
			Exchange: r.name, RoutingKey: step, Name: step})
	}
	return slip
}

// step turns a declared step name into somewhere to publish.
func (r *Route) step(name string) Step {
	return Step{Exchange: r.name, RoutingKey: name, Name: name}
}

func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A run identifier that repeats groups two runs together in a trace and
		// breaks nothing else, so a failed read is not worth refusing to publish
		// over. The clock is a poor identifier and a present one.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

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
// # Two forms on the wire
//
// This type is the same either way; only [SlipForm] changes what is written. A
// slip read off a message keeps the form it arrived in, so a Go step in a
// Java-declared pipeline answers in the shape the next Java step is looking for
// without being told to.
type RoutingSlip struct {
	// Steps still to do, in order.
	Steps []Step `json:"steps"`

	// Done is what has already happened, oldest first, so a slip that fails
	// halfway says how far it got.
	Done []Step `json:"done,omitempty"`

	// Unexported, so the JSON form on the wire is unchanged by their presence:
	// encoding/json ignores them, and a slip written by an older version of this
	// library is byte-identical to one written now.
	form  SlipForm
	runID string
	route *Route
}

// NewRoutingSlip starts an empty itinerary, written as JSON.
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
		form:  s.form, runID: s.runID, route: s.route,
	}
}

// Finished reports whether every step has been done.
func (s *RoutingSlip) Finished() bool { return len(s.Steps) == 0 }

// Form is how this slip will be written. [FormJSON] unless it was read from a
// declared route's headers or [AsSteps] was called.
func (s *RoutingSlip) Form() SlipForm { return s.form }

// RunID identifies one run through a pipeline across every hop, for a slip in
// [FormSteps]. Empty for a JSON slip, which has no such field.
func (s *RoutingSlip) RunID() string { return s.runID }

// AsSteps returns a copy written in [FormSteps], against a declared route.
//
//	route := patterns.NewRoute("orders", "validate", "charge", "ship")
//	slip := patterns.NewRoutingSlip().
//		Then("orders", "charge", "charge").
//		Then("orders", "ship", "ship").
//		AsSteps(route)
//
// Usually unnecessary: [Route.Start] produces one already, and a slip read from
// a Java message keeps the form it arrived in. This is for a Go publisher that
// built an itinerary by hand and wants Java steps to be able to follow it.
//
// A run identifier is generated when there is not one already, so the run is
// traceable across hops the way Java's is.
func (s *RoutingSlip) AsSteps(route *Route) *RoutingSlip {
	next := *s
	next.form = FormSteps
	next.route = route
	if next.runID == "" {
		next.runID = newRunID()
	}
	return &next
}

// AsJSON returns a copy written as the JSON slip, whatever form it arrived in.
//
// Turning a declared route back into a self-describing slip: the step names lose
// their dependence on a declaration, at the cost of a bigger header and of Java
// steps downstream no longer recognising it.
func (s *RoutingSlip) AsJSON() *RoutingSlip {
	next := *s
	next.form = FormJSON
	return &next
}

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

// Header renders the JSON form of the slip, whatever form it will be written in.
//
// Kept because it is what callers already build headers from by hand. Prefer
// [RoutingSlip.HeaderOptions], which writes whichever form the slip is in.
func (s *RoutingSlip) Header() (string, error) {
	encoded, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("acemq: cannot write the routing slip: %w", err)
	}
	return string(encoded), nil
}

// HeaderOptions renders the slip as the envelope options that put it on a
// message, in whichever [SlipForm] the slip is in.
func (s *RoutingSlip) HeaderOptions() ([]acemq.EnvelopeOption, error) {
	if s.form == FormSteps {
		// Every step, not only the ones left: the declared form carries the whole
		// route and a position into it, so a reader can see where a message is
		// within the run rather than only where it is going.
		names := make([]string, 0, len(s.Done)+len(s.Steps))
		for _, step := range s.Done {
			names = append(names, step.String())
		}
		for _, step := range s.Steps {
			names = append(names, step.String())
		}
		return []acemq.EnvelopeOption{acemq.Route(names, len(s.Done), s.runID)}, nil
	}

	header, err := s.Header()
	if err != nil {
		return nil, err
	}
	return []acemq.EnvelopeOption{acemq.Header(HeaderRoutingSlip, header)}, nil
}

// SlipFrom reads the itinerary off a message, if it has one, in either form.
//
// The JSON slip is looked for first because it is this library's default and
// three of the five write it. A message carrying a declared route instead — the
// three x-acemq-route headers a Java pipeline writes — is read too, and the slip
// that comes back remembers which it was, so sending it onwards keeps the shape
// the rest of that pipeline expects.
//
// route resolves a declared route's step names into somewhere to publish, and
// may be nil. Without it a declared route is still readable — the names are on
// the message — but every step has an empty exchange, so it cannot be sent
// onwards. [FollowSlip] says so rather than publishing to the wrong place; see
// [AlongRoute].
func SlipFrom(env acemq.Envelope, route ...*Route) (*RoutingSlip, bool, error) {
	var declared *Route
	if len(route) > 0 {
		declared = route[0]
	}

	if raw, present := env.Headers[HeaderRoutingSlip]; present {
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
		slip.route = declared
		return &slip, true, nil
	}

	names := env.RouteSteps()
	if len(names) == 0 {
		return nil, false, nil
	}

	// The position is where the message is now, so everything before it has been
	// done and everything from it onwards has not. A position past the end is a
	// finished route rather than an error: Java advances past the last step and
	// the message that carries it is the one nobody has to send anywhere.
	at := env.RoutePosition
	if at < 0 {
		at = 0
	}
	if at > len(names) {
		at = len(names)
	}

	slip := &RoutingSlip{form: FormSteps, runID: env.RouteID, route: declared}
	for i, name := range names {
		var step Step
		if declared != nil {
			step = declared.step(name)
		} else {
			step = Step{Name: name}
		}
		if i < at {
			slip.Done = append(slip.Done, step)
		} else {
			slip.Steps = append(slip.Steps, step)
		}
	}
	return slip, true, nil
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

	carrying, err := slip.HeaderOptions()
	if err != nil {
		return err
	}

	all := append(append([]acemq.EnvelopeOption(nil), opts...), carrying...)
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
//
// Name the pipeline with [InPipeline] and the end of the itinerary is reported
// as a completed run. The step's own name comes off the slip, so [AtStep] is
// only needed to override it.
//
// # Following a Java-declared pipeline
//
// Both forms of slip are read, and a message is sent onwards in the form it
// arrived in. A Java pipeline writes step names against a route declared in
// code, so tell this handler which route those names belong to:
//
//	route := patterns.NewRoute("orders", "validate", "charge", "ship")
//
//	sub, err := acemq.Consume(ctx, mq, route.QueueFor("charge"),
//		patterns.FollowSlip(mq, charge, patterns.AlongRoute(route)))
//
// The Go step then publishes to the orders exchange with the next step's name as
// the routing key and the position advanced — which is exactly what the Java
// step after it is waiting for.
func FollowSlip[T any](
	conn *acemq.Conn, step func(context.Context, acemq.Message[T]) (T, error),
	opts ...PipelineOption,
) acemq.Handler[T] {
	id := pipelineIDFrom(opts)
	return func(ctx context.Context, m acemq.Message[T]) acemq.Ack {
		slip, present, err := SlipFrom(m.Envelope, id.route)
		if err != nil {
			return acemq.Reject(err)
		}
		if !present {
			return acemq.Reject(acemq.Fatalf(
				"acemq: message %s has no routing slip, so there is nowhere to send it next",
				m.Envelope.ID))
		}
		// A declared route is names only. Without the declaration they resolve to
		// nowhere, and publishing to an empty exchange would send the message to a
		// queue named for the step instead of to the pipeline's — quietly, and to
		// the wrong place. Refusing is the honest answer, and it is fatal because
		// a missing declaration will still be missing on the fourth attempt.
		if slip.form == FormSteps && slip.route == nil {
			return acemq.Reject(acemq.Fatalf(
				"acemq: message %s carries a declared route (%s) but this handler was"+
					" not given one, so a step name cannot be turned into somewhere to"+
					" publish. Pass patterns.AlongRoute(route) naming the pipeline these"+
					" steps belong to", m.Envelope.ID, m.Envelope.Route))
		}

		payload, err := step(ctx, m)
		if err != nil {
			if acemq.IsFatal(err) {
				return acemq.Reject(err)
			}
			return acemq.Retry(err)
		}

		// The step just finished, named before the slip advances past it.
		done := id
		if done.step == "" && len(slip.Steps) > 0 {
			done.step = slip.Steps[0].String()
		}

		advanced := slip.Advance()
		next, more := advanced.Next()
		if !more {
			// The end of the itinerary. Nothing to publish, and the work is
			// done. The age is the envelope's, so what is reported is the whole
			// run rather than this step: the envelope was made when the message
			// entered the pipeline and has carried through every hop.
			done.reportRun(ctx, OutcomeCompleted, m.Envelope.Age())
			return acemq.Accept()
		}

		carrying, err := advanced.HeaderOptions()
		if err != nil {
			return acemq.Reject(acemq.Fatal(err))
		}

		onwards := append([]acemq.EnvelopeOption{
			acemq.CorrelationID(m.Envelope.CorrelationID),
			acemq.CausationID(m.Envelope.ID),
		}, carrying...)

		err = acemq.NewPublisher[T](conn, next.Exchange, next.RoutingKey).
			Send(ctx, payload, onwards...)
		if err != nil {
			return acemq.Retry(fmt.Errorf(
				"acemq: %s is done for message %s but the next step did not go out: %w",
				slip.Steps[0], m.Envelope.ID, err))
		}
		return acemq.Accept()
	}
}
