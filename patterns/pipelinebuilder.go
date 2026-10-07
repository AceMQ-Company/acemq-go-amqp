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
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/internal/onbehalf"
)

// A declared pipeline: one name, its steps in order, a queue behind each of
// them, and a consumer running on every one.
//
// Until this existed, a pipeline in Go was something a service joined rather
// than something it declared. [Then] and [FollowSlip] make one hop — consume,
// work, publish onwards — and a four-step flow meant four services each naming
// the same pipeline and each declaring its own queue, with nothing holding the
// shape in one place. Java, .NET, Python and Ruby all have the declaration; this
// is Go's.
//
// The wire contract is Java's, deliberately and to the letter, because the point
// of a pipeline in this family is that its steps need not be in the same
// language:
//
//   - the exchange is the pipeline's name, declared direct
//   - the queue behind a step is {pipeline}.{step}, bound to that exchange with
//     the step's name as the routing key
//   - a message carries the declared route — the step names and a position — so
//     the next hop is read off the message rather than worked out from whoever
//     happens to be consuming
//
// So a Go-declared pipeline can be continued by a Java service, and a Go step
// can be one hop of a Java-declared one. [Route] is the same declaration
// without the consumers, for that second case.
//
// # What Go cannot do, and says so
//
// Java's builder threads the types through at compile time: a step producing a
// Reservation can only be followed by one consuming a Reservation, and the
// compiler says so. Go's generics cannot express that — a method cannot
// introduce type parameters, so a fluent chain cannot carry the previous step's
// output type forward. Each step is therefore built on its own by
// [PipelineStep], and [NewPipeline] checks the chain with reflection when it is
// assembled: step 2's input must be step 1's output, or construction fails with
// an error naming both types.
//
// That is a worse guarantee than Java's and it is the honest one. The failure is
// loud, it happens at start-up rather than on the first message, and it is a
// check rather than a hope.
type Pipeline[T any] struct {
	route   *Route
	conn    *acemq.Conn
	running []io.Closer
	labels  []string
}

// stepConfig is what the options build.
type stepConfig struct {
	consumers int
	described string
	consume   []acemq.ConsumeOption
}

// StepOption adjusts one step of a pipeline.
type StepOption func(*stepConfig)

// StepConsumers runs this step on more than one consumer.
//
// This is how a slow step is scaled without touching the others, which is the
// reason each step has a queue of its own. One by default.
func StepConsumers(n int) StepOption {
	return func(c *stepConfig) { c.consumers = n }
}

// StepRetry gives this step a retry schedule of its own.
//
// A step that calls a payment gateway wants a different ladder from one that
// writes a row, and a pipeline where every step shares a schedule makes the
// slowest one set the pace for all of them.
func StepRetry(p acemq.RetryPolicy) StepOption {
	return func(c *stepConfig) { c.consume = append(c.consume, acemq.RetryWith(p)) }
}

// StepDescribedAs says what a step is for, in words, for whoever reads the log.
//
// Free text with no effect on routing — the name is the wire format and is
// restricted, the description is for people. Keeping them apart means improving
// how a stage reads cannot change where its messages go.
func StepDescribedAs(text string) StepOption {
	return func(c *stepConfig) { c.described = text }
}

// StepConsumeWith passes options straight to this step's consumers.
//
// The escape hatch, for anything [StepRetry] and [StepConsumers] do not cover —
// a codec for one step, a prefetch, a consumer argument.
func StepConsumeWith(opts ...acemq.ConsumeOption) StepOption {
	return func(c *stepConfig) { c.consume = append(c.consume, opts...) }
}

// StepSpec is one step, built by [PipelineStep] and assembled by [NewPipeline].
//
// An interface with unexported methods, so the only way to make one is
// [PipelineStep] — which is what keeps the input and output types on the step
// even though this interface names neither of them.
type StepSpec interface {
	stepName() string
	stepLabel() string
	inType() reflect.Type
	outType() reflect.Type
	start(ctx context.Context, conn *acemq.Conn, route *Route, position int) (io.Closer, error)
}

// PipelineStep builds one step of a [Pipeline].
//
// The handler takes what the previous step produced and returns what the next
// one consumes:
//
//	reserve := patterns.PipelineStep("reserve",
//		func(ctx context.Context, m acemq.Message[Order]) (Reservation, bool, error) {
//			if m.Payload.Digital {
//				return Reservation{}, false, nil   // nothing to reserve
//			}
//			return stock.Reserve(ctx, m.Payload)
//		},
//		patterns.StepConsumers(4),
//		patterns.StepDescribedAs("hold stock for 15 minutes so payment cannot oversell"))
//
// Returning false publishes nothing and accepts the message, which is how a step
// says "this one does not continue" without inventing an empty message — the
// same three-value shape [Then] uses, and reported as ended_early.
//
// The message is accepted only once the next one is published. A failure to
// publish retries this step, so a step that changes anything should be
// idempotent — again as [Then].
//
// The last step's output type is never published anywhere and is not checked
// against anything; a terminal step conventionally returns struct{}.
func PipelineStep[In, Out any](
	name string,
	handle func(context.Context, acemq.Message[In]) (Out, bool, error),
	opts ...StepOption,
) StepSpec {
	cfg := stepConfig{consumers: 1}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &pipelineStep[In, Out]{name: name, handle: handle, cfg: cfg}
}

type pipelineStep[In, Out any] struct {
	name   string
	handle func(context.Context, acemq.Message[In]) (Out, bool, error)
	cfg    stepConfig
}

func (s *pipelineStep[In, Out]) stepName() string { return s.name }

func (s *pipelineStep[In, Out]) stepLabel() string {
	if s.cfg.described == "" {
		return s.name
	}
	return s.name + " (" + s.cfg.described + ")"
}

func (s *pipelineStep[In, Out]) inType() reflect.Type  { return reflect.TypeFor[In]() }
func (s *pipelineStep[In, Out]) outType() reflect.Type { return reflect.TypeFor[Out]() }

func (s *pipelineStep[In, Out]) start(
	ctx context.Context, conn *acemq.Conn, route *Route, position int,
) (io.Closer, error) {
	if s.handle == nil {
		return nil, fmt.Errorf("acemq: pipeline step %q was given a nil handler", s.name)
	}

	id := pipelineID{pipeline: route.Name(), step: s.name}

	handler := func(ctx context.Context, m acemq.Message[In]) acemq.Ack {
		slip, present, err := SlipFrom(m.Envelope, route)
		if err != nil {
			return acemq.Reject(err)
		}
		if !present {
			// No slip on the message: something published straight to this step's
			// queue rather than starting a run. The declaration says where such a
			// message is, which is this step's position — the same answer Java
			// gives with RoutingSlip.startOf(stepNames()).advanceTo(position).
			slip = route.Start()
			for range position {
				slip = slip.Advance()
			}
		}

		out, carryOn, err := s.handle(ctx, m)
		if err != nil {
			if acemq.IsFatal(err) {
				return acemq.Reject(err)
			}
			return acemq.Retry(err)
		}
		if !carryOn {
			id.reportRun(ctx, OutcomeEndedEarly, m.Envelope.Age())
			return acemq.Accept()
		}

		advanced := slip.Advance()
		next, more := advanced.Next()
		if !more {
			// The end of the route. Asked before the output is looked at, because
			// the last step of a pipeline is usually a terminal action with nothing
			// to return, and reading its zero value as "ended early" would report
			// every completed run as a filtered one.
			id.reportRun(ctx, OutcomeCompleted, m.Envelope.Age())
			return acemq.Accept()
		}

		carrying, err := advanced.HeaderOptions()
		if err != nil {
			return acemq.Retry(err)
		}
		// The correlation carries so a whole run can be found, and the causation
		// names this message as the reason for the next one.
		all := append([]acemq.EnvelopeOption{
			acemq.CorrelationID(m.Envelope.CorrelationID),
			acemq.CausationID(m.Envelope.ID),
		}, carrying...)

		// Built per message and not once per step, because the stop comes off the
		// slip rather than out of the declaration: a message carrying a Java-shaped
		// itinerary names its own next exchange and routing key, which may be
		// somewhere this pipeline never declared. [FollowSlip] does the same for the
		// same reason.
		//
		// Mandatory for the reason [FollowSlip] is: the input is accepted as soon
		// as this returns, so a next step whose queue has gone would otherwise
		// lose the message with nothing reporting it.
		err = acemq.NewPublisher[Out](conn, next.Exchange, next.RoutingKey, acemq.Mandatory[Out]()).
			Send(onbehalf.Mark(ctx), out, all...)
		if err != nil {
			return acemq.Retry(fmt.Errorf(
				"acemq: pipeline %s step %s finished with message %s but the next message did"+
					" not go out: %w", route.Name(), s.name, m.Envelope.ID, err))
		}
		return acemq.Accept()
	}

	return NewConsumerGroup(ctx, conn, route.QueueFor(s.name), s.cfg.consumers, handler, s.cfg.consume...)
}

// stepNamePattern is what a step name may be, because it is a routing key and a
// queue suffix rather than a label. Java restricts it to the same characters,
// and a name that disagrees between two libraries is a message published where
// nothing is listening.
var stepNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.\-]*$`)

// NewPipeline declares a pipeline and starts a consumer on every step.
//
//	type Order struct{ ID string }
//	type Reservation struct{ OrderID string }
//
//	pipeline, err := patterns.NewPipeline[Order](ctx, mq, "fulfilment",
//		patterns.PipelineStep("validate", validate),
//		patterns.PipelineStep("reserve", reserve, patterns.StepConsumers(4)),
//		patterns.PipelineStep("dispatch", dispatch))
//	if err != nil {
//		return err
//	}
//	defer pipeline.Close()
//
//	runID, err := pipeline.Send(ctx, Order{ID: "A-1"})
//
// It declares the exchange, one queue per step and the bindings, then starts the
// consumers — so a pipeline that returns without an error is one whose topology
// exists and whose steps are running. Declaring is idempotent, and every service
// that declares the same pipeline agrees about it, which is what lets two of them
// run different steps of it.
//
// The chain is checked here rather than by the compiler: T must be the first
// step's input, and every step's output must be the next one's input. See
// [Pipeline] for why Go cannot do that at compile time.
func NewPipeline[T any](
	ctx context.Context, conn *acemq.Conn, name string, steps ...StepSpec,
) (*Pipeline[T], error) {
	if conn == nil {
		return nil, fmt.Errorf("acemq: a pipeline needs a connection")
	}
	if name == "" {
		return nil, fmt.Errorf("acemq: a pipeline needs a name, which is also its exchange")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("acemq: pipeline %q has no steps", name)
	}

	seen := make(map[string]bool, len(steps))
	names := make([]string, 0, len(steps))
	for _, step := range steps {
		stepName := step.stepName()
		if !stepNamePattern.MatchString(stepName) {
			return nil, fmt.Errorf(
				"acemq: pipeline %q has a step named %q, which cannot be a routing key:"+
					" letters, digits, dots and dashes only", name, stepName)
		}
		if seen[stepName] {
			return nil, fmt.Errorf(
				"acemq: pipeline %q declares the step %q twice, and two steps by one name are"+
					" one queue with two handlers", name, stepName)
		}
		seen[stepName] = true
		names = append(names, stepName)
	}

	// The entry type, and then each boundary. Reported with both type names and
	// the step names, because "cannot use Reservation as Order" says nothing
	// about which of four steps disagrees.
	if want, got := reflect.TypeFor[T](), steps[0].inType(); want != got {
		return nil, fmt.Errorf(
			"acemq: pipeline %q is sent %s but its first step %q consumes %s",
			name, want, names[0], got)
	}
	for i := 0; i < len(steps)-1; i++ {
		if out, in := steps[i].outType(), steps[i+1].inType(); out != in {
			return nil, fmt.Errorf(
				"acemq: pipeline %q step %q produces %s but the next step %q consumes %s",
				name, names[i], out, names[i+1], in)
		}
	}

	route := NewRoute(name, names...)
	pipeline := &Pipeline[T]{route: route, conn: conn}

	if err := conn.DeclareExchange(ctx, name, "direct"); err != nil {
		return nil, fmt.Errorf("acemq: cannot declare the exchange for pipeline %q: %w", name, err)
	}
	for _, stepName := range names {
		queue := route.QueueFor(stepName)
		// Durable, which on a broker that has quorum queues means a quorum queue:
		// a pipeline queue holds work that has already passed earlier steps, so
		// losing the node holding it loses partly-finished runs, and the steps
		// that already succeeded would have to run again.
		if err := conn.DeclareQueue(ctx, queue); err != nil {
			return nil, fmt.Errorf("acemq: cannot declare %q for pipeline %q: %w", queue, name, err)
		}
		if err := conn.Bind(ctx, queue, name, stepName); err != nil {
			return nil, fmt.Errorf("acemq: cannot bind %q to %q: %w", queue, name, err)
		}
	}

	for position, step := range steps {
		running, err := step.start(ctx, conn, route, position)
		if err != nil {
			// Whatever is already running is stopped, so a pipeline that failed to
			// start is not half a pipeline quietly consuming.
			_ = pipeline.Close()
			return nil, fmt.Errorf(
				"acemq: cannot start step %q of pipeline %q: %w", step.stepName(), name, err)
		}
		pipeline.running = append(pipeline.running, running)
	}

	for _, step := range steps {
		pipeline.labels = append(pipeline.labels, step.stepLabel())
	}
	return pipeline, nil
}

// Send starts a run at the first step, and returns the run's identifier.
//
// The identifier is on every hop and survives a dead-letter and a replay, so it
// is what a whole run is found by.
func (p *Pipeline[T]) Send(
	ctx context.Context, payload T, opts ...acemq.EnvelopeOption,
) (string, error) {
	slip := p.route.Start()
	if err := Start(ctx, p.conn, slip, payload, opts...); err != nil {
		return "", err
	}
	return slip.RunID(), nil
}

// Route is this pipeline's declaration without its consumers.
//
// What another service needs to join it: pass it to [AlongRoute] to run one step
// somewhere else, or to [FollowSlip] to continue a run.
func (p *Pipeline[T]) Route() *Route { return p.route }

// Name is the pipeline's name, which is also its exchange.
func (p *Pipeline[T]) Name() string { return p.route.Name() }

// Describe is what this pipeline does, in one line, with each step's
// description beside its name.
//
// Returned rather than logged. Java's pipeline writes this line itself; nothing
// in this package owns a logger, because a library that picks one picks it for
// every application that imports it. Log it where you log things:
//
//	log.Printf("%s", pipeline.Describe())
func (p *Pipeline[T]) Describe() string {
	return fmt.Sprintf("pipeline %s with %d steps: %s",
		p.route.Name(), len(p.labels), strings.Join(p.labels, " | "))
}

// QueueFor is the queue behind a step: {pipeline}.{step}.
func (p *Pipeline[T]) QueueFor(step string) string { return p.route.QueueFor(step) }

// Close stops every step's consumers.
//
// The queues and the exchange are left alone: they are durable, they may hold
// messages, and another service may be running steps of the same pipeline.
// Deleting them because one process is going away would take somebody else's
// work with it.
func (p *Pipeline[T]) Close() error {
	var first error
	for _, running := range p.running {
		if err := running.Close(); err != nil && first == nil {
			first = err
		}
	}
	p.running = nil
	return first
}
