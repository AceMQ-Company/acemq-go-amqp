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

package acemq

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Topology is the set of exchanges, queues and bindings a service expects.
//
// Declaring them one call at a time works, and stops working the moment
// somebody needs to know what a service will do to a broker before it does it.
// A Topology can be printed, compared against what is really there, and applied
// — which is the difference between a deployment somebody approves and one they
// find out about.
//
//	topology := acemq.NewTopology().
//		Exchange("orders-events", "topic").
//		Queue("shipping-orders", acemq.DeadLetterTo("shipping-dead")).
//		Queue("shipping-dead").
//		Binding("shipping-orders", "orders-events", "order.placed").
//		Binding("shipping-orders", "orders-events", "order.cancelled")
//
//	if err := topology.Apply(ctx, mq); err != nil {
//		return err
//	}
type Topology struct {
	exchanges []namedExchange
	queues    []namedQueue
	bindings  []BindingSpec
	err       error
}

type namedExchange struct {
	Name string
	Spec ExchangeSpec
}

type namedQueue struct {
	Name string
	Spec QueueSpec
}

// BindingSpec routes messages from an exchange to a queue.
type BindingSpec struct {
	Queue      string
	Exchange   string
	RoutingKey string
}

func (b BindingSpec) String() string {
	return fmt.Sprintf("%s -> %s (%s)", b.Exchange, b.Queue, b.RoutingKey)
}

// NewTopology starts an empty description.
func NewTopology() *Topology { return &Topology{} }

// Exchange adds a durable exchange. Kind is direct, topic, fanout or headers.
func (t *Topology) Exchange(name, kind string, opts ...ExchangeOption) *Topology {
	if t.err != nil {
		return t
	}
	if name == "" {
		t.err = fmt.Errorf("acemq: an exchange in the topology has no name")
		return t
	}
	spec := ExchangeSpec{Kind: kind, Durable: true}
	for _, opt := range opts {
		opt(&spec)
	}
	t.exchanges = append(t.exchanges, namedExchange{Name: name, Spec: spec})
	return t
}

// Queue adds a durable queue.
func (t *Topology) Queue(name string, opts ...QueueOption) *Topology {
	if t.err != nil {
		return t
	}
	if name == "" {
		t.err = fmt.Errorf("acemq: a queue in the topology has no name")
		return t
	}
	spec := QueueSpec{Durable: true}
	for _, opt := range opts {
		opt(&spec)
	}
	t.queues = append(t.queues, namedQueue{Name: name, Spec: spec})
	return t
}

// Binding routes messages matching a key from an exchange to a queue.
func (t *Topology) Binding(queue, exchange, routingKey string) *Topology {
	if t.err != nil {
		return t
	}
	t.bindings = append(t.bindings, BindingSpec{
		Queue: queue, Exchange: exchange, RoutingKey: routingKey})
	return t
}

// Err is the first problem found while building, if there was one.
func (t *Topology) Err() error { return t.err }

// Validate reports what is wrong with the description itself, before any of it
// reaches a broker.
//
// A binding naming a queue the topology does not declare is the mistake worth
// catching here. The broker would accept it if the queue happened to exist
// already, and the service would then depend on something nothing declares.
func (t *Topology) Validate() error {
	if t.err != nil {
		return t.err
	}

	queues := map[string]bool{}
	for _, q := range t.queues {
		if queues[q.Name] {
			return fmt.Errorf("acemq: the topology declares queue %q twice", q.Name)
		}
		queues[q.Name] = true
	}

	exchanges := map[string]bool{}
	for _, e := range t.exchanges {
		if exchanges[e.Name] {
			return fmt.Errorf("acemq: the topology declares exchange %q twice", e.Name)
		}
		if e.Spec.Kind == "" {
			return fmt.Errorf("acemq: exchange %q has no kind (direct, topic, fanout or headers)", e.Name)
		}
		exchanges[e.Name] = true
	}

	for _, b := range t.bindings {
		if !queues[b.Queue] {
			return fmt.Errorf(
				"acemq: binding %s names queue %q, which this topology does not declare",
				b, b.Queue)
		}
		if b.Exchange != "" && !exchanges[b.Exchange] {
			return fmt.Errorf(
				"acemq: binding %s names exchange %q, which this topology does not declare",
				b, b.Exchange)
		}
		if b.Exchange == "" {
			return fmt.Errorf(
				"acemq: binding %s names the default exchange, which cannot be bound to", b)
		}
	}
	return nil
}

// ApplyMode is whether applying a topology changes the broker.
type ApplyMode int

const (
	// Declare creates what is missing. The ordinary mode.
	Declare ApplyMode = iota

	// DryRun changes nothing and reports what applying would do.
	//
	// It asks the broker about each queue rather than guessing, so a queue that
	// already exists with different settings is reported as a difference rather
	// than as something that would be created. Exchanges and bindings cannot be
	// inspected without the management API, so those are reported as unknown —
	// a plan that guessed would be worse than one honest about what it cannot
	// see.
	DryRun
)

func (m ApplyMode) String() string {
	if m == DryRun {
		return "dry-run"
	}
	return "declare"
}

// ApplyWith applies the topology, or reports what applying it would do.
//
//	plan, err := topology.ApplyWith(ctx, mq, acemq.DryRun)
//	for _, action := range plan {
//		log.Println(action)
//	}
//
// A deployment that changes a broker should be something somebody can read
// first, against the broker it is going to change rather than in the abstract.
func (t *Topology) ApplyWith(ctx context.Context, conn *Conn, mode ApplyMode) ([]PlanAction, error) {
	if mode != DryRun {
		if err := t.Apply(ctx, conn); err != nil {
			return nil, err
		}
		return t.Plan()
	}

	if err := t.Validate(); err != nil {
		return nil, err
	}

	checker, canCheck := conn.transport.(DriftChecker)
	inspector, canInspect := conn.transport.(QueueInspector)

	actions := make([]PlanAction, 0, len(t.exchanges)+len(t.queues)+len(t.bindings))
	for _, e := range t.exchanges {
		// Nothing in AMQP reports what an exchange looks like, so this says so
		// rather than implying the exchange is absent.
		actions = append(actions, PlanAction{
			Kind: "exchange", Name: e.Name,
			Detail: describeExchange(e.Spec) + " — unknown, AMQP cannot report exchanges",
		})
	}

	for _, q := range t.queues {
		detail := describeQueue(q.Spec)
		switch {
		case !canCheck || !canInspect:
			detail += " — unknown, this transport cannot check"
		default:
			exists, err := inspector.QueueExists(ctx, q.Name)
			switch {
			case err != nil:
				detail += " — unknown, the broker could not be asked: " + err.Error()
			case !exists:
				detail += " — would create"
			default:
				// Only now is a declaration safe to make: the queue is there, so
				// the declaration is a question rather than a change.
				if err := checker.CheckQueue(ctx, q.Name, q.Spec); err != nil {
					detail += " — differs: " + err.Error()
				} else {
					detail += " — matches"
				}
			}
		}
		actions = append(actions, PlanAction{Kind: "queue", Name: q.Name, Detail: detail})
	}

	for _, b := range t.bindings {
		actions = append(actions, PlanAction{
			Kind: "binding", Name: b.Queue,
			Detail: fmt.Sprintf("from %s on %s — unknown, AMQP cannot report bindings",
				b.Exchange, b.RoutingKey),
		})
	}
	return actions, nil
}

// Apply declares everything, in the order a broker needs: exchanges, then
// queues, then the bindings between them.
//
// It stops at the first failure. A queue that already exists with different
// settings is refused by the broker with PRECONDITION_FAILED, and that refusal
// is passed on rather than swallowed — it means this service and the broker
// disagree about what the queue is.
func (t *Topology) Apply(ctx context.Context, conn *Conn) error {
	if err := t.Validate(); err != nil {
		return err
	}

	for _, e := range t.exchanges {
		if err := conn.transport.DeclareExchange(ctx, e.Name, e.Spec); err != nil {
			return fmt.Errorf("acemq: applying the topology: %w", err)
		}
	}
	for _, q := range t.queues {
		if err := conn.transport.DeclareQueue(ctx, q.Name, q.Spec); err != nil {
			return fmt.Errorf("acemq: applying the topology: %w", err)
		}
	}
	for _, b := range t.bindings {
		if err := conn.transport.Bind(ctx, b.Queue, b.Exchange, b.RoutingKey); err != nil {
			return fmt.Errorf("acemq: applying the topology: %w", err)
		}
	}
	return nil
}

// PlanAction is one thing applying the topology would do.
type PlanAction struct {
	Kind   string // "exchange", "queue" or "binding"
	Name   string
	Detail string
}

func (a PlanAction) String() string {
	if a.Detail == "" {
		return fmt.Sprintf("declare %s %s", a.Kind, a.Name)
	}
	return fmt.Sprintf("declare %s %s (%s)", a.Kind, a.Name, a.Detail)
}

// Plan is what [Topology.Apply] would do, without doing it.
//
// A deployment that changes a broker should be something somebody can read
// first. This is deliberately not a diff against the live broker: AMQP offers
// no way to enumerate what is there without the management API, and a plan that
// quietly guessed would be worse than one that is honest about being a
// statement of intent.
func (t *Topology) Plan() ([]PlanAction, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}

	actions := make([]PlanAction, 0, len(t.exchanges)+len(t.queues)+len(t.bindings))
	for _, e := range t.exchanges {
		actions = append(actions, PlanAction{
			Kind: "exchange", Name: e.Name, Detail: describeExchange(e.Spec)})
	}
	for _, q := range t.queues {
		actions = append(actions, PlanAction{
			Kind: "queue", Name: q.Name, Detail: describeQueue(q.Spec)})
	}
	for _, b := range t.bindings {
		actions = append(actions, PlanAction{
			Kind: "binding", Name: b.Queue, Detail: fmt.Sprintf("from %s on %s", b.Exchange, b.RoutingKey)})
	}
	return actions, nil
}

// String renders the plan as something worth putting in a deployment log.
func (t *Topology) String() string {
	actions, err := t.Plan()
	if err != nil {
		return "Topology{invalid: " + err.Error() + "}"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Topology: %d exchanges, %d queues, %d bindings\n",
		len(t.exchanges), len(t.queues), len(t.bindings)))
	for _, a := range actions {
		b.WriteString("  " + a.String() + "\n")
	}
	return b.String()
}

// Check reports whether the broker already agrees with this topology.
//
// It works by declaring each queue on a channel of its own and watching for the
// broker's refusal. A queue that exists with different settings answers
// PRECONDITION_FAILED, which is the only way AMQP will tell you about drift
// without the management API — and a failed declaration kills its channel,
// which is why each one needs its own.
//
// Queues that are not there yet are passed over rather than declared: a queue
// that does not exist cannot disagree with anything, and creating one as a side
// effect of asking a question would make Check unsafe to run against a broker
// somebody is only inspecting.
func (t *Topology) Check(ctx context.Context, conn *Conn) ([]DriftReport, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}

	checker, ok := conn.transport.(DriftChecker)
	if !ok {
		return nil, fmt.Errorf(
			"acemq: the %T transport cannot check for drift", conn.transport)
	}
	inspector, canInspect := conn.transport.(QueueInspector)

	var reports []DriftReport
	for _, q := range t.queues {
		if canInspect {
			exists, err := inspector.QueueExists(ctx, q.Name)
			if err != nil {
				return nil, fmt.Errorf("acemq: cannot check queue %q: %w", q.Name, err)
			}
			if !exists {
				continue
			}
		}
		err := checker.CheckQueue(ctx, q.Name, q.Spec)
		if err != nil {
			reports = append(reports, DriftReport{
				Kind: "queue", Name: q.Name, Reason: err.Error()})
		}
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].Name < reports[j].Name })
	return reports, nil
}

// DriftReport is one place the broker and this topology disagree.
type DriftReport struct {
	Kind   string
	Name   string
	Reason string
}

func (d DriftReport) String() string {
	return fmt.Sprintf("%s %s: %s", d.Kind, d.Name, d.Reason)
}

// DriftChecker is a transport that can tell whether a queue matches what is
// asked for. Both transports in this module implement it.
type DriftChecker interface {
	// CheckQueue returns nil when the broker agrees, and an error describing
	// the disagreement when it does not. It must not disturb the connection it
	// is called on.
	CheckQueue(ctx context.Context, name string, spec QueueSpec) error
}

// QueueInspector is a transport that can say whether a queue is there without
// creating it. Both transports in this module implement it.
//
// It exists because CheckQueue cannot answer that question. A declaration is
// the only thing AMQP offers, and declaring a queue that is missing creates it —
// so a dry run built on CheckQueue alone would create the very queues it was
// asked only to describe. A dry run asks this first and checks only what is
// already there.
type QueueInspector interface {
	// QueueExists reports whether the queue is on the broker. It must not
	// create it, and must not disturb the connection it is called on.
	QueueExists(ctx context.Context, name string) (bool, error)
}

func describeExchange(s ExchangeSpec) string {
	parts := []string{s.Kind}
	if !s.Durable {
		parts = append(parts, "transient")
	}
	if s.AutoDelete {
		parts = append(parts, "auto-delete")
	}
	return strings.Join(parts, ", ")
}

func describeQueue(s QueueSpec) string {
	var parts []string
	if s.Durable {
		parts = append(parts, "durable")
	} else {
		parts = append(parts, "transient")
	}
	if s.AutoDelete {
		parts = append(parts, "auto-delete")
	}
	if s.Exclusive {
		parts = append(parts, "exclusive")
	}
	if len(s.Args) > 0 {
		keys := make([]string, 0, len(s.Args))
		for k := range s.Args {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%v", k, s.Args[k]))
		}
	}
	return strings.Join(parts, ", ")
}
