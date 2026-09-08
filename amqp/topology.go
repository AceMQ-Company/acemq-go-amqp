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
	// deadLettered is the set of queues [Topology.DeadLetters] was asked for.
	// Kept because a builder is written in either order — the queue before its
	// dead letters or after them — and the arguments the source queue needs can
	// only be written once both calls have happened.
	deadLettered map[string]bool
	err          error
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

// Queue adds a durable quorum queue.
//
// Quorum is the default here for the same reason it is the default in
// [Conn.DeclareQueue]: the type is part of the queue's identity, and five
// libraries that disagreed about it would give the second service to declare a
// shared queue a PRECONDITION_FAILED and no way to consume. Ask for something
// else with [OfType], and note that a queue asked to be [Transient],
// [Exclusive] or [AutoDelete] stays classic because RabbitMQ allows a quorum
// queue to be none of those.
//
// The rungs, {queue}.dlq and {queue}.parked are classic, and are declared as
// such by [Topology.Retries] and [Topology.DeadLetters] rather than by anything
// a caller has to remember.
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
	quorumByDefault(&spec)
	queue := namedQueue{Name: name, Spec: spec}
	if t.deadLettered[name] {
		// DeadLetters was asked for first. The arguments belong on this
		// declaration, and there is nowhere else to put them.
		t.stampDeadLetter(&queue)
	}
	t.queues = append(t.queues, queue)
	return t
}

// DeadLetters adds the two queues that catch what a queue cannot handle, the
// exchange they are reached through, and the arguments that point the source
// queue at them.
//
// {queue}.dlq is where a message goes when its attempts run out or it grows too
// old, and {queue}.parked is where one goes that never reached the handler at
// all — most often because it could not be decoded. Two queues rather than one
// because they are different problems with different answers: a message that
// failed five times is usually the world, and a message nothing could read is
// usually a producer, and whoever drains the dead letters should not have to
// sort them by hand.
//
// Both are bound to [DeadLetterExchange] on their own names, and the source
// queue is declared with
//
//	x-dead-letter-exchange    acemq.dlx
//	x-dead-letter-routing-key {queue}.dlq
//
// The routing key has to be set as well as the exchange, and it is what makes a
// shared exchange work at all: a message the broker dead-letters keeps the
// routing key it arrived under, so without it a message that reached orders as
// order.placed would arrive at acemq.dlx as order.placed, match no binding, and
// be dropped — the silent loss this is here to prevent.
//
// Those two arguments are the reason this is not optional. They are part of the
// source queue's declaration, and the Java, .NET, Python and Ruby libraries all
// write them: a Go service that declared orders without them and a Python
// service that declared orders with them cannot both consume it, because the
// second to declare is refused with PRECONDITION_FAILED.
//
// This does not replace the consumer's own path, and neither is dead code. A
// consumer that gives up republishes to {queue}.dlq with the reason in
// x-acemq-error and acknowledges the original, because that is the only way the
// reason survives and the only way the destination is one this library chose.
// The broker-side route is the backstop underneath it, and it catches what the
// library never sees: a message expiring against the source queue's own
// x-message-ttl, one dropped by x-max-length, one rejected by a consumer that
// is not this library at all. Without it those messages are discarded and
// nothing anywhere records that they existed.
//
// Neither {queue}.dlq nor {queue}.parked gets dead-lettering of its own. A
// dead-letter queue that dead-letters is a loop, and a loop is how a poison
// message becomes an outage.
//
// The source queue has to be declared by this topology as well — with
// [Topology.Queue], because it usually has arguments of its own — and asking
// for dead letters on a queue that is not there is refused by
// [Topology.Validate] rather than quietly leaving the arguments off the one
// declaration that needed them.
func (t *Topology) DeadLetters(queue string) *Topology {
	if t.err != nil {
		return t
	}
	if queue == "" {
		t.err = fmt.Errorf("acemq: DeadLetters needs the name of the queue they belong to")
		return t
	}
	if t.deadLettered[queue] {
		// Asked for twice is asked for once. Declaring {queue}.dlq a second time
		// is what Validate refuses, and a caller who says the same thing twice
		// has not said anything wrong.
		return t
	}
	if t.deadLettered == nil {
		t.deadLettered = map[string]bool{}
	}
	t.deadLettered[queue] = true

	// One exchange per broker rather than one per queue. Declaring it twice is
	// harmless to the broker and is what Validate refuses, and a plan that lists
	// acemq.dlx once per queue is a plan somebody stops reading.
	if !t.hasExchange(DeadLetterExchange) {
		t.Exchange(DeadLetterExchange, "direct")
	}
	// Classic, deliberately, and the same in all five libraries: Java declares
	// both QueueType.CLASSIC. They hold what nothing could handle, they are
	// drained by a person rather than consumed, and neither the replication nor
	// the memory a quorum queue costs buys anything for that.
	for _, target := range []string{DeadLetterQueue(queue), ParkedQueue(queue)} {
		t.Queue(target, OfType(QueueClassic)).Binding(target, DeadLetterExchange, target)
	}

	// The queue may already be here, or may be declared further down the chain;
	// Queue handles the second case.
	for i := range t.queues {
		if t.queues[i].Name == queue {
			t.stampDeadLetter(&t.queues[i])
			break
		}
	}
	return t
}

// stampDeadLetter writes the two dead-letter arguments onto a source queue.
//
// A caller who has already set either one by hand — with [QueueArg] or
// [DeadLetterTo] — is refused rather than overwritten. Either answer would be a
// guess about which of two conflicting instructions was meant, and the guess
// that silently wins is the one nobody finds out about until a message is
// somewhere else.
func (t *Topology) stampDeadLetter(queue *namedQueue) {
	if t.err != nil {
		return
	}
	var conflicting []string
	for _, arg := range []string{ArgDeadLetterExchange, ArgDeadLetterRoutingKey} {
		if _, set := queue.Spec.Args[arg]; set {
			conflicting = append(conflicting, arg)
		}
	}
	if len(conflicting) > 0 {
		t.err = fmt.Errorf(
			"acemq: queue %q asks for DeadLetters and also sets %s; pick one",
			queue.Name, strings.Join(conflicting, " and "))
		return
	}
	if queue.Spec.Args == nil {
		queue.Spec.Args = map[string]any{}
	}
	queue.Spec.Args[ArgDeadLetterExchange] = DeadLetterExchange
	queue.Spec.Args[ArgDeadLetterRoutingKey] = DeadLetterQueue(queue.Name)
}

// Retries adds the rung queues a policy's long waits need, and whatever brings
// an expired message home.
//
// One {queue}.retry.{delay} per distinct delay at or past the policy's
// threshold, each with x-message-ttl set to that delay and its dead-letter
// target set back to this queue, so a message parked on a rung returns here when
// its time is up. See [RetryLadder] for why the waiting happens there at all,
// and [RungArgs] for why the arguments are what they are.
//
// It takes the policy rather than a list of delays on purpose. The rungs a
// consumer will publish to are derived from the policy it is running, so
// anything else here would be a second copy of the same list, free to drift from
// the first — and the way that drift shows up is a retry published to a queue
// nobody declared, at the moment the service is already failing.
//
// The queue itself is not declared here, because it usually has arguments of its
// own; declare it with [Topology.Queue] as well. A policy whose waits are all
// short adds nothing, which is the common case.
func (t *Topology) Retries(queue string, p RetryPolicy) *Topology {
	if t.err != nil {
		return t
	}
	if queue == "" {
		t.err = fmt.Errorf("acemq: Retries needs the name of the queue the rungs belong to")
		return t
	}

	ladder := LadderFor(queue, p)
	if ladder.Empty() {
		return t
	}

	exchange, routingKey := retryReturn(queue)
	if exchange != "" && !t.hasExchange(exchange) {
		t.Exchange(exchange, "direct")
	}
	for _, rung := range ladder.Rungs {
		// Classic, and said out loud rather than left to the default, because
		// the default is quorum and a rung must not be one. Java declares every
		// rung QueueType.CLASSIC, so a rung this library declared as quorum
		// would be a queue a Java service could not declare — and the whole
		// point of a rung is that two services consuming the same queue agree
		// about it. A rung is also the wrong shape for quorum: nothing consumes
		// it, its messages sit there until x-message-ttl expires them, and
		// replicating a queue whose entire purpose is to wait buys nothing.
		opts := make([]QueueOption, 0, len(rung.Args)+1)
		opts = append(opts, OfType(QueueClassic))
		for name, value := range rung.Args {
			opts = append(opts, QueueArg(name, value))
		}
		t.Queue(rung.Queue, opts...)
	}
	if exchange != "" {
		t.Binding(queue, exchange, routingKey)
	}
	return t
}

func (t *Topology) hasExchange(name string) bool {
	for _, e := range t.exchanges {
		if e.Name == name {
			return true
		}
	}
	return false
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

	// A queue asked to dead-letter has to be one this topology declares, because
	// the wiring is two arguments on that declaration and there is nowhere else
	// to put them. Sorted so a topology with two of these reports the same one
	// every time.
	missing := make([]string, 0, len(t.deadLettered))
	for source := range t.deadLettered {
		if !queues[source] {
			missing = append(missing, source)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf(
			"acemq: DeadLetters names queue %q, which this topology does not declare; "+
				"the dead-letter arguments belong on that declaration", missing[0])
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

// describeQueue renders a queue the way somebody comparing this topology with
// the same one in another language would want to read it.
//
// The type is named in words even when it is classic, where the wire form says
// it by leaving x-queue-type out. A plan that showed the type only when it
// happened to be quorum would make the queues that must not be quorum — the
// rungs, the dead-letter queue and the parking lot — look like queues nobody
// had thought about. x-queue-type is then left out of the argument list, since
// it would be that same word a second time.
func describeQueue(s QueueSpec) string {
	var parts []string
	if s.Durable {
		parts = append(parts, "durable")
	} else {
		parts = append(parts, "transient")
	}
	parts = append(parts, string(queueTypeOf(s)))
	if s.AutoDelete {
		parts = append(parts, "auto-delete")
	}
	if s.Exclusive {
		parts = append(parts, "exclusive")
	}
	if len(s.Args) > 0 {
		keys := make([]string, 0, len(s.Args))
		for k := range s.Args {
			if k == ArgQueueType {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%v", k, s.Args[k]))
		}
	}
	return strings.Join(parts, ", ")
}

// QueueAdmin is a transport that can report on and remove queues.
//
// Separate from Transport because not every transport has an answer: the
// question "how many messages are waiting" means nothing to a transport that
// does not hold them. Both transports in this module implement it.
type QueueAdmin interface {
	// MessageCount is how many messages are waiting on a queue.
	MessageCount(ctx context.Context, name string) (int64, error)

	// DeleteQueue removes a queue and everything on it.
	DeleteQueue(ctx context.Context, name string) error
}

// MessageCount is how many messages are waiting on a queue.
//
// A number for a dashboard or a test, not a decision to make in a handler: it
// is a snapshot of a queue that is still moving, and by the time it is read
// somewhere else it is already wrong.
func (c *Conn) MessageCount(ctx context.Context, queue string) (int64, error) {
	admin, ok := c.transport.(QueueAdmin)
	if !ok {
		return 0, Fatalf("acemq: the %T transport cannot count messages", c.transport)
	}
	return admin.MessageCount(ctx, queue)
}

// DeleteQueue removes a queue and every message still on it.
//
// For tests and for tools. A service that deletes queues is usually a service
// that has confused a queue with a session — the messages go with it, including
// the ones somebody was about to be paid for.
func (c *Conn) DeleteQueue(ctx context.Context, queue string) error {
	admin, ok := c.transport.(QueueAdmin)
	if !ok {
		return Fatalf("acemq: the %T transport cannot delete queues", c.transport)
	}
	return admin.DeleteQueue(ctx, queue)
}

// QueueExists reports whether a queue is on the broker, creating nothing.
func (c *Conn) QueueExists(ctx context.Context, queue string) (bool, error) {
	inspector, ok := c.transport.(QueueInspector)
	if !ok {
		return false, Fatalf("acemq: the %T transport cannot look for queues", c.transport)
	}
	return inspector.QueueExists(ctx, queue)
}
