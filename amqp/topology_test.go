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
	"sort"
	"strings"
	"testing"
	"time"
)

func aTopology() *Topology {
	return NewTopology().
		Exchange("orders-events", "topic").
		Queue("shipping-orders", DeadLetterTo("shipping-dead")).
		Queue("shipping-dead").
		Binding("shipping-orders", "orders-events", "order.placed").
		Binding("shipping-orders", "orders-events", "order.cancelled")
}

func TestATopologyDeclaresEverythingInIt(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// The proof is that a message routes end to end, not that the calls
	// returned nil.
	got := make(chan string, 1)
	sub, err := Consume(ctx, mq, "shipping-orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			got <- m.Payload.OrderID
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	err = NewPublisher[OrderPlaced](mq, "orders-events", "order.placed").
		Send(ctx, OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-got:
		if id != "o-1" {
			t.Errorf("got %q", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the topology was applied but nothing routed through it")
	}
}

func TestApplyingTwiceIsFine(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}
	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatalf("applying an unchanged topology a second time failed: %v", err)
	}
}

// TestABindingToAnUndeclaredQueueIsCaughtBeforeTheBroker is the mistake worth
// catching in the description itself.
//
// The broker would accept it whenever the queue happens to exist already, and
// the service would then quietly depend on something nothing declares — until
// a fresh environment, where it fails at start-up for reasons nobody can see.
func TestABindingToAnUndeclaredQueueIsCaughtBeforeTheBroker(t *testing.T) {
	topology := NewTopology().
		Exchange("orders-events", "topic").
		Binding("a-queue-nobody-declared", "orders-events", "order.placed")

	err := topology.Validate()

	if err == nil {
		t.Fatal("a binding to an undeclared queue was accepted")
	}
	if !strings.Contains(err.Error(), "a-queue-nobody-declared") {
		t.Errorf("the error does not name the queue: %v", err)
	}
}

func TestABindingToAnUndeclaredExchangeIsCaught(t *testing.T) {
	topology := NewTopology().
		Queue("orders").
		Binding("orders", "an-exchange-nobody-declared", "#")

	if topology.Validate() == nil {
		t.Fatal("a binding to an undeclared exchange was accepted")
	}
}

func TestBindingToTheDefaultExchangeIsRefused(t *testing.T) {
	// It cannot be bound to, and a topology that says otherwise is a
	// misunderstanding worth naming rather than a call that fails obscurely.
	topology := NewTopology().Queue("orders").Binding("orders", "", "orders")

	err := topology.Validate()

	if err == nil {
		t.Fatal("binding to the default exchange was accepted")
	}
	if !strings.Contains(err.Error(), "default exchange") {
		t.Errorf("the error does not explain: %v", err)
	}
}

func TestDeclaringTheSameThingTwiceInOneTopologyIsRefused(t *testing.T) {
	if NewTopology().Queue("orders").Queue("orders").Validate() == nil {
		t.Error("a queue declared twice was accepted")
	}
	if NewTopology().Exchange("e", "topic").Exchange("e", "topic").Validate() == nil {
		t.Error("an exchange declared twice was accepted")
	}
}

func TestAnExchangeWithoutAKindIsRefused(t *testing.T) {
	if NewTopology().Exchange("events", "").Validate() == nil {
		t.Error("an exchange with no kind was accepted")
	}
}

func TestApplyRefusesAnInvalidTopologyWithoutTouchingTheBroker(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	topology := NewTopology().
		Exchange("orders-events", "topic").
		Binding("never-declared", "orders-events", "#")

	if err := topology.Apply(ctx, mq); err == nil {
		t.Fatal("an invalid topology was applied")
	}

	// And nothing was created on the way to finding out.
	if _, err := Consume(ctx, mq, "never-declared",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() }); err == nil {
		t.Error("the queue was created despite the topology being refused")
	}
}

func TestThePlanSaysWhatWouldHappen(t *testing.T) {
	actions, err := aTopology().Plan()
	if err != nil {
		t.Fatal(err)
	}

	if len(actions) != 5 {
		t.Fatalf("the plan has %d actions, want 5", len(actions))
	}
	// Exchanges before queues before bindings, which is the order a broker
	// needs and the order somebody reading the plan expects.
	if actions[0].Kind != "exchange" || actions[1].Kind != "queue" {
		t.Errorf("the plan is out of order: %v", actions)
	}

	rendered := aTopology().String()
	for _, want := range []string{"orders-events", "shipping-orders", "x-dead-letter-exchange", "order.placed"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the rendered plan does not mention %s:\n%s", want, rendered)
		}
	}
}

func TestAnInvalidTopologyRendersAsInvalidRatherThanPanicking(t *testing.T) {
	rendered := NewTopology().Queue("orders").Binding("missing", "e", "#").String()

	if !strings.Contains(rendered, "invalid") {
		t.Errorf("an invalid topology rendered as if it were fine:\n%s", rendered)
	}
}

// ---- drift -----------------------------------------------------------

func TestCheckIsQuietWhenTheBrokerAgrees(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	reports, err := aTopology().Check(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Errorf("drift reported against a broker that was just given this topology: %v", reports)
	}
}

// TestCheckNoticesAQueueThatDoesNotMatch is what the whole thing is for.
//
// A service and its broker disagreeing about a queue is the failure that shows
// up as messages going somewhere nobody is looking — a dead-letter exchange
// that was changed, a durability that was not.
func TestCheckNoticesAQueueThatDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "shipping-orders", DeadLetterTo("somewhere-else")); err != nil {
		t.Fatal(err)
	}

	reports, err := aTopology().Check(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}

	if len(reports) == 0 {
		t.Fatal("a queue with a different dead-letter exchange was reported as matching")
	}
	found := false
	for _, r := range reports {
		if r.Name == "shipping-orders" && strings.Contains(r.Reason, "x-dead-letter-exchange") {
			found = true
		}
	}
	if !found {
		t.Errorf("the drift does not name the setting that differs: %v", reports)
	}
}

func TestRedeclaringWithDifferentSettingsIsRefusedInMemoryToo(t *testing.T) {
	// The in-memory transport has to be no kinder than RabbitMQ here, or a test
	// suite would certify a deployment that fails against the real broker.
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	err := mq.DeclareQueue(ctx, "orders", Transient())

	if err == nil {
		t.Fatal("redeclaring with different settings was accepted")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("the refusal does not read like the broker's: %v", err)
	}
}

func TestRedeclaringWithTheSameSettingsIsFine(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orders", DeadLetterTo("dead")); err != nil {
		t.Fatal(err)
	}
	if err := mq.DeclareQueue(ctx, "orders", DeadLetterTo("dead")); err != nil {
		t.Fatalf("an identical redeclaration was refused: %v", err)
	}
}

// ---- dry run ---------------------------------------------------------

// TestADryRunChangesNothing is the property the mode exists for: somebody can
// read what a deployment would do to a broker before it does it.
func TestADryRunChangesNothing(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	actions, err := aTopology().ApplyWith(ctx, mq, DryRun)
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 5 {
		t.Errorf("the plan has %d actions, want 5", len(actions))
	}
	for _, a := range actions {
		if a.Kind == "queue" && !strings.Contains(a.Detail, "would create") {
			t.Errorf("queue %s reads %q, want it to be missing", a.Name, a.Detail)
		}
	}

	// Nothing was created, so consuming from a queue it named still fails.
	_, err = Consume(ctx, mq, "shipping-orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() })
	if err == nil {
		t.Error("the dry run declared the queue after all")
	}
}

func TestADryRunSaysWhichQueuesAlreadyMatch(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := aTopology().Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	actions, err := aTopology().ApplyWith(ctx, mq, DryRun)
	if err != nil {
		t.Fatal(err)
	}

	for _, a := range actions {
		if a.Kind != "queue" {
			continue
		}
		if !strings.Contains(a.Detail, "matches") {
			t.Errorf("queue %s reads %q, want it to match", a.Name, a.Detail)
		}
	}
}

func TestADryRunReportsAQueueThatDiffers(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "shipping-orders", DeadLetterTo("somewhere-else")); err != nil {
		t.Fatal(err)
	}

	actions, err := aTopology().ApplyWith(ctx, mq, DryRun)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, a := range actions {
		if a.Name == "shipping-orders" && strings.Contains(a.Detail, "differs") {
			found = true
		}
	}
	if !found {
		t.Errorf("the difference was not reported: %v", actions)
	}
}

// TestADryRunIsHonestAboutWhatItCannotSee matters more than it looks.
//
// AMQP cannot report an exchange or a binding without the management API, so a
// plan that showed them as "would create" would be guessing — and a plan that
// guesses is worse than one that says it does not know.
func TestADryRunIsHonestAboutWhatItCannotSee(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	actions, err := aTopology().ApplyWith(ctx, mq, DryRun)
	if err != nil {
		t.Fatal(err)
	}

	for _, a := range actions {
		if a.Kind == "exchange" || a.Kind == "binding" {
			if !strings.Contains(a.Detail, "unknown") {
				t.Errorf("%s %s claims to know its state: %q", a.Kind, a.Name, a.Detail)
			}
		}
	}
}

func TestApplyWithDeclareActuallyDeclares(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if _, err := aTopology().ApplyWith(ctx, mq, Declare); err != nil {
		t.Fatal(err)
	}

	sub, err := Consume(ctx, mq, "shipping-orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() })
	if err != nil {
		t.Fatalf("Declare did not declare: %v", err)
	}
	_ = sub.Close()
}

func TestApplyModeNamesItself(t *testing.T) {
	if DryRun.String() != "dry-run" || Declare.String() != "declare" {
		t.Errorf("modes read as %q and %q", DryRun, Declare)
	}
}

// ---- queue admin -----------------------------------------------------

func TestCountingAndDeletingAQueue(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	count, err := mq.MessageCount(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("a fresh queue holds %d messages", count)
	}

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	count, err = mq.MessageCount(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("after one publish the queue holds %d", count)
	}

	if err := mq.DeleteQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	exists, err := mq.QueueExists(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("the queue is still there after being deleted")
	}
}

// Cleaning up after a test should not depend on knowing what ran.
func TestDeletingAQueueThatIsNotThereIsFine(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeleteQueue(ctx, "never-existed"); err != nil {
		t.Errorf("deleting a queue that does not exist failed: %v", err)
	}
}

// ---- the queues a retry policy needs ---------------------------------

func TestATopologyDeclaresTheRungsAPolicyNeeds(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	policy := ExponentialRetry(5, 20*time.Second, 0)
	topology := NewTopology().
		Queue("orders").
		DeadLetters("orders").
		Retries("orders", policy)

	if err := topology.Apply(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// The rungs are derived from the policy rather than listed beside it. A
	// second list would be free to drift from the first, and the way that drift
	// shows up is a retry published into a queue nobody declared, at the moment
	// the service is already failing.
	want := []string{
		"orders", "orders.dlq", "orders.parked",
		"orders.retry.40s", "orders.retry.80s", "orders.retry.160s",
	}
	for _, queue := range want {
		exists, err := mq.QueueExists(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("%s was not declared", queue)
		}
	}

	// The twenty-second wait is below the threshold and waits in the consumer,
	// so it costs the broker nothing.
	if exists, _ := mq.QueueExists(ctx, "orders.retry.20s"); exists {
		t.Error("a wait shorter than the threshold was given a queue")
	}

	// And a consumer's own idea of the ladder has to agree with what was
	// declared, or the two are two lists again.
	for _, queue := range LadderFor("orders", policy).Queues() {
		if exists, _ := mq.QueueExists(ctx, queue); !exists {
			t.Errorf("the consumer would publish into %s, which the topology did not declare", queue)
		}
	}
}

func TestRetriesForTwoQueuesShareOneExchange(t *testing.T) {
	// Declaring it twice is what a Topology calls an error, and two queues with
	// long retries in one service is ordinary.
	topology := NewTopology().
		Queue("orders").
		Retries("orders", FixedRetry(3, time.Minute)).
		Queue("shipments").
		Retries("shipments", FixedRetry(3, 2*time.Minute))

	if err := topology.Validate(); err != nil {
		t.Fatalf("two queues with retries: %v", err)
	}
}

func TestAPolicyWithNoLongWaitsAddsNothing(t *testing.T) {
	plain := NewTopology().Queue("orders")
	withRetries := NewTopology().Queue("orders").Retries("orders", FixedRetry(3, time.Second))

	if plain.String() != withRetries.String() {
		t.Errorf("a short-wait policy changed the topology:\n%s\nversus\n%s",
			plain, withRetries)
	}
}

// ---- the queues a message ends up in when it cannot be handled -------

// queueIn is the declaration a topology holds for a queue, so a test can assert
// on the arguments rather than on the sentence that describes them.
func queueIn(t *testing.T, topology *Topology, name string) namedQueue {
	t.Helper()
	if err := topology.Validate(); err != nil {
		t.Fatalf("the topology is not valid: %v", err)
	}
	for _, q := range topology.queues {
		if q.Name == name {
			return q
		}
	}
	t.Fatalf("the topology does not declare %s:\n%s", name, topology)
	return namedQueue{}
}

func hasBinding(topology *Topology, queue, exchange, key string) bool {
	for _, b := range topology.bindings {
		if b.Queue == queue && b.Exchange == exchange && b.RoutingKey == key {
			return true
		}
	}
	return false
}

// The argument table is a cross-language contract rather than a preference, and
// this is the test that pins it. Two services consuming orders both declare
// orders, so a Go service that wrote different arguments here would be refused
// by the broker with PRECONDITION_FAILED — or would refuse the Python service
// that declared it first, which is the same outage seen from the other side.
func TestDeadLettersPointsTheSourceQueueAtTheSharedExchange(t *testing.T) {
	topology := NewTopology().Queue("orders").DeadLetters("orders")

	args := queueIn(t, topology, "orders").Spec.Args
	if len(args) != 3 {
		t.Fatalf("orders was declared with %v, want the two dead-letter arguments and the type",
			args)
	}
	// The type belongs to the same contract as the other two. Java has declared
	// source queues quorum since before the other libraries existed, and a queue
	// that already exists as quorum cannot be redeclared as anything else.
	if got := args[ArgQueueType]; got != string(QueueQuorum) {
		t.Errorf("%s = %v, want %q", ArgQueueType, got, QueueQuorum)
	}
	if got := args[ArgDeadLetterExchange]; got != DeadLetterExchange {
		t.Errorf("%s = %v, want %q", ArgDeadLetterExchange, got, DeadLetterExchange)
	}
	// The key, not just the exchange. A dead-lettered message keeps the routing
	// key it arrived under, so without this one it reaches acemq.dlx as
	// order.placed, matches no binding and is dropped.
	if got := args[ArgDeadLetterRoutingKey]; got != "orders.dlq" {
		t.Errorf("%s = %v, want %q", ArgDeadLetterRoutingKey, got, "orders.dlq")
	}

	// No arguments at all on either of them, which includes no x-queue-type:
	// both are classic, and classic on the wire is the absence of the argument,
	// exactly as Java's QueueType.CLASSIC produces it.
	for _, target := range []string{"orders.dlq", "orders.parked"} {
		if got := queueIn(t, topology, target).Spec.Args; len(got) != 0 {
			t.Errorf("%s was declared with %v; a dead-letter queue that dead-letters is a loop, "+
				"and it must stay classic", target, got)
		}
		if !hasBinding(topology, target, DeadLetterExchange, target) {
			t.Errorf("%s is not bound to %s on its own name:\n%s",
				target, DeadLetterExchange, topology)
		}
	}

	if !topology.hasExchange(DeadLetterExchange) {
		t.Fatalf("%s was not declared:\n%s", DeadLetterExchange, topology)
	}
	for _, e := range topology.exchanges {
		if e.Name == DeadLetterExchange && (e.Spec.Kind != "direct" || !e.Spec.Durable) {
			t.Errorf("%s was declared %s; every other library declares it direct and durable",
				DeadLetterExchange, describeExchange(e.Spec))
		}
	}
}

// A builder is written in whichever order reads best, and what reaches the
// broker cannot depend on which one somebody chose. The order the declarations
// come out in is allowed to differ — a broker does not care which queue is
// declared first — so this compares the plan as a set.
func TestDeadLettersWorksInEitherOrder(t *testing.T) {
	first := NewTopology().Queue("orders").DeadLetters("orders")
	second := NewTopology().DeadLetters("orders").Queue("orders")

	lines := func(topology *Topology) string {
		t.Helper()
		actions, err := topology.Plan()
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(actions))
		for _, a := range actions {
			out = append(out, a.String())
		}
		sort.Strings(out)
		return strings.Join(out, "\n")
	}

	if lines(first) != lines(second) {
		t.Errorf("the order of the calls changed the topology:\n%s\nversus\n%s", first, second)
	}
}

// Refused rather than resolved: either answer would be a guess about which of
// two conflicting instructions was meant, and the guess that silently wins is
// the one nobody finds out about until a message is somewhere else.
func TestSettingTheArgumentsByHandAndAskingForDeadLettersIsRefused(t *testing.T) {
	cases := map[string]*Topology{
		"the option first":       NewTopology().Queue("orders", DeadLetterTo("mine")).DeadLetters("orders"),
		"DeadLetters first":      NewTopology().DeadLetters("orders").Queue("orders", DeadLetterTo("mine")),
		"only the routing key":   NewTopology().Queue("orders", QueueArg(ArgDeadLetterRoutingKey, "elsewhere")).DeadLetters("orders"),
		"a queue somewhere else": NewTopology().Queue("shipments").Queue("orders", DeadLetterTo("mine")).DeadLetters("orders"),
	}
	for name, topology := range cases {
		err := topology.Validate()
		if err == nil {
			t.Errorf("%s: a queue that dead-letters twice was accepted:\n%s", name, topology)
			continue
		}
		if !strings.Contains(err.Error(), "pick one") {
			t.Errorf("%s: %v does not say what to do about it", name, err)
		}
	}
}

// The wiring is two arguments on the source queue's own declaration. A topology
// that asks for dead letters on a queue it does not declare has nowhere to put
// them, and leaving them off quietly is how the queue ends up disagreeing with
// the same queue declared by a service in another language.
func TestDeadLettersOnAQueueThisTopologyDoesNotDeclareIsRefused(t *testing.T) {
	err := NewTopology().DeadLetters("orders").Validate()
	if err == nil {
		t.Fatal("dead letters were accepted for a queue nothing declares")
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("%v does not name the queue", err)
	}
}

// One exchange per broker, not one per queue: declaring it twice is what
// Validate refuses, and two dead-lettered queues in one service is ordinary.
func TestTwoQueuesShareOneDeadLetterExchange(t *testing.T) {
	topology := NewTopology().
		Queue("orders").
		DeadLetters("orders").
		Queue("shipments").
		DeadLetters("shipments")

	if err := topology.Validate(); err != nil {
		t.Fatalf("two dead-lettered queues: %v", err)
	}
	declared := 0
	for _, e := range topology.exchanges {
		if e.Name == DeadLetterExchange {
			declared++
		}
	}
	if declared != 1 {
		t.Errorf("%s appears %d times in the plan, want once:\n%s",
			DeadLetterExchange, declared, topology)
	}
}

// The retry exchange and the dead-letter exchange are both shared and both
// declared once, and a queue that has retries as well as dead letters gets both
// without either standing on the other.
func TestAQueueCanHaveBothLaddersAndDeadLetters(t *testing.T) {
	topology := NewTopology().
		Queue("orders").
		DeadLetters("orders").
		Retries("orders", FixedRetry(3, time.Minute))

	args := queueIn(t, topology, "orders").Spec.Args
	if args[ArgDeadLetterExchange] != DeadLetterExchange {
		t.Errorf("the source queue dead-letters to %v, not %q",
			args[ArgDeadLetterExchange], DeadLetterExchange)
	}
	// A rung dead-letters through acemq.retry, and only a rung does. The source
	// queue's own dead letters are the end of the road, not another lap.
	rung := queueIn(t, topology, "orders.retry.1m").Spec.Args
	if rung[ArgDeadLetterExchange] != RetryExchange {
		t.Errorf("the rung dead-letters to %v, not %q", rung[ArgDeadLetterExchange], RetryExchange)
	}
	if !hasBinding(topology, "orders", RetryExchange, "orders") {
		t.Errorf("nothing brings an expired rung message home:\n%s", topology)
	}
}

// Saying the same thing twice is not saying anything wrong, and declaring
// orders.dlq twice is what Validate refuses.
func TestAskingForDeadLettersTwiceIsAskingOnce(t *testing.T) {
	once := NewTopology().Queue("orders").DeadLetters("orders")
	twice := NewTopology().Queue("orders").DeadLetters("orders").DeadLetters("orders")

	if err := twice.Validate(); err != nil {
		t.Fatalf("asking twice: %v", err)
	}
	if once.String() != twice.String() {
		t.Errorf("asking twice changed the topology:\n%s\nversus\n%s", once, twice)
	}
}

// ---- the type a queue is declared as ---------------------------------

// TestADurableQueueIsQuorum pins the default that this library, Java, .NET,
// Python and Ruby all have to share.
//
// A queue's type is compared by the broker as strictly as any other argument,
// so two services consuming orders have to agree about it or the second is
// refused with PRECONDITION_FAILED and cannot consume. Java declares source
// queues quorum and has deployments; a queue that exists as quorum cannot be
// redeclared as anything else, so quorum is the only answer the five of them
// could still agree on.
func TestADurableQueueIsQuorum(t *testing.T) {
	topology := NewTopology().Queue("orders")

	if got := queueIn(t, topology, "orders").Spec.Args[ArgQueueType]; got != string(QueueQuorum) {
		t.Errorf("orders was declared with %s=%v, want %q", ArgQueueType, got, QueueQuorum)
	}
}

// A caller who wants classic can still have it, and gets it in the form the
// other four libraries put on the wire: no x-queue-type at all, which is what
// Java's QueueType.CLASSIC sends. An argument saying "classic" would be
// equivalent to the broker and different to anything comparing two argument
// tables, including the in-memory transport here.
func TestClassicCanStillBeAskedForAndIsTheAbsenceOfTheArgument(t *testing.T) {
	topology := NewTopology().Queue("orders", OfType(QueueClassic))

	args := queueIn(t, topology, "orders").Spec.Args
	if _, set := args[ArgQueueType]; set {
		t.Errorf("a classic queue was declared with %v; classic is the absence of %s",
			args, ArgQueueType)
	}
}

// A stream is not quietly turned into a quorum queue, and neither is anything
// that set x-queue-type by hand.
func TestANamedTypeIsLeftAlone(t *testing.T) {
	stream := NewTopology().Queue("events", OfType(QueueStream))
	if got := queueIn(t, stream, "events").Spec.Args[ArgQueueType]; got != string(QueueStream) {
		t.Errorf("a stream was declared %s=%v, want %q", ArgQueueType, got, QueueStream)
	}

	byHand := NewTopology().Queue("events", QueueArg(ArgQueueType, "stream"))
	if got := queueIn(t, byHand, "events").Spec.Args[ArgQueueType]; got != "stream" {
		t.Errorf("a queue that set %s itself was declared %v, want it left alone",
			ArgQueueType, got)
	}
}

// TestAQueueTheBrokerCannotReplicateStaysClassic is the guard that keeps this
// change from breaking every reply queue in every service.
//
// RabbitMQ refuses a quorum queue that is exclusive, auto-deleting or
// transient, so a default that applied to those would not be a slower queue but
// a declaration the broker rejects — and the failure would land wherever a
// requester, a health probe or a temporary queue is created, which is start-up.
func TestAQueueTheBrokerCannotReplicateStaysClassic(t *testing.T) {
	for _, c := range []struct {
		name string
		opt  QueueOption
	}{
		{"exclusive", Exclusive()},
		{"auto-delete", AutoDelete()},
		{"transient", Transient()},
	} {
		t.Run(c.name, func(t *testing.T) {
			topology := NewTopology().Queue("replies", c.opt)
			args := queueIn(t, topology, "replies").Spec.Args
			if _, set := args[ArgQueueType]; set {
				t.Errorf("an %s queue was declared with %v; RabbitMQ allows a quorum queue to "+
					"be none of exclusive, auto-deleting or transient, so this queue would be "+
					"refused outright", c.name, args)
			}
		})
	}
}

// The rungs, the dead-letter queue and the parking lot are classic, and are so
// because they say so rather than because of what they happen to look like.
// Java declares all three QueueType.CLASSIC; a rung declared quorum here would
// be a rung a Java service could not redeclare, and a rung nothing consumes has
// no use for replication anyway.
func TestTheQueuesUnderneathASourceQueueAreClassic(t *testing.T) {
	topology := NewTopology().
		Queue("orders").
		DeadLetters("orders").
		Retries("orders", FixedRetry(3, time.Minute))

	for _, name := range []string{"orders.dlq", "orders.parked", "orders.retry.1m"} {
		args := queueIn(t, topology, name).Spec.Args
		if _, set := args[ArgQueueType]; set {
			t.Errorf("%s was declared with %v; it has to be classic, and classic is the "+
				"absence of %s", name, args, ArgQueueType)
		}
	}

	// And the source queue above them is not.
	if got := queueIn(t, topology, "orders").Spec.Args[ArgQueueType]; got != string(QueueQuorum) {
		t.Errorf("orders was declared %s=%v, want %q", ArgQueueType, got, QueueQuorum)
	}
}

// Declaring one queue by hand takes the same default as declaring it in a
// topology. Two entry points that disagreed would be a service whose queue's
// type depended on which one its author happened to reach for.
func TestDeclareQueueTakesTheSameDefaultAsATopology(t *testing.T) {
	ctx := context.Background()
	mq, err := Connect(ctx, "memory://queue-type-default")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.Close() }()

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	// The memory transport refuses a redeclaration whose arguments differ, as
	// RabbitMQ does, so the topology's own declaration of orders being accepted
	// is the two of them agreeing.
	if err := NewTopology().Queue("orders").Apply(ctx, mq); err != nil {
		t.Fatalf("a topology could not redeclare a queue DeclareQueue had made: %v", err)
	}
	if err := NewTopology().Queue("orders", OfType(QueueClassic)).Apply(ctx, mq); err == nil {
		t.Fatal("redeclaring the same queue as classic was accepted, so the default is not " +
			"being compared at all")
	}
}

// The plan names the type of every queue in words, including the classic ones,
// because a plan that mentioned the type only when it was quorum would make the
// queues that must not be quorum look like queues nobody had thought about.
func TestThePlanNamesEveryQueuesType(t *testing.T) {
	topology := NewTopology().Queue("orders").DeadLetters("orders")

	plan := topology.String()
	for _, want := range []string{
		"queue orders (durable, quorum,",
		"queue orders.dlq (durable, classic)",
		"queue orders.parked (durable, classic)",
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("the plan does not contain %q:\n%s", want, plan)
		}
	}
	// And not twice: the word is the argument, so the argument is not listed
	// again beside it.
	if strings.Contains(plan, ArgQueueType) {
		t.Errorf("the plan lists %s as well as naming the type:\n%s", ArgQueueType, plan)
	}
}
