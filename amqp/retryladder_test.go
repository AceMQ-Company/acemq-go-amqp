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
	"testing"
	"time"
)

// TestARungIsDeclaredWithExactlyThreeArguments is the test that makes the
// argument table hard to change by accident.
//
// It is contract rather than preference. Two services consuming orders.new both
// declare orders.new.retry.1m, by name, from their own copy of this library or
// from another language's. If one of them declares it with a different
// x-message-ttl, a different dead-letter target, or a fourth argument the other
// does not set, the broker answers the second service PRECONDITION_FAILED and it
// cannot consume at all — not a retry that behaves oddly, a consumer that will
// not start.
//
// The dead-letter exchange is read from [RetryExchange] rather than written out,
// because which exchange a rung expires through is not yet settled across the
// five libraries: Java names acemq.retry and binds each source queue to it,
// Python and Ruby use the default exchange, which routes by queue name and needs
// no binding. This library follows Java for now. When that is decided, the
// constant changes and this test goes on pinning whatever it says — what it is
// really asserting is that there are exactly these three arguments, that the TTL
// is on the queue and not on the message, and that the routing key names the
// queue the message came from.
func TestARungIsDeclaredWithExactlyThreeArguments(t *testing.T) {
	args := RungArgs("orders.new", 90*time.Second)

	want := map[string]any{
		"x-message-ttl":             int64(90_000),
		"x-dead-letter-exchange":    RetryExchange,
		"x-dead-letter-routing-key": "orders.new",
	}

	if len(args) != len(want) {
		t.Fatalf("a rung was declared with %d arguments, want exactly %d: %v",
			len(args), len(want), args)
	}
	for name, value := range want {
		got, present := args[name]
		if !present {
			t.Errorf("a rung was declared without %s", name)
			continue
		}
		if got != value {
			t.Errorf("%s = %v (%T), want %v (%T)", name, got, got, value, value)
		}
	}

	// The names are constants for a reason: they appear in the declaration, in
	// the drift check and here, and a typo in any one of them is a queue that
	// quietly does something else.
	if _, present := args[ArgMessageTTL]; !present {
		t.Errorf("ArgMessageTTL is %q, which is not what a rung is declared with", ArgMessageTTL)
	}

	// Never a per-message TTL. RabbitMQ expires messages only from the head of a
	// queue, so one queue of per-message TTLs lets a long wait at the front hold
	// back every short one behind it.
	if _, present := args["x-expires"]; present {
		t.Error("a rung was given x-expires, which expires the queue rather than the message")
	}
}

// TestTheRungDeclarationIsPrintedForComparison writes the table out so it can be
// put beside the Python and Ruby libraries' by eye.
//
//	go test ./amqp/ -run TestTheRungDeclarationIsPrintedForComparison -v
func TestTheRungDeclarationIsPrintedForComparison(t *testing.T) {
	policy := ExponentialRetry(6, 10*time.Second, 0)
	ladder := LadderFor("orders.new", policy)

	t.Logf("policy: %s", policy)
	t.Logf("schedule: %v", policy.Schedule())
	t.Logf("threshold: %s", ladder.Threshold)
	t.Logf("retry exchange: %q", RetryExchange)
	for _, rung := range ladder.Rungs {
		names := make([]string, 0, len(rung.Args))
		for name := range rung.Args {
			names = append(names, name)
		}
		sort.Strings(names)

		pairs := make([]string, 0, len(names))
		for _, name := range names {
			pairs = append(pairs, fmt.Sprintf("%s=%#v", name, rung.Args[name]))
		}
		t.Logf("declare queue %s durable, %s", rung.Queue, strings.Join(pairs, ", "))
	}
	if RetryExchange != "" {
		t.Logf("declare exchange %s direct, durable", RetryExchange)
		t.Logf("bind %s to %s on %s", ladder.Source, RetryExchange, ladder.Source)
	}

	if ladder.Empty() {
		t.Fatal("a policy with delays past the threshold produced no rungs to print")
	}
}

func TestARetryQueueIsNamedForItsDelay(t *testing.T) {
	// The wait is fixed at declaration by x-message-ttl, so a policy with four
	// different waits needs four queues, and the name is how an operator tells
	// them apart. The same strings the Python and Ruby libraries produce.
	cases := []struct {
		delay time.Duration
		want  string
	}{
		{30 * time.Second, "orders.new.retry.30s"},
		{5 * time.Minute, "orders.new.retry.5m"},
		{2 * time.Hour, "orders.new.retry.2h"},
		{90 * time.Second, "orders.new.retry.90s"},
	}
	for _, c := range cases {
		if got := RetryQueue("orders.new", c.delay); got != c.want {
			t.Errorf("RetryQueue(%s) = %q, want %q", c.delay, got, c.want)
		}
	}
}

func TestWhereAMessageGoesWhenItCannotBeHandled(t *testing.T) {
	// Convention rather than protocol, which is why it has to be identical
	// everywhere: an operator looking for the dead letters of orders.new should
	// not have to know which language gave up on them.
	if got := DeadLetterQueue("orders.new"); got != "orders.new.dlq" {
		t.Errorf("DeadLetterQueue = %q", got)
	}
	if got := ParkedQueue("orders.new"); got != "orders.new.parked" {
		t.Errorf("ParkedQueue = %q", got)
	}
}

func TestTheLadderIsTheScheduleAboveTheThreshold(t *testing.T) {
	ladder := LadderFor("orders.new", ExponentialRetry(6, 10*time.Second, 0))

	want := []string{
		"orders.new.retry.40s",
		"orders.new.retry.80s",
		"orders.new.retry.160s",
	}
	got := ladder.Queues()
	if len(got) != len(want) {
		t.Fatalf("Queues() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Queues()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// A fixed policy that waits a minute three times needs one queue, not three:
	// a second queue by the same name with the same TTL is the same queue, and
	// with a different one it is a refusal.
	if got := LadderFor("orders.new", FixedRetry(4, time.Minute)).Queues(); len(got) != 1 {
		t.Errorf("a fixed policy produced %v, want one rung", got)
	}
	if !LadderFor("orders.new", FixedRetry(4, time.Second)).Empty() {
		t.Error("a schedule that runs in seconds asked the broker for queues")
	}
}

func TestARungIsChosenByRoundingUp(t *testing.T) {
	ladder := LadderFor("orders.new", ExponentialRetry(6, 10*time.Second, 0))

	// Waiting slightly too long is harmless; retrying early defeats the backoff.
	if got, ok := ladder.RungFor(50 * time.Second); !ok || got != "orders.new.retry.80s" {
		t.Errorf("RungFor(50s) = %q, %v; want the eighty-second rung", got, ok)
	}
	if got, ok := ladder.RungFor(40 * time.Second); !ok || got != "orders.new.retry.40s" {
		t.Errorf("RungFor(40s) = %q, %v; want its own rung", got, ok)
	}
	// Below the threshold there is no rung, and that is an answer rather than a
	// failure: the consumer does the waiting.
	if _, ok := ladder.RungFor(time.Second); ok {
		t.Error("a one-second wait was given a rung")
	}
	// Longer than the ladder goes: the longest rung is the closest it can come.
	if got, ok := ladder.RungFor(time.Hour); !ok || got != "orders.new.retry.160s" {
		t.Errorf("RungFor(1h) = %q, %v; want the longest rung", got, ok)
	}
}

func TestDeclaringALadderCreatesTheRungsAndTheirWayHome(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	ladder := LadderFor("orders", FixedRetry(3, time.Minute))
	if err := ladder.Declare(ctx, mq); err != nil {
		t.Fatal(err)
	}

	exists, err := mq.QueueExists(ctx, "orders.retry.1m")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("the rung was not declared")
	}

	// Declaring twice is how AMQP is meant to be used, and a rung declared a
	// second time with the same arguments has to be accepted or a second service
	// on the same queue cannot start.
	if err := ladder.Declare(ctx, mq); err != nil {
		t.Errorf("declaring the same ladder twice was refused: %v", err)
	}

	// And a rung declared with a different TTL is refused, which is the failure
	// the pinned argument table exists to prevent.
	err = mq.DeclareQueue(ctx, "orders.retry.1m", QueueArg(ArgMessageTTL, int64(1)))
	if err == nil {
		t.Error("a rung was redeclared with a different time-to-live without complaint")
	}
}

func TestALadderWithNoRungsDeclaresNothing(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	if err := LadderFor("orders", FixedRetry(3, time.Second)).Declare(ctx, mq); err != nil {
		t.Fatal(err)
	}

	// Not even the exchange: a service whose waits are all short should cost the
	// broker nothing at all.
	if RetryExchange != "" {
		exists, err := mq.QueueExists(ctx, "orders.retry.1s")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Error("a short-wait policy declared a rung queue")
		}
	}
}
