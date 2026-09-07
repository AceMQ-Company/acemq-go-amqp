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
	"strings"
	"time"
)

// The two exchanges this library owns rather than the caller.
//
// Both are declared direct and durable, because every binding on them matches a
// queue name exactly and a topology that vanished with the broker would leave
// queues dead-lettering into nothing. They are shared: one of each per broker,
// however many queues use them, which is why the code that adds them checks
// whether they are already there rather than declaring one per queue.
//
// The names are a cross-language contract, not a preference. Java, .NET,
// Python, Ruby and this library all name these two exchanges, and a queue
// pointed at a different one is a queue whose messages a colleague cannot find.
const (
	// RetryExchange is the exchange a rung dead-letters through on its way back
	// to the queue the message came from, bound on that queue's own name.
	//
	// This constant, and [retryReturn] beside it, are the only two places that
	// decide how an expired rung message gets home. Python and Ruby once
	// dead-lettered a rung through the default exchange instead, which routes by
	// queue name and so needs no exchange and no binding at all; that works, but
	// two libraries cannot both be right about one queue, and most of the
	// released code follows Java. All five now name this one.
	RetryExchange = "acemq.retry"

	// DeadLetterExchange is the exchange {queue}.dlq and {queue}.parked are
	// reached through, each bound on its own name.
	//
	// It is also where a source queue's own x-dead-letter-exchange points, which
	// is the argument that has to match across languages: two services consuming
	// orders declare orders, and a declaration that disagrees about this
	// argument answers the second one PRECONDITION_FAILED and leaves it unable
	// to consume at all. See [Topology.DeadLetters].
	DeadLetterExchange = "acemq.dlx"
)

// The arguments that make a queue a rung.
//
// Named rather than written out at each use, because they appear in the
// declaration, in the drift check and in the test that pins the table, and a
// typo in any one of them is a queue that quietly does something else.
const (
	// ArgMessageTTL is how long the broker holds a message before expiring it.
	ArgMessageTTL = "x-message-ttl"

	// ArgDeadLetterExchange is where an expired or rejected message is sent.
	ArgDeadLetterExchange = "x-dead-letter-exchange"

	// ArgDeadLetterRoutingKey is the key it is sent under.
	ArgDeadLetterRoutingKey = "x-dead-letter-routing-key"
)

// retryReturn is where a rung sends a message when its time is up.
//
// The exchange and the routing key are worked out together and in one place
// because they are one decision: through the default exchange the key has to be
// the queue's own name, and through a named exchange it has to be whatever that
// exchange is bound on. Split them and a change to one silently invalidates the
// other.
func retryReturn(source string) (exchange, routingKey string) {
	return RetryExchange, source
}

// RungArgs is the argument table a rung queue must be declared with.
//
// x-message-ttl and never a per-message expiration. RabbitMQ expires messages
// only from the head of a queue, so one queue holding per-message TTLs releases
// nothing while a message with a long one sits at the front: a thirty-second
// wait queued behind a ten-minute wait becomes a ten-minute wait, and the delays
// that come out bear no relation to the ones that went in. The bug only appears
// under the load that puts two different waits on the queue at once, which is
// the load nobody tests with. One queue per distinct delay is more queues and is
// the only arrangement that delivers the schedule it was given.
//
// This table is a contract rather than a preference. Two services consuming the
// same queue declare the same rung by name, so if one of them declares it with
// different arguments the second is refused with PRECONDITION_FAILED and cannot
// consume at all. It is pinned by a test for that reason.
func RungArgs(source string, delay time.Duration) map[string]any {
	exchange, routingKey := retryReturn(source)
	return map[string]any{
		ArgMessageTTL:           delay.Milliseconds(),
		ArgDeadLetterExchange:   exchange,
		ArgDeadLetterRoutingKey: routingKey,
	}
}

// Rung is one step of a retry ladder: a delay, the queue that expresses it, and
// the arguments that queue has to be declared with for it to mean anything.
type Rung struct {
	// Delay is how long a message waits on this rung.
	Delay time.Duration

	// Queue is the rung's name, which is the source queue plus the delay.
	Queue string

	// Args is what the queue must be declared with. See [RungArgs].
	Args map[string]any
}

func (r Rung) String() string { return fmt.Sprintf("%s (ttl %s)", r.Queue, r.Delay) }

// RetryLadder is the set of queues a policy's long waits need, so that the wait
// is the broker's and not this process's.
//
// A consumer that sleeps through a five-minute backoff is holding an
// unacknowledged message. Restart it — a deploy, a crash, an autoscaler — and
// the broker redelivers at once, so a five-minute policy delivers in none. That
// is a correctness bug rather than a throughput one, and it is why delays at or
// past a threshold are handed to the broker instead: the message is published
// into a rung whose x-message-ttl is the delay and whose dead-letter target is
// the queue it came from, and the broker returns it when the time is up.
// Nothing consumes a rung; the time-to-live is the only thing that ever takes a
// message out of one.
//
// For orders.new with delays of 1s, 30s and 60s and the default threshold:
//
//	orders.new.retry.30s   ttl 30s  -> orders.new
//	orders.new.retry.1m    ttl 60s  -> orders.new
//
// The one-second delay gets no queue. Below the threshold the wait happens in
// the consumer, where a second lost to a restart is a second, and the broker is
// spared a queue per rung of a schedule that mostly runs in the time it takes to
// notice.
//
// The rungs are exactly the entries of [RetryPolicy.Schedule] that reach the
// threshold, which is a finite list known before anything is published — which
// is what makes them declarable up front, by a [Topology] somebody reviewed,
// rather than conjured by a consumer at the moment it first fails.
type RetryLadder struct {
	// Source is the queue being consumed.
	Source string

	// Threshold is the delay at which waiting moves to the broker.
	Threshold time.Duration

	// Rungs are the queues, shortest delay first.
	Rungs []Rung
}

// LadderFor works out the ladder a policy needs, touching no broker.
func LadderFor(source string, p RetryPolicy) RetryLadder {
	ladder := RetryLadder{Source: source, Threshold: p.BrokerWaitThreshold}

	// Keyed by name rather than by delay. Two delays inside the same second
	// render to the same name, and a second queue by that name with a different
	// time-to-live is not a second rung — it is a PRECONDITION_FAILED at
	// declaration time.
	seen := map[string]bool{}
	for _, delay := range p.BrokerRungs() {
		name := RetryQueue(source, delay)
		if seen[name] {
			continue
		}
		seen[name] = true
		ladder.Rungs = append(ladder.Rungs, Rung{
			Delay: delay,
			Queue: name,
			Args:  RungArgs(source, delay),
		})
	}
	return ladder
}

// Empty reports whether this policy needs no rungs at all, which is the common
// case: a schedule that runs in seconds waits in the consumer and costs the
// broker nothing.
func (l RetryLadder) Empty() bool { return len(l.Rungs) == 0 }

// Queues are the rung names, in schedule order.
func (l RetryLadder) Queues() []string {
	names := make([]string, 0, len(l.Rungs))
	for _, rung := range l.Rungs {
		names = append(names, rung.Queue)
	}
	return names
}

// RungFor is the queue a delay belongs in, and whether there is one.
//
// False is the answer for anything below the threshold, and it is an answer the
// caller acts on rather than a failure: waiting in the consumer is the other
// half of the design, not a fallback.
//
// A delay that is not exactly a rung is rounded up to the next one, which cannot
// happen for a delay this ladder's own policy produced but can for one a caller
// worked out some other way. Up rather than down, because waiting slightly too
// long is harmless and retrying early defeats the backoff.
func (l RetryLadder) RungFor(delay time.Duration) (string, bool) {
	if l.Empty() || delay < l.Threshold {
		return "", false
	}

	best := ""
	var bestDelay time.Duration
	longest := l.Rungs[0]
	for _, rung := range l.Rungs {
		if rung.Delay > longest.Delay {
			longest = rung
		}
		if rung.Delay >= delay && (best == "" || rung.Delay < bestDelay) {
			best, bestDelay = rung.Queue, rung.Delay
		}
	}
	if best == "" {
		// Longer than every rung: the longest available is the closest this
		// ladder can come, and it is closer than not waiting at all.
		return longest.Queue, true
	}
	return best, true
}

// Declare creates the rungs, and whatever brings an expired message home.
//
// A [Topology] declares the same thing up front, which is where it belongs: a
// queue that appears in a plan somebody reviewed. This exists because the cost
// of a rung being absent is silent — a publish into a queue nobody declared is
// dropped by the broker — and because a test or a tool sometimes needs the
// ladder without the rest of a topology. Declaring is idempotent, and a
// duplicate declaration is a great deal cheaper than a lost message.
//
// The source queue is not declared here. It is the caller's, it usually has
// arguments of its own, and creating it as a side effect of setting up its
// retries would be this library guessing at a queue somebody else owns.
func (l RetryLadder) Declare(ctx context.Context, conn *Conn) error {
	if l.Empty() {
		return nil
	}

	exchange, routingKey := retryReturn(l.Source)
	if exchange != "" {
		if err := conn.DeclareExchange(ctx, exchange, "direct"); err != nil {
			return fmt.Errorf("acemq: cannot declare the retry exchange %q: %w", exchange, err)
		}
	}

	for _, rung := range l.Rungs {
		opts := make([]QueueOption, 0, len(rung.Args))
		for name, value := range rung.Args {
			opts = append(opts, QueueArg(name, value))
		}
		if err := conn.DeclareQueue(ctx, rung.Queue, opts...); err != nil {
			return fmt.Errorf("acemq: cannot declare the retry queue %q: %w", rung.Queue, err)
		}
	}

	if exchange != "" {
		// One binding brings every expired message back to the queue it came
		// from. Through the default exchange there is nothing to bind: every
		// queue is reachable by its own name from the moment it exists.
		if err := conn.Bind(ctx, l.Source, exchange, routingKey); err != nil {
			return fmt.Errorf("acemq: cannot bind %q to %q: %w", l.Source, exchange, err)
		}
	}
	return nil
}

// String renders the ladder as the line an operator would want in a deployment
// log, and as the thing to compare against another language's by eye.
func (l RetryLadder) String() string {
	if l.Empty() {
		return "no retry rungs for " + l.Source
	}
	parts := make([]string, 0, len(l.Rungs))
	for _, rung := range l.Rungs {
		parts = append(parts, rung.String())
	}
	return "retry rungs for " + l.Source + ": " + strings.Join(parts, ", ")
}
