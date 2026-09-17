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

package rabbitmq

// The bound on unconfirmed publishes, tested without a broker.
//
// Everything about it that can go wrong is arithmetic and blocking: the default
// has to match the rest of the family, a full semaphore has to refuse rather
// than wait for ever, and the refusal has to say which setting to change. None
// of that needs a connection, and a test that needed one would be skipped on
// every machine that has no broker — which is where this is most likely to be
// read.

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestTheDefaultPublishBoundIsJavas is the cross-language promise.
//
// Java's ConnectionConfig and .NET's both default to a thousand, and a Go
// library that chose its own number would make the same batch behave
// differently depending on which library sent it.
func TestTheDefaultPublishBoundIsJavas(t *testing.T) {
	if got := maxOutstanding(Config{}); got != 1000 {
		t.Errorf("the default bound is %d, want Java's 1000", got)
	}
	if got := maxOutstanding(Config{MaxOutstandingPublishes: 7}); got != 7 {
		t.Errorf("a configured bound of 7 came out as %d", got)
	}
	if got := maxOutstanding(Config{MaxOutstandingPublishes: -1}); got != 1000 {
		t.Errorf("a nonsense bound of -1 came out as %d, want the default", got)
	}
}

// TestAFullPublishBoundRefusesRatherThanWaitingForEver is the backpressure.
//
// A publisher that has filled the bound is publishing faster than the broker is
// confirming. Waiting for ever there turns a broker that has stopped confirming
// into a process that has stopped, with nothing anywhere saying why; refusing
// lets the caller shed load, slow down or fail the request.
func TestAFullPublishBoundRefusesRatherThanWaitingForEver(t *testing.T) {
	transport := &Transport{outstanding: make(chan struct{}, 2)}

	ctx := context.Background()
	if err := transport.acquire(ctx); err != nil {
		t.Fatalf("the first slot was refused: %v", err)
	}
	if err := transport.acquire(ctx); err != nil {
		t.Fatalf("the second slot was refused: %v", err)
	}

	deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := transport.acquire(deadline)
	if err == nil {
		t.Fatal("a third publish took a slot from a bound of two")
	}
	if waited := time.Since(started); waited > 5*time.Second {
		t.Errorf("the refusal took %v, so it was not bounded by the context", waited)
	}
	if !strings.Contains(err.Error(), "MaxOutstandingPublishes") {
		t.Errorf("the refusal reads %q and does not name the setting to change", err)
	}
	if !strings.Contains(err.Error(), "2 publishes") {
		t.Errorf("the refusal reads %q and does not say how many were waiting", err)
	}

	// A slot given back is a slot available again, which is what makes the
	// bound a bound rather than a budget spent once.
	transport.release()
	if err := transport.acquire(ctx); err != nil {
		t.Fatalf("a released slot was not reusable: %v", err)
	}
}

// TestAReturnIsFiledForItsOwnPublishAndNotAnother is the bookkeeping that had
// to replace putting returns back into the channel.
//
// With one publish outstanding, draining until the right return turned up and
// pushing the rest back was fine. With a batch outstanding it is not: the
// channel is bounded, so a busy publisher's put-back is a return dropped, and
// the publish it belonged to then reports a message as routed when the broker
// handed it straight back.
func TestAReturnIsFiledForItsOwnPublishAndNotAnother(t *testing.T) {
	returns := make(chan amqp.Return, 4)
	transport := &Transport{
		outstanding: make(chan struct{}, 8),
		returns:     returns,
		returned:    map[string]string{},
	}

	returns <- amqp.Return{MessageId: "a", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	returns <- amqp.Return{MessageId: "b", ReplyCode: 312, ReplyText: "NO_ROUTE"}
	returns <- amqp.Return{MessageId: "c", ReplyCode: 312, ReplyText: "NO_ROUTE"}

	// Asked for out of order, because publishers finish in whatever order the
	// broker confirms them and not the order they wrote in.
	if reason, found := transport.takeReturn("c"); !found || reason != "312 NO_ROUTE" {
		t.Errorf("c got (%q, %v), want its own return", reason, found)
	}
	if reason, found := transport.takeReturn("a"); !found || reason != "312 NO_ROUTE" {
		t.Errorf("a got (%q, %v); the return was lost draining past it", reason, found)
	}
	if reason, found := transport.takeReturn("b"); !found || reason != "312 NO_ROUTE" {
		t.Errorf("b got (%q, %v); the return was lost draining past it", reason, found)
	}

	// Taken once. A second ask is a different publish that happened to reuse an
	// identifier, and answering it with an old return would report a message as
	// unroutable that the broker took.
	if reason, found := transport.takeReturn("a"); found {
		t.Errorf("a's return came back a second time as %q", reason)
	}

	// A message the broker routed has no return, and must not be given one.
	if reason, found := transport.takeReturn("never-published"); found {
		t.Errorf("a message nobody returned got %q", reason)
	}
}

// TestUnclaimedReturnsDoNotAccumulateForEver covers the leak the map would
// otherwise be.
//
// A publisher whose context was cancelled between the write and the confirm
// never comes back to ask, so its return would sit in the map for the life of
// the process. The cap is the publish bound, because that is how many
// publishers can be waiting to ask at once.
func TestUnclaimedReturnsDoNotAccumulateForEver(t *testing.T) {
	const bound = 4
	returns := make(chan amqp.Return, 1)
	transport := &Transport{
		outstanding: make(chan struct{}, bound),
		returns:     returns,
		returned:    map[string]string{},
	}

	// Sixty-four is the floor the eviction keeps whatever the bound is, so this
	// has to go past it to see anything evicted at all.
	const written = 200
	for i := 0; i < written; i++ {
		returns <- amqp.Return{MessageId: string(rune('A'+i%26)) + "-" + time.Duration(i).String()}
		transport.takeReturn("nobody")
	}

	if held := len(transport.returned); held > 64 {
		t.Errorf("%d returns are still held after %d unclaimed ones", held, written)
	}
	if held, seen := len(transport.returned), len(transport.returnedSeen); held != seen {
		t.Errorf("the map holds %d returns and the order list %d; they have drifted apart", held, seen)
	}
}
