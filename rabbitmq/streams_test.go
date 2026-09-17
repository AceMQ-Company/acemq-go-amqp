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

package rabbitmq_test

// patterns/streams.go against a real stream.
//
// The in-memory transport refuses streams on purpose, so until these existed the
// file had no test that ran a message through it at all: the retention
// arguments, the offset argument and the checkpoint path were all covered by the
// documentation and nothing else. patterns/streams_wire_test.go proves what the
// library sends; this proves RabbitMQ agrees.
//
// Streams need RabbitMQ 3.9 or later. Point ACEMQ_TEST_AMQP_URL at one, as for
// every other test in this package.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type LogEntry struct {
	Line string `json:"line"`
}

// streamWith declares a stream and fills it, returning the stream's name.
//
// Retention is set on everything declared here. A stream with none means "until
// the disk is full", and a full disk is a broker-wide alarm rather than a
// problem confined to one test.
func streamWith(t *testing.T, mq *acemq.Conn, lines int) string {
	t.Helper()

	stream := queueName(t)
	removeAtEnd(t, []string{stream}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := patterns.DeclareStream(ctx, mq, stream, patterns.StreamRetention{
		MaxAge:   time.Hour,
		MaxBytes: 64 << 20,
	})
	if err != nil {
		t.Fatalf("DeclareStream: %v", err)
	}

	// One at a time, not SendAll. A batch is written from a goroutine per
	// message, so the order they reach the log is whichever won the channel
	// lock — and a test about offsets needs the log to be in a known order.
	pub := acemq.NewPublisher[LogEntry](mq, "", stream)
	for i := 0; i < lines; i++ {
		if err := pub.Send(ctx, LogEntry{Line: fmt.Sprintf("line-%d", i)}); err != nil {
			t.Fatalf("filling the stream at %d: %v", i, err)
		}
	}
	return stream
}

// collector gathers what a reader saw, with its offsets.
type collector struct {
	mu      sync.Mutex
	lines   []string
	offsets []uint64
	done    chan struct{}
	want    int
	closed  bool
}

func newCollector(want int) *collector {
	return &collector{done: make(chan struct{}), want: want}
}

func (c *collector) handle(_ context.Context, m acemq.Message[LogEntry]) acemq.Ack {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lines = append(c.lines, m.Payload.Line)
	if offset, ok := patterns.StreamOffsetOf(m.Envelope); ok {
		c.offsets = append(c.offsets, offset)
	}
	if len(c.lines) >= c.want && !c.closed {
		c.closed = true
		close(c.done)
	}
	return acemq.Accept()
}

func (c *collector) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(30 * time.Second):
		c.mu.Lock()
		got := len(c.lines)
		c.mu.Unlock()
		t.Fatalf("timed out waiting for %s: %d of %d messages arrived", what, got, c.want)
	}
}

func (c *collector) seen() ([]string, []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...), append([]uint64(nil), c.offsets...)
}

// TestAStreamIsDeclaredAsTheBrokerUnderstandsIt is the declaration against a
// broker rather than against a recording transport.
//
// The broker is the only authority on whether these argument names and value
// shapes are right, and it answers by refusing a redeclaration that does not
// match. Declaring the same stream a second time with the same retention has to
// be accepted; declaring it as a classic queue has to be refused.
func TestAStreamIsDeclaredAsTheBrokerUnderstandsIt(t *testing.T) {
	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := queueName(t)
	removeAtEnd(t, []string{stream}, nil)

	retention := patterns.StreamRetention{
		MaxAge:       time.Hour,
		MaxBytes:     64 << 20,
		SegmentBytes: 16 << 20,
	}
	if err := patterns.DeclareStream(ctx, mq, stream, retention); err != nil {
		t.Fatalf("DeclareStream: %v", err)
	}

	// Idempotent: the same declaration twice is how every service in a fleet
	// starts, and a mismatch here would be PRECONDITION_FAILED on the second.
	if err := patterns.DeclareStream(ctx, mq, stream, retention); err != nil {
		t.Fatalf("declaring the same stream again: %v", err)
	}

	// And it really is a stream, which is only provable by asking for something
	// a stream cannot be.
	err := declaredElsewhere(t, stream, amqp091.Table{"x-queue-type": "classic"})
	if err == nil {
		t.Fatal("the broker accepted the stream as a classic queue, so it is not a stream")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Errorf("redeclaring the stream as classic failed with %v, want PRECONDITION_FAILED", err)
	}
}

// TestReadingFromTheFirstOffsetReadsTheHistory is what a projection does, and
// the difference from a queue that everything else follows from: the messages
// were published before the reader existed and it still sees all of them.
func TestReadingFromTheFirstOffsetReadsTheHistory(t *testing.T) {
	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const lines = 30
	stream := streamWith(t, mq, lines)

	seen := newCollector(lines)
	sub, err := patterns.ReadStream(ctx, mq, stream, seen.handle,
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100, Name: "history"})
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer func() { _ = sub.Close() }()

	seen.wait(t, "the history")
	lines2, offsets := seen.seen()

	for i := 0; i < lines; i++ {
		if lines2[i] != fmt.Sprintf("line-%d", i) {
			t.Fatalf("message %d is %q; a stream read from the first offset is out of order", i, lines2[i])
		}
	}

	// Every delivery carries an offset, and they only go up. This is the number
	// a checkpoint is made of, so a stream that stopped stamping it would break
	// resuming and nothing else would notice.
	if len(offsets) != lines {
		t.Fatalf("%d of %d deliveries carried an x-stream-offset", len(offsets), lines)
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Fatalf("offset %d (%d) is not past offset %d (%d)",
				i, offsets[i], i-1, offsets[i-1])
		}
	}
}

// TestTwoReadersOfOneStreamBothSeeEverything is the other half of what a stream
// is, and the reason it is the wrong shape for distributing work.
//
// Two consumers on one queue compete: each message goes to one of them. Two
// consumers on one stream do not: reading removes nothing, so both see all of
// it. A port that quietly gave a stream queue semantics would pass every other
// test in this file and fail this one.
func TestTwoReadersOfOneStreamBothSeeEverything(t *testing.T) {
	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const lines = 20
	stream := streamWith(t, mq, lines)

	first := newCollector(lines)
	firstSub, err := patterns.ReadStream(ctx, mq, stream, first.handle,
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 50, Name: "reader-a"})
	if err != nil {
		t.Fatalf("first reader: %v", err)
	}
	defer func() { _ = firstSub.Close() }()

	second := newCollector(lines)
	secondSub, err := patterns.ReadStream(ctx, mq, stream, second.handle,
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 50, Name: "reader-b"})
	if err != nil {
		t.Fatalf("second reader: %v", err)
	}
	defer func() { _ = secondSub.Close() }()

	first.wait(t, "the first reader")
	second.wait(t, "the second reader")

	a, _ := first.seen()
	b, _ := second.seen()
	if len(a) != lines || len(b) != lines {
		t.Fatalf("the readers saw %d and %d messages; a stream has no competing consumers",
			len(a), len(b))
	}
	for i := 0; i < lines; i++ {
		if a[i] != b[i] {
			t.Fatalf("at %d one reader saw %q and the other %q", i, a[i], b[i])
		}
	}
}

// TestAReaderResumesFromTheOffsetItRecorded is the checkpoint path end to end.
//
// Nothing about acknowledging a stream message records a position the broker
// will give back, and nothing about naming a consumer does either. Resuming is
// the reader's own job: record x-stream-offset as you go, start the next run at
// one past the last one you kept. This reads half the stream, stops, and comes
// back to exactly the rest of it — no gap and no repeat.
func TestAReaderResumesFromTheOffsetItRecorded(t *testing.T) {
	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const lines = 20
	const half = lines / 2
	stream := streamWith(t, mq, lines)

	firstRun := newCollector(half)
	sub, err := patterns.ReadStream(ctx, mq, stream, firstRun.handle,
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 5, Name: "resuming"})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	firstRun.wait(t, "the first half")
	if err := sub.Close(); err != nil {
		t.Fatalf("closing the first run: %v", err)
	}

	before, offsets := firstRun.seen()
	if len(offsets) == 0 {
		t.Fatal("the first run recorded no offsets, so there is nothing to resume from")
	}
	// The prefetch means more may have arrived than the collector waited for;
	// the checkpoint is the last one it actually handled.
	checkpoint := offsets[len(offsets)-1]
	handled := len(before)

	secondRun := newCollector(lines - handled)
	resumed, err := patterns.ReadStream(ctx, mq, stream, secondRun.handle,
		patterns.StreamOptions{
			Offset:   patterns.FromOffset(checkpoint + 1),
			Prefetch: 100,
			Name:     "resuming",
		})
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	defer func() { _ = resumed.Close() }()

	secondRun.wait(t, "the rest of the stream")
	after, _ := secondRun.seen()

	// No gap: the first message of the second run is the one after the last of
	// the first. No repeat: it is not the same one again.
	if after[0] != fmt.Sprintf("line-%d", handled) {
		t.Fatalf("the resumed run began at %q after handling %d messages; want line-%d",
			after[0], handled, handled)
	}
	for i, line := range after[:lines-handled] {
		if want := fmt.Sprintf("line-%d", handled+i); line != want {
			t.Fatalf("the resumed run read %q at %d, want %q", line, i, want)
		}
	}
}

// TestFromNextSkipsWhatIsAlreadyThere is the default, and the trap the page
// warns about: a projection built without FromFirst silently skips its own
// history and looks perfectly healthy while being wrong.
func TestFromNextSkipsWhatIsAlreadyThere(t *testing.T) {
	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream := streamWith(t, mq, 10)

	seen := newCollector(1)
	sub, err := patterns.ReadStream(ctx, mq, stream, seen.handle,
		patterns.StreamOptions{Offset: patterns.FromNext(), Prefetch: 10, Name: "live"})
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Nothing should arrive from the ten already in the stream. A moment is
	// given for anything that would have.
	time.Sleep(500 * time.Millisecond)
	if lines, _ := seen.seen(); len(lines) != 0 {
		t.Fatalf("a FromNext reader saw %d messages published before it started", len(lines))
	}

	pub := acemq.NewPublisher[LogEntry](mq, "", stream)
	if err := pub.Send(ctx, LogEntry{Line: "after"}); err != nil {
		t.Fatalf("publishing after the reader started: %v", err)
	}

	seen.wait(t, "the message published after the reader started")
	lines, _ := seen.seen()
	if lines[0] != "after" {
		t.Errorf("a FromNext reader's first message was %q", lines[0])
	}
}

// TestARetryOnARealStreamParksInsteadOfAppending is the refusal against a
// broker, which is the only place the thing it prevents can actually happen.
//
// Without the refusal the message would be republished onto the stream: a second
// copy at a new offset, visible to every consumer and to every replay for ever.
// The assertion is both halves — the message is in {stream}.parked, and the
// stream is exactly as long as it was.
func TestARetryOnARealStreamParksInsteadOfAppending(t *testing.T) {
	mq := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream := streamWith(t, mq, 1)

	handled := make(chan struct{}, 1)
	sub, err := patterns.ReadStream(ctx, mq, stream,
		func(context.Context, acemq.Message[LogEntry]) acemq.Ack {
			select {
			case handled <- struct{}{}:
			default:
			}
			return acemq.Retry(errors.New("the projection store is down"))
		},
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 1, Name: "retrying"})
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer func() { _ = sub.Close() }()

	select {
	case <-handled:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler was never called")
	}

	// The parked message is what the handler's retry became.
	deadline := time.Now().Add(20 * time.Second)
	var parked int64
	for time.Now().Before(deadline) {
		parked, err = mq.MessageCount(ctx, acemq.ParkedQueue(stream))
		if err == nil && parked > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if parked != 1 {
		t.Fatalf("%d messages reached %s, want 1", parked, acemq.ParkedQueue(stream))
	}

	message, found, err := mq.Pull(ctx, acemq.ParkedQueue(stream))
	if err != nil || !found {
		t.Fatalf("cannot read the parked message: found=%v err=%v", found, err)
	}
	_ = message.Ack()

	// The reason travels as an envelope field, so a consumer of the parked queue
	// reads it back through the API rather than knowing the wire header name.
	reason := message.Envelope.Error
	for _, want := range []string{"acemq.Retry", "acemq.Park", "x-stream-offset"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the parked message's reason does not mention %q:\n%s", want, reason)
		}
	}

	// And nothing was appended to the stream, which is the whole point. Counted
	// by reading the log rather than by asking the broker how deep the queue is:
	// a stream is not emptied by being read, so its depth is not a count of what
	// is in it. A second copy would show up here as a second delivery.
	_ = sub.Close()

	replay := newCollector(1)
	again, err := patterns.ReadStream(ctx, mq, stream, replay.handle,
		patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 10, Name: "counting"})
	if err != nil {
		t.Fatalf("re-reading the stream: %v", err)
	}
	defer func() { _ = again.Close() }()

	replay.wait(t, "the stream to be re-read")
	time.Sleep(500 * time.Millisecond)

	if lines, _ := replay.seen(); len(lines) != 1 {
		t.Errorf("re-reading the stream found %d messages, want the 1 it started with; "+
			"a refused retry appended a copy after all", len(lines))
	}
}
