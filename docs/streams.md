# Streams

A stream is an append-only log that the broker keeps until retention removes it.
Reading one does not empty it: every consumer holds its own position, the same
message can be read again tomorrow by somebody else, and a new consumer can start
from the beginning.

```go
import "github.com/AceMQ-Company/acemq-go-amqp/patterns"

err := patterns.DeclareStream(ctx, mq, "orders.log", patterns.StreamRetention{
	MaxAge:   7 * 24 * time.Hour,
	MaxBytes: 10 << 30,
})

sub, err := patterns.ReadStream(ctx, mq, "orders.log",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		return projection.Apply(ctx, m.Payload)
	},
	patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
defer sub.Close()
```

That sounds like a queue with better retention. It is not, and every difference
that matters is silent rather than loud. The rest of this page is mostly those
differences.

## It needs a real broker

```go
sub, err := patterns.ReadStream(ctx, mq, "orders.log", handle, opts)
// acemq: this transport does not support streams; they are a RabbitMQ queue
// type and the in-memory transport has no equivalent
```

`ReadStream` asks the transport before it does anything, and `memory://` says no.
That is deliberate: falling back to an ordinary queue would lose replay, retention
and every consumer's independent position, which are the three reasons to want a
stream in the first place, and it would do it quietly.

`patterns.DeclareStream` against `memory://` does **not** fail — it is
`DeclareQueue` with `x-queue-type: stream`, and the in-memory transport records
arguments without interpreting them. So a test can build the topology and only
discovers the gap when it tries to read. **Stream tests need Docker.** See
[testing without a broker](testing.md) for what `memory://` does and does not do.

Streams are RabbitMQ 3.9 or later.

## Declaring one

```go
err := patterns.DeclareStream(ctx, mq, "orders.log", patterns.StreamRetention{
	MaxAge:       7 * 24 * time.Hour,
	MaxBytes:     10 << 30,
	SegmentBytes: 100 << 20,
})
```

| Field | Argument on the wire | |
|---|---|---|
| `MaxAge` | `x-max-age` | discards messages older than this |
| `MaxBytes` | `x-max-length-bytes` | discards the oldest once the stream exceeds this |
| `SegmentBytes` | `x-stream-max-segment-size-bytes` | how large each file on disk gets |

A `StreamRetention{}` with nothing set is legal and is **almost always a
mistake**. Unbounded retention on a stream means "until the disk is full", and a
full disk is a broker-wide alarm that blocks every publisher on the node, not a
problem confined to this stream. Set at least one of `MaxAge` and `MaxBytes` on
anything that runs for long.

`MaxAge` is rendered the way RabbitMQ wants it — a number with a unit suffix
rather than a count of anything. Seven days becomes `7D`, ninety minutes becomes
`90m`, and the largest exact unit is chosen, so `24 * time.Hour` is `1D` and not
`24h`. You do not have to know that to use it; you do have to know it if you are
comparing arguments against a stream declared by hand.

### Segment size is the granularity of everything else

Retention happens a whole segment at a time: nothing is discarded until an entire
segment can be. A stream told to keep an hour, in segments large enough to hold a
day, keeps a day.

**`SegmentBytes` is absent unless you ask**, and that is not tidiness. The broker
has its own default, tuned for the broker's storage rather than for any
particular stream — and a queue redeclared with an argument the first declaration
did not carry is answered with `PRECONDITION_FAILED` rather than adjusted. A
default invented here would break every stream first declared by a Java, .NET,
Python or Ruby service. All five leave it out the same way, under the same
argument name.

The ordinary topology declares one too, for a service that builds everything in
one place:

```go
err := acemq.NewTopology().
	Queue("orders.log",
		acemq.OfType(acemq.QueueStream),
		acemq.QueueArg("x-max-age", "7D")).
	Apply(ctx, mq)
```

`acemq.OfType(acemq.QueueStream)` forces durable and clears exclusive and
auto-delete, because RabbitMQ refuses a stream that is any of those and the error
it gives does not mention streams. `DeclareStream` is that call with the retention
arguments spelled in Go types instead of broker strings.

## Where to start reading

```go
patterns.FromFirst()                              // everything the stream still holds
patterns.FromLast()                               // the last chunk, roughly "the recent past"
patterns.FromNext()                               // only what is published from now on
patterns.FromOffset(41337)                        // an exact position
patterns.FromTimestamp(time.Now().Add(-time.Hour))
```

The offset is a **consumer** setting and not a queue setting. Two readers of one
stream sit in different places, so it could not belong to the queue.

A `StreamOptions` with no `Offset` reads from `FromNext()`. That is right for a
new consumer joining a live system and wrong for a projection: one built without
`FromFirst()` silently skips its own history and looks perfectly healthy while
being wrong. **State the position rather than inheriting it.**

`FromFirst` means the oldest message the stream *still holds*, which is not
necessarily the first one ever written — retention has been running the whole
time. A projection rebuilt from `FromFirst` against a stream with a seven-day
`MaxAge` is a projection of the last seven days.

`FromLast` is a position in the log's chunking rather than an exact message, so
it means "the recent past" and not "one message back". Use `FromOffset` when you
need a number to be honoured exactly.

## Prefetch

```go
patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100}
```

RabbitMQ refuses a stream consumer without a prefetch, and the error it gives
does not explain why, so `ReadStream` supplies one when `Prefetch` is zero or
negative.

**The default here is 10.** Java and .NET both default to 100. Ten is a
conservative number for a handler doing real work per message and a slow one for
a projection reading a large history — which is the ordinary reason to open a
stream from `FromFirst`. Set it deliberately; treat the default as a floor rather
than a recommendation.

## Naming the consumer

```go
patterns.StreamOptions{Name: "projection-a", Offset: patterns.FromNext(), Prefetch: 100}
```

`Name` becomes the consumer tag, which is what appears in the management
interface when somebody is working out who is reading a stream and how far behind
they are. It is not a group identifier and it is not a checkpoint: nothing about
naming a consumer makes the broker remember where it got to. See below.

## Nothing remembers your position

A reader starts where you tell it to, every time it starts. `ReadStream` always
puts an explicit `x-stream-offset` on the wire — `StreamOptions{}` with no
`Offset` sends `next`, rather than sending nothing and letting the broker decide
— so **where a run begins is `StreamOptions.Offset` and nothing else**. A
restarted reader on `FromFirst` reprocesses everything; one on `FromNext` misses
whatever arrived while it was down. Neither carries on where the last run
stopped, however the messages were settled. The Java and .NET libraries behave
the same way; this is what a stream is, not a gap in the port.

Checkpointing is therefore yours. RabbitMQ stamps every stream delivery with an
`x-stream-offset` header, and because that name is outside the reserved
`x-acemq-` namespace this library leaves it on the envelope rather than stripping
it:

```go
sub, err := patterns.ReadStream(ctx, mq, "orders.log",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		if err := projection.Apply(ctx, m.Payload); err != nil {
			return acemq.Retry(err)
		}
		if offset, ok := streamOffset(m.Envelope); ok {
			if err := checkpoints.Save(ctx, "projection-a", offset); err != nil {
				return acemq.Retry(err)
			}
		}
		return acemq.Accept()
	},
	patterns.StreamOptions{
		Offset:   patterns.FromOffset(resume + 1),
		Prefetch: 100,
		Name:     "projection-a",
	})

// The broker writes the offset as an integer, and which width it arrives as
// depends on the client. Read it tolerantly rather than asserting one type.
func streamOffset(env acemq.Envelope) (uint64, bool) {
	switch v := env.Headers["x-stream-offset"].(type) {
	case int64:
		return uint64(v), true
	case int32:
		return uint64(v), true
	case int:
		return uint64(v), true
	case uint64:
		return v, true
	default:
		return 0, false
	}
}
```

Resume from one past the last offset you recorded. Saving the checkpoint in the
same transaction as the projection's own writes is what makes the pair effectively
exactly once; anywhere else is at least once, which is fine as long as the handler
is idempotent — see [idempotency](patterns.md#idempotency).

**There is no `LastHandledOffset` here.** The .NET reader exposes one, along with
`Handled`, `Failed` and `Skipped` counters; `patterns.ReadStream` returns a plain
`*acemq.Consumer` and reports none of that. Read the header, or count in the
handler.

## What a stream cannot do, and what this library does anyway

A stream never removes a message, and most of this library's failure handling is
built on moving one. The results are not all the same, and the differences matter
more here than anywhere else in the docs.

| On a queue | On a stream |
|---|---|
| `acemq.Accept()` removes the message | removes nothing; the log moves on and every other consumer still sees it |
| `acemq.Retry(err)` republishes onto the same queue | **appends a new copy to the log** — see below |
| `acemq.Reject(err)` republishes to `{queue}.dlq` | republishes to `{stream}.dlq` and works, but the original also stays in the stream |
| `acemq.Park(err)` republishes to `{queue}.parked` | the same, with the same caveat |
| the broker's own dead-lettering | **none** — a stream has no `x-dead-letter-exchange` |
| requeue | nothing to put back |

None of them decides where the next run starts. That is `StreamOptions.Offset`
and your checkpoint, as above.

### Retry is the trap

This library retries by republishing the message onto the queue it came from,
with the attempt counter advanced, and acknowledging the original — which is what
lets the attempt counter live on the message rather than in a map that a restart
empties. See [the attempt counter](reliability.md#the-attempt-counter-and-why-it-rides-on-the-message).

On a stream, "republish onto the queue it came from" means **appending a second
copy to the log**. It is not a redelivery; it is a new message at a new offset.
Every other consumer of that stream will read it as well. A projection rebuilding
from `FromFirst` next month will read both. A handler that returns `Retry` on
every message turns a stream into one that grows by a copy of itself per attempt,
bounded only by the retry policy.

So: **do not return `acemq.Retry` from a stream handler.** A stream handler has
two honest choices and you have to pick one:

```go
// Stop. The checkpoint is not advanced, so a restart comes back to this message.
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	if err := projection.Apply(ctx, m.Payload); err != nil {
		log.Printf("halting at %v: %v", m.Envelope.Headers["x-stream-offset"], err)
		halt()                 // cancel the process's context; nothing is checkpointed
		return acemq.Accept()  // settles this delivery without copying it anywhere
	}
	return checkpointAndAccept(ctx, m)
}
```

```go
// Skip, and count it. Nothing else records the gap.
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	if err := projection.Apply(ctx, m.Payload); err != nil {
		skipped.Add(1)
		log.Printf("skipping %s at offset %v: %v",
			m.Envelope.ID, m.Envelope.Headers["x-stream-offset"], err)
	}
	return checkpointAndAccept(ctx, m)
}
```

`acemq.Accept()` in the first one is not the handler saying the work succeeded —
it is the cheapest way to settle a delivery without appending a copy of it
anywhere, on the way out of a process that is stopping. What makes it recoverable
is that the checkpoint was not advanced, so the restarted reader comes back to
this offset and sees the message again after the fix.

Skipping is the other honest choice, and it is invisible unless you make it
visible: Java has a `skipFailures()` mode with a `skipped()` counter behind it,
and there is no equivalent here, so the counter and the log line are yours to
write. Nothing else records the gap.

### Rejecting works, and is not what Java says

Java's streams page says a stream has no dead-letter queue, which is true of
RabbitMQ's own dead-lettering: a stream carries no `x-dead-letter-exchange` and
the broker will not move anything off it.

This library does not use the broker's mechanism. `acemq.Reject` and
`acemq.Park` **republish** the message to `{stream}.dlq` or `{stream}.parked` —
ordinary classic queues declared by `acemq.Consume` when the consumer starts —
and then acknowledge the original, which on a stream means advancing past it. So
a rejected message does land somewhere an operator can find it, with the reason
on the envelope.

What is different from a queue is that **the original is still in the stream**.
The dead letter is a copy, not a move. Anything that replays the stream will read
the failed message again, and [`patterns.Replay`](patterns.md#replay) from
`{stream}.dlq` back to the stream appends yet another copy. Draining a stream's
dead letters is a different operation from draining a queue's, and it is worth
thinking about before you build a runbook on it.

## What it is for

More than one consumer needing the same messages; history that has to be
re-readable; a projection that must be rebuildable from scratch. Event sourcing,
audit logs, analytics fan-out.

It is the wrong shape for distributing work. Consuming removes nothing, so two
workers reading one stream both do the same job — work distribution wants a
queue, with its retry ladder, its dead letters and its competing consumers.
Rebuilding those on top of a stream is how a simple job becomes a distributed
systems project.

## Next

- [Exchanges, queues and bindings](topology.md#quorum-classic-and-streams) — how a
  stream differs from quorum and classic
- [Patterns](patterns.md#streams) — the short version, beside the rest
- [Retries, redelivery and shutdown](reliability.md) — what `Retry` does, and why
  it is the wrong verb here
