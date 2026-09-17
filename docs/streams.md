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
		if err := projection.Apply(ctx, m.Payload); err != nil {
			return acemq.Reject(err)
		}
		return acemq.Accept()
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
			return acemq.Park(err) // not Retry — see below
		}
		if offset, ok := patterns.StreamOffsetOf(m.Envelope); ok {
			if err := checkpoints.Save(ctx, "projection-a", offset); err != nil {
				return acemq.Park(err)
			}
		}
		return acemq.Accept()
	},
	patterns.StreamOptions{
		Offset:   patterns.FromOffset(resume + 1),
		Prefetch: 100,
		Name:     "projection-a",
	})
```

`patterns.StreamOffsetOf` reads the header tolerantly, because the broker writes
an integer and which width it arrives as depends on the client. The second return
is whether the delivery carried an offset at all, and it matters: zero is a real
offset — the first message in the stream — so a reader that could not tell "the
beginning" from "this delivery said nothing" would write a checkpoint of zero for
a message that had no position, and the next run would replay everything
believing it was resuming.

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
| `acemq.Retry(err)` republishes onto the same queue | **refused** — it would append a new copy to the log. See below |
| `acemq.Reject(err)` republishes to `{queue}.dlq` | republishes to `{stream}.dlq` and works, but the original also stays in the stream |
| `acemq.Park(err)` republishes to `{queue}.parked` | the same, with the same caveat |
| the broker's own dead-lettering | **none** — a stream has no `x-dead-letter-exchange` |
| requeue | nothing to put back |

None of them decides where the next run starts. That is `StreamOptions.Offset`
and your checkpoint, as above.

### Retry is refused

This library retries by republishing the message onto the queue it came from,
with the attempt counter advanced, and acknowledging the original — which is what
lets the attempt counter live on the message rather than in a map that a restart
empties. See [the attempt counter](reliability.md#the-attempt-counter-and-why-it-rides-on-the-message).

On a stream, "republish onto the queue it came from" means **appending a second
copy to the log**. It is not a redelivery; it is a new message at a new offset.
Every other consumer of that stream would read it as well. A projection rebuilding
from `FromFirst` next month would read both. A handler that returned `Retry` on
every message would turn a stream into one that grows by a copy of itself per
attempt, bounded only by the retry policy.

So `ReadStream` does not obey it. A handler that returns `acemq.Retry` has the
message **parked** — republished to `{stream}.parked`, which touches nothing in
the stream — and the reason it is given is a `patterns.RetryOnStreamError` that
names the two alternatives:

```
acemq: a handler on stream "orders.log" returned acemq.Retry for message
order-1 at offset 41337, which this library refuses: a retry republishes the
message onto the queue it came from, and on a stream that appends a second copy
to the log at a new offset — for every consumer to read now and on every replay
afterwards. It was parked on orders.log.parked instead. A stream handler has two
honest choices: park it deliberately with acemq.Park(err), or record the
x-stream-offset header and return acemq.Accept() to checkpoint and move on. The
handler's reason was: the projection store is down
```

The handler's own error is wrapped, so `errors.Is` against a sentinel still
matches through the refusal. Parking is the conservative half of the choice and
not the library deciding for you — it is what leaves the stream untouched while
the message goes somewhere an operator will find it. Pick one deliberately:

```go
// Park it. The message is in {stream}.parked with the reason on its envelope,
// and the run moves on. The original is still in the stream, as always.
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	if err := projection.Apply(ctx, m.Payload); err != nil {
		return acemq.Park(err)
	}
	return checkpointAndAccept(ctx, m)
}
```

```go
// Checkpoint and move on, counting the gap. Nothing else records it.
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	if err := projection.Apply(ctx, m.Payload); err != nil {
		skipped.Add(1)
		offset, _ := patterns.StreamOffsetOf(m.Envelope)
		log.Printf("skipping %s at offset %d: %v", m.Envelope.ID, offset, err)
	}
	return checkpointAndAccept(ctx, m)
}
```

Stopping is the third thing people mean by "retry" here, and it is the second
one with the checkpoint left where it was:

```go
// Stop. The checkpoint is not advanced, so a restart comes back to this message.
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	if err := projection.Apply(ctx, m.Payload); err != nil {
		offset, _ := patterns.StreamOffsetOf(m.Envelope)
		log.Printf("halting at %d: %v", offset, err)
		halt()                 // cancel the process's context; nothing is checkpointed
		return acemq.Accept()  // settles this delivery without copying it anywhere
	}
	return checkpointAndAccept(ctx, m)
}
```

**Java draws this line in the type system and Go draws it at the verb.** Java's
`StreamConsumer` is a separate type from `MessageConsumer` precisely so the
outcomes a stream cannot honour are never offered — a single type covering both
would be one where half the methods throw. Go has one `acemq.Handler` and
`ReadStream` takes it, so the refusal happens when the verb is used rather than
when it is spelled. Same intent, one step later.

`acemq.Accept()` in the stopping example is not the handler saying the work
succeeded — it is the cheapest way to settle a delivery without appending a copy
of it anywhere, on the way out of a process that is stopping. What makes it
recoverable is that the checkpoint was not advanced, so the restarted reader
comes back to this offset and sees the message again after the fix.

Skipping is invisible unless you make it visible: Java has a `skipFailures()`
mode with a `skipped()` counter behind it, and there is no equivalent here, so
the counter and the log line are yours to write. Nothing else records the gap.

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
  it is refused here
