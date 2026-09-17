# Request and reply

```go
import "github.com/AceMQ-Company/acemq-go-amqp/patterns"

responder, err := patterns.Serve(ctx, mq, "prices",
	func(ctx context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
		return price(ctx, m.Payload)
	})
defer responder.Close()

requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices")
defer requester.Close()

answer, err := requester.Do(ctx, PriceRequest{SKU: "abc"})
```

## Read this before using it

Request and reply over a broker is **a synchronous call wearing asynchronous
clothes**, and it inherits the worst of both. The caller blocks like an HTTP
client, and the failure modes are a message broker's.

If the two services can speak HTTP or gRPC, they should. Those tools have
timeouts, retries, load balancing, circuit breakers and health checks that a
messaging library will not match, and everyone already knows how to operate them.

What this is genuinely for:

- the callee is reachable **only** on the broker — no HTTP endpoint, behind a
  firewall, on a network you do not control;
- one worker among many, where the broker is already doing the load balancing;
- a system that is already message-driven, where an HTTP hop would add a second
  failure domain to something that has one.

Those cases are real. Doing them by hand means a reply queue, a correlation
identifier, and a timeout somebody always forgets.

In Go the cost is concrete and countable: a goroutine parked on a channel, a
timer, and an entry in the requester's pending map, for as long as the answer
takes. A thousand callers waiting on a responder that has stopped answering is a
thousand of each, until the timeouts fire.

## The two halves

### The responder

```go
func Serve[Req, Resp any](
	ctx context.Context, conn *acemq.Conn, queue string,
	handle func(context.Context, acemq.Message[Req]) (Resp, error),
	opts ...acemq.ConsumeOption,
) (*Responder, error)
```

The handler returns `(Resp, error)` rather than an `acemq.Ack`, which is the one
place in this library where a handler does not settle its own message. That is
deliberate: the settlement here is not a free choice, because the caller is
blocked and something has to be sent back whichever way the work went. `Serve`
takes the decision so it cannot be got wrong.

`opts` are the ordinary [consume options](consuming.md), so a responder is tuned
like any other consumer:

```go
responder, err := patterns.Serve(ctx, mq, "prices", price,
	acemq.Concurrency(4), acemq.Prefetch(10))
```

**A responder handles one request at a time by default, and every caller behind a
slow one is waiting.** This is the first setting to reach for when request and
reply feels slow.

### The requester

```go
func NewRequester[Req, Resp any](
	ctx context.Context, conn *acemq.Conn, exchange, routingKey string, opts ...RequesterOption,
) (*Requester[Req, Resp], error)
```

The two type parameters are the shape of the question and the shape of the
answer, and they are on the requester rather than on `Do` because a method cannot
have its own type parameter before Go 1.27 — the same constraint that makes
`acemq.NewPublisher` a function. See [the overview](index.md#one-difference-from-java-and-net).

One requester holds one reply queue and one consumer on it, and many calls share
them. Build one per destination at start-up:

```go
requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices",
	patterns.Timeout(5*time.Second))
if err != nil {
	return err
}
defer requester.Close()
```

| Option | |
|---|---|
| `patterns.Timeout(d)` | how long `Do` waits. Thirty seconds by default. |
| `patterns.ReplyTo(queue)` | a named reply queue instead of a generated one |

`Do` is safe to call from as many goroutines as you like. Replies are paired with
requests by correlation identifier, so many can be in flight at once — there is a
test that runs twenty concurrently and checks each caller gets its own answer.

## The correlation identifier is not yours to set

`Do` generates one and appends it after your options, so a `CorrelationID` passed
in is overwritten:

```go
answer, err := requester.Do(ctx, request,
	acemq.CorrelationID(incoming.Envelope.CorrelationID)) // ignored
```

It has to be. The correlation is the only thing pairing a reply with the caller
waiting for it, and two callers that chose the same one would be handed each
other's answers. If you need the wider business correlation to travel, put it in
a header of your own:

```go
answer, err := requester.Do(ctx, request,
	acemq.Header("x-trace-of", incoming.Envelope.CorrelationID))
```

Everything else `Do` takes is an ordinary [envelope option](publishing.md) and is
applied as given.

## Where the reply address travels

**Write both, read either.** A request carries the reply queue twice, in the same
two places in all five AceMQ libraries:

| | |
|---|---|
| the `acemq-reply-to` application header (`patterns.HeaderReplyTo`) | travels through the envelope machinery and survives a hop through a service that rebuilds the message |
| AMQP's own `reply-to` property | what Java's and .NET's responders read |

`Do` writes both, to the same queue. `Serve` reads the header first and falls
back to the property, so it answers a request from any of the five and any of the
five can answer one of ours. Header first because it is the half that survives a
rebuild: a service that reconstructed a message kept the headers and lost the
properties, so where the two disagree the header is the more recent truth.

Neither name is in the reserved `x-acemq-` namespace, deliberately. The property
is a real AMQP property, so a service that never heard of this library can answer
a caller that uses it; the header is an ordinary application header, so it reaches
a handler instead of being stripped on the way in.

Publishing a request by hand rather than through a `Requester` means writing them
yourself:

```go
err := acemq.NewPublisher[PriceRequest](mq, "", "prices").Send(ctx, request,
	acemq.ReplyTo("my-replies"),
	acemq.Header(patterns.HeaderReplyTo, "my-replies"),
	acemq.CorrelationID(correlation))
```

A request carrying neither is **dead-lettered rather than retried**: retrying
cannot make a reply address appear. It lands in `prices.dlq` with the reason on
it.

## The reply queue

By default the requester generates one — `acemq-reply-<uuid>` — declared
transient, exclusive and auto-deleting, and therefore classic. It has to be
classic: RabbitMQ refuses a quorum queue that is any of those three, so a reply
queue that became quorum would not be a slower requester but one that cannot
start at all. It belongs to the connection and goes away with it, which is what
you want for a queue holding answers nobody will read once the process is gone.

`patterns.ReplyTo("orders-replies")` names one instead, and a named one is
**durable**, which means [quorum like any other durable queue](topology.md#quorum-classic-and-streams)
this library declares. Reach for it only when replies must survive a restart.

> **A named reply queue is yours to clean up.** Nothing puts an `x-expires` on
> it, so it outlives the process that declared it and keeps collecting replies
> that nobody is waiting for. The Java library sets `x-expires` on its generated
> queue; this library relies on `auto-delete` for the generated one, which is
> equivalent, and offers no such protection for a named one. Give a named reply
> queue a `x-expires` of your own through `acemq.QueueArg` if the service that
> reads it is not permanent.

`requester.ReplyQueue()` returns the name, which is worth logging at start-up:
it is the queue to look at in the management interface when replies are not
arriving.

## Timeouts

```go
answer, err := requester.Do(ctx, request)
if errors.Is(err, patterns.ErrRequestTimedOut) {
	// The work may well have been done.
}
```

The error reads:

```
acemq: no reply arrived before the deadline after 5s (correlation 9f2c…)
```

**A timeout is the absence of an answer, not evidence that nothing happened.**
The request may still be queued, may be being handled, or may have been handled
with the reply lost on the way back. Retrying is a decision about whether the
responder is idempotent, not a reflex — and where the work is not idempotent,
taking money or sending an email, a timeout is a question for a human or for a
[shared idempotency store](patterns.md#idempotency), not for a retry loop.

Two clocks bound a call and they are independent:

| | |
|---|---|
| `patterns.Timeout(d)` | the requester's own, and the one that returns `ErrRequestTimedOut` |
| the caller's `ctx` | cancelling it returns `ctx.Err()` — `context.Canceled` or `context.DeadlineExceeded` |

Neither is derived from the other. A `ctx` with a two-second deadline against a
requester built with thirty seconds gives up after two, with `context.DeadlineExceeded`.
Handle both, or handle neither and treat any non-nil error as "no answer":

```go
answer, err := requester.Do(ctx, request)
switch {
case errors.Is(err, patterns.ErrRequestTimedOut), errors.Is(err, context.DeadlineExceeded):
	return fallbackPrice(request), nil
case err != nil:
	return PriceResponse{}, err
}
```

**A reply that arrives after its caller gave up is dropped.** There is nowhere to
put it — the goroutine that wanted it has gone — and blocking would stall the
reply consumer for every other caller. Nothing counts these, which is the one
number this library would most like to have: replies arriving unmatched, rising
alongside timeouts, is the signature of a responder that is slower than callers
expect rather than of anything being broken.

## When the responder fails

An error from the handler is **sent back to the caller** rather than swallowed,
so a blocked caller learns it failed instead of waiting out the timeout. It
travels as the `acemq-error` header (`patterns.HeaderError`) on an otherwise
empty reply, and `Do` turns it back into an error:

```
acemq: the responder failed: no such product
```

Two things about that are worth knowing before you build on it.

**It is a string, not the error.** The responder's error crosses a broker as
text, so `errors.Is` and `errors.As` on the requester's side reach nothing of the
original. A caller that has to distinguish failures needs the distinction in the
payload — a `Resp` with a status field — rather than in the error.

**The request is dead-lettered as well as answered.** After the failure has been
sent, `Serve` rejects the request, so it lands in `prices.dlq` with the reason on
it. A failed request therefore leaves evidence in two places: the caller's error,
and a message an operator can look at. This is not configurable, and it means a
responder failing steadily fills a dead-letter queue steadily — which is the
right alarm, and worth knowing before it goes off.

An error from the handler is never retried, whether or not it is
`acemq.Fatal`. Retrying would answer the caller twice.

**A reply that will not publish is a different case**: the work is done but the
answer did not get out, and `Serve` returns `acemq.Retry`, so the request comes
round again and the work runs a second time. That is why **a responder should be
idempotent** — the same reason a timeout is not automatically retryable, seen
from the other end.

## What the counters promise

A responder reports two numbers, and both are safe to read the instant a round
trip returns:

```go
responder.Answered()      // requests answered, counted before the reply left
responder.Unanswerable()  // requests that named nowhere to reply
```

`Answered()` is incremented **before** the reply is published, so a caller
holding its answer can rely on the count already including it. The other order
looks more natural and is wrong: it leaves a window where the reply is in the
caller's hands and the responder still says nothing has been answered, which is a
monitoring dashboard reporting an idle service that is demonstrably working. A
publish that fails takes its increment back, so this counts replies that were
sent rather than replies that were attempted.

The counters exist before the responder subscribes, so a request the broker hands
over during start-up — what a queue with a backlog looks like from in here — is
counted like any other. Neither number needs a wait before it can be trusted, and
code that sleeps before reading one is working around a defect that is fixed.

A handler that returned an error is **not** counted as answered. The failure does
go back to the caller, but it is not an answer, and counting it as one would make
a responder that fails every request look like one that works.

`Unanswerable()` above zero means a caller is publishing where it means to
request: a message that names neither the `acemq-reply-to` header nor AMQP's own
`reply-to` property cannot be answered by anybody. This library dead-letters such
a request where Java logs it and acknowledges it — both count it the same way, at
the same moment, and what differs is where the message ends up. A request nobody
can answer is worth keeping on `{queue}.dlq` for whoever has to find the sender.

Java and .NET promise exactly this ordering, in those words. Python and Ruby
expose neither number yet.

## What the requester measures

```
acemq.request.total{routing.key="pricing", outcome="answered"}
acemq.request.duration{routing.key="pricing", outcome="timed_out"}
```

`Requester.Do` writes both, timing the round trip as the caller experienced it —
from before the request is published to the moment `Do` is about to return,
timeout included. The publish was already timed by the publish metrics and the
reply's delivery by the responder's consume metrics, and neither of those is the
number a blocked caller is holding.

| `outcome` | |
|---|---|
| `answered` | a reply came back |
| `timed_out` | none did before the deadline |
| `failed` | the publish failed, the context was cancelled, or the reply carried the responder's error |

A timeout is `timed_out` rather than `failed` on purpose: the work may well have
been done, and a counter saying otherwise sends somebody looking for a failure
that did not happen. A reply that came back carrying an error *is* `failed` — the
round trip completed and the answer was bad news, which is a different thing from
no answer.

Java also tags `message.type` and `transport`. The type is built inside
`Publisher.Send` from options `Do` only passes through, so writing it here would
mean guessing at a value the caller may have overridden.

The tracing adapter spans the same round trip, with the same vocabulary:

```go
import "github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel"

tracing := otel.New()

answer, err := otel.Ask[PriceRequest, PriceResponse](ctx, tracing, "prices", requester, request)
```

That produces a `prices request` span of kind `CLIENT` — `CLIENT` rather than
`PRODUCER` because it waits, and its duration is a round trip — tagged
`answered`, `timed_out` or `failed`. Queried for the same round trip, the counter
and the trace answer with the same word.

The responder's side is the ordinary consume metrics on its queue:
`acemq.consume.total` tagged `acked` against `rejected`, and
`acemq.consume.duration` for how long answers take. See
[metrics, tracing and health](observability.md).

## Closing

```go
defer requester.Close()   // stops the reply consumer
defer responder.Close()   // stops answering
```

`requester.Close()` stops the reply consumer; the generated reply queue goes with
the connection. Calls still waiting when it happens run out their timeouts rather
than being woken, so close a requester after the callers using it have finished.

`responder.Close()` closes the underlying consumer, which
[finishes what it has already started](reliability.md#shutdown) rather than
abandoning it — a request being answered right now has a caller blocked on the
other side, and dropping it turns their call into a timeout.

## Next

- [Patterns](patterns.md) — idempotency, which decides whether a timeout can be
  retried
- [Consuming](consuming.md) — the options `Serve` passes through
- [Metrics, tracing and health](observability.md) — what `otel.Ask` records
