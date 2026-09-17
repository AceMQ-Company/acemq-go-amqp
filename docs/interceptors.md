# Interceptors

Every organisation has something that belongs on every message and no library can
guess: a tenant identifier, an audit record, a policy check on what is allowed
out, a trace context. Without a seam these get copied into every call site, where
one of them is eventually forgotten — and nobody finds out until the message that
needed it is the one that went without it.

```go
mq, err := acemq.Connect(ctx, url,
	acemq.WithPublishInterceptor(stampTenant(tenant)),
	acemq.WithConsumeInterceptor(refuseOtherTenants(tenant)))
```

## Two chains, and they are functions

There are no interfaces here and nothing to implement. An interceptor is a
function, which is what the seam is in Go:

```go
type PublishInterceptor func(ctx context.Context, c *PublishContext) error
type ConsumeInterceptor func(ctx context.Context, c *ConsumeContext) error
```

The whole shape is in those two lines: it is handed a context and a mutable
description of the message, and the error it returns is the decision. Nothing
subclasses anything, there is no `Order` field to fight over, and a closure over
whatever the interceptor needs is the dependency injection.

```go
func stampTenant(tenant string) acemq.PublishInterceptor {
	return func(_ context.Context, c *acemq.PublishContext) error {
		c.SetHeader("x-tenant", tenant)
		return nil
	}
}
```

The .NET library needs a base class per side with three overridable methods, and
`Order` on every implementation, because C# has no closures over an interface and
cannot carry an implementation on one under `netstandard2.0`. Go pays none of
that, and the price is that there is no "after" hook at all — see
[what there is no hook for](#what-there-is-no-hook-for).

## Registration is a connection option

```go
mq, err := acemq.Connect(ctx, url,
	acemq.WithPublishInterceptor(first),
	acemq.WithPublishInterceptor(second),
	acemq.WithConsumeInterceptor(check))
```

Both are `ConnOption`, so **interceptors are fixed when the connection is
built**. There is no way to add one later and no way to remove one. That is
narrower than .NET, where a consumer re-reads its chain on every delivery and a
publisher does not, and the asymmetry there is a documented thing to trip over.
Here there is nothing to trip over: the set is whatever `Connect` was given, for
the life of the connection.

**They run in the order given, and only on the way in.** There is no reversal,
because there is no way out to reverse: a chain that opens something on the way
in has nothing to close it with.

`acemq.WithPublishInterceptor(nil)` fails the connection rather than being
ignored, with `acemq: the interceptor is nil`. A nil function in the chain would
panic on the first publish, a long way from the line that registered it.

## What a publish interceptor can change

`PublishContext` is a pointer to a mutable struct, and **everything on it is
yours to change**:

| | |
|---|---|
| `Exchange` | where it is going. Changing it redirects the message. |
| `RoutingKey` | what it is published under. Changing it redirects the message. |
| `Envelope` | a `*acemq.Envelope` — headers, correlation, type, all of it |
| `Payload` | the value about to be encoded, as an `any` |

```go
func quarantineLargeOrders(limit int) acemq.PublishInterceptor {
	return func(_ context.Context, c *acemq.PublishContext) error {
		if order, ok := c.Payload.(OrderPlaced); ok && order.TotalCents > int64(limit) {
			c.RoutingKey = "orders.review"
			c.SetHeader("x-quarantined-because", "over the limit")
		}
		return nil
	}
}
```

This is the opposite of .NET, where only the envelope can be replaced and the
destination and payload are read-only on purpose — the argument being that an
interceptor able to redirect a message has a surprising amount of power for
something usually added to attach a header. Go took the other side: a pointer to
a struct with exported fields is the ordinary Go shape, and hiding three of them
behind accessors to prevent a misuse that the reader can see in one line is not
how this language reads. **You get the power and the responsibility with it.**

`c.SetHeader(name, value)` is a convenience that creates the header map if it is
nil; `c.Envelope.Headers[name] = value` panics on a message that has no headers
yet, so prefer the method.

> **`SetHeader` does not check the reserved namespace, and `acemq.Header` does.**
> A publish given `acemq.Header("x-acemq-id", …)` is refused with an error naming
> the namespace. The same name through `SetHeader` is accepted, and then dropped
> without a word when the envelope is rendered onto the wire, because
> `x-acemq-` belongs to the engine — see [the envelope](envelope.md). An
> interceptor is the one place in this library where writing a reserved header
> fails silently. Use a prefix of your own.

**Replacing the payload has one condition.** The publisher reads it back with a
type assertion to the publisher's own `T`, and a value that does not assert is
silently ignored rather than being an error:

```go
c.Payload = OrderPlaced{OrderID: "redacted"}   // a Publisher[OrderPlaced] takes this
c.Payload = map[string]any{"orderId": "x"}     // ignored; the original is encoded
```

There is no way to be told that it was ignored. If an interceptor rewrites
payloads, assert the type on the way in as well as setting it on the way out, so
the two cannot drift apart.

The chain runs **before the codec**, so everything above is decided against your
value rather than against bytes.

## What a consume interceptor can change

Less than the publish side, and not what the field names suggest.

| | |
|---|---|
| `Queue` | where it came from — changing it does nothing |
| `Envelope` | a `*acemq.Envelope`, and **changes to it reach the handler** |
| `Body` | the undecoded payload — **changing it does nothing** |
| `ContentType` | what the sender said it was — **changing it does nothing** |
| `Redelivered` | the broker saying it has handed this over before |

The envelope on the context points at the same value the handler is later given,
so a header added here arrives:

```go
func stampReceivedAt() acemq.ConsumeInterceptor {
	return func(_ context.Context, c *acemq.ConsumeContext) error {
		c.Envelope.Headers["x-received-at"] = time.Now().UTC().Format(time.RFC3339)
		return nil
	}
}

// and in the handler
received, _ := m.Envelope.Headers["x-received-at"].(string)
```

That is worth saying plainly, because it is the thing .NET, whose consume context
is read-only throughout, explicitly cannot do.

`Body` and `ContentType` are a different story and the one real trap on this
page. They are **copies of what arrived**, put on the context so an interceptor
can look at the bytes before anything decodes them. The decode that follows reads
the delivery, not the context, so rewriting either changes nothing at all and
nothing tells you so:

```go
c.Body = decrypt(c.Body)   // has no effect; the original body is decoded
```

Decryption and redaction on the way in belong in a [codec](serialization.md) or
in a [middleware around the one handler](patterns.md#pipelines) that needs it,
not here.

## Refusing a message

**A publish interceptor that returns an error stops the publish**, and the caller
gets that error back, unwrapped and unprefixed:

```go
err := pub.Send(ctx, order)
// too big to publish
```

That is the point of intercepting rather than observing: a message that must not
go out is stopped once, here, rather than in every publisher. Interceptors
registered after the one that refused do not run, and the ones that already ran
are not told — an interceptor with something to undo cannot rely on being called
back.

**A consume interceptor that returns an error dead-letters the message**, without
the handler running:

```go
func refuseOtherTenants(mine string) acemq.ConsumeInterceptor {
	return func(_ context.Context, c *acemq.ConsumeContext) error {
		if tenant, _ := c.Envelope.Headers["x-tenant"].(string); tenant != mine {
			return fmt.Errorf("tenant %q is not served by this process", tenant)
		}
		return nil
	}
}
```

The message goes to `{queue}.dlq` with the reason on its envelope:

```
an interceptor refused it: tenant "beta" is not served by this process
```

Dead-lettered rather than retried, because an interceptor that says no will say
no again to the same message, and a retry ladder would only spend five attempts
finding that out. It is counted as `dead_lettered` on `acemq.consume.total` like
any other, and reported through `acemq.OnSettled` like any other, so a span
covering the delivery closes correctly.

This is a real difference from .NET, where an exception from a consume
interceptor escapes the retry ladder entirely, nacks the delivery for a plain
redelivery, and produces an infinite loop with no counter moving. Here the
refusal is a first-class outcome.

**Do not panic.** A handler that panics is recovered and turned into a rejection;
an interceptor that panics is not. It unwinds a consumer's worker goroutine, and
an unrecovered panic in a goroutine takes the process with it. Return an error.

## Where the chains sit

On a publish:

```
interceptors  →  codec  →  transport  →  confirm
```

Before the codec, as above. Also **before the duration is timed**: the clock for
`acemq.publish.duration` starts after encoding, so time spent in an interceptor
is in neither the metric nor anything else.

It is, however, **inside the span** when the publisher is a traced one:

```go
tracing := otel.New()
mq, err := acemq.Connect(ctx, url,
	acemq.WithPublishInterceptor(tracing.PublishInterceptor()))

orders := otel.NewPublisher[OrderPlaced](tracing, mq, "", "orders")
```

`otel.Publisher` opens the span and then delegates, so the interceptor runs with
the publish span current — which is exactly what makes
`tracing.PublishInterceptor()` work: it reads the trace context out of the
`ctx` it is handed, writes `traceparent` onto the envelope, and completes the
attributes of a span that was opened before the envelope existed. A plain
`acemq.Publisher` opens no span at all, and the interceptor sees whatever the
caller's context was carrying. This is the reverse of .NET, where the publish
chain runs before the span starts.

On a consume:

```
interceptors  →  decode  →  handler (and the span, if the handler is wrapped)
```

Interceptors run **first**, before the body has been decoded. Two consequences,
both the opposite of .NET's:

- **A body that will not decode still reaches the chain.** A consume interceptor
  here *is* a complete record of what arrived. It is the only place that sees a
  message which is about to be [parked](consuming.md#reject-or-park) as
  unreadable.
- **A duplicate still reaches the chain.** Idempotency in this library is
  [a wrapper around the handler](patterns.md#idempotency) rather than a stage in
  the engine, so the claim is taken after the interceptors have run, not before.

And the asymmetry that is the same in both libraries: the consume span is opened
by `otel.Handle`, which wraps the *handler*, so it starts after the interceptors
have finished. **A consume interceptor cannot enrich the consume span**, and
decode time is inside neither the span nor `acemq.consume.duration`. If you are
adding an interceptor to measure how long a message takes, what you will measure
is the handler and not the delivery. See
[metrics, tracing and health](observability.md#tracing) for what is measured
instead.

## Not every publish goes through the chain

This is the one on this page most likely to surprise, and it is checkable in a
line of code: the chain lives in `Publisher.publish`, so anything that reaches
the broker through `Conn.PublishRaw` bypasses it.

| Goes through the publish chain | Does not |
|---|---|
| `Publisher.Send`, `SendResult`, `SendAll`, `SendEnvelope` | a retry republished onto its own queue |
| a request from `patterns.Requester` | a dead letter to `{queue}.dlq` |
| a reply from `patterns.Serve` | a park to `{queue}.parked` |
| a [routing slip](patterns.md#routing-slips) forwarding a message | `patterns.Replay` putting messages back |
| a [pipeline](patterns.md#pipelines) step publishing onwards | the `patterns.OutboxRelay` |
| | `patterns.Scheduler` moving a message between rungs |

The right-hand column is the engine moving a message it is already holding,
usually with an envelope it has already built, and running an application's
interceptor over it would let a header be added to a message the application
never published.

The practical consequences are worth stating rather than deriving:

- **An audit interceptor does not see dead letters or retries.** Use
  `acemq.OnSettled` or the [consume metrics](observability.md#metrics) for those.
- **Outbox-relayed messages carry no trace context from the relay.** Whatever was
  stored with the record at write time is what goes out.
- **A tenant stamp added by an interceptor survives a dead letter**, because the
  header is on the envelope by then and the republish copies the envelope. It is
  the interceptor that does not run again, not the header that is lost.

## Batches

`Publisher.SendAll` runs the publish chain **once per message**, the same as a
loop over `Send` would. Nothing about the interceptors is batched.

What is different is that **they run concurrently**: `SendAll` starts one
goroutine per payload, so an interceptor keeping state of its own has to be safe
for concurrent use. A counter needs `sync/atomic` or a mutex; a map needs a lock.
The same was already true of a program publishing from several goroutines, and
`SendAll` makes it true of a program that never wrote a `go` statement.

Ordering follows: the interceptors for the whole batch run before any confirm is
awaited, and they do not run in payload order. The `[]PublishResult` that comes
back is in payload order; nothing about the chain is.

And one thing that is *not* different: at the RabbitMQ transport,
`Publish` holds its mutex across the confirm, so publishes are still serialised
against the broker. `SendAll` buys ordering and counting semantics — see
[publishing a batch](publishing.md#publishing-a-batch) — rather than throughput,
today.

## What there is no hook for

There is no "after". No `AfterConfirm`, no `AfterHandle`, no `OnError`. An
interceptor sees a message on its way in and never learns how it ended.

That is a real limit and the honest answer to "how do I record what happened to
every message" is that you do not use an interceptor for it:

| What you want | What to use |
|---|---|
| how a delivery was settled — acked, retried, dead-lettered, parked | `acemq.OnSettled(ctx, f)` inside the handler, or [`otel.Handle`](observability.md#tracing) which does it for you |
| counts and durations of everything | an [`acemq.Observer`](observability.md#metrics) on the connection |
| what the broker said about a publish | `Publisher.SendResult`, which returns the `PublishResult` |
| one handler wrapped, on the way in *and* out | [`patterns.Chain`](patterns.md#pipelines) and a `patterns.Middleware[T]` |

The last one is the important one, because it is the thing an interceptor is
usually reached for by mistake. A middleware wraps one handler, sees the `Ack`
that comes back, and can change what the handler is given:

```go
handler := patterns.Chain(handle,
	otel.Middleware[OrderPlaced](tracing, "orders"),
	patterns.WithTimeout[OrderPlaced](10*time.Second),
	patterns.WithIdempotency[OrderPlaced](store))
```

Outermost first, so the span records what the timeout and the idempotency guard
decided.

Interceptors are cross-cutting policy on a whole connection; middleware is one
consumer's business. A ten-second timeout in an interceptor would also apply to
the queue whose handler is meant to take four minutes.

## Concurrency

An interceptor is called on whichever goroutine is publishing or handling. There
is no per-message state bag on either context and no guarantee about which
goroutine you are on, so anything an interceptor remembers between calls has to
be safe for concurrent use.

To carry state from an interceptor into a handler, use the context you were
handed — that is what it is for, and it is the one channel that is per-message by
construction. Note that the `ctx` is passed by value and a `context.WithValue`
inside an interceptor does not propagate out of it; put the value on the envelope
instead, which does.

## Next

- [Publishing](publishing.md) — what `Send` puts on the wire once the chain has
  finished with it
- [Patterns](patterns.md#pipelines) — the per-handler version
- [Metrics, tracing and health](observability.md) — the numbers you would
  otherwise write an interceptor for
