# Publishing

## A publisher per message type

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "orders-exchange", "orders.created")
```

The two strings are the exchange and the routing key. Leave the exchange empty
to publish straight to a queue by name — on RabbitMQ the default exchange routes
to the queue whose name matches the routing key:

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
```

A publisher is safe for concurrent use and holds nothing but configuration.
Build one per message type at start-up and keep it; building one per message
achieves nothing and reads worse.

### Why it is a function

`NewPublisher` takes the connection rather than hanging off it, because a method
cannot have its own type parameter before Go 1.27. The module targets 1.23. See
[the overview](index.md) for the reasoning.

## Sending

```go
if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250}); err != nil {
	return err
}
```

The envelope is built for you: an identifier, a type defaulting to the routing
key, a correlation defaulting to the identifier, the connection's origin, and
the current time.

Options override any of it:

```go
err := pub.Send(ctx, order,
	acemq.MessageID("order-o-1"),
	acemq.CorrelationID(incoming.Envelope.CorrelationID),
	acemq.CausationID(incoming.Envelope.ID),
	acemq.SchemaVersion(2),
	acemq.Header("x-tenant", "acme"))
```

| Option | |
|---|---|
| `MessageID` | the identifier, and the default idempotency key. Generated when unset. |
| `MessageType` | overrides the type, which otherwise comes from the routing key |
| `SchemaVersion` | the payload's version. Starts at 1. |
| `CorrelationID` | propagated unchanged across hops. Defaults to the message id. |
| `CausationID` | the message that caused this one |
| `Origin` | the publishing process. Defaults to the connection's. |
| `Header` | one of your own headers |
| `Attempt`, `FirstSeen`, `DeadLetterReason` | for a message being replayed or moved by hand |

## Publishing a batch

A loop over `Send` pays a full broker round trip per message: publish, wait for
the confirm, publish the next. A thousand messages is a thousand waits, one
after another, and the broker spends nearly all of that time idle.

`SendAll` publishes every message first and waits for all the confirms
afterwards:

```go
results, err := pub.SendAll(ctx, orders)
if err != nil {
	return err
}
```

The results come back in the order the payloads were given, whatever order the
broker answered in, and there is one per payload:

```go
for i, r := range results {
	log.Printf("%s went out as %s, routed=%v", orders[i].OrderID, r.MessageID, r.Routed)
}
```

Options apply to every message in the batch, which is what makes a shared
correlation worth having here:

```go
results, err := pub.SendAll(ctx, orders,
	acemq.CorrelationID(incoming.Envelope.CorrelationID),
	acemq.Header("x-tenant", "acme"))
```

Do not pin `MessageID` for a batch. Every message would go out under the same
identifier, which is an instruction to a deduplicating consumer to keep one of
them and discard the rest. Leave it to be generated.

### It is not atomic

There is no such thing in AMQP. There is no way to publish a hundred messages so
that all or none arrive, and a library offering one would be lying about what
the protocol can do. What `SendAll` promises is narrower and still worth having:
every message was attempted, and every message was waited for.

So a batch can fail halfway. That is the ordinary outcome of a broker problem
partway through, and it is why the error carries counts rather than just a
reason:

```go
results, err := pub.SendAll(ctx, orders)

var failed *acemq.BatchPublishFailedError
if errors.As(err, &failed) {
	log.Printf("%d of %d confirmed", failed.Confirmed, failed.Total)
	for i, err := range failed.Errors {
		if err != nil {
			resend(orders[i]) // only the ones that did not arrive
		}
	}
}
```

| | |
|---|---|
| `Total` | how many payloads were in the batch |
| `Confirmed` | how many the broker took |
| `Failed` | how many it did not |
| `First` | the first failure **in payload order**, not the first the broker answered. The error unwraps to it. |
| `Errors` | one entry per payload, in payload order, `nil` where the message was confirmed |

`results` is returned alongside the error and is always as long as the batch, so
`results[i]`, `failed.Errors[i]` and `payloads[i]` are the same message.

The message reads:

```
2 of 5 messages were not confirmed; 3 were. The first failure was: ...
```

word for word what the Java and .NET libraries produce for the same failure, so
one line in a runbook covers a fleet in three languages.

A failure early in the batch does not cut the rest short. Everything is awaited
before the error is returned — stopping at the first failure is what loses the
count of what did arrive, and the count is the whole reason to report it.

### What bounds it

`SendAll` does not open a second route to the broker. Every message goes out
through the same path as `Send`, so whatever bounds publishes in flight still
does: the RabbitMQ transport shares one channel under a lock, because an AMQP
channel is not safe for concurrent use.

[Publish interceptors](interceptors.md) run on one goroutine per message during a
batch, so an interceptor keeping state of its own has to be safe for concurrent
use.

## Carrying context forward

Correlation is what lets somebody follow one business action across a dozen
services. Propagate it from the message you are handling, and record what caused
what:

```go
sub, err := acemq.Consume(ctx, mq, "orders",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		err := shipments.Send(ctx, ShipmentRequested{OrderID: m.Payload.OrderID},
			acemq.CorrelationID(m.Envelope.CorrelationID),
			acemq.CausationID(m.Envelope.ID))
		if err != nil {
			return acemq.Retry(err)
		}
		return acemq.Accept()
	})
```

Correlation stays the same the whole way along. Causation changes at every hop
and points back one step, so the chain can be walked in either direction.

## Your own headers

```go
acemq.Header("x-tenant", "acme")
```

**`x-acemq-` is reserved.** A header in that namespace belongs to the engine: it
is read onto the envelope if this version knows it, and dropped before the
application sees it either way. Rather than accept one and silently discard it,
`Send` refuses:

```
acemq: header "x-acemq-id" is in the reserved "x-acemq-" namespace and would be
dropped on consume; use a namespace of your own, such as x-yourcompany-
```

Use your own prefix for anything that has to survive the round trip.

## Durability

Messages are published persistent by default — the broker writes them to disk.
That is a bargain worth keeping unless you are moving telemetry that nobody will
miss:

```go
pub := acemq.NewPublisher[Heartbeat](mq, "", "heartbeats",
	acemq.NotPersistent[Heartbeat]())
```

Persistence is only half the promise. A persistent message on a queue that is
not durable still dies with the broker, so declare the queue durable too — which
is the default.

## A different codec for one publisher

```go
pub := acemq.NewPublisher[Telemetry](mq, "", "telemetry",
	acemq.PublishWith[Telemetry](myCodec))
```

One connection can send JSON to one queue and something denser to another. See
[codecs](serialization.md).

## Building the envelope yourself

When one message's metadata is derived from another's in a way the options do
not cover:

```go
env, err := acemq.NewEnvelope("order.placed",
	acemq.MessageID(id),
	acemq.FirstSeen(originallySeenAt),
	acemq.Attempt(3))
if err != nil {
	return err
}

if err := pub.SendEnvelope(ctx, order, env); err != nil {
	return err
}
```

Mostly useful for replaying a message out of a dead-letter queue with its
history intact.

## What can go wrong

`Send` returns an error when the payload will not encode, when a header name is
refused, or when the broker will not take the message. All three are worth
handling: a publish that fails and is ignored is a message that never existed,
and nothing downstream will ever notice.
