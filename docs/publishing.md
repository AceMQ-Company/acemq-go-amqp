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
