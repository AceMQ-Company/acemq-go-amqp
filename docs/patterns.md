# Patterns

Everything above publishing and consuming lives in one package:

```go
import "github.com/AceMQ-Company/acemq-go-amqp/patterns"
```

It is separate because none of it is needed to send a message, and a core that
carries everything is a core nobody can read.

## Request and reply

```go
requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices")
defer requester.Close()

response, err := requester.Do(ctx, PriceRequest{SKU: "abc"})
```

And the other side:

```go
responder, err := patterns.Serve(ctx, mq, "prices",
	func(ctx context.Context, m acemq.Message[PriceRequest]) (PriceResponse, error) {
		return price(ctx, m.Payload)
	})
defer responder.Close()
```

Replies are paired with requests by correlation identifier, so many can be in
flight at once. There is a test that runs twenty concurrently and checks each
caller gets its own answer.

**This is a synchronous shape drawn on an asynchronous system, and that costs
something.** A caller blocked on a reply holds a goroutine, a connection and a
deadline; a queue that backs up turns into a service that stops responding.
Reach for it where a caller genuinely cannot proceed without the answer, and
publish an event otherwise.

A responder that returns an error sends it back rather than swallowing it, so a
blocked caller learns it failed instead of waiting out the timeout:

```
acemq: the responder failed: no such product
```

`ErrRequestTimedOut` says nothing about whether the request was handled. A
timeout is the absence of an answer, and the work may well have been done —
which is why a request that changes anything should be idempotent.

By default replies come back on an exclusive, auto-deleting queue that goes away
with the process. `patterns.ReplyTo("queue")` names one that survives a restart.

## Idempotency

```go
store := patterns.NewInMemoryIdempotencyStore(time.Hour)

sub, err := acemq.Consume(ctx, mq, "orders",
	patterns.Idempotent(store, handle))
```

A duplicate is **accepted**, not rejected: the work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right.

When the handler fails the key is forgotten, so the retry actually runs.
Remembering a message that then failed would mean the retry silently does
nothing and the message is dropped after appearing to succeed.

**This is a guard against duplicates, not a guarantee of exactly-once.** Between
the handler finishing and the acknowledgement reaching the broker, a crash still
leaves a message that will be delivered again. Only a store written in the same
transaction as the work closes that gap.

`NewInMemoryIdempotencyStore` is for tests and for one worker. With two, each
has its own memory and both will believe they are first.

`patterns.IdempotentBy` takes a key from the payload, for when two different
messages are about the same thing.

## The outbox

A service that writes to a database and then publishes has two things that can
fail independently, and the gap between them is where messages are lost or
invented. Writing the message into the same transaction as the work removes the
gap:

```go
tx, _ := db.Begin()
placeOrder(tx, order)

record, _ := patterns.Record(mq, "orders-events", "order.placed", event)
store.Add(ctx, record)      // the same tx

tx.Commit()
```

A relay publishes what was committed:

```go
relay := patterns.NewOutboxRelay(mq, store)
relay.Start(ctx)
defer relay.Close()
```

A record is removed only after the broker confirms, so a crash in between
republishes it. That is **at-least-once by construction** — which is why
consumers of anything sent this way need to be idempotent. Removing first would
lose messages instead, and a lost message is worse than a repeated one.

An `OutboxStore` is only worth having if `Add` can join the caller's
transaction. `InMemoryOutboxStore` cannot, and so has none of the property the
pattern exists for; it is there for tests and for seeing the shape.

The relay publishes the bytes that were recorded rather than re-encoding, because
a record outlives the process that wrote it and the Go type may not survive a
deployment.

## Ordering

A queue delivers in order; `acemq.Concurrency` above one stops honouring that.
Usually the right trade, and the wrong one where later messages about the same
entity must not overtake earlier ones.

```go
sub, err := acemq.Consume(ctx, mq, "orders",
	patterns.Ordered(patterns.ByHeader[OrderEvent]("x-order-id"), handle),
	acemq.Concurrency(16))
```

Messages sharing a key are handled one at a time; different keys still run at
once. Both halves are tested — ordering per key that became ordering overall
would just be concurrency turned off.

**What it does not do.** It orders messages already delivered to this process. It
cannot reorder what the broker delivered out of order, and with several consumers
on one queue it orders only within each. Ordering across consumers needs the
messages to reach the same consumer, which is a routing decision:

```go
key := patterns.PartitionedRoutingKey("orders", order.ID, 8)  // "orders.3"
```

## Pipelines

```go
handler := patterns.Chain(handle,
	patterns.WithLogging[OrderPlaced](log.Printf),
	patterns.WithTimeout[OrderPlaced](10*time.Second),
	patterns.WithIdempotency[OrderPlaced](store))
```

The first named is outermost, so logging records what the others decided.

`WithTimeout` reports a retry whenever the deadline passed, **whatever the
handler says about itself**. Between retrying work that may have succeeded and
accepting work that may have failed, duplicates are a problem you can solve and a
lost message is not. It cancels the context, so a cooperative handler stops
promptly; one that ignores its context will not stop, and no wrapper can make it.

`patterns.Then` publishes the result onwards, carrying the correlation and
recording causation:

```go
step := patterns.Then(
	acemq.NewPublisher[Shipment](mq, "shipping-events", "shipment.requested"),
	func(ctx context.Context, m acemq.Message[OrderPlaced]) (Shipment, bool, error) {
		if m.Payload.Digital {
			return Shipment{}, false, nil    // nothing to ship
		}
		return Shipment{OrderID: m.Payload.OrderID}, true, nil
	})
```

The input is accepted only once the output is published. If publishing fails the
input is retried and the work runs again, so a step that changes anything should
be idempotent.

## Replay

What somebody actually does at three in the morning: a dead-letter queue has two
thousand messages, the bug is fixed, and they need to go back — but not all of
them, and not silently.

```go
result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
	Queue:    "orders-dead",
	Exchange: "orders-events",
	Limit:    500,
	Filter: func(env acemq.Envelope, body []byte) bool {
		return strings.Contains(env.Error, "timeout")
	},
})
// moved 500, skipped 31 (limit)
```

The reason is part of the answer: "moved 500" means something quite different
when the limit was 500.

Messages the filter declines stay where they are. Each replayed message is
stamped `acemq-replayed-from`, `acemq-replayed-at` and `acemq-replay-count`, so a
consumer that needs to treat them differently can.

**How it knows when to stop.** Leaving a message in place means returning it, and
a returned message comes straight back — so a naive replay loops for ever. It
remembers the identifiers it has seen: the second sighting means the queue has
come full circle and everything left has been considered. An idle timer is the
second answer, for a queue somebody is still writing to.

Bodies are read as bytes, not through the connection's codec. A body that will
not decode is exactly the kind that ends up in a dead-letter queue.
