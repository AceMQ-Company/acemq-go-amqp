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

## Routing slips

An itinerary the message carries, instead of a central orchestrator:

```go
slip := patterns.NewRoutingSlip().
	Then("orders-events", "order.validate", "validate").
	Then("orders-events", "order.charge", "charge").
	Then("orders-events", "order.ship", "ship")

err := patterns.Start(ctx, mq, slip, order)
```

Each service does its part and the message goes to the next stop:

```go
sub, err := acemq.Consume(ctx, mq, "charge-queue",
	patterns.FollowSlip(mq, func(ctx context.Context, m acemq.Message[Order]) (Order, error) {
		return charge(ctx, m.Payload)
	}))
```

The route is decided once, by whoever started the work, and travels with the
message rather than living in a component every service has to talk to.

**What it costs:** no single place knows the whole route at runtime, so a route
that is wrong is discovered one hop at a time. Worth it when the steps vary per
message; not worth it when every message goes the same way, where a fixed chain
of consumers is simpler to follow.

## Consumer groups

```go
group, err := patterns.NewConsumerGroup(ctx, mq, "orders", 4, handle)
defer group.Close()
```

Several consumers over one queue, stopped together.

**Concurrency or a group?** `acemq.Concurrency` runs several handlers on one
consumer and one channel. A group runs several consumers, each with its own
channel and prefetch. Use a group when handlers are slow enough that one
channel's prefetch is the limit, or when a fair share across processes matters —
the broker round-robins between consumers, so four here compete evenly with four
in another instance.

A group that cannot start closes what it already started, because a half-started
group holds messages nothing is going to handle.

## Schema registry

```go
registry := patterns.NewInMemorySchemaRegistry()
schema, err := registry.Register(ctx, "order.placed", "avro", definition)
```

Messages carry a small identifier and consumers look the schema up, which is
what lets a producer add a field without every consumer being redeployed the
same afternoon. The [Avro codec](serialization.md) uses it directly.

Definitions are fingerprinted, so a service registering on start-up does not add
a version per restart. A lookup that finds nothing returns `ErrSchemaNotFound`
rather than a zero value — a consumer that cannot find its schema has a real
problem and must not carry on with an empty definition.

`NewInMemorySchemaRegistry` shares nothing between processes, which is the whole
point of a registry. Use the SQL one, or Confluent's, for anything real.

## Stores that survive a restart

The in-memory idempotency store and outbox are for tests and for one worker.
The SQL ones are the point:

```go
tx, _ := db.BeginTx(ctx, nil)
placeOrder(ctx, tx, order)

record, _ := patterns.Record(mq, "orders-events", "order.placed", event)
patterns.NewSQLOutboxStore(tx, patterns.PostgresDialect).Add(ctx, record)

tx.Commit()
```

They take anything with `ExecContext` and `QueryContext`, so `*sql.Tx` satisfies
them and **the record commits with the work**. There is a test that rolls a
transaction back and proves neither the order nor the message survived.

`Schema()` returns the DDL rather than running it. A library that runs
migrations against your database on start-up is a library fighting your
migration tool — put it in a migration.

Dialects cover Postgres, MySQL and SQLite: placeholder syntax and the spelling
of "insert unless it is already there" genuinely differ, and getting either
wrong fails at runtime on one database and not another.

## Streams

A stream keeps its messages after they are read, so several consumers can each
read from their own position:

```go
err := patterns.DeclareStream(ctx, mq, "events", patterns.StreamRetention{
	MaxAge: 7 * 24 * time.Hour,
})

sub, err := patterns.ReadStream(ctx, mq, "events", handle,
	patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 100})
```

**It is not a queue with a longer memory.** Acknowledging advances this
consumer's position rather than removing anything, and rejecting dead-letters
nothing, because there is nothing to remove it from. A message that cannot be
handled has to be dealt with by the handler — logged, copied elsewhere, counted
— and the stream moves on regardless. Nothing is lost, and nothing is retried
for you.

Retention is unbounded by default, which for a stream means "until the disk is
full". Set `MaxAge` or `MaxBytes` on anything that runs for long.
