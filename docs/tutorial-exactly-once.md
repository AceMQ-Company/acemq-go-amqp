# 3. Never processing twice

**25 minutes. Needs Docker.**

"Exactly once" is a phrase brokers sell and none deliver. What is actually
available is **at-least-once delivery** plus **effects that happen once**, and
this is how to build the second half.

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
go get modernc.org/sqlite
```

SQLite because it needs no server; everything here works the same against
Postgres or MySQL by changing the dialect and the driver.

## Watch a duplicate happen

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders.placed")

env, err := acemq.NewEnvelope("order.placed", acemq.MessageID("dup-1"))
if err != nil {
	log.Fatal(err)
}

order := OrderPlaced{OrderID: "A-1", TotalCents: 4250}
if err := pub.SendEnvelope(ctx, order, env); err != nil {
	log.Fatal(err)
}
if err := pub.SendEnvelope(ctx, order, env); err != nil {   // the same envelope
	log.Fatal(err)
}
```

```
charged A-1
charged A-1
```

The customer was charged twice. That is not a contrived case — it is what a
broker does after a consumer crashes between doing the work and acknowledging,
and what an outbox relay does when it publishes and dies before recording that it
did.

`SendEnvelope` is used here to force the point: an envelope built once and sent
twice is two deliveries of one message identity. Ordinarily `Send` generates a
fresh identifier per message, which is why pinning `MessageID` across a
`SendAll` batch is the one thing [publishing](publishing.md#publishing-a-batch)
tells you not to do.

## Remember what you handled

```go
import "github.com/AceMQ-Company/acemq-go-amqp/patterns"

store := patterns.NewInMemoryIdempotencyStore(time.Hour)

sub, err := acemq.Consume(ctx, mq, "orders.placed",
	patterns.Idempotent(store, func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		log.Printf("charged %s", m.Payload.OrderID)
		return acemq.Accept()
	}))
```

```
charged A-1
```

`patterns.Idempotent` wraps a handler; it is not a consumer option and not a
flag. That is the Go shape for this: a `Handler[T]` in and a `Handler[T]` out, so
it composes with everything else that wraps a handler and there is no registry
deciding what applies to whom.

The key is `m.Envelope.ID`, which is stable across every redelivery of one
message — which is why sending the same envelope twice is recognised as one
message rather than two.

Use `patterns.IdempotentBy` when the natural key is in the payload instead:

```go
handler := patterns.IdempotentBy(store,
	func(m acemq.Message[OrderPlaced]) string { return "order:" + m.Payload.OrderID },
	handle)
```

### A duplicate is accepted, not rejected

The work was done, so the message has been handled. Dead-lettering it would raise
an alarm about something that went right.

### The key is forgotten when the handler fails

```go
FirstTime(ctx, key) (bool, error)   // claim
Forget(ctx, key) error              // release
```

Two methods, not three. When the handler fails, `Idempotent` calls `Forget`, so
the retry can run. That is the honest ordering — remembering a message that then
failed would mean the retry silently does nothing — and it is why this is a guard
against duplicates rather than a guarantee of exactly-once. Between the handler
finishing and the acknowledgement reaching the broker, a crash still leaves a
message that will be delivered again. Only a store written in the same
transaction as the work closes that gap, which is the next section.

### The in-memory one is not enough

It is per process. Run two instances of your consumer and a duplicate delivered
to the *other* one is handled twice. It does not fail; it deduplicates less than
it looks like it does.

```go
import (
	"database/sql"
	_ "modernc.org/sqlite"
)

db, err := sql.Open("sqlite", "orders.db")
if err != nil {
	log.Fatal(err)
}

store := patterns.NewSQLIdempotencyStore(db, patterns.SQLiteDialect)
if _, err := db.ExecContext(ctx, store.Schema()); err != nil {
	log.Fatal(err)
}
```

`Schema()` returns the DDL rather than running it. Running it here is fine for a
tutorial; in a real service put it in a migration, because a library that runs
migrations against your database on start-up is a library fighting your migration
tool.

The claim is an **insert**, so the primary key does the mutual exclusion: two
consumers racing, one insert succeeds, one does not, and nobody has to hold a
lock. `patterns.PostgresDialect`, `MySQLDialect` and `SQLiteDialect` differ in
exactly two things — the placeholder syntax and the spelling of "insert unless it
is already there" — and getting either wrong fails at runtime on one database and
not another.

Keys do not expire by themselves. `store.Prune(ctx, 30*24*time.Hour)` on a
schedule is the housekeeping.

## Closing the gap properly

The store is only a guarantee when the claim commits with the work:

```go
func handle(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return acemq.Retry(err)
	}
	defer func() { _ = tx.Rollback() }()

	first, err := patterns.NewSQLIdempotencyStore(tx, patterns.SQLiteDialect).
		FirstTime(ctx, m.Envelope.ID)
	if err != nil {
		return acemq.Retry(err)
	}
	if !first {
		return acemq.Accept()
	}

	if err := charge(ctx, tx, m.Payload); err != nil {
		return acemq.Retry(err)
	}
	if err := tx.Commit(); err != nil {
		return acemq.Retry(err)
	}
	return acemq.Accept()
}
```

`patterns.DB` is satisfied by both `*sql.DB` and `*sql.Tx`, which is the whole
point of the interface being three methods rather than a concrete type. The
claim and the charge are now one commit: either both happened or neither did, and
a crash anywhere in between leaves the message to be delivered again and
correctly reclaimed.

## The other half: publishing exactly once

Your service saves an order and publishes a message about it.

```go
if err := saveOrder(ctx, db, order); err != nil {   // committed
	return err
}
if err := pub.Send(ctx, orderPlaced); err != nil {  // the process dies here
	return err
}
```

The order exists and nobody was told. Swap the two and you announce an order that
might still roll back.

Neither ordering works, because there are two systems and no transaction across
them.

## The outbox

Write the message **in the same transaction as the order**:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer func() { _ = tx.Rollback() }()

if err := saveOrder(ctx, tx, order); err != nil {
	return err
}

record, err := patterns.Record(mq, "", "orders.placed", orderPlaced)
if err != nil {
	return err
}
if err := patterns.NewSQLOutboxStore(tx, patterns.SQLiteDialect).Add(ctx, record); err != nil {
	return err
}

return tx.Commit()
```

**Pass the transaction, not the database.** `NewSQLOutboxStore(db, …)` compiles
just as well, opens its own connection, and quietly gives up the entire
guarantee. This is the single most important line on the page, and the type
system cannot help you with it — both satisfy `patterns.DB`, which is what makes
the good version possible in the first place.

`patterns.Record` encodes the payload with the connection's codec and builds the
envelope, so what is stored is bytes and headers rather than a Go value. The
record outlives the process that wrote it, and the type may not survive a
deployment.

Then something publishes them:

```go
relay := patterns.NewOutboxRelay(mq, patterns.NewSQLOutboxStore(db, patterns.SQLiteDialect),
	patterns.RelayInterval(time.Second),
	patterns.RelayBatch(100))
relay.Start(ctx)
defer relay.Close()
```

The relay uses the **database**, not a transaction — it is a separate loop that
reads what has been committed. `relay.Sweep(ctx)` runs one pass by hand and
returns how many it moved, which is what to call in a test rather than sleeping.

There is a test in this repository that rolls the transaction back and proves
that neither the order nor the message survived.

## Why this is still not exactly once

The relay publishes, then marks the record as published. A crash between the two
republishes the message next sweep, because the record was never marked.

So delivery is still **at least once**, and it always will be. What you have
built is the other half: the consumer recognises the duplicate and the effect
happens once. That is what people mean when they say exactly once, and it is
worth knowing it is two mechanisms you assembled rather than a broker feature you
switched on.

Watch the lag rather than assuming it is zero:

```
acemq.outbox.lag     how far behind the relay is, in seconds
acemq.outbox.total   tagged published or failed
```

A relay that has stopped is a service whose messages have stopped, with no error
anywhere and a database quietly filling up. See
[tutorial 4](tutorial-observability.md).

## What you have

Duplicates that do nothing, and messages that cannot be lost between your
database and the broker. Next: [seeing what happens](tutorial-observability.md).
