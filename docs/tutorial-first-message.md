# 1. Your first message

**10 minutes. No broker needed.**

By the end you will have published a message and consumed it, and know what each
line was for.

```bash
mkdir orders && cd orders
go mod init example.com/orders
go get github.com/AceMQ-Company/acemq-go-amqp
```

## Connect to nothing

Start with the in-process broker. It routes the way RabbitMQ routes, so what you
learn here holds later:

```go
package main

import (
	"context"
	"log"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

func main() {
	ctx := context.Background()

	mq, err := acemq.Connect(ctx, "memory://tutorial")
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	log.Println("connected")
}
```

```bash
go run .
# connected
```

`defer mq.Close()` matters and is not only tidiness: the connection owns every
consumer on it, and closing it stops them and waits for handlers that are
part-way through. There is more on that at the end.

**The error is a value, and the `if err != nil` is the design.** Every call in
this library that can fail returns one, including the ones that look like they
could not. Java throws and .NET throws; here the failure is in the signature
where the compiler can see it, and a program that ignores one is a program you
can grep for.

## Declare where messages go

```go
if err := mq.DeclareExchange(ctx, "orders", "topic"); err != nil {
	log.Fatal(err)
}
if err := mq.DeclareQueue(ctx, "orders.placed"); err != nil {
	log.Fatal(err)
}
if err := mq.Bind(ctx, "orders.placed", "orders", "order.placed"); err != nil {
	log.Fatal(err)
}
```

Three things, and they are different things. An **exchange** receives published
messages. A **queue** holds them for a consumer. A **binding** is the rule that
connects the two — without it a message reaches the exchange and goes nowhere.

`ctx` is the first argument to all three, and to nearly everything else in this
library. It is how a caller says "stop waiting": cancel it and a declare, a
publish or a request gives up rather than hanging on a broker that has stopped
answering.

Declaring is idempotent, so this is safe to run at every start-up. Three calls
get tedious once there is more than one queue, and
[the topology builder](topology.md#declaring-it-all-at-once) does the same thing
in one:

```go
err := acemq.NewTopology().
	Exchange("orders", "topic").
	Queue("orders.placed").
	Binding("orders.placed", "orders", "order.placed").
	Apply(ctx, mq)
```

## Define a message

```go
type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}
```

**Tag the fields.** Without a tag, Go's JSON encoder writes the exported field
name — `OrderID`, `TotalCents` — and a Java or .NET consumer reading `orderId`
finds nothing and gets a zero value rather than an error. The tag is the wire
contract. See [codecs](serialization.md#tag-your-fields).

Cents rather than a float, for the reason every money bug has.

## Publish

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "orders", "order.placed")

result, err := pub.SendResult(ctx, OrderPlaced{OrderID: "A-1", TotalCents: 4250})
if err != nil {
	log.Fatal(err)
}
log.Printf("published %s, routed=%v, confirmed=%v", result.MessageID, result.Routed, result.Confirmed)
```

A publisher is built once and kept. It is safe to use from as many goroutines as
you like and holds nothing but configuration, so building one per message
achieves nothing and reads worse.

`Send` returns only an error; `SendResult` also returns what the broker said.
Either way the call **returns once the broker has confirmed the message**, not
when the bytes reach the socket. Those are different events, and the gap between
them is where messages are lost on a broker restart.

### Why `NewPublisher` is a function

```go
func (c *Conn) Publisher[T any](...)   // a generic method: needs Go 1.27
func NewPublisher[T any](c *Conn, ...) // a generic function: works everywhere
```

A method cannot have its own type parameter before Go 1.27, and this module
declares `go 1.23` so it can be used by projects that have not moved. `mq.Publisher[OrderPlaced](...)`
would read more like the Java and .NET libraries and would not compile for most
people. `acemq.Consume` is a function for the same reason.

## Delete the binding and watch it fail

Comment out the `Bind` call and run it again. Nothing happens — the publish
succeeds and the message goes nowhere, because that is what an AMQP broker does
with a message no queue is bound to receive. Ask to be told instead:

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "orders", "order.placed",
	acemq.Mandatory[OrderPlaced]())
```

```
acemq: message 7d3f… to exchange "orders" with key "order.placed" reached no
queue: no queue is bound to receive it
```

Most brokers drop that message silently. Finding out hours later, from an absence
of messages, is the failure this library exists to make loud. Put the binding
back.

## Consume

```go
sub, err := acemq.Consume(ctx, mq, "orders.placed",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		log.Printf("got %s for %d", m.Payload.OrderID, m.Payload.TotalCents)
		return acemq.Accept()
	})
if err != nil {
	log.Fatal(err)
}
defer sub.Close()
```

Start the consumer **before** publishing, then run:

```
published 7d3f…, routed=true, confirmed=true
got A-1 for 4250
```

The handler returns a decision rather than throwing, returning nothing, or
calling an `ack` method on something:

| | |
|---|---|
| `acemq.Accept()` | handled; the broker may forget it |
| `acemq.Retry(err)` | failed, but another attempt might work |
| `acemq.Reject(err)` | will never work; stop and keep the evidence in `orders.placed.dlq` |
| `acemq.Park(err)` | nobody could read it; keep it in `orders.placed.parked` |

**A handler that forgets to decide does not compile.** That is the whole reason
for the return value: a function typed to return an `acemq.Ack` has to return
one, and there is no path through it that leaves a message in limbo. A handler
that called `message.ack()` could always have a branch that did not.

Try returning `acemq.Retry(errors.New("not yet"))` and watch the message arrive
again. Then print `m.Envelope.Attempt` and watch it climb. (Without a retry
policy it climbs for ever — that is
[tutorial 2](tutorial-surviving-failure.md).)

## Several messages at once

A loop over `Send` pays a full broker round trip per message: publish, wait for
the confirm, publish the next. `SendAll` sends the whole batch first and waits
for all the confirms afterwards:

```go
orders := []OrderPlaced{
	{OrderID: "A-1", TotalCents: 4250},
	{OrderID: "A-2", TotalCents: 1999},
	{OrderID: "A-3", TotalCents: 7800},
}

results, err := pub.SendAll(ctx, orders)
if err != nil {
	log.Fatal(err)
}
for i, r := range results {
	log.Printf("%s went out as %s", orders[i].OrderID, r.MessageID)
}
```

`results` is always as long as the batch and always in payload order, whatever
order the broker answered in, so `results[i]` is `orders[i]`.

It is not atomic — AMQP has no such thing, and a library offering one would be
lying — so a batch can fail halfway. The error says how far it got:

```go
results, err := pub.SendAll(ctx, orders)

var failed *acemq.BatchPublishFailedError
if errors.As(err, &failed) {
	log.Printf("%d of %d confirmed", failed.Confirmed, failed.Total)
	for i, err := range failed.Errors {
		if err != nil {
			log.Printf("resend %s: %v", orders[i].OrderID, err)
		}
	}
}
```

More in [publishing a batch](publishing.md#publishing-a-batch).

## What came with the message

```go
log.Println(m.Envelope.ID)
log.Println(m.Envelope.CorrelationID)
log.Println(m.Envelope.Type)
log.Println(m.Envelope.Attempt)
log.Println(m.Envelope.FirstSeen)
```

Every message carries an [envelope](envelope.md) — identity, type, correlation,
causation, attempt count, origin, and when it was first published. You set none
of it. The defaults are a contract rather than a convenience: a Java or .NET
consumer reads exactly these fields off exactly these headers, and there are
fixtures generated by the Java implementation that prove it byte for byte.

## Point it at a real broker

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Two changes, and no third:

```go
import (
	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
```

The blank import is what registers the `amqp` and `amqps` schemes — the same
arrangement `database/sql` uses for its drivers, so a program that only talks to
`memory://` never compiles an AMQP client in. Leave it out and you get a clear
error rather than a mystery:

```
acemq: no transport is registered for "amqp"; known schemes are [memory]
```

Run it, then open <http://localhost:15672> (guest/guest) and watch the queue.

## Shutting down

```go
stop := make(chan os.Signal, 1)
signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
<-stop
```

with the `defer mq.Close()` from the top still in place. `Close` stops every
consumer, lets the handlers already running finish, settles their messages, and
only then releases the connection. A message being worked on is **finished and
acknowledged** rather than abandoned for the broker to hand to somebody else —
which would mean the work happened twice.

## The whole thing

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}

func main() {
	ctx := context.Background()

	mq, err := acemq.Connect(ctx, "memory://tutorial")
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	err = acemq.NewTopology().
		Exchange("orders", "topic").
		Queue("orders.placed").
		Binding("orders.placed", "orders", "order.placed").
		Apply(ctx, mq)
	if err != nil {
		log.Fatal(err)
	}

	sub, err := acemq.Consume(ctx, mq, "orders.placed",
		func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			log.Printf("got %s for %d (attempt %d)",
				m.Payload.OrderID, m.Payload.TotalCents, m.Envelope.Attempt)
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	pub := acemq.NewPublisher[OrderPlaced](mq, "orders", "order.placed",
		acemq.Mandatory[OrderPlaced]())

	results, err := pub.SendAll(ctx, []OrderPlaced{
		{OrderID: "A-1", TotalCents: 4250},
		{OrderID: "A-2", TotalCents: 1999},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published %d", len(results))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}
```

## What you have

A publisher, a consumer, a topology, and messages that confirmed. Next:
[surviving failure](tutorial-surviving-failure.md) — what happens when the
handler does not work.
