# Getting started

## Install it

```bash
go get github.com/AceMQ-Company/acemq-go-amqp
```

The module needs Go 1.23 or newer.

## Two imports

```go
import (
	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)
```

The named import is the library. The blank one registers the `amqp` and `amqps`
URL schemes with it — without it, `Connect` will tell you exactly that:

```
acemq: no transport is registered for "amqp"; known schemes are [memory].
For a broker, add the blank import
_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
```

Keeping the transport behind an import is what stops a program that only uses
`memory://` from linking an AMQP client it never calls.

## Connect

```go
mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
if err != nil {
	return err
}
defer mq.Close()
```

`Close` stops every consumer on the connection first and lets handlers already
running finish, so a message being worked on is acknowledged rather than
returned to the queue for somebody else to redo.

Options go on the end:

```go
mq, err := acemq.Connect(ctx, url,
	acemq.WithOrigin("orders@"+hostname),
	acemq.WithRetry(acemq.ExponentialRetry(5, time.Second, time.Minute)),
	acemq.WithPrefetch(50))
```

`WithOrigin` is worth setting. The default is `acemq@{hostname}`, which names
the machine but not the service, and the first time you are reading a
dead-letter queue at three in the morning you will want to know which service
published the thing.

## Declare what you use

```go
if err := mq.DeclareQueue(ctx, "orders"); err != nil {
	return err
}
```

Durable by default: the queue survives a broker restart. Declaring is idempotent
— unless the queue already exists with *different* settings, in which case
RabbitMQ refuses with `PRECONDITION_FAILED` and that refusal is passed straight
on. It means the code and the broker disagree about what the queue is, which is
worth stopping for rather than papering over.

See [exchanges, queues and bindings](topology.md) for the rest.

## Define a message

```go
type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}
```

**Tag the fields.** The tag is what makes a Go field and a Java field the same
field. Without one the name goes on the wire capitalised — `OrderID` rather than
`orderId` — and the Java consumer reads nothing.

## Publish

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")

if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250}); err != nil {
	return err
}
```

The empty exchange is the default exchange, which routes to the queue whose name
matches the routing key. A publisher is cheap to keep and safe for concurrent
use: build one per message type at start-up, not one per message.

More in [publishing](publishing.md).

## Consume

```go
sub, err := acemq.Consume(ctx, mq, "orders",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		if err := place(ctx, m.Payload); err != nil {
			return acemq.Retry(err)
		}
		return acemq.Accept()
	})
if err != nil {
	return err
}
defer sub.Close()
```

The handler returns its decision rather than calling a method, so a handler that
forgets to decide does not compile. A message nobody acknowledges sits
unacknowledged until the connection drops, and then comes back — usually to the
same handler, with the same outcome.

More in [consuming](consuming.md).

## Run it without a broker

Every URL beginning `memory://` is an in-process broker, and each distinct URL
is a separate one:

```go
mq, err := acemq.Connect(ctx, "memory://"+t.Name())
```

Nothing else in the code changes. See [testing](testing.md).

## A whole program

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}

func main() {
	ctx := context.Background()

	mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
		acemq.WithOrigin("orders@example"),
		acemq.WithRetry(acemq.ExponentialRetry(5, time.Second, time.Minute)))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		log.Fatal(err)
	}

	sub, err := acemq.Consume(ctx, mq, "orders",
		func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			log.Printf("order %s, attempt %d", m.Payload.OrderID, m.Envelope.Attempt)
			return acemq.Accept()
		})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
	if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250}); err != nil {
		log.Fatal(err)
	}

	// Wait for a signal, then let the deferred Close drain what is in flight.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}
```

A broker to point it at:

```bash
docker run -d -p 5672:5672 rabbitmq:4-alpine
```
