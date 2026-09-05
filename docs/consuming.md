# Consuming

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

`Consume` takes the connection rather than being a method on it, for the same
reason `NewPublisher` does: a method cannot have its own type parameter before
Go 1.27.

## The decision is the return value

| | |
|---|---|
| `acemq.Accept()` | it worked. The message is acknowledged and gone. |
| `acemq.Retry(err)` | try again, if the policy allows another attempt |
| `acemq.Reject(err)` | never try again. Dead-letter it. |

Returning the decision rather than calling a method means a handler that forgets
to decide does not compile. That matters more than it sounds: a message nobody
acknowledges is not lost and is not delivered either — it sits unacknowledged
until the connection drops, and then comes back, usually to the same handler,
with the same outcome.

### Retry or reject?

`Retry` is for the world being temporarily unhelpful: a database that is down, a
service returning 503, a lock somebody else holds. `Reject` is for the message
being the problem: a field that cannot be missing is missing, a reference points
at nothing. Retrying that one produces the identical failure five more times and
delays the dead-lettering that was always going to happen.

When you are inside a call and cannot see which it is, mark the error instead:

```go
func place(ctx context.Context, o OrderPlaced) error {
	if o.CustomerID == "" {
		return acemq.Fatal(errors.New("no customer"))
	}
	return db.Insert(ctx, o)
}
```

A `Retry` carrying a reason marked with `Fatal` is dead-lettered immediately.
The mark wins over the request, which is the point of having it. `Fatal` looks
through wrapping, so an error marked deep inside a call still reads as fatal
where the engine asks.

## What the handler receives

```go
type Message[T any] struct {
	Payload     T
	Envelope    Envelope
	RoutingKey  string
	ContentType string
	Redelivered bool
	Body        []byte
}
```

`Envelope.Attempt` is the count for this delivery, worked out by the engine; the
rest came off the wire. `Body` is there for a handler that wants the undecoded
bytes. See [the envelope](envelope.md).

## Concurrency

One message at a time by default, which keeps a queue's messages in order:

```go
sub, err := acemq.Consume(ctx, mq, "orders", handler,
	acemq.Concurrency(8))
```

Raising it gives up that order for throughput, and is the right trade for
handlers that spend their time waiting on something else. It is the wrong trade
where later messages about the same entity must not overtake earlier ones.

## Prefetch

How many unacknowledged messages the broker will hand over — twenty by default:

```go
acemq.Prefetch(100)
```

Too low and the consumer waits on the network between messages. Too high and one
consumer hoards a queue while its neighbours idle, and a restart returns a large
batch at once. Note that a [delayed retry](reliability.md) holds its slot while
it waits.

## Naming the consumer

```go
acemq.ConsumerTag("orders-worker-3")
```

This is what appears in RabbitMQ's management interface. Worth setting before
you need it, which will be while working out who is holding a message.

## A different codec

```go
acemq.ConsumeWith(myCodec)
```

See [codecs](serialization.md).

## When a handler panics

A panic is caught, the message is dead-lettered, and the consumer carries on. A
panic is a bug, and a bug repeats, so retrying would produce the same panic
until the attempts ran out — meanwhile the consumer would have died and every
other message with it.

## Stopping

```go
sub.Close()
```

Stops delivery and waits for handlers already running. A message being worked on
when `Close` is called is finished and acknowledged rather than abandoned for
the broker to hand to somebody else. Closing twice is not an error.

`mq.Close()` closes every consumer on the connection the same way, so a
deferred `mq.Close()` is usually all a program needs:

```go
mq, err := acemq.Connect(ctx, url)
if err != nil {
	return err
}
defer mq.Close()
```
