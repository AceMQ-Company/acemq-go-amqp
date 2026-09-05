# Exchanges, queues and bindings

## Queues

```go
if err := mq.DeclareQueue(ctx, "orders"); err != nil {
	return err
}
```

Durable by default. Options:

| | |
|---|---|
| `acemq.Transient()` | does not survive a broker restart |
| `acemq.AutoDelete()` | removed when its last consumer goes away |
| `acemq.Exclusive()` | only this connection can use it |
| `acemq.DeadLetterTo(exchange)` | where rejected messages go |
| `acemq.QueueArg(name, value)` | any other broker argument |

```go
err := mq.DeclareQueue(ctx, "orders",
	acemq.DeadLetterTo("orders-dead"),
	acemq.QueueArg("x-max-length", 100000))
```

### Redeclaring

Declaring is idempotent while the settings match. When they do not, RabbitMQ
refuses:

```
acemq: cannot declare queue "orders": Exception (406) Reason: "PRECONDITION_FAILED
- inequivalent arg 'durable' for queue 'orders'"
```

That refusal is passed straight on rather than swallowed. It means the code and
the broker disagree about what the queue is — usually because somebody changed
the declaration without migrating the queue — and carrying on regardless would
leave a service running against a queue it does not think it has.

It also kills the channel it happened on, which is why the error surfaces
immediately rather than at the next publish.

## Exchanges

```go
if err := mq.DeclareExchange(ctx, "events", "topic"); err != nil {
	return err
}
```

Durable by default; `acemq.TransientExchange()` and `acemq.ExchangeArg(...)`
adjust it. The kind is `direct`, `topic`, `fanout` or `headers`.

## Bindings

```go
if err := mq.Bind(ctx, "eu-orders", "events", "orders.*.eu"); err != nil {
	return err
}
```

Both the queue and the exchange must already be declared.

## The default exchange

Publishing with an empty exchange name uses the default exchange, which routes
to the queue whose name matches the routing key. No exchange, no binding:

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
```

Right for a work queue. Wrong as soon as a second consumer wants the same
messages, because it can only ever reach one queue.

## Topic patterns

`*` is exactly one word. `#` is zero or more.

| Pattern | Matches | Does not match |
|---|---|---|
| `orders.created` | `orders.created` | `orders.updated` |
| `orders.*` | `orders.created` | `orders.created.eu` |
| `orders.#` | `orders`, `orders.created`, `orders.created.eu` | `shipments.created` |
| `orders.*.eu` | `orders.created.eu` | `orders.eu` |
| `orders.#.eu` | `orders.eu`, `orders.created.paid.eu` | `orders.created.us` |
| `#` | everything, including the empty key | |

Note `orders.#` matching the bare word `orders`: a trailing `#` matches nothing
at all, which is the case most implementations get wrong.

The in-memory transport matches word by word rather than by translating the
pattern into a regular expression. The translation looks shorter and is wrong on
exactly the patterns nobody writes until they do — a trailing `#`, a `#` beside
a `*`, a bare `#`. Twenty-six cases pin it, and a test against a real broker
checks that RabbitMQ agrees; if the two disagreed, the in-memory transport would
be certifying behaviour that does not happen.

## Fanout

Every bound queue gets a copy and the routing key is ignored:

```go
if err := mq.DeclareExchange(ctx, "broadcast", "fanout"); err != nil {
	return err
}
for _, q := range []string{"audit", "search-index"} {
	if err := mq.DeclareQueue(ctx, q); err != nil {
		return err
	}
	if err := mq.Bind(ctx, q, "broadcast", ""); err != nil {
		return err
	}
}
```

## A shape that usually works

One topic exchange per bounded context, one queue per consuming service, bound
to the patterns it cares about:

```go
if err := mq.DeclareExchange(ctx, "orders-events", "topic"); err != nil {
	return err
}

if err := mq.DeclareQueue(ctx, "shipping-orders",
	acemq.DeadLetterTo("shipping-dead")); err != nil {
	return err
}
if err := mq.Bind(ctx, "shipping-orders", "orders-events", "order.placed"); err != nil {
	return err
}
if err := mq.Bind(ctx, "shipping-orders", "orders-events", "order.cancelled"); err != nil {
	return err
}
```

The publisher names an event, not a destination. A new consumer is a new queue
and a new binding, and the publisher never learns about it.

## Where to declare

At start-up, in the service that owns the queue, before consuming from it.
Declaring on every publish costs a round trip; declaring nowhere means the first
deployment to a fresh broker fails. Consumers declare their own queues and
bindings; publishers declare the exchange they publish to.
