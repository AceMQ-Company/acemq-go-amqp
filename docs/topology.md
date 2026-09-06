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

## Declaring it all at once

Call-at-a-time works, and stops working the moment somebody needs to know what
a service will do to a broker *before* it does it.

```go
topology := acemq.NewTopology().
	Exchange("orders-events", "topic").
	Queue("shipping-orders", acemq.DeadLetterTo("shipping-dead")).
	Queue("shipping-dead").
	Binding("shipping-orders", "orders-events", "order.placed").
	Binding("shipping-orders", "orders-events", "order.cancelled")

if err := topology.Apply(ctx, mq); err != nil {
	return err
}
```

Exchanges, then queues, then bindings — the order a broker needs.

### It is checked before the broker sees it

A binding naming a queue the topology does not declare is refused here rather
than by the broker:

```
acemq: binding orders-events -> a-queue-nobody-declared (order.placed) names
queue "a-queue-nobody-declared", which this topology does not declare
```

The broker would have accepted it, whenever that queue happened to exist
already — and the service would then depend on something nothing declares,
until a fresh environment where it fails at start-up for reasons nobody can see.

### Reading it before applying it

```go
fmt.Println(topology)
```

```
Topology: 1 exchanges, 2 queues, 2 bindings
  declare exchange orders-events (topic)
  declare queue shipping-orders (durable, x-dead-letter-exchange=shipping-dead)
  declare queue shipping-dead (durable)
  declare binding shipping-orders (from orders-events on order.placed)
  declare binding shipping-orders (from orders-events on order.cancelled)
```

A deployment that changes a broker should be something somebody can read first.
This is a statement of intent, not a diff against the live broker — AMQP cannot
enumerate what is there without the management API, and a plan that quietly
guessed would be worse than one honest about what it is.

### Reading it against the broker it will change

`ApplyWith` takes a mode. `DryRun` asks the broker about each queue and changes
nothing:

```go
plan, err := topology.ApplyWith(ctx, mq, acemq.DryRun)
for _, action := range plan {
	log.Println(action)
}
```

```
exchange orders-events: topic — unknown, AMQP cannot report exchanges
queue shipping-orders: durable, x-dead-letter-exchange=shipping-dead — differs:
  the broker refused the declaration: Exception (406) PRECONDITION_FAILED
queue shipping-dead: durable — would create
binding shipping-orders: from orders-events on order.placed — unknown, AMQP
  cannot report bindings
```

Three things are worth knowing about that output:

- **Queues are asked about, not declared.** A passive declare answers
  `NOT_FOUND` for a queue that is not there rather than creating it, so a dry
  run never creates what it describes. There is a test against a real broker
  that the queue is still missing afterwards.
- **Exchanges and bindings say "unknown".** AMQP has no way to read them back,
  and "would create" about something that already exists is the kind of
  plausible-looking output that stops being read.
- **`Declare` is the ordinary mode**, and `Apply` is the same thing without the
  plan.

A transport that cannot be asked — a custom one that implements neither
`DriftChecker` nor `QueueInspector` — reports every queue as unknown rather than
pretending.

## Drift

A service and its broker disagreeing about a queue is the failure that shows up
as messages going somewhere nobody is looking: a dead-letter exchange that was
changed, a durability that was not.

```go
reports, err := topology.Check(ctx, mq)
for _, r := range reports {
	log.Printf("drift: %s", r)
}
```

```
drift: queue shipping-orders: the broker refused the declaration:
Exception (406) PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange'
```

`PRECONDITION_FAILED` is the only way AMQP will report this without the
management API. Two consequences worth knowing:

- **A failed declaration kills its channel**, so the check uses a channel of its
  own and throws it away. Doing it on the shared publishing channel would take
  the connection's publishing down with a question. There is a test that the
  connection still works afterwards.
- **A queue that is not there is passed over**, not declared. A queue that does
  not exist cannot disagree with anything, and a check that created one as a
  side effect of asking would not be safe to run against a broker somebody is
  only inspecting.

## Where to declare

At start-up, in the service that owns the queue, before consuming from it.
Declaring on every publish costs a round trip; declaring nowhere means the first
deployment to a fresh broker fails. Consumers declare their own queues and
bindings; publishers declare the exchange they publish to.
