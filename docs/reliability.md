# Retries, redelivery and shutdown

## The attempt counter, and why it rides on the message

A broker requeues **the bytes it was given**. The `x-acemq-attempt` header on a
requeued message therefore still reads whatever the publisher wrote — 1 —
however many times the message has come back.

So a retry is not a requeue. The message is republished onto its own queue with
the header advanced, and the original is then acknowledged. The counter lives on
the message, which is the only place a fleet of consumers can share it: an
earlier version counted redeliveries in a map on the consumer, and that map is
per-process, unbounded, and empty again after the restart that the service which
had been failing was about to have.

`Envelope.Attempt` is that header. A consumer that has never seen the message
before reads it and carries on counting.

The cost is that a retry goes to the back of the queue rather than the front, so
it is no longer in order with its neighbours. For a message that has already
failed once, that is the better trade.

This is verified against a real RabbitMQ rather than only against the in-memory
transport, which is written to behave this way and on its own would prove only
that it matches its own design.

## Where a message goes when it is given up on

Republished, with the reason in `x-acemq-error`, and the original acknowledged
afterwards:

| | |
|---|---|
| `{queue}.dlq` | attempts exhausted, too old, rejected, or a fatal reason |
| `{queue}.parked` | never reached the handler — usually a body that would not decode |

Acknowledging a message that failed looks wrong and is what makes this reliable:
the message has already been safely republished somewhere else, so acknowledging
the original is removing the copy that has been dealt with. Rejecting it instead
would either requeue it into a hot loop or, with a dead-letter exchange on the
queue, send it somewhere this library did not choose and without the reason.

Declare both with the topology, or a consumer that gives up has nowhere to put
the message and falls back to rejecting it:

```go
acemq.NewTopology().Queue("orders").DeadLetters("orders")
```

That one call declares more than the two queues. It also declares the shared
`acemq.dlx` exchange, binds `orders.dlq` and `orders.parked` to it on their own
names, and declares `orders` itself with

| argument | value |
|---|---|
| `x-queue-type` | `quorum` |
| `x-dead-letter-exchange` | `acemq.dlx` |
| `x-dead-letter-routing-key` | `orders.dlq` |

which is the same table Java, .NET, Python and Ruby write. `orders.dlq` and
`orders.parked` are classic, with no `x-queue-type` at all — see
[quorum, classic and streams](topology.md#quorum-classic-and-streams). It has to be, because
it is part of the declaration of `orders`: two services consuming that queue both
declare it, and a disagreement about these arguments is answered with
`PRECONDITION_FAILED` — the second service cannot consume at all. The routing key
matters as much as the exchange, because a dead-lettered message keeps the key it
arrived under, and a message reaching `acemq.dlx` under `order.placed` matches no
binding and is dropped.

### Two paths, and why both

The broker-side route is a backstop, not a replacement for the republish above.
The library's own path carries the reason and chooses the destination; it runs
whenever a handler returns and the consumer decides. The broker's runs for what
the library never sees:

- a message expiring against the source queue's own `x-message-ttl`
- a message dropped because `x-max-length` was reached
- a rejection from something that is not this library — another service, a
  management console, a script

Without the arguments on the source queue, all three are discarded silently.
Both paths end in the same `{queue}.dlq`, which is the point: an operator draining
dead letters looks in one place regardless of which one put the message there.

A service that wants its own dead-letter exchange uses `acemq.DeadLetterTo` and
leaves `DeadLetters` alone. Asking for both on one queue is refused rather than
resolved — either answer would be a guess about which of two conflicting
instructions was meant.

## A policy

Without one, a message returned by `Retry` is republished at once with nothing to
slow it down, and the broker hands it straight back as fast as it can. That is
rarely what anybody wants for long.

```go
mq, err := acemq.Connect(ctx, url,
	acemq.WithRetry(acemq.ExponentialRetry(5, time.Second, time.Minute)))
```

| | |
|---|---|
| `acemq.NoRetry()` | one delivery, no more |
| `acemq.FixedRetry(attempts, delay)` | the same wait every time |
| `acemq.ExponentialRetry(attempts, initial, max)` | doubling, capped |

`MaxAttempts` counts the first delivery, so 3 means one try and two retries.

Per consumer instead:

```go
acemq.Consume(ctx, mq, "orders", handler,
	acemq.RetryWith(acemq.FixedRetry(3, 5*time.Second)))
```

### Seeing what a policy would do

```go
acemq.ExponentialRetry(4, time.Second, 3*time.Second).Schedule()
// [1s 2s 3s]
```

The un-jittered delays, which is what to look at when deciding whether a policy
is the one you meant.

## Jitter

Retries are spread by ±20% by default, in **both** directions: jitter that only
ever delays turns a thundering herd into a slower thundering herd. Messages that
failed together — because the same database was down — must not come back
together, or the retry becomes the second outage.

A wait spent in the broker is never jittered. A rung queue's time-to-live is
fixed when it is declared, so a moved delay would name a queue that is not there
— and the spread is free anyway, because each message's TTL starts when it
arrives rather than when the batch failed.

```go
acemq.FixedRetry(5, time.Second).WithJitter(0.5) // ±50%
acemq.FixedRetry(5, time.Second).WithJitter(0)   // exactly a second, every time
```

## Giving up on age

```go
acemq.ExponentialRetry(10, time.Second, time.Minute).
	GiveUpAfter(30 * time.Minute)
```

The bound that matters when something downstream has been down for an hour. The
attempts may still be there, but the twelfth attempt on an hour-old message is
rarely worth making, and the queue behind it is.

## Errors that will not improve

```go
return acemq.Retry(acemq.Fatal(errors.New("no customer on this order")))
```

Dead-lettered at once rather than after four more identical failures. `IsFatal`
looks through wrapping, so marking an error deep inside a call works:

```go
if o.CustomerID == "" {
	return acemq.Fatal(errors.New("no customer"))
}
```

A body that will not decode is treated the same way, and never reaches the
handler: the same bytes fail the same way every time.

## Where the waiting happens

A short wait is spent **in the consumer**. It holds the delivery, and so holds
one of the consumer's prefetch slots for the length of the delay: with
`Prefetch(20)` and twenty messages each waiting five seconds, that consumer does
nothing else for five seconds.

A long wait is spent **in the broker**, on a rung queue, and costs this process
nothing at all:

```
orders.retry.40s    ttl 40s   -> orders
orders.retry.80s    ttl 80s   -> orders
orders.retry.160s   ttl 160s  -> orders
```

The line between the two is `BrokerWaitThreshold`, thirty seconds by default.
Below it a wait lost to a restart costs seconds and a queue per rung is not worth
having. Above it the lost wait is the whole delay: a consumer sleeping on a
five-minute backoff that is restarted at minute one does not resume at minute
one, because the broker redelivers the unacknowledged message immediately and a
five-minute policy delivers in none.

```go
policy.WaitInBrokerFrom(10 * time.Second) // move the line
policy.WaitInBrokerFrom(0)                // no rungs at all; every wait is held here
```

Nothing consumes a rung. The time-to-live is the only thing that ever takes a
message out of one, and the message comes home one attempt further on, through
the rung's dead-letter target.

One queue per distinct delay, and never a per-message TTL: RabbitMQ expires
messages only from the head of a queue, so a single queue of per-message TTLs
lets a ten-minute wait at the front hold back every thirty-second one behind it.

Declare the rungs from the same policy the consumer runs, so the two lists cannot
drift apart:

```go
policy := acemq.ExponentialRetry(6, 10*time.Second, 0)

acemq.NewTopology().
	Queue("orders").
	DeadLetters("orders").
	Retries("orders", policy)
```

A rung is declared with exactly three arguments, which are a cross-language
contract rather than a preference — two services on the same queue declare the
same rung by name, so different arguments mean the second is refused with
`PRECONDITION_FAILED` and cannot consume at all:

| argument | value |
|---|---|
| `x-message-ttl` | the delay in milliseconds |
| `x-dead-letter-exchange` | `acemq.RetryExchange` |
| `x-dead-letter-routing-key` | the source queue |

Three, and no `x-queue-type`: a rung is a classic queue, and classic is the
absence of that argument in all five libraries. The source queue above it is
quorum, and a rung is not, because nothing consumes a rung and replicating a
queue whose whole purpose is to wait buys nothing.

A consumer whose rungs are missing still retries — it waits in the process
instead — and counts `acemq.MetricRungMissing` each time, because a topology that
declares the queue and forgets its rungs otherwise looks like it works.

## When the connection drops

On by default. Without it a dropped connection is the quietest failure there is:
the delivery channel closes, every consumer goroutine ends, and the `Consumer`
objects still look alive. The service consumes nothing, for ever, and says
nothing about it — you find out from a queue-depth graph.

The transport watches for the close, redials with a capped backoff, **redeclares
the topology it created** and reattaches every consumer. Redeclaring matters: a
broker that restarted has lost anything not durable, and a consumer reattached
to a queue that no longer exists receives nothing for ever, which looks exactly
like a quiet queue.

```go
transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{
	RecoveryDelay: time.Second,
	OnRecovery: func(e rabbitmq.RecoveryEvent) {
		log.Printf("acemq: %s", e)
	},
})
mq, err := acemq.NewConn(transport)
```

`OnRecovery` reports every loss and every attempt — `lost`, `retrying`,
`recovered`, `gave-up`, `blocked`, `unblocked`. Recovery nobody can see is only
half an improvement on dying quietly.

Verified against a real broker restart: the consumer resumed in eight seconds.

`rabbitmq.Config{WithoutRecovery: true}` turns it off, for where something
outside the process is expected to restart it.

### Messages in flight

Anything unacknowledged when the connection went is redelivered by the broker,
marked as a redelivery — so the attempt counter keeps counting and a message
that was already failing does not get a fresh set of attempts.

## When the broker runs out of room

RabbitMQ sends `connection.blocked` when it is low on memory or disk, and every
publish on that connection then blocks until it unblocks. A publisher that does
not know this looks exactly like one that has hung, and restarting the service
does not help.

Publishing while blocked returns an error instead:

```go
if acemq.IsBlocked(err) {
	// shed load, buffer, or fail the request
}
```

That is a deliberate choice: a service that knows can do something, where one
piling up goroutines against a broker that has already said it cannot take any
more can only get worse.

## Publisher confirms

On by default. Without them a `Send` that returns nil means the bytes reached
the socket — not that the broker has taken responsibility for them, and the
difference is only ever noticed after messages have been lost.

```go
result, err := pub.SendResult(ctx, order)
// result.Confirmed — the broker has it
// result.Routed    — it reached a queue (with Mandatory)
```

A broker that refuses a message is an error rather than a silent drop. Turning
them off is possible where losing a message costs less than the round trip:

```go
transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{WithoutConfirms: true})
mq, err := acemq.NewConn(transport)
```

`Confirmed` is then false, because nothing was promised and claiming otherwise
would be a lie that looks like a guarantee.

## Messages that reach no queue

An unroutable message is dropped by the broker, silently. The publisher
succeeds, the consumer waits, and nothing anywhere says why — usually because a
binding was never made.

```go
pub := acemq.NewPublisher[OrderPlaced](mq, "events", "order.placed",
	acemq.Mandatory[OrderPlaced]())
```

Now that is an error at the point of publishing:

```
acemq: message 7f3c… to exchange "events" with key "order.placed" reached no
queue (312 NO_ROUTE); the broker dropped it
```

It costs a round trip only when the message is actually unroutable. RabbitMQ
sends the return *before* the confirm for the same publish, which is what lets
this be reported synchronously rather than arriving later with nothing to
attach it to.

## Dead-lettering

A rejected message is dropped unless the queue sends it somewhere:

```go
err := mq.DeclareQueue(ctx, "orders",
	acemq.DeadLetterTo("orders-dead"))
```

Then declare `orders-dead` and read it. A dead-letter queue nobody reads is a
place messages go to be forgotten quietly rather than loudly, which is worse
than dropping them: it looks like nothing is wrong.

Messages arriving there carry `x-acemq-error`, and the envelope's full history —
the original `FirstSeen`, the correlation, the causation — so a replay can be
made deliberately. See [publishing](publishing.md) for `SendEnvelope`.

## Duplicates

Retries mean a message can be delivered more than once, so handlers should be
idempotent. `Envelope.ID` is stable across every redelivery of the same message
and is the natural key:

```go
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	inserted, err := db.InsertIfAbsent(ctx, m.Envelope.ID, m.Payload)
	if err != nil {
		return acemq.Retry(err)
	}
	if !inserted {
		// Seen before. Acknowledging is right: the work is done.
		return acemq.Accept()
	}
	return acemq.Accept()
}
```

Doing this in the same transaction as the work is what makes it hold. A separate
"have I seen this?" table written outside the transaction can be updated by a
process that then crashes before doing the work.

## Shutdown

```go
defer mq.Close()
```

Closes every consumer, waits for handlers already running, and then releases the
connection. A message being worked on is finished and acknowledged rather than
abandoned for the broker to hand to somebody else — which would mean the work
happened twice.

Wire it to a signal so a deployment does not cut messages in half:

```go
stop := make(chan os.Signal, 1)
signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
<-stop
mq.Close()
```

Give the process long enough to drain. A container killed nine seconds into a
ten-second handler leaves that message to be redone by somebody else.
