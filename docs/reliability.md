# Retries, redelivery and shutdown

## The attempt counter, and why it is not the header

A broker requeues **the bytes it was given**. The `x-acemq-attempt` header on a
redelivered message therefore still reads whatever the publisher wrote — 1 —
however many times the message has come back.

Counting that header gives a retry limit that never trips. The message goes
round for ever, and the symptom shows up as a queue that will not drain rather
than as anything that looks like a bug in retrying.

So the count comes from the broker's redelivery flag, kept per consumer and
keyed by message id. `Envelope.Attempt` is that count, not the header.

This is verified against a real RabbitMQ rather than only against the in-memory
transport, which is written to behave this way and on its own would prove only
that it matches its own design.

## A policy

Without one, a message returned by `Retry` is simply requeued and the broker
hands it straight back, as fast as it can. That is rarely what anybody wants for
long.

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

Retries are spread by ±20% by default. Messages that failed together — because
the same database was down — must not come back together, or the retry becomes
the second outage.

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

## The cost of a delayed retry

Waiting before a retry **holds the delivery**, and so holds one of the
consumer's prefetch slots for the length of the delay. With
`Prefetch(20)` and twenty messages each waiting a minute, that consumer does
nothing else for a minute.

The alternative — acknowledge and republish later — turns one message into two
and loses the redelivery flag the attempt count depends on. Neither is free.
This library takes the honest cost rather than the invisible one, and says so
here so it can be planned for: use short delays with a high attempt count, or
raise the prefetch, or move long waits to a delay queue in the broker.

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
