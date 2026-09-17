# 2. Surviving failure

**20 minutes. Needs Docker.**

Everything so far assumed the handler works. This is about when it does not.

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

(For TLS instead of plaintext, the `acemq-certs` command generates everything a
broker needs — see [security](security.md).)

## Declare somewhere for failures to go

```go
import (
	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
if err != nil {
	log.Fatal(err)
}
defer mq.Close()

err = acemq.NewTopology().
	Queue("orders.placed").
	DeadLetters("orders.placed").
	Apply(ctx, mq)
if err != nil {
	log.Fatal(err)
}
```

`DeadLetters` declares four things, and they are only correct together: the
shared `acemq.dlx` exchange, `orders.placed.dlq` and `orders.placed.parked` bound
to it on their own names, and the `x-dead-letter-exchange` and
`x-dead-letter-routing-key` arguments on `orders.placed` itself pointing at it.

Wire them by hand and forget one, and a rejected message is **discarded
silently**: the broker nacks it, finds no dead-letter exchange, and drops it.
Nothing reports that.

Look at <http://localhost:15672/#/queues> — three queues where you declared one.

**Two queues, because there are two problems.** `orders.placed.dlq` is where a
message goes when it was tried and failed; `orders.placed.parked` is where one
goes when nothing could read it — a body that would not decode, or a handler that
said so. One is usually the world and the other is usually a producer, and
whoever drains them should not have to sort the two apart by hand.

## Fail on purpose

```go
sub, err := acemq.Consume(ctx, mq, "orders.placed",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		log.Printf("attempt %d for %s", m.Envelope.Attempt, m.Payload.OrderID)
		return acemq.Retry(errors.New("the pricing service is down"))
	})
if err != nil {
	log.Fatal(err)
}
defer sub.Close()

pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders.placed")
if err := pub.Send(ctx, OrderPlaced{OrderID: "A-1", TotalCents: 4250}); err != nil {
	log.Fatal(err)
}
time.Sleep(30 * time.Second)
```

```
attempt 1 for A-1
attempt 2 for A-1
attempt 3 for A-1
...
```

Forever, and as fast as the broker can manage it. With no policy configured a
message asking to be retried is sent straight back with nothing to stop it. That
consumer is now occupied by one message that will never succeed.

Notice that `attempt` climbs. It comes off the message rather than out of a map
in this process, because a retry here **republishes** the message with the
counter advanced rather than requeueing it — a requeue hands the broker back the
bytes it already had, so a counter kept in the header would never move, and one
kept in memory would be empty again after the restart that the failing service is
about to have.

## Bound it

```go
mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
	acemq.WithRetry(acemq.ExponentialRetry(3, time.Second, 10*time.Second)))
```

```
attempt 1 for A-1
attempt 2 for A-1
attempt 3 for A-1
```

Then it stops, and the message is in `orders.placed.dlq` with the reason on it.
Check the management interface: one message.

```go
n, err := mq.MessageCount(ctx, "orders.placed.dlq")
log.Printf("%d dead letters", n)
```

The policy belongs to the connection, so every consumer on it gets the same one.
`acemq.RetryWith(policy)` overrides it for a single consumer, which is what to do
for the one queue whose work is worth ten attempts.

**Jitter is on by default.** The waits are not exactly 1s and 2s; they are spread
by ±20%. Without that, every consumer that failed at the same moment retries at
the same moment, and a dependency that was struggling gets the whole herd at
once.

Ask a policy what it will do before trusting it:

```go
policy := acemq.ExponentialRetry(5, time.Second, time.Minute)
log.Println(policy)
// RetryPolicy[attempts=5, initial=1s, x2, max=1m0s, jitter=0.2, broker=30s]
log.Println(policy.Schedule())
// [1s 2s 4s 8s]
```

`Schedule` is the waits it will actually use, jitter aside, and it is four
entries for five attempts because the last attempt is not followed by a wait.

### Where the waiting happens

A short wait is spent in this process, holding one of the consumer's prefetch
slots. A long one is not: past a threshold the message goes onto a **rung queue**
— `orders.placed.retry.30s` and friends — with an `x-message-ttl` and a
dead-letter target pointing back at the source, so the broker holds it and
returns it when the time is up. Ten minutes of waiting in a Go process is ten
minutes of a prefetch slot doing nothing; ten minutes on a rung costs nothing at
all.

The rungs have to exist:

```go
policy := acemq.ExponentialRetry(8, time.Second, 10*time.Minute)

err := acemq.NewTopology().
	Queue("orders.placed").
	DeadLetters("orders.placed").
	Retries("orders.placed", policy).
	Apply(ctx, mq)
```

A consumer whose rungs are missing still retries — it waits in the process
instead — and counts `acemq.retry.rung.missing` each time, with the rung it
wanted as a label. That metric above zero is a topology call somebody has not
made. More in [retries and redelivery](reliability.md#where-the-waiting-happens).

## Say what you mean instead of always retrying

An error on its own cannot tell "try again shortly" from "this will never work".
The return value can, and this is the reason it is a return value:

```go
func handle(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	switch quote, err := price(ctx, m.Payload); {
	case err == nil:
		_ = quote
		return acemq.Accept()
	case errors.Is(err, errNoSuchItem):
		return acemq.Reject(err)          // never works; keep the evidence
	case errors.Is(err, errUnreachable):
		return acemq.Retry(err)           // the world; try again
	default:
		return acemq.Park(err)            // nobody could read it
	}
}
```

Retrying the second case for ever is how one bad message becomes an outage.

There is a third way to say it, for a failure deep inside the work where
returning the right `Ack` would mean threading a decision back up through five
functions:

```go
return acemq.Retry(acemq.Fatalf("no such item %q", m.Payload.OrderID))
```

`acemq.Fatal` marks the reason rather than the request, and the engine honours
the mark over the request: the message is dead-lettered rather than retried. That
is the point of having it — a library function three layers down can say "this
will not improve" without knowing anything about messaging.

## Fill the dead-letter queue, then look at it

```go
orders := make([]OrderPlaced, 20)
for i := range orders {
	orders[i] = OrderPlaced{OrderID: fmt.Sprintf("A-%d", i), TotalCents: int64(1000 + i)}
}

results, err := pub.SendAll(ctx, orders)
if err != nil {
	log.Fatal(err)
}
log.Printf("published %d", len(results))
```

`SendAll` rather than a loop: every message goes out before any confirm is
awaited, instead of a full broker round trip each. Wait for the retries to run
out, and all twenty are in `orders.placed.dlq`.

Read one without starting a consumer:

```go
pulled, payload, found, err := acemq.PullInto[OrderPlaced](ctx, mq, "orders.placed.dlq")
if err != nil {
	log.Fatal(err)
}
if found {
	log.Printf("%s failed: %s", payload.OrderID, pulled.Envelope.Error)
	if err := pulled.Ack(); err != nil {
		log.Fatal(err)
	}
}
```

```
A-0 failed: exhausted 3 attempts: the pricing service is down
```

The reason is a field on the envelope rather than a header you have to know the
name of. `Pull` is the right shape for a tool that drains a queue and the wrong
one for ordinary work — it costs a round trip per message whether one is there or
not.

## Fix the cause, replay the messages

The pricing service is back. The messages are still in the dead-letter queue:

```go
import "github.com/AceMQ-Company/acemq-go-amqp/patterns"

result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
	Queue: "orders.placed.dlq",
	Limit: 500,
})
if err != nil {
	log.Fatal(err)
}
log.Println(result)   // moved 20, skipped 0 (drained)
```

With no `Exchange` set they go back through the default exchange under their own
routing key, which is `orders.placed` — the queue the dead-letter queue is named
after. Each one is stamped `acemq-replayed-from`, `acemq-replayed-at` and
`acemq-replay-count`, which reach your handler on `m.Envelope.Headers` because
they are deliberately outside the reserved `x-acemq-` namespace that the engine
strips on the way in.

**A replayed message goes back on attempt one** with its dead-letter reason
cleared, unless `KeepAttempts` says otherwise. It has to: a message dead-lettered
on the last attempt of a three-attempt policy would otherwise be dead-lettered
again before any handler saw it, and the operator who has just fixed the bug
would have moved two thousand messages from one queue to the same queue.

Replay only some of them:

```go
result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
	Queue: "orders.placed.dlq",
	Limit: 100,
	Filter: func(env acemq.Envelope, _ []byte) bool {
		return strings.Contains(env.Error, "pricing")
	},
})
```

What the filter rejects goes **back**, not away. Losing the rest as a side effect
of looking at them would be a poor trade.

**Set a `Limit`.** Without one, a replay against a queue somebody is still
writing to may not stop. `result.Reason` says which of `drained`, `limit`,
`deadline` or `cancelled` ended it, and "moved 500" means something quite
different when the limit was 500.

## Shut down without losing work

Stop the process mid-handler and the message comes back — but any side effect it
already applied has happened twice by the time it does.

```go
stop := make(chan os.Signal, 1)
signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
<-stop

log.Println("draining")
if err := mq.Close(); err != nil {
	log.Printf("close: %v", err)
}
log.Println("stopped")
```

`Close` stops every consumer, waits for the handlers already running to return,
settles their messages, and only then releases the connection. That order is the
whole point: a settlement travels on the channel its delivery arrived on, so
releasing the connection first would acknowledge every in-flight message into a
channel that has gone. The broker would hear nothing, hand the messages to
somebody else, and one that had already been republished for its next attempt
would now be on the queue twice.

There is no timeout on it, and no `Drain(30 * time.Second)` that gives up. A
handler that never returns holds `Close` open for as long as it runs, so **give
the process long enough to drain** — a container killed nine seconds into a
ten-second handler leaves that message to be redone by somebody else. Bound the
work rather than the shutdown:

```go
handler := patterns.Chain(handle,
	patterns.WithTimeout[OrderPlaced](10*time.Second))
```

## What you have

Bounded retries that do not block a consumer, dead-letter queues that exist, a
way to put messages back, and a clean shutdown. Next:
[never processing twice](tutorial-exactly-once.md).
