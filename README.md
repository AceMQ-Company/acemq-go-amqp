# AceMQ for Go

[![ci](https://github.com/AceMQ-Company/acemq-go-amqp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-go-amqp/actions/workflows/ci.yml)
[![release](https://github.com/AceMQ-Company/acemq-go-amqp/actions/workflows/release.yml/badge.svg)](https://github.com/AceMQ-Company/acemq-go-amqp/actions/workflows/release.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-go-amqp/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-go-amqp/actions/workflows/attribution-guard.yml)
[![version](https://img.shields.io/badge/version-0.1.4-blue)](https://github.com/AceMQ-Company/acemq-go-amqp/releases)
[![reference](https://img.shields.io/badge/reference-pkg.go.dev-blue)](https://pkg.go.dev/github.com/AceMQ-Company/acemq-go-amqp/amqp)
[![docs](https://img.shields.io/badge/docs-acemq.org-blue)](https://acemq.org/acemq-go-amqp/)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.23%2B-00ADD8)](#what-is-in-the-box)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#what-is-in-the-box)

A Go client for AceMQ messaging over AMQP, speaking the same wire contract as
the [Java](https://github.com/AceMQ-Company/acemq-java-amqp) and
[.NET](https://github.com/AceMQ-Company/acemq-dotnet-amqp) libraries: the same
reserved headers, the same defaults, the same retry semantics. A Go consumer
reads what a Java producer writes, and fixtures generated from the Java
implementation pin that rather than leaving it to be discovered in production.
Two of them, carried byte for byte by all five libraries: one for the headers on
the wire, one for the retry schedule, the queue names and the topology. See
[how we know the five libraries agree](https://acemq.org/acemq-go-amqp/testing.html#how-we-know-the-five-libraries-agree).

```bash
go get github.com/AceMQ-Company/acemq-go-amqp
```

## Getting a message across

```go
import (
	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}

mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
	acemq.WithRetry(acemq.ExponentialRetry(5, time.Second, time.Minute)))
if err != nil {
	return err
}
defer mq.Close()

if err := mq.DeclareQueue(ctx, "orders"); err != nil {
	return err
}

pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
if err := pub.Send(ctx, OrderPlaced{OrderID: "o-1", TotalCents: 4250}); err != nil {
	return err
}

sub, err := acemq.Consume(ctx, mq, "orders",
	func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
		if err := place(ctx, m.Payload); err != nil {
			return acemq.Retry(err)
		}
		return acemq.Accept()
	})
defer sub.Close()
```

The blank import registers the `amqp` and `amqps` schemes. It is what keeps the
AMQP client out of programs that only use the in-memory transport.

`orders` there is a **durable quorum queue**, which is what a durable queue is
in all five AceMQ libraries. See [what type a queue is](#what-type-a-queue-is).

## Deciding what happens to a message

A handler returns its decision rather than calling a method, so a handler that
forgets to decide does not compile:

| | |
|---|---|
| `acemq.Accept()` | it worked; the message is gone |
| `acemq.Retry(err)` | try again, if the policy allows another attempt |
| `acemq.Reject(err)` | never try again; dead-letter it |

`acemq.Fatal(err)` marks a reason that retrying cannot fix. A `Retry` carrying a
fatal reason is dead-lettered immediately, because the remaining attempts would
all fail the same way.

## Testing without a broker

`memory://` is built in. Each distinct URL is a separate broker, so tests can
run in parallel without coordinating:

```go
mq, err := acemq.Connect(ctx, "memory://"+t.Name())
```

It is deliberately not more forgiving than RabbitMQ: a message returned to a
queue comes back marked redelivered, an unroutable mandatory publish is reported
as unroutable, and redeclaring a queue with different arguments is refused with
`PRECONDITION_FAILED` — a test transport that is kinder than the real one
certifies code that then fails in production. It does not expire messages from a
rung queue, though, because nothing in memory keeps time; the proof that a broker
wait works end to end is a test against a real broker.

## Why publishing and consuming are functions

`NewPublisher` and `Consume` take the connection as an argument instead of
hanging off it, because a method cannot have its own type parameter before Go
1.27:

```go
func (c *Conn) Publisher[T any](...)   // generic method: needs go1.27
func NewPublisher[T any](c *Conn, ...) // generic function: works everywhere
```

This module declares `go 1.23`, so it can be used by projects that have not
moved to the newest toolchain. For a library that matters more than mirroring
the exact shape of the Java and .NET APIs. `Publisher[T]` is a generic type with
ordinary methods, which has no such restriction, so once you have one it reads
the same as it does in the other languages.

## What is in the box

A package per concern, with nothing at the module root:

| Package | |
|---|---|
| `amqp/` | envelopes, codecs, publishing, consuming, retry, the in-memory transport. No dependencies outside the standard library. Named `acemq`. |
| `rabbitmq/` | the RabbitMQ transport, on `github.com/rabbitmq/amqp091-go`. |
| `security/` | TLS modes, trusted authorities, credentials. No dependencies either. |
| `patterns/` | request-reply, idempotency, outbox, ordering, pipelines, replay, routing slips, streams, consumer groups, schema registry, SQL-backed stores. |
| `actuator/` | metrics, health and info over HTTP, on the same paths as Java and .NET. |
| `crypto/` | encrypted message bodies, AES-GCM. Standard library only. |
| `codec/xml`, `codec/yaml`, `codec/toml`, `codec/protobuf`, `codec/avro` | one module each, so the core keeps its single dependency. |
| `devcerts/` | development certificates, behind `cmd/acemq-certs`. |

## Security

```go
mq, err := acemq.Connect(ctx, "amqps://broker:5671/",
	acemq.WithSecurity(security.Required().
		TrustCertificateAuthorityFile("/etc/acemq/ca.crt").
		WithCredentials(security.EnvironmentCredentials("MQ_USER", "MQ_PASSWORD"))))
```

Naming an authority replaces the system trust store rather than adding to it.
Credentials are read on every connection, so a rotated password is picked up,
and they override the URL so the password stays out of logs. Certificates
stamped `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` are refused on every path —
including `Insecure` — unless `AllowDevelopmentCertificates()` says otherwise.

Certificates for local work:

```bash
go install github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@latest
acemq-certs --out certs --broker localhost --days 30
```

Full detail in [the security guide](https://acemq.org/acemq-go-amqp/security.html).

## Connections that come back

A dropped connection is redialled with a capped backoff, the topology this
transport declared is redeclared, and every consumer is reattached. Without it a
dropped connection is the quietest failure there is: the delivery channel
closes, the consumer goroutines end, and the objects still look alive while the
service consumes nothing for ever.

```go
transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{
	OnRecovery: func(e rabbitmq.RecoveryEvent) { log.Printf("acemq: %s", e) },
})
```

Verified against a real broker restart.

## What type a queue is

A durable queue declared through `mq.DeclareQueue(...)` or `Topology.Queue(...)`
is a **quorum** queue. `x-queue-type` is compared by the broker as strictly as
any other argument, so this is a cross-language contract rather than a
preference: a Go service and a Java service consuming `orders` both declare
`orders`, and a disagreement about its type refuses the second one with
`PRECONDITION_FAILED` and leaves it unable to consume at all.

Quorum rather than classic because Java has declared source queues quorum since
before the other four libraries existed, and **a queue that already exists as
quorum cannot be redeclared as anything else**. Classic would have been an
equally good agreement to have reached first; it is no longer one that is
available.

Some queues stay classic, and none of them is an oversight:

| queue | why |
|---|---|
| `{queue}.retry.{delay}` | a rung is never consumed; Java declares every rung classic |
| `{queue}.dlq`, `{queue}.parked` | drained by a person, not consumed; classic in Java too |
| anything `Exclusive()`, `AutoDelete()` or `Transient()` | RabbitMQ allows a quorum queue to be none of the three, so these would be refused outright — this is what keeps a requester's generated reply queue working |
| anything that named its own type | `acemq.OfType(acemq.QueueClassic)`, `QueueStream`, or `x-queue-type` set by hand |

`acemq.OfType(acemq.QueueClassic)` is the way to ask for classic, and it sends
**no** `x-queue-type` argument at all — which is what classic looks like on the
wire in Java, .NET, Python and Ruby. A rung declared by any of them and
redeclared here produces the identical argument table, not merely an equivalent
one.

A queue that already exists as classic is **not** converted by any of this. The
broker refuses the declaration, because a queue's type cannot be changed after
it is created; drain it and recreate it, or declare it
`acemq.OfType(acemq.QueueClassic)`.

## Retry, and the attempt counter

A retry is **republished** onto the same queue with `x-acemq-attempt` advanced,
and the original acknowledged. It is not requeued. A requeue hands back the bytes
the broker was given, so the header would read 1 for ever and the count would
have to live in a map on the consumer — which is per-process, unbounded across a
fleet, and empty again after the restart the failing service was about to have.
Republishing puts the counter on the message, where every consumer in every
language can read it. The cost is that a retry goes to the back of the queue
rather than the front; for a message that has already failed once that is the
better trade.

Giving up works the same way. A message that runs out of attempts, grows too old,
or is rejected is republished to `{queue}.dlq` with the reason in
`x-acemq-error`, and only then is the original acknowledged — so the reason
survives and the destination is one this library chose. A body that will not
decode goes to `{queue}.parked` instead: a message that failed five times and a
message nothing could read are different problems, and whoever drains the dead
letters should not have to sort them by hand.

### The backstop underneath it

`DeadLetters` declares those two queues, binds them to the shared `acemq.dlx`
exchange on their own names, and declares the **source** queue with

| argument | value |
|---|---|
| `x-queue-type` | `quorum` — see [what type a queue is](#what-type-a-queue-is) |
| `x-dead-letter-exchange` | `acemq.dlx` (`DeadLetterExchange`) |
| `x-dead-letter-routing-key` | `{queue}.dlq` |

Those two arguments are part of the source queue's declaration, which is what
makes them a cross-language contract: a Go service and a Python service
consuming `orders` both declare `orders`, and a declaration that disagrees about
them is refused with `PRECONDITION_FAILED`, leaving whichever service started
second unable to consume at all.

The routing key has to be overridden as well as the exchange. A dead-lettered
message keeps the key it arrived under, so without it a message that reached
`orders` as `order.placed` would arrive at `acemq.dlx` as `order.placed`, match
no binding, and be dropped.

This does not replace the republish-with-the-reason path, and neither is dead
code. The broker-side route catches what the library never sees: a message
expiring against the source queue's own `x-message-ttl`, one dropped by
`x-max-length`, or a rejection from a consumer that is not this library. Without
it, those are discarded and nothing records that they existed.

`acemq.DeadLetterTo(exchange)` is the way out, for a service that wants its own
dead-letter exchange. It and `DeadLetters` are mutually exclusive on one queue:
a topology asking for both is refused rather than resolved, because either
answer would be a guess about which of two conflicting instructions was meant.

### Where the waiting happens

Short waits are spent in the consumer, holding the delivery and one prefetch
slot. Waits at or past `BrokerWaitThreshold` — thirty seconds by default — are
spent in the **broker**, on a `{queue}.retry.{delay}` rung whose `x-message-ttl`
is the wait and whose dead-letter target is the queue it came from. Nothing
consumes a rung; the time-to-live is the only thing that ever takes a message out
of one.

The threshold exists because a consumer sleeping through a five-minute backoff is
holding an unacknowledged message: restart it and the broker redelivers at once,
so a five-minute policy delivers in none. Below thirty seconds a lost wait costs
seconds and a queue per rung is not worth it; above it the lost wait is the whole
delay. `WaitInBrokerFrom(0)` turns rungs off entirely, for a service that may not
declare queues on its broker.

One queue per distinct delay, and **never** a per-message TTL: RabbitMQ expires
messages only from the head of a queue, so one queue of per-message TTLs lets a
ten-minute wait at the front hold back every thirty-second one behind it.

Declare the rungs with the topology, from the same policy the consumer runs:

```go
policy := acemq.ExponentialRetry(6, 10*time.Second, 0)

err := acemq.NewTopology().
	Queue("orders").
	DeadLetters("orders").   // orders.dlq and orders.parked
	Retries("orders", policy). // orders.retry.40s, .80s, .160s
	Apply(ctx, mq)
```

A rung is declared with exactly three arguments, and they are a cross-language
contract rather than a preference — two services on the same queue declare the
same rung by name, so different arguments mean the second is refused with
`PRECONDITION_FAILED` and cannot consume at all:

| argument | value |
|---|---|
| `x-message-ttl` | the delay in milliseconds |
| `x-dead-letter-exchange` | `acemq.retry` (`RetryExchange`) |
| `x-dead-letter-routing-key` | the source queue |

Three, and no `x-queue-type`: a rung is classic, and classic is the absence of
that argument in every one of the five libraries.

Java, .NET, Python, Ruby and this library all name `acemq.retry` here and bind
each source queue to it. Python and Ruby once dead-lettered a rung through the
default exchange instead, which routes by queue name and needs no binding; that
works, but two libraries cannot both be right about one queue. Both shared
exchange names live in one place — `RetryExchange` and `DeadLetterExchange` in
`amqp/retryladder.go` — with tests pinning what they say.

### The schedule

Exponential doubling with 20% jitter applied in **both** directions, because
jitter that only ever delays turns a thundering herd into a slower one. The
ceiling is applied inside the loop as well as after it, so a large multiplier
cannot run the delay towards overflow before the ceiling is reached. A policy can
also give up on age — `GiveUpAfter(time.Hour)` — which is the bound that matters
when a queue has been paused: attempts say nothing about how long a message has
been waiting.

A broker wait is never jittered. A rung's TTL is fixed at declaration, so a moved
delay would name a queue that is not there — and the spread is free anyway,
because each message's TTL starts when it arrives rather than when the batch
failed.

`Schedule()` is the policy without jitter, which is what to read when deciding
whether a policy is the one you meant: `ExponentialRetry(5, time.Second,
time.Minute)` is `[1s 2s 4s 8s]` in Go, Java, .NET, Python and Ruby alike.

### Replay

`patterns.Replay` puts a dead-letter queue back through the system, and resets
each message to attempt one unless `KeepAttempts` says otherwise. Without the
reset a message dead-lettered on the last attempt of a five-attempt policy is
dead-lettered again before any handler sees it, and the operator who has just
fixed the bug has moved two thousand messages from one queue to the same queue.

## Running the tests

```bash
go test ./...                    # everything that needs no broker
docker run -d -p 5672:5672 rabbitmq:4-alpine
ACEMQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/ go test ./...
```

The broker tests skip when `ACEMQ_TEST_AMQP_URL` is unset, so `go test ./...`
works on a machine without Docker. CI sets it, so skipping is not a way for them
to quietly stop running.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
