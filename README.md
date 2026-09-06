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

It is deliberately not more forgiving than RabbitMQ. A message returned for
retry comes back marked redelivered, exactly as a broker would return it — a
test transport that is kinder than the real one certifies code that then fails
in production.

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

## Retry, and the attempt counter

A broker requeues the bytes it was given, so the attempt header on the wire
still reads 1 however many times a message has come back. Counting that header
makes a retry limit that never trips and a message that goes round for ever.
The count comes from the broker's redelivery flag instead, kept per consumer and
keyed by message id, and there is a test against a real broker that proves it.

Delaying a retry holds the delivery, and so holds one of the consumer's prefetch
slots. That is the honest cost of a delay without a delay queue: the alternative
is to acknowledge and republish, which turns one message into two and loses the
redelivery flag the count depends on.

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
