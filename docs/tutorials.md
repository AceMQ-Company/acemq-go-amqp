# Tutorials

Step by step, in order, each one ending with something that runs.

The [guide](index.md) explains how a thing works and why it is that way. These
are the other shape: start with nothing, finish with a working service, and
understand what you typed by the end rather than before the beginning.

| | | |
|---|---|---|
| 1 | [Your first message](tutorial-first-message.md) | Connect, declare, publish, consume. No broker needed | 10 min |
| 2 | [Surviving failure](tutorial-surviving-failure.md) | Retries that do not block, dead letters, and replaying them | 20 min |
| 3 | [Never processing twice](tutorial-exactly-once.md) | Idempotency, the outbox, and why "exactly once" is a lie | 25 min |
| 4 | [Seeing what happens](tutorial-observability.md) | Metrics and traces, and reading them when something is wrong | 20 min |

Each builds on the one before it, and each is a single `main.go` you can paste
into a scratch module. Nothing is left as an exercise. The four teach the same
four things in every AceMQ library, so a team moving between Go, Java, .NET,
Python and Ruby learns the idea once.

## Before you start

```bash
mkdir orders && cd orders
go mod init example.com/orders
go get github.com/AceMQ-Company/acemq-go-amqp
```

Go 1.23 or newer. The module has no dependencies outside the standard library
except in its subpackages, so that one `go get` is the whole install.

Tutorial 1 needs nothing else at all — it runs against the in-process broker
behind a `memory://` URL, which routes the way RabbitMQ routes. Tutorials 2, 3
and 4 use Docker, because what they teach is about how a broker behaves and an
in-memory one would be teaching you a simplification:

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Tutorial 3 also uses SQLite, and tutorial 4 adds the OpenTelemetry module:

```bash
go get modernc.org/sqlite
go get github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
```

## Two imports, every time

```go
import (
	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)
```

The package in `amqp/` is named `acemq`, and the alias makes that explicit at the
import rather than surprising at the call site. The blank import registers the
`amqp` and `amqps` URL schemes — the arrangement `database/sql` uses for its
drivers, so a program that only ever talks to `memory://` never links an AMQP
client in. Forget it and `acemq.Connect` says so:

```
acemq: no transport is registered for "amqp"; known schemes are [memory]. For a
broker, add the blank import _ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
```

## If you would rather read code

The [examples repository](https://github.com/AceMQ-Company/acemq-go-amqp-examples)
holds runnable programs, each verified on every commit. Tutorials teach; examples
demonstrate. Start here, go there when you want to see a whole system rather than
one idea.

## The other libraries

The same four tutorials exist for [Java](https://acemq.org/) and
[.NET](https://acemq.org/acemq-dotnet-amqp/), numbered the same way and teaching
the same things. Where this library differs from those two — errors as values
rather than exceptions, a context as the first argument, functions where a
generic method would be needed, options instead of builders — the tutorials say
so rather than pretending the four are translations of each other.
