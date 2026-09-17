# 4. Seeing what happens

**20 minutes. Needs Docker.**

A message that fails silently is the whole problem. This is how to see the
traffic, and how to read it when something is wrong.

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## Nothing is measured until you ask

Go has no standard metrics interface, so there is nothing here to turn on by
importing it. The library counts things and hands them to an `Observer`:

```go
type Observer interface {
	Count(metric string, delta int64, labels map[string]string)
	Observe(metric string, seconds float64, labels map[string]string)
	Gauge(metric string, value int64, labels map[string]string)
}
```

An interface rather than a dependency, because picking Prometheus or
OpenTelemetry for you would put every user of this package on the same one. The
default is `acemq.NopObserver`, so a program that never reads metrics does not
pay for them — and a program that wants them has to say so:

```go
metrics := acemq.NewMetrics()

mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
	acemq.WithObserver(metrics))
if err != nil {
	log.Fatal(err)
}
defer mq.Close()
```

`acemq.Metrics` is a working implementation for when you only want the numbers.
For anything else — a real Prometheus registry, an OTel meter, a statsd client,
a log line — implement three methods over what you already use. Both methods must
be safe to call from several goroutines and must not block: they are called on
the path a message takes.

## Serve them

```go
import "github.com/AceMQ-Company/acemq-go-amqp/actuator"

act := actuator.New(actuator.Options{
	Metrics: metrics,
	Conn:    mq,
	Name:    "orders",
	Version: "1.0.0",
})
go func() {
	if err := http.ListenAndServe("127.0.0.1:9090", act); err != nil {
		log.Fatal(err)
	}
}()
```

Three paths, and they match the Java and .NET libraries exactly, so a scrape
configuration or a Kubernetes probe written for one works against another:

| | |
|---|---|
| `/acemq-metrics` | Prometheus text format |
| `/acemq-health` | JSON, **503** when anything is down |
| `/acemq-info` | version, and what the transport can do |

**`127.0.0.1` on purpose.** Nothing here checks who is asking, and health says
which dependencies are down while metrics say how much traffic there is — more
than an anonymous caller should learn about a service. Bind it to loopback, put
it on a port your ingress does not publish, or wrap the handler in your own
middleware. The library will not do it for you, and says so rather than implying
otherwise by shipping a token check nobody configures.

## Look at it

Publish twenty, dead-letter a few:

```go
orders := make([]OrderPlaced, 20)
for i := range orders {
	orders[i] = OrderPlaced{OrderID: fmt.Sprintf("A-%d", i), TotalCents: int64(1000 + i)}
}
if _, err := pub.SendAll(ctx, orders); err != nil {
	log.Fatal(err)
}
```

```bash
curl -s localhost:9090/acemq-metrics | grep -E '^acemq_(publish|consume)_total'
```

```
acemq_publish_total{exchange="",outcome="confirmed",routing_key="orders.placed"} 20
acemq_consume_total{outcome="acked",queue="orders.placed"} 16
acemq_consume_total{outcome="dead_lettered",queue="orders.placed"} 4
acemq_consume_in_flight{queue="orders.placed"} 0
```

Twenty published, sixteen handled, four given up on. The `outcome` label is what
makes that readable, and it is worth alerting on the difference between
`unroutable` and `failed`: one is a binding that was never declared and the other
is the broker or the network.

The labels differ between the two sides on purpose: a publish is tagged by where
it went, `exchange` and `routing.key`, and a consume by where it came from,
`queue`. Neither carries the message type, so a dashboard split by payload shape
needs the type in the queue or the routing key — which it usually already is.

The names are dotted in code and underscored when scraped —
`acemq.publish.duration` becomes `acemq_publish_duration_*`. **They are the
Java library's names**, so a dashboard written for one works against the other.
They all changed in 0.5.0 to make that true; if you have a dashboard from 0.3.0
see [the rename table](observability.md#metrics).

The Prometheus text is written directly rather than through a client library, so
the actuator needs nothing outside the standard library either. Durations are a
summary with a count and a sum — enough for a rate and an average — plus min and
max as gauges. No quantiles, because a quantile computed per process and then
averaged across a fleet is a number that means nothing.

If you only want the numbers in a test, skip the HTTP entirely:

```go
log.Println(metrics.Counts())
log.Println(metrics.Durations())
log.Println(metrics.Gauges())
```

## The outcome is the engine's decision, not the handler's request

This is the one thing worth understanding before reading any of it.

A handler returning `acemq.Retry` is making a **request**. The engine still looks
at the policy, and a message on its last attempt is dead-lettered instead. The
counter records what happened — `dead_lettered` — and not what was asked for.

Anything that counted the handler's return value would report a retry for a
message nobody will ever try again, which is exactly how a dashboard ends up
showing no dead letters while the dead-letter queue fills up.

## Health, for a probe

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:9090/acemq-health   # 200
curl -s localhost:9090/acemq-health
```

```json
{"status":"up","checked":"2026-09-17T09:12:44.463866Z",
 "parts":{"broker":{"status":"up","checked":"2026-09-17T09:12:44.46387Z",
 "parts":{"consumers":1,"roundTripMillis":0}}}}
```

Stop the broker and ask again: `503`, and `"status":"down"`. A Kubernetes probe
reads the status code without parsing anything. The statuses are lowercase —
`up`, `down`, `degraded` — and the connection reports itself under `broker`.

`degraded` answers **200**, deliberately. It is worth an alert and not worth
taking the instance out of rotation — a process that reports itself down gets
restarted, which loses whatever it was holding and fixes nothing.

Add your own checks, which are combined with the connection's:

```go
act := actuator.New(actuator.Options{
	Metrics: metrics,
	Conn:    mq,
	Checks:  []acemq.HealthCheck{databaseCheck{db}},
})
```

## Follow one message across services

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
```

```go
import "github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel"

tracing := otel.New()

mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
	acemq.WithObserver(metrics),
	acemq.WithPublishInterceptor(tracing.PublishInterceptor()))

orders := otel.NewPublisher[OrderPlaced](tracing, mq, "", "orders.placed")
if err := orders.Send(ctx, order); err != nil {
	log.Fatal(err)
}

sub, err := acemq.Consume(ctx, mq, "orders.placed",
	otel.Handle(tracing, "orders.placed", handle))
```

Three pieces, and each does one thing:

| | |
|---|---|
| `tracing.PublishInterceptor()` | writes `traceparent` onto **every** message published on the connection, including a `patterns.Requester`'s |
| `otel.NewPublisher` | opens a `PRODUCER` span around a send — the span is named after the destination, which is what a publisher knows and a call site would have to repeat |
| `otel.Handle` | opens a `CONSUMER` span around a handler, parented by the publish that caused it |

```
4bf92f3577b34da6 orders.placed publish   8.2ms
4bf92f3577b34da6 orders.placed process  41.7ms
```

**The same trace identifier on both.** The publish wrote `traceparent` into the
envelope and the consumer read it out, so the trace crosses the broker rather
than stopping at it. The parent comes out of **the message's own headers** rather
than out of whatever the delivery goroutine happened to be doing — those are two
different traces, minutes and machines apart, and joining them is the one thing a
messaging system needs from tracing that an HTTP client does not.

It crosses languages too: a Go consumer of a Java publisher's message joins the
same trace, because all five libraries read the same two headers. `traceparent`
is deliberately **not** `x-acemq-` prefixed — it is the W3C name that every other
tracing tool already knows, and renaming it would make this library's traces
invisible to all of them.

`otel.New()` takes the process's tracer provider, so **nothing is emitted until
the application configures an SDK**. The exporter and the sampler are yours; this
module only says what happened. It is a module of its own, so
`go.opentelemetry.io/otel` never becomes a dependency of the core.

### The engine has the last word on the span too

`otel.Handle` does not end the span when the handler returns. It hands the ending
to the engine through `acemq.OnSettled`, and the span is closed where the
settlement is decided — with the same word the counter used.

That is the same rule as the metrics, enforced the same way, and it means a
dashboard filtered to dead-lettered messages and a trace search for the same
thing cannot return different sets. A span ended when the handler returned would
say `retried` for a message that was actually dead-lettered, which is precisely
what somebody searching a trace backend for dead letters fails to find.

## Reading it when something is wrong

| What you see | What it usually means |
|---|---|
| `acemq_publish_total{outcome="unroutable"}` climbing | a binding was never declared, or a routing key has a typo |
| `acemq_publish_total{outcome="published"}` rather than `confirmed` | confirms are off; nothing has been promised about those messages |
| `acemq_messages_retried_total` climbing steadily | a dependency is flapping; the retry reasons are on the spans |
| `acemq_messages_dead_lettered_total` climbing | a retry policy is running out — usually something that is down |
| `acemq_consume_total{outcome="parked"}` climbing | messages nothing could read — a schema change or a bad deploy; look in `{queue}.parked` |
| `acemq_retry_rung_missing` above zero | a rung queue was never declared; the `rung` label names the one to declare |
| `acemq_consume_in_flight` at the prefetch and flat | handlers are stuck, not slow |
| `acemq_messages_set_aside_failed` above zero | a dead letter could not be republished. **This is the one that loses messages** |
| `acemq_outbox_lag` growing | the relay has stopped; nothing errors and the database fills |
| `/acemq-health` 503 | the connection is down |

## What you have

Metrics a dashboard can read, health a probe can read, and traces that cross the
broker and the language boundary. That is the set — the
[guide](observability.md) has the detail on each, and the
[interceptors page](interceptors.md) explains why the tracing hook is a publish
interceptor rather than something built in.
