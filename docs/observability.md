# Metrics, tracing and health

## Metrics

Go has no standard metrics interface, so this library counts things itself and
hands them to whatever you use:

```go
type Observer interface {
	Count(metric string, delta int64, labels map[string]string)
	Observe(metric string, seconds float64, labels map[string]string)
	Gauge(metric string, value int64, labels map[string]string)
}
```

An interface rather than a dependency, because choosing Prometheus or
OpenTelemetry for you would put every user of this package on the same one.

```go
mq, err := acemq.Connect(ctx, url, acemq.WithObserver(myObserver))
```

**Nothing is measured until you ask.** The default is `NopObserver`, so a program
that never reads metrics does not pay for them.

### What is reported

| Metric | |
|---|---|
| `acemq.messages.published` | handed to the broker |
| `acemq.messages.publish.failed` | did not get there, including unroutable |
| `acemq.messages.consumed` | delivered to a handler |
| `acemq.messages.accepted` / `.retried` / `.rejected` | what handlers decided |
| `acemq.messages.dead.lettered` | ran out of attempts, or would not decode |
| `acemq.handler.duration` | seconds per message |
| `acemq.messages.in.flight` | being handled right now |

The names match the Java and .NET libraries, so a dashboard built against one
reads against another.

`acemq.messages.dead.lettered` is the one to alert on. It is the count of
messages that are gone.

### If you only want the numbers

```go
metrics := acemq.NewMetrics()
mq, err := acemq.Connect(ctx, url, acemq.WithObserver(metrics))

metrics.Counts()     // map[string]int64
metrics.Durations()  // count, sum, min, max per metric
```

Enough to expose from a health endpoint or assert on in a test. **Not a
substitute for a real metrics system**: no histograms, no percentiles, nothing
exported anywhere. Percentiles need every sample kept or a sketch, and a library
that quietly did either would be making a decision about memory that belongs to
the application.

Labels are sorted into the key. Go randomises map iteration deliberately, so a
key built by walking the map would differ each time and one counter would quietly
become many — there is a test for that.

## Tracing

Metrics answer *how much*. A trace answers *what happened to this message*: it
was published by the checkout service, retried twice over four minutes and given
up on — which is the question somebody is actually holding when they open a
dashboard.

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
```

```go
import "github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel"

tracing := otel.New()

mq, err := acemq.Connect(ctx, url,
	acemq.WithPublishInterceptor(tracing.PublishInterceptor()))

orders := otel.NewPublisher[OrderPlaced](tracing, mq, "", "orders")
err = orders.Send(ctx, order)

_, err = acemq.Consume(ctx, mq, "orders",
	otel.Handle(tracing, "orders", handle))
```

A module of its own, so `go.opentelemetry.io/otel` never becomes a dependency of
the core — the same arrangement as the codec modules. A service that publishes
messages and traces nothing resolves nothing new.

`otel.New()` takes the process's tracer provider, so **nothing is emitted until
the application configures an SDK**. The exporter and the sampler are the
application's business; this module only says what happened.

### The join across processes is the point

A consumer's span is a child of the publish that caused it, and the parent comes
out of **the message's own headers** rather than out of whatever the delivery
goroutine happened to be doing. Those are two different traces, minutes and
machines apart, and joining them is the one thing a messaging system needs from
tracing that an HTTP client does not.

That is what `traceparent` carries. It is deliberately **not** `x-acemq-`
prefixed: it is the W3C name, which every other tracing tool already knows, and
renaming it would make this library's traces invisible to all of them. The Java,
.NET, Python and Ruby libraries write the same two headers, so a Go consumer
joins a Java producer's trace without either side being configured for the other.

### Span names and kinds

| | | |
|---|---|---|
| `<destination> publish` | `PRODUCER` | a publish |
| `<queue> process` | `CONSUMER` | a handler |
| `<destination> request` | `CLIENT` | a request that waits for its reply |

`CLIENT` for a request rather than `PRODUCER` because that span waits: its
duration is a round trip. A reader who cannot tell the two apart cannot tell a
slow broker from a slow responder.

### Attributes

| Attribute | |
|---|---|
| `messaging.system` | `rabbitmq`, or whatever `otel.WithSystem` says |
| `messaging.destination.name` | the exchange, or the queue on the way in |
| `messaging.operation` | `publish`, `process` or `request` |
| `messaging.message.id` | the envelope's identifier |
| `messaging.message.conversation_id` | its correlation |
| `messaging.rabbitmq.destination.routing_key` | the key it went out under |
| `messaging.acemq.message_type` | the logical type |
| `messaging.acemq.attempt` | which delivery attempt this is |
| `messaging.acemq.outcome` | how it ended |

The first six are the OpenTelemetry messaging conventions; the last three have no
standard names and are the three things most often wanted. Every AceMQ library
writes the same set.

### Which outcomes are errors

`unroutable`, `failed` and `dead_lettered` set the span status to `ERROR`.

`acked`, `retried`, `rejected`, `confirmed`, `published`, `answered` and
`timed_out` do not. A retry is the system working — the message will be tried
again and very often succeeds — and a message the handler refused on purpose is a
decision rather than a fault. Marking either as an error is how a trace view
fills with red and stops meaning anything.

### Events rather than spans

`outbox.publish_failed`, `pipeline.run_finished`, `message.retried` and
`message.dead_lettered` are events on the span that is already open, not spans of
their own. A zero-length span at the end of a trace adds a row and no
information.

```go
tracing.MessageDeadLettered(ctx, "orders", envelope, "out of attempts")
```

An event with no span open is dropped rather than opening one for itself. That
is a legitimate answer: an outbox relay on its own goroutine with no delivery in
flight has nothing to hang an event on.

### Trace context for a message this library does not publish

```go
headers := tracing.PropagationHeaders(ctx)  // map[string]string, empty when untraced
tracing.Inject(ctx, &envelope)              // the same thing, onto an envelope
```

For a record going into an outbox, whose publish happens later and elsewhere.
Every message published on a connection carrying `tracing.PublishInterceptor()`
already has this written for it.

## Health

```go
report := mq.Health(ctx)
// {Status: up, Parts: {consumers: 3, roundTripMillis: 2}}
```

The check declares a temporary exclusive queue, which is the cheapest thing AMQP
offers that actually proves the connection works. A TCP connection that is open
but wedged — the broker paused, the network black-holing — looks identical to a
healthy one until something is asked of it.

It costs a round trip, so wire it to a readiness probe and let the probe's
interval decide how often.

### Combining checks

```go
report := acemq.AggregateHealth(ctx,
	acemq.ConnHealth{Conn: mq, Label: "orders-broker"},
	myDatabaseCheck{})

if report.Status == acemq.HealthDown {
	w.WriteHeader(http.StatusServiceUnavailable)
}
json.NewEncoder(w).Encode(report)
```

The combined status is the worst of them: one thing down makes the whole report
down, because a service that cannot reach its broker is not ready however healthy
the rest of it is.

`HealthDegraded` is deliberately not down. Worth an alert; not worth taking the
instance out of rotation, because its replacement will almost certainly be
degraded too.

Checks run at once rather than in turn, so a slow one does not add its latency to
the others. A check that ignores its context can still hang the whole report,
which is why the interface says not to.

## The HTTP endpoints

```go
import "github.com/AceMQ-Company/acemq-go-amqp/actuator"

metrics := acemq.NewMetrics()
mq, err := acemq.Connect(ctx, url, acemq.WithObserver(metrics))

act := actuator.New(actuator.Options{Metrics: metrics, Conn: mq, Name: "orders"})
go http.ListenAndServe("127.0.0.1:9090", act)
```

| | |
|---|---|
| `/acemq-metrics` | Prometheus text format |
| `/acemq-health` | JSON, **503** when anything is down |
| `/acemq-info` | version, and what the transport can do |

The paths match the Java and .NET libraries, so a scrape configuration or a
probe written for one works against another.

The Prometheus output is written directly rather than through a client library,
so this package needs nothing outside the standard library either. Durations
appear as a summary with a count and a sum — enough for a rate and an average —
plus min and max as gauges. No quantiles, for the reason above.

### It is not authenticated

Nothing checks who is asking. Health says which dependencies are down and
metrics say how much traffic there is, which is more than an anonymous caller
should learn about a service.

Bind it to loopback, put it on a port your ingress does not publish, or wrap the
handler in your own middleware. The library will not do it for you, and says so
rather than implying otherwise by shipping a token check nobody configures.
