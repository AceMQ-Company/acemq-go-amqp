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
| `acemq.messages.consumed` | deliveries settled, tagged `outcome` |
| `acemq.messages.accepted` | the handler accepted it |
| `acemq.messages.retried` | another attempt was actually scheduled |
| `acemq.messages.rejected` | the handler gave up on it by name |
| `acemq.messages.dead.lettered` | the engine gave up on it |
| `acemq.messages.parked` | nothing could read it |
| `acemq.handler.duration` | seconds per message, tagged `outcome` |
| `acemq.messages.in.flight` | being handled right now |
| `acemq.outbox.total` | outbox records the relay handled, tagged `outcome` |
| `acemq.outbox.lag` | seconds between an outbox record being committed and published |

The names match the Python and Ruby libraries. The Java and .NET libraries count
the same things under `acemq.consume.total` and `acemq.consume.duration` with the
same `outcome` tag; that split is older than this library and is not one it can
settle on its own.

`acemq.messages.dead.lettered` is the one to alert on. It is the count of
messages that are gone.

### The tag names

| Tag | On | |
|---|---|---|
| `queue` | the consume metrics | the queue the delivery arrived on |
| `outcome` | the consume and outbox metrics | what the engine did |
| `exchange` | the publish and outbox metrics | where it was sent |
| `routing.key` | the publish and outbox metrics | the key it went out under |
| `rung` | `acemq.retry.rung.missing` | the rung queue that is not there |
| `target` | `acemq.messages.set.aside.failed` | the queue it could not be moved to |

> **`routing.key` was `key` until this release.** Java and .NET already wrote
> `routing.key`; Go and Python wrote `key`, and neither reading was wrong. The
> fully-qualified name says *which* key it means next to a tag called `queue`,
> and Java is the library the others are ported from, so the two moved rather
> than the four staying split. **A dashboard that groups publishes by `key` has
> to be edited.** Python is making the same change.
>
> The Prometheus endpoint in `actuator` converts label names the way it already
> converted metric names, so `routing.key` is scraped as `routing_key`. A dot is
> legal in an AceMQ tag and illegal in a Prometheus label; left in place it would
> have made the whole scrape unparseable rather than one label wrong.

### The outcome is the engine's decision, not the handler's request

Every delivery increments `acemq.messages.consumed` exactly once and exactly one
of the five outcome counters, so they add up. The `outcome` tag takes one of five
words:

| `outcome` | |
|---|---|
| `acked` | accepted and acknowledged |
| `retried` | another attempt was scheduled |
| `rejected` | the handler gave up on it by name |
| `dead_lettered` | the attempts ran out, the message aged out, the failure was marked unprocessable, or an interceptor refused it |
| `parked` | nothing could read it: the codec refused the body, or the handler returned `acemq.Park` |

It is the same word the tracing adapter puts on that delivery's span as
`messaging.acemq.outcome`, taken from the same `acemq.Settlement`, so a dashboard
filtered to dead letters and a trace search for them return the same set. There
is a test that asserts exactly that.

> **This changes numbers an existing dashboard may rely on.** Until this release
> the classification came from what the handler asked for. A handler that asked
> for a retry on its last permitted attempt was counted as a retry and then
> *again* as a dead letter, so `acemq.messages.retried` counted retries that
> never happened and included every message about to be given up on. From this
> release **retries fall and dead letters rise, with no change in what the
> service does** — the numbers were wrong and are now right. Two smaller
> corrections come with it: a retry that waits on a rung queue is no longer
> counted twice (once bare and once under a `rung` label, which is now only on
> `acemq.retry.rung.missing`), and a message nothing could decode is counted as
> `acemq.messages.parked` rather than `acemq.messages.rejected`. The .NET library
> made the same correction; Python and Ruby are making it too.

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

`acked`, `retried`, `rejected`, `parked`, `confirmed`, `published`, `answered`
and `timed_out` do not. A retry is the system working — the message will be tried
again and very often succeeds — and a message the handler refused on purpose is a
decision rather than a fault. Marking either as an error is how a trace view
fills with red and stops meaning anything.

### Every failure has an outcome

`Span.Failed` writes `messaging.acemq.outcome = failed` as well as recording the
exception and the `ERROR` status. Before this release it wrote neither the
attribute nor anything else a query could group by, so a publish that threw was
counted as `failed` and carried a span with no outcome at all — the counter and
the trace disagreeing about the same message, which is the one thing this shared
vocabulary exists to prevent.

An outcome named out loud wins, whichever order the two calls come in:

```go
span.Outcome(otel.OutcomeTimedOut)   // a request that ran out of time
span.Failed(err)                     // still timed_out, not failed
```

`timed_out` and `unroutable` are both more useful than `failed`, and only a
failure nobody has a better word for is called `failed`. Java's adapter was
fixed the same way; Python already did it.

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

Three of the four are written by the library itself. `message.retried` and
`message.dead_lettered` come from the engine when it settles a delivery, and
`pipeline.run_finished` from `patterns.FollowSlip` and `patterns.Then` when a run
leaves the pipeline — a pipeline step runs inside the delivery's span, so there
is something open to write onto:

```go
acemq.Consume(ctx, mq, "charge-queue", otel.Handle(tracing, "charge-queue",
	patterns.FollowSlip(mq, charge, patterns.InPipeline("fulfilment"))))
```

`InPipeline` is what turns it on, and it has to be given: Go has no `Pipeline`
type that owns its steps the way the Java library does, so a step that wants to
be reported has to say which pipeline it belongs to. `FollowSlip` takes the
step's own name off the routing slip; `patterns.AtStep` overrides it, and `Then`
needs it. A step that finishes the itinerary reports `completed`; a `Then` whose
step returns `false` — this message does not continue — reports `ended_early`.

#### The outbox relay is the one seam that stays open

`outbox.publish_failed` and the `messaging.acemq.outbox_lag_ms` attribute are
still methods you call, and the relay does not call them. It cannot: `Sweep` runs
on a goroutine of the relay's own with no span open, and opening one per record
would produce exactly the zero-length spans this adapter avoids. What the relay
does instead is count — `acemq.outbox.total` and `acemq.outbox.lag` are written
on every sweep and need no span at all. An application that wants the trace side
calls `Sweep` itself, from inside a span of its own:

```go
ctx, span := tracing.Tracer().Start(ctx, "outbox sweep")
defer span.End()

if _, err := relay.Sweep(ctx); err != nil {
	tracing.OutboxPublishFailed(ctx, "orders", err.Error())
}
```

### The engine has the last word on a delivery

`message.retried` and `message.dead_lettered` are written by the engine rather
than by the handler, because the handler does not know which of the two
happened. A handler asking for a retry is a request: the engine still has to
look at the policy, and a message on its last attempt is dead-lettered instead.

`otel.Handle` therefore hands the ending of a delivery's span to the engine,
through `acemq.OnSettled`:

```go
acemq.OnSettled(ctx, func(s acemq.Settlement) {
	// s.Action is accepted, retried, dead_lettered or parked — the four
	//   physical fates a delivery can have.
	// s.Outcome is the word a counter and a span use: acked, retried,
	//   rejected, dead_lettered or parked. Finer than Action in one place, a
	//   message the handler rejected is dead-lettered like an exhausted one
	//   and only this tells them apart.
	// s.Delay is the retry delay the engine actually chose.
	// s.Reason is why it was given up on.
})
```

Call it from inside a handler with the handler's own context. `f` runs exactly
once, on the same goroutine, after the handler has returned and after the engine
has decided — which is the only moment at which a retry's delay or a dead
letter's reason exists. It reports `false` when there is no engine listening,
which is what a handler called directly from a test is, and a caller holding
something that has to be finished either way should then finish it itself.

Nothing is allocated per delivery for this and nothing is called when nobody has
registered: a consumer's worker holds one hook and reuses it, so a service that
publishes plaintext and traces nothing pays nothing for the seam.

What this fixes: a message that exhausted its attempts used to carry
`outcome="retried"` on its span, because that is what the handler asked for and
the span ended before the engine answered. It now carries `dead_lettered`, with
`message.dead_lettered` and the reason on it. A handler that rejected a message
on purpose still carries `rejected` — that decision arrived where it was meant
to — with the dead-letter event alongside it.

The span does not work the outcome out for itself. It writes `s.Outcome`, which
is the same string the engine tagged `acemq.messages.consumed` with for this
delivery, so there is one derivation rather than two that could drift. The one
exception is a handler that *panics* while running directly, with no consumer
around it: there is no engine to settle the delivery and the span says `failed`.
Under a consumer the panic is recorded on the span as an exception and the
outcome stays the engine's word, which is `rejected` — a handler that panics is
dead-lettered rather than retried, because a bug repeats.

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
