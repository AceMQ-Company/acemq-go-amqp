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

> ### Every metric was renamed
>
> **This breaks every existing Go dashboard, alert rule and recording rule.**
>
> Java's `MetricNames` is the family's vocabulary and Go, Python and Ruby have
> moved onto it. Until now the four libraries emitted disjoint sets of names, so
> the promise this page used to make — that a dashboard reads the same against
> another AceMQ library — was simply untrue. It is true now, and it costs one
> editing pass over whatever you have built.
>
> | Old | New |
> |---|---|
> | `acemq.messages.published` | `acemq.publish.total{outcome=confirmed}` or `{outcome=published}` |
> | `acemq.messages.publish.failed` | `acemq.publish.total{outcome=failed}` or `{outcome=unroutable}` |
> | — | `acemq.publish.duration` (new) |
> | `acemq.messages.consumed` | `acemq.consume.total` |
> | `acemq.messages.accepted` | `acemq.consume.total{outcome=acked}` |
> | `acemq.messages.rejected` | `acemq.consume.total{outcome=rejected}` |
> | `acemq.messages.retried` | `acemq.messages.retried.total` |
> | `acemq.messages.dead.lettered` | `acemq.messages.dead.lettered.total` |
> | `acemq.messages.parked` | `acemq.consume.total{outcome=parked}` |
> | `acemq.handler.duration` | `acemq.consume.duration` |
> | `acemq.messages.in.flight` | `acemq.consume.in.flight` |
> | `acemq.messages.set.aside.failed` | unchanged |
> | `acemq.retry.rung.missing` | unchanged |
> | `acemq.outbox.lag`, `acemq.outbox.total` | unchanged |
>
> **Four of the old counters became tag filters rather than new names, so an
> alert on one has to be rewritten and not renamed.** `accepted`, `rejected` and
> `parked` were always `acemq.consume.total` split by its own `outcome` tag;
> writing both gave a dashboard two ways to be wrong about one thing. Java keeps
> only `retried` and `dead_lettered` standing alone, because those two are what
> an alert is written against, and this library now keeps the same two and no
> others.
>
> `acemq.messages.published` and `acemq.messages.publish.failed` merged into one
> counter with an `outcome` tag, which the old split could not express: an
> unroutable mandatory message was counted as a failure, so a broken publisher
> and an unbound routing key were indistinguishable. They are now `unroutable`
> and `failed`.
>
> Scraping through `actuator`, the Prometheus names follow: `acemq_publish_total`,
> `acemq_consume_total`, `acemq_consume_duration_count`, `acemq_consume_in_flight`.

### What is reported

| Metric | |
|---|---|
| `acemq.publish.total` | messages handed to the broker, tagged `outcome` |
| `acemq.publish.duration` | seconds per publish, tagged `outcome` |
| `acemq.consume.total` | deliveries settled, tagged `outcome` |
| `acemq.consume.duration` | seconds per message in a handler, tagged `outcome` |
| `acemq.consume.in.flight` | being handled right now |
| `acemq.messages.retried.total` | another attempt was actually scheduled |
| `acemq.messages.dead.lettered.total` | the engine gave up on it |
| `acemq.messages.set.aside.failed` | it could not be moved to a dead-letter or parking queue |
| `acemq.retry.rung.missing` | a long retry had to wait in the consumer |
| `acemq.outbox.total` | outbox records the relay handled, tagged `outcome` |
| `acemq.outbox.lag` | seconds between an outbox record being committed and published |

**These are Java's names.** They are the family's vocabulary, and Go, Python and
Ruby have moved onto them — see the rename note below, because **every existing
Go dashboard has to be edited.**

`acemq.messages.dead.lettered.total` is the one to alert on. It is the count of
messages that are gone.

`acemq.retry.rung.missing` started here — the retry ladder is this library's own
— and Java has since taken the same name, so it is family vocabulary too.

#### Named but not written

Four more names are part of the family vocabulary and are declared in
`amqp/telemetry.go` so an `Observer` can be written against one list — but this
library does not emit them, and says so rather than leaving you to wonder why the
series is empty.

| Metric | Why not |
|---|---|
| `acemq.consume.attempts` | `Observer` has counters, gauges and durations and no general distribution. The number is on every message as `Envelope.Attempt`, so a handler can record it in one line. |
| `acemq.request.duration`, `acemq.request.total` | `patterns.Request` is a function over a connection rather than something the connection knows it is doing, so no point on the path holds an observer. The tracing adapter spans the round trip instead. |
| `acemq.pipeline.run.duration`, `acemq.pipeline.run.total` | Go has no `Pipeline` type owning its steps the way Java does. A finished run is reported through `patterns.RunObserver`; install your own and write these two names if you want counters. |

### The tag names

| Tag | On | |
|---|---|---|
| `queue` | the consume metrics | the queue the delivery arrived on |
| `outcome` | the publish, consume and outbox metrics | what happened |
| `exchange` | the publish and outbox metrics | where it was sent |
| `routing.key` | the publish and outbox metrics | the key it went out under |
| `rung` | `acemq.retry.rung.missing` | the rung queue that is not there |
| `target` | `acemq.messages.set.aside.failed` | the queue it could not be moved to |

`message.type`, `transport`, `pipeline` and `step` are declared as constants for
the same reason the unemitted metrics are — they are the family's spellings, and
an application writing its own tags should not invent a second one.

> **A dot is legal here and illegal in Prometheus.** `routing.key` and
> `message.type` both carry one. Emitted verbatim they do not make one label
> wrong, they make the **whole scrape unparseable**. The Prometheus endpoint in
> `actuator` therefore converts label names the way it already converted metric
> names, so they are scraped as `routing_key` and `message_type`. **An `Observer`
> you write by hand has to do the same.** There is a test that walks the entire
> tag vocabulary against the Prometheus label grammar, so a third dotted tag
> cannot reach a scrape endpoint unnoticed.

> **`routing.key` was `key` until the previous release.** Java and .NET already
> wrote `routing.key`; Go and Python wrote `key`, and neither reading was wrong.
> The fully-qualified name says *which* key it means next to a tag called
> `queue`, and Java is the library the others are ported from, so the two moved
> rather than the four staying split.

### One counter, tagged, rather than one counter per outcome

Every delivery increments `acemq.consume.total` exactly once, and the `outcome`
tag partitions those deliveries — the tags do not overlap, so grouping by
`outcome` and adding the groups back up gives the total again. The tag takes one
of five words:

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

`acemq.publish.total` carries the same tag, with four words of its own:

| `outcome` | |
|---|---|
| `confirmed` | the broker took responsibility for it |
| `published` | it went out with nothing promised, which is what a publisher without confirms gets |
| `unroutable` | it was mandatory and reached no queue at all |
| `failed` | the publish errored |

`unroutable` is deliberately not `failed`. Nothing went wrong — nothing was
listening — and a single failure counter cannot tell a broken publisher from an
unbound routing key, which is the question somebody actually has when the number
moves.

#### Two counters stand beside the tagged one

`acemq.messages.retried.total` and `acemq.messages.dead.lettered.total` are the
same events as `acemq.consume.total{outcome=retried}` and
`{outcome=dead_lettered}` seen a second time, not additions to them. Java keeps
these two standing alone because they are what an alert is written against, and
an alert should not have to know a tag vocabulary to fire.

There is deliberately **no third counter for parking.** A parked message is
`acemq.consume.total{outcome=parked}` and nothing else. That is what Java
settled on, and inventing a counter the rest of the family would not have is the
way these vocabularies drifted apart in the first place.

**Do not add them to `acemq.consume.total`.** That double-counts every retry.
There is a test pinning the two to agree.

### The outcome is the engine's decision, not the handler's request

Until an earlier release the classification came from what the handler asked for.
A handler that asked for a retry on its last permitted attempt was counted as a
retry and then *again* as a dead letter, so the retry counter counted retries that
never happened and included every message about to be given up on. Retries fell
and dead letters rose when that was fixed, with no change in what the service
does — the numbers were wrong and are now right.

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
