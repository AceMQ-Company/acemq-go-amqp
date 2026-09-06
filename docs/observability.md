# Metrics and health

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

### There is no built-in endpoint

The Java and .NET libraries ship actuator-style HTTP endpoints. This one does
not: Go services differ too much in how they serve HTTP for a library to pick,
and `net/http` makes it three lines.

```go
http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
	report := acemq.AggregateHealth(r.Context(), acemq.ConnHealth{Conn: mq})
	if report.Status == acemq.HealthDown {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(report)
})
```

Whatever you serve it from is yours to authenticate. This library does not.
