# The lifecycle of a service

A consumer is a process that is supposed to run for months and then stop
politely. The library gives you a constructor, a `context` and `Close()`; what
this page is about is the shape you put them in — where the context comes from,
what `Close` actually finishes, and what is lost when it runs out of time.

There is no framework integration here to do it for you, and that is deliberate.
[The last section](#what-this-is-not) says why.

## The shape

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
```

The context comes from the signal, at the top, before anything is connected.
That ordering is the whole recipe and it is easy to get wrong in the other
direction: a program that dials the broker first and only then works out how it
will be told to stop has already lost the connection it needs to close, because
the thing holding it is a package-level variable and the thing cancelling it is
a goroutine that was started somewhere else. A context threaded in later is a
context that reaches some of the program.

`signal.NotifyContext` has one behaviour worth knowing before it surprises you
in an incident: the handler stays installed until `stop` is called, so a second
SIGINT does nothing at all. Somebody pressing Ctrl-C twice, or an operator who
has decided the drain is taking too long, gets no response from a process that
looks wedged. Calling `stop` when the drain begins puts the default disposition
back, and the next signal ends the process.

### Connecting so that the transport stays in reach

```go
metrics := acemq.NewMetrics()

transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{
	Name:       "orders-worker",
	OnRecovery: func(e rabbitmq.RecoveryEvent) { log.Printf("acemq: %s", e) },
})
if err != nil {
	return err
}
mq, err := acemq.NewConn(transport,
	acemq.WithRetry(policy), acemq.WithObserver(metrics))
if err != nil {
	return err
}
```

`acemq.Connect(ctx, url)` is shorter and is the right thing in a test. A
long-running service wants the two-step form, because `*Conn` deliberately does
not expose the transport underneath it — so the handle returned by `Dial` is the
only route to `OnRecovery`, which is something a service wants to log. Reaching
for it after the fact is not possible; keeping the handle costs one variable.

`BlockedReason()` used to be on that list and no longer is: `*Conn` forwards it,
so a health check does not need the transport handle to ask whether the broker
has blocked this connection — and the built-in check asks for itself.

### The consumers get a context of their own

```go
handlers, releaseHandlers := context.WithCancel(context.Background())
defer releaseHandlers()

sub, err := acemq.Consume(handlers, mq, "orders", handle,
	acemq.Prefetch(20), acemq.Concurrency(4))
if err != nil {
	return err
}
```

This is the one place the recipe does *not* pass the signal context down, and it
is the most important line on the page.

The context given to `Consume` is the context the engine settles on. Every
acknowledgement is cheap, but the interesting settlements are publishes: a retry
is a republish onto the queue or onto a rung, a dead letter is a republish onto
`{queue}.dlq`, a park is a republish onto `{queue}.parked`. All of those go
through a context check first, and a cancelled context fails them. So a consumer
handed the signal context loses the ability to file messages correctly at exactly
the moment it is being asked to stop. It is measurable: with the context live, a
rejected message reaches `orders.dlq`; with it cancelled a fraction of a second
earlier, the same rejection is a `basic.nack` with no requeue, and where that
message ends up is the queue's `x-dead-letter-exchange` or nowhere.

Cancelling that context is still useful — it is the only lever that shortens a
drain — so it gets cancelled, deliberately, once `Close` has returned or the
deadline has gone. That is what `releaseHandlers` is for below.

Handlers see this context too, which is the other half of why it should outlive
the signal. During a graceful drain you want the handler in flight to *finish*,
not to have the database call under it cancelled. A handler that ought to give up
early on shutdown can watch the signal context itself; most should not.

### Several things at once

```go
g, gctx := errgroup.WithContext(ctx)

g.Go(func() error {
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
})

g.Go(func() error {
	<-gctx.Done()
	ready.Store(false)

	stopServing, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return srv.Shutdown(stopServing)
})

g.Go(func() error {
	<-gctx.Done()
	// Restores the default disposition of SIGINT and SIGTERM, so a second
	// signal kills the process instead of being swallowed by the handler
	// that is still installed. Somebody pressing Ctrl-C twice means it.
	stop()
	return drain(sub, mq, releaseHandlers, 20*time.Second)
})

return g.Wait()
```

`errgroup.WithContext` is doing two jobs, and the second is the reason to prefer
it to a `sync.WaitGroup`. The first is the obvious one: `Wait` returns the first
error any of them produced. The second is that `gctx` is cancelled when *any* of
them returns an error, as well as when `ctx` is — so an HTTP listener that cannot
bind its port shuts the consumers down instead of leaving a half-started process
running quietly with no way to scrape it. A `WaitGroup` gives you neither, and
the version of this written with one always grows a second channel to carry the
error and a third to carry the cancellation, which is `errgroup` with the name
filed off.

`golang.org/x/sync` is a module outside the standard library, and the only one
this page asks for. The library itself still depends on nothing but the standard
library and, in the `rabbitmq` package, the AMQP client; an application is a
different kind of thing and can afford a dependency the library cannot.

## What `Close` finishes, precisely

`sub.Close()` does three things in a fixed order: it stops delivery, it waits,
and only then does it let go of the channel the settlements travel on. That last
ordering is not cosmetic — releasing the channel first leaves every message in
flight acknowledged into a channel that has gone, so the broker hears nothing,
hands the message to another consumer, and a retry that had already been
republished is now on the queue twice.

`mq.Close()` closes every consumer the same way and then releases the connection.

What that means in the four situations that actually turn up at shutdown:

| at the moment of `Close` | what happens |
|---|---|
| a handler is running | it runs to completion, and its decision is carried out |
| a delivery has arrived but no handler has it yet | it is handled too, in full |
| a publish is waiting for its confirm | nothing waits for it; it is cut off |
| a retry is waiting out a backoff in this process | `Close` waits with it, for the whole delay |

### The handler in flight

It finishes. Not "is given a chance to finish" — `Close` blocks on it. A handler
sleeping for 300 milliseconds when `Close` is called makes `Close` take 300
milliseconds, and the message is accepted and acknowledged rather than abandoned
for the broker to give to somebody else.

That is the guarantee worth having, and it is also the whole reason a shutdown
needs a deadline: the library will wait indefinitely for a handler that never
returns.

### The delivery that never reached a handler

This one is usually a surprise. `Close` does not just finish the message in the
handler; it finishes everything the transport had already handed over. The
subscription cancels the consumer, waits for the delivery channel to close, and
the loop reading that channel passes on every delivery still sitting in it. Each
of those goes through a handler exactly like any other message.

So the work a drain has to get through is bounded by **`Prefetch`**, not by
`Concurrency`. With `Prefetch(100)`, `Concurrency(1)` and a handler that takes
200 milliseconds, a shutdown can have twenty seconds of work in hand before it
starts. That is a good reason to keep prefetch proportionate to what a handler
costs, and it is a better reason than the usual one about fairness between
consumers.

This is the right behaviour — those messages are the consumer's responsibility
and it settles them — but it is a cost that is invisible until a deployment
starts timing out.

### The publish waiting for a confirm

Nothing waits for it. `Conn.Close()` closes consumers, then closes the transport,
and the transport closes the channel the confirms were going to arrive on. A
publish that had been written but not yet confirmed gets an error saying so, and
that error is genuinely ambiguous: the broker may well have taken the message.

There is no `Publisher.Close()`, and nothing tracks publishers, because a
publisher is a struct with a codec and a routing key rather than a live resource.
The consequence is that a publisher outside a handler needs a barrier of its own:

```go
// publishers is the barrier a publisher outside a handler needs. Nothing in the
// library tracks one, so nothing in the library waits for one.
var publishers sync.WaitGroup

func publish(ctx context.Context, pub *acemq.Publisher[OrderPlaced], order OrderPlaced) {
	publishers.Add(1)
	go func() {
		defer publishers.Done()
		if err := pub.Send(ctx, order); err != nil {
			log.Printf("acemq: %v", err)
		}
	}()
}

func closeWhenPublishersAreDone(mq *acemq.Conn) error {
	publishers.Wait()
	return mq.Close()
}
```

A publisher *inside* a handler needs none of this: `Close` waits for the handler,
and the handler has not returned until its publish has.

### The retry waiting out a backoff

A short wait is spent in this process, holding the delivery; a long one is spent
on a rung queue in the broker, holding nothing. Which is which is
`BrokerWaitThreshold`, thirty seconds by default — see
[retries and redelivery](reliability.md).

At shutdown the difference stops being about prefetch slots and becomes about
whether the drain finishes at all, because **`Close` waits out an in-process
backoff in full**. A consumer two seconds into a two-second wait makes `Close`
take the rest of it. A consumer that fell back to waiting here because its rung
queue was missing, on a five-minute schedule, makes `Close` take five minutes —
which is to say it makes the drain fail, because nothing grants a pod five
minutes.

So the rung queues are a shutdown concern and not only a reliability one. A wait
that lives on the broker is a wait a restart does not have to survive and a drain
does not have to sit through:

```go
policy := acemq.ExponentialRetry(6, 10*time.Second, 5*time.Minute).
	WaitInBrokerFrom(10 * time.Second)

err := acemq.NewTopology().
	Queue("orders").
	DeadLetters("orders").
	Retries("orders", policy).
	Apply(ctx, mq)
```

`acemq.Consume` declares the rungs of the policy it is given, so a consumer
normally finds them. One that does not still retries, waiting here instead, and
counts `acemq.MetricRungMissing` every time it does. That counter is worth an
alert on its own merits; it is also the early warning that the next deployment's
drain is going to be slow.

The lever for a drain that has to end now is the consumers' context. Cancelling
it aborts an in-process wait immediately — measured at microseconds rather than
the ten seconds the policy asked for — and the delivery goes back to the broker
with a requeue, not lost. It comes round again after the restart at the same
attempt number, because a requeue hands back the bytes the broker was given and
the attempt counter rides on those bytes. One extra attempt, no lost message.

The price is the one named earlier: with that context cancelled, nothing else can
be republished either. A handler that rejects a message during that window
cannot file it in `{queue}.dlq`; the delivery is nacked without requeue and the
broker's own dead-lettering, if the queue has any, is what catches it.
`acemq.MetricSetAsideFailed` counts each one.

**Which is why the cancellation comes last, and never first.** Cancelling the
consumers' context is not a way to hurry a drain along. It is the thing you do
when the drain has already failed, to get the deliveries back to the broker
rather than leave them unsettled.

## Bounding it

A drain that never finishes is a pod that gets SIGKILLed, and SIGKILL is the
worst of all the outcomes on this page: no handler completes, nothing is
settled, and every delivery the consumer was holding comes back to whoever
replaces it.

```go
// drain stops the consumer, waits for what it is already holding, and gives up
// when it runs out of time.
func drain(sub *acemq.Consumer, mq *acemq.Conn, releaseHandlers func(), within time.Duration) error {
	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()

	select {
	case err := <-closed:
		releaseHandlers()
		return errors.Join(err, mq.Close())

	case <-time.After(within):
		// Out of time. Cancelling the handlers' context is the only lever that
		// shortens a drain: it aborts a backoff being waited out in this process
		// and hands that delivery back to the broker. Close itself takes no
		// context and cannot be abandoned, so what this bounds is how long the
		// process waits, not how long Close takes.
		releaseHandlers()

		select {
		case err := <-closed:
			return errors.Join(err, mq.Close())
		case <-time.After(2 * time.Second):
			return errors.New("the drain did not finish; some deliveries were left unsettled")
		}
	}
}
```

`Close` takes no context and cannot be abandoned. That is worth saying plainly,
because the shape above looks like a timeout on `Close` and is not one: the
goroutine holding `sub.Close()` is still in there when the second `select`
expires, and `mq.Close()` would block behind it. What the deadline bounds is how
long *the process* waits before deciding the drain has failed and returning, at
which point `main` ends and the runtime takes the goroutine with it.

The consequence is that the deadline has to be comfortably shorter than whatever
will kill the process:

| | |
|---|---|
| Kubernetes | `terminationGracePeriodSeconds`, thirty by default |
| systemd | `TimeoutStopSec` |
| Docker | `docker stop -t`, ten by default |

Twenty seconds inside a thirty-second grace period leaves ten for the HTTP
server, for whatever else is in the group, and for the process to actually exit.
Setting the two equal means the orchestrator wins the race sometimes, and a
shutdown that is correct on most deployments is one nobody debugs until it is
not.

**What is lost when the deadline expires** is worth being exact about, because
"the drain timed out" sounds worse than it is:

- Messages whose handlers had not finished are **not acknowledged**, so the
  broker redelivers them. The work may be done twice. This is the ordinary
  at-least-once case that handlers should already be idempotent against — see
  [duplicates](reliability.md#duplicates).
- Messages that were mid-settlement are back with the broker, one attempt
  behind where they would have been.
- Messages that needed dead-lettering in that window went to the broker's
  dead-letter exchange without the reason attached, or nowhere.

Nothing is silently dropped. What is lost is the *reasons* — which is exactly the
thing an operator draining `{queue}.dlq` a week later needs and cannot
reconstruct.

## Health and readiness

Two questions that an orchestrator asks separately and that are tempting to
answer with one handler:

```go
// probes serves what an orchestrator asks for and what a scrape asks for, on
// one loopback port.
func probes(metrics *acemq.Metrics, mq *acemq.Conn) http.Handler {
	// ConnHealth reads the blocked state itself and reports it up with the
	// reason, so there is nothing here to compose around it. Three seconds
	// rather than the aggregate's five, so the check that looked is the one
	// that describes what it found.
	checks := []acemq.HealthCheck{acemq.ConnHealth{Conn: mq, Timeout: 3 * time.Second}}

	// Conn as well as Checks: it is what /acemq-info reads the transport's
	// capabilities from, and WithoutConnHealth keeps /acemq-health to the
	// checks above rather than adding a second opinion of the same connection.
	act := actuator.New(actuator.Options{
		Metrics:           metrics,
		Conn:              mq,
		WithoutConnHealth: true,
		Checks:            checks,
		Name:              "orders-worker",
	})

	mux := http.NewServeMux()
	mux.Handle("/", act)

	// Liveness is "this process is running". Nothing about the broker belongs
	// here: a process that cannot reach its broker is not one a restart fixes,
	// and the restart throws away whatever it was holding.
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness is "this process can do the work". The drain flag is read first,
	// so an instance that has been told to stop leaves the rotation before it
	// starts refusing anything.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		report := acemq.AggregateHealth(r.Context(), checks...)
		w.Header().Set("Content-Type", "application/json")
		if report.Status == acemq.HealthDown {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(report)
	})

	return mux
}
```

The [`actuator` package](observability.md#the-http-endpoints) serves
`/acemq-metrics`, `/acemq-health` and `/acemq-info` on paths that match the Java
and .NET libraries, so a scrape configuration written for one works against
another. It is mounted whole above and then given two probes beside it, because
the cross-language paths are the right answer for a dashboard and the wrong
answer for a liveness probe.

**Liveness must not consult the broker.** A process that cannot reach its broker
is not a process that restarting will fix; restarting it throws away every
message it was holding and reconnects to the same unreachable broker, and a
fleet doing that in unison is a queue that stops being drained at the moment it
most needs draining. Liveness is for a process that has stopped being a process
— deadlocked, out of file descriptors, a goroutine leak that ate the heap.

**Readiness may.** It is the honest place for "this instance cannot do the work
right now", and taking it out of rotation costs nothing but a load balancer entry
— which for a pure consumer is nothing at all, though it still gates a rolling
update, which is the part that matters here.

### A blocked connection is not a reason to restart

`mq.Health(ctx)` declares a temporary exclusive queue and reports `up` if the
broker answers. That is the cheapest thing AMQP offers that actually proves the
connection works: a TCP connection that is open but wedged answers exactly like
a healthy one until something is asked of it.

**Unless the broker has blocked this connection, in which case nothing is
asked.** RabbitMQ sends `connection.blocked` when it is low on memory or disk,
and a blocked connection is one the broker has **stopped reading**. The probe's
declaration is therefore not refused — it goes unanswered, for as long as the
alarm lasts. The check reads the blocked state instead and answers straight
away:

```go
report := mq.Health(ctx)
// {Status: up,
//  Detail: "the broker has blocked this connection; publishing is paused: low on memory",
//  Parts: {consumers: 3, blocked: true, blockedReason: "low on memory"}}
```

Both halves of that are deliberate.

**Nothing is asked** because the broker telling this socket that it blocked it is
livelier proof that the broker is there than any declaration could be. Against a
real broker under a memory alarm, `Health` answers in **under a millisecond**
where the round trip it used to make had not come back after twelve seconds — and
that was with a five-second context, because the RabbitMQ transport's declaration
is a synchronous AMQP round trip that no context reaches.

**It is `up`** because a blocked connection is the broker protecting itself, and
an application that fails its own readiness check for it is one an orchestrator
restarts into the same blocked broker, having thrown away whatever it was
holding. A fleet doing that together stops draining the queues at the moment the
broker most needs them drained. Java's `AceMqHealthIndicator`, .NET's `Health()`,
Python's `health()` and Ruby's `Health` all say the same thing in the same words:
the text before the colon is fixed, because it is what an alert rule matches on,
and the broker's own words follow it.

It is not `degraded` either, which is what this page used to recommend composing
by hand. Degraded is for this instance being worse at its job than it should be;
a block is the broker's state, identical for every replica, and saying `degraded`
for it means a fleet-wide alert that no deployment can act on.

**The probe that is made has a deadline.** `ctx` bounds it, and
`acemq.DefaultHealthTimeout` — three seconds — bounds it when `ctx` carries none,
which is the shape `http.Request.Context()` has. A probe that runs out of time is
abandoned rather than waited for, because cancelling a request to a broker that
is not reading means waiting for a cancellation that travels the same way the
request did. A block that arrives *during* a probe is read as the explanation for
the silence rather than as a second fault beside it.

### Asking about it without a round trip

```go
if reason := mq.BlockedReason(); reason != "" {
	// Shed load, buffer, fail the request — anything but hand it over.
}

blocked, known := mq.Blocked() // known is false when nobody could be asked
```

`BlockedReason()` also still lives on `*rabbitmq.Transport`, where it always did;
`*acemq.Conn` now forwards it through a `BlockedReporter` seam, so a check does
not have to be handed the concrete transport to ask, and a test double answers it
by implementing one method. `Blocked()`'s second return keeps "no" apart from
"nobody looked": a transport that cannot be asked reports `blocked: null` rather
than `blocked: false`, and in an incident those are not the same sentence.

### Keeping `Conn` without being checked twice

An application whose own check of the connection is the one it wants in the
report says so, and keeps `Conn` for what else it feeds:

```go
act := actuator.New(actuator.Options{
	Metrics: metrics,
	Conn:    mq,                 // /acemq-info still lists the transport's capabilities
	WithoutConnHealth: true,     // but /acemq-health takes only the checks below
	Checks:  []acemq.HealthCheck{myBrokerCheck{mq}},
	Name:    "orders-worker",
})
```

This used to be a trade with no way out. `AggregateHealth` takes the worst report,
so the library's own `ConnHealth` overruled any more careful answer composed
beside it, and the only way to keep it out was to leave `Conn` unset — which took
the transport's capabilities out of `/acemq-info` with it. Both halves are fixed:
`ConnHealth` passes the connection's own answer through and adds no opinion of its
own, so there is usually nothing to get out from under; and when there is,
`WithoutConnHealth` is how, without giving up the rest of what `Conn` is for.

### Marking the drain

```go
// ready is false until the consumers are running and false again the moment the
// drain begins, so a readiness probe stops counting this instance before it
// stops being able to do the work.
var ready atomic.Bool
```

Set it once the consumers are attached, clear it in the goroutine that hears the
signal. A rolling update that waits for the replacement to be ready before
signalling the old instance is the difference between a deployment that drains
and a deployment that drops to zero consumers for a few seconds — which, on a
queue with a retry ladder, is a few seconds of everything going round again.

## Publishing at shutdown

[`MaxOutstandingPublishes`](publishing.md) bounds how many publishes may be
waiting for a confirm at once: a thousand by default, which is Java's number and
.NET's. Ordinarily it is a memory bound and a backpressure signal. At shutdown it
is also the size of what gets cut off, because each slot holds a body the broker
has not acknowledged and `Close` does not wait for any of them.

A thousand unconfirmed publishes at the moment of `Close` is a thousand messages
whose fate is genuinely unknown to this process. Lowering the bound for a service
that publishes in bursts and restarts often is a reasonable thing to do for
shutdown reasons alone, and the honest framing is that the bound is not there to
make throughput safe — it is there so that the amount of work in an unknown state
is one you chose.

`SendAll` makes this concrete, because a batch holds its whole set of
unconfirmed publishes at once:

```go
// relay publishes a batch and records what the broker actually took.
func relay(ctx context.Context, pub *acemq.Publisher[OrderPlaced], batch []OrderPlaced) error {
	results, err := pub.SendAll(ctx, batch)

	var failed *acemq.BatchPublishFailedError
	if errors.As(err, &failed) {
		// A shutdown that cut the batch in half is the ordinary way to get here,
		// and resending all of it would deliver the confirmed half twice.
		for i, r := range results {
			if failed.Errors[i] == nil {
				markSent(r.MessageID)
			}
		}
	}
	return err
}
```

`SendAll` waits for every confirm before it returns, so it is not an escape from
the barrier — a batch in flight is a `publishers.Wait()` that has not finished.
What it gives you is the only thing that makes a half-finished batch recoverable:
`BatchPublishFailedError` says how many were confirmed, and `Errors` says which,
by index. A caller told only "it failed" resends the whole batch and delivers the
confirmed half twice.

A batch is also the one place where the publish bound and the shutdown deadline
interact badly. A batch of five thousand against a bound of a thousand is five
sequential waves of confirms, and a signal arriving partway through leaves four
waves' worth of publishes that this process will never learn the outcome of. If a
service publishes batches larger than the bound, size them so that one batch
fits inside the shutdown deadline, or keep an outbox and let the relay do the
resuming — see [patterns](patterns.md).

## What this is not

This is a page, not a package. Go gets no dependency-injection integration from
this library, and will not get one.

The other four libraries have or are getting one, because in each of them there
is an obvious thing to integrate with: Java has a Spring Boot starter, .NET has
a DI container and `IHostedService`, Python has FastAPI's lifespan, Ruby has a
Rails railtie. Each of those is *the* convention in its language, and the
integration is a few dozen lines that save every user the same few dozen lines.

Go has no equivalent. `fx` and `wire` both exist, both have real users, and
neither is dominant — and a large share of Go services use neither, because a
`func main` that constructs its dependencies in order is a perfectly good
container. Picking one would serve a minority of users while adding to this
library precisely the kind of dependency the rest of its design avoids: a core
that needs nothing outside the standard library, a transport behind a registry
so a program that only uses `memory://` never compiles the AMQP client in, and
every codec beyond JSON in a module of its own. A DI integration would be the
first thing here with an opinion about how your whole program is assembled.

And there is nothing to bridge. Go's idiomatic answer is already the library's
API:

```go
// forAnyContainer is the whole surface a container has to be told about.
func forAnyContainer(
	ctx context.Context, transport *rabbitmq.Transport, handle acemq.Handler[OrderPlaced],
) error {
	mq, err := acemq.NewConn(transport) // a constructor that returns an error
	if err != nil {
		return err
	}
	defer mq.Close() // an io.Closer

	sub, err := acemq.Consume(ctx, mq, "orders", handle) // takes a context
	if err != nil {
		return err
	}
	defer sub.Close()

	return nil
}
```

A constructor returning `(T, error)`, a `Close()`, and a `context` on everything
that blocks. That is exactly the triple `fx.Provide` and `fx.Lifecycle` want, so
somebody using `fx` writes:

```go
fx.Provide(func(lc fx.Lifecycle, cfg Config) (*acemq.Conn, error) { ... })
```

and the `OnStop` hook is `mq.Close()`. There is no adapter for this library to
ship, because there is no mismatch to adapt. Same for `wire`, which is a code
generator and wants a provider function it can call — which is what `NewConn`
is.

What a Go service author actually needs is not a container. It is to know that
`Close` waits for handlers but not for publishers, that an in-process backoff is
inside the drain, that the consumers' context should outlive the signal, and how
long the whole thing may take. None of that is something a container could have
told you, and all of it is on this page.

## Where to go next

- [Consuming](consuming.md) — handlers, the four decisions, prefetch and
  concurrency
- [Retries, redelivery and shutdown](reliability.md) — the retry ladder, rung
  queues, recovery
- [Publishing](publishing.md) — confirms, `SendAll`, `MaxOutstandingPublishes`
- [Metrics, tracing and health](observability.md) — the actuator, and what is
  counted
- [Patterns](patterns.md) — the outbox, for publishes that must survive a
  restart
