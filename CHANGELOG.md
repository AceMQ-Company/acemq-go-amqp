# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added

- **`patterns.Saga`, for work that spans services.** Steps run in order and the
  completed ones are undone in reverse when one fails, because that is the order
  the world was changed in. A step whose `Undo` is nil is skipped rather than
  refused — a step that only read something needs no undo — and a compensation
  that itself fails does not stop the others: it is collected into
  `SagaResult.Unresolved`, which is the list to alert on, because those are
  real-world effects that happened, were meant to be undone, were not, and that
  no retry will resolve.

  `Run` returns a `SagaResult` rather than an error, matching Java's decision
  and for its reason: a failed saga is not an exceptional condition to a caller
  that has to decide what happens next. A step that panics is treated as a step
  that failed, so everything before it is still compensated.

  Nothing is published and no header is set, so this is the same idea as Java's
  `Saga` rather than the other end of one conversation. What matches is the
  behaviour.

- **`patterns.Scheduler`, for delivering a message later.** A ladder of classic
  queues with uniform times to live — `acemq.schedule.1h`, `.10m`, `.1m`, `.10s`,
  `.1s` — bound to the `acemq.schedule` direct exchange and dead-lettering into
  `acemq.schedule.due`, where the scheduler either delivers the message or moves
  it to the largest rung that does not overshoot what is left. A one-day delay is
  twenty-four hops and a one-minute delay is one.

  Not a per-message expiration, which is the usual suggestion and is wrong for
  anything but a single fixed delay: a classic queue expires messages only at its
  head, so a one-minute message behind a four-hour one is delivered in four hours
  and nothing reports it.

  Every name and argument is the cross-language contract Java already writes.
  Each rung carries exactly `x-message-ttl`, `x-dead-letter-exchange` and
  `x-dead-letter-routing-key` and is classic — which is the absence of
  `x-queue-type`, not `x-queue-type=classic` — so a Go service and a Java service
  scheduling on one broker declare the identical queue rather than an equivalent
  one. A message carries `x-schedule-exchange`, `x-schedule-routing-key`,
  `x-schedule-due-at` (epoch milliseconds, as Java's `Instant.toEpochMilli`
  writes it) and `x-schedule-content-type`, deliberately outside the reserved
  `x-acemq-` namespace, which the envelope drops on the way in. None of the four
  reaches the consumer.

  The control consumer reads raw bytes at prefetch 50 and never decodes a
  payload. `patterns.ScheduleTopology()` is the whole declaration, exported so a
  deployment can apply it up front or compare it with another AceMQ library's by
  eye.

## [0.3.0] - 2026-09-08

### Changed

- **A consumer declares the dead-letter half of its topology when it starts.**
  `RetryLadder.Declare` created the retry exchange, the rungs and the binding
  that brings an expired message home; it now creates `acemq.dlx`,
  `{queue}.dlq`, `{queue}.parked` and their two bindings as well, and
  `acemq.Consume` calls it once as the consumer starts. Java's consumer half has
  always declared them, which is why the cross-language fixture marks all five
  `declaredBy: "both"` (ADR-032).

  The union of the two halves has not changed, so a service that applies its
  `Topology` sees the same broker as before — the declarations are idempotent
  and use the arguments `Topology` uses, in either order. What is gone is the
  failure mode when the topology was never applied: a consumer that gave up
  republished into a queue nobody had declared, and the broker discards an
  unroutable message without a trace. The one message somebody had just decided
  was worth keeping was the one that vanished.

  The dead-letter half is declared whether or not there is a retry policy,
  because a rejection, a fatal error, an interceptor that refuses a message and
  a body that will not decode all reach `{queue}.dlq` or `{queue}.parked`
  without consulting one. The retry exchange and the rungs are still declared
  only when the policy has waits long enough to need them.

## [0.2.0] — 2026-09-07

> ### ⚠ Migrating: a retry is republished, and a dead-lettered queue gains
> two arguments
>
> Two changes touch a broker that is already running 0.1.x.
>
> **`Topology.DeadLetters(queue)` now stamps the source queue** with
> `x-dead-letter-exchange: acemq.dlx` and `x-dead-letter-routing-key:
> {queue}.dlq`. A queue that already exists without them cannot be redeclared
> with them: AMQP forbids changing a queue's arguments in place, so the declare
> is refused with `PRECONDITION_FAILED`. Drain and recreate the queue, or
> declare it yourself with the new arguments.
>
> **A retry is now republished rather than requeued**, so a message that is
> retried goes to the back of its queue rather than the front, and
> `x-acemq-attempt` really advances instead of the count living in the memory of
> the process that has been failing.
>
> Both changes exist because the other four libraries already behaved this way,
> and two services on one queue that disagree about its arguments cannot both
> consume it.

> ### ⚠ Migrating: a durable queue is now a quorum queue
>
> **`mq.DeclareQueue(...)` and `Topology.Queue(...)` now declare a durable queue
> with `x-queue-type: quorum`.** Java, .NET, Python and Ruby all do, and
> `x-queue-type` is compared by the broker as strictly as any other argument: a
> Go service and a Java service consuming `orders` both declare `orders`, and
> until now they disagreed about what `orders` is.
>
> **A queue that already exists as classic cannot be redeclared as quorum.** A
> queue's type is fixed when it is created, so the declaration is refused with
> `PRECONDITION_FAILED` and the service does not start. There is no in-place
> conversion and no flag that performs one. Drain the queue and recreate it, or
> keep it as it is by declaring it `acemq.OfType(acemq.QueueClassic)`.
>
> Quorum rather than classic because Java has been declaring source queues
> quorum since before the other libraries existed and has deployments with
> quorum queues on real brokers — and a queue that exists as quorum cannot be
> redeclared as anything else. Classic was never still available as the answer
> all five could give.
>
> Three things stay classic and need no action:
> `{queue}.retry.{delay}`, `{queue}.dlq` and `{queue}.parked`, which Java
> declares classic too; anything `Exclusive()`, `AutoDelete()` or `Transient()`,
> because RabbitMQ allows a quorum queue to be none of the three — this covers
> the reply queue a `patterns.Requester` generates and the health probe; and
> anything that names its own type.

### Added

- **A retry ladder.** Delays at or above 30 seconds wait in the broker, in a
  `{queue}.retry.{delay}` queue whose `x-message-ttl` is the wait and whose
  dead-letter target returns it to the source queue through `acemq.retry`.
  Shorter delays still wait in the consumer. The threshold is configurable, and
  `WaitInBrokerFrom(0)` keeps every wait in the process. A consumer sleeping on
  a five-minute backoff loses the whole wait when it restarts — the broker
  redelivers the unacknowledged message at once, so a five-minute policy
  delivers in none.
- **`acemq.dlx`**, a durable direct exchange reaching `{queue}.dlq` and
  `{queue}.parked`, with the source queue pointed at it. This is a backstop
  underneath the republish path, not a replacement for it: it catches what the
  library never sees — a source-queue TTL expiring, an `x-max-length` drop, a
  rejection from something that is not this library.
- `Replay` resets `x-acemq-attempt` to 1, with `KeepAttempts: true` to opt out.
- A release preflight, so a tag is not how a problem gets found.

### Changed

- **A retry is republished with the attempt advanced, not requeued**, and
  dead-lettering and parking republish with the reason and then acknowledge the
  original rather than using `nack(requeue: false)`. See the migration note.
- **`FixedRetry` no longer applies jitter.** It defaulted to 0.2, which the
  Python and Ruby libraries do not; avoiding the spread is the reason to ask for
  a fixed policy in the first place.
- **A durable queue is declared quorum**, matching the other four libraries. See
  the migration note. `acemq.OfType(acemq.QueueClassic)` asks for classic, and
  now sends no `x-queue-type` argument at all rather than `x-queue-type:
  classic` — which is what classic is on the wire everywhere else, so a rung
  declared by a Java service and redeclared here produces the identical argument
  table rather than an equivalent one.
- **A topology plan names every queue's type in words**, classic ones included,
  so that the queues that must not be quorum do not read as queues nobody
  thought about: `declare queue orders (durable, quorum, …)` beside `declare
  queue orders.dlq (durable, classic)`. `x-queue-type` is no longer listed among
  the arguments, since it would be that same word twice.

### Fixed

- **`Consumer.Close` released the AMQP channel before draining handlers**, so a
  settlement still in flight went into a dead channel. Invisible while a retry
  was a requeue; once a retry became a republish it produced a genuine duplicate
  — the rung held a copy and the source queue got the original back.
- `removeAtEnd` returned at the first failed delete and skipped the rest, so one
  missing queue left every later one behind.
