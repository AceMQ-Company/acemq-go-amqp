# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

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

### Fixed

- **`Consumer.Close` released the AMQP channel before draining handlers**, so a
  settlement still in flight went into a dead channel. Invisible while a retry
  was a requeue; once a retry became a republish it produced a genuine duplicate
  — the rung held a copy and the source queue got the original back.
- `removeAtEnd` returned at the first failed delete and skipped the rest, so one
  missing queue left every later one behind.
