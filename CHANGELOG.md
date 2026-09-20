# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Security

- **`telemetry/otel` moves to OpenTelemetry v1.46.0, clearing four advisories
  against v1.38.0. None of the four was reachable through this module, and the
  core library was never affected by any of them.** The core is a separate
  module and does not depend on OpenTelemetry at all, so a service that does not
  import `telemetry/otel` was never exposed — that is the whole point of the
  split, and it held.

  What the four actually are, and where each one stops:

  - **[GHSA-9h8m-3fm2-qjrq][] / CVE-2026-24051** (high) and
    **[GHSA-hfvc-g4fc-pqhx][] / CVE-2026-39883** (high), both in
    `go.opentelemetry.io/otel/sdk`, are the same defect twice: the SDK's host-ID
    resource detector ran `ioreg` on macOS, and then `kenv` on the BSDs and
    Solaris, by bare name rather than absolute path, so a local attacker who
    could prepend to `PATH` got arbitrary code execution inside the application.
    The detector is `resource.WithHostID()`, which is opt-in — `resource.Default()`
    is service name, environment and SDK attributes and nothing else — and this
    module never builds a resource, never constructs a `TracerProvider`, and
    imports `otel/sdk` only from its tests. Reaching it needed an application to
    ask for host-ID detection itself, on one of those platforms, with a
    compromised `PATH`.

  - **[GHSA-mh2q-q3fh-2475][] / CVE-2026-29181** (high) in
    `go.opentelemetry.io/otel` is remote DoS amplification in baggage
    extraction: `extractMultiBaggage` parsed each `baggage` header value against
    its own 8192-byte limit rather than a combined one, so many header lines
    multiplied CPU and allocations. Two things keep it out of reach here. The
    multi-value path is taken only when the carrier implements
    `propagation.ValuesGetter` — an HTTP header map — and this module's carrier
    over AMQP headers offers `Get`, `Set` and `Keys` only, so extraction takes
    the single-value path. And the default propagator is `TraceContext`, not
    `Baggage`. This one is an HTTP-server-instrumentation bug, the way the
    Micrometer advisory before it was; a message header is not a repeated HTTP
    field.

  - **[GHSA-8wmf-6v46-5gfg][] / CVE-2026-81870** (low) in
    `go.opentelemetry.io/otel/sdk` is `sdk/trace.NewTracerProvider` logging its
    exporter configuration at Info, which discloses an endpoint URL and any
    credentials embedded in one. The application builds the `TracerProvider`,
    never this module — `otel.New()` takes the process's provider precisely so
    the exporter stays the application's business — and the only provider built
    in this repository is a test's, over an in-memory exporter with no endpoint.

  Bumped rather than pinned because being unreachable is not a reason to ship
  known-vulnerable versions: reachability is an argument about today's call
  sites, and the call sites move.

- **`telemetry/otel` now declares `go 1.25`; the core library still declares
  `go 1.23`.** Every OpenTelemetry release from v1.39.0 on raises its own Go
  floor — 1.24 at v1.39.0, 1.25 at v1.45.0 — and the earliest release clearing
  all four advisories is v1.45.0. The note in that module's `go.mod` about
  v1.38.0 being the newest that builds on Go 1.23 is therefore retired: holding
  the floor now means shipping the unpatched versions, which is the worse trade.

  The core's floor is untouched and its `go.mod` and `go.sum` are byte-identical
  — no OpenTelemetry module appears in either, and none of the codec modules
  moved. A project on Go 1.23 can still take the library; it cannot take
  `telemetry/otel` with it. `patterns/sqltest` already sat at 1.25 for a
  comparable reason. See [docs/observability.md](docs/observability.md).

  No span name, attribute or propagation assertion changed. The tracing tests
  are unedited and the same 37 pass on v1.46.0 as on v1.38.0.

[GHSA-9h8m-3fm2-qjrq]: https://github.com/open-telemetry/opentelemetry-go/security/advisories/GHSA-9h8m-3fm2-qjrq
[GHSA-hfvc-g4fc-pqhx]: https://github.com/open-telemetry/opentelemetry-go/security/advisories/GHSA-hfvc-g4fc-pqhx
[GHSA-mh2q-q3fh-2475]: https://github.com/open-telemetry/opentelemetry-go/security/advisories/GHSA-mh2q-q3fh-2475
[GHSA-8wmf-6v46-5gfg]: https://github.com/open-telemetry/opentelemetry-go/security/advisories/GHSA-8wmf-6v46-5gfg

## [0.7.0] - 2026-09-20

### Fixed

- **`Conn.Health` no longer hangs on a connection the broker has blocked. It is
  the defect this library was held up as the reference for not having.**
  `docs/lifecycle.md` argued correctly that a blocked connection must not be
  reported unhealthy — Ruby, Python and .NET were all pointed at that page while
  they were fixed — and the check itself had no notion of blocking at all.

  RabbitMQ sends `connection.blocked` when it is low on memory or disk and then
  **stops reading that connection**. The probe is a `queue.declare`, so on a
  blocked connection it is not refused: it goes unanswered for as long as the
  alarm lasts. Measured against a broker with the memory high watermark at
  `0.0001`, `Health` given a five-second context **had still not returned after
  twelve seconds**; it now answers in **under a millisecond**, `up`, with the
  broker's own reason:

  ```go
  report := mq.Health(ctx)
  // {Status: up,
  //  Detail: "the broker has blocked this connection; publishing is paused: low on memory",
  //  Parts: {consumers: 3, blocked: true, blockedReason: "low on memory"}}
  ```

  **The round trip is skipped rather than shortened**, because the broker telling
  this socket that it blocked it is livelier proof that the broker is there than
  any declaration could be. **And the answer is `up`**: a blocked connection is
  the broker protecting itself, and an application that fails its own readiness
  check for it is one an orchestrator restarts into the same blocked broker,
  having thrown away whatever it was holding — a fleet doing that together stops
  draining the queues at the moment the broker most needs them drained. Java's
  `AceMqHealthIndicator`, .NET's `Health()`, Python's `health()` and Ruby's
  `Health` say the same sentence; the text before the colon is fixed, because it
  is what an alert rule matches on.

  It is not `degraded` either, which is what `docs/lifecycle.md` used to
  recommend composing by hand. Degraded is for this instance being worse at its
  job than it should be. A block is the broker's state, identical across every
  replica, and an alert that fires for all of them at once is one no deployment
  can act on.

  Asserted against a broker that is really blocked, in
  `rabbitmq/blocked_test.go`: it drops the memory high watermark with
  `rabbitmqctl`, publishes so that the connection is one the broker blocks —
  RabbitMQ sends the frame only to a connection that publishes under an alarm —
  waits for it to arrive, and puts the watermark back before anything is closed,
  because closing a blocked connection waits on a broker that is not reading. A
  test that sets a flag and asserts on the flag proves the flag is readable and
  nothing else. It reaches `rabbitmqctl` through `ACEMQ_TEST_RABBITMQCTL`, a
  command prefix — `docker exec <container> rabbitmqctl`, or just `rabbitmqctl` —
  and **fails rather than skipping** when it is unset, because a skipping test is
  how this survived three libraries. CI sets it.

- **`Conn.Health` has a deadline.** Its own `HealthCheck` documentation says a
  check must not hang, and this one could: the RabbitMQ transport's
  `DeclareQueue` took a `context.Context` and discarded it, because amqp091-go's
  declarations are request-and-wait calls with no context in their signatures. A
  context that reaches a call which ignores it is not a deadline, and a readiness
  endpoint wired to this one hung with it.

  The probe is now bounded by `ctx`, and by the new `acemq.DefaultHealthTimeout`
  — three seconds — when `ctx` carries no deadline of its own, which is the shape
  `http.Request.Context()` has. A probe that runs out of time is **abandoned**
  rather than waited for: cancelling a request to a broker that is not reading
  means waiting for a cancellation that travels the same way the request did. A
  block that arrives *during* a probe is read as the explanation for the silence
  rather than as a second fault beside it.

  `ConnHealth` takes a `Timeout` of its own, the same three seconds by default
  against the actuator's five, so a broker that has gone quiet is described by
  the check that looked rather than by the aggregate giving up on it.
  `rabbitmq.Transport`'s declarations now honour their context at the one point
  where honouring it costs nothing — they refuse to put a frame on the wire for a
  caller whose deadline has already gone — and say in their documentation that
  this is all a context can do there.

- **`AggregateHealth` is bounded by its context, and keeps what the parts
  said.** It waited for every check to answer with no deadline at all, so one
  check that hung hung the whole report; a check that does not answer in time is
  now reported `down` **by name**, alongside the answers that did arrive, which
  is what an operator needs and what a bare timeout on the endpoint would have
  hidden.

  Its detail now names every part that had something to say and repeats what it
  said, not only the parts that were not up. Summarising by status alone answered
  `up` with an **empty** detail for an aggregate holding a blocked connection —
  measured, with the reason sitting in the parts the whole time — throwing away
  the one fact the check had gone to the trouble of finding at exactly the line
  an operator reads first. Ruby had the identical defect.

  ```
  up: broker: the broker has blocked this connection; publishing is paused: low on memory
  ```

- **`Conn.Health` leaves one probe queue on the broker instead of one per
  check.** The probe declared `acemq-health-{a fresh id}` every time, exclusive
  and auto-deleting — and an auto-delete queue that never had a consumer is never
  auto-deleted, so each one stayed until the connection went. Three checks left
  three queues, measured on a real broker. A readiness probe runs every few
  seconds for the life of a process. The name is now generated once per
  connection and reused, which is still unique to this connection and so still
  cannot collide with another instance running the same check.

### Added

- **`Conn.Blocked` and `Conn.BlockedReason`: whether the broker has blocked this
  connection, free, with no round trip.** For a publisher that would rather shed
  load than hand a message to a broker that has stopped reading, and for a health
  check that wants to tell back pressure from a wedged socket.

  ```go
  if reason := mq.BlockedReason(); reason != "" { … }

  blocked, known := mq.Blocked() // known is false when nobody could be asked
  ```

  The state was only ever on `*rabbitmq.Transport`, so reaching it meant keeping
  the concrete transport handle beside the connection and passing it to anything
  that wanted to ask — which `docs/lifecycle.md` talked callers through. It now
  travels a `BlockedReporter` seam beside `CapabilityReporter`, which the
  RabbitMQ transport already satisfied and which a test double satisfies with one
  method; a transport that has never heard of it is simply never blocked.

  `Blocked`'s second return keeps "no" apart from "nobody looked". A transport
  that cannot be asked puts `blocked: null` in a health report rather than
  `blocked: false`, and in an incident those are not the same sentence. It is the
  shape Java has as `isBlocked()`, .NET as `IsBlocked`, Python as
  `Connection.blocked` and Ruby as `blocked?`.

- **`actuator.Options.WithoutConnHealth`, so `Conn` can feed `/acemq-info`
  without also being checked by `/acemq-health`.** Setting `Conn` appended the
  library's own `ConnHealth` beside whatever the application supplied, and
  `AggregateHealth` takes the worst report, so a more careful check composed
  beside it was overruled by the plain one. The only way out was to leave `Conn`
  unset, which took the transport's capabilities out of `/acemq-info` with it —
  `docs/lifecycle.md` called that a real trade with no way to have both, and .NET
  hit the same wall from the other side.

  Both halves are fixed. `ConnHealth` passes the connection's own answer through
  and adds no opinion of its own, so there is usually nothing to get out from
  under; and when there is, this is how:

  ```go
  act := actuator.New(actuator.Options{
  	Metrics:           metrics,
  	Conn:              mq,      // /acemq-info still lists the transport's capabilities
  	WithoutConnHealth: true,    // but /acemq-health takes only the checks below
  	Checks:            []acemq.HealthCheck{myBrokerCheck{mq}},
  })
  ```

- **A regression test for the 0.6.0 blocked-recovery fix, against a real
  block.** A blocked connection used to stay blocked for ever after a
  reconnection, because the reason was never cleared; the fix was covered by a
  unit test that set the flag itself. `rabbitmq/blocked_test.go` now blocks a
  connection with a genuine memory alarm, has the **broker** close it with
  `close_all_connections`, and asserts the reason is clear on the connection that
  replaces it. The alarm is left on across the reconnection and the fresh
  connection is then published on until it blocks in its turn, which is how the
  test knows the state was cleared by this library rather than by the broker
  having sent an unblock. It holds.

### Changed

- **`Conn.Health` reports `blocked` in its parts, and a blocked connection as
  `up`.** A behaviour change to a published API. A readiness probe that was
  taking instances out of rotation for a blocked broker will stop doing so, which
  is the point. `/acemq-health` answered 503 for it before and answers 200 now.

  **If you want the old reading**, make it your application's policy rather than
  the library's, by adding a check that says so beside the connection's:

  ```go
  type blockedIsDegraded struct{ mq *acemq.Conn }

  func (b blockedIsDegraded) Name() string { return "broker-pressure" }

  func (b blockedIsDegraded) Check(context.Context) acemq.HealthReport {
  	if reason := b.mq.BlockedReason(); reason != "" {
  		return acemq.HealthReport{
  			Status: acemq.HealthDegraded, Detail: reason, Checked: time.Now().UTC()}
  	}
  	return acemq.HealthReport{Status: acemq.HealthUp, Checked: time.Now().UTC()}
  }
  ```

  Handed to `actuator.Options.Checks`, it is one named component's opinion,
  which a reader can see the source of and a caller can choose not to add.

### Documentation

- **`docs/lifecycle.md` no longer talks callers through composing a blocked-aware
  health check by hand, because the library now is one.** The page kept the
  argument — a blocked connection is not a reason to restart — and replaced the
  worked example with what `Conn.Health` does, the measurement behind it, and the
  reason `up` beats `degraded` for a state every replica shares. Its claim that
  `Conn` and the transport capabilities were a trade with no way out is gone with
  the trade. `docs/observability.md` carries the new parts, the deadline, and the
  aggregate's detail.

## [0.6.1] - 2026-09-17

### Fixed

- **The nested modules asked for the wrong parent.** Every module with its own
  `go.mod` — the four codecs, `telemetry/otel` and `patterns/sqltest` — still
  required `github.com/AceMQ-Company/acemq-go-amqp v0.5.0` when 0.6.0 was
  tagged. They now require `v0.6.1`.

  **What this meant for 0.6.0.** A consumer requiring the parent *and* a codec at
  `v0.6.0` resolved the 0.6.0 parent and was unaffected — minimal version
  selection takes the higher of the two. A consumer requiring only
  `codec/avro@v0.6.0` silently resolved the **0.5.0** parent. It compiled, so
  nothing said otherwise, and the codec ran against an engine without this
  release's transport fixes.

  The 0.6.0 tags cannot be corrected. A Go tag is permanent: the proxy caches it
  on first fetch and deleting a tag un-publishes nothing. 0.6.1 carries the
  corrected requirement rather than any library change — the parent's source is
  identical to 0.6.0.

  **If you are on 0.6.0**, move to 0.6.1. If your `go.mod` already names the
  parent at `v0.6.0` this is tidiness; if it names only a codec, it is the fix.

## [0.6.0] - 2026-09-17

### Removed

- **Breaking change: reading the legacy Go encryption framing. `crypto` reads one
  framing now, and a body written by this library at v0.3.0 or earlier can no
  longer be opened.** This is the removal 0.5.0 deprecated, on the release it was
  promised for.

  Up to v0.3.0 this library framed an encrypted body as a version byte, a
  two-byte big-endian key id length, the key id, the nonce and the ciphertext,
  with no magic byte in front — a shape no other AceMQ library could read. 0.5.0
  moved to the family framing, `0xAE` and a one-byte length, and kept reading the
  old one so that a queue filled before the change could be drained by a consumer
  that had already been upgraded. That affordance is gone. `crypto.Codec.Decode`
  and `crypto.KeyIDOf` accept `0xAE` and nothing else, and the constants and the
  unframing function that existed only to read the old shape have gone with it,
  along with the tests that pinned the behaviour.

  **There is no partial read and no recovery path, because these are encrypted
  payloads.** The bytes are refused, not misread — the two framings are
  unambiguously distinguishable by their first byte, which is what made keeping
  both safe and makes dropping one safe as well — but refused is refused. A body
  beginning `0x01` now fails exactly as an unencrypted body does, fatally, with
  the error that says the message is not in the framing `crypto.Codec` reads. It
  fails that way for the holder of the very key that wrote it.

  **`crypto.KeyIDOf` stops answering for those bodies too**, and that deserves
  saying plainly: it reads a key id out of the header without needing any key,
  which makes it the natural tool for an operator triaging a dead-letter queue and
  working out which key a stuck message needs. Pointed at one of these bodies it
  now returns the same refusal rather than a key id. There is no longer any way in
  this library to learn which key one of them was written with — the id is still
  in the bytes, in the clear, three bytes in, but nothing here will read it out.

  **What to do about it.** The instruction since 0.5.0 has been to drain the
  queues holding those bodies or re-encrypt their contents before upgrading past
  the removal, and that was the moment to do it. If it was not done, the way back
  is a v0.5.x build: 0.5.0 and 0.5.x read both framings, so pin one, drain or
  re-encrypt what is still sitting there, and upgrade afterwards. Nothing in this
  library can be configured to read the old framing again — there is no option, no
  constructor and no environment variable, exactly as there was never one to write
  it, because two readers of a divergence outlive two writers of it.

  **The date moved once, and this is it being kept rather than moved again.** The
  original deprecation text said the removal landed in v0.5.0 — the release that
  introduced the deprecation — which made the migration window zero releases long
  and broke the promise on the day it shipped. That was corrected to v0.6.0, the
  successor release, in every place the promise was written down. This is that
  release.

  The exposure was always narrow. The old framing was written only up to v0.3.0,
  tagged 2026-09-08, and 0.5.0 landed 2026-09-09, so the window in which a
  released version of this library wrote those bytes was about a day, on a library
  distributed through the Go module proxy alone.

- **`acemq.HeaderReplayedFrom`, `acemq.HeaderReplayedAt` and
  `acemq.HeaderReplayCount`, which named three headers this library never
  wrote.** They were `x-acemq-replayed-from`, `x-acemq-replayed-at` and
  `x-acemq-replay-count`, and no code here has ever put them on a message — nor
  read one. (.NET does write those spellings, and this library drops them on the
  way in exactly as it drops any other unrecognised reserved header; see the
  patterns.md note under Fixed.)

  The real stamps are `patterns.HeaderReplayedFrom`, `HeaderReplayedAt` and
  `HeaderReplayCount` — `acemq-replayed-from`, `acemq-replayed-at` and
  `acemq-replay-count`, without the `x-`. `patterns.Replay` has always written
  those, the envelope fixtures Java generates carry those, and Java moved its own
  write side onto them in 0.5.0.

  **The missing prefix is the design, not an inconsistency to be tidied up.**
  `x-acemq-` is the engine's namespace, and any header in it that the engine does
  not materialise onto `acemq.Envelope` is dropped on the way in. A replay stamp
  written there would reach the broker and vanish before the handler saw it —
  which is exactly what a port implementing against the deleted constants would
  have built. Two names for one thing, one of them fiction, is worse than one
  name; a constant nothing writes reads as a supported feature.

  The doc comment on the deleted `HeaderReplayedAt` also claimed its value
  matched Java's. It no longer did: Java writes RFC 3339 there as of 0.5.0,
  matching what this library, .NET, Python and Ruby already wrote.

  Nothing in this repository referenced the deleted constants, so no code that
  compiled against 0.5.0 breaks unless it named them directly — in which case it
  was looking for a header that was never on the wire. `amqp/headers.go` keeps a
  note where they were, so the next port does not put them back.

### Fixed

- **Publishing raced the connection recovery. `go test -race` found it on CI,
  during a broker restart, and nothing that runs on a laptop could have.**
  `Transport.Publish` read `confirming` — the flag that decides whether a publish
  waits for a confirm — outside the mutex `reconnectOnce` holds while it writes
  it. Both accesses were introduced by the two entries below: taking the confirm
  wait out from under the channel lock narrowed the critical section to the write
  itself, which was the point, and left the branch that chooses between the two
  publish paths reading a field that a reconnection assigns.

  **The pipelining is untouched.** The confirm is still awaited outside the lock
  and a batch still costs one broker round trip; the lock was not widened back.
  The field is simply no longer written on reconnection at all. A recovery
  redials with the transport's own `WithoutConfirms`, and `Dial` fails rather
  than handing back a connection without the confirms it was asked for, so
  `confirming` is whatever `Dial` decided and cannot become anything else.
  Assigning it again was writing the value that was already there — a data race
  whose only effect, besides being one, was nothing.

  **Three more unsynchronised reads went with it**, found by auditing every
  field the recovery path writes against every field publishing touches rather
  than by fixing the one line the report named. `conn` is replaced on
  reconnection too, and `Consume`, `CheckQueue`, `MessageCount`, `DeleteQueue`
  and `QueueExists` all read it without the lock to open a channel of their own;
  they take a snapshot under it now. `Supports(CapabilityPublisherConfirms)` read
  `confirming` unguarded as well, which the same fix settles. `Close` read `conn`
  outside the lock it had just released, which was safe only because the recovery
  goroutine has already been waited for by then; it now reads it inside.

  **A publish interrupted by the reconnection no longer reports that the broker
  refused it.** The client answers every publish still waiting on a channel that
  closes with a nack, which is true of the channel and a lie about the broker, and
  the transport passed that on as `the broker refused it` — sending the caller to
  look for a fault in a message the broker never objected to and may well have
  taken. A publish now carries the channel and a connection generation from the
  write to the confirm, and a nack that arrives with either of them gone says the
  connection was lost or the channel closed before the broker confirmed it, that
  whether it arrived is unknown, and that republishing is for callers who can
  afford a duplicate.

  **This is the argument for the integration job.** The race needs a connection
  to be replaced underneath a publish in flight, which means a broker going away
  while the process keeps running — nothing a unit test or a local
  `go test ./...` does. `rabbitmq/recovery_test.go` is the regression test and
  needs no restart: it drives the recovery path by hand while sixteen goroutines
  publish, and with the defect restored it reproduced the race in six runs out of
  six.

- **A connection the broker had blocked stayed blocked for ever after a
  reconnection.** `connection.blocked` is recorded so that publishing can be
  refused rather than hang, and `Publish` consults it first. The reason was not
  cleared when the connection it belonged to was replaced, and the `unblocked`
  that would have cleared it could only arrive on a connection nobody was
  watching — recovery dials a replacement whose own watcher writes into a struct
  that is then discarded. A broker that blocked a connection while it was low on
  disk, and dropped it before recovering, left every subsequent publish on the
  new connection refused with a reason from a connection that no longer existed.
  The state is cleared on reconnection now, and the new connection is watched.

- **Documentation said the stream prefetch default was a gap between the
  libraries. It is not, and [docs/streams.md](docs/streams.md#prefetch) now says
  so.** Go supplies 10 when `StreamOptions.Prefetch` is unset, Java and .NET
  supply 100, and the page read as though somebody had got it wrong.

  **The number is unchanged in all three.** What changed is the framing, and the
  same wording is going into the other four repositories so the five pages agree:
  the default is a per-library choice about memory against throughput, and **it
  is not part of the cross-language contract**. What the five libraries promise
  each other is the wire — the header names, the envelope fields, the retention
  arguments, the `x-stream-offset` a consumer sends and the one it reads back.
  How many messages one library's reader keeps buffered while a handler works is
  a local decision about the memory of the process it runs in, and matching
  numbers would not make a Go service and a Java service behave alike anyway,
  because the handlers, the payloads and the machines all differ.

  **What you do about it:** set it. Ten is conservative — a handler doing real
  work per message holds ten bodies rather than a hundred — and slow for a
  projection reading a large history, which is the ordinary reason to open a
  stream from `FromFirst`. A service that cares about the number should say it
  rather than inherit it, in any of the five libraries.

  ```go
  patterns.StreamOptions{Offset: patterns.FromFirst(), Prefetch: 500}
  ```

- **`patterns.FromOffset` produced a consumer the broker refused, and nothing
  noticed because `patterns/streams.go` had no tests at all.** An exact offset
  was put on the wire as the `uint64` the function takes. AMQP's field table has
  no unsigned long, so the client would not encode it and the subscribe failed
  with `table field "x-stream-offset" value uint64 not supported` — which means
  the one position a reader resuming from a checkpoint needs has never worked
  since it was added.

  It goes out as an `int64` now. `FromOffset` still takes a `uint64`, because an
  offset is a position in a log and cannot be negative; the conversion belongs
  at the wire and not in the signature. An offset past the signed range is
  clamped rather than allowed to wrap into a negative the broker would read as
  something else entirely.

  This is the first thing the new broker-backed tests found, which is the
  argument for having written them: `memory://` refuses streams on purpose, so
  every one of these arguments had been checked against the documentation and
  nothing else. `patterns/streams_test.go` and `patterns/streams_wire_test.go`
  now cover the parts that need no broker — the retention arguments, the offset
  argument, the prefetch default, the consumer tag, the retry refusal and the
  offset reader — and `rabbitmq/streams_test.go` covers the parts that do:
  declaring a stream the broker agrees is one, reading history from `FromFirst`,
  two readers of one stream both seeing everything, resuming from a recorded
  offset with no gap and no repeat, and a refused retry landing in
  `{stream}.parked` while the log stays exactly as long as it was.

- **`Publisher.SendAll` pipelined at the library layer and then waited one
  message at a time underneath, so a batch bought no throughput at all.** The
  RabbitMQ transport took its publishing mutex at the top of `Transport.Publish`
  and held it — deferred unlock — across `DeferredConfirmation.WaitContext`. Every
  publish therefore owned the channel for a full broker round trip, and a batch
  of N that `SendAll` had carefully started on N goroutines queued up behind that
  lock and paid N round trips anyway. The batch tests could not see it: the
  in-memory transport has no round trip to pay.

  The lock now covers the write and only the write. An AMQP channel is still not
  safe for concurrent writes, and the sequence number a confirm will name is
  assigned by the write itself, so both stay inside; the wait moved out. Measured
  against a local broker, a batch of 200 messages with a 1.15 ms round trip went
  from **364 ms to 12 ms** — from slightly worse than a loop over `Send` to about
  nineteen times better than one. `rabbitmq/confirms_test.go` asserts the batch
  costs less than a quarter of what one round trip per message would, which is
  the assertion that failed before this change.

- **Nothing bounded how many publishes could be unconfirmed at once, and now
  something does.** With the confirm wait under the lock the bound was one, by
  accident. Taking the wait out from under the lock without replacing that bound
  would have traded a throughput bug for a memory one: a caller publishing faster
  than the broker confirms accumulates unconfirmed bodies until the process dies,
  which looks like throughput right up to the moment it does not.

  `rabbitmq.Config.MaxOutstandingPublishes` is the bound, and a publish takes its
  slot **before** the write — after the write is too late to refuse one. It
  defaults to **1000**, matching `maxOutstandingPublishes` in Java's
  `ConnectionConfig` and `MaxOutstandingPublishes` in .NET's, so the same batch
  behaves the same whichever library sends it.

  ```go
  transport, err := rabbitmq.Dial(ctx, url, rabbitmq.Config{
      MaxOutstandingPublishes: 200,
  })
  ```

  A publish that finds every slot taken waits for one and fails when its context
  is done, rather than waiting for ever:

  ```
  acemq: 1000 publishes are already waiting for a confirm and none completed in
  time; the broker is not keeping up, so publish more slowly rather than
  buffering more (rabbitmq.Config.MaxOutstandingPublishes)
  ```

  **What you do about it:** nothing, unless you were relying on the accidental
  serialisation. Raising the bound buys latency hiding and costs memory, because
  each slot holds a body the broker has not acknowledged. Setting it to `1`
  restores exactly the old behaviour — one round trip per message — for anywhere
  that wants it back.

- **A `basic.return` could be lost when several publishes were outstanding.** The
  broker sends a return before the confirm for the same message, which is why the
  transport drains rather than waits for one. The old drain put returns belonging
  to other publishes back into the buffered notify channel, which was fine while
  only one publish could be in flight and is not fine now: a full buffer drops
  what it cannot hold, and the publish that return belonged to would then report
  a message as routed that the broker had handed straight back.

  Returns are filed by message id instead and wait there for the publish they
  belong to, capped so a return nobody ever asks for — a publisher whose context
  was cancelled between the write and the confirm — cannot accumulate. The notify
  channel is also sized to the publish bound rather than to a fixed 64: the client
  hands a return to that channel with a blocking send from the same goroutine
  that resolves confirms, so a full buffer would stop confirms arriving while
  every publisher that could drain it waited for exactly those confirms.

- **Documentation said .NET could not read an encrypted body from this
  library.** `crypto`'s package comment, the doc on `crypto.ContentType`,
  [docs/security.md](docs/security.md) and
  [docs/serialization.md](docs/serialization.md) all described .NET as the
  remaining exception, using AES-256-CBC with a separate HMAC rather than
  AES-GCM. That was true up to .NET's 0.3.0 and stopped being true in its 0.5.0,
  which moved to AES-GCM in the family framing in the same round this library
  moved to it. A .NET producer and a Go consumer can share a key. What still does
  not cross is .NET's own *legacy* bodies, which only .NET reads; this library's
  own v0.3.0 bodies are now read by nothing at all — see Removed. Both of those
  old framings begin `0x01`, so one arriving here is refused rather than misread.

- **[docs/envelope.md](docs/envelope.md) documented the replay stamps under the
  wrong names**, said two and listed three, and described them as read when
  present, which nothing does. It now names the three that are really written,
  says they are deliberately outside the reserved namespace and why, and gives
  `acemq-replayed-at` as RFC 3339 against `x-acemq-first-seen`'s epoch
  milliseconds.

- **[docs/patterns.md](docs/patterns.md)** names the encoding of
  `acemq-replayed-at` and the reason the stamps carry no prefix, rather than
  listing the three names and leaving both to be inferred.

  It also records a divergence found while checking this, which no document in
  this repository mentioned: **.NET writes the `x-acemq-` spellings of all three
  replay stamps**, and writes them for real rather than merely declaring them.
  This library drops them, because that is what the reserved prefix means — so a
  message a .NET operator replayed by hand reaches a Go handler without its
  stamps. It costs an audit trail, not a message, and it is the .NET library's to
  fix; documented here so nobody writes a Go consumer that expects to tell a
  .NET-replayed message from an original.

- **The patterns list in [docs/index.md](docs/index.md)'s navigation** had not
  been updated since pipelines, routing slips, claim check, streams, consumer
  groups and the schema registry landed. The table earlier on the same page
  listed them all, so the page disagreed with itself.

- **Three doc comments that described something the code does not do**, all
  found by checking them before writing them into a page.

  `patterns.ReadStream` said acknowledging advances this consumer's position
  "so restarting from `FromNext` carries on rather than re-reading". It does not:
  the function always puts an explicit `x-stream-offset` on the wire, so where a
  run starts is `StreamOptions.Offset` and never a position the broker kept. A
  consumer restarted on `FromNext` misses everything published while it was down.
  The comment now says that, says what `acemq.Retry` does to a stream — appends a
  second copy of the message — and corrects "rejecting does not dead-letter it
  either", which is true of the broker's mechanism and not of this library's
  republish to `{stream}.dlq`.

  `otel.Tracing.PublishInterceptor` claimed it covered "the ones inside
  `patterns.Requester` and the outbox relay". The relay publishes through
  `Conn.PublishRaw`, which has no interceptor chain, so an outbox-relayed message
  never carried a trace context from it. The comment now lists what the chain
  does and does not reach.

  `patterns.Chain`'s example did not compile: it passed
  `patterns.Idempotent[OrderPlaced](store)` where a `Middleware[T]` is wanted,
  and `Idempotent` takes the handler as well as the store.
  `patterns.WithIdempotency` is the middleware, and the comment now says which is
  which.

- **[docs/observability.md](docs/observability.md) counted the metrics that are
  named but not written as four. There are five** — `acemq.consume.attempts`,
  `acemq.request.duration`, `acemq.request.total`,
  `acemq.pipeline.run.duration` and `acemq.pipeline.run.total`. The 0.5.0 entry
  below called them two. The table itself was right; only the sentence over it
  was wrong, which is the kind of error that makes a reader stop trusting the
  table.

- **Three passages dated changes relative to "this release" or "the previous
  release"**, written while 0.5.0 was still `## [Unreleased]` and wrong from the
  moment it was tagged: from 0.5.0 the previous release is 0.3.0, and the
  renaming happened *in* 0.5.0 rather than before it. The metric rename, the
  `key` to `routing.key` move and `Span.Failed`'s outcome attribute now name
  0.5.0 outright.

- **[docs/consuming.md](docs/consuming.md) said Python and Ruby were still adding
  a handler-requested park.** Both shipped theirs in the same round as this one —
  Python's `park()` and Ruby's `Ack.park` — so all five libraries have it.

### Added

- **`docs/lifecycle.md`, the shape of a long-running consumer process: signal
  handling, graceful drain, a bounded shutdown, and what a probe should say to an
  orchestrator.** Nothing about the library changes. What the page adds is the
  four things `Close` does at shutdown, stated separately because they are not
  the same thing, and two of them are not what the short answer in
  `reliability.md` implies.

  A handler in flight is finished — `Close` blocks on it, which is the guarantee
  and also the reason a shutdown needs a deadline. **A delivery that has arrived
  but reached no handler yet is finished too**, because the subscription waits for
  the delivery channel to close and the loop reading it passes on everything still
  in it: the work a drain has to get through is bounded by `Prefetch`, not by
  `Concurrency`, and with `Prefetch(100)` and a 200ms handler that is twenty
  seconds of work in hand before the drain starts. **A publish waiting for its
  confirm is cut off** — nothing tracks publishers, so nothing waits for them, and
  the error a cut-off publish returns is genuinely ambiguous about whether the
  broker took the message. **A retry waiting out a backoff in this process is
  waited out in full**, so a consumer whose rung queue is missing, on a five-minute
  schedule, makes the drain take five minutes, which is to say makes it fail.

  The consequence that earns the page is about **which context the consumers get**,
  and it is the opposite of the obvious answer. The context given to `Consume` is
  the one the engine settles on, and every interesting settlement is a publish: a
  retry republishes, a dead letter republishes onto `{queue}.dlq`, a park
  republishes onto `{queue}.parked`. All of them check the context first. So a
  consumer handed the signal context loses the ability to file messages correctly
  at the moment it is asked to stop — verified on `memory://`: with the context
  live a rejected message reaches `orders.dlq`, and with it cancelled a fraction of
  a second earlier the same rejection is a `basic.nack` with no requeue and
  `acemq.MetricSetAsideFailed`. Cancelling it is still the only lever that shortens
  a drain — an aborted ten-second in-process wait returned in microseconds, with
  the delivery requeued rather than lost — which makes it the thing to do *after*
  the deadline has gone, never before.

  `Close` takes no context and cannot be abandoned, which the page says plainly
  rather than showing a timeout that is not one: what a deadline bounds is how long
  the process waits before giving up, not how long `Close` takes, so it has to sit
  well inside `terminationGracePeriodSeconds`.

  On health, the page draws the line the Java starter's health indicator draws and
  Go's `ConnHealth` does not: **liveness must not consult the broker**, because a
  process that cannot reach its broker is not one a restart fixes, and a fleet
  restarting in unison drops every held message onto a broker that is already
  struggling. `Conn.Health` has no notion of a blocked connection — it declares a
  temporary queue and reports what came back — so a broker under disk pressure can
  read as `down`. The page composes a `HealthCheck` that asks
  `rabbitmq.Transport.BlockedReason()` first and reports `HealthDegraded`, and
  names the two wrinkles honestly: `BlockedReason()` is on the transport, so the
  connection has to be built as `rabbitmq.Dial` plus `acemq.NewConn` to keep the
  handle, and the check has to be passed through `actuator.Options.Checks` with
  `Conn` left unset, because setting `Conn` appends the library's own `ConnHealth`
  beside it and `AggregateHealth` takes the worst of them. The cost of leaving
  `Conn` unset is that `/acemq-info` stops listing the transport's capabilities.
  There is currently no way to have both.

  `MaxOutstandingPublishes` gets a shutdown reading as well: it is ordinarily a
  memory bound, and at `Close` it is the size of what gets cut off, so a batch
  larger than the bound is several waves of confirms and a signal partway through
  leaves the later waves in an unknown state. `SendAll` is where that is
  recoverable, through `BatchPublishFailedError.Confirmed` and the per-index
  `Errors`, and the page says to size a batch to the deadline or keep an outbox.

  **Go deliberately gets no dependency-injection integration**, and the page says
  so in its own section rather than leaving it to be inferred. The other four
  libraries each have one obvious convention to integrate with; Go has `fx` and
  `wire`, neither dominant, and a large share of services that use neither. There
  is also nothing to bridge: a constructor returning `(T, error)`, a `Close()` and
  a `context` on everything that blocks is exactly what `fx.Provide` and
  `fx.Lifecycle` want, and `wire` wants a provider function, which `NewConn` is.

  Every sample on the page was compiled and vetted as a package before being
  written down, and the four shutdown situations were run against `memory://`
  rather than reasoned about. The page is linked from the navigation, from
  `docs/index.md`, and from the shutdown sections of `reliability.md` and
  `consuming.md`, which now point at it rather than repeating it.

- **A source-side link check in `.github/scripts/build-docs-site.sh`, which reads
  `docs/*.md` rather than the rendered site.** It walks every markdown link,
  skips external, `mailto:` and pure-fragment targets, and requires each of the
  rest to exist as a file inside `docs/`. A `.html` target is reported as *a docs
  page link belongs in .md* rather than as a missing file, because that is the
  mistake it exists to catch. It runs before pandoc — it needs nothing rendered,
  and a bad link is cheapest to find before a minute of rendering.

  **The existing check could not find this class of bug even in principle.** It
  runs on the rewritten output, where cross-page links have already been turned
  from `.md` into `.html`, so it is satisfied by `guide.html` existing in `site/`
  whatever the markdown said. The `.md` spelling is the one that matters for
  anyone reading these pages on GitHub, which is where most people meet them
  first, and nothing was checking it. Java had 75 links written as `.html`:
  correct on the site, dead in the repository, and found by a person rather than
  by a build.

  These 21 pages and their 109 links pass unchanged — there was nothing to fix
  here. One thing did have to be adapted rather than copied: the check skips
  fenced blocks and inline code before scanning, because `](` is ordinary Go.
  Instantiating a generic is `patterns.WithTimeout[OrderPlaced](10*time.Second)`,
  and a link-shaped regex reads that as a link to `10*time.Second` — ten such
  false reports across these pages before the code was excluded. A link never
  lives inside code, so skipping it costs the check nothing.

- **`internal/testdata/avro-resolution-fixtures.json`, the shared fixture that
  pins when an AceMQ library resolves an Avro message onto a reader schema, and
  what the same bytes decode to when it does not.** Generated by Java, carried
  byte for byte by all five libraries, and asserted here from `codec/avro`.

  Nothing about this library's behaviour changes. The rule was already the rule
  and is the same one everywhere: **resolution happens when the library has a
  reader schema to resolve onto.** What differs between the languages is where a
  reader schema comes from, so they differ in how often there is one. Go has one
  when the caller passes `avro.ReaderSchema(...)` and not otherwise, because a Go
  struct carries no schema — which makes this the one library that demonstrates
  both of the fixture's columns through the same call with one option added, and
  both are now asserted against it.

  The case that earns the fixture is a field the **writer removed** that the
  reader declares with a default of `"GBP"`. Resolved, it arrives carrying that
  default; unresolved, the key is not there at all. The default is deliberately
  not the zero value: `""` would pass whether resolution happened or not, and
  `"GBP"` can only have come from the reader's declaration. The other direction —
  a field the writer added — is pinned too, precisely because it is the one
  everybody expects to be dangerous and is not.

  `docs/serialization.md` gains a **Schema resolution** section carrying the
  wording all five libraries share, including the table of which library lands on
  which column and why.

  The fixture and that table have both since been refreshed from Java, and the
  correction is worth naming because it ran through every library: the `.NET` row
  said *Always — the codec is constructed with a schema*, and .NET reaches
  `writerShape` too, through `WithoutReaderSchema()`. It now reads *By default*
  and says how the caller declines. The fixture's own `writerShape` description
  was narrowed in the same way — it described only a reader holding no reader
  schema, which left out the route Python and Ruby take to that column, holding
  the writer's schema as their own. Both corrections are prose. The bytes, the
  two cases, their schema ids and both decoded columns are unchanged, so every
  assertion here passes untouched, including the one that checks the fixture
  still files Go under `writerShape`. It asserts on the column rather than on the
  prose, which is why the reworded Go entry did not reach it.

- **`acemq.Envelope.Claim` and `acemq.Claim`, so the reserved `x-acemq-claim`
  header is a field rather than a trap.** The constant has been declared here
  since the header names were transliterated from Java, and nothing read it or
  wrote it. That is the worst arrangement available: `x-acemq-` is the engine's
  namespace, and **any header in it that this version does not materialise onto
  the envelope is dropped before a handler sees it** — so the name looked
  supported, a message carrying it arrived, and the value vanished.

  Python's `Envelope.claim` and Ruby's `:claim` are first-class fields that write
  exactly this header. A claim set by a Python or Ruby publisher now reaches a Go
  handler instead of being swallowed on the way in.

  ```go
  pub.Send(ctx, reference, acemq.Claim("s3://payloads/2026/09/order-1"))
  ```

  ```go
  func(ctx context.Context, m acemq.Message[Reference]) acemq.Ack {
      if m.Envelope.Claim != "" {
          // the payload is over there
      }
      return acemq.Accept()
  }
  ```

  A convention rather than a mechanism: nothing here reads it or fetches
  anything, and what the string means is between the publisher and the consumer.
  What it buys is an operator looking at a dead-lettered message being able to
  see where the payload went without decoding the body.

  **The claim-check pattern is unaffected and is a different thing.**
  `patterns.ClaimCheckCodec` frames the body — see the three bytes the family
  agreed on — because a header can be stripped by a shovel or a federation link
  and the body cannot, and because the framing has to say whether a payload
  travelled inline at all, which a present-or-absent header cannot express for a
  message that predates the codec. Setting `Claim` does not make a message a
  claim check, and the claim check does not set `Claim`.

  **Nothing changes for a message that has no claim.** An absent value is an
  absent header, never an empty one, exactly as with `x-acemq-causation` and
  `x-acemq-error`, so the bytes on the wire for every existing message are
  unchanged and the envelope fixtures still pass.

- **A requester and a responder count something at last.**
  `acemq.MetricRequestDuration` and `acemq.MetricRequestTotal` have been names
  this library declared and never wrote, and a `Responder` reported nothing at
  all where Java reports `answered()` and `unanswerable()`. Both gaps are closed,
  with Java's semantics rather than approximations of them.

  `patterns.Requester.Do` now writes both metrics, timing the round trip as the
  caller experienced it — from before the request is published to the moment `Do`
  is about to return, timeout included:

  ```
  acemq.request.total{routing.key="pricing", outcome="answered"}
  acemq.request.duration{routing.key="pricing", outcome="timed_out"}
  ```

  The publish was already timed by the publish metrics and the reply's delivery
  by the responder's consume metrics; neither of those is the number a blocked
  caller is holding, which is what this adds. `answered`, `timed_out` and
  `failed` are the outcomes, and the distinction between the last two is
  deliberate: a timeout is the absence of an answer and not evidence that nothing
  happened, so a counter calling it a failure sends somebody looking for one that
  did not occur. A reply that came back carrying the responder's error *is*
  `failed` — the round trip completed and the answer was bad news. The tracing
  adapter has drawn that line since it was written; the counter now uses the same
  word, so a counter and a trace queried for the same round trip agree.

  Java also tags `message.type` and `transport`. The type is built inside
  `Publisher.Send` from options `Do` only passes through, so writing it here
  would mean guessing at a value the caller may have overridden.

  `patterns.Responder` reports two numbers, and **the ordering is the point**:

  ```go
  responder.Answered()      // requests answered, counted before the reply left
  responder.Unanswerable()  // requests that named nowhere to reply
  ```

  `Answered()` is incremented **before** the reply is published, so a caller
  holding its answer can rely on the count already including it. The other order
  looks more natural and is wrong: it leaves a window in which the reply is in the
  caller's hands and the responder still says nothing has been answered, which is
  a dashboard reporting an idle service that is demonstrably working. A publish
  that fails hands its increment back, so this counts replies that were sent
  rather than replies that were attempted — without which counting early would
  introduce a failure of its own.

  The counters exist **before** `Serve` subscribes, and are reached by the handler
  through a value of their own rather than through the `Responder` the subscribe
  has not returned yet. A broker may hand the first request over from inside the
  subscribe, which is what a queue with a backlog looks like from in here, and the
  handler reads them on that very delivery. .NET had to lift its counters out of
  its responder to get this guarantee and Java initialises them at their
  declaration; this does the same thing in Go's idiom.

  A handler that returned an error is not counted as answered. The failure still
  goes back to the caller, but it is not an answer, and counting it as one would
  make a responder that fails every request look like one that works.

  `Unanswerable()` above zero means a caller is publishing where it means to
  request. This library dead-letters such a request where Java logs it and
  acknowledges it — both count it at the same moment and in the same way, and
  what differs is only where the message ends up. A request nobody can answer is
  worth keeping on `{queue}.dlq` for whoever has to find the sender.

  **What you do about it:** neither number needs a wait before it can be trusted,
  so code that sleeps before reading one is working around a defect that is not
  there. Dashboards that were graphing the responder's consume metrics as a proxy
  can keep doing so; `acemq.request.*` is the more direct answer and it is now
  there to use.

- **`patterns.ReadStream` refuses `acemq.Retry`, and says what to do instead.**
  A retry in this library republishes the message onto the queue it came from,
  with the attempt counter advanced, and acknowledges the original. On a stream
  "republish onto the queue it came from" means **appending a second copy to the
  log at a new offset** — which every other consumer of that stream then reads,
  and which a projection rebuilding from `FromFirst` next month reads as well. A
  handler failing on every message turned a stream into one that grew by a copy
  of itself per attempt. The page had said "do not do this" since it was written,
  which is not the same as the library not doing it.

  A handler that returns `acemq.Retry` now has the message parked — republished
  to `{stream}.parked`, which touches nothing in the stream — with a
  `patterns.RetryOnStreamError` as the reason:

  ```
  acemq: a handler on stream "orders.log" returned acemq.Retry for message
  order-1 at offset 41337, which this library refuses: a retry republishes the
  message onto the queue it came from, and on a stream that appends a second copy
  to the log at a new offset [...] A stream handler has two honest choices: park
  it deliberately with acemq.Park(err), or record the x-stream-offset header and
  return acemq.Accept() to checkpoint and move on.
  ```

  The handler's own error is wrapped, so `errors.Is` against a sentinel still
  matches through the refusal.

  **Java draws this line in the type system; Go draws it at the verb.** Java's
  `StreamConsumer` is a separate type from `MessageConsumer` exactly so that the
  outcomes a stream cannot honour are never on offer, because a single type
  covering both would be one where half the methods throw. Go has one
  `acemq.Handler` and `ReadStream` takes it, so the line is drawn when the verb is
  used rather than when it is spelled. Same intent, one step later.

  **What you do about it:** if any stream handler returns `acemq.Retry`, decide
  which of the two it meant. `acemq.Park(err)` if somebody should look at the
  message; record the offset and `acemq.Accept()` if the run should move on —
  and count the gap, because nothing else records it. Leaving it as `Retry` is
  now a parked message rather than a duplicated one, which is the safer failure
  but still a failure.

  `acemq.Reject` is unchanged and is worth saying out loud, because the table on
  the page now has three entries that read alike: it republishes a copy to
  `{stream}.dlq` and acknowledges the original, and **the original stays in the
  stream**. It is a copy, not a move, and it always was — a stream has no
  `x-dead-letter-exchange` and nothing can be removed from one.

- **`patterns.StreamOffsetOf`, which reads the `x-stream-offset` header off a
  delivery.** The checkpointing section of
  [docs/streams.md](docs/streams.md#nothing-remembers-your-position) carried this
  as a function to copy into your own code, because the broker writes an integer
  and which width it arrives as depends on the client. Copying a type switch out
  of a documentation page is how five services end up with five slightly
  different ones.

  ```go
  if offset, ok := patterns.StreamOffsetOf(m.Envelope); ok {
      checkpoints.Save(ctx, "projection-a", offset)
  }
  ```

  The second return is whether the delivery carried an offset at all, and it is
  not decoration. Zero is a real offset — the first message in the stream — so a
  reader that could not tell "the beginning" from "this delivery said nothing"
  would write a checkpoint of zero for a message that had no position, and the
  next run would replay everything believing it was resuming.

- **`acemq.Ack.IsRetry`, so another package can see which verb a handler used.**
  Added for one caller: `patterns.ReadStream` has to recognise a retry in order
  to refuse it, and the action behind an `Ack` is unexported. Without it the only
  way to ask would be to compare `Ack.String()` against the literal `"retry"` — a
  wire between two packages made of a word.

  It is not an invitation to second-guess handlers in general. Everything the
  engine does with an `Ack` is decided in `Settlement`, where the retry policy and
  the attempt counter live, and a caller branching on this instead is
  reimplementing that badly.

- **`Publisher.SendAll`, which publishes a batch and then waits for every
  confirm, instead of waiting for each one where it was published.** Java has had
  `Publisher.sendAll` and .NET `IPublisher.SendAllAsync`; in Go the only way to
  send a thousand messages was a loop over `Send`, and a loop over `Send` pays a
  full broker round trip per message — publish, wait, publish the next, with the
  broker idle for nearly all of it.

  ```go
  results, err := pub.SendAll(ctx, orders)
  ```

  Everything goes out before anything is awaited, and only then is the whole
  batch checked. That ordering is the entire feature: a version that awaited each
  confirm as it published would be the loop the caller could already write, with
  goroutines added to it for nothing. The results come back in the order the
  payloads were given, whatever order the broker answered in, and there is
  exactly one per payload, so `results[i]` is `payloads[i]`.

  Envelope options apply to every message in the batch, which is what makes a
  shared `CorrelationID` or `Header` useful here. Leave `MessageID` to be
  generated: pinning it sends the whole batch under one identifier, which is an
  instruction to a deduplicating consumer to keep one message and discard the
  rest.

  **It is not atomic and does not pretend to be.** AMQP has no all-or-nothing
  publish — there is no way to send a hundred messages such that all or none
  arrive — and a library offering one would be lying about what the protocol can
  do. What this promises is narrower: everything was attempted, and everything was
  waited for.

  So a batch can fail halfway, and that is the ordinary outcome of a broker
  problem partway through rather than an exotic case. A caller told only "it
  failed" resends messages that already arrived, so the new
  `acemq.BatchPublishFailedError` carries the counts as fields — `Total`,
  `Confirmed`, `Failed` — along with `First`, the first failure *in payload
  order* rather than the first one the broker answered, and `Errors`, one entry
  per payload and `nil` where the message was confirmed. The results slice is
  returned alongside the error and is always as long as the batch, so
  `results[i]`, `err.Errors[i]` and `payloads[i]` are the same message and
  resending exactly what did not arrive is a loop over `Errors`.

  Its message reads `2 of 5 messages were not confirmed; 3 were. The first
  failure was: ...` — word for word what Java and .NET produce for the same
  failure, and deliberately without this library's `acemq:` prefix so that one
  line in a runbook covers a fleet in three languages. The error unwraps to
  `First`, so `errors.As` still reaches the `*PublishFailedError` underneath it.

  A failure early in the batch does not cut the rest short: every send is awaited
  before the error is returned. Stopping at the first failure is what loses the
  count of what arrived, and the count is the reason the error exists.

  **Nothing about it widens what the transport allows.** Each message goes out
  through the same publish path as `Send`, so whatever bounds publishes in flight
  still bounds them — the RabbitMQ transport shares one channel under a lock,
  because an AMQP channel is not safe for concurrent use. One consequence worth
  knowing: publish interceptors now run on one goroutine per message during a
  batch, so an interceptor keeping state of its own has to be safe for concurrent
  use. The same was already true of a program publishing from several goroutines.

  What a reader does about it: replace loops that publish a known set of messages
  one at a time, handle `*BatchPublishFailedError` where a partial batch matters,
  and check any publish interceptor for state it shares between calls. Nothing
  that compiled before changes — this is a new method and a new error type.

- **`avro.ReaderSchema`, so a registered Avro codec reads every message against the
  schema the consumer was written against rather than the one the producer
  sent.** Java has had `AvroCodec.registered(registry, readerSchema)` and .NET
  resolves against its own schema on every message; this library looked the
  writer's schema up and then decoded with it, which is only half of schema
  evolution and the half that does less work.

  ```go
  consumer, err := avro.Registered(registry, "order.placed", schema,
      avro.ReaderSchema(schema))
  ```

  What the missing half cost: a consumer decoded whatever shape the producer
  happened to send. A field it had never heard of arrived and a field it expected
  came back as the zero value — `""`, `0`, `false` — for as long as the producer
  had not started sending it, with no way to tell *absent* from *empty*. Given
  both schemas Avro resolves them: the unknown field is skipped rather than
  shifting every field after it, and the missing one is filled in from the
  reader's own default, so a schema saying `"default":"public"` produces
  `"public"` and not `""`. The consumer sees the shape it was written against
  whichever version wrote the message, which is the entire reason the registered
  mode exists.

  The resolution is Avro's, through `SchemaCompatibility.Resolve` — a composite
  schema built from the pair, not a re-parse of the bytes against a different
  schema and not a field-by-field copy. That is what makes the failure cases
  right as well as the happy one: a writer schema that cannot be resolved onto
  the reader's — a field added without a default, a type changed to one Avro will
  not promote — is a fatal error printing **both** schemas in full, because the
  two are usually versions of one record and share a name, so naming one of them
  says nothing. Resolution is done once per schema identifier and remembered; an
  identifier stands for one schema forever.

  **Nothing changes for code that does not pass it.** `avro.Registered` gained a
  variadic option parameter, so every existing call still compiles and still
  decodes against the writer's schema exactly as it did. That default is
  deliberate and is where this library parts from .NET, which resolves
  unconditionally: turning resolution on underneath an unchanged call would
  change what every deployed consumer sees — a field it had been ignoring starts
  arriving as a default — and a silent change of meaning is worse than an
  argument. Opting in is one argument.

  **It was called `avro.ReadAs` earlier in this same unreleased cycle, and it is
  `avro.ReaderSchema` now.** Renamed outright, with no alias: the library is
  pre-1.0, nothing has shipped under the old name, and an alias kept for
  compatibility nobody needs is a second spelling that outlives the reason for
  it. Java names this `readerSchema`, Python `reader_schema`, Ruby
  `reader_schema:` and .NET is gaining `readerSchema` in the same round — so one
  idea has one spelling in all five, and a reader moving between them is not
  learning a synonym. If you took this library from `main` between the two
  commits, the fix is the name.

  **The wire format is untouched.** `ReaderSchema` is a read-side decision and the
  bytes a producer writes are identical with it and without it: one zero byte,
  four bytes of identifier, big-endian, then the Avro body. Content types,
  `CanDecode` and the gate between the two modes are unchanged, and there is a
  test asserting a codec built with a reader schema encodes byte-for-byte what
  one built without it encodes. Java, .NET, Python and Ruby read the same
  messages they always did.

  What a reader has to do about it: nothing, unless producers and consumers are
  deployed independently — in which case add `avro.ReaderSchema(schema)` to the
  consumer's `avro.Registered` call, passing the same schema the codec was built
  with, and audit any handler that has been treating a zero value as *the
  producer has not sent this yet*. That reading stops being true once the
  reader's default fills the field in.

- **Eight documentation pages this library was missing, and a place in the
  navigation for each.** Java and .NET both had a page on request and reply, a
  page on streams and four tutorials; .NET had just gained a page on
  interceptors. This library had a paragraph on the first two inside
  [docs/patterns.md](docs/patterns.md), nothing at all on the third, and no
  tutorials. A reader arriving at the Go site was told less about the same
  library than a reader arriving at either of the others.

  [docs/request-reply.md](docs/request-reply.md),
  [docs/streams.md](docs/streams.md),
  [docs/interceptors.md](docs/interceptors.md),
  [docs/tutorials.md](docs/tutorials.md) and four numbered tutorials —
  `tutorial-first-message`, `tutorial-surviving-failure`, `tutorial-exactly-once`
  and `tutorial-observability` — teaching the same four subjects in the same
  order as Java's and .NET's, so a team moving between the five learns each idea
  once.

  They are not translations. Where this library's design differs the page argues
  the Go side rather than describing another language's API in Go syntax: errors
  as values and the `if err != nil` that follows, `context.Context` first,
  functions where a generic method would need Go 1.27, options rather than
  builders, and closures rather than an interface with an `Order` field.

  Every sample was compiled against the current API before it shipped, and the
  behavioural claims were run rather than reasoned about. Several came back
  different from what the equivalent page in another language says, and the
  pages say the Go answer:

  - **A consume interceptor that refuses a message dead-letters it**, with
    `an interceptor refused it: …` on the envelope, counted as `dead_lettered`
    like anything else. .NET's equivalent escapes the retry ladder into an
    unbounded redelivery loop with no counter moving.
  - **A consume interceptor's envelope changes reach the handler**, which .NET's
    read-only context cannot do — while `ConsumeContext.Body` and `ContentType`
    are copies and rewriting them does nothing, silently, because the decode
    reads the delivery. That trap is stated where a reader will meet it.
  - **Retries, dead letters, parks, `patterns.Replay`, the outbox relay and the
    scheduler publish through `Conn.PublishRaw` and never run the publish
    chain.** Requests, replies, pipeline steps and routing-slip forwards do.
    .NET's page says every publish the library makes goes through its chain; four
    of the six named there do not go through this one.
  - **A publish interceptor runs inside the span** for an `otel.Publisher`,
    because the wrapper opens the span before delegating — the reverse of .NET,
    where the chain runs first. It is what makes `Tracing.PublishInterceptor`
    work at all.
  - **A stream's default prefetch is 10 here and 100 in Java and .NET.**
  - **`acemq.Retry` from a stream handler appends a second copy of the message to
    the log**, because a retry republishes onto the queue it came from and a
    stream never removes anything. There is no `skipFailures` and no
    `lastHandledOffset` in this library; the two honest handler shapes are
    written out instead.
  - **`acemq.Reject` on a stream does reach `{stream}.dlq`**, because this
    library republishes rather than using the broker's dead-lettering — which
    Java's streams page says is impossible, correctly, of the broker's mechanism
    and not of this one. The original stays in the stream, so the dead letter is
    a copy rather than a move.
  - **A requester counts nothing.** `acemq.MetricRequestDuration` and
    `MetricRequestTotal` are names this library never writes, and there is no
    equivalent of Java's `timedOut()`, `unmatched()`, `answered()` or
    `unanswerable()`. The page says so and points at `otel.Ask` and the
    responder's ordinary consume metrics instead of implying numbers that are not
    there.

- A test asserting the three replay stamps stay outside the `x-acemq-` namespace
  and keep their exact spelling, and one asserting a replayed message reaches a
  handler carrying all three with `acemq-replayed-at` parseable as RFC 3339. The
  existing test checked only that `acemq-replayed-from` was present, which is why
  the encoding and the namespace could be described wrongly for so long without
  anything failing.

## [0.5.0] - 2026-09-09

### Added

- **The claim check, so a payload too large for a broker goes to a store and the
  message carries the key.** Java, Python and Ruby have had one; this library
  declared `x-acemq-claim` and wrote it nowhere, which is a defined header with
  no implementation and worse than an absent feature — it looks like support.

  ```go
  store := patterns.NewFilesystemClaimCheckStore("/mnt/claims")
  checked := patterns.ClaimCheck(acemq.JSONCodec{}, store)

  mq, err := acemq.Connect(ctx, url, acemq.WithCodec(checked))
  ```

  The wire contract is the family's, and it is three bytes:

  ```
  0xAC  0x01  0x00  payload   inline, and identical to what the delegate wrote
  0xAC  0x01  0x01  key       a claim check
  ```

  The key is the store's key as bare UTF-8 — not a URI, not a scheme — so a Go
  consumer pointed at the same store reads a document a Java publisher checked
  in. `patterns.DefaultClaimCheckThreshold` is 64 KiB and is compared strictly
  less than, so a payload *at* the threshold is offloaded, which is what the
  other three do: a payload on the boundary must not be inline from one library
  and checked from another. `patterns.OffloadAbove(n)` changes it and zero
  offloads everything. There are tests pinning the magic byte and the boundary,
  and they fail if either moves.

  Below the threshold the payload travels inline and the content type is the
  delegate's, unchanged — a claim-checked message is still a document, it is a
  document that is somewhere else. A body with no framing is read as the delegate
  would read it, which is what makes adding this codec to a live queue safe.

  **The codec still does not write `x-acemq-claim`,** and that is deliberate
  rather than unfinished: the framing is the contract because a header can be
  stripped by a shovel or a federation link, and because a present-or-absent
  header cannot say whether a payload travelled inline. The header stays reserved
  for an application that wants to say where a payload went;
  `patterns.ClaimKeyOf(body)` reads the key without fetching it, which is the
  question an operator holds in front of a dead-letter queue.

  `patterns.ClaimCheckStore` is three methods, so a store in front of S3 or Azure
  Blob Storage is a small type. `Get` reports *not found* separately from
  *failed*, and the codec treats them differently: a store that timed out is
  retryable, a key it does not hold is fatal and the message stops rather than
  circling for ever over a payload that will never come back. There is no context
  on the store because `acemq.Codec` has none to pass on, so a store doing
  network I/O has to carry its own timeout.

  `patterns.NewInMemoryClaimCheckStore` is for tests and copies on the way in and
  out, so a codec reusing a buffer cannot change what was stored.
  `patterns.NewFilesystemClaimCheckStore` writes to a temporary file and renames
  into place, because a consumer fast enough to read the key before the writer
  finished would otherwise get a truncated payload — and messaging is exactly the
  arrangement that makes a consumer that fast normal. A key becomes a path
  segment, so one arriving from a message is checked against the UUID shape every
  key it issues has rather than trusted; `../../etc/passwd` is a key too.

  Wrapping an `acemq.CompositeCodec` works: the codec forwards `DecodeAs`, so a
  queue carrying more than one format still chooses by content type.

  See [docs/patterns.md](docs/patterns.md#claim-check), and read the retention
  warning there — a store whose retention is shorter than the queue's produces
  messages nobody can read, which is worse than losing them because they still
  look like messages.

- **A routing slip can be read and written in either of the family's two forms,
  so a Go step can be one step of a Java-declared pipeline.**

  Java writes `x-acemq-route` — the step names comma-joined — with
  `x-acemq-route-position` and `x-acemq-route-id`, resolved against a `Pipeline`
  declared in code. This library, Python and Ruby write `acemq-routing-slip`, a
  JSON document holding every step's exchange and routing key. Neither could read
  the other, so a Go consumer could not take part in a Java pipeline at all.

  **Go now reads both and still writes JSON by default.** Three of the five
  libraries write the JSON slip and it is the self-describing one — a message
  carries its whole itinerary, so any consumer can send it onwards with nothing
  declared, and the route can differ per message. The declared form is smaller
  and stays readable in a management console, at the cost of meaning nothing
  without the declaration.

  A slip keeps the form it arrived in, so a Go step in a Java pipeline answers in
  the shape the next Java step expects without being told to.

  ```go
  route := patterns.NewRoute("orders", "validate", "charge", "ship")

  sub, err := acemq.Consume(ctx, mq, route.QueueFor("charge"),
      patterns.FollowSlip(mq, charge, patterns.AlongRoute(route)))
  ```

  `patterns.Route` mirrors Java's `Pipeline` topology — the name is the exchange,
  a step name is the routing key, the queue is `{route}.{step}` — and
  `patterns.AlongRoute` is what turns a step name back into somewhere to publish.
  Without it a declared route is **rejected fatally** rather than followed: the
  names carry no destinations, and publishing with an empty exchange would send
  the message to a queue named for the step instead of to the pipeline's, quietly
  and to the wrong place. The error names `AlongRoute`.

  `Route.Start` begins a run Java steps will follow, `slip.AsSteps(route)` and
  `slip.AsJSON()` convert between the forms, and `slip.Form()` and `slip.RunID()`
  say what a slip is.

  The three route headers are materialised onto `acemq.Envelope` as `Route`,
  `RoutePosition` and `RouteID`, with `Envelope.RouteSteps()` and the
  `acemq.Route(steps, position, runID)` option. **That was the part that had to
  change for any of this to work:** `x-acemq-` is a reserved prefix and the
  engine drops any header in it that this version does not know, so a
  Java-declared route reaching a Go consumer arrived on the wire and vanished
  before the handler saw it. Looking for it in `Envelope.Headers` would have found
  nothing for ever.

  `patterns.SlipFrom` takes an optional `*Route` and is otherwise unchanged. The
  JSON on the wire is byte-identical to what earlier versions wrote — the new
  fields on `RoutingSlip` are unexported, so `encoding/json` ignores them. See
  [docs/patterns.md](docs/patterns.md#two-forms-on-the-wire-and-both-are-read).

- **A handler can park a message.** `acemq.Park(err)` joins `Accept`, `Retry` and
  `Reject` in the vocabulary a handler returns. The engine could already park —
  it does so for a body no codec would decode — but a handler could not ask for
  it, so a handler that knew a message was unreadable had to `Reject` it into the
  dead letters and lose the distinction the parked queue exists to make.

  `{queue}.dlq` is a queue of work that failed, drained by somebody looking for a
  broker or a downstream that has since recovered. `{queue}.parked` is a queue of
  messages nobody could read, drained by somebody looking for the producer that
  sent them. A parked message settles to `{queue}.parked` with the handler's
  reason attached, counts as `acemq.consume.total{outcome=parked}`, and carries
  `messaging.acemq.outcome = parked` on its span — the same reporting the engine's
  own parking gets, because which layer noticed is not the drainer's question.

  .NET has had `Ack.Park` since its first release; Python and Ruby are adding
  theirs alongside this one. See
  [docs/consuming.md](docs/consuming.md#reject-or-park).

- **`acemq.ReplyTo` and `Envelope.ReplyTo`**, which write and read AMQP's own
  `reply-to` property. `Outbound` and `Delivery` carry it, and both transports
  put it on the wire and take it off. See the request/reply entry under Fixed.

- **`pipeline.run_finished` is written by the library rather than only being
  callable.** `patterns.InPipeline` and `patterns.AtStep` name a `patterns.Then`
  or a `patterns.FollowSlip`, and a named step writes the event onto the
  delivery's own span when a message leaves the pipeline: `completed` when
  `FollowSlip` finishes the itinerary, `ended_early` when a `Then` step returns
  `false` because this message does not continue. A pipeline step runs inside the
  handler's span, which is what makes there be something to write onto. An
  unnamed step reports nothing rather than an event tagged with two empty
  strings: Go has no `Pipeline` type that owns its steps the way Java's does, so
  a step that wants to be reported has to say which pipeline it is in.

- **The outbox relay reports what it did.** Every sweep counts
  `acemq.outbox.total`, tagged `published` or `failed`, and records
  `acemq.outbox.lag` — measured from when the record was committed rather than
  from when the sweep claimed it, because what a lag answers is how long somebody
  has been owed this message. The names are the Java library's.

  The tracing adapter's `outbox.publish_failed` event and its
  `messaging.acemq.outbox_lag_ms` attribute remain application-callable and are
  **not** written by the relay. They cannot be: `Sweep` runs on a goroutine of the
  relay's own with no span open, and opening one per record would produce exactly
  the zero-length spans that adapter exists to avoid. Counters need no span, which
  is why the relay reports through them instead. An application that wants the
  trace side calls `Sweep` from inside a span of its own — see
  [docs/observability.md](docs/observability.md).

- **`telemetry/otel`, OpenTelemetry spans for publishes and deliveries.** A
  module of its own — `github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel` —
  so `go.opentelemetry.io/otel` never becomes a dependency of anyone who only
  wanted a message queue, the same arrangement as the codec modules. It pins
  OpenTelemetry v1.38.0, the newest release that still builds on Go 1.23; v1.39.0
  requires 1.24 and taking it would move the library's floor for everyone.

  A handler's span is a child of the publish that caused it, and the parent comes
  out of the message's own `traceparent` header rather than out of whatever the
  delivery goroutine happened to be doing. Those are two different traces,
  minutes and machines apart, and joining them is the one thing a messaging
  system needs from tracing that an HTTP client does not. There is a test that
  hands a handler a message from one trace while a live, unrelated span is
  current on the goroutine, and fails if the ambient one wins.

  `traceparent` and `tracestate` are deliberately not `x-acemq-` prefixed. They
  are the W3C names that every other tracing tool already knows, and the Java,
  .NET, Python and Ruby libraries write the same two, so a Go consumer joins a
  Java producer's trace with neither side configured for the other.

  Span names, kinds and attributes are the Java adapter's: `<destination>
  publish` as a PRODUCER, `<queue> process` as a CONSUMER, `<destination>
  request` as a CLIENT — CLIENT because that span waits for an answer, and a
  reader who cannot tell it from a publish cannot tell a slow broker from a slow
  responder. `unroutable`, `failed` and `dead_lettered` set the span status to
  ERROR; `acked`, `retried` and `rejected` do not, because a retry is the system
  working and a rejection is a decision, and marking either as an error is how a
  trace view fills with red and stops meaning anything.

  `outbox.publish_failed`, `pipeline.run_finished`, `message.retried` and
  `message.dead_lettered` are events on the span already open rather than spans of
  their own: a zero-length span at the end of a trace adds a row and no
  information. An event with no span open is dropped rather than opening one.

  `Tracing.PropagationHeaders` injects the current context into a fresh carrier,
  for a message this library does not publish — an outbox record, whose publish
  happens later and elsewhere.

  Go's interceptors run before a publish rather than around it, so the shape
  differs from the Python and Ruby adapters: the span comes from
  `otel.NewPublisher` and `otel.Handle`, and the interceptor writes the headers
  and completes the publish span's message attributes. The interceptor leaves a
  span it did not open alone, so an HTTP server span that happens to be current
  does not acquire messaging attributes.

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

### Changed

- **Every metric was renamed onto Java's vocabulary. This breaks every existing
  Go dashboard, alert rule and recording rule.**

  Java's `MetricNames` is the family's vocabulary and this library, Python and
  Ruby have all moved onto it. Until now the four libraries emitted disjoint sets
  of names while several of them documented the opposite — that a dashboard built
  against one AceMQ library reads the same against another. That was not true and
  is now.

  | Old | New |
  |---|---|
  | `acemq.messages.published` | `acemq.publish.total{outcome=confirmed}` or `{outcome=published}` |
  | `acemq.messages.publish.failed` | `acemq.publish.total{outcome=failed}` or `{outcome=unroutable}` |
  | — | `acemq.publish.duration` (new) |
  | `acemq.messages.consumed` | `acemq.consume.total` |
  | `acemq.messages.accepted` | `acemq.consume.total{outcome=acked}` |
  | `acemq.messages.rejected` | `acemq.consume.total{outcome=rejected}` |
  | `acemq.messages.retried` | `acemq.messages.retried.total` |
  | `acemq.messages.dead.lettered` | `acemq.messages.dead.lettered.total` |
  | `acemq.messages.parked` | `acemq.consume.total{outcome=parked}` |
  | `acemq.handler.duration` | `acemq.consume.duration` |
  | `acemq.messages.in.flight` | `acemq.consume.in.flight` |
  | `acemq.messages.set.aside.failed` | unchanged |
  | `acemq.retry.rung.missing` | unchanged |
  | `acemq.outbox.lag`, `acemq.outbox.total` | unchanged |

  The constants moved with the names: `acemq.MetricPublishTotal`,
  `MetricPublishDuration`, `MetricConsumeTotal`, `MetricConsumeDuration`,
  `MetricConsumeInFlight`, `MetricRetriedTotal` and `MetricDeadLetteredTotal`.
  `MetricPublished`, `MetricPublishFailed`,
  `MetricConsumed`, `MetricAccepted`, `MetricRetried`, `MetricRejected`,
  `MetricDeadLettered`, `MetricParked`, `MetricHandlerDuration` and
  `MetricInFlight` are gone.

  **Three of the old counters have no direct replacement, because they were the
  same numbers twice.** `acemq.messages.accepted` and `acemq.messages.rejected`
  were always `acemq.consume.total` filtered by its own `outcome` tag; writing
  both gave a dashboard two ways to be wrong about one thing. Java keeps only
  `acemq.messages.retried.total` and `acemq.messages.dead.lettered.total`
  standing alone, because those two are what an alert is written against and an
  alert should not have to know a tag vocabulary to fire — and this library now
  keeps the same two and no others. **They are a second view of the same events
  and must not be added to `acemq.consume.total`;** doing so double-counts every
  retry, and there is a test pinning the two to agree.

  `acemq.messages.parked` gets no replacement counter: a parked message is
  `acemq.consume.total{outcome=parked}` and nothing else, which is what Java
  settled on. **An alert on the old counter has to be rewritten against the tag,
  not renamed.**

  `acemq.messages.published` and `acemq.messages.publish.failed` became one
  counter with an `outcome` tag, which the split could not express: an unroutable
  mandatory message was counted as a failure, and a broken publisher and an
  unbound routing key were indistinguishable. They are now `unroutable` and
  `failed`. `confirmed` and `published` are likewise told apart, which is the
  difference between the broker's word and having reached the socket.

  Five names Java has and this library does not write are declared anyway, so an
  `Observer` can be written against one list: see
  [docs/observability.md](docs/observability.md#named-but-not-written) for
  `acemq.consume.attempts`, the request metrics and the pipeline metrics, and why
  each is absent.

  `message.type`, `transport`, `pipeline` and `step` join the tag constants.
  `message.type` carries a dot, the same hazard `routing.key` did, and the
  Prometheus endpoint converts it to `message_type` — there is now a test that
  walks the entire tag vocabulary against the Prometheus label grammar, so a
  third dotted tag cannot reach a scrape endpoint unnoticed.

- **The publish metric tag `key` is now `routing.key`. This changes a label an
  existing dashboard may group by.** Java and .NET already wrote `routing.key`;
  this library and Python wrote `key`. Neither reading was wrong, but the
  fully-qualified name says *which* key it means next to a tag called `queue`,
  and Java is the library the others are ported from — so the two moved rather
  than the four staying split. Python is making the same change. The tag is on
  `acemq.publish.total`, `acemq.publish.duration`, `acemq.outbox.total` and
  `acemq.outbox.lag`, and is `acemq.TagRoutingKey` in code.

  **A dashboard that groups publishes by `key` has to be edited.** The counters
  themselves are unchanged; only the label name moved.

  The Prometheus endpoint in `actuator` now converts label names the way it
  already converted metric names, so `routing.key` is scraped as `routing_key`. A
  dot is legal in an AceMQ tag and illegal in a Prometheus label; emitted
  verbatim it would have made the whole scrape unparseable rather than one label
  wrong. Any tag of your own carrying a dot or a dash is converted too.

- **The consume counters classify by the engine's decision, not the handler's
  request. This changes numbers an existing dashboard may rely on.** A handler
  asking for a retry is a request: the engine still has to look at the policy,
  and a message on its last permitted attempt is dead-lettered instead. The
  counters were classified from the `Ack`, so that message incremented the retry
  counter — a retry nobody would ever make — *and* the dead-letter counter, and a
  dashboard's retry rate included every message about to be given up on.

  **Retries fall and dead letters rise, with no change in what the service
  does.** The numbers were wrong and are now right. Two smaller corrections come
  with it: a retry waiting on a rung queue was counted twice, once bare and once
  under a `rung` label, and is now counted once (the `rung` label lives on
  `acemq.retry.rung.missing`); and a message nothing could decode is now counted
  as parked rather than rejected. The .NET library made the same correction, and
  Python and Ruby are making it too.

  `acemq.consume.total` is now counted when a delivery settles rather than when
  it arrives, and carries an `outcome` tag — `acked`, `retried`, `rejected`,
  `dead_lettered` or `parked`. Every delivery increments it exactly once, so the
  tag partitions the deliveries rather than overlapping them.
  `acemq.consume.duration` carries the same tag.

  The tag is the same string `telemetry/otel` writes on that delivery's span as
  `messaging.acemq.outcome`. Both read `acemq.Settlement.Outcome`, a new field the
  engine fills in, so there is one derivation rather than two that could drift —
  and there is a test that fails if a counter and a span disagree about the same
  deliveries.

- **Breaking change to the wire format: `crypto` now writes the framing Java,
  Python and Ruby write.** A body encrypted by this library up to v0.3.0 is not
  the shape any of the others read, and a body encrypted by any of them was
  refused here. That is fixed by changing what Go writes, because Go was the
  outlier:

  ```
  0xAE   0x01   len   key id   12-byte nonce   ciphertext + 16-byte tag
  ```

  Magic byte, version, a **one-byte** key id length — previously two, big-endian,
  with no magic byte at all. The header is still authenticated as associated data
  and the cipher is still AES-GCM with a 128-bit tag, so only the bytes in front
  of the nonce moved.

  The magic byte is the point of the exercise. The old framing began `0x01`,
  which is a plausible first byte of a protobuf or an Avro body, so a consumer
  configured to decrypt and pointed at a plaintext queue could not tell it had
  been: it reported a decryption failure for a message that was never encrypted.
  `0xAE` is not a plausible first byte of anything else this library writes, so
  that message is now refused as what it is.

  The test suite pins the vector the other libraries pin — key `00 01 … 1f`,
  key id `2026-01`, nonce `00 01 … 0b`, plaintext `hello` — and this library
  produces it byte for byte. It was checked against the compiled Java
  `EncryptedCodec` and against Ruby's implementation, in both directions.

  **What to do.** Upgrade consumers before producers, as with any format change:
  a consumer on this version reads both framings, a consumer on v0.3.0 reads
  neither this one's nor Java's. Then drain or re-encrypt anything still holding
  the old framing before v0.6.0 removes the ability to read it.

- **A `crypto` key may be 16, 24 or 32 bytes.** It had to be exactly 32. Java,
  Python and Ruby all take the three lengths AES takes, and refusing the other
  two meant a key that already worked in Java had to be re-issued to be used
  here. `crypto.NewKey` still draws 32, and `crypto.KeySize` is still 32 — it now
  documents what `NewKey` produces rather than the only length accepted. A key of
  any other length is still refused rather than padded or hashed into shape.

- **A `crypto` key id is at most 255 UTF-8 bytes**, refused by `Keyring.Add`
  rather than truncated by the single length byte into the name of a key nobody
  holds.

### Deprecated

- **Reading the legacy Go encryption framing, which goes away in v0.6.0.**
  v0.3.0 is released, so queues can be holding bodies framed as version, two-byte
  big-endian key id length, key id, nonce, ciphertext. `crypto.Codec.Decode` and
  `crypto.KeyIDOf` still read them: a body beginning `0xAE` is the current
  framing, a body beginning `0x01` is the legacy one, and anything else is
  refused as before. The two are unambiguously distinguishable, which is what
  makes reading both safe.

  **Nothing writes the legacy framing, and nothing can be made to.** There is no
  option, no constructor and no environment variable for it, because two writers
  is how a divergence survives being fixed. It is a migration affordance with an
  end date: drain those queues or re-encrypt their contents before v0.6.0, after
  which those bodies are refused rather than misread.

### Fixed

- **A Java or .NET requester and a Go responder can talk.** They could not.
  This library, Python and Ruby put the reply address in the `acemq-reply-to`
  application header; Java and .NET read AMQP's own `reply-to` property. Neither
  side looked where the other wrote, so a cross-language request was
  dead-lettered as having nowhere to reply — and no fixture covered it, which is
  why it survived this long.

  The rule now, in all five libraries: **write both, read either.**
  `Requester.Do` sets the native `reply-to` property *and* the `acemq-reply-to`
  header, to the same queue. `patterns.Serve` reads the header first and falls
  back to the property when the header is absent. Header first because it is the
  half that survives a rebuild — a service that reconstructed the message kept the
  headers and lost the properties — so where the two disagree the header is the
  more recent of the two.

  Nothing has to be changed in an application that uses `NewRequester` and
  `Serve`. A request published by hand should now carry both; see
  [docs/patterns.md](docs/patterns.md#where-the-reply-address-travels).

- **`telemetry/otel`: a failure with no outcome.** `Span.Failed` recorded the
  exception and the `ERROR` status and wrote no `messaging.acemq.outcome` at all,
  so a publish that threw was counted as `failed` and carried a span a query
  filtered on outcomes could not find — the counter and the trace disagreeing
  about the same message, which is the one thing that vocabulary exists to
  prevent. `Failed` now writes `outcome = failed` as well.

  An outcome named through `Span.Outcome` still wins, whichever order the two
  calls come in, so a request that ran out of time stays `timed_out` and an
  unroutable message stays `unroutable`. Java's adapter was fixed the same way;
  Python already did it.

- **`codec/yaml` reads `text/x-yaml` again.** `CanDecode` matched
  `application/x-yaml` and `text/yaml` but not `text/x-yaml`, which the Java,
  Python and Ruby libraries all send and read. A YAML message from a Ruby
  publisher was therefore undecodable to a Go consumer — parked, with no codec
  claiming it, for a spelling difference.

- **`codec/avro` no longer decodes a message written in the other mode into
  nonsense.** `CanDecode` accepted all four Avro content-type spellings whatever
  mode the codec was in, so a fixed-schema codec claimed a registry-framed
  message and handed the five Confluent framing bytes — one zero byte and four
  bytes of schema identifier — to `avro.Unmarshal` as though they were the start
  of the first field. Avro does not object to that. It reads the shifted bytes as
  whatever they happen to mean and returns a record where every value is wrong,
  with no error, no log line and nothing on any dashboard. A round trip through a
  registered producer and a fixed consumer came back as an empty order for zero
  pence.

  A codec now claims only the spelling its own mode can read: `avro.Of` claims
  `avro/binary` and refuses `application/vnd.acemq.avro`, `avro.Registered` the
  reverse. Both still take `application/avro` and any `*+avro` suffix type,
  because neither of those says how the body was framed and a message nothing
  claims is a message nobody reads. Java, .NET, Python and Ruby have always
  gated this way; Go was the only one that did not.

  The other direction was never silent — the registered decoder checks the
  framing byte and then the schema identifier — and there is now a test that
  says so rather than assuming it, using a body that starts with the framing byte
  by coincidence so the length check alone cannot be what saves it.

- **A dead-lettered message's span said `retried`.** `telemetry/otel` exposed
  `message.retried` and `message.dead_lettered` as methods an application could
  call, and nothing in the engine called them: Go's engine had only
  `Observer.Count`, which carries no span context. So the last thing written on a
  delivery's span was whatever the handler asked for, and a handler that asks for
  a retry it will not get — because the attempts have run out — left a span
  saying `retried` for a message nobody would ever try again. Anybody querying a
  trace backend for dead letters found nothing at all.

  The engine now settles every delivery through a seam of its own,
  `acemq.OnSettled`, which reports what actually happened: `accepted`, `retried`
  with the delay the policy chose, `dead_lettered` with the reason, or `parked`.
  `otel.Handle` registers on it and hands the ending of the span to the engine,
  which is the only way the two events can land on it — a span ended when the
  handler returned is closed before the engine decides. A handler that rejected a
  message on purpose keeps `rejected`: that decision arrived where it was meant
  to, and a trace view that fills with red for decisions stops meaning anything.

  Java calls its `Telemetry` interface at these two moments and reaches the span
  through `Span.current()`. Go has no ambient current span, so the seam passes
  the handler's own context back instead. It costs a process that traces nothing
  nothing at all: each consumer worker holds one hook and clears it between
  deliveries, so there is no per-delivery allocation and no call when nobody has
  registered. `telemetry/otel` remains a module of its own and the root module's
  dependencies are unchanged.

- **`crypto.ContentType` no longer claims an interoperability that does not
  exist.** Its documentation said the content type was "what Java and .NET
  write", which is true of the string and of nothing after it. All five libraries
  write `application/vnd.acemq.encrypted`, and until this release four of them
  meant three different things by it.

  The comment was corrected first and the framing second — see the breaking
  change above, which is what actually closes the gap for Java, Python and Ruby.
  .NET remains genuinely incompatible: it does not use AES-GCM at all, but
  AES-256-CBC with a separate HMAC-SHA-256, and the documentation now says so
  rather than implying the whole family diverges. The security guide said the
  library did not encrypt bodies at all, which had been untrue since `crypto`
  landed; it now describes what is really there.

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
