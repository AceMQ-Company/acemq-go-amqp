# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

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

  Two names Java has and this library does not write are declared anyway, so an
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
  the old framing before v0.5.0 removes the ability to read it.

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

- **Reading the legacy Go encryption framing, which goes away in v0.5.0.**
  v0.3.0 is released, so queues can be holding bodies framed as version, two-byte
  big-endian key id length, key id, nonce, ciphertext. `crypto.Codec.Decode` and
  `crypto.KeyIDOf` still read them: a body beginning `0xAE` is the current
  framing, a body beginning `0x01` is the legacy one, and anything else is
  refused as before. The two are unambiguously distinguishable, which is what
  makes reading both safe.

  **Nothing writes the legacy framing, and nothing can be made to.** There is no
  option, no constructor and no environment variable for it, because two writers
  is how a divergence survives being fixed. It is a migration affordance with an
  end date: drain those queues or re-encrypt their contents before v0.5.0, after
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
