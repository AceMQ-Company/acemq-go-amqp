# The envelope

What travels with a message besides its body: identity, causation, and the
counters the retry engine keeps.

```go
type Envelope struct {
	ID            string
	Type          string
	Version       int
	CorrelationID string
	CausationID   string
	Attempt       int
	FirstSeen     time.Time
	Origin        string
	Error         string
	ReplyTo       string
	Headers       map[string]any
}
```

| Field | | Default |
|---|---|---|
| `ID` | unique identifier, and the default idempotency key | a UUID v4 |
| `Type` | logical type, such as `order.placed` | the routing key |
| `Version` | schema version of the payload | 1 |
| `CorrelationID` | propagated unchanged across hops | the `ID` |
| `CausationID` | the message that caused this one | empty |
| `Attempt` | delivery attempt | 1 |
| `FirstSeen` | when it was first published | now |
| `Origin` | the publishing process | `acemq@{hostname}` |
| `Error` | why it was dead-lettered | empty |
| `ReplyTo` | where an answer should go | empty |
| `Headers` | your own headers | empty |

Those defaults are part of the wire contract rather than conveniences. A port
that starts `Version` at 0, or lets `Type` be empty where Java falls back to the
routing key, produces messages that differ from Java's for the same logical
content.

## On the wire

| Header | |
|---|---|
| `x-acemq-id` | `ID` |
| `x-acemq-type` | `Type` |
| `x-acemq-version` | `Version`, an integer |
| `x-acemq-correlation` | `CorrelationID` |
| `x-acemq-causation` | `CausationID`, omitted when empty |
| `x-acemq-attempt` | `Attempt`, an integer |
| `x-acemq-first-seen` | `FirstSeen`, epoch milliseconds |
| `x-acemq-origin` | `Origin`, omitted when empty |
| `x-acemq-error` | `Error`, omitted when empty. Present only in a dead-letter queue. |
| `x-acemq-route` | `Route`, the ordered step names of a declared pipeline, comma joined. All three below are omitted together when empty. |
| `x-acemq-route-position` | `RoutePosition`, an integer, counting from zero |
| `x-acemq-route-id` | `RouteID`, identifying one run across every hop |

The three route headers are Java's form of a routing slip. They are on the
envelope rather than among the application's headers for a reason that is not
tidiness: the reserved prefix means the engine drops any `x-acemq-` header it
does not know, so a Java-declared route reaching a Go consumer used to arrive on
the wire and vanish before the handler saw it.

`Envelope.RouteSteps()` splits `Route` into the names.
[`patterns.SlipFrom`](patterns.md#two-forms-on-the-wire-and-both-are-read) reads
either these or this library's own JSON slip, which is what Go writes by default.

`ReplyTo` is deliberately not in that table. It is AMQP's own `reply-to`
*property* rather than a header, which is where Java and .NET read it from — see
[where the reply address travels](patterns.md#where-the-reply-address-travels).

**An absent value is an absent header, never a null one.** Java omits
`x-acemq-causation` entirely when there is no causation, and a port that writes
a null there produces a different message for the same content.

**The replay stamps are not in that table and not in this namespace.** A
[replay](patterns.md#replay) writes `acemq-replayed-from`, `acemq-replayed-at`
and `acemq-replay-count` — no `x-`, and that is the point. Anything under
`x-acemq-` that the engine does not put on the envelope is dropped on the way in,
so a stamp written there would reach the wire and vanish before the handler saw
it. Unprefixed, the three arrive as ordinary entries in `Envelope.Headers`, which
is where `patterns.HeaderReplayedFrom` and its two siblings read them.

`acemq-replayed-at` is an **RFC 3339** string — `2026-02-03T04:05:06Z` — unlike
`x-acemq-first-seen`, which is epoch milliseconds. The two timestamps on the wire
really are encoded differently. Go, Java, Python and Ruby all write RFC 3339
here; Java also reads the epoch milliseconds it wrote before 0.5.0.

.NET writes the prefixed `x-acemq-` spellings of all three instead, so its replay
stamps are dropped on the way in here rather than reaching a handler — see
[replay](patterns.md#replay).

`x-acemq-claim` is reserved and **written by nothing in this library**. It is for
an application that wants an operator reading a dead-letter queue to see where a
payload went. The [claim check](patterns.md#claim-check) frames the body rather
than setting a header, because a header can be stripped by a shovel or a
federation link and because a present-or-absent header cannot say whether a
payload travelled inline. Python and Ruby reserve it the same way.

## The reserved namespace

Anything beginning `x-acemq-` belongs to the engine. On the way in it is read
onto the envelope if this version knows it, and **dropped from
`Envelope.Headers` either way** — including headers this version has never heard
of, so a header written by a newer library cannot masquerade as one the
application set.

On the way out, `Header` refuses a reserved name rather than accepting one that
would vanish:

```
acemq: header "x-acemq-id" is in the reserved "x-acemq-" namespace and would be
dropped on consume; use a namespace of your own, such as x-yourcompany-
```

Use `x-yourcompany-` for anything of yours.

## Reading it in a handler

```go
func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
	log.Printf("%s type=%s attempt=%d age=%s from=%s tenant=%v",
		m.Envelope.ID,
		m.Envelope.Type,
		m.Envelope.Attempt,
		m.Envelope.Age(),
		m.Envelope.Origin,
		m.Envelope.Headers["x-tenant"])
	return acemq.Accept()
}
```

`Age()` is how long ago the message was first published — the figure a retry
policy uses to give up on something too old to be worth delivering, and the one
worth logging when a queue has backed up.

## Building one

```go
env, err := acemq.NewEnvelope("order.placed",
	acemq.MessageID("order-o-1"),
	acemq.CorrelationID("corr-1"),
	acemq.Header("x-tenant", "acme"))
```

It returns an error because `Header` can refuse a name. `Send` builds the
envelope for you and returns the same error, so most code never calls this
directly. See [publishing](publishing.md).

`NextAttempt()` returns a copy with the counter advanced, leaving the original
alone and keeping the identity — which matters, because the identity is what
idempotency is keyed on.

## Reading values off the wire

Broker clients are not consistent about header types: a long string may arrive
as a `string` or as `[]byte`, and an integer may be any width, or a string, or —
from a JSON fixture — a float. All of it is coerced rather than one shape being
assumed, because assuming is how a port works against one broker and fails
against another.

## How this is kept honest

`internal/testdata/envelope-fixtures.json` was generated by the Java
implementation. A test reads each case, builds an envelope from it, writes it
back, and compares: no header gained, none lost, none renamed, none with a
different value.

The test was checked by breaking it. Dropping the causation header on the way
out makes it fail and say so:

```
header "x-acemq-causation" was lost: Java wrote cause-1 (string)
```

A test that has never been seen to fail is a test nobody should trust.
