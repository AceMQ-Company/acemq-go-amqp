// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package acemq

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Envelope is what travels with a message besides its body: identity,
// causation, and the counters the retry engine keeps.
//
// The defaults are part of the wire contract, not conveniences, and are pinned
// by the fixtures in internal/testdata: a type that falls back to the routing
// key, a correlation that falls back to the id, an origin of acemq@{host},
// version and attempt starting at 1.
//
// An Envelope is a value. Copying one is fine; Headers is shared with the copy,
// so treat it as read-only once the envelope exists.
type Envelope struct {
	// ID is the unique message identifier, and the default idempotency key.
	ID string

	// Type is the logical message type. Defaults to the routing key when unset.
	Type string

	// Version is the schema version of the payload. Starts at 1.
	Version int

	// CorrelationID defaults to ID when unset.
	CorrelationID string

	// CausationID is the message that caused this one, empty when there was none.
	CausationID string

	// Attempt is the delivery attempt, starting at 1.
	Attempt int

	// FirstSeen is when the message was first published.
	FirstSeen time.Time

	// Origin is the publishing process, conventionally service@host.
	Origin string

	// Error says why the message was dead-lettered, when it was.
	Error string

	// ReplyTo is the queue an answer to this message should be sent to.
	//
	// It travels as AMQP's own reply-to property rather than as a header, which
	// is where the Java and .NET libraries look for it. Nothing in this package
	// reads it; the patterns package's Serve does, after its own acemq-reply-to
	// header. See [ReplyTo].
	ReplyTo string

	// Route, RoutePosition and RouteID carry a declared pipeline's itinerary:
	// the ordered step names, which of them this message is for, and the
	// identifier of the run.
	//
	// Java's shape, and materialised here rather than left among Headers for a
	// reason that is not tidiness: the reserved prefix means the engine drops
	// them from the application's headers, so a Java-shaped route reaching a Go
	// consumer would arrive on the wire and vanish before the handler saw it.
	//
	// Empty Route means the message is not travelling a declared route, which is
	// the usual case — this library's own routing slip is a JSON header outside
	// the reserved namespace. patterns.SlipFrom reads either. See [RouteSteps].
	Route         string
	RoutePosition int
	RouteID       string

	// Headers are the application's own. Never contains anything in the reserved
	// namespace.
	Headers map[string]any
}

// EnvelopeOption sets a field on a message being built. The same options work
// for [NewEnvelope] and for Publisher.Send:
//
//	pub.Send(ctx, order,
//		acemq.CorrelationID("corr-1"),
//		acemq.Header("x-tenant", "acme"))
type EnvelopeOption func(*envelopeBuilder) error

type envelopeBuilder struct {
	env       Envelope
	firstSeen *time.Time
}

// MessageID sets the message identifier. One is generated when this is not given.
func MessageID(id string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.ID = id; return nil }
}

// MessageType overrides the logical message type, which otherwise comes from
// the routing key the publisher was built with.
func MessageType(t string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.Type = t; return nil }
}

// SchemaVersion sets the payload's schema version.
func SchemaVersion(v int) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.Version = v; return nil }
}

// CorrelationID sets the correlation identifier. Defaults to the message id.
func CorrelationID(id string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.CorrelationID = id; return nil }
}

// CausationID records the message that caused this one.
func CausationID(id string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.CausationID = id; return nil }
}

// Attempt sets the delivery attempt counter.
func Attempt(n int) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.Attempt = n; return nil }
}

// FirstSeen sets when the message was first published.
func FirstSeen(t time.Time) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.firstSeen = &t; return nil }
}

// Origin names the publishing process. Defaults to acemq@{hostname}.
func Origin(origin string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.Origin = origin; return nil }
}

// Route puts a declared pipeline's itinerary on the message: the ordered step
// names, which of them this message is for, and the identifier of the run.
//
// Java's form. Most Go code reaches for patterns.RoutingSlip instead, which
// writes the self-describing JSON slip and needs nothing declared anywhere; this
// is what a Go step uses to answer a Java-declared pipeline in the shape its
// other steps expect. See patterns.Route.
func Route(steps []string, position int, runID string) EnvelopeOption {
	return func(b *envelopeBuilder) error {
		if len(steps) == 0 {
			return Fatalf("acemq: a route needs at least one step")
		}
		for _, step := range steps {
			// A comma in a step name would split it into two steps at the next
			// hop, which sends the message somewhere nobody named. Caught here,
			// where the route is written, rather than there.
			if strings.ContainsAny(step, ",") || strings.TrimSpace(step) == "" {
				return Fatalf(
					"acemq: %q is not a usable step name; a route is comma separated,"+
						" so a step name cannot contain a comma or be blank", step)
			}
		}
		if position < 0 {
			return Fatalf("acemq: a route position cannot be negative, was %d", position)
		}
		b.env.Route = strings.Join(steps, ",")
		b.env.RoutePosition = position
		b.env.RouteID = runID
		return nil
	}
}

// ReplyTo names the queue an answer to this message should go to, as AMQP's own
// reply-to property.
//
// A requester built by the patterns package sets this and the acemq-reply-to
// header to the same queue, so a responder written against either convention
// can answer it. Set it directly only when publishing a request by hand.
func ReplyTo(queue string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.ReplyTo = queue; return nil }
}

// DeadLetterReason records why a message was dead-lettered.
func DeadLetterReason(reason string) EnvelopeOption {
	return func(b *envelopeBuilder) error { b.env.Error = reason; return nil }
}

// Header adds an application header.
//
// It refuses a name in the reserved namespace rather than accepting one that
// would be silently dropped when the message is consumed.
func Header(name string, value any) EnvelopeOption {
	return func(b *envelopeBuilder) error {
		if IsAceHeader(name) {
			return fmt.Errorf(
				"acemq: header %q is in the reserved %q namespace and would be dropped on consume; "+
					"use a namespace of your own, such as x-yourcompany-", name, HeaderPrefix)
		}
		if b.env.Headers == nil {
			b.env.Headers = make(map[string]any)
		}
		b.env.Headers[name] = value
		return nil
	}
}

// NewEnvelope builds an envelope for a message type, applying the same defaults
// the Java and .NET libraries apply.
func NewEnvelope(msgType string, opts ...EnvelopeOption) (Envelope, error) {
	b := &envelopeBuilder{env: Envelope{Type: msgType, Version: 1, Attempt: 1}}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return Envelope{}, err
		}
	}

	env := b.env
	if env.ID == "" {
		env.ID = newID()
	}
	if env.CorrelationID == "" {
		env.CorrelationID = env.ID
	}
	if env.Origin == "" {
		env.Origin = defaultOrigin()
	}
	if b.firstSeen != nil {
		env.FirstSeen = *b.firstSeen
	} else {
		env.FirstSeen = time.Now().UTC()
	}
	if env.Headers == nil {
		env.Headers = map[string]any{}
	}
	return env, nil
}

// EnvelopeFromWire reads an envelope back off the wire.
//
// headers is every header on the message, engine and application alike.
// routingKey supplies Type when the header is absent, and messageID supplies ID.
func EnvelopeFromWire(headers map[string]any, routingKey, messageID string) Envelope {
	application := make(map[string]any, len(headers))
	for k, v := range headers {
		// Reserved headers are the engine's and never reach the application,
		// whether or not this version understands them.
		if !IsAceHeader(k) {
			application[k] = v
		}
	}

	id := headerString(headers, HeaderID)
	if id == "" {
		id = messageID
	}
	if id == "" {
		id = newID()
	}

	msgType := headerString(headers, HeaderType)
	if msgType == "" {
		msgType = routingKey
	}

	version := headerInt(headers, HeaderVersion)
	if version == 0 {
		version = 1
	}

	correlation := headerString(headers, HeaderCorrelation)
	if correlation == "" {
		correlation = id
	}

	attempt := headerInt(headers, HeaderAttempt)
	if attempt == 0 {
		attempt = 1
	}

	return Envelope{
		ID:            id,
		Type:          msgType,
		Version:       version,
		CorrelationID: correlation,
		CausationID:   headerString(headers, HeaderCausation),
		Attempt:       attempt,
		FirstSeen:     time.UnixMilli(headerInt64(headers, HeaderFirstSeen)).UTC(),
		Origin:        headerString(headers, HeaderOrigin),
		Error:         headerString(headers, HeaderError),
		Route:         headerString(headers, HeaderRoute),
		// Zero when the header is absent or unreadable, which is also the right
		// answer for a route that has one: a position nobody can read would send
		// the message to an arbitrary step, and starting the route over is the
		// only defensible reading. Java does the same.
		RoutePosition: headerInt(headers, HeaderRoutePosition),
		RouteID:       headerString(headers, HeaderRouteID),
		Headers:       application,
	}
}

// RouteSteps is the declared route's step names, in order, or nil when the
// message is not travelling one.
func (e Envelope) RouteSteps() []string {
	if e.Route == "" {
		return nil
	}
	parts := strings.Split(e.Route, ",")
	steps := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			steps = append(steps, trimmed)
		}
	}
	if len(steps) == 0 {
		return nil
	}
	return steps
}

// envelopeFromDelivery is [EnvelopeFromWire] with the delivery's own AMQP
// properties folded in.
//
// reply-to is a property and not a header, so it cannot come out of the header
// map [EnvelopeFromWire] is handed. Adding a parameter to an exported function
// that applications and the wire fixtures already call would be a worse trade
// than this one line.
func envelopeFromDelivery(d Delivery) Envelope {
	env := EnvelopeFromWire(d.Headers, d.RoutingKey, d.MessageID)
	env.ReplyTo = d.ReplyTo
	return env
}

// ToWire renders the envelope as the headers to put on the message.
//
// ReplyTo is deliberately absent: it is an AMQP property, carried on
// [Outbound.ReplyTo] rather than among the headers.
//
// An absent value is an absent header, never a null one. The Java
// implementation omits x-acemq-causation entirely when there is no causation
// rather than writing a null, and a port that writes nulls produces messages
// that differ from Java's for the same logical content.
func (e Envelope) ToWire() map[string]any {
	wire := map[string]any{
		HeaderID:          e.ID,
		HeaderType:        e.Type,
		HeaderVersion:     e.Version,
		HeaderCorrelation: e.CorrelationID,
		HeaderAttempt:     e.Attempt,
		HeaderFirstSeen:   e.FirstSeen.UnixMilli(),
	}

	if e.CausationID != "" {
		wire[HeaderCausation] = e.CausationID
	}
	if e.Origin != "" {
		wire[HeaderOrigin] = e.Origin
	}
	if e.Error != "" {
		wire[HeaderError] = e.Error
	}
	if e.Route != "" {
		// All three or none. A route with no position would be read as position
		// zero by the next hop, which restarts the run rather than continuing it.
		wire[HeaderRoute] = e.Route
		wire[HeaderRoutePosition] = e.RoutePosition
		if e.RouteID != "" {
			wire[HeaderRouteID] = e.RouteID
		}
	}

	for k, v := range e.Headers {
		if !IsAceHeader(k) {
			wire[k] = v
		}
	}
	return wire
}

// Age is how long ago the message was first published.
func (e Envelope) Age() time.Duration { return time.Since(e.FirstSeen) }

// NextAttempt returns a copy with the attempt counter advanced.
func (e Envelope) NextAttempt() Envelope {
	next := e
	next.Attempt = e.Attempt + 1
	return next
}

func defaultOrigin() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return "acemq@" + host
}

// headerString coerces a header value to a string.
//
// Broker clients are not consistent about this: a long string may arrive as a
// string or as a []byte depending on the peer, so both are read rather than
// trusting one shape.
func headerString(h map[string]any, key string) string {
	v, ok := h[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func headerInt(h map[string]any, key string) int { return int(headerInt64(h, key)) }

// headerInt64 coerces a header value to an integer, returning 0 when it is
// absent or unreadable so callers can apply their own default.
func headerInt64(h map[string]any, key string) int64 {
	v, ok := h[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		return int64(t)
	case float32:
		return int64(t)
	case float64:
		// JSON decodes every number as a float64, so fixtures and any
		// JSON-shaped header arrive here rather than in an integer case.
		return int64(t)
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0
		}
		return n
	case []byte:
		n, err := strconv.ParseInt(string(t), 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}
