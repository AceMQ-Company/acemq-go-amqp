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

import "strings"

// The header names AceMQ puts on the wire.
//
// Transliterated from org.acemq.amqp.api.AceHeaders and pinned by fixtures
// generated from the Java implementation rather than copied from its
// documentation. A port that hand-copies a wire contract acquires a difference
// nobody notices until two languages disagree in production, which is the
// failure this arrangement exists to prevent.
//
// HeaderPrefix is reserved. A header carrying it is the engine's: it is
// materialised onto the [Envelope] if this version knows it, and dropped from
// the application's headers either way. Use your own namespace —
// x-yourcompany- — for anything that must survive the round trip.
const (
	// HeaderPrefix is shared by every AceMQ-defined header.
	HeaderPrefix = "x-acemq-"

	// HeaderID is the unique message identifier, and the default idempotency key.
	HeaderID = HeaderPrefix + "id"

	// HeaderType is the logical message type, for example order.placed.
	HeaderType = HeaderPrefix + "type"

	// HeaderVersion is the schema version of the payload, as an integer.
	HeaderVersion = HeaderPrefix + "version"

	// HeaderCorrelation is the business correlation identifier, propagated
	// unchanged across hops.
	HeaderCorrelation = HeaderPrefix + "correlation"

	// HeaderCausation identifies the message that caused this one to be published.
	HeaderCausation = HeaderPrefix + "causation"

	// HeaderAttempt is the delivery attempt counter, starting at 1.
	HeaderAttempt = HeaderPrefix + "attempt"

	// HeaderFirstSeen is epoch milliseconds of the first publish, used for
	// age-based give-up.
	HeaderFirstSeen = HeaderPrefix + "first-seen"

	// HeaderOrigin identifies the publishing process, conventionally service@host.
	HeaderOrigin = HeaderPrefix + "origin"

	// HeaderClaim is where an externalised payload is, when an application
	// chooses to say so.
	//
	// The library's own claim check does not write it. That pattern frames the
	// body — see patterns.ClaimCheckCodec — because a header can be stripped by a
	// shovel or a federation link and the body cannot, and because the framing
	// has to say whether a payload travelled inline or not, which a present-or-
	// absent header cannot express for a message that predates the codec.
	//
	// This header is for an application that wants an operator reading a
	// dead-letter queue to see where the payload went without decoding anything.
	// Python and Ruby reserve it the same way and for the same reason.
	HeaderClaim = HeaderPrefix + "claim"

	// HeaderRoute is the ordered step names of a declared pipeline, comma
	// separated, and HeaderRoutePosition is which of them the message is for,
	// counting from zero. HeaderRouteID identifies one run across every hop.
	//
	// The form the Java library writes. A step name is resolved against a
	// pipeline declared in code, so the names alone are enough for a message to
	// be readable in a management console — validate,enrich,dispatch at position
	// 1 says where a message is without anybody decoding anything.
	//
	// These are materialised onto [Envelope] rather than left among the
	// application's headers, because the reserved prefix means they would
	// otherwise be dropped: a Java-shaped route reaching a Go consumer would
	// vanish between the wire and the handler. See patterns.RoutingSlip for the
	// JSON form this library writes by default, and for reading either.
	HeaderRoute         = HeaderPrefix + "route"
	HeaderRoutePosition = HeaderPrefix + "route-position"
	HeaderRouteID       = HeaderPrefix + "route-id"

	// HeaderError says why a message was dead-lettered. Present only in a
	// dead-letter queue.
	HeaderError = HeaderPrefix + "error"

	// HeaderReplayedFrom is the queue a message was replayed from.
	HeaderReplayedFrom = HeaderPrefix + "replayed-from"

	// HeaderReplayedAt is when the message was last replayed, as an ISO-8601
	// instant.
	//
	// A string, unlike [HeaderFirstSeen], which is an integer. The two timestamps
	// on the wire are encoded differently and it is not an oversight to be tidied
	// up here: matching the Java implementation is the entire point.
	HeaderReplayedAt = HeaderPrefix + "replayed-at"

	// HeaderReplayCount is how many times the message has been replayed.
	HeaderReplayCount = HeaderPrefix + "replay-count"

	// HeaderTraceParent carries W3C trace context.
	HeaderTraceParent = "traceparent"

	// HeaderTraceState carries W3C trace state.
	HeaderTraceState = "tracestate"
)

// IsAceHeader reports whether a header name belongs to the engine's reserved
// namespace.
func IsAceHeader(name string) bool {
	return strings.HasPrefix(name, HeaderPrefix)
}
