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

import "time"

type ackAction int

const (
	ackAccept ackAction = iota
	ackRetry
	ackReject
	ackPark
	ackInProgress
)

// Ack is what a handler says about a message: it worked, it should be tried
// again, it should never be tried again, or nobody could read it.
//
// Returning a value rather than calling a method means a handler that forgets
// to decide does not compile, which is the failure this shape exists to
// prevent. A message nobody acknowledges sits unacknowledged until the
// connection drops, and then comes back — usually to the same handler, with the
// same outcome.
type Ack struct {
	action ackAction
	err    error
	delay  time.Duration
}

// Accept confirms the message. It will not be delivered again.
func Accept() Ack { return Ack{action: ackAccept} }

// Retry returns the message to the queue to be tried again.
//
// The retry policy decides whether there is another attempt left; when there is
// not, the message is dead-lettered with err as the reason. An err marked with
// [Fatal] skips the remaining attempts, because they would all fail the same
// way.
func Retry(err error) Ack { return Ack{action: ackRetry, err: err} }

// Reject dead-letters the message without trying again.
//
// Use it when the message itself is the problem — a field that cannot be
// missing is missing, a reference points at nothing — rather than when the
// world is temporarily unhelpful.
func Reject(err error) Ack { return Ack{action: ackReject, err: err} }

// Park sends the message to {queue}.parked without trying again.
//
// Use it when the message cannot be read at all — a body that is not the shape
// it claims, a schema version from a future nobody deployed yet — as against
// [Reject], which is for a message that was read and found wanting.
//
// Both are final and neither is retried; what differs is where the message ends
// up and therefore who has to look at it. The dead-letter queue is a queue of
// work that failed, and somebody drains it looking for a broker or a downstream
// that has since recovered. The parked queue is a queue of messages nothing
// could read, and somebody drains it looking for the producer that sent them.
// Mixing the two makes both drains guesswork.
//
// The engine parks a body it cannot decode by itself, and this is the same
// destination and the same reporting — parked on the counter and on the span —
// reached by a handler that got further before it found out.
func Park(err error) Ack { return Ack{action: ackPark, err: err} }

// DefaultInProgressDelay is how long a message found in progress waits before it
// is put back, when nothing says otherwise. The same five seconds every AceMQ
// library uses.
const DefaultInProgressDelay = 5 * time.Second

// InProgress says somebody else holds this message: neither run it nor accept
// it, but put it back and look again after delay.
//
// patterns.Idempotent answers with it when a redelivery finds a claim that is
// live and unconfirmed — a handler still running, or one that died and could
// not release its claim. Accepting that redelivery as a duplicate loses the
// message if the claim never becomes a completion.
//
// The engine waits delay, republishes the message to its own queue with the
// attempt unchanged, and acknowledges the original (or returns it to the broker
// if the republish is not routed). The retry policy is not consulted: holding a
// message elsewhere is not a failure of this one, so it spends no attempt and is
// never dead-lettered for it. Counted as outcome in_progress. A stream consumer
// refuses it as it refuses [Retry], because putting it back would append a second
// copy to the log. A negative delay is zero.
func InProgress(delay time.Duration) Ack {
	return Ack{action: ackInProgress, delay: max(delay, 0)}
}

// Err is the reason the handler gave, if it gave one.
func (a Ack) Err() error { return a.err }

// IsRetry reports whether a handler asked for another attempt.
//
// Exported for one caller and one reason: patterns.ReadStream has to refuse a
// retry, because on a stream "republish onto the queue it came from" appends a
// second copy of the message to the log. It lives in another package and the
// action is unexported, so without this the only way to ask would be to compare
// [Ack.String] against a literal — a wire between two packages made of a word.
//
// It is not a way to second-guess a handler in general. Everything the engine
// does with an Ack is decided in [Settlement], where the retry policy and the
// attempt counter are, and a caller branching on this instead is reimplementing
// that badly.
func (a Ack) IsRetry() bool { return a.action == ackRetry }

// IsInProgress reports whether a handler answered [InProgress], for the same one
// caller and reason as [Ack.IsRetry]: a stream has to refuse it.
func (a Ack) IsInProgress() bool { return a.action == ackInProgress }

// String makes an Ack readable in a log line.
func (a Ack) String() string {
	switch a.action {
	case ackAccept:
		return "accept"
	case ackRetry:
		return "retry"
	case ackReject:
		return "reject"
	case ackPark:
		return "park"
	case ackInProgress:
		return "in_progress"
	default:
		return "unknown"
	}
}
