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

type ackAction int

const (
	ackAccept ackAction = iota
	ackRetry
	ackReject
)

// Ack is what a handler says about a message: it worked, it should be tried
// again, or it should never be tried again.
//
// Returning a value rather than calling a method means a handler that forgets
// to decide does not compile, which is the failure this shape exists to
// prevent. A message nobody acknowledges sits unacknowledged until the
// connection drops, and then comes back — usually to the same handler, with the
// same outcome.
type Ack struct {
	action ackAction
	err    error
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

// Err is the reason the handler gave, if it gave one.
func (a Ack) Err() error { return a.err }

// String makes an Ack readable in a log line.
func (a Ack) String() string {
	switch a.action {
	case ackAccept:
		return "accept"
	case ackRetry:
		return "retry"
	case ackReject:
		return "reject"
	default:
		return "unknown"
	}
}
