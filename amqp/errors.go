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
	"errors"
	"fmt"
)

// FatalError marks a failure that will happen again on every attempt.
//
// The distinction is the one the retry engine acts on. A malformed body, a
// payload that does not fit its type, a message addressed to a queue that does
// not exist: retrying any of these produces the same failure, so the message is
// dead-lettered immediately rather than occupying the queue until it ages out.
// Everything else is treated as worth another go.
//
// Wrap your own errors with [Fatal] to say so:
//
//	if !order.Valid() {
//		return acemq.Reject(acemq.Fatal(errors.New("no customer")))
//	}
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

// Fatal marks an error as one that retrying cannot fix.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return &FatalError{Err: err}
}

// Fatalf marks a new formatted error as one that retrying cannot fix.
func Fatalf(format string, args ...any) error {
	return &FatalError{Err: fmt.Errorf(format, args...)}
}

// IsFatal reports whether an error is one retrying cannot fix.
//
// It looks through wrapping, so an error marked fatal deep inside a call still
// reads as fatal at the point the engine asks.
func IsFatal(err error) bool {
	var fatal *FatalError
	return errors.As(err, &fatal)
}

// The rest of the taxonomy, matching the Java and .NET libraries.
//
// They exist so a caller can tell one failure from another with errors.As
// rather than matching on the text of a message, which changes:
//
//	var blocked *acemq.ConnectionBlockedError
//	if errors.As(err, &blocked) {
//		// the broker is out of disk; shed load rather than piling up
//	}

// RetryableError marks a failure worth another attempt.
//
// Everything unmarked is retryable already, so this is only needed to say so
// explicitly — most usefully to override something further down that would
// otherwise read as fatal.
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable marks an error as worth another attempt.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

// Retryablef marks a new formatted error as worth another attempt.
func Retryablef(format string, args ...any) error {
	return &RetryableError{Err: fmt.Errorf(format, args...)}
}

// IsRetryable reports whether an error is worth another attempt.
//
// Anything not marked fatal is, so this is the inverse of [IsFatal] rather than
// a search for a mark of its own.
func IsRetryable(err error) bool { return err != nil && !IsFatal(err) }

// TransportError is the broker or the network failing, rather than the message
// being wrong.
//
// Worth telling apart because it says nothing about the message: the same one
// may well succeed once the broker is back.
type TransportError struct {
	Op  string
	Err error
}

func (e *TransportError) Error() string { return fmt.Sprintf("acemq: %s: %v", e.Op, e.Err) }
func (e *TransportError) Unwrap() error { return e.Err }

// Transportf wraps a transport failure with what was being attempted.
func Transportf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return &TransportError{Op: fmt.Sprintf(format, args...), Err: err}
}

// PublishFailedError is a message the broker would not take, or would not route.
//
// It carries where the message was going, which is the first thing anybody
// wants when a routing key was built from a variable.
type PublishFailedError struct {
	MessageID  string
	Exchange   string
	RoutingKey string

	// Unroutable is true when the message reached no queue rather than being
	// refused. The broker was working; nothing was listening.
	Unroutable bool

	Err error
}

func (e *PublishFailedError) Error() string {
	where := fmt.Sprintf("exchange %q with key %q", e.Exchange, e.RoutingKey)
	if e.Unroutable {
		return fmt.Sprintf("acemq: message %s to %s reached no queue: %v", e.MessageID, where, e.Err)
	}
	return fmt.Sprintf("acemq: cannot publish message %s to %s: %v", e.MessageID, where, e.Err)
}

func (e *PublishFailedError) Unwrap() error { return e.Err }

// ConnectionBlockedError is the broker telling publishers to stop.
//
// RabbitMQ sends connection.blocked when it is low on memory or disk, and every
// publish on that connection then blocks until it sends connection.unblocked. A
// publisher that does not know this looks exactly like one that has hung.
//
// Reason is the broker's own words: "low on disk space", "low on memory".
type ConnectionBlockedError struct{ Reason string }

func (e *ConnectionBlockedError) Error() string {
	return fmt.Sprintf(
		"acemq: the broker has blocked publishing on this connection (%s); "+
			"it is out of resources and will unblock when that is dealt with", e.Reason)
}

// PublishingPausedError is a publish refused because the connection is blocked.
//
// Returned rather than blocking, so a service can shed load, buffer, or fail a
// request instead of piling up goroutines against a broker that has already
// said it cannot take any more.
type PublishingPausedError struct{ Reason string }

func (e *PublishingPausedError) Error() string {
	return fmt.Sprintf(
		"acemq: publishing is paused because the broker blocked this connection (%s)", e.Reason)
}

// IsBlocked reports whether an error is the broker refusing traffic, either
// because the connection is blocked or because publishing is paused for it.
func IsBlocked(err error) bool {
	var blocked *ConnectionBlockedError
	var paused *PublishingPausedError
	return errors.As(err, &blocked) || errors.As(err, &paused)
}
