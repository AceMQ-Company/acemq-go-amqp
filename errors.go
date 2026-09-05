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
