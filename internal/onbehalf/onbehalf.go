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

// Package onbehalf marks a publish the library makes for its user rather than
// one the user asked for: a retry hop, a set-aside, an outbox relay, a routing
// slip or pipeline hop, a scheduler hop or delivery, a replay.
//
// Each of those settles something as soon as the publish returns — acknowledges
// the input, marks the record sent — so each has to know whether the message
// reached a queue. That answer only exists with publisher confirms: the broker
// sends basic.return before basic.ack, so the ack proves no return is coming.
// A transport therefore confirms a marked publish even on a connection whose
// own publishing has confirms turned off.
package onbehalf

import "context"

type key struct{}

// Mark says the publish made with ctx is the library's own.
func Mark(ctx context.Context) context.Context { return context.WithValue(ctx, key{}, true) }

// Marked reports whether ctx was marked.
func Marked(ctx context.Context) bool {
	marked, _ := ctx.Value(key{}).(bool)
	return marked
}

type mandatoryKey struct{}

// MarkMandatory marks ctx as [Mark] does, and also makes the publish mandatory
// whatever the publisher was built with: for a hop that is given the user's
// publisher, such as patterns.Then, and settles its input on the answer.
func MarkMandatory(ctx context.Context) context.Context {
	return context.WithValue(Mark(ctx), mandatoryKey{}, true)
}

// Mandatory reports whether ctx was marked with [MarkMandatory].
func Mandatory(ctx context.Context) bool {
	mandatory, _ := ctx.Value(mandatoryKey{}).(bool)
	return mandatory
}
