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
	"time"
)

// Where a message goes when it cannot be handled.
//
// These names are convention rather than protocol, which is exactly why they
// have to be identical in every language: an operator looking for the dead
// letters of orders.new should find them in orders.new.dlq whether the consumer
// that gave up was written in Java, Go, .NET, Python or Ruby.
const (
	// DeadLetterSuffix names where a message goes when every attempt has been
	// used, or when it has grown too old to be worth delivering.
	DeadLetterSuffix = ".dlq"

	// ParkedSuffix names where a message goes when a person has to look at it —
	// most often one that could not be decoded at all.
	ParkedSuffix = ".parked"

	// RetryInfix separates a source queue from the delay one of its rungs holds.
	RetryInfix = ".retry."
)

// DeadLetterQueue turns orders.new into orders.new.dlq.
func DeadLetterQueue(queue string) string { return queue + DeadLetterSuffix }

// ParkedQueue turns orders.new into orders.new.parked.
func ParkedQueue(queue string) string { return queue + ParkedSuffix }

// RetryQueue turns orders.new and 30s into orders.new.retry.30s.
//
// The delay is in the name because a delay queue is per-delay: its
// x-message-ttl is fixed at declaration, so a policy with four different waits
// needs four queues, and an operator should be able to tell which is which
// without reading their arguments.
func RetryQueue(queue string, delay time.Duration) string {
	return queue + RetryInfix + shortDuration(delay)
}

// shortDuration renders a delay as the shortest thing that reads as one: 30s,
// 5m, 2h.
//
// Whole seconds only, and anything under a second becomes 0s, which is what the
// Python and Ruby libraries produce for the same input. It is not a rounding
// bug to be tidied up here: a rung queue's name is half of a cross-language
// contract, and a Go consumer that named a queue orders.new.retry.500ms while a
// Python one named the same rung orders.new.retry.0s would give the second
// service to declare it a PRECONDITION_FAILED and no way to consume at all.
//
// Sub-second rungs cannot arise from the default threshold, which is thirty
// seconds. They can only appear when a policy has deliberately lowered it, and
// a policy that does that is asking the broker to hold a message for less time
// than it takes to publish it.
func shortDuration(delay time.Duration) string {
	seconds := int64(delay / time.Second)
	switch {
	case seconds <= 0:
		return "0s"
	case seconds%3600 == 0:
		return fmt.Sprintf("%dh", seconds/3600)
	case seconds%60 == 0:
		return fmt.Sprintf("%dm", seconds/60)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}
