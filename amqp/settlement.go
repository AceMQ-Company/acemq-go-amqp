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
	"context"
	"time"
)

// SettlementAction is what the engine did with a delivery.
//
// The values are the outcome names the tracing adapters and the metric tags in
// every AceMQ library already use, so a reader does not have to learn two
// vocabularies for the same four things.
type SettlementAction string

const (
	// SettledAccepted is the handler accepting the message and the engine
	// acknowledging it. Nothing further happens to it.
	SettledAccepted SettlementAction = "accepted"

	// SettledRetried is another attempt being scheduled, either on a rung queue
	// or after a wait here. [Settlement.Delay] is how long, and
	// [Settlement.Envelope] is the message as it goes back, one attempt on.
	SettledRetried SettlementAction = "retried"

	// SettledDeadLettered is the engine giving up: the attempts ran out, the
	// message got too old, the handler rejected it, or an interceptor refused
	// it. [Settlement.Reason] is which.
	SettledDeadLettered SettlementAction = "dead_lettered"

	// SettledParked is a message that never reached the handler because nothing
	// could decode it. A different problem from a dead letter with a different
	// answer, which is why it goes to a different queue.
	SettledParked SettlementAction = "parked"
)

// Settlement is what the engine did with a delivery once the handler had
// returned.
//
// The handler's decision is not the last word on a message. A handler that asks
// for a retry may get one, or may be out of attempts and have the message
// dead-lettered instead, and only the engine knows which — so instrumentation
// that stops at the handler reports a message as retried that nobody will ever
// try again.
type Settlement struct {
	// Queue is the queue the delivery arrived on.
	Queue string

	// Action is what happened to it.
	Action SettlementAction

	// Envelope is the message. For [SettledRetried] it is the envelope as it
	// goes back on the queue, with the attempt already advanced, which is the
	// attempt the next delivery will report.
	Envelope Envelope

	// Delay is how long before the next attempt, for [SettledRetried] and zero
	// otherwise. The delay the engine actually chose, after the retry policy
	// has jittered it and decided whether the waiting happens on a rung queue
	// or in this process.
	Delay time.Duration

	// Reason is why the message was given up on, for [SettledDeadLettered] and
	// [SettledParked]. The same sentence that travels on the message.
	Reason string
}

// OnSettled asks the engine to say what it did with the delivery being handled.
//
//	acemq.OnSettled(ctx, func(s acemq.Settlement) {
//		if s.Action == acemq.SettledDeadLettered {
//			span.SetAttributes(attribute.String("outcome", "dead_lettered"))
//		}
//		span.End()
//	})
//
// Call it from inside a handler, with the handler's own context, before doing
// the work. f is called exactly once, on the same goroutine, after the handler
// has returned and after the engine has decided — which is the only moment at
// which a retry's delay or a dead letter's reason exists. A second call
// replaces the first, so a chain of wrappers registers at most one.
//
// It reports whether anything was registered. False means there is no engine
// listening — a handler called directly, from a test or from code of your own —
// and a caller holding something that has to be finished either way should
// finish it itself.
//
// This is the seam the [telemetry/otel] adapter uses to move a span's ending
// from the handler's return to the engine's decision. Go has no ambient current
// span the way Java has, so an engine that did not offer the context back could
// not be given one; and a hook the caller installs is nothing at all to a
// process that installs none. Nothing is allocated per delivery for it and
// nothing is called when nobody has registered.
//
// [telemetry/otel]: https://pkg.go.dev/github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel
func OnSettled(ctx context.Context, f func(Settlement)) bool {
	if f == nil {
		return false
	}
	hook, _ := ctx.Value(settlementKey{}).(*settlementHook)
	if hook == nil {
		return false
	}
	hook.f = f
	return true
}

// settlementHook is one delivery's registration.
//
// One of these belongs to each consumer worker rather than to each delivery: a
// worker handles one message at a time, so the hook is cleared and reused and
// the context carrying it is built once, when the worker starts. A consumer
// that nothing has instrumented therefore allocates nothing at all for this,
// which is the price a seam like this has to cost a service that traces
// nothing.
type settlementHook struct {
	f func(Settlement)
}

type settlementKey struct{}

// withSettlementHook is how a worker offers its hook to the handlers it runs.
func withSettlementHook(ctx context.Context, hook *settlementHook) context.Context {
	return context.WithValue(ctx, settlementKey{}, hook)
}

// reset forgets the previous delivery's registration, before the next one.
func (h *settlementHook) reset() {
	if h != nil {
		h.f = nil
	}
}

// settled tells whoever registered, once.
//
// Cleared before the call rather than after, so a settlement reported twice by
// a future engine path is still delivered once, and so a callback that somehow
// settles again cannot recur.
func (h *settlementHook) settled(s Settlement) {
	if h == nil || h.f == nil {
		return
	}
	f := h.f
	h.f = nil
	f(s)
}
