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
	"sync"
	"time"
)

// Metric names, so a dashboard built against one AceMQ library reads the same
// against another.
//
// Java and .NET publish these through Micrometer and System.Diagnostics.Metrics
// respectively. Go's standard library has no metrics interface at all, so this
// package counts them itself and hands them to whatever you use — see
// [Observer].
const (
	// MetricPublished counts messages handed to the broker.
	MetricPublished = "acemq.messages.published"

	// MetricPublishFailed counts publishes that did not succeed.
	MetricPublishFailed = "acemq.messages.publish.failed"

	// MetricConsumed counts deliveries this library has finished with, tagged
	// with the outcome the engine settled on.
	//
	// Counted when the delivery is settled rather than when it arrives, because
	// the outcome does not exist until then. Summing it across every outcome
	// gives the number of deliveries handled.
	MetricConsumed = "acemq.messages.consumed"

	// MetricAccepted, MetricRetried, MetricRejected, MetricDeadLettered and
	// MetricParked count what the engine did, one of them per delivery.
	//
	// What the engine did, and not what the handler asked for. A handler that
	// asks for a retry on its last permitted attempt gets a dead letter, and
	// counting the request rather than the decision is how this library used to
	// report a retry that never happened. See [Settlement].
	MetricAccepted = "acemq.messages.accepted"
	MetricRetried  = "acemq.messages.retried"

	// MetricRejected counts messages a handler gave up on by name. They go to
	// the dead-letter queue like an exhausted message, and only the word keeps
	// the two apart — which is the difference between a decision somebody took
	// and a message the world defeated.
	MetricRejected = "acemq.messages.rejected"

	// MetricDeadLettered counts messages the engine gave up on: the attempts
	// ran out, the message got too old, the handler reported the failure as
	// unprocessable, or an interceptor refused it.
	MetricDeadLettered = "acemq.messages.dead.lettered"

	// MetricParked counts messages nothing could decode, which never reached a
	// handler at all and go to {queue}.parked rather than the dead letters.
	MetricParked = "acemq.messages.parked"

	// MetricHandlerDuration is how long handlers take, in seconds, tagged with
	// what the engine then did about it.
	//
	// Only deliveries that reached a handler are timed. A body that would not
	// decode never ran one, and recording a zero for it would drag down the
	// average of a metric named for handlers.
	MetricHandlerDuration = "acemq.handler.duration"

	// MetricInFlight is how many messages are being handled right now.
	MetricInFlight = "acemq.messages.in.flight"

	// MetricRungMissing counts long retries that had to wait in the consumer
	// because the rung queue they were meant to wait on is not on the broker.
	//
	// Worth an alert. Nothing breaks: the message is still retried, and the wait
	// still happens. What is lost is the reason the rung exists — a consumer
	// restart mid-wait now shortens a five-minute backoff to nothing — and there
	// would otherwise be no sign of it, because this library writes no log lines.
	MetricRungMissing = "acemq.retry.rung.missing"

	// MetricSetAsideFailed counts messages that could not be moved to a
	// dead-letter or parking queue, usually because it has not been declared.
	//
	// The message is rejected to the broker instead, which is the last thing
	// between it and nothing.
	MetricSetAsideFailed = "acemq.messages.set.aside.failed"

	// MetricOutboxLag is how long an outbox record waited between being
	// committed and being published, in seconds.
	//
	// The question an outbox raises is never "how many" but "how far behind",
	// and it is measured from when the row was written rather than from when
	// the sweep picked it up: what a lag answers is how long somebody has been
	// owed this message, and the wait for a sweep is part of the answer rather
	// than the start of it.
	MetricOutboxLag = "acemq.outbox.lag"

	// MetricOutboxTotal counts outbox records the relay has handled, tagged
	// published or failed.
	MetricOutboxTotal = "acemq.outbox.total"
)

// The outcomes, which are the values of the outcome tag on the consume metrics
// and of the messaging.acemq.outcome attribute the tracing adapters write. One
// vocabulary, so a counter and a span for the same delivery say the same word.
//
// These are the consume side. The publish and request outcomes — confirmed,
// published, unroutable, answered, timed_out — are the tracing adapter's, which
// is the only place they are written.
const (
	// OutcomeAcked is the handler accepting the message and the engine
	// acknowledging it.
	OutcomeAcked = "acked"

	// OutcomeRetried is another attempt actually being scheduled — not merely
	// asked for.
	OutcomeRetried = "retried"

	// OutcomeRejected is a handler giving up on the message by name.
	OutcomeRejected = "rejected"

	// OutcomeDeadLettered is the engine giving up: the attempts ran out, the
	// message got too old, the failure was reported as unprocessable, or an
	// interceptor refused it.
	OutcomeDeadLettered = "dead_lettered"

	// OutcomeParked is a message nothing could decode.
	OutcomeParked = "parked"

	// OutcomeFailed and OutcomePublished are the outbox relay's, and are the
	// same two words the tracing adapter writes for a publish.
	OutcomeFailed    = "failed"
	OutcomePublished = "published"
)

// The metric tag names this library writes, named so a dashboard query and the
// code that feeds it cannot drift apart.
//
// TagRoutingKey is key rather than Java's routing.key. That is a difference
// between the libraries and not one this package gets to settle on its own —
// the Python library writes key as well.
const (
	TagQueue      = "queue"
	TagOutcome    = "outcome"
	TagExchange   = "exchange"
	TagRoutingKey = "key"
	TagRung       = "rung"
	TagTarget     = "target"
)

// Observer is told what the library is doing.
//
// An interface rather than a dependency on a metrics library, because Go has no
// standard one and choosing for you would put every user of this package on the
// same one. Implement it against Prometheus, OpenTelemetry, statsd or a log
// line; [Metrics] is a working implementation for when you only want the
// numbers.
//
// Every method must be safe to call from several goroutines, and must not
// block: they are called on the path a message takes.
type Observer interface {
	// Count adds to a counter. Labels are name/value pairs, already flattened.
	Count(metric string, delta int64, labels map[string]string)

	// Observe records a duration in seconds.
	Observe(metric string, seconds float64, labels map[string]string)

	// Gauge sets a current value.
	Gauge(metric string, value int64, labels map[string]string)
}

// NopObserver ignores everything. The default, so nothing is measured until
// somebody asks for it.
type NopObserver struct{}

func (NopObserver) Count(string, int64, map[string]string)     {}
func (NopObserver) Observe(string, float64, map[string]string) {}
func (NopObserver) Gauge(string, int64, map[string]string)     {}

// Metrics is an [Observer] that keeps the numbers in memory.
//
// Enough to expose from a health endpoint, assert on in a test, or print on a
// signal. It is not a substitute for a real metrics system: there are no
// histograms, no percentiles, and nothing is exported anywhere.
type Metrics struct {
	mu        sync.Mutex
	counts    map[string]int64
	gauges    map[string]int64
	durations map[string]*durationSummary
}

type durationSummary struct {
	Count   int64
	Sum     float64
	Min     float64
	Max     float64
	started bool
}

// NewMetrics returns an empty collector.
func NewMetrics() *Metrics {
	return &Metrics{
		counts:    map[string]int64{},
		gauges:    map[string]int64{},
		durations: map[string]*durationSummary{},
	}
}

// Count adds to a counter.
func (m *Metrics) Count(metric string, delta int64, labels map[string]string) {
	key := metricKey(metric, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[key] += delta
}

// Observe records a duration.
func (m *Metrics) Observe(metric string, seconds float64, labels map[string]string) {
	key := metricKey(metric, labels)
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.durations[key]
	if !ok {
		s = &durationSummary{}
		m.durations[key] = s
	}
	s.Count++
	s.Sum += seconds
	if !s.started || seconds < s.Min {
		s.Min = seconds
	}
	if !s.started || seconds > s.Max {
		s.Max = seconds
	}
	s.started = true
}

// Gauge sets a value.
func (m *Metrics) Gauge(metric string, value int64, labels map[string]string) {
	key := metricKey(metric, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[key] = value
}

// Counts returns every counter, keyed by metric and labels.
func (m *Metrics) Counts() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.counts))
	for k, v := range m.counts {
		out[k] = v
	}
	return out
}

// Gauges returns every gauge.
func (m *Metrics) Gauges() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.gauges))
	for k, v := range m.gauges {
		out[k] = v
	}
	return out
}

// Durations returns a summary per metric: how many, the total, the fastest and
// the slowest.
func (m *Metrics) Durations() map[string]DurationSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]DurationSummary, len(m.durations))
	for k, v := range m.durations {
		out[k] = DurationSummary{Count: v.Count, Sum: v.Sum, Min: v.Min, Max: v.Max}
	}
	return out
}

// DurationSummary is what a [Metrics] knows about a timing.
//
// Deliberately not percentiles. Computing those needs either every sample kept
// or a sketch, and a library that quietly did either would be making a decision
// about memory that belongs to the application.
type DurationSummary struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// Mean is the average, or zero when nothing has been recorded.
func (d DurationSummary) Mean() float64 {
	if d.Count == 0 {
		return 0
	}
	return d.Sum / float64(d.Count)
}

// metricKey flattens a metric and its labels into one string.
//
// Sorted, so the same labels always give the same key however the map was
// iterated — otherwise one counter would become several.
func metricKey(metric string, labels map[string]string) string {
	if len(labels) == 0 {
		return metric
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sortStrings(keys)

	key := metric
	for _, k := range keys {
		key += "{" + k + "=" + labels[k] + "}"
	}
	return key
}

// sortStrings is a small insertion sort: label sets are tiny, and this avoids
// pulling the sort package into the message path.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// WithObserver reports what the connection does to an [Observer].
//
//	metrics := acemq.NewMetrics()
//	mq, err := acemq.Connect(ctx, url, acemq.WithObserver(metrics))
func WithObserver(o Observer) ConnOption {
	return func(cfg *connConfig) error {
		if o == nil {
			return errNilObserver
		}
		cfg.observer = o
		return nil
	}
}

// observeConsume records what the engine did with one delivery.
//
// The outcome is an argument rather than something worked out from the
// handler's [Ack], and that is the whole point of this function's shape.
// Deriving it from what the handler asked for was the bug: a message that used
// up its last attempt asked to be retried and was dead-lettered, so
// [MetricRetried] counted a retry nobody would ever make and [MetricDeadLettered]
// only ever saw the give-ups a handler named itself. The same outcome goes on
// the delivery's span through [Settlement], so a counter and a trace queried for
// the same delivery answer with the same word.
//
// Exactly one of the five per-outcome counters is incremented, so they sum to
// [MetricConsumed].
func observeConsume(o Observer, queue, outcome string) {
	if o == nil {
		return
	}
	labels := map[string]string{TagQueue: queue, TagOutcome: outcome}

	o.Count(MetricConsumed, 1, labels)
	switch outcome {
	case OutcomeAcked:
		o.Count(MetricAccepted, 1, labels)
	case OutcomeRetried:
		o.Count(MetricRetried, 1, labels)
	case OutcomeRejected:
		o.Count(MetricRejected, 1, labels)
	case OutcomeDeadLettered:
		o.Count(MetricDeadLettered, 1, labels)
	case OutcomeParked:
		o.Count(MetricParked, 1, labels)
	}
}

// observeHandler records how long a handler took, under what the engine then
// decided.
//
// Separate from [observeConsume] because a delivery that never reached a
// handler — an interceptor refused it, or nothing could decode the body — still
// settles and still counts, and timing it at zero would make a metric named for
// handlers report work no handler did.
func observeHandler(o Observer, queue, outcome string, took time.Duration) {
	if o == nil {
		return
	}
	o.Observe(MetricHandlerDuration, took.Seconds(),
		map[string]string{TagQueue: queue, TagOutcome: outcome})
}

// ObserveOutbox records what became of one outbox record.
//
// Exported because the outbox relay lives in the patterns package, which cannot
// reach an unexported function here. Applications have no reason to call it —
// the relay does — and it is the relay's only way to be visible at all: a sweep
// runs on a goroutine of its own with no span open, so the tracing adapter's
// outbox events have nothing to attach to and these counters are what is left.
// See [MetricOutboxLag].
func ObserveOutbox(o Observer, exchange, routingKey, outcome string, lag time.Duration) {
	if o == nil {
		return
	}
	labels := map[string]string{
		TagExchange: exchange, TagRoutingKey: routingKey, TagOutcome: outcome}

	o.Count(MetricOutboxTotal, 1, labels)
	if outcome == OutcomePublished {
		o.Observe(MetricOutboxLag, lag.Seconds(), labels)
	}
}
