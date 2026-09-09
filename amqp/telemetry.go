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
// These are Java's, from MetricNames in acemq-amqp-api, and Java is the library
// the rest of the family is ported from. Go, Python and Ruby each grew their own
// names first and the four sets were disjoint, so the promise this comment used
// to make — that a dashboard reads the same against another library — was not
// true. It is now, and the cost of making it true is that every dashboard
// written against the old names has to be edited. See the changelog.
//
// Java and .NET publish these through Micrometer and System.Diagnostics.Metrics
// respectively. Go's standard library has no metrics interface at all, so this
// package counts them itself and hands them to whatever you use — see
// [Observer].
//
// # One counter, tagged, rather than one counter per outcome
//
// The family counts a publish and a delivery once each, under [MetricPublishTotal]
// and [MetricConsumeTotal], and says what happened in the outcome tag. Go used to
// write a separate counter per outcome as well as the total, which is the same
// numbers twice and gave a dashboard two ways to be wrong about one thing.
//
// The two exceptions are [MetricRetriedTotal] and [MetricDeadLetteredTotal],
// which Java keeps standing alone because they are what an alert is written
// against and an alert should not have to know a tag vocabulary to fire. There
// is deliberately no third for parking: a parked message is
// MetricConsumeTotal{outcome=parked} and nothing else, which is what Java
// settled on and what this library follows rather than inventing a counter the
// rest of the family would not have.
const (
	// MetricPublishDuration is how long a publish took, in seconds, from the
	// call to the broker answering, tagged with the outcome.
	MetricPublishDuration = "acemq.publish.duration"

	// MetricPublishTotal counts messages handed to the broker, tagged with the
	// outcome: confirmed, published, unroutable or failed.
	//
	// confirmed means the broker took responsibility. published means it went
	// out with nothing promised, which is what a publisher without confirms
	// gets. The difference matters and only the tag records it.
	MetricPublishTotal = "acemq.publish.total"

	// MetricConsumeDuration is how long handlers take, in seconds, tagged with
	// what the engine then did about it.
	//
	// Only deliveries that reached a handler are timed. A body that would not
	// decode never ran one, and recording a zero for it would drag down the
	// average of a metric that is read as "how long does the work take".
	MetricConsumeDuration = "acemq.consume.duration"

	// MetricConsumeTotal counts deliveries this library has finished with,
	// tagged with the outcome the engine settled on.
	//
	// Counted when the delivery is settled rather than when it arrives, because
	// the outcome does not exist until then. Summing it across every outcome
	// gives the number of deliveries handled.
	//
	// The outcome is what the engine did, and not what the handler asked for. A
	// handler that asks for a retry on its last permitted attempt gets a dead
	// letter, and counting the request rather than the decision is how this
	// library used to report a retry that never happened. See [Settlement].
	MetricConsumeTotal = "acemq.consume.total"

	// MetricConsumeInFlight is how many messages are being handled right now,
	// bounded by prefetch times concurrency.
	MetricConsumeInFlight = "acemq.consume.in.flight"

	// MetricRetriedTotal counts messages sent to a retry queue — another
	// attempt actually being scheduled, not merely asked for.
	//
	// Also visible as MetricConsumeTotal{outcome=retried}. It stands alone
	// because a rising retry rate is the first sign of a struggling dependency
	// and is worth an alert of its own.
	MetricRetriedTotal = "acemq.messages.retried.total"

	// MetricDeadLetteredTotal counts messages the engine gave up on: the
	// attempts ran out, the message got too old, the handler reported the
	// failure as unprocessable, or an interceptor refused it.
	//
	// Also visible as MetricConsumeTotal{outcome=dead_lettered}, and standing
	// alone for the same reason: it is the counter an alert is written against.
	MetricDeadLetteredTotal = "acemq.messages.dead.lettered.total"

	// MetricSetAsideFailed counts messages that could not be moved to a
	// dead-letter or parking queue, usually because it has not been declared.
	//
	// The message is rejected to the broker instead, which is the last thing
	// between it and nothing.
	//
	// The counter that separates two failures which look identical from anywhere
	// else. A message dead-lettered normally leaves the source queue and appears
	// in the dead-letter queue; a message whose dead-letter queue was never
	// declared leaves the source queue and appears nowhere. Queue depths show one
	// queue going down in both cases, and only this number says which happened.
	MetricSetAsideFailed = "acemq.messages.set.aside.failed"

	// MetricRungMissing counts long retries that had to wait in the consumer
	// because the rung queue they were meant to wait on is not on the broker.
	//
	// Worth an alert. Nothing breaks: the message is still retried, and the wait
	// still happens. What is lost is the reason the rung exists — a consumer
	// restart mid-wait now shortens a five-minute backoff to nothing — and there
	// would otherwise be no sign of it, because this library writes no log lines.
	MetricRungMissing = "acemq.retry.rung.missing"

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

// The rest of the family's vocabulary, which this library names but does not
// write. Declared so that an [Observer] bridging AceMQ onto a metrics system can
// be written against one list, and so that adding the emission later cannot
// invent a second spelling for something Java already named.
const (
	// MetricConsumeAttempts is which attempt a delivery was, as a distribution,
	// so a rising one shows a struggling dependency.
	//
	// Not written by this library. [Observer] has counters, gauges and durations
	// and no general distribution, and adding one to a published interface to
	// carry a small integer is not worth what it breaks. The number is on every
	// message as Envelope.Attempt, so an application that wants it can record it
	// from a handler in one line.
	MetricConsumeAttempts = "acemq.consume.attempts"

	// MetricRequestDuration is the round trip of a request/reply call as the
	// caller experienced it, and MetricRequestTotal counts those calls, tagged
	// answered, timed_out or failed.
	//
	// Not written by this library: patterns.Request is a function over a
	// connection rather than something the connection knows it is doing, so
	// there is no point on the path holding an observer. The tracing adapter
	// spans the round trip instead.
	MetricRequestDuration = "acemq.request.duration"
	MetricRequestTotal    = "acemq.request.total"

	// MetricPipelineRunDuration is how long a message had existed when it left a
	// pipeline, and MetricPipelineRunTotal counts runs that finished, tagged
	// with the outcome and the step it ended at.
	//
	// Not written by this library. Go has no Pipeline type owning its steps the
	// way Java does; a run finishing is reported through patterns.RunObserver,
	// which the tracing adapter turns into a span event. An application that
	// wants counters can install its own RunObserver and write these two names.
	MetricPipelineRunDuration = "acemq.pipeline.run.duration"
	MetricPipelineRunTotal    = "acemq.pipeline.run.total"
)

// The outcomes, which are the values of the outcome tag on the publish and
// consume metrics and of the messaging.acemq.outcome attribute the tracing
// adapters write. One vocabulary, so a counter and a span for the same delivery
// say the same word.
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

	// OutcomeConfirmed is the broker taking responsibility for a published
	// message, which is what a publisher with confirms gets.
	OutcomeConfirmed = "confirmed"

	// OutcomePublished is a message that went out with nothing promised about
	// it, which is what a publisher without confirms gets. Also the outbox
	// relay's word for a record it got out.
	OutcomePublished = "published"

	// OutcomeUnroutable is a mandatory message that reached no queue at all.
	// The broker handed it back rather than dropping it.
	OutcomeUnroutable = "unroutable"

	// OutcomeFailed is a publish that errored, or an outbox record the relay
	// could not get out.
	OutcomeFailed = "failed"

	// OutcomeAnswered and OutcomeTimedOut are a request/reply round trip's, and
	// are written by the tracing adapter rather than by a counter here. See
	// [MetricRequestTotal].
	OutcomeAnswered = "answered"
	OutcomeTimedOut = "timed_out"
)

// The metric tag names this library writes, named so a dashboard query and the
// code that feeds it cannot drift apart.
//
// TagRoutingKey was key until it was aligned on Java's and .NET's routing.key.
// Two libraries wrote each spelling and neither reading was wrong, but the
// fully-qualified one says which key it means next to a tag called queue, and
// Java is the library the others are ported from. A dashboard that groups
// publishes by key has to be edited; see the changelog.
//
// # A dot is legal here and illegal in Prometheus
//
// routing.key and message.type are the family's spellings and both carry a dot,
// which a Prometheus label name may not. Emitted verbatim they would not make
// one label wrong, they would make the whole scrape unparseable — so the
// actuator rewrites a label name the same way it rewrites a metric name, and
// they arrive as routing_key and message_type. An Observer written by hand has
// to do the same; see docs/observability.md.
const (
	TagQueue       = "queue"
	TagOutcome     = "outcome"
	TagExchange    = "exchange"
	TagRoutingKey  = "routing.key"
	TagMessageType = "message.type"
	TagTransport   = "transport"
	TagPipeline    = "pipeline"
	TagStep        = "step"
	TagRung        = "rung"
	TagTarget      = "target"
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
// [MetricRetriedTotal] counted a retry nobody would ever make and
// [MetricDeadLetteredTotal] only ever saw the give-ups a handler named itself.
// The same outcome goes on
// the delivery's span through [Settlement], so a counter and a trace queried for
// the same delivery answer with the same word.
//
// Every delivery increments [MetricConsumeTotal] exactly once, so summing it
// across the outcome tag gives the number of deliveries handled. Two outcomes
// also increment a counter of their own — see [MetricRetriedTotal] — and those
// two are a second view of the same events rather than an addition to them.
func observeConsume(o Observer, queue, outcome string) {
	if o == nil {
		return
	}
	labels := map[string]string{TagQueue: queue, TagOutcome: outcome}

	o.Count(MetricConsumeTotal, 1, labels)
	switch outcome {
	case OutcomeRetried:
		o.Count(MetricRetriedTotal, 1, labels)
	case OutcomeDeadLettered:
		o.Count(MetricDeadLetteredTotal, 1, labels)
	}
}

// observeHandler records how long a handler took, under what the engine then
// decided.
//
// Separate from [observeConsume] because a delivery that never reached a
// handler — an interceptor refused it, or nothing could decode the body — still
// settles and still counts, and timing it at zero would make a metric read as
// "how long does the work take" report work no handler did.
func observeHandler(o Observer, queue, outcome string, took time.Duration) {
	if o == nil {
		return
	}
	o.Observe(MetricConsumeDuration, took.Seconds(),
		map[string]string{TagQueue: queue, TagOutcome: outcome})
}

// observePublish records one publish under [MetricPublishTotal] and
// [MetricPublishDuration].
//
// The outcome is worked out from what the broker actually said rather than from
// whether Publish returned an error, which is the same discipline the consume
// side follows: a mandatory message the broker handed back is not a failure —
// nothing went wrong, nothing was listening — and calling it one would hide the
// difference between a broken publisher and an unbound routing key.
func observePublish(o Observer, exchange, routingKey, outcome string, took time.Duration) {
	if o == nil {
		return
	}
	labels := map[string]string{
		TagExchange: exchange, TagRoutingKey: routingKey, TagOutcome: outcome}

	o.Count(MetricPublishTotal, 1, labels)
	o.Observe(MetricPublishDuration, took.Seconds(), labels)
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
