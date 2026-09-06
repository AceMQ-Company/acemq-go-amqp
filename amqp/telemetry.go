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

	// MetricConsumed counts messages delivered to a handler.
	MetricConsumed = "acemq.messages.consumed"

	// MetricAccepted, MetricRetried and MetricRejected count what handlers
	// decided.
	MetricAccepted = "acemq.messages.accepted"
	MetricRetried  = "acemq.messages.retried"
	MetricRejected = "acemq.messages.rejected"

	// MetricDeadLettered counts messages that ran out of attempts.
	MetricDeadLettered = "acemq.messages.dead.lettered"

	// MetricHandlerDuration is how long handlers take, in seconds.
	MetricHandlerDuration = "acemq.handler.duration"

	// MetricInFlight is how many messages are being handled right now.
	MetricInFlight = "acemq.messages.in.flight"
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

// observeConsume records what happened to one message.
func observeConsume(o Observer, queue string, ack Ack, took time.Duration) {
	if o == nil {
		return
	}
	labels := map[string]string{"queue": queue}

	o.Observe(MetricHandlerDuration, took.Seconds(), labels)
	switch ack.action {
	case ackAccept:
		o.Count(MetricAccepted, 1, labels)
	case ackRetry:
		o.Count(MetricRetried, 1, labels)
	case ackReject:
		o.Count(MetricRejected, 1, labels)
	}
}
