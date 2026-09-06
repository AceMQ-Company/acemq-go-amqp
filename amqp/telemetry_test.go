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
	"errors"
	"strings"
	"testing"
	"time"
)

func countFor(m *Metrics, metric string) int64 {
	var total int64
	for key, value := range m.Counts() {
		if strings.HasPrefix(key, metric) {
			total += value
		}
	}
	return total
}

func TestPublishingAndConsumingAreCounted(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithObserver(metrics))
	declare(t, mq, "orders")

	done := make(chan struct{}, 3)
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			done <- struct{}{}
			return Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	for i := range 3 {
		if err := pub.Send(ctx, OrderPlaced{OrderID: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("messages did not arrive")
		}
	}
	// The gauge is decremented in a deferred call, so give the last handler a
	// moment to unwind before reading.
	time.Sleep(100 * time.Millisecond)

	if got := countFor(metrics, MetricPublished); got != 3 {
		t.Errorf("%s = %d, want 3", MetricPublished, got)
	}
	if got := countFor(metrics, MetricConsumed); got != 3 {
		t.Errorf("%s = %d, want 3", MetricConsumed, got)
	}
	if got := countFor(metrics, MetricAccepted); got != 3 {
		t.Errorf("%s = %d, want 3", MetricAccepted, got)
	}
	if len(metrics.Durations()) == 0 {
		t.Error("no handler durations were recorded")
	}
}

func TestRetriesAndDeadLetteringAreCounted(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithObserver(metrics), WithRetry(FixedRetry(2, 0)))
	declare(t, mq, "orders")

	attempts := make(chan struct{}, 10)
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			attempts <- struct{}{}
			return Retry(errors.New("still broken"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		select {
		case <-attempts:
		case <-time.After(3 * time.Second):
			t.Fatal("the message was not retried")
		}
	}
	time.Sleep(200 * time.Millisecond)

	if got := countFor(metrics, MetricRetried); got < 2 {
		t.Errorf("%s = %d, want at least 2", MetricRetried, got)
	}
	// The point of the metric: a message that ran out of attempts is gone, and
	// that should be visible without reading a log.
	if got := countFor(metrics, MetricDeadLettered); got != 1 {
		t.Errorf("%s = %d, want 1", MetricDeadLettered, got)
	}
}

func TestAFailedPublishIsCounted(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithObserver(metrics))

	if err := mq.DeclareExchange(ctx, "events", "topic"); err != nil {
		t.Fatal(err)
	}

	err := NewPublisher[OrderPlaced](mq, "events", "nothing.listens", Mandatory[OrderPlaced]()).
		Send(ctx, OrderPlaced{OrderID: "o-1"})
	if err == nil {
		t.Fatal("the unroutable publish succeeded")
	}

	if got := countFor(metrics, MetricPublishFailed); got != 1 {
		t.Errorf("%s = %d, want 1", MetricPublishFailed, got)
	}
}

func TestLabelsDoNotSplitOneCounterIntoSeveral(t *testing.T) {
	// Map iteration order is deliberately random in Go, so a key built by
	// walking the map would give a different key each time and one counter
	// would quietly become many.
	metrics := NewMetrics()
	labels := map[string]string{"queue": "orders", "exchange": "events", "kind": "topic"}

	for range 100 {
		metrics.Count("test.metric", 1, labels)
	}

	counts := metrics.Counts()
	if len(counts) != 1 {
		t.Fatalf("one counter became %d: %v", len(counts), counts)
	}
	for _, v := range counts {
		if v != 100 {
			t.Errorf("counted %d, want 100", v)
		}
	}
}

func TestDurationsSummariseWithoutKeepingEverySample(t *testing.T) {
	metrics := NewMetrics()

	for _, seconds := range []float64{0.1, 0.5, 0.2} {
		metrics.Observe("test.duration", seconds, nil)
	}

	summary := metrics.Durations()["test.duration"]
	if summary.Count != 3 {
		t.Errorf("Count = %d, want 3", summary.Count)
	}
	if summary.Min != 0.1 {
		t.Errorf("Min = %v, want 0.1", summary.Min)
	}
	if summary.Max != 0.5 {
		t.Errorf("Max = %v, want 0.5", summary.Max)
	}
	if mean := summary.Mean(); mean < 0.26 || mean > 0.27 {
		t.Errorf("Mean = %v, want about 0.267", mean)
	}
	if (DurationSummary{}).Mean() != 0 {
		t.Error("the mean of nothing is not zero")
	}
}

func TestNothingIsMeasuredUntilAskedFor(t *testing.T) {
	// The default has to cost nothing, or every program pays for telemetry it
	// never reads.
	mq := brokerFor(t)

	if _, ok := mq.Observer().(NopObserver); !ok {
		t.Errorf("the default observer is %T, want NopObserver", mq.Observer())
	}
}

func TestAnObserverIsRequiredToBeSomething(t *testing.T) {
	_, err := Connect(context.Background(), "memory://"+t.Name(), WithObserver(nil))
	if err == nil {
		t.Fatal("a nil observer was accepted, and would panic on the first message")
	}
}

// ---- health ----------------------------------------------------------

func TestAWorkingConnectionIsUp(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	report := mq.Health(ctx)

	if report.Status != HealthUp {
		t.Errorf("Status = %s (%s)", report.Status, report.Detail)
	}
	if report.Checked.IsZero() {
		t.Error("the report does not say when it was checked")
	}
	if _, ok := report.Parts["roundTripMillis"]; !ok {
		t.Error("the report does not say how long the broker took")
	}
}

func TestAClosedConnectionIsDown(t *testing.T) {
	ctx := context.Background()
	mq, err := Connect(ctx, "memory://"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if err := mq.Close(); err != nil {
		t.Fatal(err)
	}

	report := mq.Health(ctx)

	if report.Status != HealthDown {
		t.Errorf("Status = %s, want down for a closed connection", report.Status)
	}
	if !strings.Contains(report.Detail, "closed") {
		t.Errorf("Detail = %q", report.Detail)
	}
}

func TestHealthCountsConsumers(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack { return Accept() })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	report := mq.Health(ctx)

	if report.Parts["consumers"] != 1 {
		t.Errorf("consumers = %v, want 1", report.Parts["consumers"])
	}
}

// TestOneThingBeingDownMakesTheWholeReportDown is the property a readiness
// probe depends on. A service that cannot reach its broker is not ready,
// however healthy the rest of it is.
func TestOneThingBeingDownMakesTheWholeReportDown(t *testing.T) {
	report := AggregateHealth(context.Background(),
		staticCheck{name: "database", status: HealthUp},
		staticCheck{name: "broker", status: HealthDown},
		staticCheck{name: "cache", status: HealthDegraded})

	if report.Status != HealthDown {
		t.Errorf("Status = %s, want down", report.Status)
	}
	if !strings.Contains(report.Detail, "broker") {
		t.Errorf("the report does not name what is wrong: %q", report.Detail)
	}
	if len(report.Parts) != 3 {
		t.Errorf("the report has %d parts, want all three", len(report.Parts))
	}
}

func TestDegradedIsNotDown(t *testing.T) {
	// Worth an alert, not worth taking the instance out of rotation — its
	// replacement would be degraded too.
	report := AggregateHealth(context.Background(),
		staticCheck{name: "database", status: HealthUp},
		staticCheck{name: "cache", status: HealthDegraded})

	if report.Status != HealthDegraded {
		t.Errorf("Status = %s, want degraded", report.Status)
	}
}

func TestEverythingUpIsUp(t *testing.T) {
	report := AggregateHealth(context.Background(),
		staticCheck{name: "database", status: HealthUp},
		staticCheck{name: "broker", status: HealthUp})

	if report.Status != HealthUp {
		t.Errorf("Status = %s, want up", report.Status)
	}
	if report.Detail != "" {
		t.Errorf("Detail = %q, want nothing to report", report.Detail)
	}
}

func TestChecksRunAtOnceRatherThanInTurn(t *testing.T) {
	// A slow check should not add its latency to every other one.
	checks := make([]HealthCheck, 5)
	for i := range checks {
		checks[i] = staticCheck{
			name: string(rune('a' + i)), status: HealthUp, delay: 150 * time.Millisecond}
	}

	started := time.Now()
	AggregateHealth(context.Background(), checks...)
	took := time.Since(started)

	if took > 500*time.Millisecond {
		t.Errorf("five 150ms checks took %s, so they ran in turn", took)
	}
}

func TestAnEmptyAggregateIsUp(t *testing.T) {
	if report := AggregateHealth(context.Background()); report.Status != HealthUp {
		t.Errorf("Status = %s for no checks at all", report.Status)
	}
}

func TestAConnectionCanBeAHealthCheck(t *testing.T) {
	mq := brokerFor(t)

	report := AggregateHealth(context.Background(), ConnHealth{Conn: mq, Label: "orders-broker"})

	if report.Status != HealthUp {
		t.Errorf("Status = %s", report.Status)
	}
	if _, ok := report.Parts["orders-broker"]; !ok {
		t.Errorf("the label was not used: %v", report.Parts)
	}
	if (ConnHealth{Conn: mq}).Name() != "broker" {
		t.Error("the default label is not broker")
	}
}

type staticCheck struct {
	name   string
	status HealthStatus
	delay  time.Duration
}

func (c staticCheck) Name() string { return c.name }

func (c staticCheck) Check(context.Context) HealthReport {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return HealthReport{Status: c.status, Checked: time.Now()}
}
