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

	if got := countFor(metrics, MetricPublishTotal); got != 3 {
		t.Errorf("%s = %d, want 3", MetricPublishTotal, got)
	}
	if got := countFor(metrics, MetricConsumeTotal); got != 3 {
		t.Errorf("%s = %d, want 3", MetricConsumeTotal, got)
	}
	acked := metrics.Counts()[metricKey(MetricConsumeTotal,
		map[string]string{TagQueue: "orders", TagOutcome: OutcomeAcked})]
	if acked != 3 {
		t.Errorf("%s{outcome=%s} = %d, want 3", MetricConsumeTotal, OutcomeAcked, acked)
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

	// Two attempts, and only the first of them is a retry. The handler asked
	// for a retry both times; the second ask had no attempt left to spend, and
	// counting what was asked for rather than what was done is what used to
	// make this two. A retry that never happens is not a retry.
	if got := countFor(metrics, MetricRetriedTotal); got != 1 {
		t.Errorf("%s = %d, want 1: only the first of two attempts was retried",
			MetricRetriedTotal, got)
	}
	// The point of the metric: a message that ran out of attempts is gone, and
	// that should be visible without reading a log.
	if got := countFor(metrics, MetricDeadLetteredTotal); got != 1 {
		t.Errorf("%s = %d, want 1", MetricDeadLetteredTotal, got)
	}
	// Every delivery is counted once, whatever became of it, so the outcome tag
	// on this one partitions the deliveries rather than overlapping them.
	if got := countFor(metrics, MetricConsumeTotal); got != 2 {
		t.Errorf("%s = %d, want 2", MetricConsumeTotal, got)
	}
}

// TestTheOutcomeTagIsTheEnginesDecisionNotTheHandlersRequest pins the tag on the
// consume counter, which is the half of the agreement this package can see. The
// other half — that the span for the same delivery carries the same word — is
// TestTheCounterAndTheSpanAgreeOnEveryOutcome in telemetry/otel.
func TestTheOutcomeTagIsTheEnginesDecisionNotTheHandlersRequest(t *testing.T) {
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

	counts := metrics.Counts()
	retried := counts[metricKey(MetricConsumeTotal,
		map[string]string{TagQueue: "orders", TagOutcome: OutcomeRetried})]
	deadLettered := counts[metricKey(MetricConsumeTotal,
		map[string]string{TagQueue: "orders", TagOutcome: OutcomeDeadLettered})]

	if retried != 1 {
		t.Errorf("%s tagged %s = %d, want 1", MetricConsumeTotal, OutcomeRetried, retried)
	}
	if deadLettered != 1 {
		t.Errorf("%s tagged %s = %d, want 1: the handler asked for a retry it could not have",
			MetricConsumeTotal, OutcomeDeadLettered, deadLettered)
	}
}

// TestEveryDeliveryIsCountedUnderExactlyOneOutcome is what makes the outcome tag
// a partition rather than a set of overlapping labels. A delivery counted twice,
// or under a label that splits it, makes every dashboard built on it wrong in a
// way nothing reports.
//
// It also pins the relationship between acemq.consume.total and the two counters
// that stand beside it: acemq.messages.retried.total is the same events as
// acemq.consume.total{outcome=retried}, a second view rather than an addition.
// Adding the two would double every retry, which is exactly the shape of bug the
// old per-outcome counters invited.
func TestEveryDeliveryIsCountedUnderExactlyOneOutcome(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := brokerFor(t, WithObserver(metrics), WithRetry(FixedRetry(3, 0)))
	declare(t, mq, "orders")

	seen := make(chan string, 20)
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, m Message[OrderPlaced]) Ack {
			seen <- m.Payload.OrderID
			switch m.Payload.OrderID {
			case "accept":
				return Accept()
			case "reject":
				return Reject(errors.New("no"))
			default:
				return Retry(errors.New("again"))
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	for _, id := range []string{"accept", "reject", "retry"} {
		if err := pub.Send(ctx, OrderPlaced{OrderID: id}); err != nil {
			t.Fatal(err)
		}
	}
	// Three messages, and the retrying one is delivered three times.
	for range 5 {
		select {
		case <-seen:
		case <-time.After(3 * time.Second):
			t.Fatal("the messages did not all arrive")
		}
	}
	time.Sleep(200 * time.Millisecond)

	counts := metrics.Counts()
	consumed := countFor(metrics, MetricConsumeTotal)
	if consumed != 5 {
		t.Errorf("%s = %d, want 5: three messages, one of them delivered three times",
			MetricConsumeTotal, consumed)
	}

	// Every series of the counter has to carry an outcome from the vocabulary,
	// and adding them back up has to give the total again. A series tagged with
	// something not on this list is a delivery a dashboard grouping by outcome
	// would lose.
	known := map[string]bool{
		OutcomeAcked: true, OutcomeRetried: true, OutcomeRejected: true,
		OutcomeDeadLettered: true, OutcomeParked: true,
	}
	var partitioned int64
	for outcome := range known {
		partitioned += counts[metricKey(MetricConsumeTotal,
			map[string]string{TagQueue: "orders", TagOutcome: outcome})]
	}
	if partitioned != consumed {
		t.Errorf("the known outcomes add to %d but %s is %d, so a delivery was tagged"+
			" with something outside the vocabulary", partitioned, MetricConsumeTotal, consumed)
	}

	// The standalone counters are the same events seen again, not extra ones.
	retriedTagged := counts[metricKey(MetricConsumeTotal,
		map[string]string{TagQueue: "orders", TagOutcome: OutcomeRetried})]
	if got := countFor(metrics, MetricRetriedTotal); got != retriedTagged {
		t.Errorf("%s = %d but %s{outcome=%s} = %d; they are one set of events and"+
			" have to agree", MetricRetriedTotal, got, MetricConsumeTotal,
			OutcomeRetried, retriedTagged)
	}
	// Both outcomes that set a message aside land on this counter, tagged
	// apart -- Java's rule, so that "how much is this queue giving up on" is
	// one number in every language rather than one number in some of them.
	setAsideTagged := counts[metricKey(MetricConsumeTotal,
		map[string]string{TagQueue: "orders", TagOutcome: OutcomeDeadLettered})] +
		counts[metricKey(MetricConsumeTotal,
			map[string]string{TagQueue: "orders", TagOutcome: OutcomeParked})]
	if got := countFor(metrics, MetricDeadLetteredTotal); got != setAsideTagged {
		t.Errorf("%s = %d but %s{outcome=%s} plus {outcome=%s} = %d",
			MetricDeadLetteredTotal, got, MetricConsumeTotal,
			OutcomeDeadLettered, OutcomeParked, setAsideTagged)
	}
}

// TestAParkedMessageCountsAsSetAside pins the half of the rule a
// dead-lettering alone cannot: parking increments the same counter, tagged
// parked. Java routes both through MicrometerTelemetry so an operator gets one
// number; a Go service that counted only dead letters would under-report every
// message nothing could read.
func TestAParkedMessageCountsAsSetAside(t *testing.T) {
	metrics := NewMetrics()
	observeConsume(metrics, "orders", OutcomeParked)

	if got := countFor(metrics, MetricDeadLetteredTotal); got != 1 {
		t.Errorf("%s = %d, want 1 -- a parked message is a message set aside",
			MetricDeadLetteredTotal, got)
	}
	tagged := metrics.Counts()[metricKey(MetricDeadLetteredTotal,
		map[string]string{TagQueue: "orders", TagOutcome: OutcomeParked})]
	if tagged != 1 {
		t.Errorf("%s{outcome=%s} = %d, want 1; the two reasons have to stay"+
			" separable", MetricDeadLetteredTotal, OutcomeParked, tagged)
	}
}

// TestAnUnroutablePublishIsCountedApartFromAFailedOne is the reason the publish
// counter carries an outcome tag rather than being split into published and
// failed. A mandatory message the broker handed back is not a broken publisher:
// nothing went wrong, nothing was listening — and a single failure counter
// cannot say which of those two happened, which is the question somebody
// actually has when the number moves.
func TestAnUnroutablePublishIsCountedApartFromAFailedOne(t *testing.T) {
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

	counts := metrics.Counts()
	labels := map[string]string{TagExchange: "events", TagRoutingKey: "nothing.listens"}

	labels[TagOutcome] = OutcomeUnroutable
	if got := counts[metricKey(MetricPublishTotal, labels)]; got != 1 {
		t.Errorf("%s{outcome=%s} = %d, want 1", MetricPublishTotal, OutcomeUnroutable, got)
	}
	labels[TagOutcome] = OutcomeFailed
	if got := counts[metricKey(MetricPublishTotal, labels)]; got != 0 {
		t.Errorf("%s{outcome=%s} = %d, want 0: nothing failed, nothing was bound",
			MetricPublishTotal, OutcomeFailed, got)
	}

	// The publish was still timed. A publish that got as far as the broker and
	// came back has a duration, and leaving it out would make the timing series
	// silently exclude the slow unroutable case.
	if len(metrics.Durations()) == 0 {
		t.Errorf("%s recorded nothing for an unroutable publish", MetricPublishDuration)
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
