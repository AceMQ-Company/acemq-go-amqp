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

package rabbitmq_test

// What the library publishes on the caller's behalf, against a real broker.
//
// The in-memory transport reports routing honestly, so the patterns tests
// cover the logic. These cover the part only RabbitMQ can: that a basic.return
// for a mandatory raw publish reaches PublishResult.Routed, and that a replay,
// a routing-slip hop and a scheduled delivery into nowhere leave the message
// where it was rather than acknowledging the last copy.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

func onBehalfBroker(t *testing.T, opts ...acemq.ConnOption) *acemq.Conn {
	t.Helper()
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}
	mq, err := acemq.Connect(context.Background(), url, opts...)
	if strings.Contains(t.Name(), withoutConfirms) {
		var transport *rabbitmq.Transport
		transport, err = rabbitmq.Dial(context.Background(), url, rabbitmq.Config{WithoutConfirms: true})
		if err == nil {
			mq, err = acemq.NewConn(transport, opts...)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

// withoutConfirms in a test's name makes onBehalfBroker dial without
// publisher confirms.
const withoutConfirms = "/without-confirms/"

// TestOnBehalfHopsWithoutConfirmsAgainstARealBroker runs the on-behalf tests on
// a connection dialled WithoutConfirms. The caller's publishes are unconfirmed
// there; the library's own hops are confirmed on a channel of their own,
// because without a confirm a basic.return can never be ruled out and each of
// them settles its input on that answer.
func TestOnBehalfHopsWithoutConfirmsAgainstARealBroker(t *testing.T) {
	for name, test := range map[string]func(*testing.T){
		"replay":         TestReplayIntoNowhereKeepsTheMessageAgainstARealBroker,
		"slip":           TestASlipHopIntoNowhereIsSetAsideAgainstARealBroker,
		"scheduler":      TestAScheduledDeliveryIntoNowhereIsKeptAgainstARealBroker,
		"outbox":         TestAnOutboxRecordNothingIsBoundToIsKeptUntilSomethingIs,
		"pipeline":       TestAPipelineHopIntoNowhereIsSetAsideAgainstARealBroker,
		"retry-rung":     TestARetryToAMissingRungIsKeptAgainstARealBroker,
		"set-aside":      TestASetAsideToAMissingDeadLetterQueueIsKeptAgainstARealBroker,
		"scheduler-rung": TestAScheduledHopToAMissingRungIsKeptAgainstARealBroker,
	} {
		t.Run(strings.TrimPrefix(withoutConfirms, "/")+name, test)
	}
}

func TestWithoutConfirmsTheCallersPublisherIsStillUnconfirmed(t *testing.T) {
	url := brokerURL(t)
	transport, err := rabbitmq.Dial(context.Background(), url, rabbitmq.Config{WithoutConfirms: true})
	if err != nil {
		t.Fatal(err)
	}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatal(err)
	}
	defer mq.Close()

	// Mandatory, into nowhere: a confirmed publish would come back unroutable.
	result, err := acemq.NewPublisher[OrderPlaced](mq, "", queueName(t)+"-nowhere",
		acemq.Mandatory[OrderPlaced]()).SendResult(context.Background(), OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("an unconfirmed publish failed: %v", err)
	}
	if result.Confirmed || !result.Routed {
		t.Errorf("got %+v; the caller's own publish should be unconfirmed, with nothing known", result)
	}
	if mq.Supports(acemq.CapabilityPublisherConfirms) {
		t.Error("the connection claims publisher confirms it was dialled without")
	}
}

func TestAPipelineHopIntoNowhereIsSetAsideAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := onBehalfBroker(t, acemq.WithRetry(acemq.NoRetry()))

	name := fmt.Sprintf("acemq-pipe-%d", time.Now().UnixNano())
	pipeline, err := patterns.NewPipeline[OrderPlaced](ctx, mq, name,
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, bool, error) {
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("dispatch",
			func(_ context.Context, _ acemq.Message[OrderPlaced]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()
	for _, q := range []string{name + ".validate", name + ".dispatch"} {
		for _, each := range []string{q, acemq.DeadLetterQueue(q), acemq.ParkedQueue(q)} {
			defer func() { _ = mq.DeleteQueue(context.Background(), each) }()
		}
	}

	if err := mq.DeleteQueue(ctx, name+".dispatch"); err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	eventuallyHolds(t, mq, acemq.DeadLetterQueue(name+".validate"), 1)
}

func TestARetryToAMissingRungIsKeptAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	metrics := acemq.NewMetrics()
	policy := acemq.FixedRetry(2, 50*time.Millisecond).WaitInBrokerFrom(10 * time.Millisecond)
	mq := onBehalfBroker(t, acemq.WithRetry(policy), acemq.WithObserver(metrics))

	queue := fmt.Sprintf("acemq-rung-%d", time.Now().UnixNano())
	if err := mq.DeclareQueue(ctx, queue, acemq.OfType(acemq.QueueClassic)); err != nil {
		t.Fatal(err)
	}
	rung, _ := acemq.LadderFor(queue, policy).RungFor(50 * time.Millisecond)
	for _, q := range []string{queue, rung, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		defer func() { _ = mq.DeleteQueue(context.Background(), q) }()
	}

	var calls atomic.Int32
	sub, err := acemq.Consume(ctx, mq, queue, func(_ context.Context, _ acemq.Message[OrderPlaced]) acemq.Ack {
		calls.Add(1)
		return acemq.Retry(errors.New("still broken"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := mq.DeleteQueue(ctx, rung); err != nil {
		t.Fatal(err)
	}
	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the retry whose rung is gone to run again", func() bool { return calls.Load() >= 2 })
}

func TestASetAsideToAMissingDeadLetterQueueIsKeptAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := onBehalfBroker(t, acemq.WithRetry(acemq.NoRetry()))

	queue := fmt.Sprintf("acemq-set-aside-%d", time.Now().UnixNano())
	if err := mq.DeclareQueue(ctx, queue, acemq.OfType(acemq.QueueClassic)); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		defer func() { _ = mq.DeleteQueue(context.Background(), q) }()
	}

	var calls atomic.Int32
	sub, err := acemq.Consume(ctx, mq, queue, func(_ context.Context, _ acemq.Message[OrderPlaced]) acemq.Ack {
		calls.Add(1)
		return acemq.Reject(errors.New("bad order"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := mq.DeleteQueue(ctx, acemq.DeadLetterQueue(queue)); err != nil {
		t.Fatal(err)
	}
	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the rejected message to come back", func() bool { return calls.Load() >= 2 })
}

func TestAScheduledHopToAMissingRungIsKeptAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := onBehalfBroker(t, acemq.WithRetry(acemq.NoRetry()))

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	// The next NewScheduler declares the rung again.
	rung := patterns.ScheduleRungName(10 * time.Minute)
	if err := mq.DeleteQueue(ctx, rung); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.In(ctx, 45*time.Minute, "", "nowhere", OrderPlaced{OrderID: "o-1"}); err == nil {
		t.Error("scheduling onto a rung that is gone succeeded")
	}
	if scheduler.Hops() != 0 {
		t.Errorf("hops %d, want 0: the rung was not there", scheduler.Hops())
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func eventuallyHolds(t *testing.T, mq *acemq.Conn, queue string, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var got int64
	var err error
	for time.Now().Before(deadline) {
		if got, err = mq.MessageCount(context.Background(), queue); err == nil && got == n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s holds %d messages (error %v); expected %d", queue, got, err, n)
}

func TestAMandatoryRawPublishIntoNowhereIsNotRoutedAgainstARealBroker(t *testing.T) {
	mq := onBehalfBroker(t)
	result, err := mq.PublishRaw(context.Background(), "",
		fmt.Sprintf("acemq-nowhere-%d", time.Now().UnixNano()),
		acemq.Outbound{Body: []byte("x"), MessageID: "raw-1", Mandatory: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Routed || !result.Confirmed || result.ReturnReason == "" {
		t.Errorf("an unroutable mandatory publish came back %+v; expected confirmed, not routed, with a reason",
			result)
	}
}

func TestReplayIntoNowhereKeepsTheMessageAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := onBehalfBroker(t)

	dead := fmt.Sprintf("acemq-replay-dead-%d", time.Now().UnixNano())
	if err := mq.DeclareQueue(ctx, dead, acemq.OfType(acemq.QueueClassic)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mq.DeleteQueue(context.Background(), dead) }()
	if err := acemq.NewPublisher[OrderPlaced](mq, "", dead).Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{Queue: dead, RoutingKey: dead + "-nowhere"})
	var failed *acemq.PublishFailedError
	if !errors.As(err, &failed) || !failed.Unroutable || result.Moved != 0 {
		t.Errorf("replaying into nowhere moved %d with error %v; expected 0 and an unroutable error",
			result.Moved, err)
	}
	eventuallyHolds(t, mq, dead, 1)
}

func TestASlipHopIntoNowhereIsSetAsideAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := onBehalfBroker(t, acemq.WithRetry(acemq.NoRetry()))

	queue := fmt.Sprintf("acemq-slip-hop-%d", time.Now().UnixNano())
	if err := mq.DeclareQueue(ctx, queue, acemq.OfType(acemq.QueueClassic)); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{queue, acemq.DeadLetterQueue(queue), acemq.ParkedQueue(queue)} {
		defer func() { _ = mq.DeleteQueue(context.Background(), q) }()
	}
	sub, err := acemq.Consume(ctx, mq, queue, patterns.FollowSlip(mq,
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
			return m.Payload, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	slip := patterns.NewRoutingSlip().
		Then("", queue, queue).
		Then("", queue+"-next", queue+"-next")
	if err := patterns.Start(ctx, mq, slip, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	eventuallyHolds(t, mq, acemq.DeadLetterQueue(queue), 1)
}

func TestAScheduledDeliveryIntoNowhereIsKeptAgainstARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := onBehalfBroker(t, acemq.WithRetry(acemq.NoRetry()))

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	// Due straight away is an error to the caller rather than a silence.
	nowhere := fmt.Sprintf("acemq-scheduled-nowhere-%d", time.Now().UnixNano())
	err = scheduler.In(ctx, 0, "", nowhere, OrderPlaced{OrderID: "o-1"})
	if err == nil || !strings.Contains(err.Error(), "reached no queue") {
		t.Errorf("scheduling into nowhere returned %v; expected it to say it reached no queue", err)
	}
	if scheduler.Delivered() != 0 {
		t.Errorf("delivered %d into nowhere", scheduler.Delivered())
	}
}
