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
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

func onBehalfBroker(t *testing.T, opts ...acemq.ConnOption) *acemq.Conn {
	t.Helper()
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}
	mq, err := acemq.Connect(context.Background(), url, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
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
