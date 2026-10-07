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

package patterns_test

// Messages the library publishes on the caller's behalf, and then settles.
//
// Each of these published without mandatory and then accepted, acknowledged or
// counted the work as done. A destination nothing was bound to was dropped by
// the broker without a word, and the one copy that was left -- the input, the
// dead letter, the scheduled message -- went with it. Each test publishes into
// nowhere and asserts the message is still somewhere.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// withoutConfirms in a test's name makes brokerFor's broker one without
// publisher confirms.
const withoutConfirms = "/without-confirms/"

// TestOnBehalfHopsWithoutConfirms runs every test above on a connection
// without publisher confirms. The caller's own publishes are unconfirmed there,
// but the library's hops are confirmed regardless: without a confirm a return
// cannot be ruled out, and each of these settles its input on the answer.
func TestOnBehalfHopsWithoutConfirms(t *testing.T) {
	for name, test := range map[string]func(*testing.T){
		"slip":           TestASlipHopNothingIsBoundToIsNotAccepted,
		"pipeline":       TestADeclaredPipelineHopNothingIsBoundToIsNotAccepted,
		"replay":         TestReplayKeepsAMessageItHasNowhereToPut,
		"scheduler-due":  TestTheSchedulerKeepsADueMessageNothingIsBoundTo,
		"scheduler-rung": TestTheSchedulerKeepsAMessageWhoseRungIsGone,
		"scheduler-now":  TestSchedulingStraightIntoNowhereIsAnError,
		"outbox":         TestARecordStaysInTheOutboxWhenPublishingFails,
	} {
		t.Run(strings.TrimPrefix(withoutConfirms, "/")+name, test)
	}
}

// waitForCount waits for a queue to hold n messages.
func waitForCount(t *testing.T, mq *acemq.Conn, queue string, n int64) {
	t.Helper()
	waitFor(t, queue+" to hold the message", func() bool {
		got, err := mq.MessageCount(context.Background(), queue)
		return err == nil && got == n
	})
}

// setAsideFor pulls the one message on a dead-letter queue and checks it says
// it reached no queue.
func setAsideFor(t *testing.T, mq *acemq.Conn, queue string) {
	t.Helper()
	waitForCount(t, mq, queue, 1)
	kept, ok, err := mq.Pull(context.Background(), queue)
	if err != nil || !ok {
		t.Fatalf("nothing on %s: %v", queue, err)
	}
	defer func() { _ = kept.Ack() }()
	if !strings.Contains(kept.Envelope.Error, "reached no queue") {
		t.Errorf("%s holds the message with reason %q; expected it to say it reached no queue",
			queue, kept.Envelope.Error)
	}
}

func TestASlipHopNothingIsBoundToIsNotAccepted(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, acemq.WithRetry(acemq.NoRetry()))

	// "charge" is never declared: the next stop has nothing bound to it.
	// Consume declares validate.dlq, where the input has to end up.
	if err := mq.DeclareQueue(ctx, "validate"); err != nil {
		t.Fatal(err)
	}
	sub, err := acemq.Consume(ctx, mq, "validate", patterns.FollowSlip(mq,
		func(_ context.Context, m acemq.Message[OrderPlaced]) (OrderPlaced, error) {
			return m.Payload, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	slip := patterns.NewRoutingSlip().
		Then("", "validate", "validate").
		Then("", "charge", "charge")
	if err := patterns.Start(ctx, mq, slip, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	setAsideFor(t, mq, acemq.DeadLetterQueue("validate"))
}

func TestADeclaredPipelineHopNothingIsBoundToIsNotAccepted(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, acemq.WithRetry(acemq.NoRetry()))

	pipeline, err := patterns.NewPipeline[pipeOrder](ctx, mq, "lossy",
		patterns.PipelineStep("validate",
			func(_ context.Context, m acemq.Message[pipeOrder]) (pipeOrder, bool, error) {
				return m.Payload, true, nil
			}),
		patterns.PipelineStep("dispatch",
			func(_ context.Context, m acemq.Message[pipeOrder]) (struct{}, bool, error) {
				return struct{}{}, true, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pipeline.Close() }()

	// The next step's queue goes away, as it does when that service's queue is
	// deleted and not yet redeclared.
	if err := mq.DeleteQueue(ctx, "lossy.dispatch"); err != nil {
		t.Fatal(err)
	}

	if _, err := pipeline.Send(ctx, pipeOrder{ID: "A-1"}); err != nil {
		t.Fatal(err)
	}

	setAsideFor(t, mq, acemq.DeadLetterQueue("lossy.validate"))
}

func TestReplayKeepsAMessageItHasNowhereToPut(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "orders-dead"); err != nil {
		t.Fatal(err)
	}
	if err := acemq.NewPublisher[OrderPlaced](mq, "", "orders-dead").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	// "orders" was never declared: the replay has nowhere to put the message.
	result, err := patterns.Replay(ctx, mq, patterns.ReplayFrom{
		Queue:      "orders-dead",
		RoutingKey: "orders",
	})
	var failed *acemq.PublishFailedError
	if !errors.As(err, &failed) || !failed.Unroutable {
		t.Errorf("replaying into nowhere returned %v; expected an unroutable PublishFailedError", err)
	}
	if result.Moved != 0 {
		t.Errorf("moved %d into nowhere; expected 0", result.Moved)
	}
	waitForCount(t, mq, "orders-dead", 1)
}

func TestTheSchedulerKeepsADueMessageNothingIsBoundTo(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, acemq.WithRetry(acemq.NoRetry()))

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	// As if a rung had just expired it, due, for a queue that does not exist.
	publishToControl(t, mq, "", "invoices", time.Now().Add(-time.Minute))

	setAsideFor(t, mq, acemq.DeadLetterQueue(patterns.ScheduleControlQueue))
	if scheduler.Delivered() != 0 {
		t.Errorf("delivered %d, want 0: it reached no queue", scheduler.Delivered())
	}
}

func TestTheSchedulerKeepsAMessageWhoseRungIsGone(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t, acemq.WithRetry(acemq.NoRetry()))

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	if err := mq.DeleteQueue(ctx, "acemq.schedule.10m"); err != nil {
		t.Fatal(err)
	}
	publishToControl(t, mq, "billing", "invoice.due", time.Now().Add(45*time.Minute))

	setAsideFor(t, mq, acemq.DeadLetterQueue(patterns.ScheduleControlQueue))
	if scheduler.Hops() != 0 {
		t.Errorf("hops %d, want 0: the rung was not there", scheduler.Hops())
	}
}

func TestSchedulingStraightIntoNowhereIsAnError(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	scheduler, err := patterns.NewScheduler(ctx, mq)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()

	err = scheduler.In(ctx, 0, "", "invoices", Invoice{Number: "INV-9"})
	var failed *acemq.PublishFailedError
	if !errors.As(err, &failed) || !failed.Unroutable {
		t.Errorf("scheduling for now into a queue that does not exist returned %v;"+
			" expected an unroutable PublishFailedError", err)
	}
}

// publishToControl puts a scheduled message on the control queue as a rung
// expiring it would.
func publishToControl(t *testing.T, mq *acemq.Conn, exchange, routingKey string, due time.Time) {
	t.Helper()
	env, err := acemq.NewEnvelope(patterns.ScheduledMessageType,
		acemq.Header(patterns.HeaderScheduleExchange, exchange),
		acemq.Header(patterns.HeaderScheduleRoutingKey, routingKey),
		acemq.Header(patterns.HeaderScheduleDueAt, due.UnixMilli()),
		acemq.Header(patterns.HeaderScheduleContentType, acemq.JSONContentType))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mq.PublishRaw(context.Background(), patterns.ScheduleExchange,
		patterns.ScheduleControlQueue, acemq.Outbound{
			Body:        []byte(`{"number":"INV-5","cents":100}`),
			ContentType: acemq.BytesContentType,
			MessageID:   env.ID,
			Headers:     env.ToWire(),
			Persistent:  true,
		}); err != nil {
		t.Fatal(err)
	}
}
