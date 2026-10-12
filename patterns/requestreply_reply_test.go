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

import (
	"context"
	"errors"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// TestAReplyThatWillNotDecodeIsReturnedToTheCaller: a reply the requester's
// codec cannot read is the caller's error, returned at once, not a message
// parked by the reply consumer while Do waits out its timeout.
func TestAReplyThatWillNotDecodeIsReturnedToTheCaller(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareQueue(ctx, "prices"); err != nil {
		t.Fatal(err)
	}
	// A string where the requester expects a PriceResponse object.
	responder, err := patterns.Serve(ctx, mq, "prices",
		func(_ context.Context, _ acemq.Message[PriceRequest]) (string, error) {
			return "not a price", nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices",
		patterns.Timeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()

	started := time.Now()
	_, err = requester.Do(ctx, PriceRequest{SKU: "abc"})
	if err == nil {
		t.Fatal("an unreadable reply produced no error")
	}
	if errors.Is(err, patterns.ErrRequestTimedOut) {
		t.Fatalf("the caller waited out its timeout instead of hearing the decode error: %v", err)
	}
	if took := time.Since(started); took > 3*time.Second {
		t.Errorf("the decode error took %s to arrive", took)
	}
}

// TestARequesterLeavesNoDeadLetterQueuesBehind: a generated reply queue is
// exclusive and auto-deleting, so nothing durable may be declared alongside it.
// A .dlq and .parked per requester would outlive every requester ever started.
func TestARequesterLeavesNoDeadLetterQueuesBehind(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	requester, err := patterns.NewRequester[PriceRequest, PriceResponse](ctx, mq, "", "prices")
	if err != nil {
		t.Fatal(err)
	}
	replyQueue := requester.ReplyQueue()
	if err := requester.Close(); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{replyQueue + ".dlq", replyQueue + ".parked"} {
		there, err := mq.QueueExists(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if there {
			t.Errorf("%s was declared for a generated reply queue", q)
		}
	}
}
