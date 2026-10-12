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

import (
	"context"
	"errors"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// TestAnUnreadableReplyIsReturnedPromptlyOnARealBroker: the responder answers
// with a body the requester's codec cannot decode, and Do says so at once
// rather than waiting out its timeout while the reply sits in a parked queue.
func TestAnUnreadableReplyIsReturnedPromptlyOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)

	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}
	responder, err := patterns.Serve(ctx, mq, queue,
		func(_ context.Context, _ acemq.Message[OrderPlaced]) (string, error) {
			return "not an order", nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer responder.Close()

	requester, err := patterns.NewRequester[OrderPlaced, OrderPlaced](ctx, mq, "", queue,
		patterns.Timeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer requester.Close()
	removeAtEnd(t, []string{requester.ReplyQueue()}, nil)

	started := time.Now()
	_, err = requester.Do(ctx, OrderPlaced{OrderID: "o-1"})
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

// TestARequesterLeavesNothingOnARealBroker: a generated reply queue goes when
// its requester closes, and nothing declared on its behalf may stay behind.
func TestARequesterLeavesNothingOnARealBroker(t *testing.T) {
	ctx := context.Background()
	mq := connect(t)

	requester, err := patterns.NewRequester[OrderPlaced, OrderPlaced](ctx, mq, "", queueName(t))
	if err != nil {
		t.Fatal(err)
	}
	reply := requester.ReplyQueue()
	// Cleans up after a failing run, so the leak does not accumulate here too.
	removeAtEnd(t, []string{reply}, nil)
	if err := requester.Close(); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{reply, reply + ".dlq", reply + ".parked"} {
		there, err := mq.QueueExists(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if there {
			t.Errorf("%s is still on the broker after the requester closed", q)
		}
	}
}
