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
	"sync/atomic"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// releaseFails is an idempotency store whose Release never reaches it: the
// claim a failed handler held stays in the store until its lease runs out.
type releaseFails struct {
	*patterns.InMemoryIdempotencyStore
}

func (releaseFails) Release(context.Context, string) error {
	return errors.New("the store is not answering")
}

// TestAMessageWhoseClaimOutlivedAFailedHandlerIsHandledOnceLater is the loss
// the in-progress rule closes, on a real broker. The first handler dies with its
// claim held and cannot release it. The redelivery finds the claim live and
// unconfirmed; it used to be acknowledged as a duplicate, and the message was
// gone with the work never done. Now it is put back without spending an attempt
// — the policy allows only two, so spending them would dead-letter it — and
// once the lease runs out it is handled, exactly once.
func TestAMessageWhoseClaimOutlivedAFailedHandlerIsHandledOnceLater(t *testing.T) {
	ctx := context.Background()
	mq := connect(t, acemq.WithRetry(acemq.FixedRetry(2, 0)))
	queue := queueName(t)
	removeAtEnd(t, []string{queue}, nil)
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}

	memory := patterns.NewInMemoryIdempotencyStore(time.Hour)
	memory.SetClaimTimeout(time.Second)
	store := releaseFails{memory}

	var calls, done atomic.Int32
	sub, err := acemq.Consume(ctx, mq, queue, patterns.Idempotent(store,
		func(context.Context, acemq.Message[OrderPlaced]) acemq.Ack {
			if calls.Add(1) == 1 {
				return acemq.Retry(errors.New("the handler died mid-work"))
			}
			done.Add(1)
			return acemq.Accept()
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "", queue).
		Send(ctx, OrderPlaced{OrderID: "o-1"}, acemq.MessageID("m-1")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the message to be handled after its claim expired", func() bool {
		return done.Load() == 1
	})
	time.Sleep(500 * time.Millisecond)

	if n := done.Load(); n != 1 {
		t.Errorf("the work was done %d times, want exactly once", n)
	}
	for _, q := range []string{acemq.DeadLetterQueue(queue), queue} {
		if n, err := mq.MessageCount(ctx, q); err != nil || n != 0 {
			t.Errorf("%s holds %d messages (%v), want none", q, n, err)
		}
	}
}
