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

// A connection without publisher confirms still has to know where the
// library's own hops went. Without a confirm a return can never be ruled out,
// so a retry hop or a set-aside to a queue that has gone used to be taken as
// delivered, and the original acknowledged into nowhere. memory://?confirms=off
// reports exactly what the RabbitMQ transport does in that mode.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func unconfirmedBrokerFor(t *testing.T, opts ...ConnOption) *Conn {
	t.Helper()
	mq, err := Connect(context.Background(), "memory://"+t.Name()+"?confirms=off", opts...)
	if err != nil {
		t.Fatalf("cannot connect: %v", err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

func TestWithoutConfirmsARetryToAMissingRungIsKept(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	policy := FixedRetry(2, 50*time.Millisecond).WaitInBrokerFrom(10 * time.Millisecond)
	mq := unconfirmedBrokerFor(t, WithRetry(policy), WithObserver(metrics))
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, _ Message[OrderPlaced]) Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			return Retry(errors.New("still broken"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	rung, _ := LadderFor("orders", policy).RungFor(50 * time.Millisecond)
	if err := mq.DeleteQueue(ctx, rung); err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher[OrderPlaced](mq, "", "orders").Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the retry whose rung is gone", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	if countOf(metrics, MetricRungMissing) == 0 {
		t.Errorf("%s was never counted", MetricRungMissing)
	}
}

func TestWithoutConfirmsASetAsideToAMissingDeadLetterQueueIsKept(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	mq := unconfirmedBrokerFor(t, WithRetry(NoRetry()), WithObserver(metrics))
	declare(t, mq, "orders")

	var mu sync.Mutex
	calls := 0
	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, _ Message[OrderPlaced]) Ack {
			mu.Lock()
			calls++
			mu.Unlock()
			return Reject(errors.New("bad order"))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := mq.DeleteQueue(ctx, DeadLetterQueue("orders")); err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher[OrderPlaced](mq, "", "orders").Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the rejected message to come back", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	if countOf(metrics, MetricSetAsideFailed) == 0 {
		t.Errorf("%s was never counted", MetricSetAsideFailed)
	}
}

func TestWithoutConfirmsTheCallersOwnPublishIsStillUnconfirmed(t *testing.T) {
	// Only the library's hops are confirmed. A publisher of the caller's own,
	// mandatory or not, gets exactly what it asked for: nothing promised.
	mq := unconfirmedBrokerFor(t)
	result, err := NewPublisher[OrderPlaced](mq, "", "nowhere", Mandatory[OrderPlaced]()).
		SendResult(context.Background(), OrderPlaced{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("an unconfirmed publish into nowhere failed: %v", err)
	}
	if result.Confirmed || !result.Routed {
		t.Errorf("got %+v; expected unconfirmed and routed, as RabbitMQ reports it", result)
	}
}
