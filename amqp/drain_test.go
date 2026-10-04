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
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseStopsWaitingAtTheDrainBound(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var cancelled atomic.Bool

	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, _ Message[OrderPlaced]) Ack {
			close(started)
			select {
			case <-ctx.Done():
				cancelled.Store(true)
			case <-release:
			}
			<-release // ignores the cancellation, as a stuck handler does
			return Accept()
		}, DrainTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher[OrderPlaced](mq, "", "orders").Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	<-started

	began := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()

	select {
	case err = <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close was still waiting three seconds after a 200ms drain bound")
	}
	if took := time.Since(began); took > 200*time.Millisecond+drainGrace+300*time.Millisecond {
		t.Errorf("Close took %s with a 200ms bound", took)
	}

	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Close returned %v, want ErrDrainTimeout", err)
	}
	var timeout *DrainTimeoutError
	if !errors.As(err, &timeout) || timeout.Stranded != 1 || timeout.Queue != "orders" {
		t.Errorf("Close returned %#v, want one stranded handler on orders", timeout)
	}
	if !cancelled.Load() {
		t.Error("the handler's context was not cancelled at the bound")
	}
}

func TestClosingDoesNotRunWhatWasPrefetchedButNotStarted(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	pub := NewPublisher[OrderPlaced](mq, "", "orders")
	for range 5 {
		if err := pub.Send(ctx, OrderPlaced{OrderID: "o"}); err != nil {
			t.Fatal(err)
		}
	}

	var handled atomic.Int32
	started := make(chan struct{}, 5)
	release := make(chan struct{})

	sub, err := Consume(ctx, mq, "orders",
		func(_ context.Context, _ Message[OrderPlaced]) Ack {
			handled.Add(1)
			started <- struct{}{}
			<-release
			return Accept()
		}, Prefetch(5))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()
	time.Sleep(50 * time.Millisecond)
	close(release)

	if err := <-closed; err != nil {
		t.Fatalf("a drain that finished in time returned %v", err)
	}
	if n := handled.Load(); n != 1 {
		t.Errorf("%d handlers ran, want only the one already running", n)
	}
	if n, err := mq.MessageCount(ctx, "orders"); err != nil || n != 4 {
		t.Errorf("orders holds %d (%v), want the four that never started back on it", n, err)
	}
}

func TestAHandlerGivingUpAtTheDrainBoundStillDeadLetters(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	started := make(chan struct{})
	sub, err := Consume(ctx, mq, "orders",
		func(ctx context.Context, _ Message[OrderPlaced]) Ack {
			close(started)
			<-ctx.Done()
			return Reject(ctx.Err())
		}, DrainTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher[OrderPlaced](mq, "", "orders").Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	<-started

	if err := sub.Close(); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Close returned %v, want ErrDrainTimeout", err)
	}
	if n, err := mq.MessageCount(ctx, "orders.dlq"); err != nil || n != 1 {
		t.Errorf("orders.dlq holds %d (%v), want the message the handler gave up on", n, err)
	}
	if n, err := mq.MessageCount(ctx, "orders"); err != nil || n != 0 {
		t.Errorf("orders holds %d (%v), want it filed once, in the dead-letter queue", n, err)
	}
}
