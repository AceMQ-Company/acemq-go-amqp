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
)

// drainFixture puts five messages on a fresh queue and starts a consumer with
// prefetch 5 whose handler blocks until released, so all five are on this
// consumer's channel and exactly one is running when Close is called.
func drainFixture(
	t *testing.T, opts ...acemq.ConsumeOption,
) (mq *acemq.Conn, queue string, sub *acemq.Consumer, handled *atomic.Int32, release chan struct{}) {
	t.Helper()
	ctx := context.Background()
	mq = connect(t)
	queue = queueName(t)
	if err := mq.DeclareQueue(ctx, queue); err != nil {
		t.Fatal(err)
	}
	removeAtEnd(t, []string{queue}, nil)

	pub := acemq.NewPublisher[OrderPlaced](mq, "", queue)
	for range 5 {
		if err := pub.Send(ctx, OrderPlaced{OrderID: "o"}); err != nil {
			t.Fatal(err)
		}
	}

	handled = &atomic.Int32{}
	started := make(chan struct{}, 5)
	release = make(chan struct{})
	sub, err := acemq.Consume(ctx, mq, queue,
		func(_ context.Context, _ acemq.Message[OrderPlaced]) acemq.Ack {
			handled.Add(1)
			started <- struct{}{}
			<-release
			return acemq.Accept()
		}, append([]acemq.ConsumeOption{acemq.Prefetch(5)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("no message arrived")
	}
	// Give the broker time to push the other four into the prefetch window.
	time.Sleep(300 * time.Millisecond)
	return mq, queue, sub, handled, release
}

func TestClosingAgainstARealBrokerRequeuesWhatNeverStarted(t *testing.T) {
	mq, queue, sub, handled, release := drainFixture(t)

	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()
	time.Sleep(100 * time.Millisecond)
	close(release)

	if err := <-closed; err != nil {
		t.Fatalf("a drain that finished in time returned %v", err)
	}
	if n := handled.Load(); n != 1 {
		t.Errorf("%d handlers ran during shutdown, want only the one already running", n)
	}
	waitFor(t, "the four prefetched messages to be back on the queue", func() bool {
		n, err := mq.MessageCount(context.Background(), queue)
		return err == nil && n == 4
	})
}

func TestClosingAgainstARealBrokerStopsAtTheDrainBound(t *testing.T) {
	mq, queue, sub, _, release := drainFixture(t, acemq.DrainTimeout(300*time.Millisecond))
	defer close(release)

	began := time.Now()
	err := sub.Close()
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("Close took %s with a 300ms bound", took)
	}

	var timeout *acemq.DrainTimeoutError
	if !errors.Is(err, acemq.ErrDrainTimeout) || !errors.As(err, &timeout) || timeout.Stranded != 1 {
		t.Fatalf("Close returned %v, want a drain timeout with one stranded handler", err)
	}
	// The stranded handler's message is unacknowledged on a channel that has
	// closed, so the broker has it back: all five, none lost.
	waitFor(t, "all five messages to be back on the queue", func() bool {
		n, err := mq.MessageCount(context.Background(), queue)
		return err == nil && n == 5
	})
}
