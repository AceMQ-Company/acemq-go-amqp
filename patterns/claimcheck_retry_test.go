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
	"sync/atomic"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// slowStore times out on its first Get, as an object store under load does.
type slowStore struct {
	*patterns.InMemoryClaimCheckStore
	gets atomic.Int32
}

func (s *slowStore) Get(key string) ([]byte, bool, error) {
	if s.gets.Add(1) == 1 {
		return nil, false, context.DeadlineExceeded
	}
	return s.InMemoryClaimCheckStore.Get(key)
}

// TestAClaimCheckStoreTimeoutIsRetriedNotParked: a store that timed out may
// answer next time, so the message goes round the retry policy. It used to be
// parked with every other decode error, as if nothing could ever read it.
func TestAClaimCheckStoreTimeoutIsRetriedNotParked(t *testing.T) {
	ctx := context.Background()
	store := &slowStore{InMemoryClaimCheckStore: patterns.NewInMemoryClaimCheckStore()}
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store, patterns.OffloadAbove(16))
	mq := brokerFor(t, acemq.WithCodec(codec), acemq.WithRetry(acemq.FixedRetry(3, 0)))
	if err := mq.DeclareQueue(ctx, "documents"); err != nil {
		t.Fatal(err)
	}

	arrived := make(chan acemq.Message[Document], 1)
	sub, err := acemq.Consume(ctx, mq, "documents",
		func(_ context.Context, m acemq.Message[Document]) acemq.Ack {
			arrived <- m
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[Document](mq, "", "documents").Send(ctx, aDocument(256)); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-arrived:
		if m.Envelope.Attempt != 2 {
			t.Errorf("handled on attempt %d, want 2: the timeout should have cost one retry", m.Envelope.Attempt)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the message was never handled")
	}
	if n, _ := mq.MessageCount(ctx, acemq.ParkedQueue("documents")); n != 0 {
		t.Errorf("%d messages parked; a store timeout is not an unreadable body", n)
	}
}

// TestAnUnreadableBodyIsStillParked is the other side: a body nothing can read
// reads no better next time, and must not go round the retry schedule.
func TestAnUnreadableBodyIsStillParked(t *testing.T) {
	ctx := context.Background()
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, patterns.NewInMemoryClaimCheckStore())
	mq := brokerFor(t, acemq.WithCodec(codec), acemq.WithRetry(acemq.FixedRetry(3, 0)))
	if err := mq.DeclareQueue(ctx, "documents"); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	sub, err := acemq.Consume(ctx, mq, "documents",
		func(context.Context, acemq.Message[Document]) acemq.Ack {
			calls.Add(1)
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if _, err := mq.PublishRaw(ctx, "", "documents", acemq.Outbound{
		Body: []byte("{not json"), ContentType: acemq.JSONContentType, MessageID: "m-1",
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := mq.MessageCount(ctx, acemq.ParkedQueue("documents")); n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if n, _ := mq.MessageCount(ctx, acemq.ParkedQueue("documents")); n != 1 {
		t.Fatalf("%d messages parked, want the unreadable one", n)
	}
	if calls.Load() != 0 {
		t.Error("the handler ran on a body nothing could decode")
	}
}

// A decode error marked retryable is the codec's way of saying so.
func TestAStoreFailureIsMarkedRetryable(t *testing.T) {
	store := &slowStore{InMemoryClaimCheckStore: patterns.NewInMemoryClaimCheckStore()}
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store, patterns.OffloadAbove(16))
	body, err := codec.Encode(aDocument(256))
	if err != nil {
		t.Fatal(err)
	}
	var doc Document
	err = codec.Decode(body, &doc)
	var retryable *acemq.RetryableError
	if !errors.As(err, &retryable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a store timeout decoded as %v, want a *RetryableError wrapping it", err)
	}
}
