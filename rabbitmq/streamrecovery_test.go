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

package rabbitmq

// A stream reader across a reconnection.
//
// A queue forgets what it delivered, so consuming it again after a recovery is
// consuming whatever is left. A stream forgets nothing: consuming it again
// starts wherever x-stream-offset says, and the subscription used to say the
// same thing the second time as the first. A reader that started at "first"
// read the whole stream again -- every entry handed to the handler twice, which
// for a projection is every balance doubled -- and one that started at "next"
// skipped everything published while it was away. Found by apps/03-ledger in
// the examples repository, whose statement projection is exactly such a reader.
//
// Recovery is played by hand, as in recovery_test.go, so the test needs only the
// ordinary broker.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

func TestAStreamReaderCarriesOnAfterARecoveryRatherThanStartingAgain(t *testing.T) {
	for _, start := range []string{"first", "next"} {
		t.Run("from "+start, func(t *testing.T) {
			url := os.Getenv("ACEMQ_TEST_AMQP_URL")
			if url == "" {
				t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
			}
			ctx := context.Background()

			reader, err := Dial(ctx, url, Config{WithoutRecovery: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			// A second connection publishes, so it can keep publishing while the
			// reader's is down.
			writer, err := Dial(ctx, url, Config{WithoutRecovery: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = writer.Close() }()

			stream := fmt.Sprintf("acemq-stream-across-recovery-%s-%d", start, time.Now().UnixNano())
			if err := writer.DeclareQueue(ctx, stream, acemq.QueueSpec{
				Durable: true, Args: map[string]any{"x-queue-type": "stream"},
			}); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = writer.DeleteQueue(context.Background(), stream) }()

			next := 0
			publish := func(n int) {
				t.Helper()
				for i := 0; i < n; i++ {
					if _, err := writer.Publish(ctx, "", stream, acemq.Outbound{
						Body: []byte("entry"), MessageID: fmt.Sprint(next), Persistent: true,
					}); err != nil {
						t.Fatal(err)
					}
					next++
				}
			}

			var (
				mu   sync.Mutex
				seen = map[string]int{}
			)
			received := func() (distinct, total int) {
				mu.Lock()
				defer mu.Unlock()
				for _, times := range seen {
					total += times
				}
				return len(seen), total
			}
			waitFor := func(want int) {
				t.Helper()
				deadline := time.Now().Add(15 * time.Second)
				for {
					if distinct, _ := received(); distinct >= want {
						return
					}
					if time.Now().After(deadline) {
						distinct, total := received()
						t.Fatalf("received %d distinct entries (%d deliveries); expected %d", distinct, total, want)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			if start == "first" {
				publish(30) // history, more than one prefetch's worth
			}
			sub, err := reader.Consume(ctx, stream, acemq.ConsumeSpec{
				Prefetch: 10, Args: map[string]any{"x-stream-offset": start},
			}, func(d acemq.Delivery) {
				mu.Lock()
				seen[d.MessageID]++
				mu.Unlock()
				_ = d.Ack()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			if start == "next" {
				publish(30)
			}
			waitFor(30)

			// The reader's connection goes, entries are appended while it is gone,
			// and it comes back.
			_ = reader.connection().Close()
			publish(5)
			if _, err := reader.reconnectOnce(); err != nil {
				t.Fatal(err)
			}
			publish(5)

			waitFor(40)
			time.Sleep(500 * time.Millisecond) // anything doubled arrives after the original
			distinct, total := received()
			if distinct != 40 || total != 40 {
				t.Errorf("after one reconnection the reader received %d distinct entries in %d deliveries;"+
					" expected each of the 40 exactly once", distinct, total)
			}
		})
	}
}
