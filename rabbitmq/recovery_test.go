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

// Publishing while the connection underneath it is being replaced.
//
// The transport's recovery is the only thing that writes conn, channel and
// generation after Dial, and publishing is the only thing that reads them
// constantly. The one test that put the two together needed a broker it was
// allowed to restart, which meant the combination was exercised on CI and
// nowhere else — and that is where the race that prompted this file was found,
// on a restart no local run performs.
//
// This drives the recovery path by hand instead, from inside the package, so a
// reconnection lands in the middle of a publish without anything having to
// restart. It needs only the ordinary broker every other test here uses.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// TestPublishingSurvivesTheConnectionBeingReplacedUnderIt is the regression
// test for the race CI caught at rabbitmq.go's publish path.
//
// Two things are asserted, and the race detector asserts a third. Publishing
// keeps working across the swaps; a publish the reconnection interrupted is
// never reported as a message the broker refused, because the broker said
// nothing at all about it; and -race sees every read of the replaced fields.
//
// Recovery is off and this test plays the recovery goroutine, which is the
// honest way to make the swaps frequent: the loop calls exactly what a real
// recovery calls, and the old connection is closed afterwards the way a dropped
// one would be.
func TestPublishingSurvivesTheConnectionBeingReplacedUnderIt(t *testing.T) {
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}

	ctx := context.Background()
	transport, err := Dial(ctx, url, Config{WithoutRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.Close() }()

	// Durable, and deleted at the end. RabbitMQ 4 refuses a transient
	// non-exclusive queue outright, and an auto-delete one would be gone the
	// moment a swap left it without the connection that declared it.
	queue := fmt.Sprintf("acemq-publish-across-recovery-%d", time.Now().UnixNano())
	if err := transport.DeclareQueue(ctx, queue, acemq.QueueSpec{Durable: true}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.DeleteQueue(context.Background(), queue) }()

	var (
		mu       sync.Mutex
		sent     int
		refused  []string
		problems []string
	)

	stop := make(chan struct{})
	var publishers sync.WaitGroup
	for publisher := 0; publisher < 16; publisher++ {
		publishers.Add(1)
		go func(publisher int) {
			defer publishers.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}

				_, err := transport.Publish(ctx, "", queue, acemq.Outbound{
					Body:      []byte("across a reconnection"),
					MessageID: fmt.Sprintf("%d-%d", publisher, n),
					Mandatory: true,
				})

				mu.Lock()
				switch {
				case err == nil:
					sent++
				case strings.Contains(err.Error(), "the broker refused it"):
					// The broker refuses a message for reasons of its own, and
					// none of them apply to a transient queue that exists. A
					// nack here is the client answering for a channel that
					// died, which is a different fact and has to read as one.
					refused = append(refused, err.Error())
				default:
					problems = append(problems, err.Error())
				}
				mu.Unlock()
			}
		}(publisher)
	}

	// Each swap lands in the middle of whatever the publishers are doing. Ten
	// of them rather than one, because a race needs the detector to be looking
	// at the right moment: with one swap this caught the original defect in
	// roughly one run in three, and with ten it has not missed it yet.
	const swaps = 10
	for swap := 0; swap < swaps; swap++ {
		time.Sleep(30 * time.Millisecond)

		// Killed first and replaced second, which is the order a real outage
		// happens in: whatever was waiting for a confirm on that connection is
		// answered with a nack by the client, before any reconnection exists to
		// notice. Those publishes have to fail legibly, and in particular must
		// not be reported as messages the broker refused.
		previous := transport.connection()
		_ = previous.Close()

		if _, err := transport.reconnectOnce(); err != nil {
			close(stop)
			publishers.Wait()
			t.Fatalf("swap %d could not reconnect: %v", swap+1, err)
		}
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	publishers.Wait()

	mu.Lock()
	defer mu.Unlock()

	if sent == 0 {
		t.Errorf("nothing was published at all across %d reconnections: %v", swaps, problems)
	}
	if len(refused) > 0 {
		t.Errorf("%d publishes were reported as refused by the broker, which never spoke: %s",
			len(refused), refused[0])
	}
	if generation, want := transport.generation, uint64(swaps+1); generation != want {
		t.Errorf("the transport is on generation %d after %d reconnections, want %d",
			generation, swaps, want)
	}
}
