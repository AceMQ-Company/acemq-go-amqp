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
	"sync"
	"testing"
	"time"
)

// TestTopicMatching pins the cases that a pattern translated into a regular
// expression gets wrong. That translation is the obvious implementation and it
// is where this went wrong in another language: a trailing # that has to match
// nothing at all, a # beside a *, and a pattern that is nothing but #.
func TestTopicMatching(t *testing.T) {
	cases := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"orders.created", "orders.created", true},
		{"orders.created", "orders.updated", false},
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.created.eu", false},
		{"orders.#", "orders.created", true},
		{"orders.#", "orders.created.eu", true},

		// A trailing # matches zero words, so this matches the bare word.
		{"orders.#", "orders", true},

		{"#", "", true},
		{"#", "orders.created.eu", true},
		{"#.eu", "orders.created.eu", true},
		{"#.eu", "eu", true},
		{"*.#", "orders", true},
		{"*.#", "orders.created.eu", true},
		{"#.*", "orders", true},
		{"orders.#.eu", "orders.eu", true},
		{"orders.#.eu", "orders.created.eu", true},
		{"orders.#.eu", "orders.created.paid.eu", true},
		{"orders.#.eu", "orders.created.us", false},
		{"*", "orders", true},
		{"*", "orders.created", false},
		{"*", "", false},
		{"orders.*.eu", "orders.created.eu", true},
		{"orders.*.eu", "orders.eu", false},
		{"", "", true},
		{"", "orders", false},
	}

	for _, c := range cases {
		if got := topicMatches(c.pattern, c.key); got != c.want {
			t.Errorf("topicMatches(%q, %q) = %v, want %v", c.pattern, c.key, got, c.want)
		}
	}
}

func TestATopicExchangeRoutesByPattern(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareExchange(ctx, "events", "topic"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"all-orders", "eu-only"} {
		declare(t, mq, q)
	}
	if err := mq.Bind(ctx, "all-orders", "events", "orders.#"); err != nil {
		t.Fatal(err)
	}
	if err := mq.Bind(ctx, "eu-only", "events", "orders.*.eu"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	arrived := map[string][]string{}
	watch := func(queue string) *Consumer {
		sub, err := Consume(ctx, mq, queue,
			func(_ context.Context, m Message[OrderPlaced]) Ack {
				mu.Lock()
				arrived[queue] = append(arrived[queue], m.RoutingKey)
				mu.Unlock()
				return Accept()
			})
		if err != nil {
			t.Fatal(err)
		}
		return sub
	}
	defer watch("all-orders").Close()
	defer watch("eu-only").Close()

	for _, key := range []string{"orders.created.eu", "orders.created.us"} {
		if err := NewPublisher[OrderPlaced](mq, "events", key).
			Send(ctx, OrderPlaced{OrderID: key}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "both messages on the catch-all queue", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(arrived["all-orders"]) == 2
	})
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(arrived["eu-only"]) != 1 || arrived["eu-only"][0] != "orders.created.eu" {
		t.Errorf("the eu-only queue got %v, want just orders.created.eu", arrived["eu-only"])
	}
}

func TestAFanoutExchangeReachesEveryBoundQueue(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)

	if err := mq.DeclareExchange(ctx, "broadcast", "fanout"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"first", "second"} {
		declare(t, mq, q)
		// A fanout ignores the routing key, and binding with an empty one is how
		// that is usually written.
		if err := mq.Bind(ctx, q, "broadcast", ""); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	counts := map[string]int{}
	for _, q := range []string{"first", "second"} {
		queue := q
		sub, err := Consume(ctx, mq, queue,
			func(_ context.Context, m Message[OrderPlaced]) Ack {
				mu.Lock()
				counts[queue]++
				mu.Unlock()
				return Accept()
			})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
	}

	if err := NewPublisher[OrderPlaced](mq, "broadcast", "anything").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "both queues", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return counts["first"] == 1 && counts["second"] == 1
	})
}

func TestBindingToAnUndeclaredExchangeSaysSo(t *testing.T) {
	ctx := context.Background()
	mq := brokerFor(t)
	declare(t, mq, "orders")

	err := mq.Bind(ctx, "orders", "never-declared", "#")

	if err == nil {
		t.Fatal("binding to an exchange that does not exist succeeded")
	}
}
