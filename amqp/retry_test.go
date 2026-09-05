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
	"testing"
	"time"
)

func TestExponentialDelaysDoubleAndThenStop(t *testing.T) {
	p := ExponentialRetry(4, 100*time.Millisecond, time.Minute).WithJitter(0)

	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for attempt, w := range want {
		got, again := p.NextDelay(attempt+1, 0)
		if !again {
			t.Fatalf("attempt %d: gave up early", attempt+1)
		}
		if got != w {
			t.Errorf("attempt %d: delay = %s, want %s", attempt+1, got, w)
		}
	}

	// The fourth delivery is the last one the policy allows.
	if _, again := p.NextDelay(4, 0); again {
		t.Error("a fifth attempt was offered by a policy allowing four")
	}
}

func TestTheDelayIsCapped(t *testing.T) {
	p := ExponentialRetry(10, time.Second, 4*time.Second).WithJitter(0)

	for attempt := 1; attempt < 10; attempt++ {
		got, again := p.NextDelay(attempt, 0)
		if !again {
			t.Fatalf("attempt %d: gave up early", attempt)
		}
		if got > 4*time.Second {
			t.Errorf("attempt %d: delay = %s, which is past the cap", attempt, got)
		}
	}
}

func TestAnOldMessageIsAbandonedWhateverTheAttemptCount(t *testing.T) {
	// The bound that matters when a downstream service has been down for an
	// hour: the attempts may be left, but the message is no longer worth
	// delivering.
	p := ExponentialRetry(100, time.Second, time.Minute).GiveUpAfter(30 * time.Minute)

	if _, again := p.NextDelay(2, time.Minute); !again {
		t.Error("a young message was abandoned")
	}
	if _, again := p.NextDelay(2, time.Hour); again {
		t.Error("an hour-old message was still being retried with 98 attempts left")
	}
}

func TestNoRetryMeansOneDelivery(t *testing.T) {
	if _, again := NoRetry().NextDelay(1, 0); again {
		t.Error("NoRetry offered a second attempt")
	}
}

func TestFixedDelaysDoNotGrow(t *testing.T) {
	p := FixedRetry(4, 250*time.Millisecond).WithJitter(0)

	for attempt := 1; attempt < 4; attempt++ {
		got, again := p.NextDelay(attempt, 0)
		if !again {
			t.Fatalf("attempt %d: gave up early", attempt)
		}
		if got != 250*time.Millisecond {
			t.Errorf("attempt %d: delay = %s, want it unchanged", attempt, got)
		}
	}
}

func TestJitterSpreadsRetriesWithoutLeavingTheBand(t *testing.T) {
	// Messages that failed together must not all come back together, which is
	// the whole reason for jitter. What matters is that it varies and stays
	// within the factor.
	p := FixedRetry(2, time.Second).WithJitter(0.2)

	seen := map[time.Duration]bool{}
	for range 200 {
		got, again := p.NextDelay(1, 0)
		if !again {
			t.Fatal("gave up early")
		}
		if got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("delay = %s, outside the 20%% band around a second", got)
		}
		seen[got] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct delays in 200 draws; the jitter is not spreading anything", len(seen))
	}
}

func TestScheduleShowsWhatThePolicyWouldDo(t *testing.T) {
	got := ExponentialRetry(4, time.Second, 3*time.Second).Schedule()

	want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("Schedule() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Schedule()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestAnUnusablePolicyIsRefusedAtTheConnection(t *testing.T) {
	// Better to fail at start-up than to discover at the first failure that the
	// policy never retries anything.
	_, err := Connect(context.Background(), "memory://"+t.Name(),
		WithRetry(RetryPolicy{MaxAttempts: 0, Multiplier: 1}))

	if err == nil {
		t.Fatal("a policy allowing zero attempts was accepted")
	}
}
