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

// The tests below are the ones that have to agree with the other languages,
// value for value. Their counterparts are tests/test_retry.py in the Python
// library and spec/retry_policy_spec.rb in the Ruby one; a message retried by a
// Python consumer and then by a Go one must not wait different amounts for the
// same attempt.

func TestTheScheduleIsTheSameInEveryLanguage(t *testing.T) {
	// The same four numbers Python's
	// test_the_schedule_doubles_and_is_the_same_everywhere asserts.
	assertSchedule(t, ExponentialRetry(5, time.Second, time.Minute),
		time.Second, 2*time.Second, 4*time.Second, 8*time.Second)

	assertSchedule(t, ExponentialRetry(6, time.Second, 4*time.Second),
		time.Second, 2*time.Second, 4*time.Second, 4*time.Second, 4*time.Second)

	assertSchedule(t, FixedRetry(4, 30*time.Second),
		30*time.Second, 30*time.Second, 30*time.Second)

	assertSchedule(t, NoRetry())
}

func TestTheCeilingHoldsInsideTheLoopAsWellAsAfterIt(t *testing.T) {
	// Without the cap inside the loop a policy with a large multiplier and many
	// attempts runs the delay towards overflow before the ceiling is ever
	// applied, and comes out negative.
	p := RetryPolicy{
		MaxAttempts:  200,
		InitialDelay: time.Second,
		Multiplier:   10,
		MaxDelay:     time.Minute,
	}

	for attempt := 1; attempt < 200; attempt++ {
		delay, again := p.NextDelay(attempt, 0)
		if !again {
			t.Fatalf("attempt %d: gave up early", attempt)
		}
		if delay < 0 || delay > time.Minute {
			t.Fatalf("attempt %d: delay = %s, outside the ceiling", attempt, delay)
		}
	}
}

func TestAShortWaitIsSpentInTheConsumer(t *testing.T) {
	// Below the threshold the seconds a restart loses are only seconds, and a
	// held prefetch slot is cheaper than a queue nobody asked for.
	wait, again := FixedRetry(2, 5*time.Second).NextWait(1, 0)

	if !again {
		t.Fatal("gave up on the first of two attempts")
	}
	if wait.InBroker {
		t.Errorf("a five-second wait was sent to the broker")
	}
}

func TestALongWaitIsSpentInTheBroker(t *testing.T) {
	// A consumer sleeping on a five-minute backoff loses the whole wait when it
	// restarts: the broker redelivers the unacknowledged message at once, so a
	// five-minute policy delivers in none.
	wait, again := FixedRetry(2, 5*time.Minute).NextWait(1, 0)

	if !again {
		t.Fatal("gave up on the first of two attempts")
	}
	if !wait.InBroker {
		t.Error("a five-minute wait was held in the consumer")
	}
	if wait.Delay != 5*time.Minute {
		t.Errorf("Delay = %s, want the rung's own five minutes", wait.Delay)
	}
}

func TestTheThresholdIsReachedRatherThanPassed(t *testing.T) {
	atIt, _ := FixedRetry(2, DefaultBrokerWaitThreshold).NextWait(1, 0)
	belowIt, _ := FixedRetry(2, DefaultBrokerWaitThreshold-time.Second).NextWait(1, 0)

	if !atIt.InBroker {
		t.Errorf("a wait of exactly %s stayed in the consumer", DefaultBrokerWaitThreshold)
	}
	if belowIt.InBroker {
		t.Errorf("a wait of %s was sent to the broker", DefaultBrokerWaitThreshold-time.Second)
	}
}

func TestABrokerWaitIsNeverJittered(t *testing.T) {
	// A rung queue's time-to-live is fixed when it is declared, so a moved delay
	// would name a queue that is not there. The spread is free anyway: each
	// message's TTL starts when it arrives rather than when the batch failed.
	p := ExponentialRetry(2, time.Minute, 0)

	for range 200 {
		wait, again := p.NextWait(1, 0)
		if !again {
			t.Fatal("gave up early")
		}
		if !wait.InBroker || wait.Delay != time.Minute {
			t.Fatalf("a broker wait came back as %+v, want exactly a minute in the broker", wait)
		}
	}
}

func TestTheThresholdMovesAndZeroTurnsTheRungsOff(t *testing.T) {
	p := FixedRetry(2, 5*time.Second).WaitInBrokerFrom(time.Second)

	moved, _ := p.NextWait(1, 0)
	if !moved.InBroker {
		t.Error("a five-second wait stayed in the consumer under a one-second threshold")
	}

	// Zero is the way out, for a service that may not declare queues on its
	// broker: nothing is long enough to reach one.
	off, _ := p.WaitInBrokerFrom(0).NextWait(1, 0)
	if off.InBroker {
		t.Error("a threshold of zero still sent a wait to the broker")
	}
	if off.Delay != 5*time.Second {
		t.Errorf("Delay = %s, want the wait itself", off.Delay)
	}
	if len(p.WaitInBrokerFrom(0).BrokerRungs()) != 0 {
		t.Error("a policy with no threshold still wanted rung queues")
	}
}

func TestARetryPolicyWrittenOutByHandWaitsInTheConsumer(t *testing.T) {
	// Go's zero value rather than the default, and deliberately so: a wait held
	// here is slow, and a message published into a rung nothing declared is gone.
	p := RetryPolicy{MaxAttempts: 3, InitialDelay: time.Hour, Multiplier: 1}

	wait, _ := p.NextWait(1, 0)
	if wait.InBroker {
		t.Error("a policy written as a struct literal used rungs it never asked for")
	}
}

func TestTheRungsAreTheScheduleAboveTheThresholdWithoutRepeats(t *testing.T) {
	// Finite, because the schedule is. That is what lets the queues be declared
	// with the topology rather than conjured when a consumer first fails.
	p := ExponentialRetry(6, 10*time.Second, 0)

	assertSchedule(t, p, 10*time.Second, 20*time.Second, 40*time.Second,
		80*time.Second, 160*time.Second)
	assertDelays(t, "BrokerRungs", p.BrokerRungs(),
		40*time.Second, 80*time.Second, 160*time.Second)

	// A fixed policy that waits a minute three times needs one queue, not three.
	assertDelays(t, "BrokerRungs", FixedRetry(4, time.Minute).BrokerRungs(), time.Minute)
	assertDelays(t, "BrokerRungs", NoRetry().BrokerRungs())
}

func TestJitterMovesBothWaysAndStaysInsideTheFactor(t *testing.T) {
	// Both directions, and genuinely either side: jitter that only ever delays
	// turns a thundering herd into a slower thundering herd.
	p := ExponentialRetry(2, 10*time.Second, 0)

	shorter, longer := false, false
	for range 500 {
		delay, again := p.NextDelay(1, 0)
		if !again {
			t.Fatal("gave up early")
		}
		if delay < 8*time.Second || delay > 12*time.Second {
			t.Fatalf("delay = %s, outside 20%% either side of ten seconds", delay)
		}
		if delay < 10*time.Second {
			shorter = true
		}
		if delay > 10*time.Second {
			longer = true
		}
	}
	if !shorter || !longer {
		t.Errorf("jitter went short=%v, long=%v; it has to go both ways", shorter, longer)
	}
}

func TestADelayIsNeverNegative(t *testing.T) {
	reckless := RetryPolicy{MaxAttempts: 2, InitialDelay: time.Second, Multiplier: 1, JitterFactor: 1}

	for range 500 {
		delay, again := reckless.NextDelay(1, 0)
		if !again {
			t.Fatal("gave up early")
		}
		if delay < 0 {
			t.Fatalf("delay = %s", delay)
		}
	}
}

func assertSchedule(t *testing.T, p RetryPolicy, want ...time.Duration) {
	t.Helper()
	assertDelays(t, "Schedule", p.Schedule(), want...)
}

func assertDelays(t *testing.T, what string, got []time.Duration, want ...time.Duration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s() = %v, want %v", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s()[%d] = %s, want %s (all: %v)", what, i, got[i], want[i], got)
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
