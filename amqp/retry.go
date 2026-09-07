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
	"fmt"
	"math/rand"
	"time"
)

// DefaultBrokerWaitThreshold is the delay at which waiting moves from the
// consumer to the broker.
//
// Thirty seconds is roughly where the two costs cross. Below it, the seconds a
// restart loses are only seconds and a held prefetch slot is cheap; above it, a
// consumer that restarts mid-wait loses the wait entirely, because the broker
// redelivers the unacknowledged message at once and a five-minute backoff
// becomes instant.
const DefaultBrokerWaitThreshold = 30 * time.Second

// Wait is how long before the next attempt, and where the message spends it.
//
// Two answers rather than one because they cannot be worked out separately:
// jitter applies only to a wait spent in the consumer, so a caller given a delay
// alone could not tell whether it had already been moved — and a jittered delay
// does not name a rung queue.
type Wait struct {
	// Delay is how long the message waits.
	Delay time.Duration

	// InBroker is whether it waits on a rung queue rather than here.
	InBroker bool
}

// RetryPolicy decides whether a failed message gets another attempt, how long to
// wait first, and where the waiting happens.
//
// The arithmetic is part of the cross-language contract rather than a local
// choice: the same policy must produce the same delays in Java, Go, .NET, Python
// and Ruby, because the same message can be retried by a consumer written in any
// of them. Jitter is the exception — it is random by definition — so
// [RetryPolicy.Schedule] reports the delays without it, which is what to read
// when deciding whether a policy is the one you meant.
//
// MaxMessageAge bounds by the message's age rather than by attempts, which is
// the bound that matters when a downstream service has been down for an hour:
// the twelfth attempt on an hour-old message is rarely worth making.
type RetryPolicy struct {
	// MaxAttempts counts the first delivery, so 3 means one try and two retries.
	MaxAttempts int

	// InitialDelay is how long to wait before the second attempt.
	InitialDelay time.Duration

	// Multiplier grows the delay after each attempt. 1 keeps it fixed.
	Multiplier float64

	// MaxDelay caps the growth.
	MaxDelay time.Duration

	// MaxMessageAge gives up on a message older than this, whatever the attempt
	// count says. Zero means no age limit.
	MaxMessageAge time.Duration

	// JitterFactor spreads retries so a batch that failed together does not
	// return together. 0.2 varies each delay by up to a fifth either way.
	JitterFactor float64

	// BrokerWaitThreshold is the delay at or past which the message waits on a
	// rung queue rather than in the consumer. Zero turns rungs off entirely, so
	// every wait is spent here and no queue beyond the source is needed — which
	// is the right setting for a service that may not declare queues on its
	// broker, and the wrong one for a policy with delays measured in minutes.
	//
	// The constructors set it to [DefaultBrokerWaitThreshold]. A policy written
	// out as a struct literal gets Go's zero value and so gets no rungs, which is
	// the safe direction to be wrong in: a wait held here is slow, and a message
	// published into a rung that nothing declared is gone.
	BrokerWaitThreshold time.Duration
}

// NoRetry is one attempt and no more.
func NoRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, Multiplier: 1, BrokerWaitThreshold: DefaultBrokerWaitThreshold}
}

// FixedRetry waits the same time before each attempt, with no jitter.
//
// No jitter because a fixed policy is usually chosen for a schedule somebody
// wants to be able to predict, and because the Python and Ruby libraries make
// the same choice for the same policy: a delay that differed between languages
// would be a difference nobody could see in the code.
func FixedRetry(maxAttempts int, delay time.Duration) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:         maxAttempts,
		InitialDelay:        delay,
		Multiplier:          1,
		MaxDelay:            delay,
		BrokerWaitThreshold: DefaultBrokerWaitThreshold,
	}
}

// ExponentialRetry doubles the delay before each attempt, capped at maxDelay,
// with 20% jitter, which is the sane default.
func ExponentialRetry(maxAttempts int, initialDelay, maxDelay time.Duration) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:         maxAttempts,
		InitialDelay:        initialDelay,
		Multiplier:          2,
		MaxDelay:            maxDelay,
		JitterFactor:        0.2,
		BrokerWaitThreshold: DefaultBrokerWaitThreshold,
	}
}

// GiveUpAfter returns a copy that abandons a message older than age.
func (p RetryPolicy) GiveUpAfter(age time.Duration) RetryPolicy {
	p.MaxMessageAge = age
	return p
}

// WithJitter returns a copy using a different jitter factor, between 0 and 1.
func (p RetryPolicy) WithJitter(factor float64) RetryPolicy {
	p.JitterFactor = factor
	return p
}

// WaitInBrokerFrom returns a copy that moves the line between waiting here and
// waiting there.
//
// Zero is the way out: with no threshold nothing is long enough to reach the
// broker, so every wait is spent in the consumer and no rung queue is needed.
func (p RetryPolicy) WaitInBrokerFrom(threshold time.Duration) RetryPolicy {
	p.BrokerWaitThreshold = threshold
	return p
}

// WaitsInBroker reports whether a wait of this length belongs on a rung queue.
//
// The delay must be one [RetryPolicy.Schedule] reports — that is, before jitter.
// A jittered delay names no queue.
func (p RetryPolicy) WaitsInBroker(delay time.Duration) bool {
	return p.BrokerWaitThreshold > 0 && delay > 0 && delay >= p.BrokerWaitThreshold
}

// BrokerRungs are the delays this policy needs a rung queue for, shortest first.
//
// Exactly the entries of [RetryPolicy.Schedule] that reach the threshold, with
// repeats removed — a fixed policy that waits a minute three times needs one
// queue, not three. It is a finite list because the schedule is, which is what
// makes the queues declarable up front rather than conjured by a consumer at the
// moment it first fails.
func (p RetryPolicy) BrokerRungs() []time.Duration {
	var rungs []time.Duration
	seen := map[time.Duration]bool{}
	for _, delay := range p.Schedule() {
		if !p.WaitsInBroker(delay) || seen[delay] {
			continue
		}
		seen[delay] = true
		rungs = append(rungs, delay)
	}
	return rungs
}

// Validate reports whether the policy is usable.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return fmt.Errorf("acemq: RetryPolicy.MaxAttempts must be at least 1, got %d", p.MaxAttempts)
	}
	if p.Multiplier < 1 {
		return fmt.Errorf("acemq: RetryPolicy.Multiplier must be at least 1, got %v", p.Multiplier)
	}
	if p.JitterFactor < 0 || p.JitterFactor > 1 {
		return fmt.Errorf("acemq: RetryPolicy.JitterFactor must be between 0 and 1, got %v", p.JitterFactor)
	}
	if p.BrokerWaitThreshold < 0 {
		return fmt.Errorf(
			"acemq: RetryPolicy.BrokerWaitThreshold must not be negative, got %s; "+
				"zero is how a policy says every wait happens in the consumer",
			p.BrokerWaitThreshold)
	}
	return nil
}

// NextDelay is how long to wait before the attempt after the given one, and
// whether there is to be one at all.
//
// attempt is the number of the delivery that just failed, counting from 1.
//
// The delay of a wait that belongs in the broker comes back unjittered, because
// that is the delay that names a rung queue. [RetryPolicy.NextWait] is the whole
// answer and is what the consumer asks; this is the half of it a caller wants
// when it only needs a number.
func (p RetryPolicy) NextDelay(attempt int, messageAge time.Duration) (time.Duration, bool) {
	wait, again := p.NextWait(attempt, messageAge)
	return wait.Delay, again
}

// NextWait is how long to wait before the next attempt and where the message
// waits, or false when there should be no further attempt.
//
// attempt is the number of the delivery that just failed, counting from 1.
func (p RetryPolicy) NextWait(attempt int, messageAge time.Duration) (Wait, bool) {
	if attempt >= p.MaxAttempts {
		return Wait{}, false
	}
	if p.MaxMessageAge > 0 && messageAge >= p.MaxMessageAge {
		return Wait{}, false
	}

	delay := p.backoff(attempt)

	if p.WaitsInBroker(delay) {
		// Deliberately not jittered. A rung queue's time-to-live is fixed when it
		// is declared, so a moved delay would name a queue that does not exist;
		// and the spread jitter buys is already there, because each message's TTL
		// starts when it arrives rather than when the batch failed. A fleet that
		// failed over ten seconds is released over ten seconds.
		return Wait{Delay: delay, InBroker: true}, true
	}

	if p.JitterFactor > 0 && delay > 0 {
		// Both directions, so a fleet of consumers that failed together does not
		// come back together. One-sided jitter only ever delays, which turns a
		// thundering herd into a slower thundering herd.
		factor := 1 + ((rand.Float64()*2 - 1) * p.JitterFactor)
		delay = time.Duration(float64(delay) * factor)
	}
	if delay < 0 {
		delay = 0
	}
	return Wait{Delay: delay}, true
}

// backoff is the delay after an attempt, before jitter and before the threshold.
//
// Capped inside the loop as well as after it: without the first, a policy with a
// large multiplier and many attempts runs the delay towards overflow before the
// ceiling is ever applied.
func (p RetryPolicy) backoff(attempt int) time.Duration {
	delay := p.InitialDelay
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * p.Multiplier)
		if p.MaxDelay > 0 && delay > p.MaxDelay {
			delay = p.MaxDelay
			break
		}
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}

// Schedule is the un-jittered delays this policy would use, which is what to
// look at when deciding whether a policy is the one you meant.
func (p RetryPolicy) Schedule() []time.Duration {
	delays := make([]time.Duration, 0, max(0, p.MaxAttempts-1))
	delay := p.InitialDelay
	for attempt := 1; attempt < p.MaxAttempts; attempt++ {
		capped := delay
		if p.MaxDelay > 0 && capped > p.MaxDelay {
			capped = p.MaxDelay
		}
		delays = append(delays, capped)
		delay = time.Duration(float64(delay) * p.Multiplier)
	}
	return delays
}

// String makes a policy readable in a log line.
func (p RetryPolicy) String() string {
	return fmt.Sprintf("RetryPolicy[attempts=%d, initial=%s, x%v, max=%s, jitter=%v, broker=%s]",
		p.MaxAttempts, p.InitialDelay, p.Multiplier, p.MaxDelay, p.JitterFactor,
		p.BrokerWaitThreshold)
}
