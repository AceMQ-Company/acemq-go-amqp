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

// RetryPolicy decides whether a failed message gets another attempt, and how
// long to wait first.
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
}

// NoRetry is one attempt and no more.
func NoRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, Multiplier: 1}
}

// FixedRetry waits the same time before each attempt.
func FixedRetry(maxAttempts int, delay time.Duration) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  maxAttempts,
		InitialDelay: delay,
		Multiplier:   1,
		MaxDelay:     delay,
		JitterFactor: 0.2,
	}
}

// ExponentialRetry doubles the delay before each attempt, capped at maxDelay.
func ExponentialRetry(maxAttempts int, initialDelay, maxDelay time.Duration) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  maxAttempts,
		InitialDelay: initialDelay,
		Multiplier:   2,
		MaxDelay:     maxDelay,
		JitterFactor: 0.2,
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
	return nil
}

// NextDelay is how long to wait before the attempt after the given one, and
// whether there is to be one at all.
//
// attempt is the number of the delivery that just failed, counting from 1.
func (p RetryPolicy) NextDelay(attempt int, messageAge time.Duration) (time.Duration, bool) {
	if attempt >= p.MaxAttempts {
		return 0, false
	}
	if p.MaxMessageAge > 0 && messageAge >= p.MaxMessageAge {
		return 0, false
	}

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

	if p.JitterFactor > 0 && delay > 0 {
		factor := 1 + ((rand.Float64()*2 - 1) * p.JitterFactor)
		delay = time.Duration(float64(delay) * factor)
	}
	if delay < 0 {
		delay = 0
	}
	return delay, true
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
	return fmt.Sprintf("RetryPolicy[attempts=%d, initial=%s, x%v, max=%s, jitter=%v]",
		p.MaxAttempts, p.InitialDelay, p.Multiplier, p.MaxDelay, p.JitterFactor)
}
