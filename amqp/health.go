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
	"fmt"
	"time"
)

// HealthStatus is how a check turned out.
type HealthStatus string

const (
	// HealthUp means working.
	HealthUp HealthStatus = "up"

	// HealthDown means not working. A readiness probe should fail on this.
	HealthDown HealthStatus = "down"

	// HealthDegraded means working, but not as well as it should. Worth an
	// alert; not worth taking the instance out of rotation, because the
	// replacement will almost certainly be degraded too.
	HealthDegraded HealthStatus = "degraded"
)

// HealthReport is what a check found.
type HealthReport struct {
	Status  HealthStatus   `json:"status"`
	Detail  string         `json:"detail,omitempty"`
	Checked time.Time      `json:"checked"`
	Parts   map[string]any `json:"parts,omitempty"`
}

func (r HealthReport) String() string {
	if r.Detail == "" {
		return string(r.Status)
	}
	return string(r.Status) + ": " + r.Detail
}

// Health reports whether this connection can reach its broker.
//
// The check is a declaration of a temporary queue, which is the cheapest thing
// AMQP offers that actually proves the connection works. A TCP connection that
// is open but wedged — the broker paused, the network black-holing — answers
// the same as a healthy one until something is asked of it.
//
// It costs a round trip, so it is not something to call on every request. Wire
// it to a readiness probe and let the probe's interval decide how often.
func (c *Conn) Health(ctx context.Context) HealthReport {
	report := HealthReport{Checked: time.Now().UTC(), Parts: map[string]any{}}

	c.mu.Lock()
	closed := c.closed
	consumers := len(c.subs)
	c.mu.Unlock()

	report.Parts["consumers"] = consumers

	if closed {
		report.Status = HealthDown
		report.Detail = "the connection has been closed"
		return report
	}

	// A queue named for this moment, exclusive and auto-deleting, so the check
	// leaves nothing behind and cannot collide with another instance running
	// the same check.
	probe := "acemq-health-" + newID()
	started := time.Now()

	err := c.transport.DeclareQueue(ctx, probe, QueueSpec{
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
	})
	report.Parts["roundTripMillis"] = time.Since(started).Milliseconds()

	if err != nil {
		report.Status = HealthDown
		report.Detail = fmt.Sprintf("the broker did not answer: %v", err)
		return report
	}

	report.Status = HealthUp
	return report
}

// HealthCheck is anything that can report on itself.
//
// The library provides one for the connection; an application adds its own —
// a database, a downstream service — and [AggregateHealth] combines them.
type HealthCheck interface {
	// Name identifies this check in the combined report.
	Name() string

	// Check reports the current state. It should return quickly and must not
	// panic; a check that hangs makes a readiness probe hang with it.
	Check(ctx context.Context) HealthReport
}

// ConnHealth adapts a connection to [HealthCheck].
type ConnHealth struct {
	Conn  *Conn
	Label string
}

// Name is the label, or "broker".
func (c ConnHealth) Name() string {
	if c.Label != "" {
		return c.Label
	}
	return "broker"
}

// Check asks the connection.
func (c ConnHealth) Check(ctx context.Context) HealthReport { return c.Conn.Health(ctx) }

// AggregateHealth runs every check and combines the answers.
//
// The combined status is the worst of them: one thing being down makes the
// whole report down, because a service that cannot reach its broker is not
// ready however healthy the rest of it is.
//
// Checks run at once rather than in turn, so a slow one does not add its
// latency to the others. A check that ignores its context can still hang the
// whole report, which is why the interface says not to.
func AggregateHealth(ctx context.Context, checks ...HealthCheck) HealthReport {
	report := HealthReport{
		Status:  HealthUp,
		Checked: time.Now().UTC(),
		Parts:   map[string]any{},
	}
	if len(checks) == 0 {
		return report
	}

	type result struct {
		name   string
		report HealthReport
	}
	results := make(chan result, len(checks))

	for _, check := range checks {
		go func(c HealthCheck) {
			results <- result{name: c.Name(), report: c.Check(ctx)}
		}(check)
	}

	worst := HealthUp
	var reasons []string
	for range checks {
		r := <-results
		report.Parts[r.name] = r.report
		switch r.report.Status {
		case HealthDown:
			worst = HealthDown
			reasons = append(reasons, r.name)
		case HealthDegraded:
			if worst != HealthDown {
				worst = HealthDegraded
			}
			reasons = append(reasons, r.name)
		}
	}

	report.Status = worst
	if len(reasons) > 0 {
		report.Detail = fmt.Sprintf("%v", reasons)
	}
	return report
}
