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
	"strings"
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

// DefaultHealthTimeout bounds a health probe that has not been given a deadline
// of its own.
//
// Three seconds rather than the actuator's five, deliberately: a check that
// gives up first is a check that can describe what it found, where a check the
// aggregate gives up on is described by the aggregate as having said nothing.
const DefaultHealthTimeout = 3 * time.Second

// BlockedReporter is a transport that can say whether the broker has blocked
// this connection.
//
// RabbitMQ sends connection.blocked when it is low on memory or disk, and then
// stops reading that connection. This is the only way to tell that apart from a
// socket that has wedged, because every other way of asking needs an answer
// from a broker that is no longer listening.
//
// A transport that does not implement it is treated as never blocked, and a
// report says so as "not known" rather than as "no" — see [Conn.Blocked].
type BlockedReporter interface {
	// BlockedReason is the broker's reason for blocking this connection, or
	// empty when it is not blocked.
	BlockedReason() string
}

// Blocked reports whether the broker has blocked this connection.
//
// The second return is false when the transport cannot be asked at all — the
// in-memory transport, a hand-written one — and the first is then an assumption
// rather than an answer. A health report keeps the two apart: a part that reads
// blocked: null was never asked, and is more use in an incident than one
// reading false because nobody looked.
func (c *Conn) Blocked() (blocked, known bool) {
	reporter, ok := c.transport.(BlockedReporter)
	if !ok {
		return false, false
	}
	return reporter.BlockedReason() != "", true
}

// BlockedReason is the broker's reason for blocking this connection, empty when
// it is not blocked or the transport cannot be asked.
//
// Free, with no round trip, for a publisher that would rather shed load than
// hand a message to a broker that has stopped reading.
func (c *Conn) BlockedReason() string {
	reporter, ok := c.transport.(BlockedReporter)
	if !ok {
		return ""
	}
	return reporter.BlockedReason()
}

// blockedPrefix is what an alert rule matches on. The broker's own words follow
// it, and Java, .NET, Python and Ruby all say this same sentence.
const blockedPrefix = "the broker has blocked this connection; publishing is paused"

func blockedDetail(reason string) string {
	if reason == "" {
		return blockedPrefix
	}
	return blockedPrefix + ": " + reason
}

// Health reports whether this connection can reach its broker.
//
// The check is a declaration of a temporary queue, which is the cheapest thing
// AMQP offers that actually proves the connection works. A TCP connection that
// is open but wedged — the network black-holing, the broker paused — answers
// the same as a healthy one until something is asked of it.
//
// It costs a round trip, so it is not something to call on every request. Wire
// it to a readiness probe and let the probe's interval decide how often.
//
// # A blocked connection is not probed, and is reported up
//
// The round trip is skipped entirely while the broker has blocked this
// connection, and the report says up with the broker's reason. Both halves of
// that matter.
//
// It is skipped because a blocked connection is one the broker has stopped
// reading: the declaration does not fail, it goes unanswered, and the probe
// spends its whole deadline finding out what the connection already knew. The
// broker telling this socket that it blocked it is livelier proof that the
// broker is there than any declaration could be.
//
// It is up rather than down because a blocked connection is the broker
// protecting itself from a memory or disk alarm, and an application that fails
// its own readiness check for it is one an orchestrator restarts into the same
// blocked broker, having thrown away whatever it was holding. A fleet doing
// that together stops draining the queues at the moment the broker most needs
// them drained. Java, .NET, Python and Ruby all report it the same way.
//
// # It has a deadline
//
// The probe is bounded by ctx, and by [DefaultHealthTimeout] when ctx carries no
// deadline of its own — the interface below says a check must not hang, and this
// one could: the RabbitMQ transport's declaration is a synchronous AMQP round
// trip that no context reaches.
//
// A probe that runs out of time is *abandoned* rather than waited for, because
// cancelling a request to a broker that is not reading means waiting for a
// cancellation that travels the same way the request did.
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

	blocked, known := c.Blocked()
	if !known {
		// Not false: the question was never asked, and a report that says so is
		// worth more in an incident than one that guesses.
		report.Parts["blocked"] = nil
	} else {
		report.Parts["blocked"] = blocked
	}
	if blocked {
		return c.blockedReport(report)
	}

	ctx, cancel := healthContext(ctx)
	defer cancel()

	// One queue per connection rather than one per check: exclusive, so it goes
	// when the connection does, and named once so that a readiness probe running
	// every few seconds reuses it instead of leaving a queue behind on the
	// broker for every check it ever ran.
	//
	// Classic, necessarily: RabbitMQ allows a quorum queue to be neither
	// exclusive nor auto-deleting, so a probe that took the durable default
	// would be refused by the broker and report every healthy broker as down.
	// The spec is built here rather than through [Conn.DeclareQueue] and its
	// options, so the default does not reach it in the first place.
	started := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- c.transport.DeclareQueue(ctx, c.probe, QueueSpec{
			Durable:    false,
			AutoDelete: true,
			Exclusive:  true,
		})
	}()

	select {
	case err := <-done:
		report.Parts["roundTripMillis"] = time.Since(started).Milliseconds()
		if err != nil {
			// A block that arrived while the probe was in flight is the
			// explanation for what happened to it, not a second fault beside it.
			if c.BlockedReason() != "" {
				report.Parts["blocked"] = true
				return c.blockedReport(report)
			}
			report.Status = HealthDown
			report.Detail = fmt.Sprintf("the broker did not answer: %v", err)
			return report
		}
		report.Status = HealthUp
		return report

	case <-ctx.Done():
		// No roundTripMillis. Nothing was timed: the number would be this
		// check's own deadline, which says nothing about the broker.
		if c.BlockedReason() != "" {
			report.Parts["blocked"] = true
			return c.blockedReport(report)
		}
		report.Status = HealthDown
		report.Detail = fmt.Sprintf(
			"the broker did not answer within %s", time.Since(started).Round(time.Millisecond))
		return report
	}
}

// blockedReport fills in the rest of a report for a connection the broker has
// blocked, and answers up.
func (c *Conn) blockedReport(report HealthReport) HealthReport {
	reason := c.BlockedReason()
	report.Parts["blockedReason"] = reason
	report.Status = HealthUp
	report.Detail = blockedDetail(reason)
	return report
}

// healthContext bounds a probe that arrived without a deadline of its own.
func healthContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, DefaultHealthTimeout)
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
//
// It passes the connection's own answer through and adds no opinion of its own.
// That is deliberate: [AggregateHealth] takes the worst report, so a built-in
// check with a cruder reading than the connection's would quietly overrule a
// careful one composed beside it, and there would be no way out from under it.
type ConnHealth struct {
	Conn  *Conn
	Label string

	// Timeout bounds this check, [DefaultHealthTimeout] when it is zero. Shorter
	// than the aggregate's own deadline, so a broker that has gone quiet is
	// described by the check that looked rather than by the aggregate giving up
	// on it.
	Timeout time.Duration
}

// Name is the label, or "broker".
func (c ConnHealth) Name() string {
	if c.Label != "" {
		return c.Label
	}
	return "broker"
}

// Check asks the connection, within [ConnHealth.Timeout].
func (c ConnHealth) Check(ctx context.Context) HealthReport {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultHealthTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.Conn.Health(ctx)
}

// AggregateHealth runs every check and combines the answers.
//
// The combined status is the worst of them: one thing being down makes the
// whole report down, because a service that cannot reach its broker is not
// ready however healthy the rest of it is.
//
// Checks run at once rather than in turn, so a slow one does not add its
// latency to the others, and the whole aggregate is bounded by ctx: a check
// that hangs is reported as down by name rather than hanging the readiness
// endpoint with it. The interface still says a check must not hang, because a
// check named in a report as having failed to answer is a worse answer than one
// that answered.
//
// The detail names every part that had something to say and repeats what it
// said, not only the ones that were not up. Summarising by status alone answered
// up with an empty detail for an aggregate holding a blocked connection, which
// threw away — at exactly the line an operator reads first — the one fact the
// check went to the trouble of finding.
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

	// Named up front so that a check which never answers is still a named part
	// of the report rather than a gap in it.
	answered := map[string]bool{}

	worst := HealthUp
	var reasons []string
	note := func(name string, r HealthReport) {
		report.Parts[name] = r
		switch r.Status {
		case HealthDown:
			worst = HealthDown
		case HealthDegraded:
			if worst != HealthDown {
				worst = HealthDegraded
			}
		}
		if r.Status != HealthUp || r.Detail != "" {
			reasons = append(reasons, strings.TrimSuffix(name+": "+r.Detail, ": "))
		}
	}

	waiting := time.Now()
	for range checks {
		select {
		case r := <-results:
			answered[r.name] = true
			note(r.name, r.report)
		case <-ctx.Done():
			// Whatever has already arrived is kept; the rest are named as
			// having not answered, which is what an operator needs to know and
			// what a bare timeout on the endpoint would have hidden.
			waited := time.Since(waiting).Round(time.Millisecond)
			for _, check := range checks {
				name := check.Name()
				if answered[name] {
					continue
				}
				note(name, HealthReport{
					Status:  HealthDown,
					Detail:  fmt.Sprintf("the check did not answer within %s", waited),
					Checked: time.Now().UTC(),
				})
			}
			report.Status = worst
			report.Detail = strings.Join(reasons, "; ")
			return report
		}
	}

	report.Status = worst
	report.Detail = strings.Join(reasons, "; ")
	return report
}
