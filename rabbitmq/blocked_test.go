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

// Health against a connection the broker has really blocked.
//
// This file exists because a test that sets a flag and asserts on the flag
// proves the flag is readable and nothing else. Every library in this family
// modelled blocked state correctly and then had its health check hang on a
// blocked connection anyway, for years, because nothing ever pointed one at a
// broker under a real alarm. Go's was the worst of the three: with a five
// second context, Conn.Health had still not returned after twelve.
//
// A block is produced the only way RabbitMQ produces one — drop the memory high
// watermark until the alarm goes off, then publish, because the broker sends
// connection.blocked to a connection that publishes while an alarm is on and to
// no other. The watermark is restored before anything is closed: closing a
// blocked connection waits for a broker that has stopped reading.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// blockedWatermark is low enough that RabbitMQ raises the alarm at once on any
// machine, and restoreWatermark is RabbitMQ's own default.
const (
	blockedWatermark  = "0.0001"
	restoreWatermark  = "0.4"
	blockWaitLimit    = 45 * time.Second
	unblockWaitLimit  = 45 * time.Second
	recoverWaitLimit  = 60 * time.Second
	blockedAnswerFast = time.Second
)

// rabbitmqctl runs the broker's own CLI, reached through a command prefix.
//
// A prefix rather than a path, because the broker is usually in a container:
// ACEMQ_TEST_RABBITMQCTL=docker exec -e HOME=/var/lib/rabbitmq acemq-tls rabbitmqctl,
// or just rabbitmqctl for a broker on the host.
//
// It fails rather than skipping when the variable is unset against a broker that
// is there. A skipping test is exactly how this defect survived three libraries.
func rabbitmqctl(t *testing.T, args ...string) string {
	t.Helper()

	prefix := strings.Fields(os.Getenv("ACEMQ_TEST_RABBITMQCTL"))
	if len(prefix) == 0 {
		t.Fatal("ACEMQ_TEST_RABBITMQCTL is not set, so the blocked-connection tests " +
			"cannot reach the broker's CLI. Set it to a command prefix, such as " +
			"\"docker exec -e HOME=/var/lib/rabbitmq acemq-tls rabbitmqctl\". " +
			"These tests fail rather than skip: a health check that has never met a " +
			"blocked broker has never been tested.")
	}

	cmd := exec.Command(prefix[0], append(append([]string{}, prefix[1:]...), args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", strings.Join(prefix, " "), strings.Join(args, " "), err, out)
	}
	return string(out)
}

func setWatermark(t *testing.T, value string) {
	t.Helper()
	rabbitmqctl(t, "set_vm_memory_high_watermark", value)
}

// blockTheConnection puts the broker into a memory alarm and publishes until it
// blocks this connection.
//
// Each publish gets a deadline of its own: with the alarm on, the broker stops
// reading, so a publish waiting for a confirm waits for one that will not come.
func blockTheConnection(t *testing.T, transport *Transport, queue string) string {
	t.Helper()

	setWatermark(t, blockedWatermark)

	deadline := time.Now().Add(blockWaitLimit)
	for time.Now().Before(deadline) {
		if reason := transport.BlockedReason(); reason != "" {
			return reason
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		_, _ = transport.Publish(ctx, "", queue, acemq.Outbound{Body: []byte("pressure")})
		cancel()
		time.Sleep(200 * time.Millisecond)
	}

	setWatermark(t, restoreWatermark)
	t.Fatalf("the broker never blocked this connection within %s, so nothing below "+
		"tested what it claims to", blockWaitLimit)
	return ""
}

// unblockTheConnection puts the watermark back and waits for the broker to say
// so, which has to happen before anything is closed.
func unblockTheConnection(t *testing.T, transport *Transport) {
	t.Helper()

	setWatermark(t, restoreWatermark)

	deadline := time.Now().Add(unblockWaitLimit)
	for time.Now().Before(deadline) {
		if transport.BlockedReason() == "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the broker never unblocked this connection within %s", unblockWaitLimit)
}

func blockedTestQueue(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("acemq-blocked-%s-%d", strings.ToLower(t.Name()), time.Now().UnixNano())
}

// TestHealthOnAConnectionTheBrokerHasReallyBlocked is the regression test for
// the defect: a health check that probes a connection the broker has stopped
// reading does not fail, it hangs, and the readiness endpoint hangs with it.
func TestHealthOnAConnectionTheBrokerHasReallyBlocked(t *testing.T) {
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}

	ctx := context.Background()
	transport, err := Dial(ctx, url, Config{WithoutRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	mq, err := acemq.NewConn(transport)
	if err != nil {
		t.Fatal(err)
	}

	queue := blockedTestQueue(t)
	if err := transport.DeclareQueue(ctx, queue, acemq.QueueSpec{Durable: true}); err != nil {
		t.Fatal(err)
	}

	blocked := false
	defer func() {
		if blocked {
			unblockTheConnection(t, transport)
		}
		_ = transport.DeleteQueue(ctx, queue)
		_ = mq.Close()
	}()

	// Unblocked, for comparison: up, timed, and known not to be blocked.
	before := mq.Health(ctx)
	if before.Status != acemq.HealthUp {
		t.Fatalf("an unblocked connection is %s (%s)", before.Status, before.Detail)
	}
	if _, timed := before.Parts["roundTripMillis"]; !timed {
		t.Error("an unblocked check did not make the round trip it exists to make")
	}
	if before.Parts["blocked"] != false {
		t.Errorf("blocked = %v, want false before the alarm", before.Parts["blocked"])
	}

	reason := blockTheConnection(t, transport, queue)
	blocked = true
	t.Logf("the broker blocked this connection: %q", reason)

	// The measurement. Five seconds is the actuator's default, and the old code
	// had not returned after twelve.
	hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	answered := make(chan acemq.HealthReport, 1)
	started := time.Now()
	go func() { answered <- mq.Health(hctx) }()

	var report acemq.HealthReport
	select {
	case report = <-answered:
	case <-time.After(20 * time.Second):
		// Not t.Fatal from here: the watermark has to go back, and a Fatal in
		// this goroutine would still run the defer but leave the report unread.
		t.Error("Health never returned on a blocked connection, with a 5s deadline")
		unblockTheConnection(t, transport)
		blocked = false
		return
	}
	took := time.Since(started)
	t.Logf("Health on a blocked connection answered %s in %s", report.Status, took)

	if took > blockedAnswerFast {
		t.Errorf("Health took %s on a blocked connection; the broker had already said "+
			"it was blocked over that same socket, so nothing needed asking", took)
	}
	if report.Status != acemq.HealthUp {
		t.Errorf("Status = %s, want up: restarting into the same blocked broker helps "+
			"nobody and throws away what this instance was holding", report.Status)
	}
	if !strings.Contains(report.Detail, reason) {
		t.Errorf("Detail = %q, want the broker's own reason %q", report.Detail, reason)
	}
	if report.Parts["blocked"] != true {
		t.Errorf("blocked = %v, want true", report.Parts["blocked"])
	}
	if report.Parts["blockedReason"] != reason {
		t.Errorf("blockedReason = %v, want %q", report.Parts["blockedReason"], reason)
	}
	if _, timed := report.Parts["roundTripMillis"]; timed {
		t.Error("a round trip was timed on a connection the broker is not reading")
	}

	// The check holds nothing on the way out. The probe that used to hang took
	// the transport's mutex with it, so every declaration, every publish and the
	// reconnection itself queued behind a readiness check that was never going
	// to return; a second check answering at once is that mutex being free.
	//
	// A real declaration is not asserted here and could not be: a connection the
	// broker has stopped reading answers no round trip at all, whoever makes it.
	// That is the transport's nature rather than a defect in the check, and it is
	// the whole reason the check stopped making one.
	second := make(chan acemq.HealthReport, 1)
	go func() { second <- mq.Health(ctx) }()
	select {
	case r := <-second:
		if r.Status != acemq.HealthUp {
			t.Errorf("the second check is %s (%s)", r.Status, r.Detail)
		}
	case <-time.After(5 * time.Second):
		t.Error("a second health check did not answer, so the first is still holding " +
			"the transport while it waits on a broker that is not reading")
	}

	// And a declaration whose deadline has already gone is refused rather than
	// started: there is no abandoning an AMQP round trip once its frame is out.
	spent, cancelSpent := context.WithCancel(ctx)
	cancelSpent()
	refused := make(chan error, 1)
	go func() {
		refused <- transport.DeclareQueue(spent, queue+"-after", acemq.QueueSpec{Durable: true})
	}()
	select {
	case err := <-refused:
		if err == nil {
			t.Error("a declaration with a spent context was made anyway")
		}
	case <-time.After(5 * time.Second):
		t.Error("a declaration with a spent context went to the broker regardless")
	}

	// The aggregate keeps the reason. Summarising by status alone answers up
	// with an empty detail, which throws the one useful fact away at exactly the
	// line an operator reads first.
	actx, acancel := context.WithTimeout(ctx, 5*time.Second)
	aggregate := acemq.AggregateHealth(actx, acemq.ConnHealth{Conn: mq})
	acancel()

	if aggregate.Status != acemq.HealthUp {
		t.Errorf("the aggregate is %s (%s)", aggregate.Status, aggregate.Detail)
	}
	if !strings.Contains(aggregate.Detail, reason) {
		t.Errorf("the aggregate's detail is %q; the reason did not survive it", aggregate.Detail)
	}
	if !strings.Contains(aggregate.Detail, "broker") {
		t.Errorf("the aggregate's detail is %q; it does not name the part", aggregate.Detail)
	}

	unblockTheConnection(t, transport)
	blocked = false

	after := mq.Health(ctx)
	if after.Status != acemq.HealthUp {
		t.Errorf("Status = %s after the alarm cleared (%s)", after.Status, after.Detail)
	}
	if after.Parts["blocked"] != false {
		t.Errorf("blocked = %v after the alarm cleared", after.Parts["blocked"])
	}
	if _, timed := after.Parts["roundTripMillis"]; !timed {
		t.Error("the round trip did not come back when the connection did")
	}

	// One probe queue for the whole connection, however many checks ran. A queue
	// per check is a queue per check left on the broker until the connection
	// goes, and a readiness probe runs for the life of the process.
	if n := probeQueues(t); n != 1 {
		t.Errorf("%d health probe queues are on the broker after four checks, want 1", n)
	}
}

// probeQueues counts the queues Conn.Health leaves on the broker.
func probeQueues(t *testing.T) int {
	t.Helper()

	n := 0
	for _, line := range strings.Split(rabbitmqctl(t, "list_queues", "name"), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "acemq-health-") {
			n++
		}
	}
	return n
}

// TestABlockedConnectionDoesNotStayBlockedAfterARecovery is the 0.6.0 fix,
// asserted against a real block rather than against the unit test's flag.
//
// The blocked state belongs to the connection that carried it. A reconnection
// that left it set meant a connection that had been blocked once refused every
// publish for the rest of the process's life, on a broker that was fine.
func TestABlockedConnectionDoesNotStayBlockedAfterARecovery(t *testing.T) {
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}

	ctx := context.Background()
	transport, err := Dial(ctx, url)
	if err != nil {
		t.Fatal(err)
	}

	queue := blockedTestQueue(t)
	if err := transport.DeclareQueue(ctx, queue, acemq.QueueSpec{Durable: true}); err != nil {
		t.Fatal(err)
	}

	restored := false
	defer func() {
		if !restored {
			setWatermark(t, restoreWatermark)
			time.Sleep(time.Second)
		}
		_ = transport.DeleteQueue(ctx, queue)
		_ = transport.Close()
	}()

	reason := blockTheConnection(t, transport, queue)
	t.Logf("the broker blocked this connection: %q", reason)

	// The broker closes it, which is a reconnection the client did not ask for
	// and the one that used to leave the state behind. Closing it from here
	// would wait on a broker that is not reading this socket.
	transport.mu.Lock()
	generation := transport.generation
	transport.mu.Unlock()

	rabbitmqctl(t, "close_all_connections", "acemq blocked-recovery test")

	deadline := time.Now().Add(recoverWaitLimit)
	for time.Now().Before(deadline) && !transport.reconnectedSince(generation) {
		time.Sleep(200 * time.Millisecond)
	}
	if !transport.reconnectedSince(generation) {
		t.Fatalf("the transport did not reconnect within %s", recoverWaitLimit)
	}

	if reason := transport.BlockedReason(); reason != "" {
		t.Fatalf("BlockedReason = %q after a reconnection; the state belonged to the "+
			"connection that has gone, and this one has published nothing", reason)
	}

	// And the alarm was still on the whole time, so the clearing above was this
	// library's doing rather than the broker having sent an unblock: publishing
	// on the fresh connection blocks it again.
	second := blockTheConnection(t, transport, queue)
	t.Logf("the fresh connection blocked in its turn: %q", second)

	unblockTheConnection(t, transport)
	restored = true
}
