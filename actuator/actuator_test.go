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

package actuator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

func get(t *testing.T, a *Actuator, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func brokerFor(t *testing.T) *acemq.Conn {
	t.Helper()
	mq, err := acemq.Connect(context.Background(), "memory://"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mq.Close() })
	return mq
}

func TestMetricsAreServedInPrometheusFormat(t *testing.T) {
	metrics := acemq.NewMetrics()
	metrics.Count(acemq.MetricPublishTotal, 3, map[string]string{
		acemq.TagExchange: "events", acemq.TagRoutingKey: "order.placed"})
	metrics.Gauge(acemq.MetricConsumeInFlight, 2, map[string]string{"queue": "orders"})
	metrics.Observe(acemq.MetricConsumeDuration, 0.25, map[string]string{"queue": "orders"})

	body := get(t, New(Options{Metrics: metrics}), MetricsPath).Body.String()

	// Dots become underscores, and labels are rendered the way Prometheus
	// wants rather than the way the key stores them. The label name is
	// converted too: routing.key is a legal AceMQ tag and an illegal
	// Prometheus label, and one dot left in place makes the whole scrape
	// unparseable.
	if !strings.Contains(body, `acemq_publish_total{exchange="events",routing_key="order.placed"} 3`) {
		t.Errorf("counter not rendered:\n%s", body)
	}
	if !strings.Contains(body, `acemq_consume_in_flight{queue="orders"} 2`) {
		t.Errorf("gauge not rendered:\n%s", body)
	}
	if !strings.Contains(body, "acemq_consume_duration_count") ||
		!strings.Contains(body, "acemq_consume_duration_sum") {
		t.Errorf("summary not rendered:\n%s", body)
	}
	if !strings.Contains(body, "# TYPE acemq_publish_total counter") {
		t.Errorf("no TYPE line:\n%s", body)
	}
}

// TestEveryFamilyTagNameSurvivesPrometheus walks the whole tag vocabulary rather
// than the two tags that happen to be used above.
//
// routing.key was found the hard way: it is a legal AceMQ tag and an illegal
// Prometheus label, and emitted verbatim it does not make one label wrong, it
// makes the entire scrape unparseable. message.type has a dot for exactly the
// same reason and would have been the second time. This test is here so a third
// dotted tag added to the family cannot reach a scrape endpoint unnoticed.
func TestEveryFamilyTagNameSurvivesPrometheus(t *testing.T) {
	tags := []string{
		acemq.TagQueue, acemq.TagOutcome, acemq.TagExchange, acemq.TagRoutingKey,
		acemq.TagMessageType, acemq.TagTransport, acemq.TagPipeline, acemq.TagStep,
		acemq.TagRung, acemq.TagTarget,
	}

	metrics := acemq.NewMetrics()
	labels := map[string]string{}
	for _, tag := range tags {
		labels[tag] = "v"
	}
	metrics.Count(acemq.MetricConsumeTotal, 1, labels)

	body := get(t, New(Options{Metrics: metrics}), MetricsPath).Body.String()

	// A Prometheus label name is [a-zA-Z_][a-zA-Z0-9_]*. Nothing else may reach
	// the wire, whatever the tag is called in this library.
	legal := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	for _, line := range strings.Split(body, "\n") {
		open := strings.Index(line, "{")
		if strings.HasPrefix(line, "#") || open < 0 {
			continue
		}
		inside := line[open+1 : strings.LastIndex(line, "}")]
		for _, pair := range strings.Split(inside, ",") {
			name, _, found := strings.Cut(pair, "=")
			if !found {
				continue
			}
			if !legal.MatchString(name) {
				t.Errorf("label name %q is not a legal Prometheus label; the scrape"+
					" would not parse at all. Line: %s", name, line)
			}
		}
	}

	// And the two dotted ones specifically, so a change that dropped the
	// rewriting entirely would fail here by name rather than by regex.
	for _, want := range []string{`routing_key="v"`, `message_type="v"`} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %s in the output:\n%s", want, body)
		}
	}
}

func TestMetricsWithoutACollectorAreEmptyRatherThanBroken(t *testing.T) {
	// A service may want health without metrics, and asking for metrics it is
	// not collecting should not be an error.
	w := get(t, New(Options{}), MetricsPath)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d", w.Code)
	}
}

// TestHealthAnswersAProbeWithItsStatusCode is what a readiness probe reads.
func TestHealthAnswersAProbeWithItsStatusCode(t *testing.T) {
	up := get(t, New(Options{Conn: brokerFor(t)}), HealthPath)
	if up.Code != http.StatusOK {
		t.Errorf("a working connection answered %d", up.Code)
	}

	down := get(t, New(Options{Checks: []acemq.HealthCheck{failing{}}}), HealthPath)
	if down.Code != http.StatusServiceUnavailable {
		t.Errorf("a failing check answered %d, want 503", down.Code)
	}

	var report acemq.HealthReport
	if err := json.Unmarshal(down.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != acemq.HealthDown {
		t.Errorf("the body says %s", report.Status)
	}
}

func TestDegradedStaysInRotation(t *testing.T) {
	// Worth an alert, not worth replacing an instance whose replacement would
	// be degraded too.
	w := get(t, New(Options{Checks: []acemq.HealthCheck{degraded{}}}), HealthPath)

	if w.Code != http.StatusOK {
		t.Errorf("a degraded service answered %d, want 200", w.Code)
	}
}

func TestInfoSaysWhatIsRunning(t *testing.T) {
	mq := brokerFor(t)
	w := get(t, New(Options{Conn: mq, Name: "orders", Version: "1.4.2"}), InfoPath)

	var info map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}

	if info["name"] != "orders" || info["version"] != "1.4.2" {
		t.Errorf("info = %v", info)
	}
	if info["library"] != "acemq-go-amqp" {
		t.Errorf("the library does not name itself: %v", info)
	}

	transport, ok := info["transport"].(map[string]any)
	if !ok {
		t.Fatalf("no transport capabilities: %v", info)
	}
	// The in-memory transport claims less than RabbitMQ, deliberately, and the
	// endpoint should report that honestly.
	if transport["quorum-queues"] != false {
		t.Errorf("the in-memory transport claims quorum queues: %v", transport)
	}
}

func TestThePathsMatchTheOtherLibraries(t *testing.T) {
	// A scrape configuration or a probe written for Java or .NET has to work
	// here without being rewritten.
	if MetricsPath != "/acemq-metrics" || HealthPath != "/acemq-health" || InfoPath != "/acemq-info" {
		t.Errorf("paths are %s, %s, %s", MetricsPath, HealthPath, InfoPath)
	}
}

func TestAPrefixMovesEverything(t *testing.T) {
	a := New(Options{Prefix: "/internal"})

	if w := get(t, a, "/internal"+HealthPath); w.Code == http.StatusNotFound {
		t.Error("the prefixed path is not served")
	}
	if w := get(t, a, HealthPath); w.Code != http.StatusNotFound {
		t.Error("the unprefixed path is still served")
	}
	if paths := a.Paths(); paths[0] != "/internal"+MetricsPath {
		t.Errorf("Paths() = %v", paths)
	}
}

type failing struct{}

func (failing) Name() string { return "database" }
func (failing) Check(context.Context) acemq.HealthReport {
	return acemq.HealthReport{Status: acemq.HealthDown, Detail: "unreachable"}
}

type degraded struct{}

func (degraded) Name() string { return "cache" }
func (degraded) Check(context.Context) acemq.HealthReport {
	return acemq.HealthReport{Status: acemq.HealthDegraded, Detail: "slow"}
}
