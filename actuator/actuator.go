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

// Package actuator serves metrics, health and version over HTTP.
//
//	metrics := acemq.NewMetrics()
//	mq, err := acemq.Connect(ctx, url, acemq.WithObserver(metrics))
//
//	act := actuator.New(actuator.Options{Metrics: metrics, Conn: mq})
//	http.Handle("/", act)
//	http.ListenAndServe("127.0.0.1:9090", nil)
//
// The paths match the Java and .NET libraries, so a scrape configuration or a
// probe written for one works against another:
//
//	/acemq-metrics   Prometheus text format
//	/acemq-health    JSON, 503 when anything is down
//	/acemq-info      JSON: version, and what the transport can do
//
// # It is not authenticated
//
// Nothing here checks who is asking. Health says which dependencies are down
// and metrics say how much traffic there is, which is more than an anonymous
// caller should learn about a service.
//
// Bind it to loopback, put it on a port your ingress does not publish, or wrap
// [Actuator] in your own middleware. The library will not do it for you, and
// says so rather than implying otherwise by shipping a token check nobody
// configures.
package actuator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// Default paths, matching the Java and .NET libraries.
const (
	MetricsPath = "/acemq-metrics"
	HealthPath  = "/acemq-health"
	InfoPath    = "/acemq-info"
)

// Options configure an actuator.
type Options struct {
	// Metrics is where the numbers come from. Without it /acemq-metrics
	// reports nothing rather than failing, since a service may want health
	// without metrics.
	Metrics *acemq.Metrics

	// Conn is checked by /acemq-health.
	Conn *acemq.Conn

	// Checks are the application's own, combined with the connection's.
	Checks []acemq.HealthCheck

	// Prefix replaces the default paths, for a service that already namespaces
	// its operational endpoints.
	Prefix string

	// Name and Version appear in /acemq-info, so a running instance can say
	// what it is without being asked.
	Name    string
	Version string

	// Timeout bounds a health check, so a probe cannot hang on a dependency
	// that never answers. Five seconds by default.
	Timeout time.Duration
}

// Actuator serves the endpoints.
type Actuator struct {
	opts Options
	mux  *http.ServeMux
}

// New builds an actuator.
func New(opts Options) *Actuator {
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}

	a := &Actuator{opts: opts, mux: http.NewServeMux()}
	a.mux.HandleFunc(opts.Prefix+MetricsPath, a.metrics)
	a.mux.HandleFunc(opts.Prefix+HealthPath, a.health)
	a.mux.HandleFunc(opts.Prefix+InfoPath, a.info)
	return a
}

// ServeHTTP makes this an http.Handler.
func (a *Actuator) ServeHTTP(w http.ResponseWriter, r *http.Request) { a.mux.ServeHTTP(w, r) }

// Paths lists what this actuator serves, for logging at start-up.
func (a *Actuator) Paths() []string {
	return []string{
		a.opts.Prefix + MetricsPath,
		a.opts.Prefix + HealthPath,
		a.opts.Prefix + InfoPath,
	}
}

// metrics writes the Prometheus text format.
//
// Written directly rather than through a client library, so this package needs
// nothing outside the standard library. The format is stable and small: a HELP
// line, a TYPE line, then samples.
func (a *Actuator) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	if a.opts.Metrics == nil {
		return
	}

	counts := a.opts.Metrics.Counts()
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metric, labels := splitKey(name)
		fmt.Fprintf(w, "# TYPE %s counter\n", prometheusName(metric))
		fmt.Fprintf(w, "%s%s %d\n", prometheusName(metric), labels, counts[name])
	}

	gauges := a.opts.Metrics.Gauges()
	names = names[:0]
	for name := range gauges {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metric, labels := splitKey(name)
		fmt.Fprintf(w, "# TYPE %s gauge\n", prometheusName(metric))
		fmt.Fprintf(w, "%s%s %d\n", prometheusName(metric), labels, gauges[name])
	}

	durations := a.opts.Metrics.Durations()
	names = names[:0]
	for name := range durations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metric, labels := splitKey(name)
		summary := durations[name]
		base := prometheusName(metric)

		// A summary without quantiles: the count and the sum are what
		// Prometheus needs to compute a rate and an average, and quantiles
		// would mean keeping every sample. Min and max ride alongside as
		// gauges, which is honest about what they are.
		fmt.Fprintf(w, "# TYPE %s summary\n", base)
		fmt.Fprintf(w, "%s_count%s %d\n", base, labels, summary.Count)
		fmt.Fprintf(w, "%s_sum%s %g\n", base, labels, summary.Sum)
		fmt.Fprintf(w, "# TYPE %s_min gauge\n", base)
		fmt.Fprintf(w, "%s_min%s %g\n", base, labels, summary.Min)
		fmt.Fprintf(w, "# TYPE %s_max gauge\n", base)
		fmt.Fprintf(w, "%s_max%s %g\n", base, labels, summary.Max)
	}
}

// health answers a readiness probe.
func (a *Actuator) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.opts.Timeout)
	defer cancel()

	checks := append([]acemq.HealthCheck(nil), a.opts.Checks...)
	if a.opts.Conn != nil {
		checks = append(checks, acemq.ConnHealth{Conn: a.opts.Conn})
	}

	report := acemq.AggregateHealth(ctx, checks...)

	w.Header().Set("Content-Type", "application/json")
	// A probe reads the status code, not the body, so the code has to carry
	// the answer. Degraded stays 200: it is worth an alert and not worth
	// taking the instance out of rotation.
	if report.Status == acemq.HealthDown {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(report)
}

// info says what is running.
func (a *Actuator) info(w http.ResponseWriter, _ *http.Request) {
	info := map[string]any{
		"library":        "acemq-go-amqp",
		"libraryVersion": acemq.Version,
	}
	if a.opts.Name != "" {
		info["name"] = a.opts.Name
	}
	if a.opts.Version != "" {
		info["version"] = a.opts.Version
	}
	if a.opts.Conn != nil {
		capabilities := map[string]bool{}
		for _, c := range []acemq.Capability{
			acemq.CapabilityPublisherConfirms,
			acemq.CapabilityDeadLettering,
			acemq.CapabilityQuorumQueues,
			acemq.CapabilityStreams,
			acemq.CapabilityRecovery,
		} {
			capabilities[string(c)] = a.opts.Conn.Supports(c)
		}
		info["transport"] = capabilities
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(info)
}

// splitKey pulls a metric key apart into its name and its Prometheus labels.
//
// The key is built as name{k=v}{k=v}; Prometheus wants name{k="v",k="v"}.
func splitKey(key string) (string, string) {
	open := strings.Index(key, "{")
	if open < 0 {
		return key, ""
	}

	name := key[:open]
	var pairs []string
	for _, part := range strings.Split(key[open:], "{") {
		part = strings.TrimSuffix(part, "}")
		if part == "" {
			continue
		}
		k, v, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%q", k, v))
	}
	if len(pairs) == 0 {
		return name, ""
	}
	return name, "{" + strings.Join(pairs, ",") + "}"
}

// prometheusName converts a dotted metric name to Prometheus's underscores.
func prometheusName(name string) string {
	return strings.NewReplacer(".", "_", "-", "_").Replace(name)
}
