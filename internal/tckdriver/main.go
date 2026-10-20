// Command tckdriver is the AceMQ TCK driver for this library. It is test
// tooling, not part of the library's API.
//
// The TCK runner (github.com/AceMQ-Company/acemq-amqp-tck) starts it once,
// writes one JSON request per line to stdin and reads one JSON answer per line
// from stdout. Every operation goes through the public API with default
// settings; nothing here asserts, because the runner checks the outcome on the
// broker. The protocol is docs/driver-contract.md in the TCK repository.
//
//	go run ./internal/tckdriver
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	"github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

var ops = []string{"hello", "begin", "end", "publish", "consume", "handled", "schedule", "request",
	"declareStream", "readStream", "metrics", "replay", "pipeline"}

type args map[string]any

func (a args) str(name, fallback string) string {
	if s, ok := a[name].(string); ok {
		return s
	}
	return fallback
}

func (a args) num(name string, fallback int) int {
	if n, ok := a[name].(float64); ok {
		return int(n)
	}
	return fallback
}

func (a args) obj(name string) args {
	m, _ := a[name].(map[string]any)
	return m
}

func (a args) millis(name string, fallback int) time.Duration {
	return time.Duration(a.num(name, fallback)) * time.Millisecond
}

type unsupportedError struct{ op string }

func (e unsupportedError) Error() string { return "tck: unsupported op " + e.op }

type tally struct {
	mu          sync.Mutex
	counts      map[string]int
	redelivered map[string]int
	order       []string
}

// count records a delivery and returns how many times its id has been seen.
func (t *tally) count(m acemq.Message[map[string]any]) (string, int) {
	id := fmt.Sprint(m.Payload["id"])
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counts[id]++
	if m.Redelivered {
		t.redelivered[id]++
	}
	t.order = append(t.order, id)
	return id, t.counts[id]
}

// recorder is the telemetry seam's observer: one sample per report.
type recorder struct {
	mu      sync.Mutex
	samples []map[string]any
}

func (r *recorder) add(name string, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, map[string]any{"name": name, "tags": labels})
}

func (r *recorder) Count(name string, _ int64, labels map[string]string)     { r.add(name, labels) }
func (r *recorder) Observe(name string, _ float64, labels map[string]string) { r.add(name, labels) }
func (r *recorder) Gauge(name string, _ int64, labels map[string]string)     { r.add(name, labels) }

type driver struct {
	url        string
	ctx        context.Context
	cancel     context.CancelFunc
	conn       *acemq.Conn
	consumers  []*acemq.Consumer
	pipelines  []io.Closer
	tallies    map[string]*tally
	metrics    *recorder
	scheduler  *patterns.Scheduler
	requesters map[[2]string]*patterns.Requester[any, any]
}

func (d *driver) mq() (*acemq.Conn, error) {
	if d.conn == nil {
		return nil, errors.New("tck: begin was not called")
	}
	return d.conn, nil
}

func (d *driver) hello(args) (any, error) {
	return map[string]any{"library": "acemq-go-amqp", "version": acemq.Version, "ops": ops}, nil
}

func (d *driver) begin(a args) (any, error) {
	if _, err := d.end(nil); err != nil {
		return nil, err
	}
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.metrics = &recorder{}
	opts := []acemq.ConnOption{
		acemq.WithOrigin(a.str("clientName", "tck") + "@" + host), acemq.WithObserver(d.metrics)}
	if confirms, ok := a["confirms"].(bool); ok && !confirms {
		// The library's switch for the user's own publishes lives on the
		// RabbitMQ transport's configuration.
		transport, err := rabbitmq.Dial(d.ctx, d.url, rabbitmq.Config{WithoutConfirms: true})
		if err != nil {
			return nil, err
		}
		d.conn, err = acemq.NewConn(transport, opts...)
		return map[string]any{}, err
	}
	d.conn, err = acemq.Connect(d.ctx, d.url, opts...)
	return map[string]any{}, err
}

func (d *driver) end(args) (any, error) {
	for _, r := range d.requesters {
		_ = r.Close()
	}
	d.requesters = map[[2]string]*patterns.Requester[any, any]{}
	if d.scheduler != nil {
		_ = d.scheduler.Close()
		d.scheduler = nil
	}
	for _, p := range d.pipelines {
		_ = p.Close()
	}
	d.pipelines = nil
	for _, c := range d.consumers {
		_ = c.Close()
	}
	d.consumers = nil
	d.tallies = map[string]*tally{}
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
	}
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	return map[string]any{}, nil
}

func (d *driver) publish(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	var opts []acemq.EnvelopeOption
	if env := a.obj("envelope"); env != nil {
		if v, ok := env["id"].(string); ok {
			opts = append(opts, acemq.MessageID(v))
		}
		if v, ok := env["type"].(string); ok {
			opts = append(opts, acemq.MessageType(v))
		}
		if _, ok := env["version"].(float64); ok {
			opts = append(opts, acemq.SchemaVersion(env.num("version", 0)))
		}
		if v, ok := env["correlationId"].(string); ok {
			opts = append(opts, acemq.CorrelationID(v))
		}
		if v, ok := env["causationId"].(string); ok {
			opts = append(opts, acemq.CausationID(v))
		}
		for name, value := range env.obj("headers") {
			opts = append(opts, acemq.Header(name, value))
		}
	}
	// Absent is the library's default, which is mandatory.
	var popts []acemq.PublisherOption[any]
	if mandatory, ok := a["mandatory"].(bool); ok {
		if mandatory {
			popts = append(popts, acemq.Mandatory[any]())
		} else {
			popts = append(popts, acemq.AllowUnroutable[any]())
		}
	}
	publisher := acemq.NewPublisher[any](conn, a.str("exchange", ""), a.str("routingKey", ""), popts...)
	result, err := publisher.SendResult(d.ctx, a["body"], opts...)
	if err != nil {
		return nil, err
	}
	return map[string]any{"messageId": result.MessageID}, nil
}

func (d *driver) consume(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	queue := a.str("queue", "")
	t := &tally{counts: map[string]int{}, redelivered: map[string]int{}}
	handle := failing(t, a.obj("behaviour"))
	if idempotent, _ := a["idempotent"].(bool); idempotent {
		// The count is inside the wrapper, so a suppressed duplicate is not
		// counted. Zero is the store's default retention.
		handle = patterns.Idempotent(patterns.NewInMemoryIdempotencyStore(0), handle)
	}

	topology := acemq.NewTopology().Queue(queue).DeadLetters(queue)
	opts := []acemq.ConsumeOption{acemq.Prefetch(a.num("prefetch", 10))}
	// Absent is no retry policy at all: the consumer as the library builds it
	// when the caller configures none.
	if retry := a.obj("retry"); retry != nil {
		policy := acemq.FixedRetry(retry.num("maxAttempts", 3), retry.millis("delayMs", 1000))
		topology = topology.Retries(queue, policy)
		opts = append(opts, acemq.RetryWith(policy))
	}
	if err := topology.Apply(d.ctx, conn); err != nil {
		return nil, err
	}
	consumer, err := acemq.Consume(d.ctx, conn, queue, handle, opts...)
	if err != nil {
		return nil, err
	}
	d.consumers = append(d.consumers, consumer)
	d.tallies[a.str("handle", "")] = t
	return map[string]any{}, nil
}

// failing is the contract's handler: count, sleep, and fail with the library's
// ordinary retryable failure for the first n deliveries of an id in failFirst.
func failing(t *tally, behaviour args) acemq.Handler[map[string]any] {
	failFirst := behaviour.obj("failFirst")
	sleep := behaviour.millis("sleepMs", 0)
	return func(_ context.Context, m acemq.Message[map[string]any]) acemq.Ack {
		id, n := t.count(m)
		time.Sleep(sleep)
		if n <= failFirst.num(id, 0) {
			return acemq.Retry(fmt.Errorf("tck: failing %s on purpose, delivery %d", id, n))
		}
		return acemq.Accept()
	}
}

func (d *driver) handled(a args) (any, error) {
	t, ok := d.tallies[a.str("handle", "")]
	if !ok {
		return nil, fmt.Errorf("tck: no consumer with handle %q", a.str("handle", ""))
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	counts, redelivered := map[string]int{}, map[string]int{}
	for k, v := range t.counts {
		counts[k] = v
	}
	for k, v := range t.redelivered {
		redelivered[k] = v
	}
	order := append([]string{}, t.order...)
	return map[string]any{"counts": counts, "redelivered": redelivered, "order": order}, nil
}

func (d *driver) declareStream(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	err = patterns.DeclareStream(d.ctx, conn, a.str("queue", ""),
		patterns.StreamRetention{MaxAge: a.millis("maxAgeMs", 0)})
	return map[string]any{}, err
}

func (d *driver) readStream(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	var offset patterns.StreamOffset
	switch a.str("offset", "next") {
	case "first":
		offset = patterns.FromFirst()
	case "next":
		offset = patterns.FromNext()
	default:
		return nil, unsupportedError{"readStream offset " + a.str("offset", "")}
	}
	t := &tally{counts: map[string]int{}, redelivered: map[string]int{}}
	consumer, err := patterns.ReadStream(d.ctx, conn, a.str("queue", ""),
		failing(t, a.obj("behaviour")), patterns.StreamOptions{Offset: offset})
	if err != nil {
		return nil, err
	}
	d.consumers = append(d.consumers, consumer)
	d.tallies[a.str("handle", "")] = t
	return map[string]any{}, nil
}

func (d *driver) metricsOp(args) (any, error) {
	if d.metrics == nil {
		return nil, errors.New("tck: begin was not called")
	}
	d.metrics.mu.Lock()
	defer d.metrics.mu.Unlock()
	names, seen := []string{}, map[string]bool{}
	for _, s := range d.metrics.samples {
		if name := s["name"].(string); !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return map[string]any{"names": names, "samples": append([]map[string]any{}, d.metrics.samples...)}, nil
}

func (d *driver) schedule(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	if d.scheduler == nil {
		if d.scheduler, err = patterns.NewScheduler(d.ctx, conn); err != nil {
			return nil, err
		}
	}
	err = d.scheduler.In(d.ctx, a.millis("delayMs", 0), a.str("exchange", ""), a.str("routingKey", ""), a["body"])
	return map[string]any{}, err
}

func (d *driver) request(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	key := [2]string{a.str("exchange", ""), a.str("routingKey", "")}
	requester, ok := d.requesters[key]
	if !ok {
		requester, err = patterns.NewRequester[any, any](d.ctx, conn, key[0], key[1],
			patterns.Timeout(a.millis("timeoutMs", 30000)))
		if err != nil {
			return nil, err
		}
		d.requesters[key] = requester
	}
	reply, err := requester.Do(d.ctx, a["body"])
	if err != nil {
		return nil, err
	}
	return map[string]any{"reply": reply}, nil
}

// replay is one filtered patterns.Replay with default settings, answered as
// soon as it returns.
func (d *driver) replay(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	if list, ok := a["ids"].([]any); ok {
		for _, id := range list {
			if s, ok := id.(string); ok {
				ids[s] = true
			}
		}
	}
	result, err := patterns.Replay(d.ctx, conn, patterns.ReplayFrom{
		Queue: a.str("from", ""), Exchange: "", RoutingKey: a.str("queue", ""),
		Filter: func(_ acemq.Envelope, body []byte) bool {
			var msg struct {
				ID string `json:"id"`
			}
			return json.Unmarshal(body, &msg) == nil && ids[msg.ID]
		},
	})
	if err != nil {
		// The contract reports every replay failure as kind "other".
		return nil, fmt.Errorf("tck: replay failed: %v", err)
	}
	return map[string]any{"replayed": result.Moved}, nil
}

// pipeline declares and starts a pipeline whose every step is the identity on
// the JSON body, counted under handle, failing as behaviour names it.
func (d *driver) pipeline(a args) (any, error) {
	conn, err := d.mq()
	if err != nil {
		return nil, err
	}
	t := &tally{counts: map[string]int{}, redelivered: map[string]int{}}
	behaviour := a.obj("behaviour")
	var steps []patterns.StepSpec
	if names, ok := a["steps"].([]any); ok {
		for _, name := range names {
			b := behaviour.obj(fmt.Sprint(name))
			failFirst, sleep := b.obj("failFirst"), b.millis("sleepMs", 0)
			// The step's own handler fails, with no retry policy configured
			// anywhere: the step is what the library builds by default.
			step := func(_ context.Context, m acemq.Message[map[string]any]) (map[string]any, bool, error) {
				id, n := t.count(m)
				time.Sleep(sleep)
				if n <= failFirst.num(id, 0) {
					return nil, false, fmt.Errorf("tck: failing %s on purpose, delivery %d", id, n)
				}
				return m.Payload, true, nil
			}
			steps = append(steps, patterns.PipelineStep(fmt.Sprint(name), step))
		}
	}
	p, err := patterns.NewPipeline[map[string]any](d.ctx, conn, a.str("name", ""), steps...)
	if err != nil {
		return nil, err
	}
	d.pipelines = append(d.pipelines, p)
	d.tallies[a.str("handle", "")] = t
	return map[string]any{}, nil
}

// kindOf maps an error onto the TCK's closed list of failure kinds.
func kindOf(err error) string {
	var unsupported unsupportedError
	var failed *acemq.PublishFailedError
	var paused *acemq.PublishingPausedError
	switch {
	case errors.As(err, &paused):
		return "paused"
	case errors.As(err, &unsupported):
		return "unsupported"
	case errors.Is(err, patterns.ErrRequestTimedOut):
		return "timeout"
	case errors.As(err, &failed):
		if failed.Unroutable {
			return "unroutable"
		}
		return "nacked"
	case errors.Is(err, patterns.ErrResponderFailed):
		return "responder"
	case acemq.IsFatal(err):
		return "decode"
	}
	return "other"
}

func main() {
	url := os.Getenv("ACEMQ_TCK_AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	d := &driver{url: url, tallies: map[string]*tally{}, requesters: map[[2]string]*patterns.Requester[any, any]{}}
	handlers := map[string]func(args) (any, error){
		"hello": d.hello, "begin": d.begin, "end": d.end, "publish": d.publish,
		"consume": d.consume, "handled": d.handled, "schedule": d.schedule, "request": d.request,
		"declareStream": d.declareStream, "readStream": d.readStream, "metrics": d.metricsOp,
		"replay": d.replay, "pipeline": d.pipeline,
	}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 16<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var req args
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			fmt.Fprintln(os.Stderr, "tck: not a JSON request:", err)
			continue
		}
		op := req.str("op", "")
		answer := map[string]any{"id": req["id"]}
		var result any
		var err error
		if handler, ok := handlers[op]; ok {
			result, err = handler(req)
		} else {
			err = unsupportedError{op}
		}
		if err != nil {
			answer["ok"] = false
			answer["error"] = map[string]any{
				"kind": kindOf(err), "type": reflect.TypeOf(err).String(), "message": err.Error(),
			}
		} else {
			answer["ok"] = true
			answer["result"] = result
		}
		if err := out.Encode(answer); err != nil {
			fmt.Fprintln(os.Stderr, "tck: cannot write the answer:", err)
			os.Exit(1)
		}
	}
	_, _ = d.end(nil)
}
