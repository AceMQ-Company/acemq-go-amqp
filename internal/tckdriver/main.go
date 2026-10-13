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
	"os"
	"reflect"
	"sync"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
	_ "github.com/AceMQ-Company/acemq-go-amqp/rabbitmq"
)

var ops = []string{"hello", "begin", "end", "publish", "consume", "handled", "schedule", "request"}

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
}

type driver struct {
	url        string
	ctx        context.Context
	cancel     context.CancelFunc
	conn       *acemq.Conn
	consumers  []*acemq.Consumer
	tallies    map[string]*tally
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
	d.conn, err = acemq.Connect(d.ctx, d.url, acemq.WithOrigin(a.str("clientName", "tck")+"@"+host))
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
	publisher := acemq.NewPublisher[any](conn, a.str("exchange", ""), a.str("routingKey", ""))
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
	retry := a.obj("retry")
	policy := acemq.FixedRetry(retry.num("maxAttempts", 3), retry.millis("delayMs", 1000))
	behaviour := a.obj("behaviour")
	failFirst := behaviour.obj("failFirst")
	sleep := behaviour.millis("sleepMs", 0)
	t := &tally{counts: map[string]int{}, redelivered: map[string]int{}}

	handle := func(_ context.Context, m acemq.Message[map[string]any]) acemq.Ack {
		id := fmt.Sprint(m.Payload["id"])
		t.mu.Lock()
		t.counts[id]++
		n := t.counts[id]
		if m.Redelivered {
			t.redelivered[id]++
		}
		t.mu.Unlock()
		time.Sleep(sleep)
		if n <= failFirst.num(id, 0) {
			return acemq.Retry(fmt.Errorf("tck: failing %s on purpose, delivery %d", id, n))
		}
		return acemq.Accept()
	}

	topology := acemq.NewTopology().Queue(queue).DeadLetters(queue).Retries(queue, policy)
	if err := topology.Apply(d.ctx, conn); err != nil {
		return nil, err
	}
	consumer, err := acemq.Consume(d.ctx, conn, queue, handle,
		acemq.RetryWith(policy), acemq.Prefetch(a.num("prefetch", 10)))
	if err != nil {
		return nil, err
	}
	d.consumers = append(d.consumers, consumer)
	d.tallies[a.str("handle", "")] = t
	return map[string]any{}, nil
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
	return map[string]any{"counts": counts, "redelivered": redelivered}, nil
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

// kindOf maps an error onto the TCK's closed list of failure kinds.
func kindOf(err error) string {
	var unsupported unsupportedError
	var failed *acemq.PublishFailedError
	switch {
	case errors.As(err, &unsupported):
		return "unsupported"
	case errors.Is(err, patterns.ErrRequestTimedOut):
		return "timeout"
	case errors.As(err, &failed):
		if failed.Unroutable {
			return "unroutable"
		}
		return "refused"
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
