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
	"net/url"
	"strings"
	"sync"
)

// The in-memory transport, reached as memory://name.
//
// It exists so tests can exercise real publishing, consuming, acknowledgement
// and retry without a broker. It is deliberately not more forgiving than
// RabbitMQ: a message returned for retry comes back marked redelivered, exactly
// as a broker would return it, because a test transport that is kinder than the
// real one certifies code that then fails in production.
//
// Each distinct URL is a separate broker, so tests running in parallel do not
// have to coordinate:
//
//	mq, err := acemq.Connect(ctx, "memory://"+t.Name())
func init() {
	RegisterTransport("memory", dialMemory)
}

var (
	brokersMu sync.Mutex
	brokers   = map[string]*memBroker{}
)

func dialMemory(_ context.Context, rawURL string) (Transport, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("acemq: %q is not a usable memory URL: %w", rawURL, err)
	}
	name := parsed.Host + parsed.Path

	brokersMu.Lock()
	defer brokersMu.Unlock()
	broker, ok := brokers[name]
	if !ok {
		broker = &memBroker{
			queues:    map[string]*memQueue{},
			exchanges: map[string]*memExchange{},
		}
		brokers[name] = broker
	}
	return &memTransport{broker: broker}, nil
}

type memBroker struct {
	mu        sync.Mutex
	queues    map[string]*memQueue
	exchanges map[string]*memExchange
}

type memExchange struct {
	kind     string
	bindings []memBinding
}

type memBinding struct {
	queue      string
	routingKey string
}

type memTransport struct {
	broker *memBroker

	mu     sync.Mutex
	subs   []*memSubscription
	closed bool
}

func (t *memTransport) DeclareQueue(_ context.Context, name string, _ QueueSpec) error {
	t.broker.mu.Lock()
	defer t.broker.mu.Unlock()
	if _, ok := t.broker.queues[name]; !ok {
		t.broker.queues[name] = newMemQueue(name)
	}
	return nil
}

func (t *memTransport) DeclareExchange(_ context.Context, name string, spec ExchangeSpec) error {
	kind := spec.Kind
	if kind == "" {
		kind = "direct"
	}
	t.broker.mu.Lock()
	defer t.broker.mu.Unlock()
	if _, ok := t.broker.exchanges[name]; !ok {
		t.broker.exchanges[name] = &memExchange{kind: kind}
	}
	return nil
}

func (t *memTransport) Bind(_ context.Context, queue, exchange, routingKey string) error {
	t.broker.mu.Lock()
	defer t.broker.mu.Unlock()

	ex, ok := t.broker.exchanges[exchange]
	if !ok {
		return fmt.Errorf("acemq: cannot bind %q: no exchange named %q has been declared", queue, exchange)
	}
	if _, ok := t.broker.queues[queue]; !ok {
		return fmt.Errorf("acemq: cannot bind %q: no queue of that name has been declared", queue)
	}
	ex.bindings = append(ex.bindings, memBinding{queue: queue, routingKey: routingKey})
	return nil
}

func (t *memTransport) Publish(_ context.Context, exchange, routingKey string, msg Outbound) error {
	t.broker.mu.Lock()
	targets := t.broker.route(exchange, routingKey)
	t.broker.mu.Unlock()

	for _, q := range targets {
		q.push(memMsg{
			body:        append([]byte(nil), msg.Body...),
			contentType: msg.ContentType,
			messageID:   msg.MessageID,
			routingKey:  routingKey,
			headers:     copyHeaders(msg.Headers),
		})
	}
	return nil
}

// route finds the queues a message should reach. Called with the broker locked.
func (b *memBroker) route(exchange, routingKey string) []*memQueue {
	if exchange == "" {
		// The default exchange delivers to the queue named by the routing key,
		// as RabbitMQ's does.
		if q, ok := b.queues[routingKey]; ok {
			return []*memQueue{q}
		}
		return nil
	}

	ex, ok := b.exchanges[exchange]
	if !ok {
		return nil
	}

	var out []*memQueue
	seen := map[string]bool{}
	for _, binding := range ex.bindings {
		if seen[binding.queue] {
			continue
		}
		matched := false
		switch ex.kind {
		case "fanout":
			matched = true
		case "topic":
			matched = topicMatches(binding.routingKey, routingKey)
		default:
			matched = binding.routingKey == routingKey
		}
		if matched {
			if q, ok := b.queues[binding.queue]; ok {
				out = append(out, q)
				seen[binding.queue] = true
			}
		}
	}
	return out
}

func (t *memTransport) Consume(
	_ context.Context, queue string, _ ConsumeSpec, deliver func(Delivery),
) (Subscription, error) {
	t.broker.mu.Lock()
	q, ok := t.broker.queues[queue]
	t.broker.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("acemq: no queue named %q has been declared", queue)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, fmt.Errorf("acemq: the connection to the in-memory broker is closed")
	}
	sub := &memSubscription{queue: q, stop: make(chan struct{})}
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	sub.wg.Add(1)
	go sub.run(deliver)
	return sub, nil
}

func (t *memTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	subs := t.subs
	t.subs = nil
	t.mu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}
	return nil
}

type memMsg struct {
	body        []byte
	contentType string
	messageID   string
	routingKey  string
	headers     map[string]any
	redelivered bool
}

type memQueue struct {
	name string

	mu      sync.Mutex
	notify  *sync.Cond
	pending []memMsg
}

func newMemQueue(name string) *memQueue {
	q := &memQueue{name: name}
	q.notify = sync.NewCond(&q.mu)
	return q
}

func (q *memQueue) push(m memMsg) {
	q.mu.Lock()
	q.pending = append(q.pending, m)
	q.mu.Unlock()
	q.notify.Broadcast()
}

// requeue puts a message back at the front, marked redelivered.
//
// The mark is the point. A broker sets it, the retry engine counts it, and a
// test transport that omitted it would let attempt counting appear to work here
// and fail against a real broker.
func (q *memQueue) requeue(m memMsg) {
	m.redelivered = true
	q.mu.Lock()
	q.pending = append([]memMsg{m}, q.pending...)
	q.mu.Unlock()
	q.notify.Broadcast()
}

// pop waits for a message, or gives up when stop is closed.
func (q *memQueue) pop(stop <-chan struct{}) (memMsg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.pending) == 0 {
		select {
		case <-stop:
			return memMsg{}, false
		default:
		}
		// Broadcast on stop wakes this; the select above then sees it.
		q.notify.Wait()
		select {
		case <-stop:
			return memMsg{}, false
		default:
		}
	}

	m := q.pending[0]
	q.pending = q.pending[1:]
	return m, true
}

// Depth is how many messages are waiting, which is what a test asserts on when
// it wants to know a message was dead-lettered rather than requeued.
func (q *memQueue) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

type memSubscription struct {
	queue     *memQueue
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func (s *memSubscription) run(deliver func(Delivery)) {
	defer s.wg.Done()
	for {
		m, ok := s.queue.pop(s.stop)
		if !ok {
			return
		}

		msg := m
		var settled sync.Once
		deliver(Delivery{
			Body:        msg.body,
			ContentType: msg.contentType,
			RoutingKey:  msg.routingKey,
			MessageID:   msg.messageID,
			Headers:     copyHeaders(msg.headers),
			Redelivered: msg.redelivered,
			Ack: func() error {
				settled.Do(func() {})
				return nil
			},
			Nack: func(requeue bool) error {
				settled.Do(func() {
					if requeue {
						s.queue.requeue(msg)
					}
					// Without requeue the message is dropped, which is what a
					// broker does when there is no dead-letter exchange.
				})
				return nil
			},
		})
	}
}

// Close stops delivery and waits for the delivering goroutine to finish, so no
// further deliveries happen once it returns. The consumer relies on that to
// close its work channel safely.
func (s *memSubscription) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		// Broadcasting under the queue's lock is what makes this safe. A waiter
		// holds that lock while it checks stop and calls Wait, and Wait releases
		// it atomically, so there is no window in which the close is missed and
		// the waiter sleeps for ever.
		s.queue.mu.Lock()
		s.queue.notify.Broadcast()
		s.queue.mu.Unlock()
		s.wg.Wait()
	})
	return nil
}

// topicMatches implements AMQP topic matching: * is exactly one word, # is zero
// or more.
//
// Word by word rather than by translating the pattern into a regular
// expression. The translation looks shorter and is wrong in ways that only show
// up on patterns nobody writes until they do — a dot inside a word, a # next to
// a *, a trailing # that has to match nothing at all.
func topicMatches(pattern, key string) bool {
	p := splitTopic(pattern)
	k := splitTopic(key)

	// matched[j] is whether the first i pattern words can match the first j key
	// words; it is rebuilt for each i.
	matched := make([]bool, len(k)+1)
	matched[0] = true

	for i := 0; i < len(p); i++ {
		next := make([]bool, len(k)+1)
		for j := 0; j <= len(k); j++ {
			if !matched[j] {
				continue
			}
			switch p[i] {
			case "#":
				// Zero or more words: every remaining position stays reachable.
				for n := j; n <= len(k); n++ {
					next[n] = true
				}
			case "*":
				if j < len(k) {
					next[j+1] = true
				}
			default:
				if j < len(k) && k[j] == p[i] {
					next[j+1] = true
				}
			}
		}
		matched = next
	}
	return matched[len(k)]
}

func splitTopic(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ".")
}

func copyHeaders(h map[string]any) map[string]any {
	if h == nil {
		return nil
	}
	out := make(map[string]any, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}
