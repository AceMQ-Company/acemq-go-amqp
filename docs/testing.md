# Testing without a broker

`memory://` is built into the core package. Nothing needs to be installed, no
container has to start, and the code under test does not change:

```go
func TestOrdersArePlaced(t *testing.T) {
	ctx := context.Background()

	mq, err := acemq.Connect(ctx, "memory://"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer mq.Close()

	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	got := make(chan OrderPlaced, 1)
	sub, err := acemq.Consume(ctx, mq, "orders",
		func(_ context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
			got <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[OrderPlaced](mq, "", "orders").
		Send(ctx, OrderPlaced{OrderID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	select {
	case order := <-got:
		if order.OrderID != "o-1" {
			t.Errorf("got %+v", order)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the message never arrived")
	}
}
```

## One broker per test

Each distinct URL is a separate broker, so `"memory://"+t.Name()` gives every
test its own and `t.Parallel()` needs no coordination. Sharing one and clearing
it between tests is the arrangement that eventually deletes another test's
messages while it is running.

## It is not kinder than RabbitMQ

This is the point of it, and the reason to trust a test that uses it.

- A message returned for retry comes back **marked redelivered**, exactly as a
  broker returns it, so [attempt counting](reliability.md) is exercised rather
  than assumed.
- A rejected message with nowhere to go is dropped, as a broker drops it.
- Consuming from a queue that was never declared fails.
- Binding to an exchange that was never declared fails.
- Topic patterns are matched the way the broker matches them, and a test against
  a real broker checks that claim.

A test transport that is more forgiving than the real one certifies code that
then fails in production, which is worse than having no test transport at all.

## What it does not do

It is an in-process queue, not a broker. It does not implement durability,
clustering, publisher confirms, flow control, or the management interface, and
it does not enforce prefetch — the consumer's own
[`Concurrency`](consuming.md) bounds how many messages are in flight instead.

So it will not catch a queue argument RabbitMQ rejects, or a topology that
deadlocks under real flow control. For those, use a broker.

## Against a real broker

```bash
docker run -d -p 5672:5672 rabbitmq:4-alpine
ACEMQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/ go test ./...
```

The library's own broker tests read that variable and **skip** when it is unset,
so `go test ./...` works on a machine without Docker. A skip reads as success,
which is how a suite quietly stops testing anything, so CI runs a broker and
then fails the build if any of those tests skipped — the skip is a convenience
for a laptop, not a way out.

Worth copying for your own tests:

```go
func brokerURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("ACEMQ_TEST_AMQP_URL")
	if url == "" {
		t.Skip("ACEMQ_TEST_AMQP_URL is not set; skipping the tests that need a broker")
	}
	return url
}
```

## Waiting for a message

Messages arrive on another goroutine, so a test has to wait — but never for ever,
or a failing test hangs the suite instead of saying what did not happen:

```go
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
```

A channel with a `select` and a timeout does the same job when there is one
message to wait for. Use `-race`: message handling is concurrent, and a test
that shares a variable between a handler and the test body without a mutex will
pass a thousand times and then not.

## Testing a handler on its own

A handler is a function. The cheapest test does not involve a broker at all:

```go
ack := handleOrder(ctx, acemq.Message[OrderPlaced]{
	Payload:  OrderPlaced{OrderID: "o-1"},
	Envelope: acemq.Envelope{ID: "m-1", Attempt: 3},
})

if ack.String() != "reject" {
	t.Errorf("got %s, want reject on the third attempt", ack)
}
```

Use `memory://` for what needs the machinery around the handler — routing,
retrying, acknowledgement — and call the function directly for what does not.
