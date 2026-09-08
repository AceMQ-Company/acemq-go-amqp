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

## How we know the five libraries agree

Not by reading each other's documentation. The Java, Go, .NET, Python and Ruby
libraries are held to two generated fixture files, both produced by the Java
implementation and carried byte for byte by every repository:

| File | What it pins |
| --- | --- |
| [`internal/testdata/envelope-fixtures.json`](envelope.md) | the headers a publish puts on the wire |
| `internal/testdata/contract-fixtures.json` | the retry schedule, the queue names and the topology |

`contract-fixtures.json` is read by `amqp/contract_test.go`, which asserts every
section of it: the five named retry policies and their unjittered delays, the
rung delays each one needs, the attempt and age limits as a table of decisions,
the jitter bounds, the thirty rows of the broker-wait threshold table, the
`.dlq`, `.parked` and `.retry.{delay}` names, the three arguments a rung is
declared with, the whole declared topology with each entry's type and arguments,
and the queue-type defaults.

Two things about how it does that matter more than the list.

**Expectations are derived twice wherever they can be.** A test that reads a
number out of the fixture and compares it against the same number read out of
this library proves only that both can read. So the doubling is checked by
asserting each delay is twice the one before it, the threshold table is
recomputed from the rule the file states, the rung names are rendered a second
time by an obvious implementation written out longhand beside the real one, and
the jitter bounds are established by rolling this library's own jitter twenty
thousand times and looking at where the results landed.

**This is why it exists.** Java shipped `exponential()` with a multiplier of five
and ten percent jitter through ten releases while Go, .NET, Python and Ruby all
doubled with twenty percent — `exponential(5, 1s, 1m)` gave `1s, 5s, 25s, 60s`
in Java and `1s, 2s, 4s, 8s` here. Nobody noticed for months, because every
library tested its own arithmetic against its own expectations and every suite
was green.

### Disagreements are recorded, not smoothed over

Three differences between this library and the fixture are pinned by tests that
say exactly what each side does, so that closing one is a decision somebody makes
rather than something a green suite hides:

- **Sub-second rung names.** Java renders `orders.new.retry.500ms`; this library,
  Python and Ruby render `orders.new.retry.0s`. The two names are not the same
  queue, so whichever service declares it second is answered
  `PRECONDITION_FAILED`. Unreachable through the default thirty-second
  threshold. The file records it under `naming.subSecondDisagreement`.
- **The default message-age limit.** Java's constructors carry a 365-day age
  limit; this library, Python, Ruby and .NET treat no limit as no limit. A
  message a year old has outlived every queue it could be sitting in.
- **Where the line between the two halves of the topology falls.** The operator
  declares the source queue and its dead letters through `Topology`; the consumer
  declares the rungs. Java's consumer half also declares `acemq.dlx`,
  `{queue}.dlq` and `{queue}.parked`; `RetryLadder.Declare` here declares only
  the rungs and the binding that brings an expired message home. The union of
  the two halves is identical in both, and it is what
  [a topology written the way `Topology` documents](topology.md) produces.

### Checking the copies have not drifted

A copy that has drifted is worse than no fixture at all: the library carrying it
still passes its own suite, agreeing with the wrong file in private. The
workspace script `check-fixtures.sh` compares every library's copy of every
fixture by hash and fails on a single changed character.

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
