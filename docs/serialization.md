# Codecs

A codec turns a message payload into bytes and back.

```go
type Codec interface {
	ContentType() string
	Encode(payload any) ([]byte, error)
	Decode(body []byte, dst any) error
	CanDecode(contentType string) bool
}
```

`Decode` takes a pointer to the destination rather than returning a value, which
is how `encoding/json` and every other Go decoder works. It is the one place
this API deliberately departs from the Java and .NET shape: returning an `any`
and asking the caller to type-assert would be worse Go for no gain.

## JSON

The default, and the only one the core package carries — it needs nothing
outside the standard library.

```go
mq, err := acemq.Connect(ctx, url) // JSONCodec unless told otherwise
```

### Tag your fields

```go
type OrderPlaced struct {
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
}
```

Unlike the Java and .NET codecs there is no camelCase policy to configure,
because Go already puts the wire name in the tag. The corollary is that an
untagged field goes on the wire under its Go name — `OrderID`, capitals and all
— and nothing in another language will read it. Tag the fields.

### It answers for a message with no content type

`CanDecode("")` returns true. JSON is the default format, an untyped message is
far more likely to be JSON than anything else, and something has to read it.

This is the opposite of the choice the YAML and TOML codecs make in the .NET
library, where answering for an untyped message would record traffic under a
format nobody sent. Being the default is what earns JSON the benefit of the
doubt.

### A malformed body is fatal

```go
if err := json.Unmarshal(body, dst); err != nil {
	return Fatalf("acemq: this message is not JSON that reads as %T: %w", dst, err)
}
```

The same bytes fail the same way every time, so the message is dead-lettered
rather than retried until it ages out. It never reaches the handler. See
[retries](reliability.md).

## Choosing one

Per connection:

```go
mq, err := acemq.Connect(ctx, url, acemq.WithCodec(myCodec))
```

Per publisher or per consumer, so one connection can send JSON to one queue and
something denser to another:

```go
pub := acemq.NewPublisher[Telemetry](mq, "", "telemetry",
	acemq.PublishWith[Telemetry](myCodec))

sub, err := acemq.Consume(ctx, mq, "telemetry", handler,
	acemq.ConsumeWith(myCodec))
```

## Writing one

```go
type CSVCodec struct{}

func (CSVCodec) ContentType() string { return "text/csv" }

func (CSVCodec) Encode(payload any) ([]byte, error) {
	row, ok := payload.(Row)
	if !ok {
		return nil, acemq.Fatalf("acemq: CSVCodec cannot encode a %T", payload)
	}
	return []byte(row.String()), nil
}

func (CSVCodec) Decode(body []byte, dst any) error {
	row, ok := dst.(*Row)
	if !ok {
		return acemq.Fatalf("acemq: CSVCodec cannot decode into a %T", dst)
	}
	parsed, err := ParseRow(string(body))
	if err != nil {
		return acemq.Fatalf("acemq: this message is not a CSV row: %w", err)
	}
	*row = parsed
	return nil
}

func (CSVCodec) CanDecode(contentType string) bool {
	return strings.HasPrefix(contentType, "text/csv")
}
```

Two things worth getting right:

**Mark decoding failures fatal.** A body that will not decode will not decode
next time either. Without the mark it is retried until the attempts run out,
which delays the dead-lettering that was always going to happen and holds a
prefetch slot while it does.

**Decide what `CanDecode("")` should mean.** Answering for a message whose
sender set no content type is right for a default format and wrong for a
specialised one — it records traffic as a format nobody sent, and the mistake
surfaces much later.

## The registry

Configuration can name a format without the calling code importing it:

```go
acemq.RegisterCodec("csv", func() acemq.Codec { return CSVCodec{} })

codec, err := acemq.CodecByName(os.Getenv("ACEMQ_CODEC"))
if err != nil {
	return err
}
mq, err := acemq.Connect(ctx, url, acemq.WithCodec(codec))
```

`json` is registered by the package itself. An unknown name lists what is
available, because somebody who typoed one cannot see the registry:

```
acemq: no codec named "jsn" is registered; known: [csv json]
```

## Compared with the other libraries

Java and .NET ship Protocol Buffers, Avro, YAML and TOML codecs as separate
packages, so the core carries no serialization dependency. Go has only JSON so
far. The `Codec` interface is the extension point in the meantime, and a
message written by a Go publisher and read by a Java consumer needs both ends to
agree on the format either way.
