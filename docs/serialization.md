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

## The other formats

Every format the Java and .NET libraries have. Each is a **module of its own**,
so the core keeps its single dependency and an application takes only what it
sends:

```bash
go get github.com/AceMQ-Company/acemq-go-amqp/codec/yaml
```

| Module | Format | Brings |
|---|---|---|
| `codec/xml` | XML | nothing — standard library |
| `codec/yaml` | YAML | `gopkg.in/yaml.v3` |
| `codec/toml` | TOML | `github.com/BurntSushi/toml` |
| `codec/protobuf` | Protocol Buffers | `google.golang.org/protobuf` |
| `codec/avro` | Avro | `github.com/hamba/avro/v2` |
| `crypto` | encryption | nothing — standard library |

**None of them answers for a message with no content type.** JSON is the default
and gets that benefit of the doubt; the others would be recording traffic under
a format nobody sent.

### YAML and TOML

Both are for a message a person reads and edits. YAML writes block style, which
is the only reason to pay for it over JSON. TOML has no **Norway problem**: in
YAML `country: NO` is the boolean false, while in TOML an unquoted `NO` is a
parse error rather than a country that quietly became `false`.

TOML is a **table format**, so a body must be a struct or a map at the top level.
The encoder underneath will happily write a bare list as `["a", "b"]` — which is
not a TOML document — so this codec refuses it before it goes out rather than
letting it fail in the consumer with no clue where it came from.

### Avro, and schema evolution

Two modes. `avro.Of(schema)` carries nothing on the wire and expects both ends
to hold the same schema: smallest, and most brittle.

```go
codec, err := avro.Registered(registry, "order.placed", schema)
```

`Registered` frames each message with a schema identifier the way Confluent's
clients do — one zero byte, four bytes of identifier, big-endian — which is what
lets Confluent's clients, the Java library and this one read each other. A
producer can then add a field with a default and a consumer that has never heard
of it still reads the message. There is a test for exactly that.

### Encryption

```go
keyring, err := crypto.NewKeyring(crypto.Key{ID: "2026-01", Secret: secret})
codec := crypto.Wrap(acemq.JSONCodec{}, keyring)
```

The payload is encoded as usual and the bytes are then encrypted with AES-GCM,
so the broker, its disk, its backups and its management interface see
ciphertext. GCM authenticates as well as encrypts, so a body altered in the
broker fails to open rather than decrypting into something else.

**Headers travel in the clear.** The envelope is how the library routes and
retries, so it cannot be encrypted without the broker losing the ability to do
its job. Do not put anything secret in a header.

A keyring holds more than one key because rotation needs an overlap: add the new
key everywhere first so every consumer can read it, then make it current. A
keyring with one key cannot rotate without an outage.

A short key is refused rather than padded or hashed into shape — both would make
a weak key look like a strong one.

## Several formats on one queue

```go
codec := acemq.NewCompositeCodec(acemq.JSONCodec{}, yaml.Codec{})
```

The first is what it writes; all of them are offered a message to read, and the
first that claims the content type does. For a migration, or a queue several
producers write to in different formats.
