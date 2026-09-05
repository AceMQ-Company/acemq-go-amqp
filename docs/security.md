# Security

Three things: whether the connection is encrypted, which authority is trusted,
and what credentials are presented.

```go
import "github.com/AceMQ-Company/acemq-go-amqp/security"

mq, err := acemq.Connect(ctx, "amqps://broker.internal:5671/",
	acemq.WithSecurity(security.Required().
		TrustCertificateAuthorityFile("/etc/acemq/ca.crt").
		WithCredentials(security.EnvironmentCredentials("MQ_USER", "MQ_PASSWORD"))))
```

The `security` package has no dependencies outside the standard library and does
not import the rest of the library, so it can be built and reasoned about on its
own.

## The three modes

They are named rather than being a boolean, because somebody reading a
configuration should not have to work out which meaning a `false` carries.

| | |
|---|---|
| `security.Required()` | encrypt and verify. The one to use. |
| `security.Insecure()` | encrypt, accept any certificate |
| `security.Disabled()` | do not encrypt |

**`Insecure` is worth being precise about.** The traffic cannot be read by
somebody watching the network — but nothing stops that somebody from *being* the
broker, and everything you publish then goes to them. It is a step above
plaintext and a long way below verification. `TrustCertificateAuthority` is
barely more work.

**`Disabled` sends the password in the clear**, along with everything else.
Reasonable on a laptop; against a broker across a network you do not entirely
control, it is not.

### The URL has to agree

Asking for TLS and then connecting to `amqp://` would give a plaintext
connection with no warning at all — the quietest possible way to lose the thing
that was configured most deliberately. So it is refused before the socket opens:

```
acemq: security is set to required but the URL is amqp://, which is plaintext.
Use amqps://, or security.Disabled() if plaintext is what you meant
```

The reverse is refused too.

## Trusting one authority

```go
security.Required().TrustCertificateAuthorityFile("/etc/acemq/ca.crt")
```

This **replaces** the system trust store rather than adding to it. That is the
point: a broker holding a certificate from a public authority is not your
broker, and the hundreds of authorities a machine trusts by default are hundreds
of ways to be wrong.

A certificate from any other authority is refused, and there is a test that
proves it — the line between "encrypted" and "encrypted to the right party" is
exactly the thing that looks fine until somebody is on the network.

`TrustCertificateAuthority` takes an already-parsed `*x509.Certificate` when the
authority comes from somewhere other than a file.

### When the address is not the name

```go
security.Required().
	TrustCertificateAuthorityFile("ca.crt").
	WithServerName("broker.internal")
```

For connecting through an IP address, a tunnel, or a container's internal name.

### Client certificates

```go
security.Required().
	TrustCertificateAuthorityFile("ca.crt").
	WithClientCertificateFiles("client.crt", "client.key")
```

For a broker using `EXTERNAL` authentication rather than a password.

## Development certificates

Everything `acemq-certs` writes is stamped:

```
ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
```

A certificate carrying that mark is **refused on every path**, including
`Insecure`, unless you say otherwise:

```go
security.Required().
	TrustCertificateAuthorityFile("certs/ca.crt").
	AllowDevelopmentCertificates()
```

The refusal under `Insecure` is the one that matters most. `Insecure` is reached
for by somebody who could not get verification working — which is exactly the
person most likely to be pointing at a development broker, and later to leave
the setting in place against a real one.

Calling `AllowDevelopmentCertificates` is a deliberate statement that this
process is not production. That is a statement worth making hard to make by
accident.

The mark is looked for on the subject *and* the issuer, and on every certificate
in the chain, so a development authority signing an ordinarily-named broker
certificate is caught too.

### Generating them

```bash
go install github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@latest
acemq-certs --out certs --broker localhost --days 30
```

```
certs/ca.crt         trust this
certs/ca.key         the authority's key
certs/server.crt     the broker's certificate
certs/server.key     the broker's key
certs/client.crt     for EXTERNAL authentication
certs/client.key     the client's key
certs/rabbitmq.conf  mount at /etc/rabbitmq/rabbitmq.conf
```

The command prints the `docker run` that serves them and the Go that connects to
it. Keys are written readable only by their owner: a development key is still a
key.

Thirty days by default, deliberately short. A development certificate that lasts
a year is one that ends up somewhere it should not; regenerating is a second's
work.

A certificate for `localhost` also covers `127.0.0.1` and `::1`, because a broker
reached by one name is very often reached by another and a certificate covering
only one fails in a way that reads like a trust problem rather than a naming one.

## Credentials

```go
security.Required().
	TrustCertificateAuthorityFile("ca.crt").
	WithCredentials(security.EnvironmentCredentials("MQ_USER", "MQ_PASSWORD"))
```

Credentials given here **override whatever the URL carried**, which is how a
password stays out of the URL, and so out of logs, error messages and process
listings.

| | |
|---|---|
| `StaticCredentials(user, pass)` | the same every time. Fine for development. |
| `EnvironmentCredentials(userVar, passVar)` | read from the environment on every connection |
| `FileCredentials(path, user)` | read from a file on every connection |
| `CredentialsFunc(fn)` | anything else — a secret manager, a token exchange |

They are read **at the moment a connection is made**, not once at start-up. A
password rotated by a sidecar is only useful if something asks for it again.

`FileCredentials` is how a mounted Kubernetes or Docker secret arrives. The file
holds the password alone, or `username:password` when it carries both. Trailing
whitespace is trimmed — every editor adds a newline, and a password with one on
the end fails in a way nobody enjoys diagnosing.

### The secret does not print

```go
creds := security.Of("orders", "hunter2")
fmt.Println(creds)          // Credentials{orders}
fmt.Println(sec)            // security.Options{mode=required, credentials=provided}
```

That is the whole reason `Credentials` is a type rather than two strings. A
struct printed with `%v`, a connection logged on failure, an error wrapping the
configuration — none of them should put a password somewhere a colleague can
read it. There is a test asserting the secret never appears, including in the
error from a failed login.

## A production checklist

- `security.Required()`, never `Insecure` and never `Disabled`.
- `TrustCertificateAuthorityFile` naming your own authority, not the system store.
- Credentials from the environment, a file or a secret manager — not the URL and
  not the source.
- `AllowDevelopmentCertificates` **not** called. If it is, something is wrong.
- The broker account has the narrowest permissions that do the job. This library
  does not manage broker users; that is the broker's own configuration.
- Message bodies are not encrypted by this library. If the payload needs
  protecting at rest inside the broker, encrypt it before publishing — the Java
  library's `EncryptedCodec` has no Go equivalent yet.

## What this library does not do

It secures the connection. It does not manage broker users or permissions, hold
your keys, encrypt message bodies, or decide who may publish what.

Reporting a problem: [SECURITY.md](https://github.com/AceMQ-Company/acemq-go-amqp/blob/main/SECURITY.md).
