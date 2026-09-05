# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do
not open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of
whether it is a vulnerability, what is affected, and a rough timeline within a week.
If a fix is warranted, we will tell you when it is released and credit you unless you
would rather we did not.

## What is in scope

Everything in this repository: the `amqp` package and the `rabbitmq` transport.

Things worth reporting even if they feel minor:

- A way to reach a broker without the certificate verification the `tls.Config`
  passed to `rabbitmq.Dial` asked for.
- Anything that renders a credential — including the password in a broker URL — into
  a log line, an error message, or a panic.
- A message body or header that reaches a handler after failing to decode.
- A header in the reserved `x-acemq-` namespace that reaches application code, or an
  application header that can impersonate one.
- A way to make the attempt counter stop advancing, since a retry limit that never
  trips is a queue that never drains.
- Anything in the in-memory transport that makes it accept what RabbitMQ would
  reject, because that would certify code that then fails in production.

## What is not

- **Message bodies not being encrypted.** This library does not encrypt payloads yet;
  the Java library's `EncryptedCodec` has no Go equivalent. Bring your own codec.
- **A broker URL containing a password.** That is how AMQP URLs work. Keep them out
  of source control and out of process listings.
- **Vulnerabilities in `github.com/rabbitmq/amqp091-go`** — report those upstream.
- **Vulnerabilities in RabbitMQ itself** — report those to Broadcom.
- Findings from a scanner with no demonstrated impact.

## Supported versions

Nothing has been released. The main branch is what exists, and a fix means a commit
on it. Once versions are tagged this section will say which of them get fixes.

## What this library does not do for you

It carries messages. It does not manage broker users or permissions, hold your keys,
encrypt payloads, or authenticate anything. TLS is configured by handing a
`*tls.Config` to `rabbitmq.Dial`, and what that config says is what you get.
