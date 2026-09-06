# Releasing

Go publishes by tag. There is no artifact to upload: the module proxy and
pkg.go.dev pick a version up on their own, and once they have, **that version is
permanent** — the proxy caches it and a deleted tag does not un-publish
anything. So everything that can be checked is checked before the tag exists.

## Before tagging

1. **Set the version constant.** `acemq.Version` in `amqp/interceptors.go`
   is `"dev"` on main. A released library that reports `dev` tells an operator
   nothing about what is running.

   ```go
   const Version = "0.1.0"
   ```

2. **Point the nested modules at the version being released.** Each codec module
   and `patterns/sqltest` has its own `go.mod` requiring the parent:

   ```
   require github.com/AceMQ-Company/acemq-go-amqp v0.1.0
   ```

   The `replace` line beside it is for local development and is **ignored by
   anyone importing the module**, so the `require` has to name a version that
   really exists. A codec requiring `v0.0.0` fails for consumers with an error
   that reads like a proxy problem.

3. **Commit both**, and let CI go green.

4. **Tag and push the root module.**

   ```bash
   git tag -a v0.1.0 -m "..."
   git push origin v0.1.0
   ```

5. **Tag every nested module too.** This is the step that is easy to miss, and
   the failure it causes looks like something else entirely.

   A nested module is a module in its own right, and Go publishes it by a tag
   carrying its path:

   ```bash
   for m in codec/yaml codec/toml codec/protobuf codec/avro; do
     git tag -a "$m/v0.1.0" v0.1.0^{} -m "AceMQ for Go $m 0.1.0"
   done
   git push origin codec/yaml/v0.1.0 codec/toml/v0.1.0 \
                   codec/protobuf/v0.1.0 codec/avro/v0.1.0
   ```

   Tagged at the same commit as the root, so the version numbers mean the same
   thing. Without these tags `go get .../codec/yaml@v0.1.0` fails with

   ```
   module github.com/AceMQ-Company/acemq-go-amqp@v0.1.0 found,
   but does not contain package .../codec/yaml
   ```

   which reads like the package is missing from the release rather than like a
   tag nobody pushed. This was discovered by trying to consume 0.1.0 rather
   than by reading the workflow, which is the only way it would have been found.

   `patterns/sqltest` is not tagged: it holds tests and nothing imports it.

The release workflow then verifies the tag: that it is a `0.1.x` version, that
`go.mod` still targets Go 1.23, that `acemq.Version` matches the tag, that every
codec module builds, that the nested modules require the version being released,
and that the whole suite passes against a real broker with nothing skipped.

It runs **after** the tag is pushed, because that is when a tag event happens.
It cannot prevent a bad release, only tell you loudly that you have made one —
which is why the checks above are worth doing first rather than relying on it.

## After tagging

`pkg.go.dev` indexes on first request. Fetching the module is enough:

```bash
GOPROXY=https://proxy.golang.org go list -m github.com/AceMQ-Company/acemq-go-amqp@v0.1.0
```

Then check a consumer can actually use it — a fresh module, no `replace`, no
local paths:

```bash
go get github.com/AceMQ-Company/acemq-go-amqp@v0.1.0
go get github.com/AceMQ-Company/acemq-go-amqp/codec/yaml@v0.1.0
```

The proxy caches a negative answer, so a module fetched **before** its tag
existed keeps failing locally for a few minutes after the tag is pushed. Confirm
against the proxy itself rather than trusting a local error:

```bash
curl -s "https://proxy.golang.org/github.com/!ace!m!q-!company/acemq-go-amqp/codec/yaml/@v/list"
```

## The version line

`0.1.x` until somebody decides otherwise. The release workflow refuses anything
else, so moving the line is a deliberate edit rather than a typo in a tag.
