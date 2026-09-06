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

4. **Tag and push.**

   ```bash
   git tag -a v0.1.0 -m "..."
   git push origin v0.1.0
   ```

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

## The version line

`0.1.x` until somebody decides otherwise. The release workflow refuses anything
else, so moving the line is a deliberate edit rather than a typo in a tag.
