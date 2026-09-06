#!/usr/bin/env bash
# Run the release checks before the tag exists.
#
#   ./scripts/release-preflight.sh 0.1.5
#
# Go publishes by tag and the module proxy caches a version for ever, so a tag
# cannot be withdrawn or moved. That makes the release workflow the wrong place
# to find out that something was forgotten: by the time it says so, the version
# is public and the only way forward is another version. 0.1.1, 0.1.2 and 0.1.3
# were each burned that way -- the version constant, then a tag check, then the
# nested modules' parent requirement -- and every one of them is a check that
# could have run here in five seconds.
#
# Run this, fix whatever it names, commit, and only then tag.
set -euo pipefail

version="${1:-}"
if [ -z "$version" ]; then
  echo "usage: $0 <version without the leading v, e.g. 0.1.5>" >&2
  exit 2
fi
version="${version#v}"

cd "$(dirname "$0")/.."

nested_codecs="codec/yaml codec/toml codec/protobuf codec/avro"
nested_all="$nested_codecs patterns/sqltest"

failed=0
fail() { echo "  NO   $*"; failed=1; }
pass() { echo "  yes  $*"; }

echo "Preflight for v$version"

# --- the version this library reports ---------------------------------------

declared=$(grep -oE 'Version = "[^"]+"' amqp/interceptors.go | cut -d'"' -f2)
if [ "$declared" = "$version" ]; then
  pass "acemq.Version says $declared"
else
  fail "acemq.Version says \"$declared\", not \"$version\" (amqp/interceptors.go)"
fi

# --- the Go the module claims to support ------------------------------------

directive=$(grep -E '^go [0-9]' go.mod | awk '{print $2}')
if [ "$directive" = "1.23" ]; then
  pass "go.mod targets go $directive"
else
  fail "go.mod targets go $directive; releasing something that needs a newer toolchain breaks the promise the API rests on"
fi

# --- the nested modules -----------------------------------------------------
#
# A replace directive is ignored by anyone importing the module, so a codec
# resolves its parent by the require version. That version has to be this one.

for module in $nested_all; do
  required=$(grep -oE 'github.com/AceMQ-Company/acemq-go-amqp v[0-9][^ ]*' "$module/go.mod" | awk '{print $2}')
  if [ "$required" = "v$version" ]; then
    pass "$module requires the parent at $required"
  else
    fail "$module requires the parent at $required, not v$version"
  fi
done

# --- the tags that will have to exist ---------------------------------------
#
# Not an error yet: they are pushed with the release. This says what to push,
# because a nested module without its own tag fails with "module found, but
# does not contain package", which reads like the package was left out.

echo "  --   tag these together with v$version:"
for module in $nested_codecs; do
  echo "         $module/v$version"
done

# --- and it has to build and pass ------------------------------------------

echo "  ..   building and vetting every module"
go build ./... >/dev/null
go vet ./... >/dev/null
for module in $nested_all; do
  (cd "$module" && go build ./... >/dev/null && go vet ./... >/dev/null)
done
pass "every module builds and vets"

if [ -n "${ACEMQ_TEST_AMQP_URL:-}" ]; then
  echo "  ..   running the suite against $ACEMQ_TEST_AMQP_URL"
  go test -count=1 ./... >/dev/null
  pass "the suite passes against a real broker"
else
  echo "  --   ACEMQ_TEST_AMQP_URL is not set, so the broker tests were skipped."
  echo "       Set it before a release: the integration suite is the point of one."
fi

echo
if [ "$failed" -ne 0 ]; then
  echo "Not ready. Fix what is marked NO above, commit, then run this again."
  exit 1
fi

echo "Ready. Tag it:"
echo
for module in $nested_codecs; do
  echo "  git tag -a $module/v$version -m 'AceMQ for Go $module $version'"
done
echo "  git tag -a v$version   # with release notes"
echo "  git push origin $(for m in $nested_codecs; do printf '%s/v%s ' "$m" "$version"; done)"
echo "  git push origin v$version"
