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

package patterns

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// The claim check's framing, which is the wire contract every AceMQ library
// shares.
//
//	0xAC  0x01  0x00  payload   inline, and identical to what the delegate wrote
//	0xAC  0x01  0x01  key       a claim check
//
// Three bytes, and the third is how a consumer decides whether it is holding a
// payload or a reference to one. Nothing else distinguishes them, which is why
// the framing rather than a header is the contract: a header can be stripped by
// a shovel or a federation link, and the body cannot.
//
// The key is the store's key as bare UTF-8 — not a URI, not a scheme, nothing
// wrapped around it. A Go consumer pointed at the same store therefore reads a
// document a Java, Python or Ruby publisher checked in, and the other way round.
const (
	// claimMagic marks the framing as this pattern's, so a body that was never
	// framed is not mistaken for one that was.
	claimMagic = 0xAC

	// claimVersion is the version byte this library writes, and the only one it
	// reads. A second version can be added without every consumer being
	// redeployed the same afternoon.
	claimVersion = 0x01

	// claimInline says the payload is in the message.
	claimInline = 0x00

	// claimChecked says the message carries a key and the payload is in the store.
	claimChecked = 0x01

	// claimHeaderBytes is how many bytes the framing takes.
	claimHeaderBytes = 3
)

// DefaultClaimCheckThreshold is the size at or above which a payload is
// offloaded.
//
// 64 KiB: comfortably above an ordinary event and comfortably below the size at
// which a broker starts to care. RabbitMQ will accept far larger, which is the
// problem — nothing refuses a 40 MB message, it simply makes everything worse
// afterwards.
const DefaultClaimCheckThreshold = 64 * 1024

// ClaimCheckStore is where a payload too large for a broker actually goes.
//
// Three methods, so a store in front of S3, Azure Blob Storage, a filesystem or
// a database table is a small type. Nothing here knows about messaging: the
// store holds bytes under a key and hands them back, and [ClaimCheck] is what
// turns that into a claim check on the wire.
//
// # No context, and it is not an oversight
//
// [acemq.Codec] takes no context — Encode and Decode are called from inside the
// publish and delivery paths, which own theirs — so there is none to pass on. A
// store doing network I/O should carry its own timeout, because without one a
// hung object store hangs a consumer's whole delivery.
//
// # Retention is the part that goes wrong
//
// The store and the queue have different lifetimes, and nothing enforces a
// relationship between them. A message replayed a month later carries a key, and
// if the store expired that key the replay produces a message nobody can read —
// worse than a lost message, because it looks like a message and fails deep
// inside a consumer rather than visibly.
//
// So the store's retention must exceed every retention that could bring a
// message back: queue TTLs, dead-letter queues, and however long somebody might
// sit on a message before replaying it by hand. When in doubt, longer.
//
// An implementation is shared by every publisher and consumer on a connection,
// so it has to be safe to call from several goroutines.
type ClaimCheckStore interface {
	// Put stores a payload and returns the key the message will carry, which
	// must be unique for the life of the store.
	Put(content []byte) (string, error)

	// Get redeems a claim check. found is false when the store no longer holds
	// the key, which is retention having expired underneath a message that
	// outlived it — an answer rather than a failure, and told apart from err so
	// that the two get different messages.
	Get(key string) (content []byte, found bool, err error)

	// Delete removes a payload.
	//
	// Not called by the codec. Deleting on read would break a second consumer of
	// the same message, and deleting on acknowledgement would break a replay — so
	// when a payload may be removed is a retention decision, and retention
	// decisions belong to whoever owns the data.
	Delete(key string) error
}

// ClaimCheckOption configures a [ClaimCheckCodec].
type ClaimCheckOption func(*ClaimCheckCodec)

// OffloadAbove sets the size at or above which a payload goes to the store,
// instead of [DefaultClaimCheckThreshold].
//
// Zero offloads everything, which is occasionally what a store-backed audit
// trail wants.
func OffloadAbove(bytes int) ClaimCheckOption {
	return func(c *ClaimCheckCodec) { c.threshold = bytes }
}

// ClaimCheckCodec keeps large payloads off the broker.
//
//	store := patterns.NewFilesystemClaimCheckStore("/mnt/claims")
//	checked := patterns.ClaimCheck(acemq.JSONCodec{}, store)
//
//	mq, err := acemq.Connect(ctx, url, acemq.WithCodec(checked))
//
// A scanned medical report is tens of megabytes. Putting it on a queue is
// possible and is a mistake: it fills the broker's memory, it is copied to every
// bound queue, it makes a dead-letter queue impossible to inspect, and it turns a
// broker into a filesystem with worse tools. What travels instead is a claim
// check — the payload goes to a [ClaimCheckStore], and the message carries the
// key.
//
// # Only when it is worth it
//
// Below the threshold the payload travels inline, exactly as it would without
// this codec. That matters more than it sounds: offloading a two-hundred-byte
// message turns one broker round trip into a store round trip and a broker round
// trip, so an unconditional claim check makes the common case slower to fix the
// rare one.
//
// The framing therefore says which of the two it is, and a consumer handles both
// without being told. That is what allows the threshold to be changed, or this
// codec to be introduced, without a flag day: messages written before the change
// are still readable after it.
//
// # The content type is the delegate's
//
// Unchanged, unlike encryption, where the bytes really are something else. A
// claim-checked message is still a document; it is a document that is somewhere
// else, and a consumer that lacks the store gets a clear failure rather than a
// parser error.
//
// # It does not write x-acemq-claim
//
// That header is reserved for an application that wants to say where a payload
// went, so an operator reading a dead-letter queue can see it without decoding
// anything. Python and Ruby reserve it the same way. This codec frames the body
// instead, because a header can be stripped by a shovel or a federation link and
// because the framing has to distinguish inline from checked — which a header
// that is present or absent cannot do for a message written before the codec
// existed. Use [ClaimKeyOf] to read the key out of a body without fetching it.
type ClaimCheckCodec struct {
	delegate  acemq.Codec
	store     ClaimCheckStore
	threshold int
}

// ClaimCheck wraps a codec so payloads at or above the threshold go to the store
// and the message carries the key.
func ClaimCheck(
	delegate acemq.Codec, store ClaimCheckStore, opts ...ClaimCheckOption,
) *ClaimCheckCodec {
	c := &ClaimCheckCodec{
		delegate:  delegate,
		store:     store,
		threshold: DefaultClaimCheckThreshold,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.threshold < 0 {
		c.threshold = 0
	}
	return c
}

// Delegate is the wrapped codec, whose output is what gets stored or inlined.
func (c *ClaimCheckCodec) Delegate() acemq.Codec { return c.delegate }

// Threshold is the size at or above which a payload is offloaded.
func (c *ClaimCheckCodec) Threshold() int { return c.threshold }

// ContentType is the delegate's. A claim-checked document is still a document.
func (c *ClaimCheckCodec) ContentType() string { return c.delegate.ContentType() }

// CanDecode is whatever the delegate accepts. A claim check does not change what
// the message is.
func (c *ClaimCheckCodec) CanDecode(contentType string) bool {
	return c.delegate.CanDecode(contentType)
}

// Encode writes the payload, offloading it when it is large enough to be worth
// it.
func (c *ClaimCheckCodec) Encode(payload any) ([]byte, error) {
	encoded, err := c.delegate.Encode(payload)
	if err != nil {
		return nil, err
	}
	// Strictly less than, so a threshold of zero offloads everything and a
	// payload exactly at the threshold is offloaded. The same comparison in
	// Java, Python and Ruby, which matters: a payload sitting on the boundary
	// must not be inline from one library and checked from another.
	if len(encoded) < c.threshold {
		return claimFrame(claimInline, encoded), nil
	}

	key, err := c.store.Put(encoded)
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot put a payload in the claim-check store: %w", err)
	}
	if key == "" {
		return nil, acemq.Fatalf(
			"acemq: the claim-check store returned an empty key, which no message could" +
				" ever redeem")
	}
	return claimFrame(claimChecked, []byte(key)), nil
}

// Decode reads a body, fetching the payload when it was offloaded.
func (c *ClaimCheckCodec) Decode(body []byte, dst any) error {
	return c.decode(body, dst, func(b []byte, d any) error { return c.delegate.Decode(b, d) })
}

// DecodeAs reads a body using the delegate's content-type-aware decoding, so
// wrapping an [acemq.CompositeCodec] works as well as wrapping a plain codec.
//
// The consumer looks for this method rather than calling Decode when a codec has
// it. Without it, wrapping a composite would reach a Decode that cannot choose
// and fails by design.
func (c *ClaimCheckCodec) DecodeAs(contentType string, body []byte, dst any) error {
	return c.decode(body, dst, func(b []byte, d any) error {
		if composite, ok := c.delegate.(interface {
			DecodeAs(string, []byte, any) error
		}); ok {
			return composite.DecodeAs(contentType, b, d)
		}
		return c.delegate.Decode(b, d)
	})
}

func (c *ClaimCheckCodec) decode(body []byte, dst any, delegate func([]byte, any) error) error {
	if !claimFramed(body) {
		// Written before this codec was introduced, or by a publisher that does
		// not use it. Reading it as the delegate would is the only useful answer,
		// and it is what makes adding a claim check to a live queue safe.
		return delegate(body, dst)
	}

	rest := body[claimHeaderBytes:]
	if body[2] == claimInline {
		return delegate(rest, dst)
	}

	key := string(rest)
	content, found, err := c.store.Get(key)
	if err != nil {
		// The store failed rather than answered. Retryable, unlike a key it does
		// not hold: an object store that timed out may well answer next time.
		return fmt.Errorf("acemq: cannot read the claim check %q from the store: %w", key, err)
	}
	if !found {
		// Fatal rather than retryable, and said at length because the cause is
		// never where somebody looks first.
		return acemq.Fatalf(
			"acemq: the claim check %q is not in the store, so this message cannot be"+
				" read. The payload was removed while a message referring to it was still"+
				" deliverable -- the store's retention has to outlast every queue, every"+
				" dead-letter queue, and any replay somebody might do by hand", key)
	}
	return delegate(content, dst)
}

func (c *ClaimCheckCodec) String() string {
	return fmt.Sprintf("ClaimCheckCodec{%T, above %d bytes}", c.delegate, c.threshold)
}

// ClaimKeyOf reads the key a message refers to, without fetching it.
//
// For the operator looking at a dead-letter queue: which object does this need,
// and is it still in the store? Answering that from the message alone is the
// difference between a five-minute check and restoring a backup.
//
// Empty when the payload travelled inline or this codec did not write the
// message.
func ClaimKeyOf(body []byte) string {
	if !claimFramed(body) || body[2] != claimChecked {
		return ""
	}
	return string(body[claimHeaderBytes:])
}

// IsClaimChecked reports whether a body carries a key rather than a payload.
func IsClaimChecked(body []byte) bool {
	return claimFramed(body) && body[2] == claimChecked
}

func claimFramed(body []byte) bool {
	return len(body) >= claimHeaderBytes && body[0] == claimMagic && body[1] == claimVersion &&
		(body[2] == claimInline || body[2] == claimChecked)
}

func claimFrame(kind byte, rest []byte) []byte {
	framed := make([]byte, claimHeaderBytes+len(rest))
	framed[0] = claimMagic
	framed[1] = claimVersion
	framed[2] = kind
	copy(framed[claimHeaderBytes:], rest)
	return framed
}

// InMemoryClaimCheckStore holds payloads in a map.
//
// Not for production, and the reason is the point of the pattern. The payloads
// are in the publisher's own memory — which is where they were going to be
// anyway, so this takes them off the broker and does nothing else. A claim check
// that does not outlive the process that wrote it is a message nobody else can
// read, and every consumer in another process gets "the claim check is not in
// the store". It is lost on restart too, which turns every message still in a
// queue into one that can never be read.
//
// It is genuinely useful in a test, where the publisher and the consumer are the
// same process and the thing being proved is the framing rather than the
// storage. Anything else wants a store the processes share.
type InMemoryClaimCheckStore struct {
	mu       sync.Mutex
	contents map[string][]byte
}

// NewInMemoryClaimCheckStore returns an empty store.
func NewInMemoryClaimCheckStore() *InMemoryClaimCheckStore {
	return &InMemoryClaimCheckStore{contents: map[string][]byte{}}
}

// Put stores a payload.
func (s *InMemoryClaimCheckStore) Put(content []byte) (string, error) {
	key, err := newClaimKey()
	if err != nil {
		return "", err
	}
	// Copied, because the caller owns the slice it handed over and a codec is
	// entitled to reuse a buffer. A store that keeps somebody else's slice is a
	// store whose contents change after they were stored.
	held := make([]byte, len(content))
	copy(held, content)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.contents[key] = held
	return key, nil
}

// Get redeems a claim check.
func (s *InMemoryClaimCheckStore) Get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	held, ok := s.contents[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(held))
	copy(out, held)
	return out, true, nil
}

// Delete removes a payload.
func (s *InMemoryClaimCheckStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.contents, key)
	return nil
}

// Len is how many payloads are held.
func (s *InMemoryClaimCheckStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.contents)
}

// Clear empties the store, which is what a test between cases wants.
func (s *InMemoryClaimCheckStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contents = map[string][]byte{}
}

func (s *InMemoryClaimCheckStore) String() string {
	return fmt.Sprintf("InMemoryClaimCheckStore{%d held}", s.Len())
}

// safeClaimKey is what a key must look like to become a path segment.
//
// A key reaches the filesystem as one, so it is checked rather than trusted.
// Every key this store issues is a UUID; one arriving from a message is whatever
// a publisher put there, and ../../etc/passwd is a key too.
var safeClaimKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// FilesystemClaimCheckStore holds payloads in a directory.
//
//	store := patterns.NewFilesystemClaimCheckStore("/mnt/claims")
//
// Useful where the filesystem is shared and durable — an NFS mount, a persistent
// volume — and the honest middle ground between a map and object storage. On a
// container's local disk it is [InMemoryClaimCheckStore] with extra steps: the
// consumer is on another host and finds nothing.
//
// Object storage is the usual right answer, and a store in front of S3 or Azure
// Blob Storage is three short methods. This one exists because "write it to the
// mount everything already has" is a real deployment and not a bad one.
//
// # Writes are atomic
//
// The payload is written to a temporary file and renamed into place. Without
// that, a consumer fast enough to read the key before the writer finished gets a
// truncated payload and a parse error somewhere unhelpful — and messaging is
// exactly the arrangement that makes a consumer that fast normal rather than
// unlikely. A rename within one directory is atomic, so a reader sees the whole
// payload or no payload.
type FilesystemClaimCheckStore struct {
	directory string
}

// NewFilesystemClaimCheckStore writes payloads to a directory, creating it when
// it is not there.
//
// The directory is created lazily, on the first Put, so building a store cannot
// fail and a consumer that only ever reads does not need write permission.
func NewFilesystemClaimCheckStore(directory string) *FilesystemClaimCheckStore {
	return &FilesystemClaimCheckStore{directory: directory}
}

// Directory is where payloads are written.
func (s *FilesystemClaimCheckStore) Directory() string { return s.directory }

// Put stores a payload, atomically.
func (s *FilesystemClaimCheckStore) Put(content []byte) (string, error) {
	if err := os.MkdirAll(s.directory, 0o755); err != nil {
		return "", fmt.Errorf("acemq: cannot create the claim-check directory %s: %w",
			s.directory, err)
	}
	key, err := newClaimKey()
	if err != nil {
		return "", err
	}

	staging, err := os.CreateTemp(s.directory, key+".*.partial")
	if err != nil {
		return "", fmt.Errorf("acemq: cannot write a claim-check payload in %s: %w",
			s.directory, err)
	}
	name := staging.Name()

	if _, err := staging.Write(content); err != nil {
		_ = staging.Close()
		_ = os.Remove(name)
		return "", fmt.Errorf("acemq: cannot write the claim-check payload %s: %w", key, err)
	}
	if err := staging.Close(); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("acemq: cannot write the claim-check payload %s: %w", key, err)
	}

	// Moved into place rather than written in place, so a reader either finds
	// the whole payload or finds nothing.
	if err := os.Rename(name, filepath.Join(s.directory, key)); err != nil {
		_ = os.Remove(name)
		return "", fmt.Errorf("acemq: cannot move the claim-check payload %s into place: %w",
			key, err)
	}
	return key, nil
}

// Get redeems a claim check.
func (s *FilesystemClaimCheckStore) Get(key string) ([]byte, bool, error) {
	path, err := s.pathFor(key)
	if err != nil {
		return nil, false, err
	}

	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// Absent rather than failed: a key the store no longer holds is a
		// retention answer, and the codec turns it into a message that explains
		// itself.
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("acemq: cannot read the claim-check payload %s: %w",
			key, err)
	}
	return content, true, nil
}

// Delete removes a payload.
func (s *FilesystemClaimCheckStore) Delete(key string) error {
	path, err := s.pathFor(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("acemq: cannot delete the claim-check payload %s: %w", key, err)
	}
	return nil
}

func (s *FilesystemClaimCheckStore) pathFor(key string) (string, error) {
	if !safeClaimKey.MatchString(key) {
		// Fatal, and it says why: a key arriving from a message is whatever a
		// publisher put there. This is not a transient failure and will not
		// become one on the fourth attempt, so a message carrying it has to stop
		// rather than circle.
		return "", acemq.Fatalf(
			"acemq: %q is not a key this store issued. A key becomes a path segment,"+
				" so one arriving from a message is checked rather than trusted", key)
	}
	return filepath.Join(s.directory, key), nil
}

func (s *FilesystemClaimCheckStore) String() string {
	return "FilesystemClaimCheckStore{" + s.directory + "}"
}

// newClaimKey mints a UUIDv4, which is what every AceMQ store issues. It is what
// makes [safeClaimKey] a check rather than a restriction.
func newClaimKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Unlike a run identifier, a key that repeats overwrites somebody else's
		// payload, so there is no falling back to the clock here.
		return "", fmt.Errorf("acemq: cannot generate a claim-check key: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	var sb strings.Builder
	fmt.Fprintf(&sb, "%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	return sb.String(), nil
}
