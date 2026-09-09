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

// Package crypto encrypts message bodies.
//
// It wraps another codec: the payload is encoded as usual and the bytes are
// then encrypted, so the broker, its disk, its backups and anybody reading its
// management interface see ciphertext.
//
//	keyring, err := crypto.NewKeyring(crypto.Key{ID: "2026-01", Secret: secret})
//	codec := crypto.Wrap(acemq.JSONCodec{}, keyring)
//	mq, err := acemq.Connect(ctx, url, acemq.WithCodec(codec))
//
// # Reading and writing with the other libraries
//
// The framing is the Java, Python and Ruby one byte for byte, so a Java producer
// and a Go consumer can share a key. See [ContentType] for the bytes, and the
// security guide for what to do about anything encrypted by this library up to
// v0.3.0, whose framing was Go's alone. .NET is the remaining exception: it uses
// AES-256-CBC with a separate HMAC rather than AES-GCM.
//
// # What this does not protect
//
// Headers travel in the clear. The envelope — identity, type, correlation,
// causation — is how the library routes and retries, so it cannot be encrypted
// without the broker losing the ability to do its job. Do not put anything
// secret in a header.
//
// It also does not authenticate the sender. Anybody holding the key can write a
// message this codec will happily decrypt, so a key is a shared secret between
// everyone who may publish and everyone who may read, and nothing more.
//
// Nothing outside the standard library is needed.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

// ContentType is what an encrypted message carries. Java, .NET, Python and Ruby
// write the same string.
//
// A message is framed as
//
//	[0xAE][1 byte version][1 byte key id length][key id][12 byte nonce][ciphertext+tag]
//
// which is byte for byte what Java, Python and Ruby write, so a body encrypted
// here opens there and theirs opens here, given the same key. .NET is the
// remaining exception: it does not use AES-GCM at all, but AES-256-CBC with a
// separate HMAC-SHA-256, and neither library can read the other's messages.
//
// The magic byte is the reason for the shape. Without it the first byte of a
// body is a version number small enough to be the first byte of a protobuf or
// an Avro record, and a consumer pointed at a plaintext queue would try to
// decrypt it and report the failure as a decryption problem. 0xAE is not a
// plausible first byte of anything else this library writes, so a body that
// does not start with it is refused as what it is.
const ContentType = "application/vnd.acemq.encrypted"

// magic marks the framing as this family's, so a body that was never encrypted
// is refused rather than misparsed.
const magic = 0xAE

// KeySize is the length of the key NewKey draws. AES-256.
//
// It is not the only length accepted: see Key.Secret.
const KeySize = 32

// MaxKeyIDBytes is the longest a key id may be, in UTF-8 bytes.
//
// It is one byte of framing in front of every message, so it is a name rather
// than a description.
const MaxKeyIDBytes = 255

// keySizes are the key lengths AES takes. 16 and 24 are accepted as well as 32
// because Java, Python and Ruby accept them, and a key that already opens
// messages there should not need re-issuing to be used here.
var keySizes = [...]int{16, 24, 32}

// Key is one encryption key.
type Key struct {
	// ID names the key on the wire, so a message says which key opens it
	// without saying anything about the key itself. Something datelike —
	// "2026-01" — reads well when somebody is working out what to rotate. At
	// most MaxKeyIDBytes bytes, because it travels in front of every message.
	ID string

	// Secret is 16, 24 or 32 bytes — the three lengths AES takes, and the same
	// three the other libraries accept. NewKey draws 32. Anything else is
	// refused rather than padded or hashed into shape, because both would make
	// a weak key look like a strong one.
	Secret []byte
}

// NewKey draws a fresh random key.
func NewKey(id string) (Key, error) {
	secret := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, secret); err != nil {
		return Key{}, fmt.Errorf("acemq: cannot generate a key: %w", err)
	}
	return Key{ID: id, Secret: secret}, nil
}

// String never includes the secret.
func (k Key) String() string { return "crypto.Key{" + k.ID + "}" }

// Keyring holds the keys a process can use.
//
// More than one, because rotation needs an overlap: new messages are written
// with the newest key while messages written with the old one are still being
// read. A keyring with one key cannot rotate without an outage.
type Keyring struct {
	mu      sync.RWMutex
	keys    map[string]Key
	current string
}

// NewKeyring builds a keyring. The first key is the one used for writing.
func NewKeyring(keys ...Key) (*Keyring, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("acemq: a keyring needs at least one key")
	}

	ring := &Keyring{keys: map[string]Key{}}
	for _, key := range keys {
		if err := ring.Add(key); err != nil {
			return nil, err
		}
	}
	ring.current = keys[0].ID
	return ring, nil
}

// Add puts a key on the ring without making it the one used for writing.
//
// The order rotation happens in: add the new key everywhere first, so every
// consumer can read it, and only then make it current somewhere.
func (r *Keyring) Add(key Key) error {
	if key.ID == "" {
		return fmt.Errorf("acemq: a key needs an id, so a message can say which one opens it")
	}
	if len(key.ID) > MaxKeyIDBytes {
		return fmt.Errorf(
			"acemq: key id %q is %d bytes and at most %d fit in the framing. "+
				"It travels in front of every message, so it is a name rather than a description",
			key.ID, len(key.ID), MaxKeyIDBytes)
	}
	if !isKeySize(len(key.Secret)) {
		return fmt.Errorf(
			"acemq: key %q is %d bytes; an AES key is 16, 24 or 32. "+
				"Padding or hashing a short key here would make a weak one look strong. "+
				"If it came from a passphrase it needs a key derivation function such as "+
				"PBKDF2, scrypt or Argon2 rather than being used as it stands",
			key.ID, len(key.Secret))
	}
	if strings.ContainsRune(key.ID, 0) {
		return fmt.Errorf("acemq: key id %q contains a null byte", key.ID)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[key.ID] = key
	if r.current == "" {
		r.current = key.ID
	}
	return nil
}

// Use makes a key the one new messages are written with.
func (r *Keyring) Use(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, present := r.keys[id]; !present {
		return fmt.Errorf("acemq: there is no key %q on this keyring", id)
	}
	r.current = id
	return nil
}

// Current is the key new messages are written with.
func (r *Keyring) Current() (Key, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, present := r.keys[r.current]
	if !present {
		return Key{}, fmt.Errorf("acemq: this keyring has no current key")
	}
	return key, nil
}

// Get looks a key up by id.
func (r *Keyring) Get(id string) (Key, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, present := r.keys[id]
	if !present {
		return Key{}, acemq.Fatalf(
			"acemq: this message was encrypted with key %q, which is not on this keyring (it holds %s). "+
				"Retrying will not help; add the key or dead-letter the message",
			id, strings.Join(r.ids(), ", "))
	}
	return key, nil
}

// IDs lists the keys on the ring, for a health endpoint or a log line at
// start-up. Never the secrets.
func (r *Keyring) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ids()
}

func (r *Keyring) ids() []string {
	out := make([]string, 0, len(r.keys))
	for id := range r.keys {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Codec encrypts what another codec produces.
type Codec struct {
	inner   acemq.Codec
	keyring *Keyring
}

// Wrap encrypts the output of another codec.
func Wrap(inner acemq.Codec, keyring *Keyring) *Codec {
	return &Codec{inner: inner, keyring: keyring}
}

// ContentType returns application/vnd.acemq.encrypted.
//
// The inner codec's type is not visible on the wire, because saying "this is
// encrypted JSON" tells an observer more than they need. A consumer knows what
// to expect from its own configuration.
func (c *Codec) ContentType() string { return ContentType }

// Inner is the codec whose output is encrypted.
func (c *Codec) Inner() acemq.Codec { return c.inner }

// Encode encodes and then encrypts.
//
// The wire format is:
//
//	[0xAE][1 byte version][1 byte key id length][key id][12 byte nonce][ciphertext+tag]
//
// The key id travels in the clear, which is the point: a consumer has to know
// which key to try before it can decrypt anything. It names a key rather than
// revealing one.
//
// This is the only framing this library writes. Bodies in the framing Go wrote
// up to v0.3.0 are still read — see [Codec.Decode] — but never written, because
// two writers is how a divergence survives being fixed.
func (c *Codec) Encode(payload any) ([]byte, error) {
	plaintext, err := c.inner.Encode(payload)
	if err != nil {
		return nil, err
	}

	key, err := c.keyring.Current()
	if err != nil {
		return nil, err
	}

	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("acemq: cannot generate a nonce: %w", err)
	}

	header := frame(key.ID)
	// The header is authenticated but not encrypted, so a key id changed in
	// flight makes the message fail to open rather than opening as something
	// else.
	sealed := gcm.Seal(nil, nonce, plaintext, header)

	out := make([]byte, 0, len(header)+len(nonce)+len(sealed))
	out = append(out, header...)
	out = append(out, nonce...)
	out = append(out, sealed...)
	return out, nil
}

// Decode decrypts and then decodes.
//
// Two framings are read. A body beginning 0xAE is the family framing, which is
// what this library and Java, Python and Ruby write. A body beginning 0x01 is
// the legacy Go framing, written by this library up to v0.3.0; it is read so
// that a queue filled before the change can be drained, and nothing writes it
// any more. Reading it goes away in v0.5.0. Anything else is refused.
//
// A body that will not decrypt is fatal: the same bytes fail the same way every
// time, whether they were tampered with, encrypted with a key this process does
// not have, or were never encrypted at all.
func (c *Codec) Decode(body []byte, dst any) error {
	keyID, header, sealed, err := unframe(body)
	if err != nil {
		return err
	}

	key, err := c.keyring.Get(keyID)
	if err != nil {
		return err
	}

	gcm, err := newGCM(key)
	if err != nil {
		return err
	}

	nonceSize := gcm.NonceSize()
	if len(sealed) < nonceSize+tagBytes {
		return acemq.Fatalf(
			"acemq: this message is too short to hold a nonce and an authentication tag, "+
				"so it was truncated between being written with key %q and being read", keyID)
	}

	// The header is authenticated but not encrypted, so a key id changed in
	// flight makes the message fail to open rather than opening as something
	// else. It is taken from the body rather than rebuilt, so the bytes
	// authenticated are the bytes that arrived.
	plaintext, err := gcm.Open(nil, sealed[:nonceSize], sealed[nonceSize:], header)
	if err != nil {
		// Deliberately vague about which of the possible causes it was.
		return acemq.Fatalf(
			"acemq: this message did not decrypt with key %q; it was altered, "+
				"or encrypted with a different key of the same name", keyID)
	}

	return c.inner.Decode(plaintext, dst)
}

// CanDecode accepts only the encrypted content type.
func (c *Codec) CanDecode(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(contentType), ContentType)
}

// KeyIDOf reads which key a message was encrypted with, without decrypting it.
//
// For working out why a message will not open, and for a tool that has to route
// messages to whoever holds the key. It reads both framings Decode reads.
func KeyIDOf(body []byte) (string, error) {
	id, _, _, err := unframe(body)
	return id, err
}

const (
	// formatVersion is the version byte this library writes, and the only one
	// it reads. A later framing is told apart from this one by the first two
	// bytes together.
	formatVersion = 0x01

	// prefixBytes is the magic, the version and the length byte.
	prefixBytes = 3

	// tagBytes is GCM's authentication tag, 128 bits. Java, Python and Ruby
	// write the same length, and a shorter one is only a cheaper forgery.
	tagBytes = 16
)

// frame is the header a message written with keyID carries: magic, version,
// one length byte and the id in UTF-8. These are the bytes ahead of the nonce
// and the bytes GCM authenticates.
func frame(keyID string) []byte {
	out := make([]byte, 0, prefixBytes+len(keyID))
	out = append(out, magic, formatVersion, byte(len(keyID)))
	return append(out, keyID...)
}

// unframe reads a body's header, whichever of the two framings it is in, and
// returns the key id, the header bytes exactly as they arrived — GCM's
// associated data — and everything after them.
func unframe(body []byte) (keyID string, header, sealed []byte, err error) {
	if len(body) >= prefixBytes && body[0] == magic {
		if body[1] != formatVersion {
			return "", nil, nil, acemq.Fatalf(
				"acemq: this message is encryption format %d and this library reads %d",
				body[1], formatVersion)
		}
		idLen := int(body[2])
		if idLen == 0 {
			return "", nil, nil, acemq.Fatalf(
				"acemq: this message carries no key id, so nothing can say which key opens it")
		}
		if len(body) < prefixBytes+idLen {
			return "", nil, nil, acemq.Fatalf("acemq: the key id on this message is truncated")
		}
		end := prefixBytes + idLen
		return string(body[prefixBytes:end]), body[:end], body[end:], nil
	}

	if len(body) >= legacyPrefixBytes && body[0] == legacyFormatVersion {
		return unframeLegacyGo(body)
	}

	return "", nil, nil, acemq.Fatalf(
		"acemq: this message was not written by crypto.Codec — it does not start with the " +
			"framing this codec writes. A consumer configured to decrypt has been pointed at " +
			"a queue carrying something else")
}

// The legacy Go framing.
//
// Up to v0.3.0 this library wrote
//
//	[1 byte version][2 bytes key id length, big-endian][key id][12 byte nonce][ciphertext+tag]
//
// with no magic byte, which no other AceMQ library could read. It is still read
// here so that a queue filled before the change can be drained by a consumer
// that has been upgraded, and it is deliberately never written: two writers is
// how a divergence survives being fixed.
//
// Deprecated: this is a migration affordance and goes away in v0.5.0. Drain or
// re-encrypt anything still holding these bodies before then. The two framings
// cannot be confused — the current one begins 0xAE and this one begins 0x01 —
// so removing it will refuse those bodies rather than misread them.
const (
	legacyFormatVersion = 0x01
	legacyPrefixBytes   = 3
)

// unframeLegacyGo reads the framing described above. Deprecated along with it.
func unframeLegacyGo(body []byte) (keyID string, header, sealed []byte, err error) {
	idLen := int(binary.BigEndian.Uint16(body[1:legacyPrefixBytes]))
	if idLen == 0 {
		return "", nil, nil, acemq.Fatalf(
			"acemq: this message carries no key id, so nothing can say which key opens it")
	}
	if len(body) < legacyPrefixBytes+idLen {
		return "", nil, nil, acemq.Fatalf("acemq: the key id on this message is truncated")
	}
	end := legacyPrefixBytes + idLen
	return string(body[legacyPrefixBytes:end]), body[:end], body[end:], nil
}

func isKeySize(n int) bool {
	for _, size := range keySizes {
		if n == size {
			return true
		}
	}
	return false
}

func newGCM(key Key) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key.Secret)
	if err != nil {
		return nil, fmt.Errorf("acemq: key %q cannot be used: %w", key.ID, err)
	}
	// GCM authenticates as well as encrypts, so a body altered in the broker
	// fails to open rather than decrypting into something else. Go has it in
	// the standard library, which is why this package needs nothing else.
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("acemq: cannot set up encryption with key %q: %w", key.ID, err)
	}
	return gcm, nil
}
