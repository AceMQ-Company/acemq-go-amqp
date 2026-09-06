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

// ContentType is what an encrypted message carries, and what Java and .NET
// write.
const ContentType = "application/vnd.acemq.encrypted"

// KeySize is the length of a key in bytes. AES-256.
const KeySize = 32

// Key is one encryption key.
type Key struct {
	// ID names the key on the wire, so a message says which key opens it
	// without saying anything about the key itself. Something datelike —
	// "2026-01" — reads well when somebody is working out what to rotate.
	ID string

	// Secret is 32 bytes. Anything else is refused rather than padded or
	// hashed into shape, because both would make a weak key look like a strong
	// one.
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
	if len(key.Secret) != KeySize {
		return fmt.Errorf(
			"acemq: key %q is %d bytes; it must be exactly %d. "+
				"Padding or hashing a short key here would make a weak one look strong",
			key.ID, len(key.Secret), KeySize)
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
//	[1 byte version][2 bytes key id length][key id][12 byte nonce][ciphertext]
//
// The key id travels in the clear, which is the point: a consumer has to know
// which key to try before it can decrypt anything. It names a key rather than
// revealing one.
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
// A body that will not decrypt is fatal: the same bytes fail the same way every
// time, whether they were tampered with, encrypted with a key this process does
// not have, or were never encrypted at all.
func (c *Codec) Decode(body []byte, dst any) error {
	keyID, rest, err := unframe(body)
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
	if len(rest) < nonceSize {
		return acemq.Fatalf("acemq: this message is too short to be encrypted with %q", keyID)
	}

	header := frame(keyID)
	plaintext, err := gcm.Open(nil, rest[:nonceSize], rest[nonceSize:], header)
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
// messages to whoever holds the key.
func KeyIDOf(body []byte) (string, error) {
	id, _, err := unframe(body)
	return id, err
}

const formatVersion = 1

func frame(keyID string) []byte {
	out := make([]byte, 0, 3+len(keyID))
	out = append(out, formatVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(keyID)))
	return append(out, keyID...)
}

func unframe(body []byte) (string, []byte, error) {
	if len(body) < 3 {
		return "", nil, acemq.Fatalf("acemq: this message is too short to be encrypted")
	}
	if body[0] != formatVersion {
		return "", nil, acemq.Fatalf(
			"acemq: this message uses encryption format %d and this library writes %d",
			body[0], formatVersion)
	}

	idLen := int(binary.BigEndian.Uint16(body[1:3]))
	if len(body) < 3+idLen {
		return "", nil, acemq.Fatalf("acemq: the key id on this message is truncated")
	}
	return string(body[3 : 3+idLen]), body[3+idLen:], nil
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
