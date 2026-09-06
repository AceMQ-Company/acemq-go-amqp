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

package crypto

import (
	"bytes"
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type Order struct {
	OrderID string `json:"orderId"`
	Card    string `json:"card"`
}

func keyring(t *testing.T, ids ...string) *Keyring {
	t.Helper()
	keys := make([]Key, 0, len(ids))
	for _, id := range ids {
		key, err := NewKey(id)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	ring, err := NewKeyring(keys...)
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func TestARoundTrip(t *testing.T) {
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))

	encrypted, err := codec.Encode(Order{OrderID: "o-1", Card: "4111111111111111"})
	if err != nil {
		t.Fatal(err)
	}

	var back Order
	if err := codec.Decode(encrypted, &back); err != nil {
		t.Fatal(err)
	}
	if back.OrderID != "o-1" || back.Card != "4111111111111111" {
		t.Errorf("decoded %+v", back)
	}
}

// TestThePayloadIsNotReadableOnTheWire is the whole point.
func TestThePayloadIsNotReadableOnTheWire(t *testing.T) {
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))

	encrypted, err := codec.Encode(Order{OrderID: "o-1", Card: "4111111111111111"})
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(encrypted, []byte("4111111111111111")) {
		t.Error("the card number is in the ciphertext")
	}
	if bytes.Contains(encrypted, []byte("orderId")) {
		t.Error("the field names are in the ciphertext")
	}
	// The key id is deliberately visible: a consumer must know which key to
	// try before it can decrypt anything.
	if !bytes.Contains(encrypted, []byte("2026-01")) {
		t.Error("the key id is not on the message, so nothing could open it")
	}
}

func TestAnAlteredMessageWillNotOpen(t *testing.T) {
	// GCM authenticates as well as encrypts, so tampering fails rather than
	// decrypting into something else.
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))

	encrypted, err := codec.Encode(Order{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	encrypted[len(encrypted)-1] ^= 0xff

	var back Order
	err = codec.Decode(encrypted, &back)
	if err == nil {
		t.Fatal("an altered message decrypted")
	}
	if !acemq.IsFatal(err) {
		t.Error("a message that will not decrypt is not fatal, so it would be retried for ever")
	}
}

func TestChangingTheKeyIdIsCaught(t *testing.T) {
	// The header is authenticated but not encrypted, so a key id swapped in
	// flight has to make the message fail rather than open as something else.
	ring := keyring(t, "2026-01", "2026-02")
	codec := Wrap(acemq.JSONCodec{}, ring)

	encrypted, err := codec.Encode(Order{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	// Rewrite "2026-01" as "2026-02" — same length, a key that exists.
	tampered := bytes.Replace(encrypted, []byte("2026-01"), []byte("2026-02"), 1)

	var back Order
	if err := codec.Decode(tampered, &back); err == nil {
		t.Fatal("a message with a swapped key id decrypted")
	}
}

func TestAKeyThatIsNotHeldIsFatalAndSaysWhichOne(t *testing.T) {
	written := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))
	encrypted, err := written.Encode(Order{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	other := Wrap(acemq.JSONCodec{}, keyring(t, "2026-99"))

	var back Order
	err = other.Decode(encrypted, &back)
	if err == nil {
		t.Fatal("a message decrypted with a keyring that does not hold its key")
	}
	if !strings.Contains(err.Error(), "2026-01") {
		t.Errorf("the error does not name the key that is missing: %v", err)
	}
	if !acemq.IsFatal(err) {
		t.Error("a missing key is not fatal, so the message would be retried for ever")
	}
}

// TestRotationOverlaps is the reason a keyring holds more than one key.
func TestRotationOverlaps(t *testing.T) {
	ring := keyring(t, "2026-01")
	codec := Wrap(acemq.JSONCodec{}, ring)

	old, err := codec.Encode(Order{OrderID: "written-with-the-old-key"})
	if err != nil {
		t.Fatal(err)
	}

	// The new key is added everywhere first, then made current. A consumer
	// must still read what was written before the change.
	fresh, err := NewKey("2026-02")
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.Add(fresh); err != nil {
		t.Fatal(err)
	}
	if err := ring.Use("2026-02"); err != nil {
		t.Fatal(err)
	}

	var back Order
	if err := codec.Decode(old, &back); err != nil {
		t.Fatalf("a message written with the previous key no longer opens: %v", err)
	}
	if back.OrderID != "written-with-the-old-key" {
		t.Errorf("decoded %+v", back)
	}

	// And new messages use the new key.
	recent, err := codec.Encode(Order{OrderID: "new"})
	if err != nil {
		t.Fatal(err)
	}
	id, err := KeyIDOf(recent)
	if err != nil {
		t.Fatal(err)
	}
	if id != "2026-02" {
		t.Errorf("new messages are written with %q", id)
	}
}

func TestAShortKeyIsRefusedRatherThanPadded(t *testing.T) {
	// Padding or hashing a short key would make a weak one look strong.
	_, err := NewKeyring(Key{ID: "weak", Secret: []byte("too short")})

	if err == nil {
		t.Fatal("a nine-byte key was accepted as AES-256")
	}
	if !strings.Contains(err.Error(), "32") {
		t.Errorf("the error does not say how long a key must be: %v", err)
	}
}

func TestAKeyNeedsAnId(t *testing.T) {
	key, err := NewKey("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewKeyring(key); err == nil {
		t.Fatal("a key with no id was accepted; nothing could say which key opens a message")
	}
}

func TestTheSecretNeverPrints(t *testing.T) {
	key, err := NewKey("2026-01")
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(key.String(), string(key.Secret)) {
		t.Error("String() leaked the secret")
	}
	if !strings.Contains(key.String(), "2026-01") {
		t.Errorf("String() does not name the key: %s", key.String())
	}

	ring := keyring(t, "2026-01", "2026-02")
	if ids := ring.IDs(); len(ids) != 2 || ids[0] != "2026-01" {
		t.Errorf("IDs() = %v", ids)
	}
}

func TestSomethingThatWasNeverEncryptedIsFatal(t *testing.T) {
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))

	var back Order
	err := codec.Decode([]byte(`{"orderId":"o-1"}`), &back)

	if err == nil {
		t.Fatal("plaintext JSON was accepted as an encrypted message")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so it would be retried for ever")
	}
}

func TestItClaimsOnlyItsOwnContentType(t *testing.T) {
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))

	if codec.ContentType() != ContentType {
		t.Errorf("ContentType = %q", codec.ContentType())
	}
	if !codec.CanDecode(ContentType) {
		t.Error("it refuses its own content type")
	}
	// The inner format is deliberately not on the wire: "this is encrypted
	// JSON" tells an observer more than they need.
	if codec.CanDecode("application/json") {
		t.Error("it claims plain JSON")
	}
	if codec.CanDecode("") {
		t.Error("it claims a message with no content type")
	}
}

func TestTheInnerCodecIsReachable(t *testing.T) {
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))
	if _, ok := codec.Inner().(acemq.JSONCodec); !ok {
		t.Errorf("Inner() = %T", codec.Inner())
	}
}
