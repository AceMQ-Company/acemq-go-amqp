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
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type Order struct {
	OrderID string `json:"orderId"`
	Card    string `json:"card"`
}

// rawCodec passes bytes through untouched, so a test can pin the framing around
// a plaintext it chose rather than around whatever JSON happened to produce.
type rawCodec struct{}

func (rawCodec) ContentType() string { return "application/octet-stream" }

func (rawCodec) Encode(payload any) ([]byte, error) {
	text, ok := payload.(string)
	if !ok {
		return nil, fmt.Errorf("rawCodec takes a string, not %T", payload)
	}
	return []byte(text), nil
}

func (rawCodec) Decode(body []byte, dst any) error {
	into, ok := dst.(*string)
	if !ok {
		return fmt.Errorf("rawCodec decodes into *string, not %T", dst)
	}
	*into = string(body)
	return nil
}

func (rawCodec) CanDecode(string) bool { return true }

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

// TestEveryLengthAESTakesIsAccepted is what lets a key that already works in
// Java, Python or Ruby be used here without being re-issued.
func TestEveryLengthAESTakesIsAccepted(t *testing.T) {
	for _, size := range []int{16, 24, 32} {
		ring, err := NewKeyring(Key{ID: "2026-01", Secret: make([]byte, size)})
		if err != nil {
			t.Fatalf("a %d-byte AES key was refused: %v", size, err)
		}

		codec := Wrap(rawCodec{}, ring)
		encrypted, err := codec.Encode("hello")
		if err != nil {
			t.Fatalf("a %d-byte key could not encrypt: %v", size, err)
		}

		var back string
		if err := codec.Decode(encrypted, &back); err != nil {
			t.Fatalf("a %d-byte key could not decrypt: %v", size, err)
		}
		if back != "hello" {
			t.Errorf("a %d-byte key round-tripped to %q", size, back)
		}
	}
}

func TestAKeyIdIsAtMostTwoHundredAndFiftyFiveBytes(t *testing.T) {
	// One length byte in the framing is the whole reason for the limit, so a
	// longer id has to be refused when it is added rather than truncated into
	// a different key's name when it is written.
	_, err := NewKeyring(Key{ID: strings.Repeat("k", 256), Secret: make([]byte, KeySize)})

	if err == nil {
		t.Fatal("a 256-byte key id was accepted; the length byte cannot hold it")
	}
	if !strings.Contains(err.Error(), "255") {
		t.Errorf("the error does not say how long a key id may be: %v", err)
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

// The key, nonce and plaintext the family's test vector is written for. The
// same three appear in the Java, Python and Ruby test suites.
var (
	vectorKey       = vectorBytes(32)
	vectorNonce     = vectorBytes(12)
	vectorKeyID     = "2026-01"
	vectorPlaintext = "hello"

	// What Java, Python and Ruby produce from those three, byte for byte:
	//
	//	ae        magic
	//	01        version
	//	07        the key id is seven bytes
	//	32...31   "2026-01"
	//	0001..0b  the nonce
	//	2f67...   the ciphertext and its 16-byte tag
	//
	// Verified against the compiled Java EncryptedCodec and against Ruby's
	// implementation, both of which return exactly this string.
	vectorHex = "ae0107323032362d3031" +
		"000102030405060708090a0b" +
		"2f67ba77aa632797b83b1f88ef1394bb9ff6e85641"
)

func vectorBytes(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i)
	}
	return out
}

// TestTheFramingIsTheFamilys is the test that says this library and the others
// mean the same thing by application/vnd.acemq.encrypted.
//
// It is a fixed vector rather than a round trip on purpose: a round trip passes
// just as happily against a framing only this library understands, which is
// precisely the failure it exists to catch.
func TestTheFramingIsTheFamilys(t *testing.T) {
	header := frame(vectorKeyID)

	block, err := aes.NewCipher(vectorKey)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}

	body := append(append([]byte{}, header...), vectorNonce...)
	body = append(body, gcm.Seal(nil, vectorNonce, []byte(vectorPlaintext), header)...)

	if got := hex.EncodeToString(body); got != vectorHex {
		t.Errorf("this library writes a framing the other libraries cannot read:\n"+
			" got %s\nwant %s", got, vectorHex)
	}
}

// TestTheVectorDecodes reads the same bytes back through the codec, so the
// vector pins both halves rather than only the one this library happens to be
// exercising.
func TestTheVectorDecodes(t *testing.T) {
	ring, err := NewKeyring(Key{ID: vectorKeyID, Secret: vectorKey})
	if err != nil {
		t.Fatal(err)
	}

	body, err := hex.DecodeString(vectorHex)
	if err != nil {
		t.Fatal(err)
	}

	var back string
	if err := Wrap(rawCodec{}, ring).Decode(body, &back); err != nil {
		t.Fatalf("a message written by Java, Python or Ruby did not open here: %v", err)
	}
	if back != vectorPlaintext {
		t.Errorf("decoded %q", back)
	}

	id, err := KeyIDOf(body)
	if err != nil {
		t.Fatal(err)
	}
	if id != vectorKeyID {
		t.Errorf("KeyIDOf = %q", id)
	}
}

func TestEveryMessageStartsWithTheMagicByte(t *testing.T) {
	codec := Wrap(acemq.JSONCodec{}, keyring(t, "2026-01"))

	encrypted, err := codec.Encode(Order{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}

	// 0xAE is what tells a body that was encrypted from a body that merely
	// begins with a small number, which is what the old framing could not do.
	if encrypted[0] != magic {
		t.Errorf("a message begins %#x rather than %#x", encrypted[0], magic)
	}
	if encrypted[1] != formatVersion {
		t.Errorf("a message is version %#x", encrypted[1])
	}
	if int(encrypted[2]) != len("2026-01") {
		t.Errorf("the key id length byte says %d", encrypted[2])
	}
}

// legacyGoBody writes the framing this library wrote up to v0.3.0, which
// nothing writes any more. Only a test needs to build one, and only so that
// reading it can be proven to still work.
func legacyGoBody(t *testing.T, key Key, plaintext string) []byte {
	t.Helper()

	header := make([]byte, 0, 3+len(key.ID))
	header = append(header, legacyFormatVersion)
	header = binary.BigEndian.AppendUint16(header, uint16(len(key.ID)))
	header = append(header, key.ID...)

	block, err := aes.NewCipher(key.Secret)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}

	nonce := make([]byte, gcm.NonceSize())
	body := append(append([]byte{}, header...), nonce...)
	return append(body, gcm.Seal(nil, nonce, []byte(plaintext), header)...)
}

// TestTheLegacyGoFramingStillReads is the migration affordance: v0.3.0 is
// released, so queues can hold bodies in the framing this library used to
// write, and a consumer upgraded ahead of them has to be able to drain them.
//
// It goes away in v0.6.0, and this test goes with it.
func TestTheLegacyGoFramingStillReads(t *testing.T) {
	key, err := NewKey("2026-01")
	if err != nil {
		t.Fatal(err)
	}
	ring, err := NewKeyring(key)
	if err != nil {
		t.Fatal(err)
	}

	old := legacyGoBody(t, key, "written before the framing changed")
	if old[0] != legacyFormatVersion {
		t.Fatalf("the fixture is not in the legacy framing: it begins %#x", old[0])
	}

	var back string
	if err := Wrap(rawCodec{}, ring).Decode(old, &back); err != nil {
		t.Fatalf("a body written by v0.3.0 no longer opens: %v", err)
	}
	if back != "written before the framing changed" {
		t.Errorf("decoded %q", back)
	}

	// And an operator can still tell which key it needs.
	id, err := KeyIDOf(old)
	if err != nil {
		t.Fatal(err)
	}
	if id != "2026-01" {
		t.Errorf("KeyIDOf on a legacy body = %q", id)
	}
}

// TestALegacyBodyIsStillAuthenticated: reading the old framing must not mean
// reading it more trustingly than the new one.
func TestALegacyBodyIsStillAuthenticated(t *testing.T) {
	key, err := NewKey("2026-01")
	if err != nil {
		t.Fatal(err)
	}
	ring, err := NewKeyring(key)
	if err != nil {
		t.Fatal(err)
	}

	old := legacyGoBody(t, key, "written before the framing changed")
	old[len(old)-1] ^= 0xff

	var back string
	err = Wrap(rawCodec{}, ring).Decode(old, &back)
	if err == nil {
		t.Fatal("an altered legacy body decrypted")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so it would be retried for ever")
	}
}

// TestNothingWritesTheLegacyFraming: two writers is how the divergence this
// change removes would come straight back.
func TestNothingWritesTheLegacyFraming(t *testing.T) {
	ring := keyring(t, "2026-01")

	for range 20 {
		encrypted, err := Wrap(rawCodec{}, ring).Encode("hello")
		if err != nil {
			t.Fatal(err)
		}
		if encrypted[0] != magic {
			t.Fatalf("a message was written in the legacy framing: it begins %#x", encrypted[0])
		}
	}
}

func TestATruncatedMessageIsFatal(t *testing.T) {
	codec := Wrap(rawCodec{}, keyring(t, "2026-01"))

	encrypted, err := codec.Encode("hello")
	if err != nil {
		t.Fatal(err)
	}

	// Everything but the nonce and the tag it needs.
	var back string
	err = codec.Decode(encrypted[:len(encrypted)-4], &back)
	if err == nil {
		t.Fatal("a truncated message decrypted")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so it would be retried for ever")
	}
}

func TestAMessageInNeitherFramingSaysSo(t *testing.T) {
	codec := Wrap(rawCodec{}, keyring(t, "2026-01"))

	// Neither 0xAE nor 0x01. A protobuf body, say.
	var back string
	err := codec.Decode([]byte{0x0a, 0x05, 'h', 'e', 'l', 'l', 'o'}, &back)

	if err == nil {
		t.Fatal("something that was never encrypted was accepted")
	}
	if !acemq.IsFatal(err) {
		t.Error("it is not fatal, so it would be retried for ever")
	}
	if !strings.Contains(err.Error(), "framing") {
		t.Errorf("the error does not say what is wrong with the body: %v", err)
	}
}
