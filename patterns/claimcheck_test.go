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

package patterns_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
	"github.com/AceMQ-Company/acemq-go-amqp/patterns"
)

// ---- claim check -----------------------------------------------------

// Document is a payload big enough to be worth offloading.
type Document struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

func aDocument(size int) Document {
	return Document{ID: "doc-1", Body: strings.Repeat("x", size)}
}

// TestTheClaimCheckFramingIsTheFamilys pins the wire contract byte for byte.
//
// This is the whole of the interoperability. A Go publisher and a Java consumer
// share no code and no schema; they share three bytes at the front of a body and
// a key in UTF-8 after them. Get one of those bytes wrong and every message
// between the two languages is unreadable in a way that looks like a corrupt
// payload rather than a version mismatch.
//
// Taken from ClaimCheckCodec in acemq-amqp-patterns, which documents:
//
//	0xAC  0x01  0x00  payload
//	0xAC  0x01  0x01  key
func TestTheClaimCheckFramingIsTheFamilys(t *testing.T) {
	store := patterns.NewInMemoryClaimCheckStore()

	t.Run("inline", func(t *testing.T) {
		codec := patterns.ClaimCheck(acemq.JSONCodec{}, store)

		body, err := codec.Encode(aDocument(10))
		if err != nil {
			t.Fatal(err)
		}

		if len(body) < 3 {
			t.Fatalf("body is %d bytes, too short to be framed", len(body))
		}
		if body[0] != 0xAC || body[1] != 0x01 || body[2] != 0x00 {
			t.Errorf("framing = %#x %#x %#x, want 0xac 0x01 0x00",
				body[0], body[1], body[2])
		}

		// And after the three bytes, exactly what the delegate wrote. A byte of
		// this library's own in there is a byte the other four would choke on.
		plain, err := acemq.JSONCodec{}.Encode(aDocument(10))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body[3:], plain) {
			t.Errorf("the inline payload is not what the delegate wrote:\n got %q\nwant %q",
				body[3:], plain)
		}
	})

	t.Run("checked", func(t *testing.T) {
		codec := patterns.ClaimCheck(acemq.JSONCodec{}, store, patterns.OffloadAbove(16))

		body, err := codec.Encode(aDocument(200))
		if err != nil {
			t.Fatal(err)
		}

		if body[0] != 0xAC || body[1] != 0x01 || body[2] != 0x01 {
			t.Errorf("framing = %#x %#x %#x, want 0xac 0x01 0x01",
				body[0], body[1], body[2])
		}

		// The key is bare UTF-8 after the header: not a URI, not a scheme,
		// nothing wrapped around it. A library that wrapped it would hand the
		// other four a key their stores have never heard of.
		key := string(body[3:])
		if key == "" {
			t.Fatal("no key after the header")
		}
		if strings.Contains(key, "://") || strings.HasPrefix(key, "{") {
			t.Errorf("key = %q, want the store's key bare", key)
		}
		if got := patterns.ClaimKeyOf(body); got != key {
			t.Errorf("ClaimKeyOf = %q, want %q", got, key)
		}
		if !patterns.IsClaimChecked(body) {
			t.Error("a checked body does not say it is checked")
		}
	})
}

// TestTheThresholdIsComparedTheWayTheFamilyComparesIt is the boundary that has
// to match exactly.
//
// A payload sitting on the threshold must not be inline from one library and
// checked from another: the two would still interoperate, because the framing
// says which it is, but the threshold would mean different things in different
// services and nobody could reason about when offloading happens. Java, Python
// and Ruby all compare strictly less than, so a payload *at* the threshold is
// offloaded.
func TestTheThresholdIsComparedTheWayTheFamilyComparesIt(t *testing.T) {
	if patterns.DefaultClaimCheckThreshold != 65536 {
		t.Errorf("DefaultClaimCheckThreshold = %d, want 65536 (64 KiB), which is what"+
			" the rest of the family uses", patterns.DefaultClaimCheckThreshold)
	}

	store := patterns.NewInMemoryClaimCheckStore()
	// The delegate is bytes, so the encoded length is the payload length and the
	// boundary can be hit exactly rather than approached through JSON overhead.
	codec := patterns.ClaimCheck(acemq.BytesCodec{}, store, patterns.OffloadAbove(100))

	for _, tc := range []struct {
		size    int
		checked bool
		why     string
	}{
		{99, false, "below the threshold travels inline"},
		{100, true, "at the threshold is offloaded: the comparison is strictly less than"},
		{101, true, "above the threshold is offloaded"},
	} {
		body, err := codec.Encode(bytes.Repeat([]byte("x"), tc.size))
		if err != nil {
			t.Fatal(err)
		}
		if got := patterns.IsClaimChecked(body); got != tc.checked {
			t.Errorf("%d bytes: checked = %v, want %v — %s", tc.size, got, tc.checked, tc.why)
		}
	}

	// Zero offloads everything, which is occasionally what a store-backed audit
	// trail wants.
	everything := patterns.ClaimCheck(acemq.BytesCodec{}, store, patterns.OffloadAbove(0))
	body, err := everything.Encode([]byte(""))
	if err != nil {
		t.Fatal(err)
	}
	if !patterns.IsClaimChecked(body) {
		t.Error("a threshold of zero left an empty payload inline")
	}
}

// TestAClaimCheckedMessageIsStillADocument pins the content type.
//
// Unlike encryption, where the bytes really are something else, a claim-checked
// message is a document that happens to be somewhere else. Changing the content
// type would make a consumer choose a different codec for it, which is exactly
// wrong.
func TestAClaimCheckedMessageIsStillADocument(t *testing.T) {
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, patterns.NewInMemoryClaimCheckStore())

	if got, want := codec.ContentType(), (acemq.JSONCodec{}).ContentType(); got != want {
		t.Errorf("ContentType = %q, want the delegate's %q", got, want)
	}
	if !codec.CanDecode((acemq.JSONCodec{}).ContentType()) {
		t.Error("the codec will not read what it writes")
	}
}

// TestAnUnframedBodyIsReadAsTheDelegateWould is what makes adoption safe.
//
// Adding this codec to a live queue leaves messages already on it, written by a
// publisher that framed nothing. Refusing them would mean draining the queue
// before deploying, which nobody does; reading them as the delegate would is the
// only useful answer.
func TestAnUnframedBodyIsReadAsTheDelegateWould(t *testing.T) {
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, patterns.NewInMemoryClaimCheckStore())

	plain, err := acemq.JSONCodec{}.Encode(aDocument(10))
	if err != nil {
		t.Fatal(err)
	}

	var got Document
	if err := codec.Decode(plain, &got); err != nil {
		t.Fatalf("a body written before this codec existed is unreadable: %v", err)
	}
	if got.ID != "doc-1" {
		t.Errorf("got %+v", got)
	}

	// And a body that merely starts with 0xAC is not framing. The magic byte
	// narrows the accident; the version and the kind byte close it.
	notFraming := []byte{0xAC, 0x01, 0x09, 'j', 'u', 'n', 'k'}
	if patterns.IsClaimChecked(notFraming) {
		t.Error("a body with an unknown kind byte was read as a claim check")
	}
	if patterns.ClaimKeyOf(notFraming) != "" {
		t.Error("a key was read out of a body that is not framed")
	}
	if patterns.ClaimKeyOf([]byte{0xAC}) != "" {
		t.Error("a one-byte body was read as framed")
	}
}

// TestAKeyFromAnotherLibraryIsRedeemed is the reading half of the wire contract.
//
// The frame is built by hand rather than by this library's Encode, because what
// is being proved is that Go reads what the other four *write*. Round-tripping
// Go's own output through Go's own reader proves the two halves agree with each
// other and nothing about whether either matches the wire.
func TestAKeyFromAnotherLibraryIsRedeemed(t *testing.T) {
	store := patterns.NewInMemoryClaimCheckStore()
	payload, err := acemq.JSONCodec{}.Encode(aDocument(20))
	if err != nil {
		t.Fatal(err)
	}
	key, err := store.Put(payload)
	if err != nil {
		t.Fatal(err)
	}

	// What a Java publisher put on the wire: three bytes and the key in UTF-8.
	body := append([]byte{0xAC, 0x01, 0x01}, []byte(key)...)

	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store)
	var got Document
	if err := codec.Decode(body, &got); err != nil {
		t.Fatalf("a claim check another library wrote was not redeemed: %v", err)
	}
	if got.ID != "doc-1" {
		t.Errorf("got %+v", got)
	}

	// And the inline framing another library writes, which is the far commoner
	// message and the one that must not need the store at all.
	inline := append([]byte{0xAC, 0x01, 0x00}, payload...)
	var alsoGot Document
	if err := patterns.ClaimCheck(acemq.JSONCodec{}, nil).
		Decode(inline, &alsoGot); err != nil {
		t.Fatalf("an inline body from another library needed the store: %v", err)
	}
	if alsoGot.ID != "doc-1" {
		t.Errorf("got %+v", alsoGot)
	}
}

// TestAMissingPayloadIsFatalRatherThanRetried is the failure this pattern
// actually produces in anger.
//
// The store's retention expired underneath a message that outlived it. No number
// of attempts brings the payload back, so retrying only moves the message
// through its attempts and into a dead-letter queue by the slow road, with an
// error that says "attempts exhausted" instead of what happened.
func TestAMissingPayloadIsFatalRatherThanRetried(t *testing.T) {
	store := patterns.NewInMemoryClaimCheckStore()
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store, patterns.OffloadAbove(0))

	body, err := codec.Encode(aDocument(10))
	if err != nil {
		t.Fatal(err)
	}
	// Retention expiring underneath a deliverable message.
	store.Clear()

	var got Document
	err = codec.Decode(body, &got)
	if err == nil {
		t.Fatal("a message whose payload is gone decoded successfully")
	}
	if !acemq.IsFatal(err) {
		t.Errorf("not fatal, so the message would be retried for ever: %v", err)
	}
	// The message has to say what happened, because the cause is never where
	// somebody looks first.
	if !strings.Contains(err.Error(), "retention") {
		t.Errorf("the error does not explain itself: %v", err)
	}
	if !strings.Contains(err.Error(), patterns.ClaimKeyOf(body)) {
		t.Errorf("the error does not name the key, so nobody can go and look: %v", err)
	}
}

// failingClaimStore answers with an error rather than an absence.
type failingClaimStore struct{ err error }

func (f failingClaimStore) Put([]byte) (string, error)       { return "", f.err }
func (f failingClaimStore) Get(string) ([]byte, bool, error) { return nil, false, f.err }
func (f failingClaimStore) Delete(string) error              { return f.err }

// TestAStoreThatFailedIsToldFromAKeyItDoesNotHold tells a blip from a
// permanent absence. An object store that timed
// out may well answer next time; a key it does not hold will never be there. One
// is worth retrying and the other is not, and collapsing them into one error
// means choosing wrong for one of the two.
func TestAStoreThatFailedIsToldFromAKeyItDoesNotHold(t *testing.T) {
	store := failingClaimStore{err: errors.New("connection reset")}
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store)

	body := append([]byte{0xAC, 0x01, 0x01}, []byte("some-key")...)
	var got Document
	err := codec.Decode(body, &got)

	if err == nil {
		t.Fatal("a store that failed decoded successfully")
	}
	if acemq.IsFatal(err) {
		t.Errorf("a store that timed out was treated as permanent, so the message is"+
			" dead-lettered over a blip: %v", err)
	}

	// And on the way out: a store that will not take the payload must not let
	// the publish succeed with a key nothing can redeem.
	if _, err := codec.Encode(aDocument(100000)); err == nil {
		t.Error("a payload the store refused was published anyway")
	}
}

// TestAClaimCheckRoundTripsThroughABroker is the pattern doing its job: a
// payload too big for a queue goes out and comes back, and what crossed the
// broker was the key.
func TestAClaimCheckRoundTripsThroughABroker(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryClaimCheckStore()
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store, patterns.OffloadAbove(1024))

	mq := brokerFor(t, acemq.WithCodec(codec))
	if err := mq.DeclareQueue(ctx, "documents"); err != nil {
		t.Fatal(err)
	}

	arrived := make(chan Document, 1)
	sub, err := acemq.Consume(ctx, mq, "documents",
		func(_ context.Context, m acemq.Message[Document]) acemq.Ack {
			arrived <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	big := aDocument(4096)
	if err := acemq.NewPublisher[Document](mq, "", "documents").Send(ctx, big); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-arrived:
		if got.Body != big.Body {
			t.Errorf("the payload came back %d bytes, want %d", len(got.Body), len(big.Body))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the document never arrived")
	}

	// The payload went to the store rather than to the broker, which is the
	// entire point and the only part a passing round trip would not prove.
	if store.Len() != 1 {
		t.Errorf("the store holds %d payloads, want 1: the payload went over the"+
			" broker instead", store.Len())
	}
}

// TestASmallMessageIsNotOffloaded is the other half of the round trip, and the
// reason for the threshold: offloading a small message turns one broker round
// trip into a store round trip and a broker round trip.
func TestASmallMessageIsNotOffloaded(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryClaimCheckStore()
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store)

	mq := brokerFor(t, acemq.WithCodec(codec))
	if err := mq.DeclareQueue(ctx, "documents"); err != nil {
		t.Fatal(err)
	}

	arrived := make(chan Document, 1)
	sub, err := acemq.Consume(ctx, mq, "documents",
		func(_ context.Context, m acemq.Message[Document]) acemq.Ack {
			arrived <- m.Payload
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[Document](mq, "", "documents").
		Send(ctx, aDocument(10)); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-arrived:
		if got.ID != "doc-1" {
			t.Errorf("got %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the document never arrived")
	}

	if store.Len() != 0 {
		t.Errorf("the store holds %d payloads; a small message should not have gone"+
			" near it", store.Len())
	}
}

// TestTheCodecDoesNotWriteTheClaimHeader. x-acemq-claim is reserved for an
// application that wants to say where a payload went. The codec frames the body
// instead, because a header can be stripped by a shovel or a federation link and
// because a present-or-absent header cannot say whether a payload travelled
// inline. Python and Ruby reserve it the same way, and a Go library that started
// writing it would be the odd one out.
func TestTheCodecDoesNotWriteTheClaimHeader(t *testing.T) {
	ctx := context.Background()
	store := patterns.NewInMemoryClaimCheckStore()
	codec := patterns.ClaimCheck(acemq.JSONCodec{}, store, patterns.OffloadAbove(0))

	mq := brokerFor(t, acemq.WithCodec(codec))
	if err := mq.DeclareQueue(ctx, "documents"); err != nil {
		t.Fatal(err)
	}

	arrived := make(chan acemq.Envelope, 1)
	sub, err := acemq.Consume(ctx, mq, "documents",
		func(_ context.Context, m acemq.Message[Document]) acemq.Ack {
			arrived <- m.Envelope
			return acemq.Accept()
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := acemq.NewPublisher[Document](mq, "", "documents").
		Send(ctx, aDocument(10)); err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-arrived:
		if _, present := env.Headers[acemq.HeaderClaim]; present {
			t.Error("the codec wrote x-acemq-claim; that header is the application's")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the document never arrived")
	}
}

// ---- the stores ------------------------------------------------------

func TestTheFilesystemStoreRoundTripsAndDeletes(t *testing.T) {
	dir := t.TempDir()
	store := patterns.NewFilesystemClaimCheckStore(filepath.Join(dir, "claims"))

	key, err := store.Put([]byte("the payload"))
	if err != nil {
		t.Fatal(err)
	}

	content, found, err := store.Get(key)
	if err != nil || !found {
		t.Fatalf("Get(%q) = %v, %v", key, found, err)
	}
	if string(content) != "the payload" {
		t.Errorf("got %q", content)
	}

	// Nothing partial is left behind. A .partial file that survived would be
	// read by nobody and would grow the directory for ever.
	entries, err := os.ReadDir(filepath.Join(dir, "claims"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".partial") {
			t.Errorf("a staging file was left behind: %s", e.Name())
		}
	}

	if err := store.Delete(key); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.Get(key); found {
		t.Error("the payload survived deletion")
	}
	// Deleting twice is not an error: a retention sweep should not have to know
	// whether somebody else already got there.
	if err := store.Delete(key); err != nil {
		t.Errorf("deleting an absent key failed: %v", err)
	}

	// A key the store no longer holds is an absence rather than a failure, which
	// is what lets the codec turn it into a message that explains itself.
	content, found, err = store.Get("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
	if err != nil || found || content != nil {
		t.Errorf("an unknown key gave %q, %v, %v; want nil, false, nil", content, found, err)
	}
}

// TestAKeyFromAMessageIsCheckedRatherThanTrusted. A key becomes a path segment,
// and one arriving from a message is whatever a publisher put there — including
// a publisher that is not on your side. Every key this store issues is a UUID,
// which is what makes the check a check rather than a restriction.
func TestAKeyFromAMessageIsCheckedRatherThanTrusted(t *testing.T) {
	dir := t.TempDir()
	store := patterns.NewFilesystemClaimCheckStore(dir)

	// A file the caller must not be able to reach through a key.
	secret := filepath.Join(dir, "..", "secret")
	if err := os.WriteFile(secret, []byte("not yours"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{
		"../secret",
		"../../etc/passwd",
		"/etc/passwd",
		"a/b",
		"",
		".hidden",
		strings.Repeat("a", 129),
	} {
		if _, _, err := store.Get(key); err == nil {
			t.Errorf("Get(%q) was allowed", key)
		} else if !acemq.IsFatal(err) {
			t.Errorf("Get(%q) is not fatal, so the message would circle: %v", key, err)
		}
		if err := store.Delete(key); err == nil {
			t.Errorf("Delete(%q) was allowed", key)
		}
	}

	// And a key the store actually issued still works, so the check has not
	// simply refused everything.
	key, err := store.Put([]byte("mine"))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Get(key); err != nil || !found {
		t.Errorf("a key this store issued was refused: %v", err)
	}
}

func TestTheInMemoryStoreDoesNotShareTheCallersBuffer(t *testing.T) {
	store := patterns.NewInMemoryClaimCheckStore()

	buffer := []byte("the payload")
	key, err := store.Put(buffer)
	if err != nil {
		t.Fatal(err)
	}
	// A codec is entitled to reuse a buffer. A store that kept this slice is a
	// store whose contents change after they were stored.
	copy(buffer, "OVERWRITTEN")

	content, found, err := store.Get(key)
	if err != nil || !found {
		t.Fatal(err)
	}
	if string(content) != "the payload" {
		t.Errorf("the stored payload changed underneath the store: %q", content)
	}

	// And the other way: mutating what Get returned must not reach the store.
	copy(content, "MUTATED")
	again, _, _ := store.Get(key)
	if string(again) != "the payload" {
		t.Errorf("a reader mutated the store's own copy: %q", again)
	}

	if store.Len() != 1 {
		t.Errorf("Len = %d, want 1", store.Len())
	}
	store.Clear()
	if store.Len() != 0 {
		t.Errorf("Len = %d after Clear, want 0", store.Len())
	}
}

// TestWrappingACompositeCodecStillChoosesByContentType. A composite cannot
// decode without being told the content type, so a wrapper that called plain
// Decode would break every queue carrying more than one format — which is
// exactly the kind of queue an old, large payload turns up on.
func TestWrappingACompositeCodecStillChoosesByContentType(t *testing.T) {
	store := patterns.NewInMemoryClaimCheckStore()
	composite := acemq.NewCompositeCodec(acemq.JSONCodec{}, acemq.StringCodec{})
	codec := patterns.ClaimCheck(composite, store, patterns.OffloadAbove(0))

	body, err := codec.Encode(aDocument(10))
	if err != nil {
		t.Fatal(err)
	}

	var got Document
	if err := codec.DecodeAs(acemq.JSONCodec{}.ContentType(), body, &got); err != nil {
		t.Fatalf("a claim check over a composite codec was unreadable: %v", err)
	}
	if got.ID != "doc-1" {
		t.Errorf("got %+v", got)
	}
}
