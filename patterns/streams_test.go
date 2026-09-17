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

// The half of streams.go that needs no broker: what a retry from a stream
// handler becomes, and how an offset is read back off a delivery.
//
// An internal test because refuseRetry is where the refusal lives and an
// external one could only reach it through a running consumer, which needs a
// broker. The refusal is a decision about a verb, not about a connection, and
// testing it through a connection would mean this never runs on a machine
// without Docker.

import (
	"context"
	"errors"
	"strings"
	"testing"

	acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
)

type streamed struct {
	OrderID string `json:"orderId"`
}

func streamDelivery(id string, offset any) acemq.Message[streamed] {
	headers := map[string]any{}
	if offset != nil {
		headers["x-stream-offset"] = offset
	}
	return acemq.Message[streamed]{
		Payload:  streamed{OrderID: id},
		Envelope: acemq.Envelope{ID: id, Headers: headers},
	}
}

// TestARetryFromAStreamHandlerIsRefused is the decision this file exists for.
//
// A retry republishes the message onto the queue it came from. On a stream that
// is not a redelivery: it is a new message at a new offset, which every other
// consumer reads, which a projection rebuilding from the start next month reads,
// and which a handler failing on every message turns into a log that grows by a
// copy of itself per attempt. So the verb is refused rather than obeyed.
func TestARetryFromAStreamHandlerIsRefused(t *testing.T) {
	cause := errors.New("the projection store is down")

	wrapped := refuseRetry("orders.log", func(_ context.Context, _ acemq.Message[streamed]) acemq.Ack {
		return acemq.Retry(cause)
	})

	ack := wrapped(context.Background(), streamDelivery("order-1", int64(41337)))

	if ack.IsRetry() {
		t.Fatal("the retry went through, so a second copy of the message is in the log")
	}
	if ack.String() != "park" {
		t.Errorf("the refusal settled as %q, want park", ack)
	}

	var refused *RetryOnStreamError
	if !errors.As(ack.Err(), &refused) {
		t.Fatalf("the park carries %v, want a *RetryOnStreamError", ack.Err())
	}
	if refused.Stream != "orders.log" {
		t.Errorf("the error names stream %q", refused.Stream)
	}
	if refused.MessageID != "order-1" {
		t.Errorf("the error names message %q", refused.MessageID)
	}
	if refused.Offset != 41337 {
		t.Errorf("the error names offset %d, want the delivery's 41337", refused.Offset)
	}

	// The handler's own reason survives the refusal, so a sentinel it was
	// checking for still matches after the library has had its say.
	if !errors.Is(ack.Err(), cause) {
		t.Error("the handler's reason did not survive errors.Is through the refusal")
	}
}

// TestTheRefusalNamesBothHonestAlternatives is the whole point of refusing
// rather than quietly doing something else.
//
// A handler author who wrote Retry believed a retry was available. The message
// has to say it is not, why, and what the two things they can do instead are —
// otherwise this is a library silently substituting its own judgement, which is
// the failure it was meant to prevent.
func TestTheRefusalNamesBothHonestAlternatives(t *testing.T) {
	refused := &RetryOnStreamError{
		Stream:    "orders.log",
		Offset:    17,
		MessageID: "order-9",
		Err:       errors.New("boom"),
	}
	message := refused.Error()

	for _, want := range []string{
		"acemq.Retry",     // the verb that was used
		"second copy",     // what it would have done
		"acemq.Park",      // the first alternative
		"x-stream-offset", // the second
		"acemq.Accept",
		"orders.log.parked", // where the message actually went
		"offset 17",
		"boom", // the handler's own reason
	} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, message)
		}
	}
}

// TestTheRefusalStillReadsWithoutAnOffset covers a delivery that carried no
// x-stream-offset — an ordinary queue message, or one a hop rebuilt without
// restamping it. Naming "offset 0" there would be a lie about where it sat.
func TestTheRefusalStillReadsWithoutAnOffset(t *testing.T) {
	wrapped := refuseRetry("orders.log", func(_ context.Context, _ acemq.Message[streamed]) acemq.Ack {
		return acemq.Retry(errors.New("boom"))
	})

	ack := wrapped(context.Background(), streamDelivery("order-2", nil))

	var refused *RetryOnStreamError
	if !errors.As(ack.Err(), &refused) {
		t.Fatalf("the park carries %v, want a *RetryOnStreamError", ack.Err())
	}
	if refused.Offset != 0 {
		t.Errorf("an offsetless delivery reported offset %d", refused.Offset)
	}
	if strings.Contains(refused.Error(), "at offset") {
		t.Errorf("the refusal claims a position the delivery never carried:\n%s", refused.Error())
	}
}

// TestEveryOtherOutcomeIsLeftAlone is the other side of the refusal. Wrapping a
// handler to catch one verb must not rewrite the rest: accept, reject and park
// all mean on a stream what they mean everywhere else, caveats and all.
func TestEveryOtherOutcomeIsLeftAlone(t *testing.T) {
	reason := errors.New("unreadable")

	for _, c := range []struct {
		name string
		ack  acemq.Ack
	}{
		{"accept", acemq.Accept()},
		{"reject", acemq.Reject(reason)},
		{"park", acemq.Park(reason)},
	} {
		wrapped := refuseRetry("orders.log", func(_ context.Context, _ acemq.Message[streamed]) acemq.Ack {
			return c.ack
		})
		got := wrapped(context.Background(), streamDelivery("order-3", int64(1)))
		if got.String() != c.name {
			t.Errorf("%s came back as %s", c.name, got)
		}
		if c.name != "accept" && !errors.Is(got.Err(), reason) {
			t.Errorf("%s lost the handler's reason", c.name)
		}
	}
}

// TestAnOffsetIsReadWhateverWidthItArrivesAs is the checkpoint path's first
// step. The broker writes an integer and which width it lands as depends on the
// client, so a reader that asserted one type would work until it did not.
func TestAnOffsetIsReadWhateverWidthItArrivesAs(t *testing.T) {
	for _, c := range []struct {
		name  string
		value any
		want  uint64
		found bool
	}{
		{"int64", int64(41337), 41337, true},
		{"int32", int32(7), 7, true},
		{"int", 12, 12, true},
		{"uint64", uint64(1 << 40), 1 << 40, true},
		{"zero is a real offset", int64(0), 0, true},
		{"absent", nil, 0, false},
		{"a string nobody can trust", "41337", 0, false},
		{"negative, which no offset is", int64(-1), 0, false},
	} {
		env := acemq.Envelope{Headers: map[string]any{}}
		if c.value != nil {
			env.Headers["x-stream-offset"] = c.value
		}

		got, found := StreamOffsetOf(env)
		if found != c.found {
			t.Errorf("%s: found=%v, want %v", c.name, found, c.found)
		}
		if got != c.want {
			t.Errorf("%s: offset=%d, want %d", c.name, got, c.want)
		}
	}
}

// TestAnAbsentOffsetIsNotAZeroCheckpoint is why StreamOffsetOf reports whether
// it found one rather than just returning a number.
//
// Zero is a real offset — the first message in a stream. A reader that could not
// tell "the beginning" from "the delivery said nothing" would write a checkpoint
// of zero for a message that carried no position, and the next run would replay
// the whole stream believing it was resuming.
func TestAnAbsentOffsetIsNotAZeroCheckpoint(t *testing.T) {
	beginning, found := StreamOffsetOf(acemq.Envelope{
		Headers: map[string]any{"x-stream-offset": int64(0)}})
	if !found || beginning != 0 {
		t.Errorf("the first message in a stream read as (%d, %v)", beginning, found)
	}

	_, found = StreamOffsetOf(acemq.Envelope{Headers: map[string]any{}})
	if found {
		t.Error("a delivery with no offset header reported one")
	}
}
