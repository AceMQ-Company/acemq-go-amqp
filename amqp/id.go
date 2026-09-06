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

package acemq

import (
	"crypto/rand"
	"encoding/hex"
)

// newID returns a random UUID version 4 in the canonical 8-4-4-4-12 form.
//
// Written out rather than taken from a module because it is thirty lines and
// this package otherwise needs nothing outside the standard library. A message
// identifier is the default idempotency key, so it is drawn from crypto/rand
// rather than math/rand: a predictable identifier would let one publisher
// suppress another's message.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on any supported platform without the
		// system being unusable. Carrying on with a predictable identifier
		// would silently weaken idempotency, so this is not recoverable.
		panic("acemq: cannot read random bytes for a message id: " + err.Error())
	}

	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}

// NewID returns a random UUID version 4, the same kind of identifier an
// [Envelope] generates for itself.
//
// Exported for the patterns package, which needs correlation identifiers drawn
// the same way rather than a second scheme that looks almost the same.
func NewID() string { return newID() }
