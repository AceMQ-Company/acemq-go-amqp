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
	"fmt"
	"strings"
)

// BytesContentType is what the bytes codec writes when nothing else is known.
const BytesContentType = "application/octet-stream"

// BytesCodec passes bodies through without touching them.
//
// For a payload that is already encoded — an image, something another system's
// serializer produced — and for reading a message whose type this process does
// not have. Replaying a dead-lettered message uses it: the bytes that were
// committed are the bytes that should go back, and re-encoding through a Go
// type that has since changed would produce something else.
//
// It answers for any content type, which means it must be asked for
// explicitly rather than found by content type.
type BytesCodec struct{}

// ContentType returns application/octet-stream.
func (BytesCodec) ContentType() string { return BytesContentType }

// Encode accepts []byte or a string and returns the bytes unchanged.
func (BytesCodec) Encode(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case nil:
		return nil, nil
	default:
		return nil, Fatalf(
			"acemq: BytesCodec cannot encode a %T; it takes []byte or string", payload)
	}
}

// Decode copies the body into dst, which must be *[]byte or *string.
func (BytesCodec) Decode(body []byte, dst any) error {
	switch v := dst.(type) {
	case *[]byte:
		// Copied rather than aliased: the transport may reuse its buffer, and a
		// payload that changes underneath a handler is a bug nobody would
		// think to look for.
		out := make([]byte, len(body))
		copy(out, body)
		*v = out
		return nil
	case *string:
		*v = string(body)
		return nil
	default:
		return Fatalf(
			"acemq: BytesCodec cannot decode into a %T; it needs *[]byte or *string", dst)
	}
}

// CanDecode accepts anything, which is why this codec has to be asked for by
// name rather than chosen by content type.
func (BytesCodec) CanDecode(string) bool { return true }

// TextContentType is what the string codec writes.
const TextContentType = "text/plain; charset=utf-8"

// StringCodec reads and writes text.
//
// For messages that really are text — a line of a log, a command somebody typed
// — rather than a structure that happens to be readable. Prefer [JSONCodec]
// for anything with fields.
type StringCodec struct{}

// ContentType returns text/plain; charset=utf-8.
func (StringCodec) ContentType() string { return TextContentType }

// Encode accepts a string, a []byte, or anything with a String method.
func (StringCodec) Encode(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case fmt.Stringer:
		return []byte(v.String()), nil
	default:
		return nil, Fatalf(
			"acemq: StringCodec cannot encode a %T; it takes a string, []byte or fmt.Stringer", payload)
	}
}

// Decode copies the body into dst, which must be *string or *[]byte.
func (StringCodec) Decode(body []byte, dst any) error {
	switch v := dst.(type) {
	case *string:
		*v = string(body)
		return nil
	case *[]byte:
		out := make([]byte, len(body))
		copy(out, body)
		*v = out
		return nil
	default:
		return Fatalf(
			"acemq: StringCodec cannot decode into a %T; it needs *string or *[]byte", dst)
	}
}

// CanDecode accepts text/* and nothing else.
func (StringCodec) CanDecode(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(contentType), "text/")
}

func init() {
	RegisterCodec("bytes", func() Codec { return BytesCodec{} })
	RegisterCodec("string", func() Codec { return StringCodec{} })
}
