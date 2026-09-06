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

// CompositeCodec picks a codec by the message's content type.
//
// For a queue that carries more than one format — during a migration, or where
// several producers were written at different times:
//
//	codec := acemq.NewCompositeCodec(acemq.JSONCodec{}, yaml.Codec{})
//	mq, err := acemq.Connect(ctx, url, acemq.WithCodec(codec))
//
// The first codec is what it writes; all of them are offered a message to read,
// in order, and the first that says it can decode does. Order therefore matters
// when two overlap — put the more specific first, since [BytesCodec] answers for
// everything and would win from anywhere in the list.
type CompositeCodec struct {
	codecs []Codec
}

// NewCompositeCodec combines codecs. The first is used for encoding.
func NewCompositeCodec(codecs ...Codec) *CompositeCodec {
	return &CompositeCodec{codecs: codecs}
}

// ContentType is the first codec's, since that is what gets written.
func (c *CompositeCodec) ContentType() string {
	if len(c.codecs) == 0 {
		return ""
	}
	return c.codecs[0].ContentType()
}

// Encode uses the first codec.
func (c *CompositeCodec) Encode(payload any) ([]byte, error) {
	if len(c.codecs) == 0 {
		return nil, Fatalf("acemq: this CompositeCodec has no codecs in it")
	}
	return c.codecs[0].Encode(payload)
}

// Decode uses the first codec that will accept the content type.
func (c *CompositeCodec) Decode(body []byte, dst any) error {
	return Fatalf(
		"acemq: CompositeCodec.Decode needs the content type; " +
			"it is only usable through a consumer, which passes one")
}

// DecodeAs reads a body using whichever codec claims the content type.
//
// The consumer calls this rather than Decode, because choosing needs the
// content type and the Codec interface does not carry one into Decode.
func (c *CompositeCodec) DecodeAs(contentType string, body []byte, dst any) error {
	for _, codec := range c.codecs {
		if codec.CanDecode(contentType) {
			return codec.Decode(body, dst)
		}
	}
	return Fatalf(
		"acemq: no codec in this CompositeCodec will read %q; it holds %s",
		contentType, c.describe())
}

// CanDecode is true when any codec in the set will take it.
func (c *CompositeCodec) CanDecode(contentType string) bool {
	for _, codec := range c.codecs {
		if codec.CanDecode(contentType) {
			return true
		}
	}
	return false
}

func (c *CompositeCodec) describe() string {
	names := make([]string, 0, len(c.codecs))
	for _, codec := range c.codecs {
		names = append(names, codec.ContentType())
	}
	return strings.Join(names, ", ")
}

// contentTypeDecoder is a codec that needs the content type to choose.
//
// The consumer looks for it so a CompositeCodec works without every codec
// having to take a content type it does not need.
type contentTypeDecoder interface {
	DecodeAs(contentType string, body []byte, dst any) error
}
