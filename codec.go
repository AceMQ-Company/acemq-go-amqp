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
	"sort"
	"sync"
)

// Codec turns a message payload into bytes and back.
//
// Decode takes a pointer to the destination rather than returning a value,
// which is how encoding/json and every other Go decoder works. That is the one
// place this API deliberately departs from the Java and .NET shape: returning
// an any and asking the caller to assert would be worse Go for no gain.
type Codec interface {
	// ContentType is what this codec writes onto the message.
	ContentType() string

	// Encode turns a payload into bytes.
	Encode(payload any) ([]byte, error)

	// Decode reads bytes into dst, which must be a non-nil pointer.
	Decode(body []byte, dst any) error

	// CanDecode reports whether this codec should handle a message carrying the
	// given content type. An empty string means the sender set none.
	CanDecode(contentType string) bool
}

var (
	codecsMu sync.RWMutex
	codecs   = map[string]func() Codec{}
)

// RegisterCodec makes a codec available by name, so configuration can name a
// format without the calling code importing it.
//
// Registering the same name twice replaces the first, which is what makes it
// possible to override a default from a test.
func RegisterCodec(name string, make func() Codec) {
	codecsMu.Lock()
	defer codecsMu.Unlock()
	codecs[name] = make
}

// CodecByName builds the codec registered under a name.
func CodecByName(name string) (Codec, error) {
	codecsMu.RLock()
	make, ok := codecs[name]
	codecsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("acemq: no codec named %q is registered; known: %v", name, CodecNames())
	}
	return make(), nil
}

// CodecNames lists the registered codec names, sorted.
func CodecNames() []string {
	codecsMu.RLock()
	defer codecsMu.RUnlock()
	names := make([]string, 0, len(codecs))
	for name := range codecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func init() {
	RegisterCodec("json", func() Codec { return JSONCodec{} })
}
