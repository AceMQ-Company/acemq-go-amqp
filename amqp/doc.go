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

// Package acemq is a Go client for AceMQ messaging over AMQP.
//
// It speaks the same wire contract as the Java and .NET libraries: the same
// reserved headers, the same defaults, the same retry semantics. A Go consumer
// reads what a Java producer writes, and the fixtures in internal/testdata pin
// that rather than leaving it to be discovered in production.
//
//	import acemq "github.com/AceMQ-Company/acemq-go-amqp/amqp"
//
// The package is named acemq and the directory amqp, so the import needs the
// name on it. That is deliberate: the module root stays free for the packages
// beside this one, and "acemq.Connect" reads better at every call site than
// "amqp.Connect" would.
//
// # Getting a message across
//
//	mq, err := acemq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
//	if err != nil {
//		return err
//	}
//	defer mq.Close()
//
//	if err := mq.DeclareQueue(ctx, "orders"); err != nil {
//		return err
//	}
//
//	pub := acemq.NewPublisher[OrderPlaced](mq, "", "orders")
//	if err := pub.Send(ctx, OrderPlaced{ID: "o-1"}); err != nil {
//		return err
//	}
//
//	sub, err := acemq.Consume(ctx, mq, "orders",
//		func(ctx context.Context, m acemq.Message[OrderPlaced]) acemq.Ack {
//			log.Println(m.Payload.ID, "attempt", m.Envelope.Attempt)
//			return acemq.Accept()
//		})
//
// # Why publishing and consuming are functions rather than methods
//
// [NewPublisher] and [Consume] take the connection as their first argument
// instead of hanging off it, because a method cannot have its own type
// parameter before Go 1.27:
//
//	func (c *Conn) Publisher[T any](...)  // generic method: needs go1.27
//	func NewPublisher[T any](c *Conn, ...) // generic function: works everywhere
//
// This module declares go1.23 so it can be used by projects that have not moved
// to the newest toolchain, which for a library matters more than mirroring the
// exact shape of the Java and .NET APIs. Generic types with ordinary methods —
// [Publisher] is one — have no such restriction, so once you have a publisher it
// reads the same as it does in the other languages.
//
// # Layout
//
// The module is a package per concern, with nothing at its root:
//
//	amqp/      this package: envelopes, codecs, publishing, consuming, retry,
//	           and the in-memory transport
//	rabbitmq/  the RabbitMQ transport
//
// This package has no dependencies outside the standard library. The RabbitMQ
// transport brings its own; importing this package alone does not compile it
// in.
package acemq
