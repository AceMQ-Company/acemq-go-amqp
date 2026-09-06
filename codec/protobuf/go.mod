// A module of its own, so the core keeps its single dependency.
module github.com/AceMQ-Company/acemq-go-amqp/codec/protobuf

go 1.23

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.1.4
	google.golang.org/protobuf v1.36.10
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
