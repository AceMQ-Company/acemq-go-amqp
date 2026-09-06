// A module of its own, so the core keeps its single dependency.
module github.com/AceMQ-Company/acemq-go-amqp/codec/toml

go 1.23

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.1.0
	github.com/BurntSushi/toml v1.5.0
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
