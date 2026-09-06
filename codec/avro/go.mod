// A module of its own, so the core keeps its single dependency.
module github.com/AceMQ-Company/acemq-go-amqp/codec/avro

go 1.23.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.1.0
	github.com/hamba/avro/v2 v2.30.0
)

require (
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
