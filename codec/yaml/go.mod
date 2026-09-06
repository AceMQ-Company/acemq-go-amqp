// A module of its own, so the core keeps its single dependency.
//
// Same reasoning as the separate packages in the Java and .NET libraries: an
// application takes only the formats it sends, and pays only for those.
module github.com/AceMQ-Company/acemq-go-amqp/codec/yaml

go 1.23

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.1.0
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
