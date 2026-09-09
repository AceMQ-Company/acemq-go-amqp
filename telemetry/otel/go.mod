// A module of its own, so the core keeps its single dependency.
//
// Same reasoning as the codec modules and as the separate packages in the Java
// and .NET libraries: an application takes only what it uses. A service that
// publishes messages and traces nothing never resolves go.opentelemetry.io/otel
// at all.
//
// v1.38.0 is the newest OpenTelemetry release that still builds on Go 1.23,
// which is the floor the library targets. v1.39.0 requires 1.24, and taking it
// would move that floor for everyone who only wanted a message queue.
module github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel

go 1.23.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.5.0
	go.opentelemetry.io/otel v1.38.0
	go.opentelemetry.io/otel/sdk v1.38.0
	go.opentelemetry.io/otel/trace v1.38.0
)

require (
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
