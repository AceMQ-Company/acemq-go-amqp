// A module of its own, so the core keeps its single dependency.
//
// Same reasoning as the codec modules and as the separate packages in the Java
// and .NET libraries: an application takes only what it uses. A service that
// publishes messages and traces nothing never resolves go.opentelemetry.io/otel
// at all.
//
// This module's Go floor is 1.25, and the core library's is still 1.23. Four
// advisories against go.opentelemetry.io/otel and .../otel/sdk are fixed no
// earlier than otel v1.45.0, and every otel release from v1.39.0 on requires a
// newer Go than v1.38.0 did — 1.24 from v1.39.0, 1.25 from v1.45.0. Staying on
// v1.38.0 to hold the floor at 1.23 would mean shipping the unpatched
// versions, so the floor moved instead.
//
// Being a module of its own is what makes that affordable: the floor that moved
// is this module's, and a service that publishes messages and traces nothing
// never resolves it, so the core library still builds on Go 1.23 for everyone
// who only wanted a message queue. patterns/sqltest already sits at 1.25 for
// the same kind of reason.
module github.com/AceMQ-Company/acemq-go-amqp/telemetry/otel

go 1.25.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.7.2
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
