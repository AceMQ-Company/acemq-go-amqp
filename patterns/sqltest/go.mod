// A module of its own, holding the tests that need a real database.
//
// The pure-Go SQLite driver requires a newer Go than the library targets, and
// raising the whole library's floor to test three files would cost every user
// of it the thing the floor is there for. A nested module keeps that cost here:
// nothing in the parent's go.mod changes, and a consumer never sees this.
module github.com/AceMQ-Company/acemq-go-amqp/patterns/sqltest

go 1.25.0

require (
	github.com/AceMQ-Company/acemq-go-amqp v0.1.4
	modernc.org/sqlite v1.58.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.47.0 // indirect
	modernc.org/libc v1.75.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)

replace github.com/AceMQ-Company/acemq-go-amqp => ../..
