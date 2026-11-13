// The study's first contender: the HTTP server of the standard library and
// pgx, written as their documentation teaches, without grouping. A module of
// its own so that its graph is nobody else's: it does not require the root
// module (a contender owes Sluice.go nothing), and pgx and quic-go are the
// versions comparison/sluice pins, so the two measure the same driver and the
// same HTTP/3 stack.
module github.com/mlagarrigue/sluice/comparison/go-nethttp

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/quic-go/quic-go v0.65.0
	github.com/vikstrous/dataloadgen v0.0.10
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	go.opentelemetry.io/otel v1.11.1 // indirect
	go.opentelemetry.io/otel/trace v1.11.1 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)
