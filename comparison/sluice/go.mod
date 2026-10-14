// The study's server lives in its own module, like benchmarks/ on the
// benchmarks branch: it is a program, not a package, and the root module
// must not see it in ./... — a main with flags and an integration test that
// needs a PostgreSQL server has no place in a library's own test run.
//
// It replaces the root with the working tree, so a campaign measures the
// commit it is run from and nothing else. pgx is required here and only here
// (axis B of the study): the root module's graph is untouched (ADR 0002), and
// the version is the one benchmarks/go.mod pins, so the two modules measure
// the same driver. quic-go, likewise, is interop/go.mod's version: the test's
// HTTP/3 client (axis A), never a dependency of the root.
module github.com/mlagarrigue/sluice/comparison/sluice

go 1.26.0

replace github.com/mlagarrigue/sluice => ../..

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/mlagarrigue/sluice v0.0.0
	github.com/quic-go/quic-go v0.63.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)
