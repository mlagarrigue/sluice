// The study's server lives in its own module, like benchmarks/ on the
// benchmarks branch: it is a program, not a package, and the root module
// must not see it in ./... — a main with flags and an integration test that
// needs a PostgreSQL server has no place in a library's own test run.
//
// It replaces the root with the working tree, so a campaign measures the
// commit it is run from and nothing else. The day axis B lands, pgx is
// required here and only here.
module github.com/mlagarrigue/sluice/comparison/sluice

go 1.26.0

replace github.com/mlagarrigue/sluice => ../..

require github.com/mlagarrigue/sluice v0.0.0

require (
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
