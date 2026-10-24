// The audit tool lives in its own module: it is a program that reads a
// machine, an image and a binary and reports what their settings cost,
// and the root module must not see it in ./... .
//
// The standard library alone, and no replace of the root module. It audits
// what it is pointed at — a Go binary here, a host and its dependencies
// later — so it must not depend on the thing it audits, and nothing it
// requires may widen the graph the root module's ADR 0002 fixes.
module github.com/mlagarrigue/sluice/audit

go 1.26.0
