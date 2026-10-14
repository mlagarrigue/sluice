// The study's load generator lives in its own module, like the study's
// server: it is a program, and the root module must not see it in ./... .
//
// It shares no code with Sluice.go, on purpose: a bug symmetric between the
// client and the server under test would mask itself. So no replace of the
// root, and no import of it — the standard library only, until HTTP/3 asks
// for a client (a decision to record then).
module github.com/mlagarrigue/sluice/comparison/loadgen

go 1.26.0
