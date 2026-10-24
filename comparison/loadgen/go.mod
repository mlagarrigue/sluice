// The study's load generator lives in its own module, like the study's
// server: it is a program, and the root module must not see it in ./... .
//
// It shares no code with Sluice.go, on purpose: a bug symmetric between the
// client and the server under test would mask itself. So no replace of the
// root, and no import of it. HTTP/1.1 and HTTP/2 are net/http's; HTTP/3 is
// quic-go's client, required here and only here, the version interop/go.mod
// and comparison/sluice/go.mod pin: the root module's graph is untouched
// (ADR 0002), and the generator never measures Sluice.go against itself.
module github.com/mlagarrigue/sluice/comparison/loadgen

go 1.26.0

require github.com/quic-go/quic-go v0.64.1

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)
