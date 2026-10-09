// Package postgres speaks the PostgreSQL frontend/backend protocol, version 3,
// over the standard library alone.
//
// It exists because database/sql cannot batch (docs/design/architecture.md,
// "`database/sql` does not fit"):
// its interface hands back one row at a time, which is the shape this library
// was built to escape. Reimplementing the wire protocol is what buys
// `= ANY($1)` over a thousand keys in one round trip, and binary COPY over a
// batch — the difference the gateway's measured ×6.9 is made of.
//
// # The user path
//
// [Startup] opens a connection over any [io.ReadWriteCloser] — a plain socket
// or a *tls.Conn the caller has already verified — and authenticates it. From
// there a [Conn] runs [Conn.Query] for rows as a [sluice.Source] of [Row]
// batches, [Conn.Exec] for statements without a result, [Conn.Begin] for a
// [Tx] with savepoints and SET LOCAL, and [Conn.CopyFrom] for binary COPY of
// a batch at a time. Parameters and results are binary: the Append* and
// Decode* codecs in codec.go, array.go and nullable.go encode a Go value to
// the server's own representation and back, and the array codecs are what
// make `WHERE id = ANY($1)` with a thousand keys one round trip rather than a
// thousand. [Conn.PrepareStatements] turns on a bounded statement cache.
//
// # Authentication
//
// SCRAM-SHA-256 is the default and needs nothing turned on. Cleartext
// password authentication is an explicit opt-in through
// [StartupConfig.AllowCleartextPassword], refused otherwise: the bytes on the
// wire are the credential, and only the caller knows whether the transport
// they handed over may carry one.
//
// # The wire layer is internal
//
// The protocol's framing and message layer lives in internal/pgwire and is
// not part of this package's API: a mistake at the message layer is a
// connection whose position in the protocol is unknown rather than an error
// that can be returned. A caller wants [Startup] and what hangs off a [Conn];
// [NewConn] exists for a transport the caller has already authenticated.
package postgres
