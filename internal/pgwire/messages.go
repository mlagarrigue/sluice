// Package pgwire is the PostgreSQL frontend/backend protocol at the byte
// level: message framing in both directions, the message type bytes, and the
// frontend messages a client writes. It knows nothing of rows, codecs or
// connections; database/postgres builds those on it, and the examples'
// fake backend speaks it from the other side.
//
// It is internal: callers of database/postgres never see a message.
package pgwire

import (
	"fmt"
	"math"
)

// Message type bytes, backend then frontend. They are named because a switch
// on 'D' is unreadable and a switch on two different 'D's — DataRow from the
// backend, Describe from the frontend — is a bug waiting for a tired reader.
const (
	// Backend.
	BackendErrorResponse   = 'E'
	BackendNoticeResponse  = 'N'
	BackendRowDescription  = 'T'
	BackendDataRow         = 'D'
	BackendCommandComplete = 'C'
	BackendReadyForQuery   = 'Z'
	BackendParseComplete   = '1'
	BackendBindComplete    = '2'
	BackendNoData          = 'n'
	BackendPortalSuspended = 's'
	BackendEmptyQuery      = 'I'
	BackendAuthentication  = 'R'
	BackendParameterStatus = 'S'
	BackendBackendKeyData  = 'K'
	BackendCloseComplete   = '3'

	// BackendNotificationResponse is asynchronous by design: the docs say to
	// accept it at any point in the protocol, including just before
	// ReadyForQuery, and LISTEN is one Exec away on any connection.
	BackendNotificationResponse = 'A'

	// BackendCopyOutResponse answers a COPY ... TO STDOUT this package has no
	// API to receive; it is recognised so the statement can fail cleanly
	// instead of killing the connection or silently discarding the data.
	BackendCopyOutResponse = 'H'

	// Frontend.
	FrontendQuery     = 'Q'
	FrontendParse     = 'P'
	FrontendBind      = 'B'
	FrontendDescribe  = 'D'
	FrontendExecute   = 'E'
	FrontendSync      = 'S'
	FrontendFlush     = 'H'
	FrontendClose     = 'C'
	FrontendTerminate = 'X'

	// FrontendPassword is the one type byte the frontend reuses: a cleartext
	// PasswordMessage, a SASLInitialResponse and a SASLResponse are all 'p',
	// and which one it is depends only on where the exchange had got to. That
	// is the protocol's design, not an abbreviation made here — and it is why
	// the SASL steps are written as one state machine rather than a switch on
	// the message type.
	FrontendPassword = 'p'
)

// FormatBinary and FormatText are the two wire formats a parameter or a result
// column can take.
//
// This package uses binary throughout, which §8.6 requires rather than
// prefers: text means parsing a number out of decimal digits per value, which
// is the per-element cost the batch exists to remove — and it is lossy in both
// directions for floats.
const (
	FormatText   int16 = 0
	FormatBinary int16 = 1
)

// maxUint16 bounds every count the protocol carries as a 16-bit field —
// parameters, parameter OIDs, result columns. Anything past it cannot be
// written without truncating the count, which is stream corruption; callers
// refuse it before buffering a byte.
const maxUint16 = math.MaxUint16

// CheckParamCounts refuses parameter and OID counts the protocol cannot
// carry. Written once so every caller reports the same limit
// with the same advice, and the next 16-bit count needing the guard has a
// place to join.
func CheckParamCounts(params [][]byte, oids []uint32) error {
	if len(params) > maxUint16 {
		return fmt.Errorf("postgres: %d parameters, and the protocol carries at most %d — pass them as an array parameter", len(params), maxUint16)
	}
	if len(oids) > maxUint16 {
		return fmt.Errorf("postgres: %d parameter OIDs, and the protocol carries at most %d", len(oids), maxUint16)
	}
	return nil
}

// Query appends a simple-protocol query.
//
// It takes no parameters, by construction rather than by omission: the simple
// protocol interpolates values into SQL text, which is the injection surface
// §8.8 removes. Use [Writer.Parse] and [Writer.Bind] for anything carrying a
// value — every value then travels as a parameter, and a thousand of them
// travel as one.
func (wr *Writer) Query(sql string) {
	at := wr.Begin(FrontendQuery)
	wr.AppendString(sql)
	wr.End(at)
}

// Parse appends a Parse message: prepare sql under name, with the given
// parameter type OIDs. An empty name is the unnamed statement, which the
// server replaces on the next Parse — the right choice for a query built per
// batch rather than kept.
//
// A nil or empty paramOIDs leaves the types to the server to infer.
func (wr *Writer) Parse(name, sql string, paramOIDs []uint32) {
	at := wr.Begin(FrontendParse)
	wr.AppendString(name)
	wr.AppendString(sql)
	wr.AppendUint16(uint16(len(paramOIDs))) //nolint:gosec // G115: a statement's parameter count
	for _, oid := range paramOIDs {
		wr.AppendUint32(oid)
	}
	wr.End(at)
}

// BindBinary appends a Bind message with every parameter and every result
// column in binary format.
//
// A nil parameter is sent as NULL, which is why the type is [][]byte and not
// []string: the protocol distinguishes a missing value from an empty one, and
// so must anything that speaks it.
func (wr *Writer) BindBinary(portal, stmt string, params [][]byte) {
	at := wr.Begin(FrontendBind)
	wr.AppendString(portal)
	wr.AppendString(stmt)

	// One format code, applied to every parameter: the protocol's shorthand,
	// and it keeps a batch of a thousand parameters from carrying a thousand
	// identical format codes.
	wr.AppendUint16(1)
	wr.AppendUint16(uint16(FormatBinary))

	wr.AppendUint16(uint16(len(params))) //nolint:gosec // G115: a statement's parameter count
	for _, p := range params {
		if p == nil {
			wr.AppendUint32(^uint32(0)) // -1: NULL
			continue
		}
		wr.AppendUint32(uint32(len(p))) //nolint:gosec // G115: bounded by the caller's value
		wr.AppendBytes(p)
	}

	wr.AppendUint16(1) // one result format code, for every column
	wr.AppendUint16(uint16(FormatBinary))
	wr.End(at)
}

// Describe appends a Describe for a statement ('S') or a portal ('P').
func (wr *Writer) Describe(kind byte, name string) {
	at := wr.Begin(FrontendDescribe)
	wr.AppendBytes([]byte{kind})
	wr.AppendString(name)
	wr.End(at)
}

// Execute appends an Execute for a portal. maxRows of zero means every row,
// which is what a batched read wants: the batch bound lives in [Rows], where
// it can be enforced without a round trip.
func (wr *Writer) Execute(portal string, maxRows uint32) {
	at := wr.Begin(FrontendExecute)
	wr.AppendString(portal)
	wr.AppendUint32(maxRows)
	wr.End(at)
}

// Close appends a Close for a prepared statement ('S') or a portal ('P'),
// releasing what the server holds for it.
//
// It is what makes a bounded statement cache bounded on the *server* as well
// as in this process: evicting an entry from a map frees a string here and
// nothing there, and a backend accumulating prepared statements nobody will
// bind again is a memory leak with no client-side symptom.
func (wr *Writer) Close(kind byte, name string) {
	at := wr.Begin(FrontendClose)
	wr.AppendBytes([]byte{kind})
	wr.AppendString(name)
	wr.End(at)
}

// Sync appends a Sync, which ends the extended-query sequence and is the
// per-batch error boundary §8.5 relies on: on error the backend skips
// everything up to the next Sync, so a batch fails as a batch.
func (wr *Writer) Sync() { wr.Message(FrontendSync, nil) }

// FlushMessage appends a Flush, which tells the backend to deliver what it has
// already produced.
//
// # Why this is not called Flush, and why it is not optional
//
// [Writer.Flush] pushes this client's bytes towards the server. This pushes
// the *server's* bytes towards the client, and the two are opposite
// directions with one name in two vocabularies — hence the suffix rather than
// an overload a reader would have to disambiguate by argument count.
//
// The backend buffers everything an extended-query sequence produces and
// delivers it only on Sync or on this. A client that sends Parse, Bind,
// Describe and Execute and then reads is therefore waiting for an answer the
// server has already written down and is holding, while the server waits for
// the client to say something — a deadlock that ends at whichever deadline
// expires first, and never otherwise. Sync would resolve it and is not usable
// here: Sync closes the portal, and the portal must survive for the Executes
// that fetch the following batches.
//
// This is the one part of the extended protocol a scripted server cannot
// enforce, because a script answers whenever it is read from. It cost a
// connector that could not run a single query.
func (wr *Writer) FlushMessage() { wr.Message(FrontendFlush, nil) }

// Terminate appends a Terminate, the polite end of a connection.
func (wr *Writer) Terminate() { wr.Message(FrontendTerminate, nil) }

// PasswordMessage appends a cleartext password, which is what the server asked
// for when it sent AuthenticationCleartextPassword.
//
// Cleartext means cleartext: the bytes on the wire are the credential. That is
// the caller's transport decision, taken when it chose whether to hand this
// package a *tls.Conn or a plain socket. It is reached only through
// [StartupConfig.AllowCleartextPassword]'s opt-in, which is why it is not
// exported: nothing outside [Startup] should be able to put a credential on
// the wire in the clear by accident.
func (wr *Writer) PasswordMessage(password string) {
	at := wr.Begin(FrontendPassword)
	wr.AppendString(password)
	wr.End(at)
}

// SASLInitialResponse appends the first message of a SASL exchange: the chosen
// mechanism, then the initial response with its own length.
//
// The length is a separate int32 rather than the message's own, because the
// response is binary-safe and the mechanism name before it is not: a C string
// cannot be followed by bytes whose extent nothing declares.
func (wr *Writer) SASLInitialResponse(mechanism string, initial []byte) {
	at := wr.Begin(FrontendPassword)
	wr.AppendString(mechanism)
	wr.AppendUint32(uint32(len(initial))) //nolint:gosec // G115: bounded by the caller's message
	wr.AppendBytes(initial)
	wr.End(at)
}

// SASLResponse appends a continuation of a SASL exchange, which is the message
// bytes and nothing else.
func (wr *Writer) SASLResponse(data []byte) {
	at := wr.Begin(FrontendPassword)
	wr.AppendBytes(data)
	wr.End(at)
}

// Copy-phase message types. CopyData and CopyDone travel in both directions
// under the same type byte — the backend sends them during copy-out — so the
// Backend names are the same values, kept separate for the read loops that
// must say which side they expect them from.
const (
	BackendCopyInResponse = 'G'
	BackendCopyData       = 'd'
	BackendCopyDone       = 'c'
	FrontendCopyData      = 'd'
	FrontendCopyDone      = 'c'
	FrontendCopyFail      = 'f'
)
