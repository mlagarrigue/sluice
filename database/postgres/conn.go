package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
	"github.com/mlagarrigue/sluice/pushdown"
)

// Row is one row of a result: a view into the flat storage its batch shares,
// not a copy of it.
//
// It is where the row lives, which one it is, and which result it belonged
// to, so a batch of them is one reused slice and the values stay packed end
// to end where a generated hydration function can walk a column across every
// row. Like every batch in this library, a Row is valid only for the
// duration of the call that received its batch.
type Row struct {
	rows *Rows
	idx  int

	// gen is the accumulator's generation when this view was made. The
	// accumulator is the connection's scratch, reused by the next query on
	// the same connection: without the check below, a Row retained past its
	// query would silently read that next query's bytes — under row-level
	// security, another tenant's — where it used to hit a freshly allocated,
	// already-reset accumulator and panic. The check keeps the violation a
	// deterministic crash rather than plausible wrong data.
	gen uint32
}

// ErrRowRetained is what a [Row] panics with when read after its query: the
// batch it arrived in was valid only for the duration of the call that
// received it, and the storage it viewed now belongs to a later result.
var ErrRowRetained = errors.New("postgres: Row read after its query — a batch is valid only for the duration of the call that received it; copy what must be retained")

// Value returns the bytes of one column, and whether it is NULL. The slice
// borrows the batch's storage.
func (r Row) Value(col int) (b []byte, isNull bool) {
	if r.gen != r.rows.gen {
		panic(ErrRowRetained)
	}
	return r.rows.Value(r.idx, col)
}

// Rows returns the batch this row is a view into, for the column scanners
// ([ScanInt8] and its siblings) and the hydration functions sluicegen
// generates, which walk a whole batch at once rather than a row at a time.
// Every Row of one [sluice.Batch] views the same Rows, so one call on any
// item — the first, say — reaches the batch they all came from. Like
// [Row.Value], it refuses a row retained past its query.
func (r Row) Rows() *Rows {
	if r.gen != r.rows.gen {
		panic(ErrRowRetained)
	}
	return r.rows
}

// Fields describes the columns, in order. It is the same slice for every row
// of a result, which is why a column is addressed by index in the hot path and
// looked up by name only when discovering a shape.
func (r Row) Fields() []Field {
	if r.gen != r.rows.gen {
		panic(ErrRowRetained)
	}
	return r.rows.desc
}

// ErrConnBroken reports a connection whose position in the protocol is no
// longer known — a malformed message, a failed write, a read that stopped
// mid-message.
//
// There is no recovery, and pretending otherwise is worse than failing: the
// next query on such a connection would read the previous one's leftovers and
// return them as its own answer, which is a wrong result rather than an error.
var ErrConnBroken = errors.New("postgres: connection is out of sync and cannot be reused")

// ErrConnBusy reports a query started while another is still being consumed on
// the same connection — typically from inside a consumer's own callback.
//
// The protocol allows one conversation at a time, so this was always a
// mistake; what makes it an error rather than a documented caveat is that the
// read path reuses per-query buffers. A nested query would rewrite the storage
// the outer query's delivered rows still point into, which is a wrong answer
// where this is merely a refused one.
var ErrConnBusy = errors.New("postgres: a query is already in progress on this connection")

// Conn is one connection: one conversation, one query at a time.
//
// It is not a pool and not a driver. Pooling is a policy — how many, how long,
// what to do when they run out — and a policy belongs where the application's
// constraints are known, not inside a protocol implementation.
//
// A Conn is not safe for concurrent use. The protocol is a sequence of
// messages with a position in it, so two goroutines sharing one connection
// interleave two conversations into nonsense; that is a protocol violation
// before it is a data race, and no mutex would make it mean anything.
type Conn struct {
	rw     io.ReadWriteCloser
	r      *pgwire.Reader
	w      *pgwire.Writer
	broken error

	// params holds the ParameterStatus values the server announced during
	// startup. It is nil for a Conn built by NewConn, which never saw them.
	params map[string]string

	// encodingChosen is set when the caller named client_encoding in
	// [Config.Params]. Without it, recordParameter breaks the
	// connection on any client_encoding other than UTF8, whenever the server
	// announces it: at startup, or after a SET mid-session.
	encodingChosen bool

	// cancelKey is the pgwire.BackendKeyData the server sent during startup. Written
	// once, there, and read-only afterwards — which is what makes
	// [Conn.CancelKey] the one method another goroutine may call while a
	// query is running. Zero for a Conn built by NewConn.
	cancelKey CancelKey

	// protocol is the version the server settled on during startup, written
	// once there, like cancelKey. Zero for a Conn built by NewConn.
	protocol ProtocolVersion

	// cancelDial is the recipe [Connect] was given, kept so a context that
	// ends mid-statement can reach the server through a second connection.
	// Nil for a Conn built by NewConn.
	cancelDial DialFunc

	// cancelTimeout and logger are [Config.CancelTimeout] and
	// [Config.Logger] as given, zero and nil included; [Conn.sendCancel]
	// and [Conn.log] read the defaults into them.
	cancelTimeout time.Duration
	logger        *slog.Logger

	// inTx records that a transaction is open, whether it was opened by
	// [Conn.Begin] or by a [Copier]. It exists so a second BEGIN is refused:
	// PostgreSQL answers a nested one with a warning and ignores it, and the
	// COMMIT that follows then closes a transaction its author never opened.
	inTx bool

	// txFailed records that the open block is aborted: the server answered
	// ReadyForQuery with 'E'. A COMMIT sent in that state is answered with
	// CommandComplete "ROLLBACK" and no ErrorResponse, so without this byte
	// a Commit that persisted nothing would report success.
	txFailed bool

	// txGen counts finished transactions. A [Tx] captures it and its lazy
	// Sources compare it on consumption, which turns "this stream outlived
	// its transaction" from a wrong answer into an error.
	txGen uint64

	// stmts is the prepared-statement cache, nil when off. See
	// [Conn.PrepareStatements] — including the two places where it loses.
	stmts *stmtCache

	// pendingParse marks a cache entry recorded by prepareFor whose Parse the
	// server has not yet acknowledged. See [Conn.settleParse].
	pendingParse bool
	pendingKey   stmtKey

	// closesSent and closesDone count the evictions prepareFor pipelined in
	// front of the current query's Parse, and how many the server confirmed.
	// See [Conn.settleParse].
	closesSent, closesDone int

	// busy guards the per-query scratch below: a second query started from
	// inside a consumer's callback would silently rewrite the buffers the
	// outer query's delivered rows point into. See [Conn.beginBusy].
	busy bool

	// rtt is the shortest round trip a batched read on this connection
	// timed, from its write to its BindComplete; zero until one did. It
	// sets how many batches the next ones keep asked for: see [depth].
	rtt time.Duration

	// copier is the [Copier] whose COPY is open, nil when none is: from its
	// CopyInResponse to the ReadyForQuery that ends it, the copier's reader
	// goroutine owns r, and every other entry point is refused. See
	// [Conn.claim].
	copier *Copier

	// Per-query scratch, reused across queries: a connection runs one query
	// at a time, and a point-read workload otherwise pays a fresh row
	// accumulator and view slice — the row buffer alone is BatchRows-sized —
	// for every query. The batch contract already forbids retaining any of
	// it beyond the call that received it, and [Row]'s generation check turns
	// a retention that crosses into the next query from silent wrong data
	// into a panic. Measured on the replayed-read benchmarks when this was
	// introduced: BenchmarkPGReadRows 51.0 → 20.5 ns/element and 36 → 6
	// allocations per query, BenchmarkPGPrepareHit −32%, paired A/B against
	// the parent commit on a quiet machine.
	//
	// The reuse protocol lives in [Conn.beginResult] and [Conn.emitRows],
	// which both query.run and readPipeline go through — it is
	// aliasing-sensitive, and two hand-rolled copies of it would drift.
	rowsScratch  Rows
	itemsScratch []Row
	// cutLogged records that the current result's byte-bound cut has been
	// logged, so a wide result warns once rather than once per batch. Reset
	// by [Conn.beginResult].
	cutLogged bool
}

// beginBusy claims the connection for one result's lifetime, refusing a
// nested claim. Before the scratch was shared, a query started from inside a
// consumer's callback failed loudly somewhere in the protocol layer; with the
// shared scratch it would instead rewrite the buffers the outer query's
// delivered rows point into — silent wrong data. One boolean turns that back
// into an immediate error. The caller releases with endBusy on every path out.
func (c *Conn) beginBusy() error { return c.claim(nil) }

// errCopyOpen refuses a statement while a COPY is open on the connection. It
// matches [ErrConnBusy], the family it belongs to, rather than standing as a
// sentinel of its own: the remedy is the same — finish the other
// conversation first — and the message says which one.
var errCopyOpen = fmt.Errorf("%w: a COPY is open on this connection — Close or Abort the Copier first", ErrConnBusy)

// claim is beginBusy for an entry point that may run while a COPY is open:
// owner is the [Copier] that holds it, nil everywhere else.
//
// The refusal is what keeps the COPY finishable. A statement written in
// copy-in mode does not run — the server takes it for a broken COPY and
// aborts the transaction — and its reply would be read by the copy's reader
// goroutine, which owns the connection's Reader until the COPY ends.
func (c *Conn) claim(owner *Copier) error {
	// Every wire-touching entry point comes through here, so this is the one
	// place a broken connection is refused before a byte is written or read.
	if c.broken != nil {
		return c.broken
	}
	if c.copier != nil && c.copier != owner {
		return errCopyOpen
	}
	if c.busy {
		return fmt.Errorf("%w: one conversation at a time, and not from inside a consumer's callback", ErrConnBusy)
	}
	c.busy = true
	return nil
}

func (c *Conn) endBusy() { c.busy = false }

// execBusy is exec behind the busy guard, for the simple-statement entry
// points. The guard covers every entry point, not only query.run and
// Tx.Pipeline, because the gap is not harmless: an Exec issued from inside a
// consumer's callback would go straight to the wire mid-result, and the outer
// query's reader would take the nested statement's answer for its own —
// swapped result streams, reported as ordinary rows. The internal callers that run while a query legitimately
// holds the connection (statement-cache eviction, the Copier's own
// choreography) keep using exec directly, which is why the guard lives in a
// wrapper rather than in exec itself.
func (c *Conn) execBusy(sql string) error {
	if err := c.beginBusy(); err != nil {
		return err
	}
	defer c.endBusy()
	return c.exec(sql)
}

// beginResult readies the connection's per-query scratch for a result with
// the given description, returning the accumulator and the truncated view
// slice to accumulate into.
//
// desc is a fresh slice per result, not scratch: [Row.Fields] hands it out,
// and rewriting a retained description in place would corrupt it silently
// where a fresh one leaves it stale but stable. It is one small allocation
// per query, against the accumulator's buffers — the expensive part — which
// are reused.
func (c *Conn) beginResult(desc []Field, batchRows, batchBytes int) (*Rows, []Row) {
	c.rowsScratch.Init(desc, batchRows, batchBytes)
	c.cutLogged = false
	return &c.rowsScratch, c.itemsScratch[:0]
}

// emitRows hands the accumulated rows to deliver as one batch of row views
// and resets the accumulator. It returns the view slice — written back to the
// scratch so the next result starts from the grown capacity — and whether the
// consumer wants more. A nil or empty accumulator delivers nothing and
// reports true.
//
// The first batch of a result that the byte bound cut is logged, once: a
// result whose rows are so wide that BatchBytes closes batches before
// BatchRows is a result the caller sized for something else. ctx is the
// statement's, and serves the log alone.
func (c *Conn) emitRows(ctx context.Context, rows *Rows, items []Row, deliver func(sluice.Batch[Row]) bool) ([]Row, bool) {
	if rows == nil || rows.Len() == 0 {
		return items, true
	}
	if !c.cutLogged && rows.fullOnBytes() {
		c.cutLogged = true
		c.log().LogAttrs(context.WithoutCancel(ctx), slog.LevelWarn, "postgres: batch cut by its byte bound before its row limit",
			slog.Int("rows", rows.Len()),
			slog.Int("bytes", len(rows.buf)),
			slog.Int("largest_row_bytes", rows.largestRow()),
			slog.Int("batch_rows", rows.maxRows),
			slog.Int("batch_bytes", rows.maxBytes))
	}
	items = items[:0]
	for i := range rows.Len() {
		items = append(items, Row{rows: rows, idx: i, gen: rows.gen})
	}
	c.itemsScratch = items
	ok := deliver(sluice.Batch[Row]{Items: items})
	rows.Reset()
	return items, ok
}

// NewConn wraps a connection whose startup exchange the caller has already
// completed — a net.Conn, a TLS connection, or anything else with the same
// shape.
//
// Use [Connect] instead unless you have a reason not to: it dials, performs
// the startup packet and authentication, and hands back the same thing —
// with cancellation, which a Conn built here does not have. NewConn is
// for a caller who has done that themselves, or who is speaking to something
// that is already past it.
//
// A Conn built this way reports nothing from [Conn.Parameter], because the
// ParameterStatus messages went past before it existed. It holds the one
// parameter it judges to the same rule as a [Connect] without
// client_encoding in its Params: a later announcement of any encoding other
// than UTF8 breaks it.
//
// It starts with the statement cache on; [Conn.PrepareStatements] changes the
// bound or turns it off.
//
// rw is read on one goroutine while it is written on another during a
// [Copier]'s COPY, as a [net.Conn] allows: the server's notices and errors
// are read while the rows go out.
func NewConn(rw io.ReadWriteCloser) *Conn {
	return &Conn{
		rw: rw, r: pgwire.NewReaderSize(rw, ReadBufferSize), w: pgwire.NewWriter(rw),
		stmts: newStmtCache(defaultStatementCacheBound),
	}
}

// ReadBufferSize is how much of the stream a connection pulls in one read.
//
// It exists because the protocol's messages are small and numerous: a result
// of twenty thousand rows is twenty thousand DataRows, and reading each one
// straight from a socket costs two system calls — one for the five-byte
// header, one for the body. Profiling a wide scan put **79% of its time in
// those calls** and 3% in decoding, which is not where anyone would have
// looked. Buffering took that scan from 33.5 ms to 13.5 ms, and a 1000-key
// `= ANY` from 1.80 ms to 0.69 ms.
//
// Every transport is read this way, not only a [net.Conn]: an ssh.Channel or
// a wrapper of the caller's own reaches a socket underneath too, and would pay
// the same two reads per message. A message that fits in the buffer is handed
// over where it lies, with no copy, which made the in-memory replay of a
// thousand rows 25% faster than reading the transport directly.
//
// Sixty-four kilobytes per connection, and a connection is already an
// expensive thing to hold.
const ReadBufferSize = 64 << 10

// Close closes the underlying connection, sending a Terminate first when the
// protocol position still allows it.
func (c *Conn) Close() error {
	if c.broken == nil {
		c.w.Terminate()
		_ = c.w.Flush() // best effort: the socket is closing either way
	}
	return c.rw.Close()
}

// Err reports the failure that broke the connection, or nil.
func (c *Conn) Err() error { return c.broken }

// Parameter returns a runtime parameter the server announced during startup —
// server_version, server_encoding, integer_datetimes, TimeZone — or the empty
// string for one it did not send.
//
// These are the server describing itself, and the values the binary codecs are
// decoded against: a client that assumes UTF-8 and microsecond timestamps can
// check here rather than assume. A Conn from [NewConn] has none of them.
func (c *Conn) Parameter(name string) string { return c.params[name] }

// Protocol returns the protocol version the server settled on during
// startup: [Config.MaxProtocol], or the older version a
// NegotiateProtocolVersion named in its place — [Protocol30] from any
// PostgreSQL before 18 asked for 3.2. A Conn from [NewConn] reports zero, it
// never saw a startup.
func (c *Conn) Protocol() ProtocolVersion { return c.protocol }

// Fetch says how a query asks the server for its rows: in batches, or all at
// once. The zero value, FetchAuto, decides per statement.
type Fetch uint8

const (
	// FetchAuto asks for every row at once when the statement is known to
	// fit in one batch, and in batches otherwise.
	//
	// "Known" means the statement cache remembers it: its last execution on
	// this connection was read to the end and produced at most BatchRows rows.
	// A point read, a lookup, an INSERT ... RETURNING then cost one round
	// trip, as with FetchAll, from their second execution on. A statement run
	// for the first time, one whose last result was larger, or one whose last
	// execution was stopped early or failed is fetched in batches, so a scan
	// keeps its cheap early stop. With the cache off ([Conn.PrepareStatements]
	// with zero) nothing is known and every query is fetched in batches.
	//
	// # Where it loses, which is not a footnote
	//
	// A statement that usually returns a handful of rows, called with a
	// parameter for which it returns millions, **and** abandoned by its
	// consumer partway, pays once what [FetchAll] costs on an early stop: a
	// cancellation for a read outside a transaction block, the whole rest
	// drained for anything else. The statement is then known to be large and
	// fetched in batches the next time. A caller who cannot pay that once
	// sets [FetchBatches].
	FetchAuto Fetch = iota

	// FetchAll asks the server for the whole result at once, saving a round
	// trip and giving up a cheap early stop.
	//
	// # What it saves
	//
	// The batched sequence is Execute-with-a-row-limit, then Flush to make the
	// backend deliver without closing the portal, then a Sync to end the
	// sequence. That Sync is a **second round trip**, and on a result that fit
	// in one batch it buys nothing: the portal was already finished. Measured
	// on loopback, where a round trip is 130 µs, a point read cost 280 µs in
	// batches and 155 µs all at once — the difference is exactly one trip.
	//
	// # What it costs
	//
	// An early stop costs more. With a row limit, a consumer that stops
	// pulling leaves the batches asked for ahead to drain — two on loopback,
	// about one round trip of the server's work further away, see BatchRows. With FetchAll the server has been told to produce
	// everything, and has to be told to stop.
	//
	// When the statement opens with SELECT, VALUES or TABLE and no
	// transaction block is open, the rest is cancelled: two batches are read
	// past the stop, the floor a batched read drains, then a [CancelRequest] goes out by the recipe [Connect]
	// was given while the read goes on to the server's query_canceled, which
	// is not reported. The stop costs those two batches, the cancellation's
	// round trips and what was in flight, not the rest of the result. A
	// cancellation the server does not confirm within [Config.CancelTimeout]
	// closes the connection, as it does for a context.
	//
	// Anything else is drained to its end: inside a transaction block,
	// because a cancellation would abort the whole block; WITH, INSERT ...
	// RETURNING, CALL, because a cancellation would roll back what reading to
	// the end commits; and on a Conn from [NewConn], which has no recipe to
	// cancel with. There, abandoning a ten-million-row result means draining
	// ten million rows to get the connection back to a known position — or
	// closing it.
	//
	// A SELECT that calls a function which writes is cancelled like any
	// SELECT, and its writes are rolled back with it where a drain would
	// have committed them: fetch such a statement with FetchBatches.
	//
	// Client memory is still bounded by BatchRows either way, because rows are
	// decoded and handed on a batch at a time and TCP stops the server when
	// nobody reads. What changes is the cost of not finishing.
	//
	// So: set it for reads whose result is small and always consumed whole,
	// when even the first execution should cost one round trip. [FetchAuto]
	// does the same from the second execution on without being told.
	FetchAll

	// FetchBatches always asks for BatchRows rows at a time, whatever the
	// statement returned before: an early stop always drains at most two
	// batches, and a read that fits in one batch pays a second round trip.
	FetchBatches
)

// defaultBatchBytes is the byte bound on a batch when [QueryConfig.BatchBytes]
// is zero: the same 64 MiB as a message from the server, so the two defaults
// bound memory by one number.
const defaultBatchBytes = 64 << 20

// maxBatchBytes is the largest byte bound a batch accepts. A batch closes
// with the row that crossed its bound in it, and that row is at most
// [maxMaxMessage], so a bound of 1 GiB keeps the batch under the 2 GiB its
// int32 offsets allow: past it, [Rows.Append] would refuse a row the bound
// was meant to cut, not fail on.
const maxBatchBytes = 1 << 30

// batchBounds resolves a QueryConfig's row and byte bounds, zero to its
// default and anything else out of range to an error: a typo behind a
// fallback runs with a bound nobody chose and cannot be found from the
// behaviour it produces (ADR 0018).
func batchBounds(cfg QueryConfig) (rows, size int, err error) {
	rows, size = cfg.BatchRows, cfg.BatchBytes
	if rows < 0 {
		return 0, 0, fmt.Errorf("postgres: QueryConfig.BatchRows %d is negative — zero means the default, %d", rows, sluice.DefaultBatchSize)
	}
	if size < 0 || size > maxBatchBytes {
		return 0, 0, fmt.Errorf("postgres: QueryConfig.BatchBytes %d is outside 1..%d bytes — zero means the default, %d", size, maxBatchBytes, defaultBatchBytes)
	}
	if rows == 0 {
		rows = sluice.DefaultBatchSize
	}
	if size == 0 {
		size = defaultBatchBytes
	}
	return rows, size, nil
}

// QueryConfig states the bounds a query runs under. They are the caller's,
// per S1: none has a value that suits every result.
type QueryConfig struct {
	// BatchRows is how many rows a batch holds, and — because the server is
	// asked for exactly that many at a time — what bounds the rows in flight.
	// Zero means [sluice.DefaultBatchSize]; a negative value is refused.
	//
	// # Batches in flight
	//
	// The next batches are asked for before the current one reaches the
	// consumer, so the server keeps producing while the consumer works:
	// without that, every batch waited one round trip, measured at 3.0 ms of
	// a wide read's 3.5 ms gap with pgx. How many is set once per query, at
	// the end of its first batch: enough to cover one round trip of the
	// connection — the shortest it measured — in batches the size of the
	// first, plus one, and never fewer than two or more than 64. On loopback
	// that is two; at a 10 ms round trip, a wide read of 1,024-row batches
	// keeps about twenty. A statement whose portal suspended before sends
	// that many with the query itself, so its second batch is not a round
	// trip behind its first. A consumer that stops early leaves that many
	// batches to drain, about one round trip of the server's work.
	//
	// The drained batches are executed like any other: a server error in
	// them — a division by zero further down the scan — is reported by the
	// query, and inside a transaction it aborts the transaction, though the
	// consumer never asked for those rows. With a Demand set, the next batch
	// is asked for after delivery instead, one at a time, because the
	// request depends on what the consumer says once it has the batch.
	//
	// # Do not set it to exactly the number of rows you expect
	//
	// Asking for N rows from a query that returns N costs an extra round trip.
	// The server sends the N rows and then PortalSuspended, because it has
	// produced everything it was asked for and cannot know whether more
	// follow; this client then sends another Execute to be told there are
	// none. Measured on loopback, a point read with BatchRows: 1 took 405 µs
	// against 280 µs with BatchRows: 2 — one whole round trip, bought by
	// asking for one row less than you could have.
	//
	// So bound it by what memory allows, not by what the result holds. The
	// trap is gone once the statement is known to fit — [FetchAuto] then asks
	// for every row — but the first execution still falls into it.
	BatchRows int

	// BatchBytes bounds the values a batch holds, in bytes. BatchRows bounds
	// a batch in rows, which says nothing about its size: a thousand rows of
	// a megabyte each are a gigabyte, held until the batch is delivered — and
	// a server, pooler or man in the middle can make every row that wide. A
	// batch whose values reach BatchBytes is delivered with the row that
	// crossed the bound in it, and the next row opens the next batch; nothing
	// fails, the consumer sees more and smaller batches, and [Config.Logger]
	// receives one warning per result. A single row wider than the bound
	// travels in a batch of its own — it is bounded by [Config.MaxMessage] —
	// so memory per batch is at most BatchBytes plus one row.
	//
	// Zero means 64 MiB, the same default as a message from the server; a
	// negative value or one above 1 GiB is refused, the ceiling keeping a
	// batch and its last row within the 2 GiB a batch can address. The cut is on
	// this side of the wire: the server still sends BatchRows rows per round
	// trip.
	BatchBytes int

	// Fetch says whether the rows are asked for in batches of BatchRows or
	// all at once. The zero value, [FetchAuto], asks for everything when the
	// statement is known to fit in one batch, and in batches otherwise.
	Fetch Fetch

	// ParamOIDs gives the types of the parameters, when the server should not
	// infer them. Inference is usually right and occasionally surprising: an
	// integer literal compared against a bigint column can be inferred as
	// int4, and the query then fails on a value that does not fit. State the
	// OIDs where the types matter.
	ParamOIDs []uint32

	// Demand, when set, lets the consumer tell this query what it no longer
	// needs. It is consulted once per batch — one atomic load, the mechanism
	// [pushdown] exists for — and two of its three fields are pushed down:
	//
	//   - The **limit** becomes the row count of the next Execute, so the
	//     server produces exactly what is still wanted and the stream ends
	//     without draining a batch nobody asked for. The figure is the
	//     consumer's: it says how many rows it still wants, republished as it
	//     takes them, and this query never subtracts what it delivered from
	//     it. A demand with nothing left to want ends the query at the next
	//     batch boundary; one that wants nothing from the start sends
	//     nothing to the server at all.
	//   - The **lower bound** is bound, as it is, to the parameter KeyParam
	//     names, and the portal is re-bound: `WHERE id > $1 ORDER BY id`
	//     then resumes from the bound instead of walking the rows below it.
	//     The bound's bytes are the parameter's binary encoding — big-endian
	//     for an integer key, which is what [pushdown.AdvanceBy]'s key
	//     function produces when it encodes the key the way the wire does.
	//
	// The column set is not pushed down: the SQL is the caller's, and
	// rewriting its select list is not this package's job. A consumer that
	// narrows columns is not refused; the narrowing is simply not acted on.
	//
	// # What the query must look like
	//
	// It is the caller's SQL that makes a bound mean "skip": the query is
	// ordered by the key, the key is unique, and the parameter KeyParam names
	// compares against it. Whether the comparison is `>` or `>=` depends on
	// who publishes the bound. [pushdown.AdvanceBy] publishes the key of the
	// last row delivered, so `>` resumes right after it; a bound published by
	// hand that names the first key still wanted takes `>=`, which is the
	// reading [pushdown.Snapshot.LowerBound] documents. The parameter's own
	// value in params stands until the first bound arrives — the floor of the
	// key space, typically.
	//
	// # What it costs
	//
	// A re-bind is a Bind and an Execute in the write the next batch's request
	// was already making — no extra round trip, by the protocol's own rule
	// that binding the unnamed portal again destroys the previous one. It
	// does start the query over from the bound, which on an index scan is one
	// seek. A publisher that advances on every batch, AdvanceBy among them,
	// therefore re-binds on every batch; that is cheap, and what makes the
	// skip real when the bound jumps ahead, but a demand nothing advances
	// costs nothing beyond the atomic load.
	//
	// A demand always fetches in batches, whatever [FetchAuto] knows of the
	// statement. Demand and [FetchAll] are exclusive: FetchAll asks for every
	// row at once, which is the one thing a demand is there to avoid.
	Demand *pushdown.Demand

	// KeyParam is the 1-based index of the parameter the demand's lower bound
	// replaces: 1 for `$1`. Zero means the bound is not pushed down, and only
	// the limit is. It is only read when Demand is set.
	KeyParam int

	// Tag, when set, receives the statement's command tag: an
	// UPDATE ... RETURNING read as a stream delivers its rows and leaves
	// "UPDATE 3" here, whose count [CommandTag.RowsAffected] reads. It is
	// written when the result is read to its CommandComplete, so read it
	// once the source is consumed, next to [sluice.Source.Err].
	//
	// The count is the whole result's. Read in batches, the server's own tag
	// counts only the last one — five rows in batches of two complete as
	// "UPDATE 1" — so the rows of the batches before it are added; the verb
	// is the server's.
	//
	// A query that failed, or whose consumer stopped before the last batch,
	// leaves it empty: the count is unknown, and a tag next to an error
	// would report writes the server may have rolled back.
	// Nil, the default, copies nothing, so a query that does not ask for the
	// tag pays no allocation for it.
	Tag *CommandTag
}

// Query runs sql with the given binary parameters and returns its rows as a
// stream.
//
//	ids := AppendInt8Array(nil, keys)          // one parameter, a thousand keys
//	src := conn.Query(ctx, "SELECT id, total FROM orders WHERE id = ANY($1)",
//	    [][]byte{ids}, QueryConfig{BatchRows: 1024})
//	for b := range src.Stream() {
//	    for _, row := range b.Items { ... }
//	}
//	if err := src.Err(); err != nil { ... }
//
// That shape is the point of this package: a thousand keys leave as one
// parameter in one round trip, no value ever touches SQL text (§8.8), and the
// rows come back as batches the rest of the library already knows how to
// operate on.
//
// # What ctx stops, and what it does not
//
// ctx is checked **once per batch** — before the query is sent, and after
// each batch is delivered. That granularity is not a compromise, it is the
// one the protocol already imposes: rows arrive in groups of BatchRows
// because Execute names a count, so a batch is the smallest unit of work
// there is to interrupt. Checking per row would add a branch to the decode
// loop and buy no earlier stop.
//
// A cancelled query **does not break the connection**. The stream ends, the
// connection is resynchronised to the next ReadyForQuery — bounded by two
// batches, the rows already asked for — and the next query on it works.
// [sluice.Source.Err] reports ctx.Err() wrapped, so errors.Is against
// [context.Canceled] and [context.DeadlineExceeded] answers.
//
// What ctx cannot do on its own is interrupt a statement the server is still
// executing. Between sending a query and the first row, this client is
// blocked in a read and the server is doing work no context of ours reaches.
// A Conn opened by [Connect] closes that gap: a ctx that ends during that
// read opens a second connection with the same recipe and sends
// [CancelRequest], the server aborts the statement, and the stream ends with
// ctx.Err() wrapped — a round trip later rather than when the statement
// would have finished — on a connection that stays usable. A Conn built by
// [NewConn] has no recipe, and two things bound its statement, neither of
// them ctx: a server-side statement timeout, and a [CancelRequest] the
// caller sends itself.
//
// # The consumer's pull reaches the server
//
// Rows are asked for a batch at a time — Execute names the count, the server
// sends that many and suspends — so a consumer that stops pulling stops the
// server a few batches later, and memory is bounded by BatchRows rather than
// by the size of the result. A statement known to fit in one batch is the
// exception: it is asked for whole, in one round trip ([FetchAuto]). The next
// batches are asked for before the current one is delivered — as many as
// cover one round trip — so the server keeps producing while the consumer
// works and the read pays no round trip per batch (see
// [QueryConfig.BatchRows]). That is the deliberate trade: a result of ten
// million rows is readable at all, and back-pressure is real rather than a
// buffer that fills.
//
// # Errors
//
// A [Source] carries them, which is what it exists for (§4.7): a query that
// fails to start produced no row to attach an error to, and one that fails at
// the end has already produced all of them. Consume, then check
// [sluice.Source.Err]. A server error arrives as [*Error] with its SQLSTATE.
//
// An early stop is safe: the connection is resynchronised to the next
// ReadyForQuery before Query's stream returns, so the connection stays usable.
// That drain is bounded by the batches already asked for, which
// is the other reason the server is asked a batch at a time. A query fetched
// whole drains the rest of its result instead; [FetchAuto] says when that
// happens without being asked.
func (c *Conn) Query(ctx context.Context, sql string, params [][]byte, cfg QueryConfig) sluice.Source[Row] {
	// queryErr is what Source.Err reports. It is written by the stream below
	// and read after consumption, on the same goroutine: a Stream is
	// synchronous, so there is nothing to synchronise.
	var queryErr error

	stream := sluice.Stream[Row](func(yield func(sluice.Batch[Row]) bool) {
		// Cleared on both sides: a stream runs the query again each time it
		// is read, and a tag left by the last run must not stand for this one.
		if cfg.Tag != nil {
			*cfg.Tag = ""
		}
		queryErr = c.runQuery(ctx, sql, params, cfg, yield)
		if queryErr != nil && cfg.Tag != nil {
			*cfg.Tag = ""
		}
	})

	return sluice.NewSource(stream, func() error { return queryErr })
}

// runQuery is one execution of [Conn.Query], delivering to yield. A non-nil
// [QueryConfig.Tag] receives the statement's CommandComplete tag; nil copies
// nothing, so a streamed query pays for no tag it was not asked for.
func (c *Conn) runQuery(ctx context.Context, sql string, params [][]byte, cfg QueryConfig, yield func(sluice.Batch[Row]) bool) error {
	if c.broken != nil {
		return c.broken
	}
	// Checked before anything is written: a caller who has already given
	// up should cost the server nothing at all, not one wasted round trip
	// followed by a resynchronisation.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("abandoning the query before it was sent: %w", err)
	}
	if cfg.Fetch > FetchBatches {
		return fmt.Errorf("postgres: QueryConfig.Fetch %d is not FetchAuto, FetchAll or FetchBatches", cfg.Fetch)
	}
	batchRows, batchBytes, err := batchBounds(cfg)
	if err != nil {
		return err
	}
	q := &query{conn: c, ctx: ctx, batchRows: batchRows, batchBytes: batchBytes, fetch: cfg.Fetch, entry: -1, tag: cfg.Tag, ahead: minAhead}
	if cfg.Demand != nil {
		if cfg.Fetch == FetchAll {
			return errors.New("postgres: QueryConfig.Demand and FetchAll are exclusive")
		}
		if cfg.KeyParam < 0 || cfg.KeyParam > len(params) {
			return fmt.Errorf("postgres: QueryConfig.KeyParam %d names no parameter among %d", cfg.KeyParam, len(params))
		}
		q.demand, q.keyParam = pushdown.NewReader(cfg.Demand), cfg.KeyParam
	}
	w := c.watchCancel(ctx)
	defer w.disarm()
	queryErr := c.settleCancel(ctx, w, q.run(sql, params, cfg.ParamOIDs, yield))
	q.recordRows(queryErr)
	// Settled after the deferred resync has read to ReadyForQuery, so the
	// cache's provisional entry is confirmed or dropped on the server's
	// last word — see [Conn.settleParse] for what a kept-but-unparsed
	// entry would poison.
	c.settleParse(queryErr)
	// A plan the server rejected as stale is dropped here rather than
	// retried: the retry would usually work and would hide that something
	// migrated the schema under a live connection. The entry is gone, so
	// the caller's own retry re-prepares and succeeds.
	if isStalePlan(queryErr) {
		c.forgetStatement(sql, cfg.ParamOIDs)
		queryErr = fmt.Errorf("%w: %w", ErrStalePlan, queryErr)
	}
	// A statement the server says it does not hold is dropped for the
	// same reason, from the other direction: this happens on a cache hit
	// whose server-side half has vanished — a pooler's DISCARD ALL, a
	// swapped backend — and keeping the entry re-binds the dead name
	// forever. See [isMissingStatement].
	if isMissingStatement(queryErr) {
		c.forgetStatement(sql, cfg.ParamOIDs)
	}
	return queryErr
}

// query holds one execution's state, so Conn keeps none between calls.
type query struct {
	conn       *Conn
	ctx        context.Context
	batchRows  int
	batchBytes int
	fetch      Fetch
	allRows    bool // decided in run: every row asked for at once
	synced     bool // the Sync went out with the query, so resync must not send another
	stopped    bool // the consumer stopped before the end of the result
	rows       *Rows
	items      []Row // reused: one Row view per row of the current batch

	// cancelOnStop says an early stop may cancel the rest of a result asked
	// for whole rather than read it, if sql only reads: see [query.resync]
	// for when. The verb is read at the stop, never on the way out.
	cancelOnStop bool
	sql          string

	// The pushdown side, nil without a QueryConfig.Demand. stmt and params
	// are kept so a bound can re-bind the portal; bound is the one last
	// bound, so a demand whose generation moved for its limit alone does not
	// re-plan. rowsWanted is the row count the next Execute asks for.
	demand     *pushdown.Reader
	keyParam   int
	stmt       string
	params     [][]byte
	bound      []byte
	rowsWanted int

	// asked counts the Executes sent whose batch has not ended yet; ahead is
	// how many a query without a demand keeps asked for, set by [depth] at
	// its first PortalSuspended. sentAt and boundAt time the round trip and
	// the first batch it is set from: the write, then the BindComplete; each
	// is zero once read, and both are for a query that times nothing.
	asked, ahead    int
	sentAt, boundAt time.Time

	// entry is the statement's place in the cache, -1 without one; rowsRead
	// counts the rows of the result and complete says it was read to its
	// CommandComplete. Together they are what [FetchAuto] knows next time.
	entry    int
	rowsRead int
	complete bool

	// tag receives the CommandComplete tag, nil when nobody asked for it;
	// suspendedRows counts the rows of the Executes the portal suspended,
	// which the server's tag does not.
	tag           *CommandTag
	suspendedRows int
}

// recordRows tells the cache how many rows this execution produced, or that
// nobody knows: an execution stopped early or failed says nothing about the
// statement's size, and must not let the next one be asked for whole.
//
// A query under a demand records nothing. Its limit and its re-bound portal
// shape the result, so its count is not the statement's.
func (q *query) recordRows(err error) {
	c := q.conn
	if q.entry < 0 || q.demand != nil || c.stmts == nil {
		return
	}
	n := -1
	if err == nil && q.complete {
		n = q.rowsRead
	}
	c.stmts.entries[q.entry].lastRows = n
}

func (q *query) run(sql string, params [][]byte, oids []uint32, yield func(sluice.Batch[Row]) bool) (err error) {
	c := q.conn

	// Refused before a byte is buffered: a count past uint16 would be written
	// truncated, which is stream corruption rather than an error.
	if err := pgwire.CheckParamCounts(params, oids); err != nil {
		return err
	}
	if err := checkSQLText(sql); err != nil {
		return err
	}
	// A consumer that already wants nothing costs the server nothing: no
	// statement is prepared, no byte leaves. Checked before beginBusy so the
	// connection is not even marked.
	if q.demand != nil && q.demand.Exhausted() {
		return nil
	}
	if err := c.beginBusy(); err != nil {
		return err
	}
	defer c.endBusy()

	// The statement cache decides what gets bound against. A hit binds a
	// statement the server already parsed and planned — which is the whole
	// point, since re-parsing the same SQL for every batch measures the parser
	// rather than the transport.
	stmt, needsParse, entry := c.prepareFor(sql, oids)
	q.entry = entry
	// The fetch is decided here, once the cache has said what it knows. A
	// statement whose last complete execution fit in one batch is asked for
	// whole: one round trip instead of two — see [FetchAuto].
	switch q.fetch {
	case FetchAll:
		q.allRows = true
	case FetchAuto:
		if entry >= 0 && q.demand == nil {
			last := c.stmts.entries[entry].lastRows
			q.allRows = last >= 0 && last <= q.batchRows
		}
	}

	// Parse, Bind, Describe and the first Execute leave together: §8.5's
	// pipelining, and the reason a query costs two round trips rather than
	// eleven. No Sync yet — Sync ends the sequence, and the portal must stay
	// open for the Executes that follow.
	if needsParse {
		c.w.Parse(stmt, sql, oids)
	}
	q.stmt, q.params, q.rowsWanted = stmt, params, q.batchRows
	if q.demand != nil {
		// A limit or a bound published before the first batch shapes the
		// first request: the Execute asks for what is wanted, the key
		// parameter carries the bound. Nothing to re-bind yet, so the plan
		// is folded into the one Bind that was going out anyway.
		q.replan()
	}
	c.w.BindBinary("", stmt, q.params)
	c.w.Describe('P', "")
	if q.allRows {
		// No row limit and a Sync rather than a Flush: the backend produces
		// everything and ends the sequence itself, so the answer and the
		// ReadyForQuery arrive together. One round trip instead of two — see
		// [FetchAll] for what that gives up.
		c.w.Execute("", 0)
		c.w.Sync()
		q.synced = true
		q.cancelOnStop, q.sql = !c.inTx, sql
	} else {
		c.w.Execute("", uint32(q.rowsWanted)) //nolint:gosec // G115: a caller's batch size
		q.asked = 1
		if q.demand == nil {
			// Asking ahead is safe only for a portal that suspends — see the
			// PortalSuspended case of readChunk — and a statement whose
			// portal suspended before is one: the kind of portal is fixed by
			// the statement. So its Executes leave with the query, and the
			// second batch is not a round trip behind the first.
			if entry >= 0 {
				if n := c.stmts.entries[entry].ahead; n > 0 {
					q.askAhead(n)
				}
			}
			q.sentAt = time.Now()
		}
		// Without this the backend holds everything it has produced until a
		// Sync, and a Sync would close the portal the next Execute needs. See
		// [Writer.FlushMessage].
		c.w.FlushMessage()
	}
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("sending the query: %w", err))
	}

	// resync runs on every path out, including an early stop and a panic in
	// the consumer: a connection left mid-result would hand the next query
	// this one's rows (S10 meeting the protocol's own requirement). An error
	// the drain uncovers — typically the server aborting the statement after
	// the consumer stopped reading — becomes the query's own when nothing
	// more specific already is; swallowing it reported success for a
	// statement the server rolled back.
	defer func() {
		if c.broken != nil {
			return
		}
		if rerr := q.resync(); rerr != nil && err == nil {
			err = rerr
		}
	}()

	for {
		done, err := q.readChunk(yield)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		// The batch boundary, which is where cancellation lands. Returning
		// here leaves the deferred resync to put the connection back at a
		// known position, so the caller loses the query and keeps the
		// connection.
		if err := q.ctx.Err(); err != nil {
			return fmt.Errorf("abandoning the query: %w", err)
		}
		// Without a demand the next batches were asked for before this one
		// was delivered — see the PortalSuspended case of readChunk.
		if q.demand == nil {
			continue
		}
		// The demand is read here, once per batch: the consumer that has
		// just taken a batch may have said it wants nothing more — the
		// query ends, the deferred resync drains the portal — or that it
		// wants less, or from further on.
		if q.demand.Exhausted() {
			return nil
		}
		if q.replan() {
			// The bound moved: the unnamed portal is bound again, which
			// the protocol says destroys the suspended one, so no Sync
			// and no round trip sit between the two. The statement is
			// the same, so no Describe either — the rows keep the shape
			// the first one announced.
			c.w.BindBinary("", q.stmt, q.params)
		}
		// The portal suspended with rows still to come, and the consumer is
		// still taking them: ask for the next batch. This is the pull
		// reaching the server.
		if err := q.askNext(1); err != nil {
			return err
		}
	}
}

// minAhead and maxAhead bound how many batches a query without a demand keeps
// asked for beyond the one being delivered: see [depth].
const (
	minAhead = 2
	maxAhead = 64
)

// depth is how many batches a query without a demand keeps asked for, from
// the connection's round trip and the time its first batch took to arrive:
// enough Executes that, while one travels, the server still has work queued,
// so it never idles waiting to hear of the next batch. That is one round
// trip's worth of batches, rounded up, plus the one being produced.
//
// Two was a constant, and it is still the floor: measured on a wide read
// through a delay proxy, it matched FetchAll on loopback but was 1.6 times
// slower at a 1 ms round trip and 6.3 times at 10 ms, where the server spent
// most of each round trip waiting. A constant large enough for 10 ms would
// make every early stop on loopback drain that many batches for nothing;
// scaled by the round trip, the batches in flight stay about one round trip
// of the server's work, which is also what a stop drains. The ceiling bounds
// what a slow network and a cheap batch would otherwise ask for.
//
// A clock that did not move — a round trip of zero — gives the floor: there
// is nothing to scale by. A batch of zero under a round trip that is not
// gives the ceiling.
func depth(rtt, batch time.Duration) int {
	switch {
	case rtt <= 0:
		return minAhead
	case batch <= 0:
		return maxAhead
	}
	n := int((rtt+batch-1)/batch) + 1
	return min(max(n, minAhead), maxAhead)
}

// askAhead buffers n Executes for the next n batches.
func (q *query) askAhead(n int) {
	for range n {
		q.conn.w.Execute("", uint32(q.rowsWanted)) //nolint:gosec // G115: a caller's batch size
	}
	q.asked += n
}

// askNext asks the suspended portal for the next n batches, in one write.
func (q *query) askNext(n int) error {
	c := q.conn
	q.askAhead(n)
	c.w.FlushMessage()
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("asking for the next batch: %w", err))
	}
	return nil
}

// replan brings the next request in line with the demand, and reports whether
// the portal must be bound again — that is, whether the lower bound moved.
//
// The limit does not need a re-bind: it becomes the row count of the next
// Execute, which the suspended portal honours as it is. Only a bound changes
// what the server would produce next, and only when KeyParam names where it
// goes. A limit below BatchRows caps the request so the server stops exactly
// where the consumer will; a snapshot is taken only when the demand's
// generation moved, so the common batch costs the atomic load and nothing
// else.
func (q *query) replan() (rebind bool) {
	snap, changed := q.demand.Current()
	if !changed {
		return false
	}
	q.rowsWanted = q.batchRows
	if snap.Limit != pushdown.Unlimited && snap.Limit < int64(q.rowsWanted) {
		// Limit 0 never reaches here: Exhausted was checked first, and a
		// zero would make Execute ask for every row.
		q.rowsWanted = int(snap.Limit)
	}
	if q.keyParam == 0 || snap.LowerBound == nil || bytes.Equal(snap.LowerBound, q.bound) {
		return false
	}
	// The caller's slice is not written to: the bound replaces one value
	// in a copy, taken once, that the query then owns.
	if q.bound == nil {
		q.params = append([][]byte(nil), q.params...)
	}
	q.bound = snap.LowerBound
	q.params[q.keyParam-1] = q.bound
	return true
}

// readChunk reads messages until the portal suspends or the query ends,
// yielding whatever rows accumulate. It reports whether the query is finished.
func (q *query) readChunk(yield func(sluice.Batch[Row]) bool) (done bool, err error) {
	c := q.conn
	for {
		m, err := c.r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF // the server closed mid-query
			}
			return false, c.breakConn(fmt.Errorf("reading the result: %w", err))
		}

		switch m.Type {
		case pgwire.BackendRowDescription:
			desc, err := ParseRowDescription(nil, m.Body)
			if err != nil {
				// Not a broken connection. The reader framed this message
				// whole, so the position is still known and the deferred
				// resync can put the connection back — what failed is making
				// sense of a payload, which is a failed query rather than a
				// lost conversation.
				return false, err
			}
			q.rows, q.items = c.beginResult(desc, q.batchRows, q.batchBytes)

		case pgwire.BackendDataRow:
			if q.rows == nil {
				// A row before its description means the stream is being read
				// at the wrong offset, not that the server forgot.
				return false, c.breakConn(fmt.Errorf("%w: DataRow before any RowDescription", ErrProtocol))
			}
			if err := q.rows.Append(m.Body); err != nil {
				// A DataRow the reader framed whole but this package could
				// not read — a column wider than its description said, a
				// field count that disagrees — or, in the batched fetch, a
				// row past the BatchRows the Execute asked for: a server that
				// did not listen. The connection is still at a known position
				// — the message was consumed in full — so the query fails and
				// the resync recovers it. Breaking the connection here throws
				// away a working socket over one unreadable row.
				return false, err
			}
			// Full, the batch is handed on and the next row opens the next
			// one. The batched fetch waits for the PortalSuspended when the
			// row limit is what filled it — the server was told how many to
			// send, and a row past them must reach Append to be refused — but
			// the byte bound cuts on either fetch: it is this side's, and the
			// server knows nothing of it. Polling here, on the per-row loop
			// the regression gate watches, measured within noise of the
			// error path it replaced in a paired A/B of BenchmarkPGReadRows.
			if q.rows.Full() && (q.allRows || q.rows.fullOnBytes()) {
				if !q.emit(yield) {
					q.stopped = true
					return true, nil // the consumer stopped; the deferred resync tidies up
				}
			}

		case pgwire.BackendPortalSuspended:
			// Exactly batchRows rows arrived and more remain.
			q.asked--
			// Without a demand, the next batches are asked for now, before
			// this one reaches the consumer, and at least two are kept asked
			// for: the server produces batch k+1 while the consumer works
			// through batch k, and finds the Execute for k+2 waiting when it
			// is done, so it never idles for a round trip. When a round trip
			// outlasts a batch, two are not enough for that, and the first
			// PortalSuspended sets how many: see [depth]. Asking after delivery put
			// that round trip on every batch, 3.0 ms of a 3.5 ms gap with pgx
			// on a wide read, measured; one batch ahead took back a fifth of
			// it, because the server still waited for k+1 to be read before
			// it heard of k+2, and two kept asked for but topped up after
			// delivery took back three fifths. The price is a first batch
			// that arrives one write later — 5% on its p50, measured — and
			// an early stop that drains the batches asked for.
			//
			// An Execute that reaches a portal already at its end is answered
			// with no rows and a CommandComplete, which the resync discards:
			// a portal that suspends is one the backend fetches from — a
			// SELECT, or a statement whose RETURNING rows it holds in a store
			// — and only the run-once kind, which never suspends, refuses a
			// second Execute (PortalRun in the backend's pquery.c). So asking
			// ahead is safe from the first PortalSuspended on, and never
			// before it.
			//
			// A demand keeps the request after delivery, because the request
			// is shaped by what the consumer says once it has the batch. Sent
			// ahead, it could only carry the bound published after batch
			// k-1, and AdvanceBy, which publishes the last key delivered,
			// would re-bind the portal to start again at batch k.
			if q.demand == nil {
				if !q.boundAt.IsZero() {
					q.ahead = depth(c.rtt, time.Since(q.boundAt))
					q.boundAt = time.Time{}
					if q.entry >= 0 && c.stmts != nil {
						c.stmts.entries[q.entry].ahead = q.ahead
					}
				}
				// Executes sent with the query may already cover the depth.
				if n := q.ahead - q.asked; n > 0 {
					if err := q.askNext(n); err != nil {
						return false, err
					}
				}
			}
			more := q.emit(yield)
			q.suspendedRows = q.rowsRead
			if !more {
				q.stopped = true
				return true, nil // the consumer stopped; the deferred resync tidies up
			}
			return false, nil

		case pgwire.BackendCommandComplete, pgwire.BackendEmptyQuery:
			// The result is finished; what is held is the last batch. Read to
			// the end, its row count is now the statement's to remember.
			q.complete = true
			if q.tag != nil && m.Type == pgwire.BackendCommandComplete {
				// Copied: the body borrows the reader's buffer.
				*q.tag = resultTag(bytes.TrimSuffix(m.Body, []byte{0}), q.suspendedRows)
			}
			q.emit(yield)
			return true, nil

		case pgwire.BackendErrorResponse:
			serverErr, parseErr := ParseError(m.Body)
			if parseErr != nil {
				return false, c.breakConn(parseErr)
			}
			// The connection is not broken by a query error — the server
			// skips to the next Sync and carries on — so the deferred resync
			// puts it back in shape and this connection stays usable.
			return true, serverErr

		case pgwire.BackendNoticeResponse:
			// Notices are out of band and may arrive at any point. Ignored
			// here rather than surfaced: routing them belongs to a diagnostics
			// sink, not to the reader that happens to see them first.

		case pgwire.BackendParameterStatus:
			// The protocol permits one at any point in a session. This server
			// defers them to just before ReadyForQuery, but a client that
			// only tolerates them where it has seen them is relying on an
			// implementation detail to stay connected.
			if err := c.recordParameter(m.Body); err != nil {
				// A payload this package cannot read is not a lost connection:
				// the reader framed the message whole, so the position is
				// still known. Same class as an unreadable DataRow above — the
				// statement fails, the deferred resync recovers, and the
				// socket stays. drainToReady makes the same call. An
				// encoding nobody asked for is the exception: recordParameter
				// broke the connection, and the deferred resync skips it.
				return false, err
			}

		case pgwire.BackendParseComplete:
			// The acknowledgement, and one fact worth keeping: the server now
			// holds the statement, so the cache's provisional entry for it is
			// confirmed.
			c.parseCompleted()

		case pgwire.BackendCloseComplete:
			// An eviction pipelined in front of this query's Parse.
			c.closeCompleted()

		case pgwire.BackendBindComplete:
			// Expected, and the end of the round trip a batched read times.
			// The first batch is timed from here too, rather than from its
			// first DataRow, which would put a branch on every row: only the
			// description, which costs the server nothing, comes between.
			if !q.sentAt.IsZero() {
				now := time.Now()
				if d := now.Sub(q.sentAt); c.rtt == 0 || d < c.rtt {
					c.rtt = d
				}
				q.sentAt, q.boundAt = time.Time{}, now
			}

		case pgwire.BackendNoData:
			// Expected acknowledgement.

		case pgwire.BackendReadyForQuery:
			// Can only appear here if a Sync was already outstanding, and
			// resync consumes it — but its status byte is still the server's
			// word on the transaction state.
			c.noteTxStatus(m.Body)

		case pgwire.BackendNotificationResponse:
			// LISTEN is reachable through Exec, and the docs warn a
			// notification can land just before ReadyForQuery. There is no
			// LISTEN/NOTIFY API to hand it to, so it is discarded — but
			// tolerated, where breaking the connection punished the caller
			// for a message the protocol allows.

		case pgwire.BackendCopyInResponse:
			// A COPY ... FROM STDIN issued through Query. CopyFail must go
			// out before the deferred resync runs: the backend ignores Sync
			// while it waits for CopyData, so without it the resync blocks
			// until the socket deadline. With it, the statement fails and the
			// connection is recovered like any other failed query.
			if err := c.refuseCopyIn(); err != nil {
				return false, err
			}
			return false, errCopyFromStdin

		case pgwire.BackendCopyOutResponse:
			// A COPY ... TO STDOUT issued through Query. Failing the
			// statement — rather than breaking the connection on an unknown
			// type — leaves the deferred resync to discard the CopyData
			// stream up to the ReadyForQuery the Sync draws.
			return false, errCopyToStdout

		default:
			return false, c.breakConn(fmt.Errorf("%w: unexpected message type %q during a query", ErrProtocol, m.Type))
		}
	}
}

// emit hands the accumulated rows to the consumer as one batch and resets the
// accumulator. It reports whether the consumer wants more.
func (q *query) emit(yield func(sluice.Batch[Row]) bool) bool {
	if q.rows != nil {
		q.rowsRead += q.rows.Len()
	}
	items, ok := q.conn.emitRows(q.ctx, q.rows, q.items, yield)
	q.items = items
	return ok
}

// resync sends a Sync and reads to the ReadyForQuery that answers it, leaving
// the connection at a known position.
//
// Everything in between is discarded, including rows the consumer never asked
// for: they were already on their way when it stopped — the batches asked
// for ahead of delivery, and the empty answer of an Execute that reached the
// portal after its end. That is bounded by the batches asked for ahead —
// about one round trip of the server's work, see [depth] — which is the second
// reason the server is asked for a batch at a time rather than for the whole
// result.
//
// A result asked for whole has no such bound: the server was told to
// produce everything. When the consumer stopped early, the statement only
// reads and no transaction block is open, the rest is cancelled instead of
// read — once two batches' worth have been read past the stop, the floor a
// batched read drains, so a short rest costs no cancellation. The
// cancellation goes out by the recipe [Connect] was given, beside this read:
// a server blocked writing to a socket nobody reads takes no cancel, so the
// read must go on until it does. Its query_canceled is this package's own
// doing and is not reported, and the connection is handed back only once
// the server confirmed the request — a request still travelling would
// cancel the next statement instead.
func (q *query) resync() error {
	c := q.conn
	// Fetched whole, the Sync left with the query, so sending another would
	// draw a second ReadyForQuery that nothing reads — and the next query on
	// this connection would take it for its own. What is left to do is read up
	// to the one already coming.
	if !q.synced {
		c.w.Sync()
		if err := c.w.Flush(); err != nil {
			return c.breakConn(fmt.Errorf("resynchronising: %w", err))
		}
	}
	// A server error met on the way is kept rather than skipped. The drain
	// used to discard it, and the discard was not neutral: a consumer that
	// stopped early left the error that aborted its transaction unread here,
	// the query reported success, and every later statement failed 25P02 with
	// nothing client-side to explain why.
	var serverErr error
	var cancel *cancelWatch
	aheadRows, aheadBytes := q.ahead*q.batchRows, q.ahead*q.batchBytes
	for {
		m, err := c.r.Next()
		if err != nil {
			if cancel != nil {
				if cerr := cancel.end(); cerr != nil {
					// The watch closed the connection under this read: the
					// error to report is why, not the read it cut short.
					c.cancelUnconfirmed(q.ctx, cerr, true)
					return withoutOwnCancel(serverErr)
				}
			}
			return c.breakConn(fmt.Errorf("resynchronising: %w", err))
		}
		switch m.Type {
		case pgwire.BackendDataRow:
			if q.stopped && q.cancelOnStop {
				aheadRows--
				aheadBytes -= len(m.Body)
				if aheadRows < 0 || aheadBytes < 0 {
					cancel = q.cancelRest()
				}
			}
		case pgwire.BackendReadyForQuery:
			c.noteTxStatus(m.Body)
			if cancel == nil {
				return serverErr
			}
			if cerr := cancel.end(); cerr != nil {
				c.cancelUnconfirmed(q.ctx, cerr, true)
			}
			return withoutOwnCancel(serverErr)
		case pgwire.BackendErrorResponse:
			if e, parseErr := ParseError(m.Body); serverErr == nil {
				if parseErr != nil {
					serverErr = parseErr
				} else {
					serverErr = e
				}
			}
		case pgwire.BackendParseComplete:
			// Confirmed even while draining: whether the Parse completed is
			// what decides if the cache entry made for it is real — a query
			// abandoned between sending and reading would otherwise forget a
			// statement the server does hold, or worse, keep one it does not.
			c.parseCompleted()
		case pgwire.BackendCloseComplete:
			// Same reasoning for a pipelined eviction: confirmed means the
			// server released it, unconfirmed means it goes out again.
			c.closeCompleted()
		case pgwire.BackendParameterStatus:
			// Kept rather than discarded with the rest: this is where a SET's
			// announcement lands, and dropping it is what makes
			// [Conn.Parameter] report the connection's opening state forever.
			if err := c.recordParameter(m.Body); err != nil {
				// A payload this package cannot read is not a lost
				// connection: the reader framed the message whole, so the
				// position is still known and this read can carry on to the
				// ReadyForQuery it came for. Giving up here would break a
				// connection *while recovering it*, which is the one place
				// that has to succeed — the same call drainToReady makes.
				// Unless recordParameter broke it: an encoding nobody asked
				// for leaves nothing worth recovering.
				if c.broken != nil {
					return err
				}
				if serverErr == nil {
					serverErr = err
				}
			}
		}
	}
}

// cancelRest sends the cancellation for the rest of a result the consumer
// stopped reading, and is called once: it returns nil, and the read goes on
// to the end, when the statement may write, when the context has already
// ended — its own watch is cancelling — or when there is no recipe or key to
// cancel with.
func (q *query) cancelRest() *cancelWatch {
	c := q.conn
	q.cancelOnStop = false
	if !readsOnly(q.sql) || q.ctx.Err() != nil || c.cancelDial == nil || !c.cancelKey.Known() {
		return nil
	}
	return c.cancelNow(q.ctx)
}

// withoutOwnCancel drops the query_canceled an early stop's cancellation
// drew: the consumer asked for nothing more, and the statement only read.
func withoutOwnCancel(err error) error {
	if e, ok := errors.AsType[*Error](err); ok && e.Code == sqlstateQueryCanceled {
		return nil
	}
	return err
}

// noteTxStatus derives the connection's transaction state from ReadyForQuery's
// status byte: 'I' idle, 'T' in a transaction block, 'E' in a failed one.
//
// The byte is the protocol's own answer to a question client bookkeeping can
// only guess at — a guess that `Exec(ctx, "BEGIN")` or a SQL-level COMMIT
// desynchronises silently, after which the next Begin either nests or is
// refused for a transaction that no longer exists. §53.2.3 names this
// byte as *the* way to detect an open or failed block, so every loop that
// consumes a ReadyForQuery reads it. A body that is not the one status byte
// is tolerated rather than fatal: the callers are drains and recovery paths,
// and the message framed whole, so the position is still known.
func (c *Conn) noteTxStatus(body []byte) {
	if len(body) == 1 {
		c.inTx = body[0] == 'T' || body[0] == 'E'
		c.txFailed = body[0] == 'E'
	}
}

// breakConn records why the connection can no longer be used and returns that
// reason, so a caller cannot keep going by ignoring one error.
func (c *Conn) breakConn(cause error) error {
	if c.broken == nil {
		if errors.Is(cause, pgwire.ErrMessageTooLarge) {
			// The one break the caller can configure away, so the error
			// names the field rather than leaving a number to look up.
			cause = fmt.Errorf("%w; Config.MaxMessage raises the limit", cause)
		}
		c.broken = fmt.Errorf("%w: %w", ErrConnBroken, cause)
	}
	return c.broken
}
