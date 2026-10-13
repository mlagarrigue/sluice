package postgres

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

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

	// cancelKey is the pgwire.BackendKeyData the server sent during startup. Written
	// once, there, and read-only afterwards — which is what makes
	// [Conn.CancelKey] the one method another goroutine may call while a
	// query is running. Zero for a Conn built by NewConn.
	cancelKey CancelKey

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
}

// beginBusy claims the connection for one result's lifetime, refusing a
// nested claim. Before the scratch was shared, a query started from inside a
// consumer's callback failed loudly somewhere in the protocol layer; with the
// shared scratch it would instead rewrite the buffers the outer query's
// delivered rows point into — silent wrong data. One boolean turns that back
// into an immediate error. The caller releases with endBusy on every path out.
func (c *Conn) beginBusy() error {
	// Every wire-touching entry point comes through here, so this is the one
	// place a broken connection is refused before a byte is written or read.
	if c.broken != nil {
		return c.broken
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
func (c *Conn) beginResult(desc []Field, batchRows int) (*Rows, []Row) {
	c.rowsScratch.Init(desc, batchRows)
	return &c.rowsScratch, c.itemsScratch[:0]
}

// emitRows hands the accumulated rows to deliver as one batch of row views
// and resets the accumulator. It returns the view slice — written back to the
// scratch so the next result starts from the grown capacity — and whether the
// consumer wants more. A nil or empty accumulator delivers nothing and
// reports true.
func (c *Conn) emitRows(rows *Rows, items []Row, deliver func(sluice.Batch[Row]) bool) ([]Row, bool) {
	if rows == nil || rows.Len() == 0 {
		return items, true
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
// Use [Startup] instead unless you have a reason not to: it performs the
// startup packet and authentication and hands back the same thing. NewConn is
// for a caller who has done that themselves, or who is speaking to something
// that is already past it.
//
// A Conn built this way reports nothing from [Conn.Parameter], because the
// ParameterStatus messages went past before it existed.
func NewConn(rw io.ReadWriteCloser) *Conn {
	return &Conn{rw: rw, r: pgwire.NewReader(readSource(rw)), w: pgwire.NewWriter(rw)}
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
// Sixty-four kilobytes per connection, and a connection is already an
// expensive thing to hold.
const ReadBufferSize = 64 << 10

// readSource buffers a transport whose reads cost a system call, and leaves
// everything else alone.
//
// The test is [net.Conn], because that is where the cost is certain: a socket
// read is a syscall whatever its size. Anything else — a scripted server, a
// replayed conversation, a bytes.Reader — is not wrapped, because buffering
// memory is a pure copy and measured +5% on the replay benchmark. Making a
// harness slower to make a socket faster is a trade nobody asked for.
func readSource(rw io.ReadWriteCloser) io.Reader {
	if _, ok := rw.(net.Conn); ok {
		return bufio.NewReaderSize(rw, ReadBufferSize)
	}
	return rw
}

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

// QueryConfig states the bounds a query runs under. Both are the caller's,
// per S1: neither has a value that suits every result.
type QueryConfig struct {
	// BatchRows is how many rows a batch holds, and — because the server is
	// asked for exactly that many at a time — how many rows may be in flight.
	// A value of zero or less means [sluice.DefaultBatchSize].
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
	// So bound it by what memory allows, not by what the result holds.
	BatchRows int

	// AllRows asks the server for the whole result at once, saving a round
	// trip and giving up a cheap early stop.
	//
	// # What it saves
	//
	// The default sequence is Execute-with-a-row-limit, then Flush to make the
	// backend deliver without closing the portal, then a Sync to end the
	// sequence. That Sync is a **second round trip**, and on a result that fit
	// in one batch it buys nothing: the portal was already finished. Measured
	// on loopback, where a round trip is 130 µs, a point read costs 280 µs the
	// default way and 155 µs this way — the difference is exactly one trip.
	//
	// # What it costs
	//
	// An early stop stops being cheap. With a row limit, a consumer that stops
	// pulling leaves at most BatchRows rows to drain. With AllRows the server
	// has been told to produce everything, so abandoning a ten-million-row
	// result means draining ten million rows to get the connection back to a
	// known position — or closing it.
	//
	// Client memory is still bounded by BatchRows either way, because rows are
	// decoded and handed on a batch at a time and TCP stops the server when
	// nobody reads. What changes is the cost of not finishing.
	//
	// So: set it for reads whose result is small and always consumed whole — a
	// point read, a lookup, a `= ANY($1)` over a bounded key list. Leave it off
	// for a scan that might stop early.
	AllRows bool

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
	// Demand and AllRows are exclusive: AllRows asks for every row at once,
	// which is the one thing a demand is there to avoid.
	Demand *pushdown.Demand

	// KeyParam is the 1-based index of the parameter the demand's lower bound
	// replaces: 1 for `$1`. Zero means the bound is not pushed down, and only
	// the limit is. It is only read when Demand is set.
	KeyParam int
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
// ctx is checked **once per batch** — before the query is sent, and before
// each request for the next batch. That granularity is not a compromise, it
// is the one the protocol already imposes: rows arrive in groups of BatchRows
// because Execute names a count, so between two checks there is exactly one
// batch of work in flight and nothing finer to interrupt. Checking per row
// would add a branch to the decode loop and buy no earlier stop.
//
// A cancelled query **does not break the connection**. The stream ends, the
// connection is resynchronised to the next ReadyForQuery — bounded by
// BatchRows, the rows already asked for — and the next query on it works.
// [sluice.Source.Err] reports ctx.Err() wrapped, so errors.Is against
// [context.Canceled] and [context.DeadlineExceeded] answers.
//
// What ctx cannot do is interrupt a statement the server is still executing.
// Between sending a query and the first row, this client is blocked in a read
// and the server is doing work no context of ours reaches. Two things bound
// that, and neither is ctx: a server-side statement timeout, which is the one
// to reach for, and [CancelRequest], which is best-effort and needs a second
// connection.
//
// # The consumer's pull reaches the server
//
// Rows are asked for a batch at a time — Execute names the count, the server
// sends that many and suspends — so a consumer that stops pulling stops the
// server, and memory is bounded by BatchRows rather than by the size of the
// result. The cost is one round trip per batch instead of one for the whole
// result. That is the deliberate trade: a result of ten million rows is
// readable at all, and back-pressure is real rather than a buffer that fills.
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
// That drain is bounded by BatchRows — the rows already asked for — which is
// the other reason the server is asked a batch at a time.
func (c *Conn) Query(ctx context.Context, sql string, params [][]byte, cfg QueryConfig) sluice.Source[Row] {
	batchRows := cfg.BatchRows
	if batchRows <= 0 {
		batchRows = sluice.DefaultBatchSize
	}

	// queryErr is what Source.Err reports. It is written by the stream below
	// and read after consumption, on the same goroutine: a Stream is
	// synchronous, so there is nothing to synchronise.
	var queryErr error

	stream := sluice.Stream[Row](func(yield func(sluice.Batch[Row]) bool) {
		if c.broken != nil {
			queryErr = c.broken
			return
		}
		// Checked before anything is written: a caller who has already given
		// up should cost the server nothing at all, not one wasted round trip
		// followed by a resynchronisation.
		if err := ctx.Err(); err != nil {
			queryErr = fmt.Errorf("abandoning the query before it was sent: %w", err)
			return
		}
		q := &query{conn: c, ctx: ctx, batchRows: batchRows, allRows: cfg.AllRows}
		if cfg.Demand != nil {
			if cfg.AllRows {
				queryErr = errors.New("postgres: QueryConfig.Demand and AllRows are exclusive")
				return
			}
			if cfg.KeyParam < 0 || cfg.KeyParam > len(params) {
				queryErr = fmt.Errorf("postgres: QueryConfig.KeyParam %d names no parameter among %d", cfg.KeyParam, len(params))
				return
			}
			q.demand, q.keyParam = pushdown.NewReader(cfg.Demand), cfg.KeyParam
		}
		queryErr = q.run(sql, params, cfg.ParamOIDs, yield)
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
	})

	return sluice.NewSource(stream, func() error { return queryErr })
}

// query holds one execution's state, so Conn keeps none between calls.
type query struct {
	conn      *Conn
	ctx       context.Context
	batchRows int
	allRows   bool
	synced    bool // the Sync went out with the query, so resync must not send another
	rows      *Rows
	items     []Row // reused: one Row view per row of the current batch

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
	stmt, needsParse, err := c.prepareFor(sql, oids)
	if err != nil {
		return err
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
		// [QueryConfig.AllRows] for what that gives up.
		c.w.Execute("", 0)
		c.w.Sync()
		q.synced = true
	} else {
		c.w.Execute("", uint32(q.rowsWanted)) //nolint:gosec // G115: a caller's batch size
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
		// The demand is read here, once per batch: the consumer that has
		// just taken a batch may have said it wants nothing more — the
		// query ends, the deferred resync drains the portal — or that it
		// wants less, or from further on.
		if q.demand != nil {
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
		}
		// The portal suspended with rows still to come, and the consumer is
		// still taking them: ask for the next batch. This is the pull
		// reaching the server.
		c.w.Execute("", uint32(q.rowsWanted)) //nolint:gosec // G115: a caller's batch size
		c.w.FlushMessage()
		if err := c.w.Flush(); err != nil {
			return c.breakConn(fmt.Errorf("asking for the next batch: %w", err))
		}
	}
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
			q.rows, q.items = c.beginResult(desc, q.batchRows)

		case pgwire.BackendDataRow:
			if q.rows == nil {
				// A row before its description means the stream is being read
				// at the wrong offset, not that the server forgot.
				return false, c.breakConn(fmt.Errorf("%w: DataRow before any RowDescription", ErrProtocol))
			}
			if err := q.rows.Append(m.Body); err != nil {
				// Cold path. Under AllRows the backend was given no row limit,
				// so nothing pauses it at BatchRows — the pause that used to
				// do that was the PortalSuspended a limit produces — and the
				// batch fills mid-result. Emitting here rather than testing
				// Full() before every Append is not a style choice: this is
				// the per-row loop the regression gate watches, and a test in
				// front of the Append measured +2.4% in a paired A/B. The
				// Append already had to be checked, so the check is free.
				if !q.allRows || !q.rows.Full() {
					// A DataRow the reader framed whole but this package
					// could not read: a column wider than its description
					// said, a field count that disagrees. The connection is
					// still at a known position — the message was consumed in
					// full — so the query fails and the resync recovers it.
					// Breaking the connection here throws away a working
					// socket over one unreadable row.
					return false, err
				}
				if !q.emit(yield) {
					return true, nil // the consumer stopped; the deferred resync tidies up
				}
				if err := q.rows.Append(m.Body); err != nil {
					// The batch was just emptied, so this retry cannot fail for
					// fullness: the row itself is unreadable. Same class as the
					// direct path above — the message was consumed whole, the
					// position is known, the statement fails and the connection
					// stays.
					return false, err
				}
			}

		case pgwire.BackendPortalSuspended:
			// Exactly batchRows rows arrived and more remain.
			if !q.emit(yield) {
				return true, nil // the consumer stopped; the deferred resync tidies up
			}
			return false, nil

		case pgwire.BackendCommandComplete, pgwire.BackendEmptyQuery:
			// The result is finished; what is held is the last batch.
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
				// socket stays. drainToReady makes the same call.
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

		case pgwire.BackendBindComplete, pgwire.BackendNoData:
			// Expected acknowledgements.

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
	items, ok := q.conn.emitRows(q.rows, q.items, yield)
	q.items = items
	return ok
}

// resync sends a Sync and reads to the ReadyForQuery that answers it, leaving
// the connection at a known position.
//
// Everything in between is discarded, including rows the consumer never asked
// for: they were already on their way when it stopped. That is bounded by the
// batch size, which is the second reason the server is asked for a batch at a
// time rather than for the whole result.
func (q *query) resync() error {
	c := q.conn
	// Under AllRows the Sync left with the query, so sending another would
	// draw a second ReadyForQuery that nothing reads — and the next query on
	// this connection would take it for its own. What is left to do is read up
	// to the one already coming.
	//
	// That read is bounded by the rest of the result rather than by BatchRows,
	// which is the cost AllRows names: an abandoned ten-million-row scan is
	// drained ten million rows.
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
	for {
		m, err := c.r.Next()
		if err != nil {
			return c.breakConn(fmt.Errorf("resynchronising: %w", err))
		}
		switch m.Type {
		case pgwire.BackendReadyForQuery:
			c.noteTxStatus(m.Body)
			return serverErr
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
				if serverErr == nil {
					serverErr = err
				}
			}
		}
	}
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
		c.broken = fmt.Errorf("%w: %w", ErrConnBroken, cause)
	}
	return c.broken
}
