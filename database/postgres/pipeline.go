package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// ErrPipelineAborted reports the parts of a pipeline the server never ran.
//
// It exists because of how the protocol handles a failure mid-sequence: on an
// error the backend discards everything up to the next Sync. A pipeline sends
// one Sync at the end, so a part that fails takes every part after it with it —
// they are not slow, not queued, not retried. They did not happen.
//
// That is the price of the round trip this type saves, and it is surfaced with
// the index rather than folded into the failure, because a caller holding
// sixty-four waiting callers needs to know which of them were answered.
var ErrPipelineAborted = errors.New("postgres: the remaining pipeline parts were discarded by the server's error recovery")

// Part is one principal's query inside a [Tx.Pipeline].
type Part struct {
	// Setting and Value are the transaction-local parameter to establish
	// before the query — `app.tenant_id` and the tenant, in the row-level
	// security case this type exists for. An empty Setting runs the query
	// under whatever is already in force.
	Setting string
	Value   string

	// SQL is the statement, and Params its binary parameters.
	SQL    string
	Params [][]byte

	// ParamOIDs declares the parameter types, as [QueryConfig.ParamOIDs] does.
	ParamOIDs []uint32
}

// Pipeline runs one query per part in a single round trip.
//
// # What it is for
//
// Row-level security keys on transaction state, so a batch carrying several
// principals needs one `SET LOCAL` and one query per principal. Issued the
// ordinary way those are sequential: on a single-goroutine connection, K
// principals cost K round trips of latency borne by *every* caller in the
// batch, including the ones whose principal was resolved first. Measured on a
// loopback connection — the smallest round trip there is — that curve is
// linear at roughly 660 µs per principal, so a batch of sixty-four distinct
// tenants costs 42 ms and the gateway has bought nothing.
//
// This sends all of it before a single Sync and reads the answers back in
// order, which turns K round trips into one plus K times the server's own
// work. Measured against the sequential form on the same loopback connection:
//
//	principals   sequential   pipelined
//	         1       797 µs      444 µs
//	         4      2.81 ms      547 µs
//	        16      10.8 ms      835 µs
//	        64      42.7 ms     1.98 ms
//
// The pipelined curve is nearly flat — sixty-four times the principals for
// four and a half times the latency — because what grows is the server's work
// rather than the number of waits. The marginal cost of a principal falls from
// a round trip to about 24 µs.
//
//	err := tx.Pipeline(ctx, parts, cfg, func(i int, b sluice.Batch[Row]) bool {
//	    return serve(parts[i], b)   // rows for part i
//	})
//
// # A failure stops everything after it
//
// The protocol gives no choice here: an error makes the backend skip to the
// next Sync, and there is one Sync. So a part that fails aborts the rest, and
// the error returned wraps [ErrPipelineAborted] carrying the index that failed.
// Everything before it ran and its rows were delivered; everything after it did
// not run at all. A caller with waiting callers to answer must treat the tail
// as unserved rather than as failed.
//
// Retrying the tail needs a **new transaction**. The failure left this one
// aborted: every further statement in it fails with SQLSTATE 25P02 until it
// ends, and [Tx.Commit] then returns [ErrTxCommitRollback] — the server rolls
// back instead of committing. That rollback takes the parts before the failure
// with it, so their rows were read but anything they wrote is gone; a part
// whose effect mattered has to be retried too, not only the tail.
//
// That is a real cost against issuing the queries separately, where each has
// its own Sync and its own error boundary. It buys the round trip, and which
// side of the trade a workload wants depends on how often a part fails.
//
// # ctx guards the start, not the read
//
// ctx is checked **once**, before anything is sent. After the Flush the whole
// sequence is already on its way to the server and the reader runs to the
// final ReadyForQuery without consulting ctx again — unlike [Conn.Query],
// which checks once per batch because its Execute asks for a batch at a time
// and each pause is a place to stop. Here every part runs with no row limit,
// so there is no pause to stop at: a cancellation observed mid-read could
// only abandon the connection or drain it anyway, and the drain is the same
// cost as finishing. What bounds a read against a hung server is the socket
// deadline, and what bounds a long statement is a server-side timeout — the
// same two answers as for Query.
//
// # The rows belong to the batch
//
// fn is called once per part that produced rows, with the batch valid only for
// the duration of the call — the library's ordinary rule. Returning false
// stops delivery; the connection is still drained to its ReadyForQuery, because
// leaving it mid-pipeline would hand the next query these rows.
func (tx *Tx) Pipeline(ctx context.Context, parts []Part, cfg QueryConfig, fn func(int, sluice.Batch[Row]) bool) error {
	if fn == nil {
		return errors.New("postgres: Tx.Pipeline requires a callback")
	}
	if err := tx.check(ctx); err != nil {
		return err
	}
	if len(parts) == 0 {
		return nil
	}
	c := tx.conn

	batchRows := cfg.BatchRows
	if batchRows <= 0 {
		batchRows = sluice.DefaultBatchSize
	}

	// Every part is validated before the first byte is buffered — the same
	// order [Tx.Savepoints] uses, and not for style: a refusal discovered
	// mid-loop would leave the parts already written sitting in the Writer's
	// buffer, unsent. The next query on this connection would flush them ahead
	// of its own messages, the server would execute them, and the reader would
	// take their answers for the new query's — a silently wrong result, not an
	// error.
	for i, p := range parts {
		if p.Setting != "" {
			if err := validSettingName(p.Setting); err != nil {
				return fmt.Errorf("pipeline part %d: %w", i, err)
			}
		}
		if err := pgwire.CheckParamCounts(p.Params, p.ParamOIDs); err != nil {
			return fmt.Errorf("pipeline part %d: %w", i, err)
		}
		if err := checkSQLText(p.SQL); err != nil {
			return fmt.Errorf("pipeline part %d: %w", i, err)
		}
	}

	if err := c.beginBusy(); err != nil {
		return err
	}
	defer c.endBusy()

	// Everything goes out before anything is read. The statement cache is
	// deliberately not consulted: a cached name would have to be prepared in
	// an earlier round trip, which is the round trip this is removing.
	for _, p := range parts {
		if p.Setting != "" {
			c.w.Parse("", "SELECT set_config($1, $2, true)", []uint32{OIDText, OIDText})
			c.w.BindBinary("", "", [][]byte{AppendText(nil, p.Setting), AppendText(nil, p.Value)})
			// Zero, not one. A row limit makes the backend answer
			// PortalSuspended instead of CommandComplete when it has produced
			// that many and does not know whether more follow — and this
			// reader counts CommandCompletes to know which part it is on. Asking
			// for "one row" from a function that returns exactly one row is the
			// intuitive spelling and it silently shifts every later part's rows
			// onto the part before it.
			c.w.Execute("", 0)
		}
		c.w.Parse("", p.SQL, p.ParamOIDs)
		c.w.BindBinary("", "", p.Params)
		c.w.Describe('P', "")
		// Zero means every row: a suspended portal would need another Execute,
		// and asking for one mid-pipeline is a second round trip — the thing
		// this exists to avoid. The bound that matters here is the caller's
		// query, not the transport.
		c.w.Execute("", 0)
	}
	c.w.Sync()
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("sending the pipeline: %w", err))
	}

	return c.readPipeline(parts, batchRows, fn)
}

// readPipeline consumes the answers to a pipelined sequence, in order.
//
// The state it tracks is which part is being answered and whether the current
// part is the set_config or the query. Nothing separates them on the wire —
// they are two extended-query sequences back to back — so the reader counts
// CommandCompletes rather than looking for a marker that does not exist.
//
// ctx is deliberately absent: cancellation guarded the start, in
// [Tx.Pipeline], and everything this reads was bought by the Flush already.
// Stopping mid-answer would still owe the drain to ReadyForQuery — the whole
// pipeline, since parts run with no row limit — so checking here buys no
// earlier stop. See the "ctx guards the start" section on [Tx.Pipeline].
func (c *Conn) readPipeline(parts []Part, batchRows int, fn func(int, sluice.Batch[Row]) bool) error {
	// A panic in fn would otherwise abandon the connection mid-pipeline with
	// broken still nil — and the next query would read this pipeline's
	// leftover rows as its own. The same rule as query.run's deferred resync
	// (S10), paid only on the panic path. The drain reads every undelivered
	// row of every remaining part before the panic continues — the same cost
	// the documented early-stop path (fn returning false) already carries,
	// since parts run with no row limit. A caller who must abandon the
	// connection immediately closes it instead of recovering.
	defer func() {
		if r := recover(); r != nil {
			if c.broken == nil {
				_ = c.drainToReady()
			}
			panic(r)
		}
	}()
	var (
		part      int
		inSetting = parts[0].Setting != ""
		rows      *Rows
		items     []Row
		stopped   bool
		failed    error
		failedAt  = -1
	)

	deliver := func(b sluice.Batch[Row]) bool { return fn(part, b) }
	emit := func() bool {
		var ok bool
		items, ok = c.emitRows(rows, items, deliver)
		return ok
	}

	for {
		m, err := c.r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return c.breakConn(fmt.Errorf("reading the pipeline: %w", err))
		}

		switch m.Type {
		case pgwire.BackendRowDescription:
			if err := c.checkPipelinePart(part, len(parts)); err != nil {
				return err
			}
			desc, err := ParseRowDescription(nil, m.Body)
			if err != nil {
				return c.abortPipeline(err)
			}
			if !inSetting {
				// The connection's per-query scratch, exactly as query.run
				// uses it: parts run one after another, and each part's rows
				// are delivered before the next one's description arrives.
				rows, items = c.beginResult(desc, batchRows)
			}

		case pgwire.BackendDataRow:
			if err := c.checkPipelinePart(part, len(parts)); err != nil {
				return err
			}
			// A set_config's row is its return value, which nobody reads.
			if inSetting || rows == nil || stopped {
				continue
			}
			if err := rows.Append(m.Body); err != nil {
				return c.abortPipeline(err)
			}
			if rows.Full() && !emit() {
				stopped = true
			}

		case pgwire.BackendCommandComplete, pgwire.BackendEmptyQuery:
			if err := c.checkPipelinePart(part, len(parts)); err != nil {
				return err
			}
			if inSetting {
				inSetting = false
				continue
			}
			if !stopped && !emit() {
				stopped = true
			}
			rows = nil
			part++
			if part < len(parts) {
				inSetting = parts[part].Setting != ""
			}

		case pgwire.BackendErrorResponse:
			serverErr, parseErr := ParseError(m.Body)
			if parseErr != nil {
				return c.abortPipeline(parseErr)
			}
			if failed == nil {
				failed, failedAt = serverErr, part
			}
			// Everything after this is discarded by the backend until the
			// Sync, so there is nothing left to read but the ReadyForQuery.

		case pgwire.BackendReadyForQuery:
			c.noteTxStatus(m.Body)
			if failed != nil {
				skipped := len(parts) - failedAt - 1
				return fmt.Errorf("pipeline part %d: %w (%w: %d later parts never ran)",
					failedAt, failed, ErrPipelineAborted, skipped)
			}
			return nil

		case pgwire.BackendParseComplete, pgwire.BackendBindComplete, pgwire.BackendNoData,
			pgwire.BackendPortalSuspended, pgwire.BackendCloseComplete:
			// Acknowledgements. PortalSuspended cannot happen with a row limit
			// of zero, and is tolerated rather than treated as a protocol
			// error so that a future change to that limit fails a test rather
			// than the connection.

		case pgwire.BackendNoticeResponse:
			// Out of band, as everywhere else.

		case pgwire.BackendNotificationResponse:
			// Also out of band: possible after a LISTEN earlier on this
			// connection, and the docs warn it may land just before
			// ReadyForQuery. Discarded, there being no API to hand it to.

		case pgwire.BackendParameterStatus:
			// This is where a SET's announcement lands, and dropping it is
			// what makes Conn.Parameter report the opening state forever.
			if err := c.recordParameter(m.Body); err != nil {
				return c.abortPipeline(err)
			}

		default:
			return c.breakConn(fmt.Errorf("%w: unexpected message type %q in a pipeline", ErrProtocol, m.Type))
		}
	}
}

// checkPipelinePart refuses a result cycle beyond the parts that were sent.
//
// The reader counts CommandCompletes to know which part it is on, which makes
// the count an input the *server* controls — and it was trusted unbounded. A
// hostile or broken peer (a compromised pooler, a MITM on an unencrypted
// link) appending one extra RowDescription/DataRow/CommandComplete cycle
// drove fn(len(parts), …), which the documented usage `serve(parts[i], b)`
// turns into an index-out-of-range panic; worse, an early split
// CommandComplete shifted every later result one part forward, delivering
// rows under the wrong principal's index on the RLS path this type exists
// for. Once the count has run ahead of the parts, the attribution of
// everything already delivered is suspect too, so this breaks the connection
// rather than recovering it: there is no position worth going back to.
func (c *Conn) checkPipelinePart(part, parts int) error {
	if part < parts {
		return nil
	}
	return c.breakConn(fmt.Errorf("%w: the server answered more result cycles than the pipeline sent parts (%d)", ErrProtocol, parts))
}

// abortPipeline gives up on a pipeline whose position is still known — the
// message that could not be read was framed whole — and puts the connection
// back at the ReadyForQuery the Sync already asked for.
//
// The distinction it draws is the one the whole reader turns on. A framing
// failure means the next byte could be anything, and there is no recovering
// from that; a payload this package cannot make sense of means one statement
// produced an answer it cannot use, with the conversation intact underneath.
// Behind a gateway the difference is one caller getting an error against every
// caller waiting on a reconnection.
//
// If the drain itself fails, the connection really is gone and its error is
// the one worth reporting.
func (c *Conn) abortPipeline(cause error) error {
	// drainToReady also reports the first server error it passed on the way,
	// which here belongs to a later part — secondary to the reason this
	// pipeline is being abandoned, and dropped rather than substituted for it.
	drained := c.drainToReady()
	if c.broken != nil {
		return drained
	}
	return cause
}
