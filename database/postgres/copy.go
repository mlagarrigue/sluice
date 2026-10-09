package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// copySignature opens a binary COPY stream: the eleven bytes PostgreSQL uses
// to recognise the format and to reject a file that went through a
// line-ending conversion — the \r\n and the high bit are there to be
// corrupted by exactly that.
var copySignature = []byte("PGCOPY\n\xff\r\n\x00")

// ErrCopyRefused reports a server that answered a COPY with something other
// than CopyInResponse — almost always an error naming a missing table or a
// column mismatch, which is then wrapped here.
var ErrCopyRefused = errors.New("postgres: the server did not enter copy mode")

// CopyConfig states how a copy is cut into transactions.
//
// There is no default for [CopyConfig.RowsPerTx], and that is the point of the
// type. The obvious first implementation of a bulk load — one transaction
// around the whole thing — is not a slow option, it is a defect that only
// shows at scale, and it is the one everybody writes first.
type CopyConfig struct {
	// RowsPerTx is how many rows one transaction carries at least before it
	// is committed. It is required and positive.
	//
	// It is a floor, not a cap: the copier commits at the end of the
	// [Copier.WriteTuples] call that reaches it, and it does not split a
	// batch, so a transaction holds up to RowsPerTx plus one batch less a
	// row. Keep batches small next to RowsPerTx when the bound must be tight.
	//
	// Three things go wrong with "all of them", and only the first is
	// obvious. **Nothing is durable until the end**: eight hours of work and
	// one network blip lose all of it. **A long transaction pins the vacuum
	// horizon for the whole database**, not just for this table — dead rows
	// from unrelated workloads cannot be reclaimed while it runs, so a bulk
	// load degrades everything else on the server in a way no amount of
	// in-pipeline correctness addresses. And **restart is all or nothing**.
	//
	// Against that, a small value costs a transaction's overhead per chunk
	// and gives up atomicity across the load: after a failure the table holds
	// a prefix, and the caller needs its own way to resume. That is the trade
	// this parameter exists to make the caller take knowingly. 100 000 rows
	// is a reasonable starting point on a wide table.
	RowsPerTx int

	// MaxMessage is how many bytes accumulate in one CopyData before it is
	// sent. Message boundaries need not fall on row boundaries — the server
	// concatenates them — so this is purely a buffering choice, and a value
	// of zero or less means 64 KiB.
	MaxMessage int
}

// AppendTupleHeader begins one row of a binary COPY stream by writing its
// field count.
//
//	buf = AppendTupleHeader(buf, 2)
//	scratch = AppendInt8(scratch[:0], id)
//	buf = AppendField(buf, scratch)
//	buf = AppendFieldNull(buf)
//
// The row is built into a caller-owned buffer rather than through a stateful
// writer, so a batch of rows is one buffer filled in one pass with no
// allocation per row — and there is no order to remember beyond the one the
// bytes themselves impose.
func AppendTupleHeader(dst []byte, fields int) []byte {
	return binary.BigEndian.AppendUint16(dst, uint16(fields)) //nolint:gosec // G115: a table's column count
}

// AppendField appends one field: its length, then its bytes.
func AppendField(dst, v []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(v))) //nolint:gosec // G115: bounded by the caller's value
	return append(dst, v...)
}

// AppendFieldNull appends a NULL field, which is a length of -1 and no bytes.
func AppendFieldNull(dst []byte) []byte {
	return binary.BigEndian.AppendUint32(dst, ^uint32(0))
}

// BeginField starts a field whose length is not known until it is written, and
// returns the offset [EndField] needs.
//
//	buf, at := BeginField(buf)
//	buf = AppendNullableInt8Array(buf, tags)
//	buf = EndField(buf, at)
//
// [AppendField] takes bytes that already exist, so an array field costs an
// encode into a scratch buffer and then a copy of the whole thing into the
// tuple. This writes it once, in place — the same trick [Writer.Begin] plays
// for a Bind carrying a batch of parameters, and for the same reason: the
// value is the size of a batch, so copying it per row is copying the load.
//
// The reserved length is four bytes of zero until EndField fills it in. A
// tuple sent without the matching EndField declares a zero-length field, which
// the server reads as an empty value rather than as an error — so the two
// belong together at the same nesting, the way an open brace does.
func BeginField(dst []byte) (out []byte, at int) {
	at = len(dst)
	return binary.BigEndian.AppendUint32(dst, 0), at
}

// EndField writes the length of the field [BeginField] opened at.
func EndField(dst []byte, at int) []byte {
	n := len(dst) - at - 4
	binary.BigEndian.PutUint32(dst[at:at+4], uint32(n)) //nolint:gosec // G115: bounded by what the caller wrote
	return dst
}

// Copier writes rows into a table with binary COPY, cutting the load into
// transactions of the size the caller chose.
//
// One transaction cannot span several COPY statements and a COPY cannot be
// committed part-way, so a transaction boundary is a whole cycle: BEGIN, COPY,
// the rows, CopyDone, COMMIT. That is why [CopyConfig.RowsPerTx] drives a loop
// rather than setting a flag — the parameter is not a tuning knob laid over
// one long stream, it changes the shape of the conversation.
//
// # Delivery semantics
//
// At-least-once, and the caller owns the idempotence. A copy interrupted after
// its third commit leaves three transactions' rows in the table and no record
// of where it stopped; resuming means either a key the load can delete past,
// or a target that tolerates the repeat. This package does not persist a
// watermark, because a watermark that is not committed in the same transaction
// as the data is a second source of truth — and one that is would have to know
// the caller's schema.
//
// # Index after, not during
//
// Loading into an indexed table costs roughly twice loading into a bare one
// and rebuilding after. Drop what can be dropped, copy, then rebuild — this
// package cannot do it for the caller, since only the caller knows which
// indexes may go.
type Copier struct {
	conn   *Conn
	ctx    context.Context
	sql    string
	cfg    CopyConfig
	buf    []byte // tuples waiting for a CopyData
	inTx   bool   // a transaction and a COPY are open
	rowsTx int    // rows written in the open transaction
	total  int64
	err    error
}

// CopyFrom starts a binary copy into the target named by sql, which must be a
// complete COPY statement:
//
//	cp, err := conn.CopyFrom(ctx,
//	    "COPY orders (id, total) FROM STDIN WITH (FORMAT BINARY)",
//	    CopyConfig{RowsPerTx: 100_000})
//
// The statement is the caller's rather than assembled from a table name and
// columns, because assembling it would mean quoting identifiers — and §8.8
// says identifiers come from a generated catalog, not from strings this
// package escapes hopefully.
//
// # What ctx stops
//
// ctx is checked once per [Copier.WriteTuples] — the same per-batch
// granularity [Conn.Query] uses, and for the same reason: a batch of tuples is
// the unit that goes on the wire, so there is nothing finer to stop between.
//
// A cancelled copy leaves the open transaction **open**, deliberately. The
// caller decides what a half-loaded transaction is worth: [Copier.Abort] rolls
// it back and tells the server why, [Copier.Close] commits what got through.
// Neither consults ctx — a cleanup path that refuses to run because the
// context is cancelled is how a transaction is left open for the vacuum
// horizon to trip over, which is the failure this type spends its
// documentation avoiding.
func (c *Conn) CopyFrom(ctx context.Context, sql string, cfg CopyConfig) (*Copier, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("abandoning the copy before it was started: %w", err)
	}
	if err := checkSQLText(sql); err != nil {
		return nil, err
	}
	if cfg.RowsPerTx <= 0 {
		return nil, errors.New("postgres: CopyConfig.RowsPerTx must be positive — a transaction granularity nobody chose is one nobody owns")
	}
	if cfg.MaxMessage <= 0 {
		cfg.MaxMessage = 64 << 10
	}
	return &Copier{conn: c, ctx: ctx, sql: sql, cfg: cfg}, nil
}

// Rows reports how many rows have been handed to the copier.
func (cp *Copier) Rows() int64 { return cp.total }

// WriteTuples appends rows already encoded with [AppendTupleHeader],
// [AppendField] and [AppendFieldNull], and reports how many of them there
// are so the copier can honour its transaction size.
//
// The buffer is copied out before returning, so the caller may reuse it for
// the next batch — the batch contract, applied to the one place in this
// package where the caller owns the bytes.
func (cp *Copier) WriteTuples(tuples []byte, rows int) error {
	if cp.err != nil {
		return cp.err
	}
	if rows <= 0 {
		return nil
	}
	// Deliberately not routed through fail: fail clears inTx, which would
	// leave Abort with nothing to roll back and the transaction open on a
	// connection that is otherwise healthy. A cancelled copy is a copy the
	// caller must still finish or abandon, and both paths need inTx intact.
	if err := cp.ctx.Err(); err != nil {
		return fmt.Errorf("abandoning the copy: %w", err)
	}
	// Refused, not fail()ed, like the cancellation above: a WriteTuples
	// issued from inside a consumer's callback is the caller's bug, and its
	// CopyData landing mid-result would corrupt the outer query's stream —
	// but the copy itself is untouched and stays finishable.
	if err := cp.conn.beginBusy(); err != nil {
		return err
	}
	defer cp.conn.endBusy()
	if !cp.inTx {
		if err := cp.beginTx(); err != nil {
			return cp.fail(err)
		}
	}

	cp.total += int64(rows)
	cp.rowsTx += rows

	if len(cp.buf) == 0 && len(tuples) >= cp.cfg.MaxMessage {
		// Nothing is waiting and this batch already exceeds the threshold, so
		// copying it into cp.buf only to copy it straight back out is two
		// passes over the same megabytes. Profiling a bulk COPY put **18% of
		// its time in memmove**, and this is one of the two copies.
		//
		// Safe for the caller's buffer: the write below is synchronous, so by
		// the time WriteTuples returns the bytes are gone and `tuples` is
		// theirs again — which is the contract this method already promises.
		if err := cp.writeData(tuples); err != nil {
			return cp.fail(err)
		}
	} else {
		cp.buf = append(cp.buf, tuples...)
		if len(cp.buf) >= cp.cfg.MaxMessage {
			if err := cp.flushData(); err != nil {
				return cp.fail(err)
			}
		}
	}
	// The transaction is closed on the row count, not on the byte count: the
	// bound the caller chose is about durability and the vacuum horizon, and
	// both are counted in rows. It is checked after the batch rather than
	// within it, which is what makes RowsPerTx a floor: splitting the
	// caller's bytes at a row boundary would mean parsing every tuple.
	if cp.rowsTx >= cp.cfg.RowsPerTx {
		if err := cp.commitTx(); err != nil {
			return cp.fail(err)
		}
	}
	return nil
}

// Close finishes the copy: the open transaction, if any, is committed.
//
// A copier that is not closed leaves a transaction open on the connection,
// which is the vacuum horizon problem in its most avoidable form. Close is
// idempotent.
func (cp *Copier) Close() error {
	if cp.err != nil {
		return cp.err
	}
	if !cp.inTx {
		return nil
	}
	if err := cp.conn.beginBusy(); err != nil {
		return err
	}
	defer cp.conn.endBusy()
	if err := cp.commitTx(); err != nil {
		return cp.fail(err)
	}
	return nil
}

// Abort ends the copy without committing the open transaction, telling the
// server why through CopyFail so the reason appears in its log rather than
// only in the client's.
func (cp *Copier) Abort(reason string) error {
	if !cp.inTx {
		return cp.err
	}
	// Claimed before any state is cleared, so a nested Abort leaves the
	// copier exactly as it found it — still abortable once the outer result
	// is finished.
	if err := cp.conn.beginBusy(); err != nil {
		return err
	}
	defer cp.conn.endBusy()
	cp.inTx = false
	cp.buf = cp.buf[:0]
	c := cp.conn

	// The reason travels as a C string, so a NUL in it would cut the
	// server's log line short; it is spelled out instead.
	c.w.Message(pgwire.FrontendCopyFail, append([]byte(strings.ReplaceAll(reason, "\x00", `\x00`)), 0))
	if err := c.w.Flush(); err != nil {
		return cp.fail(c.breakConn(err))
	}
	// The server answers CopyFail with an ErrorResponse: that is the
	// acknowledgement, not a failure, so it is discarded here where everywhere
	// else it would be surfaced. Only a connection-level failure — which is
	// what breaks the connection — still counts.
	if err := c.drainToReady(); err != nil && c.broken != nil {
		return cp.fail(err)
	}
	c.inTx = false
	if err := c.exec("ROLLBACK"); err != nil {
		return cp.fail(err)
	}
	return nil
}

func (cp *Copier) beginTx() error {
	c := cp.conn
	// A copy opens its own transactions, so it has to agree with [Conn.Begin]
	// about whether one is already open — otherwise the nested BEGIN is
	// ignored by the server and this copy's COMMIT closes the caller's
	// transaction instead of its own.
	if c.inTx {
		return ErrTxOpen
	}
	if err := c.exec("BEGIN"); err != nil {
		return err
	}
	c.inTx = true
	c.w.Query(cp.sql)
	if err := c.w.Flush(); err != nil {
		return c.breakConn(err)
	}
	// The server enters copy mode with CopyInResponse; anything else — almost
	// always an ErrorResponse about a missing table — ends this here.
	for {
		m, err := c.r.Next()
		if err != nil {
			return c.breakConn(fmt.Errorf("starting the copy: %w", eofToUnexpected(err)))
		}
		switch m.Type {
		case pgwire.BackendCopyInResponse:
			cp.inTx, cp.rowsTx = true, 0
			cp.buf = append(cp.buf[:0], copySignature...)
			cp.buf = binary.BigEndian.AppendUint32(cp.buf, 0) // flags
			cp.buf = binary.BigEndian.AppendUint32(cp.buf, 0) // header extension
			return nil
		case pgwire.BackendErrorResponse:
			serverErr, parseErr := ParseError(m.Body)
			if parseErr != nil {
				// Same rule: the copy is refused either way, and an error
				// whose payload could not be read says nothing about the
				// position. Recover it and report what happened.
				drained := c.drainToReady()
				if c.broken != nil {
					return drained
				}
				cp.rollback()
				return fmt.Errorf("%w: %w", ErrCopyRefused, parseErr)
			}
			// A drain error that did not break the connection is a later
			// server message this package could not read — secondary to the
			// refusal, and no reason to leave the transaction open.
			if err := c.drainToReady(); err != nil && c.broken != nil {
				return err
			}
			cp.rollback()
			return fmt.Errorf("%w: %w", ErrCopyRefused, serverErr)
		case pgwire.BackendNoticeResponse:
		case pgwire.BackendParameterStatus:
			// A ParameterStatus is legal at any point in a session, and this
			// window is where a SIGHUP config reload lands when it races the
			// COPY statement: tolerating it is the difference between a
			// reload and a dead bulk load.
			if err := c.recordParameter(m.Body); err != nil {
				return c.breakConn(err)
			}
		case pgwire.BackendNotificationResponse:
			// Possible after a LISTEN earlier on this connection; discarded
			// like everywhere else, there being no API to hand it to.
		default:
			return c.breakConn(fmt.Errorf("%w: %q where CopyInResponse was expected", ErrProtocol, m.Type))
		}
	}
}

func (cp *Copier) commitTx() error {
	c := cp.conn
	// The trailer says "no more tuples": a field count of -1.
	cp.buf = binary.BigEndian.AppendUint16(cp.buf, ^uint16(0))
	if err := cp.flushData(); err != nil {
		return err
	}
	c.w.Message(pgwire.FrontendCopyDone, nil)
	if err := c.w.Flush(); err != nil {
		return c.breakConn(err)
	}
	if err := c.drainToReady(); err != nil {
		if c.broken != nil {
			return err
		}
		// The COPY failed at completion — a constraint, a trigger, the
		// server's last look at the data — on a connection whose position is
		// still known. Returning with the transaction flags still set would
		// wedge the connection: fail clears the copier's flag but not the
		// Conn's, so Abort would find nothing to roll back and every later
		// Begin or CopyFrom would refuse with ErrTxOpen, forever, on a healthy
		// socket. Roll back instead, and let the statement's failure be what
		// the caller sees.
		cp.rollback()
		return err
	}
	cp.inTx, cp.rowsTx = false, 0
	c.inTx = false
	return c.exec("COMMIT")
}

// rollback ends the open transaction after a failure, clearing both the
// copier's and the connection's view of it before telling the server. It is
// the one spelling of the un-wedge sequence: four sites needed it, and the
// copies had already started to disagree about which flags to clear.
//
// ROLLBACK's own error is discarded — every caller is already carrying a more
// specific one, and whether the copier stays usable is the caller's decision
// (the write paths route theirs through fail; beginTx's refusal does not).
func (cp *Copier) rollback() {
	cp.inTx, cp.rowsTx = false, 0
	cp.conn.inTx = false
	_ = cp.conn.exec("ROLLBACK")
}

// flushData sends what is buffered as one CopyData. Message boundaries need
// not fall on row boundaries — the server concatenates them — which is what
// lets a batch of rows leave as one message rather than one per row.
func (cp *Copier) flushData() error {
	if len(cp.buf) == 0 {
		return nil
	}
	if err := cp.writeData(cp.buf); err != nil {
		return err
	}
	cp.buf = cp.buf[:0]
	return nil
}

// writeData sends one CopyData carrying body, which may be the copier's own
// buffer or the caller's.
func (cp *Copier) writeData(body []byte) error {
	c := cp.conn
	c.w.Message(pgwire.FrontendCopyData, body)
	if err := c.w.Flush(); err != nil {
		return c.breakConn(err)
	}
	return nil
}

// fail records why the copy stopped, so a caller that ignores one error cannot
// keep writing into a copy that is already over.
func (cp *Copier) fail(err error) error {
	if cp.err == nil {
		cp.err = err
	}
	cp.inTx = false
	return cp.err
}

// exec runs a statement that returns no rows and waits for the connection to
// be ready again.
func (c *Conn) exec(sql string) error {
	_, err := c.execRolledBack(sql)
	return err
}

// execRolledBack is exec reporting whether the statement completed as
// "ROLLBACK", for the one caller that has to know: a COMMIT in an aborted
// block.
func (c *Conn) execRolledBack(sql string) (rolledBack bool, err error) {
	c.w.Query(sql)
	if err := c.w.Flush(); err != nil {
		return false, c.breakConn(fmt.Errorf("running %q: %w", sql, err))
	}
	return c.drainToReadyRolledBack()
}

// errCopyFromStdin and errCopyToStdout report a COPY statement issued through
// a path that cannot carry it. They are statement errors, not broken
// connections: in both cases the conversation is steered back to ReadyForQuery
// and the connection stays usable.
var (
	errCopyFromStdin = errors.New("postgres: the statement is a COPY ... FROM STDIN, which Exec and Query cannot run — use Conn.CopyFrom")
	errCopyToStdout  = errors.New("postgres: the statement is a COPY ... TO STDOUT, which this package has no API to receive — the statement failed rather than have its data silently discarded")
)

// copyFailReason is what the server logs when a COPY reached a path that
// refuses it; the client-side explanation is errCopyFromStdin.
const copyFailReason = "the client issued COPY FROM STDIN outside CopyFrom and cannot supply data on this path"

// refuseCopyIn answers an unexpected CopyInResponse with CopyFail.
//
// Sending something is not optional: the backend is now waiting for CopyData
// and *ignores Flush and Sync while it waits* (§53.2.6), so a drain that
// merely skips the message never sees another ReadyForQuery and the
// connection hangs until its deadline. CopyFail is the one message that ends
// copy-in from here; the ErrorResponse it draws is the acknowledgement.
func (c *Conn) refuseCopyIn() error {
	c.w.Message(pgwire.FrontendCopyFail, append([]byte(copyFailReason), 0))
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("refusing an unexpected copy-in: %w", err))
	}
	return nil
}

// drainToReady reads to the next ReadyForQuery, returning a server error if
// one appeared on the way.
//
// The error is returned but the connection is not broken by it: the server
// finishes what it was doing and reports ready, so the position stays known —
// which is the difference between a statement that failed and a connection
// that cannot be used.
func (c *Conn) drainToReady() error {
	_, err := c.drainToReadyRolledBack()
	return err
}

// drainToReadyRolledBack is drainToReady reporting whether the first
// CommandComplete on the way was "ROLLBACK" — the first, because in an
// aborted block only a transaction-control statement completes at all, so
// the first completion is the one that ended the block. An error is not the only way a
// statement can fail to do what it says: a COMMIT in an aborted block
// completes as "ROLLBACK" with no ErrorResponse at all, and the tag is the
// only place the server says so. The tag is tested in place rather than
// copied out: the Message borrows the reader's buffer, and a copy on every
// Exec would pay for the one caller that reads it.
func (c *Conn) drainToReadyRolledBack() (rolledBack bool, err error) {
	var (
		serverErr error
		completed bool
	)
	for {
		m, err := c.r.Next()
		if err != nil {
			return rolledBack, c.breakConn(eofToUnexpected(err))
		}
		switch m.Type {
		case pgwire.BackendReadyForQuery:
			c.noteTxStatus(m.Body)
			return rolledBack, serverErr
		case pgwire.BackendCommandComplete:
			if !completed {
				completed = true
				rolledBack = string(m.Body) == "ROLLBACK\x00"
			}
		case pgwire.BackendCopyInResponse:
			// Exec met a COPY ... FROM STDIN. Refused with CopyFail — see
			// refuseCopyIn for why silence here is a deadlock, not a skip.
			if err := c.refuseCopyIn(); err != nil {
				return rolledBack, err
			}
			if serverErr == nil {
				// Claimed before the ErrorResponse the CopyFail draws, so the
				// caller reads the actionable explanation rather than the
				// server echoing the refusal back.
				serverErr = errCopyFromStdin
			}
		case pgwire.BackendCopyOutResponse:
			// COPY ... TO STDOUT through Exec. The data that follows is
			// unwanted and legally discardable (§53.2.6), and the loop's
			// default case does discard it — but the statement must fail
			// rather than "succeed" with its entire output thrown away.
			if serverErr == nil {
				serverErr = errCopyToStdout
			}
		case pgwire.BackendParameterStatus:
			if err := c.recordParameter(m.Body); err != nil {
				// A payload this package cannot read is not a lost
				// connection: the reader framed the message whole, so the
				// position is still known and this drain can carry on to the
				// ReadyForQuery it came for. Giving up here would break a
				// connection *while recovering it*, which is the one place
				// that has to succeed.
				if serverErr == nil {
					serverErr = err
				}
			}
		case pgwire.BackendErrorResponse:
			e, parseErr := ParseError(m.Body)
			if parseErr != nil {
				if serverErr == nil {
					serverErr = parseErr
				}
				continue
			}
			if serverErr == nil {
				serverErr = e // the first error is the cause; later ones follow from it
			}
		}
	}
}

func eofToUnexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
