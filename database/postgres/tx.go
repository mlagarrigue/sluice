package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mlagarrigue/sluice"
)

// ErrTxDone reports an operation on a transaction that has already committed
// or rolled back.
//
// It is returned rather than ignored because the alternative is a wrong
// answer: a [sluice.Source] built inside a transaction and consumed after it
// ended would run its query outside the transaction, under whatever settings
// the session happens to carry at that moment. With row-level security in the
// picture, "whatever settings the session happens to carry" is another
// tenant's data or none at all — reported, either way, as an ordinary result.
var ErrTxDone = errors.New("postgres: the transaction is already finished")

// ErrTxOpen reports an attempt to begin a transaction on a connection that
// already has one.
//
// PostgreSQL answers a nested BEGIN with a warning and ignores it, so the
// COMMIT that follows closes the *outer* transaction and the code that wrote
// it believes it closed its own. This package refuses instead: a connection
// has one transaction because it has one conversation.
var ErrTxOpen = errors.New("postgres: a transaction is already open on this connection")

// ErrTxCommitRollback reports a [Tx.Commit] on a transaction that was in an
// aborted state: the server rolled back instead of committing, and nothing in
// the block was persisted.
//
// PostgreSQL answers COMMIT inside a failed block with CommandComplete
// "ROLLBACK" and *no* ErrorResponse, so a client that only watches for errors
// reports success for writes that never happened. This is the case after a
// statement in the block failed — including a pipeline that stopped at part
// k, whose earlier parts ran but are gone with the rest. The transaction is
// finished either way; there is nothing to roll back.
var ErrTxCommitRollback = errors.New("postgres: the transaction was in an aborted state, the server rolled back instead of committing; nothing in the block was persisted")

// IsolationLevel names the isolation a transaction runs under. The zero value
// is the server's default, which is what almost every transaction wants.
type IsolationLevel uint8

// The levels PostgreSQL implements. Repeatable read and serializable can fail
// at commit with a serialization error (SQLSTATE 40001), which is a retry, not
// a bug — the caller has to be willing to run the transaction again.
const (
	IsolationDefault IsolationLevel = iota
	IsolationReadCommitted
	IsolationRepeatableRead
	IsolationSerializable
)

// sql returns the fragment for a BEGIN. These are constants chosen by a closed
// enumeration, never text from a caller: nothing here reaches the wire that a
// value could have influenced.
func (l IsolationLevel) sql() (string, error) {
	switch l {
	case IsolationDefault:
		return "", nil
	case IsolationReadCommitted:
		return " ISOLATION LEVEL READ COMMITTED", nil
	case IsolationRepeatableRead:
		return " ISOLATION LEVEL REPEATABLE READ", nil
	case IsolationSerializable:
		return " ISOLATION LEVEL SERIALIZABLE", nil
	default:
		return "", fmt.Errorf("postgres: unknown IsolationLevel %d", uint8(l))
	}
}

// TxConfig states how a transaction opens. The zero value is a plain read-write
// transaction at the server's default isolation, which is the common case and
// therefore costs nothing to ask for.
type TxConfig struct {
	// Isolation is the level to open at. Zero means the server's default.
	Isolation IsolationLevel

	// ReadOnly opens the transaction READ ONLY. It is worth setting even when
	// the statements are obviously reads: it is the server refusing a write
	// rather than the code promising not to attempt one, and the two differ
	// exactly when a bug makes them differ.
	ReadOnly bool

	// Timeout, when positive, sets statement_timeout for the transaction's
	// lifetime. This is the only bound that stops the *server* working: a
	// context stops this client waiting, and CancelRequest is advisory. In a
	// batched pipeline one unbounded statement stalls every caller in the
	// batch, so a transaction with no timeout is a decision, not a default.
	Timeout time.Duration
}

// Tx is one transaction on one connection.
//
// It exists for two things this package cannot do without: a business method
// that writes two tables and needs both or neither, and session state scoped
// to a piece of work rather than to the connection — which is what makes
// row-level security usable from a pooled or reused connection at all. See
// [Tx.SetLocal].
//
//	tx, err := conn.Begin(ctx, postgres.TxOptions{Timeout: 5 * time.Second})
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback() // no-op once Commit has run
//
//	if err := tx.SetLocal(ctx, "app.tenant_id", tenant); err != nil {
//	    return err
//	}
//	src := tx.Query(ctx, "SELECT id, total FROM orders", nil, postgres.QueryConfig{})
//	// ... consume src, *before* Commit ...
//	return tx.Commit(ctx)
//
// A Tx is not safe for concurrent use, for the same reason a [Conn] is not:
// it is a position in one conversation.
type Tx struct {
	conn *Conn
	// gen is the connection's transaction counter at the moment this Tx
	// opened. A Source built here compares it on consumption, which is how a
	// stream outliving its transaction becomes an error instead of a silently
	// wrong result.
	gen  uint64
	done bool
}

// Begin opens a transaction.
//
// The deferred Rollback in the example above is the shape to copy: it is a
// no-op after a successful Commit, and it is the only thing that closes the
// transaction on the paths nobody writes a test for — an early return, a
// validation failure, a panic caught upstream. A transaction left open holds
// the vacuum horizon back for as long as the connection lives.
func (c *Conn) Begin(ctx context.Context, opts TxConfig) (*Tx, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if c.inTx {
		return nil, ErrTxOpen
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("abandoning the transaction before it was opened: %w", err)
	}

	level, err := opts.Isolation.sql()
	if err != nil {
		return nil, err
	}
	begin := "BEGIN" + level
	if opts.ReadOnly {
		begin += " READ ONLY"
	}
	if err := c.execBusy(begin); err != nil {
		return nil, fmt.Errorf("opening the transaction: %w", err)
	}
	c.inTx = true
	tx := &Tx{conn: c, gen: c.txGen}

	if opts.Timeout > 0 {
		if err := tx.SetLocalTimeout(ctx, opts.Timeout); err != nil {
			// The transaction opened but is not in the state that was asked
			// for. Running it anyway would mean running it unbounded, which is
			// the opposite of what the caller requested.
			_ = tx.Rollback()
			return nil, err
		}
	}
	return tx, nil
}

// Exec runs a statement that returns no rows, outside any transaction.
//
// It is for DDL and for control statements — the things that have no rows and
// no parameters. It takes no values because it cannot: this is the simple
// Query message, which has nowhere to put a bound parameter, so anything
// carrying a value goes through [Conn.Query] instead, where it travels in
// binary and cannot be read as SQL.
//
// A statement that fails leaves the connection usable: the server reports the
// error and returns to ready, which is the difference between a statement that
// failed and a connection that cannot be used.
//
// A COMMIT (or END, or PREPARE TRANSACTION) sent here inside an aborted block
// returns [ErrTxCommitRollback], exactly as [Tx.Commit] does: the server
// answers it with CommandComplete "ROLLBACK" and no error, and nil would
// report writes that never happened.
func (c *Conn) Exec(ctx context.Context, sql string) error {
	if c.broken != nil {
		return c.broken
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("abandoning the statement: %w", err)
	}
	return c.execUser(sql)
}

// execUser runs a caller's simple statement behind the busy guard and turns a
// COMMIT the server answered with a rollback into [ErrTxCommitRollback].
//
// The rule needs three facts, and only the third comes from the SQL text:
//
//   - the block was aborted when the statement left (ReadyForQuery 'E'), or
//   - the server completed the first statement as "ROLLBACK", and
//   - that first statement asked to commit.
//
// The wire cannot supply the third: a ROLLBACK in an aborted block completes
// with the same tag and the same status as a refused COMMIT, so the only thing
// that tells them apart is what the caller asked for. Only the leading keyword
// is read, and only once one of the first two facts already holds, so the
// ordinary path pays nothing. The first CommandComplete is the one consulted
// because in an aborted block the server refuses every statement except a
// transaction-control one: if the first statement completed at all, it is the
// one that ended the block.
func (c *Conn) execUser(sql string) error {
	if err := checkSQLText(sql); err != nil {
		return err
	}
	if err := c.beginBusy(); err != nil {
		return err
	}
	defer c.endBusy()
	failed := c.txFailed
	rolledBack, err := c.execRolledBack(sql)
	if (failed || rolledBack) && c.broken == nil && asksToCommit(sql) {
		if err != nil {
			return errors.Join(ErrTxCommitRollback, err)
		}
		return ErrTxCommitRollback
	}
	return err
}

// asksToCommit reports whether the first statement in sql is COMMIT, END or
// PREPARE TRANSACTION — the three statements PostgreSQL answers with
// "ROLLBACK" in an aborted block instead of doing what they say. Leading
// whitespace and comments are skipped; the keywords are case-insensitive.
func asksToCommit(sql string) bool {
	word, rest := leadingKeyword(sql)
	switch {
	case strings.EqualFold(word, "commit"), strings.EqualFold(word, "end"):
		return true
	case strings.EqualFold(word, "prepare"):
		next, _ := leadingKeyword(rest)
		return strings.EqualFold(next, "transaction")
	}
	return false
}

// leadingKeyword returns the first bare word of sql after whitespace and
// comments, and what follows it.
func leadingKeyword(sql string) (word, rest string) {
	i := skipSpaceAndComments(sql)
	j := i
	for j < len(sql) && (sql[j] >= 'a' && sql[j] <= 'z' || sql[j] >= 'A' && sql[j] <= 'Z' || sql[j] == '_') {
		j++
	}
	return sql[i:j], sql[j:]
}

// skipSpaceAndComments returns the offset of the first byte of sql that is
// neither whitespace nor inside a -- or /* */ comment (nested, as PostgreSQL
// nests them).
func skipSpaceAndComments(sql string) int {
	i := 0
	for i < len(sql) {
		switch {
		case sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r' || sql[i] == '\f':
			i++
		case strings.HasPrefix(sql[i:], "--"):
			nl := strings.IndexByte(sql[i:], '\n')
			if nl < 0 {
				return len(sql)
			}
			i += nl + 1
		case strings.HasPrefix(sql[i:], "/*"):
			depth := 0
			for i < len(sql) {
				//nolint:gocritic // ifElseChain: a switch would capture the break that leaves the loop
				if strings.HasPrefix(sql[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(sql[i:], "*/") {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
		default:
			return i
		}
	}
	return i
}

// Commit ends the transaction, keeping its effects. A context that is
// already done abandons the commit without sending it, as every statement
// does; the transaction then stays open until [Tx.Rollback].
func (tx *Tx) Commit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("abandoning the commit: %w", err)
	}
	return tx.end("COMMIT")
}

// Rollback ends the transaction, discarding its effects. It is a no-op on a
// transaction that has already finished, so `defer tx.Rollback()` is safe
// beside a Commit on the success path — which is the point.
//
// It takes no context, deliberately: it runs from a defer, after the context
// that drove the transaction may already be done, and a block left open on a
// connection that will be reused is the worse outcome.
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}
	return tx.end("ROLLBACK")
}

func (tx *Tx) end(verb string) error {
	if tx.done {
		return ErrTxDone
	}
	// A broken connection has no position to send a COMMIT or ROLLBACK from;
	// writing one anyway is how a reply meant for something else gets read
	// as this statement's. The server rolls back when the socket closes.
	if err := tx.conn.broken; err != nil {
		return err
	}
	// The busy guard is claimed before the transaction is marked finished: a
	// Commit issued from inside a consumer's callback is refused while the Tx
	// and the connection's view of it stay intact, so the real end can still
	// run once the outer query is done.
	if err := tx.conn.beginBusy(); err != nil {
		return err
	}
	defer tx.conn.endBusy()
	// Marked finished before the statement runs, not after. A COMMIT that
	// fails has still ended the transaction — the server rolled it back — and
	// a Tx that stayed usable after that would let a caller keep writing into
	// something that no longer exists.
	tx.done = true
	tx.conn.inTx = false
	tx.conn.txGen++
	// Read before the statement runs: the ReadyForQuery that answers it
	// resets the flag to the clean state the connection is now in.
	failed := tx.conn.txFailed
	rolledBack, err := tx.conn.execRolledBack(verb)
	if err != nil {
		return err
	}
	// Belt and braces: the status byte said the block was aborted, or the
	// server's own tag says what it did. Either one is a COMMIT that
	// committed nothing, reported without an ErrorResponse.
	if verb == "COMMIT" && (failed || rolledBack) {
		return ErrTxCommitRollback
	}
	return nil
}

// Exec runs a statement that returns no rows.
//
// The statement is the caller's text with no parameters, so it is for DDL and
// for control statements — not for anything carrying a value. Values go
// through [Tx.Query]'s binary parameters, where they cannot be read as SQL.
// A COMMIT sent this way in an aborted block reports [ErrTxCommitRollback], as
// [Conn.Exec] does.
func (tx *Tx) Exec(ctx context.Context, sql string) error {
	if err := tx.check(ctx); err != nil {
		return err
	}
	err := tx.conn.execUser(sql)
	// A COMMIT, ROLLBACK or END sent as SQL ends the block on the server.
	// ReadyForQuery said so; a Tx left open past it would run the caller's
	// next statements in autocommit, outside any transaction, with no error.
	if !tx.conn.inTx && tx.conn.broken == nil {
		tx.done = true
		tx.conn.txGen++
	}
	return err
}

// Query runs a query inside the transaction. It is [Conn.Query], with one
// difference that matters.
//
// # Consume the stream before ending the transaction
//
// A [sluice.Source] is lazy: nothing is sent until the stream is consumed. A
// source built here and consumed after Commit or Rollback would therefore run
// its query *outside* the transaction — under the session's settings rather
// than the transaction's, which with [Tx.SetLocal] in the picture means under
// nobody's tenant. That is a wrong answer with no error, so this returns
// [ErrTxDone] instead.
func (tx *Tx) Query(ctx context.Context, sql string, params [][]byte, cfg QueryConfig) sluice.Source[Row] {
	if tx.done {
		return errSource(ErrTxDone)
	}
	gen := tx.gen
	c := tx.conn
	inner := c.Query(ctx, sql, params, cfg)

	var err error
	stream := sluice.Stream[Row](func(yield func(sluice.Batch[Row]) bool) {
		// Checked at consumption, not at construction: that is the whole
		// point — the gap between the two is where the transaction ends.
		if tx.done || c.txGen != gen {
			err = ErrTxDone
			return
		}
		inner.Stream()(yield)
		err = inner.Err()
	})
	return sluice.NewSource(stream, func() error { return err })
}

// SetLocal sets a run-time parameter for the rest of the transaction, and
// reverts it when the transaction ends.
//
//	tx.SetLocal(ctx, "app.tenant_id", tenantFromTheBearerToken)
//
// This is what makes row-level security work from a connection that serves
// more than one principal: the policy reads `current_setting('app.tenant_id')`,
// the server enforces it, and the boundary stays on the server side of the
// wire. An application-side `WHERE tenant_id = ...` is a filter someone can
// forget; this is not.
//
// # Why this is not "SET LOCAL " + name + " = " + value
//
// Because that is an injection, and the injected value is an identity. SET
// does not take bound parameters — the grammar has nowhere to put one — so the
// obvious implementation concatenates a caller's value into SQL text, and a
// tenant identifier arriving from a bearer token becomes SQL the moment
// somebody crafts one.
//
// So this uses `set_config($1, $2, true)`, which is the same setting through a
// function call, where both arguments are ordinary bound parameters travelling
// in binary. No value reaches the wire as SQL text (§8.8). The *name* is still
// a value here rather than an identifier, which is why it can be a parameter
// at all — and it is validated anyway, because a name assembled from user
// input is a caller reaching a setting they were not meant to reach.
func (tx *Tx) SetLocal(ctx context.Context, name, value string) error {
	if err := tx.check(ctx); err != nil {
		return err
	}
	if err := validSettingName(name); err != nil {
		return err
	}
	return tx.conn.execParams(ctx, "SELECT set_config($1, $2, true)",
		[][]byte{AppendText(nil, name), AppendText(nil, value)},
		[]uint32{OIDText, OIDText})
}

// SetLocalTimeout bounds how long any single statement in this transaction may
// run on the server.
//
// This is the bound a context cannot be: ctx stops this client pulling, and it
// cannot reach a backend that is inside a statement. In a batched pipeline
// that difference is the difference between one slow query and sixty-four
// stalled callers.
//
// A duration below a millisecond is rounded up to one: statement_timeout is
// expressed in milliseconds, and zero means *no limit* — silently turning
// "500 µs" into "unbounded" is the sort of rounding that is discovered during
// an incident.
func (tx *Tx) SetLocalTimeout(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("postgres: SetLocalTimeout needs a positive duration; %v would mean no limit at all", d)
	}
	// Divided first and rounded up after: adding a millisecond-minus-one
	// before dividing overflows for a d near the top of its range, and the
	// wrapped result is negative.
	ms := d / time.Millisecond
	if d%time.Millisecond != 0 {
		ms++
	}
	return tx.SetLocal(ctx, "statement_timeout", strconv.FormatInt(int64(ms), 10))
}

// Savepoint marks a point this transaction can be rolled back to without
// losing everything before it.
//
// # What they cost
//
// Each savepoint is a subtransaction, and each one costs a round trip — that
// is the measured price, and [Tx.Savepoints] is how a batch avoids paying it
// per element. PostgreSQL also tracks a transaction's open subtransactions in
// a per-backend cache of 64; past it, other sessions' visibility checks
// consult pg_subtrans instead. The cliff that overflow is reputed to cause did
// not appear on loopback up to 256 (see [Tx.Savepoints]), but it depends on
// concurrent load this package cannot see, so release savepoints as you go
// rather than accumulating them.
//
// The name is an SQL identifier, not a value, so it cannot be a bound
// parameter and is validated instead. Do not build it from anything a request
// carried.
func (tx *Tx) Savepoint(ctx context.Context, name string) error {
	if err := tx.checkName(ctx, name); err != nil {
		return err
	}
	return tx.conn.execBusy("SAVEPOINT " + name)
}

// Savepoints establishes several savepoints in one round trip.
//
// A savepoint costs a round trip like anything else, and that is what makes
// one-per-element unaffordable rather than anything about subtransactions:
// measured on loopback, sixty-four savepoints issued one at a time cost
// **21 ms**, at a flat ~330 µs each with no cliff anywhere — the
// subtransaction-cache overflow the design was warned about did not appear,
// and a concurrent reader scanning the same table was unaffected from one
// subtransaction to two hundred and fifty-six.
//
// So the fix is the one [Tx.Pipeline] already uses: send them all before a
// single Sync.
//
//	tx.Savepoints(ctx, names...)   // one round trip, not len(names)
//
// The names are identifiers and are validated as such; do not build one from
// anything a request carried. As on [Tx.Savepoint], release what is no longer
// needed — the count still bounds what the backend tracks even when
// establishing them is cheap.
func (tx *Tx) Savepoints(ctx context.Context, names ...string) error {
	if err := tx.check(ctx); err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}
	for _, name := range names {
		if err := validIdentifier(name); err != nil {
			return err
		}
	}
	c := tx.conn
	// Same guard as every other wire-touching entry point: pipelined or not,
	// these statements are a conversation of their own and may not interleave
	// with a result someone is still consuming.
	if err := c.beginBusy(); err != nil {
		return err
	}
	defer c.endBusy()
	for _, name := range names {
		c.w.Query("SAVEPOINT " + name)
	}
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("establishing %d savepoints: %w", len(names), err))
	}
	// A simple Query message is its own transaction boundary, so each one
	// answers with its own ReadyForQuery. They were all sent before any was
	// read, which is where the round trips went.
	var first error
	for range names {
		if err := c.drainToReady(); err != nil && first == nil {
			first = err
		}
		// A broken connection has no further ReadyForQuery to read: the next
		// read would block on, or misframe, whatever the socket holds.
		if c.broken != nil {
			return c.broken
		}
	}
	return first
}

// RollbackTo undoes everything since the named savepoint, leaving the
// transaction open and the savepoint still established.
func (tx *Tx) RollbackTo(ctx context.Context, name string) error {
	if err := tx.checkName(ctx, name); err != nil {
		return err
	}
	return tx.conn.execBusy("ROLLBACK TO SAVEPOINT " + name)
}

// Release forgets a savepoint, keeping its effects. Releasing one that is no
// longer needed is what keeps the subtransaction count away from the cliff
// described on [Tx.Savepoint].
func (tx *Tx) Release(ctx context.Context, name string) error {
	if err := tx.checkName(ctx, name); err != nil {
		return err
	}
	return tx.conn.execBusy("RELEASE SAVEPOINT " + name)
}

func (tx *Tx) check(ctx context.Context) error {
	if tx.done || tx.gen != tx.conn.txGen {
		return ErrTxDone
	}
	if err := tx.conn.broken; err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("abandoning the statement: %w", err)
	}
	return nil
}

func (tx *Tx) checkName(ctx context.Context, name string) error {
	if err := tx.check(ctx); err != nil {
		return err
	}
	return validIdentifier(name)
}

// validIdentifier accepts the unquoted identifiers this package is willing to
// concatenate into SQL, and nothing else.
//
// The rule is deliberately narrower than PostgreSQL's: lower-case ASCII
// letters, digits and underscore, not starting with a digit, at most 63 bytes
// — which is where the server truncates anyway. Anything outside that is
// refused rather than quoted, because quoting is where escaping bugs live and
// a savepoint name is never legitimately exotic.
func validIdentifier(name string) error {
	if name == "" {
		return errors.New("postgres: the identifier is empty")
	}
	if len(name) > 63 {
		return fmt.Errorf("postgres: the identifier %q is %d bytes; PostgreSQL truncates at 63, so two of them would silently become one", name, len(name))
	}
	for i := range len(name) {
		ch := name[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch == '_':
		case ch >= '0' && ch <= '9' && i > 0:
		default:
			return fmt.Errorf("postgres: %q is not an identifier this package will put into SQL — use lower-case letters, digits and underscore, and never build one from a request", name)
		}
	}
	return nil
}

// validSettingName accepts a run-time parameter name, which is an identifier
// or two joined by a dot — the `app.tenant_id` custom-setting form.
func validSettingName(name string) error {
	if before, after, ok := strings.Cut(name, "."); ok {
		if err := validIdentifier(before); err != nil {
			return err
		}
		return validIdentifier(after)
	}
	return validIdentifier(name)
}

// execParams runs a statement with bound parameters and discards its rows.
// It is the extended-query path, which is the only one that takes parameters
// at all — the simple Query message has nowhere to put them.
func (c *Conn) execParams(ctx context.Context, sql string, params [][]byte, oids []uint32) error {
	// AllRows, not a row limit. These statements return at most one row that
	// nobody reads, and a row limit here walks into the trap
	// [QueryConfig.BatchRows] documents: asking for exactly what the result
	// holds costs an extra Execute to learn there is no more, and the Flush
	// path adds the Sync as a round trip of its own — three trips where
	// AllRows takes one. On the per-transaction RLS path that tax lands on
	// every SetLocal, which is every principal.
	src := c.Query(ctx, sql, params, QueryConfig{AllRows: true, ParamOIDs: oids})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if err := src.Err(); err != nil {
		return fmt.Errorf("running %q: %w", sql, err)
	}
	return nil
}

// errSource is a Source that produces nothing and reports err, for the paths
// that fail before there is anything to stream.
func errSource(err error) sluice.Source[Row] {
	return sluice.NewSource(
		sluice.Stream[Row](func(func(sluice.Batch[Row]) bool) {}),
		func() error { return err },
	)
}
