package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// ReplicationConfig says which changes [Replicate] streams and how.
type ReplicationConfig struct {
	// Slot is the logical replication slot to stream from, created with the
	// pgoutput plugin: SELECT pg_create_logical_replication_slot('name',
	// 'pgoutput'). Required. The slot is the caller's to create and drop: it
	// holds the server's write-ahead log for as long as it exists, and that
	// deserves to be a visible step rather than a side effect.
	Slot string

	// Publications names the publications whose tables are streamed
	// (CREATE PUBLICATION). At least one is required.
	Publications []string

	// Start is the position to stream from. Zero means the slot's
	// confirmed_flush_lsn, which is where the server starts anyway when Start
	// is lower: a stream resumes after the last position acknowledged.
	Start LSN

	// BatchChanges is the most changes one batch holds. Zero means
	// [sluice.DefaultBatchSize]; a negative value is refused. A batch also
	// ends as soon as the next message is not already received, so a
	// change is never held back waiting for the one after it.
	BatchChanges int

	// StatusInterval is how often, at most, the position acknowledged
	// through [Replication.Ack] is reported to the server. Zero means ten
	// seconds, the server's own wal_receiver_status_interval; a negative
	// value is refused. The position is also reported whenever the server
	// asks for a reply, and when the stream stops.
	StatusInterval time.Duration
}

// defaultStatusInterval is [ReplicationConfig.StatusInterval] when zero.
const defaultStatusInterval = 10 * time.Second

// Replication is a logical replication connection streaming one slot's
// changes. [Replicate] opens it, [Replication.Changes] reads it, and
// [Replication.Ack] tells the server what has been processed.
type Replication struct {
	c        *Conn
	batch    int
	interval time.Duration

	// acked is the highest position the consumer acknowledged; sent is the
	// one last reported to the server, and sentAt when.
	acked  atomic.Uint64
	sent   LSN
	sentAt time.Time

	started atomic.Bool
	done    bool // the server left copy mode, or the stream stopped
	err     error

	dec replDecoder

	// The batch's storage, reused: arena holds the message payloads the
	// values borrow, in chunks that never move once written.
	arena   [][]byte
	chunk   int
	changes []Change
	spans   []tupleSpans
	vals    []Value
	scratch []byte
}

// Replicate opens a logical replication connection and starts streaming the
// slot's changes with the pgoutput plugin, values in binary.
//
// The connection is opened as [Connect] opens one — dial, cfg and ctx mean
// the same — with replication=database added to the startup packet, which is
// the only way this package opens one: Params may not carry it. The role
// needs the REPLICATION attribute, and the server wal_level = logical.
//
// Changes arrive at least once. The server holds every change the consumer
// has not acknowledged through [Replication.Ack], and a stream that stops
// and starts again resumes after the last acknowledged position; what came
// after it is delivered again.
func Replicate(ctx context.Context, dial DialFunc, cfg Config, rc ReplicationConfig) (*Replication, error) {
	sql, err := startReplicationSQL(rc)
	if err != nil {
		return nil, err
	}
	batch := rc.BatchChanges
	if batch == 0 {
		batch = sluice.DefaultBatchSize
	}
	interval := rc.StatusInterval
	if interval == 0 {
		interval = defaultStatusInterval
	}
	cfg.replication = true
	c, err := Connect(ctx, dial, cfg)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.rw.Close() })
	err = c.startReplication(sql)
	if !stop() {
		err = fmt.Errorf("postgres: starting replication: %w", context.Cause(ctx))
	}
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	r := &Replication{c: c, batch: batch, interval: interval, sent: rc.Start, sentAt: time.Now()}
	r.acked.Store(uint64(rc.Start))
	return r, nil
}

// startReplicationSQL builds the START_REPLICATION command, refusing a
// configuration it cannot express before anything is dialled.
func startReplicationSQL(rc ReplicationConfig) (string, error) {
	if rc.Slot == "" {
		return "", errors.New("postgres: ReplicationConfig.Slot is empty — the slot is created by the caller, with pg_create_logical_replication_slot")
	}
	if len(rc.Publications) == 0 {
		return "", errors.New("postgres: ReplicationConfig.Publications is empty — pgoutput streams the tables of at least one publication")
	}
	if rc.BatchChanges < 0 {
		return "", fmt.Errorf("postgres: ReplicationConfig.BatchChanges %d is negative — zero means %d", rc.BatchChanges, sluice.DefaultBatchSize)
	}
	if rc.StatusInterval < 0 {
		return "", fmt.Errorf("postgres: ReplicationConfig.StatusInterval %v is negative — zero means %v", rc.StatusInterval, defaultStatusInterval)
	}
	pubs := make([]string, len(rc.Publications))
	for i, p := range rc.Publications {
		if p == "" {
			return "", errors.New("postgres: ReplicationConfig.Publications holds an empty name")
		}
		pubs[i] = quoteIdent(p)
	}
	sql := "START_REPLICATION SLOT " + quoteIdent(rc.Slot) + " LOGICAL " + rc.Start.String() +
		" (proto_version '1', publication_names " + quoteLiteral(strings.Join(pubs, ",")) + ", binary 'true')"
	if err := checkSQLText(sql); err != nil {
		return "", err
	}
	return sql, nil
}

// quoteIdent writes name as a quoted identifier, its case and characters kept.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteLiteral writes s as a standard string constant.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// startReplication sends START_REPLICATION and reads until the server enters
// copy mode or refuses.
func (c *Conn) startReplication(sql string) error {
	c.w.Query(sql)
	if err := c.w.Flush(); err != nil {
		return c.breakConn(err)
	}
	for {
		m, err := c.r.Next()
		if err != nil {
			return c.breakConn(err)
		}
		switch m.Type {
		case pgwire.BackendCopyBothResponse:
			return nil
		case pgwire.BackendErrorResponse:
			e, perr := parseError(m.Body)
			if perr != nil {
				return c.breakConn(perr)
			}
			// The server follows its refusal with ReadyForQuery; the
			// connection is closed by the caller either way.
			return e
		case pgwire.BackendNoticeResponse, pgwire.BackendParameterStatus:
		default:
			return c.breakConn(fmt.Errorf("%w: message %q while starting replication", ErrProtocol, m.Type))
		}
	}
}

// Ack declares every change before lsn processed, so that the server may
// release the write-ahead log it holds for them and a later stream resumes
// there. Hand it a Commit's EndLSN once the transaction is processed. A
// position lower than one already acknowledged is ignored. Ack is safe to call
// from any goroutine; the position reaches the server with the next status
// report.
func (r *Replication) Ack(lsn LSN) {
	for {
		cur := r.acked.Load()
		if uint64(lsn) <= cur || r.acked.CompareAndSwap(cur, uint64(lsn)) {
			return
		}
	}
}

// Changes streams the slot's changes, one batch at a time, until the consumer
// stops or ctx ends. It may be called once.
//
// The stream is pulled: while the consumer works on a batch, nothing is read
// and the server waits. A batch held longer than the server's
// wal_sender_timeout (a minute by default) leaves its keepalives unanswered,
// and the server closes the connection.
//
// When the consumer stops, the acknowledged position is reported, copy mode
// is left and the connection is ready to close. When ctx ends, the connection
// is closed at once and Err reports why; the slot resumes, either way, after
// the last position the server was told.
func (r *Replication) Changes(ctx context.Context) sluice.Source[Change] {
	if !r.started.CompareAndSwap(false, true) {
		return sluice.NewSource[Change](nil, func() error {
			return errors.New("postgres: Replication.Changes called twice — a slot is streamed once per connection")
		})
	}
	stream := func(yield func(sluice.Batch[Change]) bool) {
		stop := context.AfterFunc(ctx, func() { _ = r.c.rw.Close() })
		defer func() {
			if !stop() && r.err == nil {
				r.err = context.Cause(ctx)
			}
		}()
		for !r.done {
			b, err := r.readBatch()
			if err != nil {
				r.err = err
				if ctx.Err() != nil {
					r.err = fmt.Errorf("postgres: replication stopped: %w", context.Cause(ctx))
				}
				return
			}
			if len(b.Items) > 0 && !yield(b) {
				if !r.done {
					r.err = r.finish()
				}
				return
			}
			if r.done {
				// The server left copy mode: nothing more may be sent in it.
				return
			}
			if err := r.report(false); err != nil {
				r.err = err
				return
			}
		}
	}
	return sluice.NewSource(sluice.Stream[Change](stream), func() error { return r.err })
}

// Close closes the connection. Call it once the stream has ended.
func (r *Replication) Close() error { return r.c.Close() }

// readBatch reads until a batch is full, or holds something and the next
// message is not already received.
func (r *Replication) readBatch() (sluice.Batch[Change], error) {
	r.changes, r.spans, r.vals = r.changes[:0], r.spans[:0], r.vals[:0]
	r.chunk = 0
	if len(r.arena) > 0 {
		r.arena[0] = r.arena[0][:0]
	}
	for len(r.changes) < r.batch && (len(r.changes) == 0 || r.c.r.Buffered()) {
		m, err := r.c.r.Next()
		if err != nil {
			return sluice.Batch[Change]{}, r.c.breakConn(err)
		}
		switch m.Type {
		case pgwire.BackendCopyData:
			if err := r.copyData(m.Body); err != nil {
				return sluice.Batch[Change]{}, r.c.breakConn(err)
			}
		case pgwire.BackendCopyDone:
			// The server ended the stream on its own (a timeline switch, a
			// shutdown); what follows is its CommandComplete and
			// ReadyForQuery.
			r.done = true
			r.c.w.Message(pgwire.FrontendCopyDone, nil)
			if err := r.c.w.Flush(); err != nil {
				return sluice.Batch[Change]{}, r.c.breakConn(err)
			}
			if err := r.drain(); err != nil {
				return sluice.Batch[Change]{}, err
			}
			return r.materialize(), nil
		case pgwire.BackendErrorResponse:
			e, perr := parseError(m.Body)
			if perr != nil {
				return sluice.Batch[Change]{}, r.c.breakConn(perr)
			}
			return sluice.Batch[Change]{}, r.c.breakConn(e)
		case pgwire.BackendNoticeResponse, pgwire.BackendParameterStatus:
		default:
			return sluice.Batch[Change]{}, r.c.breakConn(fmt.Errorf("%w: message %q during replication", ErrProtocol, m.Type))
		}
	}
	return r.materialize(), nil
}

// copyData handles one CopyData of the replication stream: WAL data, which
// carries one pgoutput message, or a keepalive.
func (r *Replication) copyData(p []byte) error {
	if len(p) == 0 {
		return fmt.Errorf("%w: empty CopyData during replication", ErrProtocol)
	}
	switch p[0] {
	case 'w':
		if len(p) < 25 {
			return fmt.Errorf("%w: XLogData of %d bytes", ErrProtocol, len(p))
		}
		at := LSN(binary.BigEndian.Uint64(p[1:9]))
		var err error
		r.changes, r.spans, r.vals, err = r.dec.decode(r.keep(p[25:]), at, r.changes, r.spans, r.vals)
		return err
	case 'k':
		if len(p) != 18 {
			return fmt.Errorf("%w: keepalive of %d bytes", ErrProtocol, len(p))
		}
		if p[17] == 1 {
			return r.report(true)
		}
		return nil
	}
	return fmt.Errorf("%w: CopyData %q during replication", ErrProtocol, p[0])
}

// arenaChunk is the size of one arena chunk; a payload larger than it gets a
// chunk of its own.
const arenaChunk = 64 << 10

// keep copies a payload into the batch's arena, where it stays put until the
// next batch: the values decoded from it borrow it.
func (r *Replication) keep(p []byte) []byte {
	for r.chunk < len(r.arena) {
		c := r.arena[r.chunk]
		if cap(c)-len(c) >= len(p) {
			r.arena[r.chunk] = append(c, p...)
			return r.arena[r.chunk][len(c) : len(c)+len(p) : len(c)+len(p)]
		}
		r.chunk++
		if r.chunk < len(r.arena) {
			r.arena[r.chunk] = r.arena[r.chunk][:0]
		}
	}
	r.arena = append(r.arena, make([]byte, 0, max(arenaChunk, len(p))))
	r.arena[r.chunk] = append(r.arena[r.chunk], p...)
	return r.arena[r.chunk][:len(p):len(p)]
}

// materialize points each change at its tuples, now that the value slab no
// longer grows.
func (r *Replication) materialize() sluice.Batch[Change] {
	for i, sp := range r.spans {
		if sp.hasOld {
			r.changes[i].Old = Tuple(r.vals[sp.old[0]:sp.old[1]:sp.old[1]])
		}
		if sp.hasNew {
			r.changes[i].New = Tuple(r.vals[sp.new[0]:sp.new[1]:sp.new[1]])
		}
	}
	return sluice.Batch[Change]{Items: r.changes}
}

// report sends a standby status update when the acknowledged position moved
// and the interval passed, or when force is set.
func (r *Replication) report(force bool) error {
	acked := LSN(r.acked.Load())
	if !force && (acked == r.sent || time.Since(r.sentAt) < r.interval) {
		return nil
	}
	now := time.Now()
	b := append(r.scratch[:0], 'r')
	for range 3 { // written, flushed, applied: all the acknowledged position
		b = binary.BigEndian.AppendUint64(b, uint64(acked))
	}
	b = binary.BigEndian.AppendUint64(b, uint64(now.UnixMicro()-pgEpochMicros)) //nolint:gosec // G115: a clock after 2000
	b = append(b, 0)
	r.scratch = b
	r.c.w.Message(pgwire.FrontendCopyData, b)
	if err := r.c.w.Flush(); err != nil {
		return r.c.breakConn(err)
	}
	r.sent, r.sentAt = acked, now
	return nil
}

// finish reports the acknowledged position, leaves copy mode and reads until
// the connection is ready for the next command.
func (r *Replication) finish() error {
	r.done = true
	if err := r.report(true); err != nil {
		return err
	}
	r.c.w.Message(pgwire.FrontendCopyDone, nil)
	if err := r.c.w.Flush(); err != nil {
		return r.c.breakConn(err)
	}
	return r.drain()
}

// drain reads what the server sends after copy mode ends — WAL it had sent
// before it saw our CopyDone, its own CopyDone, CommandComplete — up to
// ReadyForQuery.
func (r *Replication) drain() error {
	for {
		m, err := r.c.r.Next()
		if err != nil {
			return r.c.breakConn(err)
		}
		switch m.Type {
		case pgwire.BackendReadyForQuery:
			return nil
		case pgwire.BackendErrorResponse:
			e, perr := parseError(m.Body)
			if perr != nil {
				return r.c.breakConn(perr)
			}
			return e
		case pgwire.BackendCopyData, pgwire.BackendCopyDone, pgwire.BackendCommandComplete,
			pgwire.BackendNoticeResponse, pgwire.BackendParameterStatus, pgwire.BackendRowDescription, pgwire.BackendDataRow:
		default:
			return r.c.breakConn(fmt.Errorf("%w: message %q while leaving replication", ErrProtocol, m.Type))
		}
	}
}
