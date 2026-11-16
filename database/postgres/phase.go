package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/phase"
)

// PhaseStore keeps long jobs' records ([phase.Job]) in a table of the
// application's own schema, beside the working tables their phases fill,
// and their locks as the server's advisory locks (ADR 0027).
//
// Lock opens a connection of the job's own for the whole run and takes a
// session-level advisory lock on it, which PostgreSQL holds "until
// explicitly released or the session ends": a process that dies releases
// it with its connection, and the next run takes it at once. The record is
// written on that same connection, so a connection lost — and the lock with
// it — fails the next write with [phase.ErrLeaseLost]: no process writes a
// record without holding the lock. The table is created on first use.
//
// The lock's key is a 64-bit hash of the table and the job's name: jobs of
// two tables never exclude each other, and a collision between two names
// would make them exclude each other, never share a lock silently.
type PhaseStore struct {
	// Dial and Config open the job's connection, as for [Connect].
	Dial   DialFunc
	Config Config

	// Table is where the records live, "schema.table" or "table" (the
	// search path's), each part letters, digits and '_'.
	Table string
}

var phaseIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// table is the store's table, each part quoted.
func (s *PhaseStore) table() (string, error) {
	parts := strings.Split(s.Table, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("postgres: PhaseStore.Table %q: want table or schema.table", s.Table)
	}
	for i, p := range parts {
		if !phaseIdent.MatchString(p) {
			return "", fmt.Errorf("postgres: PhaseStore.Table %q: %q is not letters, digits and '_'", s.Table, p)
		}
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, "."), nil
}

// Lock opens the job's connection, takes its advisory lock, or fails at
// once with an error wrapping [phase.ErrBusy] when another session holds
// it.
func (s *PhaseStore) Lock(ctx context.Context, job string) (phase.Lease, error) {
	table, err := s.table()
	if err != nil {
		return nil, err
	}
	conn, err := Connect(ctx, s.Dial, s.Config)
	if err != nil {
		return nil, err
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s.Table + "/" + job))
	key := int64(h.Sum64()) //nolint:gosec // G115: a hash's bits, reinterpreted as the bigint the lock takes
	got, _, err := queryOne(ctx, conn, `SELECT pg_try_advisory_lock($1)::text`,
		[]Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, key)}})
	if err == nil && got != "true" {
		err = fmt.Errorf("%w: job %q of %s is locked by another session (advisory lock %d)", phase.ErrBusy, job, s.Table, key)
	}
	if err == nil {
		err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+table+
			` (job text PRIMARY KEY, record jsonb NOT NULL, updated timestamptz NOT NULL DEFAULT now())`)
	}
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return &pgLease{conn: conn, table: table, job: job, key: key}, nil
}

// pgLease is a PhaseStore's lock, held on its connection.
type pgLease struct {
	conn  *Conn
	table string
	job   string
	key   int64
}

func (l *pgLease) jobParam() Param {
	return Param{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, l.job)}
}

// lost wraps an error of the lease's connection in ErrLeaseLost when the
// connection broke: its session, and the lock with it, are gone.
func (l *pgLease) lost(err error) error {
	if err != nil && l.conn.Err() != nil {
		return fmt.Errorf("%w: %w", phase.ErrLeaseLost, err)
	}
	return err
}

func (l *pgLease) Load(ctx context.Context) (phase.Record, bool, error) {
	text, found, err := queryOne(ctx, l.conn, `SELECT record::text FROM `+l.table+` WHERE job = $1`, []Param{l.jobParam()})
	if err != nil || !found {
		return phase.Record{}, false, l.lost(err)
	}
	var r phase.Record
	if err := json.Unmarshal([]byte(text), &r); err != nil {
		return phase.Record{}, false, fmt.Errorf("postgres: the record of job %q in %s: %w", l.job, l.table, err)
	}
	return r, true, nil
}

func (l *pgLease) Save(ctx context.Context, rec phase.Record) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = l.conn.ExecParams(ctx, `INSERT INTO `+l.table+` (job, record, updated) VALUES ($1, $2::jsonb, now())
		ON CONFLICT (job) DO UPDATE SET record = EXCLUDED.record, updated = now()`,
		[]Param{l.jobParam(), {OID: pgtype.OIDText, Value: pgtype.AppendText(nil, string(b))}})
	return l.lost(err)
}

// Release gives the lock up and closes the connection; closing alone
// would release it, the unlock says so to a server that keeps the
// connection open behind a proxy.
func (l *pgLease) Release(ctx context.Context) error {
	if l.conn.Err() != nil {
		return l.conn.Close()
	}
	_, _, err := queryOne(ctx, l.conn, `SELECT pg_advisory_unlock($1)::text`,
		[]Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, l.key)}})
	return errors.Join(err, l.conn.Close())
}

// queryOne reads the first column of the first row as text, false when the
// query returned no row.
func queryOne(ctx context.Context, conn *Conn, sql string, params []Param) (value string, found bool, err error) {
	src := conn.Query(ctx, sql, params, QueryConfig{})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		if len(b.Items) > 0 && !found {
			v, _ := b.Items[0].Value(0)
			value, found = string(v), true // copied: the row is valid for this call only
		}
		return true
	})
	return value, found, src.Err()
}
