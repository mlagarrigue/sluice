// Package pgfake is just enough of a PostgreSQL backend to run the examples'
// stores without a server: simple-query statements, the extended protocol
// with row limits and portal suspension, set_config, transaction blocks and
// errors with their SQLSTATE — each answered only when the client asks (Sync,
// Flush or a simple Query), as a real backend answers.
//
// What a statement returns is the caller's [Backend.Exec]; the fake is the
// protocol around it. It is test support for the examples and nothing else.
package pgfake

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// Result is what one statement produces. Values are binary-format column
// values; a nil value is NULL.
type Result struct {
	Columns []string
	OIDs    []uint32
	Rows    [][][]byte
	Tag     string // CommandComplete tag; defaults to the server's for the statement's verb
	Err     string // non-empty answers the statement with an ErrorResponse
	Code    string // SQLSTATE of Err; defaults to XX000, internal_error
}

// Backend answers statements. Exec sees the SQL text, the bound binary
// parameters (nil for a simple Query), and the transaction-local settings
// established so far with set_config.
type Backend struct {
	Exec func(sql string, params [][]byte, settings map[string]string) Result

	mu       sync.Mutex
	execs    []string
	executes int
}

// Statements returns the SQL of every statement executed, in order.
func (b *Backend) Statements() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.execs...)
}

// Executes returns how many Execute messages the backend has answered: one
// per batch a client asked for, plus the one that finds a portal suspended
// at exactly its last row.
func (b *Backend) Executes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.executes
}

func (b *Backend) exec(sql string, params [][]byte, settings map[string]string) Result {
	b.mu.Lock()
	b.execs = append(b.execs, sql)
	b.mu.Unlock()
	if b.Exec == nil {
		return Result{}
	}
	return b.Exec(sql, params, settings)
}

// Conn starts the backend on one end of a pipe and returns a client
// connection on the other, closed when the test ends.
func (b *Backend) Conn(tb testing.TB) *postgres.Conn {
	tb.Helper()
	return postgres.NewConn(b.pipe(tb, false), postgres.Config{})
}

// Dial is a [postgres.DialFunc] onto this backend, for the callers that go
// through [postgres.Connect]: the connection then begins with a startup
// packet, which the fake answers the way a trusting server does —
// AuthenticationOk, the ParameterStatus a session starts with, BackendKeyData
// and ReadyForQuery. No credential is checked: this is a fake.
func (b *Backend) Dial(tb testing.TB) postgres.DialFunc {
	tb.Helper()
	return func(context.Context) (io.ReadWriteCloser, error) {
		return b.pipe(tb, true), nil
	}
}

// pipe starts a served connection and returns the client's end.
func (b *Backend) pipe(tb testing.TB, startup bool) net.Conn {
	tb.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.serve(server, startup)
	}()
	tb.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return client
}

type portal struct {
	sql  string
	res  Result
	next int
	ran  bool // completed once
}

// serve speaks the server side of one connection, in this order: start the
// writer goroutine that stands for a socket's buffers; answer the startup when
// asked to; define the transaction rules a statement applies (fail, refused,
// endTx, complete); then read messages, one case per frontend message — Query,
// Parse, Bind, Describe, Execute, Close, Flush, Sync, Terminate — until the
// client leaves.
func (b *Backend) serve(nc net.Conn, startup bool) {
	defer func() { _ = nc.Close() }()
	rd := pgwire.NewReader(nc)
	var (
		out      []byte
		stmts    = map[string]string{}
		settings = map[string]string{}
		local    = map[string]bool{} // settings that end with the transaction
		cur      *portal
		status   byte = 'I'
		skipping bool // an error occurred; discard until Sync
	)
	// A server's socket buffers what it sends and a net.Pipe does not: a
	// client that asked for batches ahead writes its Sync before reading
	// them, and on the bare pipe both ends would wait on the other.
	sent := make(chan []byte, 64)
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		for b := range sent {
			_, _ = nc.Write(b) // after a failure, keep draining: the reader ends serve
		}
	}()
	defer func() {
		close(sent)
		<-wrote
	}()
	write := func() {
		sent <- bytes.Clone(out)
		out = out[:0]
	}
	if startup {
		// The startup packet carries no type byte. Answered as a server
		// that trusts the connection would: authenticated at once, the
		// parameters every session is told, a cancellation key, ready.
		if _, err := rd.NextStartup(); err != nil {
			return
		}
		out = append(out, msg(pgwire.BackendAuthentication, []byte{0, 0, 0, 0})...)
		for _, kv := range [][2]string{{"server_version", "18.0"}, {"client_encoding", "UTF8"}, {"server_encoding", "UTF8"}, {"DateStyle", "ISO, MDY"}, {"integer_datetimes", "on"}} {
			out = append(out, msg(pgwire.BackendParameterStatus, append(append([]byte(kv[0]), 0), append([]byte(kv[1]), 0)...))...)
		}
		out = append(out, msg(pgwire.BackendBackendKeyData, []byte{0, 0, 0, 1, 0, 0, 0, 2})...)
		out = append(out, msg(pgwire.BackendReadyForQuery, []byte{status})...)
		write()
	}
	fail := func(code, m string) {
		out = append(out, errorResponse(code, m)...)
		if status == 'T' {
			status = 'E'
		}
	}
	// In an aborted block a server runs only what leaves it — COMMIT,
	// ROLLBACK, ROLLBACK TO, PREPARE TRANSACTION — and refuses the rest, at
	// Parse and Bind as for a simple Query.
	refused := func(sql string) bool {
		if status != 'E' || endsBlock(verb(sql)) {
			return false
		}
		fail("25P02", "current transaction is aborted, commands ignored until end of transaction block")
		return true
	}
	endTx := func() {
		status = 'I'
		for k := range local {
			delete(settings, k)
		}
		clear(local)
	}
	// complete applies what a statement that ran does to the transaction
	// block and returns the tag the server reports for its n rows.
	complete := func(sql string, res Result, n int) string {
		v, next := verb(sql)
		switch {
		case v == "BEGIN" || v == "START":
			status = 'T'
		case v == "ROLLBACK" && next == "TO":
			if status == 'E' {
				status = 'T' // back to the savepoint, the block usable again
			}
		case endsBlock(v, next):
			aborted := status == 'E'
			endTx()
			if aborted && v != "ROLLBACK" && v != "ABORT" {
				// The server rolls back instead, and says so in the tag
				// alone: no ErrorResponse (EndTransactionBlock in xact.c).
				return "ROLLBACK"
			}
			if v == "PREPARE" {
				return "PREPARE TRANSACTION"
			}
		}
		if res.Tag != "" {
			return res.Tag
		}
		return commandTag(v, n)
	}
	for {
		m, err := rd.Next()
		if err != nil {
			return
		}
		// After an error everything up to Sync is discarded, but a Flush
		// still delivers what is pending — the error the client waits for.
		if skipping && m.Type != pgwire.FrontendSync && m.Type != pgwire.FrontendFlush {
			continue
		}
		switch m.Type {
		case pgwire.FrontendQuery:
			// A simple Query may carry several statements; each answers
			// in turn, and an error ends the run (the rest is skipped), with
			// one ReadyForQuery for the whole string, as the server does.
			text, _, _ := cut(m.Body)
			for _, sql := range statements(text) {
				if refused(sql) {
					break
				}
				res := b.exec(sql, nil, settings)
				if res.Err != "" {
					fail(res.Code, res.Err)
					break
				}
				if res.Columns != nil {
					out = append(out, rowDescription(res.Columns, res.OIDs)...)
					for _, r := range res.Rows {
						out = append(out, dataRow(r)...)
					}
				}
				out = append(out, commandComplete(complete(sql, res, len(res.Rows)))...)
			}
			out = append(out, msg(pgwire.BackendReadyForQuery, []byte{status})...)
			write()
		case pgwire.FrontendParse:
			name, rest, _ := cut(m.Body)
			sql, _, _ := cut(rest)
			if refused(sql) {
				skipping = true
				continue
			}
			stmts[name] = sql
			out = append(out, msg(pgwire.BackendParseComplete, nil)...)
		case pgwire.FrontendBind:
			_, rest, _ := cut(m.Body)
			stmt, rest, _ := cut(rest)
			sql, prepared := stmts[stmt]
			if !prepared {
				// The server's answer to a name it never parsed, SQLSTATE
				// 26000 (invalid_sql_statement_name), not a silent success.
				fail("26000", fmt.Sprintf("prepared statement %q does not exist", stmt))
				skipping = true
				continue
			}
			if refused(sql) {
				skipping = true
				continue
			}
			params := bindParams(rest)
			var res Result
			if strings.Contains(sql, "set_config") && len(params) >= 2 {
				settings[string(params[0])] = string(params[1])
				local[string(params[0])] = true
				res = Result{Columns: []string{"set_config"}, OIDs: []uint32{pgtype.OIDText}, Rows: [][][]byte{{params[1]}}}
			}
			if r := b.exec(sql, params, settings); r.Err != "" || r.Columns != nil || r.Tag != "" {
				res = r
			}
			if res.Err != "" {
				fail(res.Code, res.Err)
				skipping = true
				continue
			}
			cur = &portal{sql: sql, res: res}
			out = append(out, msg(pgwire.BackendBindComplete, nil)...)
		case pgwire.FrontendDescribe:
			if len(m.Body) > 0 && m.Body[0] == 'S' {
				// A statement: ParameterDescription (none typed here) then
				// what a row of it would look like, as the server answers
				// from the plan — not NoData whatever the statement.
				name, _, _ := cut(m.Body[1:])
				sql, prepared := stmts[name]
				if !prepared {
					fail("26000", fmt.Sprintf("prepared statement %q does not exist", name))
					skipping = true
					continue
				}
				out = append(out, msg(pgwire.BackendParameterDescription, []byte{0, 0})...)
				if res := b.exec(sql, nil, settings); res.Columns != nil {
					out = append(out, rowDescription(res.Columns, res.OIDs)...)
				} else {
					out = append(out, msg(pgwire.BackendNoData, nil)...)
				}
				continue
			}
			if cur == nil || cur.res.Columns == nil {
				out = append(out, msg(pgwire.BackendNoData, nil)...)
				continue
			}
			out = append(out, rowDescription(cur.res.Columns, cur.res.OIDs)...)
		case pgwire.FrontendExecute:
			b.mu.Lock()
			b.executes++
			b.mu.Unlock()
			if cur == nil {
				fail("34000", `portal "" does not exist`)
				skipping = true
				continue
			}
			if cur.ran && cur.res.Columns == nil {
				// A statement without rows runs once; only a portal the
				// server fetches from completes again (PortalRun in pquery.c).
				fail("55000", `portal "" cannot be run`)
				skipping = true
				continue
			}
			_, rest, _ := cut(m.Body)
			limit := 0
			if len(rest) >= 4 {
				limit = int(binary.BigEndian.Uint32(rest))
			}
			rows := cur.res.Rows[cur.next:]
			// The server stops at the limit without looking ahead: with
			// exactly limit rows left it suspends, and the next Execute
			// finds the portal at its end and completes with none.
			suspend := limit > 0 && len(rows) >= limit
			if suspend {
				rows = rows[:limit]
			}
			for _, r := range rows {
				out = append(out, dataRow(r)...)
			}
			cur.next += len(rows)
			if suspend {
				out = append(out, msg(pgwire.BackendPortalSuspended, nil)...)
				continue
			}
			// The tag counts this Execute's rows, not the portal's.
			out = append(out, commandComplete(complete(cur.sql, cur.res, len(rows)))...)
			cur.ran = true
		case pgwire.FrontendClose:
			out = append(out, msg(pgwire.BackendCloseComplete, nil)...)
		case pgwire.FrontendFlush:
			write()
		case pgwire.FrontendSync:
			skipping = false
			cur = nil
			if status == 'I' {
				// Outside a transaction block a statement is its own
				// transaction, and its local settings end with it.
				endTx()
			}
			out = append(out, msg(pgwire.BackendReadyForQuery, []byte{status})...)
			write()
		case pgwire.FrontendTerminate:
			return
		}
	}
}

// statements splits a simple Query's text on the semicolons outside single
// quotes, dropping empty statements — the server's own split, for the
// quoting this fake meets.
func statements(text string) []string {
	var out []string
	start, quoted := 0, false
	for i := range len(text) {
		switch {
		case text[i] == '\'':
			quoted = !quoted
		case text[i] == ';' && !quoted:
			if s := strings.TrimSpace(text[start:i]); s != "" {
				out = append(out, s)
			}
			start = i + 1
		}
	}
	if s := strings.TrimSpace(text[start:]); s != "" {
		out = append(out, s)
	}
	return out
}

// verb returns a statement's first two words, upper-cased.
func verb(sql string) (first, second string) {
	f := strings.Fields(strings.ToUpper(sql))
	if len(f) > 0 {
		first = f[0]
	}
	if len(f) > 1 {
		second = f[1]
	}
	return first, second
}

// endsBlock reports whether a statement leaves an aborted transaction
// block, the only kind a server runs there (IsTransactionExitStmt in
// postgres.c).
func endsBlock(first, second string) bool {
	switch first {
	case "COMMIT", "END", "ROLLBACK", "ABORT":
		return true
	case "PREPARE":
		return second == "TRANSACTION"
	}
	return false
}

// commandTag is the CommandComplete tag a server sends for a statement
// starting with verb that produced n rows.
func commandTag(verb string, n int) string {
	switch verb {
	case "SELECT", "VALUES", "TABLE", "WITH":
		return "SELECT " + strconv.Itoa(n)
	case "INSERT":
		return "INSERT 0 " + strconv.Itoa(n)
	case "UPDATE", "DELETE", "MERGE":
		return verb + " " + strconv.Itoa(n)
	case "END":
		return "COMMIT"
	case "ABORT":
		return "ROLLBACK"
	case "START":
		return "START TRANSACTION"
	}
	return verb
}

func commandComplete(tag string) []byte {
	return msg(pgwire.BackendCommandComplete, append([]byte(tag), 0))
}

func cut(b []byte) (field string, rest []byte, ok bool) {
	before, after, ok := bytes.Cut(b, []byte{0})
	if !ok {
		return "", nil, false
	}
	return string(before), after, true
}

// bindParams reads the parameter values of a Bind body positioned after the
// portal and statement names.
func bindParams(b []byte) [][]byte {
	take := func(n int) []byte {
		if n < 0 || len(b) < n {
			b = nil
			return make([]byte, max(n, 0))
		}
		v := b[:n]
		b = b[n:]
		return v
	}
	formats := int(binary.BigEndian.Uint16(take(2)))
	take(2 * formats)
	n := int(binary.BigEndian.Uint16(take(2)))
	params := make([][]byte, 0, n)
	for range n {
		l := int32(binary.BigEndian.Uint32(take(4))) //nolint:gosec // G115: the protocol field is int32
		if l < 0 {
			params = append(params, nil)
			continue
		}
		params = append(params, bytes.Clone(take(int(l))))
	}
	return params
}

func msg(typ byte, body []byte) []byte {
	out := []byte{typ, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], uint32(len(body)+4)) //nolint:gosec // G115: small test messages
	return append(out, body...)
}

func rowDescription(names []string, oids []uint32) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(names))) //nolint:gosec // G115: a few columns
	for i, n := range names {
		b = append(b, n...)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, 0) // table OID
		b = binary.BigEndian.AppendUint16(b, 0) // column number
		b = binary.BigEndian.AppendUint32(b, oids[i])
		b = binary.BigEndian.AppendUint16(b, 8) // type size
		b = binary.BigEndian.AppendUint32(b, 0) // type modifier
		b = binary.BigEndian.AppendUint16(b, uint16(pgwire.FormatBinary))
	}
	return msg(pgwire.BackendRowDescription, b)
}

func dataRow(values [][]byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(values))) //nolint:gosec // G115: a few columns
	for _, v := range values {
		if v == nil {
			b = binary.BigEndian.AppendUint32(b, ^uint32(0)) // -1: NULL
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(v))) //nolint:gosec // G115: small values
		b = append(b, v...)
	}
	return msg(pgwire.BackendDataRow, b)
}

func errorResponse(code, m string) []byte {
	if code == "" {
		code = "XX000"
	}
	var b []byte
	for _, kv := range [][2]string{{"S", "ERROR"}, {"V", "ERROR"}, {"C", code}, {"M", m}} {
		b = append(b, kv[0][0])
		b = append(b, kv[1]...)
		b = append(b, 0)
	}
	return msg(pgwire.BackendErrorResponse, append(b, 0))
}
