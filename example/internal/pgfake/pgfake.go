// Package pgfake is just enough of a PostgreSQL backend to run the examples'
// stores without a server: simple-query statements, the extended protocol
// with row limits and portal suspension, set_config, and errors — each
// answered only when the client asks (Sync, Flush or a simple Query), as a
// real backend answers.
//
// What a statement returns is the caller's [Backend.Exec]; the fake is the
// protocol around it. It is test support for the examples and nothing else.
package pgfake

import (
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// Result is what one statement produces. Values are binary-format column
// values; a nil value is NULL.
type Result struct {
	Columns []string
	OIDs    []uint32
	Rows    [][][]byte
	Tag     string // CommandComplete tag; defaults to "SELECT <n>"
	Err     string // non-empty answers the statement with an ErrorResponse
}

// Backend answers statements. Exec sees the SQL text, the bound binary
// parameters (nil for a simple Query), and the transaction-local settings
// established so far with set_config.
type Backend struct {
	Exec func(sql string, params [][]byte, settings map[string]string) Result

	mu    sync.Mutex
	execs []string
}

// Statements returns the SQL of every statement executed, in order.
func (b *Backend) Statements() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.execs...)
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
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.serve(server)
	}()
	tb.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return postgres.NewConn(client)
}

type portal struct {
	res  Result
	next int
}

func (b *Backend) serve(nc net.Conn) {
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
	write := func() bool {
		_, err := nc.Write(out)
		out = out[:0]
		return err == nil
	}
	fail := func(m string) {
		out = append(out, errorResponse(m)...)
		if status == 'T' {
			status = 'E'
		}
	}
	endTx := func() {
		status = 'I'
		for k := range local {
			delete(settings, k)
		}
		clear(local)
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
			sql, _, _ := cut(m.Body)
			tag := strings.ToUpper(strings.Fields(sql + " x")[0])
			if res := b.exec(sql, nil, settings); res.Err != "" {
				fail(res.Err)
			} else {
				switch tag {
				case "BEGIN":
					status = 'T'
				case "ROLLBACK", "COMMIT", "END":
					endTx()
				}
				out = append(out, msg(pgwire.BackendCommandComplete, append([]byte(tag), 0))...)
			}
			out = append(out, msg(pgwire.BackendReadyForQuery, []byte{status})...)
			if !write() {
				return
			}
		case pgwire.FrontendParse:
			name, rest, _ := cut(m.Body)
			sql, _, _ := cut(rest)
			stmts[name] = sql
			out = append(out, msg(pgwire.BackendParseComplete, nil)...)
		case pgwire.FrontendBind:
			_, rest, _ := cut(m.Body)
			stmt, rest, _ := cut(rest)
			sql := stmts[stmt]
			params := bindParams(rest)
			var res Result
			if strings.Contains(sql, "set_config") && len(params) >= 2 {
				settings[string(params[0])] = string(params[1])
				local[string(params[0])] = true
				res = Result{Columns: []string{"set_config"}, OIDs: []uint32{postgres.OIDText}, Rows: [][][]byte{{params[1]}}}
			}
			if r := b.exec(sql, params, settings); r.Err != "" || r.Columns != nil || r.Tag != "" {
				res = r
			}
			if res.Err != "" {
				fail(res.Err)
				skipping = true
				continue
			}
			cur = &portal{res: res}
			out = append(out, msg(pgwire.BackendBindComplete, nil)...)
		case pgwire.FrontendDescribe:
			if cur == nil || cur.res.Columns == nil {
				out = append(out, msg(pgwire.BackendNoData, nil)...)
				continue
			}
			out = append(out, rowDescription(cur.res.Columns, cur.res.OIDs)...)
		case pgwire.FrontendExecute:
			if cur == nil {
				fail("no portal")
				skipping = true
				continue
			}
			_, rest, _ := cut(m.Body)
			limit := 0
			if len(rest) >= 4 {
				limit = int(binary.BigEndian.Uint32(rest))
			}
			rows := cur.res.Rows[cur.next:]
			if limit > 0 && len(rows) > limit {
				for _, r := range rows[:limit] {
					out = append(out, dataRow(r)...)
				}
				cur.next += limit
				out = append(out, msg(pgwire.BackendPortalSuspended, nil)...)
				continue
			}
			for _, r := range rows {
				out = append(out, dataRow(r)...)
			}
			tag := cur.res.Tag
			if tag == "" {
				tag = "SELECT " + strconv.Itoa(len(cur.res.Rows))
			}
			out = append(out, msg(pgwire.BackendCommandComplete, append([]byte(tag), 0))...)
			cur.next = len(cur.res.Rows)
		case pgwire.FrontendClose:
			out = append(out, msg(pgwire.BackendCloseComplete, nil)...)
		case pgwire.FrontendFlush:
			if !write() {
				return
			}
		case pgwire.FrontendSync:
			skipping = false
			cur = nil
			if status == 'I' {
				// Outside a transaction block a statement is its own
				// transaction, and its local settings end with it.
				endTx()
			}
			out = append(out, msg(pgwire.BackendReadyForQuery, []byte{status})...)
			if !write() {
				return
			}
		case pgwire.FrontendTerminate:
			return
		}
	}
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

func errorResponse(m string) []byte {
	var b []byte
	for _, kv := range [][2]string{{"S", "ERROR"}, {"V", "ERROR"}, {"C", "XX000"}, {"M", m}} {
		b = append(b, kv[0][0])
		b = append(b, kv[1]...)
		b = append(b, 0)
	}
	return msg(pgwire.BackendErrorResponse, append(b, 0))
}
