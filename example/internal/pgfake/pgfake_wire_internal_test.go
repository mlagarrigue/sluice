package pgfake

import (
	"encoding/binary"
	"testing"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// wire opens a served connection and returns a writer and a reader on it,
// for what the client package never sends by itself.
func wire(t *testing.T, b *Backend) (*pgwire.Writer, *pgwire.Reader) {
	t.Helper()
	c := b.pipe(t, false)
	return pgwire.NewWriter(c), pgwire.NewReader(c)
}

// types reads messages until ReadyForQuery and returns their type bytes, with
// the SQLSTATE of an ErrorResponse appended after its 'E'.
func types(t *testing.T, rd *pgwire.Reader) []string {
	t.Helper()
	var out []string
	for {
		m, err := rd.Next()
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		out = append(out, string(m.Type))
		if m.Type == pgwire.BackendErrorResponse {
			for i := 0; i+1 < len(m.Body); {
				end := i + 1
				for end < len(m.Body) && m.Body[end] != 0 {
					end++
				}
				if m.Body[i] == 'C' {
					out = append(out, string(m.Body[i+1:end]))
				}
				i = end + 1
			}
		}
		if m.Type == pgwire.BackendReadyForQuery {
			return out
		}
	}
}

// Describe of a statement answers from the plan, as the server does: a
// ParameterDescription, then the row a SELECT would produce or NoData for a
// statement without one — not NoData whatever the statement, which the
// client package never asks for but a conformance test would.
func TestDescribeStatementAnswersFromThePlan(t *testing.T) {
	b := &Backend{Exec: func(sql string, _ [][]byte, _ map[string]string) Result {
		if sql == "SELECT n" {
			return Result{Columns: []string{"n"}, OIDs: []uint32{20}}
		}
		return Result{}
	}}
	w, rd := wire(t, b)
	w.Parse("sel", "SELECT n", nil)
	w.Describe('S', "sel")
	w.Parse("upd", "UPDATE t SET x = 1", nil)
	w.Describe('S', "upd")
	w.Sync()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	got := types(t, rd)
	want := []string{"1", "t", "T", "1", "t", "n", "Z"}
	if len(got) != len(want) {
		t.Fatalf("answers %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("answers %q, want %q", got, want)
		}
	}
}

// Bind of a name never parsed is SQLSTATE 26000, and the rest of the batch
// is skipped until Sync, not a silent success on an empty statement.
func TestBindOfAnUnknownStatementIs26000(t *testing.T) {
	b := &Backend{}
	w, rd := wire(t, b)
	var body []byte
	body = append(body, 0)                        // the unnamed portal
	body = append(body, "never-parsed"...)        // a statement nobody prepared
	body = append(body, 0)                        //
	body = binary.BigEndian.AppendUint16(body, 0) // parameter format codes
	body = binary.BigEndian.AppendUint16(body, 0) // parameters
	body = binary.BigEndian.AppendUint16(body, 0) // result format codes
	w.Message(pgwire.FrontendBind, body)
	w.Execute("", 0)
	w.Sync()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	got := types(t, rd)
	if len(got) != 3 || got[0] != "E" || got[1] != "26000" || got[2] != "Z" {
		t.Fatalf("answers %q, want an ErrorResponse 26000 then ReadyForQuery", got)
	}
}
