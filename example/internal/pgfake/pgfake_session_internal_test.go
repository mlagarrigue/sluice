package pgfake

import (
	"encoding/binary"
	"slices"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// seenInTheNextTransaction sets app.tenant_id with set_config, its third
// argument isLocal written in the SQL text, and returns what the next
// transaction's handler sees of it.
func seenInTheNextTransaction(t *testing.T, isLocal string) string {
	t.Helper()
	var mu sync.Mutex
	seen := "<not run>"
	b := &Backend{Exec: func(sql string, _ [][]byte, settings map[string]string) Result {
		if sql == "SELECT 1" {
			mu.Lock()
			seen = settings["app.tenant_id"]
			mu.Unlock()
		}
		return Result{}
	}}
	conn := b.Conn(t)
	param := func(s string) postgres.Param {
		return postgres.Param{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, s)}
	}
	if _, err := conn.ExecParams(t.Context(), "SELECT set_config($1, $2, "+isLocal+")",
		[]postgres.Param{param("app.tenant_id"), param("acme")}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecParams(t.Context(), "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	return seen
}

// set_config's third argument says whether the setting ends with the
// transaction (true) or lasts the session (false). The fake took every one
// for local, so a tenant setting turned into a session one passed green in
// the examples. The witness is the local form.
func TestASessionSetConfigOutlivesItsTransaction(t *testing.T) {
	if got := seenInTheNextTransaction(t, "true"); got != "" {
		t.Fatalf("witness: set_config(..., true) still seen as %q in the next transaction", got)
	}
	if got := seenInTheNextTransaction(t, "false"); got != "acme" {
		t.Errorf("set_config(..., false) seen as %q in the next transaction, want %q: the fake ends a session setting with the transaction", got, "acme")
	}
}

// answerToBind sends a Bind of one binary parameter declared eight bytes
// long, carrying value, and returns the message types answered.
func answerToBind(t *testing.T, value []byte) []string {
	t.Helper()
	b := &Backend{}
	w, rd := wire(t, b)
	w.Parse("", "SELECT $1::int8", nil)
	var body []byte
	body = append(body, 0, 0)                     // unnamed portal, unnamed statement
	body = binary.BigEndian.AppendUint16(body, 1) // one format code
	body = binary.BigEndian.AppendUint16(body, 1) // binary
	body = binary.BigEndian.AppendUint16(body, 1) // one parameter
	body = binary.BigEndian.AppendUint32(body, 8) // declared eight bytes
	body = append(body, value...)
	body = binary.BigEndian.AppendUint16(body, 0) // result format codes
	w.Message(pgwire.FrontendBind, body)
	w.Sync()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return types(t, rd)
}

// A Bind whose parameter is shorter than its declared length is refused by
// a server; the fake filled it with zeros and answered BindComplete, so a
// client that wrote one would pass the examples.
func TestATruncatedBindIsRefused(t *testing.T) {
	if got := answerToBind(t, []byte{0, 0, 0, 0, 0, 0, 0, 7}); slices.Contains(got, "E") {
		t.Fatalf("witness: a well-formed Bind answered %q", got)
	}
	if got := answerToBind(t, []byte{0, 7}); !slices.Contains(got, "E") {
		t.Errorf("a Bind declaring 8 bytes and carrying 2 answered %q, want an ErrorResponse", got)
	}
}
