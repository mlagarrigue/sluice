package postgres

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
)

// resync, recovering a connection to its ReadyForQuery, ends at once when
// the ParameterStatus it meets breaks the connection: a client_encoding this
// package cannot decode leaves nothing worth recovering, and the server may
// well send no ReadyForQuery. Without that return, resync went on reading
// for a ReadyForQuery that never comes — here a deadlock of the bubble,
// against a server a read until the deadline — and no test held it. The
// witness is the same script with the encoding the package decodes.
func TestResyncStopsAtAParameterThatBreaksTheConnection(t *testing.T) {
	run := func(t *testing.T, encoding string) (err error, broken error) {
		t.Helper()
		synctest.Test(t, func(t *testing.T) {
			srv, conn := newScriptedPipe(t)
			conn.inTx = true // a read in a block waits for its ReadyForQuery: it resyncs
			done := make(chan struct{})
			go func() {
				defer close(done)
				srv.read() // the query
				srv.answer()
				if got := srv.read(); got != "S" {
					t.Errorf("the client sent %q after the result, want its Sync", got)
				}
				srv.write(beParameterStatus("client_encoding", encoding))
				if encoding == "UTF8" {
					srv.write(frame('Z', "T"))
				}
			}()
			src := conn.Query(t.Context(), "SELECT 1", nil, QueryConfig{BatchRows: 2})
			src.Stream()(func(sluice.Batch[Row]) bool { return true })
			err, broken = src.Err(), conn.Err()
			<-done
		})
		return err, broken
	}
	if err, broken := run(t, "UTF8"); err != nil || broken != nil {
		t.Fatalf("witness: the query ended with %v, the connection with %v", err, broken)
	}
	err, broken := run(t, "LATIN1")
	if err == nil || !strings.Contains(err.Error(), "client_encoding") || !errors.Is(broken, ErrConnBroken) {
		t.Fatalf("the query ended with %v, the connection with %v; want the encoding's error and a broken connection", err, broken)
	}
}
