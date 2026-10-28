package postgres

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
)

// A query parameter past what a server accepts in one message is refused
// before a byte leaves, and the connection serves the next query. The wire
// layer refused it at Flush with the read side's sentinel, after the
// statement cache and the query's state had moved for a batch that never
// left; the connector then broke a connection whose stream was intact, and
// advised Config.MaxMessage, which does not move this bound
// (RP-20261025-pg-02).
func TestAnOversizedBindLeavesTheConnectionUsable(t *testing.T) {
	saved := maxOutgoing
	maxOutgoing = 256 // stand-in for 1 GiB, so the test allocates nothing
	t.Cleanup(func() { maxOutgoing = saved })

	synctest.Test(t, func(t *testing.T) {
		srv, conn := newScriptedPipe(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			// The first write the server sees is the second query's: the
			// oversized one sent nothing.
			if got := srv.read(); !strings.HasPrefix(got, "P") {
				t.Errorf("the first write the server saw was %q, want the next query's Parse", got)
			}
			srv.answer()
			srv.read() // its Sync
			srv.write(beReadyForQuery())
		}()

		big := Param{Value: make([]byte, 300)}
		src := conn.Query(t.Context(), "SELECT $1::bytea", []Param{big}, QueryConfig{})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		err := src.Err()
		if !errors.Is(err, ErrOutgoingTooLarge) || errors.Is(err, ErrConnBroken) {
			t.Fatalf("the oversized query ended with %v, want ErrOutgoingTooLarge and not ErrConnBroken", err)
		}
		if strings.Contains(err.Error(), "MaxMessage") {
			t.Errorf("the error advises Config.MaxMessage, which does not move this bound: %v", err)
		}
		if err := conn.Err(); err != nil {
			t.Fatalf("the connection is broken after a refusal that sent nothing: %v", err)
		}
		readAll(t, conn, "SELECT 1")
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the ReadyForQuery owed: %v", err)
		}
		<-done
	})
}
