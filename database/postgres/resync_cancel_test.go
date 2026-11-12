package postgres

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// closeSpy counts the Close calls that reach the main connection's socket.
type closeSpy struct {
	io.ReadWriteCloser
	closes *atomic.Int32
}

func (s closeSpy) Close() error { s.closes.Add(1); return s.ReadWriteCloser.Close() }

// resync's early-stop cancellation (cancelNow) ends with resync, whatever
// way it leaves. It was ended on the read-error and ReadyForQuery exits
// only, not on the unknown-type exit nor on recordParameter breaking the
// connection: the watch outlived the call that started it, and
// CancelTimeout later closed the connection's socket, after Query had
// returned. The witness ends the statement with the server's
// query_canceled and ReadyForQuery.
func TestResyncEndsItsCancelWatchOnEveryExit(t *testing.T) {
	run := func(t *testing.T, hostileTail bool) (before, after int32) {
		t.Helper()
		synctest.Test(t, func(t *testing.T) {
			srv, conn := newPipeServer(t)
			conn.cancelKey = CancelKey{PID: 1, Secret: testSecret}
			conn.cancelTimeout = 10 * time.Second
			packets := make(chan []byte, 1)
			conn.cancelDial = func(context.Context) (io.ReadWriteCloser, error) {
				c, s := net.Pipe()
				go func() {
					defer func() { _ = s.Close() }() // the confirmation
					pkt := make([]byte, 16)
					if _, err := io.ReadFull(s, pkt); err == nil {
						packets <- pkt
					}
				}()
				return c, nil
			}
			var closes atomic.Int32
			conn.rw = closeSpy{ReadWriteCloser: conn.rw, closes: &closes}

			go func() {
				<-srv.read
				srv.write(t, beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
					beRowDescription([]string{"n"}, []uint32{pgtype.OIDInt8}))
				for n := int64(1); n < 100_000; n++ {
					synctest.Wait()
					select {
					case <-packets:
						if hostileTail {
							srv.write(t, frame('~', ""))
						} else {
							srv.write(t, beErrorResponse("57014", "canceling statement due to user request"), beReadyForQuery())
						}
						return
					default:
					}
					srv.write(t, beDataRow(int8Val(n)))
				}
			}()

			src := conn.Query(t.Context(), "SELECT n FROM generate_series(1, 100000) n", nil,
				QueryConfig{Fetch: FetchAll, BatchRows: 10})
			src.Stream()(func(sluice.Batch[Row]) bool { return false })
			_ = src.Err()
			before = closes.Load()
			time.Sleep(conn.cancelTimeout + time.Second)
			after = closes.Load()
		})
		return before, after
	}

	t.Run("witness: query_canceled then ReadyForQuery", func(t *testing.T) {
		if before, after := run(t, false); after != before {
			t.Fatalf("the socket was closed %d times after Query returned, want 0", after-before)
		}
	})
	t.Run("unknown message type on the drain", func(t *testing.T) {
		if before, after := run(t, true); after != before {
			t.Fatalf("the cancellation watch closed the socket %d time(s) CancelTimeout after Query returned: it outlived resync", after-before)
		}
	})
}
