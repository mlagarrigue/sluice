package postgres

import (
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// The connection keeps the shortest round trip it has seen, which is what
// depth reads to decide how far ahead to ask. Kept the other way, one slow
// read would make every later one ask ahead too much, and the suite stayed
// green: the two tests that read c.rtt's effect
// (TestBatchedReadAsksAheadByRoundTrip,
// TestBatchedReadKeepsTheDepthOfItsShortestFirstBatch) each use a single
// round-trip sample after the first. Three reads, 5, 2 and 9 ms from the
// query's flush to its BindComplete: the connection keeps 2.
func TestTheConnectionKeepsItsShortestRoundTrip(t *testing.T) {
	delays := []time.Duration{5 * time.Millisecond, 2 * time.Millisecond, 9 * time.Millisecond}
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = client.Close(); _ = server.Close() }()
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 64<<10)
			read := func() {
				if _, err := server.Read(buf); err != nil {
					t.Errorf("scripted server reading: %v", err)
				}
			}
			write := func(msgs ...[]byte) {
				for _, m := range msgs {
					if _, err := server.Write(m); err != nil {
						t.Errorf("scripted server writing: %v", err)
					}
				}
			}
			for i, d := range delays {
				read() // the query
				if i > 0 {
					write(beReadyForQuery()) // the previous read's, owed
				}
				time.Sleep(d)
				write(beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
					beRowDescription([]string{"n"}, []uint32{pgtype.OIDInt8}), beDataRow(int8Val(1)),
					beCommandComplete("SELECT 1"))
				read() // its Sync
			}
			write(beReadyForQuery())
		}()
		conn := NewConn(client, Config{})
		for i := range delays {
			// A different text each time: a cached statement sends no Parse.
			src := conn.Query(t.Context(), "SELECT "+string(rune('1'+i)), nil, QueryConfig{BatchRows: 2})
			src.Stream()(func(sluice.Batch[Row]) bool { return true })
			if err := src.Err(); err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
		}
		if err := conn.settleReady(); err != nil {
			t.Fatalf("reading the owed ReadyForQuery: %v", err)
		}
		if conn.rtt != delays[1] {
			t.Errorf("the connection keeps a round trip of %v after reads of %v, want the shortest, %v", conn.rtt, delays, delays[1])
		}
		<-done
	})
}
