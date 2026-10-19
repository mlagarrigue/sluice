package postgres

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A batched read keeps one round trip's worth of batches asked for: the
// round trip and the first batch are timed, and the first PortalSuspended
// asks for enough Executes that the server is never left waiting to hear of
// the next one. On loopback, where a batch outlasts the round trip, that is
// the floor of two; it never exceeds the ceiling.
func TestBatchedReadAsksAheadByRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		rtt, batch time.Duration
		want       int
	}{
		{10 * time.Millisecond, time.Millisecond, 11},
		{10 * time.Millisecond, 3 * time.Millisecond, 5},
		{time.Millisecond, time.Millisecond, minAhead},
		{time.Millisecond, 4 * time.Millisecond, minAhead},
		{time.Second, time.Millisecond, maxAhead},
	} {
		t.Run(fmt.Sprintf("rtt=%v/batch=%v", tc.rtt, tc.batch), func(t *testing.T) {
			if got := executesAfterFirstBatch(t, tc.rtt, tc.batch); got != tc.want {
				t.Errorf("%d Executes sent at the first PortalSuspended, want %d", got, tc.want)
			}
		})
	}
}

// executesAfterFirstBatch plays a server rtt away whose first batch of two
// rows takes batch to produce, and reports how many Executes the client sent
// on its first PortalSuspended. The result ends there.
func executesAfterFirstBatch(t *testing.T, rtt, batch time.Duration) (executes int) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = client.Close(); _ = server.Close() }()
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 64<<10)
			read := func() string {
				n, err := server.Read(buf)
				if err != nil {
					t.Errorf("scripted server reading: %v", err)
				}
				return frontendTypes(buf[:n])
			}
			write := func(msgs ...[]byte) {
				for _, m := range msgs {
					if _, err := server.Write(m); err != nil {
						t.Errorf("scripted server writing: %v", err)
					}
				}
			}
			read() // the query
			time.Sleep(rtt)
			write(beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"n"}, []uint32{OIDInt8}))
			time.Sleep(batch)
			write(beDataRow(int8Val(1)), beDataRow(int8Val(2)), beSimple(pgwire.BackendPortalSuspended))
			executes = strings.Count(read(), "E")
			write(beCommandComplete("SELECT 0"))
			read() // Sync
			write(beReadyForQuery())
		}()
		conn := NewConn(client)
		src := conn.Query(t.Context(), "SELECT n", nil, QueryConfig{BatchRows: 2})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		if err := src.Err(); err != nil {
			t.Errorf("query: %v", err)
		}
		// The read left its ReadyForQuery owed, and the scripted server
		// writes it on a pipe, which waits for a reader.
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the owed ReadyForQuery: %v", err)
		}
		<-done
	})
	return executes
}

// A statement keeps the depth of its shortest first batch, not of its latest:
// a first batch is the server's work plus whatever delayed this side, and one
// slow sample taken for the work would make every later read of the statement
// ask too shallow. Measured on a 10 ms link, a read of 98 batches was 12%
// behind FetchAll for it. Here the second read's first batch takes four times
// the first's, and the read after it must still leave with the Executes the
// 1 ms batch earned.
func TestBatchedReadKeepsTheDepthOfItsShortestFirstBatch(t *testing.T) {
	const rtt = 10 * time.Millisecond
	batches := []time.Duration{time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond}
	want := 1 + depth(rtt, batches[0])
	if shallow := 1 + depth(rtt, batches[1]); shallow >= want {
		t.Fatalf("the slow batch asks for %d Executes, no fewer than the fast one's %d: the test cannot tell them apart", shallow, want)
	}
	var executes []int
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = client.Close(); _ = server.Close() }()
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 64<<10)
			read := func() string {
				n, err := server.Read(buf)
				if err != nil {
					t.Errorf("scripted server reading: %v", err)
				}
				return frontendTypes(buf[:n])
			}
			write := func(msgs ...[]byte) {
				for _, m := range msgs {
					if _, err := server.Write(m); err != nil {
						t.Errorf("scripted server writing: %v", err)
					}
				}
			}
			for i, batch := range batches {
				executes = append(executes, strings.Count(read(), "E")) // the query
				if i > 0 {
					// The previous read's ReadyForQuery, owed: the client reads
					// it behind the query, and a pipe write waits for a reader,
					// so it cannot be written before the query is read.
					write(beReadyForQuery())
				}
				time.Sleep(rtt)
				if i == 0 {
					write(beSimple(pgwire.BackendParseComplete))
				}
				write(beSimple(pgwire.BackendBindComplete), beRowDescription([]string{"n"}, []uint32{OIDInt8}))
				time.Sleep(batch)
				write(beDataRow(int8Val(1)), beDataRow(int8Val(2)), beSimple(pgwire.BackendPortalSuspended))
				if i == 0 {
					read() // the Executes the first PortalSuspended tops up with
				}
				write(beCommandComplete("SELECT 0"))
				read() // Sync
			}
			write(beReadyForQuery())
		}()
		conn := NewConn(client)
		for i := range batches {
			src := conn.Query(t.Context(), "SELECT n", nil, QueryConfig{BatchRows: 2, Fetch: FetchBatches})
			src.Stream()(func(sluice.Batch[Row]) bool { return true })
			if err := src.Err(); err != nil {
				t.Errorf("read %d: %v", i+1, err)
			}
		}
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the owed ReadyForQuery: %v", err)
		}
		<-done
	})
	if len(executes) != len(batches) {
		t.Fatalf("the server saw %d reads, want %d", len(executes), len(batches))
	}
	if executes[0] != 1 {
		t.Errorf("read 1 sent %d Executes with its query, want 1: nothing is known of the statement yet", executes[0])
	}
	if executes[1] != want {
		t.Errorf("read 2 sent %d Executes with its query, want %d", executes[1], want)
	}
	if executes[2] != want {
		t.Errorf("read 3 sent %d Executes with its query, want %d: the slow first batch of read 2 made the statement shallower", executes[2], want)
	}
}
