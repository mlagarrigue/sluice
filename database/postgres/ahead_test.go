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
		src := NewConn(client).Query(t.Context(), "SELECT n", nil, QueryConfig{BatchRows: 2})
		src.Stream()(func(sluice.Batch[Row]) bool { return true })
		if err := src.Err(); err != nil {
			t.Errorf("query: %v", err)
		}
		<-done
	})
	return executes
}
