package postgres

import (
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
	"github.com/mlagarrigue/sluice/pushdown"
)

// demandAheadSQL is ordered by a unique key compared against $1, the shape a
// demand's bound needs.
const demandAheadSQL = "SELECT n FROM t WHERE n > $1 ORDER BY n"

// readDemandAhead runs demandAheadSQL in batches of two under d, against a
// scripted server that serves keys 1-2 and 3-4, asked ahead for two batches
// past each while the demand holds. publish runs once the consumer has 3-4.
// rest plays the server from there, given read and write; the keys the
// consumer received are returned.
func readDemandAhead(t *testing.T, d *pushdown.Demand, publish func(), rest func(read func() string, write func(...[]byte))) (keys []int64) {
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
			// One write per call: the pipe hands the client all of it in
			// one read, so a client that stops halfway and writes its Sync
			// is not left waiting on a server still writing the rest.
			write := func(msgs ...[]byte) {
				if _, err := server.Write(slices.Concat(msgs...)); err != nil {
					t.Errorf("scripted server writing: %v", err)
				}
			}
			read() // the query
			write(beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
				beRowDescription([]string{"n"}, []uint32{OIDInt8}),
				beDataRow(int8Val(1)), beDataRow(int8Val(2)), beSimple(pgwire.BackendPortalSuspended))
			// A round trip of zero gives the floor of two batches ahead,
			// asked before the first one is delivered, then topped up.
			if got := strings.Count(read(), "E"); got != minAhead {
				t.Errorf("%d Executes after the first batch of a demand that holds, want %d", got, minAhead)
			}
			write(beDataRow(int8Val(3)), beDataRow(int8Val(4)), beSimple(pgwire.BackendPortalSuspended))
			if got := read(); got != "EH" {
				t.Errorf("the top-up after the second batch sent %q, want EH", got)
			}
			rest(read, write)
		}()
		conn := NewConn(client)
		src := conn.Query(t.Context(), demandAheadSQL, [][]byte{int8Val(0)},
			QueryConfig{BatchRows: 2, ParamOIDs: []uint32{OIDInt8}, Demand: d, KeyParam: 1})
		src.Stream()(func(b sluice.Batch[Row]) bool {
			for _, r := range b.Items {
				v, _ := r.Value(0)
				k, err := DecodeInt8(v)
				if err != nil {
					t.Errorf("decoding a key: %v", err)
				}
				keys = append(keys, k)
			}
			if slices.Contains(keys, 4) && len(keys) == 4 {
				publish()
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Errorf("query: %v", err)
		}
		if err := conn.settleReady(); err != nil {
			t.Errorf("reading the owed ReadyForQuery: %v", err)
		}
		<-done
	})
	return keys
}

// A bound that moves while batches are asked ahead re-binds the portal, and
// the batches asked for the old one — keys 5-8, below the bound — are read
// and dropped up to the re-bind's BindComplete: the consumer sees the new
// portal's rows and nothing under the bound.
func TestDemandAheadDropsTheOldPortal(t *testing.T) {
	var d pushdown.Demand
	keys := readDemandAhead(t, &d, func() { d.AdvanceTo(int8Val(10)) },
		func(read func() string, write func(...[]byte)) {
			if got := read(); got != "BEH" {
				t.Errorf("the re-bind sent %q, want BEH", got)
			}
			write(beDataRow(int8Val(5)), beDataRow(int8Val(6)), beSimple(pgwire.BackendPortalSuspended),
				beDataRow(int8Val(7)), beDataRow(int8Val(8)), beSimple(pgwire.BackendPortalSuspended),
				beSimple(pgwire.BackendBindComplete),
				beDataRow(int8Val(11)), beDataRow(int8Val(12)), beCommandComplete("SELECT 2"))
			read() // Sync
			write(beReadyForQuery())
		})
	if want := []int64{1, 2, 3, 4, 11, 12}; !slices.Equal(keys, want) {
		t.Errorf("keys %v, want %v", keys, want)
	}
}

// A limit that appears while batches are asked ahead asks for nothing more,
// and the first batch already on its way is cut to it: one row wanted, one
// row delivered, the query ended on it, the last batch drained unseen.
func TestDemandAheadCutsToALimit(t *testing.T) {
	var d pushdown.Demand
	keys := readDemandAhead(t, &d, func() { d.LimitTo(1) },
		func(read func() string, write func(...[]byte)) {
			write(beDataRow(int8Val(5)), beDataRow(int8Val(6)), beSimple(pgwire.BackendPortalSuspended),
				beDataRow(int8Val(7)), beDataRow(int8Val(8)), beSimple(pgwire.BackendPortalSuspended))
			if got := read(); got != "S" {
				t.Errorf("after the limit ran out the client sent %q, want the Sync alone", got)
			}
			write(beReadyForQuery())
		})
	if want := []int64{1, 2, 3, 4, 5}; !slices.Equal(keys, want) {
		t.Errorf("keys %v, want %v", keys, want)
	}
}

// A Demand is safe for concurrent use: a source reads it while operators
// tighten it. run checks Exhausted and then calls replan, so a LimitTo(0)
// landing between the two reached replan, which copied it into rowsWanted —
// and Execute with a row count of 0 asks the server for every row. replan
// now never asks for fewer than one: the row it may fetch is cut by emit,
// which honours allowed, and the next batch's check ends the query.
func TestReplanNeverAsksForEveryRowWhenTheLimitFallsToZero(t *testing.T) {
	d := new(pushdown.Demand)
	d.LimitTo(5)
	q := &query{demand: pushdown.NewReader(d), batchRows: 100, rowsWanted: 100, allowed: -1}
	if q.demand.Exhausted() {
		t.Fatal("a limit of 5 reads as exhausted")
	}
	d.LimitTo(0) // the concurrent tightening, landed after the check
	q.replan()
	if q.rowsWanted == 0 {
		t.Fatal(`replan set rowsWanted to 0: Execute("", 0) asks the server for every row where the consumer wants none`)
	}
	if q.allowed != 0 {
		t.Errorf("allowed = %d after a limit of 0, want 0 so the next batch ends the query", q.allowed)
	}
}
