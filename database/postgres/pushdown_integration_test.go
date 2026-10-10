package postgres_test

import (
	"encoding/binary"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/pushdown"
)

// The first source in the repository to read a [pushdown.Demand], checked
// against a server that can disagree about what a re-bound portal and a
// capped Execute produce. Ten thousand ordered keys, a hundred per batch, and
// a consumer that changes its mind between two batches.
const pushdownSQL = "SELECT g FROM generate_series(1::bigint, 10000::bigint) AS g WHERE g > $1 ORDER BY g"

func int8Key(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func pushdownQuery(t *testing.T, conn *postgres.Conn, d *pushdown.Demand) sluice.Source[postgres.Row] {
	t.Helper()
	return conn.Query(t.Context(), pushdownSQL,
		[][]byte{int8Key(0)}, // the floor: every key is above it until a bound says otherwise
		postgres.QueryConfig{BatchRows: 100, ParamOIDs: []uint32{postgres.OIDInt8}, Demand: d, KeyParam: 1},
	)
}

// readKeys consumes src, calling between after each batch, and returns the
// keys and the batch count.
func readKeys(t *testing.T, src sluice.Source[postgres.Row], between func(batch int)) (keys []int64, batches int) {
	t.Helper()
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		batches++
		for _, row := range b.Items {
			v, isNull := row.Value(0)
			if isNull {
				t.Fatal("a key came back NULL")
			}
			k, err := postgres.DecodeInt8(v)
			if err != nil {
				t.Fatalf("decoding a key: %v", err)
			}
			keys = append(keys, k)
		}
		if between != nil {
			between(batches)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	return keys, batches
}

func expectRange(t *testing.T, keys []int64, from, to int64) {
	t.Helper()
	if got, want := int64(len(keys)), to-from+1; got != want {
		t.Fatalf("got %d keys, want %d (%d..%d)", got, want, from, to)
	}
	for i, k := range keys {
		if k != from+int64(i) {
			t.Fatalf("key %d is %d, want %d", i, k, from+int64(i))
		}
	}
}

// A limit becomes the row count the server is asked for: 250 wanted, three
// batches, the last one short, and nothing drained past it.
func TestIntegrationPushdownLimit(t *testing.T) {
	conn := dial(t)
	var d pushdown.Demand
	d.LimitTo(250)

	// The consumer owns the count: the limit is what it still wants, so it
	// republishes after each batch it took. The source caps each request at
	// the last published figure and never subtracts on its own.
	keys, batches := readKeys(t, pushdownQuery(t, conn, &d), func(batch int) {
		d.LimitTo(250 - int64(batch)*100)
	})
	expectRange(t, keys, 1, 250)
	if batches != 3 {
		t.Errorf("%d batches for a limit of 250 at 100 per batch, want 3", batches)
	}
	if v := d.Violations(); v != 0 {
		t.Errorf("Violations = %d, want 0", v)
	}
	assertConnUsable(t, conn)
}

// A limit that falls mid-stream is honoured by the next request: the server
// is asked for exactly the remainder, and the stream ends on it.
func TestIntegrationPushdownLimitTightens(t *testing.T) {
	conn := dial(t)
	var d pushdown.Demand
	d.LimitTo(1000)

	keys, batches := readKeys(t, pushdownQuery(t, conn, &d), func(batch int) {
		switch batch {
		case 1:
			d.LimitTo(900)
		case 2:
			d.LimitTo(30) // 200 taken, and the consumer decides 30 more will do
		default:
			d.LimitTo(0)
		}
	})
	expectRange(t, keys, 1, 230)
	if batches != 3 {
		t.Errorf("%d batches, want 3 (100, 100, 30)", batches)
	}
	if v := d.Violations(); v != 0 {
		t.Errorf("Violations = %d, want 0", v)
	}
	assertConnUsable(t, conn)
}

// A bound published after the first batch re-binds the portal: the second
// batch resumes above the bound, the rows between are never produced.
func TestIntegrationPushdownAdvance(t *testing.T) {
	conn := dial(t)
	var d pushdown.Demand

	keys, batches := readKeys(t, pushdownQuery(t, conn, &d), func(batch int) {
		if batch == 1 {
			d.AdvanceTo(int8Key(9900))
		}
	})
	if len(keys) != 200 {
		t.Fatalf("got %d keys, want 200 (100 before the bound, 100 after)", len(keys))
	}
	expectRange(t, keys[:100], 1, 100)
	expectRange(t, keys[100:], 9901, 10000)
	if batches != 2 {
		t.Errorf("%d batches, want 2", batches)
	}
	if v := d.Violations(); v != 0 {
		t.Errorf("Violations = %d, want 0", v)
	}
	assertConnUsable(t, conn)
}

// AdvanceBy is the publisher the package ships: it advances on every batch,
// so every batch re-binds — and the result is the same rows, in the same
// order, with the violation counter still at zero.
func TestIntegrationPushdownAdvanceBy(t *testing.T) {
	conn := dial(t)
	var d pushdown.Demand
	d.LimitTo(1234)

	src := pushdownQuery(t, conn, &d)
	rows := pushdown.AdvanceBy(src.Stream(), &d, func(r postgres.Row) []byte {
		v, _ := r.Value(0)
		return v // already big-endian int8: the wire's encoding is the key's
	})
	keys, batches := readKeys(t, sluice.NewSource(rows, src.Err), func(batch int) {
		d.LimitTo(max(0, 1234-int64(batch)*100))
	})
	expectRange(t, keys, 1, 1234)
	if batches != 13 {
		t.Errorf("%d batches, want 13", batches)
	}
	if v := d.Violations(); v != 0 {
		t.Errorf("Violations = %d, want 0", v)
	}
	assertConnUsable(t, conn)
}

// A demand that wants nothing before the query starts sends nothing: no
// rows, no error, and a connection that was never made busy.
func TestIntegrationPushdownNothingWanted(t *testing.T) {
	conn := dial(t)
	var d pushdown.Demand
	d.LimitTo(0)

	keys, batches := readKeys(t, pushdownQuery(t, conn, &d), nil)
	if len(keys) != 0 || batches != 0 {
		t.Errorf("got %d keys in %d batches, want none", len(keys), batches)
	}
	assertConnUsable(t, conn)
}

// The connection is at a known position after a pushed-down query: the next
// one answers as if nothing had happened.
func assertConnUsable(t *testing.T, conn *postgres.Conn) {
	t.Helper()
	src := conn.Query(t.Context(), "SELECT 7::bigint", nil, postgres.QueryConfig{AllRows: true})
	keys, _ := readKeys(t, src, nil)
	if len(keys) != 1 || keys[0] != 7 {
		t.Fatalf("the next query returned %v, want [7]", keys)
	}
}
