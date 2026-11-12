package postgres

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A ReadyForQuery that arrives before every part has answered, with no
// ErrorResponse on the way, is a protocol error that breaks the connection:
// a server that omits part 0's CommandComplete merges part 1's result into
// part 0 — part 1's row delivered under index 0, part 1 never delivered —
// and Pipeline returned nil. The mirror of
// TestPipelineRefusesAnEarlySplitCommandComplete. The witness answers every
// part.
func TestPipelineRefusesAReadyForQueryBeforeEveryPart(t *testing.T) {
	part := func(v int64, complete bool) []byte {
		var s []byte
		s = append(s, beSimple(pgwire.BackendParseComplete)...)
		s = append(s, beSimple(pgwire.BackendBindComplete)...)
		s = append(s, beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8})...)
		s = append(s, beDataRow(int8Val(v))...)
		if complete {
			s = append(s, beCommandComplete("SELECT 1")...)
		}
		return s
	}
	run := func(script []byte) ([]int, error) {
		srv := &fakeServer{responses: [][]byte{script}}
		tx := &Tx{conn: NewConn(srv, Config{})}
		parts := []Part{{SQL: "SELECT a"}, {SQL: "SELECT b"}}
		var indices []int
		err := tx.Pipeline(t.Context(), parts, QueryConfig{BatchRows: 4},
			func(i int, b sluice.Batch[Row]) bool {
				indices = append(indices, i)
				return true
			})
		return indices, err
	}

	t.Run("witness: every part answered", func(t *testing.T) {
		script := append(part(1, true), part(2, true)...)
		script = append(script, beReadyForQuery()...)
		indices, err := run(script)
		if err != nil || len(indices) != 2 || indices[0] != 0 || indices[1] != 1 {
			t.Fatalf("err = %v, indices = %v, want nil and [0 1]", err, indices)
		}
	})

	t.Run("part 0's CommandComplete missing", func(t *testing.T) {
		script := append(part(1, false), part(2, true)...)
		script = append(script, beReadyForQuery()...)
		indices, err := run(script)
		if !errors.Is(err, ErrProtocol) || !errors.Is(err, ErrConnBroken) {
			t.Fatalf("Pipeline returned %v with fn called for parts %v; want ErrProtocol and ErrConnBroken: the ReadyForQuery came after 1 of 2 parts", err, indices)
		}
	})
}
