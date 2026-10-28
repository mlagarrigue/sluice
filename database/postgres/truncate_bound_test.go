package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// truncMsg is a pgoutput Truncate naming oids, in that order.
func truncMsg(oids ...uint32) []byte {
	b := binary.BigEndian.AppendUint32([]byte{'T'}, uint32(len(oids))) //nolint:gosec // G115: a test's handful
	b = append(b, 0)                                                   // no CASCADE, no RESTART IDENTITY
	for _, oid := range oids {
		b = binary.BigEndian.AppendUint32(b, oid)
	}
	return b
}

// A Truncate names each relation once (§55.9; PostgreSQL's
// logicalrep_write_truncate writes one OID per relation truncated). One
// relation named five thousand times made five thousand changes out of
// twenty kilobytes, in one batch past BatchChanges: a Change per four bytes
// of a message the default MaxMessage lets reach 64 MiB
// (RP-20261025-secapp-02). A repeated relation is now malformed, so what a
// Truncate yields is bounded by the relation table, each entry of which
// cost the server a Relation message of its own.
func TestATruncateNamingARelationTwiceIsRefused(t *testing.T) {
	run := func(oids ...uint32) ([]int, error) {
		var script []byte
		script = append(script, xlog(0x20, relMsg(1, "s", "t", "a"))...)
		script = append(script, xlog(0x21, relMsg(2, "s", "u", "a"))...)
		script = append(script, xlog(0x30, truncMsg(oids...))...)
		script = append(script, frame(pgwire.BackendCopyDone, "")...)
		script = append(script, frame(pgwire.BackendCommandComplete, "START_REPLICATION\x00")...)
		script = append(script, frame(pgwire.BackendReadyForQuery, "I")...)
		r := &Replication{c: NewConn(&scriptedRW{Reader: bytes.NewReader(script)}), batch: 100, batchBytes: 1 << 20, interval: time.Hour}
		var sizes []int
		src := r.Changes(t.Context())
		src.Stream()(func(b sluice.Batch[Change]) bool { sizes = append(sizes, len(b.Items)); return true })
		return sizes, src.Err()
	}

	repeated := make([]uint32, 5000)
	for i := range repeated {
		repeated[i] = 1
	}
	if sizes, err := run(repeated...); !errors.Is(err, ErrProtocol) {
		t.Errorf("a Truncate naming one relation 5000 times: batches %v, err %v; want ErrProtocol", sizes, err)
	}
	if _, err := run(1, 2, 1); !errors.Is(err, ErrProtocol) {
		t.Errorf("a Truncate naming relation 1 twice among others: err %v, want ErrProtocol", err)
	}
	sizes, err := run(1, 2)
	if err != nil || len(sizes) != 1 || sizes[0] != 2 {
		t.Errorf("a Truncate of two relations: batches %v, err %v; want one batch of 2 changes", sizes, err)
	}
}
