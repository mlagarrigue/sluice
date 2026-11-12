package postgres

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// QueryConfig.BatchBytes bounds what a batch holds: the values, and the
// offset each field takes in Rows.ends, NULL included. Counted on the values
// alone, a batch of wide, mostly-NULL rows never closed on its byte bound —
// only BatchRows ended it, at four bytes per field per row the bound did not
// see. The control: values close the batch on the bound.
func TestABatchByteBoundCountsTheFieldsOffsets(t *testing.T) {
	const fields, maxBytes = 1000, 1 << 10

	ctl := newRows(1, 1<<20, maxBytes)
	value := binary.BigEndian.AppendUint16(nil, 1)
	value = binary.BigEndian.AppendUint32(value, 2*maxBytes)
	value = append(value, make([]byte, 2*maxBytes)...)
	if err := ctl.appendRow(value); err != nil || !ctl.full() {
		t.Fatalf("control: a row of %d bytes under a %d-byte bound: err %v, full %v", 2*maxBytes, maxBytes, err, ctl.full())
	}

	nulls := binary.BigEndian.AppendUint16(nil, fields)
	for range fields {
		nulls = binary.BigEndian.AppendUint32(nulls, ^uint32(0))
	}
	rows := newRows(fields, 1<<20, maxBytes)
	if err := rows.appendRow(nulls); err != nil {
		t.Fatal(err)
	}
	if !rows.full() || !rows.fullOnBytes() {
		t.Errorf("a row of %d NULL fields — %d bytes of offsets — under a %d-byte bound: full %v, on bytes %v",
			fields, 4*len(rows.ends), maxBytes, rows.full(), rows.fullOnBytes())
	}
}

// valueBytes is what one decoded column costs a replication batch.
func TestValueBytesIsTheSizeOfAValue(t *testing.T) {
	if got := int(unsafe.Sizeof(Value{})); got != valueBytes {
		t.Errorf("unsafe.Sizeof(Value{}) = %d, valueBytes says %d", got, valueBytes)
	}
}

// ReplicationConfig.BatchBytes counts what the batch holds besides the
// payloads: a Value per decoded column. Inserts of five hundred NULL columns
// carry a few hundred bytes of payload each and a Value per column; counted
// on payloads alone, the batch took every one of them under a 4 KiB bound.
func TestAReplicationBatchByteBoundCountsTheColumns(t *testing.T) {
	const cols, inserts = 500, 40
	names := make([]string, cols)
	nulls := make([][]byte, cols)
	for i := range names {
		names[i] = fmt.Sprint("c", i)
	}
	var script []byte
	script = append(script, xlog(0x10, beginMsg(0x30, 1))...)
	script = append(script, xlog(0x20, relMsg(1, "s", "t", names...))...)
	for range inserts {
		script = append(script, xlog(0x20, rowMsg('I', 1, byte('N'), tupleData(nulls...)))...)
	}
	script = append(script, xlog(0x30, commitMsg(0x30, 0x38))...)
	script = append(script, frame(pgwire.BackendCopyDone, "")...)
	script = append(script, frame(pgwire.BackendCommandComplete, "START_REPLICATION\x00")...)
	script = append(script, frame(pgwire.BackendReadyForQuery, "I")...)
	r := &Replication{c: NewConn(&scriptedRW{Reader: bytes.NewReader(script)}, Config{}), batch: 1000, batchBytes: 4 << 10, interval: time.Hour}

	var sizes []int
	src := r.Changes(t.Context())
	src.Stream()(func(b sluice.Batch[Change]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	// 4 KiB holds no more than a few hundred Values: every insert closes a
	// batch, as the bound says.
	if len(sizes) < inserts/2 {
		t.Errorf("batches of %v under a 4 KiB bound for %d inserts of %d columns, want them cut on the columns' Values", sizes, inserts, cols)
	}
}
