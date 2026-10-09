package postgres

import (
	"testing"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// discardTransport accepts anything and answers whatever the script says, so a
// COPY can be measured without a server or a socket in the way.
type discardTransport struct {
	responses [][]byte
	pos       int
	buf       []byte
	written   int64
}

func (d *discardTransport) Read(p []byte) (int, error) {
	if len(d.buf) == 0 {
		if d.pos >= len(d.responses) {
			d.pos = 0
		}
		d.buf = d.responses[d.pos]
		d.pos++
	}
	n := copy(p, d.buf)
	d.buf = d.buf[n:]
	return n, nil
}

func (d *discardTransport) Write(p []byte) (int, error) {
	d.written += int64(len(p))
	return len(p), nil
}
func (d *discardTransport) Close() error { return nil }

// BenchmarkCopyWriteTuples measures the path a bulk load actually spends its
// time in, with the socket removed.
//
// Profiling a COPY against a real server put 38% in the write syscall and
// **18% in memmove** — the caller's buffer copied into the copier's, and the
// copier's copied into the writer's, two passes over the same megabytes. This
// isolates that.
func BenchmarkCopyWriteTuples(b *testing.B) {
	const rows = 4096
	tuples := make([]byte, 0, rows*24)
	scratch := make([]byte, 0, 16)
	for i := range rows {
		tuples = AppendTupleHeader(tuples, 2)
		scratch = AppendInt8(scratch[:0], int64(i))
		tuples = AppendField(tuples, scratch)
		tuples = AppendField(tuples, []byte("a label of some length"))
	}

	srv := &discardTransport{responses: [][]byte{
		append(beCommandCompleteBench("BEGIN"), beReadyForQueryBench()...),
		beCopyInResponseBench(),
		append(beCommandCompleteBench("COPY 0"), beReadyForQueryBench()...),
		append(beCommandCompleteBench("COMMIT"), beReadyForQueryBench()...),
	}}
	conn := NewConn(srv)

	cp, err := conn.CopyFrom(b.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)",
		CopyConfig{RowsPerTx: 1 << 30, MaxMessage: 64 << 10})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(tuples)))
	b.ReportAllocs()
	for b.Loop() {
		if err := cp.WriteTuples(tuples, rows); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sinkBytes = srv.written
}

var sinkBytes int64

func beCommandCompleteBench(tag string) []byte {
	return frame(pgwire.BackendCommandComplete, tag+"\x00")
}
func beReadyForQueryBench() []byte { return frame(pgwire.BackendReadyForQuery, "I") }
func beCopyInResponseBench() []byte {
	return frame(pgwire.BackendCopyInResponse, "\x01\x00\x02\x00\x01\x00\x01")
}
