package postgres

import (
	"io"
	"net"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// Run under -race. Once readPipeline falls back to drainToReady
// (abortPipeline here; the recovery from a panic in fn takes the same
// path), a CopyInResponse met on the drain is refused with a CopyFail
// written on c.w — which the pipeline's sender goroutine owned until it was
// joined, after readPipeline returned: the race detector reported the two
// goroutines on Writer.buf. The refusal now waits for the sender. The
// witness plays the same script without the CopyInResponse: the drain only
// reads.
func TestPipelineDrainWaitsForTheSenderBeforeWriting(t *testing.T) {
	run := func(t *testing.T, copyIn bool) {
		t.Helper()
		client, server := net.Pipe()
		defer func() { _ = client.Close(); _ = server.Close() }()
		go func() {
			var s []byte
			// Framed whole, unparsable: one field announced, none present.
			s = append(s, frame(pgwire.BackendRowDescription, "\x00\x01")...)
			if copyIn {
				// Text format, zero columns.
				s = append(s, frame(pgwire.BackendCopyInResponse, "\x00\x00\x00")...)
			}
			s = append(s, beErrorResponse("57014", "canceling")...)
			s = append(s, beReadyForQuery()...)
			_, _ = server.Write(s)
			_, _ = io.Copy(io.Discard, server)
		}()
		tx := &Tx{conn: NewConn(client, Config{})}
		parts := []Part{{SQL: "SELECT a"}, {SQL: "COPY t FROM STDIN"}}
		err := tx.Pipeline(t.Context(), parts, QueryConfig{BatchRows: 4},
			func(int, sluice.Batch[Row]) bool { return true })
		if err == nil {
			t.Fatal("Pipeline accepted an unparsable RowDescription")
		}
	}
	t.Run("witness: drain without copy-in", func(t *testing.T) { run(t, false) })
	t.Run("drain meets copy-in", func(t *testing.T) { run(t, true) })
}
