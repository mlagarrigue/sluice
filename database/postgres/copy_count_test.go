package postgres

import "testing"

// WriteTuples takes the tuples and the number of rows they hold. A count of
// zero or less returned nil before the tuples were looked at: bytes handed
// over with a wrong count were dropped without an error, nothing was sent
// and Rows kept saying nothing was written. An empty batch — no bytes, no
// rows — is still nothing to write; bytes with no row, or a negative count,
// are refused, and the copy stays finishable.
func TestWriteTuplesRefusesBytesWithAWrongCount(t *testing.T) {
	srv := &fakeServer{responses: txCycle()}
	conn := NewConn(srv, Config{})
	cp, err := conn.CopyFrom(t.Context(), "COPY t FROM STDIN WITH (FORMAT BINARY)", CopyConfig{RowsPerTx: 1000})
	if err != nil {
		t.Fatal(err)
	}
	// Control: an empty batch is nothing to write, and that is not an error.
	if err := cp.WriteTuples(nil, 0); err != nil {
		t.Fatalf("control: an empty batch = %v", err)
	}

	row := tuple(new(int64(1)), nil)
	for _, rows := range []int{0, -1} {
		if err := cp.WriteTuples(row, rows); err == nil {
			t.Errorf("WriteTuples(%d bytes, rows=%d) = nil: the tuples were dropped without an error (%d bytes sent, Rows %d)",
				len(row), rows, srv.sent.Len(), cp.Rows())
		}
	}
}
