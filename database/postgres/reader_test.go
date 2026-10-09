package postgres

import (
	"errors"
	"io"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// dribbleReader hands back one byte per Read, which is what a socket is
// allowed to do and what a test using bytes.Reader never does.
type dribbleReader struct {
	data []byte
	pos  int
}

func (d *dribbleReader) Read(p []byte) (int, error) {
	if d.pos >= len(d.data) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = d.data[d.pos]
	d.pos++
	return 1, nil
}

// A message must arrive whole however the transport chops it up. TCP may
// deliver five bytes as five reads, and a header assembled from a single Read
// would take the first byte for the whole length.
func TestReaderSurvivesAFragmentedStream(t *testing.T) {
	var stream []byte
	stream = append(stream, frame(pgwire.BackendRowDescription, string(rowDesc([]string{"id"}, []uint32{OIDInt8})))...)
	stream = append(stream, frame(pgwire.BackendDataRow, string(dataRow(AppendInt8(nil, 7))))...)
	stream = append(stream, frame(pgwire.BackendCommandComplete, "SELECT 1\x00")...)
	stream = append(stream, frame(pgwire.BackendReadyForQuery, "I")...)

	rd := pgwire.NewReader(&dribbleReader{data: stream})
	want := []byte{pgwire.BackendRowDescription, pgwire.BackendDataRow, pgwire.BackendCommandComplete, pgwire.BackendReadyForQuery}

	for i, wantType := range want {
		m, err := rd.Next()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if m.Type != wantType {
			t.Fatalf("message %d has type %q, want %q", i, m.Type, wantType)
		}
	}
	if _, err := rd.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("after the last message: %v, want io.EOF", err)
	}
}

// And the same through a whole query, so the failure would show as a broken
// conversation rather than as one odd message.
func TestQuerySurvivesAFragmentedStream(t *testing.T) {
	var stream []byte
	stream = append(stream, frame(pgwire.BackendParseComplete, "")...)
	stream = append(stream, frame(pgwire.BackendBindComplete, "")...)
	stream = append(stream, frame(pgwire.BackendRowDescription, string(rowDesc([]string{"id"}, []uint32{OIDInt8})))...)
	for i := range 3 {
		stream = append(stream, frame(pgwire.BackendDataRow, string(dataRow(AppendInt8(nil, int64(i)))))...)
	}
	stream = append(stream, frame(pgwire.BackendCommandComplete, "SELECT 3\x00")...)
	stream = append(stream, frame(pgwire.BackendReadyForQuery, "I")...)

	conn := NewConn(&dribbleTransport{dribbleReader{data: stream}})
	var got []int64
	src := conn.Query(t.Context(), "SELECT id", nil, QueryConfig{BatchRows: 8, AllRows: true})
	src.Stream()(func(b sluice.Batch[Row]) bool {
		for _, r := range b.Items {
			v, _ := r.Value(0)
			n, _ := DecodeInt8(v)
			got = append(got, n)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Errorf("read %v, want [0 1 2]", got)
	}
}

type dribbleTransport struct{ dribbleReader }

func (d *dribbleTransport) Write(p []byte) (int, error) { return len(p), nil }
func (d *dribbleTransport) Close() error                { return nil }
