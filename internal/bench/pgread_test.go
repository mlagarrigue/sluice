package bench

import (
	"encoding/binary"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// The connector's per-message read loop, which nothing measured until now.
//
// Every benchmark in this package until this one measured an operator: code
// that runs once per element inside a pipeline. This measures the loop that
// runs once per protocol message on the way in — Parse acknowledgements, the
// row description, then one DataRow per row — which is upstream of every
// operator and therefore in front of every figure the rest of this harness
// reports. A regression here does not show up in any of them.
//
// It is deliberately socket-free: the server is a canned conversation replayed
// out of memory, so what is timed is framing and decoding rather than the
// kernel. That makes it a poor model of a real query's latency and a good
// model of the only part this package can make faster or slower.

// replayServer answers with the same conversation over and over.
//
// Each Query consumes the script exactly — the response messages, then the
// ReadyForQuery its resync waits for — so the position lands back at zero on a
// message boundary and the next iteration reads a well-formed stream. A script
// that did not line up would desynchronise after one iteration and the
// benchmark would measure error handling.
type replayServer struct {
	script []byte
	pos    int
}

func (s *replayServer) Read(p []byte) (int, error) {
	if s.pos >= len(s.script) {
		s.pos = 0
	}
	n := copy(p, s.script[s.pos:])
	s.pos += n
	return n, nil
}

func (s *replayServer) Write(p []byte) (int, error) { return len(p), nil }
func (s *replayServer) Close() error                { return nil }

// frame wraps a body in the type byte and self-counting length the protocol
// puts in front of every message.
func frame(typ byte, body []byte) []byte {
	out := append([]byte{typ}, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(body)))
	return append(out, body...)
}

// pgScript builds one query's worth of backend traffic: two acknowledgements,
// a description of two int8 columns, rows rows, and the two messages that end
// the exchange.
func pgScript(rows int) []byte {
	var desc []byte
	desc = binary.BigEndian.AppendUint16(desc, 2)
	for _, name := range []string{"id", "total"} {
		desc = append(append(desc, name...), 0)
		desc = binary.BigEndian.AppendUint32(desc, 0)                // table OID
		desc = binary.BigEndian.AppendUint16(desc, 0)                // column number
		desc = binary.BigEndian.AppendUint32(desc, postgres.OIDInt8) // type
		desc = binary.BigEndian.AppendUint16(desc, 8)                // type size
		desc = binary.BigEndian.AppendUint32(desc, 0)                // type modifier
		desc = binary.BigEndian.AppendUint16(desc, uint16(pgwire.FormatBinary))
	}

	var row []byte
	row = binary.BigEndian.AppendUint16(row, 2)
	for _, v := range []int64{42, 1000} {
		row = binary.BigEndian.AppendUint32(row, 8)
		row = postgres.AppendInt8(row, v)
	}

	out := frame('1', nil) // ParseComplete
	out = append(out, frame('2', nil)...)
	out = append(out, frame('T', desc)...)
	for range rows {
		out = append(out, frame('D', row)...)
	}
	out = append(out, frame('C', append([]byte("SELECT"), 0))...)
	return append(out, frame('Z', []byte("I"))...)
}

// BenchmarkPGReadRows: the whole read path for a result of a thousand rows,
// reported per row so it sits on the same scale as every operator here.
func BenchmarkPGReadRows(b *testing.B) {
	const rows = 1000
	srv := &replayServer{script: pgScript(rows)}
	conn := postgres.NewConn(srv)

	var seen int
	for b.Loop() {
		src := conn.Query(b.Context(), "SELECT id, total FROM orders", nil,
			postgres.QueryConfig{BatchRows: rows})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			seen += len(batch.Items)
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatalf("the replayed conversation desynchronised: %v", err)
		}
	}
	sink = int64(seen)
	reportPerElem(b, rows)
}

// BenchmarkPGReadDecode: the same rows, with each value decoded — which is
// what a caller actually does, and what makes the framing cost above readable
// as a share of the whole.
func BenchmarkPGReadDecode(b *testing.B) {
	const rows = 1000
	srv := &replayServer{script: pgScript(rows)}
	conn := postgres.NewConn(srv)

	var acc int64
	for b.Loop() {
		src := conn.Query(b.Context(), "SELECT id, total FROM orders", nil,
			postgres.QueryConfig{BatchRows: rows})
		src.Stream()(func(batch sluice.Batch[postgres.Row]) bool {
			for _, r := range batch.Items {
				idBytes, _ := r.Value(0)
				id, _ := postgres.DecodeInt8(idBytes)
				totalBytes, isNull := r.Value(1)
				if !isNull {
					total, _ := postgres.DecodeInt8(totalBytes)
					acc += total
				}
				acc += id
			}
			return true
		})
		if err := src.Err(); err != nil {
			b.Fatalf("the replayed conversation desynchronised: %v", err)
		}
	}
	sink = acc
	reportPerElem(b, rows)
}

// Decoding an int8[] parameter's worth of keys, which is the other half of the
// `= ANY($1)` claim: a thousand keys go out as one parameter and a thousand
// values come back out of one array.
//
// It is measured per element because that is the scale the array is chosen
// for, and because the decoder walks a length prefix and eight bytes per
// element — the loop that gets refactored and the loop nobody watches.
func BenchmarkPGDecodeInt8Array(b *testing.B) {
	const n = 1000
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(i)
	}
	encoded := postgres.AppendInt8Array(nil, values)

	dst := make([]int64, 0, n)
	var acc int64
	for b.Loop() {
		var err error
		dst, err = postgres.DecodeInt8Array(dst, encoded)
		if err != nil {
			b.Fatal(err)
		}
		acc += dst[n-1]
	}
	sink = acc
	reportPerElem(b, n)
}

// And encoding it, which is what a batch of keys costs on the way out.
func BenchmarkPGEncodeInt8Array(b *testing.B) {
	const n = 1000
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(i)
	}

	buf := make([]byte, 0, 8*n+32)
	for b.Loop() {
		buf = postgres.AppendInt8Array(buf[:0], values)
	}
	sink = int64(len(buf))
	reportPerElem(b, n)
}
