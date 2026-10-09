package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// Rows.Append is where the connector spends most of its time and takes most of
// its input: one call per row, on bytes the far end chose.
//
// The framing layer bounds how long a DataRow may be and says nothing about
// what is in it, so everything below — the field count, each field's declared
// length, the NULL marker — is attacker-controlled. It has golden vectors and
// hand-written cases; it has never been shown arbitrary bytes.
//
// `FuzzRowsAppend` next door already checks that every value of an accepted
// row is *reachable* — that the offsets describe the buffer they index. This
// checks something else: that the row **reproduces itself**. A decoder can
// store perfectly reachable values and still have dropped a field or read one
// short, and reachability says nothing about that.
//
// The property is that an accepted row reproduces itself. A decoder that reads
// a prefix it finds agreeable and ignores the rest is the defect this has
// already found twice elsewhere in the package, and the accumulator is the one
// place where getting it wrong misaligns every value after it rather than
// spoiling one.
func FuzzRowsAppendReproducesTheRow(f *testing.F) {
	f.Add(dataRow(AppendInt8(nil, 1), AppendInt8(nil, 2)))
	f.Add(dataRow(AppendInt8(nil, 1), nil)) // a NULL, which is not an empty value
	f.Add(dataRow(nil, nil))
	f.Add(dataRow([]byte{}, []byte{})) // empty values, which are not NULLs
	f.Add([]byte{0, 2})
	f.Add([]byte{0, 2, 0, 0, 0, 1})
	f.Add([]byte{255, 255})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) < 2 {
			return
		}
		fields := int(binary.BigEndian.Uint16(body))
		// A width the accumulator was not built for is refused by design, and
		// that refusal is tested elsewhere; here the accumulator is built to
		// match so the field parsing is what gets exercised.
		if fields > 64 {
			return
		}

		r := NewRows(fields, 4)
		if err := r.Append(body); err != nil {
			if !errors.Is(err, ErrProtocol) && !errors.Is(err, ErrTooManyRows) {
				t.Fatalf("unclassified error %v for %x", err, body)
			}
			return
		}
		if r.Len() != 1 {
			t.Fatalf("Append(%x) accepted and holds %d rows", body, r.Len())
		}

		// Rebuild the row from what was stored. Anything the accumulator
		// dropped or invented shows up here, and nowhere else: a value read
		// short leaves the rest of the buffer unreachable rather than wrong,
		// which is exactly the kind of loss no assertion about one field
		// catches.
		var back []byte
		back = binary.BigEndian.AppendUint16(back, uint16(fields)) //nolint:gosec // G115: bounded above
		for i := range fields {
			v, isNull := r.Value(0, i)
			if isNull {
				back = binary.BigEndian.AppendUint32(back, ^uint32(0)) // -1
				continue
			}
			back = binary.BigEndian.AppendUint32(back, uint32(len(v))) //nolint:gosec // G115: bounded by the input
			back = append(back, v...)
		}
		if !bytes.Equal(back, body) {
			t.Fatalf("Append(%x) stored a row that rebuilds as %x", body, back)
		}
	})
}

// The same for a whole batch, because the accumulator packs every field of
// every row into one buffer with an offset table beside it — so a row that
// stores correctly on its own can still land at the wrong offset once there is
// something before it.
func FuzzRowsAppendBatchReproducesEachRow(f *testing.F) {
	f.Add(dataRow(AppendInt8(nil, 1)), dataRow(AppendInt8(nil, 2)))
	f.Add(dataRow(nil), dataRow([]byte{}))
	f.Add(dataRow([]byte("a")), dataRow(nil))
	f.Add([]byte{0, 1}, []byte{0, 1})

	f.Fuzz(func(t *testing.T, first, second []byte) {
		if len(first) < 2 || len(second) < 2 {
			return
		}
		fields := int(binary.BigEndian.Uint16(first))
		if fields > 16 || int(binary.BigEndian.Uint16(second)) != fields {
			return
		}

		r := NewRows(fields, 4)
		if err := r.Append(first); err != nil {
			return
		}
		if err := r.Append(second); err != nil {
			return
		}
		if r.Len() != 2 {
			t.Fatalf("two rows appended, %d held", r.Len())
		}

		// The first row must still read as itself with the second behind it.
		for row, body := range [][]byte{first, second} {
			var back []byte
			back = binary.BigEndian.AppendUint16(back, uint16(fields)) //nolint:gosec // G115: bounded above
			for i := range fields {
				v, isNull := r.Value(row, i)
				if isNull {
					back = binary.BigEndian.AppendUint32(back, ^uint32(0))
					continue
				}
				back = binary.BigEndian.AppendUint32(back, uint32(len(v))) //nolint:gosec // G115: bounded by the input
				back = append(back, v...)
			}
			if !bytes.Equal(back, body) {
				t.Fatalf("row %d was %x and rebuilds as %x", row, body, back)
			}
		}

		// And Reset must leave nothing readable behind: the byte buffer is
		// reused without clearing, so a stale tail must be unreachable rather
		// than merely unlikely.
		r.Reset()
		if r.Len() != 0 {
			t.Fatalf("Reset left %d rows", r.Len())
		}
	})
}
