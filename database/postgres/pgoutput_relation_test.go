package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// replDecoder.relation reads a column count, then loops that many times. It
// did not look at r.err in the loop, and replReader.cstring walked everything
// that remained when it found no NUL, consuming nothing, while the other
// reads of one turn consumed nine bytes: a Relation whose first column name
// does not end was scanned whole once per declared column. The test counts
// turns, not time: a loop that stops at its first error leaves all but nine
// bytes of the body, one that does not leaves the body minus nine bytes per
// declared column.
func TestARelationStopsAtItsFirstFailingColumn(t *testing.T) {
	const columns = 1000
	body := func(tail []byte) []byte {
		b := binary.BigEndian.AppendUint32(nil, 1) // OID
		b = append(b, 'n', 0, 't', 0, 'd')
		b = binary.BigEndian.AppendUint16(b, columns)
		return append(b, tail...)
	}

	// Control: a well-formed Relation of as many columns is read to its end.
	var whole []byte
	for range columns {
		whole = append(whole, 0, 'c', 0)
		whole = binary.BigEndian.AppendUint32(whole, 20)
		whole = binary.BigEndian.AppendUint32(whole, 0xFFFFFFFF)
	}
	var d replDecoder
	r := replReader{b: body(whole)}
	d.relation(&r)
	if r.err != nil || len(r.b) != 0 {
		t.Fatalf("control: well-formed Relation: err %v, %d bytes left", r.err, len(r.b))
	}

	// The defect: no NUL anywhere after the column count, as many bytes as
	// the count check admits (ten per column).
	tail := bytes.Repeat([]byte{1}, 10*columns)
	r = replReader{b: body(tail)}
	d = replDecoder{}
	d.relation(&r)
	if !errors.Is(r.err, ErrProtocol) {
		t.Fatalf("err = %v, want ErrProtocol", r.err)
	}
	left := len(r.b)
	if left == len(tail)-9*columns {
		t.Errorf("relation ran all %d turns after the first column name failed (%d of %d bytes left): "+
			"each turn's cstring walked the whole remainder", columns, left, len(tail))
	}
	if left < len(tail)-9 {
		t.Errorf("%d bytes left, want at least %d: the loop went on after r.err was set", left, len(tail)-9)
	}
}
