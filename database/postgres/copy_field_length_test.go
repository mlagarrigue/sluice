package postgres

import (
	"encoding/binary"
	"testing"
)

// A field longer than its four-byte length can say is written with a length
// the server refuses, not with its length wrapped: a COPY batch is cut into
// as many CopyData messages as it needs, so nothing else stops a field past
// 4 GiB, whose wrapped length would have the server read its tail as the
// rows after it. The bound is lowered here; a 4 GiB field is not built.
func TestATooLongFieldIsWrittenWithARefusedLength(t *testing.T) {
	defer func(n int) { maxFieldBytes = n }(maxFieldBytes)
	maxFieldBytes = 8
	length := func(b []byte) uint32 { return binary.BigEndian.Uint32(b[:4]) }

	if got := length(AppendField(nil, make([]byte, 8))); got != 8 {
		t.Errorf("a field at the bound: length %d, want 8", got)
	}
	if got := length(AppendField(nil, make([]byte, 9))); got != refusedFieldLength {
		t.Errorf("AppendField past the bound: length %#x, want %#x", got, refusedFieldLength)
	}
	buf, at := BeginField(nil)
	buf = EndField(append(buf, make([]byte, 9)...), at)
	if got := length(buf); got != refusedFieldLength {
		t.Errorf("EndField past the bound: length %#x, want %#x", got, refusedFieldLength)
	}
	if n := int32(binary.BigEndian.Uint32(binary.BigEndian.AppendUint32(nil, refusedFieldLength))); n >= -1 { //nolint:gosec // G115: the bits as the server reads them
		t.Errorf("the refused length reads as %d: not below NULL's -1", n)
	}
}
