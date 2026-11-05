//go:build linux || darwin || windows

package udp

import "testing"

// A control message with no data, and one that ends exactly where the
// buffer does, are read.
func TestNextCmsgAtItsEdges(t *testing.T) {
	oob, _ := appendCmsg(nil, 1, 2, 0)
	if level, typ, data, _, ok := nextCmsg(oob[:cmsgLen(0)]); !ok || level != 1 || typ != 2 || len(data) != 0 {
		t.Errorf("an empty message: %d %d % x %v", level, typ, data, ok)
	}
	oob, data := appendCmsg(nil, 1, 2, 4)
	copy(data, []byte{9, 8, 7, 6})
	if _, _, got, _, ok := nextCmsg(oob[:cmsgLen(4)]); !ok || len(got) != 4 {
		t.Errorf("a message ending with the buffer: % x %v", got, ok)
	}
}
