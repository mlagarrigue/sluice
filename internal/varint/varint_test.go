package varint

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

// RFC 9000 Appendix A.1's examples, read in the text: each sequence decodes
// to its value, the two-byte 0x4025 too — the long form of 37, which a
// decoder must accept — and Append writes the shortest form.
func TestReadTheRFCExamples(t *testing.T) {
	for _, tc := range []struct {
		hex      string
		v        uint64
		shortest bool
	}{
		{"c2197c5eff14e88c", 151_288_809_941_952_652, true},
		{"9d7f3e7d", 494_878_333, true},
		{"7bbd", 15_293, true},
		{"25", 37, true},
		{"4025", 37, false},
	} {
		b, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatal(err)
		}
		v, rest, err := Read(append(b, 0xff))
		if err != nil || v != tc.v || !bytes.Equal(rest, []byte{0xff}) {
			t.Errorf("Read(%s) = %d, rest %x, %v; want %d and the byte after it", tc.hex, v, rest, err, tc.v)
		}
		if got := Append(nil, tc.v); tc.shortest && !bytes.Equal(got, b) {
			t.Errorf("Append(%d) = %x, want %s", tc.v, got, tc.hex)
		}
	}
}

// Each width's largest value and the next one, written and read back: the
// boundaries between one, two, four and eight bytes, and Max.
func TestAppendAtTheWidthBoundaries(t *testing.T) {
	for _, tc := range []struct {
		v     uint64
		width int
	}{
		{0, 1},
		{63, 1},
		{64, 2},
		{16383, 2},
		{16384, 4},
		{1<<30 - 1, 4},
		{1 << 30, 8},
		{Max, 8},
	} {
		b := Append(nil, tc.v)
		if len(b) != tc.width {
			t.Errorf("Append(%d) is %d bytes, want %d", tc.v, len(b), tc.width)
		}
		v, rest, err := Read(b)
		if err != nil || v != tc.v || len(rest) != 0 {
			t.Errorf("Read(Append(%d)) = %d, %d left, %v", tc.v, v, len(rest), err)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("Append(Max+1) did not panic")
		}
	}()
	Append(nil, Max+1)
}

// A buffer that stops inside the integer its first byte announces is
// ErrTruncated, at every width, the empty buffer included.
func TestReadRefusesATruncatedInteger(t *testing.T) {
	for _, b := range [][]byte{{}, {0x40}, {0x80, 0, 0}, {0xc0, 0, 0, 0, 0, 0, 0}} {
		if _, _, err := Read(b); !errors.Is(err, ErrTruncated) {
			t.Errorf("Read(%x) = %v, want ErrTruncated", b, err)
		}
	}
}
