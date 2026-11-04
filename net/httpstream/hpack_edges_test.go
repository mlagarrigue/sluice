package httpstream

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// The decoded list is bounded at its exact size, whichever representation
// brings each field in: an index into the static table, a literal the table
// does not keep, a literal it does and an index to that one.
func TestHPACKListBoundIsExact(t *testing.T) {
	const field = 2 + 4 + hpackEntrySize // "ab: cdef"
	tests := []struct {
		name  string
		size  int
		block []byte
	}{
		{"static index", 7 + 3 + hpackEntrySize, []byte{0x82, 0x82}},
		{"literal without indexing", field, append(hpackLiteral(0x00, "ab", "cdef"), hpackLiteral(0x00, "ab", "cdef")...)},
		{"literal with indexing", field, append(hpackLiteral(0x40, "ab", "cdef"), hpackLiteral(0x40, "ab", "cdef")...)},
		{"dynamic index", field, append(hpackLiteral(0x40, "ab", "cdef"), 0xbe)},
	}
	for _, tt := range tests {
		if fields, err := newHPACKDecoder(0, 2*tt.size).Decode(nil, tt.block); err != nil || len(fields) != 2 {
			t.Errorf("%s at the bound: %d fields, %v", tt.name, len(fields), err)
		}
		if _, err := newHPACKDecoder(0, 2*tt.size-1).Decode(nil, tt.block); !errors.Is(err, ErrTooLarge) {
			t.Errorf("%s one byte over: %v, want ErrTooLarge", tt.name, err)
		}
	}
}

// Past the list bound, the last static index still resolves as one.
func TestHPACKLastStaticIndexPastTheBound(t *testing.T) {
	_, err := newHPACKDecoder(0, 40).Decode(nil, []byte{0x82, 0x80 | byte(len(staticTable))})
	if !errors.Is(err, ErrTooLarge) || errors.Is(err, ErrHPACK) {
		t.Fatalf("Decode = %v, want the list bound and no HPACK fault", err)
	}
}

func TestHPACKIndexPastTheTablesSaysHowManyItHolds(t *testing.T) {
	_, err := newHPACKDecoder(0, 0).Decode(nil, []byte{0xbe})
	if !errors.Is(err, ErrHPACK) || !strings.Contains(err.Error(), "the table holds 61 entries") {
		t.Fatalf("Decode = %v, want the 61 entries named", err)
	}
}

// An entry exactly the table's size is kept.
func TestHPACKEntryOfTheTablesSize(t *testing.T) {
	d := newHPACKDecoder(2+4+hpackEntrySize, 0)
	if _, err := d.Decode(nil, hpackLiteral(0x40, "ab", "cdef")); err != nil {
		t.Fatal(err)
	}
	fields, err := d.Decode(nil, []byte{0xbe})
	if err != nil || len(fields) != 1 || string(fields[0].Value) != "cdef" {
		t.Fatalf("the entry of the table's size: %v, %v", fields, err)
	}
}

func TestCloneHeadersCopiesEveryByte(t *testing.T) {
	in := []Header{{Name: []byte("a"), Value: []byte("bcd")}, {Name: []byte("efg"), Value: nil}}
	out := cloneHeaders(in)
	if len(out) != 2 || string(out[0].Name) != "a" || string(out[0].Value) != "bcd" || string(out[1].Name) != "efg" || len(out[1].Value) != 0 {
		t.Fatalf("cloneHeaders = %q", out)
	}
	in[0].Value[0] = 'x'
	if string(out[0].Value) != "bcd" {
		t.Error("the clone shares the original's bytes")
	}
}

// RFC 7541 §5.1: a remainder of exactly 128 takes two continuation octets.
func TestHPACKIntegerOfAContinuationBoundary(t *testing.T) {
	if got := appendHPACKInt(nil, 31+128, 5, 0); !bytes.Equal(got, []byte{0x1f, 0x80, 0x01}) {
		t.Errorf("appendHPACKInt(159, 5) = %x, want 1f8001", got)
	}
	if got := appendHPACKInt(nil, 31+127, 5, 0); !bytes.Equal(got, []byte{0x1f, 0x7f}) {
		t.Errorf("appendHPACKInt(158, 5) = %x, want 1f7f", got)
	}
}

func TestLowerASCIIAtTheLettersEdges(t *testing.T) {
	for s, want := range map[string]bool{"A": false, "Z": false, "@": true, "[": true, "az": true} {
		if got := lowerASCII(s); got != want {
			t.Errorf("lowerASCII(%q) = %v, want %v", s, got, want)
		}
	}
	var scratch []byte
	if got := lowerName([]byte("AZ@[az"), &scratch); string(got) != "az@[az" {
		t.Errorf("lowerName = %q, want %q", got, "az@[az")
	}
}

// Eight bits of padding are a code the encoder did not emit, even all ones.
func TestHuffmanRefusesAWholeOctetOfPadding(t *testing.T) {
	if _, err := huffmanDecode(nil, []byte{0xff}); !errors.Is(err, ErrHPACK) {
		t.Errorf("huffmanDecode(ff) = %v, want ErrHPACK", err)
	}
}

func TestEndsStreamOnlyOnDataAndHeaders(t *testing.T) {
	for _, tt := range []struct {
		typ  byte
		want bool
	}{
		{frameData, true},
		{frameHeaders, true},
		{frameSettings, false},
		{framePing, false},
	} {
		if got := (h2Frame{Type: tt.typ, Flags: flagEndStream}).EndsStream(); got != tt.want {
			t.Errorf("EndsStream of type %#x = %v, want %v", tt.typ, got, tt.want)
		}
	}
}

// The frame reader at its bounds: frames per batch, a frame of
// MaxFrameSize, CONTINUATION frames per block, a header block's bytes.
func TestFrameReaderAtItsBounds(t *testing.T) {
	ping := frame(framePing, 0, 0, make([]byte, 8))
	batches, err := readFrames(t, append(prefaced(), bytes.Repeat(ping, 3)...), Config{MaxFramesPerBatch: 2})
	if err != nil || len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 2 {
		t.Errorf("four frames two per batch: %d batches, %v", len(batches), err)
	}

	cfg := Config{}
	big := frame(frameData, 0, 1, make([]byte, cfg.mustResolve().MaxFrameSize))
	if _, err := readFrames(t, append(prefaced(), big...), cfg); err != nil {
		t.Errorf("a frame of MaxFrameSize: %v", err)
	}

	cfg = Config{MaxContinuationFrames: 2, MaxHeaderBytes: 32}
	open := frame(frameHeaders, 0, 1, make([]byte, 16))
	cont := frame(frameContinuation, 0, 1, nil)
	last := frame(frameContinuation, flagEndHeaders, 1, nil)
	tests := []struct {
		name   string
		frames [][]byte
		ok     bool
	}{
		{"CONTINUATION frames of the bound", [][]byte{open, cont, last}, true},
		{"one CONTINUATION over", [][]byte{open, cont, cont, last}, false},
		{"a HEADERS of the bound", [][]byte{frame(frameHeaders, flagEndHeaders, 1, make([]byte, 32))}, true},
		{"a HEADERS over", [][]byte{frame(frameHeaders, flagEndHeaders, 1, make([]byte, 33))}, false},
		{"a block of the bound", [][]byte{open, frame(frameContinuation, flagEndHeaders, 1, make([]byte, 16))}, true},
		{"a block over", [][]byte{open, frame(frameContinuation, flagEndHeaders, 1, make([]byte, 17))}, false},
	}
	for _, tt := range tests {
		_, err := readFrames(t, append(prefaced(), bytes.Join(tt.frames, nil)...), cfg)
		if tt.ok != (err == nil) {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
}

// A preface is judged once its 24 bytes are in, the last one included.
func TestFramesRefusesAPrefaceWrongInItsLastByte(t *testing.T) {
	bad := []byte(Preface)
	bad[len(bad)-1] = 'X'
	if _, err := readFrames(t, bad, Config{}); !errors.Is(err, ErrH2Protocol) {
		t.Errorf("readFrames = %v, want ErrH2Protocol", err)
	}
}

func TestRSTStreamOnStreamZero(t *testing.T) {
	for _, f := range [][]byte{frame(frameRSTStream, 0, 0, make([]byte, 4)), frame(frameRSTStream, 0, 1, make([]byte, 3))} {
		if _, err := readFrames(t, append(prefaced(), f...), Config{}); !errors.Is(err, ErrH2Protocol) {
			t.Errorf("RST_STREAM %x: %v, want ErrH2Protocol", f[:9], err)
		}
	}
}

func TestFramePayloadAtTheLengthFieldsBound(t *testing.T) {
	if _, err := appendH2Frame(nil, h2Frame{Type: frameData, StreamID: 1, Payload: make([]byte, 1<<24-1)}); err != nil {
		t.Errorf("a payload of 2^24-1 bytes: %v", err)
	}
	if _, err := appendH2Frame(nil, h2Frame{Type: frameData, StreamID: 1, Payload: make([]byte, 1<<24)}); !errors.Is(err, ErrH2Protocol) {
		t.Errorf("a payload of 2^24 bytes: %v, want ErrH2Protocol", err)
	}
}

// deadlineConn drips a byte per read and records every read deadline.
type deadlineConn struct {
	memConn
	deadlines []time.Time
}

func (c *deadlineConn) Read(p []byte) (int, error) { return c.memConn.Read(p[:1]) }
func (c *deadlineConn) SetReadDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	return nil
}

// Once part of a frame has arrived, the reads that complete it all wait
// for the one deadline its first byte set.
func TestAFrameDrippedKeepsItsDeadline(t *testing.T) {
	c := &deadlineConn{memConn: memConn{in: []byte(Preface)}}
	src := h2Frames(c, Config{}.mustResolve())
	src.Stream()(func(b sluice.Batch[h2Frame]) bool { return true })
	if len(c.deadlines) < len(Preface) {
		t.Fatalf("%d deadlines for %d reads", len(c.deadlines), len(Preface))
	}
	held := c.deadlines[1 : len(Preface)-1]
	for i, d := range held {
		if !d.Equal(held[0]) {
			t.Fatalf("read %d of the preface waited until %v, the first until %v", i+2, d, held[0])
		}
	}
}
