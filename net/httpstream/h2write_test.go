package httpstream

import (
	"bytes"
	"testing"
)

// A head that already spans HEADERS and CONTINUATION is reframed whole when a
// table size update is prefixed to it: the update leads the block, END_STREAM
// stays on HEADERS, END_HEADERS moves to whichever frame is now last, and the
// block decodes to what it did before.
func TestPrefixTableUpdateReframesAMultiFrameHead(t *testing.T) {
	block := appendHPACKStatus(nil, 200)
	block = appendHPACK(block, "x-large", bytes.Repeat([]byte("v"), 3*defaultMaxFrameSize))
	head, err := appendHeaderFrames(nil, 7, flagEndStream, block, defaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	w := &h2Writer{maxFrame: defaultMaxFrameSize, tableMin: 0, tableFinal: 2048}
	out, err := w.prefixTableUpdate(&h2Job{id: 7, head: head})
	if err != nil {
		t.Fatal(err)
	}

	var got []byte
	var frames []h2Frame
	for rest := out; len(rest) > 0; {
		n := int(rest[0])<<16 | int(rest[1])<<8 | int(rest[2])
		frames = append(frames, h2Frame{Type: rest[3], Flags: rest[4]})
		if n > defaultMaxFrameSize {
			t.Fatalf("a frame of %d bytes", n)
		}
		got = append(got, rest[frameHeaderSize:frameHeaderSize+n]...)
		rest = rest[frameHeaderSize+n:]
	}
	if frames[0].Type != frameHeaders || frames[0].Flags&flagEndStream == 0 {
		t.Errorf("the first frame is %#x flags %#x, want HEADERS with END_STREAM", frames[0].Type, frames[0].Flags)
	}
	for i, f := range frames {
		last := i == len(frames)-1
		if (f.Flags&flagEndHeaders != 0) != last {
			t.Errorf("frame %d of %d: END_HEADERS is %v", i, len(frames), !last)
		}
		if i > 0 && f.Type != frameContinuation {
			t.Errorf("frame %d is %#x, want CONTINUATION", i, f.Type)
		}
	}
	want := appendHPACKInt(appendHPACKInt(nil, 0, 5, 0x20), 2048, 5, 0x20)
	if !bytes.Equal(got, append(want, block...)) {
		t.Fatal("the reframed block is not the updates followed by the original block")
	}
	fields, err := newHPACKDecoder(4096, 1<<20).Decode(nil, got)
	if err != nil || len(fields) != 2 || string(fields[1].Name) != "x-large" {
		t.Fatalf("decoded %d fields, %v", len(fields), err)
	}
}
