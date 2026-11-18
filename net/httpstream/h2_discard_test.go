package httpstream

import (
	"strings"
	"testing"
)

// discardHeld is the bytes of decoded strings s.discard still reaches, over
// its whole capacity.
func discardHeld(s *h2Conn) int {
	held := 0
	for _, h := range s.discard[:cap(s.discard)] {
		held += cap(h.Name) + cap(h.Value)
	}
	return held
}

// A header block on a stream this end already closed is decoded into
// s.discard and dropped; the connection keeps nothing of it once consume
// returns. The slice was kept whole, and with it the fields and the arena the
// decoder allocated for their literals, until the next dropped block or the
// connection's end.
func TestH2ADroppedBlockLeavesNothingInTheDiscard(t *testing.T) {
	s := newBenchH2Server(Config{})
	head := h2HeadersFrame(1, "/")
	first := h2Frame{Type: head[3], Flags: head[4], StreamID: 1, Payload: head[9:]}
	if ready, _, err := s.consume([]h2Frame{first}); err != nil || len(ready) != 1 {
		t.Fatalf("open: %d ready, %v", len(ready), err)
	}
	rst := h2Frame{Type: frameRSTStream, StreamID: 1, Payload: []byte{0, 0, 0, 8}}
	if _, _, err := s.consume([]h2Frame{rst}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if cap(s.discard) != 0 {
		t.Fatalf("witness: s.discard holds %d fields before any dropped block", cap(s.discard))
	}
	var block []byte
	for i := range 3 {
		block = appendHPACK(block, "x-pad-"+string(rune('a'+i)), strings.Repeat("v", 8000))
	}
	late := h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: block}
	if _, _, err := s.consume([]h2Frame{late}); err != nil {
		t.Fatalf("late block: %v", err)
	}
	if held := discardHeld(s); held != 0 {
		t.Errorf("a dropped block's strings stay reachable from the connection: %d bytes", held)
	}
}

// The trailers of a stream already handed to its handler take the same road:
// decoded into s.discard, checked, copied to the body queue, and let go.
func TestH2HandedTrailersLeaveNothingInTheDiscard(t *testing.T) {
	s := newBenchH2Server(Config{})
	head := appendHPACK(nil, ":method", "POST")
	head = appendHPACK(head, ":path", "/")
	head = appendHPACK(head, ":scheme", "https")
	head = appendHPACK(head, ":authority", "example.com")
	head = appendHPACK(head, "content-length", "10")
	ready, _, err := s.consume([]h2Frame{{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: head}})
	if err != nil || len(ready) != 1 {
		t.Fatalf("open: %d ready, %v", len(ready), err)
	}
	if cap(s.discard) != 0 {
		t.Fatalf("witness: s.discard holds %d fields before the trailers", cap(s.discard))
	}
	var block []byte
	for i := range 3 {
		block = appendHPACK(block, "x-trailer-"+string(rune('a'+i)), strings.Repeat("v", 8000))
	}
	if _, _, err := s.consume([]h2Frame{{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: block}}); err != nil {
		t.Fatalf("trailers: %v", err)
	}
	if held := discardHeld(s); held != 0 {
		t.Errorf("a checked trailer section's strings stay reachable from the connection: %d bytes", held)
	}
}
