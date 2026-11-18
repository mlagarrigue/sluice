package httpstream

import (
	"strings"
	"testing"
)

// Once a request's head is decoded, the fragments it was assembled from have
// no further use: a stream kept in the connection's table while its body
// arrives holds none of their capacity, which can reach MaxHeaderBytes.
func TestH2HandedStreamLetsGoOfItsHeaderBlock(t *testing.T) {
	s := newBenchH2Server(Config{})
	block := appendHPACK(nil, ":method", "POST")
	block = appendHPACK(block, ":path", "/")
	block = appendHPACK(block, ":scheme", "https")
	block = appendHPACK(block, ":authority", "example.com")
	block = appendHPACK(block, "content-length", "10")
	block = appendHPACK(block, "x-pad", strings.Repeat("v", 20000))
	head := h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: block}
	ready, _, err := s.consume([]h2Frame{head})
	if err != nil || len(ready) != 1 {
		t.Fatalf("%d ready, %v", len(ready), err)
	}
	st := s.streams[1]
	if st == nil || !st.handed {
		t.Fatalf("stream 1 not handed over and kept: %+v", st)
	}
	if cap(st.block) != 0 {
		t.Errorf("a handed stream keeps %d bytes of header-block scratch for the life of its body", cap(st.block))
	}
}
