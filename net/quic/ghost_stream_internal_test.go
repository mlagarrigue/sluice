package quic

import (
	"errors"
	"testing"
	"time"
)

// streamFrame encodes one STREAM frame with its offset and length fields,
// and the FIN bit when asked.
func streamFrame(dst []byte, id, off uint64, data string, fin bool) []byte {
	typ := uint64(FrameStream | streamOFF | streamLEN)
	if fin {
		typ |= streamFIN
	}
	dst = AppendVarint(dst, typ)
	dst = AppendVarint(dst, id)
	dst = AppendVarint(dst, off)
	dst = AppendVarint(dst, uint64(len(data)))
	return append(dst, data...)
}

// lateFrames are the four frames a peer may still have in flight for a
// stream that closed at this end.
func lateFrames(id uint64) map[string][]byte {
	return map[string][]byte{
		"STREAM":          streamFrame(nil, id, 0, "req", true),
		"MAX_STREAM_DATA": appendFrame(nil, frameMaxStreamData, id, 1<<20),
		"STOP_SENDING":    appendFrame(nil, frameStopSending, id, 0),
		"RESET_STREAM":    appendFrame(nil, FrameResetStream, id, 0, 3),
	}
}

// tableSize returns how many streams the connection holds and the MAX_STREAMS
// credit it has granted the peer for bidirectional streams.
func tableSize(c *Conn) (live int, credit uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.streams), c.localMaxStreamsBidi
}

// A frame for a stream that closed must be ignored however many streams
// closed since. The memory of closed streams used to be the last 1024
// identifiers: past that, a late or forged STREAM frame on an older
// identifier opened the stream anew — the same HTTP/3 request handed to
// AcceptStream a second time, and a second MAX_STREAMS credit returned when
// it closed again.
func TestGhostStream(t *testing.T) {
	c, _ := newBenchConn(t)
	defer c.Close()

	const streams = 4096
	for i := range uint64(streams) {
		id := 4 * i // the peer's bidirectional streams, seen from this server
		// The peer's request with its FIN, then its STOP_SENDING: both
		// directions end in one packet and the stream leaves the table.
		payload := streamFrame(nil, id, 0, "req", true)
		payload = appendFrame(payload, frameStopSending, id, 0)
		if _, err := c.frames(spaceApplication, payload, time.Now()); err != nil {
			t.Fatalf("stream %d: %v", id, err)
		}
		if s, err := c.AcceptStream(t.Context()); err != nil || s.id != id {
			t.Fatalf("AcceptStream after stream %d: %v, %v", id, s, err)
		}
	}
	live, credit := tableSize(c)
	if live != 0 {
		t.Fatalf("%d streams still in the table after every one closed", live)
	}

	for _, id := range []uint64{0, 4, 4 * (streams - 1)} {
		for name, payload := range lateFrames(id) {
			if _, err := c.frames(spaceApplication, payload, time.Now()); err != nil {
				t.Fatalf("%s on closed stream %d: %v", name, id, err)
			}
		}
	}
	select {
	case s := <-c.accepted:
		t.Fatalf("stream %d was revived and queued for AcceptStream", s.id)
	default:
	}
	if live, after := tableSize(c); live != 0 || after != credit {
		t.Fatalf("after the late frames: %d streams in the table, MAX_STREAMS credit %d (was %d)", live, after, credit)
	}
	select {
	case <-c.closed:
		t.Fatal("the connection closed")
	default:
	}
}

// What makes the rule above sound: opening a stream out of order opens
// every lower-numbered stream of its kind with it (RFC 9000 §2.1), and they
// are registered — in identifier order — so a frame for one of them that
// arrives later finds its stream, not the closed-stream tolerance.
func TestOutOfOrderIDOpensTheLowerStreams(t *testing.T) {
	c, _ := newBenchConn(t)
	defer c.Close()

	if _, err := c.frames(spaceApplication, streamFrame(nil, 8, 0, "third", false), time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{0, 4, 8} {
		s, err := c.AcceptStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if s.id != want {
			t.Fatalf("AcceptStream gave stream %d, want %d", s.id, want)
		}
	}
	if _, err := c.frames(spaceApplication, streamFrame(nil, 0, 0, "first", false), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	s := c.streams[0]
	c.mu.Unlock()
	buf := make([]byte, 16)
	if n, err := s.Read(buf); err != nil || string(buf[:n]) != "first" {
		t.Fatalf("Read on stream 0: %q, %v", buf[:n], err)
	}
}

// On a stream this end opened and has since closed, a late frame from the
// peer is tolerated just the same — it used to be a fatal STREAM_STATE_ERROR
// once the identifier fell out of the closed set. STREAM_STATE_ERROR is for
// a locally initiated stream that was never opened (§19.8, §19.10).
func TestLateFrameOnOwnClosedStream(t *testing.T) {
	c, _ := newBenchConn(t)
	defer c.Close()
	const streams = 1100 // past the 1024 the closed set used to remember
	c.mu.Lock()
	c.peerMaxStreamsBidi = streams
	c.mu.Unlock()

	for i := range uint64(streams) {
		s, err := c.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if s.id != 4*i+1 {
			t.Fatalf("the server's bidirectional stream %d is %d, want %d", i, s.id, 4*i+1)
		}
		if err := s.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		if _, err := c.frames(spaceApplication, streamFrame(nil, s.id, 0, "", true), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if live, _ := tableSize(c); live != 0 {
		t.Fatalf("%d streams in the table after every one closed in both directions", live)
	}

	for name, payload := range lateFrames(1) {
		if _, err := c.frames(spaceApplication, payload, time.Now()); err != nil {
			t.Fatalf("%s on this end's closed stream 1: %v", name, err)
		}
	}
	if live, _ := tableSize(c); live != 0 {
		t.Fatalf("a late frame put %d streams back in the table", live)
	}

	never := uint64(4*streams + 1)
	_, err := c.frames(spaceApplication, appendFrame(nil, frameMaxStreamData, never, 1<<20), time.Now())
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportStreamState {
		t.Fatalf("MAX_STREAM_DATA on stream %d, never opened: %v, want STREAM_STATE_ERROR", never, err)
	}
}
