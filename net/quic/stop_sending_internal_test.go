package quic

import (
	"testing"
	"time"
)

// A packet that carries only a STOP_SENDING reaches the batch callback: the
// frame used to be answered by the transport and dropped, so an application
// whose stream the peer refused to read — HTTP/3's control stream, a
// connection error under RFC 9114 §6.2.1 — learned of it only at the next
// datagram that carried stream data. A retransmission of the same frame is
// not handed up a second time.
func TestStopSendingAloneReachesTheCallback(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()

	// Application keys, so the RESET_STREAM that answers the STOP_SENDING
	// (RFC 9000 §3.5) has somewhere to go.
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = sealer
	c.mu.Unlock()

	var got [][]Frame
	c.OnStreamFrames(func(frames []Frame) error {
		got = append(got, cloneFrames(frames))
		return nil
	})
	s := c.Stream(0) // this client's first bidirectional stream

	payload := appendFrame(nil, FrameStopSending, 0, 0x10c)
	if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatalf("STOP_SENDING on a local stream: %v", err)
	}
	if len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("the callback saw %v, want one batch of one frame", got)
	}
	if f := got[0][0]; f.Type != FrameStopSending || f.StreamID != 0 {
		t.Fatalf("the callback saw type %#x on stream %d, want STOP_SENDING (%#x) on stream 0",
			f.Type, f.StreamID, FrameStopSending)
	}
	if code, reset := s.WasReset(); !reset || code != 0x10c {
		t.Fatalf("WasReset = (%#x, %v), want (0x10c, true)", code, reset)
	}

	if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatalf("the retransmitted STOP_SENDING: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("the retransmitted STOP_SENDING reached the callback again: %v", got)
	}
}
