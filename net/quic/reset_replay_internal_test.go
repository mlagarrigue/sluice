package quic

import (
	"testing"
	"time"
)

// A client connection with 1-RTT keys to seal with, no peer reading: what
// the tests inspect is the record of what was sent.
func newResetReplayConn(t *testing.T) *Conn {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
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
	c.peerMaxStreamsBidi = 1
	c.mu.Unlock()
	return c
}

// framesOfStream counts, in the packets recorded from index from on, the
// STREAM and STREAM_DATA_BLOCKED frames of stream id, and the RESET_STREAM
// frames of it. Callers hold mu.
func framesOfStream(t *testing.T, sp *space, from int, id uint64) (streams, blocked, resets int) {
	t.Helper()
	for _, p := range sp.sent.packets[from:] {
		fs, err := parseFrames(nil, p.payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fs {
			if f.StreamID != id {
				continue
			}
			switch {
			case f.IsStream():
				streams++
			case f.Type == frameStreamDataBlocked:
				blocked++
			case f.Type == FrameResetStream:
				resets++
			}
		}
	}
	return
}

// lostPayload is what a lost packet of stream 0 leaves to replay: a STREAM
// frame (offset 0, three bytes), a STREAM_DATA_BLOCKED for it, and a PING
// that owes nothing to the stream.
var lostPayload = []byte{
	0x0a, 0x00, 0x03, 'a', 'b', 'c', // STREAM, stream 0, LEN
	frameStreamDataBlocked, 0x00, 0x0a, // STREAM_DATA_BLOCKED, stream 0, limit 10
	framePing,
}

// RFC 9000 §3.3: after RESET_STREAM, the sender MUST NOT send STREAM or
// STREAM_DATA_BLOCKED frames for the stream; §19.4: it ceases transmission
// and retransmission of STREAM frames. A lost payload replayed after the
// reset — by the retransmission pass, or lifted by the probe — goes without
// the stream's frames and with the rest, and so does a payload whose stream
// has already been retired from the connection's table.
func TestResetStreamReplaysNoStreamFrames(t *testing.T) {
	for _, tc := range []struct {
		name   string
		replay func(c *Conn, sp *space)
	}{
		{"retransmit", func(c *Conn, _ *space) { c.retransmitLocked() }},
		{"probe", func(c *Conn, _ *space) { c.sendProbeLocked(spaceApplication) }},
		{"retired", func(c *Conn, _ *space) { delete(c.streams, 0); c.retransmitLocked() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newResetReplayConn(t)
			s, err := c.Stream(0)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Reset(7); err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			sp := &c.spaces[spaceApplication]
			sp.retrans = append(sp.retrans, append([]byte(nil), lostPayload...))
			// The record starts empty: the probe would otherwise lift the
			// RESET_STREAM packet too, and shift what is inspected.
			sp.sent.packets = nil
			tc.replay(c, sp)
			if len(sp.sent.packets) == 0 {
				t.Fatal("nothing replayed at all")
			}
			streams, blocked, _ := framesOfStream(t, sp, 0, 0)
			if streams != 0 || blocked != 0 {
				t.Fatalf("replayed %d STREAM and %d STREAM_DATA_BLOCKED frames of a reset stream", streams, blocked)
			}
			rest := false
			for _, p := range sp.sent.packets {
				rest = rest || string(p.payload) == string([]byte{framePing})
			}
			if !rest {
				t.Fatal("the rest of the payload, a PING, did not go as it was")
			}
			if len(sp.retrans) != 0 {
				t.Fatalf("%d payloads still queued after the replay", len(sp.retrans))
			}
		})
	}
}

// The RESET_STREAM itself, when it is what was lost, goes again: the filter
// drops the stream's data, not the frame that ends it.
func TestResetStreamReplaysItsOwnReset(t *testing.T) {
	c := newResetReplayConn(t)
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(7); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	reset := sp.sent.packets[len(sp.sent.packets)-1].payload
	sp.retrans = append(sp.retrans, append([]byte(nil), reset...))
	from := len(sp.sent.packets)
	c.retransmitLocked()
	if _, _, resets := framesOfStream(t, sp, from, 0); resets != 1 {
		t.Fatalf("the lost RESET_STREAM was replayed %d times", resets)
	}
}

// The reset is forgotten once nothing sent before it is in flight and the
// queue is empty: the record does not grow with the resets of a connection.
func TestResetStreamRecordIsPrunedOnceNothingPredatesIt(t *testing.T) {
	c := newResetReplayConn(t)
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	// A packet in flight from before the reset: packet 0, the reset at 1.
	c.mu.Lock()
	err = c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{})
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(7); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if mark, ok := c.resetIDs[0]; !ok || mark != 1 {
		t.Fatalf("the reset was recorded at mark %d, %v", mark, ok)
	}
	sp := &c.spaces[spaceApplication]
	c.retransmitLocked()
	if _, ok := c.resetIDs[0]; !ok {
		t.Fatal("the reset was forgotten with a packet older than it in flight")
	}
	sp.sent.packets = sp.sent.packets[1:] // packet 0 acknowledged
	c.retransmitLocked()
	if _, ok := c.resetIDs[0]; ok {
		t.Fatal("the reset was kept with nothing older than it in flight")
	}
}

// §3.3 for the timer half of §4.1: a stream parked at its flow-control
// limit and then reset re-announces no STREAM_DATA_BLOCKED.
func TestResetStreamResendsNoBlockedFrame(t *testing.T) {
	c := newResetReplayConn(t)
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	s.sendQ.max, s.sendQ.sent, s.sendQ.blockedSent = 10, 10, true
	c.mu.Unlock()
	if err := s.Reset(7); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.streams[0]; !ok {
		t.Fatal("the stream left the table before its receiving side ended")
	}
	sp := &c.spaces[spaceApplication]
	from := len(sp.sent.packets)
	c.resendBlockedFramesLocked(time.Now())
	if _, blocked, _ := framesOfStream(t, sp, from, 0); blocked != 0 {
		t.Fatalf("resent %d STREAM_DATA_BLOCKED for a reset stream", blocked)
	}
}
