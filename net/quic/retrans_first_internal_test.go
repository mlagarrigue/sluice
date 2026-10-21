package quic

import (
	"testing"
)

// RFC 9000 §13.3: lost data goes before new data. A writer that reaches the
// window ahead of the retransmission pass — woken by the acknowledgement
// that opened it — used to spend the window on its own bytes while the
// queue waited for the next packet received; now each packet of a write
// serves the queue first.
func TestAWriteSendsTheLostDataBeforeItsOwn(t *testing.T) {
	c := newResetReplayConn(t)
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	s.sendQ.max, c.connSend.max = 1<<20, 1<<20   // the peer's credit, as if granted
	sp.retrans = append(sp.retrans, lostPayload) // a declared loss, waiting to go again
	from := len(sp.sent.packets)
	c.mu.Unlock()

	if _, err := s.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	packets := sp.sent.packets[from:]
	if len(packets) < 2 {
		t.Fatalf("%d packets left for a write behind a lost payload, want the replay and the write", len(packets))
	}
	first, err := parseFrames(nil, packets[0].payload)
	if err != nil {
		t.Fatal(err)
	}
	var ping, replayed bool
	for _, f := range first {
		switch {
		case f.Type == framePing:
			ping = true
		case f.IsStream() && f.StreamID == 0 && f.offset == 0 && len(f.Data) == 3:
			replayed = true
		}
	}
	if !ping || !replayed {
		t.Fatalf("the first packet after the write carries ping=%v replayed stream data=%v, want the lost payload before the write's own bytes", ping, replayed)
	}
	if n := len(sp.retrans); n != 0 {
		t.Fatalf("%d payloads still queued for retransmission after the write", n)
	}
}
