package quic

import (
	"errors"
	"os"
	"testing"
	"time"
)

// appConn is a server connection that seals 1-RTT packets (with its
// Initial keys) under a NewReno controller the test sizes.
func appConn(t *testing.T) (*Conn, *newReno) {
	t.Helper()
	c := newIdleServerConn(t)
	cc := newNewReno(1200)
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = c.spaces[spaceInitial].sealer
	c.handshakeConfirmed = true
	c.cc = cc
	c.mu.Unlock()
	return c, cc
}

// oweAck makes the application space owe an ACK whose delay field takes two
// bytes for the next hundred milliseconds, and returns its length. Callers
// hold mu.
func oweAck(c *Conn) int {
	sp := &c.spaces[spaceApplication]
	at := time.Now().Add(-10 * time.Millisecond)
	sp.ack.record(7, at)
	sp.ack.onAckEliciting(spaceApplication, at, true)
	return len(sp.ack.encodeAck(nil, time.Now()))
}

// sent sends payload in the application space and reports how many packet
// numbers it took.
func sent(t *testing.T, c *Conn, payload []byte) uint64 {
	t.Helper()
	sp := &c.spaces[spaceApplication]
	before := sp.nextPN
	if err := c.sendPacketLocked(spaceApplication, payload, sendOpts{}); err != nil {
		t.Fatal(err)
	}
	return sp.nextPN - before
}

// An ACK rides a payload that fills the datagram with it exactly; with no
// payload and no room left in the window, nothing leaves.
func TestAckBesidePayloadAtTheDatagramSize(t *testing.T) {
	c, cc := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	ack := oweAck(c)
	if n := sent(t, c, make([]byte, c.maxDatagram-c.packetOverhead(spaceApplication)-ack)); n != 1 {
		t.Errorf("a payload filling the datagram beside its ACK took %d packets", n)
	}

	oweAck(c)
	cc.inFlight = cc.cwnd - c.packetOverhead(spaceApplication)
	if n := sent(t, c, nil); n != 0 {
		t.Errorf("an empty send with the window short of the ACK took %d packets", n)
	}
}

// Queued grants join a packet only where they fit beside its ACK and
// payload; queued control frames only where the window takes them as well.
func TestQueuedFramesJoinWhereTheyFit(t *testing.T) {
	c, cc := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	ack := oweAck(c)
	grant := appendFrame(nil, frameMaxData, 1<<20)
	c.grants = append([]byte(nil), grant...)
	payload := make([]byte, c.maxDatagram-c.packetOverhead(spaceApplication)-ack-(len(grant)-1))
	sent(t, c, payload)
	if len(c.grants) != len(grant) {
		t.Errorf("a grant one byte too large for the room went out (%d bytes left queued)", len(c.grants))
	}

	c.grants = nil
	ack = oweAck(c)
	c.control = []byte{framePing}
	payload = make([]byte, 100)
	cc.inFlight = cc.cwnd - (ack + len(payload) + 1 + 64 - 1)
	sent(t, c, payload)
	if len(c.control) != 1 {
		t.Error("a control frame the window refuses beside the packet went out")
	}
}

// A paced writer waits for a window with room for its payload and 64
// bytes; woken to a window with room, a payload that fills the datagram
// exactly still fits.
func TestWindowWaitAtItsEdges(t *testing.T) {
	c, cc := appConn(t)
	c.mu.Lock()
	payload := make([]byte, 100)
	cc.inFlight = cc.cwnd - (len(payload) + 63)
	past := time.Now().Add(-time.Second)
	err := c.sendPacketLocked(spaceApplication, payload, sendOpts{waitCwnd: true, deadline: &past})
	c.mu.Unlock()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("a window 63 bytes past the payload: %v, want the deadline", err)
	}

	c, cc = appConn(t)
	c.mu.Lock()
	cc.inFlight = cc.cwnd
	go func() {
		time.Sleep(20 * time.Millisecond)
		c.mu.Lock()
		cc.inFlight = 0
		c.cond.Broadcast()
		c.mu.Unlock()
	}()
	err = c.sendPacketLocked(spaceApplication, make([]byte, c.maxDatagram-c.packetOverhead(spaceApplication)), sendOpts{waitCwnd: true})
	c.mu.Unlock()
	if err != nil {
		t.Errorf("a full-size payload woken to an open window: %v", err)
	}
}

// The ACK overflows the window when the packet fits without it and not
// with it.
func TestAckOverflowsTheWindowAtItsEdges(t *testing.T) {
	cc := newNewReno(1200)
	cc.cwnd, cc.inFlight = 12000, 11000
	c := &Conn{cc: cc}
	for _, tc := range []struct {
		size, ack int
		want      bool
	}{
		{1050, 100, true},
		{1000, 100, false}, // fits the 1000 left exactly
		{1100, 100, true},  // fits exactly without its ACK
	} {
		if got := c.ackOverflowsWindowLocked(tc.size, tc.ack); got != tc.want {
			t.Errorf("size %d, ack %d: %v, want %v", tc.size, tc.ack, got, tc.want)
		}
	}
}

// The queues are cut on frame boundaries, a frame that fills the room
// exactly included, and the identifiers counted are the ones that leave.
func TestCutFramesAtItsEdges(t *testing.T) {
	ncid := appendFrame(nil, frameNewConnectionID, 1, 0)
	ncid = append(ncid, 8)
	ncid = append(ncid, make([]byte, 8+16)...)
	q := append(append([]byte(nil), ncid...), framePing)
	if n, _, newCIDs := cutFrames(q, len(ncid)); n != len(ncid) || newCIDs != 1 {
		t.Errorf("cutFrames(room = first frame) = %d, %d NEW_CONNECTION_ID; want %d, 1", n, newCIDs, len(ncid))
	}
}

// takeQueuedLocked asks the window about control frames only when there
// are some, with their own size and the grants taken before them.
func TestTakeQueuedAsksTheWindow(t *testing.T) {
	cc := newNewReno(1200)
	cc.inFlight = cc.cwnd
	c := &Conn{cc: cc}
	c.takeQueuedLocked(100, 0)
	if cc.limited {
		t.Error("no control frames, and the window was asked")
	}

	cc = newNewReno(1200)
	c = &Conn{cc: cc, control: make([]byte, 10)}
	for i := range c.control {
		c.control[i] = framePing
	}
	cc.inFlight = cc.cwnd - (10 + 64 - 1)
	c.takeQueuedLocked(100, 0)
	if len(c.control) != 10 {
		t.Error("control frames past the window went out")
	}

	cc = newNewReno(1200)
	grant := appendFrame(nil, frameMaxData, 1<<20)
	c = &Conn{cc: cc, grants: append([]byte(nil), grant...), control: []byte{framePing}}
	cc.inFlight = cc.cwnd - (len(grant) + 1 + 64 - 1)
	c.takeQueuedLocked(100, 0)
	if len(c.control) != 1 || len(c.grants) != 0 {
		t.Errorf("grants %d, control %d left: the control frame went out past the window the grants used", len(c.grants), len(c.control))
	}
}

// The application space skips a number where the tracker says, and not
// within one of the 2^62-1 ceiling.
func TestTakePNSkipsAtItsEdges(t *testing.T) {
	for _, tc := range []struct {
		next, want uint64
	}{
		{5, 6},
		{1<<62 - 2, 1<<62 - 2},
	} {
		c := &Conn{maxDatagram: 1200, cc: newNewReno(1200)}
		sp := &c.spaces[spaceApplication]
		sp.sent.skipArmed, sp.sent.nextSkip = true, tc.next
		sp.nextPN = tc.next
		if got := c.takePNLocked(sp, spaceApplication); got != tc.want {
			t.Errorf("next %d: took %d, want %d", tc.next, got, tc.want)
		}
	}

	// A window of ten datagrams skips a number every five to fifteen.
	c := &Conn{maxDatagram: 1200, cc: newNewReno(1200)}
	sp := &c.spaces[spaceApplication]
	var last uint64
	for range 200 {
		last = c.takePNLocked(sp, spaceApplication)
	}
	if last < 210 {
		t.Errorf("200 packets took numbers up to %d: fewer than ten skipped", last)
	}
}

// A long-header packet's size on the wire, across the Length field's
// width.
func TestSealedLen(t *testing.T) {
	for _, tc := range []struct{ prefix, payload, pad, want int }{
		{10, 100, 5, 10 + 2 + 4 + 100 + 5 + 16},
		{0, 44, 0, 2 + 4 + 44 + 16}, // 64 bytes after the Length: two bytes of it
		{0, 40, 4, 2 + 4 + 40 + 4 + 16},
		{0, 43, 0, 1 + 4 + 43 + 16},
	} {
		if got := sealedLen(tc.prefix, tc.payload, tc.pad); got != tc.want {
			t.Errorf("sealedLen(%d, %d, %d) = %d, want %d", tc.prefix, tc.payload, tc.pad, got, tc.want)
		}
	}
}

// The CRYPTO room beside a gathered datagram, to the byte, down to 32.
func TestCryptoRoomAtItsEdges(t *testing.T) {
	const overhead = 1 + 4 + 1 + 8 + 1 + 8 + 1 + 2 + 4 + 16 + 1 + 8 + 4
	c := &Conn{gather: true, maxDatagram: 1200, dcid: make([]byte, 8), scid: make([]byte, 8)}
	if got := c.cryptoRoomLocked(); got != 0 {
		t.Errorf("nothing gathered: room %d", got)
	}
	c.dgram = make([]byte, 100)
	if got, want := c.cryptoRoomLocked(), 1200-100-overhead; got != want {
		t.Errorf("100 bytes gathered: room %d, want %d", got, want)
	}
	c.dgram = make([]byte, 1200-overhead-32)
	if got := c.cryptoRoomLocked(); got != 32 {
		t.Errorf("32 bytes of room: %d", got)
	}
	c.dgram = make([]byte, 1200-overhead-31)
	if got := c.cryptoRoomLocked(); got != 0 {
		t.Errorf("31 bytes of room: %d", got)
	}
}

// A lone writer's 0-RTT packet leaves at once rather than waiting gathered.
func TestLoneEarlyWriterIsNotGathered(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := assembleConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.earlySealer = c.spaces[spaceInitial].sealer
	c.writersIn.Store(1)
	if err := c.writePacketLocked(&c.spaces[spaceApplication], spaceApplication, []byte{framePing}, nil, sendOpts{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.dgramPend != nil || len(c.dgram) != 0 {
		t.Error("a lone writer's 0-RTT packet stayed gathered")
	}
}
