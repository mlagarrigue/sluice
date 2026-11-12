package quic

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// A grant at the limit already held gains nothing and keeps the BLOCKED
// sent at it; a receive window renews once less than half of it is left,
// not at half.
func TestQuotasAtTheirEdges(t *testing.T) {
	q := sendQuota{max: 100, blockedSent: true}
	if q.grant(100) || !q.blockedSent {
		t.Errorf("re-granting 100: blockedSent %v", q.blockedSent)
	}
	r := newRecvQuota(100)
	if r.consume(50) {
		t.Error("a grant with exactly half the window left")
	}
	if !r.consume(1) {
		t.Error("no grant with less than half left")
	}
}

// A socket that keeps failing is backed off, not spun on.
func TestReadLoopBacksOffAFailingSocket(t *testing.T) {
	const fails = 200
	script := make([]scriptedRead, fails)
	for i := range script {
		script[i] = scriptedRead{err: errors.New("a socket error")}
	}
	pc := newScriptedPC(script...)
	c := newConn(pc, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}, []byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
	c.closing.loop = true
	done := make(chan struct{})
	go func() { c.readLoop(); close(done) }()
	time.Sleep(100 * time.Millisecond)
	left := len(pc.reads)
	_ = c.Close()
	<-done
	if left < fails/2 {
		t.Errorf("%d of %d failing reads taken in 100 ms", fails-left, fails)
	}
}

// Bytes from one off-path address add up for its amplification budget.
func TestOffPathBytesAddUp(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()
	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 9), Port: 9}
	d := []byte{0x40, 1, 2, 3}
	c.admitSource(d, other)
	c.admitSource(d, other)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.offPath.recv != 8 {
		t.Errorf("offPath.recv = %d after two 4-byte datagrams, want 8", c.offPath.recv)
	}
}

func appOpener(t *testing.T) *packetOpener {
	t.Helper()
	s, err := initialSecrets([]byte("app keys"))
	if err != nil {
		t.Fatal(err)
	}
	op, err := newPacketOpener(s.Client)
	if err != nil {
		t.Fatal(err)
	}
	op.phase = &keyPhase{next: op.k}
	return op
}

// The previous phase's keys go when their time is up, on a 1-RTT packet;
// a level without keys holds sixteen packets for them.
func TestOpenPacketAtItsEdges(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	c.spaces[spaceApplication].opener = appOpener(t)
	c.ku.dropPrevAt = now.Add(-time.Second)
	c.mu.Unlock()
	_, _ = c.openPacket(&rxPacket{space: spaceApplication, raw: make([]byte, 40), pnOffset: 9}, now)
	c.mu.Lock()
	dropped := c.ku.dropPrevAt.IsZero()
	c.mu.Unlock()
	if !dropped {
		t.Error("the previous keys outlived their time")
	}

	for range 20 {
		_, _ = c.openPacket(&rxPacket{space: spaceHandshake, raw: make([]byte, 40), pnOffset: 9}, now)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.deferred[spaceHandshake]); n != 16 {
		t.Errorf("%d packets held for keys, want 16", n)
	}
}

// resetWith ends datagram with a token the peer announced for an
// identifier in use.
func resetWith(c *Conn, datagram []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy(c.peerCIDs[0].token[:], datagram[len(datagram)-16:])
	c.peerCIDs[0].used = true
}

// A stateless reset is 21 bytes at least, and is found behind an
// identifier of this end's that does not authenticate.
func TestStatelessResetAtItsEdges(t *testing.T) {
	c := newIdleServerConn(t)
	d := append([]byte{0x40}, bytes.Repeat([]byte{0xee}, 20)...)
	resetWith(c, d)
	if err := c.receive(d, c.peer); !errors.Is(err, ErrStatelessReset) {
		t.Errorf("a 21-byte reset: %v", err)
	}

	c = newIdleServerConn(t)
	d = append(append([]byte{0x40}, c.scid...), bytes.Repeat([]byte{0xdd}, 40)...)
	resetWith(c, d)
	c.mu.Lock()
	c.spaces[spaceApplication].opener = appOpener(t)
	c.mu.Unlock()
	if err := c.receive(d, c.peer); !errors.Is(err, ErrStatelessReset) {
		t.Errorf("a reset naming this end's identifier: %v", err)
	}
}

// The first 1-RTT packet drops the 0-RTT keys, and a marked packet is
// counted.
func TestProcessPacketDropsEarlyKeysAndCountsECN(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	c.earlyOpener = appOpener(t)
	c.rxECN = ecnECT0
	c.mu.Unlock()
	if err := c.processPacket(spaceApplication, false, 0, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.earlyOpener != nil || c.ecnReceived[ecnECT0] != 1 {
		t.Errorf("earlyOpener %v, ECT(0) received %d", c.earlyOpener, c.ecnReceived[ecnECT0])
	}
}

// An empty identifier is not a server's Retry identifier, and an empty
// token does not fit a Retry.
func TestEmptyIdentifierAndToken(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	c.odcid = []byte("odcid000")
	c.mu.Unlock()
	if c.expectedDCID(nil, spaceInitial) {
		t.Error("an empty identifier expected in Initial")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retryTokenFits(nil) {
		t.Error("an empty Retry token fits")
	}
}

// framesTo is a server connection whose batch callback records what it is
// handed.
func framesTo(t *testing.T) (*Conn, *[][]Frame) {
	t.Helper()
	c := boundsConn(t, false)
	var got [][]Frame
	c.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		got = append(got, cloneFrames(fs))
		return nil
	})
	return c, &got
}

func in(t *testing.T, c *Conn, payload []byte) {
	t.Helper()
	if err := c.framesIn(spaceApplication, false, payload, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// A repeated frame delivers nothing; a FIN ends the stream for Read, and
// only a FIN does.
func TestBatchDeliveryAtItsEdges(t *testing.T) {
	c, got := framesTo(t)
	in(t, c, streamFrame(nil, 0, 0, "abc", false))
	in(t, c, streamFrame(nil, 0, 0, "abc", false))
	if len(*got) != 1 {
		t.Errorf("%d batches for one frame and its repeat, want 1", len(*got))
	}
	s := c.streamIfOpen(0)
	s.mu.Lock()
	ended := s.readEnded
	s.mu.Unlock()
	if ended {
		t.Fatal("the stream ended for Read before its FIN")
	}
	in(t, c, streamFrame(nil, 0, 3, "", true))
	s.mu.Lock()
	ended = s.readEnded
	s.mu.Unlock()
	if !ended {
		t.Error("the FIN did not end the stream for Read")
	}
}

// Deltas held before a callback is registered are replayed batch by batch,
// at exactly twice the window as below it.
func TestHeldDeltasAtTheirBound(t *testing.T) {
	for _, data := range []string{"a", string(bytes.Repeat([]byte{'x'}, 100-pendingFrameCost))} {
		c := boundsConn(t, false)
		c.connRecv.window = 100
		in(t, c, streamFrame(nil, 0, 0, data, false))
		in(t, c, streamFrame(nil, 4, 0, data, false))
		calls := 0
		c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { calls++; return nil })
		if calls != 2 {
			t.Errorf("%d-byte deltas: %d batches replayed, want 2", len(data), calls)
		}
	}
}

// Only bytes below what was delivered mark an Initial CRYPTO frame stale,
// the delivered edge included.
func TestStaleCryptoAtTheEdge(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	c.spaces[spaceInitial].crypto.consumed = 5
	c.mu.Unlock()
	if err := c.handleCrypto(spaceInitial, Frame{offset: 5}); err != nil || c.rxStaleCrypto {
		t.Errorf("an empty frame at the edge: %v, stale %v", err, c.rxStaleCrypto)
	}
	if err := c.handleCrypto(spaceInitial, Frame{offset: 0, Data: []byte("abcde")}); err != nil || !c.rxStaleCrypto {
		t.Errorf("five bytes ending at the edge: %v, stale %v", err, c.rxStaleCrypto)
	}
}

// ackOf is an ACK frame acknowledging largest and the first below it.
func ackOf(t *testing.T, largest, first byte) Frame {
	t.Helper()
	f, _, err := parseFrame(ackFrame(largest, first))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// An ACK of the next number to send names a packet never sent.
func TestAckOfTheNextNumber(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	c.spaces[spaceApplication].nextPN = 3
	c.mu.Unlock()
	if err := c.handleAck(spaceApplication, ackOf(t, 3, 0), time.Now()); err == nil {
		t.Error("an ACK of packet 3 with 3 the next to send")
	}
}

// An ACK counts an application-limited resend only when it declared a loss
// from an idle connection.
func TestAckAppLimitedResend(t *testing.T) {
	for _, c := range []struct {
		name    string
		full    bool
		largest byte
		want    int
	}{
		{"idle, a loss", false, 3, 1},
		{"window full, a loss", true, 3, 0},
		{"idle, no loss", false, 0, 0},
	} {
		conn := newIdleServerConn(t)
		now := time.Now()
		conn.mu.Lock()
		fullWindow(conn, c.full)
		sp := &conn.spaces[spaceInitial]
		for pn := range uint64(4) {
			sp.sent.record(sentPacket{pn: pn, sentAt: now.Add(-time.Millisecond), size: 100, ackEliciting: true, payload: []byte{framePing}})
		}
		sp.nextPN = 4
		conn.mu.Unlock()
		if err := conn.handleAck(spaceInitial, ackOf(t, c.largest, 0), now); err != nil {
			t.Fatal(err)
		}
		conn.mu.Lock()
		got := conn.appLimitedResends
		conn.mu.Unlock()
		if got != c.want {
			t.Errorf("%s: %d application-limited resends, want %d", c.name, got, c.want)
		}
	}
}

// ackWithDelay is an ACK of packet 0 declaring delay.
func ackWithDelay(t *testing.T, delay uint64) Frame {
	t.Helper()
	b := varint.Append([]byte{frameACK, 0}, delay)
	f, _, err := parseFrame(append(b, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// A declared delay whose scaling overflows is huge, not what the
// overflow wrapped to.
func TestAckDelayOverflow(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	c.hasPeerParams = true
	c.peerParams.sawAckDelayExponent, c.peerParams.AckDelayExponent = true, 20
	c.rtt.sample(10*time.Millisecond, 0)
	sp := &c.spaces[spaceApplication]
	sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-2 * time.Second), size: 100, ackEliciting: true})
	sp.nextPN = 1
	c.mu.Unlock()
	if err := c.handleAck(spaceApplication, ackWithDelay(t, 1<<44+1), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if want := (7*10*time.Millisecond + 2*time.Second) / 8; c.rtt.smoothed != want {
		t.Errorf("smoothed = %v, want %v (the delay ignored as too large)", c.rtt.smoothed, want)
	}
}

// What an ACK sets, at its edges: the key phase is acknowledged by its
// first packet, progress needs something newly acknowledged, and so does
// the server's address validation.
func TestAckBookkeepingAtItsEdges(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	for pn := range uint64(3) {
		sp.sent.record(sentPacket{pn: pn, sentAt: now, size: 100, ackEliciting: true})
	}
	sp.nextPN, c.ku.firstPN = 3, 2
	c.mu.Unlock()
	if err := c.handleAck(spaceApplication, ackOf(t, 2, 0), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	acked, progress := c.ku.acked, c.rxProgress
	c.rxProgress = false
	c.mu.Unlock()
	if !acked || !progress {
		t.Errorf("an ACK of the phase's first packet: acked %v, progress %v", acked, progress)
	}
	if err := c.handleAck(spaceApplication, ackOf(t, 2, 0), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	progress = c.rxProgress
	hs := &c.spaces[spaceHandshake]
	hs.nextPN = 1
	c.mu.Unlock()
	if progress {
		t.Error("a repeated ACK counted as progress")
	}
	if err := c.handleAck(spaceHandshake, ackOf(t, 0, 0), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peerAddrValidated {
		t.Error("a Handshake ACK of nothing validated the address")
	}
}

// Losses exactly the persistent-congestion duration apart are not
// persistent congestion.
func TestPersistentCongestionAtItsDuration(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	t0 := now.Add(-2 * time.Second)
	c.mu.Lock()
	c.rtt.sample(100*time.Millisecond, 0)
	c.rtt.firstAt = t0.Add(-time.Second)
	// The ACK's own sample, 100 ms, leaves smoothed at 100 ms and the
	// variance at 37.5 ms: a PTO of 275 ms and a duration of 825 ms.
	span := 3 * (100*time.Millisecond + 4*37500*time.Microsecond + 25*time.Millisecond)
	sp := &c.spaces[spaceApplication]
	sp.sent.record(sentPacket{pn: 0, sentAt: t0, size: 100, ackEliciting: true, payload: []byte{framePing}})
	sp.sent.record(sentPacket{pn: 1, sentAt: t0.Add(span), size: 100, ackEliciting: true, payload: []byte{framePing}})
	sp.sent.record(sentPacket{pn: 5, sentAt: now.Add(-100 * time.Millisecond), size: 100, ackEliciting: true})
	sp.nextPN = 6
	c.mu.Unlock()
	if err := c.handleAck(spaceApplication, ackOf(t, 5, 0), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.persistentCongestions != 0 {
		t.Errorf("losses %v apart declared persistent congestion", span)
	}
}

// A frame on the last stream this end opened, now closed, is ignored; the
// peer's next stream is ahead of what it opened.
func TestStreamCountsAtTheirEdges(t *testing.T) {
	c := boundsConn(t, true)
	c.mu.Lock()
	c.openedBidi = 1
	c.mu.Unlock()
	if err := c.framesIn(spaceApplication, false, streamFrame(nil, 0, 0, "x", false), time.Now()); err != nil {
		t.Errorf("a frame on this end's closed stream 0: %v", err)
	}

	c = boundsConn(t, false)
	c.mu.Lock()
	c.seenPeerBidi = 1
	ahead, behind := c.pickedAheadLocked(4), c.pickedAheadLocked(0)
	c.mu.Unlock()
	if !ahead || behind {
		t.Errorf("with one peer stream seen: 4 ahead %v, 0 ahead %v", ahead, behind)
	}

	c = boundsConn(t, true)
	c.mu.Lock()
	c.openedBidi, c.peerMaxStreamsBidi = 1, 10
	c.mu.Unlock()
	s, err := c.Stream(0)
	if err != nil || !s.sendClosed {
		t.Errorf("Stream(0) after it closed: %v, closed %v", err, s != nil && s.sendClosed)
	}

	c = boundsConn(t, false)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.params.InitialMaxStreamsBidi = 80
	c.localMaxStreamsBidi, c.seenPeerBidi, c.retiredBidi, c.grants = 100, 90, 0, nil
	c.returnStreamSlotLocked(false)
	if len(c.grants) != 0 {
		t.Error("a MAX_STREAMS with exactly a batch of room left")
	}
}

// Batch consumption grants a stream more once half its window is spent,
// nothing before, and keeps its small scratch.
func TestBatchConsumptionGrants(t *testing.T) {
	c, _ := framesTo(t)
	in(t, c, streamFrame(nil, 0, 0, "a", false))
	c.mu.Lock()
	grants, scratch := len(c.grants), c.settleScratch
	c.mu.Unlock()
	if grants != 0 || scratch == nil {
		t.Errorf("one byte consumed: %d bytes of grants, scratch kept %v", grants, scratch != nil)
	}

	s := c.streamIfOpen(0)
	s.mu.Lock()
	s.recvQ = recvQuota{max: 11, window: 10, highest: 1, consumed: 1}
	s.mu.Unlock()
	in(t, c, streamFrame(nil, 0, 1, "bcdefg", false))
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.grants) == 0 || c.grants[0] != frameMaxStreamData {
		t.Errorf("half the stream window consumed: grants % x", c.grants)
	}
}

// Read marks the end taken with the last byte of a finished stream, not
// before.
func TestEndTakenWithTheLastByte(t *testing.T) {
	c := boundsConn(t, false)
	in(t, c, streamFrame(nil, 0, 0, "abcdef", true))
	s := c.streamIfOpen(0)
	buf := make([]byte, 3)
	for i, want := range []bool{false, true} {
		if _, err := s.Read(buf); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		got := s.endTaken
		s.mu.Unlock()
		if got != want {
			t.Errorf("after read %d: endTaken %v, want %v", i+1, got, want)
		}
	}
}

// A callback registered after Read took every byte is handed nothing.
func TestCallbackAfterReadTookEverything(t *testing.T) {
	c := boundsConn(t, false)
	in(t, c, streamFrame(nil, 0, 0, "abc", false))
	if _, err := io.ReadFull(c.streamIfOpen(0), make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { calls++; return nil })
	if calls != 0 {
		t.Errorf("%d batches replayed after Read took everything", calls)
	}
}

// Two acceptors waiting both get a stream when two arrive together.
func TestTwoAcceptorsBothServed(t *testing.T) {
	c := boundsConn(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := c.AcceptStream(ctx)
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	// Two streams queued behind one wake: the acceptor it wakes passes the
	// next one on.
	c.mu.Lock()
	for _, id := range []StreamID{0, 4} {
		s := newStream(c, id)
		s.queued = true
		c.acceptQ = append(c.acceptQ, s)
	}
	c.signalAccept()
	c.mu.Unlock()
	for range 2 {
		if err := <-errs; err != nil {
			t.Errorf("an acceptor: %v", err)
		}
	}
}

// Accepting a stream sends the grants queued for the peer.
func TestAcceptSendsQueuedGrants(t *testing.T) {
	client, server, _ := pmtuPair(t, 1500)
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		server.mu.Lock()
		queued := len(server.acceptQ)
		server.mu.Unlock()
		if queued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the stream never queued for Accept")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // the exchange's acknowledgements settle
	server.mu.Lock()
	server.grants = appendFrame(server.grants, frameMaxData, server.connRecv.max)
	server.mu.Unlock()
	if _, err := server.AcceptStream(t.Context()); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.grants) != 0 {
		t.Errorf("% x still queued after Accept", server.grants)
	}
}

// A RESET_STREAM that releases unread bytes without raising the flight
// gives the connection's window back.
func TestResetReleasesConnectionCredit(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	c.connRecv = newRecvQuota(100)
	c.mu.Unlock()
	in(t, c, streamFrame(nil, 0, 20, string(make([]byte, 40)), false)) // parked behind a gap
	in(t, c, appendFrame(nil, FrameResetStream, 0, 0, 60))
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.grants) == 0 || c.grants[0] != frameMaxData {
		t.Errorf("grants after the reset released 60 of 100: % x", c.grants)
	}
}

// A write leaves no writer counted behind it.
func TestWriteLeavesNoWriterCounted(t *testing.T) {
	client, _, _ := pmtuPair(t, 1500)
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if n := client.writersIn.Load(); n != 0 {
		t.Errorf("writersIn = %d after the write returned", n)
	}
}
