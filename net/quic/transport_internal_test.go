package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Regression tests for the transport layer: each fails against the
// behaviour it documents.

// newIdleServerConn is a server-side Conn with Initial keys and no
// amplification limit — a bare transport for driving the timer and send
// paths directly.
func newIdleServerConn(t *testing.T) *Conn {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	c := assembleConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.amplActive = false
	c.mu.Unlock()
	return c
}

// RFC 9002 A.9: pto_count increases once per PTO expiry. A pass that finds
// the Initial and Handshake probe timeouts both matured is one expiry — it
// used to count two, doubling the next interval twice over.
func TestPTOCountsOncePerExpiryNotPerSpace(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	c.spaces[spaceHandshake].sealer = c.spaces[spaceInitial].sealer
	for _, space := range []int{spaceInitial, spaceHandshake} {
		sp := &c.spaces[space]
		sp.lastElicit = now.Add(-time.Minute)
		sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Minute), size: 100, ackEliciting: true, payload: []byte{framePing}})
		sp.nextPN = 1
	}
	c.mu.Unlock()

	c.actOnDeadlines(now)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ptoCount != 1 {
		t.Errorf("ptoCount = %d after one expiry covering two spaces, want 1", c.ptoCount)
	}
	// A probe is two datagrams (§6.2.4), here two copies of the one payload.
	for _, space := range []int{spaceInitial, spaceHandshake} {
		if c.spaces[space].nextPN != 3 {
			t.Errorf("space %d: nextPN = %d, want a two-packet probe sent (3)", space, c.spaces[space].nextPN)
		}
	}
}

// dialAgainst dials a scripted server: the first datagram the client sends
// is parsed and handed to answer, which writes whatever reply the test is
// about. It returns how long Dial took and its error.
func dialAgainst(t *testing.T, answer func(srv net.PacketConn, from net.Addr, h longHeader)) (time.Duration, error) {
	t.Helper()
	cli, srv := newLoopbackPair(t)
	go func() {
		buf := make([]byte, 2048)
		_ = srv.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, from, err := srv.ReadFrom(buf)
		if err != nil {
			return
		}
		h, _, err := parseLongHeader(buf[:n])
		if err != nil {
			return
		}
		answer(srv, from, h)
	}()
	start := time.Now()
	c, err := Dial(context.Background(), cli, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if c != nil {
		_ = c.Close()
	}
	return time.Since(start), err
}

// A Version Negotiation during the handshake closes the connection — and
// Dial used to keep reading regardless, re-arming the read deadline Close
// had expired, then report the 10-second handshake timeout instead.
func TestDialReportsVersionNegotiationPromptly(t *testing.T) {
	took, err := dialAgainst(t, func(srv net.PacketConn, from net.Addr, h longHeader) {
		vn := []byte{0x80 | 0x40, 0, 0, 0, 0}
		vn = append(vn, byte(len(h.SCID)))
		vn = append(vn, h.SCID...)
		vn = append(vn, byte(len(h.DCID)))
		vn = append(vn, h.DCID...)
		vn = append(vn, 0x1a, 0x2a, 0x3a, 0x4a) // a reserved version, not 1
		_, _ = srv.WriteTo(vn, from)
	})
	if err == nil || !strings.Contains(err.Error(), "version 1") {
		t.Fatalf("Dial = %v, want the version-negotiation failure", err)
	}
	if took > 3*time.Second {
		t.Errorf("Dial took %v to report it; the handshake timeout was waited out", took)
	}
}

// A server's CONNECTION_CLOSE in an Initial — a listener's stateless
// CONNECTION_REFUSED is exactly this — reaches the dialer as the peer's
// close, now, not as an i/o timeout ten seconds later.
func TestDialReportsAHandshakeCloseAsItself(t *testing.T) {
	took, err := dialAgainst(t, func(srv net.PacketConn, from net.Addr, h longHeader) {
		s, err := initialSecrets(h.DCID)
		if err != nil {
			return
		}
		sealer, err := newPacketSealer(s.Server)
		if err != nil {
			return
		}
		frame := AppendVarint(nil, frameConnectionClose)
		frame = AppendVarint(frame, transportConnectionRefused)
		frame = AppendVarint(frame, 0)
		frame = AppendVarint(frame, 0)
		const pnLen = 4
		header := []byte{0xc0 | packetInitial<<4 | pnLen - 1, 0, 0, 0, 1}
		header = append(header, byte(len(h.SCID)))
		header = append(header, h.SCID...)
		header = append(header, byte(len(h.DCID)))
		header = append(header, h.DCID...)
		header = AppendVarint(header, 0)
		header = AppendVarint(header, uint64(pnLen+len(frame)+16))
		pnOffset := len(header)
		header = append(header, 0, 0, 0, 0)
		pkt, err := sealer.Seal(nil, header, frame, 0, pnOffset, pnLen)
		if err != nil {
			return
		}
		_, _ = srv.WriteTo(pkt, from)
	})
	if !isRefusedClose(err) {
		t.Fatalf("Dial = %v, want the peer's CONNECTION_REFUSED", err)
	}
	if took > 3*time.Second {
		t.Errorf("Dial took %v to report it; the handshake timeout was waited out", took)
	}
}

// scriptedPC is a net.PacketConn whose reads follow a script, then block
// until a read deadline already in the past is set — what Close's
// wakeOwnedReadLoop does.
type scriptedPC struct {
	writes atomic.Int64
	reads  chan scriptedRead
	wake   chan struct{}
	local  net.Addr
}

type scriptedRead struct {
	data []byte
	from net.Addr
	err  error
}

func newScriptedPC(script ...scriptedRead) *scriptedPC {
	p := &scriptedPC{
		reads: make(chan scriptedRead, len(script)),
		wake:  make(chan struct{}, 1),
		local: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
	}
	for _, r := range script {
		p.reads <- r
	}
	return p
}

func (p *scriptedPC) ReadFrom(b []byte) (int, net.Addr, error) {
	select {
	case r := <-p.reads:
		return copy(b, r.data), r.from, r.err
	default:
	}
	select {
	case r := <-p.reads:
		return copy(b, r.data), r.from, r.err
	case <-p.wake:
		return 0, nil, os.ErrDeadlineExceeded
	}
}

func (p *scriptedPC) SetReadDeadline(t time.Time) error {
	if !t.IsZero() && !t.After(time.Now()) {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (p *scriptedPC) WriteTo(b []byte, _ net.Addr) (int, error) {
	p.writes.Add(1)
	return len(b), nil
}
func (p *scriptedPC) Close() error                     { return nil }
func (p *scriptedPC) LocalAddr() net.Addr              { return p.local }
func (p *scriptedPC) SetDeadline(time.Time) error      { return nil }
func (p *scriptedPC) SetWriteDeadline(time.Time) error { return nil }

// A ReadFrom error that is not the socket closing — WSAECONNRESET from a
// forged ICMP, on Windows — used to end an established connection. It is
// now counted and read past: the datagram after it is still processed.
func TestReadLoopSurvivesATransientReadError(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	pc := newScriptedPC(
		scriptedRead{err: errors.New("wsarecvfrom: An existing connection was forcibly closed by the remote host.")},
		scriptedRead{data: []byte{0x40, 1, 2, 3}, from: peer}, // undecryptable: counted and dropped
	)
	c := newConn(pc, peer, []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	c.tls = tls.QUICClient(&tls.QUICConfig{TLSConfig: ClientTLSForTest()})
	loopDone := make(chan struct{})
	go func() { c.readLoop(); close(loopDone) }()

	deadline := time.Now().Add(5 * time.Second)
	for c.Stats().DiscardedPackets < 2 {
		select {
		case <-c.Done():
			t.Fatalf("a transient read error closed the connection: %v", c.Err())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the loop never read past the error (discarded %d)", c.Stats().DiscardedPackets)
		}
		runtime.Gosched()
	}
	select {
	case <-c.Done():
		t.Fatalf("a transient read error closed the connection: %v", c.Err())
	default:
	}

	_ = c.Close()
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the read loop did not exit on Close")
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err = %v after a clean close", err)
	}
}

// A noRetrans packet withheld by the anti-amplification budget is skipped
// outright — and the grants and control frames it had picked up from the
// queues went with it, silently: a MAX_DATA the peer was blocked on, never
// sent and never retransmitted.
func TestWithheldNoRetransPacketKeepsQueuedFrames(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spaces[spaceApplication].sealer = c.spaces[spaceInitial].sealer
	c.amplActive, c.amplRecv, c.amplSent = true, 0, 0 // nothing may leave
	grant := appendFrame(nil, frameMaxData, 1<<20)
	ctl := appendFrame(nil, frameDataBlocked, 1<<10)
	c.grants = append([]byte(nil), grant...)
	c.control = append([]byte(nil), ctl...)

	if err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{noRetrans: true}); err != nil {
		t.Fatal(err)
	}
	if got := c.spaces[spaceApplication].nextPN; got != 0 {
		t.Fatalf("nextPN = %d: the packet left despite a zero budget", got)
	}
	var queued strings.Builder
	queued.WriteString(string(c.grants) + string(c.control))
	for _, sp := range c.spaces[spaceApplication].retrans {
		queued.WriteString(string(sp))
	}
	if !strings.Contains(queued.String(), string(grant)) || !strings.Contains(queued.String(), string(ctl)) {
		t.Errorf("the withheld packet's grant and control frames are queued nowhere (grants %x, control %x, retrans %x)",
			c.grants, c.control, c.spaces[spaceApplication].retrans)
	}
}

// isRefusedClose reports whether err is the peer's CONNECTION_CLOSE with
// CONNECTION_REFUSED, read by its code rather than its text.
func isRefusedClose(err error) bool {
	ce, ok := errors.AsType[*ConnectionError](err)
	return ok && !ce.Application && ce.Code == transportConnectionRefused
}

// The idle timeout firing: the connection ends, silently — no
// CONNECTION_CLOSE (§10.1) — and Err names the cause, ErrIdleTimeout. The
// existing idle tests only pinned when it does not fire.
func TestIdleTimeoutExpiresSilently(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	pc := newScriptedPC()
	c := assembleConn(pc, peer, []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c.mu.Lock()
	c.idleTimeout = 10 * time.Millisecond
	c.idleDeadline = now.Add(-time.Minute) // past even the 3-PTO floor
	c.mu.Unlock()
	before := pc.writes.Load()

	if next := c.actOnDeadlines(now); !next.IsZero() {
		t.Errorf("an expired connection still schedules a deadline at %v", next)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("the idle timeout matured and the connection is still open")
	}
	if err := c.Err(); !errors.Is(err, ErrIdleTimeout) {
		t.Errorf("Err = %v, want ErrIdleTimeout", err)
	}
	if n := pc.writes.Load() - before; n != 0 {
		t.Errorf("%d datagrams written on idle expiry; it must send nothing", n)
	}
}

// The loss timer firing on its own, with no ACK to trigger it: the packet
// an earlier ACK left too young to declare is declared once the timer
// matures, resent, and the timer re-arms on the survivor. The earlier ACK
// covered packet 2, which makes 0 and 1 candidates (RFC 9002 §6.1: sent
// before an acknowledged packet).
func TestLossTimerDeclaresWithoutAnAck(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	sp := &c.spaces[spaceInitial]
	old := []byte{framePing, 0xaa}
	sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Minute), size: 100, ackEliciting: true, payload: old})
	sp.sent.record(sentPacket{pn: 1, sentAt: now, size: 100, ackEliciting: true, payload: []byte{framePing, 0xbb}})
	sp.sent.largestAcked, sp.sent.hasLargestAcked = 2, true
	sp.nextPN = 3
	sp.lastElicit = now
	sp.sent.lossTime = now.Add(-time.Millisecond)
	c.mu.Unlock()

	c.actOnDeadlines(now)

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range sp.sent.packets {
		if p.pn == 0 {
			t.Fatal("packet 0 is still in flight after its loss timer matured")
		}
	}
	if sp.nextPN != 4 {
		t.Errorf("nextPN = %d, want the lost payload resent as packet 3", sp.nextPN)
	}
	resent := false
	for _, p := range sp.sent.packets {
		if p.pn == 3 && bytes.Contains(p.payload, old) {
			resent = true
		}
	}
	if !resent {
		t.Error("the lost payload was not resent")
	}
	if sp.sent.lossTime.IsZero() {
		t.Error("the loss timer did not re-arm on packet 1, still too young to call")
	}
	if c.ptoCount != 0 {
		t.Errorf("ptoCount = %d; a loss-timer firing is not a probe timeout", c.ptoCount)
	}
}

// A probe with lost payloads queued sends from the head of that queue — two
// of them (RFC 9002 §6.2.4 allows two datagrams), the third waits — each
// recorded for recovery like any packet, since it is the only copy.
func TestProbeSendsTheRetransmissionQueueFirst(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceInitial]
	first, second, third := []byte{framePing, 1}, []byte{framePing, 2}, []byte{framePing, 3}
	sp.retrans = [][]byte{first, second, third}

	c.sendProbeLocked(spaceInitial)

	if len(sp.retrans) != 1 || !bytes.Equal(sp.retrans[0], third) {
		t.Fatalf("retrans = %x, want only the third payload left", sp.retrans)
	}
	if sp.nextPN != 2 {
		t.Fatalf("nextPN = %d, want two probes sent", sp.nextPN)
	}
	if len(sp.sent.packets) != 2 || !bytes.Contains(sp.sent.packets[0].payload, first) || !bytes.Contains(sp.sent.packets[1].payload, second) {
		t.Errorf("the probes' payloads are not tracked for recovery: %+v", sp.sent.packets)
	}
}

// A probe with nothing queued lifts the payloads of the oldest unacknowledged
// packets and resends them as new packets. The originals stay in the flight
// record, now without a payload, so that their own acknowledgement or loss
// retires them and the next timeout moves on to what still has a payload
// instead of copying the same packet again (liftOldestResendable). The
// bytes in flight grow by the two probes (RFC 9002 §7.5): a timeout is not
// a loss, and the probes are additional load.
func TestProbeLiftsTheOldestPacketsIntoNewOnes(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceInitial]
	now := time.Now()
	for pn := range uint64(3) {
		sp.sent.record(sentPacket{pn: pn, sentAt: now, size: 100, ackEliciting: true, payload: []byte{framePing, byte(pn)}})
	}
	sp.nextPN = 3
	c.reno().inFlight = 300

	c.sendProbeLocked(spaceInitial)

	if sp.nextPN != 5 {
		t.Fatalf("nextPN = %d, want two probes sent", sp.nextPN)
	}
	var pns []uint64
	for _, p := range sp.sent.packets {
		pns = append(pns, p.pn)
	}
	if len(pns) != 5 || pns[0] != 0 || pns[1] != 1 || pns[2] != 2 || pns[3] != 3 || pns[4] != 4 {
		t.Fatalf("packets tracked = %v, want the originals 0, 1 and 2 still in flight and the copies 3 and 4", pns)
	}
	if len(sp.sent.packets[0].payload) != 0 || len(sp.sent.packets[1].payload) != 0 {
		t.Errorf("the lifted packets kept their payloads, which the next timeout would copy again: %+v", sp.sent.packets[:2])
	}
	if !bytes.Contains(sp.sent.packets[3].payload, []byte{framePing, 0}) || !bytes.Contains(sp.sent.packets[4].payload, []byte{framePing, 1}) {
		t.Errorf("the copies do not carry the lifted payloads: %+v", sp.sent.packets[3:])
	}
	// The 300 bytes of the three originals, still in flight, plus the two
	// probes at their sealed size: RFC 9002 §7.5, "additionally in flight".
	want := 300 + sp.sent.packets[3].size + sp.sent.packets[4].size
	if c.reno().inFlight != want {
		t.Errorf("inFlight = %d, want %d: the probes are counted on top of what they resend", c.reno().inFlight, want)
	}
}

// RFC 9000 §13.2.1: a packet sent for any reason SHOULD carry an ACK frame
// if one has not been sent recently. In the handshake spaces every packet
// does once anything was received there: the ACK already paid for may have
// been lost, and a probe is the only packet the peer will see for a PTO.
func TestAHandshakeProbeCarriesTheAckAlreadySent(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("probeack:self")}
	c := assembleConn(pc, fakeFuzzAddr("probeack:peer"), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	sec, err := initialSecrets([]byte("dcid0000"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(sec.Server)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	c.mu.Lock()
	c.amplActive = false
	sp := &c.spaces[spaceInitial]
	sp.ack.record(4, now)
	sp.ack.onAckEliciting(spaceInitial, now, false)
	sp.ack.settle() // the ACK of packet 4 already left, and was lost
	sp.sent.record(sentPacket{pn: 0, sentAt: now, size: 100, ackEliciting: true, payload: []byte{framePing}})
	sp.nextPN = 1
	c.sendProbeLocked(spaceInitial)
	c.mu.Unlock()

	writes := pc.recorded()
	if len(writes) == 0 {
		t.Fatal("the probe sent nothing")
	}
	for i, w := range writes {
		h, _, err := parseLongHeader(w.data)
		if err != nil {
			t.Fatal(err)
		}
		payload, _, err := opener.Open(h.Raw, h.PNOffset, 0)
		if err != nil {
			t.Fatalf("probe %d does not open: %v", i, err)
		}
		frames, err := parseFrames(nil, payload)
		if err != nil {
			t.Fatal(err)
		}
		acked := false
		for _, f := range frames {
			acked = acked || f.Type == frameACK || f.Type == frameACK|1
		}
		if !acked {
			t.Errorf("probe %d carries no ACK: the peer learns nothing of packet 4 until another of its packets is answered", i)
		}
	}
}

// A certificate chain the anti-amplification budget holds back leaves in
// offset order once the budget lifts. Each withheld packet goes back to the
// head of the queue; a chain sent chunk by chunk came out reversed, its
// first packet thousands of bytes past what the client held contiguous —
// past the 4096 out-of-order bytes RFC 9000 §7.5 obliges a peer to buffer.
func TestAWithheldCertificateChainLeavesInOrder(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	c.spaces[spaceHandshake].sealer = c.spaces[spaceInitial].sealer
	c.amplActive = true
	c.amplRecv = 1200 // one client Initial's worth: 3600 bytes may leave
	c.mu.Unlock()

	if err := c.sendCrypto(tls.QUICEncryptionLevelHandshake, make([]byte, 10000)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.amplRecv = 1 << 20 // the client's next datagrams validate nothing yet but raise the budget
	c.retransmitLocked()

	sp := &c.spaces[spaceHandshake]
	var offsets []uint64
	for _, p := range sp.sent.packets {
		frames, err := parseFrames(nil, p.payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range frames {
			if f.Type == frameCrypto {
				offsets = append(offsets, f.offset)
			}
		}
	}
	if len(offsets) < 5 || len(sp.retrans) != 0 {
		t.Fatalf("sent CRYPTO at %v with %d payloads still queued; want the whole chain sent", offsets, len(sp.retrans))
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			t.Fatalf("CRYPTO left at offsets %v: out of order once the budget lifted", offsets)
		}
	}
}

// RFC 9002 §6.2.3: a server that receives Initial CRYPTO bytes it already
// took learns that its own Initial answer did not arrive, and resends it
// before the probe timeout — twice per connection at most, so that a client
// whose every packet repeats itself cannot make each one draw a flight.
func TestARepeatedClientHelloDrawsAnEarlyResend(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	sp := &c.spaces[spaceInitial]
	sp.crypto.consumed = 100 // the ClientHello's bytes, taken
	sp.sent.record(sentPacket{pn: 0, sentAt: time.Now(), size: 1200, ackEliciting: true, payload: appendFrame(nil, frameCrypto, 0, 0)})
	sp.nextPN = 1
	c.mu.Unlock()

	stale := append(appendFrame(nil, frameCrypto, 0, 50), make([]byte, 50)...)
	for i, want := range []int{1, 2, 2} {
		pkt := sealedInitialCarrying(t, false, []byte("dcid0000"), []byte("scid0000"), []byte("dcid0000"), uint64(i), stale, true)
		c.mu.Lock()
		before := sp.nextPN
		c.mu.Unlock()
		if err := c.receive(pkt, c.peer); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		got, sent := c.earlyResends, sp.nextPN-before
		c.mu.Unlock()
		if got != want {
			t.Fatalf("after repeat %d: %d early resends, want %d", i+1, got, want)
		}
		if want > i && sent < 2 {
			t.Errorf("repeat %d sent %d packets, want the two probe datagrams", i+1, sent)
		}
		if want <= i && sent != 1 {
			t.Errorf("repeat %d past the limit sent %d packets, want the ACK alone", i+1, sent)
		}
	}
}

// Manual credit: delivery to the batch consumer earns the peer nothing, and
// only ReleaseStreamBytes reopens the window — at both levels.
func TestManualCreditGrantsOnlyOnRelease(t *testing.T) {
	params := DefaultParameters()
	params.InitialMaxData = 1000
	params.InitialMaxStreamDataBidiRemote = 1000
	c := newConn(&fuzzSink{addr: "manual:self"}, fakeFuzzAddr("manual:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, params)
	defer c.Close()
	held := 0
	c.OnStreamFrames(CreditOnRelease, func(fs []Frame) error {
		for _, f := range fs {
			held += len(f.Data)
		}
		return nil
	})

	chunk := make([]byte, 100)
	var off uint64
	send := func() error {
		err := c.frames(spaceApplication, streamFrameBytes(0, off, chunk), time.Now())
		if err == nil {
			off += uint64(len(chunk))
		}
		return err
	}
	for range 10 {
		if err := send(); err != nil {
			t.Fatalf("data within the window refused: %v", err)
		}
	}
	c.mu.Lock()
	grants := len(c.grants)
	c.mu.Unlock()
	if held != 1000 || grants != 0 {
		t.Fatalf("held %d bytes, %d grant bytes queued; delivery must credit nothing", held, grants)
	}

	c.ReleaseStreamBytes(0, 1000)
	c.mu.Lock()
	queued := c.grants
	c.mu.Unlock()
	frames, err := parseFrames(nil, queued)
	if err != nil {
		t.Fatal(err)
	}
	var sawConn, sawStream bool
	for _, f := range frames {
		switch f.Type {
		case frameMaxData:
			sawConn = f.value > 1000
		case frameMaxStreamData:
			sawStream = f.StreamID == 0 && f.value > 1000
		}
	}
	if !sawConn || !sawStream {
		t.Errorf("release queued %+v; want MAX_DATA and MAX_STREAM_DATA past 1000", frames)
	}
	if err := send(); err != nil {
		t.Errorf("data past the original window refused after release: %v", err)
	}
}

// BenchmarkSameAddr is the per-datagram sender check: every received
// datagram is compared against the peer's address. Comparing String()
// renderings allocated six times per call.
func BenchmarkSameAddr(b *testing.B) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4433}
	p := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4433}
	b.ReportAllocs()
	for b.Loop() {
		if !sameAddr(a, p) {
			b.Fatal("equal addresses compared unequal")
		}
	}
}

// BenchmarkReadLoopFed is a Listener-managed connection's per-datagram
// receive loop, with the datagram itself refused cheaply (it does not
// authenticate): what is left is the loop's own overhead per round.
func BenchmarkReadLoopFed(b *testing.B) {
	peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4433}
	c := newConn(&fuzzSink{addr: "bench:self"}, peer, []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	c.tls = tls.QUICServer(&tls.QUICConfig{TLSConfig: &tls.Config{}})
	c.incoming = make(chan rawDatagram)
	done := make(chan struct{})
	go func() { c.readLoopFed(); close(done) }()
	d := rawDatagram{data: []byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, from: peer}
	b.ReportAllocs()
	for b.Loop() {
		c.incoming <- d
	}
	b.StopTimer()
	_ = c.Close()
	<-done
}

// sameAddr's allocation-free UDP path must keep the string comparison's
// answers: an IPv4 address equals its IPv4-mapped form, ports and
// addresses both count, and other address types still compare.
func TestSameAddrMatchesTheStringComparison(t *testing.T) {
	v4 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}
	mapped := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443} // 16-byte form
	cases := []struct {
		a, b net.Addr
		want bool
	}{
		{v4, mapped, true},
		{v4, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 444}, false},
		{v4, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 443}, false},
		{&net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1}, &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1}, true},
		{&net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: "eth0"}, &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 1, Zone: "eth1"}, false},
		{fakeFuzzAddr("x"), fakeFuzzAddr("x"), true},
		{fakeFuzzAddr("x"), v4, false},
		{nil, nil, true},
		{v4, nil, false},
	}
	for i, tc := range cases {
		if got := sameAddr(tc.a, tc.b); got != tc.want {
			t.Errorf("case %d: sameAddr(%v, %v) = %v, want %v", i, tc.a, tc.b, got, tc.want)
		}
	}
}

// crypto/tls's QUICErrorEvent ends the handshake: pump used to fall through
// its switch on it and report nothing, leaving the failure to the deadline.
// It now fails with the TLS error, alert code intact.
func TestPumpFailsOnTLSErrorEvent(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	c := newConn(newScriptedPC(), peer, []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}
	c.tls = tls.QUICClient(&tls.QUICConfig{TLSConfig: ClientTLSForTest()})
	if err := c.start(); err != nil {
		t.Fatal(err)
	}
	// A handshake message of an unknown type: TLS fails the handshake.
	if err := c.tls.HandleData(tls.QUICEncryptionLevelInitial, []byte{0xfe, 0, 0, 1, 0}); err == nil {
		t.Fatal("precondition: crypto/tls accepted a garbage handshake message")
	}
	done, err := c.pump()
	if done || err == nil {
		t.Fatalf("pump = %v, %v; want the TLS failure", done, err)
	}
	if code := closeCodeFor(err); code < transportCryptoErrorBase || code > transportCryptoErrorBase+0xff {
		t.Errorf("close code %#x for %v; want the TLS alert's crypto-range code", code, err)
	}
}

// RFC 9002 §6.2.4: when a PTO fires in one space, the other spaces with
// data in flight are probed too — the peer may hold receive keys for only
// one of them, and the Handshake deadline, armed later than the Initial one,
// would otherwise make it wait. The Handshake timer here is nowhere near
// due; its space is probed because the Initial one fired.
func TestPTOProbesTheOtherSpacesWithDataInFlight(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	c.spaces[spaceHandshake].sealer = c.spaces[spaceInitial].sealer
	for space, age := range map[int]time.Duration{spaceInitial: time.Minute, spaceHandshake: time.Millisecond} {
		sp := &c.spaces[space]
		sp.lastElicit = now.Add(-age)
		sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-age), size: 100, ackEliciting: true, payload: []byte{framePing}})
		sp.nextPN = 1
	}
	c.mu.Unlock()

	c.actOnDeadlines(now)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ptoCount != 1 {
		t.Errorf("ptoCount = %d, want 1: one expiry, the Initial space's", c.ptoCount)
	}
	for _, space := range []int{spaceInitial, spaceHandshake} {
		if c.spaces[space].nextPN != 3 {
			t.Errorf("space %d: nextPN = %d, want a two-packet probe sent (3)", space, c.spaces[space].nextPN)
		}
	}
}
