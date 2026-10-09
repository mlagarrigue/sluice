package quic

import (
	"bytes"
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

// Regression tests for the transport-layer findings of the October 2026
// audit pass: each fails against the behaviour it documents.

// newIdleServerConn is a server-side Conn with Initial keys and no
// amplification limit — a bare transport for driving the timer and send
// paths directly.
func newIdleServerConn(t *testing.T) *Conn {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
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
	for _, space := range []int{spaceInitial, spaceHandshake} {
		if c.spaces[space].nextPN != 2 {
			t.Errorf("space %d: nextPN = %d, want a probe sent (2)", space, c.spaces[space].nextPN)
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
	c, err := Dial(cli, srv.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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
	if !errors.Is(err, ErrQUIC) || !strings.Contains(err.Error(), "code 0x2") {
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

// The idle timeout firing: the connection ends, silently — no
// CONNECTION_CLOSE (§10.1), and no error, since idleness is how the protocol
// expects an unused connection to end. The existing idle tests only pinned
// when it does not fire.
func TestIdleTimeoutExpiresSilently(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	pc := newScriptedPC()
	c := newConn(pc, peer, []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
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
	if err := c.Err(); err != nil {
		t.Errorf("Err = %v; an idle timeout is not an error", err)
	}
	if n := pc.writes.Load() - before; n != 0 {
		t.Errorf("%d datagrams written on idle expiry; it must send nothing", n)
	}
}

// The loss timer firing on its own, with no ACK to trigger it: the packet
// an earlier ACK left too young to declare is declared once the timer
// matures, resent, and the timer re-arms on the survivor.
func TestLossTimerDeclaresWithoutAnAck(t *testing.T) {
	c := newIdleServerConn(t)
	now := time.Now()
	c.mu.Lock()
	sp := &c.spaces[spaceInitial]
	old := []byte{framePing, 0xaa}
	sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Minute), size: 100, ackEliciting: true, payload: old})
	sp.sent.record(sentPacket{pn: 1, sentAt: now, size: 100, ackEliciting: true, payload: []byte{framePing, 0xbb}})
	sp.nextPN = 2
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
	if sp.nextPN != 3 {
		t.Errorf("nextPN = %d, want the lost payload resent as packet 2", sp.nextPN)
	}
	resent := false
	for _, p := range sp.sent.packets {
		if p.pn == 2 && bytes.Contains(p.payload, old) {
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

// A probe with lost payloads queued resends the head of that queue —
// recorded for recovery like any packet, since it is the only copy.
func TestProbeSendsTheRetransmissionQueueFirst(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceInitial]
	first, second := []byte{framePing, 1}, []byte{framePing, 2}
	sp.retrans = [][]byte{first, second}

	c.sendProbeLocked(spaceInitial)

	if len(sp.retrans) != 1 || !bytes.Equal(sp.retrans[0], second) {
		t.Fatalf("retrans = %x, want only the second payload left", sp.retrans)
	}
	if sp.nextPN != 1 {
		t.Fatalf("nextPN = %d, want one probe sent", sp.nextPN)
	}
	if len(sp.sent.packets) != 1 || !bytes.Contains(sp.sent.packets[0].payload, first) {
		t.Errorf("the probe's payload is not tracked for recovery: %+v", sp.sent.packets)
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
	c.SetManualCredit(true)
	held := 0
	c.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			held += len(f.Data)
		}
		return nil
	})

	chunk := make([]byte, 100)
	var off uint64
	send := func() error {
		_, err := c.frames(spaceApplication, streamFrameBytes(0, off, chunk), time.Now())
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
