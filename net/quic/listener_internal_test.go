package quic

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// syntheticInitial builds a datagram that parses as an Initial long-header
// packet but does not decrypt — its payload is noise under no key. Padded
// to 1200 bytes so it clears the §14.1 size check and fails exactly at the
// decrypt check the listener now runs before spending anything on it.
func syntheticInitial(t *testing.T, dcid, scid []byte) []byte {
	t.Helper()
	var b []byte
	b = append(b, 0xc0)       // long header, fixed bit, type Initial, pnLen 1
	b = append(b, 0, 0, 0, 1) // version 1
	b = append(b, byte(len(dcid)))
	b = append(b, dcid...)
	b = append(b, byte(len(scid)))
	b = append(b, scid...)
	b = AppendVarint(b, 0)        // token length
	payload := make([]byte, 1180) // pn (1) + noise; enough to pass 1200 as a datagram
	b = AppendVarint(b, uint64(len(payload)))
	return append(b, payload...)
}

// sealedClientInitial builds what a real client's first datagram looks like
// to a listener: an Initial sealed under the keys its own destination
// identifier derives, carrying a PING, padded to the full 1200 bytes. The
// connection it spawns never finishes a handshake — the tests below don't
// need it to — but it decrypts, which is now the price of spawning one.
func sealedClientInitial(tb testing.TB, dcid, scid []byte) []byte {
	tb.Helper()
	return sealedClientInitialWithToken(tb, dcid, scid, nil)
}

// sealedClientInitialWithToken is sealedClientInitial carrying a token, the
// shape of a client's second Initial after a Retry.
func sealedClientInitialWithToken(tb testing.TB, dcid, scid, token []byte) []byte {
	tb.Helper()
	secrets, err := initialSecrets(dcid)
	if err != nil {
		tb.Fatal(err)
	}
	sealer, err := newPacketSealer(secrets.Client)
	if err != nil {
		tb.Fatal(err)
	}
	const pnLen = 4
	var header []byte
	header = append(header, 0xc0|byte(packetInitial)<<4|byte(pnLen-1))
	header = append(header, 0, 0, 0, 1) // version 1
	header = append(header, byte(len(dcid)))
	header = append(header, dcid...)
	header = append(header, byte(len(scid)))
	header = append(header, scid...)
	header = AppendVarint(header, uint64(len(token)))
	header = append(header, token...)
	payload := []byte{framePing}
	if pad := initialPadding(len(header), pnLen, len(payload)); pad > 0 {
		payload = append(payload, make([]byte, pad)...)
	}
	header = AppendVarint(header, uint64(pnLen+len(payload)+16))
	pnOffset := len(header)
	header = append(header, 0, 0, 0, 0) // packet number 0
	pkt, err := sealer.Seal(nil, header, payload, 0, pnOffset, pnLen)
	if err != nil {
		tb.Fatal(err)
	}
	return pkt
}

// A client whose first Initial got no answer in time retransmits it — same
// bytes, same original destination identifier. The listener must route the
// duplicate to the connection attempt it already has, not mint a second one
// for the same client.
func TestListenerRetransmittedInitialDoesNotDuplicateTheConnection(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555}
	dg := sealedClientInitial(t, []byte("odcid-aa"), []byte("cscid-bb"))

	l.dispatch(append([]byte(nil), dg...), from, l.pc)
	l.dispatch(append([]byte(nil), dg...), from, l.pc)

	l.mu.Lock()
	pending := len(l.pendingConns)
	entries := len(l.byDCID)
	l.mu.Unlock()
	if pending != 1 {
		t.Errorf("%d connection attempts for one retransmitted Initial, want 1", pending)
	}
	// One attempt holds two routing keys: its own scid and the client's
	// original identifier.
	if entries != 2 {
		t.Errorf("%d demux entries, want 2 (scid + odcid) for a single attempt", entries)
	}
}

// An Initial that does not decrypt under its own identifier's keys earns
// nothing: no connection, no table entry, no goroutines held for the 10 s
// handshake deadline. Before this check, ~50 KB/s of spoofed-source noise
// kept all MaxPendingConns slots permanently full.
func TestListenerDiscardsAnInitialThatDoesNotDecrypt(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555}
	l.dispatch(syntheticInitial(t, []byte("odcid-aa"), []byte("cscid-bb")), from, l.pc)

	l.mu.Lock()
	pending, entries := len(l.pendingConns), len(l.byDCID)
	l.mu.Unlock()
	if pending != 0 || entries != 0 {
		t.Errorf("an undecryptable Initial created state: %d pending, %d demux entries", pending, entries)
	}
	if l.DiscardedPackets() == 0 {
		t.Error("the refusal was not counted")
	}
}

// RFC 9000 §14.1: an Initial in a datagram under 1200 bytes is discarded —
// even one that decrypts. A real client always pads; answering a small one
// (with a Retry, above all) is a stateless amplifier aimed at the spoofed
// source.
func TestListenerDiscardsSmallInitialDatagrams(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{AlwaysRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPC.Close()

	// A sealed Initial, genuinely decryptable, just not padded: the sealed
	// packet without its PADDING would be well under 200 bytes, so build
	// one by trimming the construction — a 1-byte-payload seal.
	dg := sealedSmallInitial(t, []byte("odcid-aa"), []byte("cscid-bb"))
	if len(dg) >= 1200 {
		t.Fatalf("the test datagram is %d bytes; it must be under 1200 to prove anything", len(dg))
	}
	l.dispatch(dg, clientPC.LocalAddr(), l.pc)

	l.mu.Lock()
	pending := len(l.pendingConns)
	l.mu.Unlock()
	if pending != 0 {
		t.Errorf("a sub-1200 Initial created %d connection attempts", pending)
	}
	// No Retry comes back either: with AlwaysRetry on, this used to be a
	// ~3x reflection toward the spoofed source.
	if err := clientPC.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	if n, _, err := clientPC.ReadFrom(buf); err == nil {
		t.Errorf("a %d-byte answer came back for a sub-1200 Initial", n)
	}
}

// Past half of MaxPendingConns, a tokenless Initial gets a stateless Retry
// instead of a connection: the remaining slots are kept for addresses that
// prove themselves across the extra round trip.
func TestListenerRetriesPastThePendingHighWaterMark(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{MaxPendingConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Fill the table to the mark (2/2 = 1) with one genuine attempt.
	first := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555}
	l.dispatch(sealedClientInitial(t, []byte("odcid-aa"), []byte("cscid-bb")), first, l.pc)
	l.mu.Lock()
	pending := len(l.pendingConns)
	l.mu.Unlock()
	if pending != 1 {
		t.Fatalf("the priming attempt left %d pending connections, want 1", pending)
	}

	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPC.Close()
	l.dispatch(sealedClientInitial(t, []byte("odcid-cc"), []byte("cscid-dd")), clientPC.LocalAddr(), l.pc)

	l.mu.Lock()
	pending = len(l.pendingConns)
	l.mu.Unlock()
	if pending != 1 {
		t.Errorf("past the high-water mark, a tokenless Initial still created a connection (%d pending)", pending)
	}
	if err := clientPC.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _, err := clientPC.ReadFrom(buf)
	if err != nil {
		t.Fatalf("no Retry came back: %v", err)
	}
	if buf[0]&0xf0 != 0xf0 {
		t.Errorf("the answer's first byte is %#x, want a Retry long header", buf[0])
	}
	_ = n
}

// sealedSmallInitial is sealedClientInitial without the §14.1 padding — the
// shape a forged minimal Initial takes.
func sealedSmallInitial(t *testing.T, dcid, scid []byte) []byte {
	t.Helper()
	secrets, err := initialSecrets(dcid)
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(secrets.Client)
	if err != nil {
		t.Fatal(err)
	}
	const pnLen = 4
	var header []byte
	header = append(header, 0xc0|byte(packetInitial)<<4|byte(pnLen-1))
	header = append(header, 0, 0, 0, 1)
	header = append(header, byte(len(dcid)))
	header = append(header, dcid...)
	header = append(header, byte(len(scid)))
	header = append(header, scid...)
	header = AppendVarint(header, 0)
	payload := make([]byte, 16) // PING plus a little padding; nowhere near 1200
	payload[0] = framePing
	header = AppendVarint(header, uint64(pnLen+len(payload)+16))
	pnOffset := len(header)
	header = append(header, 0, 0, 0, 0)
	pkt, err := sealer.Seal(nil, header, payload, 0, pnOffset, pnLen)
	if err != nil {
		t.Fatal(err)
	}
	return pkt
}

// The RFC 9000 §8.1 arithmetic at its enforcement point: an unvalidated
// connection's sends stop silently at three times what was received, and
// flow again once the address validates.
func TestAmplificationLimitBlocksUnvalidatedSends(t *testing.T) {
	serverPC, clientPC := newLoopbackPair(t)

	c := newConn(serverPC, clientPC.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	c.amplActive = true
	c.amplRecv = 1200 // one client Initial's worth
	payload := make([]byte, 900)
	for range 8 {
		if err := c.sendPacketLocked(spaceInitial, payload, sendOpts{noRetrans: true}); err != nil {
			c.mu.Unlock()
			t.Fatal(err)
		}
	}
	blockedAt := c.amplSent
	if blockedAt > 3*c.amplRecv {
		c.mu.Unlock()
		t.Fatalf("amplSent = %d, over the 3x limit of %d", blockedAt, 3*c.amplRecv)
	}
	if blockedAt == 0 {
		c.mu.Unlock()
		t.Fatal("nothing was sent at all — the limit should allow up to 3x, not 0")
	}
	// Address validated: the limit stops applying and sends flow again.
	c.amplActive = false
	if err := c.sendPacketLocked(spaceInitial, payload, sendOpts{noRetrans: true}); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	after := c.amplSent
	c.mu.Unlock()

	if after != blockedAt {
		t.Errorf("amplSent moved from %d to %d after validation — accounting should have stopped", blockedAt, after)
	}
}

// End to end over one shared socket, watched from inside: by the time a
// connection reaches Accept, its address is validated and the bytes it sent
// while unvalidated never exceeded three times what it had received.
func TestListenerValidatesTheAddressDuringTheHandshake(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	done := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(done)
			return
		}
		done <- c
	}()

	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(clientPC, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	select {
	case c, ok := <-done:
		if !ok {
			t.Fatal("Accept failed")
		}
		c.mu.Lock()
		active, recvd, sentB := c.amplActive, c.amplRecv, c.amplSent
		c.mu.Unlock()
		if active {
			t.Error("the connection reached Accept with its address still unvalidated")
		}
		if sentB > 3*recvd {
			t.Errorf("while unvalidated, %d bytes went out against %d received — over the 3x limit", sentB, recvd)
		}
		if recvd == 0 || sentB == 0 {
			t.Errorf("amplRecv=%d amplSent=%d — the accounting never ran at all", recvd, sentB)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}
}

// newLoopbackPair gives two real UDP sockets addressed at each other, for
// internal tests that need a Conn without a full handshake.
func newLoopbackPair(t *testing.T) (a, b net.PacketConn) {
	t.Helper()
	pa, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	pb, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pa.Close(); _ = pb.Close() })
	return pa, pb
}

// ServerTLSForTest and ClientTLSForTest are the package's one copy of the
// test TLS setup: a freshly minted self-signed certificate and the one suite
// this package protects packets with, which keeps a mismatch a refusal
// rather than a wrong key. Exported only in test builds, so the external
// quic_test package shares them instead of restating them.
func ServerTLSForTest(tb testing.TB) *tls.Config {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		tb.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
		NextProtos:   []string{"h3"},
	}
}

func ClientTLSForTest() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: a certificate this test minted
		MinVersion:         tls.VersionTLS13,
		CipherSuites:       []uint16{tls.TLS_AES_128_GCM_SHA256},
		NextProtos:         []string{"h3"},
		ServerName:         "127.0.0.1",
	}
}

// capturePC records what the listener writes, for tests that assert on the
// datagrams it answers with.
type capturePC struct {
	fuzzSink
	mu      sync.Mutex
	writes  [][]byte
	stopped chan struct{}
}

// ReadFrom blocks until Close so the listener's read loop can exit —
// fuzzSink's own ReadFrom parks forever and would hang Listener.Close.
func (c *capturePC) ReadFrom(p []byte) (int, net.Addr, error) {
	<-c.stopped
	return 0, nil, net.ErrClosed
}

func (c *capturePC) Close() error {
	select {
	case <-c.stopped:
	default:
		close(c.stopped)
	}
	return nil
}

func (c *capturePC) WriteTo(p []byte, addr net.Addr) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	c.mu.Unlock()
	return len(p), nil
}

func (c *capturePC) taken() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.writes
	c.writes = nil
	return w
}

// RFC 9000 §10.3: a short-header datagram naming an identifier nothing
// routes to is answered with a stateless reset — random-looking, smaller
// than its trigger, ending in the token the identifier derives — so a
// client holding state for a connection this listener lost learns now
// rather than at its idle timeout. Rate-limited, and never for triggers
// too small to answer without a loop.
func TestListenerSendsStatelessResetForUnknownShortHeader(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	from := fakeFuzzAddr("client:lost")

	dcid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	trigger := make([]byte, 60)
	trigger[0] = 0x40
	copy(trigger[1:], dcid)
	l.dispatch(append([]byte(nil), trigger...), from, l.pc)

	writes := pc.taken()
	if len(writes) != 1 {
		t.Fatalf("the listener wrote %d datagrams, want 1 reset", len(writes))
	}
	reset := writes[0]
	if len(reset) < 21 || len(reset) >= len(trigger) {
		t.Errorf("a %d-byte reset for a %d-byte trigger violates §10.3's size rules", len(reset), len(trigger))
	}
	if reset[0]&0xc0 != 0x40 {
		t.Errorf("reset first byte %#x does not have the short-header shape", reset[0])
	}
	want := l.resetTokenFor(dcid)
	if !bytes.Equal(reset[len(reset)-16:], want[:]) {
		t.Error("the reset does not end in the trigger identifier's token")
	}

	// Too small to answer without a possible loop: silence.
	l.dispatch(append([]byte(nil), trigger[:21]...), from, l.pc)
	if got := pc.taken(); len(got) != 0 {
		t.Errorf("a 21-byte trigger was answered with %d datagrams, want none", len(got))
	}

	// The budget: within one window, emission stops at the cap.
	for range 3 * maxResetsPerSecond {
		l.dispatch(append([]byte(nil), trigger...), from, l.pc)
	}
	if got := len(pc.taken()); got > maxResetsPerSecond {
		t.Errorf("%d resets in one window, over the %d budget", got, maxResetsPerSecond)
	}
}

// One source address gets a share of the stateless reset budget, not all
// of it: a sender spraying unknown identifiers must not leave every other
// client whose connection was lost to wait out its idle timeout. The share
// is per IP — varying the port buys nothing.
func TestStatelessResetBudgetIsPerSource(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	trigger := make([]byte, 60)
	trigger[0] = 0x40
	copy(trigger[1:], []byte{1, 2, 3, 4, 5, 6, 7, 8})

	for port := range 3 * maxResetsPerSecond {
		from := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000 + port}
		l.dispatch(append([]byte(nil), trigger...), from, l.pc)
	}
	if got := len(pc.taken()); got != maxResetsPerSourcePerSecond {
		t.Errorf("one address drew %d resets in one window, want its share of %d", got, maxResetsPerSourcePerSecond)
	}

	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 443}
	l.dispatch(append([]byte(nil), trigger...), other, l.pc)
	if got := len(pc.taken()); got != 1 {
		t.Errorf("another address drew %d resets after the first one's flood, want 1", got)
	}
}

// erroringPC fails every read the way a broken socket does, counting the
// attempts, until closed.
type erroringPC struct {
	fuzzSink
	reads   atomic.Int64
	stopped chan struct{}
	once    sync.Once
}

func (p *erroringPC) ReadFrom([]byte) (int, net.Addr, error) {
	p.reads.Add(1)
	select {
	case <-p.stopped:
		return 0, nil, net.ErrClosed
	default:
		return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: syscall.ENOBUFS}
	}
}

func (p *erroringPC) Close() error {
	p.once.Do(func() { close(p.stopped) })
	return nil
}

// A socket whose every read fails used to spin the listener's read loop
// at full CPU. It now backs off like a connection's own read loop does,
// and Close still ends it promptly.
func TestListenerReadLoopBacksOffAPersistentError(t *testing.T) {
	pc := &erroringPC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a few failed reads", func() bool { return pc.reads.Load() >= 5 })
	start := time.Now()
	window := 100 * time.Millisecond
	before := pc.reads.Load()
	for time.Since(start) < window {
		runtime.Gosched()
	}
	// 1 ms doubling: a handful of reads in 100 ms, where a spinning loop
	// makes hundreds of thousands.
	if n := pc.reads.Load() - before; n > 50 {
		t.Errorf("%d reads in %v of a persistently failing socket; the loop is spinning", n, window)
	}
	if got := l.DiscardedPackets(); got < 5 {
		t.Errorf("DiscardedPackets = %d; the failed reads were not counted", got)
	}
	closed := make(chan struct{})
	go func() { _ = l.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited out the read loop's backoff")
	}
}

// forget deletes a connection's own demux keys through the per-connection
// index — every key it registered, whatever path registered it — and
// leaves alone a key the connection registered that now routes elsewhere.
func TestForgetUsesThePerConnectionIndex(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	a, b := &Conn{}, &Conn{}
	l.mu.Lock()
	l.routeLocked("a-scid", a)
	l.routeLocked("shared", a) // a client-chosen first identifier...
	l.routeLocked("shared", b) // ...that another connection since took over
	l.routeLocked("b-scid", b)
	l.mu.Unlock()
	issued, err := l.addLocalCID(a)
	if err != nil {
		t.Fatal(err)
	}
	retired, err := l.addLocalCID(a)
	if err != nil {
		t.Fatal(err)
	}
	l.removeLocalCID(retired)

	l.mu.Lock()
	keysA := len(l.keysOf[a])
	l.mu.Unlock()
	if keysA != 3 {
		t.Errorf("a's index holds %d keys after a retirement, want 3", keysA)
	}

	l.forget(a)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range []string{"a-scid", string(issued), string(retired)} {
		if _, ok := l.byDCID[k]; ok {
			t.Errorf("key %q survived forget", k)
		}
	}
	if l.byDCID["shared"] != b || l.byDCID["b-scid"] != b {
		t.Error("forget removed a key that routes to another connection")
	}
	if _, ok := l.keysOf[a]; ok {
		t.Error("the forgotten connection's index entry was kept")
	}
}

// BenchmarkListenerForget measures forgetting one connection out of a
// table holding many: proportional to the connection's own keys, not to
// the table, since every dispatch waits on the same lock.
func BenchmarkListenerForget(b *testing.B) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.mu.Lock()
	for i := range 10000 {
		other := &Conn{}
		l.routeLocked(strconv.Itoa(i)+"-0", other)
		l.routeLocked(strconv.Itoa(i)+"-1", other)
	}
	l.mu.Unlock()
	c := &Conn{}
	b.ReportAllocs()
	for b.Loop() {
		l.mu.Lock()
		l.routeLocked("mine-0", c)
		l.routeLocked("mine-1", c)
		l.mu.Unlock()
		l.forget(c)
	}
}

// One connection whose consumer stops draining — a stream callback applying
// back-pressure, as a slow ServeH3 handler does — must not freeze the
// listener's single dispatch loop, and with it every other connection and
// handshake on the socket.
func TestListenerSlowConnectionDoesNotFreezeOthers(t *testing.T) {
	spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(spc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	addr := spc.LocalAddr()

	accepted := make(chan *Conn, 2)
	go func() {
		for range 2 {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	dial := func() <-chan *Conn {
		out := make(chan *Conn, 1)
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			c, err := Dial(pc, addr, ClientTLSForTest(), DefaultParameters())
			if err != nil {
				t.Errorf("Dial: %v", err)
				close(out)
				return
			}
			t.Cleanup(func() { _ = c.Close() })
			out <- c
		}()
		return out
	}
	recv := func(ch <-chan *Conn, what string) *Conn {
		t.Helper()
		select {
		case c, ok := <-ch:
			if !ok {
				t.Fatalf("%s: no connection", what)
			}
			return c
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: nothing after 10s — the listener appears frozen", what)
		}
		return nil
	}

	slowClient := recv(dial(), "slow client Dial")
	slowServer := recv(accepted, "slow server Accept")
	defer func() { _ = slowServer.Close() }()
	release := make(chan struct{})
	defer close(release) // runs before the Close above
	entered := make(chan struct{})
	var once sync.Once
	slowServer.OnStreamFrames(func([]Frame) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	})
	s, err := slowClient.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("stall")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the slow server's callback never ran")
	}

	// Its read goroutine is parked in the callback: datagrams naming its
	// identifier now fill its channel. Send well past the channel's
	// capacity — the content need not authenticate, routing is by
	// identifier alone.
	noise, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer noise.Close()
	before := l.DiscardedPackets()
	junk := append([]byte{0x40}, slowServer.scid...)
	junk = append(junk, make([]byte, 40)...)
	for range 4 * cap(slowServer.incoming) {
		if _, err := noise.WriteTo(junk, addr); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "the listener to shed datagrams the slow connection cannot take", func() bool {
		return l.DiscardedPackets() > before
	})

	// A second connection still handshakes and exchanges data.
	fastClient := recv(dial(), "second client Dial")
	fastServer := recv(accepted, "second server Accept")
	defer func() { _ = fastServer.Close() }()
	go func() {
		ss, err := fastServer.AcceptStream()
		if err != nil {
			return
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(ss, buf); err != nil {
			return
		}
		_, _ = ss.Write(buf)
		_ = ss.CloseWrite()
	}()
	fs, err := fastClient.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(fs)
		got <- string(b)
	}()
	select {
	case g := <-got:
		if g != "hello" {
			t.Fatalf("echo = %q, want %q", g, "hello")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second connection's echo never came back")
	}
}

// BenchmarkListenerRefusedInitial measures the front door's cost for a
// decryptable Initial it refuses with a stateless CONNECTION_REFUSED: the
// decrypt check and the answer both need the Initial keys, which are
// derived once per Initial, not once per use.
func BenchmarkListenerRefusedInitial(b *testing.B) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.lcfg.MaxPendingConns = 0 // every Initial meets a full table
	dgram := sealedClientInitial(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 9, 9, 9})
	from := fakeFuzzAddr("client:refused")
	b.ReportAllocs()
	for b.Loop() {
		l.dispatch(dgram, from, l.pc)
		pc.taken()
	}
}
