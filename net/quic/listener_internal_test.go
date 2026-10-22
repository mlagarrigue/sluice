package quic

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"runtime"
	"strconv"
	"strings"
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
	return sealedClientInitialFirstByte(tb, 0xc0, dcid, scid, token)
}

// sealedClientInitialFirstByte is sealedClientInitialWithToken with the
// high nibble of the first byte chosen by the caller — 0xc0 for a client's
// real Initial, 0xcc for one with the reserved bits set.
func sealedClientInitialFirstByte(tb testing.TB, first byte, dcid, scid, token []byte) []byte {
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
	header = append(header, first|byte(packetInitial)<<4|byte(pnLen-1))
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

	l.dispatch(append([]byte(nil), dg...), from, l.pc, 0)
	l.dispatch(append([]byte(nil), dg...), from, l.pc, 0)

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
	l.dispatch(syntheticInitial(t, []byte("odcid-aa"), []byte("cscid-bb")), from, l.pc, 0)

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
	l.dispatch(dg, clientPC.LocalAddr(), l.pc, 0)

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

// statelessCloseCodes opens every Initial among writes under the server
// keys dcid derives (what a stateless close is sealed with) and returns the
// transport code of each CONNECTION_CLOSE found — empty when the listener
// answered with anything else, or nothing.
func statelessCloseCodes(tb testing.TB, writes [][]byte, dcid []byte) []uint64 {
	tb.Helper()
	secrets, err := initialSecrets(dcid)
	if err != nil {
		tb.Fatal(err)
	}
	opener, err := newPacketOpener(secrets.Server)
	if err != nil {
		tb.Fatal(err)
	}
	var codes []uint64
	for _, w := range writes {
		h, _, err := parseLongHeader(w)
		if err != nil || h.Type != packetInitial {
			continue
		}
		opened, _, err := opener.Open(h.Raw, h.PNOffset, 0)
		if err != nil {
			continue
		}
		frames, err := parseFrames(nil, opened)
		if err != nil {
			tb.Fatal(err)
		}
		for _, f := range frames {
			if f.Type == frameConnectionClose {
				codes = append(codes, f.value)
			}
		}
	}
	return codes
}

// RFC 9000 §8.1.3: a token this listener's key never signed — a NEW_TOKEN
// from another server of the same name, or a Retry token from a listener
// since restarted — proves nothing and is not an error either: the client
// is unvalidated, which with the Retry flow on means a Retry, not the
// INVALID_TOKEN close §8.1.2 reserves for a Retry token this listener
// issued and cannot accept.
func TestForeignTokenIsAnsweredWithARetry(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{AlwaysRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	from := fakeFuzzAddr("client:foreign")
	dcid, scid := []byte("dst-cid1"), []byte("src-cid1")

	// Genuinely encoded, same shape, same address — under another key.
	other, err := newRetryTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	foreign := appendRetryToken(nil, other, from, []byte("orig-dst"), dcid)
	l.dispatch(sealedClientInitialWithToken(t, dcid, scid, foreign), from, l.pc, 0)

	writes := pc.taken()
	if len(writes) != 1 {
		t.Fatalf("the listener wrote %d datagrams for a foreign token, want one Retry", len(writes))
	}
	if h, _, err := parseLongHeader(writes[0]); err != nil || h.Type != packetRetry {
		t.Errorf("the answer is %v (type %d), want a Retry", err, h.Type)
	}
	if codes := statelessCloseCodes(t, writes, dcid); len(codes) != 0 {
		t.Errorf("a foreign token was closed with %v; §8.1.3 wants the client treated as unvalidated", codes)
	}

	// The witness: the same token under this listener's key but for another
	// address is a Retry token this listener issued and cannot accept —
	// INVALID_TOKEN (§8.1.2), as before.
	elsewhere := appendRetryToken(nil, l.lcfg.TokenKey, fakeFuzzAddr("client:other"), []byte("orig-dst"), dcid)
	l.dispatch(sealedClientInitialWithToken(t, dcid, scid, elsewhere), from, l.pc, 0)
	if codes := statelessCloseCodes(t, pc.taken(), dcid); len(codes) != 1 || codes[0] != transportInvalidToken {
		t.Errorf("this listener's token for another address got %v, want one INVALID_TOKEN close", codes)
	}
}

// Without the Retry flow, the same foreign token leaves the client exactly
// where a tokenless one stands: admitted on the direct path, under the
// amplification limit, and never closed for it.
func TestForeignTokenStartsAConnectionWithoutRetry(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	from := fakeFuzzAddr("client:foreign")
	dcid, scid := []byte("dst-cid2"), []byte("src-cid2")

	other, err := newRetryTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	foreign := appendRetryToken(nil, other, from, []byte("orig-dst"), dcid)
	l.dispatch(sealedClientInitialWithToken(t, dcid, scid, foreign), from, l.pc, 0)

	l.mu.Lock()
	pending := len(l.pendingConns)
	c := l.byDCID[string(dcid)]
	l.mu.Unlock()
	if pending != 1 || c == nil {
		t.Fatalf("a foreign token started %d connection attempts (routed: %v), want one on the direct path", pending, c != nil)
	}
	if !c.amplActive {
		t.Error("the connection a foreign token started is exempt from the amplification limit; it proved nothing")
	}
	if codes := statelessCloseCodes(t, pc.taken(), dcid); len(codes) != 0 {
		t.Errorf("a foreign token was closed with %v on the direct path", codes)
	}
}

// RFC 9000 §8.1.4: a Retry token admits one connection. Once that
// connection is forgotten — its identifiers free again, most of the token's
// TTL still ahead — the same token from the same address is answered with
// INVALID_TOKEN (§8.1.2: the client holds a Retry token and accepts no
// other), not with a second connection that skipped the round trip.
func TestRetryTokenIsConsumedWithItsConnection(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{AlwaysRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	from := fakeFuzzAddr("client:replay")
	scid := []byte("src-cid3")

	// The Retry flow, as a client would walk it: a tokenless Initial, the
	// Retry it earns, and the second Initial echoing the token under the
	// identifier the Retry chose.
	l.dispatch(sealedClientInitial(t, []byte("dst-cid3"), scid), from, l.pc, 0)
	writes := pc.taken()
	if len(writes) != 1 {
		t.Fatalf("the listener wrote %d datagrams for a first Initial, want one Retry", len(writes))
	}
	retry, _, err := parseLongHeader(writes[0])
	if err != nil || retry.Type != packetRetry {
		t.Fatalf("the answer is %v (type %d), want a Retry", err, retry.Type)
	}
	newSCID, token := retry.SCID, retry.Token
	second := sealedClientInitialWithToken(t, newSCID, scid, token)
	l.dispatch(second, from, l.pc, 0)

	l.mu.Lock()
	c := l.byDCID[string(newSCID)]
	l.mu.Unlock()
	if c == nil {
		t.Fatal("the Initial echoing the token started no connection")
	}
	if c.amplActive {
		t.Error("the connection the token admitted still pays the amplification limit")
	}

	// The connection ends and is forgotten, as a failed handshake or a
	// finished closing period would leave it; the token has most of its
	// TTL left.
	_ = c.Close()
	l.forget(c)
	pc.taken()

	l.dispatch(second, from, l.pc, 0)
	l.mu.Lock()
	again := l.byDCID[string(newSCID)]
	l.mu.Unlock()
	if again != nil {
		t.Error("the same token opened a second connection from the same address")
	}
	if codes := statelessCloseCodes(t, pc.taken(), newSCID); len(codes) != 1 || codes[0] != transportInvalidToken {
		t.Errorf("the replayed token got %v, want one INVALID_TOKEN close", codes)
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
	l.dispatch(sealedClientInitial(t, []byte("odcid-aa"), []byte("cscid-bb")), first, l.pc, 0)
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
	l.dispatch(sealedClientInitial(t, []byte("odcid-cc"), []byte("cscid-dd")), clientPC.LocalAddr(), l.pc, 0)

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
func newLoopbackPair(tb testing.TB) (a, b net.PacketConn) {
	tb.Helper()
	pa, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		tb.Fatal(err)
	}
	pb, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = pa.Close(); _ = pb.Close() })
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
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	from := fakeFuzzAddr("client:lost")

	dcid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	trigger := make([]byte, 60)
	trigger[0] = 0x40
	copy(trigger[1:], dcid)
	l.dispatch(append([]byte(nil), trigger...), from, l.pc, 0)

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
	l.dispatch(append([]byte(nil), trigger[:21]...), from, l.pc, 0)
	if got := pc.taken(); len(got) != 0 {
		t.Errorf("a 21-byte trigger was answered with %d datagrams, want none", len(got))
	}

	// The budget: within one window, emission stops at the cap.
	for range 3 * maxResetsPerSecond {
		l.dispatch(append([]byte(nil), trigger...), from, l.pc, 0)
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
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	trigger := make([]byte, 60)
	trigger[0] = 0x40
	copy(trigger[1:], []byte{1, 2, 3, 4, 5, 6, 7, 8})

	for port := range 3 * maxResetsPerSecond {
		from := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000 + port}
		l.dispatch(append([]byte(nil), trigger...), from, l.pc, 0)
	}
	if got := len(pc.taken()); got != maxResetsPerSourcePerSecond {
		t.Errorf("one address drew %d resets in one window, want its share of %d", got, maxResetsPerSourcePerSecond)
	}

	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 443}
	l.dispatch(append([]byte(nil), trigger...), other, l.pc, 0)
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
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
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
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	a, b := &Conn{}, &Conn{}
	// Bare values with no state to end: Close ends every routed connection,
	// so b leaves the table before it, and a already did.
	defer func() { l.forget(b); _ = l.Close() }()
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
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
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
	s, err := slowClient.OpenStream(t.Context())
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
		ss, err := fastServer.AcceptStream(t.Context())
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
	fs, err := fastClient.OpenStream(t.Context())
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
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.lcfg.MaxPendingConns = 0 // every Initial meets a full table
	dgram := sealedClientInitial(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 9, 9, 9})
	from := fakeFuzzAddr("client:refused")
	b.ReportAllocs()
	for b.Loop() {
		l.dispatch(dgram, from, l.pc, 0)
		pc.taken()
	}
}

// BenchmarkListenerRetriedInitial measures the front door's cost for a
// first Initial answered with a Retry: the decrypt check, then a token and
// a packet sealed under Retry's fixed key — the server's own Initial secret
// is never needed, and never derived.
func BenchmarkListenerRetriedInitial(b *testing.B) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{AlwaysRetry: true})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	dgram := sealedClientInitial(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 9, 9, 9})
	from := fakeFuzzAddr("client:retried")
	b.ReportAllocs()
	for b.Loop() {
		l.dispatch(dgram, from, l.pc, 0)
		pc.taken()
	}
}

// BenchmarkListenerUndecryptableInitial measures what a flood of noise
// shaped like Initials costs: a full key schedule for the client's
// direction and one failed decrypt per datagram, nothing more.
func BenchmarkListenerUndecryptableInitial(b *testing.B) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	dgram := sealedClientInitial(b, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 9, 9, 9})
	dgram[len(dgram)-1] ^= 0xff // the tag no longer matches
	from := fakeFuzzAddr("client:noise")
	before := l.DiscardedPackets()
	l.dispatch(dgram, from, l.pc, 0)
	if l.DiscardedPackets() != before+1 || len(pc.taken()) != 0 {
		b.Fatal("the corrupted Initial was not refused silently")
	}
	b.ReportAllocs()
	for b.Loop() {
		l.dispatch(dgram, from, l.pc, 0)
	}
}

// The listener expands the server's direction of the Initial keys only on
// the one refusal that seals under it: never for noise shaped like an
// Initial, whatever room the table has, never for a Retry, and once — not
// twice — for the CONNECTION_REFUSED a full table answers. The three
// benchmarks above measure what that saves; this counts it, so moving the
// expansion back to the head of the path turns it red.
func TestListenerExpandsTheServerKeyOnlyToSeal(t *testing.T) {
	dcid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	initial, err := extractInitial(dcid)
	if err != nil {
		t.Fatal(err)
	}
	sealed := sealedClientInitial(t, dcid, []byte{9, 9, 9, 9})
	noise := append([]byte(nil), sealed...)
	noise[len(noise)-1] ^= 0xff // the tag no longer matches
	for _, tc := range []struct {
		name      string
		lcfg      ListenerConfig
		full      bool
		dgram     []byte
		expanded  int64
		answers   int
		discarded uint64
	}{
		{"noise", ListenerConfig{}, false, noise, 0, 0, 1},
		{"noise on a full table", ListenerConfig{}, true, noise, 0, 0, 1},
		{"retried", ListenerConfig{AlwaysRetry: true}, false, sealed, 0, 1, 0},
		{"refused on a full table", ListenerConfig{}, true, sealed, 1, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
			l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), tc.lcfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Close() }()
			if tc.full {
				l.lcfg.MaxPendingConns = 0
			}
			var n atomic.Int64
			seen := func(secret []byte, label string) {
				if label == "server in" && bytes.Equal(secret, initial) {
					n.Add(1)
				}
			}
			expansionSeen.Store(&seen)
			l.dispatch(tc.dgram, fakeFuzzAddr("client:"+tc.name), l.pc, 0)
			expansionSeen.Store(nil)
			if got := n.Load(); got != tc.expanded {
				t.Errorf("the server's Initial secret was expanded %d times, want %d", got, tc.expanded)
			}
			if got := len(pc.taken()); got != tc.answers {
				t.Errorf("%d answers sent, want %d", got, tc.answers)
			}
			if got := l.DiscardedPackets(); got != tc.discarded {
				t.Errorf("DiscardedPackets = %d, want %d", got, tc.discarded)
			}
		})
	}
}

// A connection whose read loop is busy for a moment must not lose the burst
// a large response draws back: the listener queues up to connQueueLen
// datagrams before it drops. A queue of 8 dropped this burst and stalled
// each affected request for a client PTO.
func TestListenerQueuesABurstForABusyConnection(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// A connection nothing reads: every datagram routed to it stays queued.
	c := &Conn{scid: bytes.Repeat([]byte{0xab}, listenerCIDLen)}
	if err := l.prepareConn(c); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	l.routeLocked(string(c.scid), c)
	l.mu.Unlock()
	defer l.forget(c)

	const burst = 32 // what a 64 KiB HTTP/3 response drew back, measured
	dg := append([]byte{0x40}, c.scid...)
	dg = append(dg, make([]byte, 40)...)
	before := l.DiscardedPackets()
	for range burst {
		l.dispatch(append([]byte(nil), dg...), pc.LocalAddr(), l.pc, 0)
	}
	if n := l.DiscardedPackets() - before; n != 0 {
		t.Errorf("the listener dropped %d of a %d-datagram burst", n, burst)
	}
	if n := len(c.incoming); n != burst {
		t.Errorf("%d datagrams queued, want %d", n, burst)
	}
}

// A handshake that never completes holds a pending slot only for
// HandshakeTimeout: the slot is given back when it fires. The control, with
// the timeout unset, keeps the slot well past the configured figure — the
// client's default is what bounds it then.
func TestHandshakeTimeoutFreesThePendingSlot(t *testing.T) {
	pendingAfter := func(t *testing.T, lcfg ListenerConfig, wait time.Duration) int {
		t.Helper()
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), lcfg)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()

		from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5556}
		l.dispatch(sealedClientInitial(t, []byte("odcid-ht"), []byte("cscid-ht")), from, l.pc, 0)
		l.mu.Lock()
		pending := len(l.pendingConns)
		l.mu.Unlock()
		if pending != 1 {
			t.Fatalf("%d pending connections right after the Initial, want 1", pending)
		}

		deadline := time.Now().Add(wait)
		for {
			l.mu.Lock()
			pending = len(l.pendingConns)
			l.mu.Unlock()
			if pending == 0 || !time.Now().Before(deadline) {
				return pending
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if pending := pendingAfter(t, ListenerConfig{HandshakeTimeout: 200 * time.Millisecond}, time.Second); pending != 0 {
		t.Errorf("%d pending connections a second after a 200 ms HandshakeTimeout, want 0", pending)
	}
	if pending := pendingAfter(t, ListenerConfig{}, 600*time.Millisecond); pending != 1 {
		t.Errorf("%d pending connections 600 ms into the default handshake timeout, want 1 still", pending)
	}
	if _, err := NewListener(nil, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{HandshakeTimeout: -time.Second}); err == nil {
		t.Error("NewListener accepted a negative HandshakeTimeout")
	}
}

// capRefusesTheSecondDial checks one admission bound end to end: a listener
// under lcfg admits one connection, refuses the next from the same loopback
// address with CONNECTION_REFUSED, and admits again once the first is gone
// and the listener has forgotten it.
func capRefusesTheSecondDial(t *testing.T, lcfg ListenerConfig) {
	t.Helper()
	spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(spc, ServerTLSForTest(t), DefaultParameters(), lcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	dial := func() (*Conn, error) {
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		return Dial(pc, spc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	}

	first, err := dial()
	if err != nil {
		t.Fatalf("the first Dial under the cap: %v", err)
	}
	start := time.Now()
	second, err := dial()
	if err == nil {
		_ = second.Close()
		t.Fatal("the second Dial got a connection; the cap is 1")
	}
	if ce, ok := errors.AsType[*ConnectionError](err); !ok || ce.Application || ce.Code != transportConnectionRefused {
		t.Fatalf("the second Dial failed with %v; want CONNECTION_REFUSED (0x2) during the handshake", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("the refusal took %v to reach the client", elapsed)
	}

	// Closing the first lets the listener forget it, which gives the
	// slot back — to the total and to the address alike — once the server's
	// closing period of three PTOs has run. On Windows no loopback round
	// trip is sampled, the clock being coarser than it, so that period runs
	// on the initial RTT's PTO: about three seconds.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		carried, perIP := len(l.keysOf), len(l.connsByIP)
		l.mu.Unlock()
		if carried == 0 && perIP == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("5 s after the close the listener still carries %d connections, %d addresses", carried, perIP)
		}
		time.Sleep(10 * time.Millisecond)
	}
	third, err := dial()
	if err != nil {
		t.Fatalf("the Dial after the first closed: %v", err)
	}
	_ = third.Close()
}

// MaxConns bounds what the listener carries in total, accepted connections
// included — the pending table alone would have let the second one in.
func TestMaxConnsRefusesBeyondTheCap(t *testing.T) {
	capRefusesTheSecondDial(t, ListenerConfig{MaxConns: 1})
	if _, err := NewListener(nil, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{MaxConns: -1}); err == nil {
		t.Error("NewListener accepted a negative MaxConns")
	}
}

// MaxConnsPerIP applies the same bound per source prefix: ports differ
// between the two dials, and an IPv4-mapped IPv6 address counts with its
// IPv4 form. Two IPv4 addresses stay apart, two IPv6 addresses of one /64
// share a key, and the next /64 has its own. (Proving the second address is
// admitted end to end is TestMaxConnsPerIPCountsEachFamilyApart's.)
func TestMaxConnsPerIPCountsBySourcePrefix(t *testing.T) {
	capRefusesTheSecondDial(t, ListenerConfig{MaxConnsPerIP: 1})
	if _, err := NewListener(nil, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{MaxConnsPerIP: -1}); err == nil {
		t.Error("NewListener accepted a negative MaxConnsPerIP")
	}

	lcfg, err := ListenerConfig{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	l := &Listener{lcfg: lcfg}
	key := func(ip string, port int) string { return l.connKey(&net.UDPAddr{IP: net.ParseIP(ip), Port: port}) }
	v4a, v4b := key("127.0.0.1", 1000), key("127.0.0.1", 2000)
	mapped, other := key("::ffff:127.0.0.1", 3000), key("127.0.0.2", 1000)
	if v4a != v4b {
		t.Errorf("two ports of one address key apart: %q vs %q", v4a, v4b)
	}
	if v4a != mapped {
		t.Errorf("an IPv4-mapped address keys apart from its IPv4 form: %q vs %q", mapped, v4a)
	}
	if v4a == other {
		t.Errorf("two addresses share the key %q", v4a)
	}
	if a, b := key("2001:db8:0:1::1", 1000), key("2001:db8:0:1:ffff::1", 1000); a != b {
		t.Errorf("two addresses of one /64 key apart: %q vs %q", a, b)
	}
	if a, b := key("2001:db8:0:1::1", 1000), key("2001:db8:0:2::1", 1000); a == b {
		t.Errorf("two neighbouring /64 share the key %q", a)
	}
}

// On a dual-stack socket 127.0.0.1 arrives IPv4-mapped and ::1 as IPv6: the
// two are different peers and each gets its own MaxConnsPerIP, while a
// second connection from 127.0.0.1 is still refused.
func TestMaxConnsPerIPCountsEachFamilyApart(t *testing.T) {
	if pc, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback}); err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	} else {
		_ = pc.Close()
	}
	spc, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(spc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{MaxConnsPerIP: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	port := spc.LocalAddr().(*net.UDPAddr).Port
	dial := func(ip net.IP) (*Conn, error) {
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip})
		if err != nil {
			t.Fatal(err)
		}
		return Dial(pc, &net.UDPAddr{IP: ip, Port: port}, ClientTLSForTest(), DefaultParameters())
	}
	v4, err := dial(net.IPv4(127, 0, 0, 1))
	if err != nil {
		t.Fatalf("the first Dial from 127.0.0.1: %v", err)
	}
	defer v4.Close()
	v6, err := dial(net.IPv6loopback)
	if err != nil {
		t.Fatalf("the Dial from ::1 while 127.0.0.1 is at the cap: %v", err)
	}
	defer v6.Close()
	if again, err := dial(net.IPv4(127, 0, 0, 1)); err == nil {
		_ = again.Close()
		t.Fatal("a second Dial from 127.0.0.1 was admitted over MaxConnsPerIP 1")
	}
}

// A negative bound, or a prefix longer than its address, is refused rather
// than turned into the default, and every faulty field is named in the one
// error, with its value and what zero means (docs/design/adr/0018).
func TestListenerConfigRefusesOutOfRangeValues(t *testing.T) {
	_, err := NewListener(nil, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{
		MaxPendingConns:  -1,
		HandshakeTimeout: -time.Second,
		MaxConns:         -2,
		MaxConnsPerIP:    -3,
		IPv4PrefixBits:   33,
		IPv6PrefixBits:   -1,
	})
	if err == nil {
		t.Fatal("NewListener accepted negative ListenerConfig values")
	}
	for _, want := range []string{
		"ListenerConfig.MaxPendingConns -1 is negative — zero means 1024",
		"ListenerConfig.HandshakeTimeout -1s is negative — zero means " + DefaultHandshakeTimeout.String(),
		"ListenerConfig.MaxConns -2 is negative — zero means no bound",
		"ListenerConfig.MaxConnsPerIP -3 is negative — zero means no bound",
		"ListenerConfig.IPv4PrefixBits 33 is outside 1..32 — zero means 32",
		"ListenerConfig.IPv6PrefixBits -1 is outside 1..128 — zero means 64",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
}

// Every bound added to ListenerConfig on 2026-10-18 refuses a negative value
// with an error naming its field, and the two keys refuse to be one; zero
// resolves to the documented defaults, and a zero key to a random one per
// listener. ListenerConfig stays comparable: the == below is the check.
func TestListenerConfigTuningAndKeys(t *testing.T) {
	refused := []struct {
		field string
		lcfg  ListenerConfig
	}{
		{"ConnQueueLen", ListenerConfig{ConnQueueLen: -1}},
		{"RetryTokenTTL", ListenerConfig{RetryTokenTTL: -time.Second}},
		{"MaxSpentRetryTokens", ListenerConfig{MaxSpentRetryTokens: -1}},
		{"TokenKey", ListenerConfig{TokenKey: [32]byte{1}, StatelessResetKey: [32]byte{1}}},
	}
	for _, r := range refused {
		_, err := NewListener(nil, ServerTLSForTest(t), DefaultParameters(), r.lcfg)
		if err == nil || !strings.Contains(err.Error(), "ListenerConfig."+r.field) {
			t.Errorf("%s: NewListener returned %v, want an error naming the field", r.field, err)
		}
	}

	if a, b := (ListenerConfig{}), (ListenerConfig{}); a != b {
		t.Error("two zero ListenerConfigs compare unequal")
	}
	given := ListenerConfig{StatelessResetKey: [32]byte{1}, TokenKey: [32]byte{2}}
	got, err := given.resolve()
	if err != nil {
		t.Fatal(err)
	}
	if got.ConnQueueLen != 64 || got.RetryTokenTTL != 10*time.Second || got.MaxSpentRetryTokens != 16384 {
		t.Errorf("zero resolved to ConnQueueLen %d, RetryTokenTTL %v, MaxSpentRetryTokens %d; want 64, 10s, 16384",
			got.ConnQueueLen, got.RetryTokenTTL, got.MaxSpentRetryTokens)
	}
	if got.StatelessResetKey != given.StatelessResetKey || got.TokenKey != given.TokenKey {
		t.Error("a given key was replaced")
	}
	r1, _ := ListenerConfig{}.resolve()
	r2, _ := ListenerConfig{}.resolve()
	var zero [32]byte
	if r1.StatelessResetKey == zero || r1.TokenKey == zero || r1.StatelessResetKey == r2.StatelessResetKey || r1.TokenKey == r2.TokenKey {
		t.Error("a zero key did not resolve to a fresh random one per listener")
	}

	pc := &capturePC{fuzzSink: fuzzSink{addr: "cfg:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{ConnQueueLen: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c := newConn(l.pc, fakeFuzzAddr("cfg:peer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	if err := l.prepareConn(c); err != nil {
		t.Fatal(err)
	}
	if cap(c.incoming) != 3 {
		t.Errorf("a connection's queue holds %d datagrams, want ConnQueueLen's 3", cap(c.incoming))
	}
}

// crashablePC is a server socket that can fall silent at once, as a process
// that dies does: after crash, nothing it is asked to write leaves.
type crashablePC struct {
	net.PacketConn
	crashed atomic.Bool
}

func (p *crashablePC) WriteTo(b []byte, addr net.Addr) (int, error) {
	if p.crashed.Load() {
		return len(b), nil
	}
	return p.PacketConn.WriteTo(b, addr)
}

// A listener restarted on the same address with the same StatelessResetKey
// answers a client of its previous life with a reset that client recognises
// (RFC 9000 §10.3.2), so the client ends at once with ErrStatelessReset
// instead of waiting out its idle timeout; restarted with a fresh key, the
// reset it sends is one the client cannot tell from noise.
func TestRestartedListenerResetsWithTheSameKey(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sameKey   bool
		wantReset bool
	}{
		{"same key", true, true},
		{"fresh key", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := ListenerConfig{StatelessResetKey: [32]byte{7}}
			raw, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			addr := raw.LocalAddr().(*net.UDPAddr)
			pc := &crashablePC{PacketConn: raw}
			l1, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), first)
			if err != nil {
				t.Fatal(err)
			}
			accepted := make(chan struct{})
			go func() {
				if _, err := l1.Accept(); err == nil {
					close(accepted)
				}
			}()
			clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			client, err := Dial(clientPC, addr, ClientTLSForTest(), DefaultParameters())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			select {
			case <-accepted:
			case <-time.After(10 * time.Second):
				t.Fatal("the handshake never completed")
			}

			// The crash: the first listener's goodbyes never leave, then a
			// second one takes the same address.
			pc.crashed.Store(true)
			_ = l1.Close()
			raw2, err := net.ListenUDP("udp", addr)
			if err != nil {
				t.Fatal(err)
			}
			second := ListenerConfig{}
			if tc.sameKey {
				second.StatelessResetKey = first.StatelessResetKey
			}
			l2, err := NewListener(raw2, ServerTLSForTest(t), DefaultParameters(), second)
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_ = client.Ping(ctx)
			select {
			case <-client.Done():
			case <-time.After(500 * time.Millisecond):
			}
			if gotReset := errors.Is(client.Err(), ErrStatelessReset); gotReset != tc.wantReset {
				t.Errorf("the client ended with %v; a recognised reset: %v, want %v", client.Err(), gotReset, tc.wantReset)
			}
		})
	}
}

// RFC 9000 §10.3.1: a datagram the listener cannot route may be a stateless
// reset from the peer of one of its connections — a client that lost its
// state and answers the server's packets with a reset ending in the token
// it was given. The listener compares before it answers with a reset of its
// own, and the connection ends now rather than at its idle timeout.
func TestListenerRecognisesAPeersStatelessReset(t *testing.T) {
	pc := &capturePC{fuzzSink: fuzzSink{addr: "listener:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	c := newConn(pc, fakeFuzzAddr("client:peer"), []byte("cli-cid0"), []byte("srv-cid0"), false, DefaultParameters())
	defer c.Close()
	token := [16]byte{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3}
	c.mu.Lock()
	c.peerCIDs = []peerCID{{seq: 0, cid: []byte("cli-cid0"), used: true, token: token}}
	c.mu.Unlock()
	l.mu.Lock()
	l.routeLocked("srv-cid0", c)
	l.mu.Unlock()

	// A short-header datagram naming an identifier nobody issued, ending in
	// the token: §10.3's reset, as the client sends it.
	reset := make([]byte, 60)
	reset[0] = 0x40
	copy(reset[1:], "made-up0")
	copy(reset[len(reset)-16:], token[:])
	l.dispatch(append([]byte(nil), reset...), fakeFuzzAddr("client:peer"), l.pc, 0)

	if err := c.Err(); !errors.Is(err, ErrStatelessReset) {
		t.Fatalf("the connection's Err = %v after its peer's stateless reset, want ErrStatelessReset", err)
	}
	if got := pc.taken(); len(got) != 0 {
		t.Errorf("the listener answered the peer's reset with %d datagrams of its own, want none", len(got))
	}
}
