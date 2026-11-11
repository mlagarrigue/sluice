package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// A writer parked on the congestion window when the server refuses 0-RTT
// has charged its frame to the connection's flow-control account, which the
// refusal starts over (RFC 9001 §4.6.2). Its refund, once the write fails
// with ErrEarlyDataRejected, must not come out of the new account: the
// counter is unsigned, so it wrapped, and no write on the connection ever
// found credit again. End to end against this package's own listener — a
// conformant server that refuses the 0-RTT of a ticket whose 0-RTT it
// already admitted once (earlyGate, RFC 9846 §8).
func TestRefusedZeroRTTLeavesCreditForNewStreams(t *testing.T) {
	serverPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{EarlyData: true})
	if err != nil {
		_ = serverPC.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go echoEachStream(l)

	lru := tls.NewLRUClientSessionCache(4)
	cache := &ticketLanded{ClientSessionCache: lru, stored: make(chan struct{}, 1)}
	clientCfg := ClientTLSForTest()
	clientCfg.ClientSessionCache = cache
	listen := func() net.PacketConn {
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pc.Close() })
		return pc
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	first, err := Dial(ctx, listen(), l.Addr(), clientCfg, DefaultConfig())
	if err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	select {
	case <-cache.stored:
	case <-ctx.Done():
		t.Fatal("no session ticket reached the client")
	}
	_ = first.Close()
	ticket, ok := lru.Get(clientCfg.ServerName)
	if !ok || ticket == nil {
		t.Fatal("no ticket in the cache")
	}

	// Spend the ticket's 0-RTT once: accepted.
	lru.Put(clientCfg.ServerName, ticket)
	spend, err := DialEarly(ctx, listen(), l.Addr(), clientCfg, DefaultConfig())
	if err != nil {
		t.Fatalf("spending DialEarly: %v", err)
	}
	select {
	case <-spend.HandshakeComplete():
	case <-ctx.Done():
		t.Fatal("spending handshake did not complete")
	}
	spend.mu.Lock()
	spentRejected := spend.earlyRejected
	spend.mu.Unlock()
	_ = spend.Close()
	if spentRejected {
		t.Fatal("precondition lost: the first 0-RTT was refused already")
	}

	// The same ticket again: the server refuses its 0-RTT.
	lru.Put(clientCfg.ServerName, ticket)
	held := &heldReads{PacketConn: listen(), release: make(chan struct{})}
	c, err := DialEarly(ctx, held, l.Addr(), clientCfg, DefaultConfig())
	if err != nil {
		t.Fatalf("DialEarly: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	select {
	case <-c.HandshakeComplete():
		t.Fatal("precondition lost: no 0-RTT attempted")
	default:
	}
	early, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("opening a 0-RTT stream: %v", err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := early.Write(make([]byte, 64<<10))
		wrote <- err
	}()
	parked := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		c.mu.Lock()
		full := !c.cc.canSend(1300)
		charged := c.connSend.sent > 0
		c.mu.Unlock()
		if full && charged {
			parked = true
			break
		}
	}
	if !parked {
		t.Fatal("precondition lost: the 0-RTT writer never filled the window")
	}
	select {
	case err := <-wrote:
		t.Fatalf("precondition lost: the 0-RTT write returned before the refusal: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(held.release)
	select {
	case <-c.HandshakeComplete():
	case <-ctx.Done():
		t.Fatal("the handshake did not complete")
	}
	select {
	case err := <-wrote:
		if !errors.Is(err, ErrEarlyDataRejected) {
			t.Fatalf("precondition lost: the parked write ended with %v, want ErrEarlyDataRejected", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("precondition lost: the parked 0-RTT write never returned")
	}
	c.mu.Lock()
	rejected := c.earlyRejected
	sent, maxData := c.connSend.sent, c.connSend.max
	c.mu.Unlock()
	if !rejected {
		t.Fatal("precondition lost: the server accepted the replayed 0-RTT")
	}
	t.Logf("after the refusal: connSend.sent=%d max=%d", sent, maxData)

	s, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("opening a 1-RTT stream: %v", err)
	}
	_ = s.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := s.WriteFinal([]byte("after")); err != nil {
		t.Fatalf("a 1-RTT write after the refused 0-RTT failed: %v (connSend.sent=%d max=%d)", err, sent, maxData)
	}
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(s)
	if err != nil || string(got) != "after" {
		t.Fatalf("echo = %q, %v", got, err)
	}
}

// A 0-RTT packet still gathered (writersIn > 1) when the server's refusal
// arrives must not leave again under 1-RTT: RFC 9001 §4.6.2 has the client
// reset every stream, and the stream the application opens next takes the
// same identifier, so the refused bytes would reach the server at the new
// stream's offsets.
func TestRefusedZeroRTTGatheredPacketDoesNotLeaveInOneRTT(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "refused:self"})
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := assembleConn(tap, fakeFuzzAddr("refused:peer"), dcid, scid, true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid
	c.wantsEarly = true
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}
	params := TransportParameters{
		InitialMaxData:          1 << 20,
		InitialMaxStreamDataUni: 1 << 16,
		InitialMaxStreamsUni:    8,
		ActiveConnIDLimit:       4,
	}
	c.remembered = params
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}
	a, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	oldID := a.ID()

	marker := bytes.Repeat([]byte{0xA5}, 300)
	c.writersIn.Add(1) // a second writer counted on its way to mu
	if _, err := a.Write(marker); err != nil {
		t.Fatalf("0-RTT write: %v", err)
	}
	c.mu.Lock()
	pending := c.dgramPend != nil && c.dgramPend.early
	c.mu.Unlock()
	if !pending {
		t.Fatal("precondition lost: the 0-RTT packet is not pending")
	}

	// The server refuses 0-RTT; crypto/tls reports it before the 1-RTT keys.
	if _, err := c.handleTLSEvent(tls.QUICEvent{Kind: tls.QUICRejectedEarlyData}); err != nil {
		t.Fatalf("rejection: %v", err)
	}
	c.mu.Lock()
	c.peerParams, c.hasPeerParams = params, true
	c.connSend.grant(params.InitialMaxData)
	c.peerMaxStreamsUni = params.InitialMaxStreamsUni
	c.mu.Unlock()
	if _, err := c.handleTLSEvent(tls.QUICEvent{
		Kind: tls.QUICSetWriteSecret, Level: tls.QUICEncryptionLevelApplication,
		Suite: tls.TLS_AES_128_GCM_SHA256, Data: make([]byte, 32),
	}); err != nil {
		t.Fatalf("1-RTT write secret: %v", err)
	}
	c.writersIn.Add(-1)

	// The application opens its stream again, as ErrEarlyDataRejected asks.
	b, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("fresh")); err != nil {
		t.Fatalf("1-RTT write: %v", err)
	}
	c.mu.Lock()
	c.retransmitLocked()
	sp := &c.spaces[spaceApplication]
	var sent, queued int
	for _, r := range sp.sent.packets {
		if bytes.Contains(r.payload, marker[:64]) {
			sent++
		}
	}
	for _, r := range sp.retrans {
		if bytes.Contains(r, marker[:64]) {
			queued++
		}
	}
	c.mu.Unlock()
	if sent > 0 || queued > 0 {
		t.Fatalf("after the refusal, the refused stream's bytes are recorded as sent in %d 1-RTT packets and queued in %d payloads; reopened stream id %d, refused stream id %d",
			sent, queued, b.ID(), oldID)
	}
}
