package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// The gate admits a ticket of its own instance once, within the window; it
// refuses a replay, an old ticket, another instance's, a ticket without its
// entry, and — full — the next ticket rather than forgetting one.
func TestEarlyGateAdmitsATicketOnce(t *testing.T) {
	now := time.Now()
	g := newEarlyGate(time.Minute, 2)
	ticket := [][]byte{g.ticketExtra(now)}
	if !admitOnce(g, ticket, now.Add(time.Second)) {
		t.Fatal("a fresh ticket of this instance was refused")
	}
	if admitOnce(g, ticket, now.Add(2*time.Second)) {
		t.Error("the same ticket was admitted twice")
	}
	if admitOnce(g, [][]byte{g.ticketExtra(now.Add(-time.Minute))}, now) {
		t.Error("a ticket one window old was admitted")
	}
	if admitOnce(g, [][]byte{g.ticketExtra(now.Add(time.Second))}, now) {
		t.Error("a ticket issued in the future was admitted")
	}
	other := newEarlyGate(time.Minute, 2)
	if admitOnce(g, [][]byte{other.ticketExtra(now)}, now) {
		t.Error("another instance's ticket was admitted")
	}
	if admitOnce(g, nil, now) || admitOnce(g, [][]byte{[]byte("unrelated")}, now) {
		t.Error("a ticket without this package's entry was admitted")
	}

	if !admitOnce(g, [][]byte{g.ticketExtra(now)}, now) {
		t.Fatal("a second fresh ticket was refused")
	}
	if admitOnce(g, [][]byte{g.ticketExtra(now)}, now) {
		t.Error("a full register admitted a third ticket; it must refuse, not forget one")
	}
	// A window later the first entries expire, and room comes back.
	later := now.Add(2 * time.Minute)
	if !admitOnce(g, [][]byte{g.ticketExtra(later)}, later) {
		t.Error("the register did not free its expired entries")
	}
}

// RFC 9000 §12.4, Table 3: ACK, CRYPTO, NEW_TOKEN, PATH_RESPONSE and
// HANDSHAKE_DONE in a 0-RTT packet are a PROTOCOL_VIOLATION; a STREAM frame
// is what 0-RTT is for.
func TestZeroRTTRefusesTheFramesTable3Forbids(t *testing.T) {
	forbidden := map[string][]byte{
		"ACK":            appendFrame(nil, frameACK, 0, 0, 0, 0),
		"CRYPTO":         cryptoFrame(0, "x"),
		"NEW_TOKEN":      append(appendFrame(nil, frameNewToken, 1), 'x'),
		"PATH_RESPONSE":  append([]byte{framePathResponse}, make([]byte, 8)...),
		"HANDSHAKE_DONE": {frameHandshakeDone},
	}
	for name, payload := range forbidden {
		c := boundsConn(t, false)
		err := c.framesIn(spaceApplication, true, payload, time.Now())
		te, ok := errors.AsType[*transportError](err)
		// Several of these are refused anyway on a server, by the rule of
		// their own frame; the message says the 0-RTT table is what fired.
		if !ok || te.code != transportProtocolViolation || !strings.Contains(err.Error(), "in a 0-RTT packet") {
			t.Errorf("%s in 0-RTT returned %v, want PROTOCOL_VIOLATION from the 0-RTT table", name, err)
		}
	}
	c := boundsConn(t, false)
	if err := c.framesIn(spaceApplication, true, streamFrame(nil, 0, 0, "early", false), time.Now()); err != nil {
		t.Errorf("a STREAM frame in 0-RTT was refused: %v", err)
	}
}

// A client never receives 0-RTT, and a server without the early secret reads
// none: the packet is dropped before any key is tried, and a server keeps it
// only while the secret may still come.
func TestZeroRTTPacketsWithoutTheEarlySecret(t *testing.T) {
	zeroRTT := func(dcid []byte) []byte {
		// A long header of type 0-RTT (§17.2.3): form and fixed bits, type
		// 1, version 1, the identifiers, a length, and bytes that never
		// authenticate.
		b := []byte{0xc0 | packetZeroRTT<<4, 0, 0, 0, 1, byte(len(dcid))}
		b = append(b, dcid...)
		b = append(b, 0)
		b = varint.Append(b, 24)
		return append(b, make([]byte, 24)...)
	}
	client := boundsConn(t, true)
	if err := client.receiveAt(zeroRTT(client.scid), fakeFuzzAddr("bounds:peer"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if client.Stats().DiscardedPackets != 1 {
		t.Errorf("a client counted %d discards for a 0-RTT packet, want 1", client.Stats().DiscardedPackets)
	}

	server := boundsConn(t, false)
	if err := server.receiveAt(zeroRTT(server.scid), fakeFuzzAddr("bounds:peer"), time.Now()); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	held := len(server.deferredEarly)
	server.mu.Unlock()
	if held != 1 {
		t.Errorf("a server before its keys held %d 0-RTT packets, want 1", held)
	}
}

// A server whose 0-RTT was accepted is handed to Accept only once it can
// write in 1-RTT: crypto/tls installs the early read secret first, and an
// application that writes the moment Accept returns — an HTTP/3 server's
// SETTINGS — found nothing to seal its packets with, and the connection
// died with it.
func TestEarlyAcceptanceWaitsForTheWriteKeys(t *testing.T) {
	secret := make([]byte, 32)
	c := &Conn{cc: newNewReno(1200), earlyAccepted: make(chan struct{})}
	c.early, c.earlyTicket = eligibleTicket(t)
	event := func(kind tls.QUICEventKind, level tls.QUICEncryptionLevel) {
		t.Helper()
		if _, err := c.handleTLSEvent(tls.QUICEvent{Kind: kind, Level: level, Suite: tls.TLS_AES_128_GCM_SHA256, Data: secret}); err != nil {
			t.Fatal(err)
		}
	}
	accepted := func() bool {
		select {
		case <-c.earlyAccepted:
			return true
		default:
			return false
		}
	}
	event(tls.QUICSetReadSecret, tls.QUICEncryptionLevelEarly)
	event(tls.QUICSetWriteSecret, tls.QUICEncryptionLevelHandshake)
	if accepted() {
		t.Fatal("the connection was released for Accept with no 1-RTT write keys")
	}
	event(tls.QUICSetWriteSecret, tls.QUICEncryptionLevelApplication)
	if !accepted() {
		t.Fatal("the 1-RTT write keys are in and the connection was not released")
	}
	event(tls.QUICSetReadSecret, tls.QUICEncryptionLevelApplication) // a second release would close twice
}

// admitOnce is what the server does for a client that offers 0-RTT: check
// the ticket at resumption, spend it when the 0-RTT is accepted.
func admitOnce(g *earlyGate, extra [][]byte, now time.Time) bool {
	id, ok := g.eligible(extra, now)
	return ok && g.take(id, now)
}

// Checking a ticket spends nothing — crypto/tls reports every resumption,
// 0-RTT offered or not, and one without it must leave the ticket's 0-RTT
// intact — and of two connections that found it eligible, one takes it.
func TestEarlyGateChecksWithoutSpending(t *testing.T) {
	now := time.Now()
	g := newEarlyGate(time.Minute, 4)
	ticket := [][]byte{g.ticketExtra(now)}
	id1, ok1 := g.eligible(ticket, now)
	id2, ok2 := g.eligible(ticket, now)
	if !ok1 || !ok2 {
		t.Fatal("a check spent the ticket")
	}
	if !g.take(id1, now) {
		t.Fatal("the first take was refused")
	}
	if g.take(id2, now) {
		t.Error("the second of two racing connections took the ticket too")
	}
	if _, ok := g.eligible(ticket, now); ok {
		t.Error("a spent ticket is still eligible")
	}
}

// eligibleTicket is a gate and the identifier of a ticket it finds eligible,
// as a server holds them once a resuming client has offered 0-RTT.
func eligibleTicket(t *testing.T) (*earlyGate, []byte) {
	t.Helper()
	now := time.Now()
	g := newEarlyGate(time.Minute, 4)
	id, ok := g.eligible([][]byte{g.ticketExtra(now)}, now)
	if !ok {
		t.Fatal("a fresh ticket is not eligible")
	}
	return g, id
}

// RFC 9001 §4.6.2: once the server refused 0-RTT, an acknowledgement of a
// packet sent in it is PROTOCOL_VIOLATION. The same ACK without the refusal
// names packets merely forgotten, and passes.
func TestAnAckOfRefusedZeroRTTIsAViolation(t *testing.T) {
	for _, refused := range []bool{true, false} {
		c := boundsConn(t, true)
		c.mu.Lock()
		sp := &c.spaces[spaceApplication]
		sp.nextPN = 3
		if refused {
			sp.sent.refusedBelow = 3
		}
		c.mu.Unlock()
		ranges := varint.Append(varint.Append(nil, 0), 1) // no further range; the first covers packets 0 and 1
		err := c.handleAck(spaceApplication, Frame{Type: frameACK, largest: 1, Data: ranges}, time.Now())
		te, isViolation := errors.AsType[*transportError](err)
		isViolation = isViolation && te.code == transportProtocolViolation
		if isViolation != refused {
			t.Errorf("refused %v: handleAck = %v", refused, err)
		}
	}
}

// RFC 9000 §7.4.1: "Remembered transport parameters apply to the new
// connection until the handshake completes and the client starts sending
// 1-RTT packets. Once the handshake completes, the client uses the transport
// parameters established in the handshake."
//
// The server's parameters arrive in EncryptedExtensions, long before this
// client's 1-RTT write keys — those come after the rest of the server's
// flight and this client's own Finished — and until then every application
// packet is still a 0-RTT packet. Until 2026-10-26 the raise happened the
// moment the parameters parsed, and 300 bytes of stream data left in real
// 0-RTT packets against a remembered limit of 100. The window is open
// whenever the server's flight takes more than one packet, which a
// certificate chain and §8.1's amplification limit make ordinary.
func TestZeroRTTWritesStayUnderRememberedLimits(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "held08:self"})
	dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	c := assembleConn(tap, fakeFuzzAddr("held08:peer"), dcid, scid, true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid

	const remembered = 100
	c.remembered = TransportParameters{
		InitialMaxData:          remembered,
		InitialMaxStreamDataUni: remembered,
		InitialMaxStreamsUni:    4,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}

	// The server's own parameters, far wider than the ticket's. §7.4.1 has
	// them no smaller than the remembered ones when 0-RTT is accepted, so
	// holding the raise back can only ever withhold room, never credit
	// already spent.
	srv := DefaultParameters()
	srv.initialSourceCID = dcid
	srv.originalDCID = dcid
	if err := c.handlePeerParameters(appendParameters(nil, srv)); err != nil {
		t.Fatalf("server parameters: %v", err)
	}

	c.mu.Lock()
	hasOneRTT, early := c.spaces[spaceApplication].sealer != nil, c.earlySealer != nil
	c.mu.Unlock()
	if hasOneRTT || !early {
		t.Fatalf("precondition lost: 1-RTT sealer %v, early sealer %v", hasOneRTT, early)
	}

	s, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	// In a goroutine, because the write is meant to run out of credit and
	// park: there is no timer loop here to fire a write deadline, and the
	// whole point is that the remembered limit stops it.
	wrote := make(chan int, 1)
	go func() {
		n, _ := s.Write(make([]byte, 3*remembered))
		wrote <- n
	}()
	settled := 0
	for range 2000 {
		c.mu.Lock()
		off := int(s.writeOff)
		c.mu.Unlock()
		if off == settled && off > 0 {
			break
		}
		settled = off
		time.Sleep(time.Millisecond)
	}
	early0RTT := tap.recorded()
	for _, w := range early0RTT {
		for _, typ := range packetTypes(t, w.data) {
			if typ != packetZeroRTT {
				t.Fatalf("a packet of type %#x left; this test only reasons about 0-RTT packets", typ)
			}
		}
	}
	if settled > remembered {
		c.mu.Lock()
		connMax := c.connSend.max
		c.mu.Unlock()
		t.Errorf("the client put %d bytes of stream data in 0-RTT packets against a remembered limit of %d (connSend.max is %d, the server's value)",
			settled, remembered, connMax)
	}

	// The first 1-RTT write keys take over: the server's value applies from
	// there on, which is the other half of §7.4.1, and the parked writer
	// finishes under it.
	if err := c.install(tls.QUICEncryptionLevelApplication, tls.TLS_AES_128_GCM_SHA256, make([]byte, 32), true); err != nil {
		t.Fatalf("1-RTT write keys: %v", err)
	}
	select {
	case n := <-wrote:
		if n != 3*remembered {
			t.Errorf("the write finished at %d bytes, want the whole %d once the server's limit applied", n, 3*remembered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write never finished after the server's limits took over")
	}
	c.mu.Lock()
	connMax, held := c.connSend.max, c.limitsHeld
	c.mu.Unlock()
	if held {
		t.Error("the server's limits are still held after the 1-RTT write keys landed")
	}
	if connMax != DefaultParameters().InitialMaxData {
		t.Errorf("connSend.max is %d after the 1-RTT keys, want the server's %d", connMax, DefaultParameters().InitialMaxData)
	}
}
