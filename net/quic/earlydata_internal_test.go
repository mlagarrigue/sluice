package quic

import (
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
