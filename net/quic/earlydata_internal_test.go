package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"
	"unsafe"

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

// RFC 9000 §12.4, Table 3: ACK, CRYPTO, NEW_TOKEN, PATH_RESPONSE,
// RETIRE_CONNECTION_ID and HANDSHAKE_DONE in a 0-RTT packet are a
// PROTOCOL_VIOLATION — RETIRE_CONNECTION_ID by the verified erratum EID 7365,
// which corrects its row to ___1 as §12.5 has it; a STREAM frame is what
// 0-RTT is for.
func TestZeroRTTRefusesTheFramesTable3Forbids(t *testing.T) {
	forbidden := map[string][]byte{
		"ACK":                  appendFrame(nil, frameACK, 0, 0, 0, 0),
		"CRYPTO":               cryptoFrame(0, "x"),
		"NEW_TOKEN":            append(appendFrame(nil, frameNewToken, 1), 'x'),
		"PATH_RESPONSE":        append([]byte{framePathResponse}, make([]byte, 8)...),
		"RETIRE_CONNECTION_ID": appendFrame(nil, frameRetireConnectionID, 0),
		"HANDSHAKE_DONE":       {frameHandshakeDone},
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
// only while the secret may still come — which on a server that does not
// admit 0-RTT it never does, so that one keeps nothing.
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

	for _, admits := range []bool{true, false} {
		server := boundsConn(t, false)
		if admits {
			server.mu.Lock()
			server.early = newEarlyGate(0, 0)
			server.mu.Unlock()
		}
		if err := server.receiveAt(zeroRTT(server.scid), fakeFuzzAddr("bounds:peer"), time.Now()); err != nil {
			t.Fatal(err)
		}
		server.mu.Lock()
		held := len(server.deferredEarly)
		server.mu.Unlock()
		if want := map[bool]int{true: 1, false: 0}[admits]; held != want {
			t.Errorf("a server that admits 0-RTT: %v; before its keys it held %d 0-RTT packets, want %d", admits, held, want)
		}
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

// admitOnce is what the server does for a client that resumes: check the
// ticket when crypto/tls asks, which reserves it.
func admitOnce(g *earlyGate, extra [][]byte, now time.Time) bool {
	_, ok := g.eligible(extra, now)
	return ok
}

// Checking a ticket reserves it: of two connections that present the same
// one — a replay racing the original — the first is the only one eligible,
// and crypto/tls answers the second without early_data. A reservation the
// connection does not use is given back by release, and the ticket's 0-RTT
// is intact; one the 0-RTT confirmed stays spent whatever release says.
// Until 2026-10-27 the check spent nothing and the race was settled later,
// with early_data already announced to the loser.
func TestEarlyGateReservesAtTheCheck(t *testing.T) {
	now := time.Now()
	g := newEarlyGate(time.Minute, 4)
	ticket := [][]byte{g.ticketExtra(now)}
	id1, ok1 := g.eligible(ticket, now)
	if !ok1 {
		t.Fatal("a fresh ticket is not eligible")
	}
	if _, ok := g.eligible(ticket, now); ok {
		t.Fatal("the second of two racing connections found the ticket eligible too")
	}
	// The first connection served a resumption without 0-RTT: the ticket
	// is released, and a later 0-RTT with it is accepted — once.
	g.release(id1)
	id3, ok := g.eligible(ticket, now)
	if !ok {
		t.Fatal("a released ticket is not eligible again")
	}
	g.release(id3) // released twice: harmless
	g.release(id3)
	if _, ok := g.eligible(ticket, now); !ok {
		t.Fatal("a ticket released twice is not eligible again")
	}
	if _, ok := g.eligible(ticket, now); ok {
		t.Error("a reserved ticket is still eligible")
	}
}

// A full register refuses the next ticket when crypto/tls asks, not after
// it has announced early_data: the ticket is reserved by the check. Until
// 2026-10-27 the check ignored the register's capacity, and the ticket was
// spent only once the 0-RTT was accepted; full, the register then refused
// it with early_data already in the EncryptedExtensions, the connection
// kept no early key, and the client's 0-RTT packets were dropped — a loss
// to it, resent in 1-RTT after a probe timeout — instead of the TLS refusal
// ADR 0019 promises. A release gives the slot back.
func TestAFullTicketRegisterRefusesEarlyDataInTLS(t *testing.T) {
	g := newEarlyGate(10*time.Minute, 1) // room for one admitted ticket
	now := time.Now()
	first, second := g.ticketExtra(now), g.ticketExtra(now)
	if _, ok := g.eligible([][]byte{first}, now); !ok {
		t.Fatal("the first ticket is not eligible; the case under test is not reached")
	}
	id2, ok := g.eligible([][]byte{second}, now)
	if ok {
		t.Fatal("eligible said yes to a second ticket in a register of one: crypto/tls announces early_data for a 0-RTT the register cannot admit")
	}
	g.release(id2) // nothing was reserved: nothing to give back, and no slot freed
	if _, ok := g.eligible([][]byte{second}, now); ok {
		t.Fatal("a refused ticket's release freed a slot it never held")
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
		InitialMaxStreamDataUni: 10 * remembered, // wide: only the connection credit can stop the write
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

// The per-stream limit is held back with the connection credit and the
// stream counts: RFC 9000 §7.4.1, "A server might provide larger initial
// stream flow control limits for streams than the remembered values that a
// client applies when sending 0-RTT", under the same MUST. Until 2026-10-27
// handlePeerParameters raised every open stream's credit to the server's
// value the moment its parameters parsed, and a stream born in the window
// took its credit from c.peerParams, already the server's: 300 bytes of
// stream data left in 0-RTT packets against a remembered 100 (measured),
// which a strict server may close as PROTOCOL_VIOLATION. The connection
// credit in TestZeroRTTWritesStayUnderRememberedLimits does not see it —
// here it is wide, so only the per-stream limit can stop the write.
func TestZeroRTTStreamWritesStayUnderTheRememberedStreamLimit(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "held04:self"})
	dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	c := assembleConn(tap, fakeFuzzAddr("held04:peer"), dcid, scid, true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid

	const perStream = 100
	c.remembered = TransportParameters{
		InitialMaxData:          10 * perStream, // wide: only the per-stream limit can stop the write
		InitialMaxStreamDataUni: perStream,
		InitialMaxStreamsUni:    4,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}
	// One stream open before the server's parameters, one born after them:
	// the first is raised by the loop over open streams, the second takes
	// its credit at birth. Both must wait.
	before, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatalf("OpenUniStream before the server's parameters: %v", err)
	}
	srv := DefaultParameters() // InitialMaxStreamDataUni 1<<16, far past the ticket's
	srv.initialSourceCID = dcid
	srv.originalDCID = dcid
	if err := c.handlePeerParameters(appendParameters(nil, srv)); err != nil {
		t.Fatalf("server parameters: %v", err)
	}
	after, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatalf("OpenUniStream after the server's parameters: %v", err)
	}
	c.mu.Lock()
	hasOneRTT, early := c.spaces[spaceApplication].sealer != nil, c.earlySealer != nil
	beforeMax, afterMax := before.sendQ.max, after.sendQ.max
	c.mu.Unlock()
	if hasOneRTT || !early {
		t.Fatalf("precondition lost: 1-RTT sealer %v, early sealer %v", hasOneRTT, early)
	}
	if beforeMax != perStream || afterMax != perStream {
		t.Errorf("per-stream credit while 0-RTT packets are still written: %d on the stream opened before the server's parameters, %d on the one opened after, want the remembered %d on both",
			beforeMax, afterMax, perStream)
	}

	wrote := make(chan int, 1)
	go func() {
		n, _ := after.Write(make([]byte, 3*perStream))
		wrote <- n
	}()
	settled := 0
	for range 2000 {
		c.mu.Lock()
		off := int(after.writeOff)
		c.mu.Unlock()
		if off == settled && off > 0 {
			break
		}
		settled = off
		time.Sleep(time.Millisecond)
	}
	for _, w := range tap.recorded() {
		for _, typ := range packetTypes(t, w.data) {
			if typ != packetZeroRTT {
				t.Fatalf("a packet of type %#x left; this test only reasons about 0-RTT packets", typ)
			}
		}
	}
	if settled > perStream {
		t.Errorf("the client put %d bytes of stream data in 0-RTT packets against a remembered per-stream limit of %d", settled, perStream)
	}

	// The first 1-RTT write keys take over: the server's per-stream value
	// applies to both streams, and the parked writer finishes under it.
	if err := c.install(tls.QUICEncryptionLevelApplication, tls.TLS_AES_128_GCM_SHA256, make([]byte, 32), true); err != nil {
		t.Fatalf("1-RTT write keys: %v", err)
	}
	select {
	case n := <-wrote:
		if n != 3*perStream {
			t.Errorf("the write finished at %d bytes, want the whole %d once the server's limit applied", n, 3*perStream)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write never finished after the server's limits took over")
	}
	c.mu.Lock()
	beforeMax, afterMax = before.sendQ.max, after.sendQ.max
	c.mu.Unlock()
	if want := srv.InitialMaxStreamDataUni; beforeMax != want || afterMax != want {
		t.Errorf("per-stream credit after the 1-RTT keys: %d and %d, want the server's %d on both", beforeMax, afterMax, want)
	}
}

// RFC 9001 §4.9.3: a server "MAY discard 0-RTT keys as soon as it receives
// a 1-RTT packet", and this one does — its first authenticated 1-RTT
// packet drops the 0-RTT opener and whatever 0-RTT packets still wait for
// it; a 0-RTT packet reordered behind is resent by the client in 1-RTT.
func TestTheFirst1RTTPacketDropsThe0RTTKeys(t *testing.T) {
	c := newConn(&recordingPC{addr: fakeFuzzAddr("drop0rtt:self")}, fakeFuzzAddr("drop0rtt:peer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	installAppSealer(t, c)
	opener, err := newPacketOpener(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.earlyOpener = opener
	c.deferredEarly = append(c.deferredEarly, deferredPacket{raw: []byte{0xd0}, at: time.Now()})
	c.mu.Unlock()

	if err := c.processPacket(spaceApplication, false, 0, []byte{framePing}, time.Now()); err != nil {
		t.Fatalf("a 1-RTT PING: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.earlyOpener != nil || c.deferredEarly != nil {
		t.Fatalf("after the first 1-RTT packet: 0-RTT opener kept %v, %d 0-RTT packets still waiting; want both dropped", c.earlyOpener != nil, len(c.deferredEarly))
	}
}

// A server that admits 0-RTT and then had it refused for this connection —
// no early secret, the 1-RTT keys in — holds the 0-RTT packets that came
// before the ClientHello was read, and its first 1-RTT packet frees them:
// they wait for a secret that will never come. Until 2026-10-28 both ways
// out of the queue required the 0-RTT opener, and they stayed until the
// connection ended.
func TestTheFirst1RTTPacketDropsRefused0RTTPackets(t *testing.T) {
	c := newConn(&recordingPC{addr: fakeFuzzAddr("refuse0rtt:self")}, fakeFuzzAddr("refuse0rtt:peer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.early = newEarlyGate(0, 0)
	for i := range 3 {
		pkt := &rxPacket{space: spaceApplication, early: true, raw: []byte{0xd0, byte(i)}}
		if v, err := c.openPacket(pkt, time.Now()); err != nil || v != rxSkip {
			t.Fatalf("0-RTT packet %d before the early secret: (%v, %v), want held", i, v, err)
		}
	}
	installAppSealer(t, c)
	if err := c.processPacket(spaceApplication, false, 0, []byte{framePing}, time.Now()); err != nil {
		t.Fatalf("a 1-RTT PING: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.earlyOpener != nil || c.deferredEarly != nil {
		t.Fatalf("after the first 1-RTT packet, 0-RTT refused: %d 0-RTT packets still held", len(c.deferredEarly))
	}
}

// The 0-RTT packets that arrive before the early secret wait for it, at
// most sixteen: the one memory bound of that path. A peer sending more
// before its ClientHello is read loses the rest, which its 1-RTT resends.
func TestDeferred0RTTPacketsAreBounded(t *testing.T) {
	c := newConn(&recordingPC{addr: fakeFuzzAddr("defer0rtt:self")}, fakeFuzzAddr("defer0rtt:peer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.early = newEarlyGate(0, 0) // the queue exists only where 0-RTT is admitted
	for i := range 40 {
		pkt := &rxPacket{space: spaceApplication, early: true, raw: []byte{0xd0, byte(i)}}
		if v, err := c.openPacket(pkt, time.Now()); err != nil || v != rxSkip {
			t.Fatalf("0-RTT packet %d before the early secret: (%v, %v), want held and skipped", i, v, err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.deferredEarly); n != 16 {
		t.Fatalf("%d 0-RTT packets held before the early secret, want the bound of 16", n)
	}
}

// RFC 9846 §8: 0-RTT is accepted at most once per ticket. The ticket's age
// is judged on the wall clock — its time of issue is a Unix timestamp, so
// that it survives a restart — while the spent entry expired a window
// after the admission on the monotonic clock that time.Now carries. A wall
// clock stepped back after an admission left the ticket young after its
// entry was gone, and the same ticket was admitted again. The entry now
// lives on the ticket's clock: it carries no monotonic reading and expires
// with the ticket, at its time of issue plus the window. The entry's clock
// is checked, then the replay itself, at an instant whose monotonic reading
// is a window ahead of its wall reading (monotonicAhead): the ticket is
// young on the wall clock, and an entry timed on the monotonic one would
// be gone.
func TestASpentTicketLivesOnTheTicketsClock(t *testing.T) {
	const window = 10 * time.Minute
	now := time.Now()
	issued := now.Add(-time.Minute)
	g := newEarlyGate(window, 4)
	ticket := [][]byte{g.ticketExtra(issued)}
	id, ok := g.eligible(ticket, now)
	if !ok {
		t.Fatal("a fresh ticket is not eligible")
	}
	exp := g.spent.byID[string(id)]
	if want := time.Unix(0, issued.UnixNano()).Add(window); exp != exp.Round(0) || !exp.Equal(want) {
		t.Errorf("the spent entry expires at %v (monotonic reading kept: %v), want the ticket's own expiry %v on the wall clock",
			exp, exp != exp.Round(0), want)
	}

	// A second later on the wall clock, a window and a second later on the
	// monotonic one: the wall clock was stepped back a window meanwhile.
	// The ticket is a minute and a second old, and its entry must still be
	// there.
	later := monotonicAhead(t, now.Add(time.Second), window)
	if admitOnce(g, ticket, later) {
		t.Error("the ticket was admitted a second time after the wall clock stepped back")
	}
}

// monotonicAhead returns at with its monotonic reading moved d further and
// its wall reading unchanged: what time.Now returns once the wall clock has
// been stepped back by d. No exported function builds such an instant —
// Add moves both readings, Round(0) drops the monotonic one — so the
// reading is written in place, after the layout of src/time/time.go (wall
// uint64, ext int64 holding the monotonic nanoseconds, loc). A Go release
// that changes the layout fails the checks here, rather than leaving the
// test it serves green for nothing.
func monotonicAhead(t *testing.T, at time.Time, d time.Duration) time.Time {
	t.Helper()
	type layout struct {
		wall uint64
		ext  int64
		loc  *time.Location
	}
	if !strings.Contains(at.String(), " m=") || unsafe.Sizeof(at) != unsafe.Sizeof(layout{}) {
		t.Fatal("no monotonic reading to move, or time.Time is no longer {wall, ext, loc}")
	}
	ahead := at
	(*layout)(unsafe.Pointer(&ahead)).ext += int64(d) //nolint:gosec // G103: the layout is checked before and after
	if ahead.Sub(at) != d || !ahead.Round(0).Equal(at.Round(0)) {
		t.Fatalf("moving the monotonic reading by %v moved it by %v and the wall reading by %v: time.Time's layout changed",
			d, ahead.Sub(at), ahead.Round(0).Sub(at.Round(0)))
	}
	return ahead
}

// A Retry restarts the handshake and drops the 0-RTT that came before it:
// the server kept no state. RFC 9000 §17.2.5.3 expects that 0-RTT data to go
// again ("will most likely be retransmitted on receiving a Retry packet"),
// and RFC 9002 §6.3 resets congestion control. The client used to put back
// its Initial packets only: the 0-RTT bytes stayed in flight, never to be
// acknowledged, and their payloads waited for the 1-RTT keys — which made
// the QUIC Interop Runner's zerortt case fail against lsquic, whose server
// answered a two-packet ClientHello with a Retry. Here the whole 0-RTT flight
// goes again in 0-RTT, coalesced, and only it counts in flight.
func TestARetrySendsThe0RTTFlightAgainIn0RTT(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "retry0:self"})
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := assembleConn(tap, fakeFuzzAddr("retry0:peer"), dcid, scid, true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}
	c.remembered = TransportParameters{
		InitialMaxData:          1 << 20,
		InitialMaxStreamDataUni: 1 << 16,
		InitialMaxStreamsUni:    64,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}

	// Thirty requests, three quarters of the initial window: enough that
	// the pacer's bucket, left as the first flight emptied it, would not
	// hold the flight a second time.
	const streams, request = 30, 250
	for range streams {
		s, err := c.OpenUniStream(context.Background())
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		if _, err := s.WriteFinal(make([]byte, request)); err != nil {
			t.Fatalf("writing in 0-RTT: %v", err)
		}
	}
	before := len(tap.recorded())

	// The Retry arrives as the read path delivers a datagram: under
	// gathering, closed once it is processed.
	retry := appendRetryPacket(nil, scid, []byte("srv-cid0"), dcid, []byte("token"))
	c.mu.Lock()
	c.gatherLocked(true)
	c.mu.Unlock()
	if err := c.receive(retry, c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.gatherLocked(false)
	retried, inFlight := c.retried, c.cc.bytesInFlight()
	c.mu.Unlock()
	if !retried {
		t.Fatal("the Retry was not accepted; the test is checking the wrong thing")
	}

	again := tap.recorded()[before:]
	zeroRTT := 0
	for _, w := range again {
		for _, ty := range packetTypes(t, w.data) {
			if ty != packetZeroRTT {
				t.Fatalf("a datagram after the Retry carried packet type %#x, want 0-RTT only", ty)
			}
		}
		zeroRTT += len(w.data)
	}
	if zeroRTT < streams*request {
		t.Fatalf("%d bytes of 0-RTT after the Retry, under the %d bytes of requests it dropped: the 0-RTT flight did not go again", zeroRTT, streams*request)
	}
	if inFlight > zeroRTT {
		t.Errorf("%d bytes in flight after the Retry, %d sent since: the dropped 0-RTT packets still count (RFC 9002 §6.3)", inFlight, zeroRTT)
	}
	if len(again) >= streams {
		t.Errorf("%d requests went again in %d datagrams: one each, nothing coalesced", streams, len(again))
	}
}
