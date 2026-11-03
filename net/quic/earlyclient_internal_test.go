package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"testing"
	"time"
)

// A 0-RTT writer that parks on flow-control credit while another writer is
// counted in (writersIn > 1) flushes the datagram it gathered: held, the
// peer would never see the bytes that open the window it waits for.
func TestParkedWriterDoesNotHoldAGatheredDatagram(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "parked:self"})
	dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	c := assembleConn(tap, fakeFuzzAddr("parked:peer"), dcid, scid, true, DefaultConfig())
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
	srv := DefaultConfig()
	srv.initialSourceCID = dcid
	srv.originalDCID = dcid
	if err := c.handlePeerParameters(appendParameters(nil, srv.TransportParameters)); err != nil {
		t.Fatalf("server parameters: %v", err)
	}
	s, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	// Another writer is on its way: counted in, as Stream.write counts itself
	// before taking mu.
	c.writersIn.Add(1)
	go func() { _, _ = s.Write(make([]byte, 3*remembered)) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		off := int(s.writeOff)
		c.mu.Unlock()
		if off >= remembered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the write never reached the remembered limit (writeOff %d)", off)
		}
		time.Sleep(time.Millisecond)
	}
	// Parked on credit now, with the first bytes in a gathered datagram.
	for range 200 {
		if len(tap.recorded()) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the writer parked on credit with %d bytes of stream data gathered and nothing sent: the peer cannot grant what it never received", remembered)
}

// A 0-RTT packet gathered while a second writer is on its way (writersIn >
// 1) is still pending when crypto/tls hands the client its 1-RTT write
// secret. The 0-RTT keys go then (RFC 9001 §4.9.3), but the packet built
// under them leaves first: its stream bytes were already counted as sent,
// and nothing would ever send them again (RFC 9000 §13.3).
func TestZeroRTTPacketPendingAtTheKeyChangeLeaves(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "keychange:self"})
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := assembleConn(tap, fakeFuzzAddr("keychange:peer"), dcid, scid, true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}
	c.remembered = TransportParameters{
		InitialMaxData:          1 << 20,
		InitialMaxStreamDataUni: 1 << 16,
		InitialMaxStreamsUni:    8,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}
	a, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Writer B is counted on its way to mu (stream.go: writersIn.Add before
	// Lock), so writer A gathers its 0-RTT packet and leaves without flushing.
	marker := bytes.Repeat([]byte{0xA5}, 300)
	c.writersIn.Add(1)
	if _, err := a.Write(marker); err != nil {
		t.Fatalf("0-RTT write: %v", err)
	}
	c.mu.Lock()
	pending := c.dgramPend != nil && c.dgramPend.early
	c.mu.Unlock()
	if !pending {
		t.Fatal("precondition lost: writer A's 0-RTT packet is not pending")
	}
	wireBefore := len(tap.recorded())

	// The handshake goroutine takes mu before B: the 1-RTT write secret.
	if _, err := c.handleTLSEvent(tls.QUICEvent{
		Kind: tls.QUICSetWriteSecret, Level: tls.QUICEncryptionLevelApplication,
		Suite: tls.TLS_AES_128_GCM_SHA256, Data: make([]byte, 32),
	}); err != nil {
		t.Fatalf("1-RTT write secret: %v", err)
	}

	// B gets mu now; its first packet flushes whatever is still gathered.
	c.writersIn.Add(-1)
	if _, err := b.Write([]byte("b")); err != nil {
		t.Fatalf("1-RTT write: %v", err)
	}

	zeroRTT := 0
	for _, w := range tap.recorded()[wireBefore:] {
		for _, ty := range packetTypes(t, w.data) {
			if ty == packetZeroRTT {
				zeroRTT++
			}
		}
	}
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	recorded := false
	for _, r := range sp.sent.packets {
		recorded = recorded || bytes.Contains(r.payload, marker[:64])
	}
	queued := false
	for _, r := range sp.retrans {
		queued = queued || bytes.Contains(r, marker[:64])
	}
	c.mu.Unlock()
	if zeroRTT == 0 || !recorded {
		t.Fatalf("writer A's 300 bytes: %d 0-RTT packets on the wire, recorded as sent %v, queued for retransmission %v; want one 0-RTT packet recorded as sent",
			zeroRTT, recorded, queued)
	}
}

// sealLocked never drops a packet it cannot seal while its space lives: a
// 0-RTT packet whose keys left between building and sealing gives its
// frames back to the front of the retransmission queue, which sends them
// again under the 1-RTT keys.
func TestSealWithoutZeroRTTKeysRequeuesThePayload(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "nosealer:self"})
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := assembleConn(tap, fakeFuzzAddr("nosealer:peer"), dcid, scid, true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })

	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	sp.retrans = [][]byte{[]byte("already queued")}
	frames := []byte("stream frames under 0-RTT")
	p := &pendingPacket{
		sp: sp, space: spaceApplication, early: true,
		prefix: []byte{0xd0}, payload: frames, retransPart: frames, pn: 7,
	}
	c.earlySealer = nil
	out, err := c.sealLocked(nil, p, 0, 0)
	if err != nil || len(out) != 0 {
		t.Fatalf("sealLocked without a sealer = %d bytes, %v; want nothing written and no error", len(out), err)
	}
	if len(sp.retrans) != 2 || !bytes.Equal(sp.retrans[0], frames) {
		t.Fatalf("retransmission queue = %q; want the unsealed frames first", sp.retrans)
	}
}

// RFC 9000 §2.2 and sealLocked's model: a datagram the socket refuses after
// its packet was sealed and recorded is a packet the network lost, and
// recovery sends its frames again. A lone 0-RTT writer, whose packet is
// written at once, used to take the write error as a refusal: it gave the
// stream and connection credit back and left writeOff where it was, while
// the frame sat in the sent record — credit spent twice, and a Write
// replayed put other bytes at the same offset. The write counts as sent now.
func TestAZeroRTTWriteTheSocketRefusedStillAdvancesTheStream(t *testing.T) {
	pc := &failingWritePC{fuzzSink{addr: "refused:self"}}
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := assembleConn(pc, fakeFuzzAddr("refused:peer"), dcid, scid, true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}
	c.remembered = TransportParameters{
		InitialMaxData:          1 << 20,
		InitialMaxStreamDataUni: 1 << 16,
		InitialMaxStreamsUni:    8,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}
	s, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	marker := bytes.Repeat([]byte{0x5A}, 200)
	n, err := s.Write(marker)

	c.mu.Lock()
	defer c.mu.Unlock()
	recorded := false
	for _, p := range c.spaces[spaceApplication].sent.packets {
		recorded = recorded || bytes.Contains(p.payload, marker[:64])
	}
	if !recorded {
		t.Fatalf("the 0-RTT frame is not in the sent record (Write = %d, %v): the test proves nothing", n, err)
	}
	if s.writeOff != uint64(len(marker)) || c.connSend.sent != uint64(len(marker)) {
		t.Fatalf("Write = %d, %v; writeOff %d, connection credit spent %d; want both %d, the frame recovery will resend",
			n, err, s.writeOff, c.connSend.sent, len(marker))
	}
	if err != nil || n != len(marker) {
		t.Errorf("Write = %d, %v; want %d, nil: the bytes are sent as far as the stream goes", n, err, len(marker))
	}
}
