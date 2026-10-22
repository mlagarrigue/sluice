package quic

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
)

// sealedInitialFrom seals one Initial packet the way an endpoint's first
// flight carries it: keyed on keyID (the client's first destination choice,
// RFC 9001 §5.2), under the server's or the client's Initial secret,
// addressed to dcid and naming scid as its source. Padded to the 1200-byte
// datagram §14.1 asks of a client unless padded is false.
func sealedInitialFrom(tb testing.TB, fromServer bool, keyID, dcid, scid []byte, pn uint64, padded bool) []byte {
	tb.Helper()
	return sealedInitialCarrying(tb, fromServer, keyID, dcid, scid, pn, []byte{framePing}, padded)
}

// sealedInitialCarrying is sealedInitialFrom with the frames given.
func sealedInitialCarrying(tb testing.TB, fromServer bool, keyID, dcid, scid []byte, pn uint64, frames []byte, padded bool) []byte {
	tb.Helper()
	secrets, err := initialSecrets(keyID)
	if err != nil {
		tb.Fatal(err)
	}
	secret := secrets.Client
	if fromServer {
		secret = secrets.Server
	}
	sealer, err := newPacketSealer(secret)
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
	header = AppendVarint(header, 0) // no token
	payload := append([]byte(nil), frames...)
	if padded {
		if pad := initialPadding(len(header), pnLen, len(payload)); pad > 0 {
			payload = append(payload, make([]byte, pad)...)
		}
	}
	header = AppendVarint(header, uint64(pnLen+len(payload)+16))
	pnOffset := len(header)
	header = append(header, byte(pn>>24), byte(pn>>16), byte(pn>>8), byte(pn))
	pkt, err := sealer.Seal(nil, header, payload, pn, pnOffset, pnLen)
	if err != nil {
		tb.Fatal(err)
	}
	return pkt
}

// initialRanges is how many packets the Initial space has recorded — the
// sign that a packet was processed rather than discarded.
func initialRanges(c *Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.spaces[spaceInitial].ack.ranges)
}

func discardedCount(c *Conn) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.discarded
}

// serverConnFor builds a server-side connection the way newServerConn
// leaves it after the client's first Initial: the client's SCID pinned as
// the peer's identifier, Initial keys derived from the client's first
// destination choice.
func serverConnFor(t *testing.T, odcid, clientSCID []byte) *Conn {
	t.Helper()
	c := newConn(&fuzzSink{addr: "cid:server"}, fakeFuzzAddr("cid:client"), clientSCID, []byte("srv-scid"), false, DefaultParameters())
	t.Cleanup(func() { c.Close() })
	c.odcid = append([]byte(nil), odcid...)
	if err := c.installInitial(odcid); err != nil {
		t.Fatal(err)
	}
	return c
}

// RFC 9000 §7.2: "Once a client has received a valid Initial packet from
// the server, it MUST discard any subsequent packet it receives on that
// connection with a different Source Connection ID." Initial keys are
// public, so the second Initial here is what anyone on the path can send;
// the Handshake packet is the peer's own change of identifier, refused the
// same way.
func TestClientDiscardsALongHeaderPacketUnderAnotherSCID(t *testing.T) {
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := newConn(&fuzzSink{addr: "cid:self"}, fakeFuzzAddr("cid:peer"), dcid, scid, true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}

	first := []byte("srv-cid0")
	if err := c.receive(sealedInitialFrom(t, true, dcid, scid, first, 0, false), c.peer); err != nil {
		t.Fatal(err)
	}
	if initialRanges(c) != 1 {
		t.Fatal("the server's first Initial was not processed; the test is checking the wrong thing")
	}
	c.mu.Lock()
	pinned := bytes.Equal(c.dcid, first)
	c.mu.Unlock()
	if !pinned {
		t.Fatalf("the client did not adopt the server's SCID %q", first)
	}

	other := []byte("srv-cid1")
	if err := c.receive(sealedInitialFrom(t, true, dcid, scid, other, 2, false), c.peer); err != nil {
		t.Fatal(err)
	}
	if n := initialRanges(c); n != 1 {
		t.Errorf("Initial ranges = %d after an Initial under another SCID, want 1: §7.2 says discard it", n)
	}
	if n := discardedCount(c); n != 1 {
		t.Errorf("discarded = %d, want 1", n)
	}
	c.mu.Lock()
	moved := !bytes.Equal(c.dcid, first)
	c.mu.Unlock()
	if moved {
		t.Error("the client's destination identifier moved on a second Initial; §7.2 permits only the first")
	}

	// The same under Handshake keys, where the packet is provably the
	// peer's: still discarded, since the identifier was pinned.
	sec, err := initialSecrets([]byte("handshake-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.install(tls.QUICEncryptionLevelHandshake, tls.TLS_AES_128_GCM_SHA256, sec.Server, false); err != nil {
		t.Fatal(err)
	}
	peer, err := newSealerSuite(sec.Server, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.receive(handshakePacket(t, peer, scid, other, 0, []byte{framePing}), c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	hs := len(c.spaces[spaceHandshake].ack.ranges)
	c.mu.Unlock()
	if hs != 0 {
		t.Errorf("Handshake ranges = %d after a Handshake packet under another SCID, want 0", hs)
	}
	// The control: the same Handshake packet under the pinned SCID is read.
	if err := c.receive(handshakePacket(t, peer, scid, first, 1, []byte{framePing}), c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	hs = len(c.spaces[spaceHandshake].ack.ranges)
	c.mu.Unlock()
	if hs != 1 {
		t.Error("the Handshake control packet was not processed; the test is checking the wrong thing")
	}
}

// RFC 9000 §7.2, the server's side: "if subsequent Initial packets include
// a different Source Connection ID, they MUST be discarded" — authenticated
// under public Initial keys, so anyone on the path can send one.
func TestServerDiscardsAnInitialUnderAnotherSCID(t *testing.T) {
	odcid, clientSCID := []byte("odcid-00"), []byte("cli-cid0")
	c := serverConnFor(t, odcid, clientSCID)

	if err := c.receive(sealedInitialFrom(t, false, odcid, odcid, clientSCID, 0, true), c.peer); err != nil {
		t.Fatal(err)
	}
	if initialRanges(c) != 1 {
		t.Fatal("the client's Initial under its pinned SCID was not processed; the test is checking the wrong thing")
	}

	if err := c.receive(sealedInitialFrom(t, false, odcid, odcid, []byte("cli-cid1"), 2, true), c.peer); err != nil {
		t.Fatal(err)
	}
	if n := initialRanges(c); n != 1 {
		t.Errorf("Initial ranges = %d after an Initial under another SCID, want 1: §7.2 says discard it", n)
	}
	if n := discardedCount(c); n != 1 {
		t.Errorf("discarded = %d, want 1", n)
	}
	c.mu.Lock()
	moved := !bytes.Equal(c.dcid, clientSCID)
	c.mu.Unlock()
	if moved {
		t.Error("the server's destination identifier moved on a second Initial; §7.2 permits only NEW_CONNECTION_ID to move it")
	}
}

// RFC 9000 §14.1: "A server MUST discard an Initial packet that is carried
// in a UDP datagram with a payload that is smaller than ... 1200 bytes" —
// on an Initial routed to an existing connection as on a Listener's first.
func TestServerDiscardsAShortInitialOnAnExistingConn(t *testing.T) {
	odcid, clientSCID := []byte("odcid-00"), []byte("cli-cid0")
	c := serverConnFor(t, odcid, clientSCID)

	short := sealedInitialFrom(t, false, odcid, odcid, clientSCID, 0, false)
	if len(short) >= 1200 {
		t.Fatalf("the short Initial is %d bytes; the test is checking the wrong thing", len(short))
	}
	if err := c.receive(short, c.peer); err != nil {
		t.Fatal(err)
	}
	if n := initialRanges(c); n != 0 {
		t.Errorf("Initial ranges = %d after a %d-byte Initial datagram, want 0: §14.1 says discard it", n, len(short))
	}
	if n := discardedCount(c); n != 1 {
		t.Errorf("discarded = %d, want 1", n)
	}

	// The control: the same Initial padded to 1200 is processed.
	if err := c.receive(sealedInitialFrom(t, false, odcid, odcid, clientSCID, 0, true), c.peer); err != nil {
		t.Fatal(err)
	}
	if initialRanges(c) != 1 {
		t.Error("the padded control Initial was not processed; the test is checking the wrong thing")
	}
}

// The same rule at accept, the other entry a client's Initial has into
// a server: a short one earns no connection.
func TestAcceptRefusesAShortInitial(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	peer := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}
	odcid, scid := []byte("odcid-00"), []byte("cli-cid0")

	short := sealedInitialFrom(t, false, odcid, odcid, scid, 0, false)
	c, err := accept(pc, datagram{Peer: peer, Payload: short}, cfg, DefaultParameters())
	if err == nil {
		c.Close()
		t.Fatalf("Accept opened a connection on a %d-byte Initial datagram; §14.1 says discard it", len(short))
	}
	// The refusal must be this one, not a later failure of the handshake
	// the same Initial padded to 1200 would start (conn_test.go accepts
	// those against a real client; a short one must not get that far).
	if !errors.Is(err, ErrQUIC) || !strings.Contains(err.Error(), "§14.1") {
		t.Errorf("Accept error = %v, want the §14.1 refusal wrapping ErrQUIC", err)
	}
}
