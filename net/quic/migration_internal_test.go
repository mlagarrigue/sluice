package quic

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// Tests for connection-identifier rotation (RFC 9000 §5.1) and server-side
// passive migration (§9): the synthetic ones drive a Conn's frame handlers
// directly, the way quic_scenarios_test.go does; the end-to-end ones run a
// real handshake over loopback UDP, the way the listener tests do.

// ncidFrame encodes one NEW_CONNECTION_ID frame as a peer would send it.
func ncidFrame(seq, retire uint64, cid, token []byte) []byte {
	b := varint.Append(nil, frameNewConnectionID)
	b = varint.Append(b, seq)
	b = varint.Append(b, retire)
	b = append(b, byte(len(cid)))
	b = append(b, cid...)
	b = append(b, token...)
	return b
}

// controlFrames parses what a connection has queued on its control buffer.
func controlFrames(t *testing.T, c *Conn) []Frame {
	t.Helper()
	c.mu.Lock()
	raw := append([]byte(nil), c.control...)
	c.mu.Unlock()
	frames, err := parseFrames(nil, raw)
	if err != nil {
		t.Fatalf("the control queue does not parse: %v", err)
	}
	return frames
}

// testRegistrar is a cidRegistrar with nothing but the table — what a
// Listener provides, minus the Listener.
type testRegistrar struct {
	mu    sync.Mutex
	table map[string]*Conn
}

func newTestRegistrar() *testRegistrar {
	return &testRegistrar{table: make(map[string]*Conn)}
}

func (r *testRegistrar) resetTokenFor(cid []byte) [16]byte {
	var t [16]byte
	copy(t[:], cid) // deterministic and distinct per identifier; enough for tests
	t[15] = 0xeb
	return t
}

func (r *testRegistrar) addLocalCID(c *Conn) ([]byte, error) {
	cid, err := randomID(listenerCIDLen)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.table[string(cid)] = c
	r.mu.Unlock()
	return cid, nil
}

func (r *testRegistrar) removeLocalCID(cid []byte) {
	r.mu.Lock()
	delete(r.table, string(cid))
	r.mu.Unlock()
}

func (r *testRegistrar) has(cid []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.table[string(cid)]
	return ok
}

// recordingPC captures every outgoing datagram and its destination — the
// observable a path-selection test needs — and swallows nothing else.
type recordingPC struct {
	mu     sync.Mutex
	writes []recordedWrite
	addr   fakeFuzzAddr
}

type recordedWrite struct {
	data []byte
	to   net.Addr
}

func (r *recordingPC) WriteTo(p []byte, addr net.Addr) (int, error) {
	r.mu.Lock()
	r.writes = append(r.writes, recordedWrite{data: append([]byte(nil), p...), to: addr})
	r.mu.Unlock()
	return len(p), nil
}

func (r *recordingPC) recorded() []recordedWrite {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedWrite(nil), r.writes...)
}

func (r *recordingPC) ReadFrom(p []byte) (int, net.Addr, error) { return 0, nil, net.ErrClosed }
func (r *recordingPC) Close() error                             { return nil }
func (r *recordingPC) LocalAddr() net.Addr                      { return r.addr }
func (r *recordingPC) SetDeadline(t time.Time) error            { return nil }
func (r *recordingPC) SetReadDeadline(t time.Time) error        { return nil }
func (r *recordingPC) SetWriteDeadline(t time.Time) error       { return nil }

// installAppSealer gives a bare test Conn something to seal 1-RTT packets
// with, the same trick conformance_internal_test.go uses.
func installAppSealer(t *testing.T, c *Conn) {
	t.Helper()
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = sealer
	c.mu.Unlock()
}

// wantViolation asserts an error is a *transportError with the given code.
func wantViolation(t *testing.T, err error, code uint64, what string) {
	t.Helper()
	te := &transportError{}
	if !errors.As(err, &te) || te.code != code {
		t.Fatalf("%s returned %v, want transport error %#x", what, err, code)
	}
}

// --- A. Local identifier issuance -----------------------------------------

// A connection with a registrar tops its identifiers up to min(the peer's
// active_connection_id_limit, its own) once confirmed, numbering from 1 with
// Retire Prior To 0, and queues each as a retransmittable NEW_CONNECTION_ID.
func TestIssuanceTopsUpToTheLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		own, peers uint64
		want       int // active local identifiers, sequence 0 included
	}{
		{"the peer invites more than this end keeps", 4, 8, 4},
		{"both ends at 6", 6, 6, 6},
		{"the peer keeps fewer than this end allows", 6, 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := newTestRegistrar()
			params := DefaultParameters()
			params.ActiveConnIDLimit = tc.own
			c := newConn(&fuzzSink{addr: "issue:self"}, fakeFuzzAddr("issue:peer"),
				[]byte("peercid0"), []byte("selfcid0"), false, params)
			defer c.Close()

			c.mu.Lock()
			c.registrar = reg
			c.handshakeConfirmed = true
			c.hasPeerParams = true
			c.peerParams.ActiveConnIDLimit = tc.peers
			c.issueLocalCIDsLocked()
			locals := len(c.localCIDs)
			nextSeq := c.nextLocalSeq
			c.mu.Unlock()

			if locals != tc.want {
				t.Fatalf("%d active local identifiers, want %d", locals, tc.want)
			}
			if nextSeq != uint64(tc.want) {
				t.Errorf("nextLocalSeq = %d, want %d (sequences 1..%d issued)", nextSeq, tc.want, tc.want-1)
			}
			var issued int
			for _, f := range controlFrames(t, c) {
				if f.Type != frameNewConnectionID {
					continue
				}
				issued++
				if f.offset != 0 {
					t.Errorf("sequence %d was issued with Retire Prior To %d, want 0", f.value, f.offset)
				}
				if cid := f.Data[:len(f.Data)-16]; !reg.has(cid) {
					t.Errorf("sequence %d's identifier is not registered for routing", f.value)
				}
			}
			if issued != tc.want-1 {
				t.Errorf("%d NEW_CONNECTION_ID frames queued, want %d", issued, tc.want-1)
			}
		})
	}
}

// §19.16's two violations, and the legal retirement between them: a retired
// identifier leaves the routing table and a replacement is issued so the
// active count holds — sequence 0 included, once alternatives exist.
func TestRetireConnectionIDHandling(t *testing.T) {
	reg := newTestRegistrar()
	c := newConn(&fuzzSink{addr: "retire:self"}, fakeFuzzAddr("retire:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	c.mu.Lock()
	c.registrar = reg
	c.handshakeConfirmed = true
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 4
	c.issueLocalCIDsLocked()
	var seq1CID []byte
	for _, e := range c.localCIDs {
		if e.seq == 1 {
			seq1CID = e.cid
		}
	}
	c.mu.Unlock()

	// A sequence this end never issued.
	err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 9), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring a never-issued sequence")

	// The identifier the frame's own packet arrived under.
	c.mu.Lock()
	c.rxLocalCIDSeq = 2
	c.mu.Unlock()
	err = c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 2), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring the arrival identifier")

	// Legal: retire sequence 1 (the packet arrived under 2) — replaced,
	// count kept, routing updated.
	if err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 1), time.Now()); err != nil {
		t.Fatalf("a legal retirement failed: %v", err)
	}
	c.mu.Lock()
	locals := len(c.localCIDs)
	var has1, has4 bool
	for _, e := range c.localCIDs {
		if e.seq == 1 {
			has1 = true
		}
		if e.seq == 4 {
			has4 = true
		}
	}
	c.mu.Unlock()
	if locals != 4 || has1 || !has4 {
		t.Errorf("after retiring 1: %d active, seq1=%v seq4=%v — want 4 active, 1 gone, 4 issued", locals, has1, has4)
	}
	if reg.has(seq1CID) {
		t.Error("the retired identifier still routes")
	}

	// Legal too: retiring sequence 0 once alternatives exist.
	if err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 0), time.Now()); err != nil {
		t.Fatalf("retiring sequence 0 with alternatives available failed: %v", err)
	}
	c.mu.Lock()
	for _, e := range c.localCIDs {
		if e.seq == 0 {
			t.Error("sequence 0 is still active after being retired")
		}
	}
	c.mu.Unlock()

	// A duplicate retirement is a retransmission, not a violation.
	if err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 1), time.Now()); err != nil {
		t.Fatalf("a duplicate retirement failed: %v", err)
	}
}

// A connection without a registrar never issued anything, so the only
// retirable sequence is 0 — the identifier in use, which stays forbidden.
func TestRetireOnAConnectionThatIssuedNothing(t *testing.T) {
	c := newConn(&fuzzSink{addr: "bare:self"}, fakeFuzzAddr("bare:peer"),
		[]byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 0), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring sequence 0 on a single-identifier connection")
	err = c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 3), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring a sequence never issued")
}

// A spare is any active identifier the peer has not sent under. lsquic
// moves to the newest first: with 1 and 2 still unused, a packet under 3
// forces no retirement — one sent then retired the identifier of the path
// lsquic was leaving, and its own retirement of it failed an assertion.
func TestAPeerOnTheNewestIdentifierStillHasSpares(t *testing.T) {
	self := &recordingPC{addr: fakeFuzzAddr("newest:self")}
	c, peer := newKeyedConnOn(t, self, &recordingPC{addr: fakeFuzzAddr("newest:peer")})
	c.mu.Lock()
	c.registrar = newTestRegistrar()
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 4
	c.issueLocalCIDsLocked()
	c.control, c.controlNewCIDs = nil, 0
	var seq3 []byte
	for _, e := range c.localCIDs {
		if e.seq == 3 {
			seq3 = e.cid
		}
	}
	c.mu.Unlock()
	if seq3 == nil {
		t.Fatal("no sequence 3 issued under a limit of 4")
	}
	ping := append([]byte{framePing}, make([]byte, 40)...)
	if err := c.receive(shortPacket(t, peer, 0, seq3, 0, ping), c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.localRetirePrior != 0 || c.nextLocalSeq != 4 {
		t.Errorf("Retire Prior To %d, next sequence %d after a packet under 3 with 1 and 2 unused; want 0 and 4, nothing forced", c.localRetirePrior, c.nextLocalSeq)
	}
}

// A peer with active_connection_id_limit 2 holds sequences 0 and 1. Once its
// packets arrive under 1 it has no spare, and nginx, which neither reuses
// the current identifier on a rebound address nor retires the abandoned
// one, stops following the next NAT rebind. The next NEW_CONNECTION_ID
// carries Retire Prior To 1 (RFC 9000 §5.1.2); the value moves again only
// after the peer retired everything below it.
func TestAPeerOnItsLastIdentifierIsGivenASpare(t *testing.T) {
	self := &recordingPC{addr: fakeFuzzAddr("spare:self")}
	c, peer := newKeyedConnOn(t, self, &recordingPC{addr: fakeFuzzAddr("spare:peer")})
	reg := newTestRegistrar()
	c.mu.Lock()
	c.registrar = reg
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 2
	c.issueLocalCIDsLocked()
	c.control, c.controlNewCIDs = nil, 0
	cidOf := func(seq uint64) []byte {
		for _, e := range c.localCIDs {
			if e.seq == seq {
				return e.cid
			}
		}
		return nil
	}
	seq1 := cidOf(1)
	c.mu.Unlock()
	if seq1 == nil {
		t.Fatal("no sequence 1 issued under a limit of 2")
	}

	sec, err := initialSecrets([]byte("app-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(sec.Server)
	if err != nil {
		t.Fatal(err)
	}
	// newCIDs returns the NEW_CONNECTION_ID frames sent since the last call.
	sent := 0
	newCIDs := func() []Frame {
		t.Helper()
		var out []Frame
		writes := self.recorded()
		for _, w := range writes[sent:] {
			payload, _, err := opener.Open(w.data, 1+len(c.dcid), 0)
			if err != nil {
				t.Fatalf("a datagram does not open: %v", err)
			}
			frames, err := parseFrames(nil, payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range frames {
				if f.Type == frameNewConnectionID {
					out = append(out, f)
				}
			}
		}
		sent = len(writes)
		return out
	}
	pn := uint64(0)
	deliver := func(dcid, payload []byte) {
		t.Helper()
		if err := c.receive(shortPacket(t, peer, 0, dcid, pn, payload), c.peer); err != nil {
			t.Fatal(err)
		}
		pn++
	}
	ping := append([]byte{framePing}, make([]byte, 40)...)

	deliver([]byte("scid0000"), ping)
	if got := newCIDs(); len(got) != 0 {
		t.Fatalf("%d NEW_CONNECTION_ID sent while the peer still has sequence 1 spare", len(got))
	}

	deliver(seq1, ping)
	got := newCIDs()
	if len(got) != 1 || got[0].value != 2 || got[0].offset != 1 {
		t.Fatalf("after the peer moved to sequence 1: %+v, want one NEW_CONNECTION_ID 2 with Retire Prior To 1", got)
	}
	seq2 := got[0].Data[:len(got[0].Data)-16]

	// Sequence 0 still routes until the peer retires it; while it has not,
	// a move to 2 raises nothing (§5.1.2's SHOULD NOT).
	c.mu.Lock()
	kept := cidOf(0) != nil
	c.mu.Unlock()
	if !kept {
		t.Fatal("sequence 0 stopped being accepted before the peer retired it")
	}
	deliver(seq2, ping)
	if got := newCIDs(); len(got) != 0 {
		t.Fatalf("Retire Prior To raised again before sequence 0 was retired: %+v", got)
	}

	// The retirement lands: the one below the threshold leaves without a
	// replacement — it no longer counted — and the move to 2, still with
	// no spare, now gets sequence 3 with Retire Prior To 2.
	deliver(seq2, append(appendFrame(nil, frameRetireConnectionID, 0), ping...))
	got = newCIDs()
	if len(got) != 1 || got[0].value != 3 || got[0].offset != 2 {
		t.Fatalf("after sequence 0 was retired: %+v, want one NEW_CONNECTION_ID 3 with Retire Prior To 2", got)
	}
	deliver(seq2, append(appendFrame(nil, frameRetireConnectionID, 1), ping...))
	if got := newCIDs(); len(got) != 0 {
		t.Fatalf("retiring sequence 1 below the threshold issued %+v", got)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.localCIDs) != 2 || cidOf(2) == nil || cidOf(3) == nil {
		t.Errorf("active identifiers %+v, want sequences 2 and 3", c.localCIDs)
	}
}

// --- B. Peer identifier consumption ----------------------------------------

// Retire Prior To retires every pooled sequence below it — RETIRE frames
// queued for each — and when the identifier in use is among them, sending
// switches to a fresh one at once.
func TestRetirePriorToRetiresAndSwitches(t *testing.T) {
	c := newConn(&fuzzSink{addr: "pool:self"}, fakeFuzzAddr("pool:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	c.pinPeerSCID([]byte("srv-cid0"))

	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pool := len(c.peerCIDs)
	c.mu.Unlock()
	if pool != 2 {
		t.Fatalf("pool holds %d identifiers after one NEW_CONNECTION_ID, want 2", pool)
	}

	// Sequence 2, Retire Prior To 2: sequences 0 and 1 go, and 0 is in use.
	if err := c.frames(spaceApplication, ncidFrame(2, 2, []byte("srv-cid2"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	dcid := append([]byte(nil), c.dcid...)
	cur := c.currentPeerSeq
	pool = len(c.peerCIDs)
	c.mu.Unlock()
	if string(dcid) != "srv-cid2" || cur != 2 {
		t.Errorf("after Retire Prior To 2, sending under %q (seq %d), want srv-cid2 (seq 2)", dcid, cur)
	}
	if pool != 1 {
		t.Errorf("pool holds %d identifiers, want 1", pool)
	}
	var retired []uint64
	for _, f := range controlFrames(t, c) {
		if f.Type == frameRetireConnectionID {
			retired = append(retired, f.value)
		}
	}
	if len(retired) != 2 {
		t.Fatalf("%d RETIRE_CONNECTION_ID frames queued (%v), want 2", len(retired), retired)
	}

	// An identifier arriving already below the threshold is retired at
	// once, never pooled.
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pool = len(c.peerCIDs)
	c.mu.Unlock()
	if pool != 1 {
		t.Errorf("a below-threshold identifier was pooled (%d entries)", pool)
	}
}

func TestNewConnectionIDViolations(t *testing.T) {
	tok := make([]byte, 16)
	newClient := func(name string) *Conn {
		c := newConn(&fuzzSink{addr: fakeFuzzAddr(name + ":self")}, fakeFuzzAddr(name+":peer"),
			[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		c.pinPeerSCID([]byte("srv-cid0"))
		return c
	}

	t.Run("same sequence, different content", func(t *testing.T) {
		c := newClient("dup")
		if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
			t.Fatal(err)
		}
		// The identical repeat is a retransmission, tolerated.
		if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
			t.Fatalf("an identical repeat was refused: %v", err)
		}
		err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cidX"), tok), time.Now())
		wantViolation(t, err, transportProtocolViolation, "sequence 1 reissued with different content")
	})

	t.Run("more active identifiers than advertised", func(t *testing.T) {
		c := newClient("over")
		for seq := uint64(1); seq <= 3; seq++ {
			cid := []byte("srv-cid0")
			cid[7] = byte('0' + seq)
			if err := c.frames(spaceApplication, ncidFrame(seq, 0, cid, tok), time.Now()); err != nil {
				t.Fatalf("sequence %d, within the limit of 4: %v", seq, err)
			}
		}
		err := c.frames(spaceApplication, ncidFrame(4, 0, []byte("srv-cid4"), tok), time.Now())
		wantViolation(t, err, transportConnectionIDLimit, "a fifth simultaneously-active identifier")
	})

	t.Run("zero-length peer identifier", func(t *testing.T) {
		// A server whose client chose a zero-length source identifier: the
		// peer has nothing to rotate, and §19.15 forbids it the frame.
		c := newConn(&fuzzSink{addr: "zero:self"}, fakeFuzzAddr("zero:peer"),
			nil, []byte("selfcid0"), false, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now())
		wantViolation(t, err, transportProtocolViolation, "NEW_CONNECTION_ID under a zero-length peer identifier")
	})
}

// --- C. Server-side passive migration --------------------------------------

// newMigratableServer is a confirmed server-side Conn whose sends are
// recorded: the smallest thing the §9.3 decision can be observed on.
func newMigratableServer(t *testing.T, name string) (*Conn, *recordingPC) {
	t.Helper()
	pc := &recordingPC{addr: fakeFuzzAddr(name + ":self")}
	c := newConn(pc, fakeFuzzAddr(name+":peerA"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()
	return c, pc
}

// rxFromNewAddress stamps the per-datagram state receiveAt would have set
// for an authenticated 1-RTT packet from addr.
func rxFromNewAddress(c *Conn, addr net.Addr, size int, highest bool) {
	c.mu.Lock()
	if !sameAddr(addr, c.peer) {
		if c.offPath.addr == nil || !sameAddr(c.offPath.addr, addr) {
			c.offPath.addr, c.offPath.recv, c.offPath.sent = addr, 0, 0
		}
		c.offPath.recv += int64(size)
	}
	c.rxFrom = addr
	c.rxDatagramLen = size
	c.rxHighest = highest
	c.mu.Unlock()
}

// §9.1/§9.3: a packet of nothing but probing frames does not move the path,
// and its PATH_CHALLENGE is answered to the address it came from — not to
// the peer's current address.
func TestProbingOnlyPacketDoesNotMovePath(t *testing.T) {
	c, pc := newMigratableServer(t, "probe")
	newAddr := fakeFuzzAddr("probe:peerB")
	rxFromNewAddress(c, newAddr, 1200, true)

	challenge := varint.Append(nil, framePathChallenge)
	challenge = append(challenge, 1, 2, 3, 4, 5, 6, 7, 8)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	peer := c.peer
	migrations := c.migrations
	probing := c.pathProbe.pending
	c.mu.Unlock()
	if !sameAddr(peer, fakeFuzzAddr("probe:peerA")) || migrations != 0 {
		t.Errorf("a probing-only packet moved the path to %v (migrations %d)", peer, migrations)
	}
	if probing {
		t.Error("a probing-only packet started this end's own path validation")
	}
	writes := pc.recorded()
	if len(writes) != 1 {
		t.Fatalf("%d datagrams left, want 1 (the PATH_RESPONSE)", len(writes))
	}
	if !sameAddr(writes[0].to, newAddr) {
		t.Errorf("the PATH_RESPONSE went to %v, want the challenge's source %v (§9.3)", writes[0].to, newAddr)
	}
	if len(writes[0].data) < 1200 {
		t.Errorf("the PATH_RESPONSE left in a %d-byte datagram, want 1200 (§8.2.2)", len(writes[0].data))
	}

	// §8.2.2's exception: a challenge that arrived in a small datagram from
	// an off-path address earns no expansion — the response must not be a
	// 1200-byte reflection of a few bytes aimed at a spoofed source.
	rxFromNewAddress(c, fakeFuzzAddr("probe:peerC"), 60, true)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	writes = pc.recorded()
	if len(writes) != 2 {
		t.Fatalf("%d datagrams left, want 2", len(writes))
	}
	if len(writes[1].data) >= 1200 {
		t.Errorf("a 60-byte off-path challenge got a %d-byte answer — amplification (§8.2.2)", len(writes[1].data))
	}
}

// §9.3/§9.4/§9.5: a non-probing packet from a new address — if it is the
// highest-numbered received — moves the path: amplification re-armed and
// seeded with the datagram, congestion and round-trip state reset, a fresh
// pooled identifier adopted, a padded PATH_CHALLENGE probing the new path.
// The peer's echo then validates it; a mismatched echo validates nothing.
func TestNonProbingPacketFromNewAddressMigrates(t *testing.T) {
	c, pc := newMigratableServer(t, "mig")
	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}

	// Pollute the path state so the reset is observable.
	c.mu.Lock()
	c.rtt.sample(50*time.Millisecond, 0)
	c.reno().cwnd = 999_999
	c.ptoCount = 2
	c.mu.Unlock()

	// A stale packet (not the highest-numbered) must not move the path.
	rxFromNewAddress(c, fakeFuzzAddr("mig:peerB"), 1200, false)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	moved := !sameAddr(c.peer, fakeFuzzAddr("mig:peerA"))
	c.mu.Unlock()
	if moved {
		t.Fatal("a non-highest-numbered packet moved the path")
	}

	// The highest-numbered one does.
	newAddr := fakeFuzzAddr("mig:peerB")
	rxFromNewAddress(c, newAddr, 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	peer := c.peer
	migrations := c.migrations
	ampl, amplRecv := c.amplActive, c.amplRecv
	rttReset := !c.rtt.sampled
	cwnd := c.reno().cwnd
	ptoCount := c.ptoCount
	probing := c.pathProbe.pending
	dcid := append([]byte(nil), c.dcid...)
	challengeData := c.pathProbe.data
	c.mu.Unlock()

	if !sameAddr(peer, newAddr) || migrations != 1 {
		t.Fatalf("the path did not move: peer %v, migrations %d", peer, migrations)
	}
	if !ampl || amplRecv != 1200 {
		t.Errorf("amplification not re-armed on the new path: active=%v recv=%d", ampl, amplRecv)
	}
	if !rttReset || ptoCount != 0 {
		t.Errorf("round-trip state survived the migration: sampled=%v ptoCount=%d (§9.4)", !rttReset, ptoCount)
	}
	if want := newNewReno(1200).cwnd; cwnd != want {
		t.Errorf("congestion window is %d after migration, want the initial %d (§9.4)", cwnd, want)
	}
	if string(dcid) != "cli-cid1" {
		t.Errorf("sending under %q after migration, want the unused pooled cli-cid1 (§9.5)", dcid)
	}
	if !probing {
		t.Fatal("no path validation in flight after migration")
	}
	writes := pc.recorded()
	if len(writes) == 0 {
		t.Fatal("no PATH_CHALLENGE left for the new path")
	}
	last := writes[len(writes)-1]
	if !sameAddr(last.to, newAddr) || len(last.data) < 1200 {
		t.Errorf("the challenge went to %v in %d bytes, want %v in ≥1200 (§8.2.1)", last.to, len(last.data), newAddr)
	}

	// A response echoing the wrong data validates nothing (an old probe's
	// answer, perhaps), and is not fatal.
	wrong := varint.Append(nil, framePathResponse)
	wrong = append(wrong, 9, 9, 9, 9, 9, 9, 9, 9)
	rxFromNewAddress(c, newAddr, 100, true)
	if err := c.frames(spaceApplication, wrong, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	stillArmed := c.amplActive
	c.mu.Unlock()
	if !stillArmed {
		t.Error("a mismatched PATH_RESPONSE validated the path")
	}

	// The right echo validates — from any address (§8.2.3).
	right := varint.Append(nil, framePathResponse)
	right = append(right, challengeData[:]...)
	rxFromNewAddress(c, fakeFuzzAddr("mig:peerC"), 100, true)
	if err := c.frames(spaceApplication, right, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	validated := !c.amplActive && !c.pathProbe.pending
	c.mu.Unlock()
	if !validated {
		t.Error("the echoed challenge did not validate the path")
	}
}

// §9: an address change before the handshake confirms is a spoof signal —
// the datagram is counted and dropped, the path untouched.
func TestPreConfirmationAddressChangeIsIgnored(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("pre:self")}
	c := newConn(pc, fakeFuzzAddr("pre:peerA"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)

	if err := c.receive([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fakeFuzzAddr("pre:peerB")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	peer := c.peer
	discarded := c.discarded
	c.mu.Unlock()
	if !sameAddr(peer, fakeFuzzAddr("pre:peerA")) {
		t.Errorf("a pre-confirmation address change moved the path to %v", peer)
	}
	if discarded == 0 {
		t.Error("the refused datagram was not counted")
	}
	// A client never follows anyone, confirmed or not.
	cl := newConn(&fuzzSink{addr: "pre:cself"}, fakeFuzzAddr("pre:cpeerA"),
		[]byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer cl.Close()
	cl.mu.Lock()
	cl.handshakeConfirmed = true
	cl.mu.Unlock()
	if err := cl.receive([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fakeFuzzAddr("pre:cpeerB")); err != nil {
		t.Fatal(err)
	}
	cl.mu.Lock()
	clPeer := cl.peer
	cl.mu.Unlock()
	if !sameAddr(clPeer, fakeFuzzAddr("pre:cpeerA")) {
		t.Errorf("a client followed an address change to %v", clPeer)
	}
}

// --- End to end over loopback ----------------------------------------------

// A listener-managed connection issues identifiers after confirmation; the
// client pools them, and a packet sent under one of the new identifiers —
// same address — still routes to the same connection.
func TestListenerIssuedIdentifiersRoute(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(context.Background(), clientPC, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server *Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}

	// The client should see the issued identifiers arrive.
	deadline := time.Now().Add(5 * time.Second)
	var alt []byte
	for time.Now().Before(deadline) {
		client.mu.Lock()
		for _, e := range client.peerCIDs {
			if e.seq != 0 {
				alt = append([]byte(nil), e.cid...)
			}
		}
		client.mu.Unlock()
		if alt != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if alt == nil {
		t.Fatal("no NEW_CONNECTION_ID reached the client after confirmation")
	}

	// Every identifier the server holds active routes to it in the demux.
	server.mu.Lock()
	locals := make([][]byte, 0, len(server.localCIDs))
	for _, e := range server.localCIDs {
		locals = append(locals, append([]byte(nil), e.cid...))
	}
	server.mu.Unlock()
	if len(locals) < 2 {
		t.Fatalf("the server holds %d active identifiers, want at least 2", len(locals))
	}
	l.mu.Lock()
	for _, cid := range locals {
		if l.byDCID[string(cid)] != server {
			t.Errorf("identifier %x is not registered to its connection", cid)
		}
	}
	l.mu.Unlock()

	// Switch the client's sends to the issued identifier and prove the
	// packets still reach the same connection: data written under it must
	// arrive.
	client.mu.Lock()
	client.dcid = alt
	client.mu.Unlock()

	got := make(chan []byte, 16)
	server.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() && len(f.Data) > 0 {
				got <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("under the new identifier")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if string(data) != "under the new identifier" {
			t.Errorf("the server read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data sent under the issued identifier never arrived — it did not route")
	}
}

// rebindablePC is a client-side packet connection that can change which
// underlying UDP socket its writes leave from — a NAT rebind, from the
// server's point of view — while reading from all of them.
type rebindablePC struct {
	mu       sync.Mutex
	socks    []*net.UDPConn
	active   int
	deadline time.Time
	bump     chan struct{}
	incoming chan rebindDatagram
	closed   chan struct{}
	once     sync.Once
}

type rebindDatagram struct {
	data []byte
	from net.Addr
}

func newRebindablePC(t *testing.T, sockets int) *rebindablePC {
	t.Helper()
	r := &rebindablePC{
		bump:     make(chan struct{}),
		incoming: make(chan rebindDatagram, 64),
		closed:   make(chan struct{}),
	}
	for range sockets {
		s, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		r.socks = append(r.socks, s)
		go r.pump(s)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func (r *rebindablePC) pump(s *net.UDPConn) {
	buf := make([]byte, 2048)
	for {
		n, from, err := s.ReadFrom(buf)
		if err != nil {
			return
		}
		select {
		case r.incoming <- rebindDatagram{data: append([]byte(nil), buf[:n]...), from: from}:
		case <-r.closed:
			return
		}
	}
}

func (r *rebindablePC) rebind(i int) {
	r.mu.Lock()
	r.active = i
	r.mu.Unlock()
}

func (r *rebindablePC) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		r.mu.Lock()
		dl, bump := r.deadline, r.bump
		r.mu.Unlock()
		var timeout <-chan time.Time
		var timer *time.Timer
		if !dl.IsZero() {
			timer = time.NewTimer(time.Until(dl))
			timeout = timer.C
		}
		select {
		case d := <-r.incoming:
			if timer != nil {
				timer.Stop()
			}
			return copy(p, d.data), d.from, nil
		case <-timeout:
			return 0, nil, os.ErrDeadlineExceeded
		case <-bump:
			if timer != nil {
				timer.Stop()
			}
		case <-r.closed:
			if timer != nil {
				timer.Stop()
			}
			return 0, nil, net.ErrClosed
		}
	}
}

func (r *rebindablePC) WriteTo(p []byte, addr net.Addr) (int, error) {
	r.mu.Lock()
	s := r.socks[r.active]
	r.mu.Unlock()
	return s.WriteTo(p, addr)
}

func (r *rebindablePC) Close() error {
	r.once.Do(func() {
		close(r.closed)
		for _, s := range r.socks {
			_ = s.Close()
		}
	})
	return nil
}

func (r *rebindablePC) LocalAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.socks[r.active].LocalAddr()
}

func (r *rebindablePC) SetDeadline(t time.Time) error      { return r.SetReadDeadline(t) }
func (r *rebindablePC) SetWriteDeadline(t time.Time) error { return nil }

func (r *rebindablePC) SetReadDeadline(t time.Time) error {
	r.mu.Lock()
	r.deadline = t
	close(r.bump) // wake a blocked ReadFrom so it re-reads the deadline
	r.bump = make(chan struct{})
	r.mu.Unlock()
	return nil
}

// The NAT-rebind scenario end to end: a real client keeps its connection
// state but its datagrams start leaving from a second socket. The server
// follows — new peer address, a migration counted, a PATH_CHALLENGE the
// client's machinery answers on its own — and once the path validates, the
// amplification limit disarms and data keeps flowing.
func TestServerFollowsClientRebind(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	rp := newRebindablePC(t, 2)
	client, err := Dial(context.Background(), rp, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server *Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}

	var recvMu sync.Mutex
	var received int
	server.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		recvMu.Lock()
		for _, f := range fs {
			if f.IsStream() {
				received += len(f.Data)
			}
		}
		recvMu.Unlock()
		return nil
	})
	waitReceived := func(n int, what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			recvMu.Lock()
			got := received
			recvMu.Unlock()
			if got >= n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s: the server never received the %d bytes", what, n)
	}

	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	waitReceived(100, "before the rebind")

	oldAddr := rp.socks[0].LocalAddr()
	newAddr := rp.socks[1].LocalAddr()
	server.mu.Lock()
	peerBefore := server.peer
	server.mu.Unlock()
	if !sameAddr(peerBefore, oldAddr) {
		t.Fatalf("the server's peer is %v before the rebind, want %v", peerBefore, oldAddr)
	}

	// The rebind: same connection state, new source socket, and a small
	// write — the migrating datagram funds far less than a 1200-byte probe,
	// which is the NAT-rebind common case: the probe must fit the budget,
	// and no extra traffic may be needed to get the path validated.
	rp.rebind(1)
	if _, err := s.Write(make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	waitReceived(120, "across the rebind")
	waitFor(t, "the server to follow and validate the new path", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return sameAddr(server.peer, newAddr) && server.migrations == 1 &&
			!server.amplActive && !server.pathProbe.pending
	})

	// Validated: data still flows on the new path.
	recvMu.Lock()
	mark := received
	recvMu.Unlock()
	if _, err := s.Write(make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	waitReceived(mark+300, "after validation")
	if got := server.Migrations(); got != 1 {
		t.Errorf("Migrations() = %d, want 1", got)
	}
}

// A client issues identifiers for itself too, and a server needs them to
// follow: RFC 9000 §9.5 forbids sending under one identifier to two
// destination addresses, so a server holding no spare one cannot probe the
// client's new address — quic-go then "skips validation of new path … since
// no connection ID is available" and keeps sending where the NAT no longer
// listens, until the idle timer fires (the runner's rebind-port and
// rebind-addr, before Dial issued any). Here the server is sluice's own, so
// the proof is on its state: the pool holds the client's identifiers before
// any move, and each of two rebinds is followed, validated, and sent to
// under an identifier that is not the client's original — which the client,
// owning its socket, answers to without any routing table.
func TestClientIssuesIdentifiersForTheServerToFollow(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	rp := newRebindablePC(t, 3)
	client, err := Dial(context.Background(), rp, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server *Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}

	var recvMu sync.Mutex
	var received int
	server.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		recvMu.Lock()
		for _, f := range fs {
			if f.IsStream() {
				received += len(f.Data)
			}
		}
		recvMu.Unlock()
		return nil
	})
	waitReceived := func(n int, what string) {
		t.Helper()
		waitFor(t, what, func() bool {
			recvMu.Lock()
			defer recvMu.Unlock()
			return received >= n
		})
	}

	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	waitReceived(100, "the first bytes to arrive")

	// The client's NEW_CONNECTION_ID frames ride the first 1-RTT packets
	// after confirmation; the server's pool shows them. Red before Dial
	// issued any: the pool stays at the client's original identifier.
	waitFor(t, "the client's identifiers to reach the server", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return len(server.peerCIDs) >= 2
	})

	for i, sock := range []int{1, 2} {
		rp.rebind(sock)
		if _, err := s.Write(make([]byte, 20)); err != nil {
			t.Fatal(err)
		}
		waitReceived(100+20*(i+1), "bytes across the rebind")
		want := rp.socks[sock].LocalAddr()
		waitFor(t, "the server to follow and validate the new path", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return sameAddr(server.peer, want) && server.migrations == uint64(i+1) &&
				!server.amplActive && !server.pathProbe.pending
		})
		// Validated means the client answered a PATH_CHALLENGE that named
		// an identifier it issued, not its original (§9.5).
		server.mu.Lock()
		dcid := append([]byte(nil), server.dcid...)
		server.mu.Unlock()
		if bytes.Equal(dcid, client.scid) {
			t.Fatalf("rebind %d: the server still sends under the client's original identifier", i+1)
		}
		if _, ok := client.localCIDSeqFor(dcid); !ok {
			t.Fatalf("rebind %d: the server sends under %x, which the client never issued", i+1, dcid)
		}
	}
	if got := server.Migrations(); got != 2 {
		t.Errorf("Migrations() = %d, want 2", got)
	}
}

// --- D. Client-initiated migration: Rebind ---------------------------------

// The deliberate move end to end: a real client swaps its socket with Rebind,
// closes the old one, and keeps writing. The server follows the new address
// (one migration counted on each side), the client's own probe validates,
// and the connection is in no error state afterwards.
func TestClientRebindMovesThePath(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	first, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(context.Background(), first, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server *Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}
	got := make(chan []byte, 8)
	server.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				got <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})

	// The client confirms on HANDSHAKE_DONE, which may still be in flight
	// when Dial returns, and needs an identifier of the server's never used
	// on the old path, which a NEW_CONNECTION_ID in a later packet brings;
	// Rebind refuses until both, so wait for both. Waiting on the first
	// alone failed on arm64, where that later packet had not been read yet.
	waitFor(t, "the client to confirm the handshake and hold a fresh identifier", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.handshakeConfirmed && client.pickPeerCIDLocked(true) >= 0
	})

	second, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := client.Rebind(second); err != nil {
		t.Fatalf("Rebind: %v", err)
	}
	_ = first.Close() // the old socket is the caller's, and the caller is done with it

	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("from the second socket")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if string(data) != "from the second socket" {
			t.Errorf("the server read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data written after Rebind never reached the server")
	}

	waitFor(t, "the server to follow the client", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return sameAddr(server.peer, second.LocalAddr())
	})
	if n := server.Migrations(); n != 1 {
		t.Errorf("server migrations = %d, want 1", n)
	}
	if n := client.Migrations(); n != 1 {
		t.Errorf("client migrations = %d, want 1", n)
	}
	waitFor(t, "the client's path validation", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return !client.pathProbe.pending
	})
	if err := client.Err(); err != nil {
		t.Fatalf("the client ended with %v after a Rebind the server answered", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

// waitFor polls cond until it holds or the test's patience runs out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: still not the case after 10s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Rebind's refusals, each on the smallest Conn that exhibits it: a server, a
// client before confirmation, a closed connection, a nil socket.
func TestRebindRefusals(t *testing.T) {
	server := newConn(&recordingPC{addr: fakeFuzzAddr("rb:sself")}, fakeFuzzAddr("rb:speer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer server.Close()
	server.mu.Lock()
	server.handshakeConfirmed = true
	server.mu.Unlock()
	if err := server.Rebind(&recordingPC{}); err == nil {
		t.Error("a server accepted Rebind")
	}

	client := newConn(&recordingPC{addr: fakeFuzzAddr("rb:cself")}, fakeFuzzAddr("rb:cpeer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer client.Close()
	if err := client.Rebind(&recordingPC{}); err == nil {
		t.Error("a client accepted Rebind before its handshake confirmed")
	}
	if err := client.Rebind(nil); err == nil {
		t.Error("Rebind(nil) was accepted")
	}
	client.mu.Lock()
	client.handshakeConfirmed = true
	client.mu.Unlock()
	installAppSealer(t, client)
	_ = client.Close()
	if err := client.Rebind(&recordingPC{}); !errors.Is(err, errClosed) {
		t.Errorf("Rebind on a closed connection: %v, want errClosed", err)
	}
}

// A Rebind the server never answers: the PATH_CHALLENGE retransmits on its
// backoff, and after the last unanswered one the connection ends with an
// error naming the cause — not the silent idle timeout a server-side probe
// settles for, because here the old path is gone and nothing else will
// ever arrive. Driven through the timer's own entry point with a clock
// advanced by hand, so no real seconds pass.
func TestRebindToADeadPathEndsTheConnection(t *testing.T) {
	old := &recordingPC{addr: fakeFuzzAddr("dead:old")}
	c := newConn(old, fakeFuzzAddr("dead:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")}) // what Rebind sends under (§9.5)
	c.mu.Unlock()

	fresh := &recordingPC{addr: fakeFuzzAddr("dead:new")}
	if err := c.Rebind(fresh); err != nil {
		t.Fatal(err)
	}
	if n := len(fresh.recorded()); n != 1 {
		t.Fatalf("%d datagrams left on the new socket right after Rebind, want 1 (the PATH_CHALLENGE)", n)
	}
	if n := len(old.recorded()); n != 0 {
		t.Fatalf("%d datagrams left on the old socket after Rebind", n)
	}
	c.mu.Lock()
	if !c.pathProbe.pending || !c.pathProbe.fatal {
		t.Fatalf("probe pending=%v fatal=%v after Rebind, want both true", c.pathProbe.pending, c.pathProbe.fatal)
	}
	now := c.pathProbe.sentAt
	for i := 0; i <= pathProbeMaxRetransmits; i++ {
		now = now.Add(time.Hour) // past any backoff
		c.pathProbeDeadlineLocked(now)
	}
	c.mu.Unlock()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the connection did not end after the last unanswered PATH_CHALLENGE")
	}
	if err := c.Err(); err == nil {
		t.Fatal("the connection ended without an error naming the dead path")
	}
	if n := len(fresh.recorded()); n != 1+pathProbeMaxRetransmits+1 {
		// The first challenge, its retransmissions, and the CONNECTION_CLOSE.
		t.Errorf("%d datagrams on the new socket, want %d", n, 1+pathProbeMaxRetransmits+1)
	}
}

// The same probe answered in time is not fatal: the response clears the
// flag along with the probe, and the connection stays up.
func TestRebindValidatedByAResponseIsNotFatal(t *testing.T) {
	c := newConn(&recordingPC{addr: fakeFuzzAddr("ok:old")}, fakeFuzzAddr("ok:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")}) // what Rebind sends under (§9.5)
	c.mu.Unlock()
	if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("ok:new")}); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	resp := varint.Append(nil, framePathResponse)
	resp = append(resp, c.pathProbe.data[:]...)
	c.mu.Unlock()
	if err := c.frames(spaceApplication, resp, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending, fatal := c.pathProbe.pending, c.pathProbe.fatal
	c.mu.Unlock()
	if pending || fatal {
		t.Fatalf("pending=%v fatal=%v after the PATH_RESPONSE, want both false", pending, fatal)
	}
	select {
	case <-c.Done():
		t.Fatalf("the connection ended: %v", c.Err())
	default:
	}
}

// pathResponse encodes a PATH_RESPONSE echoing data.
func pathResponse(data [8]byte) []byte {
	return append(varint.Append(nil, framePathResponse), data[:]...)
}

// writesTo filters recorded datagrams by destination.
func writesTo(ws []recordedWrite, addr net.Addr) []recordedWrite {
	var out []recordedWrite
	for _, w := range ws {
		if sameAddr(w.to, addr) {
			out = append(out, w)
		}
	}
	return out
}

// RFC 9000 §8.2.1: a migration announced by a 40-byte datagram funds a
// 120-byte budget. The PATH_CHALLENGE used to be padded to 1200 regardless,
// withheld by the limit on every attempt, and the path stayed unvalidated
// and throttled to three times the client's bytes for the connection's
// life. It must go out within the budget, validate the address, earn the
// full-size second validation, and leave the connection unthrottled.
func TestSmallDatagramMigrationValidatesWithinTheBudget(t *testing.T) {
	c, pc := newMigratableServer(t, "small")
	newAddr := fakeFuzzAddr("small:peerB")
	rxFromNewAddress(c, newAddr, 40, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	probes := writesTo(pc.recorded(), newAddr)
	if len(probes) != 1 {
		t.Fatalf("%d datagrams to the new path after a 40-byte migration, want the PATH_CHALLENGE", len(probes))
	}
	if n := len(probes[0].data); n > 120 {
		t.Fatalf("the challenge is %d bytes, over the 120-byte budget", n)
	}

	c.mu.Lock()
	first := c.pathProbe.data
	c.mu.Unlock()
	if err := c.frames(spaceApplication, pathResponse(first), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	ampl, pending, second := c.amplActive, c.pathProbe.pending, c.pathProbe.data
	c.mu.Unlock()
	if ampl {
		t.Fatal("the answered challenge did not lift the amplification limit")
	}
	if !pending || second == first {
		t.Fatal("no second, full-size validation after a small probe (§8.2.1)")
	}
	probes = writesTo(pc.recorded(), newAddr)
	if last := probes[len(probes)-1]; len(last.data) < 1200 {
		t.Fatalf("the second validation left in %d bytes, want ≥1200", len(last.data))
	}
	if err := c.frames(spaceApplication, pathResponse(second), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending = c.pathProbe.pending
	c.mu.Unlock()
	if pending {
		t.Fatal("the full-size validation's answer did not complete it")
	}

	// Unthrottled: ten 1000-byte sends all leave.
	before := len(writesTo(pc.recorded(), newAddr))
	payload := append([]byte{framePing}, make([]byte, 999)...)
	c.mu.Lock()
	for range 10 {
		if err := c.sendPacketLocked(spaceApplication, payload, sendOpts{}); err != nil {
			c.mu.Unlock()
			t.Fatal(err)
		}
	}
	c.mu.Unlock()
	if got := len(writesTo(pc.recorded(), newAddr)) - before; got != 10 {
		t.Errorf("%d of 10 datagrams left after validation, want all", got)
	}
}

// RFC 9000 §9.3.2: a migrated path whose validation fails sends the
// connection back to the last validated peer address — limit disarmed,
// identifier restored — where it used to stay on the dead path,
// amplification-limited, for good. §9.3.3: the old path is challenged at
// migration.
func TestUnvalidatedMigrationRevertsToTheLastValidatedPath(t *testing.T) {
	c, pc := newMigratableServer(t, "revert")
	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	oldAddr, newAddr := fakeFuzzAddr("revert:peerA"), fakeFuzzAddr("revert:peerB")
	rxFromNewAddress(c, newAddr, 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	old := writesTo(pc.recorded(), oldAddr)
	if len(old) != 1 || len(old[0].data) < 1200 || string(old[0].data[1:9]) != "peercid0" {
		t.Fatalf("want one ≥1200-byte challenge on the old path under its own identifier (§9.3.3), got %d datagrams", len(old))
	}

	c.mu.Lock()
	if !sameAddr(c.peer, newAddr) || string(c.dcid) != "cli-cid1" {
		c.mu.Unlock()
		t.Fatalf("the path did not move: peer %v dcid %q", c.peer, c.dcid)
	}
	now := c.pathProbe.sentAt
	for i := 0; i <= pathProbeMaxRetransmits; i++ {
		now = now.Add(time.Hour)
		c.pathProbeDeadlineLocked(now)
	}
	peer, dcid, ampl, pending, migrations := c.peer, string(c.dcid), c.amplActive, c.pathProbe.pending, c.migrations
	c.mu.Unlock()
	if !sameAddr(peer, oldAddr) || dcid != "peercid0" {
		t.Errorf("after the failed validation: peer %v dcid %q, want %v under peercid0 (§9.3.2)", peer, dcid, oldAddr)
	}
	if ampl || pending {
		t.Errorf("after reverting: amplActive=%v probing=%v, want both false", ampl, pending)
	}
	if migrations != 1 {
		t.Errorf("migrations = %d, want 1: a revert undoes a move, it is not one", migrations)
	}
	select {
	case <-c.Done():
		t.Fatalf("the connection ended: %v", c.Err())
	default:
	}
	if n := len(writesTo(pc.recorded(), oldAddr)); n < 1+pathProbeMaxRetransmits {
		t.Errorf("%d challenges on the old path, want it retransmitted with the new path's probe", n)
	}

	before := len(writesTo(pc.recorded(), oldAddr))
	payload := append([]byte{framePing}, make([]byte, 999)...)
	c.mu.Lock()
	for range 10 {
		_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{})
	}
	c.mu.Unlock()
	if got := len(writesTo(pc.recorded(), oldAddr)) - before; got != 10 {
		t.Errorf("%d of 10 datagrams left on the reverted path, want all", got)
	}
}

// RFC 9000 §9.3.3: a migration forged from a copied packet is undone as
// soon as the legitimate peer's non-probing packets arrive again from the
// last validated path — no probe, no amplification limit on the way back.
func TestReturnToTheValidatedPathEndsAnUnprovenMigration(t *testing.T) {
	c, _ := newMigratableServer(t, "spoof")
	oldAddr := fakeFuzzAddr("spoof:peerA")
	rxFromNewAddress(c, fakeFuzzAddr("spoof:attacker"), 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	rxFromNewAddress(c, oldAddr, 50, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	peer, ampl, pending := c.peer, c.amplActive, c.pathProbe.pending
	c.mu.Unlock()
	if !sameAddr(peer, oldAddr) || ampl || pending {
		t.Errorf("peer %v amplActive=%v probing=%v, want back on %v unthrottled", peer, ampl, pending, oldAddr)
	}
}

// §5.1.2: NEW_CONNECTION_ID with Retire Prior To equal to its own sequence,
// sent over and over, makes this end queue one RETIRE_CONNECTION_ID per
// frame. While nothing drains the control queue — here no 1-RTT sealer, on
// the wire a peer withholding ACKs to keep the window shut — that queue
// used to grow without bound; past twice the advertised limit it is now
// CONNECTION_ID_LIMIT_ERROR.
func TestRetirementFloodIsBounded(t *testing.T) {
	c := newConn(&fuzzSink{addr: "flood:self"}, fakeFuzzAddr("flood:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	c.pinPeerSCID([]byte("srv-cid0"))
	tok := make([]byte, 16)

	limit := int(max(2*c.params.ActiveConnIDLimit, 8))
	var err error
	seq := uint64(1)
	for ; seq <= 1000 && err == nil; seq++ {
		cid := []byte{'f', 'l', 'o', 'o', 'd', byte(seq >> 16), byte(seq >> 8), byte(seq)}
		err = c.frames(spaceApplication, ncidFrame(seq, seq, cid, tok), time.Now())
	}
	wantViolation(t, err, transportConnectionIDLimit, "an unending retirement flood")
	if retired := len(controlFrames(t, c)); retired > limit {
		t.Errorf("%d frames queued, over the bound of %d", retired, limit)
	}
	if seq > uint64(limit)+2 {
		t.Errorf("the flood ran %d frames before being refused, want about %d", seq-1, limit+1)
	}
}

// Draining the control queue resets the count: retirements a peer forces
// at the pace they leave are not a flood.
func TestRetirementsThatLeaveDoNotAccumulate(t *testing.T) {
	c := newConn(&fuzzSink{addr: "drain:self"}, fakeFuzzAddr("drain:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.pinPeerSCID([]byte("srv-cid0"))
	tok := make([]byte, 16)
	for seq := uint64(1); seq <= 100; seq++ {
		cid := []byte{'d', 'r', 'a', 'i', 'n', byte(seq >> 16), byte(seq >> 8), byte(seq)}
		if err := c.frames(spaceApplication, ncidFrame(seq, seq, cid, tok), time.Now()); err != nil {
			t.Fatalf("sequence %d: %v", seq, err)
		}
		c.mu.Lock()
		c.flushControlLocked()
		c.mu.Unlock()
	}
}

// openAppPackets decrypts every datagram a recordingPC captured from a Conn
// that installAppSealer armed, and parses their frames: a cut that fell
// inside a frame would not parse.
func openAppPackets(t *testing.T, c *Conn, pc *recordingPC) [][]Frame {
	t.Helper()
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]Frame
	for i, w := range pc.recorded() {
		// The DPLPMTUD probe (pmtu.go) is the one datagram sized past
		// maxDatagram, by design; everything else is bounded by it.
		if len(w.data) > c.maxDatagram && len(w.data) != pmtuCommonSize {
			t.Errorf("datagram %d is %d bytes, over maxDatagram %d", i, len(w.data), c.maxDatagram)
		}
		payload, _, err := opener.Open(w.data, 1+len(c.dcid), 0)
		if err != nil {
			t.Fatalf("datagram %d does not open: %v", i, err)
		}
		frames, err := parseFrames(nil, payload)
		if err != nil {
			t.Fatalf("datagram %d does not parse: %v", i, err)
		}
		out = append(out, frames)
	}
	return out
}

// Four hundred RETIRE_CONNECTION_ID frames fit one 1200-byte packet, each
// legal when read — the sequences are predictable — and each used to queue
// a 29-byte NEW_CONNECTION_ID: the control queue grew ten times faster
// than the peer's line. Now at most the active limit of replacements wait
// on the queue; past it a retirement drops the identifier without one, so
// nextLocalSeq stops growing and the next invented sequence is the §19.16
// violation. The registrar's table never holds more than the limit either.
func TestRetirementChainIsBounded(t *testing.T) {
	reg := newTestRegistrar()
	pc := &recordingPC{addr: fakeFuzzAddr("chain:self")}
	c := newConn(pc, fakeFuzzAddr("chain:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.registrar = reg
	c.handshakeConfirmed = true
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 4
	c.issueLocalCIDsLocked()
	c.flushControlLocked() // the first issuance leaves; the queue is empty
	c.mu.Unlock()

	var flood []byte
	for seq := uint64(1); seq <= 400; seq++ {
		flood = appendFrame(flood, frameRetireConnectionID, seq)
	}
	err := c.frames(spaceApplication, flood, time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring past what was issued")

	c.mu.Lock()
	queued, newCIDs, nextSeq, locals := len(c.control), c.controlNewCIDs, c.nextLocalSeq, len(c.localCIDs)
	c.mu.Unlock()
	const limit = 4
	var issued int
	for _, f := range controlFrames(t, c) {
		if f.Type == frameNewConnectionID {
			issued++
		}
	}
	if issued > limit || newCIDs != issued {
		t.Errorf("%d NEW_CONNECTION_ID frames queued (counter %d), want at most %d", issued, newCIDs, limit)
	}
	if queued > limit*29 {
		t.Errorf("the control queue holds %d bytes after a 400-frame flood, want at most %d", queued, limit*29)
	}
	if nextSeq > 2*limit {
		t.Errorf("nextLocalSeq = %d after the flood, want at most %d: issuance did not stop", nextSeq, 2*limit)
	}
	if n := len(reg.table); n > limit || locals > limit {
		t.Errorf("%d identifiers routed, %d active, want at most %d", n, locals, limit)
	}
	openAppPackets(t, c, pc) // nothing that left exceeded maxDatagram
}

// Once the queue drains, the deficit a bounded retirement left is made up:
// the flush after the next processed packet issues the missing
// identifiers, so a conformant peer that retires at the pace of the
// replacements never runs short.
func TestRetirementDeficitIsMadeUpOnFlush(t *testing.T) {
	reg := newTestRegistrar()
	pc := &recordingPC{addr: fakeFuzzAddr("deficit:self")}
	c := newConn(pc, fakeFuzzAddr("deficit:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.registrar = reg
	c.handshakeConfirmed = true
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 4
	c.issueLocalCIDsLocked()
	c.flushControlLocked()
	c.mu.Unlock()

	// Retire 1..7: 4 replacements (4..7) fill the queue, then 5, 6 and 7
	// go without one.
	var retire []byte
	for seq := uint64(1); seq <= 7; seq++ {
		retire = appendFrame(retire, frameRetireConnectionID, seq)
	}
	if err := c.frames(spaceApplication, retire, time.Now()); err != nil {
		t.Fatalf("retiring what was issued: %v", err)
	}
	c.mu.Lock()
	before := len(c.localCIDs)
	c.flushControlLocked()
	after, pending := len(c.localCIDs), c.controlNewCIDs
	c.mu.Unlock()
	if before != 1 {
		t.Errorf("%d identifiers active after the queue filled, want 1 (sequence 0)", before)
	}
	if after != 4 || pending != 0 {
		t.Errorf("after the flush: %d active, %d replacements still queued, want 4 and 0", after, pending)
	}
	for _, frames := range openAppPackets(t, c, pc) {
		for _, f := range frames {
			if f.Type == frameNewConnectionID && f.value >= 8 && reg.has(f.Data[:len(f.Data)-16]) {
				return // a top-up identifier left, and it routes
			}
		}
	}
	t.Error("no top-up NEW_CONNECTION_ID (sequence 8 or above) was sent")
}

// A queue longer than one packet leaves in several, each under
// maxDatagram and cut on a frame boundary, grants first; the counters
// follow the frames out. The control flush used to copy the whole queue
// into one packet — 11.5 KB observed on a 1200-byte path.
func TestQueuedFramesLeaveWithinTheMTU(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("mtu:self")}
	c := newConn(pc, fakeFuzzAddr("mtu:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	for i := range 100 {
		c.grants = appendFrame(c.grants, frameMaxStreamData, uint64(i)<<40, 1<<40) // 17 bytes each
	}
	for i := range 60 {
		c.control = appendFrame(c.control, frameStreamDataBlocked, uint64(i)<<40, 1<<40)
	}
	for seq := range uint64(20) {
		c.control = appendFrame(c.control, frameRetireConnectionID, seq)
		c.controlRetires++
	}
	queued := len(c.grants) + len(c.control)
	c.flushControlLocked()
	left, retires := len(c.grants)+len(c.control), c.controlRetires
	c.mu.Unlock()
	if left != 0 || retires != 0 {
		t.Fatalf("%d bytes and %d retirements still queued after the flush, want none", left, retires)
	}
	packets := openAppPackets(t, c, pc)
	if len(packets) < queued/c.maxDatagram+1 {
		t.Errorf("%d bytes of frames left in %d packets, want at least %d", queued, len(packets), queued/c.maxDatagram+1)
	}
	var grants, blocked, retired int
	grantsDone := false
	for _, frames := range packets {
		for _, f := range frames {
			switch f.Type {
			case frameMaxStreamData:
				grants++
				if grantsDone {
					t.Error("a grant left after a control frame: grants go first")
				}
			case frameStreamDataBlocked:
				blocked++
				grantsDone = true
			case frameRetireConnectionID:
				retired++
				grantsDone = true
			}
		}
	}
	if grants != 100 || blocked != 60 || retired != 20 {
		t.Errorf("%d grants, %d blocked, %d retirements left, want 100, 60, 20", grants, blocked, retired)
	}
}

// What rides a data packet is cut to the room beside it: a full stream
// chunk with a long queue waiting still leaves under maxDatagram, and the
// rest of the queue leaves on its own afterwards.
func TestQueueRidingADataPacketIsCutToTheRoom(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("ride:self")}
	c := newConn(pc, fakeFuzzAddr("ride:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	for i := range 100 {
		c.grants = appendFrame(c.grants, frameMaxStreamData, uint64(i)<<40, 1<<40)
	}
	chunk := []byte{FrameStream | 0x02} // length present, offset 0
	chunk = varint.Append(chunk, 4)
	chunk = varint.Append(chunk, uint64(c.maxStreamChunk()-8))
	chunk = append(chunk, make([]byte, c.maxStreamChunk()-len(chunk))...)
	if err := c.sendPacketLocked(spaceApplication, chunk, sendOpts{}); err != nil {
		t.Fatal(err)
	}
	c.mu.Unlock()
	packets := openAppPackets(t, c, pc)
	if len(packets) != 1 {
		t.Fatalf("%d packets left, want the one data packet", len(packets))
	}
	var rode int
	for _, f := range packets[0] {
		if f.Type == frameMaxStreamData {
			rode++
		}
	}
	if rode == 0 || rode == 100 {
		t.Errorf("%d grants rode the data packet, want some but not all 100", rode)
	}
	c.mu.Lock()
	c.flushControlLocked()
	left := len(c.grants)
	c.mu.Unlock()
	if left != 0 {
		t.Errorf("%d bytes of grants still queued after the flush", left)
	}
	openAppPackets(t, c, pc)
}

// RFC 9000 §18.2: disable_active_migration is a zero-length value, read and
// announced; one that carries bytes is TRANSPORT_PARAMETER_ERROR (§7.4,
// invalid value), and one repeated is refused like every known identifier.
func TestDisableActiveMigrationParameter(t *testing.T) {
	p := DefaultParameters()
	again, err := parseParameters(appendParameters(nil, p))
	if err != nil {
		t.Fatal(err)
	}
	if again.DisableActiveMigration {
		t.Fatal("the default parameters announce disable_active_migration")
	}
	p.DisableActiveMigration = true
	raw := appendParameters(nil, p)
	again, err = parseParameters(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !again.DisableActiveMigration {
		t.Fatal("disable_active_migration did not survive the round trip")
	}

	wantParamError := func(t *testing.T, raw []byte, what string) {
		t.Helper()
		_, err := parseParameters(raw)
		te := &transportError{}
		if !errors.As(err, &te) || te.code != transportParameterError {
			t.Errorf("%s: %v, want TRANSPORT_PARAMETER_ERROR", what, err)
		}
	}
	oneByte := varint.Append(nil, paramDisableActiveMigration)
	oneByte = varint.Append(oneByte, 1)
	oneByte = append(oneByte, 0)
	wantParamError(t, oneByte, "a one-byte disable_active_migration")
	twice := varint.Append(nil, paramDisableActiveMigration)
	twice = varint.Append(twice, 0)
	wantParamError(t, append(twice, twice...), "disable_active_migration twice")
}

// RFC 9000 §18.2: a client whose server announced disable_active_migration
// refuses Rebind — nothing leaves on the new socket, the connection keeps
// the old one — until it has moved to that server's preferred address,
// after which the handshake's address is no longer the one it sends to.
// TestRebindValidatedByAResponseIsNotFatal is the control without the
// parameter.
func TestRebindRefusedWhenMigrationIsDisabled(t *testing.T) {
	old := &recordingPC{addr: fakeFuzzAddr("dam:old")}
	peer := fakeFuzzAddr("dam:peer")
	c := newConn(old, peer, []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerParams.DisableActiveMigration = true
	c.hasPeerParams = true
	c.mu.Unlock()

	fresh := &recordingPC{addr: fakeFuzzAddr("dam:new")}
	if err := c.Rebind(fresh); !errors.Is(err, ErrMigrationDisabled) {
		t.Fatalf("Rebind under disable_active_migration: %v, want ErrMigrationDisabled", err)
	}
	if n := len(fresh.recorded()); n != 0 {
		t.Errorf("%d datagrams left on the new socket after a refused Rebind", n)
	}
	c.mu.Lock()
	if c.pc != old {
		t.Error("a refused Rebind replaced the socket")
	}
	if c.pathProbe.pending {
		t.Error("a refused Rebind started a path probe")
	}
	// A preferred address offered but not yet adopted changes nothing:
	// the connection still sends to the handshake's address.
	c.preferred = fakeFuzzAddr("dam:preferred")
	c.mu.Unlock()
	if err := c.Rebind(fresh); !errors.Is(err, ErrMigrationDisabled) {
		t.Fatalf("Rebind with a preferred address not yet adopted: %v, want ErrMigrationDisabled", err)
	}
	// Once adopted — the connection sends to the preferred address, as
	// adoptPreferredPathLocked leaves it — §18.2's last sentence applies.
	c.mu.Lock()
	c.peer = c.preferred
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")}) // what Rebind sends under (§9.5)
	c.mu.Unlock()
	if err := c.Rebind(fresh); err != nil {
		t.Fatalf("Rebind after acting on preferred_address: %v", err)
	}
	if n := len(fresh.recorded()); n != 1 {
		t.Errorf("%d datagrams left on the new socket after Rebind, want 1 (the PATH_CHALLENGE)", n)
	}
}

// RFC 9000 §8.2.2 and §8: every PATH_CHALLENGE is answered, and what goes
// to an address off the path — never validated, since a probing-only packet
// does not move the path — stays under three times what that address sent.
// One packet of k challenges used to earn k datagrams of 1200 bytes each: a
// reflection of k× toward a spoofed source. Now its k answers share one
// expanded datagram, and the address's own books cap the rest.
func TestOffPathChallengesShareOneDatagramWithinBudget(t *testing.T) {
	c, pc := newMigratableServer(t, "budget")
	source := fakeFuzzAddr("budget:peerB")
	const k = 8
	var challenges []byte
	for i := range k {
		challenges = varint.Append(challenges, framePathChallenge)
		challenges = append(challenges, bytes.Repeat([]byte{byte(i + 1)}, 8)...)
	}
	rxFromNewAddress(c, source, 1200, true)
	if err := c.frames(spaceApplication, challenges, time.Now()); err != nil {
		t.Fatal(err)
	}
	writes := pc.recorded()
	if len(writes) != 1 {
		t.Fatalf("%d datagrams answered one packet of %d challenges, want 1", len(writes), k)
	}
	if !sameAddr(writes[0].to, source) || len(writes[0].data) != 1200 {
		t.Errorf("the answer is %d bytes to %v, want 1200 to %v", len(writes[0].data), writes[0].to, source)
	}
	if got := countFrames(t, c, writes[0].data, framePathResponse); got != k {
		t.Errorf("the datagram carries %d PATH_RESPONSE frames, want %d", got, k)
	}

	// The books: 1200 received funds 3600; two more such packets spend it
	// (1200 each), and a fourth answer — nothing more received — is
	// withheld rather than sent past the limit.
	for range 2 {
		if err := c.frames(spaceApplication, challenges, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.frames(spaceApplication, challenges, time.Now()); err != nil {
		t.Fatal(err)
	}
	var total int
	for _, w := range pc.recorded() {
		if sameAddr(w.to, source) {
			total += len(w.data)
		}
	}
	if total > 3*1200 {
		t.Errorf("%d bytes went to an address that sent 1200: past the 3× limit (§8)", total)
	}
	if total != 3*1200 {
		t.Errorf("%d bytes went to the source, want exactly the 3600 its datagram funds", total)
	}
}

// countFrames opens a short-header datagram c sealed (the test key serves
// both directions) and counts the frames of type typ in it.
func countFrames(t *testing.T, c *Conn, datagram []byte, typ byte) int {
	t.Helper()
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := opener.Open(append([]byte(nil), datagram...), 1+len(c.dcid), 0)
	if err != nil {
		t.Fatalf("the datagram does not open: %v", err)
	}
	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatalf("the payload does not parse: %v", err)
	}
	n := 0
	for _, f := range frames {
		if f.Type == typ {
			n++
		}
	}
	return n
}

// RFC 9000 §9.4: "Packets sent on the old path MUST NOT contribute to
// congestion control or RTT estimation for the new path." The flight the
// migration finds in the record is still owed — a lost payload is queued
// again — but its loss is no congestion event for the fresh window, its
// acknowledgement releases nothing from a flight it never joined and dates
// no round trip. Packets sent after the move are the new path's own: their
// loss halves the window as ever.
func TestOldPathFlightDoesNotShapeTheNewPath(t *testing.T) {
	c, _ := newMigratableServer(t, "flight")
	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}

	// Ten packets in flight on the old path.
	before := time.Now().Add(-10 * time.Millisecond)
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	for pn := range uint64(10) {
		sp.sent.record(sentPacket{pn: pn, sentAt: before, size: 1200, ackEliciting: true, payload: []byte{framePing}})
	}
	sp.nextPN = 10
	c.cc.onSent(before, 12_000)
	c.mu.Unlock()

	rxFromNewAddress(c, fakeFuzzAddr("flight:peerB"), 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	migrations, flightAfterMove := c.migrations, c.reno().inFlight
	c.mu.Unlock()
	if migrations != 1 {
		t.Fatalf("migrations = %d, want 1", migrations)
	}

	// The peer acknowledges the old path's packet 9 alone: 0 to 6 are lost
	// by the packet threshold, 9 is newly acknowledged and ack-eliciting.
	var peer ackTracker
	peer.record(9, time.Now())
	if err := c.handleAck(spaceApplication, ackFrameFor(t, &peer), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	cwnd, sampled, flight, queued := c.reno().cwnd, c.rtt.sampled, c.reno().inFlight, len(sp.retrans)
	c.mu.Unlock()
	if want := newNewReno(1200).cwnd; cwnd != want {
		t.Errorf("cwnd = %d after the old path's losses, want the initial %d (§9.4)", cwnd, want)
	}
	if sampled {
		t.Error("the old path's acknowledgement gave the new path a round-trip sample (§9.4)")
	}
	if flight != flightAfterMove {
		t.Errorf("in flight = %d after the old path's acknowledgement, want %d: those bytes were never the new path's", flight, flightAfterMove)
	}
	if queued != 7 {
		t.Errorf("%d payloads queued for retransmission, want 7: the old path's losses are still owed", queued)
	}

	// Witness: ten packets sent on the new path, seven of them lost.
	after := time.Now()
	c.mu.Lock()
	base := sp.nextPN
	for i := range uint64(10) {
		sp.sent.record(sentPacket{pn: base + i, sentAt: after, size: 1200, ackEliciting: true, payload: []byte{framePing}})
	}
	sp.nextPN = base + 10
	c.cc.onSent(after, 12_000)
	c.mu.Unlock()
	// Read 10 ms after the send: Windows reads the monotonic clock at the
	// system timer's resolution, so an acknowledgement read on the send's
	// tick measures a round trip of zero, which is no sample. (Sending
	// earlier instead would date the packets before the move, onto the old
	// path.)
	acked := after.Add(10 * time.Millisecond)
	peer.record(base+9, acked)
	if err := c.handleAck(spaceApplication, ackFrameFor(t, &peer), acked); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	cwnd, sampled = c.reno().cwnd, c.rtt.sampled
	c.mu.Unlock()
	// The batch's loss halves the window first, and the acknowledged
	// packet, sent before the recovery period that loss opened, grows
	// nothing (RFC 9002 A.7, B.5).
	if want := newNewReno(1200).cwnd / 2; cwnd != want {
		t.Errorf("cwnd = %d after the new path's own losses, want %d: a loss on this path is a congestion event", cwnd, want)
	}
	if !sampled {
		t.Error("the new path's acknowledgement gave no round-trip sample")
	}
}

// challengeFromNewAddress builds a confirmed server whose peer pool holds
// "peercid0", in use on the path, plus fresh identifiers "peercid1",
// "peercid2", …, and feeds it one sealed 1-RTT datagram from a new address,
// naming the local identifier given and carrying a PATH_CHALLENGE padded to
// 1200 bytes: RFC 9000 §9.5's probe of a new path when the identifier is
// not the path's, a NAT rebinding when it is. Returns what it takes to send
// more from that address.
func challengeFromNewAddress(t *testing.T, named []byte, fresh int) (c *Conn, pc *recordingPC, newAddr net.Addr, seal func(pn uint64, payload []byte) []byte) {
	t.Helper()
	pc = &recordingPC{addr: fakeFuzzAddr("np:self")}
	c = newConn(pc, fakeFuzzAddr("np:peerA"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	secrets, err := initialSecrets([]byte("np secrets"))
	if err != nil {
		t.Fatal(err)
	}
	opener, _ := newPacketOpener(secrets.Client)
	sealer, _ := newPacketSealer(secrets.Server)
	peerSealer, _ := newPacketSealer(secrets.Client)
	c.mu.Lock()
	c.spaces[spaceApplication].opener = opener
	c.spaces[spaceApplication].sealer = sealer
	c.handshakeConfirmed = true
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: []byte("selfcid1")})
	for i := 1; i <= fresh; i++ {
		c.peerCIDs = append(c.peerCIDs, peerCID{seq: uint64(i), cid: []byte("peercid" + string(rune('0'+i)))})
	}
	c.mu.Unlock()
	seal = func(pn uint64, payload []byte) []byte {
		const pnLen = 4
		header := []byte{0x40 | byte(pnLen-1)}
		header = append(header, named...)
		pnOffset := len(header)
		header = append(header, byte(pn>>24), byte(pn>>16), byte(pn>>8), byte(pn))
		pkt, err := peerSealer.Seal(nil, header, payload, pn, pnOffset, pnLen)
		if err != nil {
			t.Fatal(err)
		}
		return pkt
	}
	challenge := varint.Append(nil, framePathChallenge)
	challenge = append(challenge, 1, 2, 3, 4, 5, 6, 7, 8)
	challenge = append(challenge, make([]byte, 1200-len(challenge)-1-8-4-16)...) // PADDING frames
	newAddr = fakeFuzzAddr("np:peerB")
	if err := c.receive(seal(1, challenge), newAddr); err != nil {
		t.Fatal(err)
	}
	w := pc.recorded()
	if len(w) != 1 || !sameAddr(w[0].to, newAddr) {
		t.Fatalf("writes = %d, want one to %v", len(w), newAddr)
	}
	return c, pc, newAddr, seal
}

// answerDCID is the destination identifier named by the short header of the
// n-th datagram recorded toward to, identifiers being 8 bytes in these
// tests. Filtered by address because the connection also writes to the
// path in use on its own clock — the delayed ACK for the challenge, when
// the test is stalled past ackTimerDelay — and that datagram is not an
// answer.
func answerDCID(t *testing.T, pc *recordingPC, to net.Addr, n int) []byte {
	t.Helper()
	for _, w := range pc.recorded() {
		if !sameAddr(w.to, to) {
			continue
		}
		if n == 0 {
			return w.data[1:9]
		}
		n--
	}
	t.Fatalf("fewer than %d datagrams were written to %v", n+1, to)
	return nil
}

// RFC 9000 §9.5: a peer probing a new path does so under a new identifier
// of this end, and the answer must not name the identifier in use toward
// the old address, or an observer links the two paths. The answer takes a
// pooled identifier never used, a repeat of the challenge the same one,
// and the migration that follows adopts it rather than burning another.
func TestNewPathChallengeIsAnsweredUnderAFreshCID(t *testing.T) {
	c, pc, newAddr, seal := challengeFromNewAddress(t, []byte("selfcid1"), 2)
	if got := answerDCID(t, pc, newAddr, 0); !bytes.Equal(got, []byte("peercid1")) {
		t.Fatalf("answer to the new address names %q, want the never-used peercid1", got)
	}
	// The repeat (the first answer may have been lost) names the same one.
	challenge := varint.Append(nil, framePathChallenge)
	challenge = append(challenge, 8, 7, 6, 5, 4, 3, 2, 1)
	challenge = append(challenge, make([]byte, 1200-len(challenge)-1-8-4-16)...)
	if err := c.receive(seal(2, challenge), newAddr); err != nil {
		t.Fatal(err)
	}
	if got := answerDCID(t, pc, newAddr, 1); !bytes.Equal(got, []byte("peercid1")) {
		t.Fatalf("repeated answer names %q, want peercid1 again", got)
	}
	// The migration adopts the identifier the answers went under.
	if err := c.receive(seal(3, varint.Append(nil, framePing)), newAddr); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	dcid, seq, migrations := c.dcid, c.currentPeerSeq, c.migrations
	var unused []uint64
	for _, e := range c.peerCIDs {
		if !e.used {
			unused = append(unused, e.seq)
		}
	}
	c.mu.Unlock()
	if migrations != 1 || !bytes.Equal(dcid, []byte("peercid1")) || seq != 1 {
		t.Errorf("after the migration dcid = %q (seq %d, migrations %d), want peercid1 (seq 1, 1 migration)", dcid, seq, migrations)
	}
	if len(unused) != 1 || unused[0] != 2 {
		t.Errorf("unused peer identifiers = %v, want [2]: the migration adopted the answer's rather than taking another", unused)
	}
}

// §9.5's exception: a challenge from a new address under the identifier
// the path in use already names is a NAT rebinding, and the current
// identifier serves.
func TestRebindChallengeIsAnsweredUnderTheCurrentCID(t *testing.T) {
	_, pc, newAddr, _ := challengeFromNewAddress(t, []byte("selfcid0"), 2)
	if got := answerDCID(t, pc, newAddr, 0); !bytes.Equal(got, []byte("peercid0")) {
		t.Fatalf("answer to the rebound address names %q, want the current peercid0", got)
	}
}

// With no pooled identifier to take, the answer goes under the current one
// rather than not at all: §9.3 owes it.
func TestNewPathChallengeWithAnEmptyPoolIsStillAnswered(t *testing.T) {
	_, pc, newAddr, _ := challengeFromNewAddress(t, []byte("selfcid1"), 0)
	if got := answerDCID(t, pc, newAddr, 0); !bytes.Equal(got, []byte("peercid0")) {
		t.Fatalf("answer names %q, want peercid0, the only identifier the peer issued", got)
	}
}

// RFC 9000 §9.5: a new local address never sends under a connection
// identifier another local address used. Rebind therefore needs an
// identifier never used on any path, and is refused — nothing moved — when
// the server issued none: before its NEW_CONNECTION_ID frames, and one
// Rebind past what it issued. Until 2026-10-21 the switch's result was
// ignored and the new socket sent under the old path's identifier.
func TestRebindNeedsAFreshConnectionID(t *testing.T) {
	first := &recordingPC{addr: fakeFuzzAddr("fresh:first")}
	c := newConn(first, fakeFuzzAddr("fresh:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()

	// The pool holds the handshake's identifier alone, already in use.
	if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("fresh:none")}); !errors.Is(err, ErrNoFreshConnectionID) {
		t.Fatalf("Rebind with no fresh identifier: %v, want ErrNoFreshConnectionID", err)
	}
	c.mu.Lock()
	if c.pc != first || c.currentPeerSeq != 0 || string(c.dcid) != "peercid0" || c.pathProbe.pending || c.migrations != 0 {
		t.Fatalf("a refused Rebind moved state: pc=%v seq=%d dcid=%q probe=%v migrations=%d",
			c.pc != first, c.currentPeerSeq, c.dcid, c.pathProbe.pending, c.migrations)
	}
	for i := 1; i <= 3; i++ {
		c.peerCIDs = append(c.peerCIDs, peerCID{seq: uint64(i), cid: []byte("peercid" + string(rune('0'+i)))})
	}
	c.mu.Unlock()

	seen := map[string]bool{"peercid0": true}
	for i := 1; i <= 3; i++ {
		if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("fresh:" + string(rune('0'+i)))}); err != nil {
			t.Fatalf("Rebind %d with %d fresh identifiers left: %v", i, 4-i, err)
		}
		c.mu.Lock()
		cid := string(c.dcid)
		c.mu.Unlock()
		if seen[cid] {
			t.Fatalf("Rebind %d sends under %q, already used on another local address", i, cid)
		}
		seen[cid] = true
	}
	if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("fresh:4")}); !errors.Is(err, ErrNoFreshConnectionID) {
		t.Fatalf("the fourth Rebind on three identifiers: %v, want ErrNoFreshConnectionID", err)
	}
}

// Once the new path is validated, the identifier the old path sent under
// is retired (RFC 9000 §5.1.2): a RETIRE_CONNECTION_ID carrying its
// sequence is queued and the entry leaves the pool, so the server issues a
// replacement and the next Rebind has something fresh to send under.
func TestRebindRetiresTheOldPathIdentifierOnceValidated(t *testing.T) {
	c := newConn(&recordingPC{addr: fakeFuzzAddr("retire:first")}, fakeFuzzAddr("retire:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")})
	c.mu.Unlock()
	if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("retire:second")}); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	data := c.pathProbe.data
	retired := bytes.Contains(c.control, appendFrame(nil, frameRetireConnectionID, 0))
	c.mu.Unlock()
	if retired {
		t.Fatal("the old identifier was retired before the new path was validated")
	}

	c.handlePathResponse(Frame{Type: framePathResponse, Data: data[:]}, time.Now())
	c.mu.Lock()
	defer c.mu.Unlock()
	if !bytes.Contains(c.control, appendFrame(nil, frameRetireConnectionID, 0)) {
		t.Fatal("no RETIRE_CONNECTION_ID for sequence 0 queued once the new path was validated")
	}
	for _, e := range c.peerCIDs {
		if e.seq == 0 {
			t.Fatal("the retired identifier is still pooled")
		}
	}
	if c.currentPeerSeq != 1 {
		t.Fatalf("sending under sequence %d, want 1", c.currentPeerSeq)
	}
}

// RFC 9000 §9.4: the PATH_CHALLENGE timer is RFC 9002 §6.2.1's PTO and MUST
// NOT be more aggressive — max_ack_delay included, as in the application
// space. At pto(0) the challenge is not resent yet; at pto(max_ack_delay)
// it is.
func TestPathChallengeTimerIsNotMoreAggressiveThanThePTO(t *testing.T) {
	old := &recordingPC{addr: fakeFuzzAddr("timer:old")}
	c := newConn(old, fakeFuzzAddr("timer:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")})
	c.rtt = rttEstimator{latest: 100 * time.Millisecond, min: 100 * time.Millisecond, smoothed: 100 * time.Millisecond, sampled: true}
	c.mu.Unlock()

	fresh := &recordingPC{addr: fakeFuzzAddr("timer:new")}
	if err := c.Rebind(fresh); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sentAt := c.pathProbe.sentAt
	short, full := c.rtt.pto(0), c.rtt.pto(c.peerMaxAckDelayLocked())
	if full <= short {
		t.Fatalf("pto(max_ack_delay) = %v is not above pto(0) = %v; the peer's max_ack_delay reads as zero", full, short)
	}
	c.pathProbeDeadlineLocked(sentAt.Add(short))
	if n := len(fresh.recorded()); n != 1 {
		t.Fatalf("%d datagrams after pto(0) = %v, want the first challenge alone: the timer is more aggressive than §6.2.1's PTO", n, short)
	}
	c.pathProbeDeadlineLocked(sentAt.Add(full))
	if n := len(fresh.recorded()); n != 2 {
		t.Errorf("%d datagrams after pto(max_ack_delay) = %v, want the challenge resent once", n, full)
	}
}

// RFC 9000 §9.5: no migration toward a peer that uses zero-length connection
// IDs — every path of its would be told apart by address alone. Rebind
// refuses before anything moves, and the new socket sends nothing.
func TestRebindRefusesAPeerWithZeroLengthIDs(t *testing.T) {
	old := &recordingPC{addr: fakeFuzzAddr("zero:old")}
	c := newConn(old, fakeFuzzAddr("zero:peer"), nil, []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.dcid = nil
	c.mu.Unlock()

	fresh := &recordingPC{addr: fakeFuzzAddr("zero:new")}
	err := c.Rebind(fresh)
	if err == nil || !strings.Contains(err.Error(), "zero-length") {
		t.Fatalf("Rebind toward a zero-length-ID peer = %v, want a refusal naming zero-length connection IDs", err)
	}
	if n := len(fresh.recorded()); n != 0 {
		t.Errorf("%d datagrams left on the new socket after a refused Rebind", n)
	}
}

// RFC 9000 §8.2.1: unpredictable data in every PATH_CHALLENGE frame, resends
// included (§13.3: "a different payload each time"). Two challenges of one
// probe, the second a timer resend, carry different bytes.
func TestPathChallengeResendsCarryFreshData(t *testing.T) {
	old := &recordingPC{addr: fakeFuzzAddr("fresh:old")}
	c := newConn(old, fakeFuzzAddr("fresh:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")})
	c.mu.Unlock()

	fresh := &recordingPC{addr: fakeFuzzAddr("fresh:new")}
	if err := c.Rebind(fresh); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.pathProbeDeadlineLocked(c.pathProbe.sentAt.Add(time.Hour)) // past any backoff: one resend
	c.mu.Unlock()
	writes := fresh.recorded()
	if len(writes) != 2 {
		t.Fatalf("%d datagrams on the new socket, want the challenge and one resend", len(writes))
	}
	var data [2][]byte
	for i, w := range writes {
		for _, f := range openShort(t, w.data, len("peercid1")) {
			if f.Type == framePathChallenge {
				data[i] = append([]byte(nil), f.Data...)
			}
		}
		if data[i] == nil {
			t.Fatalf("datagram %d carries no PATH_CHALLENGE", i)
		}
	}
	if bytes.Equal(data[0], data[1]) {
		t.Errorf("the resent PATH_CHALLENGE repeats the first one's data %x; §8.2.1 wants fresh data in every frame", data[0])
	}
}

// RFC 9000 §21.5.6: a server does not follow its peer to a loopback address
// it was not using — a forged source of 127.0.0.1 would otherwise draw up
// to three times the received bytes toward a local service. Any other new
// address is still followed, and a peer already on loopback may move within
// it: that is not a service "previously available at a different
// interface".
func TestMigrationToLoopbackIsNotFollowed(t *testing.T) {
	newServerAt := func(peer *net.UDPAddr) (*Conn, *recordingPC) {
		pc := &recordingPC{addr: fakeFuzzAddr("loop:self")}
		c := newConn(pc, peer, []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		installAppSealer(t, c)
		c.mu.Lock()
		c.handshakeConfirmed = true
		c.mu.Unlock()
		return c, pc
	}
	moveTo := func(c *Conn, to *net.UDPAddr) net.Addr {
		rxFromNewAddress(c, to, 1200, true)
		if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.peer
	}
	global := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 5000}
	loop := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1).To4(), Port: 6000}
	other := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7).To4(), Port: 6000}

	c, pc := newServerAt(global)
	if got := moveTo(c, loop); !sameAddr(got, global) {
		t.Errorf("peer = %v after a non-probing packet from loopback, want %v kept (§21.5.6)", got, global)
	}
	for _, w := range pc.recorded() {
		if sameAddr(w.to, loop) {
			t.Fatalf("a datagram left for the loopback address %v", loop)
		}
	}

	// Controls against a false green: any other new address is followed,
	// and loopback to loopback too.
	c, _ = newServerAt(global)
	if got := moveTo(c, other); !sameAddr(got, other) {
		t.Errorf("peer = %v after a non-probing packet from %v, want it followed", got, other)
	}
	c, _ = newServerAt(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1).To4(), Port: 5000})
	if got := moveTo(c, loop); !sameAddr(got, loop) {
		t.Errorf("peer = %v after a move within loopback, want %v followed", got, loop)
	}
}

// §9.3.3 has the path left challenged at every apparent migration, "the
// previously active path" — proven or not. A second migration before the
// first validated used to leave the unproven path unchallenged, the
// challenge being aimed at the last validated path only; now the path left
// is challenged, and its answer makes it the path to fall back to (§9.3.2).
func TestTheUnprovenPathLeftIsChallengedToo(t *testing.T) {
	c, pc := newMigratableServer(t, "chain")
	tok := make([]byte, 16)
	for _, f := range [][]byte{ncidFrame(1, 0, []byte("cli-cid1"), tok), ncidFrame(2, 0, []byte("cli-cid2"), tok)} {
		if err := c.frames(spaceApplication, f, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	peerB, peerC := fakeFuzzAddr("chain:peerB"), fakeFuzzAddr("chain:peerC")
	rxFromNewAddress(c, peerB, 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	before := len(pc.recorded())
	// B never answers; the peer shows up at C.
	rxFromNewAddress(c, peerC, 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	peer, migrations := c.peer, c.migrations
	old := c.pathProbe.oldData
	c.mu.Unlock()
	if !sameAddr(peer, peerC) || migrations != 2 {
		t.Fatalf("after two moves the peer is %v with %d migrations, want %v and 2", peer, migrations, peerC)
	}
	challenged := false
	for _, w := range pc.recorded()[before:] {
		if sameAddr(w.to, peerB) && len(w.data) >= 1200 {
			challenged = true
		}
	}
	if !challenged {
		t.Fatal("the unproven path left (B) got no PATH_CHALLENGE at the second migration (§9.3.3)")
	}
	// B answers: it is proven now, and the path to fall back to.
	resp := varint.Append(nil, framePathResponse)
	resp = append(resp, old[:]...)
	if err := c.frames(spaceApplication, resp, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	fallback := c.lastValidatedPeer
	c.mu.Unlock()
	if !sameAddr(fallback, peerB) {
		t.Fatalf("the path to fall back to is %v after B answered its challenge, want %v", fallback, peerB)
	}
}

// The §9.3.3 challenge to the path left goes on under a fresh identifier
// when the peer has retired the one that path used: a never-used
// identifier links the two paths no more than silence would (§9.5), where
// the challenge used to be given up. Any other identifier would tie them.
func TestThePathLeftIsChallengedUnderAFreshIDWhenItsOwnIsRetired(t *testing.T) {
	c, pc := newMigratableServer(t, "retired")
	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	peerA, peerB := fakeFuzzAddr("retired:peerA"), fakeFuzzAddr("retired:peerB")
	rxFromNewAddress(c, peerB, 1200, true)
	if err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// The peer retires everything before sequence 1: the identifier A was
	// used under is gone, and a fresh one (2) is in the pool.
	if err := c.frames(spaceApplication, ncidFrame(2, 1, []byte("cli-cid2"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	before := len(pc.recorded())
	c.mu.Lock()
	c.pathProbeDeadlineLocked(time.Now().Add(10 * time.Second)) // the resend of both challenges
	current := append([]byte(nil), c.dcid...)
	c.mu.Unlock()
	var toA [][]byte
	for _, w := range pc.recorded()[before:] {
		if sameAddr(w.to, peerA) && len(w.data) >= 1200 {
			toA = append(toA, w.data)
		}
	}
	if len(toA) == 0 {
		t.Fatal("the path left (A) got no PATH_CHALLENGE once its identifier was retired (§9.3.3)")
	}
	for _, d := range toA {
		if id := string(d[1:9]); id == "peercid0" || id == string(current) {
			t.Fatalf("the challenge to A left under %q: a retired identifier or the new path's (§9.5)", id)
		}
	}
}

// RFC 9000 §9.6.2 lets a server move its sends to its preferred-address
// socket once two conditions hold: a non-probing packet with a new largest
// packet number arrived there, and this end's own probe from it was
// answered. The second is remembered in pathProbe.viaValidated, beside the
// socket itself in pathProbe.via — and until 2026-10-26
// startPathChallengeData cleared via on every new probe while leaving both
// flags standing. A client that validated the preferred address and then
// migrated (§9.3) left a validation behind that belonged to no socket; its
// next non-probing packet arriving on the preferred socket — §9.6.3's own
// example, "a different NAT binding for the server's preferred address" —
// met the stale flag, and the server moved its sends to a nil socket. The
// next send dereferenced it and the server process panicked, through
// Conn.Close and sendCloseLocked.
//
// Nothing hostile is needed: a conformant client reaches it. The guard that
// would have caught the mismatch (maybeMigrate, "no probe went out from
// this socket") is skipped for a peer that chose a zero-length connection
// identifier, which §5.1 allows.
func TestPreferredMoveRefusesAStaleValidation(t *testing.T) {
	main := &recordingPC{addr: fakeFuzzAddr("pref0:self")}
	pref := &recordingPC{addr: fakeFuzzAddr("pref0:preferred")}
	peerA := fakeFuzzAddr("pref0:peerA")
	peerB := fakeFuzzAddr("pref0:peerB")

	// A server whose client chose a zero-length connection identifier.
	c := newConn(main, peerA, nil, []byte("selfcid0"), false, DefaultParameters())
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		if c.pc == nil { // so Close reports the failure instead of panicking on it
			c.pc = main
		}
		c.mu.Unlock()
		_ = c.Close()
	})

	rx := func(from net.Addr, via net.PacketConn) {
		c.mu.Lock()
		c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = from, via, 1200, true
		c.mu.Unlock()
	}

	// The client validates the offered preferred address: its PATH_CHALLENGE
	// reaches the preferred socket and the server probes back from it.
	challenge := append(varint.Append(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	rx(peerA, pref)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	armed, data := c.pathProbe.via == net.PacketConn(pref), c.pathProbe.data
	c.mu.Unlock()
	if !armed {
		t.Fatalf("the server did not probe from its preferred socket; the sequence under test cannot be reached")
	}

	// The client answers that probe: §9.6.2's second condition holds.
	rx(peerA, pref)
	if err := c.frames(spaceApplication, append(varint.Append(nil, framePathResponse), data[:]...), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	validated := c.pathProbe.viaValidated
	c.mu.Unlock()
	if !validated {
		t.Fatalf("the answered probe did not set viaValidated; the sequence under test cannot be reached")
	}

	// The client moves to a new address, on the main socket: an ordinary
	// migration, which arms a fresh probe and so clears pathProbe.via.
	rx(peerB, main)
	c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())
	c.mu.Lock()
	via, stale := c.pathProbe.via, c.pathProbe.viaValidated
	c.mu.Unlock()
	if via != nil {
		t.Fatalf("the migration left pathProbe.via at %v; the sequence under test cannot be reached", via)
	}
	if stale {
		t.Errorf("the migration cleared pathProbe.via and kept viaValidated: a validation that belongs to no socket")
	}

	// A non-probing packet from that same address on the preferred socket.
	// §9.6.2's first condition is recorded; the second must not be believed.
	rx(peerB, pref)
	c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())
	c.mu.Lock()
	pc, migrations := c.pc, c.migrations
	c.mu.Unlock()
	if pc == nil {
		t.Fatalf("the connection moved to a nil socket: the next send dereferences it")
	}
	if pc != net.PacketConn(main) {
		t.Errorf("the connection sends from %v, want the socket it migrated to", pc)
	}
	// One move, the client's migration to peerB. The preferred socket was
	// never moved to: its probe belongs to the path left behind.
	if migrations != 1 {
		t.Errorf("migrations = %d, want 1: only the client's migration happened", migrations)
	}
}
