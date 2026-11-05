package quic

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// lastSize is the size of the last packet recorded in the application
// space. Callers hold mu.
func lastSize(t *testing.T, c *Conn) int {
	t.Helper()
	p := c.spaces[spaceApplication].sent.packets
	if len(p) == 0 {
		t.Fatal("nothing recorded")
	}
	return p[len(p)-1].size
}

func writeApp(t *testing.T, c *Conn, payload []byte, opts sendOpts) error {
	t.Helper()
	return c.writePacketLocked(&c.spaces[spaceApplication], spaceApplication, payload, payload, opts, time.Now())
}

// A padded datagram is cut to what each budget allows: the current path's
// amplification limit, an off-path address's, the unproven old path's.
func TestPaddingStopsAtEachBudget(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	c.amplActive, c.amplRecv, c.amplSent = true, 400, 200
	if err := writeApp(t, c, []byte{framePing}, sendOpts{padTo: 1200}); err != nil {
		t.Fatal(err)
	}
	if got := lastSize(t, c); got != 1000 {
		t.Errorf("padded under the amplification limit to %d bytes, want 1000", got)
	}
	c.mu.Unlock()

	other := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	c, _ = appConn(t)
	c.mu.Lock()
	c.offPath.addr, c.offPath.recv, c.offPath.sent = other, 300, 100
	if err := writeApp(t, c, []byte{framePing}, sendOpts{padTo: 1200, to: other, noRetrans: true}); err != nil {
		t.Fatal(err)
	}
	if c.offPath.sent != 900 {
		t.Errorf("off-path bytes sent = %d, want the 900 the budget allows", c.offPath.sent)
	}
	c.mu.Unlock()

	c, _ = appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pathProbe.oldPeer, c.pathProbe.oldRecv, c.pathProbe.oldSent = other, 300, 100
	if err := writeApp(t, c, []byte{framePing}, sendOpts{padTo: 1200, to: other, noRetrans: true}); err != nil {
		t.Fatal(err)
	}
	if c.pathProbe.oldSent != 900 {
		t.Errorf("old path bytes sent = %d, want the 900 the budget allows", c.pathProbe.oldSent)
	}
}

// A short 1-RTT packet is padded to 22 bytes past this end's identifier,
// and no further.
func TestShortPacketPaddedPastAReset(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := writeApp(t, c, []byte{framePing}, sendOpts{}); err != nil {
		t.Fatal(err)
	}
	if got, want := lastSize(t, c), 22+len(c.scid); got != want {
		t.Errorf("a PING packet of %d bytes, want %d", got, want)
	}
}

// The last packet number, 2^62-1, is never used.
func TestPacketNumberSpaceExhausted(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spaces[spaceApplication].nextPN = 1<<62 - 1
	err := writeApp(t, c, []byte{framePing}, sendOpts{})
	if err == nil || !strings.HasSuffix(err.Error(), ": the packet-number space is exhausted") || c.closeErr == nil {
		t.Errorf("at 2^62-1: %v, closeErr %v", err, c.closeErr)
	}
}

// writeInitial sends an ack-eliciting Initial while gathering.
func writeInitial(t *testing.T, c *Conn, payload []byte) {
	t.Helper()
	if err := c.writePacketLocked(&c.spaces[spaceInitial], spaceInitial, payload, payload, sendOpts{}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// A gathered Initial counts against the amplification limit as the room it
// will pad, and joins a datagram it fills exactly.
func TestGatheredInitialAtItsEdges(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	c.gather = true
	c.amplActive, c.amplRecv, c.amplSent = true, 400, 0
	c.dgram = make([]byte, 100)
	writeInitial(t, c, []byte{framePing})
	if len(c.spaces[spaceInitial].retrans) != 0 {
		t.Error("an Initial padding to 1200 beside 100 gathered bytes was withheld under a 1200-byte budget")
	}
	c.mu.Unlock()

	c = newIdleServerConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gather = true
	payload := make([]byte, 100)
	payload[0] = framePing
	gathered := 1200 - (c.packetOverhead(spaceInitial) + len(payload))
	c.dgram = make([]byte, gathered)
	writeInitial(t, c, payload)
	if len(c.dgram) != gathered {
		t.Errorf("a packet filling the datagram exactly flushed it (%d bytes gathered)", len(c.dgram))
	}
}

// Withholding nothing queues nothing.
func TestWithholdNothing(t *testing.T) {
	var sp space
	withholdLocked(&sp, nil, sendOpts{})
	if len(sp.retrans) != 0 {
		t.Errorf("%d entries queued for an empty payload", len(sp.retrans))
	}
}

// A connection at its confidentiality limit ends with AEAD_LIMIT_REACHED.
func TestConfidentialityLimitEndsTheConnection(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.atConfidentialityLimitLocked()
	te, ok := errors.AsType[*transportError](c.closeErr)
	if !ok || te.code != transportAEADLimitReached {
		t.Errorf("closeErr = %v", c.closeErr)
	}
}

// An ACK covering the packet that answered the peer's rotation completes
// the update; a marked packet counts toward the space's ECN total.
func TestSealBookkeeping(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	c.ku.responseDue, c.ku.responsePN, sp.largest = true, 5, 5
	c.ecn.phase = ecnCapable
	if err := c.writePacketLocked(sp, spaceApplication, appendFrame(nil, frameACK, 5, 0, 0, 0), nil, sendOpts{ackOnly: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.ku.responseDue || sp.ecnSent != 1 {
		t.Errorf("responseDue %v, ecnSent %d", c.ku.responseDue, sp.ecnSent)
	}
}

// CRYPTO beside a Retry token: chunks shrink by the token, a token that
// leaves no byte is refused, and a frame the window cannot take with 64
// bytes to spare queues.
func TestCryptoChunksAtTheirEdges(t *testing.T) {
	c := newIdleServerConn(t)
	cc := newNewReno(1200)
	c.mu.Lock()
	cc.inFlight = cc.cwnd
	c.cc = cc
	c.retryToken = make([]byte, 100)
	chunk := c.maxStreamChunk() - 2 - 100
	c.mu.Unlock()
	if err := c.sendCrypto(tls.QUICEncryptionLevelInitial, make([]byte, 3000)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	f, _, err := parseFrame(c.spaces[spaceInitial].retrans[0])
	c.mu.Unlock()
	if err != nil || len(f.Data) != chunk {
		t.Errorf("first chunk beside a 100-byte token: %d bytes, %v; want %d", len(f.Data), err, chunk)
	}

	c = newIdleServerConn(t)
	c.mu.Lock()
	c.retryToken = make([]byte, c.maxStreamChunk()-2)
	c.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- c.sendCrypto(tls.QUICEncryptionLevelInitial, []byte("x")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("a token leaving no room was taken")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendCrypto beside a token leaving no room did not return")
	}

	c = newIdleServerConn(t)
	cc = newNewReno(1200)
	c.mu.Lock()
	cc.inFlight = cc.cwnd - (6 + 63)
	c.cc = cc
	c.mu.Unlock()
	if err := c.sendCrypto(tls.QUICEncryptionLevelInitial, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.spaces[spaceInitial].retrans) != 1 {
		t.Error("a 6-byte CRYPTO frame went out with 63 bytes of window beyond it")
	}
}

// Retransmission waits for 64 bytes of window beyond the payload and for
// the pacer to hold the whole packet.
func TestRetransmissionGates(t *testing.T) {
	payload := bytes.Repeat([]byte{framePing}, 10)
	c, cc := appConn(t)
	c.mu.Lock()
	c.spaces[spaceApplication].retrans = [][]byte{payload}
	cc.inFlight = cc.cwnd - (len(payload) + 63)
	c.retransmitLocked()
	if len(c.spaces[spaceApplication].retrans) != 1 {
		t.Error("a payload went out with 63 bytes of window beyond it")
	}
	c.mu.Unlock()

	c, cc = appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	cc.cwnd = 2400
	c.spaces[spaceApplication].retrans = [][]byte{payload}
	c.pacer = pacer{burst: 1 << 20, tokens: int64(len(payload)), last: time.Now()}
	c.retransmitLocked()
	if len(c.spaces[spaceApplication].retrans) != 1 {
		t.Error("a payload went out with the pacer holding its frames but not its header")
	}
}

// carriesStream finds a STREAM frame behind others.
func TestCarriesStreamBehindAnotherFrame(t *testing.T) {
	p := streamFrame([]byte{framePing}, 4, 0, "x", false)
	if !carriesStream(p, 4) || carriesStream(p, 8) {
		t.Errorf("carriesStream(PING + STREAM 4): 4 %v, 8 %v", carriesStream(p, 4), carriesStream(p, 8))
	}
}

// fitFrames takes whole frames up to the room exactly, and a payload that
// fits leaves no rest; splitDataFrame splits only with a byte on each side.
func TestFitFramesAtItsEdges(t *testing.T) {
	pings := []byte{framePing, framePing, framePing}
	if now, rest := fitFrames(pings, 3); len(now) != 3 || rest != nil {
		t.Errorf("a payload of the room: %d now, rest %v", len(now), rest)
	}
	if now, rest := fitFrames(pings, 2); len(now) != 2 || len(rest) != 1 {
		t.Errorf("three PINGs in two bytes: %d now, %d rest", len(now), len(rest))
	}
	f, _, err := parseFrame(streamFrame(nil, 4, 0, "0123456789", false))
	if err != nil {
		t.Fatal(err)
	}
	const header = 4 // type, offset, length, stream ID: a byte each
	for _, room := range []int{header, header + 10} {
		if first, _ := splitDataFrame(f, room); first != nil {
			t.Errorf("split in %d bytes: % x", room, first)
		}
	}
	if first, _ := splitDataFrame(f, header+1); first == nil {
		t.Error("no split with one byte of data in room")
	}
}

// staleBlocked is a DATA_BLOCKED at a limit the connection is not at:
// pruned before any resend.
func staleBlocked() []byte { return appendFrame(nil, frameDataBlocked, 999) }

// A probe skips payloads pruning empties and sends two packets of what is
// left.
func TestProbeSkipsEmptiedPayloads(t *testing.T) {
	for _, fromSent := range []bool{false, true} {
		c, _ := appConn(t)
		c.mu.Lock()
		sp := &c.spaces[spaceApplication]
		if fromSent {
			sp.sent.record(sentPacket{pn: 0, ackEliciting: true, payload: staleBlocked()})
			sp.sent.record(sentPacket{pn: 1, ackEliciting: true, payload: []byte{framePing}})
			sp.nextPN = 2
		} else {
			sp.retrans = [][]byte{staleBlocked(), {framePing}}
		}
		before := sp.nextPN
		c.sendProbeLocked(spaceApplication)
		if got := sp.nextPN - before; got != 2 {
			t.Errorf("payloads from the sent record %v: %d probe packets, want 2", fromSent, got)
		}
		c.mu.Unlock()
	}
}

// A server resends its Initial flight early only with a payload to resend.
func TestEarlyResendNeedsAPayload(t *testing.T) {
	c := newIdleServerConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resendEarlyLocked()
	sp := &c.spaces[spaceInitial]
	sp.sent.record(sentPacket{pn: 0, ackEliciting: true})
	sp.nextPN = 1
	c.resendEarlyLocked()
	if c.earlyResends != 0 {
		t.Errorf("%d early resends with nothing to resend", c.earlyResends)
	}
	sp.retrans = [][]byte{{framePing}}
	c.resendEarlyLocked()
	if c.earlyResends != 1 {
		t.Errorf("%d early resends with a payload queued, want 1", c.earlyResends)
	}
}

// The *_BLOCKED frames and their re-announcement at their edges.
func TestBlockedFramesAtTheirEdges(t *testing.T) {
	c := boundsConn(t, false)
	s := newStream(c, 0)
	c.mu.Lock()
	c.connSend = sendQuota{max: 10, sent: 10}
	s.sendQ = sendQuota{max: 5}
	c.idleTimeout = 0
	c.sendBlockedFramesLocked(s)
	if !bytes.Contains(c.control, appendFrame(nil, frameDataBlocked, 10)) {
		t.Errorf("no DATA_BLOCKED at the limit: % x", c.control)
	}
	if !c.blockedResend.IsZero() {
		t.Error("a re-announcement armed with no idle timeout")
	}
	c.blockedResend = time.Time{}
	c.idleTimeout = 30 * time.Second
	c.connSend.blockedSent = false
	c.sendBlockedFramesLocked(s)
	if c.blockedResend.IsZero() {
		t.Error("no re-announcement armed under an idle timeout")
	}

	c.hasPeerParams, c.peerParams.MaxIdleTimeout = true, 9*time.Second
	if got := c.blockedResendInterval(); got != 3*time.Second {
		t.Errorf("interval under a 9 s peer timeout = %v, want 3s", got)
	}
	c.peerParams.MaxIdleTimeout = 15 * time.Millisecond
	if got := c.blockedResendInterval(); got != 10*time.Millisecond {
		t.Errorf("interval under a 15 ms peer timeout = %v, want the 10 ms floor", got)
	}

	c.control = nil
	s.sendQ = sendQuota{max: 0, blockedSent: true}
	c.streams[0] = s
	c.resendBlockedFramesLocked(time.Now())
	c.mu.Unlock()
	if !bytes.Contains(c.control, appendFrame(nil, frameStreamDataBlocked, 0, 0)) {
		t.Errorf("no STREAM_DATA_BLOCKED re-announced: % x", c.control)
	}
}

// writeBatched sends a 1-RTT packet of n payload bytes into the open batch.
func writeBatched(t *testing.T, c *Conn, n int) {
	t.Helper()
	payload := make([]byte, n)
	payload[0] = framePing
	if err := writeApp(t, c, payload, sendOpts{}); err != nil {
		t.Fatal(err)
	}
}

// batchConn is an application-space connection batching its writes.
func batchConn(t *testing.T) *Conn {
	t.Helper()
	c, _ := appConn(t)
	c.mu.Lock()
	c.batching = true
	c.ecn.phase = ecnFailed
	c.mu.Unlock()
	return c
}

// The batch at its edges: equal sizes join, a shorter one closes it, it
// stops at 65507 bytes exactly, and one datagram is not a segmented write.
func TestBatchAtItsEdges(t *testing.T) {
	c := batchConn(t)
	c.mu.Lock()
	writeBatched(t, c, 500)
	writeBatched(t, c, 500)
	if c.batchN != 2 {
		t.Errorf("two equal packets: %d in the batch", c.batchN)
	}
	writeBatched(t, c, 200)
	writeBatched(t, c, 500)
	if c.batchN != 1 {
		t.Errorf("a packet after a shorter one: %d in the batch, want a new batch of 1", c.batchN)
	}
	c.mu.Unlock()

	c = batchConn(t)
	c.mu.Lock()
	writeBatched(t, c, 1000)
	base := c.batchSeg - 1000
	c.flushBatchLocked()
	// 63 segments of 1030 bytes and a last one of 616 make 65507 exactly.
	for range 63 {
		writeBatched(t, c, 1030-base)
	}
	writeBatched(t, c, maxBatchBytes-63*1030-base)
	if c.batchN != 64 || len(c.batch) != maxBatchBytes {
		t.Errorf("a batch reaching %d bytes exactly: %d packets, %d bytes", maxBatchBytes, c.batchN, len(c.batch))
	}
	c.mu.Unlock()

	c = batchConn(t)
	c.mu.Lock()
	for range 60 {
		writeBatched(t, c, 1100)
	}
	if c.batchN >= 60 {
		t.Errorf("%d packets in one batch, past %d bytes", c.batchN, maxBatchBytes)
	}
	c.mu.Unlock()

	c = batchConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	writeBatched(t, c, 500)
	c.flushBatchLocked()
	if c.segmented != 0 {
		t.Error("a batch of one datagram counted as a segmented write")
	}
}
