package quic

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// PATH_CHALLENGE after PATH_CHALLENGE on the validated path, never an ACK:
// the answers stop once the congestion window is full (RFC 9000 §8.2.2,
// "unless constrained by congestion control"), so neither the sent record
// nor the bytes in flight grow past it.
func TestPathResponsesStopAtTheCongestionWindow(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
	defer c.Close()
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

	// Drain the peer socket so the sends never block on a full buffer.
	done := make(chan int)
	go func() {
		buf := make([]byte, 2048)
		total := 0
		for {
			_ = pcB.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, _, err := pcB.ReadFrom(buf)
			if err != nil {
				done <- total
				return
			}
			total += n
		}
	}()

	const challenges = 2000
	for i := range challenges {
		payload := varint.Append(nil, framePathChallenge)
		payload = append(payload, byte(i), byte(i>>8), 3, 4, 5, 6, 7, 8)
		if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
			t.Fatalf("challenge %d: %v", i, err)
		}
	}
	received := <-done

	c.mu.Lock()
	tracked := len(c.spaces[spaceApplication].sent.packets)
	inFlight := c.cc.bytesInFlight()
	roomFor1200 := c.cc.canSend(1200)
	c.mu.Unlock()
	t.Logf("challenges=%d tracked=%d inFlight=%d canSend(1200)=%v bytesReceivedByPeer=%d", challenges, tracked, inFlight, roomFor1200, received)

	const bound = 64
	if tracked > bound {
		t.Errorf("sent record holds %d packets after %d unacknowledged challenges; want at most %d", tracked, challenges, bound)
	}
	if inFlight > 64*1200 {
		t.Errorf("bytes in flight = %d without any ACK; the window was crossed by ungated PATH_RESPONSE", inFlight)
	}
}

// A peer opens a bidirectional stream with a FIN and a STOP_SENDING, again
// and again, opening only what the server granted (localMaxStreamsBidi);
// every "packet" ends as the read loop ends one (flushAcksLocked,
// retransmitLocked, flushControlLocked), so a MAX_STREAMS earned leaves
// before the next stream opens. Each STOP_SENDING is answered with an
// ungated RESET_STREAM; the stream's slot comes back only once that reset
// is no longer in flight, so a peer that never acknowledges runs out of
// streams and the resets in flight stay under its limit. Two consumers:
// batch mode, and AcceptStream with the application reading every stream
// to EOF.
func unackedResetServer(t *testing.T) *Conn {
	t.Helper()
	c := newConn(&fuzzSink{addr: "unacked-reset:self"}, fakeFuzzAddr("unacked-reset:peer"),
		[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(sec.Server)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = sealer
	c.handshakeConfirmed = true
	c.mu.Unlock()
	return c
}

func unackedResetDrive(t *testing.T, c *Conn, after func(id uint64)) (opened uint64, limit uint64) {
	t.Helper()
	c.mu.Lock()
	limit = c.params.InitialMaxStreamsBidi
	c.mu.Unlock()
	now := time.Now()
	for i := range 4 * limit {
		c.mu.Lock()
		granted := c.localMaxStreamsBidi
		c.mu.Unlock()
		if i >= granted {
			break // a peer that respects the limit stops here
		}
		id := 4 * i
		payload := varint.Append(nil, FrameStream|streamLEN|streamFIN)
		payload = varint.Append(payload, id)
		payload = varint.Append(payload, 0)
		payload = appendFrame(payload, FrameStopSending, id, 0x10c)
		if err := c.frames(spaceApplication, payload, now); err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		c.mu.Lock()
		c.flushAcksLocked(now)
		c.retransmitLocked()
		c.flushControlLocked()
		c.mu.Unlock()
		if after != nil {
			after(id)
		}
		opened = i + 1
	}
	return opened, limit
}

func unackedResetCount(c *Conn) (resetPackets, inLedger, resets int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	for i := range sp.sent.packets {
		p := sp.sent.packets[i].payload
		for n := 0; n < len(p); {
			f, tail, err := parseFrame(p[n:])
			if err != nil {
				break
			}
			if f.Type == FrameResetStream {
				resetPackets++
				break
			}
			n = len(p) - len(tail)
		}
	}
	return resetPackets, len(sp.sent.packets), len(c.resetIDs)
}

func TestUnacknowledgedResetsStayUnderTheStreamLimitInBatchMode(t *testing.T) {
	c := unackedResetServer(t)
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })
	opened, limit := unackedResetDrive(t, c, nil)
	rp, all, ids := unackedResetCount(c)
	t.Logf("batch: limit %d, streams opened within grants %d, RESET_STREAM packets in flight %d, ledger %d, resetIDs %d",
		limit, opened, rp, all, ids)
	if uint64(rp) > limit+16 || uint64(ids) > limit+16 {
		t.Fatalf("batch mode: %d streams opened within the grants (initial limit %d) left %d RESET_STREAM packets in flight and %d reset identifiers",
			opened, limit, rp, ids)
	}
}

func TestUnacknowledgedResetsStayUnderTheStreamLimitWhenAccepted(t *testing.T) {
	c := unackedResetServer(t)
	opened, limit := unackedResetDrive(t, c, func(id uint64) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := c.AcceptStream(ctx)
		if err != nil {
			t.Fatalf("accept after stream %d: %v", id, err)
		}
		if _, err := io.ReadAll(s); err != nil {
			t.Fatalf("read stream %d: %v", id, err)
		}
		c.mu.Lock()
		c.flushControlLocked()
		c.mu.Unlock()
	})
	rp, all, ids := unackedResetCount(c)
	t.Logf("accept: limit %d, streams opened within grants %d, RESET_STREAM packets in flight %d, ledger %d, resetIDs %d",
		limit, opened, rp, all, ids)
	if uint64(rp) > limit+16 || uint64(ids) > limit+16 {
		t.Fatalf("accept mode: %d streams opened within the grants (initial limit %d) left %d RESET_STREAM packets in flight and %d reset identifiers",
			opened, limit, rp, ids)
	}
}

// ackInFlight has the peer acknowledge every application packet in flight,
// then runs the replay pass that ends the read loop's turn — where the
// reset records nothing predates go, and the streams they held retire.
func ackInFlight(t *testing.T, c *Conn) {
	t.Helper()
	// The tracked numbers as ranges, highest first, as an ACK frame writes
	// them.
	c.mu.Lock()
	var ranges []pnRange
	for _, p := range c.spaces[spaceApplication].sent.packets {
		if n := len(ranges); n > 0 && ranges[0].hi+1 == p.pn {
			ranges[0].hi = p.pn
			continue
		}
		ranges = append([]pnRange{{lo: p.pn, hi: p.pn}}, ranges...)
	}
	c.mu.Unlock()
	if len(ranges) == 0 {
		return
	}
	if err := c.frames(spaceApplication, appendAckFrame(nil, ranges, 0, [3]uint64{}), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.retransmitLocked()
	c.mu.Unlock()
}

// The other half: once the peer acknowledges the resets, their records go
// and the streams they held retire, so the slots come back and the peer may
// open as many streams again.
func TestAcknowledgedResetsGiveTheirStreamSlotsBack(t *testing.T) {
	c := unackedResetServer(t)
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })
	opened, _ := unackedResetDrive(t, c, nil)

	c.mu.Lock()
	held, granted := len(c.streams), c.localMaxStreamsBidi
	c.mu.Unlock()
	// The first reset's record goes at once: nothing older than it is in
	// flight (pruneResetIDsLocked), so its stream retires; every other one
	// holds its slot.
	if held != int(opened)-1 { //nolint:gosec // G115: a stream count under the limit
		t.Fatalf("%d streams held before any ACK, want %d of the %d opened", held, opened-1, opened)
	}

	ackInFlight(t, c)
	c.mu.Lock()
	held, resets := len(c.streams), len(c.resetIDs)
	regranted := c.localMaxStreamsBidi
	c.mu.Unlock()
	if held != 0 || resets != 0 {
		t.Fatalf("after the ACK: %d streams held, %d reset records; want none", held, resets)
	}
	if regranted <= granted {
		t.Fatalf("MAX_STREAMS stayed at %d after every reset was acknowledged", regranted)
	}
}

// A PATH_CHALLENGE on the path in use that finds the congestion window full
// is answered once the window opens, not dropped: RFC 9000 §8.2.2 lets
// congestion control delay a PATH_RESPONSE ("MUST NOT delay ... unless
// constrained by congestion control"), not suppress it. A peer validating
// the path during a bulk transfer from this end otherwise saw every
// challenge go unanswered. The control, with the window open, checks the
// answer can be seen at all.
func TestAPathResponseHeldByAFullWindowLeavesWhenItOpens(t *testing.T) {
	data := []byte{9, 8, 7, 6, 5, 4, 3, 2}
	challenge := append(varint.Append(nil, framePathChallenge), data...)
	answered := func(pc *recordingPC) bool {
		for _, w := range pc.recorded() {
			for _, f := range openShort(t, w.data, len("peercid0")) {
				if f.Type == framePathResponse && bytes.Equal(f.Data, data) {
					return true
				}
			}
		}
		return false
	}

	c0, pc0 := newMigratableServer(t, "held-ctl")
	c0.mu.Lock()
	c0.rxFrom, c0.rxVia, c0.rxDatagramLen, c0.rxHighest = c0.peer, nil, 1200, true
	c0.mu.Unlock()
	if err := c0.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !answered(pc0) {
		t.Fatal("control: no PATH_RESPONSE even with the window open")
	}

	c, pc := newMigratableServer(t, "held-full")
	c.mu.Lock()
	for c.cc.canSend(1200) {
		c.cc.onSent(time.Now(), 1200)
	}
	c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = c.peer, nil, 1200, true
	c.mu.Unlock()
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	if answered(pc) {
		t.Fatal("precondition lost: the answer left through a full window")
	}
	c.mu.Lock()
	c.cc.onLifted(c.cc.bytesInFlight())
	c.mu.Unlock()
	if err := c.frames(spaceApplication, varint.Append(nil, framePing), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.flushControlLocked()
	c.flushBatchLocked()
	c.mu.Unlock()
	if !answered(pc) {
		t.Errorf("the challenge met a full window and the window opened: still no PATH_RESPONSE in %d datagrams", len(pc.recorded()))
	}
}

// BenchmarkPruneResetIDsBehindAnOldPacket is the end of every packet a peer
// sends while it leaves a thousand RESET_STREAMs and an older packet
// unacknowledged: nothing can be forgotten.
func BenchmarkPruneResetIDsBehindAnOldPacket(b *testing.B) {
	c := assembleConn(&fuzzSink{addr: "prune:self"}, fakeFuzzAddr("prune:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultConfig())
	b.Cleanup(func() { _ = c.Close() })
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	sp.sent.packets = []sentPacket{{pn: 0, ackEliciting: true}}
	sp.nextPN = 1
	for id := range StreamID(1000) {
		c.noteResetLocked(4 * id)
		sp.nextPN++
	}
	b.ReportAllocs()
	for b.Loop() {
		c.pruneResetIDsLocked()
	}
	if len(c.resetIDs) != 1000 {
		b.Fatalf("%d reset streams remembered, want the 1000", len(c.resetIDs))
	}
}

// A peer that leaves its RESET_STREAMs and an older packet unacknowledged
// keeps up to its stream limit of reset records, and every packet it sends
// ends with a prune. The prune walked every record each time, though
// nothing could be forgotten until the oldest packet in flight moved; it
// walks only once that packet reaches the lowest mark, and forgets one
// record at least when it does.
func TestPruneSkipsTheResetRecordsWhileTheOldestPacketStays(t *testing.T) {
	c := unackedResetServer(t)
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })
	unackedResetDrive(t, c, nil)
	c.mu.Lock()
	records, walks := len(c.resetIDs), c.resetWalks
	c.mu.Unlock()
	if records < 2 {
		t.Fatalf("%d reset records after the drive, want many", records)
	}

	now := time.Now()
	for range 1000 {
		if err := c.frames(spaceApplication, []byte{framePing}, now); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		c.retransmitLocked()
		c.mu.Unlock()
	}
	c.mu.Lock()
	sp := &c.spaces[spaceApplication]
	oldest, least, left := sp.sent.packets[0].pn, c.resetMinMark, len(c.resetIDs)
	walked := c.resetWalks - walks
	for _, mark := range c.resetIDs {
		if mark < least {
			t.Errorf("a record at %d under resetMinMark %d", mark, least)
		}
	}
	c.mu.Unlock()
	if left != records {
		t.Fatalf("%d reset records after a thousand PINGs, want the %d", left, records)
	}
	if walked != 0 {
		t.Errorf("%d walks of the %d reset records over a thousand PINGs that moved nothing, want none", walked, records)
	}
	if oldest >= least {
		t.Fatalf("the oldest packet in flight %d is at or past the lowest mark %d: every packet walks the records", oldest, least)
	}

	ackInFlight(t, c)
	c.mu.Lock()
	left = len(c.resetIDs)
	c.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d reset records after everything was acknowledged, want none", left)
	}
}
