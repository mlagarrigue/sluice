package quic

import (
	"context"
	"crypto/tls"
	"testing"
	"time"
)

// RFC 9000 §14: a datagram larger than the path carries, or than the peer's
// max_udp_payload_size, may be discarded by the peer (§14.1). The ACK rides
// every packet that owes one, and its size is decided by the peer: 32
// ranges of received packets (maxAckRanges) make it a few hundred bytes,
// which a full stream chunk leaves no room for — the datagram used to leave
// oversized, on a data flight, whenever the peer had opened gaps.
func TestLongAckBesideFullChunkStaysUnderMaxDatagram(t *testing.T) {
	const ranges = maxAckRanges
	for _, tc := range []struct {
		name string
		gaps bool
	}{
		{"with gaps", true},
		{"without gaps", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tap := sealedAppConn(t)
			now := time.Now()
			c.mu.Lock()
			sp := &c.spaces[spaceApplication]
			if tc.gaps {
				// Every other packet number, spaced so each gap takes a
				// four-byte varint: the widest ACK one space can owe.
				for i := range uint64(ranges) {
					sp.ack.record(1+i<<20, now)
				}
				sp.ack.onAckEliciting(spaceApplication, now, false)
			} else {
				sp.ack.record(1, now)
				sp.ack.onAckEliciting(spaceApplication, now, false)
			}
			chunk := c.maxStreamChunk()
			limit := c.maxDatagram
			c.peerMaxStreamsBidi = 1 // the grant the handshake would have brought
			c.mu.Unlock()

			s, err := c.Stream(0)
			if err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			s.sendQ.max = 1 << 20
			c.mu.Unlock()
			if _, err := s.Write(make([]byte, chunk)); err != nil {
				t.Fatal(err)
			}
			writes := tap.recorded()
			for _, w := range writes {
				if len(w.data) > limit {
					t.Errorf("a datagram of %d bytes left; maxDatagram is %d", len(w.data), limit)
				}
			}
			c.mu.Lock()
			owed := sp.ack.owes(time.Now())
			c.mu.Unlock()
			if owed {
				t.Error("the ACK is still owed after the chunk left")
			}
			// A short ACK rides the chunk's packet; the long one leaves
			// ahead on its own, and the chunk is not cut for it.
			want := 1
			if tc.gaps {
				want = 2
			}
			if len(writes) != want {
				t.Errorf("%d datagrams left, want %d", len(writes), want)
			}
		})
	}
}

// sealedAppConn is a client with application keys and send credit, with a
// tap on its socket — enough for Stream.Write to produce packets without a
// peer at the other end.
func sealedAppConn(t *testing.T) (*Conn, *tapPC) {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	tap := newTap(pcA)
	c := newConn(tap, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
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
	c.connSend.max = 1 << 20
	c.mu.Unlock()
	return c, tap
}

// RFC 9000 §14.2: no datagram leaves larger than the path's maximum. A
// writer builds its frame for the datagram size of the moment and may wait
// for the congestion window with mu released; meanwhile the size can fall —
// a black hole detected, a migration, a Rebind — and the frame built for
// 1452 bytes would leave whole on a path just found not to carry it. The
// writer wakes to the size it finds, and its frame is cut again.
func TestAParkedWriterIsRefitToTheDatagramSizeItWakesTo(t *testing.T) {
	c, tap := sealedAppConn(t)
	c.mu.Lock()
	nr := newNewReno(1452)
	c.cc = nr
	c.maxDatagram = 1452
	c.peerMaxStreamsBidi = 1
	nr.inFlight = nr.cwnd // the window full
	c.mu.Unlock()
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	s.sendQ.max = 1 << 20
	chunk := c.maxStreamChunk()
	c.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := s.Write(make([]byte, chunk)); done <- err }()
	waitFor(t, "the writer to park on the window", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.connSend.sent > 0
	})
	time.Sleep(50 * time.Millisecond) // past the charge, into the wait
	c.mu.Lock()
	c.blackHoleLocked()
	md := c.maxDatagram
	c.cc.(*newReno).inFlight = 0
	c.cond.Broadcast()
	c.mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never woke")
	}
	if md != 1200 {
		t.Fatalf("maxDatagram is %d after the black hole, want 1200; the case under test is not reached", md)
	}
	sent := tap.recorded()
	if len(sent) == 0 {
		t.Fatal("nothing was written")
	}
	for i, w := range sent {
		if len(w.data) > md {
			t.Errorf("datagram %d of %d is %d bytes, over the %d the path was just found to carry", i+1, len(sent), len(w.data), md)
		}
	}
}

// §14.2 again, for the two packets whose header is longer than the room
// their payload was cut for: a 0-RTT packet carries a long header where
// the queue joined to it was sized for a short one, and after a Retry a
// client's Initial carries the token, which a CRYPTO chunk cut to
// maxStreamChunk does not leave room for.
func TestPacketsFitTheDatagramWhateverTheirHeader(t *testing.T) {
	t.Run("0-RTT with the control queue", func(t *testing.T) {
		tap := newTap(&fuzzSink{addr: "fit:self"})
		dcid, scid := []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 10, 11, 12, 13, 14, 15, 16}
		c := assembleConn(tap, fakeFuzzAddr("fit:peer"), dcid, scid, true, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		c.initialDCID = dcid
		c.remembered = TransportParameters{
			InitialMaxData:          1 << 20,
			InitialMaxStreamDataUni: 1 << 16,
			InitialMaxStreamsUni:    4,
			ActiveConnIDLimit:       4,
		}
		if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
			t.Fatalf("early sealer: %v", err)
		}
		srv := DefaultParameters()
		srv.initialSourceCID = dcid
		srv.originalDCID = dcid
		if err := c.handlePeerParameters(appendParameters(nil, srv)); err != nil {
			t.Fatalf("server parameters: %v", err)
		}
		s, err := c.OpenUniStream(context.Background())
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		c.mu.Lock()
		for i := range 40 {
			c.control = appendFrame(c.control, frameMaxStreams|0x01, uint64(100+i))
		}
		chunk := c.maxStreamChunk()
		md := c.maxDatagram
		c.mu.Unlock()
		if _, err := s.Write(make([]byte, chunk)); err != nil {
			t.Fatalf("writing in 0-RTT: %v", err)
		}
		for i, w := range tap.recorded() {
			if len(w.data) > md {
				t.Errorf("datagram %d is %d bytes, over maxDatagram %d", i+1, len(w.data), md)
			}
		}
	})
	t.Run("an Initial with a Retry token", func(t *testing.T) {
		tap := newTap(&fuzzSink{addr: "fit2:self"})
		dcid, scid := []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 10, 11, 12, 13, 14, 15, 16}
		c := assembleConn(tap, fakeFuzzAddr("fit2:peer"), dcid, scid, true, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		if err := c.installInitial(dcid); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		c.retryToken = make([]byte, 100)
		md := c.maxDatagram
		c.mu.Unlock()
		if err := c.sendCrypto(tls.QUICEncryptionLevelInitial, make([]byte, 3000)); err != nil {
			t.Fatalf("sendCrypto: %v", err)
		}
		sent := tap.recorded()
		if len(sent) == 0 {
			t.Fatal("nothing was written")
		}
		for i, w := range sent {
			if len(w.data) > md {
				t.Errorf("datagram %d is %d bytes, over maxDatagram %d", i+1, len(w.data), md)
			}
		}
	})
}

// RFC 9002 §7: the sender does not send packets that would exceed the
// congestion window. The writer's gate counted the stream frame and 64
// bytes of header, tag and slack; the ACK the packet then carried was
// encoded after the gate, and a peer that opened maxAckRanges gaps makes it
// a few hundred bytes — the flight left above the window by that much.
func TestWindowGateCountsTheAckTheWriterCarries(t *testing.T) {
	c, _ := sealedAppConn(t)
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	for i := range uint64(maxAckRanges) {
		sp.ack.record(1+i<<20, now)
	}
	sp.ack.onAckEliciting(spaceApplication, now, false)
	if !sp.ack.owes(now) {
		t.Fatal("the ACK is not owed")
	}
	payload := streamFrameBytes(0, 0, make([]byte, 100))
	reno := c.reno()
	reno.cwnd = reno.inFlight + len(payload) + 64 + 1
	if err := c.sendPacketLocked(spaceApplication, payload, sendOpts{waitCwnd: true}); err != nil {
		t.Fatal(err)
	}
	if in, w := c.cc.bytesInFlight(), c.cc.window(); in > w {
		t.Errorf("%d bytes in flight, the window is %d", in, w)
	}
	if sp.ack.owes(time.Now()) {
		t.Error("the ACK is still owed after the packet left")
	}
}

// RFC 9000 §13.2.1 with §8.1: an endpoint before address validation may
// not send past three times what it received, and the ACK-only packet the
// budget withholds was still settled — the debt cleared with nothing sent,
// so the ACK waited for the next ack-eliciting packet. The debt stays until
// a packet carries it: the peer's next datagram raises the budget, and the
// flush that follows its processing sends the ACK.
func TestAWithheldAckIsOwedAgainWhenTheBudgetLifts(t *testing.T) {
	c, tap := sealedAppConn(t)
	now := time.Now()
	c.mu.Lock()
	c.amplActive, c.amplRecv, c.amplSent = true, 1200, 3600 // the budget is spent
	sp := &c.spaces[spaceApplication]
	sp.ack.record(1, now)
	sp.ack.onAckEliciting(spaceApplication, now, false)
	due := now.Add(time.Second) // the delayed-ACK deadline has passed
	c.flushAcksLocked(due)
	if n := len(tap.recorded()); n != 0 {
		t.Fatalf("%d datagrams left past the budget", n)
	}
	if !sp.ack.owes(due) {
		t.Error("the withheld ACK is no longer owed")
	}
	c.mu.Unlock()

	// The timer does not spin on it while the budget stays spent: the
	// next deadline is not one already past.
	if next := c.actOnDeadlines(due); !next.IsZero() && !next.After(due) {
		t.Errorf("the timer re-arms at %v, at or before now", next.Sub(due))
	}
	if n := len(tap.recorded()); n != 0 {
		t.Fatalf("%d datagrams left past the budget", n)
	}

	c.mu.Lock()
	c.amplRecv += 50 // the client's next datagram, an ACK of its own
	c.flushAcksLocked(due)
	owed := sp.ack.owes(due)
	c.mu.Unlock()
	if n := len(tap.recorded()); n != 1 {
		t.Errorf("%d datagrams left once the budget lifted, want the ACK", n)
	}
	if owed {
		t.Error("the ACK is still owed after it left")
	}
}

// RFC 9000 §10.3: an endpoint SHOULD send packets at least 22 bytes longer
// than the connection ID length it asks the peer to use, so that a
// stateless reset the peer sends is no shorter than a valid packet to this
// end. A short-header packet is 21 bytes plus the peer's identifier plus
// the payload: a PING toward a peer with 4-byte identifiers left in 26
// bytes, under the 30 that this end's 8-byte identifiers call for.
func TestSmallestPacketReachesTheStatelessResetFloor(t *testing.T) {
	for _, dcid := range []string{"dc04", ""} {
		c, tap := sealedAppConn(t)
		now := time.Now()
		c.mu.Lock()
		c.dcid = []byte(dcid)
		floor := 22 + len(c.scid)
		if err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{}); err != nil {
			t.Fatal(err)
		}
		sp := &c.spaces[spaceApplication]
		sp.ack.record(1, now)
		sp.ack.onAckEliciting(spaceApplication, now, false)
		c.flushAcksLocked(now.Add(time.Second))
		c.mu.Unlock()
		writes := tap.recorded()
		if len(writes) != 2 {
			t.Fatalf("identifier of %d bytes: %d datagrams, want the PING and the ACK", len(dcid), len(writes))
		}
		for i, what := range []string{"PING", "ACK"} {
			if n := len(writes[i].data); n < floor {
				t.Errorf("identifier of %d bytes: the %s left in %d bytes, under the %d-byte floor", len(dcid), what, n, floor)
			}
		}
	}
}
