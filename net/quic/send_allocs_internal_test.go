package quic

import (
	"testing"
	"time"
)

// Sealing a packet into a buffer that has room allocates nothing: the nonce
// and the header-protection mask go through interfaces, and a local of
// either escaped — two allocations per packet sent, on every stream write.
func TestSealAllocatesNothing(t *testing.T) {
	secrets, err := initialSecrets([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(secrets.Server)
	if err != nil {
		t.Fatal(err)
	}
	// A short header: the first byte, an eight-byte connection ID, a
	// four-byte packet number.
	header := []byte{0x43, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0}
	const pnOffset, pnLen = 9, 4
	payload := make([]byte, 1200)
	dst := make([]byte, 0, 1500)
	pn := uint64(0)
	allocs := testing.AllocsPerRun(100, func() {
		pn++
		if _, err := sealer.Seal(dst[:0], header, payload, pn, pnOffset, pnLen); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("sealing a packet allocates %.0f times, want none", allocs)
	}
}

// The sent record of a steady flow — packets recorded at its end, freed
// from its front as ACKs arrive — stays in one array: the record moves back
// to the array's start rather than growing a new one each time its end is
// reached, and the slots it leaves keep no payload reachable.
func TestSentRecordOfASteadyFlowKeepsItsArray(t *testing.T) {
	var s sentTracker
	pn := uint64(0)
	send := func() {
		s.record(sentPacket{pn: pn, payload: []byte{byte(pn)}})
		pn++
	}
	ack := func() { s.closeGap(s.packets[:0], 1) } // the oldest, as an ACK frees it
	for range 64 {
		send()
	}
	allocs := testing.AllocsPerRun(10, func() {
		for range 1000 {
			send()
			ack()
		}
	})
	// The payloads the test makes are one allocation each.
	if allocs > 1000 {
		t.Errorf("a thousand packets through a record of 64 allocate %.0f times beyond their payloads", allocs-1000)
	}
	if len(s.packets) != 64 {
		t.Fatalf("%d packets recorded, want 64", len(s.packets))
	}
	for i, p := range s.packets {
		if p.pn != pn-64+uint64(i) {
			t.Fatalf("slot %d holds packet %d, want %d", i, p.pn, pn-64+uint64(i))
		}
	}
	for i, p := range s.store[:cap(s.store)] {
		if p.payload == nil {
			continue
		}
		live := false
		for j := range s.packets {
			if &s.store[i] == &s.packets[j] {
				live = true
				break
			}
		}
		if !live {
			t.Errorf("slot %d of the array, outside the record, still holds packet %d's payload", i, p.pn)
		}
	}
}

// A STREAM frame's buffer is handed back when the peer acknowledges its
// packet, and only then: a probe that lifts the payload takes the buffer
// with it, since the probe's packet will be read again and resent, and a
// buffer reused under it would be written over by the next frame; a
// payload that is not spare is the garbage collector's.
func TestStreamFrameBufferReturnsOnAcknowledgementAlone(t *testing.T) {
	var back [][]byte
	s := sentTracker{handBack: func(frame []byte) { back = append(back, frame) }}
	take := func(text string) []byte {
		frame, spare := takeStreamFrame(len(text))
		if !spare {
			t.Fatalf("a %d-byte frame is not built in a spare buffer", len(text))
		}
		return append(frame, text...)
	}
	lifted, acked := take("lifted"), take("acked")
	s.record(sentPacket{pn: 1, ackEliciting: true, payload: lifted, spare: true})
	s.record(sentPacket{pn: 2, ackEliciting: true, payload: acked, spare: true})
	s.record(sentPacket{pn: 3, ackEliciting: true, payload: []byte("collected")})

	payload, ok := s.liftOldestResendable()
	if !ok || string(payload) != "lifted" {
		t.Fatalf("lifted %q, %v", payload, ok)
	}
	if s.packets[0].spare {
		t.Error("the record kept the mark of a payload a probe lifted")
	}
	for i := range s.packets {
		s.packets[i].acked = true
	}
	out := ackOutcome{newlyAckedCount: 3}
	s.release(time.Now(), &rttEstimator{}, nil, nil, &out)
	if len(back) != 1 || string(back[0]) != "acked" {
		t.Fatalf("handed back %q, want the acknowledged spare frame alone", back)
	}

	// The buffer goes back whole: a frame that starts at its first byte
	// converts back to the array it was cut from.
	releaseStreamFrame(back[0])

	if _, spare := takeStreamFrame(len(frameBuf{}) + 1); spare {
		t.Error("a frame larger than a spare buffer was given one")
	}
}
