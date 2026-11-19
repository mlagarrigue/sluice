package quic

import "testing"

// countOffPathAnswerLocked admits an answer toward another path while fewer
// than maxOffPathAnswers off-path packets wait for their acknowledgement;
// an acknowledgement gives a place back, and so must a loss. A count that
// only grew on loss would refuse every later PATH_CHALLENGE from another
// path once that many answers had been lost.
func TestAnOffPathPacketDeclaredLostLeavesTheQuota(t *testing.T) {
	s := &sentTracker{}
	for pn := range uint64(maxOffPathAnswers) {
		s.record(sentPacket{pn: pn, size: 100, offPath: true, payload: []byte{framePing}})
	}
	if s.offPathOut != maxOffPathAnswers {
		t.Fatalf("offPathOut = %d after %d off-path packets, want all of them", s.offPathOut, maxOffPathAnswers)
	}
	var out ackOutcome
	for i := range s.packets {
		s.declareLost(&out, &s.packets[i], nil)
		if want := maxOffPathAnswers - 1 - i; s.offPathOut != want {
			t.Fatalf("offPathOut = %d after %d off-path packets declared lost, want %d", s.offPathOut, i+1, want)
		}
	}
}
