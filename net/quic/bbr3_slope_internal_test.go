package quic

import "testing"

// BBRRaiseInflightLongtermSlope spreads one datagram over a window of
// acknowledgements the first round of ProbeBW_UP, two the next, and so on,
// a datagram at least. Computed as the draft writes it, in int, the product
// cwnd*maxDatagram passed 2^31 on a 32-bit platform from a two-megabyte
// window, so the first round added a datagram per datagram acknowledged,
// and the divisor maxDatagram<<rounds reached zero by round 28 and the
// division panicked. GOARCH=386 runs this test as it runs on amd64.
func TestBBR3SlopeHoldsOnAnyIntSize(t *testing.T) {
	b := newBBR3(1200, &deliveryEstimator{})
	b.cwnd = 2_000_000
	b.bwProbeUpRounds = 0
	b.raiseInflightLongtermSlope()
	if b.probeUpCnt != b.cwnd {
		t.Errorf("the first round's probeUpCnt = %d, want the window, %d", b.probeUpCnt, b.cwnd)
	}
	for range 40 {
		b.raiseInflightLongtermSlope() // up to and past round 30, without a panic
	}
	if b.bwProbeUpRounds != 30 || b.probeUpCnt != b.maxDatagram {
		t.Errorf("after 40 rounds: %d rounds, probeUpCnt %d; want 30 and a datagram, %d", b.bwProbeUpRounds, b.probeUpCnt, b.maxDatagram)
	}
}
