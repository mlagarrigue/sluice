package quic_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// A datagram overtaken by two of its successors is reordered, not lost:
// RFC 9002 §6.1.1's packet threshold of three exists so that it is not
// called lost. So a path that reorders two datagrams in a hundred by that
// count, and loses nothing, costs no retransmission at all — for every
// controller, since loss detection is not theirs. The wait is bounded at a
// quarter of a millisecond in both directions, far inside §6.1.2's time
// threshold of 9/8 of a round trip, so only the count is under test.
//
// This found the skipped packet numbers of RFC 9000 §21.4 counted in the
// threshold (sentTracker.skippedAbove): one to thirteen spurious losses
// per transfer, on one run in eight. A threshold lowered to one declares
// every reordered datagram lost (35 to 49 per transfer); one lowered to two
// mostly does not, since the receiver acknowledges the first successor at
// once (it opens a gap, §13.2.1) and the second only with the next packet,
// by which time the held one has arrived. The time-based
// BenchmarkTransferReorder cannot gate either: 21 to 55 losses over 24
// transfers of one binary, a threshold of two moving the median from 34 to
// 40.5, inside the spread of an eight-round median.
//
// BBR is out, by name: about one transfer in a hundred, a packet one behind
// the largest acknowledged is still unacknowledged 9/8 of a round trip after
// it was sent, and the time threshold declares it — the packet count is
// not what fails, and what holds that acknowledgement is not traced yet.
func TestReorderUnderThresholdIsNoLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("three 4 MiB transfers over an emulated path")
	}
	prof := netemProfile{
		owd:          10 * time.Millisecond,
		reorder:      0.02,
		reorderDepth: 2,
		reorderDelay: 250 * time.Microsecond,
	}
	for _, cc := range []quic.CongestionAlgorithm{quic.CongestionNewReno, quic.CongestionCubic, quic.CongestionBBRv1} {
		t.Run(cc.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				tr, err := transferOnce(t, &netemLink{prof: prof, cc: cc}, 4<<20)
				if err != nil {
					t.Fatal(err)
				}
				if tr.stats.LostPackets != 0 || tr.stats.BytesRetransmitted != 0 {
					t.Fatalf("%d packets declared lost, %d bytes retransmitted, on a path that only reorders under the threshold; want 0",
						tr.stats.LostPackets, tr.stats.BytesRetransmitted)
				}
			})
		})
	}
}
