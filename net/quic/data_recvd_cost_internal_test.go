package quic

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// otherStreamPayload is one in-flight packet's payload: a STREAM frame of
// stream id carrying n bytes.
func otherStreamPayload(id uint64, n int) []byte {
	p := varint.Append(nil, FrameStream|streamOFF|streamLEN)
	p = varint.Append(p, id)
	p = varint.Append(p, 0)
	p = varint.Append(p, uint64(n))
	return append(p, make([]byte, n)...)
}

// dataRecvdUnderAFlight returns a connection with stream 0 in "Data Recvd" —
// its FIN sent, every byte acknowledged, so nothing tracked carries it — and
// flight packets of another stream in flight.
func dataRecvdUnderAFlight(flight int) (*Conn, *Stream) {
	c := assembleConn(&fuzzSink{addr: "drecvd:self"}, fakeFuzzAddr("drecvd:peer"), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	s := newStream(c, 0)
	s.sendClosed = true
	c.streams[0] = s
	sp := &c.spaces[spaceApplication]
	for i := range flight {
		payload := otherStreamPayload(4, 1200)
		sp.sent.record(sentPacket{pn: uint64(i), size: len(payload) + 32, payload: payload, ackEliciting: true})
	}
	return c, s
}

// RFC 9000 §3.1: "Data Recvd" is terminal. A peer may keep a stream open on
// its side and send STOP_SENDING for this end's finished direction again and
// again (§3.5 has it answered with nothing); each one is answered without a
// change of state, at a cost that does not grow with what else is in
// flight: the verdict, found once, is kept.
func TestARepeatedStopSendingOnDataRecvdChangesNothing(t *testing.T) {
	c, s := dataRecvdUnderAFlight(4096)
	t.Cleanup(func() { _ = c.Close() })
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range 3 {
		stopped, err := s.onStopSendingLocked(0x10c)
		if stopped || err != nil {
			t.Fatalf("STOP_SENDING %d on a Data Recvd stream: stopped=%v err=%v, want neither", i+1, stopped, err)
		}
		if s.writeReset {
			t.Fatalf("STOP_SENDING %d reset a direction in Data Recvd", i+1)
		}
	}
	if !s.dataRecvd {
		t.Fatal("the Data Recvd verdict was not kept: the next STOP_SENDING walks the whole flight again")
	}
}

// BenchmarkStopSendingOnDataRecvd measures one STOP_SENDING on a Data Recvd
// stream under a flight of 64 and of 4,096 packets of another stream: with
// the verdict kept, the figure does not move with the flight.
func BenchmarkStopSendingOnDataRecvd(b *testing.B) {
	for _, flight := range []int{64, 4096} {
		b.Run(fmt.Sprintf("flight=%d", flight), func(b *testing.B) {
			c, s := dataRecvdUnderAFlight(flight)
			b.Cleanup(func() { _ = c.Close() })
			c.mu.Lock()
			defer c.mu.Unlock()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if stopped, err := s.onStopSendingLocked(0); stopped || err != nil {
					b.Fatalf("stopped=%v err=%v: the stream was not read as Data Recvd", stopped, err)
				}
			}
		})
	}
}
