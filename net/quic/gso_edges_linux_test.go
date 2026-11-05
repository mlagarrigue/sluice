package quic

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// The UDP_SEGMENT control message carries its level, type and size where
// the kernel reads them.
func TestSegmentSizeMessage(t *testing.T) {
	msgs, err := syscall.ParseSocketControlMessage(appendSegmentSize(nil, 1200))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("%d messages, %v", len(msgs), err)
	}
	m := msgs[0]
	if m.Header.Level != solUDP || m.Header.Type != udpSegment || len(m.Data) < 2 || int(m.Data[0])|int(m.Data[1])<<8 != 1200 {
		t.Errorf("level %d type %d data % x", m.Header.Level, m.Header.Type, m.Data)
	}
}

// Segments marked ECT(0) arrive marked.
func TestSegmentsCarryTheirECNMark(t *testing.T) {
	recv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer recv.Close()
	send, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer send.Close()
	if !ecnSocket(recv) || !ecnSocket(send) {
		t.Skip("no ECN on this platform")
	}
	_, ok, err := writeSegments(send, make([]byte, 200), 100, recv.LocalAddr(), ecnECT0, nil)
	if err != nil || !ok {
		t.Skipf("no segmentation here: %v, %v", ok, err)
	}
	_ = recv.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf, oob := make([]byte, 1500), make([]byte, 128)
	for range 2 {
		_, _, ecn, err := readDatagram(recv, true, buf, oob)
		if err != nil {
			t.Fatal(err)
		}
		if ecn != ecnECT0 {
			t.Errorf("a segment arrived with ECN %d, want ECT(0)", ecn)
		}
	}
}
