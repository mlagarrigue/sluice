package quic

import (
	"encoding/binary"
	"net"
	"syscall"
	"testing"
	"unsafe"
)

// UDP_SEND_MSG_SIZE travels as WSA_CMSG_SPACE(sizeof(DWORD)) bytes: the
// WSACMSGHDR, then the segment size as a DWORD at WSA_CMSG_DATA, the
// layout msquic writes (datapath_winuser.c). A wrong width or offset would
// leave Winsock reading another size than the one the batch was cut to.
func TestSegmentSizeMessageLayout(t *testing.T) {
	oob := appendSegmentSize([]byte{0xAA}, 1252)[1:]
	ptr := int(unsafe.Sizeof(uintptr(0)))
	hdr := ptr + 8
	if want := hdr + (4+ptr-1)&^(ptr-1); len(oob) != want {
		t.Fatalf("message is %d bytes, want WSA_CMSG_SPACE(4) = %d", len(oob), want)
	}
	length := int(binary.NativeEndian.Uint32(oob))
	if ptr == 8 {
		length = int(binary.NativeEndian.Uint64(oob)) //nolint:gosec // G115: a small test value
	}
	level := binary.NativeEndian.Uint32(oob[ptr:])
	typ := binary.NativeEndian.Uint32(oob[ptr+4:])
	size := binary.NativeEndian.Uint32(oob[hdr:])
	if length != hdr+4 || level != 17 || typ != 2 || size != 1252 {
		t.Fatalf("cmsg_len %d level %d type %d size %d, want %d 17 2 1252", length, level, typ, size, hdr+4)
	}
}

// The three codes Winsock gives an option it does not apply mean a
// refusal, and the write falls back; a full buffer or a port unreachable
// is the write's own failure, which the fallback must not hide.
func TestRefusedSegmentationCodes(t *testing.T) {
	for _, err := range []error{wsaEINVAL, wsaENOPROTOOPT, wsaEOPNOTSUPP} {
		if !refusedSegmentation(err) {
			t.Errorf("%v not taken for a refusal", err)
		}
	}
	const wsaENOBUFS, wsaECONNRESET syscall.Errno = 10055, 10054
	for _, err := range []error{nil, wsaENOBUFS, wsaECONNRESET} {
		if refusedSegmentation(err) {
			t.Errorf("%v taken for a refusal", err)
		}
	}
}

// The answer kept for the process is Winsock's about the option, never a
// failure of the socket that asked: a closed socket first leaves the
// question open, and the next, open one answers it.
func TestSegmentationIsAskedAgainAfterAClosedSocket(t *testing.T) {
	segmentation.mu.Lock()
	decided, offered := segmentation.decided.Load(), segmentation.offered.Load()
	segmentation.decided.Store(false)
	segmentation.offered.Store(false)
	segmentation.mu.Unlock()
	t.Cleanup(func() {
		segmentation.decided.Store(decided)
		segmentation.offered.Store(offered)
	})

	closed, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	if offersSegmentation(closed) || segmentation.decided.Load() {
		t.Fatal("a closed socket decided the process's answer")
	}
	open, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	got := offersSegmentation(open)
	if !segmentation.decided.Load() {
		t.Fatal("an open socket left the question open")
	}
	t.Logf("UDP_SEND_MSG_SIZE offered: %v", got)
}
