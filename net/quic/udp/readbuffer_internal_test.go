package udp

import (
	"errors"
	"syscall"
	"testing"
)

// Listen asks for a ReceiveBuffer-sized buffer. Linux caps the request
// without an error; Darwin refuses one above its limit, and Listen failed
// there, closing a socket that would have worked with a smaller buffer. The
// request is halved until the kernel accepts it, and only a kernel that
// refuses the floor fails Listen.
func TestReadBufferRequestHalvesUntilAccepted(t *testing.T) {
	limit := 2 << 20 // a kern.ipc.maxsockbuf lowered to 2 MiB
	var asked []int
	set := func(n int) error {
		asked = append(asked, n)
		if n > limit {
			return syscall.ENOBUFS
		}
		return nil
	}
	if err := askReadBuffer(set, ReceiveBuffer); err != nil {
		t.Fatalf("a kernel that grants 2 MiB refused Listen: %v (asked %v)", err, asked)
	}
	if got := asked[len(asked)-1]; got > limit || got < limit/2 {
		t.Errorf("settled on %d bytes under a 2 MiB limit, asked %v", got, asked)
	}

	refuseAll := func(int) error { return syscall.ENOBUFS }
	if err := askReadBuffer(refuseAll, ReceiveBuffer); !errors.Is(err, syscall.ENOBUFS) {
		t.Errorf("a kernel that refuses every size: %v, want its refusal", err)
	}
}
