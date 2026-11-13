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

// From ReceiveBuffer the halvings skip 64 KiB — 112 KiB, then 56 — so a
// kernel that grants minReceiveBuffer and refuses anything larger was never
// asked for it, and Listen failed against the godoc's "down to 64 KiB". The
// floor itself is the last request.
func TestAKernelGrantingTheFloorIsAccepted(t *testing.T) {
	var asked []int
	set := func(n int) error {
		asked = append(asked, n)
		if n > minReceiveBuffer {
			return syscall.ENOBUFS
		}
		return nil
	}
	if err := askReadBuffer(set, ReceiveBuffer); err != nil {
		t.Fatalf("askReadBuffer = %v after asking %v; want minReceiveBuffer (%d) asked and granted", err, asked, minReceiveBuffer)
	}
	if last := asked[len(asked)-1]; last != minReceiveBuffer {
		t.Errorf("the last request was %d, want minReceiveBuffer (%d)", last, minReceiveBuffer)
	}
}
