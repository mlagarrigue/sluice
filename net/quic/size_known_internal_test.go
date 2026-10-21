package quic

import (
	"io"
	"testing"
	"time"
)

// Once a stream's final size is known — its FIN has arrived — the peer has
// nothing left to send on it, and the receiver grants it no further
// MAX_STREAM_DATA (RFC 9000 §3.3: the frame leaves in "Recv" only; §13.3).
// Reading a finished body past the half-window used to queue one anyway:
// a redundant frame the peer ignored. The body is read only once the FIN is
// in, since a half-window crossed while the stream is still receiving earns
// a grant the rules allow.
func TestNoMaxStreamDataOnceTheSizeIsKnown(t *testing.T) {
	client, server := plainPair(t)
	const body = 192 << 10 // over half the 256 KiB stream window, under the window
	cs, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	initial := cs.sendQ.max
	client.mu.Unlock()
	if initial != 1<<18 {
		t.Fatalf("the peer's stream window is %d, the test assumes %d", initial, 1<<18)
	}
	if _, err := cs.WriteClose(make([]byte, body)); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ss.mu.Lock()
		fin := ss.fin
		ss.mu.Unlock()
		if fin {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the FIN never arrived")
		}
		time.Sleep(time.Millisecond)
	}
	if got, err := io.ReadAll(ss); err != nil || len(got) != body {
		t.Fatalf("read %d bytes, err %v, want %d and nil", len(got), err, body)
	}
	// A MAX_STREAM_DATA would cross the loopback within a millisecond.
	time.Sleep(50 * time.Millisecond)
	client.mu.Lock()
	limit := cs.sendQ.max
	client.mu.Unlock()
	if limit != initial {
		t.Fatalf("the peer raised the stream's limit from %d to %d after its final size was known", initial, limit)
	}
}
