package httpstream

import (
	"encoding/binary"
	"testing"
)

// RFC 9113 §10.5 lists the frames a peer can send without opening a stream
// — PING, SETTINGS, PRIORITY, WINDOW_UPDATE, empty DATA — and asks an
// implementation to track their use. This server keeps no counter per kind,
// and this test is the measurement that position rests on: none of them
// makes the server answer with more bytes than it received (PING and
// SETTINGS get an acknowledgement of their own size at most, the others get
// nothing), and none costs it a heap allocation per frame. A flood of them
// is therefore paid in the peer's own bandwidth, linearly, and bounded by
// the control queue's backpressure (Config.MaxPendingControlBytes) and by
// the idle timeout, which they do not restart
// (TestH2PingAloneDoesNotKeepAnIdleConnection). The vectors that do amplify
// — rapid reset, CONTINUATION floods, header blocks — have their own bounds.
func TestH2ControlFramesAnswerNoMoreThanTheyCost(t *testing.T) {
	const n = 400 // 400 PINGs are 6.8 KiB of acknowledgements: under the queue bound, so nothing waits
	frames := func(f h2Frame) (parsed []h2Frame, in int) {
		raw, err := appendH2Frame(nil, f)
		if err != nil {
			t.Fatal(err)
		}
		p, _, err := parseFrame(raw, defaultMaxFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		for range n {
			parsed = append(parsed, p)
		}
		return parsed, n * len(raw)
	}
	var one [4]byte
	binary.BigEndian.PutUint32(one[:], 1)
	kinds := []struct {
		name  string
		frame h2Frame
		// answered is the largest answer each frame may earn: its own size
		// for the two that are acknowledged, nothing for the others.
		answered int
	}{
		{"PING", h2Frame{Type: framePing, Payload: []byte("flooding")}, 17},
		{"an empty SETTINGS", h2Frame{Type: frameSettings}, 9},
		{"PRIORITY", h2Frame{Type: framePriority, StreamID: 3, Payload: []byte{0, 0, 0, 0, 16}}, 0},
		{"a WINDOW_UPDATE of one byte", h2Frame{Type: frameWindowUpdate, Payload: one[:]}, 0},
		{"an empty DATA on an open stream", h2Frame{Type: frameData, StreamID: 1}, 0},
	}
	for _, k := range kinds {
		t.Run(k.name, func(t *testing.T) {
			s := newBenchH2Server(Config{})
			// A request in progress, so that the connection is not idle and
			// the empty DATA has a stream to land on.
			block := appendHPACK(nil, ":method", "POST")
			block = appendHPACK(block, ":path", "/upload")
			block = appendHPACK(block, ":scheme", "https")
			block = appendHPACK(block, ":authority", "example.com")
			head, _ := frames(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: block})
			if _, _, err := s.consume(head[:1]); err != nil {
				t.Fatal(err)
			}
			s.w.ctrl = s.w.ctrl[:0]

			flood, in := frames(k.frame)
			if _, _, err := s.consume(flood); err != nil {
				t.Fatalf("%d frames were refused: %v", n, err)
			}
			out := len(s.w.ctrl)
			if out > n*k.answered {
				t.Errorf("%d frames (%d bytes) were answered with %d bytes, want at most %d", n, in, out, n*k.answered)
			}
			if out > in {
				t.Errorf("amplification: %d bytes in, %d bytes out", in, out)
			}
			t.Logf("%d bytes in, %d bytes out", in, out)

			allocs := testing.AllocsPerRun(5, func() {
				s.w.ctrl = s.w.ctrl[:0]
				if _, _, err := s.consume(flood); err != nil {
					t.Fatal(err)
				}
			})
			if allocs > 0 {
				t.Errorf("%v allocations per %d frames, want none: the flood is paid on this side's heap", allocs, n)
			}
		})
	}
}
