package httpstream

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// FuzzH2Connection drives [ServeH2] itself, not just [parseFrame]: bytes go
// through the frame layer, HPACK, padding and priority stripping
// (h2conn.go's headers, around the block that removes PADDED and PRIORITY),
// stream lifecycle and window accounting — the whole assembly a real
// connection exercises, which FuzzParseFrame and FuzzHPACKDecode stop short
// of on their own.
//
// The property is the same one every fuzzer in this package checks: for any
// bytes at all, this end must refuse or make progress, never panic and never
// hang. A refusal must surface as one of the errors this package names —
// [ErrH2Protocol], [ErrHPACK] or [ErrTooLarge] — or as the ordinary shutdown
// of a pipe that was closed out from under it; anything else is a bug this
// fuzzer exists to find.
func FuzzH2Connection(f *testing.F) {
	f.Add(append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, nil)...))
	f.Add(fuzzSeedRequest())
	f.Add(fuzzSeedSplitHeaders())
	f.Add(fuzzSeedPaddedPriority())
	f.Add(fuzzSeedPaddedPriorityEdge())
	f.Add(fuzzSeedSettingsBurst())
	f.Add([]byte(Preface))
	f.Add([]byte{})
	f.Add([]byte("GET / HTTP/1.1\r\n\r\n")) // not the preface at all

	f.Fuzz(func(t *testing.T, data []byte) {
		// A fuzz-generated input has no reason to grow past what one frame
		// buffer already bounds in production; skipping the rest keeps one
		// case from spending the whole fuzzing budget on a single huge
		// input rather than exploring more of the state space.
		const fuzzMaxInput = 1 << 16
		if len(data) > fuzzMaxInput {
			t.Skip("input larger than this fuzzer bounds itself to")
		}

		client, server := net.Pipe()
		// A t.Cleanup, not a defer: the watchdog below ends the subtest with
		// t.Fatal, which unwinds through runtime.Goexit and would skip a
		// plain defer-free "close after waiting" placed after the select.
		// Cleanup still runs, and closing both ends unblocks whichever
		// goroutine (ServeH2 or the drain loop) was parked on an I/O read
		// that will never see the client's pipe write a byte — rather than
		// leaking it past this subtest, to accumulate across a long -fuzz
		// run that keeps hitting the watchdog.
		t.Cleanup(func() {
			_ = client.Close()
			_ = server.Close()
		})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Timeouts far under the per-case watchdog below, so a connection
		// that is legitimately waiting on flow-control credit or a peer
		// that never arrives unwinds well within the deadline instead of
		// racing it.
		cfg := H2Config{
			ReadTimeout:  200 * time.Millisecond,
			WriteTimeout: 200 * time.Millisecond,
			IdleTimeout:  200 * time.Millisecond,
		}

		done := make(chan error, 1)
		go func() { done <- ServeH2(ctx, server, cfg, fuzzH2Handler) }()

		// A net.Pipe is synchronous and unbuffered: ServeH2's writer would
		// block forever on a response nobody reads. Draining in the
		// background is what lets the fuzzed bytes actually flow instead of
		// deadlocking on the first frame that provokes an answer.
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			buf := make([]byte, 4096)
			for {
				if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
					return
				}
				if _, err := client.Read(buf); err != nil {
					return
				}
			}
		}()

		if err := client.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		_, _ = client.Write(data) // a refused or partial write is a fine outcome; a hang is not
		_ = client.Close()

		select {
		case err := <-done:
			if !fuzzH2AcceptableErr(err) {
				t.Fatalf("ServeH2 returned an unexpected error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ServeH2 did not return: a livelock somewhere in frame or HPACK assembly")
		}
		<-drained
	})
}

// fuzzH2Handler is a small, fast handler with nothing that could itself stall
// a fuzz case: the property under test is the connection's own bookkeeping,
// not a handler's behaviour.
func fuzzH2Handler(b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for _, r := range b.Items {
		out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
	}
	return sluice.Batch[Response]{Items: out}
}

// fuzzH2AcceptableErr is what [ServeH2] may return for arbitrary input
// without that being a bug: one of this package's own named refusals, or the
// ordinary shutdown of a pipe this test closed from its own end.
func fuzzH2AcceptableErr(err error) bool {
	if err == nil {
		return true
	}
	switch {
	case errors.Is(err, ErrH2Protocol),
		errors.Is(err, ErrHPACK),
		errors.Is(err, ErrTooLarge),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, io.ErrClosedPipe),
		errors.Is(err, net.ErrClosed),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled),
		// The harness's own short Read/WriteTimeout firing: a fuzzed peer
		// that stops mid-frame, or a drain that falls behind a response,
		// is cut off by the clock, which is the bound working.
		errors.Is(err, os.ErrDeadlineExceeded):
		return true
	}
	return false
}

// A peer that stops mid-frame is cut off by ReadTimeout, and ServeH2 says so
// with the socket's deadline error. That is the bound working, and the
// fuzzer used to report it as a bug.
func TestFuzzH2AcceptsAReadTimeout(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	done := make(chan error, 1)
	go func() {
		done <- ServeH2(t.Context(), server, H2Config{ReadTimeout: 50 * time.Millisecond}, fuzzH2Handler)
	}()
	go func() { _, _ = io.Copy(io.Discard, client) }()

	in := append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, nil)...)
	in = append(in, 0, 0, 8) // the first three bytes of a frame header
	if _, err := client.Write(in); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("ServeH2 returned %v, want the read deadline", err)
		}
		if !fuzzH2AcceptableErr(err) {
			t.Errorf("fuzzH2AcceptableErr refuses %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH2 did not time out")
	}
}

// fuzzFrame frames one frame for a seed; a seed that fails to frame is a bug
// in the seed, not something to fuzz around.
func fuzzFrame(typ, flags byte, id uint32, payload []byte) []byte {
	b, err := appendH2Frame(nil, h2Frame{Type: typ, Flags: flags, StreamID: id, Payload: payload})
	if err != nil {
		panic(err)
	}
	return b
}

// fuzzLiteral encodes one field as HPACK literal-without-indexing, name
// included — the same representation h2conn_test.go's client uses, since any
// conformant decoder must accept it.
func fuzzLiteral(dst []byte, name, value string) []byte {
	dst = append(dst, 0x00, byte(len(name)))
	dst = append(dst, name...)
	dst = append(dst, byte(len(value)))
	return append(dst, value...)
}

func fuzzHeadBlock(method, path string) []byte {
	var block []byte
	block = fuzzLiteral(block, ":method", method)
	block = fuzzLiteral(block, ":path", path)
	block = fuzzLiteral(block, ":scheme", "http")
	return block
}

// fuzzSeedRequest is the smallest whole request: preface, an empty SETTINGS,
// and one HEADERS frame that both opens and ends the stream.
func fuzzSeedRequest() []byte {
	data := append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, nil)...)
	block := fuzzHeadBlock("GET", "/a")
	return append(data, fuzzFrame(frameHeaders, flagEndHeaders|flagEndStream, 1, block)...)
}

// fuzzSeedSplitHeaders splits a header block across HEADERS and CONTINUATION
// mid-field, which RFC 7541 permits and which a decoder that only handles
// per-frame boundaries refuses.
func fuzzSeedSplitHeaders() []byte {
	data := append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, nil)...)
	block := fuzzHeadBlock("GET", "/split")
	head, rest := block[:5], block[5:]
	data = append(data, fuzzFrame(frameHeaders, flagEndStream, 1, head)...)
	return append(data, fuzzFrame(frameContinuation, flagEndHeaders, 1, rest)...)
}

// fuzzSeedPaddedPriority carries both PADDED and PRIORITY, which is the
// stripping logic h2conn.go's headers exercises before HPACK ever sees the
// block.
func fuzzSeedPaddedPriority() []byte {
	data := append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, nil)...)
	block := fuzzHeadBlock("GET", "/pp")
	payload := append([]byte{4}, make([]byte, 5)...) // pad length 4, 5 priority bytes
	payload = append(payload, block...)
	payload = append(payload, make([]byte, 4)...) // the padding itself
	return append(data, fuzzFrame(frameHeaders, flagEndHeaders|flagEndStream|flagPadded|flagPriority, 1, payload)...)
}

// fuzzSeedPaddedPriorityEdge is the edge case in the same stripping logic:
// pad length equal to what is left of the block once the 5 priority bytes
// are removed, so nothing remains for HPACK at all. This is legal framing —
// RFC 9113 does not forbid an empty header block — and worth seeding so the
// fuzzer starts near the boundary h2conn.go:241-257 has to get right rather
// than only reaching it by chance.
func fuzzSeedPaddedPriorityEdge() []byte {
	data := append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, nil)...)
	// 1 pad-length byte + 5 priority bytes + 3 bytes of padding, pad == 3.
	payload := append([]byte{3}, make([]byte, 5)...)
	payload = append(payload, make([]byte, 3)...)
	return append(data, fuzzFrame(frameHeaders, flagEndHeaders|flagEndStream|flagPadded|flagPriority, 1, payload)...)
}

// fuzzSeedSettingsBurst is two SETTINGS_INITIAL_WINDOW_SIZE changes around an
// open stream, which is the one setting RFC 9113 §6.9.2 applies as a delta
// against the previous value rather than the protocol's default.
func fuzzSeedSettingsBurst() []byte {
	data := append([]byte(Preface), fuzzFrame(frameSettings, 0, 0, settingsPayload([2]uint32{settingInitialWindowSize, 1000}))...)
	block := fuzzHeadBlock("POST", "/burst")
	data = append(data, fuzzFrame(frameHeaders, flagEndHeaders, 1, block)...)
	data = append(data, fuzzFrame(frameSettings, 0, 0, settingsPayload([2]uint32{settingInitialWindowSize, 2000}))...)
	return append(data, fuzzFrame(frameData, flagEndStream, 1, []byte("x"))...)
}

// settingsPayload encodes SETTINGS entries (RFC 9113 §6.5.1).
func settingsPayload(pairs ...[2]uint32) []byte {
	var p []byte
	for _, kv := range pairs {
		p = append(p, byte(kv[0]>>8), byte(kv[0]))
		p = append(p, byte(kv[1]>>24), byte(kv[1]>>16), byte(kv[1]>>8), byte(kv[1]))
	}
	return p
}
