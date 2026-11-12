package httpstream

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// The read-timeout sweep is armed at the assembler's earliest deadline
// (h3Serving.arm) and re-armed after each run at whatever nextExpiry then
// says. expire let a sweep in from the deadline on (it returns only while
// now is before nextExpiry) but failed a stream only once now was after its
// deadline: a sweep whose clock read the deadline exactly failed nothing and
// left nextExpiry where it was — at now. The timer was then re-armed with a
// wait of zero, fired at once, and so on for as long as the clock read that
// instant: for ever on the clock of a testing/synctest bubble, which does
// not move while a goroutine runs. A stream at its deadline fails now.
func TestH3ASweepAtTheDeadlineFailsTheStream(t *testing.T) {
	cfg, err := Config{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	a := newH3Assembler(cfg)
	request := h3Request("GET", "/")
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(0, request[:len(request)-2], false)}); err != nil {
		t.Fatal(err)
	}
	deadline := a.nextExpiry
	if deadline.IsZero() {
		t.Fatal("a stream is assembling and no deadline is armed: the test proves nothing")
	}
	failed := a.sweep(deadline) // the timer fired, and the clock reads the deadline
	if len(failed) == 0 && !a.nextExpiry.IsZero() && !a.nextExpiry.After(deadline) {
		t.Fatalf("a sweep at the deadline failed no stream and left the next expiry at the same instant: the timer is re-armed with a wait of zero, and fires again with the clock unmoved")
	}
}

// The same end to end: a client sends a HEADERS frame cut short, then only
// PINGs, and ServeH3Conn — in a synctest bubble, over an in-memory QUIC
// pair — resets the stream with H3_REQUEST_INCOMPLETE once ReadTimeout has
// passed. Before the sweep failed a stream at its deadline, the bubble's
// clock stopped at that instant: the sweep's timer re-armed at zero for
// ever, and the test ran into go test's -timeout instead of failing here.
// IdleTimeout and QUIC's idle timeout are far past the observation, so that
// only the read timeout can end the stream.
func TestH3TruncatedHeadersThenPingFailsAtTheDeadline(t *testing.T) {
	const readTimeout = 300 * time.Millisecond
	synctest.Test(t, func(t *testing.T) {
		serverPC, clientPC := dgram.Pair()
		defer func() { _ = serverPC.Close() }()
		defer func() { _ = clientPC.Close() }()
		// As in h3idle_conformance_test.go: the closing period runs out
		// before the bubble's clock stops.
		defer time.Sleep(10 * time.Second)
		params := quic.DefaultConfig()
		params.MaxIdleTimeout = time.Hour
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			conn, err := acceptOne(t, serverPC, h3ServerTLS(t), params)
			if err != nil {
				return
			}
			_ = ServeH3Conn(ctx, conn, Config{ReadTimeout: readTimeout, IdleTimeout: time.Hour}, echoTarget)
		}()
		conn, err := quic.Dial(t.Context(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), params)
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		s, err := conn.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		request := h3Request("GET", "/")
		start := time.Now()
		if _, err := s.Write(request[:len(request)-2]); err != nil {
			t.Fatal(err)
		}
		read := make(chan error, 1)
		go func() {
			_, err := io.ReadAll(s)
			read <- err
		}()
		const watch = 20 * readTimeout
		for time.Since(start) < watch {
			pctx, stop := context.WithTimeout(t.Context(), readTimeout/3)
			_ = conn.Ping(pctx)
			<-pctx.Done()
			stop()
			select {
			case err := <-read:
				elapsed := time.Since(start)
				if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%#x", h3RequestIncomplete)) {
					t.Fatalf("the truncated request's stream ended with %v, want a reset with H3_REQUEST_INCOMPLETE (%#x)", err, h3RequestIncomplete)
				}
				if elapsed < readTimeout || elapsed > 2*readTimeout {
					t.Fatalf("the stream was reset %v after its first bytes, want between ReadTimeout (%v) and twice it", elapsed, readTimeout)
				}
				return
			default:
			}
		}
		t.Fatalf("the truncated request's stream still stands %v after its first bytes, with ReadTimeout %v", time.Since(start), readTimeout)
	})
}
