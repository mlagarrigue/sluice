package httpstream

import (
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ReadTimeout bounds how long a request may take to arrive, whatever else the
// client keeps sending. The sweep used to run only when stream frames were
// delivered, so a client that truncated its HEADERS and then sent nothing but
// PINGs — which keep the transport's idle timer from firing — held its
// partial request, and the memory and admission slot behind it, for as long
// as it kept pinging. A timer now runs the sweep at the earliest deadline.
func TestH3ReadTimeoutRunsWithoutStreamFrames(t *testing.T) {
	const readTimeout = 200 * time.Millisecond
	conn, _, _ := startH3With(t, Config{ReadTimeout: readTimeout}, quic.DefaultParameters(), quic.DefaultParameters(), echoTarget)
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request := h3Request("GET", "/")
	if _, err := s.Write(request[:len(request)-2]); err != nil { // HEADERS, two bytes short
		t.Fatal(err)
	}
	start := time.Now()

	ctx, stop := context.WithTimeout(t.Context(), 5*readTimeout)
	defer stop()
	go func() {
		for ctx.Err() == nil {
			_ = conn.Ping(ctx)
			select {
			case <-ctx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	for ctx.Err() == nil {
		if code, reset := s.WasReset(); reset {
			if code != h3RequestIncomplete {
				t.Fatalf("stream reset with %#x, want H3_REQUEST_INCOMPLETE", code)
			}
			if took := time.Since(start); took > 2*readTimeout {
				t.Errorf("reset after %v, want within 2×ReadTimeout (%v)", took, 2*readTimeout)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no reset after %v of PINGs: the partial request outlived ReadTimeout (%v)", time.Since(start), readTimeout)
}
