package quic

import (
	"io"
	"testing"
	"time"
)

// A sending direction whose FIN and every byte before it are acknowledged is
// in RFC 9000 §3.1's "Data Recvd", a terminal state from which nothing may
// be sent (§3.3): a STOP_SENDING that arrives then is answered with no
// RESET_STREAM (§3.5 asks for one in "Ready" or "Send" only), and an
// application Reset sends none either. Until 2026-10-21 both sent one, the
// FIN's departure having been taken for the end of the direction.
func TestNoResetStreamOnceTheStreamIsAcknowledged(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(t *testing.T, client, server *Stream)
	}{
		{"stop_sending", func(t *testing.T, client, server *Stream) {
			t.Helper()
			// Forged, not asked of the server's stream: a sluice peer that
			// has read the FIN sends no STOP_SENDING (nothing is left to
			// stop), so the late frame is one a peer sent before it read
			// the FIN, or retransmitted after.
			peerSends(t, server.conn, appendFrame(nil, FrameStopSending, client.id, 0x10c))
		}},
		{"reset", func(t *testing.T, client, _ *Stream) {
			t.Helper()
			if err := client.Reset(0x10c); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := plainPair(t)
			cs, err := client.OpenStream(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cs.WriteFinal(make([]byte, 100)); err != nil {
				t.Fatal(err)
			}
			ss, err := server.AcceptStream(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(ss); err != nil {
				t.Fatal(err)
			}
			// The whole write, FIN included, acknowledged: nothing of the
			// client's stream is in flight any more. The server keeps its
			// own direction open, so the stream stays on both sides.
			deadline := time.Now().Add(5 * time.Second)
			for client.Stats().BytesInFlight > 0 {
				if time.Now().After(deadline) {
					t.Fatal("the write was never acknowledged")
				}
				time.Sleep(time.Millisecond)
			}

			tc.stop(t, cs, ss)

			// A RESET_STREAM would cross the loopback within a millisecond;
			// none does in a hundred.
			until := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(until) {
				if code, reset := ss.WasReset(); reset {
					t.Fatalf("the server saw RESET_STREAM(%#x) for a stream it had fully received and acknowledged", code)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if _, reset := cs.WasReset(); reset {
				t.Fatal("the client's direction reads as reset after it was finished and acknowledged")
			}
		})
	}
}
