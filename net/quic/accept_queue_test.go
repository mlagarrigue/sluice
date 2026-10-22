package quic_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// openUntilRefused opens streams of one kind, each with a one-byte body and
// its FIN, until OpenStream or OpenUniStream parks past a short wait, and
// reports how many came out. It stops at most streams.
func openUntilRefused(t *testing.T, c *quic.Conn, uni bool, most int) int {
	t.Helper()
	for n := range most {
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		var s *quic.Stream
		var err error
		if uni {
			s, err = c.OpenUniStream(ctx)
		} else {
			s, err = c.OpenStream(ctx)
		}
		cancel()
		if err != nil {
			return n
		}
		if _, err := s.WriteFinal([]byte{'x'}); err != nil {
			t.Fatalf("stream %d: %v", s.ID(), err)
		}
	}
	return most
}

// writeLong writes more than the congestion window lets out at once on s, so
// the Write finishes only if the peer keeps acknowledging — that is, only if
// its read loop still runs.
func writeLong(t *testing.T, s *quic.Stream) {
	t.Helper()
	if err := s.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(bytes.Repeat([]byte{'y'}, 60<<10)); err != nil {
		t.Fatalf("a Write on stream %d while the peer's accept queue fills: %v", s.ID(), err)
	}
}

// A peer opening unidirectional streams with a FIN, faster than the server
// accepts them, is held by MAX_STREAMS: no slot comes back before the stream
// is read (RFC 9000 §3.2, "Data Read"). It used to come back on the FIN: the
// client opened streams without bound, the 65th filled the server's
// 64-stream accept channel, and the server's read loop stopped — no more
// acknowledgements, so a Write on any other stream stalled with it.
func TestUnacceptedUniStreamsHoldTheirSlots(t *testing.T) {
	client, _, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	limit := int(quic.DefaultParameters().InitialMaxStreamsUni)
	if got := openUntilRefused(t, client, true, 200); got != limit {
		t.Fatalf("the client opened %d unidirectional streams nobody accepted; the server's limit is %d", got, limit)
	}
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	writeLong(t, s)
}

// The default bidirectional limit is 100, over the 64 the accept channel
// held: a client using all of it stopped the server's read loop on the
// 65th stream.
func TestEveryBidiStreamTheLimitAllowsCanWaitForAccept(t *testing.T) {
	client, _, _, _ := handshakePair(t, serverTLS(t), clientTLS())

	limit := int(quic.DefaultParameters().InitialMaxStreamsBidi)
	if got := openUntilRefused(t, client, false, limit-1); got != limit-1 {
		t.Fatalf("the client opened %d bidirectional streams, want %d", got, limit-1)
	}
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	writeLong(t, s)
}
