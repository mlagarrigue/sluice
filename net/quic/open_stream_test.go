package quic_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// oneStreamParams lets the client open a single bidirectional stream at a
// time: the second OpenStream has to wait for the first to retire.
func oneStreamParams() quic.TransportParameters {
	p := quic.DefaultParameters()
	p.InitialMaxStreamsBidi = 1
	return p
}

// With the peer's grant spent, OpenStream parks until the peer retires a
// stream and raises MAX_STREAMS, then hands out the next stream.
func TestOpenStreamWaitsForMaxStreams(t *testing.T) {
	client, server, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneStreamParams())

	first, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := first.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	type opened struct {
		s   *quic.Stream
		err error
	}
	second := make(chan opened, 1)
	go func() {
		s, err := client.OpenStream(t.Context())
		second <- opened{s, err}
	}()
	select {
	case got := <-second:
		t.Fatalf("second OpenStream came back at once with %v, %v; the grant is spent", got.s, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	// The server finishes the first stream in both directions, which
	// retires it and grants the slot back.
	ss, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	if err := ss.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-second:
		if got.err != nil {
			t.Fatalf("second OpenStream after the grant: %v", got.err)
		}
		if got.s.ID() != first.ID()+4 {
			t.Fatalf("second stream is %d, want %d", got.s.ID(), first.ID()+4)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second OpenStream still parked 2 s after the peer granted a stream")
	}
}

// The wait is the caller's to bound: with the grant spent and a context
// that expires, OpenStream reports the context's error and leaves the
// connection as it was.
func TestOpenStreamGivesUpWithTheContext(t *testing.T) {
	client, _, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneStreamParams())

	if _, err := client.OpenStream(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	s, err := client.OpenStream(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStream with the grant spent: stream %v, err %v; want context.DeadlineExceeded", s, err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("OpenStream took %v to notice the deadline", elapsed)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("the connection ended with %v; the context should only end the wait", err)
	}
}

// oneUniStreamParams lets the client open a single unidirectional stream at
// a time: the second OpenUniStream has to wait for the first to retire.
func oneUniStreamParams() quic.TransportParameters {
	p := quic.DefaultParameters()
	p.InitialMaxStreamsUni = 1
	return p
}

// With the peer's unidirectional grant spent, OpenUniStream parks until the
// peer raises MAX_STREAMS; the next stream then comes out with the next
// identifier of its kind. The peer raises it only once the first stream is
// over at its end — FIN received, stream accepted and read to the end, RFC
// 9000 §3.2's "Data Read": a FIN alone, with the stream still waiting to be
// accepted, grants nothing back, or a peer could open streams faster than
// the application takes them with nothing to bound the pile.
func TestOpenUniStreamWaitsForMaxStreams(t *testing.T) {
	client, server, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneUniStreamParams())

	first, err := client.OpenUniStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if id := first.ID(); id&0x03 != 0x02 {
		t.Fatalf("first unidirectional stream is %d; want a client-initiated unidirectional identifier (2, 6, …)", id)
	}
	if _, err := first.WriteClose([]byte("one")); err != nil {
		t.Fatal(err)
	}

	type opened struct {
		s   *quic.Stream
		err error
	}
	second := make(chan opened, 1)
	go func() {
		s, err := client.OpenUniStream(t.Context())
		second <- opened{s, err}
	}()
	select {
	case got := <-second:
		t.Fatalf("second OpenUniStream came back with %v, %v before the server read the first", got.s, got.err)
	case <-time.After(200 * time.Millisecond):
	}

	// Accepting and reading to the end retires the stream on the server and
	// grants the slot back.
	ss, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if ss.ID() != first.ID() {
		t.Fatalf("the server accepted stream %d, want %d", ss.ID(), first.ID())
	}
	if got, err := io.ReadAll(ss); err != nil || string(got) != "one" {
		t.Fatalf("the server read %q, %v; want \"one\", nil", got, err)
	}
	select {
	case got := <-second:
		if got.err != nil {
			t.Fatalf("second OpenUniStream after the grant: %v", got.err)
		}
		if got.s.ID() != first.ID()+4 {
			t.Fatalf("second stream is %d, want %d", got.s.ID(), first.ID()+4)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second OpenUniStream still parked 2 s after the server read the first stream")
	}
}

// The wait is the caller's to bound, as for OpenStream: with the grant
// spent and a context that expires, OpenUniStream reports the context's
// error and leaves the connection as it was.
func TestOpenUniStreamGivesUpWithTheContext(t *testing.T) {
	client, _, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneUniStreamParams())

	if _, err := client.OpenUniStream(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	s, err := client.OpenUniStream(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenUniStream with the grant spent: stream %v, err %v; want context.DeadlineExceeded", s, err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("OpenUniStream took %v to notice the deadline", elapsed)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("the connection ended with %v; the context should only end the wait", err)
	}
}

// A unidirectional stream this end opened has no receive side: Read says so
// at once instead of waiting for bytes the peer can never send.
func TestOpenUniStreamRefusesReads(t *testing.T) {
	client, _, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), oneUniStreamParams())

	s, err := client.OpenUniStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, quic.ErrQUIC) {
			t.Fatalf("Read on a send-only stream returned %v; want an error wrapping ErrQUIC", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read on a send-only stream is still parked after 1 s")
	}
}
