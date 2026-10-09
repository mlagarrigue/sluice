package dgram_test

import (
	"errors"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
)

// The properties the tests above this one rely on. A harness that is subtly
// wrong is worse than no harness: it makes every failure look like a failure
// of the thing under test.
func TestPairDeliversWholeDatagramsInOrder(t *testing.T) {
	a, b := dgram.Pair()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for _, want := range []string{"first", "second", "third"} {
		if _, err := a.WriteTo([]byte(want), b.LocalAddr()); err != nil {
			t.Fatalf("WriteTo: %v", err)
		}
	}
	buf := make([]byte, 64)
	for _, want := range []string{"first", "second", "third"} {
		n, from, err := b.ReadFrom(buf)
		if err != nil {
			t.Fatalf("ReadFrom: %v", err)
		}
		if got := string(buf[:n]); got != want {
			t.Errorf("read %q, want %q", got, want)
		}
		if from.String() != a.LocalAddr().String() {
			t.Errorf("datagram came from %v, want %v", from, a.LocalAddr())
		}
	}
}

// Boundaries are the whole difference between a datagram transport and a byte
// stream, and a pair that concatenated two writes would let a QUIC bug through
// by reassembling packets the peer never coalesced.
func TestPairKeepsDatagramBoundaries(t *testing.T) {
	a, b := dgram.Pair()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if _, err := a.WriteTo([]byte("one"), b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.WriteTo([]byte("two"), b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _, err := b.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "one" {
		t.Errorf("one read returned %q; the two datagrams were joined", got)
	}
}

// The payload is copied, because every caller reuses its buffer the moment
// WriteTo returns — a real socket does, and a pair that borrowed instead would
// pass until the day a caller wrote into the buffer again.
func TestPairCopiesThePayload(t *testing.T) {
	a, b := dgram.Pair()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	out := []byte("original")
	if _, err := a.WriteTo(out, b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	copy(out, "OVERWRIT")

	buf := make([]byte, 64)
	n, _, err := b.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "original" {
		t.Errorf("read %q; the payload was borrowed rather than copied", got)
	}
}

func TestPairHonoursTheReadDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := dgram.Pair()
		defer func() { _ = a.Close() }()
		defer func() { _ = b.Close() }()

		if err := b.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		_, _, err := b.ReadFrom(make([]byte, 8))
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("ReadFrom returned %v, want a deadline error", err)
		}
		// It must be a net.Error reporting a timeout, which is what every caller
		// in this repository distinguishes a slow peer by.
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Errorf("the deadline error does not report itself as a timeout: %v", err)
		}
	})
}

func TestPairReportsClosure(t *testing.T) {
	a, b := dgram.Pair()
	defer func() { _ = a.Close() }()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.ReadFrom(make([]byte, 8)); !errors.Is(err, net.ErrClosed) {
		t.Errorf("reading a closed end returned %v, want net.ErrClosed", err)
	}
	if _, err := a.WriteTo([]byte("x"), b.LocalAddr()); !errors.Is(err, net.ErrClosed) {
		t.Errorf("writing to a closed end returned %v, want net.ErrClosed", err)
	}
}

// The package doc's promise is "close either one and both report
// net.ErrClosed to whoever is blocked on them" — including a reader parked
// with no read deadline set, woken by the *other* end closing rather than
// its own.
func TestPairWakesABlockedReaderWhenThePeerCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := dgram.Pair()
		defer func() { _ = a.Close() }()

		done := make(chan error, 1)
		go func() {
			_, _, err := a.ReadFrom(make([]byte, 8))
			done <- err
		}()

		// Once every goroutine in the bubble is blocked, a.ReadFrom is parked
		// in its select with no timeout case to save it — the shape the bug
		// needed.
		synctest.Wait()
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}

		select {
		case err := <-done:
			if !errors.Is(err, net.ErrClosed) {
				t.Errorf("ReadFrom returned %v, want net.ErrClosed", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ReadFrom never woke up when the peer closed")
		}
	})
}

// Writing anywhere but the other end is a mistake in the test rather than a
// packet to drop in silence — which is the failure mode this package exists to
// remove, not to reproduce at a different layer.
func TestPairRefusesAnUnknownAddress(t *testing.T) {
	a, b := dgram.Pair()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if _, err := a.WriteTo([]byte("x"), a.LocalAddr()); err == nil {
		t.Error("writing to itself was accepted")
	}
	if _, err := a.WriteTo([]byte("x"), nil); err == nil {
		t.Error("writing to a nil address was accepted")
	}
}

// A deadline already past fails the call before any data is looked at, as a
// real socket's does. With an expired timer and a queued datagram both ready,
// the select used to pick one at random, so a read past its deadline handed
// back a datagram about half the time.
func TestPairExpiredDeadlineWinsOverQueuedData(t *testing.T) {
	a, b := dgram.Pair()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	const queued = 64
	for range queued {
		if _, err := a.WriteTo([]byte("x"), b.LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-time.Second)
	if err := b.SetReadDeadline(past); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	for i := range queued {
		if _, _, err := b.ReadFrom(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read %d past the deadline returned %v, want os.ErrDeadlineExceeded", i, err)
		}
	}

	if err := a.SetWriteDeadline(past); err != nil {
		t.Fatal(err)
	}
	if _, err := a.WriteTo([]byte("x"), b.LocalAddr()); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a write past the deadline returned %v, want os.ErrDeadlineExceeded", err)
	}
}
