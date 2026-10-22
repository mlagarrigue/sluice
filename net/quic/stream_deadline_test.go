package quic_test

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// openAccepted opens a stream on the client, sends one byte so the server
// learns of it, and returns both ends once the server has accepted it.
func openAccepted(t *testing.T, client, server *quic.Conn) (cs, ss *quic.Stream) {
	t.Helper()
	cs, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	ss, err = server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := ss.Read(one[:]); err != nil {
		t.Fatal(err)
	}
	return cs, ss
}

// A Read with nothing to deliver gives up at the deadline, and says so the
// way net.Conn does: errors.Is(err, os.ErrDeadlineExceeded).
func TestReadDeadlinePassesWithNothingToRead(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())
	_, ss := openAccepted(t, client, server)

	// The clock is read before the deadline is set, so the measured wait
	// covers the whole deadline: read after, a coarse Windows clock made
	// the wait look 0.4 ms short.
	start := time.Now()
	if err := ss.SetReadDeadline(start.Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := ss.Read(make([]byte, 16))
	if n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read = %d, %v; want 0, os.ErrDeadlineExceeded", n, err)
	}
	if took := time.Since(start); took < 50*time.Millisecond || took > 5*time.Second {
		t.Fatalf("Read returned after %v, want the 50ms deadline, not sooner", took)
	}
	// The deadline stays passed until it is moved: the next Read fails too.
	if _, err := ss.Read(make([]byte, 16)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second Read = %v, want os.ErrDeadlineExceeded still", err)
	}
}

// A deadline set while a Read is already parked wakes it: the caller that
// changes its mind does not wait for data that may never come.
func TestReadDeadlineSetDuringReadWakesIt(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())
	_, ss := openAccepted(t, client, server)

	done := make(chan error, 1)
	go func() {
		_, err := ss.Read(make([]byte, 16))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // long enough for the goroutine to park
	select {
	case err := <-done:
		t.Fatalf("Read returned %v before any deadline was set", err)
	default:
	}
	if err := ss.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read = %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the parked Read never woke at the deadline set under it")
	}
}

// The zero time removes the deadline: a Read then waits for data and gets it.
func TestZeroReadDeadlineRemovesIt(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())
	cs, ss := openAccepted(t, client, server)

	if err := ss.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Read(make([]byte, 16)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read with a passed deadline = %v, want os.ErrDeadlineExceeded", err)
	}
	if err := ss.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		n, err := ss.Read(buf)
		if err == nil && (n != 5 || string(buf[:n]) != "hello") {
			err = errors.New("Read did not deliver the five bytes written")
		}
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Read returned %v with no data and no deadline", err)
	default:
	}
	if _, err := cs.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read never saw the bytes written after the deadline was removed")
	}
}

// A Write parked on a peer that never grants credit gives up at the write
// deadline, after reporting the bytes that did go out. SetDeadline sets both.
func TestWriteDeadlinePassesWithoutCredit(t *testing.T) {
	serverParams := quic.DefaultParameters()
	serverParams.InitialMaxStreamDataBidiRemote = 64
	client, server, _, _ := handshakePairParams(t, serverTLS(t), clientTLS(), serverParams)
	server.SetManualCredit(true) // the server reads but never grants more
	cs, _ := openAccepted(t, client, server)

	// The clock starts before the deadline is set, not after: measured
	// from a later instant, a deadline that matured on time looks early.
	start := time.Now()
	if err := cs.SetDeadline(start.Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := cs.Write(make([]byte, 1024))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write = %d, %v; want os.ErrDeadlineExceeded", n, err)
	}
	if n != 63 { // 64 of initial credit, one byte spent by openAccepted
		t.Fatalf("Write reported %d bytes, want the 63 the credit allowed", n)
	}
	if took := time.Since(start); took < 100*time.Millisecond || took > 5*time.Second {
		t.Fatalf("Write returned after %v, want the 100ms deadline, not sooner", took)
	}
	// SetDeadline covered the read side too.
	if _, err := cs.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read after SetDeadline = %v, want os.ErrDeadlineExceeded", err)
	}
}
