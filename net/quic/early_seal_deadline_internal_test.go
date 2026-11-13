package quic

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// A DialEarly client whose 0-RTT the server refused waits for the 1-RTT keys
// before its next packet can leave (awaitSealLocked). A stream opened in
// that gap finds credit at once — the refusal starts the account over from
// the server's own parameters — so its write builds its frame and parks on
// the keys, not on credit. That wait ignored the write deadline: a server
// that withholds its Finished kept the Write until the handshake timeout,
// whatever SetWriteDeadline said.
func TestWriteDeadlineBoundsTheWaitForOneRTTKeys(t *testing.T) {
	c := newConn(&fuzzSink{addr: "seal:self"}, fakeFuzzAddr("seal:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	c.mu.Lock()
	c.peerParams.InitialMaxData = 1 << 20
	c.peerParams.InitialMaxStreamDataBidiRemote = 1 << 20
	c.peerParams.InitialMaxStreamsBidi = 10
	c.hasPeerParams = true
	c.rejectEarlyLocked() // crypto/tls reports the refusal before the 1-RTT keys
	c.mu.Unlock()

	s, err := c.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	done := make(chan error, 1)
	go func() {
		_, err := s.Write([]byte("after the refusal"))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write returned %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		_ = c.Close() // release the writer
		<-done
		t.Fatal("Write still waiting for the 1-RTT keys 5s after a 50ms write deadline")
	}
}
