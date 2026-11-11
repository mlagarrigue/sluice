package quic

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// refusingStreamConn is a client connection whose socket refuses every send
// (refusingPC, ENETUNREACH), with 1-RTT keys installed and stream 0 open.
func refusingStreamConn(t *testing.T) (*Conn, *Stream) {
	t.Helper()
	c := newConn(&refusingPC{fuzzSink{addr: "refused:self"}}, fakeFuzzAddr("refused:peer"),
		[]byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	installAppSealer(t, c)
	c.mu.Lock()
	c.peerMaxStreamsBidi = 1
	c.mu.Unlock()
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

// A packet sealed and recorded whose datagram the socket refuses — a lost
// route, a firewall — is lost as far as recovery goes, and its frames go
// again (errSentUnwritten); Stats.WriteFailures counts it, and "no write
// error ends the connection or reaches a caller". Reset changed the
// stream's state, recorded its RESET_STREAM, and still returned the
// refusal.
func TestAResetWhoseDatagramIsRefusedSucceeds(t *testing.T) {
	c, s := refusingStreamConn(t)
	before := c.Stats().WriteFailures
	err := s.Reset(0x10c)
	if c.Stats().WriteFailures == before {
		t.Fatal("the RESET_STREAM never reached the socket: the test proves nothing")
	}
	if err != nil {
		t.Fatalf("Reset = %v (errSentUnwritten: %v) for a packet recorded for recovery", err, errors.Is(err, errSentUnwritten))
	}
}

// StopSending, the same.
func TestAStopSendingWhoseDatagramIsRefusedSucceeds(t *testing.T) {
	c, s := refusingStreamConn(t)
	before := c.Stats().WriteFailures
	err := s.StopSending(0x10c)
	if c.Stats().WriteFailures == before {
		t.Fatal("the STOP_SENDING never reached the socket: the test proves nothing")
	}
	if err != nil {
		t.Fatalf("StopSending = %v (errSentUnwritten: %v)", err, errors.Is(err, errSentUnwritten))
	}
}

// The HTTP/3 server's pattern (net/httpstream, refuse): the batch callback
// resets a stream and returns the error, and processPacket ended the
// connection, every other stream with it.
func TestARefusedResetFromTheBatchCallbackKeepsTheConnection(t *testing.T) {
	c, _ := refusingStreamConn(t)
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error {
		st, err := c.Stream(0)
		if err != nil {
			return err
		}
		if err := st.Reset(0x10c); err != nil {
			return err
		}
		return st.StopSending(0x10c)
	})
	frame := varint.Append(nil, FrameStream|streamOFF|streamLEN)
	frame = varint.Append(frame, 0)
	frame = varint.Append(frame, 0)
	frame = varint.Append(frame, 3)
	frame = append(frame, "abc"...)
	before := c.Stats().WriteFailures
	err := c.processPacket(spaceApplication, false, 0, frame, time.Now())
	if c.Stats().WriteFailures == before {
		t.Fatal("nothing reached the socket: the test proves nothing")
	}
	if err != nil || c.Err() != nil {
		t.Fatalf("a refused send from the batch callback ended the connection: processPacket = %v, Err = %v", err, c.Err())
	}
}

// sendCrypto at the application level (the server's session ticket), the
// same.
func TestARefusedApplicationCryptoSendSucceeds(t *testing.T) {
	c, _ := refusingStreamConn(t)
	before := c.Stats().WriteFailures
	err := c.sendCrypto(tls.QUICEncryptionLevelApplication, []byte("session-ticket"))
	if c.Stats().WriteFailures == before {
		t.Fatal("the CRYPTO frame never reached the socket (queued behind the window?): the test proves nothing")
	}
	if err != nil {
		t.Fatalf("sendCrypto(Application) = %v (errSentUnwritten: %v)", err, errors.Is(err, errSentUnwritten))
	}
}
