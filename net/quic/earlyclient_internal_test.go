package quic

import (
	"context"
	"crypto/tls"
	"testing"
	"time"
)

// A 0-RTT writer that parks on flow-control credit while another writer is
// counted in (writersIn > 1) flushes the datagram it gathered: held, the
// peer would never see the bytes that open the window it waits for.
func TestParkedWriterDoesNotHoldAGatheredDatagram(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "parked:self"})
	dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	c := assembleConn(tap, fakeFuzzAddr("parked:peer"), dcid, scid, true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid

	const remembered = 100
	c.remembered = TransportParameters{
		InitialMaxData:          remembered,
		InitialMaxStreamDataUni: remembered,
		InitialMaxStreamsUni:    4,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}
	srv := DefaultParameters()
	srv.initialSourceCID = dcid
	srv.originalDCID = dcid
	if err := c.handlePeerParameters(appendParameters(nil, srv)); err != nil {
		t.Fatalf("server parameters: %v", err)
	}
	s, err := c.OpenUniStream(context.Background())
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	// Another writer is on its way: counted in, as Stream.write counts itself
	// before taking mu.
	c.writersIn.Add(1)
	go func() { _, _ = s.Write(make([]byte, 3*remembered)) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		off := int(s.writeOff)
		c.mu.Unlock()
		if off >= remembered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the write never reached the remembered limit (writeOff %d)", off)
		}
		time.Sleep(time.Millisecond)
	}
	// Parked on credit now, with the first bytes in a gathered datagram.
	for range 200 {
		if len(tap.recorded()) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the writer parked on credit with %d bytes of stream data gathered and nothing sent: the peer cannot grant what it never received", remembered)
}
