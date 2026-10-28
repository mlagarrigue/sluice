package quic

import (
	"crypto/tls"
	"testing"
)

// A resumption crypto/tls attempts or accepts hands this package the
// 0-RTT secret at tls.QUICEncryptionLevelEarly, and spaceFor files every
// level it does not name under the application space: the early secret used
// to become 1-RTT keys, sealing short-header packets with a key the peer
// holds only for 0-RTT. A client that did not offer 0-RTT (Dial, not
// DialEarly) keeps no early key at all; a server reads the client's 0-RTT
// with its own opener, never with the application space's.
func TestEarlySecretInstallsNoApplicationKeys(t *testing.T) {
	secret := make([]byte, 32)
	for _, isClient := range []bool{true, false} {
		for level, want := range map[tls.QUICEncryptionLevel]bool{
			tls.QUICEncryptionLevelEarly:       false,
			tls.QUICEncryptionLevelApplication: true,
		} {
			c := &Conn{cc: newNewReno(1200), isClient: isClient, earlyAccepted: make(chan struct{})}
			c.early, c.earlyTicket = eligibleTicket(t)
			for _, kind := range []tls.QUICEventKind{tls.QUICSetWriteSecret, tls.QUICSetReadSecret} {
				done, err := c.handleTLSEvent(tls.QUICEvent{Kind: kind, Level: level, Suite: tls.TLS_AES_128_GCM_SHA256, Data: secret})
				if err != nil || done {
					t.Fatalf("client %v, level %v, kind %v: (%v, %v)", isClient, level, kind, done, err)
				}
			}
			sp := &c.spaces[spaceApplication]
			if got := sp.sealer != nil; got != want {
				t.Errorf("client %v, level %v: application sealer installed = %v, want %v", isClient, level, got, want)
			}
			if got := sp.opener != nil; got != want {
				t.Errorf("client %v, level %v: application opener installed = %v, want %v", isClient, level, got, want)
			}
			early := level == tls.QUICEncryptionLevelEarly && !isClient
			if got := c.earlyOpener != nil; got != early {
				t.Errorf("client %v, level %v: 0-RTT opener installed = %v, want %v", isClient, level, got, early)
			}
			if got := c.Stats().UnhandledTLSEvents; got != 0 {
				t.Errorf("client %v, level %v: UnhandledTLSEvents = %d; a secret this package declines is not an unknown event", isClient, level, got)
			}
		}
	}
	// 0-RTT rejected by the server: nothing went out under the early
	// secret, so there is nothing to resend and nothing to count.
	c := &Conn{cc: newNewReno(1200)}
	if done, err := c.handleTLSEvent(tls.QUICEvent{Kind: tls.QUICRejectedEarlyData}); done || err != nil {
		t.Fatalf("QUICRejectedEarlyData: (%v, %v)", done, err)
	}
	if got := c.Stats().UnhandledTLSEvents; got != 0 {
		t.Errorf("UnhandledTLSEvents = %d after QUICRejectedEarlyData; want 0", got)
	}
}

// The server's limits held behind 0-RTT packets (limitsHeld) take over when
// the 1-RTT write key arrives, and they raise this end's stream counts,
// never lower them: a MAX_STREAMS read meanwhile already moved the count
// past the server's initial value (RFC 9000 §4.6: a MAX_STREAMS that does
// not raise the limit is ignored, and an initial value is no different).
// And a refused 0-RTT applies the server's limits at once, so nothing is
// held for the 1-RTT key to apply again.
func TestHeldLimitsNeverLowerTheStreamCounts(t *testing.T) {
	secret := make([]byte, 32)
	c := assembleConn(&fuzzSink{addr: "held:self"}, fakeFuzzAddr("held:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.mu.Lock()
	c.peerParams.InitialMaxStreamsBidi, c.peerParams.InitialMaxStreamsUni = 10, 10
	c.hasPeerParams, c.limitsHeld = true, true
	c.peerMaxStreamsBidi, c.peerMaxStreamsUni = 50, 50 // raised by a MAX_STREAMS
	c.mu.Unlock()
	if _, err := c.handleTLSEvent(tls.QUICEvent{Kind: tls.QUICSetWriteSecret, Level: tls.QUICEncryptionLevelApplication, Suite: tls.TLS_AES_128_GCM_SHA256, Data: secret}); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	bidi, uni, held := c.peerMaxStreamsBidi, c.peerMaxStreamsUni, c.limitsHeld
	c.mu.Unlock()
	if bidi != 50 || uni != 50 || held {
		t.Errorf("after the 1-RTT key: streams (%d, %d), held %v; want (50, 50), false", bidi, uni, held)
	}

	r := assembleConn(&fuzzSink{addr: "held:self"}, fakeFuzzAddr("held:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, true, DefaultParameters())
	t.Cleanup(func() { _ = r.Close() })
	r.mu.Lock()
	r.peerParams.InitialMaxStreamsBidi = 10
	r.hasPeerParams, r.limitsHeld = true, true
	r.rejectEarlyLocked()
	held = r.limitsHeld
	r.mu.Unlock()
	if held {
		t.Error("a refused 0-RTT left the server's limits held")
	}
}
