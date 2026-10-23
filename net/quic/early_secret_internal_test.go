package quic

import (
	"crypto/tls"
	"testing"
)

// A resumption crypto/tls attempts or accepts hands this package the
// 0-RTT secret at tls.QUICEncryptionLevelEarly, and spaceFor files every
// level it does not name under the application space: the early secret used
// to become 1-RTT keys, sealing short-header packets with a key the peer
// holds only for 0-RTT. A client sends nothing in 0-RTT and keeps no early
// key at all; a server reads the client's 0-RTT with its own opener, never
// with the application space's.
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
