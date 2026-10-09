package quic

import (
	"errors"
	"testing"
)

func TestSendQuota(t *testing.T) {
	var q sendQuota
	if q.avail() != 0 {
		t.Error("credit out of nowhere")
	}
	if !q.grant(1000) || q.avail() != 1000 {
		t.Error("a grant did not open the window")
	}
	q.sent = 400
	if q.avail() != 600 {
		t.Errorf("avail = %d", q.avail())
	}
	if q.grant(500) {
		t.Error("a stale, lower grant was honoured")
	}
	q.blockedSent = true
	if !q.grant(2000) || q.blockedSent {
		t.Error("a fresh grant did not reopen or reset the blocked marker")
	}
}

func TestRecvQuotaEnforcesAndRenews(t *testing.T) {
	q := newRecvQuota(1000)
	if err := q.receive(1000); err != nil {
		t.Fatalf("receiving up to the grant: %v", err)
	}
	if err := q.receive(1001); !errors.Is(err, errFlowControl) {
		t.Fatalf("a byte past the grant returned %v", err)
	}
	// Under half the window left → renew.
	if q.consume(400) {
		t.Error("renewed with more than half the window standing")
	}
	if !q.consume(200) {
		t.Error("did not renew once half the window was spent")
	}
	if next := q.nextMax(); next != 600+1000 {
		t.Errorf("renewed to %d", next)
	}
}

// §18.2's local/remote naming, unfolded once here so nobody re-derives it in
// their head at a call site: what this end may send on a stream is bounded
// by the *peer's* announcement, chosen by who opened the stream.
func TestInitialStreamLimitMapping(t *testing.T) {
	peer := TransportParameters{
		InitialMaxStreamDataBidiLocal:  100, // streams the peer opens
		InitialMaxStreamDataBidiRemote: 200, // streams the other end opens
		InitialMaxStreamDataUni:        300,
	}
	tests := []struct {
		id       uint64
		isClient bool
		send     uint64
	}{
		{0, true, 200},  // client-opened bidi, we are the client → peer's "remote"
		{1, true, 100},  // server-opened bidi, we are the client → peer's "local"
		{2, true, 300},  // client-opened uni
		{0, false, 100}, // client-opened bidi, we are the server → peer's "local"
		{1, false, 200}, // server-opened bidi, we are the server → peer's "remote"
	}
	for _, tt := range tests {
		if got := initialStreamSendLimit(peer, tt.id, tt.isClient); got != tt.send {
			t.Errorf("send limit(id %d, client %v) = %d, want %d", tt.id, tt.isClient, got, tt.send)
		}
	}
	// The receive side is the mirror: this end's own announcements.
	if got := initialStreamRecvLimit(peer, 0, true); got != 100 {
		t.Errorf("recv limit for a self-opened stream = %d, want the local announcement 100", got)
	}
	if got := initialStreamRecvLimit(peer, 0, false); got != 200 {
		t.Errorf("recv limit for a peer-opened stream = %d, want the remote announcement 200", got)
	}
}
