package quic

import (
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
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
		id       StreamID
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

// A packet's stream growth is summed before the connection's limit judges
// it. With stream windows near 2^62, which Config admits up to varint.Max,
// five STREAM frames wrapped the sum: the packet passed, the connection's
// credit moved a few bytes, and every stream had its highest offset at
// 2^62-1 — whatever the peer sent below it after that cost the connection
// nothing. The control, two frames whose sum does not wrap, is refused with
// FLOW_CONTROL_ERROR as the five now are.
func TestStreamGrowthDoesNotWrapTheConnectionCredit(t *testing.T) {
	build := func() *Conn {
		cfg := DefaultConfig()
		cfg.InitialMaxStreamDataBidiRemote = varint.Max
		cfg.InitialMaxData = 1 << 20
		return assembleConn(&fuzzSink{addr: "wrap:self"}, fakeFuzzAddr("wrap:peer"),
			[]byte("dcid0000"), []byte("scid0000"), false, cfg)
	}
	far := func(ids ...StreamID) []byte {
		var p []byte
		for _, id := range ids {
			p = append(p, streamFrameBytes(id, varint.Max-1, []byte{1})...)
		}
		return p
	}

	t.Run("control", func(t *testing.T) {
		c := build()
		err := c.frames(spaceApplication, far(0, 4), time.Now())
		wantCode(t, err, transportFlowControl, "two streams grown to 2^62-1 under InitialMaxData 1 MiB")
	})
	t.Run("wrapped", func(t *testing.T) {
		c := build()
		payload := append(far(0, 4, 8, 12), streamFrameBytes(16, 7, []byte{1})...)
		err := c.frames(spaceApplication, payload, time.Now())
		c.mu.Lock()
		highest, limit := c.connRecv.highest, c.connRecv.max
		c.mu.Unlock()
		if err == nil {
			t.Fatalf("a packet growing four streams to 2^62-1 and a fifth by 8 bytes was accepted: connection credit used %d of %d",
				highest, limit)
		}
		wantCode(t, err, transportFlowControl, "five streams grown past InitialMaxData")
	})
}
